package broker

import (
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"consumer-dispatcher/internal/config"
)

// Hub shares one AMQP connection per RabbitMQ server (host/port/vhost/user).
// Workers and admin ops open short-lived channels on those connections.
type Hub struct {
	log  *slog.Logger
	mu   sync.Mutex
	sess map[string]*session
}

type session struct {
	mu   sync.Mutex
	key  string
	srv  config.Server
	conn *amqp.Connection
	log  *slog.Logger
}

func NewHub(log *slog.Logger) *Hub {
	return &Hub{
		log:  log,
		sess: make(map[string]*session),
	}
}

// Key identifies a logical RabbitMQ endpoint.
func Key(serverName string, srv config.Server) string {
	return fmt.Sprintf("%s|%s|%d|%s|%s", serverName, srv.Host, srv.Port, srv.VHost, srv.Username)
}

// Channel returns a new channel on the shared connection for srv.
func (h *Hub) Channel(serverName string, srv config.Server) (*amqp.Channel, error) {
	s := h.getOrCreate(serverName, srv)
	return s.channel()
}

func (h *Hub) getOrCreate(serverName string, srv config.Server) *session {
	key := Key(serverName, srv)
	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.sess[key]; ok {
		return s
	}
	s := &session{key: key, srv: srv, log: h.log.With("amqp", key)}
	h.sess[key] = s
	return s
}

func (s *session) channel() (*amqp.Channel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.conn == nil || s.conn.IsClosed() {
		if err := s.dialLocked(); err != nil {
			return nil, err
		}
	}
	ch, err := s.conn.Channel()
	if err != nil {
		_ = s.conn.Close()
		s.conn = nil
		if err := s.dialLocked(); err != nil {
			return nil, err
		}
		ch, err = s.conn.Channel()
		if err != nil {
			return nil, fmt.Errorf("channel: %w", err)
		}
	}
	return ch, nil
}

func (s *session) dialLocked() error {
	u := &url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(s.srv.Username, s.srv.Password),
		Host:   fmt.Sprintf("%s:%d", s.srv.Host, s.srv.Port),
		Path:   "/",
	}
	conn, err := amqp.DialConfig(u.String(), amqp.Config{
		Vhost:     s.srv.VHost,
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
		Properties: amqp.Table{
			"connection_name": "consumer-dispatcher/" + s.key,
		},
	})
	if err != nil {
		return fmt.Errorf("dial %s: %w", s.srv.Host, err)
	}
	s.conn = conn
	s.log.Info("amqp connected", "host", s.srv.Host, "vhost", s.srv.VHost)

	closeCh := make(chan *amqp.Error, 1)
	conn.NotifyClose(closeCh)
	go func(key string, c *amqp.Connection) {
		err := <-closeCh
		s.mu.Lock()
		if s.conn == c {
			s.conn = nil
		}
		s.mu.Unlock()
		if err != nil {
			s.log.Warn("amqp connection closed", "err", err)
		}
	}(s.key, conn)
	return nil
}

// Close closes all shared connections.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, s := range h.sess {
		s.mu.Lock()
		if s.conn != nil {
			_ = s.conn.Close()
			s.conn = nil
		}
		s.mu.Unlock()
	}
	clear(h.sess)
}

// Ping opens and closes a channel to verify connectivity.
func (h *Hub) Ping(serverName string, srv config.Server) error {
	ch, err := h.Channel(serverName, srv)
	if err != nil {
		return err
	}
	return ch.Close()
}
