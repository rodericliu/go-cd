package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"consumer-dispatcher/internal/config"
)

const dialTimeout = 10 * time.Second

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

// NewHub returns an empty Hub; connections are dialed lazily.
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
	if err == nil {
		return ch, nil
	}
	// Only redial when the connection itself is gone. A channel-level failure
	// (e.g. channel_max reached) must not tear down a connection that other
	// workers are still using.
	if !s.conn.IsClosed() {
		return nil, fmt.Errorf("channel: %w", err)
	}
	if err := s.dialLocked(); err != nil {
		return nil, err
	}
	ch, err = s.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("channel: %w", err)
	}
	return ch, nil
}

func (s *session) dialLocked() error {
	u := &url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(s.srv.Username, s.srv.Password),
		Host:   net.JoinHostPort(s.srv.Host, strconv.Itoa(s.srv.Port)),
		Path:   "/",
	}
	conn, err := amqp.DialConfig(u.String(), amqp.Config{
		Vhost:     s.srv.VHost,
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
		Dial:      amqp.DefaultDial(dialTimeout),
		Properties: amqp.Table{
			"connection_name": "consumer-dispatcher/" + s.key,
		},
	})
	if err != nil {
		return fmt.Errorf("dial %s: %w", s.srv.Host, err)
	}
	s.conn = conn
	s.log.Info("amqp connected", "host", s.srv.Host, "vhost", s.srv.VHost)

	closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		err := <-closeCh
		s.mu.Lock()
		if s.conn == conn {
			s.conn = nil
		}
		s.mu.Unlock()
		if err != nil {
			s.log.Warn("amqp connection closed", "err", err)
		}
	}()
	return nil
}

// Close closes all shared connections.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, s := range h.sess {
		s.mu.Lock()
		if s.conn != nil {
			if err := s.conn.Close(); err != nil {
				s.log.Warn("amqp close", "err", err)
			}
			s.conn = nil
		}
		s.mu.Unlock()
	}
	clear(h.sess)
}

// Ping opens and closes a channel to verify connectivity. It returns when ctx
// is done even if dialing is still in progress.
func (h *Hub) Ping(ctx context.Context, serverName string, srv config.Server) error {
	errCh := make(chan error, 1)
	go func() {
		ch, err := h.Channel(serverName, srv)
		if err != nil {
			errCh <- err
			return
		}
		errCh <- ch.Close()
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
