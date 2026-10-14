//go:build integration

package worker

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"consumer-dispatcher/internal/broker"
	"consumer-dispatcher/internal/config"
	"consumer-dispatcher/internal/forward"
)

// Run with a broker on RABBIT_HOST/RABBIT_PORT (default 127.0.0.1:5672, guest/guest):
//
//	go test -tags=integration ./internal/worker/
func integrationJob(t *testing.T, consumerURL string) (config.ResolvedJob, *broker.Hub) {
	t.Helper()
	port, _ := strconv.Atoi(cmp.Or(os.Getenv("RABBIT_PORT"), "5672"))
	srv := config.Server{
		Host:             cmp.Or(os.Getenv("RABBIT_HOST"), "127.0.0.1"),
		Port:             port,
		Username:         cmp.Or(os.Getenv("RABBIT_USER"), "guest"),
		Password:         cmp.Or(os.Getenv("RABBIT_PASSWORD"), "guest"),
		VHost:            cmp.Or(os.Getenv("RABBIT_VHOST"), "/"),
		ReconnectSeconds: 1,
	}
	hub := broker.NewHub(discardLog)
	t.Cleanup(hub.Close)
	if _, err := hub.Channel("it", srv); err != nil {
		t.Skipf("no broker: %v", err)
	}

	name := fmt.Sprintf("cd_it_%d", time.Now().UnixNano())
	job := config.ResolvedJob{
		Name: name, LogicName: name + "-it", ServerName: "it", Server: srv,
		Queue: name, Exchange: name + "_router", Type: "direct", DLQ: name + ".dlq",
		URL: consumerURL, Timeout: 5 * time.Second, RetryLimit: 0, Prefetch: 1,
		// RabbitMQ 4.x rejects transient non-exclusive queues; queues are deleted in Cleanup.
		ExchangeDurable: true, QueueDurable: true, DeclareTopology: true,
	}
	t.Cleanup(func() {
		ch, err := hub.Channel("it", srv)
		if err != nil {
			return
		}
		defer func() { _ = ch.Close() }()
		_, _ = ch.QueueDelete(job.Queue, false, false, false)
		_, _ = ch.QueueDelete(job.DLQ, false, false, false)
		_ = ch.ExchangeDelete(job.Exchange, false, false)
	})
	return job, hub
}

func publishTo(t *testing.T, hub *broker.Hub, job config.ResolvedJob, body string) {
	t.Helper()
	ch, err := hub.Channel(job.ServerName, job.Server)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	if err := prepareTopology(ch, job); err != nil {
		t.Fatal(err)
	}
	pub, err := broker.NewPublisher(ch)
	if err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish(t.Context(), job.Queue, amqp.Publishing{Body: []byte(body)}); err != nil {
		t.Fatal(err)
	}
}

func queueDepth(t *testing.T, hub *broker.Hub, job config.ResolvedJob, queue string) int {
	t.Helper()
	ch, err := hub.Channel(job.ServerName, job.Server)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	// Passive declare only checks existence; the flags are not compared.
	q, err := ch.QueueDeclarePassive(queue, false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	return q.Messages
}

func eventually(t *testing.T, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// Shutdown with an expired deadline must requeue the in-flight message, not dead-letter it.
func TestIntegrationShutdownRequeuesInFlight(t *testing.T) {
	received := make(chan struct{}, 1)
	testDone := make(chan struct{})
	consumer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The server only notices a client disconnect once the body is consumed.
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case received <- struct{}{}:
		default:
		}
		select { // hang until the worker aborts
		case <-r.Context().Done():
		case <-testDone:
		}
	}))
	t.Cleanup(consumer.Close)
	t.Cleanup(func() { close(testDone) }) // runs before consumer.Close (LIFO)

	job, hub := integrationJob(t, consumer.URL)
	publishTo(t, hub, job, "in-flight")

	p := NewPool(job, hub, forward.New(10*time.Second), discardLog)
	p.SetWorkers(1)
	select {
	case <-received:
	case <-time.After(10 * time.Second):
		t.Fatal("consumer never called")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	p.Shutdown(ctx)

	eventually(t, "message back in main queue", func() bool { return queueDepth(t, hub, job, job.Queue) == 1 })
	if n := queueDepth(t, hub, job, job.DLQ); n != 0 {
		t.Fatalf("dlq has %d messages, want 0", n)
	}
}

// A consumer failure must land in the DLQ with diagnostic headers and leave the main queue empty.
func TestIntegrationFailureGoesToDLQ(t *testing.T) {
	consumer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "nope")
	}))
	t.Cleanup(consumer.Close)

	job, hub := integrationJob(t, consumer.URL)
	publishTo(t, hub, job, "bad")

	p := NewPool(job, hub, forward.New(10*time.Second), discardLog)
	p.SetWorkers(1)
	t.Cleanup(func() { p.Shutdown(context.Background()) })

	eventually(t, "message in dlq", func() bool { return queueDepth(t, hub, job, job.DLQ) == 1 })
	if n := queueDepth(t, hub, job, job.Queue); n != 0 {
		t.Fatalf("main queue has %d messages, want 0", n)
	}
}
