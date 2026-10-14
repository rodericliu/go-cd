//go:build integration

package broker

import (
	"cmp"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Run with a broker: AMQP_URL=amqp://guest:guest@127.0.0.1:5672/ go test -tags=integration ./...
func dialTest(t *testing.T) *amqp.Channel {
	t.Helper()
	conn, err := amqp.Dial(cmp.Or(os.Getenv("AMQP_URL"), "amqp://guest:guest@127.0.0.1:5672/"))
	if err != nil {
		t.Skipf("no broker: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

func TestPublisherConfirmsAndDetectsUnroutable(t *testing.T) {
	ch := dialTest(t)
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := NewPublisher(ch)
	if err != nil {
		t.Fatal(err)
	}

	if err := pub.Publish(t.Context(), q.Name, amqp.Publishing{Body: []byte("x")}); err != nil {
		t.Fatalf("publish to existing queue: %v", err)
	}
	missing := fmt.Sprintf("missing-%d", time.Now().UnixNano())
	if err := pub.Publish(t.Context(), missing, amqp.Publishing{Body: []byte("x")}); !errors.Is(err, ErrUnroutable) {
		t.Fatalf("publish to missing queue: err = %v, want ErrUnroutable", err)
	}
	// The publisher stays usable after an unroutable message.
	if err := pub.Publish(t.Context(), q.Name, amqp.Publishing{Body: []byte("y")}); err != nil {
		t.Fatalf("publish after unroutable: %v", err)
	}
	if got, err := ch.QueueDeclarePassive(q.Name, false, true, true, false, nil); err != nil || got.Messages != 2 {
		t.Fatalf("queue messages = %d, %v; want 2", got.Messages, err)
	}
}
