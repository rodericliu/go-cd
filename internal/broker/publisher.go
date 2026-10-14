package broker

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrNacked is returned when the broker negatively confirms a publish.
var ErrNacked = errors.New("publish nacked by broker")

// ErrUnroutable is returned when a mandatory publish could not be routed to any queue.
var ErrUnroutable = errors.New("publish unroutable")

// Publisher publishes with publisher confirms and the mandatory flag, so a nil
// error means the broker has taken responsibility for the message.
// It is not safe for concurrent use: publishes on one Publisher must be serial.
type Publisher struct {
	ch      *amqp.Channel
	returns chan amqp.Return
}

// NewPublisher puts ch into confirm mode. ch should not be used for other
// publishes afterwards.
func NewPublisher(ch *amqp.Channel) (*Publisher, error) {
	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("confirm mode: %w", err)
	}
	// Returns are dispatched synchronously by the channel's reader; keep a
	// buffer so a stray return can never block it.
	returns := ch.NotifyReturn(make(chan amqp.Return, 16))
	return &Publisher{ch: ch, returns: returns}, nil
}

// Publish sends msg to queue via the default exchange and waits for the confirm.
func (p *Publisher) Publish(ctx context.Context, queue string, msg amqp.Publishing) error {
	p.drainReturns()

	dc, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false, msg)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	acked, err := dc.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("wait confirm: %w", err)
	}
	if !acked {
		return ErrNacked
	}
	// basic.return is delivered before the matching basic.ack, so if the
	// message was unroutable it is already buffered here.
	select {
	case r := <-p.returns:
		return fmt.Errorf("%w: queue %q: %s", ErrUnroutable, queue, r.ReplyText)
	default:
		return nil
	}
}

func (p *Publisher) drainReturns() {
	for {
		select {
		case <-p.returns:
		default:
			return
		}
	}
}
