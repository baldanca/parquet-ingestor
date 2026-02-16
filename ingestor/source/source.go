package source

import "context"

type Envelope struct {
	Payload any
}

type Message interface {
	Data() Envelope
	EstimatedSizeBytes() (n int64, ok bool)
	Fail(ctx context.Context, reason error) error
}

type Sourcer interface {
	Receive(ctx context.Context) (Message, error)
	AckBatch(ctx context.Context, msgs []Message) error
}

type AckGroup struct {
	msgs []Message
}

func (g *AckGroup) Add(m Message) {
	g.msgs = append(g.msgs, m)
}

func (g *AckGroup) Commit(ctx context.Context, src Sourcer) error {
	return src.AckBatch(ctx, g.msgs)
}
