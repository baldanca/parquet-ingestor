package ingestor

import "context"

type Envelope struct {
	Payload any
	Meta    map[string]string
}

type Message interface {
	Data() Envelope
	Ack(ctx context.Context) error
	Fail(ctx context.Context, reason error) error
}

type Source interface {
	Receive(ctx context.Context) (Message, error)
	AckBatch(ctx context.Context, msgs []Message) error
}