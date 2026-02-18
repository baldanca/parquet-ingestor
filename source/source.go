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

// AckMetadata is an optional, source-agnostic acknowledgement identifier.
//
// Sources that can acknowledge messages more efficiently without receiving the
// full Message objects may implement AckBatchMeta, and messages may expose their
// AckMetadata via AckMeta.
//
// The meaning of ID/Handle is source-specific (e.g. MessageId/ReceiptHandle in SQS).
type AckMetadata struct {
	ID     string
	Handle string
}

// ackMetable is implemented by messages that can expose their acknowledgement metadata.
// It's optional to keep Message minimal and source-agnostic.
type ackMetable interface {
	AckMeta() (AckMetadata, bool)
}

// ackMetaBatcher is an optional fast-path for sources that can acknowledge by metadata.
// This keeps AckGroup/source generic and avoids referencing source-specific handle types.
type ackMetaBatcher interface {
	AckBatchMeta(ctx context.Context, metas []AckMetadata) error
}

type AckGroup struct {
	msgs  []Message
	metas []AckMetadata // scratch to avoid per-commit allocations on the fast path
}

func (g *AckGroup) Add(m Message) {
	g.msgs = append(g.msgs, m)
}

func (g *AckGroup) Commit(ctx context.Context, src Sourcer) error {
	if len(g.msgs) == 0 {
		return nil
	}

	// Fast path: if the source supports AckBatchMeta and all messages can expose AckMeta,
	// acknowledge by metadata to avoid extra conversions/allocations.
	if fast, ok := src.(ackMetaBatcher); ok {
		g.metas = g.metas[:0]
		if cap(g.metas) < len(g.msgs) {
			g.metas = make([]AckMetadata, 0, len(g.msgs))
		}

		for _, m := range g.msgs {
			if m == nil {
				continue
			}
			am, ok := m.(ackMetable)
			if !ok {
				// Fallback if any message can't provide metadata.
				return src.AckBatch(ctx, g.msgs)
			}
			meta, ok := am.AckMeta()
			if !ok {
				return src.AckBatch(ctx, g.msgs)
			}
			g.metas = append(g.metas, meta)
		}
		if len(g.metas) == 0 {
			return nil
		}
		return fast.AckBatchMeta(ctx, g.metas)
	}

	return src.AckBatch(ctx, g.msgs)
}

func (g *AckGroup) Clear() {
	for i := range g.msgs {
		g.msgs[i] = nil
	}
	g.msgs = g.msgs[:0]
}

func (g *AckGroup) TrimTo(targetCap int) {
	if targetCap <= 0 {
		g.msgs = nil
		return
	}
	if cap(g.msgs) > targetCap*3 {
		g.msgs = make([]Message, 0, targetCap)
	}
}
