package source

import "context"

// Envelope is the raw payload received from a Source.
//
// The ingestor does not impose any schema on Envelope; it is the transformer's
// responsibility to validate and convert it into a typed record.
type Envelope struct {
	Payload any
}

// Message represents one unit received from a Source.
//
// Implementations may optionally expose an estimated size to help the batcher
// flush earlier without counting exact bytes.
type Message interface {
	Data() Envelope
	EstimatedSizeBytes() (n int64, ok bool)
	Fail(ctx context.Context, reason error) error
}

// Sourcer reads messages and acknowledges them in batches.
//
// Sources should ensure that Receive blocks until a message is available or the
// context is canceled.
type Sourcer interface {
	Receive(ctx context.Context) (Message, error)
	AckBatch(ctx context.Context, msgs []Message) error
}

// VisibilityExtender can extend the visibility timeout for a batch of messages.
//
// This is primarily useful for SQS-style leases when flushing and uploading
// takes longer than the queue visibility timeout.
type VisibilityExtender interface {
	ExtendVisibilityBatch(ctx context.Context, metas []AckMetadata, timeoutSeconds int32) error
}

// AckMetadata is a compact, source-specific handle used for fast acknowledgements
// and lease extensions.
type AckMetadata struct {
	ID     string
	Handle string
}

type ackMetable interface {
	AckMeta() (AckMetadata, bool)
}

type ackMetaBatcher interface {
	AckBatchMeta(ctx context.Context, metas []AckMetadata) error
}

// AckGroup accumulates messages that should be acknowledged together.
//
// If the Source supports fast acknowledgements via AckBatchMeta, the AckGroup
// will prefer it when all messages provide AckMetadata.
type AckGroup struct {
	msgs  []Message
	metas []AckMetadata
}

// Add appends a message to the group.
func (g *AckGroup) Add(m Message) {
	g.msgs = append(g.msgs, m)

	if am, ok := m.(ackMetable); ok {
		if meta, ok := am.AckMeta(); ok {
			g.metas = append(g.metas, meta)
		}
	}
}

// Commit acknowledges the group against the given Source.
func (g *AckGroup) Commit(ctx context.Context, src Sourcer) error {
	if len(g.msgs) == 0 {
		return nil
	}

	if fast, ok := src.(ackMetaBatcher); ok && len(g.metas) == len(g.msgs) && len(g.metas) > 0 {
		return fast.AckBatchMeta(ctx, g.metas)
	}

	return src.AckBatch(ctx, g.msgs)
}

// Clear resets the group and releases references to messages.
func (g *AckGroup) Clear() {
	for i := range g.msgs {
		g.msgs[i] = nil
	}
	g.msgs = g.msgs[:0]
	g.metas = g.metas[:0]
}

// TrimTo reduces backing array capacity opportunistically to keep memory bounded.
func (g *AckGroup) TrimTo(targetCap int) {
	if targetCap <= 0 {
		g.msgs = nil
		g.metas = nil
		return
	}
	if cap(g.msgs) > targetCap*3 {
		g.msgs = make([]Message, 0, targetCap)
	}
	if cap(g.metas) > targetCap*3 {
		g.metas = make([]AckMetadata, 0, targetCap)
	}
}

// Snapshot returns a shallow copy of the group slices.
func (g AckGroup) Snapshot() AckGroup {
	if len(g.msgs) > 0 {
		cp := make([]Message, len(g.msgs))
		copy(cp, g.msgs)
		g.msgs = cp
	} else {
		g.msgs = nil
	}

	if len(g.metas) > 0 {
		cp := make([]AckMetadata, len(g.metas))
		copy(cp, g.metas)
		g.metas = cp
	} else {
		g.metas = nil
	}

	return g
}

// Metas exposes the collected metadata for lease management.
func (g *AckGroup) Metas() []AckMetadata {
	return g.metas
}
