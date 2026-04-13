package source

import "context"

// Envelope is the raw payload received from a Source.
//
// The ingestor does not impose any schema on Envelope; it is the transformer's
// responsibility to validate and convert it into typed records.
//
// Attributes carries source-specific message metadata. For SQS sources with
// IncludeAttributes enabled, the map contains system attributes (e.g.
// "ApproximateReceiveCount", "SentTimestamp"), user MessageAttributes (String
// and Number data types), and the message ID under the key "MessageId".
// Attributes is nil when no metadata is available.
type Envelope struct {
	Payload    any
	Attributes map[string]string
}

// Message represents one unit received from a Source.
//
// EstimatedSizeBytes is used by the Batcher to decide when to flush; sources
// that can provide this cheaply (e.g. SQS body length) should implement it.
// When ok is false, the ingestor falls back to JSON-encoding the payload to
// estimate the size.
//
// Fail is called when the transformer returns an error or when size estimation
// fails. Source implementations may use it to extend the visibility timeout so
// the message is retried after a delay rather than immediately.
type Message interface {
	Data() Envelope
	EstimatedSizeBytes() (n int64, ok bool)
	Fail(ctx context.Context, reason error) error
}

// Sourcer reads messages from a queue or stream and acknowledges processed
// batches. Receive must block until a message is available or ctx is cancelled.
//
// AckBatch is called after a successful sink write to confirm that the batch
// of messages was durably stored and can be removed from the source.
type Sourcer interface {
	Receive(ctx context.Context) (Message, error)
	AckBatch(ctx context.Context, msgs []Message) error
}

// VisibilityExtender is an optional interface for sources that support
// per-message visibility leases (e.g. SQS). When a source implements this
// interface and the ingestor has a lease enabled (via EnableLease), the
// ingestor will periodically call ExtendVisibilityBatch during long flushes
// to prevent messages from becoming visible and being re-delivered.
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

// AckGroup accumulates messages that should be acknowledged together after a
// successful sink write.
//
// If the Source supports the optional ackMetaBatcher interface (fast-path via
// AckBatchMeta), AckGroup will use it when all messages provided AckMetadata;
// otherwise it falls back to AckBatch.
//
// AckGroup is not safe for concurrent use. It is owned by the Batcher and
// handed off to a flush job; callers must not share it across goroutines.
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

// PollerScaler is an optional interface for sources that maintain an internal
// pool of polling goroutines. When a source implements PollerScaler, the
// adaptive runtime (EnableAdaptiveRuntime) can scale pollers up or down based
// on buffer pressure and available CPU/memory headroom.
type PollerScaler interface {
	SetPollers(n int)
	Pollers() int
}

// BufferStats is an optional interface for sources that buffer messages
// internally before they are delivered to Receive. The adaptive runtime uses
// BufferUsage to detect when the source-side buffer is filling up (scale up
// pollers) or draining (scale down or hold steady).
type BufferStats interface {
	BufferUsage() (used, capacity int)
}
