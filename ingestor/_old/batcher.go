package ingestor

import (
	"errors"
	"time"
)

type Config struct {
	// MaxEstimatedInputBytes controls flush-by-size using an *estimated* input size.
	// This is meant to be cheap (e.g. raw message body size) and does not represent encoded output size.
	MaxEstimatedInputBytes int64
	// MaxItems controls flush-by-count. If <= 0, it's ignored.
	MaxItems      int
	FlushInterval time.Duration

	// ReuseBuffers enables a double-buffer strategy to reduce allocations.
	// NOTE: Output slices must remain immutable after Flush, so we swap buffers instead of resetting in-place.
	ReuseBuffers bool

	// RetryPolicy is used to retry transient failures (e.g. S3 throttling) in sink writes.
	// If nil, no retries are performed.
	RetryPolicy RetryPolicy

	// AckRetryPolicy is used to retry AckBatch transient failures.
	// If nil, no retries are performed.
	AckRetryPolicy RetryPolicy
}

var DefaultConfig = Config{
	MaxEstimatedInputBytes: 5 * 1024 * 1024,
	FlushInterval:          5 * time.Minute,
}

func (c Config) Validate() error {
	if c.MaxEstimatedInputBytes <= 0 {
		return errors.New("MaxEstimatedInputBytes must be > 0")
	}
	if c.FlushInterval <= 0 {
		return errors.New("FlushInterval must be > 0")
	}
	if c.MaxItems < 0 {
		return errors.New("MaxItems must be >= 0")
	}
	return nil
}

type Batcher[iType any] struct {
	cfg Config

	items []iType
	bytes int64
	acks  AckGroup

	spareItems []iType
	spareAcks  []Message

	deadline time.Time
	active   bool
}

func NewBatcher[iType any](cfg Config) (*Batcher[iType], error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Batcher[iType]{cfg: cfg}, nil
}

func (b *Batcher[iType]) Add(now time.Time, item iType, msg Message, sizeBytes int64) (flushNow bool) {
	if !b.active {
		b.active = true
		b.deadline = now.Add(b.cfg.FlushInterval)
	}

	b.items = append(b.items, item)
	b.bytes += sizeBytes
	b.acks.Add(msg)

	if b.cfg.MaxItems > 0 && len(b.items) >= b.cfg.MaxItems {
		return true
	}
	if b.bytes >= b.cfg.MaxEstimatedInputBytes {
		return true
	}
	return false
}

func (b *Batcher[iType]) ShouldFlushTime(now time.Time) bool {
	if !b.active {
		return false
	}
	return !now.Before(b.deadline)
}

func (b *Batcher[iType]) Deadline() (t time.Time, ok bool) {
	if !b.active {
		return time.Time{}, false
	}
	return b.deadline, true
}

type Batch[iType any] struct {
	Items []iType
	Bytes int64
	Acks  AckGroup
}

func (b *Batcher[iType]) Flush() Batch[iType] {
	out := Batch[iType]{
		Items: b.items,
		Bytes: b.bytes,
		Acks:  b.acks,
	}

	if b.cfg.ReuseBuffers {
		// Swap buffers so the returned slices remain stable.
		b.items, b.spareItems = b.spareItems[:0], out.Items[:0]
		b.acks.msgs, b.spareAcks = b.spareAcks[:0], out.Acks.msgs[:0]
	} else {
		b.items = nil
		b.acks = AckGroup{}
	}
	b.bytes = 0
	b.active = false
	b.deadline = time.Time{}

	return out
}
