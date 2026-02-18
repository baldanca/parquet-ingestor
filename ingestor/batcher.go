package ingestor

import (
	"errors"
	"time"

	"github.com/baldanca/parquet-ingestor/source"
)

type BatcherConfig struct {
	MaxEstimatedInputBytes int64
	MaxItems               int
	FlushInterval          time.Duration
	ReuseBuffers           bool
}

var DefaultBatcherConfig = BatcherConfig{
	MaxEstimatedInputBytes: 5 * 1024 * 1024, // 5 MiB
	MaxItems:               0,               // no limit
	FlushInterval:          5 * time.Minute,
	ReuseBuffers:           true,
}

func (c BatcherConfig) validate() error {
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
	cfg BatcherConfig

	items []iType
	bytes int64
	acks  source.AckGroup

	spareItems []iType
	spareAcks  source.AckGroup

	itemsCap  int
	targetCap int

	deadline time.Time
	active   bool
}

func NewBatcher[iType any](cfg BatcherConfig) (*Batcher[iType], error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	b := &Batcher[iType]{
		cfg:      cfg,
		itemsCap: 256,
	}

	if cfg.ReuseBuffers {
		b.items = make([]iType, 0, b.itemsCap)
		b.spareItems = make([]iType, 0, b.itemsCap)
		b.acks = source.AckGroup{}
		b.spareAcks = source.AckGroup{}
	}

	return b, nil
}

func (b *Batcher[iType]) Add(now time.Time, item iType, msg source.Message, sizeBytes int64) (flushNow bool) {
	if !b.active {
		b.active = true
		b.deadline = now.Add(b.cfg.FlushInterval)
	}

	if sizeBytes < 0 {
		sizeBytes = 0
	}

	b.items = append(b.items, item)
	b.bytes += sizeBytes
	b.acks.Add(msg)

	if b.cfg.MaxItems > 0 && len(b.items) >= b.cfg.MaxItems {
		return true
	}
	return b.bytes >= b.cfg.MaxEstimatedInputBytes
}

func (b *Batcher[iType]) ShouldFlushTime(now time.Time) bool {
	return b.active && !now.Before(b.deadline)
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
	Acks  source.AckGroup
}

func (b *Batcher[iType]) Flush() Batch[iType] {
	out := Batch[iType]{
		Items: b.items,
		Bytes: b.bytes,
		Acks:  b.acks,
	}

	b.bytes = 0
	b.active = false
	b.deadline = time.Time{}

	if b.cfg.ReuseBuffers {
		b.items, b.spareItems = b.spareItems[:0], out.Items
		b.acks, b.spareAcks = b.spareAcks, out.Acks
		b.acks.Clear()

		b.updateTargetCap(out.Bytes, len(out.Items))
		b.trimItemsIfNeeded()
		b.spareAcks.TrimTo(b.targetCap)
	} else {
		b.items = nil
		b.acks = source.AckGroup{}
	}

	return out
}

func (b *Batcher[iType]) updateTargetCap(bytes int64, n int) {
	if n <= 0 || bytes <= 0 {
		if b.targetCap == 0 {
			b.targetCap = b.itemsCap
		}
		return
	}

	maxItems := b.cfg.MaxEstimatedInputBytes / (bytes / int64(n))
	ideal := int(maxItems)

	if ideal < b.itemsCap {
		ideal = b.itemsCap
	}
	if ideal > 9*b.itemsCap {
		ideal = 9 * b.itemsCap
	}

	b.targetCap = ideal
}

func (b *Batcher[iType]) trimItemsIfNeeded() {
	if b.targetCap == 0 {
		return
	}

	if cap(b.items) > b.targetCap*3 {
		b.items = make([]iType, 0, b.targetCap)
	}
}
