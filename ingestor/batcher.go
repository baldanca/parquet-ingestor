package ingestor

import (
	"errors"
	"time"
)

type Config struct {
	MaxBufferBytes int64         
	FlushInterval  time.Duration 
}

var DefaultConfig = Config{
	MaxBufferBytes: 5 * 1024 * 1024,
	FlushInterval:  5 * time.Minute,
}

func (c Config) Validate() error {
	if c.MaxBufferBytes <= 0 {
		return errors.New("MaxBufferBytes must be > 0")
	}
	if c.FlushInterval <= 0 {
		return errors.New("FlushInterval must be > 0")
	}
	return nil
}

type Batcher[iType any] struct {
	cfg Config

	items []iType
	bytes int64
	acks  AckGroup

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

	if b.bytes >= b.cfg.MaxBufferBytes {
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

	b.items = nil
	b.bytes = 0
	b.acks = AckGroup{}
	b.active = false
	b.deadline = time.Time{}

	return out
}
