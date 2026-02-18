package source

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"
)

type fakeMsg struct {
	id     string
	handle string
}

func (m fakeMsg) Data() Envelope                               { return Envelope{Payload: m.id} }
func (m fakeMsg) EstimatedSizeBytes() (int64, bool)            { return int64(len(m.id)), true }
func (m fakeMsg) Fail(ctx context.Context, reason error) error { return nil }

func (m fakeMsg) AckMeta() (AckMetadata, bool) {
	if m.handle == "" {
		return AckMetadata{}, false
	}
	return AckMetadata{ID: m.id, Handle: m.handle}, true
}

type fakeSrc struct {
	calls int
	got   []Message
	err   error

	metaCalls int
	gotMeta   []AckMetadata
}

func (s *fakeSrc) Receive(ctx context.Context) (Message, error) {
	return nil, errors.New("not implemented")
}

func (s *fakeSrc) AckBatch(ctx context.Context, msgs []Message) error {
	s.calls++
	s.got = append([]Message(nil), msgs...)
	return s.err
}

func (s *fakeSrc) AckBatchMeta(ctx context.Context, metas []AckMetadata) error {
	s.metaCalls++
	s.gotMeta = append([]AckMetadata(nil), metas...)
	return s.err
}

// benchSrcNoCopy avoids copying inputs so AckGroup benchmarks measure AckGroup overhead.
type benchSrcNoCopy struct{}

func (s *benchSrcNoCopy) Receive(ctx context.Context) (Message, error) {
	return nil, errors.New("not implemented")
}
func (s *benchSrcNoCopy) AckBatch(ctx context.Context, msgs []Message) error {
	return nil
}
func (s *benchSrcNoCopy) AckBatchMeta(ctx context.Context, metas []AckMetadata) error {
	return nil
}

type big1k struct{ buf [1024]byte }
type big4k struct{ buf [4096]byte }

type msgPtr1k struct{ p *big1k }
type msgPtr4k struct{ p *big4k }

func (m msgPtr1k) Data() Envelope                               { return Envelope{Payload: m.p} }
func (m msgPtr1k) EstimatedSizeBytes() (int64, bool)            { return 0, true }
func (m msgPtr1k) Fail(ctx context.Context, reason error) error { return nil }

func (m msgPtr4k) Data() Envelope                               { return Envelope{Payload: m.p} }
func (m msgPtr4k) EstimatedSizeBytes() (int64, bool)            { return 0, true }
func (m msgPtr4k) Fail(ctx context.Context, reason error) error { return nil }

func TestAckGroup_Add_AppendsInOrder(t *testing.T) {
	var g AckGroup

	m1 := fakeMsg{id: "a"}
	m2 := fakeMsg{id: "b"}
	m3 := fakeMsg{id: "c"}

	g.Add(m1)
	g.Add(m2)
	g.Add(m3)

	if len(g.msgs) != 3 {
		t.Fatalf("expected len=3, got %d", len(g.msgs))
	}
	if g.msgs[0] != m1 || g.msgs[1] != m2 || g.msgs[2] != m3 {
		t.Fatalf("messages not appended in order: %#v", g.msgs)
	}
}

func TestAckGroup_Commit_CallsAckBatchWithAllMessages(t *testing.T) {
	var g AckGroup
	src := &fakeSrc{}

	m1 := fakeMsg{id: "1", handle: "h-1"}
	m2 := fakeMsg{id: "2", handle: "h-2"}

	g.Add(m1)
	g.Add(m2)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := g.Commit(ctx, src); err != nil {
		t.Fatalf("commit returned error: %v", err)
	}

	// Since fakeSrc implements AckBatchMeta and messages expose AckMeta, Commit should use fast path.
	if src.metaCalls != 1 {
		t.Fatalf("expected 1 meta call, got %d", src.metaCalls)
	}
	if src.calls != 0 {
		t.Fatalf("expected 0 AckBatch calls, got %d", src.calls)
	}
	if !reflect.DeepEqual(src.gotMeta, []AckMetadata{{ID: "1", Handle: "h-1"}, {ID: "2", Handle: "h-2"}}) {
		t.Fatalf("AckBatchMeta got %v", src.gotMeta)
	}
}

func TestAckGroup_Commit_PropagatesError(t *testing.T) {
	var g AckGroup
	wantErr := errors.New("boom")
	src := &fakeSrc{err: wantErr}

	g.Add(fakeMsg{id: "x", handle: "h-x"})

	err := g.Commit(context.Background(), src)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error %v, got %v", wantErr, err)
	}
}

func TestAckGroup_Clear_EmptiesButReusesCapacity(t *testing.T) {
	var g AckGroup

	for i := 0; i < 10; i++ {
		g.Add(fakeMsg{id: "x"})
	}
	oldCap := cap(g.msgs)
	if oldCap == 0 {
		t.Fatalf("expected cap > 0")
	}

	g.Clear()

	if len(g.msgs) != 0 {
		t.Fatalf("expected len=0 after clear, got %d", len(g.msgs))
	}
	if cap(g.msgs) != oldCap {
		t.Fatalf("expected cap to be reused (%d), got %d", oldCap, cap(g.msgs))
	}

	g.Add(fakeMsg{id: "y"})
	if cap(g.msgs) != oldCap {
		t.Fatalf("expected cap to remain %d after re-add, got %d", oldCap, cap(g.msgs))
	}
}

func TestAckGroup_Clear_NilsOutReferences(t *testing.T) {
	var g AckGroup

	g.Add(msgPtr1k{p: &big1k{}})
	g.Add(msgPtr1k{p: &big1k{}})

	backing := g.msgs
	g.Clear()

	if backing[0] != nil || backing[1] != nil {
		t.Fatalf("expected backing elements to be nil after Clear, got %#v %#v", backing[0], backing[1])
	}
	if len(g.msgs) != 0 {
		t.Fatalf("expected len=0 after Clear, got %d", len(g.msgs))
	}
}

func clearNoNil(g *AckGroup) {
	g.msgs = g.msgs[:0]
}

func benchmarkAckGroupClear(b *testing.B, n int, withNil bool) {
	base := make([]Message, n)
	for i := 0; i < n; i++ {
		base[i] = msgPtr4k{p: &big4k{}}
	}

	var g AckGroup
	g.msgs = make([]Message, n)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		g.msgs = g.msgs[:n]
		copy(g.msgs, base)
		if withNil {
			g.Clear()
		} else {
			clearNoNil(&g)
		}
	}
}

func BenchmarkAckGroup_Clear_WithNil(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			benchmarkAckGroupClear(b, n, true)
		})
	}
}

func BenchmarkAckGroup_Clear_NoNil(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			benchmarkAckGroupClear(b, n, false)
		})
	}
}

func BenchmarkAckGroup_Commit(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			var g AckGroup
			for i := 0; i < n; i++ {
				g.Add(fakeMsg{id: "x", handle: "h-x"})
			}
			src := &benchSrcNoCopy{}
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := g.Commit(ctx, src); err != nil {
					b.Fatalf("commit: %v", err)
				}
			}
		})
	}
}
