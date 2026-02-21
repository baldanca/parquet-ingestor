package source

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

type testMsg struct {
	id     string
	handle string
	metaOK bool
}

func (m testMsg) Data() Envelope                               { return Envelope{Payload: m.id} }
func (m testMsg) EstimatedSizeBytes() (int64, bool)            { return int64(len(m.id)), true }
func (m testMsg) Fail(ctx context.Context, reason error) error { return nil }

func (m testMsg) AckMeta() (AckMetadata, bool) {
	if !m.metaOK || m.handle == "" {
		return AckMetadata{}, false
	}
	return AckMetadata{ID: m.id, Handle: m.handle}, true
}

type fakeSrc struct {
	ackCalls     int
	ackMetaCalls int

	gotMsgs  []Message
	gotMetas []AckMetadata

	err error
}

func (s *fakeSrc) Receive(ctx context.Context) (Message, error) {
	return nil, errors.New("not implemented")
}

func (s *fakeSrc) AckBatch(ctx context.Context, msgs []Message) error {
	s.ackCalls++
	s.gotMsgs = append([]Message(nil), msgs...)
	return s.err
}

func (s *fakeSrc) AckBatchMeta(ctx context.Context, metas []AckMetadata) error {
	s.ackMetaCalls++
	s.gotMetas = append([]AckMetadata(nil), metas...)
	return s.err
}

func TestAckGroup_Add_AppendsInOrder(t *testing.T) {
	var g AckGroup

	m1 := testMsg{id: "a"}
	m2 := testMsg{id: "b"}
	m3 := testMsg{id: "c"}

	g.Add(m1)
	g.Add(m2)
	g.Add(m3)

	if got := len(g.msgs); got != 3 {
		t.Fatalf("expected len=3, got %d", got)
	}
	if g.msgs[0] != m1 || g.msgs[1] != m2 || g.msgs[2] != m3 {
		t.Fatalf("messages not appended in order: %#v", g.msgs)
	}
}

func TestAckGroup_Commit_UsesMetaPathWhenAllMetasAvailable(t *testing.T) {
	var g AckGroup
	src := &fakeSrc{}

	g.Add(testMsg{id: "1", handle: "h-1", metaOK: true})
	g.Add(testMsg{id: "2", handle: "h-2", metaOK: true})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := g.Commit(ctx, src); err != nil {
		t.Fatalf("commit returned error: %v", err)
	}

	if src.ackMetaCalls != 1 {
		t.Fatalf("expected 1 AckBatchMeta call, got %d", src.ackMetaCalls)
	}
	if src.ackCalls != 0 {
		t.Fatalf("expected 0 AckBatch calls, got %d", src.ackCalls)
	}

	want := []AckMetadata{
		{ID: "1", Handle: "h-1"},
		{ID: "2", Handle: "h-2"},
	}
	if len(src.gotMetas) != len(want) {
		t.Fatalf("AckBatchMeta metas len=%d want=%d", len(src.gotMetas), len(want))
	}
	for i := range want {
		if src.gotMetas[i] != want[i] {
			t.Fatalf("AckBatchMeta metas[%d]=%v want=%v", i, src.gotMetas[i], want[i])
		}
	}
}

func TestAckGroup_Commit_FallsBackWhenAnyMetaMissing(t *testing.T) {
	var g AckGroup
	src := &fakeSrc{}

	g.Add(testMsg{id: "1", handle: "h-1", metaOK: true})
	g.Add(testMsg{id: "2", handle: "", metaOK: false}) // missing meta => fallback path

	if err := g.Commit(context.Background(), src); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if src.ackCalls != 1 {
		t.Fatalf("AckBatch calls=%d want=1", src.ackCalls)
	}
	if src.ackMetaCalls != 0 {
		t.Fatalf("AckBatchMeta calls=%d want=0", src.ackMetaCalls)
	}
}

func TestAckGroup_Commit_PropagatesError(t *testing.T) {
	var g AckGroup
	wantErr := errors.New("boom")
	src := &fakeSrc{err: wantErr}

	g.Add(testMsg{id: "x", handle: "h-x", metaOK: true})

	err := g.Commit(context.Background(), src)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error %v, got %v", wantErr, err)
	}
}

func TestAckGroup_Clear_EmptiesButReusesCapacity(t *testing.T) {
	var g AckGroup

	for i := 0; i < 10; i++ {
		g.Add(testMsg{id: "x"})
	}
	oldCap := cap(g.msgs)
	if oldCap == 0 {
		t.Fatalf("expected cap > 0")
	}

	g.Clear()

	if got := len(g.msgs); got != 0 {
		t.Fatalf("expected len=0 after clear, got %d", got)
	}
	if got := cap(g.msgs); got != oldCap {
		t.Fatalf("expected cap to be reused (%d), got %d", oldCap, got)
	}

	g.Add(testMsg{id: "y"})
	if got := cap(g.msgs); got != oldCap {
		t.Fatalf("expected cap to remain %d after re-add, got %d", oldCap, got)
	}
}

func TestAckGroup_Commit_EmptyGroup_NoCalls(t *testing.T) {
	var g AckGroup
	src := &fakeSrc{}

	if err := g.Commit(context.Background(), src); err != nil {
		t.Fatalf("Commit on empty group returned error: %v", err)
	}
	if src.ackCalls != 0 || src.ackMetaCalls != 0 {
		t.Fatalf("expected no ack calls, got ackCalls=%d ackMetaCalls=%d", src.ackCalls, src.ackMetaCalls)
	}
}

func TestAckGroup_TrimTo_TargetCapZero_NilsSlices(t *testing.T) {
	var g AckGroup
	g.Add(testMsg{id: "1", handle: "h-1", metaOK: true})

	g.TrimTo(0)

	if g.msgs != nil {
		t.Fatalf("expected msgs to be nil when targetCap<=0")
	}
	if g.metas != nil {
		t.Fatalf("expected metas to be nil when targetCap<=0")
	}
}

func TestAckGroup_TrimTo_ShrinksBackingArraysWhenTooLarge(t *testing.T) {
	var g AckGroup
	g.msgs = make([]Message, 0, 100)
	g.metas = make([]AckMetadata, 0, 100)

	// Trigger shrink: cap > targetCap*3
	g.TrimTo(10)

	if cap(g.msgs) != 10 {
		t.Fatalf("expected msgs cap=10, got %d", cap(g.msgs))
	}
	if cap(g.metas) != 10 {
		t.Fatalf("expected metas cap=10, got %d", cap(g.metas))
	}
	if len(g.msgs) != 0 || len(g.metas) != 0 {
		t.Fatalf("expected lens to remain 0, got msgs=%d metas=%d", len(g.msgs), len(g.metas))
	}
}

func TestAckGroup_Snapshot_DeepCopiesSlices(t *testing.T) {
	var g AckGroup
	g.Add(testMsg{id: "1", handle: "h-1", metaOK: true})
	g.Add(testMsg{id: "2", handle: "h-2", metaOK: true})

	snap := g.Snapshot()

	if len(snap.msgs) != 2 || len(snap.metas) != 2 {
		t.Fatalf("unexpected snapshot lengths: msgs=%d metas=%d", len(snap.msgs), len(snap.metas))
	}

	// Mutate original backing array; snapshot must not change.
	g.msgs[0] = testMsg{id: "Z"}
	g.metas[0] = AckMetadata{ID: "Z", Handle: "H"}

	if m := snap.msgs[0].(testMsg); m.id != "1" {
		t.Fatalf("snapshot msgs[0] mutated: got %q want %q", m.id, "1")
	}
	if snap.metas[0].ID != "1" || snap.metas[0].Handle != "h-1" {
		t.Fatalf("snapshot metas[0] mutated: got %+v want {ID:1 Handle:h-1}", snap.metas[0])
	}
}

func TestAckGroup_Snapshot_EmptyReturnsNilSlices(t *testing.T) {
	var g AckGroup
	snap := g.Snapshot()
	if snap.msgs != nil {
		t.Fatalf("expected nil msgs slice on empty snapshot")
	}
	if snap.metas != nil {
		t.Fatalf("expected nil metas slice on empty snapshot")
	}
}

func TestAckGroup_Metas_ReturnsCollectedMetas(t *testing.T) {
	var g AckGroup
	g.Add(testMsg{id: "1", handle: "h-1", metaOK: true})
	g.Add(testMsg{id: "2", handle: "h-2", metaOK: true})

	metas := g.Metas()
	if len(metas) != 2 {
		t.Fatalf("expected 2 metas, got %d", len(metas))
	}
	if metas[0].ID != "1" || metas[1].ID != "2" {
		t.Fatalf("unexpected metas: %+v", metas)
	}
}

type big1k struct{ _ [1024]byte }
type big4k struct{ _ [4096]byte }

type msgPtr1k struct{ p *big1k }
type msgPtr4k struct{ p *big4k }

func (m msgPtr1k) Data() Envelope                               { return Envelope{Payload: m.p} }
func (m msgPtr1k) EstimatedSizeBytes() (int64, bool)            { return 0, true }
func (m msgPtr1k) Fail(ctx context.Context, reason error) error { return nil }
func (m msgPtr1k) AckMeta() (AckMetadata, bool)                 { return AckMetadata{}, false }

func (m msgPtr4k) Data() Envelope                               { return Envelope{Payload: m.p} }
func (m msgPtr4k) EstimatedSizeBytes() (int64, bool)            { return 0, true }
func (m msgPtr4k) Fail(ctx context.Context, reason error) error { return nil }
func (m msgPtr4k) AckMeta() (AckMetadata, bool)                 { return AckMetadata{}, false }

func TestAckGroup_Clear_NilsOutReferences(t *testing.T) {
	var g AckGroup

	g.Add(msgPtr1k{p: &big1k{}})
	g.Add(msgPtr1k{p: &big1k{}})

	backing := g.msgs
	g.Clear()

	if backing[0] != nil || backing[1] != nil {
		t.Fatalf("expected backing elements to be nil after Clear, got %#v %#v", backing[0], backing[1])
	}
	if got := len(g.msgs); got != 0 {
		t.Fatalf("expected len=0 after Clear, got %d", got)
	}
}

type benchSrc struct{}

func (benchSrc) Receive(ctx context.Context) (Message, error)                { return nil, context.Canceled }
func (benchSrc) AckBatch(ctx context.Context, msgs []Message) error          { return nil }
func (benchSrc) AckBatchMeta(ctx context.Context, metas []AckMetadata) error { return nil }

func clearNoNil(g *AckGroup) { g.msgs = g.msgs[:0] }

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
				g.Add(testMsg{id: "x", handle: "h-x", metaOK: true})
			}

			src := benchSrc{}
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

func BenchmarkAckGroup_Commit_Parallel(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			src := benchSrc{}
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				var g AckGroup
				g.msgs = make([]Message, 0, n)
				for i := 0; i < n; i++ {
					g.Add(testMsg{id: "x", handle: "h-x", metaOK: true})
				}
				for pb.Next() {
					if err := g.Commit(ctx, src); err != nil {
						b.Fatalf("commit: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkAckGroup_Commit_FastPath_MetaBatch(b *testing.B) {
	ctx := context.Background()
	src := benchSrc{}

	const n = 1000
	var g AckGroup
	g.msgs = make([]Message, 0, n)
	for i := 0; i < n; i++ {
		g.Add(testMsg{id: "x", handle: "h-x", metaOK: true})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = g.Commit(ctx, src)
	}
}

func BenchmarkAckGroup_Commit_Fallback_AckBatch(b *testing.B) {
	ctx := context.Background()
	src := benchSrc{}

	const n = 1000
	var g AckGroup
	g.msgs = make([]Message, 0, n)
	for i := 0; i < n; i++ {
		g.Add(testMsg{id: "x", metaOK: false}) // missing meta => fallback
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = g.Commit(ctx, src)
	}
}
