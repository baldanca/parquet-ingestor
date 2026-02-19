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

type VisibilityExtender interface {
	ExtendVisibilityBatch(ctx context.Context, metas []AckMetadata, timeoutSeconds int32) error
}

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

type AckGroup struct {
	msgs  []Message     // opcional: pode manter se você ainda usa em algum lugar
	metas []AckMetadata // agora é o canonical: metas de todos os msgs que entraram
}

func (g *AckGroup) Add(m Message) {
	g.msgs = append(g.msgs, m)

	am, ok := m.(ackMetable)
	if !ok {
		// Se você quer "só o mais eficiente", aqui pode dar panic/erro.
		// Ou você decide: não adiciona meta e depois falha no Commit.
		return
	}
	meta, ok := am.AckMeta()
	if !ok {
		return
	}
	g.metas = append(g.metas, meta)
}

func (g *AckGroup) Commit(ctx context.Context, src Sourcer) error {
	if len(g.metas) == 0 {
		return nil
	}
	fast, ok := src.(ackMetaBatcher)
	if !ok {
		// Se você quer só o mais eficiente, aqui pode retornar erro.
		return src.AckBatch(ctx, g.msgs)
	}
	return fast.AckBatchMeta(ctx, g.metas)
}

func (g *AckGroup) Clear() {
	for i := range g.msgs {
		g.msgs[i] = nil
	}
	g.msgs = g.msgs[:0]
	g.metas = g.metas[:0]
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

// Só pra lease:
func (g *AckGroup) Metas() []AckMetadata {
	return g.metas
}
