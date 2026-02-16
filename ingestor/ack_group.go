package ingestor

import "context"

type AckGroup struct {
	msgs []Message
}

func (g *AckGroup) Add(m Message) {
	g.msgs = append(g.msgs, m)
}

func (g *AckGroup) Commit(ctx context.Context, src Source) error {
	return src.AckBatch(ctx, g.msgs)
}

func (g *AckGroup) Reset() {
	g.msgs = g.msgs[:0]
}
