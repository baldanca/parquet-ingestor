package encoder

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

type testItem struct {
	ID    int64   `parquet:"name=id"`
	Name  string  `parquet:"name=name"`
	Value float64 `parquet:"name=value"`
}

func readAllParquet[T any](t *testing.T, b []byte) ([]T, error) {
	t.Helper()

	r := parquet.NewGenericReader[T](bytes.NewReader(b))
	defer r.Close()

	const batchSize = 256
	buf := make([]T, batchSize)

	out := make([]T, 0, batchSize)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			out = append(out, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return out, err
		}
	}

	return out, nil
}

func TestParquetEncoder_FileExtension(t *testing.T) {
	e := ParquetEncoder[testItem]{}
	if got := e.FileExtension(); got != ".parquet" {
		t.Fatalf("FileExtension() = %q; want %q", got, ".parquet")
	}
}

func TestParquetEncoder_UnsupportedCompression(t *testing.T) {
	e := ParquetEncoder[testItem]{Compression: "brotli"} // não suportado
	_, _, err := e.Encode(context.Background(), []testItem{{ID: 1}})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestParquetEncoder_ContextCanceledBefore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	e := ParquetEncoder[testItem]{}
	_, _, err := e.Encode(ctx, []testItem{{ID: 1}})
	if err == nil {
		t.Fatal("expected context error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestParquetEncoder_EncodeRoundTrip_NoCompression(t *testing.T) {
	items := []testItem{
		{ID: 1, Name: "a", Value: 1.25},
		{ID: 2, Name: "b", Value: 2.50},
		{ID: 3, Name: "c", Value: 3.75},
	}

	e := ParquetEncoder[testItem]{Compression: ""}
	data, ct, err := e.Encode(context.Background(), items)
	if err != nil {
		t.Fatalf("Encode() error: %v", err)
	}
	if ct != "application/vnd.apache.parquet" {
		t.Fatalf("contentType = %q; want %q", ct, "application/vnd.apache.parquet")
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty parquet bytes")
	}

	got, err := readAllParquet[testItem](t, data)
	if err != nil {
		t.Fatalf("read parquet error: %v", err)
	}
	if len(got) != len(items) {
		t.Fatalf("expected %d rows back, got %d", len(items), len(got))
	}

	// valida campos básicos
	for i := range items {
		if got[i] != items[i] {
			t.Fatalf("row %d mismatch: got=%+v want=%+v", i, got[i], items[i])
		}
	}
}

func TestParquetEncoder_EncodeRoundTrip_WithCompression_Snappy(t *testing.T) {
	items := []testItem{
		{ID: 10, Name: "x", Value: 10},
		{ID: 11, Name: "y", Value: 11},
	}

	e := ParquetEncoder[testItem]{Compression: "snappy"}
	data, ct, err := e.Encode(context.Background(), items)
	if err != nil {
		t.Fatalf("Encode() error: %v", err)
	}
	if ct != "application/vnd.apache.parquet" {
		t.Fatalf("contentType = %q; want %q", ct, "application/vnd.apache.parquet")
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty parquet bytes")
	}

	got, err := readAllParquet[testItem](t, data)
	if err != nil {
		t.Fatalf("read parquet error: %v", err)
	}
	if len(got) != len(items) {
		t.Fatalf("expected %d rows back, got %d", len(items), len(got))
	}
}

func TestParquetEncoder_ContextDeadlineExceededBefore(t *testing.T) {
	// ctx já expirado antes do Encode começar (mais determinístico do que tentar "expirar durante")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	time.Sleep(1 * time.Millisecond)

	e := ParquetEncoder[testItem]{}
	_, _, err := e.Encode(ctx, []testItem{{ID: 1, Name: "late", Value: 1}})
	if err == nil {
		t.Fatal("expected context deadline error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

// -------------------- Benchmarks --------------------

type benchItem struct {
	ID    int64   `parquet:"name=id"`
	Name  string  `parquet:"name=name"`
	Value float64 `parquet:"name=value"`
}

func makeBenchItems(n int) []benchItem {
	items := make([]benchItem, n)
	for i := 0; i < n; i++ {
		items[i] = benchItem{
			ID:    int64(i),
			Name:  fmt.Sprintf("item-%d", i),
			Value: float64(i) * 1.337,
		}
	}
	return items
}

func benchmarkParquetEncode(b *testing.B, n int, compression string) {
	b.Helper()

	items := makeBenchItems(n)
	enc := ParquetEncoder[benchItem]{Compression: compression}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data, ct, err := enc.Encode(ctx, items)
		if err != nil {
			b.Fatalf("Encode error: %v", err)
		}
		if ct == "" || len(data) == 0 {
			b.Fatalf("invalid result: ct=%q len=%d", ct, len(data))
		}
		_ = data[len(data)-1]
	}
}

func BenchmarkParquetEncoder_NoCompression(b *testing.B) {
	for _, n := range []int{10, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			benchmarkParquetEncode(b, n, "")
		})
	}
}

func BenchmarkParquetEncoder_Snappy(b *testing.B) {
	for _, n := range []int{10, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			benchmarkParquetEncode(b, n, "snappy")
		})
	}
}

func BenchmarkParquetEncoder_Gzip(b *testing.B) {
	for _, n := range []int{10, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			benchmarkParquetEncode(b, n, "gzip")
		})
	}
}

func BenchmarkParquetEncoder_Zstd(b *testing.B) {
	for _, n := range []int{10, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			benchmarkParquetEncode(b, n, "zstd")
		})
	}
}
