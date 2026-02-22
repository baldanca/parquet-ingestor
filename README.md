# Parquet Ingestor

High-performance ETL pipeline for batching, transforming, and delivering data to **S3 as Parquet**.

**Firehose-like behaviour** with full control, extensibility, and **FinOps efficiency**.

---

<p align="center">
  <a href="https://github.com/baldanca/parquet-ingestor/actions/workflows/ci.yml">
    <img alt="CI" src="https://github.com/baldanca/parquet-ingestor/actions/workflows/ci.yml/badge.svg">
  </a>
  <a href="https://codecov.io/gh/baldanca/parquet-ingestor">
    <img alt="Coverage" src="https://img.shields.io/codecov/c/github/baldanca/parquet-ingestor">
  </a>
  <img alt="Go" src="https://img.shields.io/badge/Go-1.26-blue">
  <img alt="License" src="https://img.shields.io/badge/License-MIT-green">
  <img alt="Performance" src="https://img.shields.io/badge/Performance-optimized-orange">
</p>

---

## 🚀 TL;DR

Use this when you need:

- **batching** by **size** _or_ **time**
- **custom transforms** (JSON → struct, protobuf → struct, etc.)
- **Parquet** output for **Athena / Spark / Trino / Data Mesh**
- **autoscaling workers**
- **graceful shutdown flush** (Kubernetes-friendly)
- **FinOps** (reduce PUTs, control buffering, tune throughput)

---

## 📑 Table of Contents

- [Why](#-why)
- [Features](#-features)
- [Demo](#-demo)
- [Quick Start](#-quick-start)
- [Architecture](#-architecture)
- [Configuration](#-configuration)
- [Reliability & Delivery Guarantees](#-reliability--delivery-guarantees)
- [Kubernetes](#-kubernetes)
- [Performance](#-performance)
- [Firehose vs Parquet Ingestor](#-firehose-vs-parquet-ingestor)
- [Enterprise Adoption Notes](#-enterprise-adoption-notes)
- [CI/CD](#-cicd)
- [Contributing](#-contributing)
- [Security](#-security)
- [Roadmap](#-roadmap)
- [FAQ](#-faq)
- [Maintainer / Hiring](#-maintainer--hiring)
- [License](#-license)

---

## 💡 Why

AWS Firehose is powerful, but at scale you may hit:

- **cost** (especially with small objects and high ingestion)
- **limited customization** in batching/transformations
- **vendor constraints** when you need deeper pipeline control
- **harder local testing** for performance regressions

Parquet Ingestor is designed to be:

- **pluggable** (sources/sinks/transformers)
- **performance-first** (predictable memory + low allocations)
- **cost-aware** (batch to reduce PUTs, tune flush strategy)
- **production ready** (graceful shutdown flush semantics)

---

## ✅ Features

- Batch by **size** or **time**
- Pluggable **transformer** pipeline
- **Parquet** encoding optimized for throughput
- Source: **SQS** (and more planned)
- Sink: **S3**
- **Autoscaling workers**
- **Graceful shutdown flush**
- Low allocation design (benchmarked)

---

## ⚡ Quick Start

Install:

```bash
go get github.com/baldanca/parquet-ingestor
```

Minimal example:

```go
package main

import (
  "context"
  "log"

  "github.com/baldanca/parquet-ingestor/ingestor"
)

func main() {
  ctx := context.Background()

  ing := ingestor.NewDefault()

  if err := ing.Run(ctx); err != nil {
    log.Fatal(err)
  }
}
```

---

## 🏗 Architecture

Pipeline:

```
Source → Transformer → Batcher → Encoder → Sink
```

### Flush triggers

A batch is flushed when:

- **batch size** reaches the configured limit
- **flush timeout** is reached
- **shutdown** signal is received (graceful flush)

---

## ⚙️ Configuration

> Defaults are tuned for general production workloads; override per traffic profile.

### Batch

- `BatchSizeMB`  
  Maximum buffer size before flushing (default example: **5MB**)

- `FlushInterval`  
  Maximum time before forced flush (default example: **5 minutes**)

### Workers

Autoscaler modes:

- **Fixed**: constant number of workers
- **High Performance**: dynamic scaling based on:
  - CPU availability
  - memory pressure
  - buffer backlog

---

## 🛡 Reliability & Delivery Guarantees

Designed for consistent delivery under normal operating conditions:

- Flush on **size/time**
- Flush on **shutdown**
- Ack strategy designed to avoid losing already-processed batches

> If you need stricter semantics (exactly-once), you typically design it end-to-end with idempotency + dedupe downstream.

---

## ☸ Kubernetes

Recommended:

- `terminationGracePeriodSeconds >= FlushInterval` _(or a sensible upper bound for your flush)_

Behaviour on pod termination:

1. stop ingestion
2. flush remaining batch
3. ack processed messages
4. exit

---

## 📊 Performance

Example benchmark (Ryzen 5600G):

- 10 records: ~28µs, ~134 allocs/op
- 10,000 records: ~468µs, ~137 allocs/op

Run locally:

```bash
go test ./... -bench=. -benchmem
```

### Benchmark policy

Performance-sensitive changes should:

- include benchmarks
- keep allocations stable (or justify increases)
- explain tradeoffs in PR description

---

## 🔥 Firehose vs Parquet Ingestor

| Capability             | Firehose | Parquet Ingestor |
| ---------------------- | -------: | ---------------: |
| Custom batching        |  limited |          ✅ full |
| Custom transformations |  limited |          ✅ full |
| Local dev / profiling  |     hard |          ✅ easy |
| Vendor lock-in         |   higher |         ✅ lower |
| FinOps control         |    lower |        ✅ higher |
| Pipeline extensibility |  limited |     ✅ pluggable |

---

## 🏢 Enterprise Adoption Notes

This project is a good fit when you have:

- high TPS ingestion pipelines
- analytics-first storage formats (Parquet)
- data mesh requirements (S3 as lake storage)
- strong cost controls (FinOps)

Recommended additions (optional, common in enterprise):

- OpenTelemetry metrics/exporter
- structured logging
- integration tests in CI (localstack / testcontainers)

---

## 🤖 CI/CD

This repo ships with a GitHub Actions workflow (`.github/workflows/ci.yml`) that runs:

- `go test` + coverage
- `golangci-lint`
- (optional) benchmarks (kept off by default in CI)

Coverage upload is configured for **Codecov** (works well for public repos).

---

## 🤝 Contributing

### Requirements

- Go **1.24**
- Make sure tests and lint pass

### Local checks

```bash
go test ./...
go test ./... -coverprofile=coverage.out
golangci-lint run
```

### Benchmarks

```bash
go test ./... -bench=. -benchmem
```

Guidelines:

- prefer composition
- keep allocations low
- add tests + benchmarks for perf changes

---

## 🔐 Security

Please report vulnerabilities privately:

- **luiz.baldanca@gmail.com**

Do not open public issues for security vulnerabilities.

---

## 🗺 Roadmap

- Kafka source
- Compression options
- Metrics exporter (OpenTelemetry)
- Multi-sink strategies
- Backpressure / adaptive buffering (if needed)

---

## ❓ FAQ

### Is it production ready?

Yes — designed for high throughput ingestion with graceful shutdown flush.

### Does it guarantee delivery?

It is designed for consistent delivery with shutdown flush support. For strict guarantees, combine with idempotent sinks and downstream dedupe where needed.

### Does it support other sources/sinks?

The architecture is pluggable; SQS/S3 are the primary supported implementations currently.

---

## ⭐ Maintainer / Hiring

Maintainer: **Luiz Baldança**

If you want help integrating this into your data platform or need consulting on:

- high throughput Go pipelines
- AWS messaging (SQS/SNS)
- FinOps optimization

Reach out: **luiz.baldanca@gmail.com**

---

## 📄 License

MIT License.
