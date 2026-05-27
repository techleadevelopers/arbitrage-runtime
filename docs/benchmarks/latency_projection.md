# Latency Projection Model

This model separates measured hot-gate latency from estimated end-to-end MEV latency.

The current measured artifact is `exports/runtime_latency_benchmarks.json`, generated on Windows/x86_64 in release mode with AVX2 enabled. The adversarial hot-gate path reports:

- `decode.p99`: 1 us
- `fast_preflight.p99`: 6 us
- `adaptive_preflight.p99`: 26 us
- `adaptive_quote.p99`: 30 us
- `total_hot_gate.p99`: 74 us
- `budget_pass`: true

These numbers do not include websocket mempool delivery, pending transaction lookup, pool-state RPC reads, payload construction RPC misses, transaction submission, inclusion, finalization, dashboard IO, persistence, or receipt observation.

## Projection Table

| Stage | Current / Measured | Optimized Target | Evidence Level |
| --- | ---: | ---: | --- |
| Mempool ingest | ~50 ms websocket estimate | 0.1 ms AF_XDP target | estimated |
| Decode | 1 us p99 measured | 2 us target | measured current |
| Fast preflight | 6 us p99 measured | 10 us target | measured current |
| Adaptive preflight | 26 us p99 measured | 5 us target | measured current, target pending |
| Payload build | ~0.82 ms README estimate | 0.2 ms target | needs replay/live measurement |
| Adaptive quote | 30 us p99 measured | 10 us target | measured current, target pending |
| Hot-gate total | 74 us p99 measured | <200 us target | measured current |
| Dedicated RPC | ~50 ms public/paid estimate | 10 ms target | needs network benchmark |
| End-to-end total | ~51 ms estimated | ~10.3 ms target | projected |

## Competitive Interpretation

- A hot-gate under 250 us is now demonstrated in the isolated release benchmark.
- A total path near 10 ms still requires low-latency mempool delivery and a dedicated RPC path near 10 ms.
- A top-1 path below 5 ms total requires co-located RPC or builder infrastructure and a pipeline below roughly 200 us after packet arrival.

## Current Truth

The active runtime still uses websocket mempool ingestion. The `XdpIngest` type intentionally returns an error until a real AF_XDP backend exists. Treat any XDP/io_uring claim as a deployment target, not current production capability.

## Validation Required

Before using this projection as an operational claim, collect:

- network benchmark read/submit p50/p95/p99 for each RPC endpoint
- replay payload-build p50/p95/p99 on Tenderly fork
- live mempool `pending hash -> transaction lookup` latency
- direct-RPC submit latency and inclusion outcome
- realized PnL delta at `profitRecipient`
