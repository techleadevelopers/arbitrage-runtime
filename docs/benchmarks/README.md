# Runtime Benchmark Artifacts

This directory stores measured runtime load-test outputs used as operational evidence.

## Files

- `runtime_baseline_report.json`: clean synthetic flow through the hot-gate path.
- `runtime_adversarial_report.json`: hostile synthetic flow with malformed calldata, unknown selectors, small-notional spam, toxic gas, long paths, cluster bursts, and valid opportunities.

## Interpretation

Use these files to compare:

- `total_hot_gate.p50`, `p95`, `p99`, and `max`
- per-stage latency for `decode`, `fast_preflight`, `adaptive_preflight`, and `adaptive_quote`
- `throughput_tps`
- total `rejection_rate`
- per-scenario rejection rates in the adversarial profile

The load-test harness is intentionally narrower than a live runtime profile. It excludes RPC, payload lookup, relay submission, dashboard IO, persistence, and receipt observation. Treat these artifacts as hot-path evidence, not as end-to-end profitability evidence.

## Regeneration

For a copyable environment template, see `runtime-load-test.env.example`.

Baseline:

```bash
RUN_RUNTIME_LOAD_TEST=true \
RUNTIME_LOAD_TEST_PROFILE=baseline \
RUNTIME_LOAD_TEST_CASES=100000 \
RUNTIME_LOAD_TEST_OUTPUT_PATH=runtime_baseline_report.json \
cargo run --release -- --network polygon
```

Adversarial:

```bash
RUN_RUNTIME_LOAD_TEST=true \
RUNTIME_LOAD_TEST_PROFILE=adversarial \
RUNTIME_LOAD_TEST_CASES=100000 \
RUNTIME_LOAD_TEST_OUTPUT_PATH=runtime_adversarial_report.json \
cargo run --release -- --network polygon
```
