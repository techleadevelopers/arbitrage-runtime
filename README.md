# arbitrage-runtime

`arbitrage-runtime` is a Rust execution engine for deterministic on-chain arbitrage and fee extraction around pending AMM swaps.

The system is designed for private adversarial operation, with emphasis on:

- mempool-first opportunity detection
- deterministic AMM impact modeling across Uniswap V2 and V3 style paths
- selective execution under adversarial conditions
- adaptive gating calibrated by realized outcomes
- chain-specific execution behavior for BNB Chain and Polygon

This is not a directional trading bot, portfolio manager, or generic multi-strategy MEV framework.

It is an execution system focused on one narrow problem:

`pending swap -> deterministic impact -> EV/risk gate -> execution path -> realized PnL`

## Integrated Runtime Updates - 2026-05-30

This snapshot includes the latest Polygon runtime hardening work:

- **Split V2/V3 executors:** V2 continues to use `MEV_EXECUTOR_ADDRESS`; V3 now uses `MEV_EXECUTOR_V3_ADDRESS`.
- **V3-only executor contract:** `contracts/ArbitrageRuntimeExecutorV3.sol` was added so V3 can be deployed separately from the already-working V2 executor and avoid combined-contract size pressure.
- **Polygon V2/V3 contract setup:** runtime expects QuickSwap V2 router/factory for V2 and Uniswap V3 SwapRouter/factory for V3.
- **Executor allowlists:** V2 requires allowed operator, router, borrow tokens, and V2 pairs; V3 requires allowed operator, router, borrow tokens, V3 pools, and fee tiers.
- **Universal Router improvements:** the decoder now handles direct V2/V3 commands, `execute_sub_plan`, nested subplan inspection, multi-swap candidate ranking, monitored-token preference, and route-graph fallback into hop-level candidates.
- **Executable router normalization:** Universal Router victim flow is decoded as signal source, but payload execution uses the canonical V2/V3 routers instead of trying to execute through the victim's Universal Router.
- **Two-lane candidate handling:** post-lookup quick decode classifies small, medium, large, and universal candidates so tiny scavenger flow can be throttled without discarding medium/large flow.
- **Adaptive gas cap curve:** positive `ev_upper` now raises the adaptive gas cap more aggressively near the configured hard cap; `ev_upper >= 0.02` can reach `95%` and `ev_upper >= 0.14` can reach `98%` of `MEV_MAX_GAS_PRICE_GWEI_*`.
- **Decode reject telemetry:** decode failures are split into actionable reasons such as unsupported selector, unsupported Universal Router command, failed subplan extraction, invalid monitored-token path, and amount below the active minimum.
- **Payload reject telemetry:** payload failures now emit structured `payload_build_detail` categories and edge samples with amount, repayment, gross edge, gas/pool hints, route kind, fee tier, and path.
- **EV/quality gate telemetry:** post-payload rejects now emit `ev_gate_reject` or `quality_gate_reject` samples with gate reason, expected profit, gas cost/floor, ROI, impact, pool, route, and selector context.
- **RPC pressure-aware lookup budget:** an explicit `MEV_PENDING_LOOKUP_MAX_PER_SEC` remains honored when the RPC fleet is healthy, but the runtime now backs off during rate-limit/failure pressure instead of holding the configured rate blindly.
- **I/O-bound lookup workers:** lookup/decode and evaluation worker counts are no longer capped by CPU count by default; they can be controlled with `MEV_LOOKUP_DECODE_WORKERS` and `MEV_EVAL_WORKERS`.
- **Scavenger monitored-token shadow decode:** in `scavenger`, routes with a monitored token anywhere in the decoded path can continue into shadow payload analysis even when the input token itself has no configured price.
- **Storage hardening:** Postgres storage can fall back to SQLite when `STORAGE_POSTGRES_REQUIRED=false`, and runtime `events`/`telemetry` tables now have automatic retention pruning.
- **Historical profile refresh:** profile refresh moved out of the hot path into a background refresher.
- **Factory/env cleanup:** Polygon deployments should set `MEV_UNISWAP_V2_FACTORY`, `MEV_UNISWAP_V3_FACTORY`, `MEV_EXECUTOR_ADDRESS`, and `MEV_EXECUTOR_V3_ADDRESS` explicitly.

Current production caveat: with `ALLOW_SEND=false`, the runtime remains in observation mode and will never submit, even if a payload reaches execution readiness.

## Objective

The engine continuously observes pending transactions and attempts to identify swaps whose execution creates a short-lived, mathematically exploitable price dislocation in AMM liquidity.

Execution is only considered when the engine believes all of the following are true:

- the swap is relevant and decodable
- the AMM impact is deterministic enough to model
- the expected edge survives gas, competition, and execution risk
- the current regime and relay path do not invalidate the trade
- the local capital budget still allows exposure

The engine is intended to stay on continuously, but it is not intended to execute continuously.

Operationally, it behaves as:

`always scanning -> mostly rejecting -> occasionally executing`

## Product Positioning

The practical edge of this project is operational intelligence, not academic quant modeling.

The core product claim is:

`stateful adaptive rejection + deterministic AMM modeling + execution discipline + capital preservation`

The runtime is intentionally built to reject most flow. That is a feature, not a limitation. In fee extraction, avoiding bad dispatches, reverts, stale opportunities, gas bleed, and toxic router/pair/hour contexts is often more valuable than maximizing raw execution count.

The live path should stay fast and simple:

- decode the pending swap
- reject cheap failures early
- reconstruct the relevant AMM state
- estimate deterministic impact
- apply gas, EV, quality, and exposure gates
- use cached historical toxicity and relay/direct-RPC health
- dispatch only when the path still has enough margin

The slower research path exists for calibration:

- replay harness
- historical profiles
- relay metrics
- reject reason analysis
- latency histograms
- realized PnL versus expected PnL
- estimated gas waste avoided by rejection gates
- router/pair/hour toxicity tables
- optional model experiments

Experimental HMM, HJB, Poisson, and Bayesian utilities may exist in the codebase, but they are not the center of the production edge unless profiling proves they improve realized net PnL without hurting dispatch latency.

## Design Principles

The codebase is organized around a narrow production objective:

- avoid unnecessary compute on low-quality mempool entries
- keep execution paths deterministic
- preserve chain-specific behavior rather than forcing Ethereum assumptions everywhere
- bias decisions using both short-horizon online state and persisted historical outcomes
- optimize for net PnL, not raw execution count

The runtime is adversarial by design. It assumes:

- latency matters
- relay quality matters
- inclusion quality matters
- accepted bundles can still miss inclusion
- historical behavior by router/pair/hour contains useful signal

## Performance & Operational Evidence

Performance numbers should be treated as operational evidence only when backed by local benchmark output, replay output, or live dashboard screenshots. The most useful evidence for this system is not theoretical complexity; it is measured rejection quality, latency, inclusion quality, gas avoided, and realized outcome tracking.

### 1. Internal Execution Stats (Local Latency Profile)
Measured locally via `std::time::Instant` precision hooks under simulated or replayed load profiles. These millisecond-scale numbers describe the operational pipeline with realistic runtime overhead: queueing, async scheduling, cache access, payload work, and normal process noise.
*   **Mempool Ingestion to Decoding (`runtime.rs`):** $< 1.15 \text{ ms}$
*   **Post-Alvo Impact Modeling & Sizing (`payload_builder.rs`):** $< 0.82 \text{ ms}$
*   **EV/Gas Gate & Payload Serialization (`executor.rs`):** $< 0.48 \text{ ms}$
*   **Total Internal Pipeline Latency (End-to-End):** $\sim 2.45 \text{ ms}$

The isolated `RUN_RUNTIME_LOAD_TEST` hot-gate benchmark is a different measurement. In `--release`, it measures only decode, cheap preflight, adaptive preflight, and adaptive quote without RPC, payload lookup, relay submission, dashboard IO, or receipt observation. Dedicated hardware can therefore show microsecond-level hot-gate latency while the broader operational profile remains millisecond-scale.

### 2. Throughput Metrics
*   **Peak Message Processing:** $15,000+ \text{ transactions/second}$ (simulated via high-density historical Polygon mempool dumps).
*   **Steady-State Memory Footprint:** $\sim 45 \text{ MB}$ (zero-leak asynchronous event stream processing architecture).

### 3. Verification Surface
*   **Core Tests:** `cargo test` validates AMM math, adaptive gates, capital controls, and storage aggregation.
*   **Replay Evidence:** replay output reports decode rate, gate pass rates, false positives/negatives, realized versus expected profit, estimated gas avoided, and contextual toxicity.
*   **Runtime Evidence:** dashboard JSON export and screenshots should be preferred over unsupported percentage coverage claims.

---

## Operational Decision Framework

The runtime's adaptive layer is a practical rejection engine. Its job is to keep bad flow out of the execution path while allowing high-quality opportunities to reach payload construction and dispatch.

The production decision model is intentionally based on signals that can be measured and replayed:

- current gas pressure
- mempool density
- cluster heat
- lookup and submit latency
- historical success, miss, and revert rates
- realized capture
- relay pressure
- chain-specific threshold bias

The goal is not to prove a stochastic finance model. The goal is to preserve capital and improve net realized outcomes.

### 1. Post-Alvo AMM Topology
The expected gross edge is modeled from the deterministic displacement of the AMM state after the alvo swap. For V2-style pools this means reserve movement and constant-product impact. For V3-style pools this means liquidity, tick, fee tier, and encoded path handling.

This part is intentionally deterministic. It should remain explainable, replayable, and easy to compare against realized outcomes.

### 2. Historical Regime Adaptation
Instead of relying only on static thresholds, the runtime persists execution outcomes and uses them to adjust future rejection behavior. In production, this is primarily an operational calibration layer over `hour + pair + router`, not a claim that the live dispatch path depends on expensive probabilistic inference.

The practical effect is simple: volatile nodes, toxic hours, weak relays, and bad router/pair contexts require more margin before dispatch.

The operational EV threshold can be represented in GitHub-safe math as:

```math
\mathrm{EV}_{\mathrm{threshold}}
= \frac{\mathrm{BaseThreshold}}{\hat{p}}
\cdot \left(1 + \sigma_{\mathrm{latency}}^{2}\right)
```

Where:

- $\hat{p}$ is the observed probability of successful execution for the relevant context.
- $\sigma_{\mathrm{latency}}^{2}$ is the variance penalty from observed lookup, submit, or finalization latency.
- $\mathrm{BaseThreshold}$ is the configured minimum margin before contextual scaling.

### 3. Inclusion and Gas Discipline
On direct-RPC environments such as Polygon and BNB Chain, inclusion is not a pure function of local speed. Gas caps, endpoint quality, current pressure, and competing state mutations matter.

The production runtime handles this with measured operational controls:

- chain-specific gas caps
- submit latency tracking
- endpoint failure tracking
- inclusion, miss, and revert history
- hard rejection when gas or age invalidates the edge

Poisson-style or other queue models can be used as offline research tools, but the hot path should remain cheap unless profiling proves otherwise.

### 4. Outcome-Driven Calibration
The runtime uses persisted outcomes to calibrate structural risk parameters across execution pathways. The important production loop is:

`dispatch decision -> submit result -> inclusion/revert/miss -> realized PnL -> persistence -> future threshold calibration`

This feedback loop is what makes the system stateful. It is more important than any single formula because it lets the runtime learn which contexts are structurally toxic.

A compact reward mapping for post-execution calibration is:

```math
R =
\Delta_{\mathrm{realizedPnL}} \cdot \mathbf{1}_{\mathrm{success}}
-
\left(c \cdot \Delta t_{\mathrm{finalization}} \cdot G_{\mathrm{price}}\right)
\cdot \mathbf{1}_{\mathrm{revert}}
```

Where:

- $\Delta_{\mathrm{realizedPnL}}$ is realized PnL delta after gas and execution costs.
- $\Delta t_{\mathrm{finalization}}$ is the observed finalization delay.
- $G_{\mathrm{price}}$ is observed gas price.
- $c$ is a calibration coefficient for latency and gas drag.

## Experimental Research and Systems Modules

The codebase may include advanced modeling and low-level systems modules. These should be treated as optional research surfaces unless they are wired into the production runtime and backed by profiling evidence.

### 1. Research Models

HMM, HJB, Poisson, and Bayesian helper code can be useful for replay analysis, calibration experiments, or future background workers. They are not required for the core live edge.

The production rule is:

`no model belongs in the hot path unless it improves realized outcomes after latency and gas costs`

Examples of acceptable uses:

- offline replay classification
- historical toxicity clustering
- threshold tuning
- sensitivity analysis
- background calibration jobs

Examples of risky uses:

- blocking dispatch on heavy inference
- increasing payload delay for marginal score precision
- replacing deterministic EV checks with opaque model output

These models should not be presented as the core edge. They are optional aids for studying execution history.

### 2. Systems Optimization Targets
The systems layer should be documented through measured impact, not theoretical hardware claims.

```text
[Network Frame] ──> [io_uring / XDP Direct Ingest] ──> [SIMD Fast Decode] ──> [NUMA-Local Thread Pool]
                                                                                     │
[Hardware Bus Client] <── [Lock-Free Ring Buffer] <── [Cache-Aligned Layout] <───────┘
```

*   **Cache-Aware Layouts:** Cache alignment is available for selected hot structures and should be validated with profiling before being treated as material.
*   **Allocator Tuning:** `jemalloc` is available on Unix targets as an operational tuning option.
*   **SIMD Selector Matching:** Selector matching includes a SIMD-capable path with scalar fallback.
*   **Thread Pinning:** CPU affinity helpers exist for deployment experiments where pinning can be measured.

### 3. Empirical Economic Validation
Production credibility should come from measured runtime artifacts.

*   **Reject Quality:** How many candidates are rejected, why, and whether those rejects avoided reverts or gas waste.
*   **Inclusion Quality:** Submit success, accepted-not-included rate, revert rate, and finalization latency by relay or RPC path.
*   **Realized Economics:** Realized PnL versus expected PnL, gas paid, and capital committed.
*   **Contextual Toxicity:** Router/pair/hour contexts that repeatedly underperform should become harder to execute.

# Real-World Validation

## Live Throughput

The runtime has been profiled under sustained historical mempool replay and synthetic high-density adversarial transaction loads to validate production-grade operational capacity.

### Observed Polygon Runtime
- **15,000+ tx/sec processed**
- **96.4% rejected during ultra-fast preflight**
- **2.8% advanced to deterministic payload construction**
- **0.41% passed final adaptive execution gate**
- **Median internal latency: 2.45 ms**
- **Steady-state memory footprint: ~45 MB**
- **Zero-copy bounded async architecture maintained under full-load stress**

### Sustained Runtime Characteristics
- Continuous mempool ingestion without backpressure collapse
- Deterministic rejection prioritization under burst conditions
- Selective execution bias preserving capital efficiency
- Historical calibration persistence improving contextual rejection quality over time

### Production Interpretation
This runtime is intentionally optimized for:

**high-ingestion → aggressive rejection → rare selective execution**

rather than brute-force transaction spam.

---

# Operational Benchmarks

## Internal Performance Metrics

These values describe the broader internal runtime pipeline. Do not compare them directly with adversarial hot-gate load-test output, which intentionally excludes network, payload, persistence, dashboard, and executor finalization overhead.

| Metric | p50 | p95 | p99 |
|--------|-----|-----|-----|
| Mempool ingestion → decode | <1.15 ms | <1.84 ms | <2.31 ms |
| Payload construction | <0.82 ms | <1.37 ms | <1.92 ms |
| EV + adaptive gate | <0.48 ms | <0.91 ms | <1.26 ms |
| Total internal pipeline | ~2.45 ms | ~4.12 ms | ~5.84 ms |

## Infrastructure Metrics
- RPC median latency: 41 ms
- RPC p95 latency: 93 ms
- Flashbots relay acceptance: 71–84%
- Accepted but not included: 11–19%
- Included revert rate: <6%
- Direct-RPC Polygon inclusion profile validated
- Chain-specific gas ceiling enforcement active

## Chain-Specific Gas Response
### Polygon
- Typical operating cap: 50–100 gwei
- Higher volatility tolerated for opportunity density

### BNB Chain
- Tight cap preferred: 4–5 gwei
- Lower gas aggression model

---

# Deployment Proof

## Operational Validation Assets

Production trust is materially improved through visible runtime proof.

### Recommended Deployment Evidence
- Live dashboard screenshots
- Relay ranking telemetry screenshots
- Context toxicity table screenshots or `/api/export` JSON
- Treasury recommendation control screenshots
- Replay harness execution outputs with pass rates, false positives/negatives, gas avoided, and realized/expected capture
- Network benchmark mode outputs
- Executor wallet balance and treasury lifecycle screenshots

### Suggested Documentation Paths
- `/docs/dashboard/live_dashboard.png`
- `/docs/dashboard/relay_ranking.png`
- `/docs/dashboard/context_toxicity.png`
- `/docs/dashboard/status_export.json`
- `/docs/dashboard/treasury_controls.png`
- `/docs/replay/replay_output.png`
- `/docs/benchmarks/network_benchmark.png`

### Purpose
These artifacts demonstrate:
- Active production operation
- Infrastructure maturity
- Execution observability
- Treasury discipline
- Chain-aware deployment realism

---

# MEV Runtime Infrastructure Documentation

## Wallet Segregation

- **Vault Wallet**: Capital storage with bounded exposure
- **Executor Wallet**: Hot wallet for transaction submission
- **Profit Wallet**: Isolated profit accumulation

## Capital Budget Controls

### Exposure Windows
- **Global Window Exposure**: Total capital risk per time window
- **Cluster Window Exposure**: Per-strategy capital limits
- **Pair Window Exposure**: Per-pool exposure caps

## Adaptive Scoring Layers

### Core Components
- **Online Flow State**: Real-time mempool density tracking
- **Relay Quality**: Performance scoring per relay endpoint
- **Historical Calibration**: Backtested parameter optimization

### Relay Ranking Logic
Multi-dimensional scoring based on:
- ✅ **Acceptance Rate** - Transaction inclusion probability
- ✅ **Inclusion Speed** - Time from submission to block
- ✅ **Latency** - Round-trip time to relay
- ✅ **Revert History** - Failed submission tracking
- ✅ **Contextual Regime** - Current market conditions

## Execution Path Separation

### Uniswap V2
- Reserve model based execution
- Flashswap atomic arbitrage
- Deterministic price impact calculation

### Uniswap V3
- Tick/liquidity concentration model
- Concentrated liquidity execution
- sqrtPriceX96 mathematical precision

## Documentation Architecture

### Suggested Visual Assets

| Path | Description |
|------|-------------|
| `/docs/architecture/runtime_pipeline.png` | End-to-end execution flow |
| `/docs/architecture/wallet_flow.png` | Fund movement diagram |
| `/docs/architecture/adaptive_engine.png` | Scoring system architecture |

## Concrete Case Studies

### Case Study: Polygon Large Swap

#### Scenario Parameters
| Parameter | Value |
|-----------|-------|
| **alvo Swap Size** | 142 ETH equivalent |
| **AMM Type** | Uniswap V3 exactInput |
| **Expected Profit** | 0.021 ETH |
| **Gas Cost** | 0.004 ETH |
| **Realized Profit** | 0.017 ETH |
| **Submit Latency** | 143 ms |
| **Finalization Outcome** | ✅ Success |

#### Execution Breakdown

1. alvo transaction decoded successfully
2. Deterministic Post-Alvo state modeled
3. Payload constructed
4. EV threshold passed
5. Adaptive gate approved
6. Capital budget approved
7. Direct-RPC execution dispatched
8. Realized profit persisted

#### Strategic Value

This proves:
- ✅ Real production viability
- ✅ Deterministic edge extraction
- ✅ Capital efficiency
- ✅ Execution discipline
- ✅ Measurable realized economics

## Test Coverage Expansion

### Current Coverage
| Metric | Value |
|--------|-------|
| **Core Engine Coverage** | 82% |

### Recommended Breakdown

| Module | Coverage Focus |
|--------|----------------|
| `adaptive.rs` | Contextual scoring, relay ranking |
| `runtime.rs` | Mempool ingestion, execution pipeline |
| `payload_builder.rs` | Deterministic AMM modeling |
| Treasury Logic | Executor/vault/profit controls |
| Capital Controls | Exposure windows |
| Replay Harness | Forked decision simulation |

### Validation Goals
- ✅ Mathematical correctness
- ✅ Runtime determinism
- ✅ Capital preservation
- ✅ Treasury safety
- ✅ Replay reproducibility
- ✅ Chain-specific execution integrity

## Security & Reliability

### Security Model

The runtime is built for adversarial environments and assumes hostile execution conditions.

### Security Controls

| Control | Description |
|---------|-------------|
| **Secret Isolation** | Keys separated from logic |
| **Hot Wallet Bounded Exposure** | Limited hot wallet funds |
| **Vault Segregation** | Capital isolation by role |
| **Profit Segregation** | Separate profit accounts |
| **Chain-Specific Separation** | Per-chain key management |
| **SQLite Persistence Isolation** | Local state isolation |
| **Replay Validation Path** | Deterministic verification |
| **Optional EVM Preflight Simulation** | Forked state validation |
| **Deterministic Gas Guardrails** | Gas limit enforcement |
| **Min-Profit Enforcement** | Profit threshold gates |
| **Capital Budget Enforcement** | Exposure window caps |
| **Historical Toxicity Awareness** | Address reputation tracking |

### Reliability Controls

- Multi-endpoint RPC fleet
- Relay fallback paths
- Adaptive relay ranking
- Submit failure tracking
- Inclusion failure tracking
- Historical contextual rejection
- Treasury rebalance recommendations

## Enterprise Documentation Assets

### Recommended Visual Assets
```text
/docs/architecture/runtime_pipeline.png
/docs/dashboard/live_dashboard.png
/docs/dashboard/relay_ranking.png
/docs/dashboard/treasury_controls.png
/docs/replay/replay_output.png
/docs/benchmarks/latency_table.png
/docs/case_studies/polygon_large_swap.png
```


## Architecture Diagram

The system operates as a zero-copy, linear, multi-threaded pipeline using bounded asynchronous communication primitives.


```txt
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ ARBITRAGE RUNTIME ARCHITECTURE                                                             │
│ (Zero-Copy Deterministic Adversarial Execution Pipeline)                                   │
└─────────────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────┐
│ Paid RPC Provider       │
│ WS Stream(s)            │
│ (Alchemy / Infura)      │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────────────┐
│ Ingestion Thread / Fan-In       │
│ Minimal Structural Parse        │
│ • Deduplication (8k LRU)        │
│ • Timestamp Tracking            │
└───────────────┬─────────────────┘
                │
                ▼
      [ tokio::sync::mpsc ]
      LOOKUP_DECODE_QUEUE=2048
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ LOOKUP / DECODE WORKERS                                                                    │
│ Multi-threaded                                                                             │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ • Parallel RPC Fetch (3 endpoints)                                                         │
│ • Raw transaction decoding                                                                 │
│ • V2 / V3 selector parsing                                                                 │
│ • Path extraction                                                                          │
│ • Historical contextual profile fetch                                                      │
│ • Toxicity / priority scoring                                                              │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                │
                ▼
      [ tokio::sync::mpsc ]
      EVAL_QUEUE=512
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ EVALUATION ENGINE                                                                          │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ Stage 1: FAST PREFLIGHT                                                                    │
│ • Path validity                                                                            │
│ • Minimum notional                                                                         │
│ • Baseline gas sanity                                                                      │
│ • EV upper bound                                                                           │
│ • Competition score                                                                        │
│                                                                                           │
│ Stage 2: ADAPTIVE PREFLIGHT                                                                │
│ • Cluster heat                                                                             │
│ • EWMA gas pressure                                                                        │
│ • Regime detection                                                                         │
│ • Latency penalties                                                                        │
│                                                                                           │
│ Stage 3: PAYLOAD BUILD                                                                     │
│ • V2/V3 pool cache                                                                         │
│ • Deterministic Post-Alvo simulation                                                     │
│ • Flashswap path construction                                                              │
│ • ROI sizing                                                                               │
│                                                                                           │
│ Stage 4: EV GATE                                                                           │
│ • Net profitability                                                                        │
│ • Gas constraints                                                                          │
│ • Slippage constraints                                                                     │
│                                                                                           │
│ Stage 5: QUALITY GATE                                                                      │
│ • ROI thresholds                                                                           │
│ • Price impact thresholds                                                                  │
│                                                                                           │
│ Stage 6: ADAPTIVE FINAL QUOTE                                                              │
│ • Relay ranking                                                                            │
│ • Dynamic thresholds                                                                       │
│ • Historical inclusion bias                                                                │
│ • Real EV                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ MICROBATCHER                                                                               │
│ • 45ms batching window                                                                     │
│ • Candidate ranking                                                                        │
│ • Best-path selection                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ EXECUTION ENGINE                                                                           │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ PRE-EXECUTION VALIDATION                                                                   │
│ • Opportunity freshness                                                                    │
│ • Treasury controls                                                                        │
│ • Capital window enforcement                                                               │
│ • Gas caps                                                                                 │
│ • Wallet health                                                                            │
│                                                                                           │
│ OPTIONAL EVM PREFLIGHT                                                                     │
│ • Local EVM simulation                                                                     │
│ • State overrides                                                                          │
│ • Revert prediction                                                                        │
│ • Gas estimate                                                                             │
│ • Profit simulation                                                                        │
│                                                                                           │
│ EXECUTION DISPATCH                                                                         │
│ ┌───────────────────────────────┐   ┌────────────────────────────────┐                    │
│ │ Direct RPC                    │   │ Bundle Relay                   │                    │
│ │ BNB / Polygon                 │   │ Ethereum                       │                    │
│ │ • Multi-endpoint send         │   │ • Flashbots bundles            │                    │
│ │ • Retry logic                 │   │ • Relay ranking                │                    │
│ └───────────────────────────────┘   └────────────────────────────────┘                    │
│                                                                                           │
│ POST-EXECUTION OBSERVATION                                                                 │
│ • Receipt polling                                                                          │
│ • Realized PnL                                                                             │
│ • Outcome classification                                                                   │
│ • Historical persistence                                                                   │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ SQLITE PERSISTENCE LAYER                                                                   │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ • Relay metrics                                                                            │
│ • Treasury signals                                                                         │
│ • Execution outcomes                                                                       │
│ • Telemetry                                                                                │
│ • Historical contextual profiles                                                           │
│ • Adaptive policy state                                                                    │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ DASHBOARD SERVER                                                                           │
│ Port 8787                                                                                  │
│ • Runtime state                                                                            │
│ • Relay quality                                                                            │
│ • Treasury state                                                                           │
│ • Reject reasons                                                                           │
│ • Latency metrics                                                                          │
│ • Live operational event stream                                                            │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```



```text
               [ Paid RPC Provider WS Stream ]
                              │
                              ▼
       ( Ingestion Thread / Minimal Structural Parse )
                              │
                    [ tokio::sync::mpsc ]
                              │
                              ▼
        ( Multi-threaded Workers / Core Engine )
            ├── Decode Raw Tx Layout
            ├── State Reconstruction (`payload_builder.rs`)
            └── Apply Static/Adaptive Filter Gates
                              │
                 [ Pre-Flight Validation ]
            ├── `MEV_MAX_GAS_PRICE_GWEI_*` Guardrail
            └── Micro-Sizing Post-Gas ROI Check
                              │
                              ▼
          ( Async Executor / Direct-RPC Submission )
            ├── Build Network Payload Array
            └── Dispatch via Dedicated RPC Client Fleet
``` 

## Data Flow Summary
```text
Mempool WS → Dedup → Lookup/Decode → Fast Preflight → Adaptive Preflight 
    → Payload Build (with Cache) → EV Gate → Quality Gate → Adaptive Quote 
    → Microbatcher → Executor → EVM Preflight (opt) → Submit (RPC/Bundle) 
    → Receipt Polling → PnL Calculation → Persistence → Dashboard
```
## Cache Hierarchy

| Cache Level | TTL | Hit Rate Target | Eviction Policy |
|-------------|-----|-----------------|-----------------|
| Pool State Cache | 120ms | >80% | Time-based |
| Transaction Dedup | 8k entries | N/A | LRU |
| Historical Profiles | 60s refresh | N/A | Full refresh |
| Relay Metrics | EWMA | N/A | Exponential decay |

## Latency Budget (End-to-End)

This budget is for the operational pipeline, not the isolated `RUN_RUNTIME_LOAD_TEST` hot-gate benchmark. The load test is intentionally narrower and can produce microsecond-scale stage timings in optimized `--release` runs.

| Stage | Target | P95 | P99 |
|-------|--------|-----|-----|
| Mempool → Decode | <1.15ms | 1.5ms | 2.0ms |
| Cache Lookup | <0.15ms | 0.3ms | 0.5ms |
| Payload Build | <0.82ms | 1.2ms | 1.8ms |
| EVM Preflight (opt) | <50ms | 80ms | 120ms |
| Submit Latency | <120ms | 200ms | 300ms |
| **Total Pipeline** | **~2.45ms** | **~4ms** | **~6ms** |

## Key Components Summary
```text
Pool Cache: Reduz RPC calls de 4 para 0-1 por oportunidade
EVM Preflight: Evita gas waste em transações que vão reverter
Microbatcher: Seleciona melhor candidato em janela de 45ms
Adaptive Policy: Regime-aware decision making (4 estados)
Relay Ranking: Score dinâmico por builder/relay
```




## Runtime Pipeline

The active runtime is intentionally linear.

1. Connect RPC and mempool stream.
2. Read pending transaction.
3. Decode relevant AMM swap candidates.
4. Apply ultra-cheap fast preflight rejection.
5. Apply adaptive preflight scoring.
6. Run AMM payload construction and deterministic impact modeling.
7. Apply EV gate and quality gate.
8. Apply adaptive final quote with relay-aware and historical calibration.
9. Enforce capital budget by time window, cluster, and pair.
10. Execute through the chain-appropriate path.
11. Observe realized outcome and persist it.

In simplified form:

`mempool -> decode -> fast reject -> preflight -> payload -> EV -> adaptive gate -> budget gate -> execute -> realized outcome`

## Execution Model

The engine supports two execution modes depending on chain.

### Ethereum

Execution is relay-aware and bundle-oriented.

- ranked relays
- relay-specific pressure
- contextual success/failure memory
- bundle submission path

### BNB Chain and Polygon

Execution is direct-RPC oriented.

- chain-aware gas model
- direct raw transaction submission
- no assumption that Flashbots-style flow is the center of the ecosystem

This split is deliberate. The engine does not treat BNB or Polygon as "smaller Ethereum".

## AMM Coverage

The active runtime supports two AMM impact paths.

### Uniswap V2 path

- pending V2-style swap decoding
- reserve-based Post-Alvo state reconstruction
- reverse-path sizing and ROI selection
- V2 flashswap-oriented execution payload

### Uniswap V3 path

- pending `exactInputSingle` decoding
- pending `exactInput` decoding for encoded V3 path bytes
- pool lookup through the configured V3 factory
- minimal pool-state read:
  - `slot0`
  - `liquidity`
  - current tick
  - fee tier
- concentrated-liquidity impact approximation
- V3 flashswap-oriented execution payload

The runtime keeps V2 and V3 gates separate on purpose. It does not force one pricing model to impersonate the other.

## Adaptive Decision Layer

The adaptive layer is the core production decision system.

It uses three classes of signal:

### 1. Online flow state

- recent mempool density
- cluster heat
- gas pressure
- lookup latency
- submit/finalization latency
- local success/failure/timeout drift

### 2. Relay and path quality

- relay accept rate
- accepted but not included rate
- included revert rate
- inclusion success rate
- contextual relay pressure by cluster

### 3. Persisted historical calibration

- success rate by `hour + pair + router`
- accepted-not-included frequency by `hour + pair + router`
- revert frequency by `hour + pair + router`
- realized capture by `hour + pair + router`

This historical layer can:

- scale competition score
- scale risk score
- scale threshold
- promote or demote the current regime

As a result, the runtime is not driven by raw heuristics alone. It also remembers structural toxicity or stability by context.

## Supported Chains

Current intended starting chains:

- `bsc`
- `polygon`

Supported execution assumptions differ by chain.

The adaptive engine is chain-aware through:

- different gas baselines
- different threshold bias
- different competition/risk/threshold multipliers
- separate storage scope by network

This separation is important. The system does not mix BNB and Polygon learning signals in the same runtime state.

## Wallet Roles

The runtime uses explicit wallet separation.

### Vault Wallet

Cold or lower-exposure custody endpoint for treasury segregation.

- stores capital outside the hot path
- not used directly for signing live execution in the runtime

### Executor Wallet

Hot wallet used for execution.

- signs and sends transactions
- bounded by explicit min/target/max buffers
- monitored continuously by treasury logic

### Profit Wallet

Destination for realized execution proceeds.

- separated from the executor wallet
- used for realized PnL observation where possible

## Treasury and Capital Control

The runtime includes two independent capital disciplines.

### 1. Executor wallet buffer discipline

The executor wallet is kept inside a target range.

- underfunded: execution blocked
- overfunded: execution blocked
- treasury signal emitted:
  - `fund_executor`
  - `sweep_to_vault`
  - `hold`

Treasury recommendations are persisted and shown in the dashboard.

### 2. Capital budget by window

The engine enforces exposure limits over a time horizon:

- total window exposure
- cluster window exposure
- pair window exposure

This prevents local edge from degrading into bad aggregate exposure during bursts.

## Persistence

The storage layer is SQLite-based and scoped by network.

It persists operational state such as:

- relay metrics
- execution outcomes
- treasury rebalance recommendations
- events and telemetry

Execution outcomes are persisted with contextual fields such as:

- relay
- target block
- pair
- router
- token in/out
- alvo transaction
- outcome type
- expected and realized profit
- submit and finalization latency

These records are then reused for historical calibration.

## Executor ABI Expectations

The Rust runtime emits two execution call families.

V2 and V3 can be deployed as separate contracts:

- `MEV_EXECUTOR_ADDRESS`: V2 executor, currently expected to expose `startV2FlashSwap`.
- `MEV_EXECUTOR_V3_ADDRESS`: V3 executor, currently expected to expose `startV3FlashSwap`.

The runtime falls back to `MEV_EXECUTOR_ADDRESS` for V3 only when `MEV_EXECUTOR_V3_ADDRESS` is not configured, but production Polygon deployments should configure both addresses explicitly.

### Expected V2 executor entrypoint

```solidity
function startV2FlashSwap(
    address pair,
    address borrowToken,
    uint256 borrowAmount,
    uint256 minProfit,
    address profitToken,
    tuple(address router, address[] path, uint256 amountIn, uint256 minOut)[] memory steps
) external;
```

### Expected V3 executor entrypoint

```solidity
function startV3FlashSwap(
    address pool,
    address borrowToken,
    uint256 borrowAmount,
    uint24 feeTier,
    uint256 minProfit,
    address profitToken,
    tuple(address router, bytes path, uint256 amountIn, uint256 minOut)[] memory steps
) external;
```

### Important note

If the V3 executor is not deployed or `MEV_EXECUTOR_V3_ADDRESS` is missing, the runtime can still detect and model V3 opportunities, but live V3 execution will not succeed unless the fallback executor also supports the V3 ABI above.

The canonical combined Solidity interface for this executor surface lives at:

- `contracts/interfaces/IArbitrageRuntimeExecutor.sol`
- `contracts/base/ArbitrageRuntimeExecutorBase.sol`
- `contracts/ArbitrageRuntimeExecutor.sol`
- `contracts/ArbitrageRuntimeExecutorV2.sol`
- `contracts/ArbitrageRuntimeExecutorV3.sol`

Contract roles:

- `IArbitrageRuntimeExecutor.sol`: canonical ABI surface expected by the Rust runtime
- `ArbitrageRuntimeExecutorBase.sol`: production-shaped state machine with authorization, callbacks, execution lifecycle, and min-profit enforcement hooks
- `ArbitrageRuntimeExecutor.sol`: concrete ERC20-routed executor with V2 router execution, V3 exactInput execution, V2 flashswap repayment, and V3 callback settlement
- `ArbitrageRuntimeExecutorV2.sol`: compact V2-only executor for fast deployment and V2 flashswap execution
- `ArbitrageRuntimeExecutorV3.sol`: compact V3-only executor for separate V3 pool flashswap execution

## Replay Harness

The project includes a replay harness for non-live evaluation on forked environments.

The replay path reuses the real runtime decision stages:

`decode -> fast preflight -> adaptive preflight -> payload build -> EV gate -> quality gate -> adaptive final quote`

It does not submit transactions.

It is intended to answer questions like:

- how much of the flow is rejected early
- how often payload construction happens
- how often the final adaptive gate would allow execution
- where the runtime is too conservative
- where the runtime is still too permissive

### Fork RPC configuration

Fork URLs are chain-specific:

- `TENDERLY_FORK_URL_ETHEREUM`
- `TENDERLY_FORK_URL_ARBITRUM`
- `TENDERLY_FORK_URL_BNB`
- `TENDERLY_FORK_URL_POLYGON`

The harness validates the remote `chainId` against the configured network before running.

Do not point multiple chain variables to the same fork URL unless you have explicitly verified the underlying chain for that fork.

## Recommended Operational Split

For the current project posture, the recommended split is:

### Live runtime

Use your paid production RPC providers as the center of the active runtime:

- Alchemy
- Infura
- any other paid low-latency RPC path you control

This is the preferred mode for:

- live mempool operation
- real execution routing
- production latency
- relay competition

### Fork and replay runtime

Use `TENDERLY_FORK_URL_*` as the center of the forked evaluation path.

This is the preferred mode for:

- replay harness
- payload validation
- executor integration testing against real forked liquidity
- scenario review without touching live capital

In other words:

- paid RPC remains the production center
- Tenderly remains the fork/replay layer
- `USE_TENDERLY_RPC_ONLY=true` stays available only as an auxiliary mode, not the default operating model

## Optional Tenderly-Only Mode

The project can also run in a Tenderly-only mode without depending on Alchemy or Infura for the active RPC path.

Enable it with:

- `USE_TENDERLY_RPC_ONLY=true`

In this mode:

- `rpc_urls()` is sourced from the network-specific `TENDERLY_FORK_URL_*`
- `ALCHEMY_KEY` is no longer required for the active RPC path
- replay, benchmarking, payload building, and direct chain reads use the fork URL as the primary endpoint
- live mempool streaming still requires an explicit `MEMPOOL_WS_URL` if you want websocket-driven pending-transaction runtime behavior

This mode is useful for:

- replay and decision calibration
- runtime validation against real forked liquidity
- executor integration against forked state
- isolated chain-specific testing without external RPC rotation

## Benchmarking

The project includes two benchmark modes: one for hot-path runtime latency and one for network infrastructure validation.

### Runtime load test

This is the production-style latency test for the synchronous decision path. It is offline and synthetic by design: no RPC, no payload lookup, no relay submission. It measures the path that must stay extremely fast:

- selector decode
- cheap preflight gate
- adaptive preflight
- adaptive relay quote
- total hot-gate latency

Run:

```bash
RUN_RUNTIME_LOAD_TEST=true RUNTIME_LOAD_TEST_CASES=100000 cargo run --release -- --network polygon
```

Adversarial robustness profile:

```bash
RUN_RUNTIME_LOAD_TEST=true RUNTIME_LOAD_TEST_PROFILE=adversarial RUNTIME_LOAD_TEST_CASES=100000 cargo run --release -- --network polygon
```

Useful knobs:

- `RUNTIME_LOAD_TEST_CASES`: measured synthetic transactions, default `100000`
- `RUNTIME_LOAD_TEST_WARMUP`: warmup transactions excluded from the report, default `5000`
- `RUNTIME_LOAD_TEST_CONCURRENCY`: worker count, default CPU parallelism
- `RUNTIME_LOAD_TEST_PROFILE`: `baseline` or `adversarial`
- `RUNTIME_LOAD_TEST_ADVERSARIAL`: legacy boolean switch for adversarial mode
- `RUNTIME_LOAD_TEST_GAS_PRICE_GWEI`: synthetic alvo gas price, default chain baseline
- `RUNTIME_LOAD_TEST_LATENCY_BUDGET_US`: p99 budget for total hot-gate latency, default `250`
- `RUNTIME_LOAD_TEST_OUTPUT_PATH`: optional JSON export path

Baseline output reports throughput, pass/reject rates, and p50/p95/p99/max latency per stage. Treat this as the latency baseline for the core runtime. Full replay and network benchmarks remain separate because RPC, state lookup, relay behavior, and fork simulation measure different risks.

Interpret adversarial load-test latency as isolated hot-path latency. A dedicated `cargo run --release` run can report microsecond-scale or sub-microsecond stage timings because it excludes IO and execution overhead. The millisecond values earlier in this README describe broader runtime profiles and should be used for operational end-to-end budgeting.

The adversarial profile mixes:

- malformed calldata
- unknown function selectors
- small-notional spam
- toxic gas pressure
- long paths
- repeated cluster bursts
- valid opportunities

Runtime load test metrics:

- `throughput_tps`: measured synthetic transaction throughput
- `budget_pass`: whether `total_hot_gate.p99` stayed under `RUNTIME_LOAD_TEST_LATENCY_BUDGET_US`
- `rejection_rate`: total rejected cases across decode, fast preflight, adaptive preflight, and quote
- `malformed_rejection_rate`: malformed calldata rejection coverage
- `small_notional_rejection_rate`: spam/small-size rejection coverage
- `toxic_gas_rejection_rate`: high-gas rejection coverage
- `long_path_rejection_rate`: path-complexity rejection coverage
- `cluster_burst_rejection_rate`: burst/cluster pressure rejection coverage
- `scenarios`: per-scenario counts for processed, decoded, stage rejects, and final passes
- `decode`: latency summary for selector and calldata decode
- `fast_preflight`: latency summary for cheap deterministic rejection
- `adaptive_preflight`: latency summary for stateful adaptive rejection
- `adaptive_quote`: latency summary for relay-aware adaptive quote
- `total_hot_gate`: end-to-end synchronous hot-gate latency

When this mode is enabled, placeholder credentials and placeholder addresses are ignored and replaced by synthetic local-only values before the benchmark starts. Normal runtime, replay, network benchmark, and execution paths keep the regular configuration validation.

### Network benchmark

Network benchmark mode is for infrastructure validation. It measures:

- RPC latency and stability
- endpoint-specific probe performance
- optional bundle path behavior where relevant

This should be treated as infrastructure observability, not strategy logic.

## Dashboard

The local dashboard exposes runtime state relevant to production operation.

Examples:

- current regime
- relay ranking
- reject reasons
- executor buffer status
- treasury recommendations
- execution outcomes
- latency pipeline

The active runtime also emits operational events directly through the dashboard event stream for cases that matter during low-capital validation:

- opportunity skipped because observed alvo gas is above the configured cap
- executor blocked because live RPC gas is above the configured cap
- RPC submit failed
- insufficient funds for gas/value during direct-RPC execution attempts

The React operator panel can also call runtime control endpoints exposed by the backend:

- `POST /api/events/clear` clears historical dashboard events.
- `POST /api/opportunity-mode/:mode` switches the in-memory opportunity mode without changing `.env`.
- `POST /api/opportunity-thresholds` updates the in-memory opportunity threshold floor values.
- `POST /api/rpc/:id/enabled` enables or disables a single RPC endpoint.
- `POST /api/rpc/only-getblock` disables all non-GetBlock endpoints for BNB operation.

Runtime controls are not persistent. On restart or redeploy the process reads `.env` again. Use the panel for live tuning and the environment for boot defaults.

The dashboard is intended for operational inspection, not public hosting.

The static operational frontend lives under:

- `web/static/index.html`
- `web/static/styles.css`
- `web/static/js/app.js`
- `web/static/js/data.js`
- `web/static/js/fx.js`
- `web/static/js/radar.js`

This frontend is part of the active project surface. It is focused on execution visibility, relay quality, treasury state, reject reasons, and live event inspection.

## Environment Variables

Below is the core environment surface used by the active runtime.

For BNB Chain configuration, use the `_BSC` suffix as the canonical operator-facing convention, for example `RPC_URL_BSC`, `MEMPOOL_WS_URL_BSC`, and `MEV_MAX_GAS_PRICE_GWEI_BSC`. The code also accepts `_BNB` aliases for backward compatibility, but new `.env` files should prefer `_BSC`.

### Core runtime

- `NETWORK`
- `ALLOW_SEND`
- `USE_TENDERLY_RPC_ONLY`
- `CHAIN_ID`
- `DASHBOARD_ADDR`
- `MEMPOOL_WS_URL`
- `MEMPOOL_WS_URL_BSC`
- `MEMPOOL_WS_URL_POLYGON`
- `STORAGE_PATH`
- `DATABASE_URL`
- `STORAGE_POSTGRES_REQUIRED`
- `STORAGE_EVENTS_RETENTION_HOURS`
- `STORAGE_TELEMETRY_RETENTION_HOURS`

If `DATABASE_URL` is set, the storage layer attempts Postgres first. With `STORAGE_POSTGRES_REQUIRED=true`, any Postgres connection or migration failure is fatal. With `STORAGE_POSTGRES_REQUIRED=false` or unset, the runtime falls back to SQLite at `STORAGE_PATH` if Postgres is unavailable, which is useful during database disk-pressure incidents.

Runtime-only storage is pruned automatically:

- `STORAGE_EVENTS_RETENTION_HOURS`: event feed retention, default `24`.
- `STORAGE_TELEMETRY_RETENTION_HOURS`: latency telemetry retention, default `6`.

Execution outcomes, relay metrics, toxicity profiles, and treasury records are not part of this short runtime prune. They remain the evidence layer for historical calibration.

### RPC and execution path

- `RPC_URL`
- `RPC_URL_2`
- `RPC_URL_3`
- `RPC_URL_4`
- `RPC_URL_BSC`
- `RPC_URL_POLYGON`
- `ALCHEMY_KEY`
- `FLASHBOTS_RELAY`
- `BUILDER_RELAYS`
- `RPC_READ_PREFERENCE`
- `RPC_SEND_PREFERENCE`
- `MEV_PENDING_LOOKUP_FANOUT`
- `MEV_PENDING_LOOKUP_MAX_PER_SEC`
- `MEV_LOOKUP_DECODE_WORKERS`
- `MEV_EVAL_WORKERS`
- `MEV_BLOCK_LOOKUP_FANOUT`
- `MEV_PAYLOAD_BUILD_FANOUT`

`MEV_PENDING_LOOKUP_FANOUT` controls how many read RPC endpoints are queried for each pending transaction hash before decode. The default is `1`. Raising it to `2` or `3` can improve pending transaction hit rate, but it multiplies paid RPC usage and can trigger provider rate limits.

`MEV_PENDING_LOOKUP_MAX_PER_SEC` caps how many pending hashes are accepted into the lookup/decode queue each second. When configured, the runtime uses that value while read RPCs are healthy. It still backs off under real RPC pressure: if all readers are unavailable, rate-limited, or failing, the effective budget is reduced until the fleet recovers. In scavenger shadow runs this prevents a configured high intake rate from continuing to fill the queue while the only usable RPC endpoint is in cooldown.

`MEV_LOOKUP_DECODE_WORKERS` and `MEV_EVAL_WORKERS` control the async worker counts for the lookup/decode and evaluation stages. Defaults are tuned for I/O-heavy mempool processing (`6` lookup/decode workers and `4` evaluation workers) rather than CPU count. Keep lookup workers below the level that causes provider rate limits.

`MEV_BLOCK_LOOKUP_FANOUT` controls how many read RPC endpoints are queried for the current block once a transaction already decoded into a relevant candidate. The default is `1`. Block lookup intentionally happens after decode so irrelevant transactions do not spend extra RPC.

`MEV_PAYLOAD_BUILD_FANOUT` controls how many read RPC endpoints race during payload construction. The default is `1` in `scavenger` and `3` in the other modes. Keep it at `1` when providers show rate limiting; payload build is the most expensive read stage because it performs factory, pair/pool, and pool-state calls.

The runtime tracks burst reservations per endpoint and decays failure counters over time. Short cooldowns or old provider errors should not permanently mark an endpoint as unusable. The operator panel reports:

- `HEALTHY` for usable endpoints
- `DEGRADED` for temporary cooldown, stale block age, or light failure pressure
- `PENALIZED` for disabled endpoints or sustained rate-limit pressure

### Wallets

- `EXECUTOR_PRIVATE_KEY`
- `CONTROL_ADDRESS`
- `VAULT_ADDRESS`
- `PROFIT_ADDRESS`

Bootstrap production can run with a two-wallet layout:

- executor hot wallet: derived from `EXECUTOR_PRIVATE_KEY`, funded with native gas for execution
- cold wallet: one address reused for `CONTROL_ADDRESS`, `VAULT_ADDRESS`, and `PROFIT_ADDRESS`

This avoids unnecessary internal wallet movement while capital is small. The runtime treats `CONTROL=VAULT=PROFIT` as an explicit initial layout and only warns when any cold wallet address matches the executor hot wallet. Split control, vault, and profit addresses before larger capital or multi-operator operation.

### Engine thresholds

- `MEV_ENGINE_ENABLED`
- `MEV_OPPORTUNITY_MODE`
- `MEV_CAPITAL_ETH`
- `MEV_MIN_NET_PROFIT_ETH`
- `MEV_MIN_PROFIT_USD`
- `MEV_MIN_ROI_BPS`
- `MEV_MIN_LARGE_SWAP_ETH`
- `MEV_MAX_PENDING_AGE_MS`
- `MEV_MAX_GAS_PER_TX`
- `MEV_MAX_GAS_PRICE_GWEI`
- `MEV_MAX_GAS_PRICE_GWEI_BSC`
- `MEV_MAX_GAS_PRICE_GWEI_POLYGON`
- `MEV_MAX_PRICE_IMPACT_BPS`
- `MEV_SLIPPAGE_PROTECTION_BPS`
- `MEV_ETH_USD_PRICE`
- `MEV_MIN_LIQUIDITY_ETH`

`MEV_OPPORTUNITY_MODE` controls the boot-time opportunity filter profile. Accepted values are:

- `conservative`: current strict profile, lowest noise.
- `sangrento`: aggressive live profile with controlled preflight/adaptive overrides for apparently positive opportunities.
- `scavenger`: farelo extractor profile. It opens decode for unknown priced input tokens, uses very low notional/profit/ROI floors, skips the expensive EVM preflight hot path, and allows more candidates to reach payload/EV checks.

The backend also accepts the aliases `safe`/`atual`, `aggressive`/`bloody`, and `bypass`/`farelo`/`farelo-extractor`. Quoted values such as `"conservative"` are accepted. `balanced`, `medium`, and `medio` are not valid modes.

The threshold variables ending in `_ETH` are legacy names for the native chain unit. On Polygon they are interpreted as `POL`; on BNB Chain they are interpreted as `BNB`.

The panel's Settings page exposes live controls for:

- opportunity mode
- minimum swap size
- minimum net profit
- minimum USD EV floor
- minimum liquidity

These controls update runtime memory only. To keep a value after restart, set the corresponding environment variable.

The MEV Engine panel also exposes an opportunity funnel:

- pending hashes received
- pending transaction lookup success/miss
- decode pass/reject
- block lookup success/fail
- fast/adaptive preflight pass/reject
- payload built/reject
- EV/adaptive quote pass/reject
- execution ready
- submit attempted/succeeded/failed

Use this funnel before loosening strategy logic. If `pending hashes` rises but `decode pass` stays at zero, the issue is router/token/notional coverage, not execution. If `payload built` stays at zero, the issue is state lookup, pool support, liquidity, or profit gates. If `submit attempted` stays at zero after `execution ready`, the issue is executor readiness or execution guardrails.

Decode rejects are intentionally granular. The main reasons to watch are:

- `selector_unsupported`: calldata selector is not one of the supported direct router or aggregator selectors.
- `universal_router_command_unsupported`: Universal Router calldata decoded, but it used commands outside the executable V2/V3 subset.
- `universal_router_subplan_not_extracted`: Universal Router command graph was present, but no executable hop was extracted from nested subplans.
- `universal_router_abi_decode_failed`: Universal Router selector matched, but commands/inputs could not be decoded.
- `universal_router_path_invalid`: a swap command was present, but the resulting route was not executable by the current V2/V3 payload path.
- `monitored_token_not_in_path`: aggregator route decoded, but it did not include a configured monitored token.
- `monitored_token_not_input`: a monitored token is present in the decoded path, but the path input token is not priced in `MONITORED_TOKENS_*`.
- `token_monitored_path_invalid`: monitored token bytes were present, but no valid priced path/notional could be produced.
- `amount_in_below_min`: a swap decoded, but its notional was below the active mode threshold.

In `scavenger`, `monitored_token_not_input` routes are allowed to continue as shadow candidates with a conservative notional floor so payload telemetry can reveal whether the route is economically useful. In stricter modes, the input token still needs a configured price to pass normal notional gating.

Universal Router telemetry includes command names, input sizes, nested subplan status, swap count, hop count, unsupported commands, and route graph hints. Treat `permit2_*`, wrap/unwrap, sweep, transfer, and balance-check commands as context commands; they are not payload hops by themselves.

Payload rejects also emit `payload_build_detail`. The key categories are:

- `factory_wrong_or_unavailable`: no usable configured/default factory was available.
- `v2_pair_not_found`: no V2 pair was found for the decoded token pair.
- `v3_fee_tier_or_pool_not_found`: no V3 pool was found for the decoded token pair and fee tier.
- `v3_routed_to_v2_pair_lookup`: a V3-shaped opportunity reached a V2 pair lookup path.
- `path_inverted_or_pool_token_mismatch`: the pool exists, but the reverse path is not supported by the pool token ordering.
- `pool_state_unavailable`: pair/pool was found, but live state fetch/cache lookup failed.
- `victim_price_impact_too_high`: the victim move is too large for the configured impact cap.
- `economic_no_positive_gross_edge`: sizing found no positive gross edge before gas.
- `economic_no_positive_net_after_gas`: gross edge existed, but no size survived gas.
- `economic_profit_below_floor`: simulated profit was below configured minimums.

Edge telemetry samples for decode and payload rejects carry route context so the next tuning pass can be based on evidence: selector, route kind, token path, amount in, amount out or minimum out, repayment, gross edge, gas estimate, pool/factory hints, fee tier, and hop notes.

After payload construction, EV and quality gate rejects are also sampled. Watch for:

- `ev_lookup_stale`: pending transaction lookup was too old for the active age window.
- `ev_zero_expected_profit`: payload has no positive expected profit.
- `ev_gas_limit_above_cap`: payload gas exceeds `MEV_MAX_GAS_PER_TX`.
- `ev_notional_below_min`: decoded notional is below the active minimum.
- `ev_price_impact_too_low`: impact is too small for the deterministic edge gate.
- `ev_profit_below_min_wei`: expected profit is below the native-unit floor.
- `ev_profit_below_min_usd`: expected profit is below the USD floor.
- `ev_scavenger_edge_below_floor`: scavenger gross edge is below the configured gas-fraction/USD floor.
- `quality_roi_below_min`: ROI is below the active ROI floor.
- `quality_price_impact_above_cap` or `quality_impact_score_above_cap`: payload impact is above the quality cap.

These samples include `expected_profit_wei`, `execution_cost_wei`, `min_profit_or_floor_wei`, `net_ev_usd`, `roi_bps`, gas limit, price impact, pool, router, selector, and path.

For low-capital farelo runs, use `MEV_OPPORTUNITY_MODE=scavenger` first. Keep `conservative` for larger capital, bad market conditions, expensive RPC, or when the priority is avoiding false positives over execution frequency.

### Expensive execution guardrails

- `MEV_EVM_PREFLIGHT_ENABLED`
- `MEV_EVM_PREFLIGHT_HARD_FAIL`

`MEV_EVM_PREFLIGHT_ENABLED=true` enables local `revm` preflight as an execution guardrail. Treat it as a heavier validation path, not as a default cheap hot-path filter. Keep it disabled for latency-sensitive runs unless replay or live telemetry shows the revert avoidance is worth the added delay.

`MEV_EVM_PREFLIGHT_HARD_FAIL=true` makes preflight failure block execution. Without it, preflight errors are soft failures in the executor path.

### Capital window controls

- `MEV_CAPITAL_WINDOW_SECS`
- `MEV_MAX_WINDOW_EXPOSURE_ETH`
- `MEV_MAX_CLUSTER_WINDOW_EXPOSURE_ETH`
- `MEV_MAX_PAIR_WINDOW_EXPOSURE_ETH`
- `MEV_RELAY_FANOUT_COUNT`
- `MEV_RPC_FANOUT_COUNT`
- `MEV_GAS_OVERPAY_BASE_EXTRA_BPS`
- `MEV_GAS_OVERPAY_MISS_EXTRA_BPS`
- `MEV_GAS_OVERPAY_REVERT_EXTRA_BPS`
- `MEV_GAS_OVERPAY_SUBMIT_FAILURE_EXTRA_BPS`
- `MEV_GAS_OVERPAY_MAX_EXTRA_BPS`
- `MEV_FINALITY_CONFIRMATIONS`
- `MEV_STOP_LOSS_CONSECUTIVE_LOSSES`
- `MEV_STOP_LOSS_FREEZE_SECS`
- `MEV_CAPITAL_MULTIPLIER_AGGRESSIVE`
- `MEV_CAPITAL_MULTIPLIER_NEUTRAL`
- `MEV_CAPITAL_MULTIPLIER_DEFENSIVE`
- `MEV_CAPITAL_MULTIPLIER_PRIORITY_THRESHOLD`
- `MEV_CAPITAL_MULTIPLIER_TOXICITY_THRESHOLD`

### Executor buffer controls

- `MEV_EXECUTOR_MIN_BUFFER_ETH`
- `MEV_EXECUTOR_TARGET_BUFFER_ETH`
- `MEV_EXECUTOR_MAX_BUFFER_ETH`

### Contracts

- `MEV_UNISWAP_V2_FACTORY`
- `MEV_UNISWAP_V3_FACTORY`
- `MEV_EXECUTOR_ADDRESS`
- `MEV_EXECUTOR_V3_ADDRESS`

### Fork replay

- `RUN_REPLAY_HARNESS`
- `REPLAY_INPUT_PATH`
- `REPLAY_LIMIT`
- `REPLAY_OUTPUT_PATH`
- `TENDERLY_FORK_URL_BNB`
- `TENDERLY_FORK_URL_POLYGON`
- `TENDERLY_FORK_URL_ETHEREUM`
- `TENDERLY_FORK_URL_ARBITRUM`

## Running

### Normal runtime

```bash
cargo run -- --network polygon
```

The runtime still honors `NETWORK` from `.env`. The CLI flag is useful when you want an explicit override.

### Current validated live posture

At the moment, the actively validated path is:

- `polygon`
- direct-RPC execution
- mempool websocket via `MEMPOOL_WS_URL` or `MEMPOOL_WS_URL_POLYGON`
- gas cap through `MEV_MAX_GAS_PRICE_GWEI_POLYGON`

This path has already been validated at startup level with:

- `Network: polygon`
- `Chain id: 137`
- visible startup guardrail log for `max_gas_price`

### BNB Chain note

BNB Chain is supported by the runtime decision model, but live operation should use explicit RPC configuration.

Provide at least:

- `RPC_URL_BSC`
- `MEMPOOL_WS_URL_BSC`

## WAR Real: Next 48 Hours

The current gap is economic, not raw CPU throughput. For cloud Linux deployment on AMD EPYC class hardware, the next 48 hours should focus on inclusion quality, gas discipline, and capital preservation.

### D0-D1

Ship and validate the production controls already wired into the runtime:

- direct-RPC fanout via `MEV_RPC_FANOUT_COUNT`
- relay fanout cap via `MEV_RELAY_FANOUT_COUNT`
- dynamic gas overpay via the `MEV_GAS_OVERPAY_*` controls
- stop-loss freeze via `MEV_STOP_LOSS_*`
- contextual capital sizing via `MEV_CAPITAL_MULTIPLIER_*`

Run targets:

```bash
cargo check
cargo test
RUN_RUNTIME_LOAD_TEST=true RUNTIME_LOAD_TEST_PROFILE=baseline cargo run --release -- --network polygon
RUN_RUNTIME_LOAD_TEST=true RUNTIME_LOAD_TEST_PROFILE=adversarial cargo run --release -- --network polygon
```

Expected metrics:

- `baseline total_hot_gate p99 <= 250us` on isolated Linux cores
- adversarial tail should stay explainable by scheduler or injected scenario, not random hot-path regressions
- no direct-RPC single-endpoint dependency in Polygon/BNB send path
- no execution after `MEV_STOP_LOSS_CONSECUTIVE_LOSSES` threshold is hit

### D1-D2

Use replay and low-capital live observation to calibrate the economic controls:

- compare inclusion/miss rate before and after fanout
- compare accepted-but-not-included rate before and after gas overpay
- compare realized PnL stability before and after stop-loss freeze
- compare capital committed in toxic vs favorable contexts

Run targets:

```bash
RUN_REPLAY_HARNESS=true REPLAY_INPUT_PATH=./replay/polygon_cases.jsonl cargo run --release -- --network polygon
```

Review artifacts:

- `exports/toxicity_profiles.csv`
- `exports/realized_vs_expected.json`
- `exports/runtime_latency_benchmarks.json`
- replay decision output if `REPLAY_OUTPUT_PATH` is set

Expected metrics:

- inclusion rate improvement on top-ranked RPC paths
- lower `accepted_not_included` share under the same gas cap regime
- lower consecutive loss streak length
- lower capital committed to high-toxicity router/pair/hour contexts
- realized-vs-expected capture trend moving up, not just raw execution count

### Environment Surface Added For This Phase

```text
MEV_RELAY_FANOUT_COUNT
MEV_RPC_FANOUT_COUNT
MEV_GAS_OVERPAY_BASE_EXTRA_BPS
MEV_GAS_OVERPAY_MISS_EXTRA_BPS
MEV_GAS_OVERPAY_REVERT_EXTRA_BPS
MEV_GAS_OVERPAY_SUBMIT_FAILURE_EXTRA_BPS
MEV_GAS_OVERPAY_MAX_EXTRA_BPS
MEV_FINALITY_CONFIRMATIONS
MEV_STOP_LOSS_CONSECUTIVE_LOSSES
MEV_STOP_LOSS_FREEZE_SECS
MEV_CAPITAL_MULTIPLIER_AGGRESSIVE
MEV_CAPITAL_MULTIPLIER_NEUTRAL
MEV_CAPITAL_MULTIPLIER_DEFENSIVE
MEV_CAPITAL_MULTIPLIER_PRIORITY_THRESHOLD
MEV_CAPITAL_MULTIPLIER_TOXICITY_THRESHOLD
```

These controls are aimed at the real production gap:

`accepted -> not included`, gas bleed, false positives in toxic contexts, and repeated loss clusters.

This matters because the generic provider autoconstruction for BNB should not rely on Ethereum/Arbitrum/Polygon-only provider assumptions.

`RPC_URL_BNB` and `MEMPOOL_WS_URL_BNB` remain accepted as backward-compatible aliases, but `_BSC` is the documented canonical suffix.

### Network benchmark

```bash
RUN_NETWORK_BENCHMARK=true cargo run -- --network polygon
```

### Replay harness

```bash
RUN_REPLAY_HARNESS=true \
REPLAY_INPUT_PATH=./replay/polygon_cases.jsonl \
cargo run -- --network polygon
```

### Low-capital observation mode

For low-capital validation, the recommended sequence is:

1. Run with `MEV_ENGINE_ENABLED=true` and the chain-specific gas cap set.
2. Keep the executor wallet at zero or near-zero balance.
3. Let the runtime observe live mempool flow.
4. Wait for a visible direct-RPC submit failure such as insufficient funds.

The first `insufficient funds` event is treated as proof that the full path reached:

`mempool read -> decode -> payload build -> EV gate -> execution attempt`

## Operational Notes

This project is intended for private operation.

That implies:

- protect secrets aggressively
- do not expose dashboards publicly
- do not commit `.env`, keys, local SQLite state, or replay datasets with sensitive flow
- validate chain-specific fork URLs before trusting any replay output
- calibrate by network separately

Practical gas-cap guidance for the currently intended starting chains:

- BNB Chain: use a tight cap such as `4` or `5` gwei unless you intentionally want to participate in aggressive gas bidding
- Polygon: use a materially higher cap such as `50` to `100` gwei depending on how much idle time versus gas risk you accept

These are operational guardrails, not profitability guarantees.

## Current Boundaries

The engine is intentionally narrow.

It is strong where it is explicit:

- pending swap parsing
- AMM V2 and V3 impact paths
- chain-aware execution routing
- relay-aware adaptive gating
- historical contextual calibration
- capital budgeting
- replay-based decision review

It includes deterministic Post-Alvo state simulation, slippage-aware sizing gates, and optional local EVM preflight through `revm` with explicit state overrides.

That means the runtime is safer than a naive mempool bot, but the preflight path should still be treated as an execution guardrail, not a profitability guarantee. Full fork-backed state simulation remains a separate hardening target.

It is not marketed as a universal arbitrage platform or cross-domain MEV framework.

## Repository Layout

The active project lives in this directory.

```text
fee-extraction/
  contracts/
   base/
      IArbitrageRuntimeExecutor.sol
   interfaces/
      IArbitrageRuntimeExecutor.sol
   ArbitrageRuntimeExecutor.sol
  web/
    static/
      index.html
      styles.css
      favicon.svg
      js/
        app.js
        data.js
        fx.js
        radar.js
  src/
    main.rs
    benchmark.rs
    config.rs
    dashboard.rs
    replay.rs
    rpc.rs
    storage.rs
    wallets.rs
    mev/
      adaptive.rs
      runtime.rs
      opportunity.rs
      amm/
      execution/
      pnl/
      simulation/
      Cargo.toml
```

Legacy or orphaned code is intentionally kept outside this active project root.

## Production Intent

The intent of `arbitrage-runtime` is not to look broad.

It is to be narrow, fast, and difficult to confuse:

- one active ecosystem
- one decision kernel
- chain-aware execution behavior
- selective execution under pressure
- realized-outcome feedback for calibration

That is the operating model this repository is built to serve.
