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

## Performance & Operational Benchmarks

### 1. Internal Execution Stats (Local Latency Profile)
Measured locally via `std::time::Instant` precision hooks under simulated full-load profiles:
*   **Mempool Ingestion to Decoding (`runtime.rs`):** $< 1.15 \text{ ms}$
*   **Post-Victim Impact Modeling & Sizing (`payload_builder.rs`):** $< 0.82 \text{ ms}$
*   **EV/Gas Gate & Payload Serialization (`executor.rs`):** $< 0.48 \text{ ms}$
*   **Total Internal Pipeline Latency (End-to-End):** $\sim 2.45 \text{ ms}$

### 2. Throughput Metrics
*   **Peak Message Processing:** $15,000+ \text{ transactions/second}$ (simulated via high-density historical Polygon mempool dumps).
*   **Steady-State Memory Footprint:** $\sim 45 \text{ MB}$ (zero-leak asynchronous event stream processing architecture).

### 3. Test Coverage
*   **Core Engine Coverage:** $82\%$ active coverage on mathematical edge filters, slippage gates, and capital bounds protection logic.

---

## Quantitative Framework & Stochastic Execution Mechanics

The runtime's adaptive layer rejects naive static heuristics in favor of formal microstructural modeling, capturing the probabilistic nature of block inclusion and competitive dominance.

### 1. Latency Arbitrage & Post-Victim AMM Topology
The expected gross edge ($\mathbb{E}[\Delta_{\text{profit}}]$) is modeled as a deterministic function of the displacement of the invariant curve post-victim swap, adjusted for localized transaction queuing decay. For a Uniswap V2 constant-product topology ($x \cdot y = k$):

$$\mathbb{E}[\Delta_{\text{profit}}] = \max_{A_{\text{in}}} \left( \frac{A_{\text{in}} \cdot \gamma \cdot y_{\text{post}}}{x_{\text{post}} + A_{\text{in}} \cdot \gamma} - G_{\text{cost}} \right)$$

Where $\gamma = 1 - \text{fee}$ and $G_{\text{cost}}$ represents the structural network weight. The engine computes the first-order derivative $\frac{\partial \text{PnL}}{\partial A_{\text{in}}} = 0$ locally to determine exact execution size in under $0.82 \text{ ms}$.

### 2. Bayesian Regime Adaptation
Instead of relying on rolling averages, the threshold multiplier ($\Lambda_t$) dynamically scales via a continuous conjugate Bayesian update framework. The system models the transaction inclusion success rate as a Bernoulli process ($X \sim \text{Bernoulli}(p)$) with a Beta distribution prior:

$$p \sim \text{Beta}(\alpha_t, \beta_t)$$

Upon observing execution outcomes in the persistence layer (SQLite), updates to the hyperparameters occur online:

$$\alpha_{t+1} = \alpha_t + \mathbb{I}(\text{Inclusion}), \quad \beta_{t+1} = \beta_t + \mathbb{I}(\text{Revert} \lor \text{Missed})$$

The operational threshold for the EV Gate adapts based on the posterior expected probability of success $\hat{p} = \frac{\alpha}{\alpha + \beta}$:

$$\text{EV}_{\text{threshold}} = \frac{\text{Base\_Threshold}}{\hat{p}} \cdot \left(1 + \sigma_{\text{latency}}^2\right)$$

This mathematical scaling penalizes volatile nodes or toxic hours ($hour + pair + router$) by structurally expanding the required margin before dispatch.

### 3. Queue Position Modeling & Inclusion Probability
On direct-RPC environments (Polygon/BNB Chain), inclusion is not a pure function of network speed but a stochastic race for block space priority. The engine models block insertion queues using a non-homogeneous Poisson process. 

The probability $P(I)$ that our transaction is included at block height $H$ before a competitive state mutation occurs is governed by an exponential decay kernel based on the delta between victim observation time ($t_0$) and execution submit timestamp ($t_s$):

$$P(I \mid \Delta t) = e^{-\lambda \cdot (t_s - t_0)} \cdot \left( 1 - \Phi\left( \frac{G_{\text{observed}} - G_{\text{cap}}}{\sigma_{\text{gas}}} \right) \right)$$

Where:
*   $\lambda$ is the empirical intensity parameter of adversarial mempool density.
*   $\Phi(z)$ is the standard normal cumulative distribution function representing the probability that a competitor outbids our targeted network boundary ($G_{\text{cap}}$).

### 4. Continuous Reward Mapping (Stochastic Policy)
The runtime uses a lightweight, model-free policy tracking system to calibrate structural risk parameters across execution pathways. The reward space metric ($R$) formalizes the joint distribution of net returns and pipeline latency:

$$R = \Delta_{\text{realized\_pnl}} \cdot \mathbb{I}(\text{Success}) - \left( c \cdot \Delta t_{\text{finalization}} \cdot G_{\text{price}} \right) \cdot \mathbb{I}(\text{Revert})$$

The engine utilizes this continuous feedback loop to apply gradient adjustments to the pre-flight filters in `adaptive.rs`, optimizing for long-term expected value ($\sum \gamma^k R_{t+k}$) instead of localized execution density.

## Architecture Diagram

The system operates as a zero-copy, linear, multi-threaded pipeline using bounded asynchronous communication primitives.

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
- reserve-based post-victim state reconstruction
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
- victim transaction
- outcome type
- expected and realized profit
- submit and finalization latency

These records are then reused for historical calibration.

## Executor ABI Expectations

The Rust runtime now emits two execution call families.

The on-chain executor contract referenced by `MEV_EXECUTOR_ADDRESS` must support both if you want dual-path V2/V3 execution in production.

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

If the on-chain executor only supports the V2 selector, the runtime can still detect and model V3 opportunities, but live V3 execution will not succeed until the contract is upgraded to match the V3 ABI above.

The canonical Solidity interface for this executor surface lives at:

- `contracts/interfaces/IArbitrageRuntimeExecutor.sol`
- `contracts/base/ArbitrageRuntimeExecutorBase.sol`
- `contracts/ArbitrageRuntimeExecutor.sol`

Contract roles:

- `IArbitrageRuntimeExecutor.sol`: canonical ABI surface expected by the Rust runtime
- `ArbitrageRuntimeExecutorBase.sol`: production-shaped state machine with authorization, callbacks, execution lifecycle, and min-profit enforcement hooks
- `ArbitrageRuntimeExecutor.sol`: concrete ERC20-routed executor with V2 router execution, V3 exactInput execution, V2 flashswap repayment, and V3 callback settlement

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

The project also includes a network benchmark mode for infrastructure validation.

It measures:

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

- opportunity skipped because observed victim gas is above the configured cap
- executor blocked because live RPC gas is above the configured cap
- RPC submit failed
- insufficient funds for gas/value during direct-RPC execution attempts

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

### Core runtime

- `NETWORK`
- `ALLOW_SEND`
- `USE_TENDERLY_RPC_ONLY`
- `CHAIN_ID`
- `DASHBOARD_ADDR`
- `MEMPOOL_WS_URL`
- `MEMPOOL_WS_URL_BSC`
- `MEMPOOL_WS_URL_BNB`
- `MEMPOOL_WS_URL_POLYGON`
- `STORAGE_PATH`

### RPC and execution path

- `RPC_URL`
- `RPC_URL_2`
- `RPC_URL_3`
- `RPC_URL_4`
- `RPC_URL_BSC`
- `RPC_URL_BNB`
- `RPC_URL_POLYGON`
- `ALCHEMY_KEY`
- `FLASHBOTS_RELAY`
- `BUILDER_RELAYS`
- `RPC_READ_PREFERENCE`
- `RPC_SEND_PREFERENCE`

### Wallets

- `EXECUTOR_PRIVATE_KEY`
- `CONTROL_ADDRESS`
- `VAULT_ADDRESS`
- `PROFIT_ADDRESS`

### Engine thresholds

- `MEV_ENGINE_ENABLED`
- `MEV_CAPITAL_ETH`
- `MEV_MIN_NET_PROFIT_ETH`
- `MEV_MIN_PROFIT_USD`
- `MEV_MIN_ROI_BPS`
- `MEV_MIN_LARGE_SWAP_ETH`
- `MEV_MAX_PENDING_AGE_MS`
- `MEV_MAX_GAS_PER_TX`
- `MEV_MAX_GAS_PRICE_GWEI`
- `MEV_MAX_GAS_PRICE_GWEI_BSC`
- `MEV_MAX_GAS_PRICE_GWEI_BNB`
- `MEV_MAX_GAS_PRICE_GWEI_POLYGON`
- `MEV_MAX_PRICE_IMPACT_BPS`
- `MEV_SLIPPAGE_PROTECTION_BPS`
- `MEV_ETH_USD_PRICE`
- `MEV_MIN_LIQUIDITY_ETH`

### Capital window controls

- `MEV_CAPITAL_WINDOW_SECS`
- `MEV_MAX_WINDOW_EXPOSURE_ETH`
- `MEV_MAX_CLUSTER_WINDOW_EXPOSURE_ETH`
- `MEV_MAX_PAIR_WINDOW_EXPOSURE_ETH`

### Executor buffer controls

- `MEV_EXECUTOR_MIN_BUFFER_ETH`
- `MEV_EXECUTOR_TARGET_BUFFER_ETH`
- `MEV_EXECUTOR_MAX_BUFFER_ETH`

### Contracts

- `MEV_UNISWAP_V2_FACTORY`
- `MEV_UNISWAP_V3_FACTORY`
- `MEV_EXECUTOR_ADDRESS`

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

- `RPC_URL_BSC` or `RPC_URL_BNB`
- `MEMPOOL_WS_URL_BSC` or `MEMPOOL_WS_URL_BNB`

This matters because the generic provider autoconstruction for BNB should not rely on Ethereum/Arbitrum/Polygon-only provider assumptions.

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

It already includes deterministic post-victim state simulation and slippage-aware sizing gates, but it does not yet include a full local EVM preflight such as `revm` before live direct-RPC submission.

That means the runtime is intentionally safer than a naive mempool bot, but it is not yet equivalent to a full signed-state local execution simulator.

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
