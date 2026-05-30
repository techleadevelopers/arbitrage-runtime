#![allow(dead_code)]

use crate::config::{Config, MonitoredTokenConfig, OpportunityMode};
use crate::dashboard::DashboardHandle;
use crate::mev::adaptive::{
    AdaptivePolicy, AdaptiveQuoteInput, ClusterKey, ContextSignal, PreflightInput,
};
use crate::mev::amm::uniswap_v2::{amount_out_exact_in, price_impact_bps, V2PoolState};
use crate::mev::amm::uniswap_v3::V3PoolState;
use crate::mev::cache::pool_cache::PoolCache;
use crate::mev::execution::payload_builder::{
    AmmRouteKind, FeeExtractionBuildInput, PayloadBuilder,
};
use crate::mev::execution::payload_builder::{EdgeMetadata, ExecutionPayload};
use crate::mev::execution::ExecutionEngine;
use crate::mev::opportunity::{roi_bps, wei_to_eth_f64, MevOpportunity};
use crate::mev::simulation::state_simulator::{
    AccountState, AmmState, EvmPreflightResult, StateSimulator,
};
use crate::rpc::RpcFleet;
use crate::storage::Storage;
use chrono::Timelike;
use ethers::abi::{self, ParamType, Token};
use ethers::providers::{Middleware, Provider, StreamExt, Ws};
use ethers::types::{Address, Transaction, H256, U256};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::{Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tracing::{debug, warn};

const MICROBATCH_WINDOW_MS: u64 = 45;
const MICROBATCH_MAX_CANDIDATES: usize = 4;
const LOOKUP_DECODE_QUEUE_CAPACITY: usize = 8192;
const EVAL_QUEUE_CAPACITY: usize = 512;
const LOOKUP_DECODE_WORKERS_MAX: usize = 6;
const EVAL_WORKERS_MAX: usize = 4;
const SMALL_SCAVENGER_BUDGET_PER_SEC: u64 = 8;

const SWAP_EXACT_TOKENS_FOR_TOKENS: [u8; 4] = [0x38, 0xed, 0x17, 0x39];
const SWAP_EXACT_ETH_FOR_TOKENS: [u8; 4] = [0x7f, 0xf3, 0x6a, 0xb5];
const SWAP_EXACT_TOKENS_FOR_ETH: [u8; 4] = [0x18, 0xcb, 0xaf, 0xe5];
const SWAP_EXACT_TOKENS_FOR_TOKENS_SUPPORTING_FEE: [u8; 4] = [0x5c, 0x11, 0xd7, 0x95];
const SWAP_EXACT_ETH_FOR_TOKENS_SUPPORTING_FEE: [u8; 4] = [0xb6, 0xf9, 0xde, 0x95];
const SWAP_EXACT_TOKENS_FOR_ETH_SUPPORTING_FEE: [u8; 4] = [0x79, 0x1a, 0xc9, 0x47];
const V3_EXACT_INPUT_SINGLE: [u8; 4] = [0x41, 0x4b, 0xf3, 0x89];
const V3_EXACT_INPUT: [u8; 4] = [0xc0, 0x4b, 0x8d, 0x59];
const UNIVERSAL_ROUTER_EXECUTE: [u8; 4] = [0x35, 0x93, 0x56, 0x4c];
const UNIVERSAL_ROUTER_EXECUTE_NO_DEADLINE: [u8; 4] = [0x24, 0x85, 0x6b, 0xc3];
const UNIVERSAL_ROUTER_COMMAND_MASK: u8 = 0x7f;
const ZERO_EX_TRANSFORM_ERC20: [u8; 4] = [0x41, 0x55, 0x65, 0xb0];
const ZERO_EX_SELL_TO_UNISWAP: [u8; 4] = [0xd9, 0x62, 0x7a, 0xa4];
const ONE_INCH_SWAP: [u8; 4] = [0x12, 0xaa, 0x3c, 0x6a];
const ONE_INCH_UNOSWAP: [u8; 4] = [0x2e, 0x95, 0xb6, 0xc8];
const PARASWAP_SIMPLE_SWAP: [u8; 4] = [0x54, 0xe3, 0xf3, 0x1b];
const ODOS_SWAP_COMPACT: [u8; 4] = [0x83, 0xbd, 0x37, 0xf9];
const KYBER_SWAP: [u8; 4] = [0x3f, 0x2d, 0x5c, 0xf5];

#[derive(Debug, Clone)]
pub(crate) enum SwapKind {
    V2,
    V3 {
        fee_tier: u32,
        encoded_path: ethers::types::Bytes,
        hops: usize,
        exact_out: bool, // <-- ADICIONAR
    },
}

#[derive(Debug, Clone)]
pub(crate) struct SwapSignal {
    pub(crate) selector: [u8; 4],
    pub(crate) amount_in: U256,
    pub(crate) amount_out_min: Option<U256>,
    pub(crate) notional_wei: U256,
    pub(crate) path: Vec<Address>,
    pub(crate) router: Address,
    pub(crate) kind: SwapKind,
}

impl SwapSignal {
    fn path_len(&self) -> usize {
        match &self.kind {
            SwapKind::V2 => self.path.len(),
            SwapKind::V3 { hops, .. } => hops.saturating_add(1).max(self.path.len()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FastPreflightDecision {
    pub(crate) should_continue: bool,
    pub(crate) reject_reason: Option<&'static str>,
    pub(crate) ev_upper_bound_usd: f64,
    pub(crate) estimated_gas_cost_usd: f64,
    pub(crate) competition_score_fast: f64,
    pub(crate) gas_ratio: f64,
    pub(crate) scavenger_try_score: f64,
}

struct PendingExecutionCandidate {
    opportunity: MevOpportunity,
    ev_real_usd: f64,
    p_positive: f64,
    capital_efficiency: f64,
    relay_score: f64,
    context_priority_score: f64,
}

impl PendingExecutionCandidate {
    fn ranking_score(&self) -> f64 {
        self.ev_real_usd.max(0.0)
            * (0.65 + self.p_positive.clamp(0.0, 1.0) * 0.35)
            * (0.70 + self.context_priority_score.clamp(0.0, 1.0) * 0.30)
            * (0.72 + self.capital_efficiency.clamp(0.0, 1.0) * 0.28)
            * (1.0 - self.relay_score.clamp(0.0, 1.0) * 0.20)
    }
}

#[derive(Default)]
struct MicroBatcher {
    opened_at: Option<Instant>,
    candidates: Vec<PendingExecutionCandidate>,
}

impl MicroBatcher {
    fn push(&mut self, candidate: PendingExecutionCandidate) {
        if self.opened_at.is_none() {
            self.opened_at = Some(Instant::now());
        }
        self.candidates.push(candidate);
    }

    fn should_flush(&self) -> bool {
        self.candidates.len() >= MICROBATCH_MAX_CANDIDATES
            || self
                .opened_at
                .map(|opened| opened.elapsed() >= Duration::from_millis(MICROBATCH_WINDOW_MS))
                .unwrap_or(false)
    }

    fn has_pending(&self) -> bool {
        !self.candidates.is_empty()
    }

    fn drain_best(&mut self) -> Option<(PendingExecutionCandidate, usize)> {
        let (best_index, _) =
            self.candidates
                .iter()
                .enumerate()
                .max_by(|(_, left), (_, right)| {
                    left.ranking_score().total_cmp(&right.ranking_score())
                })?;
        let best = self.candidates.swap_remove(best_index);
        if self.candidates.is_empty() {
            self.opened_at = None;
        } else {
            self.opened_at = Some(Instant::now());
        }
        Some((best, self.candidates.len()))
    }
}

#[derive(Debug, Clone, Default)]
struct CandidateLatencyTrace {
    lookup_pending_tx_us: Option<u64>,
    decode_swap_us: Option<u64>,
    context_signal_us: Option<u64>,
    fast_preflight_us: Option<u64>,
    adaptive_preflight_us: Option<u64>,
    payload_build_us: Option<u64>,
    ev_gate_us: Option<u64>,
    quality_gate_us: Option<u64>,
    adaptive_quote_us: Option<u64>,
    total_internal_us: Option<u64>,
}

impl CandidateLatencyTrace {
    fn emit(
        &self,
        config: &Config,
        dashboard: &DashboardHandle,
        victim_tx: H256,
        outcome: &str,
        detail: &str,
    ) {
        if !config.mev.latency_trace {
            return;
        }

        for (stage, duration_us) in self.stage_pairs() {
            dashboard.record_latency(
                stage,
                duration_us.div_ceil(1_000) as u128,
                None,
                Some(&format!(
                    "victim={:?} outcome={} {}us",
                    victim_tx, outcome, duration_us
                )),
            );
        }

        let total_us = self.total_internal_us.unwrap_or_default();
        if outcome == "execution_ready" || total_us >= config.mev.latency_trace_warn_us {
            dashboard.event(
                if outcome == "execution_ready" { "info" } else { "warn" },
                format!(
                    "latency trace victim={:?} outcome={} detail={} total_us={} lookup_us={} decode_us={} context_us={} fast_us={} preflight_us={} payload_us={} ev_us={} quality_us={} adaptive_us={}",
                    victim_tx,
                    outcome,
                    detail,
                    total_us,
                    self.lookup_pending_tx_us.unwrap_or_default(),
                    self.decode_swap_us.unwrap_or_default(),
                    self.context_signal_us.unwrap_or_default(),
                    self.fast_preflight_us.unwrap_or_default(),
                    self.adaptive_preflight_us.unwrap_or_default(),
                    self.payload_build_us.unwrap_or_default(),
                    self.ev_gate_us.unwrap_or_default(),
                    self.quality_gate_us.unwrap_or_default(),
                    self.adaptive_quote_us.unwrap_or_default(),
                ),
            );
        }
    }

    fn stage_pairs(&self) -> Vec<(&'static str, u64)> {
        let mut pairs = Vec::with_capacity(10);
        push_stage_pair(
            &mut pairs,
            "rt.lookup_pending_tx",
            self.lookup_pending_tx_us,
        );
        push_stage_pair(&mut pairs, "rt.decode_swap", self.decode_swap_us);
        push_stage_pair(&mut pairs, "rt.context_signal", self.context_signal_us);
        push_stage_pair(&mut pairs, "rt.fast_preflight", self.fast_preflight_us);
        push_stage_pair(
            &mut pairs,
            "rt.adaptive_preflight",
            self.adaptive_preflight_us,
        );
        push_stage_pair(&mut pairs, "rt.payload_build", self.payload_build_us);
        push_stage_pair(&mut pairs, "rt.ev_gate", self.ev_gate_us);
        push_stage_pair(&mut pairs, "rt.quality_gate", self.quality_gate_us);
        push_stage_pair(&mut pairs, "rt.adaptive_quote", self.adaptive_quote_us);
        push_stage_pair(&mut pairs, "rt.total_internal", self.total_internal_us);
        pairs
    }
}

struct PendingHashTask {
    tx_hash: H256,
    candidate_started: Instant,
    enqueued_at: Instant,
}

#[derive(Debug, Clone, Copy)]
struct RpcLookupPressure {
    available_readers: usize,
    rate_limited_readers: usize,
    failing_readers: usize,
}

impl Default for RpcLookupPressure {
    fn default() -> Self {
        Self {
            available_readers: 1,
            rate_limited_readers: 0,
            failing_readers: 0,
        }
    }
}

struct PendingLookupBackpressure {
    window_started: Instant,
    accepted_in_window: u64,
    pressure_refreshed_at: Instant,
    pressure: RpcLookupPressure,
    dropped_since_event: u64,
    last_event_at: Instant,
}

impl PendingLookupBackpressure {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            window_started: now,
            accepted_in_window: 0,
            pressure_refreshed_at: now - Duration::from_secs(2),
            pressure: RpcLookupPressure::default(),
            dropped_since_event: 0,
            last_event_at: now,
        }
    }

    fn should_enqueue(
        &mut self,
        config: &Config,
        rpc_fleet: &RpcFleet,
        queue_remaining: usize,
        dashboard: &DashboardHandle,
    ) -> bool {
        let now = Instant::now();
        if now.saturating_duration_since(self.window_started) >= Duration::from_secs(1) {
            self.window_started = now;
            self.accepted_in_window = 0;
        }
        if now.saturating_duration_since(self.pressure_refreshed_at) >= Duration::from_millis(900) {
            self.pressure = rpc_lookup_pressure(rpc_fleet);
            self.pressure_refreshed_at = now;
        }

        let max_per_sec = effective_pending_lookup_budget(config, self.pressure);
        let queue_soft_full = queue_remaining <= LOOKUP_DECODE_QUEUE_CAPACITY / 16;
        let allowed = self.accepted_in_window < max_per_sec && !queue_soft_full;
        if allowed {
            self.accepted_in_window = self.accepted_in_window.saturating_add(1);
            return true;
        }

        self.dropped_since_event = self.dropped_since_event.saturating_add(1);
        dashboard.record_opportunity_funnel("tx_lookup_throttled");
        if now.saturating_duration_since(self.last_event_at) >= Duration::from_secs(5) {
            dashboard.record_reject_reason(
                "pending_lookup",
                if queue_soft_full {
                    "queue_backpressure"
                } else {
                    "rpc_budget"
                },
            );
            dashboard.event(
                "warn",
                format!(
                    "pending lookup backpressure shed={} budget_per_sec={} available_rpc={} rate_limited_rpc={} failing_rpc={} queue_remaining={}",
                    self.dropped_since_event,
                    max_per_sec,
                    self.pressure.available_readers,
                    self.pressure.rate_limited_readers,
                    self.pressure.failing_readers,
                    queue_remaining,
                ),
            );
            self.dropped_since_event = 0;
            self.last_event_at = now;
        }
        false
    }
}

struct LookupDecodedCandidate {
    tx: Transaction,
    signal: SwapSignal,
    lane: CandidateLane,
    hour_utc: u8,
    gas_price: U256,
    context_signal: ContextSignal,
    cluster: ClusterKey,
    lookup_latency: Duration,
    candidate_started: Instant,
    latency_trace: CandidateLatencyTrace,
    block_number: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateLane {
    Small,
    Medium,
    Large,
    Universal,
}

impl CandidateLane {
    fn as_str(self) -> &'static str {
        match self {
            CandidateLane::Small => "small",
            CandidateLane::Medium => "medium",
            CandidateLane::Large => "large",
            CandidateLane::Universal => "universal",
        }
    }
}

struct SmallSwapBudget {
    window_started: Instant,
    accepted: u64,
}

static SMALL_SCAVENGER_BUDGET: OnceLock<StdMutex<SmallSwapBudget>> = OnceLock::new();

ethers::contract::abigen!(
    UniswapV2Factory,
    r#"[
        function getPair(address tokenA, address tokenB) external view returns (address pair)
    ]"#,
);

ethers::contract::abigen!(
    UniswapV2Router,
    r#"[
        function factory() external view returns (address factory)
    ]"#,
);

ethers::contract::abigen!(
    UniswapV2Pair,
    r#"[
        function token0() external view returns (address)
        function token1() external view returns (address)
        function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast)
    ]"#,
);

ethers::contract::abigen!(
    UniswapV3Factory,
    r#"[
        function getPool(address tokenA, address tokenB, uint24 fee) external view returns (address pool)
    ]"#,
);

ethers::contract::abigen!(
    UniswapV3Pool,
    r#"[
        function token0() external view returns (address)
        function token1() external view returns (address)
        function liquidity() external view returns (uint128)
        function slot0() external view returns (uint160 sqrtPriceX96, int24 tick, uint16 observationIndex, uint16 observationCardinality, uint16 observationCardinalityNext, uint8 feeProtocol, bool unlocked)
    ]"#,
);

pub async fn run(
    config: Arc<Config>,
    rpc_fleet: Arc<RpcFleet>,
    dashboard: DashboardHandle,
    storage: Storage,
) -> Result<(), Box<dyn std::error::Error>> {
    let ws_urls = config.mempool_ws_urls();
    if ws_urls.is_empty() {
        return Err("fee extraction runtime requires MEMPOOL websocket URL".into());
    }
    if !config.allow_send {
        dashboard.event(
            "warn",
            "fee extraction runtime is in observation mode: ALLOW_SEND=false disables submit",
        );
    }

    let pool_cache = Arc::new(PoolCache::new(config.mev.pool_state_cache_ttl_ms));
    let mut tx_hash_stream = connect_mempool_fan_in(&ws_urls, &dashboard);
    let min_large_swap_wei =
        ethers::utils::parse_ether(config.mev.effective_min_large_swap_eth().to_string())?;
    let min_profit_wei =
        ethers::utils::parse_ether(config.mev.effective_min_net_profit_eth().to_string())?;
    let adaptive = AdaptivePolicy::shared(&config);
    refresh_historical_profiles(&adaptive, &storage, &dashboard);
    spawn_historical_profile_refresher(adaptive.clone(), storage.clone(), dashboard.clone());
    let executor = ExecutionEngine::new(
        config.clone(),
        rpc_fleet.clone(),
        dashboard.clone(),
        adaptive.clone(),
    );
    let lookup_decode_workers = worker_count(LOOKUP_DECODE_WORKERS_MAX);
    let eval_workers = worker_count(EVAL_WORKERS_MAX);
    let (lookup_decode_tx, lookup_decode_rx) =
        mpsc::channel::<PendingHashTask>(LOOKUP_DECODE_QUEUE_CAPACITY);
    let (eval_tx, eval_rx) = mpsc::channel::<LookupDecodedCandidate>(EVAL_QUEUE_CAPACITY);
    let (ready_tx, mut ready_rx) = mpsc::channel::<PendingExecutionCandidate>(EVAL_QUEUE_CAPACITY);
    let lookup_decode_rx = Arc::new(Mutex::new(lookup_decode_rx));
    let eval_rx = Arc::new(Mutex::new(eval_rx));
    let mut batcher = MicroBatcher::default();
    let mut flush_tick = tokio::time::interval(Duration::from_millis(MICROBATCH_WINDOW_MS));

    dashboard.event(
        "info",
        format!(
            "fee extraction runtime connected to {} mode={} min_large_swap={:.3} {} min_profit={:.6} {} lookup_workers={} eval_workers={}",
            ws_urls.join(" | "),
            config.mev.opportunity_mode().as_str(),
            config.mev.effective_min_large_swap_eth(),
            config.native_asset_symbol(),
            config.mev.effective_min_net_profit_eth(),
            config.native_asset_symbol(),
            lookup_decode_workers,
            eval_workers
        ),
    );

    for worker_idx in 0..lookup_decode_workers {
        let rx = lookup_decode_rx.clone();
        let tx = eval_tx.clone();
        let config = config.clone();
        let rpc_fleet = rpc_fleet.clone();
        let adaptive = adaptive.clone();
        let dashboard = dashboard.clone();
        tokio::spawn(async move {
            while let Some(task) = recv_from_shared_channel(&rx).await {
                if dashboard.runtime_paused() {
                    continue;
                }
                dashboard.record_latency(
                    "queue_wait",
                    elapsed_ms_ceil(task.enqueued_at),
                    None,
                    Some("lookup_decode"),
                );
                if let Some(decoded) = process_lookup_decode_task(
                    task,
                    config.clone(),
                    rpc_fleet.clone(),
                    adaptive.clone(),
                    dashboard.clone(),
                )
                .await
                {
                    if tx.send(decoded).await.is_err() {
                        break;
                    }
                }
            }
            dashboard.event(
                "info",
                format!("lookup/decode worker {} stopped", worker_idx),
            );
        });
    }
    drop(eval_tx);

    for worker_idx in 0..eval_workers {
        let rx = eval_rx.clone();
        let tx = ready_tx.clone();
        let config = config.clone();
        let rpc_fleet = rpc_fleet.clone();
        let adaptive = adaptive.clone();
        let dashboard = dashboard.clone();
        let pool_cache = pool_cache.clone();

        tokio::spawn(async move {
            while let Some(candidate) = recv_from_shared_channel(&rx).await {
                if dashboard.runtime_paused() {
                    continue;
                }
                if let Some(ready) = process_evaluation_task(
                    candidate,
                    config.clone(),
                    rpc_fleet.clone(),
                    adaptive.clone(),
                    dashboard.clone(),
                    min_large_swap_wei,
                    min_profit_wei,
                    pool_cache.clone(),
                )
                .await
                {
                    if tx.send(ready).await.is_err() {
                        break;
                    }
                }
            }
            dashboard.event("info", format!("evaluation worker {} stopped", worker_idx));
        });
    }
    drop(ready_tx);

    let mut seen_hashes = HashSet::new();
    let mut seen_order = VecDeque::new();
    let mut pending_lookup_backpressure = PendingLookupBackpressure::new();

    loop {
        tokio::select! {
            Some(tx_hash) = tx_hash_stream.recv() => {
                let scan_started = Instant::now();
                if dashboard.runtime_paused() {
                    continue;
                }
                if mark_seen_tx(&mut seen_hashes, &mut seen_order, tx_hash) {
                    dashboard.record_opportunity_funnel("pending_hashes_received");
                    if !pending_lookup_backpressure.should_enqueue(
                        &config,
                        &rpc_fleet,
                        lookup_decode_tx.capacity(),
                        &dashboard,
                    ) {
                        dashboard.record_latency(
                            "scan_cycle",
                            elapsed_ms_ceil(scan_started),
                            None,
                            Some("mempool_hash_shed"),
                        );
                        continue;
                    }
                    let enqueue_started = Instant::now();
                    let task_started = Instant::now();
                    let send_result = lookup_decode_tx
                        .send(PendingHashTask {
                            tx_hash,
                            candidate_started: task_started,
                            enqueued_at: Instant::now(),
                        })
                        .await;
                    dashboard.record_latency(
                        "enqueue_latency",
                        elapsed_ms_ceil(enqueue_started),
                        None,
                        Some("lookup_decode"),
                    );
                    if send_result.is_err() {
                        break;
                    }
                }
                dashboard.record_latency(
                    "scan_cycle",
                    elapsed_ms_ceil(scan_started),
                    None,
                    Some("mempool_hash"),
                );
            }
            Some(candidate) = ready_rx.recv() => {
                batcher.push(candidate);
                if batcher.should_flush() {
                    flush_candidate_batch(&mut batcher, &executor, &dashboard).await?;
                }
            }
            _ = flush_tick.tick() => {
                if batcher.should_flush() {
                    flush_candidate_batch(&mut batcher, &executor, &dashboard).await?;
                }
            }
            else => break,
        }
    }

    if batcher.has_pending() {
        flush_candidate_batch(&mut batcher, &executor, &dashboard).await?;
    }

    Ok(())
}

fn build_opportunity(
    tx: &Transaction,
    signal: &SwapSignal,
    payload: crate::mev::execution::payload_builder::ExecutionPayload,
    preferred_relay: Option<String>,
) -> MevOpportunity {
    MevOpportunity {
        detected_at: Instant::now(),
        victim_tx: tx.hash,
        victim_transaction: Some(tx.clone()),
        execution_payload: Some(payload),
        router: signal.router,
        token_in: signal.path[0],
        token_out: *signal.path.last().unwrap_or(&signal.path[0]),
        selector: signal.selector,
        preferred_relay,
    }
}

fn connect_mempool_fan_in(
    ws_urls: &[String],
    dashboard: &DashboardHandle,
) -> mpsc::UnboundedReceiver<H256> {
    let (tx, rx) = mpsc::unbounded_channel();
    for ws_url in ws_urls.iter().cloned() {
        if is_blocked_bnb_alchemy_ws(&ws_url) {
            dashboard.event(
                "warn",
                "blocked bnb alchemy mempool ws; using configured non-alchemy feeds only"
                    .to_string(),
            );
            continue;
        }
        let tx = tx.clone();
        let dashboard = dashboard.clone();
        tokio::spawn(async move {
            let mut reconnect_failures = 0u32;
            loop {
                if dashboard.runtime_paused() {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                match Ws::connect(ws_url.clone()).await {
                    Ok(ws) => {
                        reconnect_failures = 0;
                        dashboard.event("info", format!("mempool ws connected {}", ws_url));
                        let provider = Provider::new(ws);
                        let subscribe_result = provider.subscribe_pending_txs().await;
                        match subscribe_result {
                            Ok(mut stream) => {
                                while let Some(hash) = stream.next().await {
                                    if dashboard.runtime_paused() {
                                        break;
                                    }
                                    if tx.send(hash).is_err() {
                                        return;
                                    }
                                }
                                dashboard
                                    .event("warn", format!("mempool ws stream ended {}", ws_url));
                            }
                            Err(err) => {
                                reconnect_failures = reconnect_failures.saturating_add(1);
                                dashboard.event(
                                    "warn",
                                    format!("mempool ws subscribe failed {}: {}", ws_url, err),
                                );
                            }
                        }
                    }
                    Err(err) => {
                        reconnect_failures = reconnect_failures.saturating_add(1);
                        dashboard.event(
                            "warn",
                            format!("mempool ws connect failed {}: {}", ws_url, err),
                        );
                    }
                }
                let delay = mempool_ws_reconnect_delay(&ws_url, reconnect_failures);
                if delay >= Duration::from_secs(10) {
                    dashboard.event(
                        "warn",
                        format!(
                            "mempool ws backoff {}s after {} failures {}",
                            delay.as_secs(),
                            reconnect_failures,
                            ws_url
                        ),
                    );
                }
                tokio::time::sleep(delay).await;
            }
        });
    }
    rx
}

fn mempool_ws_reconnect_delay(ws_url: &str, failures: u32) -> Duration {
    let base_ms = std::env::var("MEMPOOL_WS_RECONNECT_BASE_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(1_500);
    let max_secs = std::env::var("MEMPOOL_WS_RECONNECT_MAX_SECS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or_else(|| {
            if ws_url.to_ascii_lowercase().contains("alchemy.com") {
                300
            } else {
                60
            }
        });
    let exponent = failures.saturating_sub(1).min(7);
    let delay_ms = base_ms.saturating_mul(2u64.saturating_pow(exponent));
    Duration::from_millis(delay_ms).min(Duration::from_secs(max_secs))
}

fn is_blocked_bnb_alchemy_ws(url: &str) -> bool {
    let normalized = url.to_ascii_lowercase();
    normalized.starts_with("wss://bnb-mainnet.g.alchemy.com/")
        || normalized.starts_with("wss://bnb-mainnet.g.alchemy.com:")
}

fn mark_seen_tx(
    seen_hashes: &mut HashSet<H256>,
    seen_order: &mut VecDeque<H256>,
    tx_hash: H256,
) -> bool {
    if seen_hashes.contains(&tx_hash) {
        return false;
    }
    seen_hashes.insert(tx_hash);
    seen_order.push_back(tx_hash);
    while seen_order.len() > 8_192 {
        if let Some(old) = seen_order.pop_front() {
            seen_hashes.remove(&old);
        }
    }
    true
}

async fn lookup_pending_tx_parallel(
    rpc_fleet: Arc<RpcFleet>,
    tx_hash: H256,
) -> Option<Transaction> {
    let mut join_set = JoinSet::new();
    for handle in rpc_fleet.read_candidates(pending_lookup_fanout()) {
        let rpc_fleet = rpc_fleet.clone();
        join_set.spawn(async move {
            rpc_fleet.reserve_read_selection(handle.id);
            let started = Instant::now();
            let result = handle.provider.get_transaction(tx_hash).await;
            match result {
                Ok(Some(tx)) => {
                    rpc_fleet.record_success(
                        handle.id,
                        started.elapsed(),
                        Some(tx.block_number.unwrap_or_default().as_u64()),
                    );
                    Some(tx)
                }
                Ok(None) => {
                    rpc_fleet.record_success(handle.id, started.elapsed(), None);
                    None
                }
                Err(err) => {
                    rpc_fleet
                        .record_failure(handle.id, RpcFleet::classify_failure(&err.to_string()));
                    None
                }
            }
        });
    }

    while let Some(result) = join_set.join_next().await {
        if let Ok(Some(tx)) = result {
            join_set.abort_all();
            return Some(tx);
        }
    }
    None
}

async fn recv_from_shared_channel<T>(rx: &Arc<Mutex<mpsc::Receiver<T>>>) -> Option<T> {
    let mut guard = rx.lock().await;
    guard.recv().await
}

async fn process_lookup_decode_task(
    task: PendingHashTask,
    config: Arc<Config>,
    rpc_fleet: Arc<RpcFleet>,
    adaptive: crate::mev::adaptive::SharedAdaptivePolicy,
    dashboard: DashboardHandle,
) -> Option<LookupDecodedCandidate> {
    let mut latency_trace = CandidateLatencyTrace::default();
    let lookup_started = Instant::now();
    let Some(tx) = lookup_pending_tx_parallel(rpc_fleet.clone(), task.tx_hash).await else {
        dashboard.record_opportunity_funnel("tx_lookup_miss");
        return None;
    };
    dashboard.record_opportunity_funnel("tx_lookup_success");
    latency_trace.lookup_pending_tx_us = Some(elapsed_us(lookup_started));

    dashboard.record_latency(
        "fee_pending_lookup",
        lookup_started.elapsed().as_millis(),
        None,
        None,
    );
    if let Ok(mut model) = adaptive.lock() {
        model.observe_lookup_latency(lookup_started.elapsed().as_millis() as f64);
    }

    let decode_started = Instant::now();
    let min_large_swap_wei =
        ethers::utils::parse_ether(config.mev.effective_min_large_swap_eth().to_string()).ok()?;
    let Some(signal) = decode_relevant_swap(
        &tx,
        &config.monitored_tokens,
        min_large_swap_wei,
        config.mev.opportunity_mode(),
    ) else {
        latency_trace.decode_swap_us = Some(elapsed_us(decode_started));
        latency_trace.total_internal_us = Some(elapsed_us(task.candidate_started));
        dashboard.record_opportunity_funnel("decode_reject");
        if let Some(name) = aggregator_name_from_tx(&tx) {
            dashboard.record_reject_reason("aggregator_decode", name);
            if let Some(sample) = aggregator_intelligence_sample(&config, &tx, task.tx_hash, name) {
                dashboard.record_edge_sample(sample);
            }
            dashboard.event(
                "warn",
                format!(
                    "aggregator flow not decoded tx={} source={}",
                    short_hash(task.tx_hash),
                    name
                ),
            );
            latency_trace.emit(
                &config,
                &dashboard,
                task.tx_hash,
                "reject",
                "aggregator_decode_missing",
            );
        } else {
            dashboard.record_reject_reason("decode", "not_relevant_or_below_min");
            dashboard.record_reject_reason("decode_selector", &decode_reject_selector_reason(&tx));
            if let Some(sample) =
                decode_reject_edge_sample(&config, &tx, task.tx_hash, "not_relevant_or_below_min")
            {
                dashboard.record_edge_sample(sample);
            }
            latency_trace.emit(
                &config,
                &dashboard,
                task.tx_hash,
                "reject",
                "decode_no_signal",
            );
        }
        return None;
    };
    dashboard.record_opportunity_funnel("decode_pass");
    dashboard.record_reject_reason("decode_selector_pass", &decode_reject_selector_reason(&tx));
    latency_trace.decode_swap_us = Some(elapsed_us(decode_started));

    let hour_utc = chrono::Utc::now().hour() as u8;
    let context_started = Instant::now();
    let context_signal = if let Ok(model) = adaptive.lock() {
        model.context_signal(signal.router, hour_utc)
    } else {
        ContextSignal {
            priority_score: 0.50,
            toxicity_score: 0.50,
            samples: 0,
        }
    };
    latency_trace.context_signal_us = Some(elapsed_us(context_started));

    let gas_price = tx.max_fee_per_gas.or(tx.gas_price).unwrap_or_default();
    if gas_price.is_zero() {
        dashboard.record_opportunity_funnel("decode_reject");
        dashboard.record_reject_reason("decode", "zero_gas_price");
        latency_trace.total_internal_us = Some(elapsed_us(task.candidate_started));
        latency_trace.emit(
            &config,
            &dashboard,
            task.tx_hash,
            "reject",
            "zero_gas_price",
        );
        return None;
    }

    let lane = classify_candidate_lane(&signal, min_large_swap_wei);
    dashboard.record_reject_reason("candidate_lane", lane.as_str());
    if lane == CandidateLane::Small
        && config.mev.opportunity_mode() == OpportunityMode::Scavenger
        && !allow_small_scavenger_candidate()
    {
        dashboard.record_opportunity_funnel("small_scavenger_throttled");
        dashboard.record_reject_reason("small_scavenger", "budget_limited");
        latency_trace.total_internal_us = Some(elapsed_us(task.candidate_started));
        latency_trace.emit(
            &config,
            &dashboard,
            task.tx_hash,
            "reject",
            "small_scavenger_budget",
        );
        return None;
    }

    let block_fetch_started = Instant::now();
    let Some(block_number) = get_current_block_parallel(rpc_fleet.clone()).await else {
        dashboard.record_latency(
            "block_fetch",
            elapsed_ms_ceil(block_fetch_started),
            None,
            Some("lookup_decode_fail"),
        );
        dashboard.record_opportunity_funnel("block_lookup_fail");
        latency_trace.total_internal_us = Some(elapsed_us(task.candidate_started));
        latency_trace.emit(&config, &dashboard, task.tx_hash, "reject", "block_lookup");
        return None;
    };
    dashboard.record_latency(
        "block_fetch",
        elapsed_ms_ceil(block_fetch_started),
        None,
        Some("lookup_decode"),
    );
    dashboard.record_opportunity_funnel("block_lookup_success");

    let cluster = ClusterKey {
        router: signal.router,
        token_in: signal.path[0],
        token_out: *signal.path.last().unwrap_or(&signal.path[0]),
        selector: signal.selector,
    };

    Some(LookupDecodedCandidate {
        tx,
        signal,
        lane,
        hour_utc,
        gas_price,
        context_signal,
        cluster,
        lookup_latency: lookup_started.elapsed(),
        candidate_started: task.candidate_started,
        latency_trace,
        block_number,
    })
}

fn pending_lookup_fanout() -> usize {
    std::env::var("MEV_PENDING_LOOKUP_FANOUT")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(1)
        .clamp(1, 3)
}

// NOVA FUNÇÃO auxiliar
fn classify_candidate_lane(signal: &SwapSignal, min_large_swap_wei: U256) -> CandidateLane {
    if matches!(
        signal.selector,
        UNIVERSAL_ROUTER_EXECUTE | UNIVERSAL_ROUTER_EXECUTE_NO_DEADLINE
    ) {
        return CandidateLane::Universal;
    }

    let medium_floor = ethers::utils::parse_ether("0.05")
        .unwrap_or_else(|_| U256::from(50_000_000_000_000_000u64));
    let large_floor = ethers::utils::parse_ether("0.5")
        .unwrap_or_else(|_| U256::from(500_000_000_000_000_000u64));
    let medium_threshold = u256_max(
        min_large_swap_wei.saturating_mul(U256::from(25u64)),
        medium_floor,
    );
    let large_threshold = u256_max(
        min_large_swap_wei.saturating_mul(U256::from(250u64)),
        large_floor,
    );

    if signal.notional_wei >= large_threshold {
        CandidateLane::Large
    } else if signal.notional_wei >= medium_threshold {
        CandidateLane::Medium
    } else {
        CandidateLane::Small
    }
}

fn allow_small_scavenger_candidate() -> bool {
    let budget = SMALL_SCAVENGER_BUDGET.get_or_init(|| {
        StdMutex::new(SmallSwapBudget {
            window_started: Instant::now(),
            accepted: 0,
        })
    });
    let Ok(mut guard) = budget.lock() else {
        return true;
    };
    let now = Instant::now();
    if now.saturating_duration_since(guard.window_started) >= Duration::from_secs(1) {
        guard.window_started = now;
        guard.accepted = 0;
    }
    if guard.accepted < SMALL_SCAVENGER_BUDGET_PER_SEC {
        guard.accepted = guard.accepted.saturating_add(1);
        true
    } else {
        false
    }
}

fn u256_max(left: U256, right: U256) -> U256 {
    if left > right {
        left
    } else {
        right
    }
}

fn rpc_lookup_pressure(rpc_fleet: &RpcFleet) -> RpcLookupPressure {
    let mut pressure = RpcLookupPressure::default();
    pressure.available_readers = 0;
    for endpoint in rpc_fleet.snapshot() {
        if endpoint.disabled {
            continue;
        }
        if endpoint.rate_limit_failures > 0 {
            pressure.rate_limited_readers = pressure.rate_limited_readers.saturating_add(1);
        }
        if endpoint.failures > 0 || endpoint.timeout_failures > 0 || endpoint.stale_failures > 0 {
            pressure.failing_readers = pressure.failing_readers.saturating_add(1);
        }
        if endpoint.cooldown_remaining_secs == 0 && endpoint.rate_limit_failures == 0 {
            pressure.available_readers = pressure.available_readers.saturating_add(1);
        }
    }
    pressure
}

fn effective_pending_lookup_budget(config: &Config, pressure: RpcLookupPressure) -> u64 {
    let configured = std::env::var("MEV_PENDING_LOOKUP_MAX_PER_SEC")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok());
    let base = configured.unwrap_or_else(|| match config.mev.opportunity_mode() {
        OpportunityMode::Conservative => 60,
        OpportunityMode::Aggressive => 90,
        OpportunityMode::Scavenger => 25,
    });
    let healthy_reader_cap = match config.mev.opportunity_mode() {
        OpportunityMode::Scavenger => pressure.available_readers.max(1) as u64 * 25,
        OpportunityMode::Aggressive => pressure.available_readers.max(1) as u64 * 60,
        OpportunityMode::Conservative => pressure.available_readers.max(1) as u64 * 40,
    };
    let base = if configured.is_some() {
        base.clamp(1, 1_000)
    } else {
        base.min(healthy_reader_cap)
    };

    let pressure_multiplier = if configured.is_some() {
        if pressure.available_readers == 0 {
            0.05
        } else if pressure.rate_limited_readers > 0 {
            0.25
        } else if pressure.failing_readers > pressure.available_readers {
            0.60
        } else {
            1.0
        }
    } else if pressure.available_readers == 0 {
        0.03
    } else if pressure.available_readers <= 1 && pressure.rate_limited_readers > 0 {
        0.18
    } else if pressure.available_readers <= 1 || pressure.rate_limited_readers > 0 {
        0.28
    } else if pressure.failing_readers > pressure.available_readers {
        0.45
    } else {
        1.0
    };

    let floor = if pressure.available_readers == 0 {
        4.0
    } else {
        10.0
    };
    ((base as f64) * pressure_multiplier).round().max(floor) as u64
}

async fn get_current_block_parallel(rpc_fleet: Arc<RpcFleet>) -> Option<u64> {
    let mut join_set = JoinSet::new();
    for handle in rpc_fleet.read_candidates(block_lookup_fanout()) {
        let rpc_fleet = rpc_fleet.clone();
        let provider = handle.provider.clone();
        join_set.spawn(async move {
            rpc_fleet.reserve_read_selection(handle.id);
            let started = Instant::now();
            match provider.get_block_number().await {
                Ok(block) => {
                    let block = block.as_u64();
                    rpc_fleet.record_success(handle.id, started.elapsed(), Some(block));
                    Some(block)
                }
                Err(err) => {
                    rpc_fleet
                        .record_failure(handle.id, RpcFleet::classify_failure(&err.to_string()));
                    None
                }
            }
        });
    }

    while let Some(result) = join_set.join_next().await {
        if let Ok(Some(block)) = result {
            join_set.abort_all();
            return Some(block);
        }
    }
    None
}

fn block_lookup_fanout() -> usize {
    std::env::var("MEV_BLOCK_LOOKUP_FANOUT")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(1)
        .clamp(1, 3)
}

/// Valida uma oportunidade MEV usando um fork do estado atual da blockchain
async fn validate_with_evm_preflight(
    payload: &ExecutionPayload,
    victim_tx: &Transaction,
    block_number: u64,
    pool_state: &AmmState,
    config: &Config,
    rpc_fleet: &Arc<RpcFleet>,
    dashboard: &DashboardHandle,
) -> Result<EvmPreflightResult, String> {
    // Obter um provider para o fork
    let _ = (rpc_fleet, dashboard);

    // Criar o estado inicial baseado no block_number
    let mut account_states = std::collections::HashMap::new();

    // Adicionar o estado do pool antes da execução
    match pool_state {
        AmmState::UniswapV2(pool) => {
            let mut account = AccountState::empty();
            account.storage.insert(U256::from(0), pool.reserve0);
            account.storage.insert(U256::from(1), pool.reserve1);
            account_states.insert(pool.pair, account);
        }
        AmmState::UniswapV3(pool) => {
            let mut account = AccountState::empty();
            account.storage.insert(U256::from(0), pool.sqrt_price_x96);
            account.storage.insert(U256::from(1), pool.liquidity);
            account
                .storage
                .insert(U256::from(2), U256::from(pool.current_tick.max(0) as u64));
            account_states.insert(pool.pool, account);
        }
    }

    if let Some(executor) = config.mev.mev_executor {
        account_states
            .entry(executor)
            .or_insert_with(AccountState::empty);
    }
    if let Some(executor) = config.mev.mev_executor_v3 {
        account_states
            .entry(executor)
            .or_insert_with(AccountState::empty);
    }
    account_states
        .entry(config.profit_address)
        .or_insert_with(AccountState::empty);

    let mock_tx = Transaction {
        hash: victim_tx.hash,
        nonce: victim_tx.nonce,
        block_hash: None,
        block_number: None,
        transaction_index: None,
        from: config.executor_address,
        to: Some(payload.target_contract),
        value: payload.value,
        gas_price: Some(
            victim_tx
                .max_fee_per_gas
                .or(victim_tx.gas_price)
                .unwrap_or_default(),
        ),
        gas: U256::from(payload.gas_limit),
        input: payload.calldata.clone(),
        chain_id: Some(U256::from(config.chain_id)),
        ..Default::default()
    };

    StateSimulator::evm_preflight_execution(config, &mock_tx, block_number, account_states).await
}

async fn process_evaluation_task(
    mut candidate: LookupDecodedCandidate,
    config: Arc<Config>,
    rpc_fleet: Arc<RpcFleet>,
    adaptive: crate::mev::adaptive::SharedAdaptivePolicy,
    dashboard: DashboardHandle,
    min_large_swap_wei: U256,
    min_profit_wei: U256,
    pool_cache: Arc<PoolCache>, // NOVO PARÂMETRO
) -> Option<PendingExecutionCandidate> {
    let tx_hash = candidate.tx.hash;
    let min_large_swap_wei =
        ethers::utils::parse_ether(config.mev.effective_min_large_swap_eth().to_string())
            .unwrap_or(min_large_swap_wei);
    let min_profit_wei =
        ethers::utils::parse_ether(config.mev.effective_min_net_profit_eth().to_string())
            .unwrap_or(min_profit_wei);
    dashboard.record_reject_reason("eval_lane", candidate.lane.as_str());

    let fast_preflight_started = Instant::now();
    let fast_gate = fast_preflight_gate(
        &candidate.signal,
        candidate.gas_price,
        min_large_swap_wei,
        &config,
        candidate.context_signal,
    );
    candidate.latency_trace.fast_preflight_us = Some(elapsed_us(fast_preflight_started));

    if let Some(max_gas_price_gwei) = config.mev.max_gas_price_gwei {
        let gas_price_gwei = wei_to_gwei_f64(candidate.gas_price);
        let notional_eth = wei_to_eth_f64(candidate.signal.notional_wei);
        let adaptive_cap_gwei = adaptive_gas_cap_gwei(
            fast_gate.ev_upper_bound_usd,
            notional_eth,
            max_gas_price_gwei as f64,
        );
        if gas_price_gwei > adaptive_cap_gwei {
            candidate.latency_trace.total_internal_us =
                Some(elapsed_us(candidate.candidate_started));
            dashboard.record_reject_reason("gas_price_cap", "victim_gas_price_above_adaptive_cap");
            dashboard.event(
                "warn",
                format!(
                    "opportunity skipped victim={:?}: gas price {:.2} gwei above adaptive cap {:.0} gwei ev_upper={:.4}usd hard_cap={} gwei",
                    tx_hash,
                    gas_price_gwei,
                    adaptive_cap_gwei,
                    fast_gate.ev_upper_bound_usd,
                    max_gas_price_gwei
                ),
            );
            candidate.latency_trace.emit(
                &config,
                &dashboard,
                tx_hash,
                "reject",
                "gas_price_adaptive_cap",
            );
            return None;
        }
    }

    if !fast_gate.should_continue {
        candidate.latency_trace.total_internal_us = Some(elapsed_us(candidate.candidate_started));
        dashboard.record_opportunity_funnel("fast_preflight_reject");
        if let Some(reason) = fast_gate.reject_reason {
            dashboard.record_reject_reason("fast_preflight", reason);
        }
        candidate.latency_trace.emit(
            &config,
            &dashboard,
            tx_hash,
            "reject",
            fast_gate.reject_reason.unwrap_or("fast_preflight"),
        );
        return None;
    }
    dashboard.record_opportunity_funnel("fast_preflight_pass");

    if let Ok(mut model) = adaptive.lock() {
        model.observe_candidate_flow(
            candidate.cluster,
            candidate.signal.notional_wei,
            candidate.gas_price,
        );
    }

    if config.mev.opportunity_mode() == OpportunityMode::Scavenger {
        candidate.latency_trace.adaptive_preflight_us = Some(0);
        dashboard.record_opportunity_funnel("adaptive_preflight_pass");
        dashboard.set_market_regime("scavenger");
    } else {
        let adaptive_preflight_started = Instant::now();
        let preflight = if let Ok(mut model) = adaptive.lock() {
            model.preflight_score(PreflightInput {
                cluster: candidate.cluster,
                notional_eth: wei_to_eth_f64(candidate.signal.notional_wei),
                gas_price_wei: candidate.gas_price,
                path_len: candidate.signal.path_len(),
            })
        } else {
            return None;
        };
        candidate.latency_trace.adaptive_preflight_us =
            Some(elapsed_us(adaptive_preflight_started));
        dashboard.set_market_regime(preflight.regime.as_str());

        let preflight_override = should_override_preflight_reject(&config, &preflight);
        if !preflight.should_continue && !preflight_override {
            candidate.latency_trace.total_internal_us =
                Some(elapsed_us(candidate.candidate_started));
            dashboard.record_opportunity_funnel("adaptive_preflight_reject");
            if let Some(reason) = preflight.reject_reason {
                dashboard.record_reject_reason("preflight", reason);
            }
            candidate.latency_trace.emit(
                &config,
                &dashboard,
                tx_hash,
                "reject",
                preflight.reject_reason.unwrap_or("preflight"),
            );
            return None;
        }
        dashboard.record_opportunity_funnel("adaptive_preflight_pass");
        if !preflight.should_continue && preflight_override {
            dashboard.event(
                "warn",
                format!(
                    "preflight bypassed mode={} tx={} reason={}",
                    config.mev.opportunity_mode().as_str(),
                    short_hash(tx_hash),
                    preflight.reject_reason.unwrap_or("preflight"),
                ),
            );
        }
    }

    let payload_started = Instant::now();
    let payload = match build_payload_with_fallback_parallel(
        rpc_fleet.clone(),
        config.clone(),
        candidate.signal.clone(),
        candidate.gas_price,
        candidate.context_signal,
        pool_cache.clone(),
        candidate.block_number,
    )
    .await
    {
        Ok(payload) => payload,
        Err(err) => {
            dashboard.record_opportunity_funnel("payload_reject");
            if let Some(sample) = extract_edge_sample(&err).map(|sample| {
                enrich_edge_explainer_sample(sample, tx_hash, &candidate.signal, &fast_gate)
            }) {
                dashboard.record_edge_sample(sample);
            }
            let human_reason = human_payload_error(&err);
            dashboard.record_reject_reason("payload_build", &human_reason);
            dashboard.event(
                "warn",
                format!(
                    "payload blocked mode={} tx={} reason={}",
                    config.mev.opportunity_mode().as_str(),
                    short_hash(tx_hash),
                    human_reason
                ),
            );
            candidate.latency_trace.payload_build_us = Some(elapsed_us(payload_started));
            candidate.latency_trace.total_internal_us =
                Some(elapsed_us(candidate.candidate_started));
            candidate
                .latency_trace
                .emit(&config, &dashboard, tx_hash, "reject", "payload_build");
            return None;
        }
    };
    let economic_payload = config.mev.opportunity_mode() != OpportunityMode::Scavenger
        || scavenger_payload_has_economic_edge(&config, &payload, candidate.gas_price);
    if economic_payload {
        dashboard.record_opportunity_funnel("payload_built");
    } else {
        dashboard.record_opportunity_funnel("payload_reject");
        dashboard.record_reject_reason("payload_build", "scavenger_dust_edge_shadow_only");
    }
    if let Some(mut sample) = payload
        .edge_metadata
        .clone()
        .map(|sample| enrich_edge_explainer_sample(sample, tx_hash, &candidate.signal, &fast_gate))
    {
        append_gas_sniper_shadow_metrics(&mut sample, &payload, candidate.gas_price);
        if !economic_payload {
            sample.status = "shadow_candidate".to_string();
            sample.reason = "dust edge below economic floor".to_string();
        }
        dashboard.record_edge_sample(sample);
    }
    candidate.latency_trace.payload_build_us = Some(elapsed_us(payload_started));

    // WAR LEVEL: EVM preflight validation (opcional, ativado por env)
    if std::env::var("MEV_EVM_PREFLIGHT_ENABLED")
        .unwrap_or_default()
        .eq_ignore_ascii_case("true")
        && config.mev.opportunity_mode() != OpportunityMode::Scavenger
    {
        let preflight_started = std::time::Instant::now();

        // Usar o estado real do pool que está no payload
        let pool_state = &payload.pool_state_before;

        // Obter a transação da vítima
        let victim_tx = &candidate.tx;

        // Chamar a validação EVM
        match validate_with_evm_preflight(
            &payload,
            victim_tx,
            candidate.block_number,
            pool_state,
            &config,
            &rpc_fleet,
            &dashboard,
        )
        .await
        {
            Ok(preflight_result) => {
                let preflight_elapsed = preflight_started.elapsed();
                candidate.latency_trace.adaptive_preflight_us = Some(elapsed_us(preflight_started));

                if !preflight_result.success {
                    candidate.latency_trace.total_internal_us =
                        Some(elapsed_us(candidate.candidate_started));
                    dashboard.record_reject_reason(
                        "evm_preflight",
                        preflight_result
                            .revert_reason
                            .as_deref()
                            .unwrap_or("preflight_failed"),
                    );
                    candidate.latency_trace.emit(
                        &config,
                        &dashboard,
                        tx_hash,
                        "reject",
                        preflight_result
                            .revert_reason
                            .as_deref()
                            .unwrap_or("evm_preflight"),
                    );
                    return None;
                }

                dashboard.event(
                    "info",
                    format!(
                        "evm preflight passed victim={:?} execution_cost_gas={} simulated_profit_wei={} preflight_ms={}",
                        tx_hash,
                        preflight_result.gas_used,
                        preflight_result.profit_wei,
                        preflight_elapsed.as_millis()
                    ),
                );
            }
            Err(err) => {
                candidate.latency_trace.total_internal_us =
                    Some(elapsed_us(candidate.candidate_started));
                dashboard.record_reject_reason("evm_preflight_error", &err);
                candidate.latency_trace.emit(
                    &config,
                    &dashboard,
                    tx_hash,
                    "reject",
                    "evm_preflight_error",
                );
                return None;
            }
        }
    }

    if config.mev.opportunity_mode() == OpportunityMode::Scavenger {
        if !economic_payload {
            candidate.latency_trace.ev_gate_us = Some(0);
            candidate.latency_trace.total_internal_us =
                Some(elapsed_us(candidate.candidate_started));
            dashboard.record_opportunity_funnel("ev_gate_reject");
            candidate.latency_trace.emit(
                &config,
                &dashboard,
                tx_hash,
                "reject",
                "scavenger_dust_edge",
            );
            return None;
        }
        let sanity_started = Instant::now();
        if let Err(reason) =
            passes_scavenger_sanity_gate(&config, &payload, candidate.lookup_latency)
        {
            candidate.latency_trace.ev_gate_us = Some(elapsed_us(sanity_started));
            candidate.latency_trace.total_internal_us =
                Some(elapsed_us(candidate.candidate_started));
            dashboard.record_opportunity_funnel("ev_gate_reject");
            dashboard.record_reject_reason("scavenger_sanity", reason);
            candidate
                .latency_trace
                .emit(&config, &dashboard, tx_hash, "reject", reason);
            return None;
        }
        candidate.latency_trace.ev_gate_us = Some(elapsed_us(sanity_started));
        candidate.latency_trace.quality_gate_us = Some(0);
        candidate.latency_trace.adaptive_quote_us = Some(0);
        dashboard.record_opportunity_funnel("ev_gate_pass");
        dashboard.record_opportunity_funnel("adaptive_quote_pass");

        let expected_profit_usd =
            wei_to_eth_f64(payload.expected_profit_wei) * config.mev.eth_usd_price;
        let opportunity = build_opportunity(&candidate.tx, &candidate.signal, payload, None);
        let capital_efficiency = opportunity
            .execution_payload
            .as_ref()
            .map(|payload| {
                let capital_eth = wei_to_eth_f64(payload.capital_committed_wei).max(1e-9);
                (expected_profit_usd / capital_eth).max(0.0) / config.mev.eth_usd_price.max(1.0)
            })
            .unwrap_or_default()
            .clamp(0.0, 1.0);

        candidate.latency_trace.total_internal_us = Some(elapsed_us(candidate.candidate_started));
        candidate.latency_trace.emit(
            &config,
            &dashboard,
            tx_hash,
            "execution_ready",
            "scavenger_sanity_fast_path",
        );
        dashboard.record_opportunity_funnel("execution_ready");
        dashboard.event(
            "info",
            format!(
                "scavenger execution ready tx={} try_score={:.2} gross={:.6}{} impact={}bps path_len={} gate=sanity",
                short_hash(tx_hash),
                fast_gate.scavenger_try_score,
                wei_to_eth_f64(
                    opportunity
                        .execution_payload
                        .as_ref()
                        .map(|payload| payload.expected_profit_wei)
                        .unwrap_or_default()
                ),
                config.native_asset_symbol(),
                opportunity
                    .execution_payload
                    .as_ref()
                    .map(|payload| payload.price_impact_bps)
                    .unwrap_or_default(),
                candidate.signal.path_len()
            ),
        );

        return Some(PendingExecutionCandidate {
            opportunity,
            ev_real_usd: expected_profit_usd,
            p_positive: (0.45 + fast_gate.scavenger_try_score * 0.50).clamp(0.05, 0.98),
            capital_efficiency,
            relay_score: fast_gate.gas_ratio.clamp(0.0, 1.0),
            context_priority_score: candidate.context_signal.priority_score,
        });
    }

    let ev_gate_started = Instant::now();
    if !passes_ev_gate(
        &config,
        &payload,
        &candidate.signal,
        candidate.lookup_latency,
        min_profit_wei,
    ) {
        candidate.latency_trace.ev_gate_us = Some(elapsed_us(ev_gate_started));
        candidate.latency_trace.total_internal_us = Some(elapsed_us(candidate.candidate_started));
        dashboard.record_opportunity_funnel("ev_gate_reject");
        candidate
            .latency_trace
            .emit(&config, &dashboard, tx_hash, "reject", "ev_gate");
        return None;
    }
    dashboard.record_opportunity_funnel("ev_gate_pass");
    candidate.latency_trace.ev_gate_us = Some(elapsed_us(ev_gate_started));

    let execution_cost_wei = candidate
        .gas_price
        .saturating_mul(U256::from(payload.gas_limit))
        .saturating_mul(U256::from(config.mev.gas_safety_margin_bps))
        / U256::from(10_000u64);

    let quality_gate_started = Instant::now();
    if !passes_quality_gate(&config, &payload, execution_cost_wei) {
        candidate.latency_trace.quality_gate_us = Some(elapsed_us(quality_gate_started));
        candidate.latency_trace.total_internal_us = Some(elapsed_us(candidate.candidate_started));
        candidate
            .latency_trace
            .emit(&config, &dashboard, tx_hash, "reject", "quality_gate");
        return None;
    }
    candidate.latency_trace.quality_gate_us = Some(elapsed_us(quality_gate_started));

    let adaptive_quote_started = Instant::now();
    let quote = if let Ok(mut model) = adaptive.lock() {
        model.quote_for_relays(
            AdaptiveQuoteInput {
                cluster: candidate.cluster,
                pair: payload.pair,
                hour_utc: candidate.hour_utc,
                context_priority_score: candidate.context_signal.priority_score,
                context_toxicity_score: candidate.context_signal.toxicity_score,
                expected_profit_wei: payload.expected_profit_wei,
                execution_cost_wei,
                gas_price_wei: candidate.gas_price,
                lookup_latency_ms: candidate.lookup_latency.as_millis() as f64,
                notional_eth: wei_to_eth_f64(candidate.signal.notional_wei),
                price_impact_bps: payload.price_impact_bps,
                relay_pressure_override: None,
            },
            &config.builder_relays,
        )
    } else {
        return None;
    };
    candidate.latency_trace.adaptive_quote_us = Some(elapsed_us(adaptive_quote_started));
    dashboard.set_market_regime(quote.regime.as_str());

    let mode_override = should_override_adaptive_reject(&config, &quote);
    if !quote.should_execute && !mode_override {
        candidate.latency_trace.total_internal_us = Some(elapsed_us(candidate.candidate_started));
        dashboard.record_opportunity_funnel("adaptive_quote_reject");
        if let Some(reason) = quote.reject_reason {
            dashboard.record_reject_reason("adaptive", reason);
        }
        candidate.latency_trace.emit(
            &config,
            &dashboard,
            tx_hash,
            "reject",
            quote.reject_reason.as_deref().unwrap_or("adaptive"),
        );
        return None;
    }
    dashboard.record_opportunity_funnel("adaptive_quote_pass");
    if !quote.should_execute && mode_override {
        if config.mev.opportunity_mode() != OpportunityMode::Scavenger {
            dashboard.event(
                "warn",
                format!(
                    "adaptive bypassed mode={} tx={} reason={}",
                    config.mev.opportunity_mode().as_str(),
                    short_hash(tx_hash),
                    quote.reject_reason.unwrap_or("adaptive"),
                ),
            );
        }
    }

    dashboard.event(
        "info",
        format!(
            "adaptive gate passed victim={:?} regime={} relay={} ev_real={:.2}usd threshold={:.2}usd p={:.2} comp={:.2} risk={:.2} builder={:.2} density={:.2} cluster={:.2} latency={:.2} gas_pressure={:.2} comp_penalty={:.2}usd risk_penalty={:.2}usd path_penalty={:.2}usd context_toxicity={:.2}",
            tx_hash,
            quote.regime.as_str(),
            quote.selected_relay.as_deref().unwrap_or("unknown"),
            quote.ev_real_usd,
            quote.threshold_dynamic_usd,
            quote.p_positive,
            quote.competition_score,
            quote.risk_score,
            quote.builder_pressure,
            quote.mempool_density,
            quote.cluster_heat,
            quote.latency_penalty,
            quote.gas_pressure,
            quote.competition_penalty_usd,
            quote.risk_penalty_usd,
            quote.path_penalty_usd,
            quote.context_toxicity_score
        ),
    );

    let opportunity = build_opportunity(
        &candidate.tx,
        &candidate.signal,
        payload,
        quote.selected_relay.clone(),
    );

    let capital_efficiency = opportunity
        .execution_payload
        .as_ref()
        .map(|payload| {
            let capital_eth = wei_to_eth_f64(payload.capital_committed_wei).max(1e-9);
            (quote.ev_real_usd / capital_eth).max(0.0) / config.mev.eth_usd_price.max(1.0)
        })
        .unwrap_or_default()
        .clamp(0.0, 1.0);

    candidate.latency_trace.total_internal_us = Some(elapsed_us(candidate.candidate_started));
    candidate.latency_trace.emit(
        &config,
        &dashboard,
        tx_hash,
        "execution_ready",
        "adaptive_passed",
    );
    dashboard.record_opportunity_funnel("execution_ready");

    Some(PendingExecutionCandidate {
        opportunity,
        ev_real_usd: quote.ev_real_usd,
        p_positive: quote.p_positive,
        capital_efficiency,
        relay_score: quote.builder_pressure,
        context_priority_score: quote.context_priority_score,
    })
}

fn should_override_preflight_reject(
    config: &Config,
    preflight: &crate::mev::adaptive::PreflightQuote,
) -> bool {
    match config.mev.opportunity_mode() {
        crate::config::OpportunityMode::Conservative => false,
        crate::config::OpportunityMode::Aggressive => {
            preflight.upper_bound_ev_usd >= config.mev.effective_min_profit_usd() * 0.45
                && preflight.preflight_score >= 0.12
                && preflight.gas_pressure <= 1.05
        }
        crate::config::OpportunityMode::Scavenger => true,
    }
}

fn should_override_adaptive_reject(
    config: &Config,
    quote: &crate::mev::adaptive::AdaptiveQuote,
) -> bool {
    match config.mev.opportunity_mode() {
        crate::config::OpportunityMode::Conservative => false,
        crate::config::OpportunityMode::Aggressive => {
            quote.ev_real_usd >= config.mev.effective_min_profit_usd()
                && quote.p_positive >= 0.20
                && quote.risk_score <= 0.95
                && quote.gas_pressure <= 1.00
        }
        crate::config::OpportunityMode::Scavenger => true,
    }
}

pub(crate) fn passes_quality_gate(
    config: &Config,
    payload: &crate::mev::execution::payload_builder::ExecutionPayload,
    execution_cost_wei: U256,
) -> bool {
    if config.mev.opportunity_mode() == OpportunityMode::Scavenger {
        return !payload.expected_profit_wei.is_zero()
            && payload.price_impact_bps <= scavenger_quality_price_impact_cap_bps(config);
    }

    let roi = roi_bps(payload.expected_profit_wei, execution_cost_wei);
    let impact_score = ((payload.price_impact_bps as f64
        / config.mev.effective_max_price_impact_bps().max(1) as f64)
        * 100.0)
        .clamp(0.0, 100.0) as u16;
    roi >= config.mev.effective_min_roi_bps() && impact_score <= 100
}

fn passes_scavenger_sanity_gate(
    config: &Config,
    payload: &crate::mev::execution::payload_builder::ExecutionPayload,
    lookup_latency: std::time::Duration,
) -> Result<(), &'static str> {
    if payload.tx.is_empty() || payload.calldata.is_empty() {
        return Err("scavenger_empty_payload");
    }
    if payload.target_contract == Address::zero() {
        return Err("scavenger_missing_target");
    }
    if payload.capital_committed_wei.is_zero() {
        return Err("scavenger_zero_capital");
    }
    if payload.expected_profit_wei.is_zero() {
        return Err("scavenger_no_positive_gross_edge");
    }
    if payload.gas_limit > config.mev.max_gas_per_tx {
        return Err("scavenger_gas_limit_above_cap");
    }
    if payload.price_impact_bps > scavenger_quality_price_impact_cap_bps(config) {
        return Err("scavenger_price_impact_above_cap");
    }
    if lookup_latency.as_millis()
        > u128::from(
            config
                .mev
                .effective_max_pending_age_ms()
                .saturating_mul(3)
                .max(1),
        )
    {
        return Err("scavenger_lookup_stale");
    }
    Ok(())
}

fn scavenger_payload_has_economic_edge(
    config: &Config,
    payload: &crate::mev::execution::payload_builder::ExecutionPayload,
    gas_price: U256,
) -> bool {
    if payload.expected_profit_wei.is_zero() {
        return false;
    }
    let gas_cost_wei = gas_price.saturating_mul(U256::from(payload.gas_limit));
    let min_gas_fraction_bps = std::env::var("MEV_SCAVENGER_MIN_GAS_FRACTION_BPS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(100)
        .clamp(1, 10_000);
    let min_edge_wei =
        gas_cost_wei.saturating_mul(U256::from(min_gas_fraction_bps)) / U256::from(10_000u64);
    let min_edge_usd = std::env::var("MEV_SCAVENGER_MIN_ECONOMIC_EDGE_USD")
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .unwrap_or(0.0005)
        .max(0.0);
    let min_edge_native = min_edge_usd / config.mev.eth_usd_price.max(0.000_001);
    let min_edge_from_usd = ethers::utils::parse_ether(format!("{:.18}", min_edge_native))
        .unwrap_or_else(|_| U256::zero());
    let floor = min_edge_wei.max(min_edge_from_usd);
    debug!(
        expected_profit_wei = %payload.expected_profit_wei,
        gas_cost_wei = %gas_cost_wei,
        min_gas_fraction_bps,
        min_edge_usd,
        min_edge_wei = %min_edge_wei,
        min_edge_from_usd = %min_edge_from_usd,
        floor_wei = %floor,
        has_edge = payload.expected_profit_wei >= floor,
        "scavenger edge check"
    );
    payload.expected_profit_wei >= floor
}

fn scavenger_quality_price_impact_cap_bps(config: &Config) -> u64 {
    config
        .mev
        .effective_max_price_impact_bps()
        .saturating_mul(15)
        .clamp(600, 4_000)
}

pub(crate) fn fast_preflight_gate(
    signal: &SwapSignal,
    gas_price: U256,
    min_large_swap_wei: U256,
    config: &Config,
    context_signal: ContextSignal,
) -> FastPreflightDecision {
    if signal.path_len() < 2 {
        return FastPreflightDecision {
            should_continue: false,
            reject_reason: Some("invalid_path"),
            ev_upper_bound_usd: 0.0,
            estimated_gas_cost_usd: 0.0,
            competition_score_fast: 1.0,
            gas_ratio: 0.0,
            scavenger_try_score: 0.0,
        };
    }
    if signal.notional_wei < min_large_swap_wei {
        return FastPreflightDecision {
            should_continue: false,
            reject_reason: Some("notional_below_min"),
            ev_upper_bound_usd: 0.0,
            estimated_gas_cost_usd: 0.0,
            competition_score_fast: 1.0,
            gas_ratio: 0.0,
            scavenger_try_score: 0.0,
        };
    }

    let notional_eth = wei_to_eth_f64(signal.notional_wei);
    let notional_usd = notional_eth * config.mev.eth_usd_price;
    let path_len = signal.path_len();
    let gas_baseline_gwei = heuristic_gas_baseline_gwei(config);
    let gas_price_gwei = wei_to_gwei_f64(gas_price);
    let gas_ratio = (gas_price_gwei / gas_baseline_gwei.max(1e-9)).max(0.0);
    let size_bucket = notional_size_bucket(notional_eth, config.mev.effective_min_large_swap_eth());
    let selector_factor = selector_heuristic_factor(signal.selector);
    let path_penalty = fast_path_penalty(path_len);
    let size_factor = match size_bucket {
        0 => 0.00022,
        1 => 0.00038,
        _ => 0.00060,
    };
    let heuristic_factor = (selector_factor
        * size_factor
        * (1.0 - path_penalty)
        * (0.88 + context_signal.priority_score.clamp(0.0, 1.0) * 0.18)
        * (1.0 - context_signal.toxicity_score.clamp(0.0, 1.0) * 0.20))
        .max(0.00005);
    let estimated_gas_cost_usd = wei_to_eth_f64(
        gas_price.saturating_mul(U256::from(
            config
                .estimated_exec_gas
                .saturating_add(config.estimated_bundle_overhead_gas)
                .max(180_000),
        )),
    ) * config.mev.eth_usd_price;
    let ev_upper_bound_usd = notional_usd * heuristic_factor - estimated_gas_cost_usd;

    if config.mev.opportunity_mode() == OpportunityMode::Scavenger {
        let scavenger_try_score =
            scavenger_fast_try_score(signal, gas_ratio, notional_eth, config, context_signal);
        let reject_reason = if scavenger_try_score < 0.18 && gas_ratio > 1.45 {
            Some("scavenger_score_below_cheap_threshold")
        } else {
            None
        };
        return FastPreflightDecision {
            should_continue: reject_reason.is_none(),
            reject_reason,
            ev_upper_bound_usd,
            estimated_gas_cost_usd,
            competition_score_fast: 1.0 - scavenger_try_score,
            gas_ratio,
            scavenger_try_score,
        };
    }

    let gas_pressure = ((gas_ratio - 1.0) / 0.8).clamp(0.0, 1.0);
    let size_pressure = match size_bucket {
        0 => 0.20,
        1 => 0.48,
        _ => 0.72,
    };
    let context_confidence = (context_signal.samples.min(24) as f64 / 24.0).clamp(0.0, 1.0);
    let path_risk = (path_len.saturating_sub(2).min(3) as f64) / 3.0;
    let competition_score_fast = (gas_pressure * 0.46
        + size_pressure * 0.34
        + path_risk * 0.14
        + context_signal.toxicity_score.clamp(0.0, 1.0) * (0.08 + context_confidence * 0.08)
        - context_signal.priority_score.clamp(0.0, 1.0) * (0.03 + context_confidence * 0.05))
        .clamp(0.0, 1.0);

    let reject_reason = if ev_upper_bound_usd < config.mev.effective_min_profit_usd() * 1.5 {
        Some("ev_upper_bound_below_min")
    } else if competition_score_fast > 0.75 {
        Some("competition_fast_too_high")
    } else {
        None
    };

    FastPreflightDecision {
        should_continue: reject_reason.is_none(),
        reject_reason,
        ev_upper_bound_usd,
        estimated_gas_cost_usd,
        competition_score_fast,
        gas_ratio,
        scavenger_try_score: 0.0,
    }
}

fn adaptive_gas_cap_gwei(ev_upper_bound_usd: f64, notional_eth: f64, hard_cap_gwei: f64) -> f64 {
    let tier_floor: f64 = if ev_upper_bound_usd >= 0.25 {
        1_000.0
    } else if ev_upper_bound_usd >= 0.14 {
        hard_cap_gwei * 0.98 // ~980 gwei
    } else if ev_upper_bound_usd >= 0.10 {
        850.0
    } else if ev_upper_bound_usd >= 0.05 {
        450.0
    } else if ev_upper_bound_usd >= 0.02 {
        hard_cap_gwei * 0.95
    } else if ev_upper_bound_usd >= 0.005 {
        350.0 // ← ANTES era 220.0
    } else if ev_upper_bound_usd >= 0.001 {
        300.0 // ← ANTES era 180.0
    } else if ev_upper_bound_usd > 0.0 {
        250.0 // ← ANTES era 180.0 (edge positivo mínimo)
    } else if ev_upper_bound_usd >= -0.02 {
        150.0
    } else {
        120.0
    };

    let continuous_cap: f64 = if ev_upper_bound_usd > 0.0 {
        100.0 + ev_upper_bound_usd * 3_000.0
    } else {
        150.0 + ev_upper_bound_usd * 1_500.0
    };
    let edge_cap = if ev_upper_bound_usd >= 0.02 {
        tier_floor
    } else {
        tier_floor.max(continuous_cap)
    };

    let size_multiplier = if ev_upper_bound_usd > 0.0 {
        (0.95 + notional_eth / 40.0).clamp(1.0, 1.18)
    } else {
        (notional_eth / 10.0).clamp(0.75, 1.15)
    };
    (edge_cap * size_multiplier).clamp(120.0, hard_cap_gwei.max(1.0))
}

async fn flush_candidate_batch(
    batcher: &mut MicroBatcher,
    executor: &ExecutionEngine,
    dashboard: &DashboardHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some((best, dropped)) = batcher.drain_best() else {
        return Ok(());
    };
    if dropped > 0 {
        dashboard.event(
            "info",
            format!(
                "microbatch selected best candidate and kept {} lower-ranked candidates pending score={:.4} ev_real={:.2}usd p={:.2}",
                dropped,
                best.ranking_score(),
                best.ev_real_usd,
                best.p_positive
            ),
        );
    }
    executor.handle(best.opportunity).await
}

pub(crate) fn passes_ev_gate(
    config: &Config,
    payload: &crate::mev::execution::payload_builder::ExecutionPayload,
    signal: &SwapSignal,
    lookup_latency: std::time::Duration,
    min_profit_wei: U256,
) -> bool {
    match signal.kind {
        SwapKind::V2 => passes_ev_gate_v2(config, payload, signal, lookup_latency, min_profit_wei),
        SwapKind::V3 { .. } => {
            passes_ev_gate_v3(config, payload, signal, lookup_latency, min_profit_wei)
        }
    }
}

pub(crate) fn passes_ev_gate_v2(
    config: &Config,
    payload: &crate::mev::execution::payload_builder::ExecutionPayload,
    signal: &SwapSignal,
    lookup_latency: std::time::Duration,
    min_profit_wei: U256,
) -> bool {
    if config.mev.opportunity_mode() == OpportunityMode::Scavenger {
        let lookup_is_fresh = lookup_latency.as_millis()
            <= u128::from(config.mev.effective_max_pending_age_ms().max(1));
        let gas_budget_ok = payload.gas_limit <= config.mev.max_gas_per_tx;

        return lookup_is_fresh && !payload.expected_profit_wei.is_zero() && gas_budget_ok;
    }

    let lookup_is_fresh =
        lookup_latency.as_millis() <= u128::from(config.mev.effective_max_pending_age_ms().max(1));
    let large_enough = signal.notional_wei
        >= ethers::utils::parse_ether(config.mev.effective_min_large_swap_eth().to_string())
            .unwrap_or_default();
    let inevitable_impact = payload.price_impact_bps >= 8;
    let profit_above_threshold = payload.expected_profit_wei >= min_profit_wei;
    let net_ev_usd = wei_to_eth_f64(payload.expected_profit_wei) * config.mev.eth_usd_price;
    let gas_budget_ok = payload.gas_limit <= config.mev.max_gas_per_tx;

    lookup_is_fresh
        && large_enough
        && inevitable_impact
        && profit_above_threshold
        && net_ev_usd >= config.mev.effective_min_profit_usd()
        && gas_budget_ok
}

pub(crate) fn passes_ev_gate_v3(
    config: &Config,
    payload: &crate::mev::execution::payload_builder::ExecutionPayload,
    signal: &SwapSignal,
    lookup_latency: std::time::Duration,
    min_profit_wei: U256,
) -> bool {
    if config.mev.opportunity_mode() == OpportunityMode::Scavenger {
        let lookup_is_fresh = lookup_latency.as_millis()
            <= u128::from(config.mev.effective_max_pending_age_ms().max(1));
        let gas_budget_ok = payload.gas_limit <= config.mev.max_gas_per_tx;

        return lookup_is_fresh && !payload.expected_profit_wei.is_zero() && gas_budget_ok;
    }

    let lookup_is_fresh =
        lookup_latency.as_millis() <= u128::from(config.mev.effective_max_pending_age_ms().max(1));
    let large_enough = signal.notional_wei
        >= ethers::utils::parse_ether(config.mev.effective_min_large_swap_eth().to_string())
            .unwrap_or_default();
    let inevitable_impact = payload.price_impact_bps >= 6;
    let profit_above_threshold = payload.expected_profit_wei >= min_profit_wei;
    let net_ev_usd = wei_to_eth_f64(payload.expected_profit_wei) * config.mev.eth_usd_price;
    let gas_budget_ok = payload.gas_limit <= config.mev.max_gas_per_tx;

    lookup_is_fresh
        && large_enough
        && inevitable_impact
        && profit_above_threshold
        && net_ev_usd >= config.mev.effective_min_profit_usd()
        && gas_budget_ok
}

pub(crate) async fn build_payload<M: Middleware + 'static>(
    provider: Arc<M>,
    config: &Config,
    signal: &SwapSignal,
    gas_price: U256,
    context_signal: ContextSignal,
    pool_cache: &PoolCache,
    block_number: u64,
) -> Result<crate::mev::execution::payload_builder::ExecutionPayload, String> {
    match &signal.kind {
        SwapKind::V2 => {
            build_v2_payload(
                provider,
                config,
                signal,
                gas_price,
                context_signal,
                pool_cache,
                block_number,
            )
            .await
        }
        SwapKind::V3 {
            fee_tier,
            encoded_path,
            ..
        } => {
            build_v3_payload(
                provider,
                config,
                signal,
                gas_price,
                context_signal,
                *fee_tier,
                encoded_path.clone(),
                pool_cache,
                block_number,
            )
            .await
        }
    }
}

async fn build_payload_with_fallback_parallel(
    rpc_fleet: Arc<RpcFleet>,
    config: Arc<Config>,
    signal: SwapSignal,
    gas_price: U256,
    context_signal: ContextSignal,
    pool_cache: Arc<PoolCache>, // NOVO
    block_number: u64,          // NOVO
) -> Result<ExecutionPayload, String> {
    let mut join_set = JoinSet::new();
    for handle in rpc_fleet.read_candidates(payload_build_fanout(&config)) {
        let rpc_fleet = rpc_fleet.clone();
        let provider = handle.provider.clone();
        let signal = signal.clone();
        let config = config.clone();
        let context_signal = context_signal;
        let pool_cache = pool_cache.clone();
        join_set.spawn(async move {
            rpc_fleet.reserve_read_selection(handle.id);
            let started = Instant::now();
            match build_payload(
                provider,
                &config,
                &signal,
                gas_price,
                context_signal,
                &pool_cache,
                block_number,
            )
            .await
            {
                Ok(payload) => {
                    rpc_fleet.record_success(handle.id, started.elapsed(), Some(block_number));
                    Ok(payload)
                }
                Err(err) => {
                    rpc_fleet.record_failure(handle.id, RpcFleet::classify_failure(&err));
                    Err(err)
                }
            }
        });
    }

    let mut errors = Vec::new();
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(payload)) => {
                join_set.abort_all();
                return Ok(payload);
            }
            Ok(Err(err)) => errors.push(err),
            Err(err) => errors.push(err.to_string()),
        }
    }
    Err(compact_payload_errors(errors))
}

fn payload_build_fanout(config: &Config) -> usize {
    if let Ok(value) = std::env::var("MEV_PAYLOAD_BUILD_FANOUT") {
        if let Ok(parsed) = value.trim().parse::<usize>() {
            return parsed.clamp(1, 3);
        }
    }

    if config.mev.opportunity_mode() == OpportunityMode::Scavenger {
        1
    } else {
        3
    }
}

fn compact_payload_errors(errors: Vec<String>) -> String {
    let mut unique = Vec::new();
    for err in errors {
        let normalized = err.trim();
        if normalized.is_empty() {
            continue;
        }
        if !unique
            .iter()
            .any(|existing: &String| existing.eq_ignore_ascii_case(normalized))
        {
            unique.push(normalized.to_string());
        }
        if unique.len() >= 3 {
            break;
        }
    }

    if unique.is_empty() {
        "all payload builders failed without error detail".to_string()
    } else {
        unique.join(" | ")
    }
}

fn human_payload_error(error: &str) -> String {
    let clean_error = strip_edge_sample(error);
    let lower = clean_error.to_ascii_lowercase();
    let reason = if lower.contains("missing uniswap v2 factory")
        || lower.contains("missing uniswap v3 factory")
    {
        "factory not configured"
    } else if lower.contains("pair not found") || lower.contains("pool not found") {
        "pool not found"
    } else if lower.contains("failed to fetch pool state") {
        "pool state unavailable"
    } else if lower.contains("no positive gross") {
        "no exploitable micro edge"
    } else if lower.contains("no roi-positive") {
        "no profitable size after gas"
    } else if lower.contains("below minimum") {
        "profit below configured floor"
    } else if lower.contains("reverse path") || lower.contains("does not support") {
        "unsupported reverse path"
    } else if lower.contains("executor_address") {
        "executor contract not configured"
    } else {
        "payload build failed"
    };

    format!("{reason} ({})", compact_text(&clean_error, 96))
}

fn strip_edge_sample(error: &str) -> String {
    error
        .split(" | edge_sample=")
        .next()
        .unwrap_or(error)
        .to_string()
}

fn extract_edge_sample(error: &str) -> Option<EdgeMetadata> {
    let (_, json) = error.split_once(" | edge_sample=")?;
    serde_json::from_str::<EdgeMetadata>(json).ok()
}

fn enrich_edge_explainer_sample(
    mut sample: EdgeMetadata,
    tx_hash: H256,
    signal: &SwapSignal,
    fast_gate: &FastPreflightDecision,
) -> EdgeMetadata {
    sample.victim_tx = short_hash(tx_hash);
    sample.selector = format!(
        "0x{:02x}{:02x}{:02x}{:02x}",
        signal.selector[0], signal.selector[1], signal.selector[2], signal.selector[3]
    );
    if sample.path.is_empty() {
        sample.path = signal
            .path
            .iter()
            .map(|address| format!("{address:?}"))
            .collect();
    }
    if sample.hops == 0 {
        sample.hops = signal.path_len().saturating_sub(1) as u64;
    }
    sample.slippage_window_score = scavenger_slippage_window_hint(signal);
    sample.pool_imbalance_score = scavenger_impact_imbalance_hint(
        wei_to_eth_f64(signal.notional_wei),
        1.0,
        signal.path_len(),
    );
    sample.cross_dex_deviation_bps =
        ((sample.gross_edge_native * 10_000.0).round() as i64).clamp(-1_000_000, 1_000_000);
    sample.gas_estimate = sample.gas_estimate.max(
        fast_gate
            .estimated_gas_cost_usd
            .max(0.0)
            .round()
            .min(u64::MAX as f64) as u64,
    );
    if sample.simulated_extraction_native == 0.0 {
        sample.simulated_extraction_native = sample.gross_edge_native;
    }
    if sample.aggregator_type.is_empty() {
        sample.aggregator_type = "direct_router".to_string();
    }
    if sample.route_complexity == 0 {
        sample.route_complexity = sample.hops.max(1);
    }
    if sample.dex_sequence.is_empty() {
        sample.dex_sequence = vec![sample.route_kind.clone()];
    }
    if sample.route_inefficiency_score == 0.0 {
        sample.route_inefficiency_score = scavenger_route_inefficiency_hint(signal);
    }
    if sample.liquidity_distortion_score == 0.0 {
        sample.liquidity_distortion_score = sample.pool_imbalance_score;
    }
    if sample.hop_profitability_rank.is_empty() {
        sample.hop_profitability_rank = (0..sample.hops.max(1).min(4))
            .map(|idx| {
                format!(
                    "hop_{}: gross={:.6} impact={}bps",
                    idx + 1,
                    sample.gross_edge_native,
                    sample.price_impact_bps
                )
            })
            .collect();
    }
    sample
}

fn append_gas_sniper_shadow_metrics(
    sample: &mut EdgeMetadata,
    payload: &ExecutionPayload,
    observed_gas_price: U256,
) {
    let base_fee_per_block = observed_gas_price;
    let base_priority =
        observed_gas_price.saturating_mul(U256::from(5_000u64)) / U256::from(10_000u64);
    let priority_to_win =
        base_priority.saturating_mul(U256::from(15_000u64)) / U256::from(10_000u64);
    let edge_gas_budget = payload
        .expected_profit_wei
        .saturating_mul(U256::from(35u64))
        / U256::from(100u64);
    let edge_capped_gas_price = if payload.gas_limit == 0 {
        U256::zero()
    } else {
        edge_gas_budget / U256::from(payload.gas_limit)
    };
    let priority_fee_to_win = priority_to_win.min(edge_capped_gas_price);
    sample.hop_profitability_rank.push(format!(
        "gas_shadow: base_fee_per_block_wei={} priority_fee_to_win_wei={} edge_cap_gas_price_wei={} missed_inclusion=false replacement_needed=false",
        base_fee_per_block, priority_fee_to_win, edge_capped_gas_price
    ));
}

fn compact_text(value: &str, max_chars: usize) -> String {
    let compact = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= max_chars {
        return compact;
    }
    let mut out: String = compact.chars().take(max_chars.saturating_sub(3)).collect();
    out.push_str("...");
    out
}

fn short_hash(hash: H256) -> String {
    let full = format!("{hash:?}");
    if full.len() <= 14 {
        full
    } else {
        format!("{}...{}", &full[..8], &full[full.len() - 6..])
    }
}

fn contextual_capital_available_wei(
    config: &Config,
    context_signal: ContextSignal,
) -> Result<U256, String> {
    let multiplier = config.mev.contextual_capital_multiplier(
        context_signal.priority_score,
        context_signal.toxicity_score,
    );
    let capital_eth = (config.mev.capital_eth * multiplier).max(0.000_001);
    ethers::utils::parse_ether(capital_eth.to_string()).map_err(|err| err.to_string())
}

pub(crate) async fn build_v2_payload<M: Middleware + 'static>(
    provider: Arc<M>,
    config: &Config,
    signal: &SwapSignal,
    gas_price: U256,
    context_signal: ContextSignal,
    pool_cache: &PoolCache, // NOVO PARÂMETRO
    block_number: u64,      // NOVO PARÂMETRO
) -> Result<ExecutionPayload, String> {
    let recipient = config.profit_address;
    let token_in = *signal
        .path
        .first()
        .ok_or_else(|| "missing token_in".to_string())?;
    let token_out = *signal
        .path
        .get(1)
        .ok_or_else(|| "missing token_out".to_string())?;

    let execution_router = execution_v2_router(config, signal.router);
    let (factory, pair) = find_v2_pair(
        provider.clone(),
        config,
        execution_router,
        token_in,
        token_out,
    )
    .await?;

    if pair == Address::zero() {
        return Err("v2 pair not found".to_string());
    }

    // Usar cache em vez de chamadas RPC diretas
    let cached_pool = pool_cache
        .get_or_fetch_v2(pair, provider.clone(), block_number)
        .await
        .ok_or_else(|| "failed to fetch pool state".to_string())?;

    let pool = V2PoolState {
        pair,
        token0: cached_pool.token0,
        token1: cached_pool.token1,
        reserve0: cached_pool.reserve0,
        reserve1: cached_pool.reserve1,
        fee_bps: 30,
    };

    let capital_available_wei = contextual_capital_available_wei(config, context_signal)?;
    let (v2_swap_path, v2_swap_pools) =
        if config.mev.opportunity_mode() == OpportunityMode::Scavenger {
            best_scavenger_v2_reverse_route(
                provider.clone(),
                config,
                signal,
                token_in,
                token_out,
                pair,
                execution_router,
                capital_available_wei,
                pool_cache,
                block_number,
            )
            .await
            .unwrap_or_else(|| (vec![token_out, token_in], vec![pool]))
        } else {
            (vec![token_out, token_in], vec![pool])
        };

    PayloadBuilder::build_fee_extraction_v2(
        config,
        FeeExtractionBuildInput {
            router: execution_router,
            factory: Some(factory),
            pair,
            recipient,
            token_in,
            token_out,
            victim_amount_in: signal.amount_in,
            state_before: crate::mev::simulation::state_simulator::AmmState::UniswapV2(pool),
            capital_available_wei,
            gas_price_wei: gas_price,
            context_priority_score: context_signal.priority_score,
            context_toxicity_score: context_signal.toxicity_score,
            route_kind: AmmRouteKind::UniswapV2,
            v2_swap_path: Some(v2_swap_path),
            v2_swap_pools,
        },
    )
}

async fn find_v2_pair<M: Middleware + 'static>(
    provider: Arc<M>,
    config: &Config,
    router: Address,
    token_in: Address,
    token_out: Address,
) -> Result<(Address, Address), String> {
    let mut factories = Vec::new();

    let router_contract = UniswapV2Router::new(router, provider.clone());
    if let Ok(factory) = router_contract.factory().call().await {
        push_unique_address(&mut factories, factory);
    }

    if let Some(factory) = config.mev.uniswap_v2_factory {
        push_unique_address(&mut factories, factory);
    }

    for factory in default_v2_factories(config.network.as_str()) {
        push_unique_address(&mut factories, factory);
    }

    if factories.is_empty() {
        return Err("v2 factory unavailable for router".to_string());
    }

    let mut errors = Vec::new();
    for factory in factories {
        let factory_contract = UniswapV2Factory::new(factory, provider.clone());
        match factory_contract.get_pair(token_in, token_out).call().await {
            Ok(pair) if pair != Address::zero() => return Ok((factory, pair)),
            Ok(_) => errors.push(format!("pair not found on factory {:?}", factory)),
            Err(err) => errors.push(format!("factory {:?} lookup failed: {}", factory, err)),
        }
    }

    Err(compact_payload_errors(errors))
}

async fn best_scavenger_v2_reverse_route<M: Middleware + 'static>(
    provider: Arc<M>,
    config: &Config,
    signal: &SwapSignal,
    token_in: Address,
    token_out: Address,
    victim_pair: Address,
    execution_router: Address,
    capital_available_wei: U256,
    pool_cache: &PoolCache,
    block_number: u64,
) -> Option<(Vec<Address>, Vec<V2PoolState>)> {
    let probe_amount =
        capital_available_wei.saturating_mul(U256::from(25u64)) / U256::from(10_000u64);
    if probe_amount.is_zero() {
        return None;
    }

    let mut best: Option<(U256, Vec<Address>, Vec<V2PoolState>)> = None;
    for route in scavenger_reverse_route_candidates(config, signal, token_in, token_out) {
        let Some(pools) = load_v2_route_pools(
            provider.clone(),
            config,
            execution_router,
            &route,
            victim_pair,
            pool_cache,
            block_number,
        )
        .await
        else {
            continue;
        };
        let Some(amount_out) = quote_v2_runtime_route_exact_in(probe_amount, &route, &pools) else {
            continue;
        };
        let route_impact_bps =
            quote_v2_runtime_route_impact_bps(probe_amount, &route, &pools).unwrap_or(10_000);
        if route_impact_bps > scavenger_quality_price_impact_cap_bps(config) {
            continue;
        }
        if route_has_low_liquidity_probe(probe_amount, &route, &pools) {
            continue;
        }
        let score = scavenger_v2_route_score(amount_out, route_impact_bps, route.len());
        let replace = best
            .as_ref()
            .map(|(best_score, _, _)| score > *best_score)
            .unwrap_or(true);
        if replace {
            best = Some((score, route, pools));
        }
    }

    best.map(|(_, route, pools)| (route, pools))
}

fn scavenger_reverse_route_candidates(
    config: &Config,
    signal: &SwapSignal,
    token_in: Address,
    token_out: Address,
) -> Vec<Vec<Address>> {
    let mut routes = Vec::new();
    routes.push(vec![token_out, token_in]);

    for token in config.monitored_tokens.iter().take(6) {
        let mid = token.address;
        if mid != token_in && mid != token_out {
            routes.push(vec![token_out, mid, token_in]);
        }
    }

    for mid in signal.path.iter().copied().skip(2).take(2) {
        if mid != token_in && mid != token_out {
            routes.push(vec![token_out, mid, token_in]);
        }
    }

    dedup_routes(routes)
}

fn dedup_routes(routes: Vec<Vec<Address>>) -> Vec<Vec<Address>> {
    let mut out = Vec::new();
    for route in routes {
        if !out.iter().any(|existing: &Vec<Address>| *existing == route) {
            out.push(route);
        }
    }
    out
}

async fn load_v2_route_pools<M: Middleware + 'static>(
    provider: Arc<M>,
    config: &Config,
    router: Address,
    route: &[Address],
    victim_pair: Address,
    pool_cache: &PoolCache,
    block_number: u64,
) -> Option<Vec<V2PoolState>> {
    if route.len() < 2 {
        return None;
    }

    let mut pools = Vec::with_capacity(route.len().saturating_sub(1));
    for edge in route.windows(2) {
        let (_, pair) = find_v2_pair(provider.clone(), config, router, edge[0], edge[1])
            .await
            .ok()?;
        if pair == Address::zero() {
            return None;
        }
        if route.len() > 2 && pair == victim_pair {
            return None;
        }
        let cached = pool_cache
            .get_or_fetch_v2(pair, provider.clone(), block_number)
            .await?;
        pools.push(V2PoolState {
            pair,
            token0: cached.token0,
            token1: cached.token1,
            reserve0: cached.reserve0,
            reserve1: cached.reserve1,
            fee_bps: 30,
        });
    }
    Some(pools)
}

fn quote_v2_runtime_route_exact_in(
    amount_in: U256,
    route: &[Address],
    pools: &[V2PoolState],
) -> Option<U256> {
    if route.len() < 2 || pools.len() + 1 != route.len() {
        return None;
    }
    let mut amount = amount_in;
    for (idx, pool) in pools.iter().enumerate() {
        let (reserve_in, reserve_out) = pool.reserves_for(route[idx], route[idx + 1])?;
        amount = amount_out_exact_in(amount, reserve_in, reserve_out, pool.fee_bps)?;
    }
    Some(amount)
}

fn quote_v2_runtime_route_impact_bps(
    amount_in: U256,
    route: &[Address],
    pools: &[V2PoolState],
) -> Option<u64> {
    if route.len() < 2 || pools.len() + 1 != route.len() {
        return None;
    }
    let mut amount = amount_in;
    let mut worst_impact_bps = 0u64;
    for (idx, pool) in pools.iter().enumerate() {
        let (reserve_in, reserve_out) = pool.reserves_for(route[idx], route[idx + 1])?;
        let amount_out = amount_out_exact_in(amount, reserve_in, reserve_out, pool.fee_bps)?;
        worst_impact_bps = worst_impact_bps.max(price_impact_bps(
            amount,
            amount_out,
            reserve_in,
            reserve_out,
        ));
        amount = amount_out;
    }
    Some(worst_impact_bps)
}

fn route_has_low_liquidity_probe(
    probe_amount: U256,
    route: &[Address],
    pools: &[V2PoolState],
) -> bool {
    if route.len() < 2 || pools.len() + 1 != route.len() {
        return true;
    }
    for (idx, pool) in pools.iter().enumerate() {
        let Some((reserve_in, reserve_out)) = pool.reserves_for(route[idx], route[idx + 1]) else {
            return true;
        };
        if reserve_in <= probe_amount.saturating_mul(U256::from(20u64))
            || reserve_out <= U256::one()
        {
            return true;
        }
    }
    false
}

fn scavenger_v2_route_score(amount_out: U256, impact_bps: u64, route_len: usize) -> U256 {
    let hop_count = route_len.saturating_sub(1).max(1) as u64;
    let penalty_bps = 10_000u64
        .saturating_add(impact_bps.saturating_mul(2))
        .saturating_add(hop_count.saturating_sub(1).saturating_mul(250));
    amount_out.saturating_mul(U256::from(10_000u64)) / U256::from(penalty_bps.max(1))
}

fn push_unique_address(addresses: &mut Vec<Address>, address: Address) {
    if address != Address::zero() && !addresses.contains(&address) {
        addresses.push(address);
    }
}

fn execution_v2_router(config: &Config, fallback: Address) -> Address {
    default_v2_router(config.network.as_str()).unwrap_or(fallback)
}

fn execution_v3_router(config: &Config, fallback: Address) -> Address {
    default_v3_router(config.network.as_str()).unwrap_or(fallback)
}

fn default_v2_router(network: &str) -> Option<Address> {
    match network {
        "polygon" => "0xa5e0829caced8ffdd4de3c43696c57f7d7a678ff".parse().ok(),
        "bsc" | "bnb" => "0x10ed43c718714eb63d5aa57b78b54704e256024e".parse().ok(),
        "ethereum" => "0x7a250d5630b4cf539739df2c5dacb4c659f2488d".parse().ok(),
        _ => None,
    }
}

fn default_v3_router(network: &str) -> Option<Address> {
    match network {
        "polygon" | "ethereum" | "arbitrum" => {
            "0xe592427a0aece92de3edee1f18e0157c05861564".parse().ok()
        }
        "bsc" | "bnb" => "0x13f4ea83d0bd40e75c8222255bc855a974568dd4".parse().ok(),
        _ => None,
    }
}

fn default_v2_factories(network: &str) -> Vec<Address> {
    match network {
        "polygon" => parse_default_addresses(&[
            "0x5757371414417b8c6caad45baef941abc7d3ab32",
            "0xc35dadb65012ec5796536bd9864ed8773abc74c4",
        ]),
        "bsc" | "bnb" => parse_default_addresses(&[
            "0xca143ce32fe78f1f7019d7d551a6402fc5350c73",
            "0xc35dadb65012ec5796536bd9864ed8773abc74c4",
        ]),
        "ethereum" => parse_default_addresses(&[
            "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f",
            "0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac",
        ]),
        _ => Vec::new(),
    }
}

fn parse_default_addresses(values: &[&str]) -> Vec<Address> {
    values
        .iter()
        .filter_map(|value| value.parse::<Address>().ok())
        .collect()
}

async fn find_v3_pool<M: Middleware + 'static>(
    provider: Arc<M>,
    config: &Config,
    token_in: Address,
    token_out: Address,
    fee_tier: u32,
) -> Result<(Address, Address), String> {
    let mut factories = Vec::new();

    if let Some(factory) = config.mev.uniswap_v3_factory {
        push_unique_address(&mut factories, factory);
    }

    for factory in default_v3_factories(config.network.as_str()) {
        push_unique_address(&mut factories, factory);
    }

    if factories.is_empty() {
        return Err("v3 factory unavailable".to_string());
    }

    let mut errors = Vec::new();
    for factory in factories {
        let factory_contract = UniswapV3Factory::new(factory, provider.clone());
        match factory_contract
            .get_pool(token_in, token_out, fee_tier)
            .call()
            .await
        {
            Ok(pool) if pool != Address::zero() => return Ok((factory, pool)),
            Ok(_) => errors.push(format!(
                "v3 pool not found on factory {:?} fee={}",
                factory, fee_tier
            )),
            Err(err) => errors.push(format!("v3 factory {:?} lookup failed: {}", factory, err)),
        }
    }

    Err(compact_payload_errors(errors))
}

fn default_v3_factories(network: &str) -> Vec<Address> {
    match network {
        "polygon" | "ethereum" | "arbitrum" => parse_default_addresses(&[
            // Uniswap V3
            "0x1f98431c8ad98523631ae4a59f267346ea31f984",
        ]),
        "bsc" | "bnb" => parse_default_addresses(&[
            // PancakeSwap V3
            "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865",
        ]),
        _ => Vec::new(),
    }
}

pub(crate) async fn build_v3_payload<M: Middleware + 'static>(
    provider: Arc<M>,
    config: &Config,
    signal: &SwapSignal,
    gas_price: U256,
    context_signal: ContextSignal,
    fee_tier: u32,
    encoded_path: ethers::types::Bytes,
    pool_cache: &PoolCache,
    block_number: u64,
) -> Result<ExecutionPayload, String> {
    let recipient = config.profit_address;
    let token_in = *signal
        .path
        .first()
        .ok_or_else(|| "missing token_in".to_string())?;
    let token_out = *signal
        .path
        .get(1)
        .ok_or_else(|| "missing edge token_out".to_string())?;

    let (factory, pool) =
        find_v3_pool(provider.clone(), config, token_in, token_out, fee_tier).await?;
    let execution_router = execution_v3_router(config, signal.router);

    if pool == Address::zero() {
        return Err("v3 pool not found".to_string());
    }

    // Usar cache em vez de chamadas RPC diretas
    let cached_pool = pool_cache
        .get_or_fetch_v3(pool, provider.clone(), block_number)
        .await
        .ok_or_else(|| "failed to fetch v3 pool state".to_string())?;

    let state = V3PoolState {
        pool,
        token0: cached_pool.token0,
        token1: cached_pool.token1,
        sqrt_price_x96: cached_pool.sqrt_price_x96,
        liquidity: cached_pool.liquidity,
        current_tick: cached_pool.current_tick,
        fee_bps: fee_tier as u64 / 100,
        initialized_ticks: Vec::new(),
    };

    let capital_available_wei = contextual_capital_available_wei(config, context_signal)?;

    // NOVO: Determinar o amount_in correto baseado no tipo de swap
    let (victim_amount_in, is_exact_out) = match &signal.kind {
        SwapKind::V3 { exact_out, .. } => {
            if *exact_out {
                // Para exact-out, amount_in é o teto (amount_in_max)
                (signal.amount_in, true)
            } else {
                (signal.amount_in, false)
            }
        }
        _ => (signal.amount_in, false),
    };

    // NOVO: Calcular edge conservador para exact-out single-hop
    if is_exact_out {
        let _amount_out_min = signal
            .amount_out_min
            .ok_or_else(|| "exact_out requires amount_out_min".to_string())?;

        // Verificar se o edge é positivo usando amount_in_max como teto
        let gas_cost_wei = gas_price.saturating_mul(U256::from(300_000u64));

        // Cálculo conservador: assumimos que vamos pagar o amount_in_max inteiro
        if victim_amount_in <= gas_cost_wei {
            return Err(format!(
                "exact_out edge below gas cost: amount_in_max={} gas_cost={}",
                victim_amount_in, gas_cost_wei
            ));
        }

        let edge_wei = victim_amount_in.saturating_sub(gas_cost_wei);
        if edge_wei.is_zero() {
            return Err("exact_out positive gross edge not found".to_string());
        }
    }

    PayloadBuilder::build_fee_extraction_v3(
        config,
        FeeExtractionBuildInput {
            router: execution_router,
            factory: Some(factory),
            pair: pool,
            recipient,
            token_in,
            token_out,
            victim_amount_in, // USAR O VALOR CORRETO (amount_in_max para exact-out)
            state_before: crate::mev::simulation::state_simulator::AmmState::UniswapV3(state),
            capital_available_wei,
            gas_price_wei: gas_price,
            context_priority_score: context_signal.priority_score,
            context_toxicity_score: context_signal.toxicity_score,
            route_kind: AmmRouteKind::UniswapV3 {
                fee_tier,
                path: encoded_path,
            },
            v2_swap_path: None,
            v2_swap_pools: Vec::new(),
        },
    )
}

pub(crate) fn decode_relevant_swap(
    tx: &Transaction,
    monitored_tokens: &[MonitoredTokenConfig],
    min_large_swap_wei: U256,
    mode: OpportunityMode,
) -> Option<SwapSignal> {
    let selector = selector(tx)?;
    let router = tx.to?;
    let args = &tx.input.as_ref()[4..];

    let mut signal = match selector {
        SWAP_EXACT_ETH_FOR_TOKENS | SWAP_EXACT_ETH_FOR_TOKENS_SUPPORTING_FEE => {
            let decoded = abi::decode(
                &[
                    ParamType::Uint(256),
                    ParamType::Array(Box::new(ParamType::Address)),
                    ParamType::Address,
                    ParamType::Uint(256),
                ],
                args,
            )
            .ok()?;
            SwapSignal {
                selector,
                amount_in: tx.value,
                amount_out_min: decoded.first().and_then(token_as_uint),
                notional_wei: tx.value,
                path: decoded.get(1).and_then(token_as_address_vec)?,
                router,
                kind: SwapKind::V2,
            }
        }
        SWAP_EXACT_TOKENS_FOR_TOKENS
        | SWAP_EXACT_TOKENS_FOR_ETH
        | SWAP_EXACT_TOKENS_FOR_TOKENS_SUPPORTING_FEE
        | SWAP_EXACT_TOKENS_FOR_ETH_SUPPORTING_FEE => {
            let decoded = abi::decode(
                &[
                    ParamType::Uint(256),
                    ParamType::Uint(256),
                    ParamType::Array(Box::new(ParamType::Address)),
                    ParamType::Address,
                    ParamType::Uint(256),
                ],
                args,
            )
            .ok()?;
            SwapSignal {
                selector,
                amount_in: decoded.first().and_then(token_as_uint)?,
                amount_out_min: decoded.get(1).and_then(token_as_uint),
                notional_wei: U256::zero(),
                path: decoded.get(2).and_then(token_as_address_vec)?,
                router,
                kind: SwapKind::V2,
            }
        }
        V3_EXACT_INPUT_SINGLE => {
            let decoded = abi::decode(
                &[ParamType::Tuple(vec![
                    ParamType::Address,
                    ParamType::Address,
                    ParamType::Uint(24),
                    ParamType::Address,
                    ParamType::Uint(256),
                    ParamType::Uint(256),
                    ParamType::Uint(256),
                    ParamType::Uint(160),
                ])],
                args,
            )
            .ok()?;
            let params = decoded.first()?;
            let Token::Tuple(values) = params else {
                return None;
            };
            let token_in = token_as_address(values.first()?)?;
            let token_out = token_as_address(values.get(1)?)?;
            let fee_tier = token_as_uint(values.get(2)?)?.as_u32();
            let amount_in = token_as_uint(values.get(5)?)?;
            SwapSignal {
                selector,
                amount_in,
                amount_out_min: values.get(6).and_then(token_as_uint),
                notional_wei: U256::zero(),
                path: vec![token_in, token_out],
                router,
                kind: SwapKind::V3 {
                    fee_tier,
                    encoded_path: encode_v3_path(token_out, fee_tier, token_in),
                    hops: 1,
                    exact_out: false,
                },
            }
        }
        V3_EXACT_INPUT => {
            let decoded = abi::decode(
                &[ParamType::Tuple(vec![
                    ParamType::Bytes,
                    ParamType::Address,
                    ParamType::Uint(256),
                    ParamType::Uint(256),
                    ParamType::Uint(256),
                ])],
                args,
            )
            .ok()?;
            let params = decoded.first()?;
            let Token::Tuple(values) = params else {
                return None;
            };
            let path_bytes = match values.first()? {
                Token::Bytes(value) => value.clone(),
                _ => return None,
            };
            let parsed = parse_v3_path(&path_bytes)?;
            let amount_in = token_as_uint(values.get(3)?)?;
            SwapSignal {
                selector,
                amount_in,
                amount_out_min: values.get(4).and_then(token_as_uint),
                notional_wei: U256::zero(),
                path: vec![parsed.token_in, parsed.edge_token_out],
                router,
                kind: SwapKind::V3 {
                    fee_tier: parsed.first_fee_tier,
                    encoded_path: encode_v3_path(
                        parsed.edge_token_out,
                        parsed.first_fee_tier,
                        parsed.token_in,
                    ),
                    hops: parsed.hops,
                    exact_out: false,
                },
            }
        }
        UNIVERSAL_ROUTER_EXECUTE | UNIVERSAL_ROUTER_EXECUTE_NO_DEADLINE => {
            decode_universal_router_swap_for_monitored(selector, router, args, monitored_tokens)
                .or_else(|| decode_universal_router_swap(selector, router, args))?
        }
        ZERO_EX_SELL_TO_UNISWAP => decode_zero_ex_sell_to_uniswap(selector, router, args)?,
        _ => return None,
    };

    let notional_wei = estimate_notional_wei(&signal, monitored_tokens)?;
    if signal.path.len() < 2 {
        return None;
    }
    if mode != OpportunityMode::Scavenger && notional_wei < min_large_swap_wei {
        return None;
    }
    signal.notional_wei = notional_wei;
    Some(signal)
}

fn decode_universal_router_swap(
    selector: [u8; 4],
    router: Address,
    args: &[u8],
) -> Option<SwapSignal> {
    decode_universal_router_swap_candidates(selector, router, args)
        .into_iter()
        .next()
}

fn decode_universal_router_swap_for_monitored(
    selector: [u8; 4],
    router: Address,
    args: &[u8],
    monitored_tokens: &[MonitoredTokenConfig],
) -> Option<SwapSignal> {
    decode_universal_router_swap_candidates(selector, router, args)
        .into_iter()
        .filter_map(|mut signal| {
            let notional_wei = estimate_notional_wei(&signal, monitored_tokens)?;
            signal.notional_wei = notional_wei;
            Some((universal_router_candidate_rank(&signal), signal))
        })
        .max_by_key(|(rank, _)| *rank)
        .map(|(_, signal)| signal)
}

fn universal_router_candidate_rank(signal: &SwapSignal) -> (U256, u8, i64) {
    let exact_in_score = match &signal.kind {
        SwapKind::V3 { exact_out, .. } if *exact_out => 0,
        _ => 1,
    };
    (
        signal.notional_wei,
        exact_in_score,
        -(signal.path_len() as i64),
    )
}

fn decode_universal_router_swap_candidates(
    selector: [u8; 4],
    router: Address,
    args: &[u8],
) -> Vec<SwapSignal> {
    let decoded = match abi::decode(
        &[
            ParamType::Bytes,
            ParamType::Array(Box::new(ParamType::Bytes)),
            ParamType::Uint(256),
        ],
        args,
    )
    .or_else(|_| {
        abi::decode(
            &[
                ParamType::Bytes,
                ParamType::Array(Box::new(ParamType::Bytes)),
            ],
            args,
        )
    }) {
        Ok(decoded) => decoded,
        Err(_) => return Vec::new(),
    };
    let commands = match decoded.first() {
        Some(Token::Bytes(value)) => value,
        _ => return Vec::new(),
    };
    let inputs = match decoded.get(1) {
        Some(Token::Array(values)) => values,
        _ => return Vec::new(),
    };

    let mut signals = decode_universal_router_parts(selector, router, commands, inputs, 0);
    if signals.is_empty() {
        signals.extend(decode_universal_router_graph_candidates(
            selector, router, commands, inputs,
        ));
    }
    signals
}

fn decode_universal_router_graph_candidates(
    selector: [u8; 4],
    router: Address,
    commands: &[u8],
    inputs: &[Token],
) -> Vec<SwapSignal> {
    let mut graph = UniversalRouterRouteGraph {
        command_count: 0,
        swap_command_count: 0,
        hops: Vec::new(),
        unsupported_commands: Vec::new(),
    };
    collect_universal_router_graph(commands, inputs, 0, &mut graph);
    graph
        .hops
        .into_iter()
        .filter_map(|hop| universal_router_hop_to_signal(selector, router, hop))
        .collect()
}

fn universal_router_hop_to_signal(
    selector: [u8; 4],
    router: Address,
    hop: UniversalRouterHop,
) -> Option<SwapSignal> {
    let amount_in = hop.amount_in?;
    if amount_in.is_zero() {
        return None;
    }
    let path = vec![hop.token_in, hop.token_out];
    let kind = if hop.dex == "v3" {
        let fee_tier = hop.fee_tier?;
        SwapKind::V3 {
            fee_tier,
            encoded_path: encode_v3_path(hop.token_in, fee_tier, hop.token_out),
            hops: 1,
            exact_out: hop.exact_out,
        }
    } else {
        SwapKind::V2
    };
    Some(SwapSignal {
        selector,
        amount_in,
        amount_out_min: hop.amount_out,
        notional_wei: U256::zero(),
        path,
        router,
        kind,
    })
}

fn decode_universal_router_parts(
    selector: [u8; 4],
    router: Address,
    commands: &[u8],
    inputs: &[Token],
    depth: usize,
) -> Vec<SwapSignal> {
    if depth > 3 {
        return Vec::new();
    }

    let mut signals = Vec::new();
    for (idx, command) in commands.iter().copied().enumerate() {
        let input = match inputs.get(idx) {
            Some(Token::Bytes(value)) => value.as_slice(),
            Some(_) => continue,
            None => break,
        };
        match command & UNIVERSAL_ROUTER_COMMAND_MASK {
            0x00 => {
                if let Some(signal) = decode_universal_router_v3_exact_in(selector, router, input) {
                    signals.push(signal);
                }
            }
            0x01 => {
                if let Some(signal) = decode_universal_router_v3_exact_out(selector, router, input)
                {
                    signals.push(signal);
                }
            }
            0x08 => {
                if let Some(signal) = decode_universal_router_v2_exact_in(selector, router, input) {
                    signals.push(signal);
                }
            }
            0x09 => {
                if let Some(signal) = decode_universal_router_v2_exact_out(selector, router, input)
                {
                    signals.push(signal);
                }
            }
            0x21 => {
                if let Some((sub_commands, sub_inputs)) = decode_universal_router_sub_plan(input) {
                    signals.extend(decode_universal_router_parts(
                        selector,
                        router,
                        &sub_commands,
                        &sub_inputs,
                        depth + 1,
                    ));
                }
            }
            _ => {}
        }
    }
    signals
}

fn decode_universal_router_sub_plan(input: &[u8]) -> Option<(Vec<u8>, Vec<Token>)> {
    let decoded = abi::decode(
        &[
            ParamType::Bytes,
            ParamType::Array(Box::new(ParamType::Bytes)),
        ],
        input,
    )
    .ok()?;
    let commands = match decoded.first()? {
        Token::Bytes(value) => value,
        _ => return None,
    }
    .clone();
    let inputs = match decoded.get(1)? {
        Token::Array(values) => values.clone(),
        _ => return None,
    };
    Some((commands, inputs))
}

fn decode_universal_router_v2_exact_in(
    selector: [u8; 4],
    router: Address,
    input: &[u8],
) -> Option<SwapSignal> {
    let decoded = abi::decode(
        &[
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Array(Box::new(ParamType::Address)),
            ParamType::Bool,
        ],
        input,
    )
    .ok()?;
    let amount_in = decoded.get(1).and_then(token_as_uint)?;
    let amount_out_min = decoded.get(2).and_then(token_as_uint);
    let path = decoded.get(3).and_then(token_as_address_vec)?;
    Some(SwapSignal {
        selector,
        amount_in,
        amount_out_min,
        notional_wei: U256::zero(),
        path,
        router,
        kind: SwapKind::V2,
    })
}

fn decode_universal_router_v2_exact_out(
    selector: [u8; 4],
    router: Address,
    input: &[u8],
) -> Option<SwapSignal> {
    let decoded = abi::decode(
        &[
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Array(Box::new(ParamType::Address)),
            ParamType::Bool,
        ],
        input,
    )
    .ok()?;
    let amount_out = decoded.get(1).and_then(token_as_uint)?;
    let amount_in_max = decoded.get(2).and_then(token_as_uint)?;
    let path = decoded.get(3).and_then(token_as_address_vec)?;
    Some(SwapSignal {
        selector,
        amount_in: amount_in_max,
        amount_out_min: Some(amount_out),
        notional_wei: U256::zero(),
        path,
        router,
        kind: SwapKind::V2,
    })
}

fn decode_universal_router_v3_exact_in(
    selector: [u8; 4],
    router: Address,
    input: &[u8],
) -> Option<SwapSignal> {
    let decoded = abi::decode(
        &[
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Bytes,
            ParamType::Bool,
        ],
        input,
    )
    .ok()?;
    let amount_in = decoded.get(1).and_then(token_as_uint)?;
    let amount_out_min = decoded.get(2).and_then(token_as_uint);
    let path_bytes = match decoded.get(3)? {
        Token::Bytes(value) => value.clone(),
        _ => return None,
    };
    let parsed = parse_v3_path(&path_bytes)?;
    Some(SwapSignal {
        selector,
        amount_in,
        amount_out_min,
        notional_wei: U256::zero(),
        path: vec![parsed.token_in, parsed.edge_token_out],
        router,
        kind: SwapKind::V3 {
            fee_tier: parsed.first_fee_tier,
            encoded_path: encode_v3_path(
                parsed.edge_token_out,
                parsed.first_fee_tier,
                parsed.token_in,
            ),
            hops: parsed.hops,
            exact_out: false,
        },
    })
}

fn decode_universal_router_v3_exact_out(
    selector: [u8; 4],
    router: Address,
    input: &[u8],
) -> Option<SwapSignal> {
    let decoded = abi::decode(
        &[
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Bytes,
            ParamType::Bool,
        ],
        input,
    )
    .ok()?;
    let amount_out = decoded.get(1).and_then(token_as_uint)?;
    let amount_in_max = decoded.get(2).and_then(token_as_uint)?;
    let path_bytes = match decoded.get(3)? {
        Token::Bytes(value) => value.clone(),
        _ => return None,
    };
    let parsed = parse_v3_exact_out_path(&path_bytes)?;
    Some(SwapSignal {
        selector,
        amount_in: amount_in_max,
        amount_out_min: Some(amount_out),
        notional_wei: U256::zero(),
        path: vec![parsed.token_in, parsed.edge_token_out],
        router,
        kind: SwapKind::V3 {
            fee_tier: parsed.first_fee_tier,
            encoded_path: encode_v3_path(
                parsed.edge_token_out,
                parsed.first_fee_tier,
                parsed.token_in,
            ),
            hops: parsed.hops,
            exact_out: true,
        },
    })
}

fn decode_zero_ex_sell_to_uniswap(
    selector: [u8; 4],
    router: Address,
    args: &[u8],
) -> Option<SwapSignal> {
    let decoded = abi::decode(
        &[
            ParamType::Array(Box::new(ParamType::Address)),
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Bool,
        ],
        args,
    )
    .ok()?;
    let path = decoded.first().and_then(token_as_address_vec)?;
    let amount_in = decoded.get(1).and_then(token_as_uint)?;
    let amount_out_min = decoded.get(2).and_then(token_as_uint);
    Some(SwapSignal {
        selector,
        amount_in,
        amount_out_min,
        notional_wei: U256::zero(),
        path,
        router,
        kind: SwapKind::V2,
    })
}

fn estimate_notional_wei(
    signal: &SwapSignal,
    monitored_tokens: &[MonitoredTokenConfig],
) -> Option<U256> {
    if matches!(
        signal.selector,
        SWAP_EXACT_ETH_FOR_TOKENS | SWAP_EXACT_ETH_FOR_TOKENS_SUPPORTING_FEE
    ) {
        return Some(signal.amount_in);
    }

    let input = signal.path.first()?;
    let token = monitored_tokens
        .iter()
        .find(|token| token.address == *input)?;
    let decimals_factor = 10f64.powi(i32::from(token.decimals));
    let normalized = signal.amount_in.to_string().parse::<f64>().ok()? / decimals_factor;
    let value_eth = normalized * token.price_eth;
    ethers::utils::parse_ether(value_eth.to_string()).ok()
}

fn selector(tx: &Transaction) -> Option<[u8; 4]> {
    let input = tx.input.as_ref();
    if let Some(index) = crate::mev::decoder::simd::find_selector(input) {
        return Some(crate::mev::decoder::simd::SELECTORS[index]);
    }
    (input.len() >= 4).then(|| [input[0], input[1], input[2], input[3]])
}

fn decode_reject_selector_reason(tx: &Transaction) -> String {
    let selector = selector(tx).unwrap_or([0, 0, 0, 0]);
    format!(
        "selector=0x{:02x}{:02x}{:02x}{:02x} source={}",
        selector[0],
        selector[1],
        selector[2],
        selector[3],
        aggregator_name_from_tx(tx).unwrap_or("direct_or_unknown")
    )
}

fn aggregator_name_from_tx(tx: &Transaction) -> Option<&'static str> {
    match selector(tx)? {
        UNIVERSAL_ROUTER_EXECUTE | UNIVERSAL_ROUTER_EXECUTE_NO_DEADLINE => Some("universal_router"),
        ZERO_EX_TRANSFORM_ERC20 | ZERO_EX_SELL_TO_UNISWAP => Some("0x_matcha"),
        ONE_INCH_SWAP | ONE_INCH_UNOSWAP => Some("1inch"),
        PARASWAP_SIMPLE_SWAP => Some("paraswap"),
        ODOS_SWAP_COMPACT => Some("odos"),
        KYBER_SWAP => Some("kyber"),
        _ => None,
    }
}

#[derive(Debug, Clone)]
struct AggregatorRouteIntel {
    command_count: u64,
    swap_command_count: u64,
    split_ratio_bps: u64,
    dex_sequence: Vec<String>,
    path: Vec<String>,
    route_inefficiency_score: f64,
    liquidity_distortion_score: f64,
    hop_profitability_rank: Vec<String>,
}

#[derive(Debug, Clone)]
struct UniversalRouterRouteGraph {
    command_count: u64,
    swap_command_count: u64,
    hops: Vec<UniversalRouterHop>,
    unsupported_commands: Vec<String>,
}

#[derive(Debug, Clone)]
struct UniversalRouterHop {
    dex: &'static str,
    command: &'static str,
    token_in: Address,
    token_out: Address,
    amount_in: Option<U256>,
    amount_out: Option<U256>,
    fee_tier: Option<u32>,
    exact_out: bool,
    depth: usize,
}

fn aggregator_intelligence_sample(
    config: &Config,
    tx: &Transaction,
    tx_hash: H256,
    source: &str,
) -> Option<EdgeMetadata> {
    let selector = selector(tx).unwrap_or([0, 0, 0, 0]);
    let intel = aggregator_route_intel(config, tx, source);
    if source == "universal_router" && intel.swap_command_count == 0 {
        return None;
    }
    let decode_hints = if source == "universal_router" {
        universal_router_decode_hints(tx)
    } else {
        Vec::new()
    };
    let reason = if decode_hints.is_empty() {
        "aggregator flow not decoded into executable graph yet".to_string()
    } else {
        format!(
            "aggregator flow not decoded into executable graph yet: {}",
            decode_hints.join(" | ")
        )
    };
    Some(EdgeMetadata {
        victim_tx: short_hash(tx_hash),
        selector: format!(
            "0x{:02x}{:02x}{:02x}{:02x}",
            selector[0], selector[1], selector[2], selector[3]
        ),
        status: "aggregator_candidate".to_string(),
        reason,
        route_kind: "aggregator".to_string(),
        path: intel.path,
        hops: intel.command_count.max(1),
        impacted_pools: Vec::new(),
        slippage_window_score: aggregator_slippage_hint(source, tx),
        pool_imbalance_score: intel.liquidity_distortion_score,
        cross_dex_deviation_bps: ((intel.route_inefficiency_score * 10_000.0).round() as i64)
            .clamp(0, 10_000),
        gas_estimate: tx.gas.as_u64(),
        simulated_extraction_native: 0.0,
        aggregator_type: source.to_string(),
        route_complexity: intel.command_count,
        split_ratio_bps: intel.split_ratio_bps,
        dex_sequence: intel.dex_sequence,
        route_inefficiency_score: intel.route_inefficiency_score,
        liquidity_distortion_score: intel.liquidity_distortion_score,
        hop_profitability_rank: if decode_hints.is_empty() {
            intel.hop_profitability_rank
        } else {
            decode_hints
        },
        best_size_bps: 0,
        amount_in_wei: tx.value.to_string(),
        amount_out_wei: "0".to_string(),
        gross_edge_wei: "0".to_string(),
        gross_edge_native: 0.0,
        repayment_wei: "0".to_string(),
        repayment_native: 0.0,
        price_impact_bps: 0,
        self_slippage_bps: 0,
        pool: "unknown".to_string(),
        factory: "aggregator".to_string(),
        router: tx
            .to
            .map(|address| format!("{address:?}"))
            .unwrap_or_else(|| "unknown".to_string()),
        token_in: "unknown".to_string(),
        token_out: "unknown".to_string(),
    })
}

fn decode_reject_edge_sample(
    config: &Config,
    tx: &Transaction,
    tx_hash: H256,
    reason: &str,
) -> Option<EdgeMetadata> {
    let selector = selector(tx)?;
    let path_hint = monitored_token_path_hint(config, tx.input.as_ref());
    if path_hint.is_empty() {
        return None;
    }
    Some(EdgeMetadata {
        victim_tx: short_hash(tx_hash),
        selector: format!(
            "0x{:02x}{:02x}{:02x}{:02x}",
            selector[0], selector[1], selector[2], selector[3]
        ),
        status: "decode_reject".to_string(),
        reason: reason.to_string(),
        route_kind: "unknown".to_string(),
        path: path_hint,
        hops: 0,
        impacted_pools: Vec::new(),
        slippage_window_score: 0.0,
        pool_imbalance_score: 0.0,
        cross_dex_deviation_bps: 0,
        gas_estimate: tx.gas.as_u64(),
        simulated_extraction_native: 0.0,
        aggregator_type: "unknown".to_string(),
        route_complexity: (tx.input.as_ref().len().saturating_sub(4) / 128).max(1) as u64,
        split_ratio_bps: 0,
        dex_sequence: vec!["decode_reject".to_string()],
        route_inefficiency_score: 0.0,
        liquidity_distortion_score: 0.0,
        hop_profitability_rank: vec!["decode rejected after monitored token hint".to_string()],
        best_size_bps: 0,
        amount_in_wei: tx.value.to_string(),
        amount_out_wei: "0".to_string(),
        gross_edge_wei: "0".to_string(),
        gross_edge_native: 0.0,
        repayment_wei: "0".to_string(),
        repayment_native: 0.0,
        price_impact_bps: 0,
        self_slippage_bps: 0,
        pool: "unknown".to_string(),
        factory: "unknown".to_string(),
        router: tx
            .to
            .map(|address| format!("{address:?}"))
            .unwrap_or_else(|| "unknown".to_string()),
        token_in: "unknown".to_string(),
        token_out: "unknown".to_string(),
    })
}

fn aggregator_route_intel(config: &Config, tx: &Transaction, source: &str) -> AggregatorRouteIntel {
    if source == "universal_router" {
        if let Some(intel) = universal_router_graph_intel(tx) {
            return intel;
        }
        if let Some(intel) = universal_router_intel(tx) {
            return intel;
        }
    }

    let input = tx.input.as_ref();
    let matched_tokens = monitored_token_path_hint(config, input);
    let complexity = ((input.len().saturating_sub(4) / 128).max(1).min(24)) as u64;
    let route_inefficiency_score = match source {
        "odos" | "1inch" | "0x_matcha" => 0.72,
        "paraswap" | "kyber" => 0.66,
        _ => 0.58,
    };
    let split_ratio_bps = if input.len() > 900 { 3_000 } else { 0 };
    AggregatorRouteIntel {
        command_count: complexity,
        swap_command_count: 1,
        split_ratio_bps,
        dex_sequence: vec![source.to_string(), "unknown_dex".to_string()],
        path: matched_tokens,
        route_inefficiency_score,
        liquidity_distortion_score: (0.30 + route_inefficiency_score * 0.45).clamp(0.0, 1.0),
        hop_profitability_rank: vec![
            "hop_1: route intent unresolved".to_string(),
            "hop_2: decode aggregator inputs next".to_string(),
        ],
    }
}

fn universal_router_graph_intel(tx: &Transaction) -> Option<AggregatorRouteIntel> {
    let graph = universal_router_route_graph(tx.input.as_ref().get(4..)?)?;
    if graph.hops.is_empty() && graph.unsupported_commands.is_empty() {
        return None;
    }

    let mut path = Vec::new();
    for hop in &graph.hops {
        let token_in = format!("{:?}", hop.token_in);
        let token_out = format!("{:?}", hop.token_out);
        if !path.contains(&token_in) {
            path.push(token_in);
        }
        if !path.contains(&token_out) {
            path.push(token_out);
        }
    }

    let mut dex_sequence = graph
        .hops
        .iter()
        .map(|hop| hop.command.to_string())
        .collect::<Vec<_>>();
    if dex_sequence.is_empty() {
        dex_sequence.push("universal_router".to_string());
    }
    for command in graph.unsupported_commands.iter().take(3) {
        dex_sequence.push(format!("unsupported:{command}"));
    }

    let split_ratio_bps = if graph.swap_command_count > 1 {
        ((graph.swap_command_count - 1) * 2_000).min(8_000)
    } else {
        0
    };
    let complexity_score = (graph.command_count as f64 / 10.0).clamp(0.0, 1.0);
    let split_score = (split_ratio_bps as f64 / 10_000.0).clamp(0.0, 1.0);
    let unsupported_score = (graph.unsupported_commands.len() as f64 / 6.0).clamp(0.0, 1.0);
    let route_inefficiency_score =
        (0.24 + complexity_score * 0.28 + split_score * 0.34 + unsupported_score * 0.14)
            .clamp(0.0, 1.0);
    let liquidity_distortion_score =
        (0.20 + graph.hops.len() as f64 * 0.06 + split_score * 0.40).clamp(0.0, 1.0);

    Some(AggregatorRouteIntel {
        command_count: graph.command_count,
        swap_command_count: graph.swap_command_count,
        split_ratio_bps,
        dex_sequence,
        path,
        route_inefficiency_score,
        liquidity_distortion_score,
        hop_profitability_rank: universal_router_graph_hop_rank(&graph, route_inefficiency_score),
    })
}

fn universal_router_intel(tx: &Transaction) -> Option<AggregatorRouteIntel> {
    let args = tx.input.as_ref().get(4..)?;
    let decoded = abi::decode(
        &[
            ParamType::Bytes,
            ParamType::Array(Box::new(ParamType::Bytes)),
            ParamType::Uint(256),
        ],
        args,
    )
    .or_else(|_| {
        abi::decode(
            &[
                ParamType::Bytes,
                ParamType::Array(Box::new(ParamType::Bytes)),
            ],
            args,
        )
    })
    .ok()?;
    let commands = match decoded.first()? {
        Token::Bytes(value) => value.clone(),
        _ => return None,
    };
    let command_count = commands.len() as u64;
    let mut swap_command_count = 0u64;
    let mut dex_sequence = Vec::new();
    let mut wraps = 0u64;
    let mut permit2 = 0u64;
    for command in commands {
        let opcode = command & UNIVERSAL_ROUTER_COMMAND_MASK;
        match opcode {
            0x00 => {
                swap_command_count += 1;
                dex_sequence.push("v3_exact_in".to_string());
            }
            0x01 => {
                swap_command_count += 1;
                dex_sequence.push("v3_exact_out".to_string());
            }
            0x08 => {
                swap_command_count += 1;
                dex_sequence.push("v2_exact_in".to_string());
            }
            0x09 => {
                swap_command_count += 1;
                dex_sequence.push("v2_exact_out".to_string());
            }
            0x0b | 0x0c => wraps += 1,
            0x02 | 0x03 | 0x0a | 0x0d => permit2 += 1,
            _ => {}
        }
    }
    if dex_sequence.is_empty() {
        dex_sequence.push("universal_router".to_string());
    }
    let split_ratio_bps = if swap_command_count > 1 {
        ((swap_command_count - 1) * 2_500).min(8_000)
    } else {
        0
    };
    let complexity_score = (command_count as f64 / 8.0).clamp(0.0, 1.0);
    let split_score = (split_ratio_bps as f64 / 10_000.0).clamp(0.0, 1.0);
    let route_inefficiency_score =
        (0.35 + complexity_score * 0.35 + split_score * 0.30).clamp(0.0, 1.0);
    let liquidity_distortion_score =
        (0.25 + split_score * 0.45 + (wraps as f64 * 0.04) + (permit2 as f64 * 0.03))
            .clamp(0.0, 1.0);
    Some(AggregatorRouteIntel {
        command_count,
        swap_command_count,
        split_ratio_bps,
        dex_sequence,
        path: Vec::new(),
        route_inefficiency_score,
        liquidity_distortion_score,
        hop_profitability_rank: universal_router_hop_rank(
            swap_command_count,
            split_ratio_bps,
            route_inefficiency_score,
        ),
    })
}

fn universal_router_route_graph(args: &[u8]) -> Option<UniversalRouterRouteGraph> {
    let decoded = abi::decode(
        &[
            ParamType::Bytes,
            ParamType::Array(Box::new(ParamType::Bytes)),
            ParamType::Uint(256),
        ],
        args,
    )
    .or_else(|_| {
        abi::decode(
            &[
                ParamType::Bytes,
                ParamType::Array(Box::new(ParamType::Bytes)),
            ],
            args,
        )
    })
    .ok()?;
    let commands = match decoded.first()? {
        Token::Bytes(value) => value.as_slice(),
        _ => return None,
    };
    let inputs = match decoded.get(1)? {
        Token::Array(values) => values.as_slice(),
        _ => return None,
    };
    let mut graph = UniversalRouterRouteGraph {
        command_count: 0,
        swap_command_count: 0,
        hops: Vec::new(),
        unsupported_commands: Vec::new(),
    };
    collect_universal_router_graph(commands, inputs, 0, &mut graph);
    Some(graph)
}

fn collect_universal_router_graph(
    commands: &[u8],
    inputs: &[Token],
    depth: usize,
    graph: &mut UniversalRouterRouteGraph,
) {
    if depth > 3 {
        graph
            .unsupported_commands
            .push("subplan_depth_limit".to_string());
        return;
    }

    for (idx, command) in commands.iter().copied().enumerate() {
        graph.command_count = graph.command_count.saturating_add(1);
        let opcode = command & UNIVERSAL_ROUTER_COMMAND_MASK;
        let input = match inputs.get(idx) {
            Some(Token::Bytes(value)) => value.as_slice(),
            Some(_) => {
                graph.unsupported_commands.push(format!(
                    "{}:input_not_bytes",
                    universal_router_command_name(opcode)
                ));
                continue;
            }
            None => {
                graph.unsupported_commands.push(format!(
                    "{}:missing_input",
                    universal_router_command_name(opcode)
                ));
                break;
            }
        };

        match opcode {
            0x00 => {
                graph.swap_command_count = graph.swap_command_count.saturating_add(1);
                graph
                    .hops
                    .extend(universal_router_v3_hops(input, false, depth));
            }
            0x01 => {
                graph.swap_command_count = graph.swap_command_count.saturating_add(1);
                graph
                    .hops
                    .extend(universal_router_v3_hops(input, true, depth));
            }
            0x08 => {
                graph.swap_command_count = graph.swap_command_count.saturating_add(1);
                graph
                    .hops
                    .extend(universal_router_v2_hops(input, false, depth));
            }
            0x09 => {
                graph.swap_command_count = graph.swap_command_count.saturating_add(1);
                graph
                    .hops
                    .extend(universal_router_v2_hops(input, true, depth));
            }
            0x21 => {
                if let Some((sub_commands, sub_inputs)) = decode_universal_router_sub_plan(input) {
                    collect_universal_router_graph(&sub_commands, &sub_inputs, depth + 1, graph);
                } else {
                    graph
                        .unsupported_commands
                        .push("execute_sub_plan:decode_failed".to_string());
                }
            }
            0x10 | 0x40 => {
                graph
                    .unsupported_commands
                    .push(universal_router_command_name(opcode).to_string());
            }
            _ => {}
        }
    }
}

fn universal_router_v2_hops(
    input: &[u8],
    exact_out: bool,
    depth: usize,
) -> Vec<UniversalRouterHop> {
    let decoded = match abi::decode(
        &[
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Array(Box::new(ParamType::Address)),
            ParamType::Bool,
        ],
        input,
    ) {
        Ok(decoded) => decoded,
        Err(_) => return Vec::new(),
    };
    let amount_primary = decoded.get(1).and_then(token_as_uint);
    let amount_secondary = decoded.get(2).and_then(token_as_uint);
    let path = match decoded.get(3).and_then(token_as_address_vec) {
        Some(path) => path,
        None => return Vec::new(),
    };
    path.windows(2)
        .map(|tokens| UniversalRouterHop {
            dex: "v2",
            command: if exact_out {
                "v2_exact_out"
            } else {
                "v2_exact_in"
            },
            token_in: tokens[0],
            token_out: tokens[1],
            amount_in: if exact_out {
                amount_secondary
            } else {
                amount_primary
            },
            amount_out: if exact_out {
                amount_primary
            } else {
                amount_secondary
            },
            fee_tier: None,
            exact_out,
            depth,
        })
        .collect()
}

fn universal_router_v3_hops(
    input: &[u8],
    exact_out: bool,
    depth: usize,
) -> Vec<UniversalRouterHop> {
    let decoded = match abi::decode(
        &[
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Bytes,
            ParamType::Bool,
        ],
        input,
    ) {
        Ok(decoded) => decoded,
        Err(_) => return Vec::new(),
    };
    let amount_primary = decoded.get(1).and_then(token_as_uint);
    let amount_secondary = decoded.get(2).and_then(token_as_uint);
    let path = match decoded.get(3) {
        Some(Token::Bytes(value)) => value.as_slice(),
        _ => return Vec::new(),
    };
    let segments = parse_v3_path_segments(path);
    if exact_out {
        segments
            .into_iter()
            .rev()
            .map(|segment| UniversalRouterHop {
                dex: "v3",
                command: "v3_exact_out",
                token_in: segment.token_out,
                token_out: segment.token_in,
                amount_in: amount_secondary,
                amount_out: amount_primary,
                fee_tier: Some(segment.fee_tier),
                exact_out,
                depth,
            })
            .collect()
    } else {
        segments
            .into_iter()
            .map(|segment| UniversalRouterHop {
                dex: "v3",
                command: "v3_exact_in",
                token_in: segment.token_in,
                token_out: segment.token_out,
                amount_in: amount_primary,
                amount_out: amount_secondary,
                fee_tier: Some(segment.fee_tier),
                exact_out,
                depth,
            })
            .collect()
    }
}

fn universal_router_decode_hints(tx: &Transaction) -> Vec<String> {
    let Some(args) = tx.input.as_ref().get(4..) else {
        return vec!["decode: calldata shorter than selector".to_string()];
    };
    let decoded = match abi::decode(
        &[
            ParamType::Bytes,
            ParamType::Array(Box::new(ParamType::Bytes)),
            ParamType::Uint(256),
        ],
        args,
    )
    .or_else(|_| {
        abi::decode(
            &[
                ParamType::Bytes,
                ParamType::Array(Box::new(ParamType::Bytes)),
            ],
            args,
        )
    }) {
        Ok(decoded) => decoded,
        Err(err) => return vec![format!("decode: abi mismatch {err}")],
    };
    let commands = match decoded.first() {
        Some(Token::Bytes(value)) => value,
        _ => return vec!["decode: commands not bytes".to_string()],
    };
    let inputs = match decoded.get(1) {
        Some(Token::Array(values)) => values,
        _ => return vec!["decode: inputs not bytes[]".to_string()],
    };

    let mut hints = Vec::new();
    for (idx, command) in commands.iter().copied().enumerate().take(8) {
        let opcode = command & UNIVERSAL_ROUTER_COMMAND_MASK;
        let input = match inputs.get(idx) {
            Some(Token::Bytes(value)) => value.as_slice(),
            Some(_) => {
                hints.push(format!(
                    "cmd#{idx} {}: input not bytes",
                    universal_router_command_name(opcode)
                ));
                continue;
            }
            None => {
                hints.push(format!(
                    "cmd#{idx} {}: missing input",
                    universal_router_command_name(opcode)
                ));
                continue;
            }
        };
        let status = match opcode {
            0x00 => universal_router_v3_exact_in_status(input),
            0x01 => universal_router_v3_exact_out_status(input),
            0x08 => universal_router_v2_swap_status(input, false),
            0x09 => universal_router_v2_swap_status(input, true),
            _ => "unsupported command".to_string(),
        };
        hints.push(format!(
            "cmd#{idx} {} input={}b {status}",
            universal_router_command_name(opcode),
            input.len()
        ));
    }
    if commands.len() > 8 {
        hints.push(format!(
            "{} additional commands omitted",
            commands.len() - 8
        ));
    }
    hints
}

fn universal_router_command_name(opcode: u8) -> &'static str {
    match opcode {
        0x00 => "v3_exact_in",
        0x01 => "v3_exact_out",
        0x02 => "permit2_transfer_from",
        0x03 => "permit2_permit_batch",
        0x04 => "sweep",
        0x05 => "transfer",
        0x06 => "pay_portion",
        0x08 => "v2_exact_in",
        0x09 => "v2_exact_out",
        0x0a => "permit2_permit",
        0x0b => "wrap_eth",
        0x0c => "unwrap_weth",
        0x0d => "permit2_transfer_from_batch",
        0x0e => "balance_check_erc20",
        0x10 => "v4_swap",
        0x21 => "execute_sub_plan",
        0x40 => "across_v4_deposit_v3",
        _ => "unknown",
    }
}

fn universal_router_graph_hop_rank(
    graph: &UniversalRouterRouteGraph,
    route_inefficiency_score: f64,
) -> Vec<String> {
    if graph.hops.is_empty() {
        let mut rank = vec!["no executable v2/v3 hop extracted".to_string()];
        for command in graph.unsupported_commands.iter().take(4) {
            rank.push(format!("unsupported command: {command}"));
        }
        return rank;
    }

    let mut rank = graph
        .hops
        .iter()
        .enumerate()
        .map(|(idx, hop)| {
            let amount = hop
                .amount_in
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let fee = hop
                .fee_tier
                .map(|fee| format!(" fee={fee}"))
                .unwrap_or_default();
            let score = ((route_inefficiency_score * 100.0) + idx as f64 * 4.0).round() as u64;
            format!(
                "candidate_hop_{}: {} {} {:?}->{:?} amount_in={} score={} depth={}{} exact_out={}",
                idx + 1,
                hop.dex,
                hop.command,
                hop.token_in,
                hop.token_out,
                amount,
                score,
                hop.depth,
                fee,
                hop.exact_out
            )
        })
        .collect::<Vec<_>>();
    for command in graph.unsupported_commands.iter().take(3) {
        rank.push(format!("unsupported command: {command}"));
    }
    rank
}

fn universal_router_v2_swap_status(input: &[u8], exact_out: bool) -> String {
    let decoded = abi::decode(
        &[
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Array(Box::new(ParamType::Address)),
            ParamType::Bool,
        ],
        input,
    );
    match decoded {
        Ok(values) => {
            let path_len = values
                .get(3)
                .and_then(token_as_address_vec)
                .map(|path| path.len())
                .unwrap_or_default();
            let mode = if exact_out { "amountInMax" } else { "amountIn" };
            format!("decoded path_len={path_len} notional={mode}")
        }
        Err(err) => format!("abi mismatch {err}"),
    }
}

fn universal_router_v3_exact_in_status(input: &[u8]) -> String {
    let decoded = abi::decode(
        &[
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Bytes,
            ParamType::Bool,
        ],
        input,
    );
    match decoded {
        Ok(values) => {
            let path = match values.get(3) {
                Some(Token::Bytes(value)) => value.as_slice(),
                _ => return "path not bytes".to_string(),
            };
            match parse_v3_path(path) {
                Some(parsed) => format!(
                    "decoded hops={} first_fee={} path={}b",
                    parsed.hops,
                    parsed.first_fee_tier,
                    path.len()
                ),
                None => format!("invalid v3 path {}b", path.len()),
            }
        }
        Err(err) => format!("abi mismatch {err}"),
    }
}

fn universal_router_v3_exact_out_status(input: &[u8]) -> String {
    let decoded = abi::decode(
        &[
            ParamType::Address,
            ParamType::Uint(256),
            ParamType::Uint(256),
            ParamType::Bytes,
            ParamType::Bool,
        ],
        input,
    );
    match decoded {
        Ok(values) => {
            let path = match values.get(3) {
                Some(Token::Bytes(value)) => value.as_slice(),
                _ => return "path not bytes".to_string(),
            };
            match parse_v3_path(path) {
                Some(parsed) if parsed.hops == 1 => format!(
                    "decoded single_hop first_fee={} path={}b",
                    parsed.first_fee_tier,
                    path.len()
                ),
                Some(parsed) => format!(
                    "decoded but unsupported multi_hop_exact_out hops={}",
                    parsed.hops
                ),
                None => format!("invalid v3 path {}b", path.len()),
            }
        }
        Err(err) => format!("abi mismatch {err}"),
    }
}

fn universal_router_hop_rank(
    swap_command_count: u64,
    split_ratio_bps: u64,
    route_inefficiency_score: f64,
) -> Vec<String> {
    if swap_command_count == 0 {
        return vec!["no swap command extracted".to_string()];
    }
    (0..swap_command_count.min(4))
        .map(|idx| {
            let score = (route_inefficiency_score * 100.0).round() as u64;
            format!(
                "hop_{}: score={} split={}bps",
                idx + 1,
                score,
                split_ratio_bps
            )
        })
        .collect()
}

fn monitored_token_path_hint(config: &Config, input: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    for token in &config.monitored_tokens {
        if input
            .windows(20)
            .any(|window| window == token.address.as_bytes())
        {
            out.push(format!("{:?}", token.address));
        }
        if out.len() >= 6 {
            break;
        }
    }
    out
}

fn aggregator_slippage_hint(source: &str, tx: &Transaction) -> f64 {
    let size_hint = (tx.input.as_ref().len() as f64 / 1600.0).clamp(0.0, 0.35);
    let source_hint = match source {
        "universal_router" => 0.62,
        "odos" | "1inch" => 0.58,
        "0x_matcha" | "paraswap" | "kyber" => 0.52,
        _ => 0.45,
    };
    (source_hint + size_hint).clamp(0.0, 0.95)
}

fn selector_heuristic_factor(selector: [u8; 4]) -> f64 {
    match selector {
        V3_EXACT_INPUT_SINGLE => 1.16,
        V3_EXACT_INPUT => 1.04,
        SWAP_EXACT_ETH_FOR_TOKENS | SWAP_EXACT_ETH_FOR_TOKENS_SUPPORTING_FEE => 1.18,
        SWAP_EXACT_TOKENS_FOR_ETH | SWAP_EXACT_TOKENS_FOR_ETH_SUPPORTING_FEE => 1.08,
        SWAP_EXACT_TOKENS_FOR_TOKENS | SWAP_EXACT_TOKENS_FOR_TOKENS_SUPPORTING_FEE => 0.94,
        _ => 0.82,
    }
}

fn notional_size_bucket(notional_eth: f64, min_large_swap_eth: f64) -> u8 {
    if notional_eth < min_large_swap_eth * 1.25 {
        0
    } else if notional_eth < min_large_swap_eth * 2.5 {
        1
    } else {
        2
    }
}

fn fast_path_penalty(path_len: usize) -> f64 {
    match path_len {
        0 | 1 | 2 => 0.0,
        3 => 0.18,
        4 => 0.30,
        _ => 0.42,
    }
}

fn scavenger_fast_try_score(
    signal: &SwapSignal,
    gas_ratio: f64,
    notional_eth: f64,
    config: &Config,
    context_signal: ContextSignal,
) -> f64 {
    let slippage_window = scavenger_slippage_window_hint(signal);
    let pool_imbalance = scavenger_impact_imbalance_hint(
        notional_eth,
        config.mev.effective_min_large_swap_eth(),
        signal.path_len(),
    );
    let low_competition = ((1.35 - gas_ratio) / 1.35).clamp(0.0, 1.0) * 0.55
        + context_signal.priority_score.clamp(0.0, 1.0) * 0.25
        + (1.0 - context_signal.toxicity_score.clamp(0.0, 1.0)) * 0.20;
    let route_quality = scavenger_route_inefficiency_hint(signal);
    let victim_size_factor =
        (notional_eth / config.mev.effective_min_large_swap_eth().max(0.000_001) / 4.0)
            .clamp(0.0, 1.0);

    (slippage_window * 0.35
        + pool_imbalance * 0.25
        + low_competition * 0.20
        + route_quality * 0.10
        + victim_size_factor * 0.10)
        .clamp(0.0, 1.0)
}

fn scavenger_slippage_window_hint(signal: &SwapSignal) -> f64 {
    let supporting_fee_selector = matches!(
        signal.selector,
        SWAP_EXACT_TOKENS_FOR_TOKENS_SUPPORTING_FEE
            | SWAP_EXACT_ETH_FOR_TOKENS_SUPPORTING_FEE
            | SWAP_EXACT_TOKENS_FOR_ETH_SUPPORTING_FEE
    );
    if signal
        .amount_out_min
        .map(|value| value.is_zero())
        .unwrap_or(false)
    {
        0.95
    } else if supporting_fee_selector {
        0.72
    } else if signal.path_len() >= 3 {
        0.52
    } else {
        0.30
    }
}

fn scavenger_impact_imbalance_hint(
    notional_eth: f64,
    min_large_swap_eth: f64,
    path_len: usize,
) -> f64 {
    let size_pressure = (notional_eth / min_large_swap_eth.max(0.000_001) / 3.0).clamp(0.0, 1.0);
    let simple_pool_bonus = if path_len <= 2 { 0.22 } else { 0.08 };
    (size_pressure * 0.78 + simple_pool_bonus).clamp(0.0, 1.0)
}

fn scavenger_route_inefficiency_hint(signal: &SwapSignal) -> f64 {
    match &signal.kind {
        SwapKind::V3 { hops, .. } if *hops >= 2 => 0.78,
        SwapKind::V2 if signal.path_len() >= 3 => 0.72,
        SwapKind::V3 { .. } => 0.46,
        SwapKind::V2 => 0.38,
    }
}

struct ParsedV3Path {
    token_in: Address,
    edge_token_out: Address,
    first_fee_tier: u32,
    hops: usize,
}

#[derive(Debug, Clone, Copy)]
struct V3PathSegment {
    token_in: Address,
    token_out: Address,
    fee_tier: u32,
}

fn parse_v3_path(path: &[u8]) -> Option<ParsedV3Path> {
    if path.len() < 43 {
        return None;
    }
    let token_in = Address::from_slice(&path[0..20]);
    let edge_token_out = Address::from_slice(&path[23..43]);
    let first_fee_tier = u32::from_be_bytes([0, path[20], path[21], path[22]]);
    let hops = (path.len().saturating_sub(20)) / 23;
    Some(ParsedV3Path {
        token_in,
        edge_token_out,
        first_fee_tier,
        hops,
    })
}

fn parse_v3_path_segments(path: &[u8]) -> Vec<V3PathSegment> {
    if path.len() < 43 {
        return Vec::new();
    }
    let hop_count = (path.len().saturating_sub(20)) / 23;
    let mut segments = Vec::with_capacity(hop_count);
    for hop in 0..hop_count {
        let token_in_offset = hop * 23;
        let fee_offset = token_in_offset + 20;
        let token_out_offset = fee_offset + 3;
        if token_out_offset + 20 > path.len() {
            break;
        }
        segments.push(V3PathSegment {
            token_in: Address::from_slice(&path[token_in_offset..token_in_offset + 20]),
            token_out: Address::from_slice(&path[token_out_offset..token_out_offset + 20]),
            fee_tier: u32::from_be_bytes([
                0,
                path[fee_offset],
                path[fee_offset + 1],
                path[fee_offset + 2],
            ]),
        });
    }
    segments
}

fn parse_v3_exact_out_path(path: &[u8]) -> Option<ParsedV3Path> {
    let forward = parse_v3_path(path)?;
    if forward.hops != 1 {
        return None;
    }
    Some(ParsedV3Path {
        token_in: forward.edge_token_out,
        edge_token_out: forward.token_in,
        first_fee_tier: forward.first_fee_tier,
        hops: forward.hops,
    })
}

fn encode_v3_path(token_in: Address, fee_tier: u32, token_out: Address) -> ethers::types::Bytes {
    let mut out = Vec::with_capacity(43);
    out.extend_from_slice(token_in.as_bytes());
    let fee = fee_tier.to_be_bytes();
    out.extend_from_slice(&fee[1..]);
    out.extend_from_slice(token_out.as_bytes());
    ethers::types::Bytes::from(out)
}

fn heuristic_gas_baseline_gwei(config: &Config) -> f64 {
    match config.network.as_str() {
        "arbitrum" => 0.15,
        "polygon" => 80.0,
        "bsc" => 3.0,
        _ => 25.0,
    }
}

fn refresh_historical_profiles(
    adaptive: &crate::mev::adaptive::SharedAdaptivePolicy,
    storage: &Storage,
    dashboard: &DashboardHandle,
) {
    match storage.outcome_profiles(3, 256) {
        Ok(profiles) => {
            let profile_count = profiles.len();
            if let Ok(mut model) = adaptive.lock() {
                model.apply_historical_profiles(profiles);
            }
            dashboard.event(
                "info",
                format!(
                    "historical profile refresh loaded {} pair/router/hour profiles",
                    profile_count
                ),
            );
        }
        Err(err) => {
            warn!("historical profile refresh failed: {}", err);
        }
    }
}

fn spawn_historical_profile_refresher(
    adaptive: crate::mev::adaptive::SharedAdaptivePolicy,
    storage: Storage,
    dashboard: DashboardHandle,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(300));

        loop {
            tick.tick().await;
            refresh_historical_profiles(&adaptive, &storage, &dashboard);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethers::abi::encode;

    #[test]
    fn adaptive_gas_cap_allows_positive_two_cent_edge_near_hard_cap() {
        let cap = adaptive_gas_cap_gwei(0.0243, 0.1, 1_000.0);

        assert!((cap - 950.0).abs() < f64::EPSILON);
    }

    #[test]
    fn adaptive_gas_cap_keeps_small_positive_edges_below_two_cent_tier() {
        let cap = adaptive_gas_cap_gwei(0.01, 0.1, 1_000.0);

        assert!(cap < 950.0);
        assert!(cap >= 350.0);
    }

    #[test]
    fn universal_router_v2_exact_out_decodes_with_amount_in_max() {
        let router = Address::from_low_u64_be(10);
        let token_in = Address::from_low_u64_be(1);
        let token_out = Address::from_low_u64_be(2);
        let input = encode(&[
            Token::Address(Address::from_low_u64_be(99)),
            Token::Uint(U256::from(50u64)),
            Token::Uint(U256::from(75u64)),
            Token::Array(vec![Token::Address(token_in), Token::Address(token_out)]),
            Token::Bool(true),
        ]);
        let args = encode(&[
            Token::Bytes(vec![0x09]),
            Token::Array(vec![Token::Bytes(input)]),
        ]);

        let signal = decode_universal_router_swap(UNIVERSAL_ROUTER_EXECUTE, router, &args).unwrap();

        assert_eq!(signal.amount_in, U256::from(75u64));
        assert_eq!(signal.amount_out_min, Some(U256::from(50u64)));
        assert_eq!(signal.path, vec![token_in, token_out]);
        assert!(matches!(signal.kind, SwapKind::V2));
    }

    #[test]
    fn universal_router_v3_exact_out_decodes_single_hop_only() {
        let router = Address::from_low_u64_be(10);
        let token_out = Address::from_low_u64_be(2);
        let token_in = Address::from_low_u64_be(1);
        let path = encode_v3_path(token_out, 500, token_in);
        let input = encode(&[
            Token::Address(Address::from_low_u64_be(99)),
            Token::Uint(U256::from(50u64)),
            Token::Uint(U256::from(75u64)),
            Token::Bytes(path.to_vec()),
            Token::Bool(true),
        ]);
        let args = encode(&[
            Token::Bytes(vec![0x01]),
            Token::Array(vec![Token::Bytes(input)]),
        ]);

        let signal = decode_universal_router_swap(UNIVERSAL_ROUTER_EXECUTE, router, &args).unwrap();

        assert_eq!(signal.amount_in, U256::from(75u64));
        assert_eq!(signal.amount_out_min, Some(U256::from(50u64)));
        assert_eq!(signal.path, vec![token_in, token_out]);
        match signal.kind {
            SwapKind::V3 { fee_tier, hops, .. } => {
                assert_eq!(fee_tier, 500);
                assert_eq!(hops, 1);
            }
            _ => panic!("expected v3 signal"),
        }
    }

    #[test]
    fn universal_router_decodes_swap_inside_subplan() {
        let router = Address::from_low_u64_be(10);
        let token_in = Address::from_low_u64_be(1);
        let token_out = Address::from_low_u64_be(2);
        let swap_input = encode(&[
            Token::Address(Address::from_low_u64_be(99)),
            Token::Uint(U256::from(25u64)),
            Token::Uint(U256::from(20u64)),
            Token::Array(vec![Token::Address(token_in), Token::Address(token_out)]),
            Token::Bool(true),
        ]);
        let subplan = encode(&[
            Token::Bytes(vec![0x08]),
            Token::Array(vec![Token::Bytes(swap_input)]),
        ]);
        let args = encode(&[
            Token::Bytes(vec![0x21]),
            Token::Array(vec![Token::Bytes(subplan)]),
        ]);

        let signal = decode_universal_router_swap(UNIVERSAL_ROUTER_EXECUTE, router, &args).unwrap();

        assert_eq!(signal.amount_in, U256::from(25u64));
        assert_eq!(signal.amount_out_min, Some(U256::from(20u64)));
        assert_eq!(signal.path, vec![token_in, token_out]);
        assert!(matches!(signal.kind, SwapKind::V2));
    }

    #[test]
    fn universal_router_prefers_monitored_swap_candidate() {
        let router = Address::from_low_u64_be(10);
        let unmonitored_in = Address::from_low_u64_be(1);
        let unmonitored_out = Address::from_low_u64_be(2);
        let monitored_in = Address::from_low_u64_be(3);
        let monitored_out = Address::from_low_u64_be(4);
        let unit = U256::exp10(18);
        let unmonitored_input = encode(&[
            Token::Address(Address::from_low_u64_be(99)),
            Token::Uint(U256::from(100u64) * unit),
            Token::Uint(U256::from(90u64)),
            Token::Array(vec![
                Token::Address(unmonitored_in),
                Token::Address(unmonitored_out),
            ]),
            Token::Bool(true),
        ]);
        let monitored_input = encode(&[
            Token::Address(Address::from_low_u64_be(99)),
            Token::Uint(U256::from(2u64) * unit),
            Token::Uint(U256::from(1u64)),
            Token::Array(vec![
                Token::Address(monitored_in),
                Token::Address(monitored_out),
            ]),
            Token::Bool(true),
        ]);
        let args = encode(&[
            Token::Bytes(vec![0x08, 0x88]),
            Token::Array(vec![
                Token::Bytes(unmonitored_input),
                Token::Bytes(monitored_input),
            ]),
        ]);
        let monitored = vec![MonitoredTokenConfig {
            address: monitored_in,
            decimals: 18,
            price_eth: 1.0,
        }];

        let signal = decode_universal_router_swap_for_monitored(
            UNIVERSAL_ROUTER_EXECUTE,
            router,
            &args,
            &monitored,
        )
        .unwrap();

        assert_eq!(signal.amount_in, U256::from(2u64) * unit);
        assert_eq!(signal.path, vec![monitored_in, monitored_out]);
        assert_eq!(signal.notional_wei, U256::from(2u64) * unit);
    }

    #[test]
    fn universal_router_graph_extracts_hop_candidates() {
        let token_a = Address::from_low_u64_be(1);
        let token_b = Address::from_low_u64_be(2);
        let token_c = Address::from_low_u64_be(3);
        let v3_path = [
            encode_v3_path(token_a, 500, token_b).to_vec(),
            vec![0x00, 0x0b, 0xb8],
            token_c.as_bytes().to_vec(),
        ]
        .concat();
        let swap_input = encode(&[
            Token::Address(Address::from_low_u64_be(99)),
            Token::Uint(U256::from(25u64)),
            Token::Uint(U256::from(20u64)),
            Token::Bytes(v3_path),
            Token::Bool(true),
        ]);
        let args = encode(&[
            Token::Bytes(vec![0x00, 0x10]),
            Token::Array(vec![Token::Bytes(swap_input), Token::Bytes(vec![0u8; 32])]),
        ]);

        let graph = universal_router_route_graph(&args).unwrap();

        assert_eq!(graph.swap_command_count, 1);
        assert_eq!(graph.hops.len(), 2);
        assert_eq!(graph.hops[0].token_in, token_a);
        assert_eq!(graph.hops[0].token_out, token_b);
        assert_eq!(graph.hops[0].fee_tier, Some(500));
        assert_eq!(graph.hops[1].token_in, token_b);
        assert_eq!(graph.hops[1].token_out, token_c);
        assert_eq!(graph.hops[1].fee_tier, Some(3000));
        assert!(graph
            .unsupported_commands
            .iter()
            .any(|cmd| cmd == "v4_swap"));
    }
}

fn wei_to_gwei_f64(value: U256) -> f64 {
    value.to_string().parse::<f64>().unwrap_or(0.0) / 1e9
}

fn elapsed_us(started: Instant) -> u64 {
    started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
}

fn elapsed_ms_ceil(started: Instant) -> u128 {
    started.elapsed().as_micros().div_ceil(1_000).max(1)
}

fn push_stage_pair(
    pairs: &mut Vec<(&'static str, u64)>,
    stage: &'static str,
    duration_us: Option<u64>,
) {
    if let Some(duration_us) = duration_us {
        pairs.push((stage, duration_us));
    }
}

fn worker_count(max_workers: usize) -> usize {
    std::thread::available_parallelism()
        .map(|value| value.get().min(max_workers).max(1))
        .unwrap_or(2)
}

fn token_as_uint(token: &Token) -> Option<U256> {
    match token {
        Token::Uint(value) => Some(*value),
        _ => None,
    }
}

fn token_as_address(token: &Token) -> Option<Address> {
    match token {
        Token::Address(value) => Some(*value),
        _ => None,
    }
}

fn token_as_address_vec(token: &Token) -> Option<Vec<Address>> {
    match token {
        Token::Array(values) => values.iter().map(token_as_address).collect(),
        _ => None,
    }
}
