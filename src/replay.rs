use crate::config::Config;
use crate::mev::adaptive::{AdaptivePolicy, AdaptiveQuoteInput, ContextSignal, PreflightInput};
use crate::mev::cache::pool_cache::PoolCache;
use crate::mev::execution::payload_builder::ExecutionPayload;
use crate::mev::opportunity::wei_to_eth_f64;
use crate::mev::runtime::{
    build_payload, decode_relevant_swap, fast_preflight_gate, passes_ev_gate, passes_quality_gate,
};
use crate::storage::{ensure_exports_dir, Storage};
use chrono::{Timelike, Utc};
use ethers::contract::abigen;
use ethers::providers::{Http, Middleware, Provider};
use ethers::signers::{LocalWallet, Signer};
use ethers::types::transaction::eip2718::TypedTransaction;
use ethers::types::{
    Address, BlockId, BlockNumber, Bytes, Transaction, TransactionRequest, H256, U256, U64,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

abigen!(
    ReplayErc20BalanceView,
    r#"[
        function balanceOf(address owner) external view returns (uint256)
    ]"#,
);

pub async fn maybe_run_replay_harness(
    config: Arc<Config>,
    storage: Storage,
) -> Result<bool, Box<dyn std::error::Error>> {
    if !env_flag("RUN_REPLAY_HARNESS") {
        return Ok(false);
    }

    let Some(fork_url) = config.fork_rpc_url() else {
        return Err(format!("missing Tenderly fork URL for network {}", config.network).into());
    };
    let input_path = env::var("REPLAY_INPUT_PATH")
        .map_err(|_| "RUN_REPLAY_HARNESS=true requires REPLAY_INPUT_PATH")?;
    let replay_limit = env_usize("REPLAY_LIMIT", 500).max(1);
    let provider = Arc::new(Provider::<Http>::try_from(fork_url.clone())?);
    let pool_cache = PoolCache::new(config.mev.pool_state_cache_ttl_ms);
    let replay_block = provider.get_block_number().await?.as_u64();
    let remote_chain_id = provider.get_chainid().await?.as_u64();
    if remote_chain_id != config.chain_id {
        return Err(format!(
            "fork chainId mismatch: config={} remote={} url={}",
            config.chain_id, remote_chain_id, fork_url
        )
        .into());
    }

    let min_large_swap_wei = ethers::utils::parse_ether(config.mev.min_large_swap_eth.to_string())?;
    let min_profit_wei = ethers::utils::parse_ether(config.mev.min_net_profit_eth.to_string())?;
    let cases = load_replay_cases(&input_path, replay_limit)?;
    let historical_profiles = storage.outcome_profiles(3, 256)?;

    info!(
        "Replay harness started network={} fork={} cases={}",
        config.network,
        fork_url,
        cases.len()
    );

    let tuning_enabled = env_flag("REPLAY_AUTO_TUNE");
    let selected_run = if tuning_enabled {
        let tuning_grid = replay_tuning_grid();
        let mut candidates = Vec::with_capacity(tuning_grid.len());
        let mut best_run: Option<(ReplayTuningResult, ReplayRunOutput)> = None;
        for tuning in tuning_grid {
            let run = run_replay_cases(
                &config,
                provider.clone(),
                &pool_cache,
                replay_block,
                &cases,
                &historical_profiles,
                min_large_swap_wei,
                min_profit_wei,
                tuning,
            )
            .await?;
            let candidate = ReplayTuningResult::from_report(tuning, &run.report);
            if best_run
                .as_ref()
                .map(|(best, _)| candidate.objective_score > best.objective_score)
                .unwrap_or(true)
            {
                best_run = Some((candidate.clone(), run));
            }
            candidates.push(candidate);
        }
        let (best, run) = best_run.ok_or("replay auto-tune produced no candidates")?;
        let recommended = recommended_env(&best);
        print_tuning_summary(&best, &candidates);
        maybe_apply_recommended_env(&recommended)?;
        write_tuning_report(&config.network, &best, &candidates, &recommended)?;
        run
    } else {
        run_replay_cases(
            &config,
            provider.clone(),
            &pool_cache,
            replay_block,
            &cases,
            &historical_profiles,
            min_large_swap_wei,
            min_profit_wei,
            ReplayTuning::default(),
        )
        .await?
    };

    print_report(&config, &selected_run.report);
    print_linux_validation_hint();
    maybe_write_decisions(&selected_run.decision_rows)?;
    for path in storage.export_evidence_artifacts(512)? {
        println!("Replay evidence export: {}", path.display());
    }
    Ok(true)
}

#[derive(Debug, Clone, Deserialize)]
struct ReplayInputCase {
    tx_hash: Option<String>,
    to: String,
    input: String,
    value_wei: Option<String>,
    gas_price_wei: Option<String>,
    max_fee_per_gas_wei: Option<String>,
    lookup_latency_ms: Option<f64>,
    hour_utc: Option<u8>,
    known_outcome: Option<String>,
    realized_profit_eth: Option<f64>,
}

struct ReplayRunOutput {
    report: ReplayReport,
    decision_rows: Vec<ReplayDecisionRow>,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct ReplayTuning {
    threshold_multiplier: f64,
    priority_shift: f64,
    toxicity_shift: f64,
    gas_extra_bps: u64,
}

impl Default for ReplayTuning {
    fn default() -> Self {
        Self {
            threshold_multiplier: 1.0,
            priority_shift: 0.0,
            toxicity_shift: 0.0,
            gas_extra_bps: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct ReplayTuningResult {
    threshold_multiplier: f64,
    priority_shift: f64,
    toxicity_shift: f64,
    gas_extra_bps: u64,
    objective_score: f64,
    realized_profit_eth: f64,
    execute_candidates: u64,
    false_positive: u64,
    false_negative: u64,
    capture_ratio: f64,
}

impl ReplayTuningResult {
    fn from_report(tuning: ReplayTuning, report: &ReplayReport) -> Self {
        let capture_ratio = if report.expected_profit_wei.is_zero() {
            0.0
        } else {
            report.realized_profit_eth / wei_to_eth_f64(report.expected_profit_wei).max(1e-12)
        };
        let objective_score = report.realized_profit_eth
            - report.false_positive as f64 * 0.0025
            - report.false_negative as f64 * 0.0015
            - report.execute_candidates as f64 * 0.0002
            - tuning.gas_extra_bps as f64 * 0.000_001_5;
        Self {
            threshold_multiplier: tuning.threshold_multiplier,
            priority_shift: tuning.priority_shift,
            toxicity_shift: tuning.toxicity_shift,
            gas_extra_bps: tuning.gas_extra_bps,
            objective_score,
            realized_profit_eth: report.realized_profit_eth,
            execute_candidates: report.execute_candidates,
            false_positive: report.false_positive,
            false_negative: report.false_negative,
            capture_ratio,
        }
    }
}

#[derive(Serialize)]
struct ReplayTuningReport {
    generated_at_utc: String,
    network: String,
    build_profile: &'static str,
    os: &'static str,
    arch: &'static str,
    auto_apply_enabled: bool,
    auto_apply_env_path: Option<String>,
    recommended_env: BTreeMap<String, String>,
    best: ReplayTuningResult,
    candidates: Vec<ReplayTuningResult>,
}

#[derive(Default)]
struct ReplayReport {
    total: u64,
    decoded: u64,
    fast_preflight_pass: u64,
    adaptive_preflight_pass: u64,
    payload_built: u64,
    ev_gate_pass: u64,
    quality_gate_pass: u64,
    adaptive_quote_pass: u64,
    execute_candidates: u64,
    true_positive: u64,
    false_positive: u64,
    true_negative: u64,
    false_negative: u64,
    fork_execution_attempts: u64,
    fork_execution_success: u64,
    fork_execution_reverts: u64,
    fork_realized_profit_eth: f64,
    estimated_gas_avoided_wei: U256,
    expected_profit_wei: U256,
    realized_profit_eth: f64,
    reasons: BTreeMap<String, u64>,
    latency: ReplayLatencySummary,
    toxicity: BTreeMap<ToxicityReplayKey, ToxicityReplayStats>,
}

impl ReplayReport {
    fn bump(&mut self, key: &str) {
        *self.reasons.entry(key.to_string()).or_insert(0) += 1;
    }

    fn bump_reason(&mut self, stage: &str, reason: &str) {
        self.bump(&format!("{stage}:{reason}"));
    }

    fn observe_labeled_outcome(&mut self, executed: bool, case: &ReplayInputCase) {
        let profitable = case
            .known_outcome
            .as_deref()
            .map(is_positive_outcome)
            .unwrap_or(false)
            || case.realized_profit_eth.unwrap_or(0.0) > 0.0;
        match (executed, profitable) {
            (true, true) => self.true_positive += 1,
            (true, false) => self.false_positive += 1,
            (false, true) => self.false_negative += 1,
            (false, false) => self.true_negative += 1,
        }
    }

    fn observe_latency(&mut self, trace: &ReplayLatencyTrace) {
        self.latency.observe(trace);
    }

    fn observe_reject_gas(&mut self, gas_price: U256, gas_limit: u64) {
        self.estimated_gas_avoided_wei = self
            .estimated_gas_avoided_wei
            .saturating_add(gas_price.saturating_mul(U256::from(gas_limit)));
    }

    fn observe_expected_profit(&mut self, expected_profit_wei: U256) {
        self.expected_profit_wei = self.expected_profit_wei.saturating_add(expected_profit_wei);
    }

    fn observe_realized(&mut self, case: &ReplayInputCase) {
        self.realized_profit_eth += case.realized_profit_eth.unwrap_or(0.0);
    }

    fn observe_toxicity_reject(
        &mut self,
        signal: &crate::mev::runtime::SwapSignal,
        hour_utc: u8,
        case: &ReplayInputCase,
        stage: &'static str,
    ) {
        let pair = *signal.path.last().unwrap_or(&signal.path[0]);
        let key = ToxicityReplayKey::new(signal.router, pair, hour_utc);
        self.toxicity
            .entry(key)
            .or_default()
            .observe(false, case, None, stage);
    }

    fn observe_toxicity_execute(
        &mut self,
        signal: &crate::mev::runtime::SwapSignal,
        pair: Address,
        hour_utc: u8,
        case: &ReplayInputCase,
        expected_profit_wei: U256,
    ) {
        let key = ToxicityReplayKey::new(signal.router, pair, hour_utc);
        self.toxicity.entry(key).or_default().observe(
            true,
            case,
            Some(expected_profit_wei),
            "execute",
        );
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ToxicityReplayKey {
    router: String,
    pair: String,
    hour_utc: u8,
}

impl ToxicityReplayKey {
    fn new(router: Address, pair: Address, hour_utc: u8) -> Self {
        Self {
            router: format!("{:?}", router),
            pair: format!("{:?}", pair),
            hour_utc,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct ToxicityReplayStats {
    samples: u64,
    rejected: u64,
    executed: u64,
    profitable_labels: u64,
    false_positive: u64,
    false_negative: u64,
    expected_profit_wei: U256,
    realized_profit_eth: f64,
    last_stage: &'static str,
}

impl ToxicityReplayStats {
    fn observe(
        &mut self,
        executed: bool,
        case: &ReplayInputCase,
        expected_profit_wei: Option<U256>,
        stage: &'static str,
    ) {
        self.samples += 1;
        self.last_stage = stage;
        let profitable = case
            .known_outcome
            .as_deref()
            .map(is_positive_outcome)
            .unwrap_or(false)
            || case.realized_profit_eth.unwrap_or(0.0) > 0.0;
        if profitable {
            self.profitable_labels += 1;
        }
        if executed {
            self.executed += 1;
            self.expected_profit_wei = self
                .expected_profit_wei
                .saturating_add(expected_profit_wei.unwrap_or_default());
            self.realized_profit_eth += case.realized_profit_eth.unwrap_or(0.0);
            if !profitable {
                self.false_positive += 1;
            }
        } else {
            self.rejected += 1;
            if profitable {
                self.false_negative += 1;
            }
        }
    }

    fn toxicity_score(&self) -> f64 {
        if self.samples == 0 {
            return 0.0;
        }
        let sample = self.samples as f64;
        let reject_rate = self.rejected as f64 / sample;
        let false_positive_rate = self.false_positive as f64 / sample;
        let false_negative_rate = self.false_negative as f64 / sample;
        let realized_capture = if self.expected_profit_wei > U256::zero() {
            (self.realized_profit_eth / wei_to_eth_f64(self.expected_profit_wei)).clamp(0.0, 1.25)
        } else {
            0.0
        };
        (reject_rate * 0.35
            + false_positive_rate * 0.30
            + false_negative_rate * 0.20
            + (1.0 - realized_capture).clamp(0.0, 1.0) * 0.15)
            .clamp(0.0, 1.0)
    }
}

#[derive(serde::Serialize)]
struct ReplayDecisionRow {
    tx_hash: String,
    stage: String,
    decision: String,
    reason: String,
    expected_profit_eth: f64,
    ev_real_usd: f64,
    threshold_usd: f64,
    total_latency_us: u64,
}

impl ReplayDecisionRow {
    fn reject(case: &ReplayInputCase, stage: &str, reason: &str) -> Self {
        Self {
            tx_hash: case
                .tx_hash
                .clone()
                .unwrap_or_else(|| "synthetic".to_string()),
            stage: stage.to_string(),
            decision: "reject".to_string(),
            reason: reason.to_string(),
            expected_profit_eth: 0.0,
            ev_real_usd: 0.0,
            threshold_usd: 0.0,
            total_latency_us: 0,
        }
    }

    fn execute(
        case: &ReplayInputCase,
        expected_profit_wei: U256,
        quote: &crate::mev::adaptive::AdaptiveQuote,
        total_latency_us: u64,
    ) -> Self {
        Self {
            tx_hash: case
                .tx_hash
                .clone()
                .unwrap_or_else(|| "synthetic".to_string()),
            stage: "adaptive".to_string(),
            decision: "execute".to_string(),
            reason: "passed".to_string(),
            expected_profit_eth: wei_to_eth_f64(expected_profit_wei),
            ev_real_usd: quote.ev_real_usd,
            threshold_usd: quote.threshold_dynamic_usd,
            total_latency_us,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ReplayLatencyTrace {
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

#[derive(Default)]
struct ReplayLatencySummary {
    per_stage_us: BTreeMap<&'static str, Vec<u64>>,
}

impl ReplayLatencySummary {
    fn observe(&mut self, trace: &ReplayLatencyTrace) {
        self.push("decode_swap", trace.decode_swap_us);
        self.push("context_signal", trace.context_signal_us);
        self.push("fast_preflight", trace.fast_preflight_us);
        self.push("adaptive_preflight", trace.adaptive_preflight_us);
        self.push("payload_build", trace.payload_build_us);
        self.push("ev_gate", trace.ev_gate_us);
        self.push("quality_gate", trace.quality_gate_us);
        self.push("adaptive_quote", trace.adaptive_quote_us);
        self.push("total_internal", trace.total_internal_us);
    }

    fn push(&mut self, stage: &'static str, duration_us: Option<u64>) {
        if let Some(duration_us) = duration_us {
            self.per_stage_us
                .entry(stage)
                .or_default()
                .push(duration_us);
        }
    }
}

async fn run_replay_cases(
    config: &Config,
    provider: Arc<Provider<Http>>,
    pool_cache: &PoolCache,
    replay_block: u64,
    cases: &[ReplayInputCase],
    historical_profiles: &[crate::mev::adaptive::HistoricalOutcomeProfile],
    min_large_swap_wei: U256,
    min_profit_wei: U256,
    tuning: ReplayTuning,
) -> Result<ReplayRunOutput, Box<dyn std::error::Error>> {
    let adaptive = AdaptivePolicy::shared(config);
    if let Ok(mut model) = adaptive.lock() {
        model.apply_historical_profiles(historical_profiles.to_vec());
    }
    let relay_paths = if config.builder_relays.is_empty() {
        vec![format!("rpc://{}", config.network)]
    } else {
        config.builder_relays.clone()
    };

    let mut report = ReplayReport::default();
    let mut decision_rows = Vec::new();
    for (index, case) in cases.iter().enumerate() {
        let case_started = Instant::now();
        let mut latency_trace = ReplayLatencyTrace::default();
        report.total += 1;
        let tx = replay_transaction(case, index as u64)?;
        let decode_started = Instant::now();
        let Some(signal) = decode_relevant_swap(
            &tx,
            &config.monitored_tokens,
            min_large_swap_wei,
            config.mev.opportunity_mode(),
        ) else {
            report.bump("decode_reject");
            latency_trace.total_internal_us = Some(elapsed_us(case_started));
            report.observe_latency(&latency_trace);
            continue;
        };
        report.decoded += 1;
        latency_trace.decode_swap_us = Some(elapsed_us(decode_started));
        let hour_utc = case.hour_utc.unwrap_or(Utc::now().hour() as u8);
        let context_started = Instant::now();
        let base_context_signal = if let Ok(model) = adaptive.lock() {
            model.context_signal(signal.router, hour_utc)
        } else {
            ContextSignal {
                priority_score: 0.50,
                toxicity_score: 0.50,
                samples: 0,
            }
        };
        let context_signal = tuned_context_signal(base_context_signal, tuning);
        latency_trace.context_signal_us = Some(elapsed_us(context_started));

        let gas_price = tx.max_fee_per_gas.or(tx.gas_price).unwrap_or_default();
        if gas_price.is_zero() {
            report.bump("missing_gas");
            latency_trace.total_internal_us = Some(elapsed_us(case_started));
            report.observe_latency(&latency_trace);
            continue;
        }

        let fast_started = Instant::now();
        let fast_gate = fast_preflight_gate(
            &signal,
            gas_price,
            min_large_swap_wei,
            config,
            context_signal,
        );
        latency_trace.fast_preflight_us = Some(elapsed_us(fast_started));
        if !fast_gate.should_continue {
            report.observe_labeled_outcome(false, case);
            report.observe_reject_gas(gas_price, config.estimated_exec_gas);
            report.bump_reason(
                "fast_preflight",
                fast_gate.reject_reason.unwrap_or("reject"),
            );
            report.observe_toxicity_reject(&signal, hour_utc, case, "fast_preflight");
            latency_trace.total_internal_us = Some(elapsed_us(case_started));
            report.observe_latency(&latency_trace);
            decision_rows.push(ReplayDecisionRow::reject(
                case,
                "fast_preflight",
                fast_gate.reject_reason.unwrap_or("reject"),
            ));
            continue;
        }
        report.fast_preflight_pass += 1;

        let cluster = crate::mev::adaptive::ClusterKey {
            router: signal.router,
            token_in: signal.path[0],
            token_out: *signal.path.last().unwrap_or(&signal.path[0]),
            selector: signal.selector,
        };
        let lookup_latency_ms = case.lookup_latency_ms.unwrap_or(75.0);
        let adaptive_preflight_started = Instant::now();
        let preflight = if let Ok(mut model) = adaptive.lock() {
            model.observe_lookup_latency(lookup_latency_ms);
            model.observe_candidate_flow(cluster, signal.notional_wei, gas_price);
            model.preflight_score(PreflightInput {
                cluster,
                notional_eth: wei_to_eth_f64(signal.notional_wei),
                gas_price_wei: gas_price,
                path_len: signal.path.len(),
            })
        } else {
            report.bump("adaptive_lock_error");
            latency_trace.total_internal_us = Some(elapsed_us(case_started));
            report.observe_latency(&latency_trace);
            continue;
        };
        latency_trace.adaptive_preflight_us = Some(elapsed_us(adaptive_preflight_started));
        if !preflight.should_continue {
            report.observe_labeled_outcome(false, case);
            report.observe_reject_gas(gas_price, config.estimated_exec_gas);
            report.bump_reason("preflight", preflight.reject_reason.unwrap_or("reject"));
            report.observe_toxicity_reject(&signal, hour_utc, case, "preflight");
            latency_trace.total_internal_us = Some(elapsed_us(case_started));
            report.observe_latency(&latency_trace);
            decision_rows.push(ReplayDecisionRow::reject(
                case,
                "preflight",
                preflight.reject_reason.unwrap_or("reject"),
            ));
            continue;
        }
        report.adaptive_preflight_pass += 1;

        let payload_started = Instant::now();
        let Ok(payload) = build_payload(
            provider.clone(),
            config,
            &signal,
            gas_price,
            context_signal,
            pool_cache,
            replay_block,
        )
        .await
        else {
            report.observe_labeled_outcome(false, case);
            report.observe_reject_gas(gas_price, config.estimated_exec_gas);
            report.bump("payload_build_reject");
            report.observe_toxicity_reject(&signal, hour_utc, case, "payload");
            latency_trace.payload_build_us = Some(elapsed_us(payload_started));
            latency_trace.total_internal_us = Some(elapsed_us(case_started));
            report.observe_latency(&latency_trace);
            decision_rows.push(ReplayDecisionRow::reject(
                case,
                "payload",
                "payload_build_reject",
            ));
            continue;
        };
        latency_trace.payload_build_us = Some(elapsed_us(payload_started));
        report.payload_built += 1;
        report.observe_expected_profit(payload.expected_profit_wei);

        let ev_gate_started = Instant::now();
        if !passes_ev_gate(
            config,
            &payload,
            &signal,
            std::time::Duration::from_millis(lookup_latency_ms as u64),
            min_profit_wei,
        ) {
            report.observe_labeled_outcome(false, case);
            report.observe_reject_gas(gas_price, payload.gas_limit);
            report.bump("ev_gate_reject");
            report.observe_toxicity_reject(&signal, hour_utc, case, "ev_gate");
            latency_trace.ev_gate_us = Some(elapsed_us(ev_gate_started));
            latency_trace.total_internal_us = Some(elapsed_us(case_started));
            report.observe_latency(&latency_trace);
            decision_rows.push(ReplayDecisionRow::reject(case, "ev_gate", "ev_gate_reject"));
            continue;
        }
        latency_trace.ev_gate_us = Some(elapsed_us(ev_gate_started));
        report.ev_gate_pass += 1;

        let gas_price_for_tuning = gas_price
            .saturating_mul(U256::from(10_000u64 + tuning.gas_extra_bps))
            / U256::from(10_000u64);
        let execution_cost_wei = gas_price_for_tuning
            .saturating_mul(U256::from(payload.gas_limit))
            .saturating_mul(U256::from(config.mev.gas_safety_margin_bps))
            / U256::from(10_000u64);
        let quality_gate_started = Instant::now();
        if !passes_quality_gate(config, &payload, execution_cost_wei) {
            report.observe_labeled_outcome(false, case);
            report.observe_reject_gas(gas_price, payload.gas_limit);
            report.bump("quality_gate_reject");
            report.observe_toxicity_reject(&signal, hour_utc, case, "quality_gate");
            latency_trace.quality_gate_us = Some(elapsed_us(quality_gate_started));
            latency_trace.total_internal_us = Some(elapsed_us(case_started));
            report.observe_latency(&latency_trace);
            decision_rows.push(ReplayDecisionRow::reject(
                case,
                "quality_gate",
                "quality_gate_reject",
            ));
            continue;
        }
        latency_trace.quality_gate_us = Some(elapsed_us(quality_gate_started));
        report.quality_gate_pass += 1;

        let adaptive_quote_started = Instant::now();
        let quote = if let Ok(mut model) = adaptive.lock() {
            model.quote_for_relays(
                AdaptiveQuoteInput {
                    cluster,
                    pair: payload.pair,
                    hour_utc,
                    context_priority_score: context_signal.priority_score,
                    context_toxicity_score: context_signal.toxicity_score,
                    expected_profit_wei: payload.expected_profit_wei,
                    execution_cost_wei,
                    gas_price_wei: gas_price_for_tuning,
                    lookup_latency_ms,
                    notional_eth: wei_to_eth_f64(signal.notional_wei),
                    price_impact_bps: payload.price_impact_bps,
                    relay_pressure_override: None,
                },
                &relay_paths,
            )
        } else {
            report.bump("adaptive_lock_error");
            latency_trace.total_internal_us = Some(elapsed_us(case_started));
            report.observe_latency(&latency_trace);
            continue;
        };
        latency_trace.adaptive_quote_us = Some(elapsed_us(adaptive_quote_started));

        if !should_execute_with_tuning(&quote, tuning) {
            report.observe_labeled_outcome(false, case);
            report.observe_reject_gas(gas_price, payload.gas_limit);
            report.bump_reason("adaptive", quote.reject_reason.unwrap_or("reject"));
            report.observe_toxicity_reject(&signal, hour_utc, case, "adaptive");
            latency_trace.total_internal_us = Some(elapsed_us(case_started));
            report.observe_latency(&latency_trace);
            decision_rows.push(ReplayDecisionRow::reject(
                case,
                "adaptive",
                quote.reject_reason.unwrap_or("reject"),
            ));
            continue;
        }

        report.execute_candidates += 1;
        report.adaptive_quote_pass += 1;
        if env_flag("REPLAY_EXECUTE_ON_FORK") {
            report.fork_execution_attempts += 1;
            match execute_payload_on_fork(config, provider.clone(), &payload, gas_price_for_tuning)
                .await
            {
                Ok(outcome) => {
                    if outcome.success {
                        report.fork_execution_success += 1;
                    } else {
                        report.fork_execution_reverts += 1;
                    }
                    report.fork_realized_profit_eth += outcome.realized_profit_eth;
                    println!(
                        "Tenderly fork execution tx={:?} success={} gas_used={} realized_profit_eth={:.12}",
                        outcome.tx_hash,
                        outcome.success,
                        outcome.gas_used,
                        outcome.realized_profit_eth
                    );
                    if !outcome.success && env_flag("REPLAY_FORK_HARD_FAIL") {
                        return Err(format!(
                            "Tenderly fork execution reverted tx={:?}",
                            outcome.tx_hash
                        )
                        .into());
                    }
                }
                Err(err) => {
                    report.fork_execution_reverts += 1;
                    if env_flag("REPLAY_FORK_HARD_FAIL") {
                        return Err(err);
                    }
                    println!("Tenderly fork execution failed: {err}");
                }
            }
        }
        report.observe_labeled_outcome(true, case);
        report.observe_realized(case);
        report.observe_toxicity_execute(
            &signal,
            payload.pair,
            hour_utc,
            case,
            payload.expected_profit_wei,
        );
        latency_trace.total_internal_us = Some(elapsed_us(case_started));
        report.observe_latency(&latency_trace);
        decision_rows.push(ReplayDecisionRow::execute(
            case,
            payload.expected_profit_wei,
            &quote,
            latency_trace.total_internal_us.unwrap_or_default(),
        ));
    }

    Ok(ReplayRunOutput {
        report,
        decision_rows,
    })
}

struct ForkExecutionOutcome {
    tx_hash: H256,
    success: bool,
    gas_used: u64,
    realized_profit_eth: f64,
}

async fn execute_payload_on_fork(
    config: &Config,
    provider: Arc<Provider<Http>>,
    payload: &ExecutionPayload,
    gas_price: U256,
) -> Result<ForkExecutionOutcome, Box<dyn std::error::Error>> {
    let wallet = config
        .executor_private_key
        .parse::<LocalWallet>()?
        .with_chain_id(config.chain_id);
    let from = wallet.address();
    let nonce = provider.get_transaction_count(from, None).await?;
    let raw_tx = sign_replay_executor_transaction(&wallet, payload, nonce, gas_price).await?;

    let pre_balance = token_balance_at(
        provider.clone(),
        payload.profit_token,
        payload.profit_recipient,
        BlockId::Number(BlockNumber::Latest),
    )
    .await?;
    let pending = provider.send_raw_transaction(raw_tx).await?;
    let tx_hash = pending.tx_hash();
    let receipt = pending
        .await?
        .ok_or_else(|| format!("fork transaction {:?} was not mined", tx_hash))?;
    let block_number = receipt
        .block_number
        .ok_or_else(|| format!("fork transaction {:?} missing block number", tx_hash))?;
    let post_balance = token_balance_at(
        provider.clone(),
        payload.profit_token,
        payload.profit_recipient,
        BlockId::Number(BlockNumber::Number(block_number)),
    )
    .await?;
    let gas_paid_wei = receipt
        .effective_gas_price
        .unwrap_or(gas_price)
        .saturating_mul(receipt.gas_used.unwrap_or_default());
    let token_meta = config
        .monitored_tokens
        .iter()
        .find(|token| token.address == payload.profit_token);
    let balance_delta = post_balance.saturating_sub(pre_balance);
    let gross_eth = if let Some(token_meta) = token_meta {
        let token_units = 10f64.powi(i32::from(token_meta.decimals));
        balance_delta.to_string().parse::<f64>().unwrap_or(0.0) / token_units * token_meta.price_eth
    } else {
        wei_to_eth_f64(balance_delta)
    };

    Ok(ForkExecutionOutcome {
        tx_hash,
        success: receipt.status == Some(U64::from(1u64)),
        gas_used: receipt.gas_used.unwrap_or_default().as_u64(),
        realized_profit_eth: gross_eth - wei_to_eth_f64(gas_paid_wei),
    })
}

async fn sign_replay_executor_transaction(
    wallet: &LocalWallet,
    payload: &ExecutionPayload,
    nonce: U256,
    gas_price: U256,
) -> Result<Bytes, Box<dyn std::error::Error>> {
    let tx: TypedTransaction = TransactionRequest::new()
        .to(payload.target_contract)
        .data(payload.calldata.clone())
        .value(payload.value)
        .gas(payload.gas_limit)
        .gas_price(gas_price)
        .nonce(nonce)
        .from(wallet.address())
        .into();
    let signature = wallet.sign_transaction(&tx).await?;
    Ok(tx.rlp_signed(&signature))
}

async fn token_balance_at(
    provider: Arc<Provider<Http>>,
    token: Address,
    owner: Address,
    block: BlockId,
) -> Result<U256, Box<dyn std::error::Error>> {
    Ok(ReplayErc20BalanceView::new(token, provider)
        .balance_of(owner)
        .block(block)
        .call()
        .await?)
}

fn load_replay_cases(
    path: &str,
    limit: usize,
) -> Result<Vec<ReplayInputCase>, Box<dyn std::error::Error>> {
    let raw = fs::read_to_string(path)?;
    if raw.trim_start().starts_with('[') {
        let mut items: Vec<ReplayInputCase> = serde_json::from_str(&raw)?;
        items.truncate(limit);
        return Ok(items);
    }

    let mut items = Vec::new();
    for line in raw
        .lines()
        .filter(|line| !line.trim().is_empty())
        .take(limit)
    {
        items.push(serde_json::from_str::<ReplayInputCase>(line)?);
    }
    Ok(items)
}

fn replay_tuning_grid() -> Vec<ReplayTuning> {
    let threshold_multipliers =
        env_f64_grid("REPLAY_TUNE_THRESHOLD_MULTIPLIERS", &[0.90, 1.00, 1.10]);
    let priority_shifts = env_f64_grid("REPLAY_TUNE_PRIORITY_SHIFTS", &[-0.05, 0.0, 0.05]);
    let toxicity_shifts = env_f64_grid("REPLAY_TUNE_TOXICITY_SHIFTS", &[-0.05, 0.0, 0.05]);
    let gas_extra_bps = env_u64_grid("REPLAY_TUNE_GAS_EXTRA_BPS", &[0, 500, 1000, 2000]);
    let mut grid = Vec::new();
    for threshold_multiplier in threshold_multipliers {
        for priority_shift in &priority_shifts {
            for toxicity_shift in &toxicity_shifts {
                for gas_extra_bps in &gas_extra_bps {
                    grid.push(ReplayTuning {
                        threshold_multiplier,
                        priority_shift: *priority_shift,
                        toxicity_shift: *toxicity_shift,
                        gas_extra_bps: *gas_extra_bps,
                    });
                }
            }
        }
    }
    if grid.is_empty() {
        grid.push(ReplayTuning::default());
    }
    grid
}

fn tuned_context_signal(base: ContextSignal, tuning: ReplayTuning) -> ContextSignal {
    ContextSignal {
        priority_score: (base.priority_score + tuning.priority_shift).clamp(0.0, 1.0),
        toxicity_score: (base.toxicity_score + tuning.toxicity_shift).clamp(0.0, 1.0),
        samples: base.samples,
    }
}

fn should_execute_with_tuning(
    quote: &crate::mev::adaptive::AdaptiveQuote,
    tuning: ReplayTuning,
) -> bool {
    let adjusted_threshold = quote.threshold_dynamic_usd * tuning.threshold_multiplier.max(0.01);
    let threshold_pass = quote.ev_real_usd > adjusted_threshold;
    if quote.should_execute {
        return threshold_pass;
    }
    matches!(quote.reject_reason, Some("ev_real_below_threshold")) && threshold_pass
}

fn env_f64_grid(name: &str, defaults: &[f64]) -> Vec<f64> {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|item| item.trim().parse::<f64>().ok())
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| defaults.to_vec())
}

fn env_u64_grid(name: &str, defaults: &[u64]) -> Vec<u64> {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|item| item.trim().parse::<u64>().ok())
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| defaults.to_vec())
}

fn print_tuning_summary(best: &ReplayTuningResult, candidates: &[ReplayTuningResult]) {
    println!("=== Replay Auto-Tune ===");
    println!("Candidates: {}", candidates.len());
    println!(
        "Best tune -> threshold_multiplier={:.3} priority_shift={:.3} toxicity_shift={:.3} gas_extra_bps={} objective={:.6} realized={:.6}ETH capture={:.2}%",
        best.threshold_multiplier,
        best.priority_shift,
        best.toxicity_shift,
        best.gas_extra_bps,
        best.objective_score,
        best.realized_profit_eth,
        best.capture_ratio * 100.0
    );
}

fn write_tuning_report(
    network: &str,
    best: &ReplayTuningResult,
    candidates: &[ReplayTuningResult],
    recommended_env: &BTreeMap<String, String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let auto_apply_path = auto_apply_env_path();
    let path = env::var("REPLAY_TUNE_OUTPUT_PATH")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            ensure_exports_dir()
                .unwrap_or_else(|_| std::path::PathBuf::from("exports"))
                .join("replay_auto_tune.json")
        });
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let report = ReplayTuningReport {
        generated_at_utc: Utc::now().to_rfc3339(),
        network: network.to_string(),
        build_profile: if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        auto_apply_enabled: env_flag("REPLAY_AUTO_TUNE_APPLY"),
        auto_apply_env_path: auto_apply_path
            .as_ref()
            .map(|path| path.display().to_string()),
        recommended_env: recommended_env.clone(),
        best: best.clone(),
        candidates: candidates.to_vec(),
    };
    let json = serde_json::to_string_pretty(&report)?;
    fs::write(&path, &json)?;
    if let Some(parent) = path.parent() {
        let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ");
        let versioned = parent.join(format!(
            "replay_auto_tune.{}.{}.{}.json",
            network,
            std::env::consts::OS,
            timestamp
        ));
        fs::write(&versioned, json)?;
        if let Some(reference) = crate::storage::maybe_freeze_reference_artifact(&versioned)? {
            println!("Replay auto-tune reference freeze: {}", reference.display());
        }
    }
    Ok(())
}

fn recommended_env(best: &ReplayTuningResult) -> BTreeMap<String, String> {
    let mut envs = BTreeMap::new();
    envs.insert(
        "REPLAY_TUNE_THRESHOLD_MULTIPLIERS".to_string(),
        format!("{:.2}", best.threshold_multiplier),
    );
    envs.insert(
        "REPLAY_TUNE_PRIORITY_SHIFTS".to_string(),
        format!("{:.2}", best.priority_shift),
    );
    envs.insert(
        "REPLAY_TUNE_TOXICITY_SHIFTS".to_string(),
        format!("{:.2}", best.toxicity_shift),
    );
    envs.insert(
        "REPLAY_TUNE_GAS_EXTRA_BPS".to_string(),
        best.gas_extra_bps.to_string(),
    );
    envs
}

fn maybe_apply_recommended_env(
    recommended_env: &BTreeMap<String, String>,
) -> Result<Option<std::path::PathBuf>, Box<dyn std::error::Error>> {
    if !env_flag("REPLAY_AUTO_TUNE_APPLY") {
        return Ok(None);
    }
    let path = auto_apply_env_path().unwrap_or_else(|| {
        ensure_exports_dir()
            .unwrap_or_else(|_| std::path::PathBuf::from("exports"))
            .join("replay_auto_tune.env")
    });
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let mut out = String::new();
    for (key, value) in recommended_env {
        out.push_str(key);
        out.push('=');
        out.push_str(value);
        out.push('\n');
    }
    fs::write(&path, out)?;
    println!("Replay auto-tune env applied: {}", path.display());
    if let Some(reference) = crate::storage::maybe_freeze_reference_artifact(&path)? {
        println!(
            "Replay auto-tune env reference freeze: {}",
            reference.display()
        );
    }
    Ok(Some(path))
}

fn auto_apply_env_path() -> Option<std::path::PathBuf> {
    env::var("REPLAY_AUTO_TUNE_APPLY_PATH")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from)
}

fn print_linux_validation_hint() {
    if cfg!(target_os = "linux") && !cfg!(debug_assertions) {
        println!("Replay validation profile: linux release");
        return;
    }
    println!(
        "Replay validation note: final WAR validation must run on pinned Linux cloud in --release (current os={} profile={}).",
        std::env::consts::OS,
        if cfg!(debug_assertions) { "debug" } else { "release" }
    );
}

fn replay_transaction(
    case: &ReplayInputCase,
    fallback_nonce: u64,
) -> Result<Transaction, Box<dyn std::error::Error>> {
    let mut tx = Transaction::default();
    tx.hash = case
        .tx_hash
        .as_deref()
        .map(H256::from_str)
        .transpose()?
        .unwrap_or_else(|| H256::from_low_u64_be(fallback_nonce));
    tx.to = Some(case.to.parse::<Address>()?);
    tx.input = Bytes::from(hex::decode(case.input.trim_start_matches("0x"))?);
    tx.value = parse_u256(case.value_wei.as_deref()).unwrap_or_default();
    tx.gas_price = parse_u256(case.gas_price_wei.as_deref());
    tx.max_fee_per_gas = parse_u256(case.max_fee_per_gas_wei.as_deref());
    tx.nonce = U256::from(fallback_nonce);
    tx.gas = U256::from(300_000u64);
    tx.transaction_index = Some(U64::zero());
    Ok(tx)
}

fn parse_u256(value: Option<&str>) -> Option<U256> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    if let Some(hex) = value.strip_prefix("0x") {
        U256::from_str_radix(hex, 16).ok()
    } else {
        U256::from_dec_str(value).ok()
    }
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .unwrap_or_default()
        .trim()
        .eq_ignore_ascii_case("true")
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(default)
}

fn maybe_write_decisions(rows: &[ReplayDecisionRow]) -> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = env::var("REPLAY_OUTPUT_PATH")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let mut out = String::new();
    for row in rows {
        out.push_str(&serde_json::to_string(row)?);
        out.push('\n');
    }
    fs::write(path, out)?;
    Ok(())
}

fn is_positive_outcome(outcome: &str) -> bool {
    matches!(outcome, "included_success" | "execute" | "profitable")
}

fn print_report(config: &Config, report: &ReplayReport) {
    println!("=== Replay Harness ===");
    println!("Network: {}", config.network);
    println!("Total cases: {}", report.total);
    println!("Decode rate: {:.2}%", percent(report.decoded, report.total));
    println!(
        "Fast preflight pass rate: {:.2}%",
        percent(report.fast_preflight_pass, report.decoded)
    );
    println!(
        "Adaptive preflight pass rate: {:.2}%",
        percent(report.adaptive_preflight_pass, report.fast_preflight_pass)
    );
    println!("Payload built: {}", report.payload_built);
    println!(
        "Payload build rate: {:.2}%",
        percent(report.payload_built, report.adaptive_preflight_pass)
    );
    println!(
        "EV gate pass rate: {:.2}%",
        percent(report.ev_gate_pass, report.payload_built)
    );
    println!(
        "Quality gate pass rate: {:.2}%",
        percent(report.quality_gate_pass, report.ev_gate_pass)
    );
    println!(
        "Adaptive quote pass rate: {:.2}%",
        percent(report.adaptive_quote_pass, report.quality_gate_pass)
    );
    println!("Execute candidates: {}", report.execute_candidates);
    if report.fork_execution_attempts > 0 {
        println!("Tenderly fork executions:");
        println!("  attempts -> {}", report.fork_execution_attempts);
        println!("  success -> {}", report.fork_execution_success);
        println!("  reverts -> {}", report.fork_execution_reverts);
        println!(
            "  realized_profit_eth -> {:.12}",
            report.fork_realized_profit_eth
        );
    }
    println!(
        "Acceptance rate: {:.2}%",
        if report.total == 0 {
            0.0
        } else {
            report.execute_candidates as f64 * 100.0 / report.total as f64
        }
    );
    let labeled_total =
        report.true_positive + report.false_positive + report.true_negative + report.false_negative;
    if labeled_total > 0 {
        println!("Labeled outcomes:");
        println!("  true_positive -> {}", report.true_positive);
        println!("  false_positive -> {}", report.false_positive);
        println!("  true_negative -> {}", report.true_negative);
        println!("  false_negative -> {}", report.false_negative);
        println!(
            "  false_positive_rate -> {:.2}%",
            percent(
                report.false_positive,
                report.false_positive + report.true_positive
            )
        );
        println!(
            "  false_negative_rate -> {:.2}%",
            percent(
                report.false_negative,
                report.false_negative + report.true_negative
            )
        );
    }
    println!(
        "Expected profit scanned after payload: {:.8} ETH",
        wei_to_eth_f64(report.expected_profit_wei)
    );
    println!(
        "Realized profit labels: {:.8} ETH",
        report.realized_profit_eth
    );
    println!(
        "Realized/expected capture: {:.2}%",
        if report.expected_profit_wei.is_zero() {
            0.0
        } else {
            report.realized_profit_eth * 100.0
                / wei_to_eth_f64(report.expected_profit_wei).max(1e-12)
        }
    );
    println!(
        "Estimated gas wasted avoided: {:.8} ETH",
        wei_to_eth_f64(report.estimated_gas_avoided_wei)
    );
    println!("Reject breakdown:");
    for (reason, count) in &report.reasons {
        println!("  {} -> {}", reason, count);
    }
    if report.reasons.is_empty() {
        println!("  no rejects recorded");
    }
    println!("Latency summary (microseconds):");
    if report.latency.per_stage_us.is_empty() {
        println!("  no latency samples recorded");
    } else {
        for (stage, samples) in &report.latency.per_stage_us {
            let snapshot = latency_snapshot(samples);
            println!(
                "  {} -> samples={} avg_us={} p50_us={} p95_us={} max_us={} avg_ms={:.3}",
                stage,
                snapshot.samples,
                snapshot.avg_us,
                snapshot.p50_us,
                snapshot.p95_us,
                snapshot.max_us,
                snapshot.avg_us as f64 / 1_000.0
            );
        }
    }
    println!("Context toxicity by router + pair + hour:");
    if report.toxicity.is_empty() {
        println!("  no contextual samples recorded");
    } else {
        let mut rows = report.toxicity.iter().collect::<Vec<_>>();
        rows.sort_by(|(_, left), (_, right)| {
            right.toxicity_score().total_cmp(&left.toxicity_score())
        });
        for (key, stats) in rows.into_iter().take(12) {
            println!(
                "  hour={} router={} pair={} samples={} rejected={} executed={} profitable_labels={} fp={} fn={} toxicity={:.2} last_stage={}",
                key.hour_utc,
                key.router,
                key.pair,
                stats.samples,
                stats.rejected,
                stats.executed,
                stats.profitable_labels,
                stats.false_positive,
                stats.false_negative,
                stats.toxicity_score(),
                stats.last_stage,
            );
        }
    }
}

fn percent(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 * 100.0 / denominator as f64
    }
}

struct LatencySnapshot {
    samples: usize,
    avg_us: u64,
    p50_us: u64,
    p95_us: u64,
    max_us: u64,
}

fn latency_snapshot(samples: &[u64]) -> LatencySnapshot {
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    let count = ordered.len().max(1);
    let sum: u128 = ordered.iter().map(|value| u128::from(*value)).sum();
    let avg_us = (sum / count as u128) as u64;
    let p50_us = percentile_us(&ordered, 0.50);
    let p95_us = percentile_us(&ordered, 0.95);
    let max_us = *ordered.last().unwrap_or(&0);
    LatencySnapshot {
        samples: samples.len(),
        avg_us,
        p50_us,
        p95_us,
        max_us,
    }
}

fn percentile_us(sorted: &[u64], percentile: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn elapsed_us(started: Instant) -> u64 {
    started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
}
