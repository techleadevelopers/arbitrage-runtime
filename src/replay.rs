use crate::config::Config;
use crate::mev::adaptive::{AdaptivePolicy, AdaptiveQuoteInput, ContextSignal, PreflightInput};
use crate::mev::opportunity::wei_to_eth_f64;
use crate::mev::runtime::{
    build_payload, decode_relevant_swap, fast_preflight_gate, passes_ev_gate,
    passes_quality_gate,
};
use crate::storage::Storage;
use chrono::{Timelike, Utc};
use ethers::providers::{Http, Middleware, Provider};
use ethers::types::{Address, Bytes, Transaction, H256, U256, U64};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::str::FromStr;
use std::sync::Arc;
use tracing::info;

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
    let remote_chain_id = provider.get_chainid().await?.as_u64();
    if remote_chain_id != config.chain_id {
        return Err(format!(
            "fork chainId mismatch: config={} remote={} url={}",
            config.chain_id, remote_chain_id, fork_url
        )
        .into());
    }

    let min_large_swap_wei =
        ethers::utils::parse_ether(config.mev.min_large_swap_eth.to_string())?;
    let min_profit_wei = ethers::utils::parse_ether(config.mev.min_net_profit_eth.to_string())?;
    let cases = load_replay_cases(&input_path, replay_limit)?;
    let adaptive = AdaptivePolicy::shared(&config);
    if let Ok(mut model) = adaptive.lock() {
        model.apply_historical_profiles(storage.outcome_profiles(3, 256)?);
    }

    info!(
        "Replay harness started network={} fork={} cases={}",
        config.network,
        fork_url,
        cases.len()
    );

    let mut report = ReplayReport::default();
    let mut decision_rows = Vec::new();
    let relay_paths = if config.builder_relays.is_empty() {
        vec![format!("rpc://{}", config.network)]
    } else {
        config.builder_relays.clone()
    };

    for (index, case) in cases.into_iter().enumerate() {
        report.total += 1;
        let tx = replay_transaction(&case, index as u64)?;
        let Some(signal) =
            decode_relevant_swap(&tx, &config.monitored_tokens, min_large_swap_wei)
        else {
            report.bump("decode_reject");
            continue;
        };
        let hour_utc = case.hour_utc.unwrap_or(Utc::now().hour() as u8);
        let context_signal = if let Ok(model) = adaptive.lock() {
            model.context_signal(signal.router, hour_utc)
        } else {
            ContextSignal {
                priority_score: 0.50,
                toxicity_score: 0.50,
                samples: 0,
            }
        };

        let gas_price = tx.max_fee_per_gas.or(tx.gas_price).unwrap_or_default();
        if gas_price.is_zero() {
            report.bump("missing_gas");
            continue;
        }

        let fast_gate = fast_preflight_gate(
            &signal,
            gas_price,
            min_large_swap_wei,
            &config,
            context_signal,
        );
        if !fast_gate.should_continue {
            report.observe_labeled_outcome(false, &case);
            report.bump_reason("fast_preflight", fast_gate.reject_reason.unwrap_or("reject"));
            decision_rows.push(ReplayDecisionRow::reject(
                &case,
                "fast_preflight",
                fast_gate.reject_reason.unwrap_or("reject"),
            ));
            continue;
        }

        let cluster = crate::mev::adaptive::ClusterKey {
            router: signal.router,
            token_in: signal.path[0],
            token_out: *signal.path.last().unwrap_or(&signal.path[0]),
            selector: signal.selector,
        };
        let lookup_latency_ms = case.lookup_latency_ms.unwrap_or(75.0);
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
            continue;
        };
        if !preflight.should_continue {
            report.observe_labeled_outcome(false, &case);
            report.bump_reason("preflight", preflight.reject_reason.unwrap_or("reject"));
            decision_rows.push(ReplayDecisionRow::reject(
                &case,
                "preflight",
                preflight.reject_reason.unwrap_or("reject"),
            ));
            continue;
        }

        let Some(payload) = build_payload(
            provider.clone(),
            &config,
            &signal,
            gas_price,
            context_signal,
        )
        .await
        else {
            report.observe_labeled_outcome(false, &case);
            report.bump("payload_build_reject");
            decision_rows.push(ReplayDecisionRow::reject(&case, "payload", "payload_build_reject"));
            continue;
        };
        report.payload_built += 1;

        if !passes_ev_gate(
            &config,
            &payload,
            &signal,
            std::time::Duration::from_millis(lookup_latency_ms as u64),
            min_profit_wei,
        ) {
            report.observe_labeled_outcome(false, &case);
            report.bump("ev_gate_reject");
            decision_rows.push(ReplayDecisionRow::reject(&case, "ev_gate", "ev_gate_reject"));
            continue;
        }

        let execution_cost_wei = gas_price
            .saturating_mul(U256::from(payload.gas_limit))
            .saturating_mul(U256::from(config.mev.gas_safety_margin_bps))
            / U256::from(10_000u64);
        if !passes_quality_gate(&config, &payload, execution_cost_wei) {
            report.observe_labeled_outcome(false, &case);
            report.bump("quality_gate_reject");
            decision_rows.push(ReplayDecisionRow::reject(
                &case,
                "quality_gate",
                "quality_gate_reject",
            ));
            continue;
        }

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
                    gas_price_wei: gas_price,
                    lookup_latency_ms,
                    notional_eth: wei_to_eth_f64(signal.notional_wei),
                    price_impact_bps: payload.price_impact_bps,
                    relay_pressure_override: None,
                },
                &relay_paths,
            )
        } else {
            report.bump("adaptive_lock_error");
            continue;
        };

        if !quote.should_execute {
            report.observe_labeled_outcome(false, &case);
            report.bump_reason("adaptive", quote.reject_reason.unwrap_or("reject"));
            decision_rows.push(ReplayDecisionRow::reject(
                &case,
                "adaptive",
                quote.reject_reason.unwrap_or("reject"),
            ));
            continue;
        }

        report.execute_candidates += 1;
        report.observe_labeled_outcome(true, &case);
        decision_rows.push(ReplayDecisionRow::execute(&case, payload.expected_profit_wei, &quote));
    }

    print_report(&config, &report);
    maybe_write_decisions(&decision_rows)?;
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

#[derive(Default)]
struct ReplayReport {
    total: u64,
    payload_built: u64,
    execute_candidates: u64,
    true_positive: u64,
    false_positive: u64,
    true_negative: u64,
    false_negative: u64,
    reasons: BTreeMap<String, u64>,
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
}

impl ReplayDecisionRow {
    fn reject(case: &ReplayInputCase, stage: &str, reason: &str) -> Self {
        Self {
            tx_hash: case.tx_hash.clone().unwrap_or_else(|| "synthetic".to_string()),
            stage: stage.to_string(),
            decision: "reject".to_string(),
            reason: reason.to_string(),
            expected_profit_eth: 0.0,
            ev_real_usd: 0.0,
            threshold_usd: 0.0,
        }
    }

    fn execute(
        case: &ReplayInputCase,
        expected_profit_wei: U256,
        quote: &crate::mev::adaptive::AdaptiveQuote,
    ) -> Self {
        Self {
            tx_hash: case.tx_hash.clone().unwrap_or_else(|| "synthetic".to_string()),
            stage: "adaptive".to_string(),
            decision: "execute".to_string(),
            reason: "passed".to_string(),
            expected_profit_eth: wei_to_eth_f64(expected_profit_wei),
            ev_real_usd: quote.ev_real_usd,
            threshold_usd: quote.threshold_dynamic_usd,
        }
    }
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
    for line in raw.lines().filter(|line| !line.trim().is_empty()).take(limit) {
        items.push(serde_json::from_str::<ReplayInputCase>(line)?);
    }
    Ok(items)
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
        .filter(|value| !value.is_empty()) else {
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
    println!("Payload built: {}", report.payload_built);
    println!("Execute candidates: {}", report.execute_candidates);
    println!(
        "Acceptance rate: {:.2}%",
        if report.total == 0 {
            0.0
        } else {
            report.execute_candidates as f64 * 100.0 / report.total as f64
        }
    );
    let labeled_total = report.true_positive + report.false_positive + report.true_negative + report.false_negative;
    if labeled_total > 0 {
        println!("Labeled outcomes:");
        println!("  true_positive -> {}", report.true_positive);
        println!("  false_positive -> {}", report.false_positive);
        println!("  true_negative -> {}", report.true_negative);
        println!("  false_negative -> {}", report.false_negative);
    }
    println!("Reject breakdown:");
    for (reason, count) in &report.reasons {
        println!("  {} -> {}", reason, count);
    }
    if report.reasons.is_empty() {
        println!("  no rejects recorded");
    }
}
