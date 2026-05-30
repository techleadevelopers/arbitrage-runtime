use crate::config::{parse_opportunity_mode, Config, OpportunityMode, OpportunityThresholds};
use crate::mev::execution::payload_builder::EdgeMetadata;
use crate::rpc::{RpcEndpointSnapshot, RpcFleet};
use crate::storage::{Storage, UnsupportedSelectorRecord};
use axum::extract::{Path, Query, State};
use axum::http::header::{CONTENT_DISPOSITION, CONTENT_TYPE};
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tower_http::cors::CorsLayer;

#[derive(Clone)]
pub struct DashboardHandle {
    inner: Arc<RwLock<DashboardState>>,
    storage: Storage,
    rpc_fleet: Arc<RpcFleet>,
    opportunity_mode: Arc<RwLock<OpportunityMode>>,
    runtime_thresholds: Arc<RwLock<OpportunityThresholds>>,
    runtime_paused: Arc<AtomicBool>,
    pending: Arc<Mutex<PendingStorageWrites>>,
    observer: Arc<Mutex<ScavengerObserver>>,
    last_storage_prune: Arc<Mutex<Instant>>,
    file_telemetry: Arc<FileTelemetry>,
}

#[derive(Default)]
struct PendingStorageWrites {
    events: Vec<(String, String)>,
    latencies: Vec<PendingLatency>,
    latency_rollups: HashMap<String, PendingLatencyRollup>,
    funnel_rollups: HashMap<String, PendingCounterRollup>,
    reject_reason_rollups: HashMap<String, PendingRejectReasonRollup>,
    relay_updates: Vec<PendingRelayUpdate>,
    treasury_updates: Vec<PendingTreasuryUpdate>,
    execution_outcomes: Vec<PendingExecutionOutcome>,
    unsupported_selectors: Vec<UnsupportedSelectorRecord>,
}

struct PendingLatency {
    stage: String,
    duration_ms: u128,
    wallet: Option<String>,
    note: Option<String>,
}

struct PendingLatencyRollup {
    bucket: String,
    stage: String,
    samples: u64,
    total_ms: u128,
    max_ms: u128,
    last_ms: u128,
}

struct PendingCounterRollup {
    bucket: String,
    stage: String,
    count: u64,
}

struct PendingRejectReasonRollup {
    bucket: String,
    stage: String,
    reason: String,
    count: u64,
}

struct PendingRelayUpdate {
    relay: String,
    accepted: u64,
    submit_failed: u64,
    included_success: u64,
    included_revert: u64,
    not_included_timeout: u64,
    submit_latency_ms: Option<f64>,
    finalization_latency_ms: Option<f64>,
    score: Option<f64>,
    pressure: Option<f64>,
    accept_rate: Option<f64>,
    inclusion_rate: Option<f64>,
}

struct PendingTreasuryUpdate {
    executor_address: String,
    vault_address: String,
    profit_address: String,
    balance_eth: f64,
    min_buffer_eth: f64,
    target_buffer_eth: f64,
    max_buffer_eth: f64,
    action: String,
    recommended_amount_eth: f64,
    status: String,
    note: String,
}

struct PendingExecutionOutcome {
    relay: String,
    target_block: u64,
    pair: String,
    router: String,
    token_in: String,
    token_out: String,
    victim_tx: String,
    outcome: String,
    expected_profit_eth: f64,
    realized_profit_eth: f64,
    gas_used: u64,
    submit_latency_ms: f64,
    finalization_latency_ms: f64,
}

struct FileTelemetry {
    enabled: bool,
    path: PathBuf,
    max_bytes: u64,
    keep_files: usize,
    decode_reject_sample_rate: u64,
    state: Mutex<FileTelemetryState>,
}

#[derive(Default)]
struct FileTelemetryState {
    edge_samples_seen: u64,
    seen_decode_reject_keys: HashSet<String>,
}

impl FileTelemetry {
    fn from_env() -> Self {
        let enabled = env_bool("MEV_FILE_TELEMETRY_ENABLED", false);
        let dir = std::env::var("MEV_FILE_TELEMETRY_DIR").unwrap_or_else(|_| "logs".to_string());
        let max_mb = env_u64("MEV_FILE_TELEMETRY_MAX_MB", 25).max(1);
        let keep_files = env_u64("MEV_FILE_TELEMETRY_KEEP_FILES", 5).clamp(1, 100) as usize;
        let decode_reject_sample_rate = env_u64("MEV_DECODE_REJECT_SAMPLE_RATE", 20).max(1);
        Self {
            enabled,
            path: PathBuf::from(dir).join("edge-shadow.jsonl"),
            max_bytes: max_mb.saturating_mul(1024 * 1024),
            keep_files,
            decode_reject_sample_rate,
            state: Mutex::new(FileTelemetryState::default()),
        }
    }

    fn record_edge_sample(&self, sample: &EdgeSampleSnapshot) {
        if !self.enabled || !self.should_write_edge_sample(sample) {
            return;
        }
        let record = serde_json::json!({
            "at": Utc::now().to_rfc3339(),
            "kind": "edge_sample",
            "sample": sample,
        });
        self.write_json_line(&record);
    }

    fn record_event(&self, level: &str, message: &str) {
        if !self.enabled {
            return;
        }
        let record = serde_json::json!({
            "at": Utc::now().to_rfc3339(),
            "kind": "event",
            "level": level,
            "message": message,
        });
        self.write_json_line(&record);
    }

    fn record_latency(
        &self,
        stage: &str,
        duration_ms: u128,
        wallet: Option<&str>,
        note: Option<&str>,
    ) {
        if !self.enabled || !file_latency_enabled() {
            return;
        }
        let record = serde_json::json!({
            "at": Utc::now().to_rfc3339(),
            "kind": "latency",
            "stage": stage,
            "duration_ms": duration_ms,
            "wallet": wallet,
            "note": note,
        });
        self.write_json_line(&record);
    }

    fn should_write_edge_sample(&self, sample: &EdgeSampleSnapshot) -> bool {
        if sample.status != "decode_reject" {
            return true;
        }
        let key = format!("{}|{}", sample.selector, sample.reason);
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        state.edge_samples_seen = state.edge_samples_seen.saturating_add(1);
        if state.seen_decode_reject_keys.insert(key) {
            return true;
        }
        state.edge_samples_seen % self.decode_reject_sample_rate == 0
    }

    fn write_json_line(&self, record: &serde_json::Value) {
        if let Some(parent) = self.path.parent() {
            if fs::create_dir_all(parent).is_err() {
                return;
            }
        }
        self.rotate_if_needed();
        let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        else {
            return;
        };
        if serde_json::to_writer(&mut file, record).is_ok() {
            let _ = file.write_all(b"\n");
        }
    }

    fn rotate_if_needed(&self) {
        let Ok(metadata) = fs::metadata(&self.path) else {
            return;
        };
        if metadata.len() < self.max_bytes {
            return;
        }
        for idx in (1..=self.keep_files).rev() {
            let from = rotated_path(&self.path, idx);
            let to = rotated_path(&self.path, idx + 1);
            if from.exists() {
                if idx == self.keep_files {
                    let _ = fs::remove_file(&from);
                } else {
                    let _ = fs::rename(&from, &to);
                }
            }
        }
        let _ = fs::rename(&self.path, rotated_path(&self.path, 1));
    }
}

fn rotated_path(path: &std::path::Path, idx: usize) -> PathBuf {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("jsonl");
    path.with_extension(format!("{idx}.{extension}"))
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

fn storage_events_enabled() -> bool {
    env_bool("STORAGE_EVENTS_ENABLED", false)
}

fn storage_telemetry_enabled() -> bool {
    env_bool("STORAGE_TELEMETRY_ENABLED", false)
}

fn storage_rollups_enabled() -> bool {
    env_bool("STORAGE_ROLLUPS_ENABLED", true)
}

fn file_latency_enabled() -> bool {
    env_bool("MEV_FILE_LATENCY_TELEMETRY_ENABLED", false)
}

fn telemetry_bucket_minute() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:00Z").to_string()
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardEvent {
    pub at: String,
    pub level: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalletSnapshot {
    pub role: String,
    pub address: String,
    pub balance_eth: String,
    pub rpc: String,
    pub status: String,
    pub note: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalletResidualSnapshot {
    pub wallet: String,
    pub asset_class: String,
    pub detections: u64,
    pub successful_sweeps: u64,
    pub detected_profit_eth: String,
    pub realized_profit_eth: String,
    pub residual_score: u64,
    pub last_seen_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RejectReasonSnapshot {
    pub stage: String,
    pub reason: String,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RelaySnapshot {
    pub relay: String,
    pub score: f64,
    pub pressure: f64,
    pub accept_rate: f64,
    pub inclusion_rate: f64,
    pub accepted: u64,
    pub submit_failed: u64,
    pub included_success: u64,
    pub included_revert: u64,
    pub not_included_timeout: u64,
    pub submit_latency_ms: f64,
    pub finalization_latency_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TreasurySnapshot {
    pub at: String,
    pub executor_address: String,
    pub vault_address: String,
    pub profit_address: String,
    pub balance_eth: f64,
    pub min_buffer_eth: f64,
    pub target_buffer_eth: f64,
    pub max_buffer_eth: f64,
    pub action: String,
    pub recommended_amount_eth: f64,
    pub status: String,
    pub note: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecutionOutcomeSnapshot {
    pub at: String,
    pub relay: String,
    pub target_block: u64,
    pub pair: String,
    pub router: String,
    pub token_in: String,
    pub token_out: String,
    pub victim_tx: String,
    pub outcome: String,
    pub expected_profit_eth: f64,
    pub realized_profit_eth: f64,
    pub gas_used: u64,
    pub submit_latency_ms: f64,
    pub finalization_latency_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ToxicitySnapshot {
    pub hour_utc: u8,
    pub pair: String,
    pub router: String,
    pub samples: u64,
    pub success_rate: f64,
    pub miss_rate: f64,
    pub revert_rate: f64,
    pub realized_capture: f64,
    pub toxicity_score: f64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct OpportunityFunnelSnapshot {
    pub pending_hashes_received: u64,
    pub tx_lookup_success: u64,
    pub tx_lookup_miss: u64,
    pub tx_lookup_throttled: u64,
    pub decode_pass: u64,
    pub decode_reject: u64,
    pub block_lookup_success: u64,
    pub block_lookup_fail: u64,
    pub fast_preflight_pass: u64,
    pub fast_preflight_reject: u64,
    pub adaptive_preflight_pass: u64,
    pub adaptive_preflight_reject: u64,
    pub payload_built: u64,
    pub payload_reject: u64,
    pub ev_gate_pass: u64,
    pub ev_gate_reject: u64,
    pub adaptive_quote_pass: u64,
    pub adaptive_quote_reject: u64,
    pub execution_ready: u64,
    pub submit_attempted: u64,
    pub submit_succeeded: u64,
    pub submit_failed: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct EdgeTelemetrySnapshot {
    pub sample_count: u64,
    pub payload_ready_count: u64,
    pub blocked_count: u64,
    pub last_sample: Option<EdgeSampleSnapshot>,
    pub samples: VecDeque<EdgeSampleSnapshot>,
    pub unsupported_selectors: Vec<UnsupportedSelectorSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UnsupportedSelectorSnapshot {
    pub selector: String,
    pub target: String,
    pub monitored_token_hint: String,
    pub input_bytes: u64,
    pub count: u64,
    pub first_seen: String,
    pub last_seen: String,
    pub sample_tx: String,
    pub sample_reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct EdgeSampleSnapshot {
    pub observed_at: String,
    pub victim_tx: String,
    pub selector: String,
    pub status: String,
    pub reason: String,
    pub route_kind: String,
    pub path: Vec<String>,
    pub hops: u64,
    pub impacted_pools: Vec<String>,
    pub slippage_window_score: f64,
    pub pool_imbalance_score: f64,
    pub cross_dex_deviation_bps: i64,
    pub gas_estimate: u64,
    pub simulated_extraction_native: f64,
    pub aggregator_type: String,
    pub route_complexity: u64,
    pub split_ratio_bps: u64,
    pub dex_sequence: Vec<String>,
    pub route_inefficiency_score: f64,
    pub liquidity_distortion_score: f64,
    pub hop_profitability_rank: Vec<String>,
    pub best_size_bps: u64,
    pub amount_in_wei: String,
    pub amount_out_wei: String,
    pub gross_edge_wei: String,
    pub gross_edge_native: f64,
    pub repayment_wei: String,
    pub repayment_native: f64,
    pub price_impact_bps: u64,
    pub self_slippage_bps: u64,
    pub pool: String,
    pub factory: String,
    pub router: String,
    pub token_in: String,
    pub token_out: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ScavengerObserverReport {
    pub status: String,
    pub phase: String,
    pub observed_secs: u64,
    pub sample_count: u64,
    pub healthy_rpc_now: u64,
    pub active_rpc_now: u64,
    pub disabled_rpc_now: u64,
    pub penalized_rpc_now: u64,
    pub avg_healthy_rpc: f64,
    pub max_block_age_secs: u64,
    pub pending_delta: u64,
    pub lookup_ok_delta: u64,
    pub lookup_miss_delta: u64,
    pub lookup_shed_delta: u64,
    pub decode_pass_delta: u64,
    pub block_fail_delta: u64,
    pub payload_reject_delta: u64,
    pub edge_samples_delta: u64,
    pub lookup_hit_rate_pct: f64,
    pub shed_rate_pct: f64,
    pub decode_pass_rate_pct: f64,
    pub block_fail_rate_pct: f64,
    pub recommendation: String,
    pub notes: Vec<String>,
    pub last_completed: Option<Box<ScavengerObserverReport>>,
}

#[derive(Debug, Clone)]
struct ScavengerObserverSample {
    observed_at: Instant,
    funnel: OpportunityFunnelSnapshot,
    edge_sample_count: u64,
    healthy_rpc: u64,
    active_rpc: u64,
    disabled_rpc: u64,
    penalized_rpc: u64,
    max_block_age_secs: u64,
}

#[derive(Default)]
struct ScavengerObserver {
    samples: VecDeque<ScavengerObserverSample>,
    current_started_at: Option<Instant>,
    last_completed: Option<ScavengerObserverReport>,
}

impl ScavengerObserver {
    fn push(&mut self, state: &DashboardState) {
        let now = Instant::now();
        self.current_started_at.get_or_insert(now);
        let healthy_rpc = state
            .rpc_endpoints
            .iter()
            .filter(|endpoint| {
                !endpoint.disabled
                    && endpoint.cooldown_remaining_secs == 0
                    && endpoint.rate_limit_failures == 0
                    && endpoint.failures == 0
            })
            .count() as u64;
        let active_rpc = state
            .rpc_endpoints
            .iter()
            .filter(|endpoint| !endpoint.disabled)
            .count() as u64;
        let disabled_rpc = state
            .rpc_endpoints
            .iter()
            .filter(|endpoint| endpoint.disabled)
            .count() as u64;
        let penalized_rpc = state
            .rpc_endpoints
            .iter()
            .filter(|endpoint| {
                !endpoint.disabled
                    && (endpoint.cooldown_remaining_secs > 0 || endpoint.rate_limit_failures > 0)
            })
            .count() as u64;
        let max_block_age_secs = state
            .rpc_endpoints
            .iter()
            .filter(|endpoint| !endpoint.disabled)
            .filter_map(|endpoint| endpoint.block_age_secs)
            .max()
            .unwrap_or_default();

        let sample = ScavengerObserverSample {
            observed_at: now,
            funnel: state.opportunity_funnel.clone(),
            edge_sample_count: state.edge_telemetry.sample_count,
            healthy_rpc,
            active_rpc,
            disabled_rpc,
            penalized_rpc,
            max_block_age_secs,
        };
        self.samples.push_back(sample.clone());

        if self
            .current_started_at
            .map(|started| now.saturating_duration_since(started) >= Duration::from_secs(15 * 60))
            .unwrap_or(false)
            && self.samples.len() >= 2
        {
            self.last_completed = Some(build_scavenger_report(&self.samples, "completed", None));
            self.samples.clear();
            self.current_started_at = Some(now);
            self.samples.push_back(sample);
        }
    }

    fn report(&self) -> ScavengerObserverReport {
        build_scavenger_report(
            &self.samples,
            "current",
            self.last_completed.clone().map(Box::new),
        )
    }
}

fn build_scavenger_report(
    samples: &VecDeque<ScavengerObserverSample>,
    phase: &str,
    last_completed: Option<Box<ScavengerObserverReport>>,
) -> ScavengerObserverReport {
    let Some(first) = samples.front() else {
        return ScavengerObserverReport {
            phase: phase.to_string(),
            last_completed,
            ..ScavengerObserverReport::default()
        };
    };
    let Some(last) = samples.back() else {
        return ScavengerObserverReport {
            phase: phase.to_string(),
            last_completed,
            ..ScavengerObserverReport::default()
        };
    };

    let observed_secs = last
        .observed_at
        .saturating_duration_since(first.observed_at)
        .as_secs();
    let sample_count = samples.len() as u64;
    let avg_healthy_rpc = if samples.is_empty() {
        0.0
    } else {
        samples
            .iter()
            .map(|sample| sample.healthy_rpc as f64)
            .sum::<f64>()
            / samples.len() as f64
    };
    let max_block_age_secs = samples
        .iter()
        .map(|sample| sample.max_block_age_secs)
        .max()
        .unwrap_or_default();

    let pending_delta = delta(
        last.funnel.pending_hashes_received,
        first.funnel.pending_hashes_received,
    );
    let lookup_ok_delta = delta(
        last.funnel.tx_lookup_success,
        first.funnel.tx_lookup_success,
    );
    let lookup_miss_delta = delta(last.funnel.tx_lookup_miss, first.funnel.tx_lookup_miss);
    let lookup_shed_delta = delta(
        last.funnel.tx_lookup_throttled,
        first.funnel.tx_lookup_throttled,
    );
    let decode_pass_delta = delta(last.funnel.decode_pass, first.funnel.decode_pass);
    let block_fail_delta = delta(
        last.funnel.block_lookup_fail,
        first.funnel.block_lookup_fail,
    );
    let payload_reject_delta = delta(last.funnel.payload_reject, first.funnel.payload_reject);
    let edge_samples_delta = delta(last.edge_sample_count, first.edge_sample_count);

    let lookup_attempts = lookup_ok_delta.saturating_add(lookup_miss_delta);
    let lookup_hit_rate_pct = pct(lookup_ok_delta, lookup_attempts);
    let shed_rate_pct = pct(lookup_shed_delta, pending_delta);
    let decode_pass_rate_pct = pct(decode_pass_delta, lookup_ok_delta);
    let block_total = delta(
        last.funnel.block_lookup_success,
        first.funnel.block_lookup_success,
    )
    .saturating_add(block_fail_delta);
    let block_fail_rate_pct = pct(block_fail_delta, block_total);

    let mut notes = Vec::new();
    if observed_secs < 15 * 60 {
        notes.push(format!(
            "warming up current cycle: {}s observed, target 900s",
            observed_secs
        ));
    }
    if last.healthy_rpc == 0 {
        notes.push("no healthy rpc at last sample".to_string());
    }
    if avg_healthy_rpc < 1.0 {
        notes.push("average healthy rpc below 1.0".to_string());
    }
    if last.penalized_rpc > 0 {
        notes.push(format!("{} rpc in cooldown/rate-limit", last.penalized_rpc));
    }
    if block_fail_rate_pct > 5.0 {
        notes.push(format!(
            "block lookup fail rate high: {:.1}%",
            block_fail_rate_pct
        ));
    }
    if shed_rate_pct < 25.0 && pending_delta > 1_000 {
        notes.push("shed rate low for high pending flow; rpc may be over-reading".to_string());
    }
    if decode_pass_delta == 0 && lookup_ok_delta > 200 {
        notes.push("lookup ok but decode pass zero; parser/scope may be too narrow".to_string());
    }
    if edge_samples_delta == 0 && decode_pass_delta > 0 {
        notes.push("decode passes without edge samples; payload instrumentation gap".to_string());
    }

    let status = if observed_secs >= 15 * 60
        && last.healthy_rpc >= 1
        && avg_healthy_rpc >= 1.0
        && block_fail_rate_pct <= 5.0
        && lookup_hit_rate_pct >= 15.0
        && edge_samples_delta > 0
    {
        "READY"
    } else if observed_secs < 15 * 60 {
        "WARMING"
    } else if last.healthy_rpc >= 1
        && block_fail_rate_pct <= 10.0
        && lookup_hit_rate_pct >= 10.0
        && decode_pass_delta > 0
    {
        "WATCH"
    } else {
        "NOT_READY"
    }
    .to_string();

    let recommendation = match status.as_str() {
        "READY" => "current cycle stable for contract deploy test; keep low capital and fanout=1"
            .to_string(),
        "WATCH" => {
            "current cycle improved; continue one more clean 15m cycle before live execution"
                .to_string()
        }
        "WARMING" => "new clean 15m cycle running; wait for this cycle to close".to_string(),
        _ => "do not deploy live execution yet; reduce lookup budget or disable degraded rpc"
            .to_string(),
    };

    ScavengerObserverReport {
        status,
        phase: phase.to_string(),
        observed_secs,
        sample_count,
        healthy_rpc_now: last.healthy_rpc,
        active_rpc_now: last.active_rpc,
        disabled_rpc_now: last.disabled_rpc,
        penalized_rpc_now: last.penalized_rpc,
        avg_healthy_rpc,
        max_block_age_secs,
        pending_delta,
        lookup_ok_delta,
        lookup_miss_delta,
        lookup_shed_delta,
        decode_pass_delta,
        block_fail_delta,
        payload_reject_delta,
        edge_samples_delta,
        lookup_hit_rate_pct,
        shed_rate_pct,
        decode_pass_rate_pct,
        block_fail_rate_pct,
        recommendation,
        notes,
        last_completed,
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardState {
    pub runtime_mode: String,
    pub market_regime: String,
    pub allow_send: bool,
    pub runtime_paused: bool,
    pub network: String,
    pub native_asset_symbol: String,
    pub control_address: String,
    pub vault_address: String,
    pub executor_address: String,
    pub profit_address: String,
    pub min_candidate_eth: String,
    pub min_net_profit_eth: String,
    pub min_profit_usd: String,
    pub min_liquidity_eth: String,
    pub executor_min_buffer_eth: String,
    pub executor_target_buffer_eth: String,
    pub executor_max_buffer_eth: String,
    pub executor_balance_eth: Option<String>,
    pub executor_buffer_status: String,
    pub treasury_action: String,
    pub treasury_recommended_amount_eth: String,
    pub treasury_status: String,
    pub treasury_note: String,
    pub private_relay_only: bool,
    pub opportunity_mode: String,
    pub scan_interval_ms: u64,
    pub wallet_count: usize,
    pub total_keys_read: usize,
    pub duplicate_keys: usize,
    pub invalid_keys: usize,
    pub last_scan_at: Option<String>,
    pub last_scan_duration_ms: Option<u128>,
    pub sweeps_attempted: u64,
    pub sweeps_succeeded: u64,
    pub sweeps_failed: u64,
    pub hot_wallets: Vec<WalletSnapshot>,
    pub top_residual_wallets: Vec<WalletResidualSnapshot>,
    pub rpc_endpoints: Vec<RpcEndpointSnapshot>,
    pub recent_events: VecDeque<DashboardEvent>,
    pub latency_metrics: Vec<LatencyMetric>,
    pub latency_risk: LatencyRiskSnapshot,
    pub opportunity_funnel: OpportunityFunnelSnapshot,
    pub edge_telemetry: EdgeTelemetrySnapshot,
    pub scavenger_observer: ScavengerObserverReport,
    pub reject_reasons: Vec<RejectReasonSnapshot>,
    pub relay_rankings: Vec<RelaySnapshot>,
    pub toxicity_profiles: Vec<ToxicitySnapshot>,
    pub treasury_rebalance_trail: Vec<TreasurySnapshot>,
    pub execution_outcomes: Vec<ExecutionOutcomeSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencyMetric {
    pub stage: String,
    pub samples: u64,
    pub last_ms: Option<u128>,
    pub avg_ms: Option<u128>,
    pub max_ms: Option<u128>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencyRiskSnapshot {
    pub updated_at: String,
    pub window_secs: u64,
    pub sample_count: u64,
    pub hot_path_avg_ms: u128,
    pub hot_path_max_ms: u128,
    pub monitor_avg_ms: u128,
    pub rpc_avg_ms: u128,
    pub score: f64,
    pub level: String,
    pub bottleneck_stage: String,
}

#[derive(Debug, Deserialize)]
struct RpcToggleRequest {
    enabled: bool,
    reason: Option<String>,
}

#[derive(Debug, Serialize)]
struct RpcControlResponse {
    ok: bool,
    message: String,
    rpc_endpoints: Vec<RpcEndpointSnapshot>,
}

#[derive(Debug, Serialize)]
struct EventsControlResponse {
    ok: bool,
    message: String,
}

#[derive(Debug, Deserialize)]
struct RuntimePauseRequest {
    paused: bool,
}

#[derive(Debug, Serialize)]
struct RuntimePauseResponse {
    ok: bool,
    paused: bool,
    message: String,
}

#[derive(Debug, Serialize)]
struct OpportunityModeResponse {
    ok: bool,
    mode: String,
    message: String,
}

#[derive(Debug, Deserialize)]
struct OpportunityThresholdRequest {
    min_large_swap_eth: Option<f64>,
    min_net_profit_eth: Option<f64>,
    min_profit_usd: Option<f64>,
    min_liquidity_eth: Option<f64>,
}

#[derive(Debug, Serialize)]
struct OpportunityThresholdResponse {
    ok: bool,
    message: String,
    thresholds: OpportunityThresholds,
}

impl DashboardHandle {
    pub fn new(
        config: &Config,
        wallet_count: usize,
        total_keys_read: usize,
        duplicate_keys: usize,
        invalid_keys: usize,
        storage: Storage,
        rpc_fleet: Arc<RpcFleet>,
    ) -> Self {
        let recent_events = storage
            .recent_events(50)
            .map(VecDeque::from)
            .unwrap_or_default();
        let (attempted, succeeded, failed) = storage.sweep_counts().unwrap_or((0, 0, 0));
        let latency_metrics =
            build_latency_metrics(storage.telemetry_summary().unwrap_or_default());
        let latency_risk = build_latency_risk(
            storage.telemetry_window_summary(300).unwrap_or_default(),
            &rpc_fleet.snapshot(),
        );
        let top_residual_wallets = storage
            .top_wallet_residuals(10)
            .unwrap_or_default()
            .into_iter()
            .map(
                |(
                    wallet,
                    asset_class,
                    detections,
                    successful_sweeps,
                    detected_profit_wei,
                    realized_profit_wei,
                    last_seen_at,
                )| {
                    let detected = wei_str_to_eth(&detected_profit_wei);
                    let realized = wei_str_to_eth(&realized_profit_wei);
                    let score = detections
                        .saturating_mul(100)
                        .saturating_add(successful_sweeps.saturating_mul(250))
                        .saturating_add((detected * 10_000.0) as u64);
                    WalletResidualSnapshot {
                        wallet,
                        asset_class,
                        detections,
                        successful_sweeps,
                        detected_profit_eth: format!("{detected:.6}"),
                        realized_profit_eth: format!("{realized:.6}"),
                        residual_score: score,
                        last_seen_at,
                    }
                },
            )
            .collect();
        let relay_rankings = storage.relay_rankings(12).unwrap_or_default();
        let toxicity_profiles = storage.toxicity_profiles(12).unwrap_or_default();
        let treasury_rebalance_trail = storage.treasury_rebalance_trail(12).unwrap_or_default();
        let execution_outcomes = storage.execution_outcomes(12).unwrap_or_default();
        let (treasury_action, treasury_recommended_amount_eth, treasury_status, treasury_note) =
            treasury_rebalance_trail
                .first()
                .map(|entry| {
                    (
                        entry.action.clone(),
                        format!("{:.6}", entry.recommended_amount_eth),
                        entry.status.clone(),
                        entry.note.clone(),
                    )
                })
                .unwrap_or_else(|| {
                    (
                        "hold".to_string(),
                        "0.000000".to_string(),
                        "unknown".to_string(),
                        "no treasury signals yet".to_string(),
                    )
                });
        Self {
            inner: Arc::new(RwLock::new(DashboardState {
                runtime_mode: "fee-extraction".to_string(),
                market_regime: "normal".to_string(),
                allow_send: config.allow_send,
                runtime_paused: false,
                network: config.network.clone(),
                native_asset_symbol: config.native_asset_symbol().to_string(),
                control_address: format!("{:?}", config.control_address),
                vault_address: format!("{:?}", config.vault_address),
                executor_address: format!("{:?}", config.executor_address),
                profit_address: format!("{:?}", config.profit_address),
                min_candidate_eth: config
                    .mev
                    .runtime_thresholds()
                    .min_large_swap_eth
                    .to_string(),
                min_net_profit_eth: config
                    .mev
                    .runtime_thresholds()
                    .min_net_profit_eth
                    .to_string(),
                min_profit_usd: config.mev.runtime_thresholds().min_profit_usd.to_string(),
                min_liquidity_eth: config
                    .mev
                    .runtime_thresholds()
                    .min_liquidity_eth
                    .to_string(),
                executor_min_buffer_eth: format!("{:.4}", config.mev.executor_min_buffer_eth),
                executor_target_buffer_eth: format!("{:.4}", config.mev.executor_target_buffer_eth),
                executor_max_buffer_eth: format!("{:.4}", config.mev.executor_max_buffer_eth),
                executor_balance_eth: None,
                executor_buffer_status: "unknown".to_string(),
                treasury_action,
                treasury_recommended_amount_eth,
                treasury_status,
                treasury_note,
                private_relay_only: true,
                opportunity_mode: config.mev.opportunity_mode().as_str().to_string(),
                scan_interval_ms: config.mev.max_pending_age_ms,
                wallet_count,
                total_keys_read,
                duplicate_keys,
                invalid_keys,
                last_scan_at: None,
                last_scan_duration_ms: None,
                sweeps_attempted: attempted,
                sweeps_succeeded: succeeded,
                sweeps_failed: failed,
                hot_wallets: Vec::new(),
                top_residual_wallets,
                rpc_endpoints: rpc_fleet.snapshot(),
                recent_events,
                latency_metrics,
                latency_risk,
                opportunity_funnel: OpportunityFunnelSnapshot::default(),
                edge_telemetry: EdgeTelemetrySnapshot::default(),
                scavenger_observer: ScavengerObserverReport::default(),
                reject_reasons: Vec::new(),
                relay_rankings,
                toxicity_profiles,
                treasury_rebalance_trail,
                execution_outcomes,
            })),
            storage,
            rpc_fleet,
            opportunity_mode: config.mev.opportunity_mode.clone(),
            runtime_thresholds: config.mev.runtime_thresholds.clone(),
            runtime_paused: Arc::new(AtomicBool::new(false)),
            pending: Arc::new(Mutex::new(PendingStorageWrites::default())),
            observer: Arc::new(Mutex::new(ScavengerObserver::default())),
            last_storage_prune: Arc::new(Mutex::new(
                Instant::now()
                    .checked_sub(Duration::from_secs(120))
                    .unwrap_or_else(Instant::now),
            )),
            file_telemetry: Arc::new(FileTelemetry::from_env()),
        }
    }

    pub fn snapshot(&self) -> DashboardState {
        let mut state = self.inner.read().expect("dashboard state lock").clone();
        state.runtime_paused = self.runtime_paused.load(Ordering::Relaxed);
        state.rpc_endpoints = self.rpc_fleet.snapshot();
        if let Ok(toxicity_profiles) = self.storage.toxicity_profiles(12) {
            state.toxicity_profiles = toxicity_profiles;
        }
        if let Ok(relay_rankings) = self.storage.relay_rankings(12) {
            state.relay_rankings = relay_rankings;
        }
        if let Ok(top_residual_wallets) = self.storage.top_wallet_residuals(10) {
            state.top_residual_wallets = top_residual_wallets
                .into_iter()
                .map(
                    |(
                        wallet,
                        asset_class,
                        detections,
                        successful_sweeps,
                        detected_profit_wei,
                        realized_profit_wei,
                        last_seen_at,
                    )| {
                        let detected = wei_str_to_eth(&detected_profit_wei);
                        let realized = wei_str_to_eth(&realized_profit_wei);
                        let score = detections
                            .saturating_mul(100)
                            .saturating_add(successful_sweeps.saturating_mul(250))
                            .saturating_add((detected * 10_000.0) as u64);
                        WalletResidualSnapshot {
                            wallet,
                            asset_class,
                            detections,
                            successful_sweeps,
                            detected_profit_eth: format!("{detected:.6}"),
                            realized_profit_eth: format!("{realized:.6}"),
                            residual_score: score,
                            last_seen_at,
                        }
                    },
                )
                .collect();
        }
        if let Ok(treasury_rebalance_trail) = self.storage.treasury_rebalance_trail(12) {
            state.treasury_rebalance_trail = treasury_rebalance_trail;
        }
        if let Ok(execution_outcomes) = self.storage.execution_outcomes(12) {
            state.execution_outcomes = execution_outcomes;
        }
        state.opportunity_mode = self
            .opportunity_mode
            .read()
            .map(|mode| mode.as_str().to_string())
            .unwrap_or_else(|_| "conservative".to_string());
        if let Ok(thresholds) = self.runtime_thresholds.read() {
            state.min_candidate_eth = thresholds.min_large_swap_eth.to_string();
            state.min_net_profit_eth = thresholds.min_net_profit_eth.to_string();
            state.min_profit_usd = thresholds.min_profit_usd.to_string();
            state.min_liquidity_eth = thresholds.min_liquidity_eth.to_string();
        }
        state.latency_risk = build_latency_risk(
            self.storage
                .telemetry_window_summary(300)
                .unwrap_or_default(),
            &state.rpc_endpoints,
        );
        state.scavenger_observer = self.observe_scavenger_window(&state);
        state
    }

    fn observe_scavenger_window(&self, state: &DashboardState) -> ScavengerObserverReport {
        let Ok(mut observer) = self.observer.lock() else {
            return ScavengerObserverReport::default();
        };
        observer.push(state);
        observer.report()
    }

    pub fn runtime_paused(&self) -> bool {
        self.runtime_paused.load(Ordering::Relaxed)
    }

    pub fn set_runtime_paused(&self, paused: bool) {
        self.runtime_paused.store(paused, Ordering::Relaxed);
        let mut state = self.inner.write().expect("dashboard state lock");
        state.runtime_paused = paused;
    }

    pub fn event(&self, level: &str, message: impl Into<String>) {
        let message = message.into();
        if let Ok(mut pending) = self.pending.lock() {
            pending.events.push((level.to_string(), message.clone()));
        }
        self.file_telemetry.record_event(level, &message);
        let mut state = self.inner.write().expect("dashboard state lock");
        push_event(&mut state.recent_events, level, message);
    }

    pub fn clear_events(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Ok(mut pending) = self.pending.lock() {
            pending.events.clear();
        }
        self.storage.clear_events()?;
        let mut state = self.inner.write().expect("dashboard state lock");
        state.recent_events.clear();
        Ok(())
    }

    pub fn record_latency(
        &self,
        stage: &str,
        duration_ms: u128,
        wallet: Option<&str>,
        note: Option<&str>,
    ) {
        if let Ok(mut pending) = self.pending.lock() {
            if storage_telemetry_enabled() {
                pending.latencies.push(PendingLatency {
                    stage: stage.to_string(),
                    duration_ms,
                    wallet: wallet.map(str::to_string),
                    note: note.map(str::to_string),
                });
            }
            if storage_rollups_enabled() {
                let bucket = telemetry_bucket_minute();
                let key = format!("{bucket}|{stage}");
                let entry =
                    pending
                        .latency_rollups
                        .entry(key)
                        .or_insert_with(|| PendingLatencyRollup {
                            bucket,
                            stage: stage.to_string(),
                            samples: 0,
                            total_ms: 0,
                            max_ms: 0,
                            last_ms: 0,
                        });
                entry.samples = entry.samples.saturating_add(1);
                entry.total_ms = entry.total_ms.saturating_add(duration_ms);
                entry.max_ms = entry.max_ms.max(duration_ms);
                entry.last_ms = duration_ms;
            }
        }
        self.file_telemetry
            .record_latency(stage, duration_ms, wallet, note);
        let mut state = self.inner.write().expect("dashboard state lock");
        upsert_latency_metric(&mut state.latency_metrics, stage, duration_ms);
    }

    pub fn set_market_regime(&self, regime: &str) {
        let mut state = self.inner.write().expect("dashboard state lock");
        state.market_regime = regime.to_string();
    }

    pub fn record_reject_reason(&self, stage: &str, reason: &str) {
        if storage_rollups_enabled() {
            if let Ok(mut pending) = self.pending.lock() {
                let bucket = telemetry_bucket_minute();
                let key = format!("{bucket}|{stage}|{reason}");
                let entry = pending.reject_reason_rollups.entry(key).or_insert_with(|| {
                    PendingRejectReasonRollup {
                        bucket,
                        stage: stage.to_string(),
                        reason: reason.to_string(),
                        count: 0,
                    }
                });
                entry.count = entry.count.saturating_add(1);
            }
        }
        let mut state = self.inner.write().expect("dashboard state lock");
        if let Some(entry) = state
            .reject_reasons
            .iter_mut()
            .find(|entry| entry.stage == stage && entry.reason == reason)
        {
            entry.count = entry.count.saturating_add(1);
        } else {
            state.reject_reasons.push(RejectReasonSnapshot {
                stage: stage.to_string(),
                reason: reason.to_string(),
                count: 1,
            });
        }
        state
            .reject_reasons
            .sort_by(|left, right| right.count.cmp(&left.count));
        state.reject_reasons.truncate(8);
    }

    pub fn record_opportunity_funnel(&self, stage: &str) {
        if storage_rollups_enabled() {
            if let Ok(mut pending) = self.pending.lock() {
                let bucket = telemetry_bucket_minute();
                let key = format!("{bucket}|{stage}");
                let entry =
                    pending
                        .funnel_rollups
                        .entry(key)
                        .or_insert_with(|| PendingCounterRollup {
                            bucket,
                            stage: stage.to_string(),
                            count: 0,
                        });
                entry.count = entry.count.saturating_add(1);
            }
        }
        let mut state = self.inner.write().expect("dashboard state lock");
        let funnel = &mut state.opportunity_funnel;
        match stage {
            "pending_hashes_received" => {
                funnel.pending_hashes_received = funnel.pending_hashes_received.saturating_add(1)
            }
            "tx_lookup_success" => {
                funnel.tx_lookup_success = funnel.tx_lookup_success.saturating_add(1)
            }
            "tx_lookup_miss" => funnel.tx_lookup_miss = funnel.tx_lookup_miss.saturating_add(1),
            "tx_lookup_throttled" => {
                funnel.tx_lookup_throttled = funnel.tx_lookup_throttled.saturating_add(1)
            }
            "decode_pass" => funnel.decode_pass = funnel.decode_pass.saturating_add(1),
            "decode_reject" => funnel.decode_reject = funnel.decode_reject.saturating_add(1),
            "block_lookup_success" => {
                funnel.block_lookup_success = funnel.block_lookup_success.saturating_add(1)
            }
            "block_lookup_fail" => {
                funnel.block_lookup_fail = funnel.block_lookup_fail.saturating_add(1)
            }
            "fast_preflight_pass" => {
                funnel.fast_preflight_pass = funnel.fast_preflight_pass.saturating_add(1)
            }
            "fast_preflight_reject" => {
                funnel.fast_preflight_reject = funnel.fast_preflight_reject.saturating_add(1)
            }
            "adaptive_preflight_pass" => {
                funnel.adaptive_preflight_pass = funnel.adaptive_preflight_pass.saturating_add(1)
            }
            "adaptive_preflight_reject" => {
                funnel.adaptive_preflight_reject =
                    funnel.adaptive_preflight_reject.saturating_add(1)
            }
            "payload_built" => funnel.payload_built = funnel.payload_built.saturating_add(1),
            "payload_reject" => funnel.payload_reject = funnel.payload_reject.saturating_add(1),
            "ev_gate_pass" => funnel.ev_gate_pass = funnel.ev_gate_pass.saturating_add(1),
            "ev_gate_reject" => funnel.ev_gate_reject = funnel.ev_gate_reject.saturating_add(1),
            "adaptive_quote_pass" => {
                funnel.adaptive_quote_pass = funnel.adaptive_quote_pass.saturating_add(1)
            }
            "adaptive_quote_reject" => {
                funnel.adaptive_quote_reject = funnel.adaptive_quote_reject.saturating_add(1)
            }
            "execution_ready" => funnel.execution_ready = funnel.execution_ready.saturating_add(1),
            "submit_attempted" => {
                funnel.submit_attempted = funnel.submit_attempted.saturating_add(1)
            }
            "submit_succeeded" => {
                funnel.submit_succeeded = funnel.submit_succeeded.saturating_add(1)
            }
            "submit_failed" => funnel.submit_failed = funnel.submit_failed.saturating_add(1),
            _ => {}
        }
    }

    pub fn record_edge_sample(&self, sample: EdgeMetadata) {
        let mut state = self.inner.write().expect("dashboard state lock");
        let snapshot = EdgeSampleSnapshot {
            observed_at: Utc::now().to_rfc3339(),
            victim_tx: sample.victim_tx,
            selector: sample.selector,
            status: sample.status,
            reason: sample.reason,
            route_kind: sample.route_kind,
            path: sample.path,
            hops: sample.hops,
            impacted_pools: sample.impacted_pools,
            slippage_window_score: sample.slippage_window_score,
            pool_imbalance_score: sample.pool_imbalance_score,
            cross_dex_deviation_bps: sample.cross_dex_deviation_bps,
            gas_estimate: sample.gas_estimate,
            simulated_extraction_native: sample.simulated_extraction_native,
            aggregator_type: sample.aggregator_type,
            route_complexity: sample.route_complexity,
            split_ratio_bps: sample.split_ratio_bps,
            dex_sequence: sample.dex_sequence,
            route_inefficiency_score: sample.route_inefficiency_score,
            liquidity_distortion_score: sample.liquidity_distortion_score,
            hop_profitability_rank: sample.hop_profitability_rank,
            best_size_bps: sample.best_size_bps,
            amount_in_wei: sample.amount_in_wei,
            amount_out_wei: sample.amount_out_wei,
            gross_edge_wei: sample.gross_edge_wei,
            gross_edge_native: sample.gross_edge_native,
            repayment_wei: sample.repayment_wei,
            repayment_native: sample.repayment_native,
            price_impact_bps: sample.price_impact_bps,
            self_slippage_bps: sample.self_slippage_bps,
            pool: sample.pool,
            factory: sample.factory,
            router: sample.router,
            token_in: sample.token_in,
            token_out: sample.token_out,
        };

        state.edge_telemetry.sample_count = state.edge_telemetry.sample_count.saturating_add(1);
        if snapshot.status == "payload_built" {
            state.edge_telemetry.payload_ready_count =
                state.edge_telemetry.payload_ready_count.saturating_add(1);
        } else {
            state.edge_telemetry.blocked_count =
                state.edge_telemetry.blocked_count.saturating_add(1);
        }
        let file_snapshot = snapshot.clone();
        state.edge_telemetry.last_sample = Some(snapshot.clone());
        state.edge_telemetry.samples.push_front(snapshot);
        while state.edge_telemetry.samples.len() > 50 {
            state.edge_telemetry.samples.pop_back();
        }
        let unsupported_selector =
            record_unsupported_selector(&mut state.edge_telemetry, &file_snapshot);
        let sample_count = state.edge_telemetry.sample_count;
        if sample_count <= 3 || sample_count % 25 == 0 {
            push_event(
                &mut state.recent_events,
                "info",
                format!("edge telemetry sample recorded count={sample_count}"),
            );
        }
        drop(state);
        if let Some(record) = unsupported_selector {
            if let Ok(mut pending) = self.pending.lock() {
                pending.unsupported_selectors.push(record);
            }
        }
        self.file_telemetry.record_edge_sample(&file_snapshot);
    }

    pub fn set_relay_rankings(&self, relays: Vec<RelaySnapshot>) {
        let mut state = self.inner.write().expect("dashboard state lock");
        state.relay_rankings = relays;
    }

    pub fn record_relay_outcome(&self, update: RelaySnapshotUpdate<'_>) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.relay_updates.push(PendingRelayUpdate {
                relay: update.relay.to_string(),
                accepted: u64::from(update.accepted),
                submit_failed: u64::from(update.submit_failed),
                included_success: u64::from(update.included_success),
                included_revert: u64::from(update.included_revert),
                not_included_timeout: u64::from(update.not_included_timeout),
                submit_latency_ms: update.submit_latency_ms,
                finalization_latency_ms: update.finalization_latency_ms,
                score: update.score,
                pressure: update.pressure,
                accept_rate: update.accept_rate,
                inclusion_rate: update.inclusion_rate,
            });
        }

        let mut state = self.inner.write().expect("dashboard state lock");
        upsert_relay_snapshot(&mut state.relay_rankings, update);
    }

    pub fn set_executor_balance(&self, balance_eth: f64, status: &str) {
        let mut state = self.inner.write().expect("dashboard state lock");
        state.executor_balance_eth = Some(format!("{:.6}", balance_eth));
        state.executor_buffer_status = status.to_string();
    }

    pub fn set_hot_wallets(&self, wallets: Vec<WalletSnapshot>) {
        let mut state = self.inner.write().expect("dashboard state lock");
        state.hot_wallets = wallets;
    }

    pub fn record_treasury_recommendation(&self, update: TreasuryRecommendationUpdate<'_>) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.treasury_updates.push(PendingTreasuryUpdate {
                executor_address: update.executor_address.to_string(),
                vault_address: update.vault_address.to_string(),
                profit_address: update.profit_address.to_string(),
                balance_eth: update.balance_eth,
                min_buffer_eth: update.min_buffer_eth,
                target_buffer_eth: update.target_buffer_eth,
                max_buffer_eth: update.max_buffer_eth,
                action: update.action.to_string(),
                recommended_amount_eth: update.recommended_amount_eth,
                status: update.status.to_string(),
                note: update.note.to_string(),
            });
        }

        let mut state = self.inner.write().expect("dashboard state lock");
        state.treasury_action = update.action.to_string();
        state.treasury_recommended_amount_eth = format!("{:.6}", update.recommended_amount_eth);
        state.treasury_status = update.status.to_string();
        state.treasury_note = update.note.to_string();
        state.treasury_rebalance_trail.insert(
            0,
            TreasurySnapshot {
                at: Utc::now().to_rfc3339(),
                executor_address: update.executor_address.to_string(),
                vault_address: update.vault_address.to_string(),
                profit_address: update.profit_address.to_string(),
                balance_eth: update.balance_eth,
                min_buffer_eth: update.min_buffer_eth,
                target_buffer_eth: update.target_buffer_eth,
                max_buffer_eth: update.max_buffer_eth,
                action: update.action.to_string(),
                recommended_amount_eth: update.recommended_amount_eth,
                status: update.status.to_string(),
                note: update.note.to_string(),
            },
        );
        state.treasury_rebalance_trail.truncate(12);
    }

    pub fn record_execution_outcome(&self, update: ExecutionOutcomeUpdate<'_>) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.execution_outcomes.push(PendingExecutionOutcome {
                relay: update.relay.to_string(),
                target_block: update.target_block,
                pair: update.pair.to_string(),
                router: update.router.to_string(),
                token_in: update.token_in.to_string(),
                token_out: update.token_out.to_string(),
                victim_tx: update.victim_tx.to_string(),
                outcome: update.outcome.to_string(),
                expected_profit_eth: update.expected_profit_eth,
                realized_profit_eth: update.realized_profit_eth,
                gas_used: update.gas_used,
                submit_latency_ms: update.submit_latency_ms,
                finalization_latency_ms: update.finalization_latency_ms,
            });
        }

        let mut state = self.inner.write().expect("dashboard state lock");
        state.execution_outcomes.insert(
            0,
            ExecutionOutcomeSnapshot {
                at: Utc::now().to_rfc3339(),
                relay: update.relay.to_string(),
                target_block: update.target_block,
                pair: update.pair.to_string(),
                router: update.router.to_string(),
                token_in: update.token_in.to_string(),
                token_out: update.token_out.to_string(),
                victim_tx: update.victim_tx.to_string(),
                outcome: update.outcome.to_string(),
                expected_profit_eth: update.expected_profit_eth,
                realized_profit_eth: update.realized_profit_eth,
                gas_used: update.gas_used,
                submit_latency_ms: update.submit_latency_ms,
                finalization_latency_ms: update.finalization_latency_ms,
            },
        );
        state.execution_outcomes.truncate(12);
    }

    pub fn flush_storage_buffers(&self) {
        let pending = {
            let Ok(mut pending) = self.pending.lock() else {
                return;
            };
            std::mem::take(&mut *pending)
        };

        if storage_events_enabled() {
            for (level, message) in pending.events {
                self.storage.log_event(&level, &message);
            }
        }

        if storage_telemetry_enabled() {
            for latency in pending.latencies {
                self.storage.log_telemetry(
                    &latency.stage,
                    latency.duration_ms,
                    latency.wallet.as_deref(),
                    latency.note.as_deref(),
                );
            }
        }

        for latency in pending.latency_rollups.into_values() {
            self.storage.record_latency_rollup(
                &latency.bucket,
                &latency.stage,
                latency.samples,
                latency.total_ms,
                latency.max_ms,
                latency.last_ms,
            );
        }

        for funnel in pending.funnel_rollups.into_values() {
            self.storage
                .record_funnel_rollup(&funnel.bucket, &funnel.stage, funnel.count);
        }

        for reject in pending.reject_reason_rollups.into_values() {
            self.storage.record_reject_reason_rollup(
                &reject.bucket,
                &reject.stage,
                &reject.reason,
                reject.count,
            );
        }

        for relay in pending.relay_updates {
            self.storage.record_relay_outcome(
                &relay.relay,
                relay.accepted,
                relay.submit_failed,
                relay.included_success,
                relay.included_revert,
                relay.not_included_timeout,
                relay.submit_latency_ms,
                relay.finalization_latency_ms,
                relay.score,
                relay.pressure,
                relay.accept_rate,
                relay.inclusion_rate,
            );
        }

        if let Ok(mut last_prune) = self.last_storage_prune.lock() {
            if last_prune.elapsed() >= Duration::from_secs(60) {
                if let Err(err) = self.storage.prune_runtime_tables() {
                    tracing::warn!("storage runtime prune failed: {}", err);
                }
                *last_prune = Instant::now();
            }
        }

        for treasury in pending.treasury_updates {
            self.storage.record_treasury_recommendation(
                &treasury.executor_address,
                &treasury.vault_address,
                &treasury.profit_address,
                treasury.balance_eth,
                treasury.min_buffer_eth,
                treasury.target_buffer_eth,
                treasury.max_buffer_eth,
                &treasury.action,
                treasury.recommended_amount_eth,
                &treasury.status,
                &treasury.note,
            );
        }

        for outcome in pending.execution_outcomes {
            self.storage.record_execution_outcome(
                &outcome.relay,
                outcome.target_block,
                &outcome.pair,
                &outcome.router,
                &outcome.token_in,
                &outcome.token_out,
                &outcome.victim_tx,
                &outcome.outcome,
                outcome.expected_profit_eth,
                outcome.realized_profit_eth,
                outcome.gas_used,
                outcome.submit_latency_ms,
                outcome.finalization_latency_ms,
            );
        }

        for unsupported in pending.unsupported_selectors {
            self.storage.record_unsupported_selector(&unsupported);
        }
    }
}

fn record_unsupported_selector(
    telemetry: &mut EdgeTelemetrySnapshot,
    sample: &EdgeSampleSnapshot,
) -> Option<UnsupportedSelectorRecord> {
    if sample.status != "decode_reject" {
        return None;
    }
    let reason = sample.reason.to_ascii_lowercase();
    if !reason.contains("selector_unsupported") && !reason.contains("unsupported selector") {
        return None;
    }

    let selector = if sample.selector.trim().is_empty() {
        "unknown".to_string()
    } else {
        sample.selector.clone()
    };
    let target = reason_field(&sample.reason, "target")
        .or_else(|| reason_field(&sample.reason, "to"))
        .unwrap_or_else(|| sample.router.clone());
    let inner_selector = reason_field(&sample.reason, "inner_selector")
        .or_else(|| reason_field(&sample.reason, "inner"))
        .unwrap_or_default();
    let monitored_token_hint = sample
        .path
        .iter()
        .take(6)
        .cloned()
        .collect::<Vec<_>>()
        .join(",");
    let monitored_token_hint = (!monitored_token_hint.is_empty())
        .then_some(monitored_token_hint)
        .unwrap_or_else(|| "unknown".to_string());
    let input_bytes = reason_field(&sample.reason, "input_bytes")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_else(|| {
            sample
                .route_complexity
                .saturating_mul(128)
                .saturating_add(4)
        });
    let now = sample.observed_at.clone();

    if let Some(existing) = telemetry
        .unsupported_selectors
        .iter_mut()
        .find(|item| item.selector == selector && item.target == target)
    {
        existing.count = existing.count.saturating_add(1);
        existing.last_seen = now;
        existing.sample_tx = sample.victim_tx.clone();
        existing.sample_reason = compact_dashboard_text(&sample.reason, 180);
        if existing.monitored_token_hint == "unknown" && monitored_token_hint != "unknown" {
            existing.monitored_token_hint = monitored_token_hint.clone();
        }
        if existing.input_bytes == 0 {
            existing.input_bytes = input_bytes;
        }
    } else {
        telemetry
            .unsupported_selectors
            .push(UnsupportedSelectorSnapshot {
                selector: selector.clone(),
                target: target.clone(),
                monitored_token_hint: monitored_token_hint.clone(),
                input_bytes,
                count: 1,
                first_seen: now.clone(),
                last_seen: now,
                sample_tx: sample.victim_tx.clone(),
                sample_reason: compact_dashboard_text(&sample.reason, 180),
            });
    }

    telemetry.unsupported_selectors.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then(left.selector.cmp(&right.selector))
    });
    telemetry.unsupported_selectors.truncate(20);

    Some(UnsupportedSelectorRecord {
        target,
        selector,
        inner_selector,
        token_hints: monitored_token_hint,
        input_bytes,
        sample_tx: sample.victim_tx.clone(),
        sample_calldata_prefix: reason_field(&sample.reason, "calldata_prefix")
            .unwrap_or_else(|| "unknown".to_string()),
        route_hint: sample
            .hop_profitability_rank
            .iter()
            .take(4)
            .cloned()
            .collect::<Vec<_>>()
            .join(" | "),
    })
}

fn reason_field(reason: &str, key: &str) -> Option<String> {
    let prefix = format!("{key}=");
    reason
        .split_whitespace()
        .find_map(|part| part.strip_prefix(&prefix))
        .map(|value| {
            value
                .trim_matches(|ch: char| ch == ',' || ch == ';' || ch == ')' || ch == '(')
                .to_string()
        })
        .filter(|value| !value.is_empty())
}

fn compact_dashboard_text(value: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for ch in value.chars().take(max_chars) {
        out.push(ch);
    }
    if value.chars().count() > max_chars {
        out.push_str("...");
    }
    out
}

pub struct RelaySnapshotUpdate<'a> {
    pub relay: &'a str,
    pub accepted: bool,
    pub submit_failed: bool,
    pub included_success: bool,
    pub included_revert: bool,
    pub not_included_timeout: bool,
    pub submit_latency_ms: Option<f64>,
    pub finalization_latency_ms: Option<f64>,
    pub score: Option<f64>,
    pub pressure: Option<f64>,
    pub accept_rate: Option<f64>,
    pub inclusion_rate: Option<f64>,
}

pub struct TreasuryRecommendationUpdate<'a> {
    pub executor_address: &'a str,
    pub vault_address: &'a str,
    pub profit_address: &'a str,
    pub balance_eth: f64,
    pub min_buffer_eth: f64,
    pub target_buffer_eth: f64,
    pub max_buffer_eth: f64,
    pub action: &'a str,
    pub recommended_amount_eth: f64,
    pub status: &'a str,
    pub note: &'a str,
}

pub struct ExecutionOutcomeUpdate<'a> {
    pub relay: &'a str,
    pub target_block: u64,
    pub pair: &'a str,
    pub router: &'a str,
    pub token_in: &'a str,
    pub token_out: &'a str,
    pub victim_tx: &'a str,
    pub outcome: &'a str,
    pub expected_profit_eth: f64,
    pub realized_profit_eth: f64,
    pub gas_used: u64,
    pub submit_latency_ms: f64,
    pub finalization_latency_ms: f64,
}

pub async fn run_server(
    dashboard: DashboardHandle,
    bind_addr: std::net::SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let app = Router::new()
        .route("/", get(index))
        .route("/styles.css", get(styles))
        .route("/favicon.svg", get(favicon))
        .route("/js/app.js", get(js_app))
        .route("/js/data.js", get(js_data))
        .route("/js/fx.js", get(js_fx))
        .route("/js/radar.js", get(js_radar))
        .route("/api/status", get(status))
        .route("/api/export", get(status))
        .route("/api/database", get(database_status))
        .route("/api/database/export", post(database_export))
        .route("/api/database/download/:artifact", get(database_download))
        .route("/api/rpc/:id/enabled", post(set_rpc_enabled))
        .route("/api/rpc/only-getblock", post(only_getblock))
        .route("/api/runtime/pause", post(set_runtime_paused))
        .route("/api/events/clear", post(clear_events))
        .route("/api/opportunity-mode/:mode", post(set_opportunity_mode))
        .route(
            "/api/opportunity-thresholds",
            post(set_opportunity_thresholds),
        )
        .with_state(dashboard)
        .layer(CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "text/html; charset=utf-8")],
        Html(STATIC_INDEX_HTML),
    )
}

async fn styles() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "text/css; charset=utf-8")],
        STATIC_STYLES_CSS,
    )
}

async fn favicon() -> impl IntoResponse {
    ([(CONTENT_TYPE, "image/svg+xml")], STATIC_FAVICON_SVG)
}

async fn js_app() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "application/javascript; charset=utf-8")],
        STATIC_JS_APP,
    )
}

async fn js_data() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "application/javascript; charset=utf-8")],
        STATIC_JS_DATA,
    )
}

async fn js_fx() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "application/javascript; charset=utf-8")],
        STATIC_JS_FX,
    )
}

async fn js_radar() -> impl IntoResponse {
    (
        [(CONTENT_TYPE, "application/javascript; charset=utf-8")],
        STATIC_JS_RADAR,
    )
}

async fn status(State(dashboard): State<DashboardHandle>) -> Json<DashboardState> {
    Json(dashboard.snapshot())
}

#[derive(Debug, Deserialize)]
struct DatabaseExportQuery {
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct DatabaseArtifact {
    name: String,
    path: String,
}

#[derive(Debug, Serialize)]
struct DatabaseTableCount {
    table: String,
    rows: u64,
}

#[derive(Debug, Serialize)]
struct DatabaseStatusResponse {
    ok: bool,
    storage_backend: String,
    database_url_configured: bool,
    downloadable: Vec<String>,
    table_counts: Vec<DatabaseTableCount>,
    recent_execution_outcomes: Vec<ExecutionOutcomeSnapshot>,
    recent_treasury: Vec<TreasurySnapshot>,
    toxicity_profiles: Vec<ToxicitySnapshot>,
}

#[derive(Debug, Serialize)]
struct DatabaseExportResponse {
    ok: bool,
    message: String,
    artifacts: Vec<DatabaseArtifact>,
}

async fn database_status(State(dashboard): State<DashboardHandle>) -> Json<DatabaseStatusResponse> {
    let table_counts = dashboard
        .storage
        .database_table_counts()
        .unwrap_or_default()
        .into_iter()
        .map(|(table, rows)| DatabaseTableCount { table, rows })
        .collect();
    let recent_execution_outcomes = dashboard.storage.execution_outcomes(12).unwrap_or_default();
    let recent_treasury = dashboard
        .storage
        .treasury_rebalance_trail(12)
        .unwrap_or_default();
    let toxicity_profiles = dashboard.storage.toxicity_profiles(12).unwrap_or_default();

    Json(DatabaseStatusResponse {
        ok: true,
        storage_backend: dashboard.storage.backend_label().to_string(),
        database_url_configured: std::env::var("DATABASE_URL")
            .ok()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false),
        downloadable: vec![
            "toxicity_profiles.csv".to_string(),
            "realized_vs_expected.json".to_string(),
        ],
        table_counts,
        recent_execution_outcomes,
        recent_treasury,
        toxicity_profiles,
    })
}

async fn database_export(
    State(dashboard): State<DashboardHandle>,
    Query(query): Query<DatabaseExportQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(512).clamp(1, 10_000);
    match dashboard.storage.export_evidence_artifacts(limit) {
        Ok(paths) => (
            StatusCode::OK,
            Json(DatabaseExportResponse {
                ok: true,
                message: format!("exported {} database artifacts", paths.len()),
                artifacts: paths
                    .into_iter()
                    .map(|path| DatabaseArtifact {
                        name: path
                            .file_name()
                            .and_then(|value| value.to_str())
                            .unwrap_or("artifact")
                            .to_string(),
                        path: path.display().to_string(),
                    })
                    .collect(),
            }),
        ),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(DatabaseExportResponse {
                ok: false,
                message: err.to_string(),
                artifacts: Vec::new(),
            }),
        ),
    }
}

async fn database_download(
    State(dashboard): State<DashboardHandle>,
    Path(artifact): Path<String>,
    Query(query): Query<DatabaseExportQuery>,
) -> Response {
    let limit = query.limit.unwrap_or(512).clamp(1, 10_000);
    let export = match artifact.as_str() {
        "toxicity_profiles.csv" => dashboard.storage.export_toxicity_profiles_csv(limit),
        "realized_vs_expected.json" => dashboard.storage.export_realized_vs_expected_json(limit),
        _ => {
            return (
                StatusCode::NOT_FOUND,
                [(CONTENT_TYPE, "text/plain; charset=utf-8")],
                "unknown database artifact".to_string(),
            )
                .into_response()
        }
    };

    let path = match export {
        Ok(path) => path,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(CONTENT_TYPE, "text/plain; charset=utf-8")],
                err.to_string(),
            )
                .into_response()
        }
    };
    let body = match fs::read_to_string(&path) {
        Ok(body) => body,
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(CONTENT_TYPE, "text/plain; charset=utf-8")],
                err.to_string(),
            )
                .into_response()
        }
    };
    let content_type = if artifact.ends_with(".csv") {
        "text/csv; charset=utf-8"
    } else {
        "application/json; charset=utf-8"
    };
    let mut response = (StatusCode::OK, body).into_response();
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    if let Ok(value) = HeaderValue::from_str(&format!("attachment; filename=\"{}\"", artifact)) {
        response.headers_mut().insert(CONTENT_DISPOSITION, value);
    }
    response
}

async fn set_rpc_enabled(
    State(dashboard): State<DashboardHandle>,
    Path(endpoint_id): Path<usize>,
    Json(payload): Json<RpcToggleRequest>,
) -> impl IntoResponse {
    match dashboard
        .rpc_fleet
        .set_endpoint_enabled(endpoint_id, payload.enabled, payload.reason)
    {
        Ok(()) => {
            let status = if payload.enabled {
                "enabled"
            } else {
                "disabled"
            };
            dashboard.event(
                "warn",
                format!("rpc endpoint {endpoint_id} {status} from dashboard"),
            );
            (
                StatusCode::OK,
                Json(RpcControlResponse {
                    ok: true,
                    message: format!("rpc endpoint {endpoint_id} {status}"),
                    rpc_endpoints: dashboard.rpc_fleet.snapshot(),
                }),
            )
        }
        Err(message) => (
            StatusCode::NOT_FOUND,
            Json(RpcControlResponse {
                ok: false,
                message,
                rpc_endpoints: dashboard.rpc_fleet.snapshot(),
            }),
        ),
    }
}

async fn only_getblock(State(dashboard): State<DashboardHandle>) -> Json<RpcControlResponse> {
    let disabled = dashboard.rpc_fleet.keep_only_getblock_enabled();
    dashboard.event(
        "warn",
        format!("getblock-only mode enabled from dashboard; disabled {disabled} rpc endpoints"),
    );
    Json(RpcControlResponse {
        ok: true,
        message: format!("getblock-only mode enabled; disabled {disabled} rpc endpoints"),
        rpc_endpoints: dashboard.rpc_fleet.snapshot(),
    })
}

async fn clear_events(State(dashboard): State<DashboardHandle>) -> impl IntoResponse {
    match dashboard.clear_events() {
        Ok(()) => (
            StatusCode::OK,
            Json(EventsControlResponse {
                ok: true,
                message: "events feed cleared".to_string(),
            }),
        ),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(EventsControlResponse {
                ok: false,
                message: err.to_string(),
            }),
        ),
    }
}

async fn set_runtime_paused(
    State(dashboard): State<DashboardHandle>,
    Json(request): Json<RuntimePauseRequest>,
) -> Json<RuntimePauseResponse> {
    dashboard.set_runtime_paused(request.paused);
    let action = if request.paused { "paused" } else { "resumed" };
    dashboard.event("warn", format!("runtime {action} from dashboard"));
    Json(RuntimePauseResponse {
        ok: true,
        paused: request.paused,
        message: format!("runtime {action}"),
    })
}

async fn set_opportunity_mode(
    State(dashboard): State<DashboardHandle>,
    Path(mode): Path<String>,
) -> impl IntoResponse {
    match parse_opportunity_mode(&mode) {
        Ok(parsed) => {
            if let Ok(mut guard) = dashboard.opportunity_mode.write() {
                *guard = parsed;
            }
            {
                let mut state = dashboard.inner.write().expect("dashboard state lock");
                state.opportunity_mode = parsed.as_str().to_string();
            }
            dashboard.event(
                "warn",
                format!("opportunity mode switched to {}", parsed.as_str()),
            );
            (
                StatusCode::OK,
                Json(OpportunityModeResponse {
                    ok: true,
                    mode: parsed.as_str().to_string(),
                    message: format!("opportunity mode set to {}", parsed.as_str()),
                }),
            )
        }
        Err(err) => (
            StatusCode::BAD_REQUEST,
            Json(OpportunityModeResponse {
                ok: false,
                mode: dashboard
                    .opportunity_mode
                    .read()
                    .map(|mode| mode.as_str().to_string())
                    .unwrap_or_else(|_| "conservative".to_string()),
                message: err.to_string(),
            }),
        ),
    }
}

async fn set_opportunity_thresholds(
    State(dashboard): State<DashboardHandle>,
    Json(request): Json<OpportunityThresholdRequest>,
) -> impl IntoResponse {
    let current = dashboard
        .runtime_thresholds
        .read()
        .map(|thresholds| *thresholds)
        .unwrap_or(OpportunityThresholds {
            min_large_swap_eth: 25.0,
            min_net_profit_eth: 0.0025,
            min_profit_usd: 2.0,
            min_liquidity_eth: 25.0,
        });
    let next = OpportunityThresholds {
        min_large_swap_eth: request
            .min_large_swap_eth
            .unwrap_or(current.min_large_swap_eth),
        min_net_profit_eth: request
            .min_net_profit_eth
            .unwrap_or(current.min_net_profit_eth),
        min_profit_usd: request.min_profit_usd.unwrap_or(current.min_profit_usd),
        min_liquidity_eth: request
            .min_liquidity_eth
            .unwrap_or(current.min_liquidity_eth),
    };

    if next.min_large_swap_eth <= 0.0
        || next.min_net_profit_eth <= 0.0
        || next.min_profit_usd <= 0.0
        || next.min_liquidity_eth <= 0.0
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(OpportunityThresholdResponse {
                ok: false,
                message: "threshold values must be positive".to_string(),
                thresholds: current,
            }),
        );
    }

    if let Ok(mut guard) = dashboard.runtime_thresholds.write() {
        *guard = next;
    }
    {
        let mut state = dashboard.inner.write().expect("dashboard state lock");
        state.min_candidate_eth = next.min_large_swap_eth.to_string();
        state.min_net_profit_eth = next.min_net_profit_eth.to_string();
        state.min_profit_usd = next.min_profit_usd.to_string();
        state.min_liquidity_eth = next.min_liquidity_eth.to_string();
    }
    dashboard.event(
        "warn",
        format!(
            "opportunity thresholds updated min_swap={:.6} min_profit={:.8} min_usd={:.4} min_liquidity={:.6}",
            next.min_large_swap_eth,
            next.min_net_profit_eth,
            next.min_profit_usd,
            next.min_liquidity_eth
        ),
    );

    (
        StatusCode::OK,
        Json(OpportunityThresholdResponse {
            ok: true,
            message: "opportunity thresholds updated".to_string(),
            thresholds: next,
        }),
    )
}

fn push_event(queue: &mut VecDeque<DashboardEvent>, level: &str, message: String) {
    queue.push_front(DashboardEvent {
        at: Utc::now().to_rfc3339(),
        level: level.to_string(),
        message,
    });
    while queue.len() > 50 {
        queue.pop_back();
    }
}

fn build_latency_metrics(summary: HashMap<String, (u64, u128, u128, u128)>) -> Vec<LatencyMetric> {
    let mut metrics = Vec::new();
    for stage in [
        "block_fetch",
        "scan_cycle",
        "enqueue_latency",
        "queue_wait",
        "tx_prepare",
        "bundle_attempt",
    ] {
        let metric = summary
            .get(stage)
            .copied()
            .map(|(samples, last_ms, avg_ms, max_ms)| LatencyMetric {
                stage: stage.to_string(),
                samples,
                last_ms: Some(last_ms),
                avg_ms: Some(avg_ms),
                max_ms: Some(max_ms),
            });
        metrics.push(metric.unwrap_or(LatencyMetric {
            stage: stage.to_string(),
            samples: 0,
            last_ms: None,
            avg_ms: None,
            max_ms: None,
        }));
    }
    metrics
}

fn build_latency_risk(
    summary: HashMap<String, (u64, u128, u128, u128)>,
    rpc_endpoints: &[RpcEndpointSnapshot],
) -> LatencyRiskSnapshot {
    let mut sample_count = 0u64;
    let mut hot_weighted_sum = 0u128;
    let mut hot_samples = 0u64;
    let mut hot_max_ms = 0u128;
    let mut monitor_weighted_sum = 0u128;
    let mut monitor_samples = 0u64;
    let mut bottleneck_stage = "none".to_string();
    let mut bottleneck_ms = 0u128;

    for (stage, (samples, _last_ms, avg_ms, max_ms)) in summary {
        sample_count = sample_count.saturating_add(samples);
        if max_ms > bottleneck_ms {
            bottleneck_ms = max_ms;
            bottleneck_stage = stage.clone();
        }

        if is_hot_path_latency_stage(&stage) {
            hot_weighted_sum =
                hot_weighted_sum.saturating_add(avg_ms.saturating_mul(u128::from(samples)));
            hot_samples = hot_samples.saturating_add(samples);
            hot_max_ms = hot_max_ms.max(max_ms);
        } else {
            monitor_weighted_sum =
                monitor_weighted_sum.saturating_add(avg_ms.saturating_mul(u128::from(samples)));
            monitor_samples = monitor_samples.saturating_add(samples);
        }
    }

    let rpc_latencies: Vec<u128> = rpc_endpoints
        .iter()
        .filter(|endpoint| !endpoint.disabled)
        .filter_map(|endpoint| endpoint.avg_latency_ms)
        .collect();
    let rpc_avg_ms = if rpc_latencies.is_empty() {
        0
    } else {
        rpc_latencies.iter().sum::<u128>() / rpc_latencies.len() as u128
    };
    let hot_path_avg_ms = if hot_samples == 0 {
        0
    } else {
        hot_weighted_sum / u128::from(hot_samples)
    };
    let monitor_avg_ms = if monitor_samples == 0 {
        0
    } else {
        monitor_weighted_sum / u128::from(monitor_samples)
    };

    let hot_pressure = pressure(hot_path_avg_ms as f64, 250.0);
    let tail_pressure = pressure(hot_max_ms as f64, 750.0);
    let rpc_pressure = pressure(rpc_avg_ms as f64, 350.0);
    let monitor_pressure = pressure(monitor_avg_ms as f64, 700.0) * 0.5;
    let score = (hot_pressure * 0.40
        + tail_pressure * 0.25
        + rpc_pressure * 0.25
        + monitor_pressure * 0.10)
        .clamp(0.0, 1.0);
    let level = if score >= 0.75 {
        "BREACH"
    } else if score >= 0.45 {
        "WARN"
    } else {
        "OK"
    };

    LatencyRiskSnapshot {
        updated_at: Utc::now().to_rfc3339(),
        window_secs: 300,
        sample_count,
        hot_path_avg_ms,
        hot_path_max_ms: hot_max_ms,
        monitor_avg_ms,
        rpc_avg_ms,
        score,
        level: level.to_string(),
        bottleneck_stage,
    }
}

fn is_hot_path_latency_stage(stage: &str) -> bool {
    matches!(
        stage,
        "decode"
            | "fast_preflight"
            | "adaptive_preflight"
            | "payload_build"
            | "quote"
            | "fee_pending_lookup"
            | "tx_prepare"
            | "bundle_attempt"
    )
}

fn pressure(value: f64, budget: f64) -> f64 {
    if value <= 0.0 || budget <= 0.0 {
        0.0
    } else {
        ((value / budget) - 0.5).clamp(0.0, 1.5) / 1.5
    }
}

fn upsert_latency_metric(metrics: &mut Vec<LatencyMetric>, stage: &str, duration_ms: u128) {
    if let Some(metric) = metrics.iter_mut().find(|metric| metric.stage == stage) {
        metric.samples = metric.samples.saturating_add(1);
        metric.last_ms = Some(duration_ms);
        metric.avg_ms = Some(match metric.avg_ms {
            Some(previous) => {
                ((previous * (metric.samples as u128 - 1)) + duration_ms) / metric.samples as u128
            }
            None => duration_ms,
        });
        metric.max_ms = Some(metric.max_ms.unwrap_or(duration_ms).max(duration_ms));
        return;
    }

    metrics.push(LatencyMetric {
        stage: stage.to_string(),
        samples: 1,
        last_ms: Some(duration_ms),
        avg_ms: Some(duration_ms),
        max_ms: Some(duration_ms),
    });
}

const STATIC_INDEX_HTML: &str = INDEX_HTML;
const STATIC_STYLES_CSS: &str = "";
const STATIC_JS_APP: &str = "";
const STATIC_JS_DATA: &str = "";
const STATIC_JS_FX: &str = "";
const STATIC_JS_RADAR: &str = "";
const STATIC_FAVICON_SVG: &[u8] = br##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><rect width="32" height="32" rx="6" fill="#0e141b"/><path d="M18 3 8 18h7l-1 11 10-16h-7l1-10Z" fill="#42c58a"/></svg>"##;

#[allow(dead_code)]
const INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Fee Extraction Dashboard</title>
  <style>
    :root {
      --bg: #0e141b;
      --panel: #17212b;
      --panel-2: #1f2c39;
      --text: #edf3f8;
      --muted: #96a7b7;
      --line: #2a3b4c;
      --accent: #42c58a;
      --warn: #f0b24a;
      --danger: #ef6b73;
      --mono: "Consolas", "SFMono-Regular", monospace;
      --sans: "Segoe UI", "Helvetica Neue", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: var(--sans);
      background:
        radial-gradient(circle at top right, rgba(66,197,138,.12), transparent 28%),
        linear-gradient(180deg, #0b1117, var(--bg));
      color: var(--text);
    }
    .wrap {
      max-width: 1240px;
      margin: 0 auto;
      padding: 28px 20px 40px;
    }
    h1 { margin: 0 0 6px; font-size: 30px; }
    .sub { color: var(--muted); margin-bottom: 22px; }
    .grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
      margin-bottom: 14px;
    }
    .panel {
      background: linear-gradient(180deg, rgba(255,255,255,.02), transparent), var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 16px;
      box-shadow: 0 12px 30px rgba(0,0,0,.22);
    }
    .metric-label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .08em; }
    .metric-value { font-size: 28px; font-weight: 700; margin-top: 8px; }
    .layout {
      display: grid;
      grid-template-columns: 1.35fr 1fr;
      gap: 14px;
      margin-top: 14px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }
    th, td {
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }
    th { color: var(--muted); font-size: 12px; text-transform: uppercase; }
    .badge {
      display: inline-block;
      padding: 3px 8px;
      border-radius: 999px;
      background: var(--panel-2);
      border: 1px solid var(--line);
      font-size: 12px;
    }
    .events {
      display: grid;
      gap: 10px;
      max-height: 540px;
      overflow: auto;
    }
    .event {
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px;
      background: rgba(255,255,255,.015);
      font-family: var(--mono);
      font-size: 12px;
    }
    .event small { color: var(--muted); display: block; margin-bottom: 6px; }
    .success { color: var(--accent); }
    .error { color: var(--danger); }
    .warn { color: var(--warn); }
    .ok { color: var(--accent); }
    .muted { color: var(--muted); }
    .mono { font-family: var(--mono); }
    @media (max-width: 980px) {
      .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .layout { grid-template-columns: 1fr; }
    }
    @media (max-width: 640px) {
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Fee Extraction Engine</h1>
    <div class="sub" id="sub">Loading status...</div>

    <div class="grid">
      <div class="panel"><div class="metric-label">Network</div><div class="metric-value" id="network">-</div></div>
      <div class="panel"><div class="metric-label">Runtime</div><div class="metric-value" id="mode">-</div></div>
      <div class="panel"><div class="metric-label">Regime</div><div class="metric-value" id="regime">-</div></div>
      <div class="panel"><div class="metric-label">Hot Wallets</div><div class="metric-value" id="hot">-</div></div>
      <div class="panel"><div class="metric-label">Last Scan</div><div class="metric-value" id="scan">-</div></div>
    </div>

    <div class="grid">
      <div class="panel"><div class="metric-label">Total Keys</div><div class="metric-value" id="total-keys">-</div></div>
      <div class="panel"><div class="metric-label">Duplicates</div><div class="metric-value" id="duplicates">-</div></div>
      <div class="panel"><div class="metric-label">Invalid</div><div class="metric-value" id="invalid">-</div></div>
      <div class="panel"><div class="metric-label">Controller</div><div class="metric-value mono" style="font-size:14px;word-break:break-all" id="contract">-</div></div>
    </div>

    <div class="grid">
      <div class="panel"><div class="metric-label">Unique Wallets</div><div class="metric-value" id="wallets">-</div></div>
      <div class="panel"><div class="metric-label">Sweeps Attempted</div><div class="metric-value" id="attempted">-</div></div>
      <div class="panel"><div class="metric-label">Sweeps Succeeded</div><div class="metric-value" id="success">-</div></div>
      <div class="panel"><div class="metric-label">Sweeps Failed</div><div class="metric-value" id="failed">-</div></div>
    </div>

    <div class="grid">
      <div class="panel"><div class="metric-label">Allow Send</div><div class="metric-value" id="allow-send">-</div></div>
      <div class="panel"><div class="metric-label">Executor Buffer</div><div class="metric-value" id="executor-buffer">-</div></div>
      <div class="panel"><div class="metric-label">Treasury Action</div><div class="metric-value" id="treasury-action">-</div></div>
      <div class="panel"><div class="metric-label">Queue Wait Avg</div><div class="metric-value" id="queue-wait-avg">-</div></div>
      <div class="panel"><div class="metric-label">TX Prepare Avg</div><div class="metric-value" id="prepare-avg">-</div></div>
      <div class="panel"><div class="metric-label">Bundle Attempt Avg</div><div class="metric-value" id="bundle-avg">-</div></div>
    </div>

    <div class="layout">
      <div class="panel">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
          <strong>RPC Fleet</strong>
          <span class="badge" id="min-balance">-</span>
        </div>
        <table>
          <thead>
            <tr><th>Name</th><th>Kind</th><th>Health</th><th>Latency</th><th>Block</th><th>429</th><th>Timeout</th><th>Stale</th><th>Cooldown</th></tr>
          </thead>
          <tbody id="rpc-body"></tbody>
        </table>
      </div>

      <div class="panel">
        <strong>Recent Events</strong>
        <div class="events" id="events" style="margin-top:12px"></div>
      </div>
    </div>

    <div class="layout">
      <div class="panel">
        <strong>Recent Hot Paths</strong>
        <table style="margin-top:12px">
          <thead>
            <tr><th>Wallet</th><th>Balance</th><th>RPC</th></tr>
          </thead>
          <tbody id="wallet-body"></tbody>
        </table>
      </div>
      <div class="panel">
        <strong>Monitor Settings</strong>
        <table style="margin-top:12px">
          <tbody>
            <tr><td>Scan interval</td><td id="interval">-</td></tr>
            <tr><td>Last scan at</td><td id="last-scan-at">-</td></tr>
            <tr><td>Scan input</td><td class="mono">keys.txt</td></tr>
            <tr><td>Dashboard refresh</td><td>2s</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="layout">
      <div class="panel">
        <strong>Wallet Roles</strong>
        <table style="margin-top:12px">
          <tbody>
            <tr><td>Vault wallet</td><td class="mono" id="vault-address">-</td></tr>
            <tr><td>Executor wallet</td><td class="mono" id="executor-address">-</td></tr>
            <tr><td>Profit wallet</td><td class="mono" id="profit-address">-</td></tr>
            <tr><td>Controller</td><td class="mono" id="controller-address">-</td></tr>
          </tbody>
        </table>
      </div>
      <div class="panel">
        <strong>Hot Wallet Buffer</strong>
        <table style="margin-top:12px">
          <tbody>
            <tr><td>Current balance</td><td id="executor-balance">-</td></tr>
            <tr><td>Status</td><td id="executor-status">-</td></tr>
            <tr><td>Min buffer</td><td id="executor-min-buffer">-</td></tr>
            <tr><td>Target buffer</td><td id="executor-target-buffer">-</td></tr>
            <tr><td>Max buffer</td><td id="executor-max-buffer">-</td></tr>
            <tr><td>Treasury action</td><td id="executor-treasury-action">-</td></tr>
            <tr><td>Recommended amount</td><td id="executor-treasury-amount">-</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="layout">
      <div class="panel">
        <strong>Execution Recurrence</strong>
        <table style="margin-top:12px">
          <thead>
            <tr><th>Wallet</th><th>Class</th><th>Detect</th><th>Success</th><th>Detected Profit</th><th>Realized</th><th>Score</th></tr>
          </thead>
          <tbody id="residual-body"></tbody>
        </table>
      </div>
      <div class="panel">
        <strong>Execution Policy</strong>
        <table style="margin-top:12px">
          <tbody>
            <tr><td>Min candidate size</td><td id="policy-min-balance">-</td></tr>
            <tr><td>Min net profit</td><td id="policy-min-profit">-</td></tr>
            <tr><td>Relay mode</td><td id="policy-fallback">-</td></tr>
            <tr><td>Market regime</td><td id="policy-regime">-</td></tr>
            <tr><td>Kernel</td><td>mempool -> impact -> ev -> execute</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="layout">
      <div class="panel">
        <strong>Latency Pipeline</strong>
        <table style="margin-top:12px">
          <thead>
            <tr><th>Stage</th><th>Samples</th><th>Last</th><th>Avg</th><th>Max</th></tr>
          </thead>
          <tbody id="latency-body"></tbody>
        </table>
      </div>
      <div class="panel">
        <strong>Operational Readiness</strong>
        <table style="margin-top:12px">
          <tbody>
            <tr><td>Current mode</td><td id="readiness-mode">-</td></tr>
            <tr><td>Current regime</td><td id="readiness-regime">-</td></tr>
            <tr><td>Send path</td><td id="readiness-send">-</td></tr>
            <tr><td>RPC endpoints</td><td id="readiness-rpc">-</td></tr>
            <tr><td>Wallets loaded</td><td id="readiness-wallets">-</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="layout">
      <div class="panel">
        <strong>Treasury Control</strong>
        <table style="margin-top:12px">
          <tbody>
            <tr><td>Current action</td><td id="treasury-current-action">-</td></tr>
            <tr><td>Current status</td><td id="treasury-current-status">-</td></tr>
            <tr><td>Recommended amount</td><td id="treasury-current-amount">-</td></tr>
            <tr><td>Note</td><td id="treasury-current-note">-</td></tr>
          </tbody>
        </table>
      </div>
      <div class="panel">
        <strong>Treasury Trail</strong>
        <table style="margin-top:12px">
          <thead>
            <tr><th>At</th><th>Action</th><th>Status</th><th>Amount</th><th>Balance</th></tr>
          </thead>
          <tbody id="treasury-body"></tbody>
        </table>
      </div>
    </div>

    <div class="layout">
      <div class="panel">
        <strong>Relay Ranking</strong>
        <table style="margin-top:12px">
          <thead>
            <tr><th>Relay</th><th>Score</th><th>Pressure</th><th>Accept</th><th>Include</th><th>Accepted</th><th>Reject</th><th>Win</th><th>Revert</th><th>Miss</th></tr>
          </thead>
          <tbody id="relay-body"></tbody>
        </table>
      </div>
      <div class="panel">
        <strong>Reject Reasons</strong>
        <table style="margin-top:12px">
          <thead>
            <tr><th>Stage</th><th>Reason</th><th>Count</th></tr>
          </thead>
          <tbody id="reject-body"></tbody>
        </table>
      </div>
      <div class="panel">
        <strong>Adaptive Notes</strong>
        <table style="margin-top:12px">
          <tbody>
            <tr><td>Purpose</td><td>regime + reject calibration</td></tr>
            <tr><td>Preflight</td><td>cheap discard before payload</td></tr>
            <tr><td>Adaptive gate</td><td>EV net + competition + risk</td></tr>
            <tr><td>Signal source</td><td>runtime local flow observations</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="layout">
      <div class="panel">
        <strong>Context Toxicity</strong>
        <table style="margin-top:12px">
          <thead>
            <tr><th>Hour</th><th>Router</th><th>Pair</th><th>Samples</th><th>Success</th><th>Miss</th><th>Revert</th><th>Capture</th><th>Toxicity</th></tr>
          </thead>
          <tbody id="toxicity-body"></tbody>
        </table>
      </div>
    </div>
  </div>

  <script>
    async function refresh() {
      const res = await fetch('/api/status', { cache: 'no-store' });
      const data = await res.json();
      const metricByStage = Object.fromEntries(data.latency_metrics.map(item => [item.stage, item]));

      document.getElementById('sub').textContent = `Fee extraction runtime for ${data.network} | ${data.rpc_endpoints.length} RPC endpoints`;
      document.getElementById('network').textContent = data.network;
      document.getElementById('mode').textContent = data.runtime_mode;
      document.getElementById('regime').textContent = data.market_regime;
      document.getElementById('wallets').textContent = data.wallet_count;
      document.getElementById('hot').textContent = data.hot_wallets.length;
      document.getElementById('scan').textContent = data.last_scan_duration_ms ? `${data.last_scan_duration_ms} ms` : '-';
      document.getElementById('allow-send').textContent = data.allow_send ? 'enabled' : 'blocked';
      document.getElementById('executor-buffer').textContent = data.executor_buffer_status;
      document.getElementById('treasury-action').textContent = data.treasury_action;
      document.getElementById('queue-wait-avg').textContent = metricByStage.queue_wait?.avg_ms ? `${metricByStage.queue_wait.avg_ms} ms` : '-';
      document.getElementById('prepare-avg').textContent = metricByStage.tx_prepare?.avg_ms ? `${metricByStage.tx_prepare.avg_ms} ms` : '-';
      document.getElementById('bundle-avg').textContent = metricByStage.bundle_attempt?.avg_ms ? `${metricByStage.bundle_attempt.avg_ms} ms` : '-';
      document.getElementById('total-keys').textContent = data.total_keys_read;
      document.getElementById('duplicates').textContent = data.duplicate_keys;
      document.getElementById('invalid').textContent = data.invalid_keys;
      document.getElementById('attempted').textContent = data.sweeps_attempted;
      document.getElementById('success').textContent = data.sweeps_succeeded;
      document.getElementById('failed').textContent = data.sweeps_failed;
      document.getElementById('contract').textContent = data.control_address;
      document.getElementById('vault-address').textContent = data.vault_address;
      document.getElementById('executor-address').textContent = data.executor_address;
      document.getElementById('profit-address').textContent = data.profit_address;
      document.getElementById('controller-address').textContent = data.control_address;
      const asset = data.native_asset_symbol || 'NATIVE';
      document.getElementById('min-balance').textContent = `Min candidate ${data.min_candidate_eth} ${asset}`;
      document.getElementById('policy-min-balance').textContent = `${data.min_candidate_eth} ${asset}`;
      document.getElementById('policy-min-profit').textContent = `${data.min_net_profit_eth} ${asset}`;
      document.getElementById('executor-balance').textContent = data.executor_balance_eth ? `${data.executor_balance_eth} ${asset}` : '-';
      document.getElementById('executor-status').textContent = data.executor_buffer_status;
      document.getElementById('executor-min-buffer').textContent = `${data.executor_min_buffer_eth} ${asset}`;
      document.getElementById('executor-target-buffer').textContent = `${data.executor_target_buffer_eth} ${asset}`;
      document.getElementById('executor-max-buffer').textContent = `${data.executor_max_buffer_eth} ${asset}`;
      document.getElementById('executor-treasury-action').textContent = data.treasury_action;
      document.getElementById('executor-treasury-amount').textContent = `${data.treasury_recommended_amount_eth} ${asset}`;
      document.getElementById('treasury-current-action').textContent = data.treasury_action;
      document.getElementById('treasury-current-status').textContent = data.treasury_status;
      document.getElementById('treasury-current-amount').textContent = `${data.treasury_recommended_amount_eth} ${asset}`;
      document.getElementById('treasury-current-note').textContent = data.treasury_note;
      document.getElementById('policy-fallback').textContent = data.private_relay_only ? 'private-only' : 'mixed';
      document.getElementById('policy-regime').textContent = data.market_regime;
      document.getElementById('interval').textContent = `${data.scan_interval_ms} ms`;
      document.getElementById('last-scan-at').textContent = data.last_scan_at || '-';
      document.getElementById('readiness-mode').textContent = data.runtime_mode;
      document.getElementById('readiness-regime').textContent = data.market_regime;
      document.getElementById('readiness-send').textContent = data.allow_send ? 'send enabled' : 'send blocked';
      document.getElementById('readiness-rpc').textContent = `${data.rpc_endpoints.length} endpoints`;
      document.getElementById('readiness-wallets').textContent = `${data.wallet_count} wallets`;

      document.getElementById('rpc-body').innerHTML = data.rpc_endpoints.map(item => {
        const health = item.cooldown_remaining_secs
          ? '<span class="badge error">cooldown</span>'
          : item.stale_failures > 0 || (item.block_age_secs && item.block_age_secs > 30)
          ? '<span class="badge warn">stale</span>'
          : '<span class="badge ok">healthy</span>';
        const block = item.last_block
          ? `${item.last_block}${item.block_age_secs ? ` <span class="muted">(${item.block_age_secs}s)</span>` : ''}`
          : '-';
        return `
        <tr>
          <td>${item.name}</td>
          <td><span class="badge">${item.kind}</span></td>
          <td>${health}</td>
          <td>${item.avg_latency_ms ? item.avg_latency_ms + ' ms' : '-'}</td>
          <td>${block}</td>
          <td>${item.rate_limit_failures}</td>
          <td>${item.timeout_failures}</td>
          <td>${item.stale_failures}</td>
          <td>${item.cooldown_remaining_secs ? item.cooldown_remaining_secs + ' s' : '-'}</td>
        </tr>
      `}).join('');

      document.getElementById('wallet-body').innerHTML = data.hot_wallets.length
        ? data.hot_wallets.map(item => `
          <tr>
            <td class="mono">${item.address}</td>
            <td>${item.balance_eth}</td>
            <td>${item.rpc}</td>
          </tr>
        `).join('')
        : '<tr><td colspan="3">No active paths yet</td></tr>';

      document.getElementById('residual-body').innerHTML = data.top_residual_wallets.length
        ? data.top_residual_wallets.map(item => `
          <tr>
            <td class="mono">${item.wallet}</td>
            <td>${item.asset_class}</td>
            <td>${item.detections}</td>
            <td>${item.successful_sweeps}</td>
            <td>${item.detected_profit_eth} ${asset}</td>
            <td>${item.realized_profit_eth} ${asset}</td>
            <td>${item.residual_score}</td>
          </tr>
        `).join('')
        : '<tr><td colspan="7">No execution recurrence data yet</td></tr>';

      document.getElementById('events').innerHTML = data.recent_events.length
        ? data.recent_events.map(item => `
          <div class="event">
            <small>${item.at}</small>
            <div class="${item.level === 'success' ? 'success' : item.level === 'error' ? 'error' : item.level === 'warn' ? 'warn' : ''}">${item.message}</div>
          </div>
        `).join('')
        : '<div class="event">No events yet</div>';

      document.getElementById('latency-body').innerHTML = data.latency_metrics.length
        ? data.latency_metrics.map(item => `
          <tr>
            <td>${item.stage}</td>
            <td>${item.samples}</td>
            <td>${item.last_ms ? item.last_ms + ' ms' : '-'}</td>
            <td>${item.avg_ms ? item.avg_ms + ' ms' : '-'}</td>
            <td>${item.max_ms ? item.max_ms + ' ms' : '-'}</td>
          </tr>
        `).join('')
        : '<tr><td colspan="5">No telemetry yet</td></tr>';

      document.getElementById('reject-body').innerHTML = data.reject_reasons.length
        ? data.reject_reasons.map(item => `
          <tr>
            <td>${item.stage}</td>
            <td>${item.reason}</td>
            <td>${item.count}</td>
          </tr>
        `).join('')
        : '<tr><td colspan="3">No reject data yet</td></tr>';

      document.getElementById('relay-body').innerHTML = data.relay_rankings.length
        ? data.relay_rankings.map(item => `
          <tr>
            <td class="mono">${item.relay}</td>
            <td>${item.score.toFixed(2)}</td>
            <td>${item.pressure.toFixed(2)}</td>
            <td>${item.accept_rate.toFixed(2)}</td>
            <td>${item.inclusion_rate.toFixed(2)}</td>
            <td>${item.accepted}</td>
            <td>${item.submit_failed}</td>
            <td>${item.included_success}</td>
            <td>${item.included_revert}</td>
            <td>${item.not_included_timeout}</td>
          </tr>
        `).join('')
        : '<tr><td colspan="10">No relay ranking data yet</td></tr>';

      document.getElementById('treasury-body').innerHTML = data.treasury_rebalance_trail.length
        ? data.treasury_rebalance_trail.map(item => `
          <tr>
            <td>${item.at}</td>
            <td>${item.action}</td>
            <td>${item.status}</td>
            <td>${item.recommended_amount_eth.toFixed(6)} ${asset}</td>
            <td>${item.balance_eth.toFixed(6)} ${asset}</td>
          </tr>
        `).join('')
        : '<tr><td colspan="5">No treasury signals yet</td></tr>';

      document.getElementById('toxicity-body').innerHTML = data.toxicity_profiles.length
        ? data.toxicity_profiles.map(item => `
          <tr>
            <td>${item.hour_utc}</td>
            <td class="mono">${item.router}</td>
            <td class="mono">${item.pair}</td>
            <td>${item.samples}</td>
            <td>${item.success_rate.toFixed(2)}</td>
            <td>${item.miss_rate.toFixed(2)}</td>
            <td>${item.revert_rate.toFixed(2)}</td>
            <td>${item.realized_capture.toFixed(2)}</td>
            <td>${item.toxicity_score.toFixed(2)}</td>
          </tr>
        `).join('')
        : '<tr><td colspan="9">No contextual toxicity data yet</td></tr>';
    }

    refresh().catch(console.error);
    setInterval(() => refresh().catch(console.error), 2000);
  </script>
</body>
</html>"#;

fn wei_str_to_eth(value: &str) -> f64 {
    value.parse::<f64>().unwrap_or(0.0) / 1e18
}

fn delta(current: u64, previous: u64) -> u64 {
    current.saturating_sub(previous)
}

fn pct(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        (part as f64 / total as f64) * 100.0
    }
}

fn upsert_relay_snapshot(relays: &mut Vec<RelaySnapshot>, update: RelaySnapshotUpdate<'_>) {
    if let Some(entry) = relays.iter_mut().find(|entry| entry.relay == update.relay) {
        entry.accepted = entry.accepted.saturating_add(u64::from(update.accepted));
        entry.submit_failed = entry
            .submit_failed
            .saturating_add(u64::from(update.submit_failed));
        entry.included_success = entry
            .included_success
            .saturating_add(u64::from(update.included_success));
        entry.included_revert = entry
            .included_revert
            .saturating_add(u64::from(update.included_revert));
        entry.not_included_timeout = entry
            .not_included_timeout
            .saturating_add(u64::from(update.not_included_timeout));
        if let Some(value) = update.submit_latency_ms {
            entry.submit_latency_ms = if entry.submit_latency_ms <= 0.0 {
                value
            } else {
                (entry.submit_latency_ms * 0.8) + (value * 0.2)
            };
        }
        if let Some(value) = update.finalization_latency_ms {
            entry.finalization_latency_ms = if entry.finalization_latency_ms <= 0.0 {
                value
            } else {
                (entry.finalization_latency_ms * 0.8) + (value * 0.2)
            };
        }
        if let Some(value) = update.score {
            entry.score = value;
        }
        if let Some(value) = update.pressure {
            entry.pressure = value;
        }
        if let Some(value) = update.accept_rate {
            entry.accept_rate = value;
        }
        if let Some(value) = update.inclusion_rate {
            entry.inclusion_rate = value;
        }
    } else {
        relays.push(RelaySnapshot {
            relay: update.relay.to_string(),
            score: update.score.unwrap_or(0.0),
            pressure: update.pressure.unwrap_or(0.0),
            accept_rate: update.accept_rate.unwrap_or(0.0),
            inclusion_rate: update.inclusion_rate.unwrap_or(0.0),
            accepted: u64::from(update.accepted),
            submit_failed: u64::from(update.submit_failed),
            included_success: u64::from(update.included_success),
            included_revert: u64::from(update.included_revert),
            not_included_timeout: u64::from(update.not_included_timeout),
            submit_latency_ms: update.submit_latency_ms.unwrap_or(0.0),
            finalization_latency_ms: update.finalization_latency_ms.unwrap_or(0.0),
        });
    }

    relays.sort_by(|left, right| left.score.total_cmp(&right.score));
    relays.truncate(12);
}
