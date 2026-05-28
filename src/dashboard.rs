use crate::config::{parse_opportunity_mode, Config, OpportunityMode, OpportunityThresholds};
use crate::rpc::{RpcEndpointSnapshot, RpcFleet};
use crate::storage::Storage;
use axum::extract::{Path, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};
use tower_http::cors::CorsLayer;

#[derive(Clone)]
pub struct DashboardHandle {
    inner: Arc<RwLock<DashboardState>>,
    storage: Storage,
    rpc_fleet: Arc<RpcFleet>,
    opportunity_mode: Arc<RwLock<OpportunityMode>>,
    runtime_thresholds: Arc<RwLock<OpportunityThresholds>>,
    pending: Arc<Mutex<PendingStorageWrites>>,
}

#[derive(Default)]
struct PendingStorageWrites {
    events: Vec<(String, String)>,
    latencies: Vec<PendingLatency>,
    relay_updates: Vec<PendingRelayUpdate>,
    treasury_updates: Vec<PendingTreasuryUpdate>,
    execution_outcomes: Vec<PendingExecutionOutcome>,
}

struct PendingLatency {
    stage: String,
    duration_ms: u128,
    wallet: Option<String>,
    note: Option<String>,
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

#[derive(Debug, Clone, Serialize)]
pub struct DashboardState {
    pub runtime_mode: String,
    pub market_regime: String,
    pub allow_send: bool,
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
                network: config.network.clone(),
                native_asset_symbol: config.native_asset_symbol().to_string(),
                control_address: format!("{:?}", config.control_address),
                vault_address: format!("{:?}", config.vault_address),
                executor_address: format!("{:?}", config.executor_address),
                profit_address: format!("{:?}", config.profit_address),
                min_candidate_eth: config.mev.runtime_thresholds().min_large_swap_eth.to_string(),
                min_net_profit_eth: config.mev.runtime_thresholds().min_net_profit_eth.to_string(),
                min_profit_usd: config.mev.runtime_thresholds().min_profit_usd.to_string(),
                min_liquidity_eth: config.mev.runtime_thresholds().min_liquidity_eth.to_string(),
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
            pending: Arc::new(Mutex::new(PendingStorageWrites::default())),
        }
    }

    pub fn snapshot(&self) -> DashboardState {
        let mut state = self.inner.read().expect("dashboard state lock").clone();
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
            self.storage.telemetry_window_summary(300).unwrap_or_default(),
            &state.rpc_endpoints,
        );
        state
    }

    pub fn event(&self, level: &str, message: impl Into<String>) {
        let message = message.into();
        if let Ok(mut pending) = self.pending.lock() {
            pending.events.push((level.to_string(), message.clone()));
        }
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
            pending.latencies.push(PendingLatency {
                stage: stage.to_string(),
                duration_ms,
                wallet: wallet.map(str::to_string),
                note: note.map(str::to_string),
            });
        }
        let mut state = self.inner.write().expect("dashboard state lock");
        upsert_latency_metric(&mut state.latency_metrics, stage, duration_ms);
    }

    pub fn set_market_regime(&self, regime: &str) {
        let mut state = self.inner.write().expect("dashboard state lock");
        state.market_regime = regime.to_string();
    }

    pub fn record_reject_reason(&self, stage: &str, reason: &str) {
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

        for (level, message) in pending.events {
            self.storage.log_event(&level, &message);
        }

        for latency in pending.latencies {
            self.storage.log_telemetry(
                &latency.stage,
                latency.duration_ms,
                latency.wallet.as_deref(),
                latency.note.as_deref(),
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
    }
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
        .route("/api/rpc/:id/enabled", post(set_rpc_enabled))
        .route("/api/rpc/only-getblock", post(only_getblock))
        .route("/api/events/clear", post(clear_events))
        .route("/api/opportunity-mode/:mode", post(set_opportunity_mode))
        .route("/api/opportunity-thresholds", post(set_opportunity_thresholds))
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

async fn set_rpc_enabled(
    State(dashboard): State<DashboardHandle>,
    Path(endpoint_id): Path<usize>,
    Json(payload): Json<RpcToggleRequest>,
) -> impl IntoResponse {
    match dashboard.rpc_fleet.set_endpoint_enabled(
        endpoint_id,
        payload.enabled,
        payload.reason,
    ) {
        Ok(()) => {
            let status = if payload.enabled { "enabled" } else { "disabled" };
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
