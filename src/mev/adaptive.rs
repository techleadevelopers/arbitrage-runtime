#![allow(dead_code)]

use crate::config::Config;
use crate::mev::opportunity::wei_to_eth_f64;
use ethers::types::{Address, U256};
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const FLOW_CAPACITY: usize = 2048;
const FLOW_HORIZON_SECS: u64 = 12;
const LOOKUP_TARGET_MS: f64 = 140.0;
const SUBMIT_TARGET_MS: f64 = 120.0;
const FINALIZE_TARGET_MS: f64 = 6_000.0;
const EWMA_FAST: f64 = 0.20;
const EWMA_SLOW: f64 = 0.08;

pub type SharedAdaptivePolicy = Arc<Mutex<AdaptivePolicy>>;

#[derive(Clone, Copy, Debug, Eq)]
pub struct ClusterKey {
    pub router: Address,
    pub token_in: Address,
    pub token_out: Address,
    pub selector: [u8; 4],
}

impl PartialEq for ClusterKey {
    fn eq(&self, other: &Self) -> bool {
        self.router == other.router
            && self.token_in == other.token_in
            && self.token_out == other.token_out
            && self.selector == other.selector
    }
}

impl Hash for ClusterKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.router.hash(state);
        self.token_in.hash(state);
        self.token_out.hash(state);
        self.selector.hash(state);
    }
}

#[derive(Clone, Debug)]
struct FlowObservation {
    key: ClusterKey,
    observed_at: Instant,
    gas_price_gwei: f64,
    notional_eth: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct AdaptiveQuoteInput {
    pub cluster: ClusterKey,
    pub pair: Address,
    pub hour_utc: u8,
    pub context_priority_score: f64,
    pub context_toxicity_score: f64,
    pub expected_profit_wei: U256,
    pub execution_cost_wei: U256,
    pub gas_price_wei: U256,
    pub lookup_latency_ms: f64,
    pub notional_eth: f64,
    pub price_impact_bps: u64,
    pub relay_pressure_override: Option<f64>,
}

#[derive(Clone, Copy, Debug)]
pub struct PreflightInput {
    pub cluster: ClusterKey,
    pub notional_eth: f64,
    pub gas_price_wei: U256,
    pub path_len: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct PreflightQuote {
    pub should_continue: bool,
    pub regime: MarketRegime,
    pub reject_reason: Option<&'static str>,
    pub preflight_score: f64,
    pub upper_bound_ev_usd: f64,
    pub estimated_gas_cost_usd: f64,
    pub mempool_density: f64,
    pub cluster_heat: f64,
    pub gas_pressure: f64,
    pub impact_hint: f64,
    pub size_score: f64,
}

#[derive(Clone, Debug)]
pub struct AdaptiveQuote {
    pub should_execute: bool,
    pub regime: MarketRegime,
    pub reject_reason: Option<&'static str>,
    pub selected_relay: Option<String>,
    pub ev_real_usd: f64,
    pub threshold_dynamic_usd: f64,
    pub p_positive: f64,
    pub competition_score: f64,
    pub risk_score: f64,
    pub competition_penalty_usd: f64,
    pub risk_penalty_usd: f64,
    pub path_penalty_usd: f64,
    pub mempool_density: f64,
    pub cluster_heat: f64,
    pub builder_pressure: f64,
    pub latency_penalty: f64,
    pub gas_pressure: f64,
    pub context_priority_score: f64,
    pub context_toxicity_score: f64,
}

#[derive(Clone, Debug)]
pub struct RelayQuote {
    pub relay: String,
    pub relay_pressure: f64,
    pub accept_rate: f64,
    pub inclusion_rate: f64,
    pub accepted_not_included_rate: f64,
    pub revert_rate: f64,
    pub submit_latency_ms: f64,
    pub finalization_latency_ms: f64,
    pub score: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarketRegime {
    Calm,
    Normal,
    Hot,
    Toxic,
}

#[derive(Clone, Debug)]
struct RelayStats {
    submit_latency_ewma_ms: f64,
    finalization_latency_ewma_ms: f64,
    relay_accept_rate_ewma: f64,
    relay_reject_rate_ewma: f64,
    inclusion_success_rate_ewma: f64,
    accepted_not_included_rate_ewma: f64,
    included_revert_rate_ewma: f64,
    realized_capture_ewma: f64,
}

#[derive(Clone, Debug)]
struct RelayContextStats {
    inclusion_success_rate_ewma: f64,
    accepted_not_included_rate_ewma: f64,
    included_revert_rate_ewma: f64,
    realized_capture_ewma: f64,
    samples: u64,
}

#[derive(Clone, Debug)]
struct HistoricalOutcomeStats {
    samples: u64,
    success_rate: f64,
    accepted_not_included_rate: f64,
    revert_rate: f64,
    realized_capture: f64,
}

#[derive(Clone, Copy, Debug)]
struct HistoricalCalibration {
    competition_mult: f64,
    risk_mult: f64,
    threshold_mult: f64,
    regime_shift: i8,
}

#[derive(Clone, Copy, Debug, Eq)]
struct HistoricalProfileKey {
    router: Address,
    pair: Address,
    hour_utc: u8,
}

#[derive(Clone, Copy, Debug)]
pub struct ContextSignal {
    pub priority_score: f64,
    pub toxicity_score: f64,
    pub samples: u64,
}

#[derive(Clone, Copy, Debug, Eq)]
struct RouterHourKey {
    router: Address,
    hour_utc: u8,
}

impl PartialEq for RouterHourKey {
    fn eq(&self, other: &Self) -> bool {
        self.router == other.router && self.hour_utc == other.hour_utc
    }
}

impl Hash for RouterHourKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.router.hash(state);
        self.hour_utc.hash(state);
    }
}

impl PartialEq for HistoricalProfileKey {
    fn eq(&self, other: &Self) -> bool {
        self.router == other.router && self.pair == other.pair && self.hour_utc == other.hour_utc
    }
}

impl Hash for HistoricalProfileKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.router.hash(state);
        self.pair.hash(state);
        self.hour_utc.hash(state);
    }
}

#[derive(Debug)]
pub struct AdaptivePolicy {
    eth_usd_price: f64,
    base_threshold_usd: f64,
    max_price_impact_bps: u64,
    min_large_swap_eth: f64,
    min_profit_eth: f64,
    preflight_gas_estimate: u64,
    recent_flows: VecDeque<FlowObservation>,
    gas_price_ewma_gwei: f64,
    lookup_latency_ewma_ms: f64,
    submit_latency_ewma_ms: f64,
    finalize_latency_ewma_ms: f64,
    relay_accept_rate_ewma: f64,
    relay_reject_rate_ewma: f64,
    inclusion_success_rate_ewma: f64,
    accepted_not_included_rate_ewma: f64,
    included_revert_rate_ewma: f64,
    success_rate_ewma: f64,
    failure_rate_ewma: f64,
    timeout_rate_ewma: f64,
    realized_capture_ewma: f64,
    relay_stats: HashMap<String, RelayStats>,
    relay_context_stats: HashMap<(String, ClusterKey), RelayContextStats>,
    historical_profiles: HashMap<HistoricalProfileKey, HistoricalOutcomeStats>,
    router_hour_profiles: HashMap<RouterHourKey, ContextSignal>,
    chain_competition_mult: f64,
    chain_risk_mult: f64,
    chain_threshold_mult: f64,
}

#[derive(Clone, Debug)]
pub struct HistoricalOutcomeProfile {
    pub hour_utc: u8,
    pub pair: Address,
    pub router: Address,
    pub samples: u64,
    pub success_rate: f64,
    pub accepted_not_included_rate: f64,
    pub revert_rate: f64,
    pub realized_capture: f64,
}

impl AdaptivePolicy {
    pub fn shared(config: &Config) -> SharedAdaptivePolicy {
        Arc::new(Mutex::new(Self::new(config)))
    }

    pub fn new(config: &Config) -> Self {
        let (
            gas_baseline_gwei,
            threshold_bias,
            success_bias,
            failure_bias,
            chain_competition_mult,
            chain_risk_mult,
            chain_threshold_mult,
        ) = chain_adaptive_preset(&config.network);
        Self {
            eth_usd_price: config.mev.eth_usd_price,
            base_threshold_usd: config
                .mev
                .effective_min_profit_usd()
                .max(config.mev.effective_min_net_profit_eth() * config.mev.eth_usd_price)
                * threshold_bias,
            max_price_impact_bps: config.mev.effective_max_price_impact_bps().max(1),
            min_large_swap_eth: config.mev.effective_min_large_swap_eth().max(0.000_1),
            min_profit_eth: config.mev.effective_min_net_profit_eth().max(0.000_001),
            preflight_gas_estimate: config
                .estimated_exec_gas
                .saturating_add(config.estimated_bundle_overhead_gas)
                .max(180_000),
            recent_flows: VecDeque::with_capacity(FLOW_CAPACITY),
            gas_price_ewma_gwei: gas_baseline_gwei,
            lookup_latency_ewma_ms: LOOKUP_TARGET_MS,
            submit_latency_ewma_ms: SUBMIT_TARGET_MS,
            finalize_latency_ewma_ms: FINALIZE_TARGET_MS,
            relay_accept_rate_ewma: 0.76,
            relay_reject_rate_ewma: 0.16,
            inclusion_success_rate_ewma: 0.58,
            accepted_not_included_rate_ewma: 0.14,
            included_revert_rate_ewma: 0.08,
            success_rate_ewma: success_bias,
            failure_rate_ewma: failure_bias,
            timeout_rate_ewma: 0.08,
            realized_capture_ewma: 0.82,
            relay_stats: HashMap::new(),
            relay_context_stats: HashMap::new(),
            historical_profiles: HashMap::new(),
            router_hour_profiles: HashMap::new(),
            chain_competition_mult,
            chain_risk_mult,
            chain_threshold_mult,
        }
    }

    pub fn apply_historical_profiles(&mut self, profiles: Vec<HistoricalOutcomeProfile>) {
        self.historical_profiles.clear();
        self.router_hour_profiles.clear();
        let mut router_hour_rollup: HashMap<RouterHourKey, Vec<HistoricalOutcomeStats>> =
            HashMap::new();
        for profile in profiles {
            let stats = HistoricalOutcomeStats {
                samples: profile.samples,
                success_rate: profile.success_rate,
                accepted_not_included_rate: profile.accepted_not_included_rate,
                revert_rate: profile.revert_rate,
                realized_capture: profile.realized_capture,
            };
            self.historical_profiles.insert(
                HistoricalProfileKey {
                    router: profile.router,
                    pair: profile.pair,
                    hour_utc: profile.hour_utc,
                },
                stats.clone(),
            );
            router_hour_rollup
                .entry(RouterHourKey {
                    router: profile.router,
                    hour_utc: profile.hour_utc,
                })
                .or_default()
                .push(stats);
        }
        for (key, values) in router_hour_rollup {
            let samples = values.iter().map(|value| value.samples).sum::<u64>().max(1);
            let weighted = |f: fn(&HistoricalOutcomeStats) -> f64| {
                values
                    .iter()
                    .map(|value| f(value) * value.samples as f64)
                    .sum::<f64>()
                    / samples as f64
            };
            let success_rate = weighted(|value| value.success_rate);
            let miss_rate = weighted(|value| value.accepted_not_included_rate);
            let revert_rate = weighted(|value| value.revert_rate);
            let realized_capture = weighted(|value| value.realized_capture);
            let priority_score = (success_rate * 0.46
                + realized_capture.clamp(0.0, 1.0) * 0.34
                + (1.0 - miss_rate).clamp(0.0, 1.0) * 0.12
                + (1.0 - revert_rate).clamp(0.0, 1.0) * 0.08)
                .clamp(0.0, 1.0);
            let toxicity_score = ((1.0 - success_rate).clamp(0.0, 1.0) * 0.28
                + miss_rate.clamp(0.0, 1.0) * 0.36
                + revert_rate.clamp(0.0, 1.0) * 0.22
                + (1.0 - realized_capture).clamp(0.0, 1.0) * 0.14)
                .clamp(0.0, 1.0);
            self.router_hour_profiles.insert(
                key,
                ContextSignal {
                    priority_score,
                    toxicity_score,
                    samples,
                },
            );
        }
    }

    pub fn context_signal(&self, router: Address, hour_utc: u8) -> ContextSignal {
        self.router_hour_profiles
            .get(&RouterHourKey { router, hour_utc })
            .copied()
            .unwrap_or(ContextSignal {
                priority_score: 0.50,
                toxicity_score: 0.50,
                samples: 0,
            })
    }

    pub fn observe_lookup_latency(&mut self, latency_ms: f64) {
        self.lookup_latency_ewma_ms = ewma(self.lookup_latency_ewma_ms, latency_ms, EWMA_FAST);
    }

    pub fn observe_candidate_flow(
        &mut self,
        key: ClusterKey,
        notional_wei: U256,
        gas_price_wei: U256,
    ) {
        let now = Instant::now();
        self.prune(now);
        let gas_price_gwei = wei_to_gwei_f64(gas_price_wei);
        let notional_eth = wei_to_eth_f64(notional_wei);
        self.gas_price_ewma_gwei = if self.gas_price_ewma_gwei <= f64::EPSILON {
            gas_price_gwei.max(1e-9)
        } else {
            ewma(self.gas_price_ewma_gwei, gas_price_gwei, EWMA_SLOW)
        };
        if self.recent_flows.len() == FLOW_CAPACITY {
            self.recent_flows.pop_front();
        }
        self.recent_flows.push_back(FlowObservation {
            key,
            observed_at: now,
            gas_price_gwei,
            notional_eth,
        });
    }

    pub fn preflight_score(&mut self, input: PreflightInput) -> PreflightQuote {
        let now = Instant::now();
        self.prune(now);

        let mempool_density =
            (self.recent_flows.len() as f64 / FLOW_CAPACITY as f64).clamp(0.0, 1.0);
        let cluster_heat = self.cluster_heat(input.cluster, now);
        let gas_pressure = self.gas_pressure(wei_to_gwei_f64(input.gas_price_wei));
        let size_score = size_pressure(input.notional_eth);
        let impact_hint = self.impact_hint(input.notional_eth, input.path_len);
        let path_penalty = path_penalty(input.path_len);
        let latency_penalty = self.latency_penalty(self.lookup_latency_ewma_ms);
        let failure_pressure =
            (self.failure_rate_ewma * 0.65 + self.timeout_rate_ewma * 0.35).clamp(0.0, 1.0);
        let poor_realization = (1.0 - self.realized_capture_ewma).clamp(0.0, 1.0);
        let builder_pressure = self.builder_path_pressure();
        let regime = self.market_regime(
            mempool_density,
            cluster_heat,
            gas_pressure,
            latency_penalty,
            failure_pressure,
            poor_realization,
            builder_pressure,
        );
        let gas_floor_usd = wei_to_eth_f64(
            input
                .gas_price_wei
                .saturating_mul(U256::from(self.preflight_gas_estimate)),
        ) * self.eth_usd_price;
        let notional_usd = input.notional_eth.max(0.0) * self.eth_usd_price;
        let gross_edge_upper_usd = notional_usd
            * (0.00020 + impact_hint * 0.00115)
            * (1.0 - cluster_heat * 0.16)
            * (1.0 - path_penalty * 0.12);
        let gas_drag_usd = gas_floor_usd * (1.0 + gas_pressure * 0.85);
        let competition_drag_usd = gross_edge_upper_usd
            * (cluster_heat * 0.42 + mempool_density * 0.18 + gas_pressure * 0.12);
        let upper_bound_ev_usd = gross_edge_upper_usd - gas_drag_usd - competition_drag_usd;

        let preflight_score = (impact_hint * 0.36
            + size_score * 0.24
            + (1.0 - cluster_heat) * 0.20
            + (1.0 - mempool_density) * 0.10
            + (1.0 - gas_pressure) * 0.10
            - path_penalty * 0.10)
            .clamp(0.0, 1.0);

        let min_preflight_ev = self.base_threshold_usd * 0.55;
        let should_continue = input.notional_eth >= self.min_large_swap_eth
            && upper_bound_ev_usd > min_preflight_ev
            && preflight_score >= 0.28
            && impact_hint >= 0.18
            && cluster_heat <= 0.92
            && gas_pressure <= 0.96
            && builder_pressure <= 0.94;
        let reject_reason = if should_continue {
            None
        } else if input.notional_eth < self.min_large_swap_eth {
            Some("notional_below_min")
        } else if upper_bound_ev_usd <= min_preflight_ev {
            Some("ev_upper_bound_below_min")
        } else if preflight_score < 0.28 {
            Some("preflight_score_too_low")
        } else if impact_hint < 0.18 {
            Some("impact_hint_too_low")
        } else if cluster_heat > 0.92 {
            Some("cluster_saturated")
        } else if gas_pressure > 0.96 {
            Some("gas_pressure_too_high")
        } else if builder_pressure > 0.94 {
            Some("builder_path_toxic")
        } else {
            Some("preflight_reject")
        };

        PreflightQuote {
            should_continue,
            regime,
            reject_reason,
            preflight_score,
            upper_bound_ev_usd,
            estimated_gas_cost_usd: gas_drag_usd,
            mempool_density,
            cluster_heat,
            gas_pressure,
            impact_hint,
            size_score,
        }
    }

    pub fn quote(&mut self, input: AdaptiveQuoteInput) -> AdaptiveQuote {
        let now = Instant::now();
        self.prune(now);

        let expected_profit_usd = wei_to_eth_f64(input.expected_profit_wei) * self.eth_usd_price;
        let gas_cost_usd = wei_to_eth_f64(input.execution_cost_wei) * self.eth_usd_price;
        let mempool_density =
            (self.recent_flows.len() as f64 / FLOW_CAPACITY as f64).clamp(0.0, 1.0);
        let cluster_heat = self.cluster_heat(input.cluster, now);
        let gas_pressure = self.gas_pressure(wei_to_gwei_f64(input.gas_price_wei));
        let size_pressure = size_pressure(input.notional_eth);
        let impact_pressure =
            (input.price_impact_bps as f64 / self.max_price_impact_bps as f64).clamp(0.0, 1.0);
        let latency_penalty = self.latency_penalty(input.lookup_latency_ms);
        let failure_pressure =
            (self.failure_rate_ewma * 0.65 + self.timeout_rate_ewma * 0.35).clamp(0.0, 1.0);
        let poor_realization = (1.0 - self.realized_capture_ewma).clamp(0.0, 1.0);
        let historical = self.historical_profile(input.pair, input.cluster.router, input.hour_utc);
        let builder_pressure = input
            .relay_pressure_override
            .unwrap_or_else(|| self.builder_path_pressure_for_cluster(None, input.cluster));
        let historical_pressure = historical
            .as_ref()
            .map(|profile| Self::historical_pressure(profile))
            .unwrap_or(0.0);
        let historical_confidence = historical
            .as_ref()
            .map(|profile| (profile.samples.min(48) as f64 / 48.0).clamp(0.0, 1.0))
            .unwrap_or(0.0);
        let historical_calibration = historical
            .as_ref()
            .map(|profile| self.historical_calibration(profile, historical_confidence))
            .unwrap_or_default();
        let builder_pressure = (builder_pressure * (1.0 - historical_confidence * 0.45)
            + historical_pressure * historical_confidence * 0.55)
            .clamp(0.0, 1.0);
        let regime = self.recalibrated_regime(
            self.market_regime(
                mempool_density,
                cluster_heat,
                gas_pressure,
                latency_penalty,
                failure_pressure,
                poor_realization,
                builder_pressure,
            ),
            historical_calibration.regime_shift,
        );

        let context_priority_score = input.context_priority_score.clamp(0.0, 1.5);
        let context_toxicity_score = input.context_toxicity_score.clamp(0.0, 1.0);
        let competition_score = (self.competition_score(
            cluster_heat,
            mempool_density,
            gas_pressure,
            size_pressure,
            impact_pressure,
            builder_pressure,
            regime,
        ) * self.chain_competition_mult
            * (1.0 + (1.0 - context_priority_score.clamp(0.0, 1.0)) * 0.06)
            * (1.0 + context_toxicity_score * 0.20)
            * historical_calibration.competition_mult.max(1e-9))
        .clamp(0.0, 1.0);
        let risk_score = (self.risk_score(
            failure_pressure,
            latency_penalty,
            poor_realization,
            gas_pressure,
            impact_pressure,
            builder_pressure,
            regime,
        ) * self.chain_risk_mult
            * (1.0 + context_toxicity_score * 0.24)
            * (1.0 - context_priority_score.clamp(0.0, 1.0) * 0.08)
            * historical_calibration.risk_mult.max(1e-9))
        .clamp(0.0, 1.0);

        let p_positive = self.probability_positive(
            competition_score,
            risk_score,
            failure_pressure,
            latency_penalty,
            poor_realization,
            gas_pressure,
            builder_pressure,
            regime,
        );
        let p_positive = if let Some(profile) = historical.as_ref() {
            let target = (profile.success_rate * 0.62
                + (1.0 - profile.accepted_not_included_rate) * 0.18
                + (1.0 - profile.revert_rate) * 0.10
                + profile.realized_capture.clamp(0.0, 1.0) * 0.10)
                .clamp(0.04, 0.99);
            (p_positive * (1.0 - historical_confidence * 0.40)
                + target * historical_confidence * 0.40)
                .clamp(0.04, 0.99)
        } else {
            p_positive
        };

        let competition_penalty_usd = self.competition_penalty(
            expected_profit_usd,
            competition_score,
            cluster_heat,
            builder_pressure,
            regime,
        );
        let risk_penalty_usd = self.risk_penalty(
            expected_profit_usd,
            risk_score,
            failure_pressure,
            builder_pressure,
            regime,
        );
        let path_penalty_usd = self.path_execution_penalty_usd(
            expected_profit_usd,
            builder_pressure,
            failure_pressure,
            latency_penalty,
            gas_pressure,
            context_toxicity_score,
        );
        let ev_real_usd = p_positive * expected_profit_usd
            - gas_cost_usd
            - competition_penalty_usd
            - risk_penalty_usd
            - path_penalty_usd;

        let threshold_dynamic_usd = self.dynamic_threshold(
            mempool_density,
            failure_pressure,
            gas_pressure,
            latency_penalty,
            poor_realization,
            cluster_heat,
            builder_pressure,
            regime,
        ) * self.chain_threshold_mult
            * (1.0 + context_toxicity_score * 0.18)
            * (1.0 - context_priority_score.clamp(0.0, 1.0) * 0.08)
            * historical_calibration.threshold_mult.max(1e-9);
        let threshold_dynamic_usd = if let Some(profile) = historical.as_ref() {
            let threshold_penalty = (profile.accepted_not_included_rate * 0.34
                + profile.revert_rate * 0.26
                + (1.0 - profile.success_rate).clamp(0.0, 1.0) * 0.24
                + (1.0 - profile.realized_capture).clamp(0.0, 1.0) * 0.16)
                .clamp(0.0, 1.0);
            threshold_dynamic_usd * (1.0 + threshold_penalty * historical_confidence * 0.75)
        } else {
            threshold_dynamic_usd
        };

        let should_execute = ev_real_usd > threshold_dynamic_usd
            && p_positive >= self.min_probability_threshold(regime)
            && competition_score <= self.max_competition_threshold(regime)
            && risk_score <= self.max_risk_threshold(regime);
        let reject_reason = if should_execute {
            None
        } else if ev_real_usd <= threshold_dynamic_usd {
            Some("ev_real_below_threshold")
        } else if p_positive < self.min_probability_threshold(regime) {
            Some("probability_too_low")
        } else if competition_score > self.max_competition_threshold(regime) {
            Some("competition_too_high")
        } else if risk_score > self.max_risk_threshold(regime) {
            Some("risk_too_high")
        } else if builder_pressure > 0.76 {
            Some("builder_pressure_too_high")
        } else {
            Some("adaptive_reject")
        };

        AdaptiveQuote {
            should_execute,
            regime,
            reject_reason,
            selected_relay: None,
            ev_real_usd,
            threshold_dynamic_usd,
            p_positive,
            competition_score,
            risk_score,
            competition_penalty_usd,
            risk_penalty_usd,
            path_penalty_usd,
            mempool_density,
            cluster_heat,
            builder_pressure,
            latency_penalty,
            gas_pressure,
            context_priority_score,
            context_toxicity_score,
        }
    }

    pub fn quote_for_relays(
        &mut self,
        input: AdaptiveQuoteInput,
        relays: &[String],
    ) -> AdaptiveQuote {
        let ranked = self.rank_relays(relays);
        let mut best = self.quote(AdaptiveQuoteInput {
            relay_pressure_override: ranked.first().map(|relay| {
                self.builder_path_pressure_for_cluster(Some(&relay.relay), input.cluster)
            }),
            ..input
        });
        if let Some(relay) = ranked.first() {
            best.selected_relay = Some(relay.relay.clone());
        }

        for relay in ranked.iter().skip(1) {
            let mut candidate = self.quote(AdaptiveQuoteInput {
                relay_pressure_override: Some(
                    self.builder_path_pressure_for_cluster(Some(&relay.relay), input.cluster),
                ),
                ..input
            });
            candidate.selected_relay = Some(relay.relay.clone());
            if is_better_quote(&candidate, &best) {
                best = candidate;
            }
        }

        best
    }

    pub fn record_submit_success(&mut self, latency_ms: f64) {
        self.submit_latency_ewma_ms = ewma(self.submit_latency_ewma_ms, latency_ms, EWMA_FAST);
        self.relay_accept_rate_ewma = ewma(self.relay_accept_rate_ewma, 1.0, EWMA_FAST);
        self.relay_reject_rate_ewma = ewma(self.relay_reject_rate_ewma, 0.0, EWMA_FAST);
        self.accepted_not_included_rate_ewma =
            ewma(self.accepted_not_included_rate_ewma, 0.0, EWMA_FAST);
        self.included_revert_rate_ewma = ewma(self.included_revert_rate_ewma, 0.0, EWMA_FAST);
    }

    pub fn record_submit_success_for_relay(&mut self, relay: &str, latency_ms: f64) {
        let stats = self
            .relay_stats
            .entry(relay.to_string())
            .or_insert_with(RelayStats::default);
        stats.submit_latency_ewma_ms = ewma(stats.submit_latency_ewma_ms, latency_ms, EWMA_FAST);
        stats.relay_accept_rate_ewma = ewma(stats.relay_accept_rate_ewma, 1.0, EWMA_FAST);
        stats.relay_reject_rate_ewma = ewma(stats.relay_reject_rate_ewma, 0.0, EWMA_FAST);
        stats.accepted_not_included_rate_ewma =
            ewma(stats.accepted_not_included_rate_ewma, 0.0, EWMA_FAST);
        stats.included_revert_rate_ewma = ewma(stats.included_revert_rate_ewma, 0.0, EWMA_FAST);
        self.record_submit_success(latency_ms);
    }

    pub fn record_submit_failure(&mut self, latency_ms: f64) {
        self.submit_latency_ewma_ms = ewma(self.submit_latency_ewma_ms, latency_ms, EWMA_FAST);
        self.relay_accept_rate_ewma = ewma(self.relay_accept_rate_ewma, 0.0, EWMA_FAST);
        self.relay_reject_rate_ewma = ewma(self.relay_reject_rate_ewma, 1.0, EWMA_FAST);
        self.success_rate_ewma = ewma(self.success_rate_ewma, 0.0, EWMA_FAST);
        self.failure_rate_ewma = ewma(self.failure_rate_ewma, 1.0, EWMA_FAST);
    }

    pub fn record_submit_failure_for_relay(&mut self, relay: &str, latency_ms: f64) {
        let stats = self
            .relay_stats
            .entry(relay.to_string())
            .or_insert_with(RelayStats::default);
        stats.submit_latency_ewma_ms = ewma(stats.submit_latency_ewma_ms, latency_ms, EWMA_FAST);
        stats.relay_accept_rate_ewma = ewma(stats.relay_accept_rate_ewma, 0.0, EWMA_FAST);
        stats.relay_reject_rate_ewma = ewma(stats.relay_reject_rate_ewma, 1.0, EWMA_FAST);
        self.record_submit_failure(latency_ms);
    }

    pub fn record_finalization(
        &mut self,
        expected_profit_wei: U256,
        realized_profit_eth: f64,
        success: bool,
        finalization_latency_ms: f64,
    ) {
        self.finalize_latency_ewma_ms = ewma(
            self.finalize_latency_ewma_ms,
            finalization_latency_ms,
            EWMA_FAST,
        );
        self.timeout_rate_ewma = ewma(self.timeout_rate_ewma, 0.0, EWMA_FAST);
        self.success_rate_ewma = ewma(
            self.success_rate_ewma,
            if success { 1.0 } else { 0.0 },
            EWMA_FAST,
        );
        self.inclusion_success_rate_ewma = ewma(
            self.inclusion_success_rate_ewma,
            if success { 1.0 } else { 0.0 },
            EWMA_FAST,
        );
        self.accepted_not_included_rate_ewma =
            ewma(self.accepted_not_included_rate_ewma, 0.0, EWMA_FAST);
        self.included_revert_rate_ewma = ewma(
            self.included_revert_rate_ewma,
            if success { 0.0 } else { 1.0 },
            EWMA_FAST,
        );
        self.failure_rate_ewma = ewma(
            self.failure_rate_ewma,
            if success { 0.0 } else { 1.0 },
            EWMA_FAST,
        );
        self.realized_capture_ewma = ewma(
            self.realized_capture_ewma,
            capture_ratio(expected_profit_wei, realized_profit_eth),
            EWMA_SLOW,
        );
    }

    pub fn record_finalization_for_relay(
        &mut self,
        relay: &str,
        expected_profit_wei: U256,
        realized_profit_eth: f64,
        success: bool,
        finalization_latency_ms: f64,
    ) {
        let stats = self
            .relay_stats
            .entry(relay.to_string())
            .or_insert_with(RelayStats::default);
        stats.finalization_latency_ewma_ms = ewma(
            stats.finalization_latency_ewma_ms,
            finalization_latency_ms,
            EWMA_FAST,
        );
        stats.inclusion_success_rate_ewma = ewma(
            stats.inclusion_success_rate_ewma,
            if success { 1.0 } else { 0.0 },
            EWMA_FAST,
        );
        stats.accepted_not_included_rate_ewma =
            ewma(stats.accepted_not_included_rate_ewma, 0.0, EWMA_FAST);
        stats.included_revert_rate_ewma = ewma(
            stats.included_revert_rate_ewma,
            if success { 0.0 } else { 1.0 },
            EWMA_FAST,
        );
        stats.realized_capture_ewma = ewma(
            stats.realized_capture_ewma,
            capture_ratio(expected_profit_wei, realized_profit_eth),
            EWMA_SLOW,
        );
        self.record_finalization(
            expected_profit_wei,
            realized_profit_eth,
            success,
            finalization_latency_ms,
        );
    }

    pub fn record_contextual_outcome(
        &mut self,
        relay: &str,
        cluster: ClusterKey,
        expected_profit_wei: U256,
        realized_profit_eth: f64,
        outcome: ContextualOutcomeKind,
    ) {
        let stats = self
            .relay_context_stats
            .entry((relay.to_string(), cluster))
            .or_insert_with(RelayContextStats::default);
        let (success, miss, revert) = match outcome {
            ContextualOutcomeKind::IncludedSuccess => (1.0, 0.0, 0.0),
            ContextualOutcomeKind::AcceptedNotIncluded => (0.0, 1.0, 0.0),
            ContextualOutcomeKind::IncludedRevert => (0.0, 0.0, 1.0),
            ContextualOutcomeKind::SubmitFailed => (0.0, 0.0, 0.0),
        };
        stats.inclusion_success_rate_ewma =
            ewma(stats.inclusion_success_rate_ewma, success, EWMA_FAST);
        stats.accepted_not_included_rate_ewma =
            ewma(stats.accepted_not_included_rate_ewma, miss, EWMA_FAST);
        stats.included_revert_rate_ewma = ewma(stats.included_revert_rate_ewma, revert, EWMA_FAST);
        stats.realized_capture_ewma = ewma(
            stats.realized_capture_ewma,
            capture_ratio(expected_profit_wei, realized_profit_eth),
            EWMA_FAST,
        );
        stats.samples = stats.samples.saturating_add(1);
    }

    pub fn record_receipt_timeout(&mut self, finalization_latency_ms: f64) {
        self.finalize_latency_ewma_ms = ewma(
            self.finalize_latency_ewma_ms,
            finalization_latency_ms,
            EWMA_FAST,
        );
        self.timeout_rate_ewma = ewma(self.timeout_rate_ewma, 1.0, EWMA_FAST);
        self.inclusion_success_rate_ewma = ewma(self.inclusion_success_rate_ewma, 0.0, EWMA_FAST);
        self.accepted_not_included_rate_ewma =
            ewma(self.accepted_not_included_rate_ewma, 1.0, EWMA_FAST);
        self.included_revert_rate_ewma = ewma(self.included_revert_rate_ewma, 0.0, EWMA_FAST);
        self.success_rate_ewma = ewma(self.success_rate_ewma, 0.0, EWMA_FAST);
        self.failure_rate_ewma = ewma(self.failure_rate_ewma, 1.0, EWMA_FAST);
    }

    pub fn record_receipt_timeout_for_relay(&mut self, relay: &str, finalization_latency_ms: f64) {
        let stats = self
            .relay_stats
            .entry(relay.to_string())
            .or_insert_with(RelayStats::default);
        stats.finalization_latency_ewma_ms = ewma(
            stats.finalization_latency_ewma_ms,
            finalization_latency_ms,
            EWMA_FAST,
        );
        stats.inclusion_success_rate_ewma = ewma(stats.inclusion_success_rate_ewma, 0.0, EWMA_FAST);
        stats.accepted_not_included_rate_ewma =
            ewma(stats.accepted_not_included_rate_ewma, 1.0, EWMA_FAST);
        stats.included_revert_rate_ewma = ewma(stats.included_revert_rate_ewma, 0.0, EWMA_FAST);
        self.record_receipt_timeout(finalization_latency_ms);
    }

    pub fn rank_relays(&self, relays: &[String]) -> Vec<RelayQuote> {
        let mut quotes = relays
            .iter()
            .map(|relay| self.relay_quote(relay))
            .collect::<Vec<_>>();
        quotes.sort_by(|left, right| left.score.total_cmp(&right.score));
        quotes
    }

    pub fn rank_relays_for_cluster(
        &self,
        relays: &[String],
        cluster: ClusterKey,
    ) -> Vec<RelayQuote> {
        let mut quotes = relays
            .iter()
            .map(|relay| self.relay_quote_for_cluster(relay, cluster))
            .collect::<Vec<_>>();
        quotes.sort_by(|left, right| left.score.total_cmp(&right.score));
        quotes
    }

    fn prune(&mut self, now: Instant) {
        while matches!(
            self.recent_flows.front(),
            Some(flow) if now.duration_since(flow.observed_at) > Duration::from_secs(FLOW_HORIZON_SECS)
        ) {
            self.recent_flows.pop_front();
        }
    }

    fn cluster_heat(&self, key: ClusterKey, now: Instant) -> f64 {
        if self.recent_flows.is_empty() {
            return 0.0;
        }

        let mut total_weight = 0.0;
        let mut cluster_weight = 0.0;
        let mut total_recent = 0.0;
        let mut cluster_recent = 0.0;
        let mut aggressive_cluster = 0.0;
        let mut aggressive_total = 0.0;
        for flow in &self.recent_flows {
            let age_secs = now.duration_since(flow.observed_at).as_secs_f64();
            let recency_weight = (-age_secs / 4.0).exp();
            let size_weight = 1.0 + (flow.notional_eth.max(0.0).ln_1p() / 8.0).clamp(0.0, 0.5);
            let gas_weight = 1.0
                + (flow.gas_price_gwei / self.gas_price_ewma_gwei.max(1.0)).clamp(0.0, 2.0) * 0.10;
            let weight = recency_weight * size_weight * gas_weight;
            total_weight += weight;
            total_recent += recency_weight;
            let is_aggressive = if self.gas_price_ewma_gwei > f64::EPSILON
                && flow.gas_price_gwei > self.gas_price_ewma_gwei * 1.18
            {
                recency_weight
            } else {
                0.0
            };
            aggressive_total += is_aggressive;
            if flow.key == key {
                cluster_weight += weight;
                cluster_recent += recency_weight;
                aggressive_cluster += is_aggressive;
            }
        }

        if total_weight <= f64::EPSILON {
            0.0
        } else {
            let local_share = (cluster_weight / total_weight).clamp(0.0, 1.0);
            let burst_share = if total_recent <= f64::EPSILON {
                0.0
            } else {
                (cluster_recent / total_recent).clamp(0.0, 1.0)
            };
            let aggression_share = if aggressive_total <= f64::EPSILON {
                0.0
            } else {
                (aggressive_cluster / aggressive_total).clamp(0.0, 1.0)
            };
            let dominance = if local_share > 0.24 && burst_share > 0.20 {
                ((local_share - 0.24) * 1.8 + (burst_share - 0.20) * 1.4).clamp(0.0, 1.0)
            } else {
                0.0
            };
            (local_share * 0.42 + burst_share * 0.28 + aggression_share * 0.15 + dominance * 0.15)
                .clamp(0.0, 1.0)
        }
    }

    fn gas_pressure(&self, current_gwei: f64) -> f64 {
        if self.gas_price_ewma_gwei <= f64::EPSILON {
            return 0.0;
        }
        ((current_gwei / self.gas_price_ewma_gwei) - 1.0).clamp(0.0, 2.0) / 2.0
    }

    fn latency_penalty(&self, current_lookup_ms: f64) -> f64 {
        let lookup = ((current_lookup_ms / LOOKUP_TARGET_MS) - 1.0).clamp(0.0, 2.0) / 2.0;
        let lookup_baseline =
            ((self.lookup_latency_ewma_ms / LOOKUP_TARGET_MS) - 1.0).clamp(0.0, 2.0) / 2.0;
        let submit = ((self.submit_latency_ewma_ms / SUBMIT_TARGET_MS) - 1.0).clamp(0.0, 2.0) / 2.0;
        let finalize =
            ((self.finalize_latency_ewma_ms / FINALIZE_TARGET_MS) - 1.0).clamp(0.0, 2.0) / 2.0;
        (lookup * 0.35 + lookup_baseline * 0.20 + submit * 0.25 + finalize * 0.20).clamp(0.0, 1.0)
    }

    fn impact_hint(&self, notional_eth: f64, path_len: usize) -> f64 {
        let size_multiple = notional_eth.max(0.0) / self.min_large_swap_eth.max(1e-9);
        let size_curve = (size_multiple.ln_1p() / 3.6).clamp(0.0, 1.0);
        let profit_curve =
            ((notional_eth / self.min_profit_eth.max(1e-9)).ln_1p() / 14.0).clamp(0.0, 1.0);
        let multi_hop_penalty = path_penalty(path_len) * 0.18;
        (size_curve * 0.58 + profit_curve * 0.42 - multi_hop_penalty).clamp(0.0, 1.0)
    }

    fn market_regime(
        &self,
        mempool_density: f64,
        cluster_heat: f64,
        gas_pressure: f64,
        latency_penalty: f64,
        failure_pressure: f64,
        poor_realization: f64,
        builder_pressure: f64,
    ) -> MarketRegime {
        let stress = (mempool_density * 0.20
            + cluster_heat * 0.20
            + gas_pressure * 0.10
            + latency_penalty * 0.12
            + failure_pressure * 0.14
            + poor_realization * 0.10
            + builder_pressure * 0.14)
            .clamp(0.0, 1.0);
        if stress >= 0.82
            || failure_pressure >= 0.72
            || poor_realization >= 0.62
            || builder_pressure >= 0.72
        {
            MarketRegime::Toxic
        } else if stress >= 0.58
            || cluster_heat >= 0.55
            || gas_pressure >= 0.52
            || builder_pressure >= 0.48
        {
            MarketRegime::Hot
        } else if stress >= 0.30 {
            MarketRegime::Normal
        } else {
            MarketRegime::Calm
        }
    }

    fn competition_score(
        &self,
        cluster_heat: f64,
        mempool_density: f64,
        gas_pressure: f64,
        size_pressure: f64,
        impact_pressure: f64,
        builder_pressure: f64,
        regime: MarketRegime,
    ) -> f64 {
        let base = (cluster_heat * 0.38
            + mempool_density * 0.15
            + gas_pressure * 0.15
            + size_pressure * 0.12
            + impact_pressure * 0.10
            + builder_pressure * 0.10)
            .clamp(0.0, 1.0);
        let jump = logistic((cluster_heat - 0.52) * 9.0) * 0.18
            + logistic((mempool_density - 0.62) * 8.0) * 0.10
            + logistic((gas_pressure - 0.58) * 8.5) * 0.08
            + logistic((builder_pressure - 0.45) * 10.0) * 0.16;
        let regime_bump = match regime {
            MarketRegime::Calm => 0.0,
            MarketRegime::Normal => 0.03,
            MarketRegime::Hot => 0.10,
            MarketRegime::Toxic => 0.18,
        };
        (base + jump + regime_bump).clamp(0.0, 1.0)
    }

    fn risk_score(
        &self,
        failure_pressure: f64,
        latency_penalty: f64,
        poor_realization: f64,
        gas_pressure: f64,
        impact_pressure: f64,
        builder_pressure: f64,
        regime: MarketRegime,
    ) -> f64 {
        let base = (failure_pressure * 0.42
            + latency_penalty * 0.20
            + poor_realization * 0.16
            + gas_pressure * 0.07
            + impact_pressure * 0.05)
            .clamp(0.0, 1.0);
        let jump = logistic((failure_pressure - 0.48) * 10.0) * 0.16
            + logistic((latency_penalty - 0.45) * 8.0) * 0.09
            + logistic((poor_realization - 0.32) * 9.0) * 0.11
            + logistic((builder_pressure - 0.42) * 10.5) * 0.18;
        let regime_bump = match regime {
            MarketRegime::Calm => 0.0,
            MarketRegime::Normal => 0.02,
            MarketRegime::Hot => 0.08,
            MarketRegime::Toxic => 0.15,
        };
        (base + jump + regime_bump).clamp(0.0, 1.0)
    }

    fn probability_positive(
        &self,
        competition_score: f64,
        risk_score: f64,
        failure_pressure: f64,
        latency_penalty: f64,
        poor_realization: f64,
        gas_pressure: f64,
        builder_pressure: f64,
        regime: MarketRegime,
    ) -> f64 {
        let base_drag = competition_score * 0.29
            + risk_score * 0.23
            + failure_pressure * 0.17
            + latency_penalty * 0.10
            + poor_realization * 0.09
            + gas_pressure * 0.06
            + builder_pressure * 0.06;
        let regime_drag = match regime {
            MarketRegime::Calm => 0.00,
            MarketRegime::Normal => 0.03,
            MarketRegime::Hot => 0.08,
            MarketRegime::Toxic => 0.16,
        };
        (1.0 - base_drag - regime_drag).clamp(0.04, 0.985)
    }

    fn competition_penalty(
        &self,
        expected_profit_usd: f64,
        competition_score: f64,
        cluster_heat: f64,
        builder_pressure: f64,
        regime: MarketRegime,
    ) -> f64 {
        let multiplier = match regime {
            MarketRegime::Calm => 0.30,
            MarketRegime::Normal => 0.40,
            MarketRegime::Hot => 0.56,
            MarketRegime::Toxic => 0.78,
        };
        let jump = if cluster_heat > 0.58 {
            logistic((cluster_heat - 0.58) * 11.0) * 0.34
        } else {
            0.0
        } + if builder_pressure > 0.44 {
            logistic((builder_pressure - 0.44) * 10.5) * 0.28
        } else {
            0.0
        };
        expected_profit_usd * competition_score * (multiplier + jump)
    }

    fn risk_penalty(
        &self,
        expected_profit_usd: f64,
        risk_score: f64,
        failure_pressure: f64,
        builder_pressure: f64,
        regime: MarketRegime,
    ) -> f64 {
        let multiplier = match regime {
            MarketRegime::Calm => 0.42,
            MarketRegime::Normal => 0.54,
            MarketRegime::Hot => 0.68,
            MarketRegime::Toxic => 0.88,
        };
        let jump = if failure_pressure > 0.42 {
            logistic((failure_pressure - 0.42) * 12.0) * 0.26
        } else {
            0.0
        } + if builder_pressure > 0.46 {
            logistic((builder_pressure - 0.46) * 9.5) * 0.22
        } else {
            0.0
        };
        expected_profit_usd * risk_score * (multiplier + jump)
    }

    fn dynamic_threshold(
        &self,
        mempool_density: f64,
        failure_pressure: f64,
        gas_pressure: f64,
        latency_penalty: f64,
        poor_realization: f64,
        cluster_heat: f64,
        builder_pressure: f64,
        regime: MarketRegime,
    ) -> f64 {
        let regime_base = match regime {
            MarketRegime::Calm => 0.92,
            MarketRegime::Normal => 1.08,
            MarketRegime::Hot => 1.42,
            MarketRegime::Toxic => 1.95,
        };
        let continuous = 1.0
            + mempool_density * 0.28
            + failure_pressure * 0.52
            + gas_pressure * 0.22
            + latency_penalty * 0.24
            + poor_realization * 0.30
            + cluster_heat * 0.24
            + builder_pressure * 0.38;
        self.base_threshold_usd * regime_base * continuous
    }

    fn path_execution_penalty_usd(
        &self,
        expected_profit_usd: f64,
        builder_pressure: f64,
        failure_pressure: f64,
        latency_penalty: f64,
        gas_pressure: f64,
        context_toxicity_score: f64,
    ) -> f64 {
        let path_stress = (builder_pressure * 0.34
            + failure_pressure * 0.24
            + latency_penalty * 0.18
            + gas_pressure * 0.12
            + context_toxicity_score * 0.12)
            .clamp(0.0, 1.0);
        expected_profit_usd * path_stress * 0.18
    }

    fn min_probability_threshold(&self, regime: MarketRegime) -> f64 {
        match regime {
            MarketRegime::Calm => 0.54,
            MarketRegime::Normal => 0.60,
            MarketRegime::Hot => 0.66,
            MarketRegime::Toxic => 0.74,
        }
    }

    fn max_competition_threshold(&self, regime: MarketRegime) -> f64 {
        match regime {
            MarketRegime::Calm => 0.78,
            MarketRegime::Normal => 0.72,
            MarketRegime::Hot => 0.64,
            MarketRegime::Toxic => 0.52,
        }
    }

    fn max_risk_threshold(&self, regime: MarketRegime) -> f64 {
        match regime {
            MarketRegime::Calm => 0.80,
            MarketRegime::Normal => 0.74,
            MarketRegime::Hot => 0.66,
            MarketRegime::Toxic => 0.56,
        }
    }

    fn builder_path_pressure(&self) -> f64 {
        let relay_rejection = self.relay_reject_rate_ewma.clamp(0.0, 1.0);
        let relay_instability = (1.0 - self.relay_accept_rate_ewma).clamp(0.0, 1.0);
        let inclusion_instability = (1.0 - self.inclusion_success_rate_ewma).clamp(0.0, 1.0);
        let accepted_not_included = self.accepted_not_included_rate_ewma.clamp(0.0, 1.0);
        let included_revert = self.included_revert_rate_ewma.clamp(0.0, 1.0);
        let submit_latency =
            ((self.submit_latency_ewma_ms / SUBMIT_TARGET_MS) - 1.0).clamp(0.0, 2.0) / 2.0;
        let finalization_latency =
            ((self.finalize_latency_ewma_ms / FINALIZE_TARGET_MS) - 1.0).clamp(0.0, 2.0) / 2.0;
        let poor_realization = (1.0 - self.realized_capture_ewma).clamp(0.0, 1.0);

        (relay_rejection * 0.24
            + relay_instability * 0.12
            + inclusion_instability * 0.12
            + accepted_not_included * 0.24
            + included_revert * 0.16
            + submit_latency * 0.10
            + finalization_latency * 0.08
            + poor_realization * 0.06)
            .clamp(0.0, 1.0)
    }

    fn builder_path_pressure_for_cluster(&self, relay: Option<&str>, cluster: ClusterKey) -> f64 {
        let base = relay
            .map(|relay| self.builder_path_pressure_for_relay(relay))
            .unwrap_or_else(|| self.builder_path_pressure());
        let Some(context) =
            relay.and_then(|relay| self.relay_context_stats.get(&(relay.to_string(), cluster)))
        else {
            return base;
        };

        let success_drag = (1.0 - context.inclusion_success_rate_ewma).clamp(0.0, 1.0);
        let miss = context.accepted_not_included_rate_ewma.clamp(0.0, 1.0);
        let revert = context.included_revert_rate_ewma.clamp(0.0, 1.0);
        let poor_realization = (1.0 - context.realized_capture_ewma).clamp(0.0, 1.0);
        let confidence = (context.samples.min(24) as f64 / 24.0).clamp(0.0, 1.0);
        let contextual =
            (success_drag * 0.24 + miss * 0.36 + revert * 0.24 + poor_realization * 0.16)
                .clamp(0.0, 1.0);
        (base * (1.0 - confidence * 0.35) + contextual * confidence * 0.65).clamp(0.0, 1.0)
    }

    fn historical_profile(
        &self,
        pair: Address,
        router: Address,
        hour_utc: u8,
    ) -> Option<&HistoricalOutcomeStats> {
        self.historical_profiles.get(&HistoricalProfileKey {
            router,
            pair,
            hour_utc,
        })
    }

    fn historical_pressure(profile: &HistoricalOutcomeStats) -> f64 {
        ((1.0 - profile.success_rate).clamp(0.0, 1.0) * 0.24
            + profile.accepted_not_included_rate.clamp(0.0, 1.0) * 0.36
            + profile.revert_rate.clamp(0.0, 1.0) * 0.24
            + (1.0 - profile.realized_capture).clamp(0.0, 1.0) * 0.16)
            .clamp(0.0, 1.0)
    }

    fn historical_calibration(
        &self,
        profile: &HistoricalOutcomeStats,
        confidence: f64,
    ) -> HistoricalCalibration {
        let instability = (1.0 - profile.success_rate).clamp(0.0, 1.0);
        let miss = profile.accepted_not_included_rate.clamp(0.0, 1.0);
        let revert = profile.revert_rate.clamp(0.0, 1.0);
        let poor_realization = (1.0 - profile.realized_capture).clamp(0.0, 1.0);

        let competition_mult = 1.0 + (miss * 0.34 + instability * 0.18) * confidence;
        let risk_mult =
            1.0 + (revert * 0.42 + poor_realization * 0.24 + instability * 0.10) * confidence;
        let threshold_mult = 1.0
            + (miss * 0.26 + revert * 0.18 + instability * 0.18 + poor_realization * 0.14)
                * confidence;
        let stress = (instability * 0.28 + miss * 0.34 + revert * 0.22 + poor_realization * 0.16)
            .clamp(0.0, 1.0);
        let regime_shift = if profile.samples >= 12 && stress >= 0.54 {
            2
        } else if profile.samples >= 6 && stress >= 0.42 {
            1
        } else if profile.samples >= 12
            && stress <= 0.18
            && profile.success_rate >= 0.82
            && profile.realized_capture >= 0.82
        {
            -1
        } else {
            0
        };

        HistoricalCalibration {
            competition_mult,
            risk_mult,
            threshold_mult,
            regime_shift,
        }
    }

    fn recalibrated_regime(&self, regime: MarketRegime, shift: i8) -> MarketRegime {
        match shift {
            i8::MIN..=-2 => MarketRegime::Calm,
            -1 => match regime {
                MarketRegime::Toxic => MarketRegime::Hot,
                MarketRegime::Hot => MarketRegime::Normal,
                MarketRegime::Normal => MarketRegime::Calm,
                MarketRegime::Calm => MarketRegime::Calm,
            },
            0 => regime,
            1 => match regime {
                MarketRegime::Calm => MarketRegime::Normal,
                MarketRegime::Normal => MarketRegime::Hot,
                MarketRegime::Hot => MarketRegime::Toxic,
                MarketRegime::Toxic => MarketRegime::Toxic,
            },
            _ => MarketRegime::Toxic,
        }
    }

    fn relay_quote(&self, relay: &str) -> RelayQuote {
        let stats = self.relay_stats.get(relay);
        let relay_pressure = self.builder_path_pressure_for_relay(relay);
        let accept_rate = stats
            .map(|stats| stats.relay_accept_rate_ewma)
            .unwrap_or(self.relay_accept_rate_ewma);
        let inclusion_rate = stats
            .map(|stats| stats.inclusion_success_rate_ewma)
            .unwrap_or(self.inclusion_success_rate_ewma);
        let accepted_not_included_rate = stats
            .map(|stats| stats.accepted_not_included_rate_ewma)
            .unwrap_or(self.accepted_not_included_rate_ewma);
        let revert_rate = stats
            .map(|stats| stats.included_revert_rate_ewma)
            .unwrap_or(self.included_revert_rate_ewma);
        let submit_latency_ms = stats
            .map(|stats| stats.submit_latency_ewma_ms)
            .unwrap_or(self.submit_latency_ewma_ms);
        let finalization_latency_ms = stats
            .map(|stats| stats.finalization_latency_ewma_ms)
            .unwrap_or(self.finalize_latency_ewma_ms);
        let submit_penalty = ((submit_latency_ms / SUBMIT_TARGET_MS) - 1.0).clamp(0.0, 2.0) / 2.0;
        let finalize_penalty =
            ((finalization_latency_ms / FINALIZE_TARGET_MS) - 1.0).clamp(0.0, 2.0) / 2.0;
        let score = relay_pressure * 0.48
            + (1.0 - accept_rate).clamp(0.0, 1.0) * 0.20
            + accepted_not_included_rate.clamp(0.0, 1.0) * 0.14
            + revert_rate.clamp(0.0, 1.0) * 0.08
            + (1.0 - inclusion_rate).clamp(0.0, 1.0) * 0.05
            + submit_penalty * 0.03
            + finalize_penalty * 0.02;

        RelayQuote {
            relay: relay.to_string(),
            relay_pressure,
            accept_rate,
            inclusion_rate,
            accepted_not_included_rate,
            revert_rate,
            submit_latency_ms,
            finalization_latency_ms,
            score,
        }
    }

    fn relay_quote_for_cluster(&self, relay: &str, cluster: ClusterKey) -> RelayQuote {
        let mut quote = self.relay_quote(relay);
        let Some(context) = self.relay_context_stats.get(&(relay.to_string(), cluster)) else {
            return quote;
        };
        let confidence = (context.samples.min(24) as f64 / 24.0).clamp(0.0, 1.0);
        let contextual_miss_penalty =
            context.accepted_not_included_rate_ewma.clamp(0.0, 1.0) * confidence * 0.22;
        let contextual_revert_penalty =
            context.included_revert_rate_ewma.clamp(0.0, 1.0) * confidence * 0.10;
        let contextual_success_credit =
            context.inclusion_success_rate_ewma.clamp(0.0, 1.0) * confidence * 0.06;
        quote.relay_pressure = self.builder_path_pressure_for_cluster(Some(relay), cluster);
        quote.score = (quote.score + contextual_miss_penalty + contextual_revert_penalty
            - contextual_success_credit)
            .clamp(0.0, 2.0);
        quote
    }

    fn builder_path_pressure_for_relay(&self, relay: &str) -> f64 {
        let Some(stats) = self.relay_stats.get(relay) else {
            return self.builder_path_pressure();
        };

        let relay_rejection = stats.relay_reject_rate_ewma.clamp(0.0, 1.0);
        let relay_instability = (1.0 - stats.relay_accept_rate_ewma).clamp(0.0, 1.0);
        let inclusion_instability = (1.0 - stats.inclusion_success_rate_ewma).clamp(0.0, 1.0);
        let accepted_not_included = stats.accepted_not_included_rate_ewma.clamp(0.0, 1.0);
        let included_revert = stats.included_revert_rate_ewma.clamp(0.0, 1.0);
        let submit_latency =
            ((stats.submit_latency_ewma_ms / SUBMIT_TARGET_MS) - 1.0).clamp(0.0, 2.0) / 2.0;
        let finalization_latency =
            ((stats.finalization_latency_ewma_ms / FINALIZE_TARGET_MS) - 1.0).clamp(0.0, 2.0) / 2.0;
        let poor_realization = (1.0 - stats.realized_capture_ewma).clamp(0.0, 1.0);

        (relay_rejection * 0.26
            + relay_instability * 0.12
            + inclusion_instability * 0.12
            + accepted_not_included * 0.24
            + included_revert * 0.14
            + submit_latency * 0.10
            + finalization_latency * 0.08
            + poor_realization * 0.04)
            .clamp(0.0, 1.0)
    }
}

impl MarketRegime {
    pub fn as_str(self) -> &'static str {
        match self {
            MarketRegime::Calm => "calm",
            MarketRegime::Normal => "normal",
            MarketRegime::Hot => "hot",
            MarketRegime::Toxic => "toxic",
        }
    }
}

fn ewma(current: f64, sample: f64, alpha: f64) -> f64 {
    current * (1.0 - alpha) + sample * alpha
}

fn wei_to_gwei_f64(value: U256) -> f64 {
    value.to_string().parse::<f64>().unwrap_or(0.0) / 1e9
}

fn size_pressure(notional_eth: f64) -> f64 {
    (notional_eth.max(0.0).ln_1p() / 8.0).clamp(0.0, 1.0)
}

fn path_penalty(path_len: usize) -> f64 {
    path_len.saturating_sub(2).min(3) as f64 / 3.0
}

fn logistic(x: f64) -> f64 {
    1.0 / (1.0 + (-x).exp())
}

fn capture_ratio(expected_profit_wei: U256, realized_profit_eth: f64) -> f64 {
    let expected_profit_eth = wei_to_eth_f64(expected_profit_wei);
    if expected_profit_eth > f64::EPSILON {
        (realized_profit_eth / expected_profit_eth).clamp(0.0, 1.25)
    } else {
        0.0
    }
}

fn is_better_quote(candidate: &AdaptiveQuote, current: &AdaptiveQuote) -> bool {
    let candidate_margin = candidate.ev_real_usd - candidate.threshold_dynamic_usd;
    let current_margin = current.ev_real_usd - current.threshold_dynamic_usd;
    if candidate.should_execute != current.should_execute {
        return candidate.should_execute;
    }
    if (candidate_margin - current_margin).abs() > f64::EPSILON {
        return candidate_margin > current_margin;
    }
    if (candidate.p_positive - current.p_positive).abs() > f64::EPSILON {
        return candidate.p_positive > current.p_positive;
    }
    candidate.builder_pressure < current.builder_pressure
}

fn chain_adaptive_preset(network: &str) -> (f64, f64, f64, f64, f64, f64, f64) {
    match network {
        "bsc" => (3.0, 0.86, 0.76, 0.16, 0.94, 1.06, 0.92),
        "polygon" => (80.0, 1.08, 0.70, 0.20, 1.05, 1.02, 1.08),
        _ => (25.0, 1.0, 0.72, 0.18, 1.0, 1.0, 1.0),
    }
}

impl Default for RelayStats {
    fn default() -> Self {
        Self {
            submit_latency_ewma_ms: SUBMIT_TARGET_MS,
            finalization_latency_ewma_ms: FINALIZE_TARGET_MS,
            relay_accept_rate_ewma: 0.76,
            relay_reject_rate_ewma: 0.16,
            inclusion_success_rate_ewma: 0.58,
            accepted_not_included_rate_ewma: 0.14,
            included_revert_rate_ewma: 0.08,
            realized_capture_ewma: 0.82,
        }
    }
}

impl Default for HistoricalCalibration {
    fn default() -> Self {
        Self {
            competition_mult: 1.0,
            risk_mult: 1.0,
            threshold_mult: 1.0,
            regime_shift: 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ContextualOutcomeKind {
    SubmitFailed,
    AcceptedNotIncluded,
    IncludedRevert,
    IncludedSuccess,
}

impl Default for RelayContextStats {
    fn default() -> Self {
        Self {
            inclusion_success_rate_ewma: 0.58,
            accepted_not_included_rate_ewma: 0.14,
            included_revert_rate_ewma: 0.08,
            realized_capture_ewma: 0.82,
            samples: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HiddenMarkovModel {
    transition_matrix: ndarray::Array2<f64>,
    emission_matrix: ndarray::Array2<f64>,
    initial_distribution: ndarray::Array1<f64>,
    current_belief: ndarray::Array1<f64>,
}

impl HiddenMarkovModel {
    pub fn new(num_states: usize, num_observations: usize) -> Self {
        let states = num_states.max(1);
        let observations = num_observations.max(1);
        Self {
            transition_matrix: ndarray::Array2::from_elem((states, states), 1.0 / states as f64),
            emission_matrix: ndarray::Array2::from_elem(
                (states, observations),
                1.0 / observations as f64,
            ),
            initial_distribution: ndarray::Array1::from_elem(states, 1.0 / states as f64),
            current_belief: ndarray::Array1::from_elem(states, 1.0 / states as f64),
        }
    }

    pub fn forward_algorithm(&self, observations: &[usize]) -> f64 {
        if observations.is_empty() {
            return 1.0;
        }
        let t_len = observations.len();
        let states = self.transition_matrix.shape()[0];
        let obs_count = self.emission_matrix.shape()[1];
        let mut alpha = ndarray::Array2::zeros((t_len, states));

        for i in 0..states {
            let obs = observations[0].min(obs_count - 1);
            alpha[[0, i]] = self.initial_distribution[i] * self.emission_matrix[[i, obs]];
        }
        normalize_row(&mut alpha, 0);

        for t in 1..t_len {
            let obs = observations[t].min(obs_count - 1);
            for j in 0..states {
                let sum = (0..states)
                    .map(|i| alpha[[t - 1, i]] * self.transition_matrix[[i, j]])
                    .sum::<f64>();
                alpha[[t, j]] = sum * self.emission_matrix[[j, obs]];
            }
            normalize_row(&mut alpha, t);
        }

        (0..states)
            .map(|i| alpha[[t_len - 1, i]])
            .sum::<f64>()
            .clamp(0.0, 1.0)
    }

    pub fn baum_welch(&mut self, observations: &[usize], max_iter: usize, tol: f64) {
        if observations.len() < 2 {
            return;
        }
        let states = self.transition_matrix.shape()[0];
        let obs_count = self.emission_matrix.shape()[1];
        let t_len = observations.len();
        let mut previous_likelihood = f64::NEG_INFINITY;

        for _ in 0..max_iter {
            let mut alpha = ndarray::Array2::zeros((t_len, states));
            let mut beta = ndarray::Array2::ones((t_len, states));

            for i in 0..states {
                let obs = observations[0].min(obs_count - 1);
                alpha[[0, i]] = self.initial_distribution[i] * self.emission_matrix[[i, obs]];
            }
            normalize_row(&mut alpha, 0);
            for t in 1..t_len {
                let obs = observations[t].min(obs_count - 1);
                for j in 0..states {
                    let sum = (0..states)
                        .map(|i| alpha[[t - 1, i]] * self.transition_matrix[[i, j]])
                        .sum::<f64>();
                    alpha[[t, j]] = sum * self.emission_matrix[[j, obs]];
                }
                normalize_row(&mut alpha, t);
            }

            for t in (0..t_len - 1).rev() {
                let next_obs = observations[t + 1].min(obs_count - 1);
                for i in 0..states {
                    beta[[t, i]] = (0..states)
                        .map(|j| {
                            self.transition_matrix[[i, j]]
                                * self.emission_matrix[[j, next_obs]]
                                * beta[[t + 1, j]]
                        })
                        .sum::<f64>();
                }
                normalize_row(&mut beta, t);
            }

            let mut new_transition = ndarray::Array2::zeros((states, states));
            let mut new_emission = ndarray::Array2::zeros((states, obs_count));
            let mut new_initial = ndarray::Array1::zeros(states);

            for t in 0..t_len {
                let gamma_den = (0..states)
                    .map(|i| alpha[[t, i]] * beta[[t, i]])
                    .sum::<f64>()
                    .max(1e-12);
                let obs = observations[t].min(obs_count - 1);
                for i in 0..states {
                    let gamma = alpha[[t, i]] * beta[[t, i]] / gamma_den;
                    if t == 0 {
                        new_initial[i] = gamma;
                    }
                    new_emission[[i, obs]] += gamma;
                    if t + 1 < t_len {
                        let next_obs = observations[t + 1].min(obs_count - 1);
                        for j in 0..states {
                            new_transition[[i, j]] += gamma
                                * self.transition_matrix[[i, j]]
                                * self.emission_matrix[[j, next_obs]];
                        }
                    }
                }
            }

            normalize_matrix_rows(&mut new_transition);
            normalize_matrix_rows(&mut new_emission);
            normalize_vector(&mut new_initial);

            self.transition_matrix = new_transition;
            self.emission_matrix = new_emission;
            self.initial_distribution = new_initial;

            let likelihood = self.forward_algorithm(observations);
            if (likelihood - previous_likelihood).abs() < tol {
                break;
            }
            previous_likelihood = likelihood;
        }
    }

    pub fn filter(&mut self, observation: usize) -> ndarray::Array1<f64> {
        let states = self.transition_matrix.shape()[0];
        let obs = observation.min(self.emission_matrix.shape()[1] - 1);
        let mut next = ndarray::Array1::zeros(states);
        for j in 0..states {
            next[j] = (0..states)
                .map(|i| self.current_belief[i] * self.transition_matrix[[i, j]])
                .sum::<f64>()
                * self.emission_matrix[[j, obs]];
        }
        normalize_vector(&mut next);
        self.current_belief = next.clone();
        next
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BetaDistribution {
    alpha: f64,
    beta: f64,
}

impl BetaDistribution {
    pub fn new(alpha: f64, beta: f64) -> Self {
        Self {
            alpha: alpha.max(1e-6),
            beta: beta.max(1e-6),
        }
    }

    pub fn update(&mut self, success: bool) {
        if success {
            self.alpha += 1.0;
        } else {
            self.beta += 1.0;
        }
    }

    pub fn mean(&self) -> f64 {
        self.alpha / (self.alpha + self.beta)
    }

    pub fn variance(&self) -> f64 {
        (self.alpha * self.beta)
            / ((self.alpha + self.beta).powi(2) * (self.alpha + self.beta + 1.0))
    }

    pub fn lower_confidence_bound(&self, z: f64) -> f64 {
        (self.mean() - z.abs() * self.variance().sqrt()).clamp(0.0, 1.0)
    }
}

#[derive(Clone, Debug)]
pub struct HjbSolver {
    value_function: ndarray::Array1<f64>,
    optimal_policy: ndarray::Array1<f64>,
    dt: f64,
}

impl HjbSolver {
    pub fn new(state_dim: usize, dt: f64) -> Self {
        let dim = state_dim.max(1);
        Self {
            value_function: ndarray::Array1::zeros(dim),
            optimal_policy: ndarray::Array1::zeros(dim),
            dt: dt.max(1e-6),
        }
    }

    pub fn solve(&mut self, reward_fn: impl Fn(usize, f64) -> f64, max_iter: usize) {
        for _ in 0..max_iter {
            let mut next_value = ndarray::Array1::zeros(self.value_function.len());
            for state in 0..self.value_function.len() {
                let mut best_value = f64::NEG_INFINITY;
                let mut best_action = 0.0;
                for action_step in 0..=100 {
                    let action = action_step as f64 / 100.0;
                    let next_state = ((state as f64 + action * self.dt).round() as usize)
                        .min(self.value_function.len() - 1);
                    let value =
                        reward_fn(state, action) + self.dt * self.value_function[next_state];
                    if value > best_value {
                        best_value = value;
                        best_action = action;
                    }
                }
                next_value[state] = best_value;
                self.optimal_policy[state] = best_action;
            }
            let diff = (&next_value - &self.value_function).mapv(f64::abs).sum();
            self.value_function = next_value;
            if diff < 1e-6 {
                break;
            }
        }
    }

    pub fn get_optimal_action(&self, state: usize) -> f64 {
        self.optimal_policy[state.min(self.optimal_policy.len() - 1)]
    }
}

#[derive(Clone, Debug)]
pub struct InclusionPoissonProcess {
    lambda: f64,
    last_observation: Instant,
}

impl InclusionPoissonProcess {
    pub fn new(initial_lambda: f64) -> Self {
        Self {
            lambda: initial_lambda.clamp(0.01, 100.0),
            last_observation: Instant::now(),
        }
    }

    pub fn inclusion_probability(&self, delta_ms: f64) -> f64 {
        (1.0 - (-self.lambda * delta_ms.max(0.0) / 1000.0).exp()).clamp(0.0, 1.0)
    }

    pub fn update_lambda(&mut self, inclusion_observed: bool, delta_ms: f64) {
        let observed_lambda = if inclusion_observed && delta_ms > 0.0 {
            1000.0 / delta_ms
        } else {
            0.0
        };
        self.lambda = (self.lambda * 0.9 + observed_lambda * 0.1).clamp(0.01, 100.0);
        self.last_observation = Instant::now();
    }

    pub fn lambda(&self) -> f64 {
        self.lambda
    }
}

fn normalize_row(matrix: &mut ndarray::Array2<f64>, row: usize) {
    let sum = matrix.row(row).sum();
    if sum > 0.0 {
        for value in matrix.row_mut(row) {
            *value /= sum;
        }
    }
}

fn normalize_matrix_rows(matrix: &mut ndarray::Array2<f64>) {
    for row in 0..matrix.shape()[0] {
        normalize_row(matrix, row);
    }
}

fn normalize_vector(vector: &mut ndarray::Array1<f64>) {
    let sum = vector.sum();
    if sum > 0.0 {
        for value in vector.iter_mut() {
            *value /= sum;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        Config, MevConfig, MonitoredTokenConfig, OpportunityMode, OpportunityThresholds,
        RpcPreference,
    };
    use ethers::types::Address;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::path::PathBuf;
    use std::sync::{Arc, RwLock};

    fn test_config(network: &str) -> Config {
        Config {
            wallets: PathBuf::from("keys.txt"),
            network: network.to_string(),
            chain_id: match network {
                "bsc" => 56,
                "polygon" => 137,
                _ => 1,
            },
            allow_send: true,
            tenderly_rpc_only: false,
            alchemy_keys: vec![("default".to_string(), "test".to_string())],
            infura_ids: Vec::new(),
            flashbots_relay: "https://relay.flashbots.net".to_string(),
            builder_relays: vec![
                "https://relay-a.test".to_string(),
                "https://relay-b.test".to_string(),
            ],
            executor_private_key:
                "0x59c6995e998f97a5a0044966f0945382d7a7d4f6d8f1f0db6b90e6a2f17d5f52".to_string(),
            executor_address: Address::from_low_u64_be(10),
            vault_address: Address::from_low_u64_be(11),
            profit_address: Address::from_low_u64_be(12),
            control_address: Address::from_low_u64_be(13),
            monitored_tokens: vec![MonitoredTokenConfig {
                address: Address::from_low_u64_be(100),
                decimals: 18,
                price_eth: 1.0,
            }],
            estimated_exec_gas: 250_000,
            estimated_bundle_overhead_gas: 25_000,
            max_infura_endpoints: 1,
            rpc_read_preference: RpcPreference::Auto,
            rpc_send_preference: RpcPreference::Auto,
            storage_path: PathBuf::from("test.sqlite"),
            dashboard_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8787),
            explicit_rpc_urls: Vec::new(),
            mempool_ws_urls: Vec::new(),
            mev: MevConfig {
                enabled: true,
                opportunity_mode: Arc::new(RwLock::new(OpportunityMode::Conservative)),
                runtime_thresholds: Arc::new(RwLock::new(OpportunityThresholds {
                    min_large_swap_eth: 5.0,
                    min_net_profit_eth: 0.001,
                    min_profit_usd: 2.0,
                    min_liquidity_eth: 10.0,
                })),
                capital_eth: 0.5,
                capital_window_secs: 90,
                max_window_exposure_eth: 1.0,
                max_cluster_window_exposure_eth: 0.5,
                max_pair_window_exposure_eth: 0.75,
                min_net_profit_eth: 0.001,
                min_roi_bps: 800,
                min_large_swap_eth: 5.0,
                gas_safety_margin_bps: 12_500,
                max_pending_age_ms: 1500,
                max_gas_per_tx: 260_000,
                max_gas_price_gwei: Some(30),
                max_price_impact_bps: 250,
                slippage_protection_bps: 50,
                min_profit_usd: 2.0,
                eth_usd_price: 3000.0,
                min_liquidity_eth: 10.0,
                latency_trace: false,
                latency_trace_warn_us: 5_000,
                pool_state_cache_ttl_ms: 120,
                executor_min_buffer_eth: 0.1,
                executor_target_buffer_eth: 0.3,
                executor_max_buffer_eth: 1.0,
                relay_fanout_count: 3,
                rpc_fanout_count: 2,
                gas_overpay_base_extra_bps: 500,
                gas_overpay_miss_extra_bps: 2_500,
                gas_overpay_revert_extra_bps: 1_200,
                gas_overpay_submit_failure_extra_bps: 1_500,
                gas_overpay_max_extra_bps: 5_000,
                finality_confirmations: 0,
                stop_loss_consecutive_losses: 3,
                stop_loss_freeze_secs: 300,
                context_stop_loss_consecutive_losses: 2,
                context_stop_loss_freeze_secs: 180,
                capital_multiplier_aggressive: 2.0,
                capital_multiplier_neutral: 1.0,
                capital_multiplier_defensive: 0.3,
                capital_multiplier_priority_threshold: 0.60,
                capital_multiplier_toxicity_threshold: 0.65,
                uniswap_v2_factory: Some(Address::from_low_u64_be(20)),
                uniswap_v3_factory: Some(Address::from_low_u64_be(22)),
                mev_executor: Some(Address::from_low_u64_be(21)),
            },
        }
    }

    fn sample_cluster() -> ClusterKey {
        ClusterKey {
            router: Address::from_low_u64_be(1),
            token_in: Address::from_low_u64_be(2),
            token_out: Address::from_low_u64_be(3),
            selector: [0x38, 0xed, 0x17, 0x39],
        }
    }

    #[test]
    fn chain_presets_are_distinct() {
        let bsc = AdaptivePolicy::new(&test_config("bsc"));
        let polygon = AdaptivePolicy::new(&test_config("polygon"));
        assert!(bsc.base_threshold_usd < polygon.base_threshold_usd);
        assert!(bsc.gas_price_ewma_gwei < polygon.gas_price_ewma_gwei);
        assert!(bsc.chain_threshold_mult < polygon.chain_threshold_mult);
    }

    #[test]
    fn bad_historical_profile_raises_threshold_and_regime() {
        let cluster = sample_cluster();
        let pair = Address::from_low_u64_be(4);
        let input = AdaptiveQuoteInput {
            cluster,
            pair,
            hour_utc: 14,
            context_priority_score: 0.50,
            context_toxicity_score: 0.50,
            expected_profit_wei: ethers::utils::parse_ether("0.01").unwrap(),
            execution_cost_wei: ethers::utils::parse_ether("0.001").unwrap(),
            gas_price_wei: U256::from(80_000_000_000u64),
            lookup_latency_ms: 120.0,
            notional_eth: 20.0,
            price_impact_bps: 35,
            relay_pressure_override: Some(0.15),
        };

        let mut baseline = AdaptivePolicy::new(&test_config("polygon"));
        let base_quote = baseline.quote(input);

        let mut stressed = AdaptivePolicy::new(&test_config("polygon"));
        stressed.apply_historical_profiles(vec![HistoricalOutcomeProfile {
            hour_utc: 14,
            pair,
            router: cluster.router,
            samples: 24,
            success_rate: 0.18,
            accepted_not_included_rate: 0.52,
            revert_rate: 0.20,
            realized_capture: 0.28,
        }]);
        let stressed_quote = stressed.quote(input);

        assert!(stressed_quote.threshold_dynamic_usd > base_quote.threshold_dynamic_usd);
        assert!(stressed_quote.competition_score >= base_quote.competition_score);
        assert!(matches!(
            stressed_quote.regime,
            MarketRegime::Hot | MarketRegime::Toxic
        ));
    }

    #[test]
    fn contextual_outcome_can_reorder_selected_relay() {
        let config = test_config("bsc");
        let cluster = sample_cluster();
        let pair = Address::from_low_u64_be(7);
        let mut policy = AdaptivePolicy::new(&config);
        for _ in 0..8 {
            policy.record_contextual_outcome(
                "https://relay-a.test",
                cluster,
                ethers::utils::parse_ether("0.01").unwrap(),
                0.0,
                ContextualOutcomeKind::AcceptedNotIncluded,
            );
        }
        let quote = policy.quote_for_relays(
            AdaptiveQuoteInput {
                cluster,
                pair,
                hour_utc: 10,
                context_priority_score: 0.50,
                context_toxicity_score: 0.50,
                expected_profit_wei: ethers::utils::parse_ether("0.01").unwrap(),
                execution_cost_wei: ethers::utils::parse_ether("0.001").unwrap(),
                gas_price_wei: U256::from(3_000_000_000u64),
                lookup_latency_ms: 90.0,
                notional_eth: 25.0,
                price_impact_bps: 30,
                relay_pressure_override: None,
            },
            &config.builder_relays,
        );

        assert_eq!(
            quote.selected_relay.as_deref(),
            Some("https://relay-b.test")
        );
    }

    #[test]
    fn cluster_ranking_penalizes_accepted_not_included_paths() {
        let config = test_config("bsc");
        let cluster = sample_cluster();
        let mut policy = AdaptivePolicy::new(&config);
        for _ in 0..12 {
            policy.record_contextual_outcome(
                "https://relay-a.test",
                cluster,
                ethers::utils::parse_ether("0.01").unwrap(),
                0.0,
                ContextualOutcomeKind::AcceptedNotIncluded,
            );
        }
        let ranked = policy.rank_relays_for_cluster(&config.builder_relays, cluster);
        assert_eq!(ranked.len(), 2);
        assert_eq!(ranked[0].relay, "https://relay-b.test");
        assert_eq!(ranked[1].relay, "https://relay-a.test");
        assert!(ranked[1].score > ranked[0].score);
        assert!(ranked[1].relay_pressure >= ranked[0].relay_pressure);
    }
}
