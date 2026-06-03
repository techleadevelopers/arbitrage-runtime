#![allow(dead_code)]

// Arquivo: src/mev/execution/payload_builder.rs

use crate::config::{Config, MonitoredTokenConfig, OpportunityMode};
use crate::mev::amm::uniswap_v2::{
    amount_out_exact_in, select_best_size_candidate, SizeCandidate, V2PoolState,
};
use crate::mev::amm::uniswap_v3::{V3PoolState, V3SizeCandidate};
use crate::mev::execution::contract_encoder::{EncodedSwapStep, EncodedV3SwapStep};
use crate::mev::execution::flashloan_builder::{build_v2_flashswap_call, build_v3_flashswap_call};
use crate::mev::opportunity::wei_to_eth_f64;
use crate::mev::simulation::state_simulator::{AmmState, StateSimulator};
use ethers::types::{Address, Bytes, U256};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub enum AmmRouteKind {
    UniswapV2,
    UniswapV3 { fee_tier: u32, path: Bytes },
}

#[derive(Debug, Clone)]
pub struct ExecutionPayload {
    pub tx: Bytes,
    pub calldata: Bytes,
    pub target_contract: Address,
    pub value: U256,
    pub pair: Address,
    pub amm_kind: AmmRouteKind,
    pub capital_committed_wei: U256,
    pub expected_profit_wei: U256,
    pub gas_limit: u64,
    pub price_impact_bps: u64,
    pub profit_token: Address,
    pub profit_recipient: Address,
    pub context_priority_score: f64,
    pub context_toxicity_score: f64,
    pub edge_metadata: Option<EdgeMetadata>,
    // NOVO: Estado do pool antes da execução para EVM preflight
    pub pool_state_before: AmmState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeMetadata {
    #[serde(default)]
    pub victim_tx: String,
    #[serde(default)]
    pub selector: String,
    pub status: String,
    pub reason: String,
    pub route_kind: String,
    #[serde(default)]
    pub path: Vec<String>,
    #[serde(default)]
    pub hops: u64,
    #[serde(default)]
    pub impacted_pools: Vec<String>,
    #[serde(default)]
    pub slippage_window_score: f64,
    #[serde(default)]
    pub pool_imbalance_score: f64,
    #[serde(default)]
    pub cross_dex_deviation_bps: i64,
    #[serde(default)]
    pub gas_estimate: u64,
    #[serde(default)]
    pub simulated_extraction_native: f64,
    #[serde(default)]
    pub aggregator_type: String,
    #[serde(default)]
    pub route_complexity: u64,
    #[serde(default)]
    pub split_ratio_bps: u64,
    #[serde(default)]
    pub dex_sequence: Vec<String>,
    #[serde(default)]
    pub route_inefficiency_score: f64,
    #[serde(default)]
    pub liquidity_distortion_score: f64,
    #[serde(default)]
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

#[derive(Debug, Clone)]
pub struct FeeExtractionBuildInput {
    pub router: Address,
    pub factory: Option<Address>,
    pub pair: Address,
    pub recipient: Address,
    pub token_in: Address,
    pub token_out: Address,
    pub victim_amount_in: U256,
    pub state_before: AmmState,
    pub capital_available_wei: U256,
    pub gas_price_wei: U256,
    pub context_priority_score: f64,
    pub context_toxicity_score: f64,
    pub route_kind: AmmRouteKind,
    pub v2_swap_path: Option<Vec<Address>>,
    pub v2_swap_pools: Vec<V2PoolState>,
}

pub struct PayloadBuilder;

impl ExecutionPayload {
    pub fn pool_state_clone(&self) -> AmmState {
        self.pool_state_before.clone()
    }
}

impl PayloadBuilder {
    pub fn build_fee_extraction_v2(
        config: &Config,
        input: FeeExtractionBuildInput,
    ) -> Result<ExecutionPayload, String> {
        let post_victim = StateSimulator::simulate_victim_exact_in(
            input.state_before.clone(),
            input.token_in,
            input.token_out,
            input.victim_amount_in,
        )
        .ok_or_else(|| {
            format!(
                "victim post-swap simulation failed token_in={:?} token_out={:?} amount_in={} pair={:?} state_before={:?}",
                input.token_in, input.token_out, input.victim_amount_in, input.pair, input.state_before
            )
        })?;

        let AmmState::UniswapV2(pool_after) = post_victim.state_after else {
            return Err("v2 payload requires UniswapV2 simulated state".to_string());
        };

        let scavenger = config.mev.opportunity_mode() == OpportunityMode::Scavenger;
        let shadow_research = payload_shadow_research_mode(config);
        if post_victim.slippage_impact_bps > effective_payload_price_impact_cap_bps(config) {
            return Err(format!(
                "victim price impact too high: {}bps",
                post_victim.slippage_impact_bps
            ));
        }

        let (reserve_in, reserve_out) = pool_after
            .reserves_for(input.token_out, input.token_in)
            .ok_or_else(|| "pool after victim does not support reverse path".to_string())?;

        let gas_estimate = if shadow_research {
            config.mev.max_gas_per_tx.min(
                config
                    .estimated_exec_gas
                    .saturating_add(config.estimated_bundle_overhead_gas)
                    .max(140_000),
            )
        } else {
            config.mev.max_gas_per_tx.min(
                config
                    .estimated_exec_gas
                    .saturating_add(config.estimated_bundle_overhead_gas)
                    .max(180_000),
            )
        };
        let gas_cost = input
            .gas_price_wei
            .saturating_mul(U256::from(gas_estimate))
            .saturating_mul(U256::from(config.mev.gas_safety_margin_bps))
            / U256::from(10_000u64);

        let sizing_fractions: &[u64] = if shadow_research {
            &[25, 50, 100, 200, 350, 500, 750, 1_000, 1_500]
        } else {
            &[1_000, 2_000, 3_500, 5_000, 7_500]
        };
        let swap_path = input
            .v2_swap_path
            .clone()
            .unwrap_or_else(|| vec![input.token_out, input.token_in]);
        let route_pools = if input.v2_swap_pools.is_empty() {
            vec![pool_after]
        } else {
            input.v2_swap_pools.clone()
        };
        let capital_cap_token = v2_borrow_cap_from_native(
            config,
            input.token_out,
            input.token_in,
            input.capital_available_wei,
            reserve_in,
            reserve_out,
        )
        .ok_or_else(|| {
            format!(
                "token normalization failed for borrow token {:?}: missing decimals/price metadata and pool-derived fallback unavailable profit_token={:?} reserve_in={} reserve_out={}",
                input.token_out, input.token_in, reserve_in, reserve_out
            )
        })?;
        let candidates = fee_extraction_v2_size_candidates(
            config,
            reserve_in,
            reserve_out,
            &swap_path,
            &route_pools,
            capital_cap_token,
            input.token_in,
            gas_cost,
            pool_after.fee_bps,
            sizing_fractions,
            scavenger,
        );
        let blocked_sample = best_v2_edge_metadata(
            config,
            reserve_in,
            reserve_out,
            &swap_path,
            &route_pools,
            capital_cap_token,
            pool_after.fee_bps,
            sizing_fractions,
            &input,
            post_victim.slippage_impact_bps,
            "blocked",
            "no positive gross edge",
        );
        let selected = if shadow_research {
            select_scavenger_v2_candidate(&candidates).ok_or_else(|| {
                let sample = blocked_sample.map(|sample| {
                    scavenger_shadow_sample(
                        config,
                        sample,
                        "shadow_candidate",
                        "gross edge below live threshold",
                    )
                });
                payload_error_with_edge_sample(
                    "no positive gross edge for scavenger payload",
                    sample,
                )
            })?
        } else {
            select_best_size_candidate(
                &candidates,
                input.context_priority_score,
                input.context_toxicity_score,
            )
            .ok_or_else(|| "no ROI-positive trade size after gas".to_string())?
        };
        let SizeCandidate {
            capital_fraction_bps,
            amount_in,
            amount_out,
            gross_profit_wei,
            net_profit_wei,
            self_slippage_bps,
            ..
        } = selected;
        let repayment_wei = v2_repayment_amount_in_profit_token(reserve_in, reserve_out, amount_in)
            .unwrap_or_else(U256::zero);
        let gross_profit_native_wei = gross_profit_wei;
        let net_profit_native_wei = net_profit_wei;
        let repayment_native = token_amount_to_native_f64(config, input.token_in, repayment_wei)
            .ok_or_else(|| {
                format!(
                    "token normalization failed for profit token {:?}: missing decimals/price metadata",
                    input.token_in
                )
            })?;
        let gross_edge_native = wei_to_eth_f64(gross_profit_native_wei);

        let simulated_profit_wei = if shadow_research {
            gross_profit_native_wei
        } else {
            net_profit_native_wei
        };

        let min_amount_out = amount_out.saturating_mul(U256::from(
            10_000u64.saturating_sub(effective_payload_slippage_bps(config)),
        )) / U256::from(10_000u64);
        let price_impact_bps = post_victim.slippage_impact_bps;
        let min_profit_wei = effective_payload_min_profit_wei(config)?;
        let min_profit_eth = wei_to_eth_f64(min_profit_wei);

        if !shadow_research && simulated_profit_wei < min_profit_wei {
            return Err(format!(
                "simulated profit {:.6} {} below minimum {:.6} {}",
                wei_to_eth_f64(simulated_profit_wei),
                config.native_asset_symbol(),
                min_profit_eth,
                config.native_asset_symbol()
            ));
        }

        let edge_metadata = EdgeMetadata {
            victim_tx: String::new(),
            selector: String::new(),
            status: "payload_built".to_string(),
            reason: "selected positive gross edge".to_string(),
            route_kind: "v2".to_string(),
            path: swap_path
                .iter()
                .map(|address| format!("{address:?}"))
                .collect(),
            hops: swap_path.len().saturating_sub(1) as u64,
            impacted_pools: route_pools
                .iter()
                .map(|pool| format!("{:?}", pool.pair))
                .collect(),
            slippage_window_score: 0.0,
            pool_imbalance_score: 0.0,
            cross_dex_deviation_bps: 0,
            gas_estimate,
            simulated_extraction_native: gross_edge_native,
            aggregator_type: "direct_router".to_string(),
            route_complexity: swap_path.len().saturating_sub(1) as u64,
            split_ratio_bps: 0,
            dex_sequence: vec!["v2".to_string()],
            route_inefficiency_score: 0.0,
            liquidity_distortion_score: 0.0,
            hop_profitability_rank: vec![
                format!("amount_in={amount_in}"),
                format!("amount_out={amount_out}"),
                format!("repayment={repayment_wei}"),
                format!("gross_edge_native_wei={gross_profit_native_wei}"),
                format!("route_kind=v2 fee_bps={}", pool_after.fee_bps),
            ],
            best_size_bps: capital_fraction_bps,
            amount_in_wei: amount_in.to_string(),
            amount_out_wei: amount_out.to_string(),
            gross_edge_wei: gross_profit_native_wei.to_string(),
            gross_edge_native,
            repayment_wei: repayment_wei.to_string(),
            repayment_native,
            price_impact_bps,
            self_slippage_bps,
            pool: format!("{:?}", input.pair),
            factory: format_optional_address(input.factory),
            router: format!("{:?}", input.router),
            token_in: format!("{:?}", input.token_in),
            token_out: format!("{:?}", input.token_out),
        };

        let executor = config.mev.mev_executor.ok_or_else(|| {
            payload_error_with_edge_sample(
                "MEV_EXECUTOR_ADDRESS is required to build V2 atomic payload",
                Some(scavenger_shadow_sample(
                    config,
                    edge_metadata.clone(),
                    "shadow_candidate",
                    "executor contract not configured",
                )),
            )
        })?;
        let step = EncodedSwapStep {
            router: input.router,
            path: swap_path,
            amount_in,
            min_out: min_amount_out,
        };
        let call = build_v2_flashswap_call(
            executor,
            input.pair,
            input.token_out,
            amount_in,
            min_profit_wei,
            input.token_in,
            input.recipient,
            &[step],
        );

        Ok(ExecutionPayload {
            tx: Bytes::new(),
            calldata: call.calldata,
            target_contract: call.target_contract,
            value: U256::zero(),
            pair: input.pair,
            amm_kind: AmmRouteKind::UniswapV2,
            capital_committed_wei: amount_in,
            expected_profit_wei: simulated_profit_wei,
            gas_limit: gas_estimate,
            price_impact_bps,
            profit_token: input.token_in,
            profit_recipient: input.recipient,
            context_priority_score: input.context_priority_score,
            context_toxicity_score: input.context_toxicity_score,
            edge_metadata: Some(edge_metadata),
            pool_state_before: input.state_before,
        })
    }

    pub fn build_fee_extraction_v3(
        config: &Config,
        input: FeeExtractionBuildInput,
    ) -> Result<ExecutionPayload, String> {
        let fee_tier = match &input.route_kind {
            AmmRouteKind::UniswapV3 { fee_tier, .. } => *fee_tier,
            _ => return Err("v3 payload requires UniswapV3 route kind".to_string()),
        };
        let path = match &input.route_kind {
            AmmRouteKind::UniswapV3 { path, .. } => path.clone(),
            _ => Bytes::new(),
        };
        let post_victim = StateSimulator::simulate_victim_exact_in(
            input.state_before.clone(),
            input.token_in,
            input.token_out,
            input.victim_amount_in,
        )
        .ok_or_else(|| {
            format!(
                "victim post-swap simulation failed token_in={:?} token_out={:?} amount_in={} pool={:?} state_before={:?}",
                input.token_in, input.token_out, input.victim_amount_in, input.pair, input.state_before
            )
        })?;

        let AmmState::UniswapV3(pool_after) = post_victim.state_after else {
            return Err("v3 payload requires UniswapV3 simulated state".to_string());
        };

        let scavenger = config.mev.opportunity_mode() == OpportunityMode::Scavenger;
        let shadow_research = payload_shadow_research_mode(config);
        if pool_after.initialized_ticks.is_empty() {
            return Err(format!(
                "v3_tick_data_missing_for_shadow_ev pool={:?} liquidity={} sqrtPriceX96={} current_tick={} shadow_v3_ev_blocked=true",
                pool_after.pool, pool_after.liquidity, pool_after.sqrt_price_x96, pool_after.current_tick
            ));
        }
        if post_victim.slippage_impact_bps > effective_payload_price_impact_cap_bps(config) {
            return Err(format!(
                "victim price impact too high: {}bps",
                post_victim.slippage_impact_bps
            ));
        }

        let reverse_pool = V3PoolState {
            pool: pool_after.pool,
            token0: pool_after.token0,
            token1: pool_after.token1,
            sqrt_price_x96: pool_after.sqrt_price_x96,
            liquidity: pool_after.liquidity,
            current_tick: pool_after.current_tick,
            fee_bps: pool_after.fee_bps,
            initialized_ticks: pool_after.initialized_ticks.clone(),
        };
        let gas_estimate = if shadow_research {
            config.mev.max_gas_per_tx.min(
                config
                    .estimated_exec_gas
                    .saturating_add(config.estimated_bundle_overhead_gas + 20_000)
                    .max(160_000),
            )
        } else {
            config.mev.max_gas_per_tx.min(
                config
                    .estimated_exec_gas
                    .saturating_add(config.estimated_bundle_overhead_gas + 35_000)
                    .max(210_000),
            )
        };
        let gas_cost = input
            .gas_price_wei
            .saturating_mul(U256::from(gas_estimate))
            .saturating_mul(U256::from(config.mev.gas_safety_margin_bps))
            / U256::from(10_000u64);
        let sizing_fractions: &[u64] = if shadow_research {
            &[25, 50, 100, 200, 350, 500, 750, 1_000, 1_500]
        } else {
            &[1_000, 2_000, 3_500, 5_000, 7_500]
        };
        let capital_cap_token =
            native_wei_to_token_amount(config, input.token_out, input.capital_available_wei)
                .ok_or_else(|| {
                    format!(
                "token normalization failed for borrow token {:?}: missing decimals/price metadata",
                input.token_out
            )
                })?;
        let candidates = normalized_v3_size_candidates(
            config,
            &reverse_pool,
            &input,
            capital_cap_token,
            fee_tier,
            gas_cost,
            sizing_fractions,
        );
        let selected = if shadow_research {
            select_scavenger_v3_candidate(&candidates).ok_or_else(|| {
                let sample = best_v3_edge_metadata(
                    &candidates,
                    &input,
                    post_victim.slippage_impact_bps,
                    "blocked",
                    "no positive gross v3 edge",
                )
                .map(|sample| {
                    scavenger_shadow_sample(
                        config,
                        sample,
                        "shadow_candidate",
                        "gross v3 edge below live threshold",
                    )
                });
                payload_error_with_edge_sample(
                    "no positive gross v3 edge for scavenger payload",
                    sample,
                )
            })?
        } else {
            select_best_v3_candidate(
                &candidates,
                input.context_priority_score,
                input.context_toxicity_score,
            )
            .ok_or_else(|| "no ROI-positive v3 trade size after gas".to_string())?
        };
        let NormalizedV3Candidate {
            candidate:
                V3SizeCandidate {
                    capital_fraction_bps,
                    amount_in,
                    amount_out,
                    self_slippage_bps,
                    ..
                },
            repayment_wei,
            amount_out_native_wei: _,
            repayment_native_wei: _,
            gross_profit_native_wei,
            net_profit_native_wei,
            ..
        } = selected;
        if gross_profit_native_wei.is_zero() {
            return Err("no positive normalized v3 gross edge".to_string());
        }
        let amount_out_native = token_amount_to_native_f64(config, input.token_in, amount_out)
            .ok_or_else(|| {
                format!(
                    "token normalization failed for v3 output token {:?}: missing decimals/price metadata",
                    input.token_in
                )
            })?;
        let repayment_native = token_amount_to_native_f64(config, input.token_out, repayment_wei)
            .ok_or_else(|| {
                format!(
                    "token normalization failed for v3 repayment token {:?}: missing decimals/price metadata",
                    input.token_out
                )
            })?;
        let gross_edge_native = wei_to_eth_f64(gross_profit_native_wei);

        let simulated_profit_wei = if shadow_research {
            gross_profit_native_wei
        } else {
            net_profit_native_wei
        };
        let min_amount_out = amount_out.saturating_mul(U256::from(
            10_000u64.saturating_sub(effective_payload_slippage_bps(config)),
        )) / U256::from(10_000u64);
        let price_impact_bps = post_victim.slippage_impact_bps;
        let min_profit_wei = effective_payload_min_profit_wei(config)?;
        let min_profit_eth = wei_to_eth_f64(min_profit_wei);

        if !shadow_research && simulated_profit_wei < min_profit_wei {
            return Err(format!(
                "simulated v3 profit {:.6} {} below minimum {:.6} {}",
                wei_to_eth_f64(simulated_profit_wei),
                config.native_asset_symbol(),
                min_profit_eth,
                config.native_asset_symbol()
            ));
        }

        let mut edge_metadata = EdgeMetadata {
            victim_tx: String::new(),
            selector: String::new(),
            status: "payload_built".to_string(),
            reason: "selected positive v3 gross edge".to_string(),
            route_kind: "v3".to_string(),
            path: vec![
                format!("{:?}", input.token_out),
                format!("{:?}", input.token_in),
            ],
            hops: 1,
            impacted_pools: vec![format!("{:?}", input.pair)],
            slippage_window_score: 0.0,
            pool_imbalance_score: 0.0,
            cross_dex_deviation_bps: 0,
            gas_estimate,
            simulated_extraction_native: gross_edge_native,
            aggregator_type: "direct_router".to_string(),
            route_complexity: 1,
            split_ratio_bps: 0,
            dex_sequence: vec!["v3".to_string()],
            route_inefficiency_score: 0.0,
            liquidity_distortion_score: 0.0,
            hop_profitability_rank: vec![
                format!("amount_in={amount_in}"),
                format!("amount_out={amount_out}"),
                format!("repayment={repayment_wei}"),
                format!("amount_out_native={amount_out_native:.12}"),
                format!("repayment_native={repayment_native:.12}"),
                format!("gross_edge_native_wei={gross_profit_native_wei}"),
                format!("route_kind=v3 fee_tier={fee_tier}"),
            ],
            best_size_bps: capital_fraction_bps,
            amount_in_wei: amount_in.to_string(),
            amount_out_wei: amount_out.to_string(),
            gross_edge_wei: gross_profit_native_wei.to_string(),
            gross_edge_native,
            repayment_wei: repayment_wei.to_string(),
            repayment_native,
            price_impact_bps,
            self_slippage_bps,
            pool: format!("{:?}", input.pair),
            factory: format_optional_address(input.factory),
            router: format!("{:?}", input.router),
            token_in: format!("{:?}", input.token_in),
            token_out: format!("{:?}", input.token_out),
        };

        if shadow_research {
            if !config.allow_send {
                edge_metadata.status = "v3_shadow_ready".to_string();
                edge_metadata.reason = format!(
                    "{} unit_safe=true shadow_payload_built=true allow_send=false normalized_net_after_gas={}",
                    edge_metadata.reason,
                    wei_to_eth_f64(net_profit_native_wei)
                );
            } else if scavenger {
                if net_profit_native_wei.is_zero() {
                    return Err(payload_error_with_edge_sample(
                        "v3 scavenger payload blocked for live send: normalized net edge is zero",
                        Some(edge_metadata),
                    ));
                }
                edge_metadata.status = "v3_live_ready".to_string();
                edge_metadata.reason = format!(
                    "{} normalized_units=true live_send_allowed=true",
                    edge_metadata.reason
                );
            }
        }
        let executor = config.mev.mev_executor_v3.or(config.mev.mev_executor).ok_or_else(|| {
            payload_error_with_edge_sample(
                "MEV_EXECUTOR_V3_ADDRESS or MEV_EXECUTOR_ADDRESS is required to build V3 atomic payload",
                Some(scavenger_shadow_sample(
                    config,
                    edge_metadata.clone(),
                    "shadow_candidate",
                    "executor contract not configured",
                )),
            )
        })?;
        let step = EncodedV3SwapStep {
            router: input.router,
            path: path.clone(),
            amount_in,
            min_out: min_amount_out,
        };
        let call = build_v3_flashswap_call(
            executor,
            input.pair,
            input.token_out,
            amount_in,
            fee_tier,
            min_profit_wei,
            input.token_in,
            input.recipient,
            &[step],
        );

        Ok(ExecutionPayload {
            tx: Bytes::new(),
            calldata: call.calldata,
            target_contract: call.target_contract,
            value: U256::zero(),
            pair: input.pair,
            amm_kind: AmmRouteKind::UniswapV3 { fee_tier, path },
            capital_committed_wei: amount_in,
            expected_profit_wei: simulated_profit_wei,
            gas_limit: gas_estimate,
            price_impact_bps,
            profit_token: input.token_in,
            profit_recipient: input.recipient,
            context_priority_score: input.context_priority_score,
            context_toxicity_score: input.context_toxicity_score,
            edge_metadata: Some(edge_metadata),
            pool_state_before: input.state_before,
        })
    }
}

fn payload_error_with_edge_sample(reason: &str, sample: Option<EdgeMetadata>) -> String {
    let Some(sample) = sample else {
        return reason.to_string();
    };
    match serde_json::to_string(&sample) {
        Ok(json) => format!("{reason} | edge_sample={json}"),
        Err(_) => reason.to_string(),
    }
}

fn payload_shadow_research_mode(config: &Config) -> bool {
    config.mev.opportunity_mode() == OpportunityMode::Scavenger
        || (config.mev.opportunity_mode() == OpportunityMode::Aggressive && !config.allow_send)
}

fn scavenger_shadow_sample(
    config: &Config,
    mut sample: EdgeMetadata,
    status: &str,
    reason: &str,
) -> EdgeMetadata {
    if payload_shadow_research_mode(config)
        && sample.gross_edge_native >= -scavenger_shadow_negative_tolerance_native(config)
    {
        sample.status = status.to_string();
        sample.reason = reason.to_string();
    }
    sample
}

fn scavenger_shadow_negative_tolerance_native(config: &Config) -> f64 {
    let tolerance_usd = std::env::var("MEV_SCAVENGER_SHADOW_NEGATIVE_TOLERANCE_USD")
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .unwrap_or(15.0)
        .abs();
    tolerance_usd / config.mev.eth_usd_price.max(1.0)
}

fn v3_flash_repayment_wei(amount: U256, fee_tier: u32) -> U256 {
    if amount.is_zero() {
        return U256::zero();
    }
    let fee_denominator = U256::from(1_000_000u64);
    let fee = amount
        .saturating_mul(U256::from(fee_tier))
        .saturating_add(fee_denominator - U256::one())
        / fee_denominator;
    amount.saturating_add(fee)
}

#[derive(Debug, Clone, Copy)]
struct NormalizedV3Candidate {
    candidate: V3SizeCandidate,
    repayment_wei: U256,
    amount_out_native_wei: U256,
    repayment_native_wei: U256,
    gross_profit_native_wei: U256,
    net_profit_native_wei: U256,
    edge_positive: bool,
    edge_abs_native_wei: U256,
}

fn normalized_v3_size_candidates(
    config: &Config,
    pool: &V3PoolState,
    input: &FeeExtractionBuildInput,
    capital_cap: U256,
    fee_tier: u32,
    gas_cost_wei: U256,
    fractions_bps: &[u64],
) -> Vec<NormalizedV3Candidate> {
    if capital_cap.is_zero() {
        return Vec::new();
    }

    let mut candidates = Vec::with_capacity(fractions_bps.len());
    for &bps in fractions_bps {
        let amount_in = capital_cap.saturating_mul(U256::from(bps)) / U256::from(10_000u64);
        if amount_in.is_zero() {
            continue;
        }
        let Some((_, result)) = pool.simulate_exact_in(input.token_out, input.token_in, amount_in)
        else {
            continue;
        };
        let repayment_wei = v3_flash_repayment_wei(amount_in, fee_tier);
        let Some(amount_out_native_wei) =
            token_amount_to_native_wei(config, input.token_in, result.amount_out)
        else {
            continue;
        };
        let Some(repayment_native_wei) =
            token_amount_to_native_wei(config, input.token_out, repayment_wei)
        else {
            continue;
        };

        let (edge_positive, edge_abs_native_wei, gross_profit_native_wei) =
            if amount_out_native_wei >= repayment_native_wei {
                let edge = amount_out_native_wei.saturating_sub(repayment_native_wei);
                (true, edge, edge)
            } else {
                (
                    false,
                    repayment_native_wei.saturating_sub(amount_out_native_wei),
                    U256::zero(),
                )
            };
        let net_profit_native_wei = gross_profit_native_wei.saturating_sub(gas_cost_wei);
        let roi_bps = if repayment_native_wei.is_zero() {
            0
        } else {
            (net_profit_native_wei.saturating_mul(U256::from(10_000u64)) / repayment_native_wei)
                .min(U256::from(u64::MAX))
                .as_u64()
        };

        candidates.push(NormalizedV3Candidate {
            candidate: V3SizeCandidate {
                capital_fraction_bps: bps,
                amount_in,
                amount_out: result.amount_out,
                gross_profit_wei: gross_profit_native_wei,
                net_profit_wei: net_profit_native_wei,
                roi_bps,
                self_slippage_bps: result.price_impact_bps,
            },
            repayment_wei,
            amount_out_native_wei,
            repayment_native_wei,
            gross_profit_native_wei,
            net_profit_native_wei,
            edge_positive,
            edge_abs_native_wei,
        });
    }
    candidates
}

fn select_scavenger_v3_candidate(
    candidates: &[NormalizedV3Candidate],
) -> Option<NormalizedV3Candidate> {
    candidates
        .iter()
        .copied()
        .filter(|candidate| {
            !candidate.candidate.amount_in.is_zero()
                && candidate.edge_positive
                && !candidate.gross_profit_native_wei.is_zero()
                && candidate.candidate.self_slippage_bps <= 2_500
        })
        .max_by_key(|candidate| candidate.gross_profit_native_wei)
}

fn select_best_v3_candidate(
    candidates: &[NormalizedV3Candidate],
    context_priority_score: f64,
    context_toxicity_score: f64,
) -> Option<NormalizedV3Candidate> {
    let priority = context_priority_score.clamp(0.0, 1.5);
    let toxicity = context_toxicity_score.clamp(0.0, 1.0);
    candidates
        .iter()
        .copied()
        .filter(|candidate| {
            candidate.edge_positive
                && !candidate.net_profit_native_wei.is_zero()
                && candidate.candidate.self_slippage_bps <= 2_500
        })
        .max_by(|left, right| {
            normalized_v3_sizing_score(*left, priority, toxicity)
                .total_cmp(&normalized_v3_sizing_score(*right, priority, toxicity))
        })
}

fn normalized_v3_sizing_score(
    candidate: NormalizedV3Candidate,
    context_priority_score: f64,
    context_toxicity_score: f64,
) -> f64 {
    let net_profit = u256_to_f64(candidate.net_profit_native_wei).unwrap_or(0.0);
    let roi_component = candidate.candidate.roi_bps as f64 / 10_000.0;
    let size_component = candidate.candidate.capital_fraction_bps as f64 / 10_000.0;
    let slippage_penalty = candidate.candidate.self_slippage_bps as f64 / 10_000.0;
    net_profit
        * (1.0 + context_priority_score * 0.18)
        * (1.0 + roi_component * 0.42)
        * (1.0 + size_component * 0.10)
        * (1.0 - context_toxicity_score * 0.46)
        * (1.0 - slippage_penalty * 0.62)
}

fn format_optional_address(address: Option<Address>) -> String {
    address
        .map(|address| format!("{address:?}"))
        .unwrap_or_else(|| "unknown".to_string())
}

fn best_v2_edge_metadata(
    config: &Config,
    borrow_reserve: U256,
    profit_reserve: U256,
    route_path: &[Address],
    route_pools: &[V2PoolState],
    capital_cap: U256,
    fee_bps: u64,
    fractions_bps: &[u64],
    input: &FeeExtractionBuildInput,
    price_impact_bps: u64,
    status: &str,
    reason: &str,
) -> Option<EdgeMetadata> {
    let mut best: Option<(bool, U256, EdgeMetadata)> = None;
    for &bps in fractions_bps {
        let amount_in = capital_cap.saturating_mul(U256::from(bps)) / U256::from(10_000u64);
        if amount_in.is_zero() || amount_in >= borrow_reserve {
            continue;
        }
        let Some(amount_out) = quote_v2_route_exact_in(amount_in, route_path, route_pools, fee_bps)
        else {
            continue;
        };
        let Some(repayment) =
            v2_repayment_amount_in_profit_token(borrow_reserve, profit_reserve, amount_in)
        else {
            continue;
        };
        let (positive, edge_abs, gross_edge_wei, gross_edge_native) = if amount_out >= repayment {
            let edge = amount_out.saturating_sub(repayment);
            let Some(edge_native_wei) = token_amount_to_native_wei(config, input.token_in, edge)
            else {
                continue;
            };
            (
                true,
                edge,
                edge_native_wei.to_string(),
                wei_to_eth_f64(edge_native_wei),
            )
        } else {
            let edge = repayment.saturating_sub(amount_out);
            let Some(edge_native_wei) = token_amount_to_native_wei(config, input.token_in, edge)
            else {
                continue;
            };
            (
                false,
                edge,
                format!("-{edge_native_wei}"),
                -wei_to_eth_f64(edge_native_wei),
            )
        };
        let Some(repayment_native) = token_amount_to_native_f64(config, input.token_in, repayment)
        else {
            continue;
        };
        let self_slippage_bps = crate::mev::amm::uniswap_v2::price_impact_bps(
            amount_in,
            amount_out,
            borrow_reserve,
            profit_reserve,
        );
        let sample = EdgeMetadata {
            victim_tx: String::new(),
            selector: String::new(),
            status: status.to_string(),
            reason: reason.to_string(),
            route_kind: "v2".to_string(),
            path: route_path
                .iter()
                .map(|address| format!("{address:?}"))
                .collect(),
            hops: route_path.len().saturating_sub(1) as u64,
            impacted_pools: route_pools
                .iter()
                .map(|pool| format!("{:?}", pool.pair))
                .collect(),
            slippage_window_score: 0.0,
            pool_imbalance_score: 0.0,
            cross_dex_deviation_bps: 0,
            gas_estimate: 0,
            simulated_extraction_native: gross_edge_native,
            aggregator_type: "direct_router".to_string(),
            route_complexity: route_path.len().saturating_sub(1) as u64,
            split_ratio_bps: 0,
            dex_sequence: vec!["v2".to_string()],
            route_inefficiency_score: 0.0,
            liquidity_distortion_score: 0.0,
            hop_profitability_rank: Vec::new(),
            best_size_bps: bps,
            amount_in_wei: amount_in.to_string(),
            amount_out_wei: amount_out.to_string(),
            gross_edge_wei,
            gross_edge_native,
            repayment_wei: repayment.to_string(),
            repayment_native,
            price_impact_bps,
            self_slippage_bps,
            pool: format!("{:?}", input.pair),
            factory: format_optional_address(input.factory),
            router: format!("{:?}", input.router),
            token_in: format!("{:?}", input.token_in),
            token_out: format!("{:?}", input.token_out),
        };

        let replace = match &best {
            None => true,
            Some((best_positive, best_abs, _)) => {
                (positive && !*best_positive)
                    || (positive == *best_positive
                        && if positive {
                            edge_abs > *best_abs
                        } else {
                            edge_abs < *best_abs
                        })
            }
        };
        if replace {
            best = Some((positive, edge_abs, sample));
        }
    }
    best.map(|(_, _, sample)| sample)
}

fn best_v3_edge_metadata(
    candidates: &[NormalizedV3Candidate],
    input: &FeeExtractionBuildInput,
    price_impact_bps: u64,
    status: &str,
    reason: &str,
) -> Option<EdgeMetadata> {
    let candidate = candidates.iter().max_by(|left, right| {
        let left_positive_score = if left.edge_positive { 1 } else { 0 };
        let right_positive_score = if right.edge_positive { 1 } else { 0 };
        left_positive_score
            .cmp(&right_positive_score)
            .then_with(|| {
                if left.edge_positive {
                    left.edge_abs_native_wei.cmp(&right.edge_abs_native_wei)
                } else {
                    right.edge_abs_native_wei.cmp(&left.edge_abs_native_wei)
                }
            })
    })?;
    let fee_tier = match &input.route_kind {
        AmmRouteKind::UniswapV3 { fee_tier, .. } => *fee_tier,
        _ => 0,
    };
    let gross_edge_wei = if candidate.edge_positive {
        candidate.gross_profit_native_wei.to_string()
    } else {
        format!("-{}", candidate.edge_abs_native_wei)
    };
    let gross_edge_native = if candidate.edge_positive {
        wei_to_eth_f64(candidate.gross_profit_native_wei)
    } else {
        -wei_to_eth_f64(candidate.edge_abs_native_wei)
    };
    let amount_out_native = wei_to_eth_f64(candidate.amount_out_native_wei);
    let repayment_native = wei_to_eth_f64(candidate.repayment_native_wei);
    Some(EdgeMetadata {
        victim_tx: String::new(),
        selector: String::new(),
        status: status.to_string(),
        reason: reason.to_string(),
        route_kind: "v3".to_string(),
        path: vec![
            format!("{:?}", input.token_out),
            format!("{:?}", input.token_in),
        ],
        hops: 1,
        impacted_pools: vec![format!("{:?}", input.pair)],
        slippage_window_score: 0.0,
        pool_imbalance_score: 0.0,
        cross_dex_deviation_bps: 0,
        gas_estimate: 0,
        simulated_extraction_native: gross_edge_native,
        aggregator_type: "direct_router".to_string(),
        route_complexity: 1,
        split_ratio_bps: 0,
        dex_sequence: vec!["v3".to_string()],
        route_inefficiency_score: 0.0,
        liquidity_distortion_score: 0.0,
        hop_profitability_rank: vec![
            format!("amount_in={}", candidate.candidate.amount_in),
            format!("amount_out={}", candidate.candidate.amount_out),
            format!("repayment={}", candidate.repayment_wei),
            format!("amount_out_native={amount_out_native:.12}"),
            format!("repayment_native={repayment_native:.12}"),
            format!(
                "gross_edge_native_wei={}",
                candidate.gross_profit_native_wei
            ),
            format!("edge_positive={}", candidate.edge_positive),
            format!("route_kind=v3 fee_tier={fee_tier}"),
        ],
        best_size_bps: candidate.candidate.capital_fraction_bps,
        amount_in_wei: candidate.candidate.amount_in.to_string(),
        amount_out_wei: candidate.candidate.amount_out.to_string(),
        gross_edge_wei,
        gross_edge_native,
        repayment_wei: candidate.repayment_wei.to_string(),
        repayment_native,
        price_impact_bps,
        self_slippage_bps: candidate.candidate.self_slippage_bps,
        pool: format!("{:?}", input.pair),
        factory: format_optional_address(input.factory),
        router: format!("{:?}", input.router),
        token_in: format!("{:?}", input.token_in),
        token_out: format!("{:?}", input.token_out),
    })
}

fn select_scavenger_v2_candidate(candidates: &[SizeCandidate]) -> Option<SizeCandidate> {
    candidates
        .iter()
        .copied()
        .filter(|candidate| {
            !candidate.amount_in.is_zero()
                && !candidate.gross_profit_wei.is_zero()
                && candidate.self_slippage_bps <= 2_500
        })
        .max_by_key(|candidate| candidate.gross_profit_wei)
}

fn fee_extraction_v2_size_candidates(
    config: &Config,
    borrow_reserve: U256,
    profit_reserve: U256,
    route_path: &[Address],
    route_pools: &[V2PoolState],
    capital_cap: U256,
    profit_token: Address,
    gas_cost_wei: U256,
    fee_bps: u64,
    fractions_bps: &[u64],
    scavenger: bool,
) -> Vec<SizeCandidate> {
    if capital_cap.is_zero() || borrow_reserve.is_zero() || profit_reserve.is_zero() {
        return Vec::new();
    }

    let mut candidates = Vec::with_capacity(fractions_bps.len());
    for &bps in fractions_bps {
        let amount_in = capital_cap.saturating_mul(U256::from(bps)) / U256::from(10_000u64);
        if amount_in.is_zero() || amount_in >= borrow_reserve {
            continue;
        }
        let Some(amount_out) = quote_v2_route_exact_in(amount_in, route_path, route_pools, fee_bps)
        else {
            continue;
        };
        let Some(repayment_in_profit_token) =
            v2_repayment_amount_in_profit_token(borrow_reserve, profit_reserve, amount_in)
        else {
            continue;
        };
        let gross_profit_token = amount_out.saturating_sub(repayment_in_profit_token);
        let Some(gross_native_wei) =
            token_amount_to_native_wei(config, profit_token, gross_profit_token)
        else {
            continue;
        };
        let net_native_wei = if scavenger {
            gross_native_wei
        } else {
            gross_native_wei.saturating_sub(gas_cost_wei)
        };
        if net_native_wei.is_zero() {
            continue;
        }
        let Some(repayment_native_wei) =
            token_amount_to_native_wei(config, profit_token, repayment_in_profit_token)
        else {
            continue;
        };
        let roi_bps = if repayment_in_profit_token.is_zero() {
            0
        } else {
            (net_native_wei.saturating_mul(U256::from(10_000u64)) / repayment_native_wei)
                .min(U256::from(u64::MAX))
                .as_u64()
        };
        candidates.push(SizeCandidate {
            capital_fraction_bps: bps,
            amount_in,
            amount_out,
            gross_profit_wei: gross_native_wei,
            net_profit_wei: net_native_wei,
            roi_bps,
            self_slippage_bps: crate::mev::amm::uniswap_v2::price_impact_bps(
                amount_in,
                amount_out,
                borrow_reserve,
                profit_reserve,
            ),
        });
    }
    candidates
}

fn quote_v2_route_exact_in(
    amount_in: U256,
    route_path: &[Address],
    route_pools: &[V2PoolState],
    fallback_fee_bps: u64,
) -> Option<U256> {
    if route_path.len() < 2 || route_pools.len() + 1 != route_path.len() {
        return None;
    }

    let mut amount = amount_in;
    for (idx, pool) in route_pools.iter().enumerate() {
        let token_in = route_path[idx];
        let token_out = route_path[idx + 1];
        let (reserve_in, reserve_out) = pool.reserves_for(token_in, token_out)?;
        amount = amount_out_exact_in(
            amount,
            reserve_in,
            reserve_out,
            if pool.fee_bps == 0 {
                fallback_fee_bps
            } else {
                pool.fee_bps
            },
        )?;
    }
    Some(amount)
}

fn v2_repayment_amount_in_profit_token(
    borrow_reserve: U256,
    profit_reserve: U256,
    borrowed_amount: U256,
) -> Option<U256> {
    if borrowed_amount.is_zero()
        || borrow_reserve.is_zero()
        || profit_reserve.is_zero()
        || borrowed_amount >= borrow_reserve
    {
        return None;
    }

    let numerator = profit_reserve
        .saturating_mul(borrowed_amount)
        .saturating_mul(U256::from(1_000u64));
    let denominator = borrow_reserve
        .saturating_sub(borrowed_amount)
        .saturating_mul(U256::from(997u64));
    if denominator.is_zero() {
        None
    } else {
        Some((numerator / denominator).saturating_add(U256::one()))
    }
}

fn effective_payload_min_profit_wei(config: &Config) -> Result<U256, String> {
    if config.mev.opportunity_mode() == OpportunityMode::Scavenger {
        Ok(U256::from(1u64))
    } else {
        ethers::utils::parse_ether(config.mev.effective_min_net_profit_eth().to_string())
            .map_err(|err| err.to_string())
    }
}

fn effective_payload_slippage_bps(config: &Config) -> u64 {
    if config.mev.opportunity_mode() == OpportunityMode::Scavenger {
        config
            .mev
            .slippage_protection_bps
            .saturating_mul(8)
            .clamp(100, 1_500)
    } else {
        config.mev.slippage_protection_bps
    }
}

fn token_metadata(config: &Config, token: Address) -> Option<&MonitoredTokenConfig> {
    config
        .monitored_tokens
        .iter()
        .find(|candidate| candidate.address == token)
}

fn u256_to_f64(value: U256) -> Option<f64> {
    value.to_string().parse::<f64>().ok()
}

fn f64_to_u256_floor(value: f64) -> Option<U256> {
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    U256::from_dec_str(&format!("{:.0}", value.floor())).ok()
}

fn token_amount_to_native_f64(config: &Config, token: Address, amount: U256) -> Option<f64> {
    let metadata = token_metadata(config, token)?;
    let raw = u256_to_f64(amount)?;
    let units = raw / 10f64.powi(i32::from(metadata.decimals));
    Some(units * metadata.price_eth)
}

fn token_amount_to_native_wei(config: &Config, token: Address, amount: U256) -> Option<U256> {
    let native = token_amount_to_native_f64(config, token, amount)?;
    f64_to_u256_floor(native * 1e18)
}

fn v2_borrow_cap_from_native(
    config: &Config,
    borrow_token: Address,
    profit_token: Address,
    native_wei: U256,
    borrow_reserve: U256,
    profit_reserve: U256,
) -> Option<U256> {
    if let Some(amount) = native_wei_to_token_amount(config, borrow_token, native_wei) {
        return Some(amount);
    }
    if borrow_reserve.is_zero() || profit_reserve.is_zero() {
        return None;
    }
    let profit_amount = native_wei_to_token_amount(config, profit_token, native_wei)?;
    Some(profit_amount.saturating_mul(borrow_reserve) / profit_reserve)
}

fn native_wei_to_token_amount(config: &Config, token: Address, native_wei: U256) -> Option<U256> {
    let metadata = token_metadata(config, token)?;
    if metadata.price_eth <= 0.0 {
        return None;
    }
    let native = wei_to_eth_f64(native_wei);
    let token_units = native / metadata.price_eth;
    f64_to_u256_floor(token_units * 10f64.powi(i32::from(metadata.decimals)))
}

fn effective_payload_price_impact_cap_bps(config: &Config) -> u64 {
    if config.mev.opportunity_mode() == OpportunityMode::Scavenger {
        config
            .mev
            .effective_max_price_impact_bps()
            .saturating_mul(12)
            .clamp(600, 3_000)
    } else {
        config.mev.effective_max_price_impact_bps()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        Config, MevConfig, MonitoredTokenConfig, OpportunityMode, OpportunityThresholds,
        RpcPreference,
    };
    use crate::mev::amm::uniswap_v3::V3Tick;
    use ethers::types::I256;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::path::PathBuf;
    use std::sync::{Arc, RwLock};

    fn test_config() -> Config {
        Config {
            wallets: PathBuf::from("keys.txt"),
            network: "polygon".to_string(),
            chain_id: 137,
            allow_send: false,
            tenderly_rpc_only: false,
            alchemy_keys: Vec::new(),
            infura_ids: Vec::new(),
            flashbots_relay: String::new(),
            builder_relays: Vec::new(),
            executor_private_key:
                "0x59c6995e998f97a5a0044966f0945382d7a7d4f6d8f1f0db6b90e6a2f17d5f52".to_string(),
            executor_address: Address::from_low_u64_be(10),
            vault_address: Address::from_low_u64_be(11),
            profit_address: Address::from_low_u64_be(12),
            control_address: Address::from_low_u64_be(13),
            monitored_tokens: vec![MonitoredTokenConfig {
                address: Address::from_low_u64_be(1),
                decimals: 18,
                price_eth: 1.0,
            }],
            estimated_exec_gas: 250_000,
            estimated_bundle_overhead_gas: 25_000,
            max_infura_endpoints: 0,
            rpc_read_preference: RpcPreference::Auto,
            rpc_send_preference: RpcPreference::Auto,
            storage_path: PathBuf::from("test.sqlite"),
            dashboard_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8787),
            explicit_rpc_urls: Vec::new(),
            mempool_ws_urls: Vec::new(),
            mev: MevConfig {
                enabled: true,
                opportunity_mode: Arc::new(RwLock::new(OpportunityMode::Scavenger)),
                runtime_thresholds: Arc::new(RwLock::new(OpportunityThresholds {
                    min_large_swap_eth: 1.0,
                    min_net_profit_eth: 0.0001,
                    min_profit_usd: 0.01,
                    min_liquidity_eth: 1.0,
                })),
                capital_eth: 0.1,
                capital_window_secs: 90,
                max_window_exposure_eth: 0.3,
                max_cluster_window_exposure_eth: 0.2,
                max_pair_window_exposure_eth: 0.2,
                min_net_profit_eth: 0.0001,
                min_roi_bps: 100,
                min_large_swap_eth: 1.0,
                gas_safety_margin_bps: 11_000,
                max_pending_age_ms: 1500,
                max_gas_per_tx: 260_000,
                max_gas_price_gwei: Some(100),
                max_price_impact_bps: 250,
                slippage_protection_bps: 50,
                min_profit_usd: 0.01,
                eth_usd_price: 0.09,
                min_liquidity_eth: 1.0,
                latency_trace: false,
                latency_trace_warn_us: 5_000,
                pool_state_cache_ttl_ms: 120,
                executor_min_buffer_eth: 0.1,
                executor_target_buffer_eth: 0.3,
                executor_max_buffer_eth: 1.0,
                relay_fanout_count: 1,
                rpc_fanout_count: 1,
                gas_overpay_base_extra_bps: 500,
                gas_overpay_miss_extra_bps: 2_500,
                gas_overpay_revert_extra_bps: 1_200,
                gas_overpay_submit_failure_extra_bps: 1_500,
                gas_overpay_max_extra_bps: 5_000,
                finality_confirmations: 1,
                stop_loss_consecutive_losses: 3,
                stop_loss_freeze_secs: 300,
                context_stop_loss_consecutive_losses: 2,
                context_stop_loss_freeze_secs: 180,
                capital_multiplier_aggressive: 2.0,
                capital_multiplier_neutral: 1.0,
                capital_multiplier_defensive: 0.3,
                capital_multiplier_priority_threshold: 0.6,
                capital_multiplier_toxicity_threshold: 0.65,
                uniswap_v2_factory: Some(Address::from_low_u64_be(20)),
                uniswap_v3_factory: Some(Address::from_low_u64_be(21)),
                mev_executor: Some(Address::from_low_u64_be(22)),
                mev_executor_v3: Some(Address::from_low_u64_be(23)),
            },
        }
    }

    #[test]
    fn scavenger_v3_stays_blocked_until_repayment_model_is_unit_safe() {
        let mut config = test_config();
        config.allow_send = true;
        let token_in = Address::from_low_u64_be(1);
        let token_out = Address::from_low_u64_be(2);
        config.monitored_tokens.push(MonitoredTokenConfig {
            address: token_out,
            decimals: 18,
            price_eth: 1.0,
        });
        let pool = V3PoolState {
            pool: Address::from_low_u64_be(30),
            token0: token_in,
            token1: token_out,
            sqrt_price_x96: U256::from_dec_str("79228162514264337593543950336").unwrap(),
            liquidity: U256::from(1_000_000_000_000_000_000u128),
            current_tick: 0,
            fee_bps: 5,
            initialized_ticks: Vec::new(),
        };

        let err = PayloadBuilder::build_fee_extraction_v3(
            &config,
            FeeExtractionBuildInput {
                router: Address::from_low_u64_be(40),
                factory: Some(Address::from_low_u64_be(21)),
                pair: pool.pool,
                recipient: Address::from_low_u64_be(12),
                token_in,
                token_out,
                victim_amount_in: U256::from(1_000u64),
                state_before: AmmState::UniswapV3(pool),
                capital_available_wei: U256::from(10_000u64),
                gas_price_wei: U256::from(1_000_000_000u64),
                context_priority_score: 0.5,
                context_toxicity_score: 0.5,
                route_kind: AmmRouteKind::UniswapV3 {
                    fee_tier: 500,
                    path: Bytes::new(),
                },
                v2_swap_path: None,
                v2_swap_pools: Vec::new(),
            },
        )
        .unwrap_err();

        assert!(err.contains("v3_tick_data_missing_for_shadow_ev"));
    }

    #[test]
    fn v2_borrow_cap_falls_back_to_pool_ratio_when_borrow_metadata_missing() {
        let config = test_config();
        let borrow = Address::from_low_u64_be(2);
        let profit = Address::from_low_u64_be(1);
        let cap = v2_borrow_cap_from_native(
            &config,
            borrow,
            profit,
            U256::exp10(18),
            U256::from(2_000u64),
            U256::from(1_000u64),
        )
        .unwrap();

        assert_eq!(cap, U256::from(2u64) * U256::exp10(18));
    }

    #[test]
    fn scavenger_v3_single_hop_unit_safe_builds_shadow_payload_when_send_blocked() {
        let mut config = test_config();
        config.mev.max_price_impact_bps = 6_000;
        let token_in = Address::from_low_u64_be(1);
        let token_out = Address::from_low_u64_be(2);
        config.monitored_tokens.push(MonitoredTokenConfig {
            address: token_out,
            decimals: 18,
            price_eth: 1.0,
        });
        let mut encoded_path = Vec::new();
        encoded_path.extend_from_slice(token_out.as_bytes());
        encoded_path.extend_from_slice(&500u32.to_be_bytes()[1..]);
        encoded_path.extend_from_slice(token_in.as_bytes());

        let pool = V3PoolState {
            pool: Address::from_low_u64_be(30),
            token0: token_in,
            token1: token_out,
            sqrt_price_x96: U256::from_dec_str("79228162514264337593543950336").unwrap(),
            liquidity: U256::from(1_000_000_000_000_000_000u128),
            current_tick: 0,
            fee_bps: 5,
            initialized_ticks: vec![V3Tick {
                index: 100_000,
                liquidity_net: I256::zero(),
            }],
        };

        let payload = PayloadBuilder::build_fee_extraction_v3(
            &config,
            FeeExtractionBuildInput {
                router: Address::from_low_u64_be(40),
                factory: Some(Address::from_low_u64_be(21)),
                pair: pool.pool,
                recipient: Address::from_low_u64_be(12),
                token_in,
                token_out,
                victim_amount_in: U256::from(150_000_000_000_000_000u128),
                state_before: AmmState::UniswapV3(pool),
                capital_available_wei: U256::from(100_000_000_000_000u128),
                gas_price_wei: U256::zero(),
                context_priority_score: 0.5,
                context_toxicity_score: 0.5,
                route_kind: AmmRouteKind::UniswapV3 {
                    fee_tier: 500,
                    path: Bytes::from(encoded_path),
                },
                v2_swap_path: None,
                v2_swap_pools: Vec::new(),
            },
        )
        .expect("unit-safe v3 single-hop should build a shadow payload while ALLOW_SEND=false");

        let sample = payload.edge_metadata.expect("edge metadata");
        assert_eq!(sample.status, "v3_shadow_ready");
        assert!(sample.reason.contains("unit_safe=true"));
        assert!(sample.reason.contains("shadow_payload_built=true"));
        assert!(payload.expected_profit_wei > U256::zero());
    }
}
