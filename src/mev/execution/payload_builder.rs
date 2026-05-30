#![allow(dead_code)]

// Arquivo: src/mev/execution/payload_builder.rs

use crate::config::{Config, OpportunityMode};
use crate::mev::amm::uniswap_v2::{
    amount_out_exact_in, select_best_size_candidate, SizeCandidate, V2PoolState,
};
use crate::mev::amm::uniswap_v3::{
    select_best_v3_size_candidate, size_candidates_v3, V3PoolState, V3SizeCandidate,
};
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
        .ok_or_else(|| "victim post-swap simulation failed".to_string())?;

        let AmmState::UniswapV2(pool_after) = post_victim.state_after else {
            return Err("v2 payload requires UniswapV2 simulated state".to_string());
        };

        let scavenger = config.mev.opportunity_mode() == OpportunityMode::Scavenger;
        if post_victim.slippage_impact_bps > effective_payload_price_impact_cap_bps(config) {
            return Err(format!(
                "victim price impact too high: {}bps",
                post_victim.slippage_impact_bps
            ));
        }

        let (reserve_in, reserve_out) = pool_after
            .reserves_for(input.token_out, input.token_in)
            .ok_or_else(|| "pool after victim does not support reverse path".to_string())?;

        let gas_estimate = if scavenger {
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

        let sizing_fractions: &[u64] = if scavenger {
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
        let candidates = fee_extraction_v2_size_candidates(
            reserve_in,
            reserve_out,
            &swap_path,
            &route_pools,
            input.capital_available_wei,
            gas_cost,
            pool_after.fee_bps,
            sizing_fractions,
            scavenger,
        );
        let blocked_sample = best_v2_edge_metadata(
            reserve_in,
            reserve_out,
            &swap_path,
            &route_pools,
            input.capital_available_wei,
            pool_after.fee_bps,
            sizing_fractions,
            &input,
            post_victim.slippage_impact_bps,
            "blocked",
            "no positive gross edge",
        );
        let selected = if scavenger {
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
        let simulated_profit_wei = if scavenger {
            gross_profit_wei
        } else {
            net_profit_wei
        };

        let repayment_wei = v2_repayment_amount_in_profit_token(reserve_in, reserve_out, amount_in)
            .unwrap_or_else(U256::zero);
        let min_amount_out = amount_out.saturating_mul(U256::from(
            10_000u64.saturating_sub(effective_payload_slippage_bps(config)),
        )) / U256::from(10_000u64);
        let price_impact_bps = post_victim.slippage_impact_bps;
        let min_profit_wei = effective_payload_min_profit_wei(config)?;
        let min_profit_eth = wei_to_eth_f64(min_profit_wei);

        if !scavenger && simulated_profit_wei < min_profit_wei {
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
            simulated_extraction_native: wei_to_eth_f64(gross_profit_wei),
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
                format!("gross_edge={gross_profit_wei}"),
                format!("route_kind=v2 fee_bps={}", pool_after.fee_bps),
            ],
            best_size_bps: capital_fraction_bps,
            amount_in_wei: amount_in.to_string(),
            amount_out_wei: amount_out.to_string(),
            gross_edge_wei: gross_profit_wei.to_string(),
            gross_edge_native: wei_to_eth_f64(gross_profit_wei),
            repayment_wei: repayment_wei.to_string(),
            repayment_native: wei_to_eth_f64(repayment_wei),
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
        .ok_or_else(|| "victim post-swap simulation failed".to_string())?;

        let AmmState::UniswapV3(pool_after) = post_victim.state_after else {
            return Err("v3 payload requires UniswapV3 simulated state".to_string());
        };

        let scavenger = config.mev.opportunity_mode() == OpportunityMode::Scavenger;
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
        let gas_estimate = if scavenger {
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
        let scavenger_zero_gas = U256::zero();
        let sizing_fractions: &[u64] = if scavenger {
            &[25, 50, 100, 200, 350, 500, 750, 1_000, 1_500]
        } else {
            &[1_000, 2_000, 3_500, 5_000, 7_500]
        };
        let candidates = size_candidates_v3(
            &reverse_pool,
            input.token_out,
            input.token_in,
            input.capital_available_wei,
            if scavenger {
                scavenger_zero_gas
            } else {
                gas_cost
            },
            sizing_fractions,
        );
        let selected = if scavenger {
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
            select_best_v3_size_candidate(
                &candidates,
                input.context_priority_score,
                input.context_toxicity_score,
            )
            .ok_or_else(|| "no ROI-positive v3 trade size after gas".to_string())?
        };
        let V3SizeCandidate {
            capital_fraction_bps,
            amount_in,
            amount_out,
            gross_profit_wei,
            net_profit_wei,
            self_slippage_bps,
            ..
        } = selected;
        let simulated_profit_wei = if scavenger {
            gross_profit_wei
        } else {
            net_profit_wei
        };
        let min_amount_out = amount_out.saturating_mul(U256::from(
            10_000u64.saturating_sub(effective_payload_slippage_bps(config)),
        )) / U256::from(10_000u64);
        let price_impact_bps = post_victim.slippage_impact_bps;
        let min_profit_wei = effective_payload_min_profit_wei(config)?;
        let min_profit_eth = wei_to_eth_f64(min_profit_wei);

        if !scavenger && simulated_profit_wei < min_profit_wei {
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
            simulated_extraction_native: wei_to_eth_f64(gross_profit_wei),
            aggregator_type: "direct_router".to_string(),
            route_complexity: 1,
            split_ratio_bps: 0,
            dex_sequence: vec!["v3".to_string()],
            route_inefficiency_score: 0.0,
            liquidity_distortion_score: 0.0,
            hop_profitability_rank: vec![
                format!("amount_in={amount_in}"),
                format!("amount_out={amount_out}"),
                format!("repayment={amount_in}"),
                format!("gross_edge={gross_profit_wei}"),
                format!("route_kind=v3 fee_tier={fee_tier}"),
            ],
            best_size_bps: capital_fraction_bps,
            amount_in_wei: amount_in.to_string(),
            amount_out_wei: amount_out.to_string(),
            gross_edge_wei: gross_profit_wei.to_string(),
            gross_edge_native: wei_to_eth_f64(gross_profit_wei),
            repayment_wei: amount_in.to_string(),
            repayment_native: wei_to_eth_f64(amount_in),
            price_impact_bps,
            self_slippage_bps,
            pool: format!("{:?}", input.pair),
            factory: format_optional_address(input.factory),
            router: format!("{:?}", input.router),
            token_in: format!("{:?}", input.token_in),
            token_out: format!("{:?}", input.token_out),
        };

        if scavenger {
            let shadow_metrics = v3_scavenger_shadow_metrics(
                amount_in,
                amount_out,
                fee_tier,
                gas_cost,
                path.len() == 43,
            );
            edge_metadata = v3_scavenger_shadow_sample(edge_metadata, shadow_metrics);

            // NOVO: Permite shadow mesmo se não for unit-safe (telemetria)
            if !config.allow_send {
                // Modo shadow: aceita qualquer V3 para coleta de dados
                edge_metadata.status = "v3_shadow_ready".to_string();
                edge_metadata.reason = format!(
                    "{} shadow_payload_built=true allow_send=false unit_safe={} net_after_gas={}",
                    edge_metadata.reason,
                    shadow_metrics.unit_safe,
                    wei_to_eth_f64(shadow_metrics.net_after_gas)
                );
                // Continua para construir o payload shadow
            } else {
                // Modo live: só permite se for unit-safe e com lucro positivo
                if !shadow_metrics.unit_safe || shadow_metrics.net_after_gas.is_zero() {
                    return Err(payload_error_with_edge_sample(
                "v3 scavenger payload blocked for live send until repayment model is unit-safe",
                Some(edge_metadata),
            ));
                }
                edge_metadata.status = "v3_live_ready".to_string();
                edge_metadata.reason = format!(
                    "{} unit_safe=true live_send_allowed=true",
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

fn scavenger_shadow_sample(
    config: &Config,
    mut sample: EdgeMetadata,
    status: &str,
    reason: &str,
) -> EdgeMetadata {
    if config.mev.opportunity_mode() == OpportunityMode::Scavenger
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

#[derive(Debug, Clone, Copy)]
struct V3ScavengerShadowMetrics {
    repayment: U256,
    gross_edge: U256,
    net_after_gas: U256,
    unit_safe: bool,
}

fn v3_scavenger_shadow_metrics(
    amount_in: U256,
    amount_out: U256,
    fee_tier: u32,
    gas_cost: U256,
    single_hop_path: bool,
) -> V3ScavengerShadowMetrics {
    let repayment = v3_flash_repayment_wei(amount_in, fee_tier);
    let gross_edge = amount_out.saturating_sub(repayment);
    let net_after_gas = gross_edge.saturating_sub(gas_cost);
    let unit_safe = single_hop_path && !repayment.is_zero() && amount_out >= repayment;

    V3ScavengerShadowMetrics {
        repayment,
        gross_edge,
        net_after_gas,
        unit_safe,
    }
}

fn v3_scavenger_shadow_sample(
    mut sample: EdgeMetadata,
    metrics: V3ScavengerShadowMetrics,
) -> EdgeMetadata {
    sample.status = "v3_shadow".to_string();
    sample.reason = format!(
        "v3_shadow_repayment_wei={} v3_shadow_gross_edge={} v3_shadow_net_after_gas={} unit_safe={}",
        metrics.repayment, metrics.gross_edge, metrics.net_after_gas, metrics.unit_safe
    );
    sample.simulated_extraction_native = wei_to_eth_f64(metrics.gross_edge);
    sample.gross_edge_wei = metrics.gross_edge.to_string();
    sample.gross_edge_native = wei_to_eth_f64(metrics.gross_edge);
    sample.repayment_wei = metrics.repayment.to_string();
    sample.repayment_native = wei_to_eth_f64(metrics.repayment);
    sample.hop_profitability_rank = vec![
        format!("v3_shadow_repayment_wei={}", metrics.repayment),
        format!("v3_shadow_gross_edge={}", metrics.gross_edge),
        format!("v3_shadow_net_after_gas={}", metrics.net_after_gas),
        format!("unit_safe={}", metrics.unit_safe),
    ];
    sample
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

fn format_optional_address(address: Option<Address>) -> String {
    address
        .map(|address| format!("{address:?}"))
        .unwrap_or_else(|| "unknown".to_string())
}

fn best_v2_edge_metadata(
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
            (true, edge, edge.to_string(), wei_to_eth_f64(edge))
        } else {
            let edge = repayment.saturating_sub(amount_out);
            (false, edge, format!("-{edge}"), -wei_to_eth_f64(edge))
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
            repayment_native: wei_to_eth_f64(repayment),
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
    candidates: &[V3SizeCandidate],
    input: &FeeExtractionBuildInput,
    price_impact_bps: u64,
    status: &str,
    reason: &str,
) -> Option<EdgeMetadata> {
    let candidate = candidates
        .iter()
        .max_by_key(|candidate| candidate.gross_profit_wei)?;
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
        simulated_extraction_native: wei_to_eth_f64(candidate.gross_profit_wei),
        aggregator_type: "direct_router".to_string(),
        route_complexity: 1,
        split_ratio_bps: 0,
        dex_sequence: vec!["v3".to_string()],
        route_inefficiency_score: 0.0,
        liquidity_distortion_score: 0.0,
        hop_profitability_rank: vec![
            format!("amount_in={}", candidate.amount_in),
            format!("amount_out={}", candidate.amount_out),
            format!("repayment={}", candidate.amount_in),
            format!("gross_edge={}", candidate.gross_profit_wei),
            "route_kind=v3".to_string(),
        ],
        best_size_bps: candidate.capital_fraction_bps,
        amount_in_wei: candidate.amount_in.to_string(),
        amount_out_wei: candidate.amount_out.to_string(),
        gross_edge_wei: candidate.gross_profit_wei.to_string(),
        gross_edge_native: wei_to_eth_f64(candidate.gross_profit_wei),
        repayment_wei: candidate.amount_in.to_string(),
        repayment_native: wei_to_eth_f64(candidate.amount_in),
        price_impact_bps,
        self_slippage_bps: candidate.self_slippage_bps,
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
    borrow_reserve: U256,
    profit_reserve: U256,
    route_path: &[Address],
    route_pools: &[V2PoolState],
    capital_cap: U256,
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
        let gross = amount_out.saturating_sub(repayment_in_profit_token);
        let net = if scavenger {
            gross
        } else {
            gross.saturating_sub(gas_cost_wei)
        };
        if net.is_zero() {
            continue;
        }
        let roi_bps = if repayment_in_profit_token.is_zero() {
            0
        } else {
            (net.saturating_mul(U256::from(10_000u64)) / repayment_in_profit_token)
                .min(U256::from(u64::MAX))
                .as_u64()
        };
        candidates.push(SizeCandidate {
            capital_fraction_bps: bps,
            amount_in,
            amount_out,
            gross_profit_wei: gross,
            net_profit_wei: net,
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
        Some(numerator / denominator + U256::from(1u64))
    }
}

fn select_scavenger_v3_candidate(candidates: &[V3SizeCandidate]) -> Option<V3SizeCandidate> {
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

        assert!(err.contains(
            "v3 scavenger payload blocked for live send until repayment model is unit-safe"
        ));
    }

    #[test]
    fn scavenger_v3_single_hop_unit_safe_builds_shadow_payload_when_send_blocked() {
        let mut config = test_config();
        config.mev.max_price_impact_bps = 6_000;
        let token_in = Address::from_low_u64_be(1);
        let token_out = Address::from_low_u64_be(2);
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
            initialized_ticks: Vec::new(),
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
