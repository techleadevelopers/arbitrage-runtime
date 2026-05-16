#![allow(dead_code)]

// Arquivo: src/mev/execution/payload_builder.rs

use crate::config::Config;
use crate::mev::amm::uniswap_v2::{
    amount_out_exact_in, select_best_size_candidate, size_candidates, SizeCandidate,
};
use crate::mev::amm::uniswap_v3::{
    select_best_v3_size_candidate, size_candidates_v3, V3PoolState, V3SizeCandidate,
};
use crate::mev::execution::contract_encoder::{EncodedSwapStep, EncodedV3SwapStep};
use crate::mev::execution::flashloan_builder::{build_v2_flashswap_call, build_v3_flashswap_call};
use crate::mev::opportunity::wei_to_eth_f64;
use crate::mev::simulation::state_simulator::{AmmState, StateSimulator};
use ethers::types::{Address, Bytes, U256};

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
    // NOVO: Estado do pool antes da execução para EVM preflight
    pub pool_state_before: AmmState,
}

#[derive(Debug, Clone)]
pub struct FeeExtractionBuildInput {
    pub router: Address,
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

        if post_victim.slippage_impact_bps > config.mev.max_price_impact_bps {
            return Err(format!(
                "victim price impact too high: {}bps",
                post_victim.slippage_impact_bps
            ));
        }

        let (reserve_in, reserve_out) = pool_after
            .reserves_for(input.token_out, input.token_in)
            .ok_or_else(|| "pool after victim does not support reverse path".to_string())?;

        let gas_estimate = config.mev.max_gas_per_tx.min(
            config
                .estimated_exec_gas
                .saturating_add(config.estimated_bundle_overhead_gas)
                .max(180_000),
        );
        let gas_cost = input
            .gas_price_wei
            .saturating_mul(U256::from(gas_estimate))
            .saturating_mul(U256::from(config.mev.gas_safety_margin_bps))
            / U256::from(10_000u64);

        let sizing_fractions = [1_000u64, 2_000, 3_500, 5_000, 7_500];
        let candidates = size_candidates(
            reserve_in,
            reserve_out,
            input.capital_available_wei,
            gas_cost,
            pool_after.fee_bps,
            &sizing_fractions,
        );
        let selected = select_best_size_candidate(
            &candidates,
            input.context_priority_score,
            input.context_toxicity_score,
        )
        .ok_or_else(|| "no ROI-positive trade size after gas".to_string())?;
        let SizeCandidate {
            amount_in,
            net_profit_wei: simulated_profit_wei,
            ..
        } = selected;

        let amount_out =
            amount_out_exact_in(amount_in, reserve_in, reserve_out, pool_after.fee_bps)
                .ok_or_else(|| "fee extraction output quote failed".to_string())?;
        let min_amount_out = amount_out.saturating_mul(U256::from(
            10_000u64.saturating_sub(config.mev.slippage_protection_bps),
        )) / U256::from(10_000u64);
        let price_impact_bps = post_victim.slippage_impact_bps;
        let min_profit_wei = ethers::utils::parse_ether(config.mev.min_net_profit_eth.to_string())
            .map_err(|err| err.to_string())?;

        if simulated_profit_wei < min_profit_wei {
            return Err(format!(
                "simulated profit {:.6} ETH below minimum {:.6} ETH",
                wei_to_eth_f64(simulated_profit_wei),
                config.mev.min_net_profit_eth
            ));
        }

        let executor = config.mev.mev_executor.ok_or_else(|| {
            "MEV_EXECUTOR_ADDRESS is required to build atomic payload".to_string()
        })?;
        let step = EncodedSwapStep {
            router: input.router,
            path: vec![input.token_out, input.token_in],
            amount_in: U256::MAX,
            min_out: min_amount_out,
        };
        let call = build_v2_flashswap_call(
            executor,
            input.pair,
            input.token_out,
            amount_in,
            min_profit_wei,
            input.token_in,
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

        if post_victim.slippage_impact_bps > config.mev.max_price_impact_bps {
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
        let gas_estimate = config.mev.max_gas_per_tx.min(
            config
                .estimated_exec_gas
                .saturating_add(config.estimated_bundle_overhead_gas + 35_000)
                .max(210_000),
        );
        let gas_cost = input
            .gas_price_wei
            .saturating_mul(U256::from(gas_estimate))
            .saturating_mul(U256::from(config.mev.gas_safety_margin_bps))
            / U256::from(10_000u64);
        let sizing_fractions = [1_000u64, 2_000, 3_500, 5_000, 7_500];
        let candidates = size_candidates_v3(
            &reverse_pool,
            input.token_out,
            input.token_in,
            input.capital_available_wei,
            gas_cost,
            &sizing_fractions,
        );
        let selected = select_best_v3_size_candidate(
            &candidates,
            input.context_priority_score,
            input.context_toxicity_score,
        )
        .ok_or_else(|| "no ROI-positive v3 trade size after gas".to_string())?;
        let V3SizeCandidate {
            amount_in,
            amount_out,
            net_profit_wei: simulated_profit_wei,
            ..
        } = selected;
        let min_amount_out = amount_out.saturating_mul(U256::from(
            10_000u64.saturating_sub(config.mev.slippage_protection_bps),
        )) / U256::from(10_000u64);
        let price_impact_bps = post_victim.slippage_impact_bps;
        let min_profit_wei = ethers::utils::parse_ether(config.mev.min_net_profit_eth.to_string())
            .map_err(|err| err.to_string())?;

        if simulated_profit_wei < min_profit_wei {
            return Err(format!(
                "simulated v3 profit {:.6} ETH below minimum {:.6} ETH",
                wei_to_eth_f64(simulated_profit_wei),
                config.mev.min_net_profit_eth
            ));
        }

        let executor = config
            .mev
            .mev_executor
            .ok_or_else(|| "MEV_EXECUTOR_ADDRESS is required to build atomic payload".to_string())?;
        let step = EncodedV3SwapStep {
            router: input.router,
            path: path.clone(),
            amount_in: U256::MAX,
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
            pool_state_before: input.state_before,
        })
    }
}
