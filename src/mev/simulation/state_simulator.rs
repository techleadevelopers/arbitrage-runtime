use crate::mev::amm::uniswap_v2::V2PoolState;
use ethers::types::{Address, U256};

#[derive(Debug, Clone)]
pub enum AmmState {
    UniswapV2(V2PoolState),
}

#[derive(Debug, Clone)]
pub struct PostSwapSimulation {
    pub state_after: AmmState,
    pub slippage_impact_bps: u64,
}

pub struct StateSimulator;

impl StateSimulator {
    pub fn simulate_victim_exact_in(
        state: AmmState,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
    ) -> Option<PostSwapSimulation> {
        match state {
            AmmState::UniswapV2(pool) => {
                let (next, result) = pool.apply_swap_exact_in(token_in, token_out, amount_in)?;
                Some(PostSwapSimulation {
                    state_after: AmmState::UniswapV2(next),
                    slippage_impact_bps: result.price_impact_bps,
                })
            }
        }
    }
}
