use ethers::types::{Address, U256};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V2PoolState {
    pub pair: Address,
    pub token0: Address,
    pub token1: Address,
    pub reserve0: U256,
    pub reserve1: U256,
    pub fee_bps: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V2SwapResult {
    pub amount_in: U256,
    pub amount_out: U256,
    pub new_reserve_in: U256,
    pub new_reserve_out: U256,
    pub price_impact_bps: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SizeCandidate {
    pub capital_fraction_bps: u64,
    pub amount_in: U256,
    pub amount_out: U256,
    pub gross_profit_wei: U256,
    pub net_profit_wei: U256,
    pub roi_bps: u64,
    pub self_slippage_bps: u64,
}

impl V2PoolState {
    pub fn reserves_for(&self, token_in: Address, token_out: Address) -> Option<(U256, U256)> {
        if token_in == self.token0 && token_out == self.token1 {
            Some((self.reserve0, self.reserve1))
        } else if token_in == self.token1 && token_out == self.token0 {
            Some((self.reserve1, self.reserve0))
        } else {
            None
        }
    }

    pub fn apply_swap_exact_in(
        &self,
        token_in: Address,
        token_out: Address,
        amount_in: U256,
    ) -> Option<(Self, V2SwapResult)> {
        let (reserve_in, reserve_out) = self.reserves_for(token_in, token_out)?;
        if amount_in.is_zero() || reserve_in.is_zero() || reserve_out.is_zero() {
            return None;
        }

        let amount_out = amount_out_exact_in(amount_in, reserve_in, reserve_out, self.fee_bps)?;
        if amount_out.is_zero() || amount_out >= reserve_out {
            return None;
        }

        let new_reserve_in = reserve_in.saturating_add(amount_in);
        let new_reserve_out = reserve_out.saturating_sub(amount_out);
        let price_impact_bps = price_impact_bps(amount_in, amount_out, reserve_in, reserve_out);

        let mut next = *self;
        if token_in == self.token0 {
            next.reserve0 = new_reserve_in;
            next.reserve1 = new_reserve_out;
        } else {
            next.reserve1 = new_reserve_in;
            next.reserve0 = new_reserve_out;
        }

        Some((
            next,
            V2SwapResult {
                amount_in,
                amount_out,
                new_reserve_in,
                new_reserve_out,
                price_impact_bps,
            },
        ))
    }
}

pub fn amount_out_exact_in(
    amount_in: U256,
    reserve_in: U256,
    reserve_out: U256,
    fee_bps: u64,
) -> Option<U256> {
    if amount_in.is_zero() || reserve_in.is_zero() || reserve_out.is_zero() || fee_bps >= 10_000 {
        return None;
    }

    let fee_denominator = U256::from(10_000u64);
    let amount_in_with_fee = amount_in.saturating_mul(U256::from(10_000u64 - fee_bps));
    let numerator = amount_in_with_fee.saturating_mul(reserve_out);
    let denominator = reserve_in
        .saturating_mul(fee_denominator)
        .saturating_add(amount_in_with_fee);
    if denominator.is_zero() {
        return None;
    }
    Some(numerator / denominator)
}

pub fn price_impact_bps(
    amount_in: U256,
    amount_out: U256,
    reserve_in: U256,
    reserve_out: U256,
) -> u64 {
    if amount_in.is_zero() || amount_out.is_zero() || reserve_in.is_zero() || reserve_out.is_zero()
    {
        return 10_000;
    }

    let ideal_out = amount_in.saturating_mul(reserve_out) / reserve_in;
    if ideal_out.is_zero() || ideal_out <= amount_out {
        return 0;
    }

    let impact = ideal_out.saturating_sub(amount_out);
    (impact.saturating_mul(U256::from(10_000u64)) / ideal_out)
        .min(U256::from(10_000u64))
        .as_u64()
}

#[allow(dead_code)]
pub fn find_roi_optimal_input(
    reserve_in: U256,
    reserve_out: U256,
    capital_cap: U256,
    gas_cost_wei: U256,
    fee_bps: u64,
) -> Option<(U256, U256)> {
    let best = select_best_size_candidate(
        &size_candidates(
            reserve_in,
            reserve_out,
            capital_cap,
            gas_cost_wei,
            fee_bps,
            &[50, 100, 200, 350, 500, 750, 1_000, 1_500, 2_000, 2_500, 3_000],
        ),
        1.0,
        0.0,
    )?;
    Some((best.amount_in, best.net_profit_wei))
}

pub fn size_candidates(
    reserve_in: U256,
    reserve_out: U256,
    capital_cap: U256,
    gas_cost_wei: U256,
    fee_bps: u64,
    fractions_bps: &[u64],
) -> Vec<SizeCandidate> {
    if capital_cap.is_zero() || reserve_in.is_zero() || reserve_out.is_zero() {
        return Vec::new();
    }

    let mut candidates = Vec::with_capacity(fractions_bps.len());
    for &bps in fractions_bps {
        let amount_in = capital_cap.saturating_mul(U256::from(bps)) / U256::from(10_000u64);
        if amount_in.is_zero() {
            continue;
        }
        let Some(amount_out) = amount_out_exact_in(amount_in, reserve_in, reserve_out, fee_bps) else {
            continue;
        };
        let gross = amount_out.saturating_sub(amount_in);
        let net = gross.saturating_sub(gas_cost_wei);
        if net.is_zero() {
            continue;
        }
        let roi_bps = if amount_in.is_zero() {
            0
        } else {
            (net.saturating_mul(U256::from(10_000u64)) / amount_in)
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
            self_slippage_bps: price_impact_bps(amount_in, amount_out, reserve_in, reserve_out),
        });
    }
    candidates
}

pub fn select_best_size_candidate(
    candidates: &[SizeCandidate],
    context_priority_score: f64,
    context_toxicity_score: f64,
) -> Option<SizeCandidate> {
    let priority = context_priority_score.clamp(0.0, 1.5);
    let toxicity = context_toxicity_score.clamp(0.0, 1.0);
    candidates.iter().copied().max_by(|left, right| {
        sizing_score(*left, priority, toxicity).total_cmp(&sizing_score(*right, priority, toxicity))
    })
}

fn sizing_score(candidate: SizeCandidate, context_priority_score: f64, context_toxicity_score: f64) -> f64 {
    let net_profit = candidate.net_profit_wei.as_u128() as f64;
    let roi_component = candidate.roi_bps as f64 / 10_000.0;
    let size_component = candidate.capital_fraction_bps as f64 / 10_000.0;
    let slippage_penalty = candidate.self_slippage_bps as f64 / 10_000.0;
    net_profit
        * (1.0 + context_priority_score * 0.20)
        * (1.0 + roi_component * 0.45)
        * (1.0 + size_component * 0.10)
        * (1.0 - context_toxicity_score * 0.42)
        * (1.0 - slippage_penalty * 0.55)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_exact_in_respects_constant_product_fee() {
        let out = amount_out_exact_in(
            U256::from(1_000u64),
            U256::from(10_000u64),
            U256::from(10_000u64),
            30,
        )
        .unwrap();
        assert_eq!(out, U256::from(906u64));
    }

    #[test]
    fn v2_state_moves_reserves_after_swap() {
        let pool = V2PoolState {
            pair: Address::zero(),
            token0: Address::from_low_u64_be(1),
            token1: Address::from_low_u64_be(2),
            reserve0: U256::from(10_000u64),
            reserve1: U256::from(10_000u64),
            fee_bps: 30,
        };
        let (next, result) = pool
            .apply_swap_exact_in(pool.token0, pool.token1, U256::from(1_000u64))
            .unwrap();
        assert_eq!(result.amount_out, U256::from(906u64));
        assert_eq!(next.reserve0, U256::from(11_000u64));
        assert_eq!(next.reserve1, U256::from(9_094u64));
    }
}
