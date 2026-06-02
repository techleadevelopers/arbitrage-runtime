use crate::mev::execution::payload_builder::ExecutionPayload;
use ethers::types::{Address, Transaction, TxHash, U256};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct MevOpportunity {
    pub detected_at: Instant,
    pub victim_tx: TxHash,
    pub victim_transaction: Option<Transaction>,
    pub execution_payload: Option<ExecutionPayload>,
    pub router: Address,
    pub token_in: Address,
    pub token_out: Address,
    pub selector: [u8; 4],
    pub preferred_relay: Option<String>,
}

impl MevOpportunity {
    pub fn age_ms(&self) -> u128 {
        self.detected_at.elapsed().as_millis()
    }
}

pub fn wei_to_eth_f64(wei: U256) -> f64 {
    wei.to_string().parse::<f64>().unwrap_or(0.0) / 1e18
}

pub fn roi_bps(profit_wei: U256, cost_wei: U256) -> u64 {
    if cost_wei.is_zero() {
        return 0;
    }
    let profit = profit_wei.to_string().parse::<f64>().unwrap_or(0.0);
    let cost = cost_wei.to_string().parse::<f64>().unwrap_or(0.0);
    if !profit.is_finite() || !cost.is_finite() || cost <= 0.0 {
        return 0;
    }
    ((profit / cost) * 10_000.0).min(u64::MAX as f64) as u64
}
