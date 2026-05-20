// Arquivo: src/mev/execution/flashloan_builder.rs

use crate::mev::execution::contract_encoder::{
    encode_start_v2_flash_swap, encode_start_v3_flash_swap, EncodedSwapStep, EncodedV3SwapStep,
};
use ethers::types::{Address, Bytes, U256};

#[derive(Debug, Clone)]
pub struct V2FlashSwapCall {
    pub target_contract: Address,
    pub calldata: Bytes,
}

#[derive(Debug, Clone)]
pub struct V3FlashSwapCall {
    pub target_contract: Address,
    pub calldata: Bytes,
}

pub fn build_v2_flashswap_call(
    executor: Address,
    pair: Address,
    borrow_token: Address,
    borrow_amount: U256,
    min_profit: U256,
    profit_token: Address,
    profit_recipient: Address,
    steps: &[EncodedSwapStep],
) -> V2FlashSwapCall {
    V2FlashSwapCall {
        target_contract: executor,
        calldata: encode_start_v2_flash_swap(
            pair,
            borrow_token,
            borrow_amount,
            min_profit,
            profit_token,
            profit_recipient,
            steps,
        ),
    }
}

pub fn build_v3_flashswap_call(
    executor: Address,
    pool: Address,
    borrow_token: Address,
    borrow_amount: U256,
    fee_tier: u32,
    min_profit: U256,
    profit_token: Address,
    profit_recipient: Address,
    steps: &[EncodedV3SwapStep],
) -> V3FlashSwapCall {
    V3FlashSwapCall {
        target_contract: executor,
        calldata: encode_start_v3_flash_swap(
            pool,
            borrow_token,
            borrow_amount,
            fee_tier,
            min_profit,
            profit_token,
            profit_recipient,
            steps,
        ),
    }
}
