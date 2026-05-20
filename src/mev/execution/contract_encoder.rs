use ethers::abi::{self, Token};
use ethers::types::{Address, Bytes, U256};

const START_V2_FLASH_SWAP: [u8; 4] = [0x00, 0x00, 0x00, 0x00];
const START_V3_FLASH_SWAP: [u8; 4] = [0x00, 0x00, 0x00, 0x01];

#[derive(Debug, Clone)]
pub struct EncodedSwapStep {
    pub router: Address,
    pub path: Vec<Address>,
    pub amount_in: U256,
    pub min_out: U256,
}

#[derive(Debug, Clone)]
pub struct EncodedV3SwapStep {
    pub router: Address,
    pub path: Bytes,
    pub amount_in: U256,
    pub min_out: U256,
}

pub fn encode_start_v2_flash_swap(
    pair: Address,
    borrow_token: Address,
    borrow_amount: U256,
    min_profit: U256,
    profit_token: Address,
    profit_recipient: Address,
    steps: &[EncodedSwapStep],
) -> Bytes {
    let selector = selector(
        "startV2FlashSwap(address,address,uint256,uint256,address,address,(address,address[],uint256,uint256)[])",
    );
    encode_with_selector(
        selector,
        &[
            Token::Address(pair),
            Token::Address(borrow_token),
            Token::Uint(borrow_amount),
            Token::Uint(min_profit),
            Token::Address(profit_token),
            Token::Address(profit_recipient),
            Token::Array(steps.iter().map(step_token).collect()),
        ],
    )
}

pub fn encode_start_v3_flash_swap(
    pool: Address,
    borrow_token: Address,
    borrow_amount: U256,
    fee_tier: u32,
    min_profit: U256,
    profit_token: Address,
    profit_recipient: Address,
    steps: &[EncodedV3SwapStep],
) -> Bytes {
    let selector = selector(
        "startV3FlashSwap(address,address,uint256,uint24,uint256,address,address,(address,bytes,uint256,uint256)[])",
    );
    encode_with_selector(
        selector,
        &[
            Token::Address(pool),
            Token::Address(borrow_token),
            Token::Uint(borrow_amount),
            Token::Uint(U256::from(fee_tier)),
            Token::Uint(min_profit),
            Token::Address(profit_token),
            Token::Address(profit_recipient),
            Token::Array(steps.iter().map(v3_step_token).collect()),
        ],
    )
}

fn step_token(step: &EncodedSwapStep) -> Token {
    Token::Tuple(vec![
        Token::Address(step.router),
        Token::Array(step.path.iter().copied().map(Token::Address).collect()),
        Token::Uint(step.amount_in),
        Token::Uint(step.min_out),
    ])
}

fn v3_step_token(step: &EncodedV3SwapStep) -> Token {
    Token::Tuple(vec![
        Token::Address(step.router),
        Token::Bytes(step.path.to_vec()),
        Token::Uint(step.amount_in),
        Token::Uint(step.min_out),
    ])
}

fn encode_with_selector(selector: [u8; 4], tokens: &[Token]) -> Bytes {
    let mut data = Vec::with_capacity(4 + 32 * tokens.len());
    data.extend_from_slice(&selector);
    data.extend(abi::encode(tokens));
    Bytes::from(data)
}

fn selector(signature: &str) -> [u8; 4] {
    let hash = ethers::utils::keccak256(signature.as_bytes());
    [hash[0], hash[1], hash[2], hash[3]]
}

#[allow(dead_code)]
fn _selector_placeholders() -> [u8; 4] {
    [
        START_V2_FLASH_SWAP[0] ^ START_V3_FLASH_SWAP[0],
        START_V2_FLASH_SWAP[1] ^ START_V3_FLASH_SWAP[1],
        START_V2_FLASH_SWAP[2] ^ START_V3_FLASH_SWAP[2],
        START_V2_FLASH_SWAP[3] ^ START_V3_FLASH_SWAP[3],
    ]
}
