use clap::Parser;
use ethers::signers::{LocalWallet, Signer};
use ethers::types::Address;
use serde::Deserialize;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "fee-extraction-engine")]
#[command(author = "GhostInject")]
#[command(version = "1.0")]
#[command(about = "Deterministic fee extraction engine for mempool AMM swaps")]
struct Cli {
    #[arg(short, long, default_value = "keys.txt")]
    wallets: PathBuf,

    #[arg(long, default_value = "ethereum")]
    network: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub wallets: PathBuf,
    pub network: String,
    pub chain_id: u64,
    pub allow_send: bool,
    pub tenderly_rpc_only: bool,
    pub alchemy_keys: Vec<String>,
    pub infura_ids: Vec<String>,
    pub flashbots_relay: String,
    pub builder_relays: Vec<String>,
    pub executor_private_key: String,
    pub executor_address: Address,
    pub vault_address: Address,
    pub profit_address: Address,
    pub control_address: Address,
    pub monitored_tokens: Vec<MonitoredTokenConfig>,
    pub estimated_exec_gas: u64,
    pub estimated_bundle_overhead_gas: u64,
    pub max_infura_endpoints: usize,
    pub rpc_read_preference: RpcPreference,
    pub rpc_send_preference: RpcPreference,
    pub storage_path: PathBuf,
    pub dashboard_addr: SocketAddr,
    pub mempool_ws_url: Option<String>,
    pub mev: MevConfig,
}

#[derive(Debug, Clone)]
pub struct MevConfig {
    pub enabled: bool,
    pub capital_eth: f64,
    pub capital_window_secs: u64,
    pub max_window_exposure_eth: f64,
    pub max_cluster_window_exposure_eth: f64,
    pub max_pair_window_exposure_eth: f64,
    pub min_net_profit_eth: f64,
    pub min_roi_bps: u64,
    pub min_large_swap_eth: f64,
    pub gas_safety_margin_bps: u64,
    pub max_pending_age_ms: u64,
    pub max_gas_per_tx: u64,
    pub max_price_impact_bps: u64,
    pub slippage_protection_bps: u64,
    pub min_profit_usd: f64,
    pub eth_usd_price: f64,
    pub min_liquidity_eth: f64,
    pub executor_min_buffer_eth: f64,
    pub executor_target_buffer_eth: f64,
    pub executor_max_buffer_eth: f64,
    pub uniswap_v2_factory: Option<Address>,
    pub uniswap_v3_factory: Option<Address>,
    pub mev_executor: Option<Address>,
}

#[derive(Debug, Clone)]
pub struct MonitoredTokenConfig {
    pub address: Address,
    pub decimals: u8,
    pub price_eth: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcPreference {
    Auto,
    Alchemy,
    Infura,
}

impl RpcPreference {
    pub fn as_str(self) -> &'static str {
        match self {
            RpcPreference::Auto => "auto",
            RpcPreference::Alchemy => "alchemy",
            RpcPreference::Infura => "infura",
        }
    }
}

impl Config {
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        dotenvy::dotenv().ok();

        let cli = Cli::parse();
        let network = env::var("NETWORK")
            .ok()
            .map(|value| value.trim().to_lowercase())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| cli.network.to_lowercase());
        let tenderly_rpc_only = env::var("USE_TENDERLY_RPC_ONLY")
            .unwrap_or_else(|_| "false".to_string())
            .trim()
            .eq_ignore_ascii_case("true");
        let allow_send = env::var("ALLOW_SEND")
            .unwrap_or_else(|_| "false".to_string())
            .trim()
            .eq_ignore_ascii_case("true");
        let alchemy_keys = if tenderly_rpc_only {
            parse_alchemy_keys()
        } else {
            let keys = parse_alchemy_keys();
            if keys.is_empty() {
                return Err("environment variable ALCHEMY_KEY is not configured".into());
            }
            keys
        };
        let flashbots_relay = if network == "ethereum" {
            required_env("FLASHBOTS_RELAY")?
        } else {
            env::var("FLASHBOTS_RELAY")
                .unwrap_or_default()
                .trim()
                .to_string()
        };
        let builder_relays = parse_builder_relays(&flashbots_relay);
        let chain_id = env::var("CHAIN_ID")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or_else(|| default_chain_id(&network));
        let executor_private_key = env::var("EXECUTOR_PRIVATE_KEY")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !is_placeholder(value))
            .or_else(|| {
                env::var("SENDER_PRIVATE_KEY")
                    .ok()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !is_placeholder(value))
            })
            .or_else(|| {
                env::var("CONTROL_PRIVATE_KEY")
                    .ok()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !is_placeholder(value))
            })
            .ok_or(
                "environment variable EXECUTOR_PRIVATE_KEY, SENDER_PRIVATE_KEY or CONTROL_PRIVATE_KEY is not configured",
            )?;
        let executor_address = executor_private_key
            .parse::<LocalWallet>()?
            .address();
        let control_address = required_env("CONTROL_ADDRESS")?.parse::<Address>()?;
        let vault_address = env::var("VAULT_ADDRESS")
            .ok()
            .map(|value| value.trim().parse::<Address>())
            .transpose()?
            .unwrap_or(control_address);
        let profit_address = env::var("PROFIT_ADDRESS")
            .ok()
            .map(|value| value.trim().parse::<Address>())
            .transpose()?
            .or_else(|| {
                env::var("MEV_SEARCHER_RECIPIENT")
                    .ok()
                    .map(|value| value.trim().parse::<Address>())
                    .transpose()
                    .ok()
                    .flatten()
            })
            .unwrap_or(control_address);
        let monitored_tokens = parse_monitored_tokens(&network)?;
        let estimated_exec_gas = env::var("ESTIMATED_EXEC_GAS")
            .ok()
            .map(|value| value.trim().parse::<u64>())
            .transpose()?
            .unwrap_or(250_000);
        let estimated_bundle_overhead_gas = env::var("ESTIMATED_BUNDLE_OVERHEAD_GAS")
            .unwrap_or_else(|_| "25000".to_string())
            .parse::<u64>()?;
        let max_infura_endpoints = env::var("MAX_INFURA_ENDPOINTS")
            .unwrap_or_else(|_| "2".to_string())
            .parse::<usize>()?;
        let rpc_read_preference = parse_rpc_preference(
            env::var("RPC_READ_PREFERENCE")
                .unwrap_or_else(|_| "auto".to_string())
                .trim(),
        )?;
        let rpc_send_preference = parse_rpc_preference(
            env::var("RPC_SEND_PREFERENCE")
                .unwrap_or_else(|_| "auto".to_string())
                .trim(),
        )?;
        let storage_path = env::var("STORAGE_PATH")
            .unwrap_or_else(|_| "bot_state.sqlite".to_string())
            .into();
        let dashboard_addr = env::var("DASHBOARD_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:8787".to_string())
            .parse::<SocketAddr>()?;
        let mempool_ws_url = env::var("MEMPOOL_WS_URL")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let mev = MevConfig {
            enabled: env::var("MEV_ENGINE_ENABLED")
                .unwrap_or_else(|_| "false".to_string())
                .trim()
                .eq_ignore_ascii_case("true"),
            capital_eth: env::var("MEV_CAPITAL_ETH")
                .unwrap_or_else(|_| "0.05".to_string())
                .parse::<f64>()?,
            capital_window_secs: env::var("MEV_CAPITAL_WINDOW_SECS")
                .unwrap_or_else(|_| "90".to_string())
                .parse::<u64>()?,
            max_window_exposure_eth: env::var("MEV_MAX_WINDOW_EXPOSURE_ETH")
                .unwrap_or_else(|_| "0.15".to_string())
                .parse::<f64>()?,
            max_cluster_window_exposure_eth: env::var("MEV_MAX_CLUSTER_WINDOW_EXPOSURE_ETH")
                .unwrap_or_else(|_| "0.08".to_string())
                .parse::<f64>()?,
            max_pair_window_exposure_eth: env::var("MEV_MAX_PAIR_WINDOW_EXPOSURE_ETH")
                .unwrap_or_else(|_| "0.10".to_string())
                .parse::<f64>()?,
            min_net_profit_eth: env::var("MEV_MIN_NET_PROFIT_ETH")
                .unwrap_or_else(|_| "0.0025".to_string())
                .parse::<f64>()?,
            min_roi_bps: env::var("MEV_MIN_ROI_BPS")
                .unwrap_or_else(|_| "1500".to_string())
                .parse::<u64>()?,
            min_large_swap_eth: env::var("MEV_MIN_LARGE_SWAP_ETH")
                .unwrap_or_else(|_| "25.0".to_string())
                .parse::<f64>()?,
            gas_safety_margin_bps: env::var("MEV_GAS_SAFETY_MARGIN_BPS")
                .unwrap_or_else(|_| "12500".to_string())
                .parse::<u64>()?,
            max_pending_age_ms: env::var("MEV_MAX_PENDING_AGE_MS")
                .unwrap_or_else(|_| "1500".to_string())
                .parse::<u64>()?,
            max_gas_per_tx: env::var("MEV_MAX_GAS_PER_TX")
                .unwrap_or_else(|_| "260000".to_string())
                .parse::<u64>()?,
            max_price_impact_bps: env::var("MEV_MAX_PRICE_IMPACT_BPS")
                .unwrap_or_else(|_| "250".to_string())
                .parse::<u64>()?,
            slippage_protection_bps: env::var("MEV_SLIPPAGE_PROTECTION_BPS")
                .unwrap_or_else(|_| "50".to_string())
                .parse::<u64>()?,
            min_profit_usd: env::var("MEV_MIN_PROFIT_USD")
                .unwrap_or_else(|_| "2.0".to_string())
                .parse::<f64>()?,
            eth_usd_price: env::var("MEV_ETH_USD_PRICE")
                .unwrap_or_else(|_| "3200.0".to_string())
                .parse::<f64>()?,
            min_liquidity_eth: env::var("MEV_MIN_LIQUIDITY_ETH")
                .unwrap_or_else(|_| "25.0".to_string())
                .parse::<f64>()?,
            executor_min_buffer_eth: env::var("MEV_EXECUTOR_MIN_BUFFER_ETH")
                .unwrap_or_else(|_| "0.20".to_string())
                .parse::<f64>()?,
            executor_target_buffer_eth: env::var("MEV_EXECUTOR_TARGET_BUFFER_ETH")
                .unwrap_or_else(|_| "0.50".to_string())
                .parse::<f64>()?,
            executor_max_buffer_eth: env::var("MEV_EXECUTOR_MAX_BUFFER_ETH")
                .unwrap_or_else(|_| "1.00".to_string())
                .parse::<f64>()?,
            uniswap_v2_factory: env::var("MEV_UNISWAP_V2_FACTORY")
                .ok()
                .map(|value| value.trim().parse::<Address>())
                .transpose()?,
            uniswap_v3_factory: env::var("MEV_UNISWAP_V3_FACTORY")
                .ok()
                .map(|value| value.trim().parse::<Address>())
                .transpose()?,
            mev_executor: env::var("MEV_EXECUTOR_ADDRESS")
                .ok()
                .map(|value| value.trim().parse::<Address>())
                .transpose()?,
        };

        let mut infura_ids = Vec::new();
        for idx in 1..=10 {
            if let Ok(value) = env::var(format!("INFURA_ID_{idx}")) {
                let trimmed = value.trim();
                if !trimmed.is_empty() {
                    infura_ids.push(trimmed.to_string());
                }
            }
        }

        Ok(Self {
            wallets: cli.wallets,
            network,
            chain_id,
            allow_send,
            tenderly_rpc_only,
            alchemy_keys,
            infura_ids,
            flashbots_relay,
            builder_relays,
            executor_private_key,
            executor_address,
            vault_address,
            profit_address,
            control_address,
            monitored_tokens,
            estimated_exec_gas,
            estimated_bundle_overhead_gas,
            max_infura_endpoints,
            rpc_read_preference,
            rpc_send_preference,
            storage_path,
            dashboard_addr,
            mempool_ws_url,
            mev,
        })
    }

    pub fn rpc_urls(&self) -> Vec<(String, String)> {
        if self.tenderly_rpc_only {
            return self
                .fork_rpc_url()
                .map(|url| vec![(format!("tenderly-{}", self.network), url)])
                .unwrap_or_default();
        }

        let mut urls = Vec::with_capacity(self.infura_ids.len() + self.alchemy_keys.len());
        for (idx, alchemy_key) in self.alchemy_keys.iter().enumerate() {
            if let Some(alchemy_url) = alchemy_url_for_network(&self.network, alchemy_key) {
                urls.push((format!("alchemy-{}", idx + 1), alchemy_url));
            }
        }

        for (idx, infura_id) in self
            .infura_ids
            .iter()
            .take(self.max_infura_endpoints)
            .enumerate()
        {
            if let Some(infura_url) = infura_url_for_network(&self.network, infura_id) {
                urls.push((format!("infura-{}", idx + 1), infura_url));
            }
        }

        urls
    }

    pub fn mempool_ws_url(&self) -> Option<String> {
        if self.tenderly_rpc_only {
            return self
                .mempool_ws_url
                .clone()
                .filter(|value| !value.is_empty());
        }
        self.mempool_ws_url
            .clone()
            .or_else(|| {
                self.alchemy_keys
                    .first()
                    .and_then(|key| alchemy_ws_url_for_network(&self.network, key))
            })
    }

    pub fn uses_bundle_relays(&self) -> bool {
        self.network == "ethereum" && !self.builder_relays.is_empty()
    }

    pub fn fork_rpc_url(&self) -> Option<String> {
        let key = match self.network.as_str() {
            "ethereum" => "TENDERLY_FORK_URL_ETHEREUM",
            "arbitrum" => "TENDERLY_FORK_URL_ARBITRUM",
            "bsc" => "TENDERLY_FORK_URL_BNB",
            "polygon" => "TENDERLY_FORK_URL_POLYGON",
            _ => return None,
        };
        env::var(key)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    }
}

#[derive(Debug, Deserialize)]
pub struct WalletEntry {
    pub private_key: String,
}

fn required_env(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let value = env::var(name)?;
    let trimmed = value.trim();
    if is_placeholder(trimmed) {
        return Err(format!("environment variable {name} is not configured").into());
    }
    Ok(trimmed.to_string())
}

fn parse_alchemy_keys() -> Vec<String> {
    let mut keys = Vec::new();

    if let Ok(value) = env::var("ALCHEMY_KEY") {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            keys.push(trimmed.to_string());
        }
    }

    for idx in 2..=8 {
        if let Ok(value) = env::var(format!("ALCHEMY_KEY_{idx}")) {
            let trimmed = value.trim();
            if !trimmed.is_empty()
                && !keys
                    .iter()
                    .any(|existing| existing.eq_ignore_ascii_case(trimmed))
            {
                keys.push(trimmed.to_string());
            }
        }
    }

    keys
}

fn is_placeholder(value: &str) -> bool {
    value.trim().is_empty()
        || value.trim() == "SUA_CHAVE_HEX_AQUI"
        || value.trim() == "0xSeuContratoAlvo"
}

fn default_chain_id(network: &str) -> u64 {
    match network {
        "ethereum" => 1,
        "bsc" => 56,
        "polygon" => 137,
        "arbitrum" => 42161,
        _ => 1,
    }
}

fn alchemy_url_for_network(network: &str, key: &str) -> Option<String> {
    match network {
        "ethereum" => Some(format!("https://eth-mainnet.g.alchemy.com/v2/{key}")),
        "arbitrum" => Some(format!("https://arb-mainnet.g.alchemy.com/v2/{key}")),
        "polygon" => Some(format!("https://polygon-mainnet.g.alchemy.com/v2/{key}")),
        _ => None,
    }
}

fn alchemy_ws_url_for_network(network: &str, key: &str) -> Option<String> {
    match network {
        "ethereum" => Some(format!("wss://eth-mainnet.g.alchemy.com/v2/{key}")),
        "arbitrum" => Some(format!("wss://arb-mainnet.g.alchemy.com/v2/{key}")),
        "polygon" => Some(format!("wss://polygon-mainnet.g.alchemy.com/v2/{key}")),
        _ => None,
    }
}

fn infura_url_for_network(network: &str, key: &str) -> Option<String> {
    match network {
        "ethereum" => Some(format!("https://mainnet.infura.io/v3/{key}")),
        "arbitrum" => Some(format!("https://arbitrum-mainnet.infura.io/v3/{key}")),
        "polygon" => Some(format!("https://polygon-mainnet.infura.io/v3/{key}")),
        _ => None,
    }
}

fn parse_rpc_preference(value: &str) -> Result<RpcPreference, Box<dyn std::error::Error>> {
    match value.trim().to_lowercase().as_str() {
        "auto" => Ok(RpcPreference::Auto),
        "alchemy" => Ok(RpcPreference::Alchemy),
        "infura" => Ok(RpcPreference::Infura),
        other => Err(format!("unsupported RPC preference: {other}").into()),
    }
}

fn parse_monitored_tokens(
    network: &str,
) -> Result<Vec<MonitoredTokenConfig>, Box<dyn std::error::Error>> {
    let env_name = match network {
        "arbitrum" => "MONITORED_TOKENS_ARBITRUM",
        "bsc" => "MONITORED_TOKENS_BSC",
        "polygon" => "MONITORED_TOKENS_POLYGON",
        "ethereum" => "MONITORED_TOKENS_ETHEREUM",
        _ => return Ok(Vec::new()),
    };

    let raw = env::var(env_name).unwrap_or_default();
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut tokens = Vec::new();
    for entry in raw.split(',') {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }

        let parts: Vec<&str> = trimmed.split(':').map(str::trim).collect();
        let token = match parts.as_slice() {
            [address, decimals, price_eth] => MonitoredTokenConfig {
                address: (*address).parse::<Address>()?,
                decimals: (*decimals).parse::<u8>()?,
                price_eth: (*price_eth).parse::<f64>()?,
            },
            [_symbol, address, decimals, price_eth] => MonitoredTokenConfig {
                address: (*address).parse::<Address>()?,
                decimals: (*decimals).parse::<u8>()?,
                price_eth: (*price_eth).parse::<f64>()?,
            },
            [_symbol, _class, address, decimals, price_eth] => MonitoredTokenConfig {
                address: (*address).parse::<Address>()?,
                decimals: (*decimals).parse::<u8>()?,
                price_eth: (*price_eth).parse::<f64>()?,
            },
            _ => {
                return Err(format!(
                    "invalid token entry in {env_name}: {trimmed}. expected address:decimals:price_eth, symbol:address:decimals:price_eth or symbol:class:address:decimals:price_eth"
                )
                .into())
            }
        };
        tokens.push(token);
    }

    Ok(tokens)
}

fn parse_builder_relays(primary: &str) -> Vec<String> {
    let mut relays = Vec::new();
    let primary = primary.trim();
    if !primary.is_empty() {
        relays.push(primary.to_string());
    }

    if let Ok(raw) = env::var("BUILDER_RELAYS") {
        for relay in raw.split(',').map(str::trim).filter(|value| !value.is_empty()) {
            if !relays.iter().any(|existing| existing.eq_ignore_ascii_case(relay)) {
                relays.push(relay.to_string());
            }
        }
    }

    relays
}
