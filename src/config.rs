use clap::Parser;
use ethers::signers::{LocalWallet, Signer};
use ethers::types::{Address, U256};
use serde::Deserialize;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing::warn;

#[derive(Parser, Debug)]
#[command(name = "fee-extraction-engine")]
#[command(author = "GhostInject")]
#[command(version = "1.0")]
#[command(about = "Deterministic fee extraction engine for mempool AMM swaps")]
struct Cli {
    #[arg(short, long, default_value = "keys.txt")]
    wallets: PathBuf,

    #[arg(long, default_value = "bsc")]
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
    pub explicit_rpc_urls: Vec<(String, String)>,
    pub mempool_ws_urls: Vec<String>,
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
    pub max_gas_price_gwei: Option<u64>,
    pub max_price_impact_bps: u64,
    pub slippage_protection_bps: u64,
    pub min_profit_usd: f64,
    pub eth_usd_price: f64,
    pub min_liquidity_eth: f64,
    pub latency_trace: bool,
    pub latency_trace_warn_us: u64,
    pub pool_state_cache_ttl_ms: u64,
    pub executor_min_buffer_eth: f64,
    pub executor_target_buffer_eth: f64,
    pub executor_max_buffer_eth: f64,
    pub relay_fanout_count: usize,
    pub rpc_fanout_count: usize,
    pub gas_overpay_base_extra_bps: u64,
    pub gas_overpay_miss_extra_bps: u64,
    pub gas_overpay_revert_extra_bps: u64,
    pub gas_overpay_submit_failure_extra_bps: u64,
    pub gas_overpay_max_extra_bps: u64,
    pub stop_loss_consecutive_losses: u32,
    pub stop_loss_freeze_secs: u64,
    pub context_stop_loss_consecutive_losses: u32,
    pub context_stop_loss_freeze_secs: u64,
    pub capital_multiplier_aggressive: f64,
    pub capital_multiplier_neutral: f64,
    pub capital_multiplier_defensive: f64,
    pub capital_multiplier_priority_threshold: f64,
    pub capital_multiplier_toxicity_threshold: f64,
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
    GetBlock,
    Alchemy,
    Infura,
}

impl RpcPreference {
    pub fn as_str(self) -> &'static str {
        match self {
            RpcPreference::Auto => "auto",
            RpcPreference::GetBlock => "getblock",
            RpcPreference::Alchemy => "alchemy",
            RpcPreference::Infura => "infura",
        }
    }
}

impl Config {
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        dotenvy::dotenv().ok();
        apply_replay_tuned_runtime_env()?;

        let cli = Cli::parse();
        let network = env::var("NETWORK")
            .ok()
            .map(|value| value.trim().to_lowercase())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| cli.network.to_lowercase());
        let runtime_load_test_mode = env_flag("RUN_RUNTIME_LOAD_TEST");
        let tenderly_rpc_only = env::var("USE_TENDERLY_RPC_ONLY")
            .unwrap_or_else(|_| "false".to_string())
            .trim()
            .eq_ignore_ascii_case("true");
        let allow_send = env::var("ALLOW_SEND")
            .unwrap_or_else(|_| "false".to_string())
            .trim()
            .eq_ignore_ascii_case("true");
        let alchemy_keys = parse_alchemy_keys();
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
        let executor_private_key_candidate = env::var("EXECUTOR_PRIVATE_KEY")
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
            .or_else(|| {
                runtime_load_test_mode.then(|| {
                    "0000000000000000000000000000000000000000000000000000000000000001"
                        .to_string()
                })
            })
            .ok_or(
                "environment variable EXECUTOR_PRIVATE_KEY, SENDER_PRIVATE_KEY or CONTROL_PRIVATE_KEY is not configured",
            )?;
        let executor_private_key = if runtime_load_test_mode
            && executor_private_key_candidate
                .parse::<LocalWallet>()
                .is_err()
        {
            "0000000000000000000000000000000000000000000000000000000000000001".to_string()
        } else {
            executor_private_key_candidate
        };
        let executor_address = executor_private_key.parse::<LocalWallet>()?.address();
        let control_address = match env::var("CONTROL_ADDRESS") {
            Ok(value) => match value.trim().parse::<Address>() {
                Ok(address) => address,
                Err(err) if runtime_load_test_mode => {
                    warn!(
                        "ignoring invalid CONTROL_ADDRESS for runtime load test: {}",
                        err
                    );
                    executor_address
                }
                Err(err) => return Err(format!("invalid CONTROL_ADDRESS: {err}").into()),
            },
            Err(_) if runtime_load_test_mode => executor_address,
            Err(_) => {
                warn!(
                    "CONTROL_ADDRESS is not configured; using executor address as control address"
                );
                executor_address
            }
        };
        let vault_address = parse_optional_address_env("VAULT_ADDRESS", runtime_load_test_mode)?
            .unwrap_or(control_address);
        let profit_address = parse_optional_address_env("PROFIT_ADDRESS", runtime_load_test_mode)?
            .or(parse_optional_address_env(
                "MEV_SEARCHER_RECIPIENT",
                runtime_load_test_mode,
            )?)
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
        let rpc_read_preference = parse_network_rpc_preference(&network, "RPC_READ_PREFERENCE")?;
        let rpc_send_preference = parse_network_rpc_preference(&network, "RPC_SEND_PREFERENCE")?;
        let storage_path = env::var("STORAGE_PATH")
            .unwrap_or_else(|_| "bot_state.sqlite".to_string())
            .into();
        let dashboard_addr = env::var("PORT")
            .map(|port| format!("0.0.0.0:{port}"))
            .or_else(|_| env::var("DASHBOARD_ADDR"))
            .unwrap_or_else(|_| "127.0.0.1:8787".to_string())
            .parse::<SocketAddr>()?;
        let explicit_rpc_urls = parse_rpc_urls(&network);
        let mempool_ws_urls = parse_mempool_ws_urls(&network, tenderly_rpc_only, &alchemy_keys);
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
            max_gas_price_gwei: parse_network_optional_u64(
                &network,
                &[
                    "MEV_MAX_GAS_PRICE_GWEI",
                    "MEV_MAX_GAS_PRICE_GWEI_BSC",
                    "MEV_MAX_GAS_PRICE_GWEI_BNB",
                    "MEV_MAX_GAS_PRICE_GWEI_POLYGON",
                ],
            )?,
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
            latency_trace: env::var("MEV_LATENCY_TRACE")
                .unwrap_or_else(|_| "false".to_string())
                .trim()
                .eq_ignore_ascii_case("true"),
            latency_trace_warn_us: env::var("MEV_LATENCY_TRACE_WARN_US")
                .unwrap_or_else(|_| "5000".to_string())
                .parse::<u64>()?,
            pool_state_cache_ttl_ms: env::var("MEV_POOL_STATE_CACHE_TTL_MS")
                .unwrap_or_else(|_| "120".to_string())
                .parse::<u64>()?,
            executor_min_buffer_eth: env::var("MEV_EXECUTOR_MIN_BUFFER_ETH")
                .unwrap_or_else(|_| "0.20".to_string())
                .parse::<f64>()?,
            executor_target_buffer_eth: env::var("MEV_EXECUTOR_TARGET_BUFFER_ETH")
                .unwrap_or_else(|_| "0.50".to_string())
                .parse::<f64>()?,
            executor_max_buffer_eth: env::var("MEV_EXECUTOR_MAX_BUFFER_ETH")
                .unwrap_or_else(|_| "1.00".to_string())
                .parse::<f64>()?,
            relay_fanout_count: env::var("MEV_RELAY_FANOUT_COUNT")
                .unwrap_or_else(|_| "3".to_string())
                .parse::<usize>()?
                .max(1),
            rpc_fanout_count: env::var("MEV_RPC_FANOUT_COUNT")
                .unwrap_or_else(|_| "2".to_string())
                .parse::<usize>()?
                .max(1),
            gas_overpay_base_extra_bps: env::var("MEV_GAS_OVERPAY_BASE_EXTRA_BPS")
                .unwrap_or_else(|_| "500".to_string())
                .parse::<u64>()?,
            gas_overpay_miss_extra_bps: env::var("MEV_GAS_OVERPAY_MISS_EXTRA_BPS")
                .unwrap_or_else(|_| "2500".to_string())
                .parse::<u64>()?,
            gas_overpay_revert_extra_bps: env::var("MEV_GAS_OVERPAY_REVERT_EXTRA_BPS")
                .unwrap_or_else(|_| "1200".to_string())
                .parse::<u64>()?,
            gas_overpay_submit_failure_extra_bps: env::var(
                "MEV_GAS_OVERPAY_SUBMIT_FAILURE_EXTRA_BPS",
            )
            .unwrap_or_else(|_| "1500".to_string())
            .parse::<u64>()?,
            gas_overpay_max_extra_bps: env::var("MEV_GAS_OVERPAY_MAX_EXTRA_BPS")
                .unwrap_or_else(|_| "5000".to_string())
                .parse::<u64>()?,
            stop_loss_consecutive_losses: env::var("MEV_STOP_LOSS_CONSECUTIVE_LOSSES")
                .unwrap_or_else(|_| "3".to_string())
                .parse::<u32>()?
                .max(1),
            stop_loss_freeze_secs: env::var("MEV_STOP_LOSS_FREEZE_SECS")
                .unwrap_or_else(|_| "300".to_string())
                .parse::<u64>()?,
            context_stop_loss_consecutive_losses: env::var(
                "MEV_CONTEXT_STOP_LOSS_CONSECUTIVE_LOSSES",
            )
            .unwrap_or_else(|_| "2".to_string())
            .parse::<u32>()?
            .max(1),
            context_stop_loss_freeze_secs: env::var("MEV_CONTEXT_STOP_LOSS_FREEZE_SECS")
                .unwrap_or_else(|_| "180".to_string())
                .parse::<u64>()?,
            capital_multiplier_aggressive: env::var("MEV_CAPITAL_MULTIPLIER_AGGRESSIVE")
                .unwrap_or_else(|_| "2.0".to_string())
                .parse::<f64>()?,
            capital_multiplier_neutral: env::var("MEV_CAPITAL_MULTIPLIER_NEUTRAL")
                .unwrap_or_else(|_| "1.0".to_string())
                .parse::<f64>()?,
            capital_multiplier_defensive: env::var("MEV_CAPITAL_MULTIPLIER_DEFENSIVE")
                .unwrap_or_else(|_| "0.3".to_string())
                .parse::<f64>()?,
            capital_multiplier_priority_threshold: env::var(
                "MEV_CAPITAL_MULTIPLIER_PRIORITY_THRESHOLD",
            )
            .unwrap_or_else(|_| "0.60".to_string())
            .parse::<f64>()?,
            capital_multiplier_toxicity_threshold: env::var(
                "MEV_CAPITAL_MULTIPLIER_TOXICITY_THRESHOLD",
            )
            .unwrap_or_else(|_| "0.65".to_string())
            .parse::<f64>()?,
            uniswap_v2_factory: parse_optional_address_env(
                "MEV_UNISWAP_V2_FACTORY",
                runtime_load_test_mode,
            )?,
            uniswap_v3_factory: parse_optional_address_env(
                "MEV_UNISWAP_V3_FACTORY",
                runtime_load_test_mode,
            )?,
            mev_executor: parse_optional_address_env(
                "MEV_EXECUTOR_ADDRESS",
                runtime_load_test_mode,
            )?,
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
            explicit_rpc_urls,
            mempool_ws_urls,
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

        let mut urls = Vec::with_capacity(
            self.explicit_rpc_urls.len() + self.infura_ids.len() + self.alchemy_keys.len(),
        );
        for (name, rpc_url) in &self.explicit_rpc_urls {
            urls.push((name.clone(), rpc_url.clone()));
        }
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

        if let Some(filter) = rpc_provider_filter(&self.network) {
            urls.retain(|(name, _)| rpc_name_matches_filter(name, &filter));
        }
        urls.retain(|(name, _)| rpc_name_allowed_for_network(&self.network, name));

        urls
    }

    pub fn mempool_ws_urls(&self) -> Vec<String> {
        if self.tenderly_rpc_only {
            return self.mempool_ws_urls.clone();
        }
        if !self.mempool_ws_urls.is_empty() {
            return self.mempool_ws_urls.clone();
        }
        if !supports_default_alchemy_mempool_ws(&self.network) {
            return Vec::new();
        }
        self.alchemy_keys
            .iter()
            .filter_map(|key| alchemy_ws_url_for_network(&self.network, key))
            .collect()
    }

    pub fn uses_bundle_relays(&self) -> bool {
        self.network == "ethereum" && !self.builder_relays.is_empty()
    }

    pub fn native_asset_symbol(&self) -> &'static str {
        match self.network.as_str() {
            "bsc" | "bnb" => "BNB",
            "polygon" => "POL",
            "ethereum" => "ETH",
            "arbitrum" | "optimism" | "base" => "ETH",
            _ => "NATIVE",
        }
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

fn apply_replay_tuned_runtime_env() -> Result<(), Box<dyn std::error::Error>> {
    if !env_flag("REPLAY_AUTO_TUNE_USE_IN_RUNTIME") {
        return Ok(());
    }
    let path = env::var("REPLAY_AUTO_TUNE_RUNTIME_ENV_PATH")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            env::var("REPLAY_AUTO_TUNE_APPLY_PATH")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
        })
        .unwrap_or_else(|| PathBuf::from("exports").join("replay_auto_tune.env"));
    if !path.exists() {
        warn!(
            "runtime auto-tune env requested but file does not exist: {}",
            path.display()
        );
        return Ok(());
    }
    let raw = fs::read_to_string(&path)?;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim();
        if key.is_empty() || value.is_empty() {
            continue;
        }
        unsafe {
            env::set_var(key, value);
        }
    }
    Ok(())
}

impl MevConfig {
    pub fn max_gas_price_wei(&self) -> Option<U256> {
        self.max_gas_price_gwei
            .map(|value| U256::from(value).saturating_mul(U256::from(1_000_000_000u64)))
    }

    pub fn contextual_capital_multiplier(&self, priority_score: f64, toxicity_score: f64) -> f64 {
        if toxicity_score >= self.capital_multiplier_toxicity_threshold {
            self.capital_multiplier_defensive
        } else if priority_score >= self.capital_multiplier_priority_threshold
            && toxicity_score <= self.capital_multiplier_toxicity_threshold * 0.5
        {
            self.capital_multiplier_aggressive
        } else {
            self.capital_multiplier_neutral
        }
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

fn parse_mempool_ws_urls(
    network: &str,
    tenderly_rpc_only: bool,
    alchemy_keys: &[String],
) -> Vec<String> {
    let mut urls: Vec<String> = Vec::new();

    for key in prioritized_network_keys(
        network,
        &[
            "MEMPOOL_WS_URL",
            "MEMPOOL_WS_URL_2",
            "MEMPOOL_WS_URL_3",
            "MEMPOOL_WS_URL_4",
            "MEMPOOL_WS_URL_5",
            "MEMPOOL_WS_URL_6",
            "MEMPOOL_WS_URL_BSC",
            "MEMPOOL_WS_URL_BNB",
            "MEMPOOL_WS_URL_POLYGON",
        ],
    ) {
        if let Ok(value) = env::var(key) {
            for item in value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .filter(|value| is_allowed_mempool_ws_url(network, value))
            {
                if !urls
                    .iter()
                    .any(|existing: &String| existing.eq_ignore_ascii_case(item))
                {
                    urls.push(item.to_string());
                }
            }
        }
    }

    if urls.is_empty() && !tenderly_rpc_only && supports_default_alchemy_mempool_ws(network) {
        for key in alchemy_keys {
            if let Some(ws_url) = alchemy_ws_url_for_network(network, key) {
                urls.push(ws_url);
            }
        }
    }

    urls
}

fn parse_rpc_urls(network: &str) -> Vec<(String, String)> {
    let mut urls = Vec::new();
    push_rpc_url_entries(
        network,
        "rpc",
        &[
            "RPC_URL",
            "RPC_URL_2",
            "RPC_URL_3",
            "RPC_URL_4",
            "RPC_URL_BSC",
            "RPC_URL_BNB",
            "RPC_URL_POLYGON",
            "RPC_URL_ETHEREUM",
            "RPC_URL_ARBITRUM",
        ],
        &mut urls,
    );
    push_rpc_url_entries(
        network,
        "getblock",
        &[
            "GETBLOCK_RPC_URL",
            "GETBLOCK_RPC_URL_2",
            "GETBLOCK_RPC_URL_3",
            "GETBLOCK_RPC_URL_BSC",
            "GETBLOCK_RPC_URL_BNB",
            "GETBLOCK_RPC_URL_POLYGON",
            "GETBLOCK_RPC_URL_ETHEREUM",
            "GETBLOCK_RPC_URL_ARBITRUM",
            "GETBLOCK_BSC",
            "GETBLOCK_BNB",
            "GETBLOCK_POLYGON",
            "GETBLOCK_ETHEREUM",
            "GETBLOCK_ARBITRUM",
        ],
        &mut urls,
    );
    urls
}

fn push_rpc_url_entries(
    network: &str,
    label: &str,
    keys: &[&str],
    urls: &mut Vec<(String, String)>,
) {
    let mut label_index = urls
        .iter()
        .filter(|(name, _)| name.starts_with(label))
        .count();
    for key in prioritized_network_keys(network, keys) {
        if let Ok(value) = env::var(key) {
            for item in value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                if !urls
                    .iter()
                    .any(|(_, existing)| existing.eq_ignore_ascii_case(item))
                {
                    label_index += 1;
                    urls.push((format!("{label}-{label_index}"), item.to_string()));
                }
            }
        }
    }
}

fn parse_optional_address_env(
    name: &str,
    tolerate_invalid: bool,
) -> Result<Option<Address>, Box<dyn std::error::Error>> {
    let Ok(value) = env::var(name) else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if is_placeholder(trimmed) {
        return Ok(None);
    }
    match trimmed.parse::<Address>() {
        Ok(address) => Ok(Some(address)),
        Err(err) if tolerate_invalid => {
            warn!("ignoring invalid {name} for runtime load test: {}", err);
            Ok(None)
        }
        Err(err) => Err(format!("invalid {name}: {err}").into()),
    }
}

fn is_placeholder(value: &str) -> bool {
    let trimmed = value.trim();
    let lower = trimmed.to_ascii_lowercase();
    trimmed.is_empty()
        || lower == "sua_chave_hex_aqui"
        || lower == "0xseucontratoalvo"
        || lower.starts_with("sua_")
        || lower.starts_with("seu_")
        || lower.starts_with("your_")
        || lower.starts_with("change_me")
        || lower.starts_with("replace_me")
        || lower.contains("placeholder")
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .unwrap_or_else(|_| "false".to_string())
        .trim()
        .eq_ignore_ascii_case("true")
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
        "bsc" | "bnb" => Some(format!("https://bnb-mainnet.g.alchemy.com/v2/{key}")),
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

fn supports_default_alchemy_mempool_ws(network: &str) -> bool {
    matches!(network, "ethereum" | "arbitrum" | "polygon")
}

fn is_allowed_mempool_ws_url(network: &str, url: &str) -> bool {
    !matches!(network, "bsc" | "bnb")
        || !url
            .to_ascii_lowercase()
            .contains("bnb-mainnet.g.alchemy.com")
}

fn infura_url_for_network(network: &str, key: &str) -> Option<String> {
    match network {
        "bsc" | "bnb" => Some(format!("https://bsc-mainnet.infura.io/v3/{key}")),
        "ethereum" => Some(format!("https://mainnet.infura.io/v3/{key}")),
        "arbitrum" => Some(format!("https://arbitrum-mainnet.infura.io/v3/{key}")),
        "polygon" => Some(format!("https://polygon-mainnet.infura.io/v3/{key}")),
        _ => None,
    }
}

fn parse_rpc_preference(value: &str) -> Result<RpcPreference, Box<dyn std::error::Error>> {
    match value.trim().to_lowercase().as_str() {
        "auto" => Ok(RpcPreference::Auto),
        "getblock" => Ok(RpcPreference::GetBlock),
        "alchemy" => Ok(RpcPreference::Alchemy),
        "infura" => Ok(RpcPreference::Infura),
        other => Err(format!("unsupported RPC preference: {other}").into()),
    }
}

fn parse_network_rpc_preference(
    network: &str,
    base_name: &str,
) -> Result<RpcPreference, Box<dyn std::error::Error>> {
    let keys = network_env_keys(base_name);
    for key in prioritized_network_key_names(network, &keys) {
        if let Ok(value) = env::var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return parse_rpc_preference(trimmed);
            }
        }
    }
    Ok(RpcPreference::Auto)
}

fn rpc_provider_filter(network: &str) -> Option<Vec<String>> {
    let mut keys = network_env_keys("RPC_PROVIDER_FILTER");
    keys.extend(network_env_keys("RPC_ENABLED_PROVIDERS"));
    let raw = prioritized_network_key_names(network, &keys)
        .into_iter()
        .find_map(|key| env::var(key).ok().filter(|value| !value.trim().is_empty()))?;
    let providers: Vec<String> = raw
        .split(',')
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty() && value != "auto" && value != "all")
        .collect();
    (!providers.is_empty()).then_some(providers)
}

fn rpc_name_matches_filter(name: &str, providers: &[String]) -> bool {
    let lower = name.to_ascii_lowercase();
    providers.iter().any(|provider| {
        lower == *provider || lower.starts_with(&format!("{provider}-"))
    })
}

fn rpc_name_allowed_for_network(network: &str, name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    match network {
        "bsc" | "bnb" => lower.starts_with("getblock-"),
        "polygon" => lower.starts_with("alchemy-") || lower.starts_with("infura-"),
        _ => true,
    }
}

fn network_env_keys(base_name: &str) -> Vec<String> {
    [
        format!("{base_name}_BSC"),
        format!("{base_name}_BNB"),
        format!("{base_name}_POLYGON"),
        format!("{base_name}_ETHEREUM"),
        format!("{base_name}_ARBITRUM"),
        base_name.to_string(),
    ]
    .into_iter()
    .collect()
}

fn prioritized_network_key_names(network: &str, keys: &[String]) -> Vec<String> {
    let mut prioritized = Vec::new();
    let suffixes: &[&str] = match network {
        "bsc" => &["_BSC", "_BNB"],
        "polygon" => &["_POLYGON"],
        "ethereum" => &["_ETHEREUM"],
        "arbitrum" => &["_ARBITRUM"],
        _ => &[],
    };

    for suffix in suffixes {
        for key in keys {
            if key.ends_with(suffix) && !prioritized.contains(key) {
                prioritized.push(key.clone());
            }
        }
    }

    for key in keys {
        let is_network_specific = key.contains("_BSC")
            || key.contains("_BNB")
            || key.contains("_POLYGON")
            || key.contains("_ETHEREUM")
            || key.contains("_ARBITRUM");
        if !is_network_specific && !prioritized.contains(key) {
            prioritized.push(key.clone());
        }
    }

    prioritized
}

fn parse_network_optional_u64(
    network: &str,
    keys: &[&str],
) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    for key in prioritized_network_keys(network, keys) {
        if let Ok(value) = env::var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Ok(Some(trimmed.parse::<u64>()?));
            }
        }
    }
    Ok(None)
}

fn prioritized_network_keys<'a>(network: &str, keys: &'a [&'a str]) -> Vec<&'a str> {
    let mut prioritized = Vec::new();
    let suffixes: &[&str] = match network {
        "bsc" => &["_BSC", "_BNB"],
        "polygon" => &["_POLYGON"],
        "ethereum" => &["_ETHEREUM"],
        "arbitrum" => &["_ARBITRUM"],
        _ => &[],
    };

    for suffix in suffixes {
        for key in keys {
            if key.ends_with(suffix) && !prioritized.contains(key) {
                prioritized.push(*key);
            }
        }
    }

    for key in keys {
        let is_network_specific = key.contains("_BSC")
            || key.contains("_BNB")
            || key.contains("_POLYGON")
            || key.contains("_ETHEREUM")
            || key.contains("_ARBITRUM");
        if !is_network_specific && !prioritized.contains(key) {
            prioritized.push(*key);
        }
    }

    prioritized
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
    let raw = if raw.trim().is_empty() {
        default_monitored_tokens(network).to_string()
    } else {
        raw
    };
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
                address: (*address)
                    .parse::<Address>()
                    .map_err(|err| format!("invalid address in {env_name}: {address}: {err}"))?,
                decimals: (*decimals).parse::<u8>()?,
                price_eth: (*price_eth).parse::<f64>()?,
            },
            [_symbol, address, decimals, price_eth] => MonitoredTokenConfig {
                address: (*address)
                    .parse::<Address>()
                    .map_err(|err| format!("invalid address in {env_name}: {address}: {err}"))?,
                decimals: (*decimals).parse::<u8>()?,
                price_eth: (*price_eth).parse::<f64>()?,
            },
            [_symbol, _class, address, decimals, price_eth] => MonitoredTokenConfig {
                address: (*address)
                    .parse::<Address>()
                    .map_err(|err| format!("invalid address in {env_name}: {address}: {err}"))?,
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

fn default_monitored_tokens(network: &str) -> &'static str {
    match network {
        "bsc" => "WBNB:0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c:18:1.0",
        "polygon" => "WPOL:0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270:18:1.0",
        "ethereum" => "WETH:0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2:18:1.0",
        "arbitrum" => "WETH:0x82aF49447D8a07e3bd95BD0d56f35241523fBab1:18:1.0",
        _ => "",
    }
}

fn parse_builder_relays(primary: &str) -> Vec<String> {
    let mut relays = Vec::new();
    let primary = primary.trim();
    if !primary.is_empty() {
        relays.push(primary.to_string());
    }

    if let Ok(raw) = env::var("BUILDER_RELAYS") {
        for relay in raw
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if !relays
                .iter()
                .any(|existing| existing.eq_ignore_ascii_case(relay))
            {
                relays.push(relay.to_string());
            }
        }
    }

    relays
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn replay_tuned_runtime_env_can_be_applied() {
        let _guard = ENV_LOCK.lock().unwrap();
        let temp = std::env::temp_dir().join(format!(
            "flash_bot_tuned_env_{}.env",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(
            &temp,
            "MEV_GAS_OVERPAY_BASE_EXTRA_BPS=777\nREPLAY_TUNE_GAS_EXTRA_BPS=2000\n",
        )
        .unwrap();

        unsafe {
            env::remove_var("MEV_GAS_OVERPAY_BASE_EXTRA_BPS");
            env::set_var("REPLAY_AUTO_TUNE_USE_IN_RUNTIME", "true");
            env::set_var("REPLAY_AUTO_TUNE_RUNTIME_ENV_PATH", &temp);
        }

        apply_replay_tuned_runtime_env().unwrap();

        assert_eq!(
            env::var("MEV_GAS_OVERPAY_BASE_EXTRA_BPS").unwrap(),
            "777".to_string()
        );

        unsafe {
            env::remove_var("MEV_GAS_OVERPAY_BASE_EXTRA_BPS");
            env::remove_var("REPLAY_AUTO_TUNE_USE_IN_RUNTIME");
            env::remove_var("REPLAY_AUTO_TUNE_RUNTIME_ENV_PATH");
        }
        let _ = fs::remove_file(temp);
    }
}
