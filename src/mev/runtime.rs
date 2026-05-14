use crate::config::{Config, MonitoredTokenConfig};
use crate::dashboard::DashboardHandle;
use crate::mev::adaptive::{AdaptivePolicy, AdaptiveQuoteInput, ClusterKey, PreflightInput};
use crate::mev::amm::uniswap_v2::V2PoolState;
use crate::mev::execution::payload_builder::{FeeExtractionBuildInput, PayloadBuilder};
use crate::mev::execution::ExecutionEngine;
use crate::mev::opportunity::{roi_bps, wei_to_eth_f64, MevOpportunity};
use crate::rpc::RpcFleet;
use crate::storage::Storage;
use chrono::Timelike;
use ethers::abi::{self, ParamType, Token};
use ethers::providers::{Middleware, Provider, StreamExt, Ws};
use ethers::types::{Address, Transaction, U256};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

const SWAP_EXACT_TOKENS_FOR_TOKENS: [u8; 4] = [0x38, 0xed, 0x17, 0x39];
const SWAP_EXACT_ETH_FOR_TOKENS: [u8; 4] = [0x7f, 0xf3, 0x6a, 0xb5];
const SWAP_EXACT_TOKENS_FOR_ETH: [u8; 4] = [0x18, 0xcb, 0xaf, 0xe5];
const SWAP_EXACT_TOKENS_FOR_TOKENS_SUPPORTING_FEE: [u8; 4] = [0x5c, 0x11, 0xd7, 0x95];
const SWAP_EXACT_ETH_FOR_TOKENS_SUPPORTING_FEE: [u8; 4] = [0xb6, 0xf9, 0xde, 0x95];
const SWAP_EXACT_TOKENS_FOR_ETH_SUPPORTING_FEE: [u8; 4] = [0x79, 0x1a, 0xc9, 0x47];

#[derive(Debug, Clone)]
pub(crate) struct SwapSignal {
    pub(crate) selector: [u8; 4],
    pub(crate) amount_in: U256,
    pub(crate) notional_wei: U256,
    pub(crate) path: Vec<Address>,
    pub(crate) router: Address,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FastPreflightDecision {
    pub(crate) should_continue: bool,
    pub(crate) reject_reason: Option<&'static str>,
    pub(crate) ev_upper_bound_usd: f64,
    pub(crate) estimated_gas_cost_usd: f64,
    pub(crate) competition_score_fast: f64,
    pub(crate) gas_ratio: f64,
}

ethers::contract::abigen!(
    UniswapV2Factory,
    r#"[
        function getPair(address tokenA, address tokenB) external view returns (address pair)
    ]"#,
);

ethers::contract::abigen!(
    UniswapV2Pair,
    r#"[
        function token0() external view returns (address)
        function token1() external view returns (address)
        function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast)
    ]"#,
);

pub async fn run(
    config: Arc<Config>,
    rpc_fleet: Arc<RpcFleet>,
    dashboard: DashboardHandle,
    storage: Storage,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(ws_url) = config.mempool_ws_url() else {
        return Err("fee extraction runtime requires MEMPOOL websocket URL".into());
    };
    if !config.allow_send {
        return Err("fee extraction runtime requires ALLOW_SEND=true".into());
    }

    let ws = Ws::connect(ws_url.clone()).await?;
    let provider = Arc::new(Provider::new(ws));
    let mut stream = provider.subscribe_pending_txs().await?;
    let min_large_swap_wei = ethers::utils::parse_ether(config.mev.min_large_swap_eth.to_string())?;
    let min_profit_wei = ethers::utils::parse_ether(config.mev.min_net_profit_eth.to_string())?;
    let adaptive = AdaptivePolicy::shared(&config);
    refresh_historical_profiles(&adaptive, &storage, &dashboard);
    let executor = ExecutionEngine::new(config.clone(), rpc_fleet, dashboard.clone(), adaptive.clone());
    let mut last_profile_refresh = Instant::now();

    dashboard.event(
        "info",
        format!(
            "fee extraction runtime connected to {} min_large_swap={:.3} ETH min_profit={:.6} ETH",
            ws_url, config.mev.min_large_swap_eth, config.mev.min_net_profit_eth
        ),
    );

    while let Some(tx_hash) = stream.next().await {
        if last_profile_refresh.elapsed() >= Duration::from_secs(60) {
            refresh_historical_profiles(&adaptive, &storage, &dashboard);
            last_profile_refresh = Instant::now();
        }
        let lookup_started = Instant::now();
        let tx = match provider.get_transaction(tx_hash).await {
            Ok(Some(tx)) => tx,
            Ok(None) => continue,
            Err(err) => {
                warn!("pending tx lookup failed for {:?}: {}", tx_hash, err);
                continue;
            }
        };

        dashboard.record_latency(
            "fee_pending_lookup",
            lookup_started.elapsed().as_millis(),
            None,
            None,
        );
        if let Ok(mut model) = adaptive.lock() {
            model.observe_lookup_latency(lookup_started.elapsed().as_millis() as f64);
        }

        let Some(signal) = decode_relevant_swap(&tx, &config.monitored_tokens, min_large_swap_wei) else {
            continue;
        };

        let gas_price = tx.max_fee_per_gas.or(tx.gas_price).unwrap_or_default();
        if gas_price.is_zero() {
            debug!("fee extraction candidate skipped {:?}: missing gas price", tx.hash);
            continue;
        }
        let fast_gate = fast_preflight_gate(&signal, gas_price, min_large_swap_wei, &config);
        if !fast_gate.should_continue {
            if let Some(reason) = fast_gate.reject_reason {
                dashboard.record_reject_reason("fast_preflight", reason);
            }
            debug!(
                "fast preflight rejected {:?}: reason={} ev_upper_bound={:.2}usd gas={:.2}usd competition_fast={:.2} gas_ratio={:.2}",
                tx.hash,
                fast_gate.reject_reason.unwrap_or("unknown"),
                fast_gate.ev_upper_bound_usd,
                fast_gate.estimated_gas_cost_usd,
                fast_gate.competition_score_fast,
                fast_gate.gas_ratio
            );
            continue;
        }
        let cluster = ClusterKey {
            router: signal.router,
            token_in: signal.path[0],
            token_out: *signal.path.last().unwrap_or(&signal.path[0]),
            selector: signal.selector,
        };
        if let Ok(mut model) = adaptive.lock() {
            model.observe_candidate_flow(cluster, signal.notional_wei, gas_price);
        }
        let preflight = if let Ok(mut model) = adaptive.lock() {
            model.preflight_score(PreflightInput {
                cluster,
                notional_eth: wei_to_eth_f64(signal.notional_wei),
                gas_price_wei: gas_price,
                path_len: signal.path.len(),
            })
        } else {
            continue;
        };
        dashboard.set_market_regime(preflight.regime.as_str());
        if !preflight.should_continue {
            if let Some(reason) = preflight.reject_reason {
                dashboard.record_reject_reason("preflight", reason);
            }
            debug!(
                "preflight rejected {:?}: regime={} reason={} score={:.2} upper_bound={:.2}usd gas={:.2}usd density={:.2} cluster={:.2} gas_pressure={:.2} impact_hint={:.2} size={:.2}",
                tx.hash,
                preflight.regime.as_str(),
                preflight.reject_reason.unwrap_or("unknown"),
                preflight.preflight_score,
                preflight.upper_bound_ev_usd,
                preflight.estimated_gas_cost_usd,
                preflight.mempool_density,
                preflight.cluster_heat,
                preflight.gas_pressure,
                preflight.impact_hint,
                preflight.size_score
            );
            continue;
        }

        let Some(payload) = build_v2_payload(provider.clone(), &config, &signal, gas_price).await else {
            continue;
        };

        if !passes_ev_gate(&config, &payload, &signal, lookup_started.elapsed(), min_profit_wei) {
            continue;
        }

        let execution_cost_wei = gas_price
            .saturating_mul(U256::from(payload.gas_limit))
            .saturating_mul(U256::from(config.mev.gas_safety_margin_bps))
            / U256::from(10_000u64);
        if !passes_quality_gate(&config, &payload, execution_cost_wei) {
            continue;
        }
        let quote = if let Ok(mut model) = adaptive.lock() {
            model.quote_for_relays(
                AdaptiveQuoteInput {
                    cluster,
                    pair: payload.pair,
                    hour_utc: chrono::Utc::now().hour() as u8,
                    expected_profit_wei: payload.expected_profit_wei,
                    execution_cost_wei,
                    gas_price_wei: gas_price,
                    lookup_latency_ms: lookup_started.elapsed().as_millis() as f64,
                    notional_eth: wei_to_eth_f64(signal.notional_wei),
                    price_impact_bps: payload.price_impact_bps,
                    relay_pressure_override: None,
                },
                &config.builder_relays,
            )
        } else {
            continue;
        };
        dashboard.set_market_regime(quote.regime.as_str());
        if !quote.should_execute {
            if let Some(reason) = quote.reject_reason {
                dashboard.record_reject_reason("adaptive", reason);
            }
            continue;
        }
        dashboard.event(
            "info",
            format!(
                "adaptive gate passed victim={:?} regime={} relay={} ev_real={:.2}usd threshold={:.2}usd p={:.2} comp={:.2} risk={:.2} builder={:.2} density={:.2} cluster={:.2} latency={:.2} gas_pressure={:.2} comp_penalty={:.2}usd risk_penalty={:.2}usd",
                tx.hash,
                quote.regime.as_str(),
                quote.selected_relay.as_deref().unwrap_or("unknown"),
                quote.ev_real_usd,
                quote.threshold_dynamic_usd,
                quote.p_positive,
                quote.competition_score,
                quote.risk_score,
                quote.builder_pressure,
                quote.mempool_density,
                quote.cluster_heat,
                quote.latency_penalty,
                quote.gas_pressure,
                quote.competition_penalty_usd,
                quote.risk_penalty_usd
            ),
        );

        let opportunity = build_opportunity(&tx, &signal, payload, quote.selected_relay.clone());
        executor.handle(opportunity).await?;
    }

    Ok(())
}

fn build_opportunity(
    tx: &Transaction,
    signal: &SwapSignal,
    payload: crate::mev::execution::payload_builder::ExecutionPayload,
    preferred_relay: Option<String>,
) -> MevOpportunity {
    MevOpportunity {
        detected_at: Instant::now(),
        victim_tx: tx.hash,
        victim_transaction: Some(tx.clone()),
        execution_payload: Some(payload),
        router: signal.router,
        token_in: signal.path[0],
        token_out: *signal.path.last().unwrap_or(&signal.path[0]),
        selector: signal.selector,
        preferred_relay,
    }
}

pub(crate) fn passes_quality_gate(
    config: &Config,
    payload: &crate::mev::execution::payload_builder::ExecutionPayload,
    execution_cost_wei: U256,
) -> bool {
    let roi = roi_bps(payload.expected_profit_wei, execution_cost_wei);
    let impact_score = ((payload.price_impact_bps as f64
        / config.mev.max_price_impact_bps.max(1) as f64)
        * 100.0)
        .clamp(0.0, 100.0) as u16;
    roi >= config.mev.min_roi_bps && impact_score <= 100
}

pub(crate) fn fast_preflight_gate(
    signal: &SwapSignal,
    gas_price: U256,
    min_large_swap_wei: U256,
    config: &Config,
) -> FastPreflightDecision {
    if signal.path.len() < 2 {
        return FastPreflightDecision {
            should_continue: false,
            reject_reason: Some("invalid_path"),
            ev_upper_bound_usd: 0.0,
            estimated_gas_cost_usd: 0.0,
            competition_score_fast: 1.0,
            gas_ratio: 0.0,
        };
    }
    if signal.notional_wei < min_large_swap_wei {
        return FastPreflightDecision {
            should_continue: false,
            reject_reason: Some("notional_below_min"),
            ev_upper_bound_usd: 0.0,
            estimated_gas_cost_usd: 0.0,
            competition_score_fast: 1.0,
            gas_ratio: 0.0,
        };
    }

    let notional_eth = wei_to_eth_f64(signal.notional_wei);
    let notional_usd = notional_eth * config.mev.eth_usd_price;
    let path_len = signal.path.len();
    let gas_baseline_gwei = heuristic_gas_baseline_gwei(config);
    let gas_price_gwei = wei_to_gwei_f64(gas_price);
    let gas_ratio = (gas_price_gwei / gas_baseline_gwei.max(1e-9)).max(0.0);
    let size_bucket = notional_size_bucket(notional_eth, config.mev.min_large_swap_eth);
    let selector_factor = selector_heuristic_factor(signal.selector);
    let path_penalty = fast_path_penalty(path_len);
    let size_factor = match size_bucket {
        0 => 0.00022,
        1 => 0.00038,
        _ => 0.00060,
    };
    let heuristic_factor = (selector_factor * size_factor * (1.0 - path_penalty)).max(0.00005);
    let estimated_gas_cost_usd = wei_to_eth_f64(
        gas_price.saturating_mul(U256::from(
            config
                .estimated_exec_gas
                .saturating_add(config.estimated_bundle_overhead_gas)
                .max(180_000),
        )),
    ) * config.mev.eth_usd_price;
    let ev_upper_bound_usd = notional_usd * heuristic_factor - estimated_gas_cost_usd;

    let gas_pressure = ((gas_ratio - 1.0) / 0.8).clamp(0.0, 1.0);
    let size_pressure = match size_bucket {
        0 => 0.20,
        1 => 0.48,
        _ => 0.72,
    };
    let path_risk = (path_len.saturating_sub(2).min(3) as f64) / 3.0;
    let competition_score_fast = (gas_pressure * 0.46
        + size_pressure * 0.34
        + path_risk * 0.20)
        .clamp(0.0, 1.0);

    let reject_reason = if ev_upper_bound_usd < config.mev.min_profit_usd * 1.5 {
        Some("ev_upper_bound_below_min")
    } else if competition_score_fast > 0.75 {
        Some("competition_fast_too_high")
    } else if gas_ratio > 1.8 {
        Some("gas_ratio_too_high")
    } else {
        None
    };

    FastPreflightDecision {
        should_continue: reject_reason.is_none(),
        reject_reason,
        ev_upper_bound_usd,
        estimated_gas_cost_usd,
        competition_score_fast,
        gas_ratio,
    }
}

pub(crate) fn passes_ev_gate(
    config: &Config,
    payload: &crate::mev::execution::payload_builder::ExecutionPayload,
    signal: &SwapSignal,
    lookup_latency: std::time::Duration,
    min_profit_wei: U256,
) -> bool {
    let lookup_is_fresh =
        lookup_latency.as_millis() <= u128::from(config.mev.max_pending_age_ms.max(1));
    let large_enough = signal.notional_wei >= ethers::utils::parse_ether(config.mev.min_large_swap_eth.to_string()).unwrap_or_default();
    let inevitable_impact = payload.price_impact_bps >= 8;
    let profit_above_threshold = payload.expected_profit_wei >= min_profit_wei;
    let net_ev_usd = wei_to_eth_f64(payload.expected_profit_wei) * config.mev.eth_usd_price;
    let gas_budget_ok = payload.gas_limit <= config.mev.max_gas_per_tx;

    lookup_is_fresh
        && large_enough
        && inevitable_impact
        && profit_above_threshold
        && net_ev_usd >= config.mev.min_profit_usd
        && gas_budget_ok
}

pub(crate) async fn build_v2_payload<M: Middleware + 'static>(
    provider: Arc<M>,
    config: &Config,
    signal: &SwapSignal,
    gas_price: U256,
) -> Option<crate::mev::execution::payload_builder::ExecutionPayload> {
    let factory = config.mev.uniswap_v2_factory?;
    let recipient = config.profit_address;
    let token_in = *signal.path.first()?;
    let token_out = *signal.path.get(1)?;
    let factory = UniswapV2Factory::new(factory, provider.clone());
    let pair = factory.get_pair(token_in, token_out).call().await.ok()?;
    if pair == Address::zero() {
        return None;
    }

    let pair_contract = UniswapV2Pair::new(pair, provider.clone());
    let token0 = pair_contract.token_0().call().await.ok()?;
    let token1 = pair_contract.token_1().call().await.ok()?;
    let reserves = pair_contract.get_reserves().call().await.ok()?;
    let pool = V2PoolState {
        pair,
        token0,
        token1,
        reserve0: U256::from(reserves.0),
        reserve1: U256::from(reserves.1),
        fee_bps: 30,
    };
    let capital_available_wei = ethers::utils::parse_ether(config.mev.capital_eth.to_string()).ok()?;
    PayloadBuilder::build_fee_extraction_v2(
        config,
        FeeExtractionBuildInput {
            router: signal.router,
            pair,
            recipient,
            token_in,
            token_out,
            victim_amount_in: signal.amount_in,
            state_before: crate::mev::simulation::state_simulator::AmmState::UniswapV2(pool),
            capital_available_wei,
            gas_price_wei: gas_price,
        },
    )
    .ok()
}

pub(crate) fn decode_relevant_swap(
    tx: &Transaction,
    monitored_tokens: &[MonitoredTokenConfig],
    min_large_swap_wei: U256,
) -> Option<SwapSignal> {
    let selector = selector(tx)?;
    let router = tx.to?;
    let args = &tx.input.as_ref()[4..];

    let mut signal = match selector {
        SWAP_EXACT_ETH_FOR_TOKENS | SWAP_EXACT_ETH_FOR_TOKENS_SUPPORTING_FEE => {
            let decoded = abi::decode(
                &[
                    ParamType::Uint(256),
                    ParamType::Array(Box::new(ParamType::Address)),
                    ParamType::Address,
                    ParamType::Uint(256),
                ],
                args,
            )
            .ok()?;
            SwapSignal {
                selector,
                amount_in: tx.value,
                notional_wei: tx.value,
                path: decoded.get(1).and_then(token_as_address_vec)?,
                router,
            }
        }
        SWAP_EXACT_TOKENS_FOR_TOKENS
        | SWAP_EXACT_TOKENS_FOR_ETH
        | SWAP_EXACT_TOKENS_FOR_TOKENS_SUPPORTING_FEE
        | SWAP_EXACT_TOKENS_FOR_ETH_SUPPORTING_FEE => {
            let decoded = abi::decode(
                &[
                    ParamType::Uint(256),
                    ParamType::Uint(256),
                    ParamType::Array(Box::new(ParamType::Address)),
                    ParamType::Address,
                    ParamType::Uint(256),
                ],
                args,
            )
            .ok()?;
            SwapSignal {
                selector,
                amount_in: decoded.first().and_then(token_as_uint)?,
                notional_wei: U256::zero(),
                path: decoded.get(2).and_then(token_as_address_vec)?,
                router,
            }
        }
        _ => return None,
    };

    let notional_wei = estimate_notional_wei(&signal, monitored_tokens)?;
    if notional_wei < min_large_swap_wei || signal.path.len() < 2 {
        return None;
    }
    signal.notional_wei = notional_wei;
    Some(signal)
}

fn estimate_notional_wei(
    signal: &SwapSignal,
    monitored_tokens: &[MonitoredTokenConfig],
) -> Option<U256> {
    if matches!(
        signal.selector,
        SWAP_EXACT_ETH_FOR_TOKENS | SWAP_EXACT_ETH_FOR_TOKENS_SUPPORTING_FEE
    ) {
        return Some(signal.amount_in);
    }

    let input = signal.path.first()?;
    let token = monitored_tokens.iter().find(|token| token.address == *input)?;
    let decimals_factor = 10f64.powi(i32::from(token.decimals));
    let normalized = signal.amount_in.to_string().parse::<f64>().ok()? / decimals_factor;
    let value_eth = normalized * token.price_eth;
    ethers::utils::parse_ether(value_eth.to_string()).ok()
}

fn selector(tx: &Transaction) -> Option<[u8; 4]> {
    let input = tx.input.as_ref();
    (input.len() >= 4).then(|| [input[0], input[1], input[2], input[3]])
}

fn selector_heuristic_factor(selector: [u8; 4]) -> f64 {
    match selector {
        SWAP_EXACT_ETH_FOR_TOKENS | SWAP_EXACT_ETH_FOR_TOKENS_SUPPORTING_FEE => 1.18,
        SWAP_EXACT_TOKENS_FOR_ETH | SWAP_EXACT_TOKENS_FOR_ETH_SUPPORTING_FEE => 1.08,
        SWAP_EXACT_TOKENS_FOR_TOKENS | SWAP_EXACT_TOKENS_FOR_TOKENS_SUPPORTING_FEE => 0.94,
        _ => 0.82,
    }
}

fn notional_size_bucket(notional_eth: f64, min_large_swap_eth: f64) -> u8 {
    if notional_eth < min_large_swap_eth * 1.25 {
        0
    } else if notional_eth < min_large_swap_eth * 2.5 {
        1
    } else {
        2
    }
}

fn fast_path_penalty(path_len: usize) -> f64 {
    match path_len {
        0 | 1 | 2 => 0.0,
        3 => 0.18,
        4 => 0.30,
        _ => 0.42,
    }
}

fn heuristic_gas_baseline_gwei(config: &Config) -> f64 {
    match config.network.as_str() {
        "arbitrum" => 0.15,
        "polygon" => 80.0,
        "bsc" => 3.0,
        _ => 25.0,
    }
}

fn refresh_historical_profiles(
    adaptive: &crate::mev::adaptive::SharedAdaptivePolicy,
    storage: &Storage,
    dashboard: &DashboardHandle,
) {
    match storage.outcome_profiles(3, 256) {
        Ok(profiles) => {
            let profile_count = profiles.len();
            if let Ok(mut model) = adaptive.lock() {
                model.apply_historical_profiles(profiles);
            }
            dashboard.event(
                "info",
                format!("historical profile refresh loaded {} pair/router/hour profiles", profile_count),
            );
        }
        Err(err) => {
            warn!("historical profile refresh failed: {}", err);
        }
    }
}

fn wei_to_gwei_f64(value: U256) -> f64 {
    value.to_string().parse::<f64>().unwrap_or(0.0) / 1e9
}

fn token_as_uint(token: &Token) -> Option<U256> {
    match token {
        Token::Uint(value) => Some(*value),
        _ => None,
    }
}

fn token_as_address(token: &Token) -> Option<Address> {
    match token {
        Token::Address(value) => Some(*value),
        _ => None,
    }
}

fn token_as_address_vec(token: &Token) -> Option<Vec<Address>> {
    match token {
        Token::Array(values) => values.iter().map(token_as_address).collect(),
        _ => None,
    }
}
