use crate::config::Config;
use crate::dashboard::{
    DashboardHandle, ExecutionOutcomeUpdate, RelaySnapshot, RelaySnapshotUpdate,
    TreasuryRecommendationUpdate,
};
use crate::mev::adaptive::{ClusterKey, ContextualOutcomeKind, RelayQuote, SharedAdaptivePolicy};
use crate::mev::execution::payload_builder::AmmRouteKind;
use crate::mev::opportunity::{wei_to_eth_f64, MevOpportunity};
use crate::mev::pnl::tracker::{ExecutionResult, PnlTracker};
use crate::mev::simulation::state_simulator::{
    AccountState, AmmState, EvmPreflightResult, StateSimulator,
};
use crate::rpc::{RpcFleet, RpcHandle};
use ethers::contract::abigen;
use ethers::prelude::*;
use ethers::types::transaction::eip2718::TypedTransaction;
use ethers_flashbots::{BundleRequest, FlashbotsMiddleware};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tracing::warn;
use url::Url;

abigen!(
    Erc20BalanceView,
    r#"[
        function balanceOf(address owner) external view returns (uint256)
    ]"#,
);

pub struct ExecutionEngine {
    config: Arc<Config>,
    rpc_fleet: Arc<RpcFleet>,
    dashboard: DashboardHandle,
    pnl: Arc<Mutex<PnlTracker>>,
    adaptive: SharedAdaptivePolicy,
    last_treasury_signal: Arc<Mutex<Option<TreasurySignal>>>,
    capital_budget: Arc<Mutex<CapitalBudget>>,
    execution_guard: Arc<Mutex<ExecutionGuard>>,
}

struct SendContext {
    endpoint: RpcHandle,
    hot_balance: U256,
    chain_nonce: U256,
    gas_price: U256,
    block: U64,
}

#[derive(Default)]
struct ExecutionGuard {
    consecutive_losses: u32,
    frozen_until: Option<Instant>,
    context_guards: HashMap<ContextFreezeKey, ContextExecutionGuard>,
}

#[derive(Clone, Debug, Eq)]
struct ContextFreezeKey {
    pair: Address,
    router: Address,
    submit_path: String,
}

impl PartialEq for ContextFreezeKey {
    fn eq(&self, other: &Self) -> bool {
        self.pair == other.pair
            && self.router == other.router
            && self.submit_path == other.submit_path
    }
}

impl Hash for ContextFreezeKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.pair.hash(state);
        self.router.hash(state);
        self.submit_path.hash(state);
    }
}

#[derive(Default)]
struct ContextExecutionGuard {
    consecutive_losses: u32,
    frozen_until: Option<Instant>,
    last_event_at: Option<Instant>,
    regime_pressure_ewma: f64,
}

struct StopLossSnapshot {
    consecutive_losses: u32,
    remaining_freeze_secs: u64,
    scope: &'static str,
}

impl ExecutionEngine {
    pub fn new(
        config: Arc<Config>,
        rpc_fleet: Arc<RpcFleet>,
        dashboard: DashboardHandle,
        adaptive: SharedAdaptivePolicy,
    ) -> Self {
        let capital_budget = Arc::new(Mutex::new(CapitalBudget::new(&config)));
        Self {
            config,
            rpc_fleet,
            dashboard,
            pnl: Arc::new(Mutex::new(PnlTracker::default())),
            adaptive,
            last_treasury_signal: Arc::new(Mutex::new(None)),
            capital_budget,
            execution_guard: Arc::new(Mutex::new(ExecutionGuard::default())),
        }
    }

    // NOVA FUNÇÃO: EVM Preflight Validation
    async fn validate_with_evm_preflight(
        &self,
        payload: &crate::mev::execution::payload_builder::ExecutionPayload,
        _tx_bytes: &Bytes,
        block_number: u64,
        pool_state: &AmmState,
        victim_tx: H256,
    ) -> Result<EvmPreflightResult, String> {
        let mut state_overrides = HashMap::new();

        // Add pool state override
        match pool_state {
            AmmState::UniswapV2(pool) => {
                let mut pool_account = AccountState::empty();
                // Slots aproximados para UniswapV2
                pool_account.storage.insert(U256::from(0), pool.reserve0);
                pool_account.storage.insert(U256::from(1), pool.reserve1);
                state_overrides.insert(pool.pair, pool_account);
            }
            AmmState::UniswapV3(pool) => {
                let mut pool_account = AccountState::empty();
                pool_account
                    .storage
                    .insert(U256::from(0), pool.sqrt_price_x96);
                pool_account.storage.insert(U256::from(1), pool.liquidity);
                pool_account
                    .storage
                    .insert(U256::from(2), U256::from(pool.current_tick as i128));
                state_overrides.insert(pool.pool, pool_account);
            }
        }

        // Add executor contract state
        if let Some(executor) = self.config.mev.mev_executor {
            state_overrides.insert(executor, AccountState::empty());
        }

        // Add profit recipient balance tracking
        state_overrides.insert(self.config.profit_address, AccountState::empty());

        // Criar transação mock para preflight
        let mock_tx = Transaction {
            hash: H256::zero(),
            nonce: U256::zero(),
            block_hash: None,
            block_number: None,
            transaction_index: None,
            from: self.config.executor_address,
            to: Some(payload.target_contract),
            value: payload.value,
            gas_price: Some(U256::from(0)),
            gas: U256::from(payload.gas_limit),
            input: payload.calldata.clone(),
            chain_id: Some(U256::from(self.config.chain_id)),
            ..Default::default()
        };

        self.dashboard.event(
            "info",
            format!(
                "EVM preflight started victim={:?} block={} gas_limit={}",
                victim_tx, block_number, payload.gas_limit
            ),
        );

        StateSimulator::evm_preflight_execution(
            &self.config,
            &mock_tx,
            block_number,
            state_overrides,
        )
        .await
    }

    pub async fn handle(
        &self,
        opportunity: MevOpportunity,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if !self.config.allow_send {
            return Err("fee extraction runtime requires ALLOW_SEND=true".into());
        }

        if opportunity.age_ms() > u128::from(self.config.mev.max_pending_age_ms.max(1)) {
            self.dashboard.event(
                "info",
                format!(
                    "fee extraction rejected victim={:?}: stale age={}ms",
                    opportunity.victim_tx,
                    opportunity.age_ms()
                ),
            );
            return Ok(());
        }

        let Some(mut payload) = opportunity.execution_payload.clone() else {
            self.dashboard.event(
                "warn",
                format!(
                    "fee extraction blocked victim={:?}: no execution payload",
                    opportunity.victim_tx
                ),
            );
            return Ok(());
        };

        if let Some(snapshot) = self.stop_loss_snapshot() {
            self.dashboard.event(
                "warn",
                format!(
                    "fee extraction frozen by {} stop-loss victim={:?}: consecutive_losses={} remaining_freeze_secs={}",
                    snapshot.scope,
                    opportunity.victim_tx,
                    snapshot.consecutive_losses,
                    snapshot.remaining_freeze_secs
                ),
            );
            return Ok(());
        }

        let wallet = self
            .config
            .executor_private_key
            .parse::<LocalWallet>()?
            .with_chain_id(self.config.chain_id);
        let wallet_address = wallet.address();
        let send_context = self
            .load_send_context(wallet_address)
            .await
            .map_err(|err| -> Box<dyn std::error::Error> { err.into() })?;
        let hot_balance = send_context.hot_balance;
        let hot_balance_eth = wei_to_eth_f64(hot_balance);
        let buffer_status = self.hot_wallet_status(hot_balance_eth);
        self.dashboard
            .set_executor_balance(hot_balance_eth, buffer_status);
        self.maybe_emit_treasury_signal(hot_balance_eth);
        if let Some(max_gas_price_wei) = self.config.mev.max_gas_price_wei() {
            if send_context.gas_price > max_gas_price_wei {
                self.dashboard.event(
                    "warn",
                    format!(
                        "fee extraction blocked victim={:?}: rpc gas price {:.2} gwei exceeds cap {} gwei",
                        opportunity.victim_tx,
                        wei_to_gwei_f64(send_context.gas_price),
                        self.config.mev.max_gas_price_gwei.unwrap_or_default()
                    ),
                );
                return Ok(());
            }
        }
        if hot_balance_eth < self.config.mev.executor_min_buffer_eth {
            self.dashboard.event(
                "warn",
                format!(
                    "fee extraction blocked victim={:?}: executor wallet underfunded balance={:.6} ETH min_buffer={:.6} ETH",
                    opportunity.victim_tx,
                    hot_balance_eth,
                    self.config.mev.executor_min_buffer_eth
                ),
            );
            return Ok(());
        }
        if hot_balance_eth > self.config.mev.executor_max_buffer_eth {
            self.dashboard.event(
                "warn",
                format!(
                    "fee extraction blocked victim={:?}: executor wallet overfunded balance={:.6} ETH max_buffer={:.6} ETH",
                    opportunity.victim_tx,
                    hot_balance_eth,
                    self.config.mev.executor_max_buffer_eth
                ),
            );
            return Ok(());
        }
        let target_block = (send_context.block + 1).as_u64();
        let cluster = ClusterKey {
            router: opportunity.router,
            token_in: opportunity.token_in,
            token_out: opportunity.token_out,
            selector: opportunity.selector,
        };

        // WAR LEVEL: EVM preflight validation (opcional, ativado por env)
        if std::env::var("MEV_EVM_PREFLIGHT_ENABLED")
            .unwrap_or_default()
            .eq_ignore_ascii_case("true")
        {
            let preflight_started = std::time::Instant::now();

            // Construir pool state a partir do payload
            // Nota: Idealmente o payload deveria carregar o pool_state, por enquanto criamos um placeholder
            let pool_state = match &payload.amm_kind {
                AmmRouteKind::UniswapV2 => {
                    AmmState::UniswapV2(crate::mev::amm::uniswap_v2::V2PoolState {
                        pair: payload.pair,
                        token0: Address::zero(),
                        token1: Address::zero(),
                        reserve0: U256::zero(),
                        reserve1: U256::zero(),
                        fee_bps: 30,
                    })
                }
                AmmRouteKind::UniswapV3 { fee_tier, .. } => {
                    AmmState::UniswapV3(crate::mev::amm::uniswap_v3::V3PoolState {
                        pool: payload.pair,
                        token0: Address::zero(),
                        token1: Address::zero(),
                        sqrt_price_x96: U256::zero(),
                        liquidity: U256::zero(),
                        current_tick: 0,
                        fee_bps: *fee_tier as u64 / 100,
                        initialized_ticks: Vec::new(),
                    })
                }
            };

            match self
                .validate_with_evm_preflight(
                    &payload,
                    &payload.tx,
                    target_block,
                    &pool_state,
                    opportunity.victim_tx,
                )
                .await
            {
                Ok(preflight_result) => {
                    self.dashboard.record_latency(
                        "evm_preflight",
                        preflight_started.elapsed().as_millis(),
                        None,
                        Some(&format!("success={}", preflight_result.success)),
                    );

                    if !preflight_result.success {
                        self.dashboard.event(
                            "warn",
                            format!(
                                "EVM preflight rejected victim={:?}: revert_reason={:?} gas_used={}",
                                opportunity.victim_tx,
                                preflight_result.revert_reason,
                                preflight_result.gas_used
                            ),
                        );
                        return Ok(());
                    }

                    self.dashboard.event(
                        "info",
                        format!(
                            "EVM preflight passed victim={:?} gas_used={} profit_wei={}",
                            opportunity.victim_tx,
                            preflight_result.gas_used,
                            preflight_result.profit_wei
                        ),
                    );
                }
                Err(err) => {
                    self.dashboard.event(
                        "warn",
                        format!(
                            "EVM preflight failed victim={:?}: {}",
                            opportunity.victim_tx, err
                        ),
                    );
                    // Não bloqueia a execução se o preflight falhar (modo soft)
                    if std::env::var("MEV_EVM_PREFLIGHT_HARD_FAIL")
                        .unwrap_or_default()
                        .eq_ignore_ascii_case("true")
                    {
                        return Ok(());
                    }
                }
            }
        }

        let budget_gate = self.check_and_reserve_budget(&opportunity, &payload, cluster);
        if !budget_gate.allowed {
            self.dashboard.event(
                "warn",
                format!(
                    "fee extraction budget blocked victim={:?}: reason={} capital={:.6} ETH total_window={:.6}/{:.6} cluster_window={:.6}/{:.6} pair_window={:.6}/{:.6}",
                    opportunity.victim_tx,
                    budget_gate.reject_reason,
                    budget_gate.capital_eth,
                    budget_gate.total_window_eth,
                    self.config.mev.max_window_exposure_eth,
                    budget_gate.cluster_window_eth,
                    self.config.mev.max_cluster_window_exposure_eth,
                    budget_gate.pair_window_eth,
                    self.config.mev.max_pair_window_exposure_eth
                ),
            );
            return Ok(());
        }
        let mut ranked_submit_paths = if self.config.uses_bundle_relays() {
            self.rank_relay_submit_paths(opportunity.preferred_relay.as_deref(), cluster)
        } else {
            self.rank_direct_rpc_submit_paths(&send_context.endpoint, cluster)
        };
        ranked_submit_paths.retain(|quote| {
            self.context_stop_loss_snapshot(payload.pair, opportunity.router, &quote.relay)
                .is_none()
        });
        if ranked_submit_paths.is_empty() {
            self.dashboard.event(
                "warn",
                format!(
                    "fee extraction frozen by contextual stop-loss victim={:?}: pair={:?} router={:?} no submit path available",
                    opportunity.victim_tx,
                    payload.pair,
                    opportunity.router
                ),
            );
            return Ok(());
        }
        let gas_price_to_use = self.dynamic_gas_price(
            send_context.gas_price,
            ranked_submit_paths.first(),
            &payload,
        );
        if gas_price_to_use != send_context.gas_price {
            self.dashboard.event(
                "info",
                format!(
                    "dynamic gas overpay victim={:?}: base={:.2} gwei adjusted={:.2} gwei",
                    opportunity.victim_tx,
                    wei_to_gwei_f64(send_context.gas_price),
                    wei_to_gwei_f64(gas_price_to_use),
                ),
            );
        }
        payload.tx = sign_executor_transaction(
            &wallet,
            &payload,
            send_context.chain_nonce,
            gas_price_to_use,
        )
        .await?;
        if !ranked_submit_paths.is_empty() {
            self.dashboard
                .set_relay_rankings(ranked_submit_paths.iter().map(relay_snapshot).collect());
            self.dashboard.event(
                "info",
                format!(
                    "submit path ranking victim={:?}: {}",
                    opportunity.victim_tx,
                    ranked_submit_paths
                        .iter()
                        .map(|quote| format!(
                            "{} score={:.2} pressure={:.2} accept={:.2} inclusion={:.2} miss={:.2} revert={:.2} submit={:.0}ms finalize={:.0}ms",
                            quote.relay,
                            quote.score,
                            quote.relay_pressure,
                            quote.accept_rate,
                            quote.inclusion_rate,
                            quote.accepted_not_included_rate,
                            quote.revert_rate,
                            quote.submit_latency_ms,
                            quote.finalization_latency_ms
                        ))
                        .collect::<Vec<_>>()
                        .join(" | ")
                ),
            );
        }

        if !self.config.uses_bundle_relays() {
            let submit_endpoints =
                self.direct_rpc_submit_endpoints_from_ranked_paths(&ranked_submit_paths);

            let mut last_submit_error: Option<String> = None;
            let mut submit_set = JoinSet::new();
            for endpoint in submit_endpoints {
                let tx = payload.tx.clone();
                let endpoint_id = endpoint.id;
                let endpoint_name = endpoint.name.clone();
                let provider = endpoint.provider.clone();
                submit_set.spawn(async move {
                    let started = Instant::now();
                    let submit_outcome = match provider.send_raw_transaction(tx).await {
                        Ok(pending) => Ok((
                            endpoint_id,
                            endpoint_name,
                            started.elapsed(),
                            pending.tx_hash(),
                        )),
                        Err(err) => Err((
                            endpoint_id,
                            endpoint_name,
                            started.elapsed(),
                            err.to_string(),
                        )),
                    };
                    submit_outcome
                });
            }

            while let Some(result) = submit_set.join_next().await {
                let Ok(submit_result) = result else {
                    continue;
                };
                match submit_result {
                    Ok((endpoint_id, endpoint_name, elapsed, tx_hash)) => {
                        self.rpc_fleet.reserve_send_selection(endpoint_id);
                        let submit_latency_ms = elapsed.as_millis() as f64;
                        self.rpc_fleet
                            .record_success(endpoint_id, elapsed, Some(target_block));
                        let relay_label = format!("rpc://{}", endpoint_name);
                        if let Ok(mut adaptive) = self.adaptive.lock() {
                            adaptive
                                .record_submit_success_for_relay(&relay_label, submit_latency_ms);
                        }
                        self.dashboard.record_latency(
                            "fee_rpc_submit",
                            submit_latency_ms as u128,
                            None,
                            Some(&endpoint_name),
                        );
                        self.dashboard.event(
                            "success",
                            format!(
                                "fee extraction tx submitted victim={:?} path={} tx={:?} expected_profit={:.6} ETH",
                                opportunity.victim_tx,
                                format_submit_path(&payload.amm_kind, &relay_label),
                                tx_hash,
                                wei_to_eth_f64(payload.expected_profit_wei)
                            ),
                        );
                        submit_set.abort_all();
                        let realized = self
                            .observe_realized_pnl(
                                endpoint_id,
                                tx_hash,
                                &opportunity,
                                &payload,
                                &relay_label,
                                target_block,
                                submit_latency_ms,
                            )
                            .await?;
                        self.record_result(
                            &realized,
                            Some(payload.pair),
                            Some(opportunity.router),
                            Some(relay_label.as_str()),
                            Some(payload.context_priority_score),
                            Some(payload.context_toxicity_score),
                        );
                        return Ok(());
                    }
                    Err((endpoint_id, endpoint_name, _elapsed, err_text)) => {
                        self.rpc_fleet.reserve_send_selection(endpoint_id);
                        if is_insufficient_funds_error(&err_text) {
                            let estimated_cost_wei = send_context
                                .gas_price
                                .saturating_mul(U256::from(payload.gas_limit))
                                .saturating_add(payload.value);
                            self.dashboard.event(
                                "error",
                                format!(
                                    "execution failed victim={:?}: insufficient funds for gas/value via {} required_estimated_cost={:.6} ETH gas_price={:.2} gwei gas_limit={}",
                                    opportunity.victim_tx,
                                    endpoint_name,
                                    wei_to_eth_f64(estimated_cost_wei),
                                    wei_to_gwei_f64(send_context.gas_price),
                                    payload.gas_limit
                                ),
                            );
                        } else {
                            self.dashboard.event(
                                "warn",
                                format!(
                                    "fee extraction rpc submit failed victim={:?} via {}: {}",
                                    opportunity.victim_tx, endpoint_name, err_text
                                ),
                            );
                        }
                        self.rpc_fleet
                            .record_failure(endpoint_id, RpcFleet::classify_failure(&err_text));
                        last_submit_error = Some(err_text);
                    }
                }
            }

            if let Some(path) = ranked_submit_paths.first() {
                self.record_result(
                    &ExecutionResult {
                        realized_profit: 0.0,
                        gas_used: 0,
                        success: false,
                    },
                    Some(payload.pair),
                    Some(opportunity.router),
                    Some(path.relay.as_str()),
                    Some(payload.context_priority_score),
                    Some(payload.context_toxicity_score),
                );
            }
            return Err(last_submit_error
                .unwrap_or_else(|| "all rpc submit endpoints failed".to_string())
                .into());
        }

        let mut last_error: Option<String> = None;
        let tx_hash = signed_tx_hash(&payload.tx);
        let mut relay_submit_set = JoinSet::new();
        for relay in ranked_submit_paths
            .iter()
            .take(self.config.mev.relay_fanout_count.max(1))
            .cloned()
        {
            let relay_url = match Url::parse(&relay.relay) {
                Ok(url) => url,
                Err(err) => {
                    last_error = Some(err.to_string());
                    continue;
                }
            };
            let relay_signer = wallet.clone();
            let flashbots_client =
                SignerMiddleware::new(send_context.endpoint.provider.clone(), wallet.clone());
            let flashbots = FlashbotsMiddleware::new(flashbots_client, relay_url, relay_signer);
            let bundle = self.build_bundle(send_context.block, &opportunity, &payload);
            relay_submit_set.spawn(async move {
                let started = Instant::now();
                match flashbots.send_bundle(&bundle).await {
                    Ok(pending) => Ok((
                        relay,
                        started.elapsed().as_millis() as f64,
                        pending.bundle_hash,
                    )),
                    Err(err) => Err((relay, started.elapsed().as_millis() as f64, err.to_string())),
                }
            });
        }

        while let Some(result) = relay_submit_set.join_next().await {
            let Ok(submit_result) = result else {
                continue;
            };
            match submit_result {
                Ok((relay, submit_latency_ms, bundle_hash)) => {
                    if let Ok(mut adaptive) = self.adaptive.lock() {
                        adaptive.record_submit_success_for_relay(&relay.relay, submit_latency_ms);
                    }
                    self.dashboard.record_relay_outcome(RelaySnapshotUpdate {
                        relay: &relay.relay,
                        accepted: true,
                        submit_failed: false,
                        included_success: false,
                        included_revert: false,
                        not_included_timeout: false,
                        submit_latency_ms: Some(submit_latency_ms),
                        finalization_latency_ms: None,
                        score: Some(relay.score),
                        pressure: Some(relay.relay_pressure),
                        accept_rate: Some(relay.accept_rate),
                        inclusion_rate: Some(relay.inclusion_rate),
                    });
                    self.dashboard.record_latency(
                        "fee_bundle_submit",
                        submit_latency_ms as u128,
                        None,
                        Some(&format!(
                            "{} via {}",
                            send_context.endpoint.name, relay.relay
                        )),
                    );
                    self.dashboard.event(
                        "success",
                        format!(
                            "fee extraction bundle submitted victim={:?} relay={} bundle={:?} tx={:?} expected_profit={:.6} ETH",
                            opportunity.victim_tx,
                            format_submit_path(&payload.amm_kind, &relay.relay),
                            bundle_hash,
                            tx_hash,
                            wei_to_eth_f64(payload.expected_profit_wei)
                        ),
                    );
                    relay_submit_set.abort_all();
                    let realized = self
                        .observe_realized_pnl(
                            send_context.endpoint.id,
                            tx_hash,
                            &opportunity,
                            &payload,
                            &relay.relay,
                            target_block,
                            submit_latency_ms,
                        )
                        .await?;
                    self.record_result(
                        &realized,
                        Some(payload.pair),
                        Some(opportunity.router),
                        Some(relay.relay.as_str()),
                        Some(payload.context_priority_score),
                        Some(payload.context_toxicity_score),
                    );
                    return Ok(());
                }
                Err((relay, submit_latency_ms, err_text)) => {
                    warn!(
                        "fee extraction bundle failed via {}: {}",
                        relay.relay, err_text
                    );
                    if let Ok(mut adaptive) = self.adaptive.lock() {
                        adaptive.record_submit_failure_for_relay(&relay.relay, submit_latency_ms);
                        adaptive.record_contextual_outcome(
                            &relay.relay,
                            cluster,
                            payload.expected_profit_wei,
                            0.0,
                            ContextualOutcomeKind::SubmitFailed,
                        );
                    }
                    self.dashboard.record_relay_outcome(RelaySnapshotUpdate {
                        relay: &relay.relay,
                        accepted: false,
                        submit_failed: true,
                        included_success: false,
                        included_revert: false,
                        not_included_timeout: false,
                        submit_latency_ms: Some(submit_latency_ms),
                        finalization_latency_ms: None,
                        score: Some(relay.score),
                        pressure: Some(relay.relay_pressure),
                        accept_rate: Some(relay.accept_rate),
                        inclusion_rate: Some(relay.inclusion_rate),
                    });
                    self.dashboard.event(
                        "warn",
                        format!(
                            "fee extraction relay failed victim={:?} relay={}: {}",
                            opportunity.victim_tx, relay.relay, err_text
                        ),
                    );
                    last_error = Some(err_text);
                }
            }
        }

        self.record_result(
            &ExecutionResult {
                realized_profit: 0.0,
                gas_used: 0,
                success: false,
            },
            Some(payload.pair),
            Some(opportunity.router),
            ranked_submit_paths
                .first()
                .map(|quote| quote.relay.as_str()),
            Some(payload.context_priority_score),
            Some(payload.context_toxicity_score),
        );
        Err(last_error
            .unwrap_or_else(|| "no relay submission path available".to_string())
            .into())
    }

    async fn observe_realized_pnl(
        &self,
        preferred_endpoint_id: usize,
        tx_hash: H256,
        opportunity: &MevOpportunity,
        payload: &crate::mev::execution::payload_builder::ExecutionPayload,
        relay: &str,
        target_block: u64,
        submit_latency_ms: f64,
    ) -> Result<ExecutionResult, Box<dyn std::error::Error>> {
        let started = std::time::Instant::now();
        let cluster = ClusterKey {
            router: opportunity.router,
            token_in: opportunity.token_in,
            token_out: opportunity.token_out,
            selector: opportunity.selector,
        };
        for _ in 0..10 {
            for handle in self.read_probe_handles(preferred_endpoint_id) {
                let receipt_started = std::time::Instant::now();
                let receipt = match handle.provider.get_transaction_receipt(tx_hash).await {
                    Ok(receipt) => receipt,
                    Err(err) => {
                        let err_text = err.to_string();
                        self.rpc_fleet
                            .record_failure(handle.id, RpcFleet::classify_failure(&err_text));
                        continue;
                    }
                };

                let Some(receipt) = receipt else {
                    continue;
                };
                self.rpc_fleet
                    .record_success(handle.id, receipt_started.elapsed(), None);
                let gas_used = receipt.gas_used.unwrap_or_default().as_u64();
                let effective_gas_price = receipt.effective_gas_price.unwrap_or_default();
                let gas_paid_wei = receipt
                    .gas_used
                    .unwrap_or_default()
                    .saturating_mul(effective_gas_price);
                let success = receipt
                    .status
                    .map(|status| status.as_u64() == 1)
                    .unwrap_or(false);
                let realized_profit = if success {
                    self.realized_profit_eth(
                        handle.provider.clone(),
                        payload,
                        &receipt,
                        gas_paid_wei,
                    )
                    .await
                    .unwrap_or_else(|| {
                        wei_to_eth_f64(payload.expected_profit_wei) - wei_to_eth_f64(gas_paid_wei)
                    })
                } else {
                    -wei_to_eth_f64(gas_paid_wei)
                };

                let result = ExecutionResult {
                    realized_profit,
                    gas_used,
                    success,
                };
                if let Ok(mut adaptive) = self.adaptive.lock() {
                    adaptive.record_finalization_for_relay(
                        relay,
                        payload.expected_profit_wei,
                        result.realized_profit,
                        result.success,
                        started.elapsed().as_millis() as f64,
                    );
                    adaptive.record_contextual_outcome(
                        relay,
                        cluster,
                        payload.expected_profit_wei,
                        result.realized_profit,
                        if success {
                            ContextualOutcomeKind::IncludedSuccess
                        } else {
                            ContextualOutcomeKind::IncludedRevert
                        },
                    );
                }
                self.dashboard.record_relay_outcome(RelaySnapshotUpdate {
                    relay,
                    accepted: false,
                    submit_failed: false,
                    included_success: success,
                    included_revert: !success,
                    not_included_timeout: false,
                    submit_latency_ms: None,
                    finalization_latency_ms: Some(started.elapsed().as_millis() as f64),
                    score: None,
                    pressure: None,
                    accept_rate: None,
                    inclusion_rate: None,
                });
                self.dashboard.event(
                    if success { "success" } else { "warn" },
                    format!(
                        "fee extraction finalized relay={} tx={:?} success={} realized_pnl={:.6} ETH gas_used={}",
                        relay, tx_hash, success, result.realized_profit, result.gas_used
                    ),
                );
                self.record_execution_outcome(
                    relay,
                    target_block,
                    opportunity,
                    payload,
                    tx_hash,
                    if success {
                        "included_success"
                    } else {
                        "included_revert"
                    },
                    result.realized_profit,
                    result.gas_used,
                    submit_latency_ms,
                    started.elapsed().as_millis() as f64,
                );
                return Ok(result);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        self.dashboard.event(
            "warn",
            format!(
                "fee extraction receipt timeout relay={} tx={:?}: no realized pnl available yet",
                relay, tx_hash
            ),
        );
        if let Ok(mut adaptive) = self.adaptive.lock() {
            adaptive.record_receipt_timeout_for_relay(relay, started.elapsed().as_millis() as f64);
            adaptive.record_contextual_outcome(
                relay,
                cluster,
                payload.expected_profit_wei,
                0.0,
                ContextualOutcomeKind::AcceptedNotIncluded,
            );
        }
        self.dashboard.record_relay_outcome(RelaySnapshotUpdate {
            relay,
            accepted: false,
            submit_failed: false,
            included_success: false,
            included_revert: false,
            not_included_timeout: true,
            submit_latency_ms: None,
            finalization_latency_ms: Some(started.elapsed().as_millis() as f64),
            score: None,
            pressure: None,
            accept_rate: None,
            inclusion_rate: None,
        });
        self.record_execution_outcome(
            relay,
            target_block,
            opportunity,
            payload,
            tx_hash,
            "accepted_not_included",
            0.0,
            0,
            submit_latency_ms,
            started.elapsed().as_millis() as f64,
        );
        Ok(ExecutionResult {
            realized_profit: 0.0,
            gas_used: 0,
            success: false,
        })
    }

    async fn realized_profit_eth(
        &self,
        provider: Arc<Provider<Http>>,
        payload: &crate::mev::execution::payload_builder::ExecutionPayload,
        receipt: &TransactionReceipt,
        gas_paid_wei: U256,
    ) -> Option<f64> {
        let block_number = receipt.block_number?.as_u64();
        let pre_block = block_number.saturating_sub(1);
        let token = Erc20BalanceView::new(payload.profit_token, provider.clone());
        let pre_balance = token
            .balance_of(payload.profit_recipient)
            .block(BlockId::Number(BlockNumber::Number(pre_block.into())))
            .call()
            .await
            .ok()?;
        let post_balance = token
            .balance_of(payload.profit_recipient)
            .block(BlockId::Number(BlockNumber::Number(block_number.into())))
            .call()
            .await
            .ok()?;
        let balance_delta = post_balance.saturating_sub(pre_balance);
        let token_meta = self
            .config
            .monitored_tokens
            .iter()
            .find(|token| token.address == payload.profit_token)?;
        let token_units = 10f64.powi(i32::from(token_meta.decimals));
        let delta_tokens = balance_delta.to_string().parse::<f64>().ok()? / token_units;
        let gross_eth = delta_tokens * token_meta.price_eth;
        Some(gross_eth - wei_to_eth_f64(gas_paid_wei))
    }

    async fn load_send_context(&self, wallet_address: Address) -> Result<SendContext, String> {
        let mut last_error: Option<String> = None;
        for endpoint in self.rpc_fleet.send_candidates(3) {
            self.rpc_fleet.reserve_send_selection(endpoint.id);
            let started = std::time::Instant::now();
            let result = async {
                let (hot_balance, chain_nonce, gas_price, block) = tokio::try_join!(
                    endpoint
                        .provider
                        .get_balance(wallet_address, Some(BlockNumber::Pending.into())),
                    endpoint
                        .provider
                        .get_transaction_count(wallet_address, Some(BlockNumber::Pending.into())),
                    endpoint.provider.get_gas_price(),
                    endpoint.provider.get_block_number(),
                )
                .map_err(|err| err.to_string())?;

                Ok::<_, String>(SendContext {
                    endpoint: endpoint.clone(),
                    hot_balance,
                    chain_nonce,
                    gas_price,
                    block,
                })
            }
            .await;

            match result {
                Ok(context) => {
                    self.rpc_fleet.record_success(
                        context.endpoint.id,
                        started.elapsed(),
                        Some(context.block.as_u64()),
                    );
                    return Ok(context);
                }
                Err(err) => {
                    self.rpc_fleet
                        .record_failure(endpoint.id, RpcFleet::classify_failure(&err));
                    last_error = Some(format!("{} via {}", err, endpoint.name));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| "no rpc endpoint available for send context".to_string()))
    }

    fn read_probe_handles(&self, preferred_endpoint_id: usize) -> Vec<RpcHandle> {
        let mut handles = Vec::new();
        if let Some(primary) = self
            .rpc_fleet
            .all_handles()
            .into_iter()
            .find(|handle| handle.id == preferred_endpoint_id)
        {
            handles.push(primary);
        }
        for handle in self.rpc_fleet.read_candidates(3) {
            if !handles.iter().any(|existing| existing.id == handle.id) {
                handles.push(handle);
            }
        }
        handles
    }

    fn record_result(
        &self,
        result: &ExecutionResult,
        pair: Option<Address>,
        router: Option<Address>,
        submit_path: Option<&str>,
        context_priority_score: Option<f64>,
        context_toxicity_score: Option<f64>,
    ) {
        let mut global_freeze_armed = false;
        let mut contextual_freeze_armed = false;
        {
            let mut guard = match self.execution_guard.lock() {
                Ok(guard) => guard,
                Err(_) => return,
            };
            if result.success && result.realized_profit > 0.0 {
                guard.consecutive_losses = 0;
                guard.frozen_until = None;
            } else {
                guard.consecutive_losses = guard.consecutive_losses.saturating_add(1);
                if guard.consecutive_losses >= self.config.mev.stop_loss_consecutive_losses {
                    guard.frozen_until = Some(
                        Instant::now() + Duration::from_secs(self.config.mev.stop_loss_freeze_secs),
                    );
                    global_freeze_armed = true;
                }
            }
            if let (Some(pair), Some(router), Some(submit_path)) = (pair, router, submit_path) {
                let key = ContextFreezeKey {
                    pair,
                    router,
                    submit_path: submit_path.to_string(),
                };
                let state = guard
                    .context_guards
                    .entry(key)
                    .or_insert_with(ContextExecutionGuard::default);
                let now = Instant::now();
                decay_context_guard(
                    state,
                    now,
                    self.config.mev.context_stop_loss_freeze_secs.max(1),
                );
                let regime_pressure = contextual_regime_pressure(
                    context_priority_score.unwrap_or(0.5),
                    context_toxicity_score.unwrap_or(0.5),
                    result.success,
                    result.realized_profit,
                );
                state.regime_pressure_ewma =
                    ewma_f64(state.regime_pressure_ewma, regime_pressure, 0.35);
                state.last_event_at = Some(now);
                if result.success && result.realized_profit > 0.0 {
                    state.consecutive_losses = 0;
                    state.frozen_until = None;
                } else {
                    let loss_increment = if state.regime_pressure_ewma >= 0.82 {
                        2
                    } else {
                        1
                    };
                    state.consecutive_losses =
                        state.consecutive_losses.saturating_add(loss_increment);
                    let dynamic_threshold = contextual_stop_loss_threshold(
                        self.config.mev.context_stop_loss_consecutive_losses,
                        state.regime_pressure_ewma,
                    );
                    if state.consecutive_losses >= dynamic_threshold {
                        let freeze_secs = contextual_freeze_secs(
                            self.config.mev.context_stop_loss_freeze_secs,
                            state.regime_pressure_ewma,
                        );
                        state.frozen_until = Some(now + Duration::from_secs(freeze_secs));
                        contextual_freeze_armed = true;
                    }
                }
            }
            prune_context_guards(&mut guard.context_guards);
        }
        if let Ok(mut pnl) = self.pnl.lock() {
            pnl.record(result);
            self.dashboard.event(
                "info",
                format!(
                    "realized pnl update executions={} failures={} daily_pnl={:.6} ETH realized_profit={:.6} ETH realized_loss={:.6} ETH",
                    pnl.executions,
                    pnl.failures,
                    pnl.daily_pnl_eth,
                    pnl.realized_profit_eth,
                    pnl.realized_loss_eth
                ),
            );
        }
        if global_freeze_armed {
            self.dashboard.event(
                "warn",
                format!(
                    "stop-loss armed: consecutive_losses>={} freeze_secs={}",
                    self.config.mev.stop_loss_consecutive_losses,
                    self.config.mev.stop_loss_freeze_secs
                ),
            );
        }
        if contextual_freeze_armed {
            if let (Some(pair), Some(router), Some(submit_path)) = (pair, router, submit_path) {
                self.dashboard.event(
                    "warn",
                    format!(
                        "contextual stop-loss armed: pair={:?} router={:?} path={} consecutive_losses>={} freeze_secs={}",
                        pair,
                        router,
                        submit_path,
                        self.config.mev.context_stop_loss_consecutive_losses,
                        contextual_freeze_secs(
                            self.config.mev.context_stop_loss_freeze_secs,
                            contextual_regime_pressure(
                                context_priority_score.unwrap_or(0.5),
                                context_toxicity_score.unwrap_or(0.5),
                                result.success,
                                result.realized_profit,
                            ),
                        )
                    ),
                );
            }
        }
    }

    fn stop_loss_snapshot(&self) -> Option<StopLossSnapshot> {
        let Ok(mut guard) = self.execution_guard.lock() else {
            return None;
        };
        let frozen_until = guard.frozen_until?;
        let now = Instant::now();
        if now >= frozen_until {
            guard.frozen_until = None;
            guard.consecutive_losses = 0;
            return None;
        }
        Some(StopLossSnapshot {
            consecutive_losses: guard.consecutive_losses,
            remaining_freeze_secs: frozen_until.saturating_duration_since(now).as_secs(),
            scope: "global",
        })
    }

    fn context_stop_loss_snapshot(
        &self,
        pair: Address,
        router: Address,
        submit_path: &str,
    ) -> Option<StopLossSnapshot> {
        let Ok(mut guard) = self.execution_guard.lock() else {
            return None;
        };
        let key = ContextFreezeKey {
            pair,
            router,
            submit_path: submit_path.to_string(),
        };
        let state = guard.context_guards.get_mut(&key)?;
        let now = Instant::now();
        decay_context_guard(
            state,
            now,
            self.config.mev.context_stop_loss_freeze_secs.max(1),
        );
        let frozen_until = state.frozen_until?;
        if now >= frozen_until {
            state.frozen_until = None;
            state.consecutive_losses = 0;
            guard.context_guards.remove(&key);
            return None;
        }
        Some(StopLossSnapshot {
            consecutive_losses: state.consecutive_losses,
            remaining_freeze_secs: frozen_until.saturating_duration_since(now).as_secs(),
            scope: "contextual",
        })
    }

    fn dynamic_gas_price(
        &self,
        base_gas_price: U256,
        best_relay: Option<&RelayQuote>,
        payload: &crate::mev::execution::payload_builder::ExecutionPayload,
    ) -> U256 {
        let mut extra_bps = self.config.mev.gas_overpay_base_extra_bps;

        if let Some(relay) = best_relay {
            if relay.accepted_not_included_rate >= 0.10 {
                extra_bps = extra_bps.saturating_add(self.config.mev.gas_overpay_miss_extra_bps);
            }
            if relay.revert_rate >= 0.05 {
                extra_bps = extra_bps.saturating_add(self.config.mev.gas_overpay_revert_extra_bps);
            }
            if relay.accept_rate <= 0.60 {
                extra_bps =
                    extra_bps.saturating_add(self.config.mev.gas_overpay_submit_failure_extra_bps);
            }
        }

        if payload.context_toxicity_score >= self.config.mev.capital_multiplier_toxicity_threshold {
            extra_bps = extra_bps.saturating_add(self.config.mev.gas_overpay_miss_extra_bps / 2);
        } else if payload.context_priority_score
            >= self.config.mev.capital_multiplier_priority_threshold
        {
            extra_bps = extra_bps.saturating_add(self.config.mev.gas_overpay_base_extra_bps / 2);
        }

        extra_bps = extra_bps.min(self.config.mev.gas_overpay_max_extra_bps);
        let adjusted = base_gas_price.saturating_mul(U256::from(10_000u64 + extra_bps))
            / U256::from(10_000u64);
        if let Some(cap) = self.config.mev.max_gas_price_wei() {
            adjusted.min(cap)
        } else {
            adjusted
        }
    }

    fn rank_relay_submit_paths(
        &self,
        preferred_relay: Option<&str>,
        cluster: ClusterKey,
    ) -> Vec<RelayQuote> {
        self.rank_relays(preferred_relay, cluster)
    }

    fn rank_direct_rpc_submit_paths(
        &self,
        preferred_endpoint: &RpcHandle,
        cluster: ClusterKey,
    ) -> Vec<RelayQuote> {
        let submit_endpoints = self.direct_rpc_submit_endpoints(preferred_endpoint);
        let relay_labels = submit_endpoints
            .iter()
            .map(|endpoint| format!("rpc://{}", endpoint.name))
            .collect::<Vec<_>>();
        if let Ok(adaptive) = self.adaptive.lock() {
            adaptive.rank_relays_for_cluster(&relay_labels, cluster)
        } else {
            relay_labels
                .into_iter()
                .map(|relay| RelayQuote {
                    relay,
                    relay_pressure: 0.0,
                    accept_rate: 1.0,
                    inclusion_rate: 1.0,
                    accepted_not_included_rate: 0.0,
                    revert_rate: 0.0,
                    submit_latency_ms: 0.0,
                    finalization_latency_ms: 0.0,
                    score: 0.0,
                })
                .collect()
        }
    }

    fn direct_rpc_submit_endpoints(&self, preferred_endpoint: &RpcHandle) -> Vec<RpcHandle> {
        let mut ranked_by_health = self
            .rpc_fleet
            .send_candidates(self.config.mev.rpc_fanout_count.max(1));
        if !ranked_by_health
            .iter()
            .any(|endpoint| endpoint.id == preferred_endpoint.id)
        {
            ranked_by_health.insert(0, preferred_endpoint.clone());
        }

        let ranked_by_path = self.rank_direct_rpc_submit_paths_from_candidates(&ranked_by_health);
        let mut ordered = Vec::with_capacity(ranked_by_health.len());
        for relay in ranked_by_path {
            if let Some(endpoint) = ranked_by_health
                .iter()
                .find(|endpoint| relay.relay == format!("rpc://{}", endpoint.name))
                .cloned()
            {
                if !ordered
                    .iter()
                    .any(|existing: &RpcHandle| existing.id == endpoint.id)
                {
                    ordered.push(endpoint);
                }
            }
        }
        for endpoint in ranked_by_health {
            if !ordered.iter().any(|existing| existing.id == endpoint.id) {
                ordered.push(endpoint);
            }
        }
        ordered.truncate(self.config.mev.rpc_fanout_count.max(1));
        ordered
    }

    fn direct_rpc_submit_endpoints_from_ranked_paths(
        &self,
        ranked_submit_paths: &[RelayQuote],
    ) -> Vec<RpcHandle> {
        let mut candidates = self
            .rpc_fleet
            .send_candidates(self.config.mev.rpc_fanout_count.max(1));
        let mut ordered = Vec::with_capacity(candidates.len());
        for relay in ranked_submit_paths {
            if let Some(endpoint_name) = relay.relay.strip_prefix("rpc://") {
                if let Some(index) = candidates
                    .iter()
                    .position(|endpoint| endpoint.name == endpoint_name)
                {
                    let endpoint = candidates.remove(index);
                    ordered.push(endpoint);
                }
            }
        }
        ordered.extend(candidates);
        ordered.truncate(self.config.mev.rpc_fanout_count.max(1));
        ordered
    }

    fn rank_direct_rpc_submit_paths_from_candidates(
        &self,
        candidates: &[RpcHandle],
    ) -> Vec<RelayQuote> {
        let relay_labels = candidates
            .iter()
            .map(|endpoint| format!("rpc://{}", endpoint.name))
            .collect::<Vec<_>>();
        if let Ok(adaptive) = self.adaptive.lock() {
            adaptive.rank_relays(&relay_labels)
        } else {
            relay_labels
                .into_iter()
                .map(|relay| RelayQuote {
                    relay,
                    relay_pressure: 0.0,
                    accept_rate: 1.0,
                    inclusion_rate: 1.0,
                    accepted_not_included_rate: 0.0,
                    revert_rate: 0.0,
                    submit_latency_ms: 0.0,
                    finalization_latency_ms: 0.0,
                    score: 0.0,
                })
                .collect()
        }
    }

    fn check_and_reserve_budget(
        &self,
        opportunity: &MevOpportunity,
        payload: &crate::mev::execution::payload_builder::ExecutionPayload,
        cluster: ClusterKey,
    ) -> BudgetDecision {
        let capital_eth = wei_to_eth_f64(payload.capital_committed_wei);
        let pair = payload.pair;
        let Ok(mut budget) = self.capital_budget.lock() else {
            return BudgetDecision {
                allowed: true,
                reject_reason: "budget_lock_unavailable",
                capital_eth,
                total_window_eth: 0.0,
                cluster_window_eth: 0.0,
                pair_window_eth: 0.0,
            };
        };
        budget.check_and_reserve(
            capital_eth,
            cluster,
            pair,
            opportunity.victim_tx,
            &self.config,
        )
    }

    fn record_execution_outcome(
        &self,
        relay: &str,
        target_block: u64,
        opportunity: &MevOpportunity,
        payload: &crate::mev::execution::payload_builder::ExecutionPayload,
        tx_hash: H256,
        outcome: &str,
        realized_profit_eth: f64,
        gas_used: u64,
        submit_latency_ms: f64,
        finalization_latency_ms: f64,
    ) {
        let pair = format!("{:?}", payload.pair);
        let router = format!("{:?}", opportunity.router);
        let token_in = format!("{:?}", opportunity.token_in);
        let token_out = format!("{:?}", opportunity.token_out);
        let victim_tx = format!("{:?}", tx_hash);
        self.dashboard
            .record_execution_outcome(ExecutionOutcomeUpdate {
                relay,
                target_block,
                pair: &pair,
                router: &router,
                token_in: &token_in,
                token_out: &token_out,
                victim_tx: &victim_tx,
                outcome,
                expected_profit_eth: wei_to_eth_f64(payload.expected_profit_wei),
                realized_profit_eth,
                gas_used,
                submit_latency_ms,
                finalization_latency_ms,
            });
    }

    fn rank_relays(&self, preferred_relay: Option<&str>, cluster: ClusterKey) -> Vec<RelayQuote> {
        let mut relays = if let Ok(adaptive) = self.adaptive.lock() {
            adaptive.rank_relays_for_cluster(&self.config.builder_relays, cluster)
        } else {
            self.config
                .builder_relays
                .iter()
                .map(|relay| RelayQuote {
                    relay: relay.clone(),
                    relay_pressure: 0.0,
                    accept_rate: 1.0,
                    inclusion_rate: 1.0,
                    accepted_not_included_rate: 0.0,
                    revert_rate: 0.0,
                    submit_latency_ms: 0.0,
                    finalization_latency_ms: 0.0,
                    score: 0.0,
                })
                .collect()
        };
        if let Some(preferred) = preferred_relay {
            if let Some(index) = relays.iter().position(|relay| relay.relay == preferred) {
                let preferred_quote = relays.remove(index);
                relays.insert(0, preferred_quote);
            }
        }
        relays
    }

    fn build_bundle(
        &self,
        block: U64,
        opportunity: &MevOpportunity,
        payload: &crate::mev::execution::payload_builder::ExecutionPayload,
    ) -> BundleRequest {
        let mut bundle = BundleRequest::new().set_block(block + 1);
        if let Some(victim) = opportunity.victim_transaction.clone() {
            bundle = bundle.push_revertible_transaction(victim);
        }
        bundle.push_transaction(payload.tx.clone())
    }

    fn hot_wallet_status(&self, balance_eth: f64) -> &'static str {
        if balance_eth < self.config.mev.executor_min_buffer_eth {
            "underfunded"
        } else if balance_eth > self.config.mev.executor_max_buffer_eth {
            "overfunded"
        } else if balance_eth < self.config.mev.executor_target_buffer_eth {
            "below_target"
        } else {
            "healthy"
        }
    }

    fn maybe_emit_treasury_signal(&self, balance_eth: f64) {
        let signal = self.treasury_signal(balance_eth);
        let rounded_amount = (signal.recommended_amount_eth * 1_000_000.0).round() as u64;

        let should_emit = {
            let Ok(mut guard) = self.last_treasury_signal.lock() else {
                return;
            };
            match guard.as_ref() {
                Some(previous)
                    if previous.action == signal.action
                        && previous.status == signal.status
                        && previous.rounded_amount == rounded_amount =>
                {
                    false
                }
                _ => {
                    *guard = Some(TreasurySignal {
                        action: signal.action.to_string(),
                        status: signal.status.to_string(),
                        rounded_amount,
                    });
                    true
                }
            }
        };

        if !should_emit {
            return;
        }

        let executor_address = format!("{:?}", self.config.executor_address);
        let vault_address = format!("{:?}", self.config.vault_address);
        let profit_address = format!("{:?}", self.config.profit_address);

        self.dashboard
            .record_treasury_recommendation(TreasuryRecommendationUpdate {
                executor_address: &executor_address,
                vault_address: &vault_address,
                profit_address: &profit_address,
                balance_eth,
                min_buffer_eth: self.config.mev.executor_min_buffer_eth,
                target_buffer_eth: self.config.mev.executor_target_buffer_eth,
                max_buffer_eth: self.config.mev.executor_max_buffer_eth,
                action: signal.action,
                recommended_amount_eth: signal.recommended_amount_eth,
                status: signal.status,
                note: signal.note,
            });
        self.dashboard.event(
            if signal.status == "critical" {
                "warn"
            } else {
                "info"
            },
            format!(
                "treasury {} executor_balance={:.6} ETH target={:.6} ETH amount={:.6} ETH note={}",
                signal.action,
                balance_eth,
                self.config.mev.executor_target_buffer_eth,
                signal.recommended_amount_eth,
                signal.note
            ),
        );
    }

    fn treasury_signal(&self, balance_eth: f64) -> TreasurySignalView<'static> {
        let target = self.config.mev.executor_target_buffer_eth;
        let min = self.config.mev.executor_min_buffer_eth;
        let max = self.config.mev.executor_max_buffer_eth;

        if balance_eth < min {
            TreasurySignalView {
                action: "fund_executor",
                status: "critical",
                recommended_amount_eth: (target - balance_eth).max(0.0),
                note: "executor below min buffer; fund from vault before next execution burst",
            }
        } else if balance_eth < target {
            TreasurySignalView {
                action: "fund_executor",
                status: "recommended",
                recommended_amount_eth: (target - balance_eth).max(0.0),
                note: "executor below target buffer; top up from vault to restore burst capacity",
            }
        } else if balance_eth > max {
            TreasurySignalView {
                action: "sweep_to_vault",
                status: "critical",
                recommended_amount_eth: (balance_eth - target).max(0.0),
                note: "executor above max buffer; sweep excess back to vault to reduce hot-wallet exposure",
            }
        } else if balance_eth > target {
            TreasurySignalView {
                action: "sweep_to_vault",
                status: "recommended",
                recommended_amount_eth: (balance_eth - target).max(0.0),
                note: "executor above target buffer; sweep excess to vault while keeping execution headroom",
            }
        } else {
            TreasurySignalView {
                action: "hold",
                status: "healthy",
                recommended_amount_eth: 0.0,
                note: "executor buffer aligned with target; no treasury rebalance required",
            }
        }
    }
}

#[derive(Clone)]
struct BudgetReservation {
    reserved_at: std::time::Instant,
    cluster: ClusterKey,
    pair: Address,
    capital_eth: f64,
}

struct CapitalBudget {
    reservations: VecDeque<BudgetReservation>,
}

struct BudgetDecision {
    allowed: bool,
    reject_reason: &'static str,
    capital_eth: f64,
    total_window_eth: f64,
    cluster_window_eth: f64,
    pair_window_eth: f64,
}

impl CapitalBudget {
    fn new(_config: &Config) -> Self {
        Self {
            reservations: VecDeque::new(),
        }
    }

    fn check_and_reserve(
        &mut self,
        capital_eth: f64,
        cluster: ClusterKey,
        pair: Address,
        _victim_tx: H256,
        config: &Config,
    ) -> BudgetDecision {
        self.prune(config.mev.capital_window_secs);

        let mut total_window_eth = 0.0;
        let mut cluster_window_eth = 0.0;
        let mut pair_window_eth = 0.0;
        for reservation in &self.reservations {
            total_window_eth += reservation.capital_eth;
            if reservation.cluster == cluster {
                cluster_window_eth += reservation.capital_eth;
            }
            if reservation.pair == pair {
                pair_window_eth += reservation.capital_eth;
            }
        }

        let total_after = total_window_eth + capital_eth;
        let cluster_after = cluster_window_eth + capital_eth;
        let pair_after = pair_window_eth + capital_eth;

        let reject_reason = if total_after > config.mev.max_window_exposure_eth {
            Some("window_exposure_limit")
        } else if cluster_after > config.mev.max_cluster_window_exposure_eth {
            Some("cluster_window_limit")
        } else if pair_after > config.mev.max_pair_window_exposure_eth {
            Some("pair_window_limit")
        } else {
            None
        };

        if reject_reason.is_none() {
            self.reservations.push_back(BudgetReservation {
                reserved_at: std::time::Instant::now(),
                cluster,
                pair,
                capital_eth,
            });
        }

        BudgetDecision {
            allowed: reject_reason.is_none(),
            reject_reason: reject_reason.unwrap_or("allowed"),
            capital_eth,
            total_window_eth: total_after,
            cluster_window_eth: cluster_after,
            pair_window_eth: pair_after,
        }
    }

    fn prune(&mut self, window_secs: u64) {
        while matches!(
            self.reservations.front(),
            Some(entry) if entry.reserved_at.elapsed().as_secs() > window_secs
        ) {
            self.reservations.pop_front();
        }
    }
}

struct TreasurySignal {
    action: String,
    status: String,
    rounded_amount: u64,
}

struct TreasurySignalView<'a> {
    action: &'a str,
    status: &'a str,
    recommended_amount_eth: f64,
    note: &'a str,
}

fn signed_tx_hash(raw: &Bytes) -> H256 {
    H256::from(ethers::utils::keccak256(raw.as_ref()))
}

fn wei_to_gwei_f64(value: U256) -> f64 {
    value.to_string().parse::<f64>().unwrap_or(0.0) / 1e9
}

fn is_insufficient_funds_error(message: &str) -> bool {
    let lowered = message.to_ascii_lowercase();
    lowered.contains("insufficient funds")
        || lowered.contains("insufficient balance")
        || lowered.contains("funds for gas")
        || lowered.contains("not enough funds")
}

fn relay_snapshot(quote: &RelayQuote) -> RelaySnapshot {
    RelaySnapshot {
        relay: quote.relay.clone(),
        score: quote.score,
        pressure: quote.relay_pressure,
        accept_rate: quote.accept_rate,
        inclusion_rate: quote.inclusion_rate,
        accepted: 0,
        submit_failed: 0,
        included_success: 0,
        included_revert: 0,
        not_included_timeout: 0,
        submit_latency_ms: quote.submit_latency_ms,
        finalization_latency_ms: quote.finalization_latency_ms,
    }
}

fn format_submit_path(amm_kind: &AmmRouteKind, relay: &str) -> String {
    match amm_kind {
        AmmRouteKind::UniswapV2 => format!("v2@{relay}"),
        AmmRouteKind::UniswapV3 { fee_tier, .. } => format!("v3:{}@{relay}", fee_tier),
    }
}

fn prune_context_guards(context_guards: &mut HashMap<ContextFreezeKey, ContextExecutionGuard>) {
    let now = Instant::now();
    context_guards.retain(|_, state| match state.frozen_until {
        Some(frozen_until) => now < frozen_until || state.consecutive_losses > 0,
        None => state.consecutive_losses > 0,
    });
}

fn decay_context_guard(state: &mut ContextExecutionGuard, now: Instant, base_freeze_secs: u64) {
    let Some(last_event_at) = state.last_event_at else {
        return;
    };
    let decay_window_secs = (base_freeze_secs / 2).max(30);
    let elapsed_secs = now.saturating_duration_since(last_event_at).as_secs();
    if elapsed_secs < decay_window_secs || state.consecutive_losses == 0 {
        return;
    }
    let decay_steps = (elapsed_secs / decay_window_secs) as u32;
    state.consecutive_losses = state.consecutive_losses.saturating_sub(decay_steps.max(1));
    state.last_event_at = Some(now);
    state.regime_pressure_ewma *= 0.88_f64.powi(decay_steps.max(1) as i32);
}

fn contextual_regime_pressure(
    context_priority_score: f64,
    context_toxicity_score: f64,
    success: bool,
    realized_profit: f64,
) -> f64 {
    let toxicity = context_toxicity_score.clamp(0.0, 1.0);
    let low_priority = (1.0 - context_priority_score.clamp(0.0, 1.0)).clamp(0.0, 1.0);
    let outcome_penalty = if success && realized_profit > 0.0 {
        0.0
    } else {
        0.24
    };
    (toxicity * 0.62 + low_priority * 0.22 + outcome_penalty).clamp(0.0, 1.0)
}

fn contextual_stop_loss_threshold(base_threshold: u32, regime_pressure: f64) -> u32 {
    if regime_pressure >= 0.78 {
        base_threshold.saturating_sub(1).max(1)
    } else {
        base_threshold.max(1)
    }
}

fn contextual_freeze_secs(base_freeze_secs: u64, regime_pressure: f64) -> u64 {
    let multiplier = if regime_pressure >= 0.90 {
        2.00
    } else if regime_pressure >= 0.78 {
        1.50
    } else if regime_pressure >= 0.62 {
        1.20
    } else {
        1.0
    };
    ((base_freeze_secs as f64) * multiplier).round() as u64
}

fn ewma_f64(current: f64, sample: f64, alpha: f64) -> f64 {
    if current <= f64::EPSILON {
        sample
    } else {
        current + (sample - current) * alpha
    }
}

async fn sign_executor_transaction(
    wallet: &LocalWallet,
    payload: &crate::mev::execution::payload_builder::ExecutionPayload,
    nonce: U256,
    gas_price: U256,
) -> Result<Bytes, Box<dyn std::error::Error>> {
    let tx: TypedTransaction = TransactionRequest::new()
        .to(payload.target_contract)
        .data(payload.calldata.clone())
        .value(payload.value)
        .gas(payload.gas_limit)
        .gas_price(gas_price)
        .nonce(nonce)
        .from(wallet.address())
        .into();
    let signature = wallet.sign_transaction(&tx).await?;
    Ok(tx.rlp_signed(&signature))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, MevConfig, MonitoredTokenConfig, RpcPreference};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::path::PathBuf;

    fn test_config(network: &str) -> Config {
        Config {
            wallets: PathBuf::from("keys.txt"),
            network: network.to_string(),
            chain_id: match network {
                "bsc" => 56,
                "polygon" => 137,
                _ => 1,
            },
            allow_send: true,
            tenderly_rpc_only: false,
            alchemy_keys: vec!["test".to_string()],
            infura_ids: Vec::new(),
            flashbots_relay: if network == "ethereum" {
                "https://relay.flashbots.net".to_string()
            } else {
                String::new()
            },
            builder_relays: if network == "ethereum" {
                vec!["https://relay.flashbots.net".to_string()]
            } else {
                Vec::new()
            },
            executor_private_key:
                "0x59c6995e998f97a5a0044966f0945382d7a7d4f6d8f1f0db6b90e6a2f17d5f52".to_string(),
            executor_address: Address::from_low_u64_be(10),
            vault_address: Address::from_low_u64_be(11),
            profit_address: Address::from_low_u64_be(12),
            control_address: Address::from_low_u64_be(13),
            monitored_tokens: vec![MonitoredTokenConfig {
                address: Address::from_low_u64_be(100),
                decimals: 18,
                price_eth: 1.0,
            }],
            estimated_exec_gas: 250_000,
            estimated_bundle_overhead_gas: 25_000,
            max_infura_endpoints: 1,
            rpc_read_preference: RpcPreference::Auto,
            rpc_send_preference: RpcPreference::Auto,
            storage_path: PathBuf::from("test.sqlite"),
            dashboard_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8787),
            explicit_rpc_urls: Vec::new(),
            mempool_ws_urls: Vec::new(),
            mev: MevConfig {
                enabled: true,
                capital_eth: 0.5,
                capital_window_secs: 90,
                max_window_exposure_eth: 0.30,
                max_cluster_window_exposure_eth: 0.20,
                max_pair_window_exposure_eth: 0.24,
                min_net_profit_eth: 0.001,
                min_roi_bps: 800,
                min_large_swap_eth: 5.0,
                gas_safety_margin_bps: 12_500,
                max_pending_age_ms: 1500,
                max_gas_per_tx: 260_000,
                max_gas_price_gwei: Some(30),
                max_price_impact_bps: 250,
                slippage_protection_bps: 50,
                min_profit_usd: 2.0,
                eth_usd_price: 3000.0,
                min_liquidity_eth: 10.0,
                latency_trace: false,
                latency_trace_warn_us: 5_000,
                pool_state_cache_ttl_ms: 120,
                executor_min_buffer_eth: 0.1,
                executor_target_buffer_eth: 0.3,
                executor_max_buffer_eth: 1.0,
                relay_fanout_count: 3,
                rpc_fanout_count: 2,
                gas_overpay_base_extra_bps: 500,
                gas_overpay_miss_extra_bps: 2_500,
                gas_overpay_revert_extra_bps: 1_200,
                gas_overpay_submit_failure_extra_bps: 1_500,
                gas_overpay_max_extra_bps: 5_000,
                stop_loss_consecutive_losses: 3,
                stop_loss_freeze_secs: 300,
                context_stop_loss_consecutive_losses: 2,
                context_stop_loss_freeze_secs: 180,
                capital_multiplier_aggressive: 2.0,
                capital_multiplier_neutral: 1.0,
                capital_multiplier_defensive: 0.3,
                capital_multiplier_priority_threshold: 0.60,
                capital_multiplier_toxicity_threshold: 0.65,
                uniswap_v2_factory: Some(Address::from_low_u64_be(20)),
                uniswap_v3_factory: Some(Address::from_low_u64_be(22)),
                mev_executor: Some(Address::from_low_u64_be(21)),
            },
        }
    }

    #[test]
    fn capital_budget_blocks_cluster_exposure_before_submit() {
        let config = test_config("bsc");
        let cluster = ClusterKey {
            router: Address::from_low_u64_be(1),
            token_in: Address::from_low_u64_be(2),
            token_out: Address::from_low_u64_be(3),
            selector: [0x38, 0xed, 0x17, 0x39],
        };
        let mut budget = CapitalBudget::new(&config);
        let first = budget.check_and_reserve(
            0.12,
            cluster,
            Address::from_low_u64_be(4),
            H256::from_low_u64_be(1),
            &config,
        );
        let second = budget.check_and_reserve(
            0.12,
            cluster,
            Address::from_low_u64_be(5),
            H256::from_low_u64_be(2),
            &config,
        );

        assert!(first.allowed);
        assert!(!second.allowed);
        assert_eq!(second.reject_reason, "cluster_window_limit");
    }

    #[test]
    fn config_switches_bundle_path_by_chain() {
        assert!(!test_config("bsc").uses_bundle_relays());
        assert!(!test_config("polygon").uses_bundle_relays());
        assert!(test_config("ethereum").uses_bundle_relays());
    }

    #[test]
    fn contextual_guard_decays_after_quiet_window() {
        let mut guard = ContextExecutionGuard {
            consecutive_losses: 3,
            frozen_until: None,
            last_event_at: Some(Instant::now() - Duration::from_secs(120)),
            regime_pressure_ewma: 0.90,
        };
        decay_context_guard(&mut guard, Instant::now(), 60);
        assert!(guard.consecutive_losses < 3);
        assert!(guard.regime_pressure_ewma < 0.90);
    }
}
