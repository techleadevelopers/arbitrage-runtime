mod benchmark;
mod config;
mod dashboard;
mod mev;
mod replay;
mod rpc;
mod storage;
mod wallets;

#[cfg(unix)]
use tikv_jemallocator::Jemalloc;

#[cfg(unix)]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use benchmark::{maybe_run_network_benchmark, maybe_run_runtime_load_test};
use config::Config;
use dashboard::{DashboardHandle, WalletSnapshot};
use ethers::providers::Middleware;
use ethers::types::U256;
#[cfg(target_os = "linux")]
use mev::execution::pinning::ThreadPinningConfig;
use replay::maybe_run_replay_harness;
use rpc::RpcFleet;
use std::sync::Arc;
use std::time::Instant;
use storage::Storage;
use tracing::{error, info};
use wallets::load_wallets;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .init();

    info!("Fee Extraction Engine");
    info!("Runtime objective: mempool -> AMM impact -> execution -> realized pnl");

    let config = Arc::new(Config::load()?);
    #[cfg(target_os = "linux")]
    if std::env::var("RUN_RUNTIME_LOAD_TEST")
        .unwrap_or_default()
        .trim()
        .eq_ignore_ascii_case("true")
    {
        let pinning = ThreadPinningConfig::auto_detect();
        info!(
            "Linux runtime load test pinning plan: network_core={} decode_core={} eval_core={} exec_core={} numa_node={}",
            pinning.network_core,
            pinning.decode_core,
            pinning.eval_core,
            pinning.exec_core,
            pinning.numa_node
        );
    }
    if maybe_run_runtime_load_test(config.clone()).await? {
        return Ok(());
    }

    let rpc_fleet = Arc::new(RpcFleet::from_config(&config)?);
    let storage = Storage::new(&config.storage_path, &config.network).await?;
    let loaded_wallets = load_wallets(&config.wallets, config.chain_id)?;
    let total_read = loaded_wallets.total_read;
    let duplicate_keys = loaded_wallets.duplicates;
    let invalid_keys = loaded_wallets.invalid;
    let unique_wallets = loaded_wallets.unique;
    let wallets = loaded_wallets.wallets;

    info!("Wallet source: {}", config.wallets.display());
    info!("Keys read: {}", total_read);
    info!("Unique wallets: {}", unique_wallets);
    info!("Duplicate keys ignored: {}", duplicate_keys);
    info!("Invalid keys ignored: {}", invalid_keys);
    info!("Network: {}", config.network);
    info!("Chain id: {}", config.chain_id);
    info!("Allow send: {}", config.allow_send);
    info!("Tenderly RPC only: {}", config.tenderly_rpc_only);
    info!("Vault wallet: {:?}", config.vault_address);
    info!("Executor wallet: {:?}", config.executor_address);
    info!("Profit wallet: {:?}", config.profit_address);
    info!(
        "RPC preference read/send: {}/{}",
        config.rpc_read_preference.as_str(),
        config.rpc_send_preference.as_str()
    );
    info!(
        "Execution path: {}",
        if config.uses_bundle_relays() {
            "bundle-relay"
        } else {
            "direct-rpc"
        }
    );
    info!("Storage: {}", config.storage_path.display());
    info!("RPC endpoints configured: {}", rpc_fleet.endpoint_count());
    if config.tenderly_rpc_only {
        info!(
            "Tenderly fork RPC: {}",
            config
                .fork_rpc_url()
                .unwrap_or_else(|| "<missing fork url>".to_string())
        );
    }
    info!("Dashboard: http://{}", config.dashboard_addr);
    info!("Fee extraction enabled: {}", config.mev.enabled);
    info!(
        "Impact gate: min_large_swap={:.3} {} min_profit={:.6} {} min_roi={}bps",
        config.mev.min_large_swap_eth,
        config.native_asset_symbol(),
        config.mev.min_net_profit_eth,
        config.native_asset_symbol(),
        config.mev.min_roi_bps
    );
    info!(
        "Gas guardrails: max_gas_per_tx={} max_gas_price={} gwei",
        config.mev.max_gas_per_tx,
        config
            .mev
            .max_gas_price_gwei
            .map(|value| value.to_string())
            .unwrap_or_else(|| "disabled".to_string())
    );
    info!(
        "Latency trace: enabled={} warn_threshold_us={}",
        config.mev.latency_trace, config.mev.latency_trace_warn_us
    );
    info!(
        "Pool cache: state_ttl_ms={}",
        config.mev.pool_state_cache_ttl_ms
    );
    info!(
        "Executor buffer: min={:.4} {} target={:.4} {} max={:.4} {}",
        config.mev.executor_min_buffer_eth,
        config.native_asset_symbol(),
        config.mev.executor_target_buffer_eth,
        config.native_asset_symbol(),
        config.mev.executor_max_buffer_eth,
        config.native_asset_symbol()
    );
    info!(
        "Capital budget: window={}s total={:.4} {} cluster={:.4} {} pair={:.4} {}",
        config.mev.capital_window_secs,
        config.mev.max_window_exposure_eth,
        config.native_asset_symbol(),
        config.mev.max_cluster_window_exposure_eth,
        config.native_asset_symbol(),
        config.mev.max_pair_window_exposure_eth,
        config.native_asset_symbol()
    );

    if maybe_run_network_benchmark(config.clone(), rpc_fleet.clone(), &wallets).await? {
        return Ok(());
    }
    if maybe_run_replay_harness(config.clone(), storage.clone()).await? {
        return Ok(());
    }

    let dashboard = DashboardHandle::new(
        &config,
        wallets.len(),
        total_read,
        duplicate_keys,
        invalid_keys,
        storage.clone(),
        rpc_fleet.clone(),
    );
    let dashboard_server = dashboard.clone();
    let dashboard_addr = config.dashboard_addr;
    tokio::spawn(async move {
        if let Err(err) = dashboard::run_server(dashboard_server, dashboard_addr).await {
            error!("Dashboard server failed: {}", err);
        }
    });

    let dashboard_rankings = dashboard.clone();
    tokio::spawn(async move {
        loop {
            dashboard_rankings.flush_storage_buffers();
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    });

    let dashboard_wallets = dashboard.clone();
    let rpc_wallets = rpc_fleet.clone();
    let config_wallets = config.clone();
    tokio::spawn(async move {
        wallet_monitor_loop(config_wallets, rpc_wallets, dashboard_wallets).await;
    });

    if duplicate_keys > 0 {
        dashboard.event(
            "warn",
            format!(
                "ignored {} duplicate keys from {}",
                duplicate_keys,
                config.wallets.display()
            ),
        );
    }
    if invalid_keys > 0 {
        dashboard.event(
            "warn",
            format!(
                "ignored {} invalid keys from {}",
                invalid_keys,
                config.wallets.display()
            ),
        );
    }
    if config.vault_address == config.executor_address {
        dashboard.event(
            "warn",
            "vault wallet matches executor wallet; production segregation is compromised",
        );
    }
    if config.profit_address == config.executor_address {
        dashboard.event(
            "warn",
            "profit wallet matches executor wallet; hot-wallet profit isolation is compromised",
        );
    }
    if config.vault_address == config.profit_address {
        dashboard.event(
            "warn",
            "vault wallet matches profit wallet; treasury separation is reduced",
        );
    }

    if !config.mev.enabled {
        let message = "fee extraction engine is disabled: set MEV_ENGINE_ENABLED=true".to_string();
        dashboard.event("warn", message.clone());
        error!("{}", message);
        std::future::pending::<()>().await;
        return Ok(());
    }

    if let Err(err) = mev::run(config, rpc_fleet, dashboard, storage).await {
        error!("Fee extraction engine failed: {}", err);
    }

    std::future::pending::<()>().await;
    Ok(())
}

async fn wallet_monitor_loop(
    config: Arc<Config>,
    rpc_fleet: Arc<RpcFleet>,
    dashboard: DashboardHandle,
) {
    let mut failure_streak = 0u32;
    let mut next_sleep = std::time::Duration::from_secs(5);

    loop {
        let Some(endpoint) = rpc_fleet.read_candidates(1).into_iter().next() else {
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            continue;
        };

        let block_started = Instant::now();
        match endpoint.provider.get_block_number().await {
            Ok(block) => {
                rpc_fleet.record_success(
                    endpoint.id,
                    block_started.elapsed(),
                    Some(block.as_u64()),
                );
            }
            Err(err) => {
                rpc_fleet.record_failure(endpoint.id, RpcFleet::classify_failure(&err.to_string()));
                failure_streak = failure_streak.saturating_add(1);
                if failure_streak <= 3 || failure_streak % 10 == 0 {
                    dashboard.event(
                        "warn",
                        format!(
                            "wallet monitor rpc probe failed endpoint={}: {}",
                            endpoint.name, err
                        ),
                    );
                }
                tokio::time::sleep(std::time::Duration::from_secs(4)).await;
                continue;
            }
        }

        let probe_started = Instant::now();
        let executor = config.executor_address;
        let vault = config.vault_address;
        let profit = config.profit_address;
        let control = config.control_address;
        let provider = endpoint.provider.clone();
        let (executor_balance, vault_balance, profit_balance, control_balance) = tokio::join!(
            provider.get_balance(executor, None),
            provider.get_balance(vault, None),
            provider.get_balance(profit, None),
            provider.get_balance(control, None),
        );

        let mut snapshots = Vec::new();
        if let Ok(balance) = executor_balance {
            let balance_eth = wei_to_eth_f64(balance);
            let status = executor_buffer_status(&config, balance_eth);
            next_sleep = if status == "underfunded" {
                std::time::Duration::from_secs(30)
            } else {
                std::time::Duration::from_secs(5)
            };
            dashboard.set_executor_balance(balance_eth, status);
            snapshots.push(WalletSnapshot {
                role: "executor".to_string(),
                address: format!("{executor:?}"),
                balance_eth: format!("{balance_eth:.6}"),
                rpc: endpoint.name.clone(),
                status: status.to_string(),
                note: "hot execution wallet".to_string(),
            });
        }
        if let Ok(balance) = profit_balance {
            let balance_eth = wei_to_eth_f64(balance);
            snapshots.push(WalletSnapshot {
                role: "profit".to_string(),
                address: format!("{profit:?}"),
                balance_eth: format!("{balance_eth:.6}"),
                rpc: endpoint.name.clone(),
                status: if balance_eth > 0.0 {
                    "harvesting"
                } else {
                    "idle"
                }
                .to_string(),
                note: "realized pnl destination".to_string(),
            });
        }
        if let Ok(balance) = vault_balance {
            let balance_eth = wei_to_eth_f64(balance);
            snapshots.push(WalletSnapshot {
                role: "vault".to_string(),
                address: format!("{vault:?}"),
                balance_eth: format!("{balance_eth:.6}"),
                rpc: endpoint.name.clone(),
                status: "reserve".to_string(),
                note: "cold treasury reserve".to_string(),
            });
        }
        if let Ok(balance) = control_balance {
            let balance_eth = wei_to_eth_f64(balance);
            snapshots.push(WalletSnapshot {
                role: "control".to_string(),
                address: format!("{control:?}"),
                balance_eth: format!("{balance_eth:.6}"),
                rpc: endpoint.name.clone(),
                status: "control".to_string(),
                note: "coordination / admin path".to_string(),
            });
        }

        if !snapshots.is_empty() {
            dashboard.set_hot_wallets(snapshots);
            dashboard.record_latency(
                "wallet_probe",
                probe_started.elapsed().as_millis(),
                None,
                Some(&format!("endpoint={}", endpoint.name)),
            );
        }
        failure_streak = 0;
        tokio::time::sleep(next_sleep).await;
    }
}

fn executor_buffer_status(config: &Config, balance_eth: f64) -> &'static str {
    if balance_eth < config.mev.executor_min_buffer_eth {
        "underfunded"
    } else if balance_eth > config.mev.executor_max_buffer_eth {
        "overfunded"
    } else if balance_eth < config.mev.executor_target_buffer_eth {
        "below_target"
    } else {
        "healthy"
    }
}

fn wei_to_eth_f64(value: U256) -> f64 {
    value.as_u128() as f64 / 1e18
}
