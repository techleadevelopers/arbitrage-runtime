mod benchmark;
mod config;
mod dashboard;
mod mev;
mod replay;
mod rpc;
mod storage;
mod wallets;

use benchmark::maybe_run_network_benchmark;
use config::Config;
use dashboard::DashboardHandle;
use replay::maybe_run_replay_harness;
use rpc::RpcFleet;
use std::sync::Arc;
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
    let rpc_fleet = Arc::new(RpcFleet::from_config(&config)?);
    let storage = Storage::new(&config.storage_path, &config.network)?;
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
        "Impact gate: min_large_swap={:.3} ETH min_profit={:.6} ETH min_roi={}bps",
        config.mev.min_large_swap_eth,
        config.mev.min_net_profit_eth,
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
        "Executor buffer: min={:.4} ETH target={:.4} ETH max={:.4} ETH",
        config.mev.executor_min_buffer_eth,
        config.mev.executor_target_buffer_eth,
        config.mev.executor_max_buffer_eth
    );
    info!(
        "Capital budget: window={}s total={:.4} ETH cluster={:.4} ETH pair={:.4} ETH",
        config.mev.capital_window_secs,
        config.mev.max_window_exposure_eth,
        config.mev.max_cluster_window_exposure_eth,
        config.mev.max_pair_window_exposure_eth
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
        &rpc_fleet,
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
        return Err(message.into());
    }

    if let Err(err) = mev::run(config, rpc_fleet, dashboard, storage).await {
        error!("Fee extraction engine failed: {}", err);
    }

    Ok(())
}
