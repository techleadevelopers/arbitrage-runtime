pub mod adaptive;
pub mod amm;
pub mod cache;
pub mod decoder;
pub mod execution;
pub mod network;
pub mod opportunity;
pub mod pnl;
pub mod runtime;
pub mod simulation;

use crate::config::Config;
use crate::dashboard::DashboardHandle;
use crate::rpc::RpcFleet;
use crate::storage::Storage;
use std::sync::Arc;

pub async fn run(
    config: Arc<Config>,
    rpc_fleet: Arc<RpcFleet>,
    dashboard: DashboardHandle,
    storage: Storage,
) -> Result<(), Box<dyn std::error::Error>> {
    dashboard.event(
        "info",
        format!(
            "fee extraction engine started capital={:.6} ETH min_profit={:.6} ETH max_gas={} min_liquidity={:.3} ETH",
            config.mev.capital_eth,
            config.mev.min_net_profit_eth,
            config.mev.max_gas_per_tx,
            config.mev.min_liquidity_eth,
        ),
    );

    runtime::run(config, rpc_fleet, dashboard, storage).await
}
