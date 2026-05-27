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
            "fee extraction engine started network={} capital={:.6} {} min_profit={:.6} {} max_gas={} min_liquidity={:.3} {}",
            config.network,
            config.mev.capital_eth,
            config.native_asset_symbol(),
            config.mev.min_net_profit_eth,
            config.native_asset_symbol(),
            config.mev.max_gas_per_tx,
            config.mev.min_liquidity_eth,
            config.native_asset_symbol(),
        ),
    );

    runtime::run(config, rpc_fleet, dashboard, storage).await
}
