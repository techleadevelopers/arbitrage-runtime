#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub realized_profit: f64,
    pub gas_used: u64,
    pub success: bool,
}

#[derive(Debug, Default)]
pub struct PnlTracker {
    pub daily_pnl_eth: f64,
    pub realized_profit_eth: f64,
    pub realized_loss_eth: f64,
    pub executions: u64,
    pub failures: u64,
}

impl PnlTracker {
    pub fn record(&mut self, result: &ExecutionResult) {
        self.executions = self.executions.saturating_add(1);
        self.daily_pnl_eth += result.realized_profit;
        if result.success {
            self.realized_profit_eth += result.realized_profit.max(0.0);
        } else {
            self.failures = self.failures.saturating_add(1);
            self.realized_loss_eth += result.realized_profit.min(0.0).abs();
        }
    }
}
