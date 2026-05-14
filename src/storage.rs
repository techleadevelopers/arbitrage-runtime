use crate::dashboard::{DashboardEvent, ExecutionOutcomeSnapshot, RelaySnapshot, TreasurySnapshot};
use crate::mev::adaptive::HistoricalOutcomeProfile;
use chrono::Utc;
use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Storage {
    conn: Arc<Mutex<Connection>>,
    network: String,
}

impl Storage {
    pub fn new(path: &Path, network: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                at TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sweeps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                at TEXT NOT NULL,
                wallet TEXT NOT NULL,
                rpc TEXT,
                status TEXT NOT NULL,
                detail TEXT
            );

            CREATE TABLE IF NOT EXISTS telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                at TEXT NOT NULL,
                stage TEXT NOT NULL,
                duration_ms INTEGER NOT NULL,
                wallet TEXT,
                note TEXT
            );

            CREATE TABLE IF NOT EXISTS wallet_residual_stats (
                wallet TEXT PRIMARY KEY,
                last_seen_at TEXT NOT NULL,
                asset_class TEXT NOT NULL,
                detections INTEGER NOT NULL DEFAULT 0,
                successful_sweeps INTEGER NOT NULL DEFAULT 0,
                small_positive_detections INTEGER NOT NULL DEFAULT 0,
                total_residual_wei TEXT NOT NULL DEFAULT '0',
                detected_profit_wei TEXT NOT NULL DEFAULT '0',
                realized_profit_wei TEXT NOT NULL DEFAULT '0'
            );

            CREATE TABLE IF NOT EXISTS relay_metrics (
                relay TEXT PRIMARY KEY,
                network TEXT NOT NULL DEFAULT 'unknown',
                last_seen_at TEXT NOT NULL,
                accepted INTEGER NOT NULL DEFAULT 0,
                submit_failed INTEGER NOT NULL DEFAULT 0,
                included_success INTEGER NOT NULL DEFAULT 0,
                included_revert INTEGER NOT NULL DEFAULT 0,
                not_included_timeout INTEGER NOT NULL DEFAULT 0,
                submit_latency_ms REAL NOT NULL DEFAULT 0,
                finalization_latency_ms REAL NOT NULL DEFAULT 0,
                score REAL NOT NULL DEFAULT 0,
                pressure REAL NOT NULL DEFAULT 0,
                accept_rate REAL NOT NULL DEFAULT 0,
                inclusion_rate REAL NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS treasury_rebalance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                network TEXT NOT NULL DEFAULT 'unknown',
                at TEXT NOT NULL,
                executor_address TEXT NOT NULL,
                vault_address TEXT NOT NULL,
                profit_address TEXT NOT NULL,
                balance_eth REAL NOT NULL,
                min_buffer_eth REAL NOT NULL,
                target_buffer_eth REAL NOT NULL,
                max_buffer_eth REAL NOT NULL,
                action TEXT NOT NULL,
                recommended_amount_eth REAL NOT NULL,
                status TEXT NOT NULL,
                note TEXT
            );

            CREATE TABLE IF NOT EXISTS execution_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                network TEXT NOT NULL DEFAULT 'unknown',
                at TEXT NOT NULL,
                relay TEXT NOT NULL,
                target_block INTEGER NOT NULL,
                pair TEXT NOT NULL,
                router TEXT NOT NULL,
                token_in TEXT NOT NULL,
                token_out TEXT NOT NULL,
                victim_tx TEXT NOT NULL,
                outcome TEXT NOT NULL,
                expected_profit_eth REAL NOT NULL,
                realized_profit_eth REAL NOT NULL,
                gas_used INTEGER NOT NULL DEFAULT 0,
                submit_latency_ms REAL NOT NULL DEFAULT 0,
                finalization_latency_ms REAL NOT NULL DEFAULT 0
            );
            "#,
        )?;
        let _ = conn.execute(
            "ALTER TABLE relay_metrics ADD COLUMN network TEXT NOT NULL DEFAULT 'unknown'",
            [],
        );
        let _ = conn.execute(
            "ALTER TABLE treasury_rebalance ADD COLUMN network TEXT NOT NULL DEFAULT 'unknown'",
            [],
        );
        let _ = conn.execute(
            "ALTER TABLE execution_outcomes ADD COLUMN network TEXT NOT NULL DEFAULT 'unknown'",
            [],
        );

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            network: network.to_string(),
        })
    }

    pub fn log_event(&self, level: &str, message: &str) {
        let now = Utc::now().to_rfc3339();
        if let Ok(conn) = self.conn.lock() {
            let _ = conn.execute(
                "INSERT INTO events (at, level, message) VALUES (?1, ?2, ?3)",
                params![now, level, message],
            );
        }
    }

    pub fn log_telemetry(
        &self,
        stage: &str,
        duration_ms: u128,
        wallet: Option<&str>,
        note: Option<&str>,
    ) {
        let now = Utc::now().to_rfc3339();
        if let Ok(conn) = self.conn.lock() {
            let _ = conn.execute(
                "INSERT INTO telemetry (at, stage, duration_ms, wallet, note) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![now, stage, duration_ms as i64, wallet, note],
            );
        }
    }

    pub fn recent_events(
        &self,
        limit: usize,
    ) -> Result<Vec<DashboardEvent>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().map_err(|_| "storage lock poisoned")?;
        let mut stmt =
            conn.prepare("SELECT at, level, message FROM events ORDER BY id DESC LIMIT ?1")?;
        let rows = stmt.query_map([limit as i64], |row| {
            Ok(DashboardEvent {
                at: row.get(0)?,
                level: row.get(1)?,
                message: row.get(2)?,
            })
        })?;

        let mut events = Vec::new();
        for row in rows {
            events.push(row?);
        }
        Ok(events)
    }

    pub fn sweep_counts(&self) -> Result<(u64, u64, u64), Box<dyn std::error::Error>> {
        let conn = self.conn.lock().map_err(|_| "storage lock poisoned")?;
        let attempted: u64 = conn.query_row("SELECT COUNT(*) FROM sweeps", [], |row| {
            row.get::<_, u64>(0)
        })?;
        let succeeded: u64 = conn.query_row(
            "SELECT COUNT(*) FROM sweeps WHERE status = 'success'",
            [],
            |row| row.get::<_, u64>(0),
        )?;
        let failed: u64 = conn.query_row(
            "SELECT COUNT(*) FROM sweeps WHERE status = 'failed'",
            [],
            |row| row.get::<_, u64>(0),
        )?;
        Ok((attempted, succeeded, failed))
    }

    pub fn telemetry_summary(
        &self,
    ) -> Result<HashMap<String, (u64, u128, u128, u128)>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().map_err(|_| "storage lock poisoned")?;
        let mut summary = HashMap::new();

        let mut stmt = conn.prepare(
            "SELECT stage, COUNT(*), AVG(duration_ms), MAX(duration_ms)
             FROM telemetry
             GROUP BY stage",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, u64>(1)?,
                row.get::<_, f64>(2)? as u128,
                row.get::<_, i64>(3)? as u128,
            ))
        })?;

        for row in rows {
            let (stage, samples, avg_ms, max_ms) = row?;
            summary.insert(stage, (samples, 0, avg_ms, max_ms));
        }

        let mut stmt = conn.prepare(
            "SELECT t.stage, t.duration_ms
             FROM telemetry t
             INNER JOIN (
                SELECT stage, MAX(id) AS last_id
                FROM telemetry
                GROUP BY stage
             ) latest ON latest.stage = t.stage AND latest.last_id = t.id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u128))
        })?;

        for row in rows {
            let (stage, last_ms) = row?;
            if let Some(entry) = summary.get_mut(&stage) {
                entry.1 = last_ms;
            }
        }

        Ok(summary)
    }

    pub fn top_wallet_residuals(
        &self,
        limit: usize,
    ) -> Result<Vec<(String, String, u64, u64, String, String, String)>, Box<dyn std::error::Error>>
    {
        let conn = self.conn.lock().map_err(|_| "storage lock poisoned")?;
        let mut stmt = conn.prepare(
            r#"
            SELECT wallet, asset_class, detections, successful_sweeps,
                   detected_profit_wei, realized_profit_wei, last_seen_at
            FROM wallet_residual_stats
            ORDER BY CAST(detected_profit_wei AS INTEGER) DESC, detections DESC
            LIMIT ?1
            "#,
        )?;
        let rows = stmt.query_map([limit as i64], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, u64>(2)?,
                row.get::<_, u64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
            ))
        })?;

        let mut stats = Vec::new();
        for row in rows {
            stats.push(row?);
        }
        Ok(stats)
    }

    pub fn record_relay_outcome(
        &self,
        relay: &str,
        accepted: u64,
        submit_failed: u64,
        included_success: u64,
        included_revert: u64,
        not_included_timeout: u64,
        submit_latency_ms: Option<f64>,
        finalization_latency_ms: Option<f64>,
        score: Option<f64>,
        pressure: Option<f64>,
        accept_rate: Option<f64>,
        inclusion_rate: Option<f64>,
    ) {
        let now = Utc::now().to_rfc3339();
        if let Ok(conn) = self.conn.lock() {
            let relay_key = self.scoped_relay(relay);
            let _ = conn.execute(
                r#"
                INSERT INTO relay_metrics (
                    relay, network, last_seen_at, accepted, submit_failed, included_success,
                    included_revert, not_included_timeout, submit_latency_ms,
                    finalization_latency_ms, score, pressure, accept_rate, inclusion_rate
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
                ON CONFLICT(relay) DO UPDATE SET
                    network = excluded.network,
                    last_seen_at = excluded.last_seen_at,
                    accepted = relay_metrics.accepted + excluded.accepted,
                    submit_failed = relay_metrics.submit_failed + excluded.submit_failed,
                    included_success = relay_metrics.included_success + excluded.included_success,
                    included_revert = relay_metrics.included_revert + excluded.included_revert,
                    not_included_timeout = relay_metrics.not_included_timeout + excluded.not_included_timeout,
                    submit_latency_ms = CASE
                        WHEN excluded.submit_latency_ms > 0 AND relay_metrics.submit_latency_ms > 0
                            THEN relay_metrics.submit_latency_ms * 0.8 + excluded.submit_latency_ms * 0.2
                        WHEN excluded.submit_latency_ms > 0 THEN excluded.submit_latency_ms
                        ELSE relay_metrics.submit_latency_ms
                    END,
                    finalization_latency_ms = CASE
                        WHEN excluded.finalization_latency_ms > 0 AND relay_metrics.finalization_latency_ms > 0
                            THEN relay_metrics.finalization_latency_ms * 0.8 + excluded.finalization_latency_ms * 0.2
                        WHEN excluded.finalization_latency_ms > 0 THEN excluded.finalization_latency_ms
                        ELSE relay_metrics.finalization_latency_ms
                    END,
                    score = CASE WHEN excluded.score > 0 THEN excluded.score ELSE relay_metrics.score END,
                    pressure = CASE WHEN excluded.pressure > 0 THEN excluded.pressure ELSE relay_metrics.pressure END,
                    accept_rate = CASE WHEN excluded.accept_rate > 0 THEN excluded.accept_rate ELSE relay_metrics.accept_rate END,
                    inclusion_rate = CASE WHEN excluded.inclusion_rate > 0 THEN excluded.inclusion_rate ELSE relay_metrics.inclusion_rate END
                "#,
                params![
                    relay_key,
                    self.network,
                    now,
                    accepted as i64,
                    submit_failed as i64,
                    included_success as i64,
                    included_revert as i64,
                    not_included_timeout as i64,
                    submit_latency_ms.unwrap_or(0.0),
                    finalization_latency_ms.unwrap_or(0.0),
                    score.unwrap_or(0.0),
                    pressure.unwrap_or(0.0),
                    accept_rate.unwrap_or(0.0),
                    inclusion_rate.unwrap_or(0.0),
                ],
            );
        }
    }

    pub fn relay_rankings(
        &self,
        limit: usize,
    ) -> Result<Vec<RelaySnapshot>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().map_err(|_| "storage lock poisoned")?;
        let mut stmt = conn.prepare(
            r#"
            SELECT relay, score, pressure, accept_rate, inclusion_rate,
                   accepted, submit_failed, included_success, included_revert,
                   not_included_timeout, submit_latency_ms, finalization_latency_ms
            FROM relay_metrics
            WHERE network = ?1
            ORDER BY score ASC, included_success DESC, accept_rate DESC
            LIMIT ?2
            "#,
        )?;
        let rows = stmt.query_map(params![self.network, limit as i64], |row| {
            Ok(RelaySnapshot {
                relay: self.unscoped_relay(&row.get::<_, String>(0)?),
                score: row.get(1)?,
                pressure: row.get(2)?,
                accept_rate: row.get(3)?,
                inclusion_rate: row.get(4)?,
                accepted: row.get(5)?,
                submit_failed: row.get(6)?,
                included_success: row.get(7)?,
                included_revert: row.get(8)?,
                not_included_timeout: row.get(9)?,
                submit_latency_ms: row.get(10)?,
                finalization_latency_ms: row.get(11)?,
            })
        })?;

        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    pub fn record_treasury_recommendation(
        &self,
        executor_address: &str,
        vault_address: &str,
        profit_address: &str,
        balance_eth: f64,
        min_buffer_eth: f64,
        target_buffer_eth: f64,
        max_buffer_eth: f64,
        action: &str,
        recommended_amount_eth: f64,
        status: &str,
        note: &str,
    ) {
        let now = Utc::now().to_rfc3339();
        if let Ok(conn) = self.conn.lock() {
            let _ = conn.execute(
                r#"
                INSERT INTO treasury_rebalance (
                    network, at, executor_address, vault_address, profit_address,
                    balance_eth, min_buffer_eth, target_buffer_eth, max_buffer_eth,
                    action, recommended_amount_eth, status, note
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                "#,
                params![
                    self.network,
                    now,
                    executor_address,
                    vault_address,
                    profit_address,
                    balance_eth,
                    min_buffer_eth,
                    target_buffer_eth,
                    max_buffer_eth,
                    action,
                    recommended_amount_eth,
                    status,
                    note,
                ],
            );
        }
    }

    pub fn treasury_rebalance_trail(
        &self,
        limit: usize,
    ) -> Result<Vec<TreasurySnapshot>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().map_err(|_| "storage lock poisoned")?;
        let mut stmt = conn.prepare(
            r#"
            SELECT at, executor_address, vault_address, profit_address,
                   balance_eth, min_buffer_eth, target_buffer_eth, max_buffer_eth,
                   action, recommended_amount_eth, status, note
            FROM treasury_rebalance
            WHERE network = ?1
            ORDER BY id DESC
            LIMIT ?2
            "#,
        )?;
        let rows = stmt.query_map(params![self.network, limit as i64], |row| {
            Ok(TreasurySnapshot {
                at: row.get(0)?,
                executor_address: row.get(1)?,
                vault_address: row.get(2)?,
                profit_address: row.get(3)?,
                balance_eth: row.get(4)?,
                min_buffer_eth: row.get(5)?,
                target_buffer_eth: row.get(6)?,
                max_buffer_eth: row.get(7)?,
                action: row.get(8)?,
                recommended_amount_eth: row.get(9)?,
                status: row.get(10)?,
                note: row.get(11)?,
            })
        })?;

        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_execution_outcome(
        &self,
        relay: &str,
        target_block: u64,
        pair: &str,
        router: &str,
        token_in: &str,
        token_out: &str,
        victim_tx: &str,
        outcome: &str,
        expected_profit_eth: f64,
        realized_profit_eth: f64,
        gas_used: u64,
        submit_latency_ms: f64,
        finalization_latency_ms: f64,
    ) {
        let now = Utc::now().to_rfc3339();
        if let Ok(conn) = self.conn.lock() {
            let _ = conn.execute(
                r#"
                INSERT INTO execution_outcomes (
                    network, at, relay, target_block, pair, router, token_in, token_out,
                    victim_tx, outcome, expected_profit_eth, realized_profit_eth,
                    gas_used, submit_latency_ms, finalization_latency_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
                "#,
                params![
                    self.network,
                    now,
                    relay,
                    target_block as i64,
                    pair,
                    router,
                    token_in,
                    token_out,
                    victim_tx,
                    outcome,
                    expected_profit_eth,
                    realized_profit_eth,
                    gas_used as i64,
                    submit_latency_ms,
                    finalization_latency_ms,
                ],
            );
        }
    }

    pub fn execution_outcomes(
        &self,
        limit: usize,
    ) -> Result<Vec<ExecutionOutcomeSnapshot>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().map_err(|_| "storage lock poisoned")?;
        let mut stmt = conn.prepare(
            r#"
            SELECT at, relay, target_block, pair, router, token_in, token_out, victim_tx,
                   outcome, expected_profit_eth, realized_profit_eth, gas_used,
                   submit_latency_ms, finalization_latency_ms
            FROM execution_outcomes
            WHERE network = ?1
            ORDER BY id DESC
            LIMIT ?2
            "#,
        )?;
        let rows = stmt.query_map(params![self.network, limit as i64], |row| {
            Ok(ExecutionOutcomeSnapshot {
                at: row.get(0)?,
                relay: row.get(1)?,
                target_block: row.get(2)?,
                pair: row.get(3)?,
                router: row.get(4)?,
                token_in: row.get(5)?,
                token_out: row.get(6)?,
                victim_tx: row.get(7)?,
                outcome: row.get(8)?,
                expected_profit_eth: row.get(9)?,
                realized_profit_eth: row.get(10)?,
                gas_used: row.get(11)?,
                submit_latency_ms: row.get(12)?,
                finalization_latency_ms: row.get(13)?,
            })
        })?;

        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    pub fn outcome_profiles(
        &self,
        min_samples: u64,
        limit: usize,
    ) -> Result<Vec<HistoricalOutcomeProfile>, Box<dyn std::error::Error>> {
        let conn = self.conn.lock().map_err(|_| "storage lock poisoned")?;
        let mut stmt = conn.prepare(
            r#"
            SELECT CAST(substr(at, 12, 2) AS INTEGER) AS hour_utc,
                   pair,
                   router,
                   COUNT(*) AS samples,
                   AVG(CASE WHEN outcome = 'included_success' THEN 1.0 ELSE 0.0 END) AS success_rate,
                   AVG(CASE WHEN outcome = 'accepted_not_included' THEN 1.0 ELSE 0.0 END) AS miss_rate,
                   AVG(CASE WHEN outcome = 'included_revert' THEN 1.0 ELSE 0.0 END) AS revert_rate,
                   AVG(
                       CASE
                           WHEN expected_profit_eth > 0 THEN
                               MIN(MAX(realized_profit_eth / expected_profit_eth, 0.0), 1.25)
                           ELSE 0.0
                       END
                   ) AS realized_capture
            FROM execution_outcomes
            WHERE network = ?1
            GROUP BY hour_utc, pair, router
            HAVING COUNT(*) >= ?2
            ORDER BY samples DESC
            LIMIT ?3
            "#,
        )?;
        let rows = stmt.query_map(params![self.network, min_samples as i64, limit as i64], |row| {
            let pair = row.get::<_, String>(1)?;
            let router = row.get::<_, String>(2)?;
            Ok(HistoricalOutcomeProfile {
                hour_utc: row.get::<_, i64>(0)? as u8,
                pair: pair.parse().unwrap_or_default(),
                router: router.parse().unwrap_or_default(),
                samples: row.get::<_, i64>(3)? as u64,
                success_rate: row.get(4)?,
                accepted_not_included_rate: row.get(5)?,
                revert_rate: row.get(6)?,
                realized_capture: row.get(7)?,
            })
        })?;

        let mut items = Vec::new();
        for row in rows {
            let profile = row?;
            if profile.pair != ethers::types::Address::zero()
                && profile.router != ethers::types::Address::zero()
            {
                items.push(profile);
            }
        }
        Ok(items)
    }

    fn scoped_relay(&self, relay: &str) -> String {
        format!("{}::{}", self.network, relay)
    }

    fn unscoped_relay(&self, relay: &str) -> String {
        relay.split_once("::")
            .map(|(_, value)| value.to_string())
            .unwrap_or_else(|| relay.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethers::types::Address;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(label: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("flash_bot_{label}_{nonce}.sqlite"))
    }

    #[test]
    fn relay_and_outcomes_are_network_scoped() {
        let path = temp_path("scoped");
        let bsc = Storage::new(&path, "bsc").unwrap();
        let polygon = Storage::new(&path, "polygon").unwrap();

        bsc.record_relay_outcome(
            "relay-a",
            1,
            0,
            1,
            0,
            0,
            Some(12.0),
            Some(500.0),
            Some(0.2),
            Some(0.1),
            Some(0.9),
            Some(0.8),
        );
        polygon.record_relay_outcome(
            "relay-b",
            1,
            0,
            0,
            1,
            0,
            Some(15.0),
            Some(600.0),
            Some(0.4),
            Some(0.3),
            Some(0.7),
            Some(0.4),
        );

        assert_eq!(bsc.relay_rankings(10).unwrap().len(), 1);
        assert_eq!(bsc.relay_rankings(10).unwrap()[0].relay, "relay-a");
        assert_eq!(polygon.relay_rankings(10).unwrap().len(), 1);
        assert_eq!(polygon.relay_rankings(10).unwrap()[0].relay, "relay-b");
    }

    #[test]
    fn outcome_profiles_aggregate_pair_router_hour() {
        let path = temp_path("profiles");
        let storage = Storage::new(&path, "bsc").unwrap();
        let pair = format!("{:?}", Address::from_low_u64_be(100));
        let router = format!("{:?}", Address::from_low_u64_be(200));
        let token_in = format!("{:?}", Address::from_low_u64_be(300));
        let token_out = format!("{:?}", Address::from_low_u64_be(400));

        for outcome in ["included_success", "accepted_not_included", "included_success"] {
            storage.record_execution_outcome(
                "relay-a",
                123,
                &pair,
                &router,
                &token_in,
                &token_out,
                "0xvictim",
                outcome,
                0.01,
                if outcome == "included_success" { 0.008 } else { 0.0 },
                210000,
                10.0,
                500.0,
            );
        }

        let profiles = storage.outcome_profiles(3, 10).unwrap();
        assert_eq!(profiles.len(), 1);
        let profile = &profiles[0];
        assert_eq!(profile.samples, 3);
        assert!(profile.success_rate > 0.60 && profile.success_rate < 0.70);
        assert!(profile.accepted_not_included_rate > 0.30 && profile.accepted_not_included_rate < 0.35);
    }
}
