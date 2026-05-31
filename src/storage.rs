use crate::dashboard::{
    DashboardEvent, ExecutionOutcomeSnapshot, RelaySnapshot, ToxicitySnapshot, TreasurySnapshot,
};
use crate::mev::adaptive::HistoricalOutcomeProfile;
use chrono::Utc;
use rusqlite::{params, Connection};
use serde::Serialize;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::warn;

const SELECTOR_POOL_PARTIAL_COLUMNS: [&str; 8] = [
    "partial_entered_payload_builder_count",
    "partial_pool_discovery_attempted_count",
    "partial_pool_found_count",
    "partial_pool_missing_count",
    "partial_shadow_ev_positive_count",
    "partial_shadow_ev_negative_count",
    "partial_replay_candidate_created_count",
    "partial_replay_candidate_rejected_count",
];

#[derive(Debug, Clone)]
pub struct UnsupportedSelectorRecord {
    pub target: String,
    pub selector: String,
    pub inner_selector: String,
    pub token_hints: String,
    pub input_bytes: u64,
    pub sample_tx: String,
    pub sample_calldata_prefix: String,
    pub route_hint: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SelectorPerformanceSnapshot {
    pub selector: String,
    pub target: String,
    pub decode_source: String,
    pub partial_signal: u64,
    pub payload_built: u64,
    pub payload_reject: u64,
    pub confidence_reject: u64,
    pub total: u64,
    pub avg_confidence: f64,
    pub avg_gas_gwei: f64,
    pub built_rate_pct: f64,
    pub reject_rate_pct: f64,
    pub classification: String,
    pub last_seen: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SelectorPoolPerformanceSnapshot {
    pub selector: String,
    pub target: String,
    pub token_pair: String,
    pub pool: String,
    pub dex_kind: String,
    pub fee_tier: u32,
    pub pool_found: u64,
    pub pool_missing: u64,
    pub payload_built: u64,
    pub shadow_ev_positive: u64,
    pub shadow_ev_negative: u64,
    pub partial_entered_payload_builder: u64,
    pub partial_pool_discovery_attempted: u64,
    pub partial_pool_found: u64,
    pub partial_pool_missing: u64,
    pub partial_shadow_ev_positive: u64,
    pub partial_shadow_ev_negative: u64,
    pub partial_replay_candidate_created: u64,
    pub partial_replay_candidate_rejected: u64,
    pub total: u64,
    pub avg_expected_profit: f64,
    pub avg_liquidity: f64,
    pub avg_gas_gwei: f64,
    pub classification: String,
    pub last_seen: String,
}

#[derive(Debug, Clone)]
pub struct ReplayCandidateRecord {
    pub tx_hash: String,
    pub selector: String,
    pub target: String,
    pub pool: String,
    pub path: String,
    pub amount_in: String,
    pub amount_out_min: String,
    pub gas_gwei: f64,
    pub block_number: u64,
    pub expected_profit: f64,
    pub confidence: f64,
    pub decode_source: String,
    pub status: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SelectorReplayScoreSnapshot {
    pub selector: String,
    pub target: String,
    pub pool: String,
    pub replay_cases: u64,
    pub replay_success_count: u64,
    pub replay_revert_count: u64,
    pub avg_expected_profit: f64,
    pub avg_simulated_profit: f64,
    pub avg_gas_used: f64,
    pub success_rate_pct: f64,
    pub revert_rate_pct: f64,
    pub recommendation: String,
    pub last_seen: String,
}

#[derive(Clone)]
pub struct Storage {
    backend: StorageBackend,
    network: String,
}

#[derive(Clone)]
enum StorageBackend {
    Sqlite(Arc<Mutex<Connection>>),
    Postgres(PgPool),
}

impl Storage {
    pub async fn new(path: &Path, network: &str) -> Result<Self, Box<dyn std::error::Error>> {
        if let Ok(database_url) = env::var("DATABASE_URL") {
            let database_url = database_url.trim();
            if !database_url.is_empty() {
                match PgPoolOptions::new()
                    .max_connections(5)
                    .connect(database_url)
                    .await
                {
                    Ok(pool) => match Self::migrate_postgres(&pool).await {
                        Ok(()) => {
                            return Ok(Self {
                                backend: StorageBackend::Postgres(pool),
                                network: network.to_string(),
                            }
                            .with_runtime_prune());
                        }
                        Err(err) if postgres_storage_required() => return Err(err),
                        Err(err) => {
                            warn!(
                                "postgres storage migration failed, falling back to sqlite: {}",
                                err
                            );
                        }
                    },
                    Err(err) if postgres_storage_required() => return Err(err.into()),
                    Err(err) => {
                        warn!(
                            "postgres storage connection failed, falling back to sqlite: {}",
                            err
                        );
                    }
                }
            }
        }

        let conn = Connection::open(path)?;
        conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA busy_timeout = 5000;
            "#,
        )?;
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

            CREATE TABLE IF NOT EXISTS unsupported_selectors (
                network TEXT NOT NULL DEFAULT 'unknown',
                target TEXT NOT NULL,
                selector TEXT NOT NULL,
                inner_selector TEXT NOT NULL DEFAULT '',
                token_hints TEXT NOT NULL DEFAULT '',
                input_bytes INTEGER NOT NULL DEFAULT 0,
                count INTEGER NOT NULL DEFAULT 0,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                sample_tx TEXT NOT NULL DEFAULT '',
                sample_calldata_prefix TEXT NOT NULL DEFAULT '',
                route_hint TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (network, target, selector, inner_selector, token_hints)
            );

            CREATE TABLE IF NOT EXISTS latency_rollups (
                network TEXT NOT NULL DEFAULT 'unknown',
                bucket TEXT NOT NULL,
                stage TEXT NOT NULL,
                samples INTEGER NOT NULL DEFAULT 0,
                total_ms INTEGER NOT NULL DEFAULT 0,
                max_ms INTEGER NOT NULL DEFAULT 0,
                last_ms INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (network, bucket, stage)
            );

            CREATE TABLE IF NOT EXISTS funnel_rollups (
                network TEXT NOT NULL DEFAULT 'unknown',
                bucket TEXT NOT NULL,
                stage TEXT NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (network, bucket, stage)
            );

            CREATE TABLE IF NOT EXISTS reject_reason_rollups (
                network TEXT NOT NULL DEFAULT 'unknown',
                bucket TEXT NOT NULL,
                stage TEXT NOT NULL,
                reason TEXT NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (network, bucket, stage, reason)
            );

            CREATE TABLE IF NOT EXISTS selector_performance_rollups (
                network TEXT NOT NULL DEFAULT 'unknown',
                bucket TEXT NOT NULL,
                selector TEXT NOT NULL,
                target TEXT NOT NULL,
                decode_source TEXT NOT NULL DEFAULT 'unknown',
                stage TEXT NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                confidence_sum REAL NOT NULL DEFAULT 0,
                gas_gwei_sum REAL NOT NULL DEFAULT 0,
                last_seen TEXT NOT NULL,
                PRIMARY KEY (network, bucket, selector, target, decode_source, stage)
            );

            CREATE TABLE IF NOT EXISTS selector_pool_performance_rollups (
                network TEXT NOT NULL DEFAULT 'unknown',
                bucket TEXT NOT NULL,
                selector TEXT NOT NULL,
                target TEXT NOT NULL,
                token_pair TEXT NOT NULL,
                pool TEXT NOT NULL,
                dex_kind TEXT NOT NULL,
                fee_tier INTEGER NOT NULL DEFAULT 0,
                pool_found_count INTEGER NOT NULL DEFAULT 0,
                pool_missing_count INTEGER NOT NULL DEFAULT 0,
                payload_built_count INTEGER NOT NULL DEFAULT 0,
                shadow_ev_positive_count INTEGER NOT NULL DEFAULT 0,
                shadow_ev_negative_count INTEGER NOT NULL DEFAULT 0,
                partial_entered_payload_builder_count INTEGER NOT NULL DEFAULT 0,
                partial_pool_discovery_attempted_count INTEGER NOT NULL DEFAULT 0,
                partial_pool_found_count INTEGER NOT NULL DEFAULT 0,
                partial_pool_missing_count INTEGER NOT NULL DEFAULT 0,
                partial_shadow_ev_positive_count INTEGER NOT NULL DEFAULT 0,
                partial_shadow_ev_negative_count INTEGER NOT NULL DEFAULT 0,
                partial_replay_candidate_created_count INTEGER NOT NULL DEFAULT 0,
                partial_replay_candidate_rejected_count INTEGER NOT NULL DEFAULT 0,
                expected_profit_sum REAL NOT NULL DEFAULT 0,
                liquidity_sum REAL NOT NULL DEFAULT 0,
                gas_gwei_sum REAL NOT NULL DEFAULT 0,
                samples INTEGER NOT NULL DEFAULT 0,
                last_seen TEXT NOT NULL,
                PRIMARY KEY (network, bucket, selector, target, token_pair, pool, dex_kind, fee_tier)
            );

            CREATE TABLE IF NOT EXISTS replay_candidates (
                network TEXT NOT NULL DEFAULT 'unknown',
                tx_hash TEXT NOT NULL,
                selector TEXT NOT NULL,
                target TEXT NOT NULL,
                pool TEXT NOT NULL,
                path TEXT NOT NULL,
                amount_in TEXT NOT NULL,
                amount_out_min TEXT NOT NULL,
                gas_gwei REAL NOT NULL DEFAULT 0,
                block_number INTEGER NOT NULL DEFAULT 0,
                expected_profit REAL NOT NULL DEFAULT 0,
                confidence REAL NOT NULL DEFAULT 0,
                decode_source TEXT NOT NULL DEFAULT 'unknown',
                status TEXT NOT NULL,
                detail TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (network, tx_hash, selector, target, pool)
            );

            CREATE TABLE IF NOT EXISTS selector_replay_scores (
                network TEXT NOT NULL DEFAULT 'unknown',
                selector TEXT NOT NULL,
                target TEXT NOT NULL,
                pool TEXT NOT NULL,
                replay_cases INTEGER NOT NULL DEFAULT 0,
                replay_success_count INTEGER NOT NULL DEFAULT 0,
                replay_revert_count INTEGER NOT NULL DEFAULT 0,
                expected_profit_sum REAL NOT NULL DEFAULT 0,
                simulated_profit_sum REAL NOT NULL DEFAULT 0,
                gas_used_sum REAL NOT NULL DEFAULT 0,
                last_seen TEXT NOT NULL,
                PRIMARY KEY (network, selector, target, pool)
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
        for column in SELECTOR_POOL_PARTIAL_COLUMNS {
            let _ = conn.execute(
                &format!(
                    "ALTER TABLE selector_pool_performance_rollups ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0"
                ),
                [],
            );
        }

        Ok(Self {
            backend: StorageBackend::Sqlite(Arc::new(Mutex::new(conn))),
            network: network.to_string(),
        }
        .with_runtime_prune())
    }

    fn with_runtime_prune(self) -> Self {
        if let Err(err) = self.prune_runtime_tables() {
            warn!("storage runtime prune skipped: {}", err);
        }
        self
    }

    pub fn backend_label(&self) -> &'static str {
        match &self.backend {
            StorageBackend::Sqlite(_) => "sqlite",
            StorageBackend::Postgres(_) => "postgres",
        }
    }

    pub fn database_table_counts(&self) -> Result<Vec<(String, u64)>, Box<dyn std::error::Error>> {
        const TABLES: [&str; 15] = [
            "events",
            "telemetry",
            "sweeps",
            "wallet_residual_stats",
            "relay_metrics",
            "treasury_rebalance",
            "execution_outcomes",
            "unsupported_selectors",
            "latency_rollups",
            "funnel_rollups",
            "reject_reason_rollups",
            "selector_performance_rollups",
            "selector_pool_performance_rollups",
            "replay_candidates",
            "selector_replay_scores",
        ];

        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                let conn = conn.lock().map_err(|_| "storage lock poisoned")?;
                let mut counts = Vec::with_capacity(TABLES.len());
                for table in TABLES {
                    let sql = format!("SELECT COUNT(*) FROM {table}");
                    let rows: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
                    counts.push((table.to_string(), rows.max(0) as u64));
                }
                Ok(counts)
            }
            StorageBackend::Postgres(pool) => {
                let mut counts = Vec::with_capacity(TABLES.len());
                for table in TABLES {
                    let sql = format!("SELECT COUNT(*)::bigint AS rows FROM {table}");
                    let row = Self::wait(sqlx::query(&sql).fetch_one(pool))?;
                    counts.push((table.to_string(), row.get::<i64, _>("rows").max(0) as u64));
                }
                Ok(counts)
            }
        }
    }

    async fn migrate_postgres(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
        let statements = [
            r#"
            CREATE TABLE IF NOT EXISTS events (
                id BIGSERIAL PRIMARY KEY,
                at TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS sweeps (
                id BIGSERIAL PRIMARY KEY,
                at TEXT NOT NULL,
                wallet TEXT NOT NULL,
                rpc TEXT,
                status TEXT NOT NULL,
                detail TEXT
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS telemetry (
                id BIGSERIAL PRIMARY KEY,
                at TEXT NOT NULL,
                stage TEXT NOT NULL,
                duration_ms BIGINT NOT NULL,
                wallet TEXT,
                note TEXT
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS wallet_residual_stats (
                wallet TEXT PRIMARY KEY,
                last_seen_at TEXT NOT NULL,
                asset_class TEXT NOT NULL,
                detections BIGINT NOT NULL DEFAULT 0,
                successful_sweeps BIGINT NOT NULL DEFAULT 0,
                small_positive_detections BIGINT NOT NULL DEFAULT 0,
                total_residual_wei TEXT NOT NULL DEFAULT '0',
                detected_profit_wei TEXT NOT NULL DEFAULT '0',
                realized_profit_wei TEXT NOT NULL DEFAULT '0'
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS relay_metrics (
                relay TEXT PRIMARY KEY,
                network TEXT NOT NULL DEFAULT 'unknown',
                last_seen_at TEXT NOT NULL,
                accepted BIGINT NOT NULL DEFAULT 0,
                submit_failed BIGINT NOT NULL DEFAULT 0,
                included_success BIGINT NOT NULL DEFAULT 0,
                included_revert BIGINT NOT NULL DEFAULT 0,
                not_included_timeout BIGINT NOT NULL DEFAULT 0,
                submit_latency_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
                finalization_latency_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
                score DOUBLE PRECISION NOT NULL DEFAULT 0,
                pressure DOUBLE PRECISION NOT NULL DEFAULT 0,
                accept_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
                inclusion_rate DOUBLE PRECISION NOT NULL DEFAULT 0
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS treasury_rebalance (
                id BIGSERIAL PRIMARY KEY,
                network TEXT NOT NULL DEFAULT 'unknown',
                at TEXT NOT NULL,
                executor_address TEXT NOT NULL,
                vault_address TEXT NOT NULL,
                profit_address TEXT NOT NULL,
                balance_eth DOUBLE PRECISION NOT NULL,
                min_buffer_eth DOUBLE PRECISION NOT NULL,
                target_buffer_eth DOUBLE PRECISION NOT NULL,
                max_buffer_eth DOUBLE PRECISION NOT NULL,
                action TEXT NOT NULL,
                recommended_amount_eth DOUBLE PRECISION NOT NULL,
                status TEXT NOT NULL,
                note TEXT
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS execution_outcomes (
                id BIGSERIAL PRIMARY KEY,
                network TEXT NOT NULL DEFAULT 'unknown',
                at TEXT NOT NULL,
                relay TEXT NOT NULL,
                target_block BIGINT NOT NULL,
                pair TEXT NOT NULL,
                router TEXT NOT NULL,
                token_in TEXT NOT NULL,
                token_out TEXT NOT NULL,
                victim_tx TEXT NOT NULL,
                outcome TEXT NOT NULL,
                expected_profit_eth DOUBLE PRECISION NOT NULL,
                realized_profit_eth DOUBLE PRECISION NOT NULL,
                gas_used BIGINT NOT NULL DEFAULT 0,
                submit_latency_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
                finalization_latency_ms DOUBLE PRECISION NOT NULL DEFAULT 0
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS unsupported_selectors (
                network TEXT NOT NULL DEFAULT 'unknown',
                target TEXT NOT NULL,
                selector TEXT NOT NULL,
                inner_selector TEXT NOT NULL DEFAULT '',
                token_hints TEXT NOT NULL DEFAULT '',
                input_bytes BIGINT NOT NULL DEFAULT 0,
                count BIGINT NOT NULL DEFAULT 0,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                sample_tx TEXT NOT NULL DEFAULT '',
                sample_calldata_prefix TEXT NOT NULL DEFAULT '',
                route_hint TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (network, target, selector, inner_selector, token_hints)
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS latency_rollups (
                network TEXT NOT NULL DEFAULT 'unknown',
                bucket TEXT NOT NULL,
                stage TEXT NOT NULL,
                samples BIGINT NOT NULL DEFAULT 0,
                total_ms BIGINT NOT NULL DEFAULT 0,
                max_ms BIGINT NOT NULL DEFAULT 0,
                last_ms BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (network, bucket, stage)
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS funnel_rollups (
                network TEXT NOT NULL DEFAULT 'unknown',
                bucket TEXT NOT NULL,
                stage TEXT NOT NULL,
                count BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (network, bucket, stage)
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS reject_reason_rollups (
                network TEXT NOT NULL DEFAULT 'unknown',
                bucket TEXT NOT NULL,
                stage TEXT NOT NULL,
                reason TEXT NOT NULL,
                count BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (network, bucket, stage, reason)
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS selector_performance_rollups (
                network TEXT NOT NULL DEFAULT 'unknown',
                bucket TEXT NOT NULL,
                selector TEXT NOT NULL,
                target TEXT NOT NULL,
                decode_source TEXT NOT NULL DEFAULT 'unknown',
                stage TEXT NOT NULL,
                count BIGINT NOT NULL DEFAULT 0,
                confidence_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
                gas_gwei_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
                last_seen TEXT NOT NULL,
                PRIMARY KEY (network, bucket, selector, target, decode_source, stage)
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS selector_pool_performance_rollups (
                network TEXT NOT NULL DEFAULT 'unknown',
                bucket TEXT NOT NULL,
                selector TEXT NOT NULL,
                target TEXT NOT NULL,
                token_pair TEXT NOT NULL,
                pool TEXT NOT NULL,
                dex_kind TEXT NOT NULL,
                fee_tier BIGINT NOT NULL DEFAULT 0,
                pool_found_count BIGINT NOT NULL DEFAULT 0,
                pool_missing_count BIGINT NOT NULL DEFAULT 0,
                payload_built_count BIGINT NOT NULL DEFAULT 0,
                shadow_ev_positive_count BIGINT NOT NULL DEFAULT 0,
                shadow_ev_negative_count BIGINT NOT NULL DEFAULT 0,
                partial_entered_payload_builder_count BIGINT NOT NULL DEFAULT 0,
                partial_pool_discovery_attempted_count BIGINT NOT NULL DEFAULT 0,
                partial_pool_found_count BIGINT NOT NULL DEFAULT 0,
                partial_pool_missing_count BIGINT NOT NULL DEFAULT 0,
                partial_shadow_ev_positive_count BIGINT NOT NULL DEFAULT 0,
                partial_shadow_ev_negative_count BIGINT NOT NULL DEFAULT 0,
                partial_replay_candidate_created_count BIGINT NOT NULL DEFAULT 0,
                partial_replay_candidate_rejected_count BIGINT NOT NULL DEFAULT 0,
                expected_profit_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
                liquidity_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
                gas_gwei_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
                samples BIGINT NOT NULL DEFAULT 0,
                last_seen TEXT NOT NULL,
                PRIMARY KEY (network, bucket, selector, target, token_pair, pool, dex_kind, fee_tier)
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS replay_candidates (
                network TEXT NOT NULL DEFAULT 'unknown',
                tx_hash TEXT NOT NULL,
                selector TEXT NOT NULL,
                target TEXT NOT NULL,
                pool TEXT NOT NULL,
                path TEXT NOT NULL,
                amount_in TEXT NOT NULL,
                amount_out_min TEXT NOT NULL,
                gas_gwei DOUBLE PRECISION NOT NULL DEFAULT 0,
                block_number BIGINT NOT NULL DEFAULT 0,
                expected_profit DOUBLE PRECISION NOT NULL DEFAULT 0,
                confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                decode_source TEXT NOT NULL DEFAULT 'unknown',
                status TEXT NOT NULL,
                detail TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (network, tx_hash, selector, target, pool)
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS selector_replay_scores (
                network TEXT NOT NULL DEFAULT 'unknown',
                selector TEXT NOT NULL,
                target TEXT NOT NULL,
                pool TEXT NOT NULL,
                replay_cases BIGINT NOT NULL DEFAULT 0,
                replay_success_count BIGINT NOT NULL DEFAULT 0,
                replay_revert_count BIGINT NOT NULL DEFAULT 0,
                expected_profit_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
                simulated_profit_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
                gas_used_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
                last_seen TEXT NOT NULL,
                PRIMARY KEY (network, selector, target, pool)
            )
            "#,
        ];

        for statement in statements {
            sqlx::query(statement).execute(pool).await?;
        }
        for column in SELECTOR_POOL_PARTIAL_COLUMNS {
            let statement = format!(
                "ALTER TABLE selector_pool_performance_rollups ADD COLUMN IF NOT EXISTS {column} BIGINT NOT NULL DEFAULT 0"
            );
            sqlx::query(&statement).execute(pool).await?;
        }
        Ok(())
    }

    fn wait<F, T>(future: F) -> Result<T, Box<dyn std::error::Error>>
    where
        F: Future<Output = Result<T, sqlx::Error>> + Send,
        T: Send,
    {
        let handle = tokio::runtime::Handle::current();
        tokio::task::block_in_place(|| handle.block_on(future)).map_err(|err| err.into())
    }

    pub fn log_event(&self, level: &str, message: &str) {
        let now = Utc::now().to_rfc3339();
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                if let Ok(conn) = conn.lock() {
                    let _ = conn.execute(
                        "INSERT INTO events (at, level, message) VALUES (?1, ?2, ?3)",
                        params![now, level, message],
                    );
                }
            }
            StorageBackend::Postgres(pool) => {
                let _ = Self::wait(
                    sqlx::query("INSERT INTO events (at, level, message) VALUES ($1, $2, $3)")
                        .bind(now)
                        .bind(level.to_string())
                        .bind(message.to_string())
                        .execute(pool),
                );
            }
        }
    }

    pub fn clear_events(&self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                let conn = conn.lock().map_err(|_| "storage lock poisoned")?;
                conn.execute("DELETE FROM events", [])?;
                Ok(())
            }
            StorageBackend::Postgres(pool) => {
                Self::wait(sqlx::query("DELETE FROM events").execute(pool))?;
                Ok(())
            }
        }
    }

    pub fn prune_runtime_tables(&self) -> Result<(), Box<dyn std::error::Error>> {
        let events_cutoff = (Utc::now()
            - chrono::Duration::hours(runtime_retention_hours(
                "STORAGE_EVENTS_RETENTION_HOURS",
                24,
            )))
        .to_rfc3339();
        let telemetry_cutoff = (Utc::now()
            - chrono::Duration::hours(runtime_retention_hours(
                "STORAGE_TELEMETRY_RETENTION_HOURS",
                6,
            )))
        .to_rfc3339();
        let rollup_cutoff = (Utc::now()
            - chrono::Duration::days(runtime_retention_days("STORAGE_ROLLUP_RETENTION_DAYS", 14)))
        .to_rfc3339();
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                let conn = conn.lock().map_err(|_| "storage lock poisoned")?;
                conn.execute("DELETE FROM events WHERE at < ?1", [events_cutoff.as_str()])?;
                conn.execute(
                    "DELETE FROM telemetry WHERE at < ?1",
                    [telemetry_cutoff.as_str()],
                )?;
                conn.execute(
                    "DELETE FROM latency_rollups WHERE bucket < ?1",
                    [rollup_cutoff.as_str()],
                )?;
                conn.execute(
                    "DELETE FROM funnel_rollups WHERE bucket < ?1",
                    [rollup_cutoff.as_str()],
                )?;
                conn.execute(
                    "DELETE FROM reject_reason_rollups WHERE bucket < ?1",
                    [rollup_cutoff.as_str()],
                )?;
                conn.execute(
                    "DELETE FROM selector_performance_rollups WHERE bucket < ?1",
                    [rollup_cutoff.as_str()],
                )?;
                conn.execute(
                    "DELETE FROM selector_pool_performance_rollups WHERE bucket < ?1",
                    [rollup_cutoff.as_str()],
                )?;
                conn.execute(
                    "DELETE FROM replay_candidates WHERE created_at < ?1 AND status IN ('queued', 'deferred', 'replay_candidate')",
                    [rollup_cutoff.as_str()],
                )?;
                Ok(())
            }
            StorageBackend::Postgres(pool) => {
                Self::wait(
                    sqlx::query("DELETE FROM events WHERE at < $1")
                        .bind(events_cutoff)
                        .execute(pool),
                )?;
                Self::wait(
                    sqlx::query("DELETE FROM telemetry WHERE at < $1")
                        .bind(telemetry_cutoff)
                        .execute(pool),
                )?;
                Self::wait(
                    sqlx::query("DELETE FROM latency_rollups WHERE bucket < $1")
                        .bind(rollup_cutoff.clone())
                        .execute(pool),
                )?;
                Self::wait(
                    sqlx::query("DELETE FROM funnel_rollups WHERE bucket < $1")
                        .bind(rollup_cutoff.clone())
                        .execute(pool),
                )?;
                Self::wait(
                    sqlx::query("DELETE FROM reject_reason_rollups WHERE bucket < $1")
                        .bind(rollup_cutoff.clone())
                        .execute(pool),
                )?;
                Self::wait(
                    sqlx::query("DELETE FROM selector_performance_rollups WHERE bucket < $1")
                        .bind(rollup_cutoff.clone())
                        .execute(pool),
                )?;
                Self::wait(
                    sqlx::query("DELETE FROM selector_pool_performance_rollups WHERE bucket < $1")
                        .bind(rollup_cutoff.clone())
                        .execute(pool),
                )?;
                Self::wait(
                    sqlx::query(
                        "DELETE FROM replay_candidates WHERE created_at < $1 AND status IN ('queued', 'deferred', 'replay_candidate')",
                    )
                        .bind(rollup_cutoff)
                        .execute(pool),
                )?;
                Ok(())
            }
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
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                if let Ok(conn) = conn.lock() {
                    let _ = conn.execute(
                        "INSERT INTO telemetry (at, stage, duration_ms, wallet, note) VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![now, stage, duration_ms as i64, wallet, note],
                    );
                }
            }
            StorageBackend::Postgres(pool) => {
                let _ = Self::wait(
                    sqlx::query(
                        "INSERT INTO telemetry (at, stage, duration_ms, wallet, note) VALUES ($1, $2, $3, $4, $5)",
                    )
                    .bind(now)
                    .bind(stage.to_string())
                    .bind(duration_ms as i64)
                    .bind(wallet.map(str::to_string))
                    .bind(note.map(str::to_string))
                    .execute(pool),
                );
            }
        }
    }

    pub fn record_unsupported_selector(&self, record: &UnsupportedSelectorRecord) {
        let now = Utc::now().to_rfc3339();
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                if let Ok(conn) = conn.lock() {
                    let _ = conn.execute(
                        r#"
                        INSERT INTO unsupported_selectors (
                            network, target, selector, inner_selector, token_hints, input_bytes,
                            count, first_seen, last_seen, sample_tx, sample_calldata_prefix, route_hint
                        )
                        VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, ?7, ?8, ?9, ?10)
                        ON CONFLICT(network, target, selector, inner_selector, token_hints) DO UPDATE SET
                            count = count + 1,
                            input_bytes = excluded.input_bytes,
                            last_seen = excluded.last_seen,
                            sample_tx = excluded.sample_tx,
                            sample_calldata_prefix = excluded.sample_calldata_prefix,
                            route_hint = excluded.route_hint
                        "#,
                        params![
                            self.network.as_str(),
                            record.target.as_str(),
                            record.selector.as_str(),
                            record.inner_selector.as_str(),
                            record.token_hints.as_str(),
                            record.input_bytes as i64,
                            now,
                            record.sample_tx.as_str(),
                            record.sample_calldata_prefix.as_str(),
                            record.route_hint.as_str(),
                        ],
                    );
                }
            }
            StorageBackend::Postgres(pool) => {
                let _ = Self::wait(
                    sqlx::query(
                        r#"
                        INSERT INTO unsupported_selectors (
                            network, target, selector, inner_selector, token_hints, input_bytes,
                            count, first_seen, last_seen, sample_tx, sample_calldata_prefix, route_hint
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $7, $8, $9, $10)
                        ON CONFLICT(network, target, selector, inner_selector, token_hints) DO UPDATE SET
                            count = unsupported_selectors.count + 1,
                            input_bytes = EXCLUDED.input_bytes,
                            last_seen = EXCLUDED.last_seen,
                            sample_tx = EXCLUDED.sample_tx,
                            sample_calldata_prefix = EXCLUDED.sample_calldata_prefix,
                            route_hint = EXCLUDED.route_hint
                        "#,
                    )
                    .bind(self.network.clone())
                    .bind(record.target.clone())
                    .bind(record.selector.clone())
                    .bind(record.inner_selector.clone())
                    .bind(record.token_hints.clone())
                    .bind(record.input_bytes as i64)
                    .bind(now)
                    .bind(record.sample_tx.clone())
                    .bind(record.sample_calldata_prefix.clone())
                    .bind(record.route_hint.clone())
                    .execute(pool),
                );
            }
        }
    }

    pub fn record_latency_rollup(
        &self,
        bucket: &str,
        stage: &str,
        samples: u64,
        total_ms: u128,
        max_ms: u128,
        last_ms: u128,
    ) {
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                if let Ok(conn) = conn.lock() {
                    let _ = conn.execute(
                        r#"
                        INSERT INTO latency_rollups (network, bucket, stage, samples, total_ms, max_ms, last_ms)
                        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                        ON CONFLICT(network, bucket, stage) DO UPDATE SET
                            samples = samples + excluded.samples,
                            total_ms = total_ms + excluded.total_ms,
                            max_ms = MAX(max_ms, excluded.max_ms),
                            last_ms = excluded.last_ms
                        "#,
                        params![
                            self.network.as_str(),
                            bucket,
                            stage,
                            samples as i64,
                            total_ms as i64,
                            max_ms as i64,
                            last_ms as i64,
                        ],
                    );
                }
            }
            StorageBackend::Postgres(pool) => {
                let _ = Self::wait(
                    sqlx::query(
                        r#"
                        INSERT INTO latency_rollups (network, bucket, stage, samples, total_ms, max_ms, last_ms)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT(network, bucket, stage) DO UPDATE SET
                            samples = latency_rollups.samples + EXCLUDED.samples,
                            total_ms = latency_rollups.total_ms + EXCLUDED.total_ms,
                            max_ms = GREATEST(latency_rollups.max_ms, EXCLUDED.max_ms),
                            last_ms = EXCLUDED.last_ms
                        "#,
                    )
                    .bind(self.network.clone())
                    .bind(bucket.to_string())
                    .bind(stage.to_string())
                    .bind(samples as i64)
                    .bind(total_ms as i64)
                    .bind(max_ms as i64)
                    .bind(last_ms as i64)
                    .execute(pool),
                );
            }
        }
    }

    pub fn record_funnel_rollup(&self, bucket: &str, stage: &str, count: u64) {
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                if let Ok(conn) = conn.lock() {
                    let _ = conn.execute(
                        r#"
                        INSERT INTO funnel_rollups (network, bucket, stage, count)
                        VALUES (?1, ?2, ?3, ?4)
                        ON CONFLICT(network, bucket, stage) DO UPDATE SET
                            count = count + excluded.count
                        "#,
                        params![self.network.as_str(), bucket, stage, count as i64],
                    );
                }
            }
            StorageBackend::Postgres(pool) => {
                let _ = Self::wait(
                    sqlx::query(
                        r#"
                        INSERT INTO funnel_rollups (network, bucket, stage, count)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT(network, bucket, stage) DO UPDATE SET
                            count = funnel_rollups.count + EXCLUDED.count
                        "#,
                    )
                    .bind(self.network.clone())
                    .bind(bucket.to_string())
                    .bind(stage.to_string())
                    .bind(count as i64)
                    .execute(pool),
                );
            }
        }
    }

    pub fn record_reject_reason_rollup(&self, bucket: &str, stage: &str, reason: &str, count: u64) {
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                if let Ok(conn) = conn.lock() {
                    let _ = conn.execute(
                        r#"
                        INSERT INTO reject_reason_rollups (network, bucket, stage, reason, count)
                        VALUES (?1, ?2, ?3, ?4, ?5)
                        ON CONFLICT(network, bucket, stage, reason) DO UPDATE SET
                            count = count + excluded.count
                        "#,
                        params![self.network.as_str(), bucket, stage, reason, count as i64],
                    );
                }
            }
            StorageBackend::Postgres(pool) => {
                let _ = Self::wait(
                    sqlx::query(
                        r#"
                        INSERT INTO reject_reason_rollups (network, bucket, stage, reason, count)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT(network, bucket, stage, reason) DO UPDATE SET
                            count = reject_reason_rollups.count + EXCLUDED.count
                        "#,
                    )
                    .bind(self.network.clone())
                    .bind(bucket.to_string())
                    .bind(stage.to_string())
                    .bind(reason.to_string())
                    .bind(count as i64)
                    .execute(pool),
                );
            }
        }
    }

    pub fn record_selector_performance_rollup(
        &self,
        bucket: &str,
        selector: &str,
        target: &str,
        decode_source: &str,
        stage: &str,
        count: u64,
        confidence_sum: f64,
        gas_gwei_sum: f64,
    ) {
        let now = Utc::now().to_rfc3339();
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                if let Ok(conn) = conn.lock() {
                    let _ = conn.execute(
                        r#"
                        INSERT INTO selector_performance_rollups (
                            network, bucket, selector, target, decode_source, stage,
                            count, confidence_sum, gas_gwei_sum, last_seen
                        )
                        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                        ON CONFLICT(network, bucket, selector, target, decode_source, stage) DO UPDATE SET
                            count = count + excluded.count,
                            confidence_sum = confidence_sum + excluded.confidence_sum,
                            gas_gwei_sum = gas_gwei_sum + excluded.gas_gwei_sum,
                            last_seen = excluded.last_seen
                        "#,
                        params![
                            self.network.as_str(),
                            bucket,
                            selector,
                            target,
                            decode_source,
                            stage,
                            count as i64,
                            confidence_sum,
                            gas_gwei_sum,
                            now,
                        ],
                    );
                }
            }
            StorageBackend::Postgres(pool) => {
                let _ = Self::wait(
                    sqlx::query(
                        r#"
                        INSERT INTO selector_performance_rollups (
                            network, bucket, selector, target, decode_source, stage,
                            count, confidence_sum, gas_gwei_sum, last_seen
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                        ON CONFLICT(network, bucket, selector, target, decode_source, stage) DO UPDATE SET
                            count = selector_performance_rollups.count + EXCLUDED.count,
                            confidence_sum = selector_performance_rollups.confidence_sum + EXCLUDED.confidence_sum,
                            gas_gwei_sum = selector_performance_rollups.gas_gwei_sum + EXCLUDED.gas_gwei_sum,
                            last_seen = EXCLUDED.last_seen
                        "#,
                    )
                    .bind(self.network.clone())
                    .bind(bucket.to_string())
                    .bind(selector.to_string())
                    .bind(target.to_string())
                    .bind(decode_source.to_string())
                    .bind(stage.to_string())
                    .bind(count as i64)
                    .bind(confidence_sum)
                    .bind(gas_gwei_sum)
                    .bind(now)
                    .execute(pool),
                );
            }
        }
    }

    pub fn selector_performance_scores(
        &self,
        limit: usize,
    ) -> Result<Vec<SelectorPerformanceSnapshot>, Box<dyn std::error::Error>> {
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                let conn = conn.lock().map_err(|_| "storage lock poisoned")?;
                let mut stmt = conn.prepare(
                    r#"
                    SELECT
                        selector,
                        target,
                        decode_source,
                        SUM(CASE WHEN stage = 'partial_signal' THEN count ELSE 0 END) AS partial_signal,
                        SUM(CASE WHEN stage = 'payload_built' THEN count ELSE 0 END) AS payload_built,
                        SUM(CASE WHEN stage = 'shadow_payload_built' THEN count ELSE 0 END) AS shadow_payload_built,
                        SUM(CASE WHEN stage = 'payload_reject' THEN count ELSE 0 END) AS payload_reject,
                        SUM(CASE WHEN stage = 'confidence_reject' THEN count ELSE 0 END) AS confidence_reject,
                        SUM(count) AS total,
                        SUM(confidence_sum) AS confidence_sum,
                        SUM(gas_gwei_sum) AS gas_gwei_sum,
                        MAX(last_seen) AS last_seen
                    FROM selector_performance_rollups
                    GROUP BY selector, target, decode_source
                    ORDER BY payload_built DESC, shadow_payload_built DESC, confidence_sum / NULLIF(total, 0) DESC, payload_reject DESC
                    LIMIT ?1
                    "#,
                )?;
                let rows = stmt.query_map([limit as i64], |row| {
                    let partial_signal = row.get::<_, i64>(3)?.max(0) as u64;
                    let payload_built =
                        (row.get::<_, i64>(4)? + row.get::<_, i64>(5)?).max(0) as u64;
                    let payload_reject = row.get::<_, i64>(6)?.max(0) as u64;
                    let confidence_reject = row.get::<_, i64>(7)?.max(0) as u64;
                    let total = row.get::<_, i64>(8)?.max(0) as u64;
                    let confidence_sum = row.get::<_, f64>(9)?;
                    let gas_gwei_sum = row.get::<_, f64>(10)?;
                    Ok(build_selector_performance_snapshot(
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        partial_signal,
                        payload_built,
                        payload_reject,
                        confidence_reject,
                        total,
                        confidence_sum,
                        gas_gwei_sum,
                        row.get(11)?,
                    ))
                })?;

                let mut out = Vec::new();
                for row in rows {
                    out.push(row?);
                }
                Ok(out)
            }
            StorageBackend::Postgres(pool) => {
                let rows = Self::wait(
                    sqlx::query(
                        r#"
                        SELECT
                            selector,
                            target,
                            decode_source,
                            SUM(CASE WHEN stage = 'partial_signal' THEN count ELSE 0 END)::bigint AS partial_signal,
                            SUM(CASE WHEN stage = 'payload_built' THEN count ELSE 0 END)::bigint AS payload_built,
                            SUM(CASE WHEN stage = 'shadow_payload_built' THEN count ELSE 0 END)::bigint AS shadow_payload_built,
                            SUM(CASE WHEN stage = 'payload_reject' THEN count ELSE 0 END)::bigint AS payload_reject,
                            SUM(CASE WHEN stage = 'confidence_reject' THEN count ELSE 0 END)::bigint AS confidence_reject,
                            SUM(count)::bigint AS total,
                            SUM(confidence_sum)::double precision AS confidence_sum,
                            SUM(gas_gwei_sum)::double precision AS gas_gwei_sum,
                            MAX(last_seen) AS last_seen
                        FROM selector_performance_rollups
                        GROUP BY selector, target, decode_source
                        ORDER BY payload_built DESC, shadow_payload_built DESC, SUM(confidence_sum) / NULLIF(SUM(count), 0) DESC, payload_reject DESC
                        LIMIT $1
                        "#,
                    )
                    .bind(limit as i64)
                    .fetch_all(pool),
                )?;
                Ok(rows
                    .into_iter()
                    .map(|row| {
                        let partial_signal = row.get::<i64, _>("partial_signal").max(0) as u64;
                        let payload_built = (row.get::<i64, _>("payload_built")
                            + row.get::<i64, _>("shadow_payload_built"))
                        .max(0) as u64;
                        let payload_reject = row.get::<i64, _>("payload_reject").max(0) as u64;
                        let confidence_reject =
                            row.get::<i64, _>("confidence_reject").max(0) as u64;
                        let total = row.get::<i64, _>("total").max(0) as u64;
                        build_selector_performance_snapshot(
                            row.get("selector"),
                            row.get("target"),
                            row.get("decode_source"),
                            partial_signal,
                            payload_built,
                            payload_reject,
                            confidence_reject,
                            total,
                            row.try_get::<Option<f64>, _>("confidence_sum")
                                .unwrap_or(None)
                                .unwrap_or(0.0),
                            row.try_get::<Option<f64>, _>("gas_gwei_sum")
                                .unwrap_or(None)
                                .unwrap_or(0.0),
                            row.get("last_seen"),
                        )
                    })
                    .collect())
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_selector_pool_performance_rollup(
        &self,
        bucket: &str,
        selector: &str,
        target: &str,
        token_pair: &str,
        pool: &str,
        dex_kind: &str,
        fee_tier: u32,
        pool_found_count: u64,
        pool_missing_count: u64,
        payload_built_count: u64,
        shadow_ev_positive_count: u64,
        shadow_ev_negative_count: u64,
        partial_entered_payload_builder_count: u64,
        partial_pool_discovery_attempted_count: u64,
        partial_pool_found_count: u64,
        partial_pool_missing_count: u64,
        partial_shadow_ev_positive_count: u64,
        partial_shadow_ev_negative_count: u64,
        partial_replay_candidate_created_count: u64,
        partial_replay_candidate_rejected_count: u64,
        expected_profit_sum: f64,
        liquidity_sum: f64,
        gas_gwei_sum: f64,
        samples: u64,
    ) {
        let now = Utc::now().to_rfc3339();
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                if let Ok(conn) = conn.lock() {
                    let _ = conn.execute(
                        r#"
                        INSERT INTO selector_pool_performance_rollups (
                            network, bucket, selector, target, token_pair, pool, dex_kind, fee_tier,
                            pool_found_count, pool_missing_count, payload_built_count,
                            shadow_ev_positive_count, shadow_ev_negative_count,
                            partial_entered_payload_builder_count, partial_pool_discovery_attempted_count,
                            partial_pool_found_count, partial_pool_missing_count,
                            partial_shadow_ev_positive_count, partial_shadow_ev_negative_count,
                            partial_replay_candidate_created_count, partial_replay_candidate_rejected_count,
                            expected_profit_sum, liquidity_sum, gas_gwei_sum, samples, last_seen
                        )
                        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)
                        ON CONFLICT(network, bucket, selector, target, token_pair, pool, dex_kind, fee_tier) DO UPDATE SET
                            pool_found_count = pool_found_count + excluded.pool_found_count,
                            pool_missing_count = pool_missing_count + excluded.pool_missing_count,
                            payload_built_count = payload_built_count + excluded.payload_built_count,
                            shadow_ev_positive_count = shadow_ev_positive_count + excluded.shadow_ev_positive_count,
                            shadow_ev_negative_count = shadow_ev_negative_count + excluded.shadow_ev_negative_count,
                            partial_entered_payload_builder_count = partial_entered_payload_builder_count + excluded.partial_entered_payload_builder_count,
                            partial_pool_discovery_attempted_count = partial_pool_discovery_attempted_count + excluded.partial_pool_discovery_attempted_count,
                            partial_pool_found_count = partial_pool_found_count + excluded.partial_pool_found_count,
                            partial_pool_missing_count = partial_pool_missing_count + excluded.partial_pool_missing_count,
                            partial_shadow_ev_positive_count = partial_shadow_ev_positive_count + excluded.partial_shadow_ev_positive_count,
                            partial_shadow_ev_negative_count = partial_shadow_ev_negative_count + excluded.partial_shadow_ev_negative_count,
                            partial_replay_candidate_created_count = partial_replay_candidate_created_count + excluded.partial_replay_candidate_created_count,
                            partial_replay_candidate_rejected_count = partial_replay_candidate_rejected_count + excluded.partial_replay_candidate_rejected_count,
                            expected_profit_sum = expected_profit_sum + excluded.expected_profit_sum,
                            liquidity_sum = liquidity_sum + excluded.liquidity_sum,
                            gas_gwei_sum = gas_gwei_sum + excluded.gas_gwei_sum,
                            samples = samples + excluded.samples,
                            last_seen = excluded.last_seen
                        "#,
                        params![
                            self.network.as_str(),
                            bucket,
                            selector,
                            target,
                            token_pair,
                            pool,
                            dex_kind,
                            fee_tier as i64,
                            pool_found_count as i64,
                            pool_missing_count as i64,
                            payload_built_count as i64,
                            shadow_ev_positive_count as i64,
                            shadow_ev_negative_count as i64,
                            partial_entered_payload_builder_count as i64,
                            partial_pool_discovery_attempted_count as i64,
                            partial_pool_found_count as i64,
                            partial_pool_missing_count as i64,
                            partial_shadow_ev_positive_count as i64,
                            partial_shadow_ev_negative_count as i64,
                            partial_replay_candidate_created_count as i64,
                            partial_replay_candidate_rejected_count as i64,
                            expected_profit_sum,
                            liquidity_sum,
                            gas_gwei_sum,
                            samples as i64,
                            now,
                        ],
                    );
                }
            }
            StorageBackend::Postgres(pool_conn) => {
                let _ = Self::wait(
                    sqlx::query(
                        r#"
                        INSERT INTO selector_pool_performance_rollups (
                            network, bucket, selector, target, token_pair, pool, dex_kind, fee_tier,
                            pool_found_count, pool_missing_count, payload_built_count,
                            shadow_ev_positive_count, shadow_ev_negative_count,
                            partial_entered_payload_builder_count, partial_pool_discovery_attempted_count,
                            partial_pool_found_count, partial_pool_missing_count,
                            partial_shadow_ev_positive_count, partial_shadow_ev_negative_count,
                            partial_replay_candidate_created_count, partial_replay_candidate_rejected_count,
                            expected_profit_sum, liquidity_sum, gas_gwei_sum, samples, last_seen
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)
                        ON CONFLICT(network, bucket, selector, target, token_pair, pool, dex_kind, fee_tier) DO UPDATE SET
                            pool_found_count = selector_pool_performance_rollups.pool_found_count + EXCLUDED.pool_found_count,
                            pool_missing_count = selector_pool_performance_rollups.pool_missing_count + EXCLUDED.pool_missing_count,
                            payload_built_count = selector_pool_performance_rollups.payload_built_count + EXCLUDED.payload_built_count,
                            shadow_ev_positive_count = selector_pool_performance_rollups.shadow_ev_positive_count + EXCLUDED.shadow_ev_positive_count,
                            shadow_ev_negative_count = selector_pool_performance_rollups.shadow_ev_negative_count + EXCLUDED.shadow_ev_negative_count,
                            partial_entered_payload_builder_count = selector_pool_performance_rollups.partial_entered_payload_builder_count + EXCLUDED.partial_entered_payload_builder_count,
                            partial_pool_discovery_attempted_count = selector_pool_performance_rollups.partial_pool_discovery_attempted_count + EXCLUDED.partial_pool_discovery_attempted_count,
                            partial_pool_found_count = selector_pool_performance_rollups.partial_pool_found_count + EXCLUDED.partial_pool_found_count,
                            partial_pool_missing_count = selector_pool_performance_rollups.partial_pool_missing_count + EXCLUDED.partial_pool_missing_count,
                            partial_shadow_ev_positive_count = selector_pool_performance_rollups.partial_shadow_ev_positive_count + EXCLUDED.partial_shadow_ev_positive_count,
                            partial_shadow_ev_negative_count = selector_pool_performance_rollups.partial_shadow_ev_negative_count + EXCLUDED.partial_shadow_ev_negative_count,
                            partial_replay_candidate_created_count = selector_pool_performance_rollups.partial_replay_candidate_created_count + EXCLUDED.partial_replay_candidate_created_count,
                            partial_replay_candidate_rejected_count = selector_pool_performance_rollups.partial_replay_candidate_rejected_count + EXCLUDED.partial_replay_candidate_rejected_count,
                            expected_profit_sum = selector_pool_performance_rollups.expected_profit_sum + EXCLUDED.expected_profit_sum,
                            liquidity_sum = selector_pool_performance_rollups.liquidity_sum + EXCLUDED.liquidity_sum,
                            gas_gwei_sum = selector_pool_performance_rollups.gas_gwei_sum + EXCLUDED.gas_gwei_sum,
                            samples = selector_pool_performance_rollups.samples + EXCLUDED.samples,
                            last_seen = EXCLUDED.last_seen
                        "#,
                    )
                    .bind(self.network.clone())
                    .bind(bucket.to_string())
                    .bind(selector.to_string())
                    .bind(target.to_string())
                    .bind(token_pair.to_string())
                    .bind(pool.to_string())
                    .bind(dex_kind.to_string())
                    .bind(fee_tier as i64)
                    .bind(pool_found_count as i64)
                    .bind(pool_missing_count as i64)
                    .bind(payload_built_count as i64)
                    .bind(shadow_ev_positive_count as i64)
                    .bind(shadow_ev_negative_count as i64)
                    .bind(partial_entered_payload_builder_count as i64)
                    .bind(partial_pool_discovery_attempted_count as i64)
                    .bind(partial_pool_found_count as i64)
                    .bind(partial_pool_missing_count as i64)
                    .bind(partial_shadow_ev_positive_count as i64)
                    .bind(partial_shadow_ev_negative_count as i64)
                    .bind(partial_replay_candidate_created_count as i64)
                    .bind(partial_replay_candidate_rejected_count as i64)
                    .bind(expected_profit_sum)
                    .bind(liquidity_sum)
                    .bind(gas_gwei_sum)
                    .bind(samples as i64)
                    .bind(now)
                    .execute(pool_conn),
                );
            }
        }
    }

    pub fn selector_pool_performance_scores(
        &self,
        limit: usize,
    ) -> Result<Vec<SelectorPoolPerformanceSnapshot>, Box<dyn std::error::Error>> {
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                let conn = conn.lock().map_err(|_| "storage lock poisoned")?;
                let mut stmt = conn.prepare(
                    r#"
                    SELECT
                        selector, target, token_pair, pool, dex_kind, fee_tier,
                        SUM(pool_found_count) AS pool_found,
                        SUM(pool_missing_count) AS pool_missing,
                        SUM(payload_built_count) AS payload_built,
                        SUM(shadow_ev_positive_count) AS shadow_ev_positive,
                        SUM(shadow_ev_negative_count) AS shadow_ev_negative,
                        SUM(partial_entered_payload_builder_count) AS partial_entered_payload_builder,
                        SUM(partial_pool_discovery_attempted_count) AS partial_pool_discovery_attempted,
                        SUM(partial_pool_found_count) AS partial_pool_found,
                        SUM(partial_pool_missing_count) AS partial_pool_missing,
                        SUM(partial_shadow_ev_positive_count) AS partial_shadow_ev_positive,
                        SUM(partial_shadow_ev_negative_count) AS partial_shadow_ev_negative,
                        SUM(partial_replay_candidate_created_count) AS partial_replay_candidate_created,
                        SUM(partial_replay_candidate_rejected_count) AS partial_replay_candidate_rejected,
                        SUM(samples) AS total,
                        SUM(expected_profit_sum) AS expected_profit_sum,
                        SUM(liquidity_sum) AS liquidity_sum,
                        SUM(gas_gwei_sum) AS gas_gwei_sum,
                        MAX(last_seen) AS last_seen
                    FROM selector_pool_performance_rollups
                    GROUP BY selector, target, token_pair, pool, dex_kind, fee_tier
                    ORDER BY shadow_ev_positive DESC, payload_built DESC, pool_found DESC, pool_missing ASC
                    LIMIT ?1
                    "#,
                )?;
                let rows = stmt.query_map([limit as i64], |row| {
                    Ok(build_selector_pool_performance_snapshot(
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get::<_, i64>(5)?.max(0) as u32,
                        row.get::<_, i64>(6)?.max(0) as u64,
                        row.get::<_, i64>(7)?.max(0) as u64,
                        row.get::<_, i64>(8)?.max(0) as u64,
                        row.get::<_, i64>(9)?.max(0) as u64,
                        row.get::<_, i64>(10)?.max(0) as u64,
                        row.get::<_, i64>(11)?.max(0) as u64,
                        row.get::<_, i64>(12)?.max(0) as u64,
                        row.get::<_, i64>(13)?.max(0) as u64,
                        row.get::<_, i64>(14)?.max(0) as u64,
                        row.get::<_, i64>(15)?.max(0) as u64,
                        row.get::<_, i64>(16)?.max(0) as u64,
                        row.get::<_, i64>(17)?.max(0) as u64,
                        row.get::<_, i64>(18)?.max(0) as u64,
                        row.get::<_, i64>(19)?.max(0) as u64,
                        row.get::<_, f64>(20)?,
                        row.get::<_, f64>(21)?,
                        row.get::<_, f64>(22)?,
                        row.get(23)?,
                    ))
                })?;
                let mut out = Vec::new();
                for row in rows {
                    out.push(row?);
                }
                Ok(out)
            }
            StorageBackend::Postgres(pool) => {
                let rows = Self::wait(
                    sqlx::query(
                        r#"
                        SELECT
                            selector, target, token_pair, pool, dex_kind, fee_tier,
                            SUM(pool_found_count)::bigint AS pool_found,
                            SUM(pool_missing_count)::bigint AS pool_missing,
                            SUM(payload_built_count)::bigint AS payload_built,
                            SUM(shadow_ev_positive_count)::bigint AS shadow_ev_positive,
                            SUM(shadow_ev_negative_count)::bigint AS shadow_ev_negative,
                            SUM(partial_entered_payload_builder_count)::bigint AS partial_entered_payload_builder,
                            SUM(partial_pool_discovery_attempted_count)::bigint AS partial_pool_discovery_attempted,
                            SUM(partial_pool_found_count)::bigint AS partial_pool_found,
                            SUM(partial_pool_missing_count)::bigint AS partial_pool_missing,
                            SUM(partial_shadow_ev_positive_count)::bigint AS partial_shadow_ev_positive,
                            SUM(partial_shadow_ev_negative_count)::bigint AS partial_shadow_ev_negative,
                            SUM(partial_replay_candidate_created_count)::bigint AS partial_replay_candidate_created,
                            SUM(partial_replay_candidate_rejected_count)::bigint AS partial_replay_candidate_rejected,
                            SUM(samples)::bigint AS total,
                            SUM(expected_profit_sum)::double precision AS expected_profit_sum,
                            SUM(liquidity_sum)::double precision AS liquidity_sum,
                            SUM(gas_gwei_sum)::double precision AS gas_gwei_sum,
                            MAX(last_seen) AS last_seen
                        FROM selector_pool_performance_rollups
                        GROUP BY selector, target, token_pair, pool, dex_kind, fee_tier
                        ORDER BY shadow_ev_positive DESC, payload_built DESC, pool_found DESC, pool_missing ASC
                        LIMIT $1
                        "#,
                    )
                    .bind(limit as i64)
                    .fetch_all(pool),
                )?;
                Ok(rows
                    .into_iter()
                    .map(|row| {
                        build_selector_pool_performance_snapshot(
                            row.get("selector"),
                            row.get("target"),
                            row.get("token_pair"),
                            row.get("pool"),
                            row.get("dex_kind"),
                            row.get::<i64, _>("fee_tier").max(0) as u32,
                            row.get::<i64, _>("pool_found").max(0) as u64,
                            row.get::<i64, _>("pool_missing").max(0) as u64,
                            row.get::<i64, _>("payload_built").max(0) as u64,
                            row.get::<i64, _>("shadow_ev_positive").max(0) as u64,
                            row.get::<i64, _>("shadow_ev_negative").max(0) as u64,
                            row.get::<i64, _>("partial_entered_payload_builder").max(0) as u64,
                            row.get::<i64, _>("partial_pool_discovery_attempted").max(0) as u64,
                            row.get::<i64, _>("partial_pool_found").max(0) as u64,
                            row.get::<i64, _>("partial_pool_missing").max(0) as u64,
                            row.get::<i64, _>("partial_shadow_ev_positive").max(0) as u64,
                            row.get::<i64, _>("partial_shadow_ev_negative").max(0) as u64,
                            row.get::<i64, _>("partial_replay_candidate_created").max(0) as u64,
                            row.get::<i64, _>("partial_replay_candidate_rejected")
                                .max(0) as u64,
                            row.get::<i64, _>("total").max(0) as u64,
                            row.try_get::<Option<f64>, _>("expected_profit_sum")
                                .unwrap_or(None)
                                .unwrap_or(0.0),
                            row.try_get::<Option<f64>, _>("liquidity_sum")
                                .unwrap_or(None)
                                .unwrap_or(0.0),
                            row.try_get::<Option<f64>, _>("gas_gwei_sum")
                                .unwrap_or(None)
                                .unwrap_or(0.0),
                            row.get("last_seen"),
                        )
                    })
                    .collect())
            }
        }
    }

    pub fn record_replay_candidate(&self, record: &ReplayCandidateRecord) {
        let now = Utc::now().to_rfc3339();
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                if let Ok(conn) = conn.lock() {
                    let _ = conn.execute(
                        r#"
                        INSERT INTO replay_candidates (
                            network, tx_hash, selector, target, pool, path, amount_in, amount_out_min,
                            gas_gwei, block_number, expected_profit, confidence, decode_source,
                            status, detail, created_at, updated_at
                        )
                        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
                        ON CONFLICT(network, tx_hash, selector, target, pool) DO UPDATE SET
                            status = excluded.status,
                            detail = excluded.detail,
                            expected_profit = excluded.expected_profit,
                            confidence = excluded.confidence,
                            gas_gwei = excluded.gas_gwei,
                            updated_at = excluded.updated_at
                        "#,
                        params![
                            self.network.as_str(),
                            record.tx_hash.as_str(),
                            record.selector.as_str(),
                            record.target.as_str(),
                            record.pool.as_str(),
                            record.path.as_str(),
                            record.amount_in.as_str(),
                            record.amount_out_min.as_str(),
                            record.gas_gwei,
                            record.block_number as i64,
                            record.expected_profit,
                            record.confidence,
                            record.decode_source.as_str(),
                            record.status.as_str(),
                            record.detail.as_str(),
                            now,
                            now,
                        ],
                    );
                }
            }
            StorageBackend::Postgres(pool) => {
                let _ = Self::wait(
                    sqlx::query(
                        r#"
                        INSERT INTO replay_candidates (
                            network, tx_hash, selector, target, pool, path, amount_in, amount_out_min,
                            gas_gwei, block_number, expected_profit, confidence, decode_source,
                            status, detail, created_at, updated_at
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                        ON CONFLICT(network, tx_hash, selector, target, pool) DO UPDATE SET
                            status = EXCLUDED.status,
                            detail = EXCLUDED.detail,
                            expected_profit = EXCLUDED.expected_profit,
                            confidence = EXCLUDED.confidence,
                            gas_gwei = EXCLUDED.gas_gwei,
                            updated_at = EXCLUDED.updated_at
                        "#,
                    )
                    .bind(self.network.clone())
                    .bind(record.tx_hash.clone())
                    .bind(record.selector.clone())
                    .bind(record.target.clone())
                    .bind(record.pool.clone())
                    .bind(record.path.clone())
                    .bind(record.amount_in.clone())
                    .bind(record.amount_out_min.clone())
                    .bind(record.gas_gwei)
                    .bind(record.block_number as i64)
                    .bind(record.expected_profit)
                    .bind(record.confidence)
                    .bind(record.decode_source.clone())
                    .bind(record.status.clone())
                    .bind(record.detail.clone())
                    .bind(now.clone())
                    .bind(now)
                    .execute(pool),
                );
            }
        }
    }

    pub fn record_selector_replay_score(
        &self,
        selector: &str,
        target: &str,
        pool: &str,
        success: bool,
        reverted: bool,
        expected_profit: f64,
        simulated_profit: f64,
        gas_used: f64,
    ) {
        let now = Utc::now().to_rfc3339();
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                if let Ok(conn) = conn.lock() {
                    let _ = conn.execute(
                        r#"
                        INSERT INTO selector_replay_scores (
                            network, selector, target, pool, replay_cases, replay_success_count,
                            replay_revert_count, expected_profit_sum, simulated_profit_sum,
                            gas_used_sum, last_seen
                        )
                        VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6, ?7, ?8, ?9, ?10)
                        ON CONFLICT(network, selector, target, pool) DO UPDATE SET
                            replay_cases = replay_cases + 1,
                            replay_success_count = replay_success_count + excluded.replay_success_count,
                            replay_revert_count = replay_revert_count + excluded.replay_revert_count,
                            expected_profit_sum = expected_profit_sum + excluded.expected_profit_sum,
                            simulated_profit_sum = simulated_profit_sum + excluded.simulated_profit_sum,
                            gas_used_sum = gas_used_sum + excluded.gas_used_sum,
                            last_seen = excluded.last_seen
                        "#,
                        params![
                            self.network.as_str(),
                            selector,
                            target,
                            pool,
                            if success { 1i64 } else { 0i64 },
                            if reverted { 1i64 } else { 0i64 },
                            expected_profit,
                            simulated_profit,
                            gas_used,
                            now,
                        ],
                    );
                }
            }
            StorageBackend::Postgres(pool_conn) => {
                let _ = Self::wait(
                    sqlx::query(
                        r#"
                        INSERT INTO selector_replay_scores (
                            network, selector, target, pool, replay_cases, replay_success_count,
                            replay_revert_count, expected_profit_sum, simulated_profit_sum,
                            gas_used_sum, last_seen
                        )
                        VALUES ($1, $2, $3, $4, 1, $5, $6, $7, $8, $9, $10)
                        ON CONFLICT(network, selector, target, pool) DO UPDATE SET
                            replay_cases = selector_replay_scores.replay_cases + 1,
                            replay_success_count = selector_replay_scores.replay_success_count + EXCLUDED.replay_success_count,
                            replay_revert_count = selector_replay_scores.replay_revert_count + EXCLUDED.replay_revert_count,
                            expected_profit_sum = selector_replay_scores.expected_profit_sum + EXCLUDED.expected_profit_sum,
                            simulated_profit_sum = selector_replay_scores.simulated_profit_sum + EXCLUDED.simulated_profit_sum,
                            gas_used_sum = selector_replay_scores.gas_used_sum + EXCLUDED.gas_used_sum,
                            last_seen = EXCLUDED.last_seen
                        "#,
                    )
                    .bind(self.network.clone())
                    .bind(selector.to_string())
                    .bind(target.to_string())
                    .bind(pool.to_string())
                    .bind(if success { 1i64 } else { 0i64 })
                    .bind(if reverted { 1i64 } else { 0i64 })
                    .bind(expected_profit)
                    .bind(simulated_profit)
                    .bind(gas_used)
                    .bind(now)
                    .execute(pool_conn),
                );
            }
        }
    }

    pub fn selector_replay_scores(
        &self,
        limit: usize,
    ) -> Result<Vec<SelectorReplayScoreSnapshot>, Box<dyn std::error::Error>> {
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                let conn = conn.lock().map_err(|_| "storage lock poisoned")?;
                let mut stmt = conn.prepare(
                    r#"
                    SELECT selector, target, pool, replay_cases, replay_success_count,
                        replay_revert_count, expected_profit_sum, simulated_profit_sum,
                        gas_used_sum, last_seen
                    FROM selector_replay_scores
                    ORDER BY replay_success_count DESC, replay_cases DESC, expected_profit_sum DESC
                    LIMIT ?1
                    "#,
                )?;
                let rows = stmt.query_map([limit as i64], |row| {
                    Ok(build_selector_replay_score_snapshot(
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get::<_, i64>(3)?.max(0) as u64,
                        row.get::<_, i64>(4)?.max(0) as u64,
                        row.get::<_, i64>(5)?.max(0) as u64,
                        row.get::<_, f64>(6)?,
                        row.get::<_, f64>(7)?,
                        row.get::<_, f64>(8)?,
                        row.get(9)?,
                    ))
                })?;
                let mut out = Vec::new();
                for row in rows {
                    out.push(row?);
                }
                Ok(out)
            }
            StorageBackend::Postgres(pool) => {
                let rows = Self::wait(
                    sqlx::query(
                        r#"
                        SELECT selector, target, pool, replay_cases, replay_success_count,
                            replay_revert_count, expected_profit_sum, simulated_profit_sum,
                            gas_used_sum, last_seen
                        FROM selector_replay_scores
                        ORDER BY replay_success_count DESC, replay_cases DESC, expected_profit_sum DESC
                        LIMIT $1
                        "#,
                    )
                    .bind(limit as i64)
                    .fetch_all(pool),
                )?;
                Ok(rows
                    .into_iter()
                    .map(|row| {
                        build_selector_replay_score_snapshot(
                            row.get("selector"),
                            row.get("target"),
                            row.get("pool"),
                            row.get::<i64, _>("replay_cases").max(0) as u64,
                            row.get::<i64, _>("replay_success_count").max(0) as u64,
                            row.get::<i64, _>("replay_revert_count").max(0) as u64,
                            row.get::<f64, _>("expected_profit_sum"),
                            row.get::<f64, _>("simulated_profit_sum"),
                            row.get::<f64, _>("gas_used_sum"),
                            row.get("last_seen"),
                        )
                    })
                    .collect())
            }
        }
    }

    pub fn recent_events(
        &self,
        limit: usize,
    ) -> Result<Vec<DashboardEvent>, Box<dyn std::error::Error>> {
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                let conn = conn.lock().map_err(|_| "storage lock poisoned")?;
                let mut stmt = conn
                    .prepare("SELECT at, level, message FROM events ORDER BY id DESC LIMIT ?1")?;
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
            StorageBackend::Postgres(pool) => {
                let rows = Self::wait(
                    sqlx::query("SELECT at, level, message FROM events ORDER BY id DESC LIMIT $1")
                        .bind(limit as i64)
                        .fetch_all(pool),
                )?;
                Ok(rows
                    .into_iter()
                    .map(|row| DashboardEvent {
                        at: row.get("at"),
                        level: row.get("level"),
                        message: row.get("message"),
                    })
                    .collect())
            }
        }
    }

    pub fn sweep_counts(&self) -> Result<(u64, u64, u64), Box<dyn std::error::Error>> {
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                let conn = conn.lock().map_err(|_| "storage lock poisoned")?;
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
            StorageBackend::Postgres(pool) => {
                let row = Self::wait(
                    sqlx::query(
                        r#"
                        SELECT
                            COUNT(*) AS attempted,
                            COUNT(*) FILTER (WHERE status = 'success') AS succeeded,
                            COUNT(*) FILTER (WHERE status = 'failed') AS failed
                        FROM sweeps
                        "#,
                    )
                    .fetch_one(pool),
                )?;
                Ok((
                    row.get::<i64, _>("attempted") as u64,
                    row.get::<i64, _>("succeeded") as u64,
                    row.get::<i64, _>("failed") as u64,
                ))
            }
        }
    }

    pub fn telemetry_summary(
        &self,
    ) -> Result<HashMap<String, (u64, u128, u128, u128)>, Box<dyn std::error::Error>> {
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                let conn = conn.lock().map_err(|_| "storage lock poisoned")?;
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
            StorageBackend::Postgres(pool) => {
                let mut summary = HashMap::new();
                let rows = Self::wait(
                    sqlx::query(
                        "SELECT
                            stage,
                            COUNT(*)::bigint AS samples,
                            AVG(duration_ms)::double precision AS avg_ms,
                            MAX(duration_ms)::bigint AS max_ms
                         FROM telemetry
                         GROUP BY stage",
                    )
                    .fetch_all(pool),
                )?;
                for row in rows {
                    summary.insert(
                        row.get::<String, _>("stage"),
                        (
                            row.get::<i64, _>("samples") as u64,
                            0,
                            row.try_get::<Option<f64>, _>("avg_ms")?.unwrap_or(0.0) as u128,
                            row.get::<i64, _>("max_ms") as u128,
                        ),
                    );
                }

                let rows = Self::wait(
                    sqlx::query(
                        "SELECT DISTINCT ON (stage) stage, duration_ms
                         FROM telemetry
                         ORDER BY stage, id DESC",
                    )
                    .fetch_all(pool),
                )?;
                for row in rows {
                    let stage = row.get::<String, _>("stage");
                    if let Some(entry) = summary.get_mut(&stage) {
                        entry.1 = row.get::<i64, _>("duration_ms") as u128;
                    }
                }
                Ok(summary)
            }
        }
    }

    pub fn telemetry_window_summary(
        &self,
        window_secs: i64,
    ) -> Result<HashMap<String, (u64, u128, u128, u128)>, Box<dyn std::error::Error>> {
        let cutoff = (Utc::now() - chrono::Duration::seconds(window_secs.max(1))).to_rfc3339();
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                let conn = conn.lock().map_err(|_| "storage lock poisoned")?;
                let mut summary = HashMap::new();

                let mut stmt = conn.prepare(
                    "SELECT stage, COUNT(*), AVG(duration_ms), MAX(duration_ms)
                     FROM telemetry
                     WHERE at >= ?1
                     GROUP BY stage",
                )?;
                let rows = stmt.query_map([cutoff.as_str()], |row| {
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
                        WHERE at >= ?1
                        GROUP BY stage
                     ) latest ON latest.stage = t.stage AND latest.last_id = t.id",
                )?;
                let rows = stmt.query_map([cutoff.as_str()], |row| {
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
            StorageBackend::Postgres(pool) => {
                let mut summary = HashMap::new();
                let rows = Self::wait(
                    sqlx::query(
                        "SELECT
                            stage,
                            COUNT(*)::bigint AS samples,
                            AVG(duration_ms)::double precision AS avg_ms,
                            MAX(duration_ms)::bigint AS max_ms
                         FROM telemetry
                         WHERE at >= $1
                         GROUP BY stage",
                    )
                    .bind(cutoff.clone())
                    .fetch_all(pool),
                )?;
                for row in rows {
                    summary.insert(
                        row.get::<String, _>("stage"),
                        (
                            row.get::<i64, _>("samples") as u64,
                            0,
                            row.try_get::<Option<f64>, _>("avg_ms")?.unwrap_or(0.0) as u128,
                            row.get::<i64, _>("max_ms") as u128,
                        ),
                    );
                }

                let rows = Self::wait(
                    sqlx::query(
                        "SELECT DISTINCT ON (stage) stage, duration_ms
                         FROM telemetry
                         WHERE at >= $1
                         ORDER BY stage, id DESC",
                    )
                    .bind(cutoff)
                    .fetch_all(pool),
                )?;
                for row in rows {
                    let stage = row.get::<String, _>("stage");
                    if let Some(entry) = summary.get_mut(&stage) {
                        entry.1 = row.get::<i64, _>("duration_ms") as u128;
                    }
                }
                Ok(summary)
            }
        }
    }

    pub fn top_wallet_residuals(
        &self,
        limit: usize,
    ) -> Result<Vec<(String, String, u64, u64, String, String, String)>, Box<dyn std::error::Error>>
    {
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                let conn = conn.lock().map_err(|_| "storage lock poisoned")?;
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
            StorageBackend::Postgres(pool) => {
                let rows = Self::wait(
                    sqlx::query(
                        r#"
                        SELECT wallet, asset_class, detections, successful_sweeps,
                               detected_profit_wei, realized_profit_wei, last_seen_at
                        FROM wallet_residual_stats
                        ORDER BY detected_profit_wei::numeric DESC, detections DESC
                        LIMIT $1
                        "#,
                    )
                    .bind(limit as i64)
                    .fetch_all(pool),
                )?;
                Ok(rows
                    .into_iter()
                    .map(|row| {
                        (
                            row.get("wallet"),
                            row.get("asset_class"),
                            row.get::<i64, _>("detections") as u64,
                            row.get::<i64, _>("successful_sweeps") as u64,
                            row.get("detected_profit_wei"),
                            row.get("realized_profit_wei"),
                            row.get("last_seen_at"),
                        )
                    })
                    .collect())
            }
        }
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
        let relay_key = self.scoped_relay(relay);
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                if let Ok(conn) = conn.lock() {
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
            StorageBackend::Postgres(pool) => {
                let _ = Self::wait(
                    sqlx::query(
                        r#"
                        INSERT INTO relay_metrics (
                            relay, network, last_seen_at, accepted, submit_failed, included_success,
                            included_revert, not_included_timeout, submit_latency_ms,
                            finalization_latency_ms, score, pressure, accept_rate, inclusion_rate
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
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
                    )
                    .bind(relay_key)
                    .bind(self.network.clone())
                    .bind(now)
                    .bind(accepted as i64)
                    .bind(submit_failed as i64)
                    .bind(included_success as i64)
                    .bind(included_revert as i64)
                    .bind(not_included_timeout as i64)
                    .bind(submit_latency_ms.unwrap_or(0.0))
                    .bind(finalization_latency_ms.unwrap_or(0.0))
                    .bind(score.unwrap_or(0.0))
                    .bind(pressure.unwrap_or(0.0))
                    .bind(accept_rate.unwrap_or(0.0))
                    .bind(inclusion_rate.unwrap_or(0.0))
                    .execute(pool),
                );
            }
        }
    }

    pub fn relay_rankings(
        &self,
        limit: usize,
    ) -> Result<Vec<RelaySnapshot>, Box<dyn std::error::Error>> {
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                let conn = conn.lock().map_err(|_| "storage lock poisoned")?;
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
            StorageBackend::Postgres(pool) => {
                let rows = Self::wait(
                    sqlx::query(
                        r#"
                        SELECT relay, score, pressure, accept_rate, inclusion_rate,
                               accepted, submit_failed, included_success, included_revert,
                               not_included_timeout, submit_latency_ms, finalization_latency_ms
                        FROM relay_metrics
                        WHERE network = $1
                        ORDER BY score ASC, included_success DESC, accept_rate DESC
                        LIMIT $2
                        "#,
                    )
                    .bind(self.network.clone())
                    .bind(limit as i64)
                    .fetch_all(pool),
                )?;
                Ok(rows
                    .into_iter()
                    .map(|row| RelaySnapshot {
                        relay: self.unscoped_relay(&row.get::<String, _>("relay")),
                        score: row.get("score"),
                        pressure: row.get("pressure"),
                        accept_rate: row.get("accept_rate"),
                        inclusion_rate: row.get("inclusion_rate"),
                        accepted: row.get::<i64, _>("accepted") as u64,
                        submit_failed: row.get::<i64, _>("submit_failed") as u64,
                        included_success: row.get::<i64, _>("included_success") as u64,
                        included_revert: row.get::<i64, _>("included_revert") as u64,
                        not_included_timeout: row.get::<i64, _>("not_included_timeout") as u64,
                        submit_latency_ms: row.get("submit_latency_ms"),
                        finalization_latency_ms: row.get("finalization_latency_ms"),
                    })
                    .collect())
            }
        }
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
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                if let Ok(conn) = conn.lock() {
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
            StorageBackend::Postgres(pool) => {
                let _ = Self::wait(
                    sqlx::query(
                        r#"
                        INSERT INTO treasury_rebalance (
                            network, at, executor_address, vault_address, profit_address,
                            balance_eth, min_buffer_eth, target_buffer_eth, max_buffer_eth,
                            action, recommended_amount_eth, status, note
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                        "#,
                    )
                    .bind(self.network.clone())
                    .bind(now)
                    .bind(executor_address.to_string())
                    .bind(vault_address.to_string())
                    .bind(profit_address.to_string())
                    .bind(balance_eth)
                    .bind(min_buffer_eth)
                    .bind(target_buffer_eth)
                    .bind(max_buffer_eth)
                    .bind(action.to_string())
                    .bind(recommended_amount_eth)
                    .bind(status.to_string())
                    .bind(note.to_string())
                    .execute(pool),
                );
            }
        }
    }

    pub fn treasury_rebalance_trail(
        &self,
        limit: usize,
    ) -> Result<Vec<TreasurySnapshot>, Box<dyn std::error::Error>> {
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                let conn = conn.lock().map_err(|_| "storage lock poisoned")?;
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
            StorageBackend::Postgres(pool) => {
                let rows = Self::wait(
                    sqlx::query(
                        r#"
                        SELECT at, executor_address, vault_address, profit_address,
                               balance_eth, min_buffer_eth, target_buffer_eth, max_buffer_eth,
                               action, recommended_amount_eth, status, note
                        FROM treasury_rebalance
                        WHERE network = $1
                        ORDER BY id DESC
                        LIMIT $2
                        "#,
                    )
                    .bind(self.network.clone())
                    .bind(limit as i64)
                    .fetch_all(pool),
                )?;
                Ok(rows
                    .into_iter()
                    .map(|row| TreasurySnapshot {
                        at: row.get("at"),
                        executor_address: row.get("executor_address"),
                        vault_address: row.get("vault_address"),
                        profit_address: row.get("profit_address"),
                        balance_eth: row.get("balance_eth"),
                        min_buffer_eth: row.get("min_buffer_eth"),
                        target_buffer_eth: row.get("target_buffer_eth"),
                        max_buffer_eth: row.get("max_buffer_eth"),
                        action: row.get("action"),
                        recommended_amount_eth: row.get("recommended_amount_eth"),
                        status: row.get("status"),
                        note: row.get("note"),
                    })
                    .collect())
            }
        }
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
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                if let Ok(conn) = conn.lock() {
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
            StorageBackend::Postgres(pool) => {
                let _ = Self::wait(
                    sqlx::query(
                        r#"
                        INSERT INTO execution_outcomes (
                            network, at, relay, target_block, pair, router, token_in, token_out,
                            victim_tx, outcome, expected_profit_eth, realized_profit_eth,
                            gas_used, submit_latency_ms, finalization_latency_ms
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                        "#,
                    )
                    .bind(self.network.clone())
                    .bind(now)
                    .bind(relay.to_string())
                    .bind(target_block as i64)
                    .bind(pair.to_string())
                    .bind(router.to_string())
                    .bind(token_in.to_string())
                    .bind(token_out.to_string())
                    .bind(victim_tx.to_string())
                    .bind(outcome.to_string())
                    .bind(expected_profit_eth)
                    .bind(realized_profit_eth)
                    .bind(gas_used as i64)
                    .bind(submit_latency_ms)
                    .bind(finalization_latency_ms)
                    .execute(pool),
                );
            }
        }
    }

    pub fn execution_outcomes(
        &self,
        limit: usize,
    ) -> Result<Vec<ExecutionOutcomeSnapshot>, Box<dyn std::error::Error>> {
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                let conn = conn.lock().map_err(|_| "storage lock poisoned")?;
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
            StorageBackend::Postgres(pool) => {
                let rows = Self::wait(
                    sqlx::query(
                        r#"
                        SELECT at, relay, target_block, pair, router, token_in, token_out, victim_tx,
                               outcome, expected_profit_eth, realized_profit_eth, gas_used,
                               submit_latency_ms, finalization_latency_ms
                        FROM execution_outcomes
                        WHERE network = $1
                        ORDER BY id DESC
                        LIMIT $2
                        "#,
                    )
                    .bind(self.network.clone())
                    .bind(limit as i64)
                    .fetch_all(pool),
                )?;
                Ok(rows
                    .into_iter()
                    .map(|row| ExecutionOutcomeSnapshot {
                        at: row.get("at"),
                        relay: row.get("relay"),
                        target_block: row.get::<i64, _>("target_block") as u64,
                        pair: row.get("pair"),
                        router: row.get("router"),
                        token_in: row.get("token_in"),
                        token_out: row.get("token_out"),
                        victim_tx: row.get("victim_tx"),
                        outcome: row.get("outcome"),
                        expected_profit_eth: row.get("expected_profit_eth"),
                        realized_profit_eth: row.get("realized_profit_eth"),
                        gas_used: row.get::<i64, _>("gas_used") as u64,
                        submit_latency_ms: row.get("submit_latency_ms"),
                        finalization_latency_ms: row.get("finalization_latency_ms"),
                    })
                    .collect())
            }
        }
    }

    pub fn outcome_profiles(
        &self,
        min_samples: u64,
        limit: usize,
    ) -> Result<Vec<HistoricalOutcomeProfile>, Box<dyn std::error::Error>> {
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                let conn = conn.lock().map_err(|_| "storage lock poisoned")?;
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
                let rows = stmt.query_map(
                    params![self.network, min_samples as i64, limit as i64],
                    |row| {
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
                    },
                )?;

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
            StorageBackend::Postgres(pool) => {
                let rows = Self::wait(
                    sqlx::query(
                        r#"
                        SELECT CAST(substr(at, 12, 2) AS INTEGER) AS hour_utc,
                               pair,
                               router,
                               COUNT(*) AS samples,
                               AVG(CASE WHEN outcome = 'included_success' THEN 1.0 ELSE 0.0 END)::double precision AS success_rate,
                               AVG(CASE WHEN outcome = 'accepted_not_included' THEN 1.0 ELSE 0.0 END)::double precision AS miss_rate,
                               AVG(CASE WHEN outcome = 'included_revert' THEN 1.0 ELSE 0.0 END)::double precision AS revert_rate,
                               AVG(
                                   CASE
                                       WHEN expected_profit_eth > 0 THEN
                                           LEAST(GREATEST(realized_profit_eth / expected_profit_eth, 0.0), 1.25)
                                       ELSE 0.0
                                   END
                               )::double precision AS realized_capture
                        FROM execution_outcomes
                        WHERE network = $1
                        GROUP BY hour_utc, pair, router
                        HAVING COUNT(*) >= $2
                        ORDER BY samples DESC
                        LIMIT $3
                        "#,
                    )
                    .bind(self.network.clone())
                    .bind(min_samples as i64)
                    .bind(limit as i64)
                    .fetch_all(pool),
                )?;
                let mut items = Vec::new();
                for row in rows {
                    let profile = HistoricalOutcomeProfile {
                        hour_utc: row.get::<i32, _>("hour_utc") as u8,
                        pair: row.get::<String, _>("pair").parse().unwrap_or_default(),
                        router: row.get::<String, _>("router").parse().unwrap_or_default(),
                        samples: row.get::<i64, _>("samples") as u64,
                        success_rate: row.get("success_rate"),
                        accepted_not_included_rate: row.get("miss_rate"),
                        revert_rate: row.get("revert_rate"),
                        realized_capture: row.get("realized_capture"),
                    };
                    if profile.pair != ethers::types::Address::zero()
                        && profile.router != ethers::types::Address::zero()
                    {
                        items.push(profile);
                    }
                }
                Ok(items)
            }
        }
    }

    pub fn toxicity_profiles(
        &self,
        limit: usize,
    ) -> Result<Vec<ToxicitySnapshot>, Box<dyn std::error::Error>> {
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                let conn = conn.lock().map_err(|_| "storage lock poisoned")?;
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
                    HAVING COUNT(*) >= 1
                    ORDER BY
                        (
                            (1.0 - AVG(CASE WHEN outcome = 'included_success' THEN 1.0 ELSE 0.0 END)) * 0.30
                            + AVG(CASE WHEN outcome = 'accepted_not_included' THEN 1.0 ELSE 0.0 END) * 0.30
                            + AVG(CASE WHEN outcome = 'included_revert' THEN 1.0 ELSE 0.0 END) * 0.25
                            + (1.0 - AVG(
                               CASE
                                   WHEN expected_profit_eth > 0 THEN
                                       MIN(MAX(realized_profit_eth / expected_profit_eth, 0.0), 1.25)
                                   ELSE 0.0
                               END
                            )) * 0.15
                        ) DESC,
                        samples DESC
                    LIMIT ?2
                    "#,
                )?;
                let rows = stmt.query_map(params![self.network, limit as i64], |row| {
                    let success_rate = row.get::<_, f64>(4)?;
                    let miss_rate = row.get::<_, f64>(5)?;
                    let revert_rate = row.get::<_, f64>(6)?;
                    let realized_capture = row.get::<_, f64>(7)?;
                    let toxicity_score = ((1.0 - success_rate).clamp(0.0, 1.0) * 0.30
                        + miss_rate.clamp(0.0, 1.0) * 0.30
                        + revert_rate.clamp(0.0, 1.0) * 0.25
                        + (1.0 - realized_capture).clamp(0.0, 1.0) * 0.15)
                        .clamp(0.0, 1.0);
                    Ok(ToxicitySnapshot {
                        hour_utc: row.get::<_, i64>(0)? as u8,
                        pair: row.get(1)?,
                        router: row.get(2)?,
                        samples: row.get::<_, i64>(3)? as u64,
                        success_rate,
                        miss_rate,
                        revert_rate,
                        realized_capture,
                        toxicity_score,
                    })
                })?;

                let mut items = Vec::new();
                for row in rows {
                    items.push(row?);
                }
                Ok(items)
            }
            StorageBackend::Postgres(pool) => {
                let rows = Self::wait(
                    sqlx::query(
                        r#"
                        SELECT CAST(substr(at, 12, 2) AS INTEGER) AS hour_utc,
                               pair,
                               router,
                               COUNT(*) AS samples,
                               AVG(CASE WHEN outcome = 'included_success' THEN 1.0 ELSE 0.0 END)::double precision AS success_rate,
                               AVG(CASE WHEN outcome = 'accepted_not_included' THEN 1.0 ELSE 0.0 END)::double precision AS miss_rate,
                               AVG(CASE WHEN outcome = 'included_revert' THEN 1.0 ELSE 0.0 END)::double precision AS revert_rate,
                               AVG(
                                   CASE
                                       WHEN expected_profit_eth > 0 THEN
                                           LEAST(GREATEST(realized_profit_eth / expected_profit_eth, 0.0), 1.25)
                                       ELSE 0.0
                                   END
                               )::double precision AS realized_capture
                        FROM execution_outcomes
                        WHERE network = $1
                        GROUP BY hour_utc, pair, router
                        HAVING COUNT(*) >= 1
                        ORDER BY samples DESC
                        LIMIT $2
                        "#,
                    )
                    .bind(self.network.clone())
                    .bind(limit as i64)
                    .fetch_all(pool),
                )?;
                Ok(rows
                    .into_iter()
                    .map(|row| {
                        let success_rate = row.get::<f64, _>("success_rate");
                        let miss_rate = row.get::<f64, _>("miss_rate");
                        let revert_rate = row.get::<f64, _>("revert_rate");
                        let realized_capture = row.get::<f64, _>("realized_capture");
                        let toxicity_score = ((1.0 - success_rate).clamp(0.0, 1.0) * 0.30
                            + miss_rate.clamp(0.0, 1.0) * 0.30
                            + revert_rate.clamp(0.0, 1.0) * 0.25
                            + (1.0 - realized_capture).clamp(0.0, 1.0) * 0.15)
                            .clamp(0.0, 1.0);
                        ToxicitySnapshot {
                            hour_utc: row.get::<i32, _>("hour_utc") as u8,
                            pair: row.get("pair"),
                            router: row.get("router"),
                            samples: row.get::<i64, _>("samples") as u64,
                            success_rate,
                            miss_rate,
                            revert_rate,
                            realized_capture,
                            toxicity_score,
                        }
                    })
                    .collect())
            }
        }
    }

    pub fn export_evidence_artifacts(
        &self,
        limit: usize,
    ) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
        let exports_dir = ensure_exports_dir()?;
        let toxicity = self.write_toxicity_profiles_csv_to(&exports_dir, limit)?;
        let realized = self.write_realized_vs_expected_json_to(&exports_dir, limit)?;
        let mut outputs = vec![toxicity.clone(), realized.clone()];
        if let Some(versioned) = self.version_export_copy(&toxicity)? {
            if let Some(reference) = maybe_freeze_reference_artifact(&versioned)? {
                outputs.push(reference);
            }
            outputs.push(versioned);
        }
        if let Some(versioned) = self.version_export_copy(&realized)? {
            if let Some(reference) = maybe_freeze_reference_artifact(&versioned)? {
                outputs.push(reference);
            }
            outputs.push(versioned);
        }
        Ok(outputs)
    }

    #[allow(dead_code)]
    pub fn export_toxicity_profiles_csv(
        &self,
        limit: usize,
    ) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let exports_dir = ensure_exports_dir()?;
        self.write_toxicity_profiles_csv_to(&exports_dir, limit)
    }

    #[allow(dead_code)]
    pub fn export_realized_vs_expected_json(
        &self,
        limit: usize,
    ) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let exports_dir = ensure_exports_dir()?;
        self.write_realized_vs_expected_json_to(&exports_dir, limit)
    }

    fn write_toxicity_profiles_csv_to(
        &self,
        exports_dir: &Path,
        limit: usize,
    ) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let path = exports_dir.join("toxicity_profiles.csv");
        let profiles = self.toxicity_profiles(limit)?;
        let mut out = String::from("router,pair,hour,revert_rate,samples,success_rate,miss_rate,realized_capture,toxicity_score\n");
        for profile in profiles {
            out.push_str(&format!(
                "{},{},{},{:.6},{},{:.6},{:.6},{:.6},{:.6}\n",
                csv_field(&profile.router),
                csv_field(&profile.pair),
                profile.hour_utc,
                profile.revert_rate,
                profile.samples,
                profile.success_rate,
                profile.miss_rate,
                profile.realized_capture,
                profile.toxicity_score,
            ));
        }
        fs::write(&path, out)?;
        Ok(path)
    }

    fn write_realized_vs_expected_json_to(
        &self,
        exports_dir: &Path,
        limit: usize,
    ) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let path = exports_dir.join("realized_vs_expected.json");
        let rows: Vec<RealizedVsExpectedRow> = self
            .execution_outcomes(limit)?
            .into_iter()
            .map(RealizedVsExpectedRow::from_snapshot)
            .collect();
        fs::write(&path, serde_json::to_string_pretty(&rows)?)?;
        Ok(path)
    }

    fn scoped_relay(&self, relay: &str) -> String {
        format!("{}::{}", self.network, relay)
    }

    fn unscoped_relay(&self, relay: &str) -> String {
        relay
            .split_once("::")
            .map(|(_, value)| value.to_string())
            .unwrap_or_else(|| relay.to_string())
    }

    fn version_export_copy(
        &self,
        path: &Path,
    ) -> Result<Option<PathBuf>, Box<dyn std::error::Error>> {
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            return Ok(None);
        };
        let timestamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
        let versioned =
            path.with_file_name(format!("{}.{}.{}", self.network, timestamp, file_name));
        fs::copy(path, &versioned)?;
        Ok(Some(versioned))
    }
}

fn postgres_storage_required() -> bool {
    env::var("STORAGE_POSTGRES_REQUIRED")
        .unwrap_or_else(|_| "false".to_string())
        .trim()
        .eq_ignore_ascii_case("true")
}

fn runtime_retention_hours(env_name: &str, default_hours: i64) -> i64 {
    env::var(env_name)
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .unwrap_or(default_hours)
        .clamp(1, 24 * 30)
}

fn build_selector_performance_snapshot(
    selector: String,
    target: String,
    decode_source: String,
    partial_signal: u64,
    payload_built: u64,
    payload_reject: u64,
    confidence_reject: u64,
    total: u64,
    confidence_sum: f64,
    gas_gwei_sum: f64,
    last_seen: String,
) -> SelectorPerformanceSnapshot {
    let avg_confidence = if total == 0 {
        0.0
    } else {
        confidence_sum / total as f64
    };
    let avg_gas_gwei = if total == 0 {
        0.0
    } else {
        gas_gwei_sum / total as f64
    };
    let built_rate_pct = percent(payload_built, total);
    let reject_rate_pct = percent(payload_reject.saturating_add(confidence_reject), total);
    let classification = if payload_built > 0 && built_rate_pct >= 5.0 {
        "good"
    } else if confidence_reject > payload_built.saturating_add(payload_reject)
        && confidence_reject >= 3
    {
        "noisy"
    } else if avg_gas_gwei >= 500.0 && payload_built == 0 {
        "expensive"
    } else if partial_signal >= 3 && payload_built == 0 {
        "promising"
    } else {
        "watch"
    };

    SelectorPerformanceSnapshot {
        selector,
        target,
        decode_source,
        partial_signal,
        payload_built,
        payload_reject,
        confidence_reject,
        total,
        avg_confidence,
        avg_gas_gwei,
        built_rate_pct,
        reject_rate_pct,
        classification: classification.to_string(),
        last_seen,
    }
}

#[allow(clippy::too_many_arguments)]
fn build_selector_pool_performance_snapshot(
    selector: String,
    target: String,
    token_pair: String,
    pool: String,
    dex_kind: String,
    fee_tier: u32,
    pool_found: u64,
    pool_missing: u64,
    payload_built: u64,
    shadow_ev_positive: u64,
    shadow_ev_negative: u64,
    partial_entered_payload_builder: u64,
    partial_pool_discovery_attempted: u64,
    partial_pool_found: u64,
    partial_pool_missing: u64,
    partial_shadow_ev_positive: u64,
    partial_shadow_ev_negative: u64,
    partial_replay_candidate_created: u64,
    partial_replay_candidate_rejected: u64,
    total: u64,
    expected_profit_sum: f64,
    liquidity_sum: f64,
    gas_gwei_sum: f64,
    last_seen: String,
) -> SelectorPoolPerformanceSnapshot {
    let avg_expected_profit = if total == 0 {
        0.0
    } else {
        expected_profit_sum / total as f64
    };
    let avg_liquidity = if total == 0 {
        0.0
    } else {
        liquidity_sum / total as f64
    };
    let avg_gas_gwei = if total == 0 {
        0.0
    } else {
        gas_gwei_sum / total as f64
    };
    let classification = if shadow_ev_positive > 0 {
        "evolve_decoder"
    } else if payload_built > 0 {
        "pool_real_payload_ready"
    } else if pool_found > 0 && pool_missing == 0 {
        "pool_real_watch"
    } else if pool_missing > pool_found {
        "pool_missing_or_wrong_path"
    } else {
        "watch"
    };

    SelectorPoolPerformanceSnapshot {
        selector,
        target,
        token_pair,
        pool,
        dex_kind,
        fee_tier,
        pool_found,
        pool_missing,
        payload_built,
        shadow_ev_positive,
        shadow_ev_negative,
        partial_entered_payload_builder,
        partial_pool_discovery_attempted,
        partial_pool_found,
        partial_pool_missing,
        partial_shadow_ev_positive,
        partial_shadow_ev_negative,
        partial_replay_candidate_created,
        partial_replay_candidate_rejected,
        total,
        avg_expected_profit,
        avg_liquidity,
        avg_gas_gwei,
        classification: classification.to_string(),
        last_seen,
    }
}

fn build_selector_replay_score_snapshot(
    selector: String,
    target: String,
    pool: String,
    replay_cases: u64,
    replay_success_count: u64,
    replay_revert_count: u64,
    expected_profit_sum: f64,
    simulated_profit_sum: f64,
    gas_used_sum: f64,
    last_seen: String,
) -> SelectorReplayScoreSnapshot {
    let avg_expected_profit = if replay_cases == 0 {
        0.0
    } else {
        expected_profit_sum / replay_cases as f64
    };
    let avg_simulated_profit = if replay_cases == 0 {
        0.0
    } else {
        simulated_profit_sum / replay_cases as f64
    };
    let avg_gas_used = if replay_cases == 0 {
        0.0
    } else {
        gas_used_sum / replay_cases as f64
    };
    let success_rate_pct = percent(replay_success_count, replay_cases);
    let revert_rate_pct = percent(replay_revert_count, replay_cases);
    let recommendation = if replay_cases >= 3 && success_rate_pct >= 70.0 && revert_rate_pct <= 20.0
    {
        "canary_ready"
    } else if replay_cases >= 3 && revert_rate_pct > 20.0 {
        "reject_revert"
    } else if replay_success_count > 0 {
        "replay_more"
    } else {
        "watch"
    };

    SelectorReplayScoreSnapshot {
        selector,
        target,
        pool,
        replay_cases,
        replay_success_count,
        replay_revert_count,
        avg_expected_profit,
        avg_simulated_profit,
        avg_gas_used,
        success_rate_pct,
        revert_rate_pct,
        recommendation: recommendation.to_string(),
        last_seen,
    }
}

fn percent(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        (part as f64 / total as f64) * 100.0
    }
}

fn runtime_retention_days(env_name: &str, default_days: i64) -> i64 {
    env::var(env_name)
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .unwrap_or(default_days)
        .clamp(1, 365)
}

#[derive(Debug, Clone, Serialize)]
struct RealizedVsExpectedRow {
    at: String,
    relay: String,
    target_block: u64,
    pair: String,
    router: String,
    token_in: String,
    token_out: String,
    victim_tx: String,
    outcome: String,
    expected_profit_eth: f64,
    realized_profit_eth: f64,
    delta_profit_eth: f64,
    capture_ratio: f64,
    gas_used: u64,
    submit_latency_ms: f64,
    finalization_latency_ms: f64,
}

impl RealizedVsExpectedRow {
    fn from_snapshot(snapshot: ExecutionOutcomeSnapshot) -> Self {
        let capture_ratio = if snapshot.expected_profit_eth.abs() <= f64::EPSILON {
            0.0
        } else {
            snapshot.realized_profit_eth / snapshot.expected_profit_eth
        };
        Self {
            delta_profit_eth: snapshot.realized_profit_eth - snapshot.expected_profit_eth,
            capture_ratio,
            at: snapshot.at,
            relay: snapshot.relay,
            target_block: snapshot.target_block,
            pair: snapshot.pair,
            router: snapshot.router,
            token_in: snapshot.token_in,
            token_out: snapshot.token_out,
            victim_tx: snapshot.victim_tx,
            outcome: snapshot.outcome,
            expected_profit_eth: snapshot.expected_profit_eth,
            realized_profit_eth: snapshot.realized_profit_eth,
            gas_used: snapshot.gas_used,
            submit_latency_ms: snapshot.submit_latency_ms,
            finalization_latency_ms: snapshot.finalization_latency_ms,
        }
    }
}

pub fn ensure_exports_dir() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let dir = PathBuf::from("exports");
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

pub fn maybe_freeze_reference_artifact(
    path: &Path,
) -> Result<Option<PathBuf>, Box<dyn std::error::Error>> {
    if !env_flag("FREEZE_REFERENCE_ARTIFACTS") {
        return Ok(None);
    }
    let base_dir = env::var("REFERENCE_ARTIFACTS_DIR")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            ensure_exports_dir()
                .unwrap_or_else(|_| PathBuf::from("exports"))
                .join("reference")
        });
    fs::create_dir_all(&base_dir)?;
    let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
        return Ok(None);
    };
    let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ");
    let frozen = base_dir.join(format!(
        "{}.{}.{}",
        timestamp,
        std::env::consts::OS,
        file_name
    ));
    fs::copy(path, &frozen)?;
    Ok(Some(frozen))
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .unwrap_or_default()
        .trim()
        .eq_ignore_ascii_case("true")
}

fn csv_field(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethers::types::Address;
    use std::path::Path;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn temp_path(label: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("flash_bot_{label}_{nonce}.sqlite"))
    }

    fn test_storage(path: &Path, network: &str) -> Storage {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::remove_var("DATABASE_URL");
        }
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(Storage::new(path, network))
            .unwrap()
    }

    #[test]
    fn relay_and_outcomes_are_network_scoped() {
        let path = temp_path("scoped");
        let bsc = test_storage(&path, "bsc");
        let polygon = test_storage(&path, "polygon");

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
        let storage = test_storage(&path, "bsc");
        let pair = format!("{:?}", Address::from_low_u64_be(100));
        let router = format!("{:?}", Address::from_low_u64_be(200));
        let token_in = format!("{:?}", Address::from_low_u64_be(300));
        let token_out = format!("{:?}", Address::from_low_u64_be(400));

        for outcome in [
            "included_success",
            "accepted_not_included",
            "included_success",
        ] {
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
                if outcome == "included_success" {
                    0.008
                } else {
                    0.0
                },
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
        assert!(
            profile.accepted_not_included_rate > 0.30 && profile.accepted_not_included_rate < 0.35
        );
    }

    #[test]
    fn toxicity_profiles_rank_bad_contexts() {
        let path = temp_path("toxicity");
        let storage = test_storage(&path, "polygon");
        let pair = format!("{:?}", Address::from_low_u64_be(101));
        let router = format!("{:?}", Address::from_low_u64_be(201));
        let token_in = format!("{:?}", Address::from_low_u64_be(301));
        let token_out = format!("{:?}", Address::from_low_u64_be(401));

        for outcome in [
            "included_revert",
            "accepted_not_included",
            "included_success",
        ] {
            storage.record_execution_outcome(
                "rpc://polygon-a",
                456,
                &pair,
                &router,
                &token_in,
                &token_out,
                "0xvictim",
                outcome,
                0.01,
                if outcome == "included_success" {
                    0.002
                } else {
                    0.0
                },
                210000,
                14.0,
                700.0,
            );
        }

        let profiles = storage.toxicity_profiles(10).unwrap();
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].samples, 3);
        assert!(profiles[0].toxicity_score > 0.50);
        assert!(profiles[0].revert_rate > 0.30);
        assert!(profiles[0].miss_rate > 0.30);
    }

    #[test]
    fn evidence_exports_write_expected_files() {
        let path = temp_path("exports");
        let storage = test_storage(&path, "polygon");
        let pair = format!("{:?}", Address::from_low_u64_be(111));
        let router = format!("{:?}", Address::from_low_u64_be(211));
        let token_in = format!("{:?}", Address::from_low_u64_be(311));
        let token_out = format!("{:?}", Address::from_low_u64_be(411));

        storage.record_execution_outcome(
            "relay-a",
            777,
            &pair,
            &router,
            &token_in,
            &token_out,
            "0xvictim",
            "included_success",
            0.015,
            0.012,
            245000,
            11.0,
            510.0,
        );

        let export_dir = std::env::temp_dir().join(format!(
            "flash_bot_exports_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&export_dir).unwrap();

        let toxicity = storage
            .write_toxicity_profiles_csv_to(&export_dir, 16)
            .unwrap();
        let realized = storage
            .write_realized_vs_expected_json_to(&export_dir, 16)
            .unwrap();

        let toxicity_raw = fs::read_to_string(toxicity).unwrap();
        let realized_raw = fs::read_to_string(realized).unwrap();
        assert!(toxicity_raw.contains("router,pair,hour,revert_rate"));
        assert!(realized_raw.contains("\"delta_profit_eth\""));
        assert!(realized_raw.contains("\"capture_ratio\""));
    }

    #[test]
    fn reference_artifact_freeze_creates_copy() {
        let _guard = ENV_LOCK.lock().unwrap();
        let export_dir = std::env::temp_dir().join(format!(
            "flash_bot_reference_exports_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&export_dir).unwrap();
        let source = export_dir.join("artifact.json");
        fs::write(&source, "{\"ok\":true}").unwrap();

        unsafe {
            std::env::set_var("FREEZE_REFERENCE_ARTIFACTS", "true");
            std::env::set_var("REFERENCE_ARTIFACTS_DIR", export_dir.join("reference"));
        }

        let frozen = maybe_freeze_reference_artifact(&source)
            .unwrap()
            .expect("reference copy");
        assert!(frozen.exists());
        assert_eq!(fs::read_to_string(&frozen).unwrap(), "{\"ok\":true}");

        unsafe {
            std::env::remove_var("FREEZE_REFERENCE_ARTIFACTS");
            std::env::remove_var("REFERENCE_ARTIFACTS_DIR");
        }
        let _ = fs::remove_dir_all(&export_dir);
    }
}
