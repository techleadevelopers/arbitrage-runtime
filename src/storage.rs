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
        ];

        for statement in statements {
            sqlx::query(statement).execute(pool).await?;
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
        match &self.backend {
            StorageBackend::Sqlite(conn) => {
                let conn = conn.lock().map_err(|_| "storage lock poisoned")?;
                conn.execute("DELETE FROM events WHERE at < ?1", [events_cutoff.as_str()])?;
                conn.execute(
                    "DELETE FROM telemetry WHERE at < ?1",
                    [telemetry_cutoff.as_str()],
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
