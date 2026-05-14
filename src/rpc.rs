use crate::config::{Config, RpcPreference};
use ethers::providers::{Http, Provider};
use ethers::types::{Address, U256};
use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Value};
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcKind {
    Tenderly,
    Alchemy,
    Infura,
}

impl RpcKind {
    fn as_str(self) -> &'static str {
        match self {
            RpcKind::Tenderly => "tenderly",
            RpcKind::Alchemy => "alchemy",
            RpcKind::Infura => "infura",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcFailureKind {
    Timeout,
    RateLimited,
    Stale,
    Transport,
    Remote,
}

#[derive(Debug)]
struct RpcEndpointState {
    failures: u32,
    timeout_failures: u32,
    rate_limit_failures: u32,
    stale_failures: u32,
    cooldown_until: Option<Instant>,
    avg_latency: Option<Duration>,
    last_block: Option<u64>,
    last_block_at: Option<Instant>,
    recent_burst_reservations: VecDeque<BurstReservation>,
    last_selected_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy)]
struct BurstReservation {
    reserved_at: Instant,
    units: u32,
}

#[derive(Debug)]
struct ScoreCacheState {
    updated_at: Instant,
    read_scores: Vec<Option<f64>>,
    send_scores: Vec<Option<f64>>,
}

#[derive(Debug)]
pub struct RpcEndpoint {
    pub id: usize,
    pub name: String,
    pub url: String,
    pub kind: RpcKind,
    pub provider: Arc<Provider<Http>>,
    client: Client,
    state: Mutex<RpcEndpointState>,
}

#[derive(Clone, Debug)]
pub struct RpcHandle {
    pub id: usize,
    pub name: String,
    pub url: String,
    pub provider: Arc<Provider<Http>>,
    client: Client,
}

#[derive(Debug)]
pub struct RpcFleet {
    endpoints: Vec<Arc<RpcEndpoint>>,
    read_preference: RpcPreference,
    send_preference: RpcPreference,
    score_cache: Mutex<ScoreCacheState>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RpcEndpointSnapshot {
    pub id: usize,
    pub name: String,
    pub url: String,
    pub kind: String,
    pub failures: u32,
    pub timeout_failures: u32,
    pub rate_limit_failures: u32,
    pub stale_failures: u32,
    pub cooldown_remaining_secs: u64,
    pub avg_latency_ms: Option<u128>,
    pub last_block: Option<u64>,
    pub block_age_secs: Option<u64>,
    pub recent_burst_units: u32,
    pub burst_capacity_units: u32,
}

impl RpcFleet {
    pub fn from_config(config: &Config) -> Result<Self, Box<dyn std::error::Error>> {
        let mut endpoints = Vec::new();

        for (idx, (name, url)) in config.rpc_urls().into_iter().enumerate() {
            let kind = if name.starts_with("tenderly-") {
                RpcKind::Tenderly
            } else if name.starts_with("alchemy-") {
                RpcKind::Alchemy
            } else {
                RpcKind::Infura
            };

            let provider = Provider::<Http>::try_from(url.as_str())?;
            endpoints.push(Arc::new(RpcEndpoint {
                id: idx,
                name,
                url,
                kind,
                provider: Arc::new(provider),
                client: Client::new(),
                state: Mutex::new(RpcEndpointState {
                    failures: 0,
                    timeout_failures: 0,
                    rate_limit_failures: 0,
                    stale_failures: 0,
                    cooldown_until: None,
                    avg_latency: None,
                    last_block: None,
                    last_block_at: None,
                    recent_burst_reservations: VecDeque::new(),
                    last_selected_at: None,
                }),
            }));
        }

        if endpoints.is_empty() {
            return Err("no RPC endpoints configured".into());
        }

        Ok(Self {
            score_cache: Mutex::new(ScoreCacheState {
                updated_at: Instant::now() - Duration::from_secs(1),
                read_scores: vec![None; endpoints.len()],
                send_scores: vec![None; endpoints.len()],
            }),
            endpoints,
            read_preference: config.rpc_read_preference,
            send_preference: config.rpc_send_preference,
        })
    }

    pub fn read_candidates(&self, limit: usize) -> Vec<RpcHandle> {
        self.select_candidates(false, limit)
    }

    pub fn send_candidates(&self, limit: usize) -> Vec<RpcHandle> {
        self.select_candidates(true, limit)
    }

    pub fn reserve_read_selection(&self, endpoint_id: usize) {
        self.reserve_selection(endpoint_id, false);
    }

    pub fn reserve_send_selection(&self, endpoint_id: usize) {
        self.reserve_selection(endpoint_id, true);
    }

    pub fn record_success(
        &self,
        endpoint_id: usize,
        latency: Duration,
        observed_block: Option<u64>,
    ) {
        let now = Instant::now();
        let Some(endpoint) = self.endpoints.iter().find(|endpoint| endpoint.id == endpoint_id) else {
            return;
        };
        let mut state = endpoint.state.lock().expect("rpc endpoint state lock");
        prune_burst_reservations(&mut state, now);
        state.avg_latency = Some(match state.avg_latency {
            Some(previous) => Duration::from_secs_f64(
                previous.as_secs_f64() * 0.72 + latency.as_secs_f64() * 0.28,
            ),
            None => latency,
        });
        state.failures = state.failures.saturating_sub(1);
        state.timeout_failures = state.timeout_failures.saturating_sub(1);
        state.rate_limit_failures = state.rate_limit_failures.saturating_sub(1);
        state.stale_failures = state.stale_failures.saturating_sub(1);
        if let Some(block) = observed_block {
            state.last_block = Some(block);
            state.last_block_at = Some(now);
        }
        if matches!(state.cooldown_until, Some(until) if until <= now) {
            state.cooldown_until = None;
        }
        drop(state);
        self.invalidate_score_cache();
    }

    pub fn record_failure(&self, endpoint_id: usize, kind: RpcFailureKind) {
        let now = Instant::now();
        let Some(endpoint) = self.endpoints.iter().find(|endpoint| endpoint.id == endpoint_id) else {
            return;
        };
        let mut state = endpoint.state.lock().expect("rpc endpoint state lock");
        prune_burst_reservations(&mut state, now);
        state.failures = state.failures.saturating_add(1);
        let cooldown = match kind {
            RpcFailureKind::RateLimited => {
                state.rate_limit_failures = state.rate_limit_failures.saturating_add(1);
                Duration::from_secs((2u64.saturating_pow(state.rate_limit_failures.min(5))) * 2)
            }
            RpcFailureKind::Timeout => {
                state.timeout_failures = state.timeout_failures.saturating_add(1);
                Duration::from_millis(400u64.saturating_mul(2u64.saturating_pow(state.timeout_failures.min(5))))
            }
            RpcFailureKind::Stale => {
                state.stale_failures = state.stale_failures.saturating_add(1);
                Duration::from_millis(900)
            }
            RpcFailureKind::Transport => Duration::from_millis(700),
            RpcFailureKind::Remote => Duration::from_millis(500),
        };
        let until = now + cooldown.min(Duration::from_secs(90));
        state.cooldown_until = Some(match state.cooldown_until {
            Some(existing) if existing > until => existing,
            _ => until,
        });
        drop(state);
        self.invalidate_score_cache();
    }

    pub fn classify_failure(error: &str) -> RpcFailureKind {
        let lower = error.to_ascii_lowercase();
        if lower.contains("429")
            || lower.contains("rate limit")
            || lower.contains("too many requests")
            || lower.contains("throughput limit")
        {
            RpcFailureKind::RateLimited
        } else if lower.contains("timeout")
            || lower.contains("timed out")
            || lower.contains("deadline exceeded")
        {
            RpcFailureKind::Timeout
        } else if lower.contains("stale") {
            RpcFailureKind::Stale
        } else if lower.contains("connection")
            || lower.contains("socket")
            || lower.contains("dns")
            || lower.contains("econnreset")
            || lower.contains("broken pipe")
        {
            RpcFailureKind::Transport
        } else {
            RpcFailureKind::Remote
        }
    }

    pub fn endpoint_count(&self) -> usize {
        self.endpoints.len()
    }

    pub fn all_handles(&self) -> Vec<RpcHandle> {
        self.endpoints
            .iter()
            .map(|endpoint| self.to_handle(endpoint))
            .collect()
    }

    pub fn snapshot(&self) -> Vec<RpcEndpointSnapshot> {
        let now = Instant::now();
        self.endpoints
            .iter()
            .map(|endpoint| {
                let state = endpoint.state.lock().expect("rpc endpoint state lock");
                let cooldown_remaining_secs = state
                    .cooldown_until
                    .and_then(|until| until.checked_duration_since(now))
                    .map(|duration| duration.as_secs())
                    .unwrap_or(0);

                RpcEndpointSnapshot {
                    id: endpoint.id,
                    name: endpoint.name.clone(),
                    url: endpoint.url.clone(),
                    kind: endpoint.kind.as_str().to_string(),
                    failures: state.failures,
                    timeout_failures: state.timeout_failures,
                    rate_limit_failures: state.rate_limit_failures,
                    stale_failures: state.stale_failures,
                    cooldown_remaining_secs,
                    avg_latency_ms: state.avg_latency.map(|value| value.as_millis()),
                    last_block: state.last_block,
                    block_age_secs: state
                        .last_block_at
                        .map(|instant| instant.elapsed().as_secs()),
                    recent_burst_units: burst_load_units(&state, now),
                    burst_capacity_units: endpoint_burst_capacity_units(endpoint.kind, false),
                }
            })
            .collect()
    }

    fn select_candidates(&self, send_mode: bool, limit: usize) -> Vec<RpcHandle> {
        let now = Instant::now();
        let preference = if send_mode {
            self.send_preference
        } else {
            self.read_preference
        };
        let mut candidates: Vec<(Arc<RpcEndpoint>, f64)> = self
            .endpoints
            .iter()
            .filter(|endpoint| self.matches_preference(endpoint.kind, preference))
            .filter_map(|endpoint| self.endpoint_score(endpoint, now, send_mode, true))
            .collect();

        if candidates.is_empty() {
            candidates = self
                .endpoints
                .iter()
                .filter_map(|endpoint| self.endpoint_score(endpoint, now, send_mode, true))
                .collect();
        }

        candidates.sort_by(|left, right| left.1.partial_cmp(&right.1).unwrap_or(Ordering::Equal));
        let top_n = candidates.len().min(3);
        if top_n > 1 {
            let rotation = self.rotation.fetch_add(1, AtomicOrdering::Relaxed) % top_n;
            candidates[..top_n].rotate_left(rotation);
        }
        candidates
            .into_iter()
            .take(limit.max(1))
            .map(|(endpoint, _)| self.to_handle(&endpoint))
            .collect()
    }

    fn endpoint_score(
        &self,
        endpoint: &Arc<RpcEndpoint>,
        now: Instant,
        send_mode: bool,
        use_cache: bool,
    ) -> Option<(Arc<RpcEndpoint>, f64)> {
        if use_cache {
            if let Some(score) = self.get_cached_score(endpoint.id, send_mode, now) {
                return Some((endpoint.clone(), score));
            }
            return None;
        }

        let state = endpoint.state.lock().expect("rpc endpoint state lock");
        if matches!(state.cooldown_until, Some(until) if until > now) {
            return None;
        }

        let burst_capacity_units = endpoint_burst_capacity_units(endpoint.kind, send_mode);
        let recent_burst_units = burst_load_units(&state, now);
        let burst_ratio = recent_burst_units as f64 / burst_capacity_units.max(1) as f64;
        let latency_ms = state
            .avg_latency
            .map(|value| value.as_secs_f64() * 1000.0)
            .unwrap_or(120.0);
        let failure_penalty = f64::from(state.failures) * 400.0;
        let rate_limit_penalty = f64::from(state.rate_limit_failures) * 5000.0;
        let stale_penalty = if matches!(state.last_block_at, Some(at) if at.elapsed() > Duration::from_secs(30))
        {
            500.0
        } else {
            0.0
        };
        let kind_bias = match (endpoint.kind, send_mode) {
            (RpcKind::Tenderly, true) => -80.0,
            (RpcKind::Tenderly, false) => -120.0,
            (RpcKind::Alchemy, true) => 0.0,
            (RpcKind::Alchemy, false) => -100.0,
            (RpcKind::Infura, true) => -50.0,
            (RpcKind::Infura, false) => 0.0,
        };
        let infura_rate_limited_penalty =
            if matches!(endpoint.kind, RpcKind::Infura) && state.rate_limit_failures > 0 {
                20_000.0
            } else {
                0.0
            };
        let burst_penalty = if burst_ratio >= 1.0 {
            25_000.0 + (burst_ratio - 1.0) * 8_000.0
        } else {
            burst_ratio.powf(2.2) * 2_200.0
        };
        let recency_penalty = state
            .last_selected_at
            .map(|last| {
                let elapsed_ms = now.saturating_duration_since(last).as_millis() as f64;
                if elapsed_ms < 180.0 {
                    (180.0 - elapsed_ms) * 2.4
                } else {
                    0.0
                }
            })
            .unwrap_or(0.0);

        Some((
            endpoint.clone(),
            latency_ms
                + failure_penalty
                + rate_limit_penalty
                + stale_penalty
                + infura_rate_limited_penalty
                + burst_penalty
                + recency_penalty
                + kind_bias,
        ))
    }

    fn get_cached_score(&self, endpoint_id: usize, send_mode: bool, now: Instant) -> Option<f64> {
        {
            let cache = self.score_cache.lock().expect("rpc score cache lock");
            if cache.updated_at.elapsed() <= Duration::from_millis(500) {
                let scores = if send_mode {
                    &cache.send_scores
                } else {
                    &cache.read_scores
                };
                return scores.get(endpoint_id).copied().flatten();
            }
        }

        self.recompute_scores(now);

        let cache = self.score_cache.lock().expect("rpc score cache lock");
        let scores = if send_mode {
            &cache.send_scores
        } else {
            &cache.read_scores
        };
        scores.get(endpoint_id).copied().flatten()
    }

    fn recompute_scores(&self, now: Instant) {
        let mut read_scores = vec![None; self.endpoints.len()];
        let mut send_scores = vec![None; self.endpoints.len()];

        for endpoint in &self.endpoints {
            read_scores[endpoint.id] = self
                .endpoint_score(endpoint, now, false, false)
                .map(|(_, score)| score);
            send_scores[endpoint.id] = self
                .endpoint_score(endpoint, now, true, false)
                .map(|(_, score)| score);
        }

        let mut cache = self.score_cache.lock().expect("rpc score cache lock");
        cache.updated_at = Instant::now();
        cache.read_scores = read_scores;
        cache.send_scores = send_scores;
    }

    fn reserve_selection(&self, endpoint_id: usize, send_mode: bool) {
        let now = Instant::now();
        let Some(endpoint) = self.endpoints.iter().find(|endpoint| endpoint.id == endpoint_id) else {
            return;
        };
        self.reserve_burst_units(endpoint, endpoint_burst_cost_units(endpoint.kind, send_mode), now);
    }

    fn reserve_burst_units(&self, endpoint: &Arc<RpcEndpoint>, units: u32, now: Instant) {
        let mut state = endpoint.state.lock().expect("rpc endpoint state lock");
        prune_burst_reservations(&mut state, now);
        state.recent_burst_reservations.push_back(BurstReservation {
            reserved_at: now,
            units,
        });
        state.last_selected_at = Some(now);

        let send_capacity = endpoint_burst_capacity_units(endpoint.kind, true);
        let recent_send_units = burst_load_units(&state, now);
        if recent_send_units >= send_capacity.saturating_mul(2) {
            state.cooldown_until = Some(now + Duration::from_millis(900));
        }
        drop(state);
        self.invalidate_score_cache();
    }

    fn invalidate_score_cache(&self) {
        let mut cache = self.score_cache.lock().expect("rpc score cache lock");
        cache.updated_at = Instant::now() - Duration::from_secs(2);
    }

    fn to_handle(&self, endpoint: &Arc<RpcEndpoint>) -> RpcHandle {
        RpcHandle {
            id: endpoint.id,
            name: endpoint.name.clone(),
            url: endpoint.url.clone(),
            provider: endpoint.provider.clone(),
            client: endpoint.client.clone(),
        }
    }

    fn matches_preference(&self, kind: RpcKind, preference: RpcPreference) -> bool {
        match preference {
            RpcPreference::Auto => true,
            RpcPreference::Alchemy => matches!(kind, RpcKind::Alchemy),
            RpcPreference::Infura => matches!(kind, RpcKind::Infura),
        }
    }
}

impl RpcHandle {
    pub async fn get_balances_batch(&self, addresses: &[Address]) -> Result<Vec<U256>, String> {
        let payload: Vec<Value> = addresses
            .iter()
            .enumerate()
            .map(|(idx, address)| {
                json!({
                    "jsonrpc": "2.0",
                    "method": "eth_getBalance",
                    "params": [format!("{:#x}", address), "latest"],
                    "id": idx,
                })
            })
            .collect();

        let response = self
            .client
            .post(&self.url)
            .json(&payload)
            .send()
            .await
            .map_err(|err| err.to_string())?
            .error_for_status()
            .map_err(|err| err.to_string())?;

        let body: Value = response.json().await.map_err(|err| err.to_string())?;
        let items = body
            .as_array()
            .ok_or_else(|| "batch eth_getBalance did not return an array response".to_string())?;

        let mut ordered = vec![U256::zero(); addresses.len()];
        for item in items {
            let id = item
                .get("id")
                .and_then(Value::as_u64)
                .ok_or_else(|| "missing id in batch response".to_string())?
                as usize;
            let result = item
                .get("result")
                .and_then(Value::as_str)
                .ok_or_else(|| "missing result in batch response".to_string())?;
            let value = parse_hex_u256(result)?;
            if id < ordered.len() {
                ordered[id] = value;
            }
        }

        Ok(ordered)
    }
}

fn parse_hex_u256(value: &str) -> Result<U256, String> {
    let trimmed = value.strip_prefix("0x").unwrap_or(value);
    if trimmed.is_empty() {
        return Ok(U256::zero());
    }
    U256::from_str_radix(trimmed, 16).map_err(|err| err.to_string())
}

const BURST_WINDOW_MS: u64 = 1_200;

fn prune_burst_reservations(state: &mut RpcEndpointState, now: Instant) {
    while matches!(
        state.recent_burst_reservations.front(),
        Some(item) if now.saturating_duration_since(item.reserved_at) > Duration::from_millis(BURST_WINDOW_MS)
    ) {
        state.recent_burst_reservations.pop_front();
    }
}

fn burst_load_units(state: &RpcEndpointState, now: Instant) -> u32 {
    state
        .recent_burst_reservations
        .iter()
        .filter(|item| {
            now.saturating_duration_since(item.reserved_at)
                <= Duration::from_millis(BURST_WINDOW_MS)
        })
        .map(|item| item.units)
        .sum()
}

fn endpoint_burst_capacity_units(kind: RpcKind, send_mode: bool) -> u32 {
    match (kind, send_mode) {
        (RpcKind::Alchemy, false) => 24,
        (RpcKind::Alchemy, true) => 18,
        (RpcKind::Infura, false) => 18,
        (RpcKind::Infura, true) => 12,
        (RpcKind::Tenderly, false) => 10,
        (RpcKind::Tenderly, true) => 8,
    }
}

fn endpoint_burst_cost_units(kind: RpcKind, send_mode: bool) -> u32 {
    match (kind, send_mode) {
        (RpcKind::Alchemy, false) => 1,
        (RpcKind::Alchemy, true) => 7,
        (RpcKind::Infura, false) => 1,
        (RpcKind::Infura, true) => 6,
        (RpcKind::Tenderly, false) => 2,
        (RpcKind::Tenderly, true) => 8,
    }
}
