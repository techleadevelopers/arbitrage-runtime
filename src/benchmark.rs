use crate::config::Config;
use crate::rpc::RpcFleet;
use ethers::middleware::SignerMiddleware;
use ethers::prelude::*;
use ethers::types::transaction::eip2718::TypedTransaction;
use ethers::types::{BlockNumber, Filter, NameOrAddress, TransactionRequest, H256};
use ethers_flashbots::{BundleRequest, FlashbotsMiddleware};
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tracing::{info, warn};
use url::Url;

pub async fn maybe_run_network_benchmark(
    config: Arc<Config>,
    rpc_fleet: Arc<RpcFleet>,
    wallets: &[LocalWallet],
) -> Result<bool, Box<dyn std::error::Error>> {
    if !env_flag("RUN_NETWORK_BENCHMARK") {
        return Ok(false);
    }

    let samples = env_u64("NETWORK_BENCHMARK_SAMPLES", 12) as usize;
    let rounds = env_usize("NETWORK_BENCHMARK_ROUNDS", 1).max(1);
    let wallet_sample_size = env_usize("NETWORK_BENCHMARK_WALLETS", 25)
        .max(1)
        .min(wallets.len().max(1));
    let bench_bundle = env_flag("NETWORK_BENCHMARK_BUNDLE") && config.uses_bundle_relays();
    let host_region = env::var("AWS_REGION")
        .or_else(|_| env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "unknown".to_string());

    info!(
        "Running network benchmark: rounds={} samples={} wallet_sample_size={} bundle={}",
        rounds, samples, wallet_sample_size, bench_bundle
    );

    let sample_addresses: Vec<Address> = wallets
        .iter()
        .take(wallet_sample_size.min(wallets.len()))
        .map(LocalWallet::address)
        .collect();

    if sample_addresses.is_empty() {
        warn!("network benchmark skipped batch/nonce probes because no wallets were loaded");
    }

    println!("=== Network Benchmark ===");
    println!("Network: {}", config.network);
    println!("Host region: {}", host_region);
    println!("RPC endpoints: {}", rpc_fleet.endpoint_count());
    println!("Rounds: {}", rounds);
    println!("Samples per endpoint: {}", samples);
    println!("Wallet sample size: {}", sample_addresses.len());
    println!("Bundle probe enabled: {}", bench_bundle);
    println!();

    let endpoint_handles = rpc_fleet.all_handles();
    let endpoint_snapshots = rpc_fleet.snapshot();
    let mut join_set = JoinSet::new();
    for handle in endpoint_snapshots {
        let endpoint = endpoint_handles
            .iter()
            .find(|candidate| candidate.id == handle.id)
            .cloned()
            .ok_or("benchmark endpoint lookup failed")?;
        let config = config.clone();
        let sample_addresses = sample_addresses.clone();
        join_set.spawn(async move {
            benchmark_endpoint(
                config,
                handle,
                endpoint,
                sample_addresses,
                samples,
                rounds,
                bench_bundle,
            )
            .await
        });
    }

    let mut reports = Vec::new();
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(report)) => reports.push(report),
            Ok(Err(err)) => return Err(err.into()),
            Err(err) => return Err(format!("benchmark task failed: {}", err).into()),
        }
    }

    reports.sort_by_key(|report| report.id);
    for report in &reports {
        print_endpoint_report(report);
    }

    print_rankings(&reports);

    Ok(true)
}

async fn benchmark_endpoint(
    config: Arc<Config>,
    handle: crate::rpc::RpcEndpointSnapshot,
    endpoint: crate::rpc::RpcHandle,
    sample_addresses: Vec<Address>,
    samples: usize,
    rounds: usize,
    bench_bundle: bool,
) -> Result<EndpointBenchmarkReport, String> {
    let receipt_probe_hash = recent_transaction_hash(endpoint.provider.clone()).await;
    let call_target = sample_addresses
        .first()
        .copied()
        .unwrap_or_else(Address::zero);
    let logs_filter = Filter::new().from_block(BlockNumber::Latest);

    let block_metrics = measure_rounds(rounds, samples, || {
        let provider = endpoint.provider.clone();
        async move {
            provider
                .get_block_number()
                .await
                .map(|_| ())
                .map_err(|err| err.to_string())
        }
    })
    .await;

    let gas_metrics = measure_rounds(rounds, samples, || {
        let provider = endpoint.provider.clone();
        async move {
            provider
                .get_gas_price()
                .await
                .map(|_| ())
                .map_err(|err| err.to_string())
        }
    })
    .await;

    let batch_metrics = if sample_addresses.is_empty() {
        ProbeMetrics::single_error("skipped: no wallets loaded".to_string())
    } else {
        measure_rounds(rounds, samples, || {
            let rpc = endpoint.clone();
            let addresses = sample_addresses.clone();
            async move { rpc.get_balances_batch(&addresses).await.map(|_| ()) }
        })
        .await
    };

    let nonce_metrics = if sample_addresses.is_empty() {
        ProbeMetrics::single_error("skipped: no wallets loaded".to_string())
    } else {
        measure_rounds(rounds, samples, || {
            let provider = endpoint.provider.clone();
            let address = sample_addresses[0];
            async move {
                provider
                    .get_transaction_count(address, None)
                    .await
                    .map(|_| ())
                    .map_err(|err| err.to_string())
            }
        })
        .await
    };

    let logs_metrics = measure_rounds(rounds, samples, || {
        let provider = endpoint.provider.clone();
        let filter = logs_filter.clone();
        async move {
            provider
                .get_logs(&filter)
                .await
                .map(|_| ())
                .map_err(|err| err.to_string())
        }
    })
    .await;

    let call_metrics = measure_rounds(rounds, samples, || {
        let provider = endpoint.provider.clone();
        let tx: TypedTransaction = TransactionRequest::new()
            .to(NameOrAddress::Address(call_target))
            .from(call_target)
            .into();
        async move {
            provider
                .call(&tx, None)
                .await
                .map(|_| ())
                .map_err(|err| err.to_string())
        }
    })
    .await;

    let receipt_metrics = if let Some(tx_hash) = receipt_probe_hash {
        measure_rounds(rounds, samples, || {
            let provider = endpoint.provider.clone();
            async move {
                provider
                    .get_transaction_receipt(tx_hash)
                    .await
                    .map(|_| ())
                    .map_err(|err| err.to_string())
            }
        })
        .await
    } else {
        ProbeMetrics::single_error("skipped: no recent transaction hash available".to_string())
    };

    let bundle_metrics = if bench_bundle {
        benchmark_bundle_submission(&config, &endpoint).await
    } else {
        ProbeMetrics::single_error("skipped: set NETWORK_BENCHMARK_BUNDLE=true".to_string())
    };

    let read_score = blended_score(&[
        &block_metrics,
        &gas_metrics,
        &batch_metrics,
        &nonce_metrics,
        &logs_metrics,
        &call_metrics,
        &receipt_metrics,
    ]);
    let send_score = if bench_bundle {
        blended_score(&[&bundle_metrics])
    } else {
        blended_score(&[&block_metrics, &nonce_metrics, &call_metrics])
    };

    Ok(EndpointBenchmarkReport {
        id: handle.id,
        kind: handle.kind,
        name: handle.name,
        url: handle.url,
        read_score,
        send_score,
        probes: vec![
            ProbeReport::new("get_block_number", block_metrics),
            ProbeReport::new("get_gas_price", gas_metrics),
            ProbeReport::new("get_balances_batch", batch_metrics),
            ProbeReport::new("get_transaction_count", nonce_metrics),
            ProbeReport::new("get_logs", logs_metrics),
            ProbeReport::new("eth_call", call_metrics),
            ProbeReport::new("get_transaction_receipt", receipt_metrics),
            ProbeReport::new("send_bundle", bundle_metrics),
        ],
    })
}

async fn benchmark_bundle_submission(
    config: &Config,
    endpoint: &crate::rpc::RpcHandle,
) -> ProbeMetrics {
    let samples = env_u64("NETWORK_BENCHMARK_BUNDLE_SAMPLES", 3) as usize;
    let sponsor_wallet = match config
        .executor_private_key
        .parse::<LocalWallet>()
        .map(|wallet| wallet.with_chain_id(config.chain_id))
    {
        Ok(wallet) => wallet,
        Err(err) => {
            return ProbeMetrics::single_error(format!("failed to load sponsor wallet: {}", err));
        }
    };

    let relay_url = match Url::parse(&config.flashbots_relay) {
        Ok(url) => url,
        Err(err) => return ProbeMetrics::single_error(format!("invalid relay url: {}", err)),
    };

    measure_async(samples, || {
        let provider = endpoint.provider.clone();
        let relay_signer = sponsor_wallet.clone();
        let bundle_signer = sponsor_wallet.clone();
        let relay_url = relay_url.clone();
        let middleware_signer = sponsor_wallet.clone();
        async move {
            let latest_block = provider
                .get_block_number()
                .await
                .map_err(|err| format!("block fetch failed before bundle: {}", err))?;

            let tx: TypedTransaction = TransactionRequest::new()
                .to(bundle_signer.address())
                .value(U256::zero())
                .gas(21_000u64)
                .gas_price(U256::from(1_000_000_000u64))
                .nonce(
                    provider
                        .get_transaction_count(bundle_signer.address(), None)
                        .await
                        .map_err(|err| format!("nonce fetch failed before bundle: {}", err))?,
                )
                .from(bundle_signer.address())
                .into();
            let signature = bundle_signer
                .sign_transaction(&tx)
                .await
                .map_err(|err| format!("bundle sign failed: {}", err))?;
            let signed_tx = tx.rlp_signed(&signature);

            let flashbots_client = SignerMiddleware::new(provider.clone(), middleware_signer);
            let flashbots =
                FlashbotsMiddleware::new(flashbots_client, relay_url.clone(), relay_signer.clone());
            let bundle = BundleRequest::new()
                .set_block(latest_block + 1)
                .push_transaction(signed_tx);

            flashbots
                .send_bundle(&bundle)
                .await
                .map(|_| ())
                .map_err(|err| err.to_string())
        }
    })
    .await
}

#[derive(Clone, Default)]
struct ProbeMetrics {
    latencies: Vec<Duration>,
    errors: Vec<String>,
    timeout_count: usize,
}

impl ProbeMetrics {
    fn single_error(message: String) -> Self {
        Self {
            latencies: Vec::new(),
            errors: vec![message],
            timeout_count: 0,
        }
    }

    fn summary(&self) -> Option<ProbeSummary> {
        if self.latencies.is_empty() {
            return None;
        }

        let mut millis: Vec<u128> = self
            .latencies
            .iter()
            .map(|latency| latency.as_millis())
            .collect();
        millis.sort_unstable();
        let avg = millis.iter().sum::<u128>() as f64 / millis.len() as f64;
        let min = *millis.first().unwrap_or(&0);
        let max = *millis.last().unwrap_or(&0);
        let p50 = percentile(&millis, 50);
        let p95 = percentile(&millis, 95);
        let jitter = if millis.len() <= 1 {
            0.0
        } else {
            let mut total_delta = 0.0;
            for pair in millis.windows(2) {
                total_delta += (pair[1] as f64 - pair[0] as f64).abs();
            }
            total_delta / (millis.len() - 1) as f64
        };
        let total = millis.len() + self.errors.len();
        let success_rate = if total == 0 {
            0.0
        } else {
            millis.len() as f64 / total as f64
        };

        Some(ProbeSummary {
            avg_ms: avg,
            min_ms: min,
            max_ms: max,
            p50_ms: p50,
            p95_ms: p95,
            jitter_ms: jitter,
            success_rate,
            ok_count: millis.len(),
            err_count: self.errors.len(),
            timeout_count: self.timeout_count,
        })
    }
}

async fn measure_async<F, Fut>(samples: usize, mut f: F) -> ProbeMetrics
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let mut metrics = ProbeMetrics::default();

    for _ in 0..samples {
        let started = Instant::now();
        match f().await {
            Ok(()) => metrics.latencies.push(started.elapsed()),
            Err(err) => {
                if is_timeout_error(&err) {
                    metrics.timeout_count += 1;
                }
                metrics.errors.push(err);
            }
        }
    }

    metrics
}

async fn measure_rounds<F, Fut>(rounds: usize, samples: usize, mut f: F) -> ProbeMetrics
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let mut combined = ProbeMetrics::default();

    for _ in 0..rounds {
        let round = measure_async(samples, &mut f).await;
        combined.latencies.extend(round.latencies);
        combined.errors.extend(round.errors);
        combined.timeout_count += round.timeout_count;
    }

    combined
}

fn print_metrics(label: &str, metrics: &ProbeMetrics) {
    if let Some(summary) = metrics.summary() {
        println!(
            "  {}: ok={} err={} timeout={} success={:.1}% avg={:.1}ms p50={}ms p95={}ms min={}ms max={}ms jitter={:.1}ms",
            label,
            summary.ok_count,
            summary.err_count,
            summary.timeout_count,
            summary.success_rate * 100.0,
            summary.avg_ms,
            summary.p50_ms,
            summary.p95_ms,
            summary.min_ms,
            summary.max_ms,
            summary.jitter_ms
        );
    } else {
        println!("  {}: no successful samples", label);
    }

    if !metrics.errors.is_empty() {
        for error in metrics.errors.iter().take(3) {
            println!("    error: {}", error);
        }
        if metrics.errors.len() > 3 {
            println!("    error: ... and {} more", metrics.errors.len() - 3);
        }
    }
}

fn percentile(values: &[u128], percentile: usize) -> u128 {
    if values.is_empty() {
        return 0;
    }
    let rank = ((values.len() - 1) * percentile) / 100;
    values[rank]
}

#[derive(Clone)]
struct ProbeReport {
    label: &'static str,
    metrics: ProbeMetrics,
}

impl ProbeReport {
    fn new(label: &'static str, metrics: ProbeMetrics) -> Self {
        Self { label, metrics }
    }
}

struct EndpointBenchmarkReport {
    id: usize,
    kind: String,
    name: String,
    url: String,
    read_score: f64,
    send_score: f64,
    probes: Vec<ProbeReport>,
}

#[derive(Clone, Copy)]
struct ProbeSummary {
    avg_ms: f64,
    min_ms: u128,
    max_ms: u128,
    p50_ms: u128,
    p95_ms: u128,
    jitter_ms: f64,
    success_rate: f64,
    ok_count: usize,
    err_count: usize,
    timeout_count: usize,
}

fn print_endpoint_report(report: &EndpointBenchmarkReport) {
    println!("[{}] {} ({})", report.kind, report.name, report.url);
    println!(
        "  scores: read={:.2} send={:.2}",
        report.read_score, report.send_score
    );
    for probe in &report.probes {
        print_metrics(probe.label, &probe.metrics);
    }
    println!();
}

fn print_rankings(reports: &[EndpointBenchmarkReport]) {
    if reports.is_empty() {
        return;
    }

    let mut read_sorted: Vec<&EndpointBenchmarkReport> = reports.iter().collect();
    read_sorted.sort_by(|left, right| left.read_score.total_cmp(&right.read_score));
    let mut send_sorted: Vec<&EndpointBenchmarkReport> = reports.iter().collect();
    send_sorted.sort_by(|left, right| left.send_score.total_cmp(&right.send_score));

    println!("=== Benchmark Ranking ===");
    if let Some(best) = read_sorted.first() {
        println!(
            "Best read RPC: {} [{}] score={:.2}",
            best.name, best.kind, best.read_score
        );
    }
    if let Some(best) = send_sorted.first() {
        println!(
            "Best send path: {} [{}] score={:.2}",
            best.name, best.kind, best.send_score
        );
    }
    println!("Read ranking:");
    for report in read_sorted.iter().take(5) {
        println!(
            "  - {} [{}] read={:.2} send={:.2}",
            report.name, report.kind, report.read_score, report.send_score
        );
    }
    println!("Send ranking:");
    for report in send_sorted.iter().take(5) {
        println!(
            "  - {} [{}] send={:.2} read={:.2}",
            report.name, report.kind, report.send_score, report.read_score
        );
    }
    println!();
}

fn blended_score(metrics: &[&ProbeMetrics]) -> f64 {
    let mut score = 0.0;
    let mut counted = 0usize;
    for metric in metrics {
        if let Some(summary) = metric.summary() {
            let error_rate = 1.0 - summary.success_rate;
            score += summary.p50_ms as f64 * 0.20
                + summary.p95_ms as f64 * 0.45
                + summary.jitter_ms * 0.15
                + error_rate * 500.0
                + summary.timeout_count as f64 * 25.0;
            counted += 1;
        }
    }
    if counted == 0 {
        f64::INFINITY
    } else {
        score / counted as f64
    }
}

async fn recent_transaction_hash(provider: Arc<Provider<Http>>) -> Option<H256> {
    let block = provider
        .get_block_with_txs(BlockNumber::Latest)
        .await
        .ok()
        .flatten()?;
    block.transactions.first().map(|tx| tx.hash)
}

fn is_timeout_error(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("timeout") || lower.contains("timed out")
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .unwrap_or_else(|_| "false".to_string())
        .trim()
        .eq_ignore_ascii_case("true")
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(default)
}
