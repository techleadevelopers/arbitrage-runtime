use crate::config::Config;
use crate::mev::adaptive::{AdaptivePolicy, AdaptiveQuoteInput, ClusterKey, PreflightInput};
#[cfg(target_os = "linux")]
use crate::mev::execution::pinning::ThreadPinningConfig;
use crate::mev::opportunity::wei_to_eth_f64;
use crate::mev::runtime::{decode_relevant_swap, fast_preflight_gate};
use crate::rpc::RpcFleet;
use chrono::Timelike;
use chrono::Utc;
use ethers::abi::{encode, Token};
use ethers::middleware::SignerMiddleware;
use ethers::prelude::*;
use ethers::types::transaction::eip2718::TypedTransaction;
use ethers::types::{
    BlockNumber, Bytes, Filter, NameOrAddress, Transaction, TransactionRequest, H256,
};
use ethers_flashbots::{BundleRequest, FlashbotsMiddleware};
use serde::Serialize;
use std::collections::BTreeMap;
use std::env;
use std::fs;
#[cfg(target_os = "macos")]
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tracing::{info, warn};
use url::Url;

pub async fn maybe_run_runtime_load_test(
    config: Arc<Config>,
) -> Result<bool, Box<dyn std::error::Error>> {
    if !env_flag("RUN_RUNTIME_LOAD_TEST") {
        return Ok(false);
    }

    let cases = env_usize("RUNTIME_LOAD_TEST_CASES", 100_000).max(1);
    let warmup = env_usize("RUNTIME_LOAD_TEST_WARMUP", 5_000);
    let concurrency = env_usize(
        "RUNTIME_LOAD_TEST_CONCURRENCY",
        std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1),
    )
    .max(1)
    .min(cases);
    let budget_p99_us = env_u64("RUNTIME_LOAD_TEST_LATENCY_BUDGET_US", 250);
    let profile = runtime_load_profile();
    let output_path = env::var("RUNTIME_LOAD_TEST_OUTPUT_PATH").ok();
    let min_large_swap_wei = ethers::utils::parse_ether(config.mev.min_large_swap_eth.to_string())?;
    let gas_price_wei = runtime_load_gas_price_wei(&config.network);
    let relay_paths = if config.builder_relays.is_empty() {
        vec!["direct-rpc".to_string()]
    } else {
        config.builder_relays.clone()
    };

    info!(
        "Running runtime load test: cases={} warmup={} concurrency={} budget_p99={}us",
        cases, warmup, concurrency, budget_p99_us
    );

    let started = Instant::now();
    let mut worker_handles = Vec::with_capacity(concurrency);
    let base_cases = cases / concurrency;
    let remainder = cases % concurrency;
    let base_warmup = warmup / concurrency;
    let warmup_remainder = warmup % concurrency;
    for worker_id in 0..concurrency {
        let config = config.clone();
        let relays = relay_paths.clone();
        let worker_cases = base_cases + usize::from(worker_id < remainder);
        let worker_warmup = base_warmup + usize::from(worker_id < warmup_remainder);
        worker_handles.push(tokio::task::spawn_blocking(move || {
            runtime_load_worker(
                config,
                worker_id,
                worker_cases,
                worker_warmup,
                min_large_swap_wei,
                gas_price_wei,
                relays,
                profile,
            )
        }));
    }

    let mut combined = RuntimeLoadReport::new(
        config.network.clone(),
        cases,
        warmup,
        concurrency,
        budget_p99_us,
        profile,
    );
    for handle in worker_handles {
        let result = handle.await;
        match result {
            Ok(Ok(report)) => combined.merge(report),
            Ok(Err(err)) => return Err(err.into()),
            Err(err) => return Err(format!("runtime load test task failed: {}", err).into()),
        }
    }
    combined.wall_ms = started.elapsed().as_secs_f64() * 1_000.0;
    combined.finalize();

    print_runtime_load_report(&combined);
    if let Some(path) = output_path {
        fs::write(&path, serde_json::to_string_pretty(&combined)?)?;
        println!("Runtime load report exported: {}", path);
    }
    export_runtime_latency_benchmarks(&combined)?;
    emit_runtime_budget_warning(&combined);

    Ok(true)
}

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
            Ok(Err(err)) => return Err(Box::<dyn std::error::Error>::from(err)),
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

fn runtime_load_worker(
    config: Arc<Config>,
    worker_id: usize,
    cases: usize,
    warmup: usize,
    min_large_swap_wei: U256,
    base_gas_price_wei: U256,
    relays: Vec<String>,
    profile: RuntimeLoadProfile,
) -> Result<RuntimeLoadWorkerReport, String> {
    let mut policy = AdaptivePolicy::new(&config);
    let router = config
        .mev
        .mev_executor
        .unwrap_or_else(|| Address::from_low_u64_be(0x7000 + worker_id as u64));
    let token_in = config
        .monitored_tokens
        .first()
        .map(|token| token.address)
        .unwrap_or_else(|| Address::from_low_u64_be(0x1000));
    let recipient = config.executor_address;
    let hour_utc = chrono::Utc::now().hour() as u8;
    let mut report = RuntimeLoadWorkerReport::default();
    let total_iterations = cases.saturating_add(warmup);

    #[cfg(target_os = "linux")]
    {
        let pinning = ThreadPinningConfig::auto_detect();
        let pin = pinning.pin_runtime_load_worker(worker_id);
        report.pinning_enabled = pin.success;
        if worker_id == 0 {
            let current = pin
                .current_core
                .map(|core| core.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            println!(
                "Runtime load test pinning: total_hot_gate worker pinned requested_core={} current_core={} numa_node={} success={}",
                pin.requested_core, current, pin.numa_node, pin.success
            );
            info!(
                "Linux runtime load test pinning: requested_core={} current_core={} numa_node={} success={}",
                pin.requested_core, current, pin.numa_node, pin.success
            );
        }
    }
    for index in 0..total_iterations {
        let record = index >= warmup;
        let scenario = runtime_load_scenario(index, profile);
        let gas_price_wei = scenario_gas_price(base_gas_price_wei, scenario);
        let tx = synthetic_runtime_tx(
            scenario,
            worker_id,
            index,
            router,
            token_in,
            Address::from_low_u64_be(0x2000 + ((worker_id * 65_537 + index) % 4096) as u64),
            recipient,
            min_large_swap_wei,
            gas_price_wei,
            config.chain_id,
        );

        let total_started = Instant::now();
        let started = Instant::now();
        let signal = decode_relevant_swap(&tx, &config.monitored_tokens, min_large_swap_wei);
        if record {
            report.samples.decode_us.push(elapsed_us(started));
            report.processed += 1;
            report.scenario_mut(scenario).processed += 1;
        }

        let Some(signal) = signal else {
            if record {
                report.decode_reject += 1;
                report.scenario_mut(scenario).decode_reject += 1;
                report
                    .samples
                    .total_hot_gate_us
                    .push(elapsed_us(total_started));
            }
            continue;
        };
        if record {
            report.decoded += 1;
            report.scenario_mut(scenario).decoded += 1;
        }

        let cluster = ClusterKey {
            router: signal.router,
            token_in: signal.path.first().copied().unwrap_or_default(),
            token_out: signal.path.last().copied().unwrap_or_default(),
            selector: signal.selector,
        };
        let context_signal = policy.context_signal(signal.router, hour_utc);
        policy.observe_candidate_flow(cluster, signal.notional_wei, gas_price_wei);

        let started = Instant::now();
        let fast = fast_preflight_gate(
            &signal,
            gas_price_wei,
            min_large_swap_wei,
            &config,
            context_signal,
        );
        if record {
            report.samples.fast_preflight_us.push(elapsed_us(started));
        }
        if !fast.should_continue {
            if record {
                report.fast_reject += 1;
                report.scenario_mut(scenario).fast_reject += 1;
                report
                    .samples
                    .total_hot_gate_us
                    .push(elapsed_us(total_started));
            }
            continue;
        }
        if record {
            report.fast_pass += 1;
            report.scenario_mut(scenario).fast_pass += 1;
        }

        let notional_eth = wei_to_eth_f64(signal.notional_wei);
        let started = Instant::now();
        let preflight = policy.preflight_score(PreflightInput {
            cluster,
            notional_eth,
            gas_price_wei,
            path_len: signal.path.len(),
        });
        if record {
            report
                .samples
                .adaptive_preflight_us
                .push(elapsed_us(started));
        }
        if !preflight.should_continue {
            if record {
                report.adaptive_preflight_reject += 1;
                report.scenario_mut(scenario).adaptive_preflight_reject += 1;
                report
                    .samples
                    .total_hot_gate_us
                    .push(elapsed_us(total_started));
            }
            continue;
        }
        if record {
            report.adaptive_preflight_pass += 1;
            report.scenario_mut(scenario).adaptive_preflight_pass += 1;
        }

        let execution_cost_wei = gas_price_wei.saturating_mul(U256::from(
            config
                .estimated_exec_gas
                .saturating_add(config.estimated_bundle_overhead_gas)
                .max(180_000),
        ));
        let expected_profit_wei = min_large_swap_wei / U256::from(200u64);
        let started = Instant::now();
        let quote = policy.quote_for_relays(
            AdaptiveQuoteInput {
                cluster,
                pair: cluster.token_out,
                hour_utc,
                context_priority_score: context_signal.priority_score,
                context_toxicity_score: context_signal.toxicity_score,
                expected_profit_wei,
                execution_cost_wei,
                gas_price_wei,
                lookup_latency_ms: f64::from(fast.gas_ratio as f32).max(0.0),
                notional_eth,
                price_impact_bps: config.mev.max_price_impact_bps.min(80),
                relay_pressure_override: None,
            },
            &relays,
        );
        if record {
            report.samples.adaptive_quote_us.push(elapsed_us(started));
            report
                .samples
                .total_hot_gate_us
                .push(elapsed_us(total_started));
        }
        if quote.should_execute {
            if record {
                report.quote_pass += 1;
                report.scenario_mut(scenario).quote_pass += 1;
            }
        } else if record {
            report.quote_reject += 1;
            report.scenario_mut(scenario).quote_reject += 1;
        }
    }

    Ok(report)
}

fn synthetic_runtime_tx(
    scenario: RuntimeLoadScenario,
    worker_id: usize,
    index: usize,
    router: Address,
    token_in: Address,
    token_out: Address,
    recipient: Address,
    min_large_swap_wei: U256,
    gas_price_wei: U256,
    chain_id: u64,
) -> Transaction {
    match scenario {
        RuntimeLoadScenario::MalformedCalldata => synthetic_raw_tx(
            worker_id,
            index,
            router,
            Bytes::from(vec![0x7f, 0xf3]),
            min_large_swap_wei,
            gas_price_wei,
            chain_id,
        ),
        RuntimeLoadScenario::UnknownSelector => synthetic_raw_tx(
            worker_id,
            index,
            router,
            Bytes::from(vec![0xde, 0xad, 0xbe, 0xef, 0, 1, 2, 3]),
            min_large_swap_wei,
            gas_price_wei,
            chain_id,
        ),
        RuntimeLoadScenario::SmallNotional => synthetic_v2_eth_swap_tx(
            worker_id,
            index,
            router,
            token_in,
            token_out,
            recipient,
            min_large_swap_wei / U256::from(10u64),
            gas_price_wei,
            chain_id,
            1,
            2,
        ),
        RuntimeLoadScenario::LongPath => synthetic_v2_eth_swap_tx(
            worker_id,
            index,
            router,
            token_in,
            token_out,
            recipient,
            min_large_swap_wei,
            gas_price_wei,
            chain_id,
            1,
            8,
        ),
        RuntimeLoadScenario::ClusterBurst => synthetic_v2_eth_swap_tx(
            worker_id,
            index,
            router,
            token_in,
            Address::from_low_u64_be(0x2bad),
            recipient,
            min_large_swap_wei,
            gas_price_wei,
            chain_id,
            3,
            2,
        ),
        RuntimeLoadScenario::ToxicGas | RuntimeLoadScenario::Valid => synthetic_v2_eth_swap_tx(
            worker_id,
            index,
            router,
            token_in,
            token_out,
            recipient,
            min_large_swap_wei,
            gas_price_wei,
            chain_id,
            3,
            2,
        ),
    }
}

fn synthetic_v2_eth_swap_tx(
    worker_id: usize,
    index: usize,
    router: Address,
    token_in: Address,
    token_out: Address,
    recipient: Address,
    base_value_wei: U256,
    gas_price_wei: U256,
    chain_id: u64,
    min_size_mult: u64,
    path_len: usize,
) -> Transaction {
    let size_mult = U256::from(min_size_mult + ((worker_id + index) % 5) as u64);
    let value = base_value_wei.saturating_mul(size_mult);
    let mut path = Vec::with_capacity(path_len.max(2));
    path.push(Token::Address(token_in));
    for hop in 1..path_len.saturating_sub(1) {
        path.push(Token::Address(Address::from_low_u64_be(
            0x3000 + ((worker_id + index + hop) % 8192) as u64,
        )));
    }
    path.push(Token::Address(token_out));
    let mut calldata = Vec::with_capacity(4 + 160);
    calldata.extend_from_slice(&[0x7f, 0xf3, 0x6a, 0xb5]);
    calldata.extend(encode(&[
        Token::Uint(U256::zero()),
        Token::Array(path),
        Token::Address(recipient),
        Token::Uint(U256::from(4_102_444_800u64)),
    ]));

    synthetic_raw_tx(
        worker_id,
        index,
        router,
        Bytes::from(calldata),
        value,
        gas_price_wei,
        chain_id,
    )
}

fn synthetic_raw_tx(
    worker_id: usize,
    index: usize,
    router: Address,
    input: Bytes,
    value: U256,
    gas_price_wei: U256,
    chain_id: u64,
) -> Transaction {
    Transaction {
        hash: H256::from_low_u64_be(((worker_id as u64) << 32) | index as u64),
        nonce: U256::from(index),
        block_hash: None,
        block_number: None,
        transaction_index: None,
        from: Address::from_low_u64_be(0x9000 + worker_id as u64),
        to: Some(router),
        value,
        gas_price: Some(gas_price_wei),
        gas: U256::from(240_000u64),
        input,
        chain_id: Some(U256::from(chain_id)),
        ..Default::default()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RuntimeLoadProfile {
    Baseline,
    Adversarial,
}

impl RuntimeLoadProfile {
    fn as_str(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::Adversarial => "adversarial",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum RuntimeLoadScenario {
    Valid,
    MalformedCalldata,
    UnknownSelector,
    SmallNotional,
    ToxicGas,
    LongPath,
    ClusterBurst,
}

impl RuntimeLoadScenario {
    fn as_str(self) -> &'static str {
        match self {
            Self::Valid => "valid",
            Self::MalformedCalldata => "malformed_calldata",
            Self::UnknownSelector => "unknown_selector",
            Self::SmallNotional => "small_notional",
            Self::ToxicGas => "toxic_gas",
            Self::LongPath => "long_path",
            Self::ClusterBurst => "cluster_burst",
        }
    }
}

fn runtime_load_profile() -> RuntimeLoadProfile {
    let explicit = env::var("RUNTIME_LOAD_TEST_PROFILE")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if explicit == "adversarial" || env_flag("RUNTIME_LOAD_TEST_ADVERSARIAL") {
        RuntimeLoadProfile::Adversarial
    } else {
        RuntimeLoadProfile::Baseline
    }
}

fn runtime_load_scenario(index: usize, profile: RuntimeLoadProfile) -> RuntimeLoadScenario {
    if profile == RuntimeLoadProfile::Baseline {
        return RuntimeLoadScenario::Valid;
    }
    match index % 12 {
        0 => RuntimeLoadScenario::MalformedCalldata,
        1 => RuntimeLoadScenario::UnknownSelector,
        2 | 3 => RuntimeLoadScenario::SmallNotional,
        4 | 5 => RuntimeLoadScenario::ToxicGas,
        6 | 7 => RuntimeLoadScenario::LongPath,
        8 | 9 => RuntimeLoadScenario::ClusterBurst,
        _ => RuntimeLoadScenario::Valid,
    }
}

fn scenario_gas_price(base: U256, scenario: RuntimeLoadScenario) -> U256 {
    if scenario == RuntimeLoadScenario::ToxicGas {
        base.saturating_mul(U256::from(4u64))
    } else {
        base
    }
}

fn runtime_load_gas_price_wei(network: &str) -> U256 {
    let default_gwei = match network {
        "arbitrum" => 1,
        "polygon" => 80,
        "bsc" => 3,
        _ => 25,
    };
    U256::from(env_u64("RUNTIME_LOAD_TEST_GAS_PRICE_GWEI", default_gwei))
        .saturating_mul(U256::from(1_000_000_000u64))
}

#[derive(Default)]
struct RuntimeLoadWorkerReport {
    processed: usize,
    decoded: usize,
    decode_reject: usize,
    fast_pass: usize,
    fast_reject: usize,
    adaptive_preflight_pass: usize,
    adaptive_preflight_reject: usize,
    quote_pass: usize,
    quote_reject: usize,
    pinning_enabled: bool,
    scenarios: BTreeMap<String, RuntimeLoadCaseStats>,
    samples: RuntimeLoadSamples,
}

impl RuntimeLoadWorkerReport {
    fn scenario_mut(&mut self, scenario: RuntimeLoadScenario) -> &mut RuntimeLoadCaseStats {
        self.scenarios
            .entry(scenario.as_str().to_string())
            .or_default()
    }
}

#[derive(Clone, Default, Serialize)]
struct RuntimeLoadCaseStats {
    processed: usize,
    decoded: usize,
    decode_reject: usize,
    fast_pass: usize,
    fast_reject: usize,
    adaptive_preflight_pass: usize,
    adaptive_preflight_reject: usize,
    quote_pass: usize,
    quote_reject: usize,
}

#[derive(Default)]
struct RuntimeLoadSamples {
    decode_us: Vec<u64>,
    fast_preflight_us: Vec<u64>,
    adaptive_preflight_us: Vec<u64>,
    adaptive_quote_us: Vec<u64>,
    total_hot_gate_us: Vec<u64>,
}

#[derive(Serialize)]
struct RuntimeLoadReport {
    network: String,
    profile: &'static str,
    cases: usize,
    warmup: usize,
    concurrency: usize,
    wall_ms: f64,
    throughput_tps: f64,
    latency_budget_p99_us: u64,
    budget_pass: bool,
    hardware_metadata: HardwareMetadata,
    processed: usize,
    decoded: usize,
    decode_reject: usize,
    fast_pass: usize,
    fast_reject: usize,
    adaptive_preflight_pass: usize,
    adaptive_preflight_reject: usize,
    quote_pass: usize,
    quote_reject: usize,
    rejection_rate: f64,
    malformed_rejection_rate: f64,
    small_notional_rejection_rate: f64,
    toxic_gas_rejection_rate: f64,
    long_path_rejection_rate: f64,
    cluster_burst_rejection_rate: f64,
    scenarios: BTreeMap<String, RuntimeLoadCaseStats>,
    decode: LatencyUsSummary,
    fast_preflight: LatencyUsSummary,
    adaptive_preflight: LatencyUsSummary,
    adaptive_quote: LatencyUsSummary,
    total_hot_gate: LatencyUsSummary,
    #[serde(skip)]
    samples: RuntimeLoadSamples,
}

impl RuntimeLoadReport {
    fn new(
        network: String,
        cases: usize,
        warmup: usize,
        concurrency: usize,
        latency_budget_p99_us: u64,
        profile: RuntimeLoadProfile,
    ) -> Self {
        Self {
            network,
            profile: profile.as_str(),
            cases,
            warmup,
            concurrency,
            wall_ms: 0.0,
            throughput_tps: 0.0,
            latency_budget_p99_us,
            budget_pass: false,
            hardware_metadata: HardwareMetadata::detect(false),
            processed: 0,
            decoded: 0,
            decode_reject: 0,
            fast_pass: 0,
            fast_reject: 0,
            adaptive_preflight_pass: 0,
            adaptive_preflight_reject: 0,
            quote_pass: 0,
            quote_reject: 0,
            rejection_rate: 0.0,
            malformed_rejection_rate: 0.0,
            small_notional_rejection_rate: 0.0,
            toxic_gas_rejection_rate: 0.0,
            long_path_rejection_rate: 0.0,
            cluster_burst_rejection_rate: 0.0,
            scenarios: BTreeMap::new(),
            decode: LatencyUsSummary::default(),
            fast_preflight: LatencyUsSummary::default(),
            adaptive_preflight: LatencyUsSummary::default(),
            adaptive_quote: LatencyUsSummary::default(),
            total_hot_gate: LatencyUsSummary::default(),
            samples: RuntimeLoadSamples::default(),
        }
    }

    fn merge(&mut self, worker: RuntimeLoadWorkerReport) {
        self.processed += worker.processed;
        self.decoded += worker.decoded;
        self.decode_reject += worker.decode_reject;
        self.fast_pass += worker.fast_pass;
        self.fast_reject += worker.fast_reject;
        self.adaptive_preflight_pass += worker.adaptive_preflight_pass;
        self.adaptive_preflight_reject += worker.adaptive_preflight_reject;
        self.quote_pass += worker.quote_pass;
        self.quote_reject += worker.quote_reject;
        self.hardware_metadata.pinning_enabled |= worker.pinning_enabled;
        for (scenario, stats) in worker.scenarios {
            merge_case_stats(self.scenarios.entry(scenario).or_default(), stats);
        }
        self.samples.decode_us.extend(worker.samples.decode_us);
        self.samples
            .fast_preflight_us
            .extend(worker.samples.fast_preflight_us);
        self.samples
            .adaptive_preflight_us
            .extend(worker.samples.adaptive_preflight_us);
        self.samples
            .adaptive_quote_us
            .extend(worker.samples.adaptive_quote_us);
        self.samples
            .total_hot_gate_us
            .extend(worker.samples.total_hot_gate_us);
    }

    fn finalize(&mut self) {
        self.hardware_metadata = HardwareMetadata::detect(self.hardware_metadata.pinning_enabled);
        self.throughput_tps = if self.wall_ms <= f64::EPSILON {
            0.0
        } else {
            self.processed as f64 / (self.wall_ms / 1_000.0)
        };
        self.decode = summarize_us(&mut self.samples.decode_us);
        self.fast_preflight = summarize_us(&mut self.samples.fast_preflight_us);
        self.adaptive_preflight = summarize_us(&mut self.samples.adaptive_preflight_us);
        self.adaptive_quote = summarize_us(&mut self.samples.adaptive_quote_us);
        self.total_hot_gate = summarize_us(&mut self.samples.total_hot_gate_us);
        self.budget_pass = self.total_hot_gate.p99 <= self.latency_budget_p99_us;
        self.rejection_rate = rate(
            self.decode_reject
                + self.fast_reject
                + self.adaptive_preflight_reject
                + self.quote_reject,
            self.processed,
        );
        self.malformed_rejection_rate =
            scenario_rejection_rate(&self.scenarios, "malformed_calldata");
        self.small_notional_rejection_rate =
            scenario_rejection_rate(&self.scenarios, "small_notional");
        self.toxic_gas_rejection_rate = scenario_rejection_rate(&self.scenarios, "toxic_gas");
        self.long_path_rejection_rate = scenario_rejection_rate(&self.scenarios, "long_path");
        self.cluster_burst_rejection_rate =
            scenario_rejection_rate(&self.scenarios, "cluster_burst");
    }
}

#[derive(Clone, Copy, Default, Serialize)]
struct LatencyUsSummary {
    count: usize,
    avg: f64,
    min: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
}

#[derive(Clone, Serialize)]
struct HardwareMetadata {
    cpu_model: String,
    physical_cores: usize,
    logical_cores: usize,
    os: &'static str,
    arch: &'static str,
    build_profile: &'static str,
    pinning_enabled: bool,
}

impl HardwareMetadata {
    fn detect(pinning_enabled: bool) -> Self {
        Self {
            cpu_model: detect_cpu_model(),
            physical_cores: num_cpus::get_physical(),
            logical_cores: num_cpus::get(),
            os: env::consts::OS,
            arch: env::consts::ARCH,
            build_profile: if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            },
            pinning_enabled,
        }
    }
}

fn detect_cpu_model() -> String {
    #[cfg(target_os = "windows")]
    {
        if let Ok(model) = env::var("PROCESSOR_IDENTIFIER") {
            let model = model.trim();
            if !model.is_empty() {
                return model.to_string();
            }
        }
    }

    #[cfg(target_os = "linux")]
    {
        if let Ok(cpuinfo) = fs::read_to_string("/proc/cpuinfo") {
            for line in cpuinfo.lines() {
                if line.starts_with("model name") {
                    if let Some((_, value)) = line.split_once(':') {
                        let value = value.trim();
                        if !value.is_empty() {
                            return value.to_string();
                        }
                    }
                }
            }
        }
    }

    #[cfg(target_os = "macos")]
    {
        if let Ok(output) = Command::new("sysctl")
            .args(["-n", "machdep.cpu.brand_string"])
            .output()
        {
            if output.status.success() {
                let model = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if !model.is_empty() {
                    return model;
                }
            }
        }
    }

    "unknown".to_string()
}

fn summarize_us(values: &mut [u64]) -> LatencyUsSummary {
    if values.is_empty() {
        return LatencyUsSummary::default();
    }
    values.sort_unstable();
    let avg = values.iter().map(|value| *value as f64).sum::<f64>() / values.len() as f64;
    LatencyUsSummary {
        count: values.len(),
        avg,
        min: values[0],
        p50: percentile_u64(values, 50),
        p95: percentile_u64(values, 95),
        p99: percentile_u64(values, 99),
        max: *values.last().unwrap_or(&0),
    }
}

fn percentile_u64(values: &[u64], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let rank = ((values.len() - 1) * percentile) / 100;
    values[rank]
}

fn merge_case_stats(target: &mut RuntimeLoadCaseStats, source: RuntimeLoadCaseStats) {
    target.processed += source.processed;
    target.decoded += source.decoded;
    target.decode_reject += source.decode_reject;
    target.fast_pass += source.fast_pass;
    target.fast_reject += source.fast_reject;
    target.adaptive_preflight_pass += source.adaptive_preflight_pass;
    target.adaptive_preflight_reject += source.adaptive_preflight_reject;
    target.quote_pass += source.quote_pass;
    target.quote_reject += source.quote_reject;
}

fn scenario_rejection_rate(
    scenarios: &BTreeMap<String, RuntimeLoadCaseStats>,
    scenario: &str,
) -> f64 {
    let Some(stats) = scenarios.get(scenario) else {
        return 0.0;
    };
    rate(
        stats.decode_reject
            + stats.fast_reject
            + stats.adaptive_preflight_reject
            + stats.quote_reject,
        stats.processed,
    )
}

fn print_runtime_load_report(report: &RuntimeLoadReport) {
    println!("=== Runtime Load Test ===");
    println!("Network: {}", report.network);
    println!("Profile: {}", report.profile);
    println!("Cases: {}", report.cases);
    println!("Warmup: {}", report.warmup);
    println!("Concurrency: {}", report.concurrency);
    println!("Wall time: {:.1}ms", report.wall_ms);
    println!("Throughput: {:.0} tx/s", report.throughput_tps);
    println!(
        "Budget: total_hot_gate p99 <= {}us => {}",
        report.latency_budget_p99_us,
        if report.budget_pass { "PASS" } else { "FAIL" }
    );
    println!();
    println!(
        "Pass rates: decoded={:.1}% fast={:.1}% adaptive_preflight={:.1}% quote={:.1}%",
        rate(report.decoded, report.processed),
        rate(report.fast_pass, report.decoded),
        rate(report.adaptive_preflight_pass, report.fast_pass),
        rate(report.quote_pass, report.adaptive_preflight_pass),
    );
    println!(
        "Rejects: decode={} fast={} adaptive_preflight={} quote={}",
        report.decode_reject,
        report.fast_reject,
        report.adaptive_preflight_reject,
        report.quote_reject
    );
    println!("Total rejection rate: {:.1}%", report.rejection_rate);
    if report.profile == RuntimeLoadProfile::Adversarial.as_str() {
        println!(
            "Adversarial rejection: malformed={:.1}% small_notional={:.1}% toxic_gas={:.1}% long_path={:.1}% cluster_burst={:.1}%",
            report.malformed_rejection_rate,
            report.small_notional_rejection_rate,
            report.toxic_gas_rejection_rate,
            report.long_path_rejection_rate,
            report.cluster_burst_rejection_rate
        );
    }
    if !report.scenarios.is_empty() {
        println!("Scenario breakdown:");
        for (name, stats) in &report.scenarios {
            println!(
                "  {}: n={} decoded={} final_pass={} rejects={}",
                name,
                stats.processed,
                stats.decoded,
                stats.quote_pass,
                stats.decode_reject
                    + stats.fast_reject
                    + stats.adaptive_preflight_reject
                    + stats.quote_reject
            );
        }
    }
    println!();
    print_latency_us("decode", report.decode);
    print_latency_us("fast_preflight", report.fast_preflight);
    print_latency_us("adaptive_preflight", report.adaptive_preflight);
    print_latency_us("adaptive_quote", report.adaptive_quote);
    print_latency_us("total_hot_gate", report.total_hot_gate);
    println!();
}

fn export_runtime_latency_benchmarks(
    report: &RuntimeLoadReport,
) -> Result<(), Box<dyn std::error::Error>> {
    let exports_dir = crate::storage::ensure_exports_dir()?;
    let path = exports_dir.join("runtime_latency_benchmarks.json");
    let mut root = if path.exists() {
        serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&fs::read_to_string(
            &path,
        )?)
        .unwrap_or_default()
    } else {
        serde_json::Map::new()
    };
    root.insert(
        "updated_at".to_string(),
        serde_json::Value::String(Utc::now().to_rfc3339()),
    );
    root.insert(
        "network".to_string(),
        serde_json::Value::String(report.network.clone()),
    );
    root.insert(
        "hardware_metadata".to_string(),
        serde_json::to_value(&report.hardware_metadata)?,
    );
    root.insert(report.profile.to_string(), serde_json::to_value(report)?);
    let json = serde_json::to_string_pretty(&root)?;
    fs::write(&path, &json)?;
    let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ");
    let versioned = exports_dir.join(format!(
        "runtime_latency_benchmarks.{}.{}.{}.{}.json",
        report.network,
        report.profile,
        std::env::consts::OS,
        timestamp
    ));
    fs::write(&versioned, json)?;
    println!("Runtime benchmark export: {}", path.display());
    println!(
        "Runtime benchmark versioned export: {}",
        versioned.display()
    );
    if let Some(reference) = crate::storage::maybe_freeze_reference_artifact(&versioned)? {
        println!(
            "Runtime benchmark reference freeze: {}",
            reference.display()
        );
    }
    Ok(())
}

fn emit_runtime_budget_warning(report: &RuntimeLoadReport) {
    if report.budget_pass {
        return;
    }

    let environment_hint = if env::var("WSL_DISTRO_NAME").is_ok() {
        "WSL scheduler jitter accumulated into the hot-path tail"
    } else if cfg!(target_os = "windows") {
        "Windows scheduler jitter accumulated into the hot-path tail"
    } else if cfg!(target_os = "linux") {
        "Linux run is likely missing isolated cores, NUMA locality, or clean pinning"
    } else {
        "the current environment is adding scheduler noise to the hot-path tail"
    };

    let message = format!(
        "runtime load test exceeded the 250us release budget: total_hot_gate p99={}us max={}us; {}",
        report.total_hot_gate.p99, report.total_hot_gate.max, environment_hint
    );

    if cfg!(debug_assertions) {
        warn!("debug build budget miss: {}", message);
    } else {
        warn!("{}", message);
        println!("WARNING: {}", message);
    }
}
fn print_latency_us(label: &str, summary: LatencyUsSummary) {
    println!(
        "  {}: n={} avg={:.2}us p50={}us p95={}us p99={}us min={}us max={}us",
        label,
        summary.count,
        summary.avg,
        summary.p50,
        summary.p95,
        summary.p99,
        summary.min,
        summary.max
    );
}

fn rate(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 * 100.0 / denominator as f64
    }
}

fn elapsed_us(started: Instant) -> u64 {
    started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64
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
