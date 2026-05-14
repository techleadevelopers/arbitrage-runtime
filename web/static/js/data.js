const NETWORKS = ["bsc", "polygon"];
const RELAYS = [
  "rpc://primary-send",
  "relay://builder-alpha",
  "relay://builder-beta",
];

function rand(min, max) { return Math.random() * (max - min) + min; }
function randInt(min, max) { return Math.floor(rand(min, max + 1)); }
function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }

function makeAddr(seed) {
  let s = seed.toString(16).padStart(8, "0");
  let body = "";
  for (let i = 0; i < 5; i++) body += s.split("").reverse().join("");
  return "0x" + body.slice(0, 40);
}

class DataSource {
  constructor() {
    this.useLive = false;
    this.live = null;
    this.tick = 0;
    this.state = this.seed();
    this.probeBackend();
    setInterval(() => this.probeBackend(), 10_000);
  }

  seed() {
    const network = pick(NETWORKS);
    const executor = makeAddr(101);
    const vault = makeAddr(202);
    const profit = makeAddr(303);
    return {
      runtime_mode: "fee-extraction",
      market_regime: "normal",
      allow_send: true,
      network,
      control_address: makeAddr(404),
      vault_address: vault,
      executor_address: executor,
      profit_address: profit,
      min_candidate_eth: "25.0",
      min_net_profit_eth: "0.0025",
      executor_min_buffer_eth: "0.2000",
      executor_target_buffer_eth: "0.5000",
      executor_max_buffer_eth: "1.0000",
      executor_balance_eth: "0.482100",
      executor_buffer_status: "healthy",
      treasury_action: "hold",
      treasury_recommended_amount_eth: "0.000000",
      treasury_status: "healthy",
      treasury_note: "executor buffer aligned with target",
      private_relay_only: network === "ethereum",
      scan_interval_ms: 1500,
      wallet_count: 1,
      total_keys_read: 1,
      duplicate_keys: 0,
      invalid_keys: 0,
      last_scan_at: new Date().toISOString(),
      last_scan_duration_ms: 54,
      sweeps_attempted: 121,
      sweeps_succeeded: 39,
      sweeps_failed: 11,
      hot_wallets: [
        { address: executor, balance_eth: "0.482100", rpc: "primary-send" }
      ],
      top_residual_wallets: [],
      rpc_endpoints: [
        {
          name: "primary-send",
          kind: "send",
          avg_latency_ms: 47,
          last_block: 54201891,
          block_age_secs: 1,
          rate_limit_failures: 0,
          timeout_failures: 0,
          stale_failures: 0,
          cooldown_remaining_secs: 0
        },
        {
          name: "read-a",
          kind: "read",
          avg_latency_ms: 63,
          last_block: 54201891,
          block_age_secs: 1,
          rate_limit_failures: 1,
          timeout_failures: 0,
          stale_failures: 0,
          cooldown_remaining_secs: 0
        }
      ],
      recent_events: [],
      latency_metrics: [
        { stage: "block_fetch", samples: 240, last_ms: 41, avg_ms: 36, max_ms: 80 },
        { stage: "scan_cycle", samples: 240, last_ms: 58, avg_ms: 51, max_ms: 130 },
        { stage: "enqueue_latency", samples: 120, last_ms: 7, avg_ms: 6, max_ms: 18 },
        { stage: "queue_wait", samples: 120, last_ms: 14, avg_ms: 10, max_ms: 31 },
        { stage: "tx_prepare", samples: 39, last_ms: 48, avg_ms: 43, max_ms: 92 },
        { stage: "bundle_attempt", samples: 39, last_ms: 92, avg_ms: 84, max_ms: 180 }
      ],
      reject_reasons: [
        { stage: "fast_preflight", reason: "ev_upper_bound_below_min", count: 61 },
        { stage: "preflight", reason: "cluster_saturated", count: 19 },
        { stage: "adaptive", reason: "ev_real_below_threshold", count: 15 },
      ],
      relay_rankings: [
        {
          relay: RELAYS[0],
          score: 0.14,
          pressure: 0.21,
          accept_rate: 0.98,
          inclusion_rate: 0.81,
          accepted: 28,
          submit_failed: 1,
          included_success: 23,
          included_revert: 2,
          not_included_timeout: 3,
          submit_latency_ms: 42,
          finalization_latency_ms: 910
        },
        {
          relay: RELAYS[1],
          score: 0.29,
          pressure: 0.37,
          accept_rate: 0.84,
          inclusion_rate: 0.55,
          accepted: 14,
          submit_failed: 3,
          included_success: 8,
          included_revert: 2,
          not_included_timeout: 4,
          submit_latency_ms: 71,
          finalization_latency_ms: 1450
        }
      ],
      treasury_rebalance_trail: [
        {
          at: new Date(Date.now() - 1000 * 60 * 9).toISOString(),
          executor_address: executor,
          vault_address: vault,
          profit_address: profit,
          balance_eth: 0.4821,
          min_buffer_eth: 0.2,
          target_buffer_eth: 0.5,
          max_buffer_eth: 1.0,
          action: "hold",
          recommended_amount_eth: 0.0,
          status: "healthy",
          note: "executor buffer aligned with target"
        }
      ],
      execution_outcomes: [
        {
          at: new Date(Date.now() - 1000 * 40).toISOString(),
          relay: RELAYS[0],
          target_block: 54201892,
          pair: makeAddr(901),
          router: makeAddr(902),
          token_in: makeAddr(903),
          token_out: makeAddr(904),
          victim_tx: makeAddr(905),
          outcome: "included_success",
          expected_profit_eth: 0.0032,
          realized_profit_eth: 0.0027,
          gas_used: 201233,
          submit_latency_ms: 41,
          finalization_latency_ms: 903
        },
        {
          at: new Date(Date.now() - 1000 * 95).toISOString(),
          relay: RELAYS[1],
          target_block: 54201891,
          pair: makeAddr(906),
          router: makeAddr(907),
          token_in: makeAddr(908),
          token_out: makeAddr(909),
          victim_tx: makeAddr(910),
          outcome: "accepted_not_included",
          expected_profit_eth: 0.0024,
          realized_profit_eth: 0.0,
          gas_used: 0,
          submit_latency_ms: 73,
          finalization_latency_ms: 2010
        }
      ]
    };
  }

  async probeBackend() {
    try {
      const r = await fetch("/api/status", { cache: "no-store" });
      if (!r.ok) throw new Error("status unavailable");
      const data = await r.json();
      if (data && typeof data === "object") {
        this.useLive = true;
        this.live = data;
        return;
      }
    } catch (_) {}
    this.useLive = false;
  }

  pushEvent(level, message) {
    this.state.recent_events.unshift({
      at: new Date().toISOString(),
      level,
      message,
    });
    this.state.recent_events = this.state.recent_events.slice(0, 80);
  }

  step() {
    if (this.useLive) return;
    this.tick++;
    this.state.last_scan_at = new Date().toISOString();
    this.state.last_scan_duration_ms = randInt(40, 90);

    this.state.latency_metrics = this.state.latency_metrics.map((m) => {
      const sample = Math.max(2, m.avg_ms + randInt(-8, 12));
      return {
        ...m,
        samples: m.samples + 1,
        last_ms: sample,
        avg_ms: Math.round((m.avg_ms * 9 + sample) / 10),
        max_ms: Math.max(m.max_ms, sample),
      };
    });

    if (Math.random() < 0.35) {
      const success = Math.random() < 0.6;
      const outcome = success ? "included_success" : pick(["accepted_not_included", "included_revert"]);
      const expected = rand(0.0018, 0.0048);
      const realized = outcome === "included_success" ? expected * rand(0.65, 0.95) : 0;
      this.state.execution_outcomes.unshift({
        at: new Date().toISOString(),
        relay: pick(RELAYS),
        target_block: 54201892 + this.tick,
        pair: makeAddr(920 + this.tick),
        router: makeAddr(930 + this.tick),
        token_in: makeAddr(940 + this.tick),
        token_out: makeAddr(950 + this.tick),
        victim_tx: makeAddr(960 + this.tick),
        outcome,
        expected_profit_eth: Number(expected.toFixed(6)),
        realized_profit_eth: Number(realized.toFixed(6)),
        gas_used: outcome === "included_success" ? randInt(180000, 240000) : 0,
        submit_latency_ms: randInt(32, 90),
        finalization_latency_ms: randInt(850, 2200)
      });
      this.state.execution_outcomes = this.state.execution_outcomes.slice(0, 20);
      this.state.sweeps_attempted += 1;
      if (outcome === "included_success") this.state.sweeps_succeeded += 1;
      if (outcome === "included_revert") this.state.sweeps_failed += 1;
      this.pushEvent(outcome === "included_success" ? "success" : "warn", `execution outcome -> ${outcome}`);
    }

    if (this.tick % 8 === 0) {
      this.pushEvent("info", `runtime pulse -> regime=${this.state.market_regime} send_path=${this.state.network === "ethereum" ? "bundle-relay" : "direct-rpc"}`);
    }
  }

  snapshot() {
    if (this.useLive && this.live) return this.live;
    return this.state;
  }
}

window.__CRS_DATA__ = new DataSource();
