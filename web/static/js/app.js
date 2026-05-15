const $ = (id) => document.getElementById(id);

const VIEWS = ["command", "wallets", "execution", "relays", "treasury", "events"];
let currentView = "command";
let lastEventCount = 0;
const startedAt = Date.now();
let tick = 0;

function fmtAddr(a) {
  if (!a || a.length < 14) return a || "--";
  return a.slice(0, 8) + "…" + a.slice(-6);
}

function fmtTime(iso) {
  if (!iso) return "--";
  try { return new Date(iso).toISOString().slice(11, 19); } catch (_) { return "--"; }
}

function fmtDuration(ms) {
  const s = Math.floor(ms / 1000);
  const h = String(Math.floor(s / 3600)).padStart(2, "0");
  const m = String(Math.floor((s % 3600) / 60)).padStart(2, "0");
  const sec = String(s % 60).padStart(2, "0");
  return `${h}:${m}:${sec}`;
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function setView(v) {
  if (!VIEWS.includes(v)) v = "command";
  currentView = v;
  document.querySelectorAll(".view").forEach((el) => {
    el.classList.toggle("active", el.dataset.view === v);
  });
  document.querySelectorAll(".nav-item").forEach((el) => {
    el.classList.toggle("active", el.dataset.view === v);
  });
}

function initRouter() {
  document.querySelectorAll(".nav-item").forEach((a) => {
    a.addEventListener("click", (e) => {
      e.preventDefault();
      const v = a.dataset.view;
      window.location.hash = v;
      setView(v);
    });
  });
  window.addEventListener("hashchange", () => {
    setView((window.location.hash || "#command").slice(1));
  });
  setView((window.location.hash || "#command").slice(1));
}

function deriveTotals(s) {
  const executionOutcomes = s.execution_outcomes || [];
  const realizedPnl = executionOutcomes.reduce((sum, item) => sum + Number(item.realized_profit_eth || 0), 0);
  const rejects = s.reject_reasons || [];
  const rejectLoad = rejects.reduce((sum, item) => sum + Number(item.count || 0), 0);
  const opportunities = (s.sweeps_attempted || 0) + rejectLoad;
  const executions = executionOutcomes.length;
  const acceptance = opportunities > 0 ? (executions / opportunities) * 100 : 0;
  return { realizedPnl, rejectLoad, opportunities, executions, acceptance };
}

function buildWalletModel(s) {
  const outcomes = s.execution_outcomes || [];
  const realizedPnl = outcomes.reduce((sum, item) => sum + Number(item.realized_profit_eth || 0), 0);
  const executorBalance = Number(s.executor_balance_eth || 0);
  const treasuryAction = s.treasury_action || "hold";
  const lastOutcome = outcomes[0] || null;
  const systemWallets = [
    {
      role: "executor",
      label: "EXECUTOR",
      address: s.executor_address || "--",
      balanceEth: executorBalance,
      pnlEth: realizedPnl,
      status: s.executor_buffer_status || "unknown",
      note: "hot execution wallet"
    },
    {
      role: "profit",
      label: "PROFIT",
      address: s.profit_address || "--",
      balanceEth: realizedPnl > 0 ? realizedPnl : 0,
      pnlEth: realizedPnl,
      status: realizedPnl > 0 ? "harvesting" : "idle",
      note: "receives realized edge"
    },
    {
      role: "vault",
      label: "VAULT",
      address: s.vault_address || "--",
      balanceEth: 0,
      pnlEth: 0,
      status: treasuryAction.includes("sweep") ? "receiving" : "standby",
      note: "cold reserve / treasury sink"
    },
    {
      role: "control",
      label: "CONTROL",
      address: s.control_address || "--",
      balanceEth: 0,
      pnlEth: 0,
      status: "control",
      note: "coordination / admin path"
    }
  ];

  return {
    realizedPnl,
    systemWallets,
    lastOutcome
  };
}

function sendPath(s) {
  return s.network === "ethereum" ? "bundle-relay" : "direct-rpc";
}

function executionStatus(s) {
  if (!s.allow_send) return "BLOCKED";
  if ((s.executor_buffer_status || "").toLowerCase() === "underfunded") return "BUFFER LOW";
  if ((s.executor_buffer_status || "").toLowerCase() === "overfunded") return "TREASURY HOLD";
  return "ARMED";
}

function renderHeader(s) {
  $("meta-runtime").textContent = (s.runtime_mode || "--").toUpperCase();
  $("meta-regime").textContent = (s.market_regime || "--").toUpperCase();
  $("meta-network").textContent = (s.network || "--").toUpperCase();
  $("meta-send-path").textContent = sendPath(s).toUpperCase();
  $("meta-allow-send").textContent = s.allow_send ? "ENABLED" : "BLOCKED";
  $("meta-executor").textContent = s.executor_address || "--";
  $("meta-uplink").innerHTML = window.__CRS_DATA__.useLive
    ? '<span class="dot ok"></span> LIVE'
    : '<span class="dot warn"></span> SIM';
  $("meta-uptime").textContent = fmtDuration(Date.now() - startedAt);
  const wallets = buildWalletModel(s);
  $("side-wallets").textContent = wallets.systemWallets.length;
  $("side-pnl").textContent = wallets.realizedPnl.toFixed(6);
  $("side-path").textContent = sendPath(s).toUpperCase();
}

function renderCommand(s) {
  const totals = deriveTotals(s);
  $("stat-opportunities").textContent = totals.opportunities;
  $("stat-fast-rejects").textContent = totals.rejectLoad;
  $("stat-executions").textContent = totals.executions;
  $("stat-acceptance").textContent = `${totals.acceptance.toFixed(1)}%`;
  $("stat-realized-pnl").textContent = totals.realizedPnl.toFixed(6);
  $("stat-reject-load").textContent = totals.rejectLoad;
  $("stat-reject-types").textContent = (s.reject_reasons || []).length;
  $("stat-execution-status").textContent = executionStatus(s);
  $("stat-treasury-action").textContent = s.treasury_action || "hold";
  $("stat-send-path").textContent = sendPath(s);

  const tbody = $("outcome-body");
  const rows = s.execution_outcomes || [];
  tbody.innerHTML = rows.length
    ? rows.slice(0, 8).map((row) => `
      <tr>
        <td>${fmtTime(row.at)}</td>
        <td class="mono">${escapeHtml(row.relay)}</td>
        <td class="mono">${fmtAddr(row.pair)}</td>
        <td>${escapeHtml(row.outcome)}</td>
        <td class="num">${Number(row.expected_profit_eth || 0).toFixed(6)}</td>
        <td class="num">${Number(row.realized_profit_eth || 0).toFixed(6)}</td>
      </tr>
    `).join("")
    : '<tr><td colspan="6">No execution outcomes yet</td></tr>';

  const walletPulse = $("wallet-pulse-grid");
  const wallets = buildWalletModel(s).systemWallets;
  walletPulse.innerHTML = wallets.map((wallet) => `
    <div class="wallet-pulse-card ${escapeHtml(wallet.role)}">
      <div class="wallet-pulse-head">
        <span class="wallet-pulse-role">${escapeHtml(wallet.label)}</span>
        <span class="wallet-pulse-status">${escapeHtml(String(wallet.status).toUpperCase())}</span>
      </div>
      <div class="wallet-pulse-addr mono">${fmtAddr(wallet.address)}</div>
      <div class="wallet-pulse-metrics">
        <div><span class="metric-label">BALANCE</span><span class="metric-value-mini">${Number(wallet.balanceEth || 0).toFixed(6)} ETH</span></div>
        <div><span class="metric-label">PNL</span><span class="metric-value-mini">${Number(wallet.pnlEth || 0).toFixed(6)} ETH</span></div>
      </div>
    </div>
  `).join("");

  if (window.__CRS_RADAR__) window.__CRS_RADAR__.setStages(s.latency_metrics || []);
}

function renderWallets(s) {
  const wallets = buildWalletModel(s);
  $("wallet-executor-balance").textContent = `${Number(s.executor_balance_eth || 0).toFixed(6)} ETH`;
  $("wallet-buffer-status").textContent = (s.executor_buffer_status || "--").toUpperCase();
  $("wallet-realized-pnl").textContent = `${wallets.realizedPnl.toFixed(6)} ETH`;
  $("wallet-treasury-action").textContent = (s.treasury_action || "hold").toUpperCase();

  $("wallet-station-grid").innerHTML = wallets.systemWallets.map((wallet) => `
    <article class="wallet-station ${escapeHtml(wallet.role)}">
      <div class="wallet-station-label">${escapeHtml(wallet.label)}</div>
      <div class="wallet-station-addr mono">${escapeHtml(wallet.address)}</div>
      <div class="wallet-station-balance">${Number(wallet.balanceEth || 0).toFixed(6)} ETH</div>
      <div class="wallet-station-note">${escapeHtml(wallet.note)}</div>
      <div class="wallet-station-status">${escapeHtml(String(wallet.status).toUpperCase())}</div>
    </article>
  `).join("");

  $("wallet-profit-body").innerHTML = wallets.systemWallets.map((wallet) => `
    <tr>
      <td class="mono">${fmtAddr(wallet.address)}</td>
      <td>${escapeHtml(wallet.label)}</td>
      <td class="num">${Number(wallet.pnlEth || 0).toFixed(6)} ETH</td>
      <td>${escapeHtml(wallets.lastOutcome ? wallets.lastOutcome.outcome : "no outcome yet")}</td>
    </tr>
  `).join("");
}

function renderExecution(s) {
  $("exec-runtime").textContent = (s.runtime_mode || "--").toUpperCase();
  $("exec-regime").textContent = (s.market_regime || "--").toUpperCase();
  $("exec-send-path").textContent = sendPath(s).toUpperCase();
  $("exec-hot-wallets").textContent = (s.hot_wallets || []).length;
  $("exec-realized-rate").textContent = `${buildWalletModel(s).realizedPnl.toFixed(6)} ETH`;

  const rejectBody = $("reject-body");
  const rejects = s.reject_reasons || [];
  rejectBody.innerHTML = rejects.length
    ? rejects.map((row) => `
      <tr>
        <td>${escapeHtml(row.stage)}</td>
        <td>${escapeHtml(row.reason)}</td>
        <td class="num">${row.count}</td>
      </tr>
    `).join("")
    : '<tr><td colspan="3">No reject data yet</td></tr>';

  const walletBody = $("wallet-body");
  const hotWallets = s.hot_wallets || [];
  walletBody.innerHTML = hotWallets.length
    ? hotWallets.map((row) => `
      <tr>
        <td class="mono">${fmtAddr(row.address)}</td>
        <td class="num">${Number(row.balance_eth || 0).toFixed(6)} ETH</td>
        <td>${escapeHtml(row.rpc || "--")}</td>
      </tr>
    `).join("")
    : '<tr><td colspan="3">No hot wallet telemetry yet</td></tr>';
}

function renderRelays(s) {
  const relayBody = $("relay-body");
  const relays = s.relay_rankings || [];
  relayBody.innerHTML = relays.length
    ? relays.map((row) => `
      <tr>
        <td class="mono">${escapeHtml(row.relay)}</td>
        <td>${Number(row.score || 0).toFixed(2)}</td>
        <td>${Number(row.pressure || 0).toFixed(2)}</td>
        <td>${Number(row.accept_rate || 0).toFixed(2)}</td>
        <td>${Number(row.inclusion_rate || 0).toFixed(2)}</td>
        <td>${row.included_success || 0}</td>
        <td>${row.not_included_timeout || 0}</td>
      </tr>
    `).join("")
    : '<tr><td colspan="7">No relay ranking data yet</td></tr>';

  const rpcGrid = $("rpc-grid");
  const rpcs = s.rpc_endpoints || [];
  rpcGrid.innerHTML = rpcs.length
    ? rpcs.map((r) => {
      const health = r.cooldown_remaining_secs
        ? "cooldown"
        : (r.stale_failures > 0 || (r.block_age_secs && r.block_age_secs > 30))
        ? "degraded"
        : "healthy";
      return `
        <div class="rpc-card">
          <div class="rpc-card-head">
            <span class="rpc-tag">${escapeHtml(r.kind || "rpc")}</span>
            <span class="rpc-card-url mono">${escapeHtml(r.name || "--")}</span>
            <span class="rpc-card-status"><span class="dot ${health === "healthy" ? "ok" : health === "degraded" ? "warn" : "err"}"></span></span>
          </div>
          <div class="rpc-card-grid">
            <div><span class="kv-k">LATENCY</span><span class="kv-v">${r.avg_latency_ms || 0} ms</span></div>
            <div><span class="kv-k">BLOCK</span><span class="kv-v mono">${r.last_block || "--"}</span></div>
            <div><span class="kv-k">429</span><span class="kv-v">${r.rate_limit_failures || 0}</span></div>
            <div><span class="kv-k">TIMEOUT</span><span class="kv-v">${r.timeout_failures || 0}</span></div>
          </div>
        </div>
      `;
    }).join("")
    : '<div class="rpc-card">No RPC telemetry yet</div>';
}

function renderTreasury(s) {
  $("treasury-vault").textContent = s.vault_address || "--";
  $("treasury-executor").textContent = s.executor_address || "--";
  $("treasury-profit").textContent = s.profit_address || "--";
  $("treasury-balance").textContent = s.executor_balance_eth ? `${s.executor_balance_eth} ETH` : "--";
  $("treasury-status").textContent = (s.executor_buffer_status || "--").toUpperCase();
  $("treasury-action").textContent = (s.treasury_action || "hold").toUpperCase();

  const body = $("treasury-body");
  const rows = s.treasury_rebalance_trail || [];
  body.innerHTML = rows.length
    ? rows.map((row) => `
      <tr>
        <td>${fmtTime(row.at)}</td>
        <td>${escapeHtml(row.action)}</td>
        <td>${escapeHtml(row.status)}</td>
        <td>${Number(row.recommended_amount_eth || 0).toFixed(6)} ETH</td>
        <td>${Number(row.balance_eth || 0).toFixed(6)} ETH</td>
      </tr>
    `).join("")
    : '<tr><td colspan="5">No treasury trail yet</td></tr>';
}

function renderEvents(s) {
  const consoleEl = $("event-console");
  const events = s.recent_events || [];
  if (events.length === lastEventCount && currentView !== "events") return;
  lastEventCount = events.length;
  consoleEl.innerHTML = events.map((e) => `
    <div class="console-line">
      <span class="console-time">${fmtTime(e.at)}</span>
      <span class="console-level ${e.level}">${escapeHtml((e.level || "").toUpperCase())}</span>
      <span class="console-msg">${escapeHtml(e.message)}</span>
    </div>
  `).join("");
}

function frame() {
  const ds = window.__CRS_DATA__;
  if (!ds) {
    requestAnimationFrame(frame);
    return;
  }
  ds.step();
  const snap = ds.snapshot();

  renderHeader(snap);
  renderEvents(snap);

  if (currentView === "command") {
    renderCommand(snap);
  } else if (currentView === "wallets") {
    renderWallets(snap);
  } else if (currentView === "execution") {
    renderExecution(snap);
  } else if (currentView === "relays") {
    renderRelays(snap);
  } else if (currentView === "treasury") {
    renderTreasury(snap);
  }

  if (window.__CRS_RADAR__) window.__CRS_RADAR__.setStages(snap.latency_metrics || []);
  $("foot-tick").textContent = ++tick;
}

initRouter();
setInterval(frame, 1000);
frame();
