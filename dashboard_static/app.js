const state = {
  overview: null,
  builder: null,
  chart: null,
  liveWindow: null,
  account: null,
  overviewStreamHistory: [],
  manualContext: null,
  manualOrderResult: null,
  symbolOptions: [],
  filteredSymbolOptions: [],
  activeSymbolIndex: -1,
  symbolQuery: "",
};

const OVERVIEW_STREAM_HISTORY_LIMIT = 90;
const OVERVIEW_STREAM_FIELDS = [
  ["stream_events_total", "Total"],
  ["stream_kline_events", "Kline"],
  ["stream_mark_price_events", "Mark"],
  ["stream_book_ticker_events", "Book"],
  ["stream_user_events", "User"],
  ["queue_size", "Queue"],
  ["queue_peak", "Peak"],
  ["book_ws_overflow_count", "Book Overflow"],
  ["book_ws_restart_count", "Book Restart"],
  ["book_ws_sampled_out", "Book Sampled"],
  ["network_rx_kbps", "Net"],
  ["tracked_pairs", "Tracked"],
  ["weak_pairs", "Weak"],
  ["blocked_pairs", "Blocked"],
  ["open_positions", "Positions"],
  ["open_orders", "Orders"],
  ["account_equity", "Equity"],
  ["available_balance", "Balance"],
  ["unrealized_pnl", "PnL"],
  ["realized_pnl_today", "Realized"],
  ["user_events", "User Cnt"],
];
const OVERVIEW_STREAM_COLORS = [
  "#59d1b8",
  "#f1b24a",
  "#8fc2ff",
  "#ef6f6c",
  "#74d49b",
  "#ecf8f5",
  "#95b3ae",
  "#ffd88a",
  "#86f0cf",
  "#ffb0ad",
  "#b0d0ff",
  "#d4f0ff",
  "#c4f5dd",
  "#ffcf7a",
  "#7be0ca",
  "#ff8f8c",
  "#f8e19a",
  "#b7c7c3",
];

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to load ${url}`);
  }
  return response.json();
}

async function postJson(url, payload) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  const text = await response.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = { detail: text };
  }

  if (!response.ok) {
    throw new Error(data?.detail || data?.message || `Request failed: ${response.status}`);
  }

  return data;
}

function setText(id, value) {
  const node = document.getElementById(id);
  if (node) {
    node.textContent = value;
  }
}

function renderBookWsMetrics(overview) {
  const bookWsQueueLimit = Number(overview?.book_ws_queue_limit);
  const bookWsOverflowCount = Number(overview?.book_ws_overflow_count);
  const bookWsRestartCount = Number(overview?.book_ws_restart_count);
  const bookWsSampledOut = Number(overview?.book_ws_sampled_out);

  setText("book-ws-queue-limit", Number.isFinite(bookWsQueueLimit) ? bookWsQueueLimit : 0);
  setText("book-ws-overflow-count", Number.isFinite(bookWsOverflowCount) ? bookWsOverflowCount : 0);
  setText("book-ws-restart-count", Number.isFinite(bookWsRestartCount) ? bookWsRestartCount : 0);
  setText("book-ws-sampled-out", Number.isFinite(bookWsSampledOut) ? bookWsSampledOut : 0);
}

function setStatusTone(id, tone) {
  const node = document.getElementById(id);
  if (!node) {
    return;
  }
  node.classList.remove("tone-ready", "tone-waiting", "tone-idle");
  node.classList.add(`tone-${tone}`);
}

function formatAgo(timestampMs) {
  if (!timestampMs) {
    return "no data";
  }

  const diffMs = Date.now() - Number(timestampMs);
  if (!Number.isFinite(diffMs)) {
    return "no data";
  }

  const seconds = Math.max(0, Math.round(diffMs / 1000));
  if (seconds < 60) {
    return `${seconds}s ago`;
  }

  const minutes = Math.floor(seconds / 60);
  const restSeconds = seconds % 60;
  return `${minutes}m ${restSeconds}s ago`;
}

function statusText(value, fallback = "waiting") {
  return String(value || fallback).replaceAll("_", " ");
}

function getSelectedSymbol() {
  return (state.chart?.symbol || document.getElementById("symbol-input")?.value || "BTCUSDT")
    .trim()
    .toUpperCase();
}

function getQuoteAssetBadge(symbol) {
  const normalized = String(symbol || "").toUpperCase();
  if (normalized.endsWith("USDC")) {
    return "USDC";
  }
  if (normalized.endsWith("USDT")) {
    return "USDT";
  }
  return "";
}

function renderOverview(overview) {
  setText("tracked-pairs", overview.tracked_pairs);
  setText("queue-size", overview.queue_size);
  setText("queue-peak", overview.queue_peak ?? 0);
  renderBookWsMetrics(overview);
  setText("network-rx-kbps", overview.network_rx_kbps ?? 0);
  setText("open-positions", overview.open_positions);
  setText("open-orders", overview.open_orders);
  setText("bot-status-pill", overview.bot_status);
  const startButton = document.getElementById("bot-start");
  const stopButton = document.getElementById("bot-stop");
  const isRunning = overview.bot_status === "running" || overview.bot_status === "starting" || overview.bot_status === "degraded";
  startButton.disabled = isRunning;
  stopButton.disabled = !isRunning;
}

function addOverviewStreamSample(overview) {
  const sample = { received_at: Date.now() };
  for (const [field] of OVERVIEW_STREAM_FIELDS) {
    const numeric = Number(overview?.[field]);
    sample[field] = Number.isFinite(numeric) ? numeric : 0;
  }
  state.overviewStreamHistory.push(sample);
  if (state.overviewStreamHistory.length > OVERVIEW_STREAM_HISTORY_LIMIT) {
    state.overviewStreamHistory.splice(0, state.overviewStreamHistory.length - OVERVIEW_STREAM_HISTORY_LIMIT);
  }
}

function formatStreamValue(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "-";
  }
  if (Math.abs(numeric) >= 1000) {
    return numeric.toLocaleString(undefined, { maximumFractionDigits: 0 });
  }
  if (Math.abs(numeric) >= 10) {
    return numeric.toLocaleString(undefined, { maximumFractionDigits: 1 });
  }
  return numeric.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function renderOverviewStreamChart() {
  const svg = document.getElementById("overview-stream-chart");
  const legend = document.getElementById("overview-stream-legend");
  if (!svg || !legend) {
    return;
  }

  svg.innerHTML = "";
  legend.innerHTML = "";
  const samples = state.overviewStreamHistory;
  setText("stream-samples-count", String(samples.length));

  const width = 280;
  const height = 150;
  const left = 8;
  const right = 8;
  const top = 10;
  const bottom = 16;
  const plotWidth = width - left - right;
  const plotHeight = height - top - bottom;

  for (let i = 0; i < 4; i += 1) {
    const y = top + (plotHeight / 3) * i;
    const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("x1", String(left));
    line.setAttribute("x2", String(width - right));
    line.setAttribute("y1", String(y));
    line.setAttribute("y2", String(y));
    line.setAttribute("stroke", "rgba(255,255,255,0.08)");
    line.setAttribute("stroke-width", "1");
    svg.appendChild(line);
  }

  if (samples.length < 2) {
    const message = document.createElementNS("http://www.w3.org/2000/svg", "text");
    message.setAttribute("x", String(width / 2));
    message.setAttribute("y", String(height / 2));
    message.setAttribute("text-anchor", "middle");
    message.setAttribute("fill", "rgba(236,248,245,0.68)");
    message.setAttribute("font-size", "12");
    message.textContent = "waiting for stream samples";
    svg.appendChild(message);
  }

  OVERVIEW_STREAM_FIELDS.forEach(([field, label], fieldIndex) => {
    const values = samples.map(sample => Number(sample[field] || 0));
    const minValue = Math.min(...values);
    const maxValue = Math.max(...values);
    const valueRange = Math.max(1e-9, maxValue - minValue);
    const points = values.map((value, index) => {
      const x = left + (plotWidth * index) / Math.max(1, samples.length - 1);
      const normalized = (value - minValue) / valueRange;
      const y = top + plotHeight - normalized * plotHeight;
      return { x, y };
    });

    if (points.length > 1 && values.some((value, index) => index > 0 && value !== values[index - 1])) {
      const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
      path.setAttribute("d", toPath(points));
      path.setAttribute("fill", "none");
      path.setAttribute("stroke", OVERVIEW_STREAM_COLORS[fieldIndex % OVERVIEW_STREAM_COLORS.length]);
      path.setAttribute("stroke-width", fieldIndex < 5 ? "2" : "1.2");
      path.setAttribute("stroke-opacity", fieldIndex < 8 ? "0.92" : "0.64");
      svg.appendChild(path);
    }

    const latestValue = values.length ? values[values.length - 1] : 0;
    const item = document.createElement("span");
    item.className = "stream-legend-item";
    item.innerHTML = `
      <i style="background:${OVERVIEW_STREAM_COLORS[fieldIndex % OVERVIEW_STREAM_COLORS.length]}"></i>
      ${label}: ${formatStreamValue(latestValue)}
    `;
    legend.appendChild(item);
  });
}

function renderCandleLoader(loader) {
  if (!loader) {
    return;
  }

  setText("loader-status-pill", loader.status || "idle");
  setText("loader-progress", `${loader.completed_symbols || 0} / ${loader.total_symbols || 0}`);
  setText("loader-current", loader.current_symbol || "-");
  setText("loader-last", loader.last_completed_symbol || "-");

  const startDelayInput = document.getElementById("loader-start-delay");
  if (document.activeElement !== startDelayInput) {
    startDelayInput.value = loader.start_delay_seconds ?? 0.7;
  }
}

function renderStrategies(builder) {
  const list = document.getElementById("strategy-list");
  list.innerHTML = "";
  setText("strategy-count", builder.strategies.length);

  for (const strategy of builder.strategies) {
    const item = document.createElement("article");
    item.className = `strategy-item${strategy.id === builder.strategy.id ? " active" : ""}`;
    item.innerHTML = `
      <header>
        <strong>${strategy.name}</strong>
        <span class="tag">${strategy.status}</span>
      </header>
      <div class="muted">${strategy.id}</div>
    `;
    list.appendChild(item);
  }
}

function createRuleNode(node) {
  if (node.type === "group") {
    const container = document.createElement("div");
    container.className = "group-card";
    container.innerHTML = `<div class="group-title">${node.operator}</div>`;
    const childrenWrap = document.createElement("div");
    childrenWrap.className = "rule-children";
    for (const child of node.children) {
      childrenWrap.appendChild(createRuleNode(child));
    }
    container.appendChild(childrenWrap);
    return container;
  }

  const condition = document.createElement("div");
  condition.className = "rule-card";
  condition.innerHTML = `
    <strong>${node.label}</strong>
    <div class="muted">${node.note}</div>
    <div class="condition-meta">
      <span>TF: ${node.timeframe}</span>
      <span>Op: ${node.operator}</span>
      <span>Value: ${node.value}</span>
    </div>
  `;
  return condition;
}

function renderBuilder(builder) {
  renderStrategies(builder);
  populateSymbolSelect(builder);
  renderCandleLoader(builder.candle_loader);

  const tree = document.getElementById("rule-tree");
  tree.innerHTML = "";
  tree.appendChild(createRuleNode(builder.rule_tree));

  const action = builder.action;
  document.getElementById("action-summary").innerHTML = `
    <strong>${action.type}</strong>
    <div class="muted">Order: ${action.order_type}</div>
    <div class="muted">Size mode: ${action.size_mode}</div>
    <div class="muted">Risk: ${action.risk_value}%</div>
    <div class="muted">Stop: ${action.stop_loss}</div>
    <div class="muted">Target: ${action.take_profit}</div>
  `;

  const catalogGroups = document.getElementById("catalog-groups");
  catalogGroups.innerHTML = "";
  for (const group of builder.catalog) {
    const section = document.createElement("section");
    section.className = "catalog-group";
    section.innerHTML = `<header><strong>${group.group}</strong></header>`;
    const items = document.createElement("div");
    items.className = "catalog-items";
    for (const item of group.items) {
      const badge = document.createElement("span");
      badge.className = "catalog-item";
      badge.textContent = item;
      items.appendChild(badge);
    }
    section.appendChild(items);
    catalogGroups.appendChild(section);
  }

  const evaluation = document.getElementById("evaluation-list");
  evaluation.innerHTML = "";
  for (const row of builder.evaluation) {
    const item = document.createElement("article");
    item.className = `evaluation-item ${row.result ? "good" : "bad"}`;
    item.innerHTML = `
      <header>
        <strong>${row.label}</strong>
        <span class="${row.result ? "result-good" : "result-bad"}">${row.result ? "true" : "false"}</span>
      </header>
      <div class="muted">Value: ${row.value}</div>
    `;
    evaluation.appendChild(item);
  }

  const recentEvents = document.getElementById("recent-user-events");
  recentEvents.innerHTML = "";
  for (const event of builder.recent_user_events.slice(0, 8)) {
    const item = document.createElement("article");
    item.className = "evaluation-item";
    item.innerHTML = `
      <header>
        <strong>${event.event_type}</strong>
        <span class="tag">${event.symbol || event.reason || "account"}</span>
      </header>
      <div class="muted">${event.side || event.execution_type || "update"}</div>
    `;
    recentEvents.appendChild(item);
  }
}

async function refreshBuilderState() {
  const builder = await fetchJson("/api/strategy/builder");
  state.builder = builder;
  renderBuilder(builder);
  renderLiveStatus();
  renderManualOrderPanel();
}

function getSelectedSymbolState() {
  const symbol = getSelectedSymbol();
  const tracked = state.builder?.tracked_symbol_states || [];
  const pending = state.builder?.pending_symbol_states || [];
  return tracked.find(item => item.symbol === symbol)
    || pending.find(item => item.symbol === symbol)
    || { symbol };
}

function renderLiveStatus() {
  const symbolState = getSelectedSymbolState();
  const liveWindow = state.liveWindow || { points: 0, data: [], symbol_state: symbolState };
  const historyReady = Boolean(symbolState.history_ready);
  const streamConfirmed = Boolean(symbolState.stream_confirmed);
  const lastPoint = liveWindow.data?.length ? liveWindow.data[liveWindow.data.length - 1] : null;

  setText("live-history-status", historyReady ? "ready" : statusText(symbolState.history_status, "pending"));
  setStatusTone(
    "live-history-status",
    historyReady ? "ready" : (symbolState.history_status === "awaiting_history" ? "waiting" : "idle")
  );
  setText(
    "live-history-meta",
    historyReady
      ? `lag ${symbolState.history_lag_candles ?? 0} candles`
      : statusText(symbolState.history_status, "startup check")
  );

  setText("live-stream-status", streamConfirmed ? "live" : "waiting");
  setStatusTone("live-stream-status", streamConfirmed ? "ready" : "idle");
  setText(
    "live-stream-meta",
    streamConfirmed
      ? `${symbolState.last_interval || "1m"} | ${formatAgo(symbolState.last_stream_event_time)} | ` +
        `60s k:${symbolState.stream_kline_events_60s ?? 0} m:${symbolState.stream_mark_price_events_60s ?? 0} b:${symbolState.stream_book_ticker_events_60s ?? 0}`
      : "no confirmed kline"
  );

  setText("live-window-points", String(liveWindow.points || 0));
  setStatusTone("live-window-points", (liveWindow.points || 0) > 0 ? "ready" : "idle");
  setText(
    "live-window-meta",
    lastPoint
      ? `${Number(lastPoint.close).toFixed(6)} | ${formatAgo(lastPoint.event_time)}`
      : "no live prices yet"
  );

  setText("live-db-sync-status", statusText(symbolState.db_sync_status, "waiting"));
  setStatusTone(
    "live-db-sync-status",
    ["saved", "up_to_date"].includes(symbolState.db_sync_status) ? "ready" :
      (["waiting_closed_candle", "waiting"].includes(symbolState.db_sync_status) ? "waiting" : "idle")
  );
  setText(
    "live-db-sync-meta",
    symbolState.db_sync_last_open_time
      ? `last 1m ${formatAgo(Number(symbolState.db_sync_last_open_time) + 60000)}`
      : "waiting for first save"
  );
}

async function refreshLiveWindow(symbol = null) {
  const resolvedSymbol = (symbol || state.chart?.symbol || document.getElementById("symbol-input")?.value || "BTCUSDT")
    .trim()
    .toUpperCase();
  const liveWindow = await fetchJson(`/api/market/live-window?symbol=${resolvedSymbol}`);
  state.liveWindow = liveWindow;
  renderLiveStatus();
  renderManualOrderPanel();
}

function populateSymbolSelect(builder) {
  const trackedSymbols = (builder.tracked_symbol_states?.length
    ? builder.tracked_symbol_states
    : (builder.tracked_symbols || []).map(symbol => ({ symbol }))
  ).map(item => ({
    ...item,
    symbol: item.symbol,
    kind: "tracked",
  }));
  const pendingSymbols = (builder.pending_symbol_states?.length
    ? builder.pending_symbol_states
    : (builder.pending_symbols || []).map(symbol => ({ symbol }))
  ).map(item => ({
    ...item,
    symbol: item.symbol,
    kind: "pending",
  }));
  state.symbolOptions = trackedSymbols.length ? trackedSymbols : [{ symbol: "BTCUSDT", kind: "tracked" }];

  for (const pending of pendingSymbols) {
    if (!state.symbolOptions.some(option => option.symbol === pending.symbol)) {
      state.symbolOptions.push(pending);
    }
  }

  const btcExists = state.symbolOptions.some(option => option.symbol === "BTCUSDT");
  const selected = state.chart?.symbol || (btcExists ? "BTCUSDT" : (builder.strategy.selected_symbol || state.symbolOptions[0].symbol));
  const input = document.getElementById("symbol-input");
  const combobox = document.getElementById("symbol-combobox");
  const isEditingSymbol = document.activeElement === input || combobox?.classList.contains("open");
  if (!isEditingSymbol) {
    input.value = selected;
  }
  const orderSymbolInput = document.getElementById("order-symbol");
  if (orderSymbolInput && !orderSymbolInput.value) {
    orderSymbolInput.value = selected;
  }
  filterSymbolOptions(isEditingSymbol ? input.value : "");
  if (!isEditingSymbol) {
    const selectedIndex = state.filteredSymbolOptions.findIndex(option => option.symbol === selected);
    state.activeSymbolIndex = selectedIndex >= 0 ? selectedIndex : (state.filteredSymbolOptions.length ? 0 : -1);
  }
  renderSymbolDropdown(false);
}

function getSymbolAvailability(option) {
  if (option.kind !== "tracked") {
    return {
      dotClass: "idle",
      statusLabel: "pending status",
      detailLabel: "exchange not in TRADING yet",
    };
  }

  if (option.is_blocked) {
    return {
      dotClass: "idle",
      statusLabel: "blocked",
      detailLabel: "stream blocked after repeated failures",
    };
  }

  if (option.is_weak) {
    return {
      dotClass: "idle",
      statusLabel: "weak",
      detailLabel: "running in isolated weak mode",
    };
  }

  if (!option.history_ready) {
    const lag = Number(option.history_lag_candles || 0);
    const historyStatus = statusText(option.history_status, "pending");
    return {
      dotClass: option.history_status === "awaiting_history" ? "waiting" : "idle",
      statusLabel: historyStatus,
      detailLabel: lag > 0 ? `lag ${lag} candles` : "history not ready yet",
    };
  }

  if (!option.stream_confirmed) {
    return {
      dotClass: "idle",
      statusLabel: "waiting stream",
      detailLabel: "history ready, first live kline not confirmed yet",
    };
  }

  return {
    dotClass: "ready",
    statusLabel: "live",
    detailLabel: option.last_interval || "stream confirmed",
  };
}

function filterSymbolOptions(query = "") {
  const normalized = query.trim().toUpperCase();
  state.symbolQuery = normalized;
  state.filteredSymbolOptions = [...state.symbolOptions];

  if (!normalized) {
    state.activeSymbolIndex = state.filteredSymbolOptions.length ? 0 : -1;
    return;
  }

  const exactIndex = state.filteredSymbolOptions.findIndex(option => option.symbol === normalized);
  if (exactIndex >= 0) {
    state.activeSymbolIndex = exactIndex;
    return;
  }

  const partialIndex = state.filteredSymbolOptions.findIndex(option => option.symbol.includes(normalized));
  state.activeSymbolIndex = partialIndex >= 0 ? partialIndex : (state.filteredSymbolOptions.length ? 0 : -1);
}

function renderSymbolDropdown(scrollToActive = false) {
  const dropdown = document.getElementById("symbol-dropdown");
  dropdown.innerHTML = "";

  if (!state.filteredSymbolOptions.length) {
    const empty = document.createElement("div");
    empty.className = "combobox-empty";
    empty.textContent = "No symbols found";
    dropdown.appendChild(empty);
    return;
  }

  state.filteredSymbolOptions.forEach((option, index) => {
    const item = document.createElement("div");
    const isMatch = state.symbolQuery && option.symbol.includes(state.symbolQuery);
    const availability = getSymbolAvailability(option);
    item.className = `combobox-option${index === state.activeSymbolIndex ? " active" : ""}${isMatch ? " match" : ""}`;
    item.dataset.index = String(index);
    item.title = `${option.symbol}: ${availability.statusLabel}${availability.detailLabel ? ` (${availability.detailLabel})` : ""}`;
    const labelHtml = formatSymbolWithBadge(option.symbol, state.symbolQuery);
    item.innerHTML = `
      <div class="option-main">
        <span class="status-dot ${availability.dotClass}"></span>
        <div class="option-copy">
          <strong class="symbol-label">${labelHtml}</strong>
          <small class="option-status">${availability.statusLabel}${availability.detailLabel ? ` · ${availability.detailLabel}` : ""}</small>
        </div>
      </div>
      <span class="kind">${option.kind}</span>
    `;
    item.addEventListener("mousedown", (event) => {
      event.preventDefault();
      selectSymbol(option.symbol);
    });
    dropdown.appendChild(item);
  });

  if (scrollToActive) {
    requestAnimationFrame(() => {
      const activeItem = dropdown.querySelector(".combobox-option.active");
      if (activeItem) {
        activeItem.scrollIntoView({ block: "center" });
      }
    });
  }
}

function highlightSymbolMatch(symbol, query) {
  if (!query) {
    return symbol;
  }

  const index = symbol.indexOf(query);
  if (index === -1) {
    return symbol;
  }

  const before = symbol.slice(0, index);
  const matched = symbol.slice(index, index + query.length);
  const after = symbol.slice(index + query.length);
  return `${before}<mark>${matched}</mark>${after}`;
}

function formatSymbolWithBadge(symbol, query = "") {
  const quoteAsset = getQuoteAssetBadge(symbol);
  const labelHtml = highlightSymbolMatch(symbol, query);
  if (!quoteAsset) {
    return labelHtml;
  }
  return `${labelHtml} <span class="quote-badge quote-${quoteAsset.toLowerCase()}">${quoteAsset}</span>`;
}

async function selectSymbol(symbol) {
  const input = document.getElementById("symbol-input");
  input.value = symbol;
  const orderSymbolInput = document.getElementById("order-symbol");
  if (orderSymbolInput) {
    orderSymbolInput.value = symbol;
  }
  document.getElementById("symbol-combobox").classList.remove("open");
  const timeframe = document.getElementById("timeframe-select").value;
  const chart = await fetchJson(`/api/market/chart?symbol=${symbol}&timeframe=${timeframe}&bars=120`);
  state.chart = chart;
  renderChart(chart);
  await refreshLiveWindow(symbol);
  await refreshManualOrderContext(symbol);
}

function toPath(points) {
  return points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");
}

function formatPrice(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "-";
  }
  const absValue = Math.abs(numeric);
  let digits = 6;
  if (absValue >= 1000) {
    digits = 2;
  } else if (absValue >= 100) {
    digits = 3;
  } else if (absValue >= 1) {
    digits = 4;
  }
  return numeric.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: digits,
  });
}

function formatBalance(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "-";
  }
  return numeric.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function formatPercent(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "-";
  }
  const sign = numeric > 0 ? "+" : "";
  return `${sign}${numeric.toFixed(2)}%`;
}

function formatClock(isoTime) {
  if (!isoTime) {
    return "-";
  }
  const date = new Date(isoTime);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }
  return date.toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatAxisTime(isoTime, timeframe) {
  if (!isoTime) {
    return "";
  }
  const date = new Date(isoTime);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  if (timeframe === "15m") {
    return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  }
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function setValueTone(id, tone) {
  const node = document.getElementById(id);
  if (!node) {
    return;
  }
  node.classList.remove("positive", "negative", "neutral");
  node.classList.add(tone);
}

function setManualOrderMessage(message, tone = "neutral") {
  const node = document.getElementById("manual-order-message");
  if (!node) {
    return;
  }
  node.textContent = message;
  node.classList.remove("ok", "error");
  if (tone === "ok") {
    node.classList.add("ok");
  } else if (tone === "error") {
    node.classList.add("error");
  }
}

function renderManualOrderResult(result) {
  const output = document.getElementById("manual-order-result");
  if (!output) {
    return;
  }
  output.textContent = result
    ? JSON.stringify(result, null, 2)
    : "Awaiting order response…";
}

function formatCommissionRate(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "-";
  }
  return `${(numeric * 100).toFixed(4)}%`;
}

function describeFeeLevel(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "fee data unavailable";
  }
  if (numeric >= 0.001) {
    return "high fee: prefer maker entries and avoid frequent flips";
  }
  if (numeric >= 0.0005) {
    return "elevated fee: taker entries need cleaner setups";
  }
  return "normal fee: execution cost is moderate";
}

function renderManualOrderPanel() {
  const context = state.manualContext || {};
  const position = context.position || null;
  const usdtBalance = context.usdt_balance || null;
  const positionSideInput = document.getElementById("order-position-side");
  const quantityInput = document.getElementById("order-quantity");
  const priceInput = document.getElementById("order-price");
  const stopPriceInput = document.getElementById("order-stop-price");
  const orderTypeNode = document.getElementById("order-type");
  const orderType = orderTypeNode?.value || "MARKET";
  const quantity = Number(quantityInput?.value || 0);
  const price = Number(priceInput?.value || 0);
  const stopPrice = Number(stopPriceInput?.value || 0);
  const markPrice = Number(context.mark_price || state.liveWindow?.data?.slice(-1)[0]?.close || 0);

  const orderSymbol = document.getElementById("order-symbol");
  if (orderSymbol && !orderSymbol.value) {
    orderSymbol.value = context.symbol || getSelectedSymbol();
  }

  setText("manual-mark-price", formatPrice(markPrice));
  setText("manual-position-mode", context.mode_label || "-");
  setText("manual-maker-fee", formatCommissionRate(context.maker_commission_rate));
  setText("manual-taker-fee", formatCommissionRate(context.taker_commission_rate));
  setText("manual-fee-note", describeFeeLevel(context.taker_commission_rate));
  setText("manual-qty-step", context.quantity_step_size || "-");
  setText("manual-price-tick", context.price_tick_size || "-");
  setText("manual-min-qty", context.min_qty || "-");
  setText("manual-min-notional", context.min_notional || "-");
  setText("manual-max-leverage", context.max_leverage || "-");
  setText("manual-trigger-protect", context.trigger_protect || "-");
  setText(
    "manual-balance",
    usdtBalance ? `${formatBalance(usdtBalance.cw || usdtBalance.wb)} USDT` : "-"
  );
  setText(
    "manual-position-size",
    position ? `${position.pa || "0"} @ ${position.ep || "-"}` : "flat"
  );

  if (quantityInput && context.quantity_step_size) {
    quantityInput.step = context.quantity_step_size;
    quantityInput.placeholder = context.min_qty || "0.001";
  }

  const allowedSides = context.allowed_position_sides || ["BOTH"];
  if (positionSideInput) {
    const currentValue = positionSideInput.value;
    positionSideInput.innerHTML = "";
    for (const side of allowedSides) {
      const option = document.createElement("option");
      option.value = side;
      option.textContent = side;
      positionSideInput.appendChild(option);
    }
    if (allowedSides.includes(currentValue)) {
      positionSideInput.value = currentValue;
    } else {
      positionSideInput.value = allowedSides[0];
    }
    positionSideInput.disabled = allowedSides.length === 1;
  }
  if (priceInput && context.price_tick_size) {
    priceInput.step = context.price_tick_size;
  }
  if (stopPriceInput && context.price_tick_size) {
    stopPriceInput.step = context.price_tick_size;
  }

  const priceWrap = document.getElementById("order-price-wrap");
  const stopPriceWrap = document.getElementById("order-stop-price-wrap");
  const postOnlyInput = document.getElementById("order-post-only");
  const reduceOnlyInput = document.getElementById("order-reduce-only");
  if (priceWrap) {
    priceWrap.style.display = orderType === "LIMIT" ? "grid" : "none";
  }
  if (stopPriceWrap) {
    stopPriceWrap.style.display = ["STOP_MARKET", "TAKE_PROFIT_MARKET"].includes(orderType) ? "grid" : "none";
  }
  if (postOnlyInput) {
    postOnlyInput.disabled = orderType !== "LIMIT";
    if (orderType !== "LIMIT") {
      postOnlyInput.checked = false;
    }
  }
  if (reduceOnlyInput) {
    const reduceOnlyAllowed = !context.hedge_mode;
    reduceOnlyInput.disabled = !reduceOnlyAllowed;
    if (!reduceOnlyAllowed) {
      reduceOnlyInput.checked = false;
    }
    const switchNote = document.getElementById("order-switch-note");
    if (switchNote) {
      switchNote.textContent = reduceOnlyAllowed
        ? "Execution flags adapt to the current Binance position mode."
        : "Reduce only is disabled in Hedge Mode because Binance rejects this flag there.";
    }
  }

  let referencePrice = markPrice;
  if (orderType === "LIMIT" && price > 0) {
    referencePrice = price;
  }
  if (["STOP_MARKET", "TAKE_PROFIT_MARKET"].includes(orderType) && stopPrice > 0) {
    referencePrice = stopPrice;
  }

  const estimatedNotional = quantity > 0 && referencePrice > 0 ? quantity * referencePrice : null;
  setText("manual-est-notional", estimatedNotional ? `${formatBalance(estimatedNotional)} USDT` : "-");
}

async function refreshAccountState() {
  const account = await fetchJson("/api/account/state");
  state.account = account;
}

async function refreshManualOrderContext(symbol = null) {
  const selectedSymbol = (symbol || getSelectedSymbol()).toUpperCase();
  const context = await fetchJson(`/api/trading/manual/context?symbol=${selectedSymbol}`);
  state.manualContext = context;
  renderManualOrderPanel();
}

function renderChart(chart) {
  const chartTitle = document.getElementById("chart-title");
  if (chartTitle) {
    const quoteAsset = getQuoteAssetBadge(chart.symbol);
    chartTitle.innerHTML = quoteAsset
      ? `${chart.symbol} <span class="quote-badge quote-${quoteAsset.toLowerCase()}">${quoteAsset}</span> <span class="chart-title-separator">|</span> ${chart.timeframe}`
      : `${chart.symbol} <span class="chart-title-separator">|</span> ${chart.timeframe}`;
  }
  const priceSvg = document.getElementById("price-chart");
  const indicatorSvg = document.getElementById("indicator-chart");
  priceSvg.innerHTML = "";
  indicatorSvg.innerHTML = "";

  const width = 1200;
  const height = 420;
  const leftPadding = 18;
  const rightPadding = 86;
  const topPadding = 18;
  const bottomPadding = 36;
  const candles = chart.candles || [];

  if (!candles.length) {
    const message = document.createElementNS("http://www.w3.org/2000/svg", "text");
    message.setAttribute("x", String(width / 2));
    message.setAttribute("y", String(height / 2));
    message.setAttribute("text-anchor", "middle");
    message.setAttribute("fill", "rgba(255,255,255,0.7)");
    message.setAttribute("font-size", "20");
    message.textContent = "No real candles yet";
    priceSvg.appendChild(message);
    setText("chart-last-price", "-");
    setText("chart-price-change", "-");
    setText("chart-open-price", "-");
    setText("chart-high-price", "-");
    setText("chart-low-price", "-");
    setText("chart-updated-at", "-");
    return;
  }

  const plotWidth = width - leftPadding - rightPadding;
  const plotHeight = height - topPadding - bottomPadding;
  const highs = candles.map(candle => candle.high);
  const lows = candles.map(candle => candle.low);
  const rawMinPrice = Math.min(...lows);
  const rawMaxPrice = Math.max(...highs);
  const priceRange = Math.max(1e-9, rawMaxPrice - rawMinPrice);
  const minPrice = rawMinPrice - priceRange * 0.04;
  const maxPrice = rawMaxPrice + priceRange * 0.04;
  const xStep = plotWidth / Math.max(1, candles.length - 1);
  const candleWidth = Math.max(3, xStep * 0.55);
  const mapPrice = (value) => {
    const ratio = (value - minPrice) / Math.max(1e-9, maxPrice - minPrice);
    return topPadding + plotHeight - ratio * plotHeight;
  };

  const priceTicks = 6;
  for (let i = 0; i < priceTicks; i += 1) {
    const ratio = i / (priceTicks - 1);
    const y = topPadding + ratio * plotHeight;
    const priceValue = maxPrice - (maxPrice - minPrice) * ratio;

    const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("x1", String(leftPadding));
    line.setAttribute("x2", String(width - rightPadding));
    line.setAttribute("y1", String(y));
    line.setAttribute("y2", String(y));
    line.setAttribute("stroke", "rgba(255,255,255,0.08)");
    line.setAttribute("stroke-width", "1");
    priceSvg.appendChild(line);

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", String(width - rightPadding + 10));
    label.setAttribute("y", String(y + 4));
    label.setAttribute("fill", "rgba(236,248,245,0.72)");
    label.setAttribute("font-size", "12");
    label.textContent = formatPrice(priceValue);
    priceSvg.appendChild(label);
  }

  const timeTicks = Math.min(6, candles.length);
  for (let i = 0; i < timeTicks; i += 1) {
    const candleIndex = Math.min(
      candles.length - 1,
      Math.round((i / Math.max(1, timeTicks - 1)) * (candles.length - 1))
    );
    const x = leftPadding + candleIndex * xStep;
    const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("x1", String(x));
    line.setAttribute("x2", String(x));
    line.setAttribute("y1", String(topPadding));
    line.setAttribute("y2", String(topPadding + plotHeight));
    line.setAttribute("stroke", "rgba(255,255,255,0.035)");
    line.setAttribute("stroke-width", "1");
    priceSvg.appendChild(line);

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", String(x));
    label.setAttribute("y", String(height - 10));
    label.setAttribute("text-anchor", i === 0 ? "start" : (i === timeTicks - 1 ? "end" : "middle"));
    label.setAttribute("fill", "rgba(149,179,174,0.86)");
    label.setAttribute("font-size", "12");
    label.textContent = formatAxisTime(candles[candleIndex].time, chart.timeframe);
    priceSvg.appendChild(label);
  }

  candles.forEach((candle, index) => {
    const x = leftPadding + index * xStep;

    const highY = mapPrice(candle.high);
    const lowY = mapPrice(candle.low);
    const openY = mapPrice(candle.open);
    const closeY = mapPrice(candle.close);
    const isUp = candle.close >= candle.open;

    const wick = document.createElementNS("http://www.w3.org/2000/svg", "line");
    wick.setAttribute("x1", String(x));
    wick.setAttribute("x2", String(x));
    wick.setAttribute("y1", String(highY));
    wick.setAttribute("y2", String(lowY));
    wick.setAttribute("stroke", isUp ? "#74d49b" : "#ef6f6c");
    wick.setAttribute("stroke-width", "1.5");
    priceSvg.appendChild(wick);

    const body = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    body.setAttribute("x", String(x - candleWidth / 2));
    body.setAttribute("y", String(Math.min(openY, closeY)));
    body.setAttribute("width", String(candleWidth));
    body.setAttribute("height", String(Math.max(2, Math.abs(closeY - openY))));
    body.setAttribute("rx", "3");
    body.setAttribute("fill", isUp ? "#74d49b" : "#ef6f6c");
    priceSvg.appendChild(body);
  });

  const smaSeries = chart.series.sma_30 || [];
  const smaPoints = smaSeries.map((point) => {
    const candleIndex = candles.findIndex(candle => candle.time === point.time);
    if (candleIndex === -1) {
      return null;
    }
    const x = leftPadding + candleIndex * xStep;
    const y = mapPrice(point.value);
    return { x, y };
  }).filter(Boolean);

  if (smaPoints.length > 1) {
    const smaPath = document.createElementNS("http://www.w3.org/2000/svg", "path");
    smaPath.setAttribute("d", toPath(smaPoints));
    smaPath.setAttribute("fill", "none");
    smaPath.setAttribute("stroke", "#f1b24a");
    smaPath.setAttribute("stroke-width", "2.2");
    priceSvg.appendChild(smaPath);
  }

  for (const signal of (chart.signals || [])) {
    const candleIndex = candles.findIndex(candle => candle.time === signal.time);
    if (candleIndex === -1) {
      continue;
    }
    const x = leftPadding + candleIndex * xStep;
    const y = mapPrice(signal.price);
    const marker = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    marker.setAttribute("cx", String(x));
    marker.setAttribute("cy", String(y));
    marker.setAttribute("r", "5");
    marker.setAttribute("fill", signal.type === "entry" ? "#59d1b8" : "#f1b24a");
    priceSvg.appendChild(marker);
  }

  const lastCandle = candles[candles.length - 1];
  const previousCandle = candles[candles.length - 2] || lastCandle;
  const currentPrice = Number(lastCandle.close);
  const previousPrice = Number(previousCandle.close);
  const changePercent = previousPrice ? ((currentPrice - previousPrice) / previousPrice) * 100 : 0;
  const currentY = mapPrice(currentPrice);
  const priceLine = document.createElementNS("http://www.w3.org/2000/svg", "line");
  priceLine.setAttribute("x1", String(leftPadding));
  priceLine.setAttribute("x2", String(width - rightPadding));
  priceLine.setAttribute("y1", String(currentY));
  priceLine.setAttribute("y2", String(currentY));
  priceLine.setAttribute("stroke", changePercent >= 0 ? "rgba(116,212,155,0.65)" : "rgba(239,111,108,0.65)");
  priceLine.setAttribute("stroke-width", "1.4");
  priceLine.setAttribute("stroke-dasharray", "6 6");
  priceSvg.appendChild(priceLine);

  const currentPriceText = formatPrice(currentPrice);
  const priceTagWidth = Math.max(74, (currentPriceText.length * 7) + 18);
  const priceTag = document.createElementNS("http://www.w3.org/2000/svg", "rect");
  priceTag.setAttribute("x", String(width - rightPadding + 6));
  priceTag.setAttribute("y", String(currentY - 11));
  priceTag.setAttribute("width", String(priceTagWidth));
  priceTag.setAttribute("height", "22");
  priceTag.setAttribute("rx", "8");
  priceTag.setAttribute("fill", changePercent >= 0 ? "#74d49b" : "#ef6f6c");
  priceSvg.appendChild(priceTag);

  const priceTagLabel = document.createElementNS("http://www.w3.org/2000/svg", "text");
  priceTagLabel.setAttribute("x", String(width - rightPadding + 6 + priceTagWidth / 2));
  priceTagLabel.setAttribute("y", String(currentY + 4));
  priceTagLabel.setAttribute("text-anchor", "middle");
  priceTagLabel.setAttribute("fill", "#08161d");
  priceTagLabel.setAttribute("font-size", "12");
  priceTagLabel.setAttribute("font-weight", "700");
  priceTagLabel.textContent = currentPriceText;
  priceSvg.appendChild(priceTagLabel);

  const indicatorHeight = 150;
  const indicatorTop = 18;
  const indicatorBottom = 20;
  const indicatorLeft = leftPadding;
  const indicatorRight = rightPadding;
  const indicatorPlotHeight = indicatorHeight - indicatorTop - indicatorBottom;
  const indicatorPlotWidth = width - indicatorLeft - indicatorRight;
  const rsiGuides = [70, 50, 30];
  for (const level of rsiGuides) {
    const y = indicatorTop + indicatorPlotHeight - (level / 100) * indicatorPlotHeight;
    const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("x1", String(indicatorLeft));
    line.setAttribute("x2", String(width - indicatorRight));
    line.setAttribute("y1", String(y));
    line.setAttribute("y2", String(y));
    line.setAttribute("stroke", level === 50 ? "rgba(255,255,255,0.12)" : "rgba(255,255,255,0.08)");
    line.setAttribute("stroke-width", "1");
    indicatorSvg.appendChild(line);

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", String(width - indicatorRight + 10));
    label.setAttribute("y", String(y + 4));
    label.setAttribute("fill", "rgba(236,248,245,0.72)");
    label.setAttribute("font-size", "12");
    label.textContent = String(level);
    indicatorSvg.appendChild(label);
  }

  const rsiPoints = (chart.series.rsi_14 || []).map((point) => {
    const candleIndex = candles.findIndex(candle => candle.time === point.time);
    if (candleIndex === -1) {
      return null;
    }
    const x = indicatorLeft + candleIndex * (indicatorPlotWidth / Math.max(1, candles.length - 1));
    const y = indicatorTop + indicatorPlotHeight - (point.value / 100) * indicatorPlotHeight;
    return { x, y };
  }).filter(Boolean);

  if (rsiPoints.length > 1) {
    const rsiPath = document.createElementNS("http://www.w3.org/2000/svg", "path");
    rsiPath.setAttribute("d", toPath(rsiPoints));
    rsiPath.setAttribute("fill", "none");
    rsiPath.setAttribute("stroke", "#59d1b8");
    rsiPath.setAttribute("stroke-width", "2");
    indicatorSvg.appendChild(rsiPath);
  }

  setText("chart-last-price", formatPrice(currentPrice));
  setText("chart-price-change", formatPercent(changePercent));
  setText("chart-open-price", formatPrice(lastCandle.open));
  setText("chart-high-price", formatPrice(lastCandle.high));
  setText("chart-low-price", formatPrice(lastCandle.low));
  setText("chart-updated-at", formatClock(lastCandle.time));
  setValueTone("chart-last-price", changePercent >= 0 ? "positive" : "negative");
  setValueTone("chart-price-change", changePercent >= 0 ? "positive" : "negative");
  setValueTone("chart-open-price", "neutral");
  setValueTone("chart-high-price", "positive");
  setValueTone("chart-low-price", "negative");
  setValueTone("chart-updated-at", "neutral");
}

async function loadBuilder(symbol = "BTCUSDT", timeframe = "1m") {
  const [overview, builder, account] = await Promise.all([
    fetchJson("/api/system/overview"),
    fetchJson("/api/strategy/builder"),
    fetchJson("/api/account/state"),
  ]);
  const trackedSymbols = (builder.tracked_symbol_states || []).map(item => item.symbol);
  const selectedSymbol = trackedSymbols?.includes("BTCUSDT")
    ? "BTCUSDT"
    : (symbol === "BTCUSDT" && trackedSymbols?.length ? trackedSymbols[0] : symbol);
  const chart = await fetchJson(
    `/api/market/chart?symbol=${selectedSymbol}&timeframe=${timeframe}&bars=120`
  );

  state.overview = overview;
  state.builder = builder;
  state.account = account;
  state.chart = chart;
  state.liveWindow = null;
  state.manualContext = null;
  state.manualOrderResult = null;

  renderOverview(overview);
  addOverviewStreamSample(overview);
  renderOverviewStreamChart();
  renderBuilder(builder);
  renderChart(chart);
  await refreshLiveWindow(selectedSymbol);
  await refreshManualOrderContext(selectedSymbol);
  renderManualOrderResult(null);
  setManualOrderMessage("Ticket is ready. Review quantity, order type and filters before sending.");
}

async function refreshCandleLoader() {
  const loader = await fetchJson("/api/loader/candles");
  if (state.builder) {
    state.builder.candle_loader = loader;
  }
  renderCandleLoader(loader);
}

function startOverviewSocket() {
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${protocol}://${window.location.host}/ws/overview`);

  socket.onmessage = (event) => {
    const message = JSON.parse(event.data);
    if (message.type === "overview_update") {
      state.overview = message.payload;
      renderOverview(message.payload);
      addOverviewStreamSample(message.payload);
      renderOverviewStreamChart();
    }
  };
}

async function refreshOverview() {
  const overview = await fetchJson("/api/system/overview");
  state.overview = overview;
  renderOverview(overview);
  addOverviewStreamSample(overview);
  renderOverviewStreamChart();
}

function bindControls() {
  const combobox = document.getElementById("symbol-combobox");
  const symbolInput = document.getElementById("symbol-input");
  const symbolToggle = document.getElementById("symbol-toggle");
  const timeframeSelect = document.getElementById("timeframe-select");
  const loaderSave = document.getElementById("loader-save");
  const loaderStart = document.getElementById("loader-start");
  const loaderStop = document.getElementById("loader-stop");
  const loaderStartDelay = document.getElementById("loader-start-delay");
  const botStart = document.getElementById("bot-start");
  const botStop = document.getElementById("bot-stop");
  const manualRefresh = document.getElementById("manual-order-refresh");
  const manualSubmit = document.getElementById("manual-order-submit");
  const orderSymbol = document.getElementById("order-symbol");
  const orderType = document.getElementById("order-type");
  const orderPositionSide = document.getElementById("order-position-side");
  const orderQuantity = document.getElementById("order-quantity");
  const orderPrice = document.getElementById("order-price");
  const orderStopPrice = document.getElementById("order-stop-price");
  const orderLeverage = document.getElementById("order-leverage");
  const orderSide = document.getElementById("order-side");
  const orderReduceOnly = document.getElementById("order-reduce-only");
  const orderPostOnly = document.getElementById("order-post-only");

  async function reloadChartForCurrentSymbol() {
    const typedSymbol = symbolInput.value.trim().toUpperCase();
    const hasExactMatch = state.symbolOptions.some(option => option.symbol === typedSymbol);
    const symbol = hasExactMatch ? typedSymbol : (state.chart?.symbol || "BTCUSDT");
    const chart = await fetchJson(
      `/api/market/chart?symbol=${symbol}&timeframe=${timeframeSelect.value}&bars=120`
    );
    state.chart = chart;
    renderChart(chart);
    await refreshLiveWindow(symbol);
    await refreshManualOrderContext(symbol);
  }

  symbolInput.addEventListener("focus", () => {
    filterSymbolOptions(symbolInput.value);
    renderSymbolDropdown(true);
    combobox.classList.add("open");
  });

  symbolInput.addEventListener("input", () => {
    filterSymbolOptions(symbolInput.value);
    renderSymbolDropdown(true);
    combobox.classList.add("open");
    if (orderSymbol) {
      orderSymbol.value = symbolInput.value.trim().toUpperCase();
    }
  });

  symbolInput.addEventListener("keydown", async (event) => {
    if (!combobox.classList.contains("open") && ["ArrowDown", "ArrowUp"].includes(event.key)) {
      combobox.classList.add("open");
    }

    if (event.key === "ArrowDown") {
      event.preventDefault();
      if (state.filteredSymbolOptions.length) {
        state.activeSymbolIndex = Math.min(state.activeSymbolIndex + 1, state.filteredSymbolOptions.length - 1);
        renderSymbolDropdown(true);
      }
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      if (state.filteredSymbolOptions.length) {
        state.activeSymbolIndex = Math.max(state.activeSymbolIndex - 1, 0);
        renderSymbolDropdown(true);
      }
    } else if (event.key === "Enter") {
      event.preventDefault();
      if (state.filteredSymbolOptions.length && state.activeSymbolIndex >= 0) {
        await selectSymbol(state.filteredSymbolOptions[state.activeSymbolIndex].symbol);
      } else {
        await reloadChartForCurrentSymbol();
        combobox.classList.remove("open");
      }
    } else if (event.key === "Escape") {
      combobox.classList.remove("open");
    }
  });

  timeframeSelect.addEventListener("change", reloadChartForCurrentSymbol);

  symbolToggle.addEventListener("click", () => {
    const willOpen = !combobox.classList.contains("open");
    if (willOpen) {
      filterSymbolOptions(symbolInput.value);
      const currentSymbol = symbolInput.value.trim().toUpperCase();
      const selectedIndex = state.filteredSymbolOptions.findIndex(option => option.symbol === currentSymbol);
      state.activeSymbolIndex = selectedIndex >= 0 ? selectedIndex : (state.filteredSymbolOptions.length ? 0 : -1);
      renderSymbolDropdown(true);
      combobox.classList.add("open");
      symbolInput.focus();
    } else {
      combobox.classList.remove("open");
    }
  });

  document.addEventListener("click", (event) => {
    if (!combobox.contains(event.target)) {
      combobox.classList.remove("open");
    }
  });

  loaderSave.addEventListener("click", async () => {
    const response = await fetch("/api/loader/candles/settings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        start_delay_seconds: Number(loaderStartDelay.value || 0.7),
      }),
    });
    const loader = await response.json();
    if (state.builder) {
      state.builder.candle_loader = loader;
    }
    renderCandleLoader(loader);
  });

  loaderStart.addEventListener("click", async () => {
    await fetch("/api/loader/candles/start", { method: "POST" });
    await refreshCandleLoader();
  });

  loaderStop.addEventListener("click", async () => {
    await fetch("/api/loader/candles/stop", { method: "POST" });
    await refreshCandleLoader();
  });

  manualRefresh.addEventListener("click", async () => {
    const symbol = (orderSymbol.value || getSelectedSymbol()).trim().toUpperCase();
    orderSymbol.value = symbol;
    await refreshManualOrderContext(symbol);
    setManualOrderMessage(`Context refreshed for ${symbol}.`);
  });

  [orderType, orderQuantity, orderPrice, orderStopPrice].forEach((node) => {
    node.addEventListener("input", () => {
      renderManualOrderPanel();
    });
  });

  orderSymbol.addEventListener("change", async () => {
    const symbol = orderSymbol.value.trim().toUpperCase() || getSelectedSymbol();
    orderSymbol.value = symbol;
    await refreshManualOrderContext(symbol);
    setManualOrderMessage(`Context refreshed for ${symbol}.`);
  });

  manualSubmit.addEventListener("click", async () => {
    const payload = {
      symbol: (orderSymbol.value || getSelectedSymbol()).trim().toUpperCase(),
      side: orderSide.value,
      position_side: orderPositionSide.value,
      order_type: orderType.value,
      quantity: orderQuantity.value,
      price: orderPrice.value,
      stop_price: orderStopPrice.value,
      leverage: orderLeverage.value,
      reduce_only: orderReduceOnly.checked,
      post_only: orderPostOnly.checked,
    };

    setText("manual-order-status", "sending");
    setManualOrderMessage("Submitting order to Binance Futures...");
    try {
      const result = await postJson("/api/trading/manual/order", payload);
      state.manualOrderResult = result;
      renderManualOrderResult(result);
      setText("manual-order-status", "submitted");
      setManualOrderMessage(
        `${result.preview.side} ${result.preview.order_type} for ${result.preview.symbol} submitted successfully.`,
        "ok"
      );
      await Promise.all([
        refreshAccountState().catch((error) => console.error(error)),
        refreshBuilderState().catch((error) => console.error(error)),
        refreshManualOrderContext(payload.symbol).catch((error) => console.error(error)),
      ]);
    } catch (error) {
      renderManualOrderResult({ error: String(error.message || error) });
      setText("manual-order-status", "error");
      setManualOrderMessage(String(error.message || error), "error");
    }
  });

  botStart.addEventListener("click", async () => {
    await fetch("/api/system/bot/start", { method: "POST" });
  });

  botStop.addEventListener("click", async () => {
    await fetch("/api/system/bot/stop", { method: "POST" });
  });
}

async function main() {
  await loadBuilder();
  bindControls();
  startOverviewSocket();
  setInterval(() => {
    refreshOverview().catch((error) => console.error(error));
  }, 3000);
  setInterval(() => {
    refreshCandleLoader().catch((error) => console.error(error));
  }, 3000);
  setInterval(() => {
    refreshBuilderState().catch((error) => console.error(error));
  }, 5000);
  setInterval(() => {
    refreshAccountState()
      .then(() => renderManualOrderPanel())
      .catch((error) => console.error(error));
  }, 5000);
  setInterval(() => {
    refreshLiveWindow().catch((error) => console.error(error));
  }, 3000);
  setInterval(() => {
    const timeframe = document.getElementById("timeframe-select")?.value || (state.chart?.timeframe || "1m");
    const symbol = state.chart?.symbol || "BTCUSDT";
    fetchJson(`/api/market/chart?symbol=${symbol}&timeframe=${timeframe}&bars=120`)
      .then((chart) => {
        state.chart = chart;
        renderChart(chart);
        renderManualOrderPanel();
      })
      .catch((error) => console.error(error));
  }, 5000);
  setInterval(() => {
    refreshManualOrderContext().catch((error) => console.error(error));
  }, 10000);
}

main().catch((error) => {
  console.error(error);
});
