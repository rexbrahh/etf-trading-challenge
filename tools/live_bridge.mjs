#!/usr/bin/env node

import net from "node:net";

const args = process.argv.slice(2);
const portIndex = args.indexOf("--port");
if (portIndex < 0 || !args[portIndex + 1]) {
  console.error("usage: node tools/live_bridge.mjs --port <port>");
  process.exit(1);
}

const controlPort = Number(args[portIndex + 1]);
if (!Number.isInteger(controlPort) || controlPort <= 0) {
  console.error("invalid control port");
  process.exit(1);
}

let controlSocket = null;
let controlBuffer = "";
let liveConfig = null;
let ws = null;
let cookieHeader = "";
let currentPhase = null;
let sawTrading = false;
let readySent = false;
let shuttingDown = false;
let bootstrapStateApplied = false;

const pendingCreates = [];
const activeByLocalId = new Map();
const serverToLocal = new Map();
const replaceAfterCancel = new Map();
const internalToExchangeMarket = new Map();
const exchangeToInternalMarket = new Map();

function sendControl(message) {
  if (!controlSocket || controlSocket.destroyed) {
    return;
  }
  controlSocket.write(`${JSON.stringify(message)}\n`);
}

function emitError(message) {
  sendControl({ type: "error", message });
}

function emitEnd(reason) {
  sendControl({ type: "end", reason });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeSide(value) {
  if (value === "Buy" || value === "buy") {
    return "buy";
  }
  if (value === "Sell" || value === "sell") {
    return "sell";
  }
  throw new Error(`unknown side: ${value}`);
}

function rememberMarkets(markets) {
  if (!Array.isArray(markets)) {
    return;
  }
  for (const market of markets) {
    const exchangeSymbol = String(market?.symbol ?? "");
    const internalSymbol = String(market?.base_asset ?? "").toUpperCase();
    if (!exchangeSymbol || !internalSymbol) {
      continue;
    }
    internalToExchangeMarket.set(internalSymbol, exchangeSymbol);
    exchangeToInternalMarket.set(exchangeSymbol, internalSymbol);
  }
}

function toExchangeMarket(symbol) {
  const normalized = String(symbol).toUpperCase();
  return internalToExchangeMarket.get(normalized) ?? `${normalized}/USD`;
}

function toInternalSymbol(market) {
  const normalized = String(market ?? "");
  if (!normalized) {
    return "ETF";
  }
  if (exchangeToInternalMarket.has(normalized)) {
    return exchangeToInternalMarket.get(normalized);
  }
  return normalized.split("/", 1)[0].toUpperCase();
}

function capitalizeSide(value) {
  return normalizeSide(value) === "buy" ? "Buy" : "Sell";
}

function toUs(value) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return Date.now() * 1000;
  }
  return Math.trunc(value * 1000);
}

function bestLevel(levels) {
  if (!Array.isArray(levels) || levels.length === 0) {
    return null;
  }
  const [top] = levels;
  return { price: Number(top.price), qty: Number(top.size) };
}

function emitBookUpdate(market, book) {
  const symbol = toInternalSymbol(market);
  sendControl({
    type: "market_event",
    event: {
      type: "book_update",
      ts: toUs(Date.now()),
      book: {
        symbol,
        bid: bestLevel(book?.bids),
        ask: bestLevel(book?.asks),
      },
    },
  });
}

function emitTradePrint(market, trade) {
  const symbol = toInternalSymbol(market);
  sendControl({
    type: "market_event",
    event: {
      type: "trade_print",
      ts: toUs(trade?.timestamp ?? Date.now()),
      symbol,
      aggressor_side: normalizeSide(trade?.side ?? "Buy"),
      price: Number(trade?.price),
      qty: Number(trade?.size),
      source: "exchange_live_tape",
      resting_order_id: null,
      team_involved: false,
    },
  });
}

function emitAck(localOrder) {
  sendControl({
    type: "market_event",
    event: {
      type: "ack",
      ts: toUs(Date.now()),
      order_id: localOrder.localOrderId,
      symbol: localOrder.symbol,
      side: localOrder.side,
      price: localOrder.price,
      qty: localOrder.size,
      tag: localOrder.tag,
    },
  });
}

function emitBootstrapFill(fill) {
  const symbol = toInternalSymbol(fill?.market);
  sendControl({
    type: "market_event",
    event: {
      type: "fill",
      ts: toUs(fill?.timestamp ?? Date.now()),
      order_id: 0,
      symbol,
      side: normalizeSide(fill?.side ?? "Buy"),
      price: Number(fill?.price),
      qty: Number(fill?.size),
      aggressor: false,
      source: "exchange_live_bootstrap",
      tag: "bootstrap_fill",
    },
  });
}

function emitFill(localOrder, qty) {
  if (qty <= 0) {
    return;
  }
  sendControl({
    type: "market_event",
    event: {
      type: "fill",
      ts: toUs(Date.now()),
      order_id: localOrder.localOrderId,
      symbol: localOrder.symbol,
      side: localOrder.side,
      price: localOrder.price,
      qty,
      aggressor: false,
      source: "exchange_live",
      tag: localOrder.tag,
    },
  });
}

function emitCancelAck(localOrder) {
  sendControl({
    type: "market_event",
    event: {
      type: "cancel_ack",
      ts: toUs(Date.now()),
      order_id: localOrder.localOrderId,
      symbol: localOrder.symbol,
    },
  });
}

function emitReject(localOrder, reason) {
  sendControl({
    type: "market_event",
    event: {
      type: "reject",
      ts: toUs(Date.now()),
      order_id: localOrder ? localOrder.localOrderId : null,
      symbol: localOrder?.symbol ?? "ETF",
      reason,
    },
  });
}

function isOpenStatus(status) {
  return status === "Open" || status === "PartiallyFilled";
}

function isCancelledStatus(status) {
  return /cancel/i.test(status);
}

function isRejectedStatus(status) {
  return /reject/i.test(status);
}

function sendWs(payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) {
    throw new Error("websocket is not open");
  }
  ws.send(JSON.stringify(payload));
}

function sendExchangeAction(data) {
  sendWs({
    type: "GameAction",
    run_id: liveConfig.run_id,
    game: "Exchange",
    data,
  });
}

function matchPendingCreate(order) {
  const index = pendingCreates.findIndex(
    (candidate) =>
      candidate.symbol === order.symbol &&
      candidate.side === order.side &&
      candidate.price === order.price &&
      candidate.size === order.size,
  );
  if (index < 0) {
    return null;
  }
  const [matched] = pendingCreates.splice(index, 1);
  return matched;
}

function dispatchPlace(localOrder) {
  const request = {
    localOrderId: Number(localOrder.localOrderId),
    symbol: localOrder.symbol,
    side: normalizeSide(localOrder.side),
    price: Number(localOrder.price),
    size: Number(localOrder.size),
    tag: localOrder.tag,
    cancelRequested: false,
    cancelSent: false,
  };
  pendingCreates.push(request);
  activeByLocalId.set(request.localOrderId, { ...request, filledSize: 0, serverId: null, status: "PendingNew" });
  sendExchangeAction({
    action: "PlaceOrder",
    market: toExchangeMarket(request.symbol),
    side: capitalizeSide(request.side),
    price: request.price,
    size: request.size,
  });
}

function cleanupLocalOrder(localOrder) {
  activeByLocalId.delete(localOrder.localOrderId);
  if (localOrder.serverId !== null && localOrder.serverId !== undefined) {
    serverToLocal.delete(localOrder.serverId);
  }
}

function maybeSendDeferredCancel(localOrder) {
  if (localOrder.cancelRequested && !localOrder.cancelSent && localOrder.serverId != null) {
    localOrder.cancelSent = true;
    sendExchangeAction({ action: "CancelOrder", order_id: localOrder.serverId });
  }
}

function handleOrderUpdate(rawOrder) {
  const order = {
    serverId: String(rawOrder.id),
    symbol: toInternalSymbol(rawOrder.market),
    side: normalizeSide(rawOrder.side),
    price: Number(rawOrder.price),
    size: Number(rawOrder.size),
    filledSize: Number(rawOrder.filled_size ?? 0),
    status: String(rawOrder.status),
  };

  let localOrder = null;
  const knownLocalId = serverToLocal.get(order.serverId);
  if (knownLocalId !== undefined) {
    localOrder = activeByLocalId.get(knownLocalId) ?? null;
  } else {
    const matched = matchPendingCreate(order);
    if (!matched) {
      return;
    }
    localOrder = {
      ...matched,
      serverId: order.serverId,
      status: order.status,
      filledSize: 0,
    };
    serverToLocal.set(order.serverId, matched.localOrderId);
    activeByLocalId.set(matched.localOrderId, localOrder);
    emitAck(localOrder);
  }

  const deltaFill = Math.max(0, order.filledSize - (localOrder.filledSize ?? 0));
  localOrder = {
    ...localOrder,
    serverId: order.serverId,
    status: order.status,
    filledSize: order.filledSize,
    price: order.price,
    size: order.size,
  };
  activeByLocalId.set(localOrder.localOrderId, localOrder);

  if (deltaFill > 0) {
    emitFill(localOrder, deltaFill);
  }

  if (isOpenStatus(order.status)) {
    maybeSendDeferredCancel(localOrder);
    return;
  }

  if (isCancelledStatus(order.status)) {
    emitCancelAck(localOrder);
  } else if (isRejectedStatus(order.status)) {
    emitReject(localOrder, order.status);
  }

  const replacement = replaceAfterCancel.get(localOrder.localOrderId);
  cleanupLocalOrder(localOrder);
  if (replacement && isCancelledStatus(order.status)) {
    replaceAfterCancel.delete(localOrder.localOrderId);
    dispatchPlace(replacement);
  }
}

function reconcileOrderUpdates(orders) {
  if (!Array.isArray(orders)) {
    return;
  }
  for (const order of orders) {
    handleOrderUpdate(order);
  }
}

function handleStateChange(state) {
  const previousPhase = currentPhase;
  currentPhase = state.phase;
  rememberMarkets(state?.markets);
  const tradingActive = currentPhase === "Trading";
  sendControl({
    type: "status",
    phase: currentPhase,
    trading_active: tradingActive,
    timer_end_ms: state.timer_end_ms ?? null,
    server_time_ms: state.server_time_ms ?? null,
  });

  if (currentPhase === "Lobby" && !readySent) {
    readySent = true;
    sendExchangeAction({ action: "MarkReady" });
  }

  if (tradingActive) {
    if (!sawTrading) {
      sawTrading = true;
      if (!bootstrapStateApplied) {
        bootstrapStateApplied = true;
        for (const fill of state?.my_state?.fills ?? []) {
          emitBootstrapFill(fill);
        }
      }
      for (const market of liveConfig.subscribe_markets) {
        sendExchangeAction({ action: "SubscribeToBook", market: toExchangeMarket(market), since: null });
      }
    }
    if (Array.isArray(state?.my_state?.open_orders)) {
      reconcileOrderUpdates(state.my_state.open_orders);
    }
    return;
  }

  if (sawTrading && previousPhase === "Trading") {
    emitEnd(`exchange phase transitioned to ${currentPhase}`);
    shutdown(0);
  }
}

function handleExchangeUpdate(update) {
  switch (update.type) {
    case "StateChange":
      handleStateChange(update.state);
      break;
    case "ReadyAcknowledged":
      readySent = true;
      break;
    case "OrderBookUpdate":
      emitBookUpdate(update.book.market, update.book);
      break;
    case "BookSubscribed":
      emitBookUpdate(update.market, update.book);
      for (const trade of update.trades ?? []) {
        emitTradePrint(update.market, trade);
      }
      break;
    case "NewTrades":
      for (const trade of update.trades ?? []) {
        emitTradePrint(update.market, trade);
      }
      break;
    case "UserStateUpdate":
      reconcileOrderUpdates(update?.delta?.orders);
      break;
    default:
      break;
  }
}

async function fetchActiveExchangeRunId() {
  const response = await fetch(`${liveConfig.api_base_url}/user/game-runs`, {
    headers: cookieHeader ? { cookie: cookieHeader } : {},
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`active-run lookup failed (${response.status}): ${body}`);
  }

  const runs = await response.json();
  if (!Array.isArray(runs)) {
    throw new Error("active-run lookup returned a non-array payload");
  }

  const preferred = runs.find((run) => run?.game_type === "Exchange" && run?.run_id === liveConfig.run_id);
  const exchangeRun = preferred ?? runs.find((run) => run?.game_type === "Exchange");
  return exchangeRun?.run_id ?? null;
}

async function waitForExchangeRunId() {
  currentPhase = "AwaitingRun";
  while (!shuttingDown) {
    const runId = await fetchActiveExchangeRunId();
    sendControl({
      type: "status",
      phase: runId ? "RunDiscovered" : "AwaitingRun",
      trading_active: false,
      waiting_for_run: !runId,
      run_id: runId,
    });
    if (runId) {
      liveConfig.run_id = runId;
      return runId;
    }
    await sleep(2000);
  }
  return null;
}

async function loginAndConnect() {
  const loginResponse = await fetch(`${liveConfig.api_base_url}/auth/user/login`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ access_token: liveConfig.access_token }),
  });

  if (!loginResponse.ok) {
    const body = await loginResponse.text();
    throw new Error(`login failed (${loginResponse.status}): ${body}`);
  }

  cookieHeader = loginResponse.headers
    .getSetCookie()
    .map((cookie) => cookie.split(";", 1)[0])
    .join("; ");

  const activeRunId = await waitForExchangeRunId();
  if (!activeRunId || shuttingDown) {
    return;
  }

  const websocketUrl = liveConfig.websocket_url || liveConfig.api_base_url.replace(/^http/, "ws") + "/ws";
  ws = new WebSocket(websocketUrl, {
    headers: cookieHeader ? { cookie: cookieHeader } : {},
  });

  ws.addEventListener("open", () => {
    sendWs({ type: "RequestState", run_id: liveConfig.run_id });
  });

  ws.addEventListener("message", (event) => {
    try {
      const message = JSON.parse(String(event.data));
      if (message.type === "Error") {
        emitError(`exchange websocket error: ${message.message}`);
        shutdown(1);
        return;
      }
      if (message.type === "GameUpdate" && message.run_id === liveConfig.run_id && message.game === "Exchange") {
        handleExchangeUpdate(message.data);
      }
    } catch (error) {
      emitError(`failed to process websocket message: ${error.message}`);
      shutdown(1);
    }
  });

  ws.addEventListener("error", () => {
    emitError("websocket connection error");
  });

  ws.addEventListener("close", () => {
    if (!shuttingDown) {
      emitEnd("websocket closed");
      shutdown(0);
    }
  });
}

function handleCommand(command) {
  const type = command.type;
  if (type === "place_limit") {
    dispatchPlace({
      localOrderId: command.order_id,
      symbol: command.symbol,
      side: command.side,
      price: command.price,
      size: command.qty,
      tag: command.tag,
    });
    return;
  }

  const localOrderId = Number(command.order_id);
  const localOrder = activeByLocalId.get(localOrderId);
  if (!localOrder) {
    return;
  }

  if (type === "cancel") {
    localOrder.cancelRequested = true;
    activeByLocalId.set(localOrderId, localOrder);
    maybeSendDeferredCancel(localOrder);
    return;
  }

  if (type === "cancel_replace") {
    const replacement = {
      localOrderId,
      symbol: localOrder.symbol,
      side: localOrder.side,
      price: Number(command.new_price),
      size: Number(command.new_qty),
      tag: command.tag,
    };
    replaceAfterCancel.set(localOrderId, replacement);
    localOrder.cancelRequested = true;
    activeByLocalId.set(localOrderId, localOrder);
    maybeSendDeferredCancel(localOrder);
  }
}

function handleControlMessage(message) {
  if (message.type === "init") {
    const config = message.config;
    liveConfig = config.run.live;
    loginAndConnect().catch((error) => {
      emitError(error.message);
      shutdown(1);
    });
    return;
  }

  if (message.type === "command") {
    handleCommand(message.command);
    return;
  }

  if (message.type === "shutdown") {
    shutdown(0);
  }
}

function shutdown(code) {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  try {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.close();
    }
  } catch {
  }
  try {
    controlSocket?.end();
  } catch {
  }
  setTimeout(() => process.exit(code), 20);
}

const server = net.createServer((socket) => {
  controlSocket = socket;
  socket.setEncoding("utf8");

  socket.on("data", (chunk) => {
    controlBuffer += chunk;
    let newline = controlBuffer.indexOf("\n");
    while (newline >= 0) {
      const line = controlBuffer.slice(0, newline).trim();
      controlBuffer = controlBuffer.slice(newline + 1);
      if (line) {
        try {
          handleControlMessage(JSON.parse(line));
        } catch (error) {
          emitError(`invalid control message: ${error.message}`);
          shutdown(1);
          return;
        }
      }
      newline = controlBuffer.indexOf("\n");
    }
  });

  socket.on("close", () => shutdown(0));
  socket.on("error", () => shutdown(1));
});

server.listen(controlPort, "127.0.0.1");
