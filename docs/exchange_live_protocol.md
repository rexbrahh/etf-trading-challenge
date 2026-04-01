# Exchange Live Protocol

This note captures the live Exchange interface protocol as observed on April 1, 2026 from the production frontend bundle served at:

- `https://comp.waterlooquantclub.com/game/exchange/a6f5e8ee-4597-485f-aa17-b90737d7acc5`

The goal is to keep the C++ strategy core stable while we build the live adapter around verified wire details.

## Verified transport

- Frontend API base URL: `https://api.comp.waterlooquantclub.com`
- Frontend websocket URL derivation: replace `http` with `ws`, then append `/ws`
- Derived websocket endpoint: `wss://api.comp.waterlooquantclub.com/ws`

## Verified auth flow

- Competitor login is a REST request:
  - `POST /auth/user/login`
  - JSON body: `{"access_token": "<token>"}`
- The frontend uses `credentials: "include"` on REST requests.
- Inference: the backend sets a session cookie on login, and both REST and websocket traffic depend on that cookie afterward.

## Verified generic websocket envelopes

- Request current run state:

```json
{
  "type": "RequestState",
  "run_id": "<run-id>"
}
```

- Send an Exchange game action:

```json
{
  "type": "GameAction",
  "run_id": "<run-id>",
  "game": "Exchange",
  "data": {
    "action": "<ExchangeAction>",
    "...": "action-specific fields"
  }
}
```

## Verified Exchange client actions

- `MarkReady`
- `PlaceOrder`
- `CancelOrder`
- `CancelAllOrders`
- `SubscribeToBook`
- `UnsubscribeFromBook`

### Verified action payloads

- Mark ready:

```json
{
  "action": "MarkReady"
}
```

- Place order:

```json
{
  "action": "PlaceOrder",
  "market": "ETF",
  "side": "Buy",
  "price": 501,
  "size": 10
}
```

- Cancel one order:

```json
{
  "action": "CancelOrder",
  "order_id": 42
}
```

- Cancel all orders, optionally one market:

```json
{
  "action": "CancelAllOrders",
  "market": "E"
}
```

or

```json
{
  "action": "CancelAllOrders",
  "market": null
}
```

- Subscribe to one book:

```json
{
  "action": "SubscribeToBook",
  "market": "E",
  "since": 123456
}
```

- Unsubscribe from one book:

```json
{
  "action": "UnsubscribeFromBook",
  "market": "E"
}
```

## Verified `GameUpdate` payload types for Exchange

- `StateChange`
- `OrderBookUpdate`
- `ReadyAcknowledged`
- `UserStateUpdate`
- `BookSubscribed`
- `BookUnsubscribed`
- `NewTrades`
- `PnlUpdate`
- `PriceUpdate`
- `SubtypeUpdate`

### Verified `SubtypeUpdate` variants

- `AbcDraw`
- `PolymarketRoll`

## Verified frontend behaviors

- The competitor exchange page redirects to `/login` when no user session is present.
- The page requests state immediately on load with `RequestState`.
- Market hotkeys visible in the UI:
  - `1-9` select markets
  - `B` queue buy at best bid
  - `S` queue sell at best offer
  - `V` queue buy at best offer
  - `A` queue sell at best bid
  - `Up/Down` adjust price by `$1`
  - `Left/Right` adjust size by `10`
  - `Enter` sends queued order
  - `Esc` clears queued order
  - `C` centers the book

## Adapter implications

- The live adapter does not need DOM scraping for order entry.
- The first functional live client must support:
  - access-token login
  - cookie persistence
  - websocket connection to `wss://api.comp.waterlooquantclub.com/ws`
  - `RequestState` and `GameAction` envelopes
- The tricky part is not the order schema anymore; it is cookie-aware websocket session management.
