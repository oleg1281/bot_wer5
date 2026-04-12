# Professional Interface Plan

## Goal

Build the first public-facing interface as a professional web dashboard for operating, monitoring, and managing the trading bot.

The first release should prioritize:

- operational control
- account visibility
- live market visibility
- risk management
- event transparency

It should not start as a marketing landing page or a mobile app.

## Recommended Stack

- Backend API: FastAPI
- Frontend: React or Next.js
- Realtime transport: WebSocket from backend to browser
- Database: PostgreSQL for production, SQLite only for local experiments
- Auth: email/password plus session or JWT

## Product Roles

### Admin

- full bot control
- exchange connection management
- strategy configuration
- risk limit editing
- logs and incident visibility
- user management

### Operator

- view system status
- monitor positions, orders, logs
- pause or resume strategies
- cannot change critical account settings

### Client

- view own account
- view bot state
- view positions, orders, pnl
- limited settings only

## Main Navigation

The first version should have these sections:

1. Overview
2. Market Monitor
3. Positions
4. Orders
5. Strategies
6. Risk
7. Streams and Health
8. Logs and Events
9. Settings

## Screen 1: Overview

This is the default home screen.

### Main blocks

- Bot status: running, paused, degraded, reconnecting
- Exchange status: connected or disconnected
- Account equity
- Available balance
- Unrealized PnL
- Realized PnL today
- Open positions count
- Open orders count
- Active symbols count
- Weak pairs count
- Blocked pairs count
- Queue size

### Visual widgets

- Equity curve for today
- PnL today chart
- Positions exposure by symbol
- Long vs short exposure
- Top movers among tracked pairs
- Recent critical events feed

## Screen 2: Market Monitor

This is the main live trading board.

### Table columns

- Symbol
- Last close
- Mark price
- Best bid
- Best ask
- Spread
- 1m change
- 5m change
- Volume
- Funding rate
- Signal state
- Strategy state
- Health state

### Filters

- search by symbol
- only active pairs
- only weak pairs
- only blocked pairs
- only signals
- only pairs with spread above threshold
- only pairs with abnormal move

### Actions

- open symbol details
- add to watchlist
- block symbol
- force weak recovery reset
- enable or disable strategy for symbol

## Screen 3: Positions

### Table columns

- Symbol
- Side
- Size
- Entry price
- Mark price
- Liquidation price
- Unrealized PnL
- Realized PnL
- Margin mode
- Leverage
- Open time

### Actions

- close position
- reduce position
- set stop loss
- set take profit
- hedge

## Screen 4: Orders

### Tabs

- Open Orders
- Filled Orders
- Cancelled Orders
- Rejected Orders

### Table columns

- Time
- Symbol
- Side
- Type
- Status
- Price
- Quantity
- Filled
- Average fill price
- Source

### Actions

- cancel order
- cancel all by symbol
- export history

## Screen 5: Strategies

This is where the bot logic is controlled.

### Blocks

- Strategy list
- Strategy status
- Allowed symbols
- Cooldowns
- Entry conditions
- Exit conditions
- Signal thresholds
- Auto-trading toggle

### Per-strategy information

- name
- enabled or disabled
- instruments scope
- last trigger time
- last execution result
- current confidence
- error state

## Screen 6: Risk

This screen is critical for professional use.

### Global limits

- max total exposure
- max positions count
- max risk per trade
- daily loss limit
- max symbol exposure
- max drawdown before pause

### Per-symbol limits

- allowed leverage
- max notional
- max spread allowed
- max slippage allowed
- ban or whitelist

### Safety controls

- emergency stop
- close all positions
- disable new entries
- disable all trading but keep monitoring

## Screen 7: Streams and Health

This screen is for technical stability and should exist from v1.

### Connection blocks

- market stream status
- book stream status
- user stream status
- Binance REST status
- network status

### Metrics

- queue size
- messages per second
- reconnect count
- blocked tasks
- weak pairs
- blocked pairs
- stale kline pairs
- stale any-data pairs

### Detailed panels

- market batch states
- book stream counters
- user stream counters
- per-symbol stale timers

## Screen 8: Logs and Events

### Event categories

- system
- network
- stream reconnects
- order updates
- account updates
- blocked pairs
- weak pair recovery
- strategy triggers
- trade executions
- risk limit hits

### Features

- filter by severity
- filter by symbol
- filter by category
- search text
- export logs

## Screen 9: Settings

### Sections

- exchange credentials
- notification channels
- default risk profile
- interface preferences
- user access roles
- retention settings

## Symbol Details Drawer

Clicking a symbol anywhere should open a side panel with:

- current market snapshot
- recent 1m candles
- mark price and funding
- bid and ask spread
- current position
- recent orders
- recent fills
- latest strategy signals
- health state for this symbol

## First API Endpoints

These are the minimum backend endpoints needed to support the first interface.

### System

- `GET /api/system/overview`
- `GET /api/system/health`
- `GET /api/system/logs`

### Market

- `GET /api/market/pairs`
- `GET /api/market/pairs/{symbol}`
- `GET /api/market/watchlist`

### Account

- `GET /api/account/balance`
- `GET /api/account/positions`
- `GET /api/account/orders`
- `GET /api/account/fills`

### Risk

- `GET /api/risk/config`
- `PUT /api/risk/config`

### Bot Control

- `POST /api/bot/pause`
- `POST /api/bot/resume`
- `POST /api/bot/emergency-stop`
- `POST /api/bot/close-all`

### Realtime

- `WS /ws/overview`
- `WS /ws/market`
- `WS /ws/positions`
- `WS /ws/logs`

## First Realtime Events

The frontend should receive these event types first:

- `system_status`
- `queue_status`
- `market_pair_update`
- `position_update`
- `order_update`
- `account_update`
- `risk_event`
- `stream_event`
- `log_event`

## First Development Order

Build in this order:

1. FastAPI service with `/api/system/overview`
2. Internal bot state snapshot provider
3. WebSocket push for live updates
4. Overview page
5. Positions page
6. Orders page
7. Streams and Health page
8. Risk controls
9. Market Monitor page

## MVP Rule

The first public interface should answer these questions instantly:

- Is the bot healthy?
- Is the exchange connection healthy?
- What positions and orders are open?
- Is risk under control?
- Are there weak or blocked symbols?
- What just happened?

If a screen does not help answer one of these questions, it is lower priority than the operator dashboard.
