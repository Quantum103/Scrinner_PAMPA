# Volume Trader

A standalone Go service for the first stage of a Bybit USDT Perpetual (Linear) market scanner. The project is separate from `Quantum103/scrinner`.

## Current scope

- Reads Bybit instrument metadata (`/v5/market/instruments-info`) and public Linear tickers (`/v5/market/tickers`).
- Keeps only instruments with `contractType=LinearPerpetual`, `quoteCoin=USDT`, and `status=Trading`.
- Limits the tracked list using `MAX_TRACKED_CONTRACTS`.
- Repeats public market-data requests at the configured interval.
- Runs in paper-only mode. The repository contains no order placement, private trading API, Bybit API key, or Bybit API secret implementation.
- Includes strategy, virtual-account, cost-model, WebSocket, and Telegram configuration placeholders for subsequent stages.

The current REST ticker collector is data ingestion only. It does not rank trading signals, calculate volume conditions, open virtual positions, or execute trades. The configured public WebSocket URL is reserved for a later streaming-data stage; there is no WebSocket connection yet. Telegram settings are also placeholders and do not send messages.

## Requirements

- Go 1.23 or newer.
- Network access to the configured Bybit public REST endpoint when market-data polling is enabled.

## Run

Copy `.env.example` to `.env`, edit values as needed, and export them before starting the process. The application intentionally uses environment variables directly and does not load `.env` files.

PowerShell:

```powershell
Copy-Item .env.example .env
Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]*)=(.*)$') {
        [Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), 'Process')
    }
}
go run ./cmd/volume-trader
```

Linux / macOS:

```sh
cp .env.example .env
set -a
. ./.env
set +a
go run ./cmd/volume-trader
```

To validate startup without making a network request:

```powershell
$env:MARKET_DATA_ENABLED = 'false'
go run ./cmd/volume-trader
```

Stop the service with `Ctrl+C`. It handles interrupt and termination signals.

## Configuration

| Variable | Default | Description |
|---|---:|---|
| `APP_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error` |
| `BYBIT_REST_URL` | `https://api.bybit.com` | Public REST base URL |
| `BYBIT_PUBLIC_WS_URL` | `wss://stream.bybit.com/v5/public/linear` | Public Linear WebSocket URL reserved for a later stage |
| `MARKET_DATA_ENABLED` | `true` | Enable or disable REST ticker polling |
| `DATA_UPDATE_INTERVAL` | `2s` | Polling interval, for example `500ms` or `2s` |
| `MAX_TRACKED_CONTRACTS` | `100` | Maximum number of active USDT perpetual symbols kept in the current tracked list |
| `STRATEGY_VOLUME_MULTIPLIER` | `2.0` | Reserved strategy parameter; not currently used |
| `STRATEGY_TRAILING_STOP_PERCENT` | `2.5` | Reserved strategy parameter; not currently used |
| `STRATEGY_REVERSAL_CONFIRMATIONS` | `2` | Reserved strategy parameter; not currently used |
| `VIRTUAL_DEPOSIT_USDT` | `10` | Virtual starting balance; no positions are opened yet |
| `RISK_PER_TRADE_PERCENT` | `1` | Reserved risk parameter; not currently used |
| `MAX_OPEN_POSITIONS` | `1` | Reserved risk parameter; not currently used |
| `TRADING_FEE_RATE` | `0.00055` | Configurable paper-simulation fee assumption per side; not currently applied |
| `SLIPPAGE_BPS` | `5` | Configurable paper-simulation slippage assumption; not currently applied |
| `TELEGRAM_BOT_TOKEN` | empty | Reserved for a future Telegram integration |
| `TELEGRAM_CHAT_ID` | empty | Reserved for a future Telegram integration |

The fee and slippage defaults are simulation assumptions, not a statement of the user's actual Bybit fee tier. Adjust them to the applicable account and instrument before implementing P&L.

The market category is fixed to `linear`, and the application is LONG-only by design at this stage. There is no configuration switch that enables real trading.

## Checks

```sh
gofmt -w ./cmd ./internal
go test ./...
go vet ./...
```
