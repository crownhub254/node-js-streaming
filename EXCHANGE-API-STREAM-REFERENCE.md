# Exchange API & Stream Collection Reference — v9.7
## 53 Exchanges | 4 Collection Methods | Per-Symbol Subscription Detail

**Script:** `compare-v7-enhanced.js` v9.7 | **Last Updated:** 2025

---

## Legend

| Term | Meaning |
|------|---------|
| **Native WS** | Direct WebSocket connection using exchange-specific protocol. Highest priority (0). |
| **Native REST** | Direct HTTP polling using exchange REST API. Used when exchange has no WS, or as supplemental. |
| **CCXT Pro** | CCXT Pro `watchTrades` + `watchOrderBook` + `watchTicker` (unified WS abstraction). Priority 1. |
| **CCXT REST** | CCXT `fetchTrades` polled every 5s. Priority 2. Runs for all exchanges where `ccxtId` is set. |
| **Direct REST Poll** | Exchange-specific `restPoll()` — separate REST loop that runs in parallel with Native WS. |
| **REST Fallback** | `restFallbackUrls[]` — polled only when the WS connection is in stale/reconnect state. |
| **skipPro** | `skipPro:true` — CCXT Pro WS disabled (CCXT REST still runs). Reason noted per exchange. |
| **skipTicker** | `skipTicker:true` — `watchTicker` disabled; only `watchTrades` + `watchOrderBook` via CCXT Pro. |

### Symbol Format Key
- **BTCUSDT** — no separator, uppercase (Binance-style)
- **BTC-USDT** — hyphen separator (OKX/Coinbase/KuCoin-style)
- **BTC_USDT** — underscore separator (Gate.io/Gate-style)
- **BTC/USDT** — slash separator (CCXT canonical / AscendEX)
- **btcusdt** — lowercase no separator (HTX-style internal channel names)
- **tBTCUSD** — Bitfinex prefix `t` = trading pair

---

## Quick Reference Table

| # | Exchange | Tier | Native Method | WS URL | CCXT ID | CCXT Pairs | REST Poll |
|---|----------|------|---------------|--------|---------|-----------|-----------|
| 1 | Binance | 1 | WS | `stream.binance.com:9443/stream` | binance | 14 | REST Fallback |
| 2 | Coinbase | 1 | WS | `ws-feed.exchange.coinbase.com` | coinbase | 19 | — |
| 3 | Kraken | 1 | WS | `ws.kraken.com/v2` | kraken | 16 | — |
| 4 | KuCoin | 1 | WS | Dynamic token endpoint | kucoin | 15 | REST Fallback |
| 5 | OKX | 1 | WS | `ws.okx.com:8443/ws/v5/public` | okx | 18 | — |
| 6 | Bybit | 1 | WS | `stream.bybit.com/v5/public/spot` | bybit | 16 | REST Fallback |
| 7 | Bitfinex | 1 | WS | `api-pub.bitfinex.com/ws/2` | bitfinex *(skipPro)* | 10 | — |
| 8 | Gate.io | 1 | WS | `api.gateio.ws/ws/v4/` | gateio | 15 | REST Fallback |
| 9 | HTX | 1 | WS | `api.huobi.pro/ws` | htx *(skipPro)* | 11 | REST Fallback |
| 10 | WOO X | 1 | WS | `wss.woo.org/v2/ws/public` | woo | 11 | — |
| 11 | Crypto.com | 2 | WS | `stream.crypto.com/exchange/v1/market` | cryptocom | 15 | — |
| 12 | Bitstamp | 2 | WS | `ws.bitstamp.net` | bitstamp | 13 | REST Fallback |
| 13 | WhiteBIT | 2 | WS | `api.whitebit.com/ws` | whitebit *(skipPro)* | 17 | REST Fallback |
| 14 | AscendEX | 2 | WS | `ascendex.com/1/api/pro/v1/stream` | ascendex | 11 | — |
| 15 | BingX | 2 | WS | `open-api-ws.bingx.com/market` | bingx | 13 | — |
| 16 | Toobit | 2 | WS | `stream.toobit.com/quote/ws/v1` | toobit | 11 | — |
| 17 | Deepcoin | 2 | WS | `stream.deepcoin.com/streamlet/trade/...` | deepcoin *(skipPro)* | 5 | Direct Poll 10s |
| 18 | XT.com | 2 | WS | `stream.xt.com/public` | xt | 13 | REST Fallback |
| 19 | Zoomex | 2 | WS | `stream.zoomex.com/v5/public/spot` | — | — | — |
| 20 | Bitget | 2 | WS | `ws.bitget.com/v2/ws/public` | bitget | 12 | — |
| 21 | Gemini | 2 | WS | `api.gemini.com/v2/marketdata` | gemini *(skipTicker)* | 14 | — |
| 22 | Binance.US | 2 | WS | `stream.binance.us:9443/stream` | binanceus | 16 | — |
| 23 | MEXC | 2 | REST | `api.mexc.com/api/v3/trades` | mexc | 16 | — |
| 24 | CoinEx | 3 | WS | `socket.coinex.com/v2/spot` | coinex | 15 | — |
| 25 | LBank | 3 | WS | `www.lbkex.net/ws/V2/` | lbank | 12 | REST Fallback |
| 26 | BitMart | 3 | WS | `ws-manager-compress.bitmart.com/api` | bitmart | 12 | — |
| 27 | Pionex | 3 | WS | `ws.pionex.com/wsPub` | — | — | — |
| 28 | Poloniex | 3 | WS | `ws.poloniex.com/ws/public` | poloniex *(skipPro)* | 10 | — |
| 29 | HitBTC | 3 | WS | `api.hitbtc.com/api/3/ws/public` | hitbtc | 12 | — |
| 30 | BTSE | 3 | WS | `ws.btse.com/ws/spot` | — | — | Direct Poll 30s |
| 31 | Biconomy | 3 | WS | `bei.biconomy.com/ws` | — | — | — |
| 32 | Hotcoin | 3 | WS+REST | `wss.hotcoinfin.com/trade/multiple` | — | — | Parallel REST 10s |
| 33 | NovaEx | 3 | WS | `wss.woox.io/ws/stream/OqdphuyIYbng-t001` | — | — | — |
| 34 | FameEX | 3 | WS | `wsapi.fameex.com/v1/ws/stream/public` | — | — | — |
| 35 | Websea | 3 | WS | `oapi.websea.com/ws/v1/spot/market` | — | — | — |
| 36 | Bullish | 3 | WS | `api.exchange.bullish.com/trading-api/v1/...` | bullish | 13 | Direct Poll 45s |
| 37 | Darkex | 3 | WS | `ws.darkex.com/kline-api/ws` | — | — | — |
| 38 | Bitrue | 3 | WS | `ws.bitrue.com/market/ws` | bitrue | 16 | Direct Poll 10s |
| 39 | BloFin | 3 | WS | `openapi.blofin.com/ws/public` | blofin | 8 | — |
| 40 | DigiFinex | 3 | WS | `openapi.digifinex.com/ws/v1/` | digifinex *(skipPro)* | 6 | — |
| 41 | EXMO | 3 | WS | `ws-api.exmo.com/v1/public` | exmo | 16 | — |
| 42 | CEX.IO | 3 | REST | `cex.io/api/trade_history/` | cex *(skipPro)* | 25 | — |
| 43 | OrangeX | 3 | REST | `api.orangex.com/api/v1/public/...` | — | — | — |
| 44 | Azbit | 3 | REST | `data.azbit.com/api/deals` | — | — | — |
| 45 | BVOX | 3 | REST | `api.bitvenus.me/openapi/quote/v1/trades` | — | — | — |
| 46 | Trubit Pro | 3 | REST | `api-spot.trubit.com/openapi/quote/v1/trades` | — | — | — |
| 47 | BigONE | 3 | REST | `big.one/api/v3/asset_pairs/` | bigone | 7 | — |
| 48 | LATOKEN | 3 | REST | `api.latoken.com/v2/trade/history/` | latoken | 9 | — |
| 49 | Coinstore | 3 | WS | `ws.coinstore.com/s/ws` | — | — | REST Fallback |
| 50 | GroveX | 3 | WS | `ws.grovex.io/kline-api/ws` | — | — | REST Fallback |
| 51 | CoinW | 3 | REST | `api.coinw.com/api/v1/public` | — | — | — |
| 52 | Batonex | 3 | REST | `api.batonex.com/openapi/quote/v1/trades` | — | — | — |
| 53 | CEEX | 3 | WS | `wsapi.ceex.com/openapi/quote/ws/v1` | — | — | REST Fallback |

---

## Per-Exchange Detail

---

### 1. Binance
**Tier:** 1 | **Batch:** 1 | **Native:** WebSocket

**WebSocket URLs (with fallbacks):**
```
wss://stream.binance.com:9443/stream        (primary)
wss://stream.binance.com:443/stream         (fallback 1)
wss://data-stream.binance.vision/stream     (fallback 2)
```

**REST Fallback URL (polled when WS is reconnecting):**
```
https://api.binance.com/api/v3/trades?symbol={SYMBOL}&limit=5
```
Symbols polled: `BTCUSDT ETHUSDT SOLUSDT BTCUSDC ETHUSDC SOLUSDC PENGUUSDT PENGUUSDC WIFUSDT WIFUSDC SUIUSDT SUIUSDC ENAUSDT ENAUSDC`

**Symbol Format:** UPPERCASE no separator — `BTCUSDT`, `ETHUSDT`, etc. (lowercase in channel names: `btcusdt@trade`)

**Subscription Message (sent on `onOpen`):**
```json
{
  "method": "SUBSCRIBE",
  "params": [
    "btcusdt@trade", "ethusdt@trade", "solusdt@trade",
    "btcusdc@trade", "ethusdc@trade", "solusdc@trade",
    "penguusdt@trade", "penguusdc@trade", "wifusdt@trade",
    "wifusdc@trade", "suiusdt@trade", "suiusdc@trade",
    "enausdt@trade", "enausdc@trade",
    "btcusdt@depth5@100ms", "ethusdt@depth5@100ms", "solusdt@depth5@100ms",
    "btcusdc@depth5@100ms", "ethusdc@depth5@100ms", "solusdc@depth5@100ms",
    "penguusdt@depth5@100ms", "penguusdc@depth5@100ms", "wifusdt@depth5@100ms",
    "wifusdc@depth5@100ms", "suiusdt@depth5@100ms", "suiusdc@depth5@100ms",
    "enausdt@depth5@100ms", "enausdc@depth5@100ms"
  ],
  "id": 1
}
```
**Streams:** `{symbol}@trade` for trades | `{symbol}@depth5@100ms` for top-5 orderbook at 100ms cadence

**CCXT Config:** `ccxtId: binance` | `skipPro: true` (CCXT Pro disabled — caused WIF/USDC, BTC/USDC, SUI/USDC connection closures + 56 timeouts in v9.4; CCXT REST still active)

**CCXT REST Pairs (fetchTrades polled every 5s):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, PENGU/USDT, PENGU/USDC, WIF/USDT, WIF/USDC, SUI/USDT, SUI/USDC, ENA/USDT, ENA/USDC` (14 pairs)

**Data Collected:** Trades (`e=trade`) + OrderBook depth-5 (`lastUpdateId + bids`)

---

### 2. Coinbase
**Tier:** 1 | **Batch:** 1 | **Native:** WebSocket

**WebSocket URLs:**
```
wss://ws-feed.exchange.coinbase.com    (primary)
wss://ws-feed.pro.coinbase.com         (fallback)
```

**Symbol Format:** Hyphen separator — `BTC-USD`, `ETH-USDT`, `SOL-USDC`, etc.

**Subscription Message (sent on `onOpen`):**
```json
{
  "type": "subscribe",
  "product_ids": [
    "BTC-USD", "ETH-USD", "SOL-USD",
    "BTC-USDT", "ETH-USDT", "SOL-USDT",
    "BTC-USDC", "ETH-USDC", "SOL-USDC",
    "PENGU-USDC", "PENGU-USD",
    "POPCAT-USDC", "POPCAT-USD",
    "WIF-USDC", "WIF-USD",
    "SUI-USDC", "SUI-USD",
    "ENA-USDC", "ENA-USD"
  ],
  "channels": ["matches", "level2_batch", "heartbeat"]
}
```
**Channels:** `matches` = trades | `level2_batch` = order book updates | `heartbeat` = keepalive

**CCXT Config:** `ccxtId: coinbase` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (19):**
`BTC/USD, ETH/USD, SOL/USD, BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, PENGU/USDC, PENGU/USD, POPCAT/USDC, POPCAT/USD, WIF/USDC, WIF/USD, SUI/USDC, SUI/USD, ENA/USDC, ENA/USD`

**Data Collected:** Trades (`type=match`) + OrderBook L2 snapshots/updates (`type=snapshot` / `l2update`)

---

### 3. Kraken
**Tier:** 1 | **Batch:** 1 | **Native:** WebSocket

**WebSocket URLs:**
```
wss://ws.kraken.com/v2             (primary)
wss://ws-auth.kraken.com/v2        (fallback)
```
**Ping:** `{"method":"ping"}` every 25s

**Symbol Format:** Slash separator — `BTC/USDT`, `ETH/USD`, `SOL/USDC`, etc.

**Subscription Messages (sent on `onOpen` — 2 separate messages):**

Trades:
```json
{
  "method": "subscribe",
  "params": {
    "channel": "trade",
    "symbol": ["BTC/USDT","ETH/USDT","SOL/USDT","BTC/USD","ETH/USD","SOL/USD",
               "BTC/USDC","ETH/USDC","SOL/USDC","PENGU/USDT","PENGU/USDC",
               "PENGU/USD","POPCAT/USD","WIF/USD","SUI/USD","ENA/USD"],
    "snapshot": false
  }
}
```

Order Book:
```json
{
  "method": "subscribe",
  "params": {
    "channel": "book",
    "symbol": ["BTC/USDT","ETH/USDT", ... (same 16 pairs)],
    "depth": 10,
    "snapshot": true
  }
}
```

**CCXT Config:** `ccxtId: kraken` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (16):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USD, ETH/USD, SOL/USD, BTC/USDC, ETH/USDC, SOL/USDC, PENGU/USDT, PENGU/USDC, PENGU/USD, POPCAT/USD, WIF/USD, SUI/USD, ENA/USD`

**Data Collected:** Trades (`channel=trade`) + OrderBook (`channel=book`)

---

### 4. KuCoin
**Tier:** 1 | **Batch:** 1 | **Native:** WebSocket (dynamic token)

**Token Fetch (HTTP POST before WS connect):**
```
POST https://api.kucoin.com/api/v1/bullet-public
```
Response provides `token` + `instanceServers[0].endpoint`. WS URL constructed as:
```
{endpoint}?token={token}
```
Default fallback: `wss://ws-api-spot.kucoin.com`

**REST Fallback URL (when WS reconnecting):**
```
https://api.kucoin.com/api/v1/market/orderbook/level2_20?symbol={SYMBOL}
```
Symbols: `BTC-USDT ETH-USDT SOL-USDT BRETT-USDT PENGU-USDT POPCAT-USDT WIF-USDT WIF-USDC SUI-USDT SUI-USDC ENA-USDT ENA-USDC`

**Ping:** `{"id": {timestamp}, "type": "ping"}` every 18s

**Symbol Format:** Hyphen separator — `BTC-USDT`, `ETH-USDC`, etc.

**Subscription Messages (sent on `onOpen` — 2 messages):**

Trades:
```json
{
  "id": 1,
  "type": "subscribe",
  "topic": "/market/match:BTC-USDT,ETH-USDT,SOL-USDT,BTC-USDC,ETH-USDC,SOL-USDC,BRETT-USDT,PENGU-USDT,POPCAT-USDT,WIF-USDT,WIF-USDC,SUI-USDT,SUI-USDC,ENA-USDT,ENA-USDC",
  "privateChannel": false,
  "response": true
}
```

Order Book (depth-5):
```json
{
  "id": 3,
  "type": "subscribe",
  "topic": "/spotMarket/level2Depth5:BTC-USDT,ETH-USDT,SOL-USDT,BTC-USDC,ETH-USDC,SOL-USDC,BRETT-USDT,PENGU-USDT,POPCAT-USDT,WIF-USDT,WIF-USDC,SUI-USDT,SUI-USDC,ENA-USDT,ENA-USDC",
  "privateChannel": false,
  "response": true
}
```

**CCXT Config:** `ccxtId: kucoin` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (15):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, BRETT/USDT, PENGU/USDT, POPCAT/USDT, WIF/USDT, WIF/USDC, SUI/USDT, SUI/USDC, ENA/USDT, ENA/USDC`

**Data Collected:** Trades (`/market/match` topic) + OrderBook L2 depth-5

---

### 5. OKX
**Tier:** 1 | **Batch:** 1 | **Native:** WebSocket

**WebSocket URLs:**
```
wss://ws.okx.com:8443/ws/v5/public       (primary)
wss://wsaws.okx.com:8443/ws/v5/public    (fallback)
```
**Ping:** raw string `"ping"` every 25s

**Symbol Format:** Hyphen separator — `BTC-USDT`, `ETH-USDC`, `WIF-USD`, etc.

**Subscription Messages (sent on `onOpen` — 2 messages):**

Trades:
```json
{
  "op": "subscribe",
  "args": [
    {"channel": "trades", "instId": "BTC-USDT"},
    {"channel": "trades", "instId": "ETH-USDT"},
    {"channel": "trades", "instId": "SOL-USDT"},
    {"channel": "trades", "instId": "BTC-USDC"},
    {"channel": "trades", "instId": "ETH-USDC"},
    {"channel": "trades", "instId": "SOL-USDC"},
    {"channel": "trades", "instId": "PENGU-USDT"},
    {"channel": "trades", "instId": "PENGU-USDC"},
    {"channel": "trades", "instId": "PENGU-USD"},
    {"channel": "trades", "instId": "WIF-USDT"},
    {"channel": "trades", "instId": "WIF-USDC"},
    {"channel": "trades", "instId": "WIF-USD"},
    {"channel": "trades", "instId": "SUI-USDT"},
    {"channel": "trades", "instId": "SUI-USDC"},
    {"channel": "trades", "instId": "SUI-USD"},
    {"channel": "trades", "instId": "ENA-USDT"},
    {"channel": "trades", "instId": "ENA-USDC"},
    {"channel": "trades", "instId": "ENA-USD"}
  ]
}
```

Order Book (top-5):
```json
{
  "op": "subscribe",
  "args": [
    {"channel": "books5", "instId": "BTC-USDT"},
    ... (same 18 pairs with channel "books5")
  ]
}
```

**CCXT Config:** `ccxtId: okx` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (18):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, PENGU/USDT, PENGU/USDC, PENGU/USD, WIF/USDT, WIF/USDC, WIF/USD, SUI/USDT, SUI/USDC, SUI/USD, ENA/USDT, ENA/USDC, ENA/USD`

**Data Collected:** Trades (`channel=trades`) + OrderBook top-5 with checksum validation (`channel=books5`)

---

### 6. Bybit
**Tier:** 1 | **Batch:** 1 | **Native:** WebSocket

**WebSocket URLs:**
```
wss://stream.bybit.com/v5/public/spot       (primary)
wss://stream.bytick.com/v5/public/spot      (fallback 1)
wss://stream.bybit.kz/v5/public/spot        (fallback 2)
```
**Ping:** `{"op":"ping"}` every 20s

**REST Fallback URL:**
```
https://api.bybit.com/v5/market/recent-trade?category=spot&symbol={SYMBOL}&limit=5
```
Symbols: `BTCUSDT ETHUSDT SOLUSDT BRETTUSDT PENGUUSDT POPCATUSDT WIFUSDT SUIUSDT ENAUSDT`

**Symbol Format:** UPPERCASE no separator — `BTCUSDT`, `BRETTUSDT`, etc.

**Subscription Messages (sent on `onOpen` in batches of 5 with 500ms delays, OB after 2s):**

Trades (batched — example first batch):
```json
{"op": "subscribe", "args": ["publicTrade.BTCUSDT","publicTrade.ETHUSDT","publicTrade.SOLUSDT","publicTrade.BTCUSDC","publicTrade.ETHUSDC"]}
```
Sent in 3 batches (0ms, 500ms, 1000ms) until all 16 pairs subscribed.

Order Book (batched — sent starting at 2000ms delay):
```json
{"op": "subscribe", "args": ["orderbook.50.BTCUSDT","orderbook.50.ETHUSDT","orderbook.50.SOLUSDT","orderbook.50.BTCUSDC","orderbook.50.ETHUSDC"]}
```
Sent in 3 batches (2000ms, 2500ms, 3000ms) until all 16 pairs subscribed.

**All Pairs subscribed:** `BTCUSDT ETHUSDT SOLUSDT BTCUSDC ETHUSDC SOLUSDC BRETTUSDT BRETTUSDC PENGUUSDT POPCATUSDT WIFUSDT WIFUSDC SUIUSDT SUIUSDC ENAUSDT ENAUSDC`

**CCXT Config:** `ccxtId: bybit` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (16):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, BRETT/USDT, BRETT/USDC, PENGU/USDT, POPCAT/USDT, WIF/USDT, WIF/USDC, SUI/USDT, SUI/USDC, ENA/USDT, ENA/USDC`

**Data Collected:** Trades (`publicTrade.*`) + OrderBook depth-50 (`orderbook.50.*`)

---

### 7. Bitfinex
**Tier:** 1 | **Batch:** 1 | **Native:** WebSocket

**WebSocket URLs:**
```
wss://api-pub.bitfinex.com/ws/2    (primary)
wss://api.bitfinex.com/ws/2        (fallback)
```
**Ping:** `{"event":"ping"}` every 25s | **Stale Timeout:** 60s

**Symbol Format:** Bitfinex prefix — `tBTCUSD`, `tETHUST`, `tSOLUST`, `tSUIUST`, `tENAUSD`, etc.
- `t` prefix = trading pair
- `USD` = US Dollar | `UST` = USDT (Bitfinex notation)

**Subscription (one message per pair per channel, sent on `onOpen`):**
```json
{"event": "subscribe", "channel": "trades", "symbol": "tBTCUSD"}
{"event": "subscribe", "channel": "book", "symbol": "tBTCUSD", "prec": "P0", "len": 25}
```
Repeated for each pair: `tBTCUSD, tETHUSD, tSOLUSD, tBTCUST, tETHUST, tSOLUST, tSUIUST, tSUIUSD, tENAUST, tENAUSD` (10 pairs)

**Channel Mapping:** Server sends `{"event":"subscribed","chanId":X,"channel":"...","symbol":"..."}` — `chanId` is stored and used to route incoming array messages.

**CCXT Config:** `ccxtId: bitfinex` | `skipPro: true` (CCXT Pro caused typeErr in `watchTrades`/`watchOB` in v9.4) | CCXT REST active

**CCXT REST Pairs (10):**
`BTC/USD, ETH/USD, SOL/USD, BTC/USDT, ETH/USDT, SOL/USDT, SUI/USDT, SUI/USD, ENA/USDT, ENA/USD`

**Data Collected:** Trades (events `te/tu` in channel arrays) + OrderBook L2 (book channel arrays)

---

### 8. Gate.io
**Tier:** 1 | **Batch:** 1 | **Native:** WebSocket

**WebSocket URLs:**
```
wss://api.gateio.ws/ws/v4/          (primary)
wss://fx-ws.gateio.ws/v4/ws/usdt    (fallback)
```
**Ping:** `{"time": {epoch_seconds}, "channel": "spot.ping"}` every 15s

**REST Fallback URL:**
```
https://api.gateio.ws/api/v4/spot/trades?currency_pair={SYMBOL}&limit=5
```
Symbols: `BTC_USDT ETH_USDT SOL_USDT BRETT_USDT PENGU_USDT POPCAT_USDT WIF_USDT SUI_USDT ENA_USDT`

**Symbol Format:** Underscore separator — `BTC_USDT`, `ETH_USDC`, etc.

**Subscription Messages (sent on `onOpen` — 1 trade sub + 1 per-pair OB sub):**

Trades (all pairs in one message):
```json
{
  "time": {epoch},
  "channel": "spot.trades",
  "event": "subscribe",
  "payload": ["BTC_USDT","ETH_USDT","SOL_USDT","BTC_USDC","ETH_USDC","SOL_USDC",
              "BRETT_USDT","PENGU_USDT","POPCAT_USDT","WIF_USDT","WIF_USDC",
              "SUI_USDT","SUI_USDC","ENA_USDT","ENA_USDC"]
}
```

Order Book (one message per pair, 1000ms update cadence):
```json
{"time": {epoch}, "channel": "spot.order_book", "event": "subscribe", "payload": ["BTC_USDT", "5", "1000ms"]}
{"time": {epoch}, "channel": "spot.order_book", "event": "subscribe", "payload": ["ETH_USDT", "5", "1000ms"]}
... (one per pair)
```

**CCXT Config:** `ccxtId: gateio` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (15):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, BRETT/USDT, PENGU/USDT, POPCAT/USDT, WIF/USDT, WIF/USDC, SUI/USDT, SUI/USDC, ENA/USDT, ENA/USDC`

**Data Collected:** Trades (`channel=spot.trades`) + OrderBook (`channel=spot.order_book`)

---

### 9. HTX (Huobi)
**Tier:** 1 | **Batch:** 1 | **Native:** WebSocket (gzip compressed)

**WebSocket URLs:**
```
wss://api.huobi.pro/ws          (primary)
wss://api-aws.huobi.pro/ws      (fallback)
```
**Compression:** gzip | **Stale Timeout:** 45s

**REST Fallback URL:**
```
https://api.huobi.pro/market/trade?symbol={symbol}
```
Symbols: `btcusdt ethusdt solusdt brettusdt penguusdt popcatusdt wifusdt suiusdt enausdt`

**Symbol Format:** lowercase no separator — `btcusdt`, `ethusdt`, etc. (used in channel names)

**Subscription (one message per pair/type, sent on `onOpen`):**
```json
{"sub": "market.btcusdt.trade.detail", "id": "btcusdtt"}
{"sub": "market.btcusdt.depth.step0",  "id": "btcusdtd"}
```
Repeated for: `btcusdt ethusdt solusdt btcusdc ethusdc brettusdt penguusdt popcatusdt wifusdt suiusdt enausdt` (11 pairs)

**Server Ping/Pong:** Server sends `{"ping": N}` → respond `{"pong": N}`

**CCXT Config:** `ccxtId: htx` | `skipPro: true` (CCXT Pro caused connection closures on PENGU/WIF/ENA in v9.4) | CCXT REST active

**CCXT REST Pairs (11):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, BRETT/USDT, PENGU/USDT, POPCAT/USDT, WIF/USDT, SUI/USDT, ENA/USDT`
*(SOL/USDC excluded — delisted from HTX)*

**Data Collected:** Trades (`market.{sym}.trade.detail`) + OrderBook depth step0 (`market.{sym}.depth.step0`)

---

### 10. WOO X
**Tier:** 1 | **Batch:** 1 | **Native:** WebSocket

**WebSocket URLs:**
```
wss://wss.woo.org/ws/stream/OqdphuyCtYWxwzhxyLLjOWNdFP7sQt8RPWzmb5xY    (primary, authenticated app-id)
wss://wss.woo.org/ws/stream/public                                          (fallback)
```
**Ping:** `{"event":"ping"}` every 9s

**Symbol Format:** `SPOT_{BASE}_{QUOTE}` — `SPOT_BTC_USDT`, `SPOT_ETH_USDC`, etc.

**Subscription (one message per pair per channel, sent on `onOpen`):**
```json
{"id": "SPOT_BTC_USDT",  "event": "subscribe", "topic": "SPOT_BTC_USDT@trade"}
{"id": "SPOT_BTC_USDTo", "event": "subscribe", "topic": "SPOT_BTC_USDT@orderbook"}
```
Repeated for all pairs: `SPOT_BTC_USDT, SPOT_ETH_USDT, SPOT_SOL_USDT, SPOT_BTC_USDC, SPOT_ETH_USDC, SPOT_BRETT_USDT, SPOT_PENGU_USDT, SPOT_POPCAT_USDT, SPOT_WIF_USDT, SPOT_SUI_USDT, SPOT_ENA_USDT`
*(SPOT_SOL_USDC removed — delisted)*

**CCXT Config:** `ccxtId: woo` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (11):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, BRETT/USDT, PENGU/USDT, POPCAT/USDT, WIF/USDT, SUI/USDT, ENA/USDT`

**Data Collected:** Trades (`@trade` topic) + OrderBook (`@orderbook` topic)

---

### 11. Crypto.com
**Tier:** 2 | **Batch:** 2 | **Native:** WebSocket

**WebSocket URLs:**
```
wss://stream.crypto.com/exchange/v1/market    (primary)
wss://stream.crypto.com/v2/market             (fallback)
```
**Ping/Heartbeat:** Server sends heartbeat — respond immediately with same message. Custom ping: `{"id":0,"method":"public/heartbeat"}` every 25s

**Symbol Format:** Underscore separator — `BTC_USDT`, `ETH_USD`, `PENGU_USDT`, etc.

**Subscription Message (sent after 1s delay on `onOpen` — per Crypto.com docs to prevent ECONNRESET):**
```json
{
  "id": 1,
  "method": "subscribe",
  "params": {
    "channels": [
      "trade.BTC_USDT", "book.BTC_USDT.10",
      "trade.ETH_USDT", "book.ETH_USDT.10",
      "trade.SOL_USDT", "book.SOL_USDT.10",
      "trade.BTC_USD",  "book.BTC_USD.10",
      "trade.ETH_USD",  "book.ETH_USD.10",
      "trade.SOL_USD",  "book.SOL_USD.10",
      "trade.PENGU_USDT","book.PENGU_USDT.10",
      "trade.PENGU_USD", "book.PENGU_USD.10",
      "trade.POPCAT_USD","book.POPCAT_USD.10",
      "trade.WIF_USDT",  "book.WIF_USDT.10",
      "trade.WIF_USD",   "book.WIF_USD.10",
      "trade.SUI_USDT",  "book.SUI_USDT.10",
      "trade.SUI_USD",   "book.SUI_USD.10",
      "trade.ENA_USDT",  "book.ENA_USDT.10",
      "trade.ENA_USD",   "book.ENA_USD.10"
    ]
  }
}
```
**Channels:** `trade.{SYMBOL}` for trades | `book.{SYMBOL}.10` for top-10 orderbook

**CCXT Config:** `ccxtId: cryptocom` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (15):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USD, ETH/USD, SOL/USD, PENGU/USDT, PENGU/USD, POPCAT/USD, WIF/USDT, WIF/USD, SUI/USDT, SUI/USD, ENA/USDT, ENA/USD`

**Data Collected:** Trades (`result.channel` starts with `trade`) + OrderBook (`result.channel` starts with `book`)

---

### 12. Bitstamp
**Tier:** 2 | **Batch:** 2 | **Native:** WebSocket

**WebSocket URL:**
```
wss://ws.bitstamp.net
```
**Ping:** `{"event":"bts:heartbeat"}` every 20s

**REST Fallback URL:**
```
https://www.bitstamp.net/api/v2/transactions/btcusd/
```

**Symbol Format:** lowercase no separator — `btcusd`, `ethusdt`, `solusdc`, etc.

**Subscription (one per pair per channel, sent on `onOpen`):**
```json
{"event": "bts:subscribe", "data": {"channel": "live_trades_btcusd"}}
{"event": "bts:subscribe", "data": {"channel": "order_book_btcusd"}}
```
Repeated for all pairs: `btcusd ethusd solusd btcusdt ethusdt btcusdc ethusdc solusdc penguusd popcatusd wifusd suiusd enausd` (13 pairs)
*(btcusdt, ethusdt added; solusdt not listed on Bitstamp)*

**Note (v9.7):** Bitstamp API v2 sends `event:'data'` for live_trades AND `event:'trade'` — both are accepted.

**CCXT Config:** `ccxtId: bitstamp` | CCXT Pro + CCXT REST active

**CCXT REST Pairs (13):**
`BTC/USD, ETH/USD, SOL/USD, BTC/USDT, ETH/USDT, BTC/USDC, ETH/USDC, SOL/USDC, PENGU/USD, POPCAT/USD, WIF/USD, SUI/USD, ENA/USD`

**Data Collected:** Trades (`live_trades_*` channel) + OrderBook (`order_book_*` channel)

---

### 13. WhiteBIT
**Tier:** 2 | **Batch:** 2 | **Native:** WebSocket

**WebSocket URL:**
```
wss://api.whitebit.com/ws
```
**Ping:** `{"id":0,"method":"server.ping","params":[]}` every 25s | **Stale Timeout:** 45s

**REST Fallback URL:**
```
https://whitebit.com/api/v4/public/trades/{SYMBOL}
```
Symbols: `BTC_USDT ETH_USDT SOL_USDT`

**Symbol Format:** Underscore separator — `BTC_USDT`, `ETH_USDC`, `WIF_USDT`, etc.

**Subscription Messages (sent on `onOpen`):**

Trades (all pairs in one message):
```json
{
  "id": 1,
  "method": "trades_subscribe",
  "params": ["BTC_USDT","ETH_USDT","SOL_USDT","BTC_USDC","ETH_USDC","SOL_USDC",
             "BTC_USD","ETH_USD","SOL_USD","PENGU_USDT","PENGU_USDC",
             "WIF_USDT","WIF_USDC","SUI_USDT","SUI_USDC","ENA_USDT","ENA_USDC"]
}
```

Order Book (one message per pair, depth 100):
```json
{"id": 2, "method": "depth_subscribe", "params": ["BTC_USDT", 100, "0", true]}
{"id": 2, "method": "depth_subscribe", "params": ["ETH_USDT", 100, "0", true]}
... (one per pair, same 17 pairs)
```

**CCXT Config:** `ccxtId: whitebit` | `skipPro: true` (CCXT Pro caused connection closures on SOL/SUI/WIF in v9.4) | CCXT REST active

**CCXT REST Pairs (17):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, BTC/USD, ETH/USD, SOL/USD, PENGU/USDT, PENGU/USDC, WIF/USDT, WIF/USDC, SUI/USDT, SUI/USDC, ENA/USDT, ENA/USDC`

**Data Collected:** Trades (`method=trades_update`) + OrderBook (`method=depth_update`)

---

### 14. AscendEX
**Tier:** 2 | **Batch:** 2 | **Native:** WebSocket

**WebSocket URL:**
```
wss://ascendex.com/1/api/pro/v1/stream
```
**Ping:** `{"op":"ping"}` every 15s | **Stale Timeout:** 45s

**Symbol Format:** Slash separator — `BTC/USDT`, `ETH/USD`, etc.

**Subscription (one per pair per channel, sent on `onOpen`):**
```json
{"op": "sub", "ch": "trades:BTC/USDT"}
{"op": "sub", "ch": "depth:BTC/USDT"}
```
Repeated for all pairs: `BTC/USDT ETH/USDT SOL/USDT BTC/USDC ETH/USDC SOL/USDC BTC/USD ETH/USD SOL/USD BRETT/USDT PENGU/USDT WIF/USDT SUI/USDT ENA/USDT`

**Server Ping:** Server sends `{"m":"ping"}` → respond with `{"op":"pong"}`

**CCXT Config:** `ccxtId: ascendex` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (11):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USD, ETH/USD, SOL/USD, BRETT/USDT, PENGU/USDT, WIF/USDT, SUI/USDT, ENA/USDT`

**Data Collected:** Trades (`m=trades`) + OrderBook (`m=depth` / `m=depth-snapshot`)

---

### 15. BingX
**Tier:** 2 | **Batch:** 2 | **Native:** WebSocket (gzip compressed)

**WebSocket URLs:**
```
wss://open-api-ws.bingx.com/market     (primary)
wss://open-api-ws.bingx.com/market/v2  (fallback)
```
**Compression:** gzip | **Ping:** send `"Pong"` string every 5s | **Stale Timeout:** 45s

**Symbol Format:** Hyphen separator — `BTC-USDT`, `ETH-USDC`, etc.

**Subscription (one per pair per stream, sent on `onOpen`):**
```json
{"id": "BTC-USDT",  "reqType": "sub", "dataType": "BTC-USDT@trade"}
{"id": "BTC-USDTd", "reqType": "sub", "dataType": "BTC-USDT@depth5"}
```
Repeated for: `BTC-USDT ETH-USDT SOL-USDT BTC-USDC ETH-USDC SOL-USDC BRETT-USDT PENGU-USDT POPCAT-USDT WIF-USDT SUI-USDT SUI-USDC ENA-USDT`

**CCXT Config:** `ccxtId: bingx` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (13):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, BRETT/USDT, PENGU/USDT, POPCAT/USDT, WIF/USDT, SUI/USDT, SUI/USDC, ENA/USDT`

**Data Collected:** Trades (`dataType` contains `@trade`) + OrderBook depth-5 (`@depth5`)

---

### 16. Toobit
**Tier:** 2 | **Batch:** 2 | **Native:** WebSocket

**WebSocket URLs:**
```
wss://stream.toobit.com/quote/ws/v1    (primary)
wss://stream.toobit.com/quote/ws/v2    (fallback)
```
**Custom Ping:** `{"ping": {timestamp}}` every 15s | **Stale Timeout:** 45s

**Symbol Format:** UPPERCASE no separator — `BTCUSDT`, `ETHUSDC`, etc.

**Subscription (one per pair per topic, sent on `onOpen`):**
```json
{"symbol": "BTCUSDT", "topic": "trade", "event": "sub", "params": {"binary": false}}
{"symbol": "BTCUSDT", "topic": "depth", "event": "sub", "params": {"binary": false}}
```
Repeated for: `BTCUSDT ETHUSDT SOLUSDT BTCUSDC ETHUSDC SOLUSDC PENGUUSDT POPCATUSDT WIFUSDT SUIUSDT ENAUSDT`
*(BRETTUSDT not listed on Toobit)*

**CCXT Config:** `ccxtId: toobit` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (11):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, PENGU/USDT, POPCAT/USDT, WIF/USDT, SUI/USDT, ENA/USDT`

**Data Collected:** Trades (`topic=trade`) + OrderBook (`topic=depth`)

---

### 17. Deepcoin
**Tier:** 2 | **Batch:** 2 | **Native:** WebSocket + Direct REST Poll

**WebSocket URL:**
```
wss://stream.deepcoin.com/streamlet/trade/public/spot?platform=api
```
**Ping:** `"ping"` string every 15s | **Stale Timeout:** 45s

**REST Poll URL (every 10s, runs in parallel with WS):**
```
https://api.deepcoin.com/deepcoin/market/books?instId={SYMBOL}&sz=5
```
Symbols polled: `BTC-USDT ETH-USDT SOL-USDT BTC-USD PENGU-USDT WIF-USDT`
*(USDC pairs removed — delisted from Deepcoin)*

**Symbol Format (WS):** UPPERCASE no separator — `BTCUSDT`, `PENGUUSDT` | **REST:** Hyphen — `BTC-USDT`

**Subscription (one per pair, sent on `onOpen`):**
```json
{
  "SendTopicAction": {
    "Action": "1",
    "FilterValue": "DeepCoin_BTCUSDT",
    "LocalNo": {timestamp},
    "ResumeNo": -2,
    "TopicID": "2"
  }
}
```
Repeated for: `BTCUSDT ETHUSDT SOLUSDT PENGUUSDT WIFUSDT`

**CCXT Config:** `ccxtId: deepcoin` | `skipPro: true` | CCXT REST active

**CCXT REST Pairs (5):**
`BTC/USDT, ETH/USDT, SOL/USDT, PENGU/USDT, WIF/USDT`

**Data Collected (WS):** Trades (`a=PMT`, row has instrument field `d.I`) | **REST:** OrderBook depth-5

---

### 18. XT.com
**Tier:** 2 | **Batch:** 2 | **Native:** WebSocket

**WebSocket URLs:**
```
wss://stream.xt.com/public     (primary)
wss://stream2.xt.com/public    (fallback 1)
wss://stream3.xt.com/public    (fallback 2)
```
**Ping:** `"ping"` string every 15s | **Stale Timeout:** 45s

**REST Fallback URL:**
```
https://sapi.xt.com/v4/public/trade/recent?symbol={symbol}&limit=5
```
Symbols: `btc_usdt eth_usdt sol_usdt brett_usdt pengu_usdt popcat_usdt wif_usdt sui_usdt ena_usdt`

**Symbol Format:** lowercase underscore — `btc_usdt`, `eth_usdc`, etc.

**Subscription Messages (sent on `onOpen` — 2 batched messages):**
```json
{
  "method": "subscribe",
  "params": ["trade@btc_usdt","trade@eth_usdt","trade@sol_usdt","trade@btc_usdc",
             "trade@eth_usdc","trade@sol_usdc","trade@brett_usdt","trade@pengu_usdt",
             "trade@popcat_usdt","trade@wif_usdt","trade@sui_usdt","trade@sui_usdc","trade@ena_usdt"],
  "id": "xt_trades"
}
{
  "method": "subscribe",
  "params": ["depth@btc_usdt,5","depth@eth_usdt,5", ... (same 13 pairs)],
  "id": "xt_depth"
}
```

**CCXT Config:** `ccxtId: xt` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (13):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, BRETT/USDT, PENGU/USDT, POPCAT/USDT, WIF/USDT, SUI/USDT, SUI/USDC, ENA/USDT`

**Data Collected:** Trades (`topic=trade`) + OrderBook (`topic=depth`/`depth_update`)

---

### 19. Zoomex
**Tier:** 2 | **Batch:** 2 | **Native:** WebSocket (no CCXT)

**WebSocket URL:**
```
wss://stream.zoomex.com/v5/public/spot
```
**Ping:** `{"op":"ping"}` every 20s | **Stale Timeout:** 45s
**Note:** REST API `api.zoomex.com` DNS unreachable — WS only.

**Symbol Format:** UPPERCASE no separator — `BTCUSDT`, `BRETTUSDT`, etc.

**Subscription (batched with delays to avoid rate-limit drops, sent on `onOpen`):**
```json
Batch 1 (0ms):   {"op":"subscribe","args":["publicTrade.BTCUSDT","publicTrade.ETHUSDT","publicTrade.SOLUSDT","publicTrade.BRETTUSDT","publicTrade.PENGUUSDT"]}
Batch 2 (500ms): {"op":"subscribe","args":["publicTrade.POPCATUSDT","publicTrade.WIFUSDT","publicTrade.SUIUSDT","publicTrade.ENAUSDT"]}
Batch 3 (1200ms):{"op":"subscribe","args":["orderbook.50.BTCUSDT","orderbook.50.ETHUSDT","orderbook.50.SOLUSDT","orderbook.50.BRETTUSDT","orderbook.50.PENGUUSDT"]}
Batch 4 (1800ms):{"op":"subscribe","args":["orderbook.50.POPCATUSDT","orderbook.50.WIFUSDT","orderbook.50.SUIUSDT","orderbook.50.ENAUSDT"]}
```

**Data Collected:** Trades (`publicTrade.*`) + OrderBook depth-50 (`orderbook.50.*`)

---

### 20. Bitget
**Tier:** 2 | **Batch:** 2 | **Native:** WebSocket

**WebSocket URL:**
```
wss://ws.bitget.com/v2/ws/public
```
**Custom Ping:** `"ping"` string every 30s

**Symbol Format:** UPPERCASE no separator — `BTCUSDT`, `BRETTUSDT`, etc. (used as `instId` inside `SPOT` instType)

**Subscription Message (single message on `onOpen`):**
```json
{
  "op": "subscribe",
  "args": [
    {"instType": "SPOT", "channel": "trade",  "instId": "BTCUSDT"},
    {"instType": "SPOT", "channel": "books5", "instId": "BTCUSDT"},
    {"instType": "SPOT", "channel": "trade",  "instId": "ETHUSDT"},
    {"instType": "SPOT", "channel": "books5", "instId": "ETHUSDT"},
    ... (all pairs interleaved)
  ]
}
```
Pairs: `BTCUSDT ETHUSDT SOLUSDT BTCUSDC ETHUSDC SOLUSDC BRETTUSDT PENGUUSDT POPCATUSDT WIFUSDT SUIUSDT ENAUSDT`

**CCXT Config:** `ccxtId: bitget` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (12):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, BRETT/USDT, PENGU/USDT, WIF/USDT, SUI/USDT, SUI/USDC, ENA/USDT`

**Data Collected:** Trades (`arg.channel=trade`) + OrderBook top-5 (`arg.channel=books5`)

---

### 21. Gemini
**Tier:** 2 | **Batch:** 3 | **Native:** WebSocket

**WebSocket URL:**
```
wss://api.gemini.com/v2/marketdata
```
**Required Header:** `Origin: https://exchange.gemini.com` (mandatory for WS handshake)
**Stale Timeout:** 90s (Gemini sends heartbeats every 5s — wider stale window)

**Symbol Format:** UPPERCASE no separator — `BTCUSD`, `ETHUSDT`, `SOLUSDC`, etc.

**Subscription Message (single message on `onOpen`):**
```json
{
  "type": "subscribe",
  "subscriptions": [{
    "name": "l2",
    "symbols": [
      "BTCUSD","ETHUSD","SOLUSD",
      "BTCUSDT","ETHUSDT",
      "BTCUSDC","ETHUSDC","SOLUSDC",
      "PENGUUSDC","PENGUUSD",
      "POPCATUSDC","POPCATUSD",
      "WIFUSDC","WIFUSD"
    ]
  }]
}
```
**Channel:** `l2` = Level 2 order book + trade data in single stream

**CCXT Config:** `ccxtId: gemini` | `skipTicker: true` (`watchTicker` = notSupported; `watchTrades` + `watchOrderBook` work fine, adds ~68K/min)

**CCXT Pro/REST Pairs (14):**
`BTC/USD, ETH/USD, SOL/USD, BTC/USDT, ETH/USDT, BTC/USDC, ETH/USDC, SOL/USDC, PENGU/USDC, PENGU/USD, POPCAT/USDC, POPCAT/USD, WIF/USDC, WIF/USD`

**Data Collected:** Trades (`type=l2_updates` with `trades[]`) + OrderBook (`type=l2_updates` with `changes[]`)

---

### 22. Binance.US
**Tier:** 2 | **Batch:** 3 | **Native:** WebSocket

**WebSocket URL:**
```
wss://stream.binance.us:9443/stream
```

**Symbol Format:** lowercase no separator in stream names — `btcusdt@trade`, `ethusd@depth5@1000ms`

**Subscription Message (sent on `onOpen`):**
```json
{
  "method": "SUBSCRIBE",
  "params": [
    "btcusdt@trade","ethusdt@trade","solusdt@trade","btcusdc@trade","ethusdc@trade",
    "solusdc@trade","btcusd@trade","ethusd@trade","solusd@trade",
    "btcusdt@depth5@1000ms","ethusdt@depth5@1000ms","solusdt@depth5@1000ms",
    "btcusdc@depth5@1000ms","ethusdc@depth5@1000ms","solusdc@depth5@1000ms",
    "btcusd@depth5@1000ms","ethusd@depth5@1000ms","solusd@depth5@1000ms",
    "brettusdt@trade","penguusdt@trade","popcatusdt@trade","wifusdt@trade",
    "suiusdt@trade","suiusd@trade","enausdt@trade",
    "brettusdt@depth5@1000ms","penguusdt@depth5@1000ms","popcatusdt@depth5@1000ms",
    "wifusdt@depth5@1000ms","suiusdt@depth5@1000ms","suiusd@depth5@1000ms",
    "enausdt@depth5@1000ms"
  ],
  "id": 1
}
```

**CCXT Config:** `ccxtId: binanceus` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (16):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, BTC/USD, ETH/USD, SOL/USD, BRETT/USDT, PENGU/USDT, POPCAT/USDT, WIF/USDT, SUI/USDT, SUI/USD, ENA/USDT`

**Data Collected:** Trades (`e=trade`) + OrderBook depth-5 at 1000ms (`lastUpdateId + bids`)

---

### 23. MEXC
**Tier:** 2 | **Batch:** 3 | **Native:** REST polling (no Native WS — REST mode)

**REST API Base:** `https://api.mexc.com`

**Endpoints polled (every interval via `runREST()`):**
```
GET https://api.mexc.com/api/v3/trades?symbol={SYMBOL}&limit=20    (trades)
GET https://api.mexc.com/api/v3/depth?symbol={SYMBOL}&limit=5      (orderbook)
```
**Symbol Format:** UPPERCASE no separator — `BTCUSDT`, `ETHUSDC`, etc.

Symbols polled: `BTCUSDT ETHUSDT SOLUSDT BTCUSDC ETHUSDC SOLUSDC BRETTUSDT PENGUUSDT POPCATUSDT WIFUSDT SUIUSDT ENAUSDT`

**CCXT Config:** `ccxtId: mexc` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (16):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, PENGU/USDT, PENGU/USDC, POPCAT/USDT, POPCAT/USDC, WIF/USDT, WIF/USDC, SUI/USDT, SUI/USDC, ENA/USDT, ENA/USDC`

**Data Collected:** Trades + OrderBook via Direct REST polling + CCXT

---

### 24. CoinEx
**Tier:** 3 | **Batch:** 3 | **Native:** WebSocket (gzip compressed)

**WebSocket URL:**
```
wss://socket.coinex.com/v2/spot
```
**Compression:** gzip | **Ping:** `{"method":"server.ping","params":{},"id":1}` every 15s

**Symbol Format:** UPPERCASE no separator — `BTCUSDT`, `BRETTUSDT`, etc.

**Subscription Messages (sent on `onOpen`):**
```json
{
  "method": "deals.subscribe",
  "params": {"market_list": ["BTCUSDT","ETHUSDT","SOLUSDT","BTCUSDC","ETHUSDC","SOLUSDC","BRETTUSDT","PENGUUSDT","POPCATUSDT","WIFUSDT","SUIUSDT","ENAUSDT"]},
  "id": 2
}
{
  "method": "depth.subscribe",
  "params": {"market_list": [["BTCUSDT",5,"0",false],["ETHUSDT",5,"0",false], ... all 12 pairs]},
  "id": 3
}
```

**Server Ping:** Server sends `{"method":"server.ping"}` → respond `{"method":"server.pong","params":{},"id":1}`

**CCXT Config:** `ccxtId: coinex` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (15):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, BRETT/USDT, PENGU/USDT, POPCAT/USDT, WIF/USDT, WIF/USDC, SUI/USDT, SUI/USDC, ENA/USDT, ENA/USDC`

**Data Collected:** Trades (`method=deals.update`) + OrderBook (`method=depth.update`)

---

### 25. LBank
**Tier:** 3 | **Batch:** 3 | **Native:** WebSocket

**WebSocket URLs:**
```
wss://www.lbkex.net/ws/V2/     (primary)
wss://www.lbkex.com/ws/V2/     (fallback)
```
**Stale Timeout:** 45s

**REST Fallback URL:**
```
https://api.lbkex.com/v2/trades.do?symbol={symbol}&size=5
```
Symbols: `btc_usdt eth_usdt sol_usdt brett_usdt pengu_usdt popcat_usdt wif_usdt sui_usdt ena_usdt`

**Symbol Format:** lowercase underscore — `btc_usdt`, `eth_usdc`, etc.

**Subscription (one per pair per stream, sent on `onOpen`):**
```json
{"action": "subscribe", "subscribe": "trade", "pair": "btc_usdt"}
{"action": "subscribe", "subscribe": "depth", "pair": "btc_usdt", "depth": 10}
```
Repeated for: `btc_usdt eth_usdt sol_usdt btc_usdc eth_usdc sol_usdc brett_usdt pengu_usdt popcat_usdt wif_usdt sui_usdt ena_usdt`

**Server Ping/Pong:** Server sends `{"action":"ping","ping":"..."}` → respond `{"action":"pong","pong":"..."}`

**CCXT Config:** `ccxtId: lbank` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (12):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, BRETT/USDT, PENGU/USDT, POPCAT/USDT, WIF/USDT, SUI/USDT, ENA/USDT`

**Data Collected:** Trades (`subscribe=trade`) + OrderBook depth-10 (`subscribe=depth`)

---

### 26. BitMart
**Tier:** 3 | **Batch:** 3 | **Native:** WebSocket (inflate compressed)

**WebSocket URL:**
```
wss://ws-manager-compress.bitmart.com/api?protocol=1.1
```
**Compression:** inflate | **Ping:** `"ping"` string every 10s

**Symbol Format:** Underscore separator — `BTC_USDT`, `ETH_USDC`, etc.

**Subscription Message (single message on `onOpen`):**
```json
{
  "op": "subscribe",
  "args": [
    "spot/trade:BTC_USDT",  "spot/depth5:BTC_USDT",
    "spot/trade:ETH_USDT",  "spot/depth5:ETH_USDT",
    "spot/trade:SOL_USDT",  "spot/depth5:SOL_USDT",
    "spot/trade:BTC_USDC",  "spot/depth5:BTC_USDC",
    "spot/trade:ETH_USDC",  "spot/depth5:ETH_USDC",
    "spot/trade:SOL_USDC",  "spot/depth5:SOL_USDC",
    "spot/trade:BRETT_USDT","spot/depth5:BRETT_USDT",
    "spot/trade:PENGU_USDT","spot/depth5:PENGU_USDT",
    "spot/trade:POPCAT_USDT","spot/depth5:POPCAT_USDT",
    "spot/trade:WIF_USDT",  "spot/depth5:WIF_USDT",
    "spot/trade:SUI_USDT",  "spot/depth5:SUI_USDT",
    "spot/trade:ENA_USDT",  "spot/depth5:ENA_USDT"
  ]
}
```

**CCXT Config:** `ccxtId: bitmart` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (12):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, BRETT/USDT, PENGU/USDT, POPCAT/USDT, WIF/USDT, SUI/USDT, ENA/USDT`

**Data Collected:** Trades (`table=spot/trade`) + OrderBook depth-5 (`table=spot/depth5`)

---

### 27. Pionex
**Tier:** 3 | **Batch:** 3 | **Native:** WebSocket (no CCXT)

**WebSocket URL:**
```
wss://ws.pionex.com/wsPub
```
*(ws2.pionex.com removed — DNS ENOTFOUND)*

**Symbol Format:** Underscore separator — `BTC_USDT`, `SUI_USDT`, etc.

**Subscription (staggered 200ms per pair on `onOpen` to avoid rejection):**
```json
{"op": "SUBSCRIBE", "topic": "TRADE", "symbol": "BTC_USDT"}
{"op": "SUBSCRIBE", "topic": "DEPTH", "symbol": "BTC_USDT", "limit": 5}
```
Repeated (with 200ms delay each) for: `BTC_USDT ETH_USDT SOL_USDT PENGU_USDT WIF_USDT SUI_USDT ENA_USDT`
*(BRETT_USDT, POPCAT_USDT not listed on Pionex)*

**Server Ping:** Server sends `{"op":"PING"}` → respond `{"op":"PONG"}`

**Data Collected:** Trades (`topic=TRADE`) + OrderBook (`topic=DEPTH`)

---

### 28. Poloniex
**Tier:** 3 | **Batch:** 3 | **Native:** WebSocket

**WebSocket URL:**
```
wss://ws.poloniex.com/ws/public
```
*(ws2.poloniex.com removed — DNS ENOTFOUND on Windows in v9.5)*
**Ping:** `{"event":"ping"}` every 20s

**Symbol Format:** Underscore separator — `BTC_USDT`, `ETH_USDC`, etc.

**Subscription Messages (sent on `onOpen` — 2 messages):**
```json
{"event":"subscribe","channel":["trades"],"symbols":["BTC_USDT","ETH_USDT","SOL_USDT","BTC_USDC","ETH_USDC","BRETT_USDT","POPCAT_USDT","WIF_USDT","SUI_USDT","ENA_USDT"]}
{"event":"subscribe","channel":["book"],  "symbols":["BTC_USDT","ETH_USDT","SOL_USDT","BTC_USDC","ETH_USDC","BRETT_USDT","POPCAT_USDT","WIF_USDT","SUI_USDT","ENA_USDT"]}
```
*(PENGU_USDT removed — NOT_LISTED on Poloniex, verified 2026-02-27)*

**CCXT Config:** `ccxtId: poloniex` | `skipPro: true` (142 timeouts + typeErr in v9.5) | CCXT REST active

**CCXT REST Pairs (10):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, BRETT/USDT, POPCAT/USDT, WIF/USDT, SUI/USDT, ENA/USDT`

**Data Collected:** Trades (`channel=trades`) + OrderBook (`channel=book`)

---

### 29. HitBTC
**Tier:** 3 | **Batch:** 3 | **Native:** WebSocket

**WebSocket URL:**
```
wss://api.hitbtc.com/api/3/ws/public
```
**Ping:** `{"method":"server.ping","params":{},"id":99}` every 20s

**Symbol Format:** UPPERCASE no separator — `BTCUSDT`, `BRETTUSDT`, etc.

**Subscription Messages (sent on `onOpen` — 2 messages):**
```json
{
  "ch": "trades",
  "method": "subscribe",
  "params": {"symbols": ["BTCUSDT","ETHUSDT","SOLUSDT","BTCUSDC","ETHUSDC","SOLUSDC","BRETTUSDT","PENGUUSDT","POPCATUSDT","WIFUSDT","SUIUSDT","ENAUSDT"]},
  "id": 1
}
{
  "ch": "orderbook/full",
  "method": "subscribe",
  "params": {"symbols": ["BTCUSDT","ETHUSDT", ... same 12], "limit": 5},
  "id": 2
}
```
**Note (v9.7):** HitBTC sends an initial `snapshot` message then `update` messages — both are processed for trades.

**CCXT Config:** `ccxtId: hitbtc` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (12):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, BRETT/USDT, PENGU/USDT, POPCAT/USDT, WIF/USDT, SUI/USDT, ENA/USDT`

**Data Collected:** Trades (`ch=trades`) + OrderBook full (`ch=orderbook/full`)

---

### 30. BTSE
**Tier:** 3 | **Batch:** 4 | **Native:** WebSocket + Direct REST Poll (dual WS connections)

**WebSocket URLs (2 separate connections):**
```
wss://ws.btse.com/ws/spot        (trades — primary)
wss://ws.btse.com/ws/oss/spot    (order book — secondary)
```
*(ws.btse.io removed — DNS ENOTFOUND)*
**Ping:** `"ping"` string every 30s

**REST Poll URL (every 30s, runs in parallel):**
```
https://api.btse.com/spot/api/v3.2/trades?symbol={SYMBOL}&count=5
```
Symbols: `BTC-USD ETH-USD SOL-USD BTC-USDT ETH-USDT SOL-USDT BRETT-USDT PENGU-USDT WIF-USDT SUI-USDT ENA-USDT`

**Symbol Format:** Hyphen separator — `BTC-USD`, `ETH-USDT`, `PENGU-USDC`, etc.

**Trade WS Subscription (`onOpen` for `ws.btse.com/ws/spot`):**
```json
{
  "op": "subscribe",
  "args": [
    "tradeHistoryApi:BTC-USD","tradeHistoryApi:ETH-USD","tradeHistoryApi:SOL-USD",
    "tradeHistoryApi:BTC-USDT","tradeHistoryApi:ETH-USDT","tradeHistoryApi:SOL-USDT",
    "tradeHistoryApi:BTC-USDC","tradeHistoryApi:ETH-USDC","tradeHistoryApi:SOL-USDC",
    "tradeHistoryApi:BRETT-USDT",
    "tradeHistoryApi:PENGU-USDT","tradeHistoryApi:PENGU-USD","tradeHistoryApi:PENGU-USDC",
    "tradeHistoryApi:POPCAT-USDT","tradeHistoryApi:POPCAT-USD","tradeHistoryApi:POPCAT-USDC",
    "tradeHistoryApi:WIF-USDT","tradeHistoryApi:WIF-USD","tradeHistoryApi:WIF-USDC",
    "tradeHistoryApi:SUI-USDT","tradeHistoryApi:SUI-USD","tradeHistoryApi:SUI-USDC",
    "tradeHistoryApi:ENA-USDT","tradeHistoryApi:ENA-USD","tradeHistoryApi:ENA-USDC"
  ]
}
```

**OB WS Subscription (`onOpen` for `ws.btse.com/ws/oss/spot`):**
```json
{
  "op": "subscribe",
  "args": ["update:BTC-USD_0","update:ETH-USD_0","update:SOL-USD_0", ... (same pairs with `_0` suffix)]
}
```
*(OB topic format: `update:{SYMBOL}_0`)*

**Data Collected:** Trades (`topic` starts with `tradeHistoryApi`) + OrderBook (`topic` starts with `update:`)

---

### 31. Biconomy
**Tier:** 3 | **Batch:** 4 | **Native:** WebSocket (no CCXT)

**WebSocket URL:**
```
wss://bei.biconomy.com/ws
```
**Ping:** `{"method":"server.ping","params":[],"id":5160}` every 30s

**Symbol Format:** Underscore separator — `BTC_USDT`, `ETH_USDC`, etc.

**Subscription (one per pair per method, staggered IDs, sent on `onOpen`):**
```json
{"method": "depth.subscribe",  "params": ["BTC_USDT", 5, "0"], "id": 10}
{"method": "deals.subscribe",  "params": ["BTC_USDT"],          "id": 11}
{"method": "depth.subscribe",  "params": ["ETH_USDT", 5, "0"], "id": 12}
{"method": "deals.subscribe",  "params": ["ETH_USDT"],          "id": 13}
... (12 pairs total)
```
Pairs: `BTC_USDT ETH_USDT SOL_USDT BTC_USDC ETH_USDC SOL_USDC BRETT_USDT PENGU_USDT POPCAT_USDT WIF_USDT SUI_USDT ENA_USDT`

**Data Collected:** Trades (`method=deals.update`) + OrderBook (`method=depth.update`)

---

### 32. Hotcoin
**Tier:** 3 | **Batch:** 4 | **Native:** WebSocket + Parallel REST (ws+rest mode)

**WebSocket URL:**
```
wss://wss.hotcoinfin.com/trade/multiple
```
**Compression:** gzip | **Stale Timeout:** 60s

**Direct REST API (parallel polling every 10s):**
```
https://api.hotcoinfin.com/v1/market/trade?symbol={symbol}&size=10   (trades)
https://api.hotcoinfin.com/v1/market/depth?symbol={symbol}&type=step0 (orderbook)
```
Symbols: `btc_usdt eth_usdt sol_usdt brett_usdt pengu_usdt popcat_usdt wif_usdt sui_usdt ena_usdt btc_usdc eth_usdc sol_usdc`

**Symbol Format:** lowercase underscore — `btc_usdt`, etc.

**WS Subscription (one per pair per channel, sent on `onOpen`):**
```json
{"sub": "market.btc_usdt.trade.depth"}
{"sub": "market.btc_usdt.trade.detail"}
```
Repeated for all 12 pairs.

**Server Ping:** Server sends `{"ping": N}` → respond `{"pong": N}`

**Data Collected:** Trades (`ch` contains `trade.detail`) + OrderBook (`ch` contains `trade.depth`) via both WS and REST

---

### 33. NovaEx
**Tier:** 3 | **Batch:** 4 | **Native:** WebSocket (WOO X white-label — no CCXT)

**WebSocket URL:**
```
wss://wss.woox.io/ws/stream/OqdphuyIYbng-t001
```
*(This is a WOO X white-label — uses woox.io endpoint with NovaEx app-id `OqdphuyIYbng-t001`)*
**Ping:** `{"event":"ping"}` every 9s

**Symbol Format:** `SPOT_{BASE}_{QUOTE}` — `SPOT_BTC_USDT`, `SPOT_ETH_USDC`, etc.

**Subscription (one per pair per channel, sent on `onOpen`):**
```json
{"id": "SPOT_BTC_USDTo", "event": "subscribe", "topic": "SPOT_BTC_USDT@orderbook"}
{"id": "SPOT_BTC_USDTt", "event": "subscribe", "topic": "SPOT_BTC_USDT@trade"}
```
Repeated for: `SPOT_BTC_USDT SPOT_ETH_USDT SPOT_SOL_USDT SPOT_BTC_USDC SPOT_ETH_USDC`

**Data Collected:** Trades (`@trade` topic) + OrderBook (`@orderbook` topic)

---

### 34. FameEX
**Tier:** 3 | **Batch:** 4 | **Native:** WebSocket (no CCXT)

**WebSocket URL:**
```
wss://wsapi.fameex.com/v1/ws/stream/public
```
**Note (v9.7):** REST API `api.fameex.com` returns 404 on all endpoints (deprecated) — WS only.

**Symbol Format:** lowercase no separator — `btcusdt`, `ethusdc`, etc. (channel name format)

**Subscription (one per pair per channel, sent on `onOpen`):**
```json
{"event": "sub", "params": {"channel": "market_btcusdt_trade_detail"}}
{"event": "sub", "params": {"channel": "market_btcusdt_depth_step0"}}
```
Repeated for: `btcusdt ethusdt solusdt btcusdc ethusdc solusdc wifusdt`

**Server Ping/Pong:** Server sends `{"ping": N}` → respond `{"pong": N}`

**Data Collected:** Trades (`channel` contains `_trade`) + OrderBook (`channel` contains `_depth`)

---

### 35. Websea
**Tier:** 3 | **Batch:** 4 | **Native:** WebSocket (no CCXT)

**WebSocket URL:**
```
wss://oapi.websea.com/ws/v1/spot/market
```

**Symbol Format:** Hyphen separator — `BTC-USDT`, `WIF-USDT`, etc.

**Subscription (one per pair per channel, sent on `onOpen`):**
```json
{"op": "sub", "channel": "trade", "symbol": "BTC-USDT"}
{"op": "sub", "channel": "depth", "symbol": "BTC-USDT"}
```
Repeated for: `BTC-USDT ETH-USDT SOL-USDT PENGU-USDT WIF-USDT SUI-USDT ENA-USDT`

**Server Ping/Pong:** Server sends `{"op":"ping"}` or `{"ping":...}` → respond `{"op":"pong"}`

**Data Collected:** Trades (`channel=trade`) + OrderBook (`channel=depth`)

---

### 36. Bullish
**Tier:** 3 | **Batch:** 4 | **Native:** WebSocket + Direct REST Poll (OB)

**WebSocket URL:**
```
wss://api.exchange.bullish.com/trading-api/v1/market-data/trades
```

**REST Poll URL (every 45s for order books — reduced from 20s to limit 429 errors):**
```
https://api.exchange.bullish.com/trading-api/v1/markets/{SYMBOL}/orderbook/hybrid
```
Symbols: `BTCUSDC ETHUSDC SOLUSDC BTCUSDT ETHUSDT SOLUSDT BTCUSD ETHUSD SOLUSD PENGUUSDT PENGUUSDC WIFUSDC SUIUSDC`

**Symbol Format:** UPPERCASE no separator — `BTCUSDC`, `ETHUSDT`, etc.

**WS Subscription (one per symbol, sent on `onOpen`):**
```json
{
  "jsonrpc": "2.0",
  "type": "command",
  "method": "subscribe",
  "params": {"topic": "anonymousTrades", "symbol": "BTCUSDC"},
  "id": "BTCUSDC_t"
}
```
Repeated for all 13 symbols.

**Server Ping/Pong:** Server sends `{"type":"ping","id":"..."}` → respond `{"type":"pong","id":"..."}`

**CCXT Config:** `ccxtId: bullish` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (13):**
`BTC/USDC, ETH/USDC, SOL/USDC, BTC/USDT, ETH/USDT, SOL/USDT, BTC/USD, ETH/USD, SOL/USD, PENGU/USDT, PENGU/USDC, WIF/USDC, SUI/USDC`

**Data Collected:** Trades (`data.trades + data.symbol`) via WS + OrderBook via REST poll

---

### 37. Darkex
**Tier:** 3 | **Batch:** 4 | **Native:** WebSocket (gzip compressed, no CCXT)

**WebSocket URL:**
```
wss://ws.darkex.com/kline-api/ws
```
**Compression:** gzip | **Stale Timeout:** 60s

**Symbol Format:** lowercase no separator in channel names — `btcusdt`, `wifusdt`, etc.

**Subscription (one per pair per channel, sent on `onOpen`):**
```json
{"event": "sub", "params": {"channel": "market_btcusdt_depth_step0"}}
{"event": "sub", "params": {"channel": "market_btcusdt_trade_ticker"}}
```
Repeated for: `btcusdt ethusdt popcatusdt wifusdt suiusdt`

**Server Ping/Pong:** Server sends `{"ping": N}` → respond `{"pong": N}`

**Data Collected:** Trades (`channel` contains `_trade_`) + OrderBook (`channel` contains `_depth_`)

---

### 38. Bitrue
**Tier:** 3 | **Batch:** 4 | **Native:** WebSocket (gzip) + Direct REST Poll

**WebSocket URLs:**
```
wss://ws.bitrue.com/market/ws         (primary)
wss://wsapi.bitrue.com/kline-api/ws   (fallback)
```
**Compression:** gzip

**REST Poll URL (every 10s, runs in parallel):**
```
https://openapi.bitrue.com/api/v1/trades?symbol={SYMBOL}&limit=5
```
Symbols: `BTCUSDT ETHUSDT SOLUSDT BTCUSDC ETHUSDC SOLUSDC BRETTUSDT PENGUUSDT POPCATUSDT WIFUSDT SUIUSDT ENAUSDT`

**Symbol Format:** UPPERCASE no separator — `BTCUSDT`, `BRETTUSDT`, etc.

**WS Subscription (one per pair, for depth only, sent on `onOpen`):**
```json
{"event": "sub", "params": {"cb_id": "BTCUSDT", "channel": "market_BTCUSDT_depth_step0"}}
```
Repeated for all 12 pairs.

**Server Ping/Pong:** Server sends `{"event":"ping"}` → respond `{"event":"pong"}`

**CCXT Config:** `ccxtId: bitrue` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (16):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, BRETT/USDT, PENGU/USDT, PENGU/USDC, POPCAT/USDT, WIF/USDT, WIF/USDC, SUI/USDT, SUI/USDC, ENA/USDT, ENA/USDC`

**Data Collected:** OrderBook via WS (`channel` contains `depth`) + Trades via REST poll

---

### 39. BloFin
**Tier:** 3 | **Batch:** 4 | **Native:** WebSocket

**WebSocket URL:**
```
wss://openapi.blofin.com/ws/public
```
**Ping:** `"ping"` string every 25s

**Symbol Format:** Hyphen separator — `BTC-USDT`, `BTC-USD`, etc.

**Subscription Messages (sent on `onOpen` — 2 messages):**
```json
{
  "op": "subscribe",
  "args": [
    {"channel": "trades", "instId": "BTC-USDT"},
    {"channel": "trades", "instId": "ETH-USDT"},
    {"channel": "trades", "instId": "SOL-USDT"},
    {"channel": "trades", "instId": "BTC-USDC"},
    {"channel": "trades", "instId": "ETH-USDC"},
    {"channel": "trades", "instId": "SOL-USDC"},
    {"channel": "trades", "instId": "BTC-USD"},
    {"channel": "trades", "instId": "ETH-USD"},
    {"channel": "trades", "instId": "SOL-USD"}
  ]
}
{
  "op": "subscribe",
  "args": [
    {"channel": "books", "instId": "BTC-USDT"},
    ... (same 9 pairs with channel "books")
  ]
}
```

**CCXT Config:** `ccxtId: blofin` | CCXT Pro + CCXT REST active
*(Note: BloFin uses perpetual contract IDs in CCXT — `BTC/USDT:USDT`, `BTC/USD:USD`, etc.)*

**CCXT Pro/REST Pairs (8):**
`BTC/USDT:USDT, ETH/USDT:USDT, SOL/USDT:USDT, BTC/USDC:USDC, SOL/USDC:USDC, BTC/USD:USD, ETH/USD:USD, SOL/USD:USD`

**Data Collected:** Trades (`arg.channel=trades`) + OrderBook (`arg.channel=books`)

---

### 40. DigiFinex
**Tier:** 3 | **Batch:** 5 | **Native:** WebSocket (inflate compressed)

**WebSocket URL:**
```
wss://openapi.digifinex.com/ws/v1/
```
**Compression:** inflate | **Custom Ping:** `{"id":0,"method":"server.ping","params":[]}` every 30s

**Symbol Format:** Underscore separator — `BTC_USDT`, `ETH_USDC`, etc.

**Subscription Messages (sent on `onOpen`):**

Trades (all pairs):
```json
{"id":1, "method":"trades.subscribe", "params":["BTC_USDT","ETH_USDT","SOL_USDT","BTC_USDC","ETH_USDC","SOL_USDC","BRETT_USDT"]}
```

Order Book (one per pair via `extra` connections):
```json
{"id":2, "method":"depth.subscribe", "params":["BTC_USDT", 5, "0"]}   (main connection)
{"id":3, "method":"depth.subscribe", "params":["ETH_USDT", 5, "0"]}   (extra connection 1)
{"id":4, "method":"depth.subscribe", "params":["SOL_USDT", 5, "0"]}   (extra connection 2)
... one depth subscription per separate WS connection
```

**CCXT Config:** `ccxtId: digifinex` | `skipPro: true` | CCXT REST active

**CCXT REST Pairs (6):**
`BTC/USDT, BTC/USDC, ETH/USDT, ETH/USDC, SOL/USDT, BRETT/USDT`

**Data Collected:** Trades (`method=trades.update`) + OrderBook (`method=depth.update`)

---

### 41. EXMO
**Tier:** 3 | **Batch:** 5 | **Native:** WebSocket

**WebSocket URLs:**
```
wss://ws-api.exmo.com/v1/public    (primary)
wss://ws-api.exmo.me/v1/public     (fallback 1)
wss://ws-api.exmo.io/v1/public     (fallback 2)
```
**Ping:** `{"method":"ping","id":99}` every 20s | **Stale Timeout:** 45s

**Symbol Format:** Underscore separator — `BTC_USDT`, `PENGU_USDC`, etc.

**Subscription Message (single message on `onOpen`):**
```json
{
  "method": "subscribe",
  "topics": [
    "spot/trades:BTC_USDT", "spot/trades:ETH_USDT", "spot/trades:SOL_USDT",
    "spot/trades:BTC_USDC", "spot/trades:ETH_USDC", "spot/trades:SOL_USDC",
    "spot/trades:BTC_USD",  "spot/trades:ETH_USD",
    "spot/trades:PENGU_USDT","spot/trades:PENGU_USDC",
    "spot/trades:WIF_USDT", "spot/trades:WIF_USDC",
    "spot/trades:SUI_USDT", "spot/trades:SUI_USDC",
    "spot/trades:ENA_USDT", "spot/trades:ENA_USDC",
    "spot/order_book_snapshots:BTC_USDT",  "spot/order_book_snapshots:ETH_USDT",
    "spot/order_book_snapshots:SOL_USDT",  "spot/order_book_snapshots:BTC_USDC",
    "spot/order_book_snapshots:ETH_USDC",  "spot/order_book_snapshots:SOL_USDC",
    "spot/order_book_snapshots:BTC_USD",   "spot/order_book_snapshots:ETH_USD",
    "spot/order_book_snapshots:PENGU_USDT","spot/order_book_snapshots:PENGU_USDC",
    "spot/order_book_snapshots:WIF_USDT",  "spot/order_book_snapshots:WIF_USDC",
    "spot/order_book_snapshots:SUI_USDT",  "spot/order_book_snapshots:SUI_USDC",
    "spot/order_book_snapshots:ENA_USDT",  "spot/order_book_snapshots:ENA_USDC"
  ]
}
```

**CCXT Config:** `ccxtId: exmo` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (16):**
`BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, BTC/USD, ETH/USD, PENGU/USDT, PENGU/USDC, WIF/USDT, WIF/USDC, SUI/USDT, SUI/USDC, ENA/USDT, ENA/USDC`

**Data Collected:** Trades (`topic` contains `trades`) + OrderBook snapshots (`topic` contains `order_book`)

---

### 42. CEX.IO
**Tier:** 3 | **Batch:** 5 | **Native:** REST polling only (no WS)

**REST API Base:** `https://cex.io`

**Endpoints polled (via `runREST()`):**
```
GET https://cex.io/api/trade_history/{BASE}/{QUOTE}/        (trades)
GET https://cex.io/api/order_book/{BASE}/{QUOTE}/?depth=5   (orderbook)
```
**Symbol Format:** Separate base/quote path segments — `BTC/USDT` pair → `cex.io/api/trade_history/BTC/USDT/`

Pairs polled: `BTC/USDT, ETH/USDT, SOL/USDT, BTC/USDC, ETH/USDC, SOL/USDC, BTC/USD, ETH/USD, SOL/USD, BRETT/USDT, BRETT/USDC, BRETT/USD, PENGU/USDT, PENGU/USD, POPCAT/USDT, POPCAT/USD, WIF/USDT, WIF/USDC, WIF/USD, SUI/USDT, SUI/USDC, SUI/USD, ENA/USDT, ENA/USDC, ENA/USD`

**CCXT Config:** `ccxtId: cex` | `skipPro: true` | CCXT REST active

**CCXT REST Pairs (25):** All 25 pairs listed above.

**Data Collected:** Trades + OrderBook via REST polling

---

### 43. OrangeX
**Tier:** 3 | **Batch:** 5 | **Native:** REST polling only (no WS, no CCXT)

**REST API Base:** `https://api.orangex.com`

**Endpoints polled:**
```
GET https://api.orangex.com/api/v1/public/get_last_trades_by_instrument?instrument_name={SYMBOL}-SPOT&count=5
GET https://api.orangex.com/api/v1/public/get_order_book?instrument_name={SYMBOL}-SPOT&depth=5
```
**Symbol Format:** Hyphen with `-SPOT` suffix — `BTC-USDT-SPOT`, `ETH-USDC-SPOT`, etc.

Pairs polled: `BTC-USDT, ETH-USDT, SOL-USDT, BTC-USDC, ETH-USDC`

**Data Collected:** Trades (`result.trades`) + OrderBook (`result.bids`) via REST

---

### 44. Azbit
**Tier:** 3 | **Batch:** 5 | **Native:** REST polling only (no WS, no CCXT)

**REST API Base:** `https://data.azbit.com`

**Endpoints polled:**
```
GET https://data.azbit.com/api/deals?currencyPairCode={SYMBOL}     (trades)
GET https://data.azbit.com/api/orderbook?currencyPairCode={SYMBOL}  (orderbook)
```
**Symbol Format:** Underscore separator — `BTC_USDT`, `ETH_USDC`, etc.

Pairs polled: `BTC_USDT ETH_USDT SOL_USDT BTC_USDC ETH_USDC SOL_USDC BRETT_USDT PENGU_USDT POPCAT_USDT WIF_USDT SUI_USDT ENA_USDT`

**Data Collected:** Trades (array response) + OrderBook (array response) via REST

---

### 45. BVOX (BitVenus)
**Tier:** 3 | **Batch:** 5 | **Native:** REST polling only (no WS, no CCXT)

**REST API Base:** `https://api.bitvenus.me`

**Endpoints polled:**
```
GET https://api.bitvenus.me/openapi/quote/v1/trades?symbol={SYMBOL}&limit=5   (trades)
GET https://api.bitvenus.me/openapi/quote/v1/depth?symbol={SYMBOL}&limit=5    (orderbook)
```
**Symbol Format:** UPPERCASE no separator — `BTCUSDT`, `ETHUSDT`, `SOLUSDT`

Pairs polled: `BTCUSDT ETHUSDT SOLUSDT`

**Data Collected:** Trades (array response) + OrderBook (`bids` field) via REST

---

### 46. Trubit Pro
**Tier:** 3 | **Batch:** 5 | **Native:** REST polling only (no WS, no CCXT)

**REST API Base:** `https://api-spot.trubit.com`

**Endpoints polled:**
```
GET https://api-spot.trubit.com/openapi/quote/v1/trades?symbol={SYMBOL}&limit=5   (trades)
GET https://api-spot.trubit.com/openapi/quote/v1/depth?symbol={SYMBOL}&limit=5    (orderbook)
```
**Symbol Format:** UPPERCASE no separator — `BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `BTCUSDC`, `ETHUSDC`

Pairs polled: `BTCUSDT ETHUSDT SOLUSDT BTCUSDC ETHUSDC`

**Data Collected:** Trades (array response) + OrderBook (`bids` field) via REST

---

### 47. BigONE
**Tier:** 3 | **Batch:** 5 | **Native:** REST polling only

**REST API Base:** `https://big.one`

**Endpoints polled:**
```
GET https://big.one/api/v3/asset_pairs/{SYMBOL}/trades   (trades)
GET https://big.one/api/v3/asset_pairs/{SYMBOL}/depth    (orderbook)
```
**Symbol Format:** Hyphen separator — `BTC-USDT`, `ETH-USDT`, `WIF-USDT`, etc.

Pairs polled: `BTC-USDT ETH-USDT SOL-USDT PENGU-USDT WIF-USDT SUI-USDT ENA-USDT`

**CCXT Config:** `ccxtId: bigone` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (7):**
`BTC/USDT, ETH/USDT, SOL/USDT, PENGU/USDT, WIF/USDT, SUI/USDT, ENA/USDT`

**Data Collected:** Trades (`data[]` array) + OrderBook (`data.bids`) via REST + CCXT

---

### 48. LATOKEN
**Tier:** 3 | **Batch:** 5 | **Native:** REST polling only (UUID-based API)

**REST API Base:** `https://api.latoken.com`

**Endpoints polled (using UUID asset IDs, not ticker symbols):**
```
GET https://api.latoken.com/v2/trade/history/{baseUUID}/{quoteUUID}   (trades)
GET https://api.latoken.com/v2/book/{baseUUID}/{quoteUUID}            (orderbook)
```
**Asset UUID Mapping:**
| Pair | Base UUID | Quote UUID |
|------|-----------|------------|
| BTC_USDT | `4f4a4e5e-7192-4e7e-9f78-d8f6e07c0001` | `0c3a106d-bde3-4c13-a26e-3fd2394529e5` |
| ETH_USDT | `620f2019-33c0-423b-8a9d-cde4d7f8ef7f` | `0c3a106d-bde3-4c13-a26e-3fd2394529e5` |
| SOL_USDT | `f5924e5b-3860-4a3c-94d0-6c3fd4999e73` | `0c3a106d-bde3-4c13-a26e-3fd2394529e5` |
| BRETT_USDT | `335f4e73-ec70-4fc7-a97c-935c399ad4cd` | `0c3a106d-...` |
| PENGU_USDT | `06775a0c-fb18-459b-b2eb-da8f028d6057` | `0c3a106d-...` |
| POPCAT_USDT | `2f685624-16d1-418a-b2b6-12524da9e203` | `0c3a106d-...` |
| WIF_USDT | `b8f1e53a-2dd6-4892-9e85-0bb6741096f9` | `0c3a106d-...` |
| SUI_USDT | `faad614d-2cce-4d21-8e1c-10790019e8d5` | `0c3a106d-...` |
| ENA_USDT | `7f2f7c8a-cc97-4f98-a289-ca763c858905` | `0c3a106d-...` |

**CCXT Config:** `ccxtId: latoken` | CCXT Pro + CCXT REST active

**CCXT Pro/REST Pairs (9):**
`BTC/USDT, ETH/USDT, SOL/USDT, BRETT/USDT, PENGU/USDT, POPCAT/USDT, WIF/USDT, SUI/USDT, ENA/USDT`

**Data Collected:** Trades (array response) + OrderBook (`bid`/`ask` arrays) via REST

---

### 49. Coinstore
**Tier:** 3 | **Batch:** 6 | **Native:** WebSocket (no CCXT)

**WebSocket URL:**
```
wss://ws.coinstore.com/s/ws
```

**REST Fallback URL:**
```
https://api.coinstore.com/api/v1/market/trade/{SYMBOL}?size=5
```
Symbols: `BTCUSDT ETHUSDT SOLUSDT PENGUUSDT WIFUSDT SUIUSDT ENAUSDT`

**Symbol Format (WS):** lowercase no separator — `btcusdt`, `ethusdc`, etc.

**Subscription Messages (sent on `onOpen`):**
```json
{"op": "SUB", "channel": ["btcusdt@trade","ethusdt@trade","solusdt@trade","btcusdc@trade","ethusdc@trade","solusdc@trade","penguusdt@trade","wifusdt@trade","suiusdt@trade","enausdt@trade"], "id": 1}
{"op": "SUB", "channel": ["btcusdt@depth@20","ethusdt@depth@20","solusdt@depth@20","btcusdc@depth@20","ethusdc@depth@20","solusdc@depth@20","penguusdt@depth@20","wifusdt@depth@20","suiusdt@depth@20","enausdt@depth@20"], "id": 2}
```

**Server Ping/Pong:** Server sends `{"ping":N}` or `{"action":"ping"}` → respond `{"op":"pong","epochMillis":{timestamp}}`

**Data Collected:** Trades (`T=trade`) + OrderBook (`T=depth`) via WS + REST fallback

---

### 50. GroveX
**Tier:** 3 | **Batch:** 6 | **Native:** WebSocket (gzip compressed, no CCXT)

**WebSocket URLs:**
```
wss://ws.grovex.io/kline-api/ws         (primary)
wss://openapi.grovex.io/kline-api/ws    (fallback)
```
**Compression:** gzip

**REST Fallback URL:**
```
https://openapi.grovex.io/open/api/market_dept?symbol=btcusdt&type=step0
```

**Symbol Format:** lowercase no separator in channel names — `btcusdt`, `brettusdt`, etc.

**Subscription (one per pair per channel, sent on `onOpen`):**
```json
{"event":"sub","params":{"channel":"market_btcusdt_depth_step0","cb_id":"btcusdtd","asks":150,"bids":150}}
{"event":"sub","params":{"channel":"market_btcusdt_trade_ticker","cb_id":"btcusdtt"}}
```
Repeated for: `btcusdt ethusdt solusdt btcusdc ethusdc solusdc brettusdt penguusdt popcatusdt wifusdt suiusdt enausdt`

**Server Ping/Pong:** Server sends `{"ping": N}` → respond `{"pong": N}`

**Data Collected:** Trades (`channel` contains `_trade_`) + OrderBook (`channel` contains `_depth_`)

---

### 51. CoinW
**Tier:** 3 | **Batch:** 6 | **Native:** REST polling only (no WS, no CCXT)

**REST API Base:** `https://api.coinw.com`

**Endpoints polled (8s interval):**
```
GET https://api.coinw.com/api/v1/public?command=returnTradeHistory&symbol={symbol}   (trades)
GET https://api.coinw.com/api/v1/public?command=returnOrderBook&symbol={symbol}      (orderbook)
```
**Symbol Format:** lowercase underscore — `btc_usdt`, `eth_usdc`, etc.

Pairs polled: `btc_usdt eth_usdt sol_usdt btc_usdc eth_usdc sol_usdc brett_usdt pengu_usdt popcat_usdt wif_usdt sui_usdt ena_usdt`

**Display Name Mapping:** lowercase keys → uppercase display (`btc_usdt` → `BTC_USDT`)

**Data Collected:** Trades (`code=200`, `data[]` array) + OrderBook (`code=200`, `data.bids/asks`) via REST

---

### 52. Batonex
**Tier:** 3 | **Batch:** 6 | **Native:** REST polling only (no WS, no CCXT)

**REST API Base:** `https://api.batonex.com`

**Endpoints polled:**
```
GET https://api.batonex.com/openapi/quote/v1/trades?symbol={SYMBOL}&limit=5   (trades)
GET https://api.batonex.com/openapi/quote/v1/depth?symbol={SYMBOL}&limit=5    (orderbook)
```
**Symbol Format:** UPPERCASE no separator — `BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `WIFUSDT`

Pairs polled: `BTCUSDT ETHUSDT SOLUSDT WIFUSDT`
*(Only 32 pairs on exchange — BRETT/PENGU/POPCAT/SUI/ENA not listed)*

**Data Collected:** Trades (array response) + OrderBook (`bids` field) via REST

---

### 53. CEEX
**Tier:** 3 | **Batch:** 6 | **Native:** WebSocket (no CCXT)

**WebSocket URL:**
```
wss://wsapi.ceex.com/openapi/quote/ws/v1
```

**REST Fallback URLs (polled when WS disconnected):**
```
https://api.ceex.com/openapi/quote/v1/trades?symbol={SYMBOL}&limit=5
https://api.ceex.com/openapi/quote/v1/depth?symbol={SYMBOL}&limit=10
```
Symbols: `BTCUSDT ETHUSDT SOLUSDT WIFUSDT SUIUSDT ENAUSDT`

**Symbol Format:** UPPERCASE no separator — `BTCUSDT`, `SUIUSDT`, etc.

**Subscription (one per pair per topic, sent on `onOpen`):**
```json
{"symbol": "BTCUSDT", "topic": "depth", "event": "sub"}
{"symbol": "BTCUSDT", "topic": "trade", "event": "sub"}
```
Repeated for: `BTCUSDT ETHUSDT SOLUSDT WIFUSDT SUIUSDT ENAUSDT`

**Server Ping/Pong:** Server sends `"ping"` string or `{"ping":N}` → respond `"pong"` or `{"pong":N}`

**Data Collected:** Trades (`topic=trade`, `data[]`) + OrderBook (`topic=depth`, `data[]`) via WS + REST fallback

---

## CCXT Pro & CCXT REST Summary

All exchanges with a `ccxtId` automatically run **both** CCXT Pro (WS) and CCXT REST polling in parallel, unless flags disable them:

| Exchange | ccxtId | skipPro | skipTicker | CCXT Pro Methods |
|----------|--------|---------|-----------|-----------------|
| Binance | binance | YES | — | CCXT REST only |
| Coinbase | coinbase | — | — | watchTrades + watchOrderBook + watchTicker |
| Kraken | kraken | — | — | watchTrades + watchOrderBook + watchTicker |
| KuCoin | kucoin | — | — | watchTrades + watchOrderBook + watchTicker |
| OKX | okx | — | — | watchTrades + watchOrderBook + watchTicker |
| Bybit | bybit | — | — | watchTrades + watchOrderBook + watchTicker |
| Bitfinex | bitfinex | YES | — | CCXT REST only |
| Gate.io | gateio | — | — | watchTrades + watchOrderBook + watchTicker |
| HTX | htx | YES | — | CCXT REST only |
| WOO X | woo | — | — | watchTrades + watchOrderBook + watchTicker |
| Crypto.com | cryptocom | — | — | watchTrades + watchOrderBook + watchTicker |
| Bitstamp | bitstamp | — | — | watchTrades + watchOrderBook + watchTicker |
| WhiteBIT | whitebit | YES | — | CCXT REST only |
| AscendEX | ascendex | — | — | watchTrades + watchOrderBook + watchTicker |
| BingX | bingx | — | — | watchTrades + watchOrderBook + watchTicker |
| Toobit | toobit | — | — | watchTrades + watchOrderBook + watchTicker |
| Deepcoin | deepcoin | YES | — | CCXT REST only |
| XT.com | xt | — | — | watchTrades + watchOrderBook + watchTicker |
| Bitget | bitget | — | — | watchTrades + watchOrderBook + watchTicker |
| Gemini | gemini | — | YES | watchTrades + watchOrderBook (no ticker) |
| Binance.US | binanceus | — | — | watchTrades + watchOrderBook + watchTicker |
| MEXC | mexc | — | — | watchTrades + watchOrderBook + watchTicker |
| CoinEx | coinex | — | — | watchTrades + watchOrderBook + watchTicker |
| LBank | lbank | — | — | watchTrades + watchOrderBook + watchTicker |
| BitMart | bitmart | — | — | watchTrades + watchOrderBook + watchTicker |
| Poloniex | poloniex | YES | — | CCXT REST only |
| HitBTC | hitbtc | — | — | watchTrades + watchOrderBook + watchTicker |
| Bullish | bullish | — | — | watchTrades + watchOrderBook + watchTicker |
| Bitrue | bitrue | — | — | watchTrades + watchOrderBook + watchTicker |
| BloFin | blofin | — | — | watchTrades + watchOrderBook + watchTicker |
| DigiFinex | digifinex | YES | — | CCXT REST only |
| EXMO | exmo | — | — | watchTrades + watchOrderBook + watchTicker |
| CEX.IO | cex | YES | — | CCXT REST only |
| BigONE | bigone | — | — | watchTrades + watchOrderBook + watchTicker |
| LATOKEN | latoken | — | — | watchTrades + watchOrderBook + watchTicker |

**Exchanges with NO CCXT (native-only):**
Zoomex, Pionex, BTSE, Biconomy, Hotcoin, NovaEx, FameEX, Websea, Darkex, OrangeX, Azbit, BVOX, Trubit Pro, Coinstore, GroveX, CoinW, Batonex, CEEX

---

## Data Type Summary

| Data Type | Channels/Methods Used | Storage Key |
|-----------|----------------------|-------------|
| **Trades** | WS trade channels, CCXT `watchTrades`, REST `fetchTrades` | `trades` counter |
| **Order Book** | WS depth/book channels, CCXT `watchOrderBook`, REST `fetchOrderBook` | `ob` counter |
| **Tickers** | CCXT `watchTicker` only (where supported) | `tickers` counter |

---

*Generated from `compare-v7-enhanced.js` v9.7 — 53 exchanges, production-tested*
