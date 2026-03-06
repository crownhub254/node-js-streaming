# Enhanced 4-Method Comparison Report v9.6 — Native × CCXT Pro × CCXT REST × Direct REST + Subscription Manager

**Date:** 2026-02-28 (v9.6 test)
**Test Duration:** 15 minutes (actual ~930s)
**Exchanges Tested:** 53
**Pairs Tested:** BTC/ETH/SOL/BRETT/PENGU/POPCAT/WIF/SUI/ENA × USDT/USDC/USD (all available per exchange)
**Method:** v9.6 Enhanced — 4 parallel methods + Subscription Manager + native DuckDB persistence + BTSE/FameEX REST fallback
**Storage:** DuckDB ✅ enabled — NOW includes native method writes (v9.6 fix)
**Health:** Avg Score 92/100 | 0 critical | 4 warnings
**Active:** 52/53 throughout
**Dashboard:** http://localhost:3456 (9 tabs: Hybrid Flow, Per Exchange, Per Pair, Dedup Analysis, Analytics, Correlation, Health, Sub Manager, Live Events)

## v9.6 Final Metrics

| Method | Total Messages | Hybrid Wins | /min (15m) |
|--------|---------------|------------|------------|
| **Native WS/REST** | 660,267 | 13 | 44,018 |
| **CCXT Pro** | 899,048 | 10 | 59,937 |
| **CCXT REST** | 74,100 | 5 | 4,940 |
| **Direct REST** | 42,677 | 0 | 2,845 |
| **HYBRID (deduped)** | **796,745** | — | **53,116** |

- **Trades:** 258,331 | **OB:** 491,290 | **Tickers:** 47,124
- **Dupes removed:** 398,934 (33.4% dupe rate — expected from 4-method parallel)
- **Sub Manager:** 76 batches | 44 stale reconnects | 42 forced reconnects | 74 failover rotations
- **DuckDB:** ✅ final flush 0 remaining — all 4 methods now write native (v9.6)

### v9.6 Key Changes vs v9.5

| Change | Impact |
|--------|--------|
| `addN()` now writes to DuckDB trades+OB buffers | All 17 native-only exchanges now persisted |
| BTSE REST fallback via `api.btse.com` every 30s | 286 trades/h → **2,132/min** (+444×) |
| FameEX REST fallback via `api.fameex.com` every 30s | Added (endpoint needs verification) |
| Coinbase: maxConns 10→15, safeMax 6→8, delay 250→300ms | Reduced connection timeout pressure |
| Kraken: maxConns 3→5, delay 150→200ms | Reduced subscription pressure |
| EXMO: maxConns 3→5, delay 200→250ms | Reduced timeout rate |
| Bullish poll interval 35s→45s | Reduces 429 rate-limit hits |
| 5-min rate snapshot (every 30 ticks) | Per-exchange throughput monitoring |

### 5-min Snapshots (from test)

**@ 5m elapsed (top 20):**
```
CoinEx        59,010 trades/5min |  11,802/min
Coinbase      37,651 trades/5min |   7,530/min
Bullish       12,008 trades/5min |   2,402/min
BitMart       10,141 trades/5min |   2,028/min
BigONE         7,965 trades/5min |   1,593/min
Crypto.com     7,681 trades/5min |   1,536/min
BingX          7,136 trades/5min |   1,427/min
Bitget         6,803 trades/5min |   1,361/min
BTSE           6,250 trades/5min |   1,250/min  ← REST fallback working
DigiFinex      5,050 trades/5min |   1,010/min
KuCoin         4,656 trades/5min |     931/min
Bybit          4,476 trades/5min |     895/min
OKX            3,529 trades/5min |     706/min
Bitfinex       3,509 trades/5min |     702/min
AscendEX       3,101 trades/5min |     620/min
WOO X          2,803 trades/5min |     561/min
Binance        2,655 trades/5min |     531/min
WhiteBIT       2,632 trades/5min |     526/min
Poloniex       2,427 trades/5min |     485/min
Toobit         2,382 trades/5min |     476/min
... and 32 more active exchanges
```

**@ 10m elapsed (top 20):**
```
Coinbase     195,101 trades/5min |  39,020/min  ← CCXT Pro streaming peaked
CoinEx       107,831 trades/5min |  21,566/min
BitMart       12,388 trades/5min |   2,478/min
BTSE          10,658 trades/5min |   2,132/min  ← REST fallback CONFIRMED
Crypto.com    10,588 trades/5min |   2,118/min
BigONE         8,680 trades/5min |   1,736/min
Bullish        8,577 trades/5min |   1,715/min
Pionex         7,417 trades/5min |   1,483/min
DigiFinex      5,744 trades/5min |   1,149/min
OKX            5,107 trades/5min |   1,021/min
Bybit          4,606 trades/5min |     921/min
LBank          4,264 trades/5min |     853/min
BingX          4,175 trades/5min |     835/min
Bitget         3,896 trades/5min |     779/min
Coinstore      3,653 trades/5min |     731/min
WOO X          3,224 trades/5min |     645/min
AscendEX       2,727 trades/5min |     545/min
HitBTC         2,607 trades/5min |     521/min
Binance        2,477 trades/5min |     495/min
OrangeX        2,420 trades/5min |     484/min
... and 31 more active exchanges
```

---

## 🏗️ System Architecture — Detailed Explanation

### Overview

This system is a **53-exchange normalized crypto streaming platform** that connects to every exchange simultaneously using **4 independent data collection methods** running in parallel. All data flows through a **Hybrid Fusion Engine** that deduplicates, normalizes, and selects the best source for each exchange-pair combination in real time.

### The 4 Streaming Methods

| # | Method | Type | How It Works | Priority |
|---|--------|------|-------------|----------|
| 1 | **Native WS/REST** | WebSocket (primary) + REST fallback | Direct WebSocket connections to each exchange's public API using exchange-specific protocols. Each exchange has custom subscribe/parse logic matching their exact API format. If WS goes silent >10s, REST endpoints are polled as fallback. | **Highest (0)** — always preferred |
| 2 | **CCXT Pro** | WebSocket (unified) | Uses the CCXT Pro library's `watch*` methods (`watchTrades`, `watchOrderBook`, `watchTicker`) in async loops. CCXT abstracts each exchange's WS protocol into a unified API. Runs with `newUpdates: false` for full snapshots. | **High (1)** — used when Native is unavailable for a pair |
| 3 | **CCXT REST** | REST polling | Uses CCXT's `fetch*` methods (`fetchTrades`, `fetchOrderBook`, `fetchTicker`) in 5-second polling loops. Markets are pre-loaded before test starts to avoid cold-start delays. | **Medium (2)** — used when WS methods go stale |
| 4 | **Direct REST** | REST polling (raw HTTP) | Raw HTTP calls to exchange REST APIs without any CCXT abstraction. Uses Node.js `https` module directly. Polls every 5-8 seconds. Serves as the last-resort data source. | **Low (3)** — fallback when all other methods fail |

### Data Flow Pipeline

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  Native WS   │  │  CCXT Pro    │  │  CCXT REST   │  │  Direct REST │
│  (Priority 0)│  │  (Priority 1)│  │  (Priority 2)│  │  (Priority 3)│
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                 │                 │                 │
       ▼                 ▼                 ▼                 ▼
  ┌─────────────────────────────────────────────────────────────┐
  │                   Symbol Normalization                      │
  │  toCanonical(): BTC/USDT, btcusdt, BTC-USDT → BTC_USDT    │
  └─────────────────────────┬───────────────────────────────────┘
                            │
                            ▼
  ┌─────────────────────────────────────────────────────────────┐
  │              shouldEmit() — Priority Gate                   │
  │  Only the highest-priority active method emits to hybrid.   │
  │  If higher-priority source goes stale (>30s), lower        │
  │  priority sources are allowed through.                      │
  └─────────────────────────┬───────────────────────────────────┘
                            │
                            ▼
  ┌─────────────────────────────────────────────────────────────┐
  │              Hybrid Fusion Engine                           │
  │  • Trade ID dedup (cross-method duplicate removal)         │
  │  • OB sequence validation (Binance, OKX checksum)          │
  │  • Stale OB detection and drop                             │
  │  • Normalized output: trades + orderbook + tickers         │
  └─────────────────────────┬───────────────────────────────────┘
                            │
              ┌─────────────┼────────────────┐
              ▼             ▼                ▼
  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
  │   DuckDB     │  │   Dashboard  │  │     SSE      │
  │  Storage     │  │  (port 3456) │  │  Live Events │
  └──────────────┘  └──────────────┘  └──────────────┘
```

### Subscription Manager

The Subscription Manager is a critical component that handles the complexity of maintaining WebSocket connections across 53 different exchanges:

- **Batched Subscriptions:** Instead of subscribing to all channels at once (which overloads many exchanges), subscriptions are sent in small batches (5-50 per batch depending on exchange) with configurable delays (100-400ms between batches)
- **Connection Pooling:** Tracks all active WebSocket connections per exchange. Exchanges with low per-connection limits (e.g., Bitfinex: 30 max) use multiple connections
- **Stale Detection:** Monitors last message time per exchange. If no data received within staleTimeout (45-60s), the connection is force-reconnected
- **Failover URL Rotation:** For 22 exchanges with multiple endpoints, automatically rotates to backup WebSocket URLs on connection failure (max 5 rotations/minute)
- **Exponential Backoff:** Reconnects use exponential backoff (1s→30s) with 30% random jitter to avoid thundering herd

### Emit Priority System (shouldEmit)

For each exchange-pair combination, only ONE method is allowed to emit to the hybrid output at a time:

1. **Native (priority 0)** — Always takes over if it has data
2. **CCXT Pro (priority 1)** — Used when Native is not available for that pair
3. **CCXT REST (priority 2)** — Fills in when WS methods go stale
4. **Direct REST (priority 3)** — Last resort fallback

If a higher-priority source goes silent for >30 seconds, lower-priority sources are automatically promoted. This prevents duplicate counting while ensuring maximum data coverage.

### Health Scoring Algorithm

Each exchange receives a health score out of 100:

```
score = 100
score -= min(20, totalErrors × 0.3)           // Max penalty: -20
score -= min(15, CONNECTION_CLOSED_count × 2)  // Max penalty: -15
score -= min(8,  TIMEOUT_count × 1)            // Max penalty: -8
score -= min(10, TYPE_ERROR_count × 5)          // Max penalty: -10
score += 5 if Native has data                   // Bonus: +5
score += 5 if CCXT Pro has data                 // Bonus: +5
score += 3 if CCXT REST has data                // Bonus: +3
Result: clamp(0, 100)
```

### DuckDB Storage Layer

All streamed data is persisted to DuckDB (`crypto_stream_data.duckdb`) with 3 tables:
- **trades:** timestamp, exchange, symbol, source, price, amount, side, trade_id
- **orderbook:** timestamp, exchange, symbol, source, best_bid, best_ask, bid_depth, ask_depth, spread
- **tickers:** timestamp, exchange, symbol, source, last_price, bid, ask, high_24h, low_24h, base_volume, quote_volume, change_pct
- Buffer flush: every 10 seconds, batch inserts up to 50,000 rows per flush

### Dashboard (9 Tabs)

| Tab | Name | Content |
|-----|------|---------|
| 0 | Hybrid Flow | Hero stats, data flow diagram, throughput chart, top 15 exchanges, data type distribution, winner analysis, summary table |
| 1 | Per Exchange | Expandable cards per exchange showing all pair data across 4 methods (H-TR, H-OB, N-TR, N-OB, P-TR, P-OB, R-TR, R-OB, D-TR, D-OB) |
| 2 | Per Pair | Aggregated view of each trading pair across all exchanges |
| 3 | Dedup Analysis | Deduplication rates per exchange, stacked chart, efficiency metrics |
| 4 | Analytics | Method distribution pie, top 10 exchanges bar, tier breakdown, error categories |
| 5 | Correlation | Trade ID cross-method matching rates (N↔P, N↔R, P↔R, N↔D) per exchange |
| 6 | Health | Sub-tabs: Overview (health grid with SVG score circles), Errors (per-category), Fixes (recommendations), Connections (status + last message times) |
| 7 | Sub Manager | Connection pools, subscriptions sent, batches, stale reconnects, forced reconnects, failover rotations per exchange |
| 8 | Live Events | Real-time SSE event stream (connect/disconnect/error events, last 500) |

---

## 🔌 Per-Exchange Streaming Architecture

### Exchange Connection Summary

| # | Exchange | Tier | Native Type | CCXT ID | WS Endpoints | Compression | Ping | Failover URLs |
|---|----------|------|-------------|---------|-------------|-------------|------|---------------|
| 1 | Binance | T1 | ws | binance | stream.binance.com:9443 | none | none | 3 |
| 2 | Coinbase | T1 | ws | coinbase | ws-feed.exchange.coinbase.com | none | none | 0 |
| 3 | Kraken | T1 | ws | kraken | ws.kraken.com/v2 | none | JSON {method:ping} 25s | 0 |
| 4 | KuCoin | T1 | ws (dynamic token) | kucoin | Dynamic (bullet-public API) | none | {type:ping} 18s | 2 |
| 5 | OKX | T1 | ws | okx | ws.okx.com:8443 | none | string "ping" 25s | 3 |
| 6 | Bybit | T1 | ws | bybit | stream.bybit.com/v5/public/spot | none | {op:ping} 20s | 2 |
| 7 | Bitfinex | T1 | ws | bitfinex | api-pub.bitfinex.com/ws/2 | none | {event:ping} 25s | 2 |
| 8 | Gate.io | T1 | ws | gateio | api.gateio.ws/ws/v4/ | none | {channel:spot.ping} 15s | 3 |
| 9 | HTX | T1 | ws | htx | api.huobi.pro/ws | **gzip** | server-push {ping}→{pong} | 3 |
| 10 | WOO X | T1 | ws | woo | wss.woo.org/ws/stream | none | {event:ping} 9s | 0 |
| 11 | Crypto.com | T2 | ws | cryptocom | stream.crypto.com/exchange/v1/market | none | {method:public/heartbeat} 25s | 0 |
| 12 | Bitstamp | T2 | ws | bitstamp | ws.bitstamp.net | none | {event:bts:heartbeat} 20s | 0 |
| 13 | WhiteBIT | T2 | ws | whitebit | api.whitebit.com/ws | none | {method:server.ping} 25s | 0 |
| 14 | AscendEX | T2 | ws | ascendex | ascendex.com/1/api/pro/v1/stream | none | {op:ping} 15s | 0 |
| 15 | BingX | T2 | ws | bingx | open-api-ws.bingx.com/market | **gzip** | string "Pong" 5s | 0 |
| 16 | Toobit | T2 | ws | toobit | stream.toobit.com/quote/ws/v1 | none | {ping:timestamp} 15s | 2 |
| 17 | Deepcoin | T2 | ws | deepcoin¹ | stream.deepcoin.com | none | string "ping" 15s | 0 |
| 18 | XT.com | T2 | ws | xt | stream.xt.com/public | none | string "ping" 15s | 3 |
| 19 | Zoomex | T2 | ws | — | stream.zoomex.com/v5/public/spot | none | {op:ping} 20s | 0 |
| 20 | Bitget | T2 | ws | bitget | ws.bitget.com/v2/ws/public | none | string "ping" 30s | 0 |
| 21 | Gemini | T2 | ws | gemini | api.gemini.com/v2/marketdata | none | {type:heartbeat} 30s | 2 |
| 22 | Binance.US | T2 | ws | binanceus | stream.binance.us:9443/stream | none | none | 2 |
| 23 | MEXC | T2 | rest | mexc | — (REST only) | none | — | 0 |
| 24 | CoinEx | T3 | ws | coinex | socket.coinex.com/v2/spot | **gzip** | {method:server.ping} 15s | 0 |
| 25 | LBank | T3 | ws | lbank | www.lbkex.net/ws/V2/ | none | server-push {action:ping} | 3 |
| 26 | BitMart | T3 | ws | bitmart | ws-manager-compress.bitmart.com | **inflate** | string "ping" 10s | 0 |
| 27 | Pionex | T3 | ws | — | ws.pionex.com/wsPub | none | server-push {op:PING} | 0 |
| 28 | Poloniex | T3 | ws | poloniex | ws.poloniex.com/ws/public | none | {event:ping} 20s | 2 |
| 29 | HitBTC | T3 | ws | hitbtc | api.hitbtc.com/api/3/ws/public | none | {method:server.ping} 20s | 0 |
| 30 | BTSE | T3 | ws | — | ws.btse.com/ws/spot | none | string "ping" 30s | 0 |
| 31 | Biconomy | T3 | ws | — | bei.biconomy.com/ws | none | {method:server.ping} 30s | 0 |
| 32 | Hotcoin | T3 | ws+rest | — | wss.hotcoinfin.com/trade/multiple | **gzip** | server-push {ping}→{pong} | 0 |
| 33 | NovaEx | T3 | ws | — | wss.woox.io/ws/stream | none | {event:ping} 9s | 0 |
| 34 | FameEX | T3 | ws | — | wsapi.fameex.com/v1/ws/stream/public | none | server-push {ping}→{pong} | 0 |
| 35 | Websea | T3 | ws | — | oapi.websea.com/ws/v1/spot/market | none | server-push {op:ping} | 0 |
| 36 | Bullish | T3 | ws | bullish | api.exchange.bullish.com (JSON-RPC) | none | server-push {type:ping} | 0 |
| 37 | Darkex | T3 | ws | — | ws.darkex.com/kline-api/ws | **gzip** | server-push {ping}→{pong} | 0 |
| 38 | Bitrue | T3 | ws | bitrue | ws.bitrue.com/market/ws | **gzip** | server-push {event:ping} | 0 |
| 39 | BloFin | T3 | ws | blofin | openapi.blofin.com/ws/public | none | string "ping" 25s | 1 |
| 40 | DigiFinex | T3 | ws | digifinex¹ | openapi.digifinex.com/ws/v1/ | **inflate** | {method:server.ping} 30s | 0 |
| 41 | EXMO | T3 | ws | exmo | ws-api.exmo.com/v1/public | none | {method:ping} 20s | 1 |
| 42 | CEX.IO | T3 | rest | cex¹ | — (REST only) | none | — | 0 |
| 43 | OrangeX | T3 | rest | — | — (REST only) | none | — | 0 |
| 44 | Azbit | T3 | rest | — | — (REST only) | none | — | 0 |
| 45 | BVOX | T3 | rest | — | — (REST only) | none | — | 0 |
| 46 | Trubit Pro | T3 | rest | — | — (REST only) | none | — | 0 |
| 47 | BigONE | T3 | rest | bigone | — (REST only) | none | — | 0 |
| 48 | LATOKEN | T3 | rest | latoken | — (REST only) | none | — | 0 |
| 49 | Coinstore | T3 | ws | — | ws.coinstore.com/s/ws | none | server-push {ping}→{pong} | 0 |
| 50 | GroveX | T3 | ws | — | ws.grovex.io/kline-api/ws | **gzip** | server-push {ping}→{pong} | 0 |
| 51 | CoinW | T3 | rest | — | — (REST only) | none | — | 0 |
| 52 | Batonex | T3 | rest | — | — (REST only) | none | — | 0 |
| 53 | CEEX | T3 | ws | — | wsapi.ceex.com/openapi/quote/ws/v1 | none | string ping/server-push | 0 |

¹ = skipPro (CCXT Pro disabled, REST-only via CCXT)

### Compression Protocols

| Protocol | Exchanges | How It Works |
|----------|-----------|-------------|
| **gzip** | HTX, BingX, CoinEx, Hotcoin, Darkex, Bitrue, GroveX | Server sends gzip-compressed binary frames. Client decompresses with `zlib.gunzipSync()` before JSON parsing. |
| **inflate** | BitMart, DigiFinex | Server sends raw deflate-compressed frames. Client decompresses with `zlib.inflateRawSync()` before JSON parsing. |
| **none** | All others (44 exchanges) | Plain text WebSocket frames, parsed directly as JSON. |

### Ping/Keep-Alive Mechanisms

| Type | Count | Exchanges | Mechanism |
|------|-------|-----------|----------|
| Client-sent JSON ping | 18 | Kraken, KuCoin, AscendEX, WhiteBIT, CoinEx, HitBTC, EXMO, Crypto.com, etc. | Client sends JSON `{method:'ping'}` at configured interval |
| Client-sent string ping | 8 | OKX, BingX, BitMart, Bitget, XT.com, Deepcoin, BloFin, BTSE | Client sends literal string `'ping'` or `'Pong'` |
| Server-push ping/pong | 9 | HTX, LBank, Pionex, FameEX, Hotcoin, Darkex, Bitrue, GroveX, Coinstore | Server sends `{ping:...}`, client must respond with `{pong:...}` |
| None (native heartbeat) | 4 | Binance, Binance.US, Coinbase (protocol-level) | WebSocket protocol-level ping/pong only |

### Failover URL Map

| Exchange | # URLs | Endpoints |
|----------|--------|----------|
| OKX | 3 | wss://ws.okx.com:8443/ws/v5/public, wss://wsaws.okx.com:8443/ws/v5/public, wss://wspap.okx.com:8443/ws/v5/public |
| HTX | 3 | wss://api.huobi.pro/ws, wss://api-aws.huobi.pro/ws, wss://api.htx.com/ws |
| Gate.io | 3 | wss://api.gateio.ws/ws/v4/, wss://fx-ws.gateio.ws/v4/ws/usdt, wss://ws.gate.io/v4/ |
| Bybit | 3 | wss://stream.bybit.com/v5/public/spot, wss://stream.bytick.com/v5/public/spot, wss://stream.bybit.kz/v5/public/spot |
| Binance | 3 | wss://stream.binance.com:9443/stream, wss://stream.binance.com:443/stream, wss://data-stream.binance.vision/stream |
| Binance.US | 2 | wss://stream.binance.us:9443/stream, wss://stream.binance.us:443/stream |
| Gemini | 2 | wss://api.gemini.com/v2/marketdata, wss://api.gemini.com/v1/marketdata/BTCUSD |
| XT.com | 3 | wss://stream.xt.com/public, wss://stream2.xt.com/public, wss://stream3.xt.com/public |
| Toobit | 2 | wss://stream.toobit.com/quote/ws/v1, wss://stream.toobit.com/quote/ws/v2 |
| LBank | 3 | wss://www.lbkex.net/ws/V2/, wss://www.lbkex.com/ws/V2/, wss://api.lbank.info/ws/V2/ |
| BloFin | 1 | wss://openapi.blofin.com/ws/public |
| EXMO | 1 | wss://ws-api.exmo.com:443/v1/public |
| KuCoin | 2 | wss://ws-api-spot.kucoin.com/, wss://ws-api.kucoin.com/ |
| Poloniex | 2 | wss://ws.poloniex.com/ws/public, wss://ws2.poloniex.com/ws/public |
| Bitfinex | 2 | wss://api-pub.bitfinex.com/ws/2, wss://api.bitfinex.com/ws/2 |
| WhiteBIT | 1 | wss://api.whitebit.com/ws |
| FameEX | 2 | wss://wsapi.fameex.com/v1/ws/stream/public, wss://api.fameex.com/v2/ws |
| CoinEx | 2 | wss://socket.coinex.com/v2/spot, wss://ws.coinex.com/ |
| BitMart | 1 | wss://ws-manager-compress.bitmart.com/api?protocol=1.1 |
| Bitrue | 1 | wss://ws.bitrue.com/kline-api/ws |
| Crypto.com | 2 | wss://stream.crypto.com/exchange/v1/market, wss://stream.crypto.com/v2/market |
| Coinstore | 1 | wss://ws.coinstore.com/s/ws |

**Failover Logic:** On connection close, timeout, or stale detection → rotate to next URL in list → max 5 rotations per minute per exchange → exponential backoff between retries.

### Exchange Tier Classification

| Tier | Description | Count | Exchanges |
|------|------------|-------|----------|
| **T1** | Top-tier, highest volume, most reliable | 10 | Binance, Coinbase, Kraken, KuCoin, OKX, Bybit, Bitfinex, Gate.io, HTX, WOO X |
| **T2** | Mid-tier, established exchanges | 13 | Crypto.com, Bitstamp, WhiteBIT, AscendEX, BingX, Toobit, Deepcoin, XT.com, Zoomex, Bitget, Gemini, Binance.US, MEXC |
| **T3** | Smaller exchanges, REST-heavy | 30 | CoinEx, LBank, BitMart, Pionex, Poloniex, HitBTC, BTSE, Biconomy, Hotcoin, NovaEx, FameEX, Websea, Bullish, Darkex, Bitrue, BloFin, DigiFinex, EXMO, CEX.IO, OrangeX, Azbit, BVOX, Trubit Pro, BigONE, LATOKEN, Coinstore, GroveX, CoinW, Batonex, CEEX |

### Batch Launch Order

Exchanges are launched in 6 sequential batches with 3-second gaps between batches to avoid overwhelming the network:

- **Batch 1:** Binance, Coinbase, Kraken, KuCoin, OKX, Bybit, Bitfinex, Gate.io, HTX, WOO X
- **Batch 2:** Crypto.com, Bitstamp, WhiteBIT, AscendEX, BingX, Toobit, Deepcoin, XT.com, Zoomex, Bitget
- **Batch 3:** Gemini, Binance.US, MEXC, CoinEx, LBank, BitMart, Pionex, Poloniex, HitBTC
- **Batch 4:** BTSE, Biconomy, Hotcoin, NovaEx, FameEX, Websea, Bullish, Darkex, Bitrue, BloFin
- **Batch 5:** DigiFinex, EXMO, CEX.IO, OrangeX, Azbit, BVOX, Trubit Pro, BigONE, LATOKEN
- **Batch 6:** Coinstore, GroveX, CoinW, Batonex, CEEX

---

## ⚙️ Enhancements Applied (v9 → v9.5)

| Feature | Description |
|---------|-------------|
| Subscription Manager | Per-exchange WS limits, batched subscription sending, stale connection monitoring |
| Batched Subscriptions | Send subscriptions in small batches (4-50 subs/batch) with 100-400ms delays between batches |
| Stale Monitor | Force reconnect if no data received for 45-60s (per-exchange configurable) |
| Connection Pool | Track all WS connections per exchange, pool health monitoring |
| Per-Exchange Limits | Official + safe max subs documented for all 53 exchanges (Groups A/B/C) |
| Reconnect | Exponential backoff (1s→30s) + random jitter (30% of base) |
| Failover URLs | Multi-endpoint for 22 exchanges with URL rotation on timeout/stale/error (v9.1) |
| Emit Priority | Hybrid source priority: Native > CCXT Pro > CCXT REST > Direct REST (v9.1) |
| REST Fallback | Auto-snapshot via REST when WS silent >10s |
| Deduplication | Trade ID dedup for 7+ exchanges (Binance, KuCoin, OKX, Gate.io, etc.) |
| OB Validation | Sequence/checksum validation for Binance, Binance.US, OKX |
| Health Metrics | Real-time health scoring + error classification + fix recommendations |
| OB Correlation | Orderbook ID/nonce/sequence matching across methods |
| Error Hardening | CCXT Pro with per-error-type recovery (conn close, timeout, typeErr) |
| Direct REST | 4th stream: raw HTTP polling without CCXT abstraction |
| CCXT Pre-Load | All CCXT markets loaded sequentially before test starts (no cold-start delay) |

## 📡 Subscription Manager Report

### Per-Exchange WS Limits Configuration

| Exchange | Group | Official Max | Safe Max | Max Conns | Batch Size | Batch Delay | Stale Timeout |
|----------|-------|-------------|----------|-----------|------------|-------------|---------------|
| Binance | A | 1024 | 180 | 5 | 20 | 200ms | 45s |
| Coinbase | A | 300 | 8 | 15 | 8 | 300ms | 45s |
| Kraken | A | 500 | 200 | 5 | 40 | 200ms | 60s |
| KuCoin | A | 300 | 120 | 7 | 20 | 120ms | 45s |
| Bybit | A | 200 | 100 | 5 | 20 | 150ms | 45s |
| Bitfinex | A | 30 | 15 | 25 | 5 | 350ms | 60s |
| Gate.io | A | 200 | 80 | 5 | 20 | 150ms | 45s |
| WhiteBIT | A | 300 | 80 | 4 | 10 | 300ms | 60s |
| AscendEX | A | 200 | 80 | 3 | 15 | 200ms | 45s |
| Bitstamp | A | 300 | 100 | 3 | 20 | 200ms | 45s |
| Bitget | A | 240 | 80 | 3 | 20 | 180ms | 45s |
| Bullish | A | 200 | 80 | 3 | 15 | 200ms | 60s |
| BloFin | A | 200 | 60 | 5 | 8 | 250ms | 50s |
| MEXC | A | 30 | 20 | 5 | 5 | 300ms | 45s |
| CoinEx | A | 200 | 100 | 3 | 20 | 200ms | 45s |
| LBank | A | 100 | 30 | 8 | 6 | 300ms | 60s |
| BitMart | A | 100 | 50 | 5 | 10 | 200ms | 45s |
| Poloniex | A | 100 | 40 | 4 | 9 | 250ms | 45s |
| HitBTC | A | 100 | 50 | 3 | 10 | 200ms | 45s |
| OKX | B | 480 | 180 | 4 | 20 | 200ms | 45s |
| HTX | B | 200 | 60 | 5 | 10 | 250ms | 45s |
| Toobit | B | 200 | 100 | 3 | 14 | 200ms | 45s |
| XT.com | B | 200 | 70 | 4 | 15 | 250ms | 45s |
| Gemini | B | 200 | 100 | 3 | 20 | 200ms | 60s |
| EXMO | B | 200 | 100 | 5 | 20 | 250ms | 45s |
| BingX | C | 300 | 150 | 3 | 20 | 100ms | 45s |
| Crypto.com | C | 300 | 150 | 3 | 20 | 200ms | 45s |
| Zoomex | C | 200 | 100 | 3 | 20 | 200ms | 45s |
| Deepcoin | C | 200 | 100 | 3 | 20 | 200ms | 45s |
| Darkex | C | 200 | 100 | 3 | 20 | 200ms | 45s |
| Bitrue | C | 200 | 100 | 3 | 20 | 200ms | 45s |
| WOO X | C | 300 | 150 | 3 | 20 | 200ms | 45s |
| Binance.US | C | 1024 | 200 | 5 | 20 | 200ms | 45s |
| BTSE | C | 200 | 100 | 3 | 20 | 200ms | 45s |
| Pionex | C | 200 | 100 | 3 | 20 | 200ms | 45s |
| Biconomy | C | 200 | 100 | 3 | 20 | 200ms | 45s |
| DigiFinex | C | 100 | 50 | 5 | 10 | 200ms | 60s |
| Hotcoin | C | 200 | 100 | 3 | 20 | 200ms | 45s |
| NovaEx | C | 300 | 150 | 3 | 20 | 200ms | 45s |
| FameEX | C | 200 | 100 | 3 | 20 | 200ms | 45s |
| Websea | C | 200 | 100 | 3 | 20 | 200ms | 45s |
| Coinstore | C | 200 | 100 | 3 | 20 | 200ms | 45s |
| GroveX | C | 200 | 100 | 3 | 20 | 200ms | 45s |
| CEEX | C | 200 | 100 | 3 | 20 | 200ms | 45s |

### Subscription Manager Runtime Stats

| Exchange | Pool Conns | Total Subs | Stale Reconnects | Forced Reconnects | Failover Rotations | Batches Sent |
|----------|-----------|------------|-----------------|-------------------|-------------------|-------------|
| Binance | 1 | 0 | 4 | 4 | 4 | 0 |
| Coinbase | 0 | 0 | 3 | 3 | 8 | 0 |
| Kraken | 1 | 0 | 1 | 1 | 1 | 0 |
| KuCoin | 1 | 0 | 0 | 0 | 0 | 0 |
| OKX | 1 | 0 | 0 | 0 | 0 | 0 |
| Bybit | 1 | 0 | 0 | 0 | 0 | 0 |
| Bitfinex | 1 | 12 | 0 | 0 | 0 | 3 |
| Gate.io | 1 | 16 | 1 | 1 | 1 | 2 |
| HTX | 1 | 22 | 1 | 1 | 2 | 12 |
| WOO X | 1 | 22 | 0 | 0 | 0 | 2 |
| Crypto.com | 1 | 0 | 0 | 0 | 1 | 0 |
| Bitstamp | 1 | 26 | 1 | 1 | 1 | 24 |
| WhiteBIT | 1 | 18 | 0 | 0 | 0 | 2 |
| AscendEX | 1 | 28 | 1 | 1 | 1 | 4 |
| BingX | 1 | 26 | 2 | 2 | 4 | 8 |
| Toobit | 1 | 22 | 0 | 0 | 0 | 2 |
| Deepcoin | 1 | 5 | 0 | 0 | 0 | 1 |
| XT.com | 0 | 0 | 0 | 0 | 0 | 0 |
| Zoomex | 1 | 0 | 1 | 1 | 1 | 0 |
| Bitget | 1 | 0 | 1 | 1 | 1 | 0 |
| Gemini | 1 | 0 | 7 | 7 | 7 | 0 |
| Binance.US | 1 | 0 | 3 | 3 | 6 | 0 |
| CoinEx | 1 | 0 | 0 | 0 | 0 | 0 |
| LBank | 1 | 24 | 3 | 3 | 5 | 16 |
| BitMart | 1 | 0 | 1 | 1 | 1 | 0 |
| Pionex | 1 | 0 | 1 | 1 | 1 | 0 |
| Poloniex | 1 | 0 | 0 | 0 | 0 | 0 |
| HitBTC | 1 | 0 | 0 | 0 | 0 | 0 |
| BTSE | 1 | 0 | 1 | 1 | 1 | 0 |
| Biconomy | 1 | 0 | 0 | 0 | 0 | 0 |
| Hotcoin | 1 | 0 | 2 | 2 | 2 | 0 |
| NovaEx | 1 | 0 | 1 | 1 | 1 | 0 |
| FameEX | 1 | 0 | 1 | 1 | 4 | 0 |
| Websea | 1 | 0 | 1 | 1 | 1 | 0 |
| Bullish | 1 | 0 | 0 | 0 | 0 | 0 |
| Darkex | 1 | 0 | 1 | 1 | 1 | 0 |
| Bitrue | 1 | 0 | 1 | 1 | 2 | 0 |
| BloFin | 1 | 0 | 1 | 1 | 1 | 0 |
| DigiFinex | 7 | 0 | 2 | 0 | 0 | 0 |
| EXMO | 1 | 0 | 1 | 1 | 1 | 0 |
| Coinstore | 1 | 0 | 1 | 1 | 1 | 0 |
| GroveX | 1 | 0 | 0 | 0 | 0 | 0 |
| CEEX | 1 | 0 | 0 | 0 | 14 | 0 |

**Summary:** 44 total stale reconnects, 42 forced reconnects, 74 failover rotations, 76 subscription batches sent

## Data Enrichment Summary

| Metric | Count |
|--------|-------|
| Total Normalized | 573,192 |
| Duplicates Dropped | 0 |
| OB Validated (seq) | 2,816 |
| Stale OB Dropped | 0 |
| REST Fallbacks | 1,081 |
| Hybrid Combined Trades | 258,331 |
| Hybrid Combined OB | 491,290 |
| Hybrid Deduped (cross-method) | 398,934 |

## 4-Way Summary Table

| # | Exchange | Tier | Native | CCXT Pro | CCXT REST | Direct REST | Health | Winner | Margin |
|---|----------|------|--------|----------|-----------|-------------|--------|--------|--------|
| 1 | Binance | T1 | **4,942** | **0** | **2,228** | **1,129** | 🟢100 | ✅ Native | +122% |
| 2 | Coinbase | T1 | **6,103** | **304,741** | **1,634** | **630** | 🟢85 | 🔵 CCXT Pro | +4893% |
| 3 | Kraken | T1 | **25,707** | **10,663** | **1,954** | **1,382** | 🟢85 | ✅ Native | +141% |
| 4 | KuCoin | T1 | **27,176** | **8,466** | **2,332** | **1,583** | 🟡70 | ✅ Native | +221% |
| 5 | OKX | T1 | **9,488** | **10,963** | **1,993** | **793** | 🟢85 | ≈ Tie | 16% |
| 6 | Bybit | T1 | **30,265** | **13,444** | **2,356** | **1,413** | 🟢85 | ✅ Native | +125% |
| 7 | Bitfinex | T1 | **235,696** | **0** | **803** | **1,142** | 🟢100 | ✅ Native | +20539% |
| 8 | Gate.io | T1 | **2,815** | **429** | **2,976** | **718** | 🟢85 | ≈ Tie | 6% |
| 9 | HTX | T1 | **4,703** | **0** | **1,544** | **289** | 🟢100 | ✅ Native | +205% |
| 10 | WOO X | T1 | **1,806** | **7,427** | **5,015** | **1,168** | 🟢85 | 🔵 CCXT Pro | +48% |
| 11 | Crypto.com | T2 | **20,224** | **17,382** | **3,565** | **838** | 🟢85 | ≈ Tie | 16% |
| 12 | Bitstamp | T2 | **191** | **1,050** | **453** | **168** | 🟢85 | 🔵 CCXT Pro | +132% |
| 13 | WhiteBIT | T2 | **4,233** | **0** | **1,759** | **505** | 🟢100 | ✅ Native | +141% |
| 14 | AscendEX | T2 | **10,919** | **10,422** | **4,117** | **164** | 🟡70 | ≈ Tie | 5% |
| 15 | BingX | T2 | **11,853** | **6,314** | **4,031** | **2,087** | 🟡78 | ✅ Native | +88% |
| 16 | Toobit | T2 | **1,362** | **1,311** | **1,588** | **0** | 🟢85 | ≈ Tie | 17% |
| 17 | Deepcoin | T2 | **162** | **0** | **1,058** | **0** | 🟢100 | 🟠 CCXT REST | +553% |
| 18 | XT.com | T2 | **1,382** | **8,980** | **1,650** | **1,384** | 🟢100 | 🔵 CCXT Pro | +444% |
| 19 | Zoomex | T2 | **21,050** | **0** | **0** | **0** | 🟢100 | 🟢 Native Only | - |
| 20 | Bitget | T2 | **16,862** | **19,791** | **1,607** | **1,376** | 🟢85 | ≈ Tie | 17% |
| 21 | Gemini | T2 | **0** | **0** | **3,805** | **1,662** | 🟢96 | 🟠 CCXT REST | +129% |
| 22 | Binance.US | T2 | **1,889** | **15,980** | **2,111** | **1,576** | 🟢85 | 🔵 CCXT Pro | +657% |
| 23 | MEXC | T2 | **3,570** | **0** | **0** | **2,218** | 🟢100 | ✅ Native | +61% |
| 24 | CoinEx | T3 | **8,139** | **361,069** | **3,151** | **2,640** | 🟢85 | 🔵 CCXT Pro | +4336% |
| 25 | LBank | T3 | **15,425** | **0** | **0** | **870** | 🟢97 | ✅ Native | +1673% |
| 26 | BitMart | T3 | **28,246** | **22,087** | **1,517** | **800** | 🟢85 | ≈ Tie | 28% |
| 27 | Pionex | T3 | **18,112** | **0** | **0** | **0** | 🟢100 | 🟢 Native Only | - |
| 28 | Poloniex | T3 | **20,903** | **0** | **1,895** | **1,091** | 🟢100 | ✅ Native | +1003% |
| 29 | HitBTC | T3 | **19,426** | **41,799** | **4,406** | **1,631** | 🟢85 | 🔵 CCXT Pro | +115% |
| 30 | BTSE | T3 | **21,306** | **0** | **0** | **164** | 🟢100 | 🟢 Native Only | - |
| 31 | Biconomy | T3 | **1,441** | **0** | **0** | **0** | 🟢100 | 🟢 Native Only | - |
| 32 | Hotcoin | T3 | **5,438** | **0** | **0** | **0** | 🟢100 | 🟢 Native Only | - |
| 33 | NovaEx | T3 | **2,809** | **0** | **0** | **0** | 🟢100 | 🟢 Native Only | - |
| 34 | FameEX | T3 | **51** | **0** | **0** | **0** | 🟢82 | 🟢 Native Only | - |
| 35 | Websea | T3 | **1,581** | **0** | **0** | **140** | 🟢100 | 🟢 Native Only | - |
| 36 | Bullish | T3 | **11,021** | **19,177** | **1,161** | **83** | 🟡70 | 🔵 CCXT Pro | +74% |
| 37 | Darkex | T3 | **811** | **0** | **0** | **0** | 🟢100 | 🟢 Native Only | - |
| 38 | Bitrue | T3 | **1,069** | **309** | **2,628** | **871** | 🟢85 | 🟠 CCXT REST | +146% |
| 39 | BloFin | T3 | **1,622** | **8,822** | **1,761** | **599** | 🟢85 | 🔵 CCXT Pro | +401% |
| 40 | DigiFinex | T3 | **19,897** | **0** | **1,304** | **891** | 🟢100 | ✅ Native | +1426% |
| 41 | EXMO | T3 | **1,947** | **8,422** | **1,739** | **2,210** | 🟢85 | 🔵 CCXT Pro | +281% |
| 42 | CEX.IO | T3 | **529** | **0** | **976** | **592** | 🟢100 | 🟠 CCXT REST | +65% |
| 43 | OrangeX | T3 | **4,641** | **0** | **0** | **1,412** | 🟢100 | 🟢 Native Only | - |
| 44 | Azbit | T3 | **238** | **0** | **0** | **905** | 🟢100 | 🟢 Native Only | - |
| 45 | BVOX | T3 | **402** | **0** | **0** | **381** | 🟢100 | 🟢 Native Only | - |
| 46 | Trubit Pro | T3 | **513** | **0** | **0** | **574** | 🟢100 | 🟢 Native Only | - |
| 47 | BigONE | T3 | **21,012** | **0** | **3,169** | **1,431** | 🟢100 | ✅ Native | +563% |
| 48 | LATOKEN | T3 | **904** | **0** | **1,814** | **0** | 🟢100 | 🟠 CCXT REST | +101% |
| 49 | Coinstore | T3 | **7,242** | **0** | **0** | **415** | 🟢100 | 🟢 Native Only | - |
| 50 | GroveX | T3 | **2,257** | **0** | **0** | **1,136** | 🟢100 | 🟢 Native Only | - |
| 51 | CoinW | T3 | **587** | **0** | **0** | **1,260** | 🟢100 | 🟢 Native Only | - |
| 52 | Batonex | T3 | **300** | **0** | **0** | **356** | 🟢100 | 🟢 Native Only | - |
| 53 | CEEX | T3 | **0** | **0** | **0** | **0** | 🟢84 | ❌ Failed | - |

## Detailed Breakdown (Trades + Orderbook + Tickers)

| # | Exchange | N-TR | N-OB | Pro-TR | Pro-OB | Pro-TK | REST-TR | REST-OB | REST-TK | D-TR | D-OB | D-TK | Best |
|---|----------|------|------|--------|--------|--------|---------|---------|---------|------|------|------|------|
| 1 | Binance | 3,930 | 1,012 | 0 | 0 | 0 | 2,020 | 103 | 105 | 940 | 94 | 95 | Native |
| 2 | Coinbase | 900 | 5,203 | 296,061 | 5,567 | 3,113 | 1,460 | 75 | 99 | 520 | 55 | 55 | Pro |
| 3 | Kraken | 104 | 25,603 | 242 | 10,391 | 30 | 1,780 | 87 | 87 | 1,150 | 115 | 117 | Native |
| 4 | KuCoin | 6,657 | 20,519 | 780 | 7,408 | 278 | 2,220 | 0 | 112 | 1,440 | 73 | 70 | Native |
| 5 | OKX | 1,951 | 7,537 | 7,209 | 999 | 2,755 | 1,800 | 97 | 96 | 650 | 72 | 71 | Tie |
| 6 | Bybit | 4,357 | 25,908 | 5,636 | 7,001 | 807 | 2,140 | 108 | 108 | 1,170 | 120 | 123 | Native |
| 7 | Bitfinex | 4,893 | 230,803 | 0 | 0 | 0 | 680 | 0 | 123 | 840 | 151 | 151 | Native |
| 8 | Gate.io | 2,156 | 659 | 284 | 73 | 72 | 2,700 | 138 | 138 | 590 | 63 | 65 | Tie |
| 9 | HTX | 1,153 | 3,550 | 0 | 0 | 0 | 1,400 | 72 | 72 | 107 | 91 | 91 | Native |
| 10 | WOO X | 474 | 1,332 | 4,166 | 2,346 | 915 | 4,780 | 235 | 0 | 1,060 | 108 | 0 | Pro |
| 11 | Crypto.com | 12,961 | 7,263 | 13,756 | 1,795 | 1,831 | 3,240 | 163 | 162 | 760 | 78 | 0 | Tie |
| 12 | Bitstamp | 10 | 181 | 823 | 227 | 0 | 403 | 24 | 26 | 145 | 10 | 13 | Pro |
| 13 | WhiteBIT | 2,052 | 2,181 | 0 | 0 | 0 | 1,600 | 80 | 79 | 460 | 23 | 22 | Native |
| 14 | AscendEX | 2,061 | 8,858 | 2,263 | 8,159 | 0 | 3,740 | 188 | 189 | 0 | 164 | 0 | Tie |
| 15 | BingX | 6,213 | 5,640 | 2,377 | 2,329 | 1,608 | 3,660 | 185 | 186 | 1,900 | 187 | 0 | Native |
| 16 | Toobit | 932 | 430 | 789 | 198 | 324 | 1,440 | 75 | 73 | 0 | 0 | 0 | Tie |
| 17 | Deepcoin | 0 | 162 | 0 | 0 | 0 | 960 | 49 | 49 | 0 | 0 | 0 | REST |
| 18 | XT.com | 394 | 988 | 957 | 7,104 | 919 | 1,500 | 76 | 74 | 1,260 | 124 | 0 | Pro |
| 19 | Zoomex | 3,526 | 17,524 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Native Only |
| 20 | Bitget | 6,419 | 10,443 | 9,530 | 3,085 | 7,176 | 1,460 | 73 | 74 | 1,250 | 126 | 0 | Tie |
| 21 | Gemini | 0 | 0 | 0 | 0 | 0 | 3,460 | 172 | 173 | 1,510 | 152 | 0 | REST |
| 22 | Binance.US | 85 | 1,804 | 307 | 10,520 | 5,153 | 1,920 | 96 | 95 | 1,430 | 146 | 0 | Pro |
| 23 | MEXC | 3,400 | 170 | 0 | 0 | 0 | 0 | 0 | 0 | 2,010 | 208 | 0 | Native |
| 24 | CoinEx | 1,802 | 6,337 | 349,674 | 11,266 | 129 | 2,860 | 145 | 146 | 2,640 | 0 | 0 | Pro |
| 25 | LBank | 6,827 | 8,598 | 0 | 0 | 0 | 0 | 0 | 0 | 790 | 80 | 0 | Native |
| 26 | BitMart | 17,660 | 10,586 | 11,234 | 8,040 | 2,813 | 1,380 | 68 | 69 | 800 | 0 | 0 | Tie |
| 27 | Pionex | 15,112 | 3,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Native Only |
| 28 | Poloniex | 4,145 | 16,758 | 0 | 0 | 0 | 1,720 | 90 | 85 | 990 | 101 | 0 | Native |
| 29 | HitBTC | 40 | 19,386 | 173 | 36,189 | 5,437 | 3,986 | 209 | 211 | 1,477 | 154 | 0 | Pro |
| 30 | BTSE | 17,467 | 3,839 | 0 | 0 | 0 | 0 | 0 | 0 | 83 | 81 | 0 | Native Only |
| 31 | Biconomy | 1,257 | 184 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Native Only |
| 32 | Hotcoin | 3,055 | 2,383 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Native Only |
| 33 | NovaEx | 1,233 | 1,576 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Native Only |
| 34 | FameEX | 23 | 28 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Native Only |
| 35 | Websea | 1,581 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 140 | 0 | Native Only |
| 36 | Bullish | 10,964 | 57 | 13,762 | 1,390 | 4,025 | 1,050 | 53 | 58 | 0 | 83 | 0 | Pro |
| 37 | Darkex | 201 | 610 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Native Only |
| 38 | Bitrue | 1,045 | 24 | 0 | 309 | 0 | 2,380 | 124 | 124 | 790 | 81 | 0 | REST |
| 39 | BloFin | 413 | 1,209 | 2,501 | 2,879 | 3,442 | 1,600 | 81 | 80 | 540 | 59 | 0 | Pro |
| 40 | DigiFinex | 12,961 | 6,936 | 0 | 0 | 0 | 1,180 | 62 | 62 | 810 | 81 | 0 | Native |
| 41 | EXMO | 57 | 1,890 | 2,639 | 3,732 | 2,051 | 1,580 | 80 | 79 | 2,100 | 110 | 0 | Pro |
| 42 | CEX.IO | 503 | 26 | 0 | 0 | 0 | 800 | 83 | 93 | 564 | 28 | 0 | REST |
| 43 | OrangeX | 4,560 | 81 | 0 | 0 | 0 | 0 | 0 | 0 | 1,340 | 72 | 0 | Native Only |
| 44 | Azbit | 200 | 38 | 0 | 0 | 0 | 0 | 0 | 0 | 860 | 45 | 0 | Native Only |
| 45 | BVOX | 335 | 67 | 0 | 0 | 0 | 0 | 0 | 0 | 315 | 66 | 0 | Native Only |
| 46 | Trubit Pro | 425 | 88 | 0 | 0 | 0 | 0 | 0 | 0 | 475 | 99 | 0 | Native Only |
| 47 | BigONE | 20,890 | 122 | 0 | 0 | 0 | 2,850 | 159 | 160 | 1,355 | 76 | 0 | Native |
| 48 | LATOKEN | 855 | 49 | 0 | 0 | 0 | 1,630 | 98 | 86 | 0 | 0 | 0 | REST |
| 49 | Coinstore | 6,073 | 1,169 | 0 | 0 | 0 | 0 | 0 | 0 | 415 | 0 | 0 | Native Only |
| 50 | GroveX | 651 | 1,606 | 0 | 0 | 0 | 0 | 0 | 0 | 1,080 | 56 | 0 | Native Only |
| 51 | CoinW | 530 | 57 | 0 | 0 | 0 | 0 | 0 | 0 | 1,200 | 60 | 0 | Native Only |
| 52 | Batonex | 250 | 50 | 0 | 0 | 0 | 0 | 0 | 0 | 295 | 61 | 0 | Native Only |
| 53 | CEEX | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | None |

## Score Summary

| Category | Count |
|----------|-------|
| ✅ Native Wins | 13 |
| 🔵 CCXT Pro Wins | 10 |
| 🟠 CCXT REST Wins | 5 |
| 🟣 Direct REST Wins | 0 |
| ≈ Ties | 7 |
| 🟢 Native Only (no CCXT) | 17 |
| ❌ All Failed | 1 |
| **Total** | **53** |

## 🏥 Health Analysis

**Average Health Score:** 92/100

**🟡 Warnings (50-79):** KuCoin, AscendEX, BingX, Bullish

| Exchange | Health | Errors | Top Error Category | Fix Recommendation |
|----------|--------|--------|-------------------|--------------------|
| Binance | 🟢 100 | 15 | unknown(15) | - |
| Coinbase | 🟢 85 | 1039 | unknown(703) | [high] Reduce subscription count per connection |
| Kraken | 🟢 85 | 757 | timeout(753) | [high] Reduce subscription count per connection |
| KuCoin | 🟡 70 | 421 | timeout(362) | [high] Add multi-endpoint failover URLs |
| OKX | 🟢 85 | 538 | timeout(537) | [high] Reduce subscription count per connection |
| Bybit | 🟢 85 | 573 | timeout(566) | [high] Reduce subscription count per connection |
| Bitfinex | 🟢 100 | 1 | unknown(1) | - |
| Gate.io | 🟢 85 | 428 | timeout(408) | [high] Reduce subscription count per connection |
| HTX | 🟢 100 | 15 | unknown(13) | - |
| WOO X | 🟢 85 | 532 | timeout(531) | [high] Reduce subscription count per connection |
| Crypto.com | 🟢 85 | 489 | timeout(477) | [high] Reduce subscription count per connection |
| Bitstamp | 🟢 85 | 358 | timeout(307) | [high] Reduce subscription count per connection |
| WhiteBIT | 🟢 100 | 5 | unknown(3) | - |
| AscendEX | 🟡 70 | 136 | timeout(66) | [high] Add multi-endpoint failover URLs |
| BingX | 🟡 78 | 151 | connectionClosed(130) | [high] Add multi-endpoint failover URLs |
| Toobit | 🟢 85 | 67 | timeout(66) | [high] Reduce subscription count per connection |
| Deepcoin | 🟢 100 | 1 | unknown(1) | - |
| XT.com | 🟢 100 | 1 | unknown(1) | - |
| Zoomex | 🟢 100 | 8 | unknown(8) | - |
| Bitget | 🟢 85 | 551 | unknown(304) | [high] Reduce subscription count per connection |
| Gemini | 🟢 96 | 25 | unknown(23) | - |
| Binance.US | 🟢 85 | 276 | timeout(249) | [high] Reduce subscription count per connection |
| MEXC | 🟢 100 | 6 | unknown(6) | - |
| CoinEx | 🟢 85 | 331 | timeout(330) | [high] Reduce subscription count per connection |
| LBank | 🟢 97 | 28 | unknown(28) | - |
| BitMart | 🟢 85 | 556 | timeout(552) | [high] Reduce subscription count per connection |
| Pionex | 🟢 100 | 9 | unknown(7) | - |
| Poloniex | 🟢 100 | 5 | unknown(5) | - |
| HitBTC | 🟢 85 | 194 | timeout(189) | [high] Reduce subscription count per connection |
| BTSE | 🟢 100 | 4 | unknown(4) | - |
| Biconomy | 🟢 100 | 1 | unknown(1) | - |
| Hotcoin | 🟢 100 | 10 | unknown(10) | - |
| NovaEx | 🟢 100 | 4 | unknown(4) | - |
| FameEX | 🟢 82 | 51 | unknown(43) | [high] Reduce subscription count per connection |
| Websea | 🟢 100 | 4 | unknown(4) | - |
| Bullish | 🟡 70 | 417 | rateLimit(234) | [high] Add multi-endpoint failover URLs |
| Darkex | 🟢 100 | 10 | unknown(8) | - |
| Bitrue | 🟢 85 | 388 | timeout(314) | [high] Reduce subscription count per connection |
| BloFin | 🟢 85 | 196 | timeout(192) | [high] Reduce subscription count per connection |
| DigiFinex | 🟢 100 | 2 | pairNotFound(1) | [medium] Verify pair availability on exchange |
| EXMO | 🟢 85 | 295 | timeout(290) | [high] Reduce subscription count per connection |
| CEX.IO | 🟢 100 | 0 | - | - |
| OrangeX | 🟢 100 | 0 | - | - |
| Azbit | 🟢 100 | 0 | - | - |
| BVOX | 🟢 100 | 0 | - | - |
| Trubit Pro | 🟢 100 | 0 | - | - |
| BigONE | 🟢 100 | 0 | - | - |
| LATOKEN | 🟢 100 | 2 | pairNotFound(2) | [medium] Verify pair availability on exchange |
| Coinstore | 🟢 100 | 7 | unknown(5) | - |
| GroveX | 🟢 100 | 1 | unknown(1) | - |
| CoinW | 🟢 100 | 0 | - | - |
| Batonex | 🟢 100 | 0 | - | - |
| CEEX | 🟢 84 | 54 | unknown(54) | - |

## Per-Exchange Per-Pair Detail

### Binance (CCXT: binance) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDC | 228 | 97 | 0 | 0 | 160 | 8 | 60 | 6 | ✅ Native |
| BTC_USDT | 819 | 106 | 0 | 0 | 160 | 8 | 70 | 7 | ✅ Native |
| ENA_USDC | 159 | 34 | 0 | 0 | 100 | 7 | 60 | 7 | ✅ Native |
| ENA_USDT | 142 | 64 | 0 | 0 | 140 | 7 | 60 | 7 | ✅ Native |
| ETH_USDC | 249 | 95 | 0 | 0 | 140 | 7 | 70 | 6 | ✅ Native |
| ETH_USDT | 816 | 105 | 0 | 0 | 160 | 8 | 70 | 7 | ✅ Native |
| PENGU_USDC | 154 | 53 | 0 | 0 | 140 | 7 | 70 | 7 | ✅ Native |
| PENGU_USDT | 200 | 77 | 0 | 0 | 160 | 8 | 60 | 6 | ✅ Native |
| SOL_USDC | 173 | 79 | 0 | 0 | 160 | 8 | 70 | 7 | ✅ Native |
| SOL_USDT | 331 | 93 | 0 | 0 | 160 | 8 | 70 | 7 | ✅ Native |
| SUI_USDC | 157 | 70 | 0 | 0 | 140 | 7 | 70 | 7 | ✅ Native |
| SUI_USDT | 187 | 85 | 0 | 0 | 120 | 7 | 70 | 7 | ✅ Native |
| WIF_USDC | 155 | 18 | 0 | 0 | 140 | 6 | 70 | 6 | ≈ Tie |
| WIF_USDT | 160 | 36 | 0 | 0 | 140 | 7 | 70 | 7 | ✅ Native |

### Coinbase (CCXT: coinbase) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USD | 357 | 613 | 1625 | 309 | 120 | 6 | 40 | 6 | 🔵 Pro |
| BTC_USDC | 0 | 0 | 242524 | 309 | 100 | 5 | 0 | 0 | 🔵 Pro |
| BTC_USDT | 8 | 594 | 37 | 288 | 120 | 6 | 50 | 4 | ✅ Native |
| ENA_USD | 11 | 257 | 12 | 32 | 100 | 5 | 50 | 5 | ✅ Native |
| ENA_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ❌ |
| ETH_USD | 231 | 609 | 687 | 306 | 100 | 6 | 50 | 5 | 🔵 Pro |
| ETH_USDC | 0 | 0 | 45476 | 306 | 100 | 5 | 0 | 0 | 🔵 Pro |
| ETH_USDT | 17 | 606 | 131 | 609 | 120 | 6 | 50 | 5 | 🔵 Pro |
| PENGU_USD | 33 | 416 | 82 | 252 | 100 | 5 | 50 | 5 | ✅ Native |
| PENGU_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ❌ |
| POPCAT_USD | 3 | 148 | 7 | 283 | 100 | 5 | 50 | 5 | 🔵 Pro |
| POPCAT_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ❌ |
| SOL_USD | 147 | 595 | 210 | 295 | 100 | 6 | 50 | 5 | ✅ Native |
| SOL_USDC | 0 | 0 | 5069 | 295 | 100 | 5 | 0 | 0 | 🔵 Pro |
| SOL_USDT | 7 | 545 | 41 | 291 | 100 | 5 | 40 | 5 | ✅ Native |
| SUI_USD | 81 | 570 | 150 | 1575 | 100 | 5 | 50 | 5 | 🔵 Pro |
| SUI_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ❌ |
| WIF_USD | 5 | 250 | 10 | 417 | 100 | 5 | 40 | 5 | 🔵 Pro |
| WIF_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ❌ |

**Native Errors:** Unexpected server response: 530; Opening handshake has timed out

**CCXT Pro Errors:** ETH/USDC:tr:rate limit exceeded; WIF/USD:ob:rate limit exceeded; WIF/USDC:ob:rate limit exceeded

**Fix Recommendations:** [high] Reduce subscription count per connection

### Kraken (CCXT: kraken) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USD | 34 | 2976 | 124 | 1227 | 120 | 6 | 0 | 0 | ✅ Native |
| BTC_USDC | 2 | 1908 | 18 | 955 | 120 | 6 | 0 | 0 | ✅ Native |
| BTC_USDT | 0 | 2938 | 12 | 1045 | 120 | 6 | 0 | 0 | ✅ Native |
| ENA_USD | 6 | 812 | 0 | 254 | 100 | 5 | 70 | 7 | ✅ Native |
| ETH_USD | 29 | 2856 | 52 | 1151 | 120 | 6 | 80 | 8 | ✅ Native |
| ETH_USDC | 2 | 1971 | 0 | 820 | 120 | 6 | 60 | 6 | ✅ Native |
| ETH_USDT | 0 | 2359 | 0 | 904 | 120 | 5 | 80 | 8 | ✅ Native |
| PENGU_USD | 9 | 1051 | 16 | 434 | 100 | 5 | 70 | 7 | ✅ Native |
| PENGU_USDC | 0 | 240 | 0 | 123 | 100 | 5 | 70 | 7 | ✅ Native |
| PENGU_USDT | 0 | 308 | 0 | 187 | 100 | 5 | 70 | 7 | ✅ Native |
| POPCAT_USD | 0 | 281 | 0 | 208 | 100 | 5 | 70 | 6 | ✅ Native |
| SOL_USD | 11 | 2943 | 19 | 1094 | 120 | 6 | 80 | 7 | ✅ Native |
| SOL_USDC | 0 | 654 | 0 | 327 | 120 | 5 | 70 | 7 | ✅ Native |
| SOL_USDT | 4 | 1814 | 1 | 713 | 120 | 6 | 80 | 8 | ✅ Native |
| SUI_USD | 7 | 1519 | 0 | 606 | 100 | 5 | 60 | 7 | ✅ Native |
| WIF_USD | 0 | 973 | 0 | 343 | 100 | 5 | 70 | 7 | ✅ Native |
| XBT_USD | 0 | 0 | 0 | 0 | 0 | 0 | 80 | 8 | 🟣 Direct |
| XBT_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 70 | 7 | 🟣 Direct |
| XBT_USDT | 0 | 0 | 0 | 0 | 0 | 0 | 70 | 8 | 🟣 Direct |

**Fix Recommendations:** [high] Reduce subscription count per connection

### KuCoin (CCXT: kucoin) — Health: 70/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 3 | 412 | 0 | 60 | 160 | 0 | 100 | 5 | ✅ Native |
| BTC_USDC | 361 | 1788 | 17 | 182 | 160 | 0 | 100 | 5 | ✅ Native |
| BTC_USDT | 2616 | 2263 | 264 | 2101 | 160 | 0 | 100 | 4 | ✅ Native |
| ENA_USDC | 0 | 186 | 0 | 1 | 120 | 0 | 80 | 4 | ✅ Native |
| ENA_USDT | 41 | 1429 | 1 | 33 | 120 | 0 | 100 | 5 | ✅ Native |
| ETH_USDC | 1045 | 1902 | 123 | 358 | 160 | 0 | 80 | 5 | ✅ Native |
| ETH_USDT | 1743 | 2153 | 262 | 1405 | 160 | 0 | 80 | 5 | ✅ Native |
| PENGU_USDT | 61 | 1326 | 3 | 254 | 160 | 0 | 100 | 5 | ✅ Native |
| POPCAT_USDT | 1 | 1044 | 0 | 2 | 160 | 0 | 100 | 5 | ✅ Native |
| SOL_USDC | 19 | 1473 | 0 | 210 | 160 | 0 | 100 | 5 | ✅ Native |
| SOL_USDT | 460 | 2047 | 67 | 2058 | 160 | 0 | 100 | 5 | ≈ Tie |
| SUI_USDC | 5 | 963 | 0 | 71 | 120 | 0 | 100 | 5 | ✅ Native |
| SUI_USDT | 246 | 1995 | 28 | 465 | 140 | 0 | 100 | 5 | ✅ Native |
| WIF_USDC | 0 | 270 | 0 | 5 | 140 | 0 | 100 | 5 | ✅ Native |
| WIF_USDT | 56 | 1268 | 15 | 203 | 140 | 0 | 100 | 5 | ✅ Native |

**Native Errors:** Opening handshake has timed out

**CCXT Pro Errors:** BTC/USDC:tk:connection closed by remote server, clos; SUI/USDT:tk:connection closed by remote server, clos; WIF/USDT:tr:connection closed by remote server, clos

**Fix Recommendations:** [high] Add multi-endpoint failover URLs; [high] Reduce subscription count per connection

### OKX (CCXT: okx) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDC | 0 | 345 | 0 | 69 | 120 | 6 | 40 | 4 | ✅ Native |
| BTC_USDT | 909 | 743 | 2840 | 76 | 120 | 6 | 20 | 4 | 🔵 Pro |
| ENA_USD | 0 | 178 | 0 | 34 | 100 | 5 | 40 | 4 | ✅ Native |
| ENA_USDC | 0 | 162 | 0 | 33 | 0 | 5 | 0 | 4 | ✅ Native |
| ENA_USDT | 27 | 421 | 98 | 57 | 100 | 5 | 30 | 4 | ✅ Native |
| ETH_USDC | 0 | 433 | 0 | 63 | 120 | 6 | 40 | 4 | ✅ Native |
| ETH_USDT | 620 | 730 | 2793 | 77 | 120 | 6 | 50 | 5 | 🔵 Pro |
| PENGU_USD | 0 | 306 | 0 | 37 | 100 | 5 | 40 | 4 | ✅ Native |
| PENGU_USDC | 0 | 246 | 0 | 32 | 120 | 6 | 40 | 4 | ✅ Native |
| PENGU_USDT | 36 | 546 | 36 | 74 | 100 | 6 | 40 | 4 | ✅ Native |
| SOL_USDC | 0 | 263 | 0 | 48 | 100 | 5 | 30 | 3 | ✅ Native |
| SOL_USDT | 159 | 758 | 700 | 76 | 120 | 6 | 50 | 5 | ≈ Tie |
| SUI_USD | 8 | 556 | 0 | 79 | 80 | 5 | 30 | 3 | ✅ Native |
| SUI_USDC | 0 | 363 | 0 | 51 | 100 | 5 | 40 | 4 | ✅ Native |
| SUI_USDT | 184 | 703 | 720 | 67 | 100 | 5 | 40 | 4 | ≈ Tie |
| WIF_USD | 0 | 231 | 0 | 31 | 100 | 5 | 40 | 4 | ✅ Native |
| WIF_USDC | 0 | 128 | 0 | 36 | 100 | 5 | 40 | 4 | ✅ Native |
| WIF_USDT | 8 | 425 | 22 | 59 | 100 | 5 | 40 | 4 | ✅ Native |

**Fix Recommendations:** [high] Reduce subscription count per connection

### Bybit (CCXT: bybit) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDC | 0 | 161 | 0 | 35 | 140 | 7 | 80 | 8 | ≈ Tie |
| BRETT_USDT | 27 | 500 | 0 | 160 | 140 | 7 | 70 | 8 | ✅ Native |
| BTC_USDC | 259 | 3250 | 288 | 792 | 120 | 6 | 70 | 8 | ✅ Native |
| BTC_USDT | 1850 | 4762 | 2515 | 1256 | 140 | 7 | 70 | 8 | ✅ Native |
| ENA_USDC | 0 | 28 | 0 | 19 | 120 | 6 | 70 | 7 | 🟠 REST |
| ENA_USDT | 31 | 551 | 1 | 169 | 120 | 6 | 70 | 7 | ✅ Native |
| ETH_USDC | 409 | 2799 | 563 | 694 | 140 | 7 | 80 | 7 | ✅ Native |
| ETH_USDT | 1164 | 3515 | 1628 | 928 | 140 | 7 | 80 | 8 | ✅ Native |
| PENGU_USDT | 52 | 1339 | 0 | 355 | 140 | 7 | 80 | 7 | ✅ Native |
| POPCAT_USDT | 28 | 450 | 0 | 155 | 140 | 7 | 80 | 8 | ✅ Native |
| SOL_USDC | 5 | 1486 | 0 | 427 | 120 | 7 | 80 | 8 | ✅ Native |
| SOL_USDT | 265 | 2364 | 384 | 643 | 140 | 7 | 80 | 7 | ✅ Native |
| SUI_USDC | 15 | 1299 | 0 | 348 | 120 | 6 | 70 | 7 | ✅ Native |
| SUI_USDT | 204 | 2528 | 257 | 688 | 140 | 7 | 60 | 7 | ✅ Native |
| WIF_USDC | 0 | 268 | 0 | 111 | 140 | 7 | 60 | 7 | ✅ Native |
| WIF_USDT | 48 | 608 | 0 | 221 | 140 | 7 | 70 | 8 | ✅ Native |

**Fix Recommendations:** [high] Reduce subscription count per connection

### Bitfinex (CCXT: bitfinex) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USD | 2571 | 50625 | 0 | 0 | 60 | 0 | 64 | 16 | ✅ Native |
| BTC_USDT | 1008 | 46343 | 0 | 0 | 60 | 0 | 94 | 15 | ✅ Native |
| ENA_USD | 0 | 0 | 0 | 0 | 60 | 0 | 80 | 15 | 🟣 Direct |
| ENA_USDT | 0 | 0 | 0 | 0 | 60 | 0 | 80 | 15 | 🟣 Direct |
| ETH_USD | 532 | 31854 | 0 | 0 | 60 | 0 | 94 | 15 | ✅ Native |
| ETH_USDT | 390 | 31489 | 0 | 0 | 80 | 0 | 87 | 15 | ✅ Native |
| SOL_USD | 190 | 34411 | 0 | 0 | 60 | 0 | 94 | 15 | ✅ Native |
| SOL_USDT | 202 | 36081 | 0 | 0 | 100 | 0 | 87 | 15 | ✅ Native |
| SUI_USD | 0 | 0 | 0 | 0 | 40 | 0 | 80 | 15 | 🟣 Direct |
| SUI_USDT | 0 | 0 | 0 | 0 | 100 | 0 | 80 | 15 | 🟠 REST |

### Gate.io (CCXT: gateio) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 152 | 36 | 0 | 3 | 180 | 9 | 40 | 4 | ≈ Tie |
| BTC_USDC | 1 | 44 | 0 | 1 | 180 | 9 | 50 | 5 | 🟠 REST |
| BTC_USDT | 451 | 46 | 8 | 1 | 200 | 10 | 40 | 4 | ✅ Native |
| ENA_USDC | 1 | 38 | 0 | 2 | 180 | 9 | 40 | 4 | 🟠 REST |
| ENA_USDT | 148 | 45 | 36 | 2 | 180 | 9 | 40 | 3 | ≈ Tie |
| ETH_USDC | 3 | 46 | 0 | 1 | 180 | 9 | 40 | 5 | 🟠 REST |
| ETH_USDT | 572 | 47 | 16 | 1 | 200 | 10 | 40 | 5 | ✅ Native |
| PENGU_USDT | 162 | 44 | 0 | 3 | 160 | 9 | 40 | 4 | ✅ Native |
| POPCAT_USDT | 145 | 45 | 4 | 2 | 160 | 9 | 30 | 3 | ≈ Tie |
| SOL_USDC | 0 | 44 | 0 | 25 | 180 | 9 | 50 | 5 | 🟠 REST |
| SOL_USDT | 203 | 46 | 102 | 1 | 180 | 10 | 50 | 5 | ✅ Native |
| SUI_USDC | 3 | 45 | 0 | 26 | 180 | 9 | 30 | 4 | 🟠 REST |
| SUI_USDT | 179 | 44 | 101 | 1 | 180 | 9 | 30 | 4 | ≈ Tie |
| WIF_USDC | 0 | 50 | 0 | 2 | 180 | 9 | 40 | 4 | 🟠 REST |
| WIF_USDT | 136 | 39 | 17 | 2 | 180 | 9 | 30 | 4 | ≈ Tie |

**CCXT Pro Errors:** SOL/USDT:ob:read ECONNRESET; SUI/USDT:ob:read ECONNRESET; ETH/USDT:ob:read ECONNRESET

**Fix Recommendations:** [high] Reduce subscription count per connection

### HTX (CCXT: htx) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 168 | 289 | 0 | 0 | 140 | 7 | 7 | 8 | ✅ Native |
| BTC_USDC | 6 | 358 | 0 | 0 | 140 | 7 | 8 | 9 | ✅ Native |
| BTC_USDT | 252 | 296 | 0 | 0 | 140 | 7 | 7 | 8 | ✅ Native |
| ENA_USDT | 120 | 290 | 0 | 0 | 100 | 6 | 8 | 8 | ✅ Native |
| ETH_USDC | 4 | 358 | 0 | 0 | 140 | 7 | 8 | 9 | ✅ Native |
| ETH_USDT | 112 | 357 | 0 | 0 | 140 | 7 | 13 | 9 | ✅ Native |
| PENGU_USDT | 79 | 305 | 0 | 0 | 120 | 6 | 10 | 8 | ✅ Native |
| POPCAT_USDT | 140 | 276 | 0 | 0 | 120 | 6 | 8 | 8 | ✅ Native |
| SOL_USDT | 61 | 358 | 0 | 0 | 140 | 7 | 17 | 9 | ✅ Native |
| SUI_USDT | 194 | 357 | 0 | 0 | 120 | 6 | 8 | 8 | ✅ Native |
| WIF_USDT | 17 | 306 | 0 | 0 | 100 | 6 | 13 | 7 | ✅ Native |

**Native Errors:** Opening handshake has timed out; getaddrinfo EAI_AGAIN api-aws.huobi.pro

### WOO X (CCXT: woo) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 1 | 115 | 30 | 202 | 440 | 22 | 120 | 11 | 🟠 REST |
| BTC_USDC | 66 | 117 | 773 | 213 | 440 | 22 | 0 | 0 | 🔵 Pro |
| BTC_USDT | 3 | 126 | 2 | 215 | 440 | 22 | 110 | 13 | 🟠 REST |
| ENA_USDT | 21 | 115 | 2 | 193 | 420 | 21 | 120 | 12 | 🟠 REST |
| ETH_USDC | 98 | 117 | 1044 | 203 | 440 | 22 | 0 | 0 | 🔵 Pro |
| ETH_USDT | 156 | 142 | 1348 | 255 | 440 | 22 | 130 | 13 | 🔵 Pro |
| PENGU_USDT | 56 | 120 | 430 | 232 | 440 | 21 | 110 | 12 | 🔵 Pro |
| POPCAT_USDT | 8 | 104 | 13 | 148 | 440 | 21 | 120 | 12 | 🟠 REST |
| SOL_USDT | 40 | 136 | 386 | 250 | 440 | 22 | 120 | 12 | 🔵 Pro |
| SUI_USDT | 9 | 141 | 112 | 249 | 420 | 20 | 120 | 11 | 🟠 REST |
| WIF_USDT | 16 | 99 | 26 | 186 | 420 | 20 | 110 | 12 | 🟠 REST |

**Fix Recommendations:** [high] Reduce subscription count per connection

### Crypto.com (CCXT: cryptocom) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USD | 2301 | 404 | 2379 | 124 | 220 | 11 | 50 | 6 | ≈ Tie |
| BTC_USDC | 0 | 404 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| BTC_USDT | 1907 | 403 | 1713 | 122 | 220 | 11 | 50 | 4 | ✅ Native |
| ENA_USD | 200 | 402 | 350 | 114 | 200 | 10 | 40 | 5 | ✅ Native |
| ENA_USDT | 200 | 404 | 350 | 121 | 220 | 11 | 40 | 5 | ✅ Native |
| ETH_USD | 3056 | 402 | 2931 | 114 | 220 | 11 | 60 | 6 | ≈ Tie |
| ETH_USDC | 0 | 404 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDT | 2279 | 403 | 2254 | 123 | 220 | 10 | 60 | 6 | ≈ Tie |
| PENGU_USD | 200 | 403 | 350 | 121 | 220 | 11 | 60 | 5 | ✅ Native |
| PENGU_USDT | 200 | 403 | 350 | 122 | 220 | 11 | 60 | 6 | ✅ Native |
| POPCAT_USD | 200 | 403 | 350 | 123 | 220 | 11 | 50 | 5 | ✅ Native |
| SOL_USD | 524 | 403 | 615 | 121 | 200 | 11 | 60 | 6 | ✅ Native |
| SOL_USDC | 0 | 406 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SOL_USDT | 393 | 405 | 506 | 115 | 220 | 11 | 60 | 6 | ✅ Native |
| SUI_USD | 655 | 403 | 495 | 115 | 220 | 11 | 50 | 4 | ✅ Native |
| SUI_USDT | 446 | 403 | 412 | 115 | 220 | 11 | 40 | 4 | ✅ Native |
| WIF_USD | 200 | 405 | 351 | 123 | 220 | 11 | 30 | 5 | ✅ Native |
| WIF_USDT | 200 | 403 | 350 | 122 | 200 | 11 | 50 | 5 | ✅ Native |

**Native Errors:** Opening handshake has timed out; read ECONNRESET

**Fix Recommendations:** [high] Reduce subscription count per connection

### Bitstamp (CCXT: bitstamp) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USD | 6 | 31 | 487 | 46 | 20 | 2 | 0 | 0 | 🔵 Pro |
| BTC_USDC | 0 | 14 | 2 | 11 | 40 | 2 | 20 | 0 | 🟠 REST |
| BTC_USDT | 0 | 21 | 5 | 22 | 40 | 2 | 20 | 1 | 🟠 REST |
| ENA_USD | 0 | 3 | 0 | 3 | 0 | 2 | 0 | 1 | ≈ Tie |
| ETH_USD | 1 | 16 | 189 | 87 | 40 | 1 | 0 | 1 | 🔵 Pro |
| ETH_USDC | 0 | 19 | 0 | 17 | 40 | 2 | 20 | 1 | 🟠 REST |
| ETH_USDT | 0 | 15 | 0 | 13 | 40 | 2 | 20 | 1 | 🟠 REST |
| PENGU_USD | 0 | 13 | 7 | 15 | 20 | 1 | 0 | 1 | 🔵 Pro |
| POPCAT_USD | 0 | 4 | 1 | 1 | 23 | 2 | 5 | 1 | 🟠 REST |
| SOL_USD | 3 | 20 | 131 | 1 | 20 | 2 | 0 | 0 | 🔵 Pro |
| SOL_USDC | 0 | 17 | 1 | 1 | 40 | 2 | 20 | 1 | 🟠 REST |
| SUI_USD | 0 | 4 | 0 | 4 | 40 | 2 | 20 | 1 | 🟠 REST |
| WIF_USD | 0 | 4 | 0 | 6 | 40 | 2 | 20 | 1 | 🟠 REST |

**CCXT Pro Errors:** BTC/USD:tk:notSupported; ETH/USD:tk:notSupported; SOL/USD:tk:notSupported

**Fix Recommendations:** [high] Reduce subscription count per connection; [low] Use REST fallback for unsupported methods

### WhiteBIT (CCXT: whitebit) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USD | 100 | 112 | 0 | 0 | 80 | 5 | 0 | 0 | ✅ Native |
| BTC_USDC | 108 | 107 | 0 | 0 | 100 | 5 | 40 | 2 | ✅ Native |
| BTC_USDT | 201 | 188 | 0 | 0 | 100 | 5 | 40 | 2 | ✅ Native |
| ENA_USDC | 100 | 37 | 0 | 0 | 80 | 4 | 20 | 1 | ✅ Native |
| ENA_USDT | 123 | 186 | 0 | 0 | 80 | 4 | 20 | 1 | ✅ Native |
| ETH_USD | 100 | 98 | 0 | 0 | 100 | 5 | 0 | 0 | ✅ Native |
| ETH_USDC | 107 | 103 | 0 | 0 | 100 | 5 | 40 | 2 | ✅ Native |
| ETH_USDT | 160 | 186 | 0 | 0 | 100 | 5 | 40 | 2 | ✅ Native |
| PENGU_USDC | 101 | 81 | 0 | 0 | 100 | 5 | 40 | 2 | ✅ Native |
| PENGU_USDT | 108 | 124 | 0 | 0 | 100 | 4 | 40 | 2 | ✅ Native |
| SOL_USD | 100 | 159 | 0 | 0 | 100 | 5 | 0 | 0 | ✅ Native |
| SOL_USDC | 105 | 162 | 0 | 0 | 100 | 4 | 40 | 2 | ✅ Native |
| SOL_USDT | 201 | 188 | 0 | 0 | 80 | 5 | 40 | 2 | ✅ Native |
| SUI_USDC | 100 | 111 | 0 | 0 | 80 | 4 | 20 | 1 | ✅ Native |
| SUI_USDT | 119 | 185 | 0 | 0 | 100 | 5 | 20 | 1 | ✅ Native |
| WIF_USDC | 100 | 49 | 0 | 0 | 100 | 5 | 20 | 1 | ✅ Native |
| WIF_USDT | 119 | 105 | 0 | 0 | 100 | 5 | 40 | 2 | ✅ Native |

**Native Errors:** Opening handshake has timed out

### AscendEX (CCXT: ascendex) — Health: 70/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 12 | 65 | 14 | 61 | 340 | 17 | 0 | 21 | 🟠 REST |
| BTC_USD | 21 | 491 | 28 | 449 | 340 | 17 | 0 | 0 | ≈ Tie |
| BTC_USDT | 651 | 2206 | 714 | 2060 | 340 | 17 | 0 | 20 | ≈ Tie |
| ENA_USDT | 120 | 92 | 126 | 72 | 340 | 17 | 0 | 20 | 🟠 REST |
| ETH_USD | 35 | 658 | 38 | 593 | 340 | 17 | 0 | 0 | ≈ Tie |
| ETH_USDT | 639 | 2207 | 708 | 2059 | 360 | 18 | 0 | 21 | ≈ Tie |
| PENGU_USDT | 13 | 149 | 14 | 136 | 340 | 17 | 0 | 21 | 🟠 REST |
| SOL_USD | 33 | 232 | 31 | 212 | 340 | 17 | 0 | 0 | 🟠 REST |
| SOL_USDT | 236 | 2184 | 260 | 2037 | 320 | 17 | 0 | 20 | ≈ Tie |
| SUI_USDT | 163 | 233 | 172 | 210 | 340 | 17 | 0 | 20 | ≈ Tie |
| WIF_USDT | 138 | 341 | 158 | 270 | 340 | 17 | 0 | 21 | ≈ Tie |

**CCXT Pro Errors:** SOL/USDT:ob:connection closed by remote server, clos; BTC/USDT:ob:connection closed by remote server, clos; ETH/USDT:ob:connection closed by remote server, clos

**Fix Recommendations:** [high] Add multi-endpoint failover URLs; [high] Reduce subscription count per connection; [low] Use REST fallback for unsupported methods

### BingX (CCXT: bingx) — Health: 78/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 57 | 286 | 19 | 106 | 280 | 14 | 150 | 14 | ≈ Tie |
| BTC_USDC | 536 | 553 | 201 | 230 | 280 | 15 | 150 | 15 | ✅ Native |
| BTC_USDT | 532 | 545 | 195 | 227 | 300 | 15 | 150 | 14 | ✅ Native |
| ENA_USDT | 525 | 354 | 207 | 153 | 260 | 13 | 130 | 13 | ✅ Native |
| ETH_USDC | 532 | 593 | 190 | 252 | 260 | 14 | 150 | 15 | ✅ Native |
| ETH_USDT | 482 | 557 | 207 | 230 | 300 | 15 | 150 | 14 | ✅ Native |
| PENGU_USDT | 531 | 442 | 183 | 184 | 280 | 14 | 150 | 15 | ✅ Native |
| POPCAT_USDT | 214 | 234 | 54 | 85 | 280 | 14 | 140 | 15 | ✅ Native |
| SOL_USDC | 546 | 467 | 224 | 203 | 280 | 14 | 150 | 15 | ✅ Native |
| SOL_USDT | 729 | 507 | 283 | 205 | 300 | 15 | 150 | 15 | ✅ Native |
| SUI_USDC | 609 | 463 | 251 | 194 | 280 | 14 | 130 | 14 | ✅ Native |
| SUI_USDT | 645 | 437 | 272 | 185 | 280 | 14 | 150 | 13 | ✅ Native |
| WIF_USDT | 275 | 202 | 91 | 75 | 280 | 14 | 150 | 15 | ✅ Native |

**Native Errors:** Unexpected server response: 403

**CCXT Pro Errors:** BTC/USDT:ob:connection closed by remote server, clos; ENA/USDT:ob:connection closed by remote server, clos; SOL/USDT:tr:connection closed by remote server, clos

**Fix Recommendations:** [high] Add multi-endpoint failover URLs

### Toobit (CCXT: toobit) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDC | 89 | 47 | 75 | 18 | 140 | 7 | 0 | 0 | ≈ Tie |
| BTC_USDT | 115 | 52 | 91 | 20 | 120 | 6 | 0 | 0 | ✅ Native |
| ENA_USDT | 67 | 21 | 62 | 5 | 120 | 6 | 0 | 0 | 🟠 REST |
| ETH_USDC | 90 | 44 | 74 | 19 | 140 | 7 | 0 | 0 | ≈ Tie |
| ETH_USDT | 102 | 57 | 78 | 30 | 120 | 7 | 0 | 0 | ✅ Native |
| PENGU_USDT | 75 | 40 | 65 | 22 | 140 | 7 | 0 | 0 | 🟠 REST |
| POPCAT_USDT | 60 | 4 | 61 | 4 | 140 | 7 | 0 | 0 | 🟠 REST |
| SOL_USDC | 79 | 50 | 67 | 23 | 140 | 7 | 0 | 0 | ≈ Tie |
| SOL_USDT | 101 | 48 | 83 | 21 | 120 | 7 | 0 | 0 | ≈ Tie |
| SUI_USDT | 88 | 53 | 73 | 35 | 120 | 7 | 0 | 0 | ≈ Tie |
| WIF_USDT | 66 | 14 | 60 | 1 | 140 | 7 | 0 | 0 | 🟠 REST |

**Fix Recommendations:** [high] Reduce subscription count per connection

### Deepcoin (CCXT: deepcoin) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 0 | 31 | 0 | 0 | 200 | 10 | 0 | 0 | 🟠 REST |
| ETH_USDT | 0 | 33 | 0 | 0 | 200 | 10 | 0 | 0 | 🟠 REST |
| PENGU_USDT | 0 | 33 | 0 | 0 | 200 | 10 | 0 | 0 | 🟠 REST |
| SOL_USDT | 0 | 32 | 0 | 0 | 200 | 10 | 0 | 0 | 🟠 REST |
| WIF_USDT | 0 | 33 | 0 | 0 | 160 | 9 | 0 | 0 | 🟠 REST |

### XT.com (CCXT: xt) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 11 | 76 | 3 | 318 | 120 | 5 | 100 | 10 | 🔵 Pro |
| BTC_USDC | 20 | 76 | 68 | 364 | 120 | 6 | 100 | 9 | 🔵 Pro |
| BTC_USDT | 83 | 76 | 227 | 841 | 120 | 6 | 100 | 10 | 🔵 Pro |
| ENA_USDT | 18 | 76 | 29 | 645 | 100 | 5 | 100 | 10 | 🔵 Pro |
| ETH_USDC | 10 | 76 | 32 | 708 | 120 | 6 | 100 | 9 | 🔵 Pro |
| ETH_USDT | 91 | 76 | 245 | 754 | 120 | 6 | 100 | 9 | 🔵 Pro |
| PENGU_USDT | 29 | 76 | 59 | 672 | 120 | 6 | 90 | 10 | 🔵 Pro |
| POPCAT_USDT | 17 | 76 | 24 | 363 | 120 | 6 | 90 | 8 | 🔵 Pro |
| SOL_USDC | 11 | 76 | 34 | 299 | 120 | 6 | 90 | 9 | 🔵 Pro |
| SOL_USDT | 50 | 76 | 125 | 644 | 120 | 6 | 100 | 10 | 🔵 Pro |
| SUI_USDC | 12 | 76 | 40 | 295 | 80 | 6 | 100 | 10 | 🔵 Pro |
| SUI_USDT | 28 | 76 | 61 | 621 | 120 | 6 | 100 | 10 | 🔵 Pro |
| WIF_USDT | 14 | 76 | 10 | 580 | 120 | 6 | 90 | 10 | 🔵 Pro |

### Zoomex (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 1 | 632 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| BTC_USDT | 2110 | 5362 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ENA_USDT | 3 | 484 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDT | 919 | 3781 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| PENGU_USDT | 15 | 1119 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| POPCAT_USDT | 10 | 288 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SOL_USDT | 322 | 2357 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SUI_USDT | 144 | 2884 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| WIF_USDT | 2 | 617 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |

### Bitget (CCXT: bitget) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 115 | 300 | 355 | 108 | 120 | 6 | 110 | 11 | 🔵 Pro |
| BTC_USDC | 268 | 1128 | 470 | 337 | 120 | 6 | 110 | 11 | ✅ Native |
| BTC_USDT | 1937 | 1130 | 2574 | 307 | 140 | 7 | 100 | 11 | ≈ Tie |
| ENA_USDT | 107 | 582 | 355 | 149 | 100 | 6 | 100 | 10 | ✅ Native |
| ETH_USDC | 230 | 1210 | 465 | 314 | 120 | 6 | 100 | 11 | ✅ Native |
| ETH_USDT | 1677 | 1134 | 1975 | 285 | 140 | 7 | 110 | 11 | ✅ Native |
| PENGU_USDT | 193 | 896 | 374 | 228 | 120 | 6 | 110 | 10 | ✅ Native |
| SOL_USDC | 132 | 975 | 354 | 195 | 120 | 6 | 100 | 11 | ✅ Native |
| SOL_USDT | 1001 | 1235 | 909 | 381 | 120 | 6 | 110 | 11 | ✅ Native |
| SUI_USDC | 0 | 0 | 353 | 313 | 120 | 6 | 100 | 9 | 🔵 Pro |
| SUI_USDT | 634 | 1109 | 990 | 307 | 120 | 5 | 100 | 10 | ✅ Native |
| WIF_USDT | 125 | 744 | 356 | 161 | 120 | 6 | 100 | 10 | ✅ Native |

**CCXT Pro Errors:** SUI/USDC:ob:bitget orderbook SUI/USDC; ENA/USDT:ob:bitget orderbook ENA/USDT; PENGU/USDT:ob:bitget orderbook PENGU/USDT

**Fix Recommendations:** [high] Reduce subscription count per connection

### Gemini (CCXT: gemini) — Health: 96/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USD | 0 | 0 | 0 | 0 | 260 | 13 | 140 | 14 | 🟠 REST |
| BTC_USDC | 0 | 0 | 0 | 0 | 240 | 12 | 0 | 0 | 🟠 REST |
| BTC_USDT | 0 | 0 | 0 | 0 | 260 | 12 | 140 | 14 | 🟠 REST |
| ETH_USD | 0 | 0 | 0 | 0 | 260 | 13 | 140 | 14 | 🟠 REST |
| ETH_USDC | 0 | 0 | 0 | 0 | 240 | 12 | 0 | 0 | 🟠 REST |
| ETH_USDT | 0 | 0 | 0 | 0 | 260 | 13 | 130 | 14 | 🟠 REST |
| PENGU_USD | 0 | 0 | 0 | 0 | 240 | 12 | 140 | 14 | 🟠 REST |
| PENGU_USDC | 0 | 0 | 0 | 0 | 240 | 12 | 140 | 14 | 🟠 REST |
| POPCAT_USD | 0 | 0 | 0 | 0 | 240 | 12 | 130 | 13 | 🟠 REST |
| POPCAT_USDC | 0 | 0 | 0 | 0 | 240 | 12 | 140 | 14 | 🟠 REST |
| SOL_USD | 0 | 0 | 0 | 0 | 260 | 13 | 140 | 14 | 🟠 REST |
| SOL_USDC | 0 | 0 | 0 | 0 | 240 | 12 | 0 | 0 | 🟠 REST |
| WIF_USD | 0 | 0 | 0 | 0 | 240 | 12 | 140 | 14 | 🟠 REST |
| WIF_USDC | 0 | 0 | 0 | 0 | 240 | 12 | 130 | 13 | 🟠 REST |

**Native Errors:** Client network socket disconnected before secure TLS connect

### Binance.US (CCXT: binanceus) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 0 | 2 | 0 | 11 | 120 | 6 | 110 | 11 | 🟠 REST |
| BTC_USD | 9 | 168 | 21 | 938 | 120 | 6 | 120 | 12 | 🔵 Pro |
| BTC_USDC | 5 | 158 | 18 | 1880 | 120 | 6 | 0 | 0 | 🔵 Pro |
| BTC_USDT | 27 | 175 | 48 | 909 | 140 | 7 | 90 | 12 | 🔵 Pro |
| ENA_USDT | 0 | 4 | 0 | 22 | 100 | 6 | 110 | 11 | 🟣 Direct |
| ETH_USD | 7 | 176 | 56 | 10 | 120 | 6 | 120 | 11 | ✅ Native |
| ETH_USDC | 0 | 11 | 1 | 0 | 120 | 6 | 0 | 0 | 🟠 REST |
| ETH_USDT | 27 | 174 | 107 | 1384 | 140 | 7 | 110 | 12 | 🔵 Pro |
| PENGU_USDT | 0 | 150 | 2 | 736 | 120 | 6 | 110 | 11 | 🔵 Pro |
| POPCAT_USDT | 0 | 0 | 0 | 4 | 100 | 5 | 110 | 10 | 🟣 Direct |
| SOL_USD | 6 | 170 | 22 | 1500 | 120 | 6 | 110 | 11 | 🔵 Pro |
| SOL_USDC | 2 | 120 | 6 | 827 | 120 | 6 | 0 | 0 | 🔵 Pro |
| SOL_USDT | 2 | 167 | 21 | 497 | 120 | 6 | 120 | 12 | 🔵 Pro |
| SUI_USD | 0 | 124 | 1 | 636 | 120 | 6 | 110 | 11 | 🔵 Pro |
| SUI_USDT | 0 | 145 | 4 | 980 | 120 | 6 | 100 | 11 | 🔵 Pro |
| WIF_USDT | 0 | 60 | 0 | 186 | 120 | 5 | 110 | 11 | 🔵 Pro |

**Native Errors:** unknown

**Fix Recommendations:** [high] Reduce subscription count per connection

### MEXC (CCXT: mexc) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDC | 320 | 16 | 0 | 0 | 0 | 0 | 120 | 13 | ✅ Native |
| BTC_USDT | 320 | 16 | 0 | 0 | 0 | 0 | 100 | 13 | ✅ Native |
| ENA_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 130 | 13 | 🟣 Direct |
| ENA_USDT | 300 | 15 | 0 | 0 | 0 | 0 | 130 | 13 | ✅ Native |
| ETH_USDC | 320 | 15 | 0 | 0 | 0 | 0 | 120 | 13 | ✅ Native |
| ETH_USDT | 320 | 16 | 0 | 0 | 0 | 0 | 130 | 13 | ✅ Native |
| PENGU_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 120 | 13 | 🟣 Direct |
| PENGU_USDT | 320 | 16 | 0 | 0 | 0 | 0 | 120 | 13 | ✅ Native |
| POPCAT_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 130 | 13 | 🟣 Direct |
| POPCAT_USDT | 300 | 14 | 0 | 0 | 0 | 0 | 130 | 13 | ✅ Native |
| SOL_USDC | 300 | 16 | 0 | 0 | 0 | 0 | 130 | 13 | ✅ Native |
| SOL_USDT | 300 | 16 | 0 | 0 | 0 | 0 | 130 | 13 | ✅ Native |
| SUI_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 130 | 13 | 🟣 Direct |
| SUI_USDT | 300 | 15 | 0 | 0 | 0 | 0 | 130 | 13 | ✅ Native |
| WIF_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 130 | 13 | 🟣 Direct |
| WIF_USDT | 300 | 15 | 0 | 0 | 0 | 0 | 130 | 13 | ✅ Native |

**CCXT Pro Errors:** loadMarkets(1):mexc GET https://contract.mexc.com/api/v1/contract; loadMarkets(2):mexc GET https://contract.mexc.com/api/v1/contract; loadMarkets(3):mexc GET https://contract.mexc.com/api/v1/contract

**CCXT REST Errors:** loadMarkets(1):mexc GET https://contract.mexc.com/api/v1/contract; loadMarkets(2):mexc GET https://contract.mexc.com/api/v1/contract; loadMarkets(3):mexc GET https://contract.mexc.com/api/v1/contract

### CoinEx (CCXT: coinex) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 130 | 139 | 14308 | 309 | 200 | 10 | 180 | 0 | 🔵 Pro |
| BTC_USDC | 106 | 590 | 4307 | 966 | 200 | 10 | 170 | 0 | 🔵 Pro |
| BTC_USDT | 325 | 1026 | 134490 | 1668 | 200 | 10 | 160 | 0 | 🔵 Pro |
| ENA_USDC | 0 | 0 | 4307 | 19 | 180 | 9 | 170 | 0 | 🔵 Pro |
| ENA_USDT | 130 | 185 | 14308 | 253 | 180 | 9 | 170 | 0 | 🔵 Pro |
| ETH_USDC | 112 | 681 | 4886 | 1081 | 200 | 10 | 170 | 0 | 🔵 Pro |
| ETH_USDT | 213 | 1083 | 77434 | 1671 | 180 | 10 | 180 | 0 | 🔵 Pro |
| PENGU_USDT | 130 | 66 | 14752 | 123 | 200 | 10 | 180 | 0 | 🔵 Pro |
| POPCAT_USDT | 130 | 396 | 14307 | 769 | 200 | 10 | 180 | 0 | 🔵 Pro |
| SOL_USDC | 105 | 458 | 3791 | 766 | 200 | 10 | 180 | 0 | 🔵 Pro |
| SOL_USDT | 161 | 812 | 23572 | 1317 | 180 | 9 | 180 | 0 | 🔵 Pro |
| SUI_USDC | 0 | 0 | 4307 | 741 | 160 | 9 | 180 | 0 | 🔵 Pro |
| SUI_USDT | 130 | 558 | 14421 | 1066 | 180 | 9 | 180 | 0 | 🔵 Pro |
| WIF_USDC | 0 | 0 | 4307 | 22 | 200 | 10 | 180 | 0 | 🔵 Pro |
| WIF_USDT | 130 | 343 | 16177 | 495 | 200 | 10 | 180 | 0 | 🔵 Pro |

**Fix Recommendations:** [high] Reduce subscription count per connection

### LBank (CCXT: lbank) — Health: 97/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 128 | 484 | 0 | 0 | 0 | 0 | 70 | 6 | ✅ Native |
| BTC_USDC | 438 | 848 | 0 | 0 | 0 | 0 | 60 | 7 | ✅ Native |
| BTC_USDT | 1624 | 1086 | 0 | 0 | 0 | 0 | 80 | 7 | ✅ Native |
| ENA_USDT | 244 | 510 | 0 | 0 | 0 | 0 | 60 | 6 | ✅ Native |
| ETH_USDC | 476 | 868 | 0 | 0 | 0 | 0 | 60 | 6 | ✅ Native |
| ETH_USDT | 1086 | 712 | 0 | 0 | 0 | 0 | 70 | 7 | ✅ Native |
| PENGU_USDT | 436 | 610 | 0 | 0 | 0 | 0 | 70 | 7 | ✅ Native |
| POPCAT_USDT | 474 | 778 | 0 | 0 | 0 | 0 | 50 | 7 | ✅ Native |
| SOL_USDC | 214 | 580 | 0 | 0 | 0 | 0 | 60 | 7 | ✅ Native |
| SOL_USDT | 935 | 1078 | 0 | 0 | 0 | 0 | 70 | 6 | ✅ Native |
| SUI_USDT | 649 | 568 | 0 | 0 | 0 | 0 | 70 | 7 | ✅ Native |
| WIF_USDT | 123 | 476 | 0 | 0 | 0 | 0 | 70 | 7 | ✅ Native |

**Native Errors:** certificate has expired

**CCXT Pro Errors:** loadMarkets(1):lbank GET https://lbkperp.lbank.com/cfd/openApi/v1; loadMarkets(2):lbank GET https://lbkperp.lbank.com/cfd/openApi/v1; loadMarkets(3):lbank GET https://lbkperp.lbank.com/cfd/openApi/v1

**CCXT REST Errors:** loadMarkets(1):lbank GET https://lbkperp.lbank.com/cfd/openApi/v1; loadMarkets(2):lbank GET https://lbkperp.lbank.com/cfd/openApi/v1; loadMarkets(3):lbank GET https://lbkperp.lbank.com/cfd/openApi/v1

### BitMart (CCXT: bitmart) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 192 | 674 | 125 | 338 | 100 | 5 | 70 | 0 | ✅ Native |
| BTC_USDC | 3117 | 922 | 1962 | 705 | 120 | 6 | 60 | 0 | ✅ Native |
| BTC_USDT | 5293 | 1064 | 3799 | 1130 | 120 | 6 | 50 | 0 | ✅ Native |
| ENA_USDT | 393 | 770 | 262 | 407 | 120 | 6 | 70 | 0 | ✅ Native |
| ETH_USDC | 275 | 866 | 181 | 584 | 120 | 6 | 50 | 0 | ✅ Native |
| ETH_USDT | 3101 | 1068 | 1535 | 1127 | 100 | 5 | 80 | 0 | ✅ Native |
| PENGU_USDT | 354 | 799 | 237 | 458 | 120 | 6 | 70 | 0 | ✅ Native |
| POPCAT_USDT | 182 | 993 | 126 | 1029 | 120 | 6 | 60 | 0 | ≈ Tie |
| SOL_USDC | 270 | 822 | 145 | 555 | 120 | 5 | 70 | 0 | ✅ Native |
| SOL_USDT | 2917 | 1029 | 1854 | 846 | 120 | 5 | 80 | 0 | ✅ Native |
| SUI_USDT | 1147 | 863 | 737 | 493 | 100 | 6 | 70 | 0 | ✅ Native |
| WIF_USDT | 419 | 716 | 271 | 368 | 120 | 6 | 70 | 0 | ✅ Native |

**Fix Recommendations:** [high] Reduce subscription count per connection

### Pionex (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 8217 | 570 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ENA_USDT | 89 | 359 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDT | 5127 | 562 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| PENGU_USDT | 78 | 352 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SOL_USDT | 1145 | 420 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SUI_USDT | 425 | 428 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| WIF_USDT | 31 | 309 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |

**Native Errors:** Opening handshake has timed out

### Poloniex (CCXT: poloniex) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 0 | 4 | 0 | 0 | 180 | 9 | 100 | 10 | 🟠 REST |
| BTC_USDC | 0 | 2905 | 0 | 0 | 160 | 9 | 110 | 11 | ✅ Native |
| BTC_USDT | 1537 | 2992 | 0 | 0 | 200 | 10 | 90 | 11 | ✅ Native |
| ENA_USDT | 0 | 209 | 0 | 0 | 180 | 9 | 90 | 9 | ≈ Tie |
| ETH_USDC | 0 | 2129 | 0 | 0 | 140 | 8 | 100 | 10 | ✅ Native |
| ETH_USDT | 573 | 2555 | 0 | 0 | 180 | 9 | 110 | 10 | ✅ Native |
| POPCAT_USDT | 0 | 7 | 0 | 0 | 160 | 9 | 100 | 9 | 🟠 REST |
| SOL_USDT | 1195 | 2830 | 0 | 0 | 180 | 9 | 110 | 11 | ✅ Native |
| SUI_USDT | 840 | 2878 | 0 | 0 | 180 | 9 | 100 | 10 | ✅ Native |
| WIF_USDT | 0 | 249 | 0 | 0 | 160 | 9 | 80 | 10 | ✅ Native |

### HitBTC (CCXT: hitbtc) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 0 | 1689 | 0 | 2948 | 126 | 18 | 77 | 13 | 🔵 Pro |
| BTC_USDC | 1 | 2108 | 10 | 3880 | 360 | 18 | 130 | 12 | 🔵 Pro |
| BTC_USDT | 7 | 2078 | 27 | 3890 | 360 | 17 | 100 | 12 | 🔵 Pro |
| ENA_USDT | 0 | 949 | 0 | 1768 | 320 | 16 | 130 | 13 | 🔵 Pro |
| ETH_USDC | 5 | 2217 | 56 | 4231 | 360 | 18 | 130 | 13 | 🔵 Pro |
| ETH_USDT | 8 | 2243 | 32 | 4133 | 360 | 18 | 130 | 13 | 🔵 Pro |
| PENGU_USDT | 0 | 1255 | 0 | 2324 | 360 | 18 | 130 | 13 | 🔵 Pro |
| POPCAT_USDT | 0 | 914 | 0 | 1705 | 340 | 17 | 130 | 13 | 🔵 Pro |
| SOL_USDC | 0 | 1860 | 0 | 3499 | 360 | 18 | 130 | 13 | 🔵 Pro |
| SOL_USDT | 19 | 1844 | 48 | 3501 | 360 | 17 | 130 | 13 | 🔵 Pro |
| SUI_USDT | 0 | 1587 | 0 | 2954 | 340 | 17 | 130 | 13 | 🔵 Pro |
| WIF_USDT | 0 | 642 | 0 | 1356 | 340 | 17 | 130 | 13 | 🔵 Pro |

**Native Errors:** Opening handshake has timed out

**Fix Recommendations:** [high] Reduce subscription count per connection

### BTSE (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 176 | 30 | 0 | 0 | 0 | 0 | 6 | 6 | ✅ Native |
| BTC_USD | 2850 | 267 | 0 | 0 | 0 | 0 | 3 | 5 | ✅ Native |
| BTC_USDC | 303 | 276 | 0 | 0 | 0 | 0 | 5 | 5 | ✅ Native |
| BTC_USDT | 1037 | 268 | 0 | 0 | 0 | 0 | 6 | 6 | ✅ Native |
| ENA_USD | 126 | 45 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ENA_USDC | 100 | 47 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ENA_USDT | 170 | 42 | 0 | 0 | 0 | 0 | 5 | 5 | ✅ Native |
| ETH_USD | 3886 | 361 | 0 | 0 | 0 | 0 | 6 | 6 | ✅ Native |
| ETH_USDC | 598 | 365 | 0 | 0 | 0 | 0 | 5 | 5 | ✅ Native |
| ETH_USDT | 2499 | 366 | 0 | 0 | 0 | 0 | 6 | 5 | ✅ Native |
| PENGU_USD | 297 | 93 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| PENGU_USDC | 108 | 94 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| PENGU_USDT | 389 | 93 | 0 | 0 | 0 | 0 | 6 | 6 | ✅ Native |
| POPCAT_USD | 103 | 30 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| POPCAT_USDC | 106 | 33 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| POPCAT_USDT | 103 | 28 | 0 | 0 | 0 | 0 | 6 | 6 | ✅ Native |
| SOL_USD | 237 | 187 | 0 | 0 | 0 | 0 | 6 | 5 | ✅ Native |
| SOL_USDC | 136 | 184 | 0 | 0 | 0 | 0 | 6 | 6 | ✅ Native |
| SOL_USDT | 105 | 185 | 0 | 0 | 0 | 0 | 6 | 5 | ✅ Native |
| SUI_USD | 1758 | 238 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SUI_USDC | 345 | 239 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SUI_USDT | 1547 | 238 | 0 | 0 | 0 | 0 | 5 | 5 | ✅ Native |
| WIF_USD | 151 | 44 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| WIF_USDC | 105 | 44 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| WIF_USDT | 232 | 42 | 0 | 0 | 0 | 0 | 6 | 5 | ✅ Native |

### Biconomy (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 100 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| BTC_USDC | 100 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| BTC_USDT | 100 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ENA_USDT | 157 | 173 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDC | 100 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDT | 100 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| PENGU_USDT | 100 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| POPCAT_USDT | 100 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SOL_USDC | 100 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SOL_USDT | 100 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SUI_USDT | 100 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| WIF_USDT | 100 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |

### Hotcoin (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 20 | 99 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| BTC_USDC | 71 | 164 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| BTC_USDT | 937 | 336 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ENA_USDT | 240 | 225 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDC | 80 | 241 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDT | 911 | 333 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| PENGU_USDT | 94 | 160 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| POPCAT_USDT | 16 | 201 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SOL_USDT | 204 | 237 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SUI_USDT | 353 | 260 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| WIF_USDT | 129 | 127 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |

### NovaEx (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDC | 260 | 288 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| BTC_USDT | 29 | 310 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDC | 335 | 290 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDT | 486 | 349 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SOL_USDT | 123 | 339 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |

### FameEX (no CCXT) — Health: 82/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 8 | 6 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDT | 8 | 7 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SOL_USDT | 7 | 8 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| WIF_USDT | 0 | 7 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |

**Native Errors:** Opening handshake has timed out; socket hang up

**Fix Recommendations:** [high] Reduce subscription count per connection

### Websea (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 501 | 0 | 0 | 0 | 0 | 0 | 0 | 16 | ✅ Native |
| ENA_USDT | 99 | 0 | 0 | 0 | 0 | 0 | 0 | 21 | ✅ Native |
| ETH_USDT | 445 | 0 | 0 | 0 | 0 | 0 | 0 | 19 | ✅ Native |
| PENGU_USDT | 139 | 0 | 0 | 0 | 0 | 0 | 0 | 21 | ✅ Native |
| SOL_USDT | 183 | 0 | 0 | 0 | 0 | 0 | 0 | 21 | ✅ Native |
| SUI_USDT | 139 | 0 | 0 | 0 | 0 | 0 | 0 | 21 | ✅ Native |
| WIF_USDT | 75 | 0 | 0 | 0 | 0 | 0 | 0 | 21 | ✅ Native |

### Bullish (CCXT: bullish) — Health: 70/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USD | 755 | 5 | 963 | 150 | 100 | 4 | 0 | 7 | 🔵 Pro |
| BTC_USDC | 3563 | 2 | 3758 | 149 | 90 | 5 | 0 | 5 | 🔵 Pro |
| BTC_USDT | 530 | 5 | 687 | 153 | 100 | 4 | 0 | 7 | 🔵 Pro |
| ETH_USD | 365 | 5 | 573 | 118 | 80 | 4 | 0 | 8 | 🔵 Pro |
| ETH_USDC | 1672 | 6 | 2081 | 140 | 100 | 5 | 0 | 5 | 🔵 Pro |
| ETH_USDT | 397 | 4 | 663 | 139 | 80 | 5 | 0 | 7 | 🔵 Pro |
| PENGU_USDC | 473 | 5 | 753 | 77 | 20 | 4 | 0 | 7 | 🔵 Pro |
| PENGU_USDT | 472 | 5 | 772 | 92 | 100 | 4 | 0 | 7 | 🔵 Pro |
| SOL_USD | 485 | 5 | 710 | 91 | 100 | 4 | 0 | 8 | 🔵 Pro |
| SOL_USDC | 1660 | 5 | 1849 | 94 | 100 | 5 | 0 | 7 | 🔵 Pro |
| SOL_USDT | 488 | 5 | 653 | 100 | 80 | 5 | 0 | 8 | 🔵 Pro |
| SUI_USDC | 104 | 5 | 300 | 87 | 100 | 4 | 0 | 7 | 🔵 Pro |
| WIF_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ❌ |

**CCXT Pro Errors:** ETH/USDT:tr:connection closed by remote server, clos; ETH/USD:tr:connection closed by remote server, clos; SUI/USDC:rateLimit

**Fix Recommendations:** [high] Add multi-endpoint failover URLs; [high] Reduce subscription count per connection; [high] Increase polling interval / add request batching

### Darkex (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 77 | 139 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDT | 77 | 148 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| POPCAT_USDT | 23 | 105 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SUI_USDT | 5 | 120 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| WIF_USDT | 19 | 98 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |

**Native Errors:** Opening handshake has timed out

### Bitrue (CCXT: bitrue) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 90 | 2 | 0 | 6 | 160 | 8 | 50 | 5 | 🟠 REST |
| BTC_USDC | 85 | 2 | 0 | 26 | 160 | 8 | 60 | 5 | 🟠 REST |
| BTC_USDT | 85 | 2 | 0 | 39 | 160 | 8 | 40 | 5 | 🟠 REST |
| ENA_USDC | 0 | 0 | 0 | 6 | 140 | 7 | 40 | 5 | 🟠 REST |
| ENA_USDT | 85 | 2 | 0 | 10 | 140 | 7 | 30 | 5 | 🟠 REST |
| ETH_USDC | 90 | 2 | 0 | 86 | 160 | 8 | 60 | 5 | 🟠 REST |
| ETH_USDT | 80 | 2 | 0 | 30 | 140 | 8 | 60 | 6 | 🟠 REST |
| PENGU_USDC | 0 | 0 | 0 | 12 | 140 | 8 | 50 | 5 | 🟠 REST |
| PENGU_USDT | 90 | 2 | 0 | 11 | 160 | 8 | 50 | 5 | 🟠 REST |
| POPCAT_USDT | 90 | 2 | 0 | 6 | 160 | 8 | 50 | 5 | 🟠 REST |
| SOL_USDC | 85 | 2 | 0 | 25 | 160 | 8 | 40 | 4 | 🟠 REST |
| SOL_USDT | 90 | 2 | 0 | 17 | 120 | 8 | 60 | 6 | 🟠 REST |
| SUI_USDC | 0 | 0 | 0 | 6 | 140 | 7 | 50 | 5 | 🟠 REST |
| SUI_USDT | 85 | 2 | 0 | 17 | 140 | 7 | 50 | 5 | 🟠 REST |
| WIF_USDC | 0 | 0 | 0 | 6 | 140 | 8 | 50 | 5 | 🟠 REST |
| WIF_USDT | 90 | 2 | 0 | 6 | 160 | 8 | 50 | 5 | 🟠 REST |

**Native Errors:** Unexpected server response: 502

**CCXT Pro Errors:** PENGU/USDC:tk:notSupported; POPCAT/USDT:tr:notSupported; POPCAT/USDT:tk:notSupported

**Fix Recommendations:** [high] Reduce subscription count per connection; [low] Use REST fallback for unsupported methods

### BloFin (CCXT: blofin) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USD | 49 | 176 | 304 | 415 | 200 | 9 | 0 | 0 | 🔵 Pro |
| BTC_USDC | 41 | 132 | 259 | 321 | 200 | 10 | 120 | 12 | 🔵 Pro |
| BTC_USDT | 95 | 142 | 578 | 337 | 220 | 11 | 70 | 11 | 🔵 Pro |
| ETH_USD | 58 | 174 | 350 | 414 | 180 | 10 | 0 | 0 | 🔵 Pro |
| ETH_USDT | 66 | 141 | 406 | 337 | 220 | 11 | 110 | 13 | 🔵 Pro |
| SOL_USD | 23 | 173 | 150 | 404 | 200 | 10 | 0 | 0 | 🔵 Pro |
| SOL_USDC | 32 | 130 | 173 | 316 | 200 | 10 | 120 | 12 | 🔵 Pro |
| SOL_USDT | 49 | 141 | 281 | 335 | 180 | 10 | 120 | 11 | 🔵 Pro |

**Fix Recommendations:** [high] Reduce subscription count per connection

### DigiFinex (CCXT: digifinex) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 1118 | 545 | 0 | 0 | 240 | 12 | 140 | 14 | ✅ Native |
| BTC_USDC | 1024 | 673 | 0 | 0 | 260 | 12 | 140 | 14 | ✅ Native |
| BTC_USDT | 4416 | 1950 | 0 | 0 | 200 | 13 | 130 | 14 | ✅ Native |
| ETH_USDC | 1328 | 1029 | 0 | 0 | 240 | 12 | 130 | 14 | ✅ Native |
| ETH_USDT | 3328 | 1546 | 0 | 0 | 240 | 13 | 140 | 12 | ✅ Native |
| SOL_USDT | 1747 | 1193 | 0 | 0 | 0 | 0 | 130 | 13 | ✅ Native |

**CCXT REST Errors:** SOL/USDT:not found

**Fix Recommendations:** [medium] Verify pair availability on exchange

### EXMO (CCXT: exmo) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_DAI | 3 | 161 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| BTC_USD | 3 | 117 | 187 | 403 | 100 | 5 | 140 | 7 | 🔵 Pro |
| BTC_USDC | 2 | 129 | 48 | 246 | 100 | 5 | 140 | 7 | 🔵 Pro |
| BTC_USDT | 7 | 180 | 596 | 346 | 120 | 5 | 80 | 7 | 🔵 Pro |
| ENA_USDC | 4 | 5 | 156 | 12 | 80 | 5 | 120 | 6 | 🔵 Pro |
| ENA_USDT | 2 | 10 | 85 | 19 | 100 | 5 | 120 | 6 | 🟣 Direct |
| ETH_USD | 2 | 121 | 191 | 388 | 100 | 5 | 140 | 7 | 🔵 Pro |
| ETH_USDC | 3 | 142 | 146 | 273 | 100 | 5 | 140 | 7 | 🔵 Pro |
| ETH_USDT | 6 | 247 | 251 | 492 | 100 | 5 | 120 | 7 | 🔵 Pro |
| PENGU_USDC | 3 | 115 | 169 | 207 | 100 | 5 | 140 | 7 | 🔵 Pro |
| PENGU_USDT | 2 | 102 | 101 | 199 | 100 | 5 | 140 | 7 | 🔵 Pro |
| SOL_USDC | 3 | 124 | 163 | 229 | 100 | 5 | 120 | 7 | 🔵 Pro |
| SOL_USDT | 3 | 97 | 86 | 235 | 100 | 5 | 140 | 7 | 🔵 Pro |
| SUI_USDC | 3 | 135 | 136 | 228 | 80 | 5 | 140 | 7 | 🔵 Pro |
| SUI_USDT | 3 | 104 | 70 | 223 | 100 | 5 | 140 | 7 | 🔵 Pro |
| WIF_USDC | 5 | 62 | 170 | 128 | 100 | 5 | 140 | 7 | 🔵 Pro |
| WIF_USDT | 3 | 39 | 84 | 104 | 100 | 5 | 140 | 7 | 🔵 Pro |

**Native Errors:** Opening handshake has timed out

**Fix Recommendations:** [high] Reduce subscription count per connection

### CEX.IO (CCXT: cex) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USD | 0 | 0 | 0 | 0 | 60 | 4 | 0 | 0 | 🟠 REST |
| BRETT_USDC | 0 | 0 | 0 | 0 | 60 | 4 | 0 | 0 | 🟠 REST |
| BRETT_USDT | 0 | 0 | 0 | 0 | 40 | 4 | 0 | 0 | 🟠 REST |
| BTC_USD | 60 | 3 | 0 | 0 | 20 | 4 | 80 | 4 | 🟣 Direct |
| BTC_USDC | 0 | 0 | 0 | 0 | 20 | 4 | 0 | 0 | 🟠 REST |
| BTC_USDT | 20 | 4 | 0 | 0 | 40 | 4 | 20 | 2 | 🟠 REST |
| ENA_USD | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ❌ |
| ENA_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ❌ |
| ENA_USDT | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ❌ |
| ETH_USD | 60 | 3 | 0 | 0 | 20 | 4 | 80 | 4 | 🟣 Direct |
| ETH_USDC | 0 | 0 | 0 | 0 | 20 | 4 | 0 | 0 | 🟠 REST |
| ETH_USDT | 80 | 4 | 0 | 0 | 20 | 4 | 40 | 4 | ✅ Native |
| PENGU_USD | 0 | 0 | 0 | 0 | 20 | 4 | 0 | 0 | 🟠 REST |
| PENGU_USDT | 0 | 0 | 0 | 0 | 60 | 4 | 0 | 0 | 🟠 REST |
| POPCAT_USD | 0 | 0 | 0 | 0 | 20 | 4 | 0 | 0 | 🟠 REST |
| POPCAT_USDT | 0 | 0 | 0 | 0 | 20 | 4 | 0 | 0 | 🟠 REST |
| SOL_USD | 60 | 3 | 0 | 0 | 40 | 4 | 80 | 4 | 🟣 Direct |
| SOL_USDC | 3 | 3 | 0 | 0 | 20 | 4 | 4 | 4 | 🟠 REST |
| SOL_USDT | 0 | 0 | 0 | 0 | 20 | 4 | 0 | 0 | 🟠 REST |
| SUI_USD | 60 | 3 | 0 | 0 | 40 | 3 | 60 | 3 | ≈ Tie |
| SUI_USDC | 0 | 0 | 0 | 0 | 40 | 2 | 0 | 0 | 🟠 REST |
| SUI_USDT | 60 | 3 | 0 | 0 | 60 | 3 | 40 | 3 | ≈ Tie |
| WIF_USD | 40 | 0 | 0 | 0 | 60 | 3 | 80 | 0 | 🟣 Direct |
| WIF_USDC | 0 | 0 | 0 | 0 | 60 | 4 | 0 | 0 | 🟠 REST |
| WIF_USDT | 60 | 0 | 0 | 0 | 40 | 4 | 80 | 0 | 🟣 Direct |

### OrangeX (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 1320 | 27 | 0 | 0 | 0 | 0 | 420 | 25 | ✅ Native |
| ETH_USDT | 1620 | 27 | 0 | 0 | 0 | 0 | 440 | 23 | ✅ Native |
| SOL_USDT | 1620 | 27 | 0 | 0 | 0 | 0 | 480 | 24 | ✅ Native |

### Azbit (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDC | 30 | 6 | 0 | 0 | 0 | 0 | 120 | 6 | 🟣 Direct |
| BTC_USDT | 20 | 3 | 0 | 0 | 0 | 0 | 60 | 5 | 🟣 Direct |
| ETH_USDC | 30 | 5 | 0 | 0 | 0 | 0 | 120 | 6 | 🟣 Direct |
| ETH_USDT | 30 | 4 | 0 | 0 | 0 | 0 | 120 | 6 | 🟣 Direct |
| PENGU_USDT | 25 | 5 | 0 | 0 | 0 | 0 | 120 | 6 | 🟣 Direct |
| SOL_USDC | 25 | 5 | 0 | 0 | 0 | 0 | 100 | 6 | 🟣 Direct |
| SOL_USDT | 20 | 5 | 0 | 0 | 0 | 0 | 100 | 4 | 🟣 Direct |
| WIF_USDT | 20 | 5 | 0 | 0 | 0 | 0 | 120 | 6 | 🟣 Direct |

### BVOX (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 105 | 23 | 0 | 0 | 0 | 0 | 85 | 20 | ✅ Native |
| ETH_USDT | 110 | 21 | 0 | 0 | 0 | 0 | 115 | 23 | 🟣 Direct |
| SOL_USDT | 120 | 23 | 0 | 0 | 0 | 0 | 115 | 23 | ≈ Tie |

### Trubit Pro (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDC | 85 | 18 | 0 | 0 | 0 | 0 | 95 | 19 | 🟣 Direct |
| BTC_USDT | 75 | 18 | 0 | 0 | 0 | 0 | 80 | 21 | 🟣 Direct |
| ETH_USDC | 85 | 18 | 0 | 0 | 0 | 0 | 100 | 20 | 🟣 Direct |
| ETH_USDT | 90 | 17 | 0 | 0 | 0 | 0 | 100 | 20 | 🟣 Direct |
| SOL_USDT | 90 | 17 | 0 | 0 | 0 | 0 | 100 | 19 | 🟣 Direct |

### BigONE (CCXT: bigone) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 3200 | 17 | 0 | 0 | 460 | 23 | 220 | 10 | ✅ Native |
| ENA_USDT | 3400 | 17 | 0 | 0 | 460 | 23 | 220 | 11 | ✅ Native |
| ETH_USDT | 3400 | 17 | 0 | 0 | 460 | 23 | 220 | 11 | ✅ Native |
| PENGU_USDT | 3600 | 18 | 0 | 0 | 440 | 23 | 220 | 12 | ✅ Native |
| SOL_USDT | 3600 | 18 | 0 | 0 | 460 | 23 | 220 | 9 | ✅ Native |
| SUI_USDT | 3600 | 17 | 0 | 0 | 460 | 23 | 200 | 11 | ✅ Native |
| WIF_USDT | 90 | 18 | 0 | 0 | 110 | 21 | 55 | 12 | 🟠 REST |

### LATOKEN (CCXT: latoken) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 200 | 11 | 0 | 0 | 240 | 14 | 0 | 0 | 🟠 REST |
| BTC_USDT | 0 | 0 | 0 | 0 | 240 | 14 | 0 | 0 | 🟠 REST |
| ENA_USDT | 160 | 10 | 0 | 0 | 260 | 14 | 0 | 0 | 🟠 REST |
| ETH_USDT | 180 | 10 | 0 | 0 | 240 | 14 | 0 | 0 | 🟠 REST |
| PENGU_USDT | 180 | 9 | 0 | 0 | 260 | 14 | 0 | 0 | 🟠 REST |
| POPCAT_USDT | 135 | 9 | 0 | 0 | 203 | 14 | 0 | 0 | 🟠 REST |
| SOL_USDT | 0 | 0 | 0 | 0 | 187 | 14 | 0 | 0 | 🟠 REST |

**CCXT REST Errors:** WIF/USDT:not found; SUI/USDT:not found

**Fix Recommendations:** [medium] Verify pair availability on exchange

### Coinstore (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 1266 | 269 | 0 | 0 | 0 | 0 | 55 | 0 | ✅ Native |
| ENA_USDT | 197 | 67 | 0 | 0 | 0 | 0 | 60 | 0 | ✅ Native |
| ETH_USDT | 1572 | 270 | 0 | 0 | 0 | 0 | 65 | 0 | ✅ Native |
| PENGU_USDT | 306 | 82 | 0 | 0 | 0 | 0 | 60 | 0 | ✅ Native |
| SOL_USDT | 1439 | 266 | 0 | 0 | 0 | 0 | 55 | 0 | ✅ Native |
| SUI_USDT | 847 | 81 | 0 | 0 | 0 | 0 | 60 | 0 | ✅ Native |
| WIF_USDT | 446 | 134 | 0 | 0 | 0 | 0 | 60 | 0 | ✅ Native |

**Native Errors:** Opening handshake has timed out

### GroveX (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 50 | 108 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| BTC_USDC | 66 | 121 | 0 | 0 | 0 | 0 | 200 | 10 | 🟣 Direct |
| BTC_USDT | 61 | 154 | 0 | 0 | 0 | 0 | 160 | 9 | ✅ Native |
| ENA_USDT | 78 | 134 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDC | 64 | 132 | 0 | 0 | 0 | 0 | 160 | 9 | ≈ Tie |
| ETH_USDT | 62 | 134 | 0 | 0 | 0 | 0 | 180 | 9 | ≈ Tie |
| PENGU_USDT | 46 | 117 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| POPCAT_USDT | 45 | 127 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SOL_USDC | 43 | 125 | 0 | 0 | 0 | 0 | 180 | 9 | 🟣 Direct |
| SOL_USDT | 47 | 126 | 0 | 0 | 0 | 0 | 200 | 10 | 🟣 Direct |
| SUI_USDT | 45 | 191 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| WIF_USDT | 44 | 137 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |

### CoinW (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 50 | 5 | 0 | 0 | 0 | 0 | 100 | 5 | 🟣 Direct |
| BTC_USDC | 40 | 5 | 0 | 0 | 0 | 0 | 100 | 6 | 🟣 Direct |
| BTC_USDT | 10 | 4 | 0 | 0 | 0 | 0 | 100 | 5 | 🟣 Direct |
| ENA_USDT | 50 | 5 | 0 | 0 | 0 | 0 | 100 | 5 | 🟣 Direct |
| ETH_USDC | 50 | 4 | 0 | 0 | 0 | 0 | 120 | 5 | 🟣 Direct |
| ETH_USDT | 50 | 5 | 0 | 0 | 0 | 0 | 80 | 5 | 🟣 Direct |
| PENGU_USDT | 40 | 5 | 0 | 0 | 0 | 0 | 100 | 5 | 🟣 Direct |
| POPCAT_USDT | 50 | 5 | 0 | 0 | 0 | 0 | 100 | 5 | 🟣 Direct |
| SOL_USDC | 50 | 5 | 0 | 0 | 0 | 0 | 100 | 4 | 🟣 Direct |
| SOL_USDT | 40 | 4 | 0 | 0 | 0 | 0 | 120 | 5 | 🟣 Direct |
| SUI_USDT | 50 | 5 | 0 | 0 | 0 | 0 | 100 | 5 | 🟣 Direct |
| WIF_USDT | 50 | 5 | 0 | 0 | 0 | 0 | 80 | 5 | 🟣 Direct |

### Batonex (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 55 | 13 | 0 | 0 | 0 | 0 | 70 | 15 | 🟣 Direct |
| ETH_USDT | 65 | 13 | 0 | 0 | 0 | 0 | 75 | 15 | 🟣 Direct |
| SOL_USDT | 65 | 12 | 0 | 0 | 0 | 0 | 75 | 16 | 🟣 Direct |
| WIF_USDT | 65 | 12 | 0 | 0 | 0 | 0 | 75 | 15 | 🟣 Direct |

## 🔬 Reliability Analysis

| Exchange | Health | Reconn | REST FB | Dedup | Native | Pro | REST | Direct |
|----------|--------|--------|---------|-------|--------|-----|------|--------|
| Binance | 🟢100 | 5 | 438 | 0 | 4,942 | 0 | 2,228 | 1,129 |
| Coinbase | 🟢85 | 11 | 0 | 0 | 6,103 | 304,741 | 1,634 | 630 |
| Kraken | 🟢85 | 1 | 0 | 0 | 25,707 | 10,663 | 1,954 | 1,382 |
| KuCoin | 🟡70 | 2 | 35 | 0 | 27,176 | 8,466 | 2,332 | 1,583 |
| OKX | 🟢85 | 0 | 0 | 0 | 9,488 | 10,963 | 1,993 | 793 |
| Bybit | 🟢85 | 3 | 27 | 0 | 30,265 | 13,444 | 2,356 | 1,413 |
| Bitfinex | 🟢100 | 0 | 0 | 0 | 235,696 | 0 | 803 | 1,142 |
| Gate.io | 🟢85 | 1 | 258 | 0 | 2,815 | 429 | 2,976 | 718 |
| HTX | 🟢100 | 6 | 36 | 0 | 4,703 | 0 | 1,544 | 289 |
| WOO X | 🟢85 | 0 | 0 | 0 | 1,806 | 7,427 | 5,015 | 1,168 |
| Crypto.com | 🟢85 | 6 | 0 | 0 | 20,224 | 17,382 | 3,565 | 838 |
| Bitstamp | 🟢85 | 12 | 0 | 0 | 191 | 1,050 | 453 | 168 |
| WhiteBIT | 🟢100 | 2 | 24 | 0 | 4,233 | 0 | 1,759 | 505 |
| AscendEX | 🟡70 | 1 | 0 | 0 | 10,919 | 10,422 | 4,117 | 164 |
| BingX | 🟡78 | 7 | 0 | 0 | 11,853 | 6,314 | 4,031 | 2,087 |
| Toobit | 🟢85 | 0 | 0 | 0 | 1,362 | 1,311 | 1,588 | 0 |
| Deepcoin | 🟢100 | 0 | 0 | 0 | 162 | 0 | 1,058 | 0 |
| XT.com | 🟢100 | 0 | 18 | 0 | 1,382 | 8,980 | 1,650 | 1,384 |
| Zoomex | 🟢100 | 3 | 0 | 0 | 21,050 | 0 | 0 | 0 |
| Bitget | 🟢85 | 1 | 0 | 0 | 16,862 | 19,791 | 1,607 | 1,376 |
| Gemini | 🟢96 | 8 | 0 | 0 | 0 | 0 | 3,805 | 1,662 |
| Binance.US | 🟢85 | 9 | 0 | 0 | 1,889 | 15,980 | 2,111 | 1,576 |
| MEXC | 🟢100 | 0 | 0 | 0 | 3,570 | 0 | 0 | 2,218 |
| CoinEx | 🟢85 | 0 | 0 | 0 | 8,139 | 361,069 | 3,151 | 2,640 |
| LBank | 🟢97 | 7 | 169 | 0 | 15,425 | 0 | 0 | 870 |
| BitMart | 🟢85 | 1 | 0 | 0 | 28,246 | 22,087 | 1,517 | 800 |
| Pionex | 🟢100 | 3 | 0 | 0 | 18,112 | 0 | 0 | 0 |
| Poloniex | 🟢100 | 2 | 0 | 0 | 20,903 | 0 | 1,895 | 1,091 |
| HitBTC | 🟢85 | 3 | 0 | 0 | 19,426 | 41,799 | 4,406 | 1,631 |
| BTSE | 🟢100 | 1 | 0 | 0 | 21,306 | 0 | 0 | 164 |
| Biconomy | 🟢100 | 0 | 0 | 0 | 1,441 | 0 | 0 | 0 |
| Hotcoin | 🟢100 | 4 | 0 | 0 | 5,438 | 0 | 0 | 0 |
| NovaEx | 🟢100 | 1 | 0 | 0 | 2,809 | 0 | 0 | 0 |
| FameEX | 🟢82 | 22 | 0 | 0 | 51 | 0 | 0 | 0 |
| Websea | 🟢100 | 1 | 0 | 0 | 1,581 | 0 | 0 | 140 |
| Bullish | 🟡70 | 0 | 0 | 0 | 11,021 | 19,177 | 1,161 | 83 |
| Darkex | 🟢100 | 4 | 0 | 0 | 811 | 0 | 0 | 0 |
| Bitrue | 🟢85 | 3 | 0 | 0 | 1,069 | 309 | 2,628 | 871 |
| BloFin | 🟢85 | 1 | 0 | 0 | 1,622 | 8,822 | 1,761 | 599 |
| DigiFinex | 🟢100 | 0 | 0 | 0 | 19,897 | 0 | 1,304 | 891 |
| EXMO | 🟢85 | 2 | 0 | 0 | 1,947 | 8,422 | 1,739 | 2,210 |
| CEX.IO | 🟢100 | 0 | 0 | 0 | 529 | 0 | 976 | 592 |
| OrangeX | 🟢100 | 0 | 0 | 0 | 4,641 | 0 | 0 | 1,412 |
| Azbit | 🟢100 | 0 | 0 | 0 | 238 | 0 | 0 | 905 |
| BVOX | 🟢100 | 0 | 0 | 0 | 402 | 0 | 0 | 381 |
| Trubit Pro | 🟢100 | 0 | 0 | 0 | 513 | 0 | 0 | 574 |
| BigONE | 🟢100 | 0 | 0 | 0 | 21,012 | 0 | 3,169 | 1,431 |
| LATOKEN | 🟢100 | 0 | 0 | 0 | 904 | 0 | 1,814 | 0 |
| Coinstore | 🟢100 | 2 | 76 | 0 | 7,242 | 0 | 0 | 415 |
| GroveX | 🟢100 | 0 | 0 | 0 | 2,257 | 0 | 0 | 1,136 |
| CoinW | 🟢100 | 0 | 0 | 0 | 587 | 0 | 0 | 1,260 |
| Batonex | 🟢100 | 0 | 0 | 0 | 300 | 0 | 0 | 356 |
| CEEX | 🟢84 | 18 | 0 | 0 | 0 | 0 | 0 | 0 |

## 🔧 Recommendations

| Exchange | Method | Reason | Health Fix |
|----------|--------|--------|------------|
| Binance | Native | Highest throughput (+122%) | - |
| Coinbase | CCXT Pro | Best push streaming (+4893%) | Reduce subscription count per connection |
| Kraken | Native | Highest throughput (+141%) | Reduce subscription count per connection |
| KuCoin | Native | Highest throughput (+221%) | Add multi-endpoint failover URLs |
| OKX | Hybrid | Similar throughput | Reduce subscription count per connection |
| Bybit | Native | Highest throughput (+125%) | Reduce subscription count per connection |
| Bitfinex | Native | Highest throughput (+20539%) | - |
| Gate.io | Hybrid | Similar throughput | Reduce subscription count per connection |
| HTX | Native | Highest throughput (+205%) | - |
| WOO X | CCXT Pro | Best push streaming (+48%) | Reduce subscription count per connection |
| Crypto.com | Hybrid | Similar throughput | Reduce subscription count per connection |
| Bitstamp | CCXT Pro | Best push streaming (+132%) | Reduce subscription count per connection |
| WhiteBIT | Native | Highest throughput (+141%) | - |
| AscendEX | Hybrid | Similar throughput | Add multi-endpoint failover URLs |
| BingX | Native | Highest throughput (+88%) | Add multi-endpoint failover URLs |
| Toobit | Hybrid | Similar throughput | Reduce subscription count per connection |
| Deepcoin | CCXT REST | Most reliable polling (+553%) | - |
| XT.com | CCXT Pro | Best push streaming (+444%) | - |
| Zoomex | Native | Only method available | - |
| Bitget | Hybrid | Similar throughput | Reduce subscription count per connection |
| Gemini | CCXT REST | Most reliable polling (+129%) | - |
| Binance.US | CCXT Pro | Best push streaming (+657%) | Reduce subscription count per connection |
| MEXC | Native | Highest throughput (+61%) | - |
| CoinEx | CCXT Pro | Best push streaming (+4336%) | Reduce subscription count per connection |
| LBank | Native | Highest throughput (+1673%) | - |
| BitMart | Hybrid | Similar throughput | Reduce subscription count per connection |
| Pionex | Native | Only method available | - |
| Poloniex | Native | Highest throughput (+1003%) | - |
| HitBTC | CCXT Pro | Best push streaming (+115%) | Reduce subscription count per connection |
| BTSE | Native | Only method available | - |
| Biconomy | Native | Only method available | - |
| Hotcoin | Native | Only method available | - |
| NovaEx | Native | Only method available | - |
| FameEX | Native | Only method available | Reduce subscription count per connection |
| Websea | Native | Only method available | - |
| Bullish | CCXT Pro | Best push streaming (+74%) | Add multi-endpoint failover URLs |
| Darkex | Native | Only method available | - |
| Bitrue | CCXT REST | Most reliable polling (+146%) | Reduce subscription count per connection |
| BloFin | CCXT Pro | Best push streaming (+401%) | Reduce subscription count per connection |
| DigiFinex | Native | Highest throughput (+1426%) | Verify pair availability on exchange |
| EXMO | CCXT Pro | Best push streaming (+281%) | Reduce subscription count per connection |
| CEX.IO | CCXT REST | Most reliable polling (+65%) | - |
| OrangeX | Native | Only method available | - |
| Azbit | Native | Only method available | - |
| BVOX | Native | Only method available | - |
| Trubit Pro | Native | Only method available | - |
| BigONE | Native | Highest throughput (+563%) | - |
| LATOKEN | CCXT REST | Most reliable polling (+101%) | Verify pair availability on exchange |
| Coinstore | Native | Only method available | - |
| GroveX | Native | Only method available | - |
| CoinW | Native | Only method available | - |
| Batonex | Native | Only method available | - |
| CEEX | Investigate | Underperforming | - |

## Conclusion (v9.3, 4-Method + Subscription Manager, 15min test)

4-method parallel comparison across 53 exchanges with Subscription Manager:

- **Native Wins:** 13 | **CCXT Pro Wins:** 10 | **CCXT REST Wins:** 5 | **Direct REST Wins:** 0 | **Ties:** 7
- **Native-Only:** 17 exchanges (no CCXT support)
- **All Failed:** 1 exchanges
- **Health:** Avg 92/100 | 0 critical | 4 warnings
- **Data Quality:** 0 dupes dropped, 2816 OB validated, 1081 REST fallbacks

### Aggregate Throughput

| Method | Total Messages | Rate (msg/min) |
|--------|---------------|----------------|
| Native | 660,267 | 42,921 |
| CCXT Pro | 899,048 | 58,443 |
| CCXT REST | 74,100 | 4,817 |
| Direct REST | 42,677 | 2,774 |
| **Combined** | **1,676,092** | **108,955** |
| **Hybrid (deduped)** | **796,745** | **51,793** |

**Hybrid Dedup:** 398,934 cross-method duplicates removed (24% of raw)

**Verdict:** Hybrid architecture recommended. Native handles 30/53, CCXT Pro handles 10/53, CCXT REST handles 5/53, Direct REST handles 0/53 exchanges optimally.
