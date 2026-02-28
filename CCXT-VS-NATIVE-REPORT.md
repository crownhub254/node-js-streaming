# Enhanced 4-Method Comparison Report v9.5 — Native × CCXT Pro × CCXT REST × Direct REST + Subscription Manager

**Date:** 2026-02-28
**Validation Test:** 5 minutes (322s actual) — v9.5 health check
**Production Run:** 4.59 hours (08:53–13:29 UTC) — ended with crash (Exit Code 1, root cause TBD)
**Exchanges Tested:** 53
**Pairs Tested:** BTC/ETH/SOL/BRETT/PENGU/POPCAT/WIF/SUI/ENA × USDT/USDC/USD (all available per exchange)
**Method:** v9.5 Enhanced — 4 parallel methods + Subscription Manager: Native WS/REST, CCXT Pro, CCXT REST, Direct REST
**Storage:** DuckDB ✅ enabled
**Health (5-min test):** Avg Score 95/100 | 0 critical | 0 warnings
**Production Run — Trades Stored:** 9,575,101 (CCXT methods; native-WS-only exchanges run in-memory)
**Production Run — Sustained Rate:** ~34,750 msgs/min | ~2,086,000 msgs/hour
**Dashboard:** http://localhost:3456 (9 tabs: Hybrid Flow, Per Exchange, Per Pair, Dedup Analysis, Analytics, Correlation, Health, Sub Manager, Live Events)

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
| Coinbase | A | 300 | 6 | 10 | 6 | 250ms | 45s |
| Kraken | A | 500 | 200 | 3 | 40 | 150ms | 60s |
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
| EXMO | B | 200 | 100 | 3 | 20 | 200ms | 45s |
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
| Binance | 1 | 0 | 0 | 0 | 0 | 0 |
| Coinbase | 1 | 0 | 0 | 0 | 0 | 0 |
| Kraken | 1 | 0 | 0 | 0 | 0 | 0 |
| KuCoin | 1 | 0 | 0 | 0 | 0 | 0 |
| OKX | 1 | 0 | 0 | 0 | 0 | 0 |
| Bybit | 1 | 0 | 0 | 0 | 0 | 0 |
| Bitfinex | 1 | 12 | 0 | 0 | 0 | 3 |
| Gate.io | 1 | 16 | 1 | 1 | 1 | 2 |
| HTX | 1 | 22 | 0 | 0 | 0 | 9 |
| WOO X | 1 | 22 | 0 | 0 | 0 | 2 |
| Crypto.com | 1 | 0 | 1 | 1 | 1 | 0 |
| Bitstamp | 1 | 26 | 0 | 0 | 0 | 8 |
| WhiteBIT | 1 | 18 | 0 | 0 | 0 | 2 |
| AscendEX | 1 | 28 | 0 | 0 | 0 | 2 |
| BingX | 1 | 26 | 0 | 0 | 0 | 2 |
| Toobit | 1 | 22 | 0 | 0 | 0 | 2 |
| Deepcoin | 1 | 5 | 0 | 0 | 0 | 1 |
| XT.com | 1 | 0 | 0 | 0 | 0 | 0 |
| Zoomex | 1 | 0 | 0 | 0 | 0 | 0 |
| Bitget | 1 | 0 | 0 | 0 | 0 | 0 |
| Gemini | 1 | 0 | 3 | 3 | 3 | 0 |
| Binance.US | 1 | 0 | 1 | 1 | 2 | 0 |
| CoinEx | 1 | 0 | 0 | 0 | 0 | 0 |
| LBank | 1 | 24 | 1 | 1 | 2 | 12 |
| BitMart | 1 | 0 | 0 | 0 | 0 | 0 |
| Pionex | 1 | 0 | 0 | 0 | 0 | 0 |
| Poloniex | 1 | 0 | 0 | 0 | 0 | 0 |
| HitBTC | 1 | 0 | 0 | 0 | 0 | 0 |
| BTSE | 2 | 0 | 0 | 0 | 0 | 0 |
| Biconomy | 1 | 0 | 0 | 0 | 0 | 0 |
| Hotcoin | 1 | 0 | 1 | 1 | 1 | 0 |
| NovaEx | 1 | 0 | 0 | 0 | 0 | 0 |
| FameEX | 1 | 0 | 0 | 0 | 1 | 0 |
| Websea | 1 | 0 | 0 | 0 | 0 | 0 |
| Bullish | 1 | 0 | 0 | 0 | 2 | 0 |
| Darkex | 1 | 0 | 0 | 0 | 0 | 0 |
| Bitrue | 1 | 0 | 1 | 1 | 2 | 0 |
| BloFin | 1 | 0 | 0 | 0 | 0 | 0 |
| DigiFinex | 7 | 0 | 0 | 0 | 0 | 0 |
| EXMO | 1 | 0 | 1 | 1 | 1 | 0 |
| Coinstore | 1 | 0 | 0 | 0 | 1 | 0 |
| GroveX | 1 | 0 | 0 | 0 | 0 | 0 |
| CEEX | 1 | 0 | 1 | 1 | 1 | 0 |

**Summary:** 11 total stale reconnects, 11 forced reconnects, 18 failover rotations, 45 subscription batches sent

## Data Enrichment Summary

| Metric | Count |
|--------|-------|
| Total Normalized | 158,533 |
| Duplicates Dropped | 300 |
| OB Validated (seq) | 1,446 |
| Stale OB Dropped | 0 |
| REST Fallbacks | 375 |
| Hybrid Combined Trades | 82,218 |
| Hybrid Combined OB | 139,234 |
| Hybrid Deduped (cross-method) | 142,033 |

## 4-Way Summary Table

| # | Exchange | Tier | Native | CCXT Pro | CCXT REST | Direct REST | Health | Winner | Margin |
|---|----------|------|--------|----------|-----------|-------------|--------|--------|--------|
| 1 | Binance | T1 | **1,864** | **0** | **530** | **333** | 🟢100 | ✅ Native | +252% |
| 2 | Coinbase | T1 | **3,289** | **132,304** | **1,026** | **369** | 🟢85 | 🔵 CCXT Pro | +3923% |
| 3 | Kraken | T1 | **7,911** | **14,124** | **967** | **623** | 🟢85 | 🔵 CCXT Pro | +79% |
| 4 | KuCoin | T1 | **21,120** | **4,040** | **1,806** | **969** | 🟢85 | ✅ Native | +423% |
| 5 | OKX | T1 | **4,315** | **6,106** | **973** | **555** | 🟢85 | 🔵 CCXT Pro | +42% |
| 6 | Bybit | T1 | **10,645** | **11,320** | **726** | **543** | 🟢85 | ≈ Tie | 6% |
| 7 | Bitfinex | T1 | **19,851** | **0** | **195** | **412** | 🟢100 | ✅ Native | +4718% |
| 8 | Gate.io | T1 | **595** | **121** | **882** | **327** | 🟢85 | 🟠 CCXT REST | +48% |
| 9 | HTX | T1 | **1,806** | **0** | **924** | **124** | 🟢100 | ✅ Native | +95% |
| 10 | WOO X | T1 | **983** | **2,060** | **2,037** | **461** | 🟢85 | ≈ Tie | 1% |
| 11 | Crypto.com | T2 | **5,823** | **7,668** | **1,232** | **394** | 🟢86 | 🔵 CCXT Pro | +32% |
| 12 | Bitstamp | T2 | **125** | **39** | **241** | **29** | 🟢85 | 🟠 CCXT REST | +93% |
| 13 | WhiteBIT | T2 | **2,798** | **0** | **1,232** | **331** | 🟢100 | ✅ Native | +127% |
| 14 | AscendEX | T2 | **4,159** | **4,023** | **1,694** | **63** | 🟢100 | ≈ Tie | 3% |
| 15 | BingX | T2 | **5,630** | **2,680** | **2,136** | **980** | 🟢100 | ✅ Native | +110% |
| 16 | Toobit | T2 | **1,173** | **1,296** | **858** | **0** | 🟢100 | ≈ Tie | 10% |
| 17 | Deepcoin | T2 | **53** | **0** | **924** | **0** | 🟢100 | 🟠 CCXT REST | +1643% |
| 18 | XT.com | T2 | **1,515** | **3,953** | **968** | **613** | 🟢85 | 🔵 CCXT Pro | +161% |
| 19 | Zoomex | T2 | **23,778** | **0** | **0** | **0** | 🟢100 | 🟢 Native Only | - |
| 20 | Bitget | T2 | **1,826** | **10,218** | **772** | **329** | 🟢85 | 🔵 CCXT Pro | +460% |
| 21 | Gemini | T2 | **0** | **0** | **859** | **572** | 🟢100 | 🟠 CCXT REST | +50% |
| 22 | Binance.US | T2 | **768** | **4,402** | **507** | **561** | 🟢85 | 🔵 CCXT Pro | +473% |
| 23 | MEXC | T2 | **1,135** | **0** | **0** | **946** | 🟢100 | ≈ Tie | 20% |
| 24 | CoinEx | T3 | **7,332** | **30,676** | **1,256** | **730** | 🟢85 | 🔵 CCXT Pro | +318% |
| 25 | LBank | T3 | **1,383** | **0** | **0** | **242** | 🟢100 | ✅ Native | +471% |
| 26 | BitMart | T3 | **6,716** | **16,559** | **882** | **480** | 🟢85 | 🔵 CCXT Pro | +147% |
| 27 | Pionex | T3 | **2,653** | **0** | **0** | **0** | 🟢100 | 🟢 Native Only | - |
| 28 | Poloniex | T3 | **6,437** | **0** | **1,694** | **430** | 🟢100 | ✅ Native | +280% |
| 29 | HitBTC | T3 | **8,471** | **7,444** | **1,003** | **626** | 🟢85 | ≈ Tie | 14% |
| 30 | BTSE | T3 | **4,711** | **0** | **0** | **95** | 🟢100 | 🟢 Native Only | - |
| 31 | Biconomy | T3 | **2,449** | **0** | **0** | **0** | 🟢100 | 🟢 Native Only | - |
| 32 | Hotcoin | T3 | **3,539** | **0** | **0** | **0** | 🟢100 | 🟢 Native Only | - |
| 33 | NovaEx | T3 | **913** | **0** | **0** | **0** | 🟢100 | 🟢 Native Only | - |
| 34 | FameEX | T3 | **0** | **0** | **0** | **0** | 🟢93 | ❌ Failed | - |
| 35 | Websea | T3 | **558** | **0** | **0** | **18** | 🟢100 | 🟢 Native Only | - |
| 36 | Bullish | T3 | **2,977** | **5,580** | **44** | **43** | 🟢86 | 🔵 CCXT Pro | +87% |
| 37 | Darkex | T3 | **287** | **0** | **0** | **0** | 🟢100 | 🟢 Native Only | - |
| 38 | Bitrue | T3 | **237** | **105** | **596** | **363** | 🟢85 | 🟠 CCXT REST | +64% |
| 39 | BloFin | T3 | **829** | **4,543** | **1,166** | **374** | 🟢95 | 🔵 CCXT Pro | +290% |
| 40 | DigiFinex | T3 | **4,193** | **0** | **726** | **384** | 🟢100 | ✅ Native | +478% |
| 41 | EXMO | T3 | **448** | **4,124** | **22** | **864** | 🟢85 | 🔵 CCXT Pro | +377% |
| 42 | CEX.IO | T3 | **213** | **0** | **263** | **147** | 🟢100 | ≈ Tie | 23% |
| 43 | OrangeX | T3 | **1,647** | **0** | **0** | **507** | 🟢100 | 🟢 Native Only | - |
| 44 | Azbit | T3 | **138** | **0** | **0** | **399** | 🟢100 | 🟢 Native Only | - |
| 45 | BVOX | T3 | **165** | **0** | **0** | **128** | 🟢100 | 🟢 Native Only | - |
| 46 | Trubit Pro | T3 | **176** | **0** | **0** | **230** | 🟢100 | 🟢 Native Only | - |
| 47 | BigONE | T3 | **8,685** | **0** | **397** | **902** | 🟢100 | ✅ Native | +863% |
| 48 | LATOKEN | T3 | **482** | **0** | **1,188** | **0** | 🟢100 | 🟠 CCXT REST | +146% |
| 49 | Coinstore | T3 | **2,320** | **0** | **0** | **150** | 🟢100 | 🟢 Native Only | - |
| 50 | GroveX | T3 | **478** | **0** | **0** | **568** | 🟢100 | 🟢 Native Only | - |
| 51 | CoinW | T3 | **374** | **0** | **0** | **381** | 🟢100 | 🟢 Native Only | - |
| 52 | Batonex | T3 | **138** | **0** | **0** | **86** | 🟢100 | 🟢 Native Only | - |
| 53 | CEEX | T3 | **601** | **0** | **0** | **0** | 🟢100 | 🟢 Native Only | - |

## Detailed Breakdown (Trades + Orderbook + Tickers)

| # | Exchange | N-TR | N-OB | Pro-TR | Pro-OB | Pro-TK | REST-TR | REST-OB | REST-TK | D-TR | D-OB | D-TK | Best |
|---|----------|------|------|--------|--------|--------|---------|---------|---------|------|------|------|------|
| 1 | Binance | 1,178 | 686 | 0 | 0 | 0 | 480 | 26 | 24 | 280 | 27 | 26 | Native |
| 2 | Coinbase | 287 | 3,002 | 128,131 | 2,452 | 1,721 | 920 | 45 | 61 | 310 | 30 | 29 | Pro |
| 3 | Kraken | 45 | 7,866 | 246 | 13,652 | 226 | 880 | 44 | 43 | 520 | 51 | 52 | Pro |
| 4 | KuCoin | 2,073 | 19,047 | 158 | 3,743 | 139 | 1,720 | 0 | 86 | 880 | 45 | 44 | Native |
| 5 | OKX | 699 | 3,616 | 1,771 | 2,425 | 1,910 | 880 | 47 | 46 | 460 | 47 | 48 | Pro |
| 6 | Bybit | 1,569 | 9,076 | 3,364 | 7,154 | 802 | 660 | 33 | 33 | 450 | 47 | 46 | Tie |
| 7 | Bitfinex | 308 | 19,543 | 0 | 0 | 0 | 120 | 0 | 75 | 270 | 71 | 71 | Native |
| 8 | Gate.io | 369 | 226 | 87 | 6 | 28 | 800 | 41 | 41 | 270 | 29 | 28 | REST |
| 9 | HTX | 214 | 1,592 | 0 | 0 | 0 | 840 | 42 | 42 | 43 | 40 | 41 | Native |
| 10 | WOO X | 141 | 842 | 359 | 1,252 | 449 | 1,940 | 97 | 0 | 420 | 41 | 0 | Tie |
| 11 | Crypto.com | 2,747 | 3,076 | 3,608 | 2,442 | 1,618 | 1,120 | 56 | 56 | 360 | 34 | 0 | Pro |
| 12 | Bitstamp | 18 | 107 | 20 | 19 | 0 | 215 | 13 | 13 | 21 | 3 | 5 | REST |
| 13 | WhiteBIT | 1,808 | 990 | 0 | 0 | 0 | 1,120 | 56 | 56 | 300 | 16 | 15 | Native |
| 14 | AscendEX | 873 | 3,286 | 856 | 3,167 | 0 | 1,540 | 77 | 77 | 0 | 63 | 0 | Tie |
| 15 | BingX | 3,255 | 2,375 | 1,095 | 779 | 806 | 1,940 | 98 | 98 | 890 | 90 | 0 | Native |
| 16 | Toobit | 852 | 321 | 737 | 149 | 410 | 780 | 39 | 39 | 0 | 0 | 0 | Tie |
| 17 | Deepcoin | 0 | 53 | 0 | 0 | 0 | 840 | 42 | 42 | 0 | 0 | 0 | REST |
| 18 | XT.com | 354 | 1,161 | 413 | 3,143 | 397 | 880 | 44 | 44 | 560 | 53 | 0 | Pro |
| 19 | Zoomex | 4,387 | 19,391 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Native Only |
| 20 | Bitget | 727 | 1,099 | 2,000 | 3,766 | 4,452 | 700 | 36 | 36 | 300 | 29 | 0 | Pro |
| 21 | Gemini | 0 | 0 | 0 | 0 | 0 | 780 | 39 | 40 | 520 | 52 | 0 | REST |
| 22 | Binance.US | 8 | 760 | 15 | 3,062 | 1,325 | 460 | 23 | 24 | 510 | 51 | 0 | Pro |
| 23 | MEXC | 1,080 | 55 | 0 | 0 | 0 | 0 | 0 | 0 | 860 | 86 | 0 | Tie |
| 24 | CoinEx | 1,661 | 5,671 | 28,249 | 2,395 | 32 | 1,140 | 58 | 58 | 730 | 0 | 0 | Pro |
| 25 | LBank | 765 | 618 | 0 | 0 | 0 | 0 | 0 | 0 | 220 | 22 | 0 | Native |
| 26 | BitMart | 4,015 | 2,701 | 3,700 | 10,425 | 2,434 | 800 | 41 | 41 | 480 | 0 | 0 | Pro |
| 27 | Pionex | 1,135 | 1,518 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Native Only |
| 28 | Poloniex | 936 | 5,501 | 0 | 0 | 0 | 1,540 | 77 | 77 | 390 | 40 | 0 | Native |
| 29 | HitBTC | 37 | 8,434 | 53 | 6,316 | 1,075 | 908 | 47 | 48 | 568 | 58 | 0 | Tie |
| 30 | BTSE | 2,229 | 2,482 | 0 | 0 | 0 | 0 | 0 | 0 | 48 | 47 | 0 | Native Only |
| 31 | Biconomy | 2,412 | 37 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Native Only |
| 32 | Hotcoin | 2,051 | 1,488 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Native Only |
| 33 | NovaEx | 178 | 735 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Native Only |
| 34 | FameEX | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | None |
| 35 | Websea | 558 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 18 | 0 | Native Only |
| 36 | Bullish | 2,954 | 23 | 3,356 | 422 | 1,802 | 40 | 2 | 2 | 0 | 43 | 0 | Pro |
| 37 | Darkex | 68 | 219 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Native Only |
| 38 | Bitrue | 230 | 7 | 0 | 105 | 0 | 540 | 28 | 28 | 330 | 33 | 0 | REST |
| 39 | BloFin | 100 | 729 | 240 | 1,933 | 2,370 | 1,060 | 53 | 53 | 340 | 34 | 0 | Pro |
| 40 | DigiFinex | 2,645 | 1,548 | 0 | 0 | 0 | 660 | 33 | 33 | 350 | 34 | 0 | Native |
| 41 | EXMO | 18 | 430 | 515 | 2,508 | 1,101 | 20 | 1 | 1 | 820 | 44 | 0 | Pro |
| 42 | CEX.IO | 202 | 11 | 0 | 0 | 0 | 180 | 40 | 43 | 141 | 6 | 0 | Tie |
| 43 | OrangeX | 1,620 | 27 | 0 | 0 | 0 | 0 | 0 | 0 | 480 | 27 | 0 | Native Only |
| 44 | Azbit | 115 | 23 | 0 | 0 | 0 | 0 | 0 | 0 | 380 | 19 | 0 | Native Only |
| 45 | BVOX | 135 | 30 | 0 | 0 | 0 | 0 | 0 | 0 | 105 | 23 | 0 | Native Only |
| 46 | Trubit Pro | 145 | 31 | 0 | 0 | 0 | 0 | 0 | 0 | 190 | 40 | 0 | Native Only |
| 47 | BigONE | 8,635 | 50 | 0 | 0 | 0 | 355 | 21 | 21 | 855 | 47 | 0 | Native |
| 48 | LATOKEN | 460 | 22 | 0 | 0 | 0 | 1,080 | 54 | 54 | 0 | 0 | 0 | REST |
| 49 | Coinstore | 1,800 | 520 | 0 | 0 | 0 | 0 | 0 | 0 | 150 | 0 | 0 | Native Only |
| 50 | GroveX | 52 | 426 | 0 | 0 | 0 | 0 | 0 | 0 | 540 | 28 | 0 | Native Only |
| 51 | CoinW | 340 | 34 | 0 | 0 | 0 | 0 | 0 | 0 | 360 | 21 | 0 | Native Only |
| 52 | Batonex | 115 | 23 | 0 | 0 | 0 | 0 | 0 | 0 | 70 | 16 | 0 | Native Only |
| 53 | CEEX | 540 | 61 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Native Only |

## Score Summary

| Category | Count |
|----------|-------|
| ✅ Native Wins | 10 |
| 🔵 CCXT Pro Wins | 12 |
| 🟠 CCXT REST Wins | 6 |
| 🟣 Direct REST Wins | 0 |
| ≈ Ties | 7 |
| 🟢 Native Only (no CCXT) | 17 |
| ❌ All Failed | 1 |
| **Total** | **53** |

## 🏥 Health Analysis

**Average Health Score:** 95/100

| Exchange | Health | Errors | Top Error Category | Fix Recommendation |
|----------|--------|--------|-------------------|--------------------|
| Binance | 🟢 100 | 3 | unknown(3) | - |
| Coinbase | 🟢 85 | 257 | unknown(143) | [high] Reduce subscription count per connection |
| Kraken | 🟢 85 | 309 | timeout(308) | [high] Reduce subscription count per connection |
| KuCoin | 🟢 85 | 121 | timeout(120) | [high] Reduce subscription count per connection |
| OKX | 🟢 85 | 171 | timeout(170) | [high] Reduce subscription count per connection |
| Bybit | 🟢 85 | 129 | timeout(128) | [high] Reduce subscription count per connection |
| Bitfinex | 🟢 100 | 7 | timeout(4) | [high] Reduce subscription count per connection |
| Gate.io | 🟢 85 | 94 | timeout(90) | [high] Reduce subscription count per connection |
| HTX | 🟢 100 | 5 | unknown(5) | - |
| WOO X | 🟢 85 | 198 | timeout(194) | [high] Reduce subscription count per connection |
| Crypto.com | 🟢 86 | 64 | timeout(60) | [high] Reduce subscription count per connection |
| Bitstamp | 🟢 85 | 163 | timeout(130) | [high] Reduce subscription count per connection |
| WhiteBIT | 🟢 100 | 1 | unknown(1) | - |
| AscendEX | 🟢 100 | 23 | notSupported(22) | [low] Use REST fallback for unsupported methods |
| BingX | 🟢 100 | 1 | unknown(1) | - |
| Toobit | 🟢 100 | 1 | unknown(1) | - |
| Deepcoin | 🟢 100 | 1 | unknown(1) | - |
| XT.com | 🟢 85 | 79 | timeout(78) | [high] Reduce subscription count per connection |
| Zoomex | 🟢 100 | 1 | unknown(1) | - |
| Bitget | 🟢 85 | 207 | unknown(111) | [high] Reduce subscription count per connection |
| Gemini | 🟢 100 | 9 | unknown(9) | - |
| Binance.US | 🟢 85 | 159 | timeout(149) | [high] Reduce subscription count per connection |
| MEXC | 🟢 100 | 6 | unknown(6) | - |
| CoinEx | 🟢 85 | 151 | timeout(150) | [high] Reduce subscription count per connection |
| LBank | 🟢 100 | 18 | unknown(18) | - |
| BitMart | 🟢 85 | 121 | timeout(120) | [high] Reduce subscription count per connection |
| Pionex | 🟢 100 | 4 | timeout(2) | - |
| Poloniex | 🟢 100 | 3 | unknown(3) | - |
| HitBTC | 🟢 85 | 121 | timeout(120) | [high] Reduce subscription count per connection |
| BTSE | 🟢 100 | 4 | unknown(4) | - |
| Biconomy | 🟢 100 | 6 | unknown(4) | - |
| Hotcoin | 🟢 100 | 4 | unknown(4) | - |
| NovaEx | 🟢 100 | 1 | unknown(1) | - |
| FameEX | 🟢 93 | 9 | unknown(5) | [high] Reduce subscription count per connection |
| Websea | 🟢 100 | 1 | unknown(1) | - |
| Bullish | 🟢 86 | 65 | timeout(54) | [high] Reduce subscription count per connection |
| Darkex | 🟢 100 | 3 | unknown(3) | - |
| Bitrue | 🟢 85 | 185 | timeout(112) | [high] Reduce subscription count per connection |
| BloFin | 🟢 95 | 33 | timeout(32) | [high] Reduce subscription count per connection |
| DigiFinex | 🟢 100 | 7 | unknown(4) | [medium] Verify pair availability on exchange |
| EXMO | 🟢 85 | 167 | timeout(162) | [high] Reduce subscription count per connection |
| CEX.IO | 🟢 100 | 0 | - | - |
| OrangeX | 🟢 100 | 0 | - | - |
| Azbit | 🟢 100 | 0 | - | - |
| BVOX | 🟢 100 | 0 | - | - |
| Trubit Pro | 🟢 100 | 0 | - | - |
| BigONE | 🟢 100 | 0 | - | - |
| LATOKEN | 🟢 100 | 2 | pairNotFound(2) | [medium] Verify pair availability on exchange |
| Coinstore | 🟢 100 | 4 | unknown(4) | - |
| GroveX | 🟢 100 | 3 | unknown(3) | - |
| CoinW | 🟢 100 | 0 | - | - |
| Batonex | 🟢 100 | 0 | - | - |
| CEEX | 🟢 100 | 7 | unknown(7) | - |

## Per-Exchange Per-Pair Detail

### Binance (CCXT: binance) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDC | 63 | 76 | 0 | 0 | 40 | 2 | 20 | 1 | ✅ Native |
| BTC_USDT | 175 | 87 | 0 | 0 | 40 | 2 | 20 | 2 | ✅ Native |
| ENA_USDC | 40 | 11 | 0 | 0 | 0 | 1 | 20 | 2 | ✅ Native |
| ENA_USDT | 52 | 22 | 0 | 0 | 20 | 1 | 20 | 2 | ✅ Native |
| ETH_USDC | 114 | 78 | 0 | 0 | 40 | 2 | 20 | 2 | ✅ Native |
| ETH_USDT | 279 | 87 | 0 | 0 | 40 | 2 | 20 | 2 | ✅ Native |
| PENGU_USDC | 53 | 20 | 0 | 0 | 20 | 2 | 20 | 2 | ✅ Native |
| PENGU_USDT | 45 | 47 | 0 | 0 | 40 | 2 | 20 | 2 | ✅ Native |
| SOL_USDC | 55 | 48 | 0 | 0 | 40 | 2 | 20 | 2 | ✅ Native |
| SOL_USDT | 106 | 77 | 0 | 0 | 40 | 2 | 20 | 2 | ✅ Native |
| SUI_USDC | 46 | 46 | 0 | 0 | 40 | 2 | 20 | 2 | ✅ Native |
| SUI_USDT | 60 | 57 | 0 | 0 | 40 | 2 | 20 | 2 | ✅ Native |
| WIF_USDC | 45 | 6 | 0 | 0 | 40 | 2 | 20 | 2 | ✅ Native |
| WIF_USDT | 45 | 24 | 0 | 0 | 40 | 2 | 20 | 2 | ✅ Native |

### Coinbase (CCXT: coinbase) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USD | 149 | 508 | 470 | 409 | 80 | 4 | 30 | 3 | 🔵 Pro |
| BTC_USDC | 0 | 0 | 95952 | 409 | 60 | 3 | 0 | 0 | 🔵 Pro |
| BTC_USDT | 2 | 451 | 4 | 324 | 80 | 4 | 30 | 3 | ✅ Native |
| ENA_USD | 1 | 50 | 0 | 0 | 60 | 3 | 20 | 2 | 🟠 REST |
| ENA_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ❌ |
| ETH_USD | 48 | 494 | 181 | 378 | 80 | 3 | 30 | 3 | 🔵 Pro |
| ETH_USDC | 0 | 0 | 14706 | 378 | 60 | 3 | 0 | 0 | 🔵 Pro |
| ETH_USDT | 1 | 353 | 1 | 0 | 60 | 3 | 30 | 3 | ✅ Native |
| PENGU_USD | 3 | 162 | 0 | 0 | 60 | 3 | 30 | 3 | ✅ Native |
| PENGU_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ❌ |
| POPCAT_USD | 1 | 27 | 0 | 0 | 60 | 3 | 30 | 3 | 🟠 REST |
| POPCAT_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ❌ |
| SOL_USD | 60 | 376 | 193 | 277 | 80 | 4 | 30 | 3 | 🔵 Pro |
| SOL_USDC | 0 | 0 | 16624 | 277 | 60 | 3 | 0 | 0 | 🔵 Pro |
| SOL_USDT | 1 | 181 | 0 | 0 | 60 | 3 | 30 | 3 | ✅ Native |
| SUI_USD | 20 | 351 | 0 | 0 | 60 | 3 | 20 | 2 | ✅ Native |
| SUI_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ❌ |
| WIF_USD | 1 | 49 | 0 | 0 | 60 | 3 | 30 | 2 | 🟠 REST |
| WIF_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ❌ |

**CCXT Pro Errors:** SOL/USDC:tk:rate limit exceeded; BTC/USDT:tr:rate limit exceeded; ETH/USD:ob:rate limit exceeded

**Fix Recommendations:** [high] Reduce subscription count per connection

### Kraken (CCXT: kraken) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USD | 27 | 960 | 136 | 1296 | 60 | 3 | 0 | 0 | 🔵 Pro |
| BTC_USDC | 0 | 854 | 4 | 2060 | 60 | 3 | 0 | 0 | 🔵 Pro |
| BTC_USDT | 0 | 1113 | 8 | 1541 | 60 | 3 | 0 | 0 | 🔵 Pro |
| ENA_USD | 0 | 48 | 0 | 386 | 40 | 2 | 30 | 3 | 🔵 Pro |
| ETH_USD | 11 | 998 | 37 | 1578 | 60 | 3 | 30 | 3 | 🔵 Pro |
| ETH_USDC | 0 | 536 | 3 | 998 | 60 | 3 | 30 | 3 | 🔵 Pro |
| ETH_USDT | 0 | 933 | 7 | 1274 | 60 | 3 | 40 | 4 | 🔵 Pro |
| PENGU_USD | 0 | 169 | 5 | 635 | 60 | 3 | 30 | 3 | 🔵 Pro |
| PENGU_USDC | 0 | 51 | 0 | 139 | 60 | 3 | 30 | 3 | 🔵 Pro |
| PENGU_USDT | 0 | 58 | 0 | 180 | 60 | 3 | 30 | 3 | 🔵 Pro |
| POPCAT_USD | 0 | 110 | 0 | 258 | 40 | 2 | 30 | 3 | 🔵 Pro |
| SOL_USD | 5 | 827 | 26 | 1104 | 60 | 3 | 30 | 3 | 🔵 Pro |
| SOL_USDC | 0 | 107 | 3 | 412 | 60 | 3 | 30 | 3 | 🔵 Pro |
| SOL_USDT | 0 | 556 | 2 | 678 | 60 | 3 | 40 | 4 | 🔵 Pro |
| SUI_USD | 2 | 303 | 15 | 645 | 40 | 2 | 30 | 3 | 🔵 Pro |
| WIF_USD | 0 | 243 | 0 | 468 | 40 | 2 | 30 | 3 | 🔵 Pro |
| XBT_USD | 0 | 0 | 0 | 0 | 0 | 0 | 40 | 4 | 🟣 Direct |
| XBT_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 30 | 3 | 🟣 Direct |
| XBT_USDT | 0 | 0 | 0 | 0 | 0 | 0 | 40 | 3 | 🟣 Direct |

**Fix Recommendations:** [high] Reduce subscription count per connection

### KuCoin (CCXT: kucoin) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 5 | 332 | 1 | 2 | 120 | 0 | 60 | 3 | ✅ Native |
| BTC_USDC | 88 | 1816 | 5 | 32 | 120 | 0 | 40 | 3 | ✅ Native |
| BTC_USDT | 837 | 2686 | 53 | 1534 | 120 | 0 | 60 | 3 | ✅ Native |
| ENA_USDC | 0 | 96 | 0 | 0 | 100 | 0 | 60 | 3 | ≈ Tie |
| ENA_USDT | 17 | 1102 | 0 | 59 | 100 | 0 | 60 | 3 | ✅ Native |
| ETH_USDC | 228 | 1865 | 26 | 128 | 120 | 0 | 60 | 3 | ✅ Native |
| ETH_USDT | 390 | 2388 | 28 | 951 | 120 | 0 | 60 | 3 | ✅ Native |
| PENGU_USDT | 57 | 988 | 0 | 66 | 120 | 0 | 60 | 3 | ✅ Native |
| POPCAT_USDT | 0 | 895 | 0 | 23 | 120 | 0 | 60 | 3 | ✅ Native |
| SOL_USDC | 4 | 928 | 0 | 125 | 120 | 0 | 60 | 3 | ✅ Native |
| SOL_USDT | 343 | 1840 | 44 | 591 | 120 | 0 | 60 | 3 | ✅ Native |
| SUI_USDC | 0 | 764 | 0 | 57 | 100 | 0 | 60 | 3 | ✅ Native |
| SUI_USDT | 79 | 1919 | 0 | 82 | 100 | 0 | 60 | 3 | ✅ Native |
| WIF_USDC | 0 | 108 | 0 | 0 | 120 | 0 | 60 | 3 | ≈ Tie |
| WIF_USDT | 25 | 1320 | 1 | 93 | 120 | 0 | 60 | 3 | ✅ Native |

**Fix Recommendations:** [high] Reduce subscription count per connection

### OKX (CCXT: okx) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDC | 2 | 114 | 1 | 183 | 60 | 3 | 30 | 3 | 🔵 Pro |
| BTC_USDT | 373 | 504 | 1062 | 331 | 60 | 3 | 20 | 3 | 🔵 Pro |
| ENA_USD | 1 | 49 | 1 | 64 | 40 | 2 | 20 | 2 | 🔵 Pro |
| ENA_USDC | 0 | 47 | 0 | 23 | 0 | 2 | 0 | 2 | ✅ Native |
| ENA_USDT | 11 | 124 | 9 | 109 | 40 | 2 | 20 | 2 | ≈ Tie |
| ETH_USDC | 1 | 177 | 0 | 133 | 60 | 3 | 30 | 3 | ✅ Native |
| ETH_USDT | 225 | 394 | 429 | 295 | 60 | 3 | 30 | 3 | 🔵 Pro |
| PENGU_USD | 0 | 100 | 0 | 53 | 60 | 3 | 30 | 3 | ✅ Native |
| PENGU_USDC | 0 | 88 | 0 | 35 | 60 | 3 | 30 | 3 | ✅ Native |
| PENGU_USDT | 9 | 219 | 27 | 147 | 40 | 3 | 30 | 3 | ✅ Native |
| SOL_USDC | 1 | 94 | 0 | 50 | 60 | 3 | 30 | 3 | ✅ Native |
| SOL_USDT | 52 | 480 | 160 | 292 | 60 | 3 | 30 | 2 | ≈ Tie |
| SUI_USD | 0 | 256 | 1 | 234 | 40 | 2 | 20 | 2 | ≈ Tie |
| SUI_USDC | 0 | 161 | 0 | 62 | 40 | 2 | 20 | 2 | ✅ Native |
| SUI_USDT | 23 | 414 | 72 | 234 | 40 | 2 | 30 | 2 | ✅ Native |
| WIF_USD | 0 | 115 | 0 | 53 | 40 | 2 | 30 | 3 | ✅ Native |
| WIF_USDC | 0 | 91 | 0 | 43 | 60 | 3 | 30 | 3 | ✅ Native |
| WIF_USDT | 1 | 189 | 9 | 84 | 60 | 3 | 30 | 3 | ✅ Native |

**Fix Recommendations:** [high] Reduce subscription count per connection

### Bybit (CCXT: bybit) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDC | 0 | 65 | 0 | 54 | 40 | 2 | 30 | 3 | ✅ Native |
| BRETT_USDT | 0 | 283 | 0 | 236 | 40 | 2 | 30 | 3 | ≈ Tie |
| BTC_USDC | 60 | 972 | 113 | 716 | 40 | 2 | 30 | 3 | ✅ Native |
| BTC_USDT | 773 | 2147 | 1924 | 1768 | 60 | 3 | 20 | 3 | 🔵 Pro |
| ENA_USDC | 0 | 30 | 0 | 26 | 40 | 2 | 20 | 2 | 🟠 REST |
| ENA_USDT | 1 | 107 | 0 | 132 | 40 | 2 | 30 | 3 | 🔵 Pro |
| ETH_USDC | 161 | 713 | 363 | 606 | 40 | 2 | 30 | 3 | 🔵 Pro |
| ETH_USDT | 299 | 1110 | 522 | 840 | 40 | 2 | 30 | 3 | ≈ Tie |
| PENGU_USDT | 11 | 368 | 11 | 260 | 40 | 2 | 30 | 3 | ✅ Native |
| POPCAT_USDT | 0 | 170 | 0 | 153 | 40 | 2 | 20 | 3 | ≈ Tie |
| SOL_USDC | 4 | 548 | 15 | 440 | 40 | 2 | 30 | 3 | ✅ Native |
| SOL_USDT | 187 | 915 | 246 | 690 | 40 | 2 | 30 | 3 | ≈ Tie |
| SUI_USDC | 2 | 377 | 2 | 275 | 40 | 2 | 30 | 3 | ✅ Native |
| SUI_USDT | 70 | 1029 | 168 | 619 | 40 | 2 | 30 | 3 | ✅ Native |
| WIF_USDC | 0 | 77 | 0 | 132 | 40 | 2 | 30 | 3 | 🔵 Pro |
| WIF_USDT | 1 | 165 | 0 | 207 | 40 | 2 | 30 | 3 | 🔵 Pro |

**Fix Recommendations:** [high] Reduce subscription count per connection

### Bitfinex (CCXT: bitfinex) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USD | 142 | 4119 | 0 | 0 | 20 | 0 | 28 | 8 | ✅ Native |
| BTC_USDT | 32 | 3822 | 0 | 0 | 20 | 0 | 28 | 7 | ✅ Native |
| ENA_USD | 0 | 0 | 0 | 0 | 0 | 0 | 21 | 7 | 🟣 Direct |
| ENA_USDT | 0 | 0 | 0 | 0 | 0 | 0 | 28 | 7 | 🟣 Direct |
| ETH_USD | 52 | 2874 | 0 | 0 | 20 | 0 | 25 | 7 | ✅ Native |
| ETH_USDT | 16 | 2598 | 0 | 0 | 20 | 0 | 28 | 7 | ✅ Native |
| SOL_USD | 32 | 2911 | 0 | 0 | 20 | 0 | 28 | 7 | ✅ Native |
| SOL_USDT | 34 | 3219 | 0 | 0 | 20 | 0 | 28 | 7 | ✅ Native |
| SUI_USD | 0 | 0 | 0 | 0 | 0 | 0 | 28 | 7 | 🟣 Direct |
| SUI_USDT | 0 | 0 | 0 | 0 | 0 | 0 | 28 | 7 | 🟣 Direct |

**Native Errors:** Opening handshake has timed out

**Fix Recommendations:** [high] Reduce subscription count per connection

### Gate.io (CCXT: gateio) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 30 | 10 | 0 | 0 | 60 | 3 | 20 | 2 | 🟠 REST |
| BTC_USDC | 0 | 17 | 0 | 3 | 60 | 3 | 20 | 2 | 🟠 REST |
| BTC_USDT | 69 | 19 | 24 | 1 | 60 | 3 | 10 | 2 | ✅ Native |
| ENA_USDC | 0 | 17 | 0 | 0 | 40 | 2 | 10 | 1 | 🟠 REST |
| ENA_USDT | 31 | 21 | 0 | 0 | 40 | 2 | 20 | 2 | ✅ Native |
| ETH_USDC | 0 | 17 | 0 | 0 | 60 | 3 | 20 | 2 | 🟠 REST |
| ETH_USDT | 88 | 19 | 59 | 1 | 60 | 3 | 20 | 2 | ✅ Native |
| PENGU_USDT | 26 | 12 | 0 | 0 | 40 | 3 | 20 | 2 | ≈ Tie |
| POPCAT_USDT | 25 | 14 | 0 | 0 | 60 | 3 | 20 | 2 | 🟠 REST |
| SOL_USDC | 0 | 15 | 0 | 0 | 60 | 3 | 20 | 2 | 🟠 REST |
| SOL_USDT | 43 | 18 | 0 | 1 | 60 | 3 | 10 | 2 | ≈ Tie |
| SUI_USDC | 0 | 19 | 0 | 0 | 40 | 2 | 20 | 2 | 🟠 REST |
| SUI_USDT | 27 | 17 | 4 | 0 | 40 | 2 | 20 | 2 | ≈ Tie |
| WIF_USDC | 0 | 0 | 0 | 0 | 60 | 3 | 20 | 2 | 🟠 REST |
| WIF_USDT | 30 | 11 | 0 | 0 | 60 | 3 | 20 | 2 | 🟠 REST |

**Fix Recommendations:** [high] Reduce subscription count per connection

### HTX (CCXT: htx) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 29 | 90 | 0 | 0 | 80 | 4 | 4 | 4 | ✅ Native |
| BTC_USDC | 3 | 180 | 0 | 0 | 80 | 4 | 4 | 4 | ✅ Native |
| BTC_USDT | 39 | 162 | 0 | 0 | 80 | 4 | 5 | 4 | ✅ Native |
| ENA_USDT | 18 | 75 | 0 | 0 | 60 | 3 | 3 | 3 | ✅ Native |
| ETH_USDC | 3 | 177 | 0 | 0 | 80 | 4 | 3 | 4 | ✅ Native |
| ETH_USDT | 20 | 181 | 0 | 0 | 80 | 4 | 4 | 4 | ✅ Native |
| PENGU_USDT | 50 | 154 | 0 | 0 | 80 | 4 | 5 | 4 | ✅ Native |
| POPCAT_USDT | 5 | 101 | 0 | 0 | 80 | 4 | 4 | 4 | ✅ Native |
| SOL_USDT | 19 | 180 | 0 | 0 | 80 | 4 | 5 | 4 | ✅ Native |
| SUI_USDT | 25 | 178 | 0 | 0 | 60 | 3 | 3 | 3 | ✅ Native |
| WIF_USDT | 3 | 114 | 0 | 0 | 80 | 4 | 3 | 2 | ✅ Native |

### WOO X (CCXT: woo) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 3 | 77 | 0 | 115 | 180 | 9 | 50 | 5 | 🟠 REST |
| BTC_USDC | 38 | 80 | 147 | 120 | 180 | 9 | 0 | 0 | 🔵 Pro |
| BTC_USDT | 0 | 90 | 0 | 131 | 180 | 9 | 50 | 5 | 🟠 REST |
| ENA_USDT | 5 | 73 | 10 | 110 | 160 | 8 | 40 | 4 | 🟠 REST |
| ETH_USDC | 33 | 81 | 30 | 117 | 180 | 9 | 0 | 0 | 🟠 REST |
| ETH_USDT | 10 | 98 | 54 | 144 | 180 | 9 | 50 | 5 | 🔵 Pro |
| PENGU_USDT | 19 | 81 | 54 | 115 | 180 | 9 | 50 | 4 | 🟠 REST |
| POPCAT_USDT | 0 | 25 | 0 | 30 | 180 | 9 | 50 | 5 | 🟠 REST |
| SOL_USDT | 27 | 91 | 63 | 135 | 180 | 9 | 50 | 5 | 🔵 Pro |
| SUI_USDT | 6 | 90 | 1 | 139 | 160 | 8 | 40 | 4 | 🟠 REST |
| WIF_USDT | 0 | 56 | 0 | 96 | 180 | 9 | 40 | 4 | 🟠 REST |

**Native Errors:** socket hang up

**Fix Recommendations:** [high] Reduce subscription count per connection

### Crypto.com (CCXT: cryptocom) — Health: 86/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USD | 400 | 172 | 504 | 164 | 80 | 4 | 30 | 3 | 🔵 Pro |
| BTC_USDC | 0 | 172 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| BTC_USDT | 262 | 170 | 310 | 161 | 80 | 4 | 30 | 3 | 🔵 Pro |
| ENA_USD | 100 | 171 | 150 | 164 | 60 | 3 | 20 | 2 | 🔵 Pro |
| ENA_USDT | 100 | 170 | 150 | 163 | 60 | 3 | 20 | 2 | 🔵 Pro |
| ETH_USD | 548 | 170 | 644 | 163 | 80 | 4 | 30 | 3 | 🔵 Pro |
| ETH_USDC | 0 | 172 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDT | 333 | 170 | 399 | 162 | 80 | 4 | 30 | 3 | 🔵 Pro |
| PENGU_USD | 100 | 170 | 150 | 162 | 80 | 4 | 20 | 2 | 🔵 Pro |
| PENGU_USDT | 100 | 171 | 150 | 164 | 80 | 4 | 20 | 2 | 🔵 Pro |
| POPCAT_USD | 100 | 171 | 150 | 163 | 80 | 4 | 20 | 1 | 🔵 Pro |
| SOL_USD | 157 | 171 | 208 | 162 | 80 | 4 | 30 | 3 | 🔵 Pro |
| SOL_USDC | 0 | 172 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SOL_USDT | 144 | 171 | 189 | 164 | 80 | 4 | 30 | 3 | 🔵 Pro |
| SUI_USD | 103 | 170 | 152 | 162 | 60 | 3 | 20 | 2 | 🔵 Pro |
| SUI_USDT | 100 | 171 | 152 | 162 | 60 | 3 | 20 | 1 | 🔵 Pro |
| WIF_USD | 100 | 171 | 150 | 163 | 80 | 4 | 20 | 2 | 🔵 Pro |
| WIF_USDT | 100 | 171 | 150 | 163 | 80 | 4 | 20 | 2 | 🔵 Pro |

**Fix Recommendations:** [high] Reduce subscription count per connection

### Bitstamp (CCXT: bitstamp) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USD | 18 | 23 | 12 | 2 | 20 | 1 | 0 | 0 | ✅ Native |
| BTC_USDC | 0 | 12 | 0 | 2 | 20 | 1 | 0 | 0 | 🟠 REST |
| BTC_USDT | 0 | 8 | 0 | 1 | 12 | 1 | 8 | 1 | 🟠 REST |
| ENA_USD | 0 | 1 | 0 | 0 | 18 | 1 | 0 | 0 | 🟠 REST |
| ETH_USD | 0 | 12 | 4 | 3 | 20 | 1 | 0 | 0 | 🟠 REST |
| ETH_USDC | 0 | 10 | 0 | 3 | 20 | 1 | 0 | 0 | 🟠 REST |
| ETH_USDT | 0 | 6 | 0 | 1 | 13 | 1 | 13 | 1 | 🟣 Direct |
| PENGU_USD | 0 | 8 | 0 | 1 | 20 | 1 | 0 | 0 | 🟠 REST |
| POPCAT_USD | 0 | 0 | 0 | 0 | 6 | 1 | 0 | 0 | 🟠 REST |
| SOL_USD | 0 | 11 | 4 | 3 | 20 | 1 | 0 | 1 | 🟠 REST |
| SOL_USDC | 0 | 7 | 0 | 2 | 6 | 1 | 0 | 0 | ≈ Tie |
| SUI_USD | 0 | 7 | 0 | 1 | 20 | 1 | 0 | 0 | 🟠 REST |
| WIF_USD | 0 | 2 | 0 | 0 | 20 | 1 | 0 | 0 | 🟠 REST |

**CCXT Pro Errors:** BTC/USD:tk:notSupported; ETH/USD:tk:notSupported; SOL/USD:tk:notSupported

**Fix Recommendations:** [high] Reduce subscription count per connection; [low] Use REST fallback for unsupported methods

### WhiteBIT (CCXT: whitebit) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USD | 100 | 73 | 0 | 0 | 60 | 3 | 0 | 0 | ✅ Native |
| BTC_USDC | 102 | 59 | 0 | 0 | 80 | 4 | 20 | 1 | ✅ Native |
| BTC_USDT | 103 | 151 | 0 | 0 | 80 | 4 | 40 | 2 | ✅ Native |
| ENA_USDC | 100 | 7 | 0 | 0 | 60 | 3 | 20 | 1 | ✅ Native |
| ENA_USDT | 118 | 44 | 0 | 0 | 60 | 3 | 20 | 1 | ✅ Native |
| ETH_USD | 100 | 47 | 0 | 0 | 60 | 3 | 0 | 0 | ✅ Native |
| ETH_USDC | 101 | 50 | 0 | 0 | 80 | 4 | 20 | 1 | ✅ Native |
| ETH_USDT | 125 | 102 | 0 | 0 | 80 | 4 | 40 | 2 | ✅ Native |
| PENGU_USDC | 102 | 33 | 0 | 0 | 60 | 3 | 20 | 1 | ✅ Native |
| PENGU_USDT | 102 | 66 | 0 | 0 | 60 | 3 | 0 | 1 | ✅ Native |
| SOL_USD | 100 | 62 | 0 | 0 | 60 | 3 | 0 | 0 | ✅ Native |
| SOL_USDC | 101 | 63 | 0 | 0 | 60 | 3 | 20 | 1 | ✅ Native |
| SOL_USDT | 128 | 124 | 0 | 0 | 80 | 4 | 20 | 1 | ✅ Native |
| SUI_USDC | 100 | 21 | 0 | 0 | 60 | 3 | 20 | 1 | ✅ Native |
| SUI_USDT | 122 | 73 | 0 | 0 | 60 | 3 | 20 | 1 | ✅ Native |
| WIF_USDC | 100 | 3 | 0 | 0 | 60 | 3 | 20 | 1 | ✅ Native |
| WIF_USDT | 104 | 12 | 0 | 0 | 60 | 3 | 20 | 1 | ✅ Native |

### AscendEX (CCXT: ascendex) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 8 | 27 | 8 | 27 | 140 | 7 | 0 | 8 | 🟠 REST |
| BTC_USD | 13 | 105 | 13 | 104 | 140 | 7 | 0 | 0 | 🟠 REST |
| BTC_USDT | 265 | 936 | 261 | 903 | 140 | 7 | 0 | 7 | ≈ Tie |
| ENA_USDT | 38 | 16 | 37 | 16 | 140 | 7 | 0 | 8 | 🟠 REST |
| ETH_USD | 17 | 118 | 18 | 115 | 140 | 7 | 0 | 0 | ≈ Tie |
| ETH_USDT | 271 | 934 | 264 | 902 | 140 | 7 | 0 | 8 | ≈ Tie |
| PENGU_USDT | 5 | 44 | 4 | 43 | 140 | 7 | 0 | 8 | 🟠 REST |
| SOL_USD | 14 | 50 | 14 | 48 | 140 | 7 | 0 | 0 | 🟠 REST |
| SOL_USDT | 96 | 926 | 93 | 893 | 140 | 7 | 0 | 8 | ≈ Tie |
| SUI_USDT | 81 | 62 | 81 | 59 | 140 | 7 | 0 | 8 | ≈ Tie |
| WIF_USDT | 65 | 68 | 63 | 57 | 140 | 7 | 0 | 8 | ≈ Tie |

**CCXT Pro Errors:** BTC/USDT:tk:notSupported; ETH/USDT:tk:notSupported; SOL/USDT:tk:notSupported

**Fix Recommendations:** [low] Use REST fallback for unsupported methods

### BingX (CCXT: bingx) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 45 | 74 | 12 | 21 | 160 | 8 | 70 | 7 | 🟠 REST |
| BTC_USDC | 394 | 293 | 143 | 111 | 140 | 8 | 70 | 7 | ✅ Native |
| BTC_USDT | 365 | 254 | 106 | 80 | 160 | 8 | 60 | 7 | ✅ Native |
| ENA_USDT | 49 | 59 | 8 | 13 | 140 | 7 | 70 | 7 | 🟠 REST |
| ETH_USDC | 405 | 281 | 137 | 91 | 160 | 8 | 70 | 7 | ✅ Native |
| ETH_USDT | 379 | 268 | 117 | 75 | 160 | 8 | 70 | 7 | ✅ Native |
| PENGU_USDT | 305 | 220 | 106 | 79 | 140 | 7 | 70 | 7 | ✅ Native |
| POPCAT_USDT | 9 | 42 | 3 | 13 | 140 | 7 | 70 | 6 | 🟠 REST |
| SOL_USDC | 331 | 230 | 135 | 90 | 160 | 8 | 70 | 7 | ✅ Native |
| SOL_USDT | 425 | 247 | 151 | 79 | 160 | 8 | 70 | 7 | ✅ Native |
| SUI_USDC | 271 | 215 | 62 | 54 | 140 | 7 | 60 | 7 | ✅ Native |
| SUI_USDT | 266 | 168 | 111 | 65 | 140 | 7 | 70 | 7 | ✅ Native |
| WIF_USDT | 11 | 24 | 4 | 8 | 140 | 7 | 70 | 7 | 🟠 REST |

### Toobit (CCXT: toobit) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDC | 84 | 24 | 68 | 8 | 80 | 4 | 0 | 0 | ✅ Native |
| BTC_USDT | 124 | 90 | 85 | 37 | 80 | 4 | 0 | 0 | ✅ Native |
| ENA_USDT | 63 | 1 | 61 | 1 | 60 | 3 | 0 | 0 | ≈ Tie |
| ETH_USDC | 75 | 21 | 65 | 9 | 80 | 4 | 0 | 0 | ≈ Tie |
| ETH_USDT | 87 | 45 | 72 | 24 | 80 | 4 | 0 | 0 | ✅ Native |
| PENGU_USDT | 64 | 22 | 61 | 10 | 60 | 3 | 0 | 0 | ✅ Native |
| POPCAT_USDT | 60 | 1 | 60 | 1 | 60 | 3 | 0 | 0 | ≈ Tie |
| SOL_USDC | 69 | 22 | 64 | 12 | 80 | 4 | 0 | 0 | ≈ Tie |
| SOL_USDT | 89 | 30 | 73 | 17 | 80 | 4 | 0 | 0 | ✅ Native |
| SUI_USDT | 77 | 64 | 68 | 29 | 60 | 3 | 0 | 0 | ✅ Native |
| WIF_USDT | 60 | 1 | 60 | 1 | 60 | 3 | 0 | 0 | ≈ Tie |

### Deepcoin (CCXT: deepcoin) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 0 | 10 | 0 | 0 | 180 | 9 | 0 | 0 | 🟠 REST |
| ETH_USDT | 0 | 10 | 0 | 0 | 180 | 9 | 0 | 0 | 🟠 REST |
| PENGU_USDT | 0 | 11 | 0 | 0 | 160 | 8 | 0 | 0 | 🟠 REST |
| SOL_USDT | 0 | 11 | 0 | 0 | 160 | 8 | 0 | 0 | 🟠 REST |
| WIF_USDT | 0 | 11 | 0 | 0 | 160 | 8 | 0 | 0 | 🟠 REST |

### XT.com (CCXT: xt) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 8 | 90 | 6 | 140 | 60 | 3 | 30 | 4 | 🔵 Pro |
| BTC_USDC | 22 | 89 | 31 | 151 | 80 | 4 | 50 | 5 | 🔵 Pro |
| BTC_USDT | 87 | 89 | 111 | 321 | 80 | 4 | 50 | 5 | 🔵 Pro |
| ENA_USDT | 13 | 89 | 8 | 303 | 60 | 3 | 40 | 3 | 🔵 Pro |
| ETH_USDC | 11 | 89 | 8 | 305 | 80 | 4 | 50 | 4 | 🔵 Pro |
| ETH_USDT | 85 | 89 | 103 | 406 | 80 | 4 | 50 | 5 | 🔵 Pro |
| PENGU_USDT | 18 | 90 | 22 | 316 | 60 | 3 | 40 | 4 | 🔵 Pro |
| POPCAT_USDT | 6 | 89 | 2 | 155 | 60 | 3 | 40 | 3 | 🔵 Pro |
| SOL_USDC | 12 | 90 | 14 | 136 | 60 | 3 | 40 | 4 | 🔵 Pro |
| SOL_USDT | 51 | 90 | 58 | 341 | 80 | 4 | 50 | 4 | 🔵 Pro |
| SUI_USDC | 11 | 89 | 16 | 53 | 60 | 3 | 40 | 4 | ✅ Native |
| SUI_USDT | 25 | 89 | 29 | 264 | 60 | 3 | 40 | 4 | 🔵 Pro |
| WIF_USDT | 5 | 89 | 5 | 252 | 60 | 3 | 40 | 4 | 🔵 Pro |

**Fix Recommendations:** [high] Reduce subscription count per connection

### Zoomex (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 7 | 937 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| BTC_USDT | 2899 | 6700 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ENA_USDT | 27 | 429 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDT | 789 | 3511 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| PENGU_USDT | 74 | 1112 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| POPCAT_USDT | 3 | 548 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SOL_USDT | 343 | 2631 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SUI_USDT | 239 | 2875 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| WIF_USDT | 6 | 648 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |

### Bitget (CCXT: bitget) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 50 | 15 | 102 | 93 | 60 | 3 | 30 | 3 | 🔵 Pro |
| BTC_USDC | 56 | 130 | 119 | 407 | 60 | 3 | 30 | 3 | 🔵 Pro |
| BTC_USDT | 133 | 167 | 353 | 514 | 60 | 3 | 30 | 2 | 🔵 Pro |
| ENA_USDT | 50 | 23 | 101 | 112 | 40 | 3 | 10 | 1 | 🔵 Pro |
| ETH_USDC | 57 | 90 | 124 | 354 | 60 | 3 | 30 | 3 | 🔵 Pro |
| ETH_USDT | 91 | 146 | 368 | 522 | 60 | 3 | 30 | 3 | 🔵 Pro |
| PENGU_USDT | 53 | 82 | 124 | 283 | 60 | 3 | 30 | 3 | 🔵 Pro |
| SOL_USDC | 50 | 80 | 103 | 252 | 60 | 3 | 30 | 3 | 🔵 Pro |
| SOL_USDT | 62 | 189 | 173 | 506 | 60 | 3 | 20 | 2 | 🔵 Pro |
| SUI_USDC | 0 | 0 | 102 | 206 | 60 | 3 | 20 | 2 | 🔵 Pro |
| SUI_USDT | 75 | 125 | 228 | 366 | 60 | 3 | 20 | 2 | 🔵 Pro |
| WIF_USDT | 50 | 52 | 103 | 151 | 60 | 3 | 20 | 2 | 🔵 Pro |

**CCXT Pro Errors:** SOL/USDT:ob:bitget SOL/USDT : orderbook data checksu; WIF/USDT:ob:bitget WIF/USDT : orderbook data checksu; SUI/USDT:ob:bitget SUI/USDT : orderbook data checksu

**Fix Recommendations:** [high] Reduce subscription count per connection

### Gemini (CCXT: gemini) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USD | 0 | 0 | 0 | 0 | 60 | 3 | 40 | 4 | 🟠 REST |
| BTC_USDC | 0 | 0 | 0 | 0 | 60 | 3 | 0 | 0 | 🟠 REST |
| BTC_USDT | 0 | 0 | 0 | 0 | 60 | 3 | 50 | 5 | 🟠 REST |
| ETH_USD | 0 | 0 | 0 | 0 | 60 | 3 | 50 | 5 | 🟠 REST |
| ETH_USDC | 0 | 0 | 0 | 0 | 40 | 3 | 0 | 0 | 🟠 REST |
| ETH_USDT | 0 | 0 | 0 | 0 | 60 | 3 | 50 | 5 | 🟠 REST |
| PENGU_USD | 0 | 0 | 0 | 0 | 60 | 3 | 50 | 5 | 🟠 REST |
| PENGU_USDC | 0 | 0 | 0 | 0 | 60 | 3 | 50 | 5 | 🟠 REST |
| POPCAT_USD | 0 | 0 | 0 | 0 | 60 | 3 | 50 | 5 | 🟠 REST |
| POPCAT_USDC | 0 | 0 | 0 | 0 | 60 | 3 | 50 | 5 | 🟠 REST |
| SOL_USD | 0 | 0 | 0 | 0 | 60 | 3 | 50 | 5 | 🟠 REST |
| SOL_USDC | 0 | 0 | 0 | 0 | 60 | 2 | 0 | 0 | 🟠 REST |
| WIF_USD | 0 | 0 | 0 | 0 | 40 | 2 | 40 | 4 | 🟣 Direct |
| WIF_USDC | 0 | 0 | 0 | 0 | 40 | 2 | 40 | 4 | 🟣 Direct |

### Binance.US (CCXT: binanceus) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 0 | 2 | 0 | 1 | 20 | 1 | 40 | 4 | 🟣 Direct |
| BTC_USD | 3 | 67 | 7 | 248 | 40 | 2 | 40 | 4 | 🔵 Pro |
| BTC_USDC | 0 | 81 | 0 | 215 | 20 | 2 | 0 | 0 | 🔵 Pro |
| BTC_USDT | 3 | 83 | 2 | 493 | 40 | 2 | 30 | 3 | 🔵 Pro |
| ENA_USDT | 0 | 12 | 0 | 13 | 20 | 1 | 40 | 4 | 🟣 Direct |
| ETH_USD | 0 | 77 | 0 | 447 | 40 | 2 | 40 | 4 | 🔵 Pro |
| ETH_USDC | 0 | 7 | 0 | 5 | 40 | 1 | 0 | 0 | 🟠 REST |
| ETH_USDT | 0 | 79 | 4 | 353 | 40 | 2 | 40 | 4 | 🔵 Pro |
| PENGU_USDT | 0 | 60 | 0 | 97 | 20 | 1 | 40 | 4 | 🔵 Pro |
| POPCAT_USDT | 0 | 1 | 0 | 3 | 20 | 1 | 40 | 4 | 🟣 Direct |
| SOL_USD | 0 | 76 | 1 | 391 | 20 | 1 | 40 | 4 | 🔵 Pro |
| SOL_USDC | 0 | 23 | 0 | 142 | 40 | 2 | 0 | 0 | 🔵 Pro |
| SOL_USDT | 1 | 68 | 1 | 355 | 40 | 2 | 40 | 4 | 🔵 Pro |
| SUI_USD | 0 | 38 | 0 | 112 | 20 | 1 | 40 | 4 | 🔵 Pro |
| SUI_USDT | 1 | 69 | 0 | 139 | 20 | 1 | 40 | 4 | 🔵 Pro |
| WIF_USDT | 0 | 17 | 0 | 48 | 20 | 1 | 40 | 4 | 🔵 Pro |

**Native Errors:** unknown

**Fix Recommendations:** [high] Reduce subscription count per connection

### MEXC (CCXT: mexc) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDC | 100 | 5 | 0 | 0 | 0 | 0 | 60 | 6 | ✅ Native |
| BTC_USDT | 100 | 5 | 0 | 0 | 0 | 0 | 60 | 6 | ✅ Native |
| ENA_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 50 | 5 | 🟣 Direct |
| ENA_USDT | 100 | 5 | 0 | 0 | 0 | 0 | 50 | 5 | ✅ Native |
| ETH_USDC | 80 | 5 | 0 | 0 | 0 | 0 | 60 | 6 | ✅ Native |
| ETH_USDT | 100 | 5 | 0 | 0 | 0 | 0 | 60 | 6 | ✅ Native |
| PENGU_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 50 | 5 | 🟣 Direct |
| PENGU_USDT | 100 | 5 | 0 | 0 | 0 | 0 | 60 | 5 | ✅ Native |
| POPCAT_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 50 | 5 | 🟣 Direct |
| POPCAT_USDT | 100 | 5 | 0 | 0 | 0 | 0 | 50 | 5 | ✅ Native |
| SOL_USDC | 100 | 5 | 0 | 0 | 0 | 0 | 60 | 6 | ✅ Native |
| SOL_USDT | 100 | 5 | 0 | 0 | 0 | 0 | 50 | 6 | ✅ Native |
| SUI_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 50 | 5 | 🟣 Direct |
| SUI_USDT | 100 | 5 | 0 | 0 | 0 | 0 | 50 | 5 | ✅ Native |
| WIF_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 50 | 5 | 🟣 Direct |
| WIF_USDT | 100 | 5 | 0 | 0 | 0 | 0 | 50 | 5 | ✅ Native |

**CCXT Pro Errors:** loadMarkets(1):mexc GET https://contract.mexc.com/api/v1/contract; loadMarkets(2):mexc GET https://contract.mexc.com/api/v1/contract; loadMarkets(3):mexc GET https://contract.mexc.com/api/v1/contract

**CCXT REST Errors:** loadMarkets(1):mexc GET https://contract.mexc.com/api/v1/contract; loadMarkets(2):mexc GET https://contract.mexc.com/api/v1/contract; loadMarkets(3):mexc GET https://contract.mexc.com/api/v1/contract

### CoinEx (CCXT: coinex) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 129 | 365 | 1966 | 203 | 80 | 4 | 50 | 0 | 🔵 Pro |
| BTC_USDC | 107 | 543 | 610 | 193 | 80 | 4 | 50 | 0 | 🔵 Pro |
| BTC_USDT | 233 | 787 | 4331 | 330 | 80 | 4 | 40 | 0 | 🔵 Pro |
| ENA_USDC | 0 | 0 | 506 | 3 | 60 | 3 | 50 | 0 | 🔵 Pro |
| ENA_USDT | 129 | 162 | 1966 | 74 | 60 | 3 | 50 | 0 | 🔵 Pro |
| ETH_USDC | 106 | 621 | 610 | 225 | 80 | 4 | 40 | 0 | 🔵 Pro |
| ETH_USDT | 154 | 807 | 4104 | 323 | 80 | 4 | 50 | 0 | 🔵 Pro |
| PENGU_USDT | 129 | 173 | 2178 | 71 | 80 | 4 | 50 | 0 | 🔵 Pro |
| POPCAT_USDT | 129 | 455 | 2178 | 195 | 80 | 4 | 50 | 0 | 🔵 Pro |
| SOL_USDC | 108 | 279 | 715 | 83 | 80 | 4 | 50 | 0 | 🔵 Pro |
| SOL_USDT | 153 | 593 | 3049 | 238 | 80 | 4 | 50 | 0 | 🔵 Pro |
| SUI_USDC | 0 | 0 | 715 | 110 | 80 | 4 | 50 | 0 | 🔵 Pro |
| SUI_USDT | 155 | 456 | 2434 | 198 | 60 | 4 | 50 | 0 | 🔵 Pro |
| WIF_USDC | 0 | 0 | 921 | 3 | 80 | 4 | 50 | 0 | 🔵 Pro |
| WIF_USDT | 129 | 430 | 1966 | 146 | 80 | 4 | 50 | 0 | 🔵 Pro |

**Fix Recommendations:** [high] Reduce subscription count per connection

### LBank (CCXT: lbank) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 37 | 34 | 0 | 0 | 0 | 0 | 20 | 2 | ✅ Native |
| BTC_USDC | 40 | 62 | 0 | 0 | 0 | 0 | 20 | 2 | ✅ Native |
| BTC_USDT | 192 | 96 | 0 | 0 | 0 | 0 | 20 | 1 | ✅ Native |
| ENA_USDT | 37 | 34 | 0 | 0 | 0 | 0 | 20 | 2 | ✅ Native |
| ETH_USDC | 36 | 64 | 0 | 0 | 0 | 0 | 20 | 2 | ✅ Native |
| ETH_USDT | 99 | 46 | 0 | 0 | 0 | 0 | 20 | 2 | ✅ Native |
| PENGU_USDT | 54 | 40 | 0 | 0 | 0 | 0 | 20 | 2 | ✅ Native |
| POPCAT_USDT | 56 | 60 | 0 | 0 | 0 | 0 | 20 | 2 | ✅ Native |
| SOL_USDC | 14 | 42 | 0 | 0 | 0 | 0 | 10 | 1 | ✅ Native |
| SOL_USDT | 89 | 70 | 0 | 0 | 0 | 0 | 20 | 2 | ✅ Native |
| SUI_USDT | 66 | 38 | 0 | 0 | 0 | 0 | 10 | 2 | ✅ Native |
| WIF_USDT | 45 | 32 | 0 | 0 | 0 | 0 | 20 | 2 | ✅ Native |

**Native Errors:** getaddrinfo EAI_AGAIN www.lbkex.com; certificate has expired

**CCXT Pro Errors:** loadMarkets(1):lbank GET https://lbkperp.lbank.com/cfd/openApi/v1; loadMarkets(2):lbank GET https://lbkperp.lbank.com/cfd/openApi/v1; loadMarkets(3):lbank GET https://lbkperp.lbank.com/cfd/openApi/v1

**CCXT REST Errors:** loadMarkets(1):lbank GET https://api.lbank.info/v2/withdrawConfig; loadMarkets(2):lbank GET https://api.lbank.info/v2/withdrawConfig; loadMarkets(3):lbank GET https://api.lbank.info/v2/withdrawConfig

### BitMart (CCXT: bitmart) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 53 | 160 | 55 | 304 | 60 | 3 | 40 | 0 | 🔵 Pro |
| BTC_USDC | 845 | 243 | 805 | 918 | 80 | 4 | 40 | 0 | 🔵 Pro |
| BTC_USDT | 931 | 287 | 748 | 1745 | 80 | 4 | 50 | 0 | 🔵 Pro |
| ENA_USDT | 108 | 193 | 102 | 416 | 60 | 3 | 30 | 0 | 🔵 Pro |
| ETH_USDC | 73 | 194 | 71 | 529 | 60 | 4 | 40 | 0 | 🔵 Pro |
| ETH_USDT | 739 | 292 | 675 | 1728 | 80 | 4 | 40 | 0 | 🔵 Pro |
| PENGU_USDT | 106 | 196 | 96 | 421 | 60 | 3 | 40 | 0 | 🔵 Pro |
| POPCAT_USDT | 53 | 285 | 51 | 1656 | 60 | 3 | 40 | 0 | 🔵 Pro |
| SOL_USDC | 25 | 217 | 33 | 818 | 60 | 3 | 40 | 0 | 🔵 Pro |
| SOL_USDT | 654 | 249 | 659 | 925 | 80 | 4 | 40 | 0 | 🔵 Pro |
| SUI_USDT | 312 | 212 | 294 | 520 | 60 | 3 | 40 | 0 | 🔵 Pro |
| WIF_USDT | 116 | 173 | 111 | 445 | 60 | 3 | 40 | 0 | 🔵 Pro |

**Fix Recommendations:** [high] Reduce subscription count per connection

### Pionex (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 339 | 281 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ENA_USDT | 30 | 165 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDT | 305 | 279 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| PENGU_USDT | 15 | 175 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SOL_USDT | 300 | 262 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SUI_USDT | 136 | 198 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| WIF_USDT | 10 | 158 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |

**Native Errors:** Opening handshake has timed out

### Poloniex (CCXT: poloniex) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 0 | 16 | 0 | 0 | 160 | 8 | 40 | 4 | 🟠 REST |
| BTC_USDC | 0 | 1078 | 0 | 0 | 160 | 8 | 40 | 4 | ✅ Native |
| BTC_USDT | 222 | 1091 | 0 | 0 | 160 | 8 | 40 | 4 | ✅ Native |
| ENA_USDT | 0 | 9 | 0 | 0 | 140 | 7 | 30 | 4 | 🟠 REST |
| ETH_USDC | 0 | 532 | 0 | 0 | 160 | 8 | 40 | 4 | ✅ Native |
| ETH_USDT | 150 | 894 | 0 | 0 | 160 | 8 | 40 | 4 | ✅ Native |
| POPCAT_USDT | 0 | 10 | 0 | 0 | 160 | 8 | 40 | 4 | 🟠 REST |
| SOL_USDT | 404 | 1066 | 0 | 0 | 160 | 8 | 40 | 4 | ✅ Native |
| SUI_USDT | 160 | 630 | 0 | 0 | 140 | 7 | 40 | 4 | ✅ Native |
| WIF_USDT | 0 | 175 | 0 | 0 | 140 | 7 | 40 | 4 | ≈ Tie |

### HitBTC (CCXT: hitbtc) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 0 | 1108 | 0 | 838 | 28 | 3 | 28 | 5 | ✅ Native |
| BTC_USDC | 5 | 820 | 6 | 611 | 80 | 4 | 50 | 5 | ✅ Native |
| BTC_USDT | 15 | 803 | 23 | 587 | 80 | 4 | 50 | 4 | ✅ Native |
| ENA_USDT | 0 | 324 | 0 | 256 | 80 | 4 | 40 | 4 | ✅ Native |
| ETH_USDC | 13 | 880 | 18 | 656 | 80 | 4 | 50 | 5 | ✅ Native |
| ETH_USDT | 2 | 906 | 3 | 666 | 80 | 4 | 50 | 5 | ✅ Native |
| PENGU_USDT | 0 | 639 | 0 | 455 | 80 | 4 | 50 | 5 | ✅ Native |
| POPCAT_USDT | 0 | 447 | 0 | 302 | 80 | 4 | 50 | 5 | ✅ Native |
| SOL_USDC | 1 | 726 | 1 | 565 | 80 | 4 | 50 | 5 | ✅ Native |
| SOL_USDT | 1 | 758 | 2 | 581 | 80 | 4 | 50 | 5 | ✅ Native |
| SUI_USDT | 0 | 688 | 0 | 530 | 80 | 4 | 50 | 5 | ✅ Native |
| WIF_USDT | 0 | 335 | 0 | 269 | 80 | 4 | 50 | 5 | ✅ Native |

**Fix Recommendations:** [high] Reduce subscription count per connection

### BTSE (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 51 | 27 | 0 | 0 | 0 | 0 | 3 | 2 | ✅ Native |
| BTC_USD | 345 | 128 | 0 | 0 | 0 | 0 | 3 | 3 | ✅ Native |
| BTC_USDC | 71 | 126 | 0 | 0 | 0 | 0 | 3 | 3 | ✅ Native |
| BTC_USDT | 103 | 125 | 0 | 0 | 0 | 0 | 4 | 4 | ✅ Native |
| ENA_USD | 62 | 23 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ENA_USDC | 50 | 23 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ENA_USDT | 50 | 21 | 0 | 0 | 0 | 0 | 3 | 3 | ✅ Native |
| ETH_USD | 282 | 240 | 0 | 0 | 0 | 0 | 3 | 4 | ✅ Native |
| ETH_USDC | 74 | 242 | 0 | 0 | 0 | 0 | 3 | 3 | ✅ Native |
| ETH_USDT | 193 | 247 | 0 | 0 | 0 | 0 | 4 | 3 | ✅ Native |
| PENGU_USD | 53 | 55 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| PENGU_USDC | 52 | 55 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| PENGU_USDT | 59 | 54 | 0 | 0 | 0 | 0 | 3 | 3 | ✅ Native |
| POPCAT_USD | 51 | 17 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| POPCAT_USDC | 51 | 17 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| POPCAT_USDT | 51 | 16 | 0 | 0 | 0 | 0 | 3 | 3 | ✅ Native |
| SOL_USD | 52 | 139 | 0 | 0 | 0 | 0 | 4 | 4 | ✅ Native |
| SOL_USDC | 50 | 156 | 0 | 0 | 0 | 0 | 3 | 3 | ✅ Native |
| SOL_USDT | 50 | 159 | 0 | 0 | 0 | 0 | 3 | 3 | ✅ Native |
| SUI_USD | 147 | 169 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SUI_USDC | 62 | 170 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SUI_USDT | 101 | 169 | 0 | 0 | 0 | 0 | 3 | 3 | ✅ Native |
| WIF_USD | 57 | 35 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| WIF_USDC | 51 | 35 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| WIF_USDT | 61 | 34 | 0 | 0 | 0 | 0 | 3 | 3 | ✅ Native |

**Native Errors:** getaddrinfo EAI_AGAIN ws.btse.com

### Biconomy (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 200 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| BTC_USDC | 200 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| BTC_USDT | 200 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ENA_USDT | 212 | 15 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDC | 200 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDT | 200 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| PENGU_USDT | 200 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| POPCAT_USDT | 200 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SOL_USDC | 200 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SOL_USDT | 200 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SUI_USDT | 200 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| WIF_USDT | 200 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |

**Native Errors:** Opening handshake has timed out

### Hotcoin (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 9 | 54 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| BTC_USDC | 48 | 98 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| BTC_USDT | 649 | 201 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ENA_USDT | 98 | 149 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDC | 61 | 143 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDT | 639 | 219 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| PENGU_USDT | 78 | 100 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| POPCAT_USDT | 6 | 125 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SOL_USDT | 169 | 154 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SUI_USDT | 216 | 158 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| WIF_USDT | 78 | 87 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |

### NovaEx (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDC | 58 | 132 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| BTC_USDT | 0 | 150 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDC | 54 | 135 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDT | 26 | 165 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SOL_USDT | 40 | 153 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |

### Websea (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 177 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | ✅ Native |
| ENA_USDT | 34 | 0 | 0 | 0 | 0 | 0 | 0 | 3 | ✅ Native |
| ETH_USDT | 152 | 0 | 0 | 0 | 0 | 0 | 0 | 2 | ✅ Native |
| PENGU_USDT | 48 | 0 | 0 | 0 | 0 | 0 | 0 | 3 | ✅ Native |
| SOL_USDT | 70 | 0 | 0 | 0 | 0 | 0 | 0 | 3 | ✅ Native |
| SUI_USDT | 51 | 0 | 0 | 0 | 0 | 0 | 0 | 3 | ✅ Native |
| WIF_USDT | 26 | 0 | 0 | 0 | 0 | 0 | 0 | 3 | ✅ Native |

### Bullish (CCXT: bullish) — Health: 86/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USD | 148 | 2 | 255 | 51 | 0 | 0 | 0 | 3 | 🔵 Pro |
| BTC_USDC | 592 | 1 | 726 | 51 | 20 | 1 | 0 | 3 | 🔵 Pro |
| BTC_USDT | 184 | 2 | 197 | 53 | 0 | 0 | 0 | 4 | 🔵 Pro |
| ETH_USD | 128 | 2 | 226 | 42 | 0 | 0 | 0 | 4 | 🔵 Pro |
| ETH_USDC | 439 | 2 | 374 | 46 | 20 | 1 | 0 | 3 | ≈ Tie |
| ETH_USDT | 219 | 2 | 175 | 35 | 0 | 0 | 0 | 4 | ≈ Tie |
| PENGU_USDC | 188 | 2 | 182 | 19 | 0 | 0 | 0 | 4 | 🔵 Pro |
| PENGU_USDT | 181 | 2 | 175 | 27 | 0 | 0 | 0 | 3 | 🔵 Pro |
| SOL_USD | 171 | 2 | 258 | 22 | 0 | 0 | 0 | 4 | 🔵 Pro |
| SOL_USDC | 429 | 2 | 344 | 29 | 0 | 0 | 0 | 4 | ≈ Tie |
| SOL_USDT | 174 | 2 | 244 | 29 | 0 | 0 | 0 | 4 | 🔵 Pro |
| SUI_USDC | 101 | 2 | 200 | 18 | 0 | 0 | 0 | 3 | 🔵 Pro |
| WIF_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ❌ |

**Native Errors:** Opening handshake has timed out; Unexpected server response: 429

**Fix Recommendations:** [high] Reduce subscription count per connection; [high] Increase polling interval / add request batching

### Darkex (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 25 | 48 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDT | 25 | 47 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| POPCAT_USDT | 7 | 43 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SUI_USDT | 3 | 47 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| WIF_USDT | 8 | 34 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |

### Bitrue (CCXT: bitrue) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 20 | 1 | 0 | 2 | 40 | 2 | 20 | 2 | 🟠 REST |
| BTC_USDC | 20 | 1 | 0 | 12 | 20 | 2 | 20 | 2 | 🟣 Direct |
| BTC_USDT | 15 | 1 | 0 | 27 | 40 | 2 | 20 | 2 | 🟠 REST |
| ENA_USDC | 0 | 0 | 0 | 2 | 20 | 1 | 20 | 2 | 🟣 Direct |
| ENA_USDT | 20 | 0 | 0 | 8 | 20 | 1 | 20 | 2 | 🟣 Direct |
| ETH_USDC | 20 | 1 | 0 | 8 | 40 | 2 | 20 | 2 | 🟠 REST |
| ETH_USDT | 20 | 1 | 0 | 15 | 40 | 2 | 30 | 3 | 🟠 REST |
| PENGU_USDC | 0 | 0 | 0 | 11 | 40 | 2 | 20 | 2 | 🟠 REST |
| PENGU_USDT | 20 | 0 | 0 | 4 | 40 | 2 | 20 | 2 | 🟠 REST |
| POPCAT_USDT | 20 | 0 | 0 | 2 | 40 | 2 | 10 | 2 | 🟠 REST |
| SOL_USDC | 15 | 1 | 0 | 4 | 40 | 2 | 20 | 2 | 🟠 REST |
| SOL_USDT | 20 | 1 | 0 | 2 | 40 | 2 | 30 | 2 | 🟠 REST |
| SUI_USDC | 0 | 0 | 0 | 2 | 20 | 1 | 20 | 2 | 🟣 Direct |
| SUI_USDT | 20 | 0 | 0 | 2 | 20 | 1 | 20 | 2 | 🟣 Direct |
| WIF_USDC | 0 | 0 | 0 | 2 | 40 | 2 | 20 | 2 | 🟠 REST |
| WIF_USDT | 20 | 0 | 0 | 2 | 40 | 2 | 20 | 2 | 🟠 REST |

**Native Errors:** Unexpected server response: 502

**CCXT Pro Errors:** PENGU/USDC:tk:notSupported; POPCAT/USDT:tr:notSupported; POPCAT/USDT:tk:notSupported

**Fix Recommendations:** [high] Reduce subscription count per connection; [low] Use REST fallback for unsupported methods

### BloFin (CCXT: blofin) — Health: 95/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USD | 10 | 108 | 20 | 284 | 120 | 6 | 0 | 0 | 🔵 Pro |
| BTC_USDC | 15 | 84 | 34 | 222 | 140 | 7 | 70 | 7 | 🔵 Pro |
| BTC_USDT | 15 | 78 | 44 | 210 | 140 | 7 | 60 | 6 | 🔵 Pro |
| ETH_USD | 15 | 107 | 25 | 285 | 120 | 6 | 0 | 0 | 🔵 Pro |
| ETH_USDT | 11 | 83 | 38 | 218 | 140 | 7 | 70 | 7 | 🔵 Pro |
| SOL_USD | 14 | 102 | 31 | 271 | 120 | 6 | 0 | 0 | 🔵 Pro |
| SOL_USDC | 9 | 84 | 21 | 217 | 140 | 7 | 70 | 7 | 🔵 Pro |
| SOL_USDT | 11 | 83 | 27 | 226 | 140 | 7 | 70 | 7 | 🔵 Pro |

**Fix Recommendations:** [high] Reduce subscription count per connection

### DigiFinex (CCXT: digifinex) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 230 | 103 | 0 | 0 | 120 | 6 | 60 | 6 | ✅ Native |
| BTC_USDC | 318 | 222 | 0 | 0 | 140 | 7 | 60 | 6 | ✅ Native |
| BTC_USDT | 793 | 357 | 0 | 0 | 140 | 7 | 50 | 4 | ✅ Native |
| ETH_USDC | 309 | 180 | 0 | 0 | 120 | 6 | 60 | 6 | ✅ Native |
| ETH_USDT | 549 | 368 | 0 | 0 | 140 | 7 | 60 | 6 | ✅ Native |
| SOL_USDT | 446 | 318 | 0 | 0 | 0 | 0 | 60 | 6 | ✅ Native |

**Native Errors:** Opening handshake has timed out

**CCXT REST Errors:** SOL/USDT:not found

**Fix Recommendations:** [medium] Verify pair availability on exchange

### EXMO (CCXT: exmo) — Health: 85/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_DAI | 1 | 26 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| BTC_USD | 1 | 58 | 28 | 495 | 0 | 0 | 60 | 3 | 🔵 Pro |
| BTC_USDC | 1 | 32 | 10 | 137 | 0 | 0 | 60 | 3 | 🔵 Pro |
| BTC_USDT | 1 | 39 | 91 | 212 | 20 | 1 | 20 | 3 | 🔵 Pro |
| ENA_USDC | 1 | 2 | 28 | 8 | 0 | 0 | 40 | 2 | 🟣 Direct |
| ENA_USDT | 1 | 4 | 15 | 13 | 0 | 0 | 40 | 2 | 🟣 Direct |
| ETH_USD | 0 | 38 | 21 | 366 | 0 | 0 | 60 | 3 | 🔵 Pro |
| ETH_USDC | 1 | 26 | 36 | 113 | 0 | 0 | 60 | 3 | 🔵 Pro |
| ETH_USDT | 2 | 42 | 91 | 263 | 0 | 0 | 40 | 3 | 🔵 Pro |
| PENGU_USDC | 1 | 27 | 28 | 129 | 0 | 0 | 60 | 3 | 🔵 Pro |
| PENGU_USDT | 1 | 21 | 15 | 112 | 0 | 0 | 60 | 3 | 🔵 Pro |
| SOL_USDC | 2 | 25 | 45 | 122 | 0 | 0 | 60 | 3 | 🔵 Pro |
| SOL_USDT | 1 | 19 | 15 | 119 | 0 | 0 | 60 | 3 | 🔵 Pro |
| SUI_USDC | 1 | 28 | 28 | 132 | 0 | 0 | 40 | 2 | 🔵 Pro |
| SUI_USDT | 1 | 23 | 15 | 152 | 0 | 0 | 40 | 2 | 🔵 Pro |
| WIF_USDC | 1 | 11 | 28 | 75 | 0 | 0 | 60 | 3 | 🔵 Pro |
| WIF_USDT | 1 | 9 | 21 | 60 | 0 | 0 | 60 | 3 | 🔵 Pro |

**Native Errors:** Opening handshake has timed out

**Fix Recommendations:** [high] Reduce subscription count per connection

### CEX.IO (CCXT: cex) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USD | 0 | 0 | 0 | 0 | 0 | 2 | 0 | 0 | 🟠 REST |
| BRETT_USDC | 0 | 0 | 0 | 0 | 0 | 2 | 0 | 0 | 🟠 REST |
| BRETT_USDT | 0 | 0 | 0 | 0 | 0 | 2 | 0 | 0 | 🟠 REST |
| BTC_USD | 20 | 1 | 0 | 0 | 0 | 2 | 20 | 1 | ≈ Tie |
| BTC_USDC | 0 | 0 | 0 | 0 | 20 | 2 | 0 | 0 | 🟠 REST |
| BTC_USDT | 20 | 2 | 0 | 0 | 40 | 2 | 0 | 0 | 🟠 REST |
| ENA_USD | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ❌ |
| ENA_USDC | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ❌ |
| ENA_USDT | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | ❌ |
| ETH_USD | 20 | 1 | 0 | 0 | 0 | 2 | 20 | 1 | ≈ Tie |
| ETH_USDC | 0 | 0 | 0 | 0 | 20 | 2 | 0 | 0 | 🟠 REST |
| ETH_USDT | 40 | 2 | 0 | 0 | 20 | 2 | 0 | 0 | ✅ Native |
| PENGU_USD | 0 | 0 | 0 | 0 | 0 | 2 | 0 | 0 | 🟠 REST |
| PENGU_USDT | 0 | 0 | 0 | 0 | 0 | 2 | 0 | 0 | 🟠 REST |
| POPCAT_USD | 0 | 0 | 0 | 0 | 0 | 2 | 0 | 0 | 🟠 REST |
| POPCAT_USDT | 0 | 0 | 0 | 0 | 0 | 2 | 0 | 0 | 🟠 REST |
| SOL_USD | 20 | 1 | 0 | 0 | 0 | 2 | 20 | 1 | ≈ Tie |
| SOL_USDC | 2 | 2 | 0 | 0 | 20 | 2 | 1 | 1 | 🟠 REST |
| SOL_USDT | 0 | 0 | 0 | 0 | 20 | 2 | 0 | 0 | 🟠 REST |
| SUI_USD | 20 | 1 | 0 | 0 | 20 | 1 | 20 | 1 | ≈ Tie |
| SUI_USDC | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 0 | 🟠 REST |
| SUI_USDT | 20 | 1 | 0 | 0 | 0 | 1 | 20 | 1 | ≈ Tie |
| WIF_USD | 20 | 0 | 0 | 0 | 0 | 1 | 20 | 0 | ≈ Tie |
| WIF_USDC | 0 | 0 | 0 | 0 | 20 | 2 | 0 | 0 | 🟠 REST |
| WIF_USDT | 20 | 0 | 0 | 0 | 0 | 2 | 20 | 0 | ≈ Tie |

### OrangeX (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 540 | 9 | 0 | 0 | 0 | 0 | 120 | 9 | ✅ Native |
| ETH_USDT | 540 | 9 | 0 | 0 | 0 | 0 | 180 | 9 | ✅ Native |
| SOL_USDT | 540 | 9 | 0 | 0 | 0 | 0 | 180 | 9 | ✅ Native |

### Azbit (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDC | 15 | 3 | 0 | 0 | 0 | 0 | 60 | 2 | 🟣 Direct |
| BTC_USDT | 10 | 3 | 0 | 0 | 0 | 0 | 40 | 3 | 🟣 Direct |
| ETH_USDC | 15 | 2 | 0 | 0 | 0 | 0 | 40 | 2 | 🟣 Direct |
| ETH_USDT | 15 | 3 | 0 | 0 | 0 | 0 | 60 | 3 | 🟣 Direct |
| PENGU_USDT | 15 | 3 | 0 | 0 | 0 | 0 | 40 | 2 | 🟣 Direct |
| SOL_USDC | 15 | 3 | 0 | 0 | 0 | 0 | 40 | 2 | 🟣 Direct |
| SOL_USDT | 15 | 3 | 0 | 0 | 0 | 0 | 60 | 3 | 🟣 Direct |
| WIF_USDT | 15 | 3 | 0 | 0 | 0 | 0 | 40 | 2 | 🟣 Direct |

### BVOX (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 35 | 10 | 0 | 0 | 0 | 0 | 25 | 7 | ✅ Native |
| ETH_USDT | 50 | 10 | 0 | 0 | 0 | 0 | 40 | 8 | ✅ Native |
| SOL_USDT | 50 | 10 | 0 | 0 | 0 | 0 | 40 | 8 | ✅ Native |

### Trubit Pro (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDC | 30 | 6 | 0 | 0 | 0 | 0 | 40 | 8 | 🟣 Direct |
| BTC_USDT | 25 | 6 | 0 | 0 | 0 | 0 | 30 | 8 | 🟣 Direct |
| ETH_USDC | 25 | 6 | 0 | 0 | 0 | 0 | 40 | 8 | 🟣 Direct |
| ETH_USDT | 35 | 7 | 0 | 0 | 0 | 0 | 40 | 8 | 🟣 Direct |
| SOL_USDT | 30 | 6 | 0 | 0 | 0 | 0 | 40 | 8 | 🟣 Direct |

### BigONE (CCXT: bigone) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 1400 | 8 | 0 | 0 | 60 | 3 | 120 | 7 | ✅ Native |
| ENA_USDT | 1400 | 7 | 0 | 0 | 60 | 3 | 140 | 7 | ✅ Native |
| ETH_USDT | 1600 | 8 | 0 | 0 | 40 | 3 | 140 | 6 | ✅ Native |
| PENGU_USDT | 1400 | 7 | 0 | 0 | 60 | 3 | 140 | 7 | ✅ Native |
| SOL_USDT | 1600 | 6 | 0 | 0 | 60 | 3 | 140 | 7 | ✅ Native |
| SUI_USDT | 1200 | 7 | 0 | 0 | 60 | 3 | 140 | 6 | ✅ Native |
| WIF_USDT | 35 | 7 | 0 | 0 | 15 | 3 | 35 | 7 | ≈ Tie |

### LATOKEN (CCXT: latoken) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 100 | 5 | 0 | 0 | 160 | 8 | 0 | 0 | 🟠 REST |
| BTC_USDT | 0 | 0 | 0 | 0 | 160 | 8 | 0 | 0 | 🟠 REST |
| ENA_USDT | 80 | 4 | 0 | 0 | 140 | 7 | 0 | 0 | 🟠 REST |
| ETH_USDT | 100 | 5 | 0 | 0 | 160 | 8 | 0 | 0 | 🟠 REST |
| PENGU_USDT | 100 | 4 | 0 | 0 | 160 | 8 | 0 | 0 | 🟠 REST |
| POPCAT_USDT | 80 | 4 | 0 | 0 | 140 | 7 | 0 | 0 | 🟠 REST |
| SOL_USDT | 0 | 0 | 0 | 0 | 160 | 8 | 0 | 0 | 🟠 REST |

**CCXT REST Errors:** WIF/USDT:not found; SUI/USDT:not found

**Fix Recommendations:** [medium] Verify pair availability on exchange

### Coinstore (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 409 | 124 | 0 | 0 | 0 | 0 | 20 | 0 | ✅ Native |
| ENA_USDT | 43 | 23 | 0 | 0 | 0 | 0 | 20 | 0 | ✅ Native |
| ETH_USDT | 510 | 124 | 0 | 0 | 0 | 0 | 25 | 0 | ✅ Native |
| PENGU_USDT | 61 | 37 | 0 | 0 | 0 | 0 | 20 | 0 | ✅ Native |
| SOL_USDT | 378 | 122 | 0 | 0 | 0 | 0 | 25 | 0 | ✅ Native |
| SUI_USDT | 238 | 38 | 0 | 0 | 0 | 0 | 20 | 0 | ✅ Native |
| WIF_USDT | 161 | 52 | 0 | 0 | 0 | 0 | 20 | 0 | ✅ Native |

**Native Errors:** read ECONNRESET

### GroveX (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 0 | 32 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| BTC_USDC | 14 | 31 | 0 | 0 | 0 | 0 | 100 | 5 | 🟣 Direct |
| BTC_USDT | 0 | 42 | 0 | 0 | 0 | 0 | 60 | 4 | 🟣 Direct |
| ENA_USDT | 0 | 29 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDC | 24 | 36 | 0 | 0 | 0 | 0 | 100 | 5 | 🟣 Direct |
| ETH_USDT | 0 | 31 | 0 | 0 | 0 | 0 | 100 | 5 | 🟣 Direct |
| PENGU_USDT | 0 | 34 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| POPCAT_USDT | 0 | 36 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SOL_USDC | 13 | 53 | 0 | 0 | 0 | 0 | 80 | 4 | 🟣 Direct |
| SOL_USDT | 1 | 33 | 0 | 0 | 0 | 0 | 100 | 5 | 🟣 Direct |
| SUI_USDT | 0 | 32 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| WIF_USDT | 0 | 37 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |

### CoinW (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BRETT_USDT | 30 | 3 | 0 | 0 | 0 | 0 | 40 | 1 | 🟣 Direct |
| BTC_USDC | 30 | 3 | 0 | 0 | 0 | 0 | 40 | 2 | 🟣 Direct |
| BTC_USDT | 10 | 2 | 0 | 0 | 0 | 0 | 0 | 2 | ✅ Native |
| ENA_USDT | 30 | 3 | 0 | 0 | 0 | 0 | 20 | 1 | ✅ Native |
| ETH_USDC | 30 | 3 | 0 | 0 | 0 | 0 | 40 | 2 | 🟣 Direct |
| ETH_USDT | 30 | 2 | 0 | 0 | 0 | 0 | 20 | 2 | ✅ Native |
| PENGU_USDT | 30 | 3 | 0 | 0 | 0 | 0 | 40 | 2 | 🟣 Direct |
| POPCAT_USDT | 30 | 3 | 0 | 0 | 0 | 0 | 40 | 2 | 🟣 Direct |
| SOL_USDC | 30 | 3 | 0 | 0 | 0 | 0 | 40 | 2 | 🟣 Direct |
| SOL_USDT | 30 | 3 | 0 | 0 | 0 | 0 | 40 | 2 | 🟣 Direct |
| SUI_USDT | 30 | 3 | 0 | 0 | 0 | 0 | 20 | 1 | ✅ Native |
| WIF_USDT | 30 | 3 | 0 | 0 | 0 | 0 | 20 | 2 | ✅ Native |

### Batonex (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 25 | 6 | 0 | 0 | 0 | 0 | 10 | 4 | ✅ Native |
| ETH_USDT | 30 | 6 | 0 | 0 | 0 | 0 | 20 | 4 | ✅ Native |
| SOL_USDT | 30 | 5 | 0 | 0 | 0 | 0 | 20 | 4 | ✅ Native |
| WIF_USDT | 30 | 6 | 0 | 0 | 0 | 0 | 20 | 4 | ✅ Native |

### CEEX (no CCXT) — Health: 100/100

| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |
|------|------|------|--------|--------|---------|---------|------|------|--------|
| BTC_USDT | 100 | 13 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| ETH_USDT | 110 | 12 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SOL_USDT | 110 | 12 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| SUI_USDT | 110 | 12 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |
| WIF_USDT | 110 | 12 | 0 | 0 | 0 | 0 | 0 | 0 | ✅ Native |

## 🔬 Reliability Analysis

| Exchange | Health | Reconn | REST FB | Dedup | Native | Pro | REST | Direct |
|----------|--------|--------|---------|-------|--------|-----|------|--------|
| Binance | 🟢100 | 1 | 124 | 0 | 1,864 | 0 | 530 | 333 |
| Coinbase | 🟢85 | 0 | 0 | 0 | 3,289 | 132,304 | 1,026 | 369 |
| Kraken | 🟢85 | 0 | 0 | 0 | 7,911 | 14,124 | 967 | 623 |
| KuCoin | 🟢85 | 0 | 0 | 0 | 21,120 | 4,040 | 1,806 | 969 |
| OKX | 🟢85 | 0 | 0 | 0 | 4,315 | 6,106 | 973 | 555 |
| Bybit | 🟢85 | 0 | 0 | 0 | 10,645 | 11,320 | 726 | 543 |
| Bitfinex | 🟢100 | 2 | 0 | 0 | 19,851 | 0 | 195 | 412 |
| Gate.io | 🟢85 | 1 | 51 | 0 | 595 | 121 | 882 | 327 |
| HTX | 🟢100 | 2 | 0 | 0 | 1,806 | 0 | 924 | 124 |
| WOO X | 🟢85 | 1 | 0 | 0 | 983 | 2,060 | 2,037 | 461 |
| Crypto.com | 🟢86 | 1 | 0 | 0 | 5,823 | 7,668 | 1,232 | 394 |
| Bitstamp | 🟢85 | 3 | 0 | 0 | 125 | 39 | 241 | 29 |
| WhiteBIT | 🟢100 | 0 | 0 | 0 | 2,798 | 0 | 1,232 | 331 |
| AscendEX | 🟢100 | 0 | 0 | 0 | 4,159 | 4,023 | 1,694 | 63 |
| BingX | 🟢100 | 0 | 0 | 0 | 5,630 | 2,680 | 2,136 | 980 |
| Toobit | 🟢100 | 0 | 0 | 0 | 1,173 | 1,296 | 858 | 0 |
| Deepcoin | 🟢100 | 0 | 0 | 0 | 53 | 0 | 924 | 0 |
| XT.com | 🟢85 | 0 | 9 | 0 | 1,515 | 3,953 | 968 | 613 |
| Zoomex | 🟢100 | 0 | 0 | 0 | 23,778 | 0 | 0 | 0 |
| Bitget | 🟢85 | 0 | 0 | 0 | 1,826 | 10,218 | 772 | 329 |
| Gemini | 🟢100 | 3 | 0 | 0 | 0 | 0 | 859 | 572 |
| Binance.US | 🟢85 | 3 | 0 | 0 | 768 | 4,402 | 507 | 561 |
| MEXC | 🟢100 | 0 | 0 | 0 | 1,135 | 0 | 0 | 946 |
| CoinEx | 🟢85 | 0 | 0 | 0 | 7,332 | 30,676 | 1,256 | 730 |
| LBank | 🟢100 | 4 | 71 | 0 | 1,383 | 0 | 0 | 242 |
| BitMart | 🟢85 | 0 | 0 | 0 | 6,716 | 16,559 | 882 | 480 |
| Pionex | 🟢100 | 1 | 0 | 0 | 2,653 | 0 | 0 | 0 |
| Poloniex | 🟢100 | 1 | 0 | 0 | 6,437 | 0 | 1,694 | 430 |
| HitBTC | 🟢85 | 0 | 0 | 0 | 8,471 | 7,444 | 1,003 | 626 |
| BTSE | 🟢100 | 1 | 0 | 0 | 4,711 | 0 | 0 | 95 |
| Biconomy | 🟢100 | 2 | 0 | 0 | 2,449 | 0 | 0 | 0 |
| Hotcoin | 🟢100 | 1 | 0 | 0 | 3,539 | 0 | 0 | 0 |
| NovaEx | 🟢100 | 0 | 0 | 0 | 913 | 0 | 0 | 0 |
| FameEX | 🟢93 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |
| Websea | 🟢100 | 0 | 0 | 0 | 558 | 0 | 0 | 18 |
| Bullish | 🟢86 | 4 | 0 | 0 | 2,977 | 5,580 | 44 | 43 |
| Darkex | 🟢100 | 1 | 0 | 0 | 287 | 0 | 0 | 0 |
| Bitrue | 🟢85 | 3 | 0 | 0 | 237 | 105 | 596 | 363 |
| BloFin | 🟢95 | 0 | 0 | 0 | 829 | 4,543 | 1,166 | 374 |
| DigiFinex | 🟢100 | 2 | 0 | 0 | 4,193 | 0 | 726 | 384 |
| EXMO | 🟢85 | 2 | 0 | 0 | 448 | 4,124 | 22 | 864 |
| CEX.IO | 🟢100 | 0 | 0 | 0 | 213 | 0 | 263 | 147 |
| OrangeX | 🟢100 | 0 | 0 | 0 | 1,647 | 0 | 0 | 507 |
| Azbit | 🟢100 | 0 | 0 | 0 | 138 | 0 | 0 | 399 |
| BVOX | 🟢100 | 0 | 0 | 0 | 165 | 0 | 0 | 128 |
| Trubit Pro | 🟢100 | 0 | 0 | 0 | 176 | 0 | 0 | 230 |
| BigONE | 🟢100 | 0 | 0 | 0 | 8,685 | 0 | 397 | 902 |
| LATOKEN | 🟢100 | 0 | 0 | 0 | 482 | 0 | 1,188 | 0 |
| Coinstore | 🟢100 | 1 | 0 | 0 | 2,320 | 0 | 0 | 150 |
| GroveX | 🟢100 | 1 | 2 | 0 | 478 | 0 | 0 | 568 |
| CoinW | 🟢100 | 0 | 0 | 0 | 374 | 0 | 0 | 381 |
| Batonex | 🟢100 | 0 | 0 | 0 | 138 | 0 | 0 | 86 |
| CEEX | 🟢100 | 3 | 118 | 300 | 601 | 0 | 0 | 0 |

## 🔧 Recommendations

| Exchange | Method | Reason | Health Fix |
|----------|--------|--------|------------|
| Binance | Native | Highest throughput (+252%) | - |
| Coinbase | CCXT Pro | Best push streaming (+3923%) | Reduce subscription count per connection |
| Kraken | CCXT Pro | Best push streaming (+79%) | Reduce subscription count per connection |
| KuCoin | Native | Highest throughput (+423%) | Reduce subscription count per connection |
| OKX | CCXT Pro | Best push streaming (+42%) | Reduce subscription count per connection |
| Bybit | Hybrid | Similar throughput | Reduce subscription count per connection |
| Bitfinex | Native | Highest throughput (+4718%) | Reduce subscription count per connection |
| Gate.io | CCXT REST | Most reliable polling (+48%) | Reduce subscription count per connection |
| HTX | Native | Highest throughput (+95%) | - |
| WOO X | Hybrid | Similar throughput | Reduce subscription count per connection |
| Crypto.com | CCXT Pro | Best push streaming (+32%) | Reduce subscription count per connection |
| Bitstamp | CCXT REST | Most reliable polling (+93%) | Reduce subscription count per connection |
| WhiteBIT | Native | Highest throughput (+127%) | - |
| AscendEX | Hybrid | Similar throughput | Use REST fallback for unsupported methods |
| BingX | Native | Highest throughput (+110%) | - |
| Toobit | Hybrid | Similar throughput | - |
| Deepcoin | CCXT REST | Most reliable polling (+1643%) | - |
| XT.com | CCXT Pro | Best push streaming (+161%) | Reduce subscription count per connection |
| Zoomex | Native | Only method available | - |
| Bitget | CCXT Pro | Best push streaming (+460%) | Reduce subscription count per connection |
| Gemini | CCXT REST | Most reliable polling (+50%) | - |
| Binance.US | CCXT Pro | Best push streaming (+473%) | Reduce subscription count per connection |
| MEXC | Hybrid | Similar throughput | - |
| CoinEx | CCXT Pro | Best push streaming (+318%) | Reduce subscription count per connection |
| LBank | Native | Highest throughput (+471%) | - |
| BitMart | CCXT Pro | Best push streaming (+147%) | Reduce subscription count per connection |
| Pionex | Native | Only method available | - |
| Poloniex | Native | Highest throughput (+280%) | - |
| HitBTC | Hybrid | Similar throughput | Reduce subscription count per connection |
| BTSE | Native | Only method available | - |
| Biconomy | Native | Only method available | - |
| Hotcoin | Native | Only method available | - |
| NovaEx | Native | Only method available | - |
| FameEX | Investigate | Underperforming | Reduce subscription count per connection |
| Websea | Native | Only method available | - |
| Bullish | CCXT Pro | Best push streaming (+87%) | Reduce subscription count per connection |
| Darkex | Native | Only method available | - |
| Bitrue | CCXT REST | Most reliable polling (+64%) | Reduce subscription count per connection |
| BloFin | CCXT Pro | Best push streaming (+290%) | Reduce subscription count per connection |
| DigiFinex | Native | Highest throughput (+478%) | Verify pair availability on exchange |
| EXMO | CCXT Pro | Best push streaming (+377%) | Reduce subscription count per connection |
| CEX.IO | Hybrid | Similar throughput | - |
| OrangeX | Native | Only method available | - |
| Azbit | Native | Only method available | - |
| BVOX | Native | Only method available | - |
| Trubit Pro | Native | Only method available | - |
| BigONE | Native | Highest throughput (+863%) | - |
| LATOKEN | CCXT REST | Most reliable polling (+146%) | Verify pair availability on exchange |
| Coinstore | Native | Only method available | - |
| GroveX | Native | Only method available | - |
| CoinW | Native | Only method available | - |
| Batonex | Native | Only method available | - |
| CEEX | Native | Only method available | - |

## Conclusion (v9.3, 4-Method + Subscription Manager, 5min test)

4-method parallel comparison across 53 exchanges with Subscription Manager:

- **Native Wins:** 10 | **CCXT Pro Wins:** 12 | **CCXT REST Wins:** 6 | **Direct REST Wins:** 0 | **Ties:** 7
- **Native-Only:** 17 exchanges (no CCXT support)
- **All Failed:** 1 exchanges
- **Health:** Avg 95/100 | 0 critical | 0 warnings
- **Data Quality:** 300 dupes dropped, 1446 OB validated, 375 REST fallbacks

### Aggregate Throughput

| Method | Total Messages | Rate (msg/min) |
|--------|---------------|----------------|
| Native | 190,712 | 35,536 |
| CCXT Pro | 273,385 | 50,941 |
| CCXT REST | 30,726 | 5,725 |
| Direct REST | 17,681 | 3,295 |
| **Combined** | **512,504** | **95,498** |
| **Hybrid (deduped)** | **246,389** | **45,911** |

**Hybrid Dedup:** 142,033 cross-method duplicates removed (28% of raw)

**Verdict:** CCXT Pro-first architecture recommended. Native handles 27/53, CCXT Pro handles 12/53, CCXT REST handles 6/53, Direct REST handles 0/53 exchanges optimally.
---

## 🏭 Production Run Statistics — 4.59-Hour Run (2026-02-28 08:53–13:29 UTC)

> **Note:** `node compare-v7-enhanced.js 1440` ran for 4.59 hours and crashed (Exit Code 1).
> Data below is from DuckDB — covers CCXT Pro / CCXT REST / Direct REST methods.
> Native-WS-only exchanges (Zoomex, Pionex, Biconomy, Hotcoin, NovaEx, FameEX, Websea, Darkex, CEEX)
> process data in memory and are NOT stored to the trades table (they were active in the hybrid engine).

### Sustained Throughput (DuckDB-stored, CCXT methods only)

| Metric | Value |
|--------|-------|
| Total trade records | **9,575,101** |
| Orderbook records | **1,778,944** |
| Run duration | **4.59 hours** (crashed at ~4.6h) |
| Sustained trade rate | **~34,750 msgs/min** |
| Sustained OB rate | **~6,460 OB updates/min** |
| Exchanges with DB data | **44/53** |
| Native-only (in-memory, no DB) | **9/53** |
| Crash status | **Exit Code 1** — root cause under investigation |

### Per-Exchange Sustained Rate (trades/hour from DuckDB)

| Exchange | Trades/Hour | OB Records | Coins | Dominant Source |
|----------|------------|-----------|-------|----------------|
| CoinEx | 1,353,709 | 101,700 | 9/9 | CCXT Pro |
| EXMO | 189,271 | 92,822 | 7/9 | CCXT Pro |
| BitMart | 100,367 | 267,049 | 9/9 | CCXT Pro |
| Coinbase | 54,594 | 20,648 | 8/9 | CCXT Pro |
| Crypto.com | 49,860 | 40,682 | 8/9 | CCXT Pro |
| Bullish | 46,027 | 6,955 | 5/9 | CCXT Pro |
| Bitget | 29,711 | 82,641 | 8/9 | CCXT Pro |
| WOO X | 28,751 | 35,550 | 9/9 | CCXT REST |
| AscendEX | 17,852 | 111,781 | 8/9 | CCXT REST |
| BingX | 15,564 | 17,370 | 9/9 | CCXT REST |
| HitBTC | 12,697 | 301,183 | 9/9 | CCXT REST |
| KuCoin | 12,677 | 160,654 | 9/9 | CCXT REST |
| Poloniex | 12,034 | 15,864 | 8/9 | CCXT REST (skipPro ✅) |
| OKX | 11,399 | 12,466 | 7/9 | CCXT REST |
| BloFin | 11,139 | 12,622 | 3/9 | CCXT REST |
| Kraken | 10,686 | 293,646 | 8/9 | CCXT REST |
| Gate.io | 10,359 | 2,592 | 9/9 | CCXT REST |
| Binance | 9,902 | 13,665 | 7/9 | CCXT REST (skipPro ✅) |
| Toobit | 9,542 | 4,836 | 8/9 | CCXT REST |
| Gemini | 9,146 | 3,067 | 6/9 | CCXT REST (skipPro ✅) |
| BigONE | 8,801 | 3,008 | 7/9 | CCXT REST |
| Binance.US | 8,289 | 102,487 | 9/9 | CCXT REST |
| Bybit | 7,939 | 24,690 | 9/9 | CCXT REST |
| WhiteBIT | 7,837 | 1,948 | 7/9 | CCXT REST (skipPro ✅) |
| Bitrue | 7,277 | 5,223 | 9/9 | CCXT REST |
| LATOKEN | 7,224 | 1,849 | 7/9 | CCXT REST |
| DigiFinex | 6,636 | 2,035 | 4/9 | CCXT REST |
| MEXC | 6,525 | 3,041 | 8/9 | Direct REST |
| XT.com | 5,605 | 19,752 | 9/9 | Direct REST |
| Bitfinex | 3,452 | 2,314 | 5/9 | Direct REST (skipPro ✅) |
| CEX.IO | 2,897 | 1,275 | 9/9 | CCXT REST |
| HTX | 2,726 | 1,823 | 9/9 | CCXT REST (skipPro ✅) |
| OrangeX | 2,268 | 1,078 | 3/9 | Direct REST |
| CoinW | 2,057 | 989 | 9/9 | Direct REST |
| LBank | 1,843 | 872 | 9/9 | Direct REST |
| Deepcoin | 1,386 | 326 | 5/9 | CCXT REST |
| Azbit | 1,346 | 630 | 5/9 | Direct REST |
| Bitstamp | 1,334 | 1,463 | 8/9 | CCXT Pro |
| Trubit Pro | 1,330 | 1,281 | 3/9 | Direct REST |
| GroveX | 1,214 | 595 | 9/9 | Direct REST |
| Coinstore | 949 | 0 | 7/9 | Direct REST |
| BVOX | 905 | 893 | 3/9 | Direct REST |
| Batonex | 668 | 646 | 4/9 | Direct REST |
| BTSE | 286 | 1,284 | 9/9 | Direct REST (DNS intermittent ⚠️) |

### Native-WS-Only Exchanges (Active in Memory, Not in DuckDB trades table)

| Exchange | OB in DB | Notes |
|----------|---------|-------|
| Zoomex | 0 | Native-only; 23K+ msgs/5min in 5-min test; no CCXT DB storage |
| Pionex | 0 | Native-only; 2,653 msgs/5min confirmed |
| Biconomy | 0 | Native-only; 2,449 msgs/5min confirmed |
| Hotcoin | 0 | Native-only; 3,539 msgs/5min confirmed |
| NovaEx | 0 | Native-only; 913 msgs/5min confirmed |
| FameEX | 0 | DNS intermittent (Windows); 0 data this run |
| Websea | 1,649 | Native WS active; OB stored via Direct REST fallback |
| Darkex | 0 | Native-only; 287 msgs/5min confirmed |
| CEEX | 0 | Native-only; 601 msgs/5min confirmed |

### ⚠️ Production Run Crash — Investigation Required

The 1440-minute run exited with **Exit Code 1** after ~4.59 hours. Immediate action needed:

1. **Capture crash output:** Run `node compare-v7-enhanced.js 1440 2>&1 | Tee-Object -FilePath crash-log.txt`
2. **Check for memory leaks:** Node.js process may have hit heap limit after ~4-5 hours with 53 WS connections
3. **Add crash recovery:** Implement `--max-old-space-size=4096` flag or hourly restart loop
4. **Suspect areas:** CCXT Pro WS reconnect loops, DuckDB flush accumulation, subscription manager growth

**Recommended immediate fix:** `node --max-old-space-size=4096 compare-v7-enhanced.js 1440`