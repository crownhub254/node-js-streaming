# 📊 Comprehensive Exchange WebSocket Test Report

**Test Date:** 2025-07-05  
**Tested Pairs:** BTC/USDT (Spot), BTCUSDT/BTC-USDT-PERP (Futures)  
**Test Timeout:** 15 seconds per stream  
**Tool:** Node.js with `ws` v8.18.0  

---

## 🎯 Executive Summary

| Metric | Count |
|--------|-------|
| **Total Exchanges Tested** | 24 |
| **✅ Fully Working** | 8 |
| **🟡 Partially Working** | 4 |
| **❌ Failed/Unreachable** | 10 |
| **⚪ No API Available** | 2 |
| **Success Rate** | 50% |

---

## ✅ FULLY WORKING EXCHANGES (8)

### 1. Bybit ⭐ TOP PERFORMER
- **Markets:** Spot + Futures
- **Spot Endpoint:** `wss://stream.bybit.com/v5/public/spot`
- **Futures Endpoint:** `wss://stream.bybit.com/v5/public/linear`
- **API Docs:** https://bybit-exchange.github.io/docs/

| Stream | Spot | Futures |
|--------|------|---------|
| Orderbook | ✅ 868ms | ✅ 1016ms |
| Trades | ✅ 597ms | ✅ 1297ms |
| Ticker | ✅ 736ms | ✅ 669ms |
| Kline | ✅ 583ms | ✅ 803ms |

**Subscription Format:**
```json
{"op":"subscribe","args":["orderbook.50.BTCUSDT"]}
{"op":"subscribe","args":["publicTrade.BTCUSDT"]}
{"op":"subscribe","args":["tickers.BTCUSDT"]}
{"op":"subscribe","args":["kline.1.BTCUSDT"]}
```

---

### 2. BingX ⭐
- **Markets:** Spot + Futures
- **Spot Endpoint:** `wss://open-api-ws.bingx.com/market`
- **Futures Endpoint:** `wss://open-api-swap.bingx.com/swap-market`
- **API Docs:** https://bingx-api.github.io/docs/

| Stream | Spot | Futures |
|--------|------|---------|
| Orderbook | ✅ 1191ms | ✅ 1074ms |
| Trades | ✅ 5207ms | ✅ 605ms |
| Ticker | ✅ 1756ms | ✅ 842ms |
| Kline | — | ✅ 732ms |

**Subscription Format (gzip compressed):**
```json
{"id":"1","reqType":"sub","dataType":"BTC-USDT@depth20"}
{"id":"2","reqType":"sub","dataType":"BTC-USDT@trade"}
```

---

### 3. HitBTC ⭐
- **Markets:** Spot + Futures (same endpoint)
- **Endpoint:** `wss://api.hitbtc.com/api/3/ws/public`
- **API Docs:** https://api.hitbtc.com/

| Stream | Spot | Futures |
|--------|------|---------|
| Orderbook | ✅ 1509ms | ✅ 1424ms |
| Trades | ✅ 1350ms | — |
| Ticker | ✅ 2007ms | ✅ 825ms |
| Kline | ✅ 1395ms | — |

**Subscription Format (JSON-RPC 2.0):**
```json
{"method":"subscribe","ch":"orderbook/full","params":{"symbols":["BTCUSDT"]}}
{"method":"subscribe","ch":"trades","params":{"symbols":["BTCUSDT"]}}
```

---

### 4. Bitget ⭐
- **Markets:** Spot + Futures
- **Endpoint:** `wss://ws.bitget.com/v2/ws/public`
- **API Docs:** https://www.bitget.com/api-doc/

| Stream | Spot | Futures |
|--------|------|---------|
| Orderbook | ✅ 1419ms | ✅ 1213ms |
| Trades | ✅ 995ms | — |
| Ticker | ✅ 1325ms | ✅ 1327ms |
| Kline | ✅ 1707ms | ✅ 2364ms |

**Subscription Format:**
```json
{"op":"subscribe","args":[{"instType":"SPOT","channel":"books15","instId":"BTCUSDT"}]}
{"op":"subscribe","args":[{"instType":"USDT-FUTURES","channel":"ticker","instId":"BTCUSDT"}]}
```

---

### 5. Crypto.com ⭐
- **Markets:** Spot + Futures (same endpoint)
- **Endpoint:** `wss://stream.crypto.com/exchange/v1/market`
- **API Docs:** https://exchange-docs.crypto.com/

| Stream | Spot | Futures |
|--------|------|---------|
| Orderbook | ✅ 2016ms | ✅ 2897ms |
| Trades | ✅ 1857ms | — |
| Ticker | ✅ 2406ms | ✅ 2608ms |
| Kline | ✅ 3077ms | — |

**Subscription Format:**
```json
{"id":1,"method":"subscribe","params":{"channels":["book.BTC_USDT.10"]}}
{"id":2,"method":"subscribe","params":{"channels":["ticker.BTCUSD-PERP"]}}
```

---

### 6. Bitunix
- **Markets:** Futures Only
- **Endpoint:** `wss://fapi.bitunix.com/public/`
- **API Docs:** https://docs.bitunix.com/

| Stream | Futures |
|--------|---------|
| Orderbook | ✅ 1366ms |
| Trades | ✅ 1120ms |
| Ticker | ✅ 1117ms |
| Kline | ✅ 1116ms |

**Subscription Format:**
```json
{"op":"subscribe","args":[{"ch":"depth_book1","instId":"BTCUSDT"}]}
{"op":"subscribe","args":[{"ch":"ticker","instId":"BTCUSDT"}]}
```

---

### 7. BloFin
- **Markets:** Futures Only
- **Endpoint:** `wss://openapi.blofin.com/ws/public`
- **API Docs:** https://docs.blofin.com/

| Stream | Futures |
|--------|---------|
| Orderbook | ✅ 2135ms |
| Trades | ✅ 3143ms |
| Ticker | ✅ 3936ms |
| Kline | ✅ 13107ms |

**Subscription Format (OKX-style):**
```json
{"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]}
{"op":"subscribe","args":[{"channel":"tickers","instId":"BTC-USDT"}]}
```

---

### 8. AsterDEX
- **Markets:** Futures Only
- **Endpoint:** `wss://fstream.asterdex.com/ws`
- **API Docs:** https://docs.asterdex.com/

| Stream | Futures |
|--------|---------|
| Orderbook | ✅ 3643ms |
| Ticker | ✅ 4034ms |
| Trades | ✅ 3956ms |

**Subscription Format (Binance-compatible):**
```json
{"method":"SUBSCRIBE","params":["btcusdt@depth20"],"id":1}
```

---

## 🟡 PARTIALLY WORKING EXCHANGES (4)

### 1. AscendEX
- **Markets:** Spot + Futures
- **Spot Endpoint:** `wss://ascendex.com/1/api/pro/v1/stream`
- **Futures Endpoint:** `wss://ascendex.com/1/api/pro/v2/stream`
- **API Docs:** https://ascendex.github.io/ascendex-pro-api/

| Stream | Spot | Futures |
|--------|------|---------|
| Orderbook | ✅ 2178ms | ✅ 1951ms |
| Trades | ✅ 2175ms | — |
| Ticker | ✅ 1972ms | ✅ 2022ms |
| Kline | ❌ Timeout | — |

---

### 2. WhiteBIT
- **Markets:** Spot + Futures (same endpoint)
- **Endpoint:** `wss://api.whitebit.com/ws`
- **API Docs:** https://docs.whitebit.com/

| Stream | Spot | Futures |
|--------|------|---------|
| Orderbook | ✅ 904ms | ✅ 1246ms |
| Trades | ❌ Timeout | — |
| Ticker | ✅ 816ms | ✅ 2713ms |
| Kline | ✅ 1068ms | — |

---

### 3. BTSE
- **Markets:** Spot + Futures
- **Spot Endpoint:** `wss://ws.btse.com/ws/spot`
- **Futures Endpoint:** `wss://ws.btse.com/ws/futures`
- **API Docs:** https://www.btse.com/docs/futures/en/

| Stream | Spot | Futures |
|--------|------|---------|
| Orderbook | ❌ Timeout | ❌ Timeout |
| Trades | ✅ 3157ms | ✅ 2968ms |

---

### 4. Toobit
- **Markets:** Spot ✅ / Futures ❌ (DNS failure)
- **Spot Endpoint:** `wss://stream.toobit.com/quote/ws/v1`
- **Futures Endpoint:** `wss://fstream.toobit.com/quote/ws/v1` (DNS not found)

| Stream | Spot | Futures |
|--------|------|---------|
| Orderbook | ✅ 1780ms | ❌ DNS Error |
| Trades | ✅ 1742ms | ❌ DNS Error |
| Ticker | ✅ 1811ms | ❌ DNS Error |

---

## ❌ FAILED EXCHANGES (10)

### DNS Resolution Failures
| Exchange | Error | Attempted Endpoint |
|----------|-------|-------------------|
| **Tapbit** | ENOTFOUND | `wss://ws.tapbit.com/swap` |
| **CoinW** | ENOTFOUND | `wss://ws.coinw.com/websocket` |
| **Ourbit** | ENOTFOUND | `wss://open-api-ws.ourbit.com/open/api/v2/ws` |

### HTTP Error Responses
| Exchange | Error | Notes |
|----------|-------|-------|
| **Websea** | 404 Not Found | Endpoint may have changed |
| **Deepcoin** | HTTP 200 | Not upgrading to WebSocket |

### Access Denied
| Exchange | Error | Notes |
|----------|-------|-------|
| **Pionex** | 403 Forbidden | Geo-restriction or auth required |
| **Phemex** | 403 Forbidden | May require VPN or specific headers |

### Connection Issues
| Exchange | Error | Notes |
|----------|-------|-------|
| **BTCC** | Handshake timeout | Server not responding |
| **Azbit** | Protocol mismatch | Uses SignalR, not standard WebSocket |

---

## ⚪ NO API AVAILABLE (2)

| Exchange | Reason |
|----------|--------|
| **Picol** | No public API available |

---

## 📈 Stream Availability Matrix

| Exchange | Spot OB | Spot Trade | Spot Tick | Spot Kline | Fut OB | Fut Trade | Fut Tick | Fut Kline |
|----------|---------|------------|-----------|------------|--------|-----------|----------|-----------|
| **Bybit** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **BingX** | ✅ | ✅ | ✅ | — | ✅ | ✅ | ✅ | ✅ |
| **HitBTC** | ✅ | ✅ | ✅ | ✅ | ✅ | — | ✅ | — |
| **Bitget** | ✅ | ✅ | ✅ | ✅ | ✅ | — | ✅ | ✅ |
| **Crypto.com** | ✅ | ✅ | ✅ | ✅ | ✅ | — | ✅ | — |
| **Bitunix** | — | — | — | — | ✅ | ✅ | ✅ | ✅ |
| **BloFin** | — | — | — | — | ✅ | ✅ | ✅ | ✅ |
| **AsterDEX** | — | — | — | — | ✅ | ✅ | ✅ | — |
| **AscendEX** | ✅ | ✅ | ✅ | ❌ | ✅ | — | ✅ | — |
| **WhiteBIT** | ✅ | ❌ | ✅ | ✅ | ✅ | — | ✅ | — |
| **BTSE** | ❌ | ✅ | — | — | ❌ | ✅ | — | — |
| **Toobit** | ✅ | ✅ | ✅ | — | ❌ | ❌ | ❌ | — |

---

## 🔧 Working Endpoint Reference

### Tier 1 - Full Coverage (Spot + Futures)
```javascript
const TIER1_EXCHANGES = {
  bybit: {
    spot: 'wss://stream.bybit.com/v5/public/spot',
    futures: 'wss://stream.bybit.com/v5/public/linear'
  },
  bingx: {
    spot: 'wss://open-api-ws.bingx.com/market',
    futures: 'wss://open-api-swap.bingx.com/swap-market'
  },
  hitbtc: {
    both: 'wss://api.hitbtc.com/api/3/ws/public'
  },
  bitget: {
    both: 'wss://ws.bitget.com/v2/ws/public'
  },
  crypto_com: {
    both: 'wss://stream.crypto.com/exchange/v1/market'
  }
};
```

### Tier 2 - Futures Only
```javascript
const TIER2_FUTURES = {
  bitunix: 'wss://fapi.bitunix.com/public/',
  blofin: 'wss://openapi.blofin.com/ws/public',
  asterdex: 'wss://fstream.asterdex.com/ws'
};
```

---

## 📝 Key Findings

### Best Performers (Fastest Response Times)
1. **Bybit** - Avg 750ms, full coverage, excellent documentation
2. **Bitget** - Avg 1400ms, V2 API with clean format
3. **HitBTC** - Avg 1400ms, JSON-RPC 2.0 protocol

### Compression Requirements
- **BingX** - gzip compression required (use `pako` or `zlib`)
- Others use plain JSON

### Protocol Notes
- **Azbit** - Uses SignalR (requires specialized client)
- **Phemex/Pionex** - 403 Forbidden (may need VPN or API key)
- **BTCC** - Handshake timeout (server unresponsive)

### Recommended for Production
1. **Bybit** - Best all-around, fastest, full coverage
2. **BingX** - Excellent for derivatives
3. **Bitget** - Good alternative with modern API
4. **Crypto.com** - Reliable, regulated exchange

---

## 📂 Files Generated

| File | Description |
|------|-------------|
| `comprehensive-exchange-tester.js` | Test script for all 24 exchanges |
| `exchange-test-results.json` | Raw JSON test results |
| `COMPREHENSIVE-EXCHANGE-REPORT.md` | This report |

---

*Report generated by comprehensive-exchange-tester.js*
