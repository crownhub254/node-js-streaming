# 📊 Corrected Exchange WebSocket Test Report

**Test Date:** 2026-02-05  
**Test Scope:** 17 exchanges with corrected endpoints based on detailed API research  
**Tested Pairs:** BTC/USDT (Spot), BTCUSDT/BTC-PERP (Futures)  
**Test Timeout:** 15 seconds per stream  

---

## 🎯 Executive Summary

| Metric | Count | Percentage |
|--------|-------|------------|
| **Total Exchanges Tested** | 17 | — |
| **✅ Fully Working** | 7 | 41% |
| **🟡 Partially Working** | 2 | 12% |
| **❌ Failed** | 7 | 41% |
| **⚪ Skipped (No API)** | 1 | 6% |
| **Success Rate** | 9/16 | **56.3%** |

### Improvements from Previous Test
| Exchange | Previous Status | Current Status | Change |
|----------|-----------------|----------------|--------|
| **AscendEX** | 🟡 Partial (kline timeout) | ✅ Fully Working (6/6) | ✅ FIXED |
| **WhiteBIT** | 🟡 Partial (trades timeout) | ✅ Fully Working (6/6) | ✅ FIXED |
| **BTSE** | 🟡 Partial (orderbook timeout) | ✅ Fully Working (4/4) | ✅ FIXED |
| **CoinW** | ❌ DNS failure | ✅ Fully Working (4/4) | ✅ FIXED |
| **Pionex** | ❌ 403 Forbidden | 🟡 Partial (2/3) | ✅ IMPROVED |

---

## ✅ FULLY WORKING EXCHANGES (7)

### 1. Bitunix ⭐ Futures Only
- **Endpoint:** `wss://fapi.bitunix.com/public/`
- **Markets:** Futures only (Spot requires REST API)
- **Docs:** https://docs.bitunix.com/

| Stream | Status | Response Time |
|--------|--------|---------------|
| Orderbook | ✅ | 3033ms |
| Trades | ✅ | 1808ms |
| Ticker | ✅ | 2269ms |
| Kline | ✅ | 2000ms |

**Subscription Format:**
```json
{"op":"subscribe","args":[{"ch":"depth_book1","instId":"BTCUSDT"}]}
{"op":"subscribe","args":[{"ch":"trade","instId":"BTCUSDT"}]}
{"op":"subscribe","args":[{"ch":"ticker","instId":"BTCUSDT"}]}
{"op":"subscribe","args":[{"ch":"kline_1m","instId":"BTCUSDT"}]}
```

---

### 2. BloFin ⭐ Futures Only (OKX-style)
- **Endpoint:** `wss://openapi.blofin.com/ws/public`
- **Markets:** Futures only
- **Docs:** https://docs.blofin.com/

| Stream | Status | Response Time |
|--------|--------|---------------|
| Orderbook | ✅ | 3835ms |
| Trades | ✅ | 2096ms |
| Ticker | ✅ | 3110ms |
| Kline | ✅ | 2780ms |
| Funding Rate | ✅ | 2600ms |

**Subscription Format (OKX-compatible):**
```json
{"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]}
{"op":"subscribe","args":[{"channel":"trades","instId":"BTC-USDT"}]}
{"op":"subscribe","args":[{"channel":"tickers","instId":"BTC-USDT"}]}
{"op":"subscribe","args":[{"channel":"candle1m","instId":"BTC-USDT"}]}
{"op":"subscribe","args":[{"channel":"funding-rate","instId":"BTC-USDT"}]}
```

---

### 3. AsterDEX ⭐ Futures Only (Binance-compatible)
- **Endpoint:** `wss://fstream.asterdex.com/ws`
- **Markets:** Futures only
- **Docs:** https://docs.asterdex.com/

| Stream | Status | Response Time |
|--------|--------|---------------|
| Orderbook (depth5) | ✅ | 5346ms |
| Trades | ✅ | 3822ms |
| Ticker | ✅ | 3659ms |
| Kline | ✅ | 6315ms |
| AggTrade | ✅ | 5396ms |

**Subscription Format (Binance-compatible):**
```json
{"method":"SUBSCRIBE","params":["btcusdt@depth5@100ms"],"id":1}
{"method":"SUBSCRIBE","params":["btcusdt@trade"],"id":2}
{"method":"SUBSCRIBE","params":["btcusdt@ticker"],"id":3}
{"method":"SUBSCRIBE","params":["btcusdt@kline_1m"],"id":4}
{"method":"SUBSCRIBE","params":["btcusdt@aggTrade"],"id":5}
```

---

### 4. AscendEX ⭐ Spot + Futures (FIXED!)
- **Spot Endpoint:** `wss://ascendex.com/1/api/pro/v1/stream`
- **Futures Endpoint:** `wss://ascendex.com/1/api/pro/v2/stream`
- **Docs:** https://ascendex.github.io/ascendex-pro-api/

| Stream | Spot | Futures |
|--------|------|---------|
| Orderbook | ✅ 1646ms | ✅ 1795ms |
| Trades | ✅ 2048ms | — |
| Ticker | ✅ 2896ms | ✅ 1610ms |
| Kline | ✅ 2271ms | — |

**Previous Issue:** Kline timeout on spot
**Fix Applied:** Corrected subscription channel format

**Subscription Format:**
```json
// Spot
{"op":"sub","ch":"depth:BTC/USDT"}
{"op":"sub","ch":"trades:BTC/USDT"}
{"op":"sub","ch":"bbo:BTC/USDT"}
{"op":"sub","ch":"bar:1:BTC/USDT"}
// Futures
{"op":"sub","ch":"futures-depth:BTC-PERP"}
{"op":"sub","ch":"futures-bbo:BTC-PERP"}
```

---

### 5. WhiteBIT ⭐ Spot + Futures (FIXED!)
- **Endpoint:** `wss://api.whitebit.com/ws`
- **Docs:** https://docs.whitebit.com/

| Stream | Spot | Futures |
|--------|------|---------|
| Orderbook | ✅ 2972ms | ✅ 796ms |
| Trades | ✅ 1495ms | — |
| Ticker | ✅ 2551ms | ✅ 1339ms |
| Kline | ✅ 858ms | — |

**Previous Issue:** Trades timeout
**Fix Applied:** Correct `trades_subscribe` method with proper params

**Subscription Format (JSON-RPC):**
```json
{"id":1,"method":"depth_subscribe","params":["BTC_USDT",20,"0",true]}
{"id":2,"method":"trades_subscribe","params":["BTC_USDT"]}
{"id":3,"method":"market_subscribe","params":["BTC_USDT"]}
{"id":4,"method":"candles_subscribe","params":["BTC_USDT",60]}
// Futures: Use BTC_PERP instead of BTC_USDT
```

---

### 6. BTSE ⭐ Spot + Futures (FIXED!)
- **Spot Endpoint:** `wss://ws.btse.com/ws/spot`
- **Futures Endpoint:** `wss://ws.btse.com/ws/futures`
- **Docs:** https://www.btse.com/docs/futures/en/

| Stream | Spot | Futures |
|--------|------|---------|
| Orderbook | ✅ 3172ms | ✅ 2408ms |
| Trades | ✅ 3763ms | ✅ 3221ms |

**Previous Issue:** Orderbook timeout
**Fix Applied:** Use `orderBookApi` channel with `_0` suffix for snapshots

**Subscription Format:**
```json
// Spot
{"op":"subscribe","args":["orderBookApi:BTC-USDT_0"]}
{"op":"subscribe","args":["tradeHistoryApi:BTC-USDT"]}
// Futures
{"op":"subscribe","args":["orderBookApi:BTCPFC_0"]}
{"op":"subscribe","args":["tradeHistoryApi:BTCPFC"]}
```

---

### 7. CoinW ⭐ Spot + Futures (FIXED!)
- **Spot Endpoint:** `wss://ws.futurescw.com`
- **Futures Endpoint:** `wss://ws.futurescw.com/perpum`
- **Docs:** https://www.coinw.com/api

| Stream | Spot | Futures |
|--------|------|---------|
| Orderbook | ✅ 1822ms | ✅ 3553ms |
| Ticker | ✅ 2483ms | ✅ 2353ms |

**Previous Issue:** DNS failure on `ws.coinw.com`
**Fix Applied:** Use alternative domain `ws.futurescw.com`

**Subscription Format:**
```json
{"event":"subscribe","args":"spot/depth:BTCUSDT"}
{"event":"subscribe","args":"spot/ticker:BTCUSDT"}
{"event":"subscribe","args":"perp/depth:BTCUSDT"}
{"event":"subscribe","args":"perp/ticker:BTCUSDT"}
```

---

## 🟡 PARTIALLY WORKING EXCHANGES (2)

### 1. Toobit
- **Spot Endpoint:** `wss://stream.toobit.com/quote/ws/v1`
- **Futures Endpoint:** `wss://stream.toobit.com/quote/ws/v2` (404 error)

| Stream | Spot | Futures |
|--------|------|---------|
| Orderbook | ✅ 1920ms | ❌ 404 |
| Trades | ✅ 1782ms | ❌ 404 |
| Ticker | ✅ 1811ms | ❌ 404 |

**Issue:** Futures WebSocket endpoint returns 404
**Recommendation:** Use spot endpoint for now; futures may require different path or IP pinning

---

### 2. Pionex
- **Endpoint:** `wss://ws.pionex.com/wsPub`

| Stream | Spot | Futures |
|--------|------|---------|
| Orderbook | ✅ 13851ms | — |
| Ticker | ❌ Timeout | ✅ 12321ms |

**Previous Issue:** 403 Forbidden
**Partial Fix:** Changed subscription format; slow but working
**Note:** Very slow response times (~12-14 seconds)

---

## ❌ FAILED EXCHANGES (7)

### 1. Tapbit
- **Attempted:** `wss://stream.tapbit.com/v5/public/spot`
- **Error:** `getaddrinfo ENOTFOUND stream.tapbit.com`
- **Analysis:** DNS resolution failure - domain may be region-restricted or down
- **Recommendation:** Try VPN or alternative DNS resolvers

### 2. Websea
- **Attempted:** `wss://oapi.websea.com`
- **Error:** HTTP 200 (not upgrading to WebSocket)
- **Analysis:** Endpoint may require different protocol or headers
- **Recommendation:** Review documentation for correct WS framing

### 3. Phemex
- **Attempted:** `wss://vapi.phemex.com/ws`
- **Error:** HTTP 403 Forbidden
- **Analysis:** Requires authentication or geo-restricted
- **Recommendation:** Sign requests with API key using HMAC-SHA256

### 5. BTCC
- **Attempted:** `wss://api.btcc.com/ws/futures`
- **Error:** Connection timeout
- **Analysis:** Server not responding
- **Recommendation:** Implement 30-second ping; may require OKX-like auth

### 6. Azbit
- **Attempted:** `wss://ws.azbit.com`
- **Error:** HTTP 404
- **Analysis:** SignalR protocol - requires specialized client
- **Recommendation:** Use SignalR library instead of raw WebSocket

### 7. Deepcoin
- **Attempted:** `wss://stream.deepcoin.com/streamlet/trade/public/spot?v2`
- **Error:** HTTP 400 Bad Request
- **Analysis:** Endpoint format may have changed
- **Recommendation:** Check for updated API documentation

---

## ⚪ SKIPPED (1)

### Ourbit
- **Reason:** No public API documentation found
- **Note:** Tutorials only for manual trading; API may be private

---

## 📈 Stream Availability Matrix

| Exchange | Spot OB | Spot Trade | Spot Tick | Spot Kline | Fut OB | Fut Trade | Fut Tick | Fut Kline |
|----------|---------|------------|-----------|------------|--------|-----------|----------|-----------|
| **Bitunix** | — | — | — | — | ✅ | ✅ | ✅ | ✅ |
| **BloFin** | — | — | — | — | ✅ | ✅ | ✅ | ✅ |
| **AsterDEX** | — | — | — | — | ✅ | ✅ | ✅ | ✅ |
| **AscendEX** | ✅ | ✅ | ✅ | ✅ | ✅ | — | ✅ | — |
| **WhiteBIT** | ✅ | ✅ | ✅ | ✅ | ✅ | — | ✅ | — |
| **BTSE** | ✅ | ✅ | — | — | ✅ | ✅ | — | — |
| **CoinW** | ✅ | — | ✅ | — | ✅ | — | ✅ | — |
| **Toobit** | ✅ | ✅ | ✅ | — | ❌ | ❌ | ❌ | — |
| **Pionex** | ✅ | — | ❌ | — | — | — | ✅ | — |

---

## 🔧 Working Endpoint Reference

### Tier 1 - Full Spot + Futures Coverage
```javascript
const TIER1_EXCHANGES = {
  ascendex: {
    spot: 'wss://ascendex.com/1/api/pro/v1/stream',
    futures: 'wss://ascendex.com/1/api/pro/v2/stream'
  },
  whitebit: {
    both: 'wss://api.whitebit.com/ws'
  },
  btse: {
    spot: 'wss://ws.btse.com/ws/spot',
    futures: 'wss://ws.btse.com/ws/futures'
  },
  coinw: {
    spot: 'wss://ws.futurescw.com',
    futures: 'wss://ws.futurescw.com/perpum'
  }
};
```

### Tier 2 - Futures Only
```javascript
const TIER2_FUTURES_ONLY = {
  bitunix: 'wss://fapi.bitunix.com/public/',
  blofin: 'wss://openapi.blofin.com/ws/public',
  asterdex: 'wss://fstream.asterdex.com/ws'
};
```

---

## 📝 Key Recommendations

### For Production Use:
1. **AscendEX** - Best for Spot + Futures, all streams working
2. **WhiteBIT** - Good alternative, fast responses
3. **Bitunix** - Excellent for futures-only trading
4. **BloFin** - OKX-compatible, includes funding rate

### Protocol Notes:
- **BloFin/Bitunix** - OKX-style `{"op":"subscribe","args":[...]}`
- **AsterDEX** - Binance-compatible `{"method":"SUBSCRIBE","params":[...]}`
- **WhiteBIT** - JSON-RPC style `{"method":"xxx_subscribe","params":[...]}`
- **BTSE** - Custom `{"op":"subscribe","args":["channel:symbol"]}`

### Ping/Pong Requirements:
- **AscendEX:** 30 second ping interval
- **WhiteBIT:** 50 second ping interval
- **Toobit:** 20 second ping interval
- **BloFin:** 25 second ping (send "ping" string)

---

## 📂 Files Generated

| File | Description |
|------|-------------|
| `corrected-exchange-tester.js` | Test script with updated endpoints |
| `corrected-exchange-results.json` | Raw JSON test results |
| `CORRECTED-EXCHANGE-REPORT.md` | This detailed report |

---

*Report generated by corrected-exchange-tester.js*  
*Based on detailed API research provided by user*
