# 🔧 DETAILED EXCHANGE FIX REPORT

**Test Date:** 2026-02-05
**Test Timeout:** 20 seconds per stream
**Total Exchanges Tested:** 19

---

## 📊 Summary

| Category | Count |
|----------|-------|
| ✅ Fully Working | 11 |
| 🟡 Partially Working | 2 |
| ❌ Failed | 6 |
| **Total** | **19** |

---

## 📋 Detailed Results

### ✅ BingX

| Stream | Status | Time/Error |
|--------|--------|------------|
| spot_ticker | ✅ | 1110ms |
| spot_kline | ✅ | 769ms |
| futures_ticker | ✅ | 1039ms |
| futures_kline | ✅ | 704ms |

**Sample Data:**
```json
{"code":0,"id":"1","msg":"SUCCESS","timestamp":1770317867112}...
```

---

### ✅ MEXC

**Note:** Spot uses REST API only

| Stream | Status | Time/Error |
|--------|--------|------------|
| futures_ticker | ✅ | 2323ms |
| futures_depth | ✅ | 819ms |
| futures_kline | ✅ | 1088ms |
| futures_trades | ✅ | 977ms |

**Sample Data:**
```json
{"channel":"rs.sub.ticker","data":"success","ts":1770317871751}...
```

---

### ✅ Coinbase

**Note:** Spot only, no klines via WS

| Stream | Status | Time/Error |
|--------|--------|------------|
| spot_ticker | ✅ | 1431ms |
| spot_orderbook | ✅ | 5366ms |
| spot_trades | ✅ | 1820ms |

**Sample Data:**
```json
{"type":"subscriptions","channels":[{"name":"heartbeat","product_ids":["BTC-USD"],"account_ids":null},{"name":"ticker","product_ids":["BTC-USD"],"acco...
```

---

### ✅ Bitstamp

**Note:** No klines via WS

| Stream | Status | Time/Error |
|--------|--------|------------|
| spot_trades | ✅ | 2150ms |
| spot_orderbook | ✅ | 8802ms |
| spot_orderbook_full | ✅ | 13679ms |
| spot_orderbook_diff | ✅ | 10866ms |
| futures_trades | ✅ | 3846ms |

**Sample Data:**
```json
{"event":"bts:subscription_succeeded","channel":"live_trades_btcusd","data":{}}...
```

---

### ✅ BitMart

| Stream | Status | Time/Error |
|--------|--------|------------|
| spot_ticker | ✅ | 4622ms |
| spot_trades | ✅ | 14351ms |
| spot_orderbook | ✅ | 4745ms |
| spot_kline | ✅ | 3507ms |

**Sample Data:**
```json
{"topic":"spot/ticker:BTC_USDT","event":"subscribe"}...
```

---

### ✅ KuCoin

| Stream | Status | Time/Error |
|--------|--------|------------|
| spot_ticker | ✅ | 6761ms |
| spot_trades | ✅ | 2343ms |
| spot_orderbook | ✅ | 1824ms |
| futures_ticker | ✅ | 2741ms |

**Sample Data:**
```json
{"id":"18EI1dAJR4q","type":"welcome"}...
```

---

### ✅ Upbit

**Note:** KRW pairs only

| Stream | Status | Time/Error |
|--------|--------|------------|
| spot_ticker | ✅ | 6416ms |
| spot_trades | ✅ | 4558ms |
| spot_orderbook | ✅ | 2929ms |

**Sample Data:**
```json
{"type":"ticker","code":"KRW-BTC","opening_price":107867000.00000000,"high_price":108262000.00000000,"low_price":96815000.00000000,"trade_price":98044...
```

---

### 🟡 Toobit

| Stream | Status | Time/Error |
|--------|--------|------------|
| spot_ticker | ✅ | 1948ms |
| spot_trades | ✅ | 2430ms |
| spot_depth | ✅ | 3263ms |
| futures_ticker | ❌ | Unexpected server response: 404 |

**Sample Data:**
```json
{"symbol":"BTCUSDT","symbolName":"BTCUSDT","topic":"realtimes","params":{"realtimeInterval":"24h","binary":"false"},"data":[{"t":1770317987795,"s":"BT...
```

---

### ✅ Pionex

**Note:** No klines/tickers via WS

| Stream | Status | Time/Error |
|--------|--------|------------|
| spot_orderbook | ✅ | 3656ms |
| spot_trades | ✅ | 3772ms |
| spot_orderbook_v2 | ✅ | 4238ms |

**Sample Data:**
```json
{"type":"ERROR","code":"PARAMETER_ERROR","message":"invalid `limit`","timestamp":1770318002123}...
```

---

### ✅ Deribit

**Note:** Derivatives only (futures/options)

| Stream | Status | Time/Error |
|--------|--------|------------|
| futures_ticker | ✅ | 1870ms |
| futures_trades | ✅ | 2453ms |
| futures_orderbook | ✅ | 2070ms |

**Sample Data:**
```json
{"jsonrpc":"2.0","id":1,"error":{"code":13778,"message":"raw_subscriptions_not_available_for_unauthorized"},"usIn":1770318012061386,"usOut":1770318012...
```

---

### ❌ Phemex

**Note:** May require VPN for some regions

| Stream | Status | Time/Error |
|--------|--------|------------|
| futures_ticker | ❌ | Unexpected server response: 410 |
| futures_ticker_v2 | ❌ | Unexpected server response: 403 |
| testnet_ticker | ❌ | Unexpected server response: 200 |

---

### ❌ Tapbit

**Note:** DNS may be region-specific

| Stream | Status | Time/Error |
|--------|--------|------------|
| v5_ticker | ❌ | getaddrinfo ENOTFOUND stream.tapbit.com |
| ws_ticker | ❌ | getaddrinfo ENOTFOUND ws.tapbit.com |
| main_ticker | ❌ | Unexpected server response: 301 |

---

### 🟡 WOO X

**Note:** May be geo-blocked

| Stream | Status | Time/Error |
|--------|--------|------------|
| public_ticker | ✅ | 3571ms |
| v2_ticker | ❌ | Unexpected server response: 404 |
| woox_io | ✅ | 2323ms |

**Sample Data:**
```json
{"success":false,"ts":1770318034417,"errorMsg":"event type is empty"}...
```

---

### ❌ Websea

| Stream | Status | Time/Error |
|--------|--------|------------|
| oapi | ❌ | Unexpected server response: 200 |
| ws | ❌ | Unexpected server response: 426 |
| main | ❌ | Unexpected server response: 404 |

---

### ❌ Deepcoin

| Stream | Status | Time/Error |
|--------|--------|------------|
| spot_trades | ❌ | Unexpected server response: 400 |
| futures_trades | ❌ | Unexpected server response: 400 |
| v2_trades | ❌ | Unexpected server response: 404 |

---

### ❌ BTCC

| Stream | Status | Time/Error |
|--------|--------|------------|
| okx_style | ❌ | getaddrinfo ENOTFOUND ws.btcc.com |
| binance_style | ❌ | Timeout |
| direct | ❌ | Timeout |

---

### ❌ Azbit

**Note:** Uses SignalR protocol

| Stream | Status | Time/Error |
|--------|--------|------------|
| jsonrpc | ❌ | Unexpected server response: 404 |
| bitstamp_style | ❌ | Unexpected server response: 404 |

---

### ✅ Poloniex

| Stream | Status | Time/Error |
|--------|--------|------------|
| public_ticker | ✅ | 2623ms |
| public_trades | ✅ | 2636ms |
| public_orderbook | ✅ | 2296ms |

**Sample Data:**
```json
{"event":"subscribe","channel":"ticker","symbols":["BTC_USDT"]}...
```

---

### ✅ Lbank

| Stream | Status | Time/Error |
|--------|--------|------------|
| ticker | ✅ | 2898ms |
| depth | ✅ | 1990ms |
| trades | ✅ | 2120ms |
| kline | ✅ | 2686ms |

**Sample Data:**
```json
{"SERVER":"V2","tick":{"to_cny":6.93,"high":74145.37,"vol":34587.3584,"low":65200.0,"change":-10.46,"usd":66040.47,"to_usd":1.0,"dir":"sell","turnover...
```

---

