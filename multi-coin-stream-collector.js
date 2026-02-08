/**
 * ╔══════════════════════════════════════════════════════════════════════════════╗
 * ║  MULTI-COIN STREAM COLLECTOR — 15 Exchanges × 3 Coins (BTC/ETH/SOL)        ║
 * ║  Orderbook + Trades via WebSocket & REST → DuckDB                           ║
 * ║  Verified: 88/90 streams (97.8%) — February 8, 2026                         ║
 * ╚══════════════════════════════════════════════════════════════════════════════╝
 *
 *  EXCHANGES:
 *   1. Biconomy (WS)         6. Bullish (WS+REST)      11. Websea (WS+REST)
 *   2. NovaEx/WOO (WS+REST)  7. Darkex (WS+REST gzip)  12. Azbit (REST)
 *   3. XT.com (WS)           8. Bitrue (WS+REST gzip)   13. BloFin (WS+REST)
 *   4. Hotcoin (WS+REST)     9. FameEX (WS+REST)        14. BVOX (REST)
 *   5. Zoomex (WS)          10. OrangeX (REST)           15. Trubit Pro (REST)
 *
 *  USAGE:
 *    node multi-coin-stream-collector.js
 *
 *  DEPENDENCIES:
 *    npm install ws @duckdb/node-api
 *
 *  OUTPUT:
 *    streaming.duckdb → tables: stream_test_trades, stream_test_orderbook, stream_test_results
 */

const WebSocket = require('ws');
const https = require('https');
const http = require('http');
const zlib = require('zlib');
const { DuckDBInstance } = require('@duckdb/node-api');

// ═══════════════════════════════════════════════════════════════════════════════
// CONFIGURATION
// ═══════════════════════════════════════════════════════════════════════════════

const TIMEOUT = 30000;  // 30s per WS stream (BloFin ETH/SOL trades need ~10s gaps)
const COINS = ['BTC', 'ETH', 'SOL'];

// ═══════════════════════════════════════════════════════════════════════════════
// UTILITIES
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Decompress gzip/deflate/inflate binary WebSocket messages
 * Used by: Hotcoin, Darkex, Bitrue
 */
function decompress(data) {
  if (!Buffer.isBuffer(data)) return data.toString();
  try { return zlib.gunzipSync(data).toString('utf8'); } catch (e) {}
  try { return zlib.inflateSync(data).toString('utf8'); } catch (e) {}
  try { return zlib.inflateRawSync(data).toString('utf8'); } catch (e) {}
  return data.toString('utf8');
}

/**
 * HTTP GET with timeout and JSON parsing
 * Used for REST API endpoints with automatic retry
 */
function httpGet(url, timeout = 15000) {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve({ ok: false, error: 'Timeout', url }), timeout);
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, { headers: { 'User-Agent': 'Mozilla/5.0', Accept: 'application/json' } }, (res) => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => {
        clearTimeout(timer);
        if (res.statusCode >= 200 && res.statusCode < 300) {
          try {
            const j = JSON.parse(body);
            const hasData = body.length > 5 && body !== '{}' && body !== '[]';
            resolve({ ok: hasData, status: res.statusCode, parsed: j, raw: body, url, empty: !hasData });
          } catch (e) { resolve({ ok: false, error: 'Invalid JSON', raw: body.substring(0, 200), url }); }
        } else { resolve({ ok: false, error: `HTTP ${res.statusCode}`, raw: body.substring(0, 150), url }); }
      });
    });
    req.on('error', e => { clearTimeout(timer); resolve({ ok: false, error: e.message, url }); });
    req.end();
  });
}

/**
 * WebSocket stream test with universal ping/pong handling
 * Supports all exchange-specific message formats
 *
 * Global ping/pong patterns handled:
 *  - {ping: <number>}        → {pong: <number>}        (Darkex)
 *  - {ping: "ping"}          → {pong: "pong"}          (Hotcoin)
 *  - {event: "ping"}         → {event: "pong"}         (Bitrue)
 *  - Subscription acks, system messages, etc → filtered out
 */
function testWS(url, opts = {}) {
  return new Promise((resolve) => {
    const { onOpen, onMsg, doDecompress, binaryUtf8 } = opts;
    let done = false, ws, timer, msgCount = 0, samples = [];

    const finish = (result) => {
      if (done) return; done = true;
      clearTimeout(timer);
      try { ws.close(); } catch (e) {}
      resolve(result);
    };

    timer = setTimeout(() => finish({ ok: false, error: 'Timeout', msgs: msgCount, samples }), TIMEOUT);

    try { ws = new WebSocket(url, { handshakeTimeout: 10000 }); }
    catch (e) { return finish({ ok: false, error: e.message, samples }); }

    ws.on('error', e => finish({ ok: false, error: e.message, samples }));
    ws.on('close', (code) => { if (!done) finish({ ok: false, error: `Closed: ${code}`, msgs: msgCount, samples }); });

    ws.on('open', () => { if (onOpen) onOpen(ws); });

    ws.on('message', (raw) => {
      let msg;
      if (binaryUtf8 && Buffer.isBuffer(raw)) msg = raw.toString('utf8');
      else if (doDecompress) msg = decompress(raw);
      else msg = typeof raw === 'string' ? raw : raw.toString();

      try {
        const p = JSON.parse(msg);

        // ── Universal Ping/Pong Handling ──
        if (p.ping) { ws.send(JSON.stringify({ pong: p.ping })); return; }                    // Darkex/Hotcoin numeric
        if (p.ping === 'ping') { ws.send(JSON.stringify({ pong: 'pong' })); return; }         // Hotcoin string
        if (p.event === 'ping') { ws.send(JSON.stringify({ event: 'pong' })); return; }       // Bitrue
        if (p.result === 'pong' || p.pong || p.ret_msg === 'pong' || p.op === 'pong') return;  // Pong responses
        if (p.event === 'pong') return;

        // ── Subscription Ack Filters ──
        if (p.event === 'subscribe' && (p.success === true || p.arg)) return;     // BloFin, Zoomex
        if (p.success === true && p.ret_msg === 'subscribe') return;              // Zoomex
        if (p.event_rep !== undefined && !p.data && !p.tick) return;              // FameEX
        if (p.status === 'ok' && p.subbed && !p.data && !p.tick) return;          // Hotcoin sub ack
        if (p.status === 'ok' && p.code === 200 && !p.data && !p.tick) return;    // Hotcoin empty ack
        if (p.channel === 'system') return;                                        // FameEX system
        if (p.error === null && p.result && p.result.status === 'success') return; // Biconomy sub ack
        if (p.state === true && p.message === 'On connect') return;               // Biconomy connect
        if (p.errMsg) return;                                                      // Error messages
        if (p.status === 'error') return;                                          // Bitrue error
        if (p.jsonrpc && p.result && p.result.responseCode) return;               // Bullish ack
        if (p.id && p.code === 0 && p.method === 'subscribe') return;             // XT.com sub ack

        const result = onMsg ? onMsg(p, msg) : null;
        if (result === 'skip') return;

        msgCount++;
        samples.push(p);
        if (msgCount >= 2) finish({ ok: true, msgs: msgCount, samples });
      } catch (e) {
        msgCount++;
        if (msgCount >= 2) finish({ ok: true, msgs: msgCount, samples });
      }
    });
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// EXCHANGE DEFINITIONS
// Each exchange config contains:
//   - method: 'ws' | 'rest' | 'ws+rest'
//   - symbols: { BTC: '...', ETH: '...', SOL: '...' }
//   - streams(sym): { orderbook: {...}, trades: {...} }   (WS configs)
//   - restEndpoints(sym): { orderbook: 'url', trades: 'url' }  (REST configs)
// ═══════════════════════════════════════════════════════════════════════════════

const EXCHANGES = {

  // ── 1. Biconomy (WS) ──────────────────────────────────────────────────────
  // Symbol: BTC_USDT  |  Ping: server.ping  |  Data: {method, params}
  Biconomy: {
    method: 'ws',
    symbols: { BTC: 'BTC_USDT', ETH: 'ETH_USDT', SOL: 'SOL_USDT' },
    streams: (sym) => ({
      orderbook: {
        url: 'wss://bei.biconomy.com/ws', decompress: false,
        sub: { method: 'depth.subscribe', params: [sym, 50, '0.01'], id: 1 },
        ping: { method: 'server.ping', params: [], id: 5160 },
        onMsg: (p) => (p.method && p.params) ? 'data' : 'skip'
      },
      trades: {
        url: 'wss://bei.biconomy.com/ws', decompress: false,
        sub: { method: 'deals.subscribe', params: [sym], id: 2 },
        ping: { method: 'server.ping', params: [], id: 5160 },
        onMsg: (p) => (p.method && p.params) ? 'data' : 'skip'
      }
    })
  },

  // ── 2. NovaEx / WOO X (WS+REST) ──────────────────────────────────────────
  // WS for orderbook, REST for trades (WOO trades extremely sparse via WS)
  // Symbol: SPOT_BTC_USDT  |  REST: api.woo.org
  NovaEx: {
    method: 'ws+rest',
    symbols: { BTC: 'SPOT_BTC_USDT', ETH: 'SPOT_ETH_USDT', SOL: 'SPOT_SOL_USDT' },
    streams: (sym) => ({
      orderbook: {
        url: 'wss://wss.woox.io/ws/stream/OqdphuyIYbng-t001',
        sub: { id: 'sub1', topic: `${sym}@orderbook`, event: 'subscribe' },
        onMsg: (p) => (p.topic && p.data) ? 'data' : 'skip'
      },
      trades: null // WOO trades are extremely sparse via WS (0 in 40s), use REST
    }),
    restEndpoints: (sym) => ({
      orderbook: `https://api.woo.org/v1/public/orderbook/${sym}`,
      trades: `https://api.woo.org/v1/public/market_trades?symbol=${sym}&limit=5`
    })
  },

  // ── 3. XT.com (WS) ────────────────────────────────────────────────────────
  // Symbol: btc_usdt (lowercase)  |  id field required in subscribe
  // Trade data: {topic:"trade", data:{s,p,q,b}}  |  OB: {topic:"depth_update", data:{...}}
  'XT.com': {
    method: 'ws',
    symbols: { BTC: 'btc_usdt', ETH: 'eth_usdt', SOL: 'sol_usdt' },
    streams: (sym) => ({
      orderbook: {
        url: 'wss://stream.xt.com/public',
        sub: { method: 'subscribe', params: [`depth_update@${sym}`], id: '1' },
        onMsg: (p) => (p.topic === 'depth_update' && p.data) ? 'data' : 'skip'
      },
      trades: {
        url: 'wss://stream.xt.com/public',
        sub: { method: 'subscribe', params: [`trade@${sym}`], id: '2' },
        onMsg: (p) => (p.topic === 'trade' && p.data) ? 'data' : 'skip'
      }
    })
  },

  // ── 4. Hotcoin (WS+REST gzip) ────────────────────────────────────────────
  // CRITICAL: Ping is {"ping":"ping"} STRING, NOT numeric timestamp!
  // Depth channel: market.{sym}.trade.depth  NOT  market.{sym}.depth.step0
  // Data format: {code:200, data:{bids,asks}}  NOT  {tick:{buys,asks}}
  Hotcoin: {
    method: 'ws+rest',
    symbols: { BTC: 'btc_usdt', ETH: 'eth_usdt', SOL: 'sol_usdt' },
    streams: (sym) => ({
      orderbook: {
        url: 'wss://wss.hotcoinfin.com/trade/multiple', decompress: true,
        sub: { sub: `market.${sym}.trade.depth` },
        onMsg: (p) => {
          if (p.code === 200 && p.data && (p.data.bids || p.data.asks)) return 'data';
          if (p.tick && (p.tick.buys || p.tick.asks)) return 'data';
          return 'skip';
        }
      },
      trades: {
        url: 'wss://wss.hotcoinfin.com/trade/multiple', decompress: true,
        sub: { sub: `market.${sym}.trade.detail` },
        onMsg: (p) => {
          if (p.code === 200 && p.data && Array.isArray(p.data) && p.data.length > 0) return 'data';
          if (p.tick && p.tick.data) return 'data';
          return 'skip';
        }
      }
    }),
    restEndpoints: (sym) => ({
      orderbook: `https://api.hotcoinfin.com/v1/depth?symbol=${sym}`,
      trades: `https://api.hotcoinfin.com/v1/trade?symbol=${sym}&count=5`
    })
  },

  // ── 5. Zoomex (WS Bybit V5 fork) ─────────────────────────────────────────
  // Symbol: BTCUSDT  |  Ping: {"op":"ping"}  |  Data: {topic, data}
  Zoomex: {
    method: 'ws',
    symbols: { BTC: 'BTCUSDT', ETH: 'ETHUSDT', SOL: 'SOLUSDT' },
    streams: (sym) => ({
      orderbook: {
        url: 'wss://stream.zoomex.com/v5/public/spot',
        sub: { op: 'subscribe', args: [`orderbook.50.${sym}`] },
        ping: { op: 'ping' },
        onMsg: (p) => (p.topic && p.data) ? 'data' : 'skip'
      },
      trades: {
        url: 'wss://stream.zoomex.com/v5/public/spot',
        sub: { op: 'subscribe', args: [`publicTrade.${sym}`] },
        ping: { op: 'ping' },
        onMsg: (p) => (p.topic && p.data) ? 'data' : 'skip'
      }
    })
  },

  // ── 6. Bullish (WS+REST JSON-RPC 2.0) ────────────────────────────────────
  // CRITICAL: Orderbook and trades use SEPARATE WS endpoints!
  //   /market-data/orderbook → l2Orderbook topic only
  //   /market-data/trades    → anonymousTrades topic only
  // OB data is FLAT ARRAY: [price, qty, price, qty, ...] — parse in pairs
  // Symbol: BTCUSDC (WS), BTCUSDT (REST)
  Bullish: {
    method: 'ws+rest',
    symbols: { BTC: 'BTCUSDC', ETH: 'ETHUSDC', SOL: 'SOLUSDC' },
    restSymbols: { BTC: 'BTCUSDT', ETH: 'ETHUSDT', SOL: 'SOLUSDT' },
    streams: (sym, restSym) => ({
      orderbook: {
        url: 'wss://api.exchange.bullish.com/trading-api/v1/market-data/orderbook',
        sub: { jsonrpc: '2.0', type: 'command', method: 'subscribe', params: { topic: 'l2Orderbook', symbol: sym }, id: '1' },
        onMsg: (p) => (p.type === 'snapshot' || p.type === 'update' || (p.dataType && p.data)) ? 'data' : 'skip'
      },
      trades: {
        url: 'wss://api.exchange.bullish.com/trading-api/v1/market-data/trades',
        sub: { jsonrpc: '2.0', type: 'command', method: 'subscribe', params: { topic: 'anonymousTrades', symbol: sym }, id: '1' },
        onMsg: (p) => (p.type === 'update' || p.type === 'snapshot' || (p.dataType && p.data)) ? 'data' : 'skip'
      }
    })
  },

  // ── 7. Darkex (WS+REST gzip) ─────────────────────────────────────────────
  // WS symbol: btcusdt (lowercase)  |  REST symbol: BTCUSDT (uppercase)
  // SOL NOT LISTED: returns {"code":"-1121","msg":"Invalid symbol"}
  Darkex: {
    method: 'ws+rest',
    symbols: { BTC: 'btcusdt', ETH: 'ethusdt', SOL: 'solusdt' },
    restSymbols: { BTC: 'BTCUSDT', ETH: 'ETHUSDT', SOL: 'SOLUSDT' },
    streams: (sym, restSym) => ({
      orderbook: {
        url: 'wss://ws.darkex.com/kline-api/ws', decompress: true,
        sub: { event: 'sub', params: { channel: `market_${sym}_depth_step0` } },
        onMsg: (p) => (p.channel && p.tick) ? 'data' : 'skip'
      },
      trades: {
        url: 'wss://ws.darkex.com/kline-api/ws', decompress: true,
        sub: { event: 'sub', params: { channel: `market_${sym}_trade_ticker` } },
        onMsg: (p) => (p.channel && p.tick) ? 'data' : 'skip'
      }
    }),
    restEndpoints: (restSym) => ({
      orderbook: `https://openapi.darkex.com/sapi/v1/depth?symbol=${restSym}&limit=5`,
      trades: `https://openapi.darkex.com/sapi/v1/trades?symbol=${restSym}&limit=5`
    })
  },

  // ── 8. Bitrue (WS OB + REST trades) ──────────────────────────────────────
  // WS only supports depth channels — NO trade channels exist (all return status:error)
  // Use market_BTCUSDT_depth_step0  NOT  simple_depth_step0  (more reliable)
  Bitrue: {
    method: 'ws+rest',
    symbols: { BTC: 'BTCUSDT', ETH: 'ETHUSDT', SOL: 'SOLUSDT' },
    streams: (sym) => ({
      orderbook: {
        url: 'wss://ws.bitrue.com/market/ws', decompress: true,
        sub: { event: 'sub', params: { cb_id: sym, channel: `market_${sym}_depth_step0` } },
        onMsg: (p) => {
          if (p.channel && p.tick) return 'data';
          return 'skip';
        }
      },
      trades: null // Bitrue WS has NO trade channels — ALL tested return {status:"error"}
    }),
    restEndpoints: (sym) => ({
      orderbook: `https://openapi.bitrue.com/api/v1/depth?symbol=${sym}&limit=5`,
      trades: `https://openapi.bitrue.com/api/v1/trades?symbol=${sym}&limit=5`
    })
  },

  // ── 9. FameEX (WS+REST) ──────────────────────────────────────────────────
  // CRITICAL: ALL channels use UNDERSCORE format {event:"sub", params:{channel:"..."}}
  //   Trade channel: market_btcusdt_trade_detail  (NOT dot-notation "market.btcusdt.trade.detail")
  //   Depth channel: market_btcusdt_depth_step0
  // WS symbol: btcusdt (lowercase)  |  REST symbol: BTCUSDT (uppercase)
  FameEX: {
    method: 'ws+rest',
    symbols: { BTC: 'btcusdt', ETH: 'ethusdt', SOL: 'solusdt' },
    restSymbols: { BTC: 'BTCUSDT', ETH: 'ETHUSDT', SOL: 'SOLUSDT' },
    streams: (sym) => ({
      orderbook: {
        url: 'wss://wsapi.fameex.com/v1/ws/stream/public',
        sub: { event: 'sub', params: { channel: `market_${sym}_depth_step0` } },
        onMsg: (p) => {
          if (p.event_rep !== undefined && p.tick) return 'data';
          if (p.channel && (p.tick || p.data)) return 'data';
          return 'skip';
        }
      },
      trades: {
        url: 'wss://wsapi.fameex.com/v1/ws/stream/public',
        sub: { event: 'sub', params: { channel: `market_${sym}_trade_detail` } },
        onMsg: (p) => {
          if (p.channel && p.data && Array.isArray(p.data) && p.data.length > 0) return 'data';
          if (p.channel && p.tick && p.tick.data) return 'data';
          return 'skip';
        }
      }
    }),
    restEndpoints: (restSym) => ({
      orderbook: `https://api.fameex.com/v2/public/orderbook?symbol=${restSym}&limit=5`,
      trades: `https://api.fameex.com/sapi/v1/trades?symbol=${restSym}&limit=5`
    })
  },

  // ── 10. OrangeX (REST Deribit-style JSON-RPC) ────────────────────────────
  // Instrument: BTC-USDT-SPOT  |  No WS available
  OrangeX: {
    method: 'rest',
    symbols: { BTC: 'BTC-USDT', ETH: 'ETH-USDT', SOL: 'SOL-USDT' },
    restEndpoints: (sym) => ({
      orderbook: `https://api.orangex.com/api/v1/public/get_order_book?instrument_name=${sym}-SPOT&depth=5`,
      trades: `https://api.orangex.com/api/v1/public/get_last_trades_by_instrument?instrument_name=${sym}-SPOT&count=5`
    })
  },

  // ── 11. Websea (WS+REST) ─────────────────────────────────────────────────
  // Symbol: BTC-USDT (hyphenated)  |  WS binary buffers (decode as UTF-8)
  Websea: {
    method: 'ws+rest',
    symbols: { BTC: 'BTC-USDT', ETH: 'ETH-USDT', SOL: 'SOL-USDT' },
    streams: (sym) => ({
      orderbook: null, // no WS orderbook — REST fallback
      trades: {
        url: 'wss://oapi.websea.com/ws/v1/spot/market', binaryUtf8: true,
        sub: { op: 'sub', channel: 'trade', symbol: sym },
        onMsg: (p) => {
          if (p.errno === 0 && p.errmsg === 'success' && !p.price && !p.amount) return 'skip';
          if (p.channel === 'trade' && (p.price || p.amount || p.id)) return 'data';
          return 'skip';
        }
      }
    }),
    restEndpoints: (sym) => ({
      orderbook: `https://oapi.websea.com/v1/spot/depth?symbol=${sym}&size=5`,
      trades: `https://oapi.websea.com/v1/spot/trade?symbol=${sym}&size=5`
    })
  },

  // ── 12. Azbit (REST) ─────────────────────────────────────────────────────
  // Uses data.azbit.com (NOT api.azbit.com)  |  Symbol: BTC_USDT (underscore)
  Azbit: {
    method: 'rest',
    symbols: { BTC: 'BTC_USDT', ETH: 'ETH_USDT', SOL: 'SOL_USDT' },
    restEndpoints: (sym) => ({
      orderbook: `https://data.azbit.com/api/orderbook?currencyPairCode=${sym}`,
      trades: `https://data.azbit.com/api/deals?currencyPairCode=${sym}`
    })
  },

  // ── 13. BloFin (WS+REST OKX-style) ───────────────────────────────────────
  // instId: BTC-USDT  |  ETH/SOL trades slow (~10s gaps, need ≥30s timeout)
  BloFin: {
    method: 'ws+rest',
    symbols: { BTC: 'BTC-USDT', ETH: 'ETH-USDT', SOL: 'SOL-USDT' },
    streams: (sym) => ({
      orderbook: {
        url: 'wss://openapi.blofin.com/ws/public',
        sub: { op: 'subscribe', args: [{ channel: 'books5', instId: sym }] },
        onMsg: (p) => (p.arg && (p.action || p.data)) ? 'data' : 'skip'
      },
      trades: {
        url: 'wss://openapi.blofin.com/ws/public',
        sub: { op: 'subscribe', args: [{ channel: 'trades', instId: sym }] },
        onMsg: (p) => (p.arg && p.data) ? 'data' : 'skip'
      }
    }),
    restEndpoints: (sym) => ({
      orderbook: `https://openapi.blofin.com/api/v1/market/books?instId=${sym}&sz=5`,
      trades: `https://openapi.blofin.com/api/v1/market/trades?instId=${sym}&limit=5`
    })
  },

  // ── 14. BVOX / BitVenus (REST Binance-style) ────────────────────────────
  // Symbol: BTCUSDT  |  Base: api.bitvenus.me/openapi/quote/v1
  BVOX: {
    method: 'rest',
    symbols: { BTC: 'BTCUSDT', ETH: 'ETHUSDT', SOL: 'SOLUSDT' },
    restEndpoints: (sym) => ({
      orderbook: `https://api.bitvenus.me/openapi/quote/v1/depth?symbol=${sym}&limit=5`,
      trades: `https://api.bitvenus.me/openapi/quote/v1/trades?symbol=${sym}&limit=5`
    })
  },

  // ── 15. Trubit Pro (REST Binance-style) ──────────────────────────────────
  // Symbol: BTCUSDT  |  Path: /openapi/quote/v1/  (old paths return {})
  'Trubit Pro': {
    method: 'rest',
    symbols: { BTC: 'BTCUSDT', ETH: 'ETHUSDT', SOL: 'SOLUSDT' },
    restEndpoints: (sym) => ({
      orderbook: `https://api-spot.trubit.com/openapi/quote/v1/depth?symbol=${sym}&limit=5`,
      trades: `https://api-spot.trubit.com/openapi/quote/v1/trades?symbol=${sym}&limit=5`
    })
  }
};

// ═══════════════════════════════════════════════════════════════════════════════
// DATA PARSERS
// Extract price/qty/side from each exchange's unique response format
// ═══════════════════════════════════════════════════════════════════════════════

/**
 * Parse orderbook data from any exchange format
 * Returns: { bids: [[price, qty], ...], asks: [[price, qty], ...] } or null
 *
 * Supported formats:
 *  - Bullish flat array: [price, qty, price, qty, ...]
 *  - Hotcoin WS: {code:200, data:{bids,asks}}
 *  - Hotcoin REST: {data:{depth:{bids,asks}}}
 *  - Standard: {bids:[[p,q],...], asks:[[p,q],...]}
 *  - Azbit: [{isBid:true, price, amount}, ...]
 *  - BloFin: {arg:{}, data:{asks,bids}}
 *  - Darkex/FameEX: {channel, tick:{buys/bids,asks}}
 *  - Zoomex/Bybit: {topic, data:{b,a}}
 *  - Biconomy: {params:[true, {asks,bids}, sym]}
 *  - OrangeX: {result:{asks,bids}}
 *  - Websea: {result:{asks,bids}}
 */
function parseOrderbook(exchange, data) {
  try {
    if (!data) return null;
    let bids = [], asks = [];

    // Bullish: flat array [price, qty, price, qty, ...]
    if (data.type === 'snapshot' && data.data && data.data.bids && typeof data.data.bids[0] === 'string') {
      const b = data.data.bids, a = data.data.asks;
      for (let i = 0; i < Math.min(b.length, 10); i += 2) bids.push([parseFloat(b[i]), parseFloat(b[i+1])]);
      for (let i = 0; i < Math.min(a.length, 10); i += 2) asks.push([parseFloat(a[i]), parseFloat(a[i+1])]);
    }
    // Hotcoin WS: {code:200, data:{bids:[...], asks:[...]}}
    else if (data.code === 200 && data.data && (data.data.bids || data.data.asks)) {
      bids = (data.data.bids || []).slice(0, 5).map(b => Array.isArray(b) ? [parseFloat(b[0]), parseFloat(b[1])] : null).filter(Boolean);
      asks = (data.data.asks || []).slice(0, 5).map(a => Array.isArray(a) ? [parseFloat(a[0]), parseFloat(a[1])] : null).filter(Boolean);
    }
    // Hotcoin REST: {data:{depth:{bids, asks}}}
    else if (data.data && data.data.depth && data.data.depth.asks) {
      bids = (data.data.depth.bids || []).slice(0, 5).map(b => Array.isArray(b) ? [parseFloat(b[0]), parseFloat(b[1])] : null).filter(Boolean);
      asks = (data.data.depth.asks || []).slice(0, 5).map(a => Array.isArray(a) ? [parseFloat(a[0]), parseFloat(a[1])] : null).filter(Boolean);
    }
    // Standard {bids, asks} (Bitrue REST, BVOX, Trubit, etc)
    else if (data.bids && data.asks) {
      bids = (Array.isArray(data.bids) ? data.bids : []).slice(0, 5).map(b => {
        if (Array.isArray(b)) return [parseFloat(b[0]), parseFloat(b[1])];
        if (b.price !== undefined) return [parseFloat(b.price), parseFloat(b.priceLevelQuantity || b.amount || b.quantity || 0)];
        return null;
      }).filter(Boolean);
      asks = (Array.isArray(data.asks) ? data.asks : []).slice(0, 5).map(a => {
        if (Array.isArray(a)) return [parseFloat(a[0]), parseFloat(a[1])];
        if (a.price !== undefined) return [parseFloat(a.price), parseFloat(a.priceLevelQuantity || a.amount || a.quantity || 0)];
        return null;
      }).filter(Boolean);
    }
    // Darkex/FameEX/Bitrue WS: {tick:{buys/bids, asks}}
    else if (data.tick && (data.tick.buys || data.tick.asks || data.tick.bids)) {
      const t = data.tick;
      bids = (t.buys || t.bids || []).slice(0, 5).map(b => Array.isArray(b) ? [parseFloat(b[0]), parseFloat(b[1])] : null).filter(Boolean);
      asks = (t.asks || []).slice(0, 5).map(a => Array.isArray(a) ? [parseFloat(a[0]), parseFloat(a[1])] : null).filter(Boolean);
    }
    // OrangeX: {result:{asks, bids}}
    else if (data.result && data.result.asks) {
      bids = (data.result.bids || []).slice(0, 5).map(b => Array.isArray(b) ? [parseFloat(b[0]), parseFloat(b[1])] : null).filter(Boolean);
      asks = (data.result.asks || []).slice(0, 5).map(a => Array.isArray(a) ? [parseFloat(a[0]), parseFloat(a[1])] : null).filter(Boolean);
    }
    // BloFin: {data:{asks, bids}} (without arg check first)
    else if (data.data && data.data.asks) {
      bids = (data.data.bids || data.data.b || []).slice(0, 5).map(b => Array.isArray(b) ? [parseFloat(b[0]), parseFloat(b[1])] : null).filter(Boolean);
      asks = (data.data.asks || data.data.a || []).slice(0, 5).map(a => Array.isArray(a) ? [parseFloat(a[0]), parseFloat(a[1])] : null).filter(Boolean);
    }
    // BloFin REST: {data:[{asks, bids}]}
    else if (data.data && Array.isArray(data.data) && data.data[0] && data.data[0].asks) {
      bids = (data.data[0].bids || []).slice(0, 5).map(b => Array.isArray(b) ? [parseFloat(b[0]), parseFloat(b[1])] : null).filter(Boolean);
      asks = (data.data[0].asks || []).slice(0, 5).map(a => Array.isArray(a) ? [parseFloat(a[0]), parseFloat(a[1])] : null).filter(Boolean);
    }
    // Biconomy: {params:[true, {asks, bids}, "BTC_USDT"]}
    else if (data.params && Array.isArray(data.params) && data.params[1]) {
      const ob = data.params[1];
      bids = (ob.bids || []).slice(0, 5).map(b => Array.isArray(b) ? [parseFloat(b[0]), parseFloat(b[1])] : null).filter(Boolean);
      asks = (ob.asks || []).slice(0, 5).map(a => Array.isArray(a) ? [parseFloat(a[0]), parseFloat(a[1])] : null).filter(Boolean);
    }
    // Zoomex/Bybit: {topic, data:{b:[[p,q]], a:[[p,q]]}}
    else if (data.topic && data.data && (data.data.b || data.data.a)) {
      bids = (data.data.b || []).slice(0, 5).map(b => Array.isArray(b) ? [parseFloat(b[0]), parseFloat(b[1])] : null).filter(Boolean);
      asks = (data.data.a || []).slice(0, 5).map(a => Array.isArray(a) ? [parseFloat(a[0]), parseFloat(a[1])] : null).filter(Boolean);
    }
    // Azbit: [{isBid:true, price, amount}, ...]
    else if (Array.isArray(data)) {
      bids = data.filter(d => d.isBid).slice(0, 5).map(d => [parseFloat(d.price), parseFloat(d.amount)]);
      asks = data.filter(d => !d.isBid).slice(0, 5).map(d => [parseFloat(d.price), parseFloat(d.amount)]);
    }
    // BloFin WS: {arg:{channel}, data:{asks, bids}}
    else if (data.arg && data.data && data.data.asks) {
      bids = (data.data.bids || []).slice(0, 5).map(b => Array.isArray(b) ? [parseFloat(b[0]), parseFloat(b[1])] : null).filter(Boolean);
      asks = (data.data.asks || []).slice(0, 5).map(a => Array.isArray(a) ? [parseFloat(a[0]), parseFloat(a[1])] : null).filter(Boolean);
    }
    // Darkex/FameEX: {channel, tick:{asks}}
    else if (data.channel && data.tick && data.tick.asks) {
      bids = (data.tick.buys || data.tick.bids || []).slice(0, 5).map(b => Array.isArray(b) ? [parseFloat(b[0]), parseFloat(b[1])] : null).filter(Boolean);
      asks = (data.tick.asks || []).slice(0, 5).map(a => Array.isArray(a) ? [parseFloat(a[0]), parseFloat(a[1])] : null).filter(Boolean);
    }

    if (bids.length === 0 && asks.length === 0) return null;
    return { bids, asks };
  } catch (e) { return null; }
}

/**
 * Parse trade data from any exchange format
 * Returns: [{ price, qty, side }, ...] or null
 *
 * Supported formats:
 *  - Bullish: {type:'snapshot', data:{trades:[{price,quantity,side}]}}
 *  - XT.com: {topic:'trade', data:{s,p,q,b}}
 *  - Hotcoin WS: {code:200, data:[{price,amount,direction}]}
 *  - Hotcoin REST: {data:{trades:[{price,amount,type}]}}
 *  - NovaEx REST: {success:true, rows:[{executed_price,executed_quantity,side}]}
 *  - Direct array: [{price, qty, isBuyerMaker}]  (Bitrue, BVOX, Trubit, Azbit)
 *  - OrangeX: {result:{trades:[{price,amount,direction}]}}
 *  - BloFin: {arg, data:[{price,size,side}]}
 *  - FameEX: {channel, data:[{price,vol,side}]}
 *  - Darkex: {channel, tick:{data:[{price,vol,side}]}}
 *  - Websea WS: {channel:'trade', price, amount, direction}
 *  - Zoomex: {topic, data:[{p,v,S}]}
 */
function parseTrades(exchange, data) {
  try {
    let trades = [];

    // Bullish WS: {type:'snapshot'|'update', data:{trades:[{price,quantity,side}]}}
    if ((data.type === 'snapshot' || data.type === 'update') && data.data && data.data.trades) {
      trades = data.data.trades.slice(0, 5).map(t => ({
        price: parseFloat(t.price || 0), qty: parseFloat(t.quantity || 0),
        side: (t.side || 'unknown').toLowerCase()
      }));
    }
    // XT.com WS: {topic:'trade', data:{s,p,q,b}}
    else if (data.topic === 'trade' && data.data && data.data.p) {
      trades = [{ price: parseFloat(data.data.p), qty: parseFloat(data.data.q || 0), side: data.data.b ? 'buy' : 'sell' }];
    }
    // Hotcoin WS: {code:200, data:[{price,amount,direction}]}
    else if (data.code === 200 && data.data && Array.isArray(data.data) && data.data.length > 0 && data.data[0].price) {
      trades = data.data.slice(0, 5).map(t => ({
        price: parseFloat(t.price || 0), qty: parseFloat(t.amount || 0), side: (t.direction || 'unknown').toLowerCase()
      }));
    }
    // Hotcoin REST: {data:{trades:[{price,amount,type}]}}
    else if (data.data && data.data.trades && Array.isArray(data.data.trades)) {
      trades = data.data.trades.slice(0, 5).map(t => ({
        price: parseFloat(t.price || 0), qty: parseFloat(t.amount || 0), side: (t.type || t.en_type || 'unknown').toLowerCase()
      }));
    }
    // NovaEx/WOO REST: {success:true, rows:[{executed_price,executed_quantity,side}]}
    else if (data.success === true && data.rows && Array.isArray(data.rows)) {
      trades = data.rows.slice(0, 5).map(t => ({
        price: parseFloat(t.executed_price || 0), qty: parseFloat(t.executed_quantity || 0),
        side: (t.side || 'unknown').toLowerCase()
      }));
    }
    // Direct array (Bitrue REST, BVOX, Trubit, Azbit)
    else if (Array.isArray(data)) {
      trades = data.slice(0, 5).map(t => ({
        price: parseFloat(t.price || t.p || 0),
        qty: parseFloat(t.qty || t.q || t.quantity || t.amount || t.volume || t.vol || 0),
        side: t.side || (t.isBuyerMaker ? 'sell' : t.isBuy === false ? 'sell' : 'buy') || (t.type || (t.b === false ? 'sell' : 'buy'))
      }));
    }
    // OrangeX: {result:{trades:[{price,amount,direction}]}}
    else if (data.result && data.result.trades) {
      trades = data.result.trades.slice(0, 5).map(t => ({
        price: parseFloat(t.price), qty: parseFloat(t.amount), side: t.direction || 'unknown'
      }));
    }
    // Websea REST: {result:{data:[{price,amount,direction}]}}
    else if (data.result && data.result.data && Array.isArray(data.result.data)) {
      trades = data.result.data.slice(0, 5).map(t => ({
        price: parseFloat(t.price), qty: parseFloat(t.amount || t.vol || 0), side: t.direction || 'unknown'
      }));
    }
    // BloFin/FameEX/generic: {data:[{price,size,side}]}
    else if (data.data && Array.isArray(data.data)) {
      trades = data.data.slice(0, 5).map(t => ({
        price: parseFloat(t.price || t.p || 0),
        qty: parseFloat(t.size || t.qty || t.amount || t.vol || t.q || 0),
        side: t.side || t.direction || 'unknown'
      }));
    }
    // Biconomy WS: {params:[symbol, [{price,amount,type}]]}
    else if (data.params && Array.isArray(data.params)) {
      const arr = data.params[1] || data.params;
      if (Array.isArray(arr)) {
        trades = arr.slice(0, 5).map(t => ({
          price: parseFloat(t.price || 0), qty: parseFloat(t.amount || 0), side: t.type || t.side || 'unknown'
        }));
      }
    }
    // Zoomex/Bybit: {topic, data:[{p,v,S}]}
    else if (data.topic && data.data && Array.isArray(data.data)) {
      trades = data.data.slice(0, 5).map(t => ({
        price: parseFloat(t.p || t.price || 0), qty: parseFloat(t.v || t.size || t.qty || 0),
        side: t.S === 'Sell' ? 'sell' : t.S === 'Buy' ? 'buy' : (t.side || 'unknown')
      }));
    }
    // BloFin WS: {arg:{channel:'trades'}, data:[{price,size,side}]}
    else if (data.arg && data.data && Array.isArray(data.data)) {
      trades = data.data.slice(0, 5).map(t => ({
        price: parseFloat(t.price || 0), qty: parseFloat(t.size || 0), side: t.side || 'unknown'
      }));
    }
    // Darkex WS: {channel, tick:{data:[{price,vol,side}]}}
    else if (data.channel && data.tick && data.tick.data) {
      trades = data.tick.data.slice(0, 5).map(t => ({
        price: parseFloat(t.price || 0), qty: parseFloat(t.vol || t.amount || 0), side: (t.side || 'unknown').toLowerCase()
      }));
    }
    // FameEX WS corrected: {channel, data:[{price,vol,side}]}
    else if (data.channel && data.data && Array.isArray(data.data) && data.data.length > 0 && data.data[0].price) {
      trades = data.data.slice(0, 5).map(t => ({
        price: parseFloat(t.price || 0), qty: parseFloat(t.vol || t.amount || 0), side: (t.side || 'unknown').toLowerCase()
      }));
    }
    // Websea WS single trade: {channel:'trade', price, amount, direction}
    else if (data.channel === 'trade' && data.price) {
      trades = [{ price: parseFloat(data.price), qty: parseFloat(data.amount || 0), side: data.direction || 'unknown' }];
    }
    // Darkex REST: {list:[{price,qty,side}]}
    else if (data.list && Array.isArray(data.list)) {
      trades = data.list.slice(0, 5).map(t => ({
        price: parseFloat(t.price || 0), qty: parseFloat(t.qty || 0), side: (t.side || 'unknown').toLowerCase()
      }));
    }

    return trades.length > 0 ? trades : null;
  } catch (e) { return null; }
}

// ═══════════════════════════════════════════════════════════════════════════════
// DUCKDB SETUP
// ═══════════════════════════════════════════════════════════════════════════════

async function initDB() {
  const instance = await DuckDBInstance.create('streaming.duckdb');
  const conn = await instance.connect();

  // Create/reset tables for this test run
  await conn.run(`DROP TABLE IF EXISTS stream_test_trades`);
  await conn.run(`DROP TABLE IF EXISTS stream_test_orderbook`);
  await conn.run(`DROP TABLE IF EXISTS stream_test_results`);

  await conn.run(`
    CREATE TABLE stream_test_trades (
      ts TIMESTAMP DEFAULT current_timestamp,
      exchange TEXT NOT NULL,
      coin TEXT NOT NULL,
      canonical_pair TEXT NOT NULL,
      source TEXT NOT NULL,
      price DOUBLE,
      qty DOUBLE,
      side TEXT
    )
  `);

  await conn.run(`
    CREATE TABLE stream_test_orderbook (
      ts TIMESTAMP DEFAULT current_timestamp,
      exchange TEXT NOT NULL,
      coin TEXT NOT NULL,
      canonical_pair TEXT NOT NULL,
      source TEXT NOT NULL,
      best_bid_price DOUBLE,
      best_bid_qty DOUBLE,
      best_ask_price DOUBLE,
      best_ask_qty DOUBLE,
      bid_levels INT,
      ask_levels INT,
      spread_pct DOUBLE
    )
  `);

  await conn.run(`
    CREATE TABLE stream_test_results (
      ts TIMESTAMP DEFAULT current_timestamp,
      exchange TEXT NOT NULL,
      coin TEXT NOT NULL,
      stream_type TEXT NOT NULL,
      source TEXT NOT NULL,
      success BOOLEAN NOT NULL,
      error TEXT,
      sample TEXT
    )
  `);

  return { instance, conn };
}

// ═══════════════════════════════════════════════════════════════════════════════
// TEST RUNNER — Tests one exchange × one coin (orderbook + trades)
// Strategy: Try WS first → fallback to REST if WS fails or unavailable
// ═══════════════════════════════════════════════════════════════════════════════

async function testExchangeCoin(conn, exchangeName, coin, exCfg) {
  const sym = exCfg.symbols[coin];
  const restSym = (exCfg.restSymbols && exCfg.restSymbols[coin]) || sym;
  if (!sym) return { ob: false, tr: false };

  const canonical = `${coin}_USDT`;
  const wsCfg = exCfg.streams ? exCfg.streams(sym, restSym) : { orderbook: null, trades: null };
  const restCfg = exCfg.restEndpoints ? exCfg.restEndpoints(restSym) : null;

  let obOk = false, trOk = false;

  // ── ORDERBOOK ──────────────────────────────────────────────────────────
  // Try WS first, fallback to REST
  if (wsCfg.orderbook) {
    const r = await testWS(wsCfg.orderbook.url, {
      doDecompress: wsCfg.orderbook.decompress || false,
      binaryUtf8: wsCfg.orderbook.binaryUtf8 || false,
      onOpen: (ws) => {
        ws.send(JSON.stringify(wsCfg.orderbook.sub));
        if (wsCfg.orderbook.ping) {
          setInterval(() => { try { ws.send(JSON.stringify(wsCfg.orderbook.ping)); } catch (e) {} }, 20000);
        }
      },
      onMsg: wsCfg.orderbook.onMsg
    });
    if (r.ok && r.samples.length > 0) {
      const ob = parseOrderbook(exchangeName, r.samples[0]);
      if (ob && (ob.bids.length > 0 || ob.asks.length > 0)) {
        obOk = true;
        const bestBid = ob.bids[0] || [0, 0];
        const bestAsk = ob.asks[0] || [0, 0];
        const spread = bestAsk[0] > 0 && bestBid[0] > 0 ? ((bestAsk[0] - bestBid[0]) / bestBid[0]) * 100 : 0;
        await conn.run(`INSERT INTO stream_test_orderbook (exchange, coin, canonical_pair, source, best_bid_price, best_bid_qty, best_ask_price, best_ask_qty, bid_levels, ask_levels, spread_pct) VALUES ('${exchangeName}', '${coin}', '${canonical}', 'WS', ${bestBid[0]}, ${bestBid[1]}, ${bestAsk[0]}, ${bestAsk[1]}, ${ob.bids.length}, ${ob.asks.length}, ${spread.toFixed(6)})`);
        await conn.run(`INSERT INTO stream_test_results (exchange, coin, stream_type, source, success, sample) VALUES ('${exchangeName}', '${coin}', 'orderbook', 'WS', true, '${JSON.stringify(r.samples[0]).substring(0, 200).replace(/'/g, "''")}')`);
      }
    }
    if (!obOk) {
      await conn.run(`INSERT INTO stream_test_results (exchange, coin, stream_type, source, success, error) VALUES ('${exchangeName}', '${coin}', 'orderbook', 'WS', false, '${(r.error || 'no data').replace(/'/g, "''")}')`);
    }
  }

  // REST fallback for orderbook
  if (!obOk && restCfg && restCfg.orderbook) {
    let r = await httpGet(restCfg.orderbook);
    if (!r.ok && r.error === 'Timeout') { await new Promise(res => setTimeout(res, 2000)); r = await httpGet(restCfg.orderbook, 20000); }
    if (r.ok) {
      const ob = parseOrderbook(exchangeName, r.parsed);
      if (ob && (ob.bids.length > 0 || ob.asks.length > 0)) {
        obOk = true;
        const bestBid = ob.bids[0] || [0, 0];
        const bestAsk = ob.asks[0] || [0, 0];
        const spread = bestAsk[0] > 0 && bestBid[0] > 0 ? ((bestAsk[0] - bestBid[0]) / bestBid[0]) * 100 : 0;
        await conn.run(`INSERT INTO stream_test_orderbook (exchange, coin, canonical_pair, source, best_bid_price, best_bid_qty, best_ask_price, best_ask_qty, bid_levels, ask_levels, spread_pct) VALUES ('${exchangeName}', '${coin}', '${canonical}', 'REST', ${bestBid[0]}, ${bestBid[1]}, ${bestAsk[0]}, ${bestAsk[1]}, ${ob.bids.length}, ${ob.asks.length}, ${spread.toFixed(6)})`);
        await conn.run(`INSERT INTO stream_test_results (exchange, coin, stream_type, source, success, sample) VALUES ('${exchangeName}', '${coin}', 'orderbook', 'REST', true, '${r.raw.substring(0, 200).replace(/'/g, "''")}')`);
      }
    }
    if (!obOk) {
      await conn.run(`INSERT INTO stream_test_results (exchange, coin, stream_type, source, success, error) VALUES ('${exchangeName}', '${coin}', 'orderbook', 'REST', false, '${((r && r.error) || 'no data').replace(/'/g, "''")}')`);
    }
  }

  // ── TRADES ─────────────────────────────────────────────────────────────
  // Try WS first, fallback to REST
  if (wsCfg.trades) {
    const r = await testWS(wsCfg.trades.url, {
      doDecompress: wsCfg.trades.decompress || false,
      binaryUtf8: wsCfg.trades.binaryUtf8 || false,
      onOpen: (ws) => {
        ws.send(JSON.stringify(wsCfg.trades.sub));
        if (wsCfg.trades.ping) {
          setInterval(() => { try { ws.send(JSON.stringify(wsCfg.trades.ping)); } catch (e) {} }, 20000);
        }
      },
      onMsg: wsCfg.trades.onMsg
    });
    if (r.ok && r.samples.length > 0) {
      const trades = parseTrades(exchangeName, r.samples[0]);
      if (trades && trades.length > 0) {
        trOk = true;
        for (const t of trades.slice(0, 3)) {
          await conn.run(`INSERT INTO stream_test_trades (exchange, coin, canonical_pair, source, price, qty, side) VALUES ('${exchangeName}', '${coin}', '${canonical}', 'WS', ${t.price || 0}, ${t.qty || 0}, '${(t.side || 'unknown').replace(/'/g, "''")}')`);
        }
        await conn.run(`INSERT INTO stream_test_results (exchange, coin, stream_type, source, success, sample) VALUES ('${exchangeName}', '${coin}', 'trades', 'WS', true, '${JSON.stringify(r.samples[0]).substring(0, 200).replace(/'/g, "''")}')`);
      }
    }
    if (!trOk) {
      await conn.run(`INSERT INTO stream_test_results (exchange, coin, stream_type, source, success, error) VALUES ('${exchangeName}', '${coin}', 'trades', 'WS', false, '${((r && r.error) || 'no data').replace(/'/g, "''")}')`);
    }
  }

  // REST fallback for trades
  if (!trOk && restCfg && restCfg.trades) {
    let r = await httpGet(restCfg.trades);
    if (!r.ok && r.error === 'Timeout') { await new Promise(res => setTimeout(res, 2000)); r = await httpGet(restCfg.trades, 20000); }
    if (r.ok) {
      const trades = parseTrades(exchangeName, r.parsed);
      if (trades && trades.length > 0) {
        trOk = true;
        for (const t of trades.slice(0, 3)) {
          await conn.run(`INSERT INTO stream_test_trades (exchange, coin, canonical_pair, source, price, qty, side) VALUES ('${exchangeName}', '${coin}', '${canonical}', 'REST', ${t.price || 0}, ${t.qty || 0}, '${(t.side || 'unknown').replace(/'/g, "''")}')`);
        }
        await conn.run(`INSERT INTO stream_test_results (exchange, coin, stream_type, source, success, sample) VALUES ('${exchangeName}', '${coin}', 'trades', 'REST', true, '${r.raw.substring(0, 200).replace(/'/g, "''")}')`);
      }
    }
    if (!trOk) {
      await conn.run(`INSERT INTO stream_test_results (exchange, coin, stream_type, source, success, error) VALUES ('${exchangeName}', '${coin}', 'trades', 'REST', false, '${((r && r.error) || 'no data').replace(/'/g, "''")}')`);
    }
  }

  // Log if no method configured for a stream type
  if (!wsCfg.orderbook && !(restCfg && restCfg.orderbook)) {
    await conn.run(`INSERT INTO stream_test_results (exchange, coin, stream_type, source, success, error) VALUES ('${exchangeName}', '${coin}', 'orderbook', 'NONE', false, 'No endpoint configured')`);
  }
  if (!wsCfg.trades && !(restCfg && restCfg.trades)) {
    await conn.run(`INSERT INTO stream_test_results (exchange, coin, stream_type, source, success, error) VALUES ('${exchangeName}', '${coin}', 'trades', 'NONE', false, 'No endpoint configured')`);
  }

  return { ob: obOk, tr: trOk };
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN — Run all exchanges × all coins, store in DuckDB, print report
// ═══════════════════════════════════════════════════════════════════════════════

async function main() {
  const startTime = Date.now();

  console.log(`
╔══════════════════════════════════════════════════════════════════════════╗
║  MULTI-COIN STREAM COLLECTOR — BTC / ETH / SOL                         ║
║  Orderbook + Trades across ${Object.keys(EXCHANGES).length} exchanges → DuckDB                       ║
║  Started: ${new Date().toISOString()}                          ║
╚══════════════════════════════════════════════════════════════════════════╝
`);

  const { instance, conn } = await initDB();
  console.log('  ✅ DuckDB initialized (stream_test_trades, stream_test_orderbook, stream_test_results)\n');

  const exchangeNames = Object.keys(EXCHANGES);
  let totalTests = 0, totalPass = 0;

  for (let i = 0; i < exchangeNames.length; i++) {
    const name = exchangeNames[i];
    const exCfg = EXCHANGES[name];

    console.log(`${'═'.repeat(65)}`);
    console.log(`  #${i + 1} ${name} [${exCfg.method.toUpperCase()}]`);
    console.log('═'.repeat(65));

    for (const coin of COINS) {
      const sym = exCfg.symbols[coin];
      if (!sym) { console.log(`    ${coin}: ⚠ No symbol mapping`); continue; }

      process.stdout.write(`    ${coin} (${sym})... `);
      const result = await testExchangeCoin(conn, name, coin, exCfg);
      totalTests += 2;
      if (result.ob) totalPass++;
      if (result.tr) totalPass++;

      const obIcon = result.ob ? '✅' : '❌';
      const trIcon = result.tr ? '✅' : '❌';
      console.log(`OB:${obIcon}  TR:${trIcon}`);
    }

    // Small delay between exchanges to avoid rate limits
    await new Promise(r => setTimeout(r, 300));
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  // ═══════════════════════════════════════════════════════════════════════════
  // DUCKDB REPORT QUERIES
  // ═══════════════════════════════════════════════════════════════════════════

  console.log(`\n${'═'.repeat(75)}`);
  console.log(`  MULTI-COIN STREAM COLLECTOR REPORT (${elapsed}s)`);
  console.log('═'.repeat(75));

  // 1. Summary by exchange
  console.log('\n┌─────────────────────────────────────────────────────────────────────────┐');
  console.log('│  RESULTS BY EXCHANGE                                                    │');
  console.log('├───┬──────────────────┬───────────┬───────────┬───────────┬───────────────┤');
  console.log('│ # │ Exchange         │ BTC       │ ETH       │ SOL       │ Score         │');
  console.log('├───┼──────────────────┼───────────┼───────────┼───────────┼───────────────┤');

  let reader = await conn.runAndReadAll(`
    SELECT exchange,
      SUM(CASE WHEN coin='BTC' AND success THEN 1 ELSE 0 END) as btc_pass,
      SUM(CASE WHEN coin='BTC' THEN 1 ELSE 0 END) as btc_total,
      SUM(CASE WHEN coin='ETH' AND success THEN 1 ELSE 0 END) as eth_pass,
      SUM(CASE WHEN coin='ETH' THEN 1 ELSE 0 END) as eth_total,
      SUM(CASE WHEN coin='SOL' AND success THEN 1 ELSE 0 END) as sol_pass,
      SUM(CASE WHEN coin='SOL' THEN 1 ELSE 0 END) as sol_total,
      SUM(CASE WHEN success THEN 1 ELSE 0 END) as total_pass,
      COUNT(*) as total
    FROM stream_test_results
    GROUP BY exchange
    ORDER BY exchange
  `);

  let num = 0;
  for (const row of reader.getRows()) {
    num++;
    const [ex, bp, bt, ep, et, sp, st, tp, tt] = row;
    const btcS = `${bp}/${bt}`;
    const ethS = `${ep}/${et}`;
    const solS = `${sp}/${st}`;
    const totalS = `${tp}/${tt}`;
    const icon = Number(tp) === Number(tt) ? '✅' : Number(tp) > 0 ? '🟡' : '❌';
    console.log(`│${String(num).padStart(2)} │ ${ex.padEnd(16)} │ ${btcS.padEnd(9)} │ ${ethS.padEnd(9)} │ ${solS.padEnd(9)} │ ${icon} ${totalS.padEnd(10)} │`);
  }
  console.log('└───┴──────────────────┴───────────┴───────────┴───────────┴───────────────┘');

  // 2. Orderbook snapshots
  console.log('\n  📊 ORDERBOOK SNAPSHOTS IN DUCKDB:');
  reader = await conn.runAndReadAll(`
    SELECT exchange, coin, source, best_bid_price, best_ask_price,
           ROUND(spread_pct, 4) as spread, bid_levels, ask_levels
    FROM stream_test_orderbook
    ORDER BY coin, exchange
  `);
  console.log('  ┌──────────────────┬─────┬────────┬──────────────┬──────────────┬──────────┬───────┐');
  console.log('  │ Exchange         │Coin │ Source │ Best Bid     │ Best Ask     │ Spread%  │ Depth │');
  console.log('  ├──────────────────┼─────┼────────┼──────────────┼──────────────┼──────────┼───────┤');
  for (const r of reader.getRows()) {
    console.log(`  │ ${r[0].padEnd(16)} │ ${r[1]}  │ ${r[2].padEnd(6)} │ ${String(r[3]).padStart(12)} │ ${String(r[4]).padStart(12)} │ ${String(r[5]).padStart(8)} │ ${r[6]}/${r[7]}   │`);
  }
  console.log('  └──────────────────┴─────┴────────┴──────────────┴──────────────┴──────────┴───────┘');

  // 3. Trade samples
  console.log('\n  📊 TRADE SAMPLES IN DUCKDB:');
  reader = await conn.runAndReadAll(`
    SELECT exchange, coin, source, price, qty, side
    FROM stream_test_trades
    ORDER BY coin, exchange, ts
  `);
  console.log(`  Total trade rows stored: ${reader.getRows().length}`);
  console.log('  ┌──────────────────┬─────┬────────┬──────────────┬──────────────┬─────────┐');
  console.log('  │ Exchange         │Coin │ Source │ Price        │ Qty          │ Side    │');
  console.log('  ├──────────────────┼─────┼────────┼──────────────┼──────────────┼─────────┤');
  // Show first trade per exchange+coin
  const shown = new Set();
  for (const r of reader.getRows()) {
    const key = `${r[0]}-${r[1]}`;
    if (shown.has(key)) continue;
    shown.add(key);
    console.log(`  │ ${r[0].padEnd(16)} │ ${r[1]}  │ ${r[2].padEnd(6)} │ ${String(r[3]).padStart(12)} │ ${String(r[4]).padStart(12)} │ ${(r[5] || '?').padEnd(7)} │`);
  }
  console.log('  └──────────────────┴─────┴────────┴──────────────┴──────────────┴─────────┘');

  // 4. Summary stats
  reader = await conn.runAndReadAll(`SELECT COUNT(*) FROM stream_test_orderbook`);
  const obRows = reader.getRows()[0][0];
  reader = await conn.runAndReadAll(`SELECT COUNT(*) FROM stream_test_trades`);
  const trRows = reader.getRows()[0][0];
  reader = await conn.runAndReadAll(`SELECT COUNT(DISTINCT exchange) FROM stream_test_results WHERE success`);
  const workingExchanges = reader.getRows()[0][0];
  reader = await conn.runAndReadAll(`
    SELECT COUNT(*) FROM (
      SELECT DISTINCT exchange, coin FROM stream_test_results WHERE success
    )
  `);
  const workingPairs = reader.getRows()[0][0];

  // 5. Failures
  console.log('\n  ❌ FAILED STREAMS:');
  reader = await conn.runAndReadAll(`
    SELECT exchange, coin, stream_type, source, error
    FROM stream_test_results
    WHERE NOT success AND source != 'NONE'
    ORDER BY exchange, coin
  `);
  if (reader.getRows().length === 0) {
    console.log('     None! All streams passed.');
  } else {
    for (const r of reader.getRows()) {
      console.log(`     ${r[0]} → ${r[1]} ${r[2]} [${r[3]}]: ${r[4]}`);
    }
  }

  console.log(`\n${'─'.repeat(65)}`);
  console.log(`  📈 FINAL: ${totalPass}/${totalTests} streams passed across ${Object.keys(EXCHANGES).length} exchanges`);
  console.log(`     ✅ Working exchanges: ${workingExchanges}`);
  console.log(`     📊 Exchange×Coin pairs with data: ${workingPairs}`);
  console.log(`     🗄️  DuckDB orderbook rows: ${obRows}`);
  console.log(`     🗄️  DuckDB trade rows: ${trRows}`);
  console.log(`     ⏱️  Elapsed: ${elapsed}s`);
  console.log(`\n  ✅ All data stored in streaming.duckdb`);
  console.log(`     Tables: stream_test_orderbook, stream_test_trades, stream_test_results`);
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
