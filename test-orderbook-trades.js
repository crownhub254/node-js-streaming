const WebSocket = require('ws');
const https = require('https');
const http = require('http');
const zlib = require('zlib');

// ═══════════════════════════════════════════════════════════════════════════
// 17-EXCHANGE ORDERBOOK + TRADES DEPTH TEST (WS+REST UPGRADE)
// Tests orderbook and trades streams — now with new WS endpoints for
// Bitrue, Bullish, Azbit, Trubit Pro + BloFin futures
// ═══════════════════════════════════════════════════════════════════════════

const TIMEOUT = 20000; // 20s per stream
const RESULTS = {};

function ts() { return new Date().toISOString().slice(11, 19); }

function decompress(data) {
  if (!Buffer.isBuffer(data)) return data.toString();
  try { return zlib.gunzipSync(data).toString('utf8'); } catch (e) {}
  try { return zlib.inflateSync(data).toString('utf8'); } catch (e) {}
  try { return zlib.inflateRawSync(data).toString('utf8'); } catch (e) {}
  return data.toString('utf8');
}

function httpGet(url, timeout = 15000) {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve({ ok: false, error: 'Timeout', url }), timeout);
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, { headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' } }, (res) => {
      let body = '';
      res.on('data', (c) => body += c);
      res.on('end', () => {
        clearTimeout(timer);
        if (res.statusCode >= 200 && res.statusCode < 300) {
          try {
            const j = JSON.parse(body);
            const hasData = body.length > 5 && body !== '{}' && body !== '[]';
            resolve({ ok: hasData, status: res.statusCode, data: body.substring(0, 300), parsed: j, url, empty: !hasData });
          } catch (e) {
            resolve({ ok: false, error: 'Invalid JSON', data: body.substring(0, 200), url });
          }
        } else {
          resolve({ ok: false, error: `HTTP ${res.statusCode}`, data: body.substring(0, 150), url });
        }
      });
    });
    req.on('error', (e) => { clearTimeout(timer); resolve({ ok: false, error: e.message, url }); });
    req.end();
  });
}

function testWS(label, url, opts = {}) {
  return new Promise((resolve) => {
    const { onOpen, onMsg, doDecompress, binaryUtf8, wsHeaders } = opts;
    let done = false, ws, timer, msgCount = 0, sample = '';

    const finish = (result) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      try { ws.close(); } catch (e) {}
      resolve(result);
    };

    timer = setTimeout(() => finish({ ok: false, error: 'Timeout (no data in 12s)', msgs: msgCount, sample }), TIMEOUT);

    try {
      ws = new WebSocket(url, { handshakeTimeout: 10000, headers: wsHeaders || {} });
    } catch (e) {
      return finish({ ok: false, error: e.message });
    }

    ws.on('error', (e) => finish({ ok: false, error: e.message }));
    ws.on('close', (code, reason) => {
      if (!done) finish({ ok: false, error: `WS closed: ${code} ${reason || ''}`, msgs: msgCount });
    });

    ws.on('open', () => {
      if (onOpen) onOpen(ws);
    });

    ws.on('message', (raw) => {
      let msg;
      if (binaryUtf8 && Buffer.isBuffer(raw)) {
        msg = raw.toString('utf8');
      } else if (doDecompress) {
        msg = decompress(raw);
      } else {
        msg = typeof raw === 'string' ? raw : raw.toString();
      }

      try {
        const p = JSON.parse(msg);

        // Handle ping/pong
        if (p.ping) { ws.send(JSON.stringify({ pong: p.ping })); return; }
        if (p.result === 'pong' || p.pong || p.ret_msg === 'pong' || p.op === 'pong') return;

        // Skip subscription confirmations
        if (p.event === 'subscribe' && (p.success === true || p.arg)) return;
        if (p.success === true && p.ret_msg === 'subscribe') return;
        if (p.event_rep !== undefined && !p.data && !p.tick) return;
        if (p.status === 'ok' && p.subbed && !p.data && !p.tick) return;
        if (p.channel === 'system') return;
        if (p.error === null && p.result && p.result.status === 'success') return;
        if (p.state === true && p.message === 'On connect') return;
        if (p.errMsg) return; // deserialization errors

        // Check for actual data
        const result = onMsg ? onMsg(p, msg) : null;
        if (result === 'skip') return;

        msgCount++;
        if (!sample) sample = msg.substring(0, 300);

        if (msgCount >= 1) {
          finish({ ok: true, msgs: msgCount, sample });
        }
      } catch (e) {
        // non-JSON message
        msgCount++;
        if (!sample) sample = msg.substring(0, 200);
      }
    });
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// ALL 17 EXCHANGES — ORDERBOOK + TRADES ONLY
// ═══════════════════════════════════════════════════════════════════════════

const TESTS = [
  // ── 1. Biconomy (WS) ──
  {
    name: 'Biconomy', type: 'ws',
    streams: {
      orderbook: {
        url: 'wss://bei.biconomy.com/ws',
        sub: { method: 'depth.subscribe', params: ['BTC_USDT', 50, '0.01'], id: 1 },
        ping: { method: 'server.ping', params: [], id: 5160 },
        onMsg: (p) => (p.method && p.params) ? 'data' : 'skip'
      },
      trades: {
        url: 'wss://bei.biconomy.com/ws',
        sub: { method: 'deals.subscribe', params: ['BTC_USDT'], id: 2 },
        ping: { method: 'server.ping', params: [], id: 5160 },
        onMsg: (p) => (p.method && p.params) ? 'data' : 'skip'
      }
    }
  },

  // ── 2. NovaEx (WS) ──
  {
    name: 'NovaEx', type: 'ws',
    streams: {
      orderbook: {
        url: 'wss://wss.woox.io/ws/stream/OqdphuyIYbng-t001',
        sub: { id: 'sub1', topic: 'SPOT_BTC_USDT@orderbook', event: 'subscribe' },
        onMsg: (p) => (p.topic && p.data) ? 'data' : 'skip'
      },
      trades: {
        url: 'wss://wss.woox.io/ws/stream/OqdphuyIYbng-t001',
        sub: { id: 'sub2', topic: 'SPOT_BTC_USDT@trade', event: 'subscribe' },
        onMsg: (p) => (p.topic && p.data) ? 'data' : 'skip'
      }
    }
  },

  // ── 3. XT.com (WS Spot + Futures) ──
  {
    name: 'XT.com', type: 'ws',
    streams: {
      spot_orderbook: {
        url: 'wss://stream.xt.com/public',
        sub: { method: 'subscribe', params: ['depth_update@btc_usdt'] },
        onMsg: (p) => (p.data || p.topic) ? 'data' : 'skip'
      },
      spot_trades: {
        url: 'wss://stream.xt.com/public',
        sub: { method: 'subscribe', params: ['trade@btc_usdt'] },
        onMsg: (p) => (p.data || p.topic) ? 'data' : 'skip'
      },
      futures_orderbook: {
        url: 'wss://fstream.xt.com/ws/market',
        sub: { method: 'subscribe', params: ['depth_update@btc_usdt'] },
        onMsg: (p) => (p.data || p.topic) ? 'data' : 'skip'
      }
    }
  },

  // ── 4. Hotcoin (WS gzip) ──
  {
    name: 'Hotcoin', type: 'ws', decompress: true,
    streams: {
      orderbook: {
        url: 'wss://wss.hotcoinfin.com/trade/multiple',
        sub: { sub: 'market.btc_usdt.depth.step0' },
        onMsg: (p) => {
          if (p.status === 'ok' && !p.tick && !p.ch) return 'skip'; // sub confirm only
          if (p.tick || p.ch) return 'data'; // actual depth data
          return 'skip';
        }
      },
      trades: {
        url: 'wss://wss.hotcoinfin.com/trade/multiple',
        sub: { sub: 'market.btc_usdt.trade.detail' },
        onMsg: (p) => {
          if (p.status === 'ok' && !p.tick && !p.ch) return 'skip'; // sub confirm only
          if (p.tick || p.ch) return 'data'; // actual trade data
          return 'skip';
        }
      }
    }
  },

  // ── 5. Zoomex (WS Spot + Futures) ──
  {
    name: 'Zoomex', type: 'ws',
    streams: {
      spot_orderbook: {
        url: 'wss://stream.zoomex.com/v5/public/spot',
        sub: { op: 'subscribe', args: ['orderbook.50.BTCUSDT'] },
        ping: { op: 'ping' },
        onMsg: (p) => (p.topic && p.data) ? 'data' : 'skip'
      },
      spot_trades: {
        url: 'wss://stream.zoomex.com/v5/public/spot',
        sub: { op: 'subscribe', args: ['publicTrade.BTCUSDT'] },
        ping: { op: 'ping' },
        onMsg: (p) => (p.topic && p.data) ? 'data' : 'skip'
      },
      futures_orderbook: {
        url: 'wss://stream.zoomex.com/v5/public/linear',
        sub: { op: 'subscribe', args: ['orderbook.50.BTCUSDT'] },
        ping: { op: 'ping' },
        onMsg: (p) => (p.topic && p.data) ? 'data' : 'skip'
      },
      futures_trades: {
        url: 'wss://stream.zoomex.com/v5/public/linear',
        sub: { op: 'subscribe', args: ['publicTrade.BTCUSDT'] },
        ping: { op: 'ping' },
        onMsg: (p) => (p.topic && p.data) ? 'data' : 'skip'
      }
    }
  },

  // ── 6. Bullish (WS + REST) ──
  {
    name: 'Bullish', type: 'ws+rest',
    streams: {
      ws_orderbook: {
        url: 'wss://api.exchange.bullish.com/trading-api/v1/market-data/orderbook',
        sub: { jsonrpc: '2.0', type: 'command', method: 'subscribe', params: { topic: 'l2Orderbook', symbol: 'BTCUSDC' }, id: '1611082473000' },
        onMsg: (p) => {
          if (p.type === 'snapshot' || p.type === 'update') return 'data';
          if (p.dataType && p.data) return 'data';
          return 'skip';
        }
      },
      ws_trades: {
        url: 'wss://api.exchange.bullish.com/trading-api/v1/market-data/trades',
        sub: { jsonrpc: '2.0', type: 'command', method: 'subscribe', params: { topic: 'anonymousTrades', symbol: 'BTCUSDC' }, id: '1611082473000' },
        onMsg: (p) => {
          if (p.type === 'update' || p.type === 'snapshot') return 'data';
          if (p.dataType && p.data) return 'data';
          return 'skip';
        }
      },
      rest_orderbook: 'https://api.exchange.bullish.com/trading-api/v1/markets/BTCUSDT/orderbook/hybrid',
      rest_trades: 'https://api.exchange.bullish.com/trading-api/v1/markets/BTCUSDT/trades'
    }
  },

  // ── 7. Darkex (WS + REST) ──
  {
    name: 'Darkex', type: 'ws+rest', decompress: true,
    streams: {
      ws_orderbook: {
        url: 'wss://ws.darkex.com/kline-api/ws',
        sub: { event: 'sub', params: { channel: 'market_btcusdt_depth_step0' } },
        onMsg: (p) => (p.channel && p.tick) ? 'data' : 'skip'
      },
      ws_trades: {
        url: 'wss://ws.darkex.com/kline-api/ws',
        sub: { event: 'sub', params: { channel: 'market_btcusdt_trade_ticker' } },
        onMsg: (p) => (p.channel && p.tick) ? 'data' : 'skip'
      },
      rest_orderbook: 'https://openapi.darkex.com/sapi/v1/depth?symbol=BTCUSDT&limit=5',
      rest_trades: 'https://openapi.darkex.com/sapi/v1/trades?symbol=BTCUSDT&limit=5'
    }
  },

  // ── 8. Bitrue (WS spot orderbook + REST) ──
  // WS spot trades: channel not available. Futures WS (wsapi.bitrue.com) returns 502.
  {
    name: 'Bitrue', type: 'ws+rest', decompress: true,
    streams: {
      ws_spot_orderbook: {
        url: 'wss://ws.bitrue.com/market/ws',
        sub: { event: 'sub', params: { cb_id: 'BTCUSDT', channel: 'market_BTCUSDT_simple_depth_step0' } },
        ping: { event: 'ping' },
        onMsg: (p) => {
          if (p.channel && p.tick) return 'data';
          if (p.ts && (p.tick || p.channel)) return 'data';
          return 'skip';
        }
      },
      rest_orderbook: 'https://openapi.bitrue.com/api/v1/depth?symbol=BTCUSDT&limit=5',
      rest_trades: 'https://openapi.bitrue.com/api/v1/trades?symbol=BTCUSDT&limit=5'
    }
  },

  // ── 9. FameEX (WS + REST) ──
  {
    name: 'FameEX', type: 'ws+rest',
    streams: {
      ws_orderbook: {
        url: 'wss://wsapi.fameex.com/v1/ws/stream/public',
        sub: { event: 'sub', params: { channel: 'market_btcusdt_depth_step0' } },
        onMsg: (p) => {
          if (p.event_rep !== undefined && p.tick) return 'data';
          if (p.channel && (p.tick || p.data)) return 'data';
          return 'skip';
        }
      },
      ws_trades: {
        url: 'wss://wsapi.fameex.com/v1/ws/stream/public',
        sub: { sub: 'market.btcusdt.trade.detail' },
        onMsg: (p) => {
          if (p.event_rep !== undefined && !p.data && !p.tick) return 'skip'; // sub confirm
          if (p.channel && (p.data || p.tick)) return 'data';
          if (p.data && Array.isArray(p.data) && p.data.length > 0 && p.data[0].price) return 'data';
          return 'skip';
        }
      },
      rest_orderbook: 'https://api.fameex.com/v2/public/orderbook?symbol=BTCUSDT&limit=5',
      rest_trades: 'https://api.fameex.com/sapi/v1/trades?symbol=BTCUSDT&limit=5'
    }
  },

  // ── 10. OrangeX (REST) ──
  {
    name: 'OrangeX', type: 'rest',
    streams: {
      spot_orderbook: 'https://api.orangex.com/api/v1/public/get_order_book?instrument_name=BTC-USDT-SPOT&depth=5',
      spot_trades: 'https://api.orangex.com/api/v1/public/get_last_trades_by_instrument?instrument_name=BTC-USDT-SPOT&count=5',
      futures_orderbook: 'https://api.orangex.com/api/v1/public/get_order_book?instrument_name=BTC-USDT-PERPETUAL&depth=5',
      futures_trades: 'https://api.orangex.com/api/v1/public/get_last_trades_by_instrument?instrument_name=BTC-USDT-PERPETUAL&count=5'
    }
  },

  // ── 11. Websea (WS + REST) ──
  {
    name: 'Websea', type: 'ws+rest', binaryUtf8: true,
    streams: {
      ws_spot_trades: {
        url: 'wss://oapi.websea.com/ws/v1/spot/market',
        sub: { op: 'sub', channel: 'trade', symbol: 'BTC-USDT' },
        onMsg: (p) => {
          if (p.errno === 0 && p.errmsg === 'success' && !p.price && !p.amount) return 'skip'; // sub confirm
          if (p.channel === 'trade' && (p.price || p.amount || p.id)) return 'data';
          return 'skip';
        }
      },
      ws_futures_trades: {
        url: 'wss://oapi.websea.com/ws/v1/futures/market',
        sub: { op: 'sub', channel: 'trade', symbol: 'BTC-USDT' },
        onMsg: (p) => {
          if (p.errno === 0 && p.errmsg === 'success' && !p.price && !p.amount) return 'skip'; // sub confirm
          if (p.channel === 'trade' && (p.price || p.amount || p.id)) return 'data';
          return 'skip';
        }
      },
      rest_spot_orderbook: 'https://oapi.websea.com/v1/spot/depth?symbol=BTC-USDT&size=5',
      rest_spot_trades: 'https://oapi.websea.com/v1/spot/trade?symbol=BTC-USDT&size=5',
      rest_futures_orderbook: 'https://oapi.websea.com/v1/futures/depth?symbol=BTC-USDT&limit=5',
      rest_futures_trades: 'https://oapi.websea.com/v1/futures/trade?symbol=BTC-USDT&size=5'
    }
  },

  // ── 12. Azbit (REST only) ──
  // WS: wss://ws.azbit.com returns 404 on all paths. No public WS available.
  {
    name: 'Azbit', type: 'rest',
    streams: {
      rest_orderbook: 'https://data.azbit.com/api/orderbook?currencyPairCode=BTC_USDT',
      rest_trades: 'https://data.azbit.com/api/deals?currencyPairCode=BTC_USDT'
    }
  },

  // ── 13. BloFin (WS + REST) ──
  {
    name: 'BloFin', type: 'ws+rest',
    streams: {
      ws_orderbook: {
        url: 'wss://openapi.blofin.com/ws/public',
        sub: { op: 'subscribe', args: [{ channel: 'books5', instId: 'BTC-USDT' }] },
        onMsg: (p) => (p.arg && (p.action || p.data)) ? 'data' : 'skip'
      },
      ws_trades: {
        url: 'wss://openapi.blofin.com/ws/public',
        sub: { op: 'subscribe', args: [{ channel: 'trades', instId: 'BTC-USDT' }] },
        onMsg: (p) => (p.arg && p.data) ? 'data' : 'skip'
      },
      // WS futures: BTC-USDT-SWAP returns error 60012 "Invalid request". WS is spot-only.
      rest_orderbook: 'https://openapi.blofin.com/api/v1/market/books?instId=BTC-USDT&sz=5',
      rest_trades: 'https://openapi.blofin.com/api/v1/market/trades?instId=BTC-USDT&limit=5'
    }
  },

  // ── 14. BVOX / BitVenus (REST) ──
  {
    name: 'BVOX (BitVenus)', type: 'rest',
    streams: {
      orderbook: 'https://api.bitvenus.me/openapi/quote/v1/depth?symbol=BTCUSDT&limit=5',
      trades: 'https://api.bitvenus.me/openapi/quote/v1/trades?symbol=BTCUSDT&limit=5'
    }
  },

  // ── 15. Trubit Pro (REST only) ──
  // WS spot: wss://ws.trubit.com returns 403 (geo-blocked).
  // WS futures: wss://api-futures.trubit.com/ws/market connects but "failed to deSerialize" on all JSON formats.
  {
    name: 'Trubit Pro', type: 'rest',
    streams: {
      rest_orderbook: 'https://api-spot.trubit.com/openapi/quote/v1/depth?symbol=BTCUSDT&limit=5',
      rest_trades: 'https://api-spot.trubit.com/openapi/quote/v1/trades?symbol=BTCUSDT&limit=5'
    }
  }
];

// ═══════════════════════════════════════════════════════════════════════════
// RUNNER
// ═══════════════════════════════════════════════════════════════════════════

async function runExchange(exchange) {
  const { name, type, streams, decompress: doDecomp, binaryUtf8 } = exchange;
  const result = { name, type, streams: {} };

  for (const [key, cfg] of Object.entries(streams)) {
    process.stdout.write(`    ${key}... `);

    if (typeof cfg === 'string') {
      // REST endpoint (with retry on timeout)
      let r = await httpGet(cfg);
      if (!r.ok && (r.error === 'Timeout' || r.error === 'ETIMEDOUT')) {
        process.stdout.write(`(retry) `);
        await new Promise(res => setTimeout(res, 2000));
        r = await httpGet(cfg, 20000);
      }
      result.streams[key] = {
        method: 'REST',
        ok: r.ok,
        error: r.ok ? null : (r.error || (r.empty ? 'Empty response {}' : 'Unknown')),
        sample: r.ok ? r.data.substring(0, 200) : (r.data || r.error || '').substring(0, 150),
        status: r.status
      };
      console.log(r.ok ? `✅ HTTP ${r.status}` : `❌ ${r.error || 'Empty'}`);
    } else {
      // WS stream
      const r = await testWS(`${name}-${key}`, cfg.url, {
        doDecompress: doDecomp || false,
        binaryUtf8: binaryUtf8 || false,
        onOpen: (ws) => {
          ws.send(JSON.stringify(cfg.sub));
          if (cfg.ping) {
            const pingData = typeof cfg.ping === 'function' ? cfg.ping() : cfg.ping;
            setInterval(() => { try { ws.send(JSON.stringify(pingData)); } catch (e) {} }, 20000);
          }
        },
        onMsg: cfg.onMsg
      });
      result.streams[key] = {
        method: 'WS',
        ok: r.ok,
        error: r.ok ? null : r.error,
        msgs: r.msgs || 0,
        sample: (r.sample || '').substring(0, 200)
      };
      console.log(r.ok ? `✅ (${r.msgs} msgs)` : `❌ ${r.error}`);
    }
  }

  return result;
}

async function main() {
  const startTime = Date.now();

  console.log(`
╔═══════════════════════════════════════════════════════════════════╗
║  17-EXCHANGE ORDERBOOK+TRADES WS+REST UPGRADE TEST              ║
║  New WS: Bitrue, Bullish, Azbit, Trubit Pro + BloFin futures     ║
║  Started: ${new Date().toISOString()}                ║
╚═══════════════════════════════════════════════════════════════════╝
`);

  for (let i = 0; i < TESTS.length; i++) {
    const ex = TESTS[i];
    console.log(`\n${'═'.repeat(60)}`);
    console.log(`  #${i + 1} ${ex.name} [${ex.type.toUpperCase()}]`);
    console.log('═'.repeat(60));
    RESULTS[ex.name] = await runExchange(ex);
    await new Promise(r => setTimeout(r, 200));
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  // ── REPORT ──
  console.log(`\n\n${'═'.repeat(80)}`);
  console.log(`  ORDERBOOK + TRADES DEPTH TEST REPORT (${elapsed}s)`);
  console.log('═'.repeat(80));

  let totalStreams = 0, passedStreams = 0, failedStreams = 0;
  const passing = [], failing = [], partial = [];

  for (const [name, result] of Object.entries(RESULTS)) {
    const entries = Object.entries(result.streams);
    const passed = entries.filter(([, v]) => v.ok).length;
    const total = entries.length;
    totalStreams += total;
    passedStreams += passed;
    failedStreams += (total - passed);

    if (passed === total) passing.push({ name, passed, total, streams: result.streams });
    else if (passed > 0) partial.push({ name, passed, total, streams: result.streams });
    else failing.push({ name, passed, total, streams: result.streams });
  }

  // Summary table
  console.log('\n┌─────┬──────────────────────┬────────────┬────────────────┬──────────────────────────────────┐');
  console.log('│  #  │ Exchange             │ Method     │ OB + Trades    │ Status                           │');
  console.log('├─────┼──────────────────────┼────────────┼────────────────┼──────────────────────────────────┤');

  let num = 0;
  for (const [name, result] of Object.entries(RESULTS)) {
    num++;
    const entries = Object.entries(result.streams);
    const passed = entries.filter(([, v]) => v.ok).length;
    const total = entries.length;
    const methods = [...new Set(entries.map(([, v]) => v.method))].join('+');
    let status;
    if (passed === total) status = '✅ All passed';
    else if (passed > 0) {
      const failedKeys = entries.filter(([, v]) => !v.ok).map(([k]) => k);
      status = `🟡 ${failedKeys.join(', ')} failed`;
    } else status = '❌ All failed';

    console.log(`│ ${String(num).padStart(3)} │ ${name.padEnd(20)} │ ${methods.padEnd(10)} │ ${`${passed}/${total}`.padEnd(14)} │ ${status.padEnd(32)} │`);
  }

  console.log('└─────┴──────────────────────┴────────────┴────────────────┴──────────────────────────────────┘');

  // Detailed failures
  if (partial.length + failing.length > 0) {
    console.log(`\n❌ FAILED STREAMS DETAIL:`);
    for (const group of [...partial, ...failing]) {
      const failedEntries = Object.entries(group.streams).filter(([, v]) => !v.ok);
      for (const [key, v] of failedEntries) {
        console.log(`   ${group.name} → ${key}: ${v.error} ${v.sample ? `| ${v.sample.substring(0, 100)}` : ''}`);
      }
    }
  }

  // Data samples
  console.log(`\n📊 DATA SAMPLES (first 200 chars):`);
  for (const [name, result] of Object.entries(RESULTS)) {
    const okStreams = Object.entries(result.streams).filter(([, v]) => v.ok);
    if (okStreams.length === 0) continue;
    console.log(`\n  ${name}:`);
    for (const [key, v] of okStreams) {
      console.log(`    ${key} [${v.method}]: ${(v.sample || '').substring(0, 180)}`);
    }
  }

  // Final stats
  console.log(`\n${'─'.repeat(60)}`);
  console.log(`📈 FINAL: ${passedStreams}/${totalStreams} streams passed across ${Object.keys(RESULTS).length} exchanges`);
  console.log(`   ✅ Fully working: ${passing.length}`);
  console.log(`   🟡 Partial: ${partial.length}`);
  console.log(`   ❌ Failed: ${failing.length}`);
  console.log(`   ⏱️  Elapsed: ${elapsed}s`);

  // Save
  const fs = require('fs');
  fs.writeFileSync('orderbook-trades-results.json', JSON.stringify(RESULTS, null, 2));
  console.log(`\n📁 Full results saved to orderbook-trades-results.json`);
}

main().catch(console.error);
