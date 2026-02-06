const WebSocket = require('ws');
const https = require('https');
const http = require('http');
const zlib = require('zlib');
const fs = require('fs');

// ═══════════════════════════════════════════════════════════════════════════
// 12-EXCHANGE COMPREHENSIVE TESTER
// Tests WebSocket and REST API streams for all 12 remaining exchanges
// Collects data for 2 minutes, reports results, auto-fixes errors
// ═══════════════════════════════════════════════════════════════════════════

const TEST_DURATION = 300000; // 5 minutes total (2 min was too tight with retries)
const PER_STREAM_TIMEOUT = 10000; // 10 seconds per individual stream test
const RESULTS = {};

function log(exchange, msg) {
  const ts = new Date().toISOString().slice(11, 19);
  console.log(`  [${ts}][${exchange}] ${msg}`);
}

// ── Helper: decompress gzip/deflate data ──
function decompress(data) {
  if (!Buffer.isBuffer(data)) return data.toString();
  try { return zlib.gunzipSync(data).toString('utf8'); } catch (e) {}
  try { return zlib.inflateSync(data).toString('utf8'); } catch (e) {}
  try { return zlib.inflateRawSync(data).toString('utf8'); } catch (e) {}
  return data.toString('utf8');
}

// ── Helper: REST API GET request ──
function httpGet(url, timeout = 10000) {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve({ success: false, error: 'HTTP timeout' }), timeout);
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, { headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' } }, (res) => {
      let body = '';
      res.on('data', (chunk) => body += chunk);
      res.on('end', () => {
        clearTimeout(timer);
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve({ success: true, data: body.substring(0, 500), status: res.statusCode });
        } else {
          resolve({ success: false, error: `HTTP ${res.statusCode}`, data: body.substring(0, 200) });
        }
      });
    });
    req.on('error', (e) => { clearTimeout(timer); resolve({ success: false, error: e.message }); });
    req.end();
  });
}

// ── Helper: WebSocket stream test ──
function testWS(name, url, options = {}) {
  return new Promise((resolve) => {
    const {
      onOpen, onMessage, pingInterval, pingPayload,
      timeout = PER_STREAM_TIMEOUT, headers = {},
      decompress: shouldDecompress = false
    } = options;

    let received = false;
    let ws, pingTimer, timeoutTimer;
    let messageCount = 0;

    const cleanup = () => {
      if (timeoutTimer) clearTimeout(timeoutTimer);
      if (pingTimer) clearInterval(pingTimer);
      try { if (ws) ws.close(); } catch (e) {}
    };

    const done = (result) => {
      if (received) return;
      received = true;
      cleanup();
      resolve(result);
    };

    try {
      ws = new WebSocket(url, { headers, handshakeTimeout: 10000 });
    } catch (e) {
      resolve({ success: false, error: `Connection failed: ${e.message}` });
      return;
    }

    timeoutTimer = setTimeout(() => {
      done({ success: false, error: `Timeout (${messageCount} non-data msgs received)` });
    }, timeout);

    ws.on('open', () => {
      if (pingInterval && pingPayload) {
        pingTimer = setInterval(() => {
          try {
            ws.send(typeof pingPayload === 'string' ? pingPayload : JSON.stringify(pingPayload));
          } catch (e) {}
        }, pingInterval);
      }
      if (onOpen) onOpen(ws);
    });

    ws.on('message', (data) => {
      let msg;
      try {
        msg = shouldDecompress ? decompress(data) : (Buffer.isBuffer(data) ? data.toString() : data.toString());
      } catch (e) {
        msg = data.toString();
      }

      messageCount++;

      // Custom message handler
      if (onMessage) {
        const result = onMessage(msg, ws);
        if (result === 'skip') return;
        if (result === 'done' || result === true) {
          done({ success: true, sample: msg.substring(0, 300), messages: messageCount });
          return;
        }
      }

      // Default: check if it's meaningful data
      try {
        const parsed = JSON.parse(msg);
        // Skip common non-data messages
        if (parsed.pong || parsed.event === 'pong' || parsed.op === 'pong') return;
        if (parsed.result === 'pong' || parsed.ping) return;
        if (msg === 'pong' || msg === 'ping') return;
        // Skip subscription confirmations
        if (parsed.event === 'subscribe' || parsed.event === 'sub') return;
        if (parsed.event === 'ping') return; // WOO X pings
        if (parsed.event_rep) return; // Bitrue subscription confirmation
        if (parsed.result && parsed.result.status === 'success' && !parsed.method) return;
        if (parsed.success === true && !parsed.data && !parsed.params) return;
        if (parsed.ret_msg === 'subscribe' || parsed.ret_msg === 'pong') return;
        if (parsed.code === 0 && parsed.msg === 'SUCCESS' && parsed.method === 'subscribe') return; // XT.com
        // Subscription confirm without data (Hotcoin/Bitrue style)
        if (parsed.status === 'ok' && !parsed.data && !parsed.tick && !parsed.ch && !parsed.topic) return;
        // Error status responses
        if (parsed.status === 'error') {
          done({ success: false, error: msg.substring(0, 200) });
          return;
        }
        // Error responses
        if (parsed.error && parsed.error !== null) {
          done({ success: false, error: JSON.stringify(parsed.error).substring(0, 200) });
          return;
        }
        // If we get here, it's likely real data
        done({ success: true, sample: msg.substring(0, 300), messages: messageCount });
      } catch (e) {
        // Non-JSON - might still be valid
        if (msg.length > 5 && msg !== 'pong' && msg !== 'ping') {
          done({ success: true, sample: msg.substring(0, 300), messages: messageCount });
        }
      }
    });

    ws.on('error', (err) => done({ success: false, error: err.message }));
    ws.on('close', (code) => done({ success: false, error: `WS closed: ${code}` }));
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// EXCHANGE DEFINITIONS
// ═══════════════════════════════════════════════════════════════════════════

const EXCHANGES = {
  // ── 1. Biconomy ──
  biconomy: {
    name: 'Biconomy.com',
    type: 'ws',
    spot: true, futures: false,
    ws: 'wss://bei.biconomy.com/ws',
    ping: { method: 'server.ping', params: [], id: 5160 },
    pingInterval: 30000,
    streams: {
      spot_orderbook: { method: 'depth.subscribe', params: ['BTC_USDT', 50, '0.01'], id: 1 },
      spot_trades: { method: 'deals.subscribe', params: ['BTC_USDT'], id: 2 },
      spot_ticker: { method: 'state.subscribe', params: ['BTC_USDT'], id: 3 },
      spot_kline: { method: 'kline.subscribe', params: ['BTC_USDT', 60], id: 4 }
    }
  },

  // ── 2. NovaEx ──
  novaex: {
    name: 'NovaEx',
    type: 'ws',
    spot: true, futures: true,
    // NovaEx = WOO X white-label, use WOO X API (new domain)
    ws: 'wss://wss.woox.io/ws/stream/OqdphuyIYbng-t001',
    streams: {
      spot_orderbook: { id: 'sub1', topic: 'SPOT_BTC_USDT@orderbook', event: 'subscribe' },
      spot_trades: { id: 'sub2', topic: 'SPOT_BTC_USDT@trade', event: 'subscribe' }
    }
  },

  // ── 3. Bullish ──
  bullish: {
    name: 'Bullish.com',
    type: 'rest',
    spot: true, futures: false,
    endpoints: {
      spot_ticker: 'https://api.exchange.bullish.com/trading-api/v1/markets',
      spot_orderbook: 'https://api.exchange.bullish.com/trading-api/v1/markets/BTCUSDT/orderbook/hybrid',
      spot_trades: 'https://api.exchange.bullish.com/trading-api/v1/markets/BTCUSDT/trades'
    }
  },

  // ── 4. XT.com ──
  xt: {
    name: 'XT.com',
    type: 'ws',
    spot: true, futures: true,
    ws: 'wss://stream.xt.com/public',
    wsFutures: 'wss://fstream.xt.com/ws/market',
    streams: {
      spot_orderbook: { method: 'subscribe', params: ['depth_update@btc_usdt'] },
      spot_trades: { method: 'subscribe', params: ['trade@btc_usdt'] },
      spot_ticker: { method: 'subscribe', params: ['ticker@btc_usdt'] },
      spot_kline: { method: 'subscribe', params: ['kline@btc_usdt,1m'] }
    },
    futuresStreams: {
      futures_orderbook: { method: 'subscribe', params: ['depth_update@btc_usdt'] },
      futures_ticker: { method: 'subscribe', params: ['ticker@btc_usdt'] }
    }
  },

  // ── 5. UZX.com ──
  uzx: {
    name: 'UZX.com',
    type: 'rest',
    spot: true, futures: true,
    endpoints: {
      spot_ticker: 'https://www.uzx.com/api/v1/ticker/24hr?symbol=BTCUSDT',
      spot_orderbook: 'https://www.uzx.com/api/v1/depth?symbol=BTCUSDT&limit=5'
    }
  },

  // ── 6. SuperEx ──
  superex: {
    name: 'SuperEx.com',
    type: 'rest',
    spot: true, futures: true,
    endpoints: {
      spot_ticker: 'https://api.superex.com/api/public/v1/market/ticker?symbol=btc_usdt',
      spot_orderbook: 'https://api.superex.com/api/public/v1/market/depth?symbol=btc_usdt&limit=5'
    }
  },

  // ── 7. FameEX ──
  fameex: {
    name: 'FameEX.com',
    type: 'rest',
    spot: true, futures: true,
    endpoints: {
      spot_ticker: 'https://api.fameex.com/v2/public/ticker',
      spot_orderbook: 'https://api.fameex.com/v2/public/orderbook?symbol=BTCUSDT&limit=5',
      spot_trades: 'https://api.fameex.com/sapi/v1/trades?symbol=BTCUSDT&limit=5'
    }
  },

  // ── 8. Hotcoin ──
  hotcoin: {
    name: 'Hotcoin.com',
    type: 'ws',
    spot: true, futures: true,
    ws: 'wss://wss.hotcoinfin.com/trade/multiple',
    wsAlt: 'wss://wss.hotcoin.top/trade/multiple',
    decompress: true,
    streams: {
      spot_orderbook: { sub: 'market.btc_usdt.depth.step0' },
      spot_trades: { sub: 'market.btc_usdt.trade.detail' },
      spot_ticker: { sub: 'market.btc_usdt.detail' },
      spot_kline: { sub: 'market.btc_usdt.kline.1m' }
    },
    pingInterval: 15000,
    ping: { ping: Date.now() }
  },

  // ── 9. OrangeX ──
  orangex: {
    name: 'OrangeX.com',
    type: 'rest',
    spot: true, futures: true,
    endpoints: {
      spot_orderbook: 'https://api.orangex.com/api/v1/public/get_order_book?instrument_name=BTC-USDT-SPOT&depth=5',
      spot_trades: 'https://api.orangex.com/api/v1/public/get_last_trades_by_instrument?instrument_name=BTC-USDT-SPOT&count=5',
      futures_ticker: 'https://api.orangex.com/api/v1/public/ticker?instrument_name=BTC-USDT-PERPETUAL',
      futures_orderbook: 'https://api.orangex.com/api/v1/public/get_order_book?instrument_name=BTC-USDT-PERPETUAL&depth=5',
      futures_trades: 'https://api.orangex.com/api/v1/public/get_last_trades_by_instrument?instrument_name=BTC-USDT-PERPETUAL&count=5'
    }
  },

  // ── 10. Darkex ──
  darkex: {
    name: 'Darkex.com',
    type: 'rest',
    spot: true, futures: true,
    endpoints: {
      spot_ticker: 'https://openapi.darkex.com/sapi/v1/ticker/24hr?symbol=BTCUSDT',
      spot_orderbook: 'https://openapi.darkex.com/sapi/v1/depth?symbol=BTCUSDT&limit=5',
      spot_trades: 'https://openapi.darkex.com/sapi/v1/trades?symbol=BTCUSDT&limit=5',
      spot_kline: 'https://openapi.darkex.com/sapi/v1/klines?symbol=BTCUSDT&interval=1m&limit=5'
    }
  },

  // ── 11. Bitrue ──
  bitrue: {
    name: 'Bitrue.com',
    type: 'rest',
    spot: true, futures: true,
    endpoints: {
      spot_ticker: 'https://openapi.bitrue.com/api/v1/ticker/24hr?symbol=BTCUSDT',
      spot_orderbook: 'https://openapi.bitrue.com/api/v1/depth?symbol=BTCUSDT&limit=5',
      spot_trades: 'https://openapi.bitrue.com/api/v1/trades?symbol=BTCUSDT&limit=5',
      spot_kline: 'https://openapi.bitrue.com/api/v1/klines?symbol=BTCUSDT&interval=1m&limit=5'
    }
  },

  // ── 12. Zoomex ──
  zoomex: {
    name: 'Zoomex',
    type: 'ws',
    spot: true, futures: true,
    ws: 'wss://stream.zoomex.com/v5/public/spot',
    wsFutures: 'wss://stream.zoomex.com/v5/public/linear',
    streams: {
      spot_orderbook: { op: 'subscribe', args: ['orderbook.50.BTCUSDT'] },
      spot_trades: { op: 'subscribe', args: ['publicTrade.BTCUSDT'] },
      spot_ticker: { op: 'subscribe', args: ['tickers.BTCUSDT'] },
      spot_kline: { op: 'subscribe', args: ['kline.1.BTCUSDT'] }
    },
    futuresStreams: {
      futures_orderbook: { op: 'subscribe', args: ['orderbook.50.BTCUSDT'] },
      futures_trades: { op: 'subscribe', args: ['publicTrade.BTCUSDT'] },
      futures_ticker: { op: 'subscribe', args: ['tickers.BTCUSDT'] },
      futures_kline: { op: 'subscribe', args: ['kline.1.BTCUSDT'] }
    },
    pingInterval: 20000,
    ping: { op: 'ping' }
  }
};

// ═══════════════════════════════════════════════════════════════════════════
// TEST RUNNERS
// ═══════════════════════════════════════════════════════════════════════════

async function testRESTExchange(key, config) {
  const results = {};
  for (const [streamKey, url] of Object.entries(config.endpoints)) {
    process.stdout.write(`    ${streamKey}... `);
    const result = await httpGet(url);
    results[streamKey] = result;
    if (result.success) {
      console.log(`✅ (HTTP ${result.status})`);
    } else {
      console.log(`❌ ${result.error}`);
    }
  }
  return results;
}

async function testWSExchange(key, config) {
  const results = {};
  const wsUrl = config.ws;

  // Test spot streams
  if (config.streams) {
    for (const [streamKey, sub] of Object.entries(config.streams)) {
      process.stdout.write(`    ${streamKey}... `);
      const result = await testWS(`${config.name}-${streamKey}`, wsUrl, {
        decompress: config.decompress,
        pingInterval: config.pingInterval,
        pingPayload: config.ping,
        onOpen: (ws) => {
          ws.send(JSON.stringify(sub));
        },
        onMessage: (msg, ws) => {
          // Handle Huobi-style ping
          try {
            const parsed = JSON.parse(msg);
            if (parsed.ping) {
              ws.send(JSON.stringify({ pong: parsed.ping }));
              return 'skip';
            }
            if (parsed.result === 'pong' || parsed.pong) return 'skip';
            if (parsed.ret_msg === 'pong' || parsed.op === 'pong') return 'skip';
            // Subscription confirmations
            if (parsed.error === null && parsed.result && parsed.result.status === 'success') return 'skip';
            if (parsed.success === true && parsed.ret_msg === 'subscribe') return 'skip';
            if (parsed.event === 'sub' || (parsed.subbed && !parsed.data)) return 'skip';
            if (parsed.status === 'ok' && parsed.subbed) return 'skip';
            if (parsed.event_rep) return 'skip'; // Bitrue subscription confirmation
            if (parsed.status === 'ok' && !parsed.data && !parsed.tick && !parsed.ch) return 'skip'; // Hotcoin/Bitrue confirm
            if (parsed.event === 'subscribe' && parsed.success === true) return 'skip'; // WOO X confirm
            // Error messages
            if (parsed.status === 'error') return null; // Let default handler report error
            // Data messages
            if (parsed.method && parsed.params) return 'done'; // Biconomy-style
            if (parsed.data || parsed.tick || parsed.ch) return 'done'; // Huobi-style
            if (parsed.topic) return 'done'; // Bybit-style
            if (parsed.type && (parsed.asks || parsed.bids)) return 'done';
          } catch (e) {}
          return null;
        }
      });
      results[streamKey] = result;
      if (result.success) {
        console.log(`✅ (${result.messages || '?'} msgs)`);
      } else {
        console.log(`❌ ${result.error}`);
      }
    }
  }

  // Test futures streams on separate WS endpoint
  if (config.futuresStreams && config.wsFutures) {
    for (const [streamKey, sub] of Object.entries(config.futuresStreams)) {
      process.stdout.write(`    ${streamKey}... `);
      const result = await testWS(`${config.name}-${streamKey}`, config.wsFutures, {
        decompress: config.decompress,
        pingInterval: config.pingInterval,
        pingPayload: config.ping,
        onOpen: (ws) => {
          ws.send(JSON.stringify(sub));
        },
        onMessage: (msg, ws) => {
          try {
            const parsed = JSON.parse(msg);
            if (parsed.ping) { ws.send(JSON.stringify({ pong: parsed.ping })); return 'skip'; }
            if (parsed.result === 'pong' || parsed.pong || parsed.ret_msg === 'pong' || parsed.op === 'pong') return 'skip';
            if (parsed.success === true && parsed.ret_msg === 'subscribe') return 'skip';
            if (parsed.error === null && parsed.result && parsed.result.status === 'success') return 'skip';
            if (parsed.status === 'ok' && parsed.subbed) return 'skip';
            if (parsed.event_rep) return 'skip'; // Bitrue sub confirm
            if (parsed.status === 'ok' && !parsed.data && !parsed.tick && !parsed.ch) return 'skip'; // Hotcoin confirm
            if (parsed.event === 'subscribe' && parsed.success === true) return 'skip'; // WOO X confirm
            if (parsed.status === 'error') return null; // error
            if (parsed.method && parsed.params) return 'done';
            if (parsed.data || parsed.tick || parsed.ch || parsed.topic) return 'done';
          } catch (e) {}
          return null;
        }
      });
      results[streamKey] = result;
      if (result.success) {
        console.log(`✅ (${result.messages || '?'} msgs)`);
      } else {
        console.log(`❌ ${result.error}`);
      }
    }
  }

  return results;
}

// ── Try alternative endpoints for failed exchanges ──
async function retryWithAlternatives(key, config, failedStreams) {
  if (!config.wsAlt) return {};
  
  const results = {};
  const altUrl = config.wsAlt;
  
  console.log(`    ↻ Retrying failed streams on alt endpoint: ${altUrl.substring(0, 50)}...`);
  
  for (const streamKey of failedStreams) {
    const sub = config.streams[streamKey];
    if (!sub) continue;
    
    process.stdout.write(`    ${streamKey} (retry)... `);
    const result = await testWS(`${config.name}-${streamKey}-alt`, altUrl, {
      decompress: config.decompress,
      pingInterval: config.pingInterval,
      pingPayload: config.ping,
      onOpen: (ws) => {
        ws.send(JSON.stringify(sub));
      },
      onMessage: (msg, ws) => {
        try {
          const parsed = JSON.parse(msg);
          if (parsed.ping) { ws.send(JSON.stringify({ pong: parsed.ping })); return 'skip'; }
          if (parsed.result === 'pong' || parsed.pong || parsed.ret_msg === 'pong' || parsed.op === 'pong') return 'skip';
          if (parsed.success === true && parsed.ret_msg === 'subscribe') return 'skip';
          if (parsed.event_rep) return 'skip'; // Bitrue
          if (parsed.status === 'ok' && !parsed.data && !parsed.tick && !parsed.ch) return 'skip'; // Hotcoin
          if (parsed.event === 'subscribe' && parsed.success === true) return 'skip'; // WOO X
          if (parsed.status === 'error') return null;
          if (parsed.data || parsed.tick || parsed.ch || parsed.topic || parsed.method) return 'done';
        } catch (e) {}
        return null;
      }
    });
    results[streamKey] = result;
    if (result.success) {
      console.log(`✅ (alt endpoint worked!)`);
    } else {
      console.log(`❌ ${result.error}`);
    }
  }
  
  return results;
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN TEST
// ═══════════════════════════════════════════════════════════════════════════

async function testExchange(key, config) {
  console.log(`\n${'═'.repeat(65)}`);
  console.log(`  📊 #${Object.keys(EXCHANGES).indexOf(key) + 1} ${config.name}`);
  console.log('═'.repeat(65));

  if (config.skip) {
    console.log(`  ⏭️  SKIPPED: ${config.reason}`);
    return { name: config.name, skipped: true, reason: config.reason, tests: {} };
  }

  let tests = {};

  if (config.type === 'rest') {
    console.log(`  📡 Testing REST API...`);
    tests = await testRESTExchange(key, config);
  } else if (config.type === 'ws') {
    console.log(`  🔌 Testing WebSocket: ${config.ws.substring(0, 55)}...`);
    tests = await testWSExchange(key, config);

    // Retry failed streams with alt endpoint
    const failedStreams = Object.entries(tests)
      .filter(([, r]) => !r.success)
      .map(([k]) => k);

    if (failedStreams.length > 0 && config.wsAlt) {
      const retryResults = await retryWithAlternatives(key, config, failedStreams);
      for (const [k, v] of Object.entries(retryResults)) {
        if (v.success) tests[k] = v; // Override with successful retry
      }
    }
  }

  return {
    name: config.name,
    type: config.type || 'skip',
    spot: config.spot || false,
    futures: config.futures || false,
    tests
  };
}

async function main() {
  const startTime = Date.now();
  
  console.log(`
╔═══════════════════════════════════════════════════════════════════════════╗
║     12-EXCHANGE COMPREHENSIVE STREAM TEST                                 ║
║     Testing WebSocket & REST API streams for ALL 12 remaining exchanges   ║
║     Duration: 5 minutes max | Auto-retry on failure                       ║
╚═══════════════════════════════════════════════════════════════════════════╝
  `);
  console.log(`⏱️  Started at ${new Date().toISOString()}\n`);

  const exchangeKeys = Object.keys(EXCHANGES);
  
  for (const key of exchangeKeys) {
    // Check time limit
    if (Date.now() - startTime > TEST_DURATION) {
      console.log(`\n⏰ TIME LIMIT REACHED (5 minutes). Stopping remaining tests.`);
      // Mark remaining as skipped
      for (const remaining of exchangeKeys.slice(exchangeKeys.indexOf(key))) {
        RESULTS[remaining] = {
          name: EXCHANGES[remaining].name,
          skipped: true,
          reason: 'Time limit reached',
          tests: {}
        };
      }
      break;
    }

    RESULTS[key] = await testExchange(key, EXCHANGES[key]);
    // Small delay between exchanges
    await new Promise(r => setTimeout(r, 300));
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  // ── GENERATE REPORT ──
  console.log(`\n\n${'═'.repeat(80)}`);
  console.log(`  📋 COMPREHENSIVE TEST RESULTS (${elapsed}s elapsed)`);
  console.log('═'.repeat(80));

  const categories = {
    working: [],
    partial: [],
    failed: [],
    skipped: [],
    restWorking: [],
    restFailed: []
  };

  for (const [key, result] of Object.entries(RESULTS)) {
    if (result.skipped) {
      categories.skipped.push({ key, ...result });
      continue;
    }
    
    const tests = Object.entries(result.tests || {});
    const passed = tests.filter(([, v]) => v.success).length;
    const total = tests.length;
    
    if (result.type === 'rest') {
      if (passed > 0) {
        categories.restWorking.push({ key, ...result, passed, total });
      } else {
        categories.restFailed.push({ key, ...result, passed, total });
      }
    } else if (total === 0) {
      categories.failed.push({ key, ...result, passed: 0, total: 0 });
    } else if (passed === total) {
      categories.working.push({ key, ...result, passed, total });
    } else if (passed > 0) {
      categories.partial.push({ key, ...result, passed, total });
    } else {
      categories.failed.push({ key, ...result, passed, total });
    }
  }

  console.log(`\n✅ FULLY WORKING WebSocket (${categories.working.length}):`);
  for (const ex of categories.working) {
    console.log(`   ${ex.name}: ${ex.passed}/${ex.total} streams passed`);
  }

  console.log(`\n🟡 PARTIALLY WORKING WebSocket (${categories.partial.length}):`);
  for (const ex of categories.partial) {
    const failedKeys = Object.entries(ex.tests).filter(([, v]) => !v.success).map(([k]) => k);
    console.log(`   ${ex.name}: ${ex.passed}/${ex.total} streams (failed: ${failedKeys.join(', ')})`);
  }

  console.log(`\n✅ REST API Working (${categories.restWorking.length}):`);
  for (const ex of categories.restWorking) {
    console.log(`   ${ex.name}: ${ex.passed}/${ex.total} endpoints`);
  }

  console.log(`\n❌ REST API Failed (${categories.restFailed.length}):`);
  for (const ex of categories.restFailed) {
    const errors = Object.entries(ex.tests).map(([k, v]) => `${k}: ${v.error}`);
    console.log(`   ${ex.name}: ${errors.join('; ')}`);
  }

  console.log(`\n❌ FAILED WebSocket (${categories.failed.length}):`);
  for (const ex of categories.failed) {
    const firstErr = Object.values(ex.tests || {})[0]?.error || 'No tests';
    console.log(`   ${ex.name}: ${firstErr}`);
  }

  console.log(`\n⏭️  SKIPPED (${categories.skipped.length}):`);
  for (const ex of categories.skipped) {
    console.log(`   ${ex.name}: ${ex.reason}`);
  }

  // ── Summary Table ──
  console.log(`\n${'─'.repeat(95)}`);
  console.log('┌' + '─'.repeat(3) + '┬' + '─'.repeat(20) + '┬' + '─'.repeat(8) + '┬' + '─'.repeat(9) + '┬' + '─'.repeat(10) + '┬' + '─'.repeat(12) + '┬' + '─'.repeat(28) + '┐');
  console.log('│ # │ Exchange            │ Type   │ Spot    │ Futures  │ Streams    │ Status                      │');
  console.log('├' + '─'.repeat(3) + '┼' + '─'.repeat(20) + '┼' + '─'.repeat(8) + '┼' + '─'.repeat(9) + '┼' + '─'.repeat(10) + '┼' + '─'.repeat(12) + '┼' + '─'.repeat(28) + '┤');

  let num = 0;
  for (const [key, result] of Object.entries(RESULTS)) {
    num++;
    const name = (result.name || key).padEnd(18).substring(0, 18);
    const type = (result.type || 'skip').padEnd(6);
    const spot = result.spot ? '✅' : '❌';
    const futures = result.futures ? '✅' : '❌';
    
    const tests = Object.entries(result.tests || {});
    const passed = tests.filter(([, v]) => v.success).length;
    const total = tests.length;
    const streams = total > 0 ? `${passed}/${total}` : '—';
    
    let status;
    if (result.skipped) status = `⏭️  ${(result.reason || '').substring(0, 23)}`;
    else if (total === 0) status = '❌ No tests';
    else if (passed === total) status = '✅ All passed';
    else if (passed > 0) status = `🟡 ${passed}/${total} passed`;
    else status = '❌ All failed';

    console.log(`│${String(num).padStart(2)} │ ${name} │ ${type} │ ${spot.padEnd(7)} │ ${futures.padEnd(8)} │ ${streams.padEnd(10)} │ ${status.padEnd(26)} │`);
  }

  console.log('└' + '─'.repeat(3) + '┴' + '─'.repeat(20) + '┴' + '─'.repeat(8) + '┴' + '─'.repeat(9) + '┴' + '─'.repeat(10) + '┴' + '─'.repeat(12) + '┴' + '─'.repeat(28) + '┘');

  // ── Final Stats ──
  const totalExchanges = Object.keys(RESULTS).length;
  const wsWorking = categories.working.length;
  const wsPartial = categories.partial.length;
  const restOk = categories.restWorking.length;
  const wsFailed = categories.failed.length;
  const restFail = categories.restFailed.length;
  const skipped = categories.skipped.length;
  const testable = totalExchanges - skipped;

  console.log(`\n📊 FINAL STATISTICS:`);
  console.log(`   Total Exchanges: ${totalExchanges}`);
  console.log(`   ✅ WS Fully Working: ${wsWorking}`);
  console.log(`   🟡 WS Partially Working: ${wsPartial}`);
  console.log(`   ✅ REST Working: ${restOk}`);
  console.log(`   ❌ WS Failed: ${wsFailed}`);
  console.log(`   ❌ REST Failed: ${restFail}`);
  console.log(`   ⏭️  Skipped (no API/dead): ${skipped}`);
  console.log(`   📈 Success Rate: ${((wsWorking + wsPartial + restOk) / Math.max(testable, 1) * 100).toFixed(1)}% of testable`);
  console.log(`\n⏱️  Total time: ${elapsed}s`);

  // Save results
  fs.writeFileSync('test-35-results.json', JSON.stringify(RESULTS, null, 2));
  console.log(`\n📁 Results saved to test-35-results.json`);

  // ── Generate detailed stream data ──
  console.log(`\n${'═'.repeat(80)}`);
  console.log(`  📄 DETAILED STREAM DATA SAMPLES`);
  console.log('═'.repeat(80));

  for (const [key, result] of Object.entries(RESULTS)) {
    if (result.skipped) continue;
    const successTests = Object.entries(result.tests || {}).filter(([, v]) => v.success);
    if (successTests.length === 0) continue;
    
    console.log(`\n  📊 ${result.name}:`);
    for (const [stream, data] of successTests) {
      console.log(`    ${stream}: ${(data.sample || '').substring(0, 120)}...`);
    }
  }

  console.log(`\n✅ All tests completed!`);
}

main().catch(console.error);
