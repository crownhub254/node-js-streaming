const WebSocket = require('ws');
const https = require('https');
const http = require('http');
const zlib = require('zlib');
const fs = require('fs');

// ═══════════════════════════════════════════════════════════════════════════
// CONFIRMED EXCHANGES - 5-MINUTE DEEP TEST
// Tests ONLY the 9 confirmed working exchanges
// Streams: Orderbook, Trades, Ticker ONLY (no kline)
// Duration: 5 minutes continuous collection
// ═══════════════════════════════════════════════════════════════════════════

const TEST_DURATION = 5 * 60 * 1000; // 5 minutes
const PER_STREAM_TIMEOUT = 20000;    // 20 seconds per stream test
const RESULTS = {};
const DATA_COUNTS = {};  // Track message counts over time

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
function httpGet(url, timeout = 20000) {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve({ success: false, error: 'HTTP timeout' }), timeout);
    const mod = url.startsWith('https') ? https : http;
    const req = mod.get(url, { headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' } }, (res) => {
      let body = '';
      res.on('data', (chunk) => body += chunk);
      res.on('end', () => {
        clearTimeout(timer);
        if (res.statusCode >= 200 && res.statusCode < 300) {
          let parsed = null;
          try { parsed = JSON.parse(body); } catch (e) {}
          resolve({ success: true, data: body.substring(0, 500), status: res.statusCode, parsed, size: body.length });
        } else {
          resolve({ success: false, error: `HTTP ${res.statusCode}`, data: body.substring(0, 200) });
        }
      });
    });
    req.on('error', (e) => { clearTimeout(timer); resolve({ success: false, error: e.message }); });
    req.end();
  });
}

// ── Helper: WebSocket stream test (extended for 5 min collection) ──
function testWSStream(name, url, options = {}) {
  return new Promise((resolve) => {
    const {
      onOpen, onMessage, pingInterval, pingPayload,
      timeout = PER_STREAM_TIMEOUT, headers = {},
      decompress: shouldDecompress = false,
      collectDuration = 0  // 0 = just verify, >0 = collect for N ms
    } = options;

    let firstDataReceived = false;
    let ws, pingTimer, timeoutTimer, collectTimer;
    let messageCount = 0;
    let dataMessages = 0;
    let firstSample = '';
    let lastSample = '';
    let errors = [];

    const cleanup = () => {
      if (timeoutTimer) clearTimeout(timeoutTimer);
      if (collectTimer) clearTimeout(collectTimer);
      if (pingTimer) clearInterval(pingTimer);
      try { if (ws && ws.readyState <= 1) ws.close(); } catch (e) {}
    };

    const done = (result) => {
      if (firstDataReceived && !collectDuration) {
        // Already resolved for non-collection mode
        return;
      }
      firstDataReceived = true;
      if (!collectDuration) {
        cleanup();
        resolve(result);
      }
    };

    try {
      ws = new WebSocket(url, { headers, handshakeTimeout: 10000 });
    } catch (e) {
      resolve({ success: false, error: `Connection failed: ${e.message}`, dataMessages: 0, messageCount: 0 });
      return;
    }

    timeoutTimer = setTimeout(() => {
      cleanup();
      resolve({
        success: dataMessages > 0,
        error: dataMessages === 0 ? `Timeout (${messageCount} non-data msgs received)` : undefined,
        firstSample, lastSample, dataMessages, messageCount, errors
      });
    }, collectDuration || timeout);

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
        if (result === 'error') {
          errors.push(msg.substring(0, 200));
          return;
        }
        if (result === 'data') {
          dataMessages++;
          if (!firstSample) firstSample = msg.substring(0, 400);
          lastSample = msg.substring(0, 400);

          if (!collectDuration && !firstDataReceived) {
            firstDataReceived = true;
            // Don't cleanup/resolve yet if collecting
          }
          return;
        }
      }

      // Default: check if it's meaningful data
      try {
        const parsed = JSON.parse(msg);
        if (parsed.pong || parsed.event === 'pong' || parsed.op === 'pong') return;
        if (parsed.result === 'pong' || parsed.ping) return;
        if (msg === 'pong' || msg === 'ping') return;
        if (parsed.event === 'subscribe' || parsed.event === 'sub') return;
        if (parsed.event === 'ping') return;
        if (parsed.event_rep) return;
        if (parsed.result && parsed.result.status === 'success' && !parsed.method) return;
        if (parsed.success === true && !parsed.data && !parsed.params) return;
        if (parsed.ret_msg === 'subscribe' || parsed.ret_msg === 'pong') return;
        if (parsed.code === 0 && parsed.msg === 'SUCCESS' && parsed.method === 'subscribe') return;
        if (parsed.status === 'ok' && !parsed.data && !parsed.tick && !parsed.ch && !parsed.topic) return;
        if (parsed.status === 'error') {
          errors.push(msg.substring(0, 200));
          return;
        }
        if (parsed.error && parsed.error !== null) {
          errors.push(JSON.stringify(parsed.error).substring(0, 200));
          return;
        }

        // Real data
        dataMessages++;
        if (!firstSample) firstSample = msg.substring(0, 400);
        lastSample = msg.substring(0, 400);
      } catch (e) {
        if (msg.length > 5 && msg !== 'pong' && msg !== 'ping') {
          dataMessages++;
          if (!firstSample) firstSample = msg.substring(0, 400);
          lastSample = msg.substring(0, 400);
        }
      }
    });

    ws.on('error', (err) => {
      errors.push(err.message);
      if (!firstDataReceived) {
        cleanup();
        resolve({ success: false, error: err.message, dataMessages, messageCount, errors });
      }
    });

    ws.on('close', (code) => {
      if (!firstDataReceived || collectDuration) {
        cleanup();
        resolve({
          success: dataMessages > 0,
          error: dataMessages === 0 ? `WS closed: ${code}` : undefined,
          firstSample, lastSample, dataMessages, messageCount, errors
        });
      }
    });
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// CONFIRMED EXCHANGE DEFINITIONS (only orderbook, trades, ticker)
// ═══════════════════════════════════════════════════════════════════════════

const EXCHANGES = {

  // ══════════════ WEBSOCKET EXCHANGES ══════════════

  biconomy: {
    name: 'Biconomy.com',
    type: 'ws',
    ws: 'wss://bei.biconomy.com/ws',
    ping: { method: 'server.ping', params: [], id: 5160 },
    pingInterval: 30000,
    streams: {
      orderbook: { method: 'depth.subscribe', params: ['BTC_USDT', 50, '0.01'], id: 1 },
      trades:    { method: 'deals.subscribe', params: ['BTC_USDT'], id: 2 },
      ticker:    { method: 'state.subscribe', params: ['BTC_USDT'], id: 3 }
    }
  },

  novaex: {
    name: 'NovaEx (WOO X)',
    type: 'ws',
    ws: 'wss://wss.woox.io/ws/stream/OqdphuyIYbng-t001',
    streams: {
      orderbook: { id: 'sub1', topic: 'SPOT_BTC_USDT@orderbook', event: 'subscribe' },
      trades:    { id: 'sub2', topic: 'SPOT_BTC_USDT@trade', event: 'subscribe' }
    }
  },

  xt: {
    name: 'XT.com',
    type: 'ws',
    ws: 'wss://stream.xt.com/public',
    wsFutures: 'wss://fstream.xt.com/ws/market',
    streams: {
      spot_orderbook: { method: 'subscribe', params: ['depth_update@btc_usdt'] },
      spot_trades:    { method: 'subscribe', params: ['trade@btc_usdt'] },
      spot_ticker:    { method: 'subscribe', params: ['ticker@btc_usdt'] }
    },
    futuresStreams: {
      futures_orderbook: { method: 'subscribe', params: ['depth_update@btc_usdt'] },
      futures_ticker:    { method: 'subscribe', params: ['ticker@btc_usdt'] }
    }
  },

  hotcoin: {
    name: 'Hotcoin.com',
    type: 'ws',
    ws: 'wss://wss.hotcoinfin.com/trade/multiple',
    wsAlt: 'wss://wss.hotcoin.top/trade/multiple',
    decompress: true,
    streams: {
      trades:    { sub: 'market.btc_usdt.trade.detail' }
    },
    // NOTE: Depth/detail subscriptions accepted but never deliver data
    // REST ticker available at: https://api.hotcoinfin.com/v1/market/ticker?symbol=btc_usdt
    restEndpoints: {
      ticker: 'https://api.hotcoinfin.com/v1/market/ticker?symbol=btc_usdt'
    },
    pingInterval: null,
    ping: null
  },

  zoomex: {
    name: 'Zoomex',
    type: 'ws',
    ws: 'wss://stream.zoomex.com/v5/public/spot',
    wsFutures: 'wss://stream.zoomex.com/v5/public/linear',
    streams: {
      spot_orderbook: { op: 'subscribe', args: ['orderbook.50.BTCUSDT'] },
      spot_trades:    { op: 'subscribe', args: ['publicTrade.BTCUSDT'] },
      spot_ticker:    { op: 'subscribe', args: ['tickers.BTCUSDT'] }
    },
    futuresStreams: {
      futures_orderbook: { op: 'subscribe', args: ['orderbook.50.BTCUSDT'] },
      futures_trades:    { op: 'subscribe', args: ['publicTrade.BTCUSDT'] },
      futures_ticker:    { op: 'subscribe', args: ['tickers.BTCUSDT'] }
    },
    pingInterval: 20000,
    ping: { op: 'ping' }
  },

  // ══════════════ REST API EXCHANGES ══════════════

  bullish: {
    name: 'Bullish.com',
    type: 'rest',
    endpoints: {
      orderbook: 'https://api.exchange.bullish.com/trading-api/v1/markets/BTCUSDT/orderbook/hybrid',
      trades:    'https://api.exchange.bullish.com/trading-api/v1/markets/BTCUSDT/trades'
    }
  },

  darkex: {
    name: 'Darkex.com',
    type: 'rest',
    endpoints: {
      orderbook: 'https://openapi.darkex.com/sapi/v1/depth?symbol=BTCUSDT&limit=10',
      trades:    'https://openapi.darkex.com/sapi/v1/trades?symbol=BTCUSDT&limit=10'
    }
  },

  bitrue: {
    name: 'Bitrue.com',
    type: 'rest',
    endpoints: {
      ticker:    'https://openapi.bitrue.com/api/v1/ticker/24hr?symbol=BTCUSDT',
      orderbook: 'https://openapi.bitrue.com/api/v1/depth?symbol=BTCUSDT&limit=10',
      trades:    'https://openapi.bitrue.com/api/v1/trades?symbol=BTCUSDT&limit=10'
    }
  },

  // SuperEx REMOVED - API now requires auth tokens (code:403 on all endpoints)

  // UZX REMOVED - API now returns HTML pages instead of JSON (website restructured)

  fameex: {
    name: 'FameEX.com',
    type: 'rest',
    endpoints: {
      ticker:    'https://api.fameex.com/v2/public/ticker',
      orderbook: 'https://api.fameex.com/v2/public/orderbook?symbol=BTCUSDT&limit=10'
    }
  },

  orangex: {
    name: 'OrangeX.com',
    type: 'rest',
    endpoints: {
      spot_trades:       'https://api.orangex.com/api/v1/public/get_last_trades_by_instrument?instrument_name=BTC-USDT-SPOT&count=10',
      futures_ticker:    'https://api.orangex.com/api/v1/public/ticker?instrument_name=BTC-USDT-PERPETUAL',
      futures_orderbook: 'https://api.orangex.com/api/v1/public/get_order_book?instrument_name=BTC-USDT-PERPETUAL&depth=10',
      futures_trades:    'https://api.orangex.com/api/v1/public/get_last_trades_by_instrument?instrument_name=BTC-USDT-PERPETUAL&count=10'
    }
  }
};

// ═══════════════════════════════════════════════════════════════════════════
// WS MESSAGE HANDLER (shared across all WS exchanges)
// ═══════════════════════════════════════════════════════════════════════════

function wsMessageHandler(msg, ws) {
  try {
    const parsed = JSON.parse(msg);
    // Pings - respond and skip
    if (parsed.ping) { ws.send(JSON.stringify({ pong: parsed.ping })); return 'skip'; }
    if (parsed.result === 'pong' || parsed.pong || parsed.ret_msg === 'pong' || parsed.op === 'pong') return 'skip';
    if (parsed.event === 'ping') return 'skip';
    // Subscription confirmations - skip
    if (parsed.error === null && parsed.result && parsed.result.status === 'success') return 'skip';
    if (parsed.success === true && parsed.ret_msg === 'subscribe') return 'skip';
    if (parsed.event === 'sub' || (parsed.subbed && !parsed.data)) return 'skip';
    if (parsed.status === 'ok' && parsed.subbed) return 'skip';
    if (parsed.event_rep) return 'skip';
    if (parsed.status === 'ok' && !parsed.data && !parsed.tick) return 'skip'; // Huobi/Hotcoin sub confirmations (has ch but no data/tick)
    if (parsed.event === 'subscribe' && parsed.success === true) return 'skip';
    if (parsed.code === 0 && parsed.msg === 'SUCCESS' && parsed.method === 'subscribe') return 'skip';
    if (parsed.code === 200 && parsed.msg === 'SUCCESS' && parsed.status === 'ok' && !parsed.data && !parsed.tick) return 'skip'; // Hotcoin sub confirmation
    // Errors
    if (parsed.status === 'error') return 'error';
    if (parsed.error && parsed.error !== null && !parsed.result) return 'error';
    // Data
    if (parsed.method && parsed.params) return 'data';
    if (parsed.data || parsed.tick || parsed.ch) return 'data';
    if (parsed.topic) return 'data';
    if (parsed.type && (parsed.asks || parsed.bids)) return 'data';
    if (parsed.result && (parsed.result.asks || parsed.result.bids || parsed.result.trades)) return 'data';
  } catch (e) {}
  return null;
}

// ═══════════════════════════════════════════════════════════════════════════
// TEST RUNNERS
// ═══════════════════════════════════════════════════════════════════════════

async function testRESTExchange(key, config) {
  const results = {};
  const pollInterval = 30000; // Poll every 30 seconds
  const pollCount = Math.floor(TEST_DURATION / pollInterval);

  // First pass: verify all endpoints work
  console.log(`  📡 Verifying REST endpoints...`);
  for (const [streamKey, url] of Object.entries(config.endpoints)) {
    process.stdout.write(`    ${streamKey}... `);
    const result = await httpGet(url);
    if (result.success) {
      console.log(`✅ HTTP ${result.status} (${result.size} bytes)`);

      // Validate data quality
      let dataQuality = 'unknown';
      if (result.parsed) {
        const p = result.parsed;
        if (p.jsonrpc) dataQuality = 'JSON-RPC 2.0';
        else if (Array.isArray(p)) dataQuality = `Array[${p.length}]`;
        else if (p.asks || p.bids) dataQuality = 'Orderbook';
        else if (p.code !== undefined) dataQuality = `code:${p.code}`;
        else if (p.result) dataQuality = 'Wrapped result';
        else dataQuality = 'Object';
      }

      results[streamKey] = {
        success: true,
        status: result.status,
        size: result.size,
        dataQuality,
        sample: result.data,
        pollResults: []
      };
    } else {
      console.log(`❌ ${result.error}`);
      results[streamKey] = { success: false, error: result.error, pollResults: [] };
    }
  }

  // Continuous polling for 5 minutes
  const workingEndpoints = Object.entries(results).filter(([, v]) => v.success);
  if (workingEndpoints.length > 0) {
    console.log(`  📊 Starting 5-minute polling (every 30s) for ${workingEndpoints.length} endpoints...`);
    const startTime = Date.now();
    let pollNum = 0;

    while (Date.now() - startTime < TEST_DURATION) {
      await new Promise(r => setTimeout(r, pollInterval));
      pollNum++;
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
      process.stdout.write(`    Poll #${pollNum} (${elapsed}s)... `);

      let pollOk = 0, pollFail = 0;
      for (const [streamKey, url] of Object.entries(config.endpoints)) {
        if (!results[streamKey]?.success) continue;
        const r = await httpGet(url);
        results[streamKey].pollResults.push({
          time: Date.now(),
          success: r.success,
          status: r.status,
          size: r.success ? r.size : undefined,
          error: r.success ? undefined : r.error
        });
        if (r.success) pollOk++; else pollFail++;
      }
      console.log(`${pollOk}✅ ${pollFail > 0 ? pollFail + '❌' : ''}`);
    }
  }

  return results;
}

async function testWSExchange(key, config) {
  const results = {};
  const collectDuration = TEST_DURATION; // Collect for full 5 minutes

  // Test spot streams - all connected simultaneously
  if (config.streams) {
    console.log(`  🔌 Connecting WebSocket: ${config.ws.substring(0, 55)}...`);

    // Connect one WS and subscribe to all streams at once for continuous collection
    const streamEntries = Object.entries(config.streams);
    const wsResults = await new Promise((resolve) => {
      const streamData = {};
      for (const [k] of streamEntries) {
        streamData[k] = { dataMessages: 0, firstSample: '', lastSample: '', errors: [], firstDataTime: null, lastDataTime: null };
      }

      let ws, pingTimer;
      const cleanup = () => {
        if (pingTimer) clearInterval(pingTimer);
        try { if (ws && ws.readyState <= 1) ws.close(); } catch (e) {}
      };

      const timer = setTimeout(() => {
        cleanup();
        resolve(streamData);
      }, collectDuration);

      // Progress reporter
      const progressTimer = setInterval(() => {
        const counts = Object.entries(streamData).map(([k, v]) => `${k}:${v.dataMessages}`).join(' | ');
        const elapsed = ((Date.now() - startTs) / 1000).toFixed(0);
        process.stdout.write(`\r    [${elapsed}s] ${counts}          `);
      }, 10000);

      const startTs = Date.now();

      try {
        ws = new WebSocket(config.ws, { handshakeTimeout: 10000 });
      } catch (e) {
        clearTimeout(timer);
        clearInterval(progressTimer);
        for (const k of Object.keys(streamData)) {
          streamData[k].errors.push(`Connection failed: ${e.message}`);
        }
        resolve(streamData);
        return;
      }

      ws.on('open', () => {
        log(config.name, `Connected! Subscribing to ${streamEntries.length} streams...`);

        // Ping setup
        if (config.pingInterval && config.ping) {
          pingTimer = setInterval(() => {
            try {
              const payload = typeof config.ping === 'function' ? config.ping() : config.ping;
              ws.send(typeof payload === 'string' ? payload : JSON.stringify(payload));
            } catch (e) {}
          }, config.pingInterval);
        }

        // Subscribe to all streams
        for (const [, sub] of streamEntries) {
          ws.send(JSON.stringify(sub));
        }
      });

      ws.on('message', (data) => {
        let msg;
        try {
          msg = config.decompress ? decompress(data) : (Buffer.isBuffer(data) ? data.toString() : data.toString());
        } catch (e) { msg = data.toString(); }

        const result = wsMessageHandler(msg, ws);
        if (result === 'skip') return;

        if (result === 'data' || result === null) {
          // Try to identify which stream this belongs to
          try {
            const parsed = JSON.parse(msg);
            let matched = false;

            for (const [streamKey, sub] of streamEntries) {
              // Match by various patterns
              if (matchMessageToStream(parsed, sub, streamKey)) {
                streamData[streamKey].dataMessages++;
                if (!streamData[streamKey].firstSample) {
                  streamData[streamKey].firstSample = msg.substring(0, 400);
                  streamData[streamKey].firstDataTime = Date.now();
                }
                streamData[streamKey].lastSample = msg.substring(0, 400);
                streamData[streamKey].lastDataTime = Date.now();
                matched = true;
                break;
              }
            }

            if (!matched) {
              // Assign to first stream if can't match
              const firstKey = streamEntries[0][0];
              streamData[firstKey].dataMessages++;
              if (!streamData[firstKey].firstSample) {
                streamData[firstKey].firstSample = msg.substring(0, 400);
                streamData[firstKey].firstDataTime = Date.now();
              }
              streamData[firstKey].lastSample = msg.substring(0, 400);
              streamData[firstKey].lastDataTime = Date.now();
            }
          } catch (e) {
            const firstKey = streamEntries[0][0];
            streamData[firstKey].dataMessages++;
          }
        }

        if (result === 'error') {
          const firstKey = streamEntries[0][0];
          streamData[firstKey].errors.push(msg.substring(0, 200));
        }
      });

      ws.on('error', (err) => {
        log(config.name, `Error: ${err.message}`);
        for (const k of Object.keys(streamData)) {
          streamData[k].errors.push(err.message);
        }
      });

      ws.on('close', (code) => {
        log(config.name, `Connection closed: ${code}`);
        clearTimeout(timer);
        clearInterval(progressTimer);
        resolve(streamData);
      });
    });

    // Process results
    for (const [streamKey, data] of Object.entries(wsResults)) {
      results[streamKey] = {
        success: data.dataMessages > 0,
        dataMessages: data.dataMessages,
        firstSample: data.firstSample,
        lastSample: data.lastSample,
        errors: data.errors,
        firstDataTime: data.firstDataTime,
        lastDataTime: data.lastDataTime,
        duration: data.lastDataTime && data.firstDataTime ? ((data.lastDataTime - data.firstDataTime) / 1000).toFixed(1) + 's' : '0s'
      };
    }
    console.log(''); // Newline after progress output
  }

  // Test futures streams on separate WS endpoint
  if (config.futuresStreams && config.wsFutures) {
    const futuresEntries = Object.entries(config.futuresStreams);
    console.log(`  🔌 Connecting Futures WS: ${config.wsFutures.substring(0, 55)}...`);

    const futResults = await new Promise((resolve) => {
      const streamData = {};
      for (const [k] of futuresEntries) {
        streamData[k] = { dataMessages: 0, firstSample: '', lastSample: '', errors: [], firstDataTime: null, lastDataTime: null };
      }

      let ws, pingTimer;
      const cleanup = () => {
        if (pingTimer) clearInterval(pingTimer);
        try { if (ws && ws.readyState <= 1) ws.close(); } catch (e) {}
      };

      const timer = setTimeout(() => {
        cleanup();
        resolve(streamData);
      }, collectDuration);

      const progressTimer = setInterval(() => {
        const counts = Object.entries(streamData).map(([k, v]) => `${k}:${v.dataMessages}`).join(' | ');
        const elapsed = ((Date.now() - startTs) / 1000).toFixed(0);
        process.stdout.write(`\r    [${elapsed}s] ${counts}          `);
      }, 10000);

      const startTs = Date.now();

      try {
        ws = new WebSocket(config.wsFutures, { handshakeTimeout: 10000 });
      } catch (e) {
        clearTimeout(timer);
        clearInterval(progressTimer);
        for (const k of Object.keys(streamData)) streamData[k].errors.push(`Connection failed: ${e.message}`);
        resolve(streamData);
        return;
      }

      ws.on('open', () => {
        log(config.name, `Futures connected! Subscribing to ${futuresEntries.length} streams...`);
        if (config.pingInterval && config.ping) {
          pingTimer = setInterval(() => {
            try {
              const payload = typeof config.ping === 'function' ? config.ping() : config.ping;
              ws.send(typeof payload === 'string' ? payload : JSON.stringify(payload));
            } catch (e) {}
          }, config.pingInterval);
        }
        for (const [, sub] of futuresEntries) ws.send(JSON.stringify(sub));
      });

      ws.on('message', (data) => {
        let msg;
        try {
          msg = config.decompress ? decompress(data) : (Buffer.isBuffer(data) ? data.toString() : data.toString());
        } catch (e) { msg = data.toString(); }

        const result = wsMessageHandler(msg, ws);
        if (result === 'skip') return;

        if (result === 'data' || result === null) {
          try {
            const parsed = JSON.parse(msg);
            let matched = false;
            for (const [streamKey, sub] of futuresEntries) {
              if (matchMessageToStream(parsed, sub, streamKey)) {
                streamData[streamKey].dataMessages++;
                if (!streamData[streamKey].firstSample) {
                  streamData[streamKey].firstSample = msg.substring(0, 400);
                  streamData[streamKey].firstDataTime = Date.now();
                }
                streamData[streamKey].lastSample = msg.substring(0, 400);
                streamData[streamKey].lastDataTime = Date.now();
                matched = true;
                break;
              }
            }
            if (!matched) {
              const firstKey = futuresEntries[0][0];
              streamData[firstKey].dataMessages++;
              if (!streamData[firstKey].firstSample) {
                streamData[firstKey].firstSample = msg.substring(0, 400);
                streamData[firstKey].firstDataTime = Date.now();
              }
              streamData[firstKey].lastSample = msg.substring(0, 400);
              streamData[firstKey].lastDataTime = Date.now();
            }
          } catch (e) {
            const firstKey = futuresEntries[0][0];
            streamData[firstKey].dataMessages++;
          }
        }
        if (result === 'error') {
          const firstKey = futuresEntries[0][0];
          streamData[firstKey].errors.push(msg.substring(0, 200));
        }
      });

      ws.on('error', (err) => {
        log(config.name, `Futures Error: ${err.message}`);
        for (const k of Object.keys(streamData)) streamData[k].errors.push(err.message);
      });

      ws.on('close', (code) => {
        log(config.name, `Futures closed: ${code}`);
        clearTimeout(timer);
        clearInterval(progressTimer);
        resolve(streamData);
      });
    });

    for (const [streamKey, data] of Object.entries(futResults)) {
      results[streamKey] = {
        success: data.dataMessages > 0,
        dataMessages: data.dataMessages,
        firstSample: data.firstSample,
        lastSample: data.lastSample,
        errors: data.errors,
        firstDataTime: data.firstDataTime,
        lastDataTime: data.lastDataTime,
        duration: data.lastDataTime && data.firstDataTime ? ((data.lastDataTime - data.firstDataTime) / 1000).toFixed(1) + 's' : '0s'
      };
    }
    console.log('');
  }

  // Test supplementary REST endpoints (for exchanges that are primarily WS but have some REST endpoints)
  if (config.restEndpoints) {
    console.log(`  📡 Testing supplementary REST endpoints...`);
    for (const [streamKey, url] of Object.entries(config.restEndpoints)) {
      process.stdout.write(`    ${streamKey}... `);
      const result = await httpGet(url);
      if (result.success) {
        console.log(`✅ HTTP ${result.status} (${result.size} bytes)`);
        results[`rest_${streamKey}`] = {
          success: true,
          dataMessages: 1,
          firstSample: result.data,
          lastSample: result.data,
          errors: [],
          duration: 'REST'
        };
      } else {
        console.log(`❌ ${result.error}`);
        results[`rest_${streamKey}`] = {
          success: false,
          dataMessages: 0,
          errors: [result.error],
          duration: '0s'
        };
      }
    }
  }

  return results;
}
function matchMessageToStream(parsed, sub, streamKey) {
  const key = streamKey.toLowerCase();

  // Biconomy style: method contains stream type
  if (parsed.method) {
    if (key.includes('orderbook') && (parsed.method.includes('depth') || parsed.method.includes('order'))) return true;
    if (key.includes('trade') && (parsed.method.includes('deal') || parsed.method.includes('trade'))) return true;
    if (key.includes('ticker') && (parsed.method.includes('state') || parsed.method.includes('ticker'))) return true;
  }

  // Topic-based (Bybit/Zoomex/WOO X style)
  if (parsed.topic) {
    if (key.includes('orderbook') && (parsed.topic.includes('orderbook') || parsed.topic.includes('depth'))) return true;
    if (key.includes('trade') && (parsed.topic.includes('trade') || parsed.topic.includes('Trade'))) return true;
    if (key.includes('ticker') && (parsed.topic.includes('ticker') || parsed.topic.includes('tickers'))) return true;
  }

  // Event-based (XT.com style)
  if (parsed.event) {
    if (key.includes('orderbook') && parsed.event.includes('depth')) return true;
    if (key.includes('trade') && parsed.event.includes('trade')) return true;
    if (key.includes('ticker') && parsed.event.includes('ticker')) return true;
  }

  // Channel-based (Huobi/Hotcoin style)
  if (parsed.ch) {
    if (key.includes('orderbook') && parsed.ch.includes('depth')) return true;
    if (key.includes('trade') && parsed.ch.includes('trade')) return true;
    if (key.includes('ticker') && parsed.ch.includes('detail') && !parsed.ch.includes('trade')) return true;
  }

  return false;
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════

async function main() {
  const startTime = Date.now();

  console.log(`
╔═══════════════════════════════════════════════════════════════════════════╗
║  CONFIRMED EXCHANGES - 5-MINUTE DEEP TEST                                ║
║  12 Exchanges | Orderbook + Trades + Ticker Only                          ║
║  Duration: 5 minutes continuous collection                                ║
╚═══════════════════════════════════════════════════════════════════════════╝
  `);
  console.log(`⏱️  Started at ${new Date().toISOString()}`);
  console.log(`📋 Testing ${Object.keys(EXCHANGES).length} confirmed exchanges\n`);

  // Run WS exchanges in parallel (they all collect for 5 min)
  const wsExchanges = Object.entries(EXCHANGES).filter(([, c]) => c.type === 'ws');
  const restExchanges = Object.entries(EXCHANGES).filter(([, c]) => c.type === 'rest');

  console.log(`${'═'.repeat(70)}`);
  console.log(`  🔌 WEBSOCKET EXCHANGES (${wsExchanges.length}) - Running in parallel for 5 min`);
  console.log(`${'═'.repeat(70)}`);

  // Start all WS tests in parallel
  const wsPromises = {};
  for (const [key, config] of wsExchanges) {
    console.log(`\n  📊 ${config.name}`);
    wsPromises[key] = testWSExchange(key, config);
  }

  // Wait for all WS to complete
  const wsKeys = Object.keys(wsPromises);
  const wsResultsArr = await Promise.all(Object.values(wsPromises));
  for (let i = 0; i < wsKeys.length; i++) {
    RESULTS[wsKeys[i]] = {
      name: EXCHANGES[wsKeys[i]].name,
      type: 'ws',
      tests: wsResultsArr[i]
    };
  }

  console.log(`\n${'═'.repeat(70)}`);
  console.log(`  📡 REST API EXCHANGES (${restExchanges.length}) - Polling every 30s for 5 min`);
  console.log(`${'═'.repeat(70)}`);

  // Run REST exchanges sequentially (they each poll for 5 min)
  // Actually run them in parallel too since they're independent
  const restPromises = {};
  for (const [key, config] of restExchanges) {
    console.log(`\n  📊 ${config.name}`);
    restPromises[key] = testRESTExchange(key, config);
  }

  const restKeys = Object.keys(restPromises);
  const restResultsArr = await Promise.all(Object.values(restPromises));
  for (let i = 0; i < restKeys.length; i++) {
    RESULTS[restKeys[i]] = {
      name: EXCHANGES[restKeys[i]].name,
      type: 'rest',
      tests: restResultsArr[i]
    };
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  // ═══════════════════════════════════════════════════════════════════════
  // RESULTS REPORT
  // ═══════════════════════════════════════════════════════════════════════

  console.log(`\n\n${'═'.repeat(80)}`);
  console.log(`  📋 5-MINUTE DEEP TEST RESULTS (${elapsed}s elapsed)`);
  console.log(`${'═'.repeat(80)}`);

  // WS Results
  console.log(`\n  ── WEBSOCKET EXCHANGES ──`);
  for (const [key, result] of Object.entries(RESULTS)) {
    if (result.type !== 'ws') continue;
    console.log(`\n  📊 ${result.name}:`);
    const tests = Object.entries(result.tests || {});
    let totalMsgs = 0;
    for (const [stream, data] of tests) {
      const status = data.success ? '✅' : '❌';
      const msgs = data.dataMessages || 0;
      totalMsgs += msgs;
      const dur = data.duration || '0s';
      const errs = (data.errors || []).length;
      console.log(`    ${status} ${stream.padEnd(20)} │ ${String(msgs).padStart(6)} msgs │ ${dur.padStart(8)} │ ${errs > 0 ? `⚠️ ${errs} errors` : 'clean'}`);
      if (data.errors && data.errors.length > 0) {
        for (const err of data.errors.slice(0, 2)) {
          console.log(`      └─ Error: ${err.substring(0, 100)}`);
        }
      }
    }
    console.log(`    ─── Total: ${totalMsgs} data messages received`);
  }

  // REST Results
  console.log(`\n  ── REST API EXCHANGES ──`);
  for (const [key, result] of Object.entries(RESULTS)) {
    if (result.type !== 'rest') continue;
    console.log(`\n  📊 ${result.name}:`);
    const tests = Object.entries(result.tests || {});
    for (const [stream, data] of tests) {
      const status = data.success ? '✅' : '❌';
      if (data.success) {
        const polls = data.pollResults || [];
        const successPolls = polls.filter(p => p.success).length;
        const failPolls = polls.filter(p => !p.success).length;
        const reliability = polls.length > 0 ? ((successPolls / polls.length) * 100).toFixed(0) : '100';
        console.log(`    ${status} ${stream.padEnd(20)} │ ${data.dataQuality.padEnd(15)} │ ${data.size}B │ polls: ${successPolls}✅/${failPolls}❌ │ ${reliability}% reliable`);
        if (failPolls > 0) {
          const failErrors = polls.filter(p => !p.success).slice(0, 2);
          for (const fe of failErrors) {
            console.log(`      └─ Poll fail: ${fe.error}`);
          }
        }
      } else {
        console.log(`    ${status} ${stream.padEnd(20)} │ ${data.error}`);
      }
    }
  }

  // ── SUMMARY TABLE ──
  console.log(`\n${'═'.repeat(90)}`);
  console.log('┌' + '─'.repeat(3) + '┬' + '─'.repeat(20) + '┬' + '─'.repeat(8) + '┬' + '─'.repeat(12) + '┬' + '─'.repeat(12) + '┬' + '─'.repeat(12) + '┬' + '─'.repeat(18) + '┐');
  console.log('│ # │ Exchange            │ Type   │ Streams    │ Msgs/Data  │ Errors     │ Verdict          │');
  console.log('├' + '─'.repeat(3) + '┼' + '─'.repeat(20) + '┼' + '─'.repeat(8) + '┼' + '─'.repeat(12) + '┼' + '─'.repeat(12) + '┼' + '─'.repeat(12) + '┼' + '─'.repeat(18) + '┤');

  let num = 0;
  let totalWorking = 0, totalPartial = 0, totalFailed = 0;

  for (const [key, result] of Object.entries(RESULTS)) {
    num++;
    const name = result.name.padEnd(18).substring(0, 18);
    const type = result.type.padEnd(6);
    const tests = Object.entries(result.tests || {});
    const passed = tests.filter(([, v]) => v.success).length;
    const total = tests.length;
    const streams = `${passed}/${total}`;

    let totalMsgs = 0;
    let totalErrors = 0;
    for (const [, data] of tests) {
      if (result.type === 'ws') {
        totalMsgs += data.dataMessages || 0;
      } else {
        totalMsgs += (data.pollResults || []).filter(p => p.success).length + (data.success ? 1 : 0);
      }
      totalErrors += (data.errors || []).length;
      if (!data.success) totalErrors++;
      // Count REST poll failures
      if (data.pollResults) totalErrors += data.pollResults.filter(p => !p.success).length;
    }

    let verdict;
    if (passed === total && totalErrors === 0) {
      verdict = '✅ Perfect';
      totalWorking++;
    } else if (passed === total) {
      verdict = '🟡 Minor issues';
      totalPartial++;
    } else if (passed > 0) {
      verdict = `🟡 ${passed}/${total} working`;
      totalPartial++;
    } else {
      verdict = '❌ Failed';
      totalFailed++;
    }

    console.log(`│${String(num).padStart(2)} │ ${name} │ ${type} │ ${streams.padEnd(10)} │ ${String(totalMsgs).padStart(10)} │ ${String(totalErrors).padStart(10)} │ ${verdict.padEnd(16)} │`);
  }

  console.log('└' + '─'.repeat(3) + '┴' + '─'.repeat(20) + '┴' + '─'.repeat(8) + '┴' + '─'.repeat(12) + '┴' + '─'.repeat(12) + '┴' + '─'.repeat(12) + '┴' + '─'.repeat(18) + '┘');

  console.log(`\n📊 FINAL VERDICT:`);
  console.log(`   ✅ Perfect (all streams, no errors): ${totalWorking}`);
  console.log(`   🟡 Partial (some issues): ${totalPartial}`);
  console.log(`   ❌ Failed: ${totalFailed}`);
  console.log(`   📈 Health Rate: ${((totalWorking + totalPartial) / Object.keys(RESULTS).length * 100).toFixed(1)}%`);
  console.log(`\n⏱️  Total test time: ${elapsed}s`);

  // Save results
  fs.writeFileSync('confirmed-test-results.json', JSON.stringify(RESULTS, null, 2));
  console.log(`📁 Results saved to confirmed-test-results.json`);

  console.log(`\n✅ 5-minute deep test completed!`);
}

main().catch(console.error);
