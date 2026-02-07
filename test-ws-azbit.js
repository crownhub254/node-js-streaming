#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════
// FameEX WS + Websea WS + Azbit REST — Deep Research Tester
// ═══════════════════════════════════════════════════════════════

const https = require('https');
const WebSocket = require('ws');

// ─── HTTP GET helper ───
function httpGet(url, timeout = 10000) {
  return new Promise(resolve => {
    const start = Date.now();
    const req = https.get(url, { timeout }, res => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        const elapsed = Date.now() - start;
        let parsed = null;
        try { parsed = JSON.parse(data); } catch(e) {}
        resolve({ url, status: res.statusCode, elapsed, parsed, raw: data.substring(0, 600) });
      });
    });
    req.on('error', err => resolve({ url, error: err.message, elapsed: Date.now() - start }));
    req.on('timeout', () => { req.destroy(); resolve({ url, error: 'TIMEOUT', elapsed: timeout }); });
  });
}

// ─── WebSocket tester ───
function testWS(label, wsUrl, subscriptions, timeout = 12000) {
  return new Promise(resolve => {
    const result = {
      label,
      url: wsUrl,
      connected: false,
      messages: [],
      dataMessages: [],
      errors: [],
      elapsed: 0
    };
    const start = Date.now();
    let ws;
    
    try {
      ws = new WebSocket(wsUrl, {
        headers: {
          'User-Agent': 'Mozilla/5.0',
          'Origin': 'https://www.fameex.com'
        }
      });
    } catch(e) {
      result.errors.push(`Constructor error: ${e.message}`);
      resolve(result);
      return;
    }

    const timer = setTimeout(() => {
      result.elapsed = Date.now() - start;
      try { ws.close(); } catch(e) {}
      resolve(result);
    }, timeout);

    ws.on('open', () => {
      result.connected = true;
      // Send subscriptions
      for (const sub of subscriptions) {
        try {
          ws.send(JSON.stringify(sub));
        } catch(e) {
          result.errors.push(`Send error: ${e.message}`);
        }
      }
    });

    ws.on('message', (msg) => {
      let data;
      // Handle binary (gzip)
      if (Buffer.isBuffer(msg)) {
        try {
          const zlib = require('zlib');
          data = zlib.gunzipSync(msg).toString();
        } catch(e) {
          try {
            const zlib = require('zlib');
            data = zlib.inflateSync(msg).toString();
          } catch(e2) {
            data = msg.toString();
          }
        }
      } else {
        data = msg.toString();
      }

      let parsed;
      try { parsed = JSON.parse(data); } catch(e) { parsed = null; }
      
      // Handle pings
      if (parsed) {
        if (parsed.ping) {
          ws.send(JSON.stringify({ pong: parsed.ping }));
          return;
        }
        if (parsed.action === 'ping' || parsed.type === 'ping') {
          ws.send(JSON.stringify({ action: 'pong' }));
          return;
        }
      }

      const entry = {
        raw: data.substring(0, 500),
        parsed: parsed ? JSON.stringify(parsed).substring(0, 500) : null,
        isData: false
      };

      // Check if this is actual market data (not just subscription confirmation)
      if (parsed) {
        const str = JSON.stringify(parsed);
        if (str.includes('asks') || str.includes('bids') || str.includes('price') || str.includes('trades') || 
            str.includes('depth') || str.includes('amount') || str.includes('vol') || str.includes('deal')) {
          entry.isData = true;
          result.dataMessages.push(entry);
        }
      }
      
      result.messages.push(entry);
      
      // If we got 5+ data messages, we can close early
      if (result.dataMessages.length >= 5) {
        result.elapsed = Date.now() - start;
        clearTimeout(timer);
        try { ws.close(); } catch(e) {}
        resolve(result);
      }
    });

    ws.on('error', err => {
      result.errors.push(err.message);
    });

    ws.on('close', (code, reason) => {
      result.closeCode = code;
      result.closeReason = reason?.toString();
      result.elapsed = Date.now() - start;
      clearTimeout(timer);
      resolve(result);
    });
  });
}

function printWSResult(r) {
  const icon = r.dataMessages.length > 0 ? '✅' : (r.connected ? '⚠️' : '❌');
  console.log(`\n${icon} ${r.label}`);
  console.log(`   URL: ${r.url}`);
  console.log(`   Connected: ${r.connected} | ${r.elapsed}ms | ${r.messages.length} msgs | ${r.dataMessages.length} data msgs`);
  if (r.errors.length > 0) console.log(`   Errors: ${r.errors.join(', ')}`);
  if (r.closeCode) console.log(`   Close: code=${r.closeCode}, reason=${r.closeReason || 'none'}`);
  
  // Show first 2 non-data messages (subscription confirmations etc)
  const nonData = r.messages.filter(m => !m.isData);
  if (nonData.length > 0) {
    console.log(`   First response: ${nonData[0].raw.substring(0, 300)}`);
  }
  
  // Show first 2 data messages
  if (r.dataMessages.length > 0) {
    console.log(`   📊 Data samples:`);
    r.dataMessages.slice(0, 2).forEach((m, i) => {
      console.log(`   [${i+1}] ${m.raw.substring(0, 300)}`);
    });
  }
}

function printRESTResult(label, r) {
  const hasData = r.parsed && !r.error && r.status === 200;
  const icon = hasData ? '✅' : '❌';
  console.log(`\n${icon} ${label}`);
  console.log(`   URL: ${r.url}`);
  if (r.error) { console.log(`   Error: ${r.error}`); return; }
  console.log(`   Status: ${r.status} | ${r.elapsed}ms`);
  console.log(`   Data: ${r.raw.substring(0, 400)}`);
}

async function main() {
  console.log('╔═══════════════════════════════════════════════════════════════════╗');
  console.log('║  FameEX WS + Websea WS + Azbit REST — Deep Research Test        ║');
  console.log('╚═══════════════════════════════════════════════════════════════════╝');

  // ═══════════════════════════════════════════════════════════
  // 1. FAMEEX WEBSOCKET TESTS
  // ═══════════════════════════════════════════════════════════
  console.log('\n' + '═'.repeat(70));
  console.log('  1. FAMEEX WEBSOCKET STREAMS');
  console.log('═'.repeat(70));

  // Test multiple WS URLs and subscription formats
  const fameexTests = [
    {
      label: 'FameEX WS (wsapi.fameex.com/v1/ws/stream/public)',
      url: 'wss://wsapi.fameex.com/v1/ws/stream/public',
      subs: [
        { method: 'SUBSCRIBE', params: ['BTCUSDT@depth'] },
        { method: 'SUBSCRIBE', params: ['BTCUSDT@trade'] }
      ]
    },
    {
      label: 'FameEX WS (www.fameex.com/push)',
      url: 'wss://www.fameex.com/push',
      subs: [
        { method: 'SUBSCRIBE', params: ['BTCUSDT@depth'] },
        { method: 'SUBSCRIBE', params: ['BTCUSDT@trade'] }
      ]
    },
    {
      label: 'FameEX WS (wsapi depth@/trades@)',
      url: 'wss://wsapi.fameex.com/v1/ws/stream/public',
      subs: [
        { method: 'SUBSCRIBE', params: ['depth@BTCUSDT'] },
        { method: 'SUBSCRIBE', params: ['trades@BTCUSDT'] }
      ]
    },
    {
      label: 'FameEX WS (wsapi sub method)',
      url: 'wss://wsapi.fameex.com/v1/ws/stream/public',
      subs: [
        { sub: 'depth', symbol: 'BTCUSDT' },
        { sub: 'trade', symbol: 'BTCUSDT' }
      ]
    },
    {
      label: 'FameEX WS (wsapi op:subscribe)',
      url: 'wss://wsapi.fameex.com/v1/ws/stream/public',
      subs: [
        { op: 'subscribe', args: ['spot.depth.BTCUSDT'] },
        { op: 'subscribe', args: ['spot.trade.BTCUSDT'] }
      ]
    },
    {
      label: 'FameEX WS (wsapi channel format)',
      url: 'wss://wsapi.fameex.com/v1/ws/stream/public',
      subs: [
        { event: 'sub', channel: 'market.BTCUSDT.depth' },
        { event: 'sub', channel: 'market.BTCUSDT.trade' }
      ]
    },
    // Alternative URLs
    {
      label: 'FameEX WS (ws.fameex.com)',
      url: 'wss://ws.fameex.com',
      subs: [
        { method: 'SUBSCRIBE', params: ['BTCUSDT@depth'] }
      ]
    },
    {
      label: 'FameEX WS (api.fameex.com/ws)',
      url: 'wss://api.fameex.com/ws',
      subs: [
        { method: 'SUBSCRIBE', params: ['BTCUSDT@depth'] }
      ]
    },
    {
      label: 'FameEX WS (api.fameex.com/ws/v1)',
      url: 'wss://api.fameex.com/ws/v1',
      subs: [
        { method: 'SUBSCRIBE', params: ['BTCUSDT@depth'] }
      ]
    },
    {
      label: 'FameEX WS (openapi.fameex.com/ws)',
      url: 'wss://openapi.fameex.com/ws',
      subs: [
        { method: 'SUBSCRIBE', params: ['BTCUSDT@depth'] }
      ]
    },
    {
      label: 'FameEX WS (wsapi no path)',
      url: 'wss://wsapi.fameex.com',
      subs: [
        { method: 'SUBSCRIBE', params: ['BTCUSDT@depth'] }
      ]
    },
    {
      label: 'FameEX WS (wsapi /ws)',
      url: 'wss://wsapi.fameex.com/ws',
      subs: [
        { method: 'SUBSCRIBE', params: ['BTCUSDT@depth'] }
      ]
    },
  ];

  for (const test of fameexTests) {
    const r = await testWS(test.label, test.url, test.subs);
    printWSResult(r);
  }

  // ═══════════════════════════════════════════════════════════
  // 2. WEBSEA WEBSOCKET TESTS
  // ═══════════════════════════════════════════════════════════
  console.log('\n' + '═'.repeat(70));
  console.log('  2. WEBSEA WEBSOCKET STREAMS');
  console.log('═'.repeat(70));

  const webseaTests = [
    {
      label: 'Websea WS (oapi.websea.com - Binance-style)',
      url: 'wss://oapi.websea.com',
      subs: [
        { method: 'SUBSCRIBE', params: ['BTC-USDT@depth', 'BTC-USDT@trade'] }
      ]
    },
    {
      label: 'Websea WS (oapi.websea.com - sub method)',
      url: 'wss://oapi.websea.com',
      subs: [
        { sub: 'market.BTC-USDT.depth' },
        { sub: 'market.BTC-USDT.trade.detail' }
      ]
    },
    {
      label: 'Websea WS (oapi.websea.com - op subscribe)',
      url: 'wss://oapi.websea.com',
      subs: [
        { op: 'subscribe', args: ['spot.orderbook.BTC-USDT'] },
        { op: 'subscribe', args: ['spot.trade.BTC-USDT'] }
      ]
    },
    {
      label: 'Websea WS (oapi.websea.com - channel format)',
      url: 'wss://oapi.websea.com',
      subs: [
        { event: 'sub', params: { channel: 'orderbook', symbol: 'BTC-USDT' } },
        { event: 'sub', params: { channel: 'trade', symbol: 'BTC-USDT' } }
      ]
    },
    // Try ws subdomain  
    {
      label: 'Websea WS (ws.websea.com)',
      url: 'wss://ws.websea.com',
      subs: [
        { method: 'SUBSCRIBE', params: ['BTC-USDT@depth'] }
      ]
    },
    {
      label: 'Websea WS (ws.websea.com/kline-api/ws)',
      url: 'wss://ws.websea.com/kline-api/ws',
      subs: [
        { event: 'sub', params: { channel: 'market_BTC-USDT_depth_step0' } }
      ]
    },
    {
      label: 'Websea WS (oapi.websea.com/ws)',
      url: 'wss://oapi.websea.com/ws',
      subs: [
        { method: 'SUBSCRIBE', params: ['BTC-USDT@depth'] }
      ]
    },
    {
      label: 'Websea WS (oapi.websea.com/ws/v1/spot)',
      url: 'wss://oapi.websea.com/ws/v1/spot',
      subs: [
        { method: 'SUBSCRIBE', params: ['BTC-USDT@depth'] }
      ]
    },
  ];

  for (const test of webseaTests) {
    const r = await testWS(test.label, test.url, test.subs);
    printWSResult(r);
  }

  // ═══════════════════════════════════════════════════════════
  // 3. AZBIT REST API TESTS
  // ═══════════════════════════════════════════════════════════
  console.log('\n' + '═'.repeat(70));
  console.log('  3. AZBIT REST API');
  console.log('═'.repeat(70));

  const azbitTests = [
    ['Azbit Orderbook (BTC_USDT)', 'https://data.azbit.com/api/orderbook?currencyPairCode=BTC_USDT'],
    ['Azbit Deals/Trades (BTC_USDT)', 'https://data.azbit.com/api/deals?currencyPairCode=BTC_USDT'],
    ['Azbit Orderbook (BTCUSDT)', 'https://data.azbit.com/api/orderbook?currencyPairCode=BTCUSDT'],
    ['Azbit Ticker', 'https://data.azbit.com/api/tickers'],
    ['Azbit Currencies', 'https://data.azbit.com/api/currencies'],
    // Try alternative paths
    ['Azbit v1 Orderbook', 'https://data.azbit.com/api/v1/orderbook?currencyPairCode=BTC_USDT'],
    ['Azbit Trades alt', 'https://data.azbit.com/api/trades?currencyPairCode=BTC_USDT'],
    // Main domain
    ['Azbit Main Orderbook', 'https://api.azbit.com/api/orderbook?currencyPairCode=BTC_USDT'],
    ['Azbit Main Deals', 'https://api.azbit.com/api/deals?currencyPairCode=BTC_USDT'],
  ];

  for (const [label, url] of azbitTests) {
    const r = await httpGet(url);
    printRESTResult(label, r);
  }

  // ═══════════════════════════════════════════════════════════
  // SUMMARY
  // ═══════════════════════════════════════════════════════════
  console.log('\n' + '═'.repeat(70));
  console.log('  SUMMARY');
  console.log('═'.repeat(70));
  console.log('\nTest complete. Check results above for working endpoints.');
}

main().catch(console.error);
