/**
 * Detailed Exchange Fixer - Tests each exchange one by one with proper configurations
 * Based on official API documentation research
 */

const WebSocket = require('ws');
const zlib = require('zlib');
const https = require('https');
const fs = require('fs');

const TIMEOUT = 20000; // 20 seconds for more thorough testing

// Results storage
const results = {};

// Helper to make HTTPS requests (for token fetching)
function httpsGet(url, options = {}) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          resolve(data);
        }
      });
    });
    req.on('error', reject);
    req.setTimeout(10000, () => {
      req.destroy();
      reject(new Error('Request timeout'));
    });
  });
}

// Generic WebSocket tester with advanced options
function testWebSocket(config) {
  return new Promise((resolve) => {
    const {
      name,
      url,
      subscribe,
      compression = false,
      pingInterval = null,
      pingMessage = null,
      validateResponse = (data) => data && data.length > 0,
      wsOptions = {}
    } = config;

    const startTime = Date.now();
    let resolved = false;
    let pingTimer = null;
    let ws;

    const finish = (success, details = {}) => {
      if (resolved) return;
      resolved = true;
      if (pingTimer) clearInterval(pingTimer);
      try { ws?.close(); } catch (e) {}
      resolve({
        success,
        time: Date.now() - startTime,
        ...details
      });
    };

    try {
      // Merge default options with custom
      const options = {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          'Accept-Language': 'en-US,en;q=0.9',
          ...wsOptions.headers
        },
        perMessageDeflate: compression ? true : false,
        ...wsOptions
      };

      ws = new WebSocket(url, options);

      const timeout = setTimeout(() => {
        finish(false, { error: 'Timeout' });
      }, TIMEOUT);

      ws.on('open', () => {
        console.log(`  [${name}] Connected`);
        
        // Start ping interval if configured
        if (pingInterval && pingMessage) {
          pingTimer = setInterval(() => {
            try {
              if (ws.readyState === WebSocket.OPEN) {
                ws.send(typeof pingMessage === 'string' ? pingMessage : JSON.stringify(pingMessage));
              }
            } catch (e) {}
          }, pingInterval);
        }

        // Send subscription
        if (subscribe) {
          const msg = typeof subscribe === 'string' ? subscribe : JSON.stringify(subscribe);
          ws.send(msg);
          console.log(`  [${name}] Subscribed: ${msg.substring(0, 80)}...`);
        }
      });

      ws.on('message', (data) => {
        let parsed;
        
        // Handle compression
        if (compression && Buffer.isBuffer(data)) {
          try {
            if (compression === 'gzip') {
              data = zlib.gunzipSync(data);
            } else if (compression === 'inflate') {
              data = zlib.inflateSync(data);
            } else if (compression === 'inflateRaw') {
              data = zlib.inflateRawSync(data);
            }
          } catch (e) {
            // Try different decompression methods
            try { data = zlib.gunzipSync(data); } catch (e2) {
              try { data = zlib.inflateSync(data); } catch (e3) {
                try { data = zlib.inflateRawSync(data); } catch (e4) {
                  // Keep original
                }
              }
            }
          }
        }

        const str = data.toString();
        
        // Handle ping/pong for HTX-style exchanges
        if (str.includes('"ping"') || str.includes('"type":"ping"')) {
          try {
            parsed = JSON.parse(str);
            if (parsed.ping) {
              ws.send(JSON.stringify({ pong: parsed.ping }));
            }
          } catch (e) {}
          return;
        }

        // Skip heartbeat/pong responses
        if (str.includes('"pong"') || str.includes('"type":"pong"')) {
          return;
        }

        clearTimeout(timeout);
        
        // Validate response
        if (validateResponse(str)) {
          finish(true, { sample: str.substring(0, 200) });
        }
      });

      ws.on('error', (err) => {
        clearTimeout(timeout);
        finish(false, { error: err.message });
      });

      ws.on('close', (code, reason) => {
        clearTimeout(timeout);
        if (!resolved) {
          finish(false, { error: `Closed: ${code} ${reason}` });
        }
      });

    } catch (err) {
      finish(false, { error: err.message });
    }
  });
}

// ==================== EXCHANGE TEST FUNCTIONS ====================

// 1. BingX - Fix with gzip and proper ping
async function testBingX() {
  console.log('\n🔧 Testing BingX...');
  const tests = {};

  // Spot Ticker (should work)
  tests.spot_ticker = await testWebSocket({
    name: 'BingX Spot Ticker',
    url: 'wss://open-api-ws.bingx.com/market',
    subscribe: { id: '1', reqType: 'sub', dataType: 'BTC-USDT@ticker' },
    compression: 'gzip',
    pingInterval: 5000,
    pingMessage: { op: 'ping' }
  });

  // Spot Kline (was timing out - try with fixes)
  tests.spot_kline = await testWebSocket({
    name: 'BingX Spot Kline',
    url: 'wss://open-api-ws.bingx.com/market',
    subscribe: { id: '2', reqType: 'sub', dataType: 'BTC-USDT@kline_1m' },
    compression: 'gzip',
    pingInterval: 5000,
    pingMessage: { op: 'ping' }
  });

  // Futures Ticker (new domain)
  tests.futures_ticker = await testWebSocket({
    name: 'BingX Futures Ticker',
    url: 'wss://open-api-swap.bingx.com/swap-market',
    subscribe: { id: '3', reqType: 'sub', dataType: 'BTC-USDT@ticker' },
    compression: 'gzip',
    pingInterval: 5000,
    pingMessage: { op: 'ping' }
  });

  // Futures Kline
  tests.futures_kline = await testWebSocket({
    name: 'BingX Futures Kline',
    url: 'wss://open-api-swap.bingx.com/swap-market',
    subscribe: { id: '4', reqType: 'sub', dataType: 'BTC-USDT@kline_1m' },
    compression: 'gzip',
    pingInterval: 5000,
    pingMessage: { op: 'ping' }
  });

  results.bingx = { name: 'BingX', tests };
  printResults('BingX', tests);
}

// 2. MEXC - Fix with proper ping and futures endpoint
async function testMEXC() {
  console.log('\n🔧 Testing MEXC...');
  const tests = {};

  // Futures Ticker
  tests.futures_ticker = await testWebSocket({
    name: 'MEXC Futures Ticker',
    url: 'wss://contract.mexc.com/edge',
    subscribe: { method: 'sub.ticker', param: { symbol: 'BTC_USDT' } },
    pingInterval: 15000,
    pingMessage: { method: 'ping' }
  });

  // Futures Depth
  tests.futures_depth = await testWebSocket({
    name: 'MEXC Futures Depth',
    url: 'wss://contract.mexc.com/edge',
    subscribe: { method: 'sub.depth', param: { symbol: 'BTC_USDT' } },
    pingInterval: 15000,
    pingMessage: { method: 'ping' }
  });

  // Futures Kline
  tests.futures_kline = await testWebSocket({
    name: 'MEXC Futures Kline',
    url: 'wss://contract.mexc.com/edge',
    subscribe: { method: 'sub.kline', param: { symbol: 'BTC_USDT', interval: 'Min1' } },
    pingInterval: 15000,
    pingMessage: { method: 'ping' }
  });

  // Futures Deal/Trades
  tests.futures_trades = await testWebSocket({
    name: 'MEXC Futures Trades',
    url: 'wss://contract.mexc.com/edge',
    subscribe: { method: 'sub.deal', param: { symbol: 'BTC_USDT' } },
    pingInterval: 15000,
    pingMessage: { method: 'ping' }
  });

  results.mexc = { name: 'MEXC', tests, note: 'Spot uses REST API only' };
  printResults('MEXC', tests);
}

// 3. Coinbase - Use heartbeat channel
async function testCoinbase() {
  console.log('\n🔧 Testing Coinbase...');
  const tests = {};

  // Ticker
  tests.spot_ticker = await testWebSocket({
    name: 'Coinbase Ticker',
    url: 'wss://ws-feed.exchange.coinbase.com',
    subscribe: {
      type: 'subscribe',
      product_ids: ['BTC-USD'],
      channels: ['ticker', 'heartbeat']
    },
    compression: true
  });

  // Level2 Orderbook
  tests.spot_orderbook = await testWebSocket({
    name: 'Coinbase Orderbook',
    url: 'wss://ws-feed.exchange.coinbase.com',
    subscribe: {
      type: 'subscribe',
      product_ids: ['BTC-USD'],
      channels: ['level2', 'heartbeat']
    },
    compression: true
  });

  // Matches (trades)
  tests.spot_trades = await testWebSocket({
    name: 'Coinbase Matches',
    url: 'wss://ws-feed.exchange.coinbase.com',
    subscribe: {
      type: 'subscribe',
      product_ids: ['BTC-USD'],
      channels: ['matches', 'heartbeat']
    },
    compression: true
  });

  results.coinbase = { name: 'Coinbase', tests, note: 'Spot only, no klines via WS' };
  printResults('Coinbase', tests);
}

// 4. Bitstamp - Use heartbeat and proper channel names
async function testBitstamp() {
  console.log('\n🔧 Testing Bitstamp...');
  const tests = {};

  // Live Trades
  tests.spot_trades = await testWebSocket({
    name: 'Bitstamp Trades',
    url: 'wss://ws.bitstamp.net',
    subscribe: {
      event: 'bts:subscribe',
      data: { channel: 'live_trades_btcusd' }
    }
  });

  // Order Book
  tests.spot_orderbook = await testWebSocket({
    name: 'Bitstamp Orderbook',
    url: 'wss://ws.bitstamp.net',
    subscribe: {
      event: 'bts:subscribe',
      data: { channel: 'order_book_btcusd' }
    }
  });

  // Detail Order Book (full depth)
  tests.spot_orderbook_full = await testWebSocket({
    name: 'Bitstamp Full Orderbook',
    url: 'wss://ws.bitstamp.net',
    subscribe: {
      event: 'bts:subscribe',
      data: { channel: 'detail_order_book_btcusd' }
    }
  });

  // Diff Order Book (incremental)
  tests.spot_orderbook_diff = await testWebSocket({
    name: 'Bitstamp Diff Orderbook',
    url: 'wss://ws.bitstamp.net',
    subscribe: {
      event: 'bts:subscribe',
      data: { channel: 'diff_order_book_btcusd' }
    }
  });

  // Try futures with -perp suffix
  tests.futures_trades = await testWebSocket({
    name: 'Bitstamp Futures Trades',
    url: 'wss://ws.bitstamp.net',
    subscribe: {
      event: 'bts:subscribe',
      data: { channel: 'live_trades_btcusd-perp' }
    }
  });

  results.bitstamp = { name: 'Bitstamp', tests, note: 'No klines via WS' };
  printResults('Bitstamp', tests);
}

// 5. BitMart - Enable zlib decompression
async function testBitMart() {
  console.log('\n🔧 Testing BitMart...');
  const tests = {};

  // Spot Ticker
  tests.spot_ticker = await testWebSocket({
    name: 'BitMart Spot Ticker',
    url: 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1',
    subscribe: { op: 'subscribe', args: ['spot/ticker:BTC_USDT'] },
    compression: 'inflate',
    pingInterval: 10000,
    pingMessage: 'ping'
  });

  // Spot Trades
  tests.spot_trades = await testWebSocket({
    name: 'BitMart Spot Trades',
    url: 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1',
    subscribe: { op: 'subscribe', args: ['spot/trade:BTC_USDT'] },
    compression: 'inflate',
    pingInterval: 10000,
    pingMessage: 'ping'
  });

  // Spot Depth
  tests.spot_orderbook = await testWebSocket({
    name: 'BitMart Spot Orderbook',
    url: 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1',
    subscribe: { op: 'subscribe', args: ['spot/depth5:BTC_USDT'] },
    compression: 'inflate',
    pingInterval: 10000,
    pingMessage: 'ping'
  });

  // Spot Kline
  tests.spot_kline = await testWebSocket({
    name: 'BitMart Spot Kline',
    url: 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1',
    subscribe: { op: 'subscribe', args: ['spot/kline1m:BTC_USDT'] },
    compression: 'inflate',
    pingInterval: 10000,
    pingMessage: 'ping'
  });

  results.bitmart = { name: 'BitMart', tests };
  printResults('BitMart', tests);
}

// 6. KuCoin - Get token via REST first
async function testKuCoin() {
  console.log('\n🔧 Testing KuCoin...');
  const tests = {};

  try {
    // Get public token
    console.log('  Getting KuCoin public token...');
    const tokenResponse = await httpsGet('https://api.kucoin.com/api/v1/bullet-public', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    });

    if (tokenResponse.data?.token) {
      const token = tokenResponse.data.token;
      const endpoint = tokenResponse.data.instanceServers[0]?.endpoint || 'wss://ws-api-spot.kucoin.com';
      const wsUrl = `${endpoint}?token=${token}`;
      console.log(`  Token obtained, connecting to: ${endpoint.substring(0, 40)}...`);

      // Spot Ticker
      tests.spot_ticker = await testWebSocket({
        name: 'KuCoin Spot Ticker',
        url: wsUrl,
        subscribe: {
          id: Date.now(),
          type: 'subscribe',
          topic: '/market/ticker:BTC-USDT',
          privateChannel: false,
          response: true
        },
        pingInterval: 18000,
        pingMessage: { id: Date.now(), type: 'ping' }
      });

      // Spot Trades
      tests.spot_trades = await testWebSocket({
        name: 'KuCoin Spot Trades',
        url: wsUrl,
        subscribe: {
          id: Date.now(),
          type: 'subscribe',
          topic: '/market/match:BTC-USDT',
          privateChannel: false,
          response: true
        },
        pingInterval: 18000,
        pingMessage: { id: Date.now(), type: 'ping' }
      });

      // Level2 Depth 5
      tests.spot_orderbook = await testWebSocket({
        name: 'KuCoin Spot Orderbook',
        url: wsUrl,
        subscribe: {
          id: Date.now(),
          type: 'subscribe',
          topic: '/spotMarket/level2Depth5:BTC-USDT',
          privateChannel: false,
          response: true
        },
        pingInterval: 18000,
        pingMessage: { id: Date.now(), type: 'ping' }
      });

    } else {
      tests.token_error = { success: false, error: 'Failed to get token', time: 0 };
    }
  } catch (err) {
    tests.token_error = { success: false, error: err.message, time: 0 };
  }

  // Try futures token
  try {
    console.log('  Getting KuCoin Futures token...');
    const futuresToken = await httpsGet('https://api-futures.kucoin.com/api/v1/bullet-public', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    });

    if (futuresToken.data?.token) {
      const token = futuresToken.data.token;
      const endpoint = futuresToken.data.instanceServers[0]?.endpoint || 'wss://ws-api-futures.kucoin.com';
      const wsUrl = `${endpoint}?token=${token}`;

      tests.futures_ticker = await testWebSocket({
        name: 'KuCoin Futures Ticker',
        url: wsUrl,
        subscribe: {
          id: Date.now(),
          type: 'subscribe',
          topic: '/contractMarket/tickerV2:XBTUSDTM',
          privateChannel: false,
          response: true
        },
        pingInterval: 18000,
        pingMessage: { id: Date.now(), type: 'ping' }
      });
    }
  } catch (err) {
    tests.futures_token_error = { success: false, error: err.message, time: 0 };
  }

  results.kucoin = { name: 'KuCoin', tests };
  printResults('KuCoin', tests);
}

// 7. Upbit - Singapore endpoint with proper format
async function testUpbit() {
  console.log('\n🔧 Testing Upbit...');
  const tests = {};

  // Ticker
  tests.spot_ticker = await testWebSocket({
    name: 'Upbit Ticker',
    url: 'wss://api.upbit.com/websocket/v1',
    subscribe: [{ ticket: 'test' }, { type: 'ticker', codes: ['KRW-BTC'] }],
    pingInterval: 30000,
    pingMessage: 'PING'
  });

  // Trade
  tests.spot_trades = await testWebSocket({
    name: 'Upbit Trades',
    url: 'wss://api.upbit.com/websocket/v1',
    subscribe: [{ ticket: 'test' }, { type: 'trade', codes: ['KRW-BTC'] }],
    pingInterval: 30000,
    pingMessage: 'PING'
  });

  // Orderbook
  tests.spot_orderbook = await testWebSocket({
    name: 'Upbit Orderbook',
    url: 'wss://api.upbit.com/websocket/v1',
    subscribe: [{ ticket: 'test' }, { type: 'orderbook', codes: ['KRW-BTC'] }],
    pingInterval: 30000,
    pingMessage: 'PING'
  });

  results.upbit = { name: 'Upbit', tests, note: 'KRW pairs only' };
  printResults('Upbit', tests);
}

// 8. Toobit - Verify spot, diagnose futures
async function testToobit() {
  console.log('\n🔧 Testing Toobit...');
  const tests = {};

  // Spot Ticker
  tests.spot_ticker = await testWebSocket({
    name: 'Toobit Spot Ticker',
    url: 'wss://stream.toobit.com/quote/ws/v1',
    subscribe: { symbol: 'BTCUSDT', topic: 'realtimes', event: 'sub', params: { binary: false } },
    pingInterval: 30000,
    pingMessage: { ping: Date.now() }
  });

  // Spot Trades
  tests.spot_trades = await testWebSocket({
    name: 'Toobit Spot Trades',
    url: 'wss://stream.toobit.com/quote/ws/v1',
    subscribe: { symbol: 'BTCUSDT', topic: 'trade', event: 'sub', params: { binary: false } },
    pingInterval: 30000,
    pingMessage: { ping: Date.now() }
  });

  // Spot Depth
  tests.spot_depth = await testWebSocket({
    name: 'Toobit Spot Depth',
    url: 'wss://stream.toobit.com/quote/ws/v1',
    subscribe: { symbol: 'BTCUSDT', topic: 'depth', event: 'sub', params: { binary: false } },
    pingInterval: 30000,
    pingMessage: { ping: Date.now() }
  });

  // Try alternate futures endpoint
  tests.futures_ticker = await testWebSocket({
    name: 'Toobit Futures Ticker',
    url: 'wss://stream.toobit.com/contract/ws/v1',
    subscribe: { symbol: 'BTCUSDT', topic: 'realtimes', event: 'sub', params: { binary: false } }
  });

  results.toobit = { name: 'Toobit', tests };
  printResults('Toobit', tests);
}

// 9. Pionex - Use wsPub with correct format
async function testPionex() {
  console.log('\n🔧 Testing Pionex...');
  const tests = {};

  // DEPTH
  tests.spot_orderbook = await testWebSocket({
    name: 'Pionex Depth',
    url: 'wss://ws.pionex.com/wsPub',
    subscribe: { op: 'SUBSCRIBE', topic: 'DEPTH', symbol: 'BTC_USDT' },
    pingInterval: 15000,
    pingMessage: { op: 'PING' }
  });

  // TRADE
  tests.spot_trades = await testWebSocket({
    name: 'Pionex Trades',
    url: 'wss://ws.pionex.com/wsPub',
    subscribe: { op: 'SUBSCRIBE', topic: 'TRADE', symbol: 'BTC_USDT' },
    pingInterval: 15000,
    pingMessage: { op: 'PING' }
  });

  // Try different format
  tests.spot_orderbook_v2 = await testWebSocket({
    name: 'Pionex Depth V2',
    url: 'wss://ws.pionex.com/wsPub',
    subscribe: { op: 'subscribe', channel: 'depth', symbol: 'BTC_USDT' },
    pingInterval: 15000,
    pingMessage: { op: 'ping' }
  });

  results.pionex = { name: 'Pionex', tests, note: 'No klines/tickers via WS' };
  printResults('Pionex', tests);
}

// 10. Deribit - JSON-RPC format
async function testDeribit() {
  console.log('\n🔧 Testing Deribit...');
  const tests = {};

  // Ticker
  tests.futures_ticker = await testWebSocket({
    name: 'Deribit Ticker',
    url: 'wss://www.deribit.com/ws/api/v2',
    subscribe: {
      jsonrpc: '2.0',
      id: 1,
      method: 'public/subscribe',
      params: { channels: ['ticker.BTC-PERPETUAL.raw'] }
    },
    pingInterval: 30000,
    pingMessage: { jsonrpc: '2.0', id: 9999, method: 'public/test', params: {} }
  });

  // Trades
  tests.futures_trades = await testWebSocket({
    name: 'Deribit Trades',
    url: 'wss://www.deribit.com/ws/api/v2',
    subscribe: {
      jsonrpc: '2.0',
      id: 2,
      method: 'public/subscribe',
      params: { channels: ['trades.BTC-PERPETUAL.raw'] }
    },
    pingInterval: 30000,
    pingMessage: { jsonrpc: '2.0', id: 9999, method: 'public/test', params: {} }
  });

  // Orderbook
  tests.futures_orderbook = await testWebSocket({
    name: 'Deribit Orderbook',
    url: 'wss://www.deribit.com/ws/api/v2',
    subscribe: {
      jsonrpc: '2.0',
      id: 3,
      method: 'public/subscribe',
      params: { channels: ['book.BTC-PERPETUAL.100ms'] }
    },
    pingInterval: 30000,
    pingMessage: { jsonrpc: '2.0', id: 9999, method: 'public/test', params: {} }
  });

  results.deribit = { name: 'Deribit', tests, note: 'Derivatives only (futures/options)' };
  printResults('Deribit', tests);
}

// 11. Phemex - Try with different headers to bypass 403
async function testPhemex() {
  console.log('\n🔧 Testing Phemex...');
  const tests = {};

  const headers = {
    'Origin': 'https://phemex.com',
    'Referer': 'https://phemex.com/',
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache'
  };

  // Try main endpoint
  tests.futures_ticker = await testWebSocket({
    name: 'Phemex Ticker',
    url: 'wss://phemex.com/ws',
    subscribe: {
      id: 1,
      method: 'market24h_p.subscribe',
      params: ['BTCUSD']
    },
    wsOptions: { headers },
    pingInterval: 5000,
    pingMessage: { id: 0, method: 'server.ping', params: [] }
  });

  // Try vapi endpoint
  tests.futures_ticker_v2 = await testWebSocket({
    name: 'Phemex Ticker V2',
    url: 'wss://vapi.phemex.com/ws',
    subscribe: {
      id: 1,
      method: 'market24h_p.subscribe',
      params: ['BTCUSD']
    },
    wsOptions: { headers },
    pingInterval: 5000,
    pingMessage: { id: 0, method: 'server.ping', params: [] }
  });

  // Try testnet
  tests.testnet_ticker = await testWebSocket({
    name: 'Phemex Testnet',
    url: 'wss://testnet.phemex.com/ws',
    subscribe: {
      id: 1,
      method: 'market24h_p.subscribe',
      params: ['BTCUSD']
    },
    wsOptions: { headers },
    pingInterval: 5000,
    pingMessage: { id: 0, method: 'server.ping', params: [] }
  });

  results.phemex = { name: 'Phemex', tests, note: 'May require VPN for some regions' };
  printResults('Phemex', tests);
}

// 12. Tapbit - DNS issues, try alternate approaches
async function testTapbit() {
  console.log('\n🔧 Testing Tapbit...');
  const tests = {};

  // Try Bybit-like endpoint
  tests.v5_ticker = await testWebSocket({
    name: 'Tapbit V5 Ticker',
    url: 'wss://stream.tapbit.com/v5/public',
    subscribe: { op: 'subscribe', args: ['tickers.BTCUSDT'] }
  });

  // Try alternate domain
  tests.ws_ticker = await testWebSocket({
    name: 'Tapbit WS Ticker',
    url: 'wss://ws.tapbit.com/v5/public',
    subscribe: { op: 'subscribe', args: ['tickers.BTCUSDT'] }
  });

  // Try without subdomain
  tests.main_ticker = await testWebSocket({
    name: 'Tapbit Main',
    url: 'wss://tapbit.com/ws/v5/public',
    subscribe: { op: 'subscribe', args: ['tickers.BTCUSDT'] }
  });

  results.tapbit = { name: 'Tapbit', tests, note: 'DNS may be region-specific' };
  printResults('Tapbit', tests);
}

// 13. WOO X - Try with different headers
async function testWooX() {
  console.log('\n🔧 Testing WOO X...');
  const tests = {};

  const headers = {
    'Origin': 'https://x.woo.org',
    'Referer': 'https://x.woo.org/',
  };

  // Try public stream
  tests.public_ticker = await testWebSocket({
    name: 'WOO X Ticker',
    url: 'wss://wss.woo.org/ws/stream/public',
    subscribe: { topic: 'BTCUSDT@ticker' },
    wsOptions: { headers },
    pingInterval: 10000,
    pingMessage: { event: 'ping' }
  });

  // Try v2 endpoint
  tests.v2_ticker = await testWebSocket({
    name: 'WOO X V2',
    url: 'wss://wss.woo.org/v2/ws/public/stream',
    subscribe: { topic: 'BTCUSDT@ticker' },
    wsOptions: { headers },
    pingInterval: 10000,
    pingMessage: { event: 'ping' }
  });

  // Try woox.io domain
  tests.woox_io = await testWebSocket({
    name: 'WOO X .io',
    url: 'wss://wss.woox.io/ws/stream/public',
    subscribe: { topic: 'BTCUSDT@ticker' },
    wsOptions: { headers },
    pingInterval: 10000,
    pingMessage: { event: 'ping' }
  });

  results.woox = { name: 'WOO X', tests, note: 'May be geo-blocked' };
  printResults('WOO X', tests);
}

// 14. Websea - HTTP 200 issue
async function testWebsea() {
  console.log('\n🔧 Testing Websea...');
  const tests = {};

  // Try oapi endpoint
  tests.oapi = await testWebSocket({
    name: 'Websea OAPI',
    url: 'wss://oapi.websea.com',
    subscribe: { method: 'subscribe', params: ['BTC_USDT', 10] }
  });

  // Try with /ws suffix
  tests.ws = await testWebSocket({
    name: 'Websea WS',
    url: 'wss://oapi.websea.com/ws',
    subscribe: { method: 'subscribe', params: ['BTC_USDT', 10] }
  });

  // Try main domain
  tests.main = await testWebSocket({
    name: 'Websea Main',
    url: 'wss://www.websea.com/ws',
    subscribe: { method: 'subscribe', params: ['BTC_USDT', 10] }
  });

  results.websea = { name: 'Websea', tests };
  printResults('Websea', tests);
}

// 15. Deepcoin - 400 Bad Request
async function testDeepcoin() {
  console.log('\n🔧 Testing Deepcoin...');
  const tests = {};

  // Spot trades
  tests.spot_trades = await testWebSocket({
    name: 'Deepcoin Spot Trades',
    url: 'wss://stream.deepcoin.com/streamlet/trade/public/spot',
    subscribe: { op: 'subscribe', args: [{ channel: 'trades', instId: 'BTC-USDT' }] }
  });

  // Swap/Futures
  tests.futures_trades = await testWebSocket({
    name: 'Deepcoin Futures Trades',
    url: 'wss://stream.deepcoin.com/streamlet/trade/public/swap',
    subscribe: { op: 'subscribe', args: [{ channel: 'trades', instId: 'BTC-USDT-SWAP' }] }
  });

  // Try v2 params
  tests.v2_trades = await testWebSocket({
    name: 'Deepcoin V2',
    url: 'wss://stream.deepcoin.com/streamlet/trade/public/spot/v2',
    subscribe: { op: 'subscribe', args: ['trades:BTC-USDT'] }
  });

  results.deepcoin = { name: 'Deepcoin', tests };
  printResults('Deepcoin', tests);
}

// 16. BTCC - Timeout issue
async function testBTCC() {
  console.log('\n🔧 Testing BTCC...');
  const tests = {};

  // Try OKX-like format
  tests.okx_style = await testWebSocket({
    name: 'BTCC OKX Style',
    url: 'wss://ws.btcc.com/ws/v5/public',
    subscribe: { op: 'subscribe', args: [{ channel: 'tickers', instId: 'BTC-USDT' }] },
    pingInterval: 30000,
    pingMessage: 'ping'
  });

  // Try Binance-like
  tests.binance_style = await testWebSocket({
    name: 'BTCC Binance Style',
    url: 'wss://stream.btcc.com/ws',
    subscribe: { method: 'SUBSCRIBE', params: ['btcusdt@ticker'], id: 1 }
  });

  // Try direct endpoint
  tests.direct = await testWebSocket({
    name: 'BTCC Direct',
    url: 'wss://api.btcc.com/ws',
    subscribe: { op: 'subscribe', channel: 'ticker', symbol: 'BTCUSDT' }
  });

  results.btcc = { name: 'BTCC', tests };
  printResults('BTCC', tests);
}

// 17. Azbit - SignalR/JSON-RPC
async function testAzbit() {
  console.log('\n🔧 Testing Azbit...');
  const tests = {};

  // Try SignalR negotiate first
  try {
    const negotiate = await httpsGet('https://azbit.com/signalr/negotiate?clientProtocol=2.1');
    console.log('  SignalR negotiate:', JSON.stringify(negotiate).substring(0, 100));
  } catch (e) {
    console.log('  SignalR negotiate failed:', e.message);
  }

  // Try JSON-RPC style
  tests.jsonrpc = await testWebSocket({
    name: 'Azbit JSON-RPC',
    url: 'wss://ws.azbit.com',
    subscribe: {
      jsonrpc: '2.0',
      id: 1,
      method: 'subscribe',
      params: { channel: 'order_book_btc_usdt' }
    }
  });

  // Try Bitstamp-like
  tests.bitstamp_style = await testWebSocket({
    name: 'Azbit Bitstamp Style',
    url: 'wss://ws.azbit.com',
    subscribe: { event: 'subscribe', channel: 'order_book_btc_usdt' }
  });

  results.azbit = { name: 'Azbit', tests, note: 'Uses SignalR protocol' };
  printResults('Azbit', tests);
}

// 18. Poloniex - Connection issues
async function testPoloniex() {
  console.log('\n🔧 Testing Poloniex...');
  const tests = {};

  // Public streams
  tests.public_ticker = await testWebSocket({
    name: 'Poloniex Ticker',
    url: 'wss://ws.poloniex.com/ws/public',
    subscribe: {
      event: 'subscribe',
      channel: ['ticker'],
      symbols: ['BTC_USDT']
    },
    pingInterval: 30000,
    pingMessage: { event: 'ping' }
  });

  // Trades
  tests.public_trades = await testWebSocket({
    name: 'Poloniex Trades',
    url: 'wss://ws.poloniex.com/ws/public',
    subscribe: {
      event: 'subscribe',
      channel: ['trades'],
      symbols: ['BTC_USDT']
    },
    pingInterval: 30000,
    pingMessage: { event: 'ping' }
  });

  // Orderbook
  tests.public_orderbook = await testWebSocket({
    name: 'Poloniex Orderbook',
    url: 'wss://ws.poloniex.com/ws/public',
    subscribe: {
      event: 'subscribe',
      channel: ['book_lv2'],
      symbols: ['BTC_USDT']
    },
    pingInterval: 30000,
    pingMessage: { event: 'ping' }
  });

  results.poloniex = { name: 'Poloniex', tests };
  printResults('Poloniex', tests);
}

// 19. Lbank - Various failures
async function testLbank() {
  console.log('\n🔧 Testing Lbank...');
  const tests = {};

  // Ticker
  tests.ticker = await testWebSocket({
    name: 'Lbank Ticker',
    url: 'wss://www.lbkex.net/ws/V2/',
    subscribe: { action: 'subscribe', subscribe: 'tick', pair: 'btc_usdt' },
    pingInterval: 30000,
    pingMessage: { action: 'ping', ping: 'ping' }
  });

  // Depth
  tests.depth = await testWebSocket({
    name: 'Lbank Depth',
    url: 'wss://www.lbkex.net/ws/V2/',
    subscribe: { action: 'subscribe', subscribe: 'depth', depth: '10', pair: 'btc_usdt' },
    pingInterval: 30000,
    pingMessage: { action: 'ping', ping: 'ping' }
  });

  // Trades
  tests.trades = await testWebSocket({
    name: 'Lbank Trades',
    url: 'wss://www.lbkex.net/ws/V2/',
    subscribe: { action: 'subscribe', subscribe: 'trade', pair: 'btc_usdt' },
    pingInterval: 30000,
    pingMessage: { action: 'ping', ping: 'ping' }
  });

  // Kline
  tests.kline = await testWebSocket({
    name: 'Lbank Kline',
    url: 'wss://www.lbkex.net/ws/V2/',
    subscribe: { action: 'subscribe', subscribe: 'kbar', kbar: 'minute1', pair: 'btc_usdt' },
    pingInterval: 30000,
    pingMessage: { action: 'ping', ping: 'ping' }
  });

  results.lbank = { name: 'Lbank', tests };
  printResults('Lbank', tests);
}

// Helper to print results
function printResults(name, tests) {
  const passed = Object.values(tests).filter(t => t.success).length;
  const total = Object.values(tests).length;
  const status = passed === total ? '✅' : passed > 0 ? '🟡' : '❌';
  console.log(`\n  ${status} ${name}: ${passed}/${total} streams working`);
  
  for (const [key, result] of Object.entries(tests)) {
    const icon = result.success ? '✅' : '❌';
    const detail = result.success ? `${result.time}ms` : result.error;
    console.log(`     ${icon} ${key}: ${detail}`);
  }
}

// Generate report
function generateReport() {
  let report = `# 🔧 DETAILED EXCHANGE FIX REPORT

**Test Date:** ${new Date().toISOString().split('T')[0]}
**Test Timeout:** ${TIMEOUT/1000} seconds per stream
**Total Exchanges Tested:** ${Object.keys(results).length}

---

## 📊 Summary

`;

  let working = 0, partial = 0, failed = 0;
  
  for (const [key, exchange] of Object.entries(results)) {
    const tests = exchange.tests;
    const passed = Object.values(tests).filter(t => t.success).length;
    const total = Object.values(tests).length;
    
    if (passed === total && total > 0) working++;
    else if (passed > 0) partial++;
    else failed++;
  }

  report += `| Category | Count |
|----------|-------|
| ✅ Fully Working | ${working} |
| 🟡 Partially Working | ${partial} |
| ❌ Failed | ${failed} |
| **Total** | **${Object.keys(results).length}** |

---

## 📋 Detailed Results

`;

  for (const [key, exchange] of Object.entries(results)) {
    const tests = exchange.tests;
    const passed = Object.values(tests).filter(t => t.success).length;
    const total = Object.values(tests).length;
    const status = passed === total && total > 0 ? '✅' : passed > 0 ? '🟡' : '❌';
    
    report += `### ${status} ${exchange.name}\n\n`;
    if (exchange.note) report += `**Note:** ${exchange.note}\n\n`;
    
    report += `| Stream | Status | Time/Error |\n|--------|--------|------------|\n`;
    
    for (const [testKey, result] of Object.entries(tests)) {
      const icon = result.success ? '✅' : '❌';
      const detail = result.success ? `${result.time}ms` : result.error?.substring(0, 50);
      report += `| ${testKey} | ${icon} | ${detail} |\n`;
    }
    
    if (passed > 0) {
      const workingTest = Object.entries(tests).find(([k, v]) => v.success);
      if (workingTest && workingTest[1].sample) {
        report += `\n**Sample Data:**\n\`\`\`json\n${workingTest[1].sample.substring(0, 150)}...\n\`\`\`\n`;
      }
    }
    
    report += '\n---\n\n';
  }

  return report;
}

// Main execution
async function main() {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('       DETAILED EXCHANGE FIXER - Testing Each Exchange');
  console.log('═══════════════════════════════════════════════════════════════\n');

  // Test all exchanges
  await testBingX();
  await testMEXC();
  await testCoinbase();
  await testBitstamp();
  await testBitMart();
  await testKuCoin();
  await testUpbit();
  await testToobit();
  await testPionex();
  await testDeribit();
  await testPhemex();
  await testTapbit();
  await testWooX();
  await testWebsea();
  await testDeepcoin();
  await testBTCC();
  await testAzbit();
  await testPoloniex();
  await testLbank();

  // Save results
  fs.writeFileSync('detailed-fix-results.json', JSON.stringify(results, null, 2));
  console.log('\n\n✅ Results saved to detailed-fix-results.json');

  // Generate report
  const report = generateReport();
  fs.writeFileSync('DETAILED-FIX-REPORT.md', report);
  console.log('✅ Report saved to DETAILED-FIX-REPORT.md');

  // Print summary
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('                        FINAL SUMMARY');
  console.log('═══════════════════════════════════════════════════════════════\n');
  
  let working = 0, partial = 0, failed = 0;
  for (const [key, exchange] of Object.entries(results)) {
    const tests = exchange.tests;
    const passed = Object.values(tests).filter(t => t.success).length;
    const total = Object.values(tests).length;
    
    const status = passed === total && total > 0 ? '✅ WORKING' : passed > 0 ? '🟡 PARTIAL' : '❌ FAILED';
    console.log(`  ${status.padEnd(12)} ${exchange.name}: ${passed}/${total}`);
    
    if (passed === total && total > 0) working++;
    else if (passed > 0) partial++;
    else failed++;
  }
  
  console.log(`\n  ═══════════════════════════════════════`);
  console.log(`  ✅ Working: ${working} | 🟡 Partial: ${partial} | ❌ Failed: ${failed}`);
  console.log(`  Total: ${Object.keys(results).length} exchanges tested`);
}

main().catch(console.error);
