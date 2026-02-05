const WebSocket = require('ws');
const https = require('https');
const http = require('http');
const zlib = require('zlib');
const fs = require('fs');

// ═══════════════════════════════════════════════════════════════════════════
// COMPREHENSIVE EXCHANGE WEBSOCKET TESTER
// Based on official API documentation research for 24 exchanges
// ═══════════════════════════════════════════════════════════════════════════

const TEST_TIMEOUT = 15000; // 15 seconds per test
const RESULTS_FILE = 'exchange-test-results.json';

// Exchange configurations based on official API docs
const EXCHANGES = {
  // ═══════════════════════════════════════════════════════════════════════
  // BATCH 1: 14 Exchanges
  // ═══════════════════════════════════════════════════════════════════════
  
  azbit: {
    name: 'Azbit',
    docs: 'https://azbit.com/en/api',
    spot: true,
    futures: false,
    endpoints: {
      spot: 'wss://data.azbit.com/signalr/connect?transport=webSockets&connectionToken=&connectionData=[{"name":"datahub"}]'
    },
    subscriptions: {
      // Azbit uses SignalR - different protocol
      spot_orderbook: null, // Skip - SignalR protocol
      spot_trades: null,
      spot_ticker: null
    }
  },

  btcc: {
    name: 'BTCC',
    docs: 'https://www.btcc.com/en-US/api',
    spot: false,
    futures: true,
    endpoints: {
      futures: 'wss://api.btcc.com/ws/futures'
    },
    subscriptions: {
      futures_orderbook: { type: 'subscribe', channel: 'depth', symbol: 'BTCUSD' },
      futures_ticker: { type: 'subscribe', channel: 'ticker', symbol: 'BTCUSD' }
    }
  },

  tapbit: {
    name: 'Tapbit',
    docs: 'https://www.tapbit.com/api-docs',
    spot: false,
    futures: true,
    endpoints: {
      futures: 'wss://ws.tapbit.com/swap'
    },
    subscriptions: {
      futures_orderbook: { op: 'subscribe', args: [{ channel: 'books', instId: 'BTCUSDT' }] },
      futures_ticker: { op: 'subscribe', args: [{ channel: 'tickers', instId: 'BTCUSDT' }] },
      futures_trades: { op: 'subscribe', args: [{ channel: 'trades', instId: 'BTCUSDT' }] }
    }
  },

  hitbtc: {
    name: 'HitBTC',
    docs: 'https://api.hitbtc.com/',
    spot: true,
    futures: true,
    endpoints: {
      spot: 'wss://api.hitbtc.com/api/3/ws/public',
      futures: 'wss://api.hitbtc.com/api/3/ws/public'
    },
    subscriptions: {
      spot_orderbook: { method: 'subscribe', ch: 'orderbook/full', params: { symbols: ['BTCUSDT'] } },
      spot_trades: { method: 'subscribe', ch: 'trades', params: { symbols: ['BTCUSDT'], limit: 10 } },
      spot_ticker: { method: 'subscribe', ch: 'ticker/1s', params: { symbols: ['BTCUSDT'] } },
      spot_kline: { method: 'subscribe', ch: 'candles/M1', params: { symbols: ['BTCUSDT'], limit: 1 } },
      futures_orderbook: { method: 'subscribe', ch: 'orderbook/full', params: { symbols: ['BTCUSDT_PERP'] } },
      futures_ticker: { method: 'subscribe', ch: 'ticker/1s', params: { symbols: ['BTCUSDT_PERP'] } }
    }
  },

  coinupio: {
    name: 'Coinup.io',
    docs: 'N/A - No public API docs',
    spot: false,
    futures: false,
    endpoints: {},
    subscriptions: {}
  },

  websea: {
    name: 'Websea',
    docs: 'https://www.websea.com/en/api',
    spot: true,
    futures: true,
    endpoints: {
      spot: 'wss://ws.websea.com/kline-api/ws',
      futures: 'wss://futuresws.websea.com/kline-api/ws'
    },
    subscriptions: {
      spot_orderbook: { event: 'sub', params: { channel: 'market_btcusdt_depth_step0' } },
      spot_trades: { event: 'sub', params: { channel: 'market_btcusdt_trade_ticker' } },
      spot_ticker: { event: 'sub', params: { channel: 'market_btcusdt_ticker' } },
      spot_kline: { event: 'sub', params: { channel: 'market_btcusdt_kline_1min' } },
      futures_orderbook: { event: 'sub', params: { channel: 'market_btcusdt_depth_step0' } },
      futures_ticker: { event: 'sub', params: { channel: 'market_btcusdt_ticker' } }
    }
  },

  deepcoin: {
    name: 'Deepcoin',
    docs: 'https://www.deepcoin.com/docs',
    spot: true,
    futures: true,
    endpoints: {
      spot: 'wss://stream.deepcoin.com/ws/spot/public',
      futures: 'wss://stream.deepcoin.com/ws/swap/public'
    },
    subscriptions: {
      spot_orderbook: { Topic: 'orderbook', Action: 'subscribe', Symbol: 'BTC-USDT', Depth: 25 },
      spot_trades: { Topic: 'trade', Action: 'subscribe', Symbol: 'BTC-USDT' },
      spot_ticker: { Topic: 'ticker', Action: 'subscribe', Symbol: 'BTC-USDT' },
      spot_kline: { Topic: 'kline', Action: 'subscribe', Symbol: 'BTC-USDT', Period: '1m' },
      futures_orderbook: { Topic: 'orderbook', Action: 'subscribe', Symbol: 'BTCUSDT', Depth: 25 },
      futures_ticker: { Topic: 'ticker', Action: 'subscribe', Symbol: 'BTCUSDT' },
      futures_kline: { Topic: 'kline', Action: 'subscribe', Symbol: 'BTCUSDT', Period: '1m' }
    }
  },

  picol: {
    name: 'Picol',
    docs: 'N/A - No public API',
    spot: false,
    futures: false,
    endpoints: {},
    subscriptions: {}
  },

  coinw: {
    name: 'CoinW',
    docs: 'https://www.coinw.com/api',
    spot: true,
    futures: true,
    endpoints: {
      spot: 'wss://ws.coinw.com/websocket',
      futures: 'wss://contract-ws.coinw.com/websocket'
    },
    subscriptions: {
      spot_orderbook: { action: 'subscribe', dataType: 'spot/level2_20:BTCUSDT' },
      spot_trades: { action: 'subscribe', dataType: 'spot/match:BTCUSDT' },
      spot_ticker: { action: 'subscribe', dataType: 'spot/ticker:BTCUSDT' },
      futures_orderbook: { action: 'subscribe', dataType: 'swap/depth:BTCUSDT' },
      futures_ticker: { action: 'subscribe', dataType: 'swap/ticker:BTCUSDT' }
    }
  },

  pionex: {
    name: 'Pionex',
    docs: 'https://docs.pionex.com/',
    spot: true,
    futures: true,
    endpoints: {
      spot: 'wss://ws.pionex.com/wsPub',
      futures: 'wss://ws.pionex.com/wsPub'
    },
    subscriptions: {
      spot_orderbook: { op: 'SUBSCRIBE', topic: 'DEPTH', symbol: 'BTC_USDT' },
      spot_trades: { op: 'SUBSCRIBE', topic: 'TRADE', symbol: 'BTC_USDT' },
      spot_ticker: { op: 'SUBSCRIBE', topic: 'TICKER', symbol: 'BTC_USDT' },
      spot_kline: { op: 'SUBSCRIBE', topic: 'KLINE', symbol: 'BTC_USDT', interval: '1M' },
      futures_orderbook: { op: 'SUBSCRIBE', topic: 'DEPTH', symbol: 'BTC_USDT_PERP' },
      futures_ticker: { op: 'SUBSCRIBE', topic: 'TICKER', symbol: 'BTC_USDT_PERP' }
    }
  },

  btse: {
    name: 'BTSE',
    docs: 'https://www.btse.com/docs/futures/en/',
    spot: true,
    futures: true,
    endpoints: {
      spot: 'wss://ws.btse.com/ws/spot',
      futures: 'wss://ws.btse.com/ws/futures'
    },
    subscriptions: {
      spot_orderbook: { op: 'subscribe', args: ['orderBookL2Api:BTC-USD'] },
      spot_trades: { op: 'subscribe', args: ['tradeHistoryApi:BTC-USD'] },
      futures_orderbook: { op: 'subscribe', args: ['orderBookL2Api:BTCPFC'] },
      futures_trades: { op: 'subscribe', args: ['tradeHistoryApi:BTCPFC'] }
    }
  },

  bingx: {
    name: 'BingX',
    docs: 'https://bingx-api.github.io/docs/',
    spot: true,
    futures: true,
    endpoints: {
      spot: 'wss://open-api-ws.bingx.com/market',
      futures: 'wss://open-api-swap.bingx.com/swap-market'
    },
    subscriptions: {
      spot_orderbook: { id: '1', reqType: 'sub', dataType: 'BTC-USDT@depth20' },
      spot_trades: { id: '2', reqType: 'sub', dataType: 'BTC-USDT@trade' },
      spot_ticker: { id: '3', reqType: 'sub', dataType: 'BTC-USDT@ticker' },
      futures_orderbook: { id: '1', reqType: 'sub', dataType: 'BTC-USDT@depth20' },
      futures_trades: { id: '2', reqType: 'sub', dataType: 'BTC-USDT@trade' },
      futures_ticker: { id: '3', reqType: 'sub', dataType: 'BTC-USDT@ticker' },
      futures_kline: { id: '4', reqType: 'sub', dataType: 'BTC-USDT@kline_1m' }
    }
  },

  batonex: {
    name: 'Batonex',
    docs: 'https://github.com/batonex',
    spot: false,
    futures: true,
    endpoints: {
      futures: 'wss://api.batonex.com/ws/futures'
    },
    subscriptions: {
      futures_orderbook: { op: 'subscribe', args: ['depth:BTCUSDT'] },
      futures_ticker: { op: 'subscribe', args: ['ticker:BTCUSDT'] }
    }
  },

  cryptocom: {
    name: 'Crypto.com',
    docs: 'https://exchange-docs.crypto.com/',
    spot: true,
    futures: true,
    endpoints: {
      spot: 'wss://stream.crypto.com/exchange/v1/market',
      futures: 'wss://stream.crypto.com/exchange/v1/market'
    },
    subscriptions: {
      spot_orderbook: { id: 1, method: 'subscribe', params: { channels: ['book.BTC_USD.10'] } },
      spot_trades: { id: 2, method: 'subscribe', params: { channels: ['trade.BTC_USD'] } },
      spot_ticker: { id: 3, method: 'subscribe', params: { channels: ['ticker.BTC_USD'] } },
      spot_kline: { id: 4, method: 'subscribe', params: { channels: ['candlestick.1m.BTC_USD'] } },
      futures_orderbook: { id: 5, method: 'subscribe', params: { channels: ['book.BTCUSD-PERP.10'] } },
      futures_ticker: { id: 6, method: 'subscribe', params: { channels: ['ticker.BTCUSD-PERP'] } }
    }
  },

  // ═══════════════════════════════════════════════════════════════════════
  // BATCH 2: 10 Exchanges
  // ═══════════════════════════════════════════════════════════════════════

  asterdex: {
    name: 'AsterDEX',
    docs: 'https://docs.asterdex.com/',
    spot: false,
    futures: true,
    endpoints: {
      futures: 'wss://fstream.asterdex.com/ws'
    },
    subscriptions: {
      futures_orderbook: 'btcusdt@depth20',  // Combined stream format
      futures_ticker: 'btcusdt@ticker',
      futures_trades: 'btcusdt@trade'
    }
  },

  bitget: {
    name: 'Bitget',
    docs: 'https://www.bitget.com/api-doc/',
    spot: true,
    futures: true,
    endpoints: {
      spot: 'wss://ws.bitget.com/v2/ws/public',
      futures: 'wss://ws.bitget.com/v2/ws/public'
    },
    subscriptions: {
      spot_orderbook: { op: 'subscribe', args: [{ instType: 'SPOT', channel: 'books15', instId: 'BTCUSDT' }] },
      spot_trades: { op: 'subscribe', args: [{ instType: 'SPOT', channel: 'trade', instId: 'BTCUSDT' }] },
      spot_ticker: { op: 'subscribe', args: [{ instType: 'SPOT', channel: 'ticker', instId: 'BTCUSDT' }] },
      spot_kline: { op: 'subscribe', args: [{ instType: 'SPOT', channel: 'candle1m', instId: 'BTCUSDT' }] },
      futures_orderbook: { op: 'subscribe', args: [{ instType: 'USDT-FUTURES', channel: 'books15', instId: 'BTCUSDT' }] },
      futures_ticker: { op: 'subscribe', args: [{ instType: 'USDT-FUTURES', channel: 'ticker', instId: 'BTCUSDT' }] },
      futures_kline: { op: 'subscribe', args: [{ instType: 'USDT-FUTURES', channel: 'candle1m', instId: 'BTCUSDT' }] }
    }
  },

  toobit: {
    name: 'Toobit',
    docs: 'https://www.toobit.com/en-US/api',
    spot: true,
    futures: true,
    endpoints: {
      spot: 'wss://stream.toobit.com/quote/ws/v1',
      futures: 'wss://fstream.toobit.com/quote/ws/v1'
    },
    subscriptions: {
      spot_orderbook: { symbol: 'BTCUSDT', topic: 'depth', event: 'sub', params: { binary: false } },
      spot_trades: { symbol: 'BTCUSDT', topic: 'trade', event: 'sub', params: { binary: false } },
      spot_ticker: { symbol: 'BTCUSDT', topic: 'realtimes', event: 'sub', params: { binary: false } },
      futures_orderbook: { symbol: 'BTCUSDT', topic: 'depth', event: 'sub', params: { binary: false } },
      futures_ticker: { symbol: 'BTCUSDT', topic: 'realtimes', event: 'sub', params: { binary: false } }
    }
  },

  bybit: {
    name: 'Bybit',
    docs: 'https://bybit-exchange.github.io/docs/',
    spot: true,
    futures: true,
    endpoints: {
      spot: 'wss://stream.bybit.com/v5/public/spot',
      futures: 'wss://stream.bybit.com/v5/public/linear'
    },
    subscriptions: {
      spot_orderbook: { op: 'subscribe', args: ['orderbook.50.BTCUSDT'] },
      spot_trades: { op: 'subscribe', args: ['publicTrade.BTCUSDT'] },
      spot_ticker: { op: 'subscribe', args: ['tickers.BTCUSDT'] },
      spot_kline: { op: 'subscribe', args: ['kline.1.BTCUSDT'] },
      futures_orderbook: { op: 'subscribe', args: ['orderbook.50.BTCUSDT'] },
      futures_trades: { op: 'subscribe', args: ['publicTrade.BTCUSDT'] },
      futures_ticker: { op: 'subscribe', args: ['tickers.BTCUSDT'] },
      futures_kline: { op: 'subscribe', args: ['kline.1.BTCUSDT'] }
    }
  },

  ourbit: {
    name: 'Ourbit',
    docs: 'https://github.com/ourbit-exchange',
    spot: true,
    futures: true,
    endpoints: {
      spot: 'wss://open-api-ws.ourbit.com/open/api/v2/ws',
      futures: 'wss://open-api-ws.ourbit.com/open/api/v2/ws'
    },
    subscriptions: {
      spot_orderbook: { method: 'SUBSCRIPTION', params: ['spot@public.depth.v3.api@BTCUSDT'] },
      spot_trades: { method: 'SUBSCRIPTION', params: ['spot@public.deals.v3.api@BTCUSDT'] },
      spot_ticker: { method: 'SUBSCRIPTION', params: ['spot@public.ticker.v3.api@BTCUSDT'] },
      futures_orderbook: { method: 'SUBSCRIPTION', params: ['futures@public.depth.v3.api@BTCUSDT'] },
      futures_ticker: { method: 'SUBSCRIPTION', params: ['futures@public.ticker.v3.api@BTCUSDT'] }
    }
  },

  ascendex: {
    name: 'AscendEX',
    docs: 'https://ascendex.github.io/ascendex-pro-api/',
    spot: true,
    futures: true,
    endpoints: {
      spot: 'wss://ascendex.com/1/api/pro/v1/stream',
      futures: 'wss://ascendex.com/1/api/pro/v2/stream'
    },
    subscriptions: {
      spot_orderbook: { op: 'sub', ch: 'depth:BTC/USDT' },
      spot_trades: { op: 'sub', ch: 'trades:BTC/USDT' },
      spot_ticker: { op: 'sub', ch: 'bbo:BTC/USDT' },
      spot_kline: { op: 'sub', ch: 'bar:1:BTC/USDT' },
      futures_orderbook: { op: 'sub', ch: 'futures-depth:BTC-PERP' },
      futures_ticker: { op: 'sub', ch: 'futures-ticker:BTC-PERP' }
    }
  },

  bitunix: {
    name: 'Bitunix',
    docs: 'https://docs.bitunix.com/',
    spot: false,
    futures: true,
    endpoints: {
      futures: 'wss://fapi.bitunix.com/public/'
    },
    subscriptions: {
      futures_orderbook: { op: 'subscribe', args: [{ channel: 'depth', instId: 'BTCUSDT' }] },
      futures_trades: { op: 'subscribe', args: [{ channel: 'trade', instId: 'BTCUSDT' }] },
      futures_ticker: { op: 'subscribe', args: [{ channel: 'ticker', instId: 'BTCUSDT' }] },
      futures_kline: { op: 'subscribe', args: [{ channel: 'kline1m', instId: 'BTCUSDT' }] }
    }
  },

  phemex: {
    name: 'Phemex',
    docs: 'https://github.com/phemex/phemex-api-docs',
    spot: true,
    futures: true,
    endpoints: {
      spot: 'wss://ws.phemex.com/ws',
      futures: 'wss://ws.phemex.com/ws'
    },
    subscriptions: {
      spot_orderbook: { id: 1, method: 'orderbook.subscribe', params: ['sBTCUSDT'] },
      spot_trades: { id: 2, method: 'trade.subscribe', params: ['sBTCUSDT'] },
      spot_ticker: { id: 3, method: 'tick.subscribe', params: ['sBTCUSDT'] },
      spot_kline: { id: 4, method: 'kline.subscribe', params: ['sBTCUSDT', 60] },
      futures_orderbook: { id: 5, method: 'orderbook.subscribe', params: ['BTCUSDT'] },
      futures_ticker: { id: 6, method: 'tick.subscribe', params: ['BTCUSDT'] }
    }
  },

  whitebit: {
    name: 'WhiteBIT',
    docs: 'https://docs.whitebit.com/',
    spot: true,
    futures: true,
    endpoints: {
      spot: 'wss://api.whitebit.com/ws',
      futures: 'wss://api.whitebit.com/ws'
    },
    subscriptions: {
      spot_orderbook: { id: 1, method: 'depth_subscribe', params: ['BTC_USDT', 20, '0', true] },
      spot_trades: { id: 2, method: 'deals_subscribe', params: [['BTC_USDT']] },
      spot_ticker: { id: 3, method: 'market_subscribe', params: ['BTC_USDT'] },
      spot_kline: { id: 4, method: 'candles_subscribe', params: ['BTC_USDT', 60] },
      futures_orderbook: { id: 5, method: 'depth_subscribe', params: ['BTC_PERP', 20, '0', true] },
      futures_ticker: { id: 6, method: 'market_subscribe', params: ['BTC_PERP'] }
    }
  },

  blofin: {
    name: 'BloFin',
    docs: 'https://docs.blofin.com/',
    spot: false,
    futures: true,
    endpoints: {
      futures: 'wss://openapi.blofin.com/ws/public'
    },
    subscriptions: {
      futures_orderbook: { op: 'subscribe', args: [{ channel: 'books', instId: 'BTC-USDT' }] },
      futures_trades: { op: 'subscribe', args: [{ channel: 'trades', instId: 'BTC-USDT' }] },
      futures_ticker: { op: 'subscribe', args: [{ channel: 'tickers', instId: 'BTC-USDT' }] },
      futures_kline: { op: 'subscribe', args: [{ channel: 'candle1m', instId: 'BTC-USDT' }] }
    }
  }
};

// ═══════════════════════════════════════════════════════════════════════════
// HELPER FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════

function decompress(data, callback) {
  if (!Buffer.isBuffer(data)) {
    callback(null, data.toString());
    return;
  }
  
  // Try gzip
  zlib.gunzip(data, (err, result) => {
    if (!err) return callback(null, result.toString());
    // Try inflate
    zlib.inflate(data, (err2, result2) => {
      if (!err2) return callback(null, result2.toString());
      // Try inflateRaw
      zlib.inflateRaw(data, (err3, result3) => {
        if (!err3) return callback(null, result3.toString());
        // Return as-is
        callback(null, data.toString());
      });
    });
  });
}

function isDataMessage(msg) {
  // Skip ping/pong, subscription confirms, errors
  const skipPatterns = [
    '"event":"subscribed"',
    '"event":"subscribe"',
    '"result":null',
    '"status":"ok"',
    '"status":"success"',
    '"type":"subscribed"',
    '"method":"pong"',
    '"action":"ping"',
    '"ping"',
    '"ret_msg":"pong"',
    '"op":"pong"',
    '"event":"pong"',
    '"msg":"Connected"',
    '"event":"error"'
  ];
  
  for (const pattern of skipPatterns) {
    if (msg.includes(pattern)) return false;
  }
  
  // Look for data indicators
  const dataPatterns = [
    '"data"',
    '"bids"',
    '"asks"',
    '"price"',
    '"amount"',
    '"volume"',
    '"last"',
    '"high"',
    '"low"',
    '"open"',
    '"close"',
    '"trades"',
    '"orderbook"',
    '"depth"',
    '"ticker"',
    '"kline"',
    '"candle"'
  ];
  
  for (const pattern of dataPatterns) {
    if (msg.includes(pattern)) return true;
  }
  
  return msg.length > 100; // Assume longer messages contain data
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN TEST FUNCTION
// ═══════════════════════════════════════════════════════════════════════════

async function testStream(exchangeId, streamKey, endpoint, subscription) {
  return new Promise((resolve) => {
    const testKey = `${exchangeId}_${streamKey}`;
    const startTime = Date.now();
    
    if (!endpoint) {
      resolve({ key: testKey, success: false, error: 'No endpoint configured', time: 0 });
      return;
    }
    
    if (subscription === null) {
      resolve({ key: testKey, success: false, error: 'Stream not supported (protocol mismatch)', time: 0 });
      return;
    }

    let ws;
    let resolved = false;
    let pingInterval;
    
    const timeout = setTimeout(() => {
      if (!resolved) {
        resolved = true;
        clearInterval(pingInterval);
        try { ws.close(); } catch(e) {}
        resolve({ key: testKey, success: false, error: 'Timeout', time: TEST_TIMEOUT });
      }
    }, TEST_TIMEOUT);

    try {
      ws = new WebSocket(endpoint, {
        headers: { 
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          'Origin': 'https://www.exchange.com'
        },
        handshakeTimeout: 10000
      });

      ws.on('open', () => {
        // Send subscription
        try {
          let msg;
          if (typeof subscription === 'string') {
            // For AsterDEX-style combined stream
            msg = JSON.stringify({ method: 'SUBSCRIBE', params: [subscription], id: 1 });
          } else {
            msg = JSON.stringify(subscription);
          }
          ws.send(msg);
        } catch (e) {
          if (!resolved) {
            resolved = true;
            clearTimeout(timeout);
            resolve({ key: testKey, success: false, error: `Send error: ${e.message}`, time: Date.now() - startTime });
          }
        }
        
        // Setup ping for exchanges that need it
        pingInterval = setInterval(() => {
          try {
            if (ws.readyState === WebSocket.OPEN) {
              // Try common ping formats
              if (exchangeId === 'bybit' || exchangeId === 'bitget') {
                ws.send(JSON.stringify({ op: 'ping' }));
              } else if (exchangeId === 'pionex') {
                ws.send(JSON.stringify({ op: 'PING' }));
              } else {
                ws.send('ping');
              }
            }
          } catch(e) {}
        }, 20000);
      });

      ws.on('message', (data) => {
        decompress(data, (err, msgStr) => {
          if (err || !msgStr) return;
          
          // Handle ping/pong
          if (msgStr.includes('"ping"') || msgStr.includes('"action":"ping"')) {
            try {
              const parsed = JSON.parse(msgStr);
              if (parsed.ping) {
                ws.send(JSON.stringify({ pong: parsed.ping }));
              } else if (parsed.action === 'ping') {
                ws.send(JSON.stringify({ action: 'pong', data: parsed.data }));
              }
            } catch(e) {}
            return;
          }
          
          // Check for actual data
          if (isDataMessage(msgStr)) {
            if (!resolved) {
              resolved = true;
              clearTimeout(timeout);
              clearInterval(pingInterval);
              try { ws.close(); } catch(e) {}
              resolve({ 
                key: testKey, 
                success: true, 
                sample: msgStr.substring(0, 200),
                time: Date.now() - startTime
              });
            }
          }
        });
      });

      ws.on('error', (err) => {
        if (!resolved) {
          resolved = true;
          clearTimeout(timeout);
          clearInterval(pingInterval);
          resolve({ key: testKey, success: false, error: err.message, time: Date.now() - startTime });
        }
      });

      ws.on('close', (code, reason) => {
        if (!resolved) {
          resolved = true;
          clearTimeout(timeout);
          clearInterval(pingInterval);
          resolve({ key: testKey, success: false, error: `Connection closed: ${code}`, time: Date.now() - startTime });
        }
      });

    } catch (err) {
      if (!resolved) {
        resolved = true;
        clearTimeout(timeout);
        resolve({ key: testKey, success: false, error: err.message, time: 0 });
      }
    }
  });
}

async function testExchange(exchangeId) {
  const config = EXCHANGES[exchangeId];
  if (!config) {
    console.log(`  ⚠️ Exchange ${exchangeId} not configured`);
    return null;
  }

  console.log(`\n${'═'.repeat(60)}`);
  console.log(`  📊 Testing ${config.name}`);
  console.log(`  📖 Docs: ${config.docs}`);
  console.log(`${'═'.repeat(60)}`);

  const results = {
    name: config.name,
    docs: config.docs,
    spot: config.spot,
    futures: config.futures,
    tests: {}
  };

  if (!config.spot && !config.futures) {
    console.log(`  ⚠️ No API documentation available - skipping`);
    return results;
  }

  // Test spot streams
  if (config.spot && config.endpoints.spot) {
    console.log(`\n  🔵 SPOT Market (${config.endpoints.spot.substring(0, 50)}...)`);
    for (const [key, sub] of Object.entries(config.subscriptions)) {
      if (key.startsWith('spot_')) {
        process.stdout.write(`    Testing ${key.replace('spot_', '')}... `);
        const result = await testStream(exchangeId, key, config.endpoints.spot, sub);
        results.tests[key] = result;
        if (result.success) {
          console.log(`✅ (${result.time}ms)`);
        } else {
          console.log(`❌ ${result.error}`);
        }
      }
    }
  } else if (config.spot) {
    console.log(`\n  🔵 SPOT: Configured but no endpoint`);
  } else {
    console.log(`\n  🔵 SPOT: Not available`);
  }

  // Test futures streams
  if (config.futures && config.endpoints.futures) {
    console.log(`\n  🟠 FUTURES Market (${config.endpoints.futures.substring(0, 50)}...)`);
    for (const [key, sub] of Object.entries(config.subscriptions)) {
      if (key.startsWith('futures_')) {
        process.stdout.write(`    Testing ${key.replace('futures_', '')}... `);
        const result = await testStream(exchangeId, key, config.endpoints.futures, sub);
        results.tests[key] = result;
        if (result.success) {
          console.log(`✅ (${result.time}ms)`);
        } else {
          console.log(`❌ ${result.error}`);
        }
      }
    }
  } else if (config.futures) {
    console.log(`\n  🟠 FUTURES: Configured but no endpoint`);
  } else {
    console.log(`\n  🟠 FUTURES: Not available`);
  }

  return results;
}

async function runAllTests() {
  console.log(`
╔═══════════════════════════════════════════════════════════════════════════╗
║     COMPREHENSIVE EXCHANGE WEBSOCKET STREAMING TEST                       ║
║     Testing ${Object.keys(EXCHANGES).length} Exchanges for Spot/Futures Streams                           ║
║     Based on Official API Documentation                                   ║
╚═══════════════════════════════════════════════════════════════════════════╝
`);

  const allResults = {};
  const exchangeIds = Object.keys(EXCHANGES);

  for (const exchangeId of exchangeIds) {
    allResults[exchangeId] = await testExchange(exchangeId);
  }

  // Save results to file
  fs.writeFileSync(RESULTS_FILE, JSON.stringify(allResults, null, 2));
  console.log(`\n\n📁 Results saved to ${RESULTS_FILE}`);

  // Print summary table
  printSummary(allResults);

  return allResults;
}

function printSummary(results) {
  console.log(`\n\n${'═'.repeat(80)}`);
  console.log(`  📋 FINAL SUMMARY - All 24 Exchanges`);
  console.log(`${'═'.repeat(80)}\n`);

  // Summary table header
  console.log('┌────────────────┬───────┬─────────┬────────────┬────────────┬────────────┬────────────┐');
  console.log('│ Exchange       │ Spot  │ Futures │ Spot OB    │ Spot Trade │ Fut OB     │ Fut Trade  │');
  console.log('├────────────────┼───────┼─────────┼────────────┼────────────┼────────────┼────────────┤');

  let workingCount = 0;
  let partialCount = 0;
  let failedCount = 0;

  for (const [id, data] of Object.entries(results)) {
    if (!data) continue;
    
    const tests = data.tests || {};
    const spotOB = tests.spot_orderbook?.success ? '✅' : (data.spot ? '❌' : '—');
    const spotTrade = tests.spot_trades?.success ? '✅' : (data.spot ? '❌' : '—');
    const futOB = tests.futures_orderbook?.success ? '✅' : (data.futures ? '❌' : '—');
    const futTrade = tests.futures_trades?.success ? '✅' : (data.futures ? '❌' : '—');
    
    const spotStatus = data.spot ? '✅' : '❌';
    const futStatus = data.futures ? '✅' : '❌';
    
    // Count results
    const passed = Object.values(tests).filter(t => t?.success).length;
    const total = Object.keys(tests).length;
    
    if (passed > 0 && passed === total) workingCount++;
    else if (passed > 0) partialCount++;
    else failedCount++;
    
    console.log(`│ ${data.name.padEnd(14)} │ ${spotStatus.padEnd(5)} │ ${futStatus.padEnd(7)} │ ${spotOB.padEnd(10)} │ ${spotTrade.padEnd(10)} │ ${futOB.padEnd(10)} │ ${futTrade.padEnd(10)} │`);
  }

  console.log('└────────────────┴───────┴─────────┴────────────┴────────────┴────────────┴────────────┘');
  
  console.log(`\n📊 Overall Statistics:`);
  console.log(`   ✅ Fully Working: ${workingCount} exchanges`);
  console.log(`   🟡 Partially Working: ${partialCount} exchanges`);
  console.log(`   ❌ Failed/No API: ${failedCount} exchanges`);
  console.log(`   📈 Success Rate: ${((workingCount + partialCount) / Object.keys(results).length * 100).toFixed(1)}%`);

  console.log(`\n✅ Tests completed!`);
}

// Run the tests
runAllTests().catch(console.error);
