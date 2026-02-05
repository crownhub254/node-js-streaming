const WebSocket = require('ws');
const https = require('https');
const http = require('http');
const zlib = require('zlib');

// Configuration for all new exchanges
const EXCHANGES = {
  // ===== BATCH 1 =====
  azbit: {
    name: 'Azbit',
    spot: {
      url: 'wss://api.azbit.com/ws',
      orderbook: { method: 'subscribe', params: { channel: 'orderbooks', currencyPair: 'BTC_USDT' } },
      trades: { method: 'subscribe', params: { channel: 'trades', currencyPair: 'BTC_USDT' } },
      ticker: { method: 'subscribe', params: { channel: 'latest-price', currencyPair: 'BTC_USDT' } }
    }
  },
  
  btcc: {
    name: 'BTCC',
    futures: {
      url: 'wss://ws.btcc.com/ws',
      orderbook: { op: 'subscribe', args: ['depth:BTCUSD'] },
      ticker: { op: 'subscribe', args: ['ticker:BTCUSD'] }
    }
  },
  
  tapbit: {
    name: 'Tapbit',
    futures: {
      url: 'wss://ws.tapbit.com/swap',
      orderbook: { op: 'subscribe', args: [{ channel: 'books', instId: 'BTCUSDT' }] },
      ticker: { op: 'subscribe', args: [{ channel: 'tickers', instId: 'BTCUSDT' }] }
    }
  },
  
  hitbtc: {
    name: 'HitBTC',
    spot: {
      url: 'wss://api.hitbtc.com/api/3/ws/public',
      orderbook: { method: 'subscribe', ch: 'orderbook/D5/1000ms', params: { symbols: ['BTCUSDT'] } },
      trades: { method: 'subscribe', ch: 'trades', params: { symbols: ['BTCUSDT'], limit: 10 } },
      ticker: { method: 'subscribe', ch: 'ticker/1s', params: { symbols: ['BTCUSDT'] } },
      kline: { method: 'subscribe', ch: 'candles/M1', params: { symbols: ['BTCUSDT'], limit: 10 } }
    },
    futures: {
      url: 'wss://api.hitbtc.com/api/3/ws/public',
      orderbook: { method: 'subscribe', ch: 'orderbook/D5/1000ms', params: { symbols: ['BTCUSDT_PERP'] } },
      ticker: { method: 'subscribe', ch: 'ticker/1s', params: { symbols: ['BTCUSDT_PERP'] } }
    }
  },
  
  websea: {
    name: 'Websea',
    spot: {
      url: 'wss://oapi.websea.com/ws',
      orderbook: { event: 'sub', params: { channel: 'depth', symbol: 'BTC-USDT' } },
      trades: { event: 'sub', params: { channel: 'trade', symbol: 'BTC-USDT' } },
      ticker: { event: 'sub', params: { channel: 'ticker', symbol: 'BTC-USDT' } }
    },
    futures: {
      url: 'wss://coapi.websea.com/ws',
      orderbook: { event: 'sub', params: { channel: 'depth', symbol: 'BTCUSDT' } },
      ticker: { event: 'sub', params: { channel: 'ticker', symbol: 'BTCUSDT' } }
    }
  },
  
  deepcoin: {
    name: 'Deepcoin',
    spot: {
      url: 'wss://stream.deepcoin.com/streamlet/trade/public/spot?platform=api',
      orderbook: JSON.stringify({ Topic: 'book25', Action: 1, Symbol: 'BTC/USDT' }),
      trades: JSON.stringify({ Topic: 'trade', Action: 1, Symbol: 'BTC/USDT' }),
      ticker: JSON.stringify({ Topic: 'market-latestTick', Action: 1, Symbol: 'BTC/USDT' }),
      kline: JSON.stringify({ Topic: 'kline', Action: 1, Symbol: 'BTC/USDT', Period: '1m' })
    },
    futures: {
      url: 'wss://stream.deepcoin.com/streamlet/trade/public/swap?platform=api',
      orderbook: JSON.stringify({ Topic: 'book25', Action: 1, Symbol: 'BTCUSDT' }),
      ticker: JSON.stringify({ Topic: 'market-latestTick', Action: 1, Symbol: 'BTCUSDT' })
    }
  },
  
  coinw: {
    name: 'CoinW',
    spot: {
      url: 'wss://ws.coinw.com/websocket',
      orderbook: { event: 'sub', params: { channel: 'spot/level2_20:BTC_USDT' } },
      trades: { event: 'sub', params: { channel: 'spot/match:BTC_USDT' } }
    }
  },
  
  pionex: {
    name: 'Pionex',
    spot: {
      url: 'wss://ws.pionex.com/wsPub',
      orderbook: { op: 'SUBSCRIBE', topic: 'DEPTH', symbol: 'BTC_USDT' },
      trades: { op: 'SUBSCRIBE', topic: 'TRADE', symbol: 'BTC_USDT' },
      ticker: { op: 'SUBSCRIBE', topic: 'TICKER', symbol: 'BTC_USDT' },
      kline: { op: 'SUBSCRIBE', topic: 'KLINE', symbol: 'BTC_USDT', interval: '1M' }
    }
  },
  
  btse: {
    name: 'BTSE',
    spot: {
      url: 'wss://ws.btse.com/ws/spot',
      orderbook: { op: 'subscribe', args: ['orderBookL2Api:BTC-USD'] },
      trades: { op: 'subscribe', args: ['tradeHistoryApi:BTC-USD'] }
    },
    futures: {
      url: 'wss://ws.btse.com/ws/futures',
      orderbook: { op: 'subscribe', args: ['orderBookL2Api:BTCPFC'] },
      trades: { op: 'subscribe', args: ['tradeHistoryApi:BTCPFC'] }
    }
  },
  
  cryptocom: {
    name: 'Crypto.com',
    spot: {
      url: 'wss://stream.crypto.com/exchange/v1/market',
      orderbook: { id: 1, method: 'subscribe', params: { channels: ['book.BTC_USD.50'] } },
      trades: { id: 2, method: 'subscribe', params: { channels: ['trade.BTC_USD'] } },
      ticker: { id: 3, method: 'subscribe', params: { channels: ['ticker.BTC_USD'] } },
      kline: { id: 4, method: 'subscribe', params: { channels: ['candlestick.1m.BTC_USD'] } }
    },
    futures: {
      url: 'wss://stream.crypto.com/exchange/v1/market',
      orderbook: { id: 1, method: 'subscribe', params: { channels: ['book.BTCUSD-PERP.50'] } },
      ticker: { id: 2, method: 'subscribe', params: { channels: ['ticker.BTCUSD-PERP'] } }
    }
  },

  // ===== BATCH 2 =====
  asterdex: {
    name: 'AsterDEX',
    futures: {
      url: 'wss://fstream.asterdex.com/ws/btcusdt@depth',
      orderbook: null, // Stream URL already specifies the channel
      ticker: null
    }
  },
  
  toobit: {
    name: 'Toobit',
    spot: {
      url: 'wss://stream.toobit.com/quote/ws/v1',
      orderbook: { symbol: 'BTCUSDT', topic: 'depth', event: 'sub', params: { binary: false } },
      trades: { symbol: 'BTCUSDT', topic: 'trade', event: 'sub', params: { binary: false } },
      ticker: { symbol: 'BTCUSDT', topic: 'realtimes', event: 'sub', params: { binary: false } }
    },
    futures: {
      url: 'wss://stream.toobit.com/quote/ws/v1',
      orderbook: { symbol: 'BTCUSDT', topic: 'depth', event: 'sub', params: { binary: false } }
    }
  },
  
  ourbit: {
    name: 'Ourbit',
    spot: {
      url: 'wss://open-api-ws.ourbit.com/open/api/v2/ws',
      orderbook: { method: 'SUBSCRIPTION', params: ['spot@public.depth.v3.api@BTCUSDT'] },
      trades: { method: 'SUBSCRIPTION', params: ['spot@public.deals.v3.api@BTCUSDT'] },
      ticker: { method: 'SUBSCRIPTION', params: ['spot@public.ticker.v3.api@BTCUSDT'] }
    }
  },
  
  ascendex: {
    name: 'AscendEX',
    spot: {
      url: 'wss://ascendex.com/1/api/pro/v1/stream',
      orderbook: { op: 'sub', ch: 'depth:BTC/USDT' },
      trades: { op: 'sub', ch: 'trades:BTC/USDT' },
      ticker: { op: 'sub', ch: 'bbo:BTC/USDT' },
      kline: { op: 'sub', ch: 'bar:1:BTC/USDT' }
    }
  },
  
  bitunix: {
    name: 'Bitunix',
    futures: {
      url: 'wss://fapi.bitunix.com/public/',
      orderbook: { op: 'subscribe', args: [{ channel: 'depth', instId: 'BTCUSDT' }] },
      trades: { op: 'subscribe', args: [{ channel: 'trade', instId: 'BTCUSDT' }] },
      ticker: { op: 'subscribe', args: [{ channel: 'ticker', instId: 'BTCUSDT' }] },
      kline: { op: 'subscribe', args: [{ channel: 'kline1m', instId: 'BTCUSDT' }] }
    }
  },
  
  phemex: {
    name: 'Phemex',
    spot: {
      url: 'wss://phemex.com/ws',
      orderbook: { id: 1, method: 'orderbook.subscribe', params: ['sBTCUSDT'] },
      trades: { id: 2, method: 'trade.subscribe', params: ['sBTCUSDT'] },
      ticker: { id: 3, method: 'tick.subscribe', params: ['sBTCUSDT'] },
      kline: { id: 4, method: 'kline.subscribe', params: ['sBTCUSDT', 60] }
    },
    futures: {
      url: 'wss://phemex.com/ws',
      orderbook: { id: 1, method: 'orderbook.subscribe', params: ['BTCUSDT'] },
      ticker: { id: 2, method: 'tick.subscribe', params: ['BTCUSDT'] }
    }
  },
  
  whitebit: {
    name: 'WhiteBIT',
    spot: {
      url: 'wss://api.whitebit.com/ws',
      orderbook: { id: 1, method: 'depth_subscribe', params: ['BTC_USDT', 20, '0', true] },
      trades: { id: 2, method: 'deals_subscribe', params: ['BTC_USDT'] },
      ticker: { id: 3, method: 'market_subscribe', params: ['BTC_USDT'] },
      kline: { id: 4, method: 'candles_subscribe', params: ['BTC_USDT', 60] }
    }
  },
  
  blofin: {
    name: 'BloFin',
    futures: {
      url: 'wss://openapi.blofin.com/ws/public',
      orderbook: { op: 'subscribe', args: [{ channel: 'books', instId: 'BTC-USDT' }] },
      trades: { op: 'subscribe', args: [{ channel: 'trades', instId: 'BTC-USDT' }] },
      ticker: { op: 'subscribe', args: [{ channel: 'tickers', instId: 'BTC-USDT' }] },
      kline: { op: 'subscribe', args: [{ channel: 'candle1m', instId: 'BTC-USDT' }] }
    }
  }
};

// Test timeout in ms
const TEST_TIMEOUT = 12000;

// Results storage
const results = {};

function decompress(data, callback) {
  // Try gzip first
  zlib.gunzip(data, (err, result) => {
    if (!err) return callback(null, result.toString());
    // Try inflate
    zlib.inflate(data, (err2, result2) => {
      if (!err2) return callback(null, result2.toString());
      // Try inflateRaw
      zlib.inflateRaw(data, (err3, result3) => {
        if (!err3) return callback(null, result3.toString());
        callback(err, null);
      });
    });
  });
}

async function testStream(exchange, market, streamType, config) {
  return new Promise((resolve) => {
    const testKey = `${exchange}_${market}_${streamType}`;
    const startTime = Date.now();
    
    if (!config || !config.url) {
      resolve({ key: testKey, success: false, error: 'No URL configured' });
      return;
    }

    const subscribeMsg = config[streamType];
    if (subscribeMsg === undefined) {
      resolve({ key: testKey, success: false, error: 'Stream type not configured' });
      return;
    }

    let ws;
    let resolved = false;
    
    const timeout = setTimeout(() => {
      if (!resolved) {
        resolved = true;
        try { ws.close(); } catch(e) {}
        resolve({ key: testKey, success: false, error: 'Timeout', time: TEST_TIMEOUT });
      }
    }, TEST_TIMEOUT);

    try {
      ws = new WebSocket(config.url, {
        headers: { 'User-Agent': 'Mozilla/5.0' }
      });

      ws.on('open', () => {
        // For streams where URL contains the subscription (e.g., AsterDEX)
        if (subscribeMsg === null) {
          // Just wait for data
          return;
        }
        
        try {
          const msg = typeof subscribeMsg === 'string' ? subscribeMsg : JSON.stringify(subscribeMsg);
          ws.send(msg);
        } catch (e) {
          if (!resolved) {
            resolved = true;
            clearTimeout(timeout);
            resolve({ key: testKey, success: false, error: `Send error: ${e.message}` });
          }
        }
      });

      ws.on('message', (data) => {
        let msgStr;
        
        if (Buffer.isBuffer(data)) {
          // Try decompression
          decompress(data, (err, decompressed) => {
            if (err) {
              msgStr = data.toString();
            } else {
              msgStr = decompressed;
            }
            processMessage(msgStr);
          });
        } else {
          msgStr = data.toString();
          processMessage(msgStr);
        }
        
        function processMessage(msg) {
          // Handle pings
          if (msg.includes('"ping"') || msg.includes('"method":"ping"')) {
            try {
              const parsed = JSON.parse(msg);
              if (parsed.ping) {
                ws.send(JSON.stringify({ pong: parsed.ping }));
              }
            } catch(e) {}
            return;
          }
          
          // Skip subscription confirmations
          if (msg.includes('"event":"subscribed"') || 
              msg.includes('"result":null') ||
              msg.includes('"status":"success"') ||
              msg.includes('"type":"subscribed"')) {
            return;
          }
          
          // Check for actual data
          if (msg.includes('error') && !msg.includes('data')) {
            if (!resolved) {
              resolved = true;
              clearTimeout(timeout);
              try { ws.close(); } catch(e) {}
              resolve({ key: testKey, success: false, error: msg.substring(0, 200), time: Date.now() - startTime });
            }
            return;
          }
          
          // Success - got data
          if (!resolved) {
            resolved = true;
            clearTimeout(timeout);
            try { ws.close(); } catch(e) {}
            resolve({ 
              key: testKey, 
              success: true, 
              sample: msg.substring(0, 150),
              time: Date.now() - startTime
            });
          }
        }
      });

      ws.on('error', (err) => {
        if (!resolved) {
          resolved = true;
          clearTimeout(timeout);
          resolve({ key: testKey, success: false, error: err.message, time: Date.now() - startTime });
        }
      });

      ws.on('close', (code, reason) => {
        if (!resolved) {
          resolved = true;
          clearTimeout(timeout);
          resolve({ key: testKey, success: false, error: `Closed: ${code} ${reason}`, time: Date.now() - startTime });
        }
      });

    } catch (err) {
      if (!resolved) {
        resolved = true;
        clearTimeout(timeout);
        resolve({ key: testKey, success: false, error: err.message });
      }
    }
  });
}

async function testExchange(exchangeId) {
  const exchange = EXCHANGES[exchangeId];
  if (!exchange) {
    console.log(`  ⚠️ Exchange ${exchangeId} not configured`);
    return;
  }

  console.log(`\n${'━'.repeat(50)}`);
  console.log(`  📊 Testing ${exchange.name}`);
  console.log(`${'━'.repeat(50)}`);

  const exchangeResults = {
    name: exchange.name,
    spot: {},
    futures: {}
  };

  // Test spot streams
  if (exchange.spot) {
    console.log(`\n  🔵 SPOT Market:`);
    for (const streamType of ['orderbook', 'trades', 'ticker', 'kline']) {
      if (exchange.spot[streamType] !== undefined) {
        process.stdout.write(`    Testing ${streamType}... `);
        const result = await testStream(exchangeId, 'spot', streamType, exchange.spot);
        exchangeResults.spot[streamType] = result;
        if (result.success) {
          console.log(`✅ (${result.time}ms)`);
        } else {
          console.log(`❌ ${result.error}`);
        }
      }
    }
  } else {
    console.log(`\n  🔵 SPOT: Not available`);
  }

  // Test futures streams
  if (exchange.futures) {
    console.log(`\n  🟠 FUTURES Market:`);
    for (const streamType of ['orderbook', 'trades', 'ticker', 'kline']) {
      if (exchange.futures[streamType] !== undefined) {
        process.stdout.write(`    Testing ${streamType}... `);
        const result = await testStream(exchangeId, 'futures', streamType, exchange.futures);
        exchangeResults.futures[streamType] = result;
        if (result.success) {
          console.log(`✅ (${result.time}ms)`);
        } else {
          console.log(`❌ ${result.error}`);
        }
      }
    }
  } else {
    console.log(`\n  🟠 FUTURES: Not available`);
  }

  return exchangeResults;
}

async function runAllTests() {
  console.log(`
╔════════════════════════════════════════════════════════════════╗
║     NEW EXCHANGE WEBSOCKET STREAMING TEST                      ║
║     Testing ${Object.keys(EXCHANGES).length} Exchanges for Spot/Futures Streams               ║
╚════════════════════════════════════════════════════════════════╝
`);

  const allResults = {};

  for (const exchangeId of Object.keys(EXCHANGES)) {
    allResults[exchangeId] = await testExchange(exchangeId);
  }

  // Print summary
  console.log(`\n\n${'═'.repeat(60)}`);
  console.log(`  📋 FINAL SUMMARY`);
  console.log(`${'═'.repeat(60)}\n`);

  console.log('| Exchange | Spot OB | Spot Trades | Spot Ticker | Spot Kline | Fut OB | Fut Trades | Fut Ticker | Fut Kline |');
  console.log('|----------|---------|-------------|-------------|------------|--------|------------|------------|-----------|');

  for (const [id, result] of Object.entries(allResults)) {
    if (!result) continue;
    
    const getStatus = (market, type) => {
      if (!result[market] || !result[market][type]) return '—';
      return result[market][type].success ? '✅' : '❌';
    };

    console.log(`| ${result.name.padEnd(8)} | ${getStatus('spot','orderbook').padEnd(7)} | ${getStatus('spot','trades').padEnd(11)} | ${getStatus('spot','ticker').padEnd(11)} | ${getStatus('spot','kline').padEnd(10)} | ${getStatus('futures','orderbook').padEnd(6)} | ${getStatus('futures','trades').padEnd(10)} | ${getStatus('futures','ticker').padEnd(10)} | ${getStatus('futures','kline').padEnd(9)} |`);
  }

  console.log(`\n✅ Tests completed!`);
  
  return allResults;
}

// Run tests
runAllTests().catch(console.error);
