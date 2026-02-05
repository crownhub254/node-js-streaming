const WebSocket = require('ws');
const https = require('https');
const http = require('http');
const zlib = require('zlib');

// Utility: Make HTTP request
function httpRequest(url, options = {}) {
  return new Promise((resolve, reject) => {
    const protocol = url.startsWith('https') ? https : http;
    const req = protocol.request(url, options, (res) => {
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
    req.end(options.body || '');
  });
}

// Utility: Test WebSocket connection
function testWebSocket(config) {
  return new Promise((resolve) => {
    const { name, url, subscribeMsg, timeout = 15000, parseMessage, onOpen, headers } = config;
    const startTime = Date.now();
    let ws;
    let receivedData = [];
    let connected = false;
    
    const cleanup = () => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.close();
      }
    };

    const timer = setTimeout(() => {
      cleanup();
      resolve({
        name,
        success: receivedData.length > 0,
        connected,
        messages: receivedData.length,
        samples: receivedData.slice(0, 3),
        duration: Date.now() - startTime,
        error: receivedData.length === 0 ? 'Timeout - no data received' : null
      });
    }, timeout);

    try {
      ws = new WebSocket(url, headers ? { headers } : undefined);
      
      ws.on('open', () => {
        connected = true;
        if (onOpen) {
          onOpen(ws);
        } else if (subscribeMsg) {
          if (Array.isArray(subscribeMsg)) {
            subscribeMsg.forEach(msg => ws.send(JSON.stringify(msg)));
          } else {
            ws.send(JSON.stringify(subscribeMsg));
          }
        }
      });

      ws.on('message', (data) => {
        try {
          let message;
          if (data instanceof Buffer) {
            try {
              const decompressed = zlib.gunzipSync(data);
              message = JSON.parse(decompressed.toString());
            } catch {
              try {
                const inflated = zlib.inflateRawSync(data);
                message = JSON.parse(inflated.toString());
              } catch {
                message = JSON.parse(data.toString());
              }
            }
          } else {
            message = JSON.parse(data.toString());
          }
          
          // Handle ping/pong for various exchanges
          if (message.ping) {
            ws.send(JSON.stringify({ pong: message.ping }));
            return;
          }
          if (message.op === 'ping' || message.action === 'ping') {
            ws.send(JSON.stringify({ op: 'pong', ts: Date.now() }));
            return;
          }
          
          const parsed = parseMessage ? parseMessage(message) : message;
          if (parsed && !parsed.isPing && parsed.type) {
            receivedData.push(parsed);
          }
          
          if (receivedData.length >= 5) {
            clearTimeout(timer);
            cleanup();
            resolve({
              name,
              success: true,
              connected: true,
              messages: receivedData.length,
              samples: receivedData.slice(0, 3),
              duration: Date.now() - startTime,
              error: null
            });
          }
        } catch (e) {
          // Ignore parse errors
        }
      });

      ws.on('error', (error) => {
        clearTimeout(timer);
        cleanup();
        resolve({
          name,
          success: false,
          connected,
          messages: receivedData.length,
          samples: receivedData.slice(0, 3),
          duration: Date.now() - startTime,
          error: error.message
        });
      });

      ws.on('close', () => {
        if (receivedData.length === 0) {
          clearTimeout(timer);
          resolve({
            name,
            success: false,
            connected,
            messages: 0,
            samples: [],
            duration: Date.now() - startTime,
            error: 'Connection closed before receiving data'
          });
        }
      });
    } catch (error) {
      clearTimeout(timer);
      resolve({
        name,
        success: false,
        connected: false,
        messages: 0,
        samples: [],
        duration: Date.now() - startTime,
        error: error.message
      });
    }
  });
}

// ============ EXCHANGE CONFIGURATIONS (FIXED) ============

const exchanges = {
  // 1. BINGX - Fix ticker and kline
  bingx: {
    name: 'BingX',
    tests: {
      spot_ticker: {
        url: 'wss://open-api-ws.bingx.com/market',
        subscribeMsg: { id: 'ticker1', reqType: 'sub', dataType: 'BTC-USDT@ticker' },
        parseMessage: (m) => m.data ? { type: 'ticker', data: m.data } : null
      },
      spot_kline: {
        url: 'wss://open-api-ws.bingx.com/market',
        subscribeMsg: { id: 'kline1', reqType: 'sub', dataType: 'BTC-USDT@kline_1m' },
        parseMessage: (m) => m.data ? { type: 'kline', data: m.data } : null
      },
      futures_ticker: {
        url: 'wss://open-api-swap.bingx.com/swap-market',
        subscribeMsg: { id: 'ticker1', reqType: 'sub', dataType: 'BTC-USDT@ticker' },
        parseMessage: (m) => m.data ? { type: 'ticker', data: m.data } : null
      },
      futures_kline: {
        url: 'wss://open-api-swap.bingx.com/swap-market',
        subscribeMsg: { id: 'kline1', reqType: 'sub', dataType: 'BTC-USDT@kline_1m' },
        parseMessage: (m) => m.data ? { type: 'kline', data: m.data } : null
      }
    }
  },

  // 2. BITMEX - Add kline
  bitmex: {
    name: 'BitMEX',
    tests: {
      futures_kline: {
        url: 'wss://ws.bitmex.com/realtime',
        subscribeMsg: { op: 'subscribe', args: ['tradeBin1m:XBTUSD'] },
        parseMessage: (m) => m.table === 'tradeBin1m' ? { type: 'kline', data: m.data } : null
      },
      futures_quote: {
        url: 'wss://ws.bitmex.com/realtime',
        subscribeMsg: { op: 'subscribe', args: ['quote:XBTUSD'] },
        parseMessage: (m) => m.table === 'quote' ? { type: 'quote', data: m.data } : null
      }
    }
  },

  // 3. MEXC - Fix spot streams
  mexc: {
    name: 'MEXC',
    tests: {
      spot_orderbook_v2: {
        url: 'wss://wbs.mexc.com/ws',
        subscribeMsg: { method: 'SUBSCRIPTION', params: ['spot@public.limit.depth.v3.api@BTCUSDT@5'] },
        parseMessage: (m) => {
          if (m.c === 'spot@public.limit.depth.v3.api@BTCUSDT@5' && m.d) {
            return { type: 'orderbook', bids: m.d.bids?.length, asks: m.d.asks?.length };
          }
          return null;
        }
      },
      spot_trades_v2: {
        url: 'wss://wbs.mexc.com/ws',
        subscribeMsg: { method: 'SUBSCRIPTION', params: ['spot@public.deals.v3.api@BTCUSDT'] },
        parseMessage: (m) => {
          if (m.c === 'spot@public.deals.v3.api@BTCUSDT' && m.d?.deals) {
            return { type: 'trades', count: m.d.deals.length };
          }
          return null;
        }
      },
      spot_kline_v2: {
        url: 'wss://wbs.mexc.com/ws',
        subscribeMsg: { method: 'SUBSCRIPTION', params: ['spot@public.kline.v3.api@BTCUSDT@Min1'] },
        parseMessage: (m) => {
          if (m.c?.includes('kline') && m.d) {
            return { type: 'kline', data: m.d };
          }
          return null;
        }
      },
      futures_trades: {
        url: 'wss://contract.mexc.com/edge',
        subscribeMsg: { method: 'sub.deal', param: { symbol: 'BTC_USDT' } },
        parseMessage: (m) => m.channel === 'push.deal' ? { type: 'trades', data: m.data } : null
      },
      futures_kline: {
        url: 'wss://contract.mexc.com/edge',
        subscribeMsg: { method: 'sub.kline', param: { symbol: 'BTC_USDT', interval: 'Min1' } },
        parseMessage: (m) => m.channel === 'push.kline' ? { type: 'kline', data: m.data } : null
      }
    }
  },

  // 4. COINBASE - Add kline (via REST since WS doesn't support)
  coinbase: {
    name: 'Coinbase',
    tests: {
      spot_ticker_batch: {
        url: 'wss://ws-feed.exchange.coinbase.com',
        subscribeMsg: { type: 'subscribe', product_ids: ['BTC-USD', 'ETH-USD', 'SOL-USD'], channels: ['ticker'] },
        parseMessage: (m) => m.type === 'ticker' ? { type: 'ticker', symbol: m.product_id, price: m.price } : null
      },
      spot_full_channel: {
        url: 'wss://ws-feed.exchange.coinbase.com',
        subscribeMsg: { type: 'subscribe', product_ids: ['BTC-USD'], channels: ['full'] },
        parseMessage: (m) => m.type === 'received' || m.type === 'open' || m.type === 'done' ? { type: 'full_' + m.type, data: m } : null
      },
      spot_status: {
        url: 'wss://ws-feed.exchange.coinbase.com',
        subscribeMsg: { type: 'subscribe', product_ids: ['BTC-USD'], channels: ['status'] },
        parseMessage: (m) => m.type === 'status' ? { type: 'status', currencies: m.currencies?.length } : null
      }
    }
  },

  // 5. BITSTAMP - Fix trades and add more streams
  bitstamp: {
    name: 'Bitstamp',
    tests: {
      spot_trades: {
        url: 'wss://ws.bitstamp.net',
        subscribeMsg: { event: 'bts:subscribe', data: { channel: 'live_trades_btcusd' } },
        parseMessage: (m) => m.event === 'trade' ? { type: 'trade', price: m.data?.price, amount: m.data?.amount } : null
      },
      spot_orderbook_full: {
        url: 'wss://ws.bitstamp.net',
        subscribeMsg: { event: 'bts:subscribe', data: { channel: 'diff_order_book_btcusd' } },
        parseMessage: (m) => m.event === 'data' && m.data?.bids ? { type: 'orderbook_diff', bids: m.data.bids.length } : null
      },
      spot_orders: {
        url: 'wss://ws.bitstamp.net',
        subscribeMsg: { event: 'bts:subscribe', data: { channel: 'live_orders_btcusd' } },
        parseMessage: (m) => m.event === 'order_created' || m.event === 'order_changed' ? { type: 'order', event: m.event } : null
      }
    }
  },

  // 6. BITMART - Fix with correct endpoints
  bitmart: {
    name: 'BitMart',
    tests: {
      spot_depth: {
        url: 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1',
        onOpen: (ws) => {
          // BitMart requires login first for some channels
          ws.send(JSON.stringify({ op: 'subscribe', args: ['spot/depth5:BTC_USDT'] }));
        },
        parseMessage: (m) => {
          if (m.table === 'spot/depth5' && m.data) {
            return { type: 'orderbook', data: m.data };
          }
          return null;
        }
      },
      spot_trade: {
        url: 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1',
        subscribeMsg: { op: 'subscribe', args: ['spot/trade:BTC_USDT'] },
        parseMessage: (m) => {
          if (m.table === 'spot/trade' && m.data) {
            return { type: 'trade', data: m.data };
          }
          return null;
        }
      },
      spot_kline: {
        url: 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1',
        subscribeMsg: { op: 'subscribe', args: ['spot/kline1m:BTC_USDT'] },
        parseMessage: (m) => {
          if (m.table?.includes('kline') && m.data) {
            return { type: 'kline', data: m.data };
          }
          return null;
        }
      },
      futures_depth: {
        url: 'wss://openapi-ws-v2.bitmart.com/api?protocol=1.1',
        subscribeMsg: { action: 'subscribe', args: ['futures/depth20:BTCUSDT'] },
        parseMessage: (m) => m.group?.includes('depth') ? { type: 'orderbook', data: m.data } : null
      },
      futures_trade: {
        url: 'wss://openapi-ws-v2.bitmart.com/api?protocol=1.1',
        subscribeMsg: { action: 'subscribe', args: ['futures/trade:BTCUSDT'] },
        parseMessage: (m) => m.group?.includes('trade') ? { type: 'trade', data: m.data } : null
      }
    }
  },

  // 7. KUCOIN - Needs token first
  kucoin: {
    name: 'KuCoin',
    needsToken: true,
    getToken: async () => {
      try {
        const response = await httpRequest('https://api.kucoin.com/api/v1/bullet-public', {
          method: 'POST'
        });
        if (response.data?.token) {
          const server = response.data.instanceServers[0];
          return {
            token: response.data.token,
            endpoint: server.endpoint,
            pingInterval: server.pingInterval
          };
        }
      } catch (e) {
        console.log('KuCoin token fetch failed:', e.message);
      }
      return null;
    },
    tests: {
      spot_ticker: {
        getUrl: (token) => `${token.endpoint}?token=${token.token}`,
        subscribeMsg: { id: Date.now(), type: 'subscribe', topic: '/market/ticker:BTC-USDT', privateChannel: false, response: true },
        parseMessage: (m) => m.topic?.includes('ticker') ? { type: 'ticker', data: m.data } : null
      },
      spot_orderbook: {
        getUrl: (token) => `${token.endpoint}?token=${token.token}`,
        subscribeMsg: { id: Date.now(), type: 'subscribe', topic: '/market/level2Depth5:BTC-USDT', privateChannel: false, response: true },
        parseMessage: (m) => m.topic?.includes('level2') ? { type: 'orderbook', data: m.data } : null
      },
      spot_trades: {
        getUrl: (token) => `${token.endpoint}?token=${token.token}`,
        subscribeMsg: { id: Date.now(), type: 'subscribe', topic: '/market/match:BTC-USDT', privateChannel: false, response: true },
        parseMessage: (m) => m.topic?.includes('match') ? { type: 'trade', data: m.data } : null
      },
      spot_kline: {
        getUrl: (token) => `${token.endpoint}?token=${token.token}`,
        subscribeMsg: { id: Date.now(), type: 'subscribe', topic: '/market/candles:BTC-USDT_1min', privateChannel: false, response: true },
        parseMessage: (m) => m.topic?.includes('candles') ? { type: 'kline', data: m.data } : null
      },
      futures_ticker: {
        getUrl: (token) => `${token.endpoint}?token=${token.token}`,
        subscribeMsg: { id: Date.now(), type: 'subscribe', topic: '/contractMarket/tickerV2:XBTUSDTM', privateChannel: false, response: true },
        parseMessage: (m) => m.topic?.includes('ticker') ? { type: 'ticker', data: m.data } : null
      }
    }
  },

  // 8. UPBIT - Korean exchange with specific format
  upbit: {
    name: 'Upbit',
    tests: {
      spot_orderbook: {
        url: 'wss://api.upbit.com/websocket/v1',
        onOpen: (ws) => {
          const ticket = { ticket: 'test-' + Date.now() };
          const format = { format: 'DEFAULT' };
          const type = { type: 'orderbook', codes: ['KRW-BTC'], isOnlyRealtime: true };
          ws.send(JSON.stringify([ticket, type, format]));
        },
        parseMessage: (m) => m.type === 'orderbook' ? { type: 'orderbook', code: m.code, units: m.orderbook_units?.length } : null
      },
      spot_ticker: {
        url: 'wss://api.upbit.com/websocket/v1',
        onOpen: (ws) => {
          const ticket = { ticket: 'test-' + Date.now() };
          const format = { format: 'DEFAULT' };
          const type = { type: 'ticker', codes: ['KRW-BTC'], isOnlyRealtime: true };
          ws.send(JSON.stringify([ticket, type, format]));
        },
        parseMessage: (m) => m.type === 'ticker' ? { type: 'ticker', code: m.code, price: m.trade_price } : null
      },
      spot_trades: {
        url: 'wss://api.upbit.com/websocket/v1',
        onOpen: (ws) => {
          const ticket = { ticket: 'test-' + Date.now() };
          const format = { format: 'DEFAULT' };
          const type = { type: 'trade', codes: ['KRW-BTC'], isOnlyRealtime: true };
          ws.send(JSON.stringify([ticket, type, format]));
        },
        parseMessage: (m) => m.type === 'trade' ? { type: 'trade', code: m.code, price: m.trade_price, volume: m.trade_volume } : null
      }
    }
  },

  // 9. WEEX - Try alternative endpoints (similar to Bitget)
  weex: {
    name: 'WEEX',
    tests: {
      futures_orderbook: {
        url: 'wss://ws.weex.com/v2/ws/public',
        subscribeMsg: { op: 'subscribe', args: [{ instType: 'USDT-FUTURES', channel: 'books15', instId: 'BTCUSDT' }] },
        parseMessage: (m) => m.data ? { type: 'orderbook', data: m.data } : null
      },
      futures_ticker: {
        url: 'wss://ws.weex.com/v2/ws/public',
        subscribeMsg: { op: 'subscribe', args: [{ instType: 'USDT-FUTURES', channel: 'ticker', instId: 'BTCUSDT' }] },
        parseMessage: (m) => m.data && m.arg?.channel === 'ticker' ? { type: 'ticker', data: m.data } : null
      },
      futures_trades: {
        url: 'wss://ws.weex.com/v2/ws/public',
        subscribeMsg: { op: 'subscribe', args: [{ instType: 'USDT-FUTURES', channel: 'trade', instId: 'BTCUSDT' }] },
        parseMessage: (m) => m.data && m.arg?.channel === 'trade' ? { type: 'trade', data: m.data } : null
      },
      futures_kline: {
        url: 'wss://ws.weex.com/v2/ws/public',
        subscribeMsg: { op: 'subscribe', args: [{ instType: 'USDT-FUTURES', channel: 'candle1m', instId: 'BTCUSDT' }] },
        parseMessage: (m) => m.data && m.arg?.channel?.includes('candle') ? { type: 'kline', data: m.data } : null
      },
      // Try spot
      spot_orderbook: {
        url: 'wss://ws.weex.com/v2/ws/public',
        subscribeMsg: { op: 'subscribe', args: [{ instType: 'SPOT', channel: 'books15', instId: 'BTCUSDT' }] },
        parseMessage: (m) => m.data ? { type: 'orderbook', data: m.data } : null
      },
      spot_ticker: {
        url: 'wss://ws.weex.com/v2/ws/public',
        subscribeMsg: { op: 'subscribe', args: [{ instType: 'SPOT', channel: 'ticker', instId: 'BTCUSDT' }] },
        parseMessage: (m) => m.data ? { type: 'ticker', data: m.data } : null
      }
    }
  }
};

// Test KuCoin with token
async function testKuCoin(exchange) {
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`  📊 ${exchange.name.toUpperCase()}`);
  console.log('═'.repeat(60));
  
  const token = await exchange.getToken();
  if (!token) {
    console.log('  ❌ Failed to get KuCoin token');
    return { name: 'KuCoin', tests: {} };
  }
  
  console.log(`  ✅ Got token, endpoint: ${token.endpoint}`);
  
  const results = { name: 'KuCoin', tests: {} };
  
  for (const [testName, testConfig] of Object.entries(exchange.tests)) {
    process.stdout.write(`  Testing ${testName}... `);
    
    try {
      const url = testConfig.getUrl(token);
      const result = await testWebSocket({
        name: testName,
        url: url,
        subscribeMsg: testConfig.subscribeMsg,
        parseMessage: testConfig.parseMessage,
        timeout: 12000
      });
      
      results.tests[testName] = result;
      
      if (result.success) {
        console.log(`✅ SUCCESS (${result.messages} msgs in ${result.duration}ms)`);
        if (result.samples[0]) {
          console.log(`     Sample: ${JSON.stringify(result.samples[0]).substring(0, 80)}...`);
        }
      } else {
        console.log(`❌ FAILED: ${result.error || 'No data'}`);
      }
    } catch (error) {
      console.log(`❌ ERROR: ${error.message}`);
      results.tests[testName] = { success: false, error: error.message };
    }
    
    await new Promise(r => setTimeout(r, 500));
  }
  
  return results;
}

// Main test runner
async function runFixedTests() {
  console.log('╔════════════════════════════════════════════════════════════════╗');
  console.log('║     FIXED EXCHANGE WEBSOCKET STREAM TESTER                     ║');
  console.log('║     Testing: Missing streams for 9 exchanges                   ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');
  
  const results = {};
  
  for (const [exchangeKey, exchange] of Object.entries(exchanges)) {
    // Special handling for KuCoin (needs token)
    if (exchange.needsToken) {
      results[exchangeKey] = await testKuCoin(exchange);
      continue;
    }
    
    console.log(`\n${'═'.repeat(60)}`);
    console.log(`  📊 ${exchange.name.toUpperCase()}`);
    console.log('═'.repeat(60));
    
    results[exchangeKey] = {
      name: exchange.name,
      tests: {}
    };
    
    for (const [testName, testConfig] of Object.entries(exchange.tests)) {
      process.stdout.write(`  Testing ${testName}... `);
      
      try {
        const result = await testWebSocket({
          name: testName,
          url: testConfig.url,
          subscribeMsg: testConfig.subscribeMsg,
          parseMessage: testConfig.parseMessage,
          onOpen: testConfig.onOpen,
          timeout: 12000
        });
        
        results[exchangeKey].tests[testName] = result;
        
        if (result.success) {
          console.log(`✅ SUCCESS (${result.messages} msgs in ${result.duration}ms)`);
          if (result.samples[0]) {
            console.log(`     Sample: ${JSON.stringify(result.samples[0]).substring(0, 80)}...`);
          }
        } else {
          console.log(`❌ FAILED: ${result.error || 'No data'}`);
        }
      } catch (error) {
        console.log(`❌ ERROR: ${error.message}`);
        results[exchangeKey].tests[testName] = { success: false, error: error.message };
      }
      
      await new Promise(r => setTimeout(r, 500));
    }
  }
  
  // Generate summary
  generateSummary(results);
}

function generateSummary(results) {
  console.log('\n\n');
  console.log('╔════════════════════════════════════════════════════════════════════════════════════╗');
  console.log('║                         FIXED STREAMS SUMMARY                                      ║');
  console.log('╚════════════════════════════════════════════════════════════════════════════════════╝\n');
  
  for (const [key, data] of Object.entries(results)) {
    const tests = data.tests;
    const passed = Object.values(tests).filter(t => t.success).length;
    const total = Object.keys(tests).length;
    const status = passed === total ? '✅ ALL PASSED' : passed > 0 ? '⚠️ PARTIAL' : '❌ ALL FAILED';
    
    console.log(`\n${data.name}: ${status} (${passed}/${total})`);
    for (const [testName, result] of Object.entries(tests)) {
      console.log(`  ${result.success ? '✅' : '❌'} ${testName}`);
    }
  }
  
  // Save results
  const fs = require('fs');
  fs.writeFileSync('fixed-exchange-results.json', JSON.stringify(results, null, 2));
  console.log('\n📁 Results saved to: fixed-exchange-results.json');
}

// Run the tests
runFixedTests().then(() => {
  console.log('\n✅ All fixed tests completed!');
  process.exit(0);
}).catch(error => {
  console.error('Error running tests:', error);
  process.exit(1);
});
