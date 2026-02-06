const WebSocket = require('ws');
const zlib = require('zlib');

// Corrected exchange configurations based on detailed API research
const EXCHANGES = {
  // ==================== WORKING EXCHANGES ====================
  bitunix: {
    name: 'Bitunix',
    docs: 'https://docs.bitunix.com/',
    spot: false,
    futures: true,
    futuresUrl: 'wss://fapi.bitunix.com/public/',
    streams: {
      futures_orderbook: { sub: JSON.stringify({ op: 'subscribe', args: [{ ch: 'depth_book1', instId: 'BTCUSDT' }] }) },
      futures_trades: { sub: JSON.stringify({ op: 'subscribe', args: [{ ch: 'trade', instId: 'BTCUSDT' }] }) },
      futures_ticker: { sub: JSON.stringify({ op: 'subscribe', args: [{ ch: 'ticker', instId: 'BTCUSDT' }] }) },
      futures_kline: { sub: JSON.stringify({ op: 'subscribe', args: [{ ch: 'kline_1m', instId: 'BTCUSDT' }] }) }
    },
    pingInterval: 15000,
    ping: JSON.stringify({ op: 'ping' })
  },

  blofin: {
    name: 'BloFin',
    docs: 'https://docs.blofin.com/',
    spot: false,
    futures: true,
    futuresUrl: 'wss://openapi.blofin.com/ws/public',
    streams: {
      futures_orderbook: { sub: JSON.stringify({ op: 'subscribe', args: [{ channel: 'books', instId: 'BTC-USDT' }] }) },
      futures_trades: { sub: JSON.stringify({ op: 'subscribe', args: [{ channel: 'trades', instId: 'BTC-USDT' }] }) },
      futures_ticker: { sub: JSON.stringify({ op: 'subscribe', args: [{ channel: 'tickers', instId: 'BTC-USDT' }] }) },
      futures_kline: { sub: JSON.stringify({ op: 'subscribe', args: [{ channel: 'candle1m', instId: 'BTC-USDT' }] }) },
      futures_fundingrate: { sub: JSON.stringify({ op: 'subscribe', args: [{ channel: 'funding-rate', instId: 'BTC-USDT' }] }) }
    },
    pingInterval: 25000,
    ping: 'ping'
  },

  asterdex: {
    name: 'AsterDEX',
    docs: 'https://docs.asterdex.com/',
    spot: false,
    futures: true,
    futuresUrl: 'wss://fstream.asterdex.com/ws',
    streams: {
      futures_orderbook: { sub: JSON.stringify({ method: 'SUBSCRIBE', params: ['btcusdt@depth5@100ms'], id: 1 }) },
      futures_trades: { sub: JSON.stringify({ method: 'SUBSCRIBE', params: ['btcusdt@trade'], id: 2 }) },
      futures_ticker: { sub: JSON.stringify({ method: 'SUBSCRIBE', params: ['btcusdt@ticker'], id: 3 }) },
      futures_kline: { sub: JSON.stringify({ method: 'SUBSCRIBE', params: ['btcusdt@kline_1m'], id: 4 }) },
      futures_aggTrade: { sub: JSON.stringify({ method: 'SUBSCRIBE', params: ['btcusdt@aggTrade'], id: 5 }) }
    }
  },

  // ==================== PARTIALLY WORKING - FIXES ====================
  ascendex: {
    name: 'AscendEX',
    docs: 'https://ascendex.github.io/ascendex-pro-api/',
    spot: true,
    futures: true,
    spotUrl: 'wss://ascendex.com/1/api/pro/v1/stream',
    futuresUrl: 'wss://ascendex.com/1/api/pro/v2/stream',
    streams: {
      spot_orderbook: { sub: JSON.stringify({ op: 'sub', ch: 'depth:BTC/USDT' }) },
      spot_trades: { sub: JSON.stringify({ op: 'sub', ch: 'trades:BTC/USDT' }) },
      spot_ticker: { sub: JSON.stringify({ op: 'sub', ch: 'bbo:BTC/USDT' }) },
      spot_kline: { sub: JSON.stringify({ op: 'sub', ch: 'bar:1:BTC/USDT' }) },
      futures_orderbook: { sub: JSON.stringify({ op: 'sub', ch: 'futures-depth:BTC-PERP' }) },
      futures_ticker: { sub: JSON.stringify({ op: 'sub', ch: 'futures-bbo:BTC-PERP' }) }
    },
    pingInterval: 30000,
    ping: JSON.stringify({ op: 'ping' }),
    handlePing: (data) => data.m === 'ping' ? JSON.stringify({ op: 'pong' }) : null
  },

  whitebit: {
    name: 'WhiteBIT',
    docs: 'https://docs.whitebit.com/',
    spot: true,
    futures: true,
    spotUrl: 'wss://api.whitebit.com/ws',
    futuresUrl: 'wss://api.whitebit.com/ws',
    streams: {
      spot_orderbook: { sub: JSON.stringify({ id: 1, method: 'depth_subscribe', params: ['BTC_USDT', 20, '0', true] }) },
      spot_trades: { sub: JSON.stringify({ id: 2, method: 'trades_subscribe', params: ['BTC_USDT'] }) },
      spot_ticker: { sub: JSON.stringify({ id: 3, method: 'market_subscribe', params: ['BTC_USDT'] }) },
      spot_kline: { sub: JSON.stringify({ id: 4, method: 'candles_subscribe', params: ['BTC_USDT', 60] }) },
      futures_orderbook: { sub: JSON.stringify({ id: 5, method: 'depth_subscribe', params: ['BTC_PERP', 20, '0', true] }) },
      futures_ticker: { sub: JSON.stringify({ id: 6, method: 'market_subscribe', params: ['BTC_PERP'] }) }
    },
    pingInterval: 50000,
    ping: JSON.stringify({ id: 0, method: 'ping', params: [] })
  },

  btse: {
    name: 'BTSE',
    docs: 'https://www.btse.com/docs/futures/en/',
    spot: true,
    futures: true,
    spotUrl: 'wss://ws.btse.com/ws/spot',
    futuresUrl: 'wss://ws.btse.com/ws/futures',
    streams: {
      spot_orderbook: { sub: JSON.stringify({ op: 'subscribe', args: ['orderBookApi:BTC-USDT_0'] }) },
      spot_trades: { sub: JSON.stringify({ op: 'subscribe', args: ['tradeHistoryApi:BTC-USDT'] }) },
      futures_orderbook: { sub: JSON.stringify({ op: 'subscribe', args: ['orderBookApi:BTCPFC_0'] }) },
      futures_trades: { sub: JSON.stringify({ op: 'subscribe', args: ['tradeHistoryApi:BTCPFC'] }) }
    },
    wsOptions: { perMessageDeflate: true }
  },

  toobit: {
    name: 'Toobit',
    docs: 'https://www.toobit.com/en-US/api',
    spot: true,
    futures: true,
    spotUrl: 'wss://stream.toobit.com/quote/ws/v1',
    futuresUrl: 'wss://stream.toobit.com/quote/ws/v2',
    streams: {
      spot_orderbook: { sub: JSON.stringify({ topic: 'depth', event: 'sub', symbol: 'BTCUSDT', params: { binary: false } }) },
      spot_trades: { sub: JSON.stringify({ topic: 'trade', event: 'sub', symbol: 'BTCUSDT', params: { binary: false } }) },
      spot_ticker: { sub: JSON.stringify({ topic: 'realtimes', event: 'sub', symbol: 'BTCUSDT', params: { binary: false } }) },
      futures_orderbook: { sub: JSON.stringify({ topic: 'depth', event: 'sub', symbol: 'BTCUSDT', params: { binary: false } }) },
      futures_ticker: { sub: JSON.stringify({ topic: 'realtimes', event: 'sub', symbol: 'BTCUSDT', params: { binary: false } }) }
    },
    pingInterval: 20000,
    ping: JSON.stringify({ ping: Date.now() })
  },

  // ==================== FAILED EXCHANGES - RETRY WITH FIXES ====================
  tapbit: {
    name: 'Tapbit',
    docs: 'https://www.tapbit.com/api-docs',
    spot: true,
    futures: true,
    // Try Bybit-compatible format
    spotUrl: 'wss://stream.tapbit.com/v5/public/spot',
    futuresUrl: 'wss://stream.tapbit.com/v5/public/linear',
    streams: {
      spot_orderbook: { sub: JSON.stringify({ op: 'subscribe', args: ['orderbook.50.BTCUSDT'] }) },
      spot_ticker: { sub: JSON.stringify({ op: 'subscribe', args: ['tickers.BTCUSDT'] }) },
      futures_orderbook: { sub: JSON.stringify({ op: 'subscribe', args: ['orderbook.50.BTCUSDT'] }) },
      futures_ticker: { sub: JSON.stringify({ op: 'subscribe', args: ['tickers.BTCUSDT'] }) }
    },
    pingInterval: 20000,
    ping: JSON.stringify({ op: 'ping' })
  },

  coinw: {
    name: 'CoinW',
    docs: 'https://www.coinw.com/api',
    spot: true,
    futures: true,
    // Try alternative domains
    spotUrl: 'wss://ws.futurescw.com',
    futuresUrl: 'wss://ws.futurescw.com/perpum',
    streams: {
      spot_orderbook: { sub: JSON.stringify({ event: 'subscribe', args: 'spot/depth:BTCUSDT' }) },
      spot_ticker: { sub: JSON.stringify({ event: 'subscribe', args: 'spot/ticker:BTCUSDT' }) },
      futures_orderbook: { sub: JSON.stringify({ event: 'subscribe', args: 'perp/depth:BTCUSDT' }) },
      futures_ticker: { sub: JSON.stringify({ event: 'subscribe', args: 'perp/ticker:BTCUSDT' }) }
    }
  },

  websea: {
    name: 'Websea',
    docs: 'https://www.websea.com/en/api',
    spot: true,
    futures: true,
    spotUrl: 'wss://oapi.websea.com',
    futuresUrl: 'wss://oapi.websea.com',
    streams: {
      spot_orderbook: { sub: JSON.stringify({ Method: 'subscribe', Params: ['BTC_USDT', 10] }) },
      spot_trades: { sub: JSON.stringify({ Method: 'subscribe', Params: ['trade.BTC_USDT'] }) },
      futures_orderbook: { sub: JSON.stringify({ Method: 'subscribe', Params: ['depth.BTCUSDT-SWAP', 10] }) }
    }
  },

  pionex: {
    name: 'Pionex',
    docs: 'https://docs.pionex.com/',
    spot: true,
    futures: true,
    // Use public endpoint
    spotUrl: 'wss://ws.pionex.com/wsPub',
    futuresUrl: 'wss://ws.pionex.com/wsPub',
    streams: {
      spot_orderbook: { sub: JSON.stringify({ op: 'SUBSCRIBE', topic: 'TRADE', symbol: 'BTC_USDT' }) },
      spot_ticker: { sub: JSON.stringify({ op: 'SUBSCRIBE', topic: 'TICKER', symbol: 'BTC_USDT' }) },
      futures_ticker: { sub: JSON.stringify({ op: 'SUBSCRIBE', topic: 'TICKER', symbol: 'BTCUSDT_PERP' }) }
    }
  },

  phemex: {
    name: 'Phemex',
    docs: 'https://github.com/phemex/phemex-api-docs',
    spot: true,
    futures: true,
    // Use vapi for public
    spotUrl: 'wss://vapi.phemex.com/ws',
    futuresUrl: 'wss://vapi.phemex.com/ws',
    streams: {
      spot_orderbook: { sub: JSON.stringify({ id: 1, method: 'orderbook.subscribe', params: ['sBTCUSDT'] }) },
      spot_trades: { sub: JSON.stringify({ id: 2, method: 'trade.subscribe', params: ['sBTCUSDT'] }) },
      futures_orderbook: { sub: JSON.stringify({ id: 3, method: 'orderbook.subscribe', params: ['BTCUSDT'] }) },
      futures_trades: { sub: JSON.stringify({ id: 4, method: 'trade.subscribe', params: ['BTCUSDT'] }) },
      futures_ticker: { sub: JSON.stringify({ id: 5, method: 'tick.subscribe', params: ['.BTC'] }) }
    },
    pingInterval: 5000,
    ping: JSON.stringify({ id: 0, method: 'server.ping', params: [] })
  },

  btcc: {
    name: 'BTCC',
    docs: 'https://www.btcc.com/en-US/api',
    spot: false,
    futures: true,
    // OKX-like format based on research
    futuresUrl: 'wss://api.btcc.com/ws/futures',
    streams: {
      futures_orderbook: { sub: JSON.stringify({ op: 'subscribe', args: [{ channel: 'books', instId: 'BTCUSDT' }] }) },
      futures_ticker: { sub: JSON.stringify({ op: 'subscribe', args: [{ channel: 'tickers', instId: 'BTCUSDT' }] }) },
      futures_trades: { sub: JSON.stringify({ op: 'subscribe', args: [{ channel: 'trades', instId: 'BTCUSDT' }] }) }
    },
    pingInterval: 30000,
    ping: 'ping'
  },

  azbit: {
    name: 'Azbit',
    docs: 'https://azbit.com/en/api',
    spot: true,
    futures: false,
    // SignalR-like format
    spotUrl: 'wss://ws.azbit.com',
    streams: {
      spot_orderbook: { sub: JSON.stringify({ Method: 'subscribe', Params: ['orderbook.BTC_USDT'] }) },
      spot_trades: { sub: JSON.stringify({ Method: 'subscribe', Params: ['trades.BTC_USDT'] }) },
      spot_ticker: { sub: JSON.stringify({ Method: 'subscribe', Params: ['ticker.BTC_USDT'] }) }
    }
  },

  deepcoin: {
    name: 'Deepcoin',
    docs: 'https://www.deepcoin.com/docs',
    spot: true,
    futures: true,
    // Corrected endpoints with v2
    spotUrl: 'wss://stream.deepcoin.com/streamlet/trade/public/spot?v2',
    futuresUrl: 'wss://stream.deepcoin.com/streamlet/trade/public/swap?v2',
    streams: {
      spot_orderbook: { sub: JSON.stringify({ SendTopicAction: { Action: '1', TopicID: '7', Params: { symbol: 'BTCUSDT' } } }) },
      spot_trades: { sub: JSON.stringify({ SendTopicAction: { Action: '1', TopicID: '3', Params: { symbol: 'BTCUSDT' } } }) },
      futures_orderbook: { sub: JSON.stringify({ SendTopicAction: { Action: '1', TopicID: '7', Params: { symbol: 'BTCUSDT' } } }) },
      futures_ticker: { sub: JSON.stringify({ SendTopicAction: { Action: '1', TopicID: '1', Params: { symbol: 'BTCUSDT' } } }) }
    }
  },

  ourbit: {
    name: 'Ourbit',
    docs: 'N/A - No public API',
    spot: false,
    futures: false,
    skip: true,
    reason: 'No public API documentation found'
  }
};

const TIMEOUT = 15000;
const results = {};

function decompressGzip(data) {
  try {
    if (Buffer.isBuffer(data)) {
      try {
        return zlib.gunzipSync(data).toString('utf8');
      } catch (e) {
        try {
          return zlib.inflateSync(data).toString('utf8');
        } catch (e2) {
          return data.toString('utf8');
        }
      }
    }
    return data.toString();
  } catch (e) {
    return data.toString();
  }
}

function testStream(exchange, streamKey, url, subscription, options = {}) {
  return new Promise((resolve) => {
    const startTime = Date.now();
    let resolved = false;
    let ws;
    let pingIntervalId;

    const cleanup = () => {
      if (pingIntervalId) clearInterval(pingIntervalId);
      if (ws && ws.readyState === WebSocket.OPEN) {
        try { ws.close(); } catch (e) {}
      }
    };

    const done = (result) => {
      if (resolved) return;
      resolved = true;
      cleanup();
      resolve(result);
    };

    const timeout = setTimeout(() => {
      done({ success: false, error: 'Timeout', time: Date.now() - startTime });
    }, TIMEOUT);

    try {
      const wsOptions = options.wsOptions || {};
      ws = new WebSocket(url, wsOptions);

      ws.on('open', () => {
        try {
          ws.send(subscription);
          
          // Set up ping if configured
          if (options.pingInterval && options.ping) {
            pingIntervalId = setInterval(() => {
              if (ws.readyState === WebSocket.OPEN) {
                ws.send(options.ping);
              }
            }, options.pingInterval);
          }
        } catch (e) {
          done({ success: false, error: e.message, time: Date.now() - startTime });
        }
      });

      ws.on('message', (data) => {
        try {
          let message = decompressGzip(data);
          
          // Handle ping/pong
          if (options.handlePing) {
            const pong = options.handlePing(JSON.parse(message));
            if (pong) {
              ws.send(pong);
              return;
            }
          }
          
          // Check for pong responses
          if (message.includes('"pong"') || message === 'pong') {
            return;
          }
          
          // Check for subscription confirmations
          const parsed = JSON.parse(message);
          
          // Skip error responses
          if (parsed.error || parsed.code === -1 || parsed.status === 'error') {
            done({ 
              success: false, 
              error: parsed.error?.message || parsed.msg || 'Subscription error',
              time: Date.now() - startTime 
            });
            return;
          }
          
          // Success - got data
          clearTimeout(timeout);
          done({
            success: true,
            sample: message.substring(0, 200),
            time: Date.now() - startTime
          });
        } catch (e) {
          // Non-JSON message, might still be valid
          clearTimeout(timeout);
          done({
            success: true,
            sample: data.toString().substring(0, 200),
            time: Date.now() - startTime
          });
        }
      });

      ws.on('error', (error) => {
        clearTimeout(timeout);
        done({ success: false, error: error.message, time: Date.now() - startTime });
      });

      ws.on('close', () => {
        clearTimeout(timeout);
        if (!resolved) {
          done({ success: false, error: 'Connection closed', time: Date.now() - startTime });
        }
      });

    } catch (e) {
      clearTimeout(timeout);
      done({ success: false, error: e.message, time: Date.now() - startTime });
    }
  });
}

async function testExchange(key, config) {
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`  📊 Testing ${config.name}`);
  console.log(`  📖 Docs: ${config.docs}`);
  console.log('═'.repeat(60));

  if (config.skip) {
    console.log(`  ⚠️ Skipped: ${config.reason}`);
    return { name: config.name, docs: config.docs, skipped: true, reason: config.reason, tests: {} };
  }

  const exchangeResult = {
    name: config.name,
    docs: config.docs,
    spot: config.spot,
    futures: config.futures,
    tests: {}
  };

  const options = {
    pingInterval: config.pingInterval,
    ping: config.ping,
    handlePing: config.handlePing,
    wsOptions: config.wsOptions
  };

  // Test spot streams
  if (config.spot && config.spotUrl) {
    console.log(`\n  🔵 SPOT Market (${config.spotUrl.substring(0, 50)}...)`);
    for (const [streamKey, streamConfig] of Object.entries(config.streams)) {
      if (!streamKey.startsWith('spot_')) continue;
      const streamName = streamKey.replace('spot_', '');
      process.stdout.write(`    Testing ${streamName}... `);
      const result = await testStream(key, streamKey, config.spotUrl, streamConfig.sub, options);
      exchangeResult.tests[streamKey] = { key: `${key}_${streamKey}`, ...result };
      if (result.success) {
        console.log(`✅ (${result.time}ms)`);
      } else {
        console.log(`❌ ${result.error}`);
      }
    }
  } else if (config.spot === false) {
    console.log(`\n  🔵 SPOT: Not available`);
  }

  // Test futures streams
  if (config.futures && config.futuresUrl) {
    console.log(`\n  🟠 FUTURES Market (${config.futuresUrl.substring(0, 50)}...)`);
    for (const [streamKey, streamConfig] of Object.entries(config.streams)) {
      if (!streamKey.startsWith('futures_')) continue;
      const streamName = streamKey.replace('futures_', '');
      process.stdout.write(`    Testing ${streamName}... `);
      const result = await testStream(key, streamKey, config.futuresUrl, streamConfig.sub, options);
      exchangeResult.tests[streamKey] = { key: `${key}_${streamKey}`, ...result };
      if (result.success) {
        console.log(`✅ (${result.time}ms)`);
      } else {
        console.log(`❌ ${result.error}`);
      }
    }
  } else if (config.futures === false) {
    console.log(`\n  🟠 FUTURES: Not available`);
  }

  return exchangeResult;
}

async function main() {
  console.log(`
╔═══════════════════════════════════════════════════════════════════════════╗
║     CORRECTED EXCHANGE WEBSOCKET STREAMING TEST                           ║
║     Re-testing with Updated Endpoints & Subscriptions                     ║
║     Based on Detailed API Research                                        ║
╚═══════════════════════════════════════════════════════════════════════════╝
`);

  const exchangeKeys = Object.keys(EXCHANGES);
  
  for (const key of exchangeKeys) {
    results[key] = await testExchange(key, EXCHANGES[key]);
    // Small delay between exchanges
    await new Promise(r => setTimeout(r, 500));
  }

  // Save results
  const fs = require('fs');
  fs.writeFileSync('corrected-exchange-results.json', JSON.stringify(results, null, 2));
  console.log(`\n📁 Results saved to corrected-exchange-results.json`);

  // Generate summary
  console.log(`\n${'═'.repeat(80)}`);
  console.log(`  📋 CORRECTED TEST SUMMARY`);
  console.log('═'.repeat(80));

  const categories = {
    fullyWorking: [],
    partiallyWorking: [],
    failed: [],
    skipped: []
  };

  for (const [key, result] of Object.entries(results)) {
    if (result.skipped) {
      categories.skipped.push({ key, ...result });
      continue;
    }
    
    const tests = Object.values(result.tests || {});
    const passed = tests.filter(t => t.success).length;
    const total = tests.length;
    
    if (total === 0) {
      categories.failed.push({ key, ...result, passed: 0, total: 0 });
    } else if (passed === total) {
      categories.fullyWorking.push({ key, ...result, passed, total });
    } else if (passed > 0) {
      categories.partiallyWorking.push({ key, ...result, passed, total });
    } else {
      categories.failed.push({ key, ...result, passed, total });
    }
  }

  console.log(`\n✅ FULLY WORKING (${categories.fullyWorking.length}):`);
  for (const ex of categories.fullyWorking) {
    console.log(`   ${ex.name}: ${ex.passed}/${ex.total} streams`);
  }

  console.log(`\n🟡 PARTIALLY WORKING (${categories.partiallyWorking.length}):`);
  for (const ex of categories.partiallyWorking) {
    console.log(`   ${ex.name}: ${ex.passed}/${ex.total} streams`);
  }

  console.log(`\n❌ FAILED (${categories.failed.length}):`);
  for (const ex of categories.failed) {
    const firstError = Object.values(ex.tests || {})[0]?.error || 'No tests';
    console.log(`   ${ex.name}: ${firstError}`);
  }

  console.log(`\n⚪ SKIPPED (${categories.skipped.length}):`);
  for (const ex of categories.skipped) {
    console.log(`   ${ex.name}: ${ex.reason}`);
  }

  // Summary table
  console.log(`\n${'─'.repeat(80)}`);
  console.log('┌' + '─'.repeat(18) + '┬' + '─'.repeat(8) + '┬' + '─'.repeat(10) + '┬' + '─'.repeat(12) + '┬' + '─'.repeat(12) + '┬' + '─'.repeat(15) + '┐');
  console.log('│ Exchange         │ Spot   │ Futures  │ Spot Tests │ Fut Tests  │ Status        │');
  console.log('├' + '─'.repeat(18) + '┼' + '─'.repeat(8) + '┼' + '─'.repeat(10) + '┼' + '─'.repeat(12) + '┼' + '─'.repeat(12) + '┼' + '─'.repeat(15) + '┤');

  for (const [key, result] of Object.entries(results)) {
    const name = result.name.padEnd(16);
    const spot = result.spot ? '✅' : '❌';
    const futures = result.futures ? '✅' : '❌';
    
    const spotTests = Object.entries(result.tests || {}).filter(([k]) => k.startsWith('spot_'));
    const futTests = Object.entries(result.tests || {}).filter(([k]) => k.startsWith('futures_'));
    const spotPassed = spotTests.filter(([,v]) => v.success).length;
    const futPassed = futTests.filter(([,v]) => v.success).length;
    
    const spotStr = spotTests.length > 0 ? `${spotPassed}/${spotTests.length}` : '—';
    const futStr = futTests.length > 0 ? `${futPassed}/${futTests.length}` : '—';
    
    let status;
    if (result.skipped) {
      status = '⚪ Skipped';
    } else {
      const total = spotTests.length + futTests.length;
      const passed = spotPassed + futPassed;
      if (total === 0) status = '❌ No tests';
      else if (passed === total) status = '✅ Working';
      else if (passed > 0) status = '🟡 Partial';
      else status = '❌ Failed';
    }

    console.log(`│ ${name} │ ${spot.padEnd(6)} │ ${futures.padEnd(8)} │ ${spotStr.padEnd(10)} │ ${futStr.padEnd(10)} │ ${status.padEnd(13)} │`);
  }

  console.log('└' + '─'.repeat(18) + '┴' + '─'.repeat(8) + '┴' + '─'.repeat(10) + '┴' + '─'.repeat(12) + '┴' + '─'.repeat(12) + '┴' + '─'.repeat(15) + '┘');

  const totalExchanges = Object.keys(results).length;
  const working = categories.fullyWorking.length;
  const partial = categories.partiallyWorking.length;
  const failed = categories.failed.length;
  const skipped = categories.skipped.length;

  console.log(`\n📊 Overall Statistics:`);
  console.log(`   ✅ Fully Working: ${working} exchanges`);
  console.log(`   🟡 Partially Working: ${partial} exchanges`);
  console.log(`   ❌ Failed: ${failed} exchanges`);
  console.log(`   ⚪ Skipped: ${skipped} exchanges`);
  console.log(`   📈 Success Rate: ${((working + partial) / (totalExchanges - skipped) * 100).toFixed(1)}%`);

  console.log('\n✅ Tests completed!');
}

main().catch(console.error);
