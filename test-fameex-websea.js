#!/usr/bin/env node
// ═══════════════════════════════════════════════════════════════
// FameEX + Websea Deep Research Tester
// Tests orderbook & trades for both SPOT and FUTURES
// ═══════════════════════════════════════════════════════════════

const https = require('https');

const results = {};

function httpGet(url, label, timeout = 10000) {
  return new Promise(resolve => {
    const start = Date.now();
    const req = https.get(url, { timeout }, res => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => {
        const elapsed = Date.now() - start;
        let parsed = null;
        let isJson = false;
        try { parsed = JSON.parse(data); isJson = true; } catch(e) {}
        
        const result = {
          url,
          status: res.statusCode,
          elapsed: `${elapsed}ms`,
          isJson,
          dataPreview: data.substring(0, 500),
          hasData: false,
          dataType: null
        };

        // Detect data quality
        if (isJson && parsed) {
          if (parsed.asks || parsed.bids) {
            result.hasData = true;
            result.dataType = 'orderbook';
            result.asks = Array.isArray(parsed.asks) ? parsed.asks.length : 0;
            result.bids = Array.isArray(parsed.bids) ? parsed.bids.length : 0;
          } else if (parsed.result && (parsed.result.asks || parsed.result.bids)) {
            result.hasData = true;
            result.dataType = 'orderbook';
            result.asks = Array.isArray(parsed.result.asks) ? parsed.result.asks.length : 0;
            result.bids = Array.isArray(parsed.result.bids) ? parsed.result.bids.length : 0;
          } else if (parsed.data && Array.isArray(parsed.data) && parsed.data.length > 0) {
            result.hasData = true;
            result.dataType = 'trades/list';
            result.count = parsed.data.length;
            result.sample = parsed.data[0];
          } else if (parsed.result && parsed.result.data && Array.isArray(parsed.result.data)) {
            result.hasData = true;
            result.dataType = 'trades/list';
            result.count = parsed.result.data.length;
            result.sample = parsed.result.data[0];
          } else if (Array.isArray(parsed) && parsed.length > 0) {
            result.hasData = true;
            result.dataType = 'array';
            result.count = parsed.length;
            result.sample = parsed[0];
          }
          // Check for error codes
          if (parsed.code && parsed.code !== 200 && parsed.code !== 0) {
            result.errorCode = parsed.code;
            result.errorMsg = parsed.msg || parsed.message;
          }
          if (parsed.errno && parsed.errno !== 0) {
            result.errorCode = parsed.errno;
            result.errorMsg = parsed.errmsg;
          }
        }

        resolve(result);
      });
    });
    req.on('error', err => {
      resolve({ url, error: err.message, elapsed: `${Date.now() - start}ms` });
    });
    req.on('timeout', () => {
      req.destroy();
      resolve({ url, error: 'TIMEOUT', elapsed: `${timeout}ms` });
    });
  });
}

function printResult(label, r) {
  const icon = r.hasData ? '✅' : (r.error ? '❌' : '⚠️');
  console.log(`\n${icon} ${label}`);
  console.log(`   URL: ${r.url}`);
  if (r.error) {
    console.log(`   Error: ${r.error}`);
    return;
  }
  console.log(`   Status: ${r.status} | ${r.elapsed} | JSON: ${r.isJson}`);
  if (r.errorCode) console.log(`   ⚠️  API Error: code=${r.errorCode}, msg=${r.errorMsg}`);
  if (r.dataType === 'orderbook') {
    console.log(`   📊 Orderbook: ${r.asks} asks, ${r.bids} bids`);
  } else if (r.dataType === 'trades/list' || r.dataType === 'array') {
    console.log(`   📊 ${r.count} items returned`);
    if (r.sample) console.log(`   Sample: ${JSON.stringify(r.sample).substring(0, 200)}`);
  }
  if (!r.hasData && r.isJson) {
    console.log(`   Data: ${r.dataPreview.substring(0, 300)}`);
  }
  if (!r.isJson) {
    console.log(`   Raw: ${r.dataPreview.substring(0, 200)}`);
  }
}

async function testFameEX() {
  console.log('\n' + '═'.repeat(70));
  console.log('  FAMEEX DEEP RESEARCH TEST');
  console.log('═'.repeat(70));

  // ── SPOT ──
  console.log('\n── SPOT ENDPOINTS ──');

  const spotTests = {
    'FameEX Spot Ticker (v2)':       'https://api.fameex.com/v2/public/ticker',
    'FameEX Spot Orderbook (v2)':    'https://api.fameex.com/v2/public/orderbook?symbol=BTCUSDT&limit=10',
    'FameEX Spot Trades (sapi/v1)':  'https://api.fameex.com/sapi/v1/trades?symbol=BTCUSDT&limit=10',
    'FameEX Spot Trades (v2 alt)':   'https://api.fameex.com/v2/public/trades?symbol=BTCUSDT&limit=10',
    'FameEX Spot Orderbook (sapi)':  'https://api.fameex.com/sapi/v1/depth?symbol=BTCUSDT&limit=10',
    'FameEX Spot Trades (BTC-USDT)': 'https://api.fameex.com/v2/public/trades?symbol=BTC-USDT&limit=10',
    'FameEX Spot Orderbook (BTC-USDT)': 'https://api.fameex.com/v2/public/orderbook?symbol=BTC-USDT&limit=10',
    // openapi domain
    'FameEX openapi Ticker':         'https://openapi.fameex.com/v2/public/ticker',
    'FameEX openapi Orderbook':      'https://openapi.fameex.com/v2/public/orderbook?symbol=BTCUSDT&limit=10',
    'FameEX openapi Trades':         'https://openapi.fameex.com/sapi/v1/trades?symbol=BTCUSDT&limit=10',
    'FameEX openapi Depth':          'https://openapi.fameex.com/sapi/v1/depth?symbol=BTCUSDT&limit=10',
  };

  const spotResults = {};
  for (const [label, url] of Object.entries(spotTests)) {
    const r = await httpGet(url, label);
    spotResults[label] = r;
    printResult(label, r);
  }

  // ── FUTURES ──
  console.log('\n── FUTURES ENDPOINTS ──');

  const futuresTests = {
    'FameEX Futures Ticker (v2)':      'https://api.fameex.com/v2/public/futures/ticker',
    'FameEX Futures Orderbook (v2)':   'https://api.fameex.com/v2/public/futures/orderbook?symbol=BTCUSDT&limit=10',
    'FameEX Futures Trades (v2)':      'https://api.fameex.com/v2/public/futures/trades?symbol=BTCUSDT&limit=10',
    'FameEX Futures Depth (v2)':       'https://api.fameex.com/v2/public/futures/depth?symbol=BTCUSDT&limit=10',
    'FameEX Futures Ticker (sapi)':    'https://api.fameex.com/sapi/v1/futures/ticker?symbol=BTCUSDT',
    'FameEX Futures Orderbook (sapi)': 'https://api.fameex.com/sapi/v1/futures/depth?symbol=BTCUSDT&limit=10',
    'FameEX Futures Trades (sapi)':    'https://api.fameex.com/sapi/v1/futures/trades?symbol=BTCUSDT&limit=10',
    // Perpetual path patterns
    'FameEX Perp Ticker':             'https://api.fameex.com/v2/public/perpetual/ticker?symbol=BTCUSDT',
    'FameEX Perp Orderbook':          'https://api.fameex.com/v2/public/perpetual/orderbook?symbol=BTCUSDT&limit=10',
    'FameEX Perp Trades':             'https://api.fameex.com/v2/public/perpetual/trades?symbol=BTCUSDT&limit=10',
    // fapi path patterns
    'FameEX fapi Ticker':             'https://api.fameex.com/fapi/v1/ticker?symbol=BTCUSDT',
    'FameEX fapi Orderbook':          'https://api.fameex.com/fapi/v1/depth?symbol=BTCUSDT&limit=10',
    'FameEX fapi Trades':             'https://api.fameex.com/fapi/v1/trades?symbol=BTCUSDT&limit=10',
    // Swap path patterns
    'FameEX Swap Ticker':             'https://api.fameex.com/swap/v1/ticker?symbol=BTCUSDT',
    'FameEX Swap Orderbook':          'https://api.fameex.com/swap/v1/depth?symbol=BTCUSDT&limit=10',
    'FameEX Swap Trades':             'https://api.fameex.com/swap/v1/trades?symbol=BTCUSDT&limit=10',
    // Contract path
    'FameEX Contract Ticker':         'https://api.fameex.com/v2/public/contract/ticker',
    'FameEX Contract Orderbook':      'https://api.fameex.com/v2/public/contract/orderbook?symbol=BTCUSDT&limit=10',
    // Also try with perpetual symbol format
    'FameEX Futures Trades (BTCUSDT_PERP)':  'https://api.fameex.com/sapi/v1/futures/trades?symbol=BTCUSDT_PERP&limit=10',
    'FameEX Futures Orderbook (BTCUSDT_PERP)': 'https://api.fameex.com/sapi/v1/futures/depth?symbol=BTCUSDT_PERP&limit=10',
  };

  const futuresResults = {};
  for (const [label, url] of Object.entries(futuresTests)) {
    const r = await httpGet(url, label);
    futuresResults[label] = r;
    printResult(label, r);
  }

  results.fameex = { spot: spotResults, futures: futuresResults };
}

async function testWebsea() {
  console.log('\n' + '═'.repeat(70));
  console.log('  WEBSEA DEEP RESEARCH TEST');
  console.log('═'.repeat(70));

  // ── SPOT ──
  console.log('\n── SPOT ENDPOINTS (documented) ──');

  const spotTests = {
    'Websea Spot Orderbook':       'https://oapi.websea.com/v1/spot/depth?symbol=BTC-USDT&size=10',
    'Websea Spot Trades':          'https://oapi.websea.com/v1/spot/trade?symbol=BTC-USDT&size=10',
    'Websea Spot 24h Ticker':      'https://oapi.websea.com/v1/spot/24kline?symbol=BTC-USDT',
    'Websea Spot Ticker List':     'https://oapi.websea.com/v1/spot/24kline-list',
    'Websea Spot Symbol List':     'https://oapi.websea.com/v1/spot/symbol-list',
    'Websea Spot Precision':       'https://oapi.websea.com/v1/spot/precision?symbol=BTC-USDT',
    // Try BTCUSDT format too
    'Websea Spot Orderbook (BTCUSDT)':  'https://oapi.websea.com/v1/spot/depth?symbol=BTCUSDT&size=10',
    'Websea Spot Trades (BTCUSDT)':     'https://oapi.websea.com/v1/spot/trade?symbol=BTCUSDT&size=10',
  };

  const spotResults = {};
  for (const [label, url] of Object.entries(spotTests)) {
    const r = await httpGet(url, label);
    spotResults[label] = r;
    printResult(label, r);
  }

  // ── FUTURES ──
  console.log('\n── FUTURES ENDPOINTS (documented) ──');

  const futuresTests = {
    'Websea Futures Orderbook':     'https://oapi.websea.com/v1/futures/depth?symbol=BTC-USDT&limit=10',
    'Websea Futures Trades':        'https://oapi.websea.com/v1/futures/trade?symbol=BTC-USDT&size=10',
    'Websea Futures 24h Ticker':    'https://oapi.websea.com/v1/futures/24kline?symbol=BTC-USDT',
    'Websea Futures Ticker List':   'https://oapi.websea.com/v1/futures/24kline-list',
    'Websea Futures Contract Info': 'https://oapi.websea.com/v1/futures/futures?symbol=BTC-USDT',
    'Websea Futures Symbol':        'https://oapi.websea.com/v1/futures/symbol?symbol=BTC-USDT',
    'Websea Futures 24h Product':   'https://oapi.websea.com/v1/futures/24hr?symbol=BTC-USDT',
    'Websea Futures Mark Price':    'https://oapi.websea.com/v1/futures/index-price?symbol=BTC-USDT',
    'Websea Futures Open Interest': 'https://oapi.websea.com/v1/futures/hold?symbol=BTC-USDT',
    'Websea Futures Funding Rate':  'https://oapi.websea.com/v1/futures/rate?symbol=BTC-USDT',
    // Try BTCUSDT format
    'Websea Futures Orderbook (BTCUSDT)': 'https://oapi.websea.com/v1/futures/depth?symbol=BTCUSDT&limit=10',
    'Websea Futures Trades (BTCUSDT)':    'https://oapi.websea.com/v1/futures/trade?symbol=BTCUSDT&size=10',
  };

  const futuresResults = {};
  for (const [label, url] of Object.entries(futuresTests)) {
    const r = await httpGet(url, label);
    futuresResults[label] = r;
    printResult(label, r);
  }

  results.websea = { spot: spotResults, futures: futuresResults };
}

async function main() {
  console.log('╔═══════════════════════════════════════════════════════════════════╗');
  console.log('║  FAMEEX + WEBSEA DEEP RESEARCH - Orderbook & Trades Test        ║');
  console.log('║  Testing SPOT + FUTURES endpoints for both exchanges            ║');
  console.log('╚═══════════════════════════════════════════════════════════════════╝');

  await testFameEX();
  await testWebsea();

  // ── SUMMARY ──
  console.log('\n\n' + '═'.repeat(70));
  console.log('  SUMMARY');
  console.log('═'.repeat(70));

  for (const [exchange, data] of Object.entries(results)) {
    console.log(`\n📊 ${exchange.toUpperCase()}`);
    for (const [category, tests] of Object.entries(data)) {
      const working = Object.entries(tests).filter(([,r]) => r.hasData).map(([l]) => l);
      const failed = Object.entries(tests).filter(([,r]) => !r.hasData && !r.error).map(([l]) => l);
      const errors = Object.entries(tests).filter(([,r]) => r.error).map(([l]) => l);
      console.log(`  ${category}: ${working.length} working, ${failed.length} no-data, ${errors.length} errors`);
      if (working.length > 0) {
        working.forEach(w => console.log(`    ✅ ${w}`));
      }
    }
  }

  // Save results
  const fs = require('fs');
  fs.writeFileSync('fameex-websea-results.json', JSON.stringify(results, null, 2));
  console.log('\n💾 Results saved to fameex-websea-results.json');
}

main().catch(console.error);
