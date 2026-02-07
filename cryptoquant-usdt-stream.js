/**
 * CryptoQuant USDT Data Streamer
 * ================================
 * Streams on-chain USDT (Tether) data from CryptoQuant's REST API v1.
 *
 * CryptoQuant does NOT offer WebSocket/streaming endpoints — their API is
 * purely REST-based. This module implements a **polling-based streaming
 * pattern**: it periodically fetches the latest data and emits new data
 * points through an EventEmitter, giving you a real-time-like stream.
 *
 * USDT Endpoints Available (under /v1/stablecoin/...):
 * ─────────────────────────────────────────────────────
 * EXCHANGE FLOWS:
 *   • exchange-reserve          – USDT held on exchanges
 *   • exchange-inflow           – USDT flowing into exchanges
 *   • exchange-outflow          – USDT flowing out of exchanges
 *   • exchange-netflow          – net inflow minus outflow
 *   • exchange-inflow-mean      – average inflow tx size
 *   • exchange-outflow-mean     – average outflow tx size
 *   • exchange-inflow-total     – total inflow count
 *   • exchange-outflow-total    – total outflow count
 *
 * FLOW INDICATORS:
 *   • exchange-whale-ratio      – whale share of exchange flows
 *   • fund-flow-ratio           – ratio of fund flows
 *   • addresses-count           – active address count
 *
 * MARKET DATA:
 *   • price-ohlcv               – OHLCV price data
 *   • market-cap                – total market capitalization
 *   • supply                    – circulating & total supply
 *
 * NETWORK DATA:
 *   • transactions-count        – on-chain transaction count
 *   • transfer-volume           – total transfer volume
 *   • active-addresses          – unique active addresses
 *   • tokens-transferred        – tokens transferred on-chain
 *
 * Authentication:
 *   • Bearer token in Authorization header
 *   • Or api_key query parameter
 *   • Get your key at: https://cryptoquant.com/settings/api
 *
 * Usage:
 *   CRYPTOQUANT_API_KEY=your_key node cryptoquant-usdt-stream.js
 */

const https = require('https');
const { EventEmitter } = require('events');

// ──────────────────────────────────────────────────────────────────────────────
// CONFIGURATION
// ──────────────────────────────────────────────────────────────────────────────

const CONFIG = {
  API_KEY: process.env.CRYPTOQUANT_API_KEY || '',
  BASE_URL: 'https://api.cryptoquant.com',
  API_VERSION: 'v1',

  // Polling intervals (milliseconds)
  POLL_INTERVAL_FAST: 30_000,       // 30s – for exchange flows
  POLL_INTERVAL_MEDIUM: 60_000,     // 60s – for market data
  POLL_INTERVAL_SLOW: 300_000,      // 5min – for network data

  // Rate limit safety
  MAX_REQUESTS_PER_MINUTE: 18,      // Stay under 20 req/min (Pro plan)
  REQUEST_DELAY_MS: 2000,           // Delay between requests to avoid bursts

  // Data window
  DEFAULT_WINDOW: 'day',            // day, hour, block (depends on plan)
  DEFAULT_LIMIT: 10,                // Number of data points per request
  
  // Stream duration
  STREAM_DURATION_MS: 3 * 60 * 1000, // 3 minutes
};

// ──────────────────────────────────────────────────────────────────────────────
// USDT ENDPOINT DEFINITIONS
// ──────────────────────────────────────────────────────────────────────────────

const USDT_ENDPOINTS = {
  // ── Exchange Flows (require token + exchange) ──
  'exchange-reserve': {
    path: '/v1/stablecoin/exchange-flows/reserve',
    category: 'Exchange Flows',
    description: 'USDT(ERC20) held on all exchanges (total reserve)',
    params: { window: 'day', limit: 5, token: 'usdt_eth', exchange: 'all_exchange' },
    pollInterval: CONFIG.POLL_INTERVAL_FAST,
  },
  'exchange-inflow': {
    path: '/v1/stablecoin/exchange-flows/inflow',
    category: 'Exchange Flows',
    description: 'USDT(ERC20) flowing INTO exchanges (sell pressure)',
    params: { window: 'day', limit: 5, token: 'usdt_eth', exchange: 'all_exchange' },
    pollInterval: CONFIG.POLL_INTERVAL_FAST,
  },
  'exchange-outflow': {
    path: '/v1/stablecoin/exchange-flows/outflow',
    category: 'Exchange Flows',
    description: 'USDT(ERC20) flowing OUT of exchanges (accumulation)',
    params: { window: 'day', limit: 5, token: 'usdt_eth', exchange: 'all_exchange' },
    pollInterval: CONFIG.POLL_INTERVAL_FAST,
  },
  'exchange-netflow': {
    path: '/v1/stablecoin/exchange-flows/netflow',
    category: 'Exchange Flows',
    description: 'Net USDT(ERC20) flow (+ = entering exchanges)',
    params: { window: 'day', limit: 5, token: 'usdt_eth', exchange: 'all_exchange' },
    pollInterval: CONFIG.POLL_INTERVAL_FAST,
  },
  'exchange-tx-count': {
    path: '/v1/stablecoin/exchange-flows/transactions-count',
    category: 'Exchange Flows',
    description: 'USDT(ERC20) exchange transaction count',
    params: { window: 'day', limit: 5, token: 'usdt_eth', exchange: 'all_exchange' },
    pollInterval: CONFIG.POLL_INTERVAL_MEDIUM,
  },
  'exchange-addresses': {
    path: '/v1/stablecoin/exchange-flows/addresses-count',
    category: 'Exchange Flows',
    description: 'USDT(ERC20) exchange active addresses',
    params: { window: 'day', limit: 5, token: 'usdt_eth', exchange: 'all_exchange' },
    pollInterval: CONFIG.POLL_INTERVAL_MEDIUM,
  },

  // ── USDT TRC20 (Tron) Exchange Flows ──
  'trc20-reserve': {
    path: '/v1/stablecoin/exchange-flows/reserve',
    category: 'Exchange Flows (TRC20)',
    description: 'USDT(TRC20) held on all exchanges',
    params: { window: 'day', limit: 5, token: 'usdt_trx', exchange: 'all_exchange' },
    pollInterval: CONFIG.POLL_INTERVAL_FAST,
  },
  'trc20-netflow': {
    path: '/v1/stablecoin/exchange-flows/netflow',
    category: 'Exchange Flows (TRC20)',
    description: 'USDT(TRC20) net flow in/out of exchanges',
    params: { window: 'day', limit: 5, token: 'usdt_trx', exchange: 'all_exchange' },
    pollInterval: CONFIG.POLL_INTERVAL_FAST,
  },

  // ── Flow Indicators (require token + exchange) ──
  'exchange-supply-ratio': {
    path: '/v1/stablecoin/flow-indicator/exchange-supply-ratio',
    category: 'Flow Indicators',
    description: 'USDT exchange supply ratio (exchange held / total)',
    params: { window: 'day', limit: 5, token: 'usdt_eth', exchange: 'all_exchange' },
    pollInterval: CONFIG.POLL_INTERVAL_MEDIUM,
  },

  // ── Market Data (require token) ──
  'price-ohlcv': {
    path: '/v1/stablecoin/market-data/price-ohlcv',
    category: 'Market Data',
    description: 'USDT(ERC20) price OHLCV',
    params: { window: 'day', limit: 5, token: 'usdt_eth', symbol: 'usdt_eth_usd', exchange: 'all_exchange', market: 'spot' },
    pollInterval: CONFIG.POLL_INTERVAL_MEDIUM,
  },
  'capitalization': {
    path: '/v1/stablecoin/market-data/capitalization',
    category: 'Market Data',
    description: 'USDT(ERC20) market capitalization',
    params: { window: 'day', limit: 5, token: 'usdt_eth' },
    pollInterval: CONFIG.POLL_INTERVAL_MEDIUM,
  },

  // ── Network Data (require token) ──
  'supply': {
    path: '/v1/stablecoin/network-data/supply',
    category: 'Network Data',
    description: 'USDT(ERC20) circulating supply',
    params: { window: 'day', limit: 5, token: 'usdt_eth' },
    pollInterval: CONFIG.POLL_INTERVAL_SLOW,
  },
  'events-count': {
    path: '/v1/stablecoin/network-data/events-count',
    category: 'Network Data',
    description: 'USDT(ERC20) number of on-chain events',
    params: { window: 'day', limit: 5, token: 'usdt_eth' },
    pollInterval: CONFIG.POLL_INTERVAL_SLOW,
  },
  'tokens-transferred': {
    path: '/v1/stablecoin/network-data/tokens-transferred',
    category: 'Network Data',
    description: 'USDT(ERC20) volume of tokens transferred on-chain',
    params: { window: 'day', limit: 5, token: 'usdt_eth' },
    pollInterval: CONFIG.POLL_INTERVAL_SLOW,
  },
  'active-addresses': {
    path: '/v1/stablecoin/network-data/addresses-count',
    category: 'Network Data',
    description: 'USDT(ERC20) unique active addresses',
    params: { window: 'day', limit: 5, token: 'usdt_eth' },
    pollInterval: CONFIG.POLL_INTERVAL_SLOW,
  },

  // ── BTC Stablecoin Indicators ──
  'btc-stablecoin-supply-ratio': {
    path: '/v1/btc/market-indicator/stablecoin-supply-ratio',
    category: 'BTC Indicator',
    description: 'BTC Stablecoin Supply Ratio (BTC mcap / stablecoin supply)',
    params: { window: 'day', limit: 5 },
    pollInterval: CONFIG.POLL_INTERVAL_SLOW,
  },
};

// ──────────────────────────────────────────────────────────────────────────────
// HTTP CLIENT
// ──────────────────────────────────────────────────────────────────────────────

function apiRequest(endpointPath, params = {}) {
  return new Promise((resolve, reject) => {
    const queryParams = new URLSearchParams(params);
    const url = `${CONFIG.BASE_URL}${endpointPath}?${queryParams.toString()}`;

    const options = {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${CONFIG.API_KEY}`,
        'Accept': 'application/json',
        'User-Agent': 'CryptoQuant-USDT-Streamer/1.0',
      },
    };

    const req = https.get(url, options, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (parsed.status && parsed.status.code !== 200) {
            reject(new Error(`API Error ${parsed.status.code}: ${parsed.status.message}`));
          } else {
            resolve(parsed);
          }
        } catch (e) {
          reject(new Error(`JSON parse error: ${e.message}`));
        }
      });
    });

    req.on('error', (err) => reject(err));
    req.setTimeout(15000, () => {
      req.destroy();
      reject(new Error('Request timeout (15s)'));
    });
  });
}

// ──────────────────────────────────────────────────────────────────────────────
// RATE LIMITER
// ──────────────────────────────────────────────────────────────────────────────

class RateLimiter {
  constructor(maxPerMinute) {
    this.maxPerMinute = maxPerMinute;
    this.requests = [];
  }

  async waitForSlot() {
    const now = Date.now();
    this.requests = this.requests.filter((t) => now - t < 60_000);

    if (this.requests.length >= this.maxPerMinute) {
      const oldest = this.requests[0];
      const waitTime = 60_000 - (now - oldest) + 100;
      console.log(`⏳ Rate limit: waiting ${(waitTime / 1000).toFixed(1)}s`);
      await sleep(waitTime);
    }

    this.requests.push(Date.now());
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ──────────────────────────────────────────────────────────────────────────────
// CRYPTOQUANT USDT STREAMER (EventEmitter-based)
// ──────────────────────────────────────────────────────────────────────────────

class CryptoQuantUSDTStream extends EventEmitter {
  constructor(apiKey, options = {}) {
    super();
    this.apiKey = apiKey;
    CONFIG.API_KEY = apiKey;

    this.endpoints = options.endpoints || Object.keys(USDT_ENDPOINTS);
    this.window = options.window || CONFIG.DEFAULT_WINDOW;
    this.limit = options.limit || CONFIG.DEFAULT_LIMIT;
    this.rateLimiter = new RateLimiter(CONFIG.MAX_REQUESTS_PER_MINUTE);

    this.timers = [];
    this.running = false;
    this.lastData = {};        // Cache last data per endpoint to detect changes
    this.requestCount = 0;
    this.errorCount = 0;
    this.startTime = null;
  }

  // ── Start streaming ──
  async start() {
    if (!this.apiKey) {
      console.error('❌ No API key provided!');
      console.error('   Set CRYPTOQUANT_API_KEY environment variable or pass it to constructor.');
      console.error('   Get your key at: https://cryptoquant.com/settings/api');
      process.exit(1);
    }

    this.running = true;
    this.startTime = Date.now();

    console.log('');
    console.log('╔══════════════════════════════════════════════════════════════╗');
    console.log('║         CryptoQuant USDT On-Chain Data Streamer             ║');
    console.log('╠══════════════════════════════════════════════════════════════╣');
    console.log(`║  Endpoints: ${this.endpoints.length} metric(s)                                   ║`);
    console.log(`║  Window:    ${this.window.padEnd(48)}║`);
    console.log(`║  API:       ${CONFIG.BASE_URL.padEnd(48)}║`);
    console.log('╚══════════════════════════════════════════════════════════════╝');
    console.log('');

    // Initial fetch for all endpoints
    await this._initialFetch();

    // Set up polling timers for each endpoint
    this._startPolling();

    this.emit('started', {
      endpoints: this.endpoints,
      window: this.window,
      startTime: new Date().toISOString(),
    });
  }

  // ── Initial fetch of all endpoints ──
  async _initialFetch() {
    console.log('📡 Initial data fetch...\n');

    for (const endpointKey of this.endpoints) {
      if (!this.running) break;

      const def = USDT_ENDPOINTS[endpointKey];
      if (!def) {
        console.warn(`⚠️  Unknown endpoint: ${endpointKey}`);
        continue;
      }

      try {
        await this.rateLimiter.waitForSlot();
        const data = await this._fetchEndpoint(endpointKey);

        if (data) {
          this.lastData[endpointKey] = data;
          this.emit('data', { endpoint: endpointKey, ...def, data, isInitial: true });
          this._displayData(endpointKey, data, def);
        }
      } catch (err) {
        this.errorCount++;
        console.error(`  ❌ ${endpointKey}: ${err.message}`);
        this.emit('error', { endpoint: endpointKey, error: err.message });
      }

      // Small delay between initial requests
      await sleep(CONFIG.REQUEST_DELAY_MS);
    }

    console.log('\n✅ Initial fetch complete. Starting continuous polling...\n');
  }

  // ── Start polling loops ──
  _startPolling() {
    // Group endpoints by their poll interval
    const intervalGroups = {};

    for (const endpointKey of this.endpoints) {
      const def = USDT_ENDPOINTS[endpointKey];
      if (!def) continue;

      const interval = def.pollInterval;
      if (!intervalGroups[interval]) intervalGroups[interval] = [];
      intervalGroups[interval].push(endpointKey);
    }

    for (const [interval, keys] of Object.entries(intervalGroups)) {
      const ms = parseInt(interval);
      console.log(`⏰ Polling group (every ${ms / 1000}s): ${keys.join(', ')}`);

      const timer = setInterval(async () => {
        if (!this.running) return;

        for (const endpointKey of keys) {
          if (!this.running) break;

          try {
            await this.rateLimiter.waitForSlot();
            const data = await this._fetchEndpoint(endpointKey);
            const def = USDT_ENDPOINTS[endpointKey];

            if (data) {
              const isNew = this._hasNewData(endpointKey, data);
              this.lastData[endpointKey] = data;

              if (isNew) {
                this.emit('data', { endpoint: endpointKey, ...def, data, isInitial: false });
                this._displayData(endpointKey, data, def, true);
              } else {
                console.log(`  ⟳ ${endpointKey}: no new data yet`);
              }
            }
          } catch (err) {
            this.errorCount++;
            this.emit('error', { endpoint: endpointKey, error: err.message });
            console.error(`  ❌ ${endpointKey}: ${err.message}`);
          }

          await sleep(CONFIG.REQUEST_DELAY_MS);
        }
      }, ms);

      this.timers.push(timer);
    }

    console.log('');
  }

  // ── Fetch a single endpoint ──
  async _fetchEndpoint(endpointKey) {
    const def = USDT_ENDPOINTS[endpointKey];
    // Use endpoint-specific params (includes required token, exchange)
    // and merge with user-level overrides for window/limit
    const params = {
      ...def.params,
      window: def.params.window || this.window,
      limit: def.params.limit || this.limit,
    };

    this.requestCount++;
    const response = await apiRequest(def.path, params);
    return response.result?.data || response.result || response;
  }

  // ── Check if data is new compared to last fetch ──
  _hasNewData(endpointKey, newData) {
    const oldData = this.lastData[endpointKey];
    if (!oldData) return true;

    // Compare last timestamp or stringified first element
    const oldStr = JSON.stringify(Array.isArray(oldData) ? oldData[0] : oldData);
    const newStr = JSON.stringify(Array.isArray(newData) ? newData[0] : newData);
    return oldStr !== newStr;
  }

  // ── Display data in terminal ──
  _displayData(endpointKey, data, def, isUpdate = false) {
    const prefix = isUpdate ? '🔄 UPDATE' : '📊 DATA';
    const time = new Date().toISOString();

    console.log(`\n${prefix} | ${def.category} | ${endpointKey}`);
    console.log(`  ⏱  ${time}`);
    console.log(`  📝 ${def.description}`);

    if (Array.isArray(data)) {
      // Show latest data point(s)
      const latest = data.slice(0, 3);
      latest.forEach((point, i) => {
        const dateStr = point.date || point.datetime || point.timestamp || 'N/A';
        const values = Object.entries(point)
          .filter(([k]) => !['date', 'datetime', 'timestamp', '_id'].includes(k))
          .map(([k, v]) => {
            if (typeof v === 'number') {
              return `${k}: ${v >= 1e6 ? (v / 1e6).toFixed(2) + 'M' : v >= 1e3 ? (v / 1e3).toFixed(2) + 'K' : v.toFixed(4)}`;
            }
            return `${k}: ${v}`;
          })
          .join(' | ');

        console.log(`  ${i === 0 ? '►' : ' '} [${dateStr}] ${values}`);
      });
    } else if (typeof data === 'object') {
      const entries = Object.entries(data)
        .filter(([k]) => !['_id'].includes(k))
        .slice(0, 5);
      entries.forEach(([k, v]) => {
        console.log(`    ${k}: ${v}`);
      });
    }
  }

  // ── Stop the streamer ──
  stop() {
    this.running = false;
    this.timers.forEach((t) => clearInterval(t));
    this.timers = [];

    const duration = ((Date.now() - this.startTime) / 1000).toFixed(0);

    console.log('\n');
    console.log('╔══════════════════════════════════════════════════════════════╗');
    console.log('║                  Stream Stopped                             ║');
    console.log('╠══════════════════════════════════════════════════════════════╣');
    console.log(`║  Duration:   ${duration}s`.padEnd(63) + '║');
    console.log(`║  Requests:   ${this.requestCount}`.padEnd(63) + '║');
    console.log(`║  Errors:     ${this.errorCount}`.padEnd(63) + '║');
    console.log('╚══════════════════════════════════════════════════════════════╝');

    this.emit('stopped', {
      duration: parseInt(duration),
      requests: this.requestCount,
      errors: this.errorCount,
    });
  }

  // ── Get stats ──
  getStats() {
    return {
      running: this.running,
      uptime: this.startTime ? Math.floor((Date.now() - this.startTime) / 1000) : 0,
      requests: this.requestCount,
      errors: this.errorCount,
      endpoints: this.endpoints.length,
      cachedMetrics: Object.keys(this.lastData),
    };
  }

  // ── Get latest cached data for an endpoint ──
  getLatest(endpointKey) {
    return this.lastData[endpointKey] || null;
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// DISCOVERY: Test API connectivity and list available endpoints
// ──────────────────────────────────────────────────────────────────────────────

async function discoverEndpoints(apiKey) {
  CONFIG.API_KEY = apiKey;
  console.log('\n🔍 Discovering available CryptoQuant Stablecoin endpoints...\n');

  const results = { accessible: [], restricted: [], errors: [] };

  for (const [key, def] of Object.entries(USDT_ENDPOINTS)) {
    try {
      const response = await apiRequest(def.path, { window: 'day', limit: 1 });
      const dataPoints = response.result?.data?.length || 0;
      results.accessible.push({ key, path: def.path, category: def.category, dataPoints });
      console.log(`  ✅ ${key.padEnd(25)} | ${def.category.padEnd(16)} | ${dataPoints} data point(s)`);
    } catch (err) {
      if (err.message.includes('403') || err.message.includes('upgrade')) {
        results.restricted.push({ key, path: def.path, error: err.message });
        console.log(`  🔒 ${key.padEnd(25)} | ${def.category.padEnd(16)} | Restricted (upgrade plan)`);
      } else {
        results.errors.push({ key, path: def.path, error: err.message });
        console.log(`  ❌ ${key.padEnd(25)} | ${def.category.padEnd(16)} | ${err.message}`);
      }
    }
    await sleep(CONFIG.REQUEST_DELAY_MS);
  }

  console.log('\n📋 Discovery Summary:');
  console.log(`   Accessible:  ${results.accessible.length}`);
  console.log(`   Restricted:  ${results.restricted.length}`);
  console.log(`   Errors:      ${results.errors.length}`);

  return results;
}

// ──────────────────────────────────────────────────────────────────────────────
// MAIN EXECUTION
// ──────────────────────────────────────────────────────────────────────────────

async function main() {
  const apiKey = process.env.CRYPTOQUANT_API_KEY;

  if (!apiKey) {
    console.log('');
    console.log('╔══════════════════════════════════════════════════════════════╗');
    console.log('║           CryptoQuant USDT Data Streamer                    ║');
    console.log('╠══════════════════════════════════════════════════════════════╣');
    console.log('║                                                             ║');
    console.log('║  ❌ No API key found!                                       ║');
    console.log('║                                                             ║');
    console.log('║  Set your CryptoQuant API key:                              ║');
    console.log('║                                                             ║');
    console.log('║  Windows (PowerShell):                                      ║');
    console.log('║    $env:CRYPTOQUANT_API_KEY = "your-api-key-here"           ║');
    console.log('║    node cryptoquant-usdt-stream.js                          ║');
    console.log('║                                                             ║');
    console.log('║  Linux/Mac:                                                 ║');
    console.log('║    CRYPTOQUANT_API_KEY=your-key node cryptoquant-usdt-stream║');
    console.log('║                                                             ║');
    console.log('║  Get your API key at:                                       ║');
    console.log('║    https://cryptoquant.com/settings/api                     ║');
    console.log('║                                                             ║');
    console.log('║  Plan Requirements:                                         ║');
    console.log('║    • Basic (Free)  : 50 req/day, daily window, 7-day hist   ║');
    console.log('║    • Advanced ($29): 100 req/day, daily window, 7-day hist  ║');
    console.log('║    • Pro ($99)     : 20 req/min, daily window, 1-year hist  ║');
    console.log('║    • Premium ($799): 800 req/min, all windows, full history ║');
    console.log('║                                                             ║');
    console.log('╚══════════════════════════════════════════════════════════════╝');
    console.log('');
    process.exit(1);
  }

  const mode = process.argv[2] || 'stream';

  switch (mode) {
    case 'discover':
      // Run endpoint discovery to see what's accessible
      await discoverEndpoints(apiKey);
      break;

    case 'stream':
    default:
      // Start the full stream
      const streamer = new CryptoQuantUSDTStream(apiKey, {
        // You can filter to specific endpoints:
        // endpoints: ['exchange-reserve', 'exchange-netflow', 'market-cap', 'supply'],
        window: 'day',
        limit: 5,
      });

      // Event listeners
      streamer.on('data', (event) => {
        // You can process data here — write to DB, forward to WebSocket server, etc.
        // console.log('EVENT:', JSON.stringify(event, null, 2));
      });

      streamer.on('error', (event) => {
        // Log errors for monitoring
      });

      streamer.on('stopped', (stats) => {
        console.log('\nFinal stats:', JSON.stringify(stats));
      });

      // Start streaming
      await streamer.start();

      // Print stats every 2 minutes
      const statsInterval = setInterval(() => {
        if (!streamer.running) return;
        const stats = streamer.getStats();
        console.log(`\n📈 Stats | Uptime: ${stats.uptime}s | Requests: ${stats.requests} | Errors: ${stats.errors}`);
      }, 120_000);

      // Auto-stop after configured duration
      setTimeout(() => {
        clearInterval(statsInterval);
        streamer.stop();
        process.exit(0);
      }, CONFIG.STREAM_DURATION_MS);

      // Graceful shutdown
      process.on('SIGINT', () => {
        console.log('\n\n⚡ Shutting down gracefully...');
        clearInterval(statsInterval);
        streamer.stop();
        process.exit(0);
      });

      break;
  }
}

// ──────────────────────────────────────────────────────────────────────────────
// EXPORTS (for use as a module)
// ──────────────────────────────────────────────────────────────────────────────

module.exports = {
  CryptoQuantUSDTStream,
  discoverEndpoints,
  USDT_ENDPOINTS,
  CONFIG,
};

// Run if executed directly
if (require.main === module) {
  main().catch((err) => {
    console.error('Fatal error:', err);
    process.exit(1);
  });
}
