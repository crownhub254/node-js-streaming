/**
 * ╔═══════════════════════════════════════════════════════════════════════════╗
 * ║  ENHANCED 4-METHOD v9.5 — Native × CCXT Pro × CCXT REST × Direct REST  ║
 * ║  ALL 53 × ALL Pairs (USDT/USDC/USD) × Ticker + Trade + OB Streaming    ║
 * ║  + Health Metrics + OB ID Correlation + Error Classification            ║
 * ║  + Fix Recommendations + Connection Stability Hardening                 ║
 * ║  Storage: DuckDB (trades, orderbook, tickers) — optional, auto-detected ║
 * ║  Usage: node compare-v7-enhanced.js [minutes]  (default=15)             ║
 * ║  Dashboard: http://localhost:3456                                        ║
 * ╚═══════════════════════════════════════════════════════════════════════════╝
 */
// Suppress Node.js v24+ TimeoutNegativeWarning from ccxt rate-limit retries
process.removeAllListeners('warning');
process.on('warning', w => { if (w.name === 'TimeoutNegativeWarning') return; console.warn(`[Warning] ${w.name}: ${w.message}`); });

const ccxt = require('ccxt');
const WebSocket = require('ws');
const https = require('https');
const http = require('http');
const zlib = require('zlib');
const fs = require('fs');
const path = require('path');
const dns = require('dns');
dns.setServers(['1.1.1.1', '8.8.8.8']);
dns.setDefaultResultOrder('verbatim');

// ═══════════════════ DUCKDB STORAGE LAYER ═══════════════════
let duckdb, duckDB, duckConn, duckEnabled = false;
try {
    duckdb = require('duckdb');
    const dbPath = path.join(__dirname, 'crypto_stream_data.duckdb');
    duckDB = new duckdb.Database(dbPath);
    duckConn = duckDB.connect();
    duckConn.run(`CREATE TABLE IF NOT EXISTS trades (
        ts BIGINT, exchange VARCHAR, symbol VARCHAR, source VARCHAR,
        price DOUBLE, amount DOUBLE, side VARCHAR, trade_id VARCHAR
    )`);
    duckConn.run(`CREATE TABLE IF NOT EXISTS orderbook (
        ts BIGINT, exchange VARCHAR, symbol VARCHAR, source VARCHAR,
        best_bid DOUBLE, best_ask DOUBLE, bid_depth INTEGER, ask_depth INTEGER, spread DOUBLE
    )`);
    duckConn.run(`CREATE TABLE IF NOT EXISTS tickers (
        ts BIGINT, exchange VARCHAR, symbol VARCHAR, source VARCHAR,
        last_price DOUBLE, bid DOUBLE, ask DOUBLE, high_24h DOUBLE, low_24h DOUBLE,
        base_volume DOUBLE, quote_volume DOUBLE, change_pct DOUBLE
    )`);
    duckEnabled = true;
    console.log(`  💾 DuckDB storage enabled: ${dbPath}`);
} catch(e) {
    console.log(`  ⚠ DuckDB not available (${e.message?.slice(0,40)}), storage disabled`);
}

const duckBuffers = { trades: [], orderbook: [], tickers: [] };
function escSQL(v) {
    if (v === null || v === undefined) return 'NULL';
    if (typeof v === 'number') return isFinite(v) ? String(v) : 'NULL';
    return "'" + String(v).replace(/'/g, "''").slice(0, 200) + "'";
}
function flushDuckDB() {
    if (!duckEnabled || !duckConn) return;
    try {
        // Flush trades
        const trades = duckBuffers.trades.splice(0, 50000);
        if (trades.length > 0) {
            const vals = trades.map(r =>
                `(${r[0]},${escSQL(r[1])},${escSQL(r[2])},${escSQL(r[3])},${r[4]||0},${r[5]||0},${escSQL(r[6])},${escSQL(r[7])})`
            ).join(',');
            duckConn.run(`INSERT INTO trades VALUES ${vals}`);
        }
        // Flush orderbook
        const obs = duckBuffers.orderbook.splice(0, 50000);
        if (obs.length > 0) {
            const vals = obs.map(r =>
                `(${r[0]},${escSQL(r[1])},${escSQL(r[2])},${escSQL(r[3])},${r[4]||0},${r[5]||0},${r[6]||0},${r[7]||0},${r[8]||0})`
            ).join(',');
            duckConn.run(`INSERT INTO orderbook VALUES ${vals}`);
        }
        // Flush tickers
        const tks = duckBuffers.tickers.splice(0, 50000);
        if (tks.length > 0) {
            const vals = tks.map(r =>
                `(${r[0]},${escSQL(r[1])},${escSQL(r[2])},${escSQL(r[3])},${r[4]||0},${r[5]||0},${r[6]||0},${r[7]||0},${r[8]||0},${r[9]||0},${r[10]||0},${r[11]||0})`
            ).join(',');
            duckConn.run(`INSERT INTO tickers VALUES ${vals}`);
        }
    } catch(e) { /* silently handle flush errors */ }
}

const MINUTES = parseInt(process.argv[2]) || 15;
const TEST_DURATION = MINUTES * 60 * 1000;
const HTTP_PORT = 3456;
const REPORT_FILE = 'CCXT-VS-NATIVE-REPORT.md';
let startTime = Date.now();
let stopFlag = false;
const stats = {};
const sseClients = [];

// ═══════════════════ DATA ENRICHMENT LAYER ═══════════════════
const tradeIdCache = {};
const obSeqCache = {};
const lastMsgTimes = {};
const enrichStats = { deduped: 0, validated: 0, staleOB: 0, restFallbacks: 0, normalized: 0 };

// ═══ HYBRID COMBINED STREAM (cross-method dedup) ═══
const hybridTradeDedup = {};  // `name:pair` → Set<tradeId>
const hybridStats = {};       // name → { trades, orderbook, tickers, deduped, pairs:{} }

// ═══ TRADE ID CORRELATION TRACKING ═══
const tradeIdCorrelation = {};  // name -> { native: Map<id,count>, ccxtPro: Map<id,count>, ccxtRest: Map<id,count>, matches: {np,nr,pr}, checked: 0 }
function initCorrelation(name) {
    tradeIdCorrelation[name] = {
        native: new Map(), ccxtPro: new Map(), ccxtRest: new Map(),
        matches: { nativePro: 0, nativeRest: 0, proRest: 0 }, checked: 0, total: { n: 0, p: 0, r: 0 }
    };
}
function trackTradeId(name, source, tradeId, pair) {
    if (!tradeId || !tradeIdCorrelation[name]) return;
    const c = tradeIdCorrelation[name];
    const key = `${pair}:${tradeId}`;
    c[source].set(key, (c[source].get(key) || 0) + 1);
    c.total[source === 'native' ? 'n' : source === 'ccxtPro' ? 'p' : 'r']++;
    // Check cross-method matches
    if (source === 'native') {
        if (c.ccxtPro.has(key)) c.matches.nativePro++;
        if (c.ccxtRest.has(key)) c.matches.nativeRest++;
    } else if (source === 'ccxtPro') {
        if (c.native.has(key)) c.matches.nativePro++;
        if (c.ccxtRest.has(key)) c.matches.proRest++;
    } else {
        if (c.native.has(key)) c.matches.nativeRest++;
        if (c.ccxtPro.has(key)) c.matches.proRest++;
    }
    c.checked++;
    // Limit memory: keep last 5000 per source
    if (c[source].size > 8000) {
        const entries = [...c[source].entries()];
        c[source] = new Map(entries.slice(-4000));
    }
}

// ═══ ORDERBOOK ID CORRELATION TRACKING ═══
const obIdCorrelation = {};  // name -> { native: Map<seqId,ts>, ccxtPro: Map<seqId,ts>, ccxtRest: Map<seqId,ts>, directRest: Map<seqId,ts>, matches:{np,nr,pr,nd}, checked:0 }
function initOBCorrelation(name) {
    obIdCorrelation[name] = {
        native: new Map(), ccxtPro: new Map(), ccxtRest: new Map(), directRest: new Map(),
        matches: { nativePro: 0, nativeRest: 0, proRest: 0, nativeDirect: 0 }, checked: 0,
        total: { n: 0, p: 0, r: 0, d: 0 }, lastSeq: { n: 0, p: 0, r: 0, d: 0 }
    };
}
function trackOBId(name, source, seqId, pair) {
    if (!seqId || !obIdCorrelation[name]) return;
    const c = obIdCorrelation[name];
    const key = `${pair}:${seqId}`;
    const srcKey = source === 'native' ? 'native' : source === 'ccxtPro' ? 'ccxtPro' : source === 'ccxtRest' ? 'ccxtRest' : 'directRest';
    const totalKey = source === 'native' ? 'n' : source === 'ccxtPro' ? 'p' : source === 'ccxtRest' ? 'r' : 'd';
    c[srcKey].set(key, Date.now());
    c.total[totalKey]++;
    c.lastSeq[totalKey] = seqId;
    // Cross-method matching
    if (srcKey === 'native') { if (c.ccxtPro.has(key)) c.matches.nativePro++; if (c.ccxtRest.has(key)) c.matches.nativeRest++; if (c.directRest.has(key)) c.matches.nativeDirect++; }
    else if (srcKey === 'ccxtPro') { if (c.native.has(key)) c.matches.nativePro++; if (c.ccxtRest.has(key)) c.matches.proRest++; }
    else if (srcKey === 'ccxtRest') { if (c.native.has(key)) c.matches.nativeRest++; if (c.ccxtPro.has(key)) c.matches.proRest++; }
    else { if (c.native.has(key)) c.matches.nativeDirect++; }
    c.checked++;
    if (c[srcKey].size > 5000) { const entries = [...c[srcKey].entries()]; c[srcKey] = new Map(entries.slice(-2500)); }
}

// ═══ HEALTH METRICS + ERROR CLASSIFICATION ═══
const healthMetrics = {};  // name -> { errors: {category: count}, events: [{ts,type,method,msg}], uptime:{}, connectionHistory:[], lastError:{} }
const ERROR_CATEGORIES = {
    CONNECTION_CLOSED: 'connectionClosed', TIMEOUT: 'timeout', NOT_SUPPORTED: 'notSupported',
    TYPE_ERROR: 'typeError', PAIR_NOT_FOUND: 'pairNotFound', RATE_LIMIT: 'rateLimit',
    AUTH_ERROR: 'authError', NETWORK_ERROR: 'networkError', PARSE_ERROR: 'parseError', UNKNOWN: 'unknown'
};
function initHealth(name) {
    healthMetrics[name] = {
        errors: {}, events: [], uptime: { native: 0, ccxtPro: 0, ccxtRest: 0, directRest: 0 },
        connectionHistory: [], lastError: {}, errorRate: { native: 0, ccxtPro: 0, ccxtRest: 0, directRest: 0 },
        connectedSince: { native: 0, ccxtPro: 0, ccxtRest: 0, directRest: 0 },
        totalErrors: 0, healthScore: 100
    };
}
function classifyError(msg) {
    const m = (msg || '').toLowerCase();
    if (m.includes('connection') && (m.includes('close') || m.includes('reset') || m.includes('refused'))) return ERROR_CATEGORIES.CONNECTION_CLOSED;
    if (m.includes('timeout') || m.includes('timed out') || m.includes('etimedout')) return ERROR_CATEGORIES.TIMEOUT;
    if (m.includes('notsupported') || m.includes('not supported') || m.includes('not a function')) return ERROR_CATEGORIES.NOT_SUPPORTED;
    if (m.includes('typeerror') || m.includes('bigint') || m.includes('cannot read') || m.includes('cannot convert')) return ERROR_CATEGORIES.TYPE_ERROR;
    if (m.includes('not found') || m.includes('invalid symbol') || m.includes('invalid pair')) return ERROR_CATEGORIES.PAIR_NOT_FOUND;
    if (m.includes('ratelimit') || m.includes('429') || m.includes('too many')) return ERROR_CATEGORIES.RATE_LIMIT;
    if (m.includes('auth') || m.includes('permission') || m.includes('403')) return ERROR_CATEGORIES.AUTH_ERROR;
    if (m.includes('enotfound') || m.includes('econnrefused') || m.includes('network') || m.includes('fetch')) return ERROR_CATEGORIES.NETWORK_ERROR;
    if (m.includes('json') || m.includes('parse') || m.includes('unexpected token')) return ERROR_CATEGORIES.PARSE_ERROR;
    return ERROR_CATEGORIES.UNKNOWN;
}
function addHealthEvent(name, method, type, msg) {
    if (!healthMetrics[name]) return;
    const h = healthMetrics[name];
    const category = classifyError(msg);
    h.errors[category] = (h.errors[category] || 0) + 1;
    h.totalErrors++;
    h.errorRate[method] = (h.errorRate[method] || 0) + 1;
    h.lastError[method] = { ts: Date.now(), msg: String(msg).slice(0, 100), category };
    h.events.push({ ts: Date.now(), type, method, msg: String(msg).slice(0, 80), category });
    if (h.events.length > 50) h.events.shift();
    h.connectionHistory.push({ ts: Date.now(), method, type, category });
    if (h.connectionHistory.length > 100) h.connectionHistory.shift();
}
function calcHealthScore(name) {
    if (!healthMetrics[name]) return 100;
    const h = healthMetrics[name];
    const elapsed = Math.max(1, (Date.now() - startTime) / 1000);
    let score = 100;
    // Penalize for errors (v9.1: tuned weights — CCXT Pro timeouts are expected, not critical)
    score -= Math.min(20, h.totalErrors * 0.3);
    // Penalize for specific error types
    score -= Math.min(15, (h.errors[ERROR_CATEGORIES.CONNECTION_CLOSED] || 0) * 2);
    score -= Math.min(8, (h.errors[ERROR_CATEGORIES.TIMEOUT] || 0) * 1);
    score -= Math.min(10, (h.errors[ERROR_CATEGORIES.TYPE_ERROR] || 0) * 5);
    // Bonus for active connections
    const s = stats[name];
    if (s) {
        if (s.native.trades + s.native.orderbook > 0) score += 5;
        if (s.ccxtPro.trades + s.ccxtPro.orderbook > 0) score += 5;
        if (s.ccxtRest.trades + s.ccxtRest.orderbook > 0) score += 3;
    }
    return Math.max(0, Math.min(100, Math.round(score)));
}
function getFixRecommendation(name) {
    if (!healthMetrics[name]) return [];
    const h = healthMetrics[name];
    const fixes = [];
    if ((h.errors[ERROR_CATEGORIES.CONNECTION_CLOSED] || 0) > 3) fixes.push({ priority: 'high', fix: 'Add multi-endpoint failover URLs', detail: 'WS connections being closed frequently — add backup WS endpoints' });
    if ((h.errors[ERROR_CATEGORIES.TIMEOUT] || 0) > 2) fixes.push({ priority: 'high', fix: 'Reduce subscription count per connection', detail: 'Timeouts suggest overloaded connections — split pairs across connections' });
    if ((h.errors[ERROR_CATEGORIES.TYPE_ERROR] || 0) > 0) fixes.push({ priority: 'medium', fix: 'Add response sanitization', detail: 'Type errors in parsing — add try/catch around data handlers' });
    if ((h.errors[ERROR_CATEGORIES.NOT_SUPPORTED] || 0) > 0) fixes.push({ priority: 'low', fix: 'Use REST fallback for unsupported methods', detail: 'Exchange does not support this stream type — fallback to REST polling' });
    if ((h.errors[ERROR_CATEGORIES.PAIR_NOT_FOUND] || 0) > 0) fixes.push({ priority: 'medium', fix: 'Verify pair availability on exchange', detail: 'Pair not listed — check exchange market listings' });
    if ((h.errors[ERROR_CATEGORIES.RATE_LIMIT] || 0) > 2) fixes.push({ priority: 'high', fix: 'Increase polling interval / add request batching', detail: 'Rate limits being hit — reduce request frequency' });
    if ((h.errors[ERROR_CATEGORIES.NETWORK_ERROR] || 0) > 3) fixes.push({ priority: 'high', fix: 'Check network connectivity / DNS resolution', detail: 'Network errors — possible DNS or firewall issues' });
    return fixes;
}

process.on('uncaughtException', e => { if (!e.message?.includes('EPIPE')) console.error('UE:', e.message?.slice(0,80)); });
process.on('unhandledRejection', () => {});

// ═══════════════════ UTILITIES ═══════════════════
function httpsReq(url, opts = {}) {
    return new Promise((resolve, reject) => {
        const u = new URL(url);
        const mod = u.protocol === 'http:' ? require('http') : https;
        const o = { hostname: u.hostname, port: u.port || (u.protocol === 'http:' ? 80 : 443),
            path: u.pathname + u.search, method: opts.method || 'GET', timeout: 20000,
            headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', 'Accept': 'application/json', ...(opts.headers || {}) } };
        const req = mod.request(o, res => {
            let d = ''; res.on('data', c => d += c);
            res.on('end', () => { try { resolve(JSON.parse(d)); } catch { resolve(d); } });
        });
        req.setTimeout(20000, () => req.destroy(new Error('timeout')));
        req.on('error', reject);
        if (opts.body) req.write(JSON.stringify(opts.body));
        req.end();
    });
}
function sleep(ms) { return new Promise(ok => setTimeout(ok, Math.max(0, ms))); }

// ═══════════════════ SUBSCRIPTION MANAGER — Per-Exchange WS Limits ═══════════════════
// Official documentation research: max subs per connection, safe practical limits, connection limits
const EXCHANGE_WS_LIMITS = {
    // ─── GROUP A: Reduce subscriptions per connection ───
    'Binance':      { officialMax: 1024, safeMax: 180, maxConns: 5,  batchSize: 20, batchDelay: 200, pingInt: 20000, staleTimeout: 45000, group: 'A' },  // batchSize 30→20, delay 150→200 (v9.4: reduce connection closures on WIF/USDC, BTC/USDC)
    'Coinbase':     { officialMax: 300,  safeMax: 6,   maxConns: 10, batchSize: 6,  batchDelay: 250, pingInt: 30000, staleTimeout: 45000, group: 'A' },  // batchSize 4→6, delay 300→250 (match safeMax for single-batch subscribe)
    'Kraken':       { officialMax: 500,  safeMax: 200, maxConns: 3,  batchSize: 40, batchDelay: 150, pingInt: 25000, staleTimeout: 60000, group: 'A' },  // batchSize 50→40, delay 200→150 (slightly smaller per batch, faster cadence)
    'KuCoin':       { officialMax: 300,  safeMax: 120, maxConns: 7,  batchSize: 20, batchDelay: 120, pingInt: 18000, staleTimeout: 45000, group: 'A' },  // batchSize 15→20, delay 150→120 (40% of limit, comfortably safe)
    'Bybit':        { officialMax: 200,  safeMax: 100, maxConns: 5,  batchSize: 20, batchDelay: 150, pingInt: 20000, staleTimeout: 45000, group: 'A' },  // batchSize 15→20, delay 200→150 (50% of limit)
    'Bitfinex':     { officialMax: 30,   safeMax: 15,  maxConns: 25, batchSize: 5,  batchDelay: 350, pingInt: 25000, staleTimeout: 60000, group: 'A' },  // batchSize 4→5, delay 400→350 (50% of limit per conn)
    'Gate.io':      { officialMax: 200,  safeMax: 80,  maxConns: 5,  batchSize: 20, batchDelay: 150, pingInt: 15000, staleTimeout: 45000, group: 'A' },  // batchSize 15→20, delay 200→150 (40% of limit)
    'WhiteBIT':     { officialMax: 300,  safeMax: 80,  maxConns: 4,  batchSize: 10, batchDelay: 300, pingInt: 25000, staleTimeout: 60000, group: 'A' },  // batchSize 15→10, staleTimeout 60s (reduce conn load + false reconnects)
    'AscendEX':     { officialMax: 200,  safeMax: 80,  maxConns: 3,  batchSize: 15, batchDelay: 200, pingInt: 15000, staleTimeout: 45000, group: 'A' },
    'Bitstamp':     { officialMax: 300,  safeMax: 100, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 20000, staleTimeout: 45000, group: 'A' },
    'Bitget':       { officialMax: 240,  safeMax: 80,  maxConns: 3,  batchSize: 20, batchDelay: 180, pingInt: 30000, staleTimeout: 45000, group: 'A' },  // batchSize 15→20, delay 200→180 (33% of limit)
    'Bullish':      { officialMax: 200,  safeMax: 80,  maxConns: 3,  batchSize: 15, batchDelay: 200, pingInt: 30000, staleTimeout: 60000, group: 'A' },
    'BloFin':       { officialMax: 200,  safeMax: 60,  maxConns: 5,  batchSize: 8,  batchDelay: 250, pingInt: 25000, staleTimeout: 50000, group: 'A' },  // batchSize 12→8 (reduce conn load, Health 70 warning)
    'MEXC':         { officialMax: 30,   safeMax: 20,  maxConns: 5,  batchSize: 5,  batchDelay: 300, pingInt: 20000, staleTimeout: 45000, group: 'A' },
    'CoinEx':       { officialMax: 200,  safeMax: 100, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 15000, staleTimeout: 45000, group: 'A' },
    'LBank':        { officialMax: 100,  safeMax: 30,  maxConns: 8,  batchSize: 6,  batchDelay: 300, pingInt: 20000, staleTimeout: 60000, group: 'A' },  // staleTimeout 50→60s (altcoins have lower data frequency)
    'BitMart':      { officialMax: 100,  safeMax: 50,  maxConns: 5,  batchSize: 10, batchDelay: 200, pingInt: 10000, staleTimeout: 45000, group: 'A' },
    'Poloniex':     { officialMax: 100,  safeMax: 40,  maxConns: 4,  batchSize: 9,  batchDelay: 250, pingInt: 20000, staleTimeout: 45000, group: 'A' },
    'HitBTC':       { officialMax: 100,  safeMax: 50,  maxConns: 3,  batchSize: 10, batchDelay: 200, pingInt: 20000, staleTimeout: 45000, group: 'A' },
    // ─── GROUP B: Add failover URLs ───
    'OKX':          { officialMax: 480,  safeMax: 180, maxConns: 4,  batchSize: 20, batchDelay: 200, pingInt: 25000, staleTimeout: 45000, group: 'B' },  // batchSize 30→20 (reverted — remote server closes at 30 per conn)
    'HTX':          { officialMax: 200,  safeMax: 60,  maxConns: 5,  batchSize: 10, batchDelay: 250, pingInt: 20000, staleTimeout: 45000, group: 'B' },  // batchSize 15→10 (reduce conn load, CCXT Pro closing)
    'Toobit':       { officialMax: 200,  safeMax: 100, maxConns: 3,  batchSize: 14, batchDelay: 200, pingInt: 15000, staleTimeout: 45000, group: 'B' },  // batchSize 20→14 (reduce conn load, Health 70 warning)
    'XT.com':       { officialMax: 200,  safeMax: 70,  maxConns: 4,  batchSize: 15, batchDelay: 250, pingInt: 15000, staleTimeout: 45000, group: 'B' },
    'Gemini':       { officialMax: 200,  safeMax: 100, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 30000, staleTimeout: 60000, group: 'B' },
    'EXMO':         { officialMax: 200,  safeMax: 100, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 20000, staleTimeout: 45000, group: 'B' },
    // ─── GROUP C: Green / near-perfect — cap at safe limits ───
    'BingX':        { officialMax: 300,  safeMax: 150, maxConns: 3,  batchSize: 20, batchDelay: 100, pingInt: 5000,  staleTimeout: 45000, group: 'C' },
    'Crypto.com':   { officialMax: 300,  safeMax: 150, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 25000, staleTimeout: 45000, group: 'C' },
    'Zoomex':       { officialMax: 200,  safeMax: 100, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 20000, staleTimeout: 45000, group: 'C' },
    'Deepcoin':     { officialMax: 200,  safeMax: 100, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 15000, staleTimeout: 45000, group: 'C' },
    'Darkex':       { officialMax: 200,  safeMax: 100, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 20000, staleTimeout: 45000, group: 'C' },
    'Bitrue':       { officialMax: 200,  safeMax: 100, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 20000, staleTimeout: 45000, group: 'C' },
    'WOO X':        { officialMax: 300,  safeMax: 150, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 9000,  staleTimeout: 45000, group: 'C' },
    'Binance.US':   { officialMax: 1024, safeMax: 200, maxConns: 5,  batchSize: 20, batchDelay: 200, pingInt: 20000, staleTimeout: 45000, group: 'C' },
    'BTSE':         { officialMax: 200,  safeMax: 100, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 30000, staleTimeout: 45000, group: 'C' },
    'Pionex':       { officialMax: 200,  safeMax: 100, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 20000, staleTimeout: 45000, group: 'C' },
    'Biconomy':     { officialMax: 200,  safeMax: 100, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 30000, staleTimeout: 45000, group: 'C' },
    'DigiFinex':    { officialMax: 100,  safeMax: 50,  maxConns: 5,  batchSize: 10, batchDelay: 200, pingInt: 30000, staleTimeout: 60000, group: 'C' },  // staleTimeout 45→60s (high stale reconnect count)
    'Hotcoin':      { officialMax: 200,  safeMax: 100, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 20000, staleTimeout: 45000, group: 'C' },
    'NovaEx':       { officialMax: 300,  safeMax: 150, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 9000,  staleTimeout: 45000, group: 'C' },
    'FameEX':       { officialMax: 200,  safeMax: 100, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 20000, staleTimeout: 45000, group: 'C' },
    'Websea':       { officialMax: 200,  safeMax: 100, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 20000, staleTimeout: 45000, group: 'C' },
    'Coinstore':    { officialMax: 200,  safeMax: 100, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 20000, staleTimeout: 45000, group: 'C' },
    'GroveX':       { officialMax: 200,  safeMax: 100, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 20000, staleTimeout: 45000, group: 'C' },
    'CEEX':         { officialMax: 200,  safeMax: 100, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 20000, staleTimeout: 45000, group: 'C' },
    // ─── Default for REST-only / unlisted exchanges ───
    '_default':     { officialMax: 200,  safeMax: 100, maxConns: 3,  batchSize: 20, batchDelay: 200, pingInt: 20000, staleTimeout: 45000, group: 'C' },
};
function getWSLimits(name) { return EXCHANGE_WS_LIMITS[name] || EXCHANGE_WS_LIMITS['_default']; }

// ═══════════════════ FAILOVER URL MAP — Alternative WebSocket Endpoints ═══════════════════
const FAILOVER_URLS = {
    'okx':       ['wss://ws.okx.com:8443/ws/v5/public', 'wss://wsaws.okx.com:8443/ws/v5/public', 'wss://wspap.okx.com:8443/ws/v5/public'],
    'htx':       ['wss://api.huobi.pro/ws', 'wss://api-aws.huobi.pro/ws', 'wss://api.htx.com/ws'],
    'gateio':    ['wss://api.gateio.ws/ws/v4/', 'wss://fx-ws.gateio.ws/v4/ws/usdt', 'wss://ws.gate.io/v4/'],
    'bybit':     ['wss://stream.bybit.com/v5/public/spot', 'wss://stream.bytick.com/v5/public/spot', 'wss://stream.bybit.kz/v5/public/spot'],
    'binance':   ['wss://stream.binance.com:9443/stream', 'wss://stream.binance.com:443/stream', 'wss://data-stream.binance.vision/stream'],
    'binanceus': ['wss://stream.binance.us:9443/stream', 'wss://stream.binance.us:443/stream'],
    'gemini':    ['wss://api.gemini.com/v2/marketdata', 'wss://api.gemini.com/v1/marketdata/BTCUSD'],
    'xt':        ['wss://stream.xt.com/public', 'wss://stream2.xt.com/public', 'wss://stream3.xt.com/public'],
    'toobit':    ['wss://stream.toobit.com/quote/ws/v1', 'wss://stream.toobit.com/quote/ws/v2'],
    'lbank':     ['wss://www.lbkex.net/ws/V2/', 'wss://www.lbkex.com/ws/V2/', 'wss://api.lbank.info/ws/V2/'],
    'blofin':    ['wss://openapi.blofin.com/ws/public'],
    'exmo':      ['wss://ws-api.exmo.com:443/v1/public'],
    'kucoin':    ['wss://ws-api-spot.kucoin.com/', 'wss://ws-api.kucoin.com/'],
    'poloniex':  ['wss://ws.poloniex.com/ws/public', 'wss://ws2.poloniex.com/ws/public'],
    'bitfinex':  ['wss://api-pub.bitfinex.com/ws/2', 'wss://api.bitfinex.com/ws/2'],
    // ─── NEW failover URLs (verified 2026-02-27) ───
    'whitebit':  ['wss://api.whitebit.com/ws'],  // single endpoint (Cloudflare), REST backup added below
    'fameex':    ['wss://wsapi.fameex.com/v1/ws/stream/public', 'wss://api.fameex.com/v2/ws'],
    'coinex':    ['wss://socket.coinex.com/v2/spot', 'wss://ws.coinex.com/'],
    'bitmart':   ['wss://ws-manager-compress.bitmart.com/api?protocol=1.1'],
    'bitrue':    ['wss://ws.bitrue.com/kline-api/ws'],
    'cryptocom': ['wss://stream.crypto.com/exchange/v1/market', 'wss://stream.crypto.com/v2/market'],
    'coinstore': ['wss://ws.coinstore.com/s/ws'],
    'default':   [],
};

const FAILOVER_KEY_MAP = {
    'Binance':'binance', 'OKX':'okx', 'Bybit':'bybit', 'Gate.io':'gateio', 'HTX':'htx',
    'Gemini':'gemini', 'XT.com':'xt', 'Toobit':'toobit', 'LBank':'lbank', 'BloFin':'blofin',
    'EXMO':'exmo', 'Binance.US':'binanceus', 'KuCoin':'kucoin', 'Poloniex':'poloniex', 'Bitfinex':'bitfinex',
    'WhiteBIT':'whitebit', 'FameEX':'fameex', 'CoinEx':'coinex', 'BitMart':'bitmart', 'Bitrue':'bitrue',
    'Crypto.com':'cryptocom', 'Coinstore':'coinstore',
};
function getFailoverKey(name) { return FAILOVER_KEY_MAP[name] || 'default'; }
function getFailoverUrls(name) { return FAILOVER_URLS[getFailoverKey(name)] || []; }

// Rate-limit failover rotations: max 5 per minute per exchange
function canRotateFailover(name) {
    const mgr = subManagerState[name];
    if (!mgr) return true;
    const now = Date.now();
    if (now - (mgr.lastFailoverTime || 0) > 60000) mgr.failoverRotationsPerMinute = 0;
    if ((mgr.failoverRotationsPerMinute || 0) >= 5) return false;
    mgr.failoverRotationsPerMinute = (mgr.failoverRotationsPerMinute || 0) + 1;
    return true;
}

// Subscription Manager — tracks connection pools, stale connections, batch sending, failover state
const subManagerState = {};

function initSubManager(name) {
    subManagerState[name] = {
        connections: [],
        totalSubs: 0,
        lastActivity: Date.now(),
        staleReconnects: 0,
        batchesSent: 0,
        forcedReconnects: 0,
        connectionPool: 0,
        failoverRotations: 0,
        failoverRotationsPerMinute: 0,
        lastFailoverTime: 0,
    };
}

// Send subscription messages in batches with delays
async function sendBatched(ws, messages, batchSize = 20, batchDelay = 200) {
    if (!messages || !messages.length) return;
    const mgr = Object.values(subManagerState).find(m => m.connections.includes(ws));
    for (let i = 0; i < messages.length; i += batchSize) {
        if (stopFlag || ws.readyState !== 1) break;
        const batch = messages.slice(i, i + batchSize);
        for (const msg of batch) {
            if (ws.readyState !== 1) break;
            try { ws.send(typeof msg === 'string' ? msg : JSON.stringify(msg)); } catch {}
        }
        if (mgr) mgr.batchesSent++;
        if (i + batchSize < messages.length) {
            await sleep(batchDelay);
        }
    }
}

// Get subscription manager stats for reporting
function getSubManagerStats() {
    const stats = {};
    for (const [name, mgr] of Object.entries(subManagerState)) {
        stats[name] = {
            connections: mgr.connectionPool,
            totalSubs: mgr.totalSubs,
            staleReconnects: mgr.staleReconnects,
            forcedReconnects: mgr.forcedReconnects,
            batchesSent: mgr.batchesSent,
            lastActivity: mgr.lastActivity,
            failoverRotations: mgr.failoverRotations,
            limits: getWSLimits(name),
        };
    }
    return stats;
}

// ═══════════════════ CCXT PRE-LOAD SYSTEM ═══════════════════
const preloadedPro = {};   // name -> ccxt.pro exchange instance (markets pre-loaded)
const preloadedRest = {};  // name -> ccxt exchange instance (markets pre-loaded)

function buildCCXTOpts(ccxtId) {
    const opts = { enableRateLimit: true, timeout: 60000, newUpdates: false, defaultType: 'spot' };
    if (ccxtId === 'bitfinex') { opts.options = { ...(opts.options||{}), precisionMode: 1, handleAllTickers: false }; }
    const spotFilterExchanges = ['gateio','mexc','bitmart','binance','bybit','bingx','bitrue','deepcoin','bitget','xt','kucoin','okx'];
    if (spotFilterExchanges.includes(ccxtId)) { opts.options = { ...(opts.options||{}), defaultType: 'spot', fetchMarkets: ['spot'] }; }
    if (ccxtId === 'blofin') { opts.defaultType = 'swap'; delete opts.options?.fetchMarkets; }
    return opts;
}

async function preloadAllMarkets(exchanges) {
    console.log('\n  ═══════════ PRE-LOADING CCXT MARKETS ═══════════');
    let proOk = 0, proFail = 0, restOk = 0, restFail = 0;
    const ccxtExchanges = exchanges.filter(e => e.ccxtId && e.ccxtPairs?.length);
    console.log(`  Loading markets for ${ccxtExchanges.length} CCXT-capable exchanges sequentially...\n`);

    for (const ex of ccxtExchanges) {
        const id = ex.ccxtId;

        // Pre-load Pro instance
        if (ccxt.pro?.[id]) {
            try {
                const ExClass = ccxt.pro[id];
                const opts = buildCCXTOpts(id);
                const inst = new ExClass(opts);
                await inst.loadMarkets();
                preloadedPro[ex.name] = inst;
                const mc = Object.keys(inst.markets).length;
                const pairsOk = ex.ccxtPairs.filter(p => inst.markets[p]).length;
                console.log(`  ✓ Pro  ${ex.name.padEnd(14)} ${mc} markets, ${pairsOk}/${ex.ccxtPairs.length} pairs`);
                proOk++;
            } catch(e) {
                console.log(`  ✗ Pro  ${ex.name.padEnd(14)} ${e.message?.slice(0,60)}`);
                proFail++;
            }
            await sleep(300);
        }

        // Pre-load REST instance
        try {
            const ExClass = ccxt[id];
            if (ExClass) {
                const opts = buildCCXTOpts(id);
                delete opts.newUpdates; // REST doesn't use newUpdates
                const inst = new ExClass(opts);
                await inst.loadMarkets();
                preloadedRest[ex.name] = inst;
                const mc = Object.keys(inst.markets).length;
                const pairsOk = ex.ccxtPairs.filter(p => inst.markets[p]).length;
                console.log(`  ✓ REST ${ex.name.padEnd(14)} ${mc} markets, ${pairsOk}/${ex.ccxtPairs.length} pairs`);
                restOk++;
            }
        } catch(e) {
            console.log(`  ✗ REST ${ex.name.padEnd(14)} ${e.message?.slice(0,60)}`);
            restFail++;
        }
        await sleep(300);
    }

    console.log(`\n  ═══ Pre-load complete: Pro ${proOk}✓/${proFail}✗ | REST ${restOk}✓/${restFail}✗ ═══\n`);
}

function toCanonical(sym) {
    if (!sym) return null;
    const s = String(sym);
    const map = {
        'BTCUSDT':'BTC_USDT','btcusdt':'BTC_USDT','BTC_USDT':'BTC_USDT','BTC-USDT':'BTC_USDT','BTC/USDT':'BTC_USDT',
        'ETHUSDT':'ETH_USDT','ethusdt':'ETH_USDT','ETH_USDT':'ETH_USDT','ETH-USDT':'ETH_USDT','ETH/USDT':'ETH_USDT',
        'SOLUSDT':'SOL_USDT','solusdt':'SOL_USDT','SOL_USDT':'SOL_USDT','SOL-USDT':'SOL_USDT','SOL/USDT':'SOL_USDT',
        'BTCUSDC':'BTC_USDC','btcusdc':'BTC_USDC','BTC_USDC':'BTC_USDC','BTC-USDC':'BTC_USDC','BTC/USDC':'BTC_USDC',
        'ETHUSDC':'ETH_USDC','ethusdc':'ETH_USDC','ETH_USDC':'ETH_USDC','ETH-USDC':'ETH_USDC','ETH/USDC':'ETH_USDC',
        'SOLUSDC':'SOL_USDC','solusdc':'SOL_USDC','SOL_USDC':'SOL_USDC','SOL-USDC':'SOL_USDC','SOL/USDC':'SOL_USDC',
        'BTCUSD':'BTC_USD','btcusd':'BTC_USD','BTC_USD':'BTC_USD','BTC-USD':'BTC_USD','BTC/USD':'BTC_USD',
        'ETHUSD':'ETH_USD','ethusd':'ETH_USD','ETH_USD':'ETH_USD','ETH-USD':'ETH_USD','ETH/USD':'ETH_USD',
        'SOLUSD':'SOL_USD','solusd':'SOL_USD','SOL_USD':'SOL_USD','SOL-USD':'SOL_USD','SOL/USD':'SOL_USD',
        'BTC_DAI':'BTC_DAI','BTC/DAI':'BTC_DAI',
        'tBTCUSD':'BTC_USD','tETHUSD':'ETH_USD','tSOLUSD':'SOL_USD',
        'tBTCUST':'BTC_USDT','tETHUST':'ETH_USDT','tSOLUST':'SOL_USDT',
        'SPOT_BTC_USDT':'BTC_USDT','SPOT_ETH_USDT':'ETH_USDT','SPOT_SOL_USDT':'SOL_USDT',
        'SPOT_BTC_USDC':'BTC_USDC','SPOT_ETH_USDC':'ETH_USDC','SPOT_SOL_USDC':'SOL_USDC',
        'XBT/USDT':'BTC_USDT','XBT/USD':'BTC_USD','XBT/USDC':'BTC_USDC',
        // Swap/futures suffix mappings
        'BTC/USDT:USDT':'BTC_USDT','ETH/USDT:USDT':'ETH_USDT','SOL/USDT:USDT':'SOL_USDT',
        'BTC/USDC:USDC':'BTC_USDC','ETH/USDC:USDC':'ETH_USDC','SOL/USDC:USDC':'SOL_USDC',
        'BTC/USD:USD':'BTC_USD','ETH/USD:USD':'ETH_USD','SOL/USD:USD':'SOL_USD',
    };
    if (map[s]) return map[s];
    const upper = s.toUpperCase();
    if (map[upper]) return map[upper];
    const cleaned = s.replace(/:.*$/, '');
    if (map[cleaned]) return map[cleaned];
    const cleanedUpper = cleaned.toUpperCase();
    if (map[cleanedUpper]) return map[cleanedUpper];
    const withUnderscore = upper.replace(/[\/-]/g, '_');
    if (map[withUnderscore]) return map[withUnderscore];
    // Generic fallback: handle any COIN/QUOTE format for known quotes
    // Supports: COIN/QUOTE, COIN-QUOTE, COIN_QUOTE, COINQUOTE, tCOINQUOTE (Bitfinex), SPOT_COIN_QUOTE (WOO X)
    let work = cleaned.toUpperCase();
    // Strip Bitfinex 't' prefix
    if (work.startsWith('T') && work.length > 5) {
        const noT = work.slice(1);
        // Check if removing 't' gives a valid pair (UST→USDT for Bitfinex)
        if (noT.endsWith('UST')) { work = noT.slice(0, -3) + 'USDT'; }
        else if (/USD[CT]?$/.test(noT)) { work = noT; }
    }
    // Strip WOO X 'SPOT_' prefix
    if (work.startsWith('SPOT_')) work = work.slice(5);
    // Try separator-based split: / - _
    const sepMatch = work.match(/^([A-Z0-9]+)[\/\-_]([A-Z0-9]+)$/);
    if (sepMatch) {
        const [, coin, quote] = sepMatch;
        if (['USDT','USDC','USD','DAI','EUR','BUSD'].includes(quote)) {
            return `${coin}_${quote}`;
        }
    }
    // Try suffix-based detection for concatenated symbols like BRETTUSDT
    const quotes = ['USDT','USDC','USD','DAI','EUR','BUSD'];
    for (const q of quotes) {
        if (work.endsWith(q) && work.length > q.length) {
            const coin = work.slice(0, -q.length);
            if (/^[A-Z0-9]{2,10}$/.test(coin)) return `${coin}_${q}`;
        }
    }
    return null;
}

// ═══════════════════ STATS (3-METHOD) ═══════════════════
function initStats(name, tier, nativeType, ccxtId, hasRestEndpoint) {
    stats[name] = {
        tier, nativeType, ccxtId, hasRestEndpoint: !!hasRestEndpoint,
        native: { trades: 0, orderbook: 0, tickers: 0, connected: false, reconnects: 0, errors: [], pairs: {},
                  deduped: 0, validated: 0, staleOB: 0, restFallbacks: 0, lastMsg: 0 },
        ccxtPro: { trades: 0, orderbook: 0, tickers: 0, connected: false, errors: [], pairs: {},
                   deduped: 0, lastMsg: 0 },
        ccxtRest: { trades: 0, orderbook: 0, tickers: 0, connected: false, errors: [], pairs: {},
                    lastMsg: 0 },
        directRest: { trades: 0, orderbook: 0, tickers: 0, connected: false, errors: [], pairs: {},
                      lastMsg: 0 }
    };
    initCorrelation(name);
    initOBCorrelation(name);
    initHealth(name);
    hybridStats[name] = { trades: 0, orderbook: 0, tickers: 0, deduped: 0, pairs: {} };
}

// ═══ DEAD / SILENT PAIRS BLACKLIST (P4 + expanded) ═══
const DEAD_PAIRS = new Set([
    // NOTE: All entries use underscore format (e.g. BTC_USDT) to match toCanonical() output — v9.4 fix
    // --- Original dead pairs ---
    'Bullish:WIF_USDC',
    'Azbit:BRETT_USDT', 'Azbit:ENA_USDT', 'Azbit:POPCAT_USDT', 'Azbit:SUI_USDT',
    'Coinbase:ENA_USDC', 'Coinbase:PENGU_USDC', 'Coinbase:POPCAT_USDC', 'Coinbase:SUI_USDC', 'Coinbase:WIF_USDC',
    // --- Deepcoin silent pairs ---
    'Deepcoin:BRETT_USDT', 'Deepcoin:POPCAT_USDT', 'Deepcoin:SUI_USDT', 'Deepcoin:ENA_USDT',
    // --- FameEX silent pairs (these 5 coins not listed on FameEX) ---
    'FameEX:BRETT_USDT', 'FameEX:PENGU_USDT', 'FameEX:POPCAT_USDT', 'FameEX:SUI_USDT', 'FameEX:ENA_USDT',
    // --- Darkex silent pairs ---
    'Darkex:SOL_USDT', 'Darkex:BRETT_USDT', 'Darkex:PENGU_USDT', 'Darkex:ENA_USDT',
    // --- CEEX silent pairs ---
    'CEEX:BRETT_USDT', 'CEEX:PENGU_USDT', 'CEEX:POPCAT_USDT', 'CEEX:ENA_USDT',
    // --- Coinstore silent pairs ---
    'Coinstore:BRETT_USDT', 'Coinstore:POPCAT_USDT',
    // --- Websea silent pairs ---
    'Websea:BRETT_USDT', 'Websea:POPCAT_USDT',
    // --- BigONE silent pairs ---
    'BigONE:BRETT_USDT', 'BigONE:POPCAT_USDT',
    // --- LATOKEN silent pairs (verified NOT_LISTED via CCXT loadMarkets) ---
    'LATOKEN:WIF_USDT', 'LATOKEN:SUI_USDT',
    // --- MEXC / Bitget silent pairs ---
    'MEXC:BRETT_USDT', 'Bitget:POPCAT_USDT',
    // --- Poloniex silent pairs (verified NOT_LISTED via API 2026-02-27) ---
    'Poloniex:PENGU_USDT',
    // --- CEX.IO ENA: listed but 0 trades (verified 2026-02-27) ---
    'CEX.IO:ENA_USDT', 'CEX.IO:ENA_USDC', 'CEX.IO:ENA_USD',
    // --- Audit 2026-02-28: confirmed delisted/not found via CCXT loadMarkets ---
    'Toobit:BRETT_USDT',                                   // not in Toobit spot markets (audit scanner)
    'HTX:SOL_USDC',                                        // delisted from HTX (11 pairs found, no solusdc)
    'WOO X:SOL_USDC',                                      // delisted from WOO X (11 pairs found, no sol_usdc)
    'Bitstamp:SOL_USDT',                                   // not on Bitstamp (only SOL/USD + SOL/USDC listed)
    'Deepcoin:BTC_USDC', 'Deepcoin:ETH_USDC', 'Deepcoin:SOL_USDC', // only USDT pairs remain on Deepcoin
    // --- Audit v9.4: BloFin ETH/USDC not in CCXT markets (native ETH-USDC works) ---
    'BloFin:ETH_USDC',                                     // CCXT can't resolve ETH/USDC:USDC on BloFin perp
]);

// ═══ Native data methods (unchanged) ═══
function addN(name, sym, tr, ob, tradeId) {
    const p = toCanonical(sym); if (!p || !stats[name]) return;
    if (DEAD_PAIRS.has(`${name}:${p}`)) return;
    const s = stats[name].native;
    if (tr > 0 && tradeId) {
        const key = `${name}:${p}`;
        if (!tradeIdCache[key]) tradeIdCache[key] = new Set();
        if (tradeIdCache[key].has(tradeId)) { s.deduped++; enrichStats.deduped++; return; }
        tradeIdCache[key].add(tradeId);
        if (tradeIdCache[key].size > 5000) { const a=[...tradeIdCache[key]]; tradeIdCache[key] = new Set(a.slice(-2500)); }
        trackTradeId(name, 'native', String(tradeId), p);
    }
    if (!s.pairs[p]) s.pairs[p] = { trades: 0, ob: 0, tickers: 0 };
    s.pairs[p].trades += (tr || 0); s.pairs[p].ob += (ob || 0);
    s.trades += (tr || 0); s.orderbook += (ob || 0);
    s.lastMsg = Date.now();
    lastMsgTimes[name] = Date.now();
    enrichStats.normalized++;
    // Native = highest priority — update tracker
    hybridSourceTracker[`${name}:${p}`] = { method: 'native', priority: 0, lastTime: Date.now() };
    if (tr > 0) addHybridTrade(name, p, tr, tradeId);
    if (ob > 0) addHybridOB(name, p, ob);
}

function addNWithOBValidation(name, sym, ob, updateId) {
    if (!updateId) { addN(name, sym, 0, ob); return; }
    const key = `${name}:${toCanonical(sym)}`;
    if (obSeqCache[key] && updateId <= obSeqCache[key]) {
        stats[name].native.staleOB++; enrichStats.staleOB++; return;
    }
    obSeqCache[key] = updateId;
    stats[name].native.validated++; enrichStats.validated++;
    addN(name, sym, 0, ob);
}

// ═══ CCXT Pro data (watch* streaming) ═══
function addCPro(name, sym, tr, ob, tradeId) {
    const p = toCanonical(sym); if (!p || !stats[name]) return;
    if (DEAD_PAIRS.has(`${name}:${p}`)) return;
    const s = stats[name].ccxtPro;
    if (!s.pairs[p]) s.pairs[p] = { trades: 0, ob: 0, tickers: 0 };
    s.pairs[p].trades += (tr || 0); s.pairs[p].ob += (ob || 0);
    s.trades += (tr || 0); s.orderbook += (ob || 0);
    s.lastMsg = Date.now();
    if (shouldEmit(name, p, 'ccxtPro')) {
        if (tr > 0) addHybridTrade(name, p, tr, tradeId);
        if (ob > 0) addHybridOB(name, p, ob);
    }
}
function addCProTicker(name, sym) {
    const p = toCanonical(sym); if (!p || !stats[name]) return;
    const s = stats[name].ccxtPro;
    if (!s.pairs[p]) s.pairs[p] = { trades: 0, ob: 0, tickers: 0 };
    s.pairs[p].tickers = (s.pairs[p].tickers || 0) + 1;
    s.tickers = (s.tickers || 0) + 1;
    s.lastMsg = Date.now();
    addHybridTicker(name, p);
}

// ═══ CCXT REST data (fetch* polling) ═══
function addCRest(name, sym, tr, ob, tradeId) {
    const p = toCanonical(sym); if (!p || !stats[name]) return;
    if (DEAD_PAIRS.has(`${name}:${p}`)) return;
    const s = stats[name].ccxtRest;
    if (!s.pairs[p]) s.pairs[p] = { trades: 0, ob: 0, tickers: 0 };
    s.pairs[p].trades += (tr || 0); s.pairs[p].ob += (ob || 0);
    s.trades += (tr || 0); s.orderbook += (ob || 0);
    s.lastMsg = Date.now();
    if (shouldEmit(name, p, 'ccxtRest')) {
        if (ob > 0) addHybridOB(name, p, ob);
    }
}
function addCRestTicker(name, sym) {
    const p = toCanonical(sym); if (!p || !stats[name]) return;
    const s = stats[name].ccxtRest;
    if (!s.pairs[p]) s.pairs[p] = { trades: 0, ob: 0, tickers: 0 };
    s.pairs[p].tickers = (s.pairs[p].tickers || 0) + 1;
    s.tickers = (s.tickers || 0) + 1;
    s.lastMsg = Date.now();
    addHybridTicker(name, p);
}

function addErr(name, source, msg) {
    const list = stats[name]?.[source]?.errors;
    if (!list) return;
    const m = String(msg || 'unknown').slice(0, 80);
    if (!list.includes(m)) { list.push(m); if (list.length > 15) list.shift(); }
    // Track in health metrics
    addHealthEvent(name, source, 'error', m);
}

// ═══ DIRECT REST (4th stream — raw HTTP polling) ═══
function addDirectRest(name, sym, tr, ob, tradeId) {
    const p = toCanonical(sym); if (!p || !stats[name]) return;
    if (DEAD_PAIRS.has(`${name}:${p}`)) return;
    const s = stats[name].directRest;
    if (!s.pairs[p]) s.pairs[p] = { trades: 0, ob: 0, tickers: 0 };
    s.pairs[p].trades += (tr || 0); s.pairs[p].ob += (ob || 0);
    s.trades += (tr || 0); s.orderbook += (ob || 0);
    s.lastMsg = Date.now();
    if (tradeId) trackTradeId(name, 'directRest', String(tradeId), p);
    if (shouldEmit(name, p, 'directRest')) {
        if (tr > 0) addHybridTrade(name, p, tr, tradeId);
        if (ob > 0) addHybridOB(name, p, ob);
    }
}
function addDirectRestTicker(name, sym) {
    const p = toCanonical(sym); if (!p || !stats[name]) return;
    const s = stats[name].directRest;
    if (!s.pairs[p]) s.pairs[p] = { trades: 0, ob: 0, tickers: 0 };
    s.pairs[p].tickers = (s.pairs[p].tickers || 0) + 1;
    s.tickers = (s.tickers || 0) + 1;
    s.lastMsg = Date.now();
    addHybridTicker(name, p);
}
function addDirectRestErr(name, msg) { addErr(name, 'directRest', msg); }

// ═══ HYBRID EMIT PRIORITY ═══
// Priority: Native(0) > CCXT Pro(1) > CCXT REST(2) > Direct REST(3)
const METHOD_PRIORITY = { native: 0, ccxtPro: 1, ccxtRest: 2, directRest: 3 };
const hybridSourceTracker = {}; // "name:pair" -> { method, priority, lastTime }

function shouldEmit(name, pair, method) {
    const key = `${name}:${pair}`;
    const myPriority = METHOD_PRIORITY[method] ?? 3;
    if (!hybridSourceTracker[key]) {
        hybridSourceTracker[key] = { method, priority: myPriority, lastTime: Date.now() };
        return true;
    }
    const tracker = hybridSourceTracker[key];
    if (method === tracker.method) { tracker.lastTime = Date.now(); return true; }
    if (myPriority < tracker.priority) {
        tracker.method = method; tracker.priority = myPriority; tracker.lastTime = Date.now();
        return true;
    }
    // Allow lower priority if higher priority went stale (>30s)
    if (Date.now() - tracker.lastTime > 30000) {
        tracker.method = method; tracker.priority = myPriority; tracker.lastTime = Date.now();
        return true;
    }
    return false;
}

// ═══ HYBRID DEDUP FUNCTIONS ═══
function addHybridTrade(name, pair, count, tradeId) {
    if (!hybridStats[name]) return;
    if (!hybridStats[name].pairs[pair]) hybridStats[name].pairs[pair] = { trades: 0, ob: 0, tickers: 0 };
    if (tradeId) {
        const key = `${name}:${pair}`;
        if (!hybridTradeDedup[key]) hybridTradeDedup[key] = new Set();
        if (hybridTradeDedup[key].has(tradeId)) { hybridStats[name].deduped++; return; }
        hybridTradeDedup[key].add(tradeId);
        if (hybridTradeDedup[key].size > 10000) { const a=[...hybridTradeDedup[key]]; hybridTradeDedup[key] = new Set(a.slice(-8000)); }
    }
    hybridStats[name].trades += (count || 1);
    hybridStats[name].pairs[pair].trades += (count || 1);
}
function addHybridOB(name, pair, count) {
    if (!hybridStats[name]) return;
    if (!hybridStats[name].pairs[pair]) hybridStats[name].pairs[pair] = { trades: 0, ob: 0, tickers: 0 };
    hybridStats[name].orderbook += (count || 1);
    hybridStats[name].pairs[pair].ob += (count || 1);
}
function addHybridTicker(name, pair) {
    if (!hybridStats[name]) return;
    if (!hybridStats[name].pairs[pair]) hybridStats[name].pairs[pair] = { trades: 0, ob: 0, tickers: 0 };
    hybridStats[name].tickers++;
    hybridStats[name].pairs[pair].tickers++;
}

// ═══════════════════ SSE ═══════════════════
function emitEvent(type, exchange, source, message) {
    const evt = { type, exchange, source, message, time: new Date().toLocaleTimeString('en-US', { hour12: false }) };
    const data = `data: ${JSON.stringify(evt)}\n\n`;
    for (const c of sseClients) { try { c.write(data); } catch {} }
}

// ═══════════════════ ENHANCED NATIVE WS ENGINE v9 — Subscription Manager ═══════════════════
function connectWS(cfg) {
    const name = cfg.name;
    const urls = cfg.urls || [cfg.url];
    let urlIdx = 0;
    let attempt = 0;
    const limits = getWSLimits(name);

    // Merge failover URLs into the URL pool (deduped)
    const failoverUrls = getFailoverUrls(name);
    for (const fu of failoverUrls) {
        if (!urls.includes(fu)) urls.push(fu);
    }

    // Initialize subscription manager state for this exchange
    if (!subManagerState[name]) initSubManager(name);
    const mgr = subManagerState[name];

    const wsOpts = {
        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', ...(cfg.wsHeaders || {}) },
        handshakeTimeout: 60000,
        perMessageDeflate: cfg.compression ? false : undefined,
        maxPayload: 50 * 1024 * 1024,
        agent: new https.Agent({ keepAlive: true, keepAliveMsecs: 30000 }),
    };

    function decompress(data) {
        if (Buffer.isBuffer(data)) {
            if (cfg.compression === 'gzip') {
                try { return zlib.gunzipSync(data).toString(); } catch {
                    try { return zlib.inflateSync(data).toString(); } catch {
                        try { return zlib.inflateRawSync(data).toString(); } catch { return data.toString(); }
                    }
                }
            }
            if (cfg.compression === 'inflate') {
                try { return zlib.inflateSync(data).toString(); } catch {
                    try { return zlib.inflateRawSync(data).toString(); } catch {
                        try { return zlib.gunzipSync(data).toString(); } catch { return data.toString(); }
                    }
                }
            }
            if (cfg.compression === 'deflate') {
                try { return zlib.inflateRawSync(data).toString(); } catch {
                    try { return zlib.inflateSync(data).toString(); } catch { return data.toString(); }
                }
            }
            return data.toString();
        }
        return String(data);
    }

    function getBackoff() {
        const base = Math.min(1000 * Math.pow(2, Math.min(attempt, 6)), 30000);
        const jitter = Math.random() * base * 0.3;
        return Math.floor(base + jitter);
    }

    // Stale timeout: use per-exchange limit config (default 45s) instead of hardcoded 10s
    const staleTimeout = cfg.staleTimeout || limits.staleTimeout || 45000;
    // Ping interval from limits if not explicitly set in cfg
    const effectivePingInt = cfg.pingInt || limits.pingInt || 20000;

    function doConnect() {
        if (stopFlag) return;
        const currentUrl = urls[urlIdx % urls.length];
        let ws;
        try { ws = new WebSocket(currentUrl, wsOpts); } catch (e) {
            addErr(name, 'native', `conn:${e.message?.slice(0,40)}`);
            addHealthEvent(name, 'native', 'CONNECTION_CLOSED', `WS create failed: ${e.message?.slice(0,40)}`);
            attempt++;
            if (attempt > 2) urlIdx++;
            if (!stopFlag) setTimeout(doConnect, getBackoff());
            return;
        }
        let lastMsgAt = Date.now();
        let restFallbackTimer = null;
        let staleMonitorTimer = null;

        // Track in connection pool
        mgr.connections.push(ws);
        mgr.connectionPool = mgr.connections.filter(w => w.readyState <= 1).length;

        ws.on('open', () => {
            attempt = 0;
            mgr.failoverRotationsPerMinute = 0; // Reset rate limit on successful connect
            stats[name].native.connected = true;
            lastMsgAt = Date.now();
            lastMsgTimes[name] = Date.now();
            mgr.lastActivity = Date.now();
            addHealthEvent(name, 'native', 'connected', `WS connected (endpoint ${(urlIdx%urls.length)+1}/${urls.length})`);
            emitEvent('connect', name, 'native', `WS connected (endpoint ${(urlIdx%urls.length)+1}/${urls.length})`);

            // ─── BATCHED SUBSCRIPTION SENDING ───
            // If cfg.subscriptions is provided, send in batches with delays
            // Otherwise fall back to cfg.onOpen (legacy mode with single onOpen call)
            if (cfg.subscriptions && cfg.subscriptions.length) {
                mgr.totalSubs = cfg.subscriptions.length;
                sendBatched(ws, cfg.subscriptions, limits.batchSize, limits.batchDelay)
                    .catch(e => addErr(name, 'native', `batchSub:${e.message?.slice(0,40)}`));
            } else {
                try { cfg.onOpen(ws); } catch (e) { addErr(name, 'native', 'onOpen:'+e.message?.slice(0,40)); }
            }

            // ─── PING/PONG with per-exchange interval ───
            if (cfg.pingMsg) ws._pt = setInterval(() => {
                if (ws.readyState === 1) ws.send(typeof cfg.pingMsg === 'string' ? cfg.pingMsg : JSON.stringify(cfg.pingMsg));
            }, effectivePingInt);
            if (cfg.customPing) ws._ct = setInterval(() => {
                if (ws.readyState === 1) try { cfg.customPing(ws); } catch {}
            }, cfg.customPingInt || limits.pingInt || 30000);
            ws.on('ping', () => { try { ws.pong(); } catch {} });

            // ─── REST FALLBACK (silent >10s → poll REST) ───
            if (cfg.restFallbackUrls) {
                restFallbackTimer = setInterval(async () => {
                    if (Date.now() - lastMsgAt > 10000 && !stopFlag) {
                        for (const rf of cfg.restFallbackUrls) {
                            try { const r = await httpsReq(rf.url).catch(()=>null); if(r) { rf.parse(r); stats[name].native.restFallbacks++; enrichStats.restFallbacks++; } } catch {}
                        }
                        emitEvent('fallback', name, 'native', 'REST snapshot (WS silent >10s)');
                    }
                }, 12000);
            }

            // ─── STALE CONNECTION MONITOR (45-60s → force reconnect) ───
            staleMonitorTimer = setInterval(() => {
                if (stopFlag) return;
                const silentMs = Date.now() - lastMsgAt;
                if (silentMs > staleTimeout && ws.readyState === 1) {
                    mgr.staleReconnects++;
                    mgr.forcedReconnects++;
                    // Rotate to failover URL on stale connection
                    if (canRotateFailover(name)) {
                        urlIdx++;
                        mgr.failoverRotations++;
                        mgr.lastFailoverTime = Date.now();
                    }
                    addHealthEvent(name, 'native', 'TIMEOUT', `Stale force-reconnect \u2192 endpoint ${(urlIdx%urls.length)+1}/${urls.length} after ${Math.round(silentMs/1000)}s`);
                    emitEvent('stale-reconnect', name, 'native', `Force reconnect (${Math.round(silentMs/1000)}s stale \u2192 endpoint ${(urlIdx%urls.length)+1}/${urls.length})`);
                    try { ws.terminate(); } catch {}
                }
            }, Math.max(staleTimeout / 2, 15000));  // Check at half the stale timeout, minimum every 15s

            if (cfg.restPoll) cfg.restPoll(name);
        });
        ws.on('message', data => {
            lastMsgAt = Date.now();
            lastMsgTimes[name] = Date.now();
            mgr.lastActivity = Date.now();
            try { cfg.onMsg(decompress(data), ws); } catch {}
        });
        ws.on('error', e => {
            addErr(name, 'native', e.message?.slice(0,60));
            addHealthEvent(name, 'native', 'CONNECTION_CLOSED', `WS error: ${e.message?.slice(0,60)}`);
            // Failover: rotate URL on timeout/reset/refused errors
            const errMsg = (e.message || '').toLowerCase();
            if (errMsg.includes('timeout') || errMsg.includes('econnreset') || errMsg.includes('econnrefused') || errMsg.includes('ehostunreach')) {
                if (canRotateFailover(name)) {
                    urlIdx++;
                    mgr.failoverRotations++;
                    mgr.lastFailoverTime = Date.now();
                    emitEvent('failover', name, 'native', `URL rotation on error \u2192 endpoint ${(urlIdx%urls.length)+1}/${urls.length}`);
                }
            }
        });
        ws.on('close', () => {
            if (ws._pt) clearInterval(ws._pt);
            if (ws._ct) clearInterval(ws._ct);
            if (restFallbackTimer) clearInterval(restFallbackTimer);
            if (staleMonitorTimer) clearInterval(staleMonitorTimer);
            stats[name].native.connected = false;

            // Remove from connection pool
            const idx = mgr.connections.indexOf(ws);
            if (idx > -1) mgr.connections.splice(idx, 1);
            mgr.connectionPool = mgr.connections.filter(w => w.readyState <= 1).length;

            if (!stopFlag) {
                stats[name].native.reconnects = (stats[name].native.reconnects || 0) + 1;
                attempt++;
                // Aggressive failover: rotate after 2 consecutive failures (was 3)
                if (attempt > 2 && canRotateFailover(name)) {
                    urlIdx++;
                    mgr.failoverRotations++;
                    mgr.lastFailoverTime = Date.now();
                    emitEvent('failover', name, 'native', `URL rotation on close \u2192 endpoint ${(urlIdx%urls.length)+1}/${urls.length}`);
                }
                addHealthEvent(name, 'native', 'CONNECTION_CLOSED', `Reconnecting attempt ${attempt}, endpoint ${(urlIdx%urls.length)+1}`);
                emitEvent('reconnect', name, 'native', `Reconnecting (attempt ${attempt}, endpoint ${(urlIdx%urls.length)+1})`);
                setTimeout(doConnect, getBackoff());
            }
        });
    }
    doConnect();

    // ─── EXTRA CONNECTIONS (secondary WS channels) ───
    if (cfg.extra) {
        for (const ec of cfg.extra) {
            let eAttempt = 0;
            (function startExtra() {
                if (stopFlag) return;
                const eUrl = ec.url || urls[urlIdx % urls.length];
                let ws;
                try { ws = new WebSocket(eUrl, wsOpts); } catch { eAttempt++; if (!stopFlag) setTimeout(startExtra, 1000*Math.min(Math.pow(2,eAttempt),30)+Math.random()*1000); return; }
                let eLast = Date.now();

                // Track extra connections in pool
                mgr.connections.push(ws);
                mgr.connectionPool++;

                ws.on('open', () => {
                    eAttempt = 0;
                    eLast = Date.now();
                    // Batched send for extra connections too
                    if (ec.subscriptions && ec.subscriptions.length) {
                        sendBatched(ws, ec.subscriptions, limits.batchSize, limits.batchDelay).catch(()=>{});
                    } else {
                        try { ec.onOpen(ws); } catch {}
                    }
                    if (cfg.pingMsg) ws._pt = setInterval(() => { if (ws.readyState===1) ws.send(typeof cfg.pingMsg==='string'?cfg.pingMsg:JSON.stringify(cfg.pingMsg)); }, effectivePingInt);
                    if (cfg.customPing) ws._ct = setInterval(() => { if (ws.readyState===1) try{cfg.customPing(ws);}catch{} }, cfg.customPingInt||limits.pingInt||30000);
                    ws.on('ping', () => { try { ws.pong(); } catch {} });

                    // Stale monitor for extra connections too
                    ws._stale = setInterval(() => {
                        if (!stopFlag && Date.now() - eLast > staleTimeout && ws.readyState === 1) {
                            mgr.staleReconnects++;
                            emitEvent('stale-reconnect', name, 'native-extra', `Force reconnect extra conn (${Math.round((Date.now()-eLast)/1000)}s stale)`);
                            try { ws.terminate(); } catch {}
                        }
                    }, Math.max(staleTimeout / 2, 15000));
                });
                ws.on('message', data => {
                    eLast = Date.now();
                    mgr.lastActivity = Date.now();
                    try { cfg.onMsg(decompress(data), ws); } catch {}
                });
                ws.on('error', () => {});
                ws.on('close', () => {
                    if(ws._pt)clearInterval(ws._pt);
                    if(ws._ct)clearInterval(ws._ct);
                    if(ws._stale)clearInterval(ws._stale);
                    const idx = mgr.connections.indexOf(ws);
                    if (idx > -1) mgr.connections.splice(idx, 1);
                    mgr.connectionPool = mgr.connections.filter(w => w.readyState <= 1).length;
                    eAttempt++;
                    if(!stopFlag) setTimeout(startExtra, 1000*Math.min(Math.pow(2,eAttempt),30)+Math.random()*1000);
                });
            })();
        }
    }
}

// ═══════════════════ REST ENGINE ═══════════════════
function runREST(name, fetches, interval = 8000) {
    stats[name].native.connected = true;
    emitEvent('connect', name, 'native', 'REST polling started');
    async function poll() {
        if (stopFlag) return;
        for (const f of fetches) { try { const r = await httpsReq(f.url).catch(()=>null); if(r) f.parse(r); } catch {} }
        if (!stopFlag) setTimeout(poll, interval);
    }
    poll();
}

// ═══════════════════ CCXT PRO ENGINE (watch* streaming + watchTicker) — HARDENED ═══════════════════
async function startCCXTPro(name, ccxtId, pairs) {
    try {
        // Use pre-loaded instance if available, otherwise create fresh
        let ex = preloadedPro[name];
        if (!ex) {
            const ExClass = ccxt.pro?.[ccxtId];
            if (!ExClass) { addErr(name,'ccxtPro','No CCXT Pro class for '+ccxtId); return; }
            const opts = buildCCXTOpts(ccxtId);
            ex = new ExClass(opts);
            let mkLoaded = false;
            for (let mkAttempt = 0; mkAttempt < 3 && !mkLoaded; mkAttempt++) {
                try { await ex.loadMarkets(); mkLoaded = true; } catch(e) {
                    addErr(name,'ccxtPro',`loadMarkets(${mkAttempt+1}):${e.message?.slice(0,50)}`);
                    if (mkAttempt < 2) await sleep(5000 * (mkAttempt + 1));
                }
            }
            if (!mkLoaded) return;
        }
        stats[name].ccxtPro.connected = true;
        emitEvent('connect', name, 'ccxtPro', `Pro connected (${pairs.length} pairs)`);
        const validPairs = pairs.filter(p => { if(ex.markets[p]) return true; addErr(name,'ccxtPro',`${p}:not found`); return false; });
        if (!validPairs.length) return;

        const workers = validPairs.flatMap(pair => [
            // watchTrades worker — HARDENED
            (async () => { let f=0; while(!stopFlag && f<50) { try { const t=await ex.watchTrades(pair);
                for (const tr of t) {
                    addCPro(name, pair, 1, 0, String(tr.id||''));
                    if (tr.id) trackTradeId(name, 'ccxtPro', String(tr.id), toCanonical(pair) || pair);
                    if (duckEnabled) { duckBuffers.trades.push([Date.now(),name,pair,'ccxtPro',tr.price||0,tr.amount||0,tr.side||'',tr.id||'']); }
                }
                f=0; } catch(e) {
                const em = e.message||''; const eCat = classifyError(em);
                addHealthEvent(name, 'ccxtPro', 'watchTradesErr', em);
                if (em.includes('RateLimitExceeded')||em.includes('429')) { addErr(name,'ccxtPro',`${pair}:rateLimit`); await sleep(15000); continue; }
                if (em.includes('NotSupported')||em.includes('not supported')) { addErr(name,'ccxtPro',`${pair}:tr:notSupported`); break; }
                if (em.includes('TypeError')||em.includes('BigInt')||em.includes('cannot read')||em.includes('Cannot convert')) { addErr(name,'ccxtPro',`${pair}:tr:typeErr`); f++; await sleep(5000); continue; }
                if (em.includes('Connection') && (em.includes('close')||em.includes('reset'))) { f++; await sleep(Math.min(2000*f,30000)); continue; }
                if (em.includes('timed out')||em.includes('timeout')||em.includes('ETIMEDOUT')) { f++; await sleep(Math.min(5000*f,60000)); continue; }
                f++; if(f<=2) addErr(name,'ccxtPro',`${pair}:tr:${em.slice(0,40)}`); await sleep(Math.min(3000*f,60000)); } } })(),
            // watchOrderBook worker — HARDENED
            (async () => { let f=0; while(!stopFlag && f<50) { try { const ob=await ex.watchOrderBook(pair); if(ob?.bids?.length) { addCPro(name,pair,0,1);
                // Track OB sequence IDs for correlation
                const obSeq = ob.nonce || ob.timestamp || ob.datetime;
                if (obSeq) trackOBId(name, 'ccxtPro', obSeq, toCanonical(pair) || pair);
                // DuckDB: store orderbook snapshot
                if (duckEnabled) { const bb=ob.bids[0]?.[0]||0,ba=ob.asks?.[0]?.[0]||0; duckBuffers.orderbook.push([Date.now(),name,pair,'ccxtPro',bb,ba,ob.bids.length,ob.asks?.length||0,ba>0&&bb>0?ba-bb:0]); }
                } f=0; } catch(e) {
                const em = e.message||'';
                addHealthEvent(name, 'ccxtPro', 'watchOBErr', em);
                if (em.includes('RateLimitExceeded')||em.includes('429')) { addErr(name,'ccxtPro',`${pair}:rateLimit`); await sleep(15000); continue; }
                if (em.includes('NotSupported')||em.includes('not supported')) { addErr(name,'ccxtPro',`${pair}:ob:notSupported`); break; }
                if (em.includes('TypeError')||em.includes('BigInt')||em.includes('cannot read')||em.includes('Cannot convert')) { addErr(name,'ccxtPro',`${pair}:ob:typeErr`); f++; await sleep(5000); continue; }
                if (em.includes('Connection') && (em.includes('close')||em.includes('reset'))) { f++; await sleep(Math.min(2000*f,30000)); continue; }
                if (em.includes('timed out')||em.includes('timeout')||em.includes('ETIMEDOUT')) { f++; await sleep(Math.min(5000*f,60000)); continue; }
                f++; if(f<=2) addErr(name,'ccxtPro',`${pair}:ob:${em.slice(0,40)}`); await sleep(Math.min(3000*f,60000)); } } })(),
            // watchTicker worker — HARDENED with fallback
            (async () => { let f=0; while(!stopFlag && f<10) { try { const tk=await ex.watchTicker(pair); addCProTicker(name,pair);
                // DuckDB: store ticker
                if (duckEnabled) { duckBuffers.tickers.push([Date.now(),name,pair,'ccxtPro',tk.last||0,tk.bid||0,tk.ask||0,tk.high||0,tk.low||0,tk.baseVolume||0,tk.quoteVolume||0,tk.percentage||0]); }
                f=0; } catch(e) {
                const em = e.message||'';
                if (em.includes('NotSupported')||em.includes('not supported')||em.includes('not a function')) { addErr(name,'ccxtPro',`${pair}:tk:notSupported`); addHealthEvent(name,'ccxtPro','tickerNotSupported',em); break; }
                if (em.includes('RateLimitExceeded')||em.includes('429')) { await sleep(15000); continue; }
                if (em.includes('Connection') && (em.includes('close')||em.includes('reset'))) { f++; await sleep(Math.min(2000*f,30000)); continue; }
                if (em.includes('timed out')||em.includes('timeout')) { f++; await sleep(Math.min(5000*f,60000)); continue; }
                f++; if(f<=1) addErr(name,'ccxtPro',`${pair}:tk:${em.slice(0,40)}`); await sleep(Math.min(5000*f,60000)); } } })()
        ]);
        await Promise.allSettled(workers);
        try { await ex.close(); } catch {}
    } catch(e) { addErr(name,'ccxtPro',`init:${e.message?.slice(0,60)}`); emitEvent('error',name,'ccxtPro',e.message?.slice(0,60)); }
}

// ═══════════════════ CCXT REST ENGINE (fetch* polling + fetchTicker) ═══════════════════
async function startCCXTRest(name, ccxtId, pairs) {
    try {
        // Use pre-loaded instance if available, otherwise create fresh
        let ex = preloadedRest[name];
        if (!ex) {
            const ExClass = ccxt[ccxtId];
            if (!ExClass) { addErr(name,'ccxtRest','No CCXT class for '+ccxtId); return; }
            const opts = buildCCXTOpts(ccxtId);
            delete opts.newUpdates;
            ex = new ExClass(opts);
            let mkLoaded = false;
            for (let mkAttempt = 0; mkAttempt < 3 && !mkLoaded; mkAttempt++) {
                try { await ex.loadMarkets(); mkLoaded = true; } catch(e) {
                    addErr(name,'ccxtRest',`loadMarkets(${mkAttempt+1}):${e.message?.slice(0,50)}`);
                    if (mkAttempt < 2) await sleep(5000 * (mkAttempt + 1));
                }
            }
            if (!mkLoaded) return;
        }
        stats[name].ccxtRest.connected = true;
        emitEvent('connect', name, 'ccxtRest', `REST poller connected (${pairs.length} pairs)`);
        const validPairs = pairs.filter(p => { if(ex.markets[p]) return true; addErr(name,'ccxtRest',`${p}:not found`); return false; });
        if (!validPairs.length) return;

        // Check fetchTicker support (some exchanges like WOO don't support it)
        let hasFetchTicker = true;
        try { if (typeof ex.fetchTicker !== 'function') hasFetchTicker = false; } catch{ hasFetchTicker = false; }

        // Polling loop — fetch trades + order book + ticker every 5s per pair
        while (!stopFlag) {
            for (const pair of validPairs) {
                if (stopFlag) break;
                try {
                    const promises = [
                        ex.fetchTrades(pair, undefined, 20),
                        ex.fetchOrderBook(pair, 5)
                    ];
                    if (hasFetchTicker) promises.push(ex.fetchTicker(pair));

                    const results = await Promise.allSettled(promises);
                    const [tr, ob] = results;
                    if (tr.status === 'fulfilled' && tr.value?.length) {
                        addCRest(name, pair, tr.value.length, 0);
                        // Per-trade hybrid dedup + correlation + DuckDB
                        for (const t of tr.value) {
                            const tid = String(t.id||t.timestamp||'');
                            if (tid) { addHybridTrade(name, toCanonical(pair)||pair, 1, tid); trackTradeId(name, 'ccxtRest', tid, toCanonical(pair)||pair); }
                            if (duckEnabled) { duckBuffers.trades.push([Date.now(),name,pair,'ccxtRest',t.price||0,t.amount||0,t.side||'',t.id||'']); }
                        }
                    }
                    if (ob.status === 'fulfilled' && ob.value?.bids?.length) {
                        addCRest(name, pair, 0, 1);
                        // Track OB sequence IDs
                        const obSeq = ob.value.nonce || ob.value.timestamp;
                        if (obSeq) trackOBId(name, 'ccxtRest', obSeq, toCanonical(pair) || pair);
                        if (duckEnabled) { const bb=ob.value.bids[0]?.[0]||0,ba=ob.value.asks?.[0]?.[0]||0; duckBuffers.orderbook.push([Date.now(),name,pair,'ccxtRest',bb,ba,ob.value.bids.length,ob.value.asks?.length||0,ba>0&&bb>0?ba-bb:0]); }
                    }
                    if (results[2]?.status === 'fulfilled' && results[2].value) {
                        const tk = results[2].value;
                        addCRestTicker(name, pair);
                        if (duckEnabled) { duckBuffers.tickers.push([Date.now(),name,pair,'ccxtRest',tk.last||0,tk.bid||0,tk.ask||0,tk.high||0,tk.low||0,tk.baseVolume||0,tk.quoteVolume||0,tk.percentage||0]); }
                    } else if (results[2]?.status === 'rejected') {
                        const em = results[2].reason?.message||'';
                        if (em.includes('not supported')||em.includes('NotSupported')) hasFetchTicker = false;
                    }
                } catch(e) {
                    const em = e.message||'';
                    if (em.includes('RateLimitExceeded')||em.includes('429')) { addErr(name,'ccxtRest',`${pair}:rateLimit`); await sleep(15000); continue; }
                    addErr(name,'ccxtRest',`${pair}:${em.slice(0,40)}`);
                }
            }
            await sleep(5000); // 5s polling interval
        }
    } catch(e) { addErr(name,'ccxtRest',`init:${e.message?.slice(0,60)}`); emitEvent('error',name,'ccxtRest',e.message?.slice(0,60)); }
}

// ═══════════════════ DIRECT REST ENGINE (4th stream — raw HTTP polling) ═══════════════════
// REST endpoint URL mappings for all exchanges (raw HTTP without CCXT)
const REST_ENDPOINTS = {
    'Binance':      { trades: s => `https://api.binance.com/api/v3/trades?symbol=${s}&limit=10`, ob: s => `https://api.binance.com/api/v3/depth?symbol=${s}&limit=5`, ticker: s => `https://api.binance.com/api/v3/ticker/24hr?symbol=${s}`, pairs: ['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','PENGUUSDT','PENGUUSDC','WIFUSDT','WIFUSDC','SUIUSDT','SUIUSDC','ENAUSDT','ENAUSDC'], parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => r?.bids?.length ? r : null, parseTicker: r => r?.lastPrice ? r : null },
    'Coinbase':     { trades: s => `https://api.exchange.coinbase.com/products/${s}/trades?limit=10`, ob: s => `https://api.exchange.coinbase.com/products/${s}/book?level=1`, ticker: s => `https://api.exchange.coinbase.com/products/${s}/ticker`, pairs: ['BTC-USD','ETH-USD','SOL-USD','BTC-USDT','ETH-USDT','SOL-USDT','PENGU-USDC','PENGU-USD','POPCAT-USDC','POPCAT-USD','WIF-USDC','WIF-USD','SUI-USDC','SUI-USD','ENA-USDC','ENA-USD'], parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => r?.bids?.length ? r : null, parseTicker: r => r?.price ? r : null },
    'Kraken':       { trades: s => `https://api.kraken.com/0/public/Trades?pair=${s}&count=10`, ob: s => `https://api.kraken.com/0/public/Depth?pair=${s}&count=5`, ticker: s => `https://api.kraken.com/0/public/Ticker?pair=${s}`, pairs: ['XBTUSDT','ETHUSDT','SOLUSDT','XBTUSD','ETHUSD','SOLUSD','XBTUSDC','ETHUSDC','SOLUSDC','PENGUUSDT','PENGUUSDC','PENGUUSD','POPCATUSD','WIFUSD','SUIUSD','ENAUSD'], parseTrades: r => { const k = Object.keys(r?.result||{}).find(x=>!x.includes('last')); return k ? r.result[k] : []; }, parseOB: r => { const k = Object.keys(r?.result||{})[0]; return k && r.result[k]?.bids?.length ? r.result[k] : null; }, parseTicker: r => { const k = Object.keys(r?.result||{})[0]; return k ? r.result[k] : null; } },
    'KuCoin':       { trades: s => `https://api.kucoin.com/api/v1/market/histories?symbol=${s}`, ob: s => `https://api.kucoin.com/api/v1/market/orderbook/level2_20?symbol=${s}`, ticker: s => `https://api.kucoin.com/api/v1/market/stats?symbol=${s}`, pairs: ['BTC-USDT','ETH-USDT','SOL-USDT','BTC-USDC','ETH-USDC','SOL-USDC','BRETT-USDT','PENGU-USDT','POPCAT-USDT','WIF-USDT','WIF-USDC','SUI-USDT','SUI-USDC','ENA-USDT','ENA-USDC'], parseTrades: r => r?.data || [], parseOB: r => r?.data?.bids?.length ? r.data : null, parseTicker: r => r?.data ? r.data : null },
    'OKX':          { trades: s => `https://www.okx.com/api/v5/market/trades?instId=${s}&limit=10`, ob: s => `https://www.okx.com/api/v5/market/books?instId=${s}&sz=5`, ticker: s => `https://www.okx.com/api/v5/market/ticker?instId=${s}`, pairs: ['BTC-USDT','ETH-USDT','SOL-USDT','BTC-USDC','ETH-USDC','SOL-USDC','PENGU-USDT','PENGU-USDC','PENGU-USD','WIF-USDT','WIF-USDC','WIF-USD','SUI-USDT','SUI-USDC','SUI-USD','ENA-USDT','ENA-USDC','ENA-USD'], parseTrades: r => r?.data || [], parseOB: r => r?.data?.[0]?.bids?.length ? r.data[0] : null, parseTicker: r => r?.data?.[0] ? r.data[0] : null },
    'Bybit':        { trades: s => `https://api.bybit.com/v5/market/recent-trade?category=spot&symbol=${s}&limit=10`, ob: s => `https://api.bybit.com/v5/market/orderbook?category=spot&symbol=${s}&limit=5`, ticker: s => `https://api.bybit.com/v5/market/tickers?category=spot&symbol=${s}`, pairs: ['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','BRETTUSDT','BRETTUSDC','PENGUUSDT','POPCATUSDT','WIFUSDT','WIFUSDC','SUIUSDT','SUIUSDC','ENAUSDT','ENAUSDC'], parseTrades: r => r?.result?.list || [], parseOB: r => r?.result?.b?.length ? r.result : null, parseTicker: r => r?.result?.list?.[0] ? r.result.list[0] : null },
    'Bitfinex':     { trades: s => `https://api-pub.bitfinex.com/v2/trades/${s}/hist?limit=10`, ob: s => `https://api-pub.bitfinex.com/v2/book/${s}/P0?len=5`, ticker: s => `https://api-pub.bitfinex.com/v2/ticker/${s}`, pairs: ['tBTCUSD','tETHUSD','tSOLUSD','tBTCUST','tETHUST','tSOLUST','tSUIUST','tSUIUSD','tENAUST','tENAUSD'], parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => Array.isArray(r) && r.length > 0 ? r : null, parseTicker: r => Array.isArray(r) && r.length >= 6 ? r : null },
    'Gate.io':      { trades: s => `https://api.gateio.ws/api/v4/spot/trades?currency_pair=${s}&limit=10`, ob: s => `https://api.gateio.ws/api/v4/spot/order_book?currency_pair=${s}&limit=5`, ticker: s => `https://api.gateio.ws/api/v4/spot/tickers?currency_pair=${s}`, pairs: ['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','SOL_USDC','BRETT_USDT','PENGU_USDT','POPCAT_USDT','WIF_USDT','WIF_USDC','SUI_USDT','SUI_USDC','ENA_USDT','ENA_USDC'], parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => r?.bids?.length ? r : null, parseTicker: r => Array.isArray(r) && r[0] ? r[0] : null },
    'HTX':          { trades: s => `https://api.huobi.pro/market/history/trade?symbol=${s}&size=10`, ob: s => `https://api.huobi.pro/market/depth?symbol=${s}&type=step0&depth=5`, ticker: s => `https://api.huobi.pro/market/detail/merged?symbol=${s}`, pairs: ['btcusdt','ethusdt','solusdt','btcusdc','ethusdc','solusdc','brettusdt','penguusdt','popcatusdt','wifusdt','suiusdt','enausdt'], parseTrades: r => r?.data?.[0]?.data || [], parseOB: r => r?.tick?.bids?.length ? r.tick : null, parseTicker: r => r?.tick ? r.tick : null },
    'WOO X':        { trades: s => `https://api.woo.org/v1/public/market_trades?symbol=${s}&limit=10`, ob: s => `https://api.woo.org/v1/public/orderbook/${s}`, ticker: s => `https://api.woo.org/v1/public/token_price?symbol=${s}`, pairs: ['SPOT_BTC_USDT','SPOT_ETH_USDT','SPOT_SOL_USDT','SPOT_BRETT_USDT','SPOT_PENGU_USDT','SPOT_POPCAT_USDT','SPOT_WIF_USDT','SPOT_SUI_USDT','SPOT_ENA_USDT'], parseTrades: r => r?.rows || [], parseOB: r => r?.bids?.length ? r : null, parseTicker: r => r?.success ? r : null },
    'Crypto.com':   { trades: s => `https://api.crypto.com/exchange/v1/public/get-trades?instrument_name=${s}&count=10`, ob: s => `https://api.crypto.com/exchange/v1/public/get-book?instrument_name=${s}&depth=5`, ticker: s => `https://api.crypto.com/exchange/v1/public/get-ticker?instrument_name=${s}`, pairs: ['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USD','ETH_USD','SOL_USD','PENGU_USDT','PENGU_USD','POPCAT_USD','WIF_USDT','WIF_USD','SUI_USDT','SUI_USD','ENA_USDT','ENA_USD'], parseTrades: r => r?.result?.data || [], parseOB: r => r?.result?.data?.[0]?.bids?.length ? r.result.data[0] : null, parseTicker: r => r?.result?.data?.[0] ? r.result.data[0] : null },
    'Bitstamp':     { trades: s => `https://www.bitstamp.net/api/v2/transactions/${s}/`, ob: s => `https://www.bitstamp.net/api/v2/order_book/${s}/`, ticker: s => `https://www.bitstamp.net/api/v2/ticker/${s}/`, pairs: ['btcusd','ethusd','solusd','btcusdt','ethusdt','solusdt','btcusdc','ethusdc','solusdc','penguusd','popcatusd','wifusd','suiusd','enausd'], parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => r?.bids?.length ? r : null, parseTicker: r => r?.last ? r : null },
    'WhiteBIT':     { trades: s => `https://whitebit.com/api/v4/public/trades/${s}`, ob: s => `https://whitebit.com/api/v4/public/orderbook/${s}?limit=5`, ticker: s => `https://whitebit.com/api/v4/public/ticker`, pairs: ['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','SOL_USDC','PENGU_USDT','PENGU_USDC','WIF_USDT','WIF_USDC','SUI_USDT','SUI_USDC','ENA_USDT','ENA_USDC'], parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => r?.bids?.length ? r : null, parseTicker: r => r ? r : null },
    'AscendEX':     { trades: s => `https://ascendex.com/api/pro/v1/trades?symbol=${s}`, ob: s => `https://ascendex.com/api/pro/v1/depth?symbol=${s}&n=5`, pairs: ['BTC/USDT','ETH/USDT','SOL/USDT','BRETT/USDT','PENGU/USDT','WIF/USDT','SUI/USDT','ENA/USDT'], parseTrades: r => r?.data || [], parseOB: r => r?.data?.data?.bids?.length ? r.data.data : null },
    'BingX':        { trades: s => `https://open-api.bingx.com/openApi/spot/v1/market/trades?symbol=${s}&limit=10`, ob: s => `https://open-api.bingx.com/openApi/spot/v1/market/depth?symbol=${s}&limit=5`, pairs: ['BTC-USDT','ETH-USDT','SOL-USDT','BTC-USDC','ETH-USDC','SOL-USDC','BRETT-USDT','PENGU-USDT','POPCAT-USDT','WIF-USDT','SUI-USDT','SUI-USDC','ENA-USDT'], parseTrades: r => r?.data || [], parseOB: r => r?.data?.bids?.length ? r.data : null },
    'Toobit':       { trades: s => `https://api.toobit.com/quote/v1/trades?symbol=${s}&limit=10`, ob: s => `https://api.toobit.com/quote/v1/depth?symbol=${s}&limit=5`, pairs: ['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','BRETTUSDT','PENGUUSDT','POPCATUSDT','WIFUSDT','SUIUSDT','ENAUSDT'], parseTrades: r => r?.result || [], parseOB: r => r?.result?.bids?.length ? r.result : null },
    'Deepcoin':     { trades: s => `https://api.deepcoin.com/deepcoin/market/trades?instId=${s}&limit=10`, ob: s => `https://api.deepcoin.com/deepcoin/market/books?instId=${s}&sz=5`, pairs: ['BTC-USDT','ETH-USDT','SOL-USDT','BTC-USDC','ETH-USDC','SOL-USDC','PENGU-USDT','WIF-USDT'], parseTrades: r => r?.data || [], parseOB: r => r?.data?.[0]?.bids?.length ? r.data[0] : null },
    'XT.com':       { trades: s => `https://sapi.xt.com/v4/public/trade/recent?symbol=${s}&limit=10`, ob: s => `https://sapi.xt.com/v4/public/depth?symbol=${s}&limit=5`, pairs: ['btc_usdt','eth_usdt','sol_usdt','btc_usdc','eth_usdc','sol_usdc','brett_usdt','pengu_usdt','popcat_usdt','wif_usdt','sui_usdt','sui_usdc','ena_usdt'], parseTrades: r => r?.result || [], parseOB: r => r?.result?.bids?.length ? r.result : null },
    'Bitget':       { trades: s => `https://api.bitget.com/api/v2/spot/market/fills?symbol=${s}&limit=10`, ob: s => `https://api.bitget.com/api/v2/spot/market/orderbook?symbol=${s}&limit=5`, pairs: ['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','BRETTUSDT','PENGUUSDT','WIFUSDT','SUIUSDT','SUIUSDC','ENAUSDT'], parseTrades: r => r?.data || [], parseOB: r => r?.data?.bids?.length ? r.data : null },
    'Gemini':       { trades: s => `https://api.gemini.com/v1/trades/${s}?limit_trades=10`, ob: s => `https://api.gemini.com/v1/book/${s}?limit_bids=5&limit_asks=5`, pairs: ['btcusd','ethusd','solusd','btcusdt','ethusdt','penguusd','penguusdc','popcatusd','popcatusdc','wifusd','wifusdc'], parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => r?.bids?.length ? r : null },
    'Binance.US':   { trades: s => `https://api.binance.us/api/v3/trades?symbol=${s}&limit=10`, ob: s => `https://api.binance.us/api/v3/depth?symbol=${s}&limit=5`, pairs: ['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSD','ETHUSD','SOLUSD','BRETTUSDT','PENGUUSDT','POPCATUSDT','WIFUSDT','SUIUSDT','SUIUSD','ENAUSDT'], parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => r?.bids?.length ? r : null },
    'MEXC':         { trades: s => `https://api.mexc.com/api/v3/trades?symbol=${s}&limit=10`, ob: s => `https://api.mexc.com/api/v3/depth?symbol=${s}&limit=5`, pairs: ['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','PENGUUSDT','PENGUUSDC','POPCATUSDT','POPCATUSDC','WIFUSDT','WIFUSDC','SUIUSDT','SUIUSDC','ENAUSDT','ENAUSDC'], parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => r?.bids?.length ? r : null },
    'CoinEx':       { trades: s => `https://api.coinex.com/v2/spot/deals?market=${s}&limit=10`, ob: s => `https://api.coinex.com/v2/spot/depth?market=${s}&limit=5`, pairs: ['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','BRETTUSDT','PENGUUSDT','POPCATUSDT','WIFUSDT','WIFUSDC','SUIUSDT','SUIUSDC','ENAUSDT','ENAUSDC'], parseTrades: r => r?.data || [], parseOB: r => r?.data?.depth?.bids?.length ? r.data.depth : null },
    'LBank':        { trades: s => `https://api.lbkex.com/v2/trades.do?symbol=${s}&size=10`, ob: s => `https://api.lbkex.com/v2/depth.do?symbol=${s}&size=5`, pairs: ['btc_usdt','eth_usdt','sol_usdt','btc_usdc','eth_usdc','sol_usdc','brett_usdt','pengu_usdt','popcat_usdt','wif_usdt','sui_usdt','ena_usdt'], parseTrades: r => r?.data || [], parseOB: r => r?.data?.bids?.length ? r.data : null },
    'BitMart':      { trades: s => `https://api-cloud.bitmart.com/spot/quotation/v3/trades?symbol=${s}&limit=10`, ob: s => `https://api-cloud.bitmart.com/spot/quotation/v3/books?symbol=${s}&limit=5`, pairs: ['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','SOL_USDC','BRETT_USDT','PENGU_USDT','POPCAT_USDT','WIF_USDT','SUI_USDT','ENA_USDT'], parseTrades: r => r?.data || [], parseOB: r => r?.data?.buys?.length ? r.data : null },
    'Poloniex':     { trades: s => `https://api.poloniex.com/markets/${s}/trades?limit=10`, ob: s => `https://api.poloniex.com/markets/${s}/orderBook?limit=5`, pairs: ['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','BRETT_USDT','POPCAT_USDT','WIF_USDT','SUI_USDT','ENA_USDT'], parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => r?.bids?.length ? r : null },
    'HitBTC':       { trades: s => `https://api.hitbtc.com/api/3/public/trades/${s}?limit=10`, ob: s => `https://api.hitbtc.com/api/3/public/orderbook/${s}?depth=5`, pairs: ['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','BRETTUSDT','PENGUUSDT','POPCATUSDT','WIFUSDT','SUIUSDT','ENAUSDT'], parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => r?.bid?.length ? r : null },
    'BTSE':         { trades: s => `https://api.btse.com/spot/api/v3.2/market_summary?symbol=${s}`, ob: s => `https://api.btse.com/spot/api/v3.2/orderbook?symbol=${s}&depth=5`, pairs: ['BTC-USD','ETH-USD','SOL-USD','BTC-USDT','ETH-USDT','SOL-USDT','BTC-USDC','ETH-USDC','SOL-USDC','BRETT-USDT','PENGU-USDT','POPCAT-USDT','WIF-USDT','SUI-USDT','ENA-USDT'], parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => r?.buyQuote?.length ? r : null },
    'Bitrue':       { trades: s => `https://openapi.bitrue.com/api/v1/trades?symbol=${s}&limit=10`, ob: s => `https://openapi.bitrue.com/api/v1/depth?symbol=${s}&limit=5`, pairs: ['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','BRETTUSDT','PENGUUSDT','PENGUUSDC','POPCATUSDT','WIFUSDT','WIFUSDC','SUIUSDT','SUIUSDC','ENAUSDT','ENAUSDC'], parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => r?.bids?.length ? r : null },
    'BloFin':       { trades: s => `https://openapi.blofin.com/api/v1/market/trades?instId=${s}&limit=10`, ob: s => `https://openapi.blofin.com/api/v1/market/books?instId=${s}&sz=5`, pairs: ['BTC-USDT','ETH-USDT','SOL-USDT','BTC-USDC','ETH-USDC','SOL-USDC'], parseTrades: r => r?.data || [], parseOB: r => r?.data?.[0]?.bids?.length ? r.data[0] : null },
    'DigiFinex':    { trades: s => `https://openapi.digifinex.com/v3/trades?symbol=${s}&limit=10`, ob: s => `https://openapi.digifinex.com/v3/order_book?symbol=${s}&limit=5`, pairs: ['btc_usdt','eth_usdt','sol_usdt','btc_usdc','eth_usdc','sol_usdc','brett_usdt'], parseTrades: r => r?.data || [], parseOB: r => r?.bids?.length ? r : null },
    'EXMO':         { trades: s => `https://api.exmo.com/v1.1/trades?pair=${s}`, ob: s => `https://api.exmo.com/v1.1/order_book?pair=${s}&limit=5`, pairs: ['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','SOL_USDC','BTC_USD','ETH_USD','PENGU_USDT','PENGU_USDC','WIF_USDT','WIF_USDC','SUI_USDT','SUI_USDC','ENA_USDT','ENA_USDC'], parseTrades: r => { const k = Object.keys(r||{})[0]; return k ? r[k] : []; }, parseOB: r => { const k = Object.keys(r||{})[0]; return k && r[k]?.bid?.length ? r[k] : null; } },
    'CEX.IO':       { trades: s => `https://cex.io/api/trade_history/${s.replace('_','/')}/`, ob: s => `https://cex.io/api/order_book/${s.replace('_','/')}/?depth=5`, pairs: ['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USD','ETH_USD','SOL_USD','BTC_USDC','ETH_USDC','SOL_USDC','BRETT_USDT','BRETT_USDC','BRETT_USD','PENGU_USDT','PENGU_USD','POPCAT_USDT','POPCAT_USD','WIF_USDT','WIF_USDC','WIF_USD','SUI_USDT','SUI_USDC','SUI_USD','ENA_USDT','ENA_USDC','ENA_USD'], parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => r?.bids?.length ? r : null },
    'OrangeX':      { trades: s => `https://api.orangex.com/api/v1/public/get_last_trades_by_instrument?instrument_name=${s}-SPOT&count=5`, ob: s => `https://api.orangex.com/api/v1/public/get_order_book?instrument_name=${s}-SPOT&depth=5`, pairs: ['BTC-USDT','ETH-USDT','SOL-USDT','BTC-USDC','ETH-USDC'], parseTrades: r => r?.result?.trades || [], parseOB: r => r?.result?.bids ? r.result : null },
    'Azbit':        { trades: s => `https://data.azbit.com/api/deals?currencyPairCode=${s}`, ob: s => `https://data.azbit.com/api/orderbook?currencyPairCode=${s}`, pairs: ['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','SOL_USDC','BRETT_USDT','PENGU_USDT','POPCAT_USDT','WIF_USDT','SUI_USDT','ENA_USDT'], parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => Array.isArray(r) && r.length ? r : null },
    'BVOX':         { trades: s => `https://api.bitvenus.me/openapi/quote/v1/trades?symbol=${s}&limit=5`, ob: s => `https://api.bitvenus.me/openapi/quote/v1/depth?symbol=${s}&limit=5`, pairs: ['BTCUSDT','ETHUSDT','SOLUSDT'], parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => r?.bids?.length ? r : null },
    'Trubit Pro':   { trades: s => `https://api-spot.trubit.com/openapi/quote/v1/trades?symbol=${s}&limit=5`, ob: s => `https://api-spot.trubit.com/openapi/quote/v1/depth?symbol=${s}&limit=5`, pairs: ['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC'], parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => r?.bids?.length ? r : null },
    'BigONE':       { trades: s => `https://big.one/api/v3/asset_pairs/${s}/trades`, ob: s => `https://big.one/api/v3/asset_pairs/${s}/depth`, pairs: ['BTC-USDT','ETH-USDT','SOL-USDT','PENGU-USDT','WIF-USDT','SUI-USDT','ENA-USDT'], parseTrades: r => r?.data || [], parseOB: r => r?.data?.bids?.length ? r.data : null },
    'LATOKEN':      { trades: s => `https://api.latoken.com/v2/trade/history/${s}`, ob: s => `https://api.latoken.com/v2/book/${s}`, pairs: [['4f4a4e5e-7192-4e7e-9f78-d8f6e07c0001','0c3a106d-bde3-4c13-a26e-3fd2394529e5'],['620f2019-33c0-423b-8a9d-cde4d7f8ef7f','0c3a106d-bde3-4c13-a26e-3fd2394529e5'],['f5924e5b-3860-4a3c-94d0-6c3fd4999e73','0c3a106d-bde3-4c13-a26e-3fd2394529e5']], isUUID: true, parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => r?.bid?.length ? r : null },
    'Coinstore':    { trades: s => `https://api.coinstore.com/api/v1/market/trade/${s}?size=5`, ob: s => `https://api.coinstore.com/api/v1/market/depth/${s}?depth=5`, pairs: ['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','BRETTUSDT','PENGUUSDT','POPCATUSDT','WIFUSDT','SUIUSDT','ENAUSDT'], parseTrades: r => r?.data || [], parseOB: r => r?.data?.bids?.length ? r.data : null },
    'GroveX':       { trades: s => `https://openapi.grovex.io/open/api/get_trades?symbol=${s}`, ob: s => `https://openapi.grovex.io/open/api/market_dept?symbol=${s}&type=step0`, pairs: ['btcusdt','ethusdt','solusdt','btcusdc','ethusdc','solusdc'], parseTrades: r => r?.data || [], parseOB: r => r?.data?.tick?.asks ? r.data.tick : null },
    'CoinW':        { trades: s => `https://api.coinw.com/api/v1/public?command=returnTradeHistory&symbol=${s}`, ob: s => `https://api.coinw.com/api/v1/public?command=returnOrderBook&symbol=${s}`, pairs: ['btc_usdt','eth_usdt','sol_usdt','btc_usdc','eth_usdc','sol_usdc','brett_usdt','pengu_usdt','popcat_usdt','wif_usdt','sui_usdt','ena_usdt'], parseTrades: r => r?.code === '200' && Array.isArray(r?.data) ? r.data : [], parseOB: r => r?.code === '200' && r?.data?.bids ? r.data : null },
    'Batonex':      { trades: s => `https://api.batonex.com/openapi/quote/v1/trades?symbol=${s}&limit=5`, ob: s => `https://api.batonex.com/openapi/quote/v1/depth?symbol=${s}&limit=5`, pairs: ['BTCUSDT','ETHUSDT','SOLUSDT','WIFUSDT'], parseTrades: r => Array.isArray(r) ? r : [], parseOB: r => r?.bids?.length ? r : null },  // WIF confirmed listed (32-pair exchange; BRETT/PENGU/POPCAT/SUI/ENA not listed)
    'Bullish':      { trades: null, ob: s => `https://api.exchange.bullish.com/trading-api/v1/markets/${s}/orderbook/hybrid`, pairs: ['BTCUSDC','ETHUSDC','SOLUSDC','BTCUSDT','ETHUSDT','SOLUSDT','BTCUSD','ETHUSD','SOLUSD','PENGUUSDC','PENGUUSDT','WIFUSDC','SUIUSDC'], parseTrades: () => [], parseOB: r => r?.bids?.length || r?.asks?.length ? r : null },
    'Hotcoin':      { trades: s => `https://api.hotcoinfin.com/v1/market/trade?symbol=${s}&size=10`, ob: s => `https://api.hotcoinfin.com/v1/market/depth?symbol=${s}&type=step0`, pairs: ['btc_usdt','eth_usdt','sol_usdt','brett_usdt','pengu_usdt','popcat_usdt','wif_usdt','sui_usdt','ena_usdt'], parseTrades: r => r?.data?.length ? r.data : (r?.data?.data?.length ? r.data.data : []), parseOB: r => r?.data?.bids?.length ? r.data : null },
    'Websea':       { trades: null, ob: s => `https://oapi.websea.com/v1/spot/depth?symbol=${s}&size=5`, pairs: ['BTC-USDT','ETH-USDT','SOL-USDT','BRETT-USDT','PENGU-USDT','POPCAT-USDT','WIF-USDT','SUI-USDT','ENA-USDT'], parseTrades: () => [], parseOB: r => r?.errno === 0 && r.result ? r.result : null },
    'Darkex':       { trades: null, ob: null, pairs: ['btcusdt','ethusdt'] },
};
// Also add entries for exchanges using WOO-like or missing endpoints
['Pionex','Zoomex','Biconomy','NovaEx','FameEX','CEEX'].forEach(n => { if (!REST_ENDPOINTS[n]) REST_ENDPOINTS[n] = null; });

async function startDirectRest(name) {
    const ep = REST_ENDPOINTS[name];
    if (!ep || !ep.pairs?.length) return; // No REST endpoints defined
    stats[name].directRest.connected = true;
    emitEvent('connect', name, 'directRest', `Direct REST poller started (${ep.pairs.length} pairs)`);
    while (!stopFlag) {
        for (const pairRaw of ep.pairs) {
            if (stopFlag) break;
            try {
                const pair = ep.isUUID ? `${pairRaw[0]}/${pairRaw[1]}` : pairRaw;
                const sym = ep.isUUID ? pairRaw.join('/') : pairRaw;
                // Fetch trades
                if (ep.trades) {
                    try {
                        const url = ep.trades(sym);
                        const r = await httpsReq(url).catch(() => null);
                        if (r) {
                            const trades = ep.parseTrades(r);
                            if (Array.isArray(trades) && trades.length > 0) {
                                addDirectRest(name, pair, Math.min(trades.length, 20), 0);
                                if (duckEnabled) { for (const t of trades.slice(0, 10)) { duckBuffers.trades.push([Date.now(), name, pair, 'directRest', t.price || t.p || 0, t.amount || t.qty || t.a || 0, t.side || t.type || '', t.id || t.trade_id || '']); } }
                            }
                        }
                    } catch (e) { addDirectRestErr(name, `${pair}:trades:${e.message?.slice(0,40)}`); }
                }
                // Fetch orderbook
                if (ep.ob) {
                    try {
                        const url = ep.ob(sym);
                        const r = await httpsReq(url).catch(() => null);
                        if (r) {
                            const ob = ep.parseOB(r);
                            if (ob) {
                                addDirectRest(name, pair, 0, 1);
                                if (duckEnabled) {
                                    const bids = ob.bids || ob.bid || [];
                                    const asks = ob.asks || ob.ask || [];
                                    const bb = (Array.isArray(bids[0]) ? bids[0][0] : bids[0]?.price) || 0;
                                    const ba = (Array.isArray(asks[0]) ? asks[0][0] : asks[0]?.price) || 0;
                                    duckBuffers.orderbook.push([Date.now(), name, pair, 'directRest', bb, ba, bids.length, asks.length, ba > 0 && bb > 0 ? ba - bb : 0]);
                                }
                            }
                        }
                    } catch (e) { addDirectRestErr(name, `${pair}:ob:${e.message?.slice(0,40)}`); }
                }
                // Fetch ticker (if available)
                if (ep.ticker && ep.parseTicker) {
                    try {
                        const url = ep.ticker(sym);
                        const r = await httpsReq(url).catch(() => null);
                        if (r) {
                            const tk = ep.parseTicker(r);
                            if (tk) {
                                addDirectRestTicker(name, pair);
                                if (duckEnabled) { duckBuffers.tickers.push([Date.now(), name, pair, 'directRest', tk.last || tk.lastPrice || tk.price || 0, tk.bid || 0, tk.ask || 0, tk.high || tk.high24h || 0, tk.low || tk.low24h || 0, tk.baseVolume || tk.volume || 0, tk.quoteVolume || 0, tk.percentage || tk.priceChangePercent || 0]); }
                            }
                        }
                    } catch {}
                }
            } catch (e) { addDirectRestErr(name, e.message?.slice(0, 60)); }
        }
        await sleep(8000); // 8s polling interval to avoid rate limits
    }
}

// ═══════════════════ HTTP SERVER + DASHBOARD ═══════════════════
function buildStats() {
    const elapsed = Math.floor((Date.now()-startTime)/1000);
    const remaining = Math.max(0, Math.floor(TEST_DURATION/1000)-elapsed);
    const exchanges = {}; let nW=0,pW=0,rW=0,dW=0,ti=0,nO=0,bf=0,totalActive=0,totalTickers=0,totalDirectRest=0;
    for (const [name,s] of Object.entries(stats)) {
        const nTot = s.native.trades+s.native.orderbook;
        const pTot = s.ccxtPro.trades+s.ccxtPro.orderbook+(s.ccxtPro.tickers||0);
        const rTot = s.ccxtRest.trades+s.ccxtRest.orderbook+(s.ccxtRest.tickers||0);
        const dTot = (s.directRest?.trades||0)+(s.directRest?.orderbook||0)+(s.directRest?.tickers||0);
        totalDirectRest += dTot;
        if (nTot > 0 || pTot > 0 || rTot > 0 || dTot > 0) totalActive++;
        totalTickers += (s.native.tickers||0) + (s.ccxtPro.tickers||0) + (s.ccxtRest.tickers||0) + (s.directRest?.tickers||0);
        let winner, margin;
        const best = Math.max(nTot, pTot, rTot, dTot);
        if (best === 0) { winner='failed'; bf++; margin='-'; }
        else if (!s.ccxtId) { winner='native-only'; nO++; margin='-'; }
        else if (nTot >= pTot && nTot >= rTot && nTot >= dTot) {
            const second = Math.max(pTot, rTot, dTot);
            if (second === 0) { winner='native'; nW++; margin='∞'; }
            else if (nTot > second * 1.3) { winner='native'; nW++; margin=`+${Math.round((nTot/second-1)*100)}%`; }
            else { winner='tie'; ti++; margin=`${Math.abs(Math.round((nTot/Math.max(second,1)-1)*100))}%`; }
        } else if (pTot >= nTot && pTot >= rTot && pTot >= dTot) {
            if (nTot === 0 && rTot === 0 && dTot === 0) { winner='ccxtPro'; pW++; margin='∞'; }
            else { const second = Math.max(nTot, rTot, dTot); if (pTot > second * 1.3) { winner='ccxtPro'; pW++; margin=`+${Math.round((pTot/second-1)*100)}%`; } else { winner='tie'; ti++; margin=`${Math.abs(Math.round((pTot/Math.max(second,1)-1)*100))}%`; } }
        } else if (dTot >= nTot && dTot >= pTot && dTot >= rTot) {
            if (nTot === 0 && pTot === 0 && rTot === 0) { winner='directRest'; dW++; margin='∞'; }
            else { const second = Math.max(nTot, pTot, rTot); if (dTot > second * 1.3) { winner='directRest'; dW++; margin=`+${Math.round((dTot/second-1)*100)}%`; } else { winner='tie'; ti++; margin=`${Math.abs(Math.round((dTot/Math.max(second,1)-1)*100))}%`; } }
        } else {
            if (nTot === 0 && pTot === 0 && dTot === 0) { winner='ccxtRest'; rW++; margin='∞'; }
            else { const second = Math.max(nTot, pTot, dTot); if (rTot > second * 1.3) { winner='ccxtRest'; rW++; margin=`+${Math.round((rTot/second-1)*100)}%`; } else { winner='tie'; ti++; margin=`${Math.abs(Math.round((rTot/Math.max(second,1)-1)*100))}%`; } }
        }
        // Trade ID correlation for this exchange
        const cor = tradeIdCorrelation[name] || { matches: { nativePro: 0, nativeRest: 0, proRest: 0, nativeDirect: 0 }, checked: 0, total: { n: 0, p: 0, r: 0, d: 0 } };
        const corPct = cor.checked > 0 ? {
            nativePro: cor.total.n > 0 && cor.total.p > 0 ? Math.round(cor.matches.nativePro / Math.min(cor.total.n, cor.total.p) * 100) : 0,
            nativeRest: cor.total.n > 0 && cor.total.r > 0 ? Math.round(cor.matches.nativeRest / Math.min(cor.total.n, cor.total.r) * 100) : 0,
            proRest: cor.total.p > 0 && cor.total.r > 0 ? Math.round(cor.matches.proRest / Math.min(cor.total.p, cor.total.r) * 100) : 0,
            nativeDirect: cor.total.n > 0 && (cor.total.d||0) > 0 ? Math.round((cor.matches.nativeDirect||0) / Math.min(cor.total.n, cor.total.d||1) * 100) : 0,
        } : { nativePro: 0, nativeRest: 0, proRest: 0, nativeDirect: 0 };

        // OB ID correlation
        const obc = obIdCorrelation[name] || { matches:{nativePro:0,nativeRest:0,proRest:0,nativeDirect:0}, checked:0, total:{n:0,p:0,r:0,d:0} };
        const obcPct = obc.checked > 0 ? {
            nativePro: obc.total.n>0&&obc.total.p>0 ? Math.round(obc.matches.nativePro/Math.min(obc.total.n,obc.total.p)*100) : 0,
            nativeRest: obc.total.n>0&&obc.total.r>0 ? Math.round(obc.matches.nativeRest/Math.min(obc.total.n,obc.total.r)*100) : 0,
            proRest: obc.total.p>0&&obc.total.r>0 ? Math.round(obc.matches.proRest/Math.min(obc.total.p,obc.total.r)*100) : 0,
            nativeDirect: obc.total.n>0&&obc.total.d>0 ? Math.round(obc.matches.nativeDirect/Math.min(obc.total.n,obc.total.d)*100) : 0,
        } : { nativePro:0, nativeRest:0, proRest:0, nativeDirect:0 };

        // Health metrics
        const hm = healthMetrics[name];
        const hs = hm ? calcHealthScore(name) : 100;
        const fixes = hm ? getFixRecommendation(name) : [];

        exchanges[name] = { tier:s.tier, nativeType:s.nativeType, ccxtId:s.ccxtId, hasRestEndpoint:s.hasRestEndpoint,
            native: { trades:s.native.trades, orderbook:s.native.orderbook, tickers:s.native.tickers||0, total:nTot, connected:s.native.connected, reconnects:s.native.reconnects||0, errors:s.native.errors, pairs:s.native.pairs, deduped:s.native.deduped, validated:s.native.validated, staleOB:s.native.staleOB, restFallbacks:s.native.restFallbacks, lastMsg:s.native.lastMsg },
            ccxtPro: { trades:s.ccxtPro.trades, orderbook:s.ccxtPro.orderbook, tickers:s.ccxtPro.tickers||0, total:pTot, connected:s.ccxtPro.connected, errors:s.ccxtPro.errors, pairs:s.ccxtPro.pairs, lastMsg:s.ccxtPro.lastMsg },
            ccxtRest: { trades:s.ccxtRest.trades, orderbook:s.ccxtRest.orderbook, tickers:s.ccxtRest.tickers||0, total:rTot, connected:s.ccxtRest.connected, errors:s.ccxtRest.errors, pairs:s.ccxtRest.pairs, lastMsg:s.ccxtRest.lastMsg },
            directRest: { trades:s.directRest?.trades||0, orderbook:s.directRest?.orderbook||0, tickers:s.directRest?.tickers||0, total:dTot, connected:s.directRest?.connected||false, errors:s.directRest?.errors||[], pairs:s.directRest?.pairs||{}, lastMsg:s.directRest?.lastMsg||0 },
            correlation: { matches: cor.matches, pct: corPct, checked: cor.checked, total: cor.total },
            obCorrelation: { matches: obc.matches, pct: obcPct, checked: obc.checked, total: obc.total },
            health: { score: hs, errors: hm?.errors||{}, totalErrors: hm?.totalErrors||0, fixes, connectionHistory: (hm?.connectionHistory||[]).slice(-10), lastError: hm?.lastError||{} },
            hybrid: { trades: hybridStats[name]?.trades||0, orderbook: hybridStats[name]?.orderbook||0, tickers: hybridStats[name]?.tickers||0, total: (hybridStats[name]?.trades||0)+(hybridStats[name]?.orderbook||0)+(hybridStats[name]?.tickers||0), deduped: hybridStats[name]?.deduped||0, pairs: hybridStats[name]?.pairs||{} },
            winner, margin };
    }
    const symbols = {};
    for (const [exName, s] of Object.entries(stats)) {
        for (const [p,v] of Object.entries(s.native.pairs)){if(!symbols[p])symbols[p]={nT:0,nO:0,pT:0,pO:0,rT:0,rO:0,dT:0,dO:0,ec:0,exchanges:{}};symbols[p].nT+=v.trades;symbols[p].nO+=v.ob;symbols[p].ec++;if(!symbols[p].exchanges[exName])symbols[p].exchanges[exName]={nT:0,nO:0,pT:0,pO:0,rT:0,rO:0,dT:0,dO:0};symbols[p].exchanges[exName].nT+=v.trades;symbols[p].exchanges[exName].nO+=v.ob;}
        for (const [p,v] of Object.entries(s.ccxtPro.pairs)){if(!symbols[p])symbols[p]={nT:0,nO:0,pT:0,pO:0,rT:0,rO:0,dT:0,dO:0,ec:0,exchanges:{}};symbols[p].pT+=v.trades;symbols[p].pO+=v.ob;if(!symbols[p].exchanges[exName])symbols[p].exchanges[exName]={nT:0,nO:0,pT:0,pO:0,rT:0,rO:0,dT:0,dO:0};symbols[p].exchanges[exName].pT+=v.trades;symbols[p].exchanges[exName].pO+=v.ob;}
        for (const [p,v] of Object.entries(s.ccxtRest.pairs)){if(!symbols[p])symbols[p]={nT:0,nO:0,pT:0,pO:0,rT:0,rO:0,dT:0,dO:0,ec:0,exchanges:{}};symbols[p].rT+=v.trades;symbols[p].rO+=v.ob;if(!symbols[p].exchanges[exName])symbols[p].exchanges[exName]={nT:0,nO:0,pT:0,pO:0,rT:0,rO:0,dT:0,dO:0};symbols[p].exchanges[exName].rT+=v.trades;symbols[p].exchanges[exName].rO+=v.ob;}
        if (s.directRest) { for (const [p,v] of Object.entries(s.directRest.pairs||{})){if(!symbols[p])symbols[p]={nT:0,nO:0,pT:0,pO:0,rT:0,rO:0,dT:0,dO:0,ec:0,exchanges:{}};symbols[p].dT+=(v.trades||0);symbols[p].dO+=(v.ob||0);if(!symbols[p].exchanges[exName])symbols[p].exchanges[exName]={nT:0,nO:0,pT:0,pO:0,rT:0,rO:0,dT:0,dO:0};symbols[p].exchanges[exName].dT+=(v.trades||0);symbols[p].exchanges[exName].dO+=(v.ob||0);} }
    }
    // Global health summary
    const healthSummary = { avgScore: 0, critical: [], warnings: [], healthy: [] };
    const scores = Object.keys(exchanges).map(n=>exchanges[n].health.score);
    healthSummary.avgScore = scores.length ? Math.round(scores.reduce((a,b)=>a+b,0)/scores.length) : 0;
    for (const [n,ex] of Object.entries(exchanges)) {
        if (ex.health.score < 50) healthSummary.critical.push(n);
        else if (ex.health.score < 80) healthSummary.warnings.push(n);
        else healthSummary.healthy.push(n);
    }
    // Hybrid summary
    const totalHybridTrades = Object.values(hybridStats).reduce((a,h)=>a+h.trades,0);
    const totalHybridOB = Object.values(hybridStats).reduce((a,h)=>a+h.orderbook,0);
    const totalHybridTickers = Object.values(hybridStats).reduce((a,h)=>a+h.tickers,0);
    const totalHybridDeduped = Object.values(hybridStats).reduce((a,h)=>a+h.deduped,0);
    const hybridSummary = { trades: totalHybridTrades, orderbook: totalHybridOB, tickers: totalHybridTickers, total: totalHybridTrades+totalHybridOB+totalHybridTickers, deduped: totalHybridDeduped };
    return { running:!stopFlag, uptime:elapsed, duration:Math.floor(TEST_DURATION/1000), remaining, exchanges, symbols,
        summary:{nativeWins:nW,ccxtProWins:pW,ccxtRestWins:rW,directRestWins:dW,ties:ti,nativeOnly:nO,bothFailed:bf,totalActive,totalTickers,totalDirectRest},
        enrichment: enrichStats, healthSummary, hybridSummary, subManager: getSubManagerStats() };
}

const server = http.createServer((req,res) => {
    if (req.url==='/'||req.url==='/dashboard') { res.writeHead(200,{'Content-Type':'text/html'}); try{res.end(fs.readFileSync(path.join(__dirname,'compare-dashboard-v7.html')));}catch{try{res.end(fs.readFileSync(path.join(__dirname,'compare-dashboard-v6.html')));}catch{res.end('<h1>Dashboard file not found</h1>');}} }
    else if (req.url==='/api/stats') { res.writeHead(200,{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'}); res.end(JSON.stringify(buildStats())); }
    else if (req.url==='/api/health') { res.writeHead(200,{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
        const h = {}; for (const n of Object.keys(stats)) { const hm = healthMetrics[n]; h[n] = { score: hm ? calcHealthScore(n) : 100, errors: hm?.errors||{}, totalErrors: hm?.totalErrors||0, events: (hm?.events||[]).slice(-20), fixes: hm ? getFixRecommendation(n) : [], connectionHistory: (hm?.connectionHistory||[]).slice(-20), connectedSince: hm?.connectedSince||{} }; }
        res.end(JSON.stringify({ exchanges: h, summary: { avgScore: Math.round(Object.values(h).reduce((a,x)=>a+x.score,0)/Math.max(Object.keys(h).length,1)), critical: Object.entries(h).filter(([,x])=>x.score<50).map(([n])=>n), warnings: Object.entries(h).filter(([,x])=>x.score>=50&&x.score<80).map(([n])=>n) } })); }
    else if (req.url==='/api/submanager') { res.writeHead(200,{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
        res.end(JSON.stringify({ stats: getSubManagerStats(), limits: EXCHANGE_WS_LIMITS, failoverUrls: FAILOVER_URLS })); }
    else if (req.url==='/api/events') { res.writeHead(200,{'Content-Type':'text/event-stream','Cache-Control':'no-cache','Connection':'keep-alive','Access-Control-Allow-Origin':'*'}); sseClients.push(res); req.on('close',()=>{const i=sseClients.indexOf(res);if(i>-1)sseClients.splice(i,1);}); }
    else { res.writeHead(404); res.end('Not found'); }
});

// ═══════════════════════════════════════════════════════════════════
//  ALL 53 EXCHANGE DEFINITIONS — v6 Hybrid 3-Method
// ═══════════════════════════════════════════════════════════════════
const EXCHANGES = [

// ═══ BATCH 1: TIER 1 ═══
{ name:'Binance', tier:1, batch:1, nativeType:'ws', ccxtId:'binance', skipPro:true, ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','PENGU/USDT','PENGU/USDC','WIF/USDT','WIF/USDC','SUI/USDT','SUI/USDC','ENA/USDT','ENA/USDC'],  // skipPro:true (v9.4 — CCXT Pro causes WIF/USDC,BTC/USDC,SUI/USDC connection closures + 56 timeouts → health 70. Native WS covers all 14 pairs with trades+depth5 OB.)
  startNative:()=>connectWS({ name:'Binance',
    urls:['wss://stream.binance.com:9443/stream','wss://stream.binance.com:443/stream','wss://data-stream.binance.vision/stream'],
    restFallbackUrls: ['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','PENGUUSDT','PENGUUSDC','WIFUSDT','WIFUSDC','SUIUSDT','SUIUSDC','ENAUSDT','ENAUSDC'].map(s=>({url:`https://api.binance.com/api/v3/trades?symbol=${s}&limit=5`,parse:r=>{if(Array.isArray(r))addN('Binance',s,r.length,0);}})),
    onOpen:ws=>ws.send(JSON.stringify({method:'SUBSCRIBE',params:['btcusdt@trade','ethusdt@trade','solusdt@trade','btcusdc@trade','ethusdc@trade','solusdc@trade','btcusdt@depth5@100ms','ethusdt@depth5@100ms','solusdt@depth5@100ms','btcusdc@depth5@100ms','ethusdc@depth5@100ms','solusdc@depth5@100ms','penguusdt@trade','penguusdc@trade','wifusdt@trade','wifusdc@trade','suiusdt@trade','suiusdc@trade','enausdt@trade','enausdc@trade','penguusdt@depth5@100ms','penguusdc@depth5@100ms','wifusdt@depth5@100ms','wifusdc@depth5@100ms','suiusdt@depth5@100ms','suiusdc@depth5@100ms','enausdt@depth5@100ms','enausdc@depth5@100ms'],id:1})),
    onMsg:(msg)=>{const d=JSON.parse(msg);if(!d.data)return;
      if(d.data.e==='trade') addN('Binance',d.data.s,1,0,String(d.data.t));
      if(d.data.lastUpdateId&&d.data.bids) addNWithOBValidation('Binance',(d.stream||'').split('@')[0].toUpperCase(),1,d.data.lastUpdateId);}
  })},

{ name:'Coinbase', tier:1, batch:1, nativeType:'ws', ccxtId:'coinbase', ccxtPairs:['BTC/USD','ETH/USD','SOL/USD','BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','PENGU/USDC','PENGU/USD','POPCAT/USDC','POPCAT/USD','WIF/USDC','WIF/USD','SUI/USDC','SUI/USD','ENA/USDC','ENA/USD'],  // +BTC/USDC,ETH/USDC,SOL/USDC (audit 2026-02-28)
  startNative:()=>connectWS({ name:'Coinbase',
    urls:['wss://ws-feed.exchange.coinbase.com','wss://ws-feed.pro.coinbase.com'],
    onOpen:ws=>ws.send(JSON.stringify({type:'subscribe',product_ids:['BTC-USD','ETH-USD','SOL-USD','BTC-USDT','ETH-USDT','SOL-USDT','BTC-USDC','ETH-USDC','SOL-USDC','PENGU-USDC','PENGU-USD','POPCAT-USDC','POPCAT-USD','WIF-USDC','WIF-USD','SUI-USDC','SUI-USD','ENA-USDC','ENA-USD'],channels:['matches','level2_batch','heartbeat']})),
    onMsg:(msg)=>{const d=JSON.parse(msg);if(d.type==='match'||d.type==='last_match')addN('Coinbase',d.product_id,1,0,String(d.trade_id));if(d.type==='l2update'||d.type==='snapshot')addN('Coinbase',d.product_id,0,1);}
  })},

{ name:'Kraken', tier:1, batch:1, nativeType:'ws', ccxtId:'kraken', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USD','ETH/USD','SOL/USD','BTC/USDC','ETH/USDC','SOL/USDC','PENGU/USDT','PENGU/USDC','PENGU/USD','POPCAT/USD','WIF/USD','SUI/USD','ENA/USD'],
  startNative:()=>connectWS({ name:'Kraken',
    urls:['wss://ws.kraken.com/v2','wss://ws-auth.kraken.com/v2'],
    pingMsg:{method:'ping'}, pingInt:25000,
    onOpen:ws=>{const s=['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USD','ETH/USD','SOL/USD','BTC/USDC','ETH/USDC','SOL/USDC','PENGU/USDT','PENGU/USDC','PENGU/USD','POPCAT/USD','WIF/USD','SUI/USD','ENA/USD'];ws.send(JSON.stringify({method:'subscribe',params:{channel:'trade',symbol:s,snapshot:false}}));ws.send(JSON.stringify({method:'subscribe',params:{channel:'book',symbol:s,depth:10,snapshot:true}}));},
    onMsg:(msg)=>{const d=JSON.parse(msg);if(!d.data||!d.channel)return;if(d.channel==='trade')for(const t of d.data)addN('Kraken',t.symbol,1,0);if(d.channel==='book')for(const b of d.data)addN('Kraken',b.symbol,0,1);}
  })},

{ name:'KuCoin', tier:1, batch:1, nativeType:'ws', ccxtId:'kucoin', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','BRETT/USDT','PENGU/USDT','POPCAT/USDT','WIF/USDT','WIF/USDC','SUI/USDT','SUI/USDC','ENA/USDT','ENA/USDC'],
  startNative:async()=>{
    try{const resp=await httpsReq('https://api.kucoin.com/api/v1/bullet-public',{method:'POST'});
    if(!resp?.data?.token){addErr('KuCoin','native','No WS token');return;}
    const ep=resp.data.instanceServers?.[0]?.endpoint||'wss://ws-api-spot.kucoin.com';
    connectWS({name:'KuCoin',url:`${ep}?token=${resp.data.token}`,pingMsg:{id:Date.now(),type:'ping'},pingInt:18000,
      restFallbackUrls:['BTC-USDT','ETH-USDT','SOL-USDT','BRETT-USDT','PENGU-USDT','POPCAT-USDT','WIF-USDT','WIF-USDC','SUI-USDT','SUI-USDC','ENA-USDT','ENA-USDC'].map(s=>({url:`https://api.kucoin.com/api/v1/market/orderbook/level2_20?symbol=${s}`,parse:r=>{if(r?.data?.bids)addN('KuCoin',s,0,1);}})),
      onOpen:ws=>{ws.send(JSON.stringify({id:1,type:'subscribe',topic:'/market/match:BTC-USDT,ETH-USDT,SOL-USDT,BTC-USDC,ETH-USDC,SOL-USDC,BRETT-USDT,PENGU-USDT,POPCAT-USDT,WIF-USDT,WIF-USDC,SUI-USDT,SUI-USDC,ENA-USDT,ENA-USDC',privateChannel:false,response:true}));ws.send(JSON.stringify({id:3,type:'subscribe',topic:'/spotMarket/level2Depth5:BTC-USDT,ETH-USDT,SOL-USDT,BTC-USDC,ETH-USDC,SOL-USDC,BRETT-USDT,PENGU-USDT,POPCAT-USDT,WIF-USDT,WIF-USDC,SUI-USDT,SUI-USDC,ENA-USDT,ENA-USDC',privateChannel:false,response:true}));},
      onMsg:(msg)=>{const d=JSON.parse(msg);if(d.type==='pong'||d.type==='welcome'||d.type==='ack')return;if(!d.topic||!d.data)return;const sym=d.data.symbol||d.topic.split(':').pop()||'';if(d.topic.includes('/market/match'))addN('KuCoin',sym,1,0,d.data.tradeId);if(d.topic.includes('level2Depth5'))addN('KuCoin',sym,0,1);}
    });}catch(e){addErr('KuCoin','native',e.message);}
  }},

{ name:'OKX', tier:1, batch:1, nativeType:'ws', ccxtId:'okx', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','PENGU/USDT','PENGU/USDC','PENGU/USD','WIF/USDT','WIF/USDC','WIF/USD','SUI/USDT','SUI/USDC','SUI/USD','ENA/USDT','ENA/USDC','ENA/USD'],
  startNative:()=>connectWS({ name:'OKX',
    urls:['wss://ws.okx.com:8443/ws/v5/public','wss://wsaws.okx.com:8443/ws/v5/public'],
    pingMsg:'ping', pingInt:25000,
    onOpen:ws=>{const p=['BTC-USDT','ETH-USDT','SOL-USDT','BTC-USDC','ETH-USDC','SOL-USDC','PENGU-USDT','PENGU-USDC','PENGU-USD','WIF-USDT','WIF-USDC','WIF-USD','SUI-USDT','SUI-USDC','SUI-USD','ENA-USDT','ENA-USDC','ENA-USD'];ws.send(JSON.stringify({op:'subscribe',args:p.map(x=>({channel:'trades',instId:x}))}));ws.send(JSON.stringify({op:'subscribe',args:p.map(x=>({channel:'books5',instId:x}))}));},
    onMsg:(msg)=>{if(msg==='pong')return;const d=JSON.parse(msg);if(!d.data||!d.arg)return;if(d.arg.channel==='trades')for(const t of d.data)addN('OKX',d.arg.instId,1,0,t.tradeId);if(d.arg.channel==='books5'){const cs=d.data?.[0]?.checksum;addNWithOBValidation('OKX',d.arg.instId,1,cs);}}
  })},

{ name:'Bybit', tier:1, batch:1, nativeType:'ws', ccxtId:'bybit', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','BRETT/USDT','BRETT/USDC','PENGU/USDT','POPCAT/USDT','WIF/USDT','WIF/USDC','SUI/USDT','SUI/USDC','ENA/USDT','ENA/USDC'],
  startNative:()=>connectWS({ name:'Bybit',
    urls:['wss://stream.bybit.com/v5/public/spot','wss://stream.bytick.com/v5/public/spot','wss://stream.bybit.kz/v5/public/spot'],
    pingMsg:{op:'ping'}, pingInt:20000,
    onOpen:ws=>{
      // Bybit V5 — batch subscriptions with delays to avoid rate-limit drops
      const allPairs=['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','BRETTUSDT','BRETTUSDC','PENGUUSDT','POPCATUSDT','WIFUSDT','WIFUSDC','SUIUSDT','SUIUSDC','ENAUSDT','ENAUSDC'];
      const batchSz=5;
      for(let i=0;i<allPairs.length;i+=batchSz){
        const batch=allPairs.slice(i,i+batchSz);
        setTimeout(()=>{
          if(ws.readyState===1)ws.send(JSON.stringify({op:'subscribe',args:batch.map(x=>`publicTrade.${x}`)}));
        },i/batchSz*500);
      }
      // Orderbook subscriptions after trades are confirmed
      setTimeout(()=>{
        for(let i=0;i<allPairs.length;i+=batchSz){
          const batch=allPairs.slice(i,i+batchSz);
          setTimeout(()=>{
            if(ws.readyState===1)ws.send(JSON.stringify({op:'subscribe',args:batch.map(x=>`orderbook.50.${x}`)}));
          },i/batchSz*500);
        }
      },2000);
    },
    staleTimeout:45000,
    restFallbackUrls:['BTCUSDT','ETHUSDT','SOLUSDT','BRETTUSDT','PENGUUSDT','POPCATUSDT','WIFUSDT','SUIUSDT','ENAUSDT'].map(s=>({url:`https://api.bybit.com/v5/market/recent-trade?category=spot&symbol=${s}&limit=5`,parse:r=>{if(r?.result?.list)addN('Bybit',s,r.result.list.length,0);}})),
    onMsg:(msg)=>{const d=JSON.parse(msg);if(d.op==='subscribe'&&d.success===false){console.error('[Bybit] subscribe failed:',d.ret_msg);return;}if(d.op==='pong'||d.op==='subscribe')return;if(!d.topic||!d.data)return;const sym=d.topic.split('.').pop();if(d.topic.startsWith('publicTrade'))addN('Bybit',sym,Array.isArray(d.data)?d.data.length:1,0,d.data?.[0]?.i);if(d.topic.startsWith('orderbook'))addN('Bybit',sym,0,1);}
  })},

{ name:'Bitfinex', tier:1, batch:1, nativeType:'ws', ccxtId:'bitfinex', skipPro:true, ccxtPairs:['BTC/USD','ETH/USD','SOL/USD','BTC/USDT','ETH/USDT','SOL/USDT','SUI/USDT','SUI/USD','ENA/USDT','ENA/USD'],  // skipPro: typeErr in watchTrades/watchOB (v9.4)
  startNative:()=>{const ch={};connectWS({name:'Bitfinex',
    urls:['wss://api-pub.bitfinex.com/ws/2','wss://api.bitfinex.com/ws/2'],
    pingMsg:JSON.stringify({event:'ping'}), pingInt:25000,
    subscriptions:['tBTCUSD','tETHUSD','tSOLUSD','tBTCUST','tETHUST','tSOLUST'].flatMap(s=>[JSON.stringify({event:'subscribe',channel:'trades',symbol:s}),JSON.stringify({event:'subscribe',channel:'book',symbol:s,prec:'P0',len:25})]),
    onOpen:ws=>{for(const s of['tBTCUSD','tETHUSD','tSOLUSD','tBTCUST','tETHUST','tSOLUST','tSUIUST','tSUIUSD','tENAUST','tENAUSD']){ws.send(JSON.stringify({event:'subscribe',channel:'trades',symbol:s}));ws.send(JSON.stringify({event:'subscribe',channel:'book',symbol:s,prec:'P0',len:25}));}},
    staleTimeout:60000,
    onMsg:(msg)=>{try{const d=JSON.parse(msg);if(d.event==='subscribed'){ch[d.chanId]={channel:d.channel,symbol:d.symbol||d.pair};return;}if(!Array.isArray(d)||d.length<2)return;const info=ch[d[0]];if(!info)return;if(d[1]==='hb')return;const sym=info.symbol.startsWith('t')?info.symbol:`t${info.symbol}`;if(info.channel==='trades'&&(d[1]==='te'||d[1]==='tu'))addN('Bitfinex',sym,1,0);if(info.channel==='book')addN('Bitfinex',sym,0,1);}catch(e){/* sanitize: ignore malformed Bitfinex msgs */}}
  });}},

{ name:'Gate.io', tier:1, batch:1, nativeType:'ws', ccxtId:'gateio', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','BRETT/USDT','PENGU/USDT','POPCAT/USDT','WIF/USDT','WIF/USDC','SUI/USDT','SUI/USDC','ENA/USDT','ENA/USDC'],
  startNative:()=>connectWS({ name:'Gate.io',
    urls:['wss://api.gateio.ws/ws/v4/','wss://fx-ws.gateio.ws/v4/ws/usdt'],
    pingMsg:{time:Math.floor(Date.now()/1000),channel:'spot.ping'}, pingInt:15000,
    restFallbackUrls:['BTC_USDT','ETH_USDT','SOL_USDT','BRETT_USDT','PENGU_USDT','POPCAT_USDT','WIF_USDT','SUI_USDT','ENA_USDT'].map(s=>({url:`https://api.gateio.ws/api/v4/spot/trades?currency_pair=${s}&limit=5`,parse:r=>{if(Array.isArray(r))addN('Gate.io',s,r.length,0);}})),
    subscriptions:(()=>{const t=Math.floor(Date.now()/1000);const p=['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','SOL_USDC','BRETT_USDT','PENGU_USDT','POPCAT_USDT','WIF_USDT','WIF_USDC','SUI_USDT','SUI_USDC','ENA_USDT','ENA_USDC'];return[{time:t,channel:'spot.trades',event:'subscribe',payload:p},...p.map(s=>({time:t,channel:'spot.order_book',event:'subscribe',payload:[s,'5','1000ms']}))];})(),
    onOpen:ws=>{const t=Math.floor(Date.now()/1000);const p=['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','SOL_USDC','BRETT_USDT','PENGU_USDT','POPCAT_USDT','WIF_USDT','WIF_USDC','SUI_USDT','SUI_USDC','ENA_USDT','ENA_USDC'];ws.send(JSON.stringify({time:t,channel:'spot.trades',event:'subscribe',payload:p}));for(const s of p)ws.send(JSON.stringify({time:t,channel:'spot.order_book',event:'subscribe',payload:[s,'5','1000ms']}));},
    staleTimeout:45000,
    onMsg:(msg)=>{const d=JSON.parse(msg);if(!d.result||d.event==='subscribe')return;if(d.channel==='spot.trades')addN('Gate.io',d.result.currency_pair,1,0,String(d.result.id));if(d.channel==='spot.order_book'&&d.result.bids)addN('Gate.io',d.result.s||d.result.currency_pair,0,1);}
  })},

{ name:'HTX', tier:1, batch:1, nativeType:'ws', ccxtId:'htx', skipPro:true, ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','BRETT/USDT','PENGU/USDT','POPCAT/USDT','WIF/USDT','SUI/USDT','ENA/USDT'],  // -SOL/USDC (delisted); skipPro: connection closed PENGU/WIF/ENA (v9.4)
  startNative:()=>connectWS({ name:'HTX',
    urls:['wss://api.huobi.pro/ws','wss://api-aws.huobi.pro/ws'],
    compression:'gzip',
    restFallbackUrls:['btcusdt','ethusdt','solusdt','brettusdt','penguusdt','popcatusdt','wifusdt','suiusdt','enausdt'].map(s=>({url:`https://api.huobi.pro/market/trade?symbol=${s}`,parse:r=>{if(r?.tick?.data)addN('HTX',s,r.tick.data.length,0);}})),
    subscriptions:['btcusdt','ethusdt','solusdt','btcusdc','ethusdc','brettusdt','penguusdt','popcatusdt','wifusdt','suiusdt','enausdt'].flatMap(s=>[{sub:`market.${s}.trade.detail`,id:s+'t'},{sub:`market.${s}.depth.step0`,id:s+'d'}]),
    onOpen:ws=>{for(const s of['btcusdt','ethusdt','solusdt','btcusdc','ethusdc','brettusdt','penguusdt','popcatusdt','wifusdt','suiusdt','enausdt']){ws.send(JSON.stringify({sub:`market.${s}.trade.detail`,id:s+'t'}));ws.send(JSON.stringify({sub:`market.${s}.depth.step0`,id:s+'d'}));}},  // removed solusdc (delisted)
    staleTimeout:45000,
    onMsg:(msg,ws)=>{const d=JSON.parse(msg);if(d.ping){ws.send(JSON.stringify({pong:d.ping}));return;}if(d.subbed)return;const ch=d.ch||'';const parts=ch.split('.');if(parts.length<3)return;const sym=parts[1];if(ch.includes('.trade.detail')&&d.tick?.data)addN('HTX',sym,d.tick.data.length,0);if(ch.includes('.depth')&&d.tick)addN('HTX',sym,0,1);}
  })},

{ name:'WOO X', tier:1, batch:1, nativeType:'ws', ccxtId:'woo', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','BRETT/USDT','PENGU/USDT','POPCAT/USDT','WIF/USDT','SUI/USDT','ENA/USDT'],  // -SOL/USDC (delisted, audit 2026-02-28)
  startNative:()=>connectWS({ name:'WOO X',
    urls:['wss://wss.woo.org/ws/stream/OqdphuyCtYWxwzhxyLLjOWNdFP7sQt8RPWzmb5xY','wss://wss.woo.org/ws/stream/public'],
    pingMsg:{event:'ping'}, pingInt:9000,
    subscriptions:['SPOT_BTC_USDT','SPOT_ETH_USDT','SPOT_SOL_USDT','SPOT_BTC_USDC','SPOT_ETH_USDC','SPOT_BRETT_USDT','SPOT_PENGU_USDT','SPOT_POPCAT_USDT','SPOT_WIF_USDT','SPOT_SUI_USDT','SPOT_ENA_USDT'].flatMap(s=>[{id:s,event:'subscribe',topic:`${s}@trade`},{id:s+'o',event:'subscribe',topic:`${s}@orderbook`}]),
    onOpen:ws=>{for(const s of['SPOT_BTC_USDT','SPOT_ETH_USDT','SPOT_SOL_USDT','SPOT_BTC_USDC','SPOT_ETH_USDC','SPOT_BRETT_USDT','SPOT_PENGU_USDT','SPOT_POPCAT_USDT','SPOT_WIF_USDT','SPOT_SUI_USDT','SPOT_ENA_USDT']){ws.send(JSON.stringify({id:s,event:'subscribe',topic:`${s}@trade`}));ws.send(JSON.stringify({id:s+'o',event:'subscribe',topic:`${s}@orderbook`}));}},  // removed SPOT_SOL_USDC (delisted)
    staleTimeout:45000,
    onMsg:(msg)=>{const d=JSON.parse(msg);if(d.event==='pong'||d.event==='subscribe')return;const t=d.topic||'';const sym=t.split('@')[0];if(!sym.startsWith('SPOT_'))return;if(t.includes('@trade')&&d.data)addN('WOO X',sym,1,0);if(t.includes('@orderbook')&&d.data)addN('WOO X',sym,0,1);}
  })},

// ═══ BATCH 2: TIER 2 ═══
{ name:'Crypto.com', tier:2, batch:2, nativeType:'ws', ccxtId:'cryptocom', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USD','ETH/USD','SOL/USD','PENGU/USDT','PENGU/USD','POPCAT/USD','WIF/USDT','WIF/USD','SUI/USDT','SUI/USD','ENA/USDT','ENA/USD'],
  startNative:()=>connectWS({ name:'Crypto.com',
    urls:['wss://stream.crypto.com/exchange/v1/market','wss://stream.crypto.com/v2/market'],
    pingMsg:{id:0,method:'public/heartbeat'}, pingInt:25000,
    onOpen:ws=>{/* 1s delay after connect per Crypto.com docs to prevent ECONNRESET */setTimeout(()=>{if(ws.readyState!==1)return;const p=['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USD','ETH_USD','SOL_USD','BTC_USDC','ETH_USDC','SOL_USDC','PENGU_USDT','PENGU_USD','POPCAT_USD','WIF_USDT','WIF_USD','SUI_USDT','SUI_USD','ENA_USDT','ENA_USD'];ws.send(JSON.stringify({id:1,method:'subscribe',params:{channels:p.flatMap(x=>[`trade.${x}`,`book.${x}.10`])}}));},1000);},
    onMsg:(msg)=>{const d=JSON.parse(msg);
      if(d.method==='public/heartbeat'){return;}
      if(!d.result)return;const ch=d.result.channel||'';const sym=d.result.instrument_name||ch.split('.')[1]||'';if(ch.startsWith('trade')&&d.result.data)addN('Crypto.com',sym,d.result.data.length,0);if(ch.startsWith('book')&&d.result.data)addN('Crypto.com',sym,0,1);}
  })},

{ name:'Bitstamp', tier:2, batch:2, nativeType:'ws', ccxtId:'bitstamp', ccxtPairs:['BTC/USD','ETH/USD','SOL/USD','BTC/USDT','ETH/USDT','BTC/USDC','ETH/USDC','SOL/USDC','PENGU/USD','POPCAT/USD','WIF/USD','SUI/USD','ENA/USD'],  // -SOL/USDT (audit 2026-02-28)
  startNative:()=>connectWS({ name:'Bitstamp',
    urls:['wss://ws.bitstamp.net'],
    pingMsg:JSON.stringify({event:'bts:heartbeat'}), pingInt:20000,
    restFallbackUrls:[{url:'https://www.bitstamp.net/api/v2/transactions/btcusd/',parse:r=>{if(Array.isArray(r))addN('Bitstamp','btcusd',Math.min(r.length,5),0);}}],
    subscriptions:['btcusd','ethusd','solusd','btcusdt','ethusdt','btcusdc','ethusdc','solusdc','penguusd','popcatusd','wifusd','suiusd','enausd'].flatMap(s=>[JSON.stringify({event:'bts:subscribe',data:{channel:`live_trades_${s}`}}),JSON.stringify({event:'bts:subscribe',data:{channel:`order_book_${s}`}})]),
    onOpen:ws=>{for(const s of['btcusd','ethusd','solusd','btcusdt','ethusdt','btcusdc','ethusdc','solusdc','penguusd','popcatusd','wifusd','suiusd','enausd']){ws.send(JSON.stringify({event:'bts:subscribe',data:{channel:`live_trades_${s}`}}));ws.send(JSON.stringify({event:'bts:subscribe',data:{channel:`order_book_${s}`}}));}},  // -solusdt (not on Bitstamp)
    onMsg:(msg)=>{const d=JSON.parse(msg);if(!d.channel)return;
      if(d.channel.startsWith('live_trades_')&&d.event==='trade'&&d.data){const sym=d.channel.replace('live_trades_','');addN('Bitstamp',sym,1,0,String(d.data.id));}
      if(d.channel.startsWith('order_book_')&&d.data?.bids)addN('Bitstamp',d.channel.replace('order_book_',''),0,1);}
  })},

{ name:'WhiteBIT', tier:2, batch:2, nativeType:'ws', ccxtId:'whitebit', skipPro:true, ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','BTC/USD','ETH/USD','SOL/USD','PENGU/USDT','PENGU/USDC','WIF/USDT','WIF/USDC','SUI/USDT','SUI/USDC','ENA/USDT','ENA/USDC'],  // skipPro: connection closed SOL/SUI/WIF (v9.4)
  startNative:()=>connectWS({ name:'WhiteBIT',
    urls:['wss://api.whitebit.com/ws'],
    restFallbackUrls:['BTC_USDT','ETH_USDT','SOL_USDT'].map(s=>({url:`https://whitebit.com/api/v4/public/trades/${s}`,parse:r=>{if(Array.isArray(r)&&r.length)addN('WhiteBIT',s,Math.min(r.length,5),0);}})),
    pingMsg:{id:0,method:'server.ping',params:[]}, pingInt:25000,
    subscriptions:[{id:1,method:'trades_subscribe',params:['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','SOL_USDC','BTC_USD','ETH_USD','SOL_USD','PENGU_USDT','PENGU_USDC','WIF_USDT','WIF_USDC','SUI_USDT','SUI_USDC','ENA_USDT','ENA_USDC']},...['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','SOL_USDC','BTC_USD','ETH_USD','SOL_USD','PENGU_USDT','PENGU_USDC','WIF_USDT','WIF_USDC','SUI_USDT','SUI_USDC','ENA_USDT','ENA_USDC'].map(s=>({id:2,method:'depth_subscribe',params:[s,100,'0',true]}))],
    onOpen:ws=>{ws.send(JSON.stringify({id:1,method:'trades_subscribe',params:['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','SOL_USDC','BTC_USD','ETH_USD','SOL_USD','PENGU_USDT','PENGU_USDC','WIF_USDT','WIF_USDC','SUI_USDT','SUI_USDC','ENA_USDT','ENA_USDC']}));for(const s of['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','SOL_USDC','BTC_USD','ETH_USD','SOL_USD','PENGU_USDT','PENGU_USDC','WIF_USDT','WIF_USDC','SUI_USDT','SUI_USDC','ENA_USDT','ENA_USDC'])ws.send(JSON.stringify({id:2,method:'depth_subscribe',params:[s,100,'0',true]}));},
    staleTimeout:45000,
    onMsg:(msg)=>{const d=JSON.parse(msg);if(d.method==='trades_update'&&d.params)addN('WhiteBIT',d.params[0],d.params[1]?.length||1,0);if(d.method==='depth_update'&&d.params)addN('WhiteBIT',d.params[2]||'',0,1);}
  })},

{ name:'AscendEX', tier:2, batch:2, nativeType:'ws', ccxtId:'ascendex', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USD','ETH/USD','SOL/USD','BRETT/USDT','PENGU/USDT','WIF/USDT','SUI/USDT','ENA/USDT'],
  startNative:()=>connectWS({ name:'AscendEX',
    urls:['wss://ascendex.com/1/api/pro/v1/stream'],
    pingMsg:{op:'ping'}, pingInt:15000,
    subscriptions:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','BTC/USD','ETH/USD','SOL/USD','BRETT/USDT','PENGU/USDT','WIF/USDT','SUI/USDT','ENA/USDT'].flatMap(s=>[{op:'sub',ch:`trades:${s}`},{op:'sub',ch:`depth:${s}`}]),
    onOpen:ws=>{for(const s of['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','BTC/USD','ETH/USD','SOL/USD','BRETT/USDT','PENGU/USDT','WIF/USDT','SUI/USDT','ENA/USDT']){ws.send(JSON.stringify({op:'sub',ch:`trades:${s}`}));ws.send(JSON.stringify({op:'sub',ch:`depth:${s}`}));}},
    staleTimeout:45000,
    onMsg:(msg,ws)=>{const d=JSON.parse(msg);if(d.m==='ping'){ws.send(JSON.stringify({op:'pong'}));return;}if(d.m==='trades'&&d.data&&d.symbol)addN('AscendEX',d.symbol,Array.isArray(d.data)?d.data.length:1,0);if((d.m==='depth'||d.m==='depth-snapshot')&&d.symbol)addN('AscendEX',d.symbol,0,1);}
  })},

{ name:'BingX', tier:2, batch:2, nativeType:'ws', ccxtId:'bingx', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','BRETT/USDT','PENGU/USDT','POPCAT/USDT','WIF/USDT','SUI/USDT','SUI/USDC','ENA/USDT'],
  startNative:()=>connectWS({ name:'BingX',
    urls:['wss://open-api-ws.bingx.com/market','wss://open-api-ws.bingx.com/market/v2'],
    compression:'gzip', pingMsg:'Pong', pingInt:5000,
    subscriptions:['BTC-USDT','ETH-USDT','SOL-USDT','BTC-USDC','ETH-USDC','SOL-USDC','BRETT-USDT','PENGU-USDT','POPCAT-USDT','WIF-USDT','SUI-USDT','SUI-USDC','ENA-USDT'].flatMap(p=>[{id:p,reqType:'sub',dataType:`${p}@trade`},{id:p+'d',reqType:'sub',dataType:`${p}@depth5`}]),
    onOpen:ws=>{for(const p of['BTC-USDT','ETH-USDT','SOL-USDT','BTC-USDC','ETH-USDC','SOL-USDC','BRETT-USDT','PENGU-USDT','POPCAT-USDT','WIF-USDT','SUI-USDT','SUI-USDC','ENA-USDT']){ws.send(JSON.stringify({id:p,reqType:'sub',dataType:`${p}@trade`}));ws.send(JSON.stringify({id:p+'d',reqType:'sub',dataType:`${p}@depth5`}));}},
    staleTimeout:45000,
    onMsg:(msg)=>{if(msg==='Ping')return;const d=JSON.parse(msg);if(!d.dataType)return;const sym=d.dataType.split('@')[0];if(d.dataType.includes('@trade'))addN('BingX',sym,1,0);if(d.dataType.includes('@depth'))addN('BingX',sym,0,1);}
  })},

{ name:'Toobit', tier:2, batch:2, nativeType:'ws', ccxtId:'toobit', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','PENGU/USDT','POPCAT/USDT','WIF/USDT','SUI/USDT','ENA/USDT'],  // -BRETT/USDT (not on Toobit, audit 2026-02-28)
  startNative:()=>connectWS({ name:'Toobit',
    urls:['wss://stream.toobit.com/quote/ws/v1','wss://stream.toobit.com/quote/ws/v2'],
    customPing:ws=>{try{ws.send(JSON.stringify({ping:Date.now()}));}catch{}}, customPingInt:15000,
    subscriptions:['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','PENGUUSDT','POPCATUSDT','WIFUSDT','SUIUSDT','ENAUSDT'].flatMap(s=>[{symbol:s,topic:'trade',event:'sub',params:{binary:false}},{symbol:s,topic:'depth',event:'sub',params:{binary:false}}]),
    onOpen:ws=>{for(const s of['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','PENGUUSDT','POPCATUSDT','WIFUSDT','SUIUSDT','ENAUSDT']){ws.send(JSON.stringify({symbol:s,topic:'trade',event:'sub',params:{binary:false}}));ws.send(JSON.stringify({symbol:s,topic:'depth',event:'sub',params:{binary:false}}));}},  // removed BRETTUSDT (not listed)
    staleTimeout:45000,
    onMsg:(msg)=>{const d=JSON.parse(msg);if(d.pong||d.ping)return;const sym=d.symbol||d.symbolName||'';if(d.topic==='trade'&&d.data)addN('Toobit',sym,d.data.length||1,0);if(d.topic==='depth'&&d.data)addN('Toobit',sym,0,d.data.length||1);}
  })},

{ name:'Deepcoin', tier:2, batch:2, nativeType:'ws', ccxtId:'deepcoin', skipPro:true, ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','PENGU/USDT','WIF/USDT'],  // -BTC/USDC,ETH/USDC,SOL/USDC (delisted from Deepcoin, audit 2026-02-28)
  startNative:()=>connectWS({ name:'Deepcoin',
    urls:['wss://stream.deepcoin.com/streamlet/trade/public/spot?platform=api'],
    pingMsg:'ping', pingInt:15000,
    subscriptions:(()=>{const reqId=Date.now();return['BTCUSDT','ETHUSDT','SOLUSDT','PENGUUSDT','WIFUSDT'].map((s,i)=>({SendTopicAction:{Action:'1',FilterValue:`DeepCoin_${s}`,LocalNo:reqId+i,ResumeNo:-2,TopicID:'2'}}));})(),
    onOpen:ws=>{const reqId=Date.now();
      ['BTCUSDT','ETHUSDT','SOLUSDT','PENGUUSDT','WIFUSDT'].forEach((s,i)=>ws.send(JSON.stringify({SendTopicAction:{Action:'1',FilterValue:`DeepCoin_${s}`,LocalNo:reqId+i,ResumeNo:-2,TopicID:'2'}})));},
    staleTimeout:45000,
    restPoll:(name)=>{const poll=async()=>{if(stopFlag)return;for(const sym of['BTC-USDT','ETH-USDT','SOL-USDT','BTC-USD','PENGU-USDT','WIF-USDT']){try{const r=await httpsReq(`https://api.deepcoin.com/deepcoin/market/books?instId=${sym}&sz=5`);if(r?.code==='0'&&r.data)addN('Deepcoin',sym,0,1);}catch{}}if(!stopFlag)setTimeout(poll,10000);};poll();},  // removed USDC pairs (delisted)
    onMsg:(msg)=>{if(msg==='pong')return;const d=JSON.parse(msg);if(d.a==='PMT'&&d.r&&Array.isArray(d.r))for(const row of d.r){const inst=row.d?.I||'';addN('Deepcoin',inst.replace('DeepCoin_',''),1,0);}}
  })},

{ name:'XT.com', tier:2, batch:2, nativeType:'ws', ccxtId:'xt', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','BRETT/USDT','PENGU/USDT','POPCAT/USDT','WIF/USDT','SUI/USDT','SUI/USDC','ENA/USDT'],
  startNative:()=>connectWS({ name:'XT.com',
    urls:['wss://stream.xt.com/public','wss://stream2.xt.com/public','wss://stream3.xt.com/public'],
    pingMsg:'ping', pingInt:15000,
    staleTimeout:45000,
    restFallbackUrls:['btc_usdt','eth_usdt','sol_usdt','brett_usdt','pengu_usdt','popcat_usdt','wif_usdt','sui_usdt','ena_usdt'].map(s=>({url:`https://sapi.xt.com/v4/public/trade/recent?symbol=${s}&limit=5`,parse:r=>{if(r?.result)addN('XT.com',s,Array.isArray(r.result)?r.result.length:1,0);}})),
    onOpen:ws=>{
      const pairs=['btc_usdt','eth_usdt','sol_usdt','btc_usdc','eth_usdc','sol_usdc','brett_usdt','pengu_usdt','popcat_usdt','wif_usdt','sui_usdt','sui_usdc','ena_usdt'];
      const tradeChannels=pairs.map(s=>`trade@${s}`);
      const depthChannels=pairs.map(s=>`depth@${s},5`);
      ws.send(JSON.stringify({method:'subscribe',params:tradeChannels,id:'xt_trades'}));
      ws.send(JSON.stringify({method:'subscribe',params:depthChannels,id:'xt_depth'}));
    },
    onMsg:(msg)=>{if(msg==='pong')return;try{const d=JSON.parse(msg);
      if(d.code!==undefined)return;
      const topic=d.topic||'';
      if(!topic)return;
      const event=d.event||'';
      const sym=event.includes('@')?event.split('@')[1]?.split(',')[0]:'';
      if(!sym)return;
      if(topic==='trade'&&d.data)addN('XT.com',sym,1,0,String(d.data?.i||''));
      if((topic==='depth'||topic==='depth_update')&&d.data)addN('XT.com',sym,0,1);
    }catch{}}
  })},

{ name:'Zoomex', tier:2, batch:2, nativeType:'ws', ccxtId:null, ccxtPairs:[],
  startNative:()=>connectWS({ name:'Zoomex',
    urls:['wss://stream.zoomex.com/v5/public/spot'],
    pingMsg:{op:'ping'}, pingInt:20000,
    staleTimeout:45000,
    onOpen:ws=>{
      // Zoomex V5 — batch subscriptions with delays
      const pairs=['BTCUSDT','ETHUSDT','SOLUSDT','BRETTUSDT','PENGUUSDT','POPCATUSDT','WIFUSDT','SUIUSDT','ENAUSDT'];
      const b1=pairs.slice(0,5),b2=pairs.slice(5);
      ws.send(JSON.stringify({op:'subscribe',args:b1.map(x=>`publicTrade.${x}`)}));
      setTimeout(()=>{if(ws.readyState===1)ws.send(JSON.stringify({op:'subscribe',args:b2.map(x=>`publicTrade.${x}`)}));},500);
      setTimeout(()=>{if(ws.readyState===1)ws.send(JSON.stringify({op:'subscribe',args:b1.map(x=>`orderbook.50.${x}`)}));},1200);
      setTimeout(()=>{if(ws.readyState===1)ws.send(JSON.stringify({op:'subscribe',args:b2.map(x=>`orderbook.50.${x}`)}));},1800);
    },
    // REST fallback disabled: api.zoomex.com DNS unreachable (verified 2026-02-27); WS stream.zoomex.com works fine
    restFallbackUrls:[],  // was: api.zoomex.com (ENOTFOUND)
    onMsg:(msg)=>{const d=JSON.parse(msg);if(d.op==='pong'||d.op==='subscribe')return;if(!d.topic||!d.data)return;const sym=d.topic.split('.').pop();if(d.topic.startsWith('publicTrade'))addN('Zoomex',sym,Array.isArray(d.data)?d.data.length:1,0);if(d.topic.startsWith('orderbook'))addN('Zoomex',sym,0,1);}
  })},

{ name:'Bitget', tier:2, batch:2, nativeType:'ws', ccxtId:'bitget', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','BRETT/USDT','PENGU/USDT','WIF/USDT','SUI/USDT','SUI/USDC','ENA/USDT'],
  startNative:()=>connectWS({ name:'Bitget',
    urls:['wss://ws.bitget.com/v2/ws/public'],
    customPing:ws=>ws.send('ping'), customPingInt:30000,
    onOpen:ws=>ws.send(JSON.stringify({op:'subscribe',args:['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','BRETTUSDT','PENGUUSDT','POPCATUSDT','WIFUSDT','SUIUSDT','ENAUSDT'].flatMap(s=>[{instType:'SPOT',channel:'trade',instId:s},{instType:'SPOT',channel:'books5',instId:s}])})),
    onMsg:(msg)=>{if(msg==='pong')return;const d=JSON.parse(msg);if(d.event||d.op==='pong')return;const sym=d.arg?.instId;if(!sym)return;if(d.arg.channel==='trade'&&d.data)addN('Bitget',sym,d.data.length,0);if(d.arg.channel==='books5'&&d.data)addN('Bitget',sym,0,d.data.length);}
  })},

// ═══ BATCH 3: TIER 2 CONT + TIER 3 ═══
{ name:'Gemini', tier:2, batch:3, nativeType:'ws', ccxtId:'gemini', skipPro:true, ccxtPairs:['BTC/USD','ETH/USD','SOL/USD','BTC/USDT','ETH/USDT','BTC/USDC','ETH/USDC','SOL/USDC','PENGU/USDC','PENGU/USD','POPCAT/USDC','POPCAT/USD','WIF/USDC','WIF/USD'],  // skipPro: watchTicker notSupported (v9.4)
  startNative:()=>connectWS({ name:'Gemini',
    urls:['wss://api.gemini.com/v2/marketdata'],
    wsHeaders:{'Origin':'https://exchange.gemini.com'},  // Origin required for Gemini WS handshake (v9.4)
    staleTimeout:90000,  // Gemini sends heartbeats every 5s — allow longer stale window
    onOpen:ws=>ws.send(JSON.stringify({type:'subscribe',subscriptions:[{name:'l2',symbols:['BTCUSD','ETHUSD','SOLUSD','BTCUSDT','ETHUSDT','BTCUSDC','ETHUSDC','SOLUSDC','PENGUUSDC','PENGUUSD','POPCATUSDC','POPCATUSD','WIFUSDC','WIFUSD']}]})),
    onMsg:(msg)=>{const d=JSON.parse(msg);if(d.type==='heartbeat')return;if(d.type==='l2_updates'&&d.symbol){if(d.trades&&d.trades.length)addN('Gemini',d.symbol,d.trades.length,0);if(d.changes&&d.changes.length)addN('Gemini',d.symbol,0,1);}}
  })},

{ name:'Binance.US', tier:2, batch:3, nativeType:'ws', ccxtId:'binanceus', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','BTC/USD','ETH/USD','SOL/USD','BRETT/USDT','PENGU/USDT','POPCAT/USDT','WIF/USDT','SUI/USDT','SUI/USD','ENA/USDT'],
  startNative:()=>connectWS({ name:'Binance.US',
    urls:['wss://stream.binance.us:9443/stream'],
    onOpen:ws=>ws.send(JSON.stringify({method:'SUBSCRIBE',params:['btcusdt@trade','ethusdt@trade','solusdt@trade','btcusdc@trade','ethusdc@trade','solusdc@trade','btcusd@trade','ethusd@trade','solusd@trade','btcusdt@depth5@1000ms','ethusdt@depth5@1000ms','solusdt@depth5@1000ms','btcusdc@depth5@1000ms','ethusdc@depth5@1000ms','solusdc@depth5@1000ms','btcusd@depth5@1000ms','ethusd@depth5@1000ms','solusd@depth5@1000ms','brettusdt@trade','penguusdt@trade','popcatusdt@trade','wifusdt@trade','suiusdt@trade','suiusd@trade','enausdt@trade','brettusdt@depth5@1000ms','penguusdt@depth5@1000ms','popcatusdt@depth5@1000ms','wifusdt@depth5@1000ms','suiusdt@depth5@1000ms','suiusd@depth5@1000ms','enausdt@depth5@1000ms'],id:1})),
    onMsg:(msg)=>{const d=JSON.parse(msg);if(!d.data)return;if(d.data.e==='trade')addN('Binance.US',d.data.s?.toLowerCase(),1,0,String(d.data.t));if(d.data.lastUpdateId)addNWithOBValidation('Binance.US',(d.stream||'').split('@')[0],1,d.data.lastUpdateId);}
  })},

{ name:'MEXC', tier:2, batch:3, nativeType:'rest', ccxtId:'mexc', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','PENGU/USDT','PENGU/USDC','POPCAT/USDT','POPCAT/USDC','WIF/USDT','WIF/USDC','SUI/USDT','SUI/USDC','ENA/USDT','ENA/USDC'],
  startNative:()=>runREST('MEXC',['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','BRETTUSDT','PENGUUSDT','POPCATUSDT','WIFUSDT','SUIUSDT','ENAUSDT'].flatMap(s=>[
    {url:`https://api.mexc.com/api/v3/trades?symbol=${s}&limit=20`,parse:r=>{if(Array.isArray(r))addN('MEXC',s,r.length,0);}},
    {url:`https://api.mexc.com/api/v3/depth?symbol=${s}&limit=5`,parse:r=>{if(r?.bids?.length)addN('MEXC',s,0,1);}}
  ]))},

{ name:'CoinEx', tier:3, batch:3, nativeType:'ws', ccxtId:'coinex', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','BRETT/USDT','PENGU/USDT','POPCAT/USDT','WIF/USDT','WIF/USDC','SUI/USDT','SUI/USDC','ENA/USDT','ENA/USDC'],
  startNative:()=>connectWS({ name:'CoinEx',
    urls:['wss://socket.coinex.com/v2/spot'],
    compression:'gzip', pingMsg:{method:'server.ping',params:{},id:1}, pingInt:15000,
    onOpen:ws=>{const p=['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','BRETTUSDT','PENGUUSDT','POPCATUSDT','WIFUSDT','SUIUSDT','ENAUSDT'];ws.send(JSON.stringify({method:'deals.subscribe',params:{market_list:p},id:2}));ws.send(JSON.stringify({method:'depth.subscribe',params:{market_list:p.map(m=>[m,5,'0',false])},id:3}));},
    onMsg:(msg,ws)=>{const d=JSON.parse(msg);if(d.method==='server.ping'){ws.send(JSON.stringify({method:'server.pong',params:{},id:d.id||1}));return;}if(!d.method)return;if(d.method==='deals.update'&&d.data)addN('CoinEx',d.data.market,d.data.deal_list?.length||1,0);if(d.method==='depth.update'&&d.data)addN('CoinEx',d.data.market,0,1);}
  })},

{ name:'LBank', tier:3, batch:3, nativeType:'ws', ccxtId:'lbank', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','BRETT/USDT','PENGU/USDT','POPCAT/USDT','WIF/USDT','SUI/USDT','ENA/USDT'],
  startNative:()=>connectWS({ name:'LBank',
    urls:['wss://www.lbkex.net/ws/V2/','wss://www.lbkex.com/ws/V2/'],
    restFallbackUrls:['btc_usdt','eth_usdt','sol_usdt','brett_usdt','pengu_usdt','popcat_usdt','wif_usdt','sui_usdt','ena_usdt'].map(s=>({url:`https://api.lbkex.com/v2/trades.do?symbol=${s}&size=5`,parse:r=>{if(r?.data&&Array.isArray(r.data))addN('LBank',s,r.data.length,0);}})),
    subscriptions:['btc_usdt','eth_usdt','sol_usdt','btc_usdc','eth_usdc','sol_usdc','brett_usdt','pengu_usdt','popcat_usdt','wif_usdt','sui_usdt','ena_usdt'].flatMap(s=>[{action:'subscribe',subscribe:'trade',pair:s},{action:'subscribe',subscribe:'depth',pair:s,depth:10}]),
    onOpen:ws=>{for(const s of['btc_usdt','eth_usdt','sol_usdt','btc_usdc','eth_usdc','sol_usdc','brett_usdt','pengu_usdt','popcat_usdt','wif_usdt','sui_usdt','ena_usdt']){ws.send(JSON.stringify({action:'subscribe',subscribe:'trade',pair:s}));ws.send(JSON.stringify({action:'subscribe',subscribe:'depth',pair:s,depth:10}));}},
    staleTimeout:45000,
    onMsg:(msg,ws)=>{const d=JSON.parse(msg);
      if(d.action==='ping'){ws.send(JSON.stringify({action:'pong',pong:d.ping}));return;}
      const sym=d.pair||'';if(d.trade)addN('LBank',sym,1,0);if(d.type==='trade'&&d.trade)addN('LBank',sym,1,0);if(d.depth)addN('LBank',sym,0,1);if(d.type==='depth'&&d.depth)addN('LBank',sym,0,1);}
  })},

{ name:'BitMart', tier:3, batch:3, nativeType:'ws', ccxtId:'bitmart', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','BRETT/USDT','PENGU/USDT','POPCAT/USDT','WIF/USDT','SUI/USDT','ENA/USDT'],
  startNative:()=>connectWS({ name:'BitMart',
    urls:['wss://ws-manager-compress.bitmart.com/api?protocol=1.1'],
    compression:'inflate', pingMsg:'ping', pingInt:10000,
    onOpen:ws=>ws.send(JSON.stringify({op:'subscribe',args:['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','SOL_USDC','BRETT_USDT','PENGU_USDT','POPCAT_USDT','WIF_USDT','SUI_USDT','ENA_USDT'].flatMap(p=>[`spot/trade:${p}`,`spot/depth5:${p}`])})),
    onMsg:(msg)=>{if(msg==='pong')return;const d=JSON.parse(msg);if(!d.data)return;if(d.table==='spot/trade'&&Array.isArray(d.data))for(const t of d.data)addN('BitMart',t.symbol,1,0);if(d.table==='spot/depth5'&&Array.isArray(d.data))for(const b of d.data)addN('BitMart',b.symbol,0,1);}
  })},

{ name:'Pionex', tier:3, batch:3, nativeType:'ws', ccxtId:null, ccxtPairs:[],
  startNative:()=>connectWS({ name:'Pionex',
    urls:['wss://ws.pionex.com/wsPub'],  // removed ws2.pionex.com (DNS ENOTFOUND)
    onOpen:ws=>{const pairs=['BTC_USDT','ETH_USDT','SOL_USDT','PENGU_USDT','WIF_USDT','SUI_USDT','ENA_USDT'];pairs.forEach((s,i)=>{setTimeout(()=>{ws.send(JSON.stringify({op:'SUBSCRIBE',topic:'TRADE',symbol:s}));ws.send(JSON.stringify({op:'SUBSCRIBE',topic:'DEPTH',symbol:s,limit:5}));},i*200);});},  // Expanded: +WIF,SUI,ENA; removed BRETT,POPCAT (NOT_LISTED on Pionex). Staggered 200ms delay per pair to avoid rejection
    onMsg:(msg,ws)=>{const d=JSON.parse(msg);if(d.op==='PING'){ws.send(JSON.stringify({op:'PONG'}));return;}if(!d.topic)return;const sym=d.symbol||'';if(d.topic==='TRADE'&&d.data)addN('Pionex',sym,Array.isArray(d.data)?d.data.length:1,0);if(d.topic==='DEPTH'&&d.data)addN('Pionex',sym,0,1);}
  })},

{ name:'Poloniex', tier:3, batch:3, nativeType:'ws', ccxtId:'poloniex', skipPro:true, ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','BRETT/USDT','POPCAT/USDT','WIF/USDT','SUI/USDT','ENA/USDT'],
  startNative:()=>connectWS({ name:'Poloniex',
    urls:['wss://ws.poloniex.com/ws/public'],  // removed ws2 (ENOTFOUND on Windows, v9.5); skipPro: 142 timeouts + typeErr (v9.5)
    pingMsg:{event:'ping'}, pingInt:20000,
    onOpen:ws=>{ws.send(JSON.stringify({event:'subscribe',channel:['trades'],symbols:['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','BRETT_USDT','POPCAT_USDT','WIF_USDT','SUI_USDT','ENA_USDT']}));ws.send(JSON.stringify({event:'subscribe',channel:['book'],symbols:['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','BRETT_USDT','POPCAT_USDT','WIF_USDT','SUI_USDT','ENA_USDT']}));},  // Removed PENGU_USDT (NOT_LISTED, verified 2026-02-27)
    onMsg:(msg)=>{const d=JSON.parse(msg);if(!d.channel||!d.data)return;const items=Array.isArray(d.data)?d.data:[d.data];for(const it of items){if(d.channel==='trades')addN('Poloniex',it.symbol,1,0,it.id);if(d.channel==='book')addN('Poloniex',it.symbol,0,1);}}
  })},

{ name:'HitBTC', tier:3, batch:3, nativeType:'ws', ccxtId:'hitbtc', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','BRETT/USDT','PENGU/USDT','POPCAT/USDT','WIF/USDT','SUI/USDT','ENA/USDT'],
  startNative:()=>connectWS({ name:'HitBTC',
    urls:['wss://api.hitbtc.com/api/3/ws/public'],
    pingMsg:{method:'server.ping',params:{},id:99}, pingInt:20000,
    onOpen:ws=>{ws.send(JSON.stringify({ch:'trades',method:'subscribe',params:{symbols:['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','BRETTUSDT','PENGUUSDT','POPCATUSDT','WIFUSDT','SUIUSDT','ENAUSDT']},id:1}));ws.send(JSON.stringify({ch:'orderbook/full',method:'subscribe',params:{symbols:['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','BRETTUSDT','PENGUUSDT','POPCATUSDT','WIFUSDT','SUIUSDT','ENAUSDT'],limit:5},id:2}));},
    onMsg:(msg)=>{const d=JSON.parse(msg);if(!d.ch)return;if(d.ch==='trades'&&d.update)for(const[sym,trades]of Object.entries(d.update))addN('HitBTC',sym,trades.length,0);if(d.ch==='orderbook/full'){const p=d.snapshot||d.update||{};for(const sym of Object.keys(p))addN('HitBTC',sym,0,1);}}
  })},

// ═══ BATCH 4: TIER 3 WS ═══
{ name:'BTSE', tier:3, batch:4, nativeType:'ws', ccxtId:null, ccxtPairs:[],
  startNative:()=>connectWS({ name:'BTSE',
    urls:['wss://ws.btse.com/ws/spot'],  // removed ws.btse.io (DNS ENOTFOUND)
    pingMsg:'ping', pingInt:30000,
    onOpen:ws=>ws.send(JSON.stringify({op:'subscribe',args:['tradeHistoryApi:BTC-USD','tradeHistoryApi:ETH-USD','tradeHistoryApi:SOL-USD','tradeHistoryApi:BTC-USDT','tradeHistoryApi:ETH-USDT','tradeHistoryApi:SOL-USDT','tradeHistoryApi:BTC-USDC','tradeHistoryApi:ETH-USDC','tradeHistoryApi:SOL-USDC','tradeHistoryApi:BRETT-USDT','tradeHistoryApi:PENGU-USDT','tradeHistoryApi:PENGU-USD','tradeHistoryApi:PENGU-USDC','tradeHistoryApi:POPCAT-USDT','tradeHistoryApi:POPCAT-USD','tradeHistoryApi:POPCAT-USDC','tradeHistoryApi:WIF-USDT','tradeHistoryApi:WIF-USD','tradeHistoryApi:WIF-USDC','tradeHistoryApi:SUI-USDT','tradeHistoryApi:SUI-USD','tradeHistoryApi:SUI-USDC','tradeHistoryApi:ENA-USDT','tradeHistoryApi:ENA-USD','tradeHistoryApi:ENA-USDC']})),  // +10 meme USD/USDC pairs (audit 2026-02-28)
    onMsg:(msg)=>{if(msg==='pong')return;const d=JSON.parse(msg);if(d.topic?.startsWith('tradeHistoryApi')&&d.data)addN('BTSE',d.topic.replace('tradeHistoryApi:',''),d.data.length,0);if(d.topic?.startsWith('update:')&&d.data)addN('BTSE',d.topic.replace('update:','').replace('_0',''),0,1);},
    extra:[{url:'wss://ws.btse.com/ws/oss/spot',onOpen:ws=>ws.send(JSON.stringify({op:'subscribe',args:['update:BTC-USD_0','update:ETH-USD_0','update:SOL-USD_0','update:BTC-USDT_0','update:ETH-USDT_0','update:SOL-USDT_0','update:BTC-USDC_0','update:ETH-USDC_0','update:SOL-USDC_0','update:BRETT-USDT_0','update:PENGU-USDT_0','update:PENGU-USD_0','update:PENGU-USDC_0','update:POPCAT-USDT_0','update:POPCAT-USD_0','update:POPCAT-USDC_0','update:WIF-USDT_0','update:WIF-USD_0','update:WIF-USDC_0','update:SUI-USDT_0','update:SUI-USD_0','update:SUI-USDC_0','update:ENA-USDT_0','update:ENA-USD_0','update:ENA-USDC_0']}))}]
  })},

{ name:'Biconomy', tier:3, batch:4, nativeType:'ws', ccxtId:null, ccxtPairs:[],
  startNative:()=>connectWS({ name:'Biconomy',
    urls:['wss://bei.biconomy.com/ws'],
    pingMsg:JSON.stringify({method:'server.ping',params:[],id:5160}), pingInt:30000,
    onOpen:ws=>{const pairs=['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','SOL_USDC','BRETT_USDT','PENGU_USDT','POPCAT_USDT','WIF_USDT','SUI_USDT','ENA_USDT'];pairs.forEach((p,i)=>{ws.send(JSON.stringify({method:'depth.subscribe',params:[p,5,'0'],id:10+i*2}));ws.send(JSON.stringify({method:'deals.subscribe',params:[p],id:10+i*2+1}));});},
    onMsg:(msg)=>{const d=JSON.parse(msg);if(d.method==='depth.update'&&d.params)addN('Biconomy',d.params[2]||'',0,1);if(d.method==='deals.update'&&d.params)addN('Biconomy',d.params[0]||'',d.params[1]?.length||1,0);}
  })},

{ name:'Hotcoin', tier:3, batch:4, nativeType:'ws+rest', ccxtId:null, ccxtPairs:[],
  startNative:()=>{
    runREST('Hotcoin',['btc_usdt','eth_usdt','sol_usdt','brett_usdt','pengu_usdt','popcat_usdt','wif_usdt','sui_usdt','ena_usdt','btc_usdc','eth_usdc','sol_usdc'].flatMap(s=>[
      {url:`https://api.hotcoinfin.com/v1/market/trade?symbol=${s}&size=10`,parse:r=>{if(r?.data?.length)addN('Hotcoin',s,r.data.length,0);else if(r?.data?.data?.length)addN('Hotcoin',s,r.data.data.length,0);}},
      {url:`https://api.hotcoinfin.com/v1/market/depth?symbol=${s}&type=step0`,parse:r=>{if(r?.data?.bids?.length)addN('Hotcoin',s,0,1);}}
    ]),10000);
    connectWS({ name:'Hotcoin',
      urls:['wss://wss.hotcoinfin.com/trade/multiple'],
      compression:'gzip',
      onOpen:ws=>{for(const s of['btc_usdt','eth_usdt','sol_usdt','brett_usdt','pengu_usdt','popcat_usdt','wif_usdt','sui_usdt','ena_usdt','btc_usdc','eth_usdc','sol_usdc']){ws.send(JSON.stringify({sub:`market.${s}.trade.depth`}));ws.send(JSON.stringify({sub:`market.${s}.trade.detail`}));}},
      staleTimeout:60000,
      onMsg:(msg,ws)=>{try{const d=JSON.parse(msg);if(d.ping!==undefined){ws.send(JSON.stringify({pong:d.ping}));return;}if(!d.ch)return;const sym=d.ch.split('.')[1]||'';if(d.ch.includes('trade.detail')&&d.data)addN('Hotcoin',sym,Array.isArray(d.data)?d.data.length:1,0);if(d.ch.includes('trade.depth')&&d.data)addN('Hotcoin',sym,0,1);}catch{}}
    });
  }},

// NovaEx is a WOO X white-label — uses woox.io endpoint intentionally (app-id OqdphuyIYbng-t001)
{ name:'NovaEx', tier:3, batch:4, nativeType:'ws', ccxtId:null, ccxtPairs:[],
  startNative:()=>connectWS({ name:'NovaEx',
    urls:['wss://wss.woox.io/ws/stream/OqdphuyIYbng-t001'],
    pingMsg:{event:'ping'}, pingInt:9000,
    onOpen:ws=>{for(const s of['SPOT_BTC_USDT','SPOT_ETH_USDT','SPOT_SOL_USDT','SPOT_BTC_USDC','SPOT_ETH_USDC']){ws.send(JSON.stringify({id:s+'o',event:'subscribe',topic:`${s}@orderbook`}));ws.send(JSON.stringify({id:s+'t',event:'subscribe',topic:`${s}@trade`}));}},
    onMsg:(msg)=>{const d=JSON.parse(msg);if(d.event==='pong'||d.event==='subscribe')return;const t=d.topic||'';const sym=t.split('@')[0];if(!sym.startsWith('SPOT_'))return;if(t.includes('@trade')&&d.data)addN('NovaEx',sym,1,0);if(t.includes('@orderbook')&&d.data)addN('NovaEx',sym,0,1);}
  })},

{ name:'FameEX', tier:3, batch:4, nativeType:'ws', ccxtId:null, ccxtPairs:[],
  startNative:()=>connectWS({ name:'FameEX',
    urls:['wss://wsapi.fameex.com/v1/ws/stream/public'],
    onOpen:ws=>{for(const s of['btcusdt','ethusdt','solusdt','btcusdc','ethusdc','solusdc','wifusdt']){ws.send(JSON.stringify({event:'sub',params:{channel:`market_${s}_trade_detail`}}));ws.send(JSON.stringify({event:'sub',params:{channel:`market_${s}_depth_step0`}}));}},
    onMsg:(msg,ws)=>{const d=JSON.parse(msg);if(d.ping){ws.send(JSON.stringify({pong:d.ping}));return;}const ch=d.channel||'';if(!ch)return;if(ch.includes('_trade')&&Array.isArray(d.data)){const sym=ch.replace('market_','').replace(/_trade.*$/,'').toUpperCase();addN('FameEX',sym,d.data.length,0);}if(ch.includes('_depth')&&d.tick){const sym=(d.tick.pair||ch.replace('market_','').replace(/_depth.*$/,'')).toUpperCase();addN('FameEX',sym,0,1);}}
  })},

{ name:'Websea', tier:3, batch:4, nativeType:'ws', ccxtId:null, ccxtPairs:[],
  startNative:()=>connectWS({ name:'Websea',
    urls:['wss://oapi.websea.com/ws/v1/spot/market'],
    onOpen:ws=>{for(const s of['BTC-USDT','ETH-USDT','SOL-USDT','PENGU-USDT','WIF-USDT','SUI-USDT','ENA-USDT']){ws.send(JSON.stringify({op:'sub',channel:'trade',symbol:s}));ws.send(JSON.stringify({op:'sub',channel:'depth',symbol:s}));}},
    onMsg:(msg,ws)=>{const d=JSON.parse(msg);if(d.op==='ping'||d.ping){ws.send(JSON.stringify({op:'pong'}));return;}if(d.channel==='trade'&&d.symbol)addN('Websea',d.symbol,1,0);if(d.channel==='depth'&&d.symbol)addN('Websea',d.symbol,0,1);}
  })},

{ name:'Bullish', tier:3, batch:4, nativeType:'ws', ccxtId:'bullish', ccxtPairs:['BTC/USDC','ETH/USDC','SOL/USDC','BTC/USDT','ETH/USDT','SOL/USDT','BTC/USD','ETH/USD','SOL/USD','PENGU/USDT','PENGU/USDC','WIF/USDC','SUI/USDC'],
  startNative:()=>connectWS({ name:'Bullish',
    urls:['wss://api.exchange.bullish.com/trading-api/v1/market-data/trades'],
    onOpen:ws=>{for(const sym of['BTCUSDC','ETHUSDC','SOLUSDC','BTCUSDT','ETHUSDT','SOLUSDT','BTCUSD','ETHUSD','SOLUSD','PENGUUSDT','PENGUUSDC','WIFUSDC','SUIUSDC'])ws.send(JSON.stringify({jsonrpc:'2.0',type:'command',method:'subscribe',params:{topic:'anonymousTrades',symbol:sym},id:sym+'_t'}));},
    restPoll:(name)=>{const poll=async()=>{if(stopFlag)return;for(const s of['BTCUSDC','ETHUSDC','SOLUSDC','BTCUSDT','ETHUSDT','SOLUSDT','BTCUSD','ETHUSD','SOLUSD','PENGUUSDT','PENGUUSDC','WIFUSDC','SUIUSDC']){try{const r=await httpsReq(`https://api.exchange.bullish.com/trading-api/v1/markets/${s}/orderbook/hybrid`);if(r?.bids?.length||r?.asks?.length)addN('Bullish',s,0,1);}catch{}}if(!stopFlag)setTimeout(poll,35000);};poll();},  // poll interval 20s→35s (reduce rate limit pressure)
    onMsg:(msg,ws)=>{const d=JSON.parse(msg);if(d.type==='ping'){ws.send(JSON.stringify({type:'pong',id:d.id||'pong'}));return;}if(d.data?.trades&&d.data.symbol)addN('Bullish',d.data.symbol,d.data.trades.length,0);}
  })},

{ name:'Darkex', tier:3, batch:4, nativeType:'ws', ccxtId:null, ccxtPairs:[],
  startNative:()=>connectWS({ name:'Darkex',
    urls:['wss://ws.darkex.com/kline-api/ws'],
    compression:'gzip',
    onOpen:ws=>{for(const s of['btcusdt','ethusdt','popcatusdt','wifusdt','suiusdt']){ws.send(JSON.stringify({event:'sub',params:{channel:`market_${s}_depth_step0`}}));ws.send(JSON.stringify({event:'sub',params:{channel:`market_${s}_trade_ticker`}}));}},
    staleTimeout:60000,
    onMsg:(msg,ws)=>{const d=JSON.parse(msg);if(d.ping!==undefined){ws.send(JSON.stringify({pong:d.ping}));return;}if(!d.channel||!d.tick)return;const m=d.channel.match(/market_(\w+?)_(depth|trade)/);if(!m)return;const sym=m[1].toUpperCase();if(d.channel.includes('_depth_'))addN('Darkex',sym,0,1);if(d.channel.includes('_trade_')&&d.tick.data)addN('Darkex',sym,d.tick.data.length,0);}
  })},

{ name:'Bitrue', tier:3, batch:4, nativeType:'ws', ccxtId:'bitrue', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','BRETT/USDT','PENGU/USDT','PENGU/USDC','POPCAT/USDT','WIF/USDT','WIF/USDC','SUI/USDT','SUI/USDC','ENA/USDT','ENA/USDC'],
  startNative:()=>connectWS({ name:'Bitrue',
    urls:['wss://ws.bitrue.com/market/ws','wss://wsapi.bitrue.com/kline-api/ws'],
    compression:'gzip',
    onOpen:ws=>{for(const sym of['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','BRETTUSDT','PENGUUSDT','POPCATUSDT','WIFUSDT','SUIUSDT','ENAUSDT'])ws.send(JSON.stringify({event:'sub',params:{cb_id:sym,channel:`market_${sym}_depth_step0`}}));},
    restPoll:(name)=>{const poll=async()=>{if(stopFlag)return;for(const s of['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC','SOLUSDC','BRETTUSDT','PENGUUSDT','POPCATUSDT','WIFUSDT','SUIUSDT','ENAUSDT']){try{const r=await httpsReq(`https://openapi.bitrue.com/api/v1/trades?symbol=${s}&limit=5`);if(Array.isArray(r))addN('Bitrue',s,r.length,0);}catch{}}if(!stopFlag)setTimeout(poll,10000);};poll();},
    onMsg:(msg,ws)=>{const d=JSON.parse(msg);if(d.event==='ping'){ws.send(JSON.stringify({event:'pong'}));return;}if(d.channel?.includes('depth')&&d.tick){const sym=d.channel.replace('market_','').replace('_depth_step0','');addN('Bitrue',sym,0,1);}}
  })},

{ name:'BloFin', tier:3, batch:4, nativeType:'ws', ccxtId:'blofin', ccxtPairs:['BTC/USDT:USDT','ETH/USDT:USDT','SOL/USDT:USDT','BTC/USDC:USDC','SOL/USDC:USDC','BTC/USD:USD','ETH/USD:USD','SOL/USD:USD'],  // removed ETH/USDC:USDC (CCXT not found, v9.4)
  startNative:()=>connectWS({ name:'BloFin',
    urls:['wss://openapi.blofin.com/ws/public'],
    pingMsg:'ping', pingInt:25000,
    onOpen:ws=>{const p=['BTC-USDT','ETH-USDT','SOL-USDT','BTC-USDC','ETH-USDC','SOL-USDC','BTC-USD','ETH-USD','SOL-USD'];ws.send(JSON.stringify({op:'subscribe',args:p.map(x=>({channel:'trades',instId:x}))}));ws.send(JSON.stringify({op:'subscribe',args:p.map(x=>({channel:'books',instId:x}))}));},
    onMsg:(msg)=>{if(msg==='pong')return;try{const d=JSON.parse(msg);if(d.event==='subscribe'||d.event==='error'||!d.arg)return;const sym=d.arg.instId;if(d.arg.channel==='trades'&&d.data)addN('BloFin',sym,d.data.length,0);if((d.arg.channel==='books'||d.arg.channel==='books5')&&d.data)addN('BloFin',sym,0,1);}catch{}}
  })},

// ═══ BATCH 5: REST + REMAINING ═══
{ name:'DigiFinex', tier:3, batch:5, nativeType:'ws', ccxtId:'digifinex', skipPro:true, ccxtPairs:['BTC/USDT','BTC/USDC','ETH/USDT','ETH/USDC','SOL/USDT','BRETT/USDT'],
  startNative:()=>connectWS({ name:'DigiFinex',
    urls:['wss://openapi.digifinex.com/ws/v1/'],
    compression:'inflate',
    customPing:ws=>ws.send(JSON.stringify({id:0,method:'server.ping',params:[]})), customPingInt:30000,
    onOpen:ws=>{ws.send(JSON.stringify({id:1,method:'trades.subscribe',params:['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','SOL_USDC','BRETT_USDT']}));ws.send(JSON.stringify({id:2,method:'depth.subscribe',params:['BTC_USDT',5,'0']}));},
    onMsg:(msg)=>{const d=JSON.parse(msg);if(d.method==='trades.update'&&d.params)addN('DigiFinex',d.params[2]||'',d.params[1]?.length||1,0);if(d.method==='depth.update'&&d.params)addN('DigiFinex',d.params[2]||'',0,1);},
    extra:['ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','SOL_USDC','BRETT_USDT'].map((p,i)=>({onOpen:ws=>ws.send(JSON.stringify({id:i+3,method:'depth.subscribe',params:[p,5,'0']}))}))
  })},

{ name:'EXMO', tier:3, batch:5, nativeType:'ws', ccxtId:'exmo', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','BTC/USD','ETH/USD','PENGU/USDT','PENGU/USDC','WIF/USDT','WIF/USDC','SUI/USDT','SUI/USDC','ENA/USDT','ENA/USDC'],
  startNative:()=>connectWS({ name:'EXMO',
    urls:['wss://ws-api.exmo.com/v1/public','wss://ws-api.exmo.me/v1/public','wss://ws-api.exmo.io/v1/public'],
    pingMsg:{method:'ping',id:99}, pingInt:20000,
    staleTimeout:45000,
    onOpen:ws=>ws.send(JSON.stringify({method:'subscribe',topics:['spot/trades:BTC_USDT','spot/trades:ETH_USDT','spot/trades:SOL_USDT','spot/trades:BTC_USDC','spot/trades:ETH_USDC','spot/trades:SOL_USDC','spot/trades:BTC_USD','spot/trades:ETH_USD','spot/trades:BTC_DAI','spot/order_book_snapshots:BTC_USDT','spot/order_book_snapshots:ETH_USDT','spot/order_book_snapshots:SOL_USDT','spot/order_book_snapshots:BTC_USDC','spot/order_book_snapshots:ETH_USDC','spot/order_book_snapshots:SOL_USDC','spot/order_book_snapshots:BTC_USD','spot/order_book_snapshots:ETH_USD','spot/order_book_snapshots:BTC_DAI','spot/trades:PENGU_USDT','spot/trades:PENGU_USDC','spot/trades:WIF_USDT','spot/trades:WIF_USDC','spot/trades:SUI_USDT','spot/trades:SUI_USDC','spot/trades:ENA_USDT','spot/trades:ENA_USDC','spot/order_book_snapshots:PENGU_USDT','spot/order_book_snapshots:PENGU_USDC','spot/order_book_snapshots:WIF_USDT','spot/order_book_snapshots:WIF_USDC','spot/order_book_snapshots:SUI_USDT','spot/order_book_snapshots:SUI_USDC','spot/order_book_snapshots:ENA_USDT','spot/order_book_snapshots:ENA_USDC']})),
    onMsg:(msg)=>{const d=JSON.parse(msg);if(d.event!=='update'&&d.event!=='snapshot')return;const t=d.topic||'';const data=d.data||{};if(t.includes('trades')){const sym=t.split(':')[1];const trades=Array.isArray(data)?data:(data.trades||[]);addN('EXMO',sym,trades.length,0);}if(t.includes('order_book')&&(data.bid||data.ask)){const sym=t.split(':')[1];addN('EXMO',sym,0,1);}}
  })},

{ name:'CEX.IO', tier:3, batch:5, nativeType:'rest', ccxtId:'cex', skipPro:true, ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BTC/USDC','ETH/USDC','SOL/USDC','BTC/USD','ETH/USD','SOL/USD','BRETT/USDT','BRETT/USDC','BRETT/USD','PENGU/USDT','PENGU/USD','POPCAT/USDT','POPCAT/USD','WIF/USDT','WIF/USDC','WIF/USD','SUI/USDT','SUI/USDC','SUI/USD','ENA/USDT','ENA/USDC','ENA/USD'],
  startNative:()=>runREST('CEX.IO',[['BTC','USDT'],['ETH','USDT'],['SOL','USDT'],['BTC','USDC'],['ETH','USDC'],['SOL','USDC'],['BTC','USD'],['ETH','USD'],['SOL','USD'],['BRETT','USDT'],['BRETT','USDC'],['BRETT','USD'],['PENGU','USDT'],['PENGU','USD'],['POPCAT','USDT'],['POPCAT','USD'],['WIF','USDT'],['WIF','USDC'],['WIF','USD'],['SUI','USDT'],['SUI','USDC'],['SUI','USD'],['ENA','USDT'],['ENA','USDC'],['ENA','USD']].flatMap(([b,q])=>[
    {url:`https://cex.io/api/trade_history/${b}/${q}/`,parse:r=>{if(Array.isArray(r))addN('CEX.IO',`${b}_${q}`,Math.min(r.length,20),0);}},
    {url:`https://cex.io/api/order_book/${b}/${q}/?depth=5`,parse:r=>{if(r?.bids?.length)addN('CEX.IO',`${b}_${q}`,0,1);}}
  ]))},

{ name:'OrangeX', tier:3, batch:5, nativeType:'rest', ccxtId:null, ccxtPairs:[],
  startNative:()=>runREST('OrangeX',['BTC-USDT','ETH-USDT','SOL-USDT','BTC-USDC','ETH-USDC'].flatMap(sym=>[
    {url:`https://api.orangex.com/api/v1/public/get_last_trades_by_instrument?instrument_name=${sym}-SPOT&count=5`,parse:r=>{if(r?.result?.trades)addN('OrangeX',sym,r.result.trades.length,0);}},
    {url:`https://api.orangex.com/api/v1/public/get_order_book?instrument_name=${sym}-SPOT&depth=5`,parse:r=>{if(r?.result?.bids)addN('OrangeX',sym,0,1);}}
  ]))},

{ name:'Azbit', tier:3, batch:5, nativeType:'rest', ccxtId:null, ccxtPairs:[],
  startNative:()=>runREST('Azbit',['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','SOL_USDC','BRETT_USDT','PENGU_USDT','POPCAT_USDT','WIF_USDT','SUI_USDT','ENA_USDT'].flatMap(sym=>[
    {url:`https://data.azbit.com/api/deals?currencyPairCode=${sym}`,parse:r=>{if(Array.isArray(r))addN('Azbit',sym,Math.min(r.length,5),0);}},
    {url:`https://data.azbit.com/api/orderbook?currencyPairCode=${sym}`,parse:r=>{if(Array.isArray(r)&&r.length)addN('Azbit',sym,0,1);}}
  ]))},

{ name:'BVOX', tier:3, batch:5, nativeType:'rest', ccxtId:null, ccxtPairs:[],
  startNative:()=>runREST('BVOX',['BTCUSDT','ETHUSDT','SOLUSDT'].flatMap(sym=>[
    {url:`https://api.bitvenus.me/openapi/quote/v1/trades?symbol=${sym}&limit=5`,parse:r=>{if(Array.isArray(r))addN('BVOX',sym,r.length,0);}},
    {url:`https://api.bitvenus.me/openapi/quote/v1/depth?symbol=${sym}&limit=5`,parse:r=>{if(r?.bids?.length)addN('BVOX',sym,0,1);}}
  ]))},

{ name:'Trubit Pro', tier:3, batch:5, nativeType:'rest', ccxtId:null, ccxtPairs:[],
  startNative:()=>runREST('Trubit Pro',['BTCUSDT','ETHUSDT','SOLUSDT','BTCUSDC','ETHUSDC'].flatMap(sym=>[
    {url:`https://api-spot.trubit.com/openapi/quote/v1/trades?symbol=${sym}&limit=5`,parse:r=>{if(Array.isArray(r))addN('Trubit Pro',sym,r.length,0);}},
    {url:`https://api-spot.trubit.com/openapi/quote/v1/depth?symbol=${sym}&limit=5`,parse:r=>{if(r?.bids?.length)addN('Trubit Pro',sym,0,1);}}
  ]))},

{ name:'BigONE', tier:3, batch:5, nativeType:'rest', ccxtId:'bigone', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','PENGU/USDT','WIF/USDT','SUI/USDT','ENA/USDT'],
  startNative:()=>runREST('BigONE',['BTC-USDT','ETH-USDT','SOL-USDT','PENGU-USDT','WIF-USDT','SUI-USDT','ENA-USDT'].flatMap(sym=>[
    {url:`https://big.one/api/v3/asset_pairs/${sym}/trades`,parse:r=>{if(r?.data?.length)addN('BigONE',sym,r.data.length,0);}},
    {url:`https://big.one/api/v3/asset_pairs/${sym}/depth`,parse:r=>{if(r?.data?.bids?.length)addN('BigONE',sym,0,1);}}
  ]))},

{ name:'LATOKEN', tier:3, batch:5, nativeType:'rest', ccxtId:'latoken', ccxtPairs:['BTC/USDT','ETH/USDT','SOL/USDT','BRETT/USDT','PENGU/USDT','POPCAT/USDT','WIF/USDT','SUI/USDT','ENA/USDT'],
  startNative:()=>runREST('LATOKEN',[
    ['4f4a4e5e-7192-4e7e-9f78-d8f6e07c0001','0c3a106d-bde3-4c13-a26e-3fd2394529e5','BTC_USDT'],
    ['620f2019-33c0-423b-8a9d-cde4d7f8ef7f','0c3a106d-bde3-4c13-a26e-3fd2394529e5','ETH_USDT'],
    ['f5924e5b-3860-4a3c-94d0-6c3fd4999e73','0c3a106d-bde3-4c13-a26e-3fd2394529e5','SOL_USDT'],
    ['335f4e73-ec70-4fc7-a97c-935c399ad4cd','0c3a106d-bde3-4c13-a26e-3fd2394529e5','BRETT_USDT'],
    ['06775a0c-fb18-459b-b2eb-da8f028d6057','0c3a106d-bde3-4c13-a26e-3fd2394529e5','PENGU_USDT'],
    ['2f685624-16d1-418a-b2b6-12524da9e203','0c3a106d-bde3-4c13-a26e-3fd2394529e5','POPCAT_USDT'],
    ['b8f1e53a-2dd6-4892-9e85-0bb6741096f9','0c3a106d-bde3-4c13-a26e-3fd2394529e5','WIF_USDT'],
    ['faad614d-2cce-4d21-8e1c-10790019e8d5','0c3a106d-bde3-4c13-a26e-3fd2394529e5','SUI_USDT'],
    ['7f2f7c8a-cc97-4f98-a289-ca763c858905','0c3a106d-bde3-4c13-a26e-3fd2394529e5','ENA_USDT']
  ].flatMap(([b,q,cn])=>[
    {url:`https://api.latoken.com/v2/trade/history/${b}/${q}`,parse:r=>{if(Array.isArray(r))addN('LATOKEN',cn,Math.min(r.length,20),0);}},
    {url:`https://api.latoken.com/v2/book/${b}/${q}`,parse:r=>{if(r?.bid?.length||r?.ask?.length)addN('LATOKEN',cn,0,1);}}
  ]))},

// ═══ BATCH 6: NEW EXCHANGES ═══
{ name:'Coinstore', tier:3, batch:6, nativeType:'ws', ccxtId:null, ccxtPairs:[],
  startNative:()=>connectWS({ name:'Coinstore',
    urls:['wss://ws.coinstore.com/s/ws'],
    restFallbackUrls:['BTCUSDT','ETHUSDT','SOLUSDT','PENGUUSDT','WIFUSDT','SUIUSDT','ENAUSDT'].map(s=>({
      url:`https://api.coinstore.com/api/v1/market/trade/${s}?size=5`,
      parse:r=>{if(r?.data&&Array.isArray(r.data))addN('Coinstore',s,r.data.length,0);}
    })),
    onOpen:ws=>{
      const pairs=['btcusdt','ethusdt','solusdt','btcusdc','ethusdc','solusdc','penguusdt','wifusdt','suiusdt','enausdt'];
      ws.send(JSON.stringify({op:'SUB',channel:pairs.map(s=>`${s}@trade`),id:1}));
      ws.send(JSON.stringify({op:'SUB',channel:pairs.map(s=>`${s}@depth@20`),id:2}));
    },
    onMsg:(msg,ws)=>{
      const d=JSON.parse(msg);
      if(d.T==='resp'||d.T==='echo')return;
      if(d.T==='trade'&&d.symbol){addN('Coinstore',d.symbol,1,0,String(d.tradeId||d.seq||''));return;}
      if(d.T==='trade'&&d.data&&Array.isArray(d.data)){for(const t of d.data)addN('Coinstore',t.symbol,1,0,String(t.tradeId||''));return;}
      if((d.T==='depth'||d.level!==undefined)&&(d.b||d.a)){addN('Coinstore',d.symbol||'',0,1);return;}
      if(d.ping!==undefined||d.action==='ping')ws.send(JSON.stringify({op:'pong',epochMillis:Date.now()}));
    }
  })},

{ name:'GroveX', tier:3, batch:6, nativeType:'ws', ccxtId:null, ccxtPairs:[],
  startNative:()=>connectWS({ name:'GroveX',
    urls:['wss://ws.grovex.io/kline-api/ws','wss://openapi.grovex.io/kline-api/ws'],
    compression:'gzip',
    restFallbackUrls:[{url:'https://openapi.grovex.io/open/api/market_dept?symbol=btcusdt&type=step0',
      parse:r=>{if(r?.data?.tick?.asks)addN('GroveX','BTCUSDT',0,1);}}],
    onOpen:ws=>{
      for(const s of['btcusdt','ethusdt','solusdt','btcusdc','ethusdc','solusdc','brettusdt','penguusdt','popcatusdt','wifusdt','suiusdt','enausdt']){
        ws.send(JSON.stringify({event:'sub',params:{channel:`market_${s}_depth_step0`,cb_id:s+'d',asks:150,bids:150}}));
        ws.send(JSON.stringify({event:'sub',params:{channel:`market_${s}_trade_ticker`,cb_id:s+'t'}}));
      }
    },
    onMsg:(msg,ws)=>{
      const d=JSON.parse(msg);
      if(d.ping!==undefined){ws.send(JSON.stringify({pong:d.ping}));return;}
      if(d.pong!==undefined||!d.channel||!d.tick)return;
      const m=d.channel.match(/market_(\w+?)_(depth|trade)/);
      if(!m)return;
      const sym=m[1].toUpperCase();
      if(d.channel.includes('_depth_'))addN('GroveX',sym,0,1);
      if(d.channel.includes('_trade_')&&d.tick.data)addN('GroveX',sym,d.tick.data.length||1,0);
    }
  })},

{ name:'CoinW', tier:3, batch:6, nativeType:'rest', ccxtId:null, ccxtPairs:[],
  startNative:()=>{
    const pm={btc_usdt:'BTC_USDT',eth_usdt:'ETH_USDT',sol_usdt:'SOL_USDT',btc_usdc:'BTC_USDC',eth_usdc:'ETH_USDC',sol_usdc:'SOL_USDC',brett_usdt:'BRETT_USDT',pengu_usdt:'PENGU_USDT',popcat_usdt:'POPCAT_USDT',wif_usdt:'WIF_USDT',sui_usdt:'SUI_USDT',ena_usdt:'ENA_USDT'};
    runREST('CoinW',Object.keys(pm).flatMap(sym=>[
      {url:`https://api.coinw.com/api/v1/public?command=returnTradeHistory&symbol=${sym}`,
       parse:r=>{if(r?.code==='200'&&Array.isArray(r?.data))addN('CoinW',pm[sym],Math.min(r.data.length,10),0);}},
      {url:`https://api.coinw.com/api/v1/public?command=returnOrderBook&symbol=${sym}`,
       parse:r=>{if(r?.code==='200'&&r?.data&&(r.data.bids||r.data.asks))addN('CoinW',pm[sym],0,1);}}
    ]),8000);
  }},

{ name:'Batonex', tier:3, batch:6, nativeType:'rest', ccxtId:null, ccxtPairs:[],
  startNative:()=>runREST('Batonex',['BTCUSDT','ETHUSDT','SOLUSDT','WIFUSDT'].flatMap(sym=>[  // +WIF confirmed live; BRETT/PENGU/POPCAT/SUI/ENA not listed (exchange has only 32 pairs)
    {url:`https://api.batonex.com/openapi/quote/v1/trades?symbol=${sym}&limit=5`,
     parse:r=>{if(Array.isArray(r))addN('Batonex',sym,r.length,0);}},
    {url:`https://api.batonex.com/openapi/quote/v1/depth?symbol=${sym}&limit=5`,
     parse:r=>{if(r?.bids?.length)addN('Batonex',sym,0,1);}}
  ]))},

{ name:'CEEX', tier:3, batch:6, nativeType:'ws', ccxtId:null, ccxtPairs:[],
  startNative:()=>connectWS({ name:'CEEX',
    urls:['wss://wsapi.ceex.com/openapi/quote/ws/v1'],
    restFallbackUrls:['BTCUSDT','ETHUSDT','SOLUSDT','WIFUSDT','SUIUSDT','ENAUSDT'].flatMap(s=>[
      {url:`https://api.ceex.com/openapi/quote/v1/trades?symbol=${s}&limit=5`,
       parse:r=>{if(Array.isArray(r))addN('CEEX',s,r.length,0);}},
      {url:`https://api.ceex.com/openapi/quote/v1/depth?symbol=${s}&limit=10`,
       parse:r=>{if(r?.bids?.length)addN('CEEX',s,0,1);}}
    ]),
    onOpen:ws=>{
      for(const s of['BTCUSDT','ETHUSDT','SOLUSDT','WIFUSDT','SUIUSDT','ENAUSDT']){
        ws.send(JSON.stringify({symbol:s,topic:'depth',event:'sub'}));
        ws.send(JSON.stringify({symbol:s,topic:'trade',event:'sub'}));
      }
    },
    onMsg:(msg,ws)=>{
      if(msg==='ping'){ws.send('pong');return;}
      const d=JSON.parse(msg);
      if(d.ping){ws.send(JSON.stringify({pong:d.ping}));return;}
      if(d.topic==='depth'&&d.data&&Array.isArray(d.data)){
        for(const snap of d.data)if(snap.b||snap.a)addN('CEEX',d.symbol||snap.s||'',0,1);
        return;
      }
      if(d.topic==='trade'&&d.data&&Array.isArray(d.data)){
        for(const t of d.data)addN('CEEX',d.symbol||'',1,0,String(t.v||''));
        return;
      }
    }
  })},
];

// ═══════════════════ REPORT GENERATOR (v9.5 — Full Architecture + Streaming Detail) ═══════════════════
function generateReport() {
    const data = buildStats();
    const smStats = getSubManagerStats();
    let r = `# Enhanced 4-Method Comparison Report v9.5 — Native × CCXT Pro × CCXT REST × Direct REST + Subscription Manager\n\n`;
    r += `**Date:** ${new Date().toISOString().split('T')[0]}\n`;
    r += `**Test Duration:** ${MINUTES} minutes (${data.uptime}s actual)\n`;
    r += `**Exchanges Tested:** ${Object.keys(data.exchanges).length}\n`;
    r += `**Pairs Tested:** BTC/ETH/SOL/BRETT/PENGU/POPCAT/WIF/SUI/ENA × USDT/USDC/USD (all available per exchange)\n`;
    r += `**Method:** v9.5 Enhanced — 4 parallel methods + Subscription Manager: Native WS/REST, CCXT Pro, CCXT REST, Direct REST\n`;
    r += `**Storage:** DuckDB ${duckEnabled ? '✅ enabled' : '❌ disabled'}\n`;
    r += `**Health:** Avg Score ${data.healthSummary.avgScore}/100 | ${data.healthSummary.critical.length} critical | ${data.healthSummary.warnings.length} warnings\n`;
    r += `**Dashboard:** http://localhost:${HTTP_PORT} (9 tabs: Hybrid Flow, Per Exchange, Per Pair, Dedup Analysis, Analytics, Correlation, Health, Sub Manager, Live Events)\n\n`;

    // ═══════════════════════════════════════════════════════
    // DETAILED ARCHITECTURE EXPLANATION
    // ═══════════════════════════════════════════════════════
    r += `---\n\n`;
    r += `## 🏗️ System Architecture — Detailed Explanation\n\n`;
    r += `### Overview\n\n`;
    r += `This system is a **53-exchange normalized crypto streaming platform** that connects to every exchange simultaneously using **4 independent data collection methods** running in parallel. All data flows through a **Hybrid Fusion Engine** that deduplicates, normalizes, and selects the best source for each exchange-pair combination in real time.\n\n`;
    
    r += `### The 4 Streaming Methods\n\n`;
    r += `| # | Method | Type | How It Works | Priority |\n`;
    r += `|---|--------|------|-------------|----------|\n`;
    r += `| 1 | **Native WS/REST** | WebSocket (primary) + REST fallback | Direct WebSocket connections to each exchange's public API using exchange-specific protocols. Each exchange has custom subscribe/parse logic matching their exact API format. If WS goes silent >10s, REST endpoints are polled as fallback. | **Highest (0)** — always preferred |\n`;
    r += `| 2 | **CCXT Pro** | WebSocket (unified) | Uses the CCXT Pro library's \`watch*\` methods (\`watchTrades\`, \`watchOrderBook\`, \`watchTicker\`) in async loops. CCXT abstracts each exchange's WS protocol into a unified API. Runs with \`newUpdates: false\` for full snapshots. | **High (1)** — used when Native is unavailable for a pair |\n`;
    r += `| 3 | **CCXT REST** | REST polling | Uses CCXT's \`fetch*\` methods (\`fetchTrades\`, \`fetchOrderBook\`, \`fetchTicker\`) in 5-second polling loops. Markets are pre-loaded before test starts to avoid cold-start delays. | **Medium (2)** — used when WS methods go stale |\n`;
    r += `| 4 | **Direct REST** | REST polling (raw HTTP) | Raw HTTP calls to exchange REST APIs without any CCXT abstraction. Uses Node.js \`https\` module directly. Polls every 5-8 seconds. Serves as the last-resort data source. | **Low (3)** — fallback when all other methods fail |\n\n`;

    r += `### Data Flow Pipeline\n\n`;
    r += `\`\`\`\n`;
    r += `┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐\n`;
    r += `│  Native WS   │  │  CCXT Pro    │  │  CCXT REST   │  │  Direct REST │\n`;
    r += `│  (Priority 0)│  │  (Priority 1)│  │  (Priority 2)│  │  (Priority 3)│\n`;
    r += `└──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘\n`;
    r += `       │                 │                 │                 │\n`;
    r += `       ▼                 ▼                 ▼                 ▼\n`;
    r += `  ┌─────────────────────────────────────────────────────────────┐\n`;
    r += `  │                   Symbol Normalization                      │\n`;
    r += `  │  toCanonical(): BTC/USDT, btcusdt, BTC-USDT → BTC_USDT    │\n`;
    r += `  └─────────────────────────┬───────────────────────────────────┘\n`;
    r += `                            │\n`;
    r += `                            ▼\n`;
    r += `  ┌─────────────────────────────────────────────────────────────┐\n`;
    r += `  │              shouldEmit() — Priority Gate                   │\n`;
    r += `  │  Only the highest-priority active method emits to hybrid.   │\n`;
    r += `  │  If higher-priority source goes stale (>30s), lower        │\n`;
    r += `  │  priority sources are allowed through.                      │\n`;
    r += `  └─────────────────────────┬───────────────────────────────────┘\n`;
    r += `                            │\n`;
    r += `                            ▼\n`;
    r += `  ┌─────────────────────────────────────────────────────────────┐\n`;
    r += `  │              Hybrid Fusion Engine                           │\n`;
    r += `  │  • Trade ID dedup (cross-method duplicate removal)         │\n`;
    r += `  │  • OB sequence validation (Binance, OKX checksum)          │\n`;
    r += `  │  • Stale OB detection and drop                             │\n`;
    r += `  │  • Normalized output: trades + orderbook + tickers         │\n`;
    r += `  └─────────────────────────┬───────────────────────────────────┘\n`;
    r += `                            │\n`;
    r += `              ┌─────────────┼────────────────┐\n`;
    r += `              ▼             ▼                ▼\n`;
    r += `  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐\n`;
    r += `  │   DuckDB     │  │   Dashboard  │  │     SSE      │\n`;
    r += `  │  Storage     │  │  (port 3456) │  │  Live Events │\n`;
    r += `  └──────────────┘  └──────────────┘  └──────────────┘\n`;
    r += `\`\`\`\n\n`;
    
    r += `### Subscription Manager\n\n`;
    r += `The Subscription Manager is a critical component that handles the complexity of maintaining WebSocket connections across 53 different exchanges:\n\n`;
    r += `- **Batched Subscriptions:** Instead of subscribing to all channels at once (which overloads many exchanges), subscriptions are sent in small batches (5-50 per batch depending on exchange) with configurable delays (100-400ms between batches)\n`;
    r += `- **Connection Pooling:** Tracks all active WebSocket connections per exchange. Exchanges with low per-connection limits (e.g., Bitfinex: 30 max) use multiple connections\n`;
    r += `- **Stale Detection:** Monitors last message time per exchange. If no data received within staleTimeout (45-60s), the connection is force-reconnected\n`;
    r += `- **Failover URL Rotation:** For ${Object.keys(FAILOVER_URLS).filter(k=>k!=='default'&&FAILOVER_URLS[k].length>0).length} exchanges with multiple endpoints, automatically rotates to backup WebSocket URLs on connection failure (max 5 rotations/minute)\n`;
    r += `- **Exponential Backoff:** Reconnects use exponential backoff (1s→30s) with 30% random jitter to avoid thundering herd\n\n`;
    
    r += `### Emit Priority System (shouldEmit)\n\n`;
    r += `For each exchange-pair combination, only ONE method is allowed to emit to the hybrid output at a time:\n\n`;
    r += `1. **Native (priority 0)** — Always takes over if it has data\n`;
    r += `2. **CCXT Pro (priority 1)** — Used when Native is not available for that pair\n`;
    r += `3. **CCXT REST (priority 2)** — Fills in when WS methods go stale\n`;
    r += `4. **Direct REST (priority 3)** — Last resort fallback\n\n`;
    r += `If a higher-priority source goes silent for >30 seconds, lower-priority sources are automatically promoted. This prevents duplicate counting while ensuring maximum data coverage.\n\n`;
    
    r += `### Health Scoring Algorithm\n\n`;
    r += `Each exchange receives a health score out of 100:\n\n`;
    r += `\`\`\`\n`;
    r += `score = 100\n`;
    r += `score -= min(20, totalErrors × 0.3)           // Max penalty: -20\n`;
    r += `score -= min(15, CONNECTION_CLOSED_count × 2)  // Max penalty: -15\n`;
    r += `score -= min(8,  TIMEOUT_count × 1)            // Max penalty: -8\n`;
    r += `score -= min(10, TYPE_ERROR_count × 5)          // Max penalty: -10\n`;
    r += `score += 5 if Native has data                   // Bonus: +5\n`;
    r += `score += 5 if CCXT Pro has data                 // Bonus: +5\n`;
    r += `score += 3 if CCXT REST has data                // Bonus: +3\n`;
    r += `Result: clamp(0, 100)\n`;
    r += `\`\`\`\n\n`;
    
    r += `### DuckDB Storage Layer\n\n`;
    r += `All streamed data is persisted to DuckDB (\`crypto_stream_data.duckdb\`) with 3 tables:\n`;
    r += `- **trades:** timestamp, exchange, symbol, source, price, amount, side, trade_id\n`;
    r += `- **orderbook:** timestamp, exchange, symbol, source, best_bid, best_ask, bid_depth, ask_depth, spread\n`;
    r += `- **tickers:** timestamp, exchange, symbol, source, last_price, bid, ask, high_24h, low_24h, base_volume, quote_volume, change_pct\n`;
    r += `- Buffer flush: every 10 seconds, batch inserts up to 50,000 rows per flush\n\n`;
    
    r += `### Dashboard (9 Tabs)\n\n`;
    r += `| Tab | Name | Content |\n`;
    r += `|-----|------|---------|\n`;
    r += `| 0 | Hybrid Flow | Hero stats, data flow diagram, throughput chart, top 15 exchanges, data type distribution, winner analysis, summary table |\n`;
    r += `| 1 | Per Exchange | Expandable cards per exchange showing all pair data across 4 methods (H-TR, H-OB, N-TR, N-OB, P-TR, P-OB, R-TR, R-OB, D-TR, D-OB) |\n`;
    r += `| 2 | Per Pair | Aggregated view of each trading pair across all exchanges |\n`;
    r += `| 3 | Dedup Analysis | Deduplication rates per exchange, stacked chart, efficiency metrics |\n`;
    r += `| 4 | Analytics | Method distribution pie, top 10 exchanges bar, tier breakdown, error categories |\n`;
    r += `| 5 | Correlation | Trade ID cross-method matching rates (N↔P, N↔R, P↔R, N↔D) per exchange |\n`;
    r += `| 6 | Health | Sub-tabs: Overview (health grid with SVG score circles), Errors (per-category), Fixes (recommendations), Connections (status + last message times) |\n`;
    r += `| 7 | Sub Manager | Connection pools, subscriptions sent, batches, stale reconnects, forced reconnects, failover rotations per exchange |\n`;
    r += `| 8 | Live Events | Real-time SSE event stream (connect/disconnect/error events, last 500) |\n\n`;

    // ═══════════════════════════════════════════════════════
    // PER-EXCHANGE STREAMING ARCHITECTURE
    // ═══════════════════════════════════════════════════════
    r += `---\n\n`;
    r += `## 🔌 Per-Exchange Streaming Architecture\n\n`;
    r += `### Exchange Connection Summary\n\n`;
    r += `| # | Exchange | Tier | Native Type | CCXT ID | WS Endpoints | Compression | Ping | Failover URLs |\n`;
    r += `|---|----------|------|-------------|---------|-------------|-------------|------|---------------|\n`;
    
    const exchangeStreamInfo = {
        'Binance':      { type:'ws', ccxt:'binance', ws:'stream.binance.com:9443', comp:'none', ping:'none', failover:3 },
        'Coinbase':     { type:'ws', ccxt:'coinbase', ws:'ws-feed.exchange.coinbase.com', comp:'none', ping:'none', failover:0 },
        'Kraken':       { type:'ws', ccxt:'kraken', ws:'ws.kraken.com/v2', comp:'none', ping:'JSON {method:ping} 25s', failover:0 },
        'KuCoin':       { type:'ws (dynamic token)', ccxt:'kucoin', ws:'Dynamic (bullet-public API)', comp:'none', ping:'{type:ping} 18s', failover:2 },
        'OKX':          { type:'ws', ccxt:'okx', ws:'ws.okx.com:8443', comp:'none', ping:'string "ping" 25s', failover:3 },
        'Bybit':        { type:'ws', ccxt:'bybit', ws:'stream.bybit.com/v5/public/spot', comp:'none', ping:'{op:ping} 20s', failover:2 },
        'Bitfinex':     { type:'ws', ccxt:'bitfinex', ws:'api-pub.bitfinex.com/ws/2', comp:'none', ping:'{event:ping} 25s', failover:2 },
        'Gate.io':      { type:'ws', ccxt:'gateio', ws:'api.gateio.ws/ws/v4/', comp:'none', ping:'{channel:spot.ping} 15s', failover:3 },
        'HTX':          { type:'ws', ccxt:'htx', ws:'api.huobi.pro/ws', comp:'**gzip**', ping:'server-push {ping}→{pong}', failover:3 },
        'WOO X':        { type:'ws', ccxt:'woo', ws:'wss.woo.org/ws/stream', comp:'none', ping:'{event:ping} 9s', failover:0 },
        'Crypto.com':   { type:'ws', ccxt:'cryptocom', ws:'stream.crypto.com/exchange/v1/market', comp:'none', ping:'{method:public/heartbeat} 25s', failover:0 },
        'Bitstamp':     { type:'ws', ccxt:'bitstamp', ws:'ws.bitstamp.net', comp:'none', ping:'{event:bts:heartbeat} 20s', failover:0 },
        'WhiteBIT':     { type:'ws', ccxt:'whitebit', ws:'api.whitebit.com/ws', comp:'none', ping:'{method:server.ping} 25s', failover:0 },
        'AscendEX':     { type:'ws', ccxt:'ascendex', ws:'ascendex.com/1/api/pro/v1/stream', comp:'none', ping:'{op:ping} 15s', failover:0 },
        'BingX':        { type:'ws', ccxt:'bingx', ws:'open-api-ws.bingx.com/market', comp:'**gzip**', ping:'string "Pong" 5s', failover:0 },
        'Toobit':       { type:'ws', ccxt:'toobit', ws:'stream.toobit.com/quote/ws/v1', comp:'none', ping:'{ping:timestamp} 15s', failover:2 },
        'Deepcoin':     { type:'ws', ccxt:'deepcoin¹', ws:'stream.deepcoin.com', comp:'none', ping:'string "ping" 15s', failover:0 },
        'XT.com':       { type:'ws', ccxt:'xt', ws:'stream.xt.com/public', comp:'none', ping:'string "ping" 15s', failover:3 },
        'Zoomex':       { type:'ws', ccxt:'—', ws:'stream.zoomex.com/v5/public/spot', comp:'none', ping:'{op:ping} 20s', failover:0 },
        'Bitget':       { type:'ws', ccxt:'bitget', ws:'ws.bitget.com/v2/ws/public', comp:'none', ping:'string "ping" 30s', failover:0 },
        'Gemini':       { type:'ws', ccxt:'gemini', ws:'api.gemini.com/v2/marketdata', comp:'none', ping:'{type:heartbeat} 30s', failover:2 },
        'Binance.US':   { type:'ws', ccxt:'binanceus', ws:'stream.binance.us:9443/stream', comp:'none', ping:'none', failover:2 },
        'MEXC':         { type:'rest', ccxt:'mexc', ws:'— (REST only)', comp:'none', ping:'—', failover:0 },
        'CoinEx':       { type:'ws', ccxt:'coinex', ws:'socket.coinex.com/v2/spot', comp:'**gzip**', ping:'{method:server.ping} 15s', failover:0 },
        'LBank':        { type:'ws', ccxt:'lbank', ws:'www.lbkex.net/ws/V2/', comp:'none', ping:'server-push {action:ping}', failover:3 },
        'BitMart':      { type:'ws', ccxt:'bitmart', ws:'ws-manager-compress.bitmart.com', comp:'**inflate**', ping:'string "ping" 10s', failover:0 },
        'Pionex':       { type:'ws', ccxt:'—', ws:'ws.pionex.com/wsPub', comp:'none', ping:'server-push {op:PING}', failover:0 },
        'Poloniex':     { type:'ws', ccxt:'poloniex', ws:'ws.poloniex.com/ws/public', comp:'none', ping:'{event:ping} 20s', failover:2 },
        'HitBTC':       { type:'ws', ccxt:'hitbtc', ws:'api.hitbtc.com/api/3/ws/public', comp:'none', ping:'{method:server.ping} 20s', failover:0 },
        'BTSE':         { type:'ws', ccxt:'—', ws:'ws.btse.com/ws/spot', comp:'none', ping:'string "ping" 30s', failover:0 },
        'Biconomy':     { type:'ws', ccxt:'—', ws:'bei.biconomy.com/ws', comp:'none', ping:'{method:server.ping} 30s', failover:0 },
        'Hotcoin':      { type:'ws+rest', ccxt:'—', ws:'wss.hotcoinfin.com/trade/multiple', comp:'**gzip**', ping:'server-push {ping}→{pong}', failover:0 },
        'NovaEx':       { type:'ws', ccxt:'—', ws:'wss.woox.io/ws/stream', comp:'none', ping:'{event:ping} 9s', failover:0 },
        'FameEX':       { type:'ws', ccxt:'—', ws:'wsapi.fameex.com/v1/ws/stream/public', comp:'none', ping:'server-push {ping}→{pong}', failover:0 },
        'Websea':       { type:'ws', ccxt:'—', ws:'oapi.websea.com/ws/v1/spot/market', comp:'none', ping:'server-push {op:ping}', failover:0 },
        'Bullish':      { type:'ws', ccxt:'bullish', ws:'api.exchange.bullish.com (JSON-RPC)', comp:'none', ping:'server-push {type:ping}', failover:0 },
        'Darkex':       { type:'ws', ccxt:'—', ws:'ws.darkex.com/kline-api/ws', comp:'**gzip**', ping:'server-push {ping}→{pong}', failover:0 },
        'Bitrue':       { type:'ws', ccxt:'bitrue', ws:'ws.bitrue.com/market/ws', comp:'**gzip**', ping:'server-push {event:ping}', failover:0 },
        'BloFin':       { type:'ws', ccxt:'blofin', ws:'openapi.blofin.com/ws/public', comp:'none', ping:'string "ping" 25s', failover:1 },
        'DigiFinex':    { type:'ws', ccxt:'digifinex¹', ws:'openapi.digifinex.com/ws/v1/', comp:'**inflate**', ping:'{method:server.ping} 30s', failover:0 },
        'EXMO':         { type:'ws', ccxt:'exmo', ws:'ws-api.exmo.com/v1/public', comp:'none', ping:'{method:ping} 20s', failover:1 },
        'CEX.IO':       { type:'rest', ccxt:'cex¹', ws:'— (REST only)', comp:'none', ping:'—', failover:0 },
        'OrangeX':      { type:'rest', ccxt:'—', ws:'— (REST only)', comp:'none', ping:'—', failover:0 },
        'Azbit':        { type:'rest', ccxt:'—', ws:'— (REST only)', comp:'none', ping:'—', failover:0 },
        'BVOX':         { type:'rest', ccxt:'—', ws:'— (REST only)', comp:'none', ping:'—', failover:0 },
        'Trubit Pro':   { type:'rest', ccxt:'—', ws:'— (REST only)', comp:'none', ping:'—', failover:0 },
        'BigONE':       { type:'rest', ccxt:'bigone', ws:'— (REST only)', comp:'none', ping:'—', failover:0 },
        'LATOKEN':      { type:'rest', ccxt:'latoken', ws:'— (REST only)', comp:'none', ping:'—', failover:0 },
        'Coinstore':    { type:'ws', ccxt:'—', ws:'ws.coinstore.com/s/ws', comp:'none', ping:'server-push {ping}→{pong}', failover:0 },
        'GroveX':       { type:'ws', ccxt:'—', ws:'ws.grovex.io/kline-api/ws', comp:'**gzip**', ping:'server-push {ping}→{pong}', failover:0 },
        'CoinW':        { type:'rest', ccxt:'—', ws:'— (REST only)', comp:'none', ping:'—', failover:0 },
        'Batonex':      { type:'rest', ccxt:'—', ws:'— (REST only)', comp:'none', ping:'—', failover:0 },
        'CEEX':         { type:'ws', ccxt:'—', ws:'wsapi.ceex.com/openapi/quote/ws/v1', comp:'none', ping:'string ping/server-push', failover:0 },
    };
    
    let exIdx = 0;
    for (const ex of EXCHANGES) {
        exIdx++;
        const info = exchangeStreamInfo[ex.name] || {};
        r += `| ${exIdx} | ${ex.name} | T${ex.tier} | ${info.type||ex.nativeType} | ${info.ccxt||ex.ccxtId||'—'} | ${info.ws||'—'} | ${info.comp||'none'} | ${info.ping||'—'} | ${info.failover||0} |\n`;
    }
    r += `\n¹ = skipPro (CCXT Pro disabled, REST-only via CCXT)\n\n`;
    
    r += `### Compression Protocols\n\n`;
    r += `| Protocol | Exchanges | How It Works |\n`;
    r += `|----------|-----------|-------------|\n`;
    r += `| **gzip** | HTX, BingX, CoinEx, Hotcoin, Darkex, Bitrue, GroveX | Server sends gzip-compressed binary frames. Client decompresses with \`zlib.gunzipSync()\` before JSON parsing. |\n`;
    r += `| **inflate** | BitMart, DigiFinex | Server sends raw deflate-compressed frames. Client decompresses with \`zlib.inflateRawSync()\` before JSON parsing. |\n`;
    r += `| **none** | All others (44 exchanges) | Plain text WebSocket frames, parsed directly as JSON. |\n\n`;
    
    r += `### Ping/Keep-Alive Mechanisms\n\n`;
    r += `| Type | Count | Exchanges | Mechanism |\n`;
    r += `|------|-------|-----------|----------|\n`;
    r += `| Client-sent JSON ping | 18 | Kraken, KuCoin, AscendEX, WhiteBIT, CoinEx, HitBTC, EXMO, Crypto.com, etc. | Client sends JSON \`{method:'ping'}\` at configured interval |\n`;
    r += `| Client-sent string ping | 8 | OKX, BingX, BitMart, Bitget, XT.com, Deepcoin, BloFin, BTSE | Client sends literal string \`'ping'\` or \`'Pong'\` |\n`;
    r += `| Server-push ping/pong | 9 | HTX, LBank, Pionex, FameEX, Hotcoin, Darkex, Bitrue, GroveX, Coinstore | Server sends \`{ping:...}\`, client must respond with \`{pong:...}\` |\n`;
    r += `| None (native heartbeat) | 4 | Binance, Binance.US, Coinbase (protocol-level) | WebSocket protocol-level ping/pong only |\n\n`;

    r += `### Failover URL Map\n\n`;
    r += `| Exchange | # URLs | Endpoints |\n`;
    r += `|----------|--------|----------|\n`;
    for (const [key, urls] of Object.entries(FAILOVER_URLS)) {
        if (key === 'default' || !urls.length) continue;
        const displayName = Object.entries(FAILOVER_KEY_MAP).find(([,v])=>v===key)?.[0] || key;
        r += `| ${displayName} | ${urls.length} | ${urls.join(', ')} |\n`;
    }
    r += `\n**Failover Logic:** On connection close, timeout, or stale detection → rotate to next URL in list → max 5 rotations per minute per exchange → exponential backoff between retries.\n\n`;
    
    r += `### Exchange Tier Classification\n\n`;
    r += `| Tier | Description | Count | Exchanges |\n`;
    r += `|------|------------|-------|----------|\n`;
    const t1 = EXCHANGES.filter(e=>e.tier===1).map(e=>e.name);
    const t2 = EXCHANGES.filter(e=>e.tier===2).map(e=>e.name);
    const t3 = EXCHANGES.filter(e=>e.tier===3).map(e=>e.name);
    r += `| **T1** | Top-tier, highest volume, most reliable | ${t1.length} | ${t1.join(', ')} |\n`;
    r += `| **T2** | Mid-tier, established exchanges | ${t2.length} | ${t2.join(', ')} |\n`;
    r += `| **T3** | Smaller exchanges, REST-heavy | ${t3.length} | ${t3.join(', ')} |\n\n`;
    
    r += `### Batch Launch Order\n\n`;
    r += `Exchanges are launched in 6 sequential batches with 3-second gaps between batches to avoid overwhelming the network:\n\n`;
    for (let b=1; b<=6; b++) {
        const batch = EXCHANGES.filter(e=>e.batch===b);
        r += `- **Batch ${b}:** ${batch.map(e=>e.name).join(', ')}\n`;
    }
    r += `\n`;

    // ═══ Enhancements Applied ═══
    r += `---\n\n`;
    r += `## ⚙️ Enhancements Applied (v9 → v9.5)\n\n`;
    r += `| Feature | Description |\n|---------|-------------|\n`;
    r += `| Subscription Manager | Per-exchange WS limits, batched subscription sending, stale connection monitoring |\n`;
    r += `| Batched Subscriptions | Send subscriptions in small batches (4-50 subs/batch) with 100-400ms delays between batches |\n`;
    r += `| Stale Monitor | Force reconnect if no data received for 45-60s (per-exchange configurable) |\n`;
    r += `| Connection Pool | Track all WS connections per exchange, pool health monitoring |\n`;
    r += `| Per-Exchange Limits | Official + safe max subs documented for all 53 exchanges (Groups A/B/C) |\n`;
    r += `| Reconnect | Exponential backoff (1s→30s) + random jitter (30% of base) |\n`;
    r += `| Failover URLs | Multi-endpoint for ${Object.keys(FAILOVER_URLS).filter(k=>k!=='default'&&FAILOVER_URLS[k].length>0).length} exchanges with URL rotation on timeout/stale/error (v9.1) |\n`;
    r += `| Emit Priority | Hybrid source priority: Native > CCXT Pro > CCXT REST > Direct REST (v9.1) |\n`;
    r += `| REST Fallback | Auto-snapshot via REST when WS silent >10s |\n`;
    r += `| Deduplication | Trade ID dedup for 7+ exchanges (Binance, KuCoin, OKX, Gate.io, etc.) |\n`;
    r += `| OB Validation | Sequence/checksum validation for Binance, Binance.US, OKX |\n`;
    r += `| Health Metrics | Real-time health scoring + error classification + fix recommendations |\n`;
    r += `| OB Correlation | Orderbook ID/nonce/sequence matching across methods |\n`;
    r += `| Error Hardening | CCXT Pro with per-error-type recovery (conn close, timeout, typeErr) |\n`;
    r += `| Direct REST | 4th stream: raw HTTP polling without CCXT abstraction |\n`;
    r += `| CCXT Pre-Load | All CCXT markets loaded sequentially before test starts (no cold-start delay) |\n\n`;

    // ─── SUBSCRIPTION MANAGER STATS ───
    r += `## 📡 Subscription Manager Report\n\n`;
    r += `### Per-Exchange WS Limits Configuration\n\n`;
    r += `| Exchange | Group | Official Max | Safe Max | Max Conns | Batch Size | Batch Delay | Stale Timeout |\n`;
    r += `|----------|-------|-------------|----------|-----------|------------|-------------|---------------|\n`;
    for (const [exName, lim] of Object.entries(EXCHANGE_WS_LIMITS)) {
        if (exName === '_default') continue;
        r += `| ${exName} | ${lim.group} | ${lim.officialMax} | ${lim.safeMax} | ${lim.maxConns} | ${lim.batchSize} | ${lim.batchDelay}ms | ${lim.staleTimeout/1000}s |\n`;
    }
    r += `\n### Subscription Manager Runtime Stats\n\n`;
    r += `| Exchange | Pool Conns | Total Subs | Stale Reconnects | Forced Reconnects | Failover Rotations | Batches Sent |\n`;
    r += `|----------|-----------|------------|-----------------|-------------------|-------------------|-------------|\n`;
    const totalStaleReconns = Object.values(smStats).reduce((a,s)=>a+s.staleReconnects,0);
    const totalForcedReconns = Object.values(smStats).reduce((a,s)=>a+s.forcedReconnects,0);
    const totalBatchesSent = Object.values(smStats).reduce((a,s)=>a+s.batchesSent,0);
    const totalFailoverRotations = Object.values(smStats).reduce((a,s)=>a+(s.failoverRotations||0),0);
    for (const [exName, sm] of Object.entries(smStats)) {
        r += `| ${exName} | ${sm.connections} | ${sm.totalSubs} | ${sm.staleReconnects} | ${sm.forcedReconnects} | ${sm.failoverRotations||0} | ${sm.batchesSent} |\n`;
    }
    r += `\n**Summary:** ${totalStaleReconns} total stale reconnects, ${totalForcedReconns} forced reconnects, ${totalFailoverRotations} failover rotations, ${totalBatchesSent} subscription batches sent\n\n`;

    r += `## Data Enrichment Summary\n\n`;
    r += `| Metric | Count |\n|--------|-------|\n`;
    r += `| Total Normalized | ${enrichStats.normalized.toLocaleString()} |\n`;
    r += `| Duplicates Dropped | ${enrichStats.deduped.toLocaleString()} |\n`;
    r += `| OB Validated (seq) | ${enrichStats.validated.toLocaleString()} |\n`;
    r += `| Stale OB Dropped | ${enrichStats.staleOB.toLocaleString()} |\n`;
    r += `| REST Fallbacks | ${enrichStats.restFallbacks.toLocaleString()} |\n`;
    r += `| Hybrid Combined Trades | ${data.hybridSummary.trades.toLocaleString()} |\n`;
    r += `| Hybrid Combined OB | ${data.hybridSummary.orderbook.toLocaleString()} |\n`;
    r += `| Hybrid Deduped (cross-method) | ${data.hybridSummary.deduped.toLocaleString()} |\n\n`;

    // 4-WAY Summary Table
    r += `## 4-Way Summary Table\n\n`;
    r += `| # | Exchange | Tier | Native | CCXT Pro | CCXT REST | Direct REST | Health | Winner | Margin |\n`;
    r += `|---|----------|------|--------|----------|-----------|-------------|--------|--------|--------|\n`;
    const names = Object.keys(data.exchanges);
    for (let i=0;i<names.length;i++) {
        const name=names[i]; const x=data.exchanges[name]; const n=x.native,p=x.ccxtPro,cr=x.ccxtRest,dr=x.directRest;
        let w;
        if(x.winner==='failed')w='❌ Failed';
        else if(x.winner==='native-only')w='🟢 Native Only';
        else if(x.winner==='native')w='✅ Native';
        else if(x.winner==='ccxtPro')w='🔵 CCXT Pro';
        else if(x.winner==='ccxtRest')w='🟠 CCXT REST';
        else if(x.winner==='directRest')w='🟣 Direct REST';
        else w='≈ Tie';
        const hs = x.health.score >= 80 ? `🟢${x.health.score}` : x.health.score >= 50 ? `🟡${x.health.score}` : `🔴${x.health.score}`;
        r += `| ${i+1} | ${name} | T${x.tier} | **${n.total.toLocaleString()}** | **${p.total.toLocaleString()}** | **${cr.total.toLocaleString()}** | **${(dr?.total||0).toLocaleString()}** | ${hs} | ${w} | ${x.margin} |\n`;
    }

    // Detailed breakdown
    r += `\n## Detailed Breakdown (Trades + Orderbook + Tickers)\n\n`;
    r += `| # | Exchange | N-TR | N-OB | Pro-TR | Pro-OB | Pro-TK | REST-TR | REST-OB | REST-TK | D-TR | D-OB | D-TK | Best |\n`;
    r += `|---|----------|------|------|--------|--------|--------|---------|---------|---------|------|------|------|------|\n`;
    for (let i=0;i<names.length;i++) {
        const name=names[i]; const x=data.exchanges[name]; const dr=x.directRest||{};
        let best = x.winner==='native-only'?'Native Only':x.winner==='failed'?'None':x.winner==='native'?'Native':x.winner==='ccxtPro'?'Pro':x.winner==='ccxtRest'?'REST':x.winner==='directRest'?'Direct':'Tie';
        r += `| ${i+1} | ${name} | ${x.native.trades.toLocaleString()} | ${x.native.orderbook.toLocaleString()} | ${x.ccxtPro.trades.toLocaleString()} | ${x.ccxtPro.orderbook.toLocaleString()} | ${(x.ccxtPro.tickers||0).toLocaleString()} | ${x.ccxtRest.trades.toLocaleString()} | ${x.ccxtRest.orderbook.toLocaleString()} | ${(x.ccxtRest.tickers||0).toLocaleString()} | ${(dr.trades||0).toLocaleString()} | ${(dr.orderbook||0).toLocaleString()} | ${(dr.tickers||0).toLocaleString()} | ${best} |\n`;
    }

    const sm = data.summary;
    r += `\n## Score Summary\n\n| Category | Count |\n|----------|-------|\n`;
    r += `| ✅ Native Wins | ${sm.nativeWins} |\n`;
    r += `| 🔵 CCXT Pro Wins | ${sm.ccxtProWins} |\n`;
    r += `| 🟠 CCXT REST Wins | ${sm.ccxtRestWins} |\n`;
    r += `| 🟣 Direct REST Wins | ${sm.directRestWins} |\n`;
    r += `| ≈ Ties | ${sm.ties} |\n`;
    r += `| 🟢 Native Only (no CCXT) | ${sm.nativeOnly} |\n`;
    r += `| ❌ All Failed | ${sm.bothFailed} |\n`;
    r += `| **Total** | **${names.length}** |\n`;

    // Health section
    r += `\n## 🏥 Health Analysis\n\n`;
    r += `**Average Health Score:** ${data.healthSummary.avgScore}/100\n\n`;
    if (data.healthSummary.critical.length) r += `**🔴 Critical (< 50):** ${data.healthSummary.critical.join(', ')}\n\n`;
    if (data.healthSummary.warnings.length) r += `**🟡 Warnings (50-79):** ${data.healthSummary.warnings.join(', ')}\n\n`;
    r += `| Exchange | Health | Errors | Top Error Category | Fix Recommendation |\n`;
    r += `|----------|--------|--------|-------------------|--------------------|\n`;
    for (const name of names) {
        const x = data.exchanges[name];
        const topErr = Object.entries(x.health.errors).sort((a,b)=>b[1]-a[1])[0];
        const topFix = x.health.fixes[0];
        const hs = x.health.score >= 80 ? `🟢 ${x.health.score}` : x.health.score >= 50 ? `🟡 ${x.health.score}` : `🔴 ${x.health.score}`;
        r += `| ${name} | ${hs} | ${x.health.totalErrors} | ${topErr?`${topErr[0]}(${topErr[1]})`:'-'} | ${topFix?`[${topFix.priority}] ${topFix.fix}`:'-'} |\n`;
    }

    r += `\n## Per-Exchange Per-Pair Detail\n\n`;
    for (const name of names) {
        const x = data.exchanges[name];
        const allPairs = [...new Set([...Object.keys(x.native.pairs),...Object.keys(x.ccxtPro.pairs),...Object.keys(x.ccxtRest.pairs),...Object.keys(x.directRest?.pairs||{})])].sort();
        if (!allPairs.length) continue;
        r += `### ${name}${x.ccxtId?` (CCXT: ${x.ccxtId})`:' (no CCXT)'} — Health: ${x.health.score}/100\n\n`;
        r += `| Pair | N-TR | N-OB | Pro-TR | Pro-OB | REST-TR | REST-OB | D-TR | D-OB | Winner |\n`;
        r += `|------|------|------|--------|--------|---------|---------|------|------|--------|\n`;
        for (const p of allPairs) {
            const np=x.native.pairs[p]||{trades:0,ob:0}, pp=x.ccxtPro.pairs[p]||{trades:0,ob:0}, rp=x.ccxtRest.pairs[p]||{trades:0,ob:0}, dp=x.directRest?.pairs[p]||{trades:0,ob:0};
            const nT=np.trades+np.ob, pT=pp.trades+pp.ob, rT=rp.trades+rp.ob, dT=(dp.trades||0)+(dp.ob||0);
            const best = Math.max(nT,pT,rT,dT);
            let w = best===0?'❌':nT===best?(nT>pT*1.2&&nT>rT*1.2&&nT>dT*1.2?'✅ Native':'≈ Tie'):pT===best?'🔵 Pro':dT===best?'🟣 Direct':(rT>nT*1.2?'🟠 REST':'≈ Tie');
            r += `| ${p} | ${np.trades} | ${np.ob} | ${pp.trades} | ${pp.ob} | ${rp.trades} | ${rp.ob} | ${dp.trades||0} | ${dp.ob||0} | ${w} |\n`;
        }
        if (x.native.errors.length) r += `\n**Native Errors:** ${x.native.errors.slice(0,3).join('; ')}\n`;
        if (x.ccxtPro.errors.length) r += `\n**CCXT Pro Errors:** ${x.ccxtPro.errors.slice(0,3).join('; ')}\n`;
        if (x.ccxtRest.errors.length) r += `\n**CCXT REST Errors:** ${x.ccxtRest.errors.slice(0,3).join('; ')}\n`;
        if (x.directRest?.errors?.length) r += `\n**Direct REST Errors:** ${x.directRest.errors.slice(0,3).join('; ')}\n`;
        if (x.health.fixes.length) r += `\n**Fix Recommendations:** ${x.health.fixes.map(f=>`[${f.priority}] ${f.fix}`).join('; ')}\n`;
        r += `\n`;
    }

    // Reliability section
    r += `## 🔬 Reliability Analysis\n\n`;
    r += `| Exchange | Health | Reconn | REST FB | Dedup | Native | Pro | REST | Direct |\n`;
    r += `|----------|--------|--------|---------|-------|--------|-----|------|--------|\n`;
    for (const name of names) {
        const x=data.exchanges[name]; const s=stats[name];
        const hs = x.health.score >= 80 ? '🟢' : x.health.score >= 50 ? '🟡' : '🔴';
        r += `| ${name} | ${hs}${x.health.score} | ${s?.native?.reconnects||0} | ${s?.native?.restFallbacks||0} | ${s?.native?.deduped||0} | ${x.native.total.toLocaleString()} | ${x.ccxtPro.total.toLocaleString()} | ${x.ccxtRest.total.toLocaleString()} | ${(x.directRest?.total||0).toLocaleString()} |\n`;
    }

    // Recommendations
    r += `\n## 🔧 Recommendations\n\n`;
    r += `| Exchange | Method | Reason | Health Fix |\n|----------|--------|--------|------------|\n`;
    for (const name of names) {
        const x=data.exchanges[name];
        const fix=x.health.fixes[0]?.fix||'-';
        if (x.winner==='native-only') r+=`| ${name} | Native | Only method available | ${fix} |\n`;
        else if (x.winner==='native') r+=`| ${name} | Native | Highest throughput (${x.margin}) | ${fix} |\n`;
        else if (x.winner==='ccxtPro') r+=`| ${name} | CCXT Pro | Best push streaming (${x.margin}) | ${fix} |\n`;
        else if (x.winner==='ccxtRest') r+=`| ${name} | CCXT REST | Most reliable polling (${x.margin}) | ${fix} |\n`;
        else if (x.winner==='directRest') r+=`| ${name} | Direct REST | Best raw HTTP (${x.margin}) | ${fix} |\n`;
        else if (x.winner==='tie') r+=`| ${name} | Hybrid | Similar throughput | ${fix} |\n`;
        else r+=`| ${name} | Investigate | Underperforming | ${fix} |\n`;
    }

    r += `\n## Conclusion (v9.3, 4-Method + Subscription Manager, ${MINUTES}min test)\n\n`;
    r += `4-method parallel comparison across ${names.length} exchanges with Subscription Manager:\n\n`;
    r += `- **Native Wins:** ${sm.nativeWins} | **CCXT Pro Wins:** ${sm.ccxtProWins} | **CCXT REST Wins:** ${sm.ccxtRestWins} | **Direct REST Wins:** ${sm.directRestWins} | **Ties:** ${sm.ties}\n`;
    r += `- **Native-Only:** ${sm.nativeOnly} exchanges (no CCXT support)\n`;
    r += `- **All Failed:** ${sm.bothFailed} exchanges\n`;
    r += `- **Health:** Avg ${data.healthSummary.avgScore}/100 | ${data.healthSummary.critical.length} critical | ${data.healthSummary.warnings.length} warnings\n`;
    r += `- **Data Quality:** ${enrichStats.deduped} dupes dropped, ${enrichStats.validated} OB validated, ${enrichStats.restFallbacks} REST fallbacks\n\n`;

    const totalN = Object.values(data.exchanges).reduce((a,x)=>a+x.native.total,0);
    const totalP = Object.values(data.exchanges).reduce((a,x)=>a+x.ccxtPro.total,0);
    const totalR = Object.values(data.exchanges).reduce((a,x)=>a+x.ccxtRest.total,0);
    const totalD = Object.values(data.exchanges).reduce((a,x)=>a+(x.directRest?.total||0),0);
    r += `### Aggregate Throughput\n\n`;
    r += `| Method | Total Messages | Rate (msg/min) |\n|--------|---------------|----------------|\n`;
    r += `| Native | ${totalN.toLocaleString()} | ${Math.round(totalN/(data.uptime/60)).toLocaleString()} |\n`;
    r += `| CCXT Pro | ${totalP.toLocaleString()} | ${Math.round(totalP/(data.uptime/60)).toLocaleString()} |\n`;
    r += `| CCXT REST | ${totalR.toLocaleString()} | ${Math.round(totalR/(data.uptime/60)).toLocaleString()} |\n`;
    r += `| Direct REST | ${totalD.toLocaleString()} | ${Math.round(totalD/(data.uptime/60)).toLocaleString()} |\n`;
    r += `| **Combined** | **${(totalN+totalP+totalR+totalD).toLocaleString()}** | **${Math.round((totalN+totalP+totalR+totalD)/(data.uptime/60)).toLocaleString()}** |\n`;
    const hT = data.hybridSummary;
    r += `| **Hybrid (deduped)** | **${hT.total.toLocaleString()}** | **${Math.round(hT.total/(data.uptime/60)).toLocaleString()}** |\n`;
    r += `\n**Hybrid Dedup:** ${hT.deduped.toLocaleString()} cross-method duplicates removed (${(totalN+totalP+totalR+totalD) > 0 ? Math.round(hT.deduped/(totalN+totalP+totalR+totalD)*100) : 0}% of raw)\n`;

    r += `\n**Verdict:** ${sm.nativeWins > sm.ccxtProWins + sm.ccxtRestWins ? 'Native-first' : sm.ccxtProWins > sm.nativeWins ? 'CCXT Pro-first' : 'Hybrid'} architecture recommended. `;
    r += `Native handles ${sm.nativeWins+sm.nativeOnly}/${names.length}, CCXT Pro handles ${sm.ccxtProWins}/${names.length}, CCXT REST handles ${sm.ccxtRestWins}/${names.length}, Direct REST handles ${sm.directRestWins}/${names.length} exchanges optimally.\n`;

    return r;
}

// ═══════════════════ MAIN (v9 — Subscription Manager Launch) ═══════════════════
async function main() {
    console.log('╔═══════════════════════════════════════════════════════════════════════════╗');
    console.log(`║  ENHANCED 4-METHOD v9.5 — ${MINUTES}-MINUTE Test + Subscription Manager              ║`);
    console.log('║  All 53 exchanges × 4 methods × Trades + OrderBook + Tickers              ║');
    console.log('║  Subscription Manager: Batched subs, stale monitor, connection pools       ║');
    console.log(`║  Dashboard: http://localhost:${HTTP_PORT}                                           ║`);
    console.log('╚═══════════════════════════════════════════════════════════════════════════╝');

    server.listen(HTTP_PORT, () => {
        console.log(`\n  🌐 Dashboard: http://localhost:${HTTP_PORT}`);
        console.log(`  📊 API: http://localhost:${HTTP_PORT}/api/stats`);
        console.log(`  🏥 Health: http://localhost:${HTTP_PORT}/api/health`);
        console.log(`  📡 SSE: http://localhost:${HTTP_PORT}/api/events`);
        console.log(`  ⏱ Duration: ${MINUTES} minutes`);
        console.log(`  📋 Subscription Manager: ${Object.keys(EXCHANGE_WS_LIMITS).length - 1} exchange configs loaded\n`);
    });

    for (const ex of EXCHANGES) initStats(ex.name, ex.tier, ex.nativeType, ex.ccxtId, true);

    // Pre-load all CCXT markets sequentially BEFORE starting the test
    await preloadAllMarkets(EXCHANGES);

    // Reset timer — test starts NOW, after pre-load is complete
    startTime = Date.now();

    for (let b=1; b<=6; b++) {
        const batch = EXCHANGES.filter(e=>e.batch===b);
        if (!batch.length) continue;
        console.log(`  🚀 Batch ${b}: ${batch.map(e=>e.name).join(', ')}`);
        for (const ex of batch) {
            // Method 1: Native WS/REST
            try { await Promise.resolve(ex.startNative()); } catch(e) { addErr(ex.name,'native',e.message); }
            // Method 2: CCXT Pro (watch* streaming) — skip if Pro broken for this exchange
            if (ex.ccxtId && ccxt.pro?.[ex.ccxtId] && !ex.skipPro) {
                startCCXTPro(ex.name, ex.ccxtId, ex.ccxtPairs).catch(()=>{});
            }
            // Method 3: CCXT REST (fetch* polling) — markets already pre-loaded
            if (ex.ccxtId && ex.ccxtPairs?.length) {
                startCCXTRest(ex.name, ex.ccxtId, ex.ccxtPairs).catch(()=>{});
            }
            // Method 4: Direct REST (raw HTTP polling — no CCXT library)
            startDirectRest(ex.name).catch(()=>{});
        }
        if (b<6) await sleep(3000);
    }

    console.log(`\n  ✅ All ${EXCHANGES.length} exchanges launched with 4 methods. Test running for ${MINUTES} minutes...`);
    console.log(`  🌐 Open http://localhost:${HTTP_PORT} to view live dashboard`);
    if (duckEnabled) console.log(`  💾 DuckDB storage active — flushing every 10s`);
    console.log('');

    // DuckDB flush timer
    const duckFlushTimer = duckEnabled ? setInterval(flushDuckDB, 10000) : null;

    const ticker = setInterval(() => {
        const elapsed = Math.floor((Date.now()-startTime)/1000);
        const mins=Math.floor(elapsed/60), secs=elapsed%60;
        const rem=Math.max(0,Math.floor(TEST_DURATION/1000)-elapsed);
        const remMin=Math.floor(rem/60), remSec=rem%60;
        let totalN=0,totalP=0,totalR=0,totalD=0,active=0;
        for(const s of Object.values(stats)){
            totalN+=s.native.trades+s.native.orderbook;
            totalP+=s.ccxtPro.trades+s.ccxtPro.orderbook;
            totalR+=s.ccxtRest.trades+s.ccxtRest.orderbook;
            totalD+=(s.directRest?.trades||0)+(s.directRest?.orderbook||0);
            if(s.native.trades+s.native.orderbook>0||s.ccxtPro.trades+s.ccxtPro.orderbook>0||s.ccxtRest.trades+s.ccxtRest.orderbook>0||(s.directRest?.trades||0)+(s.directRest?.orderbook||0)>0)active++;
        }
        // Health summary
        let hScores=0,hCount=0;
        for(const n of Object.keys(stats)){const hm=healthMetrics[n];if(hm){hScores+=calcHealthScore(n);hCount++;}}
        const avgH=hCount?Math.round(hScores/hCount):100;
        const hT=Object.values(hybridStats).reduce((a,h)=>a+h.trades+h.orderbook+h.tickers,0);
        const hD=Object.values(hybridStats).reduce((a,h)=>a+h.deduped,0);
        process.stdout.write(`\r  ⏱ ${mins}m${secs}s / ${MINUTES}m | Rem: ${remMin}m${remSec}s | Active: ${active}/${EXCHANGES.length} | N:${totalN.toLocaleString()} P:${totalP.toLocaleString()} R:${totalR.toLocaleString()} D:${totalD.toLocaleString()} | Hybrid:${hT.toLocaleString()} (-${hD}) | H:${avgH}   `);
    }, 10000);

    await sleep(TEST_DURATION);
    console.log(`\n\n  ⏹ ${MINUTES}-minute test complete. Stopping...`);
    stopFlag = true;
    clearInterval(ticker);
    if (duckFlushTimer) clearInterval(duckFlushTimer);
    await sleep(5000);

    // Final DuckDB flush
    if (duckEnabled) {
        flushDuckDB();
        console.log(`  💾 DuckDB final flush: ${duckBuffers.trades.length} trades remaining`);
        try {
            duckConn.all('SELECT COUNT(*) as trades FROM trades; SELECT COUNT(*) as obs FROM orderbook; SELECT COUNT(*) as tks FROM tickers', (err, rows) => {
                if (!err && rows) console.log(`  💾 DuckDB totals:`, rows);
            });
        } catch {}
    }

    console.log('  📝 Generating report...');
    const report = generateReport();
    fs.writeFileSync(REPORT_FILE, report);
    console.log(`  📄 Report saved to ${REPORT_FILE}`);

    const data = buildStats();
    const totalN = Object.values(data.exchanges).reduce((a,x)=>a+x.native.total,0);
    const totalP = Object.values(data.exchanges).reduce((a,x)=>a+x.ccxtPro.total,0);
    const totalR = Object.values(data.exchanges).reduce((a,x)=>a+x.ccxtRest.total,0);
    const totalD = Object.values(data.exchanges).reduce((a,x)=>a+(x.directRest?.total||0),0);
    const smFinal = getSubManagerStats();
    const totalStale = Object.values(smFinal).reduce((a,s)=>a+s.staleReconnects,0);
    const totalForced = Object.values(smFinal).reduce((a,s)=>a+s.forcedReconnects,0);
    const totalBatches = Object.values(smFinal).reduce((a,s)=>a+s.batchesSent,0);
    console.log(`\n  ═══════════ FINAL RESULTS (4-METHOD v9 + SUBSCRIPTION MANAGER) ═══════════`);
    console.log(`  Native:      ${totalN.toLocaleString()} msgs (Wins: ${data.summary.nativeWins})`);
    console.log(`  CCXT Pro:    ${totalP.toLocaleString()} msgs (Wins: ${data.summary.ccxtProWins})`);
    console.log(`  CCXT REST:   ${totalR.toLocaleString()} msgs (Wins: ${data.summary.ccxtRestWins})`);
    console.log(`  Direct REST: ${totalD.toLocaleString()} msgs (Wins: ${data.summary.directRestWins})`);
    console.log(`  ────────────────────────────────────────────────`);
    console.log(`  HYBRID:      ${data.hybridSummary.total.toLocaleString()} unique msgs (${data.hybridSummary.deduped.toLocaleString()} dupes removed)`);
    console.log(`    Trades:    ${data.hybridSummary.trades.toLocaleString()} | OB: ${data.hybridSummary.orderbook.toLocaleString()} | Tickers: ${data.hybridSummary.tickers.toLocaleString()}`);
    console.log(`  ────────────────────────────────────────────────`);
    const totalFailovers = Object.values(smFinal).reduce((a,s)=>a+(s.failoverRotations||0),0);
    console.log(`  📋 Sub Manager: ${totalBatches} batches | ${totalStale} stale | ${totalForced} forced | ${totalFailovers} failovers`);
    console.log(`  ────────────────────────────────────────────────`);
    console.log(`  Ties:        ${data.summary.ties}`);
    console.log(`  Native-Only: ${data.summary.nativeOnly}`);
    console.log(`  All Failed:  ${data.summary.bothFailed}`);
    console.log(`  Health Avg:  ${data.healthSummary.avgScore}/100 (${data.healthSummary.critical.length} critical, ${data.healthSummary.warnings.length} warnings)`);
    console.log(`  Deduped: ${enrichStats.deduped} | Validated: ${enrichStats.validated} | REST FB: ${enrichStats.restFallbacks}`);
    const totalTk = Object.values(data.exchanges).reduce((a,x)=>a+(x.ccxtPro.tickers||0)+(x.ccxtRest.tickers||0)+(x.directRest?.tickers||0),0);
    console.log(`  Tickers: ${totalTk.toLocaleString()} | DuckDB: ${duckEnabled?'✅ active':'❌ disabled'}`);
    console.log(`  ════════════════════════════════════════════════\n`);

    // Close DuckDB
    if (duckEnabled) {
        try { duckDB.close(); console.log('  💾 DuckDB closed.'); } catch {}
    }

    server.close();
    process.exit(0);
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
