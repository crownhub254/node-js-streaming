/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║   15-MINUTE PARALLEL STREAM TEST — All 48 Exchanges Fixed               ║
 * ║   BTC/ETH/SOL × USD/USDT/USDC/DAI → DuckDB + Dashboard (port 3000)    ║
 * ║   All exchanges launch in parallel, not batches                         ║
 * ║   Fixes: AscendEX URL, Bullish OB, CEX.IO SOL, FameEX heartbeat, etc  ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */

const WebSocket = require('ws');
const zlib = require('zlib');
const https = require('https');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { DuckDBInstance } = require('@duckdb/node-api');
const ccxt = require('ccxt');

// ======================== CONFIGURATION ========================
const TEST_DURATION = 15 * 60 * 1000;   // 15 minutes
const FLUSH_INTERVAL = 10000;            // Flush to DuckDB every 10s
const RECONNECT_BASE = 3000;             // Base reconnect delay
const RECONNECT_MAX = 60000;             // Max reconnect delay (60s)
const CONN_TIMEOUT = 15000;              // WS handshake timeout
const REST_POLL_INTERVAL = 10000;        // REST poll every 10s
const HTTP_PORT = 3000;                  // Dashboard port
const STAGGER_MS = 150;                  // Stagger connections slightly

// ======================== STATE ========================
let tradeBuffer = [];
let obBuffer = [];
const stats = {};
const sseClients = [];
let running = false;
let startTime = null;
let shuttingDown = false;
const activeTimers = new Set();
const activeWsList = new Set();
let dbConn = null;
let totalFlushed = 0;

// ======================== UTILITIES ========================
function httpsRequest(url, options = {}) {
    return new Promise((resolve, reject) => {
        const u = new URL(url);
        const opts = { hostname: u.hostname, port: u.port || 443, path: u.pathname + u.search,
            method: options.method || 'GET', timeout: 10000,
            headers: { 'User-Agent': 'Mozilla/5.0', 'Content-Type': 'application/json', ...(options.headers || {}) } };
        const req = https.request(opts, (res) => {
            let data = ''; res.on('data', c => data += c);
            res.on('end', () => { try { resolve(JSON.parse(data)); } catch (e) { resolve(data); } });
        });
        req.setTimeout(10000, () => { req.destroy(); reject(new Error('Request timeout')); });
        req.on('error', reject);
        if (options.body) req.write(JSON.stringify(options.body));
        req.end();
    });
}

function ts() { return new Date().toISOString().substring(11, 19); }
function nowISO() { return new Date().toISOString(); }

// ======================== SYMBOL MAP ========================
const SYMBOL_MAP = {};

function registerSymbol(exchange, exchangeSymbol, base, quote) {
    SYMBOL_MAP[`${exchange}:${exchangeSymbol}`] = { base, quote, canonical: `${base}_${quote}` };
}

function resolveSymbol(exchange, exchangeSymbol) {
    return SYMBOL_MAP[`${exchange}:${exchangeSymbol}`] || null;
}

function buildSymbolMap() {
    const map = {
        'Binance': { 'BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','SOLUSDT':'SOL_USDT','BTCUSDC':'BTC_USDC','ETHUSDC':'ETH_USDC','SOLUSDC':'SOL_USDC' },
        'Coinbase': { 'BTC-USD':'BTC_USD','ETH-USD':'ETH_USD','SOL-USD':'SOL_USD' },
        'Kraken': { 'BTC/USDT':'BTC_USDT','ETH/USDT':'ETH_USDT','SOL/USDT':'SOL_USDT','BTC/USDC':'BTC_USDC','ETH/USDC':'ETH_USDC','SOL/USDC':'SOL_USDC','BTC/USD':'BTC_USD','ETH/USD':'ETH_USD','SOL/USD':'SOL_USD' },
        'KuCoin': { 'BTC-USDT':'BTC_USDT','ETH-USDT':'ETH_USDT','SOL-USDT':'SOL_USDT' },
        'OKX': { 'BTC-USDT':'BTC_USDT','ETH-USDT':'ETH_USDT','SOL-USDT':'SOL_USDT','BTC-USDC':'BTC_USDC','ETH-USDC':'ETH_USDC','SOL-USDC':'SOL_USDC' },
        'Bybit': { 'BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','SOLUSDT':'SOL_USDT','BTCUSDC':'BTC_USDC','ETHUSDC':'ETH_USDC','SOLUSDC':'SOL_USDC' },
        'Bitfinex': { 'tBTCUSD':'BTC_USD','tETHUSD':'ETH_USD','tSOLUSD':'SOL_USD' },
        'Gate.io': { 'BTC_USDT':'BTC_USDT','ETH_USDT':'ETH_USDT','SOL_USDT':'SOL_USDT' },
        'HTX': { 'btcusdt':'BTC_USDT','ethusdt':'ETH_USDT','solusdt':'SOL_USDT' },
        'WOO X': { 'SPOT_BTC_USDT':'BTC_USDT','SPOT_ETH_USDT':'ETH_USDT','SPOT_SOL_USDT':'SOL_USDT' },
        'Crypto.com': { 'BTC_USDT':'BTC_USDT','ETH_USDT':'ETH_USDT','SOL_USDT':'SOL_USDT','BTC_USDC':'BTC_USDC','ETH_USDC':'ETH_USDC','SOL_USDC':'SOL_USDC' },
        'Bitstamp': { 'btcusd':'BTC_USD','ethusd':'ETH_USD','solusd':'SOL_USD' },
        'WhiteBIT': { 'BTC_USDT':'BTC_USDT','ETH_USDT':'ETH_USDT','SOL_USDT':'SOL_USDT' },
        'AscendEX': { 'BTC/USDT':'BTC_USDT','ETH/USDT':'ETH_USDT','SOL/USDT':'SOL_USDT','BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','SOLUSDT':'SOL_USDT' },
        'BingX': { 'BTC-USDT':'BTC_USDT','ETH-USDT':'ETH_USDT','SOL-USDT':'SOL_USDT' },
        'Toobit': { 'BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','SOLUSDT':'SOL_USDT' },
        'Deepcoin': { 'BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','SOLUSDT':'SOL_USDT','DeepCoin_BTCUSDT':'BTC_USDT','DeepCoin_ETHUSDT':'ETH_USDT','DeepCoin_SOLUSDT':'SOL_USDT','BTC-USDT':'BTC_USDT','ETH-USDT':'ETH_USDT','SOL-USDT':'SOL_USDT' },
        'XT.com': { 'btc_usdt':'BTC_USDT','eth_usdt':'ETH_USDT','sol_usdt':'SOL_USDT' },
        'Zoomex': { 'BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','SOLUSDT':'SOL_USDT' },
        'LBank': { 'btc_usdt':'BTC_USDT','eth_usdt':'ETH_USDT','sol_usdt':'SOL_USDT' },
        'BitMart': { 'BTC_USDT':'BTC_USDT','ETH_USDT':'ETH_USDT','SOL_USDT':'SOL_USDT' },
        'Pionex': { 'BTC_USDT':'BTC_USDT','ETH_USDT':'ETH_USDT','SOL_USDT':'SOL_USDT' },
        'Poloniex': { 'BTC_USDT':'BTC_USDT','ETH_USDT':'ETH_USDT','SOL_USDT':'SOL_USDT' },
        'BTSE': { 'BTC-USD':'BTC_USD','ETH-USD':'ETH_USD','SOL-USD':'SOL_USD','BTC-USD_0':'BTC_USD','ETH-USD_0':'ETH_USD','SOL-USD_0':'SOL_USD' },
        'HitBTC': { 'BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','SOLUSDT':'SOL_USDT' },
        'Biconomy': { 'BTC_USDT':'BTC_USDT','ETH_USDT':'ETH_USDT','SOL_USDT':'SOL_USDT' },
        'Hotcoin': { 'btc_usdt':'BTC_USDT','eth_usdt':'ETH_USDT','sol_usdt':'SOL_USDT' },
        'NovaEx': { 'SPOT_BTC_USDT':'BTC_USDT','SPOT_ETH_USDT':'ETH_USDT','SPOT_SOL_USDT':'SOL_USDT' },
        'FameEX': { 'BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','SOLUSDT':'SOL_USDT','BTC_USDT':'BTC_USDT','ETH_USDT':'ETH_USDT','SOL_USDT':'SOL_USDT','btcusdt':'BTC_USDT','ethusdt':'ETH_USDT','solusdt':'SOL_USDT' },
        'Websea': { 'BTC-USDT':'BTC_USDT','ETH-USDT':'ETH_USDT','SOL-USDT':'SOL_USDT' },
        'Bullish': { 'BTCUSDC':'BTC_USDC','ETHUSDC':'ETH_USDC','SOLUSDC':'SOL_USDC','BTC_USDC':'BTC_USDC','ETH_USDC':'ETH_USDC','SOL_USDC':'SOL_USDC' },
        'Darkex': { 'BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','btcusdt':'BTC_USDT','ethusdt':'ETH_USDT' },
        'Bitrue': { 'BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','SOLUSDT':'SOL_USDT' },
        'BloFin': { 'BTC-USDT':'BTC_USDT','ETH-USDT':'ETH_USDT','SOL-USDT':'SOL_USDT' },
        'OrangeX': { 'BTC-USDT':'BTC_USDT','ETH-USDT':'ETH_USDT','SOL-USDT':'SOL_USDT' },
        'Azbit': { 'BTC_USDT':'BTC_USDT','ETH_USDT':'ETH_USDT','SOL_USDT':'SOL_USDT' },
        'BVOX': { 'BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','SOLUSDT':'SOL_USDT' },
        'Trubit Pro': { 'BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','SOLUSDT':'SOL_USDT' },
        'Bitget': { 'BTC/USDT':'BTC_USDT','ETH/USDT':'ETH_USDT','SOL/USDT':'SOL_USDT' },
        'MEXC': { 'BTC/USDT':'BTC_USDT','ETH/USDT':'ETH_USDT','SOL/USDT':'SOL_USDT' },
        'Gemini': { 'BTC/USD':'BTC_USD','ETH/USD':'ETH_USD' },
        'Binance.US': { 'BTC/USDT':'BTC_USDT','ETH/USDT':'ETH_USDT','SOL/USDT':'SOL_USDT' },
        'CEX.IO': { 'BTC/USDT':'BTC_USDT','ETH/USDT':'ETH_USDT','SOL/USDT':'SOL_USDT' },
        'CoinEx': { 'BTC/USDT':'BTC_USDT','ETH/USDT':'ETH_USDT','SOL/USDT':'SOL_USDT' },
        'DigiFinex': { 'BTC/USDT':'BTC_USDT','ETH/USDT':'ETH_USDT','SOL/USDT':'SOL_USDT' },
        'BigONE': { 'BTC/USDT':'BTC_USDT','ETH/USDT':'ETH_USDT','SOL/USDT':'SOL_USDT' },
        'EXMO': { 'BTC/USDT':'BTC_USDT','ETH/USDT':'ETH_USDT','SOL/USDT':'SOL_USDT','BTC/DAI':'BTC_DAI' },
        'LATOKEN': { 'BTC/USDT':'BTC_USDT','ETH/USDT':'ETH_USDT','SOL/USDT':'SOL_USDT' },
    };
    for (const [exchange, symbols] of Object.entries(map)) {
        for (const [exSym, canonical] of Object.entries(symbols)) {
            const [base, quote] = canonical.split('_');
            registerSymbol(exchange, exSym, base, quote);
        }
    }
}

// ======================== DATA RECORDING ========================
function initStats(exchange) {
    stats[exchange] = { trades: 0, orderbook: 0, status: 'pending', errors: [], reconnects: 0, type: 'ws', tier: 1 };
}

function recordTrade(exchange, exchangeSymbol, price, qty, side, timestamp) {
    const mapping = resolveSymbol(exchange, exchangeSymbol);
    if (!mapping) return;
    tradeBuffer.push({
        ts: timestamp || nowISO(), exchange,
        canonical_pair: mapping.canonical,
        price: parseFloat(price) || 0, qty: parseFloat(qty) || 0,
        side: side || null
    });
    stats[exchange].trades++;
    if (stats[exchange].status !== 'ok') stats[exchange].status = 'ok';
}

function recordOrderbook(exchange, exchangeSymbol, bids, asks, timestamp) {
    const mapping = resolveSymbol(exchange, exchangeSymbol);
    if (!mapping) return;
    obBuffer.push({
        ts: timestamp || nowISO(), exchange,
        canonical_pair: mapping.canonical,
        bids: JSON.stringify((bids || []).slice(0, 5)),
        asks: JSON.stringify((asks || []).slice(0, 5))
    });
    stats[exchange].orderbook++;
    if (stats[exchange].status !== 'ok') stats[exchange].status = 'ok';
}

// ======================== SSE ========================
function sseEvent(type, exchange, message) {
    const evt = { type, exchange, message, time: ts() };
    const data = `data: ${JSON.stringify(evt)}\n\n`;
    for (let i = sseClients.length - 1; i >= 0; i--) {
        try { sseClients[i].write(data); } catch (e) { sseClients.splice(i, 1); }
    }
}

// ═══════════════════════════════════════════════════════════════════
// ═══════════════════════ EXCHANGE DEFINITIONS ═════════════════════
// ═══════════════════════════════════════════════════════════════════

const EXCHANGES = {

    // ═══════════════════════ TIER 1 ═══════════════════════

    'Binance': {
        tier: 1,
        getUrl: () => 'wss://stream.binance.com:9443/stream',
        onOpen: (ws) => {
            ws.send(JSON.stringify({ method: 'SUBSCRIBE', params: [
                'btcusdt@trade','ethusdt@trade','solusdt@trade',
                'btcusdt@depth5@1000ms','ethusdt@depth5@1000ms','solusdt@depth5@1000ms'
            ], id: 1 }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.data) return;
                const e = d.data;
                if (e.e === 'trade') recordTrade('Binance', e.s, e.p, e.q, e.m ? 'sell' : 'buy');
                if (e.lastUpdateId && e.bids && e.asks) {
                    const sym = (d.stream || '').split('@')[0].toUpperCase();
                    recordOrderbook('Binance', sym, e.bids, e.asks);
                }
            } catch (e) {}
        }
    },

    'Coinbase': {
        tier: 1, lowVolumeTrades: true,
        getUrl: () => 'wss://ws-feed.exchange.coinbase.com',
        onOpen: (ws) => {
            ws.send(JSON.stringify({ type: 'subscribe', product_ids: ['BTC-USD','ETH-USD','SOL-USD'],
                channels: ['matches','level2_batch','heartbeat'] }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                const p = d.product_id; if (!p) return;
                if (d.type === 'match' || d.type === 'last_match') recordTrade('Coinbase', p, d.price, d.size, d.side);
                if (d.type === 'snapshot') recordOrderbook('Coinbase', p, d.bids, d.asks);
                if (d.type === 'l2update' && d.changes) {
                    const bids = d.changes.filter(c => c[0] === 'buy').map(c => [c[1], c[2]]);
                    const asks = d.changes.filter(c => c[0] === 'sell').map(c => [c[1], c[2]]);
                    if (bids.length || asks.length) recordOrderbook('Coinbase', p, bids, asks);
                }
            } catch (e) {}
        }
    },

    'Kraken': {
        tier: 1, lowVolumeTrades: true,
        getUrl: () => 'wss://ws.kraken.com/v2',
        onOpen: (ws) => {
            ws.send(JSON.stringify({ method:'subscribe', params:{ channel:'trade', symbol:['BTC/USDT','ETH/USDT','SOL/USDT'], snapshot:false } }));
            ws.send(JSON.stringify({ method:'subscribe', params:{ channel:'book', symbol:['BTC/USDT','ETH/USDT','SOL/USDT'], depth:10, snapshot:true } }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.channel === 'heartbeat' || d.method) return;
                if (!d.channel || !d.data) return;
                if (d.channel === 'trade') { for (const t of d.data) recordTrade('Kraken', t.symbol, t.price, t.qty, t.side); }
                if (d.channel === 'book') { for (const b of d.data) recordOrderbook('Kraken', b.symbol, b.bids, b.asks); }
            } catch (e) {}
        }
    },

    'KuCoin': {
        tier: 1,
        getUrl: async () => {
            try {
                const resp = await httpsRequest('https://api.kucoin.com/api/v1/bullet-public', { method: 'POST' });
                if (resp.data?.token) {
                    const ep = resp.data.instanceServers?.[0]?.endpoint || 'wss://ws-api-spot.kucoin.com';
                    return { url: `${ep}?token=${resp.data.token}`, pingInterval: resp.data.instanceServers?.[0]?.pingInterval || 18000 };
                }
            } catch (e) {}
            return { url: 'wss://ws-api-spot.kucoin.com', pingInterval: 18000 };
        },
        pingMessage: JSON.stringify({ id: Date.now(), type: 'ping' }),
        onOpen: (ws) => {
            ws.send(JSON.stringify({ id:1, type:'subscribe', topic:'/market/match:BTC-USDT,ETH-USDT,SOL-USDT', privateChannel:false, response:true }));
            ws.send(JSON.stringify({ id:3, type:'subscribe', topic:'/spotMarket/level2Depth5:BTC-USDT,ETH-USDT,SOL-USDT', privateChannel:false, response:true }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.type === 'pong' || d.type === 'welcome' || d.type === 'ack') return;
                if (!d.topic || !d.data) return;
                const sym = d.data.symbol || d.topic.split(':').pop() || '';
                if (d.topic.includes('/market/match')) recordTrade('KuCoin', sym, d.data.price, d.data.size, d.data.side);
                if (d.topic.includes('level2Depth5')) recordOrderbook('KuCoin', sym, d.data.bids, d.data.asks);
            } catch (e) {}
        }
    },

    'OKX': {
        tier: 1,
        getUrl: () => 'wss://ws.okx.com:8443/ws/v5/public',
        pingInterval: 25000, pingMessage: 'ping',
        onOpen: (ws) => {
            const pairs = ['BTC-USDT','ETH-USDT','SOL-USDT'];
            ws.send(JSON.stringify({ op:'subscribe', args: pairs.map(p => ({ channel:'trades', instId:p })) }));
            ws.send(JSON.stringify({ op:'subscribe', args: pairs.map(p => ({ channel:'books5', instId:p })) }));
        },
        parseMessage: (msg) => {
            try {
                if (msg === 'pong') return;
                const d = JSON.parse(msg);
                if (!d.data || !d.arg) return;
                const sym = d.arg.instId;
                if (d.arg.channel === 'trades') { for (const t of d.data) recordTrade('OKX', sym, t.px, t.sz, t.side); }
                if (d.arg.channel === 'books5') { for (const b of d.data) recordOrderbook('OKX', sym, b.bids, b.asks); }
            } catch (e) {}
        }
    },

    // ═══ FIX: Bybit — V5 confirmed, improved reconnect via global backoff ═══
    'Bybit': {
        tier: 1,
        getUrl: () => 'wss://stream.bybit.com/v5/public/spot',
        pingInterval: 20000, pingMessage: JSON.stringify({ op: 'ping' }),
        onOpen: (ws) => {
            ws.send(JSON.stringify({ op:'subscribe', args: ['BTCUSDT','ETHUSDT','SOLUSDT'].flatMap(s => [`publicTrade.${s}`,`orderbook.50.${s}`]) }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.topic || !d.data) return;
                const parts = d.topic.split('.');
                const sym = parts[parts.length - 1];
                if (d.topic.startsWith('publicTrade')) {
                    for (const t of (Array.isArray(d.data) ? d.data : [d.data]))
                        recordTrade('Bybit', sym, t.p, t.v, t.S === 'Buy' ? 'buy' : 'sell');
                }
                if (d.topic.startsWith('orderbook')) recordOrderbook('Bybit', sym, d.data.b, d.data.a);
            } catch (e) {}
        }
    },

    'Bitfinex': {
        tier: 1,
        getUrl: () => 'wss://api-pub.bitfinex.com/ws/2',
        onOpen: (ws) => {
            for (const sym of ['tBTCUSD','tETHUSD','tSOLUSD']) {
                ws.send(JSON.stringify({ event:'subscribe', channel:'trades', symbol:sym }));
                ws.send(JSON.stringify({ event:'subscribe', channel:'book', symbol:sym, prec:'P0', len:25 }));
            }
        },
        parseMessage: (msg, ctx) => {
            try {
                const d = JSON.parse(msg);
                if (!ctx._bfxChannels) ctx._bfxChannels = {};
                if (d.event === 'subscribed') { ctx._bfxChannels[d.chanId] = { channel: d.channel, symbol: d.symbol || d.pair }; return; }
                if (!Array.isArray(d) || d.length < 2) return;
                const info = ctx._bfxChannels[d[0]];
                if (!info || d[1] === 'hb') return;
                const sym = info.symbol.startsWith('t') ? info.symbol : `t${info.symbol}`;
                if (info.channel === 'trades') {
                    if (d[1] === 'te' || d[1] === 'tu') {
                        const t = d[2];
                        recordTrade('Bitfinex', sym, t[3], Math.abs(t[2]), t[2] > 0 ? 'buy' : 'sell');
                    }
                }
                if (info.channel === 'book') {
                    if (Array.isArray(d[1]) && Array.isArray(d[1][0])) {
                        const bids = d[1].filter(e => e[2] > 0).map(e => [e[0], e[2]]);
                        const asks = d[1].filter(e => e[2] < 0).map(e => [e[0], Math.abs(e[2])]);
                        recordOrderbook('Bitfinex', sym, bids.slice(0, 5), asks.slice(0, 5));
                    } else if (Array.isArray(d[1])) {
                        recordOrderbook('Bitfinex', sym, [], []);
                    }
                }
            } catch (e) {}
        }
    },

    'Gate.io': {
        tier: 1,
        getUrl: () => 'wss://api.gateio.ws/ws/v4/',
        pingInterval: 15000, pingMessage: JSON.stringify({ time: Math.floor(Date.now()/1000), channel: 'spot.ping' }),
        onOpen: (ws) => {
            const t = Math.floor(Date.now()/1000);
            const pairs = ['BTC_USDT','ETH_USDT','SOL_USDT'];
            ws.send(JSON.stringify({ time:t, channel:'spot.trades', event:'subscribe', payload:pairs }));
            for (const p of pairs) ws.send(JSON.stringify({ time:t, channel:'spot.order_book', event:'subscribe', payload:[p,'5','1000ms'] }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.result || d.event === 'subscribe') return;
                if (d.channel === 'spot.trades' && d.result) {
                    recordTrade('Gate.io', d.result.currency_pair, d.result.price, d.result.amount, d.result.side);
                }
                if (d.channel === 'spot.order_book' && d.result) {
                    const b = d.result;
                    if (b.bids || b.asks) recordOrderbook('Gate.io', b.s || '', b.bids, b.asks);
                }
            } catch (e) {}
        }
    },

    'HTX': {
        tier: 1,
        getUrl: () => 'wss://api.huobi.pro/ws',
        compression: 'gzip',
        handlePing: (parsed, ws) => { if (parsed.ping) { ws.send(JSON.stringify({ pong: parsed.ping })); return true; } return false; },
        onOpen: (ws) => {
            for (const s of ['btcusdt','ethusdt','solusdt']) {
                ws.send(JSON.stringify({ sub: `market.${s}.trade.detail`, id: s+'t' }));
                ws.send(JSON.stringify({ sub: `market.${s}.depth.step0`, id: s+'d' }));
            }
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.ping || d.subbed) return;
                const ch = d.ch || '';
                const parts = ch.split('.');
                if (parts.length < 3) return;
                const sym = parts[1];
                if (ch.includes('.trade.detail') && d.tick?.data) { for (const t of d.tick.data) recordTrade('HTX', sym, t.price, t.amount, t.direction); }
                if (ch.includes('.depth') && d.tick) recordOrderbook('HTX', sym, d.tick.bids, d.tick.asks);
            } catch (e) {}
        }
    },

    'WOO X': {
        tier: 1, lowVolumeTrades: true,
        getUrl: () => 'wss://wss.woo.org/ws/stream/OqdphuyCtYWxwzhxyLLjOWNdFP7sQt8RPWzmb5xY',
        pingInterval: 9000, pingMessage: JSON.stringify({ event: 'ping' }),
        onOpen: (ws) => {
            for (const sym of ['SPOT_BTC_USDT','SPOT_ETH_USDT','SPOT_SOL_USDT']) {
                ws.send(JSON.stringify({ id: sym, event: 'subscribe', topic: `${sym}@trade` }));
                ws.send(JSON.stringify({ id: sym+'o', event: 'subscribe', topic: `${sym}@orderbook` }));
            }
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.event === 'pong' || d.event === 'subscribe') return;
                const topic = d.topic || '';
                const sym = topic.split('@')[0];
                if (!sym.startsWith('SPOT_')) return;
                if (topic.includes('@trade') && d.data) recordTrade('WOO X', sym, d.data.price, d.data.size, d.data.side?.toLowerCase());
                if (topic.includes('@orderbook') && d.data) recordOrderbook('WOO X', sym, d.data.bids, d.data.asks);
            } catch (e) {}
        }
    },

    // ═══════════════════════ TIER 2 ═══════════════════════

    'Crypto.com': {
        tier: 2,
        getUrl: () => 'wss://stream.crypto.com/exchange/v1/market',
        onOpen: (ws) => {
            // FIX: Subscribe to both USDT and USDC trades + orderbook
            const channels = ['BTC_USDT','ETH_USDT','SOL_USDT','BTC_USDC','ETH_USDC','SOL_USDC'].flatMap(p => [`trade.${p}`,`book.${p}.10`]);
            ws.send(JSON.stringify({ id:1, method:'subscribe', params:{ channels } }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.result) return;
                const ch = d.result.channel || d.result.subscription || '';
                const sym = d.result.instrument_name || ch.split('.')[1] || '';
                if (ch.startsWith('trade') && d.result.data) { for (const t of d.result.data) recordTrade('Crypto.com', sym, t.p, t.q, t.s?.toLowerCase()); }
                if (ch.startsWith('book') && d.result.data) {
                    for (const b of (Array.isArray(d.result.data) ? d.result.data : [d.result.data]))
                        recordOrderbook('Crypto.com', sym, b.bids, b.asks);
                }
            } catch (e) {}
        }
    },

    'Bitstamp': {
        tier: 2, lowVolumeTrades: true,
        getUrl: () => 'wss://ws.bitstamp.net',
        onOpen: (ws) => {
            for (const s of ['btcusd','ethusd','solusd']) {
                ws.send(JSON.stringify({ event:'bts:subscribe', data:{ channel:`live_trades_${s}` } }));
                ws.send(JSON.stringify({ event:'bts:subscribe', data:{ channel:`order_book_${s}` } }));
            }
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.channel || !d.data) return;
                const ch = d.channel;
                if (ch.startsWith('live_trades_')) recordTrade('Bitstamp', ch.replace('live_trades_', ''), d.data.price, d.data.amount, d.data.type === 0 ? 'buy' : 'sell');
                if (ch.startsWith('order_book_')) recordOrderbook('Bitstamp', ch.replace('order_book_', ''), d.data.bids, d.data.asks);
            } catch (e) {}
        }
    },

    'WhiteBIT': {
        tier: 2,
        getUrl: () => 'wss://api.whitebit.com/ws',
        pingInterval: 25000, pingMessage: JSON.stringify({ id:0, method:'server.ping', params:[] }),
        onOpen: (ws) => {
            ws.send(JSON.stringify({ id:1, method:'trades_subscribe', params:['BTC_USDT','ETH_USDT','SOL_USDT'] }));
            ws.send(JSON.stringify({ id:2, method:'depth_subscribe', params:['BTC_USDT',100,'0',true] }));
            ws.send(JSON.stringify({ id:3, method:'depth_subscribe', params:['ETH_USDT',100,'0',true] }));
            ws.send(JSON.stringify({ id:4, method:'depth_subscribe', params:['SOL_USDT',100,'0',true] }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.method === 'trades_update' && d.params) {
                    const sym = d.params[0];
                    if (Array.isArray(d.params[1])) { for (const t of d.params[1]) recordTrade('WhiteBIT', sym, t.price, t.amount, t.type); }
                }
                if (d.method === 'depth_update' && d.params) {
                    const data = d.params[1]; const sym = d.params[2] || '';
                    recordOrderbook('WhiteBIT', sym, data.bids ? Object.entries(data.bids) : [], data.asks ? Object.entries(data.asks) : []);
                }
            } catch (e) {}
        }
    },

    // ═══ FIX: AscendEX — Use /0/ for public (no account-group needed) ═══
    'AscendEX': {
        tier: 2,
        getUrl: () => 'wss://ascendex.com/0/api/pro/v1/stream',
        pingInterval: 15000, pingMessage: JSON.stringify({ op: 'ping' }),
        onOpen: (ws) => {
            // FIX: Subscribe per symbol with id fields
            for (const sym of ['BTC/USDT','ETH/USDT','SOL/USDT']) {
                ws.send(JSON.stringify({ op:'sub', id:`trades_${sym}`, ch:`trades:${sym}` }));
                ws.send(JSON.stringify({ op:'sub', id:`depth_${sym}`, ch:`depth:${sym}` }));
            }
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.m) return;
                if (d.m === 'trades' && d.data) { for (const t of d.data) recordTrade('AscendEX', d.symbol, t.p, t.q, t.bm ? 'sell' : 'buy'); }
                if ((d.m === 'depth' || d.m === 'depth-snapshot') && d.data) recordOrderbook('AscendEX', d.symbol, d.data.bids, d.data.asks);
            } catch (e) {}
        }
    },

    'BingX': {
        tier: 2,
        getUrl: () => 'wss://open-api-ws.bingx.com/market',
        compression: 'gzip',
        pingInterval: 5000, pingMessage: 'Pong',
        onOpen: (ws) => {
            for (const p of ['BTC-USDT','ETH-USDT','SOL-USDT']) {
                ws.send(JSON.stringify({ id: p, reqType: 'sub', dataType: `${p}@trade` }));
                ws.send(JSON.stringify({ id: p+'d', reqType: 'sub', dataType: `${p}@depth5` }));
            }
        },
        parseMessage: (msg) => {
            try {
                if (msg === 'Ping') return;
                const d = JSON.parse(msg);
                if (!d.dataType) return;
                const sym = d.dataType.split('@')[0];
                if (d.dataType.includes('@trade') && d.data) {
                    for (const t of (Array.isArray(d.data) ? d.data : [d.data])) recordTrade('BingX', sym, t.p, t.q, t.m ? 'sell' : 'buy');
                }
                if (d.dataType.includes('@depth') && d.data) recordOrderbook('BingX', sym, d.data.bids, d.data.asks);
            } catch (e) {}
        }
    },

    'Toobit': {
        tier: 2,
        getUrl: () => 'wss://stream.toobit.com/quote/ws/v1',
        handlePing: (parsed, ws) => { if (parsed.ping) { ws.send(JSON.stringify({ pong: parsed.ping })); return true; } return false; },
        customPingSetup: (ws) => setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ ping: Date.now() })); }, 30000),
        onOpen: (ws) => {
            for (const s of ['BTCUSDT','ETHUSDT','SOLUSDT']) {
                ws.send(JSON.stringify({ symbol:s, topic:'trade', event:'sub', params:{ binary:false } }));
                ws.send(JSON.stringify({ symbol:s, topic:'depth', event:'sub', params:{ binary:false } }));
            }
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.pong) return;
                const sym = d.symbol || d.symbolName || '';
                if (!sym) return;
                if (d.topic === 'trade' && d.data) { for (const t of d.data) recordTrade('Toobit', sym, t.p, t.q, t.m ? 'sell' : 'buy'); }
                if (d.topic === 'depth' && d.data) { for (const b of d.data) recordOrderbook('Toobit', sym, b.b, b.a); }
            } catch (e) {}
        }
    },

    'Deepcoin': {
        tier: 2,
        getUrl: () => 'wss://stream.deepcoin.com/streamlet/trade/public/spot?platform=api',
        pingInterval: 15000, pingMessage: 'ping',
        onOpen: (ws) => {
            const reqId = Date.now();
            ['BTCUSDT','ETHUSDT','SOLUSDT'].forEach((s, i) => {
                ws.send(JSON.stringify({ SendTopicAction: { Action: '1', FilterValue: `DeepCoin_${s}`, LocalNo: reqId + i, ResumeNo: -2, TopicID: '2' } }));
            });
        },
        customPingSetup: (ws) => {
            const poll = async () => {
                for (const sym of ['BTC-USDT','ETH-USDT','SOL-USDT']) {
                    try {
                        const resp = await httpsRequest(`https://api.deepcoin.com/deepcoin/market/books?instId=${sym}&sz=5`);
                        if (resp?.code === '0' && resp.data) recordOrderbook('Deepcoin', sym, resp.data.bids, resp.data.asks);
                    } catch(e) {}
                }
            };
            poll(); return setInterval(poll, REST_POLL_INTERVAL);
        },
        parseMessage: (msg) => {
            try {
                if (msg === 'pong') return;
                const d = JSON.parse(msg);
                if (d.a === 'PMT' && d.r) { for (const row of d.r) { const inst = row.d?.I || ''; recordTrade('Deepcoin', inst.replace('DeepCoin_',''), row.d?.P, row.d?.V, row.d?.S === '2' ? 'sell' : 'buy'); } }
            } catch (e) {}
        }
    },

    'XT.com': {
        tier: 2,
        getUrl: () => 'wss://stream.xt.com/public',
        pingInterval: 15000, pingMessage: 'ping',
        onOpen: (ws) => {
            for (const s of ['btc_usdt','eth_usdt','sol_usdt'])
                ws.send(JSON.stringify({ method:'subscribe', params:[`depth_update@${s}`,`trade@${s}`] }));
        },
        parseMessage: (msg) => {
            try {
                if (msg === 'pong') return;
                const d = JSON.parse(msg);
                const topic = d.event || d.topic || '';
                if (!topic.includes('@')) return;
                const sym = topic.split('@')[1] || '';
                if (topic.startsWith('trade') && d.data) { for (const t of (Array.isArray(d.data) ? d.data : [d.data])) recordTrade('XT.com', sym, t.p, t.q, t.b ? 'buy' : 'sell'); }
                if (topic.includes('depth') && d.data) recordOrderbook('XT.com', sym, d.data.b || d.data.bids, d.data.a || d.data.asks);
            } catch (e) {}
        }
    },

    'Zoomex': {
        tier: 2,
        getUrl: () => 'wss://stream.zoomex.com/v5/public/spot',
        pingInterval: 20000, pingMessage: JSON.stringify({ op: 'ping' }),
        onOpen: (ws) => {
            ws.send(JSON.stringify({ op:'subscribe', args: ['BTCUSDT','ETHUSDT','SOLUSDT'].flatMap(s => [`orderbook.50.${s}`,`publicTrade.${s}`]) }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.topic || !d.data) return;
                const parts = d.topic.split('.'); const sym = parts[parts.length - 1];
                if (d.topic.startsWith('publicTrade')) { for (const t of (Array.isArray(d.data) ? d.data : [d.data])) recordTrade('Zoomex', sym, t.p, t.v, t.S === 'Buy' ? 'buy' : 'sell'); }
                if (d.topic.startsWith('orderbook')) recordOrderbook('Zoomex', sym, d.data.b, d.data.a);
            } catch (e) {}
        }
    },

    // ═══════════════════════ TIER 3 ═══════════════════════

    'LBank': {
        tier: 3,
        getUrl: () => 'wss://www.lbkex.net/ws/V2/',
        pingInterval: 30000, pingMessage: JSON.stringify({ action:'ping', ping:'ping' }),
        onOpen: (ws) => {
            for (const s of ['btc_usdt','eth_usdt','sol_usdt']) {
                ws.send(JSON.stringify({ action:'subscribe', subscribe:'trade', pair:s }));
                ws.send(JSON.stringify({ action:'subscribe', subscribe:'depth', pair:s, depth:10 }));
            }
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.action === 'ping' || d.type === 'pong') return;
                const sym = d.pair || '';
                if (d.trade) recordTrade('LBank', sym, d.trade.price, d.trade.volume, d.trade.direction);
                if (d.type === 'trade' && d.data) recordTrade('LBank', sym, d.data.price, d.data.volume, d.data.direction);
                if (d.depth) recordOrderbook('LBank', sym, d.depth.bids, d.depth.asks);
                if (d.type === 'depth' && d.data) recordOrderbook('LBank', sym, d.data.bids, d.data.asks);
            } catch (e) {}
        }
    },

    'BitMart': {
        tier: 3,
        getUrl: () => 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1',
        compression: 'inflate',
        pingInterval: 10000, pingMessage: 'ping',
        onOpen: (ws) => {
            ws.send(JSON.stringify({ op:'subscribe', args: ['BTC_USDT','ETH_USDT','SOL_USDT'].flatMap(p => [`spot/trade:${p}`,`spot/depth5:${p}`]) }));
        },
        parseMessage: (msg) => {
            try {
                if (msg === 'pong') return;
                const d = JSON.parse(msg);
                if (!d.data) return;
                const table = d.table || '';
                if (table === 'spot/trade') { for (const t of d.data) recordTrade('BitMart', t.symbol, t.price, t.size || t.s_q, t.side); }
                if (table === 'spot/depth5') { for (const b of d.data) recordOrderbook('BitMart', b.symbol, b.bids, b.asks); }
            } catch (e) {}
        }
    },

    'Pionex': {
        tier: 3,
        getUrl: () => 'wss://ws.pionex.com/wsPub',
        pingInterval: 15000, pingMessage: JSON.stringify({ op: 'PONG' }),
        handlePing: (parsed, ws) => { if (parsed.op === 'PING') { ws.send(JSON.stringify({ op: 'PONG' })); return true; } return false; },
        onOpen: (ws) => {
            for (const s of ['BTC_USDT','ETH_USDT','SOL_USDT']) {
                ws.send(JSON.stringify({ op:'SUBSCRIBE', topic:'TRADE', symbol:s }));
                ws.send(JSON.stringify({ op:'SUBSCRIBE', topic:'DEPTH', symbol:s, limit:10 }));
            }
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.topic) return;
                const sym = d.symbol || '';
                if (d.topic === 'TRADE' && d.data) { for (const t of (Array.isArray(d.data) ? d.data : [d.data])) recordTrade('Pionex', sym, t.price, t.size, t.side?.toLowerCase()); }
                if (d.topic === 'DEPTH' && d.data) recordOrderbook('Pionex', sym, d.data.bids, d.data.asks);
            } catch (e) {}
        }
    },

    'Poloniex': {
        tier: 3,
        getUrl: () => 'wss://ws.poloniex.com/ws/public',
        pingInterval: 30000, pingMessage: JSON.stringify({ event: 'ping' }),
        onOpen: (ws) => {
            ws.send(JSON.stringify({ event:'subscribe', channel:['trades'], symbols:['BTC_USDT','ETH_USDT','SOL_USDT'] }));
            ws.send(JSON.stringify({ event:'subscribe', channel:['book'], symbols:['BTC_USDT','ETH_USDT','SOL_USDT'] }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.channel || !d.data) return;
                const items = Array.isArray(d.data) ? d.data : [d.data];
                for (const item of items) {
                    const sym = item.symbol || '';
                    if (d.channel === 'trades') recordTrade('Poloniex', sym, item.price, item.quantity, item.takerSide);
                    if (d.channel === 'book') recordOrderbook('Poloniex', sym, item.bids, item.asks);
                }
            } catch (e) {}
        }
    },

    'BTSE': {
        tier: 3,
        getUrl: () => 'wss://ws.btse.com/ws/spot',
        ossUrl: 'wss://ws.btse.com/ws/oss/spot',
        pingInterval: 30000, pingMessage: 'ping',
        onOpen: (ws) => { ws.send(JSON.stringify({ op:'subscribe', args:['tradeHistoryApi:BTC-USD','tradeHistoryApi:ETH-USD','tradeHistoryApi:SOL-USD'] })); },
        ossOnOpen: (ws) => { ws.send(JSON.stringify({ op:'subscribe', args:['update:BTC-USD_0','update:ETH-USD_0','update:SOL-USD_0'] })); },
        parseMessage: (msg) => {
            try {
                if (msg === 'pong') return;
                const d = JSON.parse(msg);
                if (d.topic?.startsWith('tradeHistoryApi') && d.data) {
                    const sym = d.topic.replace('tradeHistoryApi:','');
                    for (const t of d.data) recordTrade('BTSE', sym, t.price, t.size || t.amount, t.side?.toLowerCase());
                }
                if (d.topic?.startsWith('update:') && d.data) {
                    const sym = d.topic.replace('update:','').replace('_0','');
                    recordOrderbook('BTSE', sym, d.data.bids || d.data.buyQuote, d.data.asks || d.data.sellQuote);
                }
            } catch (e) {}
        }
    },

    'HitBTC': {
        tier: 3, lowVolumeTrades: true,
        getUrl: () => 'wss://api.hitbtc.com/api/3/ws/public',
        onOpen: (ws) => {
            const syms = ['BTCUSDT','ETHUSDT','SOLUSDT'];
            ws.send(JSON.stringify({ ch:'trades', method:'subscribe', params:{ symbols:syms }, id:1 }));
            ws.send(JSON.stringify({ ch:'orderbook/full', method:'subscribe', params:{ symbols:syms, limit:5 }, id:2 }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.ch) return;
                if (d.ch === 'trades' && d.update) { for (const [sym, trades] of Object.entries(d.update)) { for (const t of trades) recordTrade('HitBTC', sym, t.p, t.q, t.s?.toLowerCase()); } }
                if (d.ch === 'orderbook/full') {
                    for (const [sym, ob] of Object.entries(d.snapshot || d.update || {}))
                        recordOrderbook('HitBTC', sym, ob.b || ob.bid, ob.a || ob.ask);
                }
            } catch (e) {}
        }
    },

    'Biconomy': {
        tier: 3,
        getUrl: () => 'wss://bei.biconomy.com/ws',
        pingInterval: 30000, pingMessage: JSON.stringify({ method:'server.ping', params:[], id:5160 }),
        extraConnections: [
            { getUrl: () => 'wss://bei.biconomy.com/ws', onOpen: (ws) => { ws.send(JSON.stringify({ method:'depth.subscribe', params:['ETH_USDT',5,'0'], id:21 })); ws.send(JSON.stringify({ method:'deals.subscribe', params:['ETH_USDT'], id:22 })); } },
            { getUrl: () => 'wss://bei.biconomy.com/ws', onOpen: (ws) => { ws.send(JSON.stringify({ method:'depth.subscribe', params:['SOL_USDT',5,'0'], id:31 })); ws.send(JSON.stringify({ method:'deals.subscribe', params:['SOL_USDT'], id:32 })); } }
        ],
        onOpen: (ws) => {
            ws.send(JSON.stringify({ method:'depth.subscribe', params:['BTC_USDT',5,'0'], id:11 }));
            ws.send(JSON.stringify({ method:'deals.subscribe', params:['BTC_USDT'], id:12 }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.method === 'depth.update' && d.params) { const sym = d.params[2] || ''; if (d.params[1]) recordOrderbook('Biconomy', sym, d.params[1].bids, d.params[1].asks); }
                if (d.method === 'deals.update' && d.params) { const sym = d.params[0] || ''; if (Array.isArray(d.params[1])) { for (const t of d.params[1]) recordTrade('Biconomy', sym, t.price, t.amount, t.type); } }
            } catch (e) {}
        }
    },

    'Hotcoin': {
        tier: 3,
        getUrl: () => 'wss://wss.hotcoinfin.com/trade/multiple',
        compression: 'gzip',
        handlePing: (parsed, ws) => { if (parsed.ping) { ws.send(JSON.stringify({ pong: 'pong' })); return true; } return false; },
        onOpen: (ws) => {
            for (const s of ['btc_usdt','eth_usdt','sol_usdt']) {
                ws.send(JSON.stringify({ sub:`market.${s}.trade.depth`, id:s+'d' }));
                ws.send(JSON.stringify({ sub:`market.${s}.trade.detail`, id:s+'t' }));
            }
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.ping || !d.ch || !d.data) return;
                const sym = d.ch.split('.')[1] || '';
                if (d.ch.includes('trade.detail')) { const trades = Array.isArray(d.data) ? d.data : (d.data.data || [d.data]); for (const t of trades) recordTrade('Hotcoin', sym, t.price, t.amount, t.direction); }
                if (d.ch.includes('trade.depth')) recordOrderbook('Hotcoin', sym, d.data.buys || d.data.bids, d.data.asks);
            } catch (e) {}
        }
    },

    'NovaEx': {
        tier: 3,
        getUrl: () => 'wss://wss.woox.io/ws/stream/OqdphuyIYbng-t001',
        pingInterval: 9000, pingMessage: JSON.stringify({ event: 'ping' }),
        onOpen: (ws) => {
            for (const sym of ['SPOT_BTC_USDT','SPOT_ETH_USDT','SPOT_SOL_USDT']) {
                ws.send(JSON.stringify({ id: sym+'o', event: 'subscribe', topic: `${sym}@orderbook` }));
                ws.send(JSON.stringify({ id: sym+'t', event: 'subscribe', topic: `${sym}@trade` }));
            }
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.event === 'pong' || d.event === 'subscribe') return;
                const topic = d.topic || ''; const sym = topic.split('@')[0];
                if (!sym.startsWith('SPOT_')) return;
                if (topic.includes('@trade') && d.data) recordTrade('NovaEx', sym, d.data.price, d.data.size, d.data.side?.toLowerCase());
                if (topic.includes('@orderbook') && d.data) recordOrderbook('NovaEx', sym, d.data.bids, d.data.asks);
            } catch (e) {}
        }
    },

    // ═══ FIX: FameEX — Add proper heartbeat (client ping + server pong) ═══
    'FameEX': {
        tier: 3,
        getUrl: () => 'wss://wsapi.fameex.com/v1/ws/stream/public',
        pingInterval: 20000,
        pingMessage: JSON.stringify({ op: 'ping' }),
        handlePing: (parsed, ws) => {
            if (parsed.ping !== undefined) { ws.send(JSON.stringify({ pong: parsed.ping })); return true; }
            if (parsed.op === 'ping') { ws.send(JSON.stringify({ op: 'pong' })); return true; }
            return false;
        },
        onOpen: (ws) => {
            for (const s of ['btcusdt','ethusdt','solusdt']) {
                ws.send(JSON.stringify({ event: 'sub', params: { channel: `market_${s}_trade_detail` } }));
                ws.send(JSON.stringify({ event: 'sub', params: { channel: `market_${s}_depth_step0` } }));
            }
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.ping) return;
                const ch = d.channel || '';
                if (!ch || ch === 'system') return;
                if (ch.includes('_trade') && Array.isArray(d.data)) {
                    const sym = ch.replace('market_','').replace(/_trade.*$/,'').toUpperCase();
                    for (const t of d.data) recordTrade('FameEX', sym, t.price, t.amount || t.vol, t.side);
                }
                if (ch.includes('_depth') && d.tick) {
                    const sym = (d.tick.pair || ch.replace('market_','').replace(/_depth.*$/,'')).toUpperCase();
                    recordOrderbook('FameEX', sym, d.tick.bids, d.tick.asks);
                }
            } catch (e) {}
        }
    },

    'Websea': {
        tier: 3,
        getUrl: () => 'wss://oapi.websea.com/ws/v1/spot/market',
        handlePing: (parsed, ws) => { if (parsed.op === 'ping' || parsed.ping) { ws.send(JSON.stringify({ op: 'pong' })); return true; } return false; },
        onOpen: (ws) => { for (const s of ['BTC-USDT','ETH-USDT','SOL-USDT']) ws.send(JSON.stringify({ op: 'sub', channel: 'trade', symbol: s })); },
        customPingSetup: (ws) => {
            const poll = async () => { for (const sym of ['BTC-USDT','ETH-USDT','SOL-USDT']) { try { const resp = await httpsRequest(`https://oapi.websea.com/v1/spot/depth?symbol=${sym}&size=5`); if (resp?.errno === 0 && resp.result) recordOrderbook('Websea', sym, resp.result.bids, resp.result.asks); } catch(e) {} } };
            poll(); return setInterval(poll, REST_POLL_INTERVAL);
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.op === 'sub' || d.event === 'sub' || d.status) return;
                if ((d.channel === 'trade') && d.symbol) recordTrade('Websea', d.symbol, d.price, d.amount, d.direction);
            } catch (e) {}
        }
    },

    // ═══ FIX: Bullish — OB parsing now handles all message formats ═══
    'Bullish': {
        tier: 3,
        getUrl: () => 'wss://api.exchange.bullish.com/trading-api/v1/market-data/orderbook',
        ossUrl: 'wss://api.exchange.bullish.com/trading-api/v1/market-data/trades',
        onOpen: (ws) => {
            for (const sym of ['BTCUSDC','ETHUSDC','SOLUSDC'])
                ws.send(JSON.stringify({jsonrpc:'2.0',type:'command',method:'subscribe',params:{topic:'l2Orderbook',symbol:sym},id:sym+'_ob'}));
        },
        ossOnOpen: (ws) => {
            for (const sym of ['BTCUSDC','ETHUSDC','SOLUSDC'])
                ws.send(JSON.stringify({jsonrpc:'2.0',type:'command',method:'subscribe',params:{topic:'anonymousTrades',symbol:sym},id:sym+'_t'}));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                // Skip subscription confirmations only
                if (d.result && d.result.responseCode) return;
                // Handle trades
                if (d.data && d.data.trades && d.data.symbol) {
                    for (const t of d.data.trades) recordTrade('Bullish', d.data.symbol, t.price, t.quantity, (t.side||'buy').toLowerCase());
                    return;
                }
                // Handle OB: flat array format [price, qty, price, qty, ...]
                if (d.data && d.data.bids) {
                    const sym = d.data.symbol || d.data.marketSymbol || '';
                    if (typeof d.data.bids[0] === 'string') {
                        const bids = [], asks = [];
                        for (let i = 0; i + 1 < d.data.bids.length; i += 2) bids.push([d.data.bids[i], d.data.bids[i+1]]);
                        for (let i = 0; i + 1 < (d.data.asks||[]).length; i += 2) asks.push([d.data.asks[i], d.data.asks[i+1]]);
                        recordOrderbook('Bullish', sym, bids.slice(0,5), asks.slice(0,5));
                    } else if (Array.isArray(d.data.bids)) {
                        recordOrderbook('Bullish', sym, d.data.bids.slice(0,5), (d.data.asks||[]).slice(0,5));
                    }
                    return;
                }
                // Handle nested OB format
                if (d.type === 'l2Orderbook' && d.data) {
                    const sym = d.data.symbol || '';
                    recordOrderbook('Bullish', sym, d.data.bids || [], d.data.asks || []);
                }
            } catch(e) {}
        }
    },

    'Darkex': {
        tier: 3,
        getUrl: () => 'wss://ws.darkex.com/kline-api/ws',
        compression: 'gzip',
        handlePing: (parsed, ws) => { if (parsed.ping !== undefined) { ws.send(JSON.stringify({ pong: parsed.ping })); return true; } return false; },
        onOpen: (ws) => {
            for (const s of ['btcusdt','ethusdt']) {
                ws.send(JSON.stringify({event:'sub',params:{channel:`market_${s}_depth_step0`}}));
                ws.send(JSON.stringify({event:'sub',params:{channel:`market_${s}_trade_ticker`}}));
            }
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg); if (!d.channel || !d.tick) return;
                const m = d.channel.match(/market_(\w+?)_(depth|trade)/); if (!m) return;
                const sym = m[1].toUpperCase();
                if (d.channel.includes('_depth_')) recordOrderbook('Darkex', sym, d.tick.buys || d.tick.bids, d.tick.asks);
                if (d.channel.includes('_trade_') && d.tick.data) { for (const t of d.tick.data) recordTrade('Darkex', sym, t.price, t.vol || t.amount, (t.side||'sell').toLowerCase()); }
            } catch(e) {}
        }
    },

    // ═══ FIX: Bitrue — improved OB depth subscription ═══
    'Bitrue': {
        tier: 3,
        getUrl: () => 'wss://ws.bitrue.com/market/ws',
        compression: 'gzip',
        handlePing: (parsed, ws) => { if (parsed.event === 'ping') { ws.send(JSON.stringify({ event: 'pong' })); return true; } return false; },
        onOpen: (ws) => {
            for (const sym of ['BTCUSDT','ETHUSDT','SOLUSDT'])
                ws.send(JSON.stringify({event:'sub',params:{cb_id:sym,channel:`market_${sym}_depth_step0`}}));
        },
        customPingSetup: (ws) => {
            const poll = async () => { for (const sym of ['BTCUSDT','ETHUSDT','SOLUSDT']) { try { const resp = await httpsRequest(`https://openapi.bitrue.com/api/v1/trades?symbol=${sym}&limit=5`); if (Array.isArray(resp)) { for (const t of resp) recordTrade('Bitrue', sym, t.price, t.qty, t.isBuyerMaker ? 'sell' : 'buy'); } } catch(e) {} } };
            poll(); return setInterval(poll, REST_POLL_INTERVAL);
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg); if (!d.channel || !d.tick) return;
                const sym = d.channel.replace('market_','').replace('_depth_step0','');
                recordOrderbook('Bitrue', sym, d.tick.buys || d.tick.bids, d.tick.asks);
            } catch(e) {}
        }
    },

    'BloFin': {
        tier: 3,
        getUrl: () => 'wss://openapi.blofin.com/ws/public',
        onOpen: (ws) => {
            const pairs = ['BTC-USDT','ETH-USDT','SOL-USDT'];
            ws.send(JSON.stringify({op:'subscribe',args:pairs.map(p => ({channel:'books5',instId:p}))}));
            ws.send(JSON.stringify({op:'subscribe',args:pairs.map(p => ({channel:'trades',instId:p}))}));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.event === 'subscribe' || !d.arg) return;
                const sym = d.arg.instId;
                if (d.arg.channel === 'trades' && d.data) { for (const t of (Array.isArray(d.data) ? d.data : [d.data])) recordTrade('BloFin', sym, t.price, t.size, t.side); }
                if (d.arg.channel === 'books5' && d.data) recordOrderbook('BloFin', sym, d.data.bids || d.data.b, d.data.asks || d.data.a);
            } catch(e) {}
        }
    },

    // ═══════════════ REST EXCHANGES ═══════════════
    'OrangeX': {
        tier: 3, type: 'rest', symbols: ['BTC-USDT','ETH-USDT','SOL-USDT'],
        endpoints: { orderbook: (sym) => `https://api.orangex.com/api/v1/public/get_order_book?instrument_name=${sym}-SPOT&depth=5`, trades: (sym) => `https://api.orangex.com/api/v1/public/get_last_trades_by_instrument?instrument_name=${sym}-SPOT&count=5` },
        parseOrderbook: (resp, sym) => { if (resp?.result?.bids || resp?.result?.asks) recordOrderbook('OrangeX', sym, resp.result.bids, resp.result.asks); },
        parseTrades: (resp, sym) => { if (resp?.result?.trades) { for (const t of resp.result.trades) recordTrade('OrangeX', sym, t.price, t.amount, t.direction); } }
    },
    'Azbit': {
        tier: 3, type: 'rest', symbols: ['BTC_USDT','ETH_USDT','SOL_USDT'],
        endpoints: { orderbook: (sym) => `https://data.azbit.com/api/orderbook?currencyPairCode=${sym}`, trades: (sym) => `https://data.azbit.com/api/deals?currencyPairCode=${sym}` },
        parseOrderbook: (resp, sym) => { if (Array.isArray(resp)) { const bids = resp.filter(d => d.isBid).slice(0,5).map(d => [d.price, d.amount]); const asks = resp.filter(d => !d.isBid).slice(0,5).map(d => [d.price, d.amount]); if (bids.length || asks.length) recordOrderbook('Azbit', sym, bids, asks); } },
        parseTrades: (resp, sym) => { if (Array.isArray(resp)) { for (const t of resp.slice(0,5)) recordTrade('Azbit', sym, t.price, t.volume, t.isBuy ? 'buy' : 'sell'); } }
    },
    'BVOX': {
        tier: 3, type: 'rest', symbols: ['BTCUSDT','ETHUSDT','SOLUSDT'],
        endpoints: { orderbook: (sym) => `https://api.bitvenus.me/openapi/quote/v1/depth?symbol=${sym}&limit=5`, trades: (sym) => `https://api.bitvenus.me/openapi/quote/v1/trades?symbol=${sym}&limit=5` },
        parseOrderbook: (resp, sym) => { if (resp?.bids && resp?.asks) recordOrderbook('BVOX', sym, resp.bids, resp.asks); },
        parseTrades: (resp, sym) => { if (Array.isArray(resp)) { for (const t of resp) recordTrade('BVOX', sym, t.price, t.qty, t.isBuyerMaker ? 'sell' : 'buy'); } }
    },
    'Trubit Pro': {
        tier: 3, type: 'rest', symbols: ['BTCUSDT','ETHUSDT','SOLUSDT'],
        endpoints: { orderbook: (sym) => `https://api-spot.trubit.com/openapi/quote/v1/depth?symbol=${sym}&limit=5`, trades: (sym) => `https://api-spot.trubit.com/openapi/quote/v1/trades?symbol=${sym}&limit=5` },
        parseOrderbook: (resp, sym) => { if (resp?.bids && resp?.asks) recordOrderbook('Trubit Pro', sym, resp.bids, resp.asks); },
        parseTrades: (resp, sym) => { if (Array.isArray(resp)) { for (const t of resp) recordTrade('Trubit Pro', sym, t.price, t.qty, t.isBuyerMaker ? 'sell' : 'buy'); } }
    },

    // ═══════════════ CCXT WS EXCHANGES ═══════════════
    'Bitget': { tier: 2, type: 'ccxt-ws', ccxtId: 'bitget', ccxtSymbols: ['BTC/USDT','ETH/USDT','SOL/USDT'] },
    'MEXC': { tier: 2, type: 'ccxt-ws', ccxtId: 'mexc', ccxtSymbols: ['BTC/USDT','ETH/USDT','SOL/USDT'] },
    // FIX: Gemini — only BTC/USD + ETH/USD (no SOL/USDT on Gemini)
    'Gemini': { tier: 2, type: 'ccxt-ws', ccxtId: 'gemini', ccxtSymbols: ['BTC/USD','ETH/USD'] },
    'Binance.US': { tier: 2, type: 'ccxt-ws', ccxtId: 'binanceus', ccxtSymbols: ['BTC/USDT','ETH/USDT','SOL/USDT'] },
    // FIX: CEX.IO — added SOL/USDT
    'CEX.IO': { tier: 3, type: 'ccxt-ws', ccxtId: 'cex', ccxtSymbols: ['BTC/USDT','ETH/USDT','SOL/USDT'] },
    'CoinEx': { tier: 2, type: 'ccxt-ws', ccxtId: 'coinex', ccxtSymbols: ['BTC/USDT','ETH/USDT','SOL/USDT'] },

    // ═══════════════ CCXT REST EXCHANGES ═══════════════
    'DigiFinex': { tier: 3, type: 'ccxt-rest', ccxtId: 'digifinex', ccxtSymbols: ['BTC/USDT','ETH/USDT','SOL/USDT'] },
    'BigONE': { tier: 3, type: 'ccxt-rest', ccxtId: 'bigone', ccxtSymbols: ['BTC/USDT','ETH/USDT','SOL/USDT'] },
    'EXMO': { tier: 3, type: 'ccxt-rest', ccxtId: 'exmo', ccxtSymbols: ['BTC/USDT','ETH/USDT','SOL/USDT','BTC/DAI'] },
    'LATOKEN': { tier: 3, type: 'ccxt-rest', ccxtId: 'latoken', ccxtSymbols: ['BTC/USDT','ETH/USDT','SOL/USDT'] },
};

// ═══════════════════════════════════════════════════════════════════
// ═══════════════════ CONNECTION ENGINES ═══════════════════════════
// ═══════════════════════════════════════════════════════════════════

// Global error handlers for 24/7 stability
process.on('uncaughtException', (err) => { console.error(`  [${ts()}] ⚠️  Uncaught: ${err.message}`); });
process.on('unhandledRejection', (reason) => { console.error(`  [${ts()}] ⚠️  Unhandled: ${reason}`); });

// ─── WS Engine (continuous with auto-reconnect) ───
function startWSExchange(name) {
    const exDef = EXCHANGES[name];
    initStats(name);
    stats[name].type = 'ws';
    stats[name].tier = exDef.tier;

    async function connect() {
        if (shuttingDown) return;

        let wsUrl, kucoinPing;
        try {
            const urlResult = typeof exDef.getUrl === 'function' ? await exDef.getUrl() : exDef.getUrl;
            if (typeof urlResult === 'object') { wsUrl = urlResult.url; kucoinPing = urlResult.pingInterval; }
            else { wsUrl = urlResult; }
        } catch (e) {
            stats[name].errors.push(e.message);
            scheduleReconnect(); return;
        }

        const ctx = {};
        let pingTimer, customPingTimer, ossWs, ossPingTimer;
        const extraWsList = [];

        try {
            const wsOpts = { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' }, handshakeTimeout: CONN_TIMEOUT };
            if (exDef.compression) wsOpts.perMessageDeflate = false;

            const ws = new WebSocket(wsUrl, wsOpts);
            activeWsList.add(ws);
            stats[name].status = 'connecting';

            const connTimeout = setTimeout(() => { if (ws.readyState !== WebSocket.OPEN) { ws.terminate(); scheduleReconnect(); } }, CONN_TIMEOUT);

            // OSS (dual WS for BTSE, Bullish)
            if (exDef.ossUrl && exDef.ossOnOpen) {
                try {
                    ossWs = new WebSocket(exDef.ossUrl, wsOpts);
                    activeWsList.add(ossWs);
                    ossWs.on('open', () => {
                        try { exDef.ossOnOpen(ossWs); } catch(e){}
                        if (exDef.pingInterval && exDef.pingMessage)
                            ossPingTimer = setInterval(() => { if (ossWs.readyState === WebSocket.OPEN) ossWs.send(exDef.pingMessage); }, exDef.pingInterval);
                    });
                    ossWs.on('message', (data) => {
                        let str = data.toString();
                        try { exDef.parseMessage(str, ctx); } catch(e){}
                    });
                    ossWs.on('error', ()=>{});
                    ossWs.on('close', () => { if (ossPingTimer) clearInterval(ossPingTimer); activeWsList.delete(ossWs); });
                } catch(e) {}
            }

            // Extra connections (Biconomy)
            if (exDef.extraConnections) {
                for (const ec of exDef.extraConnections) {
                    try {
                        const ecUrl = typeof ec.getUrl === 'function' ? ec.getUrl() : ec.getUrl;
                        const ecWs = new WebSocket(ecUrl, wsOpts);
                        activeWsList.add(ecWs);
                        extraWsList.push(ecWs);
                        ecWs.on('open', () => {
                            try { ec.onOpen(ecWs); } catch(e){}
                            if (exDef.pingInterval && exDef.pingMessage)
                                ecWs._pt = setInterval(() => { if (ecWs.readyState === WebSocket.OPEN) ecWs.send(exDef.pingMessage); }, exDef.pingInterval);
                        });
                        ecWs.on('message', (data) => {
                            let str;
                            if (exDef.compression && Buffer.isBuffer(data)) { try { str = exDef.compression === 'gzip' ? zlib.gunzipSync(data).toString() : zlib.inflateSync(data).toString(); } catch(e) { str = data.toString(); } }
                            else { str = data.toString(); }
                            if (exDef.handlePing) { try { const p = JSON.parse(str); if (exDef.handlePing(p, ecWs)) return; } catch(e){} }
                            try { exDef.parseMessage(str, ctx); } catch(e){}
                        });
                        ecWs.on('error', ()=>{});
                        ecWs.on('close', () => { if (ecWs._pt) clearInterval(ecWs._pt); activeWsList.delete(ecWs); });
                    } catch(e) {}
                }
            }

            ws.on('open', () => {
                clearTimeout(connTimeout);
                stats[name].status = 'connected';
                console.log(`  [${ts()}] ✅ ${name} — Connected (T${exDef.tier})`);
                sseEvent('connect', name, 'Connected');

                try { exDef.onOpen(ws); } catch(e) { stats[name].errors.push(e.message); }

                const pi = exDef.pingInterval || kucoinPing;
                if (pi && exDef.pingMessage)
                    pingTimer = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.send(exDef.pingMessage); }, pi);
                if (exDef.customPingSetup) customPingTimer = exDef.customPingSetup(ws);
            });

            ws.on('message', (data) => {
                let str;
                if (exDef.compression && Buffer.isBuffer(data)) {
                    try {
                        if (exDef.compression === 'gzip') str = zlib.gunzipSync(data).toString();
                        else if (exDef.compression === 'inflate') str = zlib.inflateSync(data).toString();
                        else { try { str = zlib.gunzipSync(data).toString(); } catch(e) { try { str = zlib.inflateSync(data).toString(); } catch(e2) { str = data.toString(); } } }
                    } catch(e) { str = data.toString(); }
                } else { str = data.toString(); }

                if (str === 'Ping') { try { ws.send('Pong'); } catch(e){} return; }
                if (exDef.handlePing) { try { const p = JSON.parse(str); if (exDef.handlePing(p, ws)) return; } catch(e){} }
                try { exDef.parseMessage(str, ctx); } catch(e){}
            });

            ws.on('error', (err) => {
                clearTimeout(connTimeout);
                stats[name].errors.push(err.message);
                sseEvent('error', name, err.message);
            });

            ws.on('close', () => {
                clearTimeout(connTimeout);
                if (pingTimer) clearInterval(pingTimer);
                if (customPingTimer) clearInterval(customPingTimer);
                activeWsList.delete(ws);
                if (ossWs) { if (ossPingTimer) clearInterval(ossPingTimer); try { ossWs.close(); } catch(e){} activeWsList.delete(ossWs); }
                for (const ecWs of extraWsList) { if (ecWs._pt) clearInterval(ecWs._pt); try { ecWs.close(); } catch(e){} activeWsList.delete(ecWs); }
                stats[name].status = 'disconnected';
                if (!shuttingDown) { sseEvent('reconnect', name, 'Reconnecting...'); scheduleReconnect(); }
            });

        } catch (err) {
            stats[name].errors.push(err.message);
            scheduleReconnect();
        }
    }

    function scheduleReconnect() {
        if (shuttingDown) return;
        stats[name].reconnects = (stats[name].reconnects || 0) + 1;
        // Exponential backoff: 3s, 6s, 12s, 24s, 48s, 60s cap
        const delay = Math.min(RECONNECT_BASE * Math.pow(2, Math.min(stats[name].reconnects - 1, 5)), RECONNECT_MAX);
        const timer = setTimeout(() => { activeTimers.delete(timer); connect(); }, delay);
        activeTimers.add(timer);
    }

    connect();
}

// ─── REST Engine (continuous polling) ───
function startRESTExchange(name) {
    const exDef = EXCHANGES[name];
    initStats(name);
    stats[name].type = 'rest';
    stats[name].tier = exDef.tier;
    console.log(`  [${ts()}] 🌐 ${name} — Starting REST polling`);

    async function poll() {
        if (shuttingDown) return;
        for (const sym of exDef.symbols) {
            try { const resp = await httpsRequest(exDef.endpoints.trades(sym)); exDef.parseTrades(resp, sym); } catch (e) {}
            try { const resp = await httpsRequest(exDef.endpoints.orderbook(sym)); exDef.parseOrderbook(resp, sym); } catch (e) {}
        }
    }

    poll();
    const timer = setInterval(poll, REST_POLL_INTERVAL);
    activeTimers.add(timer);
}

// ─── CCXT WS Engine (continuous watch loops) ───
function startCCXTWSExchange(name) {
    const exDef = EXCHANGES[name];
    initStats(name);
    stats[name].type = 'ccxt-ws';
    stats[name].tier = exDef.tier;
    console.log(`  [${ts()}] 🔌 ${name} — CCXT WS (${exDef.ccxtId})`);

    let exchange;
    try { exchange = new ccxt.pro[exDef.ccxtId]({ newUpdates: true, enableRateLimit: true }); } catch (e) {
        console.log(`  [${ts()}] ❌ ${name} — CCXT init failed: ${e.message}`);
        stats[name].status = 'error'; stats[name].errors.push(e.message); return;
    }

    for (const sym of exDef.ccxtSymbols) {
        // Trade watcher loop
        (async () => {
            while (!shuttingDown) {
                try {
                    const trades = await exchange.watchTrades(sym);
                    if (stats[name].status !== 'ok') { stats[name].status = 'ok'; console.log(`  [${ts()}] ✅ ${name} — Connected`); sseEvent('connect', name, 'Connected via CCXT'); }
                    for (const t of trades) recordTrade(name, sym, t.price, t.amount, t.side, t.datetime || null);
                } catch (e) {
                    if (shuttingDown) break;
                    if (!stats[name].errors.includes(e.message)) stats[name].errors.push(e.message);
                    await new Promise(r => setTimeout(r, 3000));
                }
            }
        })();
        // Orderbook watcher loop
        (async () => {
            while (!shuttingDown) {
                try {
                    const ob = await exchange.watchOrderBook(sym, 5);
                    if (stats[name].status !== 'ok') stats[name].status = 'ok';
                    recordOrderbook(name, sym, ob.bids.slice(0, 5), ob.asks.slice(0, 5));
                } catch (e) {
                    if (shuttingDown) break;
                    if (!stats[name].errors.includes(e.message)) stats[name].errors.push(e.message);
                    await new Promise(r => setTimeout(r, 3000));
                }
            }
        })();
    }

    // Store exchange ref for cleanup
    stats[name]._exchange = exchange;
}

// ─── CCXT REST Engine (continuous polling) ───
function startCCXTRESTExchange(name) {
    const exDef = EXCHANGES[name];
    initStats(name);
    stats[name].type = 'ccxt-rest';
    stats[name].tier = exDef.tier;
    console.log(`  [${ts()}] 🌐 ${name} — CCXT REST (${exDef.ccxtId})`);

    let exchange;
    try { exchange = new ccxt[exDef.ccxtId]({ enableRateLimit: true }); } catch (e) {
        stats[name].status = 'error'; stats[name].errors.push(e.message); return;
    }

    (async () => {
        try { await exchange.loadMarkets(); } catch (e) { stats[name].errors.push(`loadMarkets: ${e.message}`); }
        while (!shuttingDown) {
            for (const sym of exDef.ccxtSymbols) {
                try { const trades = await exchange.fetchTrades(sym, undefined, 5); for (const t of trades) recordTrade(name, sym, t.price, t.amount, t.side, t.datetime || null); } catch (e) {}
                try { const ob = await exchange.fetchOrderBook(sym, 5); recordOrderbook(name, sym, ob.bids.slice(0, 5), ob.asks.slice(0, 5)); } catch (e) {}
            }
            await new Promise(r => setTimeout(r, REST_POLL_INTERVAL));
        }
    })();
}

// ═══════════════════════════════════════════════════════════════════
// ═══════════════════ DUCKDB FLUSH ═════════════════════════════════
// ═══════════════════════════════════════════════════════════════════

function esc(v) {
    if (v === null || v === undefined) return 'NULL';
    if (typeof v === 'number') return isNaN(v) ? 'NULL' : String(v);
    return `'${String(v).replace(/'/g, "''")}'`;
}

async function flushToDuckDB() {
    if (!dbConn) return 0;
    let totalRows = 0;

    // Swap buffers atomically
    const trades = tradeBuffer; tradeBuffer = [];
    const obs = obBuffer; obBuffer = [];

    // Flush trades in batches of 500
    for (let i = 0; i < trades.length; i += 500) {
        const batch = trades.slice(i, i + 500);
        try {
            const values = batch.map(t => `(${esc(t.ts)}, ${esc(t.exchange)}, ${esc(t.canonical_pair)}, ${t.price || 0}, ${t.qty || 0}, ${esc(t.side)})`).join(',\n');
            await dbConn.run(`INSERT INTO trades VALUES ${values}`);
            totalRows += batch.length;
        } catch (e) { console.error(`  [${ts()}] ❌ Trade flush error: ${e.message}`); }
    }

    // Flush orderbook
    for (let i = 0; i < obs.length; i += 500) {
        const batch = obs.slice(i, i + 500);
        try {
            const values = batch.map(o => `(${esc(o.ts)}, ${esc(o.exchange)}, ${esc(o.canonical_pair)}, ${esc(o.bids)}, ${esc(o.asks)})`).join(',\n');
            await dbConn.run(`INSERT INTO orderbook VALUES ${values}`);
            totalRows += batch.length;
        } catch (e) { console.error(`  [${ts()}] ❌ OB flush error: ${e.message}`); }
    }

    totalFlushed += totalRows;
    if (totalRows > 0) sseEvent('flush', '', `Flushed ${totalRows} rows (total: ${totalFlushed})`);
    return totalRows;
}

// ═══════════════════════════════════════════════════════════════════
// ═══════════════════ HTTP SERVER + DASHBOARD ══════════════════════
// ═══════════════════════════════════════════════════════════════════

function startHttpServer() {
    const server = http.createServer((req, res) => {
        // CORS
        res.setHeader('Access-Control-Allow-Origin', '*');

        if (req.url === '/' || req.url === '/index.html') {
            const dashPath = path.join(__dirname, 'dashboard.html');
            try {
                const html = fs.readFileSync(dashPath, 'utf8');
                res.writeHead(200, { 'Content-Type': 'text/html' });
                res.end(html);
            } catch (e) {
                res.writeHead(500);
                res.end('Dashboard file not found: ' + e.message);
            }
            return;
        }

        if (req.url === '/api/stats') {
            const exchanges = {};
            for (const [name, s] of Object.entries(stats)) {
                exchanges[name] = {
                    trades: s.trades, orderbook: s.orderbook,
                    status: s.status, tier: s.tier,
                    type: s.type, reconnects: s.reconnects,
                    errors: s.errors.slice(-5)
                };
            }

            // Symbol aggregation
            const symbols = {};
            for (const [name, s] of Object.entries(stats)) {
                // Count per canonical pair from buffers is expensive; use stats
            }
            // Quick aggregation from trade/ob buffers + totalFlushed
            // Instead, compute from stats
            const pairTrades = {}, pairOB = {}, pairExchanges = {};

            // We don't have per-pair breakdown in stats, so approximate from symbol map
            // For dashboard purposes, show total per exchange

            const uptime = startTime ? Math.floor((Date.now() - startTime) / 1000) : 0;
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ exchanges, uptime, running, totalFlushed }));
            return;
        }

        if (req.url === '/api/events') {
            res.writeHead(200, {
                'Content-Type': 'text/event-stream',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive'
            });
            res.write(`data: ${JSON.stringify({ type: 'info', message: 'Connected to SSE', time: ts() })}\n\n`);
            sseClients.push(res);
            req.on('close', () => {
                const idx = sseClients.indexOf(res);
                if (idx > -1) sseClients.splice(idx, 1);
            });
            return;
        }

        res.writeHead(404);
        res.end('Not found');
    });

    server.listen(HTTP_PORT, () => {
        console.log(`  🌐 Dashboard: http://localhost:${HTTP_PORT}`);
    });
    return server;
}

// ═══════════════════════════════════════════════════════════════════
// ═══════════════════ MAIN ═════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════

async function main() {
    console.log('\n╔' + '═'.repeat(78) + '╗');
    console.log('║  15-MINUTE PARALLEL STREAM TEST — All 48 Exchanges                          ║');
    console.log('║  Fixes: AscendEX, Bullish OB, CEX.IO SOL, FameEX heartbeat                  ║');
    console.log('║  All exchanges run in PARALLEL (not batches)                                 ║');
    console.log('╚' + '═'.repeat(78) + '╝\n');

    // Build symbol map
    buildSymbolMap();
    console.log(`  📋 Symbol map: ${Object.keys(SYMBOL_MAP).length} mappings`);

    // Open DuckDB
    const instance = await DuckDBInstance.create('streaming.duckdb');
    dbConn = await instance.connect();

    // Verify tables exist
    try { await dbConn.runAndReadAll('SELECT 1 FROM trades LIMIT 1'); } catch (e) {
        console.log('  ❌ Tables not found! Run init-schema.js first.'); process.exit(1);
    }

    // Clear previous data
    await dbConn.run('DELETE FROM trades');
    await dbConn.run('DELETE FROM orderbook');
    console.log('  🗑️  Cleared previous data');

    // Start HTTP server for dashboard
    const server = startHttpServer();
    running = true;
    startTime = Date.now();

    // Categorize exchanges
    const names = Object.keys(EXCHANGES);
    const wsNames = names.filter(n => { const t = EXCHANGES[n].type; return !t || t === 'ws'; });
    const restNames = names.filter(n => EXCHANGES[n].type === 'rest');
    const ccxtWsNames = names.filter(n => EXCHANGES[n].type === 'ccxt-ws');
    const ccxtRestNames = names.filter(n => EXCHANGES[n].type === 'ccxt-rest');

    console.log(`\n  🚀 Launching ALL ${names.length} exchanges in PARALLEL`);
    console.log(`     WS: ${wsNames.length} | REST: ${restNames.length} | CCXT-WS: ${ccxtWsNames.length} | CCXT-REST: ${ccxtRestNames.length}\n`);

    // Start ALL in parallel with slight stagger to avoid thundering herd
    let idx = 0;
    for (const name of wsNames) {
        setTimeout(() => startWSExchange(name), idx * STAGGER_MS);
        idx++;
    }
    for (const name of restNames) {
        setTimeout(() => startRESTExchange(name), idx * STAGGER_MS);
        idx++;
    }
    for (const name of ccxtWsNames) {
        setTimeout(() => startCCXTWSExchange(name), idx * STAGGER_MS);
        idx++;
    }
    for (const name of ccxtRestNames) {
        setTimeout(() => startCCXTRESTExchange(name), idx * STAGGER_MS);
        idx++;
    }

    // Periodic DuckDB flush
    const flushTimer = setInterval(async () => {
        const rows = await flushToDuckDB();
        if (rows > 0) {
            const elapsed = Math.floor((Date.now() - startTime) / 1000);
            const active = Object.values(stats).filter(s => s.trades > 0 || s.orderbook > 0).length;
            console.log(`  [${ts()}] 💾 Flush: +${rows} rows | Total: ${totalFlushed} | Active: ${active}/${names.length} | ${elapsed}s elapsed`);
        }
    }, FLUSH_INTERVAL);

    // Progress dashboard every 30s
    const progressTimer = setInterval(() => {
        const elapsed = Math.floor((Date.now() - startTime) / 1000);
        const remaining = Math.max(0, Math.floor((TEST_DURATION - (Date.now() - startTime)) / 1000));
        const active = Object.values(stats).filter(s => s.trades > 0 || s.orderbook > 0).length;
        const totalT = Object.values(stats).reduce((a, s) => a + s.trades, 0);
        const totalO = Object.values(stats).reduce((a, s) => a + s.orderbook, 0);
        console.log(`  [${ts()}] 📊 ${elapsed}s elapsed | ${remaining}s left | ${active}/${names.length} active | TR=${totalT} OB=${totalO} | Flushed=${totalFlushed}`);
    }, 30000);

    // Graceful shutdown handler
    const shutdown = async () => {
        if (shuttingDown) return;
        shuttingDown = true;
        running = false;
        console.log(`\n  [${ts()}] 🛑 Shutting down...`);

        clearInterval(flushTimer);
        clearInterval(progressTimer);
        for (const timer of activeTimers) clearTimeout(timer);
        activeTimers.clear();

        // Close all WebSockets
        for (const ws of activeWsList) { try { ws.terminate(); } catch (e) {} }
        activeWsList.clear();

        // Close CCXT exchanges
        for (const [name, s] of Object.entries(stats)) {
            if (s._exchange) { try { await s._exchange.close(); } catch (e) {} }
        }

        // Final flush
        const finalRows = await flushToDuckDB();
        console.log(`  [${ts()}] 💾 Final flush: ${finalRows} rows | Grand total: ${totalFlushed}`);

        // Print report
        await printReport();

        server.close();
        console.log(`  [${ts()}] ✅ Test complete. Dashboard closed.`);
        process.exit(0);
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);

    // Auto-stop after TEST_DURATION
    setTimeout(shutdown, TEST_DURATION);

    console.log(`  ⏱  Test will run for ${TEST_DURATION / 60000} minutes. Press Ctrl+C to stop early.\n`);
}

// ═══════════════════════════════════════════════════════════════════
// ═══════════════════ REPORT ═══════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════

async function printReport() {
    if (!dbConn) return;

    console.log('\n╔' + '═'.repeat(100) + '╗');
    console.log('║  15-MINUTE PARALLEL TEST REPORT                                                                       ║');
    console.log('╚' + '═'.repeat(100) + '╝');

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`\n  Duration: ${elapsed}s | Flushed: ${totalFlushed} rows\n`);

    // Per-exchange summary
    console.log('  ' + 'Exchange'.padEnd(15) + 'T'.padEnd(3) + 'Type'.padEnd(10) + '│ Trades'.padEnd(12) + 'OB'.padEnd(10) + '│ Status'.padEnd(12) + 'Reconn'.padEnd(8) + 'Errors');
    console.log('  ' + '─'.repeat(80));

    const sorted = Object.keys(EXCHANGES).sort((a, b) => {
        const ta = EXCHANGES[a].tier, tb = EXCHANGES[b].tier;
        if (ta !== tb) return ta - tb;
        return (stats[b]?.trades + stats[b]?.orderbook || 0) - (stats[a]?.trades + stats[a]?.orderbook || 0);
    });

    let allOk = 0, totalT = 0, totalO = 0;
    for (const name of sorted) {
        const s = stats[name] || { trades: 0, orderbook: 0, status: 'unknown', reconnects: 0, errors: [] };
        const total = s.trades + s.orderbook;
        const emoji = total > 0 ? '✅' : '❌';
        if (total > 0) allOk++;
        totalT += s.trades; totalO += s.orderbook;
        console.log('  ' + name.padEnd(15) + `${EXCHANGES[name].tier}`.padEnd(3) + (s.type || 'ws').padEnd(10) +
            `│ ${String(s.trades).padStart(7)}  ${String(s.orderbook).padStart(7)}  │ ${emoji} ${(s.status || '').padEnd(8)} ${String(s.reconnects || 0).padEnd(8)} ${s.errors.length}`);
    }
    console.log('  ' + '─'.repeat(80));
    console.log(`  ${allOk}/${sorted.length} exchanges collected data | Total: TR=${totalT} OB=${totalO}\n`);

    // DuckDB stats
    try {
        let reader;
        reader = await dbConn.runAndReadAll('SELECT COUNT(*) FROM trades');
        console.log(`  DuckDB trades:    ${reader.getRows()[0][0]}`);
        reader = await dbConn.runAndReadAll('SELECT COUNT(*) FROM orderbook');
        console.log(`  DuckDB orderbook: ${reader.getRows()[0][0]}`);

        // Per-exchange in DB
        console.log('\n  ── DuckDB Per-Exchange Breakdown ──\n');
        reader = await dbConn.runAndReadAll(`
            SELECT e.exchange, COALESCE(t.tc,0) as trades, COALESCE(o.oc,0) as ob
            FROM (SELECT DISTINCT exchange FROM (SELECT exchange FROM trades UNION ALL SELECT exchange FROM orderbook)) e
            LEFT JOIN (SELECT exchange, COUNT(*) tc FROM trades GROUP BY exchange) t ON e.exchange = t.exchange
            LEFT JOIN (SELECT exchange, COUNT(*) oc FROM orderbook GROUP BY exchange) o ON e.exchange = o.exchange
            ORDER BY trades + ob DESC
        `);
        console.log('  ' + 'Exchange'.padEnd(15) + 'Trades'.padEnd(10) + 'Orderbook');
        console.log('  ' + '─'.repeat(35));
        for (const row of reader.getRows()) console.log(`  ${String(row[0]).padEnd(15)} ${String(row[1]).padEnd(10)} ${row[2]}`);

        // Per canonical pair
        console.log('\n  ── Per Canonical Pair ──\n');
        reader = await dbConn.runAndReadAll(`
            SELECT canonical_pair, COUNT(*) as cnt, COUNT(DISTINCT exchange) as ex
            FROM trades GROUP BY canonical_pair ORDER BY cnt DESC
        `);
        for (const row of reader.getRows()) console.log(`  ${String(row[0]).padEnd(12)} ${String(row[1]).padEnd(10)} from ${row[2]} exchanges`);
    } catch (e) {
        console.log(`  ⚠ Report query error: ${e.message}`);
    }

    console.log('\n  ═══════════════════════════════════════════\n');
}

// ═══════════════════ RUN ═══════════════════
main().catch(err => { console.error('Fatal:', err); process.exit(1); });
