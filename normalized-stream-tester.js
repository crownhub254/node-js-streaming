/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║   NORMALIZED STREAM TESTER — Canonical Pair System                      ║
 * ║   30 Exchanges × BTC/ETH/SOL × USD/USDT/USDC                           ║
 * ║   Collects trades + orderbook snapshots → DuckDB                       ║
 * ║   All data stored with canonical_pair (e.g., BTC_USDT) not raw symbols  ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */

const WebSocket = require('ws');
const zlib = require('zlib');
const https = require('https');
const { DuckDBInstance } = require('@duckdb/node-api');

// ======================== CONFIGURATION ========================
const TEST_TIMEOUT = 320000;
const DATA_WAIT = 300000;     // 5 minutes to collect data
const STAGGER_DELAY = 300;
const MAX_CONCURRENT = 6;

// ======================== UTILITIES ========================
function httpsRequest(url, options = {}) {
    return new Promise((resolve, reject) => {
        const opts = { ...require('url').parse(url), method: options.method || 'GET',
            headers: { 'User-Agent': 'Mozilla/5.0', 'Content-Type': 'application/json', ...(options.headers || {}) } };
        const req = https.request(opts, (res) => {
            let data = ''; res.on('data', c => data += c);
            res.on('end', () => { try { resolve(JSON.parse(data)); } catch (e) { resolve(data); } });
        });
        req.on('error', reject);
        if (options.body) req.write(JSON.stringify(options.body));
        req.end();
    });
}

function ts() { return new Date().toISOString().substring(11, 19); }
function nowISO() { return new Date().toISOString(); }

// ======================== SYMBOL MAP (in-memory) ========================
// Maps: { "Binance:BTCUSDT" → { base: "BTC", quote: "USDT", canonical: "BTC_USDT" } }
const SYMBOL_MAP = {};

function registerSymbol(exchange, exchangeSymbol, base, quote) {
    const key = `${exchange}:${exchangeSymbol}`;
    SYMBOL_MAP[key] = { base, quote, canonical: `${base}_${quote}` };
}

function resolve(exchange, exchangeSymbol) {
    return SYMBOL_MAP[`${exchange}:${exchangeSymbol}`] || null;
}

// Register all symbols for all exchanges
function buildSymbolMap() {
    const map = {
        // ─── Binance ───
        'Binance': { 'BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','SOLUSDT':'SOL_USDT','BTCUSDC':'BTC_USDC','ETHUSDC':'ETH_USDC','SOLUSDC':'SOL_USDC' },
        // ─── Coinbase ───
        'Coinbase': { 'BTC-USD':'BTC_USD','ETH-USD':'ETH_USD','SOL-USD':'SOL_USD' },
        // ─── Kraken ───
        'Kraken': { 'BTC/USDT':'BTC_USDT','ETH/USDT':'ETH_USDT','SOL/USDT':'SOL_USDT','BTC/USDC':'BTC_USDC','ETH/USDC':'ETH_USDC','SOL/USDC':'SOL_USDC','BTC/USD':'BTC_USD' },
        // ─── KuCoin ───
        'KuCoin': { 'BTC-USDT':'BTC_USDT','ETH-USDT':'ETH_USDT','SOL-USDT':'SOL_USDT' },
        // ─── OKX ───
        'OKX': { 'BTC-USDT':'BTC_USDT','ETH-USDT':'ETH_USDT','SOL-USDT':'SOL_USDT','BTC-USDC':'BTC_USDC','ETH-USDC':'ETH_USDC','SOL-USDC':'SOL_USDC','BTC-USD':'BTC_USD','ETH-USD':'ETH_USD','SOL-USD':'SOL_USD' },
        // ─── Bybit ───
        'Bybit': { 'BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','SOLUSDT':'SOL_USDT','BTCUSDC':'BTC_USDC','ETHUSDC':'ETH_USDC','SOLUSDC':'SOL_USDC' },
        // ─── Bitfinex ───
        'Bitfinex': { 'tBTCUSD':'BTC_USD','tETHUSD':'ETH_USD','tSOLUSD':'SOL_USD' },
        // ─── Gate.io ───
        'Gate.io': { 'BTC_USDT':'BTC_USDT','ETH_USDT':'ETH_USDT','SOL_USDT':'SOL_USDT' },
        // ─── HTX ───
        'HTX': { 'btcusdt':'BTC_USDT','ethusdt':'ETH_USDT','solusdt':'SOL_USDT' },
        // ─── WOO X ───
        'WOO X': { 'SPOT_BTC_USDT':'BTC_USDT','SPOT_ETH_USDT':'ETH_USDT','SPOT_SOL_USDT':'SOL_USDT' },
        // ─── Crypto.com ───
        'Crypto.com': { 'BTC_USDT':'BTC_USDT','ETH_USDT':'ETH_USDT','SOL_USDT':'SOL_USDT' },
        // ─── Bitstamp ───
        'Bitstamp': { 'btcusd':'BTC_USD','ethusd':'ETH_USD','solusd':'SOL_USD' },
        // ─── WhiteBIT ───
        'WhiteBIT': { 'BTC_USDT':'BTC_USDT','ETH_USDT':'ETH_USDT','SOL_USDT':'SOL_USDT' },
        // ─── AscendEX ───
        'AscendEX': { 'BTC/USDT':'BTC_USDT','ETH/USDT':'ETH_USDT','SOL/USDT':'SOL_USDT' },
        // ─── BingX ───
        'BingX': { 'BTC-USDT':'BTC_USDT','ETH-USDT':'ETH_USDT','SOL-USDT':'SOL_USDT' },
        // ─── Toobit ───
        'Toobit': { 'BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','SOLUSDT':'SOL_USDT' },
        // ─── Deepcoin ───
        'Deepcoin': { 'BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','SOLUSDT':'SOL_USDT','DeepCoin_BTCUSDT':'BTC_USDT','DeepCoin_ETHUSDT':'ETH_USDT','DeepCoin_SOLUSDT':'SOL_USDT' },
        // ─── XT.com ───
        'XT.com': { 'btc_usdt':'BTC_USDT','eth_usdt':'ETH_USDT','sol_usdt':'SOL_USDT' },
        // ─── Zoomex ───
        'Zoomex': { 'BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','SOLUSDT':'SOL_USDT' },
        // ─── LBank ───
        'LBank': { 'btc_usdt':'BTC_USDT','eth_usdt':'ETH_USDT','sol_usdt':'SOL_USDT' },
        // ─── BitMart ───
        'BitMart': { 'BTC_USDT':'BTC_USDT','ETH_USDT':'ETH_USDT','SOL_USDT':'SOL_USDT' },
        // ─── Pionex ───
        'Pionex': { 'BTC_USDT':'BTC_USDT','ETH_USDT':'ETH_USDT','SOL_USDT':'SOL_USDT' },
        // ─── Poloniex ───
        'Poloniex': { 'BTC_USDT':'BTC_USDT','ETH_USDT':'ETH_USDT','SOL_USDT':'SOL_USDT' },
        // ─── BTSE ───
        'BTSE': { 'BTC-USD':'BTC_USD','ETH-USD':'ETH_USD','SOL-USD':'SOL_USD','BTC-USD_0':'BTC_USD','ETH-USD_0':'ETH_USD','SOL-USD_0':'SOL_USD' },
        // ─── HitBTC ───
        'HitBTC': { 'BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','SOLUSDT':'SOL_USDT' },
        // ─── Biconomy ───
        'Biconomy': { 'BTC_USDT':'BTC_USDT','ETH_USDT':'ETH_USDT','SOL_USDT':'SOL_USDT' },
        // ─── Hotcoin ───
        'Hotcoin': { 'btc_usdt':'BTC_USDT','eth_usdt':'ETH_USDT','sol_usdt':'SOL_USDT' },
        // ─── NovaEx ───
        'NovaEx': { 'SPOT_BTC_USDT':'BTC_USDT','SPOT_ETH_USDT':'ETH_USDT','SPOT_SOL_USDT':'SOL_USDT' },
        // ─── FameEX (REST) ───
        'FameEX': { 'BTCUSDT':'BTC_USDT','ETHUSDT':'ETH_USDT','SOLUSDT':'SOL_USDT','BTC_USDT':'BTC_USDT','ETH_USDT':'ETH_USDT','SOL_USDT':'SOL_USDT' },
        // ─── Websea (REST) ───
        'Websea': { 'BTC-USDT':'BTC_USDT','ETH-USDT':'ETH_USDT','SOL-USDT':'SOL_USDT' },
    };
    for (const [exchange, symbols] of Object.entries(map)) {
        for (const [exSym, canonical] of Object.entries(symbols)) {
            const [base, quote] = canonical.split('_');
            registerSymbol(exchange, exSym, base, quote);
        }
    }
}

// ======================== DATA BUFFERS ========================
let tradeBuffer = [];
let obBuffer = [];
const stats = {};             // { exchange: { trades: n, orderbook: n } }

function initStats(exchange) {
    stats[exchange] = { trades: 0, orderbook: 0, status: 'pending', errors: [] };
}

function recordTrade(exchange, exchangeSymbol, price, qty, side, timestamp) {
    const mapping = resolve(exchange, exchangeSymbol);
    if (!mapping) return;
    tradeBuffer.push({
        ts: timestamp || nowISO(),
        exchange,
        canonical_pair: mapping.canonical,
        price: parseFloat(price) || 0,
        qty: parseFloat(qty) || 0,
        side: side || null
    });
    stats[exchange].trades++;
}

function recordOrderbook(exchange, exchangeSymbol, bids, asks, timestamp) {
    const mapping = resolve(exchange, exchangeSymbol);
    if (!mapping) return;
    obBuffer.push({
        ts: timestamp || nowISO(),
        exchange,
        canonical_pair: mapping.canonical,
        bids: JSON.stringify((bids || []).slice(0, 5)),
        asks: JSON.stringify((asks || []).slice(0, 5))
    });
    stats[exchange].orderbook++;
}

// ======================== EXCHANGE DEFINITIONS ========================
// Each parseMessage now calls recordTrade/recordOrderbook directly
// and returns hits for counting (backward compatible with test format)

const EXCHANGES = {

    'Binance': {
        tier: 1,
        getUrl: () => 'wss://stream.binance.com:9443/stream',
        onOpen: (ws) => {
            ws.send(JSON.stringify({ method: 'SUBSCRIBE', params: [
                'btcusdt@trade','ethusdt@trade','solusdt@trade',
                'btcusdt@depth5@1000ms','ethusdt@depth5@1000ms','solusdt@depth5@1000ms'
            ], id: 1 }));
        },
        parseMessage: (msg, ctx) => {
            try {
                const d = JSON.parse(msg);
                if (!d.data) return [];
                const e = d.data;
                if (e.e === 'trade') {
                    recordTrade('Binance', e.s, e.p, e.q, e.m ? 'sell' : 'buy');
                }
                if (e.lastUpdateId && e.bids && e.asks) {
                    const stream = d.stream || '';
                    const sym = stream.split('@')[0].toUpperCase();
                    recordOrderbook('Binance', sym, e.bids, e.asks);
                }
                return [];
            } catch (e) { return []; }
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
                const p = d.product_id; if (!p) return [];
                if (d.type === 'match' || d.type === 'last_match') {
                    recordTrade('Coinbase', p, d.price, d.size, d.side);
                }
                if (d.type === 'snapshot' || d.type === 'l2update') {
                    recordOrderbook('Coinbase', p, d.bids, d.asks);
                }
                return [];
            } catch (e) { return []; }
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
                if (d.channel === 'heartbeat' || d.method) return [];
                if (!d.channel || !d.data) return [];
                if (d.channel === 'trade') {
                    for (const t of d.data) recordTrade('Kraken', t.symbol, t.price, t.qty, t.side);
                }
                if (d.channel === 'book') {
                    for (const b of d.data) recordOrderbook('Kraken', b.symbol, b.bids, b.asks);
                }
                return [];
            } catch (e) { return []; }
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
        onOpen: (ws) => {
            ws.send(JSON.stringify({ id:1, type:'subscribe', topic:'/market/match:BTC-USDT,ETH-USDT,SOL-USDT', privateChannel:false, response:true }));
            ws.send(JSON.stringify({ id:3, type:'subscribe', topic:'/spotMarket/level2Depth5:BTC-USDT,ETH-USDT,SOL-USDT', privateChannel:false, response:true }));
        },
        pingMessage: JSON.stringify({ id: Date.now(), type: 'ping' }),
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.type === 'pong' || d.type === 'welcome' || d.type === 'ack') return [];
                if (!d.topic || !d.data) return [];
                const sym = d.data.symbol || d.topic.split(':').pop() || '';
                if (!sym) return [];
                if (d.topic.includes('/market/match')) {
                    recordTrade('KuCoin', sym, d.data.price, d.data.size, d.data.side);
                }
                if (d.topic.includes('level2Depth5')) {
                    recordOrderbook('KuCoin', sym, d.data.bids, d.data.asks);
                }
                return [];
            } catch (e) { return []; }
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
                if (msg === 'pong') return [];
                const d = JSON.parse(msg);
                if (!d.data || !d.arg) return [];
                const sym = d.arg.instId;
                if (d.arg.channel === 'trades') {
                    for (const t of d.data) recordTrade('OKX', sym, t.px, t.sz, t.side);
                }
                if (d.arg.channel === 'books5') {
                    for (const b of d.data) recordOrderbook('OKX', sym, b.bids, b.asks);
                }
                return [];
            } catch (e) { return []; }
        }
    },

    'Bybit': {
        tier: 1,
        getUrl: () => 'wss://stream.bybit.com/v5/public/spot',
        pingInterval: 20000, pingMessage: JSON.stringify({ op: 'ping' }),
        onOpen: (ws) => {
            const syms = ['BTCUSDT','ETHUSDT','SOLUSDT'];
            ws.send(JSON.stringify({ op:'subscribe', args: syms.flatMap(s => [`publicTrade.${s}`,`orderbook.50.${s}`]) }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.topic || !d.data) return [];
                // Extract symbol from topic: "publicTrade.BTCUSDT" -> "BTCUSDT"
                const parts = d.topic.split('.');
                const sym = parts[parts.length - 1];
                if (d.topic.startsWith('publicTrade')) {
                    for (const t of (Array.isArray(d.data) ? d.data : [d.data]))
                        recordTrade('Bybit', sym, t.p, t.v, t.S === 'Buy' ? 'buy' : 'sell');
                }
                if (d.topic.startsWith('orderbook')) {
                    recordOrderbook('Bybit', sym, d.data.b, d.data.a);
                }
                return [];
            } catch (e) { return []; }
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
                if (d.event === 'subscribed') {
                    ctx._bfxChannels[d.chanId] = { channel: d.channel, symbol: d.symbol || d.pair };
                    return [];
                }
                if (!Array.isArray(d) || d.length < 2) return [];
                const chanId = d[0];
                const info = ctx._bfxChannels[chanId];
                if (!info) return [];
                if (d[1] === 'hb') return [];
                const sym = info.symbol.startsWith('t') ? info.symbol : `t${info.symbol}`;
                if (info.channel === 'trades') {
                    if (d[1] === 'te' || d[1] === 'tu') {
                        const t = d[2]; // [ID, MTS, AMOUNT, PRICE]
                        recordTrade('Bitfinex', sym, t[3], Math.abs(t[2]), t[2] > 0 ? 'buy' : 'sell');
                    }
                }
                if (info.channel === 'book') {
                    // Snapshot or update — just record as orderbook hit
                    if (Array.isArray(d[1]) && Array.isArray(d[1][0])) {
                        const bids = d[1].filter(e => e[2] > 0).map(e => [e[0], e[2]]);
                        const asks = d[1].filter(e => e[2] < 0).map(e => [e[0], Math.abs(e[2])]);
                        recordOrderbook('Bitfinex', sym, bids.slice(0, 5), asks.slice(0, 5));
                    } else if (Array.isArray(d[1])) {
                        recordOrderbook('Bitfinex', sym, [], []);
                    }
                }
                return [];
            } catch (e) { return []; }
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
            ws.send(JSON.stringify({ time:t, channel:'spot.order_book', event:'subscribe', payload:[...pairs.map(p => [p,5,'1000ms'])].flat() }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.result || d.event === 'subscribe') return [];
                if (d.channel === 'spot.trades' && d.result) {
                    const t = d.result;
                    const sym = t.currency_pair;
                    recordTrade('Gate.io', sym, t.price, t.amount, t.side);
                }
                if (d.channel === 'spot.order_book') {
                    const b = d.result;
                    const sym = b.s || '';
                    if (b.bids || b.asks) recordOrderbook('Gate.io', sym, b.bids, b.asks);
                }
                return [];
            } catch (e) { return []; }
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
                if (d.ping || d.subbed) return [];
                const ch = d.ch || '';
                // market.btcusdt.trade.detail, market.btcusdt.depth.step0
                const parts = ch.split('.');
                if (parts.length < 3) return [];
                const sym = parts[1]; // btcusdt
                if (ch.includes('.trade.detail') && d.tick?.data) {
                    for (const t of d.tick.data) recordTrade('HTX', sym, t.price, t.amount, t.direction);
                }
                if (ch.includes('.depth') && d.tick) {
                    recordOrderbook('HTX', sym, d.tick.bids, d.tick.asks);
                }
                return [];
            } catch (e) { return []; }
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
                if (d.event === 'pong' || d.event === 'subscribe') return [];
                const topic = d.topic || '';
                // SPOT_BTC_USDT@trade -> SPOT_BTC_USDT
                const sym = topic.split('@')[0];
                if (!sym.startsWith('SPOT_')) return [];
                if (topic.includes('@trade') && d.data) {
                    recordTrade('WOO X', sym, d.data.price, d.data.size, d.data.side?.toLowerCase());
                }
                if (topic.includes('@orderbook') && d.data) {
                    recordOrderbook('WOO X', sym, d.data.bids, d.data.asks);
                }
                return [];
            } catch (e) { return []; }
        }
    },

    'Crypto.com': {
        tier: 2,
        getUrl: () => 'wss://stream.crypto.com/exchange/v1/market',
        onOpen: (ws) => {
            const pairs = ['BTC_USDT','ETH_USDT','SOL_USDT'];
            ws.send(JSON.stringify({ id:1, method:'subscribe', params:{ channels:
                pairs.flatMap(p => [`trade.${p}`,`book.${p}.10`])
            }}));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.result) return [];
                const ch = d.result.channel || d.result.subscription || '';
                const sym = d.result.instrument_name || ch.split('.')[1] || '';
                if (ch.startsWith('trade') && d.result.data) {
                    for (const t of d.result.data) recordTrade('Crypto.com', sym, t.p, t.q, t.s?.toLowerCase());
                }
                if (ch.startsWith('book') && d.result.data) {
                    for (const b of (Array.isArray(d.result.data) ? d.result.data : [d.result.data]))
                        recordOrderbook('Crypto.com', sym, b.bids, b.asks);
                }
                return [];
            } catch (e) { return []; }
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
                if (!d.channel || !d.data) return [];
                const ch = d.channel;
                // live_trades_btcusd -> btcusd
                if (ch.startsWith('live_trades_')) {
                    const sym = ch.replace('live_trades_', '');
                    recordTrade('Bitstamp', sym, d.data.price, d.data.amount, d.data.type === 0 ? 'buy' : 'sell');
                }
                if (ch.startsWith('order_book_')) {
                    const sym = ch.replace('order_book_', '');
                    recordOrderbook('Bitstamp', sym, d.data.bids, d.data.asks);
                }
                return [];
            } catch (e) { return []; }
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
                    if (Array.isArray(d.params[1])) {
                        for (const t of d.params[1]) recordTrade('WhiteBIT', sym, t.price, t.amount, t.type);
                    }
                }
                if (d.method === 'depth_update' && d.params) {
                    const isSnap = d.params[0];
                    const data = d.params[1];
                    const sym = d.params[2] || '';
                    recordOrderbook('WhiteBIT', sym, data.bids ? Object.entries(data.bids) : [], data.asks ? Object.entries(data.asks) : []);
                }
                return [];
            } catch (e) { return []; }
        }
    },

    'AscendEX': {
        tier: 2,
        getUrl: () => 'wss://ascendex.com/1/api/pro/v1/stream',
        pingInterval: 15000, pingMessage: JSON.stringify({ op: 'ping' }),
        onOpen: (ws) => {
            ws.send(JSON.stringify({ op:'sub', ch:'trades:BTC/USDT,ETH/USDT,SOL/USDT' }));
            ws.send(JSON.stringify({ op:'sub', ch:'depth:BTC/USDT,ETH/USDT,SOL/USDT' }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.m) return [];
                if (d.m === 'trades' && d.data) {
                    for (const t of d.data) recordTrade('AscendEX', d.symbol, t.p, t.q, t.bm ? 'sell' : 'buy');
                }
                if (d.m === 'depth' && d.data) {
                    recordOrderbook('AscendEX', d.symbol, d.data.bids, d.data.asks);
                }
                return [];
            } catch (e) { return []; }
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
                if (msg === 'Ping') return [];
                const d = JSON.parse(msg);
                if (!d.dataType) return [];
                const sym = d.dataType.split('@')[0];
                if (d.dataType.includes('@trade') && d.data) {
                    for (const t of (Array.isArray(d.data) ? d.data : [d.data]))
                        recordTrade('BingX', sym, t.p, t.q, t.m ? 'sell' : 'buy');
                }
                if (d.dataType.includes('@depth') && d.data) {
                    recordOrderbook('BingX', sym, d.data.bids, d.data.asks);
                }
                return [];
            } catch (e) { return []; }
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
                if (d.pong) return [];
                const sym = d.symbol || d.symbolName || '';
                if (!sym) return [];
                if (d.topic === 'trade' && d.data) {
                    for (const t of d.data) recordTrade('Toobit', sym, t.p, t.q, t.m ? 'sell' : 'buy');
                }
                if (d.topic === 'depth' && d.data) {
                    for (const b of d.data) recordOrderbook('Toobit', sym, b.b, b.a);
                }
                return [];
            } catch (e) { return []; }
        }
    },

    'Deepcoin': {
        tier: 2, noOrderbook: true,
        getUrl: () => 'wss://stream.deepcoin.com/streamlet/trade/public/spot?platform=api',
        pingInterval: 15000, pingMessage: 'ping',
        onOpen: (ws) => {
            const reqId = Date.now();
            ['BTCUSDT','ETHUSDT','SOLUSDT'].forEach((s, i) => {
                ws.send(JSON.stringify({ SendTopicAction: { Action: '1', FilterValue: `DeepCoin_${s}`, LocalNo: reqId + i, ResumeNo: -2, TopicID: '2' } }));
            });
        },
        parseMessage: (msg) => {
            try {
                if (msg === 'pong') return [];
                const d = JSON.parse(msg);
                if (d.a === 'PMT' && d.r && Array.isArray(d.r)) {
                    for (const row of d.r) {
                        const inst = row.d?.I || '';
                        const sym = inst.replace('DeepCoin_', '');
                        recordTrade('Deepcoin', sym, row.d?.P, row.d?.V, row.d?.S === '2' ? 'sell' : 'buy');
                    }
                }
                return [];
            } catch (e) { return []; }
        }
    },

    'XT.com': {
        tier: 2,
        getUrl: () => 'wss://stream.xt.com/public',
        pingInterval: 15000, pingMessage: 'ping',
        onOpen: (ws) => {
            for (const s of ['btc_usdt','eth_usdt','sol_usdt']) {
                ws.send(JSON.stringify({ method:'subscribe', params:[`depth_update@${s}`,`trade@${s}`] }));
            }
        },
        parseMessage: (msg) => {
            try {
                if (msg === 'pong') return [];
                const d = JSON.parse(msg);
                const topic = d.event || d.topic || '';
                if (!topic || !topic.includes('@')) return [];
                const sym = topic.split('@')[1] || '';
                if (!sym) return [];
                if ((topic.startsWith('trade@') || topic.startsWith('trade')) && d.data) {
                    for (const t of (Array.isArray(d.data) ? d.data : [d.data]))
                        recordTrade('XT.com', sym, t.p, t.q, t.b ? 'buy' : 'sell');
                }
                if ((topic.startsWith('depth_update@') || topic.startsWith('depth@') || topic.startsWith('depth')) && d.data) {
                    recordOrderbook('XT.com', sym, d.data.b || d.data.bids, d.data.a || d.data.asks);
                }
                return [];
            } catch (e) { return []; }
        }
    },

    'Zoomex': {
        tier: 2,
        getUrl: () => 'wss://stream.zoomex.com/v5/public/spot',
        pingInterval: 20000, pingMessage: JSON.stringify({ op: 'ping' }),
        onOpen: (ws) => {
            const syms = ['BTCUSDT','ETHUSDT','SOLUSDT'];
            ws.send(JSON.stringify({ op:'subscribe', args: syms.flatMap(s => [`orderbook.50.${s}`,`publicTrade.${s}`]) }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.topic || !d.data) return [];
                const parts = d.topic.split('.');
                const sym = parts[parts.length - 1];
                if (d.topic.startsWith('publicTrade')) {
                    for (const t of (Array.isArray(d.data) ? d.data : [d.data]))
                        recordTrade('Zoomex', sym, t.p, t.v, t.S === 'Buy' ? 'buy' : 'sell');
                }
                if (d.topic.startsWith('orderbook')) {
                    recordOrderbook('Zoomex', sym, d.data.b, d.data.a);
                }
                return [];
            } catch (e) { return []; }
        }
    },

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
                if (d.action === 'ping' || d.type === 'pong') return [];
                const sym = d.pair || '';
                if (d.trade) {
                    const t = d.trade;
                    recordTrade('LBank', sym, t.price, t.volume, t.direction);
                } else if (d.type === 'trade' && d.data) {
                    recordTrade('LBank', sym, d.data.price, d.data.volume, d.data.direction);
                }
                if (d.depth) {
                    recordOrderbook('LBank', sym, d.depth.bids, d.depth.asks);
                } else if (d.type === 'depth' && d.data) {
                    recordOrderbook('LBank', sym, d.data.bids, d.data.asks);
                }
                return [];
            } catch (e) { return []; }
        }
    },

    'BitMart': {
        tier: 3,
        getUrl: () => 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1',
        compression: 'inflate',
        pingInterval: 10000, pingMessage: 'ping',
        onOpen: (ws) => {
            ws.send(JSON.stringify({ op:'subscribe', args: ['BTC_USDT','ETH_USDT','SOL_USDT'].flatMap(p =>
                [`spot/trade:${p}`,`spot/depth5:${p}`]
            )}));
        },
        parseMessage: (msg) => {
            try {
                if (msg === 'pong') return [];
                const d = JSON.parse(msg);
                if (!d.data) return [];
                const table = d.table || '';
                if (table === 'spot/trade' && Array.isArray(d.data)) {
                    for (const t of d.data) recordTrade('BitMart', t.symbol, t.price, t.size || t.s_q, t.side);
                }
                if (table === 'spot/depth5' && Array.isArray(d.data)) {
                    for (const b of d.data) recordOrderbook('BitMart', b.symbol, b.bids, b.asks);
                }
                return [];
            } catch (e) { return []; }
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
                if (!d.topic) return [];
                const sym = d.symbol || '';
                if (d.topic === 'TRADE' && d.data) {
                    for (const t of (Array.isArray(d.data) ? d.data : [d.data]))
                        recordTrade('Pionex', sym, t.price, t.size, t.side?.toLowerCase());
                }
                if (d.topic === 'DEPTH' && d.data) {
                    recordOrderbook('Pionex', sym, d.data.bids, d.data.asks);
                }
                return [];
            } catch (e) { return []; }
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
                if (!d.channel || !d.data) return [];
                const items = Array.isArray(d.data) ? d.data : [d.data];
                for (const item of items) {
                    const sym = item.symbol || '';
                    if (d.channel === 'trades') recordTrade('Poloniex', sym, item.price, item.quantity, item.takerSide);
                    if (d.channel === 'book') recordOrderbook('Poloniex', sym, item.bids, item.asks);
                }
                return [];
            } catch (e) { return []; }
        }
    },

    'BTSE': {
        tier: 3,
        getUrl: () => 'wss://ws.btse.com/ws/spot',
        ossUrl: 'wss://ws.btse.com/ws/oss/spot',
        pingInterval: 30000, pingMessage: 'ping',
        onOpen: (ws) => {
            ws.send(JSON.stringify({ op:'subscribe', args:['tradeHistoryApi:BTC-USD','tradeHistoryApi:ETH-USD','tradeHistoryApi:SOL-USD'] }));
        },
        ossOnOpen: (ws) => {
            ws.send(JSON.stringify({ op:'subscribe', args:['update:BTC-USD_0','update:ETH-USD_0','update:SOL-USD_0'] }));
        },
        parseMessage: (msg) => {
            try {
                if (msg === 'pong') return [];
                const d = JSON.parse(msg);
                // Trades
                if (d.topic && d.topic.startsWith('tradeHistoryApi') && d.data) {
                    const sym = d.topic.replace('tradeHistoryApi:','');
                    for (const t of d.data) recordTrade('BTSE', sym, t.price, t.size || t.amount, t.side?.toLowerCase());
                }
                // Orderbook (OSS)
                if (d.topic && d.topic.startsWith('update:') && d.data) {
                    const sym = d.topic.replace('update:','').replace('_0','');
                    recordOrderbook('BTSE', sym, d.data.bids || d.data.buyQuote, d.data.asks || d.data.sellQuote);
                }
                return [];
            } catch (e) { return []; }
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
                if (!d.ch) return [];
                if (d.ch === 'trades' && d.update) {
                    for (const [sym, trades] of Object.entries(d.update)) {
                        for (const t of trades) recordTrade('HitBTC', sym, t.p, t.q, t.s?.toLowerCase());
                    }
                }
                if (d.ch === 'orderbook/full') {
                    const payload = d.snapshot || d.update || {};
                    for (const [sym, ob] of Object.entries(payload)) {
                        recordOrderbook('HitBTC', sym, ob.b || ob.bid, ob.a || ob.ask);
                    }
                }
                return [];
            } catch (e) { return []; }
        }
    },

    'Biconomy': {
        tier: 3,
        getUrl: () => 'wss://bei.biconomy.com/ws',
        pingInterval: 30000, pingMessage: JSON.stringify({ method:'server.ping', params:[], id:5160 }),
        extraConnections: [
            { getUrl: () => 'wss://bei.biconomy.com/ws',
              onOpen: (ws) => {
                ws.send(JSON.stringify({ method:'depth.subscribe', params:['ETH_USDT',5,'0'], id:21 }));
                ws.send(JSON.stringify({ method:'deals.subscribe', params:['ETH_USDT'], id:22 }));
              }
            },
            { getUrl: () => 'wss://bei.biconomy.com/ws',
              onOpen: (ws) => {
                ws.send(JSON.stringify({ method:'depth.subscribe', params:['SOL_USDT',5,'0'], id:31 }));
                ws.send(JSON.stringify({ method:'deals.subscribe', params:['SOL_USDT'], id:32 }));
              }
            }
        ],
        onOpen: (ws) => {
            ws.send(JSON.stringify({ method:'depth.subscribe', params:['BTC_USDT',5,'0'], id:11 }));
            ws.send(JSON.stringify({ method:'deals.subscribe', params:['BTC_USDT'], id:12 }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.method === 'depth.update' && d.params) {
                    const sym = d.params[2] || '';
                    if (d.params[1]) recordOrderbook('Biconomy', sym, d.params[1].bids, d.params[1].asks);
                }
                if (d.method === 'deals.update' && d.params) {
                    const sym = d.params[0] || '';
                    if (Array.isArray(d.params[1])) {
                        for (const t of d.params[1]) recordTrade('Biconomy', sym, t.price, t.amount, t.type);
                    }
                }
                return [];
            } catch (e) { return []; }
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
                if (d.ping) return [];
                if (!d.ch || !d.data) return [];
                const ch = d.ch;
                const parts = ch.split('.');
                if (parts.length < 3) return [];
                const sym = parts[1]; // btc_usdt
                if (ch.includes('trade.detail')) {
                    const trades = Array.isArray(d.data) ? d.data : (d.data.data ? d.data.data : [d.data]);
                    for (const t of trades) recordTrade('Hotcoin', sym, t.price, t.amount, t.direction);
                }
                if (ch.includes('trade.depth')) {
                    recordOrderbook('Hotcoin', sym, d.data.buys || d.data.bids, d.data.asks);
                }
                return [];
            } catch (e) { return []; }
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
                if (d.event === 'pong' || d.event === 'subscribe') return [];
                const topic = d.topic || '';
                const sym = topic.split('@')[0];
                if (!sym.startsWith('SPOT_')) return [];
                if (topic.includes('@trade') && d.data) {
                    recordTrade('NovaEx', sym, d.data.price, d.data.size, d.data.side?.toLowerCase());
                }
                if (topic.includes('@orderbook') && d.data) {
                    recordOrderbook('NovaEx', sym, d.data.bids, d.data.asks);
                }
                return [];
            } catch (e) { return []; }
        }
    },

    // ═══════════════════════ REST API EXCHANGES ═══════════════════════

    'FameEX': {
        tier: 3, type: 'rest',
        symbols: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'],
        endpoints: {
            trades: (sym) => `https://api.fameex.com/sapi/v1/trades?symbol=${sym}&limit=10`,
            orderbook: (sym) => `https://api.fameex.com/v2/public/orderbook?symbol=${sym}&limit=5`,
        },
        parseTrades: (resp, sym) => {
            // Response: [{side, price, qty, time}]
            if (Array.isArray(resp)) {
                for (const t of resp) recordTrade('FameEX', sym, t.price, t.qty, t.side);
            }
        },
        parseOrderbook: (resp, sym) => {
            // Response: {asks: [[price,qty]], bids: [[price,qty]], timestamp}
            if (resp && resp.asks && resp.bids) {
                recordOrderbook('FameEX', sym, resp.bids, resp.asks);
            }
        },
    },

    'Websea': {
        tier: 3, type: 'rest',
        symbols: ['BTC-USDT', 'ETH-USDT', 'SOL-USDT'],
        endpoints: {
            trades: (sym) => `https://oapi.websea.com/v1/spot/trade?symbol=${sym}&size=10`,
            orderbook: (sym) => `https://oapi.websea.com/v1/spot/depth?symbol=${sym}&size=5`,
        },
        parseTrades: (resp, sym) => {
            // Response: {errno:0, result: {data: [{price, amount, direction, ts}]}}
            if (resp?.errno !== 0 || !resp.result?.data) return;
            for (const t of resp.result.data) recordTrade('Websea', sym, t.price, t.amount, t.direction);
        },
        parseOrderbook: (resp, sym) => {
            // Response: {errno:0, result: {asks: [["price","qty"]], bids: [["price","qty"]]}}
            if (resp?.errno !== 0 || !resp.result) return;
            recordOrderbook('Websea', sym, resp.result.bids, resp.result.asks);
        },
    },
};

// ======================== 24/7 ERROR HANDLING ========================

process.on('uncaughtException', (err) => {
    console.error(`  [${ts()}] ⚠️  Uncaught exception: ${err.message}`);
});
process.on('unhandledRejection', (reason) => {
    console.error(`  [${ts()}] ⚠️  Unhandled rejection: ${reason}`);
});

// Graceful shutdown
let shuttingDown = false;
const activeWsList = new Set();

function registerWs(ws) { activeWsList.add(ws); }
function unregisterWs(ws) { activeWsList.delete(ws); }

async function gracefulShutdown(conn) {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`\n  [${ts()}] 🛑 Graceful shutdown initiated...`);

    // Close all active WebSockets
    for (const ws of activeWsList) {
        try { ws.terminate(); } catch (e) {}
    }
    activeWsList.clear();

    // Final flush
    if (conn) {
        try {
            const finalRows = await flushToDuckDB(conn);
            console.log(`  [${ts()}] 💾 Final flush: ${finalRows} rows saved`);
        } catch (e) {
            console.error(`  [${ts()}] ❌ Final flush failed: ${e.message}`);
        }
    }

    console.log(`  [${ts()}] ✅ Shutdown complete`);
    process.exit(0);
}

// ======================== TEST ENGINE ========================

async function testExchange(name, retries = 2) {
    const exDef = EXCHANGES[name];
    initStats(name);
    console.log(`  [${ts()}] 🔌 ${name} — Connecting...`);

    let wsUrl, kucoinPing;
    try {
        const urlResult = await exDef.getUrl();
        if (typeof urlResult === 'object') { wsUrl = urlResult.url; kucoinPing = urlResult.pingInterval; }
        else { wsUrl = urlResult; }
    } catch (e) {
        console.log(`  [${ts()}] ❌ ${name} — URL failed: ${e.message}`);
        if (retries > 0) {
            console.log(`  [${ts()}] 🔄 ${name} — Retrying in 3s (${retries} left)...`);
            await new Promise(r => setTimeout(r, 3000));
            return testExchange(name, retries - 1);
        }
        stats[name].errors.push(e.message); stats[name].status = 'error'; return;
    }

    return new Promise((resolve) => {
        const ctx = {};
        let resolved = false;
        const done = () => { if (!resolved) { resolved = true; resolve(); } };

        const timeout = setTimeout(() => {
            console.log(`  [${ts()}] ⏰ ${name} — Timeout`);
            ws.terminate();
            if (ossWs) try { ossWs.terminate(); } catch(e){}
            for (const ecWs of extraWsList) { try { ecWs.terminate(); } catch(e){} }
            stats[name].status = 'timeout'; done();
        }, TEST_TIMEOUT);

        let ws, ossWs;
        const extraWsList = [];
        try {
            ws = new WebSocket(wsUrl, {
                headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
                handshakeTimeout: 10000,
                perMessageDeflate: exDef.compression ? false : undefined
            });
            registerWs(ws);
        } catch (e) {
            clearTimeout(timeout); stats[name].errors.push(e.message); stats[name].status = 'error'; done(); return;
        }

        let pingTimer, customPingTimer, ossPingTimer;

        // OSS (dual WS for BTSE)
        if (exDef.ossUrl && exDef.ossOnOpen) {
            try {
                ossWs = new WebSocket(exDef.ossUrl, {
                    headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' }, handshakeTimeout: 10000 });
                ossWs.on('open', () => {
                    try { exDef.ossOnOpen(ossWs); } catch(e){}
                    if (exDef.pingInterval && exDef.pingMessage)
                        ossPingTimer = setInterval(() => { if (ossWs.readyState === WebSocket.OPEN) ossWs.send(exDef.pingMessage); }, exDef.pingInterval);
                });
                ossWs.on('message', (data) => {
                    const str = data.toString();
                    try { exDef.parseMessage(str, ctx); } catch(e){}
                });
                ossWs.on('error', ()=>{});
                ossWs.on('close', () => { if (ossPingTimer) clearInterval(ossPingTimer); });
            } catch(e) { stats[name].errors.push(`OSS: ${e.message}`); }
        }

        // Extra connections (Biconomy)
        if (exDef.extraConnections) {
            for (const ec of exDef.extraConnections) {
                try {
                    const ecUrl = typeof ec.getUrl === 'function' ? ec.getUrl() : ec.getUrl;
                    const ecWs = new WebSocket(ecUrl, {
                        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' }, handshakeTimeout: 10000 });
                    extraWsList.push(ecWs);
                    ecWs.on('open', () => {
                        try { ec.onOpen(ecWs); } catch(e){}
                        if (exDef.pingInterval && exDef.pingMessage)
                            ecWs._pt = setInterval(() => { if (ecWs.readyState === WebSocket.OPEN) ecWs.send(exDef.pingMessage); }, exDef.pingInterval);
                    });
                    ecWs.on('message', (data) => {
                        let str;
                        if (exDef.compression && Buffer.isBuffer(data)) {
                            try { str = exDef.compression === 'gzip' ? zlib.gunzipSync(data).toString() : zlib.inflateSync(data).toString(); } catch(e) { str = data.toString(); }
                        } else { str = data.toString(); }
                        if (exDef.handlePing) { try { const p = JSON.parse(str); if (exDef.handlePing(p, ecWs)) return; } catch(e){} }
                        try { exDef.parseMessage(str, ctx); } catch(e){}
                    });
                    ecWs.on('error', ()=>{});
                    ecWs.on('close', () => { if (ecWs._pt) clearInterval(ecWs._pt); });
                } catch(e){}
            }
        }

        ws.on('open', () => {
            clearTimeout(timeout);
            console.log(`  [${ts()}] ✅ ${name} — Connected`);
            try { exDef.onOpen(ws); } catch(e) { stats[name].errors.push(e.message); }

            const pi = exDef.pingInterval || kucoinPing;
            if (pi && exDef.pingMessage)
                pingTimer = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.send(exDef.pingMessage); }, pi);
            if (exDef.customPingSetup) customPingTimer = exDef.customPingSetup(ws);

            setTimeout(() => {
                if (pingTimer) clearInterval(pingTimer);
                if (customPingTimer) clearInterval(customPingTimer);
                if (ossPingTimer) clearInterval(ossPingTimer);
                const s = stats[name];
                const total = s.trades + s.orderbook;
                s.status = total > 0 ? 'ok' : 'no_data';
                console.log(`  [${ts()}] 📊 ${name} — TR=${s.trades} OB=${s.orderbook}`);
                try { ws.close(); } catch(e){}
                if (ossWs) try { ossWs.close(); } catch(e){}
                for (const ecWs of extraWsList) { try { ecWs.close(); } catch(e){} }
                setTimeout(done, 500);
            }, DATA_WAIT);
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
            console.log(`  [${ts()}] ❌ ${name} — ${err.message}`);
            stats[name].errors.push(err.message);
            unregisterWs(ws);
        });
        ws.on('close', () => {
            if (pingTimer) clearInterval(pingTimer);
            if (customPingTimer) clearInterval(customPingTimer);
            unregisterWs(ws);
        });
    });
}

// ======================== REST POLLING ENGINE ========================

async function testRESTExchange(name) {
    const exDef = EXCHANGES[name];
    initStats(name);
    console.log(`  [${ts()}] 🌐 ${name} — Polling REST API...`);

    const symbols = exDef.symbols;
    const pollRounds = 4;           // Poll 4 rounds
    const pollDelay = 5000;         // 5s between rounds

    for (let round = 0; round < pollRounds; round++) {
        for (const sym of symbols) {
            // Trades
            if (exDef.endpoints.trades) {
                try {
                    const resp = await httpsRequest(exDef.endpoints.trades(sym));
                    exDef.parseTrades(resp, sym);
                } catch (e) {
                    if (round === 0) {
                        stats[name].errors.push(`trades(${sym}): ${e.message}`);
                        console.log(`  [${ts()}] ⚠️  ${name} — trades(${sym}) failed: ${e.message}`);
                    }
                }
            }
            // Orderbook
            if (exDef.endpoints.orderbook) {
                try {
                    const resp = await httpsRequest(exDef.endpoints.orderbook(sym));
                    exDef.parseOrderbook(resp, sym);
                } catch (e) {
                    if (round === 0) {
                        stats[name].errors.push(`orderbook(${sym}): ${e.message}`);
                        console.log(`  [${ts()}] ⚠️  ${name} — orderbook(${sym}) failed: ${e.message}`);
                    }
                }
            }
        }
        if (round < pollRounds - 1) await new Promise(r => setTimeout(r, pollDelay));
    }

    const s = stats[name];
    const total = s.trades + s.orderbook;
    s.status = total > 0 ? 'ok' : 'no_data';
    console.log(`  [${ts()}] 📊 ${name} — TR=${s.trades} OB=${s.orderbook} (REST)`);
}

// ======================== DUCKDB FLUSH ========================

function esc(v) {
    if (v === null || v === undefined) return 'NULL';
    if (typeof v === 'number') return isNaN(v) ? 'NULL' : String(v);
    return `'${String(v).replace(/'/g, "''")}'`;
}

async function flushToDuckDB(conn) {
    let totalRows = 0;

    // Flush trades in batches of 500
    for (let i = 0; i < tradeBuffer.length; i += 500) {
        const batch = tradeBuffer.slice(i, i + 500);
        const values = batch.map(t =>
            `(${esc(t.ts)}, ${esc(t.exchange)}, ${esc(t.canonical_pair)}, ${t.price || 0}, ${t.qty || 0}, ${esc(t.side)})`
        ).join(',\n');
        await conn.run(`INSERT INTO trades VALUES ${values}`);
        totalRows += batch.length;
    }
    tradeBuffer = [];

    // Flush orderbook
    for (let i = 0; i < obBuffer.length; i += 500) {
        const batch = obBuffer.slice(i, i + 500);
        const values = batch.map(o =>
            `(${esc(o.ts)}, ${esc(o.exchange)}, ${esc(o.canonical_pair)}, ${esc(o.bids)}, ${esc(o.asks)})`
        ).join(',\n');
        await conn.run(`INSERT INTO orderbook VALUES ${values}`);
        totalRows += batch.length;
    }
    obBuffer = [];

    return totalRows;
}

// ======================== REPORT ========================

async function printReport(conn) {
    console.log('\n');
    console.log('╔' + '═'.repeat(100) + '╗');
    console.log('║  NORMALIZED STREAM TEST — Canonical Pair System Report' + ' '.repeat(45) + '║');
    console.log('╚' + '═'.repeat(100) + '╝');

    // Per-exchange stats
    console.log('\n  ── Per-Exchange Data Collection ──\n');
    console.log('  ' + 'Exchange'.padEnd(15) + 'T'.padEnd(3) + '│ Trades'.padEnd(11) + 'Orderbook'.padEnd(11) + '│ Status');
    console.log('  ' + '─'.repeat(65));

    const sorted = Object.keys(EXCHANGES).sort((a, b) => {
        const ta = EXCHANGES[a].tier, tb = EXCHANGES[b].tier;
        if (ta !== tb) return ta - tb;
        return a.localeCompare(b);
    });

    let allOk = 0;
    for (const name of sorted) {
        const s = stats[name] || { trades: 0, orderbook: 0, status: 'unknown' };
        const total = s.trades + s.orderbook;
        const emoji = total > 0 ? '✅' : '❌';
        if (total > 0) allOk++;
        console.log('  ' + name.padEnd(15) + `${EXCHANGES[name].tier}`.padEnd(3) +
            `│ ${String(s.trades).padStart(5)}   ${String(s.orderbook).padStart(5)}   │ ${emoji} ${s.status}`);
    }
    console.log('  ' + '─'.repeat(65));
    console.log(`  ${allOk}/${sorted.length} exchanges collected data\n`);

    // DuckDB queries
    console.log('  ── DuckDB Table Stats ──\n');

    let reader;
    reader = await conn.runAndReadAll('SELECT COUNT(*) FROM trades');
    console.log(`  trades:    ${reader.getRows()[0][0]} rows`);

    reader = await conn.runAndReadAll('SELECT COUNT(*) FROM orderbook');
    console.log(`  orderbook: ${reader.getRows()[0][0]} rows`);

    // Per canonical pair breakdown
    console.log('\n  ── Trades per Canonical Pair ──\n');
    reader = await conn.runAndReadAll(`
        SELECT canonical_pair, COUNT(*) as cnt, COUNT(DISTINCT exchange) as exchanges,
               ROUND(AVG(price), 2) as avg_price
        FROM trades GROUP BY canonical_pair ORDER BY canonical_pair
    `);
    console.log('  ' + 'Canonical Pair'.padEnd(14) + 'Trades'.padEnd(10) + 'Exchanges'.padEnd(12) + 'Avg Price');
    console.log('  ' + '─'.repeat(50));
    for (const row of reader.getRows()) {
        console.log('  ' + String(row[0]).padEnd(14) + String(row[1]).padEnd(10) + String(row[2]).padEnd(12) + String(row[3]));
    }

    // Per-exchange × canonical_pair matrix
    console.log('\n  ── Trades Matrix: Exchange × Canonical Pair ──\n');
    reader = await conn.runAndReadAll(`
        SELECT exchange, canonical_pair, COUNT(*) as cnt
        FROM trades
        GROUP BY exchange, canonical_pair
        ORDER BY exchange, canonical_pair
    `);
    
    const matrix = {};
    const allPairs = new Set();
    for (const row of reader.getRows()) {
        if (!matrix[row[0]]) matrix[row[0]] = {};
        matrix[row[0]][row[1]] = row[2];
        allPairs.add(row[1]);
    }
    
    const pairList = [...allPairs].sort();
    let hdr = '  Exchange'.padEnd(17);
    for (const p of pairList) hdr += p.padEnd(12);
    console.log(hdr);
    console.log('  ' + '─'.repeat(17 + pairList.length * 12));
    for (const ex of sorted) {
        if (!matrix[ex]) continue;
        let line = `  ${ex}`.padEnd(17);
        for (const p of pairList) line += (matrix[ex][p] ? String(matrix[ex][p]) : '-').padEnd(12);
        console.log(line);
    }

    // Orderbook per canonical pair
    console.log('\n  ── Orderbook Snapshots per Canonical Pair ──\n');
    reader = await conn.runAndReadAll(`
        SELECT canonical_pair, COUNT(*) as cnt, COUNT(DISTINCT exchange) as exchanges
        FROM orderbook GROUP BY canonical_pair ORDER BY canonical_pair
    `);
    for (const row of reader.getRows()) {
        console.log(`  ${String(row[0]).padEnd(14)} ${String(row[1]).padEnd(10)} from ${row[2]} exchanges`);
    }

    // Orderbook matrix
    console.log('\n  ── Orderbook Matrix: Exchange × Canonical Pair ──\n');
    reader = await conn.runAndReadAll(`
        SELECT exchange, canonical_pair, COUNT(*) as cnt
        FROM orderbook
        GROUP BY exchange, canonical_pair
        ORDER BY exchange, canonical_pair
    `);
    const obMatrix = {};
    const obPairs = new Set();
    for (const row of reader.getRows()) {
        if (!obMatrix[row[0]]) obMatrix[row[0]] = {};
        obMatrix[row[0]][row[1]] = row[2];
        obPairs.add(row[1]);
    }
    const obPairList = [...obPairs].sort();
    let obHdr = '  Exchange'.padEnd(17);
    for (const p of obPairList) obHdr += p.padEnd(12);
    console.log(obHdr);
    console.log('  ' + '─'.repeat(17 + obPairList.length * 12));
    for (const ex of sorted) {
        if (!obMatrix[ex]) continue;
        let line = `  ${ex}`.padEnd(17);
        for (const p of obPairList) line += (obMatrix[ex][p] ? String(obMatrix[ex][p]) : '-').padEnd(12);
        console.log(line);
    }

    // Combined summary: Exchange × Pair (Trades + OB)
    console.log('\n  ── Combined Summary: Exchange × Pair (Trades / Orderbook) ──\n');
    reader = await conn.runAndReadAll(`
        SELECT e.exchange, e.canonical_pair,
               COALESCE(t.tc, 0) as trades,
               COALESCE(o.oc, 0) as orderbook
        FROM (SELECT DISTINCT exchange, canonical_pair FROM (SELECT exchange, canonical_pair FROM trades UNION ALL SELECT exchange, canonical_pair FROM orderbook)) e
        LEFT JOIN (SELECT exchange, canonical_pair, COUNT(*) as tc FROM trades GROUP BY exchange, canonical_pair) t
          ON e.exchange = t.exchange AND e.canonical_pair = t.canonical_pair
        LEFT JOIN (SELECT exchange, canonical_pair, COUNT(*) as oc FROM orderbook GROUP BY exchange, canonical_pair) o
          ON e.exchange = o.exchange AND e.canonical_pair = o.canonical_pair
        ORDER BY e.exchange, e.canonical_pair
    `);
    console.log('  ' + 'Exchange'.padEnd(15) + 'Canonical Pair'.padEnd(14) + 'Trades'.padEnd(10) + 'Orderbook');
    console.log('  ' + '─'.repeat(50));
    let totalTrades = 0, totalOB = 0;
    for (const row of reader.getRows()) {
        console.log(`  ${String(row[0]).padEnd(15)} ${String(row[1]).padEnd(14)} ${String(row[2]).padEnd(10)} ${row[3]}`);
        totalTrades += Number(row[2]); totalOB += Number(row[3]);
    }
    console.log('  ' + '─'.repeat(50));
    console.log(`  ${'TOTAL'.padEnd(15)} ${''.padEnd(14)} ${String(totalTrades).padEnd(10)} ${totalOB}`);

    // Normalization validation
    console.log('\n  ── Normalization Validation ──\n');
    reader = await conn.runAndReadAll(`
        SELECT sm.exchange, sm.exchange_symbol, sm.canonical_pair,
               (SELECT COUNT(*) FROM trades t WHERE t.exchange = sm.exchange AND t.canonical_pair = sm.canonical_pair) as trade_count,
               (SELECT COUNT(*) FROM orderbook o WHERE o.exchange = sm.exchange AND o.canonical_pair = sm.canonical_pair) as ob_count
        FROM symbol_map sm
        WHERE sm.is_active = true
        AND sm.canonical_pair IN ('BTC_USDT','ETH_USDT','SOL_USDT','BTC_USD','ETH_USD','SOL_USD')
        ORDER BY sm.canonical_pair, sm.exchange
    `);
    console.log('  ' + 'Exchange'.padEnd(15) + 'Raw Symbol'.padEnd(18) + 'Canonical'.padEnd(12) + 'Trades'.padEnd(9) + 'OB');
    console.log('  ' + '─'.repeat(60));
    for (const row of reader.getRows()) {
        const emoji = (row[3] > 0 || row[4] > 0) ? '✅' : '⬜';
        console.log(`  ${emoji} ${String(row[0]).padEnd(13)} ${String(row[1]).padEnd(18)} ${String(row[2]).padEnd(12)} ${String(row[3]).padEnd(9)} ${row[4]}`);
    }

    console.log('\n  ═══════════════════════════════════════════════');
    console.log(`  ✅ Normalization complete — all data stored with canonical pairs`);
    console.log('  ═══════════════════════════════════════════════\n');
}

// ======================== MAIN ========================

async function main() {
    console.log('\n╔' + '═'.repeat(78) + '╗');
    console.log('║  NORMALIZED STREAM TESTER — Canonical Pair System                            ║');
    console.log('║  30 Exchanges × BTC/ETH/SOL × USD/USDT/USDC                                 ║');
    console.log('║  All data normalized → DuckDB (streaming.duckdb)                             ║');
    console.log('╚' + '═'.repeat(78) + '╝\n');

    // Build symbol map
    buildSymbolMap();
    console.log(`  📋 Symbol map loaded: ${Object.keys(SYMBOL_MAP).length} mappings\n`);

    // Open DuckDB
    const instance = await DuckDBInstance.create('streaming.duckdb');
    const conn = await instance.connect();

    // Verify tables exist
    try {
        await conn.runAndReadAll('SELECT 1 FROM trades LIMIT 1');
    } catch (e) {
        console.log('  ❌ Tables not found. Run init-schema.js first!');
        process.exit(1);
    }

    // Clear previous test data
    await conn.run('DELETE FROM trades');
    await conn.run('DELETE FROM orderbook');
    console.log('  🗑️  Cleared previous test data\n');

    // Setup graceful shutdown
    process.on('SIGINT', () => gracefulShutdown(conn));
    process.on('SIGTERM', () => gracefulShutdown(conn));

    const exchangeNames = Object.keys(EXCHANGES);
    const wsExchanges = exchangeNames.filter(n => !EXCHANGES[n].type || EXCHANGES[n].type === 'ws');
    const restExchanges = exchangeNames.filter(n => EXCHANGES[n].type === 'rest');
    console.log(`  🚀 Starting ${exchangeNames.length} exchanges (${wsExchanges.length} WebSocket + ${restExchanges.length} REST)...\n`);

    // Run WebSocket exchanges in batches
    for (let i = 0; i < wsExchanges.length; i += MAX_CONCURRENT) {
        const batch = wsExchanges.slice(i, i + MAX_CONCURRENT);
        console.log(`\n  ── WS Batch ${Math.floor(i/MAX_CONCURRENT)+1}: ${batch.join(', ')} ──\n`);
        const promises = batch.map((name, idx) =>
            new Promise(r => setTimeout(r, idx * STAGGER_DELAY)).then(() => testExchange(name))
        );
        await Promise.all(promises);
        if (i + MAX_CONCURRENT < wsExchanges.length || restExchanges.length > 0) {
            console.log(`\n  ⏳ Waiting 2s...\n`);
            await new Promise(r => setTimeout(r, 2000));
        }
    }

    // Run REST exchanges in parallel
    if (restExchanges.length > 0) {
        console.log(`\n  ── REST Batch: ${restExchanges.join(', ')} ──\n`);
        await Promise.all(restExchanges.map(name => testRESTExchange(name)));
    }

    // Flush all buffered data to DuckDB
    console.log('\n  💾 Flushing data to DuckDB...');
    const totalRows = await flushToDuckDB(conn);
    console.log(`  💾 Flushed ${totalRows} total rows\n`);

    // Print report
    await printReport(conn);

    // conn disposed by GC
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
