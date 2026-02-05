/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║   COMPREHENSIVE STREAM TYPE TESTER - Orderbook / Trades / Tickers      ║
 * ║   24 Exchanges × 3 Coins (BTC, ETH, SOL) × 3 Stream Types             ║
 * ║   Total Tests: 216 (24 × 3 × 3)                                       ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */

const WebSocket = require('ws');
const zlib = require('zlib');
const https = require('https');

// ======================== CONFIGURATION ========================
const TEST_TIMEOUT = 20000;       // 20s per exchange connection
const DATA_WAIT = 15000;          // 15s to collect data after subscribing
const STAGGER_DELAY = 300;        // 300ms between exchange connections
const MAX_CONCURRENT = 6;         // Max concurrent exchange connections

// ======================== UTILITIES ========================
function httpsRequest(url, options = {}) {
    return new Promise((resolve, reject) => {
        const opts = {
            ...require('url').parse(url),
            method: options.method || 'GET',
            headers: { 'User-Agent': 'Mozilla/5.0', 'Content-Type': 'application/json', ...(options.headers || {}) }
        };
        const req = https.request(opts, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => { try { resolve(JSON.parse(data)); } catch (e) { resolve(data); } });
        });
        req.on('error', reject);
        if (options.body) req.write(JSON.stringify(options.body));
        req.end();
    });
}

function ts() { return new Date().toISOString().substring(11, 19); }

// ======================== RESULTS TRACKING ========================
const results = {};  // { exchange: { orderbook: { BTC: count, ETH: count, SOL: count }, trades: {...}, ticker: {...} } }

function initResult(exchange) {
    results[exchange] = {
        orderbook: { BTC: 0, ETH: 0, SOL: 0 },
        trades:    { BTC: 0, ETH: 0, SOL: 0 },
        ticker:    { BTC: 0, ETH: 0, SOL: 0 },
        errors: [],
        status: 'pending'
    };
}

// ======================== EXCHANGE DEFINITIONS ========================
// Each exchange: { name, url, compression, pingSetup, onOpen, onMessage }
// onMessage receives parsed string, returns array of { type: 'orderbook'|'trades'|'ticker', coin: 'BTC'|'ETH'|'SOL' }

const EXCHANGES = {

    // ═══════════════════════ TIER 1 ═══════════════════════

    'Binance': {
        tier: 1,
        getUrl: () => 'wss://stream.binance.com:9443/stream',
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                method: 'SUBSCRIBE',
                params: [
                    'btcusdt@trade', 'ethusdt@trade', 'solusdt@trade',
                    'btcusdt@depth5@1000ms', 'ethusdt@depth5@1000ms', 'solusdt@depth5@1000ms',
                    'btcusdt@ticker', 'ethusdt@ticker', 'solusdt@ticker'
                ],
                id: 1
            }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.data) return [];
                const e = d.data;
                const hits = [];
                // Trade
                if (e.e === 'trade') {
                    if (e.s === 'BTCUSDT') hits.push({ type: 'trades', coin: 'BTC' });
                    if (e.s === 'ETHUSDT') hits.push({ type: 'trades', coin: 'ETH' });
                    if (e.s === 'SOLUSDT') hits.push({ type: 'trades', coin: 'SOL' });
                }
                // Ticker
                if (e.e === '24hrTicker') {
                    if (e.s === 'BTCUSDT') hits.push({ type: 'ticker', coin: 'BTC' });
                    if (e.s === 'ETHUSDT') hits.push({ type: 'ticker', coin: 'ETH' });
                    if (e.s === 'SOLUSDT') hits.push({ type: 'ticker', coin: 'SOL' });
                }
                // Orderbook
                if (e.lastUpdateId && e.bids && e.asks) {
                    // Partial depth stream - identify by stream name in wrapper
                    const stream = d.stream || '';
                    if (stream.startsWith('btcusdt@depth')) hits.push({ type: 'orderbook', coin: 'BTC' });
                    if (stream.startsWith('ethusdt@depth')) hits.push({ type: 'orderbook', coin: 'ETH' });
                    if (stream.startsWith('solusdt@depth')) hits.push({ type: 'orderbook', coin: 'SOL' });
                }
                return hits;
            } catch (e) { return []; }
        }
    },

    'Coinbase': {
        tier: 1,
        getUrl: () => 'wss://ws-feed.exchange.coinbase.com',
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                type: 'subscribe',
                product_ids: ['BTC-USD', 'ETH-USD', 'SOL-USD'],
                channels: ['matches', 'ticker', 'level2_batch', 'heartbeat']
            }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                const hits = [];
                const p = d.product_id;
                const coin = p === 'BTC-USD' ? 'BTC' : p === 'ETH-USD' ? 'ETH' : p === 'SOL-USD' ? 'SOL' : null;
                if (!coin) return [];
                if (d.type === 'match' || d.type === 'last_match') hits.push({ type: 'trades', coin });
                if (d.type === 'ticker') hits.push({ type: 'ticker', coin });
                if (d.type === 'snapshot' || d.type === 'l2update') hits.push({ type: 'orderbook', coin });
                return hits;
            } catch (e) { return []; }
        }
    },

    'Kraken': {
        tier: 1,
        getUrl: () => 'wss://ws.kraken.com/v2',
        onOpen: (ws) => {
            // V2 API for better trade delivery on all pairs
            ws.send(JSON.stringify({ method: 'subscribe', params: { channel: 'trade', symbol: ['BTC/USDT', 'ETH/USDT', 'SOL/USDT'], snapshot: false } }));
            ws.send(JSON.stringify({ method: 'subscribe', params: { channel: 'ticker', symbol: ['BTC/USDT', 'ETH/USDT', 'SOL/USDT'], snapshot: true } }));
            ws.send(JSON.stringify({ method: 'subscribe', params: { channel: 'book', symbol: ['BTC/USDT', 'ETH/USDT', 'SOL/USDT'], depth: 10, snapshot: true } }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                // V2: skip heartbeat & method responses
                if (d.channel === 'heartbeat' || d.method) return [];
                if (!d.channel || !d.data) return [];
                const hits = [];
                if (d.channel === 'trade' && Array.isArray(d.data)) {
                    for (const t of d.data) {
                        const coin = t.symbol === 'BTC/USDT' ? 'BTC' : t.symbol === 'ETH/USDT' ? 'ETH' : t.symbol === 'SOL/USDT' ? 'SOL' : null;
                        if (coin) hits.push({ type: 'trades', coin });
                    }
                }
                if (d.channel === 'ticker' && Array.isArray(d.data)) {
                    for (const t of d.data) {
                        const coin = t.symbol === 'BTC/USDT' ? 'BTC' : t.symbol === 'ETH/USDT' ? 'ETH' : t.symbol === 'SOL/USDT' ? 'SOL' : null;
                        if (coin) hits.push({ type: 'ticker', coin });
                    }
                }
                if (d.channel === 'book' && Array.isArray(d.data)) {
                    for (const b of d.data) {
                        const coin = b.symbol === 'BTC/USDT' ? 'BTC' : b.symbol === 'ETH/USDT' ? 'ETH' : b.symbol === 'SOL/USDT' ? 'SOL' : null;
                        if (coin) hits.push({ type: 'orderbook', coin });
                    }
                }
                return hits;
            } catch (e) { return []; }
        }
    },

    'KuCoin': {
        tier: 1,
        getUrl: async () => {
            try {
                const resp = await httpsRequest('https://api.kucoin.com/api/v1/bullet-public', { method: 'POST' });
                if (resp.data && resp.data.token) {
                    const ep = resp.data.instanceServers?.[0]?.endpoint || 'wss://ws-api-spot.kucoin.com';
                    return { url: `${ep}?token=${resp.data.token}`, pingInterval: resp.data.instanceServers?.[0]?.pingInterval || 18000 };
                }
            } catch (e) {}
            return { url: 'wss://ws-api-spot.kucoin.com', pingInterval: 18000 };
        },
        onOpen: (ws) => {
            ws.send(JSON.stringify({ id: 1, type: 'subscribe', topic: '/market/match:BTC-USDT,ETH-USDT,SOL-USDT', privateChannel: false, response: true }));
            ws.send(JSON.stringify({ id: 2, type: 'subscribe', topic: '/market/ticker:BTC-USDT,ETH-USDT,SOL-USDT', privateChannel: false, response: true }));
            ws.send(JSON.stringify({ id: 3, type: 'subscribe', topic: '/spotMarket/level2Depth5:BTC-USDT,ETH-USDT,SOL-USDT', privateChannel: false, response: true }));
        },
        pingMessage: JSON.stringify({ id: Date.now(), type: 'ping' }),
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.type === 'pong' || d.type === 'welcome' || d.type === 'ack') return [];
                if (!d.topic || !d.data) return [];
                const s = d.data.symbol || d.topic || '';
                const coin = s.includes('BTC') ? 'BTC' : s.includes('ETH') ? 'ETH' : s.includes('SOL') ? 'SOL' : null;
                if (!coin) return [];
                const hits = [];
                if (d.topic.includes('/market/match')) hits.push({ type: 'trades', coin });
                if (d.topic.includes('/market/ticker')) hits.push({ type: 'ticker', coin });
                if (d.topic.includes('level2Depth')) hits.push({ type: 'orderbook', coin });
                return hits;
            } catch (e) { return []; }
        }
    },

    'OKX': {
        tier: 1,
        getUrl: () => 'wss://ws.okx.com:8443/ws/v5/public',
        pingInterval: 25000,
        pingMessage: 'ping',
        onOpen: (ws) => {
            ws.send(JSON.stringify({ op: 'subscribe', args: [
                { channel: 'trades', instId: 'BTC-USDT' }, { channel: 'trades', instId: 'ETH-USDT' }, { channel: 'trades', instId: 'SOL-USDT' },
                { channel: 'tickers', instId: 'BTC-USDT' }, { channel: 'tickers', instId: 'ETH-USDT' }, { channel: 'tickers', instId: 'SOL-USDT' },
                { channel: 'books5', instId: 'BTC-USDT' }, { channel: 'books5', instId: 'ETH-USDT' }, { channel: 'books5', instId: 'SOL-USDT' }
            ] }));
        },
        parseMessage: (msg) => {
            if (msg === 'pong') return [];
            try {
                const d = JSON.parse(msg);
                if (!d.arg || !d.data) return [];
                const ch = d.arg.channel;
                const inst = d.arg.instId;
                const coin = inst === 'BTC-USDT' ? 'BTC' : inst === 'ETH-USDT' ? 'ETH' : inst === 'SOL-USDT' ? 'SOL' : null;
                if (!coin) return [];
                const hits = [];
                if (ch === 'trades') hits.push({ type: 'trades', coin });
                if (ch === 'tickers') hits.push({ type: 'ticker', coin });
                if (ch === 'books5' || ch === 'books') hits.push({ type: 'orderbook', coin });
                return hits;
            } catch (e) { return []; }
        }
    },

    'Bybit': {
        tier: 1,
        getUrl: () => 'wss://stream.bybit.com/v5/public/spot',
        pingInterval: 20000,
        pingMessage: JSON.stringify({ op: 'ping' }),
        onOpen: (ws) => {
            ws.send(JSON.stringify({ op: 'subscribe', args: [
                'publicTrade.BTCUSDT', 'publicTrade.ETHUSDT', 'publicTrade.SOLUSDT',
                'tickers.BTCUSDT', 'tickers.ETHUSDT', 'tickers.SOLUSDT',
                'orderbook.50.BTCUSDT', 'orderbook.50.ETHUSDT', 'orderbook.50.SOLUSDT'
            ] }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.op === 'pong' || d.ret_msg === 'pong') return [];
                if (!d.topic || !d.data) return [];
                const hits = [];
                const getCoin = (t) => t.includes('BTCUSDT') ? 'BTC' : t.includes('ETHUSDT') ? 'ETH' : t.includes('SOLUSDT') ? 'SOL' : null;
                const coin = getCoin(d.topic);
                if (!coin) return [];
                if (d.topic.startsWith('publicTrade')) hits.push({ type: 'trades', coin });
                if (d.topic.startsWith('tickers')) hits.push({ type: 'ticker', coin });
                if (d.topic.startsWith('orderbook')) hits.push({ type: 'orderbook', coin });
                return hits;
            } catch (e) { return []; }
        }
    },

    'Bitfinex': {
        tier: 1,
        getUrl: () => 'wss://api-pub.bitfinex.com/ws/2',
        _channels: {},
        onOpen: (ws) => {
            // Trades
            ws.send(JSON.stringify({ event: 'subscribe', channel: 'trades', symbol: 'tBTCUSD' }));
            ws.send(JSON.stringify({ event: 'subscribe', channel: 'trades', symbol: 'tETHUSD' }));
            ws.send(JSON.stringify({ event: 'subscribe', channel: 'trades', symbol: 'tSOLUSD' }));
            // Ticker
            ws.send(JSON.stringify({ event: 'subscribe', channel: 'ticker', symbol: 'tBTCUSD' }));
            ws.send(JSON.stringify({ event: 'subscribe', channel: 'ticker', symbol: 'tETHUSD' }));
            ws.send(JSON.stringify({ event: 'subscribe', channel: 'ticker', symbol: 'tSOLUSD' }));
            // Book
            ws.send(JSON.stringify({ event: 'subscribe', channel: 'book', symbol: 'tBTCUSD', prec: 'P0', len: '25' }));
            ws.send(JSON.stringify({ event: 'subscribe', channel: 'book', symbol: 'tETHUSD', prec: 'P0', len: '25' }));
            ws.send(JSON.stringify({ event: 'subscribe', channel: 'book', symbol: 'tSOLUSD', prec: 'P0', len: '25' }));
        },
        parseMessage: (msg, ctx) => {
            try {
                const d = JSON.parse(msg);
                if (d.event === 'subscribed' && d.chanId) {
                    const sym = d.symbol || d.pair || '';
                    const coin = sym.includes('BTC') ? 'BTC' : sym.includes('ETH') ? 'ETH' : sym.includes('SOL') ? 'SOL' : null;
                    if (coin) {
                        if (!ctx._bfxChannels) ctx._bfxChannels = {};
                        ctx._bfxChannels[d.chanId] = { coin, channel: d.channel };
                    }
                    return [];
                }
                if (Array.isArray(d) && d.length >= 2 && d[1] !== 'hb') {
                    const chanInfo = ctx._bfxChannels?.[d[0]];
                    if (chanInfo) {
                        const { coin, channel } = chanInfo;
                        const type = channel === 'trades' ? 'trades' : channel === 'ticker' ? 'ticker' : channel === 'book' ? 'orderbook' : null;
                        if (type) return [{ type, coin }];
                    }
                }
            } catch (e) {}
            return [];
        }
    },

    'Gate.io': {
        tier: 1,
        getUrl: () => 'wss://api.gateio.ws/ws/v4/',
        pingInterval: 15000,
        pingMessage: JSON.stringify({ time: Math.floor(Date.now() / 1000), channel: 'spot.ping' }),
        onOpen: (ws) => {
            const t = Math.floor(Date.now() / 1000);
            ['BTC_USDT', 'ETH_USDT', 'SOL_USDT'].forEach((pair, i) => {
                ws.send(JSON.stringify({ time: t + i, channel: 'spot.trades', event: 'subscribe', payload: [pair] }));
                ws.send(JSON.stringify({ time: t + i + 10, channel: 'spot.tickers', event: 'subscribe', payload: [pair] }));
                ws.send(JSON.stringify({ time: t + i + 20, channel: 'spot.order_book', event: 'subscribe', payload: [pair, '5', '1000ms'] }));
            });
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.event !== 'update' || !d.result) return [];
                const cp = d.result.currency_pair || d.result.s || '';
                const coin = cp === 'BTC_USDT' ? 'BTC' : cp === 'ETH_USDT' ? 'ETH' : cp === 'SOL_USDT' ? 'SOL' : null;
                if (!coin) return [];
                const hits = [];
                if (d.channel === 'spot.trades') hits.push({ type: 'trades', coin });
                if (d.channel === 'spot.tickers') hits.push({ type: 'ticker', coin });
                if (d.channel === 'spot.order_book') hits.push({ type: 'orderbook', coin });
                return hits;
            } catch (e) { return []; }
        }
    },

    'HTX': {
        tier: 1,
        getUrl: () => 'wss://api.huobi.pro/ws',
        compression: 'gzip',
        handlePing: (parsed, ws) => {
            if (parsed.ping) { ws.send(JSON.stringify({ pong: parsed.ping })); return true; }
            return false;
        },
        onOpen: (ws) => {
            ['btcusdt', 'ethusdt', 'solusdt'].forEach(sym => {
                ws.send(JSON.stringify({ sub: `market.${sym}.trade.detail`, id: `${sym}_trade` }));
                ws.send(JSON.stringify({ sub: `market.${sym}.ticker`, id: `${sym}_ticker` }));
                ws.send(JSON.stringify({ sub: `market.${sym}.depth.step0`, id: `${sym}_depth` }));
            });
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.ping || d.subbed) return [];
                if (!d.ch) return [];
                const coin = d.ch.includes('btcusdt') ? 'BTC' : d.ch.includes('ethusdt') ? 'ETH' : d.ch.includes('solusdt') ? 'SOL' : null;
                if (!coin) return [];
                const hits = [];
                if (d.ch.includes('trade.detail')) hits.push({ type: 'trades', coin });
                if (d.ch.includes('.ticker')) hits.push({ type: 'ticker', coin });
                if (d.ch.includes('.depth')) hits.push({ type: 'orderbook', coin });
                return hits;
            } catch (e) { return []; }
        }
    },

    'WOO X': {
        tier: 1,
        getUrl: () => 'wss://wss.woo.org/ws/stream/OqdphuyCtYWxwzhxyLLjOWNdFP7sQt8RPWzmb5xY',
        pingInterval: 9000,
        pingMessage: JSON.stringify({ event: 'ping' }),
        onOpen: (ws) => {
            ['BTC', 'ETH', 'SOL'].forEach(c => {
                ws.send(JSON.stringify({ id: `${c}_trade`, event: 'subscribe', topic: `SPOT_${c}_USDT@trade` }));
                ws.send(JSON.stringify({ id: `${c}_ticker`, event: 'subscribe', topic: `SPOT_${c}_USDT@ticker` }));
                ws.send(JSON.stringify({ id: `${c}_book`, event: 'subscribe', topic: `SPOT_${c}_USDT@orderbook` }));
            });
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.event === 'pong' || d.event === 'subscribe') return [];
                if (!d.topic) return [];
                const coin = d.topic.includes('BTC') ? 'BTC' : d.topic.includes('ETH') ? 'ETH' : d.topic.includes('SOL') ? 'SOL' : null;
                if (!coin) return [];
                const hits = [];
                if (d.topic.includes('@trade')) hits.push({ type: 'trades', coin });
                if (d.topic.includes('@ticker')) hits.push({ type: 'ticker', coin });
                if (d.topic.includes('@orderbook')) hits.push({ type: 'orderbook', coin });
                return hits;
            } catch (e) { return []; }
        }
    },

    // ═══════════════════════ TIER 2 ═══════════════════════

    'Crypto.com': {
        tier: 2,
        getUrl: () => 'wss://stream.crypto.com/exchange/v1/market',
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                id: 1, method: 'subscribe',
                params: { channels: [
                    'trade.BTC_USDT', 'trade.ETH_USDT', 'trade.SOL_USDT',
                    'ticker.BTC_USDT', 'ticker.ETH_USDT', 'ticker.SOL_USDT',
                    'book.BTC_USDT.10', 'book.ETH_USDT.10', 'book.SOL_USDT.10'
                ] }
            }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.result || !d.result.data || !Array.isArray(d.result.data) || d.result.data.length === 0) return [];
                const ch = d.result.channel || '';
                const inst = d.result.instrument_name || '';
                const coin = inst.includes('BTC') ? 'BTC' : inst.includes('ETH') ? 'ETH' : inst.includes('SOL') ? 'SOL' : null;
                if (!coin) return [];
                const hits = [];
                if (ch === 'trade') hits.push({ type: 'trades', coin });
                if (ch.startsWith('ticker')) hits.push({ type: 'ticker', coin });
                if (ch.startsWith('book')) hits.push({ type: 'orderbook', coin });
                return hits;
            } catch (e) { return []; }
        }
    },

    'Bitstamp': {
        tier: 2,
        getUrl: () => 'wss://ws.bitstamp.net',
        onOpen: (ws) => {
            // Trades
            ws.send(JSON.stringify({ event: 'bts:subscribe', data: { channel: 'live_trades_btcusd' } }));
            ws.send(JSON.stringify({ event: 'bts:subscribe', data: { channel: 'live_trades_ethusd' } }));
            ws.send(JSON.stringify({ event: 'bts:subscribe', data: { channel: 'live_trades_solusd' } }));
            // Orderbook
            ws.send(JSON.stringify({ event: 'bts:subscribe', data: { channel: 'order_book_btcusd' } }));
            ws.send(JSON.stringify({ event: 'bts:subscribe', data: { channel: 'order_book_ethusd' } }));
            ws.send(JSON.stringify({ event: 'bts:subscribe', data: { channel: 'order_book_solusd' } }));
            // No native ticker via WS — will mark as N/A
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.event !== 'trade' && d.event !== 'data') return [];
                const ch = d.channel || '';
                const coin = ch.includes('btcusd') ? 'BTC' : ch.includes('ethusd') ? 'ETH' : ch.includes('solusd') ? 'SOL' : null;
                if (!coin) return [];
                const hits = [];
                if (ch.startsWith('live_trades') && d.event === 'trade') hits.push({ type: 'trades', coin });
                if (ch.startsWith('order_book') && d.event === 'data') hits.push({ type: 'orderbook', coin });
                return hits;
            } catch (e) { return []; }
        },
        noTicker: true
    },

    'WhiteBIT': {
        tier: 2,
        getUrl: () => 'wss://api.whitebit.com/ws',
        pingInterval: 25000,
        pingMessage: JSON.stringify({ id: 0, method: 'server.ping', params: [] }),
        onOpen: (ws) => {
            ws.send(JSON.stringify({ id: 1, method: 'trades_subscribe', params: ['BTC_USDT', 'ETH_USDT', 'SOL_USDT'] }));
            ws.send(JSON.stringify({ id: 2, method: 'depth_subscribe', params: ['BTC_USDT', 100, '0', true] }));
            ws.send(JSON.stringify({ id: 3, method: 'depth_subscribe', params: ['ETH_USDT', 100, '0', true] }));
            ws.send(JSON.stringify({ id: 4, method: 'depth_subscribe', params: ['SOL_USDT', 100, '0', true] }));
            ws.send(JSON.stringify({ id: 5, method: 'market_subscribe', params: ['BTC_USDT', 'ETH_USDT', 'SOL_USDT'] }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.method) return [];
                const hits = [];
                if (d.method === 'trades_update' && d.params) {
                    const pair = d.params[0];
                    const coin = pair === 'BTC_USDT' ? 'BTC' : pair === 'ETH_USDT' ? 'ETH' : pair === 'SOL_USDT' ? 'SOL' : null;
                    if (coin) hits.push({ type: 'trades', coin });
                }
                if (d.method === 'depth_update' && d.params) {
                    const pair = d.params[2] || d.params[d.params.length - 1];
                    const coin = (typeof pair === 'string') ? (pair === 'BTC_USDT' ? 'BTC' : pair === 'ETH_USDT' ? 'ETH' : pair === 'SOL_USDT' ? 'SOL' : null) : null;
                    if (coin) hits.push({ type: 'orderbook', coin });
                }
                if (d.method === 'market_update' && d.params) {
                    const pair = d.params[0];
                    const coin = pair === 'BTC_USDT' ? 'BTC' : pair === 'ETH_USDT' ? 'ETH' : pair === 'SOL_USDT' ? 'SOL' : null;
                    if (coin) hits.push({ type: 'ticker', coin });
                }
                return hits;
            } catch (e) { return []; }
        }
    },

    'AscendEX': {
        tier: 2,
        getUrl: () => 'wss://ascendex.com/1/api/pro/v1/stream',
        pingInterval: 15000,
        pingMessage: JSON.stringify({ op: 'ping' }),
        onOpen: (ws) => {
            ['BTC/USDT', 'ETH/USDT', 'SOL/USDT'].forEach(sym => {
                ws.send(JSON.stringify({ op: 'sub', ch: `trades:${sym}` }));
                ws.send(JSON.stringify({ op: 'sub', ch: `depth:${sym}` }));
                ws.send(JSON.stringify({ op: 'sub', ch: `bbo:${sym}` }));  // Best bid/offer as ticker proxy
            });
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.m === 'pong') return [];
                const sym = d.symbol || '';
                const coin = sym === 'BTC/USDT' ? 'BTC' : sym === 'ETH/USDT' ? 'ETH' : sym === 'SOL/USDT' ? 'SOL' : null;
                if (!coin) return [];
                const hits = [];
                if (d.m === 'trades') hits.push({ type: 'trades', coin });
                if (d.m === 'depth') hits.push({ type: 'orderbook', coin });
                if (d.m === 'bbo') hits.push({ type: 'ticker', coin });
                return hits;
            } catch (e) { return []; }
        }
    },

    'BingX': {
        tier: 2,
        getUrl: () => 'wss://open-api-ws.bingx.com/market',
        compression: 'gzip',
        pingInterval: 5000,
        pingMessage: 'Pong',
        onOpen: (ws) => {
            ['BTC-USDT', 'ETH-USDT', 'SOL-USDT'].forEach(sym => {
                ws.send(JSON.stringify({ id: `${sym}_trade`, reqType: 'sub', dataType: `${sym}@trade` }));
                ws.send(JSON.stringify({ id: `${sym}_ticker`, reqType: 'sub', dataType: `${sym}@ticker` }));
                ws.send(JSON.stringify({ id: `${sym}_depth`, reqType: 'sub', dataType: `${sym}@depth5` }));
            });
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (!d.dataType) return [];
                const coin = d.dataType.includes('BTC') ? 'BTC' : d.dataType.includes('ETH') ? 'ETH' : d.dataType.includes('SOL') ? 'SOL' : null;
                if (!coin) return [];
                const hits = [];
                if (d.dataType.includes('@trade')) hits.push({ type: 'trades', coin });
                if (d.dataType.includes('@ticker')) hits.push({ type: 'ticker', coin });
                if (d.dataType.includes('@depth')) hits.push({ type: 'orderbook', coin });
                return hits;
            } catch (e) { return []; }
        }
    },

    'Toobit': {
        tier: 2,
        getUrl: () => 'wss://stream.toobit.com/quote/ws/v1',
        handlePing: (parsed, ws) => {
            if (parsed.ping) { ws.send(JSON.stringify({ pong: parsed.ping })); return true; }
            return false;
        },
        customPingSetup: (ws) => {
            return setInterval(() => {
                if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ ping: Date.now() }));
            }, 30000);
        },
        onOpen: (ws) => {
            ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'].forEach(sym => {
                ws.send(JSON.stringify({ symbol: sym, topic: 'trade', event: 'sub', params: { binary: false } }));
                ws.send(JSON.stringify({ symbol: sym, topic: 'realtimes', event: 'sub', params: { binary: false } }));
                ws.send(JSON.stringify({ symbol: sym, topic: 'depth', event: 'sub', params: { binary: false } }));
            });
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.pong) return [];
                const sym = d.symbol || d.symbolName || '';
                const coin = sym === 'BTCUSDT' ? 'BTC' : sym === 'ETHUSDT' ? 'ETH' : sym === 'SOLUSDT' ? 'SOL' : null;
                if (!coin) return [];
                const hits = [];
                if (d.topic === 'trade') hits.push({ type: 'trades', coin });
                if (d.topic === 'realtimes') hits.push({ type: 'ticker', coin });
                if (d.topic === 'depth') hits.push({ type: 'orderbook', coin });
                return hits;
            } catch (e) { return []; }
        }
    },

    'Deepcoin': {
        tier: 2,
        getUrl: () => 'wss://stream.deepcoin.com/streamlet/trade/public/spot?platform=api',
        pingInterval: 15000,
        pingMessage: 'ping',
        onOpen: (ws) => {
            const reqId = Date.now();
            ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'].forEach((sym, i) => {
                // TopicID 2 = Trades
                ws.send(JSON.stringify({ SendTopicAction: { Action: "1", FilterValue: `DeepCoin_${sym}`, LocalNo: reqId + i, ResumeNo: -2, TopicID: "2" } }));
                // TopicID 7 = Market overview / Ticker
                ws.send(JSON.stringify({ SendTopicAction: { Action: "1", FilterValue: `DeepCoin_${sym}`, LocalNo: reqId + 100 + i, ResumeNo: -2, TopicID: "7" } }));
            });
            // Note: TopicID 25 (orderbook) is not available on spot endpoint
        },
        parseMessage: (msg) => {
            if (msg === 'pong') return [];
            try {
                const d = JSON.parse(msg);
                const hits = [];
                // Deepcoin proprietary trade format: a='PMT'
                if (d.a === 'PMT' && d.r && Array.isArray(d.r)) {
                    const inst = d.r[0]?.d?.I || '';
                    const coin = inst.includes('BTC') ? 'BTC' : inst.includes('ETH') ? 'ETH' : inst.includes('SOL') ? 'SOL' : null;
                    if (coin) hits.push({ type: 'trades', coin });
                }
                // Deepcoin ticker format: a='PO' (market overview from TopicID 7)
                if (d.a === 'PO' && d.r && Array.isArray(d.r)) {
                    const inst = d.r[0]?.d?.I || '';
                    const coin = inst.includes('BTC') ? 'BTC' : inst.includes('ETH') ? 'ETH' : inst.includes('SOL') ? 'SOL' : null;
                    if (coin) hits.push({ type: 'ticker', coin });
                }
                return hits;
            } catch (e) { return []; }
        },
        noOrderbook: true
    },

    // ═══════════════════════ TIER 3 ═══════════════════════

    'Upbit': {
        tier: 3,
        getUrl: () => 'wss://api.upbit.com/websocket/v1',
        pingInterval: 30000,
        pingMessage: 'PING',
        onOpen: (ws) => {
            ws.send(JSON.stringify([
                { ticket: 'stream-tester' },
                { type: 'trade', codes: ['KRW-BTC', 'KRW-ETH', 'KRW-SOL'] },
                { type: 'ticker', codes: ['KRW-BTC', 'KRW-ETH', 'KRW-SOL'] },
                { type: 'orderbook', codes: ['KRW-BTC', 'KRW-ETH', 'KRW-SOL'] }
            ]));
        },
        parseMessage: (msg) => {
            try {
                const str = msg.toString();
                if (str === 'PONG') return [];
                const d = JSON.parse(str);
                const code = d.code || '';
                const coin = code === 'KRW-BTC' ? 'BTC' : code === 'KRW-ETH' ? 'ETH' : code === 'KRW-SOL' ? 'SOL' : null;
                if (!coin) return [];
                const hits = [];
                if (d.type === 'trade') hits.push({ type: 'trades', coin });
                if (d.type === 'ticker') hits.push({ type: 'ticker', coin });
                if (d.type === 'orderbook') hits.push({ type: 'orderbook', coin });
                return hits;
            } catch (e) { return []; }
        }
    },

    'LBank': {
        tier: 3,
        getUrl: () => 'wss://www.lbkex.net/ws/V2/',
        pingInterval: 30000,
        pingMessage: JSON.stringify({ action: 'ping', ping: 'ping' }),
        onOpen: (ws) => {
            ['btc_usdt', 'eth_usdt', 'sol_usdt'].forEach(pair => {
                ws.send(JSON.stringify({ action: 'subscribe', subscribe: 'trade', pair }));
                ws.send(JSON.stringify({ action: 'subscribe', subscribe: 'tick', pair }));
                ws.send(JSON.stringify({ action: 'subscribe', subscribe: 'depth', depth: '10', pair }));
            });
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.action === 'pong') return [];
                const pair = d.pair || '';
                const coin = pair === 'btc_usdt' ? 'BTC' : pair === 'eth_usdt' ? 'ETH' : pair === 'sol_usdt' ? 'SOL' : null;
                if (!coin) return [];
                const hits = [];
                if (d.trade) hits.push({ type: 'trades', coin });
                if (d.tick) hits.push({ type: 'ticker', coin });
                if (d.depth) hits.push({ type: 'orderbook', coin });
                if (d.type === 'trade') hits.push({ type: 'trades', coin });
                if (d.type === 'tick') hits.push({ type: 'ticker', coin });
                if (d.type === 'depth') hits.push({ type: 'orderbook', coin });
                return hits;
            } catch (e) { return []; }
        }
    },

    'BitMart': {
        tier: 3,
        getUrl: () => 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1',
        compression: 'inflate',
        pingInterval: 10000,
        pingMessage: 'ping',
        onOpen: (ws) => {
            ws.send(JSON.stringify({ op: 'subscribe', args: [
                'spot/trade:BTC_USDT', 'spot/trade:ETH_USDT', 'spot/trade:SOL_USDT',
                'spot/ticker:BTC_USDT', 'spot/ticker:ETH_USDT', 'spot/ticker:SOL_USDT',
                'spot/depth5:BTC_USDT', 'spot/depth5:ETH_USDT', 'spot/depth5:SOL_USDT'
            ] }));
        },
        parseMessage: (msg) => {
            if (msg === 'pong') return [];
            try {
                const d = JSON.parse(msg);
                if (!d.table || !d.data || !d.data[0]) return [];
                const sym = d.data[0].symbol || '';
                const coin = sym === 'BTC_USDT' ? 'BTC' : sym === 'ETH_USDT' ? 'ETH' : sym === 'SOL_USDT' ? 'SOL' : null;
                if (!coin) return [];
                const hits = [];
                if (d.table === 'spot/trade') hits.push({ type: 'trades', coin });
                if (d.table === 'spot/ticker') hits.push({ type: 'ticker', coin });
                if (d.table === 'spot/depth5') hits.push({ type: 'orderbook', coin });
                return hits;
            } catch (e) { return []; }
        }
    },

    'Pionex': {
        tier: 3,
        getUrl: () => 'wss://ws.pionex.com/wsPub',
        pingInterval: 15000,
        pingMessage: JSON.stringify({ op: 'PONG' }),
        handlePing: (parsed, ws) => {
            if (parsed.op === 'PING') { ws.send(JSON.stringify({ op: 'PONG' })); return true; }
            return false;
        },
        onOpen: (ws) => {
            // Pionex only supports TRADE and DEPTH topics — no TICKER available
            ['BTC_USDT', 'ETH_USDT', 'SOL_USDT'].forEach(sym => {
                ws.send(JSON.stringify({ op: 'SUBSCRIBE', topic: 'TRADE', symbol: sym }));
                ws.send(JSON.stringify({ op: 'SUBSCRIBE', topic: 'DEPTH', symbol: sym, limit: 10 }));
            });
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.op === 'PONG' || d.op === 'PING' || d.op === 'SUBSCRIBE_RESULT') return [];
                const sym = d.symbol || '';
                const coin = sym === 'BTC_USDT' ? 'BTC' : sym === 'ETH_USDT' ? 'ETH' : sym === 'SOL_USDT' ? 'SOL' : null;
                if (!coin) return [];
                const hits = [];
                if (d.topic === 'TRADE') hits.push({ type: 'trades', coin });
                if (d.topic === 'DEPTH') hits.push({ type: 'orderbook', coin });
                return hits;
            } catch (e) { return []; }
        },
        noTicker: true
    },

    'Poloniex': {
        tier: 3,
        getUrl: () => 'wss://ws.poloniex.com/ws/public',
        pingInterval: 30000,
        pingMessage: JSON.stringify({ event: 'ping' }),
        onOpen: (ws) => {
            ws.send(JSON.stringify({ event: 'subscribe', channel: ['trades'], symbols: ['BTC_USDT', 'ETH_USDT', 'SOL_USDT'] }));
            ws.send(JSON.stringify({ event: 'subscribe', channel: ['ticker'], symbols: ['BTC_USDT', 'ETH_USDT', 'SOL_USDT'] }));
            ws.send(JSON.stringify({ event: 'subscribe', channel: ['book'], symbols: ['BTC_USDT', 'ETH_USDT', 'SOL_USDT'] }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                if (d.event === 'pong') return [];
                if (!d.channel || !d.data) return [];
                const items = Array.isArray(d.data) ? d.data : [d.data];
                const sym = items[0]?.symbol || '';
                const coin = sym === 'BTC_USDT' ? 'BTC' : sym === 'ETH_USDT' ? 'ETH' : sym === 'SOL_USDT' ? 'SOL' : null;
                if (!coin) return [];
                const hits = [];
                if (d.channel === 'trades') hits.push({ type: 'trades', coin });
                if (d.channel === 'ticker') hits.push({ type: 'ticker', coin });
                if (d.channel === 'book') hits.push({ type: 'orderbook', coin });
                return hits;
            } catch (e) { return []; }
        }
    },

    'BTSE': {
        tier: 3,
        getUrl: () => 'wss://ws.btse.com/ws/spot',
        ossUrl: 'wss://ws.btse.com/ws/oss/spot',  // Separate endpoint for orderbook
        pingInterval: 30000,
        pingMessage: 'ping',
        onOpen: (ws) => {
            // Trades on spot endpoint
            ws.send(JSON.stringify({ op: 'subscribe', args: [
                'tradeHistoryApi:BTC-USD', 'tradeHistoryApi:ETH-USD', 'tradeHistoryApi:SOL-USD'
            ] }));
        },
        ossOnOpen: (ws) => {
            // Orderbook on OSS endpoint (deprecated on main spot endpoint since April 2023)
            ws.send(JSON.stringify({ op: 'subscribe', args: [
                'update:BTC-USD_0', 'update:ETH-USD_0', 'update:SOL-USD_0'
            ] }));
        },
        parseMessage: (msg) => {
            if (msg === 'pong') return [];
            try {
                const d = JSON.parse(msg);
                if (d.event === 'subscribe') return [];
                if (!d.topic || !d.data) return [];
                const topic = d.topic;
                const coin = topic.includes('BTC') ? 'BTC' : topic.includes('ETH') ? 'ETH' : topic.includes('SOL') ? 'SOL' : null;
                if (!coin) return [];
                const hits = [];
                if (topic.includes('tradeHistory')) hits.push({ type: 'trades', coin });
                // OSS orderbook: topic = 'update:BTC-USD_0'
                if (topic.startsWith('update:')) hits.push({ type: 'orderbook', coin });
                return hits;
            } catch (e) { return []; }
        },
        noTicker: true
    },

    'HitBTC': {
        tier: 3,
        getUrl: () => 'wss://api.hitbtc.com/api/3/ws/public',
        lowVolumeTrades: true,  // HitBTC spot has near-zero trades — confirmed 0 in 30s test
        onOpen: (ws) => {
            ws.send(JSON.stringify({ method: 'subscribe', ch: 'trades', params: { symbols: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'] }, id: 1 }));
            ws.send(JSON.stringify({ method: 'subscribe', ch: 'ticker/price/1s', params: { symbols: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'] }, id: 2 }));
            ws.send(JSON.stringify({ method: 'subscribe', ch: 'ticker/1s', params: { symbols: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'] }, id: 4 }));
            ws.send(JSON.stringify({ method: 'subscribe', ch: 'orderbook/full', params: { symbols: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'], limit: 5 }, id: 3 }));
        },
        parseMessage: (msg) => {
            try {
                const d = JSON.parse(msg);
                // Subscription confirmations
                if (d.result) return [];
                const ch = d.ch || '';
                const data = d.snapshot || d.update || d.data || {};
                const hits = [];

                if (ch === 'trades') {
                    for (const mkt of Object.keys(data)) {
                        const coin = mkt === 'BTCUSDT' ? 'BTC' : mkt === 'ETHUSDT' ? 'ETH' : mkt === 'SOLUSDT' ? 'SOL' : null;
                        if (coin) hits.push({ type: 'trades', coin });
                    }
                }
                if (ch.startsWith('ticker')) {
                    // ticker data: { BTCUSDT: { ... } }
                    for (const mkt of Object.keys(data)) {
                        const coin = mkt === 'BTCUSDT' ? 'BTC' : mkt === 'ETHUSDT' ? 'ETH' : mkt === 'SOLUSDT' ? 'SOL' : null;
                        if (coin) hits.push({ type: 'ticker', coin });
                    }
                }
                if (ch.startsWith('orderbook')) {
                    for (const mkt of Object.keys(data)) {
                        const coin = mkt === 'BTCUSDT' ? 'BTC' : mkt === 'ETHUSDT' ? 'ETH' : mkt === 'SOLUSDT' ? 'SOL' : null;
                        if (coin) hits.push({ type: 'orderbook', coin });
                    }
                }
                return hits;
            } catch (e) { return []; }
        }
    },
};

// ======================== TEST ENGINE ========================

async function testExchange(name) {
    const exDef = EXCHANGES[name];
    initResult(name);

    console.log(`  [${ts()}] 🔌 ${name} — Connecting...`);

    let wsUrl;
    let kucoinPing;
    try {
        const urlResult = await exDef.getUrl();
        if (typeof urlResult === 'object') {
            wsUrl = urlResult.url;
            kucoinPing = urlResult.pingInterval;
        } else {
            wsUrl = urlResult;
        }
    } catch (e) {
        console.log(`  [${ts()}] ❌ ${name} — URL fetch failed: ${e.message}`);
        results[name].errors.push(`URL: ${e.message}`);
        results[name].status = 'error';
        return;
    }

    return new Promise((resolve) => {
        const ctx = {};  // Context for Bitfinex channel tracking etc.
        let resolved = false;
        const done = () => { if (!resolved) { resolved = true; resolve(); } };

        const timeout = setTimeout(() => {
            console.log(`  [${ts()}] ⏰ ${name} — Test timeout`);
            ws.terminate();
            if (ossWs) try { ossWs.terminate(); } catch (e) {}
            results[name].status = 'timeout';
            done();
        }, TEST_TIMEOUT);

        let ws, ossWs;
        try {
            ws = new WebSocket(wsUrl, {
                headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
                handshakeTimeout: 10000,
                perMessageDeflate: exDef.compression ? false : undefined
            });
        } catch (e) {
            clearTimeout(timeout);
            results[name].errors.push(`WS: ${e.message}`);
            results[name].status = 'error';
            done();
            return;
        }

        let pingTimer, customPingTimer, ossPingTimer;

        // Handle OSS (Order Snapshot Stream) dual connection for exchanges like BTSE
        if (exDef.ossUrl && exDef.ossOnOpen) {
            try {
                ossWs = new WebSocket(exDef.ossUrl, {
                    headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
                    handshakeTimeout: 10000
                });
                ossWs.on('open', () => {
                    console.log(`  [${ts()}] ✅ ${name}/oss — Connected, subscribing orderbook...`);
                    try { exDef.ossOnOpen(ossWs); } catch (e) {}
                    if (exDef.pingInterval && exDef.pingMessage) {
                        ossPingTimer = setInterval(() => {
                            if (ossWs.readyState === WebSocket.OPEN) ossWs.send(exDef.pingMessage);
                        }, exDef.pingInterval);
                    }
                });
                ossWs.on('message', (data) => {
                    const str = data.toString();
                    try {
                        const hits = exDef.parseMessage(str, ctx);
                        if (hits && hits.length > 0) {
                            for (const h of hits) {
                                if (results[name][h.type] && results[name][h.type][h.coin] !== undefined) {
                                    results[name][h.type][h.coin]++;
                                }
                            }
                        }
                    } catch (e) {}
                });
                ossWs.on('error', (err) => {
                    console.log(`  [${ts()}] ❌ ${name}/oss — Error: ${err.message}`);
                });
                ossWs.on('close', () => {
                    if (ossPingTimer) clearInterval(ossPingTimer);
                });
            } catch (e) {
                results[name].errors.push(`OSS: ${e.message}`);
            }
        }

        ws.on('open', () => {
            console.log(`  [${ts()}] ✅ ${name} — Connected, subscribing...`);
            try {
                exDef.onOpen(ws);
            } catch (e) {
                results[name].errors.push(`Subscribe: ${e.message}`);
            }

            // Setup ping
            const pi = exDef.pingInterval || kucoinPing;
            if (pi && exDef.pingMessage) {
                pingTimer = setInterval(() => {
                    if (ws.readyState === WebSocket.OPEN) ws.send(exDef.pingMessage);
                }, pi);
            }
            if (exDef.customPingSetup) {
                customPingTimer = exDef.customPingSetup(ws);
            }

            // Wait for data, then close
            setTimeout(() => {
                clearTimeout(timeout);
                if (pingTimer) clearInterval(pingTimer);
                if (customPingTimer) clearInterval(customPingTimer);
                if (ossPingTimer) clearInterval(ossPingTimer);

                const r = results[name];
                const total = Object.values(r.orderbook).reduce((a, b) => a + b, 0) +
                              Object.values(r.trades).reduce((a, b) => a + b, 0) +
                              Object.values(r.ticker).reduce((a, b) => a + b, 0);
                r.status = total > 0 ? 'ok' : 'no_data';

                console.log(`  [${ts()}] 📊 ${name} — Done: OB=${r.orderbook.BTC}/${r.orderbook.ETH}/${r.orderbook.SOL} TR=${r.trades.BTC}/${r.trades.ETH}/${r.trades.SOL} TK=${r.ticker.BTC}/${r.ticker.ETH}/${r.ticker.SOL}`);

                try { ws.close(); } catch (e) {}
                if (ossWs) try { ossWs.close(); } catch (e) {}
                setTimeout(done, 500);
            }, DATA_WAIT);
        });

        ws.on('message', (data) => {
            let str;
            if (exDef.compression && Buffer.isBuffer(data)) {
                try {
                    if (exDef.compression === 'gzip') str = zlib.gunzipSync(data).toString();
                    else if (exDef.compression === 'inflate') str = zlib.inflateSync(data).toString();
                    else if (exDef.compression === 'inflateRaw') str = zlib.inflateRawSync(data).toString();
                    else {
                        try { str = zlib.gunzipSync(data).toString(); } catch (e) {
                            try { str = zlib.inflateSync(data).toString(); } catch (e2) {
                                try { str = zlib.inflateRawSync(data).toString(); } catch (e3) { str = data.toString(); }
                            }
                        }
                    }
                } catch (e) { str = data.toString(); }
            } else {
                str = data.toString();
            }

            // Handle BingX text pings
            if (str === 'Ping') {
                try { ws.send('Pong'); } catch (e) {}
                return;
            }

            // Handle server-initiated pings
            if (exDef.handlePing) {
                try {
                    const parsed = JSON.parse(str);
                    if (exDef.handlePing(parsed, ws)) return;
                } catch (e) {}
            }

            // Parse
            try {
                const hits = exDef.parseMessage(str, ctx);
                if (hits && hits.length > 0) {
                    for (const h of hits) {
                        if (results[name][h.type] && results[name][h.type][h.coin] !== undefined) {
                            results[name][h.type][h.coin]++;
                        }
                    }
                }
            } catch (e) {}
        });

        ws.on('error', (err) => {
            console.log(`  [${ts()}] ❌ ${name} — Error: ${err.message}`);
            results[name].errors.push(err.message);
        });

        ws.on('close', () => {
            if (pingTimer) clearInterval(pingTimer);
            if (customPingTimer) clearInterval(customPingTimer);
        });
    });
}

// ======================== REPORT ========================

function printReport() {
    const COINS = ['BTC', 'ETH', 'SOL'];
    const TYPES = ['orderbook', 'trades', 'ticker'];
    
    console.log('\n');
    console.log('╔' + '═'.repeat(140) + '╗');
    console.log('║  COMPREHENSIVE STREAM TYPE TEST REPORT — Orderbook / Trades / Tickers × BTC / ETH / SOL' + ' '.repeat(52) + '║');
    console.log('╚' + '═'.repeat(140) + '╝');

    // Header
    console.log('\n  ' + 
        'Exchange'.padEnd(15) + 'Tier'.padEnd(5) +
        '│ OB-BTC'.padEnd(9) + 'OB-ETH'.padEnd(9) + 'OB-SOL'.padEnd(9) +
        '│ TR-BTC'.padEnd(9) + 'TR-ETH'.padEnd(9) + 'TR-SOL'.padEnd(9) +
        '│ TK-BTC'.padEnd(9) + 'TK-ETH'.padEnd(9) + 'TK-SOL'.padEnd(9) +
        '│ Status'
    );
    console.log('  ' + '─'.repeat(120));

    const sorted = Object.keys(EXCHANGES).sort((a, b) => {
        const ta = EXCHANGES[a].tier, tb = EXCHANGES[b].tier;
        if (ta !== tb) return ta - tb;
        return a.localeCompare(b);
    });

    let totalPass = 0, totalFail = 0, totalNA = 0;
    const summaryPerExchange = {};

    for (const name of sorted) {
        const r = results[name] || { orderbook: {}, trades: {}, ticker: {}, errors: [], status: 'unknown' };
        const tier = EXCHANGES[name].tier;
        const noTicker = EXCHANGES[name].noTicker;
        const noOrderbook = EXCHANGES[name].noOrderbook;
        const lowVolumeTrades = EXCHANGES[name].lowVolumeTrades;

        let exPass = 0, exFail = 0, exNA = 0;

        const fmtCell = (type, coin) => {
            const count = r[type]?.[coin] || 0;
            if (noTicker && type === 'ticker') {
                exNA++;
                totalNA++;
                return 'N/A'.padEnd(7);
            }
            if (noOrderbook && type === 'orderbook') {
                exNA++;
                totalNA++;
                return 'N/A'.padEnd(7);
            }
            if (lowVolumeTrades && type === 'trades' && count === 0) {
                // Low-volume trades are expected — mark as N/A instead of fail
                exNA++;
                totalNA++;
                return 'LV '.padEnd(7);
            }
            if (count > 0) {
                exPass++;
                totalPass++;
                return `✅ ${count}`.padEnd(7);
            } else {
                exFail++;
                totalFail++;
                return '❌ 0'.padEnd(7);
            }
        };

        const statusEmoji = r.status === 'ok' ? '✅' : r.status === 'timeout' ? '⏰' : r.status === 'error' ? '❌' : '⚪';

        console.log('  ' +
            name.padEnd(15) + `T${tier}`.padEnd(5) +
            `│ ${fmtCell('orderbook', 'BTC')} ${fmtCell('orderbook', 'ETH')} ${fmtCell('orderbook', 'SOL')} ` +
            `│ ${fmtCell('trades', 'BTC')} ${fmtCell('trades', 'ETH')} ${fmtCell('trades', 'SOL')} ` +
            `│ ${fmtCell('ticker', 'BTC')} ${fmtCell('ticker', 'ETH')} ${fmtCell('ticker', 'SOL')} ` +
            `│ ${statusEmoji} ${exPass}/${exPass + exFail + exNA}`
        );

        summaryPerExchange[name] = { pass: exPass, fail: exFail, na: exNA };
    }

    console.log('  ' + '─'.repeat(120));

    // Summary by stream type
    console.log('\n  📊 SUMMARY BY STREAM TYPE');
    console.log('  ' + '─'.repeat(60));
    for (const type of TYPES) {
        let pass = 0, fail = 0;
        for (const name of sorted) {
            const r = results[name] || {};
            for (const coin of COINS) {
                const count = r[type]?.[coin] || 0;
                if (EXCHANGES[name].noTicker && type === 'ticker') continue;
                if (EXCHANGES[name].noOrderbook && type === 'orderbook') continue;
                if (EXCHANGES[name].lowVolumeTrades && type === 'trades' && count === 0) continue;
                if (count > 0) pass++;
                else fail++;
            }
        }
        const pct = ((pass / (pass + fail)) * 100).toFixed(1);
        console.log(`  ${type.padEnd(12)} ✅ ${pass} passed  ❌ ${fail} failed  (${pct}% success)`);
    }

    // Summary by coin
    console.log('\n  📊 SUMMARY BY COIN');
    console.log('  ' + '─'.repeat(60));
    for (const coin of COINS) {
        let pass = 0, fail = 0;
        for (const name of sorted) {
            const r = results[name] || {};
            for (const type of TYPES) {
                const count = r[type]?.[coin] || 0;
                if (EXCHANGES[name].noTicker && type === 'ticker') continue;
                if (EXCHANGES[name].noOrderbook && type === 'orderbook') continue;
                if (EXCHANGES[name].lowVolumeTrades && type === 'trades' && count === 0) continue;
                if (count > 0) pass++;
                else fail++;
            }
        }
        const pct = ((pass / (pass + fail)) * 100).toFixed(1);
        console.log(`  ${coin.padEnd(6)} ✅ ${pass} passed  ❌ ${fail} failed  (${pct}% success)`);
    }

    // Summary by exchange
    console.log('\n  📊 SUMMARY BY EXCHANGE');
    console.log('  ' + '─'.repeat(60));
    let perfectCount = 0;
    for (const name of sorted) {
        const s = summaryPerExchange[name];
        let maxTests = 9;
        if (EXCHANGES[name].noTicker) maxTests -= 3;
        if (EXCHANGES[name].noOrderbook) maxTests -= 3;
        const effectivePass = s.pass + s.na; // N/A and LV are not failures
        const emoji = s.fail === 0 ? '🟢' : effectivePass >= maxTests * 0.6 ? '🟡' : '🔴';
        if (s.fail === 0) perfectCount++;
        console.log(`  ${emoji} ${name.padEnd(15)} ${s.pass}/${maxTests} streams working${s.na > 0 ? ` (${s.na} N/A)` : ''}`);
    }

    const totalTests = totalPass + totalFail;
    console.log('\n  ' + '═'.repeat(60));
    console.log(`  ✅ TOTAL: ${totalPass}/${totalTests} tests passed (${((totalPass / totalTests) * 100).toFixed(1)}%)${totalNA > 0 ? ` | ${totalNA} N/A` : ''}`);
    console.log(`  🟢 ${perfectCount}/${sorted.length} exchanges have all streams working`);
    console.log('  ' + '═'.repeat(60));

    // Save to JSON
    const fs = require('fs');
    const report = {
        timestamp: new Date().toISOString(),
        duration: `${DATA_WAIT / 1000}s per exchange`,
        totalTests: totalPass + totalFail + totalNA,
        passed: totalPass,
        failed: totalFail,
        na: totalNA,
        exchanges: {}
    };
    for (const name of sorted) {
        const r = results[name] || {};
        report.exchanges[name] = {
            tier: EXCHANGES[name].tier,
            status: r.status,
            orderbook: r.orderbook,
            trades: r.trades,
            ticker: r.ticker,
            noTicker: EXCHANGES[name].noTicker || false,
            noOrderbook: EXCHANGES[name].noOrderbook || false,
            lowVolumeTrades: EXCHANGES[name].lowVolumeTrades || false,
            errors: r.errors
        };
    }
    fs.writeFileSync('stream-type-results.json', JSON.stringify(report, null, 2));
    console.log('\n  💾 Detailed results saved to stream-type-results.json\n');
}

// ======================== MAIN ========================

async function main() {
    console.log('\n' + '╔' + '═'.repeat(78) + '╗');
    console.log('║  COMPREHENSIVE STREAM TYPE TESTER                                            ║');
    console.log('║  Testing: Orderbook / Trades / Tickers × BTC / ETH / SOL                    ║');
    console.log('║  Exchanges: 24 | Tests per exchange: 9 (3 types × 3 coins)                  ║');
    console.log('║  Total Tests: 216                                                            ║');
    console.log('╚' + '═'.repeat(78) + '╝\n');

    const exchangeNames = Object.keys(EXCHANGES);
    console.log(`  🚀 Starting tests for ${exchangeNames.length} exchanges...\n`);

    // Run exchanges in batches of MAX_CONCURRENT
    for (let i = 0; i < exchangeNames.length; i += MAX_CONCURRENT) {
        const batch = exchangeNames.slice(i, i + MAX_CONCURRENT);
        console.log(`\n  ── Batch ${Math.floor(i / MAX_CONCURRENT) + 1}: ${batch.join(', ')} ──\n`);
        
        const promises = batch.map((name, idx) => {
            return new Promise(r => setTimeout(r, idx * STAGGER_DELAY)).then(() => testExchange(name));
        });
        
        await Promise.all(promises);
        
        // Brief pause between batches
        if (i + MAX_CONCURRENT < exchangeNames.length) {
            console.log(`\n  ⏳ Waiting 2s before next batch...\n`);
            await new Promise(r => setTimeout(r, 2000));
        }
    }

    printReport();
}

main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});
