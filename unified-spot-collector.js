/**
 * ╔══════════════════════════════════════════════════════════════════╗
 * ║         UNIFIED SPOT COLLECTOR - BTC / ETH / SOL               ║
 * ║     22 Exchanges | Real-Time WebSocket | Per-Second Analytics   ║
 * ╚══════════════════════════════════════════════════════════════════╝
 * 
 * Streams spot trade data from 22 exchanges simultaneously for BTC, ETH, SOL.
 * Displays per-second collection rates per coin per exchange for throughput analysis.
 * 
 * Features:
 *  - Auto-reconnection with exponential backoff
 *  - Heartbeat/ping management per exchange
 *  - Connection health monitoring
 *  - Per-second throughput analytics dashboard
 *  - Graceful shutdown (Ctrl+C)
 */

const WebSocket = require('ws');
const zlib = require('zlib');
const https = require('https');

// ======================== CONFIGURATION ========================

const COINS = ['BTC', 'ETH', 'SOL'];
const DASHBOARD_INTERVAL = 5000;    // Print dashboard every 5 seconds
const RECONNECT_BASE_DELAY = 3000;  // Base reconnect delay (ms)
const RECONNECT_MAX_DELAY = 60000;  // Max reconnect delay (ms)
const CONNECTION_TIMEOUT = 15000;   // Connection timeout (ms)
const TEST_DURATION = 60000;        // Run for 60 seconds (set 0 for indefinite)

// ======================== STATE TRACKING ========================

const stats = {};       // { exchangeName: { BTC: { total: 0, perSec: [] }, ETH: {...}, SOL: {...} } }
const connections = {};  // { exchangeName: { ws, status, reconnects, lastMsg, pingInterval } }
let startTime = null;
let isShuttingDown = false;
let secondCounter = 0;

// Per-second accumulator: reset every second
const currentSecond = {}; // { exchangeName: { BTC: 0, ETH: 0, SOL: 0 } }

// ======================== UTILITY FUNCTIONS ========================

function httpsRequest(url, options = {}) {
    return new Promise((resolve, reject) => {
        const opts = {
            ...require('url').parse(url),
            method: options.method || 'GET',
            headers: {
                'User-Agent': 'Mozilla/5.0',
                'Content-Type': 'application/json',
                ...(options.headers || {})
            }
        };
        const req = https.request(opts, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try { resolve(JSON.parse(data)); } catch (e) { resolve(data); }
            });
        });
        req.on('error', reject);
        if (options.body) req.write(JSON.stringify(options.body));
        req.end();
    });
}

function initStats(exchange) {
    stats[exchange] = {};
    currentSecond[exchange] = {};
    COINS.forEach(coin => {
        stats[exchange][coin] = { total: 0, history: [] };
        currentSecond[exchange][coin] = 0;
    });
}

function recordMessage(exchange, coin) {
    if (!stats[exchange] || !stats[exchange][coin]) return;
    stats[exchange][coin].total++;
    currentSecond[exchange][coin]++;
}

function log(exchange, msg) {
    const ts = new Date().toISOString().substring(11, 19);
    console.log(`  [${ts}] [${exchange}] ${msg}`);
}

// ======================== EXCHANGE DEFINITIONS ========================
// Each exchange returns an object with:
//   { url, subscriptions: [{coin, message}], compression, pingInterval, pingMessage, 
//     handlePing(data,ws), parseMessage(data) => {coin}|null, onOpen(ws)? }

const EXCHANGES = {

    // ═══════════════════════ TIER 1 ═══════════════════════

    'Binance': {
        tier: 1,
        getConfig: () => ({
            // Use combined stream endpoint for all pairs at once
            url: 'wss://stream.binance.com:9443/stream',
            onOpen: (ws) => {
                ws.send(JSON.stringify({
                    method: 'SUBSCRIBE',
                    params: ['btcusdt@trade', 'ethusdt@trade', 'solusdt@trade'],
                    id: 1
                }));
            },
            parseMessage: (msg) => {
                try {
                    const d = JSON.parse(msg);
                    if (d.data && d.data.e === 'trade') {
                        const s = d.data.s;
                        if (s === 'BTCUSDT') return 'BTC';
                        if (s === 'ETHUSDT') return 'ETH';
                        if (s === 'SOLUSDT') return 'SOL';
                    }
                } catch (e) {}
                return null;
            }
        })
    },

    'Coinbase': {
        tier: 1,
        getConfig: () => ({
            url: 'wss://ws-feed.exchange.coinbase.com',
            onOpen: (ws) => {
                ws.send(JSON.stringify({
                    type: 'subscribe',
                    product_ids: ['BTC-USD', 'ETH-USD', 'SOL-USD'],
                    channels: ['matches', 'heartbeat']
                }));
            },
            parseMessage: (msg) => {
                try {
                    const d = JSON.parse(msg);
                    if (d.type === 'match' || d.type === 'last_match') {
                        const p = d.product_id;
                        if (p === 'BTC-USD') return 'BTC';
                        if (p === 'ETH-USD') return 'ETH';
                        if (p === 'SOL-USD') return 'SOL';
                    }
                } catch (e) {}
                return null;
            }
        })
    },

    'Kraken': {
        tier: 1,
        getConfig: () => ({
            url: 'wss://ws.kraken.com',
            onOpen: (ws) => {
                ws.send(JSON.stringify({
                    event: 'subscribe',
                    pair: ['XBT/USDT', 'ETH/USDT', 'SOL/USDT'],
                    subscription: { name: 'trade' }
                }));
            },
            parseMessage: (msg) => {
                try {
                    const d = JSON.parse(msg);
                    // Kraken sends arrays: [channelID, [[price,volume,time,side,type,misc]], channelName, pair]
                    if (Array.isArray(d) && d.length >= 4) {
                        const pair = d[d.length - 1];
                        if (pair === 'XBT/USDT') return 'BTC';
                        if (pair === 'ETH/USDT') return 'ETH';
                        if (pair === 'SOL/USDT') return 'SOL';
                    }
                } catch (e) {}
                return null;
            }
        })
    },

    'KuCoin': {
        tier: 1,
        getConfig: async () => {
            // Fetch token via REST
            let wsUrl = 'wss://ws-api-spot.kucoin.com';
            let pingMs = 18000;
            try {
                const resp = await httpsRequest('https://api.kucoin.com/api/v1/bullet-public', { method: 'POST' });
                if (resp.data && resp.data.token) {
                    const ep = resp.data.instanceServers?.[0]?.endpoint || 'wss://ws-api-spot.kucoin.com';
                    wsUrl = `${ep}?token=${resp.data.token}`;
                    pingMs = (resp.data.instanceServers?.[0]?.pingInterval || 18000);
                }
            } catch (e) {
                console.log('  [KuCoin] Token fetch failed, using default');
            }
            return {
                url: wsUrl,
                pingInterval: pingMs,
                pingMessage: JSON.stringify({ id: Date.now(), type: 'ping' }),
                onOpen: (ws) => {
                    // Subscribe to all 3 coins on single connection
                    ws.send(JSON.stringify({
                        id: Date.now(),
                        type: 'subscribe',
                        topic: '/market/match:BTC-USDT,ETH-USDT,SOL-USDT',
                        privateChannel: false,
                        response: true
                    }));
                },
                parseMessage: (msg) => {
                    try {
                        const d = JSON.parse(msg);
                        if (d.type === 'pong' || d.type === 'welcome' || d.type === 'ack') return null;
                        if (d.topic && d.data) {
                            const s = d.data.symbol || d.topic;
                            if (s.includes('BTC')) return 'BTC';
                            if (s.includes('ETH')) return 'ETH';
                            if (s.includes('SOL')) return 'SOL';
                        }
                    } catch (e) {}
                    return null;
                }
            };
        }
    },

    'OKX': {
        tier: 1,
        getConfig: () => ({
            url: 'wss://ws.okx.com:8443/ws/v5/public',
            onOpen: (ws) => {
                ws.send(JSON.stringify({
                    op: 'subscribe',
                    args: [
                        { channel: 'trades', instId: 'BTC-USDT' },
                        { channel: 'trades', instId: 'ETH-USDT' },
                        { channel: 'trades', instId: 'SOL-USDT' }
                    ]
                }));
            },
            pingInterval: 25000,
            pingMessage: 'ping',
            parseMessage: (msg) => {
                if (msg === 'pong') return null;
                try {
                    const d = JSON.parse(msg);
                    if (d.data && d.arg && d.arg.channel === 'trades') {
                        const inst = d.arg.instId;
                        if (inst === 'BTC-USDT') return 'BTC';
                        if (inst === 'ETH-USDT') return 'ETH';
                        if (inst === 'SOL-USDT') return 'SOL';
                    }
                } catch (e) {}
                return null;
            }
        })
    },

    'Bybit': {
        tier: 1,
        getConfig: () => ({
            url: 'wss://stream.bybit.com/v5/public/spot',
            onOpen: (ws) => {
                ws.send(JSON.stringify({
                    op: 'subscribe',
                    args: ['publicTrade.BTCUSDT', 'publicTrade.ETHUSDT', 'publicTrade.SOLUSDT']
                }));
            },
            pingInterval: 20000,
            pingMessage: JSON.stringify({ op: 'ping' }),
            parseMessage: (msg) => {
                try {
                    const d = JSON.parse(msg);
                    if (d.op === 'pong' || d.ret_msg === 'pong') return null;
                    if (d.topic && d.data) {
                        if (d.topic === 'publicTrade.BTCUSDT') return 'BTC';
                        if (d.topic === 'publicTrade.ETHUSDT') return 'ETH';
                        if (d.topic === 'publicTrade.SOLUSDT') return 'SOL';
                    }
                } catch (e) {}
                return null;
            }
        })
    },

    'Bitfinex': {
        tier: 1,
        getConfig: () => ({
            url: 'wss://api-pub.bitfinex.com/ws/2',
            onOpen: (ws) => {
                ws.send(JSON.stringify({ event: 'subscribe', channel: 'trades', symbol: 'tBTCUSD' }));
                ws.send(JSON.stringify({ event: 'subscribe', channel: 'trades', symbol: 'tETHUSD' }));
                ws.send(JSON.stringify({ event: 'subscribe', channel: 'trades', symbol: 'tSOLUSD' }));
            },
            parseMessage: (msg) => {
                try {
                    const d = JSON.parse(msg);
                    // Bitfinex sends subscription confirmations with chanId mapping
                    if (d.event === 'subscribed') {
                        // Store channel ID mapping on the connection
                        return null;
                    }
                    // Trade updates are arrays: [chanId, 'te'|'tu', [id, mts, amount, price]]
                    // or snapshot: [chanId, [[id, mts, amount, price], ...]]
                    if (Array.isArray(d) && d.length >= 2) {
                        // We'll need to identify by chanId. For simplicity track all as one.
                        // The exchange handler will store chanId->coin mapping
                        return '__bitfinex_array__';
                    }
                } catch (e) {}
                return null;
            },
            // Custom: Bitfinex needs channel ID tracking
            _bitfinexChannels: {}
        })
    },

    'Gate.io': {
        tier: 1,
        getConfig: () => ({
            url: 'wss://api.gateio.ws/ws/v4/',
            onOpen: (ws) => {
                const ts = Math.floor(Date.now() / 1000);
                ws.send(JSON.stringify({ time: ts, channel: 'spot.trades', event: 'subscribe', payload: ['BTC_USDT'] }));
                ws.send(JSON.stringify({ time: ts + 1, channel: 'spot.trades', event: 'subscribe', payload: ['ETH_USDT'] }));
                ws.send(JSON.stringify({ time: ts + 2, channel: 'spot.trades', event: 'subscribe', payload: ['SOL_USDT'] }));
            },
            pingInterval: 15000,
            pingMessage: JSON.stringify({ time: Math.floor(Date.now() / 1000), channel: 'spot.ping', event: 'ping' }),
            parseMessage: (msg) => {
                try {
                    const d = JSON.parse(msg);
                    if (d.channel === 'spot.trades' && d.event === 'update' && d.result) {
                        const cp = d.result.currency_pair;
                        if (cp === 'BTC_USDT') return 'BTC';
                        if (cp === 'ETH_USDT') return 'ETH';
                        if (cp === 'SOL_USDT') return 'SOL';
                    }
                } catch (e) {}
                return null;
            }
        })
    },

    'HTX': {
        tier: 1,
        getConfig: () => ({
            url: 'wss://api.huobi.pro/ws',
            compression: 'gzip',
            onOpen: (ws) => {
                ws.send(JSON.stringify({ sub: 'market.btcusdt.trade.detail', id: 'btc1' }));
                ws.send(JSON.stringify({ sub: 'market.ethusdt.trade.detail', id: 'eth1' }));
                ws.send(JSON.stringify({ sub: 'market.solusdt.trade.detail', id: 'sol1' }));
            },
            handlePing: (parsed, ws) => {
                if (parsed.ping) {
                    ws.send(JSON.stringify({ pong: parsed.ping }));
                    return true;
                }
                return false;
            },
            parseMessage: (msg) => {
                try {
                    const d = JSON.parse(msg);
                    if (d.ping) return null; // Handled by handlePing
                    if (d.ch && d.tick) {
                        if (d.ch.includes('btcusdt')) return 'BTC';
                        if (d.ch.includes('ethusdt')) return 'ETH';
                        if (d.ch.includes('solusdt')) return 'SOL';
                    }
                } catch (e) {}
                return null;
            }
        })
    },

    'WOO X': {
        tier: 1,
        getConfig: () => ({
            url: 'wss://wss.woo.org/ws/stream/OqdphuyCtYWxwzhxyLLjOWNdFP7sQt8RPWzmb5xY',
            pingInterval: 9000,
            pingMessage: JSON.stringify({ event: 'ping' }),
            onOpen: (ws) => {
                ws.send(JSON.stringify({ id: 'btc', event: 'subscribe', topic: 'SPOT_BTC_USDT@trade' }));
                ws.send(JSON.stringify({ id: 'eth', event: 'subscribe', topic: 'SPOT_ETH_USDT@trade' }));
                ws.send(JSON.stringify({ id: 'sol', event: 'subscribe', topic: 'SPOT_SOL_USDT@trade' }));
            },
            parseMessage: (msg) => {
                try {
                    const d = JSON.parse(msg);
                    if (d.event === 'pong' || d.event === 'subscribe') return null;
                    if (d.topic) {
                        if (d.topic.includes('SPOT_BTC_USDT')) return 'BTC';
                        if (d.topic.includes('SPOT_ETH_USDT')) return 'ETH';
                        if (d.topic.includes('SPOT_SOL_USDT')) return 'SOL';
                    }
                } catch (e) {}
                return null;
            }
        })
    },

    // ═══════════════════════ TIER 2 ═══════════════════════

    'Crypto.com': {
        tier: 2,
        getConfig: () => ({
            url: 'wss://stream.crypto.com/exchange/v1/market',
            onOpen: (ws) => {
                ws.send(JSON.stringify({
                    id: 1,
                    method: 'subscribe',
                    params: { channels: ['trade.BTC_USDT', 'trade.ETH_USDT', 'trade.SOL_USDT'] }
                }));
            },
            parseMessage: (msg) => {
                try {
                    const d = JSON.parse(msg);
                    // Trade data format: {code:0, method:'subscribe', result:{channel:'trade', instrument_name:'BTC_USDT', subscription:'trade.BTC_USDT', data:[{i,p,q,s,d,t,dataTime}]}}
                    // Check for trade data FIRST (has result.channel='trade' AND result.data array)
                    if (d.result && d.result.channel === 'trade' && d.result.data && Array.isArray(d.result.data) && d.result.data.length > 0) {
                        const inst = d.result.instrument_name || '';
                        if (inst === 'BTC_USDT') return 'BTC';
                        if (inst === 'ETH_USDT') return 'ETH';
                        if (inst === 'SOL_USDT') return 'SOL';
                        // Fallback: check subscription field
                        const sub = d.result.subscription || '';
                        if (sub === 'trade.BTC_USDT') return 'BTC';
                        if (sub === 'trade.ETH_USDT') return 'ETH';
                        if (sub === 'trade.SOL_USDT') return 'SOL';
                        // Fallback: check first trade item's instrument
                        const firstI = d.result.data[0].i || '';
                        if (firstI === 'BTC_USDT') return 'BTC';
                        if (firstI === 'ETH_USDT') return 'ETH';
                        if (firstI === 'SOL_USDT') return 'SOL';
                    }
                    // Ignore subscription confirmations (have id but no trade data), heartbeats, etc.
                } catch (e) {}
                return null;
            }
        })
    },

    'Bitstamp': {
        tier: 2,
        getConfig: () => ({
            url: 'wss://ws.bitstamp.net',
            onOpen: (ws) => {
                ws.send(JSON.stringify({ event: 'bts:subscribe', data: { channel: 'live_trades_btcusd' } }));
                ws.send(JSON.stringify({ event: 'bts:subscribe', data: { channel: 'live_trades_ethusd' } }));
                ws.send(JSON.stringify({ event: 'bts:subscribe', data: { channel: 'live_trades_solusd' } }));
            },
            parseMessage: (msg) => {
                try {
                    const d = JSON.parse(msg);
                    if (d.event === 'trade' && d.channel) {
                        if (d.channel === 'live_trades_btcusd') return 'BTC';
                        if (d.channel === 'live_trades_ethusd') return 'ETH';
                        if (d.channel === 'live_trades_solusd') return 'SOL';
                    }
                } catch (e) {}
                return null;
            }
        })
    },

    'WhiteBIT': {
        tier: 2,
        getConfig: () => ({
            url: 'wss://api.whitebit.com/ws',
            onOpen: (ws) => {
                ws.send(JSON.stringify({ id: 1, method: 'trades_subscribe', params: ['BTC_USDT', 'ETH_USDT', 'SOL_USDT'] }));
            },
            pingInterval: 25000,
            pingMessage: JSON.stringify({ id: 0, method: 'server.ping', params: [] }),
            parseMessage: (msg) => {
                try {
                    const d = JSON.parse(msg);
                    if (d.method === 'trades_update' && d.params) {
                        const pair = d.params[0];
                        if (pair === 'BTC_USDT') return 'BTC';
                        if (pair === 'ETH_USDT') return 'ETH';
                        if (pair === 'SOL_USDT') return 'SOL';
                    }
                } catch (e) {}
                return null;
            }
        })
    },

    'AscendEX': {
        tier: 2,
        getConfig: () => ({
            url: 'wss://ascendex.com/1/api/pro/v1/stream',
            onOpen: (ws) => {
                ws.send(JSON.stringify({ op: 'sub', ch: 'trades:BTC/USDT' }));
                ws.send(JSON.stringify({ op: 'sub', ch: 'trades:ETH/USDT' }));
                ws.send(JSON.stringify({ op: 'sub', ch: 'trades:SOL/USDT' }));
            },
            pingInterval: 15000,
            pingMessage: JSON.stringify({ op: 'ping' }),
            parseMessage: (msg) => {
                try {
                    const d = JSON.parse(msg);
                    if (d.m === 'pong') return null;
                    if (d.m === 'trades' && d.symbol) {
                        if (d.symbol === 'BTC/USDT') return 'BTC';
                        if (d.symbol === 'ETH/USDT') return 'ETH';
                        if (d.symbol === 'SOL/USDT') return 'SOL';
                    }
                } catch (e) {}
                return null;
            }
        })
    },

    // CoinW removed: Not supported by CCXT, DNS resolution fails (stream.coinw.com not found)

    'BingX': {
        tier: 2,
        getConfig: () => ({
            url: 'wss://open-api-ws.bingx.com/market',
            compression: 'gzip',
            pingInterval: 5000,
            pingMessage: JSON.stringify({ op: 'ping' }),
            onOpen: (ws) => {
                ws.send(JSON.stringify({ id: '1', reqType: 'sub', dataType: 'BTC-USDT@trade' }));
                ws.send(JSON.stringify({ id: '2', reqType: 'sub', dataType: 'ETH-USDT@trade' }));
                ws.send(JSON.stringify({ id: '3', reqType: 'sub', dataType: 'SOL-USDT@trade' }));
            },
            parseMessage: (msg) => {
                try {
                    const d = JSON.parse(msg);
                    if (d.dataType && d.data) {
                        if (d.dataType.includes('BTC-USDT')) return 'BTC';
                        if (d.dataType.includes('ETH-USDT')) return 'ETH';
                        if (d.dataType.includes('SOL-USDT')) return 'SOL';
                    }
                } catch (e) {}
                return null;
            }
        })
    },

    'Toobit': {
        tier: 2,
        getConfig: () => ({
            url: 'wss://stream.toobit.com/quote/ws/v1',
            pingInterval: 30000,
            pingMessage: null, // Will use handlePing
            handlePing: (parsed, ws) => {
                if (parsed.ping) {
                    ws.send(JSON.stringify({ pong: parsed.ping }));
                    return true;
                }
                return false;
            },
            onOpen: (ws) => {
                ws.send(JSON.stringify({ symbol: 'BTCUSDT', topic: 'trade', event: 'sub', params: { binary: false } }));
                ws.send(JSON.stringify({ symbol: 'ETHUSDT', topic: 'trade', event: 'sub', params: { binary: false } }));
                ws.send(JSON.stringify({ symbol: 'SOLUSDT', topic: 'trade', event: 'sub', params: { binary: false } }));
            },
            customPingSetup: (ws) => {
                // Send client pings that the server expects
                return setInterval(() => {
                    if (ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({ ping: Date.now() }));
                    }
                }, 30000);
            },
            parseMessage: (msg) => {
                try {
                    const d = JSON.parse(msg);
                    if (d.pong) return null;
                    if (d.data && d.topic === 'trade' && d.symbol) {
                        if (d.symbol === 'BTCUSDT') return 'BTC';
                        if (d.symbol === 'ETHUSDT') return 'ETH';
                        if (d.symbol === 'SOLUSDT') return 'SOL';
                    }
                    // Array format
                    if (d.data && d.symbolName) {
                        if (d.symbolName === 'BTCUSDT') return 'BTC';
                        if (d.symbolName === 'ETHUSDT') return 'ETH';
                        if (d.symbolName === 'SOLUSDT') return 'SOL';
                    }
                } catch (e) {}
                return null;
            }
        })
    },

    'Deepcoin': {
        tier: 2,
        getConfig: () => ({
            url: 'wss://stream.deepcoin.com/streamlet/trade/public/spot?platform=api',
            pingInterval: 15000,
            pingMessage: 'ping',
            onOpen: (ws) => {
                // FilterValue format: DeepCoin_<SYMBOL> (confirmed by debug testing)
                const reqId = Date.now();
                ws.send(JSON.stringify({ SendTopicAction: { Action: "1", FilterValue: "DeepCoin_BTCUSDT", LocalNo: reqId, ResumeNo: -2, TopicID: "2" } }));
                ws.send(JSON.stringify({ SendTopicAction: { Action: "1", FilterValue: "DeepCoin_ETHUSDT", LocalNo: reqId + 1, ResumeNo: -2, TopicID: "2" } }));
                ws.send(JSON.stringify({ SendTopicAction: { Action: "1", FilterValue: "DeepCoin_SOLUSDT", LocalNo: reqId + 2, ResumeNo: -2, TopicID: "2" } }));
            },
            parseMessage: (msg) => {
                if (msg === 'pong') return null;
                try {
                    const d = JSON.parse(msg);
                    // Trade data format: {"a":"PMT","b":0,"r":[{"d":{"TradeID":"...","I":"BTCUSDT","P":price,"V":volume,"T":ts}}]}
                    if (d.a === 'PMT' && d.r && Array.isArray(d.r)) {
                        const inst = d.r[0] && d.r[0].d && d.r[0].d.I || '';
                        if (inst === 'BTCUSDT' || inst.includes('BTC')) return 'BTC';
                        if (inst === 'ETHUSDT' || inst.includes('ETH')) return 'ETH';
                        if (inst === 'SOLUSDT' || inst.includes('SOL')) return 'SOL';
                    }
                    // Subscription confirmation: {"a":"RecvTopicAction","m":"Success",...}
                    if (d.a === 'RecvTopicAction') return null;
                } catch (e) {}
                return null;
            }
        })
    },

    // ═══════════════════════ TIER 3 ═══════════════════════

    'Upbit': {
        tier: 3,
        getConfig: () => ({
            url: 'wss://api.upbit.com/websocket/v1',
            pingInterval: 30000,
            pingMessage: 'PING',
            onOpen: (ws) => {
                ws.send(JSON.stringify([
                    { ticket: 'unified-collector' },
                    { type: 'trade', codes: ['KRW-BTC', 'KRW-ETH', 'KRW-SOL'] }
                ]));
            },
            parseMessage: (msg) => {
                try {
                    // Upbit sends binary data
                    const str = msg.toString();
                    if (str === 'PONG') return null;
                    const d = JSON.parse(str);
                    if (d.type === 'trade' && d.code) {
                        if (d.code === 'KRW-BTC') return 'BTC';
                        if (d.code === 'KRW-ETH') return 'ETH';
                        if (d.code === 'KRW-SOL') return 'SOL';
                    }
                } catch (e) {}
                return null;
            }
        })
    },

    'LBank': {
        tier: 3,
        getConfig: () => ({
            url: 'wss://www.lbkex.net/ws/V2/',
            pingInterval: 30000,
            pingMessage: JSON.stringify({ action: 'ping', ping: 'ping' }),
            onOpen: (ws) => {
                ws.send(JSON.stringify({ action: 'subscribe', subscribe: 'trade', pair: 'btc_usdt' }));
                ws.send(JSON.stringify({ action: 'subscribe', subscribe: 'trade', pair: 'eth_usdt' }));
                ws.send(JSON.stringify({ action: 'subscribe', subscribe: 'trade', pair: 'sol_usdt' }));
            },
            parseMessage: (msg) => {
                try {
                    const d = JSON.parse(msg);
                    if (d.action === 'pong') return null;
                    if (d.trade && d.pair) {
                        if (d.pair === 'btc_usdt') return 'BTC';
                        if (d.pair === 'eth_usdt') return 'ETH';
                        if (d.pair === 'sol_usdt') return 'SOL';
                    }
                    // Alternative format
                    if (d.type === 'trade' || d.action === 'trade') {
                        const p = d.pair || '';
                        if (p.includes('btc')) return 'BTC';
                        if (p.includes('eth')) return 'ETH';
                        if (p.includes('sol')) return 'SOL';
                    }
                } catch (e) {}
                return null;
            }
        })
    },

    'BitMart': {
        tier: 3,
        getConfig: () => ({
            url: 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1',
            compression: 'inflate',
            pingInterval: 10000,
            pingMessage: 'ping',
            onOpen: (ws) => {
                ws.send(JSON.stringify({ op: 'subscribe', args: ['spot/trade:BTC_USDT', 'spot/trade:ETH_USDT', 'spot/trade:SOL_USDT'] }));
            },
            parseMessage: (msg) => {
                if (msg === 'pong') return null;
                try {
                    const d = JSON.parse(msg);
                    if (d.table === 'spot/trade' && d.data) {
                        const s = d.data[0]?.symbol;
                        if (s === 'BTC_USDT') return 'BTC';
                        if (s === 'ETH_USDT') return 'ETH';
                        if (s === 'SOL_USDT') return 'SOL';
                    }
                } catch (e) {}
                return null;
            }
        })
    },

    'Pionex': {
        tier: 3,
        getConfig: () => ({
            url: 'wss://ws.pionex.com/wsPub',
            pingInterval: 15000,
            pingMessage: JSON.stringify({ op: 'PING' }),
            onOpen: (ws) => {
                ws.send(JSON.stringify({ op: 'SUBSCRIBE', topic: 'TRADE', symbol: 'BTC_USDT' }));
                ws.send(JSON.stringify({ op: 'SUBSCRIBE', topic: 'TRADE', symbol: 'ETH_USDT' }));
                ws.send(JSON.stringify({ op: 'SUBSCRIBE', topic: 'TRADE', symbol: 'SOL_USDT' }));
            },
            parseMessage: (msg) => {
                try {
                    const d = JSON.parse(msg);
                    if (d.op === 'PONG') return null;
                    if (d.topic === 'TRADE' && d.data) {
                        if (d.symbol === 'BTC_USDT') return 'BTC';
                        if (d.symbol === 'ETH_USDT') return 'ETH';
                        if (d.symbol === 'SOL_USDT') return 'SOL';
                    }
                } catch (e) {}
                return null;
            }
        })
    },

    'Poloniex': {
        tier: 3,
        getConfig: () => ({
            url: 'wss://ws.poloniex.com/ws/public',
            pingInterval: 30000,
            pingMessage: JSON.stringify({ event: 'ping' }),
            onOpen: (ws) => {
                ws.send(JSON.stringify({ event: 'subscribe', channel: ['trades'], symbols: ['BTC_USDT', 'ETH_USDT', 'SOL_USDT'] }));
            },
            parseMessage: (msg) => {
                try {
                    const d = JSON.parse(msg);
                    if (d.event === 'pong') return null;
                    if (d.channel === 'trades' && d.data) {
                        const items = Array.isArray(d.data) ? d.data : [d.data];
                        for (const item of items) {
                            if (item.symbol === 'BTC_USDT') return 'BTC';
                            if (item.symbol === 'ETH_USDT') return 'ETH';
                            if (item.symbol === 'SOL_USDT') return 'SOL';
                        }
                    }
                } catch (e) {}
                return null;
            }
        })
    },
};

// ======================== BITFINEX CHANNEL TRACKING ========================
// Bitfinex requires special handling: it maps chanIds to symbols
const bitfinexChannels = {};

// ======================== CONNECTION ENGINE ========================

async function connectExchange(name) {
    const exchangeDef = EXCHANGES[name];
    if (!exchangeDef) return;

    // Initialize stats
    initStats(name);

    // Get config (may be async for KuCoin token fetch)
    let config;
    try {
        config = typeof exchangeDef.getConfig === 'function' 
            ? await exchangeDef.getConfig() 
            : exchangeDef.getConfig;
    } catch (err) {
        log(name, `❌ Config error: ${err.message}`);
        connections[name] = { status: 'error', error: err.message, reconnects: 0 };
        return;
    }

    // Store connection state
    connections[name] = {
        status: 'connecting',
        reconnects: 0,
        lastMsg: null,
        config: config,
        tier: exchangeDef.tier
    };

    doConnect(name, config, exchangeDef);
}

function doConnect(name, config, exchangeDef) {
    if (isShuttingDown) return;

    const conn = connections[name];
    conn.status = 'connecting';

    try {
        const wsOpts = {
            headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' },
            handshakeTimeout: CONNECTION_TIMEOUT
        };

        // Some exchanges need permessage-deflate disabled
        if (config.compression) {
            wsOpts.perMessageDeflate = false;
        }

        const ws = new WebSocket(config.url, wsOpts);
        conn.ws = ws;

        // Connection timeout
        const connTimeout = setTimeout(() => {
            if (ws.readyState !== WebSocket.OPEN) {
                log(name, '⏰ Connection timeout');
                ws.terminate();
                scheduleReconnect(name, config, exchangeDef);
            }
        }, CONNECTION_TIMEOUT);

        ws.on('open', () => {
            clearTimeout(connTimeout);
            conn.status = 'connected';
            conn.reconnects = 0;
            log(name, `✅ Connected (Tier ${exchangeDef.tier})`);

            // Run onOpen to subscribe
            if (config.onOpen) {
                try {
                    config.onOpen(ws);
                } catch (e) {
                    log(name, `⚠ Subscribe error: ${e.message}`);
                }
            }

            // Setup ping interval
            if (config.pingInterval && config.pingMessage) {
                conn.pingTimer = setInterval(() => {
                    if (ws.readyState === WebSocket.OPEN) {
                        ws.send(typeof config.pingMessage === 'string' ? config.pingMessage : JSON.stringify(config.pingMessage));
                    }
                }, config.pingInterval);
            }

            // Custom ping setup (e.g., Toobit)
            if (config.customPingSetup) {
                conn.customPingTimer = config.customPingSetup(ws);
            }
        });

        ws.on('message', (data) => {
            conn.lastMsg = Date.now();

            // Handle compression
            let str;
            if (config.compression && Buffer.isBuffer(data)) {
                try {
                    if (config.compression === 'gzip') {
                        str = zlib.gunzipSync(data).toString();
                    } else if (config.compression === 'inflate') {
                        str = zlib.inflateSync(data).toString();
                    } else if (config.compression === 'inflateRaw') {
                        str = zlib.inflateRawSync(data).toString();
                    } else {
                        // Try all methods
                        try { str = zlib.gunzipSync(data).toString(); } catch (e) {
                            try { str = zlib.inflateSync(data).toString(); } catch (e2) {
                                try { str = zlib.inflateRawSync(data).toString(); } catch (e3) {
                                    str = data.toString();
                                }
                            }
                        }
                    }
                } catch (e) {
                    str = data.toString();
                }
            } else {
                str = data.toString();
            }

            // Handle server-initiated ping (HTX, Toobit, CoinW)
            if (config.handlePing) {
                try {
                    const parsed = JSON.parse(str);
                    if (config.handlePing(parsed, ws)) return;
                } catch (e) {}
            }

            // Special Bitfinex handling
            if (name === 'Bitfinex') {
                try {
                    const d = JSON.parse(str);
                    if (d.event === 'subscribed' && d.chanId) {
                        // Map channel ID to coin
                        const sym = d.symbol || d.pair || '';
                        if (sym.includes('BTC')) bitfinexChannels[d.chanId] = 'BTC';
                        else if (sym.includes('ETH')) bitfinexChannels[d.chanId] = 'ETH';
                        else if (sym.includes('SOL')) bitfinexChannels[d.chanId] = 'SOL';
                        return;
                    }
                    if (Array.isArray(d) && d.length >= 2 && d[1] !== 'hb') {
                        const coin = bitfinexChannels[d[0]];
                        if (coin) {
                            recordMessage(name, coin);
                        }
                        return;
                    }
                } catch (e) {}
                return;
            }

            // Parse message with exchange-specific parser
            const coin = config.parseMessage(str);
            if (coin && COINS.includes(coin)) {
                recordMessage(name, coin);
            }
        });

        ws.on('error', (err) => {
            clearTimeout(connTimeout);
            log(name, `❌ Error: ${err.message}`);
            conn.status = 'error';
        });

        ws.on('close', (code, reason) => {
            clearTimeout(connTimeout);
            if (conn.pingTimer) clearInterval(conn.pingTimer);
            if (conn.customPingTimer) clearInterval(conn.customPingTimer);
            conn.status = 'disconnected';
            
            if (!isShuttingDown) {
                log(name, `🔌 Disconnected (${code}). Reconnecting...`);
                scheduleReconnect(name, config, exchangeDef);
            }
        });

    } catch (err) {
        log(name, `❌ Connection error: ${err.message}`);
        conn.status = 'error';
        scheduleReconnect(name, config, exchangeDef);
    }
}

function scheduleReconnect(name, config, exchangeDef) {
    if (isShuttingDown) return;
    
    const conn = connections[name];
    conn.reconnects = (conn.reconnects || 0) + 1;
    const delay = Math.min(RECONNECT_BASE_DELAY * Math.pow(2, conn.reconnects - 1), RECONNECT_MAX_DELAY);
    
    log(name, `🔄 Reconnect #${conn.reconnects} in ${(delay / 1000).toFixed(0)}s`);
    
    conn.reconnectTimer = setTimeout(async () => {
        // For KuCoin, re-fetch token on reconnect
        if (name === 'KuCoin') {
            try {
                const newConfig = await exchangeDef.getConfig();
                conn.config = newConfig;
                doConnect(name, newConfig, exchangeDef);
            } catch (e) {
                log(name, `❌ Token re-fetch failed: ${e.message}`);
                scheduleReconnect(name, config, exchangeDef);
            }
        } else {
            doConnect(name, config, exchangeDef);
        }
    }, delay);
}

// ======================== ANALYTICS ENGINE ========================

function snapshotPerSecond() {
    secondCounter++;
    for (const exchange of Object.keys(stats)) {
        for (const coin of COINS) {
            stats[exchange][coin].history.push(currentSecond[exchange][coin]);
            currentSecond[exchange][coin] = 0;
        }
    }
}

function printDashboard() {
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    
    console.log('\n' + '═'.repeat(110));
    console.log(`  📊 UNIFIED SPOT COLLECTOR DASHBOARD | Elapsed: ${elapsed}s | Exchanges: ${Object.keys(EXCHANGES).length}`);
    console.log('═'.repeat(110));
    
    // Header
    console.log(
        '  ' + 'Exchange'.padEnd(15) +
        'Tier'.padEnd(6) +
        'Status'.padEnd(14) +
        '│ BTC Total'.padEnd(13) +
        'BTC/s'.padEnd(9) +
        '│ ETH Total'.padEnd(13) +
        'ETH/s'.padEnd(9) +
        '│ SOL Total'.padEnd(13) +
        'SOL/s'.padEnd(9) +
        '│ Reconn'
    );
    console.log('  ' + '─'.repeat(106));
    
    // Sort by tier then name
    const sorted = Object.keys(EXCHANGES).sort((a, b) => {
        const ta = EXCHANGES[a].tier, tb = EXCHANGES[b].tier;
        if (ta !== tb) return ta - tb;
        return a.localeCompare(b);
    });

    let totalBTC = 0, totalETH = 0, totalSOL = 0;
    let connectedCount = 0;
    
    for (const name of sorted) {
        const conn = connections[name] || {};
        const stat = stats[name] || {};
        const tier = EXCHANGES[name].tier;
        
        // Status emoji
        let statusStr;
        switch (conn.status) {
            case 'connected': statusStr = '🟢 Active'; connectedCount++; break;
            case 'connecting': statusStr = '🟡 Connecting'; break;
            case 'disconnected': statusStr = '🔴 Disconn'; break;
            case 'error': statusStr = '❌ Error'; break;
            default: statusStr = '⚪ Unknown';
        }

        const btcTotal = stat.BTC?.total || 0;
        const ethTotal = stat.ETH?.total || 0;
        const solTotal = stat.SOL?.total || 0;
        totalBTC += btcTotal;
        totalETH += ethTotal;
        totalSOL += solTotal;

        // Calculate msgs/sec (average over last 5 seconds)
        const btcRate = avgRate(stat.BTC?.history || []);
        const ethRate = avgRate(stat.ETH?.history || []);
        const solRate = avgRate(stat.SOL?.history || []);

        const reconn = conn.reconnects || 0;

        console.log(
            '  ' + name.padEnd(15) +
            `T${tier}`.padEnd(6) +
            statusStr.padEnd(14) +
            `│ ${btcTotal.toString().padStart(8)}`.padEnd(13) +
            `${btcRate.toFixed(1)}`.padEnd(9) +
            `│ ${ethTotal.toString().padStart(8)}`.padEnd(13) +
            `${ethRate.toFixed(1)}`.padEnd(9) +
            `│ ${solTotal.toString().padStart(8)}`.padEnd(13) +
            `${solRate.toFixed(1)}`.padEnd(9) +
            `│ ${reconn}`
        );
    }

    console.log('  ' + '─'.repeat(106));
    console.log(
        '  ' + 'TOTAL'.padEnd(15) +
        ''.padEnd(6) +
        `${connectedCount}/${Object.keys(EXCHANGES).length} connected`.padEnd(14) +
        `│ ${totalBTC.toString().padStart(8)}`.padEnd(13) +
        ''.padEnd(9) +
        `│ ${totalETH.toString().padStart(8)}`.padEnd(13) +
        ''.padEnd(9) +
        `│ ${totalSOL.toString().padStart(8)}`.padEnd(13) +
        ''.padEnd(9) +
        '│'
    );
    console.log('═'.repeat(110) + '\n');
}

function avgRate(history) {
    if (history.length === 0) return 0;
    const window = history.slice(-5);
    return window.reduce((a, b) => a + b, 0) / window.length;
}

// ======================== MAIN ========================

async function main() {
    console.log('\n' + '╔'.padEnd(109, '═') + '╗');
    console.log('║  UNIFIED SPOT COLLECTOR - BTC / ETH / SOL                                                               ║');
    console.log('║  Streaming from 22 exchanges simultaneously                                                              ║');
    console.log('║  Press Ctrl+C to stop                                                                                    ║');
    console.log('╚'.padEnd(109, '═') + '╝\n');

    startTime = Date.now();

    // Connect all exchanges concurrently
    console.log('🚀 Connecting to all exchanges...\n');
    
    const exchangeNames = Object.keys(EXCHANGES);
    
    // Stagger connections slightly to avoid thundering herd
    for (let i = 0; i < exchangeNames.length; i++) {
        connectExchange(exchangeNames[i]);
        await new Promise(r => setTimeout(r, 200)); // 200ms stagger
    }

    // Per-second snapshot timer
    const secTimer = setInterval(snapshotPerSecond, 1000);

    // Dashboard timer
    const dashTimer = setInterval(printDashboard, DASHBOARD_INTERVAL);

    // Print first dashboard after 8 seconds (give time to connect)
    setTimeout(printDashboard, 8000);

    // If TEST_DURATION > 0, auto-stop
    if (TEST_DURATION > 0) {
        setTimeout(() => {
            console.log(`\n⏱ Test duration (${TEST_DURATION / 1000}s) reached. Stopping...`);
            shutdown(secTimer, dashTimer);
        }, TEST_DURATION);
    }

    // Graceful shutdown on Ctrl+C
    process.on('SIGINT', () => shutdown(secTimer, dashTimer));
    process.on('SIGTERM', () => shutdown(secTimer, dashTimer));
}

function shutdown(secTimer, dashTimer) {
    if (isShuttingDown) return;
    isShuttingDown = true;

    console.log('\n🛑 Shutting down...');
    
    clearInterval(secTimer);
    clearInterval(dashTimer);

    // Close all connections
    for (const [name, conn] of Object.entries(connections)) {
        if (conn.pingTimer) clearInterval(conn.pingTimer);
        if (conn.customPingTimer) clearInterval(conn.customPingTimer);
        if (conn.reconnectTimer) clearTimeout(conn.reconnectTimer);
        if (conn.ws) {
            try { conn.ws.terminate(); } catch (e) {}
        }
    }

    // Final report
    printFinalReport();
    process.exit(0);
}

function printFinalReport() {
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    
    console.log('\n' + '╔'.padEnd(109, '═') + '╗');
    console.log('║  FINAL COLLECTION REPORT                                                                                 ║');
    console.log('╚'.padEnd(109, '═') + '╝');
    console.log(`  Duration: ${elapsed} seconds\n`);
    
    console.log('  ' + 'Exchange'.padEnd(15) + 'Tier'.padEnd(6) + 
        'BTC msgs'.padEnd(12) + 'BTC avg/s'.padEnd(12) +
        'ETH msgs'.padEnd(12) + 'ETH avg/s'.padEnd(12) +
        'SOL msgs'.padEnd(12) + 'SOL avg/s'.padEnd(12) +
        'Status'
    );
    console.log('  ' + '─'.repeat(95));

    const sorted = Object.keys(EXCHANGES).sort((a, b) => {
        const ta = EXCHANGES[a].tier, tb = EXCHANGES[b].tier;
        if (ta !== tb) return ta - tb;
        return a.localeCompare(b);
    });

    let grandBTC = 0, grandETH = 0, grandSOL = 0;
    let streaming = 0, notStreaming = 0;

    for (const name of sorted) {
        const stat = stats[name] || {};
        const conn = connections[name] || {};
        const tier = EXCHANGES[name].tier;
        
        const btc = stat.BTC?.total || 0;
        const eth = stat.ETH?.total || 0;
        const sol = stat.SOL?.total || 0;
        const total = btc + eth + sol;
        
        grandBTC += btc;
        grandETH += eth;
        grandSOL += sol;

        const btcRate = elapsed > 0 ? (btc / elapsed).toFixed(2) : '0';
        const ethRate = elapsed > 0 ? (eth / elapsed).toFixed(2) : '0';
        const solRate = elapsed > 0 ? (sol / elapsed).toFixed(2) : '0';

        const status = total > 0 ? '✅ STREAMING' : '❌ NO DATA';
        if (total > 0) streaming++; else notStreaming++;

        console.log(
            '  ' + name.padEnd(15) + `T${tier}`.padEnd(6) +
            btc.toString().padEnd(12) + btcRate.padEnd(12) +
            eth.toString().padEnd(12) + ethRate.padEnd(12) +
            sol.toString().padEnd(12) + solRate.padEnd(12) +
            status
        );
    }

    console.log('  ' + '─'.repeat(95));
    console.log(
        '  ' + 'TOTAL'.padEnd(15) + ''.padEnd(6) +
        grandBTC.toString().padEnd(12) + (grandBTC / elapsed).toFixed(2).padEnd(12) +
        grandETH.toString().padEnd(12) + (grandETH / elapsed).toFixed(2).padEnd(12) +
        grandSOL.toString().padEnd(12) + (grandSOL / elapsed).toFixed(2).padEnd(12)
    );
    
    console.log(`\n  📊 Summary: ${streaming} streaming, ${notStreaming} not streaming out of ${Object.keys(EXCHANGES).length} exchanges`);
    console.log(`  📈 Total messages collected: ${grandBTC + grandETH + grandSOL}`);
    console.log(`  ⏱  Average throughput: ${((grandBTC + grandETH + grandSOL) / elapsed).toFixed(2)} msgs/sec across all exchanges\n`);
}

// ======================== RUN ========================
main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});
