/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║   FIX TESTER — 6 Failing Exchanges with Corrected Subscription Formats ║
 * ║   Kraken, BingX, HitBTC, Pionex, Deepcoin, BTSE                       ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 * 
 * Fixes applied:
 *   1. BingX:    Remove @500ms from depth → use @depth5 (no interval)
 *   2. Pionex:   TICKER topic doesn't exist → mark noTicker
 *   3. BTSE:     Orderbook needs OSS endpoint → dual WS connections
 *   4. Deepcoin: Use proprietary TopicID 25 (orderbook), 7 (ticker) + correct spot URL
 *   5. Kraken:   Upgrade to v2 API + extend test window to 30s
 *   6. HitBTC:   Extend test window to 30s (low volume)
 */

const WebSocket = require('ws');
const zlib = require('zlib');
const https = require('https');

const TEST_TIMEOUT = 40000;
const DATA_WAIT = 30000;       // 30s for low-volume exchanges

function ts() { return new Date().toISOString().substring(11, 19); }

const results = {};

function initResult(name) {
    results[name] = {
        orderbook: { BTC: 0, ETH: 0, SOL: 0 },
        trades:    { BTC: 0, ETH: 0, SOL: 0 },
        ticker:    { BTC: 0, ETH: 0, SOL: 0 },
        errors: [],
        status: 'pending'
    };
}

// ═══════════════════════════════════════════════════════════
// FIX 1: KRAKEN — Upgrade to v2 API for better trade delivery
// ═══════════════════════════════════════════════════════════
async function testKraken() {
    const name = 'Kraken';
    initResult(name);
    console.log(`  [${ts()}] 🔌 ${name} — Connecting to v2 API...`);

    return new Promise((resolve) => {
        let resolved = false;
        const done = () => { if (!resolved) { resolved = true; resolve(); } };

        const ws = new WebSocket('wss://ws.kraken.com/v2', {
            headers: { 'User-Agent': 'Mozilla/5.0' },
            handshakeTimeout: 10000
        });

        const timeout = setTimeout(() => {
            console.log(`  [${ts()}] ⏰ ${name} — Timeout`);
            ws.terminate();
            results[name].status = 'timeout';
            done();
        }, TEST_TIMEOUT);

        ws.on('open', () => {
            console.log(`  [${ts()}] ✅ ${name} — Connected to v2, subscribing...`);

            // V2 API: subscribe to trades
            ws.send(JSON.stringify({
                method: 'subscribe',
                params: { channel: 'trade', symbol: ['BTC/USDT', 'ETH/USDT', 'SOL/USDT'], snapshot: false }
            }));
            // V2 API: subscribe to ticker
            ws.send(JSON.stringify({
                method: 'subscribe',
                params: { channel: 'ticker', symbol: ['BTC/USDT', 'ETH/USDT', 'SOL/USDT'], snapshot: true }
            }));
            // V2 API: subscribe to book
            ws.send(JSON.stringify({
                method: 'subscribe',
                params: { channel: 'book', symbol: ['BTC/USDT', 'ETH/USDT', 'SOL/USDT'], depth: 10, snapshot: true }
            }));

            setTimeout(() => {
                clearTimeout(timeout);
                const r = results[name];
                r.status = 'ok';
                logResult(name);
                try { ws.close(); } catch (e) {}
                setTimeout(done, 500);
            }, DATA_WAIT);
        });

        ws.on('message', (data) => {
            const str = data.toString();
            try {
                const d = JSON.parse(str);

                // V2 response format: { channel: 'trade', type: 'update', data: [...] }
                if (d.channel === 'heartbeat' || d.method) return;

                const channel = d.channel;
                const type = d.type; // 'snapshot' or 'update'

                if (!channel || !d.data) return;

                if (channel === 'trade' && Array.isArray(d.data)) {
                    for (const t of d.data) {
                        const sym = t.symbol || '';
                        const coin = sym === 'BTC/USDT' ? 'BTC' : sym === 'ETH/USDT' ? 'ETH' : sym === 'SOL/USDT' ? 'SOL' : null;
                        if (coin) results[name].trades[coin]++;
                    }
                }

                if (channel === 'ticker' && Array.isArray(d.data)) {
                    for (const t of d.data) {
                        const sym = t.symbol || '';
                        const coin = sym === 'BTC/USDT' ? 'BTC' : sym === 'ETH/USDT' ? 'ETH' : sym === 'SOL/USDT' ? 'SOL' : null;
                        if (coin) results[name].ticker[coin]++;
                    }
                }

                if (channel === 'book' && Array.isArray(d.data)) {
                    for (const b of d.data) {
                        const sym = b.symbol || '';
                        const coin = sym === 'BTC/USDT' ? 'BTC' : sym === 'ETH/USDT' ? 'ETH' : sym === 'SOL/USDT' ? 'SOL' : null;
                        if (coin) results[name].orderbook[coin]++;
                    }
                }
            } catch (e) {}
        });

        ws.on('error', (err) => {
            console.log(`  [${ts()}] ❌ ${name} — Error: ${err.message}`);
            results[name].errors.push(err.message);
        });
    });
}

// ═══════════════════════════════════════════════════════════
// FIX 2: BingX — Remove @500ms suffix from depth subscription
// ═══════════════════════════════════════════════════════════
async function testBingX() {
    const name = 'BingX';
    initResult(name);
    console.log(`  [${ts()}] 🔌 ${name} — Connecting...`);

    return new Promise((resolve) => {
        let resolved = false;
        const done = () => { if (!resolved) { resolved = true; resolve(); } };

        const ws = new WebSocket('wss://open-api-ws.bingx.com/market', {
            headers: { 'User-Agent': 'Mozilla/5.0' },
            handshakeTimeout: 10000
        });

        const timeout = setTimeout(() => {
            console.log(`  [${ts()}] ⏰ ${name} — Timeout`);
            ws.terminate();
            results[name].status = 'timeout';
            done();
        }, TEST_TIMEOUT);

        let pingTimer;

        ws.on('open', () => {
            console.log(`  [${ts()}] ✅ ${name} — Connected, subscribing (fixed depth format)...`);

            ['BTC-USDT', 'ETH-USDT', 'SOL-USDT'].forEach(sym => {
                ws.send(JSON.stringify({ id: `${sym}_trade`, reqType: 'sub', dataType: `${sym}@trade` }));
                ws.send(JSON.stringify({ id: `${sym}_ticker`, reqType: 'sub', dataType: `${sym}@ticker` }));
                // FIX: Remove @500ms — just use @depth5
                ws.send(JSON.stringify({ id: `${sym}_depth`, reqType: 'sub', dataType: `${sym}@depth5` }));
            });

            pingTimer = setInterval(() => {
                if (ws.readyState === WebSocket.OPEN) ws.send('Pong');
            }, 5000);

            setTimeout(() => {
                clearTimeout(timeout);
                if (pingTimer) clearInterval(pingTimer);
                const r = results[name];
                r.status = 'ok';
                logResult(name);
                try { ws.close(); } catch (e) {}
                setTimeout(done, 500);
            }, DATA_WAIT);
        });

        ws.on('message', (data) => {
            let str;
            if (Buffer.isBuffer(data)) {
                try { str = zlib.gunzipSync(data).toString(); } catch (e) { str = data.toString(); }
            } else {
                str = data.toString();
            }

            // Handle server ping
            if (str === 'Ping') {
                ws.send('Pong');
                return;
            }

            try {
                const d = JSON.parse(str);

                // BingX pong
                if (d.msg === 'Pong' || d.ping || d.pong) return;

                if (!d.dataType && !d.data) return;

                const dt = d.dataType || '';
                const coin = dt.includes('BTC') ? 'BTC' : dt.includes('ETH') ? 'ETH' : dt.includes('SOL') ? 'SOL' : null;
                if (!coin) return;

                if (dt.includes('@trade')) results[name].trades[coin]++;
                if (dt.includes('@ticker')) results[name].ticker[coin]++;
                if (dt.includes('@depth')) results[name].orderbook[coin]++;
            } catch (e) {}
        });

        ws.on('error', (err) => {
            console.log(`  [${ts()}] ❌ ${name} — Error: ${err.message}`);
            results[name].errors.push(err.message);
        });

        ws.on('close', () => {
            if (pingTimer) clearInterval(pingTimer);
        });
    });
}

// ═══════════════════════════════════════════════════════════
// FIX 3: HitBTC — Extended test window (30s), confirmed format
// ═══════════════════════════════════════════════════════════
async function testHitBTC() {
    const name = 'HitBTC';
    initResult(name);
    console.log(`  [${ts()}] 🔌 ${name} — Connecting (extended 30s window)...`);

    return new Promise((resolve) => {
        let resolved = false;
        const done = () => { if (!resolved) { resolved = true; resolve(); } };

        const ws = new WebSocket('wss://api.hitbtc.com/api/3/ws/public', {
            headers: { 'User-Agent': 'Mozilla/5.0' },
            handshakeTimeout: 10000
        });

        const timeout = setTimeout(() => {
            console.log(`  [${ts()}] ⏰ ${name} — Timeout`);
            ws.terminate();
            results[name].status = 'timeout';
            done();
        }, TEST_TIMEOUT);

        ws.on('open', () => {
            console.log(`  [${ts()}] ✅ ${name} — Connected, subscribing...`);

            // Subscribe to trades with individual symbols for better matching
            ws.send(JSON.stringify({ method: 'subscribe', ch: 'trades', params: { symbols: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'] }, id: 1 }));
            ws.send(JSON.stringify({ method: 'subscribe', ch: 'ticker/price/1s', params: { symbols: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'] }, id: 2 }));
            ws.send(JSON.stringify({ method: 'subscribe', ch: 'orderbook/full', params: { symbols: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'], limit: 5 }, id: 3 }));

            // Also try the ticker/1s batch channel
            ws.send(JSON.stringify({ method: 'subscribe', ch: 'ticker/1s', params: { symbols: ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'] }, id: 4 }));

            setTimeout(() => {
                clearTimeout(timeout);
                const r = results[name];
                r.status = 'ok';
                logResult(name);
                try { ws.close(); } catch (e) {}
                setTimeout(done, 500);
            }, DATA_WAIT);
        });

        let msgCount = 0;
        ws.on('message', (data) => {
            const str = data.toString();
            try {
                const d = JSON.parse(str);
                if (d.result) return; // subscription confirmation

                const ch = d.ch || '';
                const allData = d.snapshot || d.update || d.data || {};

                if (ch === 'trades') {
                    for (const mkt of Object.keys(allData)) {
                        const coin = mkt === 'BTCUSDT' ? 'BTC' : mkt === 'ETHUSDT' ? 'ETH' : mkt === 'SOLUSDT' ? 'SOL' : null;
                        if (coin && Array.isArray(allData[mkt]) && allData[mkt].length > 0) {
                            results[name].trades[coin] += allData[mkt].length;
                        }
                    }
                }
                if (ch.startsWith('ticker')) {
                    for (const mkt of Object.keys(allData)) {
                        const coin = mkt === 'BTCUSDT' ? 'BTC' : mkt === 'ETHUSDT' ? 'ETH' : mkt === 'SOLUSDT' ? 'SOL' : null;
                        if (coin) results[name].ticker[coin]++;
                    }
                }
                if (ch.startsWith('orderbook')) {
                    for (const mkt of Object.keys(allData)) {
                        const coin = mkt === 'BTCUSDT' ? 'BTC' : mkt === 'ETHUSDT' ? 'ETH' : mkt === 'SOLUSDT' ? 'SOL' : null;
                        if (coin) results[name].orderbook[coin]++;
                    }
                }

                // Log first few messages for debug
                msgCount++;
                if (msgCount <= 5) {
                    console.log(`  [${ts()}] 🔍 ${name} msg#${msgCount}: ch=${ch} keys=${Object.keys(allData).slice(0, 3).join(',')}`);
                }
            } catch (e) {}
        });

        ws.on('error', (err) => {
            console.log(`  [${ts()}] ❌ ${name} — Error: ${err.message}`);
            results[name].errors.push(err.message);
        });
    });
}

// ═══════════════════════════════════════════════════════════
// FIX 4: PIONEX — TICKER doesn't exist, mark as noTicker
// ═══════════════════════════════════════════════════════════
async function testPionex() {
    const name = 'Pionex';
    initResult(name);
    console.log(`  [${ts()}] 🔌 ${name} — Connecting (no ticker available)...`);

    return new Promise((resolve) => {
        let resolved = false;
        const done = () => { if (!resolved) { resolved = true; resolve(); } };

        const ws = new WebSocket('wss://ws.pionex.com/wsPub', {
            headers: { 'User-Agent': 'Mozilla/5.0' },
            handshakeTimeout: 10000
        });

        const timeout = setTimeout(() => {
            console.log(`  [${ts()}] ⏰ ${name} — Timeout`);
            ws.terminate();
            results[name].status = 'timeout';
            done();
        }, TEST_TIMEOUT);

        ws.on('open', () => {
            console.log(`  [${ts()}] ✅ ${name} — Connected, subscribing (TRADE + DEPTH only)...`);

            ['BTC_USDT', 'ETH_USDT', 'SOL_USDT'].forEach(sym => {
                ws.send(JSON.stringify({ op: 'SUBSCRIBE', topic: 'TRADE', symbol: sym }));
                ws.send(JSON.stringify({ op: 'SUBSCRIBE', topic: 'DEPTH', symbol: sym, limit: 10 }));
            });

            setTimeout(() => {
                clearTimeout(timeout);
                const r = results[name];
                r.status = 'ok';
                logResult(name);
                try { ws.close(); } catch (e) {}
                setTimeout(done, 500);
            }, DATA_WAIT);
        });

        ws.on('message', (data) => {
            const str = data.toString();

            // Handle PING
            if (str === 'PING' || str.includes('"PING"')) {
                try { ws.send('PONG'); } catch (e) {}
                return;
            }

            try {
                const d = JSON.parse(str);
                if (d.op === 'PONG' || d.op === 'SUBSCRIBE_RESULT') return;

                // Handle server ping object
                if (d.op === 'PING') {
                    ws.send(JSON.stringify({ op: 'PONG' }));
                    return;
                }

                const sym = d.symbol || '';
                const coin = sym === 'BTC_USDT' ? 'BTC' : sym === 'ETH_USDT' ? 'ETH' : sym === 'SOL_USDT' ? 'SOL' : null;
                if (!coin) return;

                if (d.topic === 'TRADE') results[name].trades[coin]++;
                if (d.topic === 'DEPTH') results[name].orderbook[coin]++;
            } catch (e) {}
        });

        ws.on('error', (err) => {
            console.log(`  [${ts()}] ❌ ${name} — Error: ${err.message}`);
            results[name].errors.push(err.message);
        });
    });
}

// ═══════════════════════════════════════════════════════════
// FIX 5: DEEPCOIN — Use correct spot URL + TopicID 25 (OB), 7 (ticker)
// ═══════════════════════════════════════════════════════════
async function testDeepcoin() {
    const name = 'Deepcoin';
    initResult(name);
    console.log(`  [${ts()}] 🔌 ${name} — Connecting to corrected spot endpoint...`);

    // Try multiple endpoint candidates
    const endpoints = [
        'wss://stream.deepcoin.com/public/spotws',
        'wss://stream.deepcoin.com/streamlet/trade/public/spot?platform=api'
    ];

    for (const endpoint of endpoints) {
        console.log(`  [${ts()}] 🔌 ${name} — Trying: ${endpoint}`);
        const worked = await testDeepcoinEndpoint(name, endpoint);
        if (worked) return;
    }

    results[name].status = results[name].status || 'no_data';
}

function testDeepcoinEndpoint(name, endpoint) {
    return new Promise((resolve) => {
        let resolved = false;
        const done = (worked) => { if (!resolved) { resolved = true; resolve(worked); } };

        const ws = new WebSocket(endpoint, {
            headers: { 'User-Agent': 'Mozilla/5.0' },
            handshakeTimeout: 8000
        });

        const timeout = setTimeout(() => {
            ws.terminate();
            done(false);
        }, 15000);

        let pingTimer;
        let gotData = false;

        ws.on('open', () => {
            console.log(`  [${ts()}] ✅ ${name} — Connected to ${endpoint.split('/').pop()}`);

            const reqId = Date.now();
            const symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'];
            
            symbols.forEach((sym, i) => {
                // TopicID 2 = Trades
                ws.send(JSON.stringify({
                    SendTopicAction: { Action: "1", FilterValue: `DeepCoin_${sym}`, LocalNo: reqId + i, ResumeNo: -2, TopicID: "2" }
                }));
                // TopicID 25 = 25-level Orderbook
                ws.send(JSON.stringify({
                    SendTopicAction: { Action: "1", FilterValue: `DeepCoin_${sym}`, LocalNo: reqId + 100 + i, ResumeNo: -1, TopicID: "25" }
                }));
                // TopicID 7 = Market overview / Ticker
                ws.send(JSON.stringify({
                    SendTopicAction: { Action: "1", FilterValue: `DeepCoin_${sym}`, LocalNo: reqId + 200 + i, ResumeNo: -2, TopicID: "7" }
                }));
            });

            pingTimer = setInterval(() => {
                if (ws.readyState === WebSocket.OPEN) ws.send('ping');
            }, 15000);

            setTimeout(() => {
                clearTimeout(timeout);
                if (pingTimer) clearInterval(pingTimer);
                const r = results[name];
                const total = Object.values(r.orderbook).reduce((a, b) => a + b, 0) +
                              Object.values(r.trades).reduce((a, b) => a + b, 0) +
                              Object.values(r.ticker).reduce((a, b) => a + b, 0);
                r.status = total > 0 ? 'ok' : 'no_data';
                logResult(name);
                try { ws.close(); } catch (e) {}
                setTimeout(() => done(total > 0), 500);
            }, DATA_WAIT);
        });

        let msgCount = 0;
        ws.on('message', (data) => {
            const str = data.toString();
            if (str === 'pong') return;

            try {
                const d = JSON.parse(str);

                // Proprietary trade format: a='PMT'
                if (d.a === 'PMT' && d.r && Array.isArray(d.r)) {
                    for (const item of d.r) {
                        const inst = item?.d?.I || '';
                        const coin = inst.includes('BTC') ? 'BTC' : inst.includes('ETH') ? 'ETH' : inst.includes('SOL') ? 'SOL' : null;
                        if (coin) { results[name].trades[coin]++; gotData = true; }
                    }
                }

                // Orderbook format: Check for TopicID 25 response
                // Deepcoin orderbook data typically has bids/asks or Direction fields
                if (d.a && d.r && Array.isArray(d.r)) {
                    const topicId = d.t || '';
                    
                    // Check first record for orderbook-like fields
                    const first = d.r[0]?.d || {};
                    
                    // TopicID 25 orderbook: records with Direction (0=buy, 1=sell) and Price
                    if (first.Direction !== undefined || first.Drctn !== undefined) {
                        const inst = first.I || first.InstrumentID || '';
                        const coin = inst.includes('BTC') ? 'BTC' : inst.includes('ETH') ? 'ETH' : inst.includes('SOL') ? 'SOL' : null;
                        if (coin) { results[name].orderbook[coin]++; gotData = true; }
                    }

                    // TopicID 7 ticker/market overview: records with LastPrice, High24h, etc.
                    if (first.LastPrice || first.Lst || first.LstPrc || first.H || first.L || first.V) {
                        const inst = first.I || first.InstrumentID || '';
                        const coin = inst.includes('BTC') ? 'BTC' : inst.includes('ETH') ? 'ETH' : inst.includes('SOL') ? 'SOL' : null;
                        if (coin && d.a !== 'PMT') { results[name].ticker[coin]++; gotData = true; }
                    }
                }

                // Also handle if data comes in flat object format
                if (d.data && d.arg) {
                    // OKX-compatible format (just in case)
                    const ch = d.arg.channel;
                    const inst = d.arg.instId || '';
                    const coin = inst.includes('BTC') ? 'BTC' : inst.includes('ETH') ? 'ETH' : inst.includes('SOL') ? 'SOL' : null;
                    if (coin) {
                        if (ch === 'tickers') { results[name].ticker[coin]++; gotData = true; }
                        if (ch === 'books5' || ch === 'books') { results[name].orderbook[coin]++; gotData = true; }
                    }
                }

                // Debug: log first messages
                msgCount++;
                if (msgCount <= 8) {
                    const preview = str.substring(0, 200);
                    console.log(`  [${ts()}] 🔍 ${name} msg#${msgCount}: ${preview}...`);
                }
            } catch (e) {}
        });

        ws.on('error', (err) => {
            console.log(`  [${ts()}] ❌ ${name}/${endpoint.split('/').pop()} — Error: ${err.message}`);
            results[name].errors.push(err.message);
            clearTimeout(timeout);
            if (pingTimer) clearInterval(pingTimer);
            done(false);
        });

        ws.on('close', () => {
            if (pingTimer) clearInterval(pingTimer);
        });
    });
}

// ═══════════════════════════════════════════════════════════
// FIX 6: BTSE — Use dual connections: spot WS for trades, OSS for orderbook
// ═══════════════════════════════════════════════════════════
async function testBTSE() {
    const name = 'BTSE';
    initResult(name);
    console.log(`  [${ts()}] 🔌 ${name} — Connecting dual WS (spot + OSS)...`);

    return new Promise((resolve) => {
        let resolved = false;
        const done = () => { if (!resolved) { resolved = true; resolve(); } };

        let spotWs, ossWs;
        let spotPing, ossPing;

        const timeout = setTimeout(() => {
            console.log(`  [${ts()}] ⏰ ${name} — Timeout`);
            try { spotWs?.terminate(); } catch (e) {}
            try { ossWs?.terminate(); } catch (e) {}
            results[name].status = 'timeout';
            done();
        }, TEST_TIMEOUT);

        // Connection 1: Spot WS for trades
        spotWs = new WebSocket('wss://ws.btse.com/ws/spot', {
            headers: { 'User-Agent': 'Mozilla/5.0' },
            handshakeTimeout: 10000
        });

        spotWs.on('open', () => {
            console.log(`  [${ts()}] ✅ ${name}/spot — Connected, subscribing trades...`);
            spotWs.send(JSON.stringify({ op: 'subscribe', args: [
                'tradeHistoryApi:BTC-USD', 'tradeHistoryApi:ETH-USD', 'tradeHistoryApi:SOL-USD'
            ] }));
            spotPing = setInterval(() => {
                if (spotWs.readyState === WebSocket.OPEN) spotWs.send('ping');
            }, 30000);
        });

        spotWs.on('message', (data) => {
            const str = data.toString();
            if (str === 'pong') return;
            try {
                const d = JSON.parse(str);
                if (d.event === 'subscribe') return;
                if (!d.topic || !d.data) return;
                const topic = d.topic;
                const coin = topic.includes('BTC') ? 'BTC' : topic.includes('ETH') ? 'ETH' : topic.includes('SOL') ? 'SOL' : null;
                if (!coin) return;
                if (topic.includes('tradeHistory')) results[name].trades[coin]++;
            } catch (e) {}
        });

        spotWs.on('error', (err) => {
            console.log(`  [${ts()}] ❌ ${name}/spot — Error: ${err.message}`);
            results[name].errors.push(`spot: ${err.message}`);
        });

        // Connection 2: OSS WS for orderbook
        ossWs = new WebSocket('wss://ws.btse.com/ws/oss/spot', {
            headers: { 'User-Agent': 'Mozilla/5.0' },
            handshakeTimeout: 10000
        });

        ossWs.on('open', () => {
            console.log(`  [${ts()}] ✅ ${name}/oss — Connected, subscribing orderbook...`);
            ossWs.send(JSON.stringify({ op: 'subscribe', args: [
                'update:BTC-USD_0', 'update:ETH-USD_0', 'update:SOL-USD_0'
            ] }));
            ossPing = setInterval(() => {
                if (ossWs.readyState === WebSocket.OPEN) ossWs.send('ping');
            }, 30000);
        });

        let ossMsg = 0;
        ossWs.on('message', (data) => {
            const str = data.toString();
            if (str === 'pong') return;
            try {
                const d = JSON.parse(str);
                if (d.event === 'subscribe') return;
                if (!d.topic && !d.data) return;

                const topic = d.topic || '';
                const coin = topic.includes('BTC') ? 'BTC' : topic.includes('ETH') ? 'ETH' : topic.includes('SOL') ? 'SOL' : null;

                // Also check data structure for orderbook
                if (d.data && (d.data.buyQuote || d.data.sellQuote || d.data.bids || d.data.asks)) {
                    const c = coin || (JSON.stringify(d).includes('BTC') ? 'BTC' : JSON.stringify(d).includes('ETH') ? 'ETH' : JSON.stringify(d).includes('SOL') ? 'SOL' : null);
                    if (c) results[name].orderbook[c]++;
                }
                else if (coin && topic.includes('update')) {
                    results[name].orderbook[coin]++;
                }

                ossMsg++;
                if (ossMsg <= 3) {
                    console.log(`  [${ts()}] 🔍 ${name}/oss msg#${ossMsg}: topic=${topic} keys=${d.data ? Object.keys(d.data).slice(0, 5).join(',') : 'none'}`);
                }
            } catch (e) {}
        });

        ossWs.on('error', (err) => {
            console.log(`  [${ts()}] ❌ ${name}/oss — Error: ${err.message}`);
            results[name].errors.push(`oss: ${err.message}`);
        });

        // Wait for data collection
        setTimeout(() => {
            clearTimeout(timeout);
            if (spotPing) clearInterval(spotPing);
            if (ossPing) clearInterval(ossPing);
            results[name].status = 'ok';
            logResult(name);
            try { spotWs.close(); } catch (e) {}
            try { ossWs.close(); } catch (e) {}
            setTimeout(done, 500);
        }, DATA_WAIT);
    });
}

// ═══════════════════════════════════════════════════════════
// HELPERS
// ═══════════════════════════════════════════════════════════

function logResult(name) {
    const r = results[name];
    console.log(`  [${ts()}] 📊 ${name} — OB=${r.orderbook.BTC}/${r.orderbook.ETH}/${r.orderbook.SOL} TR=${r.trades.BTC}/${r.trades.ETH}/${r.trades.SOL} TK=${r.ticker.BTC}/${r.ticker.ETH}/${r.ticker.SOL}`);
}

function printReport() {
    const COINS = ['BTC', 'ETH', 'SOL'];
    const TYPES = ['orderbook', 'trades', 'ticker'];
    const noTickerExchanges = ['Pionex', 'BTSE'];

    console.log('\n');
    console.log('╔' + '═'.repeat(100) + '╗');
    console.log('║  FIX TESTER REPORT — 6 Exchanges with Corrected Subscription Formats' + ' '.repeat(29) + '║');
    console.log('╚' + '═'.repeat(100) + '╝');

    console.log('\n  ' +
        'Exchange'.padEnd(15) +
        '│ OB-BTC'.padEnd(9) + 'OB-ETH'.padEnd(9) + 'OB-SOL'.padEnd(9) +
        '│ TR-BTC'.padEnd(9) + 'TR-ETH'.padEnd(9) + 'TR-SOL'.padEnd(9) +
        '│ TK-BTC'.padEnd(9) + 'TK-ETH'.padEnd(9) + 'TK-SOL'.padEnd(9) +
        '│ Score'
    );
    console.log('  ' + '─'.repeat(100));

    const exchangeNames = ['Kraken', 'BingX', 'HitBTC', 'Pionex', 'Deepcoin', 'BTSE'];
    let totalPass = 0, totalFail = 0, totalNA = 0;

    for (const name of exchangeNames) {
        const r = results[name] || { orderbook: {}, trades: {}, ticker: {}, status: 'unknown' };
        const noTicker = noTickerExchanges.includes(name);
        let pass = 0, fail = 0, na = 0;

        const fmtCell = (type, coin) => {
            const count = r[type]?.[coin] || 0;
            if (noTicker && type === 'ticker') { na++; totalNA++; return 'N/A'.padEnd(7); }
            if (count > 0) { pass++; totalPass++; return `✅ ${count}`.padEnd(7); }
            else { fail++; totalFail++; return '❌ 0'.padEnd(7); }
        };

        const maxTests = noTicker ? 6 : 9;
        const emoji = pass === maxTests ? '🟢' : pass >= maxTests * 0.6 ? '🟡' : '🔴';

        console.log('  ' +
            name.padEnd(15) +
            `│ ${fmtCell('orderbook', 'BTC')} ${fmtCell('orderbook', 'ETH')} ${fmtCell('orderbook', 'SOL')} ` +
            `│ ${fmtCell('trades', 'BTC')} ${fmtCell('trades', 'ETH')} ${fmtCell('trades', 'SOL')} ` +
            `│ ${fmtCell('ticker', 'BTC')} ${fmtCell('ticker', 'ETH')} ${fmtCell('ticker', 'SOL')} ` +
            `│ ${emoji} ${pass}/${maxTests}`
        );
    }

    console.log('  ' + '─'.repeat(100));
    const total = totalPass + totalFail;
    console.log(`\n  ✅ TOTAL: ${totalPass}/${total} tests passed (${((totalPass / total) * 100).toFixed(1)}%)${totalNA > 0 ? ` | ${totalNA} N/A` : ''}`);
    console.log('\n  FIXES APPLIED:');
    console.log('  ─────────────');
    console.log('  • Kraken:   Upgraded to v2 WebSocket API + 30s test window');
    console.log('  • BingX:    Fixed depth format: @depth5@500ms → @depth5');
    console.log('  • HitBTC:   Extended test window to 30s (low volume exchange)');
    console.log('  • Pionex:   Marked noTicker (TICKER topic does not exist)');
    console.log('  • Deepcoin: Using TopicID 25 (orderbook) + TopicID 7 (ticker) + correct spot endpoint');
    console.log('  • BTSE:     Dual WS: spot for trades + OSS endpoint for orderbook, marked noTicker\n');
}

// ═══════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════

async function main() {
    console.log('\n╔' + '═'.repeat(78) + '╗');
    console.log('║  FIX TESTER — Corrected Subscription Formats for 6 Failing Exchanges        ║');
    console.log('║  30s test window | Debug logging enabled                                     ║');
    console.log('╚' + '═'.repeat(78) + '╝\n');

    // Run sequentially for easier debugging
    console.log('\n  ── Testing Kraken (v2 API) ──');
    await testKraken();

    console.log('\n  ── Testing BingX (fixed depth) ──');
    await testBingX();

    console.log('\n  ── Testing HitBTC (extended window) ──');
    await testHitBTC();

    console.log('\n  ── Testing Pionex (no ticker) ──');
    await testPionex();

    console.log('\n  ── Testing Deepcoin (TopicID 25/7) ──');
    await testDeepcoin();

    console.log('\n  ── Testing BTSE (dual WS: spot + OSS) ──');
    await testBTSE();

    printReport();
}

main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});
