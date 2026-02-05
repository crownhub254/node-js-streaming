const WebSocket = require('ws');
const zlib = require('zlib');

// ============================================================
// DEEP RESEARCH TESTER - Corrected configs from API documentation
// Tests: Deepcoin, Phemex, WOO X (full), Toobit (futures)
// ============================================================

const TIMEOUT = 20000; // 20 seconds per stream
const results = {};

function log(exchange, msg) {
    console.log(`  [${exchange}] ${msg}`);
}

// Generic WebSocket test helper
function testWebSocket(name, url, options = {}) {
    return new Promise((resolve) => {
        const {
            onOpen,
            onMessage,
            pingInterval,
            pingPayload,
            timeout = TIMEOUT,
            headers = {}
        } = options;

        let received = false;
        let ws;
        let pingTimer;
        let timeoutTimer;

        try {
            ws = new WebSocket(url, { headers });
        } catch (e) {
            log(name, `Connection error: ${e.message}`);
            resolve({ success: false, error: e.message });
            return;
        }

        timeoutTimer = setTimeout(() => {
            if (!received) {
                log(name, `TIMEOUT - no data received`);
                cleanup();
                resolve({ success: false, error: 'timeout' });
            }
        }, timeout);

        function cleanup() {
            clearTimeout(timeoutTimer);
            if (pingTimer) clearInterval(pingTimer);
            try { ws.close(); } catch (e) {}
        }

        ws.on('open', () => {
            log(name, `Connected to ${url}`);
            if (pingInterval && pingPayload) {
                pingTimer = setInterval(() => {
                    try {
                        if (typeof pingPayload === 'string') {
                            ws.send(pingPayload);
                        } else {
                            ws.send(JSON.stringify(pingPayload));
                        }
                    } catch (e) {}
                }, pingInterval);
            }
            if (onOpen) onOpen(ws);
        });

        ws.on('message', (data) => {
            let msg;
            try {
                if (Buffer.isBuffer(data)) {
                    msg = data.toString('utf8');
                } else {
                    msg = data.toString();
                }
            } catch (e) {
                msg = data.toString();
            }

            // Handle custom message processing
            if (onMessage) {
                const result = onMessage(msg, ws);
                if (result === 'skip') return; // Skip pong/ping responses
                if (result === 'done') {
                    received = true;
                    cleanup();
                    resolve({ success: true, sample: msg.substring(0, 200) });
                    return;
                }
            }

            // Check if it's actual data (not just pong/subscription confirmation)
            try {
                const parsed = JSON.parse(msg);
                // Skip pong messages
                if (parsed.pong || parsed.event === 'pong' || parsed.op === 'pong' || msg === 'pong') return;
                // Skip subscription confirmations for some exchanges
                if (parsed.event === 'subscribe' && parsed.success === true) return;
                // Skip empty or error messages
                if (parsed.success === false || parsed.code < 0) {
                    log(name, `Error: ${msg.substring(0, 200)}`);
                    cleanup();
                    resolve({ success: false, error: msg.substring(0, 200) });
                    return;
                }
            } catch (e) {
                // Not JSON, check for text pong
                if (msg === 'pong' || msg === 'Pong') return;
            }

            if (!received) {
                received = true;
                log(name, `✅ Data received: ${msg.substring(0, 150)}...`);
                cleanup();
                resolve({ success: true, sample: msg.substring(0, 200) });
            }
        });

        ws.on('error', (err) => {
            log(name, `❌ Error: ${err.message}`);
            cleanup();
            resolve({ success: false, error: err.message });
        });

        ws.on('close', (code, reason) => {
            if (!received) {
                log(name, `Closed: ${code} ${reason}`);
                cleanup();
                resolve({ success: false, error: `closed: ${code}` });
            }
        });
    });
}

// ============================================================
// 1. DEEPCOIN - Full API docs found
// Endpoint: wss://stream.deepcoin.com/public/ws (futures)
// Subscription: SendTopicAction format with TopicIDs
// TopicID: 2=trades, 7=ticker, 11=klines, 25=orderbook
// FilterValue: DeepCoin_BTCUSDT
// Ping: text "ping"
// ============================================================
async function testDeepcoin() {
    console.log('\n📊 Testing DEEPCOIN (corrected from docs)...');
    const streamResults = {};

    // Test ticker (TopicID: 7)
    streamResults.ticker = await testWebSocket('Deepcoin-Ticker', 'wss://stream.deepcoin.com/public/ws', {
        pingInterval: 15000,
        pingPayload: 'ping',
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                SendTopicAction: {
                    Action: "1",
                    FilterValue: "DeepCoin_BTCUSDT",
                    LocalNo: 9,
                    ResumeNo: -2,
                    TopicID: "7"
                }
            }));
        },
        onMessage: (msg) => {
            if (msg === 'pong') return 'skip';
            try {
                const parsed = JSON.parse(msg);
                if (parsed.TopicID || parsed.data || parsed.FilterValue) return 'done';
            } catch (e) {}
            if (msg.length > 10 && msg !== 'pong') return 'done';
            return 'skip';
        }
    });

    // Test trades (TopicID: 2)
    streamResults.trades = await testWebSocket('Deepcoin-Trades', 'wss://stream.deepcoin.com/public/ws', {
        pingInterval: 15000,
        pingPayload: 'ping',
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                SendTopicAction: {
                    Action: "1",
                    FilterValue: "DeepCoin_BTCUSDT",
                    LocalNo: 9,
                    ResumeNo: -2,
                    TopicID: "2"
                }
            }));
        },
        onMessage: (msg) => {
            if (msg === 'pong') return 'skip';
            try {
                JSON.parse(msg);
                if (msg.length > 10) return 'done';
            } catch (e) {}
            if (msg.length > 10 && msg !== 'pong') return 'done';
            return 'skip';
        }
    });

    // Test orderbook (TopicID: 25)
    streamResults.orderbook = await testWebSocket('Deepcoin-Orderbook', 'wss://stream.deepcoin.com/public/ws', {
        pingInterval: 15000,
        pingPayload: 'ping',
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                SendTopicAction: {
                    Action: "1",
                    FilterValue: "DeepCoin_BTCUSDT",
                    LocalNo: 9,
                    ResumeNo: -2,
                    TopicID: "25"
                }
            }));
        },
        onMessage: (msg) => {
            if (msg === 'pong') return 'skip';
            if (msg.length > 10 && msg !== 'pong') return 'done';
            return 'skip';
        }
    });

    // Test klines (TopicID: 11)
    streamResults.klines = await testWebSocket('Deepcoin-Klines', 'wss://stream.deepcoin.com/public/ws', {
        pingInterval: 15000,
        pingPayload: 'ping',
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                SendTopicAction: {
                    Action: "1",
                    FilterValue: "DeepCoin_BTCUSDT_1m",
                    LocalNo: 9,
                    ResumeNo: -2,
                    TopicID: "11"
                }
            }));
        },
        onMessage: (msg) => {
            if (msg === 'pong') return 'skip';
            if (msg.length > 10 && msg !== 'pong') return 'done';
            return 'skip';
        }
    });

    // Also try spot endpoint
    streamResults.spot_ticker = await testWebSocket('Deepcoin-SpotTicker', 'wss://stream.deepcoin.com/public/spotws', {
        pingInterval: 15000,
        pingPayload: 'ping',
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                SendTopicAction: {
                    Action: "1",
                    FilterValue: "DeepCoin_BTCUSDT",
                    LocalNo: 9,
                    ResumeNo: -2,
                    TopicID: "7"
                }
            }));
        },
        onMessage: (msg) => {
            if (msg === 'pong') return 'skip';
            if (msg.length > 10 && msg !== 'pong') return 'done';
            return 'skip';
        }
    });

    results['Deepcoin'] = streamResults;
    const working = Object.values(streamResults).filter(r => r.success).length;
    console.log(`  Deepcoin: ${working}/${Object.keys(streamResults).length} streams working`);
}

// ============================================================
// 2. PHEMEX - Full API docs from GitHub
// Endpoints: wss://phemex.com/ws (standard), wss://vapi.phemex.com/ws (high rate)
// Subscribe: JSON-RPC style {"id":1234,"method":"xxx.subscribe","params":[...]}
// Hedged USDT: trade_p.subscribe, kline_p.subscribe with BTCUSDT
// Spot: use sBTCUSDT prefix
// ============================================================
async function testPhemex() {
    console.log('\n📊 Testing PHEMEX (corrected from docs)...');
    const streamResults = {};

    // Test trade on standard endpoint
    streamResults.trade = await testWebSocket('Phemex-Trade', 'wss://phemex.com/ws', {
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                id: 1234,
                method: "trade.subscribe",
                params: ["BTCUSD"]
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.id === 1234 && parsed.result && parsed.result.status === 'success') return 'skip';
                if (parsed.id === 1234 && parsed.error === null) return 'skip';
                if (parsed.trades || parsed.type === 'incremental') return 'done';
                if (parsed.trade) return 'done';
            } catch (e) {}
            return null;
        }
    });

    // Test hedged perpetual trade (USDT pairs)
    streamResults.trade_usdt = await testWebSocket('Phemex-TradeUSDT', 'wss://phemex.com/ws', {
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                id: 1235,
                method: "trade_p.subscribe",
                params: ["BTCUSDT"]
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.id === 1235 && parsed.error === null) return 'skip';
                if (parsed.trades || parsed.type === 'incremental' || parsed.trades_p) return 'done';
            } catch (e) {}
            return null;
        }
    });

    // Test 24h ticker
    streamResults.ticker = await testWebSocket('Phemex-Ticker', 'wss://phemex.com/ws', {
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                id: 1236,
                method: "market24h.subscribe",
                params: []
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.id === 1236 && parsed.error === null) return 'skip';
                if (parsed.market24h || parsed.data) return 'done';
            } catch (e) {}
            return null;
        }
    });

    // Test orderbook
    streamResults.orderbook = await testWebSocket('Phemex-Orderbook', 'wss://phemex.com/ws', {
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                id: 1237,
                method: "orderbook.subscribe",
                params: ["BTCUSD"]
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.id === 1237 && parsed.error === null) return 'skip';
                if (parsed.book || parsed.orderbook || parsed.orderbook_p) return 'done';
            } catch (e) {}
            return null;
        }
    });

    // Test kline
    streamResults.kline = await testWebSocket('Phemex-Kline', 'wss://phemex.com/ws', {
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                id: 1238,
                method: "kline.subscribe",
                params: ["BTCUSD", 60]
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.id === 1238 && parsed.error === null) return 'skip';
                if (parsed.kline || parsed.kline_p) return 'done';
            } catch (e) {}
            return null;
        }
    });

    // Test spot trade
    streamResults.spot_trade = await testWebSocket('Phemex-SpotTrade', 'wss://phemex.com/ws', {
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                id: 1239,
                method: "trade.subscribe",
                params: ["sBTCUSDT"]
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.id === 1239 && parsed.error === null) return 'skip';
                if (parsed.trades || parsed.type === 'incremental') return 'done';
            } catch (e) {}
            return null;
        }
    });

    results['Phemex'] = streamResults;
    const working = Object.values(streamResults).filter(r => r.success).length;
    console.log(`  Phemex: ${working}/${Object.keys(streamResults).length} streams working`);
}

// ============================================================
// 3. WOO X - Full API docs from kronosresearch.github.io
// Public WS: wss://wss.woo.org/ws/stream/{application_id}
// For public streams, use any app_id (e.g., "public")
// Topics: {symbol}@orderbook, @trade, @ticker, @kline_1m, @bbo
// PERP topics: @openinterest, @markprice, @estfundingrate
// Symbol: SPOT_BTC_USDT, PERP_BTC_USDT
// Subscribe: {"id":"xxx","event":"subscribe","topic":"SPOT_BTC_USDT@trade"}
// Ping: {"event":"ping"}
// ============================================================
async function testWooX() {
    console.log('\n📊 Testing WOO X (corrected from docs)...');
    const streamResults = {};

    const wsUrl = 'wss://wss.woo.org/ws/stream/OqdphuyCtYWxwzhxyLLjOWNdFP7sQt8RPWzmb5xY';

    // Test trade
    streamResults.trade = await testWebSocket('WooX-Trade', wsUrl, {
        pingInterval: 10000,
        pingPayload: { event: 'ping' },
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                id: 'trade1',
                event: 'subscribe',
                topic: 'SPOT_BTC_USDT@trade'
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.event === 'pong') return 'skip';
                if (parsed.event === 'subscribe' && parsed.success) return 'skip';
                if (parsed.topic && parsed.topic.includes('@trade')) return 'done';
            } catch (e) {}
            return null;
        }
    });

    // Test ticker
    streamResults.ticker = await testWebSocket('WooX-Ticker', wsUrl, {
        pingInterval: 10000,
        pingPayload: { event: 'ping' },
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                id: 'ticker1',
                event: 'subscribe',
                topic: 'SPOT_BTC_USDT@ticker'
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.event === 'pong') return 'skip';
                if (parsed.event === 'subscribe' && parsed.success) return 'skip';
                if (parsed.topic && parsed.topic.includes('@ticker')) return 'done';
            } catch (e) {}
            return null;
        }
    });

    // Test orderbook
    streamResults.orderbook = await testWebSocket('WooX-Orderbook', wsUrl, {
        pingInterval: 10000,
        pingPayload: { event: 'ping' },
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                id: 'ob1',
                event: 'subscribe',
                topic: 'SPOT_BTC_USDT@orderbook'
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.event === 'pong') return 'skip';
                if (parsed.event === 'subscribe' && parsed.success) return 'skip';
                if (parsed.topic && parsed.topic.includes('@orderbook')) return 'done';
            } catch (e) {}
            return null;
        }
    });

    // Test kline
    streamResults.kline = await testWebSocket('WooX-Kline', wsUrl, {
        pingInterval: 10000,
        pingPayload: { event: 'ping' },
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                id: 'kline1',
                event: 'subscribe',
                topic: 'SPOT_BTC_USDT@kline_1m'
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.event === 'pong') return 'skip';
                if (parsed.event === 'subscribe' && parsed.success) return 'skip';
                if (parsed.topic && parsed.topic.includes('@kline')) return 'done';
            } catch (e) {}
            return null;
        }
    });

    // Test BBO (best bid/offer)
    streamResults.bbo = await testWebSocket('WooX-BBO', wsUrl, {
        pingInterval: 10000,
        pingPayload: { event: 'ping' },
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                id: 'bbo1',
                event: 'subscribe',
                topic: 'SPOT_BTC_USDT@bbo'
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.event === 'pong') return 'skip';
                if (parsed.event === 'subscribe' && parsed.success) return 'skip';
                if (parsed.topic && parsed.topic.includes('@bbo')) return 'done';
            } catch (e) {}
            return null;
        }
    });

    // Test PERP trade
    streamResults.perp_trade = await testWebSocket('WooX-PerpTrade', wsUrl, {
        pingInterval: 10000,
        pingPayload: { event: 'ping' },
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                id: 'ptrade1',
                event: 'subscribe',
                topic: 'PERP_BTC_USDT@trade'
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.event === 'pong') return 'skip';
                if (parsed.event === 'subscribe' && parsed.success) return 'skip';
                if (parsed.topic && parsed.topic.includes('@trade')) return 'done';
            } catch (e) {}
            return null;
        }
    });

    // Test PERP open interest
    streamResults.openinterest = await testWebSocket('WooX-OpenInterest', wsUrl, {
        pingInterval: 10000,
        pingPayload: { event: 'ping' },
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                id: 'oi1',
                event: 'subscribe',
                topic: 'PERP_BTC_USDT@openinterest'
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.event === 'pong') return 'skip';
                if (parsed.event === 'subscribe' && parsed.success) return 'skip';
                if (parsed.topic && parsed.topic.includes('@openinterest')) return 'done';
            } catch (e) {}
            return null;
        }
    });

    // Test mark price
    streamResults.markprice = await testWebSocket('WooX-MarkPrice', wsUrl, {
        pingInterval: 10000,
        pingPayload: { event: 'ping' },
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                id: 'mp1',
                event: 'subscribe',
                topic: 'PERP_BTC_USDT@markprice'
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.event === 'pong') return 'skip';
                if (parsed.event === 'subscribe' && parsed.success) return 'skip';
                if (parsed.topic && parsed.topic.includes('@markprice')) return 'done';
            } catch (e) {}
            return null;
        }
    });

    results['WooX'] = streamResults;
    const working = Object.values(streamResults).filter(r => r.success).length;
    console.log(`  WOO X: ${working}/${Object.keys(streamResults).length} streams working`);
}

// ============================================================
// 4. TOOBIT FUTURES - Full API docs found
// WS endpoint: wss://stream.toobit.com/quote/ws/v1 (same for spot & futures)
// Subscribe: {"symbol":"BTC-SWAP-USDT","topic":"trade","event":"sub","params":{"binary":false}}
// Contract symbols: BTC-SWAP-USDT (USDT-margined), BTC-SWAP (coin-margined)
// Spot symbols: BTCUSDT
// Topics: trade, realtimes, kline_1m, depth, diffDepth, markPrice, index
// Ping: {"ping": timestamp}
// ============================================================
async function testToobitFutures() {
    console.log('\n📊 Testing TOOBIT FUTURES (corrected from docs)...');
    const streamResults = {};

    const wsUrl = 'wss://stream.toobit.com/quote/ws/v1';
    const contractSymbol = 'BTC-SWAP-USDT';

    // Test futures trade
    streamResults.trade = await testWebSocket('Toobit-FuturesTrade', wsUrl, {
        pingInterval: 30000,
        pingPayload: { ping: Date.now() },
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                symbol: contractSymbol,
                topic: 'trade',
                event: 'sub',
                params: { binary: false }
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.pong) return 'skip';
                if (parsed.topic === 'trade' && parsed.data) return 'done';
                if (parsed.data && parsed.symbol) return 'done';
            } catch (e) {}
            return null;
        }
    });

    // Test futures ticker (realtimes)
    streamResults.ticker = await testWebSocket('Toobit-FuturesTicker', wsUrl, {
        pingInterval: 30000,
        pingPayload: { ping: Date.now() },
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                symbol: contractSymbol,
                topic: 'realtimes',
                event: 'sub',
                params: { binary: false }
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.pong) return 'skip';
                if (parsed.topic === 'realtimes' && parsed.data) return 'done';
                if (parsed.data && parsed.symbol) return 'done';
            } catch (e) {}
            return null;
        }
    });

    // Test futures kline
    streamResults.kline = await testWebSocket('Toobit-FuturesKline', wsUrl, {
        pingInterval: 30000,
        pingPayload: { ping: Date.now() },
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                symbol: contractSymbol,
                topic: 'kline_1m',
                event: 'sub',
                params: { binary: false }
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.pong) return 'skip';
                if (parsed.topic === 'kline' && parsed.data) return 'done';
                if (parsed.data && parsed.symbol) return 'done';
            } catch (e) {}
            return null;
        }
    });

    // Test futures depth
    streamResults.orderbook = await testWebSocket('Toobit-FuturesDepth', wsUrl, {
        pingInterval: 30000,
        pingPayload: { ping: Date.now() },
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                symbol: contractSymbol,
                topic: 'depth',
                event: 'sub',
                params: { binary: false }
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.pong) return 'skip';
                if (parsed.topic === 'depth' && parsed.data) return 'done';
                if (parsed.data && parsed.symbol) return 'done';
            } catch (e) {}
            return null;
        }
    });

    // Test mark price
    streamResults.markPrice = await testWebSocket('Toobit-MarkPrice', wsUrl, {
        pingInterval: 30000,
        pingPayload: { ping: Date.now() },
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                symbol: contractSymbol,
                topic: 'markPrice',
                event: 'sub'
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.pong) return 'skip';
                if (parsed.topic === 'markPrice' && parsed.data) return 'done';
                if (parsed.data && parsed.symbol) return 'done';
            } catch (e) {}
            return null;
        }
    });

    // Test index price
    streamResults.indexPrice = await testWebSocket('Toobit-IndexPrice', wsUrl, {
        pingInterval: 30000,
        pingPayload: { ping: Date.now() },
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                id: 'index',
                topic: 'index',
                event: 'sub',
                symbol: 'BTCUSDT',
                params: { binary: false }
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.pong) return 'skip';
                if (parsed.topic === 'index' && parsed.data) return 'done';
                if (parsed.data && parsed.symbol) return 'done';
            } catch (e) {}
            return null;
        }
    });

    // Also verify spot still works
    streamResults.spot_trade = await testWebSocket('Toobit-SpotTrade', wsUrl, {
        pingInterval: 30000,
        pingPayload: { ping: Date.now() },
        onOpen: (ws) => {
            ws.send(JSON.stringify({
                symbol: 'BTCUSDT',
                topic: 'trade',
                event: 'sub',
                params: { binary: false }
            }));
        },
        onMessage: (msg) => {
            try {
                const parsed = JSON.parse(msg);
                if (parsed.pong) return 'skip';
                if (parsed.topic === 'trade' && parsed.data) return 'done';
                if (parsed.data && parsed.symbol) return 'done';
            } catch (e) {}
            return null;
        }
    });

    results['ToobitFutures'] = streamResults;
    const working = Object.values(streamResults).filter(r => r.success).length;
    console.log(`  Toobit Futures: ${working}/${Object.keys(streamResults).length} streams working`);
}

// ============================================================
// MAIN EXECUTION
// ============================================================
async function main() {
    console.log('╔══════════════════════════════════════════════════════════╗');
    console.log('║  DEEP RESEARCH EXCHANGE TESTER                         ║');
    console.log('║  Testing corrected configs from official API docs       ║');
    console.log('║  Exchanges: Deepcoin, Phemex, WOO X, Toobit Futures    ║');
    console.log('╚══════════════════════════════════════════════════════════╝');

    await testDeepcoin();
    await testPhemex();
    await testWooX();
    await testToobitFutures();

    // Print summary
    console.log('\n' + '='.repeat(60));
    console.log('DEEP RESEARCH TEST RESULTS SUMMARY');
    console.log('='.repeat(60));

    let totalWorking = 0;
    let totalStreams = 0;

    for (const [exchange, streams] of Object.entries(results)) {
        const working = Object.values(streams).filter(r => r.success).length;
        const total = Object.keys(streams).length;
        totalWorking += working;
        totalStreams += total;

        const status = working === total ? '✅ FULL' :
                       working > 0 ? '🟡 PARTIAL' : '❌ FAILED';

        console.log(`\n${status} ${exchange}: ${working}/${total} streams`);
        for (const [stream, result] of Object.entries(streams)) {
            const icon = result.success ? '  ✅' : '  ❌';
            const detail = result.success ? '' : ` (${result.error || 'unknown'})`;
            console.log(`${icon} ${stream}${detail}`);
        }
    }

    console.log(`\n${'='.repeat(60)}`);
    console.log(`TOTAL: ${totalWorking}/${totalStreams} streams working across ${Object.keys(results).length} exchanges`);
    console.log('='.repeat(60));

    // Save results
    const fs = require('fs');
    fs.writeFileSync('deep-research-results.json', JSON.stringify(results, null, 2));
    console.log('\nResults saved to deep-research-results.json');

    process.exit(0);
}

main().catch(console.error);
