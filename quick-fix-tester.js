const WebSocket = require('ws');

// ============================================================
// QUICK FIX TESTER
// 1. WOO X - verify actual data (not just pings)
// 2. Phemex - try testnet endpoint
// ============================================================

const TIMEOUT = 25000;

function log(name, msg) {
    console.log(`  [${name}] ${msg}`);
}

// ============================================================
// WOO X - Verify actual market data arrives (filter out pings)
// ============================================================
async function testWooXRealData() {
    console.log('\n📊 WOO X - Verifying actual market data (not just pings)...');
    const results = {};

    const wsUrl = 'wss://wss.woo.org/ws/stream/OqdphuyCtYWxwzhxyLLjOWNdFP7sQt8RPWzmb5xY';

    const topics = [
        { name: 'spot_trade', topic: 'SPOT_BTC_USDT@trade' },
        { name: 'spot_ticker', topic: 'SPOT_BTC_USDT@ticker' },
        { name: 'spot_orderbook', topic: 'SPOT_BTC_USDT@orderbook' },
        { name: 'spot_kline', topic: 'SPOT_BTC_USDT@kline_1m' },
        { name: 'spot_bbo', topic: 'SPOT_BTC_USDT@bbo' },
        { name: 'perp_trade', topic: 'PERP_BTC_USDT@trade' },
        { name: 'perp_ticker', topic: 'PERP_BTC_USDT@ticker' },
        { name: 'perp_oi', topic: 'PERP_BTC_USDT@openinterest' },
        { name: 'perp_markprice', topic: 'PERP_BTC_USDT@markprice' },
    ];

    for (const { name, topic } of topics) {
        results[name] = await new Promise((resolve) => {
            let received = false;
            let ws;
            let pingTimer;

            const timeoutTimer = setTimeout(() => {
                if (!received) {
                    log(name, 'TIMEOUT - no market data received');
                    cleanup();
                    resolve({ success: false, error: 'timeout' });
                }
            }, TIMEOUT);

            function cleanup() {
                clearTimeout(timeoutTimer);
                if (pingTimer) clearInterval(pingTimer);
                try { ws.close(); } catch (e) {}
            }

            ws = new WebSocket(wsUrl);

            ws.on('open', () => {
                log(name, 'Connected');
                // Send ping immediately and on interval
                pingTimer = setInterval(() => {
                    try { ws.send(JSON.stringify({ event: 'ping' })); } catch (e) {}
                }, 9000);

                ws.send(JSON.stringify({
                    id: name,
                    event: 'subscribe',
                    topic: topic
                }));
            });

            ws.on('message', (data) => {
                const msg = data.toString();
                try {
                    const parsed = JSON.parse(msg);
                    
                    // Skip pings/pongs
                    if (parsed.event === 'ping' || parsed.event === 'pong') return;
                    // Skip subscription confirmations
                    if (parsed.event === 'subscribe') return;
                    // Skip error messages
                    if (parsed.event === 'error') {
                        log(name, `Error: ${msg.substring(0, 150)}`);
                        cleanup();
                        resolve({ success: false, error: msg.substring(0, 150) });
                        return;
                    }
                    
                    // This is actual market data if it has a topic field with data
                    if (parsed.topic && parsed.data) {
                        if (!received) {
                            received = true;
                            log(name, `✅ Market data: ${msg.substring(0, 150)}...`);
                            cleanup();
                            resolve({ success: true, sample: msg.substring(0, 200) });
                        }
                    }
                } catch (e) {}
            });

            ws.on('error', (err) => {
                log(name, `❌ Error: ${err.message}`);
                cleanup();
                resolve({ success: false, error: err.message });
            });

            ws.on('close', () => {
                if (!received) {
                    cleanup();
                    resolve({ success: false, error: 'closed' });
                }
            });
        });
    }

    const working = Object.values(results).filter(r => r.success).length;
    console.log(`\n  WOO X: ${working}/${Object.keys(results).length} streams with actual market data`);
    return results;
}

// ============================================================
// PHEMEX - Try testnet and alternate endpoints
// ============================================================
async function testPhemex() {
    console.log('\n📊 PHEMEX - Testing multiple endpoints...');
    const results = {};

    const endpoints = [
        { name: 'mainnet', url: 'wss://phemex.com/ws' },
        { name: 'testnet', url: 'wss://testnet.phemex.com/ws' },
        { name: 'vapi', url: 'wss://vapi.phemex.com/ws' },
    ];

    for (const { name: epName, url } of endpoints) {
        results[epName] = await new Promise((resolve) => {
            let received = false;
            let ws;

            const timeoutTimer = setTimeout(() => {
                if (!received) {
                    log(`Phemex-${epName}`, 'TIMEOUT');
                    cleanup();
                    resolve({ success: false, error: 'timeout' });
                }
            }, 15000);

            function cleanup() {
                clearTimeout(timeoutTimer);
                try { ws.close(); } catch (e) {}
            }

            try {
                ws = new WebSocket(url);
            } catch (e) {
                log(`Phemex-${epName}`, `Connection error: ${e.message}`);
                clearTimeout(timeoutTimer);
                resolve({ success: false, error: e.message });
                return;
            }

            ws.on('open', () => {
                log(`Phemex-${epName}`, `Connected to ${url}`);
                // Subscribe to trade
                ws.send(JSON.stringify({
                    id: 1234,
                    method: 'trade.subscribe',
                    params: ['BTCUSD']
                }));
                // Also subscribe to ticker
                ws.send(JSON.stringify({
                    id: 1235,
                    method: 'market24h.subscribe',
                    params: []
                }));
            });

            ws.on('message', (data) => {
                const msg = data.toString();
                try {
                    const parsed = JSON.parse(msg);
                    // Skip subscription acks
                    if (parsed.result && parsed.result.status === 'success') return;
                    if (parsed.error === null && parsed.id) return;
                    // Check for actual data
                    if (parsed.trades || parsed.market24h || parsed.book || parsed.kline) {
                        if (!received) {
                            received = true;
                            log(`Phemex-${epName}`, `✅ Data: ${msg.substring(0, 150)}...`);
                            cleanup();
                            resolve({ success: true, sample: msg.substring(0, 200) });
                        }
                    }
                } catch (e) {}
            });

            ws.on('error', (err) => {
                log(`Phemex-${epName}`, `❌ Error: ${err.message}`);
                cleanup();
                resolve({ success: false, error: err.message });
            });

            ws.on('close', (code) => {
                if (!received) {
                    log(`Phemex-${epName}`, `Closed: ${code}`);
                    cleanup();
                    resolve({ success: false, error: `closed: ${code}` });
                }
            });
        });
    }

    const working = Object.values(results).filter(r => r.success).length;
    console.log(`\n  Phemex: ${working}/${Object.keys(results).length} endpoints working`);
    return results;
}

async function main() {
    console.log('╔══════════════════════════════════════════════════════════╗');
    console.log('║  QUICK FIX TESTER - WOO X real data + Phemex endpoints  ║');
    console.log('╚══════════════════════════════════════════════════════════╝');

    const wooResults = await testWooXRealData();
    const phemexResults = await testPhemex();

    console.log('\n' + '='.repeat(60));
    console.log('RESULTS SUMMARY');
    console.log('='.repeat(60));

    console.log('\nWOO X (actual market data):');
    for (const [stream, result] of Object.entries(wooResults)) {
        const icon = result.success ? '  ✅' : '  ❌';
        console.log(`${icon} ${stream}${result.success ? '' : ` (${result.error})`}`);
    }

    console.log('\nPhemex endpoints:');
    for (const [ep, result] of Object.entries(phemexResults)) {
        const icon = result.success ? '  ✅' : '  ❌';
        console.log(`${icon} ${ep}${result.success ? '' : ` (${result.error})`}`);
    }

    const wooWorking = Object.values(wooResults).filter(r => r.success).length;
    const phemexWorking = Object.values(phemexResults).filter(r => r.success).length;

    console.log(`\nWOO X: ${wooWorking}/9 streams verified`);
    console.log(`Phemex: ${phemexWorking}/3 endpoints accessible`);

    process.exit(0);
}

main().catch(console.error);
