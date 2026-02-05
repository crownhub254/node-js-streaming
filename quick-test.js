const WebSocket = require('ws');
const https = require('https');
const zlib = require('zlib');

console.log('╔════════════════════════════════════════════════════════════════╗');
console.log('║     QUICK EXCHANGE STREAM TEST                                 ║');
console.log('╚════════════════════════════════════════════════════════════════╝\n');

const results = {};

function quickTest(name, url, subscribeMsg, parseMessage, onOpen) {
  return new Promise((resolve) => {
    console.log(`Testing ${name}...`);
    const ws = new WebSocket(url);
    let received = 0;
    const timeout = setTimeout(() => {
      ws.close();
      resolve({ success: received > 0, count: received });
    }, 8000);

    ws.on('open', () => {
      if (onOpen) {
        onOpen(ws);
      } else if (subscribeMsg) {
        ws.send(JSON.stringify(subscribeMsg));
      }
    });

    ws.on('message', (data) => {
      try {
        let msg;
        try {
          msg = JSON.parse(zlib.gunzipSync(data).toString());
        } catch {
          try {
            msg = JSON.parse(zlib.inflateRawSync(data).toString());
          } catch {
            msg = JSON.parse(data.toString());
          }
        }
        if (msg.ping) ws.send(JSON.stringify({ pong: msg.ping }));
        const parsed = parseMessage(msg);
        if (parsed) {
          received++;
          if (received === 1) {
            console.log(`  ✅ ${name}: ${JSON.stringify(parsed).substring(0, 60)}...`);
          }
        }
        if (received >= 3) {
          clearTimeout(timeout);
          ws.close();
          resolve({ success: true, count: received });
        }
      } catch (e) {}
    });

    ws.on('error', (e) => {
      clearTimeout(timeout);
      console.log(`  ❌ ${name}: ${e.message}`);
      resolve({ success: false, error: e.message });
    });
  });
}

async function getKuCoinToken() {
  return new Promise((resolve) => {
    const req = https.request('https://api.kucoin.com/api/v1/bullet-public', { method: 'POST' }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          resolve({ endpoint: json.data.instanceServers[0].endpoint, token: json.data.token });
        } catch { resolve(null); }
      });
    });
    req.on('error', () => resolve(null));
    req.end();
  });
}

async function run() {
  // 1. BINGX
  console.log('\n━━━ BingX ━━━');
  await quickTest('bingx_spot_ticker', 'wss://open-api-ws.bingx.com/market',
    { id: '1', reqType: 'sub', dataType: 'BTC-USDT@ticker' },
    m => m.data?.p ? { ticker: m.data.p } : null);
  await quickTest('bingx_futures_kline', 'wss://open-api-swap.bingx.com/swap-market',
    { id: '1', reqType: 'sub', dataType: 'BTC-USDT@kline_1m' },
    m => m.data ? { kline: 'received' } : null);

  // 2. BITMEX
  console.log('\n━━━ BitMEX ━━━');
  await quickTest('bitmex_kline', 'wss://ws.bitmex.com/realtime',
    { op: 'subscribe', args: ['tradeBin1m:XBTUSD'] },
    m => m.table === 'tradeBin1m' ? { kline: m.data } : null);

  // 3. MEXC
  console.log('\n━━━ MEXC ━━━');
  await quickTest('mexc_spot_depth', 'wss://wbs.mexc.com/ws',
    { method: 'SUBSCRIPTION', params: ['spot@public.limit.depth.v3.api@BTCUSDT@5'] },
    m => m.d?.bids ? { orderbook: m.d.bids.length } : null);
  await quickTest('mexc_spot_trades', 'wss://wbs.mexc.com/ws',
    { method: 'SUBSCRIPTION', params: ['spot@public.deals.v3.api@BTCUSDT'] },
    m => m.d?.deals ? { trades: m.d.deals.length } : null);
  await quickTest('mexc_futures_kline', 'wss://contract.mexc.com/edge',
    { method: 'sub.kline', param: { symbol: 'BTC_USDT', interval: 'Min1' } },
    m => m.channel === 'push.kline' ? { kline: 'ok' } : null);

  // 4. COINBASE
  console.log('\n━━━ Coinbase ━━━');
  await quickTest('coinbase_matches', 'wss://ws-feed.exchange.coinbase.com',
    { type: 'subscribe', product_ids: ['BTC-USD'], channels: ['matches'] },
    m => m.type === 'match' ? { trade: m.price } : null);

  // 5. BITSTAMP
  console.log('\n━━━ Bitstamp ━━━');
  await quickTest('bitstamp_trades', 'wss://ws.bitstamp.net',
    { event: 'bts:subscribe', data: { channel: 'live_trades_btcusd' } },
    m => m.event === 'trade' ? { trade: m.data?.price } : null);

  // 6. BITMART  
  console.log('\n━━━ BitMart ━━━');
  await quickTest('bitmart_ticker', 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1',
    { op: 'subscribe', args: ['spot/ticker:BTC_USDT'] },
    m => m.table === 'spot/ticker' ? { ticker: m.data } : null);

  // 7. KUCOIN
  console.log('\n━━━ KuCoin ━━━');
  const kucoinToken = await getKuCoinToken();
  if (kucoinToken) {
    console.log('  Got token ✅');
    const kucoinUrl = `${kucoinToken.endpoint}?token=${kucoinToken.token}`;
    await quickTest('kucoin_ticker', kucoinUrl,
      { id: Date.now(), type: 'subscribe', topic: '/market/ticker:BTC-USDT', privateChannel: false, response: true },
      m => m.topic?.includes('ticker') ? { ticker: m.data } : null);
    await quickTest('kucoin_orderbook', kucoinUrl,
      { id: Date.now(), type: 'subscribe', topic: '/market/level2Depth5:BTC-USDT', privateChannel: false, response: true },
      m => m.topic?.includes('level2') ? { orderbook: m.data } : null);
    await quickTest('kucoin_trades', kucoinUrl,
      { id: Date.now(), type: 'subscribe', topic: '/market/match:BTC-USDT', privateChannel: false, response: true },
      m => m.topic?.includes('match') ? { trade: m.data } : null);
  } else {
    console.log('  ❌ Failed to get token');
  }

  // 8. UPBIT
  console.log('\n━━━ Upbit ━━━');
  await quickTest('upbit_ticker', 'wss://api.upbit.com/websocket/v1', null,
    m => m.type === 'ticker' ? { ticker: m.trade_price } : null,
    (ws) => ws.send(JSON.stringify([{ ticket: 'test' }, { type: 'ticker', codes: ['KRW-BTC'] }])));
  await quickTest('upbit_orderbook', 'wss://api.upbit.com/websocket/v1', null,
    m => m.type === 'orderbook' ? { orderbook: m.orderbook_units?.length } : null,
    (ws) => ws.send(JSON.stringify([{ ticket: 'test' }, { type: 'orderbook', codes: ['KRW-BTC'] }])));

  // 9. WEEX
  console.log('\n━━━ WEEX ━━━');
  await quickTest('weex_futures', 'wss://ws.weex.com/v2/ws/public',
    { op: 'subscribe', args: [{ instType: 'USDT-FUTURES', channel: 'ticker', instId: 'BTCUSDT' }] },
    m => m.data ? { ticker: m.data } : null);

  console.log('\n✅ Quick tests completed!');
}

run().catch(console.error);
