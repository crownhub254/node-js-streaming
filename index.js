const WebSocket = require('ws');
const https = require('https');

// Binance Futures Public WebSocket URL
const WSS_URL = 'wss://fstream.binance.com/ws';

// REST API for open interest
const OPEN_INTEREST_API = 'https://fapi.binance.com/fapi/v1/openInterest';

// Symbols to subscribe to (Binance uses lowercase)
const SYMBOLS = [
  'solusdt',
  'btcusdt',
  'ethusdt',
];

// Generate subscription topics
const orderbookTopics = SYMBOLS.map(symbol => `${symbol}@depth10@100ms`);
const markPriceTopics = SYMBOLS.map(symbol => `${symbol}@markPrice@1s`); // Mark price with funding rate
const topics = [...orderbookTopics, ...markPriceTopics];

let ws;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 5;
const RECONNECT_DELAY = 5000; // 5 seconds

// Fetch open interest from REST API
function fetchOpenInterest(symbol) {
  return new Promise((resolve, reject) => {
    const url = `${OPEN_INTEREST_API}?symbol=${symbol.toUpperCase()}`;
    https.get(url, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(e);
        }
      });
    }).on('error', reject);
  });
}

// Fetch and display open interest for all symbols
async function displayOpenInterest() {
  console.log('\n🔓 ========== OPEN INTEREST ==========');
  console.log('Time:', new Date().toISOString());
  console.log('');
  
  for (const symbol of SYMBOLS) {
    try {
      const oi = await fetchOpenInterest(symbol);
      const oiValue = parseFloat(oi.openInterest);
      console.log(`  ${symbol.toUpperCase().padEnd(10)} | OI: ${oiValue.toLocaleString()} contracts`);
    } catch (err) {
      console.log(`  ${symbol.toUpperCase().padEnd(10)} | Error fetching OI`);
    }
  }
  console.log('========================================\n');
}

function connect() {
  console.log(`Connecting to ${WSS_URL}...`);
  
  ws = new WebSocket(WSS_URL);

  ws.on('open', () => {
    console.log('✓ Connected to Binance WebSocket');
    reconnectAttempts = 0;
    
    // Binance subscription format
    const subscribeMessage = {
      method: 'SUBSCRIBE',
      params: topics,
      id: Date.now()
    };
    
    console.log('Subscribing to topics:', topics);
    ws.send(JSON.stringify(subscribeMessage));
  });

  ws.on('message', (data) => {
    try {
      const message = JSON.parse(data.toString());
      
      // Handle subscription confirmation
      if (message.result === null && message.id) {
        console.log('✓ Successfully subscribed to topics');
        // Fetch initial open interest
        displayOpenInterest();
        return;
      }

      // Handle Binance mark price stream (includes funding rate)
      if (message.e === 'markPriceUpdate') {
        const symbol = message.s;
        const markPrice = parseFloat(message.p).toFixed(2);
        const indexPrice = parseFloat(message.i).toFixed(2);
        const fundingRate = (parseFloat(message.r) * 100).toFixed(4);
        const nextFunding = new Date(message.T).toLocaleTimeString();
        
        console.log(`\n💰 MARK PRICE | ${symbol} | Mark: $${markPrice} | Index: $${indexPrice} | Funding: ${fundingRate}% | Next: ${nextFunding}`);
      }

      // Handle open interest data (WOO X format - kept for reference)
      if (message.topic && message.topic.startsWith('openinterest@')) {
        const { topic, ts, data: openInterestData } = message;
        
        console.log('\n--- Open Interest Update ---');
        console.log('Topic:', topic);
        console.log('Symbol:', openInterestData.s);
        console.log('Open Interest:', openInterestData.oi);
        console.log('Last Update:', new Date(openInterestData.ts).toISOString());
        console.log('Received at:', new Date(ts).toISOString());
        console.log('---------------------------\n');
      }

      // Handle Binance orderbook data
      if (message.e === 'depthUpdate' || (message.b && message.a)) {
        const symbol = message.s || message.stream?.split('@')[0]?.toUpperCase() || 'UNKNOWN';
        const bids = message.b || message.bids;
        const asks = message.a || message.asks;
        
        console.log('\n========== ORDERBOOK UPDATE ==========');
        console.log('Symbol:', symbol);
        console.log('Time:', new Date().toISOString());
        console.log('\n📗 BIDS (Buy Orders):');
        console.log('  Price         | Quantity');
        console.log('  --------------|----------');
        bids.slice(0, 5).forEach(([price, qty]) => {
          console.log(`  ${price.padEnd(13)} | ${qty}`);
        });
        console.log('\n📕 ASKS (Sell Orders):');
        console.log('  Price         | Quantity');
        console.log('  --------------|----------');
        asks.slice(0, 5).forEach(([price, qty]) => {
          console.log(`  ${price.padEnd(13)} | ${qty}`);
        });
        
        // Calculate spread
        const bestBid = parseFloat(bids[0][0]);
        const bestAsk = parseFloat(asks[0][0]);
        const spread = bestAsk - bestBid;
        const spreadPct = ((spread / bestBid) * 100).toFixed(4);
        console.log(`\n📊 Spread: ${spread.toFixed(4)} (${spreadPct}%)`);
        console.log('=======================================\n');
      }

      // Handle WOO X orderbook data (kept for reference)
      if (message.topic && message.topic.startsWith('orderbook')) {
        const { topic, ts, data } = message;
        
        console.log('\n========== ORDERBOOK UPDATE ==========');
        console.log('Symbol:', data.s);
        console.log('Time:', new Date(data.ts).toISOString());
        console.log('\n📗 BIDS (Buy Orders):');
        console.log('  Price      | Quantity');
        console.log('  -----------|----------');
        data.bids.slice(0, 5).forEach(([price, qty]) => {
          console.log(`  ${price.padEnd(10)} | ${qty}`);
        });
        console.log('\n📕 ASKS (Sell Orders):');
        console.log('  Price      | Quantity');
        console.log('  -----------|----------');
        data.asks.slice(0, 5).forEach(([price, qty]) => {
          console.log(`  ${price.padEnd(10)} | ${qty}`);
        });
        
        // Calculate spread
        const bestBid = parseFloat(data.bids[0][0]);
        const bestAsk = parseFloat(data.asks[0][0]);
        const spread = bestAsk - bestBid;
        const spreadPct = ((spread / bestBid) * 100).toFixed(4);
        console.log(`\n📊 Spread: ${spread.toFixed(2)} (${spreadPct}%)`);
        console.log('=======================================\n');
      }

      // Handle PONG responses
      if (message.cmd === 'PONG') {
        const latency = message.time - message.ts;
        console.log(`PONG received. Latency: ${latency}ms`);
      }

    } catch (error) {
      console.error('Error parsing message:', error);
      console.error('Raw message:', data.toString());
    }
  });

  ws.on('error', (error) => {
    console.error('WebSocket error:', error.message);
  });

  ws.on('close', (code, reason) => {
    console.log(`WebSocket closed. Code: ${code}, Reason: ${reason || 'No reason provided'}`);
    
    // Attempt reconnection
    if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
      reconnectAttempts++;
      console.log(`Reconnecting in ${RECONNECT_DELAY / 1000}s... (Attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})`);
      setTimeout(connect, RECONNECT_DELAY);
    } else {
      console.error('Max reconnection attempts reached. Exiting...');
      process.exit(1);
    }
  });

  ws.on('ping', () => {
    console.log('Received ping from server');
  });

  ws.on('pong', () => {
    console.log('Received pong from server');
  });
}

// Optional: Send PING every 30 seconds to verify connectivity
function startPingInterval() {
  setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      const pingMessage = {
        cmd: 'PING',
        ts: Date.now()
      };
      ws.send(JSON.stringify(pingMessage));
    }
  }, 30000);
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\nShutting down gracefully...');
  if (ws) {
    ws.close();
  }
  process.exit(0);
});

// Start the connection
connect();
startPingInterval();

// Fetch open interest every 10 seconds
const openInterestInterval = setInterval(() => {
  displayOpenInterest();
}, 10000);

// Auto-stop after 5 minutes (300000 ms)
const STREAM_DURATION = 5 * 60 * 1000; // 5 minutes
setTimeout(() => {
  console.log('\n⏱️  5 minutes elapsed. Stopping stream...');
  clearInterval(openInterestInterval);
  if (ws) {
    ws.close();
  }
  process.exit(0);
}, STREAM_DURATION);

console.log('Binance Futures Stream started. Will run for 5 minutes...');
console.log('Streaming: Orderbook, Mark Price, Funding Rate, Open Interest');
console.log('Press Ctrl+C to exit early.');
