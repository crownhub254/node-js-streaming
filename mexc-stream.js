const WebSocket = require('ws');
const https = require('https');
const zlib = require('zlib');

// MEXC Futures WebSocket URL
const WSS_URL = 'wss://contract.mexc.com/edge';

// REST API for open interest
const OPEN_INTEREST_API = 'https://contract.mexc.com/api/v1/contract/open_interest';

// Symbols to subscribe to (MEXC uses underscore format)
const SYMBOLS = [
  'SOL_USDT',
  'BTC_USDT',
  'ETH_USDT',
];

let ws;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 5;
const RECONNECT_DELAY = 5000; // 5 seconds

// Fetch open interest from REST API
function fetchOpenInterest(symbol) {
  return new Promise((resolve, reject) => {
    const url = `${OPEN_INTEREST_API}/${symbol}`;
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
  console.log('\n🔓 ========== MEXC OPEN INTEREST ==========');
  console.log('Time:', new Date().toISOString());
  console.log('');
  
  for (const symbol of SYMBOLS) {
    try {
      const response = await fetchOpenInterest(symbol);
      if (response.success && response.data) {
        const oi = response.data;
        console.log(`  ${symbol.padEnd(10)} | OI: ${parseFloat(oi.holdVol || oi.value || 0).toLocaleString()} contracts`);
      } else {
        console.log(`  ${symbol.padEnd(10)} | No data available`);
      }
    } catch (err) {
      console.log(`  ${symbol.padEnd(10)} | Error fetching OI`);
    }
  }
  console.log('=============================================\n');
}

function connect() {
  console.log(`Connecting to MEXC: ${WSS_URL}...`);
  
  ws = new WebSocket(WSS_URL);

  ws.on('open', () => {
    console.log('✓ Connected to MEXC WebSocket');
    reconnectAttempts = 0;
    
    // Subscribe to orderbook depth for each symbol
    SYMBOLS.forEach(symbol => {
      // Subscribe to depth (orderbook)
      const depthSub = {
        method: 'sub.depth',
        param: {
          symbol: symbol
        }
      };
      ws.send(JSON.stringify(depthSub));
      
      // Subscribe to ticker (includes funding rate info)
      const tickerSub = {
        method: 'sub.ticker',
        param: {
          symbol: symbol
        }
      };
      ws.send(JSON.stringify(tickerSub));
    });
    
    console.log('Subscribed to:', SYMBOLS.join(', '));
    
    // Fetch initial open interest
    displayOpenInterest();
  });

  ws.on('message', (data) => {
    try {
      let message;
      
      // MEXC may send compressed data
      if (data instanceof Buffer) {
        try {
          // Try to decompress if it's gzipped
          const decompressed = zlib.gunzipSync(data);
          message = JSON.parse(decompressed.toString());
        } catch (e) {
          // If decompression fails, try parsing as-is
          message = JSON.parse(data.toString());
        }
      } else {
        message = JSON.parse(data.toString());
      }

      // Handle ping/pong
      if (message.channel === 'pong' || message.data === 'pong') {
        return;
      }

      // Handle depth (orderbook) data
      if (message.channel === 'push.depth' || message.topic?.includes('depth')) {
        const depthData = message.data || message;
        const symbol = message.symbol || depthData.symbol || 'UNKNOWN';
        const bids = depthData.bids || [];
        const asks = depthData.asks || [];
        
        if (bids.length > 0 && asks.length > 0) {
          console.log('\n========== MEXC ORDERBOOK ==========');
          console.log('Symbol:', symbol);
          console.log('Time:', new Date().toISOString());
          console.log('\n📗 BIDS (Buy Orders):');
          console.log('  Price         | Quantity');
          console.log('  --------------|----------');
          bids.slice(0, 5).forEach(bid => {
            const price = bid[0] || bid.price;
            const qty = bid[1] || bid.vol;
            console.log(`  ${String(price).padEnd(13)} | ${qty}`);
          });
          console.log('\n📕 ASKS (Sell Orders):');
          console.log('  Price         | Quantity');
          console.log('  --------------|----------');
          asks.slice(0, 5).forEach(ask => {
            const price = ask[0] || ask.price;
            const qty = ask[1] || ask.vol;
            console.log(`  ${String(price).padEnd(13)} | ${qty}`);
          });
          
          // Calculate spread
          const bestBid = parseFloat(bids[0][0] || bids[0].price);
          const bestAsk = parseFloat(asks[0][0] || asks[0].price);
          const spread = bestAsk - bestBid;
          const spreadPct = ((spread / bestBid) * 100).toFixed(4);
          console.log(`\n📊 Spread: ${spread.toFixed(4)} (${spreadPct}%)`);
          console.log('=====================================\n');
        }
      }

      // Handle ticker data (includes price, volume, funding rate)
      if (message.channel === 'push.ticker') {
        const ticker = message.data || message;
        const symbol = message.symbol || ticker.symbol;
        const lastPrice = ticker.lastPrice || ticker.price;
        const fundingRate = ticker.fundingRate;
        const volume24h = ticker.volume24;
        
        console.log(`💰 TICKER | ${symbol} | Price: $${lastPrice} | Vol24h: ${volume24h || 'N/A'} | Funding: ${fundingRate ? (fundingRate * 100).toFixed(4) + '%' : 'N/A'}`);
      }

      // Handle subscription confirmation
      if (message.channel === 'rs.sub.depth' || message.channel === 'rs.sub.ticker') {
        console.log(`✓ Subscribed to ${message.channel}`);
      }

    } catch (error) {
      // Ignore parse errors for binary/ping frames
    }
  });

  ws.on('error', (error) => {
    console.error('WebSocket error:', error.message);
  });

  ws.on('close', (code, reason) => {
    console.log(`WebSocket closed. Code: ${code}, Reason: ${reason || 'No reason provided'}`);
    
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
    ws.pong();
  });
}

// Send ping every 20 seconds to keep connection alive
function startPingInterval() {
  setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ method: 'ping' }));
    }
  }, 20000);
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

// Auto-stop after 5 minutes
const STREAM_DURATION = 5 * 60 * 1000;
setTimeout(() => {
  console.log('\n⏱️  5 minutes elapsed. Stopping stream...');
  clearInterval(openInterestInterval);
  if (ws) {
    ws.close();
  }
  process.exit(0);
}, STREAM_DURATION);

console.log('MEXC Futures Stream started. Will run for 5 minutes...');
console.log('Streaming: Orderbook, Ticker, Open Interest');
console.log('Press Ctrl+C to exit early.');
