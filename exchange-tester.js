const WebSocket = require('ws');
const https = require('https');
const http = require('http');
const zlib = require('zlib');

// Test symbols
const SYMBOLS = {
  btc: { spot: 'BTCUSDT', futures: 'BTCUSDT' },
  eth: { spot: 'ETHUSDT', futures: 'ETHUSDT' },
  sol: { spot: 'SOLUSDT', futures: 'SOLUSDT' }
};

// Results storage
const results = {};

// Utility: Make HTTP request
function httpRequest(url) {
  return new Promise((resolve, reject) => {
    const protocol = url.startsWith('https') ? https : http;
    protocol.get(url, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          resolve(data);
        }
      });
    }).on('error', reject);
  });
}

// Utility: Test WebSocket connection with timeout
function testWebSocket(config) {
  return new Promise((resolve) => {
    const { name, url, subscribeMsg, timeout = 15000, parseMessage, headers } = config;
    const startTime = Date.now();
    let ws;
    let receivedData = [];
    let connected = false;
    
    const cleanup = () => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.close();
      }
    };

    const timer = setTimeout(() => {
      cleanup();
      resolve({
        name,
        success: receivedData.length > 0,
        connected,
        messages: receivedData.length,
        samples: receivedData.slice(0, 3),
        duration: Date.now() - startTime,
        error: receivedData.length === 0 ? 'Timeout - no data received' : null
      });
    }, timeout);

    try {
      ws = new WebSocket(url, headers ? { headers } : undefined);
      
      ws.on('open', () => {
        connected = true;
        if (subscribeMsg) {
          if (Array.isArray(subscribeMsg)) {
            subscribeMsg.forEach(msg => ws.send(JSON.stringify(msg)));
          } else {
            ws.send(JSON.stringify(subscribeMsg));
          }
        }
      });

      ws.on('message', (data) => {
        try {
          let message;
          if (data instanceof Buffer) {
            try {
              const decompressed = zlib.gunzipSync(data);
              message = JSON.parse(decompressed.toString());
            } catch {
              try {
                const inflated = zlib.inflateRawSync(data);
                message = JSON.parse(inflated.toString());
              } catch {
                message = JSON.parse(data.toString());
              }
            }
          } else {
            message = JSON.parse(data.toString());
          }
          
          // Handle ping/pong
          if (message.ping) {
            ws.send(JSON.stringify({ pong: message.ping }));
            return;
          }
          
          const parsed = parseMessage ? parseMessage(message) : message;
          if (parsed && !parsed.isPing) {
            receivedData.push(parsed);
          }
          
          // Stop after getting enough samples
          if (receivedData.length >= 5) {
            clearTimeout(timer);
            cleanup();
            resolve({
              name,
              success: true,
              connected: true,
              messages: receivedData.length,
              samples: receivedData.slice(0, 3),
              duration: Date.now() - startTime,
              error: null
            });
          }
        } catch (e) {
          // Ignore parse errors
        }
      });

      ws.on('error', (error) => {
        clearTimeout(timer);
        cleanup();
        resolve({
          name,
          success: false,
          connected,
          messages: receivedData.length,
          samples: receivedData.slice(0, 3),
          duration: Date.now() - startTime,
          error: error.message
        });
      });

      ws.on('close', () => {
        if (receivedData.length === 0) {
          clearTimeout(timer);
          resolve({
            name,
            success: false,
            connected,
            messages: 0,
            samples: [],
            duration: Date.now() - startTime,
            error: 'Connection closed before receiving data'
          });
        }
      });
    } catch (error) {
      clearTimeout(timer);
      resolve({
        name,
        success: false,
        connected: false,
        messages: 0,
        samples: [],
        duration: Date.now() - startTime,
        error: error.message
      });
    }
  });
}

// ============ EXCHANGE CONFIGURATIONS ============

const exchanges = {
  // 1. BINANCE
  binance: {
    name: 'Binance',
    spot: true,
    futures: true,
    tests: {
      spot_orderbook: {
        url: 'wss://stream.binance.com:9443/ws/btcusdt@depth10@100ms',
        subscribeMsg: null,
        parseMessage: (m) => m.bids ? { type: 'orderbook', bids: m.bids?.length, asks: m.asks?.length } : null
      },
      spot_ticker: {
        url: 'wss://stream.binance.com:9443/ws/btcusdt@ticker',
        subscribeMsg: null,
        parseMessage: (m) => m.s ? { type: 'ticker', symbol: m.s, price: m.c } : null
      },
      spot_trades: {
        url: 'wss://stream.binance.com:9443/ws/btcusdt@trade',
        subscribeMsg: null,
        parseMessage: (m) => m.e === 'trade' ? { type: 'trade', symbol: m.s, price: m.p, qty: m.q } : null
      },
      spot_kline: {
        url: 'wss://stream.binance.com:9443/ws/btcusdt@kline_1m',
        subscribeMsg: null,
        parseMessage: (m) => m.k ? { type: 'kline', symbol: m.s, open: m.k.o, close: m.k.c } : null
      },
      futures_orderbook: {
        url: 'wss://fstream.binance.com/ws/btcusdt@depth10@100ms',
        subscribeMsg: null,
        parseMessage: (m) => m.b ? { type: 'orderbook', bids: m.b?.length, asks: m.a?.length } : null
      },
      futures_ticker: {
        url: 'wss://fstream.binance.com/ws/btcusdt@markPrice@1s',
        subscribeMsg: null,
        parseMessage: (m) => m.p ? { type: 'markPrice', symbol: m.s, price: m.p, funding: m.r } : null
      },
      futures_trades: {
        url: 'wss://fstream.binance.com/ws/btcusdt@aggTrade',
        subscribeMsg: null,
        parseMessage: (m) => m.e === 'aggTrade' ? { type: 'trade', symbol: m.s, price: m.p, qty: m.q } : null
      },
      futures_kline: {
        url: 'wss://fstream.binance.com/ws/btcusdt@kline_1m',
        subscribeMsg: null,
        parseMessage: (m) => m.k ? { type: 'kline', symbol: m.s, open: m.k.o, close: m.k.c } : null
      }
    }
  },

  // 2. BYBIT
  bybit: {
    name: 'Bybit',
    spot: true,
    futures: true,
    tests: {
      spot_orderbook: {
        url: 'wss://stream.bybit.com/v5/public/spot',
        subscribeMsg: { op: 'subscribe', args: ['orderbook.50.BTCUSDT'] },
        parseMessage: (m) => m.topic?.includes('orderbook') ? { type: 'orderbook', data: m.data } : null
      },
      spot_ticker: {
        url: 'wss://stream.bybit.com/v5/public/spot',
        subscribeMsg: { op: 'subscribe', args: ['tickers.BTCUSDT'] },
        parseMessage: (m) => m.topic?.includes('tickers') ? { type: 'ticker', data: m.data } : null
      },
      spot_trades: {
        url: 'wss://stream.bybit.com/v5/public/spot',
        subscribeMsg: { op: 'subscribe', args: ['publicTrade.BTCUSDT'] },
        parseMessage: (m) => m.topic?.includes('Trade') ? { type: 'trade', data: m.data } : null
      },
      spot_kline: {
        url: 'wss://stream.bybit.com/v5/public/spot',
        subscribeMsg: { op: 'subscribe', args: ['kline.1.BTCUSDT'] },
        parseMessage: (m) => m.topic?.includes('kline') ? { type: 'kline', data: m.data } : null
      },
      futures_orderbook: {
        url: 'wss://stream.bybit.com/v5/public/linear',
        subscribeMsg: { op: 'subscribe', args: ['orderbook.50.BTCUSDT'] },
        parseMessage: (m) => m.topic?.includes('orderbook') ? { type: 'orderbook', data: m.data } : null
      },
      futures_ticker: {
        url: 'wss://stream.bybit.com/v5/public/linear',
        subscribeMsg: { op: 'subscribe', args: ['tickers.BTCUSDT'] },
        parseMessage: (m) => m.topic?.includes('tickers') ? { type: 'ticker', data: m.data } : null
      },
      futures_trades: {
        url: 'wss://stream.bybit.com/v5/public/linear',
        subscribeMsg: { op: 'subscribe', args: ['publicTrade.BTCUSDT'] },
        parseMessage: (m) => m.topic?.includes('Trade') ? { type: 'trade', data: m.data } : null
      },
      futures_kline: {
        url: 'wss://stream.bybit.com/v5/public/linear',
        subscribeMsg: { op: 'subscribe', args: ['kline.1.BTCUSDT'] },
        parseMessage: (m) => m.topic?.includes('kline') ? { type: 'kline', data: m.data } : null
      }
    }
  },

  // 3. OKX
  okx: {
    name: 'OKX',
    spot: true,
    futures: true,
    tests: {
      spot_orderbook: {
        url: 'wss://ws.okx.com:8443/ws/v5/public',
        subscribeMsg: { op: 'subscribe', args: [{ channel: 'books5', instId: 'BTC-USDT' }] },
        parseMessage: (m) => m.data ? { type: 'orderbook', channel: m.arg?.channel, data: m.data } : null
      },
      spot_ticker: {
        url: 'wss://ws.okx.com:8443/ws/v5/public',
        subscribeMsg: { op: 'subscribe', args: [{ channel: 'tickers', instId: 'BTC-USDT' }] },
        parseMessage: (m) => m.data ? { type: 'ticker', data: m.data } : null
      },
      spot_trades: {
        url: 'wss://ws.okx.com:8443/ws/v5/public',
        subscribeMsg: { op: 'subscribe', args: [{ channel: 'trades', instId: 'BTC-USDT' }] },
        parseMessage: (m) => m.data && m.arg?.channel === 'trades' ? { type: 'trade', data: m.data } : null
      },
      spot_kline: {
        url: 'wss://ws.okx.com:8443/ws/v5/public',
        subscribeMsg: { op: 'subscribe', args: [{ channel: 'candle1m', instId: 'BTC-USDT' }] },
        parseMessage: (m) => m.data && m.arg?.channel?.includes('candle') ? { type: 'kline', data: m.data } : null
      },
      futures_orderbook: {
        url: 'wss://ws.okx.com:8443/ws/v5/public',
        subscribeMsg: { op: 'subscribe', args: [{ channel: 'books5', instId: 'BTC-USDT-SWAP' }] },
        parseMessage: (m) => m.data ? { type: 'orderbook', data: m.data } : null
      },
      futures_ticker: {
        url: 'wss://ws.okx.com:8443/ws/v5/public',
        subscribeMsg: { op: 'subscribe', args: [{ channel: 'tickers', instId: 'BTC-USDT-SWAP' }] },
        parseMessage: (m) => m.data ? { type: 'ticker', data: m.data } : null
      },
      futures_trades: {
        url: 'wss://ws.okx.com:8443/ws/v5/public',
        subscribeMsg: { op: 'subscribe', args: [{ channel: 'trades', instId: 'BTC-USDT-SWAP' }] },
        parseMessage: (m) => m.data && m.arg?.channel === 'trades' ? { type: 'trade', data: m.data } : null
      },
      futures_openinterest: {
        url: 'wss://ws.okx.com:8443/ws/v5/public',
        subscribeMsg: { op: 'subscribe', args: [{ channel: 'open-interest', instId: 'BTC-USDT-SWAP' }] },
        parseMessage: (m) => m.data && m.arg?.channel === 'open-interest' ? { type: 'openInterest', data: m.data } : null
      }
    }
  },

  // 4. KRAKEN
  kraken: {
    name: 'Kraken',
    spot: true,
    futures: true,
    tests: {
      spot_orderbook: {
        url: 'wss://ws.kraken.com',
        subscribeMsg: { event: 'subscribe', pair: ['XBT/USD'], subscription: { name: 'book', depth: 10 } },
        parseMessage: (m) => Array.isArray(m) && m[1]?.as ? { type: 'orderbook', data: m } : null
      },
      spot_ticker: {
        url: 'wss://ws.kraken.com',
        subscribeMsg: { event: 'subscribe', pair: ['XBT/USD'], subscription: { name: 'ticker' } },
        parseMessage: (m) => Array.isArray(m) && m[1]?.c ? { type: 'ticker', data: m } : null
      },
      spot_trades: {
        url: 'wss://ws.kraken.com',
        subscribeMsg: { event: 'subscribe', pair: ['XBT/USD'], subscription: { name: 'trade' } },
        parseMessage: (m) => Array.isArray(m) && Array.isArray(m[1]) && m[1][0]?.length === 6 ? { type: 'trade', data: m } : null
      },
      spot_kline: {
        url: 'wss://ws.kraken.com',
        subscribeMsg: { event: 'subscribe', pair: ['XBT/USD'], subscription: { name: 'ohlc', interval: 1 } },
        parseMessage: (m) => Array.isArray(m) && m[1]?.length === 9 ? { type: 'kline', data: m } : null
      },
      futures_orderbook: {
        url: 'wss://futures.kraken.com/ws/v1',
        subscribeMsg: { event: 'subscribe', feed: 'book', product_ids: ['PI_XBTUSD'] },
        parseMessage: (m) => m.feed === 'book_snapshot' || m.feed === 'book' ? { type: 'orderbook', data: m } : null
      },
      futures_ticker: {
        url: 'wss://futures.kraken.com/ws/v1',
        subscribeMsg: { event: 'subscribe', feed: 'ticker', product_ids: ['PI_XBTUSD'] },
        parseMessage: (m) => m.feed === 'ticker' ? { type: 'ticker', data: m } : null
      },
      futures_trades: {
        url: 'wss://futures.kraken.com/ws/v1',
        subscribeMsg: { event: 'subscribe', feed: 'trade', product_ids: ['PI_XBTUSD'] },
        parseMessage: (m) => m.feed === 'trade' ? { type: 'trade', data: m } : null
      }
    }
  },

  // 5. GATE.IO
  gateio: {
    name: 'Gate.io',
    spot: true,
    futures: true,
    tests: {
      spot_orderbook: {
        url: 'wss://api.gateio.ws/ws/v4/',
        subscribeMsg: { time: Date.now(), channel: 'spot.order_book', event: 'subscribe', payload: ['BTC_USDT', '10', '100ms'] },
        parseMessage: (m) => m.channel === 'spot.order_book' ? { type: 'orderbook', data: m.result } : null
      },
      spot_ticker: {
        url: 'wss://api.gateio.ws/ws/v4/',
        subscribeMsg: { time: Date.now(), channel: 'spot.tickers', event: 'subscribe', payload: ['BTC_USDT'] },
        parseMessage: (m) => m.channel === 'spot.tickers' ? { type: 'ticker', data: m.result } : null
      },
      spot_trades: {
        url: 'wss://api.gateio.ws/ws/v4/',
        subscribeMsg: { time: Date.now(), channel: 'spot.trades', event: 'subscribe', payload: ['BTC_USDT'] },
        parseMessage: (m) => m.channel === 'spot.trades' ? { type: 'trade', data: m.result } : null
      },
      spot_kline: {
        url: 'wss://api.gateio.ws/ws/v4/',
        subscribeMsg: { time: Date.now(), channel: 'spot.candlesticks', event: 'subscribe', payload: ['1m', 'BTC_USDT'] },
        parseMessage: (m) => m.channel === 'spot.candlesticks' ? { type: 'kline', data: m.result } : null
      },
      futures_orderbook: {
        url: 'wss://fx-ws.gateio.ws/v4/ws/usdt',
        subscribeMsg: { time: Date.now(), channel: 'futures.order_book', event: 'subscribe', payload: ['BTC_USDT', '10', '0'] },
        parseMessage: (m) => m.channel === 'futures.order_book' ? { type: 'orderbook', data: m.result } : null
      },
      futures_ticker: {
        url: 'wss://fx-ws.gateio.ws/v4/ws/usdt',
        subscribeMsg: { time: Date.now(), channel: 'futures.tickers', event: 'subscribe', payload: ['BTC_USDT'] },
        parseMessage: (m) => m.channel === 'futures.tickers' ? { type: 'ticker', data: m.result } : null
      },
      futures_trades: {
        url: 'wss://fx-ws.gateio.ws/v4/ws/usdt',
        subscribeMsg: { time: Date.now(), channel: 'futures.trades', event: 'subscribe', payload: ['BTC_USDT'] },
        parseMessage: (m) => m.channel === 'futures.trades' ? { type: 'trade', data: m.result } : null
      }
    }
  },

  // 6. KUCOIN
  kucoin: {
    name: 'KuCoin',
    spot: true,
    futures: true,
    tests: {
      // KuCoin requires getting a token first via REST API
      // We'll test with direct connection attempt
      spot_ticker: {
        url: 'wss://ws-api-spot.kucoin.com',
        subscribeMsg: { id: Date.now(), type: 'subscribe', topic: '/market/ticker:BTC-USDT', privateChannel: false, response: true },
        parseMessage: (m) => m.topic ? { type: 'ticker', data: m.data } : null
      }
    }
  },

  // 7. MEXC
  mexc: {
    name: 'MEXC',
    spot: true,
    futures: true,
    tests: {
      spot_orderbook: {
        url: 'wss://wbs.mexc.com/ws',
        subscribeMsg: { method: 'SUBSCRIPTION', params: ['spot@public.limit.depth.v3.api@BTCUSDT@10'] },
        parseMessage: (m) => m.d?.bids ? { type: 'orderbook', data: m.d } : null
      },
      spot_ticker: {
        url: 'wss://wbs.mexc.com/ws',
        subscribeMsg: { method: 'SUBSCRIPTION', params: ['spot@public.miniTickers.v3.api@BTCUSDT'] },
        parseMessage: (m) => m.d?.s ? { type: 'ticker', data: m.d } : null
      },
      spot_trades: {
        url: 'wss://wbs.mexc.com/ws',
        subscribeMsg: { method: 'SUBSCRIPTION', params: ['spot@public.deals.v3.api@BTCUSDT'] },
        parseMessage: (m) => m.d?.deals ? { type: 'trade', data: m.d } : null
      },
      spot_kline: {
        url: 'wss://wbs.mexc.com/ws',
        subscribeMsg: { method: 'SUBSCRIPTION', params: ['spot@public.kline.v3.api@BTCUSDT@Min1'] },
        parseMessage: (m) => m.d?.k ? { type: 'kline', data: m.d } : null
      },
      futures_orderbook: {
        url: 'wss://contract.mexc.com/edge',
        subscribeMsg: { method: 'sub.depth', param: { symbol: 'BTC_USDT' } },
        parseMessage: (m) => m.channel === 'push.depth' ? { type: 'orderbook', data: m.data } : null
      },
      futures_ticker: {
        url: 'wss://contract.mexc.com/edge',
        subscribeMsg: { method: 'sub.ticker', param: { symbol: 'BTC_USDT' } },
        parseMessage: (m) => m.channel === 'push.ticker' ? { type: 'ticker', data: m.data } : null
      }
    }
  },

  // 8. COINBASE
  coinbase: {
    name: 'Coinbase',
    spot: true,
    futures: false,
    tests: {
      spot_orderbook: {
        url: 'wss://ws-feed.exchange.coinbase.com',
        subscribeMsg: { type: 'subscribe', product_ids: ['BTC-USD'], channels: ['level2_batch'] },
        parseMessage: (m) => m.type === 'l2update' || m.type === 'snapshot' ? { type: 'orderbook', data: m } : null
      },
      spot_ticker: {
        url: 'wss://ws-feed.exchange.coinbase.com',
        subscribeMsg: { type: 'subscribe', product_ids: ['BTC-USD'], channels: ['ticker'] },
        parseMessage: (m) => m.type === 'ticker' ? { type: 'ticker', price: m.price, volume: m.volume_24h } : null
      },
      spot_trades: {
        url: 'wss://ws-feed.exchange.coinbase.com',
        subscribeMsg: { type: 'subscribe', product_ids: ['BTC-USD'], channels: ['matches'] },
        parseMessage: (m) => m.type === 'match' ? { type: 'trade', price: m.price, size: m.size } : null
      }
    }
  },

  // 9. HTX (Huobi)
  htx: {
    name: 'HTX',
    spot: true,
    futures: true,
    tests: {
      spot_orderbook: {
        url: 'wss://api.huobi.pro/ws',
        subscribeMsg: { sub: 'market.btcusdt.depth.step0', id: 'depth1' },
        parseMessage: (m) => m.tick?.bids ? { type: 'orderbook', bids: m.tick.bids.length } : null
      },
      spot_ticker: {
        url: 'wss://api.huobi.pro/ws',
        subscribeMsg: { sub: 'market.btcusdt.ticker', id: 'ticker1' },
        parseMessage: (m) => m.tick?.close ? { type: 'ticker', data: m.tick } : null
      },
      spot_trades: {
        url: 'wss://api.huobi.pro/ws',
        subscribeMsg: { sub: 'market.btcusdt.trade.detail', id: 'trade1' },
        parseMessage: (m) => m.tick?.data ? { type: 'trade', data: m.tick.data } : null
      },
      spot_kline: {
        url: 'wss://api.huobi.pro/ws',
        subscribeMsg: { sub: 'market.btcusdt.kline.1min', id: 'kline1' },
        parseMessage: (m) => m.tick?.open ? { type: 'kline', data: m.tick } : null
      },
      futures_orderbook: {
        url: 'wss://api.hbdm.com/linear-swap-ws',
        subscribeMsg: { sub: 'market.BTC-USDT.depth.step0', id: 'depth1' },
        parseMessage: (m) => m.tick?.bids ? { type: 'orderbook', bids: m.tick.bids.length } : null
      },
      futures_ticker: {
        url: 'wss://api.hbdm.com/linear-swap-ws',
        subscribeMsg: { sub: 'market.BTC-USDT.detail', id: 'ticker1' },
        parseMessage: (m) => m.tick?.close ? { type: 'ticker', data: m.tick } : null
      }
    }
  },

  // 10. BITFINEX
  bitfinex: {
    name: 'Bitfinex',
    spot: true,
    futures: true,
    tests: {
      spot_orderbook: {
        url: 'wss://api-pub.bitfinex.com/ws/2',
        subscribeMsg: { event: 'subscribe', channel: 'book', symbol: 'tBTCUSD', prec: 'P0', freq: 'F0', len: 25 },
        parseMessage: (m) => Array.isArray(m) && m[1] !== 'hb' ? { type: 'orderbook', data: m } : null
      },
      spot_ticker: {
        url: 'wss://api-pub.bitfinex.com/ws/2',
        subscribeMsg: { event: 'subscribe', channel: 'ticker', symbol: 'tBTCUSD' },
        parseMessage: (m) => Array.isArray(m) && m[1] !== 'hb' ? { type: 'ticker', data: m } : null
      },
      spot_trades: {
        url: 'wss://api-pub.bitfinex.com/ws/2',
        subscribeMsg: { event: 'subscribe', channel: 'trades', symbol: 'tBTCUSD' },
        parseMessage: (m) => Array.isArray(m) && m[1] !== 'hb' && m[1] !== 'te' ? { type: 'trade', data: m } : null
      },
      spot_kline: {
        url: 'wss://api-pub.bitfinex.com/ws/2',
        subscribeMsg: { event: 'subscribe', channel: 'candles', key: 'trade:1m:tBTCUSD' },
        parseMessage: (m) => Array.isArray(m) && m[1] !== 'hb' ? { type: 'kline', data: m } : null
      },
      futures_orderbook: {
        url: 'wss://api-pub.bitfinex.com/ws/2',
        subscribeMsg: { event: 'subscribe', channel: 'book', symbol: 'tBTCF0:USTF0', prec: 'P0', freq: 'F0', len: 25 },
        parseMessage: (m) => Array.isArray(m) && m[1] !== 'hb' ? { type: 'orderbook', data: m } : null
      }
    }
  },

  // 11. BITMEX
  bitmex: {
    name: 'BitMEX',
    spot: false,
    futures: true,
    tests: {
      futures_orderbook: {
        url: 'wss://ws.bitmex.com/realtime',
        subscribeMsg: { op: 'subscribe', args: ['orderBookL2_25:XBTUSD'] },
        parseMessage: (m) => m.table === 'orderBookL2_25' ? { type: 'orderbook', data: m.data } : null
      },
      futures_ticker: {
        url: 'wss://ws.bitmex.com/realtime',
        subscribeMsg: { op: 'subscribe', args: ['instrument:XBTUSD'] },
        parseMessage: (m) => m.table === 'instrument' ? { type: 'ticker', data: m.data } : null
      },
      futures_trades: {
        url: 'wss://ws.bitmex.com/realtime',
        subscribeMsg: { op: 'subscribe', args: ['trade:XBTUSD'] },
        parseMessage: (m) => m.table === 'trade' ? { type: 'trade', data: m.data } : null
      }
    }
  },

  // 12. BITGET
  bitget: {
    name: 'Bitget',
    spot: true,
    futures: true,
    tests: {
      spot_orderbook: {
        url: 'wss://ws.bitget.com/v2/ws/public',
        subscribeMsg: { op: 'subscribe', args: [{ instType: 'SPOT', channel: 'books15', instId: 'BTCUSDT' }] },
        parseMessage: (m) => m.data ? { type: 'orderbook', data: m.data } : null
      },
      spot_ticker: {
        url: 'wss://ws.bitget.com/v2/ws/public',
        subscribeMsg: { op: 'subscribe', args: [{ instType: 'SPOT', channel: 'ticker', instId: 'BTCUSDT' }] },
        parseMessage: (m) => m.data && m.arg?.channel === 'ticker' ? { type: 'ticker', data: m.data } : null
      },
      spot_trades: {
        url: 'wss://ws.bitget.com/v2/ws/public',
        subscribeMsg: { op: 'subscribe', args: [{ instType: 'SPOT', channel: 'trade', instId: 'BTCUSDT' }] },
        parseMessage: (m) => m.data && m.arg?.channel === 'trade' ? { type: 'trade', data: m.data } : null
      },
      spot_kline: {
        url: 'wss://ws.bitget.com/v2/ws/public',
        subscribeMsg: { op: 'subscribe', args: [{ instType: 'SPOT', channel: 'candle1m', instId: 'BTCUSDT' }] },
        parseMessage: (m) => m.data && m.arg?.channel?.includes('candle') ? { type: 'kline', data: m.data } : null
      },
      futures_orderbook: {
        url: 'wss://ws.bitget.com/v2/ws/public',
        subscribeMsg: { op: 'subscribe', args: [{ instType: 'USDT-FUTURES', channel: 'books15', instId: 'BTCUSDT' }] },
        parseMessage: (m) => m.data ? { type: 'orderbook', data: m.data } : null
      },
      futures_ticker: {
        url: 'wss://ws.bitget.com/v2/ws/public',
        subscribeMsg: { op: 'subscribe', args: [{ instType: 'USDT-FUTURES', channel: 'ticker', instId: 'BTCUSDT' }] },
        parseMessage: (m) => m.data && m.arg?.channel === 'ticker' ? { type: 'ticker', data: m.data } : null
      }
    }
  },

  // 13. BINGX
  bingx: {
    name: 'BingX',
    spot: true,
    futures: true,
    tests: {
      spot_orderbook: {
        url: 'wss://open-api-ws.bingx.com/market',
        subscribeMsg: { id: 'depth1', reqType: 'sub', dataType: 'BTC-USDT@depth10' },
        parseMessage: (m) => m.data?.bids ? { type: 'orderbook', data: m.data } : null
      },
      spot_trades: {
        url: 'wss://open-api-ws.bingx.com/market',
        subscribeMsg: { id: 'trade1', reqType: 'sub', dataType: 'BTC-USDT@trade' },
        parseMessage: (m) => m.data ? { type: 'trade', data: m.data } : null
      },
      futures_orderbook: {
        url: 'wss://open-api-swap.bingx.com/swap-market',
        subscribeMsg: { id: 'depth1', reqType: 'sub', dataType: 'BTC-USDT@depth10' },
        parseMessage: (m) => m.data?.bids ? { type: 'orderbook', data: m.data } : null
      },
      futures_trades: {
        url: 'wss://open-api-swap.bingx.com/swap-market',
        subscribeMsg: { id: 'trade1', reqType: 'sub', dataType: 'BTC-USDT@trade' },
        parseMessage: (m) => m.data ? { type: 'trade', data: m.data } : null
      }
    }
  },

  // 14. BITMART
  bitmart: {
    name: 'BitMart',
    spot: true,
    futures: true,
    tests: {
      spot_orderbook: {
        url: 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1',
        subscribeMsg: { op: 'subscribe', args: ['spot/depth5:BTC_USDT'] },
        parseMessage: (m) => m.data?.asks ? { type: 'orderbook', data: m.data } : null
      },
      spot_ticker: {
        url: 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1',
        subscribeMsg: { op: 'subscribe', args: ['spot/ticker:BTC_USDT'] },
        parseMessage: (m) => m.data ? { type: 'ticker', data: m.data } : null
      },
      spot_trades: {
        url: 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1',
        subscribeMsg: { op: 'subscribe', args: ['spot/trade:BTC_USDT'] },
        parseMessage: (m) => m.data?.trades ? { type: 'trade', data: m.data } : null
      },
      spot_kline: {
        url: 'wss://ws-manager-compress.bitmart.com/api?protocol=1.1',
        subscribeMsg: { op: 'subscribe', args: ['spot/kline1m:BTC_USDT'] },
        parseMessage: (m) => m.data?.candles ? { type: 'kline', data: m.data } : null
      },
      futures_orderbook: {
        url: 'wss://openapi-ws.bitmart.com/api?protocol=1.1',
        subscribeMsg: { action: 'subscribe', args: ['futures/depth20:BTCUSDT'] },
        parseMessage: (m) => m.data?.asks ? { type: 'orderbook', data: m.data } : null
      }
    }
  },

  // 15. UPBIT
  upbit: {
    name: 'Upbit',
    spot: true,
    futures: false,
    tests: {
      spot_orderbook: {
        url: 'wss://api.upbit.com/websocket/v1',
        subscribeMsg: [{ ticket: 'test' }, { type: 'orderbook', codes: ['KRW-BTC'] }],
        parseMessage: (m) => m.type === 'orderbook' ? { type: 'orderbook', data: m } : null
      },
      spot_ticker: {
        url: 'wss://api.upbit.com/websocket/v1',
        subscribeMsg: [{ ticket: 'test' }, { type: 'ticker', codes: ['KRW-BTC'] }],
        parseMessage: (m) => m.type === 'ticker' ? { type: 'ticker', data: m } : null
      },
      spot_trades: {
        url: 'wss://api.upbit.com/websocket/v1',
        subscribeMsg: [{ ticket: 'test' }, { type: 'trade', codes: ['KRW-BTC'] }],
        parseMessage: (m) => m.type === 'trade' ? { type: 'trade', data: m } : null
      }
    }
  },

  // 16. BITSTAMP
  bitstamp: {
    name: 'Bitstamp',
    spot: true,
    futures: false,
    tests: {
      spot_orderbook: {
        url: 'wss://ws.bitstamp.net',
        subscribeMsg: { event: 'bts:subscribe', data: { channel: 'order_book_btcusd' } },
        parseMessage: (m) => m.data?.bids ? { type: 'orderbook', data: m.data } : null
      },
      spot_ticker: {
        url: 'wss://ws.bitstamp.net',
        subscribeMsg: { event: 'bts:subscribe', data: { channel: 'live_trades_btcusd' } },
        parseMessage: (m) => m.data?.price ? { type: 'trade', price: m.data.price } : null
      }
    }
  },

  // 17. WEEX
  weex: {
    name: 'WEEX',
    spot: true,
    futures: true,
    tests: {
      futures_orderbook: {
        url: 'wss://ws.weex.com/v2/ws/public',
        subscribeMsg: { op: 'subscribe', args: [{ instType: 'USDT-FUTURES', channel: 'books15', instId: 'BTCUSDT' }] },
        parseMessage: (m) => m.data ? { type: 'orderbook', data: m.data } : null
      },
      futures_ticker: {
        url: 'wss://ws.weex.com/v2/ws/public',
        subscribeMsg: { op: 'subscribe', args: [{ instType: 'USDT-FUTURES', channel: 'ticker', instId: 'BTCUSDT' }] },
        parseMessage: (m) => m.data ? { type: 'ticker', data: m.data } : null
      }
    }
  }
};

// Main test runner
async function runExchangeTests() {
  console.log('╔════════════════════════════════════════════════════════════════╗');
  console.log('║     COMPREHENSIVE EXCHANGE WEBSOCKET STREAM TESTER             ║');
  console.log('║     Testing: Orderbook, Ticker, Trades, OHLCV, Open Interest   ║');
  console.log('╚════════════════════════════════════════════════════════════════╝\n');
  
  const exchangeNames = Object.keys(exchanges);
  console.log(`Testing ${exchangeNames.length} exchanges...\n`);
  
  for (const exchangeKey of exchangeNames) {
    const exchange = exchanges[exchangeKey];
    console.log(`\n${'═'.repeat(60)}`);
    console.log(`  📊 ${exchange.name.toUpperCase()}`);
    console.log(`  Spot: ${exchange.spot ? '✅' : '❌'} | Futures: ${exchange.futures ? '✅' : '❌'}`);
    console.log('═'.repeat(60));
    
    results[exchangeKey] = {
      name: exchange.name,
      spot: exchange.spot,
      futures: exchange.futures,
      tests: {}
    };
    
    for (const [testName, testConfig] of Object.entries(exchange.tests)) {
      process.stdout.write(`  Testing ${testName}... `);
      
      try {
        const result = await testWebSocket({
          name: testName,
          url: testConfig.url,
          subscribeMsg: testConfig.subscribeMsg,
          parseMessage: testConfig.parseMessage,
          timeout: 10000
        });
        
        results[exchangeKey].tests[testName] = result;
        
        if (result.success) {
          console.log(`✅ SUCCESS (${result.messages} msgs in ${result.duration}ms)`);
          if (result.samples[0]) {
            console.log(`     Sample: ${JSON.stringify(result.samples[0]).substring(0, 80)}...`);
          }
        } else {
          console.log(`❌ FAILED: ${result.error || 'No data'}`);
        }
      } catch (error) {
        console.log(`❌ ERROR: ${error.message}`);
        results[exchangeKey].tests[testName] = { success: false, error: error.message };
      }
      
      // Small delay between tests
      await new Promise(r => setTimeout(r, 500));
    }
  }
  
  // Generate summary
  generateSummary();
}

function generateSummary() {
  console.log('\n\n');
  console.log('╔════════════════════════════════════════════════════════════════════════════════════╗');
  console.log('║                              SUMMARY REPORT                                        ║');
  console.log('╚════════════════════════════════════════════════════════════════════════════════════╝\n');
  
  const headers = ['Exchange', 'Spot', 'Futures', 'Orderbook', 'Ticker', 'Trades', 'Kline', 'OI'];
  console.log('┌' + '─'.repeat(12) + '┬' + '─'.repeat(6) + '┬' + '─'.repeat(8) + '┬' + '─'.repeat(10) + '┬' + '─'.repeat(8) + '┬' + '─'.repeat(8) + '┬' + '─'.repeat(7) + '┬' + '─'.repeat(5) + '┐');
  console.log('│ ' + headers.map((h, i) => h.padEnd([10, 4, 6, 8, 6, 6, 5, 3][i])).join(' │ ') + ' │');
  console.log('├' + '─'.repeat(12) + '┼' + '─'.repeat(6) + '┼' + '─'.repeat(8) + '┼' + '─'.repeat(10) + '┼' + '─'.repeat(8) + '┼' + '─'.repeat(8) + '┼' + '─'.repeat(7) + '┼' + '─'.repeat(5) + '┤');
  
  for (const [key, data] of Object.entries(results)) {
    const tests = data.tests;
    const hasSpotOB = tests.spot_orderbook?.success ? '✅' : (tests.spot_orderbook ? '❌' : '—');
    const hasFuturesOB = tests.futures_orderbook?.success ? '✅' : (tests.futures_orderbook ? '❌' : '—');
    const hasTicker = (tests.spot_ticker?.success || tests.futures_ticker?.success) ? '✅' : '❌';
    const hasTrades = (tests.spot_trades?.success || tests.futures_trades?.success) ? '✅' : '❌';
    const hasKline = (tests.spot_kline?.success || tests.futures_kline?.success) ? '✅' : '❌';
    const hasOI = tests.futures_openinterest?.success ? '✅' : '—';
    
    const orderbook = `${hasSpotOB}/${hasFuturesOB}`;
    
    console.log(`│ ${data.name.padEnd(10)} │ ${data.spot ? '✅' : '❌'}    │ ${data.futures ? '✅' : '❌'}      │ ${orderbook.padEnd(8)} │ ${hasTicker.padEnd(6)} │ ${hasTrades.padEnd(6)} │ ${hasKline.padEnd(5)} │ ${hasOI.padEnd(3)} │`);
  }
  
  console.log('└' + '─'.repeat(12) + '┴' + '─'.repeat(6) + '┴' + '─'.repeat(8) + '┴' + '─'.repeat(10) + '┴' + '─'.repeat(8) + '┴' + '─'.repeat(8) + '┴' + '─'.repeat(7) + '┴' + '─'.repeat(5) + '┘');
  
  console.log('\nLegend: ✅ = Working | ❌ = Failed | — = Not tested/available');
  console.log('Orderbook format: Spot/Futures\n');
  
  // Save results to file
  const fs = require('fs');
  fs.writeFileSync('exchange-test-results.json', JSON.stringify(results, null, 2));
  console.log('📁 Detailed results saved to: exchange-test-results.json');
}

// Run the tests
runExchangeTests().then(() => {
  console.log('\n✅ All tests completed!');
  process.exit(0);
}).catch(error => {
  console.error('Error running tests:', error);
  process.exit(1);
});
