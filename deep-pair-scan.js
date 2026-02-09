/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║   DEEP PAIR SCANNER — Checks real availability of 9 canonical pairs     ║
 * ║   across all 48 exchanges using live API probes                         ║
 * ║   Pairs: BTC/ETH/SOL × USDT/USD/USDC                                   ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */

const https = require('https');
const ccxt = require('ccxt');

// ── 9 canonical pairs to check ──
const PAIRS = [
    'BTC_USDT', 'BTC_USD', 'BTC_USDC',
    'ETH_USDT', 'ETH_USD', 'ETH_USDC',
    'SOL_USDT', 'SOL_USD', 'SOL_USDC',
];

function httpsGet(url) {
    return new Promise((resolve, reject) => {
        const u = new URL(url);
        const opts = {
            hostname: u.hostname, port: u.port || 443,
            path: u.pathname + u.search, method: 'GET',
            headers: { 'User-Agent': 'Mozilla/5.0', 'Content-Type': 'application/json' },
            timeout: 10000,
        };
        const req = https.request(opts, (res) => {
            let data = ''; res.on('data', c => data += c);
            res.on('end', () => resolve({ status: res.statusCode, data }));
        });
        req.on('error', reject);
        req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
        req.end();
    });
}

// Returns true if a 200-response with valid JSON trade data (at least one trade) exists
async function probeRest(url, validator) {
    try {
        const r = await httpsGet(url);
        if (r.status !== 200) return false;
        const j = JSON.parse(r.data);
        return validator(j);
    } catch { return false; }
}

// ── Exchange probe definitions ──
// Each returns a map of canonical_pair -> true/false
// We probe the orderbook endpoint (lighter than trades for most exchanges)

const EXCHANGE_PROBES = {
    'Binance': async () => {
        const syms = {
            'BTC_USDT': 'BTCUSDT', 'BTC_USD': 'BTCUSD', 'BTC_USDC': 'BTCUSDC',
            'ETH_USDT': 'ETHUSDT', 'ETH_USD': 'ETHUSD', 'ETH_USDC': 'ETHUSDC',
            'SOL_USDT': 'SOLUSDT', 'SOL_USD': 'SOLUSD', 'SOL_USDC': 'SOLUSDC',
        };
        return probeSymbols(syms, s => `https://api.binance.com/api/v3/ticker/price?symbol=${s}`,
            j => j && j.price !== undefined);
    },

    'Coinbase': async () => {
        const syms = {
            'BTC_USDT': 'BTC-USDT', 'BTC_USD': 'BTC-USD', 'BTC_USDC': 'BTC-USDC',
            'ETH_USDT': 'ETH-USDT', 'ETH_USD': 'ETH-USD', 'ETH_USDC': 'ETH-USDC',
            'SOL_USDT': 'SOL-USDT', 'SOL_USD': 'SOL-USD', 'SOL_USDC': 'SOL-USDC',
        };
        return probeSymbols(syms, s => `https://api.exchange.coinbase.com/products/${s}/ticker`,
            j => j && j.price !== undefined);
    },

    'Kraken': async () => {
        // Kraken uses XBT for BTC
        const syms = {
            'BTC_USDT': 'XBTUSDT', 'BTC_USD': 'XBTUSD', 'BTC_USDC': 'XBTUSDC',
            'ETH_USDT': 'ETHUSDT', 'ETH_USD': 'ETHUSD', 'ETH_USDC': 'ETHUSDC',
            'SOL_USDT': 'SOLUSDT', 'SOL_USD': 'SOLUSD', 'SOL_USDC': 'SOLUSDC',
        };
        return probeSymbols(syms, s => `https://api.kraken.com/0/public/Ticker?pair=${s}`,
            j => j && j.error && j.error.length === 0 && j.result && Object.keys(j.result).length > 0);
    },

    'OKX': async () => {
        const syms = {
            'BTC_USDT': 'BTC-USDT', 'BTC_USD': 'BTC-USD', 'BTC_USDC': 'BTC-USDC',
            'ETH_USDT': 'ETH-USDT', 'ETH_USD': 'ETH-USD', 'ETH_USDC': 'ETH-USDC',
            'SOL_USDT': 'SOL-USDT', 'SOL_USD': 'SOL-USD', 'SOL_USDC': 'SOL-USDC',
        };
        return probeSymbols(syms, s => `https://www.okx.com/api/v5/market/ticker?instId=${s}`,
            j => j && j.data && j.data.length > 0);
    },

    'Bybit': async () => {
        const syms = {
            'BTC_USDT': 'BTCUSDT', 'BTC_USD': 'BTCUSD', 'BTC_USDC': 'BTCUSDC',
            'ETH_USDT': 'ETHUSDT', 'ETH_USD': 'ETHUSD', 'ETH_USDC': 'ETHUSDC',
            'SOL_USDT': 'SOLUSDT', 'SOL_USD': 'SOLUSD', 'SOL_USDC': 'SOLUSDC',
        };
        return probeSymbols(syms, s => `https://api.bybit.com/v5/market/tickers?category=spot&symbol=${s}`,
            j => j && j.result && j.result.list && j.result.list.length > 0);
    },

    'Gate.io': async () => {
        const syms = {
            'BTC_USDT': 'BTC_USDT', 'BTC_USD': 'BTC_USD', 'BTC_USDC': 'BTC_USDC',
            'ETH_USDT': 'ETH_USDT', 'ETH_USD': 'ETH_USD', 'ETH_USDC': 'ETH_USDC',
            'SOL_USDT': 'SOL_USDT', 'SOL_USD': 'SOL_USD', 'SOL_USDC': 'SOL_USDC',
        };
        return probeSymbols(syms, s => `https://api.gateio.ws/api/v4/spot/tickers?currency_pair=${s}`,
            j => Array.isArray(j) && j.length > 0 && j[0].last !== undefined);
    },

    'HTX': async () => {
        const syms = {
            'BTC_USDT': 'btcusdt', 'BTC_USD': 'btcusd', 'BTC_USDC': 'btcusdc',
            'ETH_USDT': 'ethusdt', 'ETH_USD': 'ethusd', 'ETH_USDC': 'ethusdc',
            'SOL_USDT': 'solusdt', 'SOL_USD': 'solusd', 'SOL_USDC': 'solusdc',
        };
        return probeSymbols(syms, s => `https://api.huobi.pro/market/detail/merged?symbol=${s}`,
            j => j && j.status === 'ok' && j.tick);
    },

    'Bitfinex': async () => {
        const syms = {
            'BTC_USDT': 'tBTCUST', 'BTC_USD': 'tBTCUSD', 'BTC_USDC': 'tBTCUDC',
            'ETH_USDT': 'tETHUST', 'ETH_USD': 'tETHUSD', 'ETH_USDC': 'tETHUDC',
            'SOL_USDT': 'tSOLUST', 'SOL_USD': 'tSOLUSD', 'SOL_USDC': 'tSOLUDC',
        };
        return probeSymbols(syms, s => `https://api-pub.bitfinex.com/v2/ticker/${s}`,
            j => Array.isArray(j) && j.length >= 10);
    },

    'KuCoin': async () => {
        const syms = {
            'BTC_USDT': 'BTC-USDT', 'BTC_USD': 'BTC-USD', 'BTC_USDC': 'BTC-USDC',
            'ETH_USDT': 'ETH-USDT', 'ETH_USD': 'ETH-USD', 'ETH_USDC': 'ETH-USDC',
            'SOL_USDT': 'SOL-USDT', 'SOL_USD': 'SOL-USD', 'SOL_USDC': 'SOL-USDC',
        };
        return probeSymbols(syms, s => `https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=${s}`,
            j => j && j.code === '200000' && j.data && j.data.price);
    },

    'WOO X': async () => {
        const syms = {
            'BTC_USDT': 'SPOT_BTC_USDT', 'BTC_USD': 'SPOT_BTC_USD', 'BTC_USDC': 'SPOT_BTC_USDC',
            'ETH_USDT': 'SPOT_ETH_USDT', 'ETH_USD': 'SPOT_ETH_USD', 'ETH_USDC': 'SPOT_ETH_USDC',
            'SOL_USDT': 'SPOT_SOL_USDT', 'SOL_USD': 'SPOT_SOL_USD', 'SOL_USDC': 'SPOT_SOL_USDC',
        };
        return probeSymbols(syms, s => `https://api.woo.org/v1/public/market_trades?symbol=${s}&limit=1`,
            j => j && j.success === true && j.rows && j.rows.length > 0);
    },

    'Bitstamp': async () => {
        const syms = {
            'BTC_USDT': 'btcusdt', 'BTC_USD': 'btcusd', 'BTC_USDC': 'btcusdc',
            'ETH_USDT': 'ethusdt', 'ETH_USD': 'ethusd', 'ETH_USDC': 'ethusdc',
            'SOL_USDT': 'solusdt', 'SOL_USD': 'solusd', 'SOL_USDC': 'solusdc',
        };
        return probeSymbols(syms, s => `https://www.bitstamp.net/api/v2/ticker/${s}/`,
            j => j && j.last !== undefined && !j.status);
    },

    'Crypto.com': async () => {
        const syms = {
            'BTC_USDT': 'BTC_USDT', 'BTC_USD': 'BTC_USD', 'BTC_USDC': 'BTC_USDC',
            'ETH_USDT': 'ETH_USDT', 'ETH_USD': 'ETH_USD', 'ETH_USDC': 'ETH_USDC',
            'SOL_USDT': 'SOL_USDT', 'SOL_USD': 'SOL_USD', 'SOL_USDC': 'SOL_USDC',
        };
        return probeSymbols(syms, s => `https://api.crypto.com/exchange/v1/public/get-tickers?instrument_name=${s}`,
            j => j && j.result && j.result.data && j.result.data.length > 0);
    },

    'WhiteBIT': async () => {
        const syms = {
            'BTC_USDT': 'BTC_USDT', 'BTC_USD': 'BTC_USD', 'BTC_USDC': 'BTC_USDC',
            'ETH_USDT': 'ETH_USDT', 'ETH_USD': 'ETH_USD', 'ETH_USDC': 'ETH_USDC',
            'SOL_USDT': 'SOL_USDT', 'SOL_USD': 'SOL_USD', 'SOL_USDC': 'SOL_USDC',
        };
        return probeSymbols(syms, s => `https://whitebit.com/api/v4/public/ticker?market=${s}`,
            j => {
                // WhiteBIT ticker endpoint returns all tickers
                // For per-pair, check orderbook
                return false; // fallback to alternative
            });
    },

    'AscendEX': async () => {
        const syms = {
            'BTC_USDT': 'BTC/USDT', 'BTC_USD': 'BTC/USD', 'BTC_USDC': 'BTC/USDC',
            'ETH_USDT': 'ETH/USDT', 'ETH_USD': 'ETH/USD', 'ETH_USDC': 'ETH/USDC',
            'SOL_USDT': 'SOL/USDT', 'SOL_USD': 'SOL/USD', 'SOL_USDC': 'SOL/USDC',
        };
        return probeSymbols(syms, s => `https://ascendex.com/api/pro/v1/ticker?symbol=${s}`,
            j => j && j.code === 0 && j.data);
    },

    'BingX': async () => {
        const syms = {
            'BTC_USDT': 'BTC-USDT', 'BTC_USD': 'BTC-USD', 'BTC_USDC': 'BTC-USDC',
            'ETH_USDT': 'ETH-USDT', 'ETH_USD': 'ETH-USD', 'ETH_USDC': 'ETH-USDC',
            'SOL_USDT': 'SOL-USDT', 'SOL_USD': 'SOL-USD', 'SOL_USDC': 'SOL-USDC',
        };
        return probeSymbols(syms, s => `https://open-api.bingx.com/openApi/spot/v1/ticker/24hr?symbol=${s}`,
            j => j && j.code === 0 && j.data);
    },
};

// Helper: probe each symbol in parallel
async function probeSymbols(syms, urlFn, validator) {
    const result = {};
    const entries = Object.entries(syms);
    const promises = entries.map(async ([canonical, sym]) => {
        result[canonical] = await probeRest(urlFn(sym), validator);
    });
    await Promise.all(promises);
    return result;
}

// ── CCXT-based probe (for all exchanges CCXT supports) ──
async function ccxtProbe(exchangeId, label) {
    const result = {};
    for (const p of PAIRS) result[p] = false;

    try {
        const exchange = new ccxt[exchangeId]({ enableRateLimit: true, timeout: 15000 });
        await exchange.loadMarkets();

        const pairMap = {
            'BTC_USDT': 'BTC/USDT', 'BTC_USD': 'BTC/USD', 'BTC_USDC': 'BTC/USDC',
            'ETH_USDT': 'ETH/USDT', 'ETH_USD': 'ETH/USD', 'ETH_USDC': 'ETH/USDC',
            'SOL_USDT': 'SOL/USDT', 'SOL_USD': 'SOL/USD', 'SOL_USDC': 'SOL/USDC',
        };

        for (const [canonical, ccxtSym] of Object.entries(pairMap)) {
            if (exchange.markets[ccxtSym]) {
                const m = exchange.markets[ccxtSym];
                // Check it's a spot market and active
                if (m.active !== false && (m.type === 'spot' || !m.type)) {
                    result[canonical] = true;
                }
            }
        }
    } catch (e) {
        process.stderr.write(`  ⚠️  CCXT ${label} (${exchangeId}): ${e.message.slice(0, 80)}\n`);
    }
    return result;
}

// ── Master exchange list: how to probe each one ──
// Uses CCXT loadMarkets() for maximum accuracy (checks the market list, not just a price endpoint)
const ALL_EXCHANGES = [
    // Tier 1
    { name: 'Binance',     ccxtId: 'binance' },
    { name: 'Coinbase',    ccxtId: 'coinbase' },
    { name: 'Kraken',      ccxtId: 'kraken' },
    { name: 'KuCoin',      ccxtId: 'kucoin' },
    { name: 'OKX',         ccxtId: 'okx' },
    { name: 'Bybit',       ccxtId: 'bybit' },
    { name: 'Bitfinex',    ccxtId: 'bitfinex' },
    { name: 'Gate.io',     ccxtId: 'gateio' },
    { name: 'HTX',         ccxtId: 'htx' },
    { name: 'WOO X',       ccxtId: 'woo' },
    // Tier 2
    { name: 'Bitstamp',    ccxtId: 'bitstamp' },
    { name: 'Crypto.com',  ccxtId: 'cryptocom' },
    { name: 'WhiteBIT',    ccxtId: 'whitebit' },
    { name: 'AscendEX',    ccxtId: 'ascendex' },
    { name: 'BingX',       ccxtId: 'bingx' },
    { name: 'Toobit',      ccxtId: 'toobit' },
    { name: 'Deepcoin',    ccxtId: 'deepcoin' },     // may not be in CCXT
    { name: 'XT.com',      ccxtId: 'xt' },
    { name: 'Zoomex',      ccxtId: 'zoomex' },        // may not be in CCXT
    { name: 'LBank',       ccxtId: 'lbank' },
    { name: 'BitMart',     ccxtId: 'bitmart' },
    { name: 'Pionex',      ccxtId: 'pionex' },        // may not be in CCXT
    { name: 'Poloniex',    ccxtId: 'poloniex' },
    { name: 'BTSE',        ccxtId: 'btse' },           // may not be in CCXT
    { name: 'HitBTC',      ccxtId: 'hitbtc' },
    { name: 'Biconomy',    ccxtId: 'biconomy' },       // may not be in CCXT
    { name: 'Hotcoin',     ccxtId: 'hotcoin' },        // may not be in CCXT
    { name: 'NovaEx',      ccxtId: null },              // Not in CCXT
    { name: 'FameEX',      ccxtId: null },              // Not in CCXT
    { name: 'Websea',      ccxtId: null },              // Not in CCXT
    { name: 'Bullish',     ccxtId: 'bullish' },         // may not be in CCXT
    { name: 'Darkex',      ccxtId: null },              // Not in CCXT
    { name: 'Bitrue',      ccxtId: 'bitrue' },
    { name: 'BloFin',      ccxtId: 'blofin' },
    { name: 'OrangeX',     ccxtId: null },              // Not in CCXT
    { name: 'Azbit',       ccxtId: null },              // Not in CCXT
    { name: 'BVOX',        ccxtId: null },              // Not in CCXT
    { name: 'Trubit Pro',  ccxtId: null },              // Not in CCXT
    // CCXT-native exchanges
    { name: 'Bitget',      ccxtId: 'bitget' },
    { name: 'MEXC',        ccxtId: 'mexc' },
    { name: 'Gemini',      ccxtId: 'gemini' },
    { name: 'Binance.US',  ccxtId: 'binanceus' },
    { name: 'CEX.IO',      ccxtId: 'cex' },
    { name: 'CoinEx',      ccxtId: 'coinex' },
    { name: 'DigiFinex',   ccxtId: 'digifinex' },
    { name: 'BigONE',      ccxtId: 'bigone' },
    { name: 'EXMO',        ccxtId: 'exmo' },
    { name: 'LATOKEN',     ccxtId: 'latoken' },
];

// ── Fallback REST probes for exchanges not in CCXT ──
const REST_FALLBACKS = {
    'NovaEx': async () => {
        // WOO X clone — uses same API structure, only USDT
        const r = {}; for (const p of PAIRS) r[p] = false;
        for (const base of ['BTC','ETH','SOL']) {
            const sym = `SPOT_${base}_USDT`;
            r[`${base}_USDT`] = await probeRest(
                `https://api.novaex.io/v1/public/market_trades?symbol=${sym}&limit=1`,
                j => j && j.success === true && j.rows && j.rows.length > 0);
        }
        return r;
    },
    'FameEX': async () => {
        const r = {}; for (const p of PAIRS) r[p] = false;
        for (const base of ['BTC','ETH','SOL']) {
            for (const quote of ['USDT','USD','USDC']) {
                const sym = `${base}${quote}`;
                r[`${base}_${quote}`] = await probeRest(
                    `https://api.fameex.com/v2/public/ticker/price?symbol=${sym}`,
                    j => j && j.code === '200' && j.data);
            }
        }
        return r;
    },
    'Websea': async () => {
        const r = {}; for (const p of PAIRS) r[p] = false;
        for (const base of ['BTC','ETH','SOL']) {
            for (const quote of ['USDT','USD','USDC']) {
                const sym = `${base}-${quote}`;
                r[`${base}_${quote}`] = await probeRest(
                    `https://api.websea.com/openApi/market/trade?symbol=${sym}&limit=1`,
                    j => j && j.code === '0' && j.data && j.data.length > 0);
            }
        }
        return r;
    },
    'Darkex': async () => {
        const r = {}; for (const p of PAIRS) r[p] = false;
        for (const base of ['BTC','ETH','SOL']) {
            for (const quote of ['USDT','USD','USDC']) {
                const sym = `${base}${quote}`;
                r[`${base}_${quote}`] = await probeRest(
                    `https://api.darkex.com/api/v1/spot/ticker?symbol=${sym}`,
                    j => j && j.code === 0 && j.data);
            }
        }
        return r;
    },
    'OrangeX': async () => {
        const r = {}; for (const p of PAIRS) r[p] = false;
        for (const base of ['BTC','ETH','SOL']) {
            for (const quote of ['USDT','USD','USDC']) {
                const sym = `${base}-${quote}`;
                r[`${base}_${quote}`] = await probeRest(
                    `https://api.orangex.com/api/v1/public/get_book_summary_by_instrument?instrument_name=${sym}`,
                    j => j && j.result && j.result.length > 0);
            }
        }
        return r;
    },
    'Azbit': async () => {
        const r = {}; for (const p of PAIRS) r[p] = false;
        // Azbit has a currencies/pairs list endpoint
        try {
            const resp = await httpsGet('https://data.azbit.com/api/pairs');
            if (resp.status === 200) {
                const pairs = JSON.parse(resp.data);
                const pairSet = new Set();
                if (Array.isArray(pairs)) {
                    for (const p of pairs) {
                        if (p.pairName) pairSet.add(p.pairName.toUpperCase());
                        if (p.name) pairSet.add(p.name.toUpperCase());
                    }
                }
                for (const base of ['BTC','ETH','SOL']) {
                    for (const quote of ['USDT','USD','USDC']) {
                        r[`${base}_${quote}`] = pairSet.has(`${base}_${quote}`) || pairSet.has(`${base}${quote}`) || pairSet.has(`${base}/${quote}`);
                    }
                }
            }
        } catch {}
        return r;
    },
    'BVOX': async () => {
        const r = {}; for (const p of PAIRS) r[p] = false;
        for (const base of ['BTC','ETH','SOL']) {
            for (const quote of ['USDT','USD','USDC']) {
                const sym = `${base}${quote}`;
                r[`${base}_${quote}`] = await probeRest(
                    `https://api.bvox.com/open/api/v1/market/ticker?symbol=${sym.toLowerCase()}`,
                    j => j && (j.code === '0' || j.code === 0) && j.data);
            }
        }
        return r;
    },
    'Trubit Pro': async () => {
        const r = {}; for (const p of PAIRS) r[p] = false;
        for (const base of ['BTC','ETH','SOL']) {
            for (const quote of ['USDT','USD','USDC']) {
                const sym = `${base}${quote}`;
                r[`${base}_${quote}`] = await probeRest(
                    `https://api.trubit.com/openapi/quote/v1/ticker/price?symbol=${sym}`,
                    j => j && j.price !== undefined);
            }
        }
        return r;
    },
};

// ── Main ──
async function main() {
    console.log('╔══════════════════════════════════════════════════════════════╗');
    console.log('║   DEEP PAIR SCANNER — 48 Exchanges × 9 Canonical Pairs     ║');
    console.log('╚══════════════════════════════════════════════════════════════╝\n');

    // Check which CCXT exchange IDs are valid
    const ccxtExchanges = ccxt.exchanges;
    const ccxtSet = new Set(ccxtExchanges);

    const results = {}; // { exchangeName: { BTC_USDT: true/false, ... } }
    let scanned = 0;
    const total = ALL_EXCHANGES.length;

    // Process in batches of 4 to avoid rate limiting
    for (let i = 0; i < ALL_EXCHANGES.length; i += 4) {
        const batch = ALL_EXCHANGES.slice(i, i + 4);
        const promises = batch.map(async (ex) => {
            const name = ex.name;
            process.stdout.write(`  [${++scanned}/${total}] Scanning ${name}...`);

            let r;
            if (ex.ccxtId && ccxtSet.has(ex.ccxtId)) {
                r = await ccxtProbe(ex.ccxtId, name);
            } else if (REST_FALLBACKS[name]) {
                r = await REST_FALLBACKS[name]();
            } else {
                // No probe available — mark all as unknown (false)
                r = {};
                for (const p of PAIRS) r[p] = false;
                process.stderr.write(` (no CCXT/REST probe available)`);
            }
            results[name] = r;

            const yesCount = PAIRS.filter(p => r[p]).length;
            process.stdout.write(` ${yesCount}/9 pairs\n`);
        });
        await Promise.all(promises);
        // Small delay between batches
        if (i + 4 < ALL_EXCHANGES.length) await new Promise(r => setTimeout(r, 500));
    }

    // ══════════════════════════════════════════════════════════════
    // ── OUTPUT: Main Table ──
    // ══════════════════════════════════════════════════════════════
    console.log('\n\n══════════════════════════════════════════════════════════════════════════════════════════════════');
    console.log('DEEP PAIR AVAILABILITY MATRIX');
    console.log('══════════════════════════════════════════════════════════════════════════════════════════════════\n');

    const hdr = 'Exchange'.padEnd(16) + PAIRS.map(p => p.padEnd(10)).join('') + 'Total';
    console.log(hdr);
    console.log('─'.repeat(hdr.length));

    const sortedNames = ALL_EXCHANGES.map(e => e.name);
    for (const name of sortedNames) {
        const r = results[name];
        let totalYes = 0;
        let line = name.padEnd(16);
        for (const p of PAIRS) {
            const has = r[p] ? true : false;
            if (has) totalYes++;
            line += (has ? '  ✅  yes ' : '  ❌  no  ');
        }
        line += ` ${totalYes}/9`;
        console.log(line);
    }

    console.log('─'.repeat(hdr.length));

    // ── Pair support totals ──
    console.log('\n── Exchanges Supporting Each Pair ──\n');
    for (const p of PAIRS) {
        const count = sortedNames.filter(n => results[n][p]).length;
        const names = sortedNames.filter(n => results[n][p]);
        console.log(`  ${p.padEnd(12)} ${String(count).padStart(2)} exchanges: ${names.join(', ')}`);
    }

    // ── Primary pair per exchange ──
    console.log('\n── Primary Pair per Exchange (USDT > USD > USDC) ──\n');
    for (const name of sortedNames) {
        const r = results[name];
        let primary = 'NONE';
        if (r['BTC_USDT']) primary = 'BTC_USDT';
        else if (r['BTC_USD']) primary = 'BTC_USD';
        else if (r['BTC_USDC']) primary = 'BTC_USDC';
        const allPairs = PAIRS.filter(p => r[p]);
        console.log(`  ${name.padEnd(16)} Primary: ${primary.padEnd(12)} All: ${allPairs.join(', ') || '(none)'}`);
    }

    // ══════════════════════════════════════════════════════════════
    // ── INTELLIGENCE OUTPUT ──
    // ══════════════════════════════════════════════════════════════
    console.log('\n\n══════════════════════════════════════════════════════════════════════════════════════════════════');
    console.log('INTELLIGENCE ANALYSIS');
    console.log('══════════════════════════════════════════════════════════════════════════════════════════════════\n');

    console.log(`  TOTAL exchanges scanned: ${total}\n`);

    // Quote currency popularity
    const quoteCount = { USDT: 0, USD: 0, USDC: 0 };
    for (const name of sortedNames) {
        const r = results[name];
        for (const p of PAIRS) {
            if (r[p]) {
                const quote = p.split('_')[1];
                quoteCount[quote]++;
            }
        }
    }
    console.log('── Quote Currency Popularity (total pair-exchange combinations) ──\n');
    const sortedQuotes = Object.entries(quoteCount).sort((a, b) => b[1] - a[1]);
    for (const [quote, count] of sortedQuotes) {
        const bar = '█'.repeat(Math.round(count / 2));
        console.log(`  ${quote.padEnd(6)} ${String(count).padStart(3)} ${bar}`);
    }

    // Exchanges with only USD pairs
    console.log('\n── Exchanges with ONLY USD pairs (no USDT, no USDC) ──\n');
    const usdOnly = sortedNames.filter(n => {
        const r = results[n];
        const hasUsd = PAIRS.filter(p => p.includes('_USD') && !p.includes('USDT') && !p.includes('USDC')).some(p => r[p]);
        const hasUsdt = PAIRS.filter(p => p.includes('_USDT')).some(p => r[p]);
        const hasUsdc = PAIRS.filter(p => p.includes('_USDC')).some(p => r[p]);
        return hasUsd && !hasUsdt && !hasUsdc;
    });
    console.log(`  ${usdOnly.length > 0 ? usdOnly.join(', ') : '(none)'}`);

    // Exchanges with only USDT pairs
    console.log('\n── Exchanges with ONLY USDT pairs (no USD, no USDC) ──\n');
    const usdtOnly = sortedNames.filter(n => {
        const r = results[n];
        const hasUsdt = PAIRS.filter(p => p.includes('_USDT')).some(p => r[p]);
        const hasUsd = PAIRS.filter(p => p.includes('_USD') && !p.includes('USDT') && !p.includes('USDC')).some(p => r[p]);
        const hasUsdc = PAIRS.filter(p => p.includes('_USDC')).some(p => r[p]);
        return hasUsdt && !hasUsd && !hasUsdc;
    });
    console.log(`  ${usdtOnly.length > 0 ? usdtOnly.join(', ') : '(none)'}`);

    // Exchanges supporting all 3 assets fully (BTC+ETH+SOL in all quotes they support)
    console.log('\n── Exchanges supporting ALL 3 assets fully (BTC+ETH+SOL each with same quotes) ──\n');
    const fullAssetSupport = sortedNames.filter(n => {
        const r = results[n];
        for (const quote of ['USDT', 'USD', 'USDC']) {
            const btc = r[`BTC_${quote}`];
            const eth = r[`ETH_${quote}`];
            const sol = r[`SOL_${quote}`];
            // If any base has the quote, all 3 must have it
            if ((btc || eth || sol) && !(btc && eth && sol)) return false;
        }
        // Must have at least one pair
        return PAIRS.some(p => r[p]);
    });
    console.log(`  ${fullAssetSupport.length} exchanges: ${fullAssetSupport.join(', ')}`);

    // Exchanges supporting ALL 9 pairs
    console.log('\n── Exchanges supporting ALL 9 pairs (full coverage) ──\n');
    const full9 = sortedNames.filter(n => PAIRS.every(p => results[n][p]));
    console.log(`  ${full9.length} exchanges: ${full9.length > 0 ? full9.join(', ') : '(none)'}`);

    // Summary counts
    console.log('\n── Coverage Summary ──\n');
    const buckets = { 9: [], 6: [], 3: [], 0: [] };
    for (const name of sortedNames) {
        const count = PAIRS.filter(p => results[name][p]).length;
        if (count === 9) buckets[9].push(name);
        else if (count >= 6) buckets[6].push(name);
        else if (count >= 1) buckets[3].push(name);
        else buckets[0].push(name);
    }
    console.log(`  9/9 (full):    ${buckets[9].length} exchanges`);
    console.log(`  6-8 (broad):   ${buckets[6].length} exchanges`);
    console.log(`  1-5 (partial): ${buckets[3].length} exchanges`);
    console.log(`  0   (none):    ${buckets[0].length} exchanges`);

    console.log('\n══════════════════════════════════════════════════════════════════════════════════════════════════');
    console.log('Scan complete.');
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
