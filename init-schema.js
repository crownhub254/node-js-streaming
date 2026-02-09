/**
 * ╔══════════════════════════════════════════════════════════════════════════╗
 * ║   CANONICAL PAIR SYSTEM — DuckDB Schema Initialization                  ║
 * ║   Creates: assets, symbol_map, trades, orderbook tables                 ║
 * ║   Populates: assets + symbol_map for 48 exchanges × BTC/ETH/SOL        ║
 * ║   Quote currencies: USD, USDT, USDC, DAI                                ║
 * ╚══════════════════════════════════════════════════════════════════════════╝
 */

const { DuckDBInstance } = require('@duckdb/node-api');

async function main() {
    const instance = await DuckDBInstance.create('streaming.duckdb');
    const conn = await instance.connect();

    console.log('╔══════════════════════════════════════════════════════════╗');
    console.log('║  CANONICAL PAIR SYSTEM — Schema Init                    ║');
    console.log('╚══════════════════════════════════════════════════════════╝\n');

    // ═══════════════ DROP EXISTING ═══════════════
    console.log('  [1/5] Dropping existing tables...');
    await conn.run('DROP TABLE IF EXISTS trades');
    await conn.run('DROP TABLE IF EXISTS orderbook');
    await conn.run('DROP TABLE IF EXISTS symbol_map');
    await conn.run('DROP TABLE IF EXISTS assets');

    // ═══════════════ ASSETS TABLE ═══════════════
    console.log('  [2/5] Creating assets table...');
    await conn.run(`
        CREATE TABLE assets (
            asset TEXT PRIMARY KEY,
            asset_type TEXT NOT NULL,
            is_major BOOLEAN NOT NULL DEFAULT false,
            aliases TEXT[]
        )
    `);

    // Insert assets
    const assets = [
        ['BTC', 'crypto',     true,  ['XBT']],
        ['ETH', 'crypto',     true,  []],
        ['SOL', 'crypto',     true,  []],
        ['USDT','stablecoin', true,  ['UST']],
        ['USDC','stablecoin', true,  []],
        ['DAI', 'stablecoin', false, []],
        ['USD', 'fiat',       true,  []],

    ];

    for (const [asset, type, major, aliases] of assets) {
        const aliasStr = aliases.length > 0 ? `ARRAY[${aliases.map(a => `'${a}'`).join(',')}]` : `ARRAY[]::TEXT[]`;
        await conn.run(`INSERT INTO assets VALUES ('${asset}', '${type}', ${major}, ${aliasStr})`);
    }
    console.log(`         → ${assets.length} assets inserted`);

    // ═══════════════ SYMBOL MAP TABLE ═══════════════
    console.log('  [3/5] Creating symbol_map table...');
    await conn.run(`
        CREATE TABLE symbol_map (
            exchange        TEXT NOT NULL,
            exchange_symbol TEXT NOT NULL,
            base_asset      TEXT NOT NULL,
            quote_asset     TEXT NOT NULL,
            canonical_pair  TEXT NOT NULL,
            market_type     TEXT NOT NULL DEFAULT 'spot',
            is_active       BOOLEAN NOT NULL DEFAULT true,
            PRIMARY KEY (exchange, exchange_symbol)
        )
    `);

    // ═══════════════ POPULATE SYMBOL MAP ═══════════════
    // Format: [exchange, exchange_symbol, base, quote]
    // canonical_pair is auto-generated as BASE_QUOTE

    const symbolRows = [
        // ─────────── Binance ───────────
        ['Binance', 'BTCUSDT', 'BTC', 'USDT'],
        ['Binance', 'ETHUSDT', 'ETH', 'USDT'],
        ['Binance', 'SOLUSDT', 'SOL', 'USDT'],
        ['Binance', 'BTCUSDC', 'BTC', 'USDC'],
        ['Binance', 'ETHUSDC', 'ETH', 'USDC'],
        ['Binance', 'SOLUSDC', 'SOL', 'USDC'],
        ['Binance', 'BTCUSD',  'BTC', 'USD'],

        // ─────────── Coinbase ───────────
        ['Coinbase', 'BTC-USD',  'BTC', 'USD'],
        ['Coinbase', 'ETH-USD',  'ETH', 'USD'],
        ['Coinbase', 'SOL-USD',  'SOL', 'USD'],
        ['Coinbase', 'BTC-USDT', 'BTC', 'USDT'],
        ['Coinbase', 'ETH-USDT', 'ETH', 'USDT'],
        ['Coinbase', 'SOL-USDT', 'SOL', 'USDT'],

        // ─────────── Kraken ───────────
        ['Kraken', 'XBT/USDT', 'BTC', 'USDT'],
        ['Kraken', 'BTC/USDT', 'BTC', 'USDT'],   // V2 API uses BTC not XBT
        ['Kraken', 'ETH/USDT', 'ETH', 'USDT'],
        ['Kraken', 'SOL/USDT', 'SOL', 'USDT'],
        ['Kraken', 'XBT/USDC', 'BTC', 'USDC'],
        ['Kraken', 'ETH/USDC', 'ETH', 'USDC'],
        ['Kraken', 'SOL/USDC', 'SOL', 'USDC'],
        ['Kraken', 'SOL/USD',  'SOL', 'USD'],

        // ─────────── KuCoin ───────────
        ['KuCoin', 'BTC-USDT', 'BTC', 'USDT'],
        ['KuCoin', 'ETH-USDT', 'ETH', 'USDT'],
        ['KuCoin', 'SOL-USDT', 'SOL', 'USDT'],
        ['KuCoin', 'BTC-USDC', 'BTC', 'USDC'],
        ['KuCoin', 'ETH-USDC', 'ETH', 'USDC'],
        ['KuCoin', 'SOL-USDC', 'SOL', 'USDC'],

        // ─────────── OKX ───────────
        ['OKX', 'BTC-USDT', 'BTC', 'USDT'],
        ['OKX', 'ETH-USDT', 'ETH', 'USDT'],
        ['OKX', 'SOL-USDT', 'SOL', 'USDT'],
        ['OKX', 'BTC-USDC', 'BTC', 'USDC'],
        ['OKX', 'ETH-USDC', 'ETH', 'USDC'],
        ['OKX', 'SOL-USDC', 'SOL', 'USDC'],
        ['OKX', 'BTC-USD',  'BTC', 'USD'],
        ['OKX', 'ETH-USD',  'ETH', 'USD'],
        ['OKX', 'SOL-USD',  'SOL', 'USD'],

        // ─────────── Bybit ───────────
        ['Bybit', 'BTCUSDT', 'BTC', 'USDT'],
        ['Bybit', 'ETHUSDT', 'ETH', 'USDT'],
        ['Bybit', 'SOLUSDT', 'SOL', 'USDT'],
        ['Bybit', 'BTCUSDC', 'BTC', 'USDC'],
        ['Bybit', 'ETHUSDC', 'ETH', 'USDC'],
        ['Bybit', 'SOLUSDC', 'SOL', 'USDC'],

        // ─────────── Bitfinex ───────────
        ['Bitfinex', 'tBTCUSD', 'BTC', 'USD'],
        ['Bitfinex', 'tETHUSD', 'ETH', 'USD'],
        ['Bitfinex', 'tSOLUSD', 'SOL', 'USD'],
        ['Bitfinex', 'tBTCUST', 'BTC', 'USDT'],
        ['Bitfinex', 'tETHUST', 'ETH', 'USDT'],
        ['Bitfinex', 'tSOLUST', 'SOL', 'USDT'],

        // ─────────── Gate.io ───────────
        ['Gate.io', 'BTC_USDT', 'BTC', 'USDT'],
        ['Gate.io', 'ETH_USDT', 'ETH', 'USDT'],
        ['Gate.io', 'SOL_USDT', 'SOL', 'USDT'],
        ['Gate.io', 'BTC_USDC', 'BTC', 'USDC'],
        ['Gate.io', 'ETH_USDC', 'ETH', 'USDC'],
        ['Gate.io', 'SOL_USDC', 'SOL', 'USDC'],

        // ─────────── HTX ───────────
        ['HTX', 'btcusdt', 'BTC', 'USDT'],
        ['HTX', 'ethusdt', 'ETH', 'USDT'],
        ['HTX', 'solusdt', 'SOL', 'USDT'],
        ['HTX', 'btcusdc', 'BTC', 'USDC'],
        ['HTX', 'ethusdc', 'ETH', 'USDC'],

        // ─────────── WOO X ───────────
        ['WOO X', 'SPOT_BTC_USDT', 'BTC', 'USDT'],
        ['WOO X', 'SPOT_ETH_USDT', 'ETH', 'USDT'],
        ['WOO X', 'SPOT_SOL_USDT', 'SOL', 'USDT'],
        ['WOO X', 'SPOT_BTC_USDC', 'BTC', 'USDC'],
        ['WOO X', 'SPOT_ETH_USDC', 'ETH', 'USDC'],

        // ─────────── Crypto.com ───────────
        ['Crypto.com', 'BTC_USDT', 'BTC', 'USDT'],
        ['Crypto.com', 'ETH_USDT', 'ETH', 'USDT'],
        ['Crypto.com', 'SOL_USDT', 'SOL', 'USDT'],

        // ─────────── Bitstamp ───────────
        ['Bitstamp', 'btcusd',  'BTC', 'USD'],
        ['Bitstamp', 'ethusd',  'ETH', 'USD'],
        ['Bitstamp', 'solusd',  'SOL', 'USD'],
        ['Bitstamp', 'btcusdt', 'BTC', 'USDT'],
        ['Bitstamp', 'ethusdt', 'ETH', 'USDT'],
        ['Bitstamp', 'btcusdc', 'BTC', 'USDC'],
        ['Bitstamp', 'ethusdc', 'ETH', 'USDC'],
        ['Bitstamp', 'solusdc', 'SOL', 'USDC'],

        // ─────────── WhiteBIT ───────────
        ['WhiteBIT', 'BTC_USDT', 'BTC', 'USDT'],
        ['WhiteBIT', 'ETH_USDT', 'ETH', 'USDT'],
        ['WhiteBIT', 'SOL_USDT', 'SOL', 'USDT'],
        ['WhiteBIT', 'BTC_USDC', 'BTC', 'USDC'],
        ['WhiteBIT', 'ETH_USDC', 'ETH', 'USDC'],
        ['WhiteBIT', 'SOL_USDC', 'SOL', 'USDC'],
        ['WhiteBIT', 'BTC_USD',  'BTC', 'USD'],
        ['WhiteBIT', 'ETH_USD',  'ETH', 'USD'],
        ['WhiteBIT', 'SOL_USD',  'SOL', 'USD'],

        // ─────────── AscendEX ───────────
        ['AscendEX', 'BTC/USDT', 'BTC', 'USDT'],
        ['AscendEX', 'ETH/USDT', 'ETH', 'USDT'],
        ['AscendEX', 'SOL/USDT', 'SOL', 'USDT'],
        ['AscendEX', 'BTC/USD',  'BTC', 'USD'],
        ['AscendEX', 'ETH/USD',  'ETH', 'USD'],
        ['AscendEX', 'SOL/USD',  'SOL', 'USD'],

        // ─────────── BingX ───────────
        ['BingX', 'BTC-USDT', 'BTC', 'USDT'],
        ['BingX', 'ETH-USDT', 'ETH', 'USDT'],
        ['BingX', 'SOL-USDT', 'SOL', 'USDT'],
        ['BingX', 'BTC-USDC', 'BTC', 'USDC'],
        ['BingX', 'ETH-USDC', 'ETH', 'USDC'],
        ['BingX', 'SOL-USDC', 'SOL', 'USDC'],

        // ─────────── Toobit ───────────
        ['Toobit', 'BTCUSDT', 'BTC', 'USDT'],
        ['Toobit', 'ETHUSDT', 'ETH', 'USDT'],
        ['Toobit', 'SOLUSDT', 'SOL', 'USDT'],
        ['Toobit', 'BTCUSDC', 'BTC', 'USDC'],
        ['Toobit', 'ETHUSDC', 'ETH', 'USDC'],
        ['Toobit', 'SOLUSDC', 'SOL', 'USDC'],

        // ─────────── Deepcoin ───────────
        ['Deepcoin', 'BTC-USDT', 'BTC', 'USDT'],
        ['Deepcoin', 'ETH-USDT', 'ETH', 'USDT'],
        ['Deepcoin', 'SOL-USDT', 'SOL', 'USDT'],

        // ─────────── XT.com ───────────
        ['XT.com', 'btc_usdt', 'BTC', 'USDT'],
        ['XT.com', 'eth_usdt', 'ETH', 'USDT'],
        ['XT.com', 'sol_usdt', 'SOL', 'USDT'],
        ['XT.com', 'btc_usdc', 'BTC', 'USDC'],
        ['XT.com', 'eth_usdc', 'ETH', 'USDC'],
        ['XT.com', 'sol_usdc', 'SOL', 'USDC'],

        // ─────────── Zoomex ───────────
        ['Zoomex', 'BTCUSDT', 'BTC', 'USDT'],
        ['Zoomex', 'ETHUSDT', 'ETH', 'USDT'],
        ['Zoomex', 'SOLUSDT', 'SOL', 'USDT'],

        // ─────────── LBank ───────────
        ['LBank', 'btc_usdt', 'BTC', 'USDT'],
        ['LBank', 'eth_usdt', 'ETH', 'USDT'],
        ['LBank', 'sol_usdt', 'SOL', 'USDT'],
        ['LBank', 'btc_usdc', 'BTC', 'USDC'],

        // ─────────── BitMart ───────────
        ['BitMart', 'BTC_USDT', 'BTC', 'USDT'],
        ['BitMart', 'ETH_USDT', 'ETH', 'USDT'],
        ['BitMart', 'SOL_USDT', 'SOL', 'USDT'],
        ['BitMart', 'BTC_USDC', 'BTC', 'USDC'],
        ['BitMart', 'ETH_USDC', 'ETH', 'USDC'],
        ['BitMart', 'SOL_USDC', 'SOL', 'USDC'],

        // ─────────── Pionex ───────────
        ['Pionex', 'BTC_USDT', 'BTC', 'USDT'],
        ['Pionex', 'ETH_USDT', 'ETH', 'USDT'],
        ['Pionex', 'SOL_USDT', 'SOL', 'USDT'],
        ['Pionex', 'BTC_USDC', 'BTC', 'USDC'],
        ['Pionex', 'ETH_USDC', 'ETH', 'USDC'],

        // ─────────── Poloniex ───────────
        ['Poloniex', 'BTC_USDT', 'BTC', 'USDT'],
        ['Poloniex', 'ETH_USDT', 'ETH', 'USDT'],
        ['Poloniex', 'SOL_USDT', 'SOL', 'USDT'],
        ['Poloniex', 'BTC_USDC', 'BTC', 'USDC'],
        ['Poloniex', 'ETH_USDC', 'ETH', 'USDC'],

        // ─────────── BTSE ───────────
        ['BTSE', 'BTC-USD',  'BTC', 'USD'],
        ['BTSE', 'ETH-USD',  'ETH', 'USD'],
        ['BTSE', 'SOL-USD',  'SOL', 'USD'],
        ['BTSE', 'BTC-USDT', 'BTC', 'USDT'],
        ['BTSE', 'ETH-USDT', 'ETH', 'USDT'],
        ['BTSE', 'SOL-USDT', 'SOL', 'USDT'],
        ['BTSE', 'BTC-USDC', 'BTC', 'USDC'],
        ['BTSE', 'ETH-USDC', 'ETH', 'USDC'],
        ['BTSE', 'SOL-USDC', 'SOL', 'USDC'],

        // ─────────── HitBTC ───────────
        ['HitBTC', 'BTCUSDT', 'BTC', 'USDT'],
        ['HitBTC', 'ETHUSDT', 'ETH', 'USDT'],
        ['HitBTC', 'SOLUSDT', 'SOL', 'USDT'],
        ['HitBTC', 'BTCUSDC', 'BTC', 'USDC'],
        ['HitBTC', 'ETHUSDC', 'ETH', 'USDC'],
        ['HitBTC', 'SOLUSDC', 'SOL', 'USDC'],

        // ─────────── Biconomy ───────────
        ['Biconomy', 'BTC_USDT', 'BTC', 'USDT'],
        ['Biconomy', 'ETH_USDT', 'ETH', 'USDT'],
        ['Biconomy', 'SOL_USDT', 'SOL', 'USDT'],
        ['Biconomy', 'BTC_USDC', 'BTC', 'USDC'],
        ['Biconomy', 'ETH_USDC', 'ETH', 'USDC'],
        ['Biconomy', 'SOL_USDC', 'SOL', 'USDC'],

        // ─────────── Hotcoin ───────────
        ['Hotcoin', 'btc_usdt', 'BTC', 'USDT'],
        ['Hotcoin', 'eth_usdt', 'ETH', 'USDT'],
        ['Hotcoin', 'sol_usdt', 'SOL', 'USDT'],
        ['Hotcoin', 'btc_usdc', 'BTC', 'USDC'],
        ['Hotcoin', 'eth_usdc', 'ETH', 'USDC'],

        // ─────────── NovaEx ───────────
        ['NovaEx', 'SPOT_BTC_USDT', 'BTC', 'USDT'],
        ['NovaEx', 'SPOT_ETH_USDT', 'ETH', 'USDT'],
        ['NovaEx', 'SPOT_SOL_USDT', 'SOL', 'USDT'],

        // ─────────── FameEX (REST) ───────────
        ['FameEX', 'BTCUSDT', 'BTC', 'USDT'],
        ['FameEX', 'ETHUSDT', 'ETH', 'USDT'],
        ['FameEX', 'SOLUSDT', 'SOL', 'USDT'],
        ['FameEX', 'BTC_USDT', 'BTC', 'USDT'],
        ['FameEX', 'ETH_USDT', 'ETH', 'USDT'],
        ['FameEX', 'SOL_USDT', 'SOL', 'USDT'],

        // ─────────── Websea (REST) ───────────
        ['Websea', 'BTC-USDT', 'BTC', 'USDT'],
        ['Websea', 'ETH-USDT', 'ETH', 'USDT'],
        ['Websea', 'SOL-USDT', 'SOL', 'USDT'],

        // ─────────── Bullish (WS USDC pairs) ───────────
        ['Bullish', 'BTCUSDC', 'BTC', 'USDC'],
        ['Bullish', 'ETHUSDC', 'ETH', 'USDC'],
        ['Bullish', 'SOLUSDC', 'SOL', 'USDC'],

        // ─────────── Darkex (no SOL) ───────────
        ['Darkex', 'BTCUSDT', 'BTC', 'USDT'],
        ['Darkex', 'ETHUSDT', 'ETH', 'USDT'],

        // ─────────── Bitrue ───────────
        ['Bitrue', 'BTCUSDT', 'BTC', 'USDT'],
        ['Bitrue', 'ETHUSDT', 'ETH', 'USDT'],
        ['Bitrue', 'SOLUSDT', 'SOL', 'USDT'],

        // ─────────── BloFin ───────────
        ['BloFin', 'BTC-USDT', 'BTC', 'USDT'],
        ['BloFin', 'ETH-USDT', 'ETH', 'USDT'],
        ['BloFin', 'SOL-USDT', 'SOL', 'USDT'],

        // ─────────── OrangeX ───────────
        ['OrangeX', 'BTC-USDT', 'BTC', 'USDT'],
        ['OrangeX', 'ETH-USDT', 'ETH', 'USDT'],
        ['OrangeX', 'SOL-USDT', 'SOL', 'USDT'],

        // ─────────── Azbit ───────────
        ['Azbit', 'BTC_USDT', 'BTC', 'USDT'],
        ['Azbit', 'ETH_USDT', 'ETH', 'USDT'],
        ['Azbit', 'SOL_USDT', 'SOL', 'USDT'],

        // ─────────── BVOX ───────────
        ['BVOX', 'BTCUSDT', 'BTC', 'USDT'],
        ['BVOX', 'ETHUSDT', 'ETH', 'USDT'],
        ['BVOX', 'SOLUSDT', 'SOL', 'USDT'],

        // ─────────── Trubit Pro ───────────
        ['Trubit Pro', 'BTCUSDT', 'BTC', 'USDT'],
        ['Trubit Pro', 'ETHUSDT', 'ETH', 'USDT'],
        ['Trubit Pro', 'SOLUSDT', 'SOL', 'USDT'],

        // ─────────── Bitget (Native WS) ───────────
        ['Bitget', 'BTCUSDT', 'BTC', 'USDT'],
        ['Bitget', 'ETHUSDT', 'ETH', 'USDT'],
        ['Bitget', 'SOLUSDT', 'SOL', 'USDT'],

        // ─────────── Gemini (Native WS, USD pairs) ───────────
        ['Gemini', 'BTCUSD', 'BTC', 'USD'],
        ['Gemini', 'ETHUSD', 'ETH', 'USD'],

        // ─────────── Binance.US (Native WS) ───────────
        ['Binance.US', 'btcusdt', 'BTC', 'USDT'],
        ['Binance.US', 'ethusdt', 'ETH', 'USDT'],
        ['Binance.US', 'solusdt', 'SOL', 'USDT'],

        // ─────────── CEX.IO (CCXT WS) ───────────
        ['CEX.IO', 'BTC/USDT', 'BTC', 'USDT'],
        ['CEX.IO', 'ETH/USDT', 'ETH', 'USDT'],

        // ─────────── CoinEx (CCXT WS) ───────────
        ['CoinEx', 'BTC/USDT', 'BTC', 'USDT'],
        ['CoinEx', 'ETH/USDT', 'ETH', 'USDT'],
        ['CoinEx', 'SOL/USDT', 'SOL', 'USDT'],

        // ─────────── DigiFinex (Native WS) ───────────
        ['DigiFinex', 'BTC_USDT', 'BTC', 'USDT'],
        ['DigiFinex', 'ETH_USDT', 'ETH', 'USDT'],
        ['DigiFinex', 'SOL_USDT', 'SOL', 'USDT'],

        // ─────────── BigONE (CCXT REST) ───────────
        ['BigONE', 'BTC/USDT', 'BTC', 'USDT'],
        ['BigONE', 'ETH/USDT', 'ETH', 'USDT'],
        ['BigONE', 'SOL/USDT', 'SOL', 'USDT'],

        // ─────────── EXMO (Native WS) ───────────
        ['EXMO', 'BTC_USDT', 'BTC', 'USDT'],
        ['EXMO', 'ETH_USDT', 'ETH', 'USDT'],
        ['EXMO', 'SOL_USDT', 'SOL', 'USDT'],
        ['EXMO', 'BTC_DAI', 'BTC', 'DAI'],

        // ─────────── LATOKEN (CCXT REST) ───────────
        ['LATOKEN', 'BTC/USDT', 'BTC', 'USDT'],
        ['LATOKEN', 'ETH/USDT', 'ETH', 'USDT'],
        ['LATOKEN', 'SOL/USDT', 'SOL', 'USDT'],
    ];

    let inserted = 0;
    for (const [exchange, sym, base, quote] of symbolRows) {
        const canonical = `${base}_${quote}`;
        await conn.run(`INSERT INTO symbol_map VALUES ('${exchange}', '${sym}', '${base}', '${quote}', '${canonical}', 'spot', true)`);
        inserted++;
    }
    console.log(`         → ${inserted} symbol mappings inserted`);

    // ═══════════════ RAW DATA TABLES ═══════════════
    console.log('  [4/5] Creating raw data tables...');

    await conn.run(`
        CREATE TABLE trades (
            ts          TIMESTAMP NOT NULL,
            exchange    TEXT NOT NULL,
            canonical_pair TEXT NOT NULL,
            price       DOUBLE NOT NULL,
            qty         DOUBLE NOT NULL,
            side        TEXT
        )
    `);

    await conn.run(`
        CREATE TABLE orderbook (
            ts          TIMESTAMP NOT NULL,
            exchange    TEXT NOT NULL,
            canonical_pair TEXT NOT NULL,
            bids        JSON,
            asks        JSON
        )
    `);

    console.log('         → trades, orderbook tables created');

    // ═══════════════ VERIFICATION ═══════════════
    console.log('  [5/5] Verifying...\n');

    let reader;

    reader = await conn.runAndReadAll('SELECT COUNT(*) as cnt FROM assets');
    console.log(`  ✅ assets:     ${reader.getRows()[0][0]} rows`);

    reader = await conn.runAndReadAll('SELECT COUNT(*) as cnt FROM symbol_map');
    console.log(`  ✅ symbol_map: ${reader.getRows()[0][0]} rows`);

    reader = await conn.runAndReadAll('SELECT COUNT(DISTINCT exchange) as cnt FROM symbol_map');
    console.log(`  ✅ exchanges:  ${reader.getRows()[0][0]} distinct`);

    reader = await conn.runAndReadAll('SELECT COUNT(DISTINCT canonical_pair) as cnt FROM symbol_map');
    console.log(`  ✅ canonical:  ${reader.getRows()[0][0]} distinct pairs`);

    console.log('\n  ── Symbol Map Summary ──');
    reader = await conn.runAndReadAll(`
        SELECT canonical_pair, COUNT(DISTINCT exchange) as exchange_count
        FROM symbol_map
        WHERE is_active = true
        GROUP BY canonical_pair
        ORDER BY canonical_pair
    `);
    for (const row of reader.getRows()) {
        console.log(`     ${row[0].padEnd(12)} → ${row[1]} exchanges`);
    }

    console.log('\n  ── Per-Exchange Pair Count ──');
    reader = await conn.runAndReadAll(`
        SELECT exchange, COUNT(*) as pairs, 
               LIST(canonical_pair ORDER BY canonical_pair) as pair_list
        FROM symbol_map 
        WHERE is_active = true
        GROUP BY exchange
        ORDER BY exchange
    `);
    for (const row of reader.getRows()) {
        console.log(`     ${row[0].padEnd(15)} ${String(row[1]).padStart(2)} pairs`);
    }

    // conn disposed by GC
    console.log('\n  ✅ Schema initialized → streaming.duckdb');
}

main().catch(e => { console.error('Fatal:', e); process.exit(1); });
