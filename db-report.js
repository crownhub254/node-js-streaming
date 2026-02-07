const { DuckDBInstance } = require('@duckdb/node-api');

(async () => {
    const instance = await DuckDBInstance.create('streaming.duckdb');
    const conn = await instance.connect();

    console.log('\n╔════════════════════════════════════════════════╗');
    console.log('║  DuckDB Table Report                           ║');
    console.log('╚════════════════════════════════════════════════╝\n');

    // List all tables
    const tables = await conn.runAndReadAll("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name");
    console.log('  DuckDB Tables:');
    for (const row of tables.getRows()) {
        const count = await conn.runAndReadAll(`SELECT COUNT(*) FROM ${row[0]}`);
        console.log(`    - ${row[0]}: ${count.getRows()[0][0]} rows`);
    }

    console.log('\n  ── Trades per Canonical Pair ──\n');
    let r = await conn.runAndReadAll('SELECT canonical_pair, COUNT(*) as cnt, COUNT(DISTINCT exchange) as ex FROM trades GROUP BY canonical_pair ORDER BY canonical_pair');
    for (const row of r.getRows()) {
        console.log(`    ${String(row[0]).padEnd(12)} ${String(row[1]).padStart(8)} trades from ${row[2]} exchanges`);
    }

    console.log('\n  ── Orderbook per Canonical Pair ──\n');
    r = await conn.runAndReadAll('SELECT canonical_pair, COUNT(*) as cnt, COUNT(DISTINCT exchange) as ex FROM orderbook GROUP BY canonical_pair ORDER BY canonical_pair');
    for (const row of r.getRows()) {
        console.log(`    ${String(row[0]).padEnd(12)} ${String(row[1]).padStart(8)} snapshots from ${row[2]} exchanges`);
    }

    console.log('\n  ── Per-Exchange Summary ──\n');
    r = await conn.runAndReadAll(`
        SELECT e.exchange,
               COALESCE(t.tc, 0) as trades,
               COALESCE(o.oc, 0) as orderbooks
        FROM (SELECT DISTINCT exchange FROM (SELECT exchange FROM trades UNION ALL SELECT exchange FROM orderbook)) e
        LEFT JOIN (SELECT exchange, COUNT(*) as tc FROM trades GROUP BY exchange) t ON e.exchange = t.exchange
        LEFT JOIN (SELECT exchange, COUNT(*) as oc FROM orderbook GROUP BY exchange) o ON e.exchange = o.exchange
        ORDER BY e.exchange
    `);
    console.log(`    ${'Exchange'.padEnd(15)} ${'Trades'.padStart(8)} ${'Orderbook'.padStart(10)}`);
    console.log('    ' + '─'.repeat(36));
    let totalT = 0, totalO = 0;
    for (const row of r.getRows()) {
        console.log(`    ${String(row[0]).padEnd(15)} ${String(row[1]).padStart(8)} ${String(row[2]).padStart(10)}`);
        totalT += Number(row[1]); totalO += Number(row[2]);
    }
    console.log('    ' + '─'.repeat(36));
    console.log(`    ${'TOTAL'.padEnd(15)} ${String(totalT).padStart(8)} ${String(totalO).padStart(10)}`);

    // Grand total
    console.log(`\n  ══════════════════════════════════════════`);
    console.log(`  Total DuckDB rows: ${totalT + totalO} (data) + 172 (symbol_map) + 6 (assets) = ${totalT + totalO + 178}`);
    console.log(`  ══════════════════════════════════════════\n`);
})();
