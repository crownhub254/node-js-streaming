const duckdb = require('@duckdb/node-api');
(async()=>{
  const db = await duckdb.DuckDBInstance.create('streaming.duckdb');
  const conn = await db.connect();
  
  let r = await conn.run('SELECT count(*) as c FROM trades');
  const rows1 = await r.getRows(); console.log('Total trades:', rows1[0][0]);
  r = await conn.run('SELECT count(*) as c FROM orderbook');
  const rows2 = await r.getRows(); console.log('Total OB:', rows2[0][0]);
  
  r = await conn.run(`
    SELECT exchange, 
      SUM(CASE WHEN source='trades' THEN cnt ELSE 0 END) as trades,
      SUM(CASE WHEN source='orderbook' THEN cnt ELSE 0 END) as ob
    FROM (
      SELECT exchange, 'trades' as source, count(*) as cnt FROM trades GROUP BY exchange
      UNION ALL
      SELECT exchange, 'orderbook' as source, count(*) as cnt FROM orderbook GROUP BY exchange
    ) GROUP BY exchange ORDER BY (trades+ob) DESC
  `);
  const rows3 = await r.getRows();
  console.log('\nExchange | Trades | OB | Total');
  console.log('---------|--------|-----|------');
  let totalT=0, totalOB=0, withData=0, withTrades=0, withOB=0;
  for(const row of rows3) {
    const t = Number(row[1]), o = Number(row[2]);
    if(t>0||o>0) withData++;
    if(t>0) withTrades++;
    if(o>0) withOB++;
    totalT+=t; totalOB+=o;
    console.log(row[0]+' | '+t+' | '+o+' | '+(t+o));
  }
  console.log('TOTAL | '+totalT+' | '+totalOB+' | '+(totalT+totalOB));
  console.log('Exchanges with data: '+withData+'/48');
  console.log('Exchanges with trades: '+withTrades+'/48');
  console.log('Exchanges with OB: '+withOB+'/48');
  
  console.log('\n--- Focus Exchanges (Fixes Applied) ---');
  for (const ex of ['MEXC','CoinEx','CEX.IO','Bullish','EXMO','DigiFinex']) {
    r = await conn.run("SELECT count(*) FROM trades WHERE exchange='"+ex+"'");
    const t = (await r.getRows())[0][0];
    r = await conn.run("SELECT count(*) FROM orderbook WHERE exchange='"+ex+"'");
    const o = (await r.getRows())[0][0];
    console.log(ex+': Trades='+t+' OB='+o);
  }
  
  console.log('\n--- Exchanges with 0 data ---');
  r = await conn.run(`
    WITH all_ex AS (SELECT DISTINCT exchange FROM symbol_map),
    t AS (SELECT exchange, count(*) as tc FROM trades GROUP BY exchange),
    o AS (SELECT exchange, count(*) as oc FROM orderbook GROUP BY exchange)
    SELECT a.exchange FROM all_ex a 
    LEFT JOIN t ON a.exchange=t.exchange 
    LEFT JOIN o ON a.exchange=o.exchange
    WHERE COALESCE(t.tc,0)=0 AND COALESCE(o.oc,0)=0
    ORDER BY a.exchange
  `);
  const zeros = await r.getRows();
  for(const row of zeros) console.log('  '+row[0]);
  
  await conn.close();
})();
