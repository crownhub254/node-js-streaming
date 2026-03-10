const http = require('http');
http.get('http://127.0.0.1:3456/api/stats', r => {
  let d = '';
  r.on('data', c => d += c);
  r.on('end', () => {
    const j = JSON.parse(d);
    console.log('Uptime:', j.uptime + 's');
    const rows = [];
    for (const [n, e] of Object.entries(j.exchanges)) {
      const nT = e.native?.trades || 0, nO = e.native?.orderbook || 0;
      const pT = e.ccxtPro?.trades || 0, pO = e.ccxtPro?.orderbook || 0;
      const rT = e.ccxtRest?.trades || 0, rO = e.ccxtRest?.orderbook || 0;
      const dT = e.directRest?.trades || 0, dO = e.directRest?.orderbook || 0;
      const tT = nT + pT + rT + dT, tO = nO + pO + rO + dO;
      const ratio = tT > 0 ? (tO / tT).toFixed(1) : 'INF';
      rows.push({ name: n, nT, nO, pT, pO, rT, rO, dT, dO, tT, tO, ratio: parseFloat(ratio) || 999 });
    }
    rows.sort((a, b) => b.ratio - a.ratio);
    console.log('Exchange'.padEnd(16), 'nTr'.padStart(5), 'nOB'.padStart(6), 'pTr'.padStart(5), 'pOB'.padStart(6),
      'rTr'.padStart(5), 'rOB'.padStart(6), 'dTr'.padStart(5), 'dOB'.padStart(6),
      'totT'.padStart(6), 'totOB'.padStart(7), 'OB/T'.padStart(6));
    for (const r of rows) {
      console.log(r.name.padEnd(16),
        String(r.nT).padStart(5), String(r.nO).padStart(6),
        String(r.pT).padStart(5), String(r.pO).padStart(6),
        String(r.rT).padStart(5), String(r.rO).padStart(6),
        String(r.dT).padStart(5), String(r.dO).padStart(6),
        String(r.tT).padStart(6), String(r.tO).padStart(7),
        String(r.ratio).padStart(6));
    }
  });
});
