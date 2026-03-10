// Strip all non-BTC/ETH/SOL pairs from compare-v7-enhanced.js — comprehensive v2
const fs = require('fs');
const FILE = require('path').join(__dirname, 'compare-v7-enhanced.js');
let code = fs.readFileSync(FILE, 'utf8');
const original = code;

const keepBases = ['BTC','ETH','SOL','XBT'];
const removeBases = ['BRETT','PENGU','POPCAT','WIF','SUI','ENA'];

function shouldKeepPair(raw) {
    const s = raw.toUpperCase().replace(/^T(?=[A-Z]{3})/, ''); // strip Bitfinex 't' prefix
    if (s.startsWith('SPOT_')) { // WOO X format
        const rest = s.slice(5);
        return keepBases.some(b => rest.startsWith(b));
    }
    // Handle 'update:BRETT-USDT_0' format
    const clean = s.replace(/^(UPDATE:|TRADEHISTORYAPI:|SPOT\/TRADES:|SPOT\/ORDER_BOOK_SNAPSHOTS:)/, '').replace(/_0$/, '');
    return keepBases.some(b => clean.startsWith(b));
}

let totalChanges = 0;

// 1. UNIVERSAL: Find any [...] array containing quoted strings that look like pairs
//    Match balanced [...] up to ~2000 chars, containing quoted strings with USDT/USDC/USD/UST/DAI
code = code.replace(/\[([^\[\]]{10,3000})\]/g, (match, inner) => {
    // Only process arrays with quoted strings
    const items = inner.match(/'[^']+'/g);
    if (!items || items.length < 2) return match;
    
    // Check if this looks like a pair array (must have items with removable coins)
    const hasRemovable = items.some(p => {
        const v = p.replace(/'/g, '').toUpperCase();
        return removeBases.some(rb => v.includes(rb));
    });
    if (!hasRemovable) return match;
    
    // Also check it has keeper pairs (to avoid catching random arrays)
    const hasKeepers = items.some(p => {
        const v = p.replace(/'/g, '').toUpperCase();
        return keepBases.some(kb => v.includes(kb));
    });
    if (!hasKeepers) return match;
    
    const filtered = items.filter(p => shouldKeepPair(p.replace(/'/g, '')));
    if (filtered.length === items.length) return match;
    
    totalChanges++;
    return `[${filtered.join(',')}]`;
});

// 2. Fix CEX.IO _cexPairs: [['BTC','USDT'],['BRETT','USDT'],...]
// These are nested arrays so the above won't catch them properly
code = code.replace(/const\s+_cexPairs\s*=\s*\[([\s\S]*?)\];/g, (match, inner) => {
    const tuples = inner.match(/\['[^']+','[^']+'\]/g);
    if (!tuples) return match;
    const filtered = tuples.filter(t => {
        const base = (t.match(/'([^']+)'/)||[])[1]||'';
        return keepBases.includes(base.toUpperCase());
    });
    if (filtered.length === tuples.length) return match;
    totalChanges++;
    return `const _cexPairs=[${filtered.join(',')}];`;
});

// 3. Fix Binance.US _busPairs similarly
code = code.replace(/const\s+_busPairs\s*=\s*\[([\s\S]*?)\];/g, (match, inner) => {
    const tuples = inner.match(/\['[^']+','[^']+'\]/g);
    if (!tuples) return match;
    const filtered = tuples.filter(t => {
        const base = (t.match(/'([^']+)'/)||[])[1]||'';
        return keepBases.includes(base.toUpperCase());
    });
    if (filtered.length === tuples.length) return match;
    totalChanges++;
    return `const _busPairs=[${filtered.join(',')}];`;
});

// 4. Fix comma-separated lists within topic strings: /market/match:BTC-USDT,ETH-USDT,BRETT-USDT,...
code = code.replace(/(\/[^'":\s]+:)([A-Za-z0-9_\/-]+(?:,[A-Za-z0-9_\/-]+){2,})/g, (match, prefix, pairList) => {
    const items = pairList.split(',');
    const hasRemovable = items.some(i => removeBases.some(rb => i.toUpperCase().includes(rb)));
    if (!hasRemovable) return match;
    const filtered = items.filter(item => shouldKeepPair(item.trim()));
    if (filtered.length === items.length) return match;
    totalChanges++;
    return prefix + filtered.join(',');
});

// 5. Fix Binance/Bybit/Zoomex stream format: btcusdt@trade/brettusdt@trade/...
code = code.replace(/(['"]\s*)([a-zA-Z0-9_@.-]+(?:\/[a-zA-Z0-9_@.-]+){3,})(\s*['"])/g, (match, pre, streamList, post) => {
    if (!streamList.includes('@')) return match;
    const items = streamList.split('/');
    const hasRemovable = items.some(i => removeBases.some(rb => i.toUpperCase().includes(rb)));
    if (!hasRemovable) return match;
    const filtered = items.filter(item => {
        const sym = item.split('@')[0].toUpperCase();
        return keepBases.some(b => sym.startsWith(b));
    });
    if (filtered.length === items.length) return match;
    totalChanges++;
    return pre + filtered.join('/') + post;
});

// 6. Fix Hotcoin inline flatMap pairs array: ['btc_usdt','eth_usdt',...,'brett_usdt',...].flatMap(
code = code.replace(/\[([^\[\]]{20,2000})\]\.(flatMap|map|forEach)/g, (match, inner, method) => {
    const items = inner.match(/'[^']+'/g);
    if (!items || items.length < 2) return match;
    const hasRemovable = items.some(p => removeBases.some(rb => p.toUpperCase().includes(rb)));
    if (!hasRemovable) return match;
    const filtered = items.filter(p => shouldKeepPair(p.replace(/'/g, '')));
    if (filtered.length === items.length) return match;
    totalChanges++;
    return `[${filtered.join(',')}].${method}`;
});

// 7. Fix Biconomy restFallbackUrls broken structure: restore the [...spread, ...spread] format
// The previous script broke it — fix the opening [
code = code.replace(
    /restFallbackUrls:\['BTC_USDT','ETH_USDT','SOL_USDT'\]\.map\(([^)]+\))\),/,
    `restFallbackUrls:[...['BTC_USDT','ETH_USDT','SOL_USDT'].map($1),`
);

// 8. Remove BTC_DAI from toCanonical if still present
code = code.replace(/'BTC_DAI':'BTC_DAI','BTC\/DAI':'BTC_DAI',/, '');

// 9. Clean up dead pairs list entries with removed coins
code = code.replace(/'(Azbit|Coinbase|Deepcoin|FameEX|Darkex|CEEX|Coinstore|Websea|BigONE|LATOKEN|MEXC|Bitget|Poloniex|CEX\.IO|Toobit|Bullish):(BRETT|PENGU|POPCAT|WIF|SUI|ENA)[^']*'/g, (match) => {
    totalChanges++;
    return ''; // will leave empty commas, clean up below
});
// Clean up leftover commas from dead pair removal
code = code.replace(/,\s*,+/g, ',');
code = code.replace(/\[\s*,/g, '[');
code = code.replace(/,\s*\]/g, ']');

// 10. Remove the BingX second connectWS call that only has removed pairs
code = code.replace(/\n\s*connectWS\(_bxCfg\(\['PENGU-USDT','POPCAT-USDT','WIF-USDT','SUI-USDT','SUI-USDC','ENA-USDT'\]\)\);/, '');
// Also clean BRETT from the first BingX call
code = code.replace(
    "connectWS(_bxCfg(['BTC-USDT','ETH-USDT','SOL-USDT','BTC-USDC','ETH-USDC','SOL-USDC','BRETT-USDT']));",
    "connectWS(_bxCfg(['BTC-USDT','ETH-USDT','SOL-USDT','BTC-USDC','ETH-USDC','SOL-USDC']));"
);

console.log(`Total changes: ${totalChanges}`);

// Write
if (code === original) {
    console.log('No changes made!');
} else {
    fs.writeFileSync(FILE, code, 'utf8');
    console.log(`Saved. Reduced by ${original.length - code.length} bytes.`);
}

// Verify
try {
    require('child_process').execSync('node -c compare-v7-enhanced.js', { cwd: __dirname, stdio: 'pipe' });
    console.log('✓ Syntax check PASSED');
} catch (e) {
    console.log('✗ Syntax error:', e.stderr?.toString()?.trim());
}

// Count remaining
const remaining = (code.match(/\b(BRETT|PENGU|POPCAT|WIF(?!i)|SUI|ENA)\b/gi) || []).length;
console.log(`Remaining mentions of removed coins: ${remaining}`);
