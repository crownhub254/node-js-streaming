// Strip all non-BTC/ETH/SOL pairs from compare-v7-enhanced.js — v2 comprehensive
// Handles ALL array formats: [...].map, [...].flatMap, [...].forEach, for(const s of[...]),
// args:[...], product_ids:[...], const s=[...], const pairs=[...], _cexPairs, restPoll for-of, etc.
const fs = require('fs');
const path = require('path');

const FILE = path.join(__dirname, 'compare-v7-enhanced.js');
let code = fs.readFileSync(FILE, 'utf8');
const original = code;

// Coins to KEEP (base symbols in various formats)
const keepBases = ['BTC','ETH','SOL','XBT'];

// Coins to REMOVE
const removeBases = ['BRETT','PENGU','POPCAT','WIF','SUI','ENA'];

// Helper: check if a pair string has a base we want to keep
function shouldKeepPair(pair) {
    const upper = pair.toUpperCase().replace(/^T/, ''); // handle Bitfinex tBTCUSD format
    for (const b of keepBases) {
        if (upper.startsWith(b)) return true;
    }
    // Handle SPOT_BTC_USDT format (WOO X)
    if (upper.startsWith('SPOT_')) {
        const rest = upper.slice(5);
        for (const b of keepBases) {
            if (rest.startsWith(b)) return true;
        }
    }
    return false;
}

// 1. Fix ccxtPairs arrays - format: ccxtPairs:['BTC/USDT','ETH/USDT',...,'BRETT/USDT',...]
//    These use comma-separated quoted strings inside []
let changes = 0;

// Generic: find all quoted-string arrays that contain coin pairs to remove
// Match arrays like ['BTC/USDT','ETH/USDT',...] that contain removable pairs
code = code.replace(/ccxtPairs:\[([^\]]+)\]/g, (match, inner) => {
    const pairs = inner.match(/'[^']+'/g);
    if (!pairs) return match;
    const filtered = pairs.filter(p => shouldKeepPair(p.replace(/'/g, '')));
    if (filtered.length === pairs.length) return match;
    changes++;
    return `ccxtPairs:[${filtered.join(',')}]`;
});
console.log(`ccxtPairs arrays modified: ${changes}`);

// 2. Fix REST fallback pairs arrays in restFallbackUrls config
// Format: pairs: ['BTCUSDT','ETHUSDT',...,'PENGUUSDT',...]
let restChanges = 0;
code = code.replace(/pairs:\s*\[([^\]]+)\]/g, (match, inner) => {
    const pairs = inner.match(/'[^']+'/g);
    if (!pairs) return match;
    // Only process if this looks like a coin pair array (has quote currencies)
    const hasPairs = pairs.some(p => {
        const v = p.replace(/'/g, '').toUpperCase();
        return v.includes('USDT') || v.includes('USDC') || v.includes('USD') || v.includes('UST') || v.includes('DAI');
    });
    if (!hasPairs) return match;
    const filtered = pairs.filter(p => shouldKeepPair(p.replace(/'/g, '')));
    if (filtered.length === pairs.length) return match;
    restChanges++;
    return `pairs: [${filtered.join(',')}]`;
});
console.log(`REST pairs arrays modified: ${restChanges}`);

// 3. Fix native WS subscription strings in onOpen handlers
// These are comma-separated symbol lists inside strings like:
// 'BTC-USDT,ETH-USDT,SOL-USDT,BRETT-USDT,...'
// Need to handle various formats: BTC-USDT, BTC_USDT, BTCUSDT, btcusdt, BTC/USDT

// Find all subscription-like comma-separated pair strings
let wsChanges = 0;

// Handle strings with comma-separated pairs (various separators)
// Match patterns like 'BTC-USDT,ETH-USDT,...,BRETT-USDT,...'
function filterCommaSeparatedPairs(str) {
    const items = str.split(',');
    if (items.length < 3) return str; // too few to be a pair list
    
    // Check if this looks like a pair list
    const pairLike = items.filter(i => {
        const u = i.toUpperCase();
        return u.includes('USDT') || u.includes('USDC') || u.includes('USD') || u.includes('UST') || u.includes('DAI');
    });
    if (pairLike.length < items.length * 0.5) return str; // less than half look like pairs
    
    const filtered = items.filter(item => shouldKeepPair(item.trim()));
    if (filtered.length === items.length) return str;
    wsChanges++;
    return filtered.join(',');
}

// Find topic strings in WS subscription messages with comma-separated pairs
// Pattern: '/market/match:BTC-USDT,ETH-USDT,...,BRETT-USDT,...'
code = code.replace(/(\/[^'":]+:)([A-Za-z0-9_\/-]+(?:,[A-Za-z0-9_\/-]+){2,})/g, (match, prefix, pairList) => {
    const filtered = filterCommaSeparatedPairs(pairList);
    if (filtered === pairList) return match;
    return prefix + filtered;
});

// Pattern: subscribe to channels with pairs like 'btcusdt,ethusdt,...'
// Handle JSON arrays of channel subscriptions like in Binance streams
// e.g., 'btcusdt@trade/ethusdt@trade/...'
code = code.replace(/(['"]\s*)([a-zA-Z0-9_@-]+(?:\/[a-zA-Z0-9_@-]+){3,})(\s*['"])/g, (match, pre, streamList, post) => {
    if (!streamList.includes('@')) return match;
    const items = streamList.split('/');
    const filtered = items.filter(item => {
        const sym = item.split('@')[0].toUpperCase();
        for (const b of keepBases) {
            if (sym.startsWith(b)) return true;
        }
        return false;
    });
    if (filtered.length === items.length) return match;
    wsChanges++;
    return pre + filtered.join('/') + post;
});
console.log(`WS subscription strings modified: ${wsChanges}`);

// 4. Fix onOpen handlers with JSON.stringify subscription messages containing pair arrays
// Pattern within JSON.stringify: arrays of objects like [{channel:'trades',instId:'BTC-USDT'},{channel:'trades',instId:'BRETT-USDT'}]
// These are built with .map() so need to fix the source array: const p=['BTC-USDT',...,'PENGU-USDT',...]
let arrChanges = 0;
code = code.replace(/const\s+p\s*=\s*\[([^\]]+)\]/g, (match, inner) => {
    const items = inner.match(/'[^']+'/g);
    if (!items) return match;
    const hasPairs = items.some(p => {
        const v = p.replace(/'/g, '').toUpperCase();
        return v.includes('USDT') || v.includes('USDC') || v.includes('USD');
    });
    if (!hasPairs) return match;
    const filtered = items.filter(p => shouldKeepPair(p.replace(/'/g, '')));
    if (filtered.length === items.length) return match;
    arrChanges++;
    return `const p=[${filtered.join(',')}]`;
});
console.log(`const p=[] arrays modified: ${arrChanges}`);

// 5. Fix onOpen subscribe topics arrays
// Pattern: topics:['spot/trades:BTC_USDT','spot/trades:ETH_USDT',...,'spot/trades:PENGU_USDT',...]
let topicChanges = 0;
code = code.replace(/topics:\s*\[([^\]]+)\]/g, (match, inner) => {
    const items = inner.match(/'[^']+'/g);
    if (!items) return match;
    const filtered = items.filter(item => {
        const v = item.replace(/'/g, '');
        // Extract the pair part after the last colon
        const pairPart = v.includes(':') ? v.split(':').pop() : v;
        return shouldKeepPair(pairPart);
    });
    if (filtered.length === items.length) return match;
    topicChanges++;
    return `topics:[${filtered.join(',')}]`;
});
console.log(`topics:[] arrays modified: ${topicChanges}`);

// 6. Fix Bitfinex channel subscription arrays
// Pattern: channels like [{symbol:'tBTCUSD',channel:'trades'},{symbol:'tSUIUST',channel:'trades'}]
// These are in Bitfinex onOpen with individual ws.send calls for each symbol
// handled via the shouldKeepPair for 't'-prefixed symbols already

// 7. Fix inline arrays in onOpen ws.send calls
// Pattern: ['BTC_USDT','ETH_USDT',...].forEach(...)
let forEachChanges = 0;
code = code.replace(/\[([^\]]{20,})\]\.forEach/g, (match, inner) => {
    const items = inner.match(/'[^']+'/g);
    if (!items) return match;
    const hasPairs = items.some(p => {
        const v = p.replace(/'/g, '').toUpperCase();
        return v.includes('USDT') || v.includes('USDC') || v.includes('USD') || v.includes('UST') || v.includes('BTC') || v.includes('ETH');
    });
    if (!hasPairs) return match;
    const filtered = items.filter(p => shouldKeepPair(p.replace(/'/g, '')));
    if (filtered.length === items.length) return match;
    forEachChanges++;
    return `[${filtered.join(',')}].forEach`;
});
console.log(`forEach arrays modified: ${forEachChanges}`);

// 8. Fix inline arrays in onOpen ws.send calls with .map
let mapChanges = 0;
code = code.replace(/\[([^\]]{20,})\]\.map/g, (match, inner) => {
    const items = inner.match(/'[^']+'/g);
    if (!items) return match;
    const hasPairs = items.some(p => {
        const v = p.replace(/'/g, '').toUpperCase();
        return v.includes('USDT') || v.includes('USDC') || v.includes('USD') || v.includes('UST');
    });
    if (!hasPairs) return match;
    const filtered = items.filter(p => shouldKeepPair(p.replace(/'/g, '')));
    if (filtered.length === items.length) return match;
    mapChanges++;
    return `[${filtered.join(',')}].map`;
});
console.log(`map arrays modified: ${mapChanges}`);

// 9. Fix CEX.IO _cexPairs array: [['BTC','USDT'],['ETH','USDT'],...,['PENGU','USDT'],...]
let cexChanges = 0;
code = code.replace(/const\s+_cexPairs\s*=\s*\[([^\]]+)\]/g, (match, inner) => {
    const tuples = inner.match(/\[[^\]]+\]/g);
    if (!tuples) return match;
    const filtered = tuples.filter(t => {
        const base = (t.match(/'([^']+)'/)||[])[1]||'';
        return keepBases.includes(base.toUpperCase());
    });
    if (filtered.length === tuples.length) return match;
    cexChanges++;
    return `const _cexPairs=[${filtered.join(',')}]`;
});
console.log(`CEX.IO _cexPairs modified: ${cexChanges}`);

// 10. Fix Binance.US _busPairs: similar format
code = code.replace(/const\s+_busPairs\s*=\s*\[([^\]]+)\]/g, (match, inner) => {
    const tuples = inner.match(/\[[^\]]+\]/g);
    if (!tuples) return match;
    const filtered = tuples.filter(t => {
        const base = (t.match(/'([^']+)'/)||[])[1]||'';
        return keepBases.includes(base.toUpperCase());
    });
    if (filtered.length === tuples.length) return match;
    return `const _busPairs=[${filtered.join(',')}]`;
});

// 11. Clean up dead pairs exclusion list - remove entries for coins we no longer track
// These won't match anything anymore so they're harmless, but clean is better
let deadPairChanges = 0;
code = code.replace(/(const\s+_deadPairs\s*=\s*new\s+Set\s*\(\s*\[)([\s\S]*?)(\]\s*\))/g, (match, pre, inner, post) => {
    const items = inner.match(/'[^']+'/g);
    if (!items) return match;
    const filtered = items.filter(item => {
        const v = item.replace(/'/g, '');
        const pairPart = v.includes(':') ? v.split(':')[1] : v;
        return shouldKeepPair(pairPart);
    });
    deadPairChanges++;
    if (filtered.length === 0) return `${pre}\n    // v9.9.22: cleaned — BTC/ETH/SOL only\n${post}`;
    return `${pre}\n    ${filtered.join(',\n    ')}\n${post}`;
});
console.log(`Dead pairs list modified: ${deadPairChanges}`);

// 12. BTC_DAI entry in toCanonical mapping
code = code.replace(/'BTC_DAI':'BTC_DAI','BTC\/DAI':'BTC_DAI',/, '');
console.log('Removed BTC_DAI from toCanonical');

// Write back
if (code === original) {
    console.log('\nNo changes made!');
} else {
    fs.writeFileSync(FILE, code, 'utf8');
    const diffBytes = original.length - code.length;
    console.log(`\nDone! File saved. Reduced by ${diffBytes} bytes.`);
}
