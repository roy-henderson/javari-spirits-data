/**
 * JAVARI SPIRITS - FULL DATA IMPORT v2
 * Fixed: Limit Iowa data to prevent memory issues
 */

const { createClient } = require('@supabase/supabase-js');
const { parse } = require('csv-parse/sync');
const https = require('https');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

const log = (msg) => {
  const ts = new Date().toLocaleString('en-US', { timeZone: 'America/New_York' });
  console.log(`[${ts} ET] ${msg}`);
};

function download(url, maxSize = 50 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    const get = (u) => {
      https.get(u, { headers: { 'User-Agent': 'Javari/1.0' } }, (res) => {
        if (res.statusCode === 301 || res.statusCode === 302) {
          return get(res.headers.location);
        }
        let data = '';
        let size = 0;
        res.on('data', c => {
          size += c.length;
          if (size < maxSize) data += c;
        });
        res.on('end', () => resolve(data));
        res.on('error', reject);
      }).on('error', reject);
    };
    get(url);
  });
}

function insertBatch(products) {
  return new Promise((resolve) => {
    const data = JSON.stringify(products);
    const host = SUPABASE_URL.replace('https://', '').replace('http://', '');
    
    const options = {
      hostname: host,
      path: '/rest/v1/products',
      method: 'POST',
      headers: {
        'apikey': SUPABASE_KEY,
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'Content-Type': 'application/json',
        'Prefer': 'resolution=ignore-duplicates,return=minimal',
        'Content-Length': Buffer.byteLength(data)
      }
    };
    
    const req = https.request(options, (res) => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => {
        resolve({ 
          success: res.statusCode >= 200 && res.statusCode < 300,
          status: res.statusCode,
          error: body.slice(0, 200)
        });
      });
    });
    req.on('error', (e) => resolve({ success: false, error: e.message }));
    req.write(data);
    req.end();
  });
}

function getCount() {
  return new Promise((resolve) => {
    const host = SUPABASE_URL.replace('https://', '');
    const req = https.request({
      hostname: host,
      path: '/rest/v1/products?select=count',
      method: 'HEAD',
      headers: {
        'apikey': SUPABASE_KEY,
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'Prefer': 'count=exact'
      }
    }, (res) => {
      const range = res.headers['content-range'] || '0/0';
      resolve(parseInt(range.split('/')[1]) || 0);
    });
    req.on('error', () => resolve(0));
    req.end();
  });
}

const SOURCES = [
  {
    name: 'Iowa Liquor (Limited)',
    url: 'https://data.iowa.gov/api/views/m3tr-qhgy/rows.csv?accessType=DOWNLOAD&$limit=20000',
    maxSize: 10 * 1024 * 1024,
    map: (r) => ({
      name: (r['Item Description'] || '').slice(0, 255),
      category: 'spirits',
      subcategory: (r['Category Name'] || '').slice(0, 100),
      brand: (r['Vendor Name'] || '').slice(0, 100),
      price: parseFloat(r['State Bottle Retail']) || null,
      source: 'iowa_liquor',
      source_id: `iowa_${(r['Item Number'] || r['Item Description'] || Math.random()).toString().slice(0,50)}`.replace(/[^a-z0-9_]/gi, '_').slice(0, 200)
    })
  },
  {
    name: 'Craft Beers',
    url: 'https://raw.githubusercontent.com/nickhould/craft-beers-dataset/master/data/processed/beers.csv',
    map: (r) => ({
      name: (r.name || '').slice(0, 255),
      category: 'beer',
      subcategory: (r.style || '').slice(0, 100),
      source: 'craftcans',
      source_id: `craft_${r.id || Math.random()}`.replace(/[^a-z0-9_]/gi, '_').slice(0, 200)
    })
  },
  {
    name: 'Boston Cocktails',
    url: 'https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2020/2020-05-26/boston_cocktails.csv',
    map: (r) => ({
      name: (r.name || '').slice(0, 255),
      category: 'cocktails',
      subcategory: (r.category || '').slice(0, 100),
      description: `${r.ingredient}: ${r.measure}`.slice(0, 500),
      source: 'boston_cocktails',
      source_id: `boston_${r.row_id || Math.random()}`.replace(/[^a-z0-9_]/gi, '_').slice(0, 200)
    })
  },
  {
    name: 'UCI Red Wine',
    url: 'https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv',
    delimiter: ';',
    map: (r, i) => ({
      name: `Red Wine Sample ${i + 1}`,
      category: 'wine',
      subcategory: 'Red Wine',
      rating: parseFloat(r.quality) || null,
      source: 'uci_wine',
      source_id: `uci_red_${i}`.slice(0, 200)
    })
  },
  {
    name: 'UCI White Wine',
    url: 'https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-white.csv',
    delimiter: ';',
    map: (r, i) => ({
      name: `White Wine Sample ${i + 1}`,
      category: 'wine',
      subcategory: 'White Wine',
      rating: parseFloat(r.quality) || null,
      source: 'uci_wine',
      source_id: `uci_white_${i}`.slice(0, 200)
    })
  }
];

async function runImport() {
  console.log('╔══════════════════════════════════════════════════════════════════╗');
  console.log('║         JAVARI SPIRITS - FULL DATA IMPORT v2                     ║');
  console.log('╚══════════════════════════════════════════════════════════════════╝');
  
  log('Starting import...');
  
  const initialCount = await getCount();
  log(`Initial products: ${initialCount}`);
  
  let totalInserted = 0;
  
  for (const source of SOURCES) {
    log(`\n=== ${source.name} ===`);
    
    try {
      log(`Downloading...`);
      const csv = await download(source.url, source.maxSize || 50 * 1024 * 1024);
      
      if (!csv || csv.length < 100) {
        log(`⚠️ Empty response`);
        continue;
      }
      
      log(`Downloaded ${(csv.length / 1024).toFixed(0)} KB`);
      
      const records = parse(csv, { 
        columns: true, 
        skip_empty_lines: true, 
        relax_column_count: true,
        delimiter: source.delimiter || ','
      });
      log(`Parsed ${records.length} records`);
      
      const seen = new Set();
      const products = records
        .map((r, i) => source.map(r, i))
        .filter(p => {
          if (!p.name || p.name.length < 2) return false;
          if (seen.has(p.source_id)) return false;
          seen.add(p.source_id);
          return true;
        });
      
      log(`Unique: ${products.length}`);
      
      const batchSize = 200;
      let inserted = 0;
      let errors = 0;
      
      for (let i = 0; i < products.length; i += batchSize) {
        const batch = products.slice(i, i + batchSize);
        const result = await insertBatch(batch);
        
        if (result.success) {
          inserted += batch.length;
        } else {
          errors += batch.length;
          if (errors <= batchSize) log(`Error: ${result.status} - ${result.error}`);
        }
        
        if ((i + batchSize) % 2000 === 0 && i > 0) {
          log(`Progress: ${i + batchSize}/${products.length}`);
        }
      }
      
      log(`✅ Inserted: ${inserted}, Errors: ${errors}`);
      totalInserted += inserted;
      
    } catch (err) {
      log(`❌ Error: ${err.message}`);
    }
  }
  
  const finalCount = await getCount();
  
  console.log('');
  console.log('═══════════════════════════════════════════════════════════════════');
  log('IMPORT COMPLETE');
  log(`Before: ${initialCount}`);
  log(`After: ${finalCount}`);
  log(`Added: ${finalCount - initialCount}`);
  console.log('═══════════════════════════════════════════════════════════════════');
}

runImport().catch(err => {
  log(`Fatal: ${err.message}`);
  console.error(err);
  process.exit(1);
});
