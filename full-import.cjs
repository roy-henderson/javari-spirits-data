/**
 * JAVARI SPIRITS - FULL DATA IMPORT
 * Imports all available real product data
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

// Download helper
function download(url) {
  return new Promise((resolve, reject) => {
    const get = (u) => {
      https.get(u, { headers: { 'User-Agent': 'Javari/1.0' } }, (res) => {
        if (res.statusCode === 301 || res.statusCode === 302) {
          return get(res.headers.location);
        }
        let data = '';
        res.on('data', c => data += c);
        res.on('end', () => resolve(data));
        res.on('error', reject);
      }).on('error', reject);
    };
    get(url);
  });
}

// Direct Supabase REST insert (bypasses fetch issues)
function insertBatch(products) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(products);
    const options = {
      hostname: SUPABASE_URL.replace('https://', '').replace('http://', ''),
      path: '/rest/v1/products?on_conflict=source,source_id',
      method: 'POST',
      headers: {
        'apikey': SUPABASE_KEY,
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'Content-Type': 'application/json',
        'Prefer': 'resolution=ignore-duplicates',
        'Content-Length': Buffer.byteLength(data)
      }
    };
    
    const req = https.request(options, (res) => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve({ success: true, count: products.length });
        } else {
          resolve({ success: false, error: body, count: 0 });
        }
      });
    });
    req.on('error', (e) => resolve({ success: false, error: e.message, count: 0 }));
    req.write(data);
    req.end();
  });
}

// Get count via REST
function getCount() {
  return new Promise((resolve) => {
    const options = {
      hostname: SUPABASE_URL.replace('https://', ''),
      path: '/rest/v1/products?select=count',
      method: 'HEAD',
      headers: {
        'apikey': SUPABASE_KEY,
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'Prefer': 'count=exact'
      }
    };
    
    const req = https.request(options, (res) => {
      const count = res.headers['content-range']?.split('/')[1] || '0';
      resolve(parseInt(count) || 0);
    });
    req.on('error', () => resolve(0));
    req.end();
  });
}

// Data sources
const SOURCES = [
  {
    name: 'Iowa Liquor',
    url: 'https://data.iowa.gov/api/views/m3tr-qhgy/rows.csv?accessType=DOWNLOAD&$limit=50000',
    category: 'spirits',
    map: (r) => ({
      name: (r['Item Description'] || '').slice(0, 255),
      category: (r['Category Name'] || '').toLowerCase().includes('vodka') ? 'spirits' :
                (r['Category Name'] || '').toLowerCase().includes('whiskey') ? 'spirits' :
                (r['Category Name'] || '').toLowerCase().includes('bourbon') ? 'spirits' :
                (r['Category Name'] || '').toLowerCase().includes('gin') ? 'spirits' :
                (r['Category Name'] || '').toLowerCase().includes('rum') ? 'spirits' :
                (r['Category Name'] || '').toLowerCase().includes('tequila') ? 'spirits' :
                (r['Category Name'] || '').toLowerCase().includes('brandy') ? 'spirits' :
                (r['Category Name'] || '').toLowerCase().includes('wine') ? 'wine' : 'spirits',
      subcategory: (r['Category Name'] || '').slice(0, 100),
      brand: (r['Vendor Name'] || '').slice(0, 100),
      price: parseFloat(r['State Bottle Retail']) || null,
      source: 'iowa_liquor',
      source_id: `iowa_${r['Item Number'] || r['Item Description']?.slice(0,50)}`.replace(/[^a-z0-9_]/gi, '_').slice(0, 200),
      metadata: JSON.stringify({ volume_ml: r['Bottle Volume (ml)'], vendor: r['Vendor Number'] })
    })
  },
  {
    name: 'Craft Beers',
    url: 'https://raw.githubusercontent.com/nickhould/craft-beers-dataset/master/data/processed/beers.csv',
    category: 'beer',
    map: (r) => ({
      name: (r.name || '').slice(0, 255),
      category: 'beer',
      subcategory: (r.style || '').slice(0, 100),
      brand: `Brewery ${r.brewery_id || 'Unknown'}`,
      source: 'craftcans',
      source_id: `craft_${r.id || r.name?.slice(0,50)}`.replace(/[^a-z0-9_]/gi, '_').slice(0, 200),
      metadata: JSON.stringify({ abv: r.abv, ibu: r.ibu, ounces: r.ounces })
    })
  },
  {
    name: 'Boston Cocktails',
    url: 'https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2020/2020-05-26/boston_cocktails.csv',
    category: 'cocktails',
    map: (r) => ({
      name: (r.name || '').slice(0, 255),
      category: 'cocktails',
      subcategory: (r.category || '').slice(0, 100),
      description: `${r.ingredient_number}: ${r.ingredient} - ${r.measure}`.slice(0, 500),
      source: 'boston_cocktails',
      source_id: `boston_${r.row_id || r.name?.slice(0,50)}`.replace(/[^a-z0-9_]/gi, '_').slice(0, 200),
      metadata: JSON.stringify({ ingredient: r.ingredient, measure: r.measure })
    })
  }
];

async function runImport() {
  console.log('');
  console.log('╔══════════════════════════════════════════════════════════════════╗');
  console.log('║         JAVARI SPIRITS - FULL DATA IMPORT                        ║');
  console.log('╚══════════════════════════════════════════════════════════════════╝');
  console.log('');
  
  log('Starting import...');
  
  // Check initial count
  const initialCount = await getCount();
  log(`Initial products in database: ${initialCount}`);
  
  let totalInserted = 0;
  
  for (const source of SOURCES) {
    log(`\n=== Importing ${source.name} ===`);
    
    try {
      log(`Downloading from ${source.url.slice(0, 60)}...`);
      const csv = await download(source.url);
      
      if (!csv || csv.length < 100 || csv.startsWith('<!') || csv.startsWith('404')) {
        log(`⚠️ Invalid response for ${source.name}`);
        continue;
      }
      
      const records = parse(csv, { columns: true, skip_empty_lines: true, relax_column_count: true });
      log(`Parsed ${records.length} records`);
      
      // Dedupe by source_id
      const seen = new Set();
      const products = records
        .map(source.map)
        .filter(p => {
          if (!p.name || p.name.length < 2) return false;
          if (seen.has(p.source_id)) return false;
          seen.add(p.source_id);
          return true;
        });
      
      log(`Unique products: ${products.length}`);
      
      // Insert in batches
      const batchSize = 500;
      let inserted = 0;
      
      for (let i = 0; i < products.length; i += batchSize) {
        const batch = products.slice(i, i + batchSize);
        const result = await insertBatch(batch);
        
        if (result.success) {
          inserted += batch.length;
        } else if (i === 0) {
          log(`Error: ${result.error?.slice(0, 200)}`);
        }
        
        if ((i + batchSize) % 5000 === 0) {
          log(`Progress: ${i + batchSize}/${products.length}`);
        }
      }
      
      log(`✅ ${source.name}: Inserted ${inserted} products`);
      totalInserted += inserted;
      
    } catch (err) {
      log(`❌ ${source.name} error: ${err.message}`);
    }
  }
  
  // Final count
  const finalCount = await getCount();
  
  console.log('');
  console.log('═══════════════════════════════════════════════════════════════════');
  log('IMPORT COMPLETE');
  log(`Products before: ${initialCount}`);
  log(`Products after: ${finalCount}`);
  log(`New products added: ${finalCount - initialCount}`);
  console.log('═══════════════════════════════════════════════════════════════════');
}

runImport().catch(err => {
  log(`Fatal: ${err.message}`);
  process.exit(1);
});
