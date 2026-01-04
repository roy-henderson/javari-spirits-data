/**
 * JAVARI SPIRITS DATA HARVESTER v1.0
 * Continuous alcohol dataset discovery, ingestion, and enrichment
 * Created: 2026-01-04
 */

const https = require('https');
const http = require('http');
const { URL } = require('url');

// Configuration
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

const log = (msg) => {
  const ts = new Date().toLocaleString('en-US', { timeZone: 'America/New_York' });
  console.log(`[${ts} ET] ${msg}`);
};

// ============================================================================
// HTTP HELPERS
// ============================================================================

function fetch(url, options = {}) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const protocol = parsed.protocol === 'https:' ? https : http;
    
    const opts = {
      hostname: parsed.hostname,
      port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path: parsed.pathname + parsed.search,
      method: options.method || 'GET',
      headers: {
        'User-Agent': 'JavariSpirits-Harvester/1.0',
        ...options.headers
      }
    };
    
    const req = protocol.request(opts, (res) => {
      // Handle redirects
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        return fetch(res.headers.location, options).then(resolve).catch(reject);
      }
      
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, data, headers: res.headers }));
    });
    
    req.on('error', reject);
    if (options.body) req.write(options.body);
    req.end();
  });
}

async function supabaseQuery(table, method = 'GET', body = null, params = '') {
  const url = `${SUPABASE_URL}/rest/v1/${table}${params}`;
  const options = {
    method,
    headers: {
      'apikey': SUPABASE_KEY,
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      'Content-Type': 'application/json',
      'Prefer': method === 'POST' ? 'return=minimal,resolution=ignore-duplicates' : 'return=representation'
    }
  };
  if (body) options.body = JSON.stringify(body);
  
  const res = await fetch(url, options);
  return { success: res.status >= 200 && res.status < 300, data: res.data, status: res.status };
}

// ============================================================================
// DATA SOURCES CONFIGURATION
// ============================================================================

const SOURCES = {
  // Government / Official Sources
  iowa_products: {
    name: 'Iowa ABD Products',
    url: 'https://data.iowa.gov/api/views/gckp-fe7r/rows.csv?accessType=DOWNLOAD',
    type: 'csv',
    category: 'spirits',
    map: (row) => ({
      name: row['Item Description'],
      brand: row['Vendor Name'],
      category_primary: 'spirits',
      category_secondary: (row['Category Name'] || '').toLowerCase().includes('whiskey') ? 'whiskey' :
                          (row['Category Name'] || '').toLowerCase().includes('vodka') ? 'vodka' :
                          (row['Category Name'] || '').toLowerCase().includes('gin') ? 'gin' :
                          (row['Category Name'] || '').toLowerCase().includes('rum') ? 'rum' :
                          (row['Category Name'] || '').toLowerCase().includes('tequila') ? 'tequila' : 'other',
      upc_ean_gtin: row['UPC'],
      abv: parseFloat(row['Proof']) / 2 || null,
      volume_ml: parseInt(row['Bottle Volume (ml)']) || null,
      country: 'USA',
      region: 'Iowa'
    }),
    priceMap: (row) => ({
      price: parseFloat((row['State Bottle Retail'] || '0').replace('$', '')),
      currency: 'USD',
      price_type: 'retail',
      location: 'Iowa'
    })
  },
  
  connecticut_brands: {
    name: 'Connecticut Liquor Brands',
    url: 'https://data.ct.gov/api/views/u6ds-fzyp/rows.csv?accessType=DOWNLOAD',
    type: 'csv',
    category: 'all',
    map: (row) => ({
      name: row['BRAND-NAME'],
      brand: row['BRAND-NAME'],
      category_primary: 'spirits',
      producer: row['OUT-OF-STATE-SHIPPER'],
      country: 'USA',
      region: 'Connecticut'
    })
  },
  
  montgomery_county: {
    name: 'Montgomery County MD',
    url: 'https://data.montgomerycountymd.gov/api/views/v76h-r7br/rows.csv?accessType=DOWNLOAD',
    type: 'csv',
    category: 'all',
    maxRows: 50000, // Limit for memory
    map: (row) => ({
      name: row['ITEM DESCRIPTION'],
      sku: row['ITEM CODE'],
      category_primary: (row['ITEM TYPE'] || '').toLowerCase(),
      producer: row['SUPPLIER'],
      country: 'USA',
      region: 'Maryland'
    })
  },
  
  // Enrichment Sources
  cocktaildb: {
    name: 'TheCocktailDB',
    url: 'https://www.thecocktaildb.com/api/json/v1/1/search.php?f=',
    type: 'json_api',
    category: 'cocktail',
    letters: 'abcdefghijklmnopqrstuvwxyz0123456789'.split('')
  },
  
  open_brewery: {
    name: 'Open Brewery DB',
    url: 'https://api.openbrewerydb.org/v1/breweries?per_page=200&page=',
    type: 'json_api',
    category: 'beer',
    maxPages: 50
  },
  
  // Open Food Facts (Backbone - massive)
  open_food_facts: {
    name: 'Open Food Facts',
    url: 'https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz',
    type: 'csv_gz',
    category: 'all',
    alcoholFilter: true // Only process alcohol categories
  }
};

// ============================================================================
// CSV PARSER (Simple, handles quotes)
// ============================================================================

function parseCSV(text, maxRows = Infinity) {
  const lines = text.split('\n');
  if (lines.length < 2) return [];
  
  const headers = parseCSVLine(lines[0]);
  const results = [];
  
  for (let i = 1; i < lines.length && results.length < maxRows; i++) {
    if (!lines[i].trim()) continue;
    const values = parseCSVLine(lines[i]);
    const obj = {};
    headers.forEach((h, idx) => obj[h] = values[idx] || '');
    results.push(obj);
  }
  
  return results;
}

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  result.push(current.trim());
  return result;
}

// ============================================================================
// NORMALIZATION
// ============================================================================

function normalizeText(text) {
  if (!text) return '';
  return text.toLowerCase()
    .replace(/[^a-z0-9\s]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeProduct(product, sourceId) {
  return {
    name: product.name || 'Unknown',
    name_normalized: normalizeText(product.name),
    brand: product.brand || null,
    brand_normalized: normalizeText(product.brand),
    producer: product.producer || null,
    category_primary: product.category_primary || 'spirits',
    category_secondary: product.category_secondary || null,
    category_tertiary: product.category_tertiary || null,
    country: product.country || null,
    region: product.region || null,
    upc_ean_gtin: product.upc_ean_gtin || null,
    sku: product.sku || null,
    abv: product.abv || null,
    volume_ml: product.volume_ml || null,
    description: product.description || null,
    image_url: product.image_url || null,
    source_refs: JSON.stringify([{ source_id: sourceId, retrieved_at: new Date().toISOString() }])
  };
}

// ============================================================================
// INGESTION ENGINE
// ============================================================================

async function ingestSource(sourceKey) {
  const source = SOURCES[sourceKey];
  if (!source) {
    log(`❌ Unknown source: ${sourceKey}`);
    return;
  }
  
  log(`\n${'═'.repeat(60)}`);
  log(`INGESTING: ${source.name}`);
  log(`${'═'.repeat(60)}`);
  
  // Create ingest run record
  const runId = `run_${Date.now()}`;
  let stats = { read: 0, inserted: 0, updated: 0, rejected: 0 };
  
  try {
    // Download data
    log(`📥 Downloading from ${source.url}...`);
    const response = await fetch(source.url);
    
    if (response.status !== 200) {
      log(`❌ Download failed: HTTP ${response.status}`);
      return stats;
    }
    
    log(`✅ Downloaded ${(response.data.length / 1024 / 1024).toFixed(2)} MB`);
    
    // Parse based on type
    let records = [];
    if (source.type === 'csv') {
      records = parseCSV(response.data, source.maxRows || 100000);
    } else if (source.type === 'json') {
      records = JSON.parse(response.data);
    }
    
    log(`📊 Parsed ${records.length} records`);
    stats.read = records.length;
    
    // Map to canonical format
    const products = records
      .map(r => source.map(r))
      .filter(p => p && p.name && p.name.length > 1)
      .map(p => normalizeProduct(p, sourceKey));
    
    log(`✅ ${products.length} valid products after mapping`);
    
    // Batch insert
    const batchSize = 500;
    for (let i = 0; i < products.length; i += batchSize) {
      const batch = products.slice(i, i + batchSize);
      const result = await supabaseQuery('canonical_products', 'POST', batch);
      
      if (result.success) {
        stats.inserted += batch.length;
      } else {
        stats.rejected += batch.length;
        if (i === 0) log(`⚠️ Insert error: ${result.data.substring(0, 200)}`);
      }
      
      // Progress
      if ((i + batchSize) % 2000 === 0 || i + batchSize >= products.length) {
        log(`   Progress: ${Math.min(i + batchSize, products.length)}/${products.length}`);
      }
    }
    
    log(`\n✅ COMPLETE: ${stats.inserted} inserted, ${stats.rejected} rejected`);
    
  } catch (error) {
    log(`❌ Error: ${error.message}`);
    stats.rejected = stats.read;
  }
  
  return stats;
}

// ============================================================================
// COCKTAILDB INGESTION
// ============================================================================

async function ingestCocktailDB() {
  log(`\n${'═'.repeat(60)}`);
  log(`INGESTING: TheCocktailDB`);
  log(`${'═'.repeat(60)}`);
  
  const letters = 'abcdefghijklmnopqrstuvwxyz'.split('');
  let totalRecipes = 0;
  
  for (const letter of letters) {
    try {
      const url = `https://www.thecocktaildb.com/api/json/v1/1/search.php?f=${letter}`;
      const response = await fetch(url);
      const data = JSON.parse(response.data);
      
      if (!data.drinks) continue;
      
      const recipes = data.drinks.map(d => ({
        name: d.strDrink,
        name_normalized: normalizeText(d.strDrink),
        category: d.strCategory,
        glass_type: d.strGlass,
        iba_category: d.strIBA,
        instructions: d.strInstructions,
        ingredients: JSON.stringify(
          Array.from({ length: 15 }, (_, i) => ({
            ingredient: d[`strIngredient${i + 1}`],
            measure: d[`strMeasure${i + 1}`]
          })).filter(ing => ing.ingredient)
        ),
        image_url: d.strDrinkThumb,
        source_recipe_id: d.idDrink
      }));
      
      if (recipes.length > 0) {
        await supabaseQuery('cocktail_recipes', 'POST', recipes);
        totalRecipes += recipes.length;
      }
      
      // Rate limit
      await new Promise(r => setTimeout(r, 200));
      
    } catch (e) {
      // Continue on error
    }
  }
  
  log(`✅ Imported ${totalRecipes} cocktail recipes`);
  return totalRecipes;
}

// ============================================================================
// OPEN BREWERY DB INGESTION
// ============================================================================

async function ingestOpenBrewery() {
  log(`\n${'═'.repeat(60)}`);
  log(`INGESTING: Open Brewery DB`);
  log(`${'═'.repeat(60)}`);
  
  let page = 1;
  let totalBreweries = 0;
  
  while (page <= 50) {
    try {
      const url = `https://api.openbrewerydb.org/v1/breweries?per_page=200&page=${page}`;
      const response = await fetch(url);
      const breweries = JSON.parse(response.data);
      
      if (!breweries || breweries.length === 0) break;
      
      const producers = breweries.map(b => ({
        name: b.name,
        name_normalized: normalizeText(b.name),
        type: 'brewery',
        country: b.country,
        region: b.state,
        address: [b.street, b.city, b.state, b.postal_code].filter(Boolean).join(', '),
        latitude: b.latitude ? parseFloat(b.latitude) : null,
        longitude: b.longitude ? parseFloat(b.longitude) : null,
        website: b.website_url
      }));
      
      await supabaseQuery('producers', 'POST', producers);
      totalBreweries += producers.length;
      
      log(`   Page ${page}: ${producers.length} breweries`);
      page++;
      
      await new Promise(r => setTimeout(r, 100));
      
    } catch (e) {
      break;
    }
  }
  
  log(`✅ Imported ${totalBreweries} breweries`);
  return totalBreweries;
}

// ============================================================================
// DISCOVERY ENGINE
// ============================================================================

async function discoverNewSources() {
  log(`\n${'═'.repeat(60)}`);
  log(`DISCOVERY: Scanning for new datasets`);
  log(`${'═'.repeat(60)}`);
  
  const searchTargets = [
    { name: 'Data.gov', url: 'https://catalog.data.gov/api/3/action/package_search?q=liquor+OR+alcohol+OR+spirits&rows=50' },
    // Add more discovery targets
  ];
  
  for (const target of searchTargets) {
    try {
      log(`🔍 Scanning ${target.name}...`);
      const response = await fetch(target.url);
      // Parse and queue new sources
      log(`   Found potential sources (implement parsing)`);
    } catch (e) {
      log(`   ⚠️ Error scanning ${target.name}`);
    }
  }
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

async function main() {
  log('\n');
  log('╔════════════════════════════════════════════════════════════════╗');
  log('║       JAVARI SPIRITS DATA HARVESTER v1.0                       ║');
  log('║       Continuous Alcohol Dataset Discovery & Ingestion         ║');
  log('╚════════════════════════════════════════════════════════════════╝');
  log('');
  
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    log('❌ Missing SUPABASE_URL or SUPABASE_SERVICE_KEY');
    process.exit(1);
  }
  
  const mode = process.argv[2] || 'all';
  
  log(`Mode: ${mode}`);
  log(`Database: ${SUPABASE_URL}`);
  log('');
  
  const results = {
    sources: [],
    totalInserted: 0,
    totalRejected: 0
  };
  
  if (mode === 'all' || mode === 'iowa') {
    const stats = await ingestSource('iowa_products');
    results.sources.push({ name: 'Iowa Products', ...stats });
    results.totalInserted += stats.inserted;
  }
  
  if (mode === 'all' || mode === 'connecticut') {
    const stats = await ingestSource('connecticut_brands');
    results.sources.push({ name: 'Connecticut', ...stats });
    results.totalInserted += stats.inserted;
  }
  
  if (mode === 'all' || mode === 'montgomery') {
    const stats = await ingestSource('montgomery_county');
    results.sources.push({ name: 'Montgomery County', ...stats });
    results.totalInserted += stats.inserted;
  }
  
  if (mode === 'all' || mode === 'cocktails') {
    const count = await ingestCocktailDB();
    results.sources.push({ name: 'CocktailDB', inserted: count });
    results.totalInserted += count;
  }
  
  if (mode === 'all' || mode === 'breweries') {
    const count = await ingestOpenBrewery();
    results.sources.push({ name: 'Open Brewery', inserted: count });
    results.totalInserted += count;
  }
  
  if (mode === 'discover') {
    await discoverNewSources();
  }
  
  // Final summary
  log('\n');
  log('╔════════════════════════════════════════════════════════════════╗');
  log('║                    HARVESTER SUMMARY                           ║');
  log('╚════════════════════════════════════════════════════════════════╝');
  
  for (const source of results.sources) {
    log(`   ${source.name}: ${source.inserted || 0} products`);
  }
  
  log('');
  log(`   TOTAL INSERTED: ${results.totalInserted}`);
  log('');
  log('✅ Harvester complete');
}

main().catch(console.error);
