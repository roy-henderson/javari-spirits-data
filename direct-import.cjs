/**
 * JAVARI SPIRITS - DIRECT DATA IMPORT
 * Works around fetch issues in Node.js
 */

const { createClient } = require('@supabase/supabase-js');
const { parse } = require('csv-parse/sync');
const fs = require('fs');
const path = require('path');
const https = require('https');

// Supabase setup
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY,
  {
    auth: { persistSession: false },
    global: {
      fetch: (...args) => import('node-fetch').then(({default: fetch}) => fetch(...args))
    }
  }
);

const log = (msg) => {
  const ts = new Date().toLocaleString('en-US', { 
    timeZone: 'America/New_York',
    dateStyle: 'short',
    timeStyle: 'medium'
  });
  console.log(`[${ts} ET] ${msg}`);
};

// Simple HTTPS download
function download(url) {
  return new Promise((resolve, reject) => {
    https.get(url, { headers: { 'User-Agent': 'Javari-Spirits/1.0' } }, (res) => {
      if (res.statusCode === 301 || res.statusCode === 302) {
        return download(res.headers.location).then(resolve).catch(reject);
      }
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(data));
      res.on('error', reject);
    }).on('error', reject);
  });
}

// Product insertion with batching
async function insertProducts(products, source) {
  const batchSize = 500;
  let inserted = 0;
  let errors = 0;
  
  for (let i = 0; i < products.length; i += batchSize) {
    const batch = products.slice(i, i + batchSize);
    
    const { error } = await supabase
      .from('products')
      .upsert(batch, { onConflict: 'source,source_id', ignoreDuplicates: true });
    
    if (error) {
      errors += batch.length;
      if (i === 0) log(`  Error: ${error.message}`);
    } else {
      inserted += batch.length;
    }
    
    // Progress every 5000
    if ((i + batchSize) % 5000 === 0) {
      log(`  Progress: ${i + batchSize}/${products.length}`);
    }
  }
  
  return { inserted, errors };
}

// Data sources
const SOURCES = {
  // Wine
  winemag: {
    url: 'https://raw.githubusercontent.com/zackthoutt/wine-deep-learning/master/data/winemag-data-130k-v2.csv',
    category: 'wine',
    map: (row) => ({
      name: row.title?.slice(0, 255),
      category: 'wine',
      subcategory: row.variety?.slice(0, 100),
      brand: row.winery?.slice(0, 100),
      description: row.description?.slice(0, 2000),
      price: parseFloat(row.price) || null,
      rating: parseFloat(row.points) || null,
      source: 'winemag',
      source_id: `wm_${row.title?.slice(0, 50)}_${row.winery?.slice(0, 30)}`.replace(/[^a-z0-9_]/gi, '_'),
      metadata: { country: row.country, region: row.region_1, designation: row.designation }
    })
  },
  
  // Beer
  craftcans: {
    url: 'https://raw.githubusercontent.com/nickhould/craft-beers-dataset/master/data/processed/beers.csv',
    category: 'beer',
    map: (row) => ({
      name: row.name?.slice(0, 255),
      category: 'beer',
      subcategory: row.style?.slice(0, 100),
      brand: row.brewery_id ? `Brewery ${row.brewery_id}` : null,
      source: 'craftcans',
      source_id: `cc_${row.id || row.name?.slice(0, 50)}`.replace(/[^a-z0-9_]/gi, '_'),
      metadata: { abv: row.abv, ibu: row.ibu, ounces: row.ounces }
    })
  },
  
  // Cocktails - Boston
  boston: {
    url: 'https://raw.githubusercontent.com/rfordatascience/tidytuesday/master/data/2020/2020-05-26/boston_cocktails.csv',
    category: 'cocktails',
    map: (row) => ({
      name: row.name?.slice(0, 255),
      category: 'cocktails',
      subcategory: row.category?.slice(0, 100),
      description: `${row.ingredient_number || ''}: ${row.ingredient || ''} - ${row.measure || ''}`.slice(0, 2000),
      source: 'boston_cocktails',
      source_id: `bc_${row.row_id || row.name?.slice(0, 50)}`.replace(/[^a-z0-9_]/gi, '_'),
      metadata: { ingredient: row.ingredient, measure: row.measure }
    })
  },
  
  // Spirits - SAQ (large)
  saq: {
    url: 'https://raw.githubusercontent.com/Physolia/SAQ-LCBO-Crawler/main/SAQ%20Products.csv',
    category: 'spirits',
    map: (row) => ({
      name: row.name?.slice(0, 255),
      category: row.type?.toLowerCase().includes('wine') ? 'wine' : 
               row.type?.toLowerCase().includes('beer') ? 'beer' : 'spirits',
      subcategory: row.type?.slice(0, 100),
      brand: row.producer?.slice(0, 100),
      price: parseFloat(row.price?.replace(/[^0-9.]/g, '')) || null,
      source: 'saq',
      source_id: `saq_${row.saq_code || row.name?.slice(0, 50)}`.replace(/[^a-z0-9_]/gi, '_'),
      metadata: { country: row.country, region: row.region, format: row.format }
    })
  }
};

async function runImport() {
  console.log('');
  console.log('╔══════════════════════════════════════════════════════════════════╗');
  console.log('║         JAVARI SPIRITS - DIRECT DATA IMPORT                      ║');
  console.log('╚══════════════════════════════════════════════════════════════════╝');
  console.log('');
  
  log('Starting import...');
  
  // Test connection first
  log('Testing database connection...');
  const { count, error: testError } = await supabase
    .from('products')
    .select('*', { count: 'exact', head: true });
  
  if (testError) {
    log(`❌ Database error: ${testError.message}`);
    log('Creating products table...');
    
    // Try to create table (might fail if no admin access)
    const { error: createError } = await supabase.rpc('exec_sql', {
      sql: `CREATE TABLE IF NOT EXISTS products (
        id BIGSERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        category VARCHAR(50),
        subcategory VARCHAR(100),
        brand VARCHAR(100),
        description TEXT,
        price DECIMAL(10,2),
        rating DECIMAL(3,1),
        source VARCHAR(50),
        source_id VARCHAR(255),
        metadata JSONB DEFAULT '{}',
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(source, source_id)
      );`
    });
    
    if (createError) {
      log(`Note: Table creation via RPC not available. Table must exist.`);
    }
  } else {
    log(`✅ Connected. Current products: ${count || 0}`);
  }
  
  let totalInserted = 0;
  let totalErrors = 0;
  
  for (const [name, config] of Object.entries(SOURCES)) {
    log(`\nImporting ${name}...`);
    
    try {
      // Download CSV
      log(`  Downloading from ${config.url.slice(0, 60)}...`);
      const csv = await download(config.url);
      
      if (!csv || csv.length < 100) {
        log(`  ⚠️ Empty or invalid response`);
        continue;
      }
      
      // Parse CSV
      const records = parse(csv, { 
        columns: true, 
        skip_empty_lines: true,
        relax_column_count: true 
      });
      
      log(`  Parsed ${records.length} records`);
      
      // Map to products
      const products = records
        .map(config.map)
        .filter(p => p.name && p.name.length > 0);
      
      log(`  Valid products: ${products.length}`);
      
      // Insert
      const { inserted, errors } = await insertProducts(products, name);
      
      log(`  ✅ Inserted: ${inserted}, Errors: ${errors}`);
      totalInserted += inserted;
      totalErrors += errors;
      
    } catch (err) {
      log(`  ❌ Error: ${err.message}`);
      totalErrors++;
    }
  }
  
  // Final count
  const { count: finalCount } = await supabase
    .from('products')
    .select('*', { count: 'exact', head: true });
  
  console.log('');
  console.log('═══════════════════════════════════════════════════════════════════');
  log(`IMPORT COMPLETE`);
  log(`  Total attempted: ${totalInserted + totalErrors}`);
  log(`  Successful: ${totalInserted}`);
  log(`  Errors: ${totalErrors}`);
  log(`  Database total: ${finalCount || 'unknown'}`);
  console.log('═══════════════════════════════════════════════════════════════════');
}

runImport().catch(err => {
  log(`Fatal error: ${err.message}`);
  process.exit(1);
});
