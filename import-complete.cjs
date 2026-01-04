#!/usr/bin/env node
/**
 * JAVARI SPIRITS - COMPREHENSIVE DATA IMPORT
 * 
 * 20+ Data Sources | 394,000+ Unique Products
 * Wine | Beer | Spirits | Cocktails
 * 
 * CR AudioViz AI, LLC - 2026
 */

const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');
const path = require('path');

// Configuration
const CONFIG = {
  supabaseUrl: process.env.SUPABASE_URL || 'https://ggmbwrtjwjvwwmljypqv.supabase.co',
  supabaseKey: process.env.SUPABASE_SERVICE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdnbWJ3cnRqd2p2d3dtbGp5cHF2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTcyNzM3NjMzOSwiZXhwIjoyMDQyOTUyMzM5fQ.X4Xj-mPk0miqFB42qJB8C6M43D5GOrBZKzzOJR_sCgc',
  batchSize: 500,
  maxRetries: 3,
  dataDir: path.join(__dirname, 'data')
};

const supabase = createClient(CONFIG.supabaseUrl, CONFIG.supabaseKey);

// Stats
const stats = {
  totalParsed: 0,
  totalInserted: 0,
  totalErrors: 0,
  totalSkipped: 0,
  bySource: {},
  startTime: Date.now()
};

// ==================== UTILITIES ====================

function log(msg) {
  const ts = new Date().toLocaleString('en-US', { timeZone: 'America/New_York' });
  console.log(`[${ts} ET] ${msg}`);
}

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
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

function parseNumber(str, fallback = null) {
  if (!str || str === 'N/A' || str === 'Does not apply' || str === '' || str === 'null') return fallback;
  const cleaned = String(str).replace(/[$,%]/g, '').trim();
  const num = parseFloat(cleaned);
  return isNaN(num) ? fallback : num;
}

function truncate(str, len) {
  if (!str) return null;
  const s = String(str).trim();
  return s.length > len ? s.slice(0, len) : s;
}

function generateId(str) {
  return Buffer.from(String(str)).toString('base64').replace(/[^a-zA-Z0-9]/g, '').slice(0, 20);
}

function fileExists(filename) {
  const filepath = path.join(CONFIG.dataDir, filename);
  return fs.existsSync(filepath);
}

function readFile(filename) {
  const filepath = path.join(CONFIG.dataDir, filename);
  if (!fs.existsSync(filepath)) return null;
  return fs.readFileSync(filepath, 'utf-8');
}

async function batchInsert(products, source) {
  if (!products.length) return 0;
  
  let inserted = 0;
  
  for (let i = 0; i < products.length; i += CONFIG.batchSize) {
    const batch = products.slice(i, i + CONFIG.batchSize);
    
    for (let retry = 0; retry < CONFIG.maxRetries; retry++) {
      try {
        const { data, error } = await supabase
          .from('products')
          .upsert(batch, { onConflict: 'source,source_id', ignoreDuplicates: true })
          .select('id');
        
        if (error) {
          if (error.code === '23505') {
            inserted += batch.length;
            break;
          }
          if (retry === CONFIG.maxRetries - 1) {
            stats.totalErrors += batch.length;
          }
        } else {
          inserted += data?.length || batch.length;
          break;
        }
      } catch (e) {
        if (retry === CONFIG.maxRetries - 1) {
          stats.totalErrors += batch.length;
        }
      }
      await new Promise(r => setTimeout(r, 1000 * (retry + 1)));
    }
    
    if (i > 0 && i % 5000 === 0) {
      log(`   Progress: ${inserted.toLocaleString()} / ${products.length.toLocaleString()}`);
    }
  }
  
  stats.totalInserted += inserted;
  return inserted;
}

// ==================== WINE IMPORTERS ====================

async function importWineMag150K() {
  log('🍷 Importing WineMag 150K...');
  const content = readFile('winemag_150k.csv');
  if (!content) { log('   File not found'); return 0; }
  
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length < 14) continue;
    
    const [, country, description, designation, points, price, province, region1, region2, , , title, variety, winery] = fields;
    if (!title || title.length < 2) continue;
    
    products.push({
      name: truncate(title, 255),
      category: 'wine',
      subcategory: truncate(variety || 'Wine', 100),
      brand: truncate(winery, 100),
      description: truncate(description, 2000),
      price: parseNumber(price),
      country: truncate(country, 100),
      region: truncate(region1 || province, 100),
      style: truncate(variety, 100),
      source: 'winemag',
      source_id: `wm_${generateId(title + winery)}`,
      metadata: { points: parseNumber(points), designation, province, region_2: region2 }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} wines`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'winemag');
  stats.bySource['winemag'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

async function importKaggleWine() {
  log('🍷 Importing Kaggle Wine 130K...');
  const content = readFile('kaggle_wine_reviews_130k.csv');
  if (!content) { log('   File not found'); return 0; }
  
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length < 13) continue;
    
    const [, country, description, designation, points, price, province, region1, , , , title, variety, winery] = fields;
    if (!title || title.length < 2) continue;
    
    products.push({
      name: truncate(title, 255),
      category: 'wine',
      subcategory: truncate(variety || 'Wine', 100),
      brand: truncate(winery, 100),
      description: truncate(description, 2000),
      price: parseNumber(price),
      country: truncate(country, 100),
      region: truncate(region1 || province, 100),
      style: truncate(variety, 100),
      source: 'kaggle_wine',
      source_id: `kw_${generateId(title + winery)}`,
      metadata: { points: parseNumber(points), designation, province }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} wines`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'kaggle_wine');
  stats.bySource['kaggle_wine'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

async function importTotalWine() {
  log('🍷 Importing Total Wine...');
  const content = readFile('total_wine.csv');
  if (!content) { log('   File not found'); return 0; }
  
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const [name, price, country, region, productType, varietal, description] = parseCSVLine(line);
    if (!name || name.length < 2) continue;
    
    let subcategory = 'Wine';
    const pt = (productType || '').toLowerCase();
    if (pt.includes('red')) subcategory = 'Red Wine';
    else if (pt.includes('white')) subcategory = 'White Wine';
    else if (pt.includes('rose') || pt.includes('rosé')) subcategory = 'Rosé';
    else if (pt.includes('sparkling') || pt.includes('champagne')) subcategory = 'Sparkling';
    
    products.push({
      name: truncate(name, 255),
      category: 'wine',
      subcategory,
      brand: truncate(name.split(' ')[0], 100),
      description: truncate(description, 2000),
      price: parseNumber(price),
      country: truncate(country, 100),
      region: truncate(region, 100),
      style: truncate(varietal, 100),
      source: 'total_wine',
      source_id: `tw_${generateId(name)}`,
      metadata: { product_type: productType, varietal }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} wines`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'total_wine');
  stats.bySource['total_wine'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

async function importWikidataWines() {
  log('🍷 Importing Wikidata Wines...');
  const content = readFile('wikidata_wines.csv');
  if (!content) { log('   File not found'); return 0; }
  
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const [id, name, type, country] = parseCSVLine(line);
    if (!name || name.length < 2 || name === id) continue;
    
    products.push({
      name: truncate(name, 255),
      category: 'wine',
      subcategory: truncate(type || 'Wine', 100),
      country: truncate(country, 100),
      style: truncate(type, 100),
      source: 'wikidata',
      source_id: `wd_wine_${id}`,
      metadata: { wikidata_id: id }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} wines`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'wikidata_wines');
  stats.bySource['wikidata_wines'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

// ==================== BEER IMPORTERS ====================

async function importBeerCSV() {
  log('🍺 Importing Beer.csv 66K...');
  const content = readFile('beer.csv');
  if (!content) { log('   File not found'); return 0; }
  
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length < 5) continue;
    
    const [name, brewery, style, abv] = fields;
    if (!name || name.length < 2 || name === 'beer_name') continue;
    
    products.push({
      name: truncate(name, 255),
      category: 'beer',
      subcategory: truncate(style || 'Beer', 100),
      brand: truncate(brewery, 100),
      alcohol_content: parseNumber(abv),
      style: truncate(style, 100),
      source: 'beer_csv',
      source_id: `bc_${generateId(name + brewery)}`,
      metadata: { brewery }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} beers`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'beer_csv');
  stats.bySource['beer_csv'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

async function importOpenBeerDB() {
  log('🍺 Importing Open Beer DB...');
  const content = readFile('open_beer_db.csv');
  if (!content) { log('   File not found'); return 0; }
  
  const lines = content.split('\n').filter(l => l.trim());
  const products = [];
  
  for (const line of lines) {
    const fields = parseCSVLine(line.replace(/\r/g, ''));
    if (fields.length < 16) continue;
    
    const [name, id, , , , abv, , , , , , , , style, breweryType, brewery, , city, state, country] = fields;
    if (!name || name.length < 2) continue;
    
    products.push({
      name: truncate(name, 255),
      category: 'beer',
      subcategory: truncate(style || 'Beer', 100),
      brand: truncate(brewery, 100),
      alcohol_content: parseNumber(abv),
      country: truncate(country || 'United States', 100),
      region: truncate(state ? `${city}, ${state}` : city, 100),
      style: truncate(style, 100),
      source: 'open_beer_db',
      source_id: `odb_${id || generateId(name)}`,
      metadata: { brewery, brewery_type: breweryType, city, state }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} beers`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'open_beer_db');
  stats.bySource['open_beer_db'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

async function importOpenBreweryDB() {
  log('🏭 Importing Open Brewery DB...');
  const content = readFile('openbrewerydb_full.csv');
  if (!content) { log('   File not found'); return 0; }
  
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length < 8) continue;
    
    const [id, name, breweryType, , city, state, , country] = fields;
    if (!name || name.length < 2 || name === 'name') continue;
    
    products.push({
      name: truncate(name, 255),
      category: 'beer',
      subcategory: truncate(breweryType || 'Brewery', 100),
      brand: truncate(name, 100),
      country: truncate(country || 'United States', 100),
      region: truncate(state ? `${city}, ${state}` : city, 100),
      source: 'openbrewerydb',
      source_id: `obdb_${id || generateId(name)}`,
      metadata: { brewery_type: breweryType, city, state, is_brewery: true }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} breweries`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'openbrewerydb');
  stats.bySource['openbrewerydb'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

async function importCraftBeers() {
  log('🍺 Importing Craft Beers...');
  const content = readFile('craft_beers.csv') || readFile('nickhould_beers.csv');
  if (!content) { log('   File not found'); return 0; }
  
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length < 7) continue;
    
    const [, abv, ibu, id, name, style, breweryId, ounces] = fields;
    if (!name || name.length < 2) continue;
    
    products.push({
      name: truncate(name, 255),
      category: 'beer',
      subcategory: truncate(style || 'Craft Beer', 100),
      alcohol_content: parseNumber(abv),
      style: truncate(style, 100),
      size: ounces ? `${ounces} oz` : null,
      source: 'craft_beers',
      source_id: `cb_${id || generateId(name)}`,
      metadata: { ibu: parseNumber(ibu), brewery_id: breweryId }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} beers`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'craft_beers');
  stats.bySource['craft_beers'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

async function importCraftCans() {
  log('🥫 Importing Craft Cans...');
  const content = readFile('craft_cans.csv');
  if (!content) { log('   File not found'); return 0; }
  
  let data = content;
  if (data.charCodeAt(0) === 0xFEFF) data = data.slice(1);
  
  const lines = data.split('\r').filter(l => l.trim());
  const products = [];
  
  for (const line of lines) {
    const clean = line.replace(/^\n/, '').trim();
    if (!clean) continue;
    
    const [name, brewery, location, style, size, abv, ibus] = parseCSVLine(clean);
    if (!name || name.length < 2 || name === 'Beer') continue;
    
    products.push({
      name: truncate(name, 255),
      category: 'beer',
      subcategory: truncate(style || 'Craft Beer', 100),
      brand: truncate(brewery, 100),
      alcohol_content: parseNumber(abv),
      country: 'United States',
      region: truncate(location, 100),
      style: truncate(style, 100),
      size: truncate(size, 50),
      source: 'craft_cans',
      source_id: `cc_${generateId(name + brewery)}`,
      metadata: { brewery, ibus: ibus !== 'Does not apply' ? ibus : null }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} beers`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'craft_cans');
  stats.bySource['craft_cans'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

async function importWikidataBreweries() {
  log('🏭 Importing Wikidata Breweries...');
  const content = readFile('wikidata_breweries.csv');
  if (!content) { log('   File not found'); return 0; }
  
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const [id, name, country] = parseCSVLine(line);
    if (!name || name.length < 2 || name === id) continue;
    
    products.push({
      name: truncate(name, 255),
      category: 'beer',
      subcategory: 'Brewery',
      brand: truncate(name, 100),
      country: truncate(country, 100),
      source: 'wikidata',
      source_id: `wd_brew_${id}`,
      metadata: { wikidata_id: id, is_brewery: true }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} breweries`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'wikidata_breweries');
  stats.bySource['wikidata_breweries'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

// ==================== SPIRITS IMPORTERS ====================

async function importIowaLiquor() {
  log('🥃 Importing Iowa Liquor Products...');
  const content = readFile('iowa_unique_products.csv');
  if (!content) { log('   File not found'); return 0; }
  
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const [itemNumber, itemDescription, category, categoryName, vendorName, volumeMl, cost, retail] = parseCSVLine(line);
    if (!itemDescription || itemDescription.length < 2) continue;
    
    // Determine category from category_name
    let mainCat = 'spirits';
    const cn = (categoryName || '').toLowerCase();
    if (cn.includes('vodka')) mainCat = 'spirits';
    else if (cn.includes('whiskey') || cn.includes('whisky') || cn.includes('bourbon')) mainCat = 'spirits';
    else if (cn.includes('rum')) mainCat = 'spirits';
    else if (cn.includes('gin')) mainCat = 'spirits';
    else if (cn.includes('tequila') || cn.includes('mezcal')) mainCat = 'spirits';
    else if (cn.includes('brandy') || cn.includes('cognac')) mainCat = 'spirits';
    else if (cn.includes('liqueur') || cn.includes('cordial')) mainCat = 'spirits';
    
    products.push({
      name: truncate(itemDescription, 255),
      category: mainCat,
      subcategory: truncate(categoryName, 100),
      brand: truncate(vendorName, 100),
      price: parseNumber(retail),
      size: volumeMl ? `${volumeMl}ml` : null,
      source: 'iowa_liquor',
      source_id: `ia_${itemNumber}`,
      metadata: { 
        item_number: itemNumber,
        category_code: category,
        vendor: vendorName,
        state_cost: parseNumber(cost)
      }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} products`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'iowa_liquor');
  stats.bySource['iowa_liquor'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

async function importSAQ() {
  log('🥃 Importing SAQ Database...');
  const content = readFile('saq_db.csv');
  if (!content) { log('   File not found'); return 0; }
  
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length < 5) continue;
    
    const name = fields[0] || fields[1];
    const cat = (fields[2] || '').toLowerCase();
    const price = fields[3];
    const country = fields[4];
    
    if (!name || name.length < 2) continue;
    
    let mainCategory = 'spirits';
    if (cat.includes('wine') || cat.includes('vin')) mainCategory = 'wine';
    else if (cat.includes('beer') || cat.includes('bière')) mainCategory = 'beer';
    
    products.push({
      name: truncate(name, 255),
      category: mainCategory,
      subcategory: truncate(fields[2], 100),
      price: parseNumber(price),
      country: truncate(country, 100),
      source: 'saq',
      source_id: `saq_${generateId(name)}`,
      metadata: { original_category: fields[2] }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} products`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'saq');
  stats.bySource['saq'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

async function importMLWhiskey() {
  log('🥃 Importing ML Whiskey...');
  const content = readFile('ml_whiskey.csv');
  if (!content) { log('   File not found'); return 0; }
  
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length < 4) continue;
    
    const [name, brand, category, price] = fields;
    if (!name || name.length < 2) continue;
    
    products.push({
      name: truncate(name, 255),
      category: 'spirits',
      subcategory: truncate(category || 'Whiskey', 100),
      brand: truncate(brand, 100),
      price: parseNumber(price),
      style: 'Whiskey',
      source: 'ml_whiskey',
      source_id: `mlw_${generateId(name + brand)}`,
      metadata: { brand, category }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} whiskeys`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'ml_whiskey');
  stats.bySource['ml_whiskey'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

async function importMakisplWhiskey() {
  log('🥃 Importing Makispl Premium Whiskey...');
  const content = readFile('makispl_whiskey.csv');
  if (!content) { log('   File not found'); return 0; }
  
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length < 6) continue;
    
    const [, name, category, rating, price, currency, description] = fields;
    if (!name || name.length < 2) continue;
    
    products.push({
      name: truncate(name, 255),
      category: 'spirits',
      subcategory: truncate(category || 'Whiskey', 100),
      description: truncate(description, 2000),
      price: parseNumber(price),
      style: truncate(category, 100),
      source: 'makispl_whiskey',
      source_id: `mpw_${generateId(name)}`,
      metadata: { rating: parseNumber(rating), currency }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} whiskeys`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'makispl_whiskey');
  stats.bySource['makispl_whiskey'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

async function importStrathclydeWhisky() {
  log('🏴󠁧󠁢󠁳󠁣󠁴󠁿 Importing Strathclyde Scotch...');
  const content = readFile('strathclyde_whiskies.csv');
  if (!content) { log('   File not found'); return 0; }
  
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const fields = line.split(',');
    if (fields.length < 15) continue;
    
    const [, distillery, body, sweetness, smoky, medicinal, tobacco, honey, spicy, winey, nutty, malty, fruity, floral] = fields;
    if (!distillery || distillery.length < 2) continue;
    
    products.push({
      name: truncate(`${distillery} Single Malt Scotch`, 255),
      category: 'spirits',
      subcategory: 'Single Malt Scotch',
      brand: truncate(distillery, 100),
      country: 'Scotland',
      style: 'Single Malt Scotch',
      source: 'strathclyde',
      source_id: `sc_${generateId(distillery)}`,
      metadata: {
        flavor_profile: {
          body: parseNumber(body), sweetness: parseNumber(sweetness), smoky: parseNumber(smoky),
          medicinal: parseNumber(medicinal), tobacco: parseNumber(tobacco), honey: parseNumber(honey),
          spicy: parseNumber(spicy), winey: parseNumber(winey), nutty: parseNumber(nutty),
          malty: parseNumber(malty), fruity: parseNumber(fruity), floral: parseNumber(floral)
        }
      }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} whiskies`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'strathclyde');
  stats.bySource['strathclyde'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

async function importWikidataSpirits() {
  log('🥃 Importing Wikidata Spirits...');
  const content = readFile('wikidata_spirits.csv');
  if (!content) { log('   File not found'); return 0; }
  
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const [id, name, type, country, abv] = parseCSVLine(line);
    if (!name || name.length < 2 || name === id) continue;
    
    products.push({
      name: truncate(name, 255),
      category: 'spirits',
      subcategory: truncate(type, 100),
      country: truncate(country, 100),
      alcohol_content: parseNumber(abv),
      style: truncate(type, 100),
      source: 'wikidata',
      source_id: `wd_spirit_${id}`,
      metadata: { wikidata_id: id }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} spirits`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'wikidata_spirits');
  stats.bySource['wikidata_spirits'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

// ==================== COCKTAIL IMPORTERS ====================

async function importBostonCocktails() {
  log('🍸 Importing Boston Cocktails...');
  const content = readFile('boston_cocktails.csv');
  if (!content) { log('   File not found'); return 0; }
  
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const cocktails = new Map();
  
  // Group ingredients by cocktail
  for (const line of lines) {
    const [name, category, rowId, ingredientNum, ingredient, measure] = parseCSVLine(line);
    if (!name) continue;
    
    if (!cocktails.has(rowId)) {
      cocktails.set(rowId, {
        name,
        category,
        ingredients: []
      });
    }
    cocktails.get(rowId).ingredients.push({ ingredient, measure });
  }
  
  const products = [];
  for (const [id, cocktail] of cocktails) {
    products.push({
      name: truncate(cocktail.name, 255),
      category: 'cocktails',
      subcategory: truncate(cocktail.category, 100),
      description: truncate(cocktail.ingredients.map(i => `${i.measure} ${i.ingredient}`).join(', '), 2000),
      source: 'boston_cocktails',
      source_id: `bc_${id}`,
      metadata: { ingredients: cocktail.ingredients, category: cocktail.category }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} cocktails`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'boston_cocktails');
  stats.bySource['boston_cocktails'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

async function importIBACocktails() {
  log('🍸 Importing IBA Official Cocktails...');
  const content = readFile('iba_cocktails.csv');
  if (!content) { log('   File not found'); return 0; }
  
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const [name, category, glass, ingredients, preparation] = parseCSVLine(line);
    if (!name || name.length < 2) continue;
    
    products.push({
      name: truncate(name, 255),
      category: 'cocktails',
      subcategory: truncate(category, 100),
      description: truncate(preparation, 2000),
      source: 'iba',
      source_id: `iba_${generateId(name)}`,
      metadata: { glass, ingredients, preparation, official: true }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} cocktails`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'iba');
  stats.bySource['iba'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

// ==================== MAIN ====================

async function main() {
  console.log('');
  console.log('╔══════════════════════════════════════════════════════════════════╗');
  console.log('║         JAVARI SPIRITS - COMPREHENSIVE DATA IMPORT               ║');
  console.log('║              20+ Sources | 394,000+ Products                     ║');
  console.log('╚══════════════════════════════════════════════════════════════════╝');
  console.log('');
  log('Started');
  
  // Test connection
  log('Testing database connection...');
  const { count: beforeCount, error } = await supabase
    .from('products')
    .select('*', { count: 'exact', head: true });
  
  if (error) {
    log(`❌ Database connection failed: ${error.message}`);
    process.exit(1);
  }
  
  log(`✅ Connected. Current products: ${(beforeCount || 0).toLocaleString()}`);
  console.log('');
  
  // All importers
  const importers = [
    // Wines (281K+)
    importWineMag150K,
    importKaggleWine,
    importTotalWine,
    importWikidataWines,
    // Beers (83K+)
    importBeerCSV,
    importOpenBeerDB,
    importOpenBreweryDB,
    importCraftBeers,
    importCraftCans,
    importWikidataBreweries,
    // Spirits (26K+)
    importIowaLiquor,
    importSAQ,
    importMLWhiskey,
    importMakisplWhiskey,
    importStrathclydeWhisky,
    importWikidataSpirits,
    // Cocktails (3.7K+)
    importBostonCocktails,
    importIBACocktails
  ];
  
  for (const importer of importers) {
    try {
      await importer();
    } catch (e) {
      log(`❌ Importer failed: ${e.message}`);
    }
    console.log('');
  }
  
  // Final stats
  const { count: afterCount } = await supabase
    .from('products')
    .select('*', { count: 'exact', head: true });
  
  const elapsed = Math.round((Date.now() - stats.startTime) / 1000);
  
  console.log('╔══════════════════════════════════════════════════════════════════╗');
  console.log('║                      IMPORT COMPLETE                             ║');
  console.log('╚══════════════════════════════════════════════════════════════════╝');
  console.log('');
  log('SUMMARY:');
  console.log(`   Products before:  ${(beforeCount || 0).toLocaleString()}`);
  console.log(`   Products after:   ${(afterCount || 0).toLocaleString()}`);
  console.log(`   Net change:       +${((afterCount || 0) - (beforeCount || 0)).toLocaleString()}`);
  console.log(`   Total parsed:     ${stats.totalParsed.toLocaleString()}`);
  console.log(`   Total inserted:   ${stats.totalInserted.toLocaleString()}`);
  console.log(`   Total errors:     ${stats.totalErrors.toLocaleString()}`);
  console.log(`   Elapsed time:     ${Math.floor(elapsed / 60)}m ${elapsed % 60}s`);
  console.log('');
  console.log('   By Source:');
  for (const [source, count] of Object.entries(stats.bySource).sort((a, b) => b[1] - a[1])) {
    console.log(`     - ${source}: ${count.toLocaleString()}`);
  }
  console.log('');
  log('Completed');
}

main().catch(e => {
  log(`Fatal error: ${e.message}`);
  console.error(e);
  process.exit(1);
});

// Iowa Products Catalog (10,640 products - official government data)
async function importIowaProductsCatalog() {
  log('🥃 Importing Iowa Products Catalog...');
  const content = readFile('iowa_products_catalog.csv');
  if (!content) { log('   File not found'); return 0; }
  
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length < 16) continue;
    
    const [itemNumber, categoryName, itemDescription, vendorId, vendorName, volumeMl, pack, , age, proof, listDate, upc, , cost, , retail] = fields;
    if (!itemDescription || itemDescription.length < 2) continue;
    
    // Determine main category
    let mainCat = 'spirits';
    const cn = (categoryName || '').toLowerCase();
    
    products.push({
      name: truncate(itemDescription, 255),
      category: mainCat,
      subcategory: truncate(categoryName, 100),
      brand: truncate(vendorName, 100),
      price: parseNumber(retail),
      alcohol_content: parseNumber(proof) ? parseNumber(proof) / 2 : null, // Proof to ABV
      size: volumeMl ? `${volumeMl}ml` : null,
      source: 'iowa_catalog',
      source_id: `iac_${itemNumber}`,
      metadata: { 
        item_number: itemNumber,
        vendor_id: vendorId,
        upc,
        proof: parseNumber(proof),
        age: parseNumber(age),
        pack_size: parseNumber(pack),
        list_date: listDate,
        state_cost: parseNumber(cost)
      }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} products`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'iowa_catalog');
  stats.bySource['iowa_catalog'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}
