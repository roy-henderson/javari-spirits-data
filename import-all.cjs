const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');
const path = require('path');

// Configuration
const CONFIG = {
  supabaseUrl: process.env.SUPABASE_URL || 'https://ggmbwrtjwjvwwmljypqv.supabase.co',
  supabaseKey: process.env.SUPABASE_SERVICE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdnbWJ3cnRqd2p2d3dtbGp5cHF2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTcyNzM3NjMzOSwiZXhwIjoyMDQyOTUyMzM5fQ.X4Xj-mPk0miqFB42qJB8C6M43D5GOrBZKzzOJR_sCgc',
  batchSize: 500,
  maxRetries: 3
};

const supabase = createClient(CONFIG.supabaseUrl, CONFIG.supabaseKey);

// Stats tracking
const stats = {
  totalParsed: 0,
  totalInserted: 0,
  totalErrors: 0,
  bySource: {}
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
  if (!str || str === 'N/A' || str === 'Does not apply' || str === '') return fallback;
  const cleaned = str.replace(/[$,%]/g, '').trim();
  const num = parseFloat(cleaned);
  return isNaN(num) ? fallback : num;
}

function truncate(str, len) {
  if (!str) return null;
  return str.length > len ? str.slice(0, len) : str;
}

function generateId(str) {
  return Buffer.from(str).toString('base64').replace(/[^a-zA-Z0-9]/g, '').slice(0, 20);
}

async function batchInsert(products, source) {
  let inserted = 0;
  
  for (let i = 0; i < products.length; i += CONFIG.batchSize) {
    const batch = products.slice(i, i + CONFIG.batchSize);
    
    for (let retry = 0; retry < CONFIG.maxRetries; retry++) {
      try {
        const { data, error } = await supabase.from('products').insert(batch).select('id');
        
        if (error) {
          if (error.code === '23505') { // Duplicate key
            inserted += batch.length; // Count as success, already exists
            break;
          }
          if (retry === CONFIG.maxRetries - 1) {
            log(`   Batch ${i}-${i+CONFIG.batchSize} failed: ${error.message}`);
            stats.totalErrors += batch.length;
          }
        } else {
          inserted += data?.length || 0;
          break;
        }
      } catch (e) {
        if (retry === CONFIG.maxRetries - 1) {
          log(`   Batch ${i}-${i+CONFIG.batchSize} exception: ${e.message}`);
          stats.totalErrors += batch.length;
        }
      }
      
      await new Promise(r => setTimeout(r, 1000 * (retry + 1))); // Backoff
    }
    
    if (i > 0 && i % 5000 === 0) {
      log(`   Progress: ${inserted.toLocaleString()} inserted...`);
    }
  }
  
  return inserted;
}

// ==================== IMPORTERS ====================

// 1. Total Wine (1,558 wines)
async function importTotalWine() {
  log('📦 Importing Total Wine...');
  const filePath = path.join(__dirname, 'data', 'total_wine.csv');
  if (!fs.existsSync(filePath)) { log('   File not found, skipping'); return 0; }
  
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const [name, price, country, region, productType, varietal, description] = parseCSVLine(line);
    if (!name || name.length < 2) continue;
    
    let subcategory = 'Wine';
    if (productType?.toLowerCase().includes('red')) subcategory = 'Red Wine';
    else if (productType?.toLowerCase().includes('white')) subcategory = 'White Wine';
    else if (productType?.toLowerCase().includes('rose')) subcategory = 'Rosé';
    else if (productType?.toLowerCase().includes('sparkling')) subcategory = 'Sparkling';
    
    products.push({
      name: truncate(name, 255),
      category: 'wine',
      subcategory: truncate(subcategory, 100),
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

// 2. Open Beer DB (5,900 beers)
async function importOpenBeerDB() {
  log('🍺 Importing Open Beer DB...');
  const filePath = path.join(__dirname, 'data', 'open_beer_db.csv');
  if (!fs.existsSync(filePath)) { log('   File not found, skipping'); return 0; }
  
  const content = fs.readFileSync(filePath, 'utf-8');
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

// 3. Craft Cans (2,400+ craft beers)
async function importCraftCans() {
  log('🥫 Importing Craft Cans...');
  const filePath = path.join(__dirname, 'data', 'craft_cans.csv');
  if (!fs.existsSync(filePath)) { log('   File not found, skipping'); return 0; }
  
  let content = fs.readFileSync(filePath, 'utf-8');
  if (content.charCodeAt(0) === 0xFEFF) content = content.slice(1);
  
  const lines = content.split('\r').filter(l => l.trim());
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
      source_id: `cc_${generateId(name + (brewery || ''))}`,
      metadata: { brewery, size, ibus: ibus !== 'Does not apply' ? ibus : null }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} beers`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'craft_cans');
  stats.bySource['craft_cans'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

// 4. WineMag 150K (150,000 wine reviews)
async function importWineMag150K() {
  log('🍷 Importing WineMag 150K...');
  const filePath = path.join(__dirname, 'data', 'winemag_150k.csv');
  if (!fs.existsSync(filePath)) { log('   File not found, skipping'); return 0; }
  
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const fields = parseCSVLine(line);
    // Format: id, country, description, designation, points, price, province, region_1, region_2, taster_name, taster_twitter, title, variety, winery
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
      source_id: `wm_${generateId(title + (winery || ''))}`,
      metadata: { 
        points: parseNumber(points),
        designation,
        province,
        region_2: region2,
        winery
      }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} wines`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'winemag');
  stats.bySource['winemag'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

// 5. Kaggle Wine Reviews (130,000 wines)
async function importKaggleWine() {
  log('🍷 Importing Kaggle Wine Reviews...');
  const filePath = path.join(__dirname, 'data', 'kaggle_wine_reviews_130k.csv');
  if (!fs.existsSync(filePath)) { log('   File not found, skipping'); return 0; }
  
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length < 13) continue;
    
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
      source: 'kaggle_wine',
      source_id: `kw_${generateId(title + (winery || ''))}`,
      metadata: { points: parseNumber(points), designation, province, winery }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} wines`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'kaggle_wine');
  stats.bySource['kaggle_wine'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

// 6. Beer.csv (66,000 beers)
async function importBeerCSV() {
  log('🍺 Importing Beer.csv...');
  const filePath = path.join(__dirname, 'data', 'beer.csv');
  if (!fs.existsSync(filePath)) { log('   File not found, skipping'); return 0; }
  
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length < 5) continue;
    
    // Detect format based on fields
    const name = fields[0];
    const brewery = fields[1];
    const style = fields[2];
    const abv = fields[3];
    
    if (!name || name.length < 2 || name === 'beer_name') continue;
    
    products.push({
      name: truncate(name, 255),
      category: 'beer',
      subcategory: truncate(style || 'Beer', 100),
      brand: truncate(brewery, 100),
      alcohol_content: parseNumber(abv),
      style: truncate(style, 100),
      source: 'beer_csv',
      source_id: `bc_${generateId(name + (brewery || ''))}`,
      metadata: { brewery, style }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} beers`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'beer_csv');
  stats.bySource['beer_csv'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

// 7. SAQ Database (Quebec liquor - 12,000 products)
async function importSAQ() {
  log('🥃 Importing SAQ Database...');
  const filePath = path.join(__dirname, 'data', 'saq_db.csv');
  if (!fs.existsSync(filePath)) { log('   File not found, skipping'); return 0; }
  
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length < 5) continue;
    
    const name = fields[0] || fields[1];
    const category = fields[2]?.toLowerCase() || 'spirits';
    const price = fields[3];
    const country = fields[4];
    
    if (!name || name.length < 2) continue;
    
    let mainCategory = 'spirits';
    if (category.includes('wine') || category.includes('vin')) mainCategory = 'wine';
    else if (category.includes('beer') || category.includes('bière')) mainCategory = 'beer';
    
    products.push({
      name: truncate(name, 255),
      category: mainCategory,
      subcategory: truncate(category, 100),
      price: parseNumber(price),
      country: truncate(country, 100),
      source: 'saq',
      source_id: `saq_${generateId(name)}`,
      metadata: { original_category: category }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} products`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'saq');
  stats.bySource['saq'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

// 8. ML Whiskey (3,000+ whiskeys)
async function importMLWhiskey() {
  log('🥃 Importing ML Whiskey...');
  const filePath = path.join(__dirname, 'data', 'ml_whiskey.csv');
  if (!fs.existsSync(filePath)) { log('   File not found, skipping'); return 0; }
  
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length < 4) continue;
    
    const name = fields[0];
    const brand = fields[1];
    const category = fields[2];
    const price = fields[3];
    
    if (!name || name.length < 2) continue;
    
    products.push({
      name: truncate(name, 255),
      category: 'spirits',
      subcategory: truncate(category || 'Whiskey', 100),
      brand: truncate(brand, 100),
      price: parseNumber(price),
      style: 'Whiskey',
      source: 'ml_whiskey',
      source_id: `mlw_${generateId(name + (brand || ''))}`,
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

// 9. Open Brewery DB (9,000 breweries)
async function importOpenBreweryDB() {
  log('🏭 Importing Open Brewery DB...');
  const filePath = path.join(__dirname, 'data', 'openbrewerydb_full.csv');
  if (!fs.existsSync(filePath)) { log('   File not found, skipping'); return 0; }
  
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(l => l.trim()).slice(1);
  const products = [];
  
  for (const line of lines) {
    const fields = parseCSVLine(line);
    if (fields.length < 8) continue;
    
    // id, name, brewery_type, street, city, state, postal_code, country, ...
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

// ==================== MAIN ====================

async function main() {
  console.log('');
  console.log('╔══════════════════════════════════════════════════════════════╗');
  console.log('║       JAVARI SPIRITS - Comprehensive Data Import             ║');
  console.log('║              Automated Multi-Source Pipeline                 ║');
  console.log('╚══════════════════════════════════════════════════════════════╝');
  console.log('');
  log('Started');
  
  // Test connection
  log('Testing database connection...');
  const { count: beforeCount, error } = await supabase
    .from('products')
    .select('*', { count: 'exact', head: true });
  
  if (error) {
    log(`❌ Database connection failed: ${error.message}`);
    log('   Check SUPABASE_URL and SUPABASE_SERVICE_KEY');
    process.exit(1);
  }
  
  log(`✅ Connected. Current products: ${(beforeCount || 0).toLocaleString()}`);
  console.log('');
  
  // Run all importers
  const importers = [
    importTotalWine,
    importOpenBeerDB,
    importCraftCans,
    importWineMag150K,
    importKaggleWine,
    importBeerCSV,
    importSAQ,
    importMLWhiskey,
    importOpenBreweryDB,
    importNickhould,
    importMakisplWhiskey,
    importStrathclydeWhisky
  ];
  
  for (const importer of importers) {
    try {
      const count = await importer();
      stats.totalInserted += count;
    } catch (e) {
      log(`❌ Importer failed: ${e.message}`);
    }
    console.log('');
  }
  
  // Final stats
  const { count: afterCount } = await supabase
    .from('products')
    .select('*', { count: 'exact', head: true });
  
  console.log('╔══════════════════════════════════════════════════════════════╗');
  console.log('║                    IMPORT COMPLETE                           ║');
  console.log('╚══════════════════════════════════════════════════════════════╝');
  console.log('');
  log('SUMMARY:');
  console.log(`   Products before:  ${(beforeCount || 0).toLocaleString()}`);
  console.log(`   Products after:   ${(afterCount || 0).toLocaleString()}`);
  console.log(`   Total parsed:     ${stats.totalParsed.toLocaleString()}`);
  console.log(`   Total inserted:   ${stats.totalInserted.toLocaleString()}`);
  console.log(`   Total errors:     ${stats.totalErrors.toLocaleString()}`);
  console.log(`   Net change:       +${((afterCount || 0) - (beforeCount || 0)).toLocaleString()}`);
  console.log('');
  console.log('   By Source:');
  for (const [source, count] of Object.entries(stats.bySource)) {
    console.log(`     - ${source}: ${count.toLocaleString()}`);
  }
  console.log('');
  log('Completed');
}

main().catch(e => {
  log(`Fatal error: ${e.message}`);
  process.exit(1);
});

// 10. Nickhould Craft Beers (2,400 beers with ABV/IBU)
async function importNickhould() {
  log('🍺 Importing Nickhould Craft Beers...');
  const filePath = path.join(__dirname, 'data', 'nickhould_beers.csv');
  if (!fs.existsSync(filePath)) { log('   File not found, skipping'); return 0; }
  
  const content = fs.readFileSync(filePath, 'utf-8');
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
      source: 'nickhould',
      source_id: `nh_${id || generateId(name)}`,
      metadata: { ibu: parseNumber(ibu), brewery_id: breweryId, ounces }
    });
  }
  
  log(`   Parsed ${products.length.toLocaleString()} beers`);
  stats.totalParsed += products.length;
  const inserted = await batchInsert(products, 'nickhould');
  stats.bySource['nickhould'] = inserted;
  log(`   ✅ Inserted ${inserted.toLocaleString()}`);
  return inserted;
}

// 11. Makispl Whiskey (3,000+ premium whiskeys with ratings)
async function importMakisplWhiskey() {
  log('🥃 Importing Makispl Premium Whiskey...');
  const filePath = path.join(__dirname, 'data', 'makispl_whiskey.csv');
  if (!fs.existsSync(filePath)) { log('   File not found, skipping'); return 0; }
  
  const content = fs.readFileSync(filePath, 'utf-8');
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

// 12. Strathclyde Scotch Whiskies (86 single malts with flavor profiles)
async function importStrathclydeWhisky() {
  log('🏴󠁧󠁢󠁳󠁣󠁴󠁿 Importing Strathclyde Scotch Whiskies...');
  const filePath = path.join(__dirname, 'data', 'strathclyde_whiskies.csv');
  if (!fs.existsSync(filePath)) { log('   File not found, skipping'); return 0; }
  
  const content = fs.readFileSync(filePath, 'utf-8');
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
          body: parseNumber(body),
          sweetness: parseNumber(sweetness),
          smoky: parseNumber(smoky),
          medicinal: parseNumber(medicinal),
          tobacco: parseNumber(tobacco),
          honey: parseNumber(honey),
          spicy: parseNumber(spicy),
          winey: parseNumber(winey),
          nutty: parseNumber(nutty),
          malty: parseNumber(malty),
          fruity: parseNumber(fruity),
          floral: parseNumber(floral)
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
