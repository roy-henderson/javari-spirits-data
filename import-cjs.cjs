const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');
const path = require('path');

const supabaseUrl = 'https://ggmbwrtjwjvwwmljypqv.supabase.co';
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdnbWJ3cnRqd2p2d3dtbGp5cHF2Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTcyNzM3NjMzOSwiZXhwIjoyMDQyOTUyMzM5fQ.X4Xj-mPk0miqFB42qJB8C6M43D5GOrBZKzzOJR_sCgc';

const supabase = createClient(supabaseUrl, supabaseKey);

// Parse CSV handling quotes and commas
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

// Parse price string to number
function parsePrice(priceStr) {
  if (!priceStr) return undefined;
  const cleaned = priceStr.replace(/[$,]/g, '').trim();
  const num = parseFloat(cleaned);
  return isNaN(num) ? undefined : num;
}

// Parse ABV string to number
function parseABV(abvStr) {
  if (!abvStr || abvStr === 'Does not apply') return undefined;
  const cleaned = abvStr.replace(/%/g, '').trim();
  const num = parseFloat(cleaned);
  return isNaN(num) ? undefined : num;
}

// Generate unique ID
function generateId(str) {
  return Buffer.from(str).toString('base64').replace(/[^a-zA-Z0-9]/g, '').slice(0, 20);
}

// Import Total Wine data
async function importTotalWine() {
  console.log('\n📦 Importing Total Wine data...');
  
  const filePath = path.join(__dirname, 'total_wine.csv');
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(l => l.trim());
  
  // Skip header
  const dataLines = lines.slice(1);
  const products = [];
  
  for (const line of dataLines) {
    const fields = parseCSVLine(line);
    if (fields.length < 7) continue;
    
    const [wineName, price, countryState, region, productType, varietalType, description] = fields;
    
    if (!wineName || wineName.length < 2) continue;
    
    // Determine subcategory from product type
    let subcategory = productType || 'Wine';
    if (productType && productType.toLowerCase().includes('red')) subcategory = 'Red Wine';
    else if (productType && productType.toLowerCase().includes('white')) subcategory = 'White Wine';
    else if (productType && (productType.toLowerCase().includes('rose') || productType.toLowerCase().includes('rosé'))) subcategory = 'Rosé';
    else if (productType && productType.toLowerCase().includes('sparkling')) subcategory = 'Sparkling';
    
    products.push({
      name: wineName.slice(0, 255),
      category: 'wine',
      subcategory: subcategory.slice(0, 100),
      brand: wineName.split(' ')[0].slice(0, 100),
      description: description ? description.slice(0, 2000) : null,
      price: parsePrice(price),
      country: countryState ? countryState.slice(0, 100) : null,
      region: region ? region.slice(0, 100) : null,
      style: varietalType ? varietalType.slice(0, 100) : null,
      source: 'total_wine',
      source_id: `tw_${generateId(wineName)}`,
      metadata: { product_type: productType, varietal: varietalType }
    });
  }
  
  console.log(`   Parsed ${products.length} wines`);
  
  // Batch insert
  let inserted = 0;
  const batchSize = 200;
  
  for (let i = 0; i < products.length; i += batchSize) {
    const batch = products.slice(i, i + batchSize);
    const { error, data } = await supabase.from('products').insert(batch).select('id');
    
    if (error) {
      console.error(`   Batch ${i}-${i+batchSize} error:`, error.message);
      // Try individual inserts for debugging
      if (i === 0) {
        console.log('   Sample record:', JSON.stringify(batch[0], null, 2));
      }
    } else {
      inserted += data?.length || 0;
      if (i % 500 === 0) console.log(`   Progress: ${inserted} inserted...`);
    }
  }
  
  console.log(`   ✅ Inserted ${inserted} Total Wine products`);
  return inserted;
}

// Import Open Beer DB
async function importOpenBeerDB() {
  console.log('\n🍺 Importing Open Beer DB...');
  
  const filePath = path.join(__dirname, 'open_beer_db.csv');
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').filter(l => l.trim());
  
  const products = [];
  
  for (const line of lines) {
    const cleanLine = line.replace(/\r/g, '');
    const fields = parseCSVLine(cleanLine);
    
    if (fields.length < 16) continue;
    
    const beerName = fields[0];
    const beerId = fields[1];
    const abv = fields[5];
    const style = fields[13];
    const breweryType = fields[14];
    const breweryName = fields[15];
    const city = fields[17];
    const state = fields[18];
    const country = fields[19] || 'United States';
    
    if (!beerName || beerName.length < 2) continue;
    
    products.push({
      name: beerName.slice(0, 255),
      category: 'beer',
      subcategory: style ? style.slice(0, 100) : 'Beer',
      brand: breweryName ? breweryName.slice(0, 100) : null,
      alcohol_content: parseABV(abv),
      country: country.slice(0, 100),
      region: state ? `${city || ''}, ${state}`.slice(0, 100) : (city || '').slice(0, 100),
      style: style ? style.slice(0, 100) : null,
      source: 'open_beer_db',
      source_id: `odb_${beerId || generateId(beerName)}`,
      metadata: { brewery_name: breweryName, brewery_type: breweryType, city, state }
    });
  }
  
  console.log(`   Parsed ${products.length} beers`);
  
  let inserted = 0;
  const batchSize = 200;
  
  for (let i = 0; i < products.length; i += batchSize) {
    const batch = products.slice(i, i + batchSize);
    const { error, data } = await supabase.from('products').insert(batch).select('id');
    
    if (error) {
      console.error(`   Batch ${i}-${i+batchSize} error:`, error.message);
      if (i === 0) console.log('   Sample:', JSON.stringify(batch[0], null, 2));
    } else {
      inserted += data?.length || 0;
      if (i % 1000 === 0) console.log(`   Progress: ${inserted} inserted...`);
    }
  }
  
  console.log(`   ✅ Inserted ${inserted} Open Beer DB products`);
  return inserted;
}

// Import Craft Cans data
async function importCraftCans() {
  console.log('\n🥫 Importing Craft Cans data...');
  
  const filePath = path.join(__dirname, 'craft_cans.csv');
  let content = fs.readFileSync(filePath, 'utf-8');
  
  // Remove BOM
  if (content.charCodeAt(0) === 0xFEFF) content = content.slice(1);
  
  // Split by carriage return
  const lines = content.split('\r').filter(l => l.trim());
  
  const products = [];
  
  for (const line of lines) {
    const cleanLine = line.replace(/^\n/, '').trim();
    if (!cleanLine) continue;
    
    const fields = parseCSVLine(cleanLine);
    if (fields.length < 6) continue;
    
    const [beerName, brewery, location, style, size, abv, ibus] = fields;
    
    // Skip header
    if (beerName === 'Beer' || beerName.includes('Beer,Brewery')) continue;
    if (!beerName || beerName.length < 2) continue;
    
    // Parse location
    let city = '', state = '';
    if (location) {
      const parts = location.split(',').map(p => p.trim());
      city = parts[0] || '';
      state = parts[1] || '';
    }
    
    products.push({
      name: beerName.slice(0, 255),
      category: 'beer',
      subcategory: style ? style.slice(0, 100) : 'Craft Beer',
      brand: brewery ? brewery.slice(0, 100) : null,
      alcohol_content: parseABV(abv),
      country: 'United States',
      region: location ? location.slice(0, 100) : null,
      style: style ? style.slice(0, 100) : null,
      size: size ? size.slice(0, 50) : null,
      source: 'craft_cans',
      source_id: `cc_${generateId(beerName + (brewery || ''))}`,
      metadata: { brewery, city, state, size, ibus: ibus !== 'Does not apply' ? ibus : null }
    });
  }
  
  console.log(`   Parsed ${products.length} beers`);
  
  let inserted = 0;
  const batchSize = 200;
  
  for (let i = 0; i < products.length; i += batchSize) {
    const batch = products.slice(i, i + batchSize);
    const { error, data } = await supabase.from('products').insert(batch).select('id');
    
    if (error) {
      console.error(`   Batch ${i}-${i+batchSize} error:`, error.message);
      if (i === 0) console.log('   Sample:', JSON.stringify(batch[0], null, 2));
    } else {
      inserted += data?.length || 0;
    }
  }
  
  console.log(`   ✅ Inserted ${inserted} Craft Cans products`);
  return inserted;
}

// Main
async function main() {
  console.log('🚀 JAVARI SPIRITS - Multi-Source Import');
  console.log('=' .repeat(50));
  console.log(`Started: ${new Date().toLocaleString('en-US', { timeZone: 'America/New_York' })} ET`);
  
  // Test connection first
  console.log('\n🔌 Testing database connection...');
  const { count: beforeCount, error: countError } = await supabase
    .from('products')
    .select('*', { count: 'exact', head: true });
  
  if (countError) {
    console.error('Database connection error:', countError);
    return;
  }
  
  console.log(`📊 Products before import: ${(beforeCount || 0).toLocaleString()}`);
  
  let totalInserted = 0;
  
  try {
    totalInserted += await importTotalWine();
    totalInserted += await importOpenBeerDB();
    totalInserted += await importCraftCans();
  } catch (error) {
    console.error('Import error:', error);
  }
  
  // Final count
  const { count: afterCount } = await supabase
    .from('products')
    .select('*', { count: 'exact', head: true });
  
  console.log('\n' + '=' .repeat(50));
  console.log('📈 IMPORT SUMMARY');
  console.log('=' .repeat(50));
  console.log(`   Products before: ${(beforeCount || 0).toLocaleString()}`);
  console.log(`   Products after:  ${(afterCount || 0).toLocaleString()}`);
  console.log(`   Total inserted:  ${totalInserted.toLocaleString()}`);
  console.log(`   Net change:      ${((afterCount || 0) - (beforeCount || 0)).toLocaleString()}`);
  console.log(`\nCompleted: ${new Date().toLocaleString('en-US', { timeZone: 'America/New_York' })} ET`);
}

main().catch(console.error);
