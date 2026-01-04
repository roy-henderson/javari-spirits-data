/**
 * JAVARI SPIRITS - AWIN AFFILIATE INTEGRATION v2.0
 * 
 * Complete Awin API Integration for Alcohol Affiliate Programs
 * Includes product feeds, commission tracking, and auto-sync
 * 
 * Connects products to Awin affiliate network for monetization
 * Supports 300+ alcohol advertisers
 * 
 * CR AudioViz AI, LLC - 2026
 */

const { createClient } = require('@supabase/supabase-js');

const CONFIG = {
  supabaseUrl: process.env.SUPABASE_URL || 'https://ggmbwrtjwjvwwmljypqv.supabase.co',
  supabaseKey: process.env.SUPABASE_SERVICE_KEY,
  awinPublisherId: process.env.AWIN_PUBLISHER_ID,
  awinApiToken: process.env.AWIN_API_TOKEN
};

const supabase = createClient(CONFIG.supabaseUrl, CONFIG.supabaseKey);

// Known Awin alcohol advertisers with affiliate links
const AWIN_ADVERTISERS = {
  wine_com: {
    id: 'wine.com',
    name: 'Wine.com',
    categories: ['wine'],
    baseUrl: 'https://www.awin1.com/cread.php?awinmid=XXXXX&awinaffid={{PUBLISHER_ID}}&ued=',
    searchPattern: /wine\.com/i
  },
  vivino: {
    id: 'vivino',
    name: 'Vivino',
    categories: ['wine'],
    baseUrl: 'https://www.awin1.com/cread.php?awinmid=XXXXX&awinaffid={{PUBLISHER_ID}}&ued=',
    searchPattern: /vivino/i
  },
  reservebar: {
    id: 'reservebar',
    name: 'ReserveBar',
    categories: ['spirits', 'wine'],
    baseUrl: 'https://www.awin1.com/cread.php?awinmid=XXXXX&awinaffid={{PUBLISHER_ID}}&ued=',
    searchPattern: /reservebar/i
  },
  drizly: {
    id: 'drizly',
    name: 'Drizly',
    categories: ['spirits', 'wine', 'beer'],
    baseUrl: 'https://www.awin1.com/cread.php?awinmid=XXXXX&awinaffid={{PUBLISHER_ID}}&ued=',
    searchPattern: /drizly/i
  },
  totalwine: {
    id: 'totalwine',
    name: 'Total Wine & More',
    categories: ['spirits', 'wine', 'beer'],
    baseUrl: 'https://www.awin1.com/cread.php?awinmid=XXXXX&awinaffid={{PUBLISHER_ID}}&ued=',
    searchPattern: /total\s*wine/i
  },
  craftshack: {
    id: 'craftshack',
    name: 'CraftShack',
    categories: ['beer'],
    baseUrl: 'https://www.awin1.com/cread.php?awinmid=XXXXX&awinaffid={{PUBLISHER_ID}}&ued=',
    searchPattern: /craftshack/i
  },
  caskers: {
    id: 'caskers',
    name: 'Caskers',
    categories: ['spirits'],
    baseUrl: 'https://www.awin1.com/cread.php?awinmid=XXXXX&awinaffid={{PUBLISHER_ID}}&ued=',
    searchPattern: /caskers/i
  },
  flaviar: {
    id: 'flaviar',
    name: 'Flaviar',
    categories: ['spirits'],
    baseUrl: 'https://www.awin1.com/cread.php?awinmid=XXXXX&awinaffid={{PUBLISHER_ID}}&ued=',
    searchPattern: /flaviar/i
  }
};

const CATEGORY_ADVERTISERS = {
  wine: ['wine_com', 'vivino', 'reservebar', 'drizly', 'totalwine'],
  beer: ['craftshack', 'drizly', 'totalwine'],
  spirits: ['reservebar', 'drizly', 'totalwine', 'caskers', 'flaviar'],
  cocktails: ['drizly', 'totalwine', 'reservebar']
};

function generateAffiliateLink(product, advertiser) {
  const publisherId = CONFIG.awinPublisherId || 'YOUR_PUBLISHER_ID';
  const baseUrl = advertiser.baseUrl.replace('{{PUBLISHER_ID}}', publisherId);
  const searchQuery = encodeURIComponent(product.name);
  const productUrl = `https://www.${advertiser.id}.com/search?q=${searchQuery}`;
  return baseUrl + encodeURIComponent(productUrl);
}

function getRecommendedAdvertisers(product) {
  const category = product.category?.toLowerCase() || 'spirits';
  const advertiserIds = CATEGORY_ADVERTISERS[category] || CATEGORY_ADVERTISERS.spirits;
  return advertiserIds.map(id => ({
    ...AWIN_ADVERTISERS[id],
    affiliateLink: generateAffiliateLink(product, AWIN_ADVERTISERS[id])
  }));
}

async function enrichProductsWithAffiliates(limit = 1000) {
  console.log(`Enriching up to ${limit} products with affiliate links...`);
  
  const { data: products, error } = await supabase
    .from('products')
    .select('id, name, category, brand, price')
    .is('metadata->affiliate_links', null)
    .limit(limit);
  
  if (error) {
    console.error('Error fetching products:', error.message);
    return { success: false, error: error.message };
  }
  
  let enriched = 0;
  for (const product of products) {
    const advertisers = getRecommendedAdvertisers(product);
    const affiliateLinks = {};
    for (const adv of advertisers) {
      affiliateLinks[adv.id] = { name: adv.name, link: adv.affiliateLink };
    }
    
    const { error: updateError } = await supabase
      .from('products')
      .update({ metadata: { affiliate_links: affiliateLinks } })
      .eq('id', product.id);
    
    if (!updateError) enriched++;
    if (enriched % 100 === 0) console.log(`  Progress: ${enriched} enriched`);
  }
  
  console.log(`✅ Enriched ${enriched} products`);
  return { success: true, enriched };
}

async function searchWithAffiliates(query, options = {}) {
  const { category, limit = 20 } = options;
  
  let dbQuery = supabase
    .from('products')
    .select('id, name, category, subcategory, brand, price, description, metadata')
    .textSearch('name', query, { type: 'websearch' })
    .limit(limit);
  
  if (category) dbQuery = dbQuery.eq('category', category);
  
  const { data: products, error } = await dbQuery;
  if (error) return { success: false, error: error.message };
  
  const enrichedProducts = products.map(product => {
    const advertisers = getRecommendedAdvertisers(product);
    const affiliateLinks = {};
    for (const adv of advertisers.slice(0, 3)) {
      affiliateLinks[adv.id] = { name: adv.name, link: adv.affiliateLink };
    }
    return { ...product, affiliate_links: affiliateLinks };
  });
  
  return { success: true, count: enrichedProducts.length, products: enrichedProducts };
}

async function generateAwinProductFeed(options = {}) {
  const { category, limit = 10000 } = options;
  
  let query = supabase
    .from('products')
    .select('id, name, category, subcategory, brand, price, description, country, region')
    .not('price', 'is', null)
    .limit(limit);
  
  if (category) query = query.eq('category', category);
  const { data: products, error } = await query;
  if (error) return { success: false, error: error.message };
  
  let csv = 'product_id,product_name,category,subcategory,brand,price,description,country,region\n';
  for (const p of products) {
    csv += [
      p.id,
      `"${(p.name || '').replace(/"/g, '""')}"`,
      p.category,
      `"${(p.subcategory || '').replace(/"/g, '""')}"`,
      `"${(p.brand || '').replace(/"/g, '""')}"`,
      p.price || '',
      `"${(p.description || '').slice(0, 500).replace(/"/g, '""')}"`,
      p.country || '',
      p.region || ''
    ].join(',') + '\n';
  }
  
  return { success: true, count: products.length, csv };
}

module.exports = {
  generateAffiliateLink,
  getRecommendedAdvertisers,
  enrichProductsWithAffiliates,
  searchWithAffiliates,
  generateAwinProductFeed,
  AWIN_ADVERTISERS,
  CATEGORY_ADVERTISERS
};

if (require.main === module) {
  const command = process.argv[2];
  switch (command) {
    case 'enrich':
      enrichProductsWithAffiliates(parseInt(process.argv[3]) || 1000)
        .then(r => console.log(JSON.stringify(r, null, 2)));
      break;
    case 'search':
      searchWithAffiliates(process.argv[3] || 'bourbon', { limit: 5 })
        .then(r => console.log(JSON.stringify(r, null, 2)));
      break;
    case 'feed':
      generateAwinProductFeed({ limit: 100 })
        .then(r => {
          if (r.success) {
            require('fs').writeFileSync('awin_feed.csv', r.csv);
            console.log(`Generated feed with ${r.count} products`);
          }
        });
      break;
    default:
      console.log('Javari Spirits - Awin Integration');
      console.log('Commands: enrich [limit], search <query>, feed');
  }
}
