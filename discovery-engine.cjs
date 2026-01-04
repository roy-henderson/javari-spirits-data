/**
 * JAVARI SPIRITS AUTO-DISCOVERY ENGINE v1.0
 * Automatically finds NEW alcohol datasets across the internet
 * Runs daily, alerts when new sources found
 * Created: 2026-01-04
 */

const https = require('https');
const http = require('http');
const { URL } = require('url');

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
        'User-Agent': 'JavariSpirits-Discovery/1.0',
        ...options.headers
      },
      timeout: 30000
    };
    
    const req = protocol.request(opts, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        const newUrl = res.headers.location.startsWith('http') 
          ? res.headers.location 
          : `${parsed.protocol}//${parsed.hostname}${res.headers.location}`;
        return fetch(newUrl, options).then(resolve).catch(reject);
      }
      
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, data, headers: res.headers }));
    });
    
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
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
// DISCOVERY TARGETS
// ============================================================================

const DISCOVERY_SOURCES = [
  // Data.gov - US Federal & State
  {
    name: 'Data.gov Liquor',
    url: 'https://catalog.data.gov/api/3/action/package_search?q=liquor&rows=100',
    parser: parseDataGov
  },
  {
    name: 'Data.gov Alcohol',
    url: 'https://catalog.data.gov/api/3/action/package_search?q=alcohol+beverages&rows=100',
    parser: parseDataGov
  },
  {
    name: 'Data.gov Spirits',
    url: 'https://catalog.data.gov/api/3/action/package_search?q=spirits+wine+beer&rows=100',
    parser: parseDataGov
  },
  {
    name: 'Data.gov Wine',
    url: 'https://catalog.data.gov/api/3/action/package_search?q=wine+sales&rows=100',
    parser: parseDataGov
  },
  
  // GitHub - Public Datasets
  {
    name: 'GitHub Alcohol Datasets',
    url: 'https://api.github.com/search/repositories?q=alcohol+dataset+csv&sort=updated&per_page=50',
    parser: parseGitHub
  },
  {
    name: 'GitHub Whiskey Data',
    url: 'https://api.github.com/search/repositories?q=whiskey+whisky+data+csv&sort=updated&per_page=50',
    parser: parseGitHub
  },
  {
    name: 'GitHub Wine Data',
    url: 'https://api.github.com/search/repositories?q=wine+dataset+csv&sort=updated&per_page=50',
    parser: parseGitHub
  },
  {
    name: 'GitHub Beer Data',
    url: 'https://api.github.com/search/repositories?q=beer+brewery+dataset&sort=updated&per_page=50',
    parser: parseGitHub
  },
  {
    name: 'GitHub Cocktail Data',
    url: 'https://api.github.com/search/repositories?q=cocktail+recipes+dataset&sort=updated&per_page=50',
    parser: parseGitHub
  },
  
  // HuggingFace Datasets
  {
    name: 'HuggingFace Alcohol',
    url: 'https://huggingface.co/api/datasets?search=alcohol&limit=50',
    parser: parseHuggingFace
  },
  {
    name: 'HuggingFace Wine',
    url: 'https://huggingface.co/api/datasets?search=wine&limit=50',
    parser: parseHuggingFace
  },
  {
    name: 'HuggingFace Beer',
    url: 'https://huggingface.co/api/datasets?search=beer&limit=50',
    parser: parseHuggingFace
  },
  
  // Kaggle (via web search - can't use API without key)
  // We'll add manual Kaggle checks
  
  // Zenodo (Academic)
  {
    name: 'Zenodo Alcohol',
    url: 'https://zenodo.org/api/records?q=alcohol+dataset&size=50',
    parser: parseZenodo
  },
  {
    name: 'Zenodo Wine',
    url: 'https://zenodo.org/api/records?q=wine+data&size=50',
    parser: parseZenodo
  }
];

// ============================================================================
// PARSERS
// ============================================================================

function parseDataGov(data) {
  try {
    const json = JSON.parse(data);
    if (!json.result || !json.result.results) return [];
    
    return json.result.results.map(pkg => ({
      title: pkg.title,
      url: `https://catalog.data.gov/dataset/${pkg.name}`,
      description: pkg.notes?.substring(0, 500),
      format: pkg.resources?.map(r => r.format).filter(Boolean).join(', '),
      organization: pkg.organization?.title,
      modified: pkg.metadata_modified,
      resources: pkg.resources?.filter(r => 
        ['CSV', 'JSON', 'XLS', 'XLSX'].includes((r.format || '').toUpperCase())
      ).map(r => ({
        url: r.url,
        format: r.format,
        name: r.name
      }))
    })).filter(d => d.resources && d.resources.length > 0);
  } catch (e) {
    return [];
  }
}

function parseGitHub(data) {
  try {
    const json = JSON.parse(data);
    if (!json.items) return [];
    
    return json.items.map(repo => ({
      title: repo.full_name,
      url: repo.html_url,
      description: repo.description?.substring(0, 500),
      format: 'github',
      organization: repo.owner?.login,
      modified: repo.updated_at,
      stars: repo.stargazers_count,
      resources: [{
        url: repo.html_url,
        format: 'github',
        name: repo.name
      }]
    })).filter(d => d.stars > 2); // Filter low quality
  } catch (e) {
    return [];
  }
}

function parseHuggingFace(data) {
  try {
    const json = JSON.parse(data);
    if (!Array.isArray(json)) return [];
    
    return json.map(ds => ({
      title: ds.id,
      url: `https://huggingface.co/datasets/${ds.id}`,
      description: ds.description?.substring(0, 500),
      format: 'huggingface',
      organization: ds.author,
      modified: ds.lastModified,
      downloads: ds.downloads,
      resources: [{
        url: `https://huggingface.co/datasets/${ds.id}`,
        format: 'parquet',
        name: ds.id
      }]
    })).filter(d => d.downloads > 10);
  } catch (e) {
    return [];
  }
}

function parseZenodo(data) {
  try {
    const json = JSON.parse(data);
    if (!json.hits || !json.hits.hits) return [];
    
    return json.hits.hits.map(record => ({
      title: record.metadata?.title,
      url: record.links?.html || `https://zenodo.org/record/${record.id}`,
      description: record.metadata?.description?.substring(0, 500),
      format: 'zenodo',
      organization: record.metadata?.creators?.[0]?.name,
      modified: record.updated,
      resources: record.files?.filter(f => 
        /\.(csv|json|xlsx?|parquet)$/i.test(f.key)
      ).map(f => ({
        url: f.links?.self,
        format: f.key.split('.').pop(),
        name: f.key
      }))
    })).filter(d => d.resources && d.resources.length > 0);
  } catch (e) {
    return [];
  }
}

// ============================================================================
// DISCOVERY ENGINE
// ============================================================================

async function getExistingUrls() {
  try {
    const result = await supabaseQuery('dataset_sources', 'GET', null, '?select=source_url');
    const data = JSON.parse(result.data);
    return new Set(data.map(d => d.source_url));
  } catch (e) {
    return new Set();
  }
}

async function getDiscoveryQueueUrls() {
  try {
    const result = await supabaseQuery('discovery_queue', 'GET', null, '?select=discovered_url');
    const data = JSON.parse(result.data);
    return new Set(data.map(d => d.discovered_url));
  } catch (e) {
    return new Set();
  }
}

function detectAlcoholTypes(text) {
  const types = [];
  const lower = (text || '').toLowerCase();
  
  if (/whiskey|whisky|bourbon|scotch|rye/i.test(lower)) types.push('whiskey');
  if (/vodka/i.test(lower)) types.push('vodka');
  if (/gin/i.test(lower)) types.push('gin');
  if (/rum/i.test(lower)) types.push('rum');
  if (/tequila|mezcal/i.test(lower)) types.push('tequila');
  if (/wine|vino|vineyard/i.test(lower)) types.push('wine');
  if (/beer|brew|lager|ale|ipa/i.test(lower)) types.push('beer');
  if (/cocktail|mixed drink/i.test(lower)) types.push('cocktail');
  if (/spirit|liquor|alcohol/i.test(lower)) types.push('spirits');
  
  return types.length > 0 ? types : ['spirits'];
}

function scorePriority(item) {
  let score = 5;
  
  // Boost for recent updates
  if (item.modified) {
    const age = Date.now() - new Date(item.modified).getTime();
    const days = age / (1000 * 60 * 60 * 24);
    if (days < 30) score -= 2;
    else if (days < 90) score -= 1;
  }
  
  // Boost for popularity
  if (item.stars > 100) score -= 2;
  else if (item.stars > 20) score -= 1;
  if (item.downloads > 1000) score -= 2;
  else if (item.downloads > 100) score -= 1;
  
  // Boost for good formats
  if (/csv|json/i.test(item.format || '')) score -= 1;
  
  // Boost for government data
  if (/\.gov/i.test(item.url)) score -= 2;
  
  return Math.max(1, Math.min(10, score));
}

async function runDiscovery() {
  log('');
  log('╔════════════════════════════════════════════════════════════════╗');
  log('║       JAVARI SPIRITS AUTO-DISCOVERY ENGINE v1.0                ║');
  log('║       Finding NEW alcohol datasets automatically               ║');
  log('╚════════════════════════════════════════════════════════════════╝');
  log('');
  
  // Get existing URLs to avoid duplicates
  const existingUrls = await getExistingUrls();
  const queuedUrls = await getDiscoveryQueueUrls();
  const allKnownUrls = new Set([...existingUrls, ...queuedUrls]);
  
  log(`Known sources: ${existingUrls.size}`);
  log(`Already queued: ${queuedUrls.size}`);
  log('');
  
  let totalDiscovered = 0;
  let newSources = [];
  
  for (const source of DISCOVERY_SOURCES) {
    log(`🔍 Scanning ${source.name}...`);
    
    try {
      const response = await fetch(source.url);
      
      if (response.status !== 200) {
        log(`   ⚠️ HTTP ${response.status}`);
        continue;
      }
      
      const items = source.parser(response.data);
      log(`   Found ${items.length} potential datasets`);
      
      // Filter to new only
      const newItems = items.filter(item => {
        const url = item.url || item.resources?.[0]?.url;
        return url && !allKnownUrls.has(url);
      });
      
      if (newItems.length > 0) {
        log(`   ✅ ${newItems.length} NEW datasets!`);
        
        for (const item of newItems) {
          const url = item.url || item.resources?.[0]?.url;
          allKnownUrls.add(url);
          
          newSources.push({
            discovered_url: url,
            discovered_title: item.title?.substring(0, 255),
            discovered_from: source.name,
            potential_type: 'catalog',
            alcohol_types_detected: detectAlcoholTypes(item.title + ' ' + item.description),
            format_detected: item.format,
            priority_score: scorePriority(item),
            status: 'pending',
            notes: item.description?.substring(0, 500)
          });
        }
      }
      
      totalDiscovered += newItems.length;
      
      // Rate limit
      await new Promise(r => setTimeout(r, 500));
      
    } catch (e) {
      log(`   ❌ Error: ${e.message}`);
    }
  }
  
  log('');
  log('════════════════════════════════════════════════════════════════');
  log(`DISCOVERY COMPLETE: ${totalDiscovered} NEW sources found`);
  log('════════════════════════════════════════════════════════════════');
  
  // Insert new sources to queue
  if (newSources.length > 0) {
    log('');
    log('📥 Adding to discovery queue...');
    
    const batchSize = 50;
    for (let i = 0; i < newSources.length; i += batchSize) {
      const batch = newSources.slice(i, i + batchSize);
      const result = await supabaseQuery('discovery_queue', 'POST', batch);
      
      if (result.success) {
        log(`   ✅ Added ${batch.length} sources to queue`);
      } else {
        log(`   ⚠️ Some inserts failed`);
      }
    }
    
    // Create alert for new discoveries
    await supabaseQuery('harvester_alerts', 'POST', [{
      alert_type: 'new_source',
      severity: totalDiscovered > 10 ? 'high' : 'medium',
      title: `${totalDiscovered} new alcohol datasets discovered`,
      message: `Auto-discovery found ${totalDiscovered} new potential data sources. Review the discovery_queue table to approve and ingest.`,
      metadata: JSON.stringify({
        sources: newSources.slice(0, 10).map(s => s.discovered_title),
        total: totalDiscovered
      })
    }]);
    
    log('');
    log('🔔 Alert created for new discoveries');
  }
  
  // Summary of top finds
  if (newSources.length > 0) {
    log('');
    log('TOP NEW DISCOVERIES:');
    log('────────────────────────────────────────────────────────────────');
    newSources
      .sort((a, b) => a.priority_score - b.priority_score)
      .slice(0, 10)
      .forEach((s, i) => {
        log(`${i + 1}. [P${s.priority_score}] ${s.discovered_title}`);
        log(`   ${s.discovered_url}`);
      });
  }
  
  log('');
  log('✅ Discovery engine complete');
  
  return { discovered: totalDiscovered, sources: newSources };
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  if (!SUPABASE_URL || !SUPABASE_KEY) {
    log('❌ Missing SUPABASE_URL or SUPABASE_SERVICE_KEY');
    process.exit(1);
  }
  
  await runDiscovery();
}

main().catch(console.error);
