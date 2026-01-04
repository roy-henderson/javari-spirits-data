/**
 * JAVARI AI - SPIRITS DATA CONNECTOR
 * Autonomous data management for Javari AI
 * CR AudioViz AI, LLC - 2026
 */

const { createClient } = require('@supabase/supabase-js');

const CONFIG = {
  supabaseUrl: process.env.SUPABASE_URL || 'https://ggmbwrtjwjvwwmljypqv.supabase.co',
  supabaseKey: process.env.SUPABASE_SERVICE_KEY,
  githubToken: process.env.GITHUB_TOKEN,
  dataRepo: 'roy-henderson/javari-spirits-data'
};

const supabase = createClient(CONFIG.supabaseUrl, CONFIG.supabaseKey);

async function systemHealthCheck() {
  const health = { timestamp: new Date().toISOString(), status: 'healthy', components: {}, issues: [], recommendations: [] };
  try {
    const { count, error } = await supabase.from('products').select('*', { count: 'exact', head: true });
    health.components.database = { status: error ? 'error' : 'healthy', productCount: count || 0 };
    if (count < 100000) {
      health.issues.push({ type: 'low_product_count', severity: 'warning', message: `Only ${count} products` });
      health.recommendations.push({ action: 'trigger_full_import', priority: 'high' });
    }
  } catch (e) {
    health.components.database = { status: 'error', error: e.message };
    health.status = 'critical';
  }
  return health;
}

async function searchProducts(query, options = {}) {
  const { category, limit = 20 } = options;
  let dbQuery = supabase.from('products').select('*', { count: 'exact' });
  if (query) dbQuery = dbQuery.textSearch('name', query, { type: 'websearch' });
  if (category) dbQuery = dbQuery.eq('category', category);
  dbQuery = dbQuery.limit(limit);
  const { data, error, count } = await dbQuery;
  return error ? { success: false, error: error.message } : { success: true, products: data, total: count };
}

async function getDashboardStats() {
  const { data } = await supabase.from('products').select('category, source').limit(100000);
  const categoryCount = {}, sourceCount = {};
  data?.forEach(p => {
    categoryCount[p.category] = (categoryCount[p.category] || 0) + 1;
    sourceCount[p.source] = (sourceCount[p.source] || 0) + 1;
  });
  return { total: data?.length || 0, byCategory: categoryCount, bySource: sourceCount };
}

async function triggerImportWorkflow(type = 'full') {
  if (!CONFIG.githubToken) return { success: false, error: 'No GITHUB_TOKEN' };
  const res = await fetch(`https://api.github.com/repos/${CONFIG.dataRepo}/actions/workflows/import.yml/dispatches`, {
    method: 'POST',
    headers: { 'Authorization': `token ${CONFIG.githubToken}`, 'Accept': 'application/vnd.github.v3+json' },
    body: JSON.stringify({ ref: 'main', inputs: { import_type: type } })
  });
  return res.ok || res.status === 204 ? { success: true } : { success: false, error: res.status };
}

module.exports = { systemHealthCheck, searchProducts, getDashboardStats, triggerImportWorkflow };

if (require.main === module) {
  const cmd = process.argv[2];
  const run = { health: systemHealthCheck, stats: getDashboardStats, search: () => searchProducts(process.argv[3] || 'bourbon'), trigger: triggerImportWorkflow };
  if (run[cmd]) run[cmd]().then(r => console.log(JSON.stringify(r, null, 2))).catch(console.error);
  else console.log('Commands: health, stats, search <query>, trigger');
}
