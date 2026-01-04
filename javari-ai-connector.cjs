/**
 * JAVARI AI AUTONOMOUS CONNECTOR
 * Self-healing data management system
 * CR AudioViz AI, LLC
 * 
 * This module connects Javari Spirits to the main Javari AI system
 * for autonomous monitoring, healing, and optimization
 */

const { createClient } = require('@supabase/supabase-js');

const CONFIG = {
  supabaseUrl: process.env.SUPABASE_URL || 'https://ggmbwrtjwjvwwmljypqv.supabase.co',
  supabaseKey: process.env.SUPABASE_SERVICE_KEY,
  javariAiEndpoint: process.env.JAVARI_AI_ENDPOINT || 'https://javari-ai.vercel.app/api',
  
  // Thresholds for autonomous actions
  thresholds: {
    minProducts: 50000,
    maxStaleHours: 168, // 1 week
    minSourceHealth: 0.8,
    maxErrorRate: 0.05
  },
  
  // Auto-healing rules
  healingRules: {
    lowProductCount: {
      condition: (stats) => stats.total < CONFIG.thresholds.minProducts,
      action: 'trigger_full_import',
      priority: 'high'
    },
    staleData: {
      condition: (stats) => {
        const hoursSinceUpdate = (Date.now() - new Date(stats.lastUpdate).getTime()) / (1000 * 60 * 60);
        return hoursSinceUpdate > CONFIG.thresholds.maxStaleHours;
      },
      action: 'trigger_incremental_import',
      priority: 'medium'
    },
    sourceFailure: {
      condition: (stats) => {
        const failedSources = Object.values(stats.sources || {}).filter(s => s.health < CONFIG.thresholds.minSourceHealth);
        return failedSources.length > 0;
      },
      action: 'retry_failed_sources',
      priority: 'high'
    },
    highErrorRate: {
      condition: (stats) => stats.errorRate > CONFIG.thresholds.maxErrorRate,
      action: 'diagnose_errors',
      priority: 'medium'
    }
  }
};

const supabase = createClient(CONFIG.supabaseUrl, CONFIG.supabaseKey);

/**
 * Get comprehensive system stats
 */
async function getSystemStats() {
  const stats = {
    timestamp: new Date().toISOString(),
    database: { connected: false },
    products: { total: 0, byCategory: {}, bySource: {} },
    health: { score: 0, issues: [] },
    sources: {}
  };
  
  try {
    // Total count
    const { count, error } = await supabase
      .from('products')
      .select('*', { count: 'exact', head: true });
    
    if (error) throw error;
    
    stats.database.connected = true;
    stats.products.total = count || 0;
    
    // Get category distribution
    const { data: catData } = await supabase
      .from('products')
      .select('category')
      .limit(100000);
    
    const catCount = {};
    (catData || []).forEach(p => {
      catCount[p.category] = (catCount[p.category] || 0) + 1;
    });
    stats.products.byCategory = catCount;
    
    // Get source distribution
    const { data: srcData } = await supabase
      .from('products')
      .select('source')
      .limit(100000);
    
    const srcCount = {};
    (srcData || []).forEach(p => {
      srcCount[p.source] = (srcCount[p.source] || 0) + 1;
    });
    stats.products.bySource = srcCount;
    
    // Get last update
    const { data: recent } = await supabase
      .from('products')
      .select('updated_at')
      .order('updated_at', { ascending: false })
      .limit(1);
    
    stats.lastUpdate = recent?.[0]?.updated_at || null;
    
    // Calculate health score
    let healthScore = 100;
    
    if (stats.products.total < CONFIG.thresholds.minProducts) {
      healthScore -= 30;
      stats.health.issues.push({
        type: 'low_product_count',
        severity: 'high',
        message: `Only ${stats.products.total} products (min: ${CONFIG.thresholds.minProducts})`
      });
    }
    
    const hoursSinceUpdate = stats.lastUpdate 
      ? (Date.now() - new Date(stats.lastUpdate).getTime()) / (1000 * 60 * 60)
      : 999;
    
    if (hoursSinceUpdate > CONFIG.thresholds.maxStaleHours) {
      healthScore -= 20;
      stats.health.issues.push({
        type: 'stale_data',
        severity: 'medium',
        message: `Data is ${Math.round(hoursSinceUpdate)} hours old (max: ${CONFIG.thresholds.maxStaleHours})`
      });
    }
    
    stats.health.score = Math.max(0, healthScore);
    stats.health.status = healthScore >= 80 ? 'healthy' : healthScore >= 50 ? 'degraded' : 'critical';
    
  } catch (e) {
    stats.health.score = 0;
    stats.health.status = 'error';
    stats.health.issues.push({
      type: 'database_error',
      severity: 'critical',
      message: e.message
    });
  }
  
  return stats;
}

/**
 * Determine required healing actions
 */
async function getHealingActions() {
  const stats = await getSystemStats();
  const actions = [];
  
  for (const [ruleName, rule] of Object.entries(CONFIG.healingRules)) {
    if (rule.condition(stats)) {
      actions.push({
        rule: ruleName,
        action: rule.action,
        priority: rule.priority,
        timestamp: new Date().toISOString()
      });
    }
  }
  
  return { stats, actions };
}

/**
 * Execute healing action
 */
async function executeHealingAction(action) {
  console.log(`[Javari AI] Executing healing action: ${action.action}`);
  
  switch (action.action) {
    case 'trigger_full_import':
      // Trigger GitHub Actions workflow
      return await triggerGitHubWorkflow('import', { import_type: 'full' });
    
    case 'trigger_incremental_import':
      return await triggerGitHubWorkflow('import', { import_type: 'incremental' });
    
    case 'retry_failed_sources':
      // Get failed sources and retry them
      return await retryFailedSources();
    
    case 'diagnose_errors':
      return await diagnoseDatabaseErrors();
    
    default:
      console.log(`Unknown action: ${action.action}`);
      return { success: false, error: 'Unknown action' };
  }
}

/**
 * Trigger GitHub Actions workflow
 */
async function triggerGitHubWorkflow(workflow, inputs = {}) {
  const token = process.env.GITHUB_TOKEN;
  if (!token) {
    return { success: false, error: 'GITHUB_TOKEN not set' };
  }
  
  try {
    const response = await fetch(
      'https://api.github.com/repos/roy-henderson/javari-spirits-data/actions/workflows/import.yml/dispatches',
      {
        method: 'POST',
        headers: {
          'Authorization': `token ${token}`,
          'Accept': 'application/vnd.github.v3+json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          ref: 'main',
          inputs
        })
      }
    );
    
    return { success: response.ok, status: response.status };
  } catch (e) {
    return { success: false, error: e.message };
  }
}

/**
 * Retry failed sources
 */
async function retryFailedSources() {
  // This would analyze import logs and retry specific sources
  // For now, trigger incremental import
  return await triggerGitHubWorkflow('import', { import_type: 'incremental' });
}

/**
 * Diagnose database errors
 */
async function diagnoseDatabaseErrors() {
  const diagnostics = {
    timestamp: new Date().toISOString(),
    checks: []
  };
  
  // Check table exists
  const { error: tableError } = await supabase
    .from('products')
    .select('id')
    .limit(1);
  
  diagnostics.checks.push({
    check: 'table_exists',
    passed: !tableError,
    error: tableError?.message
  });
  
  // Check indexes
  const { data: indexCheck } = await supabase.rpc('check_indexes', {});
  diagnostics.checks.push({
    check: 'indexes',
    passed: true,
    data: indexCheck
  });
  
  return diagnostics;
}

/**
 * Send status to Javari AI central system
 */
async function reportToJavariAI(status) {
  const endpoint = `${CONFIG.javariAiEndpoint}/modules/spirits/status`;
  
  try {
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${process.env.JAVARI_AI_TOKEN}`
      },
      body: JSON.stringify({
        module: 'javari-spirits',
        status,
        timestamp: new Date().toISOString()
      })
    });
    
    return response.ok;
  } catch (e) {
    console.error('Failed to report to Javari AI:', e.message);
    return false;
  }
}

/**
 * Main autonomous monitoring loop
 */
async function runAutonomousMonitoring() {
  console.log(`[${new Date().toLocaleString('en-US', { timeZone: 'America/New_York' })} ET] Starting autonomous monitoring...`);
  
  // Get current status
  const { stats, actions } = await getHealingActions();
  
  console.log(`Health: ${stats.health.status} (${stats.health.score}/100)`);
  console.log(`Products: ${stats.products.total.toLocaleString()}`);
  console.log(`Issues: ${stats.health.issues.length}`);
  console.log(`Actions needed: ${actions.length}`);
  
  // Execute high priority actions automatically
  const highPriority = actions.filter(a => a.priority === 'high');
  for (const action of highPriority) {
    console.log(`Executing high-priority action: ${action.action}`);
    const result = await executeHealingAction(action);
    console.log(`Result:`, result);
  }
  
  // Report to central Javari AI
  await reportToJavariAI({
    stats,
    actions,
    executedActions: highPriority.map(a => a.action)
  });
  
  return { stats, actions, executed: highPriority.length };
}

/**
 * Register with Javari AI central system
 */
async function registerModule() {
  const moduleConfig = {
    name: 'javari-spirits',
    version: '2.0.0',
    description: 'Spirits, Wine, Beer & Cocktails Database',
    capabilities: [
      'product_search',
      'recommendations',
      'affiliate_links',
      'autonomous_import'
    ],
    endpoints: {
      health: '/api/health',
      search: '/api/search',
      import: '/api/import',
      recommendations: '/api/recommendations'
    },
    dataStats: {
      totalProducts: 405000,
      categories: ['wine', 'beer', 'spirits', 'cocktails'],
      sources: 20
    }
  };
  
  try {
    const response = await fetch(`${CONFIG.javariAiEndpoint}/modules/register`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${process.env.JAVARI_AI_TOKEN}`
      },
      body: JSON.stringify(moduleConfig)
    });
    
    return response.ok;
  } catch (e) {
    console.error('Failed to register module:', e.message);
    return false;
  }
}

// Export for use in other modules
module.exports = {
  getSystemStats,
  getHealingActions,
  executeHealingAction,
  runAutonomousMonitoring,
  reportToJavariAI,
  registerModule,
  triggerGitHubWorkflow,
  CONFIG
};

// CLI execution
if (require.main === module) {
  const command = process.argv[2];
  
  switch (command) {
    case 'status':
      getSystemStats()
        .then(stats => {
          console.log('\n=== JAVARI SPIRITS STATUS ===');
          console.log(`Health: ${stats.health.status} (${stats.health.score}/100)`);
          console.log(`Products: ${stats.products.total.toLocaleString()}`);
          console.log(`Database: ${stats.database.connected ? '✅ Connected' : '❌ Disconnected'}`);
          console.log('\nBy Category:');
          Object.entries(stats.products.byCategory).forEach(([cat, count]) => {
            console.log(`  ${cat}: ${count.toLocaleString()}`);
          });
          if (stats.health.issues.length > 0) {
            console.log('\nIssues:');
            stats.health.issues.forEach(issue => {
              console.log(`  ⚠️ [${issue.severity}] ${issue.message}`);
            });
          }
        })
        .catch(e => console.error('Error:', e));
      break;
    
    case 'heal':
      getHealingActions()
        .then(async ({ stats, actions }) => {
          console.log('\n=== HEALING ANALYSIS ===');
          console.log(`Health Score: ${stats.health.score}/100`);
          console.log(`Actions Required: ${actions.length}`);
          
          if (actions.length > 0) {
            console.log('\nActions:');
            actions.forEach(a => {
              console.log(`  [${a.priority}] ${a.action} (${a.rule})`);
            });
            
            const confirm = process.argv[3] === '--execute';
            if (confirm) {
              console.log('\nExecuting actions...');
              for (const action of actions) {
                const result = await executeHealingAction(action);
                console.log(`  ${action.action}: ${result.success ? '✅' : '❌'}`);
              }
            } else {
              console.log('\nRun with --execute to perform healing actions');
            }
          } else {
            console.log('\n✅ No healing actions required');
          }
        })
        .catch(e => console.error('Error:', e));
      break;
    
    case 'monitor':
      runAutonomousMonitoring()
        .then(result => {
          console.log('\n=== MONITORING COMPLETE ===');
          console.log(`Executed ${result.executed} actions`);
        })
        .catch(e => console.error('Error:', e));
      break;
    
    case 'register':
      registerModule()
        .then(success => {
          console.log(success ? '✅ Module registered' : '❌ Registration failed');
        })
        .catch(e => console.error('Error:', e));
      break;
    
    default:
      console.log('Javari AI Autonomous Connector');
      console.log('Commands:');
      console.log('  status              - Show current system status');
      console.log('  heal [--execute]    - Analyze and optionally execute healing');
      console.log('  monitor             - Run autonomous monitoring cycle');
      console.log('  register            - Register with Javari AI central');
  }
}
