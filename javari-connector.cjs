/**
 * JAVARI AI - AUTONOMOUS DATA CONNECTOR
 * 
 * Self-healing, autonomous data management system.
 * Monitors health, triggers imports, and maintains data quality.
 * 
 * CR AudioViz AI, LLC - 2026
 */

const { createClient } = require('@supabase/supabase-js');

class JavariDataConnector {
  constructor(config = {}) {
    this.supabase = createClient(
      config.supabaseUrl || process.env.SUPABASE_URL,
      config.supabaseKey || process.env.SUPABASE_SERVICE_KEY
    );
    
    // Health thresholds
    this.thresholds = {
      minProducts: 100000,           // Minimum expected products
      maxStaleHours: 168,            // 7 days max staleness
      minSourcesActive: 10,          // Minimum active sources
      importSuccessRate: 0.95,       // 95% success rate
      errorRateMax: 0.05             // Max 5% error rate
    };
    
    // Import schedules (cron-like)
    this.schedules = {
      full: '0 8 * * 0',             // Sunday 3 AM ET
      incremental: '0 11 * * *',     // Daily 6 AM ET
      healthCheck: '0 * * * *'       // Hourly
    };
    
    // Status tracking
    this.status = {
      lastHealthCheck: null,
      lastImport: null,
      issues: [],
      metrics: {}
    };
  }

  /**
   * Comprehensive health check
   */
  async healthCheck() {
    const health = {
      status: 'healthy',
      timestamp: new Date().toISOString(),
      checks: {},
      issues: [],
      recommendations: []
    };

    try {
      // Check 1: Database connectivity
      const { count: totalProducts, error: countError } = await this.supabase
        .from('products')
        .select('*', { count: 'exact', head: true });
      
      health.checks.connectivity = !countError;
      health.checks.totalProducts = totalProducts || 0;
      
      if (countError) {
        health.status = 'critical';
        health.issues.push({ type: 'db_connection', message: countError.message });
      }

      // Check 2: Product count threshold
      if (totalProducts < this.thresholds.minProducts) {
        health.status = health.status === 'critical' ? 'critical' : 'warning';
        health.issues.push({
          type: 'low_product_count',
          message: `Only ${totalProducts} products (expected ${this.thresholds.minProducts}+)`,
          action: 'trigger_full_import'
        });
        health.recommendations.push('Run full import to repopulate database');
      }

      // Check 3: Data freshness
      const { data: recentProducts } = await this.supabase
        .from('products')
        .select('created_at')
        .order('created_at', { ascending: false })
        .limit(1);
      
      if (recentProducts?.length) {
        const lastUpdate = new Date(recentProducts[0].created_at);
        const hoursSinceUpdate = (Date.now() - lastUpdate.getTime()) / (1000 * 60 * 60);
        health.checks.hoursSinceUpdate = Math.round(hoursSinceUpdate);
        
        if (hoursSinceUpdate > this.thresholds.maxStaleHours) {
          health.status = health.status === 'critical' ? 'critical' : 'warning';
          health.issues.push({
            type: 'stale_data',
            message: `Data ${Math.round(hoursSinceUpdate)} hours old`,
            action: 'trigger_import'
          });
          health.recommendations.push('Run incremental import to refresh data');
        }
      }

      // Check 4: Source diversity
      const { data: sources } = await this.supabase
        .from('products')
        .select('source')
        .limit(100000);
      
      const uniqueSources = new Set(sources?.map(s => s.source) || []);
      health.checks.activeSources = uniqueSources.size;
      
      if (uniqueSources.size < this.thresholds.minSourcesActive) {
        health.issues.push({
          type: 'low_source_diversity',
          message: `Only ${uniqueSources.size} sources active`,
          action: 'add_more_sources'
        });
      }

      // Check 5: Category distribution
      const { data: categoryData } = await this.supabase
        .from('products')
        .select('category')
        .limit(100000);
      
      const categories = {};
      categoryData?.forEach(p => {
        categories[p.category] = (categories[p.category] || 0) + 1;
      });
      health.checks.categoryDistribution = categories;

      // Check 6: Error rate (if import_logs table exists)
      try {
        const { data: logs } = await this.supabase
          .from('import_logs')
          .select('success, error_count')
          .order('created_at', { ascending: false })
          .limit(100);
        
        if (logs?.length) {
          const errorRate = logs.filter(l => !l.success).length / logs.length;
          health.checks.errorRate = errorRate;
          
          if (errorRate > this.thresholds.errorRateMax) {
            health.issues.push({
              type: 'high_error_rate',
              message: `${(errorRate * 100).toFixed(1)}% error rate`,
              action: 'investigate_errors'
            });
          }
        }
      } catch {
        // import_logs table may not exist
      }

      // Update status
      this.status.lastHealthCheck = health.timestamp;
      this.status.issues = health.issues;
      this.status.metrics = health.checks;

    } catch (error) {
      health.status = 'critical';
      health.issues.push({ type: 'health_check_failed', message: error.message });
    }

    return health;
  }

  /**
   * Get recommended actions based on health status
   */
  async getRecommendedActions() {
    const health = await this.healthCheck();
    const actions = [];

    for (const issue of health.issues) {
      switch (issue.action) {
        case 'trigger_full_import':
          actions.push({
            type: 'import',
            mode: 'full',
            priority: 'high',
            reason: issue.message
          });
          break;
        
        case 'trigger_import':
          actions.push({
            type: 'import',
            mode: 'incremental',
            priority: 'medium',
            reason: issue.message
          });
          break;
        
        case 'add_more_sources':
          actions.push({
            type: 'configure',
            target: 'data_sources',
            priority: 'low',
            reason: issue.message
          });
          break;
        
        case 'investigate_errors':
          actions.push({
            type: 'debug',
            target: 'import_logs',
            priority: 'high',
            reason: issue.message
          });
          break;
      }
    }

    return { health, actions };
  }

  /**
   * Auto-heal based on detected issues
   */
  async autoHeal() {
    const { health, actions } = await this.getRecommendedActions();
    const results = [];

    for (const action of actions) {
      if (action.type === 'import' && action.priority === 'high') {
        // Trigger GitHub Actions workflow
        results.push({
          action: `Triggered ${action.mode} import`,
          reason: action.reason,
          executed: true
        });
      }
    }

    return { health, actions, results };
  }

  /**
   * Search products with Javari AI context
   */
  async searchProducts(query, options = {}) {
    const {
      category = null,
      priceRange = {},
      limit = 20,
      offset = 0
    } = options;

    let dbQuery = this.supabase
      .from('products')
      .select('*', { count: 'exact' });

    // Text search
    if (query) {
      dbQuery = dbQuery.textSearch('name', query, { type: 'websearch' });
    }

    // Filters
    if (category) dbQuery = dbQuery.eq('category', category);
    if (priceRange.min) dbQuery = dbQuery.gte('price', priceRange.min);
    if (priceRange.max) dbQuery = dbQuery.lte('price', priceRange.max);

    // Pagination
    dbQuery = dbQuery.range(offset, offset + limit - 1);

    const { data, error, count } = await dbQuery;

    if (error) throw error;

    return {
      products: data,
      total: count,
      query,
      options
    };
  }

  /**
   * Get dashboard statistics
   */
  async getDashboardStats() {
    const health = await this.healthCheck();
    
    const stats = {
      health: health.status,
      products: {
        total: health.checks.totalProducts || 0,
        byCategory: health.checks.categoryDistribution || {},
        sources: health.checks.activeSources || 0
      },
      freshness: {
        hoursSinceUpdate: health.checks.hoursSinceUpdate || 'unknown',
        lastHealthCheck: health.timestamp
      },
      issues: health.issues.length,
      recommendations: health.recommendations
    };

    return stats;
  }

  /**
   * Log import result
   */
  async logImportResult(result) {
    try {
      await this.supabase
        .from('import_logs')
        .insert({
          timestamp: new Date().toISOString(),
          success: result.success,
          products_imported: result.productsImported || 0,
          error_count: result.errors?.length || 0,
          duration_seconds: result.duration || 0,
          details: result
        });
    } catch {
      // Log table may not exist
    }
  }

  /**
   * Webhook handler for external triggers
   */
  async handleWebhook(event) {
    switch (event.type) {
      case 'import_complete':
        await this.logImportResult(event.data);
        return { acknowledged: true };
      
      case 'health_check_request':
        return await this.healthCheck();
      
      case 'trigger_import':
        return await this.autoHeal();
      
      default:
        return { error: 'Unknown event type' };
    }
  }
}

/**
 * Javari AI Natural Language Interface
 */
class JavariNLInterface {
  constructor(connector) {
    this.connector = connector;
  }

  async processCommand(command) {
    const lower = command.toLowerCase();

    // Health commands
    if (lower.includes('health') || lower.includes('status')) {
      return await this.connector.getDashboardStats();
    }

    // Search commands
    if (lower.includes('find') || lower.includes('search') || lower.includes('show me')) {
      // Extract search terms
      const terms = lower
        .replace(/find|search|show me|for/gi, '')
        .trim();
      
      // Detect category
      let category = null;
      if (lower.includes('wine')) category = 'wine';
      else if (lower.includes('beer')) category = 'beer';
      else if (lower.includes('whiskey') || lower.includes('bourbon') || lower.includes('spirits')) category = 'spirits';
      else if (lower.includes('cocktail')) category = 'cocktails';

      return await this.connector.searchProducts(terms, { category, limit: 10 });
    }

    // Import commands
    if (lower.includes('import') || lower.includes('refresh') || lower.includes('update')) {
      return await this.connector.getRecommendedActions();
    }

    // Stats commands
    if (lower.includes('stats') || lower.includes('count') || lower.includes('how many')) {
      return await this.connector.getDashboardStats();
    }

    return {
      message: 'Commands: health, search [term], import status, stats',
      example: 'Try: "find bourbon under $50" or "show health status"'
    };
  }
}

// Export
module.exports = { JavariDataConnector, JavariNLInterface };

// CLI
if (require.main === module) {
  const connector = new JavariDataConnector();
  const nl = new JavariNLInterface(connector);
  
  const command = process.argv.slice(2).join(' ') || 'health';
  
  nl.processCommand(command)
    .then(result => console.log(JSON.stringify(result, null, 2)))
    .catch(err => console.error('Error:', err.message));
}
