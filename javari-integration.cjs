/**
 * Javari Spirits - Autonomous Data Management Module
 * 
 * This module enables Javari AI to autonomously manage product data:
 * - Schedule and trigger imports
 * - Monitor data health
 * - Handle affiliate link enrichment
 * - Self-heal data issues
 * 
 * Integration: Add to Javari's task queue system
 */

const { createClient } = require('@supabase/supabase-js');

class JavariSpiritsDataManager {
  constructor(config = {}) {
    this.supabase = createClient(
      config.supabaseUrl || process.env.SUPABASE_URL,
      config.supabaseKey || process.env.SUPABASE_SERVICE_KEY
    );
    this.config = {
      minProductCount: 5000,
      staleThresholdDays: 7,
      batchSize: 100,
      ...config
    };
  }

  /**
   * Health check - returns current data status
   */
  async healthCheck() {
    const checks = {
      timestamp: new Date().toISOString(),
      status: 'healthy',
      issues: []
    };

    try {
      // Total count
      const { count: totalCount, error: countError } = await this.supabase
        .from('products')
        .select('*', { count: 'exact', head: true });

      if (countError) throw countError;
      checks.totalProducts = totalCount;

      if (totalCount < this.config.minProductCount) {
        checks.issues.push({
          type: 'low_product_count',
          message: `Only ${totalCount} products (minimum: ${this.config.minProductCount})`,
          action: 'trigger_import'
        });
      }

      // Category breakdown
      const { data: categories } = await this.supabase
        .from('products')
        .select('category')
        .limit(10000);

      const categoryCount = {};
      categories?.forEach(p => {
        categoryCount[p.category] = (categoryCount[p.category] || 0) + 1;
      });
      checks.byCategory = categoryCount;

      // Source breakdown
      const { data: sources } = await this.supabase
        .from('products')
        .select('source')
        .limit(10000);

      const sourceCount = {};
      sources?.forEach(p => {
        sourceCount[p.source] = (sourceCount[p.source] || 0) + 1;
      });
      checks.bySource = sourceCount;

      // Check for stale data
      const staleDate = new Date();
      staleDate.setDate(staleDate.getDate() - this.config.staleThresholdDays);
      
      const { count: recentCount } = await this.supabase
        .from('products')
        .select('*', { count: 'exact', head: true })
        .gte('created_at', staleDate.toISOString());

      if (recentCount === 0) {
        checks.issues.push({
          type: 'stale_data',
          message: `No products added in last ${this.config.staleThresholdDays} days`,
          action: 'trigger_import'
        });
      }

      // Products without affiliate links
      const { count: noAffiliateCount } = await this.supabase
        .from('products')
        .select('*', { count: 'exact', head: true })
        .is('metadata->affiliate_url', null);

      checks.productsWithoutAffiliateLinks = noAffiliateCount;
      if (noAffiliateCount > totalCount * 0.5) {
        checks.issues.push({
          type: 'missing_affiliate_links',
          message: `${noAffiliateCount} products without affiliate links`,
          action: 'enrich_affiliate_links'
        });
      }

      checks.status = checks.issues.length === 0 ? 'healthy' : 'needs_attention';

    } catch (error) {
      checks.status = 'error';
      checks.error = error.message;
    }

    return checks;
  }

  /**
   * Get recommended actions based on health check
   */
  async getRecommendedActions() {
    const health = await this.healthCheck();
    const actions = [];

    for (const issue of health.issues) {
      switch (issue.action) {
        case 'trigger_import':
          actions.push({
            task: 'run_data_import',
            priority: 'high',
            script: 'node import-all.cjs',
            reason: issue.message
          });
          break;
        case 'enrich_affiliate_links':
          actions.push({
            task: 'enrich_affiliate_links',
            priority: 'medium',
            count: health.productsWithoutAffiliateLinks,
            reason: issue.message
          });
          break;
      }
    }

    return { health, actions };
  }

  /**
   * Search products for Javari AI responses
   */
  async searchProducts(query, options = {}) {
    const { category, limit = 10, priceRange } = options;

    let queryBuilder = this.supabase
      .from('products')
      .select('*')
      .textSearch('name', query, { type: 'websearch' })
      .limit(limit);

    if (category) {
      queryBuilder = queryBuilder.eq('category', category);
    }

    if (priceRange) {
      if (priceRange.min) queryBuilder = queryBuilder.gte('price', priceRange.min);
      if (priceRange.max) queryBuilder = queryBuilder.lte('price', priceRange.max);
    }

    const { data, error } = await queryBuilder;
    if (error) throw error;

    return data;
  }

  /**
   * Get product recommendations
   */
  async getRecommendations(preferences = {}) {
    const { category, style, priceMax, country, limit = 5 } = preferences;

    let query = this.supabase
      .from('products')
      .select('*')
      .not('price', 'is', null)
      .limit(limit);

    if (category) query = query.eq('category', category);
    if (style) query = query.ilike('style', `%${style}%`);
    if (priceMax) query = query.lte('price', priceMax);
    if (country) query = query.eq('country', country);

    // Order by rating/points if available
    query = query.order('metadata->points', { ascending: false, nullsFirst: false });

    const { data, error } = await query;
    if (error) throw error;

    return data;
  }

  /**
   * Enrich product with affiliate link (Awin integration)
   */
  async enrichAffiliateLink(productId, affiliateUrl) {
    const { error } = await this.supabase
      .from('products')
      .update({
        metadata: this.supabase.sql`metadata || '{"affiliate_url": "${affiliateUrl}"}'::jsonb`,
        updated_at: new Date().toISOString()
      })
      .eq('id', productId);

    return !error;
  }

  /**
   * Bulk enrich affiliate links
   */
  async bulkEnrichAffiliateLinks(mappings) {
    let success = 0;
    let failed = 0;

    for (const { productId, affiliateUrl } of mappings) {
      const result = await this.enrichAffiliateLink(productId, affiliateUrl);
      if (result) success++;
      else failed++;
    }

    return { success, failed };
  }

  /**
   * Get stats for dashboard
   */
  async getDashboardStats() {
    const health = await this.healthCheck();
    
    return {
      totalProducts: health.totalProducts,
      byCategory: health.byCategory,
      bySource: health.bySource,
      productsWithAffiliateLinks: health.totalProducts - health.productsWithoutAffiliateLinks,
      dataHealth: health.status,
      lastUpdated: new Date().toISOString()
    };
  }
}

// Export for use in Javari AI
module.exports = { JavariSpiritsDataManager };

// CLI usage
if (require.main === module) {
  const manager = new JavariSpiritsDataManager();
  
  const command = process.argv[2];
  
  switch (command) {
    case 'health':
      manager.healthCheck().then(r => console.log(JSON.stringify(r, null, 2)));
      break;
    case 'actions':
      manager.getRecommendedActions().then(r => console.log(JSON.stringify(r, null, 2)));
      break;
    case 'stats':
      manager.getDashboardStats().then(r => console.log(JSON.stringify(r, null, 2)));
      break;
    case 'search':
      const query = process.argv[3] || 'wine';
      manager.searchProducts(query).then(r => console.log(JSON.stringify(r, null, 2)));
      break;
    default:
      console.log('Javari Spirits Data Manager');
      console.log('Commands: health, actions, stats, search <query>');
  }
}
