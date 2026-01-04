# Javari Spirits - Automated Data Import System

**Version:** 1.0.0  
**Created:** 2026-01-03  
**Author:** CR AudioViz AI, LLC

## Overview

This package provides a fully automated product data pipeline for Javari Spirits, enabling:
- **~370,000+ products** from 9 data sources
- **GitHub Actions** scheduled weekly imports
- **Javari AI Integration** for autonomous data management
- **Self-healing** with retry logic and health monitoring

## Quick Start

```bash
# 1. Clone/extract the package
cd javari-spirits-automation

# 2. Install dependencies
npm install

# 3. Configure environment
cp .env.example .env
# Edit .env with your Supabase credentials

# 4. Run import
node import-all.cjs

# Or with auto-retry
./retry-import.sh
```

## Data Sources

| Source | Records | Category | File |
|--------|---------|----------|------|
| WineMag | 150,000 | Wine Reviews | winemag_150k.csv |
| Kaggle Wine | 130,000 | Wine Reviews | kaggle_wine_reviews_130k.csv |
| Beer.csv | 66,000 | Beer | beer.csv |
| SAQ Database | 12,000 | Quebec Liquor | saq_db.csv |
| Open Brewery | 9,000 | Breweries | openbrewerydb_full.csv |
| Open Beer DB | 5,900 | Beer | open_beer_db.csv |
| Makispl Whiskey | 3,158 | Premium Whiskey | makispl_whiskey.csv |
| ML Whiskey | 3,158 | Whiskey | ml_whiskey.csv |
| Nickhould Beers | 2,411 | Craft Beer | nickhould_beers.csv |
| Craft Cans | 2,400+ | Craft Beer | craft_cans.csv |
| Total Wine | 1,558 | Wine | total_wine.csv |
| Strathclyde | 86 | Scotch Whisky | strathclyde_whiskies.csv |

**Total Potential:** ~384,000+ products

## File Structure

```
javari-spirits-automation/
├── .github/
│   └── workflows/
│       └── import.yml          # GitHub Actions workflow
├── data/
│   ├── total_wine.csv
│   ├── open_beer_db.csv
│   ├── craft_cans.csv
│   ├── winemag_150k.csv
│   ├── kaggle_wine_reviews_130k.csv
│   ├── beer.csv
│   ├── saq_db.csv
│   ├── ml_whiskey.csv
│   └── openbrewerydb_full.csv
├── import-all.cjs              # Main comprehensive import
├── import-cjs.cjs              # Basic 3-source import
├── javari-integration.cjs      # Javari AI integration module
├── retry-import.sh             # Bash wrapper with retry
├── package.json
├── .env.example
└── README.md
```

## GitHub Actions Setup

1. Add repository secrets:
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_KEY`
   - `JAVARI_WEBHOOK_URL` (optional)

2. The workflow runs:
   - **Manually** via workflow_dispatch
   - **Weekly** on Sundays at 3 AM ET
   - **On push** to import-all.cjs or workflow file

## Javari AI Integration

```javascript
const { JavariSpiritsDataManager } = require('./javari-integration.cjs');

const manager = new JavariSpiritsDataManager();

// Health check
const health = await manager.healthCheck();
console.log(health);

// Get recommended actions
const { actions } = await manager.getRecommendedActions();
// Returns: [{ task: 'run_data_import', priority: 'high', ... }]

// Search products
const results = await manager.searchProducts('cabernet sauvignon', {
  category: 'wine',
  priceRange: { max: 50 }
});

// Get recommendations
const recs = await manager.getRecommendations({
  category: 'wine',
  style: 'Pinot Noir',
  priceMax: 30
});

// Dashboard stats
const stats = await manager.getDashboardStats();
```

### CLI Commands

```bash
# Check data health
node javari-integration.cjs health

# Get recommended actions
node javari-integration.cjs actions

# Get dashboard stats
node javari-integration.cjs stats

# Search products
node javari-integration.cjs search "bourbon whiskey"
```

## Database Schema

```sql
CREATE TABLE IF NOT EXISTS products (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR(255) NOT NULL,
  category VARCHAR(50) NOT NULL,           -- wine, beer, spirits
  subcategory VARCHAR(100),                -- Red Wine, IPA, Bourbon, etc.
  brand VARCHAR(100),
  description TEXT,
  price DECIMAL(10,2),
  alcohol_content DECIMAL(5,2),
  country VARCHAR(100),
  region VARCHAR(100),
  style VARCHAR(100),
  size VARCHAR(50),
  source VARCHAR(50) NOT NULL,             -- total_wine, winemag, etc.
  source_id VARCHAR(100),                  -- Unique ID from source
  metadata JSONB,                          -- Additional data, affiliate links
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_source ON products(source);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_products_name_search ON products USING gin(to_tsvector('english', name));
```

## Automation Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    JAVARI AI BRAIN                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │ Health      │  │ Task        │  │ Decision            │ │
│  │ Monitor     │──│ Queue       │──│ Engine              │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
         │                                    │
         ▼                                    ▼
┌─────────────────┐                ┌─────────────────────────┐
│ Health Check    │                │ Trigger Import          │
│ (Every 6 hours) │                │ (When needed)           │
└─────────────────┘                └─────────────────────────┘
         │                                    │
         ▼                                    ▼
┌─────────────────┐                ┌─────────────────────────┐
│ Status:         │                │ GitHub Actions          │
│ - Product count │                │ - Weekly scheduled      │
│ - Data freshness│                │ - Manual trigger        │
│ - Missing links │                │ - Auto-retry            │
└─────────────────┘                └─────────────────────────┘
```

## Retry Logic

The `retry-import.sh` script provides:
- **10 retry attempts**
- **Exponential backoff** (60s → 30 min max)
- **Logging** to import.log
- **Connection verification** before each attempt

## Affiliate Link Enrichment

Future integration with Awin API:

```javascript
// In Javari AI task queue
{
  task: 'enrich_affiliate_links',
  schedule: 'daily',
  handler: async () => {
    const manager = new JavariSpiritsDataManager();
    const { data } = await supabase
      .from('products')
      .select('id, name, brand')
      .is('metadata->affiliate_url', null)
      .limit(100);
    
    for (const product of data) {
      const affiliateUrl = await awinApi.searchProduct(product.name);
      if (affiliateUrl) {
        await manager.enrichAffiliateLink(product.id, affiliateUrl);
      }
    }
  }
}
```

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| SUPABASE_URL | Supabase project URL | Yes |
| SUPABASE_SERVICE_KEY | Supabase service role key | Yes |
| JAVARI_WEBHOOK_URL | Webhook for notifications | No |

## Troubleshooting

### DNS/Connection Errors
```
Error: getaddrinfo EAI_AGAIN
```
- **Cause:** Temporary DNS resolution failure
- **Solution:** Wait and retry, or run from different network

### Duplicate Key Errors
```
Error: duplicate key value violates unique constraint
```
- **Cause:** Product already exists
- **Solution:** Safe to ignore, deduplication working

### Rate Limiting
- Script uses batch inserts (500 records)
- Implements retry with backoff
- Safe for Supabase free tier

## Support

- **GitHub Issues:** [Link to repo]
- **Documentation:** docs.craudiovizai.com
- **Contact:** support@craudiovizai.com

---

**CR AudioViz AI, LLC** - Your Story. Our Design.
