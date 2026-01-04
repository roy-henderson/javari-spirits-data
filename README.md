# Javari Spirits - Complete Data Import System

**Version:** 2.0.0  
**Products:** 405,000+  
**Sources:** 20+  
**Created:** 2026-01-04  
**Author:** CR AudioViz AI, LLC

## Overview

Complete automated product data pipeline for Javari Spirits with:
- **405,000+ unique products** from 20+ verified data sources
- **GitHub Actions** for scheduled weekly imports
- **Javari AI Integration** for autonomous data management
- **Self-healing** with retry logic and health monitoring

## Quick Start

```bash
# Clone
git clone https://github.com/roy-henderson/javari-spirits-data.git
cd javari-spirits-data

# Install
npm install

# Configure
cp .env.example .env
# Add your Supabase credentials

# Run complete import
node import-complete.cjs
```

## Data Sources (405,000+ Products)

### Wines (~282,000)
| Source | Records | Description |
|--------|---------|-------------|
| WineMag | 150,936 | Wine reviews with ratings, prices, regions |
| Kaggle Wine | 129,976 | Wine reviews with descriptions |
| Total Wine | 1,559 | Retail wine data |
| Wikidata Wines | 345 | Wine entities |

### Beers (~85,000)
| Source | Records | Description |
|--------|---------|-------------|
| Beer.csv | 66,056 | Comprehensive beer database |
| Open Brewery DB | 9,077 | US/international breweries |
| Open Beer DB | 5,901 | Beer products with ABV |
| Craft Beers | 2,411 | Craft beer with IBU/ABV |
| Wikidata Breweries | 1,743 | Brewery entities |

### Spirits (~37,000)
| Source | Records | Description |
|--------|---------|-------------|
| SAQ Database | 11,953 | Quebec liquor products |
| Iowa Products Catalog | 10,640 | Official state product catalog |
| Iowa Unique Products | 7,909 | Extracted from sales data |
| Makispl Whiskey | 3,158 | Premium whiskey reviews |
| ML Whiskey | 3,158 | Whiskey ML dataset |
| Wikidata Spirits | 338 | Spirit entities |
| Strathclyde Scotch | 87 | Single malt flavor profiles |

### Cocktails (~1,000)
| Source | Records | Description |
|--------|---------|-------------|
| Boston Cocktails | 989 | Classic cocktail recipes |
| IBA Official | 77 | International Bartenders Association |

## File Structure

```
javari-spirits-data/
├── .github/workflows/
│   └── import.yml              # GitHub Actions
├── data/
│   ├── winemag_150k.csv        # 150K wine reviews
│   ├── kaggle_wine_reviews_130k.csv  # 130K wine reviews
│   ├── beer.csv                # 66K beers
│   ├── iowa_products_catalog.csv     # 10K spirits (official)
│   ├── saq_db.csv              # 12K Quebec liquor
│   ├── openbrewerydb_full.csv  # 9K breweries
│   ├── iowa_unique_products.csv      # 8K unique products
│   ├── open_beer_db.csv        # 6K beers
│   ├── boston_cocktails.csv    # 3.6K cocktails
│   ├── ml_whiskey.csv          # 3K whiskeys
│   ├── makispl_whiskey.csv     # 3K premium whiskeys
│   ├── nickhould_beers.csv     # 2.4K craft beers
│   ├── wikidata_*.csv          # Wikidata entities
│   ├── total_wine.csv          # 1.5K wines
│   ├── strathclyde_whiskies.csv    # 87 scotch profiles
│   └── iba_cocktails.csv       # 77 official cocktails
├── import-complete.cjs         # 20+ importers
├── javari-integration.cjs      # Javari AI module
├── package.json
└── README.md
```

## Database Schema

```sql
CREATE TABLE products (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR(255) NOT NULL,
  category VARCHAR(50) NOT NULL,        -- wine, beer, spirits, cocktails
  subcategory VARCHAR(100),
  brand VARCHAR(100),
  description TEXT,
  price DECIMAL(10,2),
  alcohol_content DECIMAL(5,2),
  country VARCHAR(100),
  region VARCHAR(100),
  style VARCHAR(100),
  size VARCHAR(50),
  source VARCHAR(50) NOT NULL,
  source_id VARCHAR(100),
  metadata JSONB,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(source, source_id)
);

CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_source ON products(source);
CREATE INDEX idx_products_name_gin ON products USING gin(to_tsvector('english', name));
```

## Javari AI Integration

```javascript
const { JavariSpiritsDataManager } = require('./javari-integration.cjs');
const manager = new JavariSpiritsDataManager();

// Health check
const health = await manager.healthCheck();

// Get recommendations  
const { actions } = await manager.getRecommendedActions();

// Search products
const wines = await manager.searchProducts('cabernet', {
  category: 'wine',
  priceRange: { max: 50 }
});

// Dashboard stats
const stats = await manager.getDashboardStats();
```

## GitHub Actions

Secrets required:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_KEY`

Schedule:
- Weekly on Sundays at 3 AM ET
- Manual trigger via workflow_dispatch

## Large Files

Files over 50MB need Git LFS:
- `winemag_150k.csv` (48 MB)
- `kaggle_wine_reviews_130k.csv` (51 MB)

To add locally:
```bash
git lfs install
git lfs track "data/*.csv"
```

## Deployment

### Local
```bash
npm install
node import-complete.cjs
```

### With Retry
```bash
chmod +x retry-import.sh
./retry-import.sh
```

### Docker
```bash
docker build -t javari-spirits .
docker run -e SUPABASE_URL=... -e SUPABASE_SERVICE_KEY=... javari-spirits
```

## API Usage (Future)

```bash
# Search products
GET /api/products?q=bourbon&category=spirits&limit=20

# Get product details
GET /api/products/:id

# Recommendations
GET /api/recommendations?style=whiskey&priceMax=100
```

## Data Quality

- All sources verified and publicly available
- Deduplication via source + source_id
- Price normalization
- ABV/Proof conversion
- Category standardization

## License

Data sources retain their original licenses. Import scripts are MIT.

---

**CR AudioViz AI, LLC** - Your Story. Our Design.

https://craudiovizai.com
