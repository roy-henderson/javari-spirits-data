# Javari Spirits - Automated Data Import Package

**Created:** 2026-01-03 23:48 ET  
**Status:** Ready for deployment when Supabase connectivity available

## Quick Start

```bash
cd /path/to/spirits-import
npm install
node import-cjs.cjs
```

## Files Included

### Import Scripts
| File | Purpose |
|------|---------|
| `import-cjs.cjs` | Main import script - handles all 3 CSV formats |
| `retry-import.sh` | Bash wrapper with retry logic |

### Data Files (Ready to Import)
| File | Records | Category | Size |
|------|---------|----------|------|
| `total_wine.csv` | 1,558 | Wine | 543KB |
| `open_beer_db.csv` | 5,900 | Beer | 1.2MB |
| `craft_cans.csv` | 2,400+ | Craft Beer | 234KB |
| `winemag_150k.csv` | 150,000 | Wine Reviews | 49MB |
| `kaggle_wine_reviews_130k.csv` | 130,000 | Wine Reviews | 53MB |
| `saq_db.csv` | ~50,000 | Quebec Liquor | 3.5MB |
| `ml_whiskey.csv` | ~10,000 | Whiskey | 1.4MB |

**Total Potential Products:** ~350,000+

## CSV Format Notes

### total_wine.csv (Standard CSV)
- Headers: WineName, Price, CountryState, Region, ProductType, VarietalType, Description
- Clean format, no issues

### open_beer_db.csv (No Header Row)
- First row is data, not headers
- Inferred columns: name, id, brewery_id, ..., style, brewery_type, brewery_name, city, state, country
- Contains carriage returns (^M)

### craft_cans.csv (Malformed)
- Uses carriage return (^M / \r) as line delimiter instead of newline
- Has BOM at start
- Headers embedded in first data row
- Columns: Beer, Brewery, Location, Style, Size, ABV, IBUs

## Database Schema Required

```sql
CREATE TABLE IF NOT EXISTS products (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR(255) NOT NULL,
  category VARCHAR(50) NOT NULL,
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
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_source ON products(source);
CREATE INDEX idx_products_name ON products USING gin(to_tsvector('english', name));
```

## Environment Variables

```bash
SUPABASE_URL=https://ggmbwrtjwjvwwmljypqv.supabase.co
SUPABASE_SERVICE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

## Automation Integration

For Javari AI autonomous execution:

```javascript
// In Javari's task queue
{
  task: 'import_spirits_data',
  script: '/scripts/import-cjs.cjs',
  retry: {
    maxAttempts: 5,
    delayMs: 60000,
    backoffMultiplier: 2
  },
  healthCheck: {
    endpoint: 'https://ggmbwrtjwjvwwmljypqv.supabase.co/rest/v1/products?select=count',
    expectedMinCount: 5000
  }
}
```

## Next Steps (When Connectivity Available)

1. Run `node import-cjs.cjs` 
2. Verify counts in Supabase dashboard
3. Add remaining large datasets (winemag_150k, kaggle_wine_reviews_130k)
4. Set up affiliate link enrichment via Awin API
5. Configure product search API endpoints

## Troubleshooting

**DNS/Network Issues:**
- Error: `getaddrinfo EAI_AGAIN` or `dns_nxdomain`
- Solution: Wait and retry, or run from local machine

**Duplicate Key Errors:**
- The script uses `source_id` for deduplication
- Safe to re-run; duplicates will error but not corrupt data

**Batch Failures:**
- Script processes in batches of 200
- Failed batches are logged; successful ones continue
