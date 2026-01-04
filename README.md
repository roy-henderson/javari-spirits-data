# Javari Spirits Data Harvester

Continuous alcohol dataset discovery, ingestion, and enrichment system.

## Features

- **Multi-source ingestion**: Iowa ABD, Connecticut, Montgomery County MD, and more
- **Automatic deduplication**: Barcode + brand + normalized name matching
- **Scheduled updates**: Daily enrichment, weekly full refresh
- **Discovery engine**: Finds new datasets automatically
- **Provenance tracking**: Full audit trail for compliance

## Sources

### Government (Backbone)
- Iowa ABD Products & Sales
- Connecticut Liquor Brands (72K+)
- Montgomery County MD Inventory (307K+)
- Pennsylvania PLCB (pending)
- TTB COLA Registry (pending)

### Community (Enrichment)
- Open Food Facts (global backbone)
- Wikidata (spirits taxonomy)
- TheCocktailDB (recipes)
- Open Brewery DB (breweries)
- ML Whiskey Dataset (ratings)

## Usage

```bash
# Run all sources
npm run harvest

# Run specific source
npm run harvest:iowa
npm run harvest:cocktails

# Discover new sources
npm run discover
```

## Database Schema

See `schema.sql` for complete Supabase schema including:
- `canonical_products` - Master deduplicated products
- `dataset_sources` - Source registry
- `ingest_runs` - Audit trail
- `price_history` - Pricing data
- `cocktail_recipes` - Recipe database
- `producers` - Breweries, distilleries, wineries

## License

Proprietary - CR AudioViz AI, LLC
