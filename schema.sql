-- ============================================================================
-- JAVARI SPIRITS DATA HARVESTER - SUPABASE SCHEMA v1
-- Created: 2026-01-04
-- Purpose: Continuous alcohol dataset discovery, ingestion, and enrichment
-- ============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- For fuzzy matching

-- ============================================================================
-- DATASET SOURCE REGISTRY
-- Tracks all discovered and ingested data sources
-- ============================================================================

CREATE TABLE IF NOT EXISTS dataset_sources (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_name TEXT NOT NULL,
    source_url TEXT UNIQUE NOT NULL,
    publisher TEXT,  -- gov / community / org / affiliate
    country_region TEXT,
    alcohol_types TEXT[] DEFAULT '{}',  -- gin, rum, tequila, mezcal, wine, beer, RTD, cocktail
    format TEXT,  -- csv, json, xls, pdf, html, api
    license_name TEXT,
    license_url TEXT,
    commercial_use_ok BOOLEAN DEFAULT true,
    attribution_required BOOLEAN DEFAULT false,
    attribution_text TEXT,
    last_checked_at TIMESTAMPTZ,
    last_changed_at TIMESTAMPTZ,
    etag_or_hash TEXT,  -- For change detection
    row_count INTEGER,
    status TEXT DEFAULT 'discovered',  -- discovered, approved, ingesting, ingested, blocked, error
    priority INTEGER DEFAULT 5,  -- 1=highest, 10=lowest
    notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_dataset_sources_status ON dataset_sources(status);
CREATE INDEX idx_dataset_sources_alcohol_types ON dataset_sources USING GIN(alcohol_types);

-- ============================================================================
-- DATASET FILES (individual files within a source)
-- ============================================================================

CREATE TABLE IF NOT EXISTS dataset_files (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id UUID REFERENCES dataset_sources(id) ON DELETE CASCADE,
    file_url TEXT NOT NULL,
    file_name TEXT,
    file_type TEXT,  -- csv, json, xlsx
    expected_schema JSONB,
    last_ingested_at TIMESTAMPTZ,
    file_hash TEXT,
    file_size_bytes BIGINT,
    row_count INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- INGESTION RUNS (audit trail for every import)
-- ============================================================================

CREATE TABLE IF NOT EXISTS ingest_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id UUID REFERENCES dataset_sources(id),
    run_type TEXT DEFAULT 'full',  -- full, incremental, enrichment
    started_at TIMESTAMPTZ DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT DEFAULT 'running',  -- running, completed, failed, cancelled
    rows_read INTEGER DEFAULT 0,
    rows_inserted INTEGER DEFAULT 0,
    rows_updated INTEGER DEFAULT 0,
    rows_rejected INTEGER DEFAULT 0,
    rows_deduplicated INTEGER DEFAULT 0,
    reject_reasons JSONB DEFAULT '{}',
    diff_summary JSONB,
    error_message TEXT,
    evidence_log TEXT,  -- Detailed log
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_ingest_runs_source ON ingest_runs(source_id);
CREATE INDEX idx_ingest_runs_status ON ingest_runs(status);

-- ============================================================================
-- CANONICAL PRODUCTS (Master deduplicated product table)
-- ============================================================================

CREATE TABLE IF NOT EXISTS canonical_products (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Identifiers
    upc_ean_gtin TEXT,
    sku TEXT,
    
    -- Core product info
    name TEXT NOT NULL,
    name_normalized TEXT,  -- Lowercase, no special chars for matching
    brand TEXT,
    brand_normalized TEXT,
    producer TEXT,
    
    -- Classification
    category_primary TEXT,  -- spirits, wine, beer, rtd, cocktail, ingredient
    category_secondary TEXT,  -- whiskey, vodka, gin, rum, tequila, etc.
    category_tertiary TEXT,  -- bourbon, scotch, irish, etc.
    
    -- Origin
    country TEXT,
    region TEXT,
    sub_region TEXT,
    
    -- Product details
    abv NUMERIC(5,2),
    volume_ml INTEGER,
    package_count INTEGER DEFAULT 1,
    age_statement TEXT,
    vintage INTEGER,
    cask_type TEXT,
    
    -- Enrichment
    flavor_tags TEXT[] DEFAULT '{}',
    description TEXT,
    tasting_notes TEXT,
    
    -- Media
    image_url TEXT,
    image_urls TEXT[] DEFAULT '{}',
    
    -- Ratings (aggregated)
    avg_rating NUMERIC(3,1),
    rating_count INTEGER DEFAULT 0,
    
    -- Source tracking
    source_refs JSONB DEFAULT '[]',  -- [{source_id, source_sku, confidence, retrieved_at}]
    primary_source_id UUID,
    
    -- Metadata
    is_active BOOLEAN DEFAULT true,
    confidence_score NUMERIC(3,2) DEFAULT 1.0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for fast lookup and deduplication
CREATE INDEX idx_canonical_products_upc ON canonical_products(upc_ean_gtin) WHERE upc_ean_gtin IS NOT NULL;
CREATE INDEX idx_canonical_products_name_norm ON canonical_products(name_normalized);
CREATE INDEX idx_canonical_products_brand_norm ON canonical_products(brand_normalized);
CREATE INDEX idx_canonical_products_category ON canonical_products(category_primary, category_secondary);
CREATE INDEX idx_canonical_products_country ON canonical_products(country);
CREATE INDEX idx_canonical_products_trgm_name ON canonical_products USING GIN(name_normalized gin_trgm_ops);

-- ============================================================================
-- PRICE HISTORY
-- ============================================================================

CREATE TABLE IF NOT EXISTS price_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    product_id UUID REFERENCES canonical_products(id) ON DELETE CASCADE,
    source_id UUID REFERENCES dataset_sources(id),
    price NUMERIC(10,2) NOT NULL,
    currency TEXT DEFAULT 'USD',
    price_type TEXT DEFAULT 'retail',  -- retail, wholesale, sale
    location TEXT,  -- State, store, etc.
    effective_date DATE,
    retrieved_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_price_history_product ON price_history(product_id);
CREATE INDEX idx_price_history_date ON price_history(effective_date DESC);

-- ============================================================================
-- BRANDS & PRODUCERS (Normalized entities)
-- ============================================================================

CREATE TABLE IF NOT EXISTS brands (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT UNIQUE NOT NULL,
    name_normalized TEXT,
    parent_company TEXT,
    country TEXT,
    founded_year INTEGER,
    website TEXT,
    wikidata_id TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS producers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    name_normalized TEXT,
    type TEXT,  -- distillery, winery, brewery, etc.
    country TEXT,
    region TEXT,
    address TEXT,
    latitude NUMERIC(10,7),
    longitude NUMERIC(10,7),
    founded_year INTEGER,
    website TEXT,
    wikidata_id TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- COCKTAIL RECIPES
-- ============================================================================

CREATE TABLE IF NOT EXISTS cocktail_recipes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    name_normalized TEXT,
    category TEXT,
    glass_type TEXT,
    iba_category TEXT,
    instructions TEXT,
    ingredients JSONB DEFAULT '[]',  -- [{ingredient, amount, unit}]
    garnish TEXT,
    image_url TEXT,
    source_id UUID REFERENCES dataset_sources(id),
    source_recipe_id TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- DISCOVERY QUEUE (New sources to evaluate)
-- ============================================================================

CREATE TABLE IF NOT EXISTS discovery_queue (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    discovered_url TEXT NOT NULL,
    discovered_title TEXT,
    discovered_from TEXT,  -- data.gov, kaggle, github, etc.
    search_terms TEXT,
    potential_type TEXT,  -- catalog, pricing, reviews, recipes
    alcohol_types_detected TEXT[],
    format_detected TEXT,
    license_detected TEXT,
    row_count_estimate INTEGER,
    priority_score INTEGER DEFAULT 5,
    status TEXT DEFAULT 'pending',  -- pending, approved, rejected, duplicate
    notes TEXT,
    discovered_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- HARVESTER JOBS (Scheduled tasks)
-- ============================================================================

CREATE TABLE IF NOT EXISTS harvester_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_name TEXT NOT NULL,
    job_type TEXT,  -- discovery, ingest, enrich, dedupe, alert
    schedule TEXT,  -- cron expression
    enabled BOOLEAN DEFAULT true,
    last_run_at TIMESTAMPTZ,
    next_run_at TIMESTAMPTZ,
    last_status TEXT,
    config JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- ALERTS (Notifications for new sources)
-- ============================================================================

CREATE TABLE IF NOT EXISTS harvester_alerts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    alert_type TEXT,  -- new_source, source_updated, ingest_failed, new_products
    severity TEXT DEFAULT 'medium',  -- high, medium, low
    title TEXT NOT NULL,
    message TEXT,
    source_id UUID REFERENCES dataset_sources(id),
    metadata JSONB,
    acknowledged BOOLEAN DEFAULT false,
    acknowledged_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Normalize text for matching
CREATE OR REPLACE FUNCTION normalize_text(input TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN LOWER(REGEXP_REPLACE(
        REGEXP_REPLACE(input, '[^a-zA-Z0-9\s]', '', 'g'),
        '\s+', ' ', 'g'
    ));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply triggers
CREATE TRIGGER update_dataset_sources_updated_at
    BEFORE UPDATE ON dataset_sources
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER update_canonical_products_updated_at
    BEFORE UPDATE ON canonical_products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ============================================================================
-- INITIAL SEED DATA: Known data sources
-- ============================================================================

INSERT INTO dataset_sources (source_name, source_url, publisher, country_region, alcohol_types, format, license_name, commercial_use_ok, attribution_required, status, priority) VALUES
-- Backbone sources
('Open Food Facts', 'https://world.openfoodfacts.org/data', 'community', 'global', ARRAY['spirits','wine','beer','rtd'], 'csv', 'ODbL', true, true, 'approved', 1),
('Wikidata Spirits', 'https://query.wikidata.org/', 'community', 'global', ARRAY['spirits','wine','beer'], 'sparql', 'CC0', true, false, 'approved', 2),

-- US Government
('Iowa ABD Sales', 'https://data.iowa.gov/api/views/m3tr-qhgy/rows.csv', 'gov', 'US-IA', ARRAY['spirits'], 'csv', 'Public Domain', true, false, 'ingested', 1),
('Iowa ABD Products', 'https://data.iowa.gov/api/views/gckp-fe7r/rows.csv', 'gov', 'US-IA', ARRAY['spirits'], 'csv', 'Public Domain', true, false, 'approved', 1),
('Connecticut Liquor Brands', 'https://data.ct.gov/api/views/u6ds-fzyp/rows.csv', 'gov', 'US-CT', ARRAY['spirits','wine','beer'], 'csv', 'Public Domain', true, false, 'approved', 1),
('Montgomery County MD', 'https://data.montgomerycountymd.gov/api/views/v76h-r7br/rows.csv', 'gov', 'US-MD', ARRAY['spirits','wine','beer'], 'csv', 'Public Domain', true, false, 'approved', 1),
('Pennsylvania PLCB', 'https://www.lcb.pa.gov/Consumers/Pages/ProductPrices.aspx', 'gov', 'US-PA', ARRAY['spirits','wine'], 'xls', 'Public Domain', true, false, 'discovered', 2),
('TTB COLA Registry', 'https://www.ttb.gov/labeling/cola-public-registry', 'gov', 'US', ARRAY['spirits','wine','beer'], 'html', 'Public Domain', true, false, 'discovered', 2),

-- Enrichment
('ML Whiskey Dataset', 'https://github.com/makispl/Machine-Learning-Whiskey-Dataset', 'community', 'global', ARRAY['spirits'], 'csv', 'MIT', true, false, 'ingested', 3),
('CocktailDB', 'https://www.thecocktaildb.com/api/json/v1/1/', 'community', 'global', ARRAY['cocktail'], 'json', 'CC BY-NC', true, true, 'approved', 3),
('Open Brewery DB', 'https://api.openbrewerydb.org/v1/breweries', 'community', 'global', ARRAY['beer'], 'json', 'Public Domain', true, false, 'approved', 3),
('Vivino Ratings', 'https://www.kaggle.com/datasets/budnyak/wine-rating-and-price', 'community', 'global', ARRAY['wine'], 'csv', 'CC BY 4.0', true, true, 'discovered', 3),
('WineMag Reviews', 'https://www.kaggle.com/datasets/zynicide/wine-reviews', 'community', 'global', ARRAY['wine'], 'csv', 'CC BY-NC-SA', true, true, 'discovered', 3),
('Mezcalistas Database', 'https://mezcalistas.com/mezcal-reviews/', 'community', 'MX', ARRAY['spirits'], 'html', 'Unknown', false, true, 'discovered', 4),
('Japanese Whisky Reviews', 'https://www.kaggle.com/datasets/koki25ando/japanese-whisky', 'community', 'JP', ARRAY['spirits'], 'csv', 'CC0', true, false, 'discovered', 3)

ON CONFLICT (source_url) DO UPDATE SET
    updated_at = NOW();

-- ============================================================================
-- ROW LEVEL SECURITY (Enable for production)
-- ============================================================================

-- ALTER TABLE canonical_products ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE dataset_sources ENABLE ROW LEVEL SECURITY;

