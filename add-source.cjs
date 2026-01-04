#!/usr/bin/env node
/**
 * JAVARI SPIRITS - Quick Source Adder CLI
 * Usage: node add-source.cjs <url> [name] [type]
 * 
 * Examples:
 *   node add-source.cjs https://data.gov/dataset/liquor
 *   node add-source.cjs https://github.com/user/whiskey-data "Whiskey Dataset" spirits
 *   node add-source.cjs https://example.com/wine.csv "Wine Prices" wine
 */

const https = require('https');
const { URL } = require('url');

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://kteobfyferrukqeolofj.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY || process.env.SUPABASE_KEY;

async function addSource(sourceUrl, name, alcoholType) {
    // Auto-detect info from URL
    let source = {
        source_url: sourceUrl,
        source_name: name || sourceUrl.split('/').pop() || new URL(sourceUrl).hostname,
        status: 'approved',
        priority: 3,
        alcohol_types: [alcoholType || 'spirits'],
        commercial_use_ok: true
    };

    // Publisher detection
    if (sourceUrl.includes('.gov')) {
        source.publisher = 'gov';
        source.license_name = 'Public Domain';
    } else if (sourceUrl.includes('github.com')) {
        source.publisher = 'community';
        source.format = 'github';
    } else if (sourceUrl.includes('kaggle.com')) {
        source.publisher = 'community';
    } else if (sourceUrl.includes('huggingface.co')) {
        source.publisher = 'community';
        source.format = 'parquet';
    } else {
        source.publisher = 'community';
    }

    // Format detection
    if (sourceUrl.endsWith('.csv')) source.format = 'csv';
    else if (sourceUrl.endsWith('.json')) source.format = 'json';
    else if (sourceUrl.endsWith('.xlsx') || sourceUrl.endsWith('.xls')) source.format = 'xlsx';

    // Insert to Supabase
    return new Promise((resolve, reject) => {
        const url = new URL(`${SUPABASE_URL}/rest/v1/dataset_sources`);
        const data = JSON.stringify(source);

        const req = https.request({
            hostname: url.hostname,
            path: url.pathname,
            method: 'POST',
            headers: {
                'apikey': SUPABASE_KEY,
                'Authorization': `Bearer ${SUPABASE_KEY}`,
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(data),
                'Prefer': 'return=representation'
            }
        }, (res) => {
            let body = '';
            res.on('data', chunk => body += chunk);
            res.on('end', () => {
                if (res.statusCode >= 200 && res.statusCode < 300) {
                    resolve(JSON.parse(body));
                } else {
                    reject(new Error(`HTTP ${res.statusCode}: ${body}`));
                }
            });
        });

        req.on('error', reject);
        req.write(data);
        req.end();
    });
}

async function main() {
    const args = process.argv.slice(2);
    
    if (args.length === 0) {
        console.log(`
╔═══════════════════════════════════════════════════════════════╗
║         JAVARI SPIRITS - Quick Source Adder                   ║
╚═══════════════════════════════════════════════════════════════╝

Usage: node add-source.cjs <url> [name] [type]

Arguments:
  url   - The dataset URL (required)
  name  - Source name (optional, auto-detected)
  type  - Alcohol type: spirits, whiskey, wine, beer, etc.

Examples:
  node add-source.cjs https://data.ca.gov/dataset/abc-prices
  node add-source.cjs https://github.com/user/whiskey "My Whiskey Data" whiskey
  node add-source.cjs https://example.com/wine.csv "Wine Prices" wine

Bulk add (pipe URLs):
  cat urls.txt | xargs -I {} node add-source.cjs {}
`);
        return;
    }

    const [url, name, type] = args;

    if (!SUPABASE_KEY) {
        console.error('❌ Error: SUPABASE_SERVICE_KEY or SUPABASE_KEY environment variable required');
        process.exit(1);
    }

    try {
        console.log(`Adding source: ${url}`);
        const result = await addSource(url, name, type);
        console.log(`✅ Added: ${result[0]?.source_name || 'Success'}`);
        console.log(`   ID: ${result[0]?.id}`);
        console.log(`   Status: ${result[0]?.status}`);
    } catch (error) {
        console.error(`❌ Failed: ${error.message}`);
        process.exit(1);
    }
}

main();
