#!/bin/bash
# Javari Spirits - Automated Import with Retry Logic
# Created: 2026-01-03 23:48 ET

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/import.log"
MAX_RETRIES=10
RETRY_DELAY=60  # seconds
BACKOFF_MULTIPLIER=2

log() {
    echo "[$(TZ='America/New_York' date '+%Y-%m-%d %H:%M:%S ET')] $1" | tee -a "$LOG_FILE"
}

check_connectivity() {
    curl -s --max-time 10 \
        -H "apikey: $SUPABASE_SERVICE_KEY" \
        "$SUPABASE_URL/rest/v1/products?select=id&limit=1" > /dev/null 2>&1
    return $?
}

run_import() {
    cd "$SCRIPT_DIR"
    node import-cjs.cjs 2>&1 | tee -a "$LOG_FILE"
    return ${PIPESTATUS[0]}
}

main() {
    log "=========================================="
    log "JAVARI SPIRITS - Automated Import Started"
    log "=========================================="
    
    # Load environment if .env exists
    if [ -f "$SCRIPT_DIR/.env" ]; then
        export $(grep -v '^#' "$SCRIPT_DIR/.env" | xargs)
        log "Loaded environment from .env"
    fi
    
    # Verify required vars
    if [ -z "$SUPABASE_URL" ] || [ -z "$SUPABASE_SERVICE_KEY" ]; then
        log "ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set"
        exit 1
    fi
    
    attempt=1
    delay=$RETRY_DELAY
    
    while [ $attempt -le $MAX_RETRIES ]; do
        log "Attempt $attempt of $MAX_RETRIES"
        
        # Check connectivity first
        log "Testing Supabase connectivity..."
        if check_connectivity; then
            log "✅ Connection successful"
            
            log "Running import..."
            if run_import; then
                log "=========================================="
                log "✅ IMPORT COMPLETED SUCCESSFULLY"
                log "=========================================="
                exit 0
            else
                log "❌ Import failed, will retry..."
            fi
        else
            log "❌ Cannot reach Supabase, will retry in ${delay}s..."
        fi
        
        if [ $attempt -lt $MAX_RETRIES ]; then
            log "Waiting ${delay} seconds before retry..."
            sleep $delay
            delay=$((delay * BACKOFF_MULTIPLIER))
            # Cap delay at 30 minutes
            if [ $delay -gt 1800 ]; then
                delay=1800
            fi
        fi
        
        attempt=$((attempt + 1))
    done
    
    log "=========================================="
    log "❌ IMPORT FAILED after $MAX_RETRIES attempts"
    log "=========================================="
    exit 1
}

main "$@"
