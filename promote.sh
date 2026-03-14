#!/bin/bash
# promote.sh
# Promotes the dev database to prod after a successful pipeline run.
# Run this after: ingest → load_raw → dbt build → detector → explainer

set -e  # exit on any error

echo "🚀 Promoting dev → prod..."

echo "📦 Copying database..."
cp data/finsight_dev.duckdb data/finsight_prod.duckdb

echo "✅ Promotion complete. Dashboard will now reflect latest data."