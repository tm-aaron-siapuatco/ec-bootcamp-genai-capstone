#!/bin/bash

# Dagster startup script with database environment variables
# Usage: ./start-dagster.sh

set -e

echo "🚀 Starting Dagster with PostgreSQL connection..."

# Activate virtual environment
if [ -f ".venv/bin/activate" ]; then
    echo "   Activating virtual environment..."
    source .venv/bin/activate
else
    echo "❌ Virtual environment not found. Please run 'uv sync' first."
    exit 1
fi

# Load environment variables from .env file if it exists
if [ -f ".env" ]; then
    echo "   Loading environment variables from .env file..."
    export $(grep -v '^#' .env | xargs)
else
    echo "   No .env file found. Using default database environment variables..."
    # Set default database environment variables
    export DATABASE_HOST=localhost
    export DATABASE_NAME=app-db
    export DATABASE_USER=adminuser
    export DATABASE_PASSWORD=password

    # Set default ChromaDB environment variables
    export CHROMADB_HOST=localhost
    export CHROMADB_PORT=8000
    
    # Set default Dagster home
    export DAGSTER_HOME="$(pwd)/dghome"
fi

# Ensure DAGSTER_HOME is always set (even when .env exists but doesn't have it)
if [ -z "$DAGSTER_HOME" ]; then
    export DAGSTER_HOME="$(pwd)/dghome"
fi

echo "   Dagster Home: $DAGSTER_HOME"

echo "   Database: postgresql://$DATABASE_USER:*****@$DATABASE_HOST:5432/$DATABASE_NAME"

# Check if PostgreSQL is running
if ! docker ps | grep -q postgres-app; then
    echo "❌ PostgreSQL container not running. Starting it..."
    docker start postgres-app 2>/dev/null || {
        echo "❌ PostgreSQL container not found. Please run the setup script first:"
        echo "   cd /Users/levymedina/Documents/code/ec-bootcamp/exam-output"
        echo "   docker run -d --name postgres-app -p 5432:5432 -e POSTGRES_DB=app-db -e POSTGRES_USER=adminuser -e POSTGRES_PASSWORD=password -e PGDATA=/var/lib/postgresql/data/pgdata -v postgres-data:/var/lib/postgresql/data postgres:15-alpine"
        exit 1
    }
    
    echo "⏳ Waiting for PostgreSQL to be ready..."
    sleep 3
fi

# Test database connection
echo "🔍 Testing database connection..."
python3 -c "
from sqlalchemy import create_engine
try:
    engine = create_engine('postgresql://$DATABASE_USER:$DATABASE_PASSWORD@$DATABASE_HOST:5432/$DATABASE_NAME')
    engine.connect().close()
    engine.dispose()
    print('✅ Database connection successful!')
except Exception as e:
    print(f'❌ Database connection failed: {e}')
    exit(1)
"

echo "🎯 Starting Dagster UI..."
echo "   Open http://localhost:3000 in your browser"
echo ""

# Start Dagster
dg dev
