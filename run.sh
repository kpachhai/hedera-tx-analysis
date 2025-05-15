#!/bin/bash
set -e

FILENAME=$1
RESET=$2

# Load environment variables
if [ -f .env ]; then
  export $(cat .env | xargs)
fi

# Default to localhost Mongo if not set
MONGO_URI="${MONGO_URI:-mongodb://localhost:27017}"

echo "Using Mongo URI: $MONGO_URI"

IS_LOCAL=false

# Start MongoDB locally only if using localhost URI
if [[ "$MONGO_URI" == "mongodb://localhost:27017" ]]; then
  IS_LOCAL=true
  echo "Starting MongoDB via Docker..."
  docker-compose up -d

  echo "Waiting for MongoDB to initialize..."
  sleep 5
fi

echo "Installing dependencies..."
npm install

if [ "$RESET" == "--reset" ]; then
  echo "Resetting MongoDB database..."
  docker exec hedera-mongo mongosh hedera --eval "db.dropDatabase()" > /dev/null 2>&1 || echo "⚠️ Reset failed (likely not a local instance)"
  echo "MongoDB database dropped (if applicable)."
fi

if [ -n "$FILENAME" ]; then
  echo "Importing transactions from $FILENAME"
  node import.js "$FILENAME"
else
  echo "No input file provided. Skipping import."
fi

echo "Running analysis..."
node analyze.js

# Shut down Docker container if we started it
if [ "$IS_LOCAL" = true ]; then
  echo "Shutting down MongoDB Docker container..."
  docker-compose down
fi

echo "All done!"
