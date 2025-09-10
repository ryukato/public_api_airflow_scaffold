#!/bin/bash
source .venv/bin/activate
echo "[+] Starting RAG Embedding Server..."

# Exporting .env manually if needed (optional)
export $(grep -v '^#' .env | xargs)

# Run server in background
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1 --limit-concurrency 100 --reload --log-level info

