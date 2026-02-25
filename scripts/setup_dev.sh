#!/usr/bin/env bash
# Sets up the development environment from scratch.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "==> Checking Python version..."
python3 --version | grep -E "3\.(9|10|11|12)" \
  || { echo "ERROR: Python 3.9+ required"; exit 1; }

echo "==> Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate

echo "==> Installing dependencies..."
pip3 install --upgrade pip
pip3 install -r requirements-dev.txt

echo "==> Installing pre-commit hooks..."
pre-commit install

echo "==> Starting infrastructure (Neo4j + Redis)..."
docker compose up -d

echo "==> Waiting for Neo4j to be ready..."
for i in {1..20}; do
  if curl -s http://localhost:7474 > /dev/null 2>&1; then
    echo "    Neo4j is up."
    break
  fi
  echo "    Waiting... ($i/20)"
  sleep 3
done

echo "==> Waiting for Redis to be ready..."
for i in {1..10}; do
  if docker compose exec redis redis-cli ping 2>/dev/null | grep -q PONG; then
    echo "    Redis is up."
    break
  fi
  echo "    Waiting... ($i/10)"
  sleep 2
done

echo ""
echo "Development environment ready!"
echo "  Activate venv:  source venv/bin/activate"
echo "  Run tests:      python3 -m pytest tests/"
echo "  Start API:      uvicorn app.main:app --reload"
echo "  Neo4j browser:  http://localhost:7474"
