#!/usr/bin/env bash
# All quality gates (plan items P7, P17): backend tests + coverage gate, frontend typecheck + tests + coverage
# gate + production build, lint. Run from the repository root:  tools/check_all.sh
set -euo pipefail
echo "== backend: lint";        ruff check src tests examples tools
echo "== backend: tests";       python -m pytest tests -q -W error --cov=DnA Workflows --cov-report=json:coverage.json
echo "== backend: coverage";    python tools/check_coverage.py
echo "== studio: dependencies"; (cd studio && npm ci --no-audit --no-fund >/dev/null)
echo "== studio: typecheck";    (cd studio && npm run typecheck)
echo "== studio: tests";        (cd studio && npm run coverage)
echo "== studio: build";        (cd studio && npm run build >/dev/null)
echo "ALL GATES PASSED"
