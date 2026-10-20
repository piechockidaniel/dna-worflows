"""Coverage gate (plan item P7.2).

Usage:  python -m pytest --cov=DnA Workflows --cov-report=json:coverage.json && python tools/check_coverage.py

* total line+branch coverage must be  > TOTAL_MIN
* every module matching a CRITICAL prefix must reach CRITICAL_MIN
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

TOTAL_MIN = 70.0
CRITICAL_MIN = 85.0
CRITICAL = (
    "DnA Workflows/kernel/",
    "DnA Workflows/runtime/engine.py", "DnA Workflows/runtime/lifecycle.py", "DnA Workflows/runtime/outbox.py",
    "DnA Workflows/runtime/router.py", "DnA Workflows/runtime/registry.py", "DnA Workflows/runtime/bindings.py",
    "DnA Workflows/persistence/",
    "DnA Workflows/resilience/",
    "DnA Workflows/messaging/core.py", "DnA Workflows/messaging/bus.py", "DnA Workflows/messaging/consumer.py",
    "DnA Workflows/tracing.py",
    # iteration 2 (P17.1)
    "DnA Workflows/forecasting/", "DnA Workflows/security/", "DnA Workflows/authoring/", "DnA Workflows/runtime/subflows.py",
    "DnA Workflows/mcpserver/", "DnA Workflows/operations.py",
)


def main(path: str = "coverage.json") -> int:
    data = json.loads(Path(path).read_text())
    total = data["totals"]["percent_covered"]
    failures = []
    rows = []
    for file, info in sorted(data["files"].items()):
        rel = file[file.index("DnA Workflows/"):] if "DnA Workflows/" in file else file
        pct = info["summary"]["percent_covered"]
        critical = any(rel.startswith(p) for p in CRITICAL)
        rows.append((rel, pct, critical))
        if critical and pct < CRITICAL_MIN:
            failures.append(f"critical module {rel}: {pct:.1f}% < {CRITICAL_MIN}%")
    width = max(len(r[0]) for r in rows)
    for rel, pct, critical in rows:
        flag = "CRIT" if critical else "    "
        print(f"{flag} {rel:<{width}} {pct:6.1f}%")
    print(f"\nTOTAL {total:.1f}% (required > {TOTAL_MIN}%)")
    if total <= TOTAL_MIN:
        failures.append(f"total coverage {total:.1f}% <= {TOTAL_MIN}%")
    if failures:
        print("\nCOVERAGE GATE FAILED:\n  " + "\n  ".join(failures))
        return 1
    print("COVERAGE GATE PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
