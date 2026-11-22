"""Inject and clear shop failures by flipping flags in Redis (read by sources/shop/app.py).

    docker compose exec api-gateway python /app/chaos/chaos.py payment-outage
    docker compose exec api-gateway python /app/chaos/chaos.py clear
    docker compose exec api-gateway python /app/chaos/chaos.py status

Every inject/clear is appended to /results/chaos_log.csv with its exact UTC time;
detection delay = incident opened_at - injected_at.
"""
import csv
import os
import sys
import time
from datetime import datetime, timezone

import redis

SCENARIOS = {  # scenario -> service that breaks first
    "db-slowdown": "orders",
    "payment-outage": "payments",
    "memory-leak": "inventory",
    "service-crash": "orders",
    "faulty-release": "inventory",
}
LOG = os.path.join(os.getenv("RESULTS_DIR", "/results"), "chaos_log.csv")


def record(scenario, action, at):
    new = not os.path.exists(LOG)
    with open(LOG, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["scenario", "service", "action", "at"])
        w.writerow([scenario, SCENARIOS[scenario], action, at])
    print(f"{at} {action} {scenario} ({SCENARIOS[scenario]})")


def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd not in (*SCENARIOS, "clear", "status"):
        sys.exit(f"usage: chaos.py {{{'|'.join(SCENARIOS)}|clear|status}}")
    r = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), password=os.getenv("REDIS_PASSWORD") or None,
                    decode_responses=True)
    now = time.time()
    at = datetime.fromtimestamp(now, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    if cmd == "status":
        for s in SCENARIOS:
            print(f"{s:<16}{'ACTIVE' if r.exists(f'chaos:{s}') else '-'}")
    elif cmd == "clear":
        for s in SCENARIOS:
            if r.delete(f"chaos:{s}"):
                record(s, "cleared", at)
    else:
        r.set(f"chaos:{cmd}", now)
        record(cmd, "injected", at)


if __name__ == "__main__":
    main()
