"""LogPilot demo shop: four tiny HTTP services from one file (python app.py <service>).

    api-gateway -> orders -> payments, inventory      (api-gateway -> inventory for browsing)

Each service appends lines like
    2026-11-20 14:02:11 ERROR payments Connection to provider timed out after 5000 ms for order 123456
to /logs/shop/<service>.log (UTC). Normal traffic logs only INFO lines.
Failures are flags in Redis, chaos:<scenario> = injection time, flipped by sources/chaos/chaos.py.
"""
import logging
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import redis

SERVICE = sys.argv[1]
SCENARIOS = ["db-slowdown", "payment-outage", "memory-leak", "service-crash", "faulty-release"]
r = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), password=os.getenv("REDIS_PASSWORD") or None,
                decode_responses=True, socket_timeout=1)

logging.Formatter.converter = time.gmtime
logging.addLevelName(logging.WARNING, "WARN")
logging.addLevelName(logging.CRITICAL, "FATAL")
os.makedirs("/logs/shop", exist_ok=True)
logging.basicConfig(filename=f"/logs/shop/{SERVICE}.log", level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
                    format=f"%(asctime)s %(levelname)s {SERVICE} %(message)s")
log = logging.getLogger()
release = "2.4.3"


def flags():
    try:
        return {s: float(v) for s, v in zip(SCENARIOS, r.mget([f"chaos:{s}" for s in SCENARIOS])) if v}
    except redis.RedisError:
        return {}


def call(service, path):
    """HTTP status of an upstream call; 0 when unreachable."""
    try:
        with urllib.request.urlopen(f"http://{service}:8000{path}", timeout=4) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code
    except OSError:  # refused, DNS failure, timeout
        return 0


def crash(message):
    log.critical(message)
    logging.shutdown()
    os._exit(1)  # Docker's restart policy brings the service back


def gateway(path, q, f):
    if path == "/checkout":
        upstream, t = "orders", time.time()
        status = call("orders", "/orders")
    elif path == "/products":
        upstream, t = "inventory", time.time()
        status = call("inventory", f"/stock?sku={random.randint(1000, 1099)}")
    else:
        return 404
    ms = int((time.time() - t) * 1000)
    if status == 0:
        log.error("Upstream %s unreachable for GET %s", upstream, path)
        return 502
    if status != 200:
        log.error("GET %s failed: upstream %s returned %d", path, upstream, status)
        return 502
    if ms > 1000:
        log.warning("Slow upstream %s: GET %s took %d ms", upstream, path, ms)
    else:
        log.info("GET %s 200 in %d ms", path, ms)
    return 200


def orders(path, q, f):
    if "service-crash" in f:
        crash("Unhandled exception in request worker, shutting down")
    oid = random.randint(100000, 999999)
    if "db-slowdown" in f:
        ms = random.randint(1500, 4000)
        time.sleep(min(ms, 3000) / 1000)
        if ms > 3000:
            log.error("Database query timed out after 3000 ms for order %d", oid)
            return 500
        log.warning("Slow query on orders table took %d ms", ms)
    items = random.randint(1, 4)
    status = call("inventory", f"/reserve?items={items}")
    if status != 200:
        log.error("Stock reservation failed for order %d: inventory returned %d", oid, status)
        return 503
    status = call("payments", f"/charge?order={oid}")
    if status != 200:
        log.error("Payment failed for order %d: payments returned %d", oid, status)
        return 503
    log.info("Order %d placed with %d items", oid, items)
    return 200


def payments(path, q, f):
    if "payment-outage" in f:
        time.sleep(0.2)
        log.error("Connection to provider timed out after 5000 ms for order %s", q.get("order"))
        return 503
    log.info("Charged %.2f EUR for order %s", random.uniform(5, 200), q.get("order"))
    return 200


def inventory(path, q, f):
    global release
    if "memory-leak" in f:
        heap = 60 + (time.time() - f["memory-leak"]) / 3  # +20 points per minute
        if heap >= 100:
            r.set("chaos:memory-leak", time.time(), xx=True)  # restart with a fresh heap, then leak again
            crash("MemoryError: heap exhausted at 512 MB, process exiting")
        if heap >= 80:
            log.warning("Heap usage at %d%% of 512 MB", heap)
    new = "2.5.0" if "faulty-release" in f else "2.4.3"
    if new != release:
        log.info("Deployed version %s (was %s)", new, release)
        release = new
    if path == "/reserve":
        if release == "2.5.0" and random.random() < 0.5:
            log.error("NullPointerException in StockService.reserve (StockService.java:142)")
            return 500
        log.info("Reserved %s units", q.get("items"))
    else:
        log.info("Stock lookup for SKU-%s: %d left", q.get("sku"), random.randint(0, 500))
    return 200


ROUTES = {"api-gateway": gateway, "orders": orders, "payments": payments, "inventory": inventory}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path, _, query = self.path.partition("?")
        status = ROUTES[SERVICE](path, dict(urllib.parse.parse_qsl(query)), flags())
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):  # silence http.server's access log on stderr
        pass


if __name__ == "__main__":
    log.info("Service %s %s started on port 8000", SERVICE, release)
    ThreadingHTTPServer(("", 8000), Handler).serve_forever()
