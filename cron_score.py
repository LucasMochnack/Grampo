"""
Daily cron job: scores all Grampo conversations from the last 30 days.
Runs as a Railway cron service — no web server, just a one-shot script.
"""
import os
import sys
import httpx
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("cron_score")

GRAMPO_URL = os.environ.get("GRAMPO_URL", "https://grampo-production.up.railway.app")
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")
CANAL = os.environ.get("GRAMPO_CANAL", "5519997733651")
DAYS  = int(os.environ.get("GRAMPO_DAYS", "30"))

if not DASHBOARD_PASSWORD:
    log.error("DASHBOARD_PASSWORD env var not set — aborting")
    sys.exit(1)

url = f"{GRAMPO_URL}/dashboard/cron/score-daily?canal={CANAL}&days={DAYS}"
log.info("Calling %s", url)

try:
    r = httpx.post(
        url,
        headers={"Authorization": f"Bearer {DASHBOARD_PASSWORD}"},
        timeout=1800,  # 30 min — enough for ~600 conversations
    )
    r.raise_for_status()
    data = r.json()
    log.info(
        "Done: scored=%s skipped=%s errors=%s",
        data.get("scored", "?"),
        data.get("skipped_cached", "?"),
        data.get("errors", "?"),
    )
    sys.exit(0)
except Exception as exc:
    log.error("Cron failed: %s", exc)
    sys.exit(1)
