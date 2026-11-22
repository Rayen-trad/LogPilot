#!/bin/sh
# Imports LogPilot data view + dashboards into Kibana (overwrite = idempotent).
# Runs as the one-shot `kibana-init` container. To re-export after editing in the UI:
#   Stack Management > Saved Objects > select > Export, and replace dashboards.ndjson.
set -eu
KIBANA="${KIBANA_URL:-http://kibana:5601}"
curl -sS -f -X POST "$KIBANA/api/saved_objects/_import?overwrite=true" \
  -H 'kbn-xsrf: true' -F "file=@$(dirname "$0")/dashboards.ndjson"
echo
