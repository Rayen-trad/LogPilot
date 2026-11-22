#!/bin/sh
# Idempotently installs the logs ILM policy, ingest pipeline and index template.
# Runs as the one-shot `es-init` container; safe to re-run.
set -eu
ES="${ES_URL:-http://elasticsearch:9200}"
DIR="$(dirname "$0")"

put() {
  echo "PUT $1"
  curl -sS -f -X PUT "$ES/$1" -H 'Content-Type: application/json' --data-binary "@$DIR/$2"
  echo
}

put _ilm/policy/logs-7d            logs-ilm.json
put _ingest/pipeline/logs-ingested logs-ingested-pipeline.json
put _index_template/logpilot-logs  logs-template.json
put _ilm/policy/metrics-30d         metrics-ilm.json
put _index_template/logpilot-metrics metrics-template.json
echo "Elasticsearch setup done."
