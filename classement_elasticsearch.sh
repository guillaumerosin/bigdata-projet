#!/usr/bin/env bash
# Classement des sources par nombre de news — Elasticsearch (bash, curl + jq ou grep)
# À lancer dans l'environnement Docker Elasticsearch / Kibana.

set -e

ES_HOST="${ES_HOST:-https://172.20.0.201:9200}"
ES_USER="${ES_USER:-user_kawasaki}"
ES_PASSWORD="${ES_PASSWORD:-wTwF0UQRqL4it4j}"
ES_INDEX="${ES_INDEX:-gdelt-gkg}"
ES_FIELD="${ES_SOURCE_FIELD:-source}"
TOP_N="${1:-20}"
[[ "$TOP_N" =~ ^[0-9]+$ ]] || TOP_N=20

START=$(date +%s.%N)
JSON=$(cat <<EOF
{"size":0,"aggs":{"sources":{"terms":{"field":"${ES_FIELD}.keyword","size":$TOP_N,"order":{"_count":"desc"}}}}
EOF
)

RESP=$(curl -s -k -u "${ES_USER}:${ES_PASSWORD}" -X POST "${ES_HOST}/${ES_INDEX}/_search" \
  -H "Content-Type: application/json" -d "$JSON" 2>/dev/null) || true

if [[ -z "$RESP" ]]; then
  echo "Erreur: curl vers Elasticsearch a échoué" >&2
  END=$(date +%s.%N)
  echo "TEMPS: $(echo "$END - $START" | bc 2>/dev/null || echo "?")"
  exit 1
fi

# Sans .keyword si aucun bucket
if echo "$RESP" | grep -q '"buckets":\[\]'; then
  JSON=$(cat <<EOF
{"size":0,"aggs":{"sources":{"terms":{"field":"${ES_FIELD}","size":$TOP_N,"order":{"_count":"desc"}}}}
EOF
)
  RESP=$(curl -s -k -u "${ES_USER}:${ES_PASSWORD}" -X POST "${ES_HOST}/${ES_INDEX}/_search" \
    -H "Content-Type: application/json" -d "$JSON" 2>/dev/null)
fi

END=$(date +%s.%N)
ELAPSED=$(echo "$END - $START" | bc 2>/dev/null || echo "?")

echo "Elasticsearch — classement des sources par nombre de news"
echo "============================================================"

if command -v jq &>/dev/null; then
  i=1
  while IFS=$'\t' read -r count key; do
    [[ -z "$key" ]] && continue
    key_short="${key:0:50}"
    [[ ${#key} -gt 50 ]] && key_short="${key_short}…"
    printf "  %3d. %6s  %s\n" "$i" "$count" "$key_short"
    i=$((i+1))
  done < <(echo "$RESP" | jq -r '.aggregations.sources.buckets[]? | "\(.doc_count)\t\(.key)"' 2>/dev/null)
else
  i=1
  while IFS=$'\t' read -r count key; do
    [[ -z "$key" ]] && continue
    key_short="${key:0:50}"
    [[ ${#key} -gt 50 ]] && key_short="${key_short}…"
    printf "  %3d. %6s  %s\n" "$i" "$count" "$key_short"
    i=$((i+1))
  done < <(paste <(echo "$RESP" | grep -oP '"doc_count"\s*:\s*\K[0-9]+') <(echo "$RESP" | grep -oP '"key"\s*:\s*"\K[^"]*') 2>/dev/null)
fi

echo ""
echo "TEMPS: $ELAPSED"
