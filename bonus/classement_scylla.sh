#!/usr/bin/env bash
# Classement des sources par nombre de news — Scylla DB (bash, cqlsh)
# À lancer dans l'environnement Docker Scylla (cqlsh doit être disponible).

set -e

SCYLLA_HOST="${SCYLLA_HOST:-172.20.0.171}"
SCYLLA_PORT="${SCYLLA_PORT:-9042}"
SCYLLA_USER="${SCYLLA_USER:-user_kawasaki}"
SCYLLA_PASS="${SCYLLA_PASS:-wTwF0UQRqL4it4j}"
SCYLLA_KEYSPACE="${SCYLLA_KEYSPACE:-gdelt}"
SCYLLA_TABLE="${SCYLLA_TABLE:-articles}"
TOP_N="${1:-20}"
[[ "$TOP_N" =~ ^[0-9]+$ ]] || TOP_N=20

if ! command -v cqlsh &>/dev/null; then
  echo "Erreur: cqlsh non trouvé (installez cqlsh ou utilisez le script Python)" >&2
  echo "TEMPS: -1.000"
  exit 1
fi

export CQLSH_PASSWORD="$SCYLLA_PASS"
START=$(date +%s.%N)
RAW=$(cqlsh "$SCYLLA_HOST" -p "$SCYLLA_PORT" -u "$SCYLLA_USER" -e "SELECT source FROM ${SCYLLA_KEYSPACE}.${SCYLLA_TABLE};" 2>/dev/null) || true
END=$(date +%s.%N)
ELAPSED=$(echo "$END - $START" | bc 2>/dev/null || echo "?")

if [[ -z "$RAW" ]]; then
  echo "Erreur: cqlsh a échoué (vérifiez host/user/pass)" >&2
  echo "TEMPS: $ELAPSED"
  exit 1
fi

echo "Scylla DB (CQL) — classement des sources par nombre de news"
echo "============================================================"
n=1
while read -r count source; do
  source_short="${source:0:50}"
  [[ ${#source} -gt 50 ]] && source_short="${source_short}…"
  printf "  %3d. %6s  %s\n" "$n" "$count" "$source_short"
  n=$((n+1))
done < <(echo "$RAW" | awk -F'|' 'NR>3 && NF>=1 && $1!~/^-/ && $1!="" { gsub(/^[ \t]+|[ \t]+$/,"",$1); if($1!="") print $1 }' | sort | uniq -c | sort -rn | head -n "$TOP_N")

echo ""
echo "TEMPS: $ELAPSED"
