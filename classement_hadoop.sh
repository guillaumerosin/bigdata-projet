#!/usr/bin/env bash
# Classement des sources par nombre de news — MapReduce Hadoop / YARN (bash)
# Utilise mapreduce/mapper.sh et mapreduce/reducer.sh (awk, pas de Python).
# À lancer dans le conteneur namenode.

set -e

SCRIPT_DIR="${SCRIPT_DIR:-$(cd "$(dirname "$0")" && pwd)}"
HADOOP_INPUT="${HADOOP_INPUT:-/user/gdelt/articles}"
HADOOP_OUTPUT="${HADOOP_OUTPUT:-/user/gdelt/classement_sources}"
TOP_N="${1:-20}"
[[ "$TOP_N" =~ ^[0-9]+$ ]] || TOP_N=20

MAPPER="${SCRIPT_DIR}/mapreduce/mapper.sh"
REDUCER="${SCRIPT_DIR}/mapreduce/reducer.sh"

if [[ ! -f "$MAPPER" ]] || [[ ! -f "$REDUCER" ]]; then
  echo "Erreur: mapper.sh ou reducer.sh manquant dans mapreduce/" >&2
  echo "TEMPS: -1.000"
  exit 1
fi

JAR="${HADOOP_STREAMING_JAR:-}"
if [[ -z "$JAR" ]]; then
  for p in /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
           /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming*.jar \
           /usr/lib/hadoop-mapreduce/hadoop-streaming*.jar \
           /usr/lib/hadoop/tools/lib/hadoop-streaming*.jar; do
    if [[ -f $p ]]; then
      JAR="$p"
      break
    fi
  done
fi
[[ -z "$JAR" ]] && JAR=$(find /opt /usr -name "*streaming*.jar" -path "*/tools/lib/*" 2>/dev/null | head -1)
if [[ -z "$JAR" ]] || [[ ! -f "$JAR" ]]; then
  echo "Erreur: hadoop-streaming.jar non trouvé. Définir HADOOP_STREAMING_JAR ou lancer:" >&2
  echo "  find / -name '*streaming*.jar' 2>/dev/null" >&2
  echo "TEMPS: -1.000"
  exit 1
fi

OUT="${HADOOP_OUTPUT}_$(date +%s)"
START=$(date +%s.%N)

if ! hadoop jar "$JAR" \
  -input "$HADOOP_INPUT" \
  -output "$OUT" \
  -mapper "sh mapper.sh" \
  -reducer "sh reducer.sh" \
  -file "$MAPPER" \
  -file "$REDUCER"; then
  END=$(date +%s.%N)
  echo "Erreur: le job MapReduce a échoué" >&2
  echo "TEMPS: $(echo "$END - $START" | bc 2>/dev/null || echo "?")"
  exit 1
fi

END=$(date +%s.%N)
ELAPSED=$(echo "$END - $START" | bc 2>/dev/null || echo "?")

if ! hadoop fs -cat "${OUT}/part-"* 2>/dev/null >/dev/null; then
  echo "Erreur: impossible de lire la sortie HDFS ${OUT}/part-*" >&2
  echo "TEMPS: $ELAPSED"
  exit 1
fi

echo "Hadoop MapReduce — classement des sources par nombre de news"
echo "============================================================"
n=1
while IFS=$'\t' read -r source count; do
  source_short="${source:0:50}"
  [[ ${#source} -gt 50 ]] && source_short="${source_short}…"
  printf "  %3d. %6s  %s\n" "$n" "$count" "$source_short"
  n=$((n+1))
done < <(hadoop fs -cat "${OUT}/part-"* 2>/dev/null | sort -t$'\t' -k2 -nr | head -n "$TOP_N")

echo ""
echo "TEMPS: $ELAPSED"
