import os
import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy

log = logging.getLogger(__name__)

# --- Mapping GCAM : PAS de MySQL, on garde en dict (ou fichier JSON versionné)
GCAM_MAPPING = {
    'wc': 'WordCount',
    'c1.1': 'Positive',
    'c1.2': 'Negative',
    'c2.1': 'Strong',
    'c2.2': 'Weak',
    'c2.3': 'Submissive',
    'c2.4': 'Hostile',
    'c2.5': 'Active',
    'c2.6': 'Passive',
    'c2.7': 'Sure',
    'c2.8': 'Undet',
    'c12.1': 'Anxiety',
    'c12.2': 'Anger',
    'c12.3': 'Sadness',
    'c12.4': 'Joy',
    'c12.5': 'Fear',
    'c12.6': 'Disgust',
    'c12.7': 'Surprise',
    'c12.8': 'Trust',
    'c12.9': 'Anticipation',
    'c12.10': 'Polarity',
    'c12.11': 'Positivity',
    'c12.12': 'Negativity',
    'c10.1': 'BodyParts',
    'c10.2': 'Health',
    'c10.3': 'Sexual',
    'c10.4': 'Ingestion'
}

# ceci me permet de convertir le code numérique de la source en texte
def parse_source_type(source_type: str | None):
    if not source_type:
        return None
    return {
        '1': "WEB",
        '2': "CITATIONONLY",
        '3': "CORE",
        '4': "DTIC",
        '5': "JSTOR",
        '6': "NONTEXTUALSOURCE",
    }.get(source_type)


def parse_v1_theme(v1_theme: str | None):
    if not v1_theme:
        return []
    # souvent finit par ; dans certains exports
    v = v1_theme.rstrip(";")
    return [t for t in v.split(";") if t]

def parse_v2_theme(v2_theme: str | None):
    if not v2_theme:
        return {}
    out = {}
    for theme in v2_theme.split(";"):
        if not theme:
            continue
        if ":" not in theme:
            continue
        k, v = theme.split(":", 1)
        k = k.strip()
        v = v.strip()
        try:
            out[k] = int(v)
        except ValueError:
            continue
    return out

def parse_tone_dict(tone_str: str | None):
    if not tone_str:
        return None
    parts = tone_str.split(",")
    if len(parts) < 7:
        return None
    try:
        return {
            "Tone": float(parts[0]),
            "PositiveScore": float(parts[1]),
            "NegativeScore": float(parts[2]),
            "Polarity": float(parts[3]),
            "Activity_reference_density": float(parts[4]),
            "Self_reference_density": float(parts[5]),
            "WordCount": float(parts[6]),
        }
    except ValueError:
        return None

def enrich_gcam_text(codes_string: str | None):
    if not codes_string:
        return {}
    parsed = []
    for code in codes_string.split(","):
        if ":" not in code:
            continue
        k, v = code.split(":", 1)
        k = k.strip()
        v = v.strip()
        try:
            parsed.append((k, float(v)))
        except ValueError:
            continue
    top_20 = sorted(parsed, key=lambda x: x[1], reverse=True)[:20]
    return {GCAM_MAPPING.get(k, k): v for k, v in top_20}

def parse_videos(str_videos: str | None):
    if not str_videos:
        return []
    v = str_videos.rstrip(";")
    return [x for x in v.split(";") if x]

def get_scylla_session():
    # ⚠️ évite les creds en dur : mets-les en Variables Airflow ou env
    username = os.getenv("SCYLLA_USER", "user_kawasaki")
    password = os.getenv("SCYLLA_PASS", "WtWF0UQRqL4it4j")
    keyspace = os.getenv("SCYLLA_KEYSPACE", "my_keyspace")

    auth = PlainTextAuthProvider(username=username, password=password)
    cluster = Cluster(
        contact_points=['172.20.0.171', '172.20.0.172', '172.20.0.173'],
        load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
        auth_provider=auth
    )
    session = cluster.connect(keyspace)
    return cluster, session

def consume_kafka_to_scylla(max_messages: int = 10000):
    # Kafka settings
    topic = os.getenv("KAFKA_TOPIC", "test")
    brokers = os.getenv("KAFKA_BROKERS", "172.20.0.51:9092,172.20.0.52:9092").split(",")
    group_id = os.getenv("KAFKA_GROUP_ID_SCYLLA", "airflow-scylla")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=brokers,
        auto_offset_reset="earliest",     # OK: uniquement pour le tout premier run du groupe
        group_id=group_id,               
        enable_auto_commit=False,         
        consumer_timeout_ms=5000,
    )

    cluster, session = get_scylla_session()

    insert_q = session.prepare("""
        INSERT INTO articlefulltexttest (
            id, article_date, source_type, source_common_name, source_id,
            v1_themes, v2themes, v2locations, persons, organizations,
            tone, dates_in_text, gcam, sharing_image, videos, numerical_values
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    count = 0
    try:
        for message in consumer:
            raw = message.value.decode("utf-8", errors="replace")
            parts = raw.split("\t")

            # ⚠️ sécurité: vérifier qu’on a assez de colonnes
            if len(parts) < 25:
                log.warning("Message trop court (colonnes=%s). Ignoré.", len(parts))
                continue

            msg = {
                "id": parts[0],
                "article_date": parts[1],
                "source_type": parse_source_type(parts[2]),
                "source_common_name": parts[3],
                "source_id": parts[4],
                "v1_themes": parse_v1_theme(parts[7]),
                "v2_themes": parse_v2_theme(parts[8]),
                "v2Locations": parts[10],
                "persons": [p for p in parts[11].split(";") if p],
                "organizations": [o for o in parts[13].split(";") if o],
                "tone": parse_tone_dict(parts[15]) or {},
                "dates_in_text": parts[16],
                "gcam": enrich_gcam_text(parts[17]),
                "sharing_image": parts[18],
                "videos": parse_videos(parts[21]),
                "numerical_values": parts[24],
            }

            # INSERT
            session.execute(insert_q, (
                msg["id"],
                msg["article_date"],
                msg["source_type"],
                msg["source_common_name"],
                msg["source_id"],
                msg["v1_themes"],
                msg["v2_themes"],
                msg["v2Locations"],
                msg["persons"],
                msg["organizations"],
                msg["tone"],
                msg["dates_in_text"],
                msg["gcam"],
                msg["sharing_image"],
                msg["videos"],
                msg["numerical_values"],
            ))

            # Commit Kafka offset après insertion OK
            consumer.commit()

            count += 1
            if count >= max_messages:
                break

        log.info("Scylla ingest terminé. Messages traités: %s", count)

    finally:
        consumer.close()
        session.shutdown()
        cluster.shutdown()

# --- DAG Airflow
dag = DAG(
    dag_id="a_ArticleScylla",
    description="Consommer Kafka et insérer dans ScyllaDB (sans MySQL)",
    schedule_interval=None,
    start_date=datetime.datetime(year=2026, month=2, day=15),
    catchup=False,
)

python_task = PythonOperator(
    task_id="TaskPutToScylla",
    python_callable=consume_kafka_to_scylla,
    dag=dag,
)
