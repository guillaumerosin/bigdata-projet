#!/usr/bin/env python3
# coding by guillaume rosin
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# CONNEXION A ScyllaDB
SCYLLA_NODES = ['172.20.0.171', '172.20.0.172', '172.20.0.173']
SCYLLA_USER = 'user_kawasaki'
SCYLLA_PASS = 'wTwF0UQRqL4it4j'
SCYLLA_KEYSPACE = 'keyspace_pour_les_nuls'
LOCAL_DC = "datacenter1"
PROTOCOL_VERSION = 4

KAFKA_TOPIC = "test"
KAFKA_BOOTSTRAP = ["172.20.0.51:9092","172.20.0.52:9092"]

MIN_COLUMNS = 27

def _get_cluster():
    """Crée et retourne un objet Cluster Cassandra/ScyllaDB."""
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

    auth = PlainTextAuthProvider(username=SCYLLA_USER, password=SCYLLA_PASS)
    return Cluster(
        contact_points=SCYLLA_NODES,
        port=9042,
        auth_provider=auth,
        protocol_version=PROTOCOL_VERSION,
        load_balancing_policy=TokenAwarePolicy(
            DCAwareRoundRobinPolicy(local_dc=LOCAL_DC)
        ),
    )
# Retourne parts[index] si présent, sinon None => NULL en base
def safe_get(parts, index):
    return parts[index] if index < len(parts) else None

def connexion_etablie():
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

    cluster = None
    session = None

    try:
        auth = PlainTextAuthProvider(username=SCYLLA_USER,password=SCYLLA_PASS)

        cluster = Cluster(
            contact_points=SCYLLA_NODES,
            port=9042,
            auth_provider=auth,
            protocol_version=PROTOCOL_VERSION,
            load_balancing_policy=TokenAwarePolicy(
                DCAwareRoundRobinPolicy(local_dc=LOCAL_DC)
            ),
        )

        session = cluster.connect(SCYLLA_KEYSPACE)
        row = session.execute(
            "SELECT release_version FROM system.local"
        ).one()

        log.info(
            "Connexion Scylla OK. release_version=%s",
            getattr(row, "release_version", None)
        )

    except Exception as e:
        log.exception("Erreur de connexion à ScyllaDB")
        raise

    finally:
        if session is not None:
            session.shutdown()
        if cluster is not None:
            cluster.shutdown()


def create_db():
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

    cluster = None
    session = None
    try:
        auth = PlainTextAuthProvider(username=SCYLLA_USER, password=SCYLLA_PASS)
        cluster = Cluster(
            contact_points=SCYLLA_NODES,
            port=9042,
            auth_provider=auth,
            protocol_version=PROTOCOL_VERSION,
            load_balancing_policy=TokenAwarePolicy(
                DCAwareRoundRobinPolicy(local_dc=LOCAL_DC)
            ),
        )

        session = cluster.connect()


        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS gdelt
            WITH replication = {
                 'class': 'NetworkTopologyStrategy',
                 'replication_factor': 3
            };
        """)  
    

        session.set_keyspace("gdelt")

        session.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id text PRIMARY KEY,
            date timestamp,
            source_type text,
            v1themes text,
            v2themes text,
            v1locations text,
            v2locations text,
            v1persons text,
            v2persons text,
            v1organizations text,
            v2organizations text,
            v1.5tone text,
            image text,
            videos text,
            quotations text,
            allnames text,
            extraxml text
            );
        """)

        log.info("Table créée")
    except Exception as e:
        log.exception("erreur lors de la création de la db")
        raise
    finally:
        if session is not None:
            session.shutdown()
        if cluster is not None:
            cluster.shutdown()



def read_kafka_for_scylla():
    import json
    import uuid
    from kafka import KafkaConsumer

    consumer = KafkaConsumer (
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
        # value_deserializer=lambda x: json.loads(x.decode("utf-8")),  # c'est pas du json debilous
        consumer_timeout_ms=10000, 
    )
    messages = []
    for msg in consumer:
        raw = msg.value.decode("utf-8", errors="replace")
        # log.info("Message brut Kafka: %s", raw)  # debug: décommenter pour voir chaque message brut
        # log.info("Message brut Kafka (200 premiers car.): %s", raw[:200])  # debug: version courte
        messages.append(raw)

    consumer.close()
    log.info("%d messages lus depuis Kafka", len(messages))
    return messages

def to_list_or_none_WOW(s):
    """Convertit une chaîne 'a;b;c' en liste ['a','b','c'], ou None si vide."""
    if s is None or not s.strip():
        return None
    return [x for x in s.split(";") if x]


def my_process_data(raw: str) -> dict | None:
    parts = raw.split("\t") #je parse ma data

    # log.info("Nombre de colonnes: %d", len(parts))  # debug
    # log.info("parts[0] (id)=%s, parts[1] (date)=%s", safe_get(parts, 0), safe_get(parts, 1))  # debug
    #if len(parts) < MIN_COLUMNS:  #N étant le nombre minimal que je veux utiliser
        #log.warning("Ligne trop courte (%d colonnes), ignorée: %s",len(parts),raw[:80])
        #return None
    
    # Ligne sans identifiant (je sais pas si ca sert c'est un sureté)
    if not parts[0].strip():
        log.warning("Ligne sans id, ignorée : %s", raw[:20])
        return None 

    msg = {
        "id":               safe_get(parts, 0),
        "date":             safe_get(parts, 1),
        "source_type":      safe_get(parts, 2),
        "v1themes":         safe_get(parts, 3),
        "v2themes":         safe_get(parts, 4),
        "v1locations":      safe_get(parts, 5),
        "v2locations":      safe_get(parts, 6),
        "v1persons":        safe_get(parts, 7),
        "v2persons":        safe_get(parts, 8),
        "v1organizations":  to_list_or_none_WOW(safe_get(parts, 9)),
        "v2organizations":  safe_get(parts, 10),
        "v1.5tone":         safe_get(parts, 11),
        "image":            safe_get(parts, 12),
        "videos":           safe_get(parts, 13),
        "quotations":       safe_get(parts, 14),
        "allnames":         safe_get(parts, 15),
        "extraxml":         safe_get(parts, 16),
    }
    return msg


def insertion_scylla(session, msg: dict) -> None:
    """Insère un article dans la table gdelt.articles."""
    session.execute(
        """
        INSERT INTO gdelt.articles (
            id, date, source_type,
            v1themes, v2themes,
            v1locations, v2locations,
            v1persons, v2persons,
            v1organizations, v2organizations,
            v1.5tone, image, videos,
            quotations, allnames, extraxml
        ) VALUES (
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s
        )
        """,
        (
            msg["id"],
            msg["date"],
            msg["source_type"],
            msg["v1themes"],
            msg["v2themes"],
            msg["v1locations"],
            msg["v2locations"],
            msg["v1persons"],
            msg["v2persons"],
            msg["v1organizations"],
            msg["v2organizations"],
            msg["v1.5tone"],
            msg["image"],
            msg["videos"],
            msg["quotations"],
            msg["allnames"],
            msg["extraxml"],
        ),
    )


def task_parse_messages(**context):
    """Tâche Airflow : récupère les messages Kafka via XCom, parse chaque ligne, renvoie la liste des dicts."""
    ti = context["ti"]
    messages = ti.xcom_pull(task_ids="read_kafka_for_scylla")
    if not messages:
        log.info("Aucun message à parser (XCom vide).")
        return []
    result = []
    for raw in messages:
        parsed = my_process_data(raw)
        if parsed is not None:
            result.append(parsed)
    log.info("Parsing terminé : %d messages valides sur %d.", len(result), len(messages))
    # if result:
    #     log.info("Exemple 1er message parsé: %s", result[0])  # debug
    return result


def task_insert_to_scylla(**context):
    """Tâche Airflow : récupère la liste des dicts parsés via XCom, se connecte à Scylla, insère chaque article."""
    ti = context["ti"]
    parsed_list = ti.xcom_pull(task_ids="my_process_data")
    if not parsed_list:
        log.info("Aucun message à insérer.")
        return
    cluster = _get_cluster()
    session = cluster.connect("gdelt")
    try:
        for msg in parsed_list:
            insertion_scylla(session, msg)
        log.info("Ingest terminé : %d articles insérés.", len(parsed_list))
    finally:
        session.shutdown()
        cluster.shutdown()


with DAG(
    dag_id="a1_scylladb_main_parsing",
    schedule=None,
    start_date=datetime(2026, 2, 27),
    catchup=False,
) as dag:

    connexion_task = PythonOperator(
        task_id="connexion_etablie",
        python_callable=connexion_etablie,
    )

    create_task = PythonOperator(
        task_id="create_table",
        python_callable=create_db,
    )

    read_kafka_task = PythonOperator(
        task_id="read_kafka_for_scylla",
        python_callable=read_kafka_for_scylla,
    )

    parse_task = PythonOperator(
        task_id="my_process_data",
        python_callable=task_parse_messages,
    )

    insert_task = PythonOperator(
        task_id="insertion_scylla",
        python_callable=task_insert_to_scylla,
    )
    # Ordre des tâches (graphe inchangé : 5 nœuds)
    connexion_task >> create_task >> read_kafka_task >> parse_task >> insert_task
