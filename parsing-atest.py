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
            v1organizations list<text>,
            v2organizations text,
            tone map<text, double>,
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

    # Ordre des tâches
    connexion_task >> create_task
