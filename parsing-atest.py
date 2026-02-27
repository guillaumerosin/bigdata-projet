#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

log = logging.getLogger(__name__)

# --- CONNEXION AScyllaDB ---
SCYLLA_NODES = ['172.20.0.171', '172.20.0.172', '172.20.0.173']
SCYLLA_USER = 'user_kawasaki'
SCYLLA_PASS = 'wTwF0UQRqL4it4j'
SCYLLA_KEYSPACE = 'keyspace_pour_les_nuls'
LOCAL_DC = "datacenter1" # nom du datacenter local
PROTOCOL_VERSION = 4 # pour éviter les warnings de downgrade

def connexion_etablie():
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
                DCAwareRoundRobinPolicy(local_dc=LOCAL_DC)), 
        )       
               
        session = cluster.connect(SCYLLA_KEYSPACE)
        row = session.execute("SELECT release_version FROM System.local").one()
        log.info("Connexion Scylla OK. release_version=%s", getattr(row, "release_version", None))

        # session.execute("""CREATE KEYSPACE IF NOT EXISTS lebestkeyspace""")
    
    except Exception as e:
        log.exception("erreur de connexion à Scylladb ma biche")
        raise

    finally:
        if session is not None:
            session.shutdown()
        if cluster is not None:
            cluster.shutdown()

# moderne API Dag (je me suis modernisé ainsi je suis à jour)
with DAG(
    dag_id="a1_scylladb_main_parsing",
    schedule=None,
    start_date=datetime(2026, 2, 27),
    catchup=False,
) as dag:
    PythonOperator(
        task_id="connexion_etablie",
        python_callable=connexion_etablie,
    )

