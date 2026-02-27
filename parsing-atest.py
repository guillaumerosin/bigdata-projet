#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy



# --- CONNEXION AScyllaDB ---
SCYLLA_NODES = ['172.20.0.171', '172.20.0.172', '172.20.0.173']
SCYLLA_USER = 'user_kawasaki'
SCYLLA_PASS = 'wTwF0UQRqL4it4j'
SCYLLA_KEYSPACE = 'keyspace_pour_les_nuls'
LOCAL_DC = "datacenter1" # nom du datacenter local
PROTOCOL_VERSION = 4 # pour éviter les warnings de downgrade

def connexion_établie():
    cluster = None
    session = None
    auth = PlainTextAuthProvider(username=SCYLLA_USER, password=SCYLLA_PASS)
    cluster = Cluster(SCYLLA_NODES, auth_provider=PlainTextAuthProvider(username=SCYLLA_USER, password=SCYLLA_PASS))
    session = cluster.connect(SCYLLA_KEYSPACE)
    load_balancing_policy=TokenAwarePolicy(
        DCAwareRoundRobinPolicy(local_dc=LOCAL_DC)
    )
    protocol_version=PROTOCOL_VERSION
    session = cluster.connect(SCYLLA_KEYSPACE)
    try:
        print(session, "la connexion est établie avec succès ma biche")
    except Exception as e:
        print(f"Erreur de connexion à ScyllaDB: {e}")
        raise
    finally:
        if session is not None:
            session.shutdown()
        if cluster is not None:
            cluster.shutdown()

dag = DAG(
    'a1_scylladb_main_parsing',
    description='Connexion à ScyllaDB',
    schedule_interval=None,
    start_date=datetime(year=2026, month=2, day=27),
    catchup=False,

)
task_parsing = PythonOperator(
    task_id='connexion_établie',
    python_callable=connexion_établie,
    dag=dag,
)

