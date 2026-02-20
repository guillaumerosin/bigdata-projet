from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# === Fonction principale ===
def kafka_to_scylla():
    from kafka import KafkaConsumer
    import json
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider

    # --- Kafka ---
    consumer = KafkaConsumer(
        'ton_topic',
        bootstrap_servers=['kafka:9092'],  # nom du service Docker Kafka
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='airflow-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    message = next(consumer)
    data = message.value
    print("Message reçu :", data)

    # --- Scylla ---
    auth_provider = PlainTextAuthProvider(
        username='user_kawasaki',
        password='wTwF0UQRqL4it4j'
    )

    cluster = Cluster(
        ['172.20.0.171'],
        auth_provider=auth_provider,
        protocol_version=4
    )

    session = cluster.connect('keyspace_pour_les_nuls')

    session.execute("""
        INSERT INTO articlefulltexttest (id, article_date, source_type)
        VALUES (%s, %s, %s)
    """, (
        data.get("id"),
        data.get("date"),
        data.get("source_type")
    ))

    print("Insertion OK")


# === DAG ===
dag = DAG(
    'exemple_cassandra',
    start_date=datetime(2026, 2, 18),
    schedule_interval=None,
    catchup=False
)

task_write = PythonOperator(
    task_id='kafka_to_scylla_task',
    python_callable=kafka_to_scylla,
    dag=dag,
)