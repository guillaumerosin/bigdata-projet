from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def write_to_cassandra():
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy

    KEYSPACE = "keyspace_pour_les_nuls"

    # Authentification 
    auth_provider = PlainTextAuthProvider(
        username="user_kawasaki",
        password="wTwF0UQRqL4it4j"
    )

    # Connexion cluster
    cluster = Cluster(
        ['172.20.0.171', '172.20.0.172', '172.20.0.173'],
        load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
        auth_provider=auth_provider,
        protocol_version=4,  # évite les warnings de downgrade
    )

    # 1) Connexion sans keyspace
    session = cluster.connect()

    # 2) Création du keyspace (idempotent)
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{
            'class': 'NetworkTopologyStrategy',
            'datacenter1': 3
        }}
    """)

    # 3) Sélection du keyspace
    session.set_keyspace(KEYSPACE)

    # 4) Création de la table (idempotent)
    session.execute("""
        CREATE TABLE IF NOT EXISTS articlefulltexttest (
            id TEXT PRIMARY KEY,
            article_date TEXT,
            source_type TEXT,
            source_common_name TEXT,
            source TEXT,
            v1_themes list<TEXT>,
            v2_themes map<TEXT, INT>,
            v2locations list<TEXT>,
            persons list<TEXT>,
            organisations list<TEXT>,
            v1_5_tone map<TEXT, FLOAT>,
            dates_in_text TEXT,
            gcam map<TEXT, FLOAT>,
            sharing_image TEXT,
            videos list<TEXT>,
            numerical_values TEXT
        )
    """)

    # 5) INSERT (préparé après keyspace + table)
    prepared = session.prepare("""
        INSERT INTO articlefulltexttest (id, article_date, source_type)
        VALUES (?, ?, ?)
    """)
    session.execute(prepared, ("test_auto", "20260220123000", "news"))

    print("Keyspace + table + insertion OK")

    # clean shutdown
    session.shutdown()
    cluster.shutdown()

def kafka_to_scylladb():
    from kafka import KafkaConsumer
    from json 
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider

    # Kafka
    consumer = KafkaConsumer(
        'test',
        bootstrap_servers=['172.20.0.51:9092', '172.20.0.52:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='a_airflow-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    )

   message = next(consumer)
   data = message.value
   print("Info recue de Kafka : ", data)




dag = DAG(
    'a_exemple_cassandra',
    description='A simple tutorial DAG',
    schedule_interval=None,
    start_date=datetime(2026, 2, 18),
    catchup=False
)

task_write = PythonOperator(
    task_id='write_to_cassandra_task',
    python_callable=write_to_cassandra,
    dag=dag,
)