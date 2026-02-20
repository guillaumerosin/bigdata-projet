from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# fonction pour écrire dans Cassandra
def write_to_cassandra():
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy
    #connexion à Cassandra
    auth_provider = PlainTextAuthProvider(
    username="user_kawasaki",
    password="wTwF0UQRqL4it4j")
    cluster = Cluster(
        ['172.20.0.171', '172.20.0.172', '172.20.0.173'],
        load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
        auth_provider=auth_provider
    ) 
    # Remplacez par l'hôte de votre cluster Cassandra
    session = cluster.connect() # Remplacez par le nom de votre keyspace

    #je crée mon keyspace
    prepared = session.prepare("""
	INSERT INTO articlefulltexttest (id, article_date, source_type)
    	VALUES (?, ?, ?)
    """)
    
    session.execute(prepared, (  
	"test_auto",
        "20260220123000",
        "news"
    ))
    #je lui dis qu'il doit utiliser ce keyspace
    session.set_keyspace('keyspace_pour_les_nuls')

    #Je crée ma table 
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
    #Execution d'une requete d'insertion 
    session.execute("""
        INSERT INTO articlefulltexttest (id, article_date, source_type)
        VALUES (%s, %s, %s)
    """, ("test_auto", "20260220123000", "news"))

    print("Keyspace + table + insertion c'est OKKKKKAAYY")
# Créer le DAG
dag = DAG('exemple_cassandra', description='A simple tutorial DAG',
    schedule_interval=None,
    start_date=datetime(2026, 2, 18),
    catchup=False)

task_write = PythonOperator(
    task_id='a_write_to_cassandra_task',
    python_callable=write_to_cassandra,
    dag=dag,
)
