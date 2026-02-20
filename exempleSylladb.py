from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# fonction pour écrire dans Cassandra
def write_to_cassandra():
    #connexion à Cassandra
    compte = PlainTextAuthProvider('user','mdp')
    cluster = Cluster(
        ['172.20.0.171', '172.20.0.172', '172.20.0.173'],
        load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
        auth_provider=compte
    ) # Remplacez par l'hôte de votre cluster Cassandra
    session = cluster.connect('my_keyspace') # Remplacez par le nom de votre keyspace

    #Execution d'une requete d'insertion 
    query = """
        INSERT INTO test (id, chaine) 
        VALUES (%s, %s)
    """
    values = (3, 'couilles de renard')
    session.execute(query, values)

    print("Données insérées dans Cassandra")

# Créer le DAG
dag = DAG('exemple_cassandra', description='A simple tutorial DAG',
    schedule_interval=None,
    start_date=datetime(2026, 2, 18),
    catchup=False)

task_write = PythonOperator(
    task_id='write_to_cassandra_task',
    python_callable=write_to_cassandra,
    dag=dag,
)