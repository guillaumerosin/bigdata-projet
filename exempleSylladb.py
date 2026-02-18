from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def get_scylla_session():
    auth = PlainTextAuthProvider(
        username='user_kawasaki',
        password='WtWF0UQRqL4it4j'
    )
    cluster = Cluster(
        contact_points=['172.20.0.171', '172.20.0.172', '172.20.0.173'],
        load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
        auth_provider=auth
    )