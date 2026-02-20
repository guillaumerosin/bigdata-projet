from airflow import DAG
from airflow.dummy_operators import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Definir une fonction à executer avec PythonOperator
def print_hello():
    return 'Hello from Airflow!'


# Créer un dag
dag = DAG('exemple_dag', description='A simple tutorial DAG',
          schedule_interval=None,   #déclenche manuellement
          # schedule_interval='@daily', #déclenche chaque jour à 00:00
          start_date=datetime(2026, 2, 18),  #Date de début du dag
          catchup=False)  #Ne pas executer les taches passées

# Définir les taches
start_task = DummyOperator(
    task_id='start',  #nom de la tache
    dag=dag #lien avec le dag
)

python_task = PythonOperator(
    task_id='hello_task',  #nom de la tache
    python_callable=print_hello,  #fonction à appeler
    dag=dag  #lien avec le dag
)

start_task >> end_task #Definir les dépendances

