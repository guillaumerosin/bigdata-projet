import datetime
from kafka import KafkaConsumer
from airflow import DAG
from airflow.operators.python import PythonOperator
from hdfs import InsecureClient

# créer le dag 
my_dag = DAG(
    dag_id="a_PutToHadoop",
    description="Consommer les msg du topic kafka et les envoyer vers hadoop",
    schedule_interval=None, #déclenchement manuel
    start_date=datetime.datetime(year=2026, month=2, day=15),
    catchup=False, #n'execute pas les taches passées
)

def consume_data():  # 1 usage
    client = InsecureClient('http://172.20.0.190:9870') #Pour parler à notre Namemode et insecure car pas d'authentification Kerberos
    # fichier = 'opt/airflow/dags/a_PutToHadoop.txt'
    hdfs_base_path = "/user/hadoop/"  # chemin dossier auquel on va rajouter le nom du fichier en prenant l'id de l'article
    consumer = KafkaConsumer(
        'test',
        bootstrap_servers=['172.20.0.51:9092'],
        auto_offset_reset='earliest',
        group_id='airflow-hdfs',
        enable_auto_commit=True,
        consumer_timeout_ms=5000  # après 5s sans nouveau messages ma boucle se termine
    )

    for message in consumer:
        article_data = message.value.decode('utf-8')

        # Exemple message : "cle1:valeur1, valeur2"
        if '\t' in article_data:
            key, values = article_data.split('\t', 1)
            key = key.strip()
            # values = values.strip()
            hdfs_file_path = f"{hdfs_base_path}{key}.txt"  # Création du chemin (chemin de base + id article)

            with client.write(hdfs_file_path) as writer:
                writer.write(article_data)

        else:
            print(f" mauvais format pour le message : {article_data}")

    consumer.close()

# définir les taches
python_task = PythonOperator(
    task_id='TaskPutToHadoop',  # nom de la tache
    python_callable=consume_data,  # fonction a appeler
    dag=my_dag  # lien avec le dag
)
