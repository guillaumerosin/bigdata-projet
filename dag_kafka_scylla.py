import datetime
from kafka import KafkaConsumer
from airflow import DAG
from airflow.operators.python import PythonOperator
from hdfs import InsecureClient
import mysql.connector
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.hooks.base_hook import BaseHook

def parse_sourceType(source_type):  # 1 usage
    if (source_type):
        match source_type:
            case '1':
                return "WEB"
            case '2':
                return "CITATIONONLY"
            case '3':
                return "CORE"
            case '4':
                return "DTIC"
            case '5':
                return "JSTOR"
            case '6':
                return "NONTEXTUALSOURCE"
            case _:
                return None
    else:
        return None


def parse_v1_theme(v1_theme):  # 1 usage
    themes = v1_theme[:-1].split(";")
    return themes


def parse_v2_theme(v2_theme):  # 1 usage
    try:
        my_dict = {}
        themes = v2_theme.split(";")
        for theme in themes:
            val = theme.split(":")
            my_dict.update({val[0]: int(val[1])})  # ajouter la paire clé:valeur dans le dico

        return my_dict

    except Exception as e:
        print("Erreur parsing locations", e)
        return my_dict
    
def parse_tone_dict(tone_str):  # 1 usage
    try:
        tones = tone_str.split(",")
        if len(tones) >= 7:
            return {
                'Tone': float(tones[0]),
                'PositiveScore': float(tones[1]),
                'NegativeScore': float(tones[2]),
                'Polarity': float(tones[3]),
                'Activity_reference_density': float(tones[4]),
                'Self_reference_density': float(tones[5]),
                'WordCount': float(tones[6])
            }
    except Exception as e:
        print("Erreur parsing tone", e)
        return None


def load_gcam_mapping():  # usage
    conn = mysql.connector.connect(
        host="172.20.0.210",
        user="root",
        password="wTwF0UQRqL4it4j",
        database="test"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT Variable, DimensionHumanName FROM gcam_new_table")
    mapping = {row[0].strip(): row[1].strip() for row in cursor.fetchall()}
    cursor.close()
    conn.close()
    return mapping


GCAM_MAPPING = load_gcam_mapping()


def enrich_gcam_text(codes_string):  # 1 usage
    try:
        codes = codes_string.split(",")
        parsed_codes = [
            (key.strip(), float(val.strip()))
            for code in codes if ":" in code
            for key, val in [code.split(":")]
            if val.replace('.', '', 1).isdigit()
        ]

        top_20 = sorted(parsed_codes, key=lambda x: x[1], reverse=True)[:20]

        readable_dict = {
            GCAM_MAPPING.get(k, k): v for k, v in top_20
        }

        return readable_dict

    except Exception as e:
        print(f"Erreur enrich gcam text : {e}")
        return {}


def parse_videos(str_videos):  # 1 usage
    videos = str_videos[:-1].split(";")
    return videos

def consume_kafka_messages(max_messages=10):  # 1 usage
    try:
        consumer = KafkaConsumer(
            'test',
            bootstrap_servers='172.20.0.51:9092',
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000  # arrêter de lire après 5s si plus de msg
        )

        count = 0
        for message in consumer:
            article_data = message.value.decode('utf-8')
            if article_data:
                parts = article_data.split('\t')
                msg = {
                    "id": parts[0],
                    "article_date": parts[1],
                    "source_type": parse_sourceType(parts[2]),
                    "source_common_name": parts[3],
                    "source_id": parts[4],
                    "v1_themes": parse_v1_theme(parts[7]),  # liste
                    "v2_themes": parse_v2_theme(parts[8]),  # map
                    "v2Locations": parts[10],
                    "persons": parts[11].split(";"),  # list
                    "organizations": parts[13].split(";"),
                    "tone": parse_tone_dict(parts[15]),
                    "dates_in_text": parts[16],
                    "gcam": enrich_gcam_text(parts[17]),
                    "sharing_image": parts[18],
                    "videos": parse_videos(parts[21]),
                    "numerical_values": parts[24],
                }

                log.info(f"Insertion article ID: {msg['id']}")
                write_to_Scylla(msg)

                count += 1
                if count >= max_messages:
                    break

    except Exception as e:
        print(f"Erreur dans la consommation Kafka : {e}")
        log.error(f"Erreur dans la consommation Kafka : {e}", exc_info=True)
        raise ValueError("Erreur dans la consommation Kafka")

# Définir une fonction à exécuter avec PythonOperator
def write_to_Scylla(article):  # 1 usage
    # Connexion à Scylla (ou Cassandra)
    compte = PlainTextAuthProvider(username='user_kawasaki', password='WtWF0UQRqL4it4j')
    cluster = Cluster(
        contact_points=['172.20.0.171', '172.20.0.172', '172.20.0.173'],
        load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
        auth_provider=compte
    )

    session = cluster.connect('test')  # remplacer par le nom du keyspace

    # Execution d'une requête d'insertion
    query = """
        INSERT INTO articlefulltexttest (id, article_date, source_type, source_common_name, source_id,
        v1_themes, v2themes, v2Locations, persons, organizations, tone, dates_in_text,
        gcam, sharing_image, videos, numerical_values)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    values = (
        article['id'],
        article['article_date'],
        article['source_type'],
        article['source_common_name'],
        article['source_id'],
        article['v1_themes'],
        article['v2_themes'],
        article['v2Locations'],
        article['persons'],
        article['organizations'],
        article['tone'],
        article['dates_in_text'],
        article['gcam'],
        article['sharing_image'],
        article['videos'],
        article['numerical_values'],
    )

    session.execute(query, values)
    print('data insérées dans Cassandra')


# créer le dag
dag = DAG(
    dag_id="a_ArticleScylla",  # nom du DAG
    description="Consommer les msg du topic kafka et les envoyer vers ScyllaDb",
    # schedule_interval=datetime.timedelta(minutes=30),
    schedule_interval=None,  # Déclenchement manuel
    start_date=datetime.datetime(year=2025, month=2, day=28),  # date de début du DAG
    catchup=False,  # ne pas exec des taches passées
)


# définir les taches
python_task = PythonOperator(
    task_id="TaskPutToScylla",  # nom de la tache
    python_callable=consume_kafka_messages,  # fonction a executer
    dag=dag  # lien avec le dag
)


