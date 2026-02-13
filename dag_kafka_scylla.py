import datetime
import logging
from kafka import KafkaConsumer
from airflow import DAG
from airflow.operators.python import PythonOperator
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy

# Initialiser le logger
log = logging.getLogger(__name__)

def parse_sourceType(source_type):
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


def parse_v1_theme(v1_theme):
    themes = v1_theme[:-1].split(";")
    return themes


def parse_v2_theme(v2_theme):
    try:
        my_dict = {}
        themes = v2_theme.split(";")
        for theme in themes:
            val = theme.split(":")
            my_dict.update({val[0]: int(val[1])})
        return my_dict
    except Exception as e:
        log.error(f"Erreur parsing v2_theme: {e}")
        return {}

    
def parse_tone_dict(tone_str):
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
        log.error(f"Erreur parsing tone: {e}")
        return None


# Mapping GCAM chargé depuis un fichier ou dict statique
# Remplace load_gcam_mapping() par un dict ou fichier JSON
GCAM_MAPPING = {}  # À remplir avec ton mapping


def enrich_gcam_text(codes_string):
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
        log.error(f"Erreur enrich gcam text: {e}")
        return {}


def parse_videos(str_videos):
    if str_videos:
        videos = str_videos[:-1].split(";")
        return videos
    return []


def consume_kafka_messages(max_messages=10):
    try:
        # Configuration avec plusieurs brokers Kafka
        consumer = KafkaConsumer(
            'test',
            bootstrap_servers=['172.20.0.51:9092', '172.20.0.52:9092'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )

        log.info("Connexion Kafka établie")
        count = 0
        
        for message in consumer:
            try:
                article_data = message.value.decode('utf-8')
                if article_data:
                    parts = article_data.split('\t')
                    
                    msg = {
                        "id": parts[0],
                        "article_date": parts[1],
                        "source_type": parse_sourceType(parts[2]),
                        "source_common_name": parts[3],
                        "source_id": parts[4],
                        "v1_themes": parse_v1_theme(parts[7]),
                        "v2_themes": parse_v2_theme(parts[8]),
                        "v2Locations": parts[10],
                        "persons": parts[11].split(";"),
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
                log.error(f"Erreur traitement message: {e}", exc_info=True)
                continue

        consumer.close()
        log.info(f"Total messages traités: {count}")

    except Exception as e:
        log.error(f"Erreur dans la consommation Kafka: {e}", exc_info=True)
        raise


def write_to_Scylla(article):
    try:
        # Connexion à Scylla avec les 3 nœuds
        compte = PlainTextAuthProvider(
            username='user_kawasaki', 
            password='WtWF0UQRqL4it4j'
        )
        
        cluster = Cluster(
            contact_points=['172.20.0.171', '172.20.0.172', '172.20.0.173'],
            load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
            auth_provider=compte
        )

        session = cluster.connect('test')

        query = """
            INSERT INTO articlefulltexttest (
                id, article_date, source_type, source_common_name, source_id,
                v1_themes, v2themes, v2Locations, persons, organizations, 
                tone, dates_in_text, gcam, sharing_image, videos, numerical_values
            )
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
        log.info(f"Données insérées dans Scylla pour article {article['id']}")
        
        cluster.shutdown()

    except Exception as e:
        log.error(f"Erreur insertion Scylla: {e}", exc_info=True)
        raise


# Créer le DAG
dag = DAG(
    dag_id="a_ArticleScylla",
    description="Consommer les msg du topic Kafka et les envoyer vers ScyllaDB",
    schedule_interval=None,
    start_date=datetime.datetime(year=2025, month=2, day=28),
    catchup=False,
)


# Définir la tâche
python_task = PythonOperator(
    task_id="TaskPutToScylla",
    python_callable=consume_kafka_messages,
    dag=dag
)
