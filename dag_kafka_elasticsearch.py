import datetime
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
import json 
from datetime import datetime
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from airflow import DAG
from airflow.operators.python import PythonOperator
from hdfs import InsecureClient

def load_gcam_mapping():  # 1 usage
    conn = mysql.connector.connect(
        host="172.20.0.210:3306",
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
            if val.replace(".", "", 1).isdigit()
        ]
        top_20 = sorted(parsed_codes, key=lambda x: x[1], reverse=True)[:20]
        filtered = ",".join([f"{k}:{v}" for k, v in top_20])
        readable = [(GCAM_MAPPING.get(k, k), v) for k, v in top_20]
        return filtered, readable

    except Exception as e:
        print(f"Erreur enrich_gcam_text : {e}")
        return "", []

def parse_v1_5tone(v1_5tone_str):  # 1 usage
    try:
        parts = list(map(float, v1_5tone_str.strip().split(",")))
        if len(parts) != 7:
            raise ValueError("7 dimensions attendues")

        tone_score = parts[0]

        if tone_score >= 60:
            comment = "Excellent"
        elif tone_score >= 30:
            comment = "Très positif"
        elif tone_score > 0:
            comment = "Plutôt positif"
        elif tone_score == 0:
            comment = "Neutre"
        elif tone_score > -30:
            comment = "Plutôt négatif"
        elif tone_score > -60:
            comment = "Très négatif"
        else:
            comment = "Catastrophique"

        return {
            "tone": tone_score,
            "commentaire": comment,
            "positive_score": parts[1],
            "negative_score": parts[2],
            "polarity": parts[3],
            "activity_density": parts[4],
            "self_group_density": parts[5],
            "word_count": int(parts[6])
        }

    except Exception as e:
        print(f"Erreur parse_v1_5tone : {e}")
        return None

def parseDate_text(dates_fields):  # 1 usage
    dates = []
    for date in dates_fields.split(";"):
        parts = date.split("#")
        if len(parts) == 5:
            dates.append({
                'DateResolution': parts[0],
                'Month': parts[1],
                'Day': parts[2],
                'Year': parts[3],
                'Offset': parts[4]
            })
        else:
            dates.append({
                'DateResolution': '0',
                'Month': '0',
                'Day': '0',
                'Year': '0',
                'Offset': '0'
            })
    return dates

def parse_kafka_message(message):  # 1 usage
    try:
        if isinstance(message, bytes):
            message = message.decode('utf-8')

        parts = message.split("\t")
        if len(parts) < 27:
            raise ValueError("Message Kafka incomplet")

        gcam_filtered, gcam_readable = enrich_gcam_text(parts[17])
        v1_5tone_struct = parse_v1_5tone(parts[15])
        dates_struct = parseDate_text(parts[16])

        return {
            'id': parts[0],
            'timestamp': parts[1],
            'source_type': parts[2],
            'source': parts[3],
            'source_id': parts[4],
            'V1_themes': parts[7],
            'V2_themes': parts[8],
            'V2_Locations': parts[10],
            'V1_persons': parts[11],
            'V1_organisations': parts[13],
            'V1_5tone_raw': parts[15],
            'V1_5tone_structured': v1_5tone_struct,
            'dates_text': dates_struct,
            'gcam_text': gcam_filtered,
            'gcam_text_readable': gcam_readable,
            'image_principale': parts[18],
            'videos': parts[21],
            'valeur_numerisee': parts[24],
            'extra_xml': parts[26]
        }

    except Exception as e:
        print(f"Erreur parse_kafka_message : {e}")
        return None

def consume_from_kafka(nb_messages=15):  # 1 usage
    es = Elasticsearch(
        ['https://172.20.0.201:9200'],
        http_auth=('user_kawasaki', 'wTwF0UQRqL4it4j'),
        verify_certs=False,
        ssl_show_warn=False
    )

    consumer = KafkaConsumer(
        'test',
        bootstrap_servers='172.20.0.51:9092',
        auto_offset_reset='earliest',
    )

    count = 0
    for message in consumer:
        parsed_data = parse_kafka_message(message.value)
        if parsed_data:
            try:
                ts = datetime.strptime(parsed_data['timestamp'], "%Y%m%d%H%M%S").isoformat() + "Z"
                parsed_data['date'] = ts
                es.index(index="news_data", body=parsed_data)
                print(f"Indexé : {parsed_data['id']}")
                count += 1
                if count >= nb_messages:
                    break
            except Exception as e:
                print(f"Erreur indexation : {e}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(year=2024, month=2, day=28),
    'retries': 1
}

dag = DAG(
    'a_kafka_Elasticsearch_final',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
)

task = PythonOperator(
    task_id='consume_kafka_and_index',
    python_callable=consume_from_kafka,
    dag=dag
)

