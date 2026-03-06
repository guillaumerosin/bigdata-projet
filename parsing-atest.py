#!/usr/bin/env python3
# coding by guillaume rosin
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# CONNEXION A ScyllaDB
SCYLLA_NODES = ['172.20.0.171', '172.20.0.172', '172.20.0.173']
SCYLLA_USER = 'user_kawasaki'
SCYLLA_PASS = 'wTwF0UQRqL4it4j'
SCYLLA_KEYSPACE = 'keyspace_pour_les_nuls'
LOCAL_DC = "datacenter1"
PROTOCOL_VERSION = 4

KAFKA_TOPIC = "test"
KAFKA_BOOTSTRAP = ["172.20.0.51:9092","172.20.0.52:9092"]

MIN_COLUMNS = 27

# Parsing 1 : source_type — conversion numéro → libellé
SOURCE_TYPE_MAP = {
    "1": "Web",
    "2": "Citationonly",
    "3": "Core",
    "4": "Dtic",
    "5": "Jstor",
    "6": "Nontextualsource",
}
def transform_date(raw: str) -> str:
    """
    Transforme une date GDELT de type YYYYMMDDHHMMSS (parfois partielle ou collée à d'autres caractères)
    en chaîne lisible 'YYYY-MM-DD HH:MM:SS'.
    """
    if raw is None:
        return "0000-01-01 00:00:00"
    digits = "".join(ch for ch in str(raw).strip() if ch.isdigit())
    if not digits:
        return "0000-01-01 00:00:00"
    digits = digits.ljust(14, "0")[:14]
    y, m, d = digits[0:4], digits[4:6], digits[6:8]
    hh, mm, ss = digits[8:10], digits[10:12], digits[12:14]
    return f"{y}-{m}-{d} {hh}:{mm}:{ss}"

def transform_v15tone(raw: str) -> str:
    if not raw:
        return "NA"
    parts = raw.strip().split(",")
    if len(parts) != 7:
        return f"Format invalide ({len(parts)} valeurs attendu 7)."
    try:
        tone, pos, neg, pol, act, self_, wc = (
            float(parts[0]),
            float(parts[1]),
            float(parts[2]),
            float(parts[3]),
            float(parts[4]),
            float(parts[5]),
            int(float(parts[6])),
        )
    except Exception:
        return "NA"

    if tone < -10:
        tl = "extrêmement négatif"
    elif tone < -5:
        tl = "très négatif"
    elif tone < -2:
        tl = "négatif"
    elif tone < -0.5:
        tl = "légèrement négatif"
    elif tone <= 0.5:
        tl = "neutre"
    elif tone <= 2:
        tl = "légèrement positif"
    elif tone <= 5:
        tl = "positif"
    elif tone <= 10:
        tl = "très positif"
    else:
        tl = "extrêmement positif"

    vocab = (
        f"vocabulaire négatif dominant ({neg}% vs {pos}%)"
        if neg > pos + 1
        else f"vocabulaire positif dominant ({pos}% vs {neg}%)"
        if pos > neg + 1
        else f"vocabulaire équilibré (positif {pos}%, négatif {neg}%)"
    )
    al = "très actif" if act >= 8 else "modérément actif" if act >= 3 else "passif"
    sl = (
        "subjectif"
        if self_ >= 2
        else "légèrement personnel"
        if self_ >= 0.5
        else "impersonnel"
    )
    wl = (
        "très court"
        if wc < 100
        else "court"
        if wc < 300
        else "standard"
        if wc < 800
        else "long"
        if wc < 2000
        else "très long"
    )

    return (
        f"Ton {tl} (score = {tone}). {vocab.capitalize()}. "
        f"Charge émotionnelle : {pol}. Style {al} (densité action = {act}). "
        f"Registre {sl} (self/group = {self_}). Document {wl} ({wc} mots)."
    )


# Codebook GCAM : id dimension → [catégorie, nom] (gcam_codebook.json)
_GCAM_CODEBOOK: dict = {}
try:
    import os as _os
    import json as _json
    _gcam_path = _os.path.join(_os.path.dirname(__file__), "gcam_codebook.json")
    with open(_gcam_path, "r", encoding="utf-8") as _f:
        _GCAM_CODEBOOK = _json.load(_f)
except Exception:
    pass


def _dlabel(density: float) -> str:
    """Libellé de densité pour affichage GCAM."""
    if density < 0.5:
        return "très faible"
    if density < 2:
        return "faible"
    if density < 5:
        return "modéré"
    if density < 10:
        return "élevé"
    return "très élevé"


def transform_v2gcam(raw: str) -> str:
    """Parse la colonne v2GCAM (entrées key:value séparées par des virgules) et produit un résumé lisible via gcam_codebook.json."""
    if not raw or not str(raw).strip():
        return "NA"
    raw = str(raw).strip()
    entries = [e.strip() for e in raw.split(",") if ":" in e]
    wc = 1
    for e in entries:
        k, _, v = e.partition(":")
        if k == "wc":
            try:
                wc = int(v)
            except (ValueError, TypeError):
                pass

    count_items: list[tuple[float, str]] = []
    value_items: list[str] = []
    for e in entries:
        k, _, v = e.partition(":")
        if k in ("wc", "nwc"):
            continue
        if k.startswith("c"):
            dim = k[1:]
            try:
                count = int(v)
                density = round((count / max(wc, 1)) * 100, 3)
                info = _GCAM_CODEBOOK.get(dim)
                if isinstance(info, (list, tuple)) and len(info) >= 2:
                    name = f"{info[0]} / {info[1]}"
                else:
                    name = f"dim.{dim}"
                count_items.append((density, f"{name} : {count} mots ({density}%, {_dlabel(density)})"))
            except (ValueError, TypeError):
                pass
        elif k.startswith("v"):
            dim = k[1:]
            try:
                score = float(v)
                info = _GCAM_CODEBOOK.get(dim)
                if isinstance(info, (list, tuple)) and len(info) >= 2:
                    name = f"{info[0]} / {info[1]}"
                else:
                    name = f"dim.{dim}"
                value_items.append(f"{name} : score = {round(score, 4)}")
            except (ValueError, TypeError):
                pass

    if not count_items and not value_items:
        return "NA"

    count_items.sort(key=lambda x: x[0], reverse=True)
    result = (
        f"Document de {wc} mots. {len(count_items)} dimensions, {len(value_items)} scores continus.\n"
        "Top 10 dimensions par densité :\n"
        + "\n".join(f"  • {label}" for _, label in count_items[:10])
    )
    if value_items:
        result += "\nScores continus (extrait) :\n" + "\n".join(f"  • {l}" for l in value_items[:5])
    return result


def transform_v2dates(raw: str) -> str:
    if not raw:
        return "NA"
    blocks = [b.strip() for b in str(raw).split(";") if b.strip()]
    results: list[str] = []
    for block in blocks:
        parts = [p for p in block.split("#") if p != ""]
        try:
            res, mo, d, y, offset = (
                int(parts[0]),
                int(parts[1]),
                int(parts[2]),
                int(parts[3]),
                int(parts[4]),
            )
            if res == 1:
                date_str = f"00-00-{y:04d}"
            elif res == 2:
                date_str = f"00-{mo:02d}-{y:04d}"
            elif res == 3:
                date_str = f"{d:02d}-{mo:02d}-{y:04d}"
            elif res == 4:
                date_str = f"{d:02d}-{mo:02d}-0000"
            else:
                date_str = "00-00-0000"
            results.append(date_str)
        except Exception:
            continue
    if not results:
        return "NA"
    return "\n".join(results)


def _get_cluster():
    """Crée et retourne un objet Cluster Cassandra/ScyllaDB."""
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

    auth = PlainTextAuthProvider(username=SCYLLA_USER, password=SCYLLA_PASS)
    return Cluster(
        contact_points=SCYLLA_NODES,
        port=9042,
        auth_provider=auth,
        protocol_version=PROTOCOL_VERSION,
        load_balancing_policy=TokenAwarePolicy(
            DCAwareRoundRobinPolicy(local_dc=LOCAL_DC)
        ),
    )
# Retourne parts[index] si présent, sinon None => NULL en base
def safe_get(parts, index):
    return parts[index] if index < len(parts) else None

def connexion_etablie():
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

    cluster = None
    session = None

    try:
        auth = PlainTextAuthProvider(username=SCYLLA_USER,password=SCYLLA_PASS)

        cluster = Cluster(
            contact_points=SCYLLA_NODES,
            port=9042,
            auth_provider=auth,
            protocol_version=PROTOCOL_VERSION,
            load_balancing_policy=TokenAwarePolicy(
                DCAwareRoundRobinPolicy(local_dc=LOCAL_DC)
            ),
        )

        session = cluster.connect(SCYLLA_KEYSPACE)
        row = session.execute(
            "SELECT release_version FROM system.local"
        ).one()

        log.info(
            "Connexion Scylla OK. release_version=%s",
            getattr(row, "release_version", None)
        )

    except Exception as e:
        log.exception("Erreur de connexion à ScyllaDB")
        raise

    finally:
        if session is not None:
            session.shutdown()
        if cluster is not None:
            cluster.shutdown()


def create_db():
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

    cluster = None
    session = None
    try:
        auth = PlainTextAuthProvider(username=SCYLLA_USER, password=SCYLLA_PASS)
        cluster = Cluster(
            contact_points=SCYLLA_NODES,
            port=9042,
            auth_provider=auth,
            protocol_version=PROTOCOL_VERSION,
            load_balancing_policy=TokenAwarePolicy(
                DCAwareRoundRobinPolicy(local_dc=LOCAL_DC)
            ),
        )

        session = cluster.connect()


        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS gdelt
            WITH replication = {
                 'class': 'NetworkTopologyStrategy',
                 'replication_factor': 3
            };
        """)  
    

        session.set_keyspace("gdelt")

        session.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id text PRIMARY KEY,
            date timestamp,
            source_type text,
            source text,
            source_id text,
            v1themes text,
            v2themes text,
            v2locations text,
            v1persons text,
            v1organizations text,
            tone text,
            dates_dans_texte text,
            v2GCAM text,
            image text,
            videos text,
            valeurs_numeriques text,
            extraxml text
            );
        """)

        log.info("Table créée")
    except Exception as e:
        log.exception("erreur lors de la création de la db")
        raise
    finally:
        if session is not None:
            session.shutdown()
        if cluster is not None:
            cluster.shutdown()



def read_kafka_for_scylla():
    import json
    import uuid
    from kafka import KafkaConsumer

    consumer = KafkaConsumer (
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
        # value_deserializer=lambda x: json.loads(x.decode("utf-8")),  # c'est pas du json debilous
        consumer_timeout_ms=10000, 
    )
    messages = []
    for msg in consumer:
        raw = msg.value.decode("utf-8", errors="replace")
        # log.info("Message brut Kafka: %s", raw)  # debug: décommenter pour voir chaque message brut
        # log.info("Message brut Kafka (200 premiers car.): %s", raw[:200])  # debug: version courte
        messages.append(raw)

    consumer.close()
    log.info("%d messages lus depuis Kafka", len(messages))
    return messages

def to_list_or_none_WOW(s):
    """Convertit une chaîne 'a;b;c' en liste ['a','b','c'], ou None si vide."""
    if s is None or not s.strip():
        return None
    return [x for x in s.split(";") if x]


def my_process_data(raw: str) -> dict | None:
    parts = raw.split("\t") #je parse ma data

    # log.info("Nombre de colonnes: %d", len(parts))  # debug
    # log.info("parts[0] (id)=%s, parts[1] (date)=%s", safe_get(parts, 0), safe_get(parts, 1))  # debug
    #if len(parts) < MIN_COLUMNS:  #N étant le nombre minimal que je veux utiliser
        #log.warning("Ligne trop courte (%d colonnes), ignorée: %s",len(parts),raw[:80])
        #return None
    
    # Ligne sans identifiant (je sais pas si ca sert c'est un sureté)
    if not parts[0].strip():
        log.warning("Ligne sans id, ignorée : %s", raw[:20])
        return None 

    raw_source_type = safe_get(parts, 2)
    code = str(raw_source_type).strip() if raw_source_type is not None else ""
    # Si le code est vide ou inconnu, on bascule sur un libellé de repli explicite
    if not code:
        source_type = "unknow"
    else:
        source_type = SOURCE_TYPE_MAP.get(code, "unknow")

    raw_date = safe_get(parts, 1)
    date_human = transform_date(raw_date) if raw_date else "0000-01-01 00:00:00"

    raw_tone = safe_get(parts, 12)
    tone = transform_v15tone(raw_tone) if raw_tone else " "

    raw_dates = safe_get(parts, 16)
    dates_dt = transform_v2dates(raw_dates) if raw_dates else " "

    raw_v2gcam = safe_get(parts, 14)
    v2gcam = transform_v2gcam(raw_v2gcam) if raw_v2gcam else "NA"

    msg = {
        "id":               safe_get(parts, 0),
        "date":             date_human,
        "source_type":      source_type,
        "source":           safe_get(parts, 3),
        "source_id":        safe_get(parts, 4),
        "v1themes":         safe_get(parts, 7),
        "v2themes":         safe_get(parts, 8),
        "v2locations":      safe_get(parts, 10),
        "v1persons":        safe_get(parts, 11),
        "v1organizations":  safe_get(parts, 13),
        "tone":             tone,
        "dates_dans_texte": dates_dt,
        "v2GCAM":           v2gcam,
        "image":            safe_get(parts, 18),
        "videos":           safe_get(parts, 21),
        "valeurs_numeriques": "temp",
        "extraxml":         safe_get(parts, 26),
    }
    return msg


def insertion_scylla(session, msg: dict) -> None:
    """Insère un article dans la table gdelt.articles."""
    session.execute(
        """
        INSERT INTO gdelt.articles (
            id, date, source_type, source, source_id,
            v1themes, v2themes, v2locations,
            v1persons, v1organizations,
            tone, dates_dans_texte, v2GCAM,
            image, videos, valeurs_numeriques, extraxml
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s
        )
        """,
        (
            msg["id"],
            msg["date"],
            msg["source_type"],
            msg["source"],
            msg["source_id"],
            msg["v1themes"],
            msg["v2themes"],
            msg["v2locations"],
            msg["v1persons"],
            msg["v1organizations"],
            msg["tone"],
            msg["dates_dans_texte"],
            msg["v2GCAM"],
            msg["image"],
            msg["videos"],
            msg["valeurs_numeriques"],
            msg["extraxml"],
        ),
    )


def task_parse_messages(**context):
    """Tâche Airflow : récupère les messages Kafka via XCom, parse chaque ligne, renvoie la liste des dicts."""
    ti = context["ti"]
    messages = ti.xcom_pull(task_ids="read_kafka_for_scylla")
    if not messages:
        log.info("Aucun message à parser (XCom vide).")
        return []
    # Debug: log des 100 premières valeurs brutes (Kafka) pour aider à vérifier les index
    log.info("=== Debug Kafka (100 premières lignes) — champs bruts ===")
    for i, raw in enumerate(messages[:20]):
        parts = raw.split("\t")
        v2locations_raw = safe_get(parts, 10)
        tone_raw = safe_get(parts, 12)          # v1.5tone brut
        v2gcam_raw = safe_get(parts, 14)         # v2GCAM brut
        dates_raw = safe_get(parts, 16)          # dates(dans texte) brut
        numeric_raw_guess = safe_get(parts, 17)  # hypothèse "valeurs numériques" (à confirmer)
        extraxml_raw = safe_get(parts, 26)       # extraxml brut (souvent très long)

        def _cut(s: str | None, n: int = 200):
            if not s:
                return s
            return s[:n] + "…" if len(s) > n else s

        log.info(
            "Kafka ligne %d — len(parts)=%d | v2locations=%r | v1.5tone=%r | v2GCAM=%r | dates_txt=%r | numeric?=%r | extraxml=%r",
            i + 1,
            len(parts),
            _cut(v2locations_raw),
            _cut(tone_raw),
            _cut(v2gcam_raw),
            _cut(dates_raw),
            _cut(numeric_raw_guess),
            _cut(extraxml_raw),
        )
    log.info("=== Fin debug Kafka (100 premières lignes) ===")
    result = []
    for raw in messages:
        parsed = my_process_data(raw)
        if parsed is not None:
            result.append(parsed)
    log.info("Parsing terminé : %d messages valides sur %d.", len(result), len(messages))
    return result


def task_insert_to_scylla(**context):
    """Tâche Airflow : récupère la liste des dicts parsés via XCom, se connecte à Scylla, insère chaque article."""
    ti = context["ti"]
    parsed_list = ti.xcom_pull(task_ids="my_process_data")
    if not parsed_list:
        log.info("Aucun message à insérer.")
        return
    cluster = _get_cluster()
    session = cluster.connect("gdelt")
    try:
        for msg in parsed_list:
            insertion_scylla(session, msg)
        log.info("Ingest terminé : %d articles insérés.", len(parsed_list))
    finally:
        session.shutdown()
        cluster.shutdown()


with DAG(
    dag_id="a1_scylladb_main_parsing",
    schedule=None,
    start_date=datetime(2026, 2, 27),
    catchup=False,
) as dag:

    connexion_task = PythonOperator(
        task_id="connexion_etablie",
        python_callable=connexion_etablie,
    )

    create_task = PythonOperator(
        task_id="create_table",
        python_callable=create_db,
    )

    read_kafka_task = PythonOperator(
        task_id="read_kafka_for_scylla",
        python_callable=read_kafka_for_scylla,
    )

    parse_task = PythonOperator(
        task_id="my_process_data",
        python_callable=task_parse_messages,
    )

    insert_task = PythonOperator(
        task_id="insertion_scylla",
        python_callable=task_insert_to_scylla,
    )
    # Ordre des tâches (graphe inchangé : 5 nœuds)
    connexion_task >> create_task >> read_kafka_task >> parse_task >> insert_task
