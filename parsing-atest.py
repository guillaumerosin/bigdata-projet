#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from cassandra.cluster import Cluster
from cassandra.query import PreparedStatement

# --- ScyllaDB ---
SCYLLA_CONTACT_POINTS = ["127.0.0.1"]
KEYSPACE = "gdelt"
TABLE = "articles"

GKG_COLUMNS = [
    "id",
    "date",
    "source_type",
    "source",
    "source_url",
    "v1counts",
    "v2_1counts",
    "v1themes",
    "v2themes",
    "v1locations",
    "v2locations",
    "v1persons",
    "v2persons",
    "v1organizations",
    "v2organizations",
    "v1_5tone",
    "dates_texte",
    "v2gcam",
    "image",
    "images_associees",
    "images_res_soc",
    "videos",
    "quotations",
    "allnames",
    "valeurs_numeriques",
    "info_traduction",
    "extraxml",
]


def est_vide(value: Any) -> bool:
    v = (value or "")
    if not isinstance(v, str):
        v = str(v)
    return v.strip() == ""


def parse_date(raw: str) -> Optional[datetime]:
    """
    Colonne date en format YYYYMMDDHHmmss -> datetime (timestamp Scylla).
    """
    raw = (raw or "").strip()
    if len(raw) < 14:
        return None
    try:
        return datetime.strptime(raw[:14], "%Y%m%d%H%M%S")
    except ValueError:
        return None


def transformation_source_type(source_type: str) -> Optional[str]:
    mapping = {
        "1": "WEB",
        "2": "CITATIONONLY",
        "3": "CORE",
        "4": "DTIC",
        "5": "JSTOR",
        "6": "NONTEXTUALSOURCE",
        "7": "OTHER",
    }
    return mapping.get((source_type or "").strip())


def parse_liste(raw: str, separateur: str = ";") -> List[str]:
    if est_vide(raw):
        return []
    return [x.strip() for x in raw.split(separateur) if x.strip()]


def parse_tone(raw: str) -> Dict[str, float]:
    """
    V1.5TONE : 7 valeurs séparées par virgules -> map<text,double>
    On filtre les valeurs manquantes/invalides.
    """
    if est_vide(raw):
        return {}

    keys = [
        "tone",
        "positive",
        "negative",
        "polarity",
        "activity_density",
        "self_group_density",
        "word_count",
    ]
    parts = [p.strip() for p in raw.split(",")]

    out: Dict[str, float] = {}
    for i, k in enumerate(keys):
        if i >= len(parts) or est_vide(parts[i]):
            continue
        try:
            out[k] = float(parts[i])
        except ValueError:
            continue
    return out


def build_structured(parts: List[str]) -> Optional[Dict[str, Any]]:
    if len(parts) < len(GKG_COLUMNS):
        return None

    row = dict(zip(GKG_COLUMNS, parts))
    structured: Dict[str, Any] = {}

    # Champs clés / simples
    if not est_vide(row["id"]):
        structured["id"] = row["id"].strip()

    dt = parse_date(row.get("date", ""))
    if dt is not None:
        structured["date"] = dt

    if not est_vide(row["source"]):
        structured["source"] = row["source"].strip()

    if not est_vide(row["source_url"]):
        structured["source_url"] = row["source_url"].strip()

    if not est_vide(row["image"]):
        structured["image"] = row["image"].strip()

    if not est_vide(row["source_type"]):
        st = row["source_type"].strip()
        structured["source_type"] = transformation_source_type(st) or st

    # Champs list / map
    themes = parse_liste(row.get("v1themes", ""))
    persons = parse_liste(row.get("v1persons", ""))
    orgs = parse_liste(row.get("v1organizations", ""))
    tone = parse_tone(row.get("v1_5tone", ""))

    if themes:
        structured["themes"] = themes
    if persons:
        structured["persons"] = persons
    if orgs:
        structured["organizations"] = orgs
    if tone:
        structured["tone"] = tone

    if "id" not in structured:
        return None
    return structured


def get_scylla_session():
    cluster = Cluster(contact_points=SCYLLA_CONTACT_POINTS)
    return cluster.connect()


def ensure_schema(session) -> None:
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
    )
    session.set_keyspace(KEYSPACE)

    # Table minimaliste = seulement ce que tu insères vraiment
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            id text PRIMARY KEY,
            "date" timestamp,
            source_type text,
            source text,
            source_url text,
            image text,
            themes list<text>,
            persons list<text>,
            organizations list<text>,
            tone map<text, double>
        )
        """
    )


def prepare_insert(session) -> PreparedStatement:
    """
    IMPORTANT :
    - Utilise un prepared statement
    - Utilise des bind markers %s (driver Cassandra/Scylla)
    - PAS de f-string pour injecter des valeurs, seulement pour nom table/keyspace.
    """
    session.set_keyspace(KEYSPACE)
    return session.prepare(
        f"""
        INSERT INTO {TABLE} (
            id, "date", source_type, source, source_url, image,
            themes, persons, organizations, tone
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    )


def insert_gkg(session, insert_stmt: PreparedStatement, structured: Dict[str, Any]) -> bool:
    if not structured or "id" not in structured:
        return False

    session.execute(
        insert_stmt,
        (
            structured["id"],
            structured.get("date"),  # datetime -> timestamp OK
            structured.get("source_type"),
            structured.get("source"),
            structured.get("source_url"),
            structured.get("image"),
            structured.get("themes") or [],
            structured.get("persons") or [],
            structured.get("organizations") or [],
            structured.get("tone") or {},
        ),
    )
    return True


if __name__ == "__main__":
    # --- Script : lit une ligne, parse, affiche et exporte en ScyllaDB ---
    with open("20260224154500.gkg.csv", "r", encoding="utf-8", errors="replace") as f:
        line = f.readline()

    parts = line.rstrip("\n").split("\t")

    print("On lit une ligne")
    print(f"Nombre de colonnes : {len(parts)}\n")

    structured = build_structured(parts)
    if not structured:
        raise SystemExit("Erreur: ligne invalide ou colonnes manquantes")

    print("RÉSULTAT STRUCTURÉ")
    print(f"Champs renseignés: {len(structured)}\n")
    for k, v in structured.items():
        print(f"  {k:<16} : {v}")

    print("\nExport ScyllaDB...")
    try:
        session = get_scylla_session()
        ensure_schema(session)
        insert_stmt = prepare_insert(session)
        insert_gkg(session, insert_stmt, structured)
        print("Insertion OK.")
    except Exception as e:
        print(f"Erreur ScyllaDB: {e}")