from cassandra.cluster import Cluster

cluster = Cluster(["127.0.0.1"])
session = cluster.connect()

session.execute("""
CREATE KEYSPACE IF NOT EXISTS gdelt
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
""")

session.set_keyspace("gdelt")

session.execute("""
CREATE TABLE IF NOT EXISTS articles (
    id text PRIMARY KEY,
    date timestamp,
    source_type text,
    source text,
    source_url text,
    v1themes text,
    v2themes text,
    v1locations text,
    v2locations text,
    v1persons text,
    v2persons text,
    v1organizations list<text>,
    v2organizations text,
    tone map<text, double>,
    image text,
    videos text,
    quotations text,
    allnames text,
    extraxml text
);
""")

print("Table créée")

# parsing de dinguerie
#source_type --> a transformé en texte --> 1   // 1 à 6 cf. v2SOURCECOLLECTIONIDENTIFIER
GKG_COLUMNS = [
    'id',           # [0]  20260223113000-0
    'date',         # [1]  20260223113000
    'source_type',  # [2]  1
    'source',       # [3]  thenational.scot
    'source_url',   # [4]  https://...        ← c'était source_id, c'est faux
    'v1counts',     # [5]  vide
    'v2_1counts',   # [6]  vide
    'v1themes',     # [7]  TAX_FNCACT;...
    'v2themes',     # [8]  PERSECUTION,...
    'v1locations',  # [9]  4#Glasgow,...
    'v2locations',  # [10] 1#Sudan,...
    'v1persons',    # [11] davies jr;...
    'v2persons',    # [12] Davies Jr,49;...
    'v1organizations', # [13] world service
    'v2organizations', # [14] World Service,...
    'v1_5tone',     # [15] -1.269,...
    'dates_texte',  # [16] 1#0#0#...
    'v2gcam',       # [17] wc:366,...
    'image',        # [18] https://...jpg
    'images_associees', # [19] vide
    'images_res_soc',   # [20] vide
    'videos',           # [21] vide
    'quotations',       # [22] 928|42||...
    'allnames',         # [23] Outstanding Debut,...
    'valeurs_numeriques', # [24] vide
    'info_traduction',    # [25] vide
    'extraxml'            # [26] <PAGE_LINKS>...
]

# si jamais les lignes v1Counts, v2.1counts, v1locations,v2persons,v2organisations,images(associées)
# images(res.soc.), quotations, allnames, infotraduction

# --- ScyllaDB ---
SCYLLA_CONTACT_POINTS = ['127.0.0.1']  # ou liste de nœuds du cluster
KEYSPACE = 'gkg'
TABLE_GKG = 'records'

# True si la valeur est vide 
def est_vide(value):
    v = (value or '').strip()
    return v == '' 

#deuxieme colonne date YYYYMMDDHHmmss → string lisible.
def parse_date(raw):
    raw = (raw or '').strip()
    if len(raw) < 14:
        return None
    return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]} {raw[8:10]}:{raw[10:12]}:{raw[12:14]}"

# transformation de la source_type en texte
def transformation_source_type(source_type):
    if source_type == '1':
        return 'WEB'
    if source_type == '2':
        return 'CITATIONONLY'
    if source_type == '3':
        return 'CORE'
    if source_type == '4':
        return 'DTIC'
    if source_type == '5':
        return 'JSTOR'
    if source_type == '6':
        return 'NONTEXTUALSOURCE'
    if source_type == '7':
        return 'OTHER'
    return None

def parse_liste(raw, separateur=';'):
    """Champ multi-valeurs → liste Python. Retourne [] si //osef."""
    if est_vide(raw):
        return []
    return [x.strip() for x in raw.split(separateur) if x.strip()]

# transformation de la v1_5tone en dict 
def parse_tone(raw):
    """V1.5TONE : 7 valeurs séparées par virgules → dict. Retourne {} si //osef."""
    if est_vide(raw):
        return {}
    keys = ['tone', 'positive', 'negative', 'polarity',
            'activity_density', 'self_group_density', 'word_count']
    parts_tone = raw.split(',')
    result = {}
    for i, k in enumerate(keys):
        try:
            result[k] = float(parts_tone[i])
        except (ValueError, IndexError):
            result[k] = None
    return result

def build_structured(parts):
    """Construit le dict structuré à partir des colonnes brutes (une ligne GKG)."""
    if len(parts) < len(GKG_COLUMNS):
        return None
    row = dict(zip(GKG_COLUMNS, parts))
    structured = {}
    if not est_vide(row['id']):
        structured['id'] = row['id'].strip()
    if not est_vide(row['date']):
        structured['date'] = parse_date(row['date'])
    if not est_vide(row['source']):
        structured['source'] = row['source'].strip()
    if not est_vide(row['source_url']):
        structured['source_url'] = row['source_url'].strip()
    if not est_vide(row['image']):
        structured['image'] = row['image'].strip()
    if not est_vide(row['source_type']):
        structured['source_type'] = transformation_source_type(row['source_type'].strip()) or row['source_type'].strip()
    themes = parse_liste(row['v1themes'])
    persons = parse_liste(row['v1persons'])
    orgs = parse_liste(row['v1organizations'])
    tone = parse_tone(row['v1_5tone'])
    if themes:
        structured['themes'] = themes
    if persons:
        structured['persons'] = persons
    if orgs:
        structured['organizations'] = orgs
    if tone:
        structured['tone'] = tone
    return structured


def get_scylla_session():
    """Connexion au cluster ScyllaDB et retourne une session."""
    from cassandra.cluster import Cluster
    cluster = Cluster(contact_points=SCYLLA_CONTACT_POINTS)
    return cluster.connect()


def ensure_schema(session):
    """Crée le keyspace et la table s'ils n'existent pas."""
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(KEYSPACE)
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_GKG} (
            id text PRIMARY KEY,
            date text,
            source text,
            source_url text,
            source_type text,
            image text,
            themes list<text>,
            persons list<text>,
            organizations list<text>,
            tone map<text, double>
        )
    """)


def insert_gkg(session, structured):
    """Insère un enregistrement structuré dans la table ScyllaDB."""
    if not structured or 'id' not in structured:
        return False
    tone_map = {k: v for k, v in structured.get('tone', {}).items() if v is not None}
    session.execute(
        f"""
        INSERT INTO {TABLE_GKG} (
            id, date, source, source_url, source_type, image,
            themes, persons, organizations, tone
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            structured['id'],
            structured.get('date'),
            structured.get('source'),
            structured.get('source_url'),
            structured.get('source_type'),
            structured.get('image'),
            structured.get('themes') or [],
            structured.get('persons') or [],
            structured.get('organizations') or [],
            tone_map,
        ),
    )
    return True


# --- Script : lit une ligne, parse, affiche et exporte en ScyllaDB ---
if __name__ == '__main__':
    line = open('20260224154500.gkg.csv').readline()
    parts = line.split('\t')

    print("On lit une ligne")
    print(f"Nombre de colonnes : {len(parts)}")
    print()

    structured = build_structured(parts)
    if not structured:
        print("Erreur: ligne invalide ou colonnes manquantes")
        exit(1)

    print("RÉSULTAT STRUCTURÉ")
    print(f"Champs renseignés: {len(structured)}")
    print()
    for k, v in structured.items():
        print(f"  {k:<16} : {v}")

    print()
    print("Export ScyllaDB...")
    try:
        session = get_scylla_session()
        ensure_schema(session)
        insert_gkg(session, structured)
        print("Insertion OK.")
    except Exception as e:
        print(f"Erreur ScyllaDB: {e}")
