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


line = open('20260224154500.gkg.csv').readline()
parts = line.split('\t')

print("on lit une ligne")
print(f"Nombre de colonnes :{len(parts)}") #Nombre de colonnes?
print()

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

# construction du dict — on n'ajoute un champ QUE s'il est renseigné 
row = dict(zip(GKG_COLUMNS, parts))

structured = {}

# Champs simples
if not est_vide(row['id']):               structured['id']            = row['id'].strip()
if not est_vide(row['date']):             structured['date']          = parse_date(row['date'])
if not est_vide(row['source']):           structured['source']        = row['source'].strip()
if not est_vide(row['source_url']): structured['source_url'] = row['source_url'].strip()
if not est_vide(row['image']):      structured['image']      = row['image'].strip()
if not est_vide(row['source_type']):      structured['source_type']   = row['source_type'].strip()

# Champs multi-valeurs — on n'ajoute que s'il y a vraiment des valeurs
themes  = parse_liste(row['v1themes'])
persons = parse_liste(row['v1persons'])
orgs    = parse_liste(row['v1organizations'])
tone    = parse_tone(row['v1_5tone'])

if themes:   structured['themes']        = themes
if persons:  structured['persons']       = persons
if orgs:     structured['organizations'] = orgs
if tone:     structured['tone']          = tone

# Affichage final 
print("RÉSULTAT STRUCTURÉ")
print(f"Champs renseignés: {len(structured)} ")
print()
for k, v in structured.items():
    print(f"  {k:<16} : {v}")
