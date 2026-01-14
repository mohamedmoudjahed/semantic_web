"""
Configuration et namespaces pour le Knowledge Graph Tolkien.
"""

import os
from rdflib import Namespace

# Repertoire de travail (utilise le dossier courant)
BASE_DIR = os.getcwd()
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

# API
TOLKIEN_GATEWAY_API = "https://tolkiengateway.net/w/api.php"

# ============================================================================
# FUSEKI TRIPLESTORE CONFIGURATION
# ============================================================================
FUSEKI_URL = os.environ.get("FUSEKI_URL", "http://localhost:3030")
FUSEKI_DATASET = os.environ.get("FUSEKI_DATASET", "tolkien")

# Endpoints Fuseki
FUSEKI_SPARQL_ENDPOINT = f"{FUSEKI_URL}/{FUSEKI_DATASET}/sparql"
FUSEKI_UPDATE_ENDPOINT = f"{FUSEKI_URL}/{FUSEKI_DATASET}/update"
FUSEKI_DATA_ENDPOINT = f"{FUSEKI_URL}/{FUSEKI_DATASET}/data"

# Mode de stockage: 'fuseki' ou 'file'
# Si Fuseki n'est pas disponible, le système bascule automatiquement sur fichier
STORAGE_MODE = os.environ.get("STORAGE_MODE", "fuseki")

# ============================================================================
# NAMESPACES RDF
# ============================================================================
TOLKIEN_BASE = "http://tolkien-kg.org/"
TOLKIEN_RESOURCE = Namespace(TOLKIEN_BASE + "resource/")
TOLKIEN_PAGE = Namespace(TOLKIEN_BASE + "page/")
TOLKIEN_ONTOLOGY = Namespace(TOLKIEN_BASE + "ontology/")
TOLKIEN_PROPERTY = Namespace(TOLKIEN_BASE + "property/")

SCHEMA = Namespace("http://schema.org/")
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")
DC = Namespace("http://purl.org/dc/elements/1.1/")
DCT = Namespace("http://purl.org/dc/terms/")
SHACL = Namespace("http://www.w3.org/ns/shacl#")
DBPEDIA = Namespace("http://dbpedia.org/resource/")
YAGO = Namespace("http://yago-knowledge.org/resource/")
WIKIDATA = Namespace("http://www.wikidata.org/entity/")
METW = Namespace("http://tolkien-kg.org/metw/card/")

PREFIXES = {
    "tolkien": TOLKIEN_RESOURCE,
    "tpage": TOLKIEN_PAGE,
    "tont": TOLKIEN_ONTOLOGY,
    "tprop": TOLKIEN_PROPERTY,
    "schema": SCHEMA,
    "owl": Namespace("http://www.w3.org/2002/07/owl#"),
    "skos": SKOS,
    "foaf": Namespace("http://xmlns.com/foaf/0.1/"),
    "dc": DC,
    "dct": DCT,
    "xsd": Namespace("http://www.w3.org/2001/XMLSchema#"),
    "sh": SHACL,
    "dbpedia": DBPEDIA,
    "yago": YAGO,
    "wd": WIKIDATA,
    "metw": METW,
}

HTTP_HEADERS = {"User-Agent": "TolkienKGBot/1.0 (Semantic Web Project)"}
REQUEST_DELAY = 0.5  # Delai entre requetes en secondes
REQUEST_TIMEOUT = 30

# ============================================================================
# CATEGORIES DISPONIBLES
# ============================================================================
CATEGORIES = {
    "characters": [
        ("Third Age characters", 300),
        ("Second Age characters", 200),
        ("First Age characters", 200),
        ("Characters in The Lord of the Rings", 250),
        ("Characters in The Hobbit", 150),
        ("Elves", 200),
        ("Hobbits", 150),
        ("Dwarves", 150),
        ("Wizards", 50),
    ],
    "locations": [
        ("Cities, towns and villages", 200),
        ("Fortresses", 150),
        ("Mountains", 150),
        ("Rivers", 100),
    ],
    "artifacts": [
        ("Weapons", 150),
        ("Rings and jewels", 100),
    ],
    "events": [
        ("Conflicts of the First Age", 150),
        ("Conflicts of the Second Age", 100),
        ("Conflicts of the Third Age", 150),
    ],
}


# ============================================================================
# DYNAMIC EXTERNAL LINKING
# ============================================================================

# Configuration du linking dynamique
ENABLE_DYNAMIC_LINKING = True
LINKING_CACHE_FILE = os.path.join(OUTPUT_DIR, "external_links_cache.json")

# Seuil de similarité pour le matching de noms
LINKING_SIMILARITY_THRESHOLD = 0.9

# APIs externes pour le linking
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
DBPEDIA_SPARQL = "http://dbpedia.org/sparql"
