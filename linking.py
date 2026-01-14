"""
Dynamic External Linking - Découverte automatique des liens vers DBpedia/Wikidata.
approche dynamique qui découvre les liens via l'API Wikipedia/Wikidata.
"""

import re
import time
import logging
from typing import Optional, Dict, List, Tuple
from urllib.parse import quote, unquote

import requests

from config import HTTP_HEADERS, REQUEST_TIMEOUT

logger = logging.getLogger(__name__)

# APIs
WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"

# Cache pour éviter les requêtes répétées
_link_cache: Dict[str, Dict[str, str]] = {}

# Délai entre requêtes (rate limiting)
REQUEST_DELAY = 0.5
_last_request_time = 0


def _rate_limit():
    """Respecte le rate limiting."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < REQUEST_DELAY:
        time.sleep(REQUEST_DELAY - elapsed)
    _last_request_time = time.time()


def search_wikipedia(query: str) -> Optional[str]:
    """
    Recherche une page Wikipedia correspondant à une entité Tolkien.
    Retourne le titre de la page Wikipedia ou None.
    """
    _rate_limit()
    
    # Stratégie de recherche : essayer plusieurs variantes
    search_queries = [
        query,
        f"{query} (Middle-earth)",
        f"{query} (character)",
        f"{query} Tolkien",
        f"{query} Lord of the Rings",
    ]
    
    for search_term in search_queries:
        try:
            params = {
                "action": "query",
                "list": "search",
                "srsearch": search_term,
                "srlimit": 5,
                "format": "json",
            }
            
            response = requests.get(
                WIKIPEDIA_API,
                params=params,
                headers=HTTP_HEADERS,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            data = response.json()
            
            results = data.get("query", {}).get("search", [])
            
            for result in results:
                title = result.get("title", "")
                # Vérifier si c'est une page Tolkien (heuristique)
                if _is_tolkien_related(title, query):
                    return title
                    
        except Exception as e:
            logger.debug(f"Wikipedia search error for '{search_term}': {e}")
            continue
    
    return None


def _is_tolkien_related(wiki_title: str, original_name: str) -> bool:
    """
    Vérifie si un titre Wikipedia est lié à l'univers Tolkien.
    """
    wiki_lower = wiki_title.lower()
    name_lower = original_name.lower()
    
    # Correspondance directe
    if name_lower in wiki_lower or wiki_lower.startswith(name_lower.split()[0]):
        tolkien_indicators = [
            "middle-earth", "tolkien", "lord of the rings", "hobbit",
            "silmarillion", "arda", "gondor", "rohan", "mordor",
            "(middle-earth)", "(tolkien)"
        ]
        
        # Si le titre contient un indicateur Tolkien explicite
        for indicator in tolkien_indicators:
            if indicator in wiki_lower:
                return True
        
        # Sinon, accepter si le nom correspond bien
        if name_lower == wiki_lower or f"{name_lower} (" in wiki_lower:
            return True
    
    return False


def get_wikidata_id_from_wikipedia(wikipedia_title: str) -> Optional[str]:
    """
    Récupère l'ID Wikidata à partir d'un titre Wikipedia.
    """
    _rate_limit()
    
    try:
        params = {
            "action": "query",
            "titles": wikipedia_title,
            "prop": "pageprops",
            "ppprop": "wikibase_item",
            "format": "json",
        }
        
        response = requests.get(
            WIKIPEDIA_API,
            params=params,
            headers=HTTP_HEADERS,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()
        
        pages = data.get("query", {}).get("pages", {})
        for page_id, page_data in pages.items():
            if page_id != "-1":
                return page_data.get("pageprops", {}).get("wikibase_item")
                
    except Exception as e:
        logger.debug(f"Wikidata ID lookup error for '{wikipedia_title}': {e}")
    
    return None


def search_wikidata_direct(query: str) -> Optional[str]:
    """
    Recherche directement sur Wikidata si Wikipedia ne trouve rien.
    """
    _rate_limit()
    
    try:
        params = {
            "action": "wbsearchentities",
            "search": query,
            "language": "en",
            "limit": 5,
            "format": "json",
        }
        
        response = requests.get(
            WIKIDATA_API,
            params=params,
            headers=HTTP_HEADERS,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()
        
        results = data.get("search", [])
        
        for result in results:
            description = result.get("description", "").lower()
            # Vérifier si c'est lié à Tolkien
            if any(kw in description for kw in ["tolkien", "middle-earth", "lord of the rings", "fictional"]):
                return result.get("id")
                
    except Exception as e:
        logger.debug(f"Wikidata direct search error for '{query}': {e}")
    
    return None


def get_dbpedia_uri(wikipedia_title: str) -> str:
    """
    Construit l'URI DBpedia à partir d'un titre Wikipedia.
    DBpedia utilise le même identifiant que Wikipedia.
    """
    # Normaliser le titre pour DBpedia
    dbpedia_name = wikipedia_title.replace(" ", "_")
    return f"http://dbpedia.org/resource/{quote(dbpedia_name, safe='_(),-')}"


def get_yago_uri(wikipedia_title: str, wikidata_id: str = None) -> Optional[str]:
    """
    Construit l'URI YAGO à partir du titre Wikipedia.
    
    YAGO utilise le format: http://yago-knowledge.org/resource/Nom_Entite
    """
    if not wikipedia_title:
        return None
    
    # Format YAGO: remplacer espaces par underscores
    yago_name = wikipedia_title.replace(" ", "_")
    return f"http://yago-knowledge.org/resource/{quote(yago_name, safe='_-')}"


def discover_external_links(entity_name: str) -> Dict[str, str]:
    """
    Découvre dynamiquement les liens externes pour une entité.
    
    Args:
        entity_name: Nom de l'entité (ex: "Gandalf", "Frodo Baggins")
        
    Returns:
        Dictionnaire avec les clés possibles: 'dbpedia', 'wikidata', 'yago', 'wikipedia'
    """
    # Vérifier le cache
    if entity_name in _link_cache:
        return _link_cache[entity_name]
    
    links = {}
    
    # 1. Chercher sur Wikipedia
    wiki_title = search_wikipedia(entity_name)
    
    if wiki_title:
        # Wikipedia trouvé
        links["wikipedia"] = f"https://en.wikipedia.org/wiki/{quote(wiki_title.replace(' ', '_'))}"
        
        # DBpedia (dérivé de Wikipedia)
        links["dbpedia"] = get_dbpedia_uri(wiki_title)
        
        # YAGO (dérivé de Wikipedia)
        yago_uri = get_yago_uri(wiki_title)
        if yago_uri:
            links["yago"] = yago_uri
        
        # Wikidata (via Wikipedia)
        wikidata_id = get_wikidata_id_from_wikipedia(wiki_title)
        if wikidata_id:
            links["wikidata"] = f"http://www.wikidata.org/entity/{wikidata_id}"
    else:
        # Fallback: recherche directe sur Wikidata
        wikidata_id = search_wikidata_direct(entity_name)
        if wikidata_id:
            links["wikidata"] = f"http://www.wikidata.org/entity/{wikidata_id}"
    
    # Mettre en cache
    _link_cache[entity_name] = links
    
    if links:
        logger.info(f"Found external links for '{entity_name}': {list(links.keys())}")
    else:
        logger.debug(f"No external links found for '{entity_name}'")
    
    return links


def discover_links_batch(entity_names: List[str], verbose: bool = True) -> Dict[str, Dict[str, str]]:
    """
    Découvre les liens externes pour plusieurs entités.
    
    Args:
        entity_names: Liste des noms d'entités
        verbose: Afficher la progression
        
    Returns:
        Dictionnaire {entity_name: {link_type: uri}}
    """
    results = {}
    total = len(entity_names)
    found = 0
    
    if verbose:
        print(f"\nDiscovering external links for {total} entities...")
    
    for i, name in enumerate(entity_names, 1):
        links = discover_external_links(name)
        if links:
            results[name] = links
            found += 1
        
        if verbose and i % 10 == 0:
            print(f"  Progress: {i}/{total} ({found} with links)")
    
    if verbose:
        print(f"  Complete: {found}/{total} entities linked")
    
    return results


def verify_dbpedia_exists(dbpedia_uri: str) -> bool:
    """
    Vérifie qu'une ressource DBpedia existe réellement.
    """
    _rate_limit()
    
    try:
        # Utiliser l'endpoint SPARQL de DBpedia
        sparql_endpoint = "http://dbpedia.org/sparql"
        query = f"ASK {{ <{dbpedia_uri}> ?p ?o }}"
        
        response = requests.get(
            sparql_endpoint,
            params={"query": query, "format": "json"},
            headers=HTTP_HEADERS,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()
        
        return data.get("boolean", False)
        
    except Exception as e:
        logger.debug(f"DBpedia verification error for '{dbpedia_uri}': {e}")
        # En cas d'erreur, on assume que le lien est valide
        return True


def clear_cache():
    """Vide le cache des liens."""
    global _link_cache
    _link_cache = {}


def get_external_links_for_entity(entity_name: str) -> Dict[str, str]:
    """
    Alias pour discover_external_links.
    Utilisé pour remplacer les lookups dans EXTERNAL_LINKS.
    """
    return discover_external_links(entity_name)
