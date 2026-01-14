"""
Enrichissement avec donnees externes:
- Cartes METW (Middle Earth: The Wizards)
- CSV des personnages LOTR
- Labels multilingues via LOTR Fandom Wiki
"""

import os
import csv
import json
import time
import logging
from typing import Optional, List, Dict, Tuple
from difflib import SequenceMatcher
from io import StringIO

import requests
from rdflib import Graph, URIRef, Literal, RDF, RDFS, XSD

from config import (
    TOLKIEN_RESOURCE, TOLKIEN_ONTOLOGY, TOLKIEN_PROPERTY,
    SCHEMA, OUTPUT_DIR, HTTP_HEADERS, REQUEST_TIMEOUT
)

logger = logging.getLogger(__name__)

# URLs des sources externes
METW_CARDS_URL = "https://raw.githubusercontent.com/council-of-elrond-meccg/meccg-cards-database/master/cards.json"
LOTR_FANDOM_API = {
    'en': "https://lotr.fandom.com/api.php",
    'fr': "https://lotr.fandom.com/fr/api.php",
    'de': "https://lotr.fandom.com/de/api.php",
    'es': "https://lotr.fandom.com/es/api.php",
    'it': "https://lotr.fandom.com/it/api.php",
    'pl': "https://lotr.fandom.com/pl/api.php",
    'ru': "https://lotr.fandom.com/ru/api.php",
    'pt': "https://lotr.fandom.com/pt/api.php",
    'nl': "https://lotr.fandom.com/nl/api.php",
    'ja': "https://lotr.fandom.com/ja/api.php",
    'zh': "https://lotr.fandom.com/zh/api.php",
}

# Namespace pour METW
METW = "http://tolkien-kg.org/metw/"


def normalize_name(name: str) -> str:
    """Normalise un nom pour la comparaison."""
    if not name:
        return ""
    name = name.lower().strip()
    name = name.replace("'", "").replace("'", "").replace("-", " ")
    return " ".join(name.split())


def similarity(a: str, b: str) -> float:
    """Calcule la similarite entre deux chaines."""
    return SequenceMatcher(None, normalize_name(a), normalize_name(b)).ratio()


# =============================================================================
# METW CARDS
# =============================================================================

def load_metw_cards(source: str = None) -> List[Dict]:
    """
    Charge les cartes METW depuis l'URL ou un fichier local.
    Le format JSON est imbriqué: {set_id: {cards: {card_id: card_data}}}
    On extrait toutes les cartes dans une liste plate.
    """
    local_file = os.path.join(OUTPUT_DIR, "metw_cards.json")
    
    if source is None:
        if os.path.exists(local_file):
            source = local_file
        else:
            source = METW_CARDS_URL
    
    try:
        if source.startswith('http'):
            logger.info(f"Downloading METW cards from {source}")
            response = requests.get(source, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            raw_data = response.json()
            # Sauvegarder localement
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            with open(local_file, 'w', encoding='utf-8') as f:
                json.dump(raw_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved raw data to {local_file}")
        else:
            logger.info(f"Loading METW cards from {source}")
            with open(source, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
        
        # Extraire toutes les cartes du format imbriqué
        cards = []
        for set_id, set_data in raw_data.items():
            if isinstance(set_data, dict) and 'cards' in set_data:
                # format: {set_id: {cards: {card_id: card_data}}}
                for card_id, card_data in set_data.get('cards', {}).items():
                    if isinstance(card_data, dict):
                        # Extraire le nom en anglais
                        name_data = card_data.get('name', {})
                        if isinstance(name_data, dict):
                            card_data['_name_en'] = name_data.get('en', '')
                        else:
                            card_data['_name_en'] = str(name_data) if name_data else ''
                        
                        # Extraire le texte en anglais
                        text_data = card_data.get('text', {})
                        if isinstance(text_data, dict):
                            card_data['_text_en'] = text_data.get('en', '')
                        else:
                            card_data['_text_en'] = str(text_data) if text_data else ''
                        
                        # Extraire les attributs
                        attrs = card_data.get('attributes', {})
                        if isinstance(attrs, dict):
                            card_data['_prowess'] = attrs.get('prowess')
                            card_data['_body'] = attrs.get('body')
                            card_data['_race'] = attrs.get('race')
                        
                        cards.append(card_data)
            elif isinstance(set_data, dict):
                # liste directe de cartes
                cards.append(set_data)
        
        logger.info(f"Loaded {len(cards)} METW cards")
        return cards
    except Exception as e:
        logger.error(f"Failed to load METW cards: {e}")
        return []


def find_matching_card(name: str, cards: List[Dict], threshold: float = 0.85) -> Optional[Dict]:
    """Trouve la carte correspondant le mieux a un nom."""
    best_match = None
    best_score = 0
    
    for card in cards:
        card_name = card.get('_name_en') or card.get('name') or card.get('Name') or ''
        if isinstance(card_name, dict):
            card_name = card_name.get('en', '')
        if not card_name:
            continue
        
        score = similarity(name, card_name)
        if score > best_score and score >= threshold:
            best_score = score
            best_match = card
    
    return best_match


def enrich_with_metw(graph: Graph, cards: List[Dict], verbose: bool = True) -> Dict[str, int]:
    """Enrichit le graphe avec les donnees METW."""
    stats = {'checked': 0, 'linked': 0, 'triples': 0}
    
    if not cards:
        return stats
    
    if verbose:
        print("\nEnriching with METW cards...")
    
    # Index des cartes par nom normalise
    cards_index = {}
    for card in cards:
        card_name = card.get('_name_en') or card.get('name') or card.get('Name') or ''
        if isinstance(card_name, dict):
            card_name = card_name.get('en', '')
        if card_name:
            cards_index[normalize_name(card_name)] = card
    
    # Parcourir les entites
    for entity_uri in graph.subjects(RDF.type, TOLKIEN_ONTOLOGY.Character):
        stats['checked'] += 1
        
        name = None
        for label in graph.objects(entity_uri, RDFS.label):
            name = str(label)
            break
        
        if not name:
            continue
        
        # Chercher correspondance exacte d'abord
        norm_name = normalize_name(name)
        card = cards_index.get(norm_name)
        
        # Sinon recherche floue
        if not card:
            card = find_matching_card(name, cards, threshold=0.9)
        
        if card:
            stats['linked'] += 1
            card_id = card.get('id') or card.get('code') or norm_name.replace(' ', '_')
            card_uri = URIRef(f"{METW}card/{card_id}")
            
            # Lien entite -> carte
            graph.add((entity_uri, TOLKIEN_PROPERTY.metwCard, card_uri))
            
            # Infos de la carte
            graph.add((card_uri, RDF.type, TOLKIEN_ONTOLOGY.METWCard))
            
            # Nom de la carte
            card_name = card.get('_name_en') or card.get('name') or card.get('Name')
            if isinstance(card_name, dict):
                card_name = card_name.get('en', '')
            if card_name:
                graph.add((card_uri, RDFS.label, Literal(card_name, lang="en")))
            
            # Type de carte
            card_type = card.get('type') or card.get('Type') or card.get('Primary')
            if card_type:
                graph.add((card_uri, TOLKIEN_PROPERTY.cardType, Literal(card_type)))
            
            # Texte de la carte
            card_text = card.get('_text_en') or card.get('text') or card.get('Text')
            if isinstance(card_text, dict):
                card_text = card_text.get('en', '')
            if card_text:
                # Nettoyer le HTML
                import re
                card_text = re.sub(r'<[^>]+>', '', card_text)
                graph.add((card_uri, SCHEMA.description, Literal(card_text, lang="en")))
            
            # Prowess
            prowess = card.get('_prowess') or card.get('prowess') or card.get('Prowess')
            if prowess:
                try:
                    graph.add((card_uri, TOLKIEN_PROPERTY.prowess, 
                               Literal(int(prowess), datatype=XSD.integer)))
                except (ValueError, TypeError):
                    pass
            
            # Body
            body = card.get('_body') or card.get('body') or card.get('Body')
            if body:
                try:
                    graph.add((card_uri, TOLKIEN_PROPERTY.body, 
                               Literal(int(body), datatype=XSD.integer)))
                except (ValueError, TypeError):
                    pass
            
            card_set = card.get('set') or card.get('Set')
            if card_set:
                graph.add((card_uri, TOLKIEN_PROPERTY.cardSet, Literal(card_set)))
            
            if verbose and stats['linked'] <= 20:
                display_name = card_name if isinstance(card_name, str) else str(card_name)
                print(f"  {name} -> {display_name}")
    
    stats['triples'] = stats['linked'] * 5  
    
    if verbose:
        print(f"  Checked: {stats['checked']}, Linked: {stats['linked']}")
    
    return stats


# =============================================================================
# CSV DATA
# =============================================================================

def load_csv_characters(filepath: str) -> List[Dict]:
    """Charge les personnages depuis le fichier CSV."""
    characters = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('name'):
                    characters.append(row)
        logger.info(f"Loaded {len(characters)} characters from CSV")
    except Exception as e:
        logger.error(f"Failed to load CSV: {e}")
    return characters


def enrich_with_csv(graph: Graph, csv_path: str, verbose: bool = True) -> Dict[str, int]:
    """Enrichit le graphe avec les donnees du CSV."""
    stats = {'checked': 0, 'enriched': 0, 'triples': 0}
    
    characters = load_csv_characters(csv_path)
    if not characters:
        return stats
    
    if verbose:
        print("\nEnriching with CSV data...")
    
    # Index par nom normalise
    csv_index = {}
    for char in characters:
        name = char.get('name', '')
        if name:
            csv_index[normalize_name(name)] = char
    
    # Parcourir les entites
    for entity_uri in graph.subjects(RDF.type, TOLKIEN_ONTOLOGY.Character):
        stats['checked'] += 1
        
        name = None
        for label in graph.objects(entity_uri, RDFS.label):
            name = str(label)
            break
        
        if not name:
            continue
        
        csv_data = csv_index.get(normalize_name(name))
        if not csv_data:
            continue
        
        stats['enriched'] += 1
        added = 0
        
        # Ajouter les donnees manquantes
        if csv_data.get('gender'):
            existing = list(graph.objects(entity_uri, SCHEMA.gender))
            if not existing:
                graph.add((entity_uri, SCHEMA.gender, Literal(csv_data['gender'].lower())))
                added += 1
        
        if csv_data.get('race'):
            existing = list(graph.objects(entity_uri, TOLKIEN_PROPERTY.raceLabel))
            if not existing:
                graph.add((entity_uri, TOLKIEN_PROPERTY.raceLabel, Literal(csv_data['race'])))
                added += 1
        
        if csv_data.get('hair'):
            graph.add((entity_uri, TOLKIEN_PROPERTY.hairColor, Literal(csv_data['hair'])))
            added += 1
        
        if csv_data.get('height'):
            graph.add((entity_uri, TOLKIEN_PROPERTY.height, Literal(csv_data['height'])))
            added += 1
        
        if csv_data.get('realm'):
            graph.add((entity_uri, TOLKIEN_PROPERTY.realm, Literal(csv_data['realm'])))
            added += 1
        
        stats['triples'] += added
    
    if verbose:
        print(f"  Checked: {stats['checked']}, Enriched: {stats['enriched']}, Triples: {stats['triples']}")
    
    return stats


# =============================================================================
# MULTILINGUAL LABELS (LOTR Fandom Wiki)
# =============================================================================

# Mapping des codes de langue vers les noms complets
LANGUAGE_NAMES = {
    'en': 'English',
    'fr': 'French',
    'de': 'German',
    'es': 'Spanish',
    'it': 'Italian',
    'pl': 'Polish',
    'ru': 'Russian',
    'pt': 'Portuguese',
    'nl': 'Dutch',
    'ja': 'Japanese',
    'zh': 'Chinese',
}

# Import du namespace SKOS pour les labels alternatifs
from rdflib.namespace import SKOS


def search_fandom_wiki(query: str, lang: str = 'fr') -> Optional[str]:
    """Recherche une page sur le wiki Fandom."""
    api_url = LOTR_FANDOM_API.get(lang)
    if not api_url:
        return None
    
    try:
        params = {
            'action': 'query',
            'list': 'search',
            'srsearch': query,
            'srlimit': 1,
            'format': 'json'
        }
        response = requests.get(api_url, params=params, headers=HTTP_HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        results = data.get('query', {}).get('search', [])
        if results:
            return results[0].get('title')
    except Exception as e:
        logger.debug(f"Fandom search error: {e}")
    
    return None


def enrich_multilingual(graph: Graph, languages: List[str] = None, 
                        max_entities: int = 100, verbose: bool = True) -> Dict[str, int]:
    """
    Ajoute des labels multilingues via LOTR Fandom Wiki.
    
    Les labels traduits sont ajoutés à la fois comme:
    - rdfs:label avec le tag de langue approprié
    - tprop:translatedName pour apparaître dans les propriétés
    
    Args:
        graph: Le graphe RDF à enrichir
        languages: Liste des codes de langue (ex: ['fr', 'de', 'es'])
        max_entities: Nombre maximum d'entités à traiter
        verbose: Afficher la progression
        
    Returns:
        Dictionnaire avec les statistiques
    """
    if languages is None:
        languages = ['fr', 'de', 'es']
    
    stats = {'checked': 0, 'labels_added': 0, 'entities_enriched': 0}
    
    if verbose:
        print(f"\nAdding multilingual labels ({', '.join(languages)})...")
        print(f"  Source: LOTR Fandom Wiki (lotr.fandom.com)")
    
    # Collecter toutes les entités (Characters, Locations, Artifacts)
    entities = []
    
    # Characters
    for entity_uri in graph.subjects(RDF.type, TOLKIEN_ONTOLOGY.Character):
        for label in graph.objects(entity_uri, RDFS.label):
            if not hasattr(label, 'language') or label.language in ['en', None, '']:
                entities.append((entity_uri, str(label), 'Character'))
                break
    
    # Locations
    for entity_uri in graph.subjects(RDF.type, TOLKIEN_ONTOLOGY.Location):
        for label in graph.objects(entity_uri, RDFS.label):
            if not hasattr(label, 'language') or label.language in ['en', None, '']:
                entities.append((entity_uri, str(label), 'Location'))
                break
    
    # Artifacts
    for entity_uri in graph.subjects(RDF.type, TOLKIEN_ONTOLOGY.Artifact):
        for label in graph.objects(entity_uri, RDFS.label):
            if not hasattr(label, 'language') or label.language in ['en', None, '']:
                entities.append((entity_uri, str(label), 'Artifact'))
                break
    
    entities = entities[:max_entities]
    
    if verbose:
        print(f"  Processing {len(entities)} entities...")
    
    for i, (entity_uri, name, entity_type) in enumerate(entities):
        stats['checked'] += 1
        entity_had_new_labels = False
        
        for lang in languages:
            # Vérifier si un label existe déjà pour cette langue
            has_label = False
            for lbl in graph.objects(entity_uri, RDFS.label):
                if hasattr(lbl, 'language') and lbl.language == lang:
                    has_label = True
                    break
            
            if has_label:
                continue
            
            # Rechercher sur Fandom Wiki
            found_title = search_fandom_wiki(name, lang)
            
            if found_title and found_title != name:
                # Ajouter comme rdfs:label avec tag de langue
                graph.add((entity_uri, RDFS.label, Literal(found_title, lang=lang)))
                
                # Ajouter le lien vers la page Fandom (seulement une fois par entité/langue)
                fandom_url = f"https://lotr.fandom.com/{lang}/wiki/{found_title.replace(' ', '_')}"
                graph.add((entity_uri, RDFS.seeAlso, URIRef(fandom_url)))
                
                stats['labels_added'] += 1
                entity_had_new_labels = True
                
                if verbose and stats['labels_added'] <= 15:
                    print(f"  {name} [{lang}] -> {found_title}")
            
            time.sleep(0.5)  # Rate limiting pour respecter l'API
        
        if entity_had_new_labels:
            stats['entities_enriched'] += 1
        
        if verbose and (i + 1) % 20 == 0:
            print(f"  Progress: {i + 1}/{len(entities)} ({stats['labels_added']} labels added)")
    
    if verbose:
        print(f"\n  Summary:")
        print(f"    Entities checked: {stats['checked']}")
        print(f"    Entities enriched: {stats['entities_enriched']}")
        print(f"    Labels added: {stats['labels_added']}")
    
    return stats


# =============================================================================
# MAIN ENRICHMENT FUNCTION
# =============================================================================

def enrich_all(graph: Graph, csv_path: str = None, 
               enable_metw: bool = True,
               enable_csv: bool = True,
               enable_multilingual: bool = True,
               multilingual_limit: int = 50,
               languages: List[str] = None,
               verbose: bool = True) -> Dict[str, Dict]:
    """
    Enrichit le graphe avec toutes les sources externes.
    
    Args:
        graph: Le graphe RDF à enrichir
        csv_path: Chemin vers le fichier CSV optionnel
        enable_metw: Activer l'enrichissement METW cards
        enable_csv: Activer l'enrichissement CSV
        enable_multilingual: Activer les labels multilingues
        multilingual_limit: Nombre max d'entités pour le multilingue
        languages: Liste des codes de langue (défaut: ['fr', 'de', 'es'])
        verbose: Afficher la progression
        
    Returns:
        Dictionnaire avec les résultats de chaque enrichissement
    """
    results = {}
    
    if enable_metw:
        cards = load_metw_cards()
        results['metw'] = enrich_with_metw(graph, cards, verbose=verbose)
    
    if enable_csv and csv_path and os.path.exists(csv_path):
        results['csv'] = enrich_with_csv(graph, csv_path, verbose=verbose)
    
    if enable_multilingual:
        if languages is None:
            languages = ['fr', 'de', 'es']
        results['multilingual'] = enrich_multilingual(
            graph, 
            languages=languages,
            max_entities=multilingual_limit,
            verbose=verbose
        )
    
    return results
