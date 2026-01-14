"""
Serveur Linked Data pour le Knowledge Graph Tolkien.
Intègre Apache Jena Fuseki comme triplestore.
"""

import os
import sys
import json
import queue
import threading
from urllib.parse import unquote
from typing import Tuple, Optional, List, Dict

from flask import Flask, request, Response, render_template, abort, stream_with_context
from rdflib import Graph, URIRef, RDF, RDFS
from rdflib.namespace import OWL

# Ajouter le repertoire parent au path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    TOLKIEN_RESOURCE, TOLKIEN_ONTOLOGY, TOLKIEN_PROPERTY, SCHEMA, PREFIXES, 
    OUTPUT_DIR, CATEGORIES, FUSEKI_URL, FUSEKI_DATASET, STORAGE_MODE
)
from ontology import create_ontology
from builder import KGBuilder
from rdf_generator import create_graph
from fuseki_client import FusekiClient, get_fuseki_client

app = Flask(__name__)

# Configuration
GRAPH_FILE = os.environ.get("TOLKIEN_GRAPH", os.path.join(OUTPUT_DIR, "tolkien_kg.ttl"))

# Client Fuseki
_fuseki: Optional[FusekiClient] = None
_fallback_graph: Optional[Graph] = None
_use_fuseki: bool = True

# Queue pour la progression du build (par session)
_build_progress_queues: Dict[str, queue.Queue] = {}
# Flags d'annulation par session
_build_cancel_flags: Dict[str, bool] = {}


def get_fuseki() -> FusekiClient:
    """Retourne le client Fuseki."""
    global _fuseki
    if _fuseki is None:
        _fuseki = get_fuseki_client(FUSEKI_URL, FUSEKI_DATASET)
    return _fuseki


def check_fuseki_available() -> bool:
    """Vérifie si Fuseki est disponible."""
    global _use_fuseki
    try:
        fuseki = get_fuseki()
        _use_fuseki = fuseki.is_available()
        if _use_fuseki:
            print(f"✓ Fuseki triplestore available at {FUSEKI_URL}/{FUSEKI_DATASET}")
        else:
            print(f"✗ Fuseki not available, using file-based storage")
        return _use_fuseki
    except Exception as e:
        print(f"✗ Fuseki connection error: {e}")
        _use_fuseki = False
        return False


def get_graph() -> Graph:
    """Retourne le graphe RDF."""
    global _fallback_graph
    
    if _use_fuseki:
        fuseki = get_fuseki()
        return create_graph()
    else:
        if _fallback_graph is None:
            _fallback_graph = create_graph()
            if os.path.exists(GRAPH_FILE):
                _fallback_graph.parse(GRAPH_FILE, format='turtle')
        return _fallback_graph


def reload_graph():
    """Recharge le graphe."""
    global _fallback_graph
    _fallback_graph = None
    return get_graph()


def get_entity_data_fuseki(uri: URIRef) -> Graph:
    """Récupère les données d'une entité depuis Fuseki."""
    fuseki = get_fuseki()
    return fuseki.get_entity(str(uri)) or create_graph()


def get_entity_data_file(uri: URIRef) -> Graph:
    """Récupère les données d'une entité depuis le fichier."""
    g = get_graph()
    data = create_graph()
    for p, o in g.predicate_objects(uri):
        data.add((uri, p, o))
    for s, p in g.subject_predicates(uri):
        data.add((s, p, uri))
    return data


def get_entity_data(uri: URIRef) -> Graph:
    """Récupère les données d'une entité."""
    if _use_fuseki:
        return get_entity_data_fuseki(uri)
    return get_entity_data_file(uri)


def get_entity_external_links_fuseki(entity_uri: str) -> List[Dict]:
    """Récupère les liens externes d'une entité depuis Fuseki."""
    fuseki = get_fuseki()
    links = []
    seen_types = set()  # Éviter les doublons par type
    
    query = f"""
    SELECT ?link ?type WHERE {{
        {{
            <{entity_uri}> owl:sameAs ?link .
            BIND("sameAs" AS ?type)
        }}
        UNION
        {{
            <{entity_uri}> rdfs:seeAlso ?link .
            BIND("seeAlso" AS ?type)
        }}
    }}
    """
    
    results = fuseki.query(query)
    if results and "results" in results:
        for binding in results["results"]["bindings"]:
            link_uri = binding.get('link', {}).get('value', '')
            if link_uri:
                link_type = ""
                # Ordre important : fandom avant wiki pour éviter confusion
                if "lotr.fandom.com" in link_uri or "fandom.com/wiki" in link_uri:
                    link_type = "fandom"
                elif "dbpedia.org" in link_uri:
                    link_type = "dbpedia"
                elif "wikidata.org" in link_uri:
                    link_type = "wikidata"
                elif "yago-knowledge.org" in link_uri:
                    link_type = "yago"
                elif "wikipedia.org" in link_uri:
                    link_type = "wikipedia"
                elif "tolkiengateway.net" in link_uri:
                    link_type = "tgw"  # Tolkien Gateway - on ne l'affiche pas comme externe
                    continue
                
                # Éviter les doublons (un seul lien par type)
                if link_type and link_type not in seen_types:
                    seen_types.add(link_type)
                    links.append({'type': link_type, 'uri': link_uri})
    
    # Vérifier si l'entité a une carte METW
    metw_query = f"""
    ASK {{ <{entity_uri}> tprop:metwCard ?card }}
    """
    metw_result = fuseki.query(metw_query)
    if metw_result and metw_result.get('boolean', False):
        if 'metw' not in seen_types:
            links.append({'type': 'metw', 'uri': '#metw'})
    
    # Vérifier si l'entité a des données CSV (hairColor ou height)
    csv_query = f"""
    ASK {{ {{ <{entity_uri}> tprop:hairColor ?hc }} UNION {{ <{entity_uri}> tprop:height ?ht }} }}
    """
    csv_result = fuseki.query(csv_query)
    if csv_result and csv_result.get('boolean', False):
        if 'csv' not in seen_types:
            links.append({'type': 'csv', 'uri': '#csv'})
    
    return links


def get_entity_external_links_file(uri: URIRef) -> List[Dict]:
    """Récupère les liens externes d'une entité depuis le fichier."""
    g = get_graph()
    links = []
    seen_types = set()  # Éviter les doublons
    
    for o in g.objects(uri, OWL.sameAs):
        ostr = str(o)
        link_type = ""
        if "dbpedia.org" in ostr:
            link_type = "dbpedia"
        elif "wikidata.org" in ostr:
            link_type = "wikidata"
        elif "yago-knowledge.org" in ostr:
            link_type = "yago"
        
        if link_type and link_type not in seen_types:
            seen_types.add(link_type)
            links.append({'type': link_type, 'uri': ostr})
    
    for o in g.objects(uri, RDFS.seeAlso):
        ostr = str(o)
        link_type = ""
        # Ordre important : fandom avant wikipedia
        if "lotr.fandom.com" in ostr or "fandom.com/wiki" in ostr:
            link_type = "fandom"
        elif "wikipedia.org" in ostr:
            link_type = "wikipedia"
        elif "tolkiengateway.net" in ostr:
            continue  # Ne pas afficher comme externe
        
        if link_type and link_type not in seen_types:
            seen_types.add(link_type)
            links.append({'type': link_type, 'uri': ostr})
    
    # Vérifier si l'entité a une carte METW
    if list(g.objects(uri, TOLKIEN_PROPERTY.metwCard)):
        if 'metw' not in seen_types:
            links.append({'type': 'metw', 'uri': '#metw'})
    
    # Vérifier si l'entité a des données CSV
    has_csv = list(g.objects(uri, TOLKIEN_PROPERTY.hairColor)) or list(g.objects(uri, TOLKIEN_PROPERTY.height))
    if has_csv:
        if 'csv' not in seen_types:
            links.append({'type': 'csv', 'uri': '#csv'})
    
    return links


def wants_html() -> bool:
    fmt = request.args.get('format')
    if fmt:
        return fmt.lower() == 'html'
    accept = request.headers.get('Accept', '')
    return 'text/html' in accept and 'text/turtle' not in accept


def wants_raw_rdf() -> bool:
    """Vérifie si l'utilisateur veut le RDF brut."""
    raw = request.args.get('raw')
    if raw:
        return raw.lower() in ['true', '1', 'yes']
    accept = request.headers.get('Accept', '')
    return any(ct in accept for ct in ['text/turtle', 'application/rdf+xml', 'application/ld+json'])


def get_format() -> Tuple[str, str]:
    fmt = request.args.get('format', '').lower()
    if fmt in ['rdfxml', 'xml']:
        return 'xml', 'application/rdf+xml'
    elif fmt in ['jsonld', 'json']:
        return 'json-ld', 'application/ld+json'
    elif fmt in ['nt', 'ntriples']:
        return 'nt', 'application/n-triples'
    return 'turtle', 'text/turtle'


@app.route('/')
def home():
    """Page d'accueil avec statistiques et Sample Entities filtrables."""
    import random
    
    # Récupérer les filtres de source externe
    filter_dbpedia = request.args.get('dbpedia') == '1'
    filter_wikidata = request.args.get('wikidata') == '1'
    filter_yago = request.args.get('yago') == '1'
    filter_wikipedia = request.args.get('wikipedia') == '1'
    filter_metw = request.args.get('metw') == '1'
    filter_fandom = request.args.get('fandom') == '1'
    filter_csv = request.args.get('csv') == '1'
    
    # Récupérer les filtres par catégorie
    filter_cat_character = request.args.get('cat_character') == '1'
    filter_cat_location = request.args.get('cat_location') == '1'
    filter_cat_artifact = request.args.get('cat_artifact') == '1'
    filter_cat_event = request.args.get('cat_event') == '1'
    
    has_source_filters = any([filter_dbpedia, filter_wikidata, filter_yago, filter_wikipedia, filter_metw, filter_fandom, filter_csv])
    has_cat_filters = any([filter_cat_character, filter_cat_location, filter_cat_artifact, filter_cat_event])
    has_filters = has_source_filters or has_cat_filters
    
    # Déterminer les types à récupérer
    if has_cat_filters:
        types_to_fetch = []
        if filter_cat_character:
            types_to_fetch.append(('tont:Character', 'Character'))
        if filter_cat_location:
            types_to_fetch.append(('tont:Location', 'Location'))
        if filter_cat_artifact:
            types_to_fetch.append(('tont:Artifact', 'Artifact'))
        if filter_cat_event:
            types_to_fetch.append(('tont:Battle', 'Battle'))
            types_to_fetch.append(('tont:War', 'War'))
    else:
        # Par défaut, tous les types
        types_to_fetch = [
            ('tont:Character', 'Character'),
            ('tont:Location', 'Location'),
            ('tont:Artifact', 'Artifact'),
            ('tont:Battle', 'Battle'),
            ('tont:War', 'War')
        ]
    
    if _use_fuseki:
        fuseki = get_fuseki()
        stats = fuseki.get_statistics()
        
        # Construire les clauses de filtre par source
        filter_clauses = []
        if filter_dbpedia:
            filter_clauses.append("EXISTS { ?e owl:sameAs ?db . FILTER(CONTAINS(STR(?db), 'dbpedia.org')) }")
        if filter_wikidata:
            filter_clauses.append("EXISTS { ?e owl:sameAs ?wd . FILTER(CONTAINS(STR(?wd), 'wikidata.org')) }")
        if filter_yago:
            filter_clauses.append("EXISTS { ?e owl:sameAs ?yg . FILTER(CONTAINS(STR(?yg), 'yago-knowledge.org')) }")
        if filter_wikipedia:
            filter_clauses.append("EXISTS { ?e rdfs:seeAlso ?wp . FILTER(CONTAINS(STR(?wp), 'wikipedia.org')) }")
        if filter_metw:
            filter_clauses.append("EXISTS { ?e tprop:metwCard ?card }")
        if filter_fandom:
            filter_clauses.append("EXISTS { ?e rdfs:seeAlso ?fa . FILTER(CONTAINS(STR(?fa), 'fandom.com')) }")
        if filter_csv:
            filter_clauses.append("EXISTS { { ?e tprop:hairColor ?hc } UNION { ?e tprop:height ?ht } }")
        
        filter_str = ""
        if filter_clauses:
            filter_str = "FILTER(" + " && ".join(filter_clauses) + ")"
        
        all_entities = []
        
        # Requête pour chaque type sélectionné
        for type_uri, type_label in types_to_fetch:
            query = f"""
                SELECT DISTINCT ?e ?label
                WHERE {{
                    ?e a {type_uri} ;
                       rdfs:label ?label .
                    FILTER(lang(?label) = "en" || lang(?label) = "")
                    {filter_str}
                }}
                LIMIT 50
            """
            
            results = fuseki.query(query)
            
            if results and "results" in results:
                for binding in results["results"]["bindings"]:
                    entity_uri = binding['e']['value']
                    entity_data = {
                        'uri': entity_uri.split('/')[-1],
                        'full_uri': entity_uri,
                        'label': binding['label']['value'],
                        'type': type_label,
                        'external_links': get_entity_external_links_fuseki(entity_uri)
                    }
                    all_entities.append(entity_data)
        
        # Randomiser et prendre 12
        if not has_filters:
            random.shuffle(all_entities)
        entities = all_entities[:12]
        
    else:
        g = get_graph()
        stats = {
            'total': len(g),
            'Character': len(list(g.subjects(RDF.type, TOLKIEN_ONTOLOGY.Character))),
            'Location': len(list(g.subjects(RDF.type, TOLKIEN_ONTOLOGY.Location))),
            'Artifact': len(list(g.subjects(RDF.type, TOLKIEN_ONTOLOGY.Artifact))),
            'Event': len(list(g.subjects(RDF.type, SCHEMA.Event))),
        }
        
        all_entities = []
        
        # Mapping des types pour le mode fichier
        type_mapping = []
        if not has_cat_filters or filter_cat_character:
            type_mapping.append((TOLKIEN_ONTOLOGY.Character, 'Character'))
        if not has_cat_filters or filter_cat_location:
            type_mapping.append((TOLKIEN_ONTOLOGY.Location, 'Location'))
        if not has_cat_filters or filter_cat_artifact:
            type_mapping.append((TOLKIEN_ONTOLOGY.Artifact, 'Artifact'))
        if not has_cat_filters or filter_cat_event:
            type_mapping.append((SCHEMA.Event, 'Event'))
        
        for type_uri, type_label in type_mapping:
            for e in g.subjects(RDF.type, type_uri):
                external_links = get_entity_external_links_file(e)
                link_types = {link['type'] for link in external_links}
                
                # Appliquer les filtres de source
                if filter_dbpedia and 'dbpedia' not in link_types:
                    continue
                if filter_wikidata and 'wikidata' not in link_types:
                    continue
                if filter_yago and 'yago' not in link_types:
                    continue
                if filter_wikipedia and 'wikipedia' not in link_types:
                    continue
                if filter_metw and 'metw' not in link_types:
                    continue
                if filter_fandom and 'fandom' not in link_types:
                    continue
                
                for lbl in g.objects(e, RDFS.label):
                    if not hasattr(lbl, 'language') or lbl.language in ['en', None, '']:
                        entity_data = {
                            'uri': str(e).split('/')[-1],
                            'full_uri': str(e),
                            'label': str(lbl),
                            'type': type_label,
                            'external_links': external_links
                        }
                        all_entities.append(entity_data)
                        break
        
        # Randomiser et prendre 12
        if not has_filters:
            random.shuffle(all_entities)
        entities = all_entities[:12]
    
    # Passer les états des filtres au template
    filters = {
        'dbpedia': filter_dbpedia,
        'wikidata': filter_wikidata,
        'yago': filter_yago,
        'wikipedia': filter_wikipedia,
        'metw': filter_metw,
        'fandom': filter_fandom,
        'csv': filter_csv,
        'cat_character': filter_cat_character,
        'cat_location': filter_cat_location,
        'cat_artifact': filter_cat_artifact,
        'cat_event': filter_cat_event
    }
    
    return render_template('home.html', 
                           stats=stats, 
                           entities=entities,
                           filters=filters,
                           fuseki_mode=_use_fuseki,
                           fuseki_url=f"{FUSEKI_URL}/{FUSEKI_DATASET}")


@app.route('/search')
def search():
    """Recherche d'entités avec déduplication stricte."""
    query = request.args.get('q', '').strip()
    type_filter = request.args.get('type', '')
    
    results = []
    seen_uris = set()  # FIX: Déduplication par URI complète
    
    if query:
        if _use_fuseki:
            fuseki = get_fuseki()
            
            # FIX: Requête qui ne retourne que les entités avec un type connu
            search_query = f"""
            SELECT DISTINCT ?entity (SAMPLE(?lbl) AS ?label) (SAMPLE(?t) AS ?type)
            WHERE {{
                ?entity rdfs:label ?lbl ;
                        a ?t .
                FILTER(CONTAINS(LCASE(STR(?lbl)), LCASE("{query.replace('"', '\\"')}")))
                FILTER(?t IN (tont:Character, tont:Location, tont:Artifact, tont:Battle, tont:War, schema:Event, schema:Person, schema:Place))
            }}
            GROUP BY ?entity
            LIMIT 100
            """
            
            search_results = fuseki.query(search_query)
            
            if search_results and "results" in search_results:
                for binding in search_results["results"]["bindings"]:
                    entity_uri = binding.get('entity', {}).get('value', '')
                    
                    # FIX: Déduplication stricte par URI
                    if entity_uri in seen_uris:
                        continue
                    seen_uris.add(entity_uri)
                    
                    type_uri = binding.get('type', {}).get('value', '')
                    etype = None
                    if "Character" in type_uri or "Person" in type_uri:
                        etype = "character"
                    elif "Location" in type_uri or "Place" in type_uri:
                        etype = "location"
                    elif "Artifact" in type_uri:
                        etype = "artifact"
                    elif "Event" in type_uri or "Battle" in type_uri or "War" in type_uri:
                        etype = "event"
                    
                    if type_filter and etype != type_filter:
                        continue
                    
                    # Récupérer les liens externes pour cette entité
                    external_links = get_entity_external_links_fuseki(entity_uri)
                    
                    results.append({
                        'uri': entity_uri.split('/')[-1],
                        'full_uri': entity_uri,
                        'label': binding.get('label', {}).get('value', ''),
                        'type': etype or 'other',
                        'external_links': external_links
                    })
        else:
            g = get_graph()
            q_lower = query.lower()
            
            # Types valides à rechercher
            valid_types = {
                str(TOLKIEN_ONTOLOGY.Character), str(TOLKIEN_ONTOLOGY.Location),
                str(TOLKIEN_ONTOLOGY.Artifact), str(TOLKIEN_ONTOLOGY.Battle),
                str(TOLKIEN_ONTOLOGY.War), str(SCHEMA.Event), 
                str(SCHEMA.Person), str(SCHEMA.Place)
            }
            
            for uri in g.subjects(predicate=RDFS.label):
                uri_str = str(uri)
                
                # FIX: Déduplication stricte
                if uri_str in seen_uris:
                    continue
                
                # Vérifier si l'entité a un type valide
                entity_types = list(g.objects(uri, RDF.type))
                has_valid_type = any(str(t) in valid_types for t in entity_types)
                
                if not has_valid_type:
                    continue  # Skip les entités sans type valide
                
                for lbl in g.objects(uri, RDFS.label):
                    if q_lower in str(lbl).lower():
                        seen_uris.add(uri_str)
                        
                        etype = None
                        for t in entity_types:
                            ts = str(t)
                            if "Character" in ts or "Person" in ts:
                                etype = "character"
                            elif "Location" in ts or "Place" in ts:
                                etype = "location"
                            elif "Artifact" in ts:
                                etype = "artifact"
                            elif "Event" in ts or "Battle" in ts or "War" in ts:
                                etype = "event"
                        
                        if type_filter and etype != type_filter:
                            break
                        
                        external_links = get_entity_external_links_file(uri)
                        
                        results.append({
                            'uri': uri_str.split('/')[-1],
                            'full_uri': uri_str,
                            'label': str(lbl),
                            'type': etype or 'other',
                            'external_links': external_links
                        })
                        break
    
    return render_template('search.html', query=query, type_filter=type_filter, results=results[:50])


@app.route('/resource/<path:name>')
def resource(name):
    """Page d'une ressource/entité."""
    name = unquote(name)
    uri = TOLKIEN_RESOURCE[name]
    data = get_entity_data(uri)
    
    if len(data) == 0:
        abort(404)
    
    if wants_html():
        label = name.replace('_', ' ')
        for lbl in data.objects(uri, RDFS.label):
            label = str(lbl)
            break
        
        desc = None
        for d in data.objects(uri, SCHEMA.description):
            desc = str(d)
            break
        
        image = None
        for img in data.objects(uri, SCHEMA.image):
            image = str(img)
            break
        
        types = [str(t).split('/')[-1].split('#')[-1] for t in data.objects(uri, RDF.type)]
        
        external_links = []
        properties = []
        labels = []
        metw_card = None
        
        csv_props = {'hairColor', 'height'}
        multilingual_props = {'translatedName'}  # Propriétés multilingues à afficher spécialement
        
        for p, o in data.predicate_objects(uri):
            if p == RDF.type:
                continue
            if p == RDFS.label:
                lang = o.language if hasattr(o, 'language') else 'en'
                labels.append({'lang': lang or 'en', 'value': str(o)})
                continue
            if p == SCHEMA.description or p == SCHEMA.image:
                continue
            
            pname = str(p).split('/')[-1].split('#')[-1]
            
            if p == OWL.sameAs:
                ostr = str(o)
                ltype = ""
                if "dbpedia.org" in ostr:
                    ltype = "dbpedia"
                elif "wikidata.org" in ostr:
                    ltype = "wikidata"
                elif "yago-knowledge.org" in ostr:
                    ltype = "yago"
                if ltype:
                    external_links.append({'type': ltype, 'uri': ostr})
                continue
            
            if p == RDFS.seeAlso:
                ostr = str(o)
                ltype = ""
                # Ordre important : fandom avant les autres
                if "lotr.fandom.com" in ostr or "fandom.com/wiki" in ostr:
                    ltype = "fandom"
                elif "wikipedia.org" in ostr:
                    ltype = "wikipedia"
                elif "tolkiengateway.net" in ostr:
                    ltype = "wiki"
                
                if ltype:
                    external_links.append({'type': ltype, 'uri': ostr})
                continue
            
            if pname == 'metwCard':
                card_uri = o
                metw_card = {'uri': str(card_uri)}
                
                if _use_fuseki:
                    fuseki = get_fuseki()
                    card_data = fuseki.get_entity(str(card_uri))
                    if card_data:
                        for cp, co in card_data.predicate_objects(card_uri):
                            cpname = str(cp).split('/')[-1].split('#')[-1]
                            if cpname == 'label':
                                metw_card['name'] = str(co)
                            elif cpname == 'cardType':
                                metw_card['type'] = str(co)
                            elif cpname == 'description':
                                metw_card['text'] = str(co)
                            elif cpname == 'prowess':
                                metw_card['prowess'] = str(co)
                            elif cpname == 'body':
                                metw_card['body'] = str(co)
                else:
                    g = get_graph()
                    for cp, co in g.predicate_objects(card_uri):
                        cpname = str(cp).split('/')[-1].split('#')[-1]
                        if cpname == 'label':
                            metw_card['name'] = str(co)
                        elif cpname == 'cardType':
                            metw_card['type'] = str(co)
                        elif cpname == 'description':
                            metw_card['text'] = str(co)
                        elif cpname == 'prowess':
                            metw_card['prowess'] = str(co)
                        elif cpname == 'body':
                            metw_card['body'] = str(co)
                continue
            
            source = None
            if pname in csv_props:
                source = 'csv'
            elif pname in multilingual_props:
                source = 'fandom'
            
            if isinstance(o, URIRef):
                oname = str(o).split('/')[-1].replace('_', ' ')
                if str(o).startswith(str(TOLKIEN_RESOURCE)):
                    val = f'<a href="/resource/{str(o).split("/")[-1]}">{oname}</a>'
                else:
                    val = f'<a href="{o}" target="_blank">{oname}</a>'
            else:
                val = str(o)
                # Afficher le tag de langue pour les propriétés multilingues
                if hasattr(o, 'language') and o.language:
                    lang_names = {
                        'en': 'English', 'fr': 'French', 'de': 'German', 
                        'es': 'Spanish', 'it': 'Italian', 'pl': 'Polish',
                        'ru': 'Russian', 'pt': 'Portuguese', 'nl': 'Dutch',
                        'ja': 'Japanese', 'zh': 'Chinese'
                    }
                    lang_display = lang_names.get(o.language, o.language.upper())
                    val += f' <span class="lang-tag">[{lang_display}]</span>'
            
            properties.append({'name': pname, 'value': val, 'source': source})
        
        labels.sort(key=lambda x: (0 if x['lang'] == 'en' else 1, x['lang']))
        
        return render_template('entity.html',
            uri=uri,
            label=label,
            description=desc,
            image=image,
            types=types,
            external_links=external_links,
            labels=labels,
            properties=properties,
            metw_card=metw_card
        )
    else:
        fmt, ctype = get_format()
        rdf_content = data.serialize(format=fmt)
        
        if wants_raw_rdf():
            return Response(rdf_content, mimetype=ctype)
        
        format_names = {'xml': 'RDF/XML', 'json-ld': 'JSON-LD', 'turtle': 'Turtle', 'nt': 'N-Triples'}
        format_keys = {'xml': 'rdfxml', 'json-ld': 'jsonld', 'turtle': 'turtle', 'nt': 'ntriples'}
        
        label = name.replace('_', ' ')
        for lbl in data.objects(uri, RDFS.label):
            label = str(lbl)
            break
        
        return render_template('rdf_view.html',
            label=label,
            uri=str(uri),
            format_name=format_names.get(fmt, fmt.upper()),
            format_key=format_keys.get(fmt, fmt),
            content=rdf_content,
            base_path=f"/resource/{name}"
        )


@app.route('/ontology')
def ontology():
    """Page de l'ontologie."""
    onto = create_ontology()
    
    if wants_html():
        return render_template('ontology.html', turtle=onto.serialize(format='turtle'))
    else:
        fmt, ctype = get_format()
        return Response(onto.serialize(format=fmt), mimetype=ctype)


@app.route('/sparql', methods=['GET', 'POST'])
def sparql():
    """Endpoint SPARQL."""
    default_query = '''PREFIX tolkien: <http://tolkien-kg.org/resource/>
PREFIX tont: <http://tolkien-kg.org/ontology/>
PREFIX schema: <http://schema.org/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

SELECT ?entity ?label ?type
WHERE {
    ?entity a tont:Character ;
            rdfs:label ?label .
    OPTIONAL { ?entity a ?type }
}
LIMIT 20'''
    
    if request.method == 'GET':
        return render_template('sparql.html', 
                               query=default_query,
                               fuseki_mode=_use_fuseki,
                               fuseki_endpoint=f"{FUSEKI_URL}/{FUSEKI_DATASET}/sparql")
    
    query = request.form.get('query', '')
    
    try:
        if _use_fuseki:
            fuseki = get_fuseki()
            results = fuseki.query(query)
            
            if results:
                accept = request.headers.get('Accept', '')
                if 'application/sparql-results+json' in accept:
                    return Response(str(results), mimetype='application/sparql-results+json')
                
                vars_list = results.get('head', {}).get('vars', [])
                bindings = results.get('results', {}).get('bindings', [])
                
                formatted_results = []
                for binding in bindings:
                    row = [binding[var].get('value', '') if var in binding else '' for var in vars_list]
                    formatted_results.append(row)
                
                return render_template('sparql.html', 
                    query=query, vars=vars_list, results=formatted_results,
                    fuseki_mode=_use_fuseki, fuseki_endpoint=f"{FUSEKI_URL}/{FUSEKI_DATASET}/sparql"
                )
            else:
                return render_template('sparql.html', 
                    query=query, error="Query returned no results or failed",
                    fuseki_mode=_use_fuseki, fuseki_endpoint=f"{FUSEKI_URL}/{FUSEKI_DATASET}/sparql"
                )
        else:
            g = get_graph()
            results = g.query(query)
            accept = request.headers.get('Accept', '')
            
            if 'application/sparql-results+json' in accept:
                return Response(results.serialize(format='json'), mimetype='application/sparql-results+json')
            
            return render_template('sparql.html', query=query, vars=results.vars, 
                                   results=list(results), fuseki_mode=_use_fuseki)
    except Exception as e:
        return render_template('sparql.html', query=query, error=str(e),
            fuseki_mode=_use_fuseki,
            fuseki_endpoint=f"{FUSEKI_URL}/{FUSEKI_DATASET}/sparql" if _use_fuseki else None
        )


@app.route('/build', methods=['GET', 'POST'])
def build():
    """Page de construction du KG."""
    if request.method == 'GET':
        return render_template('build.html', categories=CATEGORIES, fuseki_mode=_use_fuseki)
    
    # POST: juste afficher la page, le build se fait via /build/start
    return render_template('build.html', categories=CATEGORIES, fuseki_mode=_use_fuseki)


@app.route('/build/start', methods=['POST'])
def build_start():
    """Démarre le build et retourne un ID de session."""
    import uuid
    
    session_id = str(uuid.uuid4())
    
    selected = request.form.getlist('categories')
    if not selected:
        return {'error': 'Select at least one category.'}, 400
    
    # Récupérer les options d'enrichissement
    enable_multilingual = request.form.get('multilingual') == 'on'
    enable_metw = request.form.get('metw_enrichment') == 'on'
    enable_external_linking = request.form.get('external_linking') == 'on'
    selected_languages = request.form.getlist('languages')
    if not selected_languages:
        selected_languages = ['fr', 'de', 'es']
    
    # Construire la liste des catégories avec leurs limites personnalisées
    cats = []
    for cat_name in selected:
        limit_key = f"limit_{cat_name.replace(' ', '_')}"
        limit_value = request.form.get(limit_key, '50')
        try:
            limit = int(limit_value)
        except ValueError:
            limit = 50
        cats.append((cat_name, limit))
    
    # Créer une queue et un flag d'annulation pour cette session
    _build_progress_queues[session_id] = queue.Queue()
    _build_cancel_flags[session_id] = False
    
    # Lancer le build dans un thread séparé
    def run_build():
        q = _build_progress_queues[session_id]
        
        def check_cancelled():
            return _build_cancel_flags.get(session_id, False)
        
        def progress_callback(step, message, progress, details):
            # Vérifier si annulé
            if check_cancelled():
                raise InterruptedError("Build cancelled by user")
            q.put({
                'step': step,
                'message': message,
                'progress': progress,
                'details': details
            })
        
        try:
            builder = KGBuilder(progress_callback=progress_callback, cancel_check=check_cancelled)
            builder.build(categories=cats, verbose=False)
            
            if check_cancelled():
                raise InterruptedError("Build cancelled by user")
            
            builder.add_ontology(verbose=False)
            
            if check_cancelled():
                raise InterruptedError("Build cancelled by user")
            
            builder.enrich(
                enable_metw=enable_metw,
                enable_csv=False,
                enable_multilingual=enable_multilingual,
                multilingual_limit=50,
                languages=selected_languages if enable_multilingual else None,
                verbose=False
            )
            
            if check_cancelled():
                raise InterruptedError("Build cancelled by user")
            
            output_path = builder.save(verbose=False)
            
            fuseki_success = False
            if _use_fuseki:
                fuseki = get_fuseki()
                fuseki_success = fuseki.load_graph(builder.graph, clear_first=True)
            
            reload_graph()
            
            # Envoyer le résultat final
            q.put({
                'step': 'complete',
                'message': 'Build complete!',
                'progress': 100,
                'details': {
                    'success': True,
                    'stats': builder.stats,
                    'output_path': output_path,
                    'fuseki_loaded': fuseki_success
                }
            })
        except InterruptedError:
            q.put({
                'step': 'cancelled',
                'message': 'Build cancelled.',
                'progress': 0,
                'details': {}
            })
        except Exception as e:
            q.put({
                'step': 'error',
                'message': f'Error: {str(e)}',
                'progress': 0,
                'details': {'error': str(e)}
            })
        finally:
            # Marquer la fin
            q.put(None)
    
    thread = threading.Thread(target=run_build)
    thread.daemon = True
    thread.start()
    
    return {'session_id': session_id}


@app.route('/build/progress/<session_id>')
def build_progress(session_id):
    """Stream SSE de la progression du build."""
    
    def generate():
        if session_id not in _build_progress_queues:
            yield f"data: {json.dumps({'error': 'Session not found'})}\n\n"
            return
        
        q = _build_progress_queues[session_id]
        
        while True:
            try:
                data = q.get(timeout=60)  # Timeout 60s
                if data is None:
                    # Build terminé
                    break
                yield f"data: {json.dumps(data)}\n\n"
            except queue.Empty:
                # Envoyer un heartbeat
                yield f"data: {json.dumps({'step': 'heartbeat', 'message': 'waiting...', 'progress': -1})}\n\n"
        
        # Nettoyer la queue et le flag
        if session_id in _build_progress_queues:
            del _build_progress_queues[session_id]
        if session_id in _build_cancel_flags:
            del _build_cancel_flags[session_id]
    
    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no'
        }
    )


@app.route('/build/cancel/<session_id>', methods=['POST'])
def build_cancel(session_id):
    """Annule un build en cours."""
    if session_id in _build_cancel_flags:
        _build_cancel_flags[session_id] = True
        return {'status': 'cancelled', 'session_id': session_id}
    return {'error': 'Session not found'}, 404


@app.route('/fuseki/status')
def fuseki_status():
    fuseki = get_fuseki()
    available = fuseki.is_available()
    
    if available:
        stats = fuseki.get_statistics()
        return {'status': 'available', 'url': FUSEKI_URL, 'dataset': FUSEKI_DATASET,
                'triples': stats.get('total', 0), 'statistics': stats}
    return {'status': 'unavailable', 'url': FUSEKI_URL, 'dataset': FUSEKI_DATASET}


@app.route('/fuseki/load', methods=['POST'])
def fuseki_load():
    if not _use_fuseki:
        return {'error': 'Fuseki not available'}, 503
    
    fuseki = get_fuseki()
    if os.path.exists(GRAPH_FILE):
        success = fuseki.load_file(GRAPH_FILE, clear_first=True)
        if success:
            return {'status': 'success', 'message': f'Loaded {GRAPH_FILE} into Fuseki'}
        return {'error': 'Failed to load file into Fuseki'}, 500
    return {'error': f'Graph file not found: {GRAPH_FILE}'}, 404


@app.route('/reasoning/<path:name>')
def reasoning(name):
    from reasoning import get_all_classes_query, get_entity_relations_with_sameas_query, EXAMPLE_QUERIES
    
    name = unquote(name)
    uri = TOLKIEN_RESOURCE[name]
    
    results = {'uri': str(uri), 'name': name.replace('_', ' '), 
               'all_classes': [], 'relations_with_sameas': [], 'same_as_entities': []}
    
    if _use_fuseki:
        fuseki = get_fuseki()
        
        classes_result = fuseki.query(get_all_classes_query(str(uri)))
        if classes_result and 'results' in classes_result:
            for binding in classes_result['results']['bindings']:
                class_uri = binding.get('class', {}).get('value', '')
                class_label = binding.get('classLabel', {}).get('value', class_uri.split('/')[-1].split('#')[-1])
                results['all_classes'].append({
                    'uri': class_uri, 'label': class_label,
                    'is_inferred': 'schema.org' in class_uri or 'Thing' in class_uri
                })
        
        relations_result = fuseki.query(get_entity_relations_with_sameas_query(str(uri)))
        if relations_result and 'results' in relations_result:
            for binding in relations_result['results']['bindings']:
                source = binding.get('source', {}).get('value', 'direct')
                results['relations_with_sameas'].append({
                    'subject': binding.get('subject', {}).get('value', ''),
                    'predicate': binding.get('predicate', {}).get('value', ''),
                    'object': binding.get('object', {}).get('value', ''),
                    'source': source, 'is_inferred': 'sameAs' in source
                })
        
        for rel in results['relations_with_sameas']:
            if 'sameAs' in rel['predicate']:
                obj = rel['object']
                link_type = 'unknown'
                if 'dbpedia.org' in obj: link_type = 'dbpedia'
                elif 'wikidata.org' in obj: link_type = 'wikidata'
                elif 'yago' in obj: link_type = 'yago'
                elif 'wikipedia.org' in obj: link_type = 'wikipedia'
                results['same_as_entities'].append({'uri': obj, 'type': link_type})
    
    return render_template('reasoning.html', results=results, example_queries=EXAMPLE_QUERIES)


@app.route('/validate')
def validate_kg():
    from validation import generate_validation_report, create_extended_shacl_shapes
    
    report = None
    if request.args.get('run') == 'true':
        if _use_fuseki:
            fuseki = get_fuseki()
            data_graph = fuseki.construct("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")
            if data_graph:
                report = generate_validation_report(data_graph, verbose=False)
        else:
            report = generate_validation_report(get_graph(), verbose=False)
    
    shapes = create_extended_shacl_shapes()
    return render_template('validate.html', report=report, 
                           shapes_turtle=shapes.serialize(format='turtle'), fuseki_mode=_use_fuseki)


@app.errorhandler(404)
def not_found(e):
    return render_template('base.html'), 404


def run(host='0.0.0.0', port=5000, debug=True):
    print(f"\n{'='*60}")
    print("TOLKIEN KNOWLEDGE GRAPH SERVER")
    print(f"{'='*60}")
    check_fuseki_available()
    print(f"\nStarting server on http://{host}:{port}")
    print(f"Storage mode: {'Fuseki triplestore' if _use_fuseki else 'File-based'}")
    print(f"{'='*60}\n")
    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    run()
