"""
Module de raisonnement SPARQL pour le Knowledge Graph Tolkien.
"""

from typing import List, Dict, Optional, Any
from rdflib import Graph, URIRef

# Préfixes SPARQL standards
SPARQL_PREFIXES = """
PREFIX tolkien: <http://tolkien-kg.org/resource/>
PREFIX tont: <http://tolkien-kg.org/ontology/>
PREFIX tprop: <http://tolkien-kg.org/property/>
PREFIX schema: <http://schema.org/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""


def get_all_classes_query(entity_uri: str) -> str:
    """
    Génère une requête SPARQL qui retourne toutes les classes d'une entité,
    y compris les superclasses définies dans schema.org ou l'ontologie.
    
    Utilise rdfs:subClassOf* (property path) pour la transitivité.
    """
    return f"""{SPARQL_PREFIXES}
SELECT DISTINCT ?class ?classLabel
WHERE {{
    # Classes directes de l'entité
    <{entity_uri}> a ?directClass .
    
    # Superclasses via rdfs:subClassOf* (chemin transitif)
    ?directClass rdfs:subClassOf* ?class .
    
    # Label optionnel de la classe
    OPTIONAL {{ ?class rdfs:label ?classLabel }}
}}
ORDER BY ?class
"""


def get_entity_relations_with_sameas_query(entity_uri: str) -> str:
    """
    Génère une requête SPARQL qui retourne toutes les relations d'une entité,
    EN PRENANT EN COMPTE les triplets owl:sameAs.
    
    Si X owl:sameAs Y, alors pour chaque triplet avec X comme sujet ou objet,
    il existe un triplet équivalent avec Y.
    """
    return f"""{SPARQL_PREFIXES}
SELECT DISTINCT ?subject ?predicate ?object ?source
WHERE {{
    {{
        # Relations directes sortantes
        <{entity_uri}> ?predicate ?object .
        BIND(<{entity_uri}> AS ?subject)
        BIND("direct" AS ?source)
    }}
    UNION
    {{
        # Relations directes entrantes
        ?subject ?predicate <{entity_uri}> .
        BIND(<{entity_uri}> AS ?object)
        BIND("direct" AS ?source)
    }}
    UNION
    {{
        # Relations via owl:sameAs (sortantes) - entité est sujet du sameAs
        <{entity_uri}> owl:sameAs ?sameEntity .
        ?sameEntity ?predicate ?object .
        BIND(?sameEntity AS ?subject)
        BIND("sameAs-outgoing" AS ?source)
        FILTER(?predicate != owl:sameAs)
    }}
    UNION
    {{
        # Relations via owl:sameAs (entrantes) - entité est objet du sameAs
        ?sameEntity owl:sameAs <{entity_uri}> .
        ?sameEntity ?predicate ?object .
        BIND(?sameEntity AS ?subject)
        BIND("sameAs-incoming" AS ?source)
        FILTER(?predicate != owl:sameAs)
    }}
    UNION
    {{
        # Relations où l'objet est équivalent à l'entité
        <{entity_uri}> owl:sameAs ?sameEntity .
        ?subject ?predicate ?sameEntity .
        BIND(?sameEntity AS ?object)
        BIND("sameAs-as-object" AS ?source)
        FILTER(?predicate != owl:sameAs)
    }}
}}
ORDER BY ?predicate
"""


def get_entity_description_with_inference_query(entity_uri: str) -> str:
    """
    Génère une requête SPARQL CONSTRUCT qui retourne une description complète
    de l'entité avec inférence:
    - Toutes les classes (y compris superclasses)
    - Toutes les relations (y compris via owl:sameAs)
    """
    return f"""{SPARQL_PREFIXES}
CONSTRUCT {{
    <{entity_uri}> a ?class .
    <{entity_uri}> ?outPred ?outObj .
    ?inSubj ?inPred <{entity_uri}> .
    <{entity_uri}> owl:sameAs ?sameAs .
}}
WHERE {{
    {{
        # Classes directes et inférées
        <{entity_uri}> a ?directClass .
        ?directClass rdfs:subClassOf* ?class .
    }}
    UNION
    {{
        # Relations sortantes directes
        <{entity_uri}> ?outPred ?outObj .
    }}
    UNION
    {{
        # Relations entrantes directes
        ?inSubj ?inPred <{entity_uri}> .
    }}
    UNION
    {{
        # Équivalences owl:sameAs
        {{ <{entity_uri}> owl:sameAs ?sameAs }}
        UNION
        {{ ?sameAs owl:sameAs <{entity_uri}> }}
    }}
}}
"""


def get_characters_with_inferred_types_query(limit: int = 100) -> str:
    """
    Requête qui liste les personnages avec leurs types inférés
    (y compris schema:Person via rdfs:subClassOf).
    """
    return f"""{SPARQL_PREFIXES}
SELECT DISTINCT ?character ?label ?inferredType
WHERE {{
    ?character a tont:Character ;
               rdfs:label ?label .
    
    # Types inférés via la hiérarchie de classes
    tont:Character rdfs:subClassOf* ?inferredType .
}}
LIMIT {limit}
"""


def get_related_entities_via_sameas_query(entity_uri: str) -> str:
    """
    Trouve toutes les entités liées à travers owl:sameAs.
    """
    return f"""{SPARQL_PREFIXES}
SELECT DISTINCT ?relatedEntity ?predicate ?direction
WHERE {{
    {{
        # Entités équivalentes
        {{ <{entity_uri}> owl:sameAs ?relatedEntity }}
        UNION
        {{ ?relatedEntity owl:sameAs <{entity_uri}> }}
        BIND(owl:sameAs AS ?predicate)
        BIND("equivalent" AS ?direction)
    }}
    UNION
    {{
        # Entités liées via une entité équivalente
        <{entity_uri}> owl:sameAs ?same .
        ?same ?pred ?relatedEntity .
        FILTER(?pred != owl:sameAs)
        FILTER(isIRI(?relatedEntity))
        BIND(?pred AS ?predicate)
        BIND("via-sameAs" AS ?direction)
    }}
}}
"""


def get_dbpedia_enrichment_query(entity_uri: str) -> str:
    """
    Requête fédérée pour enrichir une entité avec des données DBpedia
    (nécessite que Fuseki soit configuré pour les requêtes fédérées).
    """
    return f"""{SPARQL_PREFIXES}
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX dbr: <http://dbpedia.org/resource/>

SELECT ?property ?value
WHERE {{
    # Trouver l'URI DBpedia équivalente
    <{entity_uri}> owl:sameAs ?dbpediaUri .
    FILTER(CONTAINS(STR(?dbpediaUri), "dbpedia.org"))
    
    # Requête fédérée vers DBpedia
    SERVICE <http://dbpedia.org/sparql> {{
        ?dbpediaUri ?property ?value .
        FILTER(LANG(?value) = "en" || !isLiteral(?value))
    }}
}}
LIMIT 50
"""


# =============================================================================
# FONCTIONS D'EXÉCUTION DES REQUÊTES
# =============================================================================

def execute_inference_query(fuseki_client, entity_uri: str) -> Dict[str, Any]:
    """
    Exécute une requête avec inférence et retourne les résultats structurés.
    """
    results = {
        'classes': [],
        'all_classes_with_superclasses': [],
        'relations': [],
        'same_as_entities': []
    }
    
    # 1. Toutes les classes (y compris superclasses)
    classes_query = get_all_classes_query(entity_uri)
    classes_result = fuseki_client.query(classes_query)
    
    if classes_result and 'results' in classes_result:
        for binding in classes_result['results']['bindings']:
            class_uri = binding.get('class', {}).get('value', '')
            class_label = binding.get('classLabel', {}).get('value', '')
            results['all_classes_with_superclasses'].append({
                'uri': class_uri,
                'label': class_label
            })
    
    # 2. Relations avec owl:sameAs
    relations_query = get_entity_relations_with_sameas_query(entity_uri)
    relations_result = fuseki_client.query(relations_query)
    
    if relations_result and 'results' in relations_result:
        for binding in relations_result['results']['bindings']:
            results['relations'].append({
                'subject': binding.get('subject', {}).get('value', ''),
                'predicate': binding.get('predicate', {}).get('value', ''),
                'object': binding.get('object', {}).get('value', ''),
                'source': binding.get('source', {}).get('value', 'direct')
            })
    
    # 3. Entités équivalentes via sameAs
    sameas_query = get_related_entities_via_sameas_query(entity_uri)
    sameas_result = fuseki_client.query(sameas_query)
    
    if sameas_result and 'results' in sameas_result:
        for binding in sameas_result['results']['bindings']:
            results['same_as_entities'].append({
                'entity': binding.get('relatedEntity', {}).get('value', ''),
                'predicate': binding.get('predicate', {}).get('value', ''),
                'direction': binding.get('direction', {}).get('value', '')
            })
    
    return results


# exemples de requêtes pour la documentation

EXAMPLE_QUERIES = {
    "all_classes_with_hierarchy": """
# Toutes les classes d'une entité, y compris les superclasses
# (rdfs:subClassOf* = chemin transitif)

SELECT DISTINCT ?class
WHERE {
    tolkien:Gandalf a ?directClass .
    ?directClass rdfs:subClassOf* ?class .
}
""",

    "relations_with_sameas": """
# Toutes les relations d'une entité, incluant celles via owl:sameAs

SELECT ?predicate ?object
WHERE {
    {
        tolkien:Gandalf ?predicate ?object .
    }
    UNION
    {
        tolkien:Gandalf owl:sameAs ?same .
        ?same ?predicate ?object .
        FILTER(?predicate != owl:sameAs)
    }
}
""",

    "characters_inferred_schema_person": """
# Personnages qui sont aussi des schema:Person (par inférence)

SELECT ?character ?name
WHERE {
    ?character a tont:Character ;
               rdfs:label ?name .
    # Inférence: tont:Character rdfs:subClassOf schema:Person
    # Donc tous les personnages sont aussi des schema:Person
}
""",

    "linked_entities_dbpedia_wikidata": """
# Entités liées à DBpedia et Wikidata via owl:sameAs

SELECT ?entity ?label ?dbpedia ?wikidata
WHERE {
    ?entity rdfs:label ?label .
    OPTIONAL {
        ?entity owl:sameAs ?dbpedia .
        FILTER(CONTAINS(STR(?dbpedia), "dbpedia.org"))
    }
    OPTIONAL {
        ?entity owl:sameAs ?wikidata .
        FILTER(CONTAINS(STR(?wikidata), "wikidata.org"))
    }
    FILTER(BOUND(?dbpedia) || BOUND(?wikidata))
}
LIMIT 50
"""
}
