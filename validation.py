"""
Module de validation SHACL pour le Knowledge Graph Tolkien.

Valide le graphe RDF contre les shapes SHACL définies dans l'ontologie.
"""

import logging
from typing import Tuple, Optional, Dict, List
from rdflib import Graph, Namespace, RDF, RDFS, Literal, URIRef, XSD

from config import TOLKIEN_ONTOLOGY, TOLKIEN_PROPERTY, SCHEMA, PREFIXES
from ontology import create_shacl_shapes

logger = logging.getLogger(__name__)

# Namespace SHACL
SH = Namespace("http://www.w3.org/ns/shacl#")


def create_extended_shacl_shapes() -> Graph:
    """
    Crée des shapes SHACL étendues basées sur les templates d'infobox.
    Plus détaillées que les shapes de base.
    """
    shapes = Graph()
    for prefix, ns in PREFIXES.items():
        shapes.bind(prefix, ns)
    shapes.bind("sh", SH)
    
    # character shape (basé sur Infobox character)
    cs = TOLKIEN_ONTOLOGY.CharacterShape
    shapes.add((cs, RDF.type, SH.NodeShape))
    shapes.add((cs, SH.targetClass, TOLKIEN_ONTOLOGY.Character))
    shapes.add((cs, RDFS.label, Literal("Validation shape for Tolkien Characters")))
    shapes.add((cs, RDFS.comment, Literal("Derived from Tolkien Gateway Infobox character template")))
    
    # Propriété rdfs:label (obligatoire)
    cs_label = URIRef(str(cs) + "_label")
    shapes.add((cs, SH.property, cs_label))
    shapes.add((cs_label, SH.path, RDFS.label))
    shapes.add((cs_label, SH.minCount, Literal(1, datatype=XSD.integer)))
    shapes.add((cs_label, SH.datatype, RDF.langString))
    shapes.add((cs_label, SH.name, Literal("Label")))
    shapes.add((cs_label, SH.description, Literal("Character must have at least one label")))
    
    # Propriété schema:name (optionnel)
    cs_name = URIRef(str(cs) + "_name")
    shapes.add((cs, SH.property, cs_name))
    shapes.add((cs_name, SH.path, SCHEMA.name))
    shapes.add((cs_name, SH.maxCount, Literal(1, datatype=XSD.integer)))
    shapes.add((cs_name, SH.datatype, RDF.langString))
    
    # Propriété schema:gender (optionnel, valeurs limitées)
    cs_gender = URIRef(str(cs) + "_gender")
    shapes.add((cs, SH.property, cs_gender))
    shapes.add((cs_gender, SH.path, SCHEMA.gender))
    shapes.add((cs_gender, SH.maxCount, Literal(1, datatype=XSD.integer)))
    shapes.add((cs_gender, SH["in"], URIRef(str(cs) + "_gender_values")))
    
    # Liste des valeurs autorisées pour gender
    gender_list = URIRef(str(cs) + "_gender_values")
    shapes.add((gender_list, RDF.first, Literal("male")))
    shapes.add((gender_list, RDF.rest, URIRef(str(cs) + "_gender_values_2")))
    shapes.add((URIRef(str(cs) + "_gender_values_2"), RDF.first, Literal("female")))
    shapes.add((URIRef(str(cs) + "_gender_values_2"), RDF.rest, RDF.nil))
    
    # Propriété tont:race (optionnel, doit être une URI)
    cs_race = URIRef(str(cs) + "_race")
    shapes.add((cs, SH.property, cs_race))
    shapes.add((cs_race, SH.path, TOLKIEN_ONTOLOGY.race))
    shapes.add((cs_race, SH.nodeKind, SH.IRI))
    
    # Propriété schema:birthDate (optionnel)
    cs_birth = URIRef(str(cs) + "_birthDate")
    shapes.add((cs, SH.property, cs_birth))
    shapes.add((cs_birth, SH.path, SCHEMA.birthDate))
    shapes.add((cs_birth, SH.maxCount, Literal(1, datatype=XSD.integer)))
    
    # Propriété schema:deathDate (optionnel)
    cs_death = URIRef(str(cs) + "_deathDate")
    shapes.add((cs, SH.property, cs_death))
    shapes.add((cs_death, SH.path, SCHEMA.deathDate))
    shapes.add((cs_death, SH.maxCount, Literal(1, datatype=XSD.integer)))
    
    # =========================================================================
    # LOCATION SHAPE (basé sur Infobox location)
    # =========================================================================
    ls = TOLKIEN_ONTOLOGY.LocationShape
    shapes.add((ls, RDF.type, SH.NodeShape))
    shapes.add((ls, SH.targetClass, TOLKIEN_ONTOLOGY.Location))
    shapes.add((ls, RDFS.label, Literal("Validation shape for Tolkien Locations")))
    
    # Label obligatoire
    ls_label = URIRef(str(ls) + "_label")
    shapes.add((ls, SH.property, ls_label))
    shapes.add((ls_label, SH.path, RDFS.label))
    shapes.add((ls_label, SH.minCount, Literal(1, datatype=XSD.integer)))
    
    # Propriété schema:containedInPlace (optionnel, URI)
    ls_contained = URIRef(str(ls) + "_containedIn")
    shapes.add((ls, SH.property, ls_contained))
    shapes.add((ls_contained, SH.path, SCHEMA.containedInPlace))
    shapes.add((ls_contained, SH.nodeKind, SH.IRI))
    
    # Propriété tprop:realm (optionnel, URI)
    ls_realm = URIRef(str(ls) + "_realm")
    shapes.add((ls, SH.property, ls_realm))
    shapes.add((ls_realm, SH.path, TOLKIEN_PROPERTY.realm))
    shapes.add((ls_realm, SH.nodeKind, SH.IRI))
    
    # =========================================================================
    # ARTIFACT SHAPE (basé sur Infobox object)
    # =========================================================================
    as_ = TOLKIEN_ONTOLOGY.ArtifactShape
    shapes.add((as_, RDF.type, SH.NodeShape))
    shapes.add((as_, SH.targetClass, TOLKIEN_ONTOLOGY.Artifact))
    shapes.add((as_, RDFS.label, Literal("Validation shape for Tolkien Artifacts")))
    
    # Label obligatoire
    as_label = URIRef(str(as_) + "_label")
    shapes.add((as_, SH.property, as_label))
    shapes.add((as_label, SH.path, RDFS.label))
    shapes.add((as_label, SH.minCount, Literal(1, datatype=XSD.integer)))
    
    # Propriété tprop:objectType (optionnel)
    as_type = URIRef(str(as_) + "_objectType")
    shapes.add((as_, SH.property, as_type))
    shapes.add((as_type, SH.path, TOLKIEN_PROPERTY.objectType))
    shapes.add((as_type, SH.maxCount, Literal(1, datatype=XSD.integer)))
    
    # Propriété schema:creator (optionnel, URI)
    as_creator = URIRef(str(as_) + "_creator")
    shapes.add((as_, SH.property, as_creator))
    shapes.add((as_creator, SH.path, SCHEMA.creator))
    shapes.add((as_creator, SH.nodeKind, SH.IRI))
    
    # EVENT SHAPE (basé sur Infobox battle/war)
    es = TOLKIEN_ONTOLOGY.EventShape
    shapes.add((es, RDF.type, SH.NodeShape))
    shapes.add((es, SH.targetClass, SCHEMA.Event))
    shapes.add((es, RDFS.label, Literal("Validation shape for Events")))
    
    # Label obligatoire
    es_label = URIRef(str(es) + "_label")
    shapes.add((es, SH.property, es_label))
    shapes.add((es_label, SH.path, RDFS.label))
    shapes.add((es_label, SH.minCount, Literal(1, datatype=XSD.integer)))
    
    # Propriété schema:startDate (optionnel)
    es_date = URIRef(str(es) + "_startDate")
    shapes.add((es, SH.property, es_date))
    shapes.add((es_date, SH.path, SCHEMA.startDate))
    shapes.add((es_date, SH.maxCount, Literal(1, datatype=XSD.integer)))
    
    # Propriété schema:location (optionnel, URI)
    es_location = URIRef(str(es) + "_location")
    shapes.add((es, SH.property, es_location))
    shapes.add((es_location, SH.path, SCHEMA.location))
    shapes.add((es_location, SH.nodeKind, SH.IRI))
    
    # METW CARD SHAPE
    ms = TOLKIEN_ONTOLOGY.METWCardShape
    shapes.add((ms, RDF.type, SH.NodeShape))
    shapes.add((ms, SH.targetClass, TOLKIEN_ONTOLOGY.METWCard))
    shapes.add((ms, RDFS.label, Literal("Validation shape for METW Cards")))
    
    # Label obligatoire
    ms_label = URIRef(str(ms) + "_label")
    shapes.add((ms, SH.property, ms_label))
    shapes.add((ms_label, SH.path, RDFS.label))
    shapes.add((ms_label, SH.minCount, Literal(1, datatype=XSD.integer)))
    
    # Propriété tprop:prowess (optionnel, integer)
    ms_prowess = URIRef(str(ms) + "_prowess")
    shapes.add((ms, SH.property, ms_prowess))
    shapes.add((ms_prowess, SH.path, TOLKIEN_PROPERTY.prowess))
    shapes.add((ms_prowess, SH.datatype, XSD.integer))
    shapes.add((ms_prowess, SH.maxCount, Literal(1, datatype=XSD.integer)))
    
    # Propriété tprop:body (optionnel, integer)
    ms_body = URIRef(str(ms) + "_body")
    shapes.add((ms, SH.property, ms_body))
    shapes.add((ms_body, SH.path, TOLKIEN_PROPERTY.body))
    shapes.add((ms_body, SH.datatype, XSD.integer))
    shapes.add((ms_body, SH.maxCount, Literal(1, datatype=XSD.integer)))
    
    return shapes


def validate_graph_simple(data_graph: Graph, shapes_graph: Graph = None) -> Tuple[bool, List[Dict]]:
    """
    Validation simplifiée du graphe (sans pyshacl).
    Vérifie les contraintes de base manuellement.
    
    Returns:
        Tuple (conforms, violations)
    """
    if shapes_graph is None:
        shapes_graph = create_extended_shacl_shapes()
    
    violations = []
    
    # Vérifier que chaque Character a un label
    for char in data_graph.subjects(RDF.type, TOLKIEN_ONTOLOGY.Character):
        labels = list(data_graph.objects(char, RDFS.label))
        if not labels:
            violations.append({
                'entity': str(char),
                'type': 'Character',
                'violation': 'Missing required rdfs:label',
                'severity': 'error'
            })
    
    # Vérifier que chaque Location a un label
    for loc in data_graph.subjects(RDF.type, TOLKIEN_ONTOLOGY.Location):
        labels = list(data_graph.objects(loc, RDFS.label))
        if not labels:
            violations.append({
                'entity': str(loc),
                'type': 'Location',
                'violation': 'Missing required rdfs:label',
                'severity': 'error'
            })
    
    # Vérifier que chaque Artifact a un label
    for art in data_graph.subjects(RDF.type, TOLKIEN_ONTOLOGY.Artifact):
        labels = list(data_graph.objects(art, RDFS.label))
        if not labels:
            violations.append({
                'entity': str(art),
                'type': 'Artifact',
                'violation': 'Missing required rdfs:label',
                'severity': 'error'
            })
    
    # Vérifier les valeurs de gender
    for entity in data_graph.subjects(RDF.type, TOLKIEN_ONTOLOGY.Character):
        for gender in data_graph.objects(entity, SCHEMA.gender):
            gender_val = str(gender).lower()
            if gender_val not in ['male', 'female']:
                violations.append({
                    'entity': str(entity),
                    'type': 'Character',
                    'violation': f'Invalid gender value: {gender_val}',
                    'severity': 'warning'
                })
    
    # Vérifier que les références sont des URI
    for entity in data_graph.subjects(RDF.type, TOLKIEN_ONTOLOGY.Character):
        for parent in data_graph.objects(entity, SCHEMA.parent):
            if not isinstance(parent, URIRef):
                violations.append({
                    'entity': str(entity),
                    'type': 'Character',
                    'violation': f'schema:parent should be a URI, got: {type(parent).__name__}',
                    'severity': 'warning'
                })
    
    conforms = len([v for v in violations if v['severity'] == 'error']) == 0
    
    return conforms, violations


def validate_with_pyshacl(data_graph: Graph, shapes_graph: Graph = None) -> Tuple[bool, str, Graph]:
    """
    Validation complète avec pyshacl (si disponible).
    
    Returns:
        Tuple (conforms, results_text, results_graph)
    """
    try:
        from pyshacl import validate
        
        if shapes_graph is None:
            shapes_graph = create_extended_shacl_shapes()
        
        conforms, results_graph, results_text = validate(
            data_graph,
            shacl_graph=shapes_graph,
            inference='rdfs',  # Active l'inférence RDFS
            abort_on_first=False
        )
        
        return conforms, results_text, results_graph
        
    except ImportError:
        logger.warning("pyshacl not installed, using simple validation")
        conforms, violations = validate_graph_simple(data_graph, shapes_graph)
        
        # Formater les résultats comme du texte
        if violations:
            results_text = "Validation Results:\n"
            results_text += f"Conforms: {conforms}\n\n"
            for v in violations:
                results_text += f"- [{v['severity'].upper()}] {v['entity']}: {v['violation']}\n"
        else:
            results_text = "Validation Results:\nConforms: True\nNo violations found."
        
        return conforms, results_text, Graph()


def generate_validation_report(data_graph: Graph, verbose: bool = True) -> Dict:
    """
    Génère un rapport de validation complet.
    """
    report = {
        'conforms': True,
        'statistics': {
            'total_triples': len(data_graph),
            'characters': 0,
            'locations': 0,
            'artifacts': 0,
            'events': 0
        },
        'violations': [],
        'warnings': []
    }
    
    # Statistiques
    report['statistics']['characters'] = len(list(data_graph.subjects(RDF.type, TOLKIEN_ONTOLOGY.Character)))
    report['statistics']['locations'] = len(list(data_graph.subjects(RDF.type, TOLKIEN_ONTOLOGY.Location)))
    report['statistics']['artifacts'] = len(list(data_graph.subjects(RDF.type, TOLKIEN_ONTOLOGY.Artifact)))
    report['statistics']['events'] = len(list(data_graph.subjects(RDF.type, SCHEMA.Event)))
    
    # Validation
    conforms, violations = validate_graph_simple(data_graph)
    report['conforms'] = conforms
    
    for v in violations:
        if v['severity'] == 'error':
            report['violations'].append(v)
        else:
            report['warnings'].append(v)
    
    if verbose:
        print("\n" + "=" * 60)
        print("SHACL VALIDATION REPORT")
        print("=" * 60)
        print(f"Total triples: {report['statistics']['total_triples']}")
        print(f"Characters: {report['statistics']['characters']}")
        print(f"Locations: {report['statistics']['locations']}")
        print(f"Artifacts: {report['statistics']['artifacts']}")
        print(f"Events: {report['statistics']['events']}")
        print(f"\nConforms: {report['conforms']}")
        print(f"Violations: {len(report['violations'])}")
        print(f"Warnings: {len(report['warnings'])}")
        
        if report['violations']:
            print("\nViolations:")
            for v in report['violations'][:10]:
                print(f"  - {v['entity'].split('/')[-1]}: {v['violation']}")
        
        if report['warnings']:
            print("\nWarnings:")
            for w in report['warnings'][:10]:
                print(f"  - {w['entity'].split('/')[-1]}: {w['violation']}")
    
    return report
