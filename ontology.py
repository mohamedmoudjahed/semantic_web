"""
Ontologie et shapes SHACL pour le Knowledge Graph Tolkien.
"""

from rdflib import Graph, Literal, URIRef, RDF, RDFS, XSD, Namespace
from rdflib.namespace import OWL

from config import TOLKIEN_ONTOLOGY, TOLKIEN_PROPERTY, SCHEMA, PREFIXES


def create_graph() -> Graph:
    g = Graph()
    for prefix, ns in PREFIXES.items():
        g.bind(prefix, ns)
    return g


def create_ontology() -> Graph:
    onto = create_graph()
    
    # Classes
    onto.add((TOLKIEN_ONTOLOGY.Character, RDF.type, OWL.Class))
    onto.add((TOLKIEN_ONTOLOGY.Character, RDFS.subClassOf, SCHEMA.Person))
    onto.add((TOLKIEN_ONTOLOGY.Character, RDFS.label, Literal("Tolkien Character", lang="en")))
    onto.add((TOLKIEN_ONTOLOGY.Character, RDFS.comment, Literal("A fictional character from Tolkien's legendarium", lang="en")))
    
    onto.add((TOLKIEN_ONTOLOGY.Location, RDF.type, OWL.Class))
    onto.add((TOLKIEN_ONTOLOGY.Location, RDFS.subClassOf, SCHEMA.Place))
    onto.add((TOLKIEN_ONTOLOGY.Location, RDFS.label, Literal("Tolkien Location", lang="en")))
    
    onto.add((TOLKIEN_ONTOLOGY.Artifact, RDF.type, OWL.Class))
    onto.add((TOLKIEN_ONTOLOGY.Artifact, RDFS.subClassOf, SCHEMA.Thing))
    onto.add((TOLKIEN_ONTOLOGY.Artifact, RDFS.label, Literal("Tolkien Artifact", lang="en")))
    
    onto.add((TOLKIEN_ONTOLOGY.Battle, RDF.type, OWL.Class))
    onto.add((TOLKIEN_ONTOLOGY.Battle, RDFS.subClassOf, SCHEMA.Event))
    onto.add((TOLKIEN_ONTOLOGY.Battle, RDFS.label, Literal("Battle", lang="en")))
    
    onto.add((TOLKIEN_ONTOLOGY.War, RDF.type, OWL.Class))
    onto.add((TOLKIEN_ONTOLOGY.War, RDFS.subClassOf, SCHEMA.Event))
    onto.add((TOLKIEN_ONTOLOGY.War, RDFS.label, Literal("War", lang="en")))
    
    onto.add((TOLKIEN_ONTOLOGY.Race, RDF.type, OWL.Class))
    onto.add((TOLKIEN_ONTOLOGY.Race, RDFS.label, Literal("Race/People", lang="en")))
    
    # Properties
    onto.add((TOLKIEN_ONTOLOGY.race, RDF.type, OWL.ObjectProperty))
    onto.add((TOLKIEN_ONTOLOGY.race, RDFS.domain, TOLKIEN_ONTOLOGY.Character))
    onto.add((TOLKIEN_ONTOLOGY.race, RDFS.range, TOLKIEN_ONTOLOGY.Race))
    onto.add((TOLKIEN_ONTOLOGY.race, RDFS.label, Literal("race", lang="en")))
    
    onto.add((TOLKIEN_PROPERTY.father, RDF.type, OWL.ObjectProperty))
    onto.add((TOLKIEN_PROPERTY.father, RDFS.subPropertyOf, SCHEMA.parent))
    onto.add((TOLKIEN_PROPERTY.father, RDFS.label, Literal("father", lang="en")))
    
    onto.add((TOLKIEN_PROPERTY.mother, RDF.type, OWL.ObjectProperty))
    onto.add((TOLKIEN_PROPERTY.mother, RDFS.subPropertyOf, SCHEMA.parent))
    onto.add((TOLKIEN_PROPERTY.mother, RDFS.label, Literal("mother", lang="en")))
    
    onto.add((TOLKIEN_PROPERTY.realm, RDF.type, OWL.ObjectProperty))
    onto.add((TOLKIEN_PROPERTY.realm, RDFS.domain, TOLKIEN_ONTOLOGY.Location))
    onto.add((TOLKIEN_PROPERTY.realm, RDFS.label, Literal("realm", lang="en")))
    
    onto.add((TOLKIEN_PROPERTY.destroyedDate, RDF.type, OWL.DatatypeProperty))
    onto.add((TOLKIEN_PROPERTY.destroyedDate, RDFS.label, Literal("destroyed date", lang="en")))
    
    onto.add((TOLKIEN_PROPERTY.result, RDF.type, OWL.DatatypeProperty))
    onto.add((TOLKIEN_PROPERTY.result, RDFS.domain, SCHEMA.Event))
    onto.add((TOLKIEN_PROPERTY.result, RDFS.label, Literal("result", lang="en")))
    
    onto.add((TOLKIEN_PROPERTY.objectType, RDF.type, OWL.DatatypeProperty))
    onto.add((TOLKIEN_PROPERTY.objectType, RDFS.label, Literal("object type", lang="en")))
    
    onto.add((TOLKIEN_PROPERTY.ownedBy, RDF.type, OWL.ObjectProperty))
    onto.add((TOLKIEN_PROPERTY.ownedBy, RDFS.label, Literal("owned by", lang="en")))
    
    onto.add((TOLKIEN_PROPERTY.raceLabel, RDF.type, OWL.DatatypeProperty))
    onto.add((TOLKIEN_PROPERTY.raceLabel, RDFS.label, Literal("race label", lang="en")))
    
    # METW Card class
    onto.add((TOLKIEN_ONTOLOGY.METWCard, RDF.type, OWL.Class))
    onto.add((TOLKIEN_ONTOLOGY.METWCard, RDFS.label, Literal("METW Card", lang="en")))
    onto.add((TOLKIEN_ONTOLOGY.METWCard, RDFS.comment, 
              Literal("A card from Middle Earth: The Wizards collectible card game", lang="en")))
    
    # METW properties
    onto.add((TOLKIEN_PROPERTY.metwCard, RDF.type, OWL.ObjectProperty))
    onto.add((TOLKIEN_PROPERTY.metwCard, RDFS.label, Literal("METW card", lang="en")))
    onto.add((TOLKIEN_PROPERTY.metwCard, RDFS.range, TOLKIEN_ONTOLOGY.METWCard))
    
    onto.add((TOLKIEN_PROPERTY.cardType, RDF.type, OWL.DatatypeProperty))
    onto.add((TOLKIEN_PROPERTY.cardType, RDFS.label, Literal("card type", lang="en")))
    
    onto.add((TOLKIEN_PROPERTY.cardSet, RDF.type, OWL.DatatypeProperty))
    onto.add((TOLKIEN_PROPERTY.cardSet, RDFS.label, Literal("card set", lang="en")))
    
    onto.add((TOLKIEN_PROPERTY.prowess, RDF.type, OWL.DatatypeProperty))
    onto.add((TOLKIEN_PROPERTY.prowess, RDFS.label, Literal("prowess", lang="en")))
    onto.add((TOLKIEN_PROPERTY.prowess, RDFS.range, XSD.integer))
    
    onto.add((TOLKIEN_PROPERTY.body, RDF.type, OWL.DatatypeProperty))
    onto.add((TOLKIEN_PROPERTY.body, RDFS.label, Literal("body", lang="en")))
    onto.add((TOLKIEN_PROPERTY.body, RDFS.range, XSD.integer))
    
    # CSV enrichment properties
    onto.add((TOLKIEN_PROPERTY.hairColor, RDF.type, OWL.DatatypeProperty))
    onto.add((TOLKIEN_PROPERTY.hairColor, RDFS.label, Literal("hair color", lang="en")))
    
    onto.add((TOLKIEN_PROPERTY.height, RDF.type, OWL.DatatypeProperty))
    onto.add((TOLKIEN_PROPERTY.height, RDFS.label, Literal("height", lang="en")))
    
    # Multilingual properties
    onto.add((TOLKIEN_PROPERTY.translatedName, RDF.type, OWL.DatatypeProperty))
    onto.add((TOLKIEN_PROPERTY.translatedName, RDFS.label, Literal("translated name", lang="en")))
    onto.add((TOLKIEN_PROPERTY.translatedName, RDFS.comment, 
              Literal("Name of the entity in a different language (from LOTR Fandom Wiki)", lang="en")))
    
    return onto


def create_shacl_shapes() -> Graph:
    shapes = create_graph()
    SH = Namespace("http://www.w3.org/ns/shacl#")
    shapes.bind("sh", SH)
    
    # Character Shape
    cs = TOLKIEN_ONTOLOGY.CharacterShape
    shapes.add((cs, RDF.type, SH.NodeShape))
    shapes.add((cs, SH.targetClass, TOLKIEN_ONTOLOGY.Character))
    shapes.add((cs, RDFS.label, Literal("Shape for Tolkien Characters")))
    
    cs_label = URIRef(str(cs) + "_label")
    shapes.add((cs, SH.property, cs_label))
    shapes.add((cs_label, SH.path, RDFS.label))
    shapes.add((cs_label, SH.minCount, Literal(1, datatype=XSD.integer)))
    
    # Location Shape
    ls = TOLKIEN_ONTOLOGY.LocationShape
    shapes.add((ls, RDF.type, SH.NodeShape))
    shapes.add((ls, SH.targetClass, TOLKIEN_ONTOLOGY.Location))
    shapes.add((ls, RDFS.label, Literal("Shape for Tolkien Locations")))
    
    ls_label = URIRef(str(ls) + "_label")
    shapes.add((ls, SH.property, ls_label))
    shapes.add((ls_label, SH.path, RDFS.label))
    shapes.add((ls_label, SH.minCount, Literal(1, datatype=XSD.integer)))
    
    # Artifact Shape
    as_ = TOLKIEN_ONTOLOGY.ArtifactShape
    shapes.add((as_, RDF.type, SH.NodeShape))
    shapes.add((as_, SH.targetClass, TOLKIEN_ONTOLOGY.Artifact))
    shapes.add((as_, RDFS.label, Literal("Shape for Tolkien Artifacts")))
    
    as_label = URIRef(str(as_) + "_label")
    shapes.add((as_, SH.property, as_label))
    shapes.add((as_label, SH.path, RDFS.label))
    shapes.add((as_label, SH.minCount, Literal(1, datatype=XSD.integer)))
    
    # Event Shape
    es = TOLKIEN_ONTOLOGY.EventShape
    shapes.add((es, RDF.type, SH.NodeShape))
    shapes.add((es, SH.targetClass, SCHEMA.Event))
    shapes.add((es, RDFS.label, Literal("Shape for Events")))
    
    es_label = URIRef(str(es) + "_label")
    shapes.add((es, SH.property, es_label))
    shapes.add((es_label, SH.path, RDFS.label))
    shapes.add((es_label, SH.minCount, Literal(1, datatype=XSD.integer)))
    
    return shapes
