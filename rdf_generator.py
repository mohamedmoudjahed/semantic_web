"""
Generateur RDF pour les infobox Tolkien Gateway.
Utilise le linking dynamique pour découvrir les liens externes.
"""

from typing import Dict, Optional
from rdflib import Graph, Literal, URIRef, RDF, RDFS, XSD
from rdflib.namespace import OWL, FOAF

from config import (
    TOLKIEN_RESOURCE, TOLKIEN_PAGE, TOLKIEN_ONTOLOGY, TOLKIEN_PROPERTY,
    SCHEMA, PREFIXES, ENABLE_DYNAMIC_LINKING
)
from wiki import (
    clean_wikitext, clean_entity_name, extract_internal_links,
    split_on_br, clean_date_field, is_valid_date, build_image_url
)
import mwparserfromhell

try:
    from linking import discover_external_links
    LINKING_AVAILABLE = True
except ImportError:
    LINKING_AVAILABLE = False
    discover_external_links = None


def create_graph() -> Graph:
    g = Graph()
    for prefix, ns in PREFIXES.items():
        g.bind(prefix, ns)
    return g


def extract_infobox(wikitext: str):
    if not wikitext:
        return None
    try:
        wikicode = mwparserfromhell.parse(wikitext)
        for template in wikicode.filter_templates():
            template_name = str(template.name).strip().lower()
            # Reconnaître les infobox classiques ET les templates campaign/battle
            if "infobox" in template_name or template_name in ["campaign", "battle"]:
                return template
    except Exception:
        pass
    return None


def detect_type(infobox) -> str:
    name = str(infobox.name).strip().lower()
    # Templates spéciaux pour les conflits
    if name == "campaign":
        return "war"
    if name == "battle":
        return "battle"
    # Infobox classiques
    for t in ["character", "place", "location", "object", "weapon", "artifact", "book", "event", "battle", "war", "conflict"]:
        if t in name:
            # "conflict" est traité comme "war" ou "battle"
            if t == "conflict":
                return "war"
            return t
    return "unknown"


def get_params(infobox) -> Dict[str, str]:
    params = {}
    for p in infobox.params:
        name = str(p.name).strip().lower()
        value = str(p.value).strip()
        if value:
            params[name] = value
    return params


class RDFGenerator:
    def __init__(self):
        self.graph = create_graph()
        # Cache pour les liens externes découverts
        self._external_links_cache = {}

    def reset(self):
        self.graph = create_graph()

    def uri(self, title: str) -> URIRef:
        return TOLKIEN_RESOURCE[clean_entity_name(title)]

    def page_uri(self, title: str) -> URIRef:
        return TOLKIEN_PAGE[clean_entity_name(title)]

    def add_base(self, entity: URIRef, page: URIRef, title: str):
        self.graph.add((entity, FOAF.isPrimaryTopicOf, page))
        self.graph.add((page, FOAF.primaryTopic, entity))
        self.graph.add((page, RDF.type, FOAF.Document))
        self.graph.add((entity, RDFS.label, Literal(title, lang="en")))
        wiki_url = f"https://tolkiengateway.net/wiki/{title.replace(' ', '_')}"
        self.graph.add((page, SCHEMA.url, URIRef(wiki_url)))
        self.graph.add((entity, RDFS.seeAlso, URIRef(wiki_url)))

    def add_types(self, entity: URIRef, itype: str):
        if itype == "character":
            self.graph.add((entity, RDF.type, SCHEMA.Person))
            self.graph.add((entity, RDF.type, TOLKIEN_ONTOLOGY.Character))
        elif itype in ["place", "location"]:
            self.graph.add((entity, RDF.type, SCHEMA.Place))
            self.graph.add((entity, RDF.type, TOLKIEN_ONTOLOGY.Location))
        elif itype in ["object", "weapon", "artifact"]:
            self.graph.add((entity, RDF.type, SCHEMA.Thing))
            self.graph.add((entity, RDF.type, TOLKIEN_ONTOLOGY.Artifact))
        elif itype == "book":
            self.graph.add((entity, RDF.type, SCHEMA.Book))
        elif itype in ["event", "battle", "war"]:
            self.graph.add((entity, RDF.type, SCHEMA.Event))
            if itype == "battle":
                self.graph.add((entity, RDF.type, TOLKIEN_ONTOLOGY.Battle))
            elif itype == "war":
                self.graph.add((entity, RDF.type, TOLKIEN_ONTOLOGY.War))
        else:
            self.graph.add((entity, RDF.type, SCHEMA.Thing))

    def process_character(self, entity: URIRef, params: Dict[str, str]):
        if "name" in params:
            self.graph.add((entity, SCHEMA.name, Literal(clean_wikitext(params["name"]), lang="en")))
        if "othernames" in params:
            for n in split_on_br(params["othernames"]):
                cn = clean_wikitext(n)
                if cn and cn.lower() not in ["see below", ""]:
                    self.graph.add((entity, SCHEMA.alternateName, Literal(cn)))
        if "gender" in params:
            g = clean_wikitext(params["gender"]).lower()
            if g in ["male", "female"]:
                self.graph.add((entity, SCHEMA.gender, Literal(g)))
        for key in ["race", "people"]:
            if key in params:
                for link in extract_internal_links(params[key]):
                    self.graph.add((entity, TOLKIEN_ONTOLOGY.race, self.uri(link)))
                cr = clean_wikitext(params[key])
                if cr:
                    self.graph.add((entity, TOLKIEN_PROPERTY.raceLabel, Literal(cr)))
        if "birth" in params:
            b = clean_date_field(params["birth"])
            if b and is_valid_date(b):
                self.graph.add((entity, SCHEMA.birthDate, Literal(b)))
        if "birthlocation" in params:
            links = extract_internal_links(params["birthlocation"])
            if links:
                self.graph.add((entity, SCHEMA.birthPlace, self.uri(links[0])))
        if "death" in params:
            d = clean_date_field(params["death"])
            if d and is_valid_date(d):
                self.graph.add((entity, SCHEMA.deathDate, Literal(d)))
        if "deathlocation" in params:
            links = extract_internal_links(params["deathlocation"])
            if links:
                self.graph.add((entity, SCHEMA.deathPlace, self.uri(links[0])))
        if "spouse" in params:
            for link in extract_internal_links(params["spouse"]):
                self.graph.add((entity, SCHEMA.spouse, self.uri(link)))
        if "children" in params:
            for link in extract_internal_links(params["children"]):
                if link.lower() not in ["twins", "twin", "several", "many", "unknown", "none"]:
                    self.graph.add((entity, SCHEMA.children, self.uri(link)))
        if "parentage" in params:
            for link in extract_internal_links(params["parentage"]):
                self.graph.add((entity, SCHEMA.parent, self.uri(link)))
        if "siblings" in params:
            for link in extract_internal_links(params["siblings"]):
                self.graph.add((entity, SCHEMA.sibling, self.uri(link)))

    def process_place(self, entity: URIRef, params: Dict[str, str]):
        if "name" in params:
            self.graph.add((entity, SCHEMA.name, Literal(clean_wikitext(params["name"]), lang="en")))
        if "location" in params:
            for link in extract_internal_links(params["location"]):
                self.graph.add((entity, SCHEMA.containedInPlace, self.uri(link)))
        if "realm" in params:
            for link in extract_internal_links(params["realm"]):
                self.graph.add((entity, TOLKIEN_PROPERTY.realm, self.uri(link)))
        if "founded" in params:
            f = clean_date_field(params["founded"])
            if f:
                self.graph.add((entity, SCHEMA.foundingDate, Literal(f)))
        if "destroyed" in params:
            d = clean_date_field(params["destroyed"])
            if d:
                self.graph.add((entity, TOLKIEN_PROPERTY.destroyedDate, Literal(d)))
        if "description" in params:
            desc = clean_wikitext(params["description"])
            if desc:
                self.graph.add((entity, SCHEMA.description, Literal(desc, lang="en")))

    def process_object(self, entity: URIRef, params: Dict[str, str]):
        if "name" in params:
            self.graph.add((entity, SCHEMA.name, Literal(clean_wikitext(params["name"]), lang="en")))
        if "type" in params:
            self.graph.add((entity, TOLKIEN_PROPERTY.objectType, Literal(clean_wikitext(params["type"]))))
        if "owner" in params:
            for link in extract_internal_links(params["owner"]):
                owner = self.uri(link)
                self.graph.add((owner, SCHEMA.owns, entity))
                self.graph.add((entity, TOLKIEN_PROPERTY.ownedBy, owner))
        for key in ["creator", "maker"]:
            if key in params:
                for link in extract_internal_links(params[key]):
                    self.graph.add((entity, SCHEMA.creator, self.uri(link)))

    def process_event(self, entity: URIRef, params: Dict[str, str]):
        if "name" in params:
            self.graph.add((entity, SCHEMA.name, Literal(clean_wikitext(params["name"]), lang="en")))
        if "date" in params:
            d = clean_date_field(params["date"])
            if d:
                self.graph.add((entity, SCHEMA.startDate, Literal(d)))
        if "location" in params:
            for link in extract_internal_links(params["location"]):
                self.graph.add((entity, SCHEMA.location, self.uri(link)))
        if "result" in params or "outcome" in params:
            r = clean_wikitext(params.get("result") or params.get("outcome"))
            if r:
                self.graph.add((entity, TOLKIEN_PROPERTY.result, Literal(r)))

    def process_image(self, entity: URIRef, params: Dict[str, str]):
        if "image" in params:
            # Essayer d'obtenir l'URL directe de l'image
            from wiki import get_image_direct_url, WikiClient
            image_name = params["image"].strip()
            
            # D'abord essayer l'URL directe
            try:
                direct_url = get_image_direct_url(image_name)
                if direct_url:
                    self.graph.add((entity, SCHEMA.image, URIRef(direct_url)))
                    return
            except Exception:
                pass
            
            # Fallback: URL de la page File
            url = build_image_url(image_name)
            if url:
                self.graph.add((entity, SCHEMA.image, URIRef(url)))

    def add_external_links(self, entity: URIRef, name: str):
        """
        Ajoute les liens externes en utilisant la découverte dynamique.
        Interroge Wikipedia et Wikidata pour trouver les correspondances.
        """
        if not ENABLE_DYNAMIC_LINKING or not LINKING_AVAILABLE:
            return
        
        # Vérifier le cache
        if name in self._external_links_cache:
            links = self._external_links_cache[name]
        else:
            # Découverte dynamique des liens
            try:
                links = discover_external_links(name)
                self._external_links_cache[name] = links
            except Exception as e:
                # En cas d'erreur, on continue sans liens externes
                import logging
                logging.debug(f"External linking failed for '{name}': {e}")
                links = {}
        
        # Ajouter les liens découverts
        if "dbpedia" in links:
            self.graph.add((entity, OWL.sameAs, URIRef(links["dbpedia"])))
        
        if "wikidata" in links:
            self.graph.add((entity, OWL.sameAs, URIRef(links["wikidata"])))
        
        if "yago" in links:
            self.graph.add((entity, OWL.sameAs, URIRef(links["yago"])))
        
        if "wikipedia" in links:
            self.graph.add((entity, RDFS.seeAlso, URIRef(links["wikipedia"])))

    def process(self, title: str, infobox) -> Graph:
        self.reset()
        entity = self.uri(title)
        page = self.page_uri(title)
        self.add_base(entity, page, title)
        itype = detect_type(infobox)
        self.add_types(entity, itype)
        params = get_params(infobox)
        if itype == "character":
            self.process_character(entity, params)
        elif itype in ["place", "location"]:
            self.process_place(entity, params)
        elif itype in ["object", "weapon", "artifact"]:
            self.process_object(entity, params)
        elif itype in ["event", "battle", "war"]:
            self.process_event(entity, params)
        else:
            self.process_character(entity, params)
        self.process_image(entity, params)
        
        # Découverte dynamique des liens externes
        self.add_external_links(entity, title)
        
        return self.graph
