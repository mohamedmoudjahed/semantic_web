"""
Client Fuseki pour le Knowledge Graph Tolkien.

Ce module gère la communication avec le triplestore Apache Jena Fuseki.
Il permet de charger, interroger et mettre à jour le graphe RDF.
"""

import logging
from typing import Optional, List, Dict, Any, Tuple

import requests
from rdflib import Graph

from config import PREFIXES

logger = logging.getLogger(__name__)

# Configuration Fuseki par défaut
DEFAULT_FUSEKI_URL = "http://localhost:3030"
DEFAULT_DATASET = "tolkien"


class FusekiClient:
    """
    Client pour interagir avec Apache Jena Fuseki.
    """
    
    def __init__(self, fuseki_url: str = None, dataset: str = None):
        """
        Initialise le client Fuseki.
        
        Args:
            fuseki_url: URL de base de Fuseki (ex: http://localhost:3030)
            dataset: Nom du dataset (ex: tolkien)
        """
        self.fuseki_url = (fuseki_url or DEFAULT_FUSEKI_URL).rstrip('/')
        self.dataset = dataset or DEFAULT_DATASET
        
        # Endpoints
        self.sparql_endpoint = f"{self.fuseki_url}/{self.dataset}/sparql"
        self.update_endpoint = f"{self.fuseki_url}/{self.dataset}/update"
        self.data_endpoint = f"{self.fuseki_url}/{self.dataset}/data"
        self.query_endpoint = f"{self.fuseki_url}/{self.dataset}/query"
        
        self.timeout = 30
        
    def is_available(self) -> bool:
        """Vérifie si le serveur Fuseki est disponible."""
        try:
            response = requests.get(
                f"{self.fuseki_url}/$/ping",
                timeout=5
            )
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Fuseki not available: {e}")
            return False
    
    def dataset_exists(self) -> bool:
        """Vérifie si le dataset existe."""
        try:
            response = requests.get(
                f"{self.fuseki_url}/$/datasets/{self.dataset}",
                timeout=5
            )
            return response.status_code == 200
        except Exception:
            return False
    
    def get_dataset_info(self) -> Optional[Dict]:
        """Récupère les informations sur le dataset."""
        try:
            response = requests.get(
                f"{self.fuseki_url}/$/datasets/{self.dataset}",
                timeout=self.timeout
            )
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Error getting dataset info: {e}")
        return None
    
    def count_triples(self) -> int:
        """Compte le nombre de triplets dans le dataset."""
        query = "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }"
        results = self.query(query)
        if results and results.get("results", {}).get("bindings"):
            return int(results["results"]["bindings"][0]["count"]["value"])
        return 0
    
    def query(self, sparql_query: str, format: str = "json") -> Optional[Dict]:
        """
        Exécute une requête SPARQL SELECT/ASK.
        
        Args:
            sparql_query: Requête SPARQL
            format: Format de réponse (json, xml)
            
        Returns:
            Résultats de la requête ou None en cas d'erreur
        """
        try:
            # Ajouter les préfixes standards
            prefixes = self._build_prefixes()
            full_query = prefixes + sparql_query
            
            headers = {
                "Accept": "application/sparql-results+json" if format == "json" else "application/sparql-results+xml"
            }
            
            response = requests.post(
                self.sparql_endpoint,
                data={"query": full_query},
                headers=headers,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            
            if format == "json":
                return response.json()
            return {"raw": response.text}
            
        except requests.exceptions.RequestException as e:
            logger.error(f"SPARQL query error: {e}")
            return None
    
    def construct(self, sparql_query: str) -> Optional[Graph]:
        """
        Exécute une requête SPARQL CONSTRUCT.
        
        Args:
            sparql_query: Requête SPARQL CONSTRUCT
            
        Returns:
            Graph RDFLib avec les résultats
        """
        try:
            prefixes = self._build_prefixes()
            full_query = prefixes + sparql_query
            
            response = requests.post(
                self.sparql_endpoint,
                data={"query": full_query},
                headers={"Accept": "text/turtle"},
                timeout=self.timeout
            )
            
            response.raise_for_status()
            
            g = Graph()
            g.parse(data=response.text, format="turtle")
            return g
            
        except Exception as e:
            logger.error(f"SPARQL CONSTRUCT error: {e}")
            return None
    
    def update(self, sparql_update: str) -> bool:
        """
        Exécute une requête SPARQL UPDATE (INSERT/DELETE).
        
        Args:
            sparql_update: Requête SPARQL UPDATE
            
        Returns:
            True si succès, False sinon
        """
        try:
            prefixes = self._build_prefixes()
            full_query = prefixes + sparql_update
            
            response = requests.post(
                self.update_endpoint,
                data={"update": full_query},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=self.timeout
            )
            
            response.raise_for_status()
            return True
            
        except Exception as e:
            logger.error(f"SPARQL UPDATE error: {e}")
            return False
    
    def load_graph(self, graph: Graph, clear_first: bool = False) -> bool:
        """
        Charge un graphe RDFLib dans Fuseki.
        
        Args:
            graph: Graphe RDFLib à charger
            clear_first: Si True, vide le dataset avant de charger
            
        Returns:
            True si succès
        """
        try:
            if clear_first:
                self.clear()
            
            # Sérialiser en Turtle
            turtle_data = graph.serialize(format="turtle")
            
            response = requests.post(
                self.data_endpoint,
                data=turtle_data.encode('utf-8'),
                headers={"Content-Type": "text/turtle; charset=utf-8"},
                timeout=120  # Timeout plus long pour les gros graphes
            )
            
            response.raise_for_status()
            logger.info(f"Loaded {len(graph)} triples into Fuseki")
            return True
            
        except Exception as e:
            logger.error(f"Error loading graph to Fuseki: {e}")
            return False
    
    def load_file(self, filepath: str, format: str = "turtle", clear_first: bool = False) -> bool:
        """
        Charge un fichier RDF dans Fuseki.
        
        Args:
            filepath: Chemin du fichier
            format: Format du fichier (turtle, xml, nt)
            clear_first: Si True, vide le dataset avant
            
        Returns:
            True si succès
        """
        try:
            g = Graph()
            g.parse(filepath, format=format)
            return self.load_graph(g, clear_first=clear_first)
        except Exception as e:
            logger.error(f"Error loading file: {e}")
            return False
    
    def clear(self) -> bool:
        """Vide complètement le dataset."""
        return self.update("CLEAR ALL")
    
    def get_entity(self, entity_uri: str) -> Optional[Graph]:
        """
        Récupère toutes les informations sur une entité.
        
        Args:
            entity_uri: URI de l'entité
            
        Returns:
            Graphe avec les triplets de l'entité
        """
        query = f"""
        CONSTRUCT {{
            <{entity_uri}> ?p ?o .
            ?s ?p2 <{entity_uri}> .
        }}
        WHERE {{
            {{ <{entity_uri}> ?p ?o }}
            UNION
            {{ ?s ?p2 <{entity_uri}> }}
        }}
        """
        return self.construct(query)
    
    def search_by_label(self, search_term: str, limit: int = 50) -> List[Dict]:
        """
        Recherche des entités par label.
        
        Args:
            search_term: Terme de recherche
            limit: Nombre max de résultats
            
        Returns:
            Liste de dictionnaires {uri, label, type}
        """
        # Échapper les caractères spéciaux pour SPARQL
        search_term_escaped = search_term.replace("'", "\\'").replace('"', '\\"')
        
        query = f"""
        SELECT DISTINCT ?entity ?label ?type
        WHERE {{
            ?entity rdfs:label ?label .
            FILTER(CONTAINS(LCASE(STR(?label)), LCASE("{search_term_escaped}")))
            OPTIONAL {{ ?entity a ?type }}
        }}
        LIMIT {limit}
        """
        
        results = self.query(query)
        entities = []
        
        if results and "results" in results:
            for binding in results["results"]["bindings"]:
                entities.append({
                    "uri": binding.get("entity", {}).get("value", ""),
                    "label": binding.get("label", {}).get("value", ""),
                    "type": binding.get("type", {}).get("value", "")
                })
        
        return entities
    
    def get_statistics(self) -> Dict[str, int]:
        """
        Récupère les statistiques du graphe.
        
        Returns:
            Dictionnaire avec les comptages par type
        """
        query = """
        SELECT ?type (COUNT(?entity) as ?count)
        WHERE {
            ?entity a ?type .
        }
        GROUP BY ?type
        ORDER BY DESC(?count)
        """
        
        results = self.query(query)
        stats = {"total": self.count_triples()}
        
        if results and "results" in results:
            for binding in results["results"]["bindings"]:
                type_uri = binding.get("type", {}).get("value", "")
                count = int(binding.get("count", {}).get("value", 0))
                
                # Simplifier le nom du type
                type_name = type_uri.split("/")[-1].split("#")[-1]
                stats[type_name] = count
        
        return stats
    
    def _build_prefixes(self) -> str:
        """Construit la chaîne des préfixes SPARQL."""
        lines = []
        for prefix, ns in PREFIXES.items():
            lines.append(f"PREFIX {prefix}: <{ns}>")
        
        # Ajouter les préfixes standard si absents
        standard = [
            ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
            ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
            ("xsd", "http://www.w3.org/2001/XMLSchema#"),
        ]
        
        for prefix, ns in standard:
            if prefix not in PREFIXES:
                lines.append(f"PREFIX {prefix}: <{ns}>")
        
        return "\n".join(lines) + "\n\n"
    
    def export_to_file(self, filepath: str, format: str = "turtle") -> bool:
        """
        Exporte le contenu du dataset vers un fichier.
        
        Args:
            filepath: Chemin du fichier de sortie
            format: Format (turtle, xml, nt)
            
        Returns:
            True si succès
        """
        try:
            query = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"
            graph = self.construct(query)
            
            if graph:
                graph.serialize(destination=filepath, format=format)
                logger.info(f"Exported {len(graph)} triples to {filepath}")
                return True
                
        except Exception as e:
            logger.error(f"Export error: {e}")
        
        return False


# Singleton pour l'accès global
_fuseki_client: Optional[FusekiClient] = None


def get_fuseki_client(fuseki_url: str = None, dataset: str = None) -> FusekiClient:
    """
    Retourne le client Fuseki (singleton).
    """
    global _fuseki_client
    
    if _fuseki_client is None:
        _fuseki_client = FusekiClient(fuseki_url, dataset)
    
    return _fuseki_client


def reset_fuseki_client():
    """Réinitialise le client Fuseki."""
    global _fuseki_client
    _fuseki_client = None
