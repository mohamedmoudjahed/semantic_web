"""
Constructeur du Knowledge Graph Tolkien.
Intègre Fuseki et le linking dynamique.
"""

import os
import time
import logging
from typing import Dict, List, Tuple, Optional, Callable

from rdflib import Graph

from config import OUTPUT_DIR, CATEGORIES, FUSEKI_URL, FUSEKI_DATASET
from wiki import WikiClient
from rdf_generator import RDFGenerator, create_graph, extract_infobox
from ontology import create_ontology, create_shacl_shapes
from enrichment import enrich_all, load_metw_cards, enrich_with_metw

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KGBuilder:
    def __init__(self, output_dir: str = None, use_fuseki: bool = True, 
                 progress_callback: Callable = None, cancel_check: Callable = None):
        """
        Initialise le constructeur de KG.
        
        Args:
            output_dir: Répertoire de sortie
            use_fuseki: Si True, tente de charger dans Fuseki après construction
            progress_callback: Fonction appelée pour reporter la progression
                               Signature: callback(step, message, progress_percent, details)
            cancel_check: Fonction qui retourne True si le build doit être annulé
        """
        self.output_dir = output_dir or OUTPUT_DIR
        os.makedirs(self.output_dir, exist_ok=True)
        self.wiki = WikiClient()
        self.generator = RDFGenerator()
        self.graph = create_graph()
        self.use_fuseki = use_fuseki
        self._fuseki_client = None
        self.progress_callback = progress_callback
        self.cancel_check = cancel_check
        
        # Set pour tracker les pages déjà traitées (évite les doublons)
        self._processed_pages = set()
        
        self.stats = {
            'processed': 0,
            'success': 0,
            'no_infobox': 0,
            'errors': 0,
            'triples': 0,
            'external_links_found': 0,
            'duplicates_skipped': 0
        }

    def _check_cancelled(self):
        """Vérifie si le build a été annulé."""
        if self.cancel_check and self.cancel_check():
            raise InterruptedError("Build cancelled by user")

    def _report_progress(self, step: str, message: str, progress: float, details: dict = None):
        """Reporte la progression via le callback si défini."""
        if self.progress_callback:
            self.progress_callback(step, message, progress, details or {})

    def get_fuseki_client(self):
        """Retourne le client Fuseki (lazy loading)."""
        if self._fuseki_client is None:
            try:
                from fuseki_client import FusekiClient
                self._fuseki_client = FusekiClient(FUSEKI_URL, FUSEKI_DATASET)
            except ImportError:
                logger.warning("Fuseki client not available")
                self._fuseki_client = None
        return self._fuseki_client

    def process_page(self, title: str) -> Tuple[Optional[Graph], str]:
        try:
            wikitext = self.wiki.get_page_wikitext(title)
            if not wikitext:
                return None, "no_page"
            infobox = extract_infobox(wikitext)
            if not infobox:
                return None, "no_infobox"
            g = self.generator.process(title, infobox)
            return g, "success"
        except Exception as e:
            logger.debug(f"Error for {title}: {e}")
            return None, f"error: {str(e)[:50]}"

    def process_category(self, category: str, limit: int = 100, verbose: bool = True,
                         base_progress: float = 0, progress_range: float = 100) -> Dict[str, int]:
        """
        Traite une catégorie.
        
        Args:
            category: Nom de la catégorie
            limit: Nombre max de pages
            verbose: Afficher les messages
            base_progress: Progression de base (pour le calcul du pourcentage global)
            progress_range: Plage de progression pour cette catégorie
        """
        cat_stats = {'processed': 0, 'success': 0, 'skipped': 0, 'errors': 0, 'duplicates': 0}
        if verbose:
            print(f"\nCategory: {category}")
            print("-" * 50)
        
        self._report_progress("fetch", f"Fetching pages from '{category}'...", base_progress, 
                             {"category": category})
        
        # Vérifier l'annulation avant de fetch
        self._check_cancelled()
        
        pages = self.wiki.get_category_members(category, limit=limit)
        if verbose:
            print(f"Pages found: {len(pages)}")
        
        for i, title in enumerate(pages, 1):
            # Vérifier l'annulation à chaque page
            self._check_cancelled()
            
            # Calculer la progression
            page_progress = base_progress + (i / len(pages)) * progress_range if pages else base_progress
            
            # Vérifier si la page a déjà été traitée
            if title in self._processed_pages:
                cat_stats['duplicates'] += 1
                self.stats['duplicates_skipped'] += 1
                if verbose:
                    print(f"  [{i}/{len(pages)}] {title[:40].ljust(40)} SKIP (duplicate)")
                continue
            
            self.stats['processed'] += 1
            cat_stats['processed'] += 1
            
            # Reporter la progression
            self._report_progress("process", f"Processing: {title}", page_progress,
                                 {"page": title, "current": i, "total": len(pages), 
                                  "category": category, "stats": self.stats.copy()})
            
            if verbose:
                print(f"  [{i}/{len(pages)}] {title[:40].ljust(40)}", end=" ")
            
            g, status = self.process_page(title)
            
            if status == "success" and g:
                triples = len(g)
                self.stats['success'] += 1
                self.stats['triples'] += triples
                cat_stats['success'] += 1
                self.graph += g
                self._processed_pages.add(title)  # Marquer comme traité
                if verbose:
                    print(f"OK ({triples} triples)")
            elif status == "no_infobox":
                self.stats['no_infobox'] += 1
                cat_stats['skipped'] += 1
                self._processed_pages.add(title)  # Marquer même sans infobox
                if verbose:
                    print("SKIP (no infobox)")
            else:
                self.stats['errors'] += 1
                cat_stats['errors'] += 1
                if verbose:
                    print(f"ERROR ({status})")
            
            time.sleep(1.5 if i % 10 == 0 else 0.8)
        
        return cat_stats

    def build(self, categories: List[Tuple[str, int]] = None, verbose: bool = True):
        if categories is None:
            categories = []
            for cat_list in CATEGORIES.values():
                categories.extend(cat_list)
        
        if verbose:
            print("\n" + "=" * 60)
            print("BUILDING TOLKIEN KNOWLEDGE GRAPH")
            print("=" * 60)
            print(f"Categories: {len(categories)}")
            print("Dynamic external linking: ENABLED")
        
        self._report_progress("start", "Starting build...", 0, 
                             {"total_categories": len(categories)})
        
        # Calculer la progression par catégorie (60% du total pour l'extraction)
        progress_per_cat = 60 / len(categories) if categories else 0
        
        for idx, (cat, limit) in enumerate(categories, 1):
            if verbose:
                print(f"\n[{idx}/{len(categories)}] ", end="")
            
            base_progress = (idx - 1) * progress_per_cat
            self.process_category(cat, limit=limit, verbose=verbose,
                                 base_progress=base_progress, progress_range=progress_per_cat)
        
        if verbose:
            print("\n" + "-" * 60)
            print(f"Processed: {self.stats['processed']}")
            print(f"Success: {self.stats['success']}")
            print(f"No infobox: {self.stats['no_infobox']}")
            print(f"Errors: {self.stats['errors']}")
            print(f"Duplicates skipped: {self.stats['duplicates_skipped']}")
            print(f"Triples: {self.stats['triples']}")

    def add_ontology(self, verbose: bool = True):
        self._report_progress("ontology", "Adding ontology definitions...", 65)
        if verbose:
            print("\nAdding ontology...")
        onto = create_ontology()
        before = len(self.graph)
        self.graph += onto
        if verbose:
            print(f"Ontology triples added: {len(self.graph) - before}")

    def enrich(self, csv_path: str = None, enable_metw: bool = True, 
               enable_csv: bool = True, enable_multilingual: bool = True,
               multilingual_limit: int = 50, languages: List[str] = None,
               verbose: bool = True):
        """Enrichit avec les donnees externes (METW, CSV, multilingue)."""
        if verbose:
            print("\n" + "=" * 60)
            print("ENRICHMENT WITH EXTERNAL DATA")
            print("=" * 60)
        
        if enable_metw:
            self._report_progress("metw", "Enriching with METW card data...", 70)
        
        if enable_multilingual:
            self._report_progress("multilingual", "Fetching multilingual labels...", 75,
                                 {"languages": languages})
        
        enrich_all(
            self.graph,
            csv_path=csv_path,
            enable_metw=enable_metw,
            enable_csv=enable_csv,
            enable_multilingual=enable_multilingual,
            multilingual_limit=multilingual_limit,
            languages=languages,
            verbose=verbose
        )

    def save(self, filename: str = "tolkien_kg.ttl", verbose: bool = True) -> str:
        """Sauvegarde le graphe en fichier Turtle."""
        self._report_progress("save", "Saving graph to file...", 90)
        path = os.path.join(self.output_dir, filename)
        self.graph.serialize(destination=path, format='turtle')
        if verbose:
            size = os.path.getsize(path) / 1024
            print(f"\nGraph saved: {path}")
            print(f"Size: {size:.1f} KB")
            print(f"Total triples: {len(self.graph)}")
        return path

    def load_to_fuseki(self, clear_first: bool = True, verbose: bool = True) -> bool:
        """
        Charge le graphe dans Fuseki.
        
        Args:
            clear_first: Si True, vide le dataset avant de charger
            verbose: Afficher les messages
            
        Returns:
            True si succès
        """
        self._report_progress("fuseki", "Loading into Fuseki triplestore...", 95)
        
        if not self.use_fuseki:
            if verbose:
                print("Fuseki loading disabled")
            return False
        
        fuseki = self.get_fuseki_client()
        if fuseki is None:
            if verbose:
                print("Fuseki client not available")
            return False
        
        if not fuseki.is_available():
            if verbose:
                print(f"Fuseki not available at {FUSEKI_URL}")
            return False
        
        if verbose:
            print(f"\nLoading graph into Fuseki ({FUSEKI_URL}/{FUSEKI_DATASET})...")
        
        success = fuseki.load_graph(self.graph, clear_first=clear_first)
        
        if verbose:
            if success:
                count = fuseki.count_triples()
                print(f"Successfully loaded {count} triples into Fuseki")
            else:
                print("Failed to load graph into Fuseki")
        
        self._report_progress("complete", "Build complete!", 100, {"success": success})
        
        return success

    def save_ontology(self, filename: str = "tolkien_ontology.ttl") -> str:
        onto = create_ontology()
        path = os.path.join(self.output_dir, filename)
        onto.serialize(destination=path, format='turtle')
        return path

    def save_shapes(self, filename: str = "tolkien_shapes.ttl") -> str:
        shapes = create_shacl_shapes()
        path = os.path.join(self.output_dir, filename)
        shapes.serialize(destination=path, format='turtle')
        return path

    def full_build(self, categories: List[Tuple[str, int]] = None,
                   csv_path: str = None,
                   enable_metw: bool = True,
                   enable_multilingual: bool = True,
                   load_fuseki: bool = True,
                   verbose: bool = True) -> Dict:
        """
        Construction complète du KG avec toutes les étapes.
        
        Args:
            categories: Liste des catégories à traiter
            csv_path: Chemin vers le fichier CSV d'enrichissement
            enable_metw: Activer l'enrichissement METW
            enable_multilingual: Activer les labels multilingues
            load_fuseki: Charger dans Fuseki après construction
            verbose: Afficher la progression
            
        Returns:
            Dictionnaire avec les statistiques
        """
        # 1. Construction
        self.build(categories=categories, verbose=verbose)
        
        # 2. Ontologie
        self.add_ontology(verbose=verbose)
        
        # 3. Enrichissement
        self.enrich(
            csv_path=csv_path,
            enable_metw=enable_metw,
            enable_csv=csv_path is not None,
            enable_multilingual=enable_multilingual,
            verbose=verbose
        )
        
        # 4. Sauvegarde fichier
        output_path = self.save(verbose=verbose)
        self.save_ontology()
        self.save_shapes()
        
        # 5. Chargement Fuseki
        fuseki_loaded = False
        if load_fuseki and self.use_fuseki:
            fuseki_loaded = self.load_to_fuseki(verbose=verbose)
        
        return {
            'stats': self.stats,
            'output_path': output_path,
            'total_triples': len(self.graph),
            'fuseki_loaded': fuseki_loaded
        }
