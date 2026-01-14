#!/usr/bin/env python3
"""
Tolkien Knowledge Graph - Point d'entree principal.

Usage:

python main.py build [--categories CAT1 CAT2 ...] [--no-fuseki]
python main.py serve [--port PORT]
python main.py ontology
python main.py shapes
python main.py fuseki-status
python main.py fuseki-load [--file PATH]
python main.py link <entity_name>
"""

import os
import sys
import argparse

from config import OUTPUT_DIR, CATEGORIES, FUSEKI_URL, FUSEKI_DATASET
from builder import KGBuilder
from ontology import create_ontology, create_shacl_shapes


def cmd_build(args):
    """Construit le Knowledge Graph."""
    builder = KGBuilder(output_dir=args.output, use_fuseki=not args.no_fuseki)
    
    if args.categories:
        cats = [(c, 100) for c in args.categories]
    else:
        cats = []
        for cat_list in CATEGORIES.values():
            cats.extend(cat_list)
    
    builder.build(categories=cats)
    builder.add_ontology()
    
    # Enrichissement - multilingue activé par défaut
    csv_path = args.csv if hasattr(args, 'csv') and args.csv else None
    enable_multilingual = not args.no_multilingual if hasattr(args, 'no_multilingual') else True
    
    if enable_multilingual:
        print("\n" + "=" * 60)
        print("MULTILINGUAL ENRICHMENT ENABLED")
        print("=" * 60)
        print("Source: LOTR Fandom Wiki (lotr.fandom.com)")
        print("Languages: French, German, Spanish")
        print("This may take a few minutes (API rate limiting)")
        print("=" * 60)
    
    builder.enrich(
        csv_path=csv_path,
        enable_metw=not args.no_metw if hasattr(args, 'no_metw') else True,
        enable_csv=csv_path is not None,
        enable_multilingual=enable_multilingual,
        multilingual_limit=50,
        languages=['fr', 'de', 'es']
    )
    
    builder.save()
    builder.save_ontology()
    builder.save_shapes()
    
    # Charger dans Fuseki si disponible
    if not args.no_fuseki:
        builder.load_to_fuseki()
    
    print("\nBuild complete.")


def cmd_serve(args):
    """Démarre le serveur web."""
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'server'))
    from server.app import run
    run(host=args.host, port=args.port)


def cmd_ontology(args):
    """Génère le fichier d'ontologie."""
    os.makedirs(args.output, exist_ok=True)
    onto = create_ontology()
    path = os.path.join(args.output, 'tolkien_ontology.ttl')
    onto.serialize(destination=path, format='turtle')
    print(f"Ontology saved: {path}")


def cmd_shapes(args):
    """Génère les shapes SHACL."""
    os.makedirs(args.output, exist_ok=True)
    shapes = create_shacl_shapes()
    path = os.path.join(args.output, 'tolkien_shapes.ttl')
    shapes.serialize(destination=path, format='turtle')
    print(f"SHACL shapes saved: {path}")


def cmd_fuseki_status(args):
    """Affiche le statut de Fuseki."""
    from fuseki_client import FusekiClient
    
    fuseki = FusekiClient(FUSEKI_URL, FUSEKI_DATASET)
    
    print(f"\nFuseki Triplestore Status")
    print("=" * 40)
    print(f"URL: {FUSEKI_URL}")
    print(f"Dataset: {FUSEKI_DATASET}")
    
    if fuseki.is_available():
        print(f"Status: ✓ AVAILABLE")
        stats = fuseki.get_statistics()
        print(f"\nStatistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
    else:
        print(f"Status: ✗ NOT AVAILABLE")
        print("\nMake sure Fuseki is running:")
        print(f"  1. Start Fuseki server")
        print(f"  2. Create dataset '{FUSEKI_DATASET}'")
        print(f"  3. Access at {FUSEKI_URL}/#/dataset/{FUSEKI_DATASET}")


def cmd_fuseki_load(args):
    """Charge un fichier RDF dans Fuseki."""
    from fuseki_client import FusekiClient
    
    fuseki = FusekiClient(FUSEKI_URL, FUSEKI_DATASET)
    
    if not fuseki.is_available():
        print(f"Error: Fuseki not available at {FUSEKI_URL}")
        sys.exit(1)
    
    filepath = args.file or os.path.join(OUTPUT_DIR, "tolkien_kg.ttl")
    
    if not os.path.exists(filepath):
        print(f"Error: File not found: {filepath}")
        sys.exit(1)
    
    print(f"Loading {filepath} into Fuseki...")
    
    success = fuseki.load_file(filepath, clear_first=args.clear)
    
    if success:
        count = fuseki.count_triples()
        print(f"✓ Successfully loaded {count} triples into {FUSEKI_DATASET}")
    else:
        print(f"✗ Failed to load file into Fuseki")
        sys.exit(1)


def cmd_fuseki_clear(args):
    """Vide le dataset Fuseki."""
    from fuseki_client import FusekiClient
    
    fuseki = FusekiClient(FUSEKI_URL, FUSEKI_DATASET)
    
    if not fuseki.is_available():
        print(f"Error: Fuseki not available at {FUSEKI_URL}")
        sys.exit(1)
    
    if not args.yes:
        response = input(f"Clear all data from {FUSEKI_DATASET}? [y/N] ")
        if response.lower() != 'y':
            print("Aborted.")
            return
    
    success = fuseki.clear()
    
    if success:
        print(f"Dataset {FUSEKI_DATASET} cleared")
    else:
        print(f"Failed to clear dataset")
        sys.exit(1)


def cmd_link(args):
    """Teste le linking dynamique pour une entité."""
    from linking import discover_external_links
    
    entity_name = args.entity
    print(f"\nDiscovering external links for: {entity_name}")
    print("=" * 50)
    
    links = discover_external_links(entity_name)
    
    if links:
        for link_type, uri in links.items():
            print(f"  {link_type.upper()}: {uri}")
    else:
        print("  No external links found")


def cmd_export(args):
    """Exporte le graphe depuis Fuseki vers un fichier."""
    from fuseki_client import FusekiClient
    
    fuseki = FusekiClient(FUSEKI_URL, FUSEKI_DATASET)
    
    if not fuseki.is_available():
        print(f"Error: Fuseki not available at {FUSEKI_URL}")
        sys.exit(1)
    
    filepath = args.output_file or os.path.join(OUTPUT_DIR, "tolkien_kg_export.ttl")
    
    print(f"Exporting from Fuseki to {filepath}...")
    
    success = fuseki.export_to_file(filepath, format=args.format)
    
    if success:
        size = os.path.getsize(filepath) / 1024
        print(f"✓ Exported to {filepath} ({size:.1f} KB)")
    else:
        print(f"✗ Failed to export")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='Tolkien Knowledge Graph')
    parser.add_argument('--output', default=OUTPUT_DIR, help='Output directory')
    
    subparsers = parser.add_subparsers(dest='command', help='Command')
    
    # build
    p_build = subparsers.add_parser('build', help='Build the knowledge graph')
    p_build.add_argument('--categories', nargs='+', help='Categories to process')
    p_build.add_argument('--csv', help='Path to LOTR characters CSV file')
    p_build.add_argument('--no-metw', action='store_true', help='Disable METW cards enrichment')
    p_build.add_argument('--no-multilingual', action='store_true', help='Disable multilingual labels (enabled by default)')
    p_build.add_argument('--no-fuseki', action='store_true', help='Do not load into Fuseki')
    
    # serve
    p_serve = subparsers.add_parser('serve', help='Start the web server')
    p_serve.add_argument('--port', type=int, default=5000)
    p_serve.add_argument('--host', default='0.0.0.0')
    
    # ontology
    subparsers.add_parser('ontology', help='Generate ontology file')
    
    # shapes
    subparsers.add_parser('shapes', help='Generate SHACL shapes file')
    
    # fuseki-status
    subparsers.add_parser('fuseki-status', help='Check Fuseki triplestore status')
    
    # fuseki-load
    p_load = subparsers.add_parser('fuseki-load', help='Load RDF file into Fuseki')
    p_load.add_argument('--file', help='Path to RDF file (default: output/tolkien_kg.ttl)')
    p_load.add_argument('--clear', action='store_true', help='Clear dataset before loading')
    
    # fuseki-clear
    p_clear = subparsers.add_parser('fuseki-clear', help='Clear Fuseki dataset')
    p_clear.add_argument('--yes', '-y', action='store_true', help='Skip confirmation')
    
    # link
    p_link = subparsers.add_parser('link', help='Test dynamic linking for an entity')
    p_link.add_argument('entity', help='Entity name to link (e.g., "Gandalf")')
    
    # export
    p_export = subparsers.add_parser('export', help='Export graph from Fuseki to file')
    p_export.add_argument('--output-file', help='Output file path')
    p_export.add_argument('--format', default='turtle', choices=['turtle', 'xml', 'nt'])
    
    args = parser.parse_args()
    
    if args.command == 'build':
        cmd_build(args)
    elif args.command == 'serve':
        cmd_serve(args)
    elif args.command == 'ontology':
        cmd_ontology(args)
    elif args.command == 'shapes':
        cmd_shapes(args)
    elif args.command == 'fuseki-status':
        cmd_fuseki_status(args)
    elif args.command == 'fuseki-load':
        cmd_fuseki_load(args)
    elif args.command == 'fuseki-clear':
        cmd_fuseki_clear(args)
    elif args.command == 'link':
        cmd_link(args)
    elif args.command == 'export':
        cmd_export(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
