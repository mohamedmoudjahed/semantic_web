# Tolkien Knowledge Graph

**Mohamed Moudjahed et Juan Obando**  
Université Jean Monnet / École des Mines de Saint-Étienne

---

## Présentation

Ce projet construit un graphe de connaissances RDF à partir des données du wiki Tolkien Gateway. Il récupère des informations sur les personnages, lieux, artefacts et événements de l'univers de Tolkien, les structure en triplets RDF, et les relie à des bases de connaissances externes (DBpedia, Wikidata, YAGO). Le graphe obtenu peut être interrogé en SPARQL.

## Sources de données

La source principale est Tolkien Gateway, un wiki collaboratif dédié aux œuvres de Tolkien. Le scraper extrait les données structurées des templates infobox : biographies de personnages, détails des lieux, propriétés des artefacts, informations sur les batailles.

Le graphe est enrichi par d'autres sources :
- La base de données du jeu de cartes Middle Earth: The Wizards (stats prowess/body des personnages)
- Le wiki LOTR Fandom pour les labels multilingues (français, allemand, espagnol, etc.)
- Des fichiers CSV optionnels pour des attributs personnalisés

## Architecture

Le pipeline fonctionne en plusieurs étapes :

**Extraction** : Le client wiki récupère les pages par catégorie (personnages du Troisième Âge, forteresses, armes, etc.) et parse les templates infobox avec mwparserfromhell.

**Génération RDF** : Chaque infobox est convertie en triplets selon une ontologie basée sur schema.org. Les personnages deviennent des schema:Person avec des propriétés comme birthDate, spouse, parent. Les lieux deviennent des schema:Place avec des relations containedInPlace.

**Linking externe** : Pour chaque entité, le système interroge les APIs Wikipedia/Wikidata pour trouver les correspondances et ajoute des liens owl:sameAs vers les URIs DBpedia, Wikidata et YAGO.

**Enrichissement** : Les données des cartes METW et les labels multilingues sont associés aux entités par similarité de nom.

**Stockage** : Le graphe est sauvegardé en fichiers Turtle et peut être chargé dans Apache Jena Fuseki pour l'interrogation SPARQL.

## Ontologie

Les classes personnalisées étendent schema.org :
- `tont:Character` (sous-classe de schema:Person)
- `tont:Location` (sous-classe de schema:Place)
- `tont:Artifact` (sous-classe de schema:Thing)
- `tont:Battle`, `tont:War` (sous-classes de schema:Event)

Des shapes SHACL sont fournies pour la validation.

## Utilisation

Construire le graphe complet :
```bash
python main.py build
```

Construire certaines catégories seulement :
```bash
python main.py build --categories "Elves" "Hobbits" "Wizards"
```

Vérifier le statut de Fuseki :
```bash
python main.py fuseki-status
```

Tester le linking externe pour une entité :
```bash
python main.py link "Gandalf"
```

Démarrer le serveur web (si le module server est présent) :
```bash
python main.py serve --port 5000
```

## Configuration

Le fichier `config.py` permet de modifier :
- L'URL et le nom du dataset Fuseki
- Les catégories à scraper et leurs limites de pages
- Les délais de rate limiting des APIs
- Le répertoire de sortie

## Fichiers générés

Après un build :
- `tolkien_kg.ttl` : graphe de connaissances principal
- `tolkien_ontology.ttl` : définitions des classes et propriétés
- `tolkien_shapes.ttl` : shapes SHACL pour la validation
- `metw_cards.json` : données du jeu de cartes en cache

## Dépendances

- rdflib
- mwparserfromhell
- requests
- pyshacl (optionnel, pour la validation SHACL complète)

## Interrogation

Avec Fuseki en fonctionnement, le graphe peut être interrogé via SPARQL. Le module `reasoning.py` inclut des exemples de requêtes exploitant l'inférence owl:sameAs pour récupérer des données liées.

Exemple pour trouver tous les personnages avec leurs liens DBpedia :
```sparql
SELECT ?character ?label ?dbpedia
WHERE {
    ?character a tont:Character ;
               rdfs:label ?label .
    OPTIONAL {
        ?character owl:sameAs ?dbpedia .
        FILTER(CONTAINS(STR(?dbpedia), "dbpedia.org"))
    }
}
```
