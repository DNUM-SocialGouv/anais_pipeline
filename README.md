# Package Anais pipeline
Package Python de la pipeline d'exécution d'Anais

# Installation & Lancement du projet DBT

Cette section décrit les étapes nécessaires pour installer les dépendances et exécuter le projet.

---

## 1. Installation du package

Le projet utilise [UV] pour la gestion des dépendances Python.  
Voici les étapes à suivre pour initialiser l’environnement :

```bash

# 1. Se placer dans le dossier du projet
cd chemin/vers/le/projet

# 2. Vérifier que uv est installé
uv --version
pip install uv # Si pas installé

# 3. Installer le package
uv uv pip install "git+https://github.com/ton-org/mon-package-pipeline.git@main"
```

---

## 2. Lancement de la pipeline :

L'ensemble de la Pipeline est exécuté depuis le `main.py`.
La Pipeline est importée sous forme de package dans les différents projets.

### Pour l'exécution de la pipeline depuis un projet:
1. Placer vous dans le bon répertoire `anais_<Nom_projet>`

```bash
# Placer vous dans anais_Nom_projet
cd anais_Nom_projet
```

2. Lancer le `main.py`
```bash
uv run -m pipeline.main --env "local" --profile "<Nom_projet>"
```
Avec `env = 'local'` ou `'anais'` selon votre environnement de travail
et `profile = '<Nom_projet>'`

## 3. Fonctionnement de la pipeline
### 3.1 Pour Staging
#### Pipeline Staging sur env 'local':
1. Récupération des fichiers d'input. Ces fichiers doivent être placés manuellement dans le dossier `input/` sous format **.csv** (les délimiteurs sont gérés automatiquement).
2. Création de la base DuckDB si inexistante.
3. Connexion à la base DuckDB.
4. Création des tables, même si déjà existantes. Les fichiers sql de création de table (CREATE TABLE) doivent être placés dans le répertoire indiqué dans le `create_table_directory` de `metadata.yml`.
5. Lecture des csv avec standardisation des colonnes (ni caractères spéciaux, ni majuscule) -> injection des données dans les tables.
6. Historisation des données pour chaque table vers les tables `z<nom_de_la_table` avec indication que la date d'injection dans la colonne `date_ingestion`.
7. Vérification de la réussite de l'injection.
8. Fermeture de la connexion à la base DuckDB.
9. Exécution de la commande `run dbt` -> Création des vues relatives au projet.


#### Pipeline Staging sur env 'anais':
1. Récupération des fichiers d'input depuis le SFTP. Ces fichiers sont placés dans le dossier `input/` sous format **.csv** (les délimiteurs sont gérés automatiquement).
2. Connexion à la base Postgres.
3. Création des tables, même si déjà existantes. Les fichiers sql de création de table (CREATE TABLE) doivent être placés dans le répertoire indiqué dans le `create_table_directory` de `metadata.yml`.
4. Lecture des csv avec standardisation des colonnes (ni caractères spéciaux, ni majuscule) -> injection des données dans les tables.
5. Historisation des données pour chaque table vers les tables `z<nom_de_la_table` avec indication que la date d'injection dans la colonne `date_ingestion`.
6. Vérification de la réussite de l'injection.
7. Fermeture de la connexion à la base Postgres.
8. Exécution de la commande `run dbt` -> Création des vues relatives au projet.

### 3.2 Pour un autre projet
#### Pipeline <Nom_projet> sur env 'local':
1. Création de la base DuckDB si inexistante.
2. Connexion à la base DuckDB.
3. Méthode 1 : Récupération des tables d'origine nécessaires à <Nom_projet> à partir de la base staging. 
3. Méthode 2 : Création des tables d'origine nécessaires à <Nom_projet> à partir des fichiers CREATE TABLE .sql (`output_sql/<Nom_projet>/`) -> injection des données dans les tables à partir des fichiers de données .csv (`input/<Nom_projet>/`).
4. Historisation des données pour chaque table vers les tables `z<nom_de_la_table` avec indication que la date d'injection dans la colonne `date_ingestion`.
5. Vérification de la réussite de l'injection.
7. Fermeture de la connexion à la base DuckDB.
8. Exécution de la commande `run dbt` -> Création des vues relatives au projet.
9. Export des vues <Nom_projet> vers le répertoire `output/<Nom_projet>/`.


#### Pipeline <Nom_projet> sur env 'anais':
1. Connexion à la base Postgres.
2. Récupération des tables d'origine nécessaires à <Nom_projet> à partir de la base staging.
3. Historisation des données pour chaque table vers les tables `z<nom_de_la_table` avec indication que la date d'injection dans la colonne `date_ingestion`.
4. Exécution de la commande `run dbt` -> Création des vues relatives au projet.
5. Export des vues <Nom_projet> vers le répertoire `output/<Nom_projet>/`.
6. Export des vues <Nom_projet> vers le SFTP `/SCN_BDD/<Nom_projet>/output`.
7. Fermeture de la connexion à la base Postgres.

## 4. Architecture du package

## 🏗️ Architecture du projet

```plaintext
.
├── pipeline
│   ├── __init__.py
│   ├── main.py
│   ├── database_management
│   │   ├── database_pipeline.py
│   │   ├── duckdb_pipeline.py
│   │   ├── __init__.py
│   │   └── postgres_loader.py
│   ├── orchestration
│   │   └── pipeline_orchestration.py
│   └── utils
│       ├── config.py
│       ├── csv_management.py
│       ├── dbt_tools.py
│       ├── __init__.py
│       ├── load_yml.py
│       ├── logging_management.py
│       └── sftp_sync.py
├── poetry.lock
├── profiles.yml
├── pyproject.toml
├── README.md
└── uv.lock
```

## 5. Utilités des fichiers
### 5.1 Fichiers à la racine `./`
Répertoire d'orchestration de la pipeline Python.

- `pyproject.toml` : Fichier contenant les dépendances et packages nécessaires pour le lancement de la pipeline.
- `main.py` : Programme d'exécution de la pipeline.

### 5.2 Fichiers à la racine `./pipeline/database_management/`
- `database_pipeline.py` : Réalise les actions communes pour n'importe quelle database (lecture de fichier SQL, exécution du fichier sql, export de vues, run de la pipeline...). Fonctionne en complément avec duckdb_pipeline.py et postgres_loader.py.
- `duckdb_pipeline.py` : Réalise les actions spécifiques à une base (connexion à la base, création de table, chargement des données dans la BDD). Fonctionne en complément avec database_pipeline.py.

- `postgres_loader.py` : Réalise les actions spécifiques à une base postgres (connexion à la base, création de table, chargement des données dans la BDD). Fonctionne en complément avec database_pipeline.py.

### 5.3 Fichiers à la racine `./pipeline/orchestration/`
- `pipeline_orchestration.py` : Orchestre les différentes étapes de la pipeline selon l'environnement (local ou anais) et le projet (Staging ou autre).

### 5.4 Fichiers à la racine `./pipeline/utils/`
- `config.py` : Définie les paramètres de configuration liés aux logs, à la database, à l'heure d'exécution ...
- `csv_management.py` : Réalise les actions relatives à la manipulation de fichier csv (transformation d'un .xlsx en .csv, lecture du .csv avec délimiteur personnalisé, standarisation des colonnes, conversion des types, export ...).
- `dbt_tools.py` : Permet de lancer des commandes dbt (dbt run, dbt test et dbt deps).
- `load_yml.py` : Permet la lecture d'un fichier .yaml
- `sftp_sync.py` : Réalise les actions relatives à une connexion SFTP (connexion, import, export...).
- `logging_management.py` : Initialise la log selon l'environnement.