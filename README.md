# Package Anais pipeline
Package Python de la pipeline d'exécution d'Anais

# Installation & Lancement du projet DBT

Cette section décrit les étapes nécessaires pour installer les dépendances, configurer DBT, instancier la base de données si besoin, et exécuter le projet.

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

## 3. Lancement de la pipeline :

L'ensemble de la Pipeline est exécuté depuis le `main.py`.

### Pour l'exécution de la pipeline:
1. Placer vous dans le bon répertoire `anais_<Nom_projet>`

```bash
# Placer vous dans anais_Nom_projet
cd anais_Nom_projet
```

2. Lancer le `main.py`
```bash
uv run main.py --env "local" --profile "Nom_projet"
```
Avec env = 'local' ou 'anais' selon votre environnement de travail
et profile = 'Nom_projet'

## 4. Fonctionnement de la pipeline
### Pipeline sur env 'local':
1. Récupération des fichiers d'input. !! Les fichiers doivent être placés manuellement dans le dossier `input/` sous format **.csv** !! (les délimiteurs sont gérés automatiquement)
2. Création de la base DuckDB si inexistante.
3. Connexion à la base DuckDB
4. Création des tables si inexistantes
5. Lecture des csv avec standardisation des colonnes (sans caractères spéciaux) -> injection des données dans les tables
6. Vérification de l'injection
7. Exécution de la commande `run dbt` -> Création des vues relatives au projet
8. Export des vues dans le dossier `output/`
9. Fermeture de la connexion à la base DuckDB

### Pipeline sur env 'anais':
1. Récupération des fichiers d'input. Ces fichiers sont récupérés automatiquement sur le SFTP et placés dans le dossier `input/` sous format **.csv** (les délimiteurs sont gérés automatiquement)
2. Création de la base Postgres si inexistante.
3. Connexion à la base Postgres
4. Création des tables si inexistantes
5. Lecture des csv avec standardisation des colonnes (sans caractères spéciaux) -> injection des données dans les tables
6. Vérification de l'injection
7. Exécution de la commande `run dbt` -> Création des vues relatives au projet
8. Export des vues dans le dossier `output/` au format **.csv**
9. Fermeture de la connexion à la base Postgres
10. Export des **.csv** en output vers le SFTP


## 5. Architecture du package

## 🏗️ Architecture du projet

```plaintext
.
├── data
│   └── duckdb_database.duckdb
├── input
│   ├── cert_dc_insern_2023_2024.csv
│   ...
│   └── v_region.csv
├── logs
│   └── sources.log
├── output
│   ├── sa_ods_insee_2025_07_09.csv
│   ...
│   └── sa_ods_pmsi_2025_07_09.csv
├── Staging
│   ├── dbtStaging
│   │   ├── dbt_project.yml
│   │   ├── logs
│   │   ├── macros
│   │   ├── models
│   │   ├── target
│   │   └── tests
│   ├── logs
│   ├── main.py
│   ├── output_sql
│   │   ├── cert_dc_insern_2023_2024.sql
│   │   ...
│   │   └── v_region.sql
│   ├── pipeline
│   │   ├── __init__.py
│   │   ├── csv_management.py
│   │   ├── database_pipeline.py
│   │   ├── duckdb_pipeline.py
│   │   ├── load_yml.py
│   │   ├── metadata.yml
│   │   ├── postgres_loader.py
│   │   └── sftp_sync.py
│   ├── staging_tables.txt
│   └── staging_views.txt
├── poetry.lock
├── profiles.yml
├── pyproject.toml
├── README.md
└── uv.lock
```

## 6. Utilités des fichiers
### ./dbtCertDC/
Répertoire de fonctionnement des modèles DBT -> création de vue SQL.

dbt_project.yml : Fichier de configuration de DBT (obligatoire)

macros/ : Répertoire de stockage des macro jinja

models/ : Répertoire de stockage des modèles dbt

### ./pipeline/
Répertoire d'orchestration de la pipeline Python.

.env : Fichier secret contenant le paramétrage vers le SFTP et les mots de passes des bases de données postgres.

database_pipeline.py : Réalise les actions communes pour n'importe quelle database (lecture de fichier SQL, exécution du fichier sql, export de vues, run de la pipeline...). Fonctionne en complément avec duckdb_pipeline.py et postgres_loader.py.

duckdb_pipeline.py : Réalise les actions spécifiques à une base (connexion à la base, création de table, chargement des données dans la BDD). Fonctionne en complément avec database_pipeline.py.

postgres_loader.py : Réalise les actions spécifiques à une base postgres (connexion à la base, création de table, chargement des données dans la BDD). Fonctionne en complément avec database_pipeline.py.

sftp_sync.py : Réalise les actions relatives à une connexion SFTP (connexion, import, export...).

csv_management.py : Réalise les actions relatives à la manipulation de fichier csv (transformation d'un .xlsx en .csv, lecture du .csv avec délimiteur personnalisé, standarisation des colonnes, conversion des types, export ...).

metadata.yml : Contient les informations relatives aux fichiers .csv provenant du SFTP.

load_yml.py : Lit un fichier .yml


./main.py : Programme d'exécution de la pipeline


./output_sql/ : Répertoire qui contient les fichiers .sql de création de table (CREATE TABLE)


./logs/ : Répertoire de la log


./data/duckdb_database.duckdb : Base duckDB


./input/ : Répertoire de stockage des fichiers .csv en entrée
./output/ : Répertoire de stockage des fichiers .csv en sortie


./profiles.yml : Contient les informations relatives aux bases des différents projets.


./poetry.lock
./uv.lock
./pyproject.toml