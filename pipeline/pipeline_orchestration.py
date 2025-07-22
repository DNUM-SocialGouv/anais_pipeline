# === Packages ===
from pathlib import Path
import subprocess
from typing import Literal
from logging import Logger

# === Modules ===
from pipeline.duckdb_pipeline import DuckDBPipeline
from pipeline.sftp_sync import SFTPSync
from pipeline.postgres_loader import PostgreSQLLoader


# === Fonctions ===
def run_dbt(profile: str, target: Literal["local", "anais"], project_dir: str, profiles_dir: str, logger: Logger):
    """
    Fonction exécutant la commande 'dbt run' avec les différentes options.
    Exécute obligatoirement le répertoire 'base' dans les modèles dbt, ainsi que le répertoire choisi.

    Parameters
    ----------
    profile : str
        Profile dbt à utiliser parmis ceux dans 'profiles.yml'.
    target : Literal["local", "anais"]
        Choix de la base à utiliser : local ou anais.
    project_dir : str
        Répertoire du projet dbt à exécuter (contenant le 'dbt_project.yml')
    profiles_dir : str
        Répertoire du projet dbt (contenant le 'profiles.yml').
    logger : Logger
        Fichier de log.
    """
    try:
        project_path = str(Path(project_dir).resolve())
        profiles_path = str(Path(profiles_dir).resolve())

        result = subprocess.run(
            ["dbt",
             "run",
             "--project-dir", project_path,
             "--profiles-dir", profiles_path,
             "--profile", profile,
             "--target", target
             ],
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"✅ Dbt run de {project_dir} terminé avec succès")
        logger.info(result.stdout)

    except subprocess.CalledProcessError as e:
        logger.error("❌ Erreur lors du dbt run :")
        logger.error(e.stdout)


def anais_staging_pipeline(profile: str, config: dict, db_config: dict, logger: Logger):
    """
    Pipeline exécuter pour Staging sur anais.
    Etapes:
        1. Récupération des fichiers d'input csv depuis le SFTP
        2. Connexion à la base Postgres Staging
        3. Création des tables et injection des données
        4. Création des vues via DBT

    Parameters
    ----------
    profile : str
        Profile dbt à utiliser parmis ceux dans 'profiles.yml'.
    config : dict
        Metadata du profile (dans metadata.yml).
    db_config : dict
        Paramètres de configuration de la base DuckDB (dans 'profiles.yml').
    logger : Logger
        Fichier de log.
    """
    # Initialisation de la config postgres
    pg_loader = PostgreSQLLoader(
        db_config=db_config,
        config=config,
        logger=logger)

    # Récupération des fichiers sur le sftp
    sftp = SFTPSync(config["local_directory_input"], logger)
    sftp.download_all(config["files_to_download"])

    # Remplissage des tables de la base postgres
    pg_loader.connect()
    pg_loader.run()
    pg_loader.close()

    # Création des vues et export
    run_dbt(profile, "anais", config["models_directory"], ".", logger)


def local_staging_pipeline(profile: str, config: dict, db_config: dict, logger: Logger):
    """
    Pipeline exécuter pour Staging en local.
    Etapes:
        1. Connexion à la base DuckDB Staging
        2. Création des tables et injection des données
        3. Création des vues via DBT

    Parameters
    ----------
    profile : str
        Profile dbt à utiliser parmis ceux dans 'profiles.yml'.
    config : dict
        Metadata du profile (dans metadata.yml).
    db_config : dict
        Paramètres de configuration de la base DuckDB (dans 'profiles.yml').
    logger : Logger
        Fichier de log.
    """
    # Initialisation de la config DuckDB
    loader = DuckDBPipeline(
        db_config=db_config,
        config=config,
        logger=logger)

    # Remplissage des tables de la base DuckDB
    loader.connect()
    loader.run()
    loader.close()

    # Création des vues et export
    run_dbt(profile, "local", config["models_directory"], ".", logger)


def anais_project_pipeline(profile: str, config: dict, db_config: dict, staging_db_config: dict, today: str, logger: Logger):
    """
    Pipeline exécuter pour un projet (différent de Staging) sur anais.
    Etapes:
        1. Connexion à la base Postgres Staging
        2. Récupération des tables nécessaires de la base Staging -> enregistrement au format csv
        3. Connexion à la base Postgres du projet spécifié
        4. Création des tables et injection des données
        5. Création des vues via DBT
        6. Export des vues au format csv
        7. Envoi des fichiers csv de vues sur le SFTP

    Parameters
    ----------
    profile : str
        Profile dbt à utiliser parmis ceux dans 'profiles.yml'.
    config : dict
        Metadata du profile (dans metadata.yml).
    db_config : dict
        Paramètres de configuration de la base DuckDB (dans 'profiles.yml').
    staging_db_config : dict
        Paramètres de configuration de la base DuckDB Staging (dans 'profiles.yml').
    today : str
        Date du jour (format YYYY_MM_DD), utilisée dans le nommage des fichiers exportés.
    logger : Logger
        Fichier de log.
    """
    # --- Staging ---
    # Initialisation de la config postgres
    pg_staging_loader = PostgreSQLLoader(
        db_config=staging_db_config,
        config=config,
        logger=logger)

    # Récupération des tables provenant de Staging
    pg_staging_loader.connect()
    pg_staging_loader.import_csv(config["input_to_download"])
    pg_staging_loader.close()

    # --- Projet ---
    # Initialisation de la config postgres
    pg_loader = PostgreSQLLoader(
        db_config=db_config,
        config=config,
        logger=logger)

    # Remplissage des tables de la base postgres
    pg_loader.connect()
    pg_loader.run()

    # Création des vues et export
    run_dbt(profile, "anais", config["models_directory"], ".", logger)

    # Upload les tables qui servent à la création des vues
    sftp = SFTPSync(config["local_directory_input"], logger)

    pg_loader.export_csv(config["input_to_download"], date=today)
    # sftp.upload_file_to_sftp(config["input_to_download"], config["local_directory_output"], config["remote_directory_input"], date=today)

    # Upload les vues
    pg_loader.export_csv(config["files_to_upload"], date=today)
    # sftp.upload_file_to_sftp(config["files_to_upload"], config["local_directory_output"], config["remote_directory_output"], date=today)
    pg_loader.close()


def local_project_pipeline(profile: str, config: dict, db_config: dict, staging_db_config: dict, today: str, logger: Logger):
    """
    Pipeline exécuter pour un projet (différent de Staging) en local.
    Nécessite la présence des fichiers csv dans le répertoire d'input.
    Etapes:
        1. Connexion à la base DuckDB Staging
        2. Récupération des tables nécessaires de la base Staging -> enregistrement au format csv
        3. Connexion à la base DuckDB du projet spécifié
        4. Création des tables et injection des données
        5. Création des vues via DBT
        6. Export des vues au format csv en local

    Parameters
    ----------
    profile : str
        Profile dbt à utiliser parmis ceux dans 'profiles.yml'.
    config : dict
        Metadata du profile (dans metadata.yml).
    db_config : dict
        Paramètres de configuration de la base DuckDB (dans 'profiles.yml').
    staging_db_config : dict
        Paramètres de configuration de la base DuckDB Staging (dans 'profiles.yml').
    today : str
        Date du jour (format YYYY_MM_DD), utilisée dans le nommage des fichiers exportés.
    logger : Logger
        Fichier de log.
    """
    # Initialisation de la config duckDB
    ddb_staging_loader = DuckDBPipeline(
        db_config=staging_db_config,
        config=config,
        logger=logger)

    # Récupération des tables provenant de Staging
    ddb_staging_loader.connect()
    ddb_staging_loader.import_csv(config["input_to_download"])
    ddb_staging_loader.close()

    # Initialisation de la config DuckDB
    ddb_loader = DuckDBPipeline(
        db_config=db_config,
        config=config,
        logger=logger)

    ddb_loader.connect()
    ddb_loader.run()
    ddb_loader.close()

    # Création des vues et export
    run_dbt(profile, "local", config["models_directory"], ".", logger)

    # Upload les vues
    ddb_loader.connect()
    ddb_loader.export_csv(config["files_to_upload"], date=today)
    ddb_loader.close()
