# === Packages ===
from logging import Logger
import os
import os.path

# === Modules ===
from pipeline.utils.sftp_sync import SFTPSync
from pipeline.database_management.duckdb_pipeline import DuckDBPipeline
from pipeline.database_management.postgres_loader import PostgreSQLLoader
from pipeline.utils.dbt_tools import dbt_exec

# === Fonctions ===
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
    dbt_exec("run", profile, "anais", config["models_directory"], ".", logger, install_deps=False)
    dbt_exec("test", profile, "anais", config["models_directory"], ".", logger)


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
    try:
        # Si la base duckDB Staging existe
        if os.listdir(config["local_directory_input"]) and os.listdir(config["create_table_directory"]):
            loader.run()
            
        else:
            logger.error(
            "❌ Aucun moyen de remplir la base DuckDB n'a été trouvé.\n"
            f"- Répertoires vides :\n"
            f"    > .csv : {config['local_directory_input']}\n"
            f"    > .sql : {config['create_table_directory']}"
        )
    finally:
        duckdb_empty = loader.is_duckdb_empty()
        loader.close()

    # Vérifie si la base DuckDB est vide ou non avant de lancer le run dbt
    if not duckdb_empty:
        # Création des vues et export
        dbt_exec("run", profile, "local", config["models_directory"], ".", logger, install_deps=False)
        dbt_exec("test", profile, "anais", config["models_directory"], ".", logger)
    else:
        logger.error(f"❌ Base {db_config["path"]} vide ")


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
    # --- Projet ---
    # Initialisation de la config postgres
    pg_loader = PostgreSQLLoader(
        db_config=db_config,
        config=config,
        logger=logger,
        staging_db_config=staging_db_config)

    # # Remplissage des tables de la base postgres
    pg_loader.connect()
    pg_loader.copy_table(config["table_to_copy"])

    # Création des vues et export
    dbt_exec("run", profile, "anais", config["models_directory"], ".", logger)
    dbt_exec("test", profile, "anais", config["models_directory"], ".", logger)

    # Upload les tables qui servent à la création des vues
    sftp = SFTPSync(config["local_directory_input"], logger)

    # pg_loader.export_csv(config["input_to_download"], date=today)
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
    # Initialisation de la config DuckDB
    ddb_loader = DuckDBPipeline(
        db_config=db_config,
        config=config,
        logger=logger,
        staging_db_config=staging_db_config
        )

    # Remplissage des tables de la base postgres  
    ddb_loader.connect()

    try:
        # Si la base duckDB Staging existe
        if os.path.isfile(staging_db_config["path"]):
            ddb_loader.copy_table(config["table_to_copy"])

        elif os.listdir(config["local_directory_input"]) and os.listdir(config["create_table_directory"]):
            ddb_loader.run()
        else:
            logger.error(
            "❌ Aucun moyen de remplir la base DuckDB n'a été trouvé.\n"
            f"- Base DuckDB Staging introuvable à : {staging_db_config['path']}\n"
            f"- OU répertoires vides :\n"
            f"    > .csv : {config['local_directory_input']}\n"
            f"    > .sql : {config['create_table_directory']}"
        )
    finally:
        duckdb_empty = ddb_loader.is_duckdb_empty()
        ddb_loader.close()

    # Vérifie si la base DuckDB est vide ou non avant de lancer le run dbt
    if not duckdb_empty:
        # Création des vues et export
        dbt_exec("run", profile, "local", config["models_directory"], ".", logger)
        dbt_exec("test", profile, "local", config["models_directory"], ".", logger)

        # Upload les vues
        ddb_loader.connect()
        # ddb_loader.export_csv(config["input_to_download"], date=today)
        ddb_loader.export_csv(config["files_to_upload"], date=today)
        ddb_loader.close()
    else:
        logger.error(f"❌ Base {db_config["path"]} vide ")