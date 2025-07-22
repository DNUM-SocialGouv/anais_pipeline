# === Packages ===
from datetime import date
import argparse
from typing import Literal
from dotenv import load_dotenv

# === Modules ===
from pipeline.load_yml import load_metadata_YAML
from pipeline.logging_management import setup_logger
from pipeline.pipeline_orchestration import (anais_staging_pipeline,
                                             local_staging_pipeline,
                                             anais_project_pipeline,
                                             local_project_pipeline)

# === Paramètres ===
today = date.strftime(date.today(), "%Y_%m_%d")
load_dotenv()

CHOICES = ["Staging", "Helios", "InspectionControlePA", "Matrice_PA", "Matrice_PH", "CertDC"]


# === Fonction ===
def main(env: Literal["local", "anais"], profile: str, profile_yml: str  = "profiles.yml", metadata_yml: str = "metadata.yml"):
    """
    Exécution de la Pipeline selon le profil indiqué.

    Parameters
    ----------
    env : Literal["local", "anais"]
        Nom de l'environnement sur lequel exécuter.
    profile : str
        Nom du profil à exécuter.
    metadata : str, optional
        Nom de fichier contenant les metadatas nécessaires au bon fonctionnement du projet, by default "metadata.yml"
    """
    logger = setup_logger(env, f"logs/log_{env}.log")
    config = load_metadata_YAML(metadata_yml, profile, logger=logger)
    db_config = load_metadata_YAML(profile_yml, profile, logger, ".")["outputs"][env]

    if profile == "Staging":
        if env == "anais":
            anais_staging_pipeline(profile, config, db_config, logger)
        if env == "local":
            local_staging_pipeline(profile, config, db_config, logger)

    elif profile in CHOICES and profile != "Staging":
        staging_db_config = load_metadata_YAML(profile_yml, "Staging", logger, ".")["outputs"][env]

        if env == "anais":
            anais_project_pipeline(profile, config, db_config, staging_db_config, today, logger)
        if env == "local":
            local_project_pipeline(profile, config, db_config, staging_db_config, today, logger)


# === Exécution ===
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exécution du pipeline")
    parser.add_argument("--env", choices=["local", "anais"], default="local", help="Environnement d'exécution")
    parser.add_argument("--profile", choices=CHOICES, default="Staging", help="Profile dbt d'exécution")
    args = parser.parse_args()

    main(args.env, args.profile)
