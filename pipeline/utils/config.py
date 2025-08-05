# === Packages ===
from dotenv import load_dotenv
import argparse
from datetime import date

# === Modules ===
from pipeline.utils.load_yml import load_metadata_YAML
from pipeline.utils.logging_management import setup_logger

# === Constantes ===
load_dotenv()
ENV_CHOICE = ["local", "anais"]
PROFILE_CHOICE = ["Staging", "CertDC", "Helios", "InspectionControlePA", "InspectionControlePH"]
METADATA_YML = "metadata.yml"
PROFILE_YML = "profiles.yml"

# === Fonctions ===
def env_var() -> dict:
    """
    Configuration des variables d'environnement nécessaires lors du lancement de la pipeline (ou d'un morceau).
        - env : Nom de l'environnement sur lequel exécuter.
        - profile : Nom du profil à exécuter.

    Returns
    -------
    dict
        Dictionnaire contenant les clés env et profile, associé à leurs valeurs respectives.
    """
    parser = argparse.ArgumentParser(description="Exécution du pipeline")
    parser.add_argument("--env", choices=ENV_CHOICE, default=ENV_CHOICE[0], help="Environnement d'exécution")
    parser.add_argument("--profile", choices=PROFILE_CHOICE, default=PROFILE_CHOICE[0], help="Profile dbt d'exécution")
    args = parser.parse_args()
    env = args.env
    profile = args.profile

    config_var = {
        "env_choice": ENV_CHOICE,
        "profile_choice": PROFILE_CHOICE,
        "env": env,
        "profile": profile
        }
    return config_var


def setup_config(config_var: dict, metadata_yml: str = METADATA_YML, profile_yml: str  = PROFILE_YML) -> dict:
    """
    Fonction de setup de variable nécessaire à la pipeline.
        - logger : Fichier de log.
        - config : Metadata du profile (dans metadata.yml).

    Parameters
    ----------
    config_var : dict
        Dictionnaire contenant les clés env et profile, associé à leur valeur respective.
    metadata_yml : str
        Nom de fichier contenant les metadatas nécessaires au bon fonctionnement du projet, by default "metadata.yml"
    profile_yml : str
        Nom de fichier contenant les identifiants nécessaires à la connexion à la base de données, by default "profiles.yml"
    Returns
    -------
    dict
        Dictionnaire contenant les clés env, profile, logger, config, associé à leurs valeurs respectives.
    """
    env = config_var.get("env")
    profile = config_var.get("profile")

    logger = setup_logger(env, f"logs/log_{env}.log")

    config = load_metadata_YAML(metadata_yml, profile, logger, ".")
    db_config = load_metadata_YAML(profile_yml, profile, logger, ".")["outputs"][env]
    staging_db_config = load_metadata_YAML(profile_yml, "Staging", logger, ".")["outputs"][env]
    today = date.strftime(date.today(), "%Y_%m_%d")

    config_var["logger"] = logger
    config_var["config"] = config
    config_var["db_config"] = db_config
    config_var["staging_db_config"] = staging_db_config
    config_var["today"] = today
    

    return config_var
