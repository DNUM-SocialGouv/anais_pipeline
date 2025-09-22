# === Packages ===
from typing import Literal

# === Modules ===
from pipeline.utils.config import env_var, setup_config
from pipeline.pipeline_orchestration import (anais_staging_pipeline,
                                             local_staging_pipeline,
                                             anais_project_pipeline,
                                             local_project_pipeline)


# === Fonction ===
def main(config_var: dict):
    """
    Exécution de la Pipeline selon le profil indiqué.

    Parameters
    ----------
    config_var : dict
        Dictionnaire contenant les configurations nécessaires à l'utilisation de la pipeline :
            - liste des environnements possibles (env_choice)
            - liste des projets existants (profile_choice)
            - environnement (env)
            - profile (profile)
            - fichier de log (logger)
            - information de gestion du projet (config)
            - identifiants de la Database (db_config)
            - identifiants de la Database Staging (staging_db_config)
    """
    # Initialisation des configurations
    config_var = setup_config(config_var)
    profile_choice = config_var["profile_choice"]
    env = config_var["env"]
    profile = config_var["profile"]
    logger = config_var["logger"]
    config = config_var["config"]
    db_config = config_var["db_config"]
    staging_db_config = config_var["staging_db_config"]
    today = config_var["today"]

    # Exécution des pipelines dépendamment des paramètres d'entrées
    if profile == "Staging":
        if env == "anais":
            anais_staging_pipeline(profile, config, db_config, logger)
        if env == "local":
            local_staging_pipeline(profile, config, db_config, logger)

    elif profile in profile_choice and profile != "Staging":
        if env == "anais":
            anais_project_pipeline(profile, config, db_config, staging_db_config, today, logger)
        if env == "local":
            local_project_pipeline(profile, config, db_config, staging_db_config, today, logger)


# === Exécution ===
if __name__ == "__main__":
    config_var = env_var()

    main(config_var)
