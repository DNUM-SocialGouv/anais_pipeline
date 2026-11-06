from pipeline.orchestration.pipeline_orchestration import (
    anais_project_pipeline,
    anais_staging_pipeline,
    local_project_pipeline,
    local_staging_pipeline,
)
from pipeline.utils.config import env_var, setup_config


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
            - information de gestion du projet (config)
            - identifiants de la Database (db_config)
            - identifiants de la Database Staging (staging_db_config)
    """
    # Initialisation des configurations
    logger = configure_logging(config_var["env"], console=config_var["env"] == "local")
    logger.info(f"Lancement de la pipeline - env={config_var['env']} - profile={config_var['profile']}")

    config_var = setup_config(config_var)
    profile_choice = config_var["profile_choice"]
    env = config_var["env"]
    profile = config_var["profile"]
    config = config_var["config"]
    db_config = config_var["db_config"]
    staging_db_config = config_var["staging_db_config"]
    today = config_var["today"]

    # Exécution des pipelines dépendamment des paramètres d'entrées
    if profile == "Staging":
        if env == "anais":
            anais_staging_pipeline(profile, config, db_config)
        if env == "local":
            local_staging_pipeline(profile, config, db_config)

    elif profile in profile_choice and profile != "Staging":
        if env == "anais":
            anais_project_pipeline(
                profile, config, db_config, staging_db_config, today
            )
        if env == "local":
            local_project_pipeline(
                profile, config, db_config, staging_db_config, today
            )


# === Exécution ===
if __name__ == "__main__":
    config_var = env_var()

    main(config_var)
