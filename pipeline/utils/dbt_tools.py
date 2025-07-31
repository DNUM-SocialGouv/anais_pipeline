# === Packages ===
import os
from pathlib import Path
import subprocess
from typing import Literal
from logging import Logger

# === Modules ===
from pipeline.utils.config import env_var, setup_config

# === Fonctions ===
def dbt_deps(project_path: str):
    return subprocess.run(
        ["dbt",
         "deps",
         "--project-dir", project_path
        ])

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

        if not os.path.exists(os.path.join(project_path, "package-lock.yml")):
            dbt_deps_install = dbt_deps(project_path)

        result = subprocess.run(
            ["dbt",
             "run",
             "--project-dir", project_path,
             "--profiles-dir", profiles_path,
             "--profile", profile,
             "--target", target,
             "--select", f"+{project_dir}"
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

if __name__ == "__main__":
    config_var = env_var() 
    config_var = setup_config(config_var)

    run_dbt(config_var["profile"], config_var["env"], config_var["config"]["models_directory"], ".", config_var["logger"])