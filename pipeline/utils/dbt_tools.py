# === Packages ===
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os
import subprocess
import logging
from pathlib import Path
from typing import Literal

# === Modules ===
from pipeline.utils.config import env_var, setup_config
from pipeline.utils.logging_management import configure_logging

# === Constantes ===
LOCAL_TZ = ZoneInfo("Europe/Paris")
# Horodatage dbt en début de ligne : "14:02:31  Running with dbt=1.9.2"
DBT_TIME_PATTERN = re.compile(r"^(\d{2}):(\d{2}):(\d{2})(?=\s)", re.MULTILINE)

# === Logger du module ===
logger = logging.getLogger(__name__)


# === Fonctions ===
def dbt_output_to_local_time(
    output: str, start_utc: datetime, tz: ZoneInfo = LOCAL_TZ
) -> str:
    """
    Convertit les horodatages UTC écrits par dbt en début de ligne vers l'heure locale.
    Les lignes sans horodatage (détails d'erreur, tableaux...) restent inchangées.

    Parameters
    ----------
    output : str
        Sortie brute de dbt (stdout ou stderr).
    start_utc : datetime
        Heure UTC du lancement de la commande dbt, sans microsecondes.
        Sert à dater les lignes et à gérer un passage de minuit pendant l'exécution.
    tz : ZoneInfo, optional
        Fuseau horaire cible, by default Europe/Paris.

    Returns
    -------
    str
        Sortie dbt avec les heures converties.
    """
    if not output:
        return ""

    def _convert(match: re.Match) -> str:
        hour, minute, second = map(int, match.groups())
        line_utc = start_utc.replace(hour=hour, minute=minute, second=second)
        # Heure inférieure au lancement : on est passé au jour suivant
        if line_utc < start_utc:
            line_utc += timedelta(days=1)
        return line_utc.astimezone(tz).strftime("%H:%M:%S")

    return DBT_TIME_PATTERN.sub(_convert, output)


def dbt_deps(project_path: str):
    return subprocess.run(["dbt", "deps", "--project-dir", project_path])


def dbt_exec(
    type_exec: str,
    profile: str,
    target: Literal["local", "anais"],
    project_dir: str,
    profiles_dir: str,
    install_deps: bool = True,
):
    """
    Fonction exécutant la commande 'dbt test' avec les différentes options.
    Exécute obligatoirement le répertoire 'base' dans les modèles dbt, ainsi que le répertoire choisi.

    Parameters
    ----------
    type_exec : str
        Type d'exécution dbt (run, test, compile)
    profile : str
        Profile dbt à utiliser parmis ceux dans 'profiles.yml'.
    target : Literal["local", "anais"]
        Choix de la base à utiliser : local ou anais.
    project_dir : str
        Répertoire du projet dbt à exécuter (contenant le 'dbt_project.yml')
    profiles_dir : str
        Répertoire du projet dbt (contenant le 'profiles.yml').
    """
    try:
        project_path = str(Path(project_dir).resolve())
        profiles_path = str(Path(profiles_dir).resolve())

        if (
            not os.path.exists(os.path.join(project_path, "package-lock.yml"))
            and install_deps
        ):
            dbt_deps_install = dbt_deps(project_path)
        start_utc = datetime.now(timezone.utc).replace(microsecond=0)

        result = subprocess.run(
            [
                "dbt",
                type_exec,
                "--project-dir",
                project_path,
                "--profiles-dir",
                profiles_path,
                "--profile",
                profile,
                "--target",
                target,
                "--select",
                f"+{project_dir}",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        logger.info(f"✅ Dbt {type_exec} de {project_dir} terminé avec succès")
        logger.info(dbt_output_to_local_time(result.stdout, start_utc))

    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Erreur lors du dbt {type_exec} :")
        logger.error(dbt_output_to_local_time(e.stdout, start_utc))
        logger.error(dbt_output_to_local_time(e.stderr, start_utc))


if __name__ == "__main__":
    logger = configure_logging("dbt")
    config_var = env_var()
    config_var = setup_config(config_var)

    dbt_exec(
        "run",
        config_var["profile"],
        config_var["env"],
        config_var["config"]["models_directory"],
        ".",
    )
    dbt_exec(
        "test",
        config_var["profile"],
        config_var["env"],
        config_var["config"]["models_directory"],
        ".",
    )
