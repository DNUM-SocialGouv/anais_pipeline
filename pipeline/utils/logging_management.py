# === Packages ===
import logging
from pathlib import Path

# === Constantes ===
LOG_DIR = "logs"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
PIPELINE_LOGGER_NAME = "pipeline"


# === Fonctions ===
def configure_logging(
    log_name: str,
    log_dir: str = LOG_DIR,
    level: int = logging.INFO,
    console: bool = False,
) -> logging.Logger:
    """
    Configure une seule fois le logging de tout le projet, depuis le point d'entrée.
    Tous les loggers créés avec logging.getLogger(__name__) dans le package
    'pipeline' héritent de cette configuration.

    Parameters
    ----------
    log_name : str
        Suffixe du fichier de log : 'local', 'anais' (plus tard 'sftp', 'dbt').
        Le fichier produit est '{log_dir}/log_{log_name}.log'.
    log_dir : str, optional
        Répertoire des fichiers de log, by default "logs".
    level : int, optional
        Niveau de log minimal, by default logging.INFO.
    console : bool, optional
        Affiche aussi les logs dans la console, by default False.

    Returns
    -------
    logging.Logger
        Logger parent du package 'pipeline'.
    """
    log_path = Path(log_dir) / f"log_{log_name}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(PIPELINE_LOGGER_NAME)
    logger.setLevel(level)

    # Idempotence : un second appel remplace la configuration au lieu de l'empiler
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # Les logs du projet ne remontent pas au root logger (pas de doublons)
    logger.propagate = False

    return logger