# === Packages ===
import os
import logging


# === Fonctions ===
def setup_logger(name: str, log_file: str, level=logging.INFO) -> logging.Logger:
    """
    Crée ou récupère un logger qui écrit dans le fichier spécifié.

    Parameters
    ----------
    name : str
        Nom du logger.
    log_file : str
        Chemin vers le fichier log (ex: "logs/mon_module.log").
    level : int
        Niveau de log (ex: logging.INFO, logging.DEBUG).

    Returns
    -------
    logging.Logger
        Instance configurée du logger.
    """
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:  # Évite les doublons si le logger est déjà configuré
        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
