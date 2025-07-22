# === Packages ===
import os
from paramiko import SFTPClient, Transport, SFTPAttributes
import datetime
from dotenv import load_dotenv
from typing import Optional, List, Dict
from logging import Logger

# === Modules ===
from pipeline.csv_management import TransformExcel

# === Classes ===
# Classe SFTPSync
class SFTPSync:
    def __init__(self, output_folder: str, logger: Logger):
        """
        Connexion au SFTP pour la récupération et l'upload de fichier.

        Parameters
        ----------
        output_folder : str
            Répertoire de destination des fichiers téléchargés.
        logger : logging.Logger
            Fichier de log.
        """
        self.logger = logger
        self.output_folder = output_folder
        load_dotenv()
        self.host = os.getenv("SFTP_HOST")
        self.port = int(os.getenv("SFTP_PORT"))
        self.username = os.getenv("SFTP_USERNAME")
        self.password = os.getenv("SFTP_PASSWORD")

    def connect(self):
        """
        Initialisation de la connexion SFTP.
        """
        try:
            self.transport = Transport((self.host, self.port))
            self.transport.connect(username=self.username, password=self.password)
            self.sftp = SFTPClient.from_transport(self.transport)
            self.logger.info("Connexion SFTP établie.")
        except Exception as e:
            self.logger.error(f"Erreur de connexion SFTP : {e}")
            raise

    def sftp_dir_exists(self, path: str) -> bool:
        """
        Vérifie l'existence d'un répertoire SFTP.

        Parameters
        ----------
        path : str
            Répertoire à vérifier.

        Returns
        -------
        bool
            True si le répertoire existe, False sinon
        """
        try:
            self.sftp.chdir(path)
            return True
        except IOError:
            return False

    def get_latest_file(self, remote_dir: str, keyword: str) -> Optional[SFTPAttributes]:
        """
        Sur le SFTP, récupère le fichier le plus récent contenant la chaine de caractère définie.

        Parameters
        ----------
        remote_dir : str
            Répertoire du fichier à trouver sur le SFTP.
        keyword : str
            Chaine de caractère présente dans le nom du fichier.

        Returns
        -------
        Optional[SFTPAttributes]
            Fichier le plus récent contenant la chaine de caractère.
        """
        try:
            files = self.sftp.listdir_attr(remote_dir)
            matching_files = [
                f for f in files
                if keyword in f.filename and not f.filename.endswith((".gpg", ".xlsx"))
            ]
            if 'DIAMANT' in remote_dir:
                matching_files = [
                    f for f in files
                    if keyword in f.filename and not f.filename.endswith((".gpg"))
                ]
            if not matching_files:
                self.logger.warning(f"Aucun fichier correspondant à '{keyword}' dans {remote_dir}")
                return None
            return max(matching_files, key=lambda f: f.st_mtime)
        except FileNotFoundError:
            self.logger.warning(f"Dossier introuvable : {remote_dir}")
            return None

    def download_file(self, remote_dir: str, local_path: str):
        """
        Récupère un fichier spécifié depuis le SFTP vers le répertoire local.

        Parameters
        ----------
        remote_dir : str
            Répertoire du fichier à récupérer sur le SFTP.
        local_path : str
            Répertoire local.
        """
        try:
            self.sftp.get(remote_dir, local_path)
            self.logger.info(f"Téléchargé : {remote_dir} → {local_path}")
        except Exception as e:
            self.logger.error(f"Échec du téléchargement {remote_dir} : {e}")

    def download_all(self, files_list: List[Dict[str, str]]):
        """
        Téléchargement de tous les fichiers indiqués dans files_list, depuis le SFTP.
        Les fichiers sont enregistrés sous format csv dans un fichier local.

        Parameters
        ----------
        files_list : List[Dict[str, str]]
            Listes des fichiers à télécharger contenant :
             - path : répertoire dans lequel trouver le fichier
             - keyword : chaine de caractère à trouver dans le nom du fichier
             - file : nom du fichier csv en sortie
        """
        self.connect()
        files_to_download = [
            (item["path"], item["keyword"], item["file"])
            for item in files_list
        ]

        # Boucle parcourant chaque fichier à télécharger
        for remote_dir, keyword, local_filename in files_to_download:
            self.logger.info(f"Recherche du fichier contenant '{keyword}' dans {remote_dir}")
            latest_file = self.get_latest_file(remote_dir, keyword)

            if latest_file:
                remote_path = os.path.join(remote_dir, latest_file.filename)
                mod_time = datetime.datetime.fromtimestamp(latest_file.st_mtime)
                local_path = os.path.join(self.output_folder, local_filename)
                self.logger.info(f"Dernière version : {latest_file.filename} (modifié le {mod_time})")

                # Gestion des fichiers au format excel (DIAMANT)
                if '.xlsx' in latest_file.filename:
                    local_xlsx_path = local_path.replace('.csv', '.xlsx')
                    self.download_file(remote_path, local_xlsx_path)
                    TransformExcel(local_xlsx_path, local_path, logger=self.logger)
                # Autres fichiers au format csv
                else:
                    self.download_file(remote_path, local_path)
        self.close()

    def upload_file_to_sftp(self, views_to_export: dict, output_dir: str, remote_dir: str, date: str):
        """
        Exporte les vues enregistrées en csv du répertoire local au répertoire distant.

        Parameters
        ----------
        views_to_export : dict
            Liste des vues à upload.
        output_dir : str
            Répertoire dans lequels trouver les fichiers csv.
        remote_dir : str
            Répertoire sur le SFTP où upload les fichiers.
        date : str
            Date présente dans le nom des fichiers à exporter.
        """
        self.connect()
        if remote_dir:
            if not self.sftp_dir_exists(remote_dir):
                self.logger.error(f"❌ Répertoire SFTP inexistant : {remote_dir}")
            else:
                for _, csv_name in views_to_export.items():
                    # Récupère le nom du fichier csv
                    file_name = f'{csv_name}_{date}.csv'
                    local_path = os.path.join(output_dir, file_name)
                    remote_path = os.path.join(remote_dir, file_name)

                    # Vérifie l'existence du fichier csv dans output
                    if os.path.exists(local_path):
                        try:
                            self.sftp.put(local_path, remote_path)
                            self.logger.info(f"✅ Upload réussi: {file_name} → {remote_path}")
                        except Exception as e:
                            self.logger.error(f"❌ Échec de l'upload {file_name} → {e}")
                    else:
                        self.logger.warning(f"⚠️ Fichier introuvable : {local_path}")
        self.close()

    def close(self):
        """ Fermeture de la connexion SFTP """
        self.sftp.close()
        self.transport.close()
        self.logger.info("Connexion SFTP fermée.")
