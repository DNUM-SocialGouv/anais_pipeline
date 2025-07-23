# === Packages ===
import os
from pathlib import Path
from typing import Callable, Any
import re
from logging import Logger

# === Modules ===
from pipeline.csv_management import TableInCsv


# === Classes ===
# Classe DataBasePipeline qui gère les actions relatives à n'importe quel database
class DataBasePipeline:
    def __init__(self, db_config: dict, config: dict, logger: Logger):
        """
        Classe qui réalise les actions communes pour n'importe quel database.
        Cette classe est héritée par une classe relative au type de base.
        ! Il est nécessaire de définir les éléments suivants dans la classe enfant:
            - self.conn = connexion à la base de données
            - self.schema = schéma de la base de données
            - self.typedb = type de la base de données
            - is_table_exist(self, conn, query_params: dict, print_log: bool) -> bool = fonction qui vérifie l'existence de la table
            - show_row_count(self, conn, query_params: dict) = fonction qui compte le nombre de lignes de la table
            - print_table(self, conn, query_params: dict, limit: int) = fonction qui affiche la table
            - create_table(self, conn, sql_query: str, query_params: str) = fonction d'exécution des fichier SQL de CREATE TABLE
            - load_csv_file(self, conn, csv_file: Path) = fonction d'injection des données d'un csv vers une table de la base de données

        Parameters
        ----------
        db_config : dict
            Paramètres de connexion vers la base.
        config : dict
            Metadata du profile (dans metadata.yml).
        logger : logging.Logger
            Fichier de log.
        """
        self.sql_folder = config["create_table_directory"]
        self.csv_folder_input = config["local_directory_input"]
        self.csv_folder_output = config["local_directory_output"]
        self.db_config = db_config
        self.logger = logger
        self.ensure_directories_exist()

    def ensure_directories_exist(self):
        """ Crée les dossiers nécessaires s'ils n'existent pas. """
        folders = [self.sql_folder, self.csv_folder_input, self.csv_folder_output]

        for folder in folders:
            os.makedirs(folder, exist_ok=True)
            self.logger.info(f"Dossier vérifié/créé : {folder}")

    def read_sql_file(self, sql_file: Path) -> str:
        """
        Lit un fichier sql.

        Parameters
        ----------
        sql_file : Path
            Fichier sql à lire.

        Returns
        -------
        str
            Contenu du fichier SQL.      
        """
        with open(sql_file, "r", encoding="utf-8") as f:
            return f.read()

    def find_table_name_in_sql(self, sql_file: str) -> str:
        """
        Cherche le nom de la table dans un fichier sql (seulement un fichier SQL CREATE TABLE).

        Parameters
        ----------
        sql_file : str
            Fichier sql de CREATE TABLE.

        Returns
        -------
        str
            Nom de la table.
        """
        match = re.compile(
            r"CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(?P<name>(\"[^\"]+\"|\w+))",
            re.IGNORECASE
        ).search(sql_file)

        if match:
            table_name = match.group("name").strip('"')
            return table_name
        else:
            return None

    def check_table(
        self,
        conn,
        query_params: dict,
        print_table: bool = False,
        show_row_count: bool = False
    ) -> bool:
        """
        Vérifie l'existence d'une table, son nombre de ligne (optionnel) et affiche son contenu (optionnel).

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Connection | duckdb.DuckDBPyConnection
            Connexion à la base de données.
        table_name : str
            Nom de la table à vérifier.
        query_params : dict
            Paramètres à injecter dans la requête SQL.
        print_table : bool, optional
            Affiche les premières lignes de la table si elle existe, False by default.
        show_row_count : bool, optional
            Affiche le nombre total de lignes, False by default.

        Returns
        -------
        bool
            True si la table existe (et non vide si applicable), False sinon.
        """
        try:
            # Vérifie si la table existe ou non
            table_exist = self.is_table_exist(conn, query_params, True)

            if table_exist:
                # Vérifie si la table est remplie ou non
                if show_row_count:
                    self.show_row_count(conn, query_params)

                # Affichage des premières valeurs d'une table
                if print_table:
                    self.print_table(conn, query_params)
            return table_exist

        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la vérification de la table '{query_params["table"]}' → {e}")
            return False

    def execute_sql_file(self, conn, sql_file: Path, create_table_func: Callable[[Any, str, dict], None]):
        """
        Exécute un fichier SQL Create Table.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Connection | duckdb.DuckDBPyConnection
            Connexion à la base de données.
        sql_file : Path
            Fichier SQL Create table.
        create_table_func : Callable[[Any, str, dict], None]
            Fonction create_table relative au type de base.
        """
        sql = self.read_sql_file(sql_file)
        table_name = self.find_table_name_in_sql(sql)

        if not table_name:
            self.logger.info(f"❌ Nom de table introuvable dans le fichier SQL : '{sql_file.name}'")
            return
        else:
            query_params = {"schema": self.schema, "table": table_name}

            try:
                create_table_func(conn, sql, query_params)
                self.logger.info(f"✅ Table créée avec succès : {sql_file.name}")
            except Exception as e:
                self.logger.error(f"❌ Erreur lors de l'exécution du SQL {sql_file.name}: {e}")

    def import_csv(self, views_to_import: dict):
        """
        Importe les vues vers un format csv.

        Parameters
        ----------
        views_to_import : dict
            Liste des vues à importer.
        """
        conn = self.conn
        for table_name, csv_name in views_to_import.items():
            if table_name:
                transfo = TableInCsv(conn, table_name, csv_name, self.fetch_df, self.csv_folder_input, self.logger)
                transfo.import_to_csv()
            else:
                self.logger.warning("⚠️ Aucune table spécifiée")

    def export_csv(self, views_to_export: dict, date: str):
        """
        Exporte les vues vers un format csv.

        Parameters
        ----------
        views_to_export : dict
            Liste des vues à exporter.
        date : str
            Date présente dans le nom des fichiers à exporter.
        """
        conn = self.conn
        for table_name, csv_name in views_to_export.items():
            if table_name:
                transfo = TableInCsv(conn, table_name, csv_name, self.fetch_df, self.csv_folder_output, self.logger)
                transfo.export_to_csv(date=date)
            else:
                self.logger.warning("⚠️ Aucune table spécifiée")

    def run(self):
        """
        Exécute toutes les étapes suivantes :
            - Création des tables SQL dans la base
            - Chargement des CSV et injection dans les tables
            - Vérification de leur création
        """
        conn = self.conn
        for sql_file in Path(self.sql_folder).glob("*.sql"):
            self.execute_sql_file(conn, sql_file, self.create_table)

        self.logger.info(f"Début du chargement des fichiers CSV vers {self.typedb}.")
        for csv_file in Path(self.csv_folder_input).glob("*.csv"):
            self.load_csv_file(conn, csv_file)
        self.logger.info(f"Fin du chargement des fichiers CSV vers {self.typedb}.")

        for csv_file in Path(self.csv_folder_input).glob("*.csv"):
            query_params = {"schema": self.schema, "table": csv_file.stem}
            self.check_table(conn, query_params, print_table=False, show_row_count = True)
