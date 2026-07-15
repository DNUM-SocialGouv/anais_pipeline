# Packages
import duckdb
import os
from pathlib import Path
import logging
import pandas as pd

# Modules
from pipeline.utils.csv_management import csv_pipeline
from pipeline.database_management.database_pipeline import DataBasePipeline

# === Configuration du logger ===
# Configuration du logger DBT
os.makedirs("logs", exist_ok=True)
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(
    filename="logs/duckdb_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Classe DuckDBPipeline qui gère les actions relatives à une database duckdb
class DuckDBPipeline(DataBasePipeline):
    def __init__(self,
                sql_folder: str = "Staging/output_sql/",
                csv_folder_input: str = "input/",
                csv_folder_output: str = "output/",
                db_path: str = 'data/duckdb_database.duckdb'):
        """
        Initialisation de la base DuckDB. Classe héritière de DataBasePipeline.

        Parameters
        ----------
        sql_folder : str, optional
            Répertoire des fichiers SQL CREATE TABLE, by default "Staging/output_sql/".
        csv_folder_input : str, optional
            Répertoire des fichiers csv importés, by default "input/".
        csv_folder_output : str, optional
            Répertoire des fichiers csv exportés, by default "output/".
        db_path : str, optional
            Répertoire de la base duckdb, by default 'data/duckdb_database.duckdb'.
        """
        super().__init__(sql_folder=sql_folder,
                         csv_folder_input=csv_folder_input,
                         csv_folder_output=csv_folder_output)
        self.db_path = db_path
        self.schema = "local"
        self.typedb = "duckdb"
        self.init_duckdb()
        self.conn = duckdb.connect(database=self.db_path)

    def init_duckdb(self):
        """ Vérifie si la base DuckDB existe, sinon la crée. """
        if not os.path.exists(self.db_path):
            logging.info("Création de la base DuckDB...")
            conn = duckdb.connect(self.db_path)
            conn.close()
        else:
            logging.info("La base DuckDB existe déjà.")

    def create_table(self, conn, sql_query: str, query_params: dict):
        """
        Exécution du fichier SQL Create Table.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Connexion à la base DuckDB.
        sql_query : str
            Contenu du fichier SQL Create table.
        query_params : dict
            Paramètres à injecter dans la requête SQL (Non nécessaire).
        """
        conn.execute(sql_query)

    def get_duckdb_schema(self, conn, table_name: str) -> pd.DataFrame:
        """
        Récupération du schéma DuckDB pour une table spécifique.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Connexion à la base DuckDB.
        table_name : str
            Nom de la table.

        Returns
        -------
        pd.DataFrame
            Schéma de la table contenant le nom des colonnes, leur type et leur format.
        """
        return conn.execute(f"DESCRIBE {table_name}").fetchdf()

    def is_table_exist(self, conn, query_params: dict) -> bool:
        """
        Indique si la table existe ou non.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Connexion à la base DuckDB.
        query_params : dict
            Paramètres à injecter dans la requête SQL.

        Returns
        -------
        bool
            True si la table existe (et non vide si applicable), False sinon.
        """
        table_name = query_params['table']
        table_exists = conn.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.tables 
                WHERE table_name = '{table_name}'
                """).fetchone()[0]

        if table_exists:
            # logging.warning(f"✅ La table '{table_name}' existe déjà.")
            return True
        else:
            logging.warning(f"❌ La table '{table_name}' n'existe pas.")
            return False

    def show_row_count(self, conn, query_params: dict):
        """
        Affiche le nombre de lignes d'une table.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Connexion à la base de données.
        query_params : dict
            Nom de la table.
        """
        table = query_params["table"]

        row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if row_count == 0:
            logging.warning(f"⚠️ La table '{table}' est vide.")
        else:
            logging.info(f"✅ La table '{table}' contient {row_count} lignes.")

    def print_table(self, conn, query_params: dict, limit: int):
        """
        Affiche les premières lignes d'une table.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Connexion à la base de données.
        query_params : dict
            Nom de la table.
        limit : int, optional
            Nombre de lignes à afficher si print_table est True, 10 by default.
        """
        table = query_params["table"]

        df = conn.execute(f"SELECT * FROM {table} LIMIT {limit}").fetchdf()
        logging.info(f"🔍 Aperçu de '{table}' ({limit} lignes) :\n{df.to_string(index=False)}")

    def load_csv_file(self, conn, csv_file: Path):
        """
        Charge un fichier CSV et l'injecte dans la base DuckDB.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Connexion à la base de données.
        csv_file : Path
            Fichier csv.
        """
        logging.info(f"📥 Chargement du fichier : {csv_file}")
        table_name = csv_file.stem
        self.query_params = {"schema": self.schema, "table": table_name}

        # Si la table est inexistante
        if not self.is_table_exist(conn, self.query_params):
            logging.warning(f"Table {table_name} non trouvée, impossible de charger {csv_file.name}")
            return

        schema_df = self.get_duckdb_schema(conn, table_name)

        # Chargement du csv
        df = csv_pipeline(csv_file, schema_df)

        # Vérification de la présence de la table
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        if row_count > 0:
            logging.info(f"Données déjà présentes dans {table_name}, passage du fichier CSV : {csv_file.name}")
        else:
            logging.info(f"🆕 Injection dans la table '{table_name}' à partir du CSV {csv_file}")
            try:
                conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
                logging.info(f"✅ Table '{table_name}' créée et remplie avec succès ({csv_file})")
            except duckdb.Error as e:
                logging.error(f"Erreur lors du chargement de {csv_file.name}: {e}")

    def list_tables(self, conn):
        """
        Liste toutes les tables existantes dans la base DuckDB.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Connexion à la base de données.
        """
        try:
            tables = conn.execute("SELECT table_schema, table_name FROM information_schema.tables").fetchall()

            if not tables:
                logging.warning("Aucune table trouvée dans la base DuckDB.")
                return

            logging.info("Tables disponibles dans DuckDB :")
            for schema, table in tables:
                logging.info(f" - {schema}.{table}")

        except Exception as e:
            logging.error(f"Erreur lors de la récupération des tables : {e}")

    def fetch_df(self, table_name: str) -> pd.DataFrame:
        """
        Fonction de chargement d'une table depuis une base DuckDB.
        Importante pour l'export des csv.

        Parameters
        ----------
        table_name : str
            Nom de la table que l'on charge.

        Returns
        -------
        pd.DataFrame
            Dataframe de la table chargée.
        """
        return self.conn.execute(f"SELECT * FROM {table_name}").df()

    def close(self):
        """ Ferme la connexion à la base de données Duckdb. """
        self.conn.close()
        logging.info("Connexion à DuckDB fermée.")
