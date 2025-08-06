# === Packages ===
import duckdb
import os
from pathlib import Path
import pandas as pd
from logging import Logger

# === Modules ===
from pipeline.utils.csv_management import ColumnsManagement
from pipeline.database_management.database_pipeline import DataBasePipeline


# === Classes ===
# Classe DuckDBPipeline qui gère les actions relatives à une database duckdb
class DuckDBPipeline(DataBasePipeline):
    def __init__(self, db_config: dict, config: dict, logger: Logger, staging_db_config: dict = None):
        """
        Initialisation de la base DuckDB. Classe héritière de DataBasePipeline.

        Parameters
        ----------
        db_config : dict
            Paramètres de connexion vers la base.
        config : dict
            Metadata du profile (dans metadata.yml).
        logger : logging.Logger
            Fichier de log.
        staging_db_config : dict
            Paramètres de connexion vers la base Staging, None by default.
        """
        super().__init__(db_config, config, logger, staging_db_config)
        self.logger = logger
        self.db_path = db_config.get("path")
        self.schema = db_config.get("schema")
        self.typedb = db_config.get("type")
        self.staging_db_config = staging_db_config
        self.init_duckdb()

    def init_duckdb(self):
        """ Vérifie si la base DuckDB existe, sinon la crée. """
        db_dir = os.path.dirname(self.db_path)

        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            self.logger.info(f"Dossier créé pour la base DuckDB : {db_dir}")

        if not os.path.exists(self.db_path):
            self.logger.info("Création de la base DuckDB...")
            conn = duckdb.connect(self.db_path)
            conn.close()
        else:
            self.logger.info("La base DuckDB existe déjà.")

    def connect(self):
        """ Connexion à la base DuckDB. """
        self.conn = duckdb.connect(database=self.db_path)

    def is_duckdb_empty(self) -> bool:
        """ Vérifie si la base DuckDB est vide ou non """
        conn = self.conn
        result = conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """).fetchone()[0]
        conn.close()
        return result == 0

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
            Paramètres à injecter dans la requête SQL (Non nécessaire pour duckDB).
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

    def is_table_exist(self, conn, query_params: dict, print_log: bool = False) -> bool:
        """
        Indique si la table existe ou non.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Connexion à la base DuckDB.
        query_params : dict
            Paramètres à injecter dans la requête SQL.
        print_log : bool
            True si on souhaite afficher la log, False sinon, by default False.

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
            if print_log:
                self.logger.info(f"✅ La table '{table_name}' existe.")
            return True
        else:
            if print_log:
                self.logger.warning(f"❌ La table '{table_name}' n'existe pas.")
            return False

    def show_row_count(self, conn, query_params: dict):
        """
        Affiche le nombre de lignes d'une table.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Connexion à la base de données DuckDB.
        query_params : dict
            Nom de la table.
        """
        table = query_params["table"]

        row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if row_count == 0:
            self.logger.warning(f"⚠️ La table '{table}' est vide.")
        else:
            self.logger.info(f"✅ La table '{table}' contient {row_count} lignes.")

    def print_table(self, conn, query_params: dict, limit: int):
        """
        Affiche les premières lignes d'une table.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Connexion à la base de données DuckDB.
        query_params : dict
            Nom de la table.
        limit : int, optional
            Nombre de lignes à afficher si print_table est True, 10 by default.
        """
        table = query_params["table"]

        df = conn.execute(f"SELECT * FROM {table} LIMIT {limit}").fetchdf()
        self.logger.info(f"🔍 Aperçu de '{table}' ({limit} lignes) :\n{df.to_string(index=False)}")

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
        self.logger.info(f"📥 Chargement du fichier : {csv_file}")
        table_name = csv_file.stem
        self.query_params = {"schema": self.schema, "table": table_name}

        # Si la table est inexistante
        if not self.is_table_exist(conn, self.query_params):
            self.logger.warning(f"Table {table_name} non trouvée, impossible de charger {csv_file.name}")
            return

        schema_df = self.get_duckdb_schema(conn, table_name)

        # Chargement du csv
        pipeline = ColumnsManagement(csv_file=csv_file, schema_df=schema_df, logger=self.logger)
        df = pipeline.df

        # Vérification de la présence de la table
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        if row_count > 0:
            self.logger.info(f"Données déjà présentes dans {table_name}, passage du fichier CSV : {csv_file.name}")
        else:
            self.logger.info(f"🆕 Injection dans la table '{table_name}' à partir du CSV {csv_file}")
            try:
                conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
                self.logger.info(f"✅ Table '{table_name}' créée et remplie avec succès ({csv_file})")
            except duckdb.Error as e:
                self.logger.error(f"Erreur lors du chargement de {csv_file.name}: {e}")

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
                self.logger.warning("Aucune table trouvée dans la base DuckDB.")
                return

            self.logger.info("Tables disponibles dans DuckDB :")
            for schema, table in tables:
                self.logger.info(f" - {schema}.{table}")

        except Exception as e:
            self.logger.error(f"Erreur lors de la récupération des tables : {e}")

    def fetch_df(self, conn, table_name: str) -> pd.DataFrame:
        """
        Fonction de chargement d'une table depuis une base DuckDB.
        Importante pour l'export des csv.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Connexion à la base de données.
        table_name : str
            Nom de la table que l'on charge.

        Returns
        -------
        pd.DataFrame
            Dataframe de la table chargée.
        """
        tables = conn.execute("SHOW TABLES").fetchall()
        table_list = [t[0] for t in tables]
        # print(f"[DEBUG] Tables disponibles : {table_list}")
        if table_name not in table_list:
            raise ValueError(f"La table '{table_name}' n'existe pas dans la base.")
        # Obtenir les colonnes et leurs types
        table_info = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()

        print(f"[DEBUG] Colonnes et types de '{table_name}' :")

        for col in table_info:
            # col est une tuple comme : (column_id, name, type, not_null, default_value, primary_key)
            print(f" - {col[1]} : {col[2]}")
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"[DEBUG] Nombre de lignes dans '{table_name}' : {row_count}")


        # try:
        #     df = conn.execute(f"SELECT * FROM {table_name}").df()
        #     print(df.shape)
        #     return df
        # except Exception as e:
        #     print(f"Erreur lors de la récupération de la table {table_name} : {e}")

    def copy_table_from_staging(self, conn, staging_table_name: str, db_table_name: str):
        """
        Copie d'une table de la base Staging vers la base cible.

        Parameters
        ----------
        staging_table_name : str
            Nom de la table que l'on "copie".
        db_table_name : str
            Nom de la table que l'on "colle". 
        """
        if self.staging_db_config:
            # Attache la base Staging
            staging_db_path = Path(self.staging_db_config.get("path"))
            staging_db_schema = self.staging_db_config.get("schema")

            conn.execute(f"ATTACH '{staging_db_path}' AS staging_db")
            df = conn.execute(f"SELECT * FROM staging_db.sa_siicea_cibles LIMIT 5").fetchdf()
            print(df.shape)
            # Crée la table dans la base cible à partir de la table source
            print(staging_db_path)
            # conn.execute(f"""
            #     CREATE TABLE main.{db_table_name} AS
            #     SELECT * FROM staging_db.{staging_table_name}
            # """)
            # conn.execute("DETACH staging_db")
            self.logger.info(f"✅ La table {staging_table_name} a bien été récupérée de la base DuckDB Staging sous le nom {db_table_name}.")
            # Récupération de la table dans Staging

            # # Récupération de la table dans Staging
            # staging_db_path = Path(self.staging_db_config.get("path"))
            # staging_connect = duckdb.connect(staging_db_path)
            # df = staging_connect.execute(
            #     f"SELECT * FROM {staging_table_name}"
            # ).fetchdf()
            # staging_connect.close()

            # # Ajout de la table dans la base du projet
            # conn.register("temp_df", df)
            # print("Ok")
            # conn.execute(f"""
            #     CREATE TABLE IF NOT EXISTS {db_table_name} AS
            #     SELECT * FROM temp_df
            # """)
            self.logger.info(f"✅ La table {staging_table_name} a bien été récupérée de la base DuckDB Staging sous le nom {db_table_name}.")
        else:
            self.logger.error("❌ La configuration de la base Staging n'a pas été indiquée.")

    def close(self):
        """ Ferme la connexion à la base de données Duckdb. """
        self.conn.close()
        self.logger.info("Connexion à DuckDB fermée.")
