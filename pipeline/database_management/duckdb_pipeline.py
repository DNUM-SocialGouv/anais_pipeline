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
            # self.logger.info("La base DuckDB existe déjà. Suppression en cours...")
            # os.remove(self.db_path)
            self.logger.info("Création de la base DuckDB.")
            conn = duckdb.connect(self.db_path)
            conn.close()

    def connect(self):
        """ Connexion à la base DuckDB. """
        self.logger.info("Connexion à la base DuckDB.")
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
        df = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
        return df


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
            query_params = {"schema": self.schema, "table": db_table_name}

            # Récupération de la table dans Staging
            staging_db_path = Path(self.staging_db_config.get("path"))
            
            # Connexion à la base staging
            staging_conn = duckdb.connect(staging_db_path)
            df = staging_conn.execute(f"SELECT * FROM {staging_table_name}").fetchdf()
            staging_conn.close()

            try:
                conn.register("staging_data", df)
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {db_table_name} AS
                    SELECT * FROM staging_data
                """)

                self.logger.info(f"✅ La table {staging_table_name} a bien été récupérée de la base DuckDB Staging sous le nom {db_table_name}.")
                
            except Exception as e:
                self.logger.error(f"❌ Erreur lors de la copie de la table {db_table_name} provenant de staging : {e}")  

        else:
            self.logger.error("❌ La configuration de la base Staging n'a pas été indiquée.")

    def copy_table_into_new(self, conn, source: str, target: str):
        """
        Copie une table dans une nouvelle.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Connexion à la base de données.
        source : str
            Nom de la table que l'on "copie".
        target : str
            Nom de la table à laquelle on ajoute les données de la première.
        """
        query = f"CREATE TABLE {target} AS SELECT * FROM {source}"
        conn.execute(query)
        self.logger.info(f"✅ Table {source} copiée vers {target}")

    def append_table(self, conn, source: str, target: str):
        """
        Ajoute les données de la table source à la table target.
        Les deux tables doivent avoir la même structure (mêmes colonnes et types).

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Connexion à la base de données.
        source : str
            Nom de la table que l'on "copie".
        target : str
            Nom de la table à laquelle on ajoute les données de la première. 

        """
        query = f"""
            INSERT INTO {target}
            SELECT * FROM {source}
        """
        conn.execute(query)
        self.logger.info(f"✅ Données de {source} ajoutées à {target}")

    def add_current_date_if_not_exist(self, conn, table_name: str, column_name: str):
        """
        Ajoute la date du jour (date d'historisation) à une table.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Connexion à la base DuckDB.
        table_name : str
            Nom de la table à vider.
        column_name : str
            Nom de la colonne date.
        """
        conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN date_historisation DATE')

    # def add_date_if_exist(self, conn, table_name: str):
    #     conn.execute(f'UPDATE "{table_name}" SET date_historisation = CURRENT_DATE')

    def truncate_table(self, conn, table_name: str):
        """
        Vide une table dans la base duckDB.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Connexion à la base DuckDB.
        table_name : str
            Nom de la table à vider.
        """
        conn.execute(f"TRUNCATE TABLE {table_name}")

    def historise_table(self, conn, query_params: dict):
        """
        Historise les données d'une table. Copie le contenue de la table dans la ztable, puis vide la table.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Connexion à la base de données.
        query_params : dict
            Paramètres à injecter dans la requête SQL.
        """
        table_name = query_params["table"]
        target_name = f"z{table_name}"
        query_params_histo = query_params.copy()
        query_params_histo['table'] = target_name

        try:
            if not self.is_table_exist(conn, query_params_histo) and self.is_table_exist(conn, query_params):
                self.add_date_if_not_exist(conn, table_name) # Ajout de la date du jour dans la table actuel -> envoyer dans l'historique
                self.copy_table_into_new(conn, table_name, target_name)
            elif self.is_table_exist(conn, query_params_histo) and self.is_table_exist(conn, query_params):
                self.add_date_if_not_exist(conn, table_name) # Ajout de la date du jour dans la table actuel -> envoyer dans l'historique
                self.append_table(conn, table_name, target_name)
            self.truncate_table(conn, table_name)
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de l'historisation : {e}")
            raise

    def close(self):
        """ Ferme la connexion à la base de données Duckdb. """
        self.conn.close()
        self.logger.info("Connexion à DuckDB fermée.")
