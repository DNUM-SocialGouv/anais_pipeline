# === Packages ===
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv
from pathlib import Path
import urllib.parse
from logging import Logger

# === Modules ===
from pipeline.csv_management import ColumnsManagement
from pipeline.database_pipeline import DataBasePipeline
from pipeline.load_yml import resolve_env_var

# === Chargement des variables d’environnement ===
load_dotenv()


# === Classes ===
# Classe PostgreSQLLoader qui gère les actions relatives à une database postgres
class PostgreSQLLoader(DataBasePipeline):
    def __init__(self, db_config: dict, config: dict, logger: Logger):
        """
        Initialisation de la base Postgres. Classe héritière de DataBasePipeline.

        Parameters
        ----------
        db_config : dict
            Paramètres de connexion vers la base.
        config : dict
            Metadata du profile (dans metadata.yml).
        logger : logging.Logger
            Fichier de log.
        """
        super().__init__(db_config, config, logger)
        self.logger = logger
        self.typedb = "postgres"
        self.host = db_config["host"]
        self.port = db_config["port"]
        self.user = db_config["user"]
        self.password = urllib.parse.quote(resolve_env_var(db_config["password"]))
        self.database = db_config["dbname"]
        self.schema = db_config["schema"]
        self.engine = self.init_engine()

    def init_engine(self):
        """ Initialisation de la connexion postgres. """
        try:
            url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            engine = create_engine(url)
            self.logger.info("Connexion PostgreSQL établie avec succès.")
            return engine
        except Exception as e:
            self.logger.error(f"Erreur de connexion PostgreSQL : {e}")
            raise

    def connect(self):
        """ Connexion à la base postgres. """
        self.conn = self.engine.connect()

    def postgres_drop_table(self, conn, query_params: dict):
        """
        Supprime une table et les vues qui lui sont liées.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Connection
            Connexion à la base postgres.
        query_params : dict
            Paramètres à injecter dans la requête SQL.
        """
        table_name = query_params['table']

        views = conn.execute(text("""
            SELECT DISTINCT dependent_ns.nspname, dependent_view.relname
            FROM pg_depend
            JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
            JOIN pg_class AS dependent_view ON pg_rewrite.ev_class = dependent_view.oid
            JOIN pg_class AS base_table ON pg_depend.refobjid = base_table.oid
            JOIN pg_namespace AS dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
            WHERE base_table.relname = :table
        """), {"table": table_name}).fetchall()

        for schema, view in views:
            # Suppression des vues liées à la table
            self.logger.info(f"🗑 Vue '{view}' existante → suppression totale (DROP VIEW)")
            conn.execute(text(f'DROP VIEW IF EXISTS "{schema}"."{view}" CASCADE'))

        # Suppression de la table
        self.logger.info(f"🗑 Table '{table_name}' existante → suppression totale (DROP TABLE)")
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))

    def create_table(self, conn, sql_query: str, query_params: str):
        """
        Exécution du fichier SQL Create Table.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Connection
            Connexion à la base postgres.
        sql_query : str
            Contenu du fichier SQL Create table.
        query_params : dict
            Paramètres à injecter dans la requête SQL.
        """
        trans = conn.begin()
        try:
            if self.is_table_exist(conn, query_params):
                self.postgres_drop_table(conn, query_params)
            conn.execute(text(sql_query))
            trans.commit()
        except Exception as e:
            trans.rollback()
            self.logger.error(f"❌ Erreur lors de l'exécution : {e}")
            raise

    def get_postgres_schema(self, conn, table_name: str) -> pd.DataFrame:
        """
        Récupération du schéma postgres pour une table spécifique.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Connection
            Connexion à la base postgres.
        table_name : str
            Nom de la table.

        Returns
        -------
        pd.DataFrame
            Schéma de la table contenant le nom des colonnes, leur type et leur format.
        """
        inspector = inspect(conn)
        columns = inspector.get_columns(table_name, schema=self.schema)
        schema_df = pd.DataFrame(columns)
        schema_df = schema_df.rename(columns={"name": "column_name", "type": "column_type"})
        return schema_df

    def is_table_exist(self, conn, query_params: dict, print_log: bool = False) -> bool:
        """
        Indique si la table existe ou non.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Connection
            Connexion à la base postgres.
        query_params : dict
            Paramètres à injecter dans la requête SQL.
        print_log : bool
            True si on souhaite afficher la log, False sinon, by default False.

        Returns
        -------
        bool
            True si la table existe (et non vide si applicable), False sinon.
        """
        table_exists = conn.execute(text(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = :schema AND table_name = :table
            )
            """), query_params).scalar()

        if table_exists:
            if print_log:
                self.logger.info(f"✅ La table '{query_params['table']}' existe.")
            return True
        else:
            if print_log:
                self.logger.warning(f"❌ La table '{query_params['table']}' du schéma {query_params['schema']} n'existe pas.")
            return False

    def show_row_count(self, conn, query_params: dict):
        """
        Affiche le nombre de lignes d'une table.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Connection
            Connexion à la base de données.
        query_params : dict
            Nom de la table.
        """
        schema = query_params["schema"]
        table = query_params["table"]

        row_count = conn.execute(text(
            f"""SELECT COUNT(*)
            FROM {schema}.{table}""")).scalar()

        if row_count == 0:
            self.logger.warning(f"⚠️ La table '{table}' du schéma {schema} est vide.")
        else:
            self.logger.info(f"✅ La table '{table}' du schéma {schema} contient {row_count} lignes.")

    def print_table(self, conn, query_params: dict, limit: int):
        """
        Affiche les premières lignes d'une table.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Connection
            Connexion à la base de données postgres.
        query_params : dict
            Nom de la table.
        limit : int, optional
            Nombre de lignes à afficher si print_table est True, 10 by default.
        """
        schema = query_params["schema"]
        table = query_params["table"]

        df = conn.execute(text(f"SELECT * FROM {schema}.{table} LIMIT {limit}"))
        self.logger.info(f"🔍 Aperçu de '{table}' du schéma {schema} ({limit} lignes) :\n{df.to_string(index=False)}")

    def load_csv_file(self, conn, csv_file: Path):
        """
        Charge un fichier CSV et l'injecte dans la base postgres.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Connection
            Connexion à la base de données.
        csv_file : Path
            Fichier csv.
        """
        self.logger.info(f"📥 Chargement du fichier : {csv_file}")
        table_name = csv_file.stem
        query_params = {"schema": self.schema, "table": table_name}

        try:
            if not self.is_table_exist(conn, query_params):
                self.logger.warning(f"Table {table_name} non trouvée, impossible de charger {csv_file.name}")
                return

            schema_df = self.get_postgres_schema(conn, table_name)

            # Chargement du csv et datamanagement
            pipeline = ColumnsManagement(csv_file=csv_file, schema_df=schema_df, logger=self.logger)
            df = pipeline.df
            self.logger.info(f"Taille de '{table_name}' : {df.shape}")

            # Création de la table avec la structure du CSV
            self.logger.info(f"🆕 Injection dans la table '{table_name}' à partir du CSV {csv_file}")

            trans = conn.get_transaction()
            try:
                df.to_sql(
                    table_name,
                    conn,
                    schema=query_params["schema"],
                    if_exists="append",
                    index=False,
                    method='multi',
                    chunksize=1000
                )
                trans.commit()
            except Exception as e:
                trans.rollback()
                self.logger.error(f"❌ Erreur lors de l'exécution : {e}")
                raise

            self.logger.info(f"✅ Table '{table_name}' créée et remplie avec succès ({csv_file})")

        except Exception as e:
            self.logger.error(f"❌ Erreur pour le fichier {csv_file} → {e}")

    def fetch_df(self, conn, table_name: str) -> pd.DataFrame:
        """
        Fonction de chargement d'une table depuis une base postgres.
        Importante pour l'export des csv.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Connection
            Connexion à la base de données.
        table_name : str
            Nom de la table que l'on charge.

        Returns
        -------
        pd.DataFrame
            Dataframe de la table chargée.
        """
        conn.execute(text(f"SET search_path TO {self.schema}"))
        return pd.read_sql_table(table_name, conn)

    def close(self):
        """Ferme la connexion à la base de données postgres."""
        self.conn.close()
        self.logger.info("Connexion à postgres fermée.")