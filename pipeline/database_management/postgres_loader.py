# === Packages ===
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv
from pathlib import Path
import urllib.parse
from logging import Logger

# === Modules ===
from pipeline.utils.csv_management import ColumnsManagement
from pipeline.database_management.database_pipeline import DataBasePipeline
from pipeline.utils.load_yml import resolve_env_var

# === Chargement des variables d’environnement ===
load_dotenv()


# === Classes ===
# Classe PostgreSQLLoader qui gère les actions relatives à une database postgres
class PostgreSQLLoader(DataBasePipeline):
    def __init__(self, db_config: dict, config: dict, logger: Logger, staging_db_config: dict = None):
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
        staging_db_config : dict
            Paramètres de connexion vers la base Staging, None by default.
        """
        super().__init__(db_config, config, logger, staging_db_config)
        self.logger = logger
        self.typedb = "postgres"
        self.schema = db_config["schema"]
        self.db_name = db_config["dbname"]
        self.engine = self.init_engine(
            db_config["user"],
            urllib.parse.quote(resolve_env_var(db_config["password"])),
            db_config["host"],
            db_config["port"],
            self.db_name
            )

    def init_engine(self, user: str, password: str, host: str, port: str, database: str):
        """ Initialisation de la connexion postgres. """
        try:
            url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
            engine = create_engine(url)
            return engine
        except Exception as e:
            self.logger.error(f"Erreur de connexion PostgreSQL : {e}")
            raise

    def connect(self):
        """ Connexion à la base postgres. """
        self.conn = self.engine.connect()
        self.logger.info("Connexion PostgreSQL établie avec succès.")

    def drop_table(self, conn, query_params: dict):
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
        try:
            conn.execute(text(sql_query))
        except Exception as e:
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
        conn.commit()
        return pd.read_sql_table(table_name, conn)

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
        staging_db_config = self.staging_db_config
        if staging_db_config:
            # Connexion aux deux bases
            engine_source = self.init_engine(
                staging_db_config["user"],
                urllib.parse.quote(resolve_env_var(staging_db_config["password"])),
                staging_db_config["host"],
                staging_db_config["port"],
                staging_db_config["dbname"]
                )
            engine_target = self.engine

            # Copier de la base Staging
            df = pd.read_sql(f"SELECT * FROM {staging_table_name}", engine_source)

            # Coller dans la base cible (suppression de la table avant)
            query_params = {"schema": self.schema, "table": db_table_name}
            trans = conn.begin()

            try:
                if self.is_table_exist(conn, query_params):
                    self.drop_table(conn, query_params)
                trans.commit()
                
                df.to_sql(db_table_name, engine_target, if_exists='replace', index=False, schema=self.schema)
                self.logger.info(f"✅ La table {staging_table_name} a bien été récupérée de la base {staging_db_config["dbname"]} vers la base {self.db_name} sous le nom {db_table_name}.")
            except Exception as e:
                trans.rollback()
                self.logger.error(f"❌ Erreur lors de l'exécution : {e}")
                raise
        else:
            self.logger.error("❌ La configuration de la base Staging n'a pas été indiquée.")

    def copy_table_into_new(self, conn, source: str, target: str):
        """
        Copie une table dans une nouvelle.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Connection
            Connexion à la base de données.
        source : str
            Nom de la table que l'on "copie".
        target : str
            Nom de la table à laquelle on ajoute les données de la première. 

        """
        query = text(f"CREATE TABLE {target} AS TABLE {source} WITH DATA")
        conn.execute(query)
        conn.commit()
        self.logger.info(f"✅ Table historique {target} créée à partir de {source}")

    def append_table(self, conn, source: str, target: str):
        """
        Ajoute les données de la table source à la table target.
        Les deux tables doivent avoir la même structure (mêmes colonnes et types).

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Connection
            Connexion à la base de données.
        source : str
            Nom de la table que l'on "copie".
        target : str
            Nom de la table à laquelle on ajoute les données de la première. 

        """
        # Récupérer les colonnes de la table source
        source_cols = [row[0] for row in conn.execute(text(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = :source
            ORDER BY ordinal_position
        """), {"source": source}).fetchall()]

        # Récupérer les colonnes de la table target
        target_cols = [row[0] for row in conn.execute(text(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = :target
            ORDER BY ordinal_position
        """), {"target": target}).fetchall()]

        # Colonnes en commun
        common_cols = [col for col in source_cols if col in target_cols]

        cols_str = ", ".join([f'"{col}"' for col in common_cols])  # protéger les noms de colonnes

        query = text(f"""
            INSERT INTO {target} ({cols_str})
            SELECT {cols_str} FROM {source}
        """)
        conn.execute(query)
        conn.commit()
        self.logger.info(f"✅ Données de {source} ajoutées à {target}")

    def add_current_date(self, conn, table_name: str, column_name: str):
        """
        Ajoute la date du jour (date d'historisation) à une table.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Connection
            Connexion à la base DuckDB.
        table_name : str
            Nom de la table à vider.
        column_name : str
            Nom de la colonne date.
        """
        tz = "Europe/Paris"
        
        # Vérifier que la colonne existe
        check_query = text(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            AND column_name = '{column_name}'
        """)
        column_exists = conn.execute(check_query).fetchone()

        if not column_exists:
            conn.execute(text(f'ALTER TABLE "{table_name}" ADD COLUMN {column_name} TIMESTAMP'))
            self.logger.info(f"Colonne {column_name} créée dans la table {table_name}")
        
        conn.execute(text(f'''
            UPDATE "{table_name}"
            SET {column_name} = CURRENT_TIMESTAMP AT TIME ZONE '{tz}'
            WHERE {column_name} IS NULL
        '''))
        conn.commit()

    def drop_column(self, conn, table_name: str, column_name: str):
        """
        Supprime une colonne d'une table dans une base Postgres.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Connection
            Connexion à la base Postgres.
        table_name : str
            Nom de la table.
        column_name : str
            Nom de la colonne à supprimer.
        """
        query = text(f'ALTER TABLE "{table_name}" DROP COLUMN "{column_name}"')
        conn.execute(query)

    def truncate_table(self, conn, table_name: str):
        """
        Vide une table dans la base duckDB.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Connection
            Connexion à la base DuckDB.
        table_name : str
            Nom de la table à vider.
        """
        query = text(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE;')
        conn.execute(query)

    def reset_histo(self):
        """
        Supprime l'ensemble des tables historiques.

        Parameters
        ----------
        conn : sqlalchemy.engine.base.Connection
            Connexion à la base DuckDB.
        schema : str
            Nom du schema postgres de l'historique à supprimer.
        """
        conn = self.conn
        schema = self.schema

        # Récupération des tables
        query = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema
            AND table_name LIKE 'z%';
        """)
        tables = [row[0] for row in conn.execute(query, {"schema": schema}).fetchall()]

        if not tables:
            self.logger.info(f"Aucune table 'z%' trouvée dans le schéma {schema}")
            return

        # Suppression des tables
        try:
            for table in tables:
                query_params = {"schema": schema, "table": table}
                self.drop_table(conn, query_params)
                self.logger.info(f"✅ Table {schema}.{table} supprimée")
            conn.commit()
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la réinitialisation de l'historique : {e}")
            raise

    def close(self):
        """Ferme la connexion à la base de données postgres."""
        self.conn.close()
        self.logger.info("Connexion à postgres fermée.")