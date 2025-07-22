# === Packages ===
import pandas as pd
import csv
import os
from io import StringIO
import re
import unicodedata
from collections.abc import Callable
from pathlib import Path
from logging import Logger


# === Classes ===
class TransformExcel:
    def __init__(self, local_xlsx_path: str, local_csv_path: str, logger: Logger):
        """
        Classe de conversion d'un fichier Excel basé sur un TCD vers un fichier csv.

        Parameters
        ----------
        local_xlsx_path : str
            Nom du fichier local au format xlsx.
        local_csv_path : str
            Nom du fichier local au format csv.
        logger : logging.Logger
            Fichier de log.
        """
        self.logger = logger
        self.local_xlsx_path = local_xlsx_path
        self.local_csv_path = local_csv_path
        self.df: pd.DataFrame = pd.DataFrame()
        self.convert_excel_to_csv()

    def fill(self) -> pd.DataFrame:
        """
        Remplit les valeurs manquantes dans la première colonne avec la méthode ffill.
        """
        # Remplir les valeurs manquantes dans la colonne "Colonne1"
        first_col = self.df.columns[0]
        self.df[first_col] = self.df[first_col].ffill()

    def TCD_management(self):
        """
        Nettoie le tableau croisé dynamique (TCD) :
        - supprime les lignes contenant "Total général"
        - remplace "(vide)" par None
        """
        for col_name in self.df.columns:
            # Si la colonne contient "Total général" -> suppression de la ligne
            self.df = self.df[self.df[col_name] != "Total général"]

            # Remplacement de "(vide)" par None
            mask = self.df[col_name] == "(vide)"
            self.df.loc[mask] = None

    def convert_excel_to_csv(self):
        """
        Convertit un fichier Excel en CSV (séparateur ';') et le nettoie.
        """
        try:
            # Conversion du fichier excel vers du csv
            self.df = pd.read_excel(self.local_xlsx_path, engine='openpyxl')
            self.fill()
            self.TCD_management()
            self.df.index.names = ['Column1']
            self.df.to_csv(self.local_csv_path, sep=";", quoting=csv.QUOTE_NONNUMERIC)

            # Suppression du fichier Excel
            os.remove(self.local_xlsx_path)
            self.logger.info(f"Fichier remplacé par un csv : {self.local_xlsx_path} -> {self.local_csv_path}")
        except Exception as e:
            self.logger.warning(f"⚠️ Erreur lors de la suppression de {self.local_xlsx_path} : {e}")


class ReadCsvWithDelimiter:
    def __init__(self, file_path: str, logger: Logger, sample_size: int = 4096):
        """
        Classe de lecture des csv grâce à la détection du délimiteur.

        Parameters
        ----------
        file_path : str
            Chemin + Fichier csv à lire.
        logger : logging.Logger
            Fichier de log.
        sample_size : int, optional
            Taille de l'échantillon de lecture pour déterminer le délimiteur, by default 4096
        """
        self.logger = logger
        self.file_path = file_path
        self.sample_size = sample_size
        self.dialect = self.detect_delimiter()

    def detect_delimiter(self):
        """ Détecte automatiquement le délimiteur du fichier csv. """
        try:
            with open(self.file_path, 'r', encoding='utf-8-sig') as f:
                sample = f.read(self.sample_size)
                sniffer = csv.Sniffer()
                dialect = sniffer.sniff(sample, delimiters=";,¤")
                return dialect.delimiter
        except Exception as e:
            self.logger.warning(f"⚠️ Impossible de détecter le délimiteur pour {self.file_path} : {e} → ';' utilisé par défaut.")
            return ";"

    def read_csv_resilient(self) -> pd.DataFrame:
        """
        Test la lecture du csv avec différents délimiteurs.
        """
        delimiters_to_try = [self.dialect, ";", ",", "¤"]
        tried = set()

        for delimiter in delimiters_to_try:
            if delimiter in tried:
                continue
            tried.add(delimiter)

            try:
                df = pd.read_csv(
                    self.file_path,
                    delimiter=delimiter,
                    dtype=str,
                    quotechar='"',
                    encoding='utf-8-sig'
                )
                self.logger.info(f"✅ Lecture réussie avec le délimiteur '{delimiter}' pour {os.path.basename(self.file_path)}")
                return df
            except pd.errors.ParserError as e:
                self.logger.warning(f"⚠️ Erreur de parsing avec '{delimiter}' pour {self.file_path} → {e}")
            except Exception as e:
                self.logger.warning(f"⚠️ Autre erreur avec '{delimiter}' → {e}")

        raise ValueError(f"❌ Impossible de lire le fichier CSV {self.file_path} avec les délimiteurs connus.")

    def read_csv_with_custom_delimiter(self, delimiter: str) -> pd.DataFrame :
        """
        Test la lecture du csv un délimiteur défini.

        Parameters
        ----------
        delimiter : str
            Délimiteur défini.

        Returns
        -------
        pd.DataFrame
            Dataframe du fichier csv.
        """
        try:
            with open(self.file_path, "rb") as f:
                raw = f.read()

            decoded = raw.decode("utf-8", errors="replace")

            df = pd.read_csv(
                StringIO(decoded),
                delimiter=delimiter,
                dtype=str,
                engine="python",
                quoting=csv.QUOTE_NONE,
                on_bad_lines="warn"
            )

            self.logger.info(f"✅ Lecture réussie avec délimiteur '¤' après détection binaire : {os.path.basename(self.file_path)}")
            return df
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la lecture de {self.file_path} avec délimiteur '¤' → {e}")
            raise

    def read_csv_files(self) -> pd.DataFrame:
        """
        Lit le csv avec le bon délimiteur.

        Returns
        -------
        pd.DataFrame
            Dataframe du csv.
        """
        try:
            if self.file_path.name == "sa_sivss.csv":
                return self.read_csv_with_custom_delimiter("¤")
            else:
                return self.read_csv_resilient()
            # return self.read_csv_resilient()
        except Exception as e:
            self.logger.error(f"❌ Lecture échouée pour {self.file_path.name} → {e}")
            return


class StandardizeColnames:
    def __init__(self, df: pd.DataFrame, logger: Logger):
        """
        Classe de standardisation du nom des colonnes.

        Parameters
        ----------
        df : pd.DataFrame
            Dataframe sur lequel appliqué les transformations.
        logger : logging.Logger
            Fichier de log.
        """
        self.logger = logger
        self.df = df

    def remove_accents(self, text: str) -> str:
        """
        Supprime les accents d'un texte.
        """
        return ''.join(
            c for c in unicodedata.normalize('NFD', text)
            if unicodedata.category(c) != 'Mn'
        )

    def shorten_column_names(self, text: str, max_length: int = 63):
        """
        Raccourcit le nom des colonnes (< 64 caractères).
        """
        if len(text) > max_length:
            shortened = text[:max_length]
            return shortened
        return text

    def standardize_column_names(self):
        """
        Standardise le nom des colonnes:
            - Retire les espaces en trop
            - Retire les accents
            - Converti les caractères spéciaux en '_'
            - Applique la miniscule
            - Réduit le nombre de caractère total à 63 (max)
        """
        new_columns = []
        for col in self.df.columns:
            original = col
            col = col.strip()
            col = self.remove_accents(col)
            col = col.lower()
            col = re.sub(r"[^\w]", "_", col)
            col = re.sub(r"__+", "_", col)
            col = col.strip("_")
            col = self.shorten_column_names(col)

            if col != original:
                # self.logger.info(f"📝 Colonne renommée : '{original}' → '{col}'")
                pass

            new_columns.append(col)

        self.df.columns = new_columns


class ColumnsManagement(StandardizeColnames):
    def __init__(self, csv_file: Path, schema_df: pd.DataFrame, logger: Logger):
        """
        Initialisation de la base DuckDB. Classe héritière de DataBasePipeline.

        Parameters
        ----------
        csv_file : Path
            Fichier csv à importer.
        schema_df : pd.DataFrame
            Schéma de la table SQL sous forme de dataframe. Contient la description des colonnes.
        logger : logging.Logger
            Fichier de log.
        """
        self.csv_file = csv_file
        self.logger = logger
        self.df = ReadCsvWithDelimiter(csv_file, logger).read_csv_files()
        super().__init__(self.df, logger)
        self.schema_df = schema_df

        self.type_mapping = {
            "INTEGER": "int",
            "BIGINT": "int",
            "FLOAT": "float",
            "DOUBLE": "float",
            "REAL": "float",
            "TEXT": "string",
            "VARCHAR": "string",
            "BOOLEAN": "bool",
            "DATE": "datetime64",
            "TIMESTAMP": "datetime64",
        }

        # Exécute la pipeline
        self.df = self.csv_pipeline()

    def check_missing_columns(self):
        """
        Vérifie la cohérence entre les colonnes du fichier csv et les colonnes de la table SQL.
        """
        csv_file_name = self.csv_file.name
        table_columns = self.schema_df["column_name"].tolist()
        csv_columns = self.df.columns.tolist()

        missing_columns = set(table_columns) - set(csv_columns)
        extra_columns = set(csv_columns) - set(table_columns)

        if missing_columns:
            self.logger.warning(f"Colonnes manquantes dans {csv_file_name} : {missing_columns}")
            for col in missing_columns:
                self.df[col] = None

        if extra_columns:
            self.logger.warning(f"Colonnes en trop dans {csv_file_name} : {extra_columns}")
            self.df = self.df[table_columns]

    def get_column_length(self):
        """
        Sépare le type de la colonne et la longueur dans le schéma de la table.
        Par exemple, VARCHAR(50) sera séparé en VARCHAR dans column_base_type et 50 dans column_length.
        Si la longueur n'existe pas, alors column_length est vide (NA).
        """
        self.schema_df["column_base_type"] = self.schema_df["column_type"].astype(str).str.extract(r"^(\w+)")
        self.schema_df["column_length"] = (
            self.schema_df["column_type"].astype(str)
            .str.extract(r"\((\d+)\)")
            .astype("Int64")
        )

    def convert_columns_type(self):
        """
        Convertie les colonnes du dataframe selon le type dans les tables SQL.
        """
        for _, row in self.schema_df.iterrows():
            col_name = row["column_name"]
            col_type = str(row["column_base_type"])
            col_length = row["column_length"]

            if col_name in self.df.columns and col_type in self.type_mapping:
                try:
                    if self.type_mapping[col_type] in ["int", "float"]:
                        self.df[col_name] = self.df[col_name].replace({None: 0, "": 0, pd.NA: 0, "nan": 0}).astype(float).astype(self.type_mapping[col_type])
                    elif self.type_mapping[col_type] == "bool":
                        self.df[col_name] = self.df[col_name].replace({None: False, "": False, pd.NA: False}).astype(bool)
                    elif self.type_mapping[col_type] == "datetime64":
                        self.df[col_name] = pd.to_datetime(self.df[col_name], format="%d-%m-%Y", errors="coerce")
                    elif self.type_mapping[col_type] == "string":
                        self.df[col_name] = self.df[col_name].astype(self.type_mapping[col_type])
                        if not pd.isna(col_length):
                            self.df[col_name] = self.df[col_name].str[:col_length]
                    else:
                        self.df[col_name] = self.df[col_name].astype(self.type_mapping[col_type])
                except ValueError as e:
                    self.logger.warning(f"Erreur de conversion de {col_name} en {col_type}: {e}, valeurs laissées en str.")

    def csv_pipeline(self) -> pd.DataFrame:
        """
        Applique la transformation d'un csv et la compare au schéma de table SQL attendu:
            - Standardisation du nom des colonnes (accents, taille, caractères spéciaux...)
            - Vérification de la présence ou absence des colonnes attendues
            - Conversion des colonnes selont leur type SQL (VARCHAR, INT, BOOLEAN ...)
        """
        self.standardize_column_names()
        self.check_missing_columns()
        self.get_column_length()
        self.convert_columns_type()

        return self.df


class TableInCsv:
    def __init__(self, conn, table_name: str, csv_name: str, df_fetch_func: Callable[[str], pd.DataFrame], folder: str, logger: Logger):
        """
        Importe ou exporte une table au format csv depuis/vers une base de données.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection | sqlalchemy.engine.base.Connection
            Connexion à la base de données.
        table_name : str
            Nom de la table SQL
        csv_name : str
            Nom du fichier csv en issue.
        df_fetch_func : Callable[[str], pd.DataFrame]
            Fonction de requêtage de la table. Une fonction existe pour duckdb et une autre pour postegres.
        folder : str
            Répertoire du fichier.
        logger : logging.Logger
            Fichier de log.
        """
        self.conn = conn
        self.table_name = table_name
        self.csv_name = csv_name
        self.df_fetch_func = df_fetch_func
        self.folder = folder
        self.logger = logger

    def import_to_csv(self):
        """
        Importe une table SQL vers un format csv.
        Le dataframe peut être issue d'un requêtage duckdb ou postgres.
        """
        input_folder = self.folder
        os.makedirs(input_folder, exist_ok=True)

        # Nom du fichier
        file_name = f'{self.csv_name}.csv'
        input_path = os.path.join(input_folder, file_name)
        self.logger.info(f"📤 Import de '{self.table_name}' → {input_path}")

        try:
            # Importation
            df = self.df_fetch_func(self.conn, self.table_name)
            df.to_csv(input_path, index=False, sep=";", encoding="utf-8-sig")
            self.logger.info(f"✅ Import réussi : {file_name}")
        except Exception as e:
            self.logger.error(f"❌ Erreur d'import pour '{self.table_name}' → {e}")

    def export_to_csv(self, date: str):
        """
        Exporte une table SQL vers un format csv.
        Le dataframe peut être issue d'un requêtage duckdb ou postgres.

        date : str
            Date présente dans le nom des fichiers à exporter.
        """
        output_folder = self.folder
        os.makedirs(output_folder, exist_ok=True)

        # Nom du fichier
        file_name = f'{self.csv_name}_{date}.csv'
        output_path = os.path.join(output_folder, file_name)
        self.logger.info(f"📤 Export de '{self.table_name}' → {output_path}")

        try:
            # Exportation
            df = self.df_fetch_func(self.conn, self.table_name)
            df.to_csv(output_path, index=False, sep=";", encoding="utf-8-sig")
            self.logger.info(f"✅ Export réussi : {file_name}")
        except Exception as e:
            self.logger.error(f"❌ Erreur d'export pour '{self.table_name}' → {e}")
