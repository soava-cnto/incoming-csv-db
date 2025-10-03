# config.py
from dotenv import load_dotenv
import os

load_dotenv()  # Charge le fichier .env

DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME")
}

TABLE_NAME = os.getenv("TABLE_NAME", "call_logs")

VIEW_NAME = os.environ.get("VIEW_NAME", "v_incoming_reiteration")

# csv_reader.py
import pandas as pd
from charset_normalizer.api import from_path

class CSVReader:
    def __init__(self, filepath, chunksize=50000, include_comment=False, encoding=None):
        self.filepath = filepath
        self.chunksize = chunksize
        self.include_comment = include_comment
        self.encoding = encoding or self._detect_encoding()
        self.used_encoding = None  # encodage réellement utilisé

    def _detect_encoding(self):
        """
        Détecte automatiquement l’encodage du fichier en lisant un échantillon.
        """
        result = from_path(self.filepath).best()
        # result = charset_normalizer.from_path(self.filepath).best()
        if result:
            print(f"[INFO] Encodage détecté automatiquement : {result.encoding} (confiance {result.chaos})")
            return result.encoding
        else:
            print("[WARN] Impossible de détecter l’encodage, fallback en utf-8")
            return "utf-8"

    def _try_read(self, **kwargs):
        """
        Lecture avec l’encodage détecté. Si ça casse, fallback latin1/cp1252.
        """
        encodings_to_try = [self.encoding, "utf-8", "latin1", "cp1252"]
        last_error = None

        for enc in encodings_to_try:
            try:
                df = pd.read_csv(self.filepath, encoding=enc, **kwargs)
                if self.used_encoding is None:
                    self.used_encoding = enc
                    print(f"[INFO] Fichier lu avec encodage : {enc}")
                return df
            except UnicodeDecodeError as e:
                print(f"[WARN] Échec lecture avec encodage {enc}")
                last_error = e
                continue

        raise last_error

    def get_chunks(self):
        # Détecter colonnes si on veut exclure COMMENTAIRE
        usecols = None
        if not self.include_comment:
            header = self._try_read(nrows=0, engine="python")
            usecols = [c for c in header.columns if c.strip().upper() != "COMMENTAIRE"]

        return self._try_read(
            sep=",",
            quotechar='"',
            doublequote=True,
            escapechar="\\",
            engine="python",
            dtype=str,
            keep_default_na=False,
            na_values=["", "NA", "NULL"],
            on_bad_lines="warn",
            usecols=usecols,
            chunksize=self.chunksize
        )

# data_cleaner.py
import pandas as pd
import re

class DataCleaner:
    @staticmethod
    def sanitize_columns(df):
        df.columns = (
            df.columns.str.strip()
                      .str.lower()
                      .str.replace(" ", "_", regex=False)
                      .str.replace("-", "_", regex=False)                      
                      .str.replace("[^0-9a-z_]", "", regex=True)
        )
        return df

    @staticmethod
    def normalize_phone(phone):
        if pd.isna(phone):
            return None
        digits = re.sub(r"\D+", "", str(phone))
        return digits or None

    @staticmethod
    def clean(df):
        df = df.copy()
        df = DataCleaner.sanitize_columns(df)

        # Trim des strings
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].str.strip().replace("", pd.NA)
            
        # Ajout semaine ISO
        if "date_appel" in df.columns:
            df["date_appel"] = pd.to_datetime(df["date_appel"], errors="coerce")
            df["semaine"] = df["date_appel"].dt.isocalendar().week
            # df["iso_year"] = df["date_appel"].dt.isocalendar().year

        # DateTime
        if "date_appel" in df.columns and "heure_appel" in df.columns:
            df["datetime_appel"] = pd.to_datetime(
                df["date_appel"].dt.strftime("%Y-%m-%d") + " " + df["heure_appel"].fillna(""),
                errors="coerce"
            )


        # Colonnes numériques
        numeric_cols = [
            "duree_prise_en_charge", "duree_post_travail_agent", 
            "duree_appel", "indice", "raccrochage", "numero_court"
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        # Normaliser téléphone
        if "numero_telephone" in df.columns:
            df["numero_telephone_clean"] = df["numero_telephone"].apply(DataCleaner.normalize_phone)

        return df

# db_writer.py
from sqlalchemy import create_engine, text
from io import StringIO
import pandas as pd

class DBWriter:
    def __init__(self, db_config: dict, table_name: str, view_name: str ):
        self.db_config = db_config
        self.table_name = table_name
        self.engine = create_engine(
            f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )
        self._ensure_log_table()
        self.view_name = view_name

    def _ensure_log_table(self):
        """Crée la table de log si elle n’existe pas"""
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS imported_files (
                    id SERIAL PRIMARY KEY,
                    file_name TEXT UNIQUE,
                    imported_at TIMESTAMP DEFAULT now()
                )
            """))
            conn.commit()

    def already_imported(self, file_name: str) -> bool:
        """Vérifie si le fichier a déjà été importé"""
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM imported_files WHERE file_name = :f"),
                {"f": file_name}
            ).fetchone()
            return result is not None

    def log_import(self, file_name: str):
        """Consigne qu’un fichier a été importé"""
        with self.engine.connect() as conn:
            conn.execute(
                text("INSERT INTO imported_files (file_name) VALUES (:f) ON CONFLICT DO NOTHING"),
                {"f": file_name}
            )
            conn.commit()

    def copy_dataframe(self, df: pd.DataFrame):
        """Insère un DataFrame en bulk via COPY"""
        conn = self.engine.raw_connection()
        cur = conn.cursor()

        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cols = ",".join(df.columns)
        sql = f"COPY {self.table_name} ({cols}) FROM STDIN WITH CSV"
        cur.copy_expert(sql, buffer)

        conn.commit()
        cur.close()
        conn.close()

    def close(self):
        self.engine.dispose()
        
    def get_engine(self):
        return self.engine

    def get_view_name(self):
        return self.view_name


import argparse
import logging
import os

from config import DB_CONFIG, TABLE_NAME
from csv_reader import CSVReader
from data_cleaner import DataCleaner
from db_writer import DBWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def process_csv(path, include_comment=False):
    file_name = os.path.basename(path)  # juste le nom du fichier
    writer = DBWriter(DB_CONFIG, TABLE_NAME)

    # Vérif si déjà importé
    if writer.already_imported(file_name):
        logging.warning(f"⚠️ Le fichier {file_name} a déjà été importé, skip.")
        writer.close()
        return

    reader = CSVReader(path, chunksize=50000, include_comment=include_comment)

    for i, chunk in enumerate(reader.get_chunks()):
        logging.info(f"Chunk {i} : {len(chunk)} lignes lues")
        clean_df = DataCleaner.clean(chunk)
        writer.copy_dataframe(clean_df)
        logging.info(f"Chunk {i} inséré dans PostgreSQL")

    # On log l’import réussi
    writer.log_import(file_name)
    writer.close()
    logging.info(f"✅ Import terminé avec succès pour {file_name} !")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestion CSV -> PostgreSQL")
    parser.add_argument("csv_path", help="Chemin du fichier CSV")
    parser.add_argument("--include_comment", action="store_true", help="Inclure la colonne COMMENTAIRE")
    args = parser.parse_args()

    process_csv(args.csv_path, include_comment=args.include_comment)

#scheduler.py
import os
import time
import datetime
import schedule
import logging

from main import process_csv
from config import DB_CONFIG, TABLE_NAME
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Répertoire où arrivent tes fichiers CSV
CSV_DIR = r"D:\Utilisateurs\soava.rakotomanana\Documents"

def get_yesterday_file():
    """Construit le chemin du fichier d'hier basé sur la convention de nommage."""
    yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    filename = f"{yesterday}_VocalCom_Incoming.csv"
    return os.path.join(CSV_DIR, filename), yesterday

def data_exists_for_date(date_str: str) -> bool:
    """Vérifie si des données pour une date donnée existent déjà dans la base."""
    engine = create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE DATE(date_appel) = :d"),
            {"d": date_str}
        ).scalar()
        return result > 0

def job():
    """Tâche planifiée qui lit et insère le fichier d’hier."""
    file_path, date_str = get_yesterday_file()

    if not os.path.exists(file_path):
        logging.warning(f"Fichier {file_path} introuvable. Rien à faire.")
        return

    # Vérifie si déjà inséré
    if data_exists_for_date(date_str):
        logging.info(f"Les données du {date_str} existent déjà. Insertion annulée pour éviter les doublons.")
        return

    # Sinon on lance l'import
    logging.info(f"Lancement de l'import du fichier {file_path}")
    process_csv(file_path)
    logging.info("✅ Import terminé sans doublons !")

# Planifier tous les jours à 08h00
schedule.every().day.at("12:02").do(job)

logging.info("Scheduler démarré... (CTRL+C pour arrêter)")
while True:
    schedule.run_pending()
    time.sleep(60)  # vérifie chaque minute


#scheduler_month.py
import os
import glob
import subprocess
import sys

# chemin vers ton dossier contenant les CSV de septembre
SEPTEMBER_DIR = r"D:\Utilisateurs\soava.rakotomanana\Documents\september"

def process_september_files():
    csv_files = glob.glob(os.path.join(SEPTEMBER_DIR, "*.csv"))

    if not csv_files:
        print("Aucun fichier trouvé dans le dossier September.")
        return

    for file_path in csv_files:
        print(f"Traitement du fichier : {file_path}")
        # Utilise le même Python que celui qui exécute scheduler.py
        result = subprocess.run(
            [sys.executable, "main.py", file_path],   # <-- changement ici
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.stderr:
            print("Erreur :", result.stderr)

if __name__ == "__main__":
    process_september_files()

#export_db_csv
import pandas as pd
from db_writer import DBWriter
from config import DB_CONFIG, TABLE_NAME, VIEW_NAME

# 🔹 Créer un DBWriter avec les bons paramètres
db_writer = DBWriter(DB_CONFIG, TABLE_NAME, VIEW_NAME)

# 🔹 Récupérer le moteur et la vue
engine = db_writer.get_engine()
view_name = db_writer.get_view_name()

# 🔹 Lire la vue
df = pd.read_sql(f"SELECT * FROM public.{view_name}", engine)

# 🔹 Export CSV
df.to_csv("incoming_reiteration.csv", index=False, encoding="utf-8")

print("✅ Export terminé : incoming_reiteration.csv")
