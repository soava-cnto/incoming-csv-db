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
