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
