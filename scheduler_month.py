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
