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
