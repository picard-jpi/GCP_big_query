# -------------------------------------------------------------
#  1. Importation des bibliothèques
# -------------------------------------------------------------
from google.cloud import bigquery, storage
import re  # expressions régulières

# -------------------------------------------------------------
# 2. Initialisation des clients Google Cloud
# -------------------------------------------------------------
# Ces clients utilisent les identifiants d’accès (fichier JSON ou compte gcloud actif)
bq_client = bigquery.Client()
storage_client = storage.Client()

# -------------------------------------------------------------
#  3. Paramètres de configuration à personnaliser
# -------------------------------------------------------------
bucket_name = "mon-bucket"        #  Nom du bucket GCS
prefix = "data/"                  #  Préfixe (chemin de base) dans le bucket
dataset_id = "mon_dataset"        #  Dataset BigQuery où seront créées les tables

# Choisir le mode de chargement :
#   - "unique"     → tous les fichiers dans une seule table
#   - "par_fichier"→ une table par fichier
#   - "par_date"   → une table par date (extraite du nom du dossier "date=YYYY-MM-DD")
table_mode = "par_date"

# Si mode "unique", préciser le nom de la table globale
table_id = "table_globale"

# -------------------------------------------------------------
#  4. Lister les fichiers CSV dans le bucket GCS
# -------------------------------------------------------------
print(f" Recherche de fichiers CSV dans gs://{bucket_name}/{prefix} ...")

# Liste tous les objets (fichiers) du bucket sous le préfixe choisi
blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

# On ne garde que les fichiers CSV (.csv ou .csv.gz)
csv_files = [
    f"gs://{bucket_name}/{blob.name}"
    for blob in blobs
    if blob.name.endswith(".csv") or blob.name.endswith(".csv.gz")
]

# Vérifie qu’il y a des fichiers trouvés
if not csv_files:
    print(" Aucun fichier CSV trouvé dans ce dossier.")
    exit()

print(f" {len(csv_files)} fichiers trouvés :")
for f in csv_files:
    print(f"   • {f}")

# -------------------------------------------------------------
#  5. Configuration du job de chargement BigQuery
# -------------------------------------------------------------
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,  # format du fichier
    skip_leading_rows=1,                      # ignorer la première ligne (entêtes)
    autodetect=True,                          # détecter automatiquement le schéma
    compression=bigquery.Compression.GZIP,    # gérer les fichiers .csv.gz
)

# -------------------------------------------------------------
# 6. Chargement dans BigQuery selon le mode choisi
# -------------------------------------------------------------
if table_mode == "unique":
    # =========================================================
    # MODE 1 : Tous les fichiers vont dans une seule table
    # =========================================================
    table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"
    print(f"\n Chargement de {len(csv_files)} fichiers dans la table {table_ref} ...")

    # Lancement du job de chargement
    load_job = bq_client.load_table_from_uri(
        csv_files,
        table_ref,
        job_config=job_config
    )
    load_job.result()  # attendre la fin du job

    # Vérification
    destination_table = bq_client.get_table(table_ref)
    print(f" Table '{destination_table.table_id}' créée avec {destination_table.num_rows} lignes.\n")

elif table_mode == "par_fichier":
    # =========================================================
    # MODE 2 : Une table BigQuery par fichier CSV
    # =========================================================
    print(f"\n Création d'une table par fichier CSV...\n")

    for gcs_uri in csv_files:
        # Extraire un nom de table basé sur le nom du fichier
        match = re.search(r"([^/]+)\.csv(?:\.gz)?$", gcs_uri)
        if not match:
            continue  # on ignore si pas de correspondance

        filename = match.group(1)
        table_name = filename.replace("-", "_").replace(".", "_")  # nettoyage
        table_ref = f"{bq_client.project}.{dataset_id}.{table_name}"

        print(f" Chargement de {gcs_uri} → table {table_ref} ...")

        load_job = bq_client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config
        )
        load_job.result()

        destination_table = bq_client.get_table(table_ref)
        print(f" Table '{destination_table.table_id}' créée avec {destination_table.num_rows} lignes.\n")

elif table_mode == "par_date":
    # =========================================================
    # MODE 3: Une table par date (dossier de type date=YYYY-MM-DD)
    # =========================================================
    print(f"\n Création d'une table par date détectée dans le chemin...\n")

    # On va regrouper les fichiers par date extraite du nom du dossier
    files_by_date = {}

    for gcs_uri in csv_files:
        # Cherche un motif "date=YYYY-MM-DD" dans le chemin
        match = re.search(r"date=(\d{4}-\d{2}-\d{2})", gcs_uri)
        if match:
            date_str = match.group(1)
            files_by_date.setdefault(date_str, []).append(gcs_uri)

    # Pour chaque date trouvée, on charge tous les fichiers associés dans une table
    for date_str, uris in files_by_date.items():
        # Exemple : date=2025-11-03 → sessions_20251103
        table_name = f"sessions_{date_str.replace('-', '')}"
        table_ref = f"{bq_client.project}.{dataset_id}.{table_name}"

        print(f" Chargement de {len(uris)} fichier(s) pour la date {date_str} → table {table_ref} ...")

        # Création du job de chargement BigQuery
        load_job = bq_client.load_table_from_uri(
            uris,            # liste des fichiers de cette date
            table_ref,
            job_config=job_config
        )
        load_job.result()  # attendre la fin du chargement

        # Vérification de la table créée
        destination_table = bq_client.get_table(table_ref)
        print(f" Table '{destination_table.table_id}' créée avec {destination_table.num_rows} lignes.\n")

else:
    print(" Mode inconnu. Utilise 'unique', 'par_fichier' ou 'par_date'.")

# -------------------------------------------------------------
#  7. Fin du script
# -------------------------------------------------------------
print(" Chargement terminé avec succès !")
