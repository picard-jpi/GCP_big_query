# -------------------------------------------------------------
# Importation des bibliothèques GCP
# -------------------------------------------------------------
# bigquery : pour interagir avec BigQuery (création de tables, chargement de données, ..)
from google.cloud import bigquery

# -------------------------------------------------------------
# 1. Initialiser le client BigQuery
# -------------------------------------------------------------
# Le client utilise les identifiants configurés dans ton environnement :
# - soit la variable d’environnement GOOGLE_APPLICATION_CREDENTIALS
# - soit les identifiants actifs de ton compte gcloud
bq_client = bigquery.Client()

# -------------------------------------------------------------
# 2. Définir les paramètres du chargement
# -------------------------------------------------------------
# Remplace ces valeurs par les tiennes :
dataset_id = "mon_dataset"               # nom du dataset BigQuery où créer la table
table_id = "ma_table"                    # nom de la table à créer ou à mettre à jour
gcs_uri = "gs://mon-bucket/fichiers/data.csv"  # chemin complet du fichier CSV dans ton bucket GCS

# -------------------------------------------------------------
# 3. Configurer le job de chargement
# -------------------------------------------------------------
# On indique ici le format du fichier et quelques options utiles :
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,  # indique que le fichier source est un CSV
    skip_leading_rows=1,                      # ignore la première ligne (en-têtes de colonnes)
    autodetect=True,                          # laisse BigQuery détecter automatiquement le schéma des colonnes
)

# -------------------------------------------------------------
# 4. Construire la référence complète de la table BigQuery
# -------------------------------------------------------------
# La référence doit avoir la forme : projet.dataset.table
# Ex: "mon-projet.marketing.clients_2025"
table_ref = f"{bq_client.project}.{dataset_id}.{table_id}"

# -------------------------------------------------------------
# 5. Lancer le job de chargement
# -------------------------------------------------------------
# On demande à BigQuery de lire le fichier CSV directement depuis GCS et de créer la table correspondante.
load_job = bq_client.load_table_from_uri(
    gcs_uri,        # chemin du fichier dans GCS (gs://...)
    table_ref,      # nom complet de la table BigQuery
    job_config=job_config  # configuration du chargement
)

print(f" Chargement du fichier {gcs_uri} vers {table_ref} ...")

# .result() bloque l’exécution jusqu’à la fin du job (synchronisation)
load_job.result()

# -------------------------------------------------------------
# 6. Vérifier le résultat et afficher un résumé
# -------------------------------------------------------------
# Une fois le job terminé, on récupère la table pour afficher quelques infos
destination_table = bq_client.get_table(table_ref)
print(f"Table '{destination_table.table_id}' créée avec {destination_table.num_rows} lignes.")
