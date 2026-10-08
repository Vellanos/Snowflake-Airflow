# NYC Yellow Taxi — Snowflake & Airflow

## Objectif

Hudson Cab Partners exploite 180 taxis et souhaite identifier les zones et les heures de forte demande, ainsi que le revenu par trajet selon le mode de paiement.
Le projet analyse les données publiques des taxis jaunes de New York de janvier à mars 2025.
La [réponse à la direction](docs/REPONSE.md) présente la requête et ses dix premiers résultats.

## Architecture

![Architecture du pipeline](docs/architecture.png)

TLC → RAW → STAGING → INTERMEDIATE → MARTS.
Snowflake stocke les fichiers sur un stage interne et exécute les transformations SQL ; Airflow orchestre le téléchargement, le chargement mensuel et les contrôles.
RAW conserve les données source, STAGING harmonise les noms, INTERMEDIATE identifie les anomalies et dédoublonne les trajets valides, puis MARTS alimente les analyses.

## Stack

- Snowflake Enterprise
- Python 3.10+
- Apache Airflow 3, avec le runtime Astro du Dockerfile
- Astro CLI et Docker
- WSL Ubuntu sous Windows

## Structure du dépôt

```text
snowflake/           infrastructure, tables RAW et vérifications
ingestion/           chargement Python d'un mois dans RAW
airflow/dags/        DAG nyc_taxi_monthly
airflow/include/sql/ transformations et contrôles qualité
docs/               architecture, résultats et captures
```

## Pré-requis

Git, Python avec `venv`, OpenSSL, Docker en fonctionnement et Astro CLI.
Sous Windows, utiliser Ubuntu dans WSL avec l'intégration Docker Desktop activée ; placer le dépôt dans le dossier personnel Linux.
Le compte Snowflake doit permettre d'utiliser `USERADMIN` et `SYSADMIN` pour l'installation.

## Installation

1. Cloner le dépôt dans le terminal Linux :

   ```bash
   git clone https://github.com/Vellanos/Snowflake-Airflow.git nyc-yellow-taxi
   cd nyc-yellow-taxi
   ```

2. Vérifier le poste avec `bash verifier_poste.sh`.
3. Ouvrir un compte Snowflake Enterprise et relever son identifiant `ORGANISATION-COMPTE`.
4. Ouvrir `snowflake/01_infrastructure.sql` dans Snowsight. Il crée le warehouse, les quatre schémas, `TRANSFORMER` et `AIRFLOW_SVC`. Avant Run All, remplacer les deux valeurs `RSA_PUBLIC_KEY` par sa propre clé publique, générée ci-dessous ; actualiser ou retirer également l'empreinte du commentaire dans la copie utilisée.
5. Générer sa paire de clés hors du dépôt :

   ```bash
   umask 077
   mkdir -p ~/.ssh/snowflake
   openssl genrsa -out ~/.ssh/snowflake/rsa_key.pem 2048
   openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt \
     -in ~/.ssh/snowflake/rsa_key.pem -out ~/.ssh/snowflake/rsa_key.p8
   openssl rsa -in ~/.ssh/snowflake/rsa_key.p8 -pubout \
     -out ~/.ssh/snowflake/rsa_key.pub
   chmod 600 ~/.ssh/snowflake/rsa_key.pem ~/.ssh/snowflake/rsa_key.p8
   ```

   Utiliser le corps de `rsa_key.pub`, sans les en-têtes ni les retours à la ligne, pour `RSA_PUBLIC_KEY`, puis exécuter le script d'infrastructure. Ne pas réutiliser la clé publique présente dans le dépôt. Ces commandes supposent que les fichiers de clés n'existent pas encore ; conserver toute paire déjà utilisée.

6. Dans Snowsight, exécuter `snowflake/02_raw.sql` avec `TRANSFORMER`. Il crée les formats, le stage `TLC_STAGE` et les tables RAW. Le format Parquet utilise `USE_LOGICAL_TYPE=TRUE`.
7. Préparer l'environnement d'ingestion, puis charger le référentiel des zones avant le DAG :

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r ingestion/requirements.txt
   export SNOWFLAKE_ACCOUNT='ORGANISATION-COMPTE'
   python - <<'PY'
   import os
   from pathlib import Path
   from tempfile import TemporaryDirectory
   import requests
   import snowflake.connector

   with TemporaryDirectory() as folder:
       path = Path(folder) / 'taxi_zone_lookup.csv'
       response = requests.get('https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv', timeout=60)
       response.raise_for_status()
       path.write_bytes(response.content)
       with snowflake.connector.connect(
           account=os.environ['SNOWFLAKE_ACCOUNT'], user='AIRFLOW_SVC',
           role='TRANSFORMER', warehouse='NYC_TAXI_WH', database='NYC_TAXI', schema='RAW',
           private_key_file=str(Path.home() / '.ssh/snowflake/rsa_key.p8'),
       ) as conn:
           with conn.cursor() as cur:
               cur.execute('USE SECONDARY ROLES NONE')
               cur.execute(f"PUT 'file://{path}' @NYC_TAXI.RAW.TLC_STAGE AUTO_COMPRESS=FALSE OVERWRITE=FALSE")
               cur.execute("""COPY INTO NYC_TAXI.RAW.TAXI_ZONE_LOOKUP
                   FROM @NYC_TAXI.RAW.TLC_STAGE FILES=('taxi_zone_lookup.csv')
                   FILE_FORMAT=(FORMAT_NAME=NYC_TAXI.RAW.CSV_FF)
                   MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
                   INCLUDE_METADATA=(_source_file=METADATA$FILENAME, _loaded_at=METADATA$START_SCAN_TIME)
                   ON_ERROR=ABORT_STATEMENT""")
   PY
   ```

   Pour tester séparément un mois, renseigner son compte Snowflake dans `ingestion/load_month.py`, puis lancer `python ingestion/load_month.py 2025-01`. Airflow peut aussi charger directement les trois mois.

8. Copier `airflow/.env.example` vers `airflow/.env`. Dans un éditeur local, renseigner son compte et sa clé privée PKCS8 dans `private_key_content`, avec les retours à la ligne encodés en `\n` dans le JSON. La connexion s'appelle `snowflake_nyc_taxi` ; conserver `AIRFLOW_SVC`, `TRANSFORMER`, `NYC_TAXI_WH` et `NYC_TAXI`.
9. Lancer `cd airflow && astro dev start`, puis ouvrir l'adresse Airflow affichée. Le projet Astro est déjà initialisé.
10. Activer `nyc_taxi_monthly`. Le catchup crée les trois runs historiques ; ne pas utiliser Trigger. Vérifier avec `astro dev run dags list-import-errors` qu'aucune erreur d'import n'est présente.

## Pipeline

Le fichier mensuel est choisi à partir du début de l'intervalle de données Airflow, associé à la date logique du run.
Le calendrier `@monthly` et le catchup couvrent janvier, février et mars 2025 ; un seul run est actif à la fois.
Après téléchargement et PUT, COPY alimente RAW, puis `raw_mois_charge` vérifie le mois avant l'initialisation et les transformations STAGING → INTERMEDIATE → MARTS.
Le contrôle de rejet passe après FLAGGED et avant ENRICHED ; le contrôle de doublons passe après FCT_TRIPS et avant les analyses.

COPY ignore les fichiers déjà chargés, sans `FORCE`.
Les transformations mensuelles suppriment puis réinsèrent uniquement le mois traité ; les dimensions et les tables d'analyse sont recalculées.
Pour rejouer un mois, utiliser Clear sur son run historique. La relance complète de février a conservé les volumes : RAW et FLAGGED **3 577 543**, ENRICHED et FCT_TRIPS **3 305 246**.

## Contrôles qualité

| Contrôle | Vérification |
|---|---|
| `raw_mois_charge` | Le fichier du mois contient des lignes dans RAW |
| `taux_rejet_acceptable` | Taux de rejet du mois strictement inférieur à 10 % |
| `fct_trips_sans_doublon` | Aucun doublon de `trip_sk` dans les faits du mois |

Un échec bloque les tâches en aval. La [capture du contrôle KO](docs/jour4_controle_ko_fevrier.png) est conservée ; le seuil courant est revenu à 10 et les trois runs sont en succès.
L'[analyse de janvier](docs/qualite_janvier.md) compare les conditions indépendantes aux catégories de rejet exclusives du mart.

## Résultats attendus

| Table | Lignes |
|---|---:|
| `RAW.YELLOW_TRIPDATA` | 11 198 026 |
| `RAW.TAXI_ZONE_LOOKUP` | 265 |
| `INTERMEDIATE.INT_TRIPS__FLAGGED` | 11 198 026 |
| `MARTS.FCT_TRIPS` | 10 382 378 |
| `MARTS.MART_ZONE_HOURLY_DEMAND` | 11 524 |
| `MARTS.MART_DATA_QUALITY` | 18 |

[03_verifications.sql](snowflake/03_verifications.sql) contient les contrôles de volumes, de droits, d'historique COPY, de crédits et les requêtes d'analyse. Exécuter ses sections séparément : le test d'accès doit échouer et la section crédits nécessite un compte d'administration dans Snowsight.
L'historique COPY couvre les [14 derniers jours](https://docs.snowflake.com/en/sql-reference/functions/copy_history) de chargement ; le suivi des crédits couvre l'historique disponible du warehouse, jusqu'à [365 jours](https://docs.snowflake.com/en/sql-reference/account-usage/warehouse_metering_history), avec un retard possible de 3 h, ou 6 h pour les cloud services.

## Sécurité

Les outils utilisent `AIRFLOW_SVC`, avec le rôle `TRANSFORMER` au moindre privilège, sans `ACCOUNTADMIN`.
La clé privée reste hors Git ; `airflow/.env`, les clés et les données téléchargées sont ignorés. Ne pas afficher la configuration de connexion dans les logs.

## Choix techniques

- Warehouse XS, auto-suspend à 60 secondes et reprise automatique.
- Stage interne pour les fichiers TLC, sans stockage externe supplémentaire.
- Types larges en RAW : montants et distances en `FLOAT`, dates en `TIMESTAMP_NTZ`.
- `USE_LOGICAL_TYPE=TRUE` pour interpréter correctement les timestamps Parquet.
- COPY sans `FORCE` et recalcul mensuel pour l'idempotence.
- Date logique Airflow pour rejouer les mois historiques.

## Auteur

Vellanos — nom configuré dans Git.
