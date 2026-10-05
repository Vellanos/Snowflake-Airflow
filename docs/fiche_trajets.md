# Fiche source — Trajets NYC Yellow Taxi, janvier 2025

## Identité

| Rubrique | Réponse |
|---|---|
| Nom de la source | Yellow Taxi Trip Records, janvier 2025 |
| Producteur des données | Fournisseurs technologiques agréés TPEP ; publication par NYC Taxi & Limousine Commission (TLC) |
| Adresse (URL) | [Parquet janvier 2025](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet) |
| Accès (public, authentifié) | Public, sans authentification |
| Format du fichier | Apache Parquet |
| Fréquence de publication | Mensuelle |
| Délai entre la période couverte et la publication | Habituellement deux mois selon TLC ; date précise de première publication de ce fichier non vérifiée |

Source : [page TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
La TLC ne garantit pas l'exactitude des données transmises par les fournisseurs.

## Volume mesuré

Mesures du 5 octobre 2026 sous WSL avec DuckDB 1.5.6. Parquet téléchargé dans un dossier temporaire hors dépôt.

| Fichier | Taille | Nombre de lignes | Nombre de colonnes | Outil et commande utilisés |
|---|---|---|---|---|
| `yellow_tripdata_2025-01.parquet` | 59,158,238 octets (56.42 Mio) | 3 475 226 | 20 | `Path(fichier).stat().st_size`, DuckDB `COUNT(*)` et `DESCRIBE` |

Commandes de mesure (DuckDB) :

```python
from pathlib import Path
import duckdb

p = Path('/tmp/nyc-day1/yellow_tripdata_2025-01.parquet')
c = duckdb.connect()
c.execute("SET memory_limit='512MB'")
print(p.stat().st_size)
print(c.execute('SELECT COUNT(*) FROM read_parquet(?)', [str(p)]).fetchone())
schema = c.execute('DESCRIBE SELECT * FROM read_parquet(?)', [str(p)]).fetchall()
print(len(schema), schema)
print(c.execute('SELECT * FROM read_parquet(?) LIMIT 1', [str(p)]).fetchone())
```

## Colonnes

Types lus par DuckDB, avant conversion Snowflake. Exemples issus d’une ligne (`LIMIT 1`), non représentative du fichier.
Significations : [dictionnaire TLC Yellow Taxi, 18 mars 2025](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).

| Colonne | Type dans le fichier (DuckDB) | Signification | Exemple de valeur |
|---|---|---|---|
| `VendorID` | `INTEGER` | Fournisseur TPEP | `1` |
| `tpep_pickup_datetime` | `TIMESTAMP` | Départ compteur | `2025-01-01 00:18:38` |
| `tpep_dropoff_datetime` | `TIMESTAMP` | Arrêt compteur | `2025-01-01 00:26:59` |
| `passenger_count` | `BIGINT` | Passagers | `1` |
| `trip_distance` | `DOUBLE` | Distance (miles) | `1.6` |
| `RatecodeID` | `BIGINT` | Tarif final | `1` |
| `store_and_fwd_flag` | `VARCHAR` | Transmission différée | `N` |
| `PULocationID` | `INTEGER` | Zone départ TLC | `229` |
| `DOLocationID` | `INTEGER` | Zone arrivée TLC | `237` |
| `payment_type` | `BIGINT` | Paiement | `1` |
| `fare_amount` | `DOUBLE` | Tarif compteur | `10.0` |
| `extra` | `DOUBLE` | Suppléments | `3.5` |
| `mta_tax` | `DOUBLE` | Taxe MTA | `0.5` |
| `tip_amount` | `DOUBLE` | Pourboire carte, hors espèces | `3.0` |
| `tolls_amount` | `DOUBLE` | Péages | `0.0` |
| `improvement_surcharge` | `DOUBLE` | Surcharge amélioration | `1.0` |
| `total_amount` | `DOUBLE` | Total, hors pourboires espèces | `18.0` |
| `congestion_surcharge` | `DOUBLE` | Surcharge NYS | `2.5` |
| `Airport_fee` | `DOUBLE` | Supplément départ LGA/JFK | `0.0` |
| `cbd_congestion_fee` | `DOUBLE` | Taxe zone MTA, depuis 05/01/2025 | `0.0` |

## Codes

Codes du dictionnaire TLC, y compris ceux absents du fichier de janvier.

| Colonne | Valeur | Signification |
|---|---|---|
| `VendorID` | `1` | Creative Mobile Technologies, LLC |
| `VendorID` | `2` | Curb Mobility, LLC |
| `VendorID` | `6` | Myle Technologies Inc |
| `VendorID` | `7` | Helix |
| `RatecodeID` | `1` | Standard |
| `RatecodeID` | `2` | JFK |
| `RatecodeID` | `3` | Newark |
| `RatecodeID` | `4` | Nassau/Westchester |
| `RatecodeID` | `5` | Négocié |
| `RatecodeID` | `6` | Groupe |
| `RatecodeID` | `99` | Inconnu |
| `payment_type` | `0` | Flex Fare |
| `payment_type` | `1` | Carte |
| `payment_type` | `2` | Espèces |
| `payment_type` | `3` | Gratuit |
| `payment_type` | `4` | Litige |
| `payment_type` | `5` | Inconnu |
| `payment_type` | `6` | Annulé |
| `store_and_fwd_flag` | `Y` | Transmission différée |
| `store_and_fwd_flag` | `N` | Sans transmission différée |

Valeurs observées (`GROUP BY`) :

- `VendorID` : 1, 2, 6, 7.
- `RatecodeID` : 1, 2, 3, 4, 5, 6, 99 et NULL.
- `payment_type` : 0, 1, 2, 3, 4, 5 ; le code 6 n'est pas présent.
- `store_and_fwd_flag` : N, Y et NULL. NULL signifie absence de valeur, pas un code métier.

## Ce qui a surpris

- **22 départs hors janvier** : minimum `2024-12-31 20:47:55`, maximum `2025-02-01 00:00:44`.
- **63 037 totaux négatifs**, minimum **−901,00** ; cause non déterminée.
- **124 arrivées antérieures au départ** : durée négative selon les deux horodatages du fichier.

Ces anomalies peuvent concerner les mêmes trajets. Requêtes de mesure :

```sql
CREATE VIEW trips AS
SELECT * FROM read_parquet('/tmp/nyc-day1/yellow_tripdata_2025-01.parquet');

SELECT COUNT(*) AS hors_janvier,
       MIN(tpep_pickup_datetime), MAX(tpep_pickup_datetime)
FROM trips
WHERE tpep_pickup_datetime < TIMESTAMP '2025-01-01'
   OR tpep_pickup_datetime >= TIMESTAMP '2025-02-01';

SELECT COUNT(*) AS totaux_negatifs, MIN(total_amount)
FROM trips WHERE total_amount < 0;

SELECT COUNT(*) AS durees_negatives
FROM trips WHERE tpep_dropoff_datetime < tpep_pickup_datetime;
```

## Place dans le pipeline

Les fichiers TLC alimentent RAW avec le nom du fichier et la date de chargement. STAGING renomme et type les colonnes ; INTERMEDIATE classe les trajets puis enrichit les trajets valides ; MARTS fournit les faits, dimensions et analyses. Airflow orchestre le dépôt sur stage, la copie, les transformations et les contrôles, à raison d’un mois par exécution. Les zones constituent la seconde source de référence.

Sources : `README.md`, `CONTRAT_RAW.md`, `docs/architecture.png`. Périmètre du Jour 1 : fiche source et infrastructure.
