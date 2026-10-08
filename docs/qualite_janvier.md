# Qualité des trajets — janvier 2025

Comptages exécutés le 8 octobre 2026 avec `AIRFLOW_SVC` / `TRANSFORMER`, sur `yellow_tripdata_2025-01.parquet` : **3 475 226 lignes**.
Les requêtes sont dans les sections F de [03_verifications.sql](../snowflake/03_verifications.sql).
Les seuils correspondent au DAG : 180 minutes et 100 miles.

| Règle, dans l'ordre du CASE | Conditions indépendantes | MART_DATA_QUALITY |
|---|---:|---:|
| `timestamp_null` | 0 | 0 |
| `duration_non_positive` | 2 051 | 2 051 |
| `duration_too_long` | 1 377 | 1 377 |
| `pickup_outside_file_month` | 22 | 22 |
| `distance_out_of_range` | 91 055 | 90 327 |
| `amount_non_positive` | 144 998 | 130 112 |
| `zone_null` | 0 | 0 |

`MART_DATA_QUALITY` contient également **3 251 337 lignes `valid`**, soit 93,558 % du fichier.
Les cinq catégories de rejet totalisent **223 889 lignes** ; avec les lignes valides, elles couvrent exactement les 3 475 226 lignes de janvier.
Les catégories `timestamp_null` et `zone_null` sont absentes du mart car leur effectif est nul.

Les conditions indépendantes peuvent se chevaucher : leurs effectifs ne s'additionnent pas pour obtenir un nombre de trajets rejetés.
`int_trips__flagged` attribue une seule `rejection_reason` à chaque trajet, la première condition vraie dans le `CASE`.
Ainsi, la règle des montants compte 144 998 lignes indépendamment, mais seules 130 112 restent dans cette catégorie après les règles précédentes.
Les effectifs du mart correspondent exactement aux catégories recalculées depuis `INT_TRIPS__FLAGGED`.
