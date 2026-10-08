# Réponse à la direction d'Hudson Cab Partners

## La question

Où et quand la demande de taxis jaunes est-elle la plus forte à New York, et combien rapporte un trajet selon la zone, l'heure et le mode de paiement ?

## La requête

La demande correspond au nombre de trajets, au grain zone de prise en charge × heure × mode de paiement, sur janvier à mars 2025.

```sql
SELECT
    COALESCE(z.zone_name, 'Inconnue') AS pickup_zone,
    COALESCE(z.borough, 'Inconnu') AS borough,
    f.pickup_hour,
    COALESCE(p.payment_type_label, 'Inconnu') AS payment_type,
    COUNT(*) AS nb_trips,
    ROUND(SUM(f.total_amount), 2) AS total_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_revenue_per_trip
FROM NYC_TAXI.MARTS.FCT_TRIPS f
LEFT JOIN NYC_TAXI.MARTS.DIM_ZONE z
    ON z.zone_key = f.pickup_zone_key
LEFT JOIN NYC_TAXI.MARTS.DIM_PAYMENT_TYPE p
    ON p.payment_type_key = f.payment_type_key
WHERE f.source_file_month >= '2025-01-01'::date
  AND f.source_file_month < '2025-04-01'::date
GROUP BY f.pickup_zone_key, z.zone_name, z.borough,
         f.pickup_hour, f.payment_type_key, p.payment_type_label
ORDER BY nb_trips DESC, f.pickup_zone_key, f.pickup_hour, f.payment_type_key
LIMIT 10;
```

## Le résultat : les 10 premières lignes

Requête exécutée le 8 octobre 2026 avec `AIRFLOW_SVC`, rôle `TRANSFORMER`. Montants en dollars américains, issus de `total_amount`.

| Zone | Borough | Heure | Paiement | Trajets | Revenu total ($) | Revenu moyen par trajet ($) |
|---|---|---:|---|---:|---:|---:|
| Midtown Center | Manhattan | 18 h | Credit card | 38 263 | 958 710,44 | 25,06 |
| Midtown Center | Manhattan | 17 h | Credit card | 38 027 | 1 002 175,17 | 26,35 |
| Midtown Center | Manhattan | 19 h | Credit card | 31 661 | 767 770,26 | 24,25 |
| Midtown Center | Manhattan | 16 h | Credit card | 30 696 | 814 345,73 | 26,53 |
| Upper East Side South | Manhattan | 15 h | Credit card | 29 985 | 609 756,01 | 20,34 |
| Upper East Side South | Manhattan | 14 h | Credit card | 29 854 | 609 412,96 | 20,41 |
| Upper East Side North | Manhattan | 15 h | Credit card | 29 757 | 614 087,29 | 20,64 |
| Upper East Side South | Manhattan | 17 h | Credit card | 29 751 | 654 470,23 | 22,00 |
| Upper East Side South | Manhattan | 18 h | Credit card | 29 553 | 636 525,50 | 21,54 |
| Upper East Side South | Manhattan | 16 h | Credit card | 28 712 | 633 439,25 | 22,06 |

## Ce qu'il faut en retenir

1. Le groupe le plus fréquent est Midtown Center à 18 h avec paiement par carte, avec 38 263 trajets et un revenu moyen de 25,06 dollars.
2. Les dix groupes les plus fréquents concernent Manhattan, entre 14 h et 19 h, avec paiement par carte.
3. Dans ces dix groupes, le revenu moyen par trajet varie de 20,34 à 26,53 dollars.

## Les limites

- La période analysée couvre janvier à mars 2025 ; chaque heure regroupe tous les jours de cette période.
- Seuls les trajets considérés valides et dédoublonnés arrivent dans `FCT_TRIPS`.
- Certaines zones ou certains codes peuvent être inconnus ; les jointures conservent ces trajets.
- Les données publiques TLC couvrent les taxis jaunes, pas uniquement les 180 taxis d'Hudson Cab Partners.
- Le revenu correspond au montant total déclaré, pourboires et frais compris, et ne représente pas le bénéfice.
- Le classement porte sur les groupes zone × heure × paiement ; il ne classe pas les zones tous paiements confondus.
