{{ config(
    materialized='table'
) }}

SELECT

    1 AS sentiment_key,
    'POSITIVE' AS sentiment_label

UNION ALL

SELECT

    2 AS sentiment_key,
    'NEUTRAL' AS sentiment_label

UNION ALL

SELECT

    3 AS sentiment_key,
    'NEGATIVE' AS sentiment_label
