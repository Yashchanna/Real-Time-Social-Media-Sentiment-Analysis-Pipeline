{{ config(
    materialized='table',
    schema='gold'
) }}

WITH trend_data AS (

    SELECT

        TO_DATE(trend_timestamp, 'dd-MM-yyyy') AS trend_date,

        topic_category,
        country,

        TRY_CAST(tweet_volume AS BIGINT) AS tweet_volume,
        TRY_CAST(mention_count AS BIGINT) AS mention_count,
        TRY_CAST(retweet_count AS BIGINT) AS retweet_count,

        TRY_CAST(trend_score AS DOUBLE) AS trend_score,
        TRY_CAST(sentiment_index AS DOUBLE) AS sentiment_index,

        TRY_CAST(impressions AS BIGINT) AS impressions,
        TRY_CAST(engagement_count AS BIGINT) AS engagement_count

    FROM {{ ref('silver_trends') }}

)

SELECT

    trend_date,

    COUNT(*) AS trend_record_count,

    COUNT(DISTINCT topic_category) AS total_topics,

    COUNT(DISTINCT country) AS total_countries,

    SUM(tweet_volume) AS total_tweet_volume,

    SUM(mention_count) AS total_mentions,

    SUM(retweet_count) AS total_retweets,

    SUM(impressions) AS total_impressions,

    SUM(engagement_count) AS total_engagement,

    ROUND(
        AVG(trend_score),
        4
    ) AS avg_trend_score,

    ROUND(
        AVG(sentiment_index),
        4
    ) AS avg_sentiment_index,

    ROUND(
        SUM(engagement_count) * 100.0
        / NULLIF(SUM(impressions), 0),
        2
    ) AS engagement_rate_pct

FROM trend_data

WHERE trend_date IS NOT NULL

GROUP BY trend_date

ORDER BY trend_date
