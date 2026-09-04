{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT

    country,

    COUNT(*) AS trend_record_count,

    COUNT(DISTINCT topic_category) AS total_topics,

    SUM(tweet_volume) AS total_tweet_volume,

    SUM(mention_count) AS total_mentions,

    SUM(retweet_count) AS total_retweets,

    SUM(impressions) AS total_impressions,

    SUM(engagement_count) AS total_engagement,

    ROUND(AVG(trend_score), 4) AS avg_trend_score,

    ROUND(AVG(sentiment_index), 4) AS avg_sentiment_index,

    ROUND(
        SUM(engagement_count) * 100.0
        / NULLIF(SUM(impressions), 0),
        2
    ) AS engagement_rate_pct

FROM {{ ref('silver_trends') }}

GROUP BY country
