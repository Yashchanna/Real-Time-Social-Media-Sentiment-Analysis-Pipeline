{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT

    COUNT(*) AS total_trend_records,

    COUNT(DISTINCT topic_category) AS total_topics,

    COUNT(DISTINCT country) AS total_countries,

    SUM(tweet_volume) AS total_tweet_volume,

    SUM(mention_count) AS total_mentions,

    SUM(retweet_count) AS total_retweets,

    SUM(impressions) AS total_impressions,

    SUM(engagement_count) AS total_engagement,

    ROUND(AVG(trend_score), 4) AS avg_trend_score,

    ROUND(AVG(sentiment_index), 4) AS avg_sentiment_index,

    ROUND(AVG(tweet_volume), 2) AS avg_tweet_volume,

    ROUND(AVG(mention_count), 2) AS avg_mentions,

    ROUND(AVG(retweet_count), 2) AS avg_retweets,

    ROUND(AVG(engagement_count), 2) AS avg_engagement,

    ROUND(
        SUM(engagement_count) * 100.0
        / NULLIF(SUM(impressions), 0),
        2
    ) AS engagement_rate_pct

FROM {{ ref('silver_trends') }}
