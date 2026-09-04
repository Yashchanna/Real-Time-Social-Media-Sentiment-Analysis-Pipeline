{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT

    COUNT(DISTINCT tweet_id) AS total_valid_tweets,

    COUNT(DISTINCT topic_category) AS total_topics,

    SUM(impressions) AS total_impressions,

    SUM(likes) AS total_likes,

    SUM(retweets) AS total_retweets,

    SUM(replies) AS total_replies,

    SUM(engagement_count) AS total_engagement,

    ROUND(
        AVG(impressions),
        2
    ) AS avg_impressions_per_tweet,

    ROUND(
        AVG(likes),
        2
    ) AS avg_likes_per_tweet,

    ROUND(
        AVG(retweets),
        2
    ) AS avg_retweets_per_tweet,

    ROUND(
        AVG(replies),
        2
    ) AS avg_replies_per_tweet,

    ROUND(
        AVG(engagement_count),
        2
    ) AS avg_engagement_per_tweet,

    ROUND(
        AVG(sentiment_score),
        4
    ) AS avg_sentiment_score,

    ROUND(
        SUM(engagement_count) * 100.0
        / NULLIF(SUM(impressions), 0),
        2
    ) AS engagement_rate_pct

FROM {{ ref('silver_valid_tweets') }}
