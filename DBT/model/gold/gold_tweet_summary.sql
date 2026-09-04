{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT

    COUNT(DISTINCT tweet_id) AS total_tweets,

    COUNT(DISTINCT user_id) AS total_users,

    SUM(likes) AS total_likes,

    SUM(retweets) AS total_retweets,

    SUM(replies) AS total_replies,

    SUM(impressions) AS total_impressions,

    SUM(engagement) AS total_engagement,

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
        AVG(impressions),
        2
    ) AS avg_impressions_per_tweet,

    ROUND(
        AVG(engagement),
        2
    ) AS avg_engagement_per_tweet,

    ROUND(
        SUM(engagement) * 100.0
        / NULLIF(SUM(impressions), 0),
        2
    ) AS engagement_rate_pct

FROM {{ ref('silver_tweets') }}
