{{ config(
    materialized='table',
    schema='gold'
) }}

WITH sentiment_classified AS (

    SELECT

        tweet_id,

        topic_category,

        sentiment_score,

        impressions,

        likes,

        engagement_count,

        CASE
            WHEN positive_score >= negative_score
                 AND positive_score >= neutral_score
                THEN 'Positive'

            WHEN negative_score >= positive_score
                 AND negative_score >= neutral_score
                THEN 'Negative'

            ELSE 'Neutral'
        END AS sentiment_category

    FROM {{ ref('silver_sentiments') }}

)

SELECT

    sentiment_category,

    COUNT(DISTINCT tweet_id) AS total_tweets,

    ROUND(
        AVG(sentiment_score),
        4
    ) AS avg_sentiment_score,

    SUM(impressions) AS total_impressions,

    SUM(likes) AS total_likes,

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
        AVG(engagement_count),
        2
    ) AS avg_engagement_per_tweet

FROM sentiment_classified

GROUP BY sentiment_category
