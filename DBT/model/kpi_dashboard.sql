{{ config(
    materialized='table'
) }}

SELECT

    COUNT(tweet_id) AS total_tweets,

    COUNT(DISTINCT user_id) AS total_users,

    SUM(likes) AS total_likes,

    SUM(retweets) AS total_retweets,

    SUM(replies) AS total_replies,

    SUM(impressions) AS total_impressions,

    SUM(engagement_count) AS total_engagement,

    AVG(sentiment_score) AS average_sentiment

FROM {{ ref('fact_social_media') }}
