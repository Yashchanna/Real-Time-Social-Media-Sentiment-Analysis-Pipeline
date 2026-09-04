{{ config(
    materialized='table',
    schema='gold'
) }}

WITH ranked_tweets AS (

    SELECT

        tweet_id,

        user_id,

        tweet_text,

        `timestamp.1` AS tweet_timestamp,

        likes,

        retweets,

        replies,

        impressions,

        engagement,

        ROUND(
            engagement * 100.0
            / NULLIF(impressions, 0),
            2
        ) AS engagement_rate_pct,

        ROW_NUMBER() OVER (
            ORDER BY engagement DESC
        ) AS engagement_rank,

        ROW_NUMBER() OVER (
            ORDER BY likes DESC
        ) AS likes_rank,

        ROW_NUMBER() OVER (
            ORDER BY retweets DESC
        ) AS retweet_rank,

        ROW_NUMBER() OVER (
            ORDER BY impressions DESC
        ) AS impression_rank

    FROM {{ ref('silver_tweets') }}

)

SELECT *

FROM ranked_tweets

WHERE engagement_rank <= 100
   OR likes_rank <= 100
   OR retweet_rank <= 100
   OR impression_rank <= 100
