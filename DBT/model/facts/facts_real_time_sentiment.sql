{{ config(
    materialized='table'
) }}

WITH tweets AS (

    SELECT *
    FROM {{ ref('stg_tweets') }}

),

valid_tweets AS (

    SELECT *
    FROM {{ ref('stg_valid_tweets') }}

),

sentiment AS (

    SELECT *
    FROM {{ ref('stg_sentiment') }}

),

users AS (

    SELECT *
    FROM {{ ref('stg_user_metadata') }}

),

topics AS (

    SELECT *
    FROM {{ ref('dim_topic') }}

),

dates AS (

    SELECT *
    FROM {{ ref('dim_date') }}

)

SELECT

    t.tweet_id,

    t.user_id,

 COALESCE(tp.topic_key, -1) AS topic_key,

    d.date_key,

    t.tweet_text,

    t.likes,

    t.retweets,

    t.replies,

    t.impressions,

    v.engagement_count,

    s.sentiment_score,

    s.positive_score,

    s.negative_score,

    s.neutral_score,

    current_timestamp() AS gold_load_time

FROM tweets t

LEFT JOIN valid_tweets v
       ON t.tweet_id = v.tweet_id

LEFT JOIN sentiment s
       ON t.tweet_id = s.tweet_id

LEFT JOIN users u
       ON t.user_id = u.user_id

LEFT JOIN topics tp
       ON u.topic_category = tp.topic_category

LEFT JOIN dates d
       ON t.tweet_date = d.tweet_date
