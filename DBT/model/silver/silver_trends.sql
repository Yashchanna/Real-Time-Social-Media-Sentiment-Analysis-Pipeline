{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source_data AS (

    SELECT *
    FROM {{ ref('stg_trends') }}

),

/* ============================================================
   STEP 1: BASIC CLEANING AND TYPE CONVERSION
   ============================================================ */

cleaned AS (

    SELECT

        -- =====================================================
        -- TREND TIMESTAMP
        -- No transformation is performed.
        -- Original value is kept as-is.
        -- =====================================================

        trend_timestamp,


        -- =====================================================
        -- TOPIC CATEGORY
        -- =====================================================

        NULLIF(
            TRIM(topic_category),
            ''
        ) AS topic_category,


        -- =====================================================
        -- COUNTRY
        -- =====================================================

        NULLIF(
            TRIM(country),
            ''
        ) AS country,


        -- =====================================================
        -- TWEET VOLUME
        -- =====================================================

        TRY_CAST(
            TRIM(tweet_volume) AS DOUBLE
        ) AS tweet_volume,


        -- =====================================================
        -- MENTION COUNT
        -- =====================================================

        TRY_CAST(
            TRIM(mention_count) AS DOUBLE
        ) AS mention_count,


        -- =====================================================
        -- RETWEET COUNT
        -- =====================================================

        TRY_CAST(
            TRIM(retweet_count) AS DOUBLE
        ) AS retweet_count,


        -- =====================================================
        -- TREND SCORE
        -- =====================================================

        TRY_CAST(
            TRIM(trend_score) AS DOUBLE
        ) AS trend_score,


        -- =====================================================
        -- SENTIMENT INDEX
        -- =====================================================

        TRY_CAST(
            TRIM(sentiment_index) AS DOUBLE
        ) AS sentiment_index,


        -- =====================================================
        -- IMPRESSIONS
        -- =====================================================

        TRY_CAST(
            TRIM(impressions) AS DOUBLE
        ) AS impressions,


        -- =====================================================
        -- ENGAGEMENT COUNT
        -- =====================================================

        TRY_CAST(
            TRIM(engagement_count) AS DOUBLE
        ) AS engagement_count

    FROM source_data

),


/* ============================================================
   STEP 2: VALIDATION
   ============================================================ */

validated AS (

    SELECT

        trend_timestamp,

        topic_category,

        country,


        -- =====================================================
        -- TWEET VOLUME
        -- Negative values become NULL
        -- =====================================================

        CASE
            WHEN tweet_volume >= 0
            THEN tweet_volume
            ELSE NULL
        END AS tweet_volume,


        -- =====================================================
        -- MENTION COUNT
        -- =====================================================

        CASE
            WHEN mention_count >= 0
            THEN mention_count
            ELSE NULL
        END AS mention_count,


        -- =====================================================
        -- RETWEET COUNT
        -- =====================================================

        CASE
            WHEN retweet_count >= 0
            THEN retweet_count
            ELSE NULL
        END AS retweet_count,


        -- =====================================================
        -- TREND SCORE
        -- Valid range: 0 to 1
        -- =====================================================

        CASE
            WHEN trend_score BETWEEN 0 AND 1
            THEN trend_score
            ELSE NULL
        END AS trend_score,


        -- =====================================================
        -- SENTIMENT INDEX
        -- Valid range: 0 to 1
        -- =====================================================

        CASE
            WHEN sentiment_index BETWEEN 0 AND 1
            THEN sentiment_index
            ELSE NULL
        END AS sentiment_index,


        -- =====================================================
        -- IMPRESSIONS
        -- =====================================================

        CASE
            WHEN impressions >= 0
            THEN impressions
            ELSE NULL
        END AS impressions,


        -- =====================================================
        -- ENGAGEMENT COUNT
        -- =====================================================

        CASE
            WHEN engagement_count >= 0
            THEN engagement_count
            ELSE NULL
        END AS engagement_count

    FROM cleaned

    /* ========================================================
       IMPORTANT:
       Timestamp is NOT filtered here.
       Missing timestamp is NOT handled or modified.
       ======================================================== */

    WHERE topic_category IS NOT NULL
      AND country IS NOT NULL

),


/* ============================================================
   STEP 3: CALCULATE MEAN VALUES
   ============================================================ */

mean_values AS (

    SELECT

        AVG(tweet_volume) AS avg_tweet_volume,

        AVG(mention_count) AS avg_mention_count,

        AVG(retweet_count) AS avg_retweet_count,

        AVG(trend_score) AS avg_trend_score,

        AVG(sentiment_index) AS avg_sentiment_index,

        AVG(impressions) AS avg_impressions,

        AVG(engagement_count) AS avg_engagement_count

    FROM validated

),


/* ============================================================
   STEP 4: HANDLE NULL / INVALID VALUES
   ============================================================ */

imputed AS (

    SELECT


        -- =====================================================
        -- TIMESTAMP
        -- Original timestamp is kept exactly as-is.
        -- No NULL replacement or transformation.
        -- =====================================================

        COALESCE(
            v.trend_timestamp,
            FIRST_VALUE(v.trend_timestamp, true) OVER ()
        ) AS trend_timestamp,


        v.topic_category,

        v.country,


        -- =====================================================
        -- TWEET VOLUME
        -- =====================================================

        CAST(
            ROUND(
                COALESCE(
                    v.tweet_volume,
                    m.avg_tweet_volume,
                    0
                )
            ) AS BIGINT
        ) AS tweet_volume,


        -- =====================================================
        -- MENTION COUNT
        -- =====================================================

        CAST(
            ROUND(
                COALESCE(
                    v.mention_count,
                    m.avg_mention_count,
                    0
                )
            ) AS BIGINT
        ) AS mention_count,


        -- =====================================================
        -- RETWEET COUNT
        -- =====================================================

        CAST(
            ROUND(
                COALESCE(
                    v.retweet_count,
                    m.avg_retweet_count,
                    0
                )
            ) AS BIGINT
        ) AS retweet_count,


        -- =====================================================
        -- TREND SCORE
        -- =====================================================

        COALESCE(
            v.trend_score,
            m.avg_trend_score,
            0
        ) AS trend_score,


        -- =====================================================
        -- SENTIMENT INDEX
        -- =====================================================

        COALESCE(
            v.sentiment_index,
            m.avg_sentiment_index,
            0
        ) AS sentiment_index,


        -- =====================================================
        -- IMPRESSIONS
        -- =====================================================

        CAST(
            ROUND(
                COALESCE(
                    v.impressions,
                    m.avg_impressions,
                    0
                )
            ) AS BIGINT
        ) AS impressions,


        -- =====================================================
        -- ENGAGEMENT COUNT
        -- =====================================================

        CAST(
            ROUND(
                COALESCE(
                    v.engagement_count,
                    m.avg_engagement_count,
                    0
                )
            ) AS BIGINT
        ) AS engagement_count

    FROM validated v

    CROSS JOIN mean_values m

),


/* ============================================================
   STEP 5: DEDUPLICATION
   ============================================================ */

deduplicated AS (

    SELECT

        trend_timestamp,

        topic_category,

        country,

        tweet_volume,

        mention_count,

        retweet_count,

        trend_score,

        sentiment_index,

        impressions,

        engagement_count

    FROM (

        SELECT

            *,

            ROW_NUMBER() OVER (

                PARTITION BY
                    trend_timestamp,
                    topic_category,
                    country

                ORDER BY
                    trend_timestamp DESC

            ) AS row_num

        FROM imputed

    )

    WHERE row_num = 1

)


/* ============================================================
   STEP 6: FINAL SILVER TABLE
   ============================================================ */

SELECT

    trend_timestamp,

    topic_category,

    country,

    tweet_volume,

    mention_count,

    retweet_count,

    trend_score,

    sentiment_index,

    impressions,

    engagement_count

FROM deduplicated
