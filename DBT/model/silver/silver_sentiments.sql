{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source_data AS (

    SELECT
        *
    FROM {{ ref('stg_sentiments') }}

),

/* ============================================================
   STEP 1: BASIC CLEANING AND TYPE CONVERSION
   ============================================================ */

cleaned AS (

    SELECT

        -- =====================================================
        -- TWEET ID
        -- Blank values become NULL
        -- =====================================================

        NULLIF(
            TRIM(tweet_id),
            ''
        ) AS tweet_id,


        -- =====================================================
        -- TOPIC CATEGORY
        -- Blank values become NULL
        -- =====================================================

        NULLIF(
            TRIM(topic_category),
            ''
        ) AS topic_category,


        -- =====================================================
        -- TWEET TIMESTAMP
        --
        -- Convert string timestamp into proper TIMESTAMP.
        --
        -- Expected format:
        -- 06-01-2025 10:18
        -- =====================================================

        TRY_TO_TIMESTAMP(
            TRIM(tweet_timestamp),
            'dd-MM-yyyy HH:mm'
        ) AS tweet_timestamp,


        -- =====================================================
        -- SENTIMENT SCORE
        -- =====================================================

        TRY_CAST(
            TRIM(sentiment_score) AS DOUBLE
        ) AS sentiment_score,


        -- =====================================================
        -- POSITIVE SCORE
        -- =====================================================

        TRY_CAST(
            TRIM(positive_score) AS DOUBLE
        ) AS positive_score,


        -- =====================================================
        -- NEGATIVE SCORE
        -- =====================================================

        TRY_CAST(
            TRIM(negative_score) AS DOUBLE
        ) AS negative_score,


        -- =====================================================
        -- NEUTRAL SCORE
        -- =====================================================

        TRY_CAST(
            TRIM(neutral_score) AS DOUBLE
        ) AS neutral_score,


        -- =====================================================
        -- IMPRESSIONS
        -- Example:
        -- 12556.0 -> 12556
        -- =====================================================

        TRY_CAST(
            TRIM(impressions) AS DOUBLE
        ) AS impressions,


        -- =====================================================
        -- LIKES
        -- =====================================================

        TRY_CAST(
            TRIM(likes) AS DOUBLE
        ) AS likes,


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
   ============================================================

   Rules:

   - tweet_id must exist
   - topic_category must exist
   - tweet_timestamp must be valid
   - sentiment scores must be between 0 and 1
   - impressions, likes and engagement_count cannot be negative
   - invalid numeric values become NULL
   ============================================================ */

validated AS (

    SELECT

        tweet_id,

        topic_category,

        tweet_timestamp,


        -- =====================================================
        -- SENTIMENT SCORE
        -- Valid range: 0 to 1
        -- =====================================================

        CASE
            WHEN sentiment_score BETWEEN 0 AND 1
            THEN sentiment_score
            ELSE NULL
        END AS sentiment_score,


        -- =====================================================
        -- POSITIVE SCORE
        -- Valid range: 0 to 1
        -- =====================================================

        CASE
            WHEN positive_score BETWEEN 0 AND 1
            THEN positive_score
            ELSE NULL
        END AS positive_score,


        -- =====================================================
        -- NEGATIVE SCORE
        -- Valid range: 0 to 1
        -- =====================================================

        CASE
            WHEN negative_score BETWEEN 0 AND 1
            THEN negative_score
            ELSE NULL
        END AS negative_score,


        -- =====================================================
        -- NEUTRAL SCORE
        -- Valid range: 0 to 1
        -- =====================================================

        CASE
            WHEN neutral_score BETWEEN 0 AND 1
            THEN neutral_score
            ELSE NULL
        END AS neutral_score,


        -- =====================================================
        -- IMPRESSIONS
        -- Negative values are invalid
        -- =====================================================

        CASE
            WHEN impressions >= 0
            THEN impressions
            ELSE NULL
        END AS impressions,


        -- =====================================================
        -- LIKES
        -- Negative values are invalid
        -- =====================================================

        CASE
            WHEN likes >= 0
            THEN likes
            ELSE NULL
        END AS likes,


        -- =====================================================
        -- ENGAGEMENT COUNT
        -- Negative values are invalid
        -- =====================================================

        CASE
            WHEN engagement_count >= 0
            THEN engagement_count
            ELSE NULL
        END AS engagement_count

    FROM cleaned

    -- =========================================================
    -- REMOVE RECORDS THAT CANNOT BE TRUSTED
    -- =========================================================

    WHERE tweet_id IS NOT NULL

      AND topic_category IS NOT NULL

      AND tweet_timestamp IS NOT NULL

),


/* ============================================================
   STEP 3: CALCULATE MEAN VALUES
   ============================================================

   Only valid values from the validated dataset are used.

   NULL and invalid values are ignored automatically by AVG().
   ============================================================ */

mean_values AS (

    SELECT

        -- =====================================================
        -- SENTIMENT SCORE MEAN
        -- =====================================================

        AVG(sentiment_score) AS avg_sentiment_score,


        -- =====================================================
        -- POSITIVE SCORE MEAN
        -- =====================================================

        AVG(positive_score) AS avg_positive_score,


        -- =====================================================
        -- NEGATIVE SCORE MEAN
        -- =====================================================

        AVG(negative_score) AS avg_negative_score,


        -- =====================================================
        -- NEUTRAL SCORE MEAN
        -- =====================================================

        AVG(neutral_score) AS avg_neutral_score,


        -- =====================================================
        -- IMPRESSIONS MEAN
        -- =====================================================

        AVG(impressions) AS avg_impressions,


        -- =====================================================
        -- LIKES MEAN
        -- =====================================================

        AVG(likes) AS avg_likes,


        -- =====================================================
        -- ENGAGEMENT COUNT MEAN
        -- =====================================================

        AVG(engagement_count) AS avg_engagement_count,


        -- =====================================================
        -- TIMESTAMP MEAN
        -- =====================================================

        FROM_UNIXTIME(
            CAST(
                AVG(
                    UNIX_TIMESTAMP(tweet_timestamp)
                ) AS BIGINT
            )
        ) AS avg_tweet_timestamp

    FROM validated

),


/* ============================================================
   STEP 4: HANDLE NULL / INVALID VALUES
   ============================================================

   Numeric NULL/invalid values are replaced by their
   respective column mean.

   Timestamp NULL values are replaced by mean timestamp.

   Score columns remain DOUBLE.

   Count columns are converted to BIGINT.
   ============================================================ */

imputed AS (

    SELECT

        v.tweet_id,

        v.topic_category,


        -- =====================================================
        -- TIMESTAMP
        -- NULL -> MEAN TIMESTAMP
        -- =====================================================

        COALESCE(
            v.tweet_timestamp,
            m.avg_tweet_timestamp
        ) AS tweet_timestamp,


        -- =====================================================
        -- SENTIMENT SCORE
        -- NULL/INVALID -> MEAN
        -- =====================================================

        COALESCE(
            v.sentiment_score,
            m.avg_sentiment_score,
            0
        ) AS sentiment_score,


        -- =====================================================
        -- POSITIVE SCORE
        -- NULL/INVALID -> MEAN
        -- =====================================================

        COALESCE(
            v.positive_score,
            m.avg_positive_score,
            0
        ) AS positive_score,


        -- =====================================================
        -- NEGATIVE SCORE
        -- NULL/INVALID -> MEAN
        -- =====================================================

        COALESCE(
            v.negative_score,
            m.avg_negative_score,
            0
        ) AS negative_score,


        -- =====================================================
        -- NEUTRAL SCORE
        -- NULL/INVALID -> MEAN
        -- =====================================================

        COALESCE(
            v.neutral_score,
            m.avg_neutral_score,
            0
        ) AS neutral_score,


        -- =====================================================
        -- IMPRESSIONS
        -- NULL/INVALID -> MEAN
        -- Example:
        -- 12556.0 -> 12556
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
        -- LIKES
        -- NULL/INVALID -> MEAN
        -- =====================================================

        CAST(
            ROUND(
                COALESCE(
                    v.likes,
                    m.avg_likes,
                    0
                )
            ) AS BIGINT
        ) AS likes,


        -- =====================================================
        -- ENGAGEMENT COUNT
        -- NULL/INVALID -> MEAN
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
   ============================================================

   Duplicate tweet IDs are removed.

   If the same tweet_id occurs multiple times,
   the latest tweet_timestamp is retained.
   ============================================================ */

deduplicated AS (

    SELECT

        tweet_id,

        topic_category,

        tweet_timestamp,

        sentiment_score,

        positive_score,

        negative_score,

        neutral_score,

        impressions,

        likes,

        engagement_count

    FROM (

        SELECT

            *,

            ROW_NUMBER() OVER (

                PARTITION BY tweet_id

                ORDER BY
                    tweet_timestamp DESC

            ) AS row_num

        FROM imputed

    )

    WHERE row_num = 1

)


/* ============================================================
   STEP 6: FINAL SILVER TABLE
   ============================================================ */

SELECT

    tweet_id,

    topic_category,

    tweet_timestamp,

    sentiment_score,

    positive_score,

    negative_score,

    neutral_score,

    impressions,

    likes,

    engagement_count

FROM deduplicated
