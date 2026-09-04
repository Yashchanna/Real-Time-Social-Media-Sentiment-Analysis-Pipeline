{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source_data AS (

    SELECT *
    FROM {{ ref('stg_valid_tweets') }}

),

/* ============================================================
   STEP 1: BASIC CLEANING AND TYPE CONVERSION
   ============================================================ */

cleaned AS (

    SELECT

        -- =====================================================
        -- TWEET ID
        -- =====================================================

        NULLIF(
            TRIM(tweet_id),
            ''
        ) AS tweet_id,


        -- =====================================================
        -- TOPIC CATEGORY
        -- =====================================================

        NULLIF(
            TRIM(topic_category),
            ''
        ) AS topic_category,


        -- =====================================================
        -- TWEET TEXT
        -- Blank text becomes NULL
        -- =====================================================

        NULLIF(
            TRIM(tweet_text),
            ''
        ) AS tweet_text,


        -- =====================================================
        -- TWEET TIMESTAMP
        --
        -- First try Spark's standard timestamp conversion.
        -- Then support common source formats.
        -- =====================================================

        COALESCE(

            TRY_CAST(
                TRIM(tweet_timestamp) AS TIMESTAMP
            ),

            TRY_TO_TIMESTAMP(
                TRIM(tweet_timestamp),
                'dd-MM-yyyy HH:mm'
            ),

            TRY_TO_TIMESTAMP(
                TRIM(tweet_timestamp),
                'dd-MM-yyyy HH:mm:ss'
            )

        ) AS tweet_timestamp,


        -- =====================================================
        -- IMPRESSIONS
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
        -- RETWEETS
        -- =====================================================

        TRY_CAST(
            TRIM(retweets) AS DOUBLE
        ) AS retweets,


        -- =====================================================
        -- REPLIES
        -- =====================================================

        TRY_CAST(
            TRIM(replies) AS DOUBLE
        ) AS replies,


        -- =====================================================
        -- ENGAGEMENT COUNT
        -- =====================================================

        TRY_CAST(
            TRIM(engagement_count) AS DOUBLE
        ) AS engagement_count,


        -- =====================================================
        -- SENTIMENT SCORE
        -- =====================================================

        TRY_CAST(
            TRIM(sentiment_score) AS DOUBLE
        ) AS sentiment_score


    FROM source_data

),


/* ============================================================
   STEP 2: VALIDATION
   ============================================================ */

validated AS (

    SELECT

        tweet_id,

        topic_category,

        tweet_text,

        tweet_timestamp,


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
        -- =====================================================

        CASE
            WHEN likes >= 0
            THEN likes
            ELSE NULL
        END AS likes,


        -- =====================================================
        -- RETWEETS
        -- =====================================================

        CASE
            WHEN retweets >= 0
            THEN retweets
            ELSE NULL
        END AS retweets,


        -- =====================================================
        -- REPLIES
        -- =====================================================

        CASE
            WHEN replies >= 0
            THEN replies
            ELSE NULL
        END AS replies,


        -- =====================================================
        -- ENGAGEMENT COUNT
        -- =====================================================

        CASE
            WHEN engagement_count >= 0
            THEN engagement_count
            ELSE NULL
        END AS engagement_count,


        -- =====================================================
        -- SENTIMENT SCORE
        --
        -- Valid range: 0 to 1
        -- =====================================================

        CASE
            WHEN sentiment_score BETWEEN 0 AND 1
            THEN sentiment_score
            ELSE NULL
        END AS sentiment_score


    FROM cleaned


    /* ========================================================
       REMOVE RECORDS THAT CANNOT BE IDENTIFIED
       
       Tweet ID is required because it is the unique identifier.
       
       Timestamp is NOT filtered here because we handle
       missing values during imputation.
       ======================================================== */

    WHERE tweet_id IS NOT NULL
      AND topic_category IS NOT NULL
      AND tweet_text IS NOT NULL

),


/* ============================================================
   STEP 3: CALCULATE MEAN VALUES
   ============================================================ */

mean_values AS (

    SELECT

        -- =====================================================
        -- NUMERIC MEANS
        -- AVG automatically ignores NULL values
        -- =====================================================

        AVG(impressions) AS avg_impressions,

        AVG(likes) AS avg_likes,

        AVG(retweets) AS avg_retweets,

        AVG(replies) AS avg_replies,

        AVG(engagement_count) AS avg_engagement_count,

        AVG(sentiment_score) AS avg_sentiment_score,


        -- =====================================================
        -- MEAN TIMESTAMP
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
   ============================================================ */

imputed AS (

    SELECT


        -- =====================================================
        -- TWEET ID
        -- =====================================================

        v.tweet_id,


        -- =====================================================
        -- TOPIC CATEGORY
        -- =====================================================

        v.topic_category,


        -- =====================================================
        -- TWEET TEXT
        -- =====================================================

        v.tweet_text,


        -- =====================================================
        -- TWEET TIMESTAMP
        -- NULL -> MEAN TIMESTAMP
        -- =====================================================

        COALESCE(
            v.tweet_timestamp,
            m.avg_tweet_timestamp
        ) AS tweet_timestamp,


        -- =====================================================
        -- IMPRESSIONS
        -- NULL / INVALID -> MEAN
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
        -- RETWEETS
        -- =====================================================

        CAST(
            ROUND(
                COALESCE(
                    v.retweets,
                    m.avg_retweets,
                    0
                )
            ) AS BIGINT
        ) AS retweets,


        -- =====================================================
        -- REPLIES
        -- =====================================================

        CAST(
            ROUND(
                COALESCE(
                    v.replies,
                    m.avg_replies,
                    0
                )
            ) AS BIGINT
        ) AS replies,


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
        ) AS engagement_count,


        -- =====================================================
        -- SENTIMENT SCORE
        -- =====================================================

        COALESCE(
            v.sentiment_score,
            m.avg_sentiment_score,
            0
        ) AS sentiment_score


    FROM validated v

    CROSS JOIN mean_values m

),


/* ============================================================
   STEP 5: DEDUPLICATION
   ============================================================

   Tweet ID should uniquely identify a tweet.

   If the same tweet_id occurs multiple times,
   keep only one record.

   ============================================================ */

deduplicated AS (

    SELECT

        tweet_id,

        topic_category,

        tweet_text,

        tweet_timestamp,

        impressions,

        likes,

        retweets,

        replies,

        engagement_count,

        sentiment_score


    FROM (

        SELECT

            *,

            ROW_NUMBER() OVER (

                PARTITION BY tweet_id

                ORDER BY tweet_timestamp DESC

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

    tweet_text,

    tweet_timestamp,

    impressions,

    likes,

    retweets,

    replies,

    engagement_count,

    sentiment_score

FROM deduplicated
