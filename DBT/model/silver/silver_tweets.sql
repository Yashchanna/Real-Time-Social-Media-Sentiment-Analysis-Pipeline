{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source_data AS (

    SELECT *
    FROM {{ ref('stg_tweets') }}

),

/* ============================================================
   STEP 1: BASIC CLEANING AND TYPE CONVERSION
   ============================================================ */

cleaned AS (

    SELECT

        -- Tweet ID
        NULLIF(
            TRIM(tweet_id),
            ''
        ) AS tweet_id,

        -- User ID
        -- Example: 5318.0 -> 5318
        CAST(
            TRY_CAST(
                TRIM(user_id) AS DOUBLE
            ) AS BIGINT
        ) AS user_id,

        -- Tweet text
        NULLIF(
            TRIM(tweet_text),
            ''
        ) AS tweet_text,

        -- Original timestamp column actually contains platform
        NULLIF(
            LOWER(TRIM(`timestamp`)),
            ''
        ) AS platform,

        -- Actual timestamp
        TRY_TO_TIMESTAMP(
            TRIM(`timestamp.1`),
            'dd-MM-yyyy HH:mm'
        ) AS tweet_timestamp,

        -- Numeric columns
        TRY_CAST(TRIM(likes) AS DOUBLE) AS likes,

        TRY_CAST(TRIM(retweets) AS DOUBLE) AS retweets,

        TRY_CAST(TRIM(replies) AS DOUBLE) AS replies,

        TRY_CAST(TRIM(impressions) AS DOUBLE) AS impressions,

        TRY_CAST(TRIM(engagement) AS DOUBLE) AS engagement

    FROM source_data

),

/* ============================================================
   STEP 2: VALIDATION AND REMOVAL OF INVALID RECORDS
   ============================================================ */

validated AS (

    SELECT

        tweet_id,

        user_id,

        tweet_text,

        platform,

        tweet_timestamp,

        -- Negative values are invalid
        CASE
            WHEN likes >= 0
            THEN likes
            ELSE NULL
        END AS likes,

        CASE
            WHEN retweets >= 0
            THEN retweets
            ELSE NULL
        END AS retweets,

        CASE
            WHEN replies >= 0
            THEN replies
            ELSE NULL
        END AS replies,

        CASE
            WHEN impressions >= 0
            THEN impressions
            ELSE NULL
        END AS impressions,

        CASE
            WHEN engagement >= 0
            THEN engagement
            ELSE NULL
        END AS engagement

    FROM cleaned

    -- Remove records where essential columns are missing
    WHERE tweet_id IS NOT NULL
      AND user_id IS NOT NULL
      AND tweet_text IS NOT NULL

),

/* ============================================================
   STEP 3: CALCULATE MEAN VALUES
   ============================================================ */

mean_values AS (

    SELECT

        AVG(likes) AS avg_likes,

        AVG(retweets) AS avg_retweets,

        AVG(replies) AS avg_replies,

        AVG(impressions) AS avg_impressions,

        AVG(engagement) AS avg_engagement,

        -- Calculate average timestamp
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
   STEP 4: NULL / INVALID VALUE IMPUTATION
   ============================================================ */

imputed AS (

    SELECT

        v.tweet_id,

        v.user_id,

        v.tweet_text,

        -- Restore original column name: timestamp
        COALESCE(
            v.platform,
            'unknown'
        ) AS `timestamp`,

        -- Missing timestamp -> average timestamp
        COALESCE(
            v.tweet_timestamp,
            m.avg_tweet_timestamp
        ) AS `timestamp.1`,

        -- Missing/invalid values -> mean
        CAST(
            ROUND(
                COALESCE(
                    v.likes,
                    m.avg_likes,
                    0
                )
            ) AS BIGINT
        ) AS likes,

        CAST(
            ROUND(
                COALESCE(
                    v.retweets,
                    m.avg_retweets,
                    0
                )
            ) AS BIGINT
        ) AS retweets,

        CAST(
            ROUND(
                COALESCE(
                    v.replies,
                    m.avg_replies,
                    0
                )
            ) AS BIGINT
        ) AS replies,

        CAST(
            ROUND(
                COALESCE(
                    v.impressions,
                    m.avg_impressions,
                    0
                )
            ) AS BIGINT
        ) AS impressions,

        CAST(
            ROUND(
                COALESCE(
                    v.engagement,
                    m.avg_engagement,
                    0
                )
            ) AS BIGINT
        ) AS engagement

    FROM validated v

    CROSS JOIN mean_values m

),

/* ============================================================
   STEP 5: REMOVE DUPLICATES
   ============================================================ */

deduplicated AS (

    SELECT

        tweet_id,

        user_id,

        tweet_text,

        `timestamp`,

        `timestamp.1`,

        likes,

        retweets,

        replies,

        impressions,

        engagement

    FROM (

        SELECT

            *,

            ROW_NUMBER() OVER (

                PARTITION BY tweet_id

                ORDER BY
                    `timestamp.1` DESC

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

    user_id,

    tweet_text,

    `timestamp`,

    `timestamp.1`,

    likes,

    retweets,

    replies,

    impressions,

    engagement

FROM deduplicated
