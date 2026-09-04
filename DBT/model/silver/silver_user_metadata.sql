{{ config(
    materialized='table',
    schema='silver'
) }}

WITH source_data AS (

    SELECT *
    FROM {{ ref('stg_user_metadata') }}

),

/* ============================================================
   STEP 1: BASIC CLEANING AND TYPE CONVERSION
   ============================================================ */

cleaned AS (

    SELECT

        -- =====================================================
        -- USER ID
        -- Remove unnecessary decimal suffix such as 1001.0
        -- while keeping the column as STRING.
        -- =====================================================

        CASE
            WHEN TRIM(user_id) RLIKE '^[0-9]+[.]0+$'
            THEN REGEXP_REPLACE(TRIM(user_id), '[.]0+$', '')
            ELSE NULLIF(TRIM(user_id), '')
        END AS user_id,


        -- =====================================================
        -- COUNTRY
        -- =====================================================

        NULLIF(
            TRIM(country),
            ''
        ) AS country,


        -- =====================================================
        -- TOPIC CATEGORY
        -- =====================================================

        NULLIF(
            TRIM(topic_category),
            ''
        ) AS topic_category,


        -- =====================================================
        -- ACCOUNT CREATED DATE
        -- Handle common date and date-time formats.
        -- Invalid/unrecognized values become NULL.
        -- =====================================================

        COALESCE(

            TRY_TO_DATE(
                TRIM(account_created_date),
                'yyyy-MM-dd'
            ),

            TRY_TO_DATE(
                TRIM(account_created_date),
                'dd-MM-yyyy'
            ),

            TRY_TO_DATE(
                TRIM(account_created_date),
                'MM-dd-yyyy'
            ),

            TRY_TO_DATE(
                TRIM(account_created_date),
                'dd/MM/yyyy'
            ),

            TRY_TO_DATE(
                TRIM(account_created_date),
                'MM/dd/yyyy'
            ),

            TRY_TO_DATE(
                TRIM(account_created_date),
                'yyyy/MM/dd'
            ),

            TRY_TO_DATE(
                TRIM(account_created_date),
                'dd-MM-yyyy HH:mm:ss'
            ),

            TRY_TO_DATE(
                TRIM(account_created_date),
                'yyyy-MM-dd HH:mm:ss'
            ),

            TRY_TO_DATE(
                TRIM(account_created_date),
                'dd/MM/yyyy HH:mm:ss'
            ),

            TRY_TO_DATE(
                TRIM(account_created_date),
                'yyyy-MM-dd HH:mm:ss.SSS'
            )

        ) AS account_created_date,


        -- =====================================================
        -- FOLLOWERS COUNT
        -- =====================================================

        TRY_CAST(
            TRIM(followers_count) AS DOUBLE
        ) AS followers_count,


        -- =====================================================
        -- FOLLOWING COUNT
        -- =====================================================

        TRY_CAST(
            TRIM(following_count) AS DOUBLE
        ) AS following_count,


        -- =====================================================
        -- LIKES COUNT
        -- =====================================================

        TRY_CAST(
            TRIM(likes_count) AS DOUBLE
        ) AS likes_count,


        -- =====================================================
        -- SHARES COUNT
        -- =====================================================

        TRY_CAST(
            TRIM(shares_count) AS DOUBLE
        ) AS shares_count,


        -- =====================================================
        -- POSTS COUNT
        -- =====================================================

        TRY_CAST(
            TRIM(posts_count) AS DOUBLE
        ) AS posts_count,


        -- =====================================================
        -- VERIFIED
        -- =====================================================

        CASE
            WHEN LOWER(TRIM(verified)) IN ('true', '1', 'yes', 'y')
                THEN TRUE
            WHEN LOWER(TRIM(verified)) IN ('false', '0', 'no', 'n')
                THEN FALSE
            ELSE NULL
        END AS verified

    FROM source_data

),


/* ============================================================
   STEP 2: VALIDATION
   ============================================================ */

validated AS (

    SELECT

        user_id,

        country,

        topic_category,

        account_created_date,


        -- =====================================================
        -- FOLLOWERS COUNT
        -- Negative values become NULL.
        -- =====================================================

        CASE
            WHEN followers_count >= 0
            THEN followers_count
            ELSE NULL
        END AS followers_count,


        -- =====================================================
        -- FOLLOWING COUNT
        -- Negative values become NULL.
        -- =====================================================

        CASE
            WHEN following_count >= 0
            THEN following_count
            ELSE NULL
        END AS following_count,


        -- =====================================================
        -- LIKES COUNT
        -- Negative values become NULL.
        -- =====================================================

        CASE
            WHEN likes_count >= 0
            THEN likes_count
            ELSE NULL
        END AS likes_count,


        -- =====================================================
        -- SHARES COUNT
        -- Negative values become NULL.
        -- =====================================================

        CASE
            WHEN shares_count >= 0
            THEN shares_count
            ELSE NULL
        END AS shares_count,


        -- =====================================================
        -- POSTS COUNT
        -- Negative values become NULL.
        -- =====================================================

        CASE
            WHEN posts_count >= 0
            THEN posts_count
            ELSE NULL
        END AS posts_count,


        -- =====================================================
        -- VERIFIED
        -- =====================================================

        verified

    FROM cleaned

    WHERE user_id IS NOT NULL

),


/* ============================================================
   STEP 3: CALCULATE MEAN VALUES
   ============================================================ */

mean_values AS (

    SELECT

        AVG(followers_count) AS avg_followers_count,

        AVG(following_count) AS avg_following_count,

        AVG(likes_count) AS avg_likes_count,

        AVG(shares_count) AS avg_shares_count,

        AVG(posts_count) AS avg_posts_count

    FROM validated

),


/* ============================================================
   STEP 4: CALCULATE DEFAULT VALUES
   ============================================================ */

default_values AS (

    SELECT

        COALESCE(
            MAX(account_created_date),
            DATE '1900-01-01'
        ) AS default_account_created_date

    FROM validated

),


/* ============================================================
   STEP 5: HANDLE NULL / INVALID VALUES
   ============================================================ */

imputed AS (

    SELECT

        -- =====================================================
        -- USER ID
        -- =====================================================

        v.user_id,


        -- =====================================================
        -- COUNTRY
        -- NULL / blank -> Unknown
        -- =====================================================

        COALESCE(
            v.country,
            'Unknown'
        ) AS country,


        -- =====================================================
        -- TOPIC CATEGORY
        -- NULL / blank -> Unknown
        -- =====================================================

        COALESCE(
            v.topic_category,
            'Unknown'
        ) AS topic_category,


        -- =====================================================
        -- ACCOUNT CREATED DATE
        -- Valid dates are preserved.
        -- NULL / invalid dates -> fallback date.
        -- =====================================================

        COALESCE(
            v.account_created_date,
            d.default_account_created_date
        ) AS account_created_date,


        -- =====================================================
        -- FOLLOWERS COUNT
        -- NULL -> MEAN -> 0
        -- =====================================================

        CAST(
            ROUND(
                COALESCE(
                    v.followers_count,
                    m.avg_followers_count,
                    0
                )
            ) AS BIGINT
        ) AS followers_count,


        -- =====================================================
        -- FOLLOWING COUNT
        -- NULL -> MEAN -> 0
        -- =====================================================

        CAST(
            ROUND(
                COALESCE(
                    v.following_count,
                    m.avg_following_count,
                    0
                )
            ) AS BIGINT
        ) AS following_count,


        -- =====================================================
        -- LIKES COUNT
        -- NULL -> MEAN -> 0
        -- =====================================================

        CAST(
            ROUND(
                COALESCE(
                    v.likes_count,
                    m.avg_likes_count,
                    0
                )
            ) AS BIGINT
        ) AS likes_count,


        -- =====================================================
        -- SHARES COUNT
        -- NULL -> MEAN -> 0
        -- =====================================================

        CAST(
            ROUND(
                COALESCE(
                    v.shares_count,
                    m.avg_shares_count,
                    0
                )
            ) AS BIGINT
        ) AS shares_count,


        -- =====================================================
        -- POSTS COUNT
        -- NULL -> MEAN -> 0
        -- =====================================================

        CAST(
            ROUND(
                COALESCE(
                    v.posts_count,
                    m.avg_posts_count,
                    0
                )
            ) AS BIGINT
        ) AS posts_count,


        -- =====================================================
        -- VERIFIED
        -- NULL -> FALSE
        -- =====================================================

        COALESCE(
            v.verified,
            FALSE
        ) AS verified

    FROM validated v

    CROSS JOIN mean_values m

    CROSS JOIN default_values d

),


/* ============================================================
   STEP 6: DEDUPLICATION
   ============================================================ */

deduplicated AS (

    SELECT

        user_id,

        country,

        topic_category,

        account_created_date,

        followers_count,

        following_count,

        likes_count,

        shares_count,

        posts_count,

        verified

    FROM (

        SELECT

            *,

            ROW_NUMBER() OVER (

                PARTITION BY
                    user_id

                ORDER BY
                    account_created_date DESC

            ) AS row_num

        FROM imputed

    )

    WHERE row_num = 1

)


/* ============================================================
   STEP 7: FINAL SILVER TABLE
   ============================================================ */

SELECT

    user_id,

    country,

    topic_category,

    account_created_date,

    followers_count,

    following_count,

    likes_count,

    shares_count,

    posts_count,

    verified

FROM deduplicated
