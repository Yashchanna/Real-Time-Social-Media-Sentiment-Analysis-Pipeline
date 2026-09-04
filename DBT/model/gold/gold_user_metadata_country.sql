{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT

    country,

    COUNT(DISTINCT user_id) AS total_users,

    COUNT(
        DISTINCT CASE
            WHEN verified = TRUE THEN user_id
        END
    ) AS verified_users,

    COUNT(
        DISTINCT CASE
            WHEN verified = FALSE THEN user_id
        END
    ) AS unverified_users,

    SUM(followers_count) AS total_followers,

    SUM(following_count) AS total_following,

    SUM(likes_count) AS total_likes,

    SUM(shares_count) AS total_shares,

    SUM(posts_count) AS total_posts,

    ROUND(
        AVG(followers_count),
        2
    ) AS avg_followers,

    ROUND(
        AVG(posts_count),
        2
    ) AS avg_posts,

    ROUND(
        AVG(likes_count),
        2
    ) AS avg_likes,

    ROUND(
        AVG(shares_count),
        2
    ) AS avg_shares,

    ROUND(
        COUNT(
            DISTINCT CASE
                WHEN verified = TRUE THEN user_id
            END
        ) * 100.0
        / NULLIF(COUNT(DISTINCT user_id), 0),
        2
    ) AS verification_rate_pct

FROM {{ ref('silver_user_metadata') }}

GROUP BY country
