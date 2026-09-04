{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT

    verified,

    COUNT(DISTINCT user_id) AS total_users,

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
        AVG(following_count),
        2
    ) AS avg_following,

    ROUND(
        AVG(likes_count),
        2
    ) AS avg_likes,

    ROUND(
        AVG(shares_count),
        2
    ) AS avg_shares,

    ROUND(
        AVG(posts_count),
        2
    ) AS avg_posts

FROM {{ ref('silver_user_metadata') }}

GROUP BY verified
