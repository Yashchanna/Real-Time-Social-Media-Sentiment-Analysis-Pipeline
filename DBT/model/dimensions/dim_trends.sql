{{ config(
    materialized='table'
) }}

WITH date_data AS (

    SELECT DISTINCT

        tweet_date

    FROM {{ ref('stg_tweets') }}

    WHERE tweet_date IS NOT NULL

),

final AS (

SELECT

    CAST(date_format(tweet_date,'yyyyMMdd') AS INT) AS date_key,

    tweet_date,

    year(tweet_date) AS year,

    quarter(tweet_date) AS quarter,

    month(tweet_date) AS month,

    monthname(tweet_date) AS month_name,

    weekofyear(tweet_date) AS week_number,

    day(tweet_date) AS day,

    dayofweek(tweet_date) AS day_number,

    date_format(tweet_date,'EEEE') AS day_name

FROM date_data

)

SELECT *
FROM final
