{{ config(materialized='table') }}

WITH source AS (
    SELECT
        symbol,
        market_timestamp,
        TRY_CAST(current_price AS DOUBLE) AS current_price_dbl
    FROM
        {{ ref('silver_cleaned_stock_quotes') }}
    WHERE
        TRY_CAST(current_price AS DOUBLE) IS NOT NULL
),
latest_prices AS (
    SELECT
        symbol,
        AVG(current_price_dbl) AS avg_price
    FROM
        source
    GROUP BY
        symbol,
        market_timestamp QUALIFY (
            ROW_NUMBER() OVER (
                PARTITION BY symbol
                ORDER BY
                    market_timestamp DESC
            )
        ) = 1
),
all_time_volatility AS (
    SELECT
        symbol,
        STDDEV_POP(current_price_dbl) AS volatility,
        CASE
            WHEN AVG(current_price_dbl) = 0 THEN NULL
            ELSE STDDEV_POP(current_price_dbl) / NULLIF(AVG(current_price_dbl), 0)
        END AS relative_volatility
    FROM
        source
    GROUP BY
        symbol
),
final AS (
    SELECT
        lp.symbol,
        lp.avg_price,
        v.volatility,
        v.relative_volatility
    FROM
        latest_prices AS lp
        INNER JOIN all_time_volatility AS v ON lp.symbol = v.symbol
    ORDER BY
        lp.symbol
)
SELECT
    *
FROM
    final