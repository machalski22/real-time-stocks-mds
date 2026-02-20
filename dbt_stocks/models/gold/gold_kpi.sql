{{ config(materialized='table') }}

select
    symbol,
    current_price,
    change_amount,
    change_percent
from {{ ref('silver_cleaned_stock_quotes') }}
qualify (row_number() over (partition by symbol order by fetched_at desc)) = 1
