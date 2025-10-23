{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['user_id', 'period_type', 'period_id']
) }}

with base as (
    select
        user_id,
        status,
        trade_value_usd,
        trade_id,
        event_ts_utc
    from {{ ref('fact_trades') }}
    where event_ts_utc is not null
)

-- Weekly Summary
select
    user_id,
    'weekly' as period_type,
    format_date('%G-%V', date(event_ts_utc)) as period_id, -- e.g. 2025-42
    count(distinct trade_id) as total_trades,
    sum(trade_value_usd) as total_value_usd,
    avg(trade_value_usd) as avg_trade_value_usd,
    countif(status = 'SUCCESS') as successful_trades
from base
group by 1, 2, 3
