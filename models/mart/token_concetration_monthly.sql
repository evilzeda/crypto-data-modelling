{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['month_id', 'token_id']
) }}

with vols as (
  select
    format_date('%Y-%m', date(event_ts_utc)) as month_id,
    token_id,
    sum(trade_value_usd) as total_volume_usd
  from {{ ref('fact_trades') }}
  where trade_value_usd is not null
  group by 1, 2
),
market as (
  select month_id, sum(total_volume_usd) as market_volume_usd
  from vols
  group by 1
),
shares as (
  select
    v.month_id,
    v.token_id,
    v.total_volume_usd,
    v.total_volume_usd / nullif(m.market_volume_usd, 0) as market_share
  from vols v
  join market m using (month_id)
)
select
  month_id,
  token_id,
  total_volume_usd,
  market_share,
  current_timestamp() as loaded_at_utc
from shares
