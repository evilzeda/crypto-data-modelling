{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date', 'token_id']
) }}

with vols as (
  select
    date(event_ts_utc) as date,
    token_id,
    sum(trade_value_usd) as total_volume_usd
  from {{ ref('fact_trades') }}
  where trade_value_usd is not null
  group by 1, 2
),

market as (
  select date, sum(total_volume_usd) as market_volume_usd
  from vols
  group by 1
),

shares as (
  select
    v.date,
    v.token_id,
    v.total_volume_usd,
    v.total_volume_usd / nullif(m.market_volume_usd, 0) as market_share
  from vols v
  join market m using (date)
)

select
  date,
  token_id,
  total_volume_usd,
  market_share,
  current_timestamp() as loaded_at_utc
from shares
