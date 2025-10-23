{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['trade_id']
) }}

with s as (
  select
    trade_id,
    user_id,
    token_id,
    side,
    safe_cast(price_usd_raw as float64) as price_usd,
    safe_cast(quantity_raw as float64) as quantity,
    case when status in ('FILLED','filled') then 'FILLED' else status end as status,
    trade_created_time_raw as trade_created_ts_utc,
    trade_updated_time_raw as trade_updated_ts_utc,
    datetime(trade_created_time_raw, 'Asia/Jakarta') as event_dt_jkt,
    datetime(trade_updated_time_raw, 'Asia/Jakarta') as updated_dt_jkt
  from {{ ref('stg_raw_trades') }}
)

select
  trade_id,
  user_id,
  token_id,
  side,
  price_usd,
  quantity,
  case when price_usd is not null and quantity is not null then price_usd * quantity end as trade_value_usd,
  status,
  trade_created_ts_utc as event_ts_utc,
  trade_updated_ts_utc,
  event_dt_jkt,
  updated_dt_jkt,
  current_timestamp() as loaded_at_utc
from s
where trade_id is not null
