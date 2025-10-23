{{ config(materialized='view') }}

-- All raw fields are STRING; parse and validate here.
select
  trade_id,
  user_id,
  token_id,
  upper(trim(side)) as side,
  price_usd as price_usd_raw,
  quantity as quantity_raw,
  upper(trim(status)) as status,
  trade_created_time as trade_created_time_raw,
  trade_updated_time as trade_updated_time_raw
from `{{ var('project') }}.crypto_dataset.raw_trades`
