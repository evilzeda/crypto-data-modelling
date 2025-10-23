{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key=['snapshot_date','check_name']
) }}

-- snapshot_date = date of observation
with snapshot_date as (select date(current_timestamp()) as snapshot_date),

-- dup trades
dup_trades as (
  select date(event_ts_utc) as snapshot_date, 'duplicate_trades' as check_name, count(*) as cnt
  from (
    select trade_id from {{ ref('fact_trades') }}
  )
  group by 1
  having count(*) > 1
),

-- failed trades count
failed_trades as (
  select date(event_ts_utc) as snapshot_date, 'failed_trades' as check_name, count(*) as cnt
  from {{ ref('fact_trades') }} where lower(status) != 'filled' or status is null
  group by 1
),

-- suspicious high value trades: above 99.9th percentile last 30 days
high_value as (
  with p as (
    select approx_quantiles(trade_value_usd, 1000)[offset(999)] as p999
    from {{ ref('fact_trades') }} where event_ts_utc >= timestamp_sub(current_timestamp(), interval 30 day)
  )
  select date(current_timestamp()) as snapshot_date, 'high_value_trades' as check_name,
         count(*) as cnt from {{ ref('fact_trades') }}
  where trade_value_usd > (select p999 from p)
),

-- large p2p transfers above 99.9pct
high_transfers as (
  with p as (
    select token_id, approx_quantiles(amount,1000)[offset(999)] as p999
    from {{ ref('fact_p2p') }} where event_ts_utc >= timestamp_sub(current_timestamp(), interval 30 day)
    group by token_id
  )
  select date(current_timestamp()) as snapshot_date, 'high_p2p_transfers' as check_name, count(*) as cnt
  from {{ ref('fact_p2p') }} p2 join p on p.token_id = p2.token_id
  where p2.amount > p.p999
),

unioned as (
  select * from dup_trades
  union all select * from failed_trades
  union all select * from high_value
  union all select * from high_transfers
)

select snapshot_date, check_name, sum(cnt) as cnt, current_timestamp() as loaded_at
from unioned
group by 1,2

{% if is_incremental() %}
where snapshot_date >= (select coalesce(max(snapshot_date), date('1970-01-01')) from {{ this }})
{% endif %}
