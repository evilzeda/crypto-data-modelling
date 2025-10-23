{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key=['cohort_date','cohort_type','region']
) }}

with first_activity as (
  select user_id,
         min(case when activity='p2p' then event_ts end) as first_p2p_ts,
         min(case when activity='trade' then event_ts end) as first_trade_ts
  from (
    select sender_id as user_id, event_ts_utc as event_ts, 'p2p' as activity from {{ ref('fact_p2p') }}
    union all
    select user_id, event_ts_utc as event_ts, 'trade' as activity from {{ ref('fact_trades') }}
  )
  group by user_id
),

cohort as (
  select
    u.user_id,
    case when f.first_p2p_ts is not null and (f.first_trade_ts is null or f.first_p2p_ts < f.first_trade_ts) then 'p2p_first'
         when f.first_trade_ts is not null then 'trade_first'
         else 'unknown' end as cohort_type,
    date(coalesce(f.first_p2p_ts, f.first_trade_ts)) as cohort_date,
    s.region
  from first_activity f
  join {{ ref('stg_raw_users') }} s on s.user_id = f.user_id
),

funnel as (
  select
    c.cohort_date,
    c.cohort_type,
    c.region,
    count(distinct c.user_id) as cohort_size,
    -- users who did p2p in 7 days after cohort (if cohort is trade_first, this counts later p2p)
    sum(case when exists (
      select 1 from {{ ref('fact_p2p') }} p 
      where p.sender_id = c.user_id 
        and p.event_ts_utc between timestamp(c.cohort_date) and timestamp_add(timestamp(c.cohort_date), INTERVAL 7 DAY)
    ) then 1 else 0 end) as did_p2p_7d,
    -- users who did trade within 30 days after cohort_date
    sum(case when exists (
      select 1 from {{ ref('fact_trades') }} t
      where t.user_id = c.user_id
        and t.event_ts_utc between timestamp(c.cohort_date) and timestamp_add(timestamp(c.cohort_date), INTERVAL 30 DAY)
    ) then 1 else 0 end) as did_trade_30d,
    -- average time to convert to trade (in days)
    avg(case when first_trade_ts is not null then date_diff(date(first_trade_ts), c.cohort_date, day) else null end) as avg_days_to_trade
  from (
    select c.*, 
      (select min(event_ts_utc) from {{ ref('fact_trades') }} t where t.user_id = c.user_id) as first_trade_ts
    from cohort c
  ) c
  group by 1,2,3
)

select
  cohort_date, cohort_type, region, cohort_size, did_p2p_7d, did_trade_30d, avg_days_to_trade, current_timestamp() as loaded_at_utc
from funnel

{% if is_incremental() %}
where cohort_date >= (select coalesce(max(cohort_date), date('1970-01-01')) from {{ this }})
{% endif %}
