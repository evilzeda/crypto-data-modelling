{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key=['cohort_date','cohort_type','period','region','token_category','period_offset']
) }}

-- Definitions:
-- cohort_date: date of user's first activity (p2p or trade)
-- cohort_type: 'p2p_first' or 'trade_first'
-- period_offset: number of days since cohort_date (0 = same day, 1 = next day, ...)

with first_activity as (
  -- find first activity (earliest event_ts) for each user and product (p2p/trade)
  select user_id,
         min(case when activity = 'p2p' then event_ts end) as first_p2p_ts,
         min(case when activity = 'trade' then event_ts end) as first_trade_ts
  from (
    select sender_id as user_id, event_ts_utc as event_ts, 'p2p' as activity from {{ ref('fact_p2p') }}
    union all
    select user_id, event_ts_utc as event_ts, 'trade' as activity from {{ ref('fact_trades') }}
  )
  group by user_id
),

cohorts as (
  select
    u.user_id,
    case
      when f.first_p2p_ts is not null and (f.first_trade_ts is null or f.first_p2p_ts < f.first_trade_ts) then 'p2p_first'
      when f.first_trade_ts is not null and (f.first_p2p_ts is null or f.first_trade_ts <= f.first_p2p_ts) then 'trade_first'
      else 'unknown'
    end as cohort_type,
    date(coalesce(
      case when f.first_p2p_ts is not null and (f.first_trade_ts is null or f.first_p2p_ts < f.first_trade_ts) then f.first_p2p_ts end,
      case when f.first_trade_ts is not null then f.first_trade_ts end
    )) as cohort_date,
    u.region
  from {{ ref('stg_raw_users') }} u
  left join first_activity f using(user_id)
  where (f.first_p2p_ts is not null or f.first_trade_ts is not null)
),

events as (
  -- user events with token category (for product-level retention)
  select
    t.user_id,
    t.event_ts_utc,
    date(t.event_ts_utc) as event_date,
    coalesce(tok.category, 'unknown') as token_category,
    'trade' as activity
  from {{ ref('fact_trades') }} t
  left join {{ ref('stg_raw_tokens') }} tok on tok.token_id = t.token_id

  union all

  select
    p.sender_id as user_id,
    p.event_ts_utc,
    date(p.event_ts_utc) as event_date,
    coalesce(tok.category, 'unknown') as token_category,
    'p2p' as activity
  from {{ ref('fact_p2p') }} p
  left join {{ ref('stg_raw_tokens') }} tok on tok.token_id = p.token_id
),

cohort_events as (
  select
    c.user_id,
    c.cohort_type,
    c.cohort_date,
    c.region,
    e.token_category,
    date_diff(e.event_date, c.cohort_date, day) as period_offset,
    e.activity
  from cohorts c
  join events e on e.user_id = c.user_id
  where date_diff(e.event_date, c.cohort_date, day) >= 0 -- only after cohort
),

aggregated as (
  select
    cohort_date,
    cohort_type,
    region,
    token_category,
    period_offset,
    count(distinct user_id) as users_active_in_period
  from cohort_events
  group by 1,2,3,4,5
)

select
  cohort_date,
  cohort_type,
  region,
  token_category,
  period_offset,
  users_active_in_period,
  current_timestamp() as loaded_at
from aggregated
{% if is_incremental() %}
where cohort_date >= (
  select coalesce(max(cohort_date), date('1970-01-01')) from {{ this }}
)
{% endif %}
