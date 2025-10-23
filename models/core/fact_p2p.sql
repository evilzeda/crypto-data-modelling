{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['transfer_id']
) }}

select
  transfer_id,
  sender_id,
  receiver_id,
  token_id,
  safe_cast(amount_raw as float64) as amount,
  case when status in ('SUCCESS','success') then 'SUCCESS' else status end as status,
  transfer_created_time_raw as event_ts_utc,
  transfer_updated_time_raw as updated_at_utc,
  datetime(transfer_created_time_raw, 'Asia/Jakarta') as event_dt_jkt,
  datetime(transfer_updated_time_raw, 'Asia/Jakarta') as updated_dt_jkt,
  current_timestamp() as loaded_at_utc
from {{ ref('stg_raw_p2p') }}
where transfer_id is not null