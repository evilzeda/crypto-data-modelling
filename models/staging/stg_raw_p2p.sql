{{ config(materialized='view') }}

select
  transfer_id,
  sender_id,
  receiver_id,
  token_id,
  amount as amount_raw,
  status as status,
  transfer_created_time as transfer_created_time_raw,
  transfer_updated_time as transfer_updated_time_raw
from `{{ var('project') }}.crypto_dataset.raw_p2p_transfers`