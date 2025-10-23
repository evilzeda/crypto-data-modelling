{{ config(materialized='view') }}

select
  user_id,
  region,
  signup_date as signup_date_raw
from `{{ var('project') }}.crypto_dataset.raw_users`