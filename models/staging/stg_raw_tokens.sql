{{ config(materialized='view') }}

select
  token_id,
  token_name,
  category
from `{{ var('project') }}.crypto_dataset.raw_tokens`