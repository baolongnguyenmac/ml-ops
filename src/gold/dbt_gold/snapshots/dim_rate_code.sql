{% snapshot dim_rate_code %}

{{
    config(
        target_schema = 'public',
        unique_key    = 'rate_code_id',
        strategy      = 'check',
        check_cols    = ['rate_code'],
    )
}}

select distinct
    rate_code_id,
    rate_code
from {{ source('staging', 'dim_rate_code') }}

{% endsnapshot %}