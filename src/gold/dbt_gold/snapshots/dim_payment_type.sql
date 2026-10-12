{% snapshot dim_payment_type %}

{{
    config(
        target_schema = 'public',
        unique_key    = 'payment_type_id',
        strategy      = 'check',
        check_cols    = ['payment_type'],
    )
}}

select distinct
    payment_type_id,
    payment_type
from {{ source('staging', 'dim_payment_type') }}

{% endsnapshot %}