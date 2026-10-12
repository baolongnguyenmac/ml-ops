{% snapshot dim_vendor %}

{{
    config(
        target_schema = 'public',
        unique_key    = 'vendor_id',
        strategy      = 'check',
        check_cols    = ['vendor_name'],
    )
}}

select distinct
    vendor_id,
    vendor_name
from {{ source('staging', 'dim_vendor') }}

{% endsnapshot %}