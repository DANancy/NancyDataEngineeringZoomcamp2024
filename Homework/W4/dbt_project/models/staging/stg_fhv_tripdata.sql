{{
    config(
        materialized='view'
    )
}}

with tripdata as (
    select *
     from {{ source('staging', 'fhv_tripdata') }}
)

select
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        sr_flag,
        affiliated_base_number
    from tripdata

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}