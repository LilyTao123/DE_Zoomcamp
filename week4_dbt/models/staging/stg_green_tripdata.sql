{{
    config(
        materialized='view'
        )
}}


with 

source as (

    select
        -- resolve duplicates
        row_number() over (partition by vendorid, lpep_pickup_datetime) as rn  
        ,* 
    from {{ source('staging', 'green_tripdata') }}
    where vendorid is not null 
),

renamed as (

    select
        {{dbt_utils.generate_surrogate_key(['vendorid', 'lpep_pickup_datetime'])}} as tripid,
        cast(vendorid as integer) as vendorid,
        cast(lpep_pickup_datetime as timestamp) as lpep_pickup_datetime,
        cast(lpep_dropoff_datetime as timestamp) as lpep_dropoff_datetime,
        store_and_fwd_flag,
        cast(ratecodeid as integer) as ratecodeid,
        passenger_count,
        trip_distance,
        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        airport_fee,
        total_amount,
        cast(split(payment_type, '.')[0] as integer) as payment_type,
        {{get_payment_type_description('payment_type')}} as payment_type_descripted,
        distance_between_service,
        time_between_service,
        trip_type,
        improvement_surcharge,
        cast(pulocationid as integer) as pickup_locationid,
        cast(dolocationid as integer) as dropoff_locationid,
        data_file_year,
        data_file_month

    from source
    where rn = 1

)

select * from renamed

-- dbt build --select stg_green_tripdata --vars '{'is_test_run': 'true'}'
{% if var('is_test_run', default=true) %}
limit 100 
{% endif %}