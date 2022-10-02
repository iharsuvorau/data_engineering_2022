{% snapshot citypop_snapshot %}

{{
    config(
        target_database='world',
        target_schema='public',
        unique_key='id',
        strategy='check',
        check_cols=['perc_of_country_pop', 'city_rank_in_country']
    )
}}

SELECT * FROM {{ source('world_data','citypopulation') }}

{% endsnapshot %}