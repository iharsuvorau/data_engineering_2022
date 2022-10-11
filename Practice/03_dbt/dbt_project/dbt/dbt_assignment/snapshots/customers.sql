{% snapshot valid_cards_snapshot %}
    {{ config(
        target_database = 'dellstore',
        target_schema = 'public',
        unique_key = 'id',
        strategy = 'check',
        check_cols = ['valid']
    ) }}

    SELECT
        *
    FROM
        {{ ref('valid_cards') }}
{% endsnapshot %}
