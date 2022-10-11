{% snapshot orders_snapshot %}
    {{ config(
        target_database = 'dellstore',
        target_schema = 'public',
        unique_key = 'orderid',
        strategy = 'check',
        check_cols = ['orderid', 'customerid', 'netamount']
    ) }}

    SELECT
        *
    FROM
        {{ source(
            'dell_data',
            'orders'
        ) }}
{% endsnapshot %}
