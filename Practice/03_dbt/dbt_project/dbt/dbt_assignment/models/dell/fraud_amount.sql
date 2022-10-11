SELECT
    SUM(netamount)
FROM
    {{ ref('fraud_orders') }}
