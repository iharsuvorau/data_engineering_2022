SELECT
    country,
    DATE_TRUNC(
        'year',
        orderdate
    ) orderdate,
    SUM(netamount) AS total_fraud
FROM
    {{ ref('fraud_orders') }}
GROUP BY
    country,
    DATE_TRUNC(
        'year',
        orderdate
    )
ORDER BY
    total_fraud DESC
