SELECT SUM(netamount)
FROM {{ ref('valid_orders') }}