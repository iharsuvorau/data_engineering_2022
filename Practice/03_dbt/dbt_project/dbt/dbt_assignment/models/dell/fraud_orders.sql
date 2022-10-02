SELECT orders.orderid,
       orders.customerid,
       orders.netamount
FROM {{ source('dell_data', 'orders') }} AS orders
JOIN
    (
        SELECT *
        FROM {{ ref('valid_cards_snapshot') }}
        WHERE (customerid, dbt_valid_from) in (
            SELECT customerid, max(dbt_valid_from)
            FROM {{ ref('valid_cards_snapshot') }}
            GROUP BY customerid
        )
    ) valid_cards
ON orders.customerid = valid_cards.customerid
WHERE valid_cards.valid = 0