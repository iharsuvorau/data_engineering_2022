SELECT
    orders.orderid,
    orders.customerid,
    orders.netamount,
    orders.orderdate,
    country
FROM
    {{ source(
        'dell_data',
        'orders'
    ) }} AS orders
    JOIN (
        SELECT
            *
        FROM
            {{ ref('valid_cards_snapshot') }}
        WHERE
            (
                customerid,
                dbt_valid_from
            ) IN (
                SELECT
                    customerid,
                    MAX(dbt_valid_from)
                FROM
                    {{ ref('valid_cards_snapshot') }}
                GROUP BY
                    customerid
            )
    ) valid_cards
    ON orders.customerid = valid_cards.customerid
    JOIN customers
    ON valid_cards.customerid = customers.customerid
WHERE
    valid_cards.valid = 0
