SELECT
    orders.orderid,
    orders.customerid,
    orders.netamount
FROM
    {{ source(
        'dell_data',
        'orders'
    ) }} AS orders
    JOIN {{ ref('valid_cards') }}
    valid_cards
    ON orders.customerid = valid_cards.customerid
WHERE
    valid_cards.valid = 1
