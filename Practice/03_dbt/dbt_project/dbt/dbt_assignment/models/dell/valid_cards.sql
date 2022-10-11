SELECT
    gen_random_uuid() :: text id,
    customerid,
    creditcard,
    {{ is_card_valid() }} AS valid
FROM
    {{ source(
        'dell_data',
        'customers'
    ) }}
