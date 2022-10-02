{% macro is_card_valid() %}
-- Source is taken from https://www.red-gate.com/simple-talk/blogs/the-luhn-algorithm-in-sql/

CASE
    WHEN creditcard LIKE '%[^0-9]%' THEN 0
    WHEN creditcard IS NULL THEN 0
    WHEN    (
            + 2 * CAST(SUBSTRING(creditcard, 1, 1) AS int8) / 10
            + 2 * CAST(SUBSTRING(creditcard, 1, 1) AS int8) % 10
            + CAST(SUBSTRING(creditcard, 2, 1) AS int8)
            + 2 * CAST(SUBSTRING(creditcard, 3, 1) AS int8) / 10
            + 2 * CAST(SUBSTRING(creditcard, 3, 1) AS int8) % 10
            + CAST(SUBSTRING(creditcard, 4, 1) AS int8)
            + 2 * CAST(SUBSTRING(creditcard, 5, 1) AS int8) / 10
            + 2 * CAST(SUBSTRING(creditcard, 5, 1) AS int8) % 10
            + CAST(SUBSTRING(creditcard, 6, 1) AS int8)
            + 2 * CAST(SUBSTRING(creditcard, 7, 1) AS int8) / 10
            + 2 * CAST(SUBSTRING(creditcard, 7, 1) AS int8) % 10
            + CAST(SUBSTRING(creditcard, 8, 1) AS int8)
            + 2 * CAST(SUBSTRING(creditcard, 9, 1) AS int8) / 10
            + 2 * CAST(SUBSTRING(creditcard, 9, 1) AS int8) % 10
            + CAST(SUBSTRING(creditcard, 10, 1) AS int8)
            + 2 * CAST(SUBSTRING(creditcard, 11, 1) AS int8) / 10
            + 2 * CAST(SUBSTRING(creditcard, 11, 1) AS int8) % 10
            + CAST(SUBSTRING(creditcard, 12, 1) AS int8)
            + 2 * CAST(SUBSTRING(creditcard, 13, 1) AS int8) / 10
            + 2 * CAST(SUBSTRING(creditcard, 13, 1) AS int8) % 10
            + CAST(SUBSTRING(creditcard, 14, 1) AS int8)
            + 2 * CAST(SUBSTRING(creditcard, 15, 1) AS int8) / 10
            + 2 * CAST(SUBSTRING(creditcard, 15, 1) AS int8) % 10
            + CAST(SUBSTRING(creditcard, 16, 1) AS int8)
    ) % 10 = 0 THEN 1
    ELSE 0
END

{% endmacro %}