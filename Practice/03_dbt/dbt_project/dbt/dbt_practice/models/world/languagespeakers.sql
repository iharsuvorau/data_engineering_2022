{{config(materialized='table')}}

SELECT cl.language, ROUND(SUM(co.population * cl.percentage / 100)::decimal, 0) as amount
FROM (SELECT code, population
      FROM {{ source('world_data', 'country') }}) co
         JOIN (SELECT countrycode, language, percentage
               FROM {{ source ('world_data', 'countrylanguage') }}) cl
              ON co.code = cl.countrycode
GROUP BY cl.language
ORDER BY amount desc