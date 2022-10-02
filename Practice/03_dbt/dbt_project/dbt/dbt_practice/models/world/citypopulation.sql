SELECT CONCAT(country_name, '|', city_name)                           as id
     , country_name
     , city_name
     , ROUND(city_pop / country_pop::decimal, 3) * 100                as perc_of_country_pop
     , RANK() OVER (PARTITION BY country_name ORDER BY city_pop DESC) as city_rank_in_country
FROM (SELECT name       as city_name
           , countrycode
           , population as city_pop
      FROM {{ source('world_data', 'city') }}) ci
         JOIN
     (SELECT name       as country_name
           , code
           , population as country_pop
      FROM {{ source('world_data', 'country')}}) co
     ON ci.countrycode = co.code