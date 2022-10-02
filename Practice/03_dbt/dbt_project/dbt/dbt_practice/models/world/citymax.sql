SELECT country_name, city_name
FROM {{ ref('citypopulation' )}}
WHERE city_rank_in_country = 1