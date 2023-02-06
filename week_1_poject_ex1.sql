--
-- Week 1 Homework project
-- Exercise 1
-- clean up cities if mutilple city/st combination selct 1 unique per partition to remove duplicates
with cs_cities as (
    select 
           lower(trim(city_name)) as city_supported
           , trim(state_abbr) as state
           , lat as latitude
           , long as longitude
           , row_number() over(partition by city_supported, state order by city_supported asc) as row_num
    from vk_data.resources.us_cities
    )
-- add customer information and match cities from customer to cities_supported to see eligible customers
, customers as (
    select 
           ccd.customer_id, ccd.first_name, ccd.last_name, ccd.email
           , cca.customer_city, cca.customer_state
           , cs_cities.latitude as customer_latitude
           , cs_cities.longitude as customer_longitude
    from vk_data.customers.customer_data as ccd 
    inner join vk_data.customers.customer_address as cca 
          using(customer_id)
    inner join cs_cities 
          on (
              lower(trim(cca.customer_city)) = lower(trim(cs_cities.city_supported))
          and cca.customer_state = cs_cities.state
    )
)
-- add supplier information and match up cities/states 
, suppliers as (
    select 
           ssi.supplier_id, ssi.supplier_name
           , lower(trim(ssi.supplier_city)) as supplier_city
           , ssi.supplier_state
           , rusc.lat as supplier_latitude
           , rusc.long as supplier_longitude
    from vk_data.suppliers.supplier_info as ssi
    left join vk_data.resources.us_cities as rusc 
           on lower(trim(ssi.supplier_city)) = lower(trim(rusc.city_name))
           and ssi.supplier_state = rusc.state_abbr
)
-- display of columns for final result - calculation of min distance 
, last_steps as (
    select 
           customer_id 
           , first_name as customer_first_name
           , last_name as customer_last_name
           , email as customer_email
           , supplier_id, supplier_name
           , st_distance(st_makepoint(customers.customer_longitude, customers.customer_latitude) 
                         , st_makepoint(suppliers.supplier_longitude, suppliers.supplier_latitude)) / 1609 as distance_in_miles
                         , row_number() over(partition by customer_id order by distance_in_miles) as top_row
    from customers
    cross join suppliers  -- join all customers with all 10 suppliers
    order by customer_last_name, customer_first_name                                     
)
-- final result of columns and selecting top/ closet supplier
select customer_id, customer_first_name, customer_last_name, customer_email
       , supplier_id, supplier_name, distance_in_miles 
from last_steps
where top_row = 1

