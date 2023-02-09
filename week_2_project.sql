/*  Week 2 Project Assignment (refactor query) 
    Mostly followed the Brooklyn Data Company style guide
    moved the subquery and two joins into separate CTEs and replaced ilike with ilike any */

/* count customer food preferences */
with 
   fd_pref_c as (
     select 
      customer_id
      , count(*) as food_pref_count
     from vk_data.customers.customer_survey
     where is_active = true
     group by 1
   )

   /* select information from supported city Chicago Illinois */
   , chic_loc as (
   select 
       geo_location
   from vk_data.resources.us_cities 
   where city_name = 'CHICAGO' and state_abbr = 'IL' 
   )    

   /* select information from supported city Gary Indiana */
   , gary_loc as (
   select 
       geo_location
   from vk_data.resources.us_cities 
   where city_name = 'GARY' and state_abbr = 'IN'
)    

select 
    first_name || ' ' || last_name as customer_name
    , ca.customer_city
    , ca.customer_state
    , fd_pref_c.food_pref_count
    , (st_distance(us.geo_location, chic_loc.geo_location) / 1609)::int as chicago_distance_miles
    , (st_distance(us.geo_location, gary_loc.geo_location) / 1609)::int as gary_distance_miles
from vk_data.customers.customer_address as ca
inner join vk_data.customers.customer_data cd on ca.customer_id = cd.customer_id
left join vk_data.resources.us_cities us on upper(trim(ca.customer_state)) = upper(trim(us.state_abbr))
     and trim(lower(ca.customer_city)) = trim(lower(us.city_name))
inner join fd_pref_c on cd.customer_id = fd_pref_c.customer_id
cross join chic_loc
cross join gary_loc
where 
   trim(city_name) ilike any ('%concord%', '%georgetown%', '%ashland%') and customer_state = 'KY'
   or trim(city_name) ilike any ('%oakland%', '%pleasant hill%') and customer_state = 'CA'
   or trim(city_name) ilike any ('%arlington%', '%brownsville%') and customer_state = 'TX'

