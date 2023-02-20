
--select * from vk_data.events.website_activity

/* We want to create a report by day to track:
[1] total unique sessions 
[2] the average length of sessions in seconds 
[3] the average number of searches completed before displaying a recipe
[4] the ID of the recipe that was most viewed */

/* extract information for events for recipe and search_tag, event_id, event_timestamp and session_id
 remove duplicate by using row_number window function and qualify to get unique sessions */
with event_sessions as (
    select
        event_id
        , session_id
        , event_timestamp
        , trim(parse_json(event_details):"recipe_id", '"') as recipe_id
        , trim(parse_json(event_details):"event", '"') as event_type
 
    from vk_data.events.website_activity
    qualify row_number() over(partition by event_id, recipe_id order by session_id) = 1
)
/* extract seconds from timestamp, get min and max to calculate session_length
 ratio of searches per session */
, grouped_session_time as (
    select
        session_id
        , date_trunc('second', max(event_timestamp)) as session_end
        , date_trunc('second', min(event_timestamp)) as session_begin
        , timediff(seconds, session_begin, session_end) as session_length
        , iff(count_if(event_type = 'view_recipe') = 0 		                          -- condition
             , null		                                                              -- expr1
             , round(count_if(event_type = 'search') / count_if(event_type = 'view_recipe')) -- expr2
             ) as searches_per_recipe_view
    from event_sessions
    group by session_id
)
/* event_date, recipe_id, and count of total_views searched for recipe_id's
 filtering out null values */
, most_viewed_recipe as (
    select 
        date(event_timestamp) as event_date
        , recipe_id
        , count(*) as total_recipe_views
    from event_sessions
    where recipe_id is not null
    group by event_date, recipe_id
    qualify row_number() over (partition by event_date order by total_recipe_views desc) = 1
)

, final_output as (
    select
        date(session_begin) as event_day
        , count(session_id) as total_sessions
        , round(avg(session_length)) as avg_session_length_sec
        , max(searches_per_recipe_view) as avg_searches_per_recipe_view
        , max(recipe_name) as top_recipe
    from grouped_session_time
    inner join most_viewed_recipe on date(grouped_session_time.session_begin) = most_viewed_recipe.event_date
    inner join vk_data.chefs.recipe using (recipe_id)
    group by 1 
)

select * from final_output
order by 2




