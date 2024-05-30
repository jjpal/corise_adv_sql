/*
Part 1
Develop a report to analyze automobile customers who have placed urgent orders. 
The output should be sorted by last_order_date descending limit 100 rows. */ 

/* Filter the data */
with priority_orders as (
    select
        customer.c_custkey
        , orders.o_orderkey
        , orders.o_totalprice
        , orders.o_orderdate
        , row_number() over (partition by customer.c_custkey order by orders.o_totalprice desc) as order_price_rank
    from snowflake_sample_data.tpch_sf1.orders as orders
    left join snowflake_sample_data.tpch_sf1.customer as customer on orders.o_custkey = customer.c_custkey
    where customer.c_mktsegment = 'AUTOMOBILE'
    and orders.o_orderpriority = '1-URGENT'
)

, top_orders as (
    select
    	c_custkey
        , max(o_orderdate) as last_order_date
        , listagg(o_orderkey, ', ') as order_numbers
        , sum(o_totalprice) as total_spent
    from priority_orders
    where order_price_rank <= 3
    group by 1
)

, orders_info_totals as (
    select
    	priority_orders.c_custkey
        , lineitem.l_partkey
        , sum(lineitem.l_quantity) as ordered_quantity
        , sum(lineitem.l_extendedprice) as part_total_spent
        , row_number() over (partition by priority_orders.c_custkey order by part_total_spent desc) as part_rank
    from snowflake_sample_data.tpch_sf1.lineitem as lineitem
    inner join priority_orders on priority_orders.o_orderkey = lineitem.l_orderkey
    group by 1, 2
    qualify row_number() over (partition by priority_orders.c_custkey order by part_total_spent desc) <= 3
)

, prep_all_parts as (
    select
       t_ord.c_custkey
       , t_ord.last_order_date
       , t_ord.order_numbers
       , t_ord.total_spent
       , ord_it.l_partkey
       , ord_it.ordered_quantity
       , ord_it.part_total_spent
       , ord_it.part_rank       
    from top_orders as t_ord
    left join orders_info_totals as ord_it on ord_it.c_custkey = t_ord.c_custkey
)

select 
    c_custkey
    , last_order_date
    , order_numbers
    , total_spent
    , max(case when part_rank = 1 then l_partkey end) as part_1_key
    , max(case when part_rank = 1 then ordered_quantity end) as part_1_quantity
    , max(case when part_rank = 1 then part_total_spent end) as part_1_total_spent
    , max(case when part_rank = 2 then l_partkey end) as part_2_key
    , max(case when part_rank = 2 then ordered_quantity end) as part_2_quantity
    , max(case when part_rank = 2 then part_total_spent end) as part_2_total_spent
    , max(case when part_rank = 3 then l_partkey end) as part_3_key
    , max(case when part_rank = 3 then ordered_quantity end) as part_3_quantity
    , max(case when part_rank = 3 then part_total_spent end) as part_3_total_spent
from prep_all_parts
group by 1, 2, 3, 4
order by last_order_date desc, total_spent desc
limit 100

/* Part 2
Review the candidate's tech exercise below, and provide a one-paragraph assessment of the SQL quality. 
Provide examples/suggestions for improvement if you think the candidate could have chosen a better approach.

Do you agree with the results returned by the query? 
   yes
   
Is it easy to understand? 
   It is understandable, but it might take some time to debug. Having CTEs to break down the make it easier to debug

Could the code be more efficient?
   Having less joins, more CTEs, using qualify to remove duplicates, and using order by until the final query.
*/
