with agent_performance as (
    select
        unnest(string_to_array(sales_agents, ', ')) as agent_name,
        count(reference_id) as sales_count,
        avg(total_revenue) as avg_revenue,
        avg(discount_amount) as avg_discount,
        sum(total_revenue) as total_revenue
    from {{ ref('fct_sales') }}
    group by agent_name
)

select
    agent_name,
    sales_count,
    avg_revenue,
    avg_discount,
    total_revenue,
    rank() over (order by total_revenue desc) as rank
from agent_performance
order by total_revenue desc;
