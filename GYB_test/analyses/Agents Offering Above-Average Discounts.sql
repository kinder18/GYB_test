with avg_discount as (
    select
        avg(discount_amount) as overall_avg_discount
    from {{ ref('fct_sales') }}
)

select
    agent_name,
    avg_discount,
    total_revenue
from (
    select
        unnest(string_to_array(sales_agents, ', ')) as agent_name,
        avg(discount_amount) as avg_discount,
        sum(total_revenue) as total_revenue
    from {{ ref('fct_sales') }}
    group by agent_name
) agent_performance
join avg_discount
on agent_performance.avg_discount > avg_discount.overall_avg_discount
order by agent_performance.avg_discount desc;
