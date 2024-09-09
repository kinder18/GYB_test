with monthly_revenue as (
    select
        date_trunc('month', order_date_kyiv) as month,
        sum(total_revenue) as total_revenue
    from {{ ref('fct_sales') }}
    group by month
)

select
    month,
    total_revenue,
    (total_revenue - lag(total_revenue) over (order by month)) / lag(total_revenue) over (order by month) * 100 as growth_percentage
from monthly_revenue
order by month;
