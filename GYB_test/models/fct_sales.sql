with base_data as (
    select
        coalesce("Reference ID", 'N/A') as reference_id,
        coalesce("Product Name", 'N/A') as product_name,
        coalesce("Country", 'N/A') as country,
        coalesce("Campaign Name", 'N/A') as campaign_name,
        coalesce("Source", 'N/A') as source,
        coalesce("Total Rebill Amount", 0) + coalesce("Total Amount ($)", 0) - coalesce("Returned Amount ($)", 0) as total_revenue,
        coalesce("Total Rebill Amount", 0) as rebill_revenue,
        coalesce("Number Of Rebills", 0) as rebill_count,
        coalesce("Discount Amount ($)", 0) as discount_amount,
        coalesce("Returned Amount ($)", 0) as returned_amount,
        coalesce("Return Date Kyiv", 'N/A') as return_date_kyiv,
        coalesce(cast("Return Date Kyiv" as timestamp) at time zone 'UTC', 'N/A') as return_date_utc,
        coalesce(cast("Return Date Kyiv" as timestamp) at time zone 'America/New_York', 'N/A') as return_date_new_york,
        coalesce("Order Date Kyiv", 'N/A') as order_date_kyiv,
        coalesce(cast("Order Date Kyiv" as timestamp) at time zone 'UTC', 'N/A') as order_date_utc,
        coalesce(cast("Order Date Kyiv" as timestamp) at time zone 'America/New_York', 'N/A') as order_date_new_york
    from {{ ref('Sample Data for GYB test - Query result') }}
),

sales_agents as (
    select
        coalesce(reference_id, 'N/A') as reference_id,
        coalesce(string_agg(distinct "Sales Agent Name", ', '), 'N/A') as sales_agents
    from {{ ref('Sample Data for GYB test - Query result') }}
    group by reference_id
)

select
    b.*,
    a.sales_agents
from base_data b
left join sales_agents a
on b.reference_id = a.reference_id
