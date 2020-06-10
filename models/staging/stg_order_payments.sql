with orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (
    select * from {{ref('stg_payments')}}
)


order_payments as (

    select
        orders.order_id,
        payments.payment_method,
        payments.payment_status,
        case 
            when payments.payment_status = "success" then payments.amount
            else 0
        end as amount
        orders.customer_id,
        orders.order_status,
        orders.order_date

    from orders

    left join payments using (order_id)

)

select * from order_payments