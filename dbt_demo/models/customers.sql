WITH customer_orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customer_payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
)

SELECT
    customer.customer_id,
    customer.first_name,
    customer.last_name,
    customer_orders.first_order_date,
    customer_orders.last_order_date,
    customer_orders.number_of_orders,
    customer_payments.total_amount
FROM
    customer
INNER JOIN customer_orders USING(customer_id)
INNER JOIN customer_payments USING(customer_id)