SELECT
    customer_id,
    COUNT(rental_id) as number_of_orders,
    MIN(rental_date) as first_order_date,
    MAX(rental_date) as last_order_date
FROM rental
GROUP BY customer_id