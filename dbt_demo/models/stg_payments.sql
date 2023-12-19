SELECT
    customer_id,
    SUM(amount) as total_amount
FROM payment 
GROUP BY customer_id