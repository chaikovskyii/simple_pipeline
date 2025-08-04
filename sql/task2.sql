SELECT 
    orders.*,
    transactions.*,
    verification.*,
    GREATEST(orders.uploaded_at, transactions.uploaded_at, verification.uploaded_at) as uploaded_at
FROM orders
INNER JOIN transactions ON orders.id = transactions.order_id
INNER JOIN verification ON transactions.id = verification.transaction_id
LEFT JOIN purchases p ON p.order_id = orders.id
WHERE 
    p.order_id IS NULL
    OR GREATEST(orders.uploaded_at, transactions.uploaded_at, verification.uploaded_at) > p.uploaded_at;