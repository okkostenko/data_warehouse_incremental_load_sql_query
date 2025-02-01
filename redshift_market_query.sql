WITH latest_processed_timestamp AS (
    SELECT MAX(processed_at) AS processed_at
    FROM purchases
)
SELECT
    orders.id AS order_id,
    orders.created_at as order_created_at,
    orders.updated_at as order_updated_at,
    orders.uploaded_at as order_uploaded_at,
    
    transactions.id AS transaction_id,
    transactions.created_at as transactions_created_at,
    transactions.updated_at as transactions_updated_at,
    transactions.uploaded_at as transactions_uploaded_at,
    
    verification.id AS verification_id,
    verification.created_at as verification_created_at,
    verification.updated_at as verification_updated_at,
    verification.uploaded_at as verification_uploaded_at,
    CURRENT_TIMESTAMP AS processed_at,
    (
        (orders.created_at = orders.updated_at AND orders.uploaded_at > latest_processed_timestamp.processed_at) OR
        (transactions.created_at = transactions.updated_at AND transactions.uploaded_at > latest_processed_timestamp.processed_at) OR
        (verification.created_at = verification.updated_at AND verification.uploaded_at > latest_processed_timestamp.processed_at)
    ) AS is_created
FROM orders
INNER JOIN transactions ON orders.id = transactions.order_id
INNER JOIN verification ON transactions.id = verification.transaction_id
CROSS JOIN latest_processed_timestamp
WHERE 
    GREATEST(
        orders.uploaded_at,
        transactions.uploaded_at,
        verification.uploaded_at
    ) > latest_processed_timestamp.processed_at;
