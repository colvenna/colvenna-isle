WITH ranked AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY order_id
               ORDER BY source_version DESC, _colvenna_commit_sequence DESC
           ) AS version_rank
    FROM events
)
SELECT order_id,
       CAST(event_time AS DATE) AS order_date,
       CAST(amount AS DECIMAL(18,2)) AS amount
FROM ranked
WHERE version_rank = 1 AND _colvenna_operation <> 'delete';
