-- Named inputs: changes = this committed ODS interval with _colvenna_operation and
-- _colvenna_commit_sequence; key_state = the target's _colvenna_version/_colvenna_sequence/_colvenna_deleted.
-- Wire output metadata is kept by the runner, not exposed as business columns.
WITH ranked AS (
    SELECT *, row_number() OVER (
        PARTITION BY order_id ORDER BY source_version DESC, _colvenna_commit_sequence DESC
    ) AS version_rank
    FROM changes
)
SELECT
    CASE WHEN c._colvenna_operation = 'delete' THEN 'DELETE' ELSE 'UPSERT' END AS _colvenna_op,
    c.order_id,
    CASE WHEN c._colvenna_operation = 'delete' THEN NULL ELSE CAST(c.event_time AS DATE) END AS order_date,
    CASE WHEN c._colvenna_operation = 'delete' THEN NULL ELSE CAST(c.amount AS DECIMAL(18,2)) END AS amount,
    c.source_version AS _colvenna_version,
    c._colvenna_commit_sequence AS _colvenna_sequence
FROM ranked c
LEFT JOIN key_state s USING (order_id)
WHERE c.version_rank = 1
  AND (s.order_id IS NULL
       OR c.source_version > s._colvenna_version
       OR (c.source_version = s._colvenna_version AND c._colvenna_commit_sequence > s._colvenna_sequence));
