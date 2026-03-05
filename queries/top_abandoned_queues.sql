SELECT
    queue_id,
    COUNT(*) AS total_calls,
    COUNT(*) FILTER (WHERE hangup_cause = 'ORIGINATOR_CANCEL') AS abandoned,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE hangup_cause = 'ORIGINATOR_CANCEL') / NULLIF(COUNT(*), 0), 2
    ) AS abandon_rate_pct,
    AVG(bill_seconds) FILTER (WHERE bill_seconds > 0) AS avg_duration_sec
FROM call_records
WHERE start_time >= now() - interval '7 days'
  AND queue_id IS NOT NULL
GROUP BY queue_id
ORDER BY abandon_rate_pct DESC
LIMIT 10;
