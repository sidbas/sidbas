CREATE OR REPLACE VIEW my_catalog.my_schema.account_monthly_report AS
WITH RECURSIVE months(month_start) AS (
    SELECT date_trunc('month', date_add('month', -23, current_date)) AS month_start
    UNION ALL
    SELECT date_add('month', 1, month_start)
    FROM months
    WHERE date_add('month', 1, month_start) <= date_trunc('month', current_date)
)
SELECT
    a.account_id,
    a.customer_id,
    m.month_start,
    CASE 
        WHEN a.close_date IS NULL 
             OR a.close_date >= date_add('day', -1, date_add('month', 1, m.month_start))
        THEN 'ACTIVE'
        ELSE 'CLOSED'
    END AS account_status
FROM my_catalog.my_schema.accounts a
JOIN months m
  ON m.month_start BETWEEN date_trunc('month', a.open_date)
                      AND date_trunc('month', coalesce(a.close_date, TIMESTAMP '2999-12-31 00:00:00'))
ORDER BY a.account_id, m.month_start;

