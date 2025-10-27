CREATE OR REPLACE VIEW my_catalog.my_schema.account_monthly_report AS
WITH
    -- 1️⃣ Get date range across all accounts
    date_range AS (
        SELECT
            -- Start at the later of earliest open date or 24 months ago
            greatest(
                date_trunc('month', min(open_date)),
                date_trunc('month', date_add('month', -23, current_date))
            ) AS min_month,
            date_trunc('month', max(coalesce(close_date, current_date))) AS max_month
        FROM my_catalog.my_schema.accounts
    ),

    -- 2️⃣ Generate month list using SEQUENCE (no recursion!)
    months AS (
        SELECT sequence(min_month, max_month, interval '1' month) AS month_list
        FROM date_range
    ),

    -- 3️⃣ Expand array of months into rows
    month_expanded AS (
        SELECT month_start
        FROM months
        CROSS JOIN UNNEST(month_list) AS t(month_start)
    )

-- 4️⃣ Final join: account × months it was active
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
JOIN month_expanded m
  ON m.month_start BETWEEN date_trunc('month', a.open_date)
                      AND date_trunc('month', coalesce(a.close_date, TIMESTAMP '2999-12-31 00:00:00'))
ORDER BY a.account_id, m.month_start;



CREATE OR REPLACE VIEW my_catalog.my_schema.account_monthly_report AS
WITH RECURSIVE
    -- 1️⃣ Determine the start and end month range
    date_range AS (
        SELECT
            -- Start at the later of earliest open_date OR 24 months ago
            greatest(
                date_trunc('month', min(open_date)),
                date_trunc('month', date_add('month', -23, current_date))
            ) AS min_month,
            date_trunc('month', max(coalesce(close_date, current_date))) AS max_month
        FROM my_catalog.my_schema.accounts
    ),

    -- 2️⃣ Generate months recursively (must have column alias)
    months(month_start) AS (
        SELECT min_month
        FROM date_range
        UNION ALL
        SELECT date_add('month', 1, month_start)
        FROM months
        JOIN date_range
          ON date_add('month', 1, month_start) <= max_month
    )

-- 3️⃣ Final report — accounts × active months
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

