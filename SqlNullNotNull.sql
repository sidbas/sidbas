SELECT column_name,
       ROUND(100 * nulls / total, 2)    AS null_pct,
       ROUND(100 * notnulls / total, 2) AS notnull_pct
FROM (
  SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN col1 IS NULL THEN 1 ELSE 0 END) AS col1_nulls,
    SUM(CASE WHEN col1 IS NOT NULL THEN 1 ELSE 0 END) AS col1_notnulls,
    SUM(CASE WHEN col2 IS NULL THEN 1 ELSE 0 END) AS col2_nulls,
    SUM(CASE WHEN col2 IS NOT NULL THEN 1 ELSE 0 END) AS col2_notnulls,
    SUM(CASE WHEN col3 IS NULL THEN 1 ELSE 0 END) AS col3_nulls,
    SUM(CASE WHEN col3 IS NOT NULL THEN 1 ELSE 0 END) AS col3_notnulls
  FROM your_table
) t
UNPIVOT (
  nulls FOR column_name IN (
    col1_nulls   AS COL1,
    col2_nulls   AS COL2,
    col3_nulls   AS COL3
  )
) u1
JOIN (
  SELECT
    SUM(CASE WHEN col1 IS NULL THEN 1 ELSE 0 END) AS col1_nulls,
    SUM(CASE WHEN col1 IS NOT NULL THEN 1 ELSE 0 END) AS col1_notnulls,
    SUM(CASE WHEN col2 IS NULL THEN 1 ELSE 0 END) AS col2_nulls,
    SUM(CASE WHEN col2 IS NOT NULL THEN 1 ELSE 0 END) AS col2_notnulls,
    SUM(CASE WHEN col3 IS NULL THEN 1 ELSE 0 END) AS col3_nulls,
    SUM(CASE WHEN col3 IS NOT NULL THEN 1 ELSE 0 END) AS col3_notnulls
  FROM your_table
) t2
UNPIVOT (
  notnulls FOR column_name2 IN (
    col1_notnulls AS COL1,
    col2_notnulls AS COL2,
    col3_notnulls AS COL3
  )
) u2
  ON u1.column_name = u2.column_name2;