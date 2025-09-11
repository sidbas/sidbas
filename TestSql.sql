WITH col_list AS (
    SELECT RTRIM(
             TO_CLOB(
               XMLAGG(
                 XMLELEMENT(e, '"' || column_name || '" AS "' || column_name || '",')
                 ORDER BY column_id
               ).getClobVal()
             ),
             ','
           ) AS cols
    FROM all_tab_columns
    WHERE owner = 'YOUR_SCHEMA'      -- replace with schema
      AND table_name = 'YOUR_TABLE'  -- replace with table
)
SELECT column_name
FROM (
    SELECT column_name, val
    FROM your_schema.your_table
    UNPIVOT (
        val FOR column_name IN (
            -- 👇 paste the output of col_list here
            "COL1" AS "COL1",
            "COL2" AS "COL2",
            "COL3" AS "COL3"
        )
    )
)
GROUP BY column_name
HAVING COUNT(val) = 0;



SELECT RTRIM(
         TO_CLOB(
           XMLAGG(
             XMLELEMENT(e, '"' || column_name || '" AS "' || column_name || '",')
             ORDER BY column_id
           ).getClobVal()
         ),
         ','
       ) AS col_list
FROM all_tab_columns
WHERE owner = 'YOUR_SCHEMA'
  AND table_name = 'YOUR_TABLE';

SELECT RTRIM(
         XMLAGG(
             XMLELEMENT(e, '"' || column_name || '" AS "' || column_name || '",')
             ORDER BY column_id
         ).EXTRACT('//text()'), ','
       ) AS col_list
FROM all_tab_columns
WHERE owner = 'YOUR_SCHEMA'
  AND table_name = 'YOUR_TABLE';


SELECT LISTAGG('"' || column_name || '" AS "' || column_name || '"', ', ')
       WITHIN GROUP (ORDER BY column_id) AS unpivot_list
FROM all_tab_columns
WHERE owner = 'YOUR_SCHEMA'
  AND table_name = 'YOUR_TABLE';

WITH unpivoted AS (
    SELECT column_name, val
    FROM your_table
    UNPIVOT (
        val FOR column_name IN (
            col1 AS 'COL1',
            col2 AS 'COL2',
            col3 AS 'COL3',
            col4 AS 'COL4'
            -- add all your columns here
        )
    )
)
SELECT column_name
FROM unpivoted
GROUP BY column_name
HAVING COUNT(val) = 0;