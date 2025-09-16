SELECT AVG(rowlen) AS avg_row_size_bytes,
       MAX(rowlen) AS max_row_size_bytes,
       AVG(cloblen) AS avg_clob_size_bytes,
       MAX(cloblen) AS max_clob_size_bytes
FROM (
  SELECT 
         ( NVL(VSIZE(id),0) +
           NVL(VSIZE(name),0) +
           NVL(VSIZE(created_at),0) +
           11 ) AS rowlen,
         ( NVL(DBMS_LOB.getlength(doc),0) ) AS cloblen
  FROM test
);