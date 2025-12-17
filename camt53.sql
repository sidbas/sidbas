SELECT
  XMLQUERY(
    'declare namespace ns="urn:iso:std:iso:20022:tech:xsd:camt.053.001.02";
     /root/ns:GrpHdr'
    PASSING xml_doc
    RETURNING CONTENT
  )
FROM your_table
WHERE ROWNUM = 1;


SELECT
    id AS _id,
    TO_CHAR(ts_col,'YYYY-MM-DD"T"HH24:MI:SS') AS timestamp,
    JSON_OBJECT(
      '_id' VALUE id,
      'timestamp' VALUE TO_CHAR(ts_col,'YYYY-MM-DD"T"HH24:MI:SS'),
      'MsgId' VALUE x.MsgId,
      'CreDtTm' VALUE x.CreDtTm,
      'NbOfStmts' VALUE x.NbOfStmts,
      'CtrlSum' VALUE x.CtrlSum,
      'AcctId' VALUE a.Id,
      'AcctCcy' VALUE a.Ccy,
      'data' VALUE XMLSERIALIZE(CONTENT y.XML_FRAGMENT AS CLOB)  -- Remaining XML
      RETURNING CLOB
    ) AS json_doc
FROM your_table t
CROSS JOIN XMLTABLE(
    '/root/GrpHdr'
    PASSING xml_doc
    COLUMNS
      MsgId      VARCHAR2(35) PATH 'MsgId',
      CreDtTm    VARCHAR2(35) PATH 'CreDtTm',
      NbOfStmts  VARCHAR2(5)  PATH 'NbOfStmts',
      CtrlSum    NUMBER       PATH 'CtrlSum'
) x
CROSS JOIN XMLTABLE(
    '/root/Stmt/Acct'
    PASSING xml_doc
    COLUMNS
      Id  VARCHAR2(34) PATH 'Id/IBAN',
      Ccy VARCHAR2(3)  PATH 'Ccy'
) a
CROSS JOIN XMLTABLE(
    '/root/Stmt'
    PASSING xml_doc
    COLUMNS
      XML_FRAGMENT XMLTYPE PATH '.'
) y;