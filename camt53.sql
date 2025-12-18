SELECT
  gh.MsgId,
  stmt.StmtId,

  -- Entry counts
  stmt.TtlCdtNtries,
  stmt.TtlDbtNtries,

  -- Entry sums
  stmt.TtlCdtSum,
  stmt.TtlDbtSum
FROM your_table t

LEFT JOIN XMLTABLE(
  XMLNAMESPACES(
    'urn:iso:std:iso:20022:tech:xsd:camt.053.001.xx' AS "ns"
  ),
  '/root/ns:group/ns:GrpHdr'
  PASSING t.xml_doc
  COLUMNS
    MsgId VARCHAR2(35) PATH 'ns:MsgId'
) gh ON 1=1

LEFT JOIN XMLTABLE(
  XMLNAMESPACES(
    'urn:iso:std:iso:20022:tech:xsd:camt.053.001.xx' AS "ns"
  ),
  '/root/ns:transaction/ns:Stmt'
  PASSING t.xml_doc
  COLUMNS
    StmtId          VARCHAR2(35) PATH 'ns:Id',
    TtlCdtNtries   NUMBER       PATH 'ns:TtlNtries/ns:TtlCdtNtries',
    TtlDbtNtries   NUMBER       PATH 'ns:TtlNtries/ns:TtlDbtNtries',
    TtlCdtSum      NUMBER       PATH 'ns:TtlNtries/ns:TtlCdtSum',
    TtlDbtSum      NUMBER       PATH 'ns:TtlNtries/ns:TtlDbtSum'
) stmt ON 1=1;




SELECT
  t.id AS src_id,

  gh.MsgId,
  stmt.StmtId,
  acct.IBAN,
  acct.Ccy AS AcctCcy,

  ntry.Amt,
  ntry.Ccy AS AmtCcy,
  ntry.DbCrInd,
  ntry.BookDt,
  ntry.ValDt,

  XMLTOJSON(ntry.NtryXml) AS ntry_json
FROM your_table t

-- ===== Group Header =====
LEFT JOIN XMLTABLE(
  XMLNAMESPACES(
    'urn:iso:std:iso:20022:tech:xsd:camt.053.001.xx' AS "ns"
  ),
  '/root/ns:group/ns:GrpHdr'
  PASSING t.xml_doc
  COLUMNS
    MsgId VARCHAR2(35) PATH 'ns:MsgId'
) gh ON 1=1

-- ===== Statement =====
LEFT JOIN XMLTABLE(
  XMLNAMESPACES(
    'urn:iso:std:iso:20022:tech:xsd:camt.053.001.xx' AS "ns"
  ),
  '/root/ns:transaction/ns:Stmt'
  PASSING t.xml_doc
  COLUMNS
    StmtId VARCHAR2(35) PATH 'ns:Id'
) stmt ON 1=1

-- ===== Account =====
LEFT JOIN XMLTABLE(
  XMLNAMESPACES(
    'urn:iso:std:iso:20022:tech:xsd:camt.053.001.xx' AS "ns"
  ),
  '/root/ns:transaction/ns:Stmt/ns:Acct'
  PASSING t.xml_doc
  COLUMNS
    IBAN VARCHAR2(34) PATH 'ns:Id/ns:IBAN',
    Ccy  VARCHAR2(3)  PATH 'ns:Ccy'
) acct ON 1=1

-- ===== Entries =====
LEFT JOIN XMLTABLE(
  XMLNAMESPACES(
    'urn:iso:std:iso:20022:tech:xsd:camt.053.001.xx' AS "ns"
  ),
  '/root/ns:transaction/ns:Stmt/ns:Ntry'
  PASSING t.xml_doc
  COLUMNS
    Amt     NUMBER       PATH 'ns:Amt',
    Ccy     VARCHAR2(3)  PATH 'ns:Amt/@Ccy',
    DbCrInd VARCHAR2(4)  PATH 'ns:CdtDbtInd',
    BookDt  VARCHAR2(10) PATH 'ns:BookgDt/ns:Dt',
    ValDt   VARCHAR2(10) PATH 'ns:ValDt/ns:Dt',
    NtryXml XMLTYPE      PATH '.'
) ntry ON 1=1;

----------------------------




SELECT
    JSON_OBJECT(
      '_id' VALUE t.id,
      'timestamp' VALUE TO_CHAR(t.ts_col, 'YYYY-MM-DD"T"HH24:MI:SS'),
      'MsgId' VALUE gh.MsgId,
      'CreDtTm' VALUE gh.CreDtTm,
      'AcctId' VALUE acct.IBAN,
      'AcctCcy' VALUE acct.Ccy,
      'data' VALUE XMLTOJSON(stmt.StmtXml)
      RETURNING CLOB
    ) AS json_doc
FROM your_table t

-- ===== Group Header =====
LEFT JOIN XMLTABLE(
  XMLNAMESPACES(
    'urn:iso:std:iso:20022:tech:xsd:camt.053.001.02' AS "ns"
  ),
  '/root/ns:GrpHdr'
  PASSING t.xml_doc
  COLUMNS
    MsgId    VARCHAR2(35) PATH 'ns:MsgId',
    CreDtTm  VARCHAR2(35) PATH 'ns:CreDtTm'
) gh ON 1 = 1

-- ===== Account =====
LEFT JOIN XMLTABLE(
  XMLNAMESPACES(
    'urn:iso:std:iso:20022:tech:xsd:camt.053.001.02' AS "ns"
  ),
  '/root/ns:Stmt/ns:Acct'
  PASSING t.xml_doc
  COLUMNS
    IBAN VARCHAR2(34) PATH 'ns:Id/ns:IBAN',
    Ccy  VARCHAR2(3)  PATH 'ns:Ccy'
) acct ON 1 = 1

-- ===== Full Statement (for Python flattening) =====
LEFT JOIN XMLTABLE(
  XMLNAMESPACES(
    'urn:iso:std:iso:20022:tech:xsd:camt.053.001.02' AS "ns"
  ),
  '/root/ns:Stmt'
  PASSING t.xml_doc
  COLUMNS
    StmtXml XMLTYPE PATH '.'
) stmt ON 1 = 1;




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