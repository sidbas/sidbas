SELECT x.instr_id
FROM   my_table t,
       XMLTABLE(
         XMLNAMESPACES(
           DEFAULT 'urn:vity:iso:20022:pacs.008.001.09'
         ),
         '/transaction/CdtTrfTxInf/PmtId'
         PASSING t.xml_data
         COLUMNS
           instr_id VARCHAR2(100) PATH 'InstrId'
       ) x;

SELECT XMLQuery(
         'declare default element namespace "urn:vity:iso:20022:pacs.008.001.09";
          /transaction/CdtTrfTxInf/PmtId/InstrId/text()'
         PASSING xml_data
         RETURNING CONTENT
       ).getStringVal() AS instr_id
FROM my_table;

SELECT x.instr_id,
       x.end_to_end_id,
       x.tx_id
FROM   my_table t,
       XMLTABLE(
         XMLNAMESPACES(
           DEFAULT 'urn:vity:iso:20022:pacs.008.001.09'
         ),
         '/transaction/CdtTrfTxInf/PmtId'
         PASSING t.xml_data
         COLUMNS
           instr_id      VARCHAR2(100) PATH 'InstrId',
           end_to_end_id VARCHAR2(100) PATH 'EndToEndId',
           tx_id         VARCHAR2(100) PATH 'TxId'
       ) x;

SELECT
  extract(value(x), 'local-name(.)').getStringVal() AS element_name,
  extract(value(x), 'string(.)').getStringVal()      AS element_value
FROM
  my_table t,
  TABLE(
    XMLSequence(
      extract(
        t.xml_data,
        'declare default element namespace "urn:vity:iso:20022:pacs.008.001.09";
         //*[text()]'
      )
    )
  ) x;

SELECT x.full_path,
       x.elem_name,
       x.elem_value
FROM   my_table t,
       XMLTABLE(
         XMLNAMESPACES(
           DEFAULT 'urn:vity:iso:20022:pacs.008.001.09'
         ),
         'for $n in /transaction//*[not(*) and normalize-space(.)]
          return
            <row>
              <path>{fn:string-join(
                        for $a in ($n/ancestor-or-self::*) 
                        return concat("/", local-name($a))
                      , "")}</path>
              <name>{local-name($n)}</name>
              <value>{normalize-space(string($n))}</value>
            </row>'
         PASSING t.xml_data
         COLUMNS
           full_path  VARCHAR2(1000) PATH 'path',
           elem_name  VARCHAR2(200)  PATH 'name',
           elem_value VARCHAR2(4000) PATH 'value'
       ) x;

 SELECT
    x.full_path,
    x.name,
    x.value
FROM   my_table t,
       XMLTABLE(
         XMLNAMESPACES(
            DEFAULT 'urn:vity:iso:20022:pacs.008.001.09'
         ),
         '
           for $n in /transaction//*[not(*)]
           return
             (
               (: leaf element :)
               <row>
                 <full_path>
                   {
                     for $a in $n/ancestor-or-self::* 
                     return concat("/", local-name($a))
                   }
                 </full_path>
                 <name>{local-name($n)}</name>
                 <value>{normalize-space(string($n))}</value>
               </row>,

               (: attributes of that leaf element :)
               for $attr in $n/@*
                 return
                   <row>
                     <full_path>
                       {
                         for $a in $n/ancestor-or-self::* 
                         return concat("/", local-name($a))
                       }
                       @{local-name($attr)}
                     </full_path>
                     <name>{local-name($attr)}</name>
                     <value>{string($attr)}</value>
                   </row>
             )
         '
         PASSING t.xml_data
         COLUMNS
           full_path VARCHAR2(4000) PATH 'full_path',
           name      VARCHAR2(200)  PATH 'name',
           value     VARCHAR2(4000) PATH 'value'
       ) x;