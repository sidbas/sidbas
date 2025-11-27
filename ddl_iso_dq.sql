CREATE OR REPLACE FUNCTION validate_iso_message (
    p_xml_msg  IN CLOB,
    p_xsd_name IN VARCHAR2
) RETURN CLOB
AS
    l_rules_json CLOB;
    l_result     CLOB;
BEGIN
    SELECT rule_json INTO l_rules_json
    FROM iso_dq_rules
    WHERE xsd_name = p_xsd_name;

    SELECT JSON_ARRAYAGG(
               JSON_OBJECT(
                    'path'      VALUE r.path,
                    'required'  VALUE r.required,
                    'exists'    VALUE EXISTSNode(
                                    XMLTYPE(p_xml_msg),
                                    CASE 
                                        WHEN SUBSTR(r.path,1,1)='/' 
                                          THEN r.path
                                        ELSE '/'||r.path
                                    END
                                 ),
                    'valid'     VALUE 
                        CASE 
                            WHEN r.required = 1 
                             AND EXISTSNode(
                                    XMLTYPE(p_xml_msg),
                                    CASE 
                                        WHEN SUBSTR(r.path,1,1)='/' 
                                          THEN r.path
                                        ELSE '/'||r.path
                                    END
                                ) = 0
                                THEN 'missing'
                            ELSE 'ok'
                        END
               )
           RETURNING CLOB)
    INTO l_result
    FROM JSON_TABLE(
        l_rules_json,
        '$.rules[*]'
        COLUMNS (
            path      VARCHAR2(400) PATH '$.path',
            required  NUMBER        PATH '$.required'
        )
    ) r;

    RETURN l_result;
END;
/



CREATE OR REPLACE FUNCTION validate_iso_message (
    p_xml_msg  IN CLOB,
    p_xsd_name IN VARCHAR2
) RETURN CLOB
AS
    l_rules_json CLOB;
    l_result     CLOB;
BEGIN
    SELECT rule_json INTO l_rules_json
    FROM iso_dq_rules
    WHERE xsd_name = p_xsd_name;

    SELECT JSON_ARRAYAGG(
               JSON_OBJECT(
                    'path'      VALUE r.path,
                    'required'  VALUE r.required,
                    'exists'    VALUE EXISTSNode(XMLTYPE(p_xml_msg), r.path),
                    'valid'     VALUE 
                        CASE 
                            WHEN r.required = 1 AND EXISTSNode(XMLTYPE(p_xml_msg), r.path) = 0
                                THEN 'missing'
                            ELSE 'ok'
                        END
               )
           RETURNING CLOB
           )
    INTO l_result
    FROM JSON_TABLE(
        l_rules_json,
        '$.rules[*]'
        COLUMNS (
            path      VARCHAR2(400) PATH '$.path',
            required  NUMBER        PATH '$.required'
        )
    ) r;

    RETURN l_result;
END;
/

SHOW ERRORS FUNCTION EXISTSNODE;
SHOW ERRORS FUNCTION VALIDATE_ISO_MESSAGE;



CREATE OR REPLACE FUNCTION validate_iso_message (
    p_xml_msg  IN CLOB,
    p_xsd_name IN VARCHAR2
) RETURN CLOB
AS
    l_rules_json CLOB;
    l_result     CLOB;
BEGIN
    SELECT rule_json INTO l_rules_json
    FROM iso_dq_rules
    WHERE xsd_name = p_xsd_name;

    SELECT JSON_ARRAYAGG(
               JSON_OBJECT(
                    'path'      VALUE r.path,
                    'required'  VALUE r.required,
                    'exists'    VALUE 
                        EXISTSNode(XMLTYPE(p_xml_msg), r.path) FORMAT JSON,
                    'valid'     VALUE 
                        CASE 
                            WHEN r.required = 1 AND NOT EXISTSNode(XMLTYPE(p_xml_msg), r.path)
                                THEN 'missing'
                            ELSE 'ok'
                        END
               )
           RETURNING CLOB
           )
    INTO l_result
    FROM JSON_TABLE(
        l_rules_json,
        '$.rules[*]'
        COLUMNS (
            path      VARCHAR2(400) PATH '$.path',
            required  NUMBER        PATH '$.required'
        )
    ) r;

    RETURN l_result;
END;
/

CREATE OR REPLACE FUNCTION EXISTSNode(p_xml XMLTYPE, p_xpath VARCHAR2)
RETURN NUMBER
AS
    cnt NUMBER;
BEGIN
    SELECT COUNT(*) INTO cnt
    FROM XMLTABLE(p_xpath PASSING p_xml);

    RETURN CASE WHEN cnt > 0 THEN 1 ELSE 0 END;
END;
/

CREATE TABLE iso_messages (
    msg_id NUMBER,
    xsd_name VARCHAR2(100),
    xml_payload CLOB
);

SELECT
    msg_id,
    validate_iso_message(xml_payload, xsd_name) AS dq_report
FROM iso_messages;





