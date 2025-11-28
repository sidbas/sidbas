CREATE OR REPLACE FUNCTION validate_iso_message (
    p_xml_msg  IN CLOB,
    p_xsd_name IN VARCHAR2
) RETURN CLOB
AS
    l_rules_json   CLOB;
    l_result       CLOB;
    l_err          VARCHAR2(4000);
    l_ns           VARCHAR2(4000);
BEGIN
    --------------------------------------------------------------------
    -- 1. Extract default namespace from XML
    --------------------------------------------------------------------
    l_ns := REGEXP_SUBSTR(
                p_xml_msg,
                'xmlns\s*=\s*"(.*?)"',
                1, 1, NULL, 1
            );

    IF l_ns IS NULL THEN
        l_ns := REGEXP_SUBSTR(
                    p_xml_msg,
                    'xmlns\s*=\s*''(.*?)''',
                    1, 1, NULL, 1
                );
    END IF;

    --------------------------------------------------------------------
    -- 2. Fetch rules JSON safely
    --------------------------------------------------------------------
    BEGIN
        SELECT rule_json
        INTO l_rules_json
        FROM iso_dq_rules
        WHERE xsd_name = p_xsd_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- Build error JSON using TO_CLOB
            l_result := TO_CLOB('{"error":"No rules found for XSD","xsd_name":"' || p_xsd_name || '"}');
            RETURN l_result;
    END;

    --------------------------------------------------------------------
    -- 3. Build DQ Report JSON safely in PL/SQL
    --------------------------------------------------------------------
    SELECT TO_CLOB(
               JSON_ARRAYAGG(
                   JSON_OBJECT(
                       'path'     VALUE r.path,
                       'required' VALUE r.required,
                       'exists'   VALUE EXISTSNode_NS(
                                       XMLTYPE(p_xml_msg),
                                       CASE WHEN SUBSTR(r.path,1,1)='/' THEN r.path ELSE '/'||r.path END,
                                       l_ns
                                   ),
                       'valid'    VALUE CASE
                                           WHEN r.required = 1 AND
                                                EXISTSNode_NS(
                                                    XMLTYPE(p_xml_msg),
                                                    CASE WHEN SUBSTR(r.path,1,1)='/' THEN r.path ELSE '/'||r.path END,
                                                    l_ns
                                                ) = 0
                                           THEN 'missing'
                                           ELSE 'ok'
                                       END
                   )
               )
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

EXCEPTION
    WHEN OTHERS THEN
        l_err := SQLERRM;
        l_result := TO_CLOB('{"error":"' || l_err || '"}');
        RETURN l_result;
END;
/


SELECT
    JSON_ARRAYAGG(
        JSON_OBJECT(
            'path'     VALUE r.path,
            'required' VALUE r.required,
            'exists'   VALUE EXISTSNode_NS(
                            XMLTYPE(p_xml_msg),
                            CASE WHEN SUBSTR(r.path,1,1)='/' THEN r.path ELSE '/'||r.path END,
                            l_ns
                        ),
            'valid'    VALUE CASE
                                WHEN r.required = 1 AND
                                     EXISTSNode_NS(
                                         XMLTYPE(p_xml_msg),
                                         CASE WHEN SUBSTR(r.path,1,1)='/' THEN r.path ELSE '/'||r.path END,
                                         l_ns
                                     ) = 0
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
            path     VARCHAR2(400) PATH '$.path',
            required NUMBER        PATH '$.required'
        )
    ) r;




CREATE OR REPLACE FUNCTION EXISTSNode_NS(
    p_xml    IN XMLTYPE,
    p_xpath  IN VARCHAR2,
    p_ns_uri IN VARCHAR2
) RETURN NUMBER
AS
    cnt      NUMBER := 0;
    l_xpath  VARCHAR2(4000);
    sql_stmt VARCHAR2(4000);
BEGIN
    IF p_xpath IS NULL OR TRIM(p_xpath) = '' THEN
        RETURN 0;
    END IF;

    -- Ensure leading slash
    IF SUBSTR(p_xpath,1,1) <> '/' THEN
        l_xpath := '/' || p_xpath;
    ELSE
        l_xpath := p_xpath;
    END IF;

    -- If namespace URI provided, prefix each element step with ns:
    IF p_ns_uri IS NOT NULL THEN
        -- Insert ns: prefix before each local-name after slash or at start.
        -- Use regexp_replace to transform "/A/B" into "/ns:A/ns:B"
        l_xpath := REGEXP_REPLACE(l_xpath, '(^|/)([^/]+)', '\1ns:\2');
        -- Build SQL using XMLNAMESPACES to bind prefix "ns" to the URI
        sql_stmt := 'SELECT COUNT(*) FROM XMLTABLE(XMLNAMESPACES(''' 
                    || p_ns_uri || ''' as "ns"), ''' || l_xpath || ''' PASSING :xml)';
    ELSE
        -- No namespace - normal xpath
        sql_stmt := 'SELECT COUNT(*) FROM XMLTABLE(''' 
                    || l_xpath || ''' PASSING :xml)';
    END IF;

    EXECUTE IMMEDIATE sql_stmt INTO cnt USING p_xml;
    RETURN CASE WHEN cnt > 0 THEN 1 ELSE 0 END;

EXCEPTION
    WHEN OTHERS THEN
        -- On error, return 0 (existence check fails); caller may choose to surface errors.
        RETURN 0;
END;
/

CREATE OR REPLACE FUNCTION validate_iso_message (
    p_xml_msg  IN CLOB,
    p_xsd_name IN VARCHAR2
) RETURN CLOB
AS
    l_rules_json   CLOB;
    l_result       CLOB;
    l_err          VARCHAR2(4000);
    l_ns           VARCHAR2(4000);
BEGIN
    --------------------------------------------------------------------
    -- 0. Extract default namespace URI from XML text (robust to " or ')
    --------------------------------------------------------------------
    -- Try to extract xmlns="..." first
    l_ns := REGEXP_SUBSTR(p_xml_msg, 'xmlns\s*=\s*"(.*?)"', 1, 1, NULL, 1);

    -- If not found, try single-quoted version
    IF l_ns IS NULL THEN
        l_ns := REGEXP_SUBSTR(p_xml_msg, 'xmlns\s*=\s*''(.*?)''', 1, 1, NULL, 1);
    END IF;

    --------------------------------------------------------------------
    -- 1. Load rules JSON safely
    --------------------------------------------------------------------
    BEGIN
        SELECT rule_json
        INTO l_rules_json
        FROM iso_dq_rules
        WHERE xsd_name = p_xsd_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            SELECT TO_CLOB(
                       JSON_OBJECT(
                           'error'           VALUE 'no rules found for xsd_name',
                           'xsd_name'        VALUE p_xsd_name,
                           'available_rules' VALUE (
                               SELECT JSON_ARRAYAGG(xsd_name) FROM iso_dq_rules
                           )
                       ) RETURNING CLOB
                   )
            INTO l_result
            FROM dual;
            RETURN l_result;
    END;

    --------------------------------------------------------------------
    -- 2. Build DQ report (namespace-aware)
    --    Use JSON_OBJECT / JSON_ARRAYAGG with RETURNING CLOB to avoid size limits
    --------------------------------------------------------------------
    SELECT JSON_ARRAYAGG(
               JSON_OBJECT(
                   'path'      VALUE r.path RETURNING CLOB,
                   'required'  VALUE r.required,
                   'exists'    VALUE EXISTSNode_NS(XMLTYPE(p_xml_msg),
                                                   CASE WHEN SUBSTR(r.path,1,1)='/' THEN r.path ELSE '/'||r.path END,
                                                   l_ns),
                   'valid'     VALUE CASE
                                       WHEN r.required = 1
                                            AND EXISTSNode_NS(XMLTYPE(p_xml_msg),
                                                              CASE WHEN SUBSTR(r.path,1,1)='/' THEN r.path ELSE '/'||r.path END,
                                                              l_ns) = 0
                                       THEN 'missing'
                                       ELSE 'ok'
                                   END
               RETURNING CLOB
               )
           RETURNING CLOB
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

EXCEPTION
    WHEN OTHERS THEN
        l_err := SQLERRM;
        SELECT TO_CLOB(JSON_OBJECT('error' VALUE l_err) RETURNING CLOB)
        INTO l_result
        FROM dual;
        RETURN l_result;
END;
/





    SELECT JSON_ARRAYAGG(
               JSON_OBJECT(
                   'path'      VALUE r.path,
                   'required'  VALUE r.required,
                   'exists'    VALUE EXISTSNode(
                                    XMLTYPE(p_xml_msg),
                                    CASE WHEN SUBSTR(r.path,1,1)='/' 
                                         THEN r.path ELSE '/'||r.path END
                                ),
                   'valid'     VALUE
                       CASE
                           WHEN r.required = 1
                                AND EXISTSNode(
                                        XMLTYPE(p_xml_msg),
                                        CASE WHEN SUBSTR(r.path,1,1)='/' 
                                             THEN r.path ELSE '/'||r.path END
                                    ) = 0
                           THEN 'missing'
                           ELSE 'ok'
                       END
               RETURNING CLOB)
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





CREATE OR REPLACE FUNCTION validate_iso_message (
    p_xml_msg  IN CLOB,
    p_xsd_name IN VARCHAR2
) RETURN CLOB
AS
    l_rules_json   CLOB;
    l_missing_rules CLOB;
    l_result       CLOB;
    l_err          VARCHAR2(4000);
BEGIN
    --------------------------------------------------------------------
    -- 1. Try to load rules for this XSD
    --------------------------------------------------------------------
    BEGIN
        SELECT rule_json
        INTO l_rules_json
        FROM iso_dq_rules
        WHERE xsd_name = p_xsd_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- Build error JSON in SQL using variables, not SQLERRM
            SELECT TO_CLOB(
                       JSON_OBJECT(
                           'error'           VALUE 'no rules found for xsd_name',
                           'xsd_name'        VALUE p_xsd_name,
                           'available_rules' VALUE (
                               SELECT JSON_ARRAYAGG(xsd_name)
                               FROM iso_dq_rules
                           )
                       )
                   )
            INTO l_missing_rules
            FROM dual;

            RETURN l_missing_rules;
    END;

    --------------------------------------------------------------------
    -- 2. Produce validation output
    --------------------------------------------------------------------
    SELECT TO_CLOB(
               JSON_ARRAYAGG(
                   JSON_OBJECT(
                       'path'      VALUE r.path,
                       'required'  VALUE r.required,
                       'exists'    VALUE EXISTSNode(
                                        XMLTYPE(p_xml_msg),
                                        CASE WHEN SUBSTR(r.path,1,1)='/' 
                                             THEN r.path ELSE '/'||r.path END
                                    ),
                       'valid'     VALUE
                           CASE
                               WHEN r.required = 1
                                    AND EXISTSNode(
                                            XMLTYPE(p_xml_msg),
                                            CASE WHEN SUBSTR(r.path,1,1)='/' 
                                                 THEN r.path ELSE '/'||r.path END
                                        ) = 0
                               THEN 'missing'
                               ELSE 'ok'
                           END
                   )
               )
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

EXCEPTION
    WHEN OTHERS THEN
        -- Capture SQLERRM safely into PL/SQL variable
        l_err := SQLERRM;

        -- Build JSON using l_err (safe)
        SELECT TO_CLOB(
                   JSON_OBJECT(
                       'error' VALUE l_err
                   )
               )
        INTO l_result
        FROM dual;

        RETURN l_result;
END;
/



CREATE OR REPLACE FUNCTION validate_iso_message (
    p_xml_msg  IN CLOB,
    p_xsd_name IN VARCHAR2
) RETURN CLOB
AS
    l_rules_json CLOB;
    l_missing_rules CLOB;
    l_result CLOB;
BEGIN
    --------------------------------------------------------------------
    -- 1. Try to load rules for this XSD
    --------------------------------------------------------------------
    BEGIN
        SELECT rule_json
        INTO l_rules_json
        FROM iso_dq_rules
        WHERE xsd_name = p_xsd_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- Build error JSON in SQL (not PL/SQL)
            SELECT TO_CLOB(
                       JSON_OBJECT(
                           'error'          VALUE 'no rules found for xsd_name',
                           'xsd_name'       VALUE p_xsd_name,
                           'available_rules' VALUE (
                               SELECT JSON_ARRAYAGG(xsd_name)
                               FROM iso_dq_rules
                           )
                       )
                   )
            INTO l_missing_rules
            FROM dual;

            RETURN l_missing_rules;
    END;

    --------------------------------------------------------------------
    -- 2. Produce validation output (must be inside SQL)
    --------------------------------------------------------------------
    SELECT TO_CLOB(
               JSON_ARRAYAGG(
                   JSON_OBJECT(
                       'path'      VALUE r.path,
                       'required'  VALUE r.required,
                       'exists'    VALUE EXISTSNode(
                                        XMLTYPE(p_xml_msg),
                                        CASE WHEN SUBSTR(r.path,1,1)='/' 
                                             THEN r.path ELSE '/'||r.path END
                                    ),
                       'valid'     VALUE
                           CASE
                               WHEN r.required = 1
                                    AND EXISTSNode(
                                            XMLTYPE(p_xml_msg),
                                            CASE WHEN SUBSTR(r.path,1,1)='/' 
                                                 THEN r.path ELSE '/'||r.path END
                                        ) = 0
                               THEN 'missing'
                               ELSE 'ok'
                           END
                   )
               )
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

EXCEPTION
    WHEN OTHERS THEN
        -- Build fallback error JSON in SQL
        SELECT TO_CLOB(JSON_OBJECT('error' VALUE SQLERRM))
        INTO l_result
        FROM dual;

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
    --------------------------------------------------------------------
    -- 1. Fetch rule JSON safely (defensive check)
    --------------------------------------------------------------------
    BEGIN
        SELECT rule_json INTO l_rules_json
        FROM iso_dq_rules
        WHERE xsd_name = p_xsd_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN TO_CLOB(
                JSON_OBJECT(
                    'error' VALUE 'no rules found for xsd_name',
                    'xsd_name' VALUE p_xsd_name,
                    'available_rules' VALUE (
                        SELECT JSON_ARRAYAGG(xsd_name)
                        FROM iso_dq_rules
                    )
                )
            );
    END;

    --------------------------------------------------------------------
    -- 2. Build DQ report
    --------------------------------------------------------------------
    SELECT TO_CLOB(
               JSON_ARRAYAGG(
                   JSON_OBJECT(
                       'path'      VALUE r.path,
                       'required'  VALUE r.required,
                       'exists'    VALUE EXISTSNode(
                                       XMLTYPE(p_xml_msg),
                                       CASE WHEN SUBSTR(r.path,1,1)='/' 
                                            THEN r.path ELSE '/'||r.path END
                                   ),
                       'valid'     VALUE CASE
                                            WHEN r.required = 1
                                             AND EXISTSNode(
                                                   XMLTYPE(p_xml_msg),
                                                   CASE WHEN SUBSTR(r.path,1,1)='/' 
                                                        THEN r.path ELSE '/'||r.path END
                                                 ) = 0
                                            THEN 'missing'
                                            ELSE 'ok'
                                        END
                   )
               )
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

EXCEPTION
    WHEN OTHERS THEN
        RETURN TO_CLOB(
            JSON_OBJECT('error' VALUE SQLERRM)
        );
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
    -- Try to fetch rules; if none, return a helpful JSON error
    BEGIN
        SELECT rule_json INTO l_rules_json
        FROM iso_dq_rules
        WHERE xsd_name = p_xsd_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN json_object('error' VALUE 'no rules found for xsd_name',
                               'xsd_name' VALUE p_xsd_name,
                               'available_rules' VALUE (
                                   SELECT JSON_ARRAYAGG(xsd_name)
                                   FROM iso_dq_rules
                               )
                              ) RETURNING CLOB;
    END;

    -- Main validation (unchanged)
    SELECT JSON_ARRAYAGG(
               JSON_OBJECT(
                    'path'      VALUE r.path,
                    'required'  VALUE r.required,
                    'exists'    VALUE EXISTSNode(
                                     XMLTYPE(p_xml_msg),
                                     CASE WHEN SUBSTR(r.path,1,1)='/' THEN r.path ELSE '/'||r.path END
                                  ),
                    'valid'     VALUE CASE 
                                        WHEN r.required = 1 
                                         AND EXISTSNode(
                                                XMLTYPE(p_xml_msg),
                                                CASE WHEN SUBSTR(r.path,1,1)='/' THEN r.path ELSE '/'||r.path END
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

EXCEPTION
    WHEN OTHERS THEN
        RETURN json_object('error' VALUE SQLERRM) RETURNING CLOB;
END;
/



-- 1. What xsd names are available in rules table?
SELECT DISTINCT xsd_name FROM iso_dq_rules ORDER BY 1;

-- 2. What xsd names are present in messages table?
SELECT DISTINCT xsd_name FROM iso_messages ORDER BY 1;

-- 3. Which message xsd_name values have no matching rule?
SELECT DISTINCT m.xsd_name
FROM iso_messages m
MINUS
SELECT DISTINCT r.xsd_name
FROM iso_dq_rules r;





CREATE OR REPLACE FUNCTION validate_iso_message (
    p_xml_msg  IN CLOB,
    p_xsd_name IN VARCHAR2
) RETURN CLOB
AS
    l_rules_json CLOB;
    l_result     CLOB;
    l_xpath      VARCHAR2(500);
BEGIN
    -- 1. Fetch the rules JSON
    SELECT rule_json 
    INTO l_rules_json
    FROM iso_dq_rules
    WHERE xsd_name = p_xsd_name;

    -- 2. Run JSON_TABLE + validation
    SELECT JSON_ARRAYAGG(
               JSON_OBJECT(
                    'path'      VALUE r.path,
                    'required'  VALUE r.required,
                    'exists'    VALUE EXISTSNode(
                                     XMLTYPE(p_xml_msg),
                                     CASE 
                                        WHEN SUBSTR(r.path,1,1)='/' THEN r.path
                                        ELSE '/'||r.path
                                     END
                                  ),
                    'valid'     VALUE 
                        CASE 
                            WHEN r.required = 1
                             AND EXISTSNode(
                                    XMLTYPE(p_xml_msg),
                                    CASE 
                                        WHEN SUBSTR(r.path,1,1)='/' THEN r.path
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

EXCEPTION 
    WHEN OTHERS THEN
        RETURN '{"error":"' 
               || REPLACE(SQLERRM,'"','''') 
               || '"}';
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





