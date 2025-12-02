CREATE OR REPLACE FUNCTION build_dq_summary_oracle_23ai_v3 (p_msg_id VARCHAR2)
RETURN CLOB
IS
    l_summary CLOB;
BEGIN
    WITH
    -------------------------------------------------------------------
    -- RULES HANDLING (two shapes supported)
    -------------------------------------------------------------------
    rules AS (
        -- Case 1: rule_json contains { "rules": [ ... ] }
        SELECT r.rowid AS rid, jt.path, r.severity
        FROM iso_dq_rules r
        CROSS APPLY (
            SELECT * FROM JSON_TABLE(
                r.rule_json,
                '$.rules[*]'
                COLUMNS ( path VARCHAR2(500) PATH '$.path' )
            )
        ) jt
        WHERE JSON_EXISTS(r.rule_json,'$.rules')

        UNION ALL

        -- Case 2: rule_json is a top-level array [ {...}, ... ]
        SELECT r.rowid AS rid, jt2.path, r.severity
        FROM iso_dq_rules r
        CROSS APPLY (
            SELECT * FROM JSON_TABLE(
                r.rule_json,
                '$[*]'
                COLUMNS ( path VARCHAR2(500) PATH '$.path' )
            )
        ) jt2
        WHERE NOT JSON_EXISTS(r.rule_json,'$.rules')
          AND JSON_EXISTS(r.rule_json,'$[0]')
    ),

    -------------------------------------------------------------------
    -- DETAILS HANDLING (two shapes supported)
    -------------------------------------------------------------------
    details AS (
        -- Case 1: dq_report contains {"dq_report":[ ... ]}
        SELECT jt.path, jt.exists_flg, jt.location_status, jt.found
        FROM iso_message_dq_report d
        CROSS APPLY (
            SELECT * FROM JSON_TABLE(
                d.dq_report,
                '$.dq_report[*]'
                COLUMNS (
                    path VARCHAR2(500) PATH '$.path',
                    exists_flg NUMBER PATH '$.exists',
                    location_status VARCHAR2(50) PATH '$.location_status',
                    found VARCHAR2(1000) PATH '$.found'
                )
            )
        ) jt
        WHERE d.msg_id = p_msg_id
          AND JSON_EXISTS(d.dq_report,'$.dq_report')

        UNION ALL

        -- Case 2: dq_report is top-level array [ ... ]
        SELECT jt2.path, jt2.exists_flg, jt2.location_status, jt2.found
        FROM iso_message_dq_report d
        CROSS APPLY (
            SELECT * FROM JSON_TABLE(
                d.dq_report,
                '$[*]'
                COLUMNS (
                    path VARCHAR2(500) PATH '$.path',
                    exists_flg NUMBER PATH '$.exists',
                    location_status VARCHAR2(50) PATH '$.location_status',
                    found VARCHAR2(1000) PATH '$.found'
                )
            )
        ) jt2
        WHERE d.msg_id = p_msg_id
          AND NOT JSON_EXISTS(d.dq_report,'$.dq_report')
          AND JSON_EXISTS(d.dq_report,'$[0]')
    ),

    -------------------------------------------------------------------
    -- COMBINE RULES + DETAILS
    -------------------------------------------------------------------
    combined AS (
        SELECT r.severity, r.path,
               NVL(d.exists_flg, 0) AS is_present,
               NVL(d.location_status, 'missing') AS location_status
        FROM rules r
        LEFT JOIN details d ON (r.path = d.path)
    ),

    stats AS (
        SELECT 
            COUNT(*) AS total_rules,
            SUM(CASE WHEN is_present = 1 AND location_status = 'correct' THEN 1 END) AS rules_passed,
            SUM(CASE WHEN is_present = 0 THEN 1 END) AS missing_tags,
            SUM(CASE WHEN location_status = 'wrong_location' THEN 1 END) AS wrong_location,
            SUM(CASE WHEN is_present = 1 AND location_status = 'correct' THEN 1 END) AS correct_location
        FROM combined
    ),

    severity_summary AS (
        SELECT JSON_ARRAYAGG(
            JSON_OBJECT(
                'severity' VALUE severity,
                'total' VALUE COUNT(*),
                'missing' VALUE SUM(CASE WHEN is_present = 0 THEN 1 END),
                'wrong_location' VALUE SUM(CASE WHEN location_status='wrong_location' THEN 1 END)
            )
        ) AS severity_json
        FROM combined
        GROUP BY severity
    )

    SELECT JSON_OBJECT(
               'message_id' VALUE p_msg_id,
               'summary' VALUE (
                   SELECT JSON_OBJECT(
                       'total_rules' VALUE total_rules,
                       'rules_passed' VALUE rules_passed,
                       'missing_tags' VALUE missing_tags,
                       'wrong_location' VALUE wrong_location,
                       'correct_location' VALUE correct_location
                   ) FROM stats
               ),
               'by_severity' VALUE (
                   SELECT COALESCE(
                       (SELECT severity_json FROM severity_summary FETCH FIRST 1 ROWS ONLY),
                       JSON_ARRAY()
                   ) FROM dual
               ),
               'overall_status' VALUE (
                   SELECT CASE
                              WHEN missing_tags > 0 THEN 'fail'
                              WHEN wrong_location > 0 THEN 'warning'
                              ELSE 'pass'
                          END
                   FROM stats
               )
           )
    INTO l_summary
    FROM dual;

    RETURN l_summary;

EXCEPTION
    WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20001,
            'build_dq_summary_oracle_23ai_v3 failed: ' || SQLERRM);
END;
/



CREATE OR REPLACE FUNCTION build_dq_summary_oracle_23ai_v2(p_msg_id VARCHAR2)
RETURN CLOB
IS
    l_summary CLOB;
BEGIN
    WITH
    -- RULES: handle rule_json either as object $.rules[*] or top-level $[*]
    rules AS (
        SELECT r.rowid AS rid, jt.path, r.severity
        FROM iso_dq_rules r
        CROSS JOIN LATERAL (
            SELECT 1 FROM DUAL
        )
        LEFT JOIN JSON_TABLE(
            r.rule_json, 
            '$.rules[*]'
            COLUMNS ( path VARCHAR2(500) PATH '$.path' )
        ) jt ON (JSON_EXISTS(r.rule_json,'$.rules'))
        WHERE r.rule_json IS NOT NULL AND (r.rule_json IS JSON OR r.rule_json IS JSON_FORMAT)
        UNION ALL
        SELECT r.rowid AS rid, jt2.path, r.severity
        FROM iso_dq_rules r
        CROSS JOIN LATERAL (
            SELECT 1 FROM DUAL
        )
        LEFT JOIN JSON_TABLE(
            r.rule_json, 
            '$[*]'
            COLUMNS ( path VARCHAR2(500) PATH '$.path' )
        ) jt2 ON (NOT JSON_EXISTS(r.rule_json,'$.rules') AND JSON_EXISTS(r.rule_json,'$[0]'))
    ),
    -- DETAILS: handle dq_report either as object $.dq_report[*] or top-level $[*]
    details AS (
        -- case: dq_report is { "dq_report":[ ... ] }
        SELECT jt.path, jt.exists_flg, jt.location_status, jt.found
        FROM iso_message_dq_report d,
             JSON_TABLE(
                 d.dq_report,
                 '$.dq_report[*]'
                 COLUMNS (
                     path VARCHAR2(500) PATH '$.path',
                     exists_flg NUMBER PATH '$.exists',
                     location_status VARCHAR2(50) PATH '$.location_status',
                     found VARCHAR2(1000) PATH '$.found'
                 )
             ) jt
        WHERE d.msg_id = p_msg_id
          AND JSON_EXISTS(d.dq_report,'$.dq_report')
        UNION ALL
        -- case: dq_report is a top-level array [ {...}, ... ]
        SELECT jt2.path, jt2.exists_flg, jt2.location_status, jt2.found
        FROM iso_message_dq_report d,
             JSON_TABLE(
                 d.dq_report,
                 '$[*]'
                 COLUMNS (
                     path VARCHAR2(500) PATH '$.path',
                     exists_flg NUMBER PATH '$.exists',
                     location_status VARCHAR2(50) PATH '$.location_status',
                     found VARCHAR2(1000) PATH '$.found'
                 )
             ) jt2
        WHERE d.msg_id = p_msg_id
          AND NOT JSON_EXISTS(d.dq_report,'$.dq_report')
    ),
    combined AS (
        SELECT r.severity, r.path,
               NVL(d.exists_flg, 0) AS is_present,
               NVL(d.location_status,'missing') AS location_status
        FROM rules r
        LEFT JOIN details d ON r.path = d.path
    ),
    stats AS (
        SELECT 
            COUNT(*) AS total_rules,
            SUM(CASE WHEN is_present = 1 AND location_status = 'correct' THEN 1 ELSE 0 END) AS rules_passed,
            SUM(CASE WHEN is_present = 0 THEN 1 ELSE 0 END) AS missing_tags,
            SUM(CASE WHEN location_status = 'wrong_location' THEN 1 ELSE 0 END) AS wrong_location,
            SUM(CASE WHEN is_present = 1 AND location_status = 'correct' THEN 1 ELSE 0 END) AS correct_location
        FROM combined
    ),
    severity_summary AS (
        SELECT JSON_ARRAYAGG(
                   JSON_OBJECT(
                       'severity' VALUE severity,
                       'total' VALUE COUNT(*),
                       'missing' VALUE SUM(CASE WHEN is_present = 0 THEN 1 ELSE 0 END),
                       'wrong_location' VALUE SUM(CASE WHEN location_status = 'wrong_location' THEN 1 ELSE 0 END)
                   )
               ) AS severity_json
        FROM combined
        GROUP BY severity
    )
    SELECT JSON_OBJECT(
               'message_id' VALUE p_msg_id,
               'summary' VALUE (
                   SELECT JSON_OBJECT(
                       'total_rules' VALUE total_rules,
                       'rules_passed' VALUE rules_passed,
                       'missing_tags' VALUE missing_tags,
                       'wrong_location' VALUE wrong_location,
                       'correct_location' VALUE correct_location
                   ) FROM stats
               ),
               'by_severity' VALUE (
                   SELECT COALESCE(
                       (SELECT severity_json FROM severity_summary FETCH FIRST 1 ROWS ONLY),
                       JSON_ARRAY()
                   ) FROM dual
               ),
               'overall_status' VALUE (
                   SELECT CASE
                              WHEN missing_tags > 0 THEN 'fail'
                              WHEN wrong_location > 0 THEN 'warning'
                              ELSE 'pass'
                          END FROM stats
               )
           )
    INTO l_summary
    FROM dual;

    RETURN l_summary;
EXCEPTION
    WHEN OTHERS THEN
        -- helpful debug info if it fails
        RAISE_APPLICATION_ERROR(-20001, 'build_dq_summary_oracle_23ai_v2 error: ' || SQLERRM);
END;
/


SELECT rowid, SUBSTR(rule_json,1,1000) AS snippet,
       CASE
         WHEN JSON_EXISTS(rule_json, '$.rules') THEN 'has_rules_array'
         WHEN JSON_EXISTS(rule_json, '$[0]') THEN 'top_level_array'
         ELSE 'other_or_invalid'
       END shape
FROM iso_dq_rules
WHERE ROWNUM <= 5;

-- if stored as { "dq_report": [ {...} ] }
SELECT JSON_VALUE(dq_report, '$.dq_report[0].path') AS path_from_nested_array
FROM iso_message_dq_report
WHERE msg_id = :your_msg_id;

-- if stored as [ {...} ] at top-level
SELECT JSON_VALUE(dq_report, '$[0].path') AS path_from_top_array
FROM iso_message_dq_report
WHERE msg_id = :your_msg_id;

SELECT msg_id,
       CASE
         WHEN JSON_EXISTS(dq_report, '$.dq_report') THEN 'has_dq_report_array_in_object'
         WHEN JSON_EXISTS(dq_report, '$[0]') THEN 'top_level_array'
         ELSE 'other_or_invalid'
       END as shape
FROM iso_message_dq_report
WHERE ROWNUM = 5;

-- show raw dq_report (first 2000 chars)
SELECT msg_id, SUBSTR(dq_report,1,2000) AS snippet
FROM iso_message_dq_report
WHERE ROWNUM = 1;

CREATE OR REPLACE FUNCTION build_dq_summary_oracle_23ai(p_msg_id VARCHAR2)
RETURN CLOB
IS
    l_summary CLOB;
BEGIN
    WITH rules AS (
        -- Extract rule paths from iso_dq_rules.rule_json
        SELECT r.rowid AS rid,
               jt.path,
               r.severity
        FROM iso_dq_rules r,
             JSON_TABLE(
                 r.rule_json,
                 '$.rules[*]'
                 COLUMNS (
                     path VARCHAR2(500) PATH '$.path'
                 )
             ) jt
        WHERE r.rule_json IS NOT NULL AND r.rule_json IS JSON
    ),
    details AS (
        -- Extract evaluated DQ report from iso_message_dq_report.dq_report
        SELECT jt.path,
               jt.exists_flg,
               jt.location_status,
               jt.found
        FROM iso_message_dq_report d,
             JSON_TABLE(
                 d.dq_report,          -- <<< FIXED
                 '$[*]'
                 COLUMNS (
                     path VARCHAR2(500) PATH '$.path',
                     exists_flg NUMBER PATH '$.exists',
                     location_status VARCHAR2(50) PATH '$.location_status',
                     found VARCHAR2(1000) PATH '$.found'
                 )
             ) jt
        WHERE d.msg_id = p_msg_id
    ),
    combined AS (
        -- Join rules with DQ report values
        SELECT r.severity,
               r.path,
               NVL(d.exists_flg, 0) AS is_present,
               NVL(d.location_status, 'missing') AS location_status
        FROM rules r
        LEFT JOIN details d
        ON r.path = d.path
    ),
    stats AS (
        -- Overall statistics
        SELECT 
            COUNT(*) AS total_rules,
            SUM(CASE WHEN is_present = 1 AND location_status = 'correct' THEN 1 ELSE 0 END) AS rules_passed,
            SUM(CASE WHEN is_present = 0 THEN 1 ELSE 0 END) AS missing_tags,
            SUM(CASE WHEN location_status = 'wrong_location' THEN 1 ELSE 0 END) AS wrong_location,
            SUM(CASE WHEN is_present = 1 AND location_status = 'correct' THEN 1 ELSE 0 END) AS correct_location
        FROM combined
    ),
    severity_summary AS (
        -- Summary by severity level
        SELECT JSON_ARRAYAGG(
                   JSON_OBJECT(
                       'severity' VALUE severity,
                       'total' VALUE COUNT(*),
                       'missing' VALUE SUM(CASE WHEN is_present = 0 THEN 1 ELSE 0 END),
                       'wrong_location' VALUE SUM(CASE WHEN location_status = 'wrong_location' THEN 1 ELSE 0 END)
                   )
               ) AS severity_json
        FROM combined
        GROUP BY severity
    )
    -- Final JSON summary object
    SELECT JSON_OBJECT(
               'message_id' VALUE p_msg_id,
               'summary' VALUE (
                   SELECT JSON_OBJECT(
                       'total_rules' VALUE total_rules,
                       'rules_passed' VALUE rules_passed,
                       'missing_tags' VALUE missing_tags,
                       'wrong_location' VALUE wrong_location,
                       'correct_location' VALUE correct_location
                   )
                   FROM stats
               ),
               'by_severity' VALUE (
                   SELECT severity_json
                   FROM severity_summary
                   FETCH FIRST 1 ROWS ONLY
               ),
               'overall_status' VALUE (
                   SELECT CASE
                              WHEN missing_tags > 0 THEN 'fail'
                              WHEN wrong_location > 0 THEN 'warning'
                              ELSE 'pass'
                          END
                   FROM stats
               )
           )
    INTO l_summary
    FROM dual;

    RETURN l_summary;
END;
/


CREATE OR REPLACE FUNCTION build_dq_summary_oracle_23ai(p_msg_id VARCHAR2)
RETURN CLOB
IS
    l_summary CLOB;
BEGIN
    WITH rules AS (
        -- Extract all paths + severity from rule_json
        SELECT r.rowid AS rid,
               jt.path,
               r.severity
        FROM iso_dq_rules r,
             JSON_TABLE(
                 r.rule_json,
                 '$.rules[*]'
                 COLUMNS (
                     path VARCHAR2(500) PATH '$.path'
                 )
             ) jt
        WHERE r.rule_json IS NOT NULL AND r.rule_json IS JSON
    ),
    details AS (
        -- Extract DQ results from ISO_MESSAGE_DQ_REPORT
        SELECT jt.path,
               jt.exists_flg,
               jt.location_status,
               jt.found
        FROM iso_message_dq_report d,
             JSON_TABLE(
                 d.dq_details,
                 '$[*]'
                 COLUMNS (
                     path VARCHAR2(500) PATH '$.path',
                     exists_flg NUMBER PATH '$.exists',
                     location_status VARCHAR2(50) PATH '$.location_status',
                     found VARCHAR2(1000) PATH '$.found'
                 )
             ) jt
        WHERE d.msg_id = p_msg_id
    ),
    combined AS (
        -- Join rules with the actual results
        SELECT r.severity,
               r.path,
               NVL(d.exists_flg, 0) AS is_present,
               NVL(d.location_status, 'missing') AS location_status
        FROM rules r
        LEFT JOIN details d
        ON r.path = d.path
    ),
    stats AS (
        -- Global DQ statistics
        SELECT 
            COUNT(*) AS total_rules,
            SUM(CASE WHEN is_present = 1 AND location_status = 'correct' THEN 1 ELSE 0 END) AS rules_passed,
            SUM(CASE WHEN is_present = 0 THEN 1 ELSE 0 END) AS missing_tags,
            SUM(CASE WHEN location_status = 'wrong_location' THEN 1 ELSE 0 END) AS wrong_location,
            SUM(CASE WHEN is_present = 1 AND location_status = 'correct' THEN 1 ELSE 0 END) AS correct_location
        FROM combined
    ),
    severity_summary AS (
        -- Severity-level summaries
        SELECT JSON_ARRAYAGG(
                   JSON_OBJECT(
                       'severity' VALUE severity,
                       'total' VALUE COUNT(*),
                       'missing' VALUE SUM(CASE WHEN is_present = 0 THEN 1 ELSE 0 END),
                       'wrong_location' VALUE SUM(CASE WHEN location_status = 'wrong_location' THEN 1 ELSE 0 END)
                   )
               ) AS severity_json
        FROM combined
        GROUP BY severity
    )
    SELECT JSON_OBJECT(
               'message_id' VALUE p_msg_id,
               'summary' VALUE (
                   SELECT JSON_OBJECT(
                       'total_rules' VALUE total_rules,
                       'rules_passed' VALUE rules_passed,
                       'missing_tags' VALUE missing_tags,
                       'wrong_location' VALUE wrong_location,
                       'correct_location' VALUE correct_location
                   )
                   FROM stats
               ),
               'by_severity' VALUE (
                   SELECT severity_json 
                   FROM severity_summary 
                   FETCH FIRST 1 ROWS ONLY
               ),
               'overall_status' VALUE (
                   SELECT CASE
                              WHEN missing_tags > 0 THEN 'fail'
                              WHEN wrong_location > 0 THEN 'warning'
                              ELSE 'pass'
                          END
                   FROM stats
               )
           )
    INTO l_summary
    FROM dual;

    RETURN l_summary;
END;
/



CREATE OR REPLACE FUNCTION build_dq_summary_oracle_23ai(p_msg_id VARCHAR2)
RETURN CLOB
IS
    l_summary CLOB;
BEGIN
    WITH rules AS (
        -- Extract paths from rule_json array safely
        SELECT r.rowid AS rid,
               jt.path,
               r.severity
        FROM iso_dq_rules r,
             JSON_TABLE(
                 r.rule_json,
                 '$.rules[*]'
                 COLUMNS (
                     path VARCHAR2(500) PATH '$.path'
                 )
             ) jt
        WHERE r.rule_json IS NOT NULL AND r.rule_json IS JSON
    ),
    details AS (
        -- Extract evaluated rules from dq_details
        SELECT jt.path,
               jt.exists_flg,
               jt.location_status,
               jt.found
        FROM iso_dq_results d,
             JSON_TABLE(
                 d.dq_details,
                 '$[*]'
                 COLUMNS (
                     path VARCHAR2(500) PATH '$.path',
                     exists_flg NUMBER PATH '$.exists',       -- renamed from reserved word
                     location_status VARCHAR2(50) PATH '$.location_status',
                     found VARCHAR2(1000) PATH '$.found'
                 )
             ) jt
        WHERE d.msg_id = p_msg_id
    ),
    combined AS (
        -- Combine rules and evaluated details
        SELECT r.severity,
               r.path,
               NVL(d.exists_flg, 0) AS is_present,
               NVL(d.location_status, 'missing') AS location_status
        FROM rules r
        LEFT JOIN details d
        ON r.path = d.path
    ),
    stats AS (
        -- Overall counts
        SELECT 
            COUNT(*) AS total_rules,
            SUM(CASE WHEN is_present = 1 AND location_status = 'correct' THEN 1 ELSE 0 END) AS rules_passed,
            SUM(CASE WHEN is_present = 0 THEN 1 ELSE 0 END) AS missing_tags,
            SUM(CASE WHEN location_status = 'wrong_location' THEN 1 ELSE 0 END) AS wrong_location,
            SUM(CASE WHEN is_present = 1 AND location_status = 'correct' THEN 1 ELSE 0 END) AS correct_location
        FROM combined
    ),
    severity_summary AS (
        -- Counts per severity
        SELECT JSON_ARRAYAGG(
                   JSON_OBJECT(
                       'severity' VALUE severity,
                       'total' VALUE COUNT(*),
                       'missing' VALUE SUM(CASE WHEN is_present = 0 THEN 1 ELSE 0 END),
                       'wrong_location' VALUE SUM(CASE WHEN location_status = 'wrong_location' THEN 1 ELSE 0 END)
                   )
               ) AS severity_json
        FROM combined
        GROUP BY severity
    )
    SELECT JSON_OBJECT(
               'message_id' VALUE p_msg_id,
               'summary' VALUE (
                   SELECT JSON_OBJECT(
                       'total_rules' VALUE total_rules,
                       'rules_passed' VALUE rules_passed,
                       'missing_tags' VALUE missing_tags,
                       'wrong_location' VALUE wrong_location,
                       'correct_location' VALUE correct_location
                   )
                   FROM stats
               ),
               'by_severity' VALUE (
                   SELECT severity_json
                   FROM severity_summary
                   FETCH FIRST 1 ROWS ONLY
               ),
               'overall_status' VALUE (
                   SELECT CASE
                              WHEN missing_tags > 0 THEN 'fail'
                              WHEN wrong_location > 0 THEN 'warning'
                              ELSE 'pass'
                          END
                   FROM stats
               )
           )
    INTO l_summary
    FROM dual;

    RETURN l_summary;
END;
/



CREATE OR REPLACE FUNCTION build_dq_summary_oracle(p_msg_id VARCHAR2)
RETURN CLOB
IS
    l_summary CLOB;
BEGIN
    WITH rules AS (
        SELECT r.rowid AS rid,
               jt.path,
               r.severity
        FROM iso_dq_rules r,
             JSON_TABLE(
                r.rule_json,
                '$.rules[*]'
                COLUMNS (
                   path VARCHAR2(500) PATH '$.path'
                )
             ) jt
        WHERE JSON_VALID(r.rule_json) = 1
    ),
    details AS (
        SELECT jt.path,
               jt.exists_flg,
               jt.location_status,
               jt.found
        FROM iso_dq_results d,
             JSON_TABLE(
                d.dq_details,
                '$[*]'
                COLUMNS (
                    path VARCHAR2(500) PATH '$.path',
                    exists_flg NUMBER PATH '$.exists',  -- renamed to avoid reserved word
                    location_status VARCHAR2(50) PATH '$.location_status',
                    found VARCHAR2(1000) PATH '$.found'
                )
             ) jt
        WHERE d.msg_id = p_msg_id
    ),
    combined AS (
        SELECT r.severity,
               r.path,
               NVL(d.exists_flg, 0) AS is_present,
               NVL(d.location_status, 'missing') AS location_status
        FROM rules r
        LEFT JOIN details d ON r.path = d.path
    ),
    stats AS (
        SELECT 
            COUNT(*) AS total_rules,
            SUM(CASE WHEN is_present = 1 AND location_status = 'correct' THEN 1 ELSE 0 END) AS rules_passed,
            SUM(CASE WHEN is_present = 0 THEN 1 ELSE 0 END) AS missing_tags,
            SUM(CASE WHEN location_status = 'wrong_location' THEN 1 ELSE 0 END) AS wrong_location,
            SUM(CASE WHEN is_present = 1 AND location_status = 'correct' THEN 1 ELSE 0 END) AS correct_location
        FROM combined
    ),
    severity_summary AS (
        SELECT JSON_ARRAYAGG(
                   JSON_OBJECT(
                       'severity' VALUE severity,
                       'total' VALUE COUNT(*),
                       'missing' VALUE SUM(CASE WHEN is_present = 0 THEN 1 ELSE 0 END),
                       'wrong_location' VALUE SUM(CASE WHEN location_status = 'wrong_location' THEN 1 ELSE 0 END)
                   )
               ) AS severity_json
        FROM combined
        GROUP BY severity
    )
    SELECT JSON_OBJECT(
               'message_id' VALUE p_msg_id,
               'summary' VALUE (
                   SELECT JSON_OBJECT(
                       'total_rules' VALUE total_rules,
                       'rules_passed' VALUE rules_passed,
                       'missing_tags' VALUE missing_tags,
                       'wrong_location' VALUE wrong_location,
                       'correct_location' VALUE correct_location
                   )
                   FROM stats
               ),
               'by_severity' VALUE (
                   SELECT severity_json FROM severity_summary FETCH FIRST 1 ROWS ONLY
               ),
               'overall_status' VALUE (
                   SELECT CASE
                              WHEN missing_tags > 0 THEN 'fail'
                              WHEN wrong_location > 0 THEN 'warning'
                              ELSE 'pass'
                          END
                   FROM stats
               )
           )
    INTO l_summary
    FROM dual;

    RETURN l_summary;
END;
/





UPDATE iso_dq_rules r
SET severity =
    (SELECT CASE
                WHEN jt.path LIKE '%GrpHdr%' THEN 'CRITICAL'
                WHEN jt.path LIKE '%CdtTrfTxInf%' THEN 'MAJOR'
                ELSE 'MINOR'
            END
     FROM JSON_TABLE(
            r.rule_json,
            '$.rules[*]'
            COLUMNS (
                path VARCHAR2(500) PATH '$.path'
            )
     ) jt
     WHERE ROWNUM = 1);   -- if multiple rules per row



ALTER TABLE iso_dq_rules ADD severity VARCHAR2(20);

UPDATE iso_dq_rules
SET severity =
    CASE
        WHEN JSON_VALUE(rule_json, '$.path') LIKE '%GrpHdr%' THEN 'CRITICAL'
        WHEN JSON_VALUE(rule_json, '$.path') LIKE '%CdtTrfTxInf%' THEN 'MAJOR'
        ELSE 'MINOR'
    END;


UPDATE iso_dq_rules
SET severity =
    CASE
        WHEN path LIKE '%GrpHdr%' THEN 'CRITICAL'
        WHEN path LIKE '%CdtTrfTxInf%' THEN 'MAJOR'
        ELSE 'MINOR'
    END;

CREATE OR REPLACE FUNCTION build_dq_summary_oracle(p_msg_id VARCHAR2)
RETURN CLOB
IS
    l_summary CLOB;
BEGIN
    WITH rules AS (
        SELECT r.path,
               r.severity
        FROM iso_dq_rules r
    ),
    details AS (
        SELECT jt.path,
               jt.exists,
               jt.location_status,
               jt.found
        FROM iso_dq_results d,
             JSON_TABLE(
                 d.dq_details,
                 '$[*]'
                 COLUMNS (
                    path            VARCHAR2(500)  PATH '$.path',
                    exists          NUMBER         PATH '$.exists',
                    location_status VARCHAR2(50)   PATH '$.location_status',
                    found           VARCHAR2(1000) PATH '$.found'
                 )
             ) jt
        WHERE d.msg_id = p_msg_id
    ),
    combined AS (
        SELECT r.severity,
               r.path,
               NVL(d.exists, 0) AS exists,
               NVL(d.location_status, 'missing') AS location_status
        FROM rules r
        LEFT JOIN details d
               ON r.path = d.path
    ),
    stats AS (
        SELECT 
            COUNT(*) AS total_rules,
            SUM(CASE WHEN exists = 1 AND location_status = 'correct' THEN 1 ELSE 0 END) AS rules_passed,
            SUM(CASE WHEN exists = 0 THEN 1 ELSE 0 END) AS missing_tags,
            SUM(CASE WHEN location_status = 'wrong_location' THEN 1 ELSE 0 END) AS wrong_location,
            SUM(CASE WHEN exists = 1 AND location_status = 'correct' THEN 1 ELSE 0 END) AS correct_location
        FROM combined
    ),
    severity_summary AS (
        SELECT JSON_ARRAYAGG(
                   JSON_OBJECT(
                       'severity' VALUE severity,
                       'total' VALUE COUNT(*),
                       'missing' VALUE SUM(CASE WHEN exists = 0 THEN 1 ELSE 0 END),
                       'wrong_location' VALUE SUM(CASE WHEN location_status = 'wrong_location' THEN 1 ELSE 0 END)
                   )
               ) AS severity_json
        FROM combined
        GROUP BY 1
    )
    SELECT JSON_OBJECT(
               'message_id' VALUE p_msg_id,
               'summary' VALUE (
                   SELECT JSON_OBJECT(
                       'total_rules' VALUE total_rules,
                       'rules_passed' VALUE rules_passed,
                       'missing_tags' VALUE missing_tags,
                       'wrong_location' VALUE wrong_location,
                       'correct_location' VALUE correct_location
                   )
                   FROM stats
               ),
               'by_severity' VALUE (
                   SELECT severity_json FROM severity_summary FETCH FIRST 1 ROWS ONLY
               ),
               'overall_status' VALUE (
                   SELECT CASE
                              WHEN missing_tags > 0 THEN 'fail'
                              WHEN wrong_location > 0 THEN 'warning'
                              ELSE 'pass'
                          END
                   FROM stats
               )
           )
    INTO l_summary
    FROM dual;

    RETURN l_summary;

END;
/




CREATE OR REPLACE FUNCTION validate_iso_message (
    p_xml_msg  IN CLOB,
    p_xsd_name IN VARCHAR2
) RETURN CLOB
AS
    l_rules_json   CLOB;
    l_result       VARCHAR2(32767) := '[';  -- use VARCHAR2 instead of CLOB
    l_err          VARCHAR2(4000);
    l_ns           VARCHAR2(4000);
    l_first        BOOLEAN := TRUE;
BEGIN
    --------------------------------------------------------------------
    -- 1. Extract default namespace from XML
    --------------------------------------------------------------------
    l_ns := REGEXP_SUBSTR(p_xml_msg,'xmlns\s*=\s*"(.*?)"',1,1,NULL,1);
    IF l_ns IS NULL THEN
        l_ns := REGEXP_SUBSTR(p_xml_msg,'xmlns\s*=\s*''(.*?)''',1,1,NULL,1);
    END IF;

    --------------------------------------------------------------------
    -- 2. Load rules
    --------------------------------------------------------------------
    BEGIN
        SELECT rule_json
        INTO l_rules_json
        FROM iso_dq_rules
        WHERE xsd_name = p_xsd_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN '{"error":"No rules found for XSD","xsd_name":"' || p_xsd_name || '"}';
    END;

    --------------------------------------------------------------------
    -- 3. Iterate rules and build JSON manually
    --------------------------------------------------------------------
    FOR r IN (
        SELECT path, required
        FROM JSON_TABLE(
            l_rules_json,
            '$.rules[*]'
            COLUMNS (
                path     VARCHAR2(4000) PATH '$.path',
                required NUMBER        PATH '$.required'
            )
        )
    ) LOOP
        IF NOT l_first THEN
            l_result := l_result || ',';  -- separate JSON objects
        ELSE
            l_first := FALSE;
        END IF;

        l_result := l_result ||
            '{"path":"' || REPLACE(r.path,'"','\"') || '"' ||
            ',"required":' || NVL(TO_CHAR(r.required),'null') ||
            ',"exists":' || EXISTSNode_NS(XMLTYPE(p_xml_msg),
                                          CASE WHEN SUBSTR(r.path,1,1)='/' THEN r.path ELSE '/'||r.path END,
                                          l_ns) ||
            ',"valid":"' || CASE
                                WHEN r.required = 1 AND 
                                     EXISTSNode_NS(XMLTYPE(p_xml_msg),
                                                   CASE WHEN SUBSTR(r.path,1,1)='/' THEN r.path ELSE '/'||r.path END,
                                                   l_ns) = 0
                                THEN 'missing'
                                ELSE 'ok'
                             END || '"}';
    END LOOP;

    -- close JSON array
    l_result := l_result || ']';

    RETURN l_result;

EXCEPTION
    WHEN OTHERS THEN
        l_err := SQLERRM;
        RETURN '{"error":"' || l_err || '"}';
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
    l_result := EMPTY_CLOB(); -- initialize empty CLOB

    -- Extract default namespace
    l_ns := REGEXP_SUBSTR(p_xml_msg,'xmlns\s*=\s*"(.*?)"',1,1,NULL,1);
    IF l_ns IS NULL THEN
        l_ns := REGEXP_SUBSTR(p_xml_msg,'xmlns\s*=\s*''(.*?)''',1,1,NULL,1);
    END IF;

    -- Load rules
    BEGIN
        SELECT rule_json INTO l_rules_json
        FROM iso_dq_rules
        WHERE xsd_name = p_xsd_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN TO_CLOB('{"error":"No rules found for XSD","xsd_name":"' || p_xsd_name || '"}');
    END;

    -- Start JSON array
    DBMS_LOB.APPEND(l_result,'[');
    DECLARE
        l_first BOOLEAN := TRUE;
    BEGIN
        FOR r IN (
            SELECT path, required
            FROM JSON_TABLE(
                l_rules_json,
                '$.rules[*]'
                COLUMNS (
                    path     CLOB PATH '$.path',
                    required NUMBER PATH '$.required'
                )
            )
        ) LOOP
            IF NOT l_first THEN
                DBMS_LOB.APPEND(l_result,',');
            ELSE
                l_first := FALSE;
            END IF;

            DBMS_LOB.APPEND(l_result,
                '{"path":"' || REPLACE(r.path,'"','\"') || '"' ||
                ',"required":' || NVL(TO_CHAR(r.required),'null') ||
                ',"exists":' || EXISTSNode_NS(XMLTYPE(p_xml_msg),
                                              CASE WHEN SUBSTR(r.path,1,1)='/' THEN r.path ELSE '/'||r.path END,
                                              l_ns) ||
                ',"valid":"' || CASE
                                    WHEN r.required = 1 AND 
                                         EXISTSNode_NS(XMLTYPE(p_xml_msg),
                                                       CASE WHEN SUBSTR(r.path,1,1)='/' THEN r.path ELSE '/'||r.path END,
                                                       l_ns) = 0
                                    THEN 'missing'
                                    ELSE 'ok'
                                 END || '"}'
            );
        END LOOP;
    END;

    DBMS_LOB.APPEND(l_result,']'); -- close JSON array

    RETURN l_result;

EXCEPTION
    WHEN OTHERS THEN
        l_err := SQLERRM;
        RETURN TO_CLOB('{"error":"' || l_err || '"}');
END;
/


CREATE OR REPLACE FUNCTION validate_iso_message (
    p_xml_msg  IN CLOB,
    p_xsd_name IN VARCHAR2
) RETURN CLOB
AS
    l_rules_json   CLOB;
    l_result       CLOB := '[';  -- start JSON array
    l_err          VARCHAR2(4000);
    l_ns           VARCHAR2(4000);
    l_first        BOOLEAN := TRUE;

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
    -- 2. Load rules JSON
    --------------------------------------------------------------------
    BEGIN
        SELECT rule_json
        INTO l_rules_json
        FROM iso_dq_rules
        WHERE xsd_name = p_xsd_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN TO_CLOB('{"error":"No rules found for XSD","xsd_name":"' || p_xsd_name || '"}');
    END;

    --------------------------------------------------------------------
    -- 3. Iterate rules and build JSON manually
    --------------------------------------------------------------------
    FOR r IN (
        SELECT path, required
        FROM JSON_TABLE(
            l_rules_json,
            '$.rules[*]'
            COLUMNS (
                path     VARCHAR2(400) PATH '$.path',
                required NUMBER        PATH '$.required'
            )
        ) 
    ) LOOP
        IF NOT l_first THEN
            l_result := l_result || ',';  -- separate JSON objects
        ELSE
            l_first := FALSE;
        END IF;

        l_result := l_result ||
            '{"path":"' || r.path || '"' ||
            ',"required":' || NVL(TO_CHAR(r.required),'null') ||
            ',"exists":' || EXISTSNode_NS(
                                XMLTYPE(p_xml_msg),
                                CASE WHEN SUBSTR(r.path,1,1)='/' THEN r.path ELSE '/'||r.path END,
                                l_ns
                             ) ||
            ',"valid":"' || CASE
                                WHEN r.required = 1 AND 
                                     EXISTSNode_NS(
                                         XMLTYPE(p_xml_msg),
                                         CASE WHEN SUBSTR(r.path,1,1)='/' THEN r.path ELSE '/'||r.path END,
                                         l_ns
                                     ) = 0
                                THEN 'missing'
                                ELSE 'ok'
                             END || '"}';
    END LOOP;

    -- close JSON array
    l_result := l_result || ']';

    RETURN l_result;

EXCEPTION
    WHEN OTHERS THEN
        l_err := SQLERRM;
        RETURN TO_CLOB('{"error":"' || l_err || '"}');
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





