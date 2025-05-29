SELECT
    id,
    CASE
        WHEN INSTR(message_clob, '<FwdgAgt>') > 0 THEN 'Forwarding Agent Case'
        ELSE 'Debtor Agent Case'
    END AS message_type
FROM
    pain_messages;