CREATE TABLE iso_xsd_repository (
    xsd_id            NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    msg_family        VARCHAR2(50) NOT NULL,   
        -- e.g., 'pacs.008', 'pacs.009', 'camt.053'

    version_no        VARCHAR2(30) NOT NULL,
        -- e.g., '001.10', '001.09', '002.06'

    schema_namespace  VARCHAR2(200) NOT NULL,
        -- e.g., 'urn:iso:std:iso:20022:tech:xsd:pacs.008.001.10'

    root_element      VARCHAR2(100),
        -- e.g., 'Document'

    xsd_content       XMLTYPE NOT NULL,
        -- full XSD stored as an XML document

    file_name         VARCHAR2(300),
        -- original filename, e.g. pacs.008.001.10.xsd

    upload_date       DATE DEFAULT SYSDATE,
    uploaded_by       VARCHAR2(50) DEFAULT USER,

    is_active         CHAR(1) DEFAULT 'Y' CHECK (is_active IN ('Y','N')),

    sha256_hash       VARCHAR2(64),
        -- used to detect duplicates or tampering

    parent_xsd_id     NUMBER,
        -- reference to another XSD (imports, includes)

    CONSTRAINT fk_xsd_parent
        FOREIGN KEY (parent_xsd_id) REFERENCES iso_xsd_repository(xsd_id)
);

INSERT INTO iso_xsd_repository
    (msg_family, version_no, schema_namespace, root_element, xsd_content, file_name)
VALUES
    ('pacs.008', '001.10',
     'urn:iso:std:iso:20022:tech:xsd:pacs.008.001.10',
     'Document',
     XMLTYPE(bfilename('XSD_DIR', 'pacs.008.001.10.xsd'), nls_charset_id('AL32UTF8')),
     'pacs.008.001.10.xsd');


