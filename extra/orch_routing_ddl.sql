-- =====================================================================
-- Oracle DDL generated from ER diagram
-- Tables: ref_region, orch_group_region, orch_execution_target,
--         orch_recalc_report_config (existing - ALTER only), orch_routing_rule
-- Flags: is_active / is_deleted are NUMBER(1) 0/1 (Oracle has no BOOLEAN before 23ai)
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. ref_region
-- ---------------------------------------------------------------------
CREATE TABLE ref_region (
    region        VARCHAR2(50 CHAR)   NOT NULL,
    description   VARCHAR2(500 CHAR),
    is_active     NUMBER(1)           DEFAULT 1 NOT NULL,
    is_deleted    NUMBER(1)           DEFAULT 0 NOT NULL,
    created_by    VARCHAR2(100 CHAR)  NOT NULL,
    created_at    TIMESTAMP           DEFAULT SYSTIMESTAMP NOT NULL,
    updated_by    VARCHAR2(100 CHAR),
    updated_at    TIMESTAMP,
    CONSTRAINT pk_ref_region PRIMARY KEY (region),
    CONSTRAINT ck_ref_region_active  CHECK (is_active  IN (0, 1)),
    CONSTRAINT ck_ref_region_deleted CHECK (is_deleted IN (0, 1))
);

-- ---------------------------------------------------------------------
-- 2. orch_group_region
--    resource_group is also referenced LOGICALLY (no FK) from
--    orch_recalc_report_config.resource_group
-- ---------------------------------------------------------------------
CREATE TABLE orch_group_region (
    resource_group VARCHAR2(100 CHAR) NOT NULL,
    region         VARCHAR2(50 CHAR)  NOT NULL,
    description    VARCHAR2(500 CHAR),
    is_active      NUMBER(1)          DEFAULT 1 NOT NULL,
    is_deleted     NUMBER(1)          DEFAULT 0 NOT NULL,
    created_by     VARCHAR2(100 CHAR) NOT NULL,
    created_at     TIMESTAMP          DEFAULT SYSTIMESTAMP NOT NULL,
    updated_by     VARCHAR2(100 CHAR),
    updated_at     TIMESTAMP,
    CONSTRAINT pk_orch_group_region PRIMARY KEY (resource_group),
    CONSTRAINT fk_ogr_region FOREIGN KEY (region) REFERENCES ref_region (region),
    CONSTRAINT ck_ogr_active  CHECK (is_active  IN (0, 1)),
    CONSTRAINT ck_ogr_deleted CHECK (is_deleted IN (0, 1))
);

CREATE INDEX ix_ogr_region ON orch_group_region (region);

-- ---------------------------------------------------------------------
-- 3. orch_execution_target
-- ---------------------------------------------------------------------
CREATE TABLE orch_execution_target (
    id             NUMBER GENERATED ALWAYS AS IDENTITY NOT NULL,
    guid           VARCHAR2(36 CHAR)   DEFAULT LOWER(REGEXP_REPLACE(RAWTOHEX(SYS_GUID()),
                       '([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})',
                       '\1-\2-\3-\4-\5', 1, 1, 'i')) NOT NULL,
    platform       VARCHAR2(50 CHAR)   NOT NULL,
    region         VARCHAR2(50 CHAR)   NOT NULL,
    base_url       VARCHAR2(500 CHAR)  NOT NULL,
    protocol       VARCHAR2(20 CHAR)   NOT NULL,
    conn_user_id   VARCHAR2(200 CHAR),
    conn_password  VARCHAR2(1000 CHAR),           -- stored encrypted (app-level)
    description    VARCHAR2(500 CHAR),
    is_active      NUMBER(1)           DEFAULT 1 NOT NULL,
    is_deleted     NUMBER(1)           DEFAULT 0 NOT NULL,
    created_by     VARCHAR2(100 CHAR)  NOT NULL,
    created_at     TIMESTAMP           DEFAULT SYSTIMESTAMP NOT NULL,
    updated_by     VARCHAR2(100 CHAR),
    updated_at     TIMESTAMP,
    CONSTRAINT pk_orch_execution_target PRIMARY KEY (id),
    CONSTRAINT uq_oet_guid UNIQUE (guid),
    CONSTRAINT uq_oet_platform_region UNIQUE (platform, region),
    CONSTRAINT fk_oet_region FOREIGN KEY (region) REFERENCES ref_region (region),
    CONSTRAINT ck_oet_active  CHECK (is_active  IN (0, 1)),
    CONSTRAINT ck_oet_deleted CHECK (is_deleted IN (0, 1))
);

-- ---------------------------------------------------------------------
-- 4. orch_recalc_report_config (EXISTING table)
--    Only needed if report_key virtual column is not there yet.
--    ASSUMPTION: report_key = concatenation of the natural-key columns.
--    Adjust the expression to match your real definition.
-- ---------------------------------------------------------------------
ALTER TABLE orch_recalc_report_config ADD (
    report_key VARCHAR2(1000 CHAR) INVISIBLE
        GENERATED ALWAYS AS (resource_group || '|' || resource_name || '|' ||
                             schedule || '|' || sub_schedule) VIRTUAL
);

ALTER TABLE orch_recalc_report_config
    ADD CONSTRAINT uq_orcrc_report_key UNIQUE (report_key);

-- ---------------------------------------------------------------------
-- 5. orch_routing_rule
--    report_key NULL = global rule
-- ---------------------------------------------------------------------
CREATE TABLE orch_routing_rule (
    id           NUMBER GENERATED ALWAYS AS IDENTITY NOT NULL,
    guid         VARCHAR2(36 CHAR)   DEFAULT LOWER(REGEXP_REPLACE(RAWTOHEX(SYS_GUID()),
                     '([0-9a-f]{8})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{4})([0-9a-f]{12})',
                     '\1-\2-\3-\4-\5', 1, 1, 'i')) NOT NULL,
    report_key   VARCHAR2(1000 CHAR),
    target_id    NUMBER              NOT NULL,
    description  VARCHAR2(500 CHAR),
    is_active    NUMBER(1)           DEFAULT 1 NOT NULL,
    is_deleted   NUMBER(1)           DEFAULT 0 NOT NULL,
    created_by   VARCHAR2(100 CHAR)  NOT NULL,
    created_at   TIMESTAMP           DEFAULT SYSTIMESTAMP NOT NULL,
    updated_by   VARCHAR2(100 CHAR),
    updated_at   TIMESTAMP,
    CONSTRAINT pk_orch_routing_rule PRIMARY KEY (id),
    CONSTRAINT uq_orr_guid UNIQUE (guid),
    CONSTRAINT uq_orr_report_key UNIQUE (report_key),
    CONSTRAINT fk_orr_report_key FOREIGN KEY (report_key)
        REFERENCES orch_recalc_report_config (report_key),
    CONSTRAINT fk_orr_target FOREIGN KEY (target_id)
        REFERENCES orch_execution_target (id),
    CONSTRAINT ck_orr_active  CHECK (is_active  IN (0, 1)),
    CONSTRAINT ck_orr_deleted CHECK (is_deleted IN (0, 1))
);

CREATE INDEX ix_orr_target ON orch_routing_rule (target_id);
