-- STAGE--

CREATE TABLE IF NOT EXISTS sttgaz.stage_workflow_status (
    id AUTO_INCREMENT NOT NULL,
    workflow_key  VARCHAR NOT NULL,
    workflow_settings  VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS sttgaz.stage_checks(
	id AUTO_INCREMENT,
	table_name VARCHAR(100),
	check_name VARCHAR(100),
    ts TIMESTAMP,
	check_result BOOLEAN
);

CREATE TABLE IF NOT EXISTS sttgaz.stage_mdaudit_checks (
    id BIGINT NOT NULL,
    template_id BIGINT NOT NULL,
    template_name VARCHAR(2000) NOT NULL,
    shop_id BIGINT NOT NULL,
    shop_sap VARCHAR NOT NULL,
    shop_locality VARCHAR(2000) NOT NULL,
    region_id INT NOT NULL,
    region_name VARCHAR(2000) NOT NULL,
    division_id INT NOT NULL,
    division_name VARCHAR(2000) NOT NULL,
    resolver_id BIGINT NOT NULL,
    resolver_first_name VARCHAR,
    resolver_last_name VARCHAR,
    resolve_date DATE,
    start_time TIMESTAMP,
    finish_time TIMESTAMP,
    last_modified_at TIMESTAMP NOT NULL,
    grade NUMERIC(6,3) NOT NULL,
    comment VARCHAR(8000),
    status VARCHAR NOT NULL,

    CONSTRAINT stage_mdaudit_checks_unique UNIQUE(id, shop_id) 
);

CREATE TABLE IF NOT EXISTS sttgaz.stage_mdaudit_answers(
    id BIGINT NOT NULL UNIQUE,
    check_id BIGINT NOT NULL,
    shop_id BIGINT NOT NULL,
    question_id BIGINT NOT NULL,
    name VARCHAR(3000) NOT NULL,
    answer NUMERIC(6,3) NOT NULL,
    weight INT NOT NULL,
    comment VARCHAR(8000),

    CONSTRAINT stage_mdaudit_answers_id_unique UNIQUE(id) 
);

CREATE TABLE IF NOT EXISTS sttgaz.stage_mdaudit_dirs(
    id BIGINT NOT NULL UNIQUE,
    active BOOLEAN,
    login VARCHAR(100),
    firstName VARCHAR(100),
    lastName VARCHAR(100),
    position VARCHAR(2000),
    email VARCHAR(250),
    level VARCHAR(250),
    businessDirId BIGINT,
    lang VARCHAR,
    invited BOOLEAN       
);

CREATE TABLE IF NOT EXISTS sttgaz.stage_mdaudit_shops(
    id BIGINT NOT NULL UNIQUE,
    active BOOLEAN,
    sap VARCHAR(100),
    locality VARCHAR(6000),
    address VARCHAR(6000),
    city VARCHAR(255),
    latitude Numeric(20,10),
    longitude Numeric(20,10),
    regionid BIGINT,
    clusterId BIGINT,
    timeZoneId VARCHAR  
);

-- DDS --

CREATE TABLE IF NOT EXISTS sttgaz.dds_mdaudit_regions(
    id AUTO_INCREMENT PRIMARY KEY,
    region_id BIGINT NOT NULL UNIQUE,
    region_name VARCHAR(2000) NOT NULL
);


CREATE TABLE IF NOT EXISTS sttgaz.dds_mdaudit_shops(
    id AUTO_INCREMENT PRIMARY KEY,
    shop_id BIGINT NOT NULL UNIQUE,
    active BOOLEAN,
    sap VARCHAR(100),
    locality VARCHAR(6000),
    address VARCHAR(6000),
    city VARCHAR(255),
    latitude Numeric(20,10),
    longitude Numeric(20,10),
    region_id BIGINT REFERENCES sttgaz.dds_mdaudit_regions(id)
);


CREATE TABLE IF NOT EXISTS sttgaz.dds_mdaudit_divisions(
    id AUTO_INCREMENT PRIMARY KEY,
    division_id BIGINT NOT NULL UNIQUE,
    division_name VARCHAR(2000) NOT NULL
);

CREATE TABLE IF NOT EXISTS sttgaz.dds_mdaudit_templates(
    id AUTO_INCREMENT PRIMARY KEY,
    template_id BIGINT NOT NULL UNIQUE,
    template_name VARCHAR(2000) NOT NULL
);

CREATE TABLE IF NOT EXISTS sttgaz.dds_mdaudit_resolvers(
    id AUTO_INCREMENT PRIMARY KEY,
    resolver_id BIGINT NOT NULL UNIQUE,
    resolver_first_name VARCHAR,
    resolver_last_name VARCHAR
);



CREATE TABLE IF NOT EXISTS sttgaz.dds_mdaudit_checks(
    id AUTO_INCREMENT PRIMARY KEY,
    check_id BIGINT NOT NULL UNIQUE,
    template_id BIGINT NOT NULL REFERENCES sttgaz.dds_mdaudit_templates(id),
    shop_id BIGINT NOT NULL REFERENCES sttgaz.dds_mdaudit_shops(id),
    division_id INT NOT NULL REFERENCES sttgaz.dds_mdaudit_divisions(id),
    resolver_id BIGINT NOT NULL REFERENCES sttgaz.dds_mdaudit_resolvers(id),
    resolve_date DATE,
    start_time TIMESTAMP,
    finish_time TIMESTAMP,
    last_modified_at TIMESTAMP NOT NULL,
    grade NUMERIC(6,3),
    comment VARCHAR(8000),
    status VARCHAR NOT NULL,

    CONSTRAINT stage_mdaudit_checks_unique UNIQUE(id, shop_id) 
);


CREATE TABLE IF NOT EXISTS sttgaz.dds_mdaudit_answers(
    id AUTO_INCREMENT PRIMARY KEY,
    answer_id BIGINT NOT NULL UNIQUE,
    check_id BIGINT NOT NULL REFERENCES sttgaz.dds_mdaudit_checks(id),
    question_id BIGINT NOT NULL,
    name VARCHAR(3000) NOT NULL,
    answer NUMERIC(6,3) NOT NULL,
    weight INT NOT NULL,
    comment VARCHAR(8000)
);










