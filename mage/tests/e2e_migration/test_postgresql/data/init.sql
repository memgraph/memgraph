-- PostgreSQL initialization script for migration testing
-- Creates a table with all common PostgreSQL data types

-- Create the test database (already created by environment variable)
-- \c test_db;

-- Create a table with all common PostgreSQL data types
CREATE TABLE IF NOT EXISTS dummy_table (
    id SERIAL PRIMARY KEY,
    smallint_col SMALLINT,
    integer_col INTEGER,
    bigint_col BIGINT,
    decimal_col DECIMAL(10,2),
    numeric_col NUMERIC(10,2),
    real_col REAL,
    double_precision_col DOUBLE PRECISION,
    money_col MONEY,
    char_col CHAR(10),
    varchar_col VARCHAR(255),
    text_col TEXT,
    bytea_col BYTEA,
    timestamp_col TIMESTAMP,
    timestamp_with_timezone_col TIMESTAMP WITH TIME ZONE,
    date_col DATE,
    time_col TIME,
    time_with_timezone_col TIME WITH TIME ZONE,
    interval_col INTERVAL,
    boolean_col BOOLEAN,
    point_col POINT,
    lseg_col LSEG,
    box_col BOX,
    path_col PATH,
    polygon_col POLYGON,
    circle_col CIRCLE,
    cidr_col CIDR,
    inet_col INET,
    macaddr_col MACADDR,
    bit_col BIT(8),
    bit_varying_col BIT VARYING(8),
    uuid_col UUID,
    xml_col XML,
    json_col JSON,
    jsonb_col JSONB,
    array_col INTEGER[],
    enum_col TEXT, -- PostgreSQL doesn't have ENUM in this context, using TEXT
    range_col INT4RANGE
);
