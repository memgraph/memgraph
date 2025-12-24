-- Insert sample data for testing migration
USE test_db;

-- Insert one row with all MySQL data types
INSERT INTO dummy_table (
    -- Numeric types
    tinyint_col,
    smallint_col,
    mediumint_col,
    bigint_col,
    decimal_col,
    float_col,
    double_col,
    bit_col,

    -- Date and time types
    date_col,
    time_col,
    datetime_col,
    year_col,

    -- String types
    char_col,
    varchar_col,
    binary_col,
    varbinary_col,
    tinyblob_col,
    tinytext_col,
    blob_col,
    text_col,
    mediumblob_col,
    mediumtext_col,
    longblob_col,
    longtext_col,
    enum_col,
    set_col,

    -- JSON type
    json_col,

    -- Boolean types
    boolean_col,
    bool_col,

    -- Spatial types (should fail)
    geometry_col,
    point_col
) VALUES (
    -- Numeric values
    127,                    -- tinyint_col
    32767,                  -- smallint_col
    8388607,                -- mediumint_col
    9223372036854775807,    -- bigint_col
    123.45,                 -- decimal_col
    3.14159,                -- float_col
    2.718281828,            -- double_col
    b'10101010',            -- bit_col

    -- Date and time values
    '2023-12-25',           -- date_col
    '14:30:45',             -- time_col
    '2023-12-25 14:30:45',  -- datetime_col
    2023,                   -- year_col

    -- String values
    'Hello',                -- char_col
    'World of MySQL',       -- varchar_col
    'Binary',               -- binary_col
    'VarBinary',            -- varbinary_col
    'TinyBlob',             -- tinyblob_col
    'TinyText',             -- tinytext_col
    'BlobData',             -- blob_col
    'This is a longer text field that can contain more data', -- text_col
    'MediumBlob',           -- mediumblob_col
    'MediumText',           -- mediumtext_col
    'LongBlob',             -- longblob_col
    'LongText',             -- longtext_col
    'medium',               -- enum_col
    'red,green',            -- set_col

    -- JSON value
    '{"name": "test", "value": 123, "active": true}', -- json_col

    -- Boolean values
    TRUE,                   -- boolean_col
    FALSE,                  -- bool_col

    -- Spatial values (should fail)
    ST_GeomFromText('POINT(1 1)'),  -- geometry_col
    ST_GeomFromText('POINT(2 2)')   -- point_col
);
