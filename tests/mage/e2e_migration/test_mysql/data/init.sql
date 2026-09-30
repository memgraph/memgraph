-- Initialize test database schema
USE test_db;

-- Create dummy table with all MySQL data types
CREATE TABLE dummy_table (
    -- Numeric types
    id INT PRIMARY KEY AUTO_INCREMENT,
    tinyint_col TINYINT,
    smallint_col SMALLINT,
    mediumint_col MEDIUMINT,
    bigint_col BIGINT,
    decimal_col DECIMAL(10,2),
    float_col FLOAT,
    double_col DOUBLE,
    bit_col BIT(8),

    -- Date and time types
    date_col DATE,
    time_col TIME,
    datetime_col DATETIME,
    timestamp_col TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    year_col YEAR,

    -- String types
    char_col CHAR(10),
    varchar_col VARCHAR(100),
    binary_col BINARY(10),
    varbinary_col VARBINARY(100),
    tinyblob_col TINYBLOB,
    tinytext_col TINYTEXT,
    blob_col BLOB,
    text_col TEXT,
    mediumblob_col MEDIUMBLOB,
    mediumtext_col MEDIUMTEXT,
    longblob_col LONGBLOB,
    longtext_col LONGTEXT,
    enum_col ENUM('small', 'medium', 'large'),
    set_col SET('red', 'green', 'blue'),

    -- JSON type
    json_col JSON,

    -- Boolean type
    boolean_col BOOLEAN,
    bool_col BOOL,

    -- Spatial types (should fail)
    geometry_col GEOMETRY,
    point_col POINT
);
