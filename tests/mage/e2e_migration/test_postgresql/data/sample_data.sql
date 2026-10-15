-- PostgreSQL sample data for migration testing
-- Inserts one row with all data types

INSERT INTO dummy_table (
    smallint_col, integer_col, bigint_col, decimal_col, numeric_col, real_col,
    double_precision_col, money_col, char_col, varchar_col, text_col, bytea_col,
    timestamp_col, timestamp_with_timezone_col, date_col, time_col,
    time_with_timezone_col, interval_col, boolean_col, point_col,
    lseg_col, box_col, path_col, polygon_col, circle_col, cidr_col, inet_col,
    macaddr_col, bit_col, bit_varying_col, uuid_col, xml_col, json_col,
    jsonb_col, array_col, enum_col, range_col
) VALUES (
    32767,                                    -- smallint_col
    2147483647,                              -- integer_col
    9223372036854775807,                     -- bigint_col
    123.45,                                  -- decimal_col
    678.90,                                  -- numeric_col
    1.23,                                    -- real_col
    4.56,                                    -- double_precision_col
    100.50,                                  -- money_col
    'char_test',                             -- char_col
    'varchar_test',                          -- varchar_col
    'This is a text field',                  -- text_col
    E'\\x48656C6C6F',                        -- bytea_col (Hello in hex)
    '2023-12-25 10:30:45',                  -- timestamp_col
    '2023-12-25 10:30:45+00',               -- timestamp_with_timezone_col
    '2023-12-25',                           -- date_col
    '10:30:45',                             -- time_col
    '10:30:45+00',                          -- time_with_timezone_col
    '1 day 2 hours 3 minutes',              -- interval_col
    true,                                    -- boolean_col
    '(1,2)',                                -- point_col
    '[(1,2),(3,4)]',                        -- lseg_col
    '(1,2),(3,4)',                          -- box_col
    '[(1,2),(3,4),(5,6)]',                  -- path_col
    '((1,2),(3,4),(5,6),(1,2))',           -- polygon_col
    '<(1,2),3>',                            -- circle_col
    '192.168.1.0/24',                       -- cidr_col
    '192.168.1.1',                          -- inet_col
    '08:00:2b:01:02:03',                    -- macaddr_col
    B'10101010',                            -- bit_col
    B'10101010',                            -- bit_varying_col
    '550e8400-e29b-41d4-a716-446655440000', -- uuid_col
    '<root><item>test</item></root>',       -- xml_col
    '{"key": "value", "number": 42}',       -- json_col
    '{"key": "value", "number": 42}',       -- jsonb_col
    '{1,2,3,4,5}',                          -- array_col
    'option1',                              -- enum_col
    '[1,10)'                                -- range_col
);
