-- Kingbase CDC rich-type demo table
-- Target schema/table: form.t_cdc_rich_types

CREATE SCHEMA IF NOT EXISTS form;

DROP TABLE IF EXISTS form.t_cdc_rich_types;

CREATE TABLE form.t_cdc_rich_types (
    id                  bigint PRIMARY KEY,
    c_tinyint           tinyint,
    c_smallint          smallint,
    c_mediumint         mediumint,
    c_int3              int3,
    c_integer           integer,
    c_bigint            bigint,
    c_number            number(20, 4),
    c_decimal           decimal(18, 6),
    c_real              real,
    c_float             float,
    c_binary_float      binary_float,
    c_binary_double     binary_double,
    c_bool              boolean,
    c_char              char(1),
    c_nchar             nchar(10),
    c_varchar           varchar(64),
    c_varchar2          varchar2(64),
    c_nvarchar2         nvarchar2(64),
    c_text              text,
    c_date              date,
    c_time              time,
    c_timetz            timetz,
    c_datetime          datetime,
    c_timestamp         timestamp,
    c_timestamptz       timestamptz,
    c_interval          interval,
    c_json              json,
    c_jsonb             jsonb,
    c_bytea             bytea,
    c_bit               bit(8),
    c_varbit            varbit(16),
    c_created_at        timestamptz DEFAULT now()
);


INSERT INTO form.t_cdc_rich_types (
    id,
    c_tinyint, c_smallint, c_mediumint, c_int3, c_integer, c_bigint,
    c_number, c_decimal, c_real, c_float, c_binary_float, c_binary_double,
    c_bool, c_char, c_nchar, c_varchar, c_varchar2, c_nvarchar2, c_text,
    c_date, c_time, c_timetz, c_datetime, c_timestamp, c_timestamptz, c_interval,
    c_json, c_jsonb, c_bytea, c_bit, c_varbit
) VALUES (
             1,
             1, 2, 3, 4, 5, 9007199254740991,
             1234567890123456.1234, 123456789012.123456, 3.14, 2.71828, 1.25, 2.5,
             true, 'A', '中文测试', 'varchar_test', 'varchar2_test', 'nvarchar2中文', 'text_test',
             DATE '2026-02-10',
             TIME '12:34:56',
             '12:34:56+08:00'::timetz,
             TIMESTAMP '2026-02-10 12:34:56',
             TIMESTAMP '2026-02-10 12:34:56',
             '2026-02-10 12:34:56+08:00'::timestamptz,
             INTERVAL '1 day 02:03:04',
             '{"k":"v","n":123}', '{"k":"v","n":123}', E'\\xDEADBEEF',
             B'10101010', B'1010101010101010'
         );
