drop table all_types if exists;

create table all_types(
      id                   integer identity
     ,c_bigint             bigint
     ,c_binary             binary(255)
     ,c_boolean            boolean
     ,c_char               char(255)
     ,c_date               date
     ,c_decimal            decimal
     ,c_double             double
     ,c_float              float
     ,c_integer            integer
     ,c_longvarbinary      longvarbinary
     ,c_longvarchar        longvarchar
     ,c_object             object
     ,c_real               real
     ,c_smallint           smallint
     ,c_time               time
     ,c_timestamp          timestamp
     ,c_tinyint            tinyint
     ,c_varbinary          varbinary(255)
     ,c_varchar            varchar(255)
     ,c_blob               blob(16)
     ,c_clob               clob(16)
     ,c_array              integer array[4]
);

insert into all_types(
     id
     ,c_bigint
     ,c_binary
     ,c_boolean
     ,c_char
     ,c_date
     ,c_decimal
     ,c_double
     ,c_float
     ,c_integer
     ,c_longvarbinary
     ,c_longvarchar
     ,c_object
     ,c_real
     ,c_smallint
     ,c_time
     ,c_timestamp
     ,c_tinyint
     ,c_varbinary
     ,c_varchar
     ,c_blob
     ,c_clob
     ,c_array
) values (
    1,
    123456789,
    X'0123456789ABCDEF',
    true,
    'CHAR  ',
    current_date,
    0.1234556789,
    0.1234556789,
    0.1234556789,
    123456789,
    X'123456789ADBDEF0123456789ADBDEF0123456789ADBDEF0123456789ADBDEF0123456789ADBDEF0123456789ADBDEF0123456789ADBDEF0',
    '0123456789~!@#$%^&*()_+|<>?:"{}`,./;''[]ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz',
    null,
    0.123456789,
    1234,
    current_time,
    current_timestamp,
    123,
    X'0123456789ABCDEF',
    'VARCHAR',
    X'0123456789ABCDEF',
    'CLOB',
    array[1,2,3,4]
);