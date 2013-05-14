drop table all_array_types if exists;

create table all_array_types(
     id                   integer primary key
     ,c_bigint             bigint array[1]
     ,c_binary             binary(16) array[1]
     ,c_boolean            boolean array[1]
     ,c_character          character(8) array[1]
     ,c_date               date array[1]
     ,c_decimal            decimal(11,10) array[1]
     ,c_double             double array[1]
     ,c_float              float array[1]
     ,c_integer            integer array[1]
     ,c_longvarbinary      longvarbinary array[1]
     ,c_longvarchar        longvarchar array[1]
     ,c_object             object array[1]
     ,c_real               real array[1]
     ,c_smallint           smallint array[1]
     ,c_time               time array[1]
     ,c_timestamp          timestamp array[1]
     ,c_tinyint            tinyint array[1]
     ,c_varbinary          varbinary(16) array[1]
     ,c_varchar            varchar(255) array[1]
);

insert into all_array_types(
     id
     ,c_bigint
     ,c_binary
     ,c_boolean
     ,c_character
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
) values (
    1
    ,array[123456789]                  -- c_bigint
    ,array[X'0123456789ABCDEF']        -- c_binary
    ,array[TRUE]                       -- c_boolean
    ,array['CHAR  ']                   -- c_character
    ,array[DATE'2010-07-04']           -- c_date
    ,array[0.1234556789]               -- c_decimal
    ,array[0.1234556789]               -- c_double
    ,array[0.1234556789]               -- c_float
    ,array[123456789]                  -- c_integer
    ,array[X'123456789ADBDEF0123456789ADBDEF0123456789ADBDEF0123456789ADBDEF0123456789ADBDEF0123456789ADBDEF0123456789ADBDEF0']
    ,array['0123456789~!@#$%^&*()_+|<>?:"{}`,./;''ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz']
    ,array[null]                       -- c_object
    ,array[0.123456789]                -- c_real
    ,array[1234]                       -- c_smallint
    ,array[TIME'23:11:54']             -- c_time
    ,array[TIMESTAMP'2010-07-04 23:06:49.005000']
    ,array[123]                        -- c_tinyint
    ,array[X'0123456789ABCDEF']        -- c_varbinary
    ,array['VARCHAR']                  -- c_varchar
);