drop table jdbc_required_get_xxx if exists;

create table jdbc_required_get_xxx(
    c_tinyint           tinyint, -- .......................................... 1
    c_smallint          smallint,
    c_integer           integer,
    c_bigint            bigint,
    c_real              real,
    c_float             float(53), -- ........................................ 6
    c_double            double,
    c_decimal           decimal(10,2),
    c_numeric           numeric(10,2),
    c_bit               bit(1),
    c_boolean           boolean, -- ......................................... 11
    c_char              char(32),
    c_varchar           varchar(255),
    c_longvarchar       longvarchar(65536),
    c_binary            binary(32),
    c_varbinary         varbinary(255), -- .................................. 16
    c_longvarbinary     longvarbinary(65536),
    c_date              date,
    c_time              time,
    c_timestamp         timestamp(6),
    c_array             integer array[1], -- ................................ 21
    c_blob              blob(65536),
    c_clob              clob(65536),
    c_struct            other,
    c_ref               other,
    c_datalink          other, -- ........................................... 26
    c_java_object       other,
    c_rowid             other,
    c_nchar             char(32),
    c_nvarchar          varchar(255),
    c_longnvarchar      longvarchar(65536), -- .............................. 31
    c_nclob             clob(65536),
    c_sqlxml            other,
    c_other             other,
    c_get_xxx_name      varchar(25) primary key
);

-- JDBC 4.0, Table B6, Use of ResultSet getter Methods to Retrieve
--                     JDBC Data Types
--                           S
--                           M I
--                         T A N
--                         I L T..........................S
--                         N L G..........................Q O
--                         Y L E..........................L T
--                         I I G..........................X H
--                         N N E..........................M E
--                         T T R..........................L R
--                         0123456789012345678901234567890123
-- {"getByte",            "1111111111111100000000000001000001"},
-- {"getShort",           "1111111111111100000000000000000001"},
-- {"getInt",             "1111111111111100000000000000000001"},
-- {"getLong",            "1111111111111100000000000000000001"},
-- {"getFloat",           "1111111111111100000000000000000001"},
-- {"getDouble",          "1111111111111100000000000000000001"},
-- {"getBigDecimal",      "1111111111111100000000000000000001"},
-- {"getBoolean",         "1111111111111100000000000000000001"},
-- {"getString",          "1111111111111111111100001000111001"},
-- {"getNString",         "1111111111111111111100001000111001"},
-- {"getBytes",           "0000000000000011100000000000000001"},
-- {"getDate",            "0000000000011100010100000000000001"},
-- {"getTime",            "0000000000011100001100000000000001"},
-- {"getTimestamp",       "0000000000011100011100000000000001"},
-- {"getAsciiStream",     "0000000000011111100010000000000101"},
-- {"getBinaryStream",    "0000000000000011100001000000000011"},
-- {"getCharacterStream", "0000000000011111100010000000111111"},
-- {"getNCharacterStream","0000000000011111100010000000111111"},
-- {"getClob",            "0000000000000000000010000000000101"},
-- {"getNClob",           "0000000000000000000010000000000101"},
-- {"getBlob",            "0000000000000000000001000000000001"},
-- {"getArray",           "0000000000000000000000100000000000"},
-- {"getRef",             "0000000000000000000000010000000000"},
-- {"getURL",             "0000000000000000000000001000000000"},
-- {"getObject",          "1111111111111111111111111111111111"},
-- {"getRowId",           "0000000000000000000000000001000000"},
-- {"getSQLXML",          "0000000000000000000000000000000010"}

-- {"getByte",            "1111111111111100000000000001000001"},
insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getByte',
    cast(1 as tinyint),
    cast(1 as smallint),
    cast(1 as integer),
    cast(1 as bigint),
    cast(1.0 as real),
    cast(1.0 as float(53)),
    cast(1.0 as double),
    cast(1 as decimal(10,2)),
    cast(1 as numeric(10,2)),
    cast(1 as bit(1)),
    cast(true as boolean),
    cast('1' as char(32)),
    cast('1' as varchar(255)),
    cast('1' as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as blob(65536)),
    cast(null as clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast(null as other) -- other
);

-- {"getShort",           "1111111111111100000000000000000001"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getShort',
    cast(1 as tinyint),
    cast(1 as smallint),
    cast(1 as integer),
    cast(1 as bigint),
    cast(1.0 as real),
    cast(1.0 as float(53)),
    cast(1.0 as double),
    cast(1 as decimal(10,2)),
    cast(1 as numeric(10,2)),
    cast(1 as bit(1)),
    cast(true as boolean),
    cast('1' as char(32)),
    cast('1' as varchar(255)),
    cast('1' as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast(null as other) -- other
);

-- {"getInt",           "1111111111111100000000000000000001"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getInt',
    cast(1 as tinyint),
    cast(1 as smallint),
    cast(1 as integer),
    cast(1 as bigint),
    cast(1.0 as real),
    cast(1.0 as float(53)),
    cast(1.0 as double),
    cast(1 as decimal(10,2)),
    cast(1 as numeric(10,2)),
    cast(1 as bit(1)),
    cast(true as boolean),
    cast('1' as char(32)),
    cast('1' as varchar(255)),
    cast('1' as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast(null as other) -- other
);

-- {"getLong",           "1111111111111100000000000000000001"},
insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getLong',
    cast(1 as tinyint),
    cast(1 as smallint),
    cast(1 as integer),
    cast(1 as bigint),
    cast(1.0 as real),
    cast(1.0 as float(53)),
    cast(1.0 as double),
    cast(1 as decimal(10,2)),
    cast(1 as numeric(10,2)),
    cast(1 as bit(1)),
    cast(true as boolean),
    cast('1' as char(32)),
    cast('1' as varchar(255)),
    cast('1' as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast(null as other) -- other
);

-- {"getFloat",           "1111111111111100000000000000000001"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getFloat',
    cast(1 as tinyint),
    cast(1 as smallint),
    cast(1 as integer),
    cast(1 as bigint),
    cast(1.0 as real),
    cast(1.0 as float(53)),
    cast(1.0 as double),
    cast(1 as decimal(10,2)),
    cast(1 as numeric(10,2)),
    cast(1 as bit(1)),
    cast(true as boolean),
    cast('1' as char(32)),
    cast('1' as varchar(255)),
    cast('1' as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast(null as other) -- other
);

-- {"getDouble",           "1111111111111100000000000000000001"},
insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getDouble',
    cast(1 as tinyint),
    cast(1 as smallint),
    cast(1 as integer),
    cast(1 as bigint),
    cast(1.0 as real),
    cast(1.0 as float(53)),
    cast(1.0 as double),
    cast(1 as decimal(10,2)),
    cast(1 as numeric(10,2)),
    cast(1 as bit(1)),
    cast(true as boolean),
    cast('1' as char(32)),
    cast('1' as varchar(255)),
    cast('1' as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast(null as other) -- other
);

-- {"getBigDecimal",           "1111111111111100000000000000000001"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getBigDecimal',
    cast(1 as tinyint),
    cast(1 as smallint),
    cast(1 as integer),
    cast(1 as bigint),
    cast(1.0 as real),
    cast(1.0 as float(53)),
    cast(1.0 as double),
    cast(1 as decimal(10,2)),
    cast(1 as numeric(10,2)),
    cast(1 as bit(1)),
    cast(true as boolean),
    cast('1' as char(32)),
    cast('1' as varchar(255)),
    cast('1' as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast(null as other) -- other
);

-- {"getBoolean",           "1111111111111100000000000000000001"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getBoolean',
    cast(1 as tinyint),
    cast(1 as smallint),
    cast(1 as integer),
    cast(1 as bigint),
    cast(1.0 as real),
    cast(1.0 as float(53)),
    cast(1.0 as double),
    cast(1 as decimal(10,2)),
    cast(1 as numeric(10,2)),
    cast(1 as bit(1)),
    cast(true as boolean),
    cast('true' as char(32)),
    cast('true' as varchar(255)),
    cast('true' as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast(null as other) -- sqlxml
);

-- {"getString",          "1111111111111111111100001000111001"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getString',
    cast(1 as tinyint),
    cast(1 as smallint),
    cast(1 as integer),
    cast(1 as bigint),
    cast(1.0 as real),
    cast(1.0 as float(53)),
    cast(1.0 as double),
    cast(1 as decimal(10,2)),
    cast(1 as numeric(10,2)),
    cast(1 as bit(1)),
    cast(true as boolean),
    cast('1' as char(32)),
    cast('1' as varchar(255)),
    cast('1' as longvarchar(65536)),
    cast(X'01' as binary(32)),
    cast(X'01' as varbinary(255)),
    cast(X'01' as longvarbinary(65536)),
    cast(current_date as date),
    cast(current_time as time),
    cast(current_timestamp as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast('1' as char(32)), -- nchar
    cast('1' as varchar(255)), -- nvarchar
    cast('1' as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast(null as other) -- other
);

-- {"getNString",         "1111111111111111111100001000111001"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getNString',
    cast(1 as tinyint),
    cast(1 as smallint),
    cast(1 as integer),
    cast(1 as bigint),
    cast(1.0 as real),
    cast(1.0 as float(53)),
    cast(1.0 as double),
    cast(1 as decimal(10,2)),
    cast(1 as numeric(10,2)),
    cast(1 as bit(1)),
    cast(true as boolean),
    cast('1' as char(32)),
    cast('1' as varchar(255)),
    cast('1' as longvarchar(65536)),
    cast(X'01' as binary(32)),
    cast(X'01' as varbinary(255)),
    cast(X'01' as longvarbinary(65536)),
    cast(current_date as date),
    cast(current_time as time),
    cast(current_timestamp as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast('1' as char(32)), -- nchar
    cast('1' as varchar(255)), -- nvarchar
    cast('1' as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast(null as other) -- other
);

-- {"getBytes",           "0000000000000011100000000000000001"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getBytes',
    cast(null as tinyint),
    cast(null as smallint),
    cast(null as integer),
    cast(null as bigint),
    cast(null as real),
    cast(null as float(53)),
    cast(null as double),
    cast(null as decimal(10,2)),
    cast(null as numeric(10,2)),
    cast(null as bit(1)),
    cast(null as boolean),
    cast(null as char(32)),
    cast(null as varchar(255)),
    cast(null as longvarchar(65536)),
    cast(X'01' as binary(32)),
    cast(X'01' as varbinary(255)),
    cast(X'01' as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast(null as other) -- other
);

-- {"getDate",            "0000000000011100010100000000000001"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getDate',
    cast(null as tinyint),
    cast(null as smallint),
    cast(null as integer),
    cast(null as bigint),
    cast(null as real),
    cast(null as float(53)),
    cast(null as double),
    cast(null as decimal(10,2)),
    cast(null as numeric(10,2)),
    cast(null as bit(1)),
    cast(null as boolean),
    cast('2012-01-20' as char(32)),
    cast('2012-01-20' as varchar(255)),
    cast('2012-01-20' as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast('2012-01-20' as date),
    cast(null as time),
    cast('2012-01-20 17:42:03.234' as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast('2012-01-20 17:42:03.234' as timestamp(6)) -- other
);

-- {"getTime",            "0000000000011100001100000000000001"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getTime',
    cast(null as tinyint),
    cast(null as smallint),
    cast(null as integer),
    cast(null as bigint),
    cast(null as real),
    cast(null as float(53)),
    cast(null as double),
    cast(null as decimal(10,2)),
    cast(null as numeric(10,2)),
    cast(null as bit(1)),
    cast(null as boolean),
    cast('17:42:03.234' as char(32)),
    cast('17:42:03.234' as varchar(255)),
    cast('17:42:03.234' as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast(null as date),
    cast('17:42:03.234' as time),
    cast('2012-01-20 17:42:03.234' as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast('2012-01-20 17:42:03.234' as timestamp(6)) -- other
);

-- {"getTimestamp",       "0000000000011100011100000000000001"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getTimestamp',
    cast(null as tinyint),
    cast(null as smallint),
    cast(null as integer),
    cast(null as bigint),
    cast(null as real),
    cast(null as float(53)),
    cast(null as double),
    cast(null as decimal(10,2)),
    cast(null as numeric(10,2)),
    cast(null as bit(1)),
    cast(null as boolean),
    cast('2012-01-20 17:42:03.234' as char(32)),
    cast('2012-01-20 17:42:03.234' as varchar(255)),
    cast('2012-01-20 17:42:03.234' as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast('2012-01-20' as date),
    cast('17:42:03.234' as time),
    cast('2012-01-20 17:42:03.234' as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast('2012-01-20 17:42:03.234' as timestamp(6)) -- other
);

-- {"getAsciiStream",     "0000000000011111100010000000000101"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getAsciiStream',
    cast(null as tinyint),
    cast(null as smallint),
    cast(null as integer),
    cast(null as bigint),
    cast(null as real),
    cast(null as float(53)),
    cast(null as double),
    cast(null as decimal(10,2)),
    cast(null as numeric(10,2)),
    cast(null as bit(1)),
    cast(null as boolean),
    cast('ABCDEFG' as char(32)),
    cast('ABCDEFG' as varchar(255)),
    cast('ABCDEFG' as longvarchar(65536)),
    cast(X'41424344454647' as binary(32)),
    cast(X'41424344454647' as varbinary(255)),
    cast(X'41424344454647' as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast('ABCDEFG' as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast('ABCDEFG' as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast('ABCDEFG' as varchar(255)) -- other
);

-- {"getBinaryStream",    "0000000000000011100001000000000011"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getBinaryStream',
    cast(null as tinyint),
    cast(null as smallint),
    cast(null as integer),
    cast(null as bigint),
    cast(null as real),
    cast(null as float(53)),
    cast(null as double),
    cast(null as decimal(10,2)),
    cast(null as numeric(10,2)),
    cast(null as bit(1)),
    cast(null as boolean),
    cast(null as char(32)),
    cast(null as varchar(255)),
    cast(null as longvarchar(65536)),
    cast(X'41424344454647' as binary(32)),
    cast(X'41424344454647' as varbinary(255)),
    cast(X'41424344454647' as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as blob(65536)),
    cast(null as clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast(null as other) -- other
);

-- {"getCharacterStream", "0000000000011111100010000000111111"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getCharacterStream',
    cast(null as tinyint),
    cast(null as smallint),
    cast(null as integer),
    cast(null as bigint),
    cast(null as real),
    cast(null as float(53)),
    cast(null as double),
    cast(null as decimal(10,2)),
    cast(null as numeric(10,2)),
    cast(null as bit(1)),
    cast(null as boolean),
    cast('ABCDEFG' as char(32)),
    cast('ABCDEFG' as varchar(255)),
    cast('ABCDEFG' as longvarchar(65536)),
    cast(X'41424344454647' as binary(32)),
    cast(X'41424344454647' as varbinary(255)),
    cast(X'41424344454647' as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast('ABCDEFG' as char(32)), -- nchar
    cast('ABCDEFG' as varchar(255)), -- nvarchar
    cast('ABCDEFG' as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast(null as other) -- other
);

-- {"getNCharacterStream","0000000000011111100010000000111111"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getNCharacterStream',
    cast(null as tinyint),
    cast(null as smallint),
    cast(null as integer),
    cast(null as bigint),
    cast(null as real),
    cast(null as float(53)),
    cast(null as double),
    cast(null as decimal(10,2)),
    cast(null as numeric(10,2)),
    cast(null as bit(1)),
    cast(null as boolean),
    cast('ABCDEFG' as char(32)),
    cast('ABCDEFG' as varchar(255)),
    cast('ABCDEFG' as longvarchar(65536)),
    cast(X'41424344454647' as binary(32)),
    cast(X'41424344454647' as varbinary(255)),
    cast(X'41424344454647' as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast('ABCDEFG' as char(32)), -- nchar
    cast('ABCDEFG' as varchar(255)), -- nvarchar
    cast('ABCDEFG' as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast(null as other) -- other
);

-- {"getClob",            "0000000000000000000010000000000101"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getClob',
    cast(null as tinyint),
    cast(null as smallint),
    cast(null as integer),
    cast(null as bigint),
    cast(null as real),
    cast(null as float(53)),
    cast(null as double),
    cast(null as decimal(10,2)),
    cast(null as numeric(10,2)),
    cast(null as bit(1)),
    cast(null as boolean),
    cast(null as char(32)),
    cast(null as varchar(255)),
    cast(null as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as varchar(255)), -- sqlxml
    cast(null as other) -- other
);

-- {"getNClob",           "0000000000000000000010000000000101"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getNClob',
    cast(null as tinyint),
    cast(null as smallint),
    cast(null as integer),
    cast(null as bigint),
    cast(null as real),
    cast(null as float(53)),
    cast(null as double),
    cast(null as decimal(10,2)),
    cast(null as numeric(10,2)),
    cast(null as bit(1)),
    cast(null as boolean),
    cast(null as char(32)),
    cast(null as varchar(255)),
    cast(null as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as varchar(255)), -- sqlxml
    cast(null as other) -- other
);

-- {"getBlob",            "0000000000000000000001000000000001"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getBlob',
    cast(null as tinyint),
    cast(null as smallint),
    cast(null as integer),
    cast(null as bigint),
    cast(null as real),
    cast(null as float(53)),
    cast(null as double),
    cast(null as decimal(10,2)),
    cast(null as numeric(10,2)),
    cast(null as bit(1)),
    cast(null as boolean),
    cast(null as char(32)),
    cast(null as varchar(255)),
    cast(null as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as varchar(255)), -- sqlxml
    cast(null as other) -- other
);

-- {"getArray",           "0000000000000000000000100000000000"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getArray',
    cast(null as tinyint),
    cast(null as smallint),
    cast(null as integer),
    cast(null as bigint),
    cast(null as real),
    cast(null as float(53)),
    cast(null as double),
    cast(null as decimal(10,2)),
    cast(null as numeric(10,2)),
    cast(null as bit(1)),
    cast(null as boolean),
    cast(null as char(32)),
    cast(null as varchar(255)),
    cast(null as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(array[1] as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as varchar(255)), -- sqlxml
    cast(null as other) -- other
);

-- {"getRef",             "0000000000000000000000010000000000"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getRef',
    cast(null as tinyint),
    cast(null as smallint),
    cast(null as integer),
    cast(null as bigint),
    cast(null as real),
    cast(null as float(53)),
    cast(null as double),
    cast(null as decimal(10,2)),
    cast(null as numeric(10,2)),
    cast(null as bit(1)),
    cast(null as boolean),
    cast(null as char(32)),
    cast(null as varchar(255)),
    cast(null as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as varchar(255)), -- sqlxml
    cast(null as other) -- other
);

-- {"getURL",             "0000000000000000000000001000000000"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getURL',
    cast(null as tinyint),
    cast(null as smallint),
    cast(null as integer),
    cast(null as bigint),
    cast(null as real),
    cast(null as float(53)),
    cast(null as double),
    cast(null as decimal(10,2)),
    cast(null as numeric(10,2)),
    cast(null as bit(1)),
    cast(null as boolean),
    cast(null as char(32)),
    cast(null as varchar(255)),
    cast(null as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as varchar(255)), -- sqlxml
    cast(null as other) -- other
);

-- {"getObject",          "1111111111111111111111111111111111"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getObject',
    cast(1 as tinyint),
    cast(1 as smallint),
    cast(1 as integer),
    cast(1 as bigint),
    cast(1 as real),
    cast(1 as float(53)),
    cast(1 as double),
    cast(1 as decimal(10,2)),
    cast(1 as numeric(10,2)),
    cast(1 as bit(1)),
    cast(true as boolean),
    cast('ABCDEFG' as char(32)),
    cast('ABCDEFG' as varchar(255)),
    cast('ABCDEFG' as longvarchar(65536)),
    cast(X'CAFEBABE' as binary(32)),
    cast(X'CAFEBABE' as varbinary(255)),
    cast(X'CAFEBABE' as longvarbinary(65536)),
    cast('2012-01-20' as date),
    cast('00:00:00.999999' as time),
    cast('2012-01-20 00:00:00.999999' as timestamp(6)),
    cast(array[4] as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast('ABCDEFG' as char(32)), -- nchar
    cast('ABCDEFG' as varchar(255)), -- nvarchar
    cast('ABCDEFG' as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast(null as other) -- other
);

-- {"getRowId",           "0000000000000000000000000001000000"},

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getRowId',
    cast(null as tinyint),
    cast(null as smallint),
    cast(null as integer),
    cast(null as bigint),
    cast(null as real),
    cast(null as float(53)),
    cast(null as double),
    cast(null as decimal(10,2)),
    cast(null as numeric(10,2)),
    cast(null as bit(1)),
    cast(null as boolean),
    cast(null as char(32)),
    cast(null as varchar(255)),
    cast(null as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as varchar(255)), -- sqlxml
    cast(null as other) -- other
);

-- {"getSQLXML",          "0000000000000000000000000000000010"}

insert into jdbc_required_get_xxx(
    c_get_xxx_name,
    c_tinyint,
    c_smallint,
    c_integer,
    c_bigint,
    c_real,
    c_float,
    c_double,
    c_decimal,
    c_numeric,
    c_bit,
    c_boolean,
    c_char,
    c_varchar,
    c_longvarchar,
    c_binary,
    c_varbinary,
    c_longvarbinary,
    c_date,
    c_time,
    c_timestamp,
    c_array,
    c_blob,
    c_clob,
    c_struct,
    c_ref,
    c_datalink,
    c_java_object,
    c_rowid,
    c_nchar,
    c_nvarchar,
    c_longnvarchar,
    c_nclob,
    c_sqlxml,
    c_other
) values (
    'getSQLXML',
    cast(null as tinyint),
    cast(null as smallint),
    cast(null as integer),
    cast(null as bigint),
    cast(null as real),
    cast(null as float(53)),
    cast(null as double),
    cast(null as decimal(10,2)),
    cast(null as numeric(10,2)),
    cast(null as bit(1)),
    cast(null as boolean),
    cast(null as char(32)),
    cast(null as varchar(255)),
    cast(null as longvarchar(65536)),
    cast(null as binary(32)),
    cast(null as varbinary(255)),
    cast(null as longvarbinary(65536)),
    cast(null as date),
    cast(null as time),
    cast(null as timestamp(6)),
    cast(null as integer array[1]),
    cast(null as  blob(65536)),
    cast(null as  clob(65536)),
    cast(null as other), -- struct
    cast(null as other), -- ref
    cast(null as other), -- datalink
    cast(null as other), -- java_object
    cast(null as other), -- rowid
    cast(null as char(32)), -- nchar
    cast(null as varchar(255)), -- nvarchar
    cast(null as longvarchar(65536)), -- longnvarchar
    cast(null as clob(65536)), -- nclob
    cast(null as other), -- sqlxml
    cast(null as other) -- other
);