/*
 * $Id$
 *
 * Create a table, CVSV-export the data, import it back.
 */

* *DSV_COL_DELIM = ,
* *DSV_COL_SPLITTER = ,

-- 1. SETTINGS
-- For applications like MS Excel, which can't import or export nulls, we have
-- to dummy down our database empty strings to export and import as if they
-- were nulls.
-- N.b.!!!!  With current version of SqlTool, setting any PL variable to an
-- empty string like this requires that you set Java sytem property
-- sqltool.REMOVE_EMPTY_VARS to 'false'.  This will become unnecessary soon
-- because this will become the default SqlTool behavior.
-- Until then, invoke SqlTool like:
--    java -Dsqltool.REMOVE_EMPTY_VARS=false -jar .../sqltool.jar...
-- or
--    java -Dsqltool.REMOVE_EMPTY_VARS=false org.hsqldb.cmdline.SqlTool...
* *NULL_REP_TOKEN =

-- Enable following line to quote every cell value
-- * *ALL_QUOTED = true


-- 2. SET UP TEST DATA
CREATE TABLE t (i INT, v VARCHAR(25), d DATE);
INSERT INTO t(i, v, d) VALUES (1, 'one two three', null);
INSERT INTO t(i, v, d) VALUES (2, null, '2007-06-24');
INSERT INTO t(i, v, d) VALUES (3, 'one,two,,three', '2007-06-24');
INSERT INTO t(i, v, d) VALUES (4, '"one"two""three', '2007-06-24');
INSERT INTO t(i, v, d) VALUES (5, '"one,two"three,', '2007-06-24');
INSERT INTO t(i, v, d) VALUES (6, '', '2007-06-24');
commit;

-- 3. CSV EXPORT
/* Export */
\xq t

-- 4. BACK UP AND ZERO SOURCE TABLE
CREATE TABLE orig AS (SELECT * FROM t) WITH DATA;
DELETE FROM t;
commit;

-- 5. CSV IMPORT
\mq t.csv
commit;

-- 6. MANUALLY EXAMINE DIFFERENCES BETWEEN SOURCE AND IMPORTED DATA.
-- See <HSQLDB_ROOT>/testrun/sqltool/csv-roundtrip.sql to see a way to make
-- this same comparison programmatically.
-- Unset *NULL_REP_TOKEN so we can distinguish nulls from empty strings in our
-- examination.
* - *NULL_REP_TOKEN
\p
\p ORIGINAL:
SELECT * FROM orig;
\p
\p IMPORTED:
SELECT * FROM t;
\p
\p If you ran with sqltool.REMOVE_EMPTY_VARS false, then the empty string in
\p the source table will have been translated to null in the imported data.
\p If so, you can see that the generated CSV file represents both nulls and
\p empty strings as nothing, hence the convergence.
