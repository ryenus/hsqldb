/*
 * $Id$
 *
 * Tests \j command
 */

CREATE TABLE t1(i int);
INSERT INTO t1 VALUES(11);
COMMIT;

\j SA jdbc:hsqldb:mem:dataSource2
SET PASSWORD sapwd;
CREATE TABLE t2(i int);
INSERT INTO t2 VALUES(11);
COMMIT;

\j SA sapwd jdbc:hsqldb:mem:dataSource2
SELECT * FROM t1;
* if (*? != 11) \q \j failed

\j SA jdbc:hsqldb:mem:utst
SELECT * FROM t1;
* if (*? != 11) \q \j failed
