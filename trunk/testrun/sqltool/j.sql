/*
 * $Id$
 *
 * Tests \j command
 */

CREATE TABLE t(i int);
INSERT INTO t VALUES(11);
COMMIT;

\j SA jdbc:hsqldb:mem:dataSource2
SET PASSWORD sapwd;

\j SA jdbc:hsqldb:mem:utst
SELECT * FROM t;
* if (*? != 11)
    \q \j failed
* end if
