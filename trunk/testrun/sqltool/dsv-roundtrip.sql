/*
 * $Id$
 *
 * Tests a DSV "round trip".
 * Create a table, export the data, import it back, cf. imported  and original.
 */

/** This is the default on UNIX.
 *  Our *.dsv test files are stored as binaries, so this is required
 *  to run tests on Windows: */
* *DSV_ROW_DELIM = \n

* *DSV_TARGET_FILE = ${java.io.tmpdir}/test-roundtrip-${user.name}.dsv
* *DSV_TARGET_TABLE = t
CREATE TABLE t (i INT, a INT, d DATE);

INSERT INTO t(i, a, d) VALUES (1, 149, null);
INSERT INTO t(i, a, d) VALUES (2, null, '2007-06-24');

/* Export */
\x t

SELECT count(*) FROM t WHERE i = 1 AND a = 149 AND d IS null;
*if (*? != 1)
    \q Pre-check of inserted data failed (1)
*end if
SELECT count(*) FROM t WHERE i = 2 AND a IS NULL AND d = '2007-06-24';
*if (*? != 1)
    \q Pre-check of inserted data failed (2)
*end if


/* Import */
\m *{*DSV_TARGET_FILE}

SELECT count(*) FROM t WHERE i = 1 AND a = 149 AND d IS null;
*if (*? != 2)
    \q Post-check of imported data failed (1)
*end if
SELECT count(*) FROM t WHERE i = 2 AND a IS NULL AND d = '2007-06-24';
*if (*? != 2)
    \q Post-check of imported data failed (2)
*end if
