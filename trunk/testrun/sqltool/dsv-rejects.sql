/*
 * $Id$
 *
 * Tests proper rejection of bad DSV input records
 */

/** This is the default on UNIX.
 *  Our *.dsv test files are stored as binaries, so this is required
 *  to run tests on Windows: */
* *DSV_ROW_DELIM = \n

CREATE TABLE t (i INT, r REAL, d DATE, t TIMESTAMP, v VARCHAR, b BOOLEAN);

* *DSV_REJECT_REPORT = ${java.io.tmpdir}/sqltoolutst-${user.name}.html
\m dsv-rejects.dsv

SELECT COUNT(*) FROM t;

*if (*? != 2)
    \q Should have imported 2 good DSV records, but imported *{?}
*end if
