/*
 * $Id$
 *
 * Tests trimming in DSV imports
 */

CREATE TABLE t (i INT, r REAL, d DATE, t TIMESTAMP, v VARCHAR(80), b BOOLEAN);

\m dsv-trimming.dsv

SELECT count(*)  FROM t WHERE i = 31;
*if (*? != 1)
    \q Import of space-embedded INT failed
*end if

SELECT count(*)  FROM t WHERE r = 3.124;
*if (*? != 1)
    \q Import of space-embedded REAL failed
*end if

SELECT count(*)  FROM t WHERE d = '2007-06-07';
*if (*? != 1)
    \q Import of space-embedded DATE failed (1)
*end if

SELECT count(*)  FROM t WHERE t = '2006-05-06 12:30:04';
*if (*? != 1)
    \q Import of space-embedded TIMESTAMP failed
*end if

SELECT count(*)  FROM t WHERE v = '  a B  ';
*if (*? != 1)
    \q Import of space-embedded VARCHAR failed
*end if

/** I dont' know if "IS true" or "= true" is preferred, but the former
 * doesn't work with HSQLDB 1.7.0.7 */
SELECT count(*)  FROM t WHERE b = true;
*if (*? != 1)
    \q Import of space-embedded BOOLEAN failed
*end if


/** Repeat test with some non-default DSV settings */
* *DSV_COL_SPLITTER = \\
* *DSV_ROW_SPLITTER = \}(?:\r\n|\r|\n)

DELETE FROM t;

\m dsv-trimming-alt.dsv

SELECT count(*)  FROM t WHERE i = 31;
*if (*? != 1)
    \q Import of space-embedded INT failed
*end if

SELECT count(*)  FROM t WHERE r = 3.124;
*if (*? != 1)
    \q Import of space-embedded REAL failed
*end if

SELECT count(*)  FROM t WHERE d = '2007-06-07';
*if (*? != 1)
    \q Import of space-embedded DATE failed (2)
*end if

SELECT count(*)  FROM t WHERE t = '2006-05-06 12:30:04';
*if (*? != 1)
    \q Import of space-embedded TIMESTAMP failed
*end if

SELECT count(*)  FROM t WHERE v = '  a B  ';
*if (*? != 1)
    \q Import of space-embedded VARCHAR failed
*end if

/** I dont' know if "IS true" or "= true" is preferred, but the former
 * doesn't work with HSQLDB 1.7.0.7 */
SELECT count(*)  FROM t WHERE b = true;
*if (*? != 1)
    \q Import of space-embedded BOOLEAN failed
*end if
