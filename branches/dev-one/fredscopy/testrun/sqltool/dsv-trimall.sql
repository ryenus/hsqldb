/*
 * $Id$
 *
 * Tests *DSV_TRIM_ALL option.
 */

CREATE TABLE t (i INT, vc VARCHAR, d DATE);

\m dsv-trimall.dsv

SELECT COUNT(*) FROM t WHERE vc = 'three';
*if (*? != 1)
    \q Sanity check failed
*end if
SELECT COUNT(*) FROM t WHERE vc = '  o n e  ';
*if (*? != 1)
    \q Default mode messed with VARCHAR-embedded whitespace
*end if
SELECT COUNT(*) FROM t WHERE vc = ' two ';
*if (*? != 1)
    \q Default mode messed with leading or trailing whitespace in VARCHAR
*end if

DELETE from t;
COMMIT;

* *DSV_TRIM_ALL = true

\m dsv-trimall.dsv

SELECT COUNT(*) FROM t WHERE vc = 'three';
*if (*? != 1)
    \q 2nd Sanity check failed
*end if
SELECT COUNT(*) FROM t WHERE vc = 'o n e';
*if (*? != 1)
    \q *DSV_TRIM_ALL didn't trim with VARCHAR-embedded whitespace properly
*end if
SELECT COUNT(*) FROM t WHERE vc = 'two';
*if (*? != 1)
    \q *DSV_TRIM_ALL didn't trim leading or trailing whitespace properly
*end if
