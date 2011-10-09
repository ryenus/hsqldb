/*
 * $Id$
 *
 * Does a DSV export using a multi-line custom query instead of just
 * specifying the table name.
 * Imports it back to validate the output.
 */

* *DSV_TARGET_FILE = ${java.io.tmpdir}/test-roundtrip-${user.name}.dsv
* *DSV_TARGET_TABLE = t
CREATE TABLE t (i INT, a INT, d DATE);

INSERT INTO t(i, a, d) VALUES (1, 149, null);

-- 1-liner sanity check:
* qpart1 = i FROM
SELECT *{qpart1} t;
* if (*? != 1)
      \q 1-line query with internal PL variable failed
* end if

-- Multi-line PL var:
\.
a
    FROM
.
* qpart2 :
SELECT *{qpart2} t;
* if (*? != 149)
      \q 1-line query with multi-line internal PL variable failed
* end if

* res1 ~
SELECT
  *{qpart2}
     t;
* if (*res1 != 149)
      \q multi-line query with multi-line internal PL variable failed
* end if

-- Multi-line Macro:
\.
  SELECT
a
.
/= m1 : FROM
* res2 ~
/m1 t;
* if (*res2 != 149)
      \q multi-line query with multi-line internal PL variable failed
* end if
