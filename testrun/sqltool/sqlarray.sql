/*
 * $Id$
 *
 * Tests basic usage of SQL Arrays
 */

CREATE TABLE a (i BIGINT PRIMARY KEY, ar INTEGER ARRAY);

INSERT INTO a VALUES (1, array [11, null, 13]);
INSERT INTO a VALUES (2, null);
INSERT INTO a VALUES (3, array [21, 22]);

* ROWCOUNT _
SELECT count(*) FROM a;

* if (*ROWCOUNT != 3)
    \q Failed to insert 3 rows with SQL Values
* end if

-- This fails with "row column count mismatch", even if I test ar[1] and all
-- rows have a valid integer in ar[1].  ???
-- SELECT count(*) FROM a WHERE i = 1 AND ar[3] = 13;
