/*
 * $Id$
 *
 * Tests auto-variable ?
 */

CREATE TABLE t (i INT);

* if (*? != 0)
    \p ? variable not capturing CREATE TABLE return value
* end if

INSERT INTO t values (21);
* if (*? != 1)
    \p ? variable not capturing INSERT return value
* end if

INSERT INTO t values (10);
* if (*? != 1)
    \p ? variable not capturing INSERT return value
* end if

INSERT INTO t values (43);
* if (*? != 1)
    \p ? variable not capturing INSERT return value
* end if

SELECT * FROM t ORDER BY i DESC;
* if (*? != 10)
    \p ? variable not capturing last fetched value
* end if

\p echo some stuff
\p to verify that ? variable value is preserved
* list

* if (*? != 10)
    \p ? value not retained after special commands
* end if
* if (*{?} != 10)
    \p ? value not dereferenced with {} usage
* end if
