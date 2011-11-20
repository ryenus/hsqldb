/*
 * $Id$
 *
 * This test written verbosely to make it easier to partially share it with
 * script sample/nullempty.sql.
 * This tests differences between null and empty strings,
 * and ? var vs. _/~ variables.
 */

\p At startup ? is equal to empty string.  See between A and B:  A*{?}B
* if (A*{?}B != AB)
    \q ? is not the empty string:  *{?}
* end if

CREATE TABLE t(i INTEGER, vc VARCHAR(20));
INSERT INTO t VALUES(1, 'one');
INSERT INTO t VALUES(2, 'two');
* res ~
SELECT * FROM t;
* if (*? != two)
    \q ? did not get last cell value
* end if
* if (*res != 1)
    \q res did not get first cell value
* end if

INSERT INTO t VALUES (3, null);
*res ~
SELECT vc FROM t WHERE i = 3;
* if (*res != **NULL)
    \q res did not get assigned the SQL Null
* end if
* if (*? != [null])
    \q ? did not get assigned the *NULL_REP_TOKEN:  *{?}
* end if

-- This will prevent SqlTool from aborting when we run a bad SQL statement:
\c true
*res ~
SELECT hocus FROM pocus;
* if (*? != **NULL)
    \q ? did not get assigned null upon SQL failure:  *{?}
* end if
* if (*res != **NULL)
    \q res did not get assigned null upon SQL failure: *{res}
* end if
