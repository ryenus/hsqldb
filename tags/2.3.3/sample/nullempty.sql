/*
 * $Id$
 *
 * This sample shows differences between null and empty strings,
 * and ? var vs. _/~ variables.
 */

\p At startup ? is equal to empty string.  See between A and B:  A*{?}B
* if (A*{?}B == AB) \p ? is the empty string

CREATE TABLE t(i INTEGER, vc VARCHAR(20));
INSERT INTO t VALUES(1, 'one');
INSERT INTO t VALUES(2, 'two');
* res ~
SELECT * FROM t;
\p *{?}
\p *{res}
* listvalues ? res

INSERT INTO t VALUES (3, null);
*res ~
SELECT vc FROM t WHERE i = 3;
\p *{?}
* if (*res == **NULL) \p res really is null
* listvalues ? res

-- This will prevent SqlTool from aborting when we run a bad SQL statement:
\c true
*res ~
SELECT hocus FROM pocus;
* if (*? == **NULL) \p ? really is null
* if (*res == **NULL) \p res really is null
* listvalues ? res
