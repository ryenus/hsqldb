/*
 * $Id$
 *
 * Tests SqlTool functions.
 */

-- Special command function
/= f() \p one *{:2} *{1} four
/f(alpha, beta)five;
/f(alpha) six;
--/f();
/f(alpha, beta)seven
:;
\p uno *{:2} *{:1} quatro
/: g() sinco
/g(alpha, beta);


CREATE TABLE t(i integer, vc varchar(20));
INSERT INTO t VALUES(1, 'one');
INSERT INTO t VALUES(2, 'two');
COMMIT;

-- SQL functions
\.
INSERT INTO t VALUES(3, 'three');
SELECT  -- Trailing whitespace on next line
    * *{1} 
.
/: h() t 
-- Trailing whitespace on previous line
* quieter ~
/h(FROM)WHERE i = 3;
* if (*? != three)
    \q SqlTool function for multi-line chunked SQL command failed
*end if
