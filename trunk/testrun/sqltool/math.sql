/*
 * $Id$
 */

*((x=1+2+3))
* if (*x != 6)
    \q Tight (non-whitespace) math expression failed
* end if

*z = notanum
  *  ((  z  =  1  +  2  +  3  ))  
* if (*z != 6)
    \q Loose (much-whitespace) math expression failed
* end if

-- Force an error with a non-integral variable
-- *x=werd

* ((y =(x*2)/3 -(2 + 2)))
* if (*y != 0)
    \q Math expression with parenthetical nesting failed
* end if

CREATE TABLE t(i INTEGER);

* ((i = 0))
* while (*i < 5)
  -- \p Next is *{i}
  INSERT INTO t VALUES(*{i});
  * ((i = i + 1))
* end while

* c _
SELECT COUNT(*) FROM t;
* if (*c != 5)
    \q Loop failed to insert right number of records
* end if

* r _
SELECT COUNT(*) FROM t WHERE i = 4;
* if (*r != 1)
    \q Loop failed to insert record with i == 4
* end if
