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

*a = 3
*((z=a*2))
*(( z *= 1 + 1 ))
* if (*z != 12)
    \q Math op #1 failed
* end if

-- Series of squares
* sum = 0
* i = 0
* while (*i < 5)
  * ((sum += i*i))
  * ((i++))
* end while
* if (*sum != 30)
    \q Math summation failed
* end if
-- Count back down
* while (*i > 0)
  * ((i-=1)) -- We do not support '--'
  * ((sum -= i*i))
* end while
* if (*sum != 0)
    \q Reversion of summation failed.  *{sum} left over.
* end if

* ((  v1 = (3 + 4) ^ (1 + 2) * 3  ))
* if (*v1 != 1029)
    \q Power operation failed
* end if
