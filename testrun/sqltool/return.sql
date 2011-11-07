/*
 * $Id$
 *
 * Tests the new "return" statement, which is equivalent to a "break" statement
 * with no parameter.
 */

* VAR=one
\i return.isql
* VAR=*{VAR} three

* EXPECTED = one two three
* if (*VAR != *EXPECTED) \q return statement failed: (*{VAR}) vs. (*{EXPECTED})
