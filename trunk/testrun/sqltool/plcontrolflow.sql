/*
 * $Id$
 *
 * Tests PL control flow.  if, foreach statements, etc.
 */

*if (*UNSET)
    \q Failed boolean test of an unset variable
*end if
*if (astring)
   * y = something
*end if
*if (*X)
    \q Failed boolean test of a simple string constant
*end if
*if (0)
    \q Failed boolean test of zero constant
*end if
*if (! x)
/* Note that there must be white space to separate the two tokens above. */
    \q Failed boolean test of a plain constant
*end if


/* Nested if tests */
* if (1)
    * L1 = true
    * if (2)
        * L2 = true
    * end if
    * L11 = true
* end if
*if (! L1)
    \q Pre-nest failure
*end if
*if (! L2)
    \q Inside-nest failure
*end if
*if (! L11)
    \q Post-nest failure
*end if
