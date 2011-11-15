/*
 * $Id$
 */

* mt1 =
* mt2 =
-- Following line has trailing white space, but that should have no effect.
* mt3 =   
* - un1
* - un2

* if (*unsetA != *unsetB)
    \q Two never-set vars differ
* end if
* if (*un1 != *un2)
    \q Two explicitly unset variables differ
* end if
* if (*un1 != *unsetA)
    \q Explicitly unset vs. neverset vars differ
* end if

* if (*mt1 != *mt2)
    \q Two legacy-unset vars differ
* end if

* if (*mt1 != *mt3)
    \q Two legacy-unset vars differ, one set to blanks
* end if

-- Reverse this test once sqltool.REMOVE_EMPTY_VARS behavior changed
* if (*mt1 != *unset)
    \q Legacy-unset var != never-set var
* end if

* if (*mt1 != *unset)
    \q Legacy-unset var != never-set var
* end if

* if (alpha != alpha)
    \q Sanity constant comparison failed
* end if

* a = alpha
* anotherA = alpha
* if (*a != *anotherA)
    \q To equivalant variable values differ
* end if

-- Following two have 2 trailing spaces:
* wsTrailed1 = alpha  
* wsTrailed2 = alpha  
* if (*wsTrailed1 == a)
    \q Trailing white-space not honored in logical evaluation
* end if
* if (*wsTrailed1 != *wsTrailed2)
    \q Trailing white-space not valued equally for logical evaluation
* end if

* wsEmbed1 = alpha  beta
* wsEmbed2 = alpha  beta
* if (*wsEmbed1 != *wsEmbed2)
    \q Embedded white-space not valued equally for logical evaluation
* end if
