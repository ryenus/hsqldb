/*
 * $Id$
 *
 * Logic tests
 */

*if (1)
    * T1 = true
*end if
*if (! *T1)
    \q Test of (1) failed
*end if
*if (0)
    \q Test of (0) failed
*end if

*if (! 1)
    \q Test of (! 1) failed
*end if
*if (! 0)
    * T2 = true
*end if
*if (! *T2)
    \q Test of (! 0) failed
*end if

*if (!1)
    \q Test of (!1) failed
*end if
*if (!0)
    * T3 = true
*end if
*if (!*T3)
    \q Test of (!0) failed
*end if

* SETVAR=3
*if (*SETVAR)
    * T4 = true
*end if
*if (! *T4)
    \q Test of (*SETVAR) failed
*end if
*if (*UNSETVAR)
    \q Test of (*UNSETVAR) failed
*end if

*if (! *SETVAR)
    \q Test of (! *SETVAR) failed
*end if
*if (! *UNSETVAR)
    * T5 = true
*end if
*if (! *T5)
    \q Test of (! *UNSETVAR) failed
*end if

*if (!*SETVAR)
    \q Test of (!*SETVAR) failed
*end if
*if (!*UNSETVAR)
    * T6 = true
*end if
*if (!*T6)
    \q Test of (!*UNSETVAR) failed
*end if
