/*
 * $Id$
 *
 * Invokes another script that defines variables.
 * Then we test that we have access to the variables.
 */

\p *{:unsetvar}

*if (*x != *y)
    \q Two unset variables are not equal
*end if

*x =
*if (*x != *y)
    \q A variable set to '' not equal to an unset variable
*end if
