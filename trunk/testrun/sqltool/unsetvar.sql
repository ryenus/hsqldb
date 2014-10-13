/*
 * $Id$
 */

\p *{:unsetvar}

*if (*x != *y) \q Two unset variables are not equal

*x =
*if (*x == *y) \q A variable set to '' is equal to an unset variable

*z =
*if (*x != *z) \q Two variables set to '' are not equal
