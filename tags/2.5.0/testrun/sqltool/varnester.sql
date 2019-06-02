/*
 * $Id$
 *
 * Invokes another script that defines variables.
 * Then we test that we have access to the variables.
 */

/* Want to test with no user variables set like this, but unfortunately the
   test script invoker always sets one user variable:
\p *{testvar} -- Should not expand the variable but should not abort
*/

\i varnestee.isql

\p *{x}
