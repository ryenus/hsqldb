/*
 * $Id$
 *
 * Test of \q with arg from nested script.
 */

\i nq-noarg1.isql
\q Should not have returned from nested script!
