/*
 * $Id$
 *
 * Imports delimiter-separated-values, and generates an output 
 * reject .dsv file, and a reject report.
 *
 * To execute, set up a SqlTool database urlid (see User Guide if you don't
 * know how to do that); then (from this directory) execute this script like
 *
 *    java ../lib/hsqldb.jar mem dsv-sample.sql
 *
 * (replace "mem" with your urlid).
 */

CREATE TABLE sampletable(i INT, d DATE NOT NULL, b BOOLEAN);

/* If you dont' set *DSV_TARGET_TABLE, it defaults to the base name of the
   .dsv file. */
* *DSV_TARGET_TABLE = sampletable

\p WARNING:  Some records will be skipped, and some others will be rejected.
\p This is on purpose, so you can work with a reject report.
\p

/* By default, no reject files are written, and the import will abort upon
 * the first error encountered.  If you set either of these settings, the
 * import will continue to completion if at all possible. */
* *DSV_REJECT_FILE = ${java.io.tmpdir}/sample-reject.dsv
* *DSV_REJECT_REPORT = ${java.io.tmpdir}/sample-reject.html
\m sample.dsv

/* Enable this line if you want to display all successfully imported data:
SELECT * FROM sampletable;
*/

\p
\p See import reject report at '*{*DSV_REJECT_REPORT}'.
