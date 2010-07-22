/*
 * $Id$
 *
 * Test loading other files with @
 */

\i @/tblx.sql

\m @/tblx.dsv

SELECT COUNT(*) FROM tblx;
*if (*? != 2)
    \q Failed to load table deta from @ directory
*end if
