/*
 * $Id$
 *
 * Sample/Template for writing an HTML Report
 */

-- Populate sample data
create table t (i integer, vc varchar(20));
insert into t values(1, 'one');
insert into t values(2, 'two');
insert into t values(3, 'three');
insert into t values(4, 'four');
insert into t values(5, 'five');
commit;


-- IMPORTANT:  \o will append by default.  If you want to write a new file,
-- it's your responsibility to check that a file of the same name does not
-- already exist (or remove it).


-- Follow the following examples to use your own HTML fragment files.
-- * *TOP_HTMLFRAG_FILE = /tmp/top.html
-- * *BOTTOM_HTMLFRAG_FILE = /tmp/bottom.html

-- The default TOP_HTMLFRAG_FILE has a reference to this PL variable.
* REPORT_TITLE = Blaine's Sample Report
-- The default will also override its CSS style settings with your own if you
-- put them in a file named "overrides.css" in same directory alongside your
-- reports ("report.html" in this example).
-- You can add references to ${system.properties} and *{PL_VARIABLES} in
-- your own custom fragment files too.


-- Turn on HTML output mode.
-- Must enable HTML _before_ opening to write top frag.
\h true
\o report.html
\p A message to appear in the Report
SELECT * FROM t;

-- Close off output just to show that you can go back and forth.
-- A close with '\o' will not write the bottom boilerplate that closes the HTML.
\o
\h false
\p Some non-HTML non-Report output:
SELECT count(*) FROM t;

\h true
-- Re-open the report
\o report.html
\d t
-- This time close it with
\oc
