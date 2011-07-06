/*
    $Id$
    Exemplifies use of SqlTool's \j command to specify the JDBC connection
    parameters right in the SQL file.

    Invoke like this:

        java -jar .../sqltool.jar - .../j-sample.sql

    (give the file paths to wherever these two files reside).
    Or start up SqlTool like this:

        java -jar .../sqltool.jar

    and then execute this script like

        \i .../j-sample.sql
*/

-- Abort this script when errors occur.
-- That's the default if the script is invoked from command-line, but not if
-- invoked by \i.
\c false
 
-- Note the new feature in HyperSQL 2, whereby you can set an SA password
-- by just specifying that as the password for the very first connection to
-- that database
\j SA fred jdbc:hsqldb:mem:fred
--  FORMAT:  \j <USERAME> <PASSWORD> <JDBC_URL>

\p You have conkected successfully
\p

CREATE TABLE t(i BIGINT, vc VARCHAR(20));
INSERT INTO t VALUES(1, 'one');
INSERT INTO t VALUES(2, 'two');

SELECT * FROM t;
