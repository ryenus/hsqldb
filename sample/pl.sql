/*
    $Id$
    SQL File to illustrate the use of some basic SqlTool PL features.
    Invoke like
        java -jar .../sqltool.jar mem .../pl.sql
                                                         -- blaine
*/

* if (! *MYTABLE)
    \p MYTABLE variable not set!
    /* You could use \q to Quit SqlTool, but it's often better to just
       break out of the current SQL file.
       If people invoke your script from SqlTool interactively (with
       \i yourscriptname.sql) any \q will kill their SqlTool session. */
    \p Use argument "-pMYTABLE=mytablename" for SqlTool
    * break
* end if

-- Turning on Continue-upon-errors so that we can check for errors ourselves.
\c true

\p
\p Loading up a table named '*{MYTABLE}'...

CREATE TABLE *{MYTABLE} (
    i int,
    s varchar(20)
);
-- PL variable ? is always set to status or fetched value of last SQL
-- statement.  It will be null/unset if the last SQL statement failed.
\p CREATE status is *{?}
\p

/* Validate our return status.
   In case of success of a CREATE TABLE, *? will be 0, and therefore a
   '* if (*?)' would be false.
   So we follow the general practice of testing *? for the error indicator
   value of null, using the reserved SqlTool system variable *NULL.
 */
* if (*? == *NULL)
    \p Our CREATE TABLE command failed.
    * break
* end if

-- Default Continue-on-error behavior is what you usually want
\c false
\p

/* Insert data with a foreach loop.
   These values could be from a read of another table or from variables
   set on the command line like
*/
\p Inserting some data into our new table
* foreach VALUE (12 22 24 15)
    * if (*VALUE > 23)
        \p Skipping *{VALUE} because it is greater than 23
        * continue
        \p YOU WILL NEVER SEE THIS LINE, because we just 'continued'.
    * end if
    INSERT INTO *{MYTABLE} VALUES (*{VALUE}, 'String of *{VALUE}');
* end foreach
\p

/* This time instead of using the ? variable, we're assigning the SELECT value
   to a User variable, 'themax'. */
* themax ~
/* Can put Special Commands and comments between "* VARNAME ~" and the target 
   SQL statement. */
\p We're saving the max value for later.  You'll still see query output here:
SELECT MAX(i) FROM *{MYTABLE};

/* No need to test for failure status (either ? or themax being unset/null),
   because we are in \c mode and would have aborted if the SELECT failed. */
* if (0 == *themax)
    \p Got 0 as the max value.
    * break
    \p YOU WILL NEVER SEE THIS LINE, because we just 'broke'.
* end if

\p
\p ##############################################################
\p The results of our work:
SELECT * FROM *{MYTABLE};
\p MAX value is *{themax}

\p
\p Counting down to exit
* ((i = 3))
* while (*i > 0)
    \p *{i}...
    * ((i -= 1))  -- i++ is supported but i-- is not, because -- marks comments
* end while

\p
\p Everything worked.  Signing off.
