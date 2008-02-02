$Id$

SqlTool Unit Tests
since:  HSQLDB 1.8.0.8
author:  Blaine Simpson (unsaved@users.sourceforge.net)


PORTABILITY (or lack thereof)

    At this time, you need to have a Bash shell to run this script, so you'll
    need a UNIX variant, Cygwin, etc.
    When I have time, I'll port the test runner script "runtests.bash" to 
    Java so that the tests will run on any Java platform.
    If you have time and the Java skills to port it now-- please contact 
    me ASAP:  blaine.simpson@admc.com
    (BTW, it will take considerable effort to just port the existing
    behavior from Bash to Perl, but the real difficultiy would be to
    improve the run-time by not invoking a new JVM for each test run.
    Specifically, Java uses a very closed design for providing stdin,
    stdout, stderr, and one can't open and close these pipes at will, as
    we do in the Bash script).


HOW TO RUN

    Typical usage (executes all tests):

        cd .../testrun/sqltool
        ./runtests.bash

    Run tests on any SQL files:

        .../testrun/sqltool/runtests.bash file1.sql file2.sql...

    If there are any test failures, the failed SQL scripts will be listed
    at the end of the results.  To get details about the failure, run
    runtests.bash with just one of the failed SQL scripts at a time, and
    with the Verbose option, like

        ./runtests.bash -v failedscript.sql

    To see all available invocation methods:

        ./runtests.bash -h


FILE NAMING AND ASCII/BINARY CONVENTIONS

    You can name SQL scripts anything that you want, except that the
    filename suffixes ".nsql"  and ".inter" are reserved for negative
    and interactive SQL scripts, correspondingly (see the next section
    about that).

    If you plan to run runtests.bash with no filename arguments, it
    will execute all *.sql, *.nsql, an d*.inter scripts in the current
    directory.
    So, if you plan to run runtests.bash this way, you must take care
    to name only scripts **which you want executed at the top level**
    with extensions sql, nsql, inter.  By "at the top level", I mean
    that if you are nesting SQL scripts with the \i command, you can't 
    name the nested script with suffix .sql, .nsql, or .inter or the
    script will accidentally be executed directly when you run
    runtests.bash without script arguments.  It's a simple concept, but,
    as you can see, it's a little difficult to explain, so here's an
    example.

    You have a script name "top.sql" which contains

         \i nested.sql

    You can run "./runtests.bash top.sql" and everything will work
    fine-- runtests.bash will execute "top.sql" which will nest
    "nested.sql".  But if you run just "./runtests.bash",
    runtests.bash will run "top.bash", which will nest "nested.bash"
    just like before, but runtests.bash will also execute
    "nested.bash" directly, since it executes all files with extensions
    sql and nsql (and inter).  Just use any filename suffix other than
    .sql, .nsql, and .inter for your nested SQL scripts and everything
    will work fine.

    If you are a HSQLDB developer and will be committing test scripts,
    then please use the following filename and type conventions:

        purpose                 suffix  filetype
        --------------------    ------  ----------------------------
        top-level SQL script    .sql    ASCII (mime-type text/plain)
        top-level neg. SQL      .nsql   ASCII (mime-type text/plain)
        interactive SQL script  .inter  ASCII (mime-type text/plain)
        nested \i SQL script    .isql   ASCII (mime-type text/plain)
        delimiter-sep-values    .dsv    Binary (no mime-type)

    If you will be adding new files to HSQLDB, please configure these
    extensions in for CVS or Subversion client accordingly.


    FINE POINT JUSTIFYING Binary TYPE FOR DSV FILES

        (Only read if you give a damned).
        The reason we're storing DSV files as binaries is because if 
        CVS or Subversion saved them as ASCII, the line delimiters would
        be determined by the platform performing the check-out, and 
        imports would fail if the computer executing nested.bash happened 
        to be of different EOL type from the computer that checked out.
        (This would be the case any time that somebody built a 
        distributable of any type which includes the .dsv files).


NEGATIVE TESTS

    runtests.bash determines if a test script succeeds by checking if
    the exit status of SqlTool is zero.  But we also need to test that
    scripts fail out when they should.  I.e., we need to verify that if
    \c (continue-on-error) is false, and there is a syntax error, or
    a runtime SQL failure, that SqlTool exits with non-zero exit status,
    as it should.  For this, I have invented the convention that SQL
    scripts named with suffix ".nsql" are just like normal SQL files,
    except that they are expected to fail.

    Here's an example to confirm that you understand.

        ./runtests.bash 1.sql 2.sql 3.nsql 4.nsql 5.sql

    This test run will report test failures for 1.sql, 2.sql, and 5.sql
    only if SqlTool fails when executing them.  It will report test
    failures for 3.nsql and 4.nsql only if SqlTool succeeds when 
    executing them.  I.e., runtests.bash _expects_ SqlTool to fail 
    when executing *.nsql files.

    Negative test scripts should be small and should fail out as
    early as possible.  The reason for this is that if there is an
    accidental error before the point of your test, the script will
    fail out early providing a false negative SqlTool exit code,
    thereby silently missing your test completely.


INTERACTIVE TESTS

    Interactive test are invoked like

        java... SqlTool... mem < scriptname.inter

    in order to test interactive ":" commands.  The : commands are
    disabled if an SQL script path(s) is given directly as an SqlTool
    parameter.  I.e., SqlTool runs non-interactively if an SQL script
    path is given as a pareter; therefore, to test interactive
    commands, we invoke SqlTool without a script name, and instead
    pipe the script into SqlTool as stdin.  (Using script name of
    "-" would do the reverse, it would run in interactive mode even
    though getting input from stdin).

    Remember to put "\c false" at the top of your interacive scripts,
    or errors will be ignored.  Account for this command when counting
    command numbers in the command history.
