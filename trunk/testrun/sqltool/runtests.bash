#!/bin/bash -p
PROGNAME="${0##*/}"

# $Id$
# author: Blaine Simpson, unsaved@users.sourceforge.net
# since: HSQLDB 1.8.0.8 / 1.9.x
# see:  README.txt in this same directory.
# In November 2011 this script converted from the sole test runner script, to
# one wrapper for Gradle (w/ "build.gradle") + runtests.groovy.

set +u
shopt -s xpg_echo   # This will fail for very old implementations of Bash

SYNTAX_MSG="$PROGNAME [-nvh] [testscript.sql...]
With -v, output from SqlTool will be shown.  Otherwise, only tests and results
will be shown.
If no script names are supplied, *.sql and *.nsql from the current directory 
will be executed.
Exit value is number of test failures, or 1 for other errors or if number of
test failures exceeds 255 (shell scripts can't handle exit values > 255).

Non-verbose Result Key:
    T = Testing
    + = test Succeeded
    - = test Failed"

while [ $# -gt 0 ]; do case "$1" in -*)
    case "$1" in *v*) VERBOSE=1;; esac
    case "$1" in *n*) NORUN=1;; esac
    case "$1" in *h*)
        echo "$SYNTAX_MSG"
        exit 0
    ;; esac
    shift;;
  *) break 2;;
esac; done

scriptsString=
while [ $# -gt 0 ]; do
    scriptsString="$scriptsString$1"
    shift
    [ $# -gt 0 ] && scriptsString="$scriptsString,"
done

[ -n "$VERBOSE" ] &&
echo ../../build/gradlew ${VERBOSE:+-Pverbose=true} ${NORUN:+-Pnorun=true} ${scriptsString:+-Pscripts=$scriptsString}
exec ../../build/gradlew ${VERBOSE:+-Pverbose=true} ${NORUN:+-Pnorun=true} ${scriptsString:+-Pscripts=$scriptsString}
