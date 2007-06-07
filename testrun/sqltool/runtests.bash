#!/bin/bash -p
PROGNAME="${0##*/}"

# $Id$

# HSQLDB is a platform-independent product.
# This script is only here until I have time to write a portable (probably
# Java) version.  Platform-dependent testing is better than no testing!
# If somebody reading this would like to port this script to Java, please do so!
#   -- blaine (unsaved@users.sourceforge.net)

set +u
shopt -s xpg_echo   # This will fail for very old implementations of Bash

# The %% is for when this script is renamed with an extension line .sh or .bash.
TMPDIR=/var/tmp/${PROGNAME%%.*}.$$
# If this is changed, make sure it never contains spaces or other shell
# metacharacters.

SYNTAX_MSG="$PROGNAME [-nvh] [testscript.sql...]
With -v, output from SqlTool will be shown.  Otherwise, only tests and results
will be shown.
If no script names are supplied, *.sql from the current directory will be
executed.
Exit value is number of test failures, or 1 for other errors or if number of
test failures exceeds 255 (shell scripts can't handle exit values > 255).

Non-verbose Result Key:
    T = Testing
    + = test Succeeded
    - = test Failed"

[ $# -gt 0 ] && case "$1" in -*)
    case "$1" in *v*) VERBOSE=1;; esac
    case "$1" in *n*) NORUN=1;; esac
    case "$1" in *h*)
        echo "$SYNTAX_MSG"
        exit 0
    ;; esac
    shift
esac

REDIR=
[ -n "$VERBOSE" ] || REDIR='>&- 2>&-'

Failout() {
    echo "Aborting $PROGNAME:  $*" 1>&2
    exit 1
}

java -version >&- 2>&- ||
Failout 'You must put the version of Java to test (early) in your search path'
[ -n "$CLASSPATH" ] ||
Failout "You must put the HSQLDB jar file (or class directory) in your CLASSPATH
(and export)"
java org.hsqldb.util.SqlTool --help >&- 2>&- ||
Failout 'org.hsqldb.util.SqlTool is not in your CLASSPATH.  Add, export, and re-run.'

declare -a Scripts
if [ $# -gt 0 ]; then
    Scripts=($@)
else
    Scripts=(*.sql)
    [ "${#Scripts[@]}" -eq 1 ] && [ "${Scripts[0]}" = '*.sql' ] &&
    Failout "No *.sql script(s) in current directory"
fi

for script in "${Scripts[@]}"; do
    [ -f "$script" ] || Failout "Script '$script' not present"
    [ -r "$script" ] || Failout "Script '$script' not readable"
done

[ -d $TMPDIR ] || {
    mkdir -p $TMPDIR || Failout "Failed to create temp directory '$TMPDIR'"
}

trap "rm -rf $TMPDIR" EXIT

declare -a FailedScripts
echo "${#Scripts[@]} tests to run..."

for script in "${Scripts[@]}"; do
    if [ -n "$VERBOSE" ]; then
        echo java org.hsqldb.util.SqlTool --inlineRc user=sa,url=jdbc:hsqldb:mem:utst "$script"
    else
        echo -n T
    fi
    [ -n "$NORUN" ] || {
        echo |  #  This is to give SqlTool a blank password for user 'sa'
        eval java org.hsqldb.util.SqlTool --inlineRc user=sa,url=jdbc:hsqldb:mem:utst "$script" $REDIR
        result=$?
        [ $result -eq 0 ] || FailedScripts=("${FailedScripts[@]}" "$script")
        if [ -n "$VERBOSE" ]; then
            if [ $result -eq 0 ]; then
                echo SUCCESS
            else
                echo FAIL
            fi
        else
            if [ $result -eq 0 ]; then
                echo -n '\b+'
            else
                echo -n '\b-'
            fi
        fi
    }
done
[ -n "$VERBOSE" ] || echo  # Line break after the plusses and minuses

[ ${#FailedScripts[@]} -eq 0 ] && exit 0

echo "${#FailedScripts[@]} tests failed out of ${#Scripts[@]}:"
IFS='
'
echo "${FailedScripts[@]}"
[ ${#FailedScripts[@]} -gt 255 ] && exit 1
exit ${#FailedScripts[@]}
