#!/bin/bash -p
PROGNAME="${0##*/}"

# $Id$
# author: Blaine Simpson, unsaved@users.sourceforge.net
# since: HSQLDB 1.8.0.8 / 1.9.x
# see:  README.txt in this same directory.

set +u
shopt -s xpg_echo   # This will fail for very old implementations of Bash

# The %% is for when this script is renamed with an extension like .sh or .bash.
TMPDIR=/var/tmp/${PROGNAME%%.*}.$$
# If this is changed, make sure it never contains spaces or other shell
# metacharacters.

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
    declare -a tmpScripts
    tmpScripts=(*.sql)
    [ "${#tmpScripts[@]}" -ne 1 ] || [ "${tmpScripts[0]}" != '*.sql' ] &&
    Scripts=("${Scripts[@]}" "${tmpScripts[@]}")
    tmpScripts=(*.nsql)
    [ "${#tmpScripts[@]}" -ne 1 ] || [ "${tmpScripts[0]}" != '*.nsql' ] &&
    Scripts=("${Scripts[@]}" "${tmpScripts[@]}")
    [ ${#Scripts[@]} -eq 0 ] &&
    Failout "No *.sql or *.nsql script(s) in current directory"
fi
[ -n "$VERBOSE" ] && echo "Scripts to execute: ${Scripts[@]}"

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
        echo java -Dsqltool.testsp=spval org.hsqldb.util.SqlTool --setVar testvar=plval --inlineRc user=sa,url=jdbc:hsqldb:mem:utst "$script"
    else
        echo -n T
    fi
    [ -n "$NORUN" ] || {
        succeed=
        echo |  #  This is to give SqlTool a blank password for user 'sa'
        eval java -Dsqltool.testsp=spval org.hsqldb.util.SqlTool --setVar testvar=plval --inlineRc user=sa,url=jdbc:hsqldb:mem:utst "$script" $REDIR
        case "$script" in
            *.nsql) [ $? -ne 0 ] && succeed=1;;
            *) [ $? -eq 0 ] && succeed=1;;
        esac
        [ -n "$succeed" ] || FailedScripts=("${FailedScripts[@]}" "$script")
        if [ -n "$VERBOSE" ]; then
            if [ -n "$succeed" ]; then
                echo SUCCESS
            else
                echo FAIL
            fi
        else
            if [ -n "$succeed" ]; then
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
echo "${FailedScripts[*]}"
[ ${#FailedScripts[@]} -gt 255 ] && exit 1
exit ${#FailedScripts[@]}
