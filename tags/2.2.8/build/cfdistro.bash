#!/bin/bash -p
PROGNAME="${0##*/}"

# Copyright (c) 2001-2009, The HSQL Development Group
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# Neither the name of the HSQL Development Group nor the names of its
# contributors may be used to endorse or promote products derived from this
# software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
# OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# This script generates a concise listing of differences between two
# distributions.
# This script obviously requires Bash.  I don't know whether it works on Cygwin.

# You run this script against two directory branches.
# If you are starting with two zip distributions, you must extract them first.
# If you are going to investigate differences, then set up the distribution
# directories so that the highest directories of interest are peers, like
#    cd /tmp/me
#    mkdir cfdir
#    unzip -q /tmp/hsqldb-x1.zip
#    mv hsqldb-x1/hsqldb cfdir/x1
#    unzip -q /tmp/hsqldb-x2.zip
#    mv hsqldb-x2/hsqldb cfdir/x2
#    cd cfdir
#    /path/to/cfdistro.bash x1 x2
# It is then very easy to view differences between the distributions textually
# or graphically.  If your file difference report (".../files.diff") contains
#  < hsqldb/testrun/hsqldb/TestSelf.txt  -rw-r--r-- 1 blaine blaine   52585
#  > hsqldb/testrun/hsqldb/TestSelf.txt  -rw-r--r-- 1 blaine blaine   52587
# then you use vim to check encodings and fileformat (EOLs), diff for
# differences in text, or gvimdiff for diffrences graphically.
#   gvim */hsqldb/testrun/hsqldb/TestSelf.txt  # Check encoding + fileformat
#   diff */hsqldb/testrun/hsqldb/TestSelf.txt  # See differences in text
#   gvimdiff */hsqldb/testrun/hsqldb/TestSelf.txt # See differences graphically

# This script can easily be enhanced to do the extraction and directory setup
# steps.
# We purposefully ignore directory and file modification timestamps.
# Modify the perl regular expressions to include or exclude other items in
# the "ls -l" listings.
# The generation timestamps and version labels inside of generated JavaDoc
# output will make these files differ.  I usually manually compare an index
# file and an actual class HTML file, then exclude the rest from the summary
# file with a command like
#   perl -ni -we 'print unless m:^. doc/apidocs/:;' /tmp/cfdistro-13756/files.diff

shopt -s xpg_echo
set +u

Failout() {
    echo "Aborting $PROGNAME:  $*" 1>&2
    exit 1
}
[ -n "$TMPDIR" ] || TMPDIR=/tmp
WORKDIR="$TMPDIR/${PROGNAME%.*}-$$"
mkdir "$WORKDIR" || Failout "Failed to make work director '$WORKDIR'"

type -t perl >&- || Failout "'$PROGNAME' requires 'perl' in your search path"

[ $# -ne 2 ] && Failout "SYNTAX:  $PROGNAME dir1/path dir2/path"
BASE1="$1"; shift
BASE2="$1"; shift

STARTDIR="$PWD"
cd "$BASE1" || Failout "Failed to cd to first base dir: $BASE1"
find * -type d | xargs ls -ld |
    perl -nwe 'm/(.+?)200\d-\d\d-\d\d \d\d:\d\d\s+(.+)/; print "$2  $1\n";' |
    sort > $WORKDIR/dirs.1
find * -type f | xargs ls -ld |
    perl -nwe 'm/(.+?)200\d-\d\d-\d\d \d\d:\d\d\s+(.+)/; print "$2  $1\n";' |
    sort > $WORKDIR/files.1

cd "$STARTDIR"
cd "$BASE2" || Failout "Failed to cd to first base dir: $BASE2"
find * -type d | xargs ls -ld |
    perl -nwe 'm/(.+?)200\d-\d\d-\d\d \d\d:\d\d\s+(.+)/; print "$2  $1\n";' |
    sort > $WORKDIR/dirs.2
find * -type f | xargs ls -ld |
    perl -nwe 'm/(.+?)200\d-\d\d-\d\d \d\d:\d\d\s+(.+)/; print "$2  $1\n";' |
    sort > $WORKDIR/files.2

cd "$STARTDIR"
cd "$WORKDIR" || Failout "Failed to cd to work dir: $WORKDIR"
declare -i retval=0
cmp -s dirs.1 dirs.2 || {
    ((retval = retval + 1))
    diff dirs.1 dirs.2 | egrep '^[<>]' > dirs.diff
    echo "See dir diffs at $WORKDIR/dirs.diff"
}
cmp -s files.1 files.2 || {
    ((retval = retval + 1))
    diff files.1 files.2 | egrep '^[<>]' > files.diff
    echo "See dir diffs at $WORKDIR/files.diff"
}

exit $retval
