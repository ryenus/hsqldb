call setenv.cmd
call ant clean hsqldb
echo hsqldb jar built
echo the hsqldbtest target is no longer available
echo build.test.suite.cmd now handles the requirements directly