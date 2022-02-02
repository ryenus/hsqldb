call setenv.cmd
call ant clean hsqldb preprocessor sqltool
call ant -f test.xml clean.test.suite make.test.suite