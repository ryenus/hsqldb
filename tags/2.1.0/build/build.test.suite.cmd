call setenv.cmd
ant clean hsqldb preprocessor sqltool
ant -f test.xml clean.test.suite make.test.suite