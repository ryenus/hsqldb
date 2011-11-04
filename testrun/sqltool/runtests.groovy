#!/local/groovy/bin/groovy

/**
 * The I/O in this class could be simplified it were not necessary to support
 * Gradle.
 * Gradle intercedes with stdout (and stderr), requiring the flushes and usage
 * of System.console().  The latter makes this script require Java 6 or later.
 */

import java.util.logging.Logger

class SqlToolTester {
    private static Logger logger = Logger.getLogger(SqlToolTester.class.name)
    private static boolean doJvmExit = true
    private static String helpMessage = '''
groovy runtests.groovy [-h] [-nv] [testscript.sql...]
-h displays this message.
With -v, output from SqlTool will be shown.  Otherwise, only tests and results
will be shown.
With -n, SqlTool will not actually be invoked.
If no script names are supplied, *.sql and *.nsql from the current directory 
will be executed.
Exit value is number of test failures, or 1 for other errors or if number of
test failures exceeds 255 (shell scripts can't handle exit values > 255).

Non-verbose Result Key:
    T = Testing
    + = test Succeeded
    - = test Failed'''

    public static void runTests(def params) {
        boolean noRun, verbose
        while (params.size() > 0
                && params[0].length() > 0 && params[0].charAt(0) == '-') {
            if (params[0].indexOf('n') > 0) noRun = true
            if (params[0].indexOf('v') > 0) verbose = true
            if (params[0].indexOf('h') > 0) {
            }
            params.remove(0)
        }
        File javaBinDir = new File(System.properties['java.home'], 'bin')
        assert ([javaBinDir.absolutePath + System.properties['file.separator']
                + 'java', '-version'].execute().waitFor() == 0) :
         "Executable Java not found under Java home '$javaBinDir.absolutePath'"
        String sqlToolPath = '../../lib/sqltool.jar'
        File sqlToolFile = new File(sqlToolPath)
        if (!sqlToolFile.canRead())
            throw new IOException("Can't read file: $sqlToolFile.absolutePath")
        if (!sqlToolFile.isFile())
            throw new IOException("Not a read file: $sqlToolFile.absolutePath")
        String[] pbParams = [
            'java',
            '-Dsqltool.REMOVE_EMPTY_VARS=true',
            '-jar',
            sqlToolPath,
            '--noAutoFile',
            '-Ptestvar=plval',
            '--inlineRc=user=sa,url=jdbc:hsqldb:mem:utst,password=,transiso=TRANSACTION_READ_COMMITTED',
            null  // Sometimes we'll specify script, sometimes not
        ]
        def scripts = []
        if (params.size() > 0) {
            for (p in params) scripts << new File(p)
        } else {
            AntBuilder ant = new AntBuilder()
            for (suffix in ['.sql', '.nsql', '.inter'])
                for (f in (ant.fileScanner {
                    fileset(dir: '.') {
                        include(name: "*$suffix")
                    }
                })) scripts << f
        }
        if (scripts.size() < 1)
            throw new IllegalStateException(
                'No *.sql, *.ndsql, or *.inter scripts in current directory')
        for (f in scripts) {
            //println f.name
            if (!f.canRead())
                throw new IOException("Can't read file: $f.absolutePath")
            if (!f.isFile())
                throw new IOException("Not a read file: $f.absolutePath")
        }

        println scripts.size() + ' test(s) to run...'
        boolean isInter
        def runParams
        boolean succeeded
        def failedScriptNames = []
        PrintWriter cPrinter = System.console().writer()
        StringWriter sWriter
StringWriter eWriter
        for (f in scripts) {
            isInter = f.name.endsWith('.inter')
            if (!isInter) pbParams[pbParams.length-1] = f.name
            runParams = isInter ? pbParams[0..pbParams.length-2] : pbParams
            if (verbose) {
                logger.info(runParams.join(' '))
            } else {
                cPrinter.print('T')
                cPrinter.flush()
            }
            if (noRun) continue
            sWriter  = new StringWriter();  // Unfortunately, can't re-use
            // To process stderr separately, remove the redirectErrorStream
            // call below, and add a p.consumeProcessErrorStream(eWriter)
            Process p = new ProcessBuilder(runParams)
                    .redirectErrorStream(true).start()
            if (isInter)
                p.withWriter { it.write(f.getText('UTF-8')) }
            else
                p.out.close()
            if (verbose)
                p.consumeProcessOutputStream(sWriter)
            else
                p.consumeProcessOutput()
            succeeded = (p.waitFor() == 0) ^ f.name.endsWith('.nsql')
            if (!succeeded) failedScriptNames << f.name
            if (verbose) {
                print sWriter.toString()
                println succeeded ? 'SUCCESS' : 'FAIL'
            } else {
                cPrinter.print('\b')
                cPrinter.print(succeeded ? '+' : '-')
                cPrinter.flush()
            }
        }
        if (!verbose) cPrinter.println()
        if (failedScriptNames.size() < 1) {
            if (doJvmExit) System.exit(0)
            return
        }

        println """For details, run again against each failed script individually with -v
switch, like 'groovy runtests.groovy -v failedscript.sql'.
${failedScriptNames.size()} tests failed out of ${scripts.size()}:"""
        println failedScriptNames.join(System.properties['line.separator'])
        if (doJvmExit)
            System.exit((failedScriptNames.size() > 255)
                        ? 1 : failedScriptNames.size())
    }
}

def ps = Arrays.asList(args) as ArrayList
if (ps.size() > 0 && ps[0] == 'NO_JVM_EXIT') {
    SqlToolTester.doJvmExit = false
    ps.remove(0)
}
SqlToolTester.runTests(ps)
