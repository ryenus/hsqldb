/* Copyright (c) 2001-2021, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.hsqldb.jdbc.scripted;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestResult;
import junit.framework.TestSuite;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.HsqldbEmbeddedDatabaseDeleter;

/**
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
@SuppressWarnings("ClassWithoutLogger")
public class TestSelfScriptsTest extends BaseTestCase {


    private static final String URL = "jdbc:hsqldb:file:scripted-test/";

    public static Test suite() throws SQLException {
        return new TestSuite(TestSelfScriptsTest.class);
    }

    public static void main(String[] args) throws Exception {
        junit.textui.TestRunner.run(suite());
    }
    
    private TestResult testResult;
    private final List<String> scripts = new ArrayList<>(16);

    public TestSelfScriptsTest() {
        super();
    }
    @Override
    public void run(TestResult result) {
        testResult = result;
        super.run(result);
    }

    @Override
    public String getUrl() {
        return URL;
    }

    @Override
    protected void preTearDown() throws Exception {
        super.preTearDown();
    }

    @Override
    protected void postTearDown() throws Exception {
        super.postTearDown();
        final boolean success = HsqldbEmbeddedDatabaseDeleter.deleteDatabase(URL);

        if (success) {
            printProgress("Database deletion succeeded for: " + URL);
        } else {
            printProgress("Database deletion failed for: {0} " + URL);
        }
    }

    @Override
    @SuppressWarnings("ManualArrayToCollectionCopy")
    public void setUp() throws Exception {
        super.setUp();

        try {
            String[] resources = getResoucesInPackage(
                    "hsqldb");

            Arrays.sort(resources);

            for (String resource : resources) {
                scripts.add(resource);
            }
        } catch (IOException ex) {
            printWarning(ex);
        }

    }

    public void testScripts() throws Exception {

        

        final Iterator<String> itr = scripts.iterator();
        while (itr.hasNext()) {
            String script = itr.next();
            File file = new File(script);

            boolean waitForShutdown = Files.readAllLines(file.toPath()).stream().anyMatch(line -> line.toUpperCase().contains(" SHUTDOWN "));

            printProgress(script);
            
            TestCase tc = new NamedDummyCase(script);
            
            testResult.startTest(tc);

            try (Connection conn = newConnection();) {
                try {
                    ScriptUtil.testScript(conn, script, new FileReader(file));
                    
                } catch (SQLException | IOException ex) {
                    println(script);
                    printWarning(ex);
                    testResult.addError(tc, ex);

                }
            }
            
            testResult.endTest(tc);
        }

//        printProgress("************** SUCEEDED ****************");
//        super.
//                printProgress("count: " + succeeded.size());
//        succeeded.forEach(script -> printProgress(script));
//        printProgress("************** FAILED ****************");
//
//        if (failCount > 0) {
//            fail("fail count: " + failCount);
//        }
    }

    @Override
    public int countTestCases() {
       return scripts.size();
    }

//public static void testScript(Connection aConnection, String sourceName,
//                                  Reader inReader)
//                                  throws SQLException, IOException {
//
//        Statement        statement = aConnection.createStatement();
//        LineNumberReader reader    = new LineNumberReader(inReader);
//        LineGroupReader  sqlReader = new LineGroupReader(reader);
//        int              startLine = 0;
//
//        System.out.println("Opened test script file: " + sourceName);
//
//        /*
//         * we read the lines from the start of one section of the script "/*"
//         *  until the start of the next section, collecting the lines in the
//         *  list.
//         *  When a new section starts, we pass the list of lines
//         *  to the test method to be processed.
//         */
//        try {
//            while (true) {
//                HsqlArrayList section = sqlReader.getNextSection();
//
//                startLine = sqlReader.getStartLineNumber();
//
//                if (section.size() == 0) {
//                    break;
//                }
//
//
//            }
//
//            statement.close();
//
//            // The following catch blocks are just to report the source location
//            // of the failure.
//        } catch (SQLException se) {
//            System.out.println("Error encountered at command beginning at "
//                               + sourceName + ':' + startLine);
//
//            throw se;
//        } catch (RuntimeException re) {
//            System.out.println("Error encountered at command beginning at "
//                               + sourceName + ':' + startLine);
//
//            throw re;
//        }
//
//        System.out.println("Processed " + reader.getLineNumber()
//                           + " lines from " + sourceName);
//    }
    private static class NamedDummyCase extends TestCase {
        
        private NamedDummyCase(String name) {
            super(name);
        }
        
    }

}
