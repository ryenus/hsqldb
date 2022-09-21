/* Copyright (c) 2001-2022, The HSQL Development Group
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
import org.hsqldb.test.TestUtil;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.HsqldbEmbeddedDatabaseDeleter;
import org.hsqldb.testbase.OfMethod;

/**
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
@SuppressWarnings("ClassWithoutLogger")
@ForSubject(TestUtil.class)
@OfMethod("testScript(java.sql.Connection,java.lang.String,java.io.Reader)")
public class TestSelfScriptsTest extends BaseTestCase {

    private static final String URL = "jdbc:hsqldb:file:tmp/scripted-test/";
    private static final String PK_IGNORE_TEST_CASE = "IgnoreTestCase";

    public static Test suite() {
        return new TestSuite(TestSelfScriptsTest.class);
    }

    public static void main(String[] args) throws Exception {
        junit.textui.TestRunner.run(suite());
    }

    private TestResult testResult;
    private final List<String> scripts;
    private String wasIgnoreCodeCase;

    @SuppressWarnings("CollectionWithoutInitialCapacity")
    public TestSelfScriptsTest() {
        super();
        this.scripts = new ArrayList<>();
    }

    @Override
    public void run(final TestResult result) {
        testResult = result;
        super.run(result);
    }

    @Override
    public String getUrl() {
        return URL;
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    protected void preTearDown() throws Exception {
        super.preTearDown();
    }

    @Override
    protected void postTearDown() throws Exception {
        super.postTearDown();
        if (this.wasIgnoreCodeCase == null) {
            System.clearProperty(PK_IGNORE_TEST_CASE);
        } else {
            System.setProperty(PK_IGNORE_TEST_CASE, this.wasIgnoreCodeCase);
        }

        final boolean success = HsqldbEmbeddedDatabaseDeleter.deleteDatabase(URL);

        if (success) {
            printProgress("Database deletion succeeded for: " + URL);
        } else {
            printProgress("Database deletion failed for: {0} " + URL);
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        scripts.clear();

        this.wasIgnoreCodeCase = System.getProperty("IgnoreCodeCase", null);

        System.setProperty("IgnoreCodeCase", "true");

        try {
            final String[] resources = getResoucesInPackage("hsqldb");

            Arrays.sort(resources);

            scripts.addAll(Arrays.asList(resources));
        } catch (IOException ex) {
            printWarning(ex);
        }

    }

    public void testScripts() throws Exception {
        final Iterator<String> itr = scripts.iterator();
        while (itr.hasNext()) {
            final String scriptPath = itr.next();
            final File file = new File(scriptPath);
            printProgress(scriptPath);

            TestCase tc = new NamedDummyCase(scriptPath);

            testResult.startTest(tc);

            try (Connection conn = newConnection();) {
                try {
                    TestUtil.testScript(conn, scriptPath, new FileReader(file));
                } catch (SQLException | IOException ex) {
                    println(scriptPath);
                    printWarning(ex);
                    testResult.addError(tc, ex);

                }
            }

            testResult.endTest(tc);
        }

        printProgress("************** SUCEEDED ****************");
        int successCount = testResult.runCount() - testResult.errorCount()
                - testResult.failureCount();
        super.printProgress("count: " + successCount);

        printProgress("************** FAILED ****************");
        super.printProgress("count: " + testResult.failureCount());

        printProgress("************** ERRORED ****************");
        super.printProgress("count: " + testResult.errorCount());

    }

    @Override
    public int countTestCases() {
        return scripts.size();
    }

    private static class NamedDummyCase extends TestCase {

        private NamedDummyCase(String name) {
            super(name);
        }

    }

}
