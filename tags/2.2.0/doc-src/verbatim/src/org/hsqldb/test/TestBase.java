/* Copyright (c) 2001-2011, The HSQL Development Group
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


package org.hsqldb.test;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.hsqldb.server.Server;
import org.hsqldb.server.WebServer;
import junit.framework.TestCase;
import junit.framework.TestResult;

/**
 * HSQLDB TestBugBase Junit test case. <p>
 *
 * @author  boucherb@users
 * @version 1.7.2
 * @since 1.7.2
 */
public abstract class TestBase extends TestCase {

    //  change the url to reflect your preferred db location and name
    String  serverProps;
    String  url;
    String  user     = "sa";
    String  password = "";
    Server  server;
    boolean isNetwork = true;
    boolean isHTTP    = false;

    public TestBase(String name) {
        super(name);
    }

    public TestBase(String name, String url, boolean isNetwork,
                    boolean isHTTP) {

        super(name);

        this.isNetwork = isNetwork;
        this.url       = url;
        this.isHTTP    = isHTTP;
    }

    protected void setUp() {

        if (isNetwork) {
            if (url == null) {
                url = isHTTP ? "jdbc:hsqldb:http://localhost/test"
                             : "jdbc:hsqldb:hsql://localhost/test";
            }

            server = isHTTP ? new WebServer()
                            : new Server();

            server.setDatabaseName(0, "test");
            server.setDatabasePath(0, "mem:test;sql.enforce_strict_size=true");
            server.setLogWriter(null);
            server.setErrWriter(null);
            server.start();
        } else {
            if (url == null) {
                url = "jdbc:hsqldb:file:test;sql.enforce_strict_size=true";
            }
        }

        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(this + ".setUp() error: " + e.getMessage());
        }
    }

    protected void tearDown() {

        if (isNetwork) {
            server.stop();

            server = null;
        }
    }

    Connection newConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    public static void runWithResult(Class testCaseClass, String testName) {

        try {
            Constructor ctor = testCaseClass.getConstructor(new Class[]{
                String.class });
            TestBase theTest = (TestBase) ctor.newInstance(new Object[]{
                testName });

            theTest.runWithResult();
        } catch (Exception ex) {
            System.err.println("couldn't execute test:");
            ex.printStackTrace(System.err);
        }
    }

    public void runWithResult() {

        TestResult result   = run();
        String     testName = this.getClass().getName();

        if (testName.startsWith("org.hsqldb.test.")) {
            testName = testName.substring(16);
        }

        testName += "." + getName();

        int failureCount = result.failureCount();

        System.out.println(testName + " failure count: " + failureCount);

        java.util.Enumeration failures = result.failures();

        while (failures.hasMoreElements()) {
            System.err.println(failures.nextElement());
        }
    }
}
