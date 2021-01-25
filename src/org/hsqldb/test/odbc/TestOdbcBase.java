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


package org.hsqldb.test.odbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.Server;
import org.hsqldb.server.ServerConstants;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;

/**
 * Base test class for ODBC testing.
 *
 * Provides utility testrunner method, plus sets up and runs a HyperSQL
 * listener.
 * <p>
 * You MUST have a native (non-Java) ODBC DSN configured with the PostgreSQL
 * ODBC driver, DSN name "PostgreSQL30", DSN database "test", port "9001",
 * server "localhost".
 * The user name and password are "SA" and "PW".
 * We use the word <I>query</i> how it is used in the JDBC API, to mean a
 * SELECT statement, not in the more general way as used in the ODBC API.
 * <p>
 * The DSN name and port may be changed from these defaults by setting Java
 * system properties "test.hsqlodbc.dsnname" and/or "test.hsqlodbc.port".
 * <p>
 * This class has been converted to JUnit 4.10.
 * Test runs are faster than the JUnit 3.x original with one-time-per-class
 * setUp and tearDown (fredt@users).
 *
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 * @since 1.9.0
 */
public abstract class TestOdbcBase {

    Connection     netConn         = null;
    static Server  server          = null;
    static String  portString      = "9001";
    static String  dsnName         = "PostgreSQL30";
    static String  password        = "PW";
    static String  inMemURL        = "jdbc:hsqldb:mem:test";
    static String  serverURL       = "jdbc:hsqldb:hsql://localhost/test";
    static boolean inProcessServer = true;

    public TestOdbcBase() {}

    static {
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(
                "<clinit> failed.  JDBC Driver class not in CLASSPATH");
        }

        String portProp = System.getProperty("test.hsqlodbc.port");

        if (portProp != null) {
            portString = portProp;
        }

        String dsnProp = System.getProperty("test.hsqlodbc.dsnname");

        if (dsnProp != null) {
            dsnName = dsnProp;
        }
    }

    /**
     * JUnit convention for cleanup.
     *
     * Called after each test*() method.
     */
    static void tearDownServer() throws SQLException {

        if (inProcessServer) {
            server.shutdown();

            if (server.getState() != ServerConstants.SERVER_STATE_SHUTDOWN) {
                throw new RuntimeException("Server failed to shut down");
            }
        }
    }

    /**
     * Specifically, this opens a mem-only DB, populates it, starts a
     * HyperSQL Server to server it, and opens network JDBC Connection
     * "netConn" to it,
     *
     * Invoked before each test*() invocation by JUnit.
     */
    void setUp() throws Exception {

        try {
            Connection setupConn;

            if (inProcessServer) {
                setupConn = DriverManager.getConnection(inMemURL, "SA",
                        password);
            } else {
                setupConn = DriverManager.getConnection(serverURL, "SA",
                        password);
            }

            setupConn.setAutoCommit(false);

            Statement st = setupConn.createStatement();

            st.execute("drop schema public cascade");
            populate(st);
            st.close();
            setupConn.commit();
            setupConn.close();
        } catch (SQLException se) {
            throw new RuntimeException("Failed to set up in-memory database",
                                       se);
        }

        try {
            netConn = DriverManager.getConnection("jdbc:odbc:" + dsnName,
                                                  "SA", password);

            //netConn.setAutoCommit(false);
        } catch (SQLException se) {
            if (se.getMessage().indexOf("No suitable driver") > -1) {
                throw new RuntimeException(
                    "You must install the native library for Sun's jdbc:odbc "
                    + "JDBC driver");
            }

            if (se.getMessage().indexOf("Data source name not found") > -1) {
                throw new RuntimeException(
                    "You must configure ODBC DSN '" + dsnName
                    + "' (you may change the name and/or port by setting Java "
                    + "system properties 'test.hsqlodbc.port' or "
                    + "'test.hsqlodbc.dsnname'");
            }

            throw new RuntimeException(
                "Failed to set up JDBC/ODBC network connection", se);
        }
    }

    static void setUpServer() {

        if (!inProcessServer) {
            return;
        }

        try {
            server = new Server();

            HsqlProperties properties = new HsqlProperties();

            if (System.getProperty("VERBOSE") == null) {
                server.setLogWriter(null);
                server.setErrWriter(null);
            } else {
                properties.setProperty("server.silent", "false");
                properties.setProperty("server.trace", "false");
            }

            properties.setProperty("server.database.0",
                                   "mem:test;user=SA;password=PW");
            properties.setProperty("server.dbname.0", "test");
            properties.setProperty("server.port", TestOdbcBase.portString);
            server.setProperties(properties);
            server.start();

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {}
        } catch (Exception e) {
            throw new RuntimeException("Failed to set up in-memory database",
                                       e);
        }

        if (server.getState() != ServerConstants.SERVER_STATE_ONLINE) {
            throw new RuntimeException("Server failed to start up");
        }
    }

    protected void enableAutoCommit() {

        try {
            netConn.setAutoCommit(false);
        } catch (SQLException se) {
            junit.framework.AssertionFailedError ase =
                new junit.framework.AssertionFailedError(se.getMessage());

            ase.initCause(se);

            throw ase;
        }
    }

    /**
     * This method allows to easily run this unit test independent of the other
     * unit tests, and without dealing with Ant or unrelated test suites.
     *
     * Invoke like this:<PRE><CODE>
     *  public static void main(String[] sa) {
     *      staticRunner(TestOdbcService.class, sa);
     *  }
     * </CODE></PRE>, but give your subclass name in place of
     * <CODE>TestOdbcService</CODE>
     */
    public static void staticRunner(Class c, String[] sa) {

        TestSuite suite = new TestSuite();

        suite.addTest(new JUnit4TestAdapter(c));
        junit.textui.TestRunner.run(suite);
    }

    abstract protected void populate(Statement st) throws SQLException;
}
