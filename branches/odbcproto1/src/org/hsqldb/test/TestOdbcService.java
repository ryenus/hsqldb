/* Copyright (c) 2001-2009, The HSQL Development Group
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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.Server;
import org.hsqldb.server.ServerConstants;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * You MUST have a native (non-Java) ODBC DSN configured with the HyperSQL
 * ODBC driver, DSN name "HSQLDB_UTEST", DSN database "/", port "9797".
 * The user name and password don't matter.
 * <P>
 * The DSN name and port may be changed from these defaults by setting Java
 * system properties "test.hsqlodbc.dsnname" and/or "test.hsqlodbc.port".
 * </P>
 */
public class TestOdbcService extends junit.framework.TestCase {

    public TestOdbcService() {}
    protected Connection netConn = null;
    protected Server server = null;
    static private String portString = null;
    static private String dsnName = null;

    static {
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");

            // TODO:  Rename to upper-class JDBC driver class name
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(
                "<clinit> failed.  JDBC Driver class not in CLASSPATH");
        }
        portString = System.getProperty("test.hsqlodbc.port");
        dsnName = System.getProperty("test.hsqlodbc.dsnname");
        if (portString == null) {
            portString = "9797";
        }
        if (dsnName == null) {
            dsnName = "HSQLDB_UTEST";
        }
    }

    /**
     * Accommodate JUnit's test-runner conventions.
     */
    public TestOdbcService(String s) {
        super(s);
    }

    /**
     * JUnit convention for cleanup.
     *
     * Called after each test*() method.
     */
    protected void tearDown() {
        if (netConn != null) try {
            netConn.createStatement().executeUpdate("SHUTDOWN");
            netConn.close();
            netConn = null;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
            }
        } catch (SQLException se) {
            se.printStackTrace();
        }
        if (server != null
            && server.getState() != ServerConstants.SERVER_STATE_SHUTDOWN) {
            throw new RuntimeException("Server failed to shut down");
        }
    }

    /**
     * Specifically, this opens a mem-only DB, populates it, starts a
     * HyperSQL Server to server it, and opens network JDBC Connection
     * "netConn" to it,
     *
     * Invoked before each test*() invocation by JUnit.
     */
    protected void setUp() {
        try {
            Connection setupConn = DriverManager.getConnection(
                "jdbc:hsqldb:mem:test", "SA", "");
            setupConn.setAutoCommit(false);
            populate(setupConn);
            setupConn.commit();
            setupConn.close();
        } catch (SQLException se) {
            throw new RuntimeException(
                "Failed to set up in-memory database", se);
        }
        try {
            server = new Server();
            HsqlProperties properties = new HsqlProperties();
            if (System.getProperty("VERBOSE") == null) {
                server.setLogWriter(null);
                server.setErrWriter(null);
            } else {
                properties.setProperty("server.silent", "false");
                properties.setProperty("server.trace", "true");
            }
            properties.setProperty("server.database.0", "mem:test");
            properties.setProperty("server.dbname.0", "");
            properties.setProperty("server.port", TestOdbcService.portString);
            server.setProperties(properties);
            server.start();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
            }
        } catch (Exception e) {
            throw new RuntimeException(
                "Failed to set up in-memory database", e);
        }
        if (server.getState() != ServerConstants.SERVER_STATE_ONLINE) {
            throw new RuntimeException("Server failed to start up");
        }
        try {
            netConn = DriverManager.getConnection(
                "jdbc:odbc:" + dsnName, "SA", "sapwd");
            netConn.setAutoCommit(false);
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

    public void testSanity() {
        try {
            ResultSet rs = netConn.createStatement().executeQuery(
                "SELECT count(*) FROM nullmix");
            if (!rs.next()) {
                throw new RuntimeException("The most basic query failed.  "
                    + "No row count from 'nullmix'.");
            }
            assertEquals("Sanity check failed.  Rowcount of 'nullmix'", 3,
                rs.getInt(1));
            rs.close();
        } catch (SQLException se) {
            throw new RuntimeException("The most basic query failed: " + se);
        }
    }

    /**
     * This method allows to easily run this unit test independent of the other
     * unit tests, and without dealing with Ant or unrelated test suites.
     */
    static public void main(String[] sa) {
        if (sa.length > 0 && sa[0].startsWith("-g")) {
            junit.swingui.TestRunner.run(TestOdbcService.class);
        } else {
            junit.textui.TestRunner runner = new junit.textui.TestRunner();
            junit.framework.TestResult result =
                runner.run(runner.getTest(TestOdbcService.class.getName()));

            System.exit(result.wasSuccessful() ? 0 : 1);
        }
    }

    protected void populate(Connection c) throws SQLException {
        Statement st = c.createStatement();
        st.executeUpdate("SET PASSWORD 'sapwd'");
        st.executeUpdate("DROP TABLE nullmix IF EXISTS");
        st.executeUpdate("CREATE TABLE nullmix "
                + "(i INT NOT NULL, vc VARCHAR(20), xtra VARCHAR(20))");

        // Would be more elegant and efficient ot use a prepared statement
        // here, but our we want this setup to be as simple as possible, and
        // leave feature testing for the actual unit tests.
        st.executeUpdate("INSERT INTO nullmix (i, vc) values(10, 'ten')");
        st.executeUpdate("INSERT INTO nullmix (i, vc) values(5, 'five')");
        st.executeUpdate("INSERT INTO nullmix (i) values(25)");
        st.close();
    }
}
