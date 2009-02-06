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
import java.sql.PreparedStatement;

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
 * We use the word <I>query</i> how it is used in the JDBC API, to mean a
 * SELECT statement, not in the more general way as used in the ODBC API.
 * </P> <P>
 * The DSN name and port may be changed from these defaults by setting Java
 * system properties "test.hsqlodbc.dsnname" and/or "test.hsqlodbc.port".
 * </P> <P>
 * Standard test methods perform the named test, then perform a simple
 * (non-prepared) query to verify the state of the server is healthy enough
 * to successfully serve a query.
 * (We may or many not add test(s) to verify behavior when no static query
 * follows).
 * </P> <P>
 * To debug server, network, or connection problems, set Java system property
 * "VERBOSE" (to anything).
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
    protected void tearDown() throws SQLException {
        if (netConn != null) {
            netConn.rollback();
            // Necessary to prevent the SHUTDOWN command from causing implied
            // transaction control commands, which will not be able to
            // complete.
            netConn.createStatement().executeUpdate("SHUTDOWN");
            netConn.close();
            netConn = null;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
            }
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

    public void testSanity() {
        try {
            ResultSet rs = netConn.createStatement().executeQuery(
                "SELECT count(*) FROM nullmix");
            if (!rs.next()) {
                throw new RuntimeException("The most basic query failed.  "
                    + "No row count from 'nullmix'.");
            }
            assertEquals("Sanity check failed.  Rowcount of 'nullmix'", 6,
                rs.getInt(1));
            rs.close();
        } catch (SQLException se) {
            junit.framework.AssertionFailedError ase
                = new junit.framework.AssertionFailedError(
                "The most basic query failed");
            ase.initCause(se);
            throw ase;
        }
    }

    /**
     * Tests with input and output parameters, and rerunning query with
     * modified input parameters.
     */
    public void testFullyPreparedQuery() {
        try {
            ResultSet rs;
            PreparedStatement ps = netConn.prepareStatement(
                "SELECT i, 3, vc, 'str' FROM nullmix WHERE i < ? OR i > ? "
                + "ORDER BY i"); 
            ps.setInt(1, 10);
            ps.setInt(2, 30);
            rs = ps.executeQuery(); 

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(5, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("five", rs.getString(3));

            assertTrue("Not enough rows fetched", rs.next());
            assertEquals(3, rs.getInt(2));
            assertEquals(40, rs.getInt(1));
            assertEquals("forty", rs.getString(3));
            assertEquals("str", rs.getString(4));

            assertFalse("Too many rows fetched", rs.next());
            rs.close();

            ps.setInt(1, 16);
            ps.setInt(2, 100);
            rs = ps.executeQuery(); 

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(5, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("five", rs.getString(3));

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(10, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("ten", rs.getString(3));

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(15, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("fifteen", rs.getString(3));

            assertFalse("Too many rows fetched", rs.next());
            rs.close();

            verifySimpleQueryOutput(); // Verify server state still good
        } catch (SQLException se) {
            junit.framework.AssertionFailedError ase
                = new junit.framework.AssertionFailedError(se.getMessage());
            ase.initCause(se);
            throw ase;
        }
    }

    public void testDetailedSimpleQueryOutput() {
        try {
            verifySimpleQueryOutput();
        } catch (SQLException se) {
            junit.framework.AssertionFailedError ase
                = new junit.framework.AssertionFailedError(se.getMessage());
            ase.initCause(se);
            throw ase;
        }
    }

    /**
     * Assumes none of the records above i=20 have been modified.
     */
    public void verifySimpleQueryOutput() throws SQLException {
        ResultSet rs = netConn.createStatement().executeQuery(
            "SELECT i, 3, vc, 'str' FROM nullmix WHERE i > 20 ORDER BY i");
        assertTrue("No rows fetched", rs.next());
        assertEquals("str", rs.getString(4));
        assertEquals(21, rs.getInt(1));
        assertEquals(3, rs.getInt(2));
        assertEquals("twenty one", rs.getString(3));

        assertTrue("Not enough rows fetched", rs.next());
        assertEquals(3, rs.getInt(2));
        assertEquals(25, rs.getInt(1));
        assertNull(rs.getString(3));
        assertEquals("str", rs.getString(4));

        assertTrue("Not enough rows fetched", rs.next());
        assertEquals("str", rs.getString(4));
        assertEquals(3, rs.getInt(2));
        assertEquals(40, rs.getInt(1));
        assertEquals("forty", rs.getString(3));

        assertFalse("Too many rows fetched", rs.next());
        rs.close();
    }

    public void testPreparedNonRowStatement() {
        try {
            PreparedStatement ps = netConn.prepareStatement(
                    "UPDATE nullmix set xtra = ? WHERE i < ?"); 
            ps.setString(1, "first");
            ps.setInt(2, 25);
            assertEquals("First update failed", 4, ps.executeUpdate());

            ps.setString(1, "second");
            ps.setInt(2, 15);
            assertEquals("Second update failed", 2, ps.executeUpdate());
            ps.close();


            ResultSet rs = netConn.createStatement().executeQuery(
                "SELECT i, 3, vc, xtra FROM nullmix ORDER BY i"); 

            assertTrue("No rows fetched", rs.next());
            assertEquals("second", rs.getString(4));
            assertEquals(5, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("five", rs.getString(3));

            assertTrue("No rows fetched", rs.next());
            assertEquals("second", rs.getString(4));
            assertEquals(10, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("ten", rs.getString(3));

            assertTrue("No rows fetched", rs.next());
            assertEquals("first", rs.getString(4));
            assertEquals(15, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("fifteen", rs.getString(3));

            assertTrue("Not enough rows fetched", rs.next());
            assertEquals(3, rs.getInt(2));
            assertEquals(21, rs.getInt(1));
            assertEquals("twenty one", rs.getString(3));
            assertEquals("first", rs.getString(4));

            assertTrue("Not enough rows fetched", rs.next());
            assertEquals(3, rs.getInt(2));
            assertEquals(25, rs.getInt(1));
            assertNull(rs.getString(3));
            assertNull(rs.getString(4));

            assertTrue("Not enough rows fetched", rs.next());
            assertEquals(3, rs.getInt(2));
            assertEquals(40, rs.getInt(1));
            assertEquals("forty", rs.getString(3));
            assertNull(rs.getString(4));

            assertFalse("Too many rows fetched", rs.next());
            rs.close();
        } catch (SQLException se) {
            junit.framework.AssertionFailedError ase
                = new junit.framework.AssertionFailedError(se.getMessage());
            ase.initCause(se);
            throw ase;
        }
    }

    public void testParamlessPreparedQuery() {
        try {
            ResultSet rs;
            PreparedStatement ps = netConn.prepareStatement(
                "SELECT i, 3, vc, 'str' FROM nullmix WHERE i != 21 "
                + "ORDER BY i"); 
            rs = ps.executeQuery(); 

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(5, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("five", rs.getString(3));

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(10, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("ten", rs.getString(3));

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(15, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("fifteen", rs.getString(3));

            assertTrue("Not enough rows fetched", rs.next());
            assertEquals(3, rs.getInt(2));
            assertEquals(25, rs.getInt(1));
            assertNull(rs.getString(3));
            assertEquals("str", rs.getString(4));

            assertTrue("Not enough rows fetched", rs.next());
            assertEquals(3, rs.getInt(2));
            assertEquals(40, rs.getInt(1));
            assertEquals("forty", rs.getString(3));
            assertEquals("str", rs.getString(4));

            assertFalse("Too many rows fetched", rs.next());
            rs.close();

            rs = ps.executeQuery(); 

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(5, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("five", rs.getString(3));

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(10, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("ten", rs.getString(3));

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(15, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("fifteen", rs.getString(3));

            assertTrue("Not enough rows fetched", rs.next());
            assertEquals(3, rs.getInt(2));
            assertEquals(25, rs.getInt(1));
            assertNull(rs.getString(3));
            assertEquals("str", rs.getString(4));

            assertTrue("Not enough rows fetched", rs.next());
            assertEquals(3, rs.getInt(2));
            assertEquals(40, rs.getInt(1));
            assertEquals("forty", rs.getString(3));
            assertEquals("str", rs.getString(4));

            assertFalse("Too many rows fetched", rs.next());
            rs.close();

            verifySimpleQueryOutput(); // Verify server state still good
        } catch (SQLException se) {
            junit.framework.AssertionFailedError ase
                = new junit.framework.AssertionFailedError(se.getMessage());
            ase.initCause(se);
            throw ase;
        }
    }

    public void testSimpleUpdate() {
        try {
            Statement st = netConn.createStatement();
            assertEquals(2, st.executeUpdate(
                    "UPDATE nullmix SET xtra = 'updated' WHERE i < 12"));
            ResultSet rs = netConn.createStatement().executeQuery(
                "SELECT * FROM nullmix WHERE xtra = 'updated'");
            assertTrue("No rows updated", rs.next());
            assertTrue("Only one row updated", rs.next());
            assertFalse("Too many rows updated", rs.next());
            rs.close();
        } catch (SQLException se) {
            junit.framework.AssertionFailedError ase
                = new junit.framework.AssertionFailedError(se.getMessage());
            ase.initCause(se);
            throw ase;
        }
    }

    private void enableAutoCommit() {
        try {
            netConn.setAutoCommit(false);
        } catch (SQLException se) {
            junit.framework.AssertionFailedError ase
                = new junit.framework.AssertionFailedError(se.getMessage());
            ase.initCause(se);
            throw ase;
        }
    }

    public void testTranSanity() {
        enableAutoCommit();
        testSanity();
    }
    public void testTranFullyPreparedQuery() {
        enableAutoCommit();
        testFullyPreparedQuery();
    }
    public void testTranDetailedSimpleQueryOutput() {
        enableAutoCommit();
        testDetailedSimpleQueryOutput();
    }
    public void testTranPreparedNonRowStatement() {
        enableAutoCommit();
        testPreparedNonRowStatement();
    }
    public void testTranParamlessPreparedQuery() {
        enableAutoCommit();
        testParamlessPreparedQuery();
    }
    public void testTranSimpleUpdate() {
        enableAutoCommit();
        testSimpleUpdate();
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
        st.executeUpdate("INSERT INTO nullmix (i, vc) values(15, 'fifteen')");
        st.executeUpdate(
                "INSERT INTO nullmix (i, vc) values(21, 'twenty one')");
        st.executeUpdate("INSERT INTO nullmix (i, vc) values(40, 'forty')");
        st.executeUpdate("INSERT INTO nullmix (i) values(25)");
        st.close();
    }
}
