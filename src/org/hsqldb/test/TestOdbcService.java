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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * See AbstractTestOdbc for more general ODBC test information.
 *
 * Standard test methods perform the named test, then perform a simple
 * (non-prepared) query to verify the state of the server is healthy enough
 * to successfully serve a query.
 * (We may or many not add test(s) to verify behavior when no static query
 * follows).
 *
 * @see AbstractTestOdbc
 */
public class TestOdbcService extends AbstractTestOdbc {

    public TestOdbcService() {}

    /**
     * Accommodate JUnit's test-runner conventions.
     */
    public TestOdbcService(String s) {
        super(s);
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

    protected void populate(Statement st) throws SQLException {
        st.executeUpdate("DROP TABLE nullmix IF EXISTS");
        st.executeUpdate("CREATE TABLE nullmix "
                + "(i INT NOT NULL, vc VARCHAR(20), xtra VARCHAR(20))");

        // Would be more elegant and efficient to use a prepared statement
        // here, but our we want this setup to be as simple as possible, and
        // leave feature testing for the actual unit tests.
        st.executeUpdate("INSERT INTO nullmix (i, vc) values(10, 'ten')");
        st.executeUpdate("INSERT INTO nullmix (i, vc) values(5, 'five')");
        st.executeUpdate("INSERT INTO nullmix (i, vc) values(15, 'fifteen')");
        st.executeUpdate(
                "INSERT INTO nullmix (i, vc) values(21, 'twenty one')");
        st.executeUpdate("INSERT INTO nullmix (i, vc) values(40, 'forty')");
        st.executeUpdate("INSERT INTO nullmix (i) values(25)");
    }

    public static void main(String[] sa) {
        staticRunner(TestOdbcService.class, sa);
    }
}
