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


package org.hsqldb.jdbc;

import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
//import java.sql.DataSet;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.util.Map;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;
import org.hsqldb.testbase.ForSubject;

/**
 * Test of interface java.sql.Connection.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
@ForSubject(JDBCConnection.class)
public class JDBCConnectionTest extends BaseJdbcTestCase {

    public JDBCConnectionTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(JDBCConnectionTest.class);

        return suite;
    }

    protected void setUpSampleData() throws Exception {
        executeScript("setup-sample-data-tables.sql");
    }

    protected Class getExpectedWrappedClass() {
        return JDBCConnection.class;
    }

    protected Object getExpectedWrappedObject(Connection conn, Class<?> ifc) {
        return conn;
    }

    /**
     * Test of createStatement method, of interface java.sql.Connection.
     */
    public void testCreateStatement() throws Exception {
        Connection conn = newConnection();
        Statement stmt = connectionFactory().createStatement(conn);

        stmt.close();
        conn.close();
    }

    /**
     * Test of createStatement method, of interface java.sql.Connection.
     */
    public void testCreateStatement_with_connection_holability_of_HOLD_CURSORS_OVER_COMMIT() throws Exception
    {
        Connection conn = newConnection();

        conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);

        if (conn.getWarnings() == null)
        {
            assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT,
                    conn.getHoldability());

            Statement stmt = connectionFactory().createStatement(conn);

            assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT,
                    stmt.getResultSetHoldability());

            stmt.close();
        }

        conn.close();
    }

    /**
     * Test of createStatement method, of interface java.sql.Connection.
     */
    public void testCreateStatement_with_connection_holability_of_CLOSE_CURSORS_AT_COMMIT() throws Exception
    {
        Connection conn = newConnection();
        Statement stmt;

        conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);

        if (conn.getWarnings() == null)
        {
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
                    conn.getHoldability());

            stmt = connectionFactory().createStatement(conn);

            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT,
                    stmt.getResultSetHoldability());
        }
    }

    /**
     * Test of prepareStatement method, of interface java.sql.Connection.
     */
    public void testPrepareStatement() throws Exception {
        setUpSampleData();

        StringBuilder      sb   = new StringBuilder();
        String            sql  = "select * from customer";
        Connection        conn = newConnection();
        PreparedStatement stmt;

        try {
            stmt = conn.prepareStatement(sql);
        } catch (SQLException ex) {
            fail(ex.toString());
        }

        sql = "select * from customer; select * from item";

        try {
            stmt = conn.prepareStatement(sql);
            sb.append("Allowed prepare > 1 statements in a single prepare call.    ");
        } catch (SQLException ex) {

        }

        sql = "select * from customer where customer.id = ?";

        try {
            stmt = conn.prepareStatement(sql);
        } catch (SQLException ex) {
            sb.append(ex.toString()).append("    ");
        }

        sql = "create table ? (id int, val varchar)";

        try {
            stmt = conn.prepareStatement(sql);
            sb.append("Allowed DDL with table name parameter marker.    ");
        } catch (SQLException ex) {

        }

        sql = "create table test (? int, val varchar)";

        try {
            stmt = conn.prepareStatement(sql);
            sb.append("Allowed DDL with column name parameter marker.    ");
        } catch (SQLException ex) {

        }

        sql = "create table test (id ?, val varchar)";

        try {
            stmt = conn.prepareStatement(sql);
            sb.append("Allowed DDL with column type parameter marker.    ");
        } catch (SQLException ex) {

        }

        sql = "SET AUTOCOMMIT ?";

        try {
            stmt = conn.prepareStatement(sql);
            sb.append("Allowed DDL with value parameter marker.    ");
        } catch (SQLException ex) {

        }

        if (sb.length() > 0) {
            fail(sb.toString());
        }
    }

    /**
     * Test of prepareCall method, of interface java.sql.Connection.
     */
    public void testPrepareCall() throws Exception {
        String            sql  = "CALL PI() + ?";
        Connection        conn = newConnection();
        CallableStatement stmt = conn.prepareCall(sql);

        stmt.setDouble(1, Math.PI);

        ResultSet rs = stmt.executeQuery();

        rs.next();

        assertEquals((double)2*Math.PI, rs.getDouble(1));
    }

    /**
     * Test of nativeSQL method, of interface java.sql.Connection.
     */
    public void testNativeSQL() throws Exception {
        String     sql       = "{?= call abs(?)}";
        Connection conn      = newConnection();
        String     expResult = "call abs(?)";
        String     result    = conn.nativeSQL(sql).toLowerCase();

        assertEquals(expResult, result.trim());
    }

    /**
     * Test of setAutoCommit method, of interface java.sql.Connection.
     */
    public void testSetAutoCommit() throws Exception {
        boolean    autoCommit = false;
        Connection conn       = newConnection();

        try {
            conn.setAutoCommit(autoCommit);
        } catch (Exception e) {
            fail(e.toString());
        }

        autoCommit = true;

        try {
            conn.setAutoCommit(autoCommit);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of getAutoCommit method, of interface java.sql.Connection.
     */
    public void testGetAutoCommit() throws Exception {
        Connection conn      = newConnection();
        boolean    expResult = true;
        boolean    result    = conn.getAutoCommit();

        assertEquals(expResult, result);

        expResult = false;

        conn.setAutoCommit(expResult);

        result = conn.getAutoCommit();

        assertEquals(expResult, result);
    }

    /**
     * Test of commit method, of interface java.sql.Connection.
     */
    public void testCommit() throws Exception {
        Connection conn = newConnection();

        conn.commit();
    }

    /**
     * Test of rollback method, of interface java.sql.Connection.
     */
    public void testRollback() throws Exception {
        Connection conn = newConnection();

        conn.rollback();
    }

    /**
     * Test of close method, of interface java.sql.Connection.
     */
    public void testClose() throws Exception {
        Connection conn = newConnection();

        conn.close();
    }

    /**
     * Test of isClosed method, of interface java.sql.Connection.
     */
    public void testIsClosed() throws Exception {
        Connection conn      = newConnection();
        boolean    expResult = false;
        boolean    result    = conn.isClosed();

        assertEquals(expResult, result);

        expResult = true;

        conn.close();

        result = conn.isClosed();

        assertEquals(expResult, result);
    }

    /**
     * Test of getMetaData method, of interface java.sql.Connection.
     */
    public void testGetMetaData() throws Exception {
        Connection conn = newConnection();

        try {
            DatabaseMetaData dbmd = conn.getMetaData();
        } catch (Exception ex) {
            fail(ex.toString());
        }
    }

    /**
     * Test of setReadOnly method, of interface java.sql.Connection.
     */
    public void testSetReadOnly() throws Exception {
        boolean    expResult = true;
        Connection conn      = newConnection();

        try {
            conn.setReadOnly(expResult);
        } catch (SQLException ex) {
            fail(ex.toString());
        }

        boolean result = conn.isReadOnly();

        assertEquals(expResult, result);

        expResult = false;

        try {
            conn.setReadOnly(expResult);
        } catch (SQLException ex) {
            fail(ex.toString());
        }

        result = conn.isReadOnly();

        assertEquals(expResult, result);

        executeScript("setup-dual-table.sql");

        conn.setReadOnly(true);

        Statement stmt = connectionFactory().createStatement(conn);

        try {
            stmt.executeUpdate("insert into dual values 'read-only'");

            fail("Allowed write while readonly");
        } catch(Exception e) {}

        conn.setReadOnly(false);

        try {
            stmt.executeUpdate("insert into dual values 'read-write'");

        } catch(Exception e) {
             fail("Insert failed while read-write");
        }
    }

    /**
     * Test of isReadOnly method, of interface java.sql.Connection.
     */
    public void testIsReadOnly() throws Exception {
        Connection conn      = newConnection();
        boolean    expResult = false;
        boolean    result    = conn.isReadOnly();

        assertEquals(expResult, result);

        expResult = true;

        conn.setReadOnly(expResult);

        result = conn.isReadOnly();

        assertEquals(expResult, result);
    }

    /**
     * Test of setCatalog method, of interface java.sql.Connection.
     */
    public void testSetCatalog() throws Exception {
        String     catalog = "PUBLIC";
        Connection conn    = newConnection();

        try {
            conn.setCatalog(catalog);
        } catch (SQLException ex) {
            fail("Failed setCatalog(PUBLIC): " + ex);
        }

        catalog = "acatalog";

        try {
            conn.setCatalog(catalog);
            fail("Allowed setCatalog(acatalog)");
        } catch (Exception ex) {
        }

        conn.close();

        catalog = "CATALOG";

        try {
            conn.setCatalog(catalog);
            fail("Allowed setCatalog(null) on closed connection");
        } catch (SQLException ex) {
        }

        catalog = "acatalog";

        try {
            conn.setCatalog(catalog);

            fail("Allowed setCatalog(acatalog) on closed connection");
        } catch (Exception e) {
        }
    }

    /**
     * Test of getCatalog method, of interface java.sql.Connection.
     */
    public void testGetCatalog() throws Exception {
        Connection conn = newConnection();

        String expResult = "PUBLIC";
        String result    = conn.getCatalog();

        assertEquals(expResult, result);
    }

    /**
     * Test of setTransactionIsolation method, of interface java.sql.Connection.
     */
    public void testSetTransactionIsolation() throws Exception {
        int[] levels = new int[] {
            Connection.TRANSACTION_READ_UNCOMMITTED,
            Connection.TRANSACTION_READ_COMMITTED,
            Connection.TRANSACTION_REPEATABLE_READ,
            Connection.TRANSACTION_SERIALIZABLE
        };

        Connection conn = newConnection();

        StringBuilder failures = new StringBuilder();

        for (int i = 0; i < levels.length; i++) {
            try {
                conn.setTransactionIsolation(levels[i]);
            } catch (Exception e) {

                if (failures.length() > 0) {
                    failures.append(", ");
                }

                switch (i) {
                    case 0 : {
                        failures.append("TRANSACTION_READ_UNCOMMITTED: ");
                        break;
                    }
                    case 1 : {
                        failures.append("TRANSACTION_READ_COMMITTED: ");
                        break;
                    }
                    case 2 : {
                        failures.append("TRANSACTION_REPEATABLE_READ: ");
                        break;
                    }
                    case 3 : {
                        failures.append("TRANSACTION_SERIALIZABLE: ");
                        break;
                    }
                }

                failures.append(e.getMessage() == null ? e.toString() : e.getMessage());
            }
        }

        if (failures.length() > 0){
            fail(failures.toString());
        }
    }

    /**
     * Test of getTransactionIsolation method, of interface java.sql.Connection.
     */
    public void testGetTransactionIsolation() throws Exception {
        Connection conn      = newConnection();
        int        expResult = Connection.TRANSACTION_READ_COMMITTED;
        int        result    = conn.getTransactionIsolation();

        assertEquals(expResult, result);

        int[] levels = new int[] {
            Connection.TRANSACTION_READ_UNCOMMITTED,
            Connection.TRANSACTION_READ_COMMITTED,
            Connection.TRANSACTION_REPEATABLE_READ,
            Connection.TRANSACTION_SERIALIZABLE
        };

        boolean wasSet;

        for (int i = 0; i < levels.length; i++) {
            wasSet = false;

            try {
                conn.setTransactionIsolation(levels[i]);
                wasSet = true;
            } catch (Exception e) {
            }

            if (wasSet)
            {
                SQLWarning warning = conn.getWarnings();

                if (warning == null)
                {
                    super.assertTrue(
                            "Reported Isolation:",
                            levels[i] <=
                            conn.getTransactionIsolation());
                }
                else
                {
                    while(warning != null)
                    {
                        printWarning(warning);

                        warning = warning.getNextWarning();
                    }
                }
            }
        }
    }

    /**
     * Test of getWarnings method, of interface java.sql.Connection.
     */
    public void testGetWarnings() throws Exception {
        Connection conn      = newConnection();
        SQLWarning expResult = null;
        SQLWarning result    = conn.getWarnings();

        assertEquals(expResult, result);
    }

    /**
     * Test of clearWarnings method, of interface java.sql.Connection.
     */
    public void testClearWarnings() throws Exception {
        Connection conn = newConnection();

        conn.clearWarnings();
    }

    /**
     * Test of getTypeMap method, of interface java.sql.Connection.
     */
    public void testGetTypeMap() throws Exception {
        if(!getBooleanProperty("test.typemap", true)) {
            return;
        }

        Connection conn = newConnection();

        Map<String, Class<?>> result = conn.getTypeMap();
        assertTrue(result != null);
    }

    /**
     * Test of setTypeMap method, of interface java.sql.Connection.
     */
    public void testSetTypeMap() throws Exception {
        if(!getBooleanProperty("test.typemap", true)) {
            return;
        }

        Map<String, Class<?>> map = null;
        Connection conn = newConnection();

        try {
            conn.setTypeMap(map);
        } catch (Throwable t) {
            assertTrue(t instanceof java.sql.SQLFeatureNotSupportedException);
        }
    }

    /**
     * Test of setHoldability method, of interface java.sql.Connection.
     */
    public void testSetHoldability_HOLD_CURSORS_OVER_COMMIT() throws Exception {
        int        holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
        Connection conn        = newConnection();

        conn.setHoldability(holdability);

        int actualHoldability = conn.getHoldability();

        SQLWarning warning = conn.getWarnings();

        if (warning == null)
        {
            assertEquals("Holdability:", holdability, actualHoldability);
        }
        else
        {
            while(warning != null)
            {
                printWarning(warning);

                warning = warning.getNextWarning();
            }
        }
    }

    /**
     * Test of setHoldability method, of interface java.sql.Connection.
     */
    public void testSetHoldability_CLOSE_CURSORS_AT_COMMIT() throws Exception {
        int        holdability = ResultSet.CLOSE_CURSORS_AT_COMMIT;
        Connection conn        = newConnection();

        conn.setHoldability(holdability);

        int actualHoldability = conn.getHoldability();

        SQLWarning warning = conn.getWarnings();

        if (warning == null)
        {
            assertEquals("Holdability:", holdability, actualHoldability);
        }
        else
        {
            while(warning != null)
            {
                printWarning(warning);

                warning = warning.getNextWarning();
            }
        }
    }

    /**
     * Test of getHoldability method, of interface java.sql.Connection.
     */
    public void testGetHoldability() throws Exception {
        Connection conn = newConnection();
        int expResult   = conn.getMetaData().getResultSetHoldability();
        int result      = conn.getHoldability();

        assertEquals(expResult, result);
    }

    /**
     * Test of setSavepoint method, of interface java.sql.Connection.
     */
    public void testSetSavepoint() throws Exception {
        Connection conn = newConnection();

        conn.setAutoCommit(true);

        try
        {
             Savepoint result = conn.setSavepoint("s1");
             fail("Allowed setSavepoint(name) in autoCommit mode.");
        } catch (Exception e){}

        conn.setAutoCommit(false);

        try {
            Savepoint result = conn.setSavepoint("s2");
        } catch (Exception e) {
             fail("setSavepoint(name): " + e.toString());
        }

        conn.setAutoCommit(true);

        try
        {
             Savepoint result = conn.setSavepoint();
             fail("Allowed setSavepoint() [no args] in autoCommit mode.");
        } catch (Exception e){}

        conn.setAutoCommit(false);

        try {
            Savepoint result = conn.setSavepoint();
        } catch (Exception e) {
             fail("setSavepoint() [no args]: " + e.toString());
        }
    }

    /**
     * Test of releaseSavepoint method, of interface java.sql.Connection.
     */
    public void testReleaseSavepoint() throws Exception {
        Savepoint  savepoint;
        Connection conn      = newConnection();

        assertEquals(true, conn.getAutoCommit());

        try {
            savepoint = conn.setSavepoint("sp1");

            fail("Allowed setSavepoint while autoCommit == true.");
        } catch (SQLException ex) {
            //ex.printStackTrace();
        }

        conn.setAutoCommit(false);

        savepoint = conn.setSavepoint("sp1");

        conn.releaseSavepoint(savepoint);

        try {
            conn.releaseSavepoint(savepoint);

            fail("Allowed releaseSavepoint on invalid Savepoint object");
        } catch (SQLException ex) {
            //ex.printStackTrace();
        }

        savepoint = conn.setSavepoint("sp1");

        conn.setAutoCommit(true);

        try {
            conn.releaseSavepoint(savepoint);

            fail("Allowed releaseSavepoint while autoCommit == true");
        } catch (SQLException ex) {
            //ex.printStackTrace();
        }

        conn.setAutoCommit(false);

        savepoint = conn.setSavepoint("sp1");

        conn.close();

        try {
            conn.releaseSavepoint(savepoint);

            fail("Allowed releaseSavepoint on closed connection");
        } catch (SQLException ex) {
            //ex.printStackTrace();
        }
    }

    /**
     * Test of createClob method, of interface java.sql.Connection.
     */
    public void testCreateClob() throws Exception {
        Connection conn = newConnection();

        try {
            Clob result = conn.createClob();
        } catch (SQLException ex) {
            fail(ex.toString());
        }
    }

    /**
     * Test of createBlob method, of interface java.sql.Connection.
     */
    public void testCreateBlob() throws Exception {
        Connection conn = newConnection();

        try {
            Blob result = conn.createBlob();
        } catch (SQLException ex) {
            fail(ex.toString());
        }
    }

    /**
     * Test of createNClob method, of interface java.sql.Connection.
     */
    public void testCreateNClob() throws Exception {
        Connection conn = newConnection();

        try {
            NClob result = conn.createNClob();
        } catch (SQLException ex) {
            fail(ex.toString());
        }
    }

    /**
     * Test of createSQLXML method, of interface java.sql.Connection.
     */
    public void testCreateSQLXML() throws Exception {
        Connection conn = newConnection();

        try {
            SQLXML result = conn.createSQLXML();
        } catch (SQLException ex) {
            fail(ex.toString());
        }
    }

    /**
     * Test of isValid method, of interface java.sql.Connection.
     */
    public void testIsValid() throws Exception {
        int        timeout = 500;
        Connection conn    = newConnection();
        boolean expResult  = true;
        boolean result     = conn.isValid(timeout);

        assertEquals("isValid", expResult, result);

        conn.close();

        expResult = false;
        result    = conn.isValid(timeout);

        assertEquals("isValid", expResult, result);
    }

    /**
     * Test of setClientInfo method, of interface java.sql.Connection.
     */
    public void testSetClientInfo() throws Exception {
        String     name  = "a";
        String     value = "b";
        Connection conn  = newConnection();

        try {
            conn.setClientInfo(name, value);
        } catch (SQLException ex) {
            return;
        }

        fail("no exception was thrown");

    }

    /**
     * Test of getClientInfo method, of interface java.sql.Connection.
     */
    public void testGetClientInfo() throws Exception {
        String     name      = "a";
        Connection conn      = newConnection();
        String     expResult = null;
        String     result    = null;

        try {
            result = conn.getClientInfo(name);
        } catch (SQLException ex) {
            fail(ex.toString());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of createQueryObject method, of interface java.sql.Connection.
     */
//    public void testCreateQueryObject() throws Exception {
//        println("createQueryObject");
//
//        setUpSampleData();
//
//        Class<CustomerDao> ifc         = CustomerDao.class;
//        Connection         conn        = newConnection();
//        CustomerDao        customerDao = conn.createQueryObject(ifc);
//        DataSet<Customer>  customers   = customerDao.getAllCustomers();
//        ResultSet          rs          = conn.createStatement()
//                                            .executeQuery(
//                "select id, firstname, lastname, street, city from customer order by 1");
//
//        for (Customer customer: customers) {
//            rs.next();
//
//            assertEquals("customer.id", rs.getInt(1), customer.id.intValue());
//            assertEquals("customer.firstname", rs.getString(2), customer.firstname);
//            assertEquals("customer.lastname", rs.getString(3), customer.lastname);
//            assertEquals("customer.street", rs.getString(4), customer.street);
//            assertEquals("customer.city", rs.getString(5), customer.city);
//
//        }
//
//        // TODO:  test update and delete functions.
//    }

    /**
     * Test of unwrap method, of interface java.sql.Connection.
     */
    public void testUnwrap() throws Exception {
        Connection conn = newConnection();
        Class      wcls = getExpectedWrappedClass();
        Object     wobj = getExpectedWrappedObject(conn, wcls);

        assertEquals("conn.unwrap(" + wcls + ").equals(" + wobj + ")",
                     wobj,
                     conn.unwrap(wcls));
    }

    /**
     * Test of isWrapperFor method, of interface java.sql.Connection.
     */
    public void testIsWrapperFor() throws Exception {
        Connection conn = newConnection();
        Class      wcls = getExpectedWrappedClass();

        assertEquals("conn.isWrapperFor(" + wcls + ")",
                      true,
                      conn.isWrapperFor(wcls));
    }

    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }

}
