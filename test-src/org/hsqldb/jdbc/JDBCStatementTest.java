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


package org.hsqldb.jdbc;

import org.hsqldb.jdbc.testbase.BaseTestCase;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test of interface java.sql.Statement.
 *
 * @author boucherb@users
 */
public class JDBCStatementTest extends BaseTestCase {

    public JDBCStatementTest(String testName) {
        super(testName);
    }

    protected void setUp() throws Exception {
        super.setUp();

        super.executeScript("setup-sample-data-tables.sql");
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(JDBCStatementTest.class);

        return suite;
    }

    protected Statement newStatement() throws Exception {
        return newConnection().createStatement();
    }

    protected int getExpectedDefaultResultSetHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    protected Class getExpectedWrappedClass() {
        return JDBCStatement.class;
    }

    protected Object getExpectedWrappedObject(Statement stmt, Class<?> ifc) {
        return stmt;
    }

    /**
     * Test of executeQuery method, of interface java.sql.Statement.
     */
    public void testExecuteQuery() throws Exception {
        println("executeQuery");

        Statement stmt = newStatement();

        ResultSet rs = stmt.executeQuery("select * from customer");

        assertEquals(true, rs.next());

        stmt.execute("create table t(id int)");

        StringBuffer sb = new StringBuffer();

        try {
            stmt.executeQuery("insert into t values(1)");
            sb.append("Allowed DML.   ");
        } catch (SQLException ex) {
            //ex.printStackTrace();
        }

        try {
            stmt.executeQuery("create table t2(id int)");
            sb.append("Allowed DDL.");
        } catch (SQLException ex) {
            //ex.printStackTrace();
        }

        if (sb.length() > 0) {
            fail(sb.toString());
        }
    }

    /**
     * Test of executeUpdate method, of interface java.sql.Statement.
     */
    public void testExecuteUpdate() throws Exception {
        println("executeUpdate");

        Statement stmt = newStatement();

        stmt.execute("create table t(id int)");

        int count = stmt.executeUpdate("insert into t values(1)");

        assertEquals(1, count);

        try {
            stmt.executeUpdate("select * from customer");
            fail("Allowed DQL.");
        } catch (SQLException ex) {
            //ex.printStackTrace();
        }
    }

    /**
     * Test of close method, of interface java.sql.Statement.
     */
    public void testClose() throws Exception {
        println("close");

        Statement stmt = newStatement();

        assertEquals(false, stmt.isClosed());

        stmt.close();

        assertEquals(true, stmt.isClosed());

        try {
            stmt.executeQuery("select * from customer");
            fail("Allowed access after close.");
        } catch (SQLException ex) {
        }
    }

    /**
     * Test of getMaxFieldSize method, of interface java.sql.Statement.
     */
    public void testGetMaxFieldSize() throws Exception {
        println("getMaxFieldSize");

        Statement stmt = newStatement();
        int       expResult = 0;
        int       result = stmt.getMaxFieldSize();

        assertEquals(expResult, result);
    }

    /**
     * Test of setMaxFieldSize method, of interface java.sql.Statement.
     */
    public void testSetMaxFieldSize() throws Exception {
        println("setMaxFieldSize");

        int       max = 0;
        Statement stmt = newStatement();

        stmt.setMaxFieldSize(max);
    }

    /**
     * Test of getMaxRows method, of interface java.sql.Statement.
     */
    public void testGetMaxRows() throws Exception {
        println("getMaxRows");

        Statement stmt      = newStatement();
        int       expResult = 0;
        int       result    = stmt.getMaxRows();

        assertEquals(expResult, result);
    }

    /**
     * Test of setMaxRows method, of interface java.sql.Statement.
     */
    public void testSetMaxRows() throws Exception {
        println("setMaxRows");

        int        max = 0;
        Statement stmt = newStatement();

        stmt.setMaxRows(max);
    }

    /**
     * Test of setEscapeProcessing method, of interface java.sql.Statement.
     */
    public void testSetEscapeProcessing() throws Exception {
        println("setEscapeProcessing");

        boolean   enable = true;
        Statement stmt   = newStatement();

        stmt.setEscapeProcessing(enable);

        enable = false;

        stmt.setEscapeProcessing(enable);
    }

    /**
     * Test of getQueryTimeout method, of interface java.sql.Statement.
     */
    public void testGetQueryTimeout() throws Exception {
        println("getQueryTimeout");

        Statement stmt      = newStatement();
        int       expResult = 0;
        int       result    = stmt.getQueryTimeout();

        assertEquals(expResult, result);
    }

    /**
     * Test of setQueryTimeout method, of interface java.sql.Statement.
     */
    public void testSetQueryTimeout() throws Exception {
        println("setQueryTimeout");

        int       seconds = 0;
        Statement stmt    = newStatement();

        stmt.setQueryTimeout(seconds);

        seconds = 1;

        stmt.setQueryTimeout(seconds);
    }

    /**
     * Test of cancel method, of interface java.sql.Statement.
     */
    public void testCancel() throws Exception {
        println("cancel");

        Statement stmt = newStatement();

        stmt.cancel();
    }

    /**
     * Test of getWarnings method, of interface java.sql.Statement.
     */
    public void testGetWarnings() throws Exception {
        println("getWarnings");

        Statement  stmt      = newStatement();
        SQLWarning expResult = null;
        SQLWarning result    = stmt.getWarnings();

        assertEquals(expResult, result);
    }

    /**
     * Test of clearWarnings method, of interface java.sql.Statement.
     */
    public void testClearWarnings() throws Exception {
        println("clearWarnings");

        Statement stmt = newStatement();

        stmt.clearWarnings();
    }

    /**
     * Test of setCursorName method, of interface java.sql.Statement.
     */
    public void testSetCursorName() throws Exception {
        println("setCursorName");

        String    name = "";
        Statement stmt = newStatement();

        stmt.setCursorName(name);
    }

    /**
     * Test of execute method, of interface java.sql.Statement.
     */
    public void testExecute() throws Exception {
        println("execute");

        String    sql       = "select * from customer";
        Statement stmt      = newStatement();
        boolean   expResult = true;
        boolean   result    = stmt.execute(sql);

        assertEquals(expResult, result);

        sql       = "delete from customer where 1=0";
        expResult = false;
        result    = stmt.execute(sql);

        assertEquals(expResult, result);
    }

    /**
     * Test of getResultSet method, of interface java.sql.Statement.
     */
    public void testGetResultSet() throws Exception {
        println("getResultSet");

        String    sql  = "select * from customer";
        Statement stmt = newStatement();

        stmt.execute(sql);

        ResultSet rs = stmt.getResultSet();

        assertNotNull(rs);
    }

    /**
     * Test of getUpdateCount method, of interface java.sql.Statement.
     */
    public void testGetUpdateCount() throws Exception {
        println("getUpdateCount");

        String    sql  = "delete from customer where 1=0";
        Statement stmt = newStatement();

        stmt.execute(sql);

        int expResult = 0;
        int result    = stmt.getUpdateCount();

        assertEquals(expResult, result);
    }

    /**
     * Test of getMoreResults method, of interface java.sql.Statement.
     */
    public void testGetMoreResults() throws Exception {
        println("getMoreResults");

        String    sql  = "select * from customer";
        Statement stmt = newStatement();

        stmt.execute(sql);

        boolean expResult = true;
        boolean result    = stmt.getMoreResults();

        assertEquals(expResult, result);

        sql = "delete from customer where 1=0";

        stmt.execute(sql);

        expResult = false;
        result    = stmt.getMoreResults();

        assertEquals(expResult, result);
    }

    /**
     * Test of setFetchDirection method, of interface java.sql.Statement.
     */
    public void testSetFetchDirection() throws Exception {
        println("setFetchDirection");

        // every driver should support at least FETCH_FORWARD

        int       direction = ResultSet.FETCH_FORWARD;
        Statement stmt      = newStatement();

        stmt.setFetchDirection(direction);

        // optional - just inform if not supported.
        // TODO:  check getWarnings

        direction = ResultSet.FETCH_REVERSE;

        try {
            stmt.setFetchDirection(direction);
        } catch (Exception e) {
            println(e.toString());
        }

        direction = ResultSet.FETCH_UNKNOWN;

        try {
            stmt.setFetchDirection(direction);
        } catch (Exception e) {
            println(e.toString());
        }
    }

    /**
     * Test of getFetchDirection method, of interface java.sql.Statement.
     */
    public void testGetFetchDirection() throws Exception {
        println("getFetchDirection");

        // default direction for every driver should be FETCH_FORWARD
        // TODO:  sets and gets with corresponding check of getWarnings

        Statement stmt = newStatement();
        int expResult  = ResultSet.FETCH_FORWARD;
        int result     = stmt.getFetchDirection();

        assertEquals(expResult, result);
    }

    /**
     * Test of setFetchSize method, of interface java.sql.Statement.
     */
    public void testSetFetchSize() throws Exception {
        println("setFetchSize");

        int       rows = 1;
        Statement stmt = newStatement();

        stmt.setFetchSize(rows);
    }

    /**
     * Test of getFetchSize method, of interface java.sql.Statement.
     */
    public void testGetFetchSize() throws Exception {
        println("getFetchSize");

        // Really, there is no default for all drivers...
        // it should be enough to be able to call the method.
        Statement stmt = newStatement();
        int expResult  = 0;
        int result     = stmt.getFetchSize();

        assertEquals(expResult, result);
    }

    /**
     * Test of getResultSetConcurrency method, of interface java.sql.Statement.
     */
    public void testGetResultSetConcurrency() throws Exception {
        println("getResultSetConcurrency");

        Statement stmt      = newStatement();
        int       expResult = ResultSet.CONCUR_READ_ONLY;
        int       result    = stmt.getResultSetConcurrency();

        assertEquals(expResult, result);
    }

    /**
     * Test of getResultSetType method, of interface java.sql.Statement.
     */
    public void testGetResultSetType() throws Exception {
        println("getResultSetType");

        Statement stmt      = newStatement();
        int       expResult = ResultSet.TYPE_FORWARD_ONLY;
        int       result    = stmt.getResultSetType();

        assertEquals(expResult, result);
    }

    /**
     * Test of addBatch method, of interface java.sql.Statement.
     */
    public void testAddBatch() throws Exception {
        println("addBatch");

        String    sql  = "delete from customer where id = 1";
        Statement stmt = newStatement();

        stmt.addBatch(sql);
    }

    /**
     * Test of clearBatch method, of interface java.sql.Statement.
     */
    public void testClearBatch() throws Exception {
        println("clearBatch");

        Statement stmt = newStatement();

        stmt.addBatch("delete from customer where id = 1");
        stmt.addBatch("delete from customer where id = 2");
        stmt.addBatch("delete from customer where id = 3");

        stmt.clearBatch();
    }

    /**
     * Test of executeBatch method, of interface java.sql.Statement.
     */
    public void testExecuteBatch() throws Exception {
        println("executeBatch");

        Statement stmt = newStatement();

        stmt.addBatch("delete from customer where id = 1");
        stmt.addBatch("delete from customer where id = 2");
        stmt.addBatch("delete from customer where id = 3");
        stmt.addBatch("delete from customer where id = 99999999999999");

        int[] expResult = new int[]{ 1, 1, 1, 0 };
        int[] result    = stmt.executeBatch();

        assertJavaArrayEquals(expResult, result);
    }

    /**
     * Test of getConnection method, of interface java.sql.Statement.
     */
    public void testGetConnection() throws Exception {
        println("getConnection");

        Statement  stmt = newStatement();
        Connection conn = stmt.getConnection();

        assertNotNull(conn);
    }

    /**
     * Test of getGeneratedKeys method, of interface java.sql.Statement.
     */
    public void testGetGeneratedKeys() throws Exception {
        println("getGeneratedKeys");

        // TODO
        fail("TODO: The test case is empty.");
    }

    /**
     * Test of getResultSetHoldability method, of interface java.sql.Statement.
     */
    public void testGetResultSetHoldability() throws Exception {
        println("getResultSetHoldability");

        Statement stmt      = newStatement();
        int       expResult = getExpectedDefaultResultSetHoldability();
        int       result    = stmt.getResultSetHoldability();

        assertEquals(expResult, result);
    }

    /**
     * Test of isClosed method, of interface java.sql.Statement.
     */
    public void testIsClosed() throws Exception {
        println("isClosed");

        Statement stmt = newStatement();

        assertEquals(false, stmt.isClosed());

        stmt.close();

        assertEquals(true, stmt.isClosed());
    }

    /**
     * Test of unwrap method, of interface java.sql.Statement.
     */
    public void testUnwrap() throws Exception {
        println("unwrap");

        Statement  stmt = newStatement();
        Class      wcls = getExpectedWrappedClass();
        Object     wobj = getExpectedWrappedObject(stmt, wcls);

        assertEquals("stmt.unwrap(" + wcls + ").equals(" + wobj + ")",
                     wobj,
                     stmt.unwrap(wcls));
    }

    /**
     * Test of isWrapperFor method, of interface java.sql.Connection.
     */
    public void testIsWrapperFor() throws Exception {
        println("isWrapperFor");

        Statement  stmt = newStatement();
        Class      wcls = getExpectedWrappedClass();

        assertEquals("stmt.isWrapperFor(" + wcls + ")",
                      true,
                      stmt.isWrapperFor(wcls));
    }

    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }

}
