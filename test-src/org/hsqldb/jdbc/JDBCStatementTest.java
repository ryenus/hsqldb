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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 * Test of interface java.sql.Statement.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
@ForSubject(Statement.class)
public class JDBCStatementTest extends BaseJdbcTestCase {

    public JDBCStatementTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        executeScript("setup-sample-data-tables.sql");
    }

    @Override
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

    protected Class<?> getExpectedWrappedClass() {
        return JDBCStatement.class;
    }

    protected Object getExpectedWrappedObject(Statement stmt, Class<?> ifc) {
        return stmt;
    }

    /**
     * Test of executeQuery method, of interface java.sql.Statement.
     */
    @OfMethod("executeQuery(java.lang.String)")
    public void testExecuteQuery() throws Exception {
        Statement stmt = newStatement();

        ResultSet rs = stmt.executeQuery("select * from customer");

        assertEquals(true, rs.next());

        stmt.execute("create table t(id int)");

        StringBuilder sb = new StringBuilder();

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
    @OfMethod("executeUpdate(java.lang.String)")
    public void testExecuteUpdate() throws Exception {
        Statement stmt = newStatement();

        stmt.execute("drop table t if exists");
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
    @OfMethod("close()")
    public void testClose() throws Exception {
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
    @OfMethod("getMaxFieldSize()")
    public void testGetMaxFieldSize() throws Exception {
        Statement stmt = newStatement();
        int       expResult = 0;
        int       result = stmt.getMaxFieldSize();

        assertEquals(expResult, result);
    }

    /**
     * Test of setMaxFieldSize method, of interface java.sql.Statement.
     */
    @OfMethod("setMaxFieldSize(int)")
    public void testSetMaxFieldSize() throws Exception {
        int       max = 0;
        Statement stmt = newStatement();

        stmt.setMaxFieldSize(max);
    }

    /**
     * Test of getMaxRows method, of interface java.sql.Statement.
     */
    @OfMethod("getMaxRows()")
    public void testGetMaxRows() throws Exception {
        Statement stmt      = newStatement();
        int       expResult = 0;
        int       result    = stmt.getMaxRows();

        assertEquals(expResult, result);
    }

    /**
     * Test of setMaxRows method, of interface java.sql.Statement.
     */
    @OfMethod("setMaxRows(int)")
    public void testSetMaxRows() throws Exception {
        int        max = 0;
        Statement stmt = newStatement();

        stmt.setMaxRows(max);
    }

    /**
     * Test of setEscapeProcessing method, of interface java.sql.Statement.
     */
    @OfMethod("setEscapeProcessing(boolean)")
    public void testSetEscapeProcessing() throws Exception {
        boolean   enable = true;
        Statement stmt   = newStatement();

        stmt.setEscapeProcessing(enable);

        enable = false;

        stmt.setEscapeProcessing(enable);
    }

    /**
     * Test of getQueryTimeout method, of interface java.sql.Statement.
     */
    @OfMethod("getQueryTimeout()")
    public void testGetQueryTimeout() throws Exception {
        Statement stmt      = newStatement();
        int       expResult = 0;
        int       result    = stmt.getQueryTimeout();

        assertEquals(expResult, result);
    }

    /**
     * Test of setQueryTimeout method, of interface java.sql.Statement.
     */
    @OfMethod("setQueryTimeout(int)")
    public void testSetQueryTimeout() throws Exception {
        int       seconds = 0;
        Statement stmt    = newStatement();

        stmt.setQueryTimeout(seconds);

        seconds = 1;

        stmt.setQueryTimeout(seconds);
    }

    /**
     * Test of cancel method, of interface java.sql.Statement.
     */
    @OfMethod("cancel()")
    public void testCancel() throws Exception {
        Statement stmt = newStatement();

        stmt.cancel();
    }

    /**
     * Test of getWarnings method, of interface java.sql.Statement.
     */
    @OfMethod("getWarnings()")
    public void testGetWarnings() throws Exception {
        Statement  stmt      = newStatement();
        SQLWarning expResult = null;
        SQLWarning result    = stmt.getWarnings();

        assertEquals(expResult, result);
    }

    /**
     * Test of clearWarnings method, of interface java.sql.Statement.
     */
    @OfMethod("clearWarnings()")
    public void testClearWarnings() throws Exception {
        Statement stmt = newStatement();

        stmt.clearWarnings();
    }

    /**
     * Test of setCursorName method, of interface java.sql.Statement.
     */
    @OfMethod("setCursorName(java.lang.String)")
    public void testSetCursorName() throws Exception {
        String    name = "";
        Statement stmt = newStatement();

        stmt.setCursorName(name);
    }

    /**
     * Test of execute method, of interface java.sql.Statement.
     */
    @OfMethod("execute()")
    public void testExecute() throws Exception {
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
    @OfMethod("getResultSet()")
    public void testGetResultSet() throws Exception {
        String    sql  = "select * from customer";
        Statement stmt = newStatement();

        stmt.execute(sql);

        ResultSet rs = stmt.getResultSet();

        assertNotNull(rs);
    }

    /**
     * Test of getUpdateCount method, of interface java.sql.Statement.
     */
    @OfMethod("getUpdateCount()")
    public void testGetUpdateCount() throws Exception {
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
    @OfMethod("getMoreResults()")
    public void testGetMoreResults() throws Exception {
        String    sql  = "select * from customer";
        Statement stmt = newStatement();

        boolean expResult = true;
        boolean result    = stmt.execute(sql);

        assertEquals(expResult, result);

        expResult = false;
        result = stmt.getMoreResults();
        sql = "delete from customer where 1=0";

        stmt.execute(sql);

        expResult = false;
        result    = stmt.getMoreResults();

        assertEquals(expResult, result);
    }

    /**
     * Test of setFetchDirection method, of interface java.sql.Statement.
     */
    @OfMethod("setFetchDirection(int)")
    public void testSetFetchDirection() throws Exception {
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
            printException(e);
        }

        direction = ResultSet.FETCH_UNKNOWN;

        try {
            stmt.setFetchDirection(direction);
        } catch (Exception e) {
            printException(e);
        }
    }

    /**
     * Test of getFetchDirection method, of interface java.sql.Statement.
     */
    @OfMethod("getFetchDirection()")
    public void testGetFetchDirection() throws Exception {
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
     @OfMethod("setFetchSize(int)")
    public void testSetFetchSize() throws Exception {
        int       rows = 1;
        Statement stmt = newStatement();

        stmt.setFetchSize(rows);
    }

    /**
     * Test of getFetchSize method, of interface java.sql.Statement.
     */
    @OfMethod("getFetchSize()")
    public void testGetFetchSize() throws Exception {
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
    @OfMethod("getResultSetConcurrency()")
    public void testGetResultSetConcurrency() throws Exception {
        Statement stmt      = newStatement();
        int       expResult = ResultSet.CONCUR_READ_ONLY;
        int       result    = stmt.getResultSetConcurrency();

        assertEquals(expResult, result);
    }

    /**
     * Test of getResultSetType method, of interface java.sql.Statement.
     */
    @OfMethod("getResultSetType()")
    public void testGetResultSetType() throws Exception {
        Statement stmt      = newStatement();
        int       expResult = ResultSet.TYPE_FORWARD_ONLY;
        int       result    = stmt.getResultSetType();

        assertEquals(expResult, result);
    }

    /**
     * Test of addBatch method, of interface java.sql.Statement.
     */
     @OfMethod("addBatch(java.lang.String)")
    public void testAddBatch() throws Exception {
        String    sql  = "delete from customer where id = 1";
        Statement stmt = newStatement();

        stmt.addBatch(sql);
    }

    /**
     * Test of clearBatch method, of interface java.sql.Statement.
     */
      @OfMethod("clearBatch()")
    public void testClearBatch() throws Exception {
        Statement stmt = newStatement();

        stmt.addBatch("delete from customer where id = 1");
        stmt.addBatch("delete from customer where id = 2");
        stmt.addBatch("delete from customer where id = 3");

        stmt.clearBatch();
    }

    /**
     * Test of executeBatch method, of interface java.sql.Statement.
     */
    @OfMethod("executeBatch()")
    public void testExecuteBatch() throws Exception {
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
    @OfMethod("getConnection()")
    public void testGetConnection() throws Exception {
        Statement  stmt = newStatement();
        Connection conn = stmt.getConnection();

        assertNotNull(conn);
    }

    /**
     * Test of getGeneratedKeys method, of interface java.sql.Statement.
     */
    @OfMethod("getGeneratedKeys()")
    public void testGetGeneratedKeys() throws Exception {
        // TODO
        stubTestResult();
    }

    /**
     * Test of getResultSetHoldability method, of interface java.sql.Statement.
     */
    @OfMethod("getResultSetHoldability()")
    public void testGetResultSetHoldability() throws Exception {
        Statement stmt      = newStatement();
        int       expResult = getExpectedDefaultResultSetHoldability();
        int       result    = stmt.getResultSetHoldability();

        assertEquals(expResult, result);
    }

    /**
     * Test of isClosed method, of interface java.sql.Statement.
     */
    @OfMethod("isClosed()")
    public void testIsClosed() throws Exception {
        Statement stmt = newStatement();

        assertEquals(false, stmt.isClosed());

        stmt.close();

        assertEquals(true, stmt.isClosed());
    }

    /**
     * Test of unwrap method, of interface java.sql.Statement.
     */
    @OfMethod("unwrap()")
    public void testUnwrap() throws Exception {
        Statement  stmt = newStatement();
        Class<?>   wcls = getExpectedWrappedClass();
        Object     wobj = getExpectedWrappedObject(stmt, wcls);

        assertEquals("stmt.unwrap(" + wcls + ").equals(" + wobj + ")",
                     wobj,
                     stmt.unwrap(wcls));
    }

    /**
     * Test of isWrapperFor method, of interface java.sql.Connection.
     */
    @OfMethod("isWrapperFor()")
    public void testIsWrapperFor() throws Exception {
        Statement  stmt = newStatement();
        Class<?>   wcls = getExpectedWrappedClass();

        assertEquals("stmt.isWrapperFor(" + wcls + ")",
                      true,
                      stmt.isWrapperFor(wcls));
    }

    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }

}
