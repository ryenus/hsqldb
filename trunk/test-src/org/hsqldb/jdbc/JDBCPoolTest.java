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

package org.hsqldb.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.DataSource;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.pool.JDBCPooledDataSource;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;
import org.hsqldb.jdbc.testbase.SqlState;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 * Test of JDBCPool
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 */
@ForSubject(JDBCPool.class)
public class JDBCPoolTest extends BaseJdbcTestCase {

    public static Test suite() {
        TestSuite suite = new TestSuite(JDBCPoolTest.class);

        return suite;
    }

    public JDBCPoolTest(String testName) {
        super(testName);
    }

    public JDBCConnection getTestedConnection(DataSource ds) throws Exception {
        JDBCConnection conn = (JDBCConnection) ds.getConnection();

        Statement st = conn.createStatement();

        ResultSet rs = st.executeQuery("call current_schema");

        rs.next();
        String s = rs.getString(1);
        assertEquals("PUBLIC", s);

        return conn;
    }

    /**
     * Test of close method, of class JDBCPool.
     */
    public void testClose() throws Exception {
        System.out.println("close");
        int wait = 0;
        JDBCPool instance = new JDBCPool();
        instance.close(wait);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of connectionClosed method, of class JDBCPool.
     */
    public void testConnectionClosed() {
        System.out.println("connectionClosed");
        ConnectionEvent event = null;
        JDBCPool instance = new JDBCPool();
        // event cannot be null
        // instance.connectionClosed(event);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of connectionErrorOccurred method, of class JDBCPool.
     */
    public void testConnectionErrorOccurred() {
        System.out.println("connectionErrorOccurred");
        ConnectionEvent event = null;
        JDBCPool instance = new JDBCPool();
        // event cannot be null
        // instance.connectionErrorOccurred(event);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of getConnection method, of class JDBCPool.
     *
     * @throws java.lang.Exception
     */
    public void testGetConnection_0args() throws Exception {
        JDBCPool pool = newTestSubject();
        try {
            Connection connection = pool.getConnection();
            assertNotNull(connection);
            assertEquals(pool.getUrl(), connection.getMetaData().getURL());
            assertEquals(pool.getUser(), connection.getMetaData().getUserName());
        } finally {
            pool.close(0);
        }
    }

    /**
     * Test of getConnection method, of class JDBCPool.
     */
    public void testGetConnection_String_String() throws Exception {
        JDBCPool pool = newTestSubject();
        try {
            Connection connection = pool.getConnection(this.getUser(), this.getPassword());
            assertNotNull(connection);
            assertEquals(pool.getUrl(), connection.getMetaData().getURL());
            assertEquals(this.getUser(), connection.getMetaData().getUserName());
        } finally {
            pool.close(0);
        }
    }

    /**
     * Test of getDataSourceName method, of class JDBCPool.
     */
    public void testGetDataSourceName() {
        System.out.println("getDataSourceName");
        JDBCPool instance = new JDBCPool();
        String expResult = "";
        String result = instance.getDataSourceName();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of getDatabase method, of class JDBCPool.
     */
    public void testGetDatabase() {
        System.out.println("getDatabase");
        JDBCPool instance = new JDBCPool();
        String expResult = "";
        String result = instance.getDatabase();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of getDatabaseName method, of class JDBCPool.
     */
    public void testGetDatabaseName() {
        System.out.println("getDatabaseName");
        JDBCPool instance = new JDBCPool();
        String expResult = "";
        String result = instance.getDatabaseName();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of getDescription method, of class JDBCPool.
     */
    public void testGetDescription() {
        System.out.println("getDescription");
        JDBCPool instance = new JDBCPool();
        String expResult = "";
        String result = instance.getDescription();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of getLogWriter method, of class JDBCPool.
     */
    public void testGetLogWriter() throws Exception {
        System.out.println("getLogWriter");
        JDBCPool instance = new JDBCPool();
        PrintWriter expResult = null;
        PrintWriter result = instance.getLogWriter();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of getLoginTimeout method, of class JDBCPool.
     */
    public void testGetLoginTimeout() throws Exception {
        System.out.println("getLoginTimeout");
        JDBCPool instance = new JDBCPool();
        int expResult = 0;
        int result = instance.getLoginTimeout();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of getParentLogger method, of class JDBCPool.
     */
    public void testGetParentLogger() throws Exception {
        System.out.println("getParentLogger");
        JDBCPool instance = new JDBCPool();
        Logger expResult = null;
        // Logger result = instance.getParentLogger();
        // assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of getReference method, of class JDBCPool.
     */
    public void testGetReference() throws Exception {
        System.out.println("getReference");
        JDBCPool instance = new JDBCPool();
        Reference expResult = null;
        Reference result = instance.getReference();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of getURL method, of class JDBCPool.
     */
    public void testGetURL() {
        System.out.println("getURL");
        JDBCPool instance = new JDBCPool();
        String expResult = "";
        String result = instance.getURL();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of getUrl method, of class JDBCPool.
     */
    public void testGetUrl() {
        System.out.println("getUrl");
        JDBCPool instance = new JDBCPool();
        String expResult = "";
        String result = instance.getUrl();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of getUser method, of class JDBCPool.
     */
    public void testGetUser() {
        System.out.println("getUser");
        JDBCPool instance = new JDBCPool();
        String expResult = "";
        String result = instance.getUser();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of isWrapperFor method, of class JDBCPool.
     */
    public void testIsWrapperFor() throws Exception {
        System.out.println("isWrapperFor");
        Class iface = null;
        JDBCPool instance = new JDBCPool();
        boolean expResult = false;
        boolean result = instance.isWrapperFor(iface);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of setDatabase method, of class JDBCPool.
     */
    public void testSetDatabase() {
        System.out.println("setDatabase");
        String database = "";
        JDBCPool instance = new JDBCPool();
        instance.setDatabase(database);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of setDatabaseName method, of class JDBCPool.
     */
    public void testSetDatabaseName() {
        System.out.println("setDatabaseName");
        String databaseName = "";
        JDBCPool instance = new JDBCPool();
        instance.setDatabaseName(databaseName);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of setLogWriter method, of class JDBCPool.
     */
    public void testSetLogWriter() throws Exception {
        System.out.println("setLogWriter");
        PrintWriter out = null;
        JDBCPool instance = new JDBCPool();
        instance.setLogWriter(out);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of setLoginTimeout method, of class JDBCPool.
     */
    public void testSetLoginTimeout() throws Exception {
        System.out.println("setLoginTimeout");
        int seconds = 0;
        JDBCPool instance = new JDBCPool();
        instance.setLoginTimeout(seconds);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of setPassword method, of class JDBCPool.
     */
    public void testSetPassword() {
        System.out.println("setPassword");
        String password = "";
        JDBCPool instance = new JDBCPool();
        instance.setPassword(password);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of setProperties method, of class JDBCPool.
     */
    public void testSetProperties() {
        System.out.println("setProperties");
        Properties props = null;
        JDBCPool instance = new JDBCPool();
        instance.setProperties(props);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of setURL method, of class JDBCPool.
     */
    public void testSetURL() {
        System.out.println("setURL");
        String url = "";
        JDBCPool instance = new JDBCPool();
        instance.setURL(url);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of setUrl method, of class JDBCPool.
     */
    public void testSetUrl() {
        System.out.println("setUrl");
        String url = "";
        JDBCPool instance = new JDBCPool();
        instance.setUrl(url);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of setUser method, of class JDBCPool.
     */
    public void testSetUser() {
        System.out.println("setUser");
        String user = "";
        JDBCPool instance = new JDBCPool();
        instance.setUser(user);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    @OfMethod("getConnection")
    public void testSingleConnectionPool() throws Exception {
        DataSource ds = newDataSource(1);
        JDBCConnection conn1 = getTestedConnection(ds);
        conn1.close();

        assertTrue(conn1.isClosed());

        JDBCConnection conn2 = getTestedConnection(ds);

        assertFalse(conn2.isClosed());
        conn2.close();
        assertTrue(conn2.isClosed());
        assertFalse(conn1 == conn2);
    }

    /**
     * Test of statementClosed method, of class JDBCPool.
     */
    public void testStatementClosed() {
        System.out.println("statementClosed");
        StatementEvent event = null;
        JDBCPool instance = new JDBCPool();
        instance.statementClosed(event);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of statementErrorOccurred method, of class JDBCPool.
     */
    public void testStatementErrorOccurred() {
        System.out.println("statementErrorOccurred");
        StatementEvent event = null;
        JDBCPool instance = new JDBCPool();
        instance.statementErrorOccurred(event);
        // TODO review the generated test code and remove the default call to fail.
        stubTestResult("The test case is a prototype.");
    }

    /**
     * Test of unwrap method, of class JDBCPool.
     */
    public void testUnwrap() throws Exception {
        final JDBCPool pool = newTestSubject();
        
        try {
            DataSource ds = pool.unwrap(DataSource.class);
        } catch (SQLException ex) {
            fail(ex.toString());
        }
        
        try {
            Referenceable r = pool.unwrap(Referenceable.class);
        } catch (SQLException ex) {
            fail(ex.toString());
        }
        
        try {
            ConnectionEventListener cel = pool.unwrap(ConnectionEventListener.class);
        } catch (SQLException ex) {
            fail(ex.toString());
        }
        
        try {
            StatementEventListener sel = pool.unwrap(StatementEventListener.class);
        } catch (SQLException ex) {
            fail(ex.toString());
        }
        
        try {
            JDBCPooledDataSource jpds = pool.unwrap(JDBCPooledDataSource.class);
            
            fail("got: "+jpds);
        } catch (SQLException ex) {
            SqlState sqlstate = SqlState.forException(ex);
            
            assertEquals(SqlState.Constant.SqlStateCategory.Exception, sqlstate.Category);
            assertEquals(SqlState.Constant.SqlStateClass.ODBC2.GeneralError, sqlstate.Class);
            assertTrue(sqlstate.ClassIsStandardDefined);
        }

    }

    protected DataSource newDataSource(int size) throws SQLException {
        JDBCPool dataSource = new JDBCPool(size);

        dataSource.setUrl(getUrl());
        dataSource.setUser(getUser());
        dataSource.setPassword(getPassword());

        return dataSource;
    }

    protected JDBCPool newTestSubject() {
        final JDBCPool result = new JDBCPool();

        result.setDatabase(this.getUrl());
        result.setUrl(this.getUrl());
        result.setUser(this.getUser());
        result.setPassword(this.getPassword());

        return result;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

}
