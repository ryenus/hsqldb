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

package org.hsqldb.jdbc.pool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.naming.Reference;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 */
@ForSubject(JDBCPooledDataSource.class)
public class JDBCPooledDataSourceTest extends BaseJdbcTestCase {

    public JDBCPooledDataSourceTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(JDBCPooledDataSourceTest.class);
        return suite;
    }

    public static void main(java.lang.String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    protected JDBCPooledDataSource newTestSubject() {
        JDBCPooledDataSource testSubject = new JDBCPooledDataSource();
        
        testSubject.setUser(getUser());
        testSubject.setPassword(getPassword());
        testSubject.setUrl(getUrl());

        return testSubject;
    }

    /**
     * Test of getPooledConnection method, of class JDBCPooledDataSource.
     */
    @OfMethod("getPooledConnection()")
    public void testGetPooledConnection_0args() throws Exception {
        JDBCPooledDataSource testSubject = newTestSubject();
        PooledConnection pooledConnection = testSubject.getPooledConnection();
        Connection connection = pooledConnection.getConnection();
        final boolean[] connectionClosedEventOccured = new boolean[1];
        final boolean[] connectionErrorEventOccured = new boolean[1];
        final SQLException[] connectionClosedEventException = new SQLException[1];
        final SQLException[] connectionErrorEventException = new SQLException[1];
        final boolean[] statementClosedEventOccured = new boolean[1];
        final boolean[] statementErrorEventOccured = new boolean[1];
        final SQLException[] statementClosedEventException = new SQLException[1];
        final SQLException[] statementErrorEventException = new SQLException[1];
        final PreparedStatement[] statementClosedEventStatement = new  PreparedStatement[1];
        final PreparedStatement[] statementErrorEventStatement = new  PreparedStatement[1];

        pooledConnection.addConnectionEventListener(new ConnectionEventListener() {
            @Override
            public void connectionClosed(ConnectionEvent event) {
               connectionClosedEventOccured[0] = true;
               connectionClosedEventException[0] = event.getSQLException();
            }

            @Override
            public void connectionErrorOccurred(ConnectionEvent event) {
               connectionErrorEventOccured[0] = true;
               connectionErrorEventException[0] = event.getSQLException();
            }
        });

        pooledConnection.addStatementEventListener(new StatementEventListener() {
            @Override
            public void statementClosed(StatementEvent event) {
                statementClosedEventOccured[0] = true;
                statementClosedEventException[0] = event.getSQLException();
                statementClosedEventStatement[0] = event.getStatement();
            }

            @Override
            public void statementErrorOccurred(StatementEvent event) {
                statementErrorEventOccured[0] = true;
                statementErrorEventException[0] = event.getSQLException();
                statementErrorEventStatement[0] = event.getStatement();
            }
        });

        assertEquals(false, connectionClosedEventOccured[0]);
        assertEquals(false, connection.isClosed());

        connection.close();

        assertEquals(true, connectionClosedEventOccured[0]);
        assertEquals(true, connection.isClosed());

        pooledConnection.close();


    }

    /**
     * Test of getPooledConnection method, of class JDBCPooledDataSource.
     */
    @OfMethod("getPooledConnection(java.lang.String,java.lang.String)")
    public void testGetPooledConnection_String_String() throws Exception {

        String user = getUser();
        String password = getPassword();
        JDBCPooledDataSource testSubject = newTestSubject();

        PooledConnection pooledConnection = testSubject.getPooledConnection(user, password);

        Connection connection = pooledConnection.getConnection();

        assertEquals(testSubject.getUser(), connection.getMetaData().getUserName());

        connection.close();
        pooledConnection.close();
    }

    /**
     * Test of getReference method, of class JDBCPooledDataSource.
     */
    @OfMethod("getPooledConnection(java.lang.String,java.lang.String)")
    public void testGetReference() throws Exception {

        JDBCPooledDataSource testSubject = newTestSubject();
        Reference reference = testSubject.getReference();

        assertEquals(testSubject.getDatabase(), reference.get("database").getContent());
        assertEquals(testSubject.getUser(), reference.get("user").getContent());
        assertEquals(getPassword(), reference.get("password").getContent());
        assertEquals(Integer.toString(testSubject.getLoginTimeout()), reference.get("loginTimeout").getContent());

    }
}
