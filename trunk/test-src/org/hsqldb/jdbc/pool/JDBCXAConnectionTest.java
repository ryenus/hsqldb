package org.hsqldb.jdbc.pool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;
import javax.transaction.xa.XAResource;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 */
@ForSubject(JDBCXAConnection.class)
public class JDBCXAConnectionTest extends BaseJdbcTestCase {

    public JDBCXAConnectionTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(JDBCXAConnectionTest.class);
        return suite;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    protected JDBCXADataSource newDataSource() throws SQLException {
        JDBCXADataSource dataSource = new JDBCXADataSource();

        dataSource.setUrl(getUrl());
        dataSource.setUser(getUser());
        dataSource.setPassword(getPassword());

        return dataSource;
    }

    JDBCXAConnection newTestSubject() throws Exception {
        return (JDBCXAConnection) newDataSource().getXAConnection();
    }

    /**
     * Test of getXAResource method, of class JDBCXAConnection.
     */
    @OfMethod("getXAResource()")
    public void testGetXAResource() throws Exception {
        JDBCXAConnection testSubject = newTestSubject();
        XAResource resource = testSubject.getXAResource();

        assertNotNull(resource);
    }

    /**
     * Test of getConnection method, of class JDBCXAConnection.
     */
    @OfMethod("getConnection()")
    public void testGetConnection() throws Exception {
        JDBCXAConnection testSubject = newTestSubject();
        
        Connection connection = testSubject.getConnection();

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

        testSubject.addConnectionEventListener(new ConnectionEventListener() {
            public void connectionClosed(ConnectionEvent event) {
               connectionClosedEventOccured[0] = true;
               connectionClosedEventException[0] = event.getSQLException();
            }

            public void connectionErrorOccurred(ConnectionEvent event) {
               connectionErrorEventOccured[0] = true;
               connectionErrorEventException[0] = event.getSQLException();
            }
        });

        testSubject.addStatementEventListener(new StatementEventListener() {
            public void statementClosed(StatementEvent event) {
                statementClosedEventOccured[0] = true;
                statementClosedEventException[0] = event.getSQLException();
                statementClosedEventStatement[0] = event.getStatement();
            }

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

        connectionClosedEventOccured[0] = false;

        connection.close();

        assertEquals(false, connectionClosedEventOccured[0]);

        connectionClosedEventOccured[0] = false;

        testSubject.close();

        assertEquals(false, connectionClosedEventOccured[0]);
    }

    /**
     * Test of close method, of class JDBCXAConnection.
     */
     @OfMethod("close()")
    public void testClose() throws Exception {
        JDBCXAConnection testSubject = newTestSubject();
        Connection connection = testSubject.getConnection();

        final boolean[] connectionClosedEventOccured = new boolean[1];
        final boolean[] connectionErrorEventOccured = new boolean[1];
        final SQLException[] connectionClosedEventException = new SQLException[1];
        final SQLException[] connectionErrorEventException = new SQLException[1];


        testSubject.addConnectionEventListener(new ConnectionEventListener() {
            public void connectionClosed(ConnectionEvent event) {
               connectionClosedEventOccured[0] = true;
               connectionClosedEventException[0] = event.getSQLException();
            }

            public void connectionErrorOccurred(ConnectionEvent event) {
               connectionErrorEventOccured[0] = true;
               connectionErrorEventException[0] = event.getSQLException();
            }
        });

        
        testSubject.close();
        
        assertEquals(true, connectionClosedEventOccured[0]);

        assertEquals(true, connection.isClosed());

        connectionClosedEventOccured[0] = false;
        testSubject = newTestSubject();

        testSubject.close();

        try {
            connection = testSubject.getConnection();
        } catch (SQLException sQLException) {
        }

    }
}
