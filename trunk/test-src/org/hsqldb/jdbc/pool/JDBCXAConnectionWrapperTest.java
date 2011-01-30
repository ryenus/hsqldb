package org.hsqldb.jdbc.pool;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 */
@ForSubject(JDBCXAConnectionWrapper.class)
public class JDBCXAConnectionWrapperTest extends BaseJdbcTestCase {

    public JDBCXAConnectionWrapperTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(JDBCXAConnectionWrapperTest.class);
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

    JDBCXAConnection newJDBCXAConnection() throws Exception {
        return (JDBCXAConnection) newDataSource().getXAConnection();
    }

    /**
     * Test of setAutoCommit method, of class JDBCXAConnectionWrapper.
     */
    @OfMethod("setAutoCommit()")
    public void testSetAutoCommit() throws Exception {
        JDBCXAConnection xaConnection = newJDBCXAConnection();

        Connection connection = xaConnection.getConnection();

        connection.setAutoCommit(true);

        Xid xid = JDBCXID.getUniqueXid((int) Thread.currentThread().getId());
        XAResource xaRes = xaConnection.getXAResource();
        xaRes.start(xid, XAResource.TMNOFLAGS);

        boolean autoCommit = connection.getAutoCommit();

        assertEquals(false, autoCommit);

        try {
            connection.setAutoCommit(true);
            fail("Action not allowed while in global transaction");
        } catch (SQLException sQLException) {
        }

        xaRes.end(xid, XAResource.TMSUCCESS);

        autoCommit = connection.getAutoCommit();

        assertEquals(true, autoCommit);

        connection.setAutoCommit(false);

        connection.close();
        xaConnection.close();
    }

    /**
     * Test of commit method, of class JDBCXAConnectionWrapper.
     */
    @OfMethod("commit()")
    public void testCommit() throws Exception {
        JDBCXAConnection xaConnection = newJDBCXAConnection();

        Connection connection = xaConnection.getConnection();

        Xid xid = JDBCXID.getUniqueXid((int) Thread.currentThread().getId());
        XAResource xaRes = xaConnection.getXAResource();
        xaRes.start(xid, XAResource.TMNOFLAGS);


        try {
            connection.commit();
            fail("Action not allowed while in global transaction");
        } catch (SQLException sQLException) {
        }

        xaRes.end(xid, XAResource.TMSUCCESS);

        connection.commit();

        connection.close();
        xaConnection.close();
    }

    /**
     * Test of rollback method, of class JDBCXAConnectionWrapper.
     */
    @OfMethod("rollback()")
    public void testRollback_0args() throws Exception {
        JDBCXAConnection xaConnection = newJDBCXAConnection();

        Connection connection = xaConnection.getConnection();

        Xid xid = JDBCXID.getUniqueXid((int) Thread.currentThread().getId());
        XAResource xaRes = xaConnection.getXAResource();
        xaRes.start(xid, XAResource.TMNOFLAGS);


        try {
            connection.rollback();
            fail("Action not allowed while in global transaction");
        } catch (SQLException sQLException) {
        }

        xaRes.end(xid, XAResource.TMSUCCESS);

        connection.rollback();

        connection.close();
        xaConnection.close();
    }

    /**
     * Test of rollback method, of class JDBCXAConnectionWrapper.
     */
    @OfMethod("rollback(java.sql.Savepoint)")
    public void testRollback_Savepoint() throws Exception {
        JDBCXAConnection xaConnection = newJDBCXAConnection();

        Connection connection = xaConnection.getConnection();

        connection.setAutoCommit(false);

        Savepoint sp = connection.setSavepoint("mysavepoint");

        Xid xid = JDBCXID.getUniqueXid((int) Thread.currentThread().getId());
        XAResource xaRes = xaConnection.getXAResource();
        xaRes.start(xid, XAResource.TMNOFLAGS);


        try {
            connection.rollback(sp);
            fail("Action not allowed while in global transaction");
        } catch (SQLException sQLException) {
        }

        xaRes.end(xid, XAResource.TMSUCCESS);

        connection.setSavepoint("s1");

        connection.close();
        xaConnection.close();
    }

    /**
     * Test of setSavepoint method, of class JDBCXAConnectionWrapper.
     */
    @OfMethod("setSavepoint()")
    public void testSetSavepoint_0args() throws Exception {
        JDBCXAConnection xaConnection = newJDBCXAConnection();

        Connection connection = xaConnection.getConnection();

        connection.setAutoCommit(false);

        Xid xid = JDBCXID.getUniqueXid((int) Thread.currentThread().getId());
        XAResource xaRes = xaConnection.getXAResource();
        xaRes.start(xid, XAResource.TMNOFLAGS);


        try {
            Savepoint sp = connection.setSavepoint();
            fail("Action not allowed while in global transaction");
        } catch (SQLException sQLException) {
        }

        xaRes.end(xid, XAResource.TMSUCCESS);

        Savepoint sp = connection.setSavepoint();

        connection.close();
        xaConnection.close();
    }

    /**
     * Test of setSavepoint method, of class JDBCXAConnectionWrapper.
     */
    @OfMethod("setSavepoint(java.lang.String)")
    public void testSetSavepoint_String() throws Exception {
        JDBCXAConnection xaConnection = newJDBCXAConnection();

        Connection connection = xaConnection.getConnection();

        connection.setAutoCommit(false);

        Xid xid = JDBCXID.getUniqueXid((int) Thread.currentThread().getId());
        XAResource xaRes = xaConnection.getXAResource();
        xaRes.start(xid, XAResource.TMNOFLAGS);


        try {
            Savepoint sp = connection.setSavepoint("SP1");
            fail("Action not allowed while in global transaction");
        } catch (SQLException sQLException) {
        }

        xaRes.end(xid, XAResource.TMSUCCESS);

        Savepoint sp = connection.setSavepoint("SP1");

        connection.close();
        xaConnection.close();
    }

    /**
     * Test of setTransactionIsolation method, of class JDBCXAConnectionWrapper.
     */
    @OfMethod("setTransactionIsolation()")
    public void testSetTransactionIsolation() throws Exception {
        JDBCXAConnection xaConnection = newJDBCXAConnection();

        Connection connection = xaConnection.getConnection();

        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        Xid xid = JDBCXID.getUniqueXid((int) Thread.currentThread().getId());
        XAResource xaRes = xaConnection.getXAResource();
        xaRes.start(xid, XAResource.TMNOFLAGS);


        try {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            fail("Action not allowed while in global transaction");
        } catch (SQLException sQLException) {
        }

        xaRes.end(xid, XAResource.TMSUCCESS);

        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        connection.close();
        xaConnection.close();
    }
}
