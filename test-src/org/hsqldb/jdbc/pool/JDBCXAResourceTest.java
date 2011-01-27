package org.hsqldb.jdbc.pool;

import java.sql.SQLException;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;

/**
 *
 * @author boucherb@users
 */
public class JDBCXAResourceTest extends BaseJdbcTestCase {

    public JDBCXAResourceTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(JDBCXAResourceTest.class);
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

    JDBCXADataSource newJDBCXADataSource() throws SQLException {
        JDBCXADataSource dataSource = new JDBCXADataSource();

        dataSource.setUrl(getUrl());
        dataSource.setUser(getUser());
        dataSource.setPassword(getPassword());

        return dataSource;
    }

    JDBCXAConnection newJDBCXAConnection() throws SQLException {
        return (JDBCXAConnection) newJDBCXADataSource().getXAConnection();
    }

    /**
     * Test of withinGlobalTransaction method, of class JDBCXAResource.
     */
    public void testWithinGlobalTransaction() throws Exception {

        JDBCXAConnection xaConnection = newJDBCXAConnection();
        JDBCXAResource testSubject = (JDBCXAResource) xaConnection.getXAResource();

        stubTestResult();
    }

    /**
     * Test of commit method, of class JDBCXAResource.
     */
    public void testCommit() throws Exception {
        Xid xid = JDBCXID.getUniqueXid((int) Thread.currentThread().getId());
        boolean onePhase = false;
        JDBCXAConnection xaConnection = newJDBCXAConnection();
        JDBCXAResource testSubject = (JDBCXAResource) xaConnection.getXAResource();


        testSubject.commit(xid, onePhase);

        stubTestResult();
    }

    /**
     * Test of commitThis method, of class JDBCXAResource.
     */
    public void testCommitThis() throws Exception {
        JDBCXAConnection xaConnection = newJDBCXAConnection();
        JDBCXAResource testSubject = (JDBCXAResource) xaConnection.getXAResource();
        boolean onePhase = false;

        testSubject.commitThis(onePhase);

        stubTestResult();
    }

    /**
     * Test of end method, of class JDBCXAResource.
     */
    public void testEnd() throws Exception {

        Xid xid = JDBCXID.getUniqueXid((int) Thread.currentThread().getId());
        int flags = XAResource.TMNOFLAGS;
        JDBCXAConnection xaConnection = newJDBCXAConnection();
        JDBCXAResource testSubject = (JDBCXAResource) xaConnection.getXAResource();

        //testSubject.end(xid, flags);

        stubTestResult();
    }

    /**
     * Test of forget method, of class JDBCXAResource.
     */
    public void testForget() throws Exception {
        Xid xid = JDBCXID.getUniqueXid((int) Thread.currentThread().getId());

        JDBCXAConnection xaConnection = newJDBCXAConnection();
        JDBCXAResource testSubject = (JDBCXAResource) xaConnection.getXAResource();

        //testSubject.forget(xid);

        stubTestResult();
    }

    /**
     * Test of getTransactionTimeout method, of class JDBCXAResource.
     */
    public void testGetTransactionTimeout() throws Exception {
        JDBCXAConnection xaConnection = newJDBCXAConnection();
        JDBCXAResource testSubject = (JDBCXAResource) xaConnection.getXAResource();
        int expResult = 0;
        int result = testSubject.getTransactionTimeout();

        assertEquals(expResult, result);
    }

    /**
     * Test of isSameRM method, of class JDBCXAResource.
     */
    public void testIsSameRM() throws Exception {
        JDBCXAConnection xaConnection = newJDBCXAConnection();
        JDBCXAResource testSubject = (JDBCXAResource) xaConnection.getXAResource();


        //boolean result = testSubject.isSameRM(?);

        stubTestResult();
    }

    /**
     * Test of prepare method, of class JDBCXAResource.
     */
    public void testPrepare() throws Exception {
        Xid xid = JDBCXID.getUniqueXid((int) Thread.currentThread().getId());
        JDBCXAConnection xaConnection = newJDBCXAConnection();
        JDBCXAResource testSubject = (JDBCXAResource) xaConnection.getXAResource();

        // int result = instance.prepare(xid);

        stubTestResult();

    }

    /**
     * Test of prepareThis method, of class JDBCXAResource.
     */
    public void testPrepareThis() throws Exception {
        JDBCXAConnection xaConnection = newJDBCXAConnection();
        JDBCXAResource testSubject = (JDBCXAResource) xaConnection.getXAResource();
        int result = testSubject.prepareThis();

        stubTestResult();
    }

    /**
     * Test of recover method, of class JDBCXAResource.
     */
    public void testRecover() throws Exception {
        JDBCXAConnection xaConnection = newJDBCXAConnection();
        JDBCXAResource testSubject = (JDBCXAResource) xaConnection.getXAResource();

        int flag = 0;

        Xid[] result = testSubject.recover(flag);

        stubTestResult();
    }

    /**
     * Test of rollback method, of class JDBCXAResource.
     */
    public void testRollback() throws Exception {
        Xid xid = JDBCXID.getUniqueXid((int) Thread.currentThread().getId());
        JDBCXAConnection xaConnection = newJDBCXAConnection();
        JDBCXAResource testSubject = (JDBCXAResource) xaConnection.getXAResource();

        //testSubject.rollback(xid);

        stubTestResult();
    }

    /**
     * Test of rollbackThis method, of class JDBCXAResource.
     */
    public void testRollbackThis() throws Exception {
        JDBCXAConnection xaConnection = newJDBCXAConnection();
        JDBCXAResource testSubject = (JDBCXAResource) xaConnection.getXAResource();

        //testSubject.rollbackThis();

        stubTestResult();
    }

    /**
     * Test of setTransactionTimeout method, of class JDBCXAResource.
     */
    public void testSetTransactionTimeout() throws Exception {
        JDBCXAConnection xaConnection = newJDBCXAConnection();
        JDBCXAResource testSubject = (JDBCXAResource) xaConnection.getXAResource();

        boolean result = testSubject.setTransactionTimeout(0);

        stubTestResult();
    }

    /**
     * Test of start method, of class JDBCXAResource.
     */
    public void testStart() throws Exception {
        Xid xid = JDBCXID.getUniqueXid((int) Thread.currentThread().getId());
        JDBCXAConnection xaConnection = newJDBCXAConnection();
        JDBCXAResource testSubject = (JDBCXAResource) xaConnection.getXAResource();
        int flags = 0;

        testSubject.start(xid, XAResource.TMNOFLAGS);

        stubTestResult();
    }
}
