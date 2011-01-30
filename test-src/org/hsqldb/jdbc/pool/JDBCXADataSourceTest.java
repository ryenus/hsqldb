package org.hsqldb.jdbc.pool;

import java.sql.SQLException;
import javax.naming.Reference;
import javax.sql.XAConnection;
import javax.transaction.xa.Xid;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;

/**
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 */
public class JDBCXADataSourceTest extends BaseJdbcTestCase {
    
    public JDBCXADataSourceTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(JDBCXADataSourceTest.class);
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

    JDBCXADataSource newTestSubject() throws SQLException {
        JDBCXADataSource testSubject = new JDBCXADataSource();

        testSubject.setUrl(getUrl());
        testSubject.setUser(getUser());
        testSubject.setPassword(getPassword());

        return testSubject;
    }

    /**
     * Test of getXAConnection method, of class JDBCXADataSource.
     */
    public void testGetXAConnection_0args() throws Exception {
        JDBCXADataSource testSubject = newTestSubject();
        XAConnection xaConnection = testSubject.getXAConnection();

        stubTestResult();
    }

    /**
     * Test of getXAConnection method, of class JDBCXADataSource.
     */
    public void testGetXAConnection_String_String() throws Exception {
        JDBCXADataSource testSubject = newTestSubject();
        XAConnection xaConnection = testSubject.getXAConnection(getUser(), getPassword());

        stubTestResult();
    }

    /**
     * Test of getReference method, of class JDBCXADataSource.
     */
    public void testGetReference() throws Exception {
        JDBCXADataSource testSubject = newTestSubject();
        Reference reference = testSubject.getReference();

        stubTestResult();
    }

    /**
     * Test of addResource method, of class JDBCXADataSource.
     */
    public void testAddResource() throws Exception {
        JDBCXADataSource testSubject = newTestSubject();

        stubTestResult();
    }

    /**
     * Test of removeResource method, of class JDBCXADataSource.
     */
    public void testRemoveResource() throws Exception {
        JDBCXADataSource testSubject = newTestSubject();

        stubTestResult();
    }

}
