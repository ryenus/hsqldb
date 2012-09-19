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

import java.sql.SQLException;
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
@ForSubject(JDBCXAResource.class)
public class JDBCXAResourceTest extends BaseJdbcTestCase {

    public JDBCXAResourceTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(JDBCXAResourceTest.class);
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
    @OfMethod("withinGlobalTransaction()")
    public void testWithinGlobalTransaction() throws Exception {

        JDBCXAConnection xaConnection = newJDBCXAConnection();
        JDBCXAResource testSubject = (JDBCXAResource) xaConnection.getXAResource();

        //testSubject.withinGlobalTransaction()
        stubTestResult();
    }

    /**
     * Test of commit method, of class JDBCXAResource.
     */
    @OfMethod("commit(javax.transaction.xa.Xid,boolean)")
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
    @OfMethod("commitThis(hoolean)")
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
    @OfMethod("end(javax.transaction.xa.Xid,int)")
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
    @OfMethod("forget(javax.transaction.xa.Xid)")
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
    @OfMethod("getTransactionTimeout()")
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
     @OfMethod("isSameRM(javax.transaction.xa.XAResource)")
    public void testIsSameRM() throws Exception {
        JDBCXAConnection xaConnection = newJDBCXAConnection();
        JDBCXAResource testSubject = (JDBCXAResource) xaConnection.getXAResource();
        //XAResource otherResouce = null;
        // ...
        //boolean result = testSubject.isSameRM(otherResouce);

        stubTestResult();
    }

    /**
     * Test of prepare method, of class JDBCXAResource.
     */
    @OfMethod("prepare(javax.transaction.xa.XAResource)")
    public void testPrepare() throws Exception {
        Xid xid = JDBCXID.getUniqueXid((int) Thread.currentThread().getId());
        JDBCXAConnection xaConnection = newJDBCXAConnection();
        JDBCXAResource testSubject = (JDBCXAResource) xaConnection.getXAResource();

        // int result = testSubject.prepare(xid);

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
