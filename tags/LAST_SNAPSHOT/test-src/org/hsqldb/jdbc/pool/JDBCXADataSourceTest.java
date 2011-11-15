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
import javax.naming.Reference;
import javax.sql.XAConnection;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 */
@ForSubject(JDBCXADataSource.class)
public class JDBCXADataSourceTest extends BaseJdbcTestCase {
    
    public JDBCXADataSourceTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(JDBCXADataSourceTest.class);
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
    @OfMethod("getXAConnection()")
    public void testGetXAConnection_0args() throws Exception {
        JDBCXADataSource testSubject = newTestSubject();
        XAConnection xaConnection = testSubject.getXAConnection();

        stubTestResult();
    }

    /**
     * Test of getXAConnection method, of class JDBCXADataSource.
     */
    @OfMethod("getXAConnection(java.lang.String,java.lang.String)")
    public void testGetXAConnection_String_String() throws Exception {
        JDBCXADataSource testSubject = newTestSubject();
        XAConnection xaConnection = testSubject.getXAConnection(getUser(), getPassword());

        stubTestResult();
    }

    /**
     * Test of getReference method, of class JDBCXADataSource.
     */
    @OfMethod("getReference()")
    public void testGetReference() throws Exception {
        JDBCXADataSource testSubject = newTestSubject();
        Reference reference = testSubject.getReference();

        stubTestResult();
    }

    /**
     * Test of addResource method, of class JDBCXADataSource.
     */
    @OfMethod("addResource(javax.transaction.xa.Xid,org.hsqldb.jdbc.pool.JDBCXAResource)")
    public void testAddResource() throws Exception {
        JDBCXADataSource testSubject = newTestSubject();

        //testSubject.addResource(null, null);

        stubTestResult();
    }

    /**
     * Test of removeResource method, of class JDBCXADataSource.
     */
    @OfMethod("removeResource(javax.transaction.xa.Xid)")
    public void testRemoveResource() throws Exception {
        JDBCXADataSource testSubject = newTestSubject();

        testSubject.removeResource(null);

        stubTestResult();
    }

    /**
     * Test of removeResource method, of class JDBCXADataSource.
     */
    @OfMethod("getResource(javax.transaction.xa.Xid)")
    public void testGetResource() throws Exception {
        JDBCXADataSource testSubject = newTestSubject();

        //testSubject.getResource(null);

        stubTestResult();
    }

    /**
     * Test of removeResource method, of class JDBCXADataSource.
     */
    @OfMethod("getPreparedXids()")
    public void testGetPreparedXids() throws Exception {
        JDBCXADataSource testSubject = newTestSubject();

        //testSubject.getResource(null);

        stubTestResult();
    }

}
