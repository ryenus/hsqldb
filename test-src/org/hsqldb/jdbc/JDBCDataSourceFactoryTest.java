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

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;
import org.hsqldb.testbase.ForSubject;

/**
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
@ForSubject(JDBCDataSourceFactory.class)
public class JDBCDataSourceFactoryTest extends BaseJdbcTestCase {

    public JDBCDataSourceFactoryTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(JDBCDataSourceFactoryTest.class);

        return suite;
    }

    public Reference newReference() throws NamingException {

        String fcname = "org.hsqldb.jdbc.JDBCDataSourceFactory";
        String dcname = "org.hsqldb.jdbc.JDBCDataSource";
        Reference ref = new Reference(dcname, fcname, null);

        ref.add(new StringRefAddr("database", getUrl()));
        ref.add(new StringRefAddr("user", getUser()));
        ref.add(new StringRefAddr("password", getPassword()));
        ref.add(new StringRefAddr("loginTimeout", "30"));

        return ref;
    }

    /**
     * Test of getObjectInstance method, of class org.hsqldb.jdbc.JDBCDataSourceFactory.
     */
    public void testGetObjectInstance() throws Exception {
        Object obj = newReference();
        Name name = null;
        Context nameCtx = null;
        @SuppressWarnings("UseOfObsoleteCollectionType")
        java.util.Hashtable environment = null;
        JDBCDataSourceFactory factory = new JDBCDataSourceFactory();
        JDBCDataSource ds;

        try {
            ds = (JDBCDataSource) factory.getObjectInstance(obj,
                    name,
                    nameCtx,
                    environment);

            ds.getConnection();
        } catch (Exception ex) {
            fail(ex.toString());
        }
    }

    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }
}
