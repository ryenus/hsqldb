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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;
import org.hsqldb.testbase.ForSubject;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test of JDBCPool
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 */

@ForSubject(JDBCPool.class)
public class JDBCPoolTest  extends BaseJdbcTestCase {

    public JDBCPoolTest(String testName) {
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
        TestSuite suite = new TestSuite(JDBCPoolTest.class);

        return suite;
    }

    protected DataSource newDataSource(int size) throws SQLException {
        JDBCPool dataSource = new JDBCPool(size);

        dataSource.setUrl(getUrl());
        dataSource.setUser(getUser());
        dataSource.setPassword(getPassword());

        return dataSource;
    }

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

    public JDBCConnection getTestedConnection(DataSource ds) throws Exception {
        JDBCConnection conn = (JDBCConnection) ds.getConnection();

        Statement st = conn.createStatement();

        ResultSet rs = st.executeQuery("call current_schema");

        rs.next();
        String s = rs.getString(1);
        assertEquals("PUBLIC", s);

        return conn;
    }




}
