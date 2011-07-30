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

import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.hsqldb.jdbc.testbase.BaseClobTestCase;
import org.hsqldb.testbase.ForSubject;

/**
 * Test of class org.hsqldb.jdbc.jdbcClobClient.
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 */
@ForSubject(JDBCClobClient.class)
public class JDBCClobClientTest extends BaseClobTestCase {

    public JDBCClobClientTest(String testName) {
        super(testName);
    }

    protected Clob handleCreateClob() throws Exception {
        Connection conn = newConnection();

        Statement  stmt = connectionFactory().createStatement(conn);

        stmt.execute("drop table clob_client_test if exists");
        stmt.execute("create table clob_client_test(id int, clob_value clob)");
        stmt.execute("insert into clob_client_test(id ,clob_value) values(1, null)");

        Clob blob = conn.createClob();
        PreparedStatement pstmt = connectionFactory().prepareStatement("update clob_client_test set clob_value = ?", conn);

        pstmt.setClob(1, blob);

        pstmt.execute();

        ResultSet rs = stmt.executeQuery("select clob_value from clob_client_test where id = 1 for update");

        rs.next();

        return rs.getClob(1);
    }

    public static Test suite() {
        return new TestSuite(JDBCClobClientTest.class);
    }

    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }
}
