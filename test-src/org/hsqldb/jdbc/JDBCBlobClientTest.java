/* Copyright (c) 2001-2022, The HSQL Development Group
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


import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.testbase.BaseBlobTestCase;
import org.hsqldb.testbase.ForSubject;

/**
 * Test of org.hsqldb.jdbc.JDBCBlobClient
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
@ForSubject(JDBCBlobClient.class)
public class JDBCBlobClientTest extends BaseBlobTestCase {

    public JDBCBlobClientTest(String testName) {
        super(testName);
    }


    @Override
    protected Blob handleCreateBlob() throws Exception {
        Connection conn = newConnection();

        Statement stmt = connectionFactory().createStatement(conn);

        stmt.execute("drop table blob_client_test if exists");
        stmt.execute("create table blob_client_test(id int, blob_value blob)");
        stmt.execute("insert into blob_client_test(id ,blob_value) values(1, null)");

        Blob blob = connectionFactory().createBlob(conn);
        PreparedStatement pstmt = connectionFactory().prepareStatement(
                "update blob_client_test set blob_value = ? where id = 1", conn);

        pstmt.setBlob(1, blob);
        pstmt.execute();
        
        ResultSet rs = connectionFactory().executeQuery(
                "select blob_value from blob_client_test where id = 1", stmt);

        rs.next();

        return rs.getBlob(1);
    }

    public static Test suite() {
        return new TestSuite(JDBCBlobTest.class);
    }

    public static void main(java.lang.String[] argList) {
        junit.textui.TestRunner.run(suite());
    }
}
