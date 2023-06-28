/* Copyright (c) 2001-2023, The HSQL Development Group
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


package org.hsqldb.test;

import org.hsqldb.jdbc.JDBCDriver;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Test for invalid blob issue.
 * <p>
 * One connection performs BLOB updates while the other connection executes
 * regular CHECKPOINTs.
 *
 * @author Rasmus Faber (rasmus_faber@users dot sourceforge.net)
 */
public class TestInvalidBlobIssue extends TestBase {

    public TestInvalidBlobIssue(String testName) {
        super(testName);
    }

    protected Blob handleCreateBlob() throws Exception {
        Connection conn = newConnection();

        Statement stmt = conn.createStatement();

        stmt.execute("drop table blob_client_test if exists");
        stmt.execute("create table blob_client_test(id int, blob_value blob)");
        stmt.execute("insert into blob_client_test(id ,blob_value) values(1, null)");

        Blob blob = conn.createBlob();
        PreparedStatement pstmt = conn.prepareStatement(
                "update blob_client_test set blob_value = ? where id = 1");

        pstmt.setBlob(1, blob);
        pstmt.execute();

        ResultSet rs = stmt.executeQuery(
                "select blob_value from blob_client_test where id = 1");

        rs.next();

        return rs.getBlob(1);
    }

    public void insertAndQuery(Connection conn, int id) throws Exception {
        PreparedStatement insertStmt = conn.prepareStatement(
                "insert into blob_client_test(id ,blob_value) values(?, ?)");
        insertStmt.setInt(1, id);
        Blob blob = conn.createBlob();
        blob.setBytes(1, new byte[1000]);
        insertStmt.setBlob(2, blob);
        insertStmt.executeUpdate();
        blob.free();
        insertStmt.close();
        PreparedStatement selectStmt = conn.prepareStatement(
                "select blob_value from blob_client_test where id = ?");
        selectStmt.setInt(1, id);
        ResultSet rs = selectStmt.executeQuery();
        rs.next();
        Blob blob2 = rs.getBlob(1);
        assertEquals(1000, blob2.length());
        blob2.free();
        rs.close();
        selectStmt.close();
        PreparedStatement deleteStmt = conn.prepareStatement(
                "delete from blob_client_test where id = ?");
        deleteStmt.setInt(1, id);
        deleteStmt.executeUpdate();
        deleteStmt.close();
    }

    public void insertAndDelete(Connection conn, int id) throws Exception {
        PreparedStatement insertStmt = conn.prepareStatement(
                "insert into blob_client_test(id ,blob_value) values(?, ?)");
        insertStmt.setInt(1, id);
        Blob blob = conn.createBlob();
        blob.setBytes(1, new byte[1000]);
        insertStmt.setBlob(2, blob);
        insertStmt.executeUpdate();
        blob.free();
        insertStmt.close();
        conn.commit();
        PreparedStatement deleteStmt = conn.prepareStatement(
                "delete from blob_client_test where id = ?");
        deleteStmt.setInt(1, id);
        deleteStmt.executeUpdate();
        deleteStmt.close();
        conn.commit();
    }

    @org.junit.Test
    public void testWithCheckpoint() throws Exception {
        new JDBCDriver();
        Connection connection1 = newConnection();
        Connection connection2 = newConnection();

        Statement stmt = connection1.createStatement();

        stmt.execute("drop table blob_client_test if exists");
        stmt.execute("create table blob_client_test(id int, blob_value blob)");
        Thread thread1 = new Thread() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < 10000; i++) {
                        insertAndQuery(connection1, i);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        };
        Thread thread2 = new Thread() {
            @Override
            public void run() {
                try {
                    while (thread1.isAlive()) {
                        PreparedStatement checkpointStatement = connection2.prepareStatement("CHECKPOINT");
                        checkpointStatement.execute();
                        checkpointStatement.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        };
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        connection1.close();
        connection2.close();
    }

    @org.junit.Test
    public void testWithCheckpoint2() throws Exception {
        new JDBCDriver();
        Connection connection1 = newConnection();
        Connection connection2 = newConnection();
        Connection connection3 = newConnection();
        connection1.setAutoCommit(false);
        connection2.setAutoCommit(false);
        connection3.setAutoCommit(false);

        Statement stmt = connection1.createStatement();

        stmt.execute("drop table blob_client_test if exists");
        stmt.execute("create table blob_client_test(id int, blob_value blob)");
        Thread thread1 = new Thread() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < 10000; i++) {
                        insertAndDelete(connection1, 1);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        };
        Thread thread2 = new Thread() {
            @Override
            public void run() {
                try {
                    while (thread1.isAlive()) {
                        PreparedStatement checkpointStatement = connection2.prepareStatement("CHECKPOINT");
                        checkpointStatement.execute();
                        checkpointStatement.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        };
        Thread thread3 = new Thread() {
            @Override
            public void run() {
                try {
                    while (thread1.isAlive()) {
                        query(connection3, 1);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        };
        thread1.start();
        thread2.start();
        thread3.start();
        thread1.join();
        thread2.join();
        thread3.join();

        connection1.close();
        connection2.close();
        connection3.close();
    }

    @org.junit.Test
    public void testInvalidLob2() throws Exception {
        new JDBCDriver();
        Connection connection1 = newConnection();
        Connection connection2 = newConnection();
        Statement  stmt        = connection1.createStatement();

        stmt.execute("drop table blob_client_test if exists");
        stmt.execute("create table blob_client_test(id int, blob_value blob)");

        PreparedStatement insertStmt = connection1.prepareStatement(
                "insert into blob_client_test(id ,blob_value) values(?, ?)");
        insertStmt.setInt(1, 1);
        Blob blob = connection1.createBlob();
        blob.setBytes(1, new byte[1000]);
        insertStmt.setBlob(2, blob);
        insertStmt.executeUpdate();
        blob.free();
        insertStmt.close();

        PreparedStatement selectStmt = connection2.prepareStatement(
                "select blob_value from blob_client_test where id = ?");
        selectStmt.setInt(1, 1);
        ResultSet rs = selectStmt.executeQuery();
        rs.next();
        Blob blob2 = rs.getBlob(1);

        PreparedStatement deleteStmt = connection1.prepareStatement(
                "delete from blob_client_test where id = ?");
        deleteStmt.setInt(1, 1);
        deleteStmt.executeUpdate();
        deleteStmt.close();
        stmt.execute("CHECKPOINT");

        // the blob row has been deleted and the blob is no longer valid
        // assertEquals(1000, blob2.length());
        blob2.free();
        rs.close();
        selectStmt.close();

        stmt.close();
        connection1.close();
        connection2.close();
    }


    public void insert(Connection conn, int id) throws Exception {
        PreparedStatement insertStmt = conn.prepareStatement(
                "insert into blob_client_test(id ,blob_value) values(?, ?)");
        insertStmt.setInt(1, id);
        Blob blob = conn.createBlob();
        blob.setBytes(1, new byte[1000]);
        insertStmt.setBlob(2, blob);
        insertStmt.executeUpdate();
        blob.free();
        insertStmt.close();
        conn.commit();
    }

    public void delete(Connection conn, int id) throws Exception {
        PreparedStatement deleteStmt = conn.prepareStatement(
                "delete from blob_client_test where id = ?");
        deleteStmt.setInt(1, id);
        deleteStmt.executeUpdate();
        deleteStmt.close();
        conn.commit();
    }

    public void query(Connection conn, int id) throws Exception {
        PreparedStatement selectStmt = conn.prepareStatement(
                "select blob_value from blob_client_test where id = ?");
        selectStmt.setInt(1, id);
        ResultSet rs = selectStmt.executeQuery();
        if(rs.next()) {
            Blob blob2 = rs.getBlob(1);

            try {
                long length = blob2.length();
                assertEquals(1000, blob2.length());
            } catch (Exception e) {
                System.out.println("lob not valid id: " + id );
            }
            blob2.free();
        }
        rs.close();
        selectStmt.close();
        conn.commit();
    }

    private void checkpoint(Connection conn) throws Exception {
        PreparedStatement checkpointStatement = conn.prepareStatement("CHECKPOINT");
        checkpointStatement.execute();
        checkpointStatement.close();
        conn.commit();
    }

    @org.junit.Test
    public void testWithCheckpoint_autocommitoff() throws Exception {
        new JDBCDriver();
        Connection connection1 = newConnection();
        connection1.setAutoCommit(false);
        Connection connection2 = newConnection();
        connection2.setAutoCommit(false);
        Connection connection3 = newConnection();
        connection3.setAutoCommit(false);
        Connection connection4 = newConnection();
        connection4.setAutoCommit(false);

        Statement stmt = connection1.createStatement();

        stmt.execute("drop table blob_client_test if exists");
        stmt.execute("create table blob_client_test(id int, blob_value blob)");

        List<Thread> threads = new ArrayList<>();

        for(int i=0;i<50000;i++){
            int id = i;
            insert(connection1, i);
            Thread thread1 = new Thread() {
                @Override
                public void run() {
                    try {
                        delete(connection2, id);
                        checkpoint(connection3);
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }
                }
            };
            Thread thread2 = new Thread() {
                @Override
                public void run() {
                    try {
                        query(connection4, id);
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }
                }
            };
            threads.add(thread1);
            threads.add(thread2);
            thread1.start();
            thread2.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        connection1.close();
        connection2.close();
        connection3.close();
        connection4.close();
    }
}
