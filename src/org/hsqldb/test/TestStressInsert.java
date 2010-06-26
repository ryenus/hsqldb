/* Copyright (c) 2001-2010, The HSQL Development Group
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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

import org.hsqldb.jdbc.JDBCBlob;

/**
 * Test with small cache and very large row inserts
 */
public class TestStressInsert {

    private Connection        con;
    private PreparedStatement insertStmtA;
    private PreparedStatement insertStmtB;
    private static final int  LOB_SIZE = 1024 * 1024;
    private static final int  MAX_SIZE = 4000;
    private final Random      random   = new Random(0);
    byte[]                    data     = getRandomBytes(LOB_SIZE);

    public void init() throws Exception {

        String driver = "org.hsqldb.jdbc.JDBCDriver";
//        String url    = "jdbc:hsqldb:file:/hsql/stress/test";
        String url    = "jdbc:hsqldb:hsql://localhost/test";

        Class.forName(driver);

        con = DriverManager.getConnection(url, "sa", "");

        con.setAutoCommit(true);

        // set cache sizes
        Statement stmt = con.createStatement();

        try {
            stmt.execute("set files write delay 10000 millis");
            stmt.execute("set files log size " + 200);

            stmt.execute("set files backup increment true");

            DatabaseMetaData metaData = con.getMetaData();
            ResultSet        rs = metaData.getTables(null, null, "B", null);
            boolean          schemaExists;

            try {
                schemaExists = rs.next();
            } finally {
                rs.close();
            }

            if (!schemaExists) {
                stmt.execute(
                    "create cached table A (ID binary(16) PRIMARY KEY, DATA longvarbinary not null)");
                stmt.execute(
                    "create cached table B (ID binary(16) PRIMARY KEY, DATA BLOB(10M) not null)");
            }

            stmt.execute("checkpoint");
        } finally {
            stmt.close();
        }

        // prepare statements
        insertStmtA =
            con.prepareStatement("insert into A (DATA, ID) values (?, ?)");
        insertStmtB =
            con.prepareStatement("insert into B (DATA, ID) values (?, ?)");
    }

    public void shutdown() throws Exception {

        insertStmtA.close();
        insertStmtB.close();

        Statement stmt = con.createStatement();

        stmt.executeUpdate("SHUTDOWN");
        con.close();
    }

    public void insertA(byte[] id) throws Exception {

        try {
            insertStmtA.setBytes(1, data);
            insertStmtA.setBytes(2, id);
            insertStmtA.execute();
        } finally {
            insertStmtA.clearParameters();
            insertStmtA.clearWarnings();
        }
    }

    public void insertB(byte[] id) throws Exception {

        try {
            insertStmtB.setBlob(1, new JDBCBlob(data));
            insertStmtB.setBytes(2, id);
            insertStmtB.execute();
        } finally {
            insertStmtB.clearParameters();
            insertStmtB.clearWarnings();
        }
    }

    private void stressInsertA() throws Exception {

        long t1 = System.currentTimeMillis();
        long t2 = System.currentTimeMillis();

        System.out.println("done " + (t2 - t1));

        for (int i = 0; i < MAX_SIZE; i++) {
            insertA(getRandomBytes(16));

            if (i % 100 == 0) {
                long t3 = System.currentTimeMillis();

                System.out.println("inserted " + i + ", 100 in " + (t3 - t2));

                t2 = t3;
            }
        }

        System.out.println("total inserted " + MAX_SIZE + " in " + (t2 - t1));
        shutdown();
    }

    private void stressInsertB() throws Exception {

        long t1 = System.currentTimeMillis();
        long t2 = System.currentTimeMillis();

        System.out.println("done " + (t2 - t1));

        for (int i = 0; i < MAX_SIZE; i++) {
            insertB(getRandomBytes(16));

            if (i % 100 == 0) {
                long t3 = System.currentTimeMillis();

                System.out.println("inserted " + i + ", 100 in " + (t3 - t2));

                t2 = t3;
            }
        }

        System.out.println("total inserted " + MAX_SIZE + " in " + (t2 - t1));
        shutdown();
    }

    public static void main(String[] args) {

        try {
            System.out.print("Initializing...");

            TestStressInsert test = new TestStressInsert();

            test.init();
            test.stressInsertB();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private byte[] getRandomBytes(int length) {

        byte[] ret = new byte[length];

        random.nextBytes(ret);

        return ret;
    }
}
