/* Copyright (c) 2001-2009, The HSQL Development Group
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

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.hsqldb.jdbc.JDBCBlob;
import org.hsqldb.jdbc.JDBCClob;

import java.sql.Clob;

import javax.sql.rowset.serial.SerialBlob;

import java.sql.DriverManager;

public class TestLobs extends TestBase {

    Connection connection;
    Statement  statement;

    public TestLobs(String name) {

        super(name);
//        super(name, "jdbc:hsqldb:file:test3", false, false);
    }

    protected void setUp() {

        super.setUp();

        try {
            connection = super.newConnection();
            statement  = connection.createStatement();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public void testBlobA() {

        try {
            String ddl0 = "DROP TABLE BLOBTEST IF EXISTS";
            String ddl1 =
                "CREATE TABLE BLOBTEST(ID IDENTITY, BLOBFIELD BLOB(1000))";

            statement.execute(ddl0);
            statement.execute(ddl1);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            String dml0 = "insert into blobtest(blobfield) values(?)";
            String            dql0 = "select * from blobtest;";
            PreparedStatement ps   = connection.prepareStatement(dml0);
            byte[]            data = new byte[] {
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10
            };
            Blob              blob = new JDBCBlob(data);

            ps.setBlob(1, blob);
            ps.executeUpdate();

            data[4] = 50;
            blob    = new JDBCBlob(data);

            ps.setBlob(1, blob);
            ps.executeUpdate();
            ps.close();

            ps = connection.prepareStatement(dql0);

            ResultSet rs = ps.executeQuery();

            rs.next();

            Blob blob1 = rs.getBlob(2);

            rs.next();

            Blob   blob2 = rs.getBlob(2);
            byte[] data1 = blob1.getBytes(1, 10);
            byte[] data2 = blob2.getBytes(1, 10);

            assertTrue(data1[4] == 5 && data2[4] == 50);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void testClobA() {

        try {
            String ddl0 = "DROP TABLE CLOBTEST IF EXISTS";
            String ddl1 =
                "CREATE TABLE CLOBTEST(ID IDENTITY, CLOBFIELD CLOB(1000))";

            statement.execute(ddl0);
            statement.execute(ddl1);
        } catch (SQLException e) {}

        try {
            String dml0 = "insert into clobtest(clobfield) values(?)";
            String            dql0 = "select * from clobtest;";
            PreparedStatement ps   = connection.prepareStatement(dml0);
            String            data = "Testing blob insert and select ops";
            Clob              clob = new JDBCClob(data);

            ps.setClob(1, clob);
            ps.executeUpdate();

            data = data.replaceFirst("insert", "INSERT");
            clob = new JDBCClob(data);

            ps.setClob(1, clob);
            ps.executeUpdate();
            ps.close();

            ps = connection.prepareStatement(dql0);

            ResultSet rs = ps.executeQuery();

            rs.next();

            Clob clob1 = rs.getClob(2);

            rs.next();

            Clob clob2 = rs.getClob(2);
            int data1 = clob1.getSubString(1, data.length()).indexOf("insert");
            int data2 = clob2.getSubString(1, data.length()).indexOf("INSERT");

            assertTrue(data1 == data2 && data1 > 0);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected void tearDown() {

        try {
            statement = connection.createStatement();

            statement.execute("SHUTDOWN");
            statement.close();
            connection.close();
        } catch (Exception e) {}

        super.tearDown();
    }

    static public void main(String[] sa) throws Exception {

        ResultSet rs;
        byte[]    ba;
        byte[]    baR1 = new byte[] {
            (byte) 0xF1, (byte) 0xF2, (byte) 0xF3, (byte) 0xF4, (byte) 0xF5,
            (byte) 0xF6, (byte) 0xF7, (byte) 0xF8, (byte) 0xF9, (byte) 0xFA
        };
        byte[] baR2 = new byte[] {
            (byte) 0xE1, (byte) 0xE2, (byte) 0xE3, (byte) 0xE4, (byte) 0xE5,
            (byte) 0xE6, (byte) 0xE7, (byte) 0xE8, (byte) 0xE9, (byte) 0xEA
        };

        Class.forName("org.hsqldb.jdbc.JDBCDriver");

        Connection dbConn = DriverManager.getConnection("jdbc:hsqldb:test3",
            "SA", "");

        //"jdbc:odbc:hsql_trunk", "BLAINE", "losung1");
        dbConn.setAutoCommit(false);

        Statement st = dbConn.createStatement();

        st = dbConn.createStatement();

        st.executeUpdate("CREATE TABLE blo (id INTEGER, b blob( 100))");

        PreparedStatement ps =
            dbConn.prepareStatement("INSERT INTO blo(id, b) values(2, ?)");

        //st.executeUpdate("INSERT INTO blo (id, b) VALUES (1, x'A003')");
        ps.setBlob(1, new SerialBlob(baR1));
        ps.executeUpdate();

        rs = st.executeQuery("SELECT b FROM blo WHERE id = 2");

        if (!rs.next()) {
            throw new Exception("No row with id 2");
        }

        java.sql.Blob blob1 = rs.getBlob("b");

        System.out.println("Size of retrieved blob: " + blob1.length());

        //System.out.println("Value = (" + rs.getString("b") + ')');
        byte[] baOut = blob1.getBytes(1, (int) blob1.length());

        if (baOut.length != baR1.length) {
            throw new Exception("Expected array len " + baR1.length
                                + ", got len " + baOut.length);
        }

        for (int i = 0; i < baOut.length; i++) {
            if (baOut[i] != baR1[i]) {
                throw new Exception("Byte " + i + " is wrong");
            }
        }

        rs.close();
        /*
        rs = st.executeQuery("SELECT b FROM blo WHERE id = 1");
        if (!rs.next())
            throw new Exception("No row with id 1");
        ba = rs.getBytes("b");
        if (ba.length != baR1.length)
            throw new Exception("row1 byte length differs");
        for (int i = 0; i < ba.length; i++)
            if (ba[i] != baR1[i])
                throw new Exception("row1 byte " + i + " differs");
        rs.close();

        rs = st.executeQuery("SELECT b FROM blo WHERE id = 2");
        if (!rs.next())
            throw new Exception("No row with id 2");
        ba = rs.getBytes("b");
        if (ba.length != baR2.length)
            throw new Exception("row2 byte length differs");
        for (int i = 0; i < ba.length; i++)
            if (ba[i] != baR2[i])
                throw new Exception("row2 byte " + i + " differs");
        */
        rs.close();
        st.close();
        dbConn.commit();
        dbConn.close();
    }
}
