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

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.rowset.serial.SerialBlob;

import org.hsqldb.jdbc.JDBCBlob;
import org.hsqldb.jdbc.JDBCClob;
import org.hsqldb.lib.StopWatch;

public class TestLobs extends TestBase {

    Connection connection;
    Statement  statement;

    public TestLobs(String name) {

       super(name);

//        super(name, "jdbc:hsqldb:file:test3", false, false);
//       super(name, "jdbc:hsqldb:mem:test3", false, false);
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
            String            data = "Testing clob insert and select ops";
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

    public void testClobB() {

        try {
            String ddl0 = "DROP TABLE CLOBTEST IF EXISTS";
            String ddl1 =
                "CREATE TABLE CLOBTEST(ID IDENTITY, V VARCHAR(10), I INT, CLOBFIELD CLOB(1000))";

            statement.execute(ddl0);
            statement.execute(ddl1);
        } catch (SQLException e) {}

        try {
            String dml0 = "insert into clobtest values(default, ?, ?, ?)";
            String            dql0 = "select * from clobtest;";
            PreparedStatement ps   = connection.prepareStatement(dml0);
            String            data = "Testing clob insert and select ops";
            Clob              clob = new JDBCClob(data);

            ps.setString(1, "test");
            ps.setInt(2, 5);
            ps.setClob(3, clob);
            ps.executeUpdate();

            data = data.replaceFirst("insert", "INSERT");
            clob = new JDBCClob(data);

            ps.setClob(3, clob);
            ps.executeUpdate();

            PreparedStatement ps2 = connection.prepareStatement(dql0);
            ResultSet         rs  = ps2.executeQuery();

            rs.next();

            Clob clob1 = rs.getClob(4);

            rs.next();

            Clob clob2 = rs.getClob(4);
            int data1 = clob1.getSubString(1, data.length()).indexOf("insert");
            int data2 = clob2.getSubString(1, data.length()).indexOf("INSERT");

            assertTrue(data1 == data2 && data1 > 0);

            //
            Clob   clob3  = new JDBCClob(data);
            Reader reader = clob3.getCharacterStream();

            ps.setCharacterStream(3, reader, (int) clob3.length());
            ps.executeUpdate();

            //
            reader = clob2.getCharacterStream();

            try {
                ps.setCharacterStream(3, reader, (int) clob3.length());
                assertTrue(false);
                ps.executeUpdate();
            } catch (SQLException e) {}

            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void testClobC() {

        try {
            String ddl0 = "DROP TABLE VARIABLE IF EXISTS";
            String ddl1 =
                "CREATE TABLE VARIABLE (stateid varchar(128), varid numeric(16,0), "
                + "scalabilitypassivated char(1) DEFAULT 'N', value clob (2G), scopeguid varchar(128),"
                + "primary key (stateid, varid, scalabilitypassivated, scopeguid))";

            statement.execute(ddl0);
            statement.execute(ddl1);
        } catch (SQLException e) {}

        try {
            String dml0 = "INSERT INTO VARIABLE VALUES (?, ?, 'N', ?, ?)";
            String dml1 =
                "UPDATE VARIABLE SET value = ? WHERE stateid = ? AND "
                + "varid = ? AND scalabilitypassivated = 'N' AND scopeguid = ?";
            PreparedStatement ps = connection.prepareStatement(dml0);

            //
            String resourceFileName  = "/org/hsqldb/resources/lob-schema.sql";
            InputStreamReader reader = null;

            try {
                InputStream fis = getClass().getResourceAsStream(resourceFileName);
                reader = new InputStreamReader(fis, "ISO-8859-1");
            } catch (Exception e) {}

            ps.setString(1, "test-id-1");
            ps.setLong(2, 23456789123456L);
            ps.setCharacterStream(3, reader, 1000);
            ps.setString(4, "test-scope-1");
            ps.executeUpdate();

            try {
                InputStream fis = getClass().getResourceAsStream(resourceFileName);
                fis    = getClass().getResourceAsStream(resourceFileName);
                reader = new InputStreamReader(fis, "ISO-8859-1");

                for (int i = 0; i < 100; i++) {
                    reader.read();
                }
            } catch (Exception e) {}

            //
            ps.setString(1, "test-id-2");
            ps.setLong(2, 23456789123457L);
            ps.setCharacterStream(3, reader, 100);
            ps.setString(4, "test-scope-2");
            ps.addBatch();
            ps.setString(1, "test-id-3");
            ps.setLong(2, 23456789123458L);
            ps.setCharacterStream(3, reader, 100);
            ps.setString(4, "test-scope-3");
            ps.addBatch();
            int[] results = ps.executeBatch();


            //
            try {
                InputStream fis = getClass().getResourceAsStream(resourceFileName);
                fis    = getClass().getResourceAsStream(resourceFileName);
                reader = new InputStreamReader(fis, "ISO-8859-1");

                for (int i = 0; i < 100; i++) {
                    reader.read();
                }
            } catch (Exception e) {}

            ps = connection.prepareStatement(dml1);

            ps.setCharacterStream(1, reader, 500);
            ps.setString(2, "test-id-1");
            ps.setLong(3, 23456789123456L);
            ps.setString(4, "test-scope-1");
            ps.executeUpdate();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void testClobD() {

        try {
            String ddl0 = "DROP TABLE VARIABLE IF EXISTS";
            String ddl1 =
                "CREATE TABLE VARIABLE (stateid varchar(128), varid numeric(16,0), "
                + "scalabilitypassivated char(1) DEFAULT 'N', value clob(2000), scopeguid varchar(128),"
                + "primary key (stateid, varid, scalabilitypassivated, scopeguid))";

            statement.execute(ddl0);
            statement.execute(ddl1);
        } catch (SQLException e) {}

        try {
            String dml0 = "INSERT INTO VARIABLE VALUES (?, ?, 'N', ?, ?)";
            String dml1 =
                "UPDATE VARIABLE SET value = ? WHERE stateid = ? AND "
                + "varid = ? AND scalabilitypassivated = 'N' AND scopeguid = ?";
            PreparedStatement ps = connection.prepareStatement(dml0);
            connection.setAutoCommit(false);
            //
            JDBCClob dataClob = new JDBCClob("the quick brown fox jumps on the lazy dog");

            Reader reader = null;

            StopWatch sw = new StopWatch();
            sw.start();

            for (int i = 0; i < 1000; i++) {

                reader = dataClob.getCharacterStream();
                ps.setString(1, "test-id-1" + i);
                ps.setLong(2, 23456789123456L + i);
                ps.setCharacterStream(3, reader, dataClob.length());
                ps.setString(4, "test-scope-1" + i);
                ps.executeUpdate();
                connection.commit();
            }

            sw.stop();
            System.out.println(sw.elapsedTimeToMessage("Time for inserts"));
            ps = connection.prepareStatement(dml1);

            sw.zero();
            sw.start();
            for (int i = 100; i < 200; i++) {

                reader = dataClob.getCharacterStream();
                ps.setCharacterStream(1, reader, dataClob.length());
                ps.setString(2, "test-id-1" + i);
                ps.setLong(3, 23456789123456L + i);
                ps.setString(4, "test-scope-1" + i);
                ps.executeUpdate();
                connection.commit();
            }

            connection.commit();
            sw.stop();
            System.out.println(sw.elapsedTimeToMessage("Time for updates"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void testClobE() {

        try {
            String ddl0 = "DROP TABLE VARIABLE IF EXISTS";
            String ddl1 =
                "CREATE TABLE VARIABLE (stateid varchar(128), varid numeric(16,0), "
                + "scalabilitypassivated char(1) DEFAULT 'N', value clob(2000), scopeguid varchar(128),"
                + "primary key (stateid, varid, scalabilitypassivated, scopeguid))";

            statement.execute(ddl0);
            statement.execute(ddl1);
        } catch (SQLException e) {}

        try {
            String dml0 = "INSERT INTO VARIABLE VALUES (?, ?, 'N', ?, ?)";
            String dml1 =
                "UPDATE VARIABLE SET varid = varid + 1 WHERE stateid = ? AND "
                + "varid = ? AND scalabilitypassivated = 'N' AND scopeguid = ?";
            PreparedStatement ps = connection.prepareStatement(dml0);
            connection.setAutoCommit(false);
            //
            JDBCClob dataClob = new JDBCClob("the quick brown fox jumps on the lazy dog");

            Reader reader = null;

            StopWatch sw = new StopWatch();
            sw.start();

            for (int i = 0; i < 100; i++) {

                reader = dataClob.getCharacterStream();
                ps.setString(1, "test-id-1" + i);
                ps.setLong(2, 23456789123456L + i);
                ps.setCharacterStream(3, reader, dataClob.length());
                ps.setString(4, "test-scope-1" + i);
                ps.executeUpdate();
                connection.commit();
            }

            sw.stop();
            System.out.println(sw.elapsedTimeToMessage("Time for inserts"));
            ps = connection.prepareStatement(dml1);

            sw.zero();
            sw.start();
            for (int i = 10; i < 20; i++) {

                ps.setString(1, "test-id-1" + i);
                ps.setLong(2, 23456789123456L + i);
                ps.setString(3, "test-scope-1" + i);
                ps.executeUpdate();
                connection.commit();
            }

            connection.commit();
            sw.stop();
            System.out.println(sw.elapsedTimeToMessage("Time for updates"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void testBlobB() {

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

        try {
            connection.setAutoCommit(false);

            Statement st = connection.createStatement();

            st.executeUpdate("CREATE TABLE blo (id INTEGER, b blob( 100))");

            PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO blo(id, b) values(2, ?)");

            //st.executeUpdate("INSERT INTO blo (id, b) VALUES (1, x'A003')");
            ps.setBlob(1, new SerialBlob(baR1));
            ps.executeUpdate();

            rs = st.executeQuery("SELECT b FROM blo WHERE id = 2");

            if (!rs.next()) {
                assertTrue("No row with id 2", false);
            }

            java.sql.Blob blob1 = rs.getBlob("b");

            System.out.println("Size of retrieved blob: " + blob1.length());

            //System.out.println("Value = (" + rs.getString("b") + ')');
            byte[] baOut = blob1.getBytes(1, (int) blob1.length());

            if (baOut.length != baR1.length) {
                assertTrue("Expected array len " + baR1.length + ", got len "
                           + baOut.length, false);
            }

            for (int i = 0; i < baOut.length; i++) {
                if (baOut[i] != baR1[i]) {
                    assertTrue("Expected array len " + baR1.length
                               + ", got len " + baOut.length, false);
                }
            }

            rs.close();

            rs = st.executeQuery("SELECT b FROM blo WHERE id = 2");

            if (!rs.next()) {
                assertTrue("No row with id 2", false);
            }

//            ba = rs.getBytes("b"); doesn't convert but throws ClassCast

            blob1 = rs.getBlob("b");
            ba = blob1.getBytes(1,baR2.length);
            if (ba.length != baR2.length) {
                assertTrue("row2 byte length differs", false);
            }

            for (int i = 0; i < ba.length; i++) {
                if (ba[i] != baR1[i]) {
                    assertTrue("row2 byte " + i + " differs", false);
                }
            }

            rs.close();
            st.close();
            connection.rollback();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    protected void tearDown() {

        try {
            statement = connection.createStatement();

//            statement.execute("SHUTDOWN IMMEDIATELY");
            statement.close();
            connection.close();
        } catch (Exception e) {}

        super.tearDown();
    }
}
