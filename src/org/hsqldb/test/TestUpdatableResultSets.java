/* Copyright (c) 2001-2024, The HSQL Development Group
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

import org.hsqldb.jdbc.JDBCBlob;
import org.hsqldb.jdbc.JDBCClob;

import java.io.Reader;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestUpdatableResultSets extends TestBase {

    Connection connection;
    Statement  statement;
    int        fetchSize = 2;

    public TestUpdatableResultSets(String name) {
        super(name);
    }

    protected void setUp() throws Exception {

        super.setUp();

        connection = super.newConnection();
        statement = connection.createStatement(
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_UPDATABLE);
    }

    public void testUpdatable() {

        try {
            statement.execute("SET DATABASE EVENT LOG SQL LEVEL 3");
            statement.execute("drop table t1 if exists cascade");
            statement.execute(
                "create table t1 (i int primary key, v varchar(10), t varbinary(3), b blob(16), c clob(16))");

            String            insert = "insert into t1 values(?,?,?,?,?)";
            String select = "select i, v, t, b, c from t1 where i > ?";
            PreparedStatement ps     = connection.prepareStatement(insert);

            for (int i = 0; i < 10; i++) {
                ps.setInt(1, i);
                ps.setString(2, String.valueOf(i) + " s");
                ps.setBytes(3, new byte[]{ (byte) i, ' ', (byte) i });
                ps.setBytes(
                    4,
                    new byte[] {
                    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
                });
                ps.setString(5, "123");
                ps.execute();
            }

            ps.close();
            connection.setAutoCommit(false);

            ps = connection.prepareStatement(
                select,
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

            ps.setInt(1, -1);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String s = rs.getString(2);

                rs.updateString(2, s + s);
                rs.updateRow();
            }

            rs.close();

            rs = ps.executeQuery();

            while (rs.next()) {
                String s = rs.getString(2);

                System.out.println(s);
            }

            connection.rollback();

            rs = ps.executeQuery();

            while (rs.next()) {
                String s = rs.getString(2);

                System.out.println(s);
            }

            connection.commit();

            rs = ps.executeQuery();

            Clob c = new JDBCClob("123456789abcdef");

            if (rs.next()) {
                rs.updateClob(5, c);
                rs.updateRow();
            }

            connection.rollback();

            rs = ps.executeQuery();

            Blob b = new JDBCBlob(
                new byte[] {
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
            });

            if (rs.next()) {
                rs.updateBlob(4, b);
                rs.updateRow();
            }

            connection.rollback();

            rs = ps.executeQuery();

            if (rs.next()) {
                rs.updateClob(5, c);
                rs.updateClob(5, c);
                rs.updateRow();
            }

            connection.rollback();

            rs = ps.executeQuery();

            Reader r = new java.io.CharArrayReader(
                "123456789abcdef".toCharArray());

            if (rs.next()) {
                rs.updateClob(5, c);
                rs.updateClob(5, r, 5);
                rs.updateRow();
            }

            connection.rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void testUpdatable2() {

        String select = "select i, v from t1 where i > ?";

        try {

            // statement.execute("SET DATABASE EVENT LOG SQL LEVEL 3");
            populateTable();
            connection.setAutoCommit(false);

            PreparedStatement ps = connection.prepareStatement(
                select,
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

            ps.setFetchSize(fetchSize);
            ps.setInt(1, -1);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String s = rs.getString(2);

                rs.updateString(2, s + s);
                rs.updateRow();
            }

            rs.close();

            rs = ps.executeQuery();

            while (rs.next()) {
                String s = rs.getString(2);

                System.out.println(s);
            }

            connection.rollback();

            rs = ps.executeQuery();

            while (rs.next()) {
                String s = rs.getString(2);

                System.out.println(s);
            }

            rs.first();
            rs.moveToInsertRow();
            rs.updateInt(1, 89);;

            try {
                rs.getString(2);
            } catch (SQLException ex) {
                System.out.println(ex.getMessage());
            }

            rs.updateString(2, "New String");

            int    value1 = rs.getInt(1);
            String value2 = rs.getString(2);

            rs.insertRow();
            connection.commit();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void testUpdatable3() {

        String select = "select i, v from t1 where i > ?";

        try {

            // statement.execute("SET DATABASE EVENT LOG SQL LEVEL 3");
            populateTable();
            connection.setAutoCommit(false);

            PreparedStatement ps = connection.prepareStatement(
                select,
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

            ps.setFetchSize(fetchSize);
            ps.setInt(1, -1);

            ResultSet rs          = ps.executeQuery();
            int       concurrency = rs.getConcurrency();
            int       holdability = rs.getHoldability();
            int       count       = 0;

            while (rs.next()) {
                String s = rs.getString(2);

                if (count == 4) {
                    count = 4;
                }

                rs.updateString(2, s + s);
                rs.updateRow();

                if (count == 4) {
                    boolean updated = rs.rowUpdated();

                    rs.updateString(2, s + s);
                    rs.updateRow();
                }

                count++;
            }

            rs.close();
            connection.commit();
            System.out.println("count 1: " + count);
            ps.setInt(1, -1);

            rs    = ps.executeQuery();
            count = 0;

            while (rs.next()) {
                count++;
            }

            System.out.println("count 2: " + count);
            connection.commit();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void testUpdatable4() {

        String select = "select i, v from t1 where i > ?";

        try {

            // statement.execute("SET DATABASE EVENT LOG SQL LEVEL 3");
            connection.setAutoCommit(false);
            populateTable();

            PreparedStatement ps = connection.prepareStatement(
                select,
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

            ps.setFetchSize(fetchSize);
            ps.setInt(1, -1);

            ResultSet result = ps.executeQuery();
            int holdability = result.getHoldability();    // CLOSE_CURSORS_AT_COMMIT == 2
            int concurrency = result.getConcurrency();    // CONCUR_UPDATABLE == 1008
            int       count  = 0;

            while (result.next()) {
                count++;
            }

            System.out.println("Count 1: " + count);

            if (count > 2) {
                result.absolute(2);
                result.updateString(2, "test1");
                result.updateRow();
                result.absolute(3);
                System.out.println("Name: " + result.getString(2));
                result.absolute(2);

                try {
                    result.updateString(2, "test2");
                    result.updateRow();
                } catch (SQLException ex) {
                    System.out.println("Exception: " + ex.getSQLState());
                }
            }

            count = 0;

            result.beforeFirst();

            while (result.next()) {
                count++;
            }

            System.out.println("Count 2: " + count);
            connection.commit();                          // closes the ResultSet

            result = ps.executeQuery();

            try {
                count = 0;

                result.beforeFirst();

                while (result.next()) {
                    count++;
                }

                System.out.println("Count 3: " + count);
            } catch (SQLException ex) {
                System.out.println(ex);                   // result set is closed
            }

            // PreparedStatement statement = (PreparedStatement) result.getStatement();
            result.close();
            ps.setInt(1, -1);

            ResultSet result2 = ps.executeQuery();

            count = 0;

            while (result2.next()) {
                count++;
            }

            System.out.println("Count 4: " + count);
            connection.commit();
            connection.setAutoCommit(true);
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void populateTable() throws SQLException {

        statement.execute("drop table t1 if exists cascade");
        statement.execute(
            "create table t1 (i int primary key, v varchar(100))");

        String            insert = "insert into t1 values(?,?)";
        PreparedStatement ps     = connection.prepareStatement(insert);

        for (int i = 0; i < 10; i++) {
            ps.setInt(1, i);
            ps.setString(2, String.valueOf(i) + " s");
            ps.execute();
        }

        ps.close();
        connection.commit();
        statement.execute("drop view v1 if exists");
        statement.execute("create view v1 as select * from t1");
    }

    public void testUpdatable5() {

        String select = "select v2, v1, id, v1 * 2 from t3";

        try {

            // statement.execute("SET DATABASE EVENT LOG SQL LEVEL 3");
            connection.setAutoCommit(false);
            populateTableBig();

            PreparedStatement ps = connection.prepareStatement(
                select,
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

            ps.setFetchSize(fetchSize);

            ResultSet result = ps.executeQuery();
            int holdability = result.getHoldability();    // CLOSE_CURSORS_AT_COMMIT == 2
            int concurrency = result.getConcurrency();    // CONCUR_UPDATABLE == 1008
            int       count  = 0;

            while (result.next()) {
                count++;
            }

            System.out.println("Count 1: " + count);

            try {
                result.updateString(1, "test1");
            } catch (SQLException x) {
                System.out.println("Update on invalid row; " + x.getMessage());
            }

            if (count > 2) {
                result.absolute(2);
                result.updateString(1, "test1");
                result.updateRow();
                result.absolute(3);
                System.out.println("Name row 3: " + result.getString(1));
                result.absolute(2);
                System.out.println("Name row 2: " + result.getString(1));

                try {
                    result.updateString(1, "test2");
                    result.updateRow();
                    System.out.println("Name row 2: " + result.getString(1));
                } catch (SQLException ex) {
                    System.out.println("Exception: " + ex.getSQLState());
                }
            }

            count = 0;

            result.beforeFirst();

            while (result.next()) {
                count++;
            }

            System.out.println("Count 2: " + count);
            connection.commit();                          // closes the ResultSet

            try {
                count = 0;

                result.beforeFirst();

                while (result.next()) {
                    count++;
                }

                System.out.println("Count 3: " + count);
            } catch (SQLException ex) {
                System.out.println(ex);                   // result set is closed
            }

            // PreparedStatement statement = (PreparedStatement) result.getStatement();
            result.close();

            ResultSet result2 = ps.executeQuery();

            count = 0;

            while (result2.next()) {
                count++;
            }

            System.out.println("Count 4: " + count);
            connection.commit();
            connection.setAutoCommit(true);
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void populateTableBig() throws SQLException {

        statement.execute("drop table t3 if exists cascade");
        statement.execute(
            "create table t3 (id int primary key, v1 int, v2 varchar(16))");

        String insert =
            "insert into t3 values (1, 11, 'elev'), (2, 13, 'thirteen'), (3, 17, 'fourteen'), (4, 19, 'nineteen'), (5, 23, 'twenty three') ";

        statement.execute(insert);
        connection.commit();
    }

    public void testDeletable4() {

        try {
            statement.execute("drop table t1 if exists cascade");
            statement.execute(
                "create table t1 (i int primary key, c varchar(10), t varbinary(3))");

            String            insert = "insert into t1 values(?,?,?)";
            String            select = "select i, c, t from t1";
            PreparedStatement ps     = connection.prepareStatement(insert);

            for (int i = 0; i < 10; i++) {
                ps.setInt(1, i);
                ps.setString(2, String.valueOf(i) + " s");
                ps.setBytes(3, new byte[]{ (byte) i, ' ', (byte) i });
                ps.execute();
            }

            connection.setAutoCommit(false);

            ps = connection.prepareStatement(
                select,
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

            ps.setFetchSize(fetchSize);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String s = rs.getString(2);

                rs.deleteRow();
            }

            rs.beforeFirst();

            while (rs.next()) {
                String  s       = rs.getString(2);
                boolean deleted = rs.rowDeleted();

                if (s != null || !deleted) {
                    System.out.println("row not deleted");
                }
            }

            rs.close();

            rs = statement.executeQuery(select);

            while (rs.next()) {
                super.fail("rows not deleted");
            }

            connection.rollback();

            rs = statement.executeQuery(select);

            while (rs.next()) {
                String s = rs.getString(2);

                System.out.println(s);
            }

            connection.commit();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void testDeletable2() {

        try {
            Connection c = newConnection();
            String dropSQL = "drop table test if exists cascade";
            String createSQL =
                "create table test (num INTEGER PRIMARY KEY, str VARCHAR(25))";
            Statement createStmt = c.createStatement();

            createStmt.execute(dropSQL);
            createStmt.execute(createSQL);
            createStmt.close();

            String            ins   = "insert into test (num,str) values (?,?)";
            PreparedStatement pStmt = c.prepareStatement(ins);

            for (int i = 0; i < 100; i++) {
                pStmt.setInt(1, i);
                pStmt.setString(2, "String" + i);
                pStmt.execute();
            }

            // there should now be 100 rows in the table
            String select = "SELECT * FROM test";
            PreparedStatement ps = c.prepareStatement(
                select,
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

            ps.setFetchSize(fetchSize);

            ResultSet rs = ps.executeQuery();

            rs.beforeFirst();

            while (rs.next()) {
                int num = rs.getInt("num");

                if ((num % 7) == 0) {
                    System.out.println("Deleting row:" + num);
                    rs.deleteRow();
                }
            }

            Statement dropStmt = c.createStatement();

            dropStmt.execute("drop table test;");
            dropStmt.close();
            connection.close();;
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void testScrollable() {

        try {
            statement.execute("drop table t1 if exists cascade");
            statement.execute(
                "create table t1 (i int primary key, c varchar(10), t varbinary(3))");
            statement.close();

            String            insert = "insert into t1 values(?,?,?)";
            String            select = "select i, c, t from t1";
            PreparedStatement ps     = connection.prepareStatement(insert);

            for (int i = 0; i < 10; i++) {
                ps.setInt(1, i);
                ps.setString(2, String.valueOf(i) + " s");
                ps.setBytes(3, new byte[]{ (byte) i, ' ', (byte) i });
                ps.execute();
            }

            connection.setAutoCommit(false);

            statement = connection.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

            statement.setFetchSize(fetchSize);

            ResultSet srs = statement.executeQuery("select * from t1 limit 2");

            srs.afterLast();

            while (srs.previous()) {
                String name = srs.getString(2);
                float  id   = srs.getFloat(1);

                System.out.println(name + "   " + id);
            }

            srs.close();

            srs = statement.executeQuery("select * from t1 limit 2");

            srs.absolute(3);

            while (srs.previous()) {
                String name = srs.getString(2);
                float  id   = srs.getFloat(1);

                System.out.println(name + "   " + id);
            }

            srs.absolute(2);

            while (srs.previous()) {
                String name = srs.getString(2);
                float  id   = srs.getFloat(1);

                System.out.println(name + "   " + id);
            }

            srs.absolute(-1);

            while (srs.previous()) {
                String name = srs.getString(2);
                float  id   = srs.getFloat(1);

                System.out.println(name + "   " + id);
            }

            connection.commit();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void populateTableUpdateCascade() throws SQLException {

        statement.execute("drop table t3 if exists cascade");
        statement.execute(
            "create table t3 (id int primary key, v1 int, v2 varchar(16), foreign key (v1) references t3(id) on update cascade)");

        String insert =
            "insert into t3 values (1, null, 'elev'), (2, 1, 'thirteen'), (3, 1, 'fourteen'), (4, 2, 'nineteen'), (5, 3, 'twenty three') ";

        statement.execute(insert);
        connection.commit();
    }

    public void testUpdatableCascade() {

        String select = "select id, v1, v2 from t3";

        try {

            // statement.execute("SET DATABASE EVENT LOG SQL LEVEL 3");
            connection.setAutoCommit(false);
            populateTableUpdateCascade();

            PreparedStatement ps = connection.prepareStatement(
                select,
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

            ps.setFetchSize(fetchSize);

            ResultSet result = ps.executeQuery();
            int       count  = 0;

            while (result.next()) {
                count++;

                if (count == 3) {
                    result.getString(3);
                }
            }

            System.out.println("Count 1: " + count);

            if (count > 2) {
                result.absolute(2);
                result.updateInt(1, 6);
                result.updateRow();
                result.absolute(4);

                String s = result.getString(3);

                System.out.println("Name row 3: " + result.getString(3));
                result.absolute(2);
                System.out.println("Name row 2: " + result.getString(3));

                try {
                    result.updateString(3, "test2");
                    result.updateRow();
                    System.out.println("Name row 2: " + result.getString(3));
                } catch (SQLException ex) {
                    System.out.println("Exception: " + ex.getSQLState());
                }
            }

            count = 0;

            result.beforeFirst();

            while (result.next()) {
                if (result.rowUpdated()) {
                    count++;
                }
            }

            System.out.println("Updated Row Count: " + count);
            connection.commit();    // closes the ResultSet

            ResultSet result2 = ps.executeQuery();

            count = 0;

            while (result2.next()) {
                count++;
            }

            System.out.println("Count After update " + count);
            connection.commit();
            connection.setAutoCommit(true);
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void populateTableDeleteCascade() throws SQLException {

        statement.execute("drop table t3 if exists cascade");
        statement.execute(
            "create table t3 (id int primary key, v1 int, v2 varchar(16), foreign key (v1) references t3(id) on delete cascade)");

        String insert =
            "insert into t3 values (1, null, 'elev'), (2, 1, 'thirteen'), (3, 1, 'fourteen'), (4, 2, 'nineteen'), (5, 3, 'twenty three') ";

        statement.execute(insert);
        connection.commit();
    }

    public void testUpdatableDeleteCascade() {

        String select = "select id, v1, v2 from t3";

        try {

            // statement.execute("SET DATABASE EVENT LOG SQL LEVEL 3");
            connection.setAutoCommit(false);

            Statement s = connection.createStatement();

            s.execute("SET DATABASE DEFAULT TABLE TYPE CACHED");
            s.execute("SET DATABASE DEFAULT RESULT MEMORY ROWS 2");
            populateTableDeleteCascade();

            PreparedStatement ps = connection.prepareStatement(
                select,
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

            ps.setFetchSize(fetchSize);

            ResultSet result = ps.executeQuery();
            int       count  = 0;

            while (result.next()) {
                count++;
            }

            System.out.println("Count 1: " + count);

            if (count > 2) {
                result.absolute(2);
                result.deleteRow();
                result.absolute(4);
                System.out.println("Name row 4: " + result.getString(3));
                result.absolute(2);
                System.out.println("Name row 2: " + result.getString(3));
                result.absolute(3);
                System.out.println("Name row 3: " + result.getString(3));

                try {
                    result.updateString(3, "test2");
                    result.updateRow();
                    System.out.println("Name row 3: " + result.getString(3));
                } catch (SQLException ex) {
                    System.out.println("Exception: " + ex.getMessage());
                }
            }

            count = 0;

            result.beforeFirst();

            while (result.next()) {
                if (!result.rowDeleted()) {
                    count++;
                }
            }

            System.out.println(
                "Count rows (not deleted) after delete cascade: " + count);
            connection.commit();    // closes the ResultSet

            ResultSet result2 = ps.executeQuery();

            count = 0;

            while (result2.next()) {
                count++;
            }

            System.out.println("Count rows after commit: " + count);
            connection.commit();
            connection.setAutoCommit(true);
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
