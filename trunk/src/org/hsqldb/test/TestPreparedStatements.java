/* Copyright (c) 2001-2021, The HSQL Development Group
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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestCase;

import java.sql.Array;

/**
 * @author fredt@users
 */
public class TestPreparedStatements extends TestCase {

    private Connection con = null;

    private static class sqlStmt {

        boolean  prepare;
        boolean  update;
        String   command;
        Object[] args;
        Object   ret;

        sqlStmt(String c) {

            command = c;
            prepare = false;
            update  = false;
        }

        sqlStmt(String c, boolean p, boolean u, Object[] a) {

            command = c;
            prepare = p;
            update  = u;
            args    = a;
        }

        sqlStmt(String c, boolean p, boolean u, Object[] a, Object r) {

            command = c;
            prepare = p;
            update  = u;
            args    = a;
            ret     = r;
        }
    }

    private sqlStmt[] stmtArray = {
        new sqlStmt("drop table public.ivtest if exists cascade"),
        new sqlStmt(
            "create cached table ivtest(interval1 INTERVAL YEAR TO MONTH,"
            + " interval2 INTERVAL DAY TO SECOND(3))"),
        new sqlStmt("drop table public.dttest if exists cascade"),
        new sqlStmt("create cached table dttest(adate date not null, "
                    + "atime time not null,bg int, primary key(adate,atime))"),
        new sqlStmt(
            "insert into dttest values(current_date - 10 day, current_time + 1 hour, 1)",
            false, true, null),
        new sqlStmt(
            "insert into dttest values(current_date - 8 day, current_time - 5 hour, 2)",
            false, true, null),
        new sqlStmt(
            "insert into dttest values(current_date - 7 day, current_time - 4 hour, 3)",
            false, true, null),
        new sqlStmt("insert into dttest values(current_date, '12:44:31', 4)",
                    false, true, null),
        new sqlStmt(
            "insert into dttest values(current_date + 3 day, current_time - 12 hour, 5)",
            false, true, null),
        new sqlStmt("insert into dttest values(current_date + 1 day, current_time - 1 hour, 6)", false, true, null),
        new sqlStmt("select atime adate from dttest where atime =  ? and adate = ?",
                    true, false, new Object[] {
            "12:44:31", new java.sql.Date(System.currentTimeMillis())
        }), new sqlStmt("insert into ivtest values ?, ?", true, true,
                        new Object[] {
            "1-10", "10 02:15:30.333"
        }), new sqlStmt(
            "insert into ivtest values CAST (? AS INTERVAL YEAR TO MONTH), CAST (? AS INTERVAL DAY TO SECOND)",
            true, true, new Object[] {
            "1-10", "10 02:15:30.333"
        }), new sqlStmt("script", true, false, null),
        new sqlStmt("drop table public.bintest if exists cascade"),
        new sqlStmt("create cached table bintest(val BIGINT, id BINARY(100))"),
        new sqlStmt("insert into bintest values ?, ?", true, true,
                    new Object[] {
            10L, new byte[] {
                1, 2, 3, 4, 5
            }
        }), new sqlStmt("select val from bintest where id = ?", true, false,
                        new Object[]{ new byte[] {
            1, 2, 3, 4, 5
        } }, 10L),
    };

    public TestPreparedStatements(String name) {
        super(name);
    }

    protected void setUp() {

        String url = "jdbc:hsqldb:mem:test";

        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");

            con = java.sql.DriverManager.getConnection(url, "sa", "");
        } catch (Exception e) {}
    }

    public void testA() {

        int i = 0;

        try {
            for (i = 0; i < stmtArray.length; i++) {
                int j;

                System.out.println(" -- #" + i + " ----------------------- ");

                if (stmtArray[i].prepare) {
                    Object[]          stmtArgs = stmtArray[i].args;
                    PreparedStatement ps       = null;

                    System.out.println(" -- preparing\n<<<\n"
                                       + stmtArray[i].command + "\n>>>\n");

                    ps = con.prepareStatement(stmtArray[i].command);

                    if (stmtArgs != null) {
                        System.out.print(" -- setting " + stmtArgs.length
                                         + " Args [");

                        for (j = 0; j < stmtArgs.length; j++) {
                            System.out.print((j > 0 ? "; "
                                                    : "") + stmtArgs[j]);
                            ps.setObject(j + 1, stmtArgs[j]);
                        }

                        System.out.println("]");
                    }

                    System.out.println(" -- executing ");

                    if (stmtArray[i].update) {
                        int r = ps.executeUpdate();

                        System.out.println(" ***** ps.executeUpdate gave me "
                                           + r);
                    } else {
                        boolean b     = ps.execute();
                        int     count = 0;

                        if (b) {
                            ResultSet rs = ps.getResultSet();

                            while (rs.next()) {
                                if (count == 0 && stmtArray[i].ret != null) {
                                    super.assertEquals(stmtArray[i].ret,
                                                       rs.getObject(1));
                                }

                                count++;
                            }

                            System.out.print(
                                " ***** ps.execute returned result row count "
                                + count);
                        } else {
                            System.out.print(" ***** ps.execute gave me " + b);
                        }
                    }
                } else {
                    System.out.println(" -- executing directly\n<<<\n"
                                       + stmtArray[i].command + "\n>>>\n");

                    Statement s = con.createStatement();
                    boolean   b = s.execute(stmtArray[i].command);

                    System.out.println(" ***** st.execute gave me " + b);
                }
            }
        } catch (Exception e) {
            System.out.println(i + " ?? Caught Exception " + e);
            super.fail();
        }

        assertTrue(true);
    }

    public void testB() throws SQLException, ClassNotFoundException {

        Statement statement = con.createStatement();

        statement.execute("DROP TABLE IF EXISTS users CASCADE");
        statement.execute(
            "CREATE TABLE IF NOT EXISTS users (id INTEGER, name VARCHAR(25), PRIMARY KEY(id))");
        statement.executeUpdate("INSERT INTO users VALUES(1, 'Ramiro')");
        statement.executeUpdate("INSERT INTO users VALUES(2, 'Chanukya')");

        String storedProcedure1 =
            "CREATE PROCEDURE sp_say_hi(IN greeting_p VARCHAR(10)) "
            + "READS SQL DATA DYNAMIC RESULT SETS 2 " + "BEGIN ATOMIC "
            + "DECLARE result CURSOR WITH RETURN FOR SELECT COALESCE(greeting_p, 'Hi')+' '+name as greeting FROM users FOR READ ONLY; "
            + "DECLARE result1 CURSOR WITH RETURN FOR SELECT * FROM users FOR READ ONLY; "
            + "OPEN result; " + "OPEN result1; " + "END";

        statement.execute(storedProcedure1);

        String            sqlCall           = "CALL sp_say_hi(?)";
        CallableStatement callableStatement = con.prepareCall(sqlCall);

        callableStatement.setObject("GREETING_P", "Hola");

        boolean result = callableStatement.execute();

        assertFalse(result);

        int value = callableStatement.getUpdateCount();

        assertTrue(value == 0);

        result = callableStatement.getMoreResults();

        assertTrue(result);

        ResultSet result1 = callableStatement.getResultSet();

        assertTrue(result1.next());

        result = callableStatement.getMoreResults();

        assertTrue(result);

        ResultSet result2 = callableStatement.getResultSet();

        assertTrue(result2.next());

        result = callableStatement.getMoreResults();

        assertFalse(result);

        value = callableStatement.getUpdateCount();

        assertTrue(value == -1);
    }

    public void testC() throws SQLException, ClassNotFoundException {

        Statement statement = con.createStatement();

        statement.execute("DROP TABLE IF EXISTS testusers");
        statement.execute(
            "CREATE TABLE IF NOT EXISTS testusers (id INTEGER, name VARCHAR(25), PRIMARY KEY(id))");
        statement.executeUpdate("INSERT INTO testusers VALUES(1, 'John')");
        statement.executeUpdate("INSERT INTO testusers VALUES(2, 'Tom')");
        statement.executeUpdate("INSERT INTO testusers VALUES(3, 'Carl')");
        statement.executeUpdate("INSERT INTO testusers VALUES(4, 'Greg')");
        statement.executeUpdate("INSERT INTO testusers VALUES(5, 'David')");
        statement.executeUpdate("INSERT INTO testusers VALUES(6, 'Keith')");

        String select = "SELECT name FROM testusers WHERE name IN (UNNEST(?))";
        PreparedStatement preparedStatement = con.prepareStatement(select,
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        String[] ids   = new String[] {
            "Paul", "Greg", "Tom"
        };
        Array    array = con.createArrayOf("VARCHAR", ids);

        preparedStatement.setArray(1, array);

        ResultSet result1 = preparedStatement.executeQuery();

        assertTrue(result1.next());
        assertTrue(result1.next());
/*
        String select2 = "SELECT name FROM testusers WHERE name IN (CAST(? AS VARCHAR(10) ARRAY))";
        PreparedStatement preparedStatement2 = con.prepareStatement(select2,
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        preparedStatement2.setArray(1, array);

        ResultSet result2 = preparedStatement2.executeQuery();

        assertTrue(result2.next());
        assertTrue(result2.next());
*/
    }

    public void testD() throws SQLException, ClassNotFoundException {

        String            select            = "SET SCHEMA ?";
        PreparedStatement preparedStatement = con.prepareStatement(select);

        preparedStatement.setString(1, "INFORMATION_SCHEMA");

        int updateCount = preparedStatement.executeUpdate();

        assertEquals(0, updateCount);
    }

    /**
     * two statements with same SQL
     */
    public void testE() throws SQLException, ClassNotFoundException {

        Statement statement = con.createStatement();

        statement.execute("DROP TABLE IF EXISTS testusers");
        statement.execute(
            "CREATE TABLE IF NOT EXISTS testusers (id INTEGER, name VARCHAR(25), PRIMARY KEY(id))");

        try {
            statement.executeUpdate("INSERT INTO testusers VALUES(1, 'John')");
            statement.executeUpdate("INSERT INTO testusers VALUES(2, 'Tom')");
            statement.executeUpdate("INSERT INTO testusers VALUES(3, 'Carl')");
            statement.executeUpdate("INSERT INTO testusers VALUES(4, 'Greg')");
            statement.executeUpdate(
                "INSERT INTO testusers VALUES(5, 'David')");
            statement.executeUpdate(
                "INSERT INTO testusers VALUES(6, 'Keith')");
        } catch (SQLException e) {}

        String select = "SELECT name FROM testusers WHERE name = ?";
        PreparedStatement preparedStatementOne = con.prepareStatement(select,
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        preparedStatementOne.setString(1, "Tom");

        ResultSet result1 = preparedStatementOne.executeQuery();

        assertTrue(result1.next());

        PreparedStatement preparedStatementTwo = con.prepareStatement(select,
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        preparedStatementTwo.setString(1, "Tom");

        ResultSet result2 = preparedStatementTwo.executeQuery();

        assertTrue(result2.next());

        // close first statement
        preparedStatementOne.close();

        // second statement should still be valid
        preparedStatementTwo.setString(1, "Greg");

        result2 = preparedStatementTwo.executeQuery();

        assertTrue(result2.next());
    }

    /**
     * two statements with same SQL
     */
    public void testF() throws SQLException, ClassNotFoundException {

        Statement st = con.createStatement();

        st.execute("DROP TABLE IF EXISTS testtable");
        st.execute("CREATE TABLE testtable (column1 DATE)");
        st.execute("INSERT INTO testtable VALUES (sysdate)");

        PreparedStatement pst = con.prepareStatement(
            "select * from testtable where column1 between ?-? and ?");
        Date date = new Date(System.currentTimeMillis());

        pst.setDate(1, date);
        pst.setInt(2, 3);
        pst.setDate(3, date);

        ResultSet resultSet = pst.executeQuery();
    }

    /**
     * generated column name case-sensitivity
     */
    public void testG() throws SQLException, ClassNotFoundException {

        Statement st = con.createStatement();

        st.execute("DROP TABLE IF EXISTS testtable");
        st.execute("CREATE TABLE testtable (id INT GENERATED BY DEFAULT AS IDENTITY, column1 DATE)");
        st.execute("INSERT INTO testtable (column1) VALUES (sysdate)", new String[]{"id"});

        ResultSet rs = st.getGeneratedKeys();

        boolean genFound = rs.next();

        assertTrue(genFound);

        PreparedStatement pst = con.prepareStatement(
            "select * from testtable where column1 between ?-? and ?");
        Date date = new Date(System.currentTimeMillis());

        pst.setDate(1, date);
        pst.setInt(2, 3);
        pst.setDate(3, date);

        ResultSet resultSet = pst.executeQuery();

        boolean found = rs.next();

    }

    public void testH() throws SQLException {

        String sqlOne = "with t1(f1) as (select cast(? as int) from (values(0)) t), t2(f2) as (select cast(? as int) from (values(0)) t) select f1 + f2 from t1, t2";
        String sqlTwo = "with t1(f1) as (select cast(:first as int) from (values(0)) t), t2(f2) as (select cast(:sec as int) from (values(0)) t) select f1 + f2 from t1, t2";

        PreparedStatement pst = con.prepareStatement(sqlOne);

        pst.setInt(1, 3);
        pst.setInt(2, 2);

        ResultSet rs = pst.executeQuery();

        assertTrue(rs.next());

        int val = rs.getInt(1);
        assertEquals(5, val);

        pst = con.prepareStatement(sqlOne);

        pst.setInt(1, 3);
        pst.setInt(2, 2);

        rs = pst.executeQuery();

        assertTrue(rs.next());

        val = rs.getInt(1);
        assertEquals(5, val);

    }


}
