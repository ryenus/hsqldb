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
import java.sql.ResultSet;
import java.sql.Statement;

import junit.framework.TestCase;
import junit.framework.TestResult;

import java.sql.DriverManager;
import java.sql.CallableStatement;
import java.sql.SQLException;

/**
 * Tests for stored procedures.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.7.2
 * @since 1.7.2
 */
public class TestStoredProcedure extends TestBase {

    public TestStoredProcedure(String name) {
        super(name);
    }

    public void testOne() throws Exception {

        Connection conn = newConnection();
        Statement  statement;

        try {
            statement = conn.createStatement();

            ResultSet rs = statement.executeQuery(
                "call \"org.hsqldb.test.TestStoredProcedure.procTest1\"()");

            rs.next();

            int cols = rs.getInt(1);

            assertFalse("test result not correct", false);
        } catch (Exception e) {}

        try {
            statement = conn.createStatement();

            statement.execute(
                "CREATE temp TABLE MYTABLE(COL1 INTEGER,COL2 VARCHAR(10));");
            statement.execute(
                "CREATE PROCEDURE proc1(IN P1 INT, IN P2 INT, OUT P3 INT) "
                + "SPECIFIC P2 LANGUAGE JAVA DETERMINISTIC MODIFIES SQL DATA EXTERNAL NAME 'CLASSPATH:org.hsqldb.test.TestStoredProcedure.procTest2'");

            CallableStatement c = conn.prepareCall("call proc1(1,2,?)");

            c.execute();

            int value = c.getInt(1);

            assertEquals(value, 2);
        } catch (Exception e) {
            assertTrue("unexpected error", true);
        } finally {
            conn.close();
        }
    }

    public void atestTwo() throws Exception {

        Connection conn = newConnection();
        Statement  statement;
        int        updateCount;

        try {
            statement = conn.createStatement();

            statement.execute("create user testuser password 'test'");
            statement.execute("create table testtable(v varchar(20))");
            statement.execute(
                "insert into testtable values ('tennis'), ('tent'), ('television'), ('radio')");

            ResultSet rs = statement.executeQuery(
                "call \"org.hsqldb.test.TestStoredProcedure.funcTest2\"('test')");

            rs.next();

            boolean b = rs.getBoolean(1);

            rs.close();
            assertTrue("test result not correct", b);
            statement.execute(
                "create function func2(varchar(20)) returns boolean "
                + "SPECIFIC F2 LANGUAGE JAVA DETERMINISTIC NO SQL CALLED ON NULL INPUT EXTERNAL NAME 'CLASSPATH:org.hsqldb.test.TestStoredProcedure.funcTest2'");

            rs = statement.executeQuery("call func2('test')");

            rs.next();

            b = rs.getBoolean(1);

            rs.close();
            assertTrue("test result not correct", b);

            rs = statement.executeQuery(
                "select count(*) from testtable where func2(v)");

            rs.next();

            int count = rs.getInt(1);

            assertTrue("test result not correct", count == 3);
            statement.execute(
                "grant execute on specific function public.f2 to testuser");
        } catch (Exception e) {
            assertTrue("unable to execute call to procedure", false);
        } finally {
            conn.close();
        }
    }

    public void testTwo() throws SQLException {
        Connection conn = newConnection();

        Statement st = conn.createStatement();
        st.execute("declare varone int default 0;");
        st.execute("create procedure proc_inout_result (inout intp int) " +
                   " language java reads sql data external name 'CLASSPATH:org.hsqldb.test.Test01JRT.procWithResultOne'");
        CallableStatement cs = conn.prepareCall("call proc_inout_result(varone)");
        cs.execute();
        ResultSet rs = cs.getResultSet();

        rs.next();

        assertEquals(rs.getString(1), "SYSTEM_LOBS");
        assertEquals(rs.getString(2), "LOB_IDS");

        rs.close();
    }

    public static void procWithResultOne(Integer[] intparam,
                                         ResultSet[] resultparam)
                                         throws SQLException {

        Connection conn =
            DriverManager.getConnection("jdbc:default:connection");

        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(
            "select count(*) from information_schema.columns where table_name='LOB_IDS' and table_schema='SYSTEM_LOBS'");

        if (rs.next()) {
            intparam[0] = rs.getInt(1);

            rs.close();
        }

        resultparam[0] = st.executeQuery(
            "select table_schema, table_name from information_schema.tables where table_name='LOB_IDS' and table_schema='SYSTEM_LOBS'");
    }

    public static void procTest1(Connection conn)
    throws java.sql.SQLException {

        int                cols;
        java.sql.Statement stmt = conn.createStatement();

        stmt.execute("INSERT INTO MYTABLE VALUES    (1,'test1');");
        stmt.execute("INSERT INTO MYTABLE VALUES(2,'test2');");

        java.sql.ResultSet rs = stmt.executeQuery("select * from MYTABLE");
        java.sql.ResultSetMetaData meta = rs.getMetaData();

        cols = meta.getColumnCount();

        rs.close();
        stmt.close();
    }

    public static void procTest2(int p1, int p2,
                                 Integer[] p3) throws java.sql.SQLException {

        Connection conn =
            DriverManager.getConnection("jdbc:default:connection");
        java.sql.Statement stmt = conn.createStatement();

        stmt.execute("INSERT INTO MYTABLE VALUES(" + p1 + ",'test1')");
        stmt.execute("INSERT INTO MYTABLE VALUES(" + p2 + ",'test2')");

        java.sql.ResultSet rs = stmt.executeQuery("select * from MYTABLE");
        java.sql.ResultSetMetaData meta = rs.getMetaData();

        int cols  = meta.getColumnCount();
        p3[0] = Integer.valueOf(cols);

        rs.close();
        stmt.close();
    }

    public static boolean funcTest2(Connection conn,
                                    String value)
                                    throws java.sql.SQLException {

        if (value != null && value.startsWith("te")) {
            return true;
        }

        return false;
    }

    public static void main(String[] args) throws Exception {

        TestResult            result;
        TestCase              test;
        java.util.Enumeration failures;
        int                   count;

        result = new TestResult();
        test   = new TestStoredProcedure("test");

        test.run(result);

        count = result.failureCount();

        System.out.println("TestStoredProcedure failure count: " + count);

        failures = result.failures();

        while (failures.hasMoreElements()) {
            System.out.println(failures.nextElement());
        }
    }
}
