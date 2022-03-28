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


package org.hsqldb.test;

import java.math.BigInteger;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.hsqldb.jdbc.JDBCConnection;
import org.hsqldb.jdbc.JDBCResultSet;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.navigator.RowSetNavigatorClient;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.Type;

//#ifdef JAVA8
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
//#endif JAVA8

public class TestJavaFunctions extends TestBase {

    public TestJavaFunctions() {
        super("TestJavaFunction");
    }

    protected void setUp() throws Exception {

        super.setUp();

        try {
            prepareDatabase();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void prepareDatabase() throws SQLException {

        Connection c = newConnection();
        Statement  s = c.createStatement();

        s.executeUpdate("DROP FUNCTION TEST_QUERY IF EXISTS");
        s.executeUpdate("DROP FUNCTION TEST_CUSTOM_RESULT IF EXISTS");
        s.executeUpdate("DROP FUNCTION SORT_BYTE_ARRAY IF EXISTS");
        s.executeUpdate("DROP TABLE T IF EXISTS");
        s.executeUpdate("CREATE TABLE T(C VARCHAR(20), I INT)");
        s.executeUpdate("INSERT INTO T VALUES 'Thames', 10");
        s.executeUpdate("INSERT INTO T VALUES 'Fleet', 12");
        s.executeUpdate("INSERT INTO T VALUES 'Brent', 14");
        s.executeUpdate("INSERT INTO T VALUES 'Westbourne', 16");
        s.executeUpdate(
            "CREATE FUNCTION TEST_QUERY(INT) RETURNS TABLE(N VARCHAR(20), I INT) "
            + " READS SQL DATA LANGUAGE JAVA EXTERNAL NAME 'CLASSPATH:org.hsqldb.test.TestJavaFunctions.getQueryResult'");
        s.executeUpdate(
            "CREATE FUNCTION TEST_CUSTOM_RESULT(BIGINT, BIGINT) RETURNS TABLE(I BIGINT, N VARBINARY(1000)) "
            + " READS SQL DATA LANGUAGE JAVA EXTERNAL NAME 'CLASSPATH:org.hsqldb.test.TestJavaFunctions.getCustomResult'");
        s.executeUpdate(
            "CREATE FUNCTION SORT_BYTE_ARRAY(VARBINARY(20)) RETURNS VARBINARY(20) "
            + " NO SQL LANGUAGE JAVA EXTERNAL NAME 'CLASSPATH:org.hsqldb.test.TestJavaFunctions.getSortedByteArray'");
        s.executeUpdate("CHECKPOINT");
        c.close();
    }

    public void testOne() throws SQLException {

        Connection        c = newConnection();
        CallableStatement s = c.prepareCall("CALL TEST_QUERY(16)");

        s.execute();

        ResultSet r = s.getResultSet();

        while (r.next()) {
            String temp = "" + r.getInt(2) + " " + r.getString(1);

            System.out.println(temp);
        }

        s = c.prepareCall("CALL TEST_CUSTOM_RESULT(6, 19)");

        s.execute();

        r = s.getResultSet();

        while (r.next()) {
            String temp =
                "" + r.getLong(1) + " "
                + org.hsqldb.lib.StringConverter.byteArrayToSQLHexString(
                    r.getBytes(2));

            System.out.println(temp);
        }

        r = s.executeQuery();
        s = c.prepareCall("CALL TEST_CUSTOM_RESULT(6, 1900)");

        try {
            s.execute();

            r = s.getResultSet();

            fail("exception not thrown");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        c.close();
    }

//#ifdef JAVA8
    public void testTwo() throws SQLException {

        Connection c         = newConnection();
        Statement  statement = c.createStatement();

        // create function and link it to Java method
        statement.executeUpdate(
            "CREATE FUNCTION EXAMPLE_FUNCTION (months INTERVAL MONTH, days INTERVAL DAY)"
            + " RETURNS CHAR VARYING(100)"
            + " LANGUAGE JAVA DETERMINISTIC NO SQL"
            + " EXTERNAL NAME 'CLASSPATH:org.hsqldb.test.TestJavaFunctions.exampleIntervalFunction';");

        // use the function and print the result
        final ResultSet result = statement.executeQuery(
            "CALL EXAMPLE_FUNCTION(INTERVAL '3' MONTH, INTERVAL '5' DAY)");

        while (result.next()) {
            System.out.println(result.getString(1));
        }
    }

    public void testThree() throws SQLException {

        Connection c         = newConnection();
        Statement  statement = c.createStatement();

        // create function and link it to Java method
        statement.executeUpdate(
            "CREATE FUNCTION TIMESTAMP_FUNCTION (ts TIMESTAMP, tsz TIMESTAMP WITH TIME ZONE)"
            + " RETURNS CHAR VARYING(100)"
            + " LANGUAGE JAVA DETERMINISTIC NO SQL"
            + " EXTERNAL NAME 'CLASSPATH:org.hsqldb.test.TestJavaFunctions.exampleTimestampFunction';");

        // use the function and print the result
        ResultSet result = statement.executeQuery(
            "CALL TIMESTAMP_FUNCTION(TIMESTAMP'2019-01-01 01:02:03', TIMESTAMP'2019-01-01 01:02:03+5:00')");

        while (result.next()) {
            System.out.println(result.getString(1));
        }

        System.out.println(
            "TIMESTAMP'2019-01-01 01:02:03', TIMESTAMP'2019-01-01 01:02:03+5:00'");
        System.out.println(OffsetDateTime.of(2019, 1, 1, 1, 2, 3, 0,
                                             ZoneOffset.ofHours(5)));

        result =
            statement.executeQuery("CALL TIMESTAMP'2019-01-01 01:02:03+5:00'");

        while (result.next()) {
            System.out.println(result.getString(1));
        }
    }

    public void testFour() throws SQLException {

        Connection c         = newConnection();
        Statement  statement = c.createStatement();

        // create function and link it to Java method
        statement.executeUpdate(
            "CREATE FUNCTION TIME_FUNCTION (t TIME, tz TIME WITH TIME ZONE)"
            + " RETURNS CHAR VARYING(100)"
            + " LANGUAGE JAVA DETERMINISTIC NO SQL"
            + " EXTERNAL NAME 'CLASSPATH:org.hsqldb.test.TestJavaFunctions.exampleTimeFunction';");

        // use the function and print the result
        ResultSet result = statement.executeQuery(
            "CALL TIME_FUNCTION(TIME'01:02:03', TIME'01:02:03+5:00')");

        while (result.next()) {
            System.out.println(result.getString(1));
        }

        System.out.println("TIME'01:02:03', TIME'01:02:03+5:00'");
        System.out.println(OffsetTime.of(1, 2, 3, 0, ZoneOffset.ofHours(5)));

        result = statement.executeQuery("CALL TIME'01:02:03+5:00'");

        while (result.next()) {
            System.out.println(result.getString(1));
        }
    }

    public void testFive() throws SQLException {

        Connection connection = newConnection();
        Statement  statement  = connection.createStatement();

        statement.execute("DROP TABLE IF EXISTS ts CASCADE");
        statement.execute(
            "CREATE TABLE ts (id INTEGER PRIMARY KEY, expiry TIMESTAMP(9))");

        String insert = "INSERT INTO ts (id, expiry) VALUES(1, ?)";
        PreparedStatement preparedStatement =
            connection.prepareStatement(insert);
        Timestamp timestampBefore = new Timestamp(System.currentTimeMillis());

        timestampBefore.setNanos(123456789);
        preparedStatement.setTimestamp(1, timestampBefore);
        preparedStatement.executeUpdate();

        ResultSet resultSet =
            statement.executeQuery("SELECT id, expiry from ts where id = 1");

        if (resultSet.next()) {
            Timestamp timestampAfter = resultSet.getTimestamp("expiry");

            assertEquals(timestampBefore, timestampAfter);
        }

        statement.execute("DELETE FROM ts");

        Instant inst = Instant.ofEpochSecond(0, 123456789);
        preparedStatement.setObject(1, inst);
        preparedStatement.executeUpdate();

        resultSet =
                statement.executeQuery("SELECT id, expiry from ts where id = 1");

        if (resultSet.next()) {
            Timestamp timestampAfter = resultSet.getTimestamp("expiry");

            assertEquals(inst.getEpochSecond(), timestampAfter.getTime() / 1000);
            assertEquals(inst.getNano(), timestampAfter.getNanos());
        }
    }

    public static String exampleIntervalFunction(final java.time.Period months,
            final java.time.Duration days) {
        return "[months, days] : " + months.toString() + ", "
               + days.toString();
    }

    public static String exampleTimestampFunction(final java.sql.Timestamp ts,
            final java.time.OffsetDateTime tsz) {
        return "[ts, tsz] : " + ts.toString() + ", " + tsz.toString();
    }

    public static String exampleTimeFunction(final java.sql.Time t,
            final java.time.OffsetTime tz) {
        return "[ts, tsz] : " + t.toString() + ", " + tz.toString();
    }

//#endif JAVA8
    public static ResultSet getQueryResult(Connection connection,
                                           int i) throws SQLException {

        Statement st = connection.createStatement();

        return st.executeQuery("SELECT * FROM T WHERE I < " + i);
    }

    public static ResultSet getQueryResult(Connection connection,
                                           String p1) throws SQLException {
        return getQueryResult(connection, 13);
    }

    public static ResultSet getQueryResult(Connection connection, String p1,
                                           String p2) throws SQLException {
        return getQueryResult(connection, 20);
    }

    public static byte[] getSortedByteArray(byte[] bytes) throws SQLException {

        bytes = (byte[]) java.util.Arrays.copyOf(bytes, bytes.length);

        java.util.Arrays.sort(bytes);

        return bytes;
    }

    public static byte[][] getSortedArrayByteArray(byte[][] bytes)
    throws SQLException {

        bytes = (byte[][]) java.util.Arrays.copyOf(bytes, bytes.length);

        java.util.Arrays.sort(bytes);

        return bytes;
    }

    private static Result newTwoColumnResult() {

        Type[] types = new Type[2];

        types[0] = Type.SQL_BIGINT;
        types[1] = Type.SQL_VARBINARY_DEFAULT;

        ResultMetaData  meta = ResultMetaData.newSimpleResultMetaData(types);
        RowSetNavigator navigator = new RowSetNavigatorClient();
        Result          result    = Result.newDataResult(meta);

        result.setNavigator(navigator);

        return result;
    }

    public static ResultSet getCustomResult(Connection connection, long start,
            long end) throws SQLException {

        Result result = newTwoColumnResult();

        if (end < start) {
            long temp = start;

            start = end;
            end   = temp;
        }

        if (end > 1000) {
            throw org.hsqldb.jdbc.JDBCUtil.invalidArgument(
                "value larger than 100");
        }

        if (end > start + 100) {
            end = start + 100;
        }

        for (long i = start; i < end; i++) {
            Object[] row = new Object[2];

            row[0] = Long.valueOf(i);
            row[1] = new BinaryData(BigInteger.valueOf(i).toByteArray(),
                                    false);

            result.navigator.add(row);
        }

        result.navigator.reset();

        return new JDBCResultSet((JDBCConnection) connection, null, result,
                                 result.metaData);
    }
}
