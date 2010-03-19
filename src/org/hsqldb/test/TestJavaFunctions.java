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

import java.sql.*;
import java.sql.SQLException;

public class TestJavaFunctions extends TestBase {

    public TestJavaFunctions() {
        super("TestJavaFunction", "jdbc:hsqldb:file:test3", false, false);
    }

    protected void setUp() {

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

        s.executeUpdate("DROP FUNCTION TESTQUERY IF EXISTS");
        s.executeUpdate("DROP TABLE T IF EXISTS");
        s.executeUpdate("CREATE TABLE T(C VARCHAR(20), I INT)");
        s.executeUpdate("INSERT INTO T VALUES 'Thames', 10");
        s.executeUpdate("INSERT INTO T VALUES 'Fleet', 12");
        s.executeUpdate("INSERT INTO T VALUES 'Brent', 14");
        s.executeUpdate("INSERT INTO T VALUES 'Westbourne', 16");
        s.executeUpdate(
            "CREATE FUNCTION TESTQUERY(INT) RETURNS TABLE(N VARCHAR(20), I INT) "
            + " READS SQL DATA LANGUAGE JAVA EXTERNAL NAME 'CLASSPATH:org.hsqldb.test.TestJavaFunctions.getQueryResult'");
        c.close();
    }

    public void testOne() throws SQLException {

        Connection        c = newConnection();
        CallableStatement s = c.prepareCall("CALL TESTQUERY(6)");

        s.execute();

        ResultSet r = s.getResultSet();

        while (r.next()) {
            r.next();
        }

        c.close();
    }

    public static void main(String[] args) throws SQLException {}

    public static ResultSet getQueryResult(Connection connection,
                                           int i) throws SQLException {

        Statement st = connection.createStatement();

        return st.executeQuery("SELECT * FROM T WHERE I < " + i);
    }
}
