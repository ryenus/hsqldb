/* Copyright (c) 2001-2025, The HSQL Development Group
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

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestRoutines extends TestBase {

    Statement  statement;
    Connection connection;

    public TestRoutines(String name) {
        super(name);
    }

    protected void setUp() throws Exception {

        super.setUp();

        connection = super.newConnection();
        statement  = connection.createStatement();
    }

    public void testOne() throws SQLException {

        String createTable =
        "CREATE TABLE testroutinestableone(id IDENTITY, value VARCHAR(20));";

        String createProc =
        "CREATE PROCEDURE testroutinesprocone(IN parameter1 INTEGER) " +
        "MODIFIES SQL DATA " +
        "BEGIN ATOMIC " +
        "DECLARE variable1 VARCHAR(20); " +
        "IF parameter1 = 1 THEN SET variable1 = 'birds'; " +
        "ELSE SET variable1 = 'beasts'; " +
        "END IF ; " +
        "INSERT INTO testroutinestableone VALUES (DEFAULT, variable1); " +
        "END;";

        statement.execute(createTable);
        statement.execute(createProc);
        statement.execute("CALL testroutinesprocone(1)");
        statement.execute("CALL testroutinesprocone(2)");
        ResultSet rs = statement.executeQuery("SELECT * FROM testroutinestableone ORDER BY id");
        rs.next();
        String s = rs.getString(2);
        assertEquals("birds", s);
    }

    public void testTwo() throws SQLException {

        String createProc =
                "CREATE FUNCTION testroutinesfunctwo(param1 VARCHAR, param2 VARCHAR) " +
                        "RETURNS VARCHAR ARRAY " +
                        "LANGUAGE JAVA PARAMETER STYLE JAVA  " +
                        "EXTERNAL NAME 'CLASSPATH:org.hsqldb.lib.StringUtil.split'";

        statement.execute(createProc);
        ResultSet rs = statement.executeQuery("CALL testroutinesfunctwo('first:second:third',':')");
        rs.next();
        String s = rs.getString(1);
        Array arr = rs.getArray(1);
        Object[] strArr = (Object[]) arr.getArray();
        System.out.println(strArr[0]);
    }

}
