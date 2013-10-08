/* Copyright (c) 2001-2011, The HSQL Development Group
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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Properties;

public class HSQLBug {

    public static void main(String[] args) throws Exception {

        Class.forName("org.hsqldb.jdbc.JDBCDriver");

        String     url        = "jdbc:hsqldb:mem:.";
        Properties properties = new Properties();

        properties.setProperty("user", "sa");
        properties.setProperty("password", "");

        Connection con = DriverManager.getConnection(url, properties);
        Statement  s   = con.createStatement();

        s.executeUpdate(
            "CREATE TABLE Table1 (Id INTEGER IDENTITY, Timec DATETIME NOT NULL)");
        s.executeUpdate(
            "CREATE TABLE Table2 (Id INTEGER NOT NULL, Value VARCHAR(100) NOT NULL)");
        s.executeUpdate("CREATE INDEX idx1 ON Table1(timec)");

        // Add test data to the tables
        GregorianCalendar gc = new GregorianCalendar();

        for (int i = 0; i < 1000; i++) {
            gc.add(Calendar.MINUTE, -1);

            String query = "INSERT INTO Table1 (Timec) VALUES ('"
                           + formatter.format(gc.getTime()) + "')";

            s.executeUpdate(query);

            ResultSet r = s.executeQuery("CALL IDENTITY();");

            r.next();

            int id = r.getInt(1);

            s.executeUpdate("INSERT INTO Table2 VALUES (" + id + ", 'ABC')");
            s.executeUpdate("INSERT INTO Table2 VALUES (" + id + ", 'DEF')");
        }

        // Build the query
        StringBuffer query = new StringBuffer();

        query.append("SELECT Timec, Value FROM Table1, Table2 ");
        query.append("WHERE Table1.Id = Table2.Id  ");
        query.append("AND Table1.Timec = ");    // The equals sign causes the CPU to go to 100% for a while

//                      query.append( "AND Table1.Time IN " );  // The work-around is to replace it with IN
        query.append("(");
        query.append(
            "SELECT MAX(Timec) FROM Table1 WHERE Timec <= '2020-01-01 00:00:00'");
        query.append(")");
        System.out.println("Query = " + query);
        System.out.println(
            "Starting Query at: "
            + formatter.format((new GregorianCalendar()).getTime()));

        ResultSet r = s.executeQuery(query.toString());

        r.next();
        System.out.println("Result    : " + r.getTimestamp(1) + "  "
                           + r.getString(2));
        System.out.println(
            "DONE Query at    : "
            + formatter.format((new GregorianCalendar()).getTime()));
    }

    private static SimpleDateFormat formatter =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
}
