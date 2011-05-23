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

import org.hsqldb.lib.StopWatch;

public class TestAnother {

    // fixed
    protected String url = "jdbc:hsqldb:";

//    protected String  filepath = "hsql://localhost/mytest";
//    protected String filepath = "mem:test";
    protected String filepath = "/hsql/testtime/test";

    public TestAnother() {}

    public void setUp() {

        String user     = "sa";
        String password = "";

        try {
            Connection conn = null;

            Class.forName("org.hsqldb.jdbc.JDBCDriver");

            conn = DriverManager.getConnection(url + filepath, user,
                                               password);

            Statement stmnt = conn.createStatement();
            Statement st    = conn.createStatement();

            st.executeUpdate("CREATE TABLE TT(D DATE)");
            st.executeUpdate("INSERT INTO TT VALUES ('2004-01-02')");
            st.executeUpdate("INSERT INTO TT VALUES ('2004-02-02')");

            ResultSet rs = st.executeQuery("SELECT * FROM TT");

            while (rs.next()) {
                System.out.println(rs.getDate(1));
            }

            st.executeUpdate("DROP TABLE TT");
            rs.close();

            Statement stm = conn.createStatement();

            stm.executeUpdate(
                "create table test (id int,atime timestamp default current_timestamp)");

            stm = conn.createStatement();

            int count = stm.executeUpdate("insert into test (id) values (1)");

            System.out.println(count);
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("TestSql.setUp() error: " + e.getMessage());
        }
    }

    public static void main(String[] argv) {

        StopWatch   sw   = new StopWatch();
        TestAnother test = new TestAnother();

        test.setUp();
        System.out.println("Total Test Time: " + sw.elapsedTime());
    }
}
