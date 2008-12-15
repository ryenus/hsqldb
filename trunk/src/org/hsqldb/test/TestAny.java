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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;

public class TestAny extends TestBase {

    public TestAny(String name) {
        super(name);
    }

    public void testScript() {

        try {
            TestUtil.testScript(newConnection(), "TestSelfCreate.txt");
        } catch (Exception e) {}
    }

    public void testQuery() {

        try {
            Connection c  = newConnection();
            Statement  st = c.createStatement();
            String     s  = "CREATE TABLE T (I INTEGER, C CHARACTER(10))";

            st.execute(s);

            s = "INSERT INTO T VALUES(0,'TEST')";

            st.execute(s);

            s = "INSERT INTO T SELECT ?, C FROM T";

            PreparedStatement ps = c.prepareStatement(s);

            ps.setInt(1, 10);
            ps.execute();

            s = "SELECT * FROM T";

            ResultSet rs = st.executeQuery(s);

            while (rs.next()) {
                System.out.println("" + rs.getInt(1) + "      "
                                   + rs.getString(2));
            }

            s  = "SELECT I, CAST(? AS VARCHAR), * FROM T";
            ps = c.prepareStatement(s);

            ps.setString(1, "param");

            rs = ps.executeQuery();

            while (rs.next()) {
                System.out.println("" + rs.getInt(1) + "      "
                                   + rs.getString(2));
            }

            s  = "SELECT I, ?, * FROM T";
            ps = c.prepareStatement(s);

            ps.setString(1, "param");

            rs = ps.executeQuery();

            while (rs.next()) {
                System.out.println("" + rs.getInt(1) + "      "
                                   + rs.getString(2));
            }

            ps.close();

            //
            st.execute(
                "CREATE CACHED TABLE managerproperties("
                + "id BIGINT IDENTITY, name VARCHAR(20),"
                + "key VARCHAR(50) NOT NULL, value VARCHAR(100) NOT NULL, "
                + "UNIQUE (name, key))");

            ps = c.prepareStatement("SELECT mp.key," + " mp.value"
                                    + " FROM managerproperties mp"
                                    + " WHERE mp.name = ?"
                                    + " OR (mp.name IS NULL"
//                                    + " -- ignore duplicates "
                                    + " AND mp.key NOT IN (SELECT mmp.key"
                                    + " FROM managerproperties mmp"
                                    + " WHERE mmp.name = ?))");

            ps.setObject(1, "NAME", Types.VARCHAR);
            ps.setObject(2, "NAME", Types.VARCHAR);
        } catch (Exception e) {
            System.out.print(e);
        }
    }
}
