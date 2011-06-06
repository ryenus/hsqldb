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

import java.io.File;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * <b>Description</b> : HsqldbTestCase
 *
 * <p><b>Society</b> : dataXpresso
 * @since 8 august 2006
 * @author Julien Blaize
 */
public class HsqldbTestCase {

    /**
     * Deletes all files and subdirectories under dir.
     * Returns true if all deletions were successful.
     * If a deletion fails, the method stops attempting to delete and returns false.
     */
    public static boolean deleteDir(File dir) {

        if (dir.isDirectory()) {
            String[] children = dir.list();

            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));

                if (!success) {
                    return false;
                }
            }
        }

        // The directory is now empty so delete it
        return dir.delete();
    }

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put("user", "sa");
        props.put("password", "");
        props.put("hsqldb.default_table_type", "cached");

        //props.put("hsqldb.cache_size_scale", "12");
        props.put("hsqldb.cache_scale", "8");

        //props.put("hsqldb.cache_file_scale", "8");
        props.put("hsqldb.applog", "0");
        props.put("hsqldb.log_size", "200");
        props.put("hsqldb.result_memory_rows", "10");
        props.put("shutdown", "true");

        String url = "jdbc:hsqldb:";

        url += "/hsql/statBase/test";

        //delete earlier files
        HsqldbTestCase.deleteDir(new File("/hsql/statBase/"));

        try {
            Class  clsDriver = Class.forName("org.hsqldb.jdbc.JDBCDriver");
            Driver driver    = (Driver) clsDriver.newInstance();

            DriverManager.registerDriver(driver);

            Connection con = DriverManager.getConnection(url, props);
            String createQuery =
                "drop table test1 if exists;create table test1 (rowNum identity, col1 varchar(50), col2 int, col3 varchar(50))";
            Statement st = con.createStatement();

            st.execute(createQuery);
            st.close();

            //we try to insert values in batch
            String insertQuery =
                "insert into test1 (col1,col2,col3) values (?,?,?)";
            PreparedStatement pst = con.prepareStatement(insertQuery);

            //we insert 1001
            for (int i = 0; i < 1001; i++) {
                pst.setString(1, "string_" + i);
                pst.setInt(2, i);
                pst.setString(3, "string2_" + i);
                pst.addBatch();

                if ((i > 0) && (i % 100 == 0)) {
                    pst.executeBatch();
                }
            }

            pst.close();

            //we try to do a select on this statement and to scroll it with the absolute method
            String selectQuery = "select * from test1";

            st = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                     ResultSet.CONCUR_READ_ONLY);

            ResultSet scrollableSet = st.executeQuery(selectQuery);

            scrollableSet.setFetchSize(100);
            scrollableSet.next();

            int tmpIndex = scrollableSet.getInt(3);

            if (tmpIndex != 0) {
                System.out.println("index at 0 is !=0");
            }

            //we go from 0 to 1000 with absolute and a gap of 100
            for (int i = 0; i <= 1000; i += 100) {
                scrollableSet.absolute(i + 1);

                tmpIndex = scrollableSet.getInt(3);

                System.out.println(tmpIndex);
            }

            //we go from 1000 to 0 with absolute and a gap of 100
            for (int i = 1000; i > 0; i -= 100) {
                scrollableSet.relative(-100);

                tmpIndex = scrollableSet.getInt(3);

                System.out.println(tmpIndex);
            }
        } catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
    }
}
