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
import java.sql.SQLException;
import java.sql.Statement;

/*
 * TestHSQLDB.java
 *
 * Created on June 10, 2004, 10:28 PM
 */

/**
 *
 * @author  Diego Ballve
 */
public class TestHSQLDB {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        java.sql.DatabaseMetaData metaData = null;
        String databaseURL                 = "jdbc:hsqldb:mem:test";
        String                    driver   = "org.hsqldb.jdbc.JDBCDriver";
        String                    user     = "sa";
        String                    password = "";

        //Table creation sql:
        String ddlStr =
            "CREATE TABLE USER_(ID VARCHAR(64) NOT NULL PRIMARY KEY,HOME VARCHAR(128),OBJECTTYPE VARCHAR(64),STATUS VARCHAR(64) NOT NULL,PERSONNAME_FIRSTNAME VARCHAR(64),PERSONNAME_MIDDLENAME VARCHAR(64),PERSONNAME_LASTNAME VARCHAR(64),URL VARCHAR(256))";
        String sqlStr =
            "UPDATE User_ SET  id=\'urn:uuid:921284f0-bbed-4a4c-9342-ecaf0625f9d7\',  home=null, objectType=\'urn:uuid:6d07b299-10e7-408f-843d-bb2bc913bfbb\', status=\'urn:uuid:37d17f1b-3245-425b-988d-e0d98200a146\' , personName_firstName=\'Registry\', personName_middleName=null, personName_lastName=\'Operator\', url=\'http://sourceforge.net/projects/ebxmlrr\' WHERE id = \'urn:uuid:921284f0-bbed-4a4c-9342-ecaf0625f9d7\' ";
        Statement stmt = null;

        try {
            Class.forName(driver);

            Connection connection = DriverManager.getConnection(databaseURL,
                user, password);

            stmt = connection.createStatement();

            stmt.addBatch(ddlStr);
            stmt.addBatch(sqlStr);

            int[] updateCounts = stmt.executeBatch();
        } catch (ClassNotFoundException e) {
            System.err.println(e.getClass().getName() + ": "
                               + e.getMessage());
            e.printStackTrace(System.err);
        } catch (SQLException e) {
            System.err.println(e.getClass().getName() + ": "
                               + e.getMessage());
            e.printStackTrace(System.err);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {}
        }
    }
}
