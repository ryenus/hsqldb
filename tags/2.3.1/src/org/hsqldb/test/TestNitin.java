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

/**
 * DB Out of memory test
 * cached tables in non-nio mode
 * @author Nitin Chauhan
 */
public class TestNitin {

    public static void main(String[] args) {

        java.sql.Connection    c  = null;
        java.sql.Statement     s  = null;
        java.io.BufferedReader br = null;

        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");

            c = java.sql.DriverManager.getConnection(
                "jdbc:hsqldb:c:/ft/hsqldb_w_1_8_0/oom/my.db", "SA", "");
            s = c.createStatement();
            br = new java.io.BufferedReader(
                new java.io.FileReader("c:/ft/hsqldb_w_1_8_0//oom//my.sql"));

            String line;
            int    lineNo = 0;

            while ((line = br.readLine()) != null) {
                if (line.length() > 0 && line.charAt(0) != '#') {
                    s.execute(line);

                    if (lineNo++ % 100 == 0) {
                        System.out.println(lineNo);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (java.io.IOException ioe) {}

            try {
                if (s != null) {
                    s.close();
                }
            } catch (java.sql.SQLException se) {}

            try {
                if (c != null) {
                    c.close();
                }
            } catch (java.sql.SQLException se) {}
        }

        System.exit(0);
    }
}
