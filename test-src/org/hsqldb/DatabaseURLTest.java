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
package org.hsqldb;

import org.hsqldb.persist.HsqlProperties;

public class DatabaseURLTest extends junit.framework.TestCase {

    public void testHsqldbUrls() {

        HsqlProperties props;

        DatabaseURL.parseURL(
            "JDBC:hsqldb:hsql://myhost:1777/mydb;filepath=c:/myfile/database/db",
            true, false);
        DatabaseURL.parseURL("JDBC:hsqldb:../data/mydb.db", true, false);
        DatabaseURL.parseURL("JDBC:hsqldb:../data/mydb.db;ifexists=true",
                             true, false);
        DatabaseURL.parseURL("JDBC:hsqldb:HSQL://localhost:9000/mydb", true,
                             false);
        DatabaseURL.parseURL(
            "JDBC:hsqldb:Http://localhost:8080/servlet/org.hsqldb.Servlet/mydb;ifexists=true",
            true, false);
        DatabaseURL.parseURL(
            "JDBC:hsqldb:Http://localhost/servlet/org.hsqldb.Servlet/", true,
            false);
        DatabaseURL.parseURL("JDBC:hsqldb:hsql://myhost", true, false);

        props = DatabaseURL.parseURL(
            "jdbc:hsqldb:res://com.anorg.APath;hsqldb.crypt_provider=org.crypt.Provider",
            true, false);

        assertEquals(props.getProperty("hsqldb.crypt_provider"),
                     "org.crypt.Provider");
        assertEquals(props.getProperty("database"), "//com.anorg.APath");
    }

    /**
     * This method allows to easily run this unit test independent of the other
     * unit tests, and without dealing with Ant or unrelated test suites.
     */
    static public void main(String[] sa) {

        if (sa.length > 0 && sa[0].startsWith("-g")) {
            junit.swingui.TestRunner.run(DatabaseURLTest.class);
        } else {
            junit.textui.TestRunner runner = new junit.textui.TestRunner();
            junit.framework.TestResult result =
                runner.run(runner.getTest(DatabaseURLTest.class.getName()));

            System.exit(result.wasSuccessful() ? 0
                                               : 1);
        }
    }
}
