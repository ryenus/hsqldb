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


package org.hsqldb.sample;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.ApplicationContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * As you can tell by the class name, this class is purposefully Spring-aware,
 * as it initiates the Spring Spring context load.
 * In a web application, a lifecycle lister or other
 * mechanism would eliminate the need for any custom Java code to load the
 * context (like what we have here).
 */
public class SpringExtAuth {
    private static Log log = LogFactory.getLog(SpringExtAuth.class);

    private static final String SYNTAX_MSG = "SYNTAX: "
            + SpringExtAuth.class.getName()
            + " {LDAP|HsqldbSlave|JAAS|JAAS_LDAP}";

    /**
     * @throws SQLException If Setup of emulation database failed, or if the
     *         application JDBC work fails.
     */
    static public void main(String[] sa) throws SQLException {
        if (sa.length != 1) throw new IllegalArgumentException(SYNTAX_MSG);
        String authSpringFile = null;
        if (sa[0].equals("LDAP")) {
            authSpringFile = "ldapbeans.xml";
        } else if (sa[0].equals("JAAS")) {
            authSpringFile = "jaasbeans.xml";
        } else if (sa[0].equals("HsqldbSlave")) {
            authSpringFile = "slavebeans.xml";
        } else if (sa[0].equals("JAAS_LDAP")) {
            authSpringFile = "jaasldapbeans.xml";
        }
        if (authSpringFile == null)
            throw new IllegalArgumentException(SYNTAX_MSG);

        SpringExtAuth.prepMemoryDatabases(!sa[0].equals("HsqldbSlave"));
        ApplicationContext ctx =
            new ClassPathXmlApplicationContext("beandefs.xml", authSpringFile);
        ListableBeanFactory bf = (ListableBeanFactory) ctx;
        JdbcAppClass appBean = bf.getBean("appBean", JdbcAppClass.class);
        appBean.doJdbcWork();
    }

    /**
     * This method prepares a memory-only catalog.
     * After this method runs, a new Connection using the same JDBC URL will
     * behave just like connecting to a populated, persistent catalog.
     *
     * Purposefully not using declarative settings here because this is purely
     * emulation setup.
     * A real application won't have any method corresponding to this method.
     *
     * @throws SQLException if setup failed
     */
    private static void prepMemoryDatabases(boolean doLdap)
            throws SQLException {
        Connection c = null;
        Statement st = null;
        try {
            c = DriverManager.getConnection(
                    "jdbc:hsqldb:mem:localDb", "SA", "");
                    // JDBC URL here must match that configured within the bean
                    // 'appBean' in "beandefs.xml" file
            c.setAutoCommit(false);
            st = c.createStatement();
            st.executeUpdate("SET DATABASE UNIQUE NAME \"AUTHSAMPLEDBNAME\"");
            st.executeUpdate(
                    "SET DATABASE AUTHENTICATION FUNCTION EXTERNAL NAME "
                    + "'CLASSPATH:"
                    + "org.hsqldb.auth.AuthBeanMultiplexer.authenticate'");
            // DB Name here must match that configured in either
            // "ldapbeans.xml" or "slavebean.xml", depending on whether you are
            // running in LDAP or HsqldbSlave mode, correspondingly.
            st.executeUpdate("SET PASSWORD 'SECRET5222173'");
            st.executeUpdate("CREATE TABLE t1(i INTEGER)");
            st.executeUpdate("GRANT SELECT ON t1 TO public");
            st.executeUpdate("INSERT INTO t1 VALUES(456)");
            // Table name and value must match what is expected by method
            // JdbcAppClass.doJdbcWork.
            c.commit();
        } finally {
            if (st != null) try {
                st.close();
            } catch (SQLException se) {
                log.error("Failed to close emulation database setup Connection",
                        se);
            } finally {
                st = null;  // Encourage GC
            }
            if (c != null) try {
                c.close();
            } catch (SQLException se) {
                log.error("Failed to close emulation database setup Connection",
                        se);
            } finally {
                c = null;  // Encourage GC
            }
        }
        if (doLdap) return;

        // Create an authentication master database
        try {
            c = DriverManager.getConnection(
                    "jdbc:hsqldb:mem:masterDb", "SA", "");
                    // JDBC URL here must match that configured for bean
                    // 'slaveSetup' in "slavebeans.xml" file
            c.setAutoCommit(false);
            st = c.createStatement();
            st.executeUpdate("SET PASSWORD 'SECRET9123113'");
            // This password will never be used again.
            // Changing it from the default just for good security practice.
            st.executeUpdate("CREATE USER \"straight\" PASSWORD 'pwd'");
            // User name and password here must match those configured in file
            // "beandefs.xml".
            c.commit();
        } finally {
            if (st != null) try {
                st.close();
            } catch (SQLException se) {
                log.error("Failed to close emulation database setup Connection",
                        se);
            } finally {
                st = null;  // Encourage GC
            }
            if (c != null) try {
                c.close();
            } catch (SQLException se) {
                log.error("Failed to close emulation database setup Connection",
                        se);
            } finally {
                c = null;  // Encourage GC
            }
        }
    }
}
