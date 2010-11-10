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


package org.hsqldb.auth;

import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.DriverManager;
import org.hsqldb.jdbc.JDBCArrayBasic;
import org.hsqldb.types.Type;
import org.hsqldb.lib.FrameworkLogger;

/**
 * Test method naming convention:
 * <UL>
 *   <LI>The substring "LocalUser" means that an account exists in the local
 *       database before the authentication is attempted.
 *   <LI>The substring "RemoteUser" means that no account exists in the local
 *       database before the authentication is attempted.
 *   <LI>The substrings "LocalRoles", "RemoteRoles", "LocalRemoteRoles"
 *       indicate whether a pre-existing local account has roles, whether
 *       the auth. function provides roles, or both.
 * </UL>
 */
public class AuthFunctionTest extends junit.framework.TestCase {
    private static FrameworkLogger logger =
            FrameworkLogger.getLog(AuthFunctionTest.class);

    private static final String[] twoRoles = new String[] { "ROLE1", "ROLE2" };

    public static Array noRoleFn(
            String database, String user, String password) {
        return new JDBCArrayBasic(new String[0], Type.SQL_VARCHAR);
    }

    public static Array twoRolesFn(
            String database, String user, String password) {
        return new JDBCArrayBasic(twoRoles, Type.SQL_VARCHAR);
    }

    public static Array nullFn(
            String database, String user, String password) {
        return new JDBCArrayBasic(twoRoles, Type.SQL_VARCHAR);
    }

    private static final Set<String> twoRolesSet =
            new HashSet<String>(Arrays.asList(twoRoles));

    public void testRemoteAccountRemoteRoles() throws SQLException {
        String jdbcUrl = "jdbc:hsqldb:mem:mem01";
        Statement st = null;
        Connection con = DriverManager.getConnection(jdbcUrl, "SA", "");
        Connection authedCon = null;
        try {
            st = con.createStatement();
            st.executeUpdate("CREATE ROLE role1");
            st.executeUpdate("CREATE ROLE role2");
            st.executeUpdate("CREATE ROLE role3");
            st.executeUpdate("CREATE TABLE t1 (i INTEGER)");
            st.executeUpdate("CREATE TABLE t3 (i INTEGER)");
            st.executeUpdate("GRANT ALL ON t1 TO role1");
            st.executeUpdate("GRANT ALL ON t3 TO role3");
            st.executeUpdate(
                    "SET DATABASE AUTHENTICATION FUNCTION EXTERNAL NAME "
                    + "'CLASSPATH:" + getClass().getName() + ".twoRolesFn'");
            con.commit();
            con.close();
            con = null;
            st.close();
            try {
                authedCon = DriverManager.getConnection(
                        jdbcUrl, "zeno", "a password");
            } catch (SQLException se) {
                fail("Access with 'twoRolesFn' failed");
            }
            st = authedCon.createStatement();
            try {
                st.executeUpdate("INSERT INTO t1 VALUES(1)");
            } catch (SQLException se) {
                fail("Positive test failed: " + se);
            }
            try {
                st.executeUpdate("INSERT INTO t3 VALUES(3)");
                fail("Negative test failed");
            } catch (SQLException se) {
                // Intentionally empty.  Expected.
            }
            assertEquals(
                    twoRolesSet, AuthFunctionUtils.getEnabledRoles(authedCon));
        } finally {
            if (authedCon != null) try {
                authedCon.rollback();
                authedCon.close();
            } catch (SQLException se) {
                System.err.println("Close of Authed Conn. failed:" + se);
            } finally {
                authedCon = null;
            }
            if (st != null) try {
                st.executeUpdate("SHUTDOWN");
                st.close();
            } catch (SQLException se) {
                System.err.println("Close of Statement failed:" + se);
            } finally {
                st = null;
            }
            if (con != null) try {
                con.close();
            } catch (SQLException se) {
                System.err.println("Close of setup Conn. failed:" + se);
            } finally {
                con = null;
            }
        }
    }

    /**
     * This method allows to easily run this unit test independent of the other
     * unit tests, and without dealing with Ant or unrelated test suites.
     */
    static public void main(String[] sa) {
        if (sa.length > 0 && sa[0].startsWith("-g")) {
            junit.swingui.TestRunner.run(AuthFunctionTest.class);
        } else {
            junit.textui.TestRunner runner = new junit.textui.TestRunner();
            junit.framework.TestResult result =
                runner.run(runner.getTest(AuthFunctionTest.class.getName()));

            System.exit(result.wasSuccessful() ? 0 : 1);
        }
    }
}
