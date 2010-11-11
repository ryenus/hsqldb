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
    private static final String[] roles34 = new String[] { "ROLE3", "ROLE4" };

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
        return null;
    }

    private static final Set<String> twoRolesSet =
            new HashSet<String>(Arrays.asList(twoRoles));
    private static final Set<String> roles34Set =
            new HashSet<String>(Arrays.asList(roles34));

    protected Connection saCon;
    protected Statement saSt;
    protected String jdbcUrl =
            "jdbc:hsqldb:mem:" + getClass().getName().replaceFirst(".+\\.", "");

    protected void setUp() throws SQLException {
        saCon = DriverManager.getConnection(jdbcUrl, "SA", "");
        saCon.setAutoCommit(false);
        saSt = saCon.createStatement();
        saSt.executeUpdate("CREATE ROLE role1");
        saSt.executeUpdate("CREATE ROLE role2");
        saSt.executeUpdate("CREATE ROLE role3");
        saSt.executeUpdate("CREATE ROLE role4");
        saSt.executeUpdate("CREATE TABLE t1 (i INTEGER)");
        saSt.executeUpdate("CREATE TABLE t2 (i INTEGER)");
        saSt.executeUpdate("CREATE TABLE t3 (i INTEGER)");
        saSt.executeUpdate("CREATE TABLE t4 (i INTEGER)");
        saSt.executeUpdate("GRANT ALL ON t1 TO role1");
        saSt.executeUpdate("GRANT ALL ON t2 TO role2");
        saSt.executeUpdate("GRANT ALL ON t3 TO role3");
        saSt.executeUpdate("GRANT ALL ON t4 TO role4");
    }

    protected void tearDown() {
        if (saSt != null) try {
            saSt.executeUpdate("DROP TABLE t1");
            saSt.executeUpdate("DROP TABLE t2");
            saSt.executeUpdate("DROP TABLE t3");
            saSt.executeUpdate("DROP TABLE t4");
            saSt.executeUpdate("DROP ROLE role1");
            saSt.executeUpdate("DROP ROLE role2");
            saSt.executeUpdate("DROP ROLE role3");
            saSt.executeUpdate("DROP ROLE role4");
            saSt.executeUpdate("SET DATABASE AUTHENTICATION FUNCTION NONE");
            saSt.executeUpdate("SHUTDOWN");
        } catch (SQLException se) {
            logger.error("Tear-down of setup Conn. failed:" + se);
        }
        if (saSt != null) try {
            saSt.close();
        } catch (SQLException se) {
            logger.error("Close of setup Statement failed:" + se);
        } finally {
            saSt = null;
        }
        if (saCon != null) try {
            saCon.close();
        } catch (SQLException se) {
            logger.error("Close of setup Conn. failed:" + se);
        } finally {
            saCon = null;
        }
    }

    public void testRemoteAccountRemoteRoles() throws SQLException {
        Statement st = null;
        Connection authedCon = null;
        try {
            saSt.executeUpdate(
                    "SET DATABASE AUTHENTICATION FUNCTION EXTERNAL NAME "
                    + "'CLASSPATH:" + getClass().getName() + ".twoRolesFn'");
            try {
                authedCon = DriverManager.getConnection(
                        jdbcUrl, "zeno", "a password");
            } catch (SQLException se) {
                fail("Access with 'twoRolesFn' failed");
            }
            st = authedCon.createStatement();
            assertFalse("Positive test failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO t1 VALUES(1)"));
            assertTrue("Negative test failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO t3 VALUES(3)"));
            assertEquals(
                    twoRolesSet, AuthFunctionUtils.getEnabledRoles(authedCon));
        } finally {
            if (st != null) try {
                st.close();
            } catch (SQLException se) {
                logger.error("Close of Statement failed:" + se);
            } finally {
                st = null;
            }
            if (authedCon != null) try {
                authedCon.rollback();
                authedCon.close();
            } catch (SQLException se) {
                logger.error("Close of Authed Conn. failed:" + se);
            } finally {
                authedCon = null;
            }
        }
    }

    public void testLocalUserAccountLocalRemoteRoles() throws SQLException {
        Statement st = null;
        Connection authedCon = null;
        try {
            saSt.executeUpdate("CREATE USER tlualrr PASSWORD 'wontuse'");
            saSt.executeUpdate("GRANT role3 TO tlualrr");
            saSt.executeUpdate("GRANT role4 TO tlualrr");
            saSt.executeUpdate(
                    "SET DATABASE AUTHENTICATION FUNCTION EXTERNAL NAME "
                    + "'CLASSPATH:" + getClass().getName() + ".twoRolesFn'");
            try {
                authedCon = DriverManager.getConnection(
                        jdbcUrl, "tlualrr", "unusedPassword");
            } catch (SQLException se) {
                fail("Access with 'twoRolesFn' failed");
            }
            st = authedCon.createStatement();
            assertFalse("Positive test #1 failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO t1 VALUES(1)"));
            assertFalse("Positive test #2 failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO t2 VALUES(2)"));
            assertTrue("Negative test #3 failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO t3 VALUES(3)"));
            assertTrue("Negative test #4 failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO t4 VALUES(4)"));
            assertEquals(
                    twoRolesSet, AuthFunctionUtils.getEnabledRoles(authedCon));
        } finally {
            if (st != null) try {
                st.close();
            } catch (SQLException se) {
                logger.error("Close of Statement failed:" + se);
            } finally {
                st = null;
            }
            if (authedCon != null) try {
                authedCon.rollback();
                authedCon.close();
            } catch (SQLException se) {
                logger.error("Close of Authed Conn. failed:" + se);
            } finally {
                authedCon = null;
            }
        }
    }

    public void testLocalUserAccountLocalRoles() throws SQLException {
        Statement st = null;
        Connection authedCon = null;
        try {
            saSt.executeUpdate("CREATE USER tlualr PASSWORD 'wontuse'");
            saSt.executeUpdate("GRANT role3 TO tlualr");
            saSt.executeUpdate("GRANT role4 TO tlualr");
            saSt.executeUpdate(
                    "SET DATABASE AUTHENTICATION FUNCTION EXTERNAL NAME "
                    + "'CLASSPATH:" + getClass().getName() + ".nullFn'");
            try {
                authedCon = DriverManager.getConnection(
                        jdbcUrl, "tlualr", "unusedPassword");
            } catch (SQLException se) {
                fail("Access with 'nullFn' failed");
            }
            st = authedCon.createStatement();
            assertTrue("Negative test #1 failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO t1 VALUES(1)"));
            assertTrue("Negative test #2 failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO t2 VALUES(2)"));
            assertFalse("Positive test #3 failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO t3 VALUES(3)"));
            assertFalse("Positive test #4 failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO t4 VALUES(4)"));
            assertEquals(
                    roles34Set, AuthFunctionUtils.getEnabledRoles(authedCon));
        } finally {
            if (st != null) try {
                st.close();
            } catch (SQLException se) {
                logger.error("Close of Statement failed:" + se);
            } finally {
                st = null;
            }
            if (authedCon != null) try {
                authedCon.rollback();
                authedCon.close();
            } catch (SQLException se) {
                logger.error("Close of Authed Conn. failed:" + se);
            } finally {
                authedCon = null;
            }
        }
    }

    public void testLocalUserAccountLocalRemote0Roles() throws SQLException {
        Statement st = null;
        Connection authedCon = null;
        try {
            saSt.executeUpdate("CREATE USER tlualr0r PASSWORD 'wontuse'");
            saSt.executeUpdate("GRANT role3 TO tlualr0r");
            saSt.executeUpdate("GRANT role4 TO tlualr0r");
            saSt.executeUpdate(
                    "SET DATABASE AUTHENTICATION FUNCTION EXTERNAL NAME "
                    + "'CLASSPATH:" + getClass().getName() + ".noRoleFn'");
            try {
                authedCon = DriverManager.getConnection(
                        jdbcUrl, "tlualr0r", "unusedPassword");
            } catch (SQLException se) {
                fail("Access with 'nullFn' failed");
            }
            st = authedCon.createStatement();
            assertTrue("Negative test #1 failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO t1 VALUES(1)"));
            assertTrue("Negative test #2 failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO t2 VALUES(2)"));
            assertTrue("Negative test #3 failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO t3 VALUES(3)"));
            assertTrue("Negative test #4 failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO t4 VALUES(4)"));
            assertEquals(
                    0, AuthFunctionUtils.getEnabledRoles(authedCon).size());
        } finally {
            if (st != null) try {
                st.close();
            } catch (SQLException se) {
                logger.error("Close of Statement failed:" + se);
            } finally {
                st = null;
            }
            if (authedCon != null) try {
                authedCon.rollback();
                authedCon.close();
            } catch (SQLException se) {
                logger.error("Close of Authed Conn. failed:" + se);
            } finally {
                authedCon = null;
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
