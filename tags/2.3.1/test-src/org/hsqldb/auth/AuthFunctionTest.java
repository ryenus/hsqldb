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


package org.hsqldb.auth;

import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.DriverManager;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;
import org.hsqldb.jdbc.JDBCArrayBasic;
import org.hsqldb.types.Type;
import org.hsqldb.lib.FrameworkLogger;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

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
public class AuthFunctionTest extends BaseTestCase {
    private static FrameworkLogger logger =
            FrameworkLogger.getLog(AuthFunctionTest.class);

    private static final String[] twoRoles = new String[] { "ROLE1", "ROLE2" };
    private static final String[] roles34 = new String[] { "ROLE3", "ROLE4" };

    public static Array schemaS2Fn(
            String database, String user, String password) {
        return new JDBCArrayBasic(new String[] { "S2" }, Type.SQL_VARCHAR);
    }

    public static Array twoRolesFn(
            String database, String user, String password) {
        return new JDBCArrayBasic(twoRoles, Type.SQL_VARCHAR);
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
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
        } catch (Exception e) {}
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

    @ForSubject(AuthFunctionTest.class)
    @OfMethod("twoRolesFn(java.lang.s]String,java.lang.String,java.lang.String)")
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
            assertEquals(twoRolesSet, AuthUtils.getEnabledRoles(authedCon));
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

    @ForSubject(AuthFunctionTest.class)
    @OfMethod("twoRolesFn(java.lang.s]String,java.lang.String,java.lang.String)")
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
                        jdbcUrl, "TLUALRR", "unusedPassword");
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
            assertEquals(twoRolesSet, AuthUtils.getEnabledRoles(authedCon));
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

    @ForSubject(AuthFunctionUtils.class)
    @OfMethod("nullFn(java.lang.String,java.lang.String,java.lang.String)")
    public void testLocalUserAccountLocalRoles() throws SQLException {
        Statement st = null;
        Connection authedCon = null;
        try {
            saSt.executeUpdate("CREATE USER tlualr PASSWORD 'wontuse'");
            saSt.executeUpdate("GRANT role3 TO tlualr");
            saSt.executeUpdate("GRANT role4 TO tlualr");
            saSt.executeUpdate(
                    "SET DATABASE AUTHENTICATION FUNCTION EXTERNAL NAME "
                    + "'CLASSPATH:"
                    + AuthFunctionUtils.class.getName() + ".nullFn'");
            try {
                authedCon = DriverManager.getConnection(
                        jdbcUrl, "TLUALR", "unusedPassword");
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
            assertEquals(roles34Set, AuthUtils.getEnabledRoles(authedCon));
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

    @ForSubject(AuthFunctionUtils.class)
    @OfMethod("noRoleFn(java.lang.String,java.lang.String,java.lang.String)")
    public void testLocalUserAccountLocalRemote0Roles() throws SQLException {
        Statement st = null;
        Connection authedCon = null;
        try {
            saSt.executeUpdate("CREATE USER tlualr0r PASSWORD 'wontuse'");
            saSt.executeUpdate("GRANT role3 TO tlualr0r");
            saSt.executeUpdate("GRANT role4 TO tlualr0r");
            saSt.executeUpdate(
                    "SET DATABASE AUTHENTICATION FUNCTION EXTERNAL NAME "
                    + "'CLASSPATH:"
                    + AuthFunctionUtils.class.getName() + ".noRoleFn'");
            try {
                authedCon = DriverManager.getConnection(
                        jdbcUrl, "TLUALR0R", "unusedPassword");
            } catch (SQLException se) {
                fail("Access with 'noRoleFn' failed");
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
            assertEquals(0, AuthUtils.getEnabledRoles(authedCon).size());
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

    @ForSubject(AuthFunctionUtils.class)
    @OfMethod("nullFn(java.lang.String,java.lang.String,java.lang.String)")
    public void testVirtualAccount() throws SQLException {
        Statement st = null;
        Connection authedCon = null;
        try {
            saSt.executeUpdate(
                    "SET DATABASE AUTHENTICATION FUNCTION EXTERNAL NAME "
                    + "'CLASSPATH:"
                    + AuthFunctionUtils.class.getName() + ".nullFn'");
            saSt.executeUpdate("GRANT ALL ON t1 TO public");
            try {
                authedCon = DriverManager.getConnection(
                        jdbcUrl, "VIRTUALUSER", "unusedPassword");
            } catch (SQLException se) {
                fail("Failed to grant access to virtual user");
            }
            st = authedCon.createStatement();
            assertFalse("Virtual user failed to write public-write table",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO t1 VALUES(2)"));
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

    @ForSubject(AuthFunctionUtils.class)
    @OfMethod("nullFn(java.lang.String,java.lang.String,java.lang.String)")
    public void testLocalOnlyAccess() throws SQLException {
        Statement st = null;
        Connection authedCon = null;
        Connection authed2Con = null;
        Connection extraCon = null;
        try {
            saSt.executeUpdate("CREATE USER tloa PASSWORD 'localPassword'");
            saSt.executeUpdate(
                    "SET DATABASE AUTHENTICATION FUNCTION EXTERNAL NAME "
                    + "'CLASSPATH:"
                    + AuthFunctionUtils.class.getName() + ".nullFn'");
            try {
                authedCon = DriverManager.getConnection(
                        jdbcUrl, "TLOA", "unusedPassword");
            } catch (SQLException se) {
                fail("Pre-test with normal access-only account failed");
            }
            try {
                extraCon = DriverManager.getConnection(jdbcUrl, "SA", "wrong");
                fail("Permitted access to SA with wrong password");
            } catch (SQLException se) {
                // Intentionally empty.  Expect this.
            }
            try {
                extraCon = DriverManager.getConnection(jdbcUrl, "SA", "");
            } catch (SQLException se) {
                fail("Pre-test with SA account failed");
            }
            saSt.executeUpdate("ALTER USER tloa SET LOCAL true");
            try {
                authed2Con = DriverManager.getConnection(
                        jdbcUrl, "TLOA", "wrongPassword");
                fail("Permitted access to local-only user with wrong password");
            } catch (SQLException se) {
                // Intentionally empty.  Expect this.
            }
            try {
                authedCon = DriverManager.getConnection(
                        jdbcUrl, "TLOA", "localPassword");
            } catch (SQLException se) {
                fail("Access to local-only account with local password failed");
            }
        } finally {
            if (extraCon != null) try {
                extraCon.rollback();
                extraCon.close();
            } catch (SQLException se) {
                logger.error("Close of Extra Conn. failed:" + se);
            } finally {
                extraCon = null;
            }
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
            if (authed2Con != null) try {
                authed2Con.rollback();
                authed2Con.close();
            } catch (SQLException se) {
                logger.error("Close of Authed Conn. #2 failed:" + se);
            } finally {
                authed2Con = null;
            }
        }
    }

    @ForSubject(AuthFunctionTest.class)
    @OfMethod("schemaS2Fn(java.lang.String,java.lang.String,java.lang.String)")
    public void testPullSchema() throws SQLException {
        Statement st = null;
        Connection authedCon = null;
        try {
            saSt.executeUpdate("CREATE USER tps PASSWORD 'wontuse'");
            saSt.executeUpdate("CREATE SCHEMA s1");
            saSt.executeUpdate("CREATE SCHEMA s2");
            saSt.executeUpdate("ALTER USER tps SET INITIAL SCHEMA s1");
            saSt.executeUpdate("CREATE TABLE s1.s1t1 (i INTEGER)");
            saSt.executeUpdate("GRANT ALL ON s1.s1t1 TO public");
            saSt.executeUpdate("CREATE TABLE s2.s2t1 (i INTEGER)");
            saSt.executeUpdate("GRANT ALL ON s2.s2t1 TO public");
            saSt.executeUpdate(
                    "SET DATABASE AUTHENTICATION FUNCTION EXTERNAL NAME "
                    + "'CLASSPATH:" + getClass().getName() + ".schemaS2Fn'");
            try {
                authedCon = DriverManager.getConnection(
                        jdbcUrl, "TPS", "unusedPassword");
            } catch (SQLException se) {
                fail("Access with 'schemaS2Fn' failed");
            }
            st = authedCon.createStatement();
            assertTrue("Negative test #1 failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO s1t1 VALUES(1)"));
            assertFalse("Positive test #2 failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO s2t1 VALUES(2)"));
            assertEquals(0, AuthUtils.getEnabledRoles(authedCon).size());
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

    @ForSubject(AuthFunctionTest.class)
    @OfMethod("twoRolesFn(java.lang.String,java.lang.String,java.lang.String)")
    public void testNoPullSchema() throws SQLException {
        Statement st = null;
        Connection authedCon = null;
        try {
            saSt.executeUpdate("CREATE USER tnps PASSWORD 'wontuse'");
            saSt.executeUpdate("CREATE SCHEMA s1");
            saSt.executeUpdate("CREATE SCHEMA s2");
            saSt.executeUpdate("ALTER USER tnps SET INITIAL SCHEMA s1");
            saSt.executeUpdate("CREATE TABLE s1.s1t1 (i INTEGER)");
            saSt.executeUpdate("GRANT ALL ON s1.s1t1 TO public");
            saSt.executeUpdate("CREATE TABLE s2.s2t1 (i INTEGER)");
            saSt.executeUpdate("GRANT ALL ON s2.s2t1 TO public");
            saSt.executeUpdate(
                    "SET DATABASE AUTHENTICATION FUNCTION EXTERNAL NAME "
                    + "'CLASSPATH:" + getClass().getName() + ".twoRolesFn'");
            // Since the auth. function returns only role names and no schema
            // role, the user's local initial schema should not be modified.
            try {
                authedCon = DriverManager.getConnection(
                        jdbcUrl, "TNPS", "unusedPassword");
            } catch (SQLException se) {
                fail("Access with 'twoRolesFn' failed");
            }
            st = authedCon.createStatement();
            assertFalse("Positive test #1 failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO s1t1 VALUES(1)"));
            assertTrue("Negative test #2 failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO s2t1 VALUES(2)"));
            assertEquals(twoRolesSet, AuthUtils.getEnabledRoles(authedCon));
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

    public static Test suite() {
        return new TestSuite(AuthFunctionTest.class);
    }

    /**
     * This method allows to easily run this unit test independent of the other
     * unit tests, and without dealing with Ant or unrelated test suites.
     */
    static public void main(String[] sa) {
        if (sa.length > 0 && sa[0].startsWith("-g")) {
            junit.swingui.TestRunner.run(AuthFunctionTest.class);
        } else {
            junit.framework.TestResult result = TestRunner.run(suite());
            
            System.exit(result.wasSuccessful() ? 0 : 1);
        }

    }
}
