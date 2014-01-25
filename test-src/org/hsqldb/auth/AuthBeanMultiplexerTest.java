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
import java.sql.Array;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.sql.Connection;
import java.sql.DriverManager;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;
import org.hsqldb.lib.FrameworkLogger;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.lib.StringUtil;

@ForSubject(AuthBeanMultiplexer.class)
public class AuthBeanMultiplexerTest extends BaseTestCase {
    private static FrameworkLogger logger =
            FrameworkLogger.getLog(AuthBeanMultiplexerTest.class);

    static {
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
        } catch (Exception e) {}
    }

    private static final String[] twoRoles = new String[] { "ROLE1", "ROLE2" };
    private static final Set<String> twoRolesSet =
            new HashSet<String>(Arrays.asList(twoRoles));
    private static final AuthFunctionBean nullPermittingAuthFunctionBean =
            new AuthFunctionBean() {
        public String[] authenticate(String userName, String password) {
            return null;
        }
    };
    private static final AuthFunctionBean twoRolePermittingAuthFunctionBean =
            new AuthFunctionBean() {
        public String[] authenticate(String userName, String password) {
            return twoRoles;
        }
    };
    private static final AuthFunctionBean purposefullyBrokenAuthFunctionBean =
            new AuthFunctionBean() {
        public String[] authenticate(String userName, String password) {
            throw new RuntimeException("Emulating broken AuthFunctionBean");
        }
    };
    private static final AuthFunctionBean denyingAuthFunctionBean =
            new AuthFunctionBean() {
        public String[] authenticate(String userName, String password)
                throws Exception {
            throw new Exception("Deny!");
        }
    };

    public void testPrecedences() {
        /* This should be more granular, using many different test methods,
         * but I want to spend as little time here as possible.  -- blaine
         */
        AuthBeanMultiplexer plexer = AuthBeanMultiplexer.getSingleton();
        plexer.clear();  // Clear in case a previous test method has populated
        Array res = null;

        try {
            AuthBeanMultiplexer.authenticate("DUMMY_NAME_12345", "x", "y");
            fail("Use of uninitialized AuthBeanMultiplexer did not throw");
        } catch (RuntimeException re) {
            // Intentionally empty.  Expect this.
        } catch (Exception e) {
            fail("Use of uninitialized AuthBeanMultiplexer threw a "
                    + e.getClass().getName() + " instead of a RTE");
        }

        plexer.setAuthFunctionBeans(Collections.singletonMap("DUMMY_NAME_12345",
                Arrays.asList(new AuthFunctionBean[] {
                        twoRolePermittingAuthFunctionBean,
                        purposefullyBrokenAuthFunctionBean,
                        denyingAuthFunctionBean})));
        try {
            res = AuthBeanMultiplexer.authenticate("DUMMY_NAME_12345", "u", "p");
        } catch (Exception e) {
            fail("2-role success test threw: " + e);
        }
        if (!AuthFunctionUtils.isWrapperFor(res, twoRoles)) {
            fail("2-role success test return success with roles: "
                    + StringUtil.arrayToString(AuthFunctionUtils.toStrings(res)));
        }
        if (!AuthFunctionUtils.isWrapperFor(res, twoRoles)) {
            fail("2-role success test return success with roles: "
                    + StringUtil.arrayToString(AuthFunctionUtils.toStrings(res)));
        }
        try {
            res = AuthBeanMultiplexer.authenticate("WRONG_NAME123456", "u", "p");
            fail("Authenticating against non-configured DB name did not throw");
        } catch (IllegalArgumentException iae) {
            // Intentionally empty.  Expect this
        } catch (Exception e) {
            fail("Authenticating against non-configured DB name did not throw "
                    + "IllegalArgumentException, but "
                    + e.getClass().getName());
        }

        try {
            plexer.setAuthFunctionBeans(Collections.singletonMap("DUMMY_NAME_12345",
                    Arrays.asList(new AuthFunctionBean[] {
                            purposefullyBrokenAuthFunctionBean,
                            twoRolePermittingAuthFunctionBean,
                            denyingAuthFunctionBean})));
            fail("Attempt to set an AuthFunctionBean without first clearing "
                    + "did not throw");
        } catch (IllegalStateException ise) {
            // Purposefully empty.  Expect this.
        } catch (Exception e) {
            fail("Attempt to set an AuthFunctionBean without first clearing did "
                    + "not throw an IllegalStateException, but a "
                    + e.getClass().getName());
        }

        plexer.clear();
        plexer.setAuthFunctionBeans(Collections.singletonMap("DUMMY_NAME_12345",
                Arrays.asList(new AuthFunctionBean[] {
                        purposefullyBrokenAuthFunctionBean,
                        purposefullyBrokenAuthFunctionBean,
                        twoRolePermittingAuthFunctionBean,
                        denyingAuthFunctionBean})));
        try {
            res = AuthBeanMultiplexer.authenticate("DUMMY_NAME_12345", "u", "p");
        } catch (Exception e) {
            fail("2-role success AFTER RTE test threw: " + e);
        }
        if (!AuthFunctionUtils.isWrapperFor(res, twoRoles)) {
            fail("2-role success AFTER RTE test return success with roles: "
                    + StringUtil.arrayToString(AuthFunctionUtils.toStrings(res)));
        }

        plexer.clear();
        plexer.setAuthFunctionBeans(Collections.singletonMap("DUMMY_NAME_12345",
                Arrays.asList(new AuthFunctionBean[] {
                        purposefullyBrokenAuthFunctionBean,
                        purposefullyBrokenAuthFunctionBean,
                        denyingAuthFunctionBean,
                        twoRolePermittingAuthFunctionBean,
                })));
        try {
            AuthBeanMultiplexer.authenticate("DUMMY_NAME_12345", "u", "p");
            fail("Denial test did not throw");
        } catch (RuntimeException e) {
            fail("Denial test threw: " + e);
        } catch (Exception e) {
            // Purposefully empty.  Expected.
        }

        plexer.clear();
        plexer.setAuthFunctionBeans(Collections.singletonMap("DUMMY_NAME_12345",
                Arrays.asList(new AuthFunctionBean[] {
                        purposefullyBrokenAuthFunctionBean,
                        purposefullyBrokenAuthFunctionBean
                })));
        try {
            AuthBeanMultiplexer.authenticate("DUMMY_NAME_12345", "u", "p");
            fail("RTE test did not throw");
        } catch (RuntimeException e) {
            // Purposefully empty.  Expected.
        } catch (Exception e) {
            fail("RTE test did not throw a RTE but a "
                    + e.getClass().getName());
        }
    }

    public void testFunctions() throws SQLException {
        String jdbcUrl = "jdbc:hsqldb:mem:memdb";
        Statement st = null;
        AuthBeanMultiplexer plexer = AuthBeanMultiplexer.getSingleton();
        plexer.clear();  // Clear in case a previous test method has popd.
        Connection con = DriverManager.getConnection(jdbcUrl, "SA", "");
        Connection authedCon = null;
        try {
            st = con.createStatement();
            try {
                DriverManager.getConnection(jdbcUrl, "zeno", "a password");
                fail("Security violation!  "
                        + "Permitted unknown user w/ no auth fn in place.");
            } catch (SQLException se) {
                if (se.getErrorCode() != -4001) {
                    fail("Auth failure generated SQL code " + se.getErrorCode()
                            + " instead of -4001");
                }
            }
            st.executeUpdate("CREATE ROLE role1");
            st.executeUpdate("CREATE ROLE role2");
            st.executeUpdate(
                    "SET DATABASE AUTHENTICATION FUNCTION EXTERNAL NAME "
                    + "'CLASSPATH:"
                    + "org.hsqldb.auth.AuthBeanMultiplexer.authenticate'");
            try {
                DriverManager.getConnection(jdbcUrl, "zeno", "a password");
                fail("Security violation!  "
                        + "Permitted unknown user w/ empty auth fn in place.");
            } catch (SQLException se) {
                if (se.getErrorCode() != -4001) {
                    fail("Auth failure generated SQL code " + se.getErrorCode()
                            + " instead of -4001");
                }
            }

            plexer.setAuthFunctionBeans(con,
                    Arrays.asList(new AuthFunctionBean[] {
                            twoRolePermittingAuthFunctionBean,
                            }));
            try {
                authedCon = DriverManager.getConnection(
                        jdbcUrl, "zeno", "a password");
            } catch (SQLException se) {
                logger.error("Multiplexer with single allow rule threw", se);
                fail("Multiplexer with single allow rule threw: " + se);
            }
            assertEquals(twoRolesSet, AuthUtils.getEnabledRoles(authedCon));
            authedCon.close();
            authedCon = null;
        } finally {
            if (authedCon != null) try {
                authedCon.close();
            } catch (SQLException se) {
                logger.error("Close of Authed Conn. failed:" + se);
            } finally {
                authedCon = null;
            }
            if (st != null) try {
                st.executeUpdate("SHUTDOWN");
                st.close();
            } catch (SQLException se) {
                logger.error("Close of Statement failed:" + se);
            } finally {
                st = null;
            }
            con.close();
        }
    }

    public static Test suite() {
        return new TestSuite(AuthBeanMultiplexerTest.class);
    }

    /**
     * This method allows to easily run this unit test independent of the other
     * unit tests, and without dealing with Ant or unrelated test suites.
     */
    static public void main(String[] sa) {
        if (sa.length > 0 && sa[0].startsWith("-g")) {
            junit.swingui.TestRunner.run(AuthBeanMultiplexerTest.class);
        } else {
            junit.framework.TestResult result = TestRunner.run(suite());

            System.exit(result.wasSuccessful() ? 0 : 1);
        }
    }
}
