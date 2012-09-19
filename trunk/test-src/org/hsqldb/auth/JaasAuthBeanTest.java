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

import java.util.HashSet;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.IOException;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;
import org.hsqldb.lib.FrameworkLogger;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;

@ForSubject(JaasAuthBean.class)
public class JaasAuthBeanTest extends BaseTestCase {
    private static FrameworkLogger logger =
            FrameworkLogger.getLog(JaasAuthBeanTest.class);

    private static final String cfgResourcePath =
            "/org/hsqldb/resources/jaas.cfg";
    protected String appDbUrl = "jdbc:hsqldb:mem:appDb";
    private static File jaasCfgFile;
    protected Connection saCon;
    protected Statement saSt;
    private String savedLoginConfig;
    protected AuthBeanMultiplexer plexer;

    protected void setUp() throws SQLException {
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
        } catch (Exception e) {}
        saCon = DriverManager.getConnection(appDbUrl, "SA", "");
        saCon.setAutoCommit(false);
        saSt = saCon.createStatement();
        saSt.executeUpdate("CREATE ROLE role1");
        saSt.executeUpdate("CREATE ROLE role2");
        saSt.executeUpdate("CREATE ROLE role3");
        saSt.executeUpdate("CREATE ROLE role4");
        saSt.executeUpdate("CREATE SCHEMA s1");
        saSt.executeUpdate("CREATE SCHEMA s2");
        saSt.executeUpdate("CREATE SCHEMA s3");
        saSt.executeUpdate(
                "SET DATABASE AUTHENTICATION FUNCTION EXTERNAL NAME "
                + "'CLASSPATH:"
                + "org.hsqldb.auth.AuthBeanMultiplexer.authenticate'");
        plexer = AuthBeanMultiplexer.getSingleton();
        plexer.clear();  // Clear in case a previous test method has popd.
        if (jaasCfgFile == null) {
            int i;
            byte[] copyBuffer = new byte[512];
            InputStream iStream = null;
            OutputStream oStream = null;
            try {
                iStream = getClass().getResourceAsStream(cfgResourcePath);
                if (iStream == null)
                    throw new IOException(
                            "Failed to read resource: " + cfgResourcePath);
                jaasCfgFile = File.createTempFile(getClass()
                        .getName().replaceFirst(".*\\.", ""), ".jaascfg");
                jaasCfgFile.deleteOnExit();
                oStream = new FileOutputStream(jaasCfgFile);
                while ((i = iStream.read(copyBuffer)) > -1)
                    oStream.write(copyBuffer, 0, i);
            } catch (IOException ioe) {
                logger.severe("Failed to prepare JAAS config file in local "
                        + "file system", ioe);
                throw new IllegalStateException("Failed to prepare JAAS "
                        + "config file in local file system", ioe);
            } finally {
                try {
                    if (oStream != null) {
                        oStream.close();
                        oStream = null;
                    }
                    if (iStream != null) {
                        iStream.close();
                        iStream = null;
                    }
                } catch (IOException ioe) {
                    logger.error("Failed to clear file objects");
                }
            }
        }
        savedLoginConfig =
                System.getProperty("java.security.auth.login.config");
        System.setProperty("java.security.auth.login.config",
                jaasCfgFile.getAbsolutePath());
    }

    protected void tearDown() {
        if (savedLoginConfig == null) {
            System.getProperties().remove("java.security.auth.login.config");
        } else {
            System.setProperty(
                    "java.security.auth.login.config", savedLoginConfig);
        }
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

    public void testViaPrincipals() throws SQLException {
        JaasAuthBean jaasBean = new JaasAuthBean();
        jaasBean.setApplicationKey("test");
        jaasBean.setRoleSchemaValuePattern(Pattern.compile("RS:(.+)"));
        // jaasBean.setRoleSchemaViaCredential(false);  this is default
        jaasBean.init();
        plexer.setAuthFunctionBeans(saCon,
                Arrays.asList(new AuthFunctionBean[] { jaasBean }));
        Connection authedCon = null;
        Statement st = null;
        boolean ok = false;
        try {
            try {
                authedCon = DriverManager.getConnection(
                        appDbUrl, "alpha", "alpha");
            } catch (SQLException se) {
                ok = true;
            }
            if (!ok) {
                fail("Access allowed even though password starts with 'a'");
            }
            try {
                authedCon = DriverManager.getConnection(
                        appDbUrl, "alpha", "beta");
            } catch (SQLException se) {
                fail("Access denied for alpha/beta: " + se);
            }
            st = authedCon.createStatement();
            assertEquals(new HashSet<String>(Arrays.asList(new String[] {
                    "ROLE2"
                    })), AuthUtils.getEnabledRoles(authedCon));
            assertEquals("S1", AuthUtils.getInitialSchema(authedCon));
        } finally {
            if (st != null) try {
                st.close();
            } catch (SQLException se) {
                logger.error("Close of Statement failed:" + se);
            } finally {
                st = null;
            }
            if (authedCon != null) try {
                authedCon.close();
            } catch (SQLException se) {
                logger.error("Close of Conn. failed:" + se);
            } finally {
                authedCon = null;
            }
        }
    }

    public void testViaCredentials() throws SQLException {
        JaasAuthBean jaasBean = new JaasAuthBean();
        jaasBean.setApplicationKey("test");
        jaasBean.setRoleSchemaValuePattern(Pattern.compile("RS:(.+)"));
        jaasBean.setRoleSchemaViaCredential(true);
        jaasBean.init();
        plexer.setAuthFunctionBeans(saCon,
                Arrays.asList(new AuthFunctionBean[] { jaasBean }));
        Connection authedCon = null;
        Statement st = null;
        boolean ok = false;
        try {
            try {
                authedCon = DriverManager.getConnection(
                        appDbUrl, "alpha", "alpha");
            } catch (SQLException se) {
                ok = true;
            }
            if (!ok) {
                fail("Access allowed even though password starts with 'a'");
            }
            try {
                authedCon = DriverManager.getConnection(
                        appDbUrl, "alpha", "beta");
            } catch (SQLException se) {
                fail("Access denied for alpha/beta: " + se);
            }
            st = authedCon.createStatement();
            assertEquals(new HashSet<String>(Arrays.asList(new String[] {
                    "CHANGE_AUTHORIZATION", "ROLE1"
                    })), AuthUtils.getEnabledRoles(authedCon));
            assertEquals(null, AuthUtils.getInitialSchema(authedCon));
        } finally {
            if (st != null) try {
                st.close();
            } catch (SQLException se) {
                logger.error("Close of Statement failed:" + se);
            } finally {
                st = null;
            }
            if (authedCon != null) try {
                authedCon.close();
            } catch (SQLException se) {
                logger.error("Close of Conn. failed:" + se);
            } finally {
                authedCon = null;
            }
        }
    }

    public static Test suite() {
        return new TestSuite(JaasAuthBeanTest.class);
    }

    /**
     * This method allows to easily run this unit test independent of the other
     * unit tests, and without dealing with Ant or unrelated test suites.
     */
    public static void main(String[] sa) {
        if (sa.length > 0 && sa[0].startsWith("-g")) {
            junit.swingui.TestRunner.run(JaasAuthBeanTest.class);
        } else {
            junit.framework.TestResult result = TestRunner.run(suite());

            System.exit(result.wasSuccessful() ? 0 : 1);
        }
    }
}
