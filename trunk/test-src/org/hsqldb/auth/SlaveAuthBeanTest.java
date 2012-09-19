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
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.DriverManager;
import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;
import org.hsqldb.lib.FrameworkLogger;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;

@ForSubject(HsqldbSlaveAuthBean.class)
public class SlaveAuthBeanTest extends BaseTestCase {
    private static FrameworkLogger logger =
            FrameworkLogger.getLog(SlaveAuthBeanTest.class);

    protected Connection saCon, masterCon;
    protected Statement saSt, masterSt;
    protected String slaveUrl = "jdbc:hsqldb:mem:slave";
    protected String masterUrl = "jdbc:hsqldb:mem:master";
    private static final String[] twoRoles = new String[] { "ROLE1", "ROLE2" };
    private static final Set<String> twoRolesSet =
            new HashSet<String>(Arrays.asList(twoRoles));

    protected void setUp() throws SQLException {
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
        } catch (Exception e) {}
        saCon = DriverManager.getConnection(slaveUrl, "SA", "");
        saCon.setAutoCommit(false);
        saSt = saCon.createStatement();
        masterCon = DriverManager.getConnection(masterUrl, "SA", "");
        masterCon.setAutoCommit(false);
        masterSt = masterCon.createStatement();
        masterSt.executeUpdate("CREATE ROLE role1");
        masterSt.executeUpdate("CREATE ROLE role2");
        masterSt.executeUpdate("CREATE ROLE role3");
        masterSt.executeUpdate("CREATE ROLE role4");
        masterSt.executeUpdate("CREATE USER u password 'u'");
        masterSt.executeUpdate("GRANT role1 TO u");
        masterSt.executeUpdate("GRANT role2 TO u");
        masterSt.executeUpdate("CREATE SCHEMA s1");
        masterSt.executeUpdate("ALTER USER u SET INITIAL SCHEMA s1");
        saSt.executeUpdate("CREATE ROLE role1");
        saSt.executeUpdate("CREATE ROLE role2");
        saSt.executeUpdate("CREATE ROLE role3");
        saSt.executeUpdate("CREATE ROLE role4");
        saSt.executeUpdate("CREATE SCHEMA s1");
        saSt.executeUpdate("CREATE SCHEMA s2");
        saSt.executeUpdate("CREATE SCHEMA s3");
        saSt.executeUpdate("CREATE TABLE s1.s1t1 (i INTEGER)");
        saSt.executeUpdate("CREATE TABLE s2.s2t1 (i INTEGER)");
        saSt.executeUpdate("CREATE TABLE s3.s3t1 (i INTEGER)");
        saSt.executeUpdate("GRANT ALL ON s1.s1t1 TO role1");
        saSt.executeUpdate("GRANT ALL ON s2.s2t1 TO role2");
        saSt.executeUpdate("GRANT ALL ON s3.s3t1 TO role3");
        saSt.executeUpdate(
                "SET DATABASE AUTHENTICATION FUNCTION EXTERNAL NAME "
                + "'CLASSPATH:"
                + "org.hsqldb.auth.AuthBeanMultiplexer.authenticate'");
        AuthBeanMultiplexer plexer = AuthBeanMultiplexer.getSingleton();
        plexer.clear();  // Clear in case a previous test method has popd.
        HsqldbSlaveAuthBean slaveBean = new HsqldbSlaveAuthBean();
        slaveBean.setMasterJdbcUrl(masterUrl);
        slaveBean.init();
        plexer.setAuthFunctionBeans(saCon,
                Arrays.asList(new AuthFunctionBean[] { slaveBean }));
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
        if (masterSt != null) try {
            masterSt.executeUpdate("SHUTDOWN");
        } catch (SQLException se) {
            logger.error("Tear-down of master Conn. failed:" + se);
        }
        if (masterSt != null) try {
            masterSt.close();
        } catch (SQLException se) {
            logger.error("Close of master Statement failed:" + se);
        } finally {
            masterSt = null;
        }
        if (masterCon != null) try {
            masterCon.close();
        } catch (SQLException se) {
            logger.error("Close of master Conn. failed:" + se);
        } finally {
            masterCon = null;
        }
    }

    public void testBasic() throws SQLException {
        Connection authedCon = null;
        Statement st = null;
        try {
            try {
                authedCon = DriverManager.getConnection(slaveUrl, "U", "u");
            } catch (SQLException se) {
                fail("Access denied for basic Slave function: " + se);
            }
            st = authedCon.createStatement();
            assertFalse("Positive test failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO s1t1 VALUES(1)"));
            assertTrue("Default schema wrong.  Seeing s2.",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO s2t1 VALUES(3)"));
            assertFalse("Access to secondary schema failed",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO s2.s2t1 VALUES(1)"));
            assertTrue("Accessed offlimits schema s3.",
                    AuthFunctionUtils.updateDoesThrow(
                    st, "INSERT INTO s3.s3t1 VALUES(3)"));
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
            junit.swingui.TestRunner.run(SlaveAuthBeanTest.class);
        } else {
            junit.framework.TestResult result = TestRunner.run(suite());

            System.exit(result.wasSuccessful() ? 0 : 1);
        }
    }
}
