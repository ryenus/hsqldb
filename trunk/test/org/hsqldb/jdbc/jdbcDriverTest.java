/* Copyright (c) 2001-2007, The HSQL Development Group
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

package org.hsqldb.jdbc;

import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.util.Properties;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 *
 * @author boucherb@users
 */
public class jdbcDriverTest extends JdbcTestCase {

    public jdbcDriverTest(String testName) {
        super(testName);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(jdbcDriverTest.class);

        return suite;
    }

    protected Driver newDriver() throws Exception {
        return (Driver) Class.forName(getDriver()).newInstance();
    }

    protected int getExpectedDriverPropertyInfoCount() {
        return getIntProperty("driver.property.info.count", 6);
    }

    protected int getExpectedMajorVersion() {
        return getIntProperty("driver.major.version", 1);
    }

    protected int getExpectedMinorVersion() {
       return getIntProperty("driver.minor.version", 9);
    }
    
    protected boolean getExpectedJdbcCompliant()
    {
        return getBooleanProperty("driver.jdbc.compliant", true);
    }

    /**
     * Test of connect method, of interface java.sql.Driver.
     */
    public void testConnect() throws Exception {
        println("connect");

        String     url    = getUrl();
        Properties info   = new Properties();
        Driver     driver = newDriver();

        info.setProperty("user", getUser());
        info.setProperty("password", getPassword());

        assertNotNull(driver.connect(url, info));
    }

    /**
     * Test of acceptsURL method, of interface java.sql.Driver.
     */
    public void testAcceptsURL() throws Exception {
        println("acceptsURL");

        String url    = getUrl();
        Driver driver = newDriver();

        assertEquals("driver.acceptsURL(" + url + ")",
                     true,
                     driver.acceptsURL(url));

        assertEquals("driver.acceptsURL(xyz:" + url + ")",
                     false,
                     driver.acceptsURL("xyz:" + url));
    }

    /**
     * Test of getPropertyInfo method, of interface java.sql.Driver.
     */
    public void testGetPropertyInfo() throws Exception {
        println("getPropertyInfo");

        String               url      = getUrl();
        Properties           info     = new Properties();
        Driver               driver   = newDriver();
        int                  expCount = getExpectedDriverPropertyInfoCount();
        DriverPropertyInfo[] result   = driver.getPropertyInfo(url, info);

        assertNotNull(result);
        assertEquals(expCount, result.length);

        for (int i = 0; i < expCount; i++) {
            assertNotNull(result[i].name);
        }
    }

    /**
     * Test of getMajorVersion method, of interface java.sql.Driver.
     */
    public void testGetMajorVersion() throws Exception {
        println("getMajorVersion");

        Driver driver = newDriver();
        int expResult = getExpectedMajorVersion();
        int result    = driver.getMajorVersion();

        assertEquals(expResult, result);
    }

    /**
     * Test of getMinorVersion method, of interface java.sql.Driver.
     */
    public void testGetMinorVersion() throws Exception {
        println("getMinorVersion");

        Driver driver = newDriver();
        int expResult = getExpectedMinorVersion();
        int result    = driver.getMinorVersion();

        assertEquals(expResult, result);
    }

    /**
     * Test of jdbcCompliant method, of interface java.sql.Driver.
     */
    public void testJdbcCompliant() throws Exception {
        println("jdbcCompliant");

        Driver  driver    = newDriver();
        boolean expResult = getExpectedJdbcCompliant();
        boolean result    = driver.jdbcCompliant();

        assertEquals(expResult, result);
    }

    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }

}
