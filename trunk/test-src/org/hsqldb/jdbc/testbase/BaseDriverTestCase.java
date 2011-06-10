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

package org.hsqldb.jdbc.testbase;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.util.Properties;

import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @version 2.1.0
 * @since HSQLDB 2.1.0
 */
@ForSubject(java.sql.Driver.class)
public abstract class BaseDriverTestCase extends BaseJdbcTestCase {

    public BaseDriverTestCase(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    protected abstract int getExpectedDriverPropertyInfoCount();

    protected abstract boolean getExpectedJdbcCompliant();

    protected abstract int getExpectedMajorVersion();

    protected abstract int getExpectedMinorVersion();

    protected Driver newDriver() throws Exception {
        return (Driver) Class.forName(getDriver()).newInstance();
    }

    @OfMethod(value = "acceptsURL()")
    public void testAcceptsURL() throws Exception {
        final String url = getUrl();
        final Driver driver = newDriver();

        assertEquals("driver.acceptsURL(" + url + ")", true, 
                driver.acceptsURL(url));

        assertEquals("driver.acceptsURL(xyz:" + url + ")", false,
                driver.acceptsURL("xyz:" + url));
    }

    @OfMethod(value = "connect()")
    public void testConnect() throws Exception {
        final String url = getUrl();
        final Properties info = new Properties();
        final Driver driver = newDriver();

        info.setProperty("user", getUser());
        info.setProperty("password", getPassword());

        Connection conn = null;

        try {
            conn = driver.connect(url, info);
            assertNotNull(conn);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                }
            }
        }
    }

    @OfMethod(value = "getMajorVersion()")
    public void testGetMajorVersion() throws Exception {
        final Driver driver = newDriver();
        final int expResult = getExpectedMajorVersion();
        final int result = driver.getMajorVersion();

        assertEquals(expResult, result);
    }

    @OfMethod(value = "getMinorVersion()")
    public void testGetMinorVersion() throws Exception {
        final Driver driver = newDriver();
        final int expResult = getExpectedMinorVersion();
        final int result = driver.getMinorVersion();

        assertEquals(expResult, result);
    }

    @OfMethod(value = "getPropertyInfo()")
    public void testGetPropertyInfo() throws Exception {
        final String url = getUrl();
        final Properties info = new Properties();
        final Driver driver = newDriver();
        final int expCount = getExpectedDriverPropertyInfoCount();
        final DriverPropertyInfo[] result = driver.getPropertyInfo(url, info);

        assertNotNull(result);
        assertEquals(expCount, result.length);

        for (int i = 0; i < expCount; i++) {
            assertNotNull(result[i].name);
        }
    }

    @OfMethod(value = "jdbcCompliant()")
    public void testJdbcCompliant() throws Exception {
        final Driver driver = newDriver();
        final boolean expResult = getExpectedJdbcCompliant();
        final boolean result = driver.jdbcCompliant();

        assertEquals(expResult, result);
    }

}
