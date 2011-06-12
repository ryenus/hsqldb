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

package org.hsqldb.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.util.Calendar;
import java.util.HashMap;
import junit.framework.TestSuite;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;
import org.hsqldb.testbase.ForSubject;
import java.io.ByteArrayInputStream;

// TODO:  See if this can be done reflectively.

/**
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 */
@ForSubject(JDBCCallableStatement.class)
public class JDBCCallableStatementWhileClosedTest extends BaseJdbcTestCase {

    public JDBCCallableStatementWhileClosedTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     *
     * @return
     */
    public static TestSuite suite() {
        return new TestSuite(JDBCCallableStatementWhileClosedTest.class);
    }

    /**
     *
     * @return
     */
    protected String getSql() {
        return "{?= call cast(1 as integer)}";
    }

    /**
     *
     * @return
     */
    protected int getParameterType() {
        return java.sql.Types.INTEGER;
    }

    /**
     *
     * @return
     */
    protected String getParameterTypeName() {
        return "INTEGER";
    }

    /**
     *
     * @return
     */
    protected int getParameterScale() {
        return 0;
    }

    /**
     *
     * @return
     */
    protected int getParameterIndex() {
        return 1;
    }

    /**
     *
     * @return
     */
    protected String getParameterName() {
        return "@p0";
    }

    protected int getStatementClosedErrorCode() {
        return -ErrorCode.X_07501;
    }

    /**
     *
     * @return
     * @throws java.lang.Exception
     */
    protected CallableStatement newClosedCall() throws Exception {
        CallableStatement instance
                = connectionFactory().prepareCall(getSql(), newConnection());

        instance.close();

        return instance;
    }

    /**
     * Checks to ensure either sql feature not supported with sql state '0A...'
     * or sql exception with error code that indicates statement is closed.
     * @param ex to check
     */
    protected void checkException(SQLException ex) {
        if (ex instanceof SQLFeatureNotSupportedException) {
            assertEquals("0A", ex.getSQLState().substring(0,2));
        } else {
            assertEquals(
                    "Error code is not 'statement closed' for exception: "
                    + ex.toString(),
                    getStatementClosedErrorCode(),
                    ex.getErrorCode());
        }
    }

    /**
     * Test of registerOutParameter method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testRegisterOutParameter() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.registerOutParameter(getParameterName(), getParameterType());
            fail("Allowed register out parameter by name and type after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.registerOutParameter(getParameterName(), getParameterType(), getParameterTypeName());
            fail("Allowed register out parameter by name, type and type name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.registerOutParameter(getParameterName(), getParameterType(), getParameterScale());
            fail("Allowed register out parameter by name, type and scale after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.registerOutParameter(getParameterIndex(), getParameterType());
            fail("Allowed register out parameter by index and type after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.registerOutParameter(getParameterIndex(), getParameterType(), getParameterTypeName());
            fail("Allowed register out parameter by index, type and type name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.registerOutParameter(getParameterIndex(), getParameterType(), getParameterScale());
            fail("Allowed register out parameter by index, type and scale after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of wasNull method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testWasNull() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.wasNull();
            fail("Allowed was null after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getString method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetString() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getString(getParameterName());
            fail("Allowed get string by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getString(getParameterIndex());
            fail("Allowed get string by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getBoolean method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetBoolean() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getBoolean(getParameterName());
            fail("Allowed get boolean by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getBoolean(getParameterIndex());
            fail("Allowed get boolean by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getByte method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetByte() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getByte(getParameterName());
            fail("Allowed get byte by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getByte(getParameterIndex());
            fail("Allowed get byte by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getShort method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetShort() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getShort(getParameterName());
            fail("Allowed get short by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getShort(getParameterIndex());
            fail("Allowed get short by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getInt method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetInt() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getInt(getParameterName());
            fail("Allowed get int by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getInt(getParameterIndex());
            fail("Allowed get int by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getLong method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetLong() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getLong(getParameterName());
            fail("Allowed get long after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getLong(getParameterIndex());
            fail("Allowed get long after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getFloat method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetFloat() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getFloat(getParameterName());
            fail("Allowed get float by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getFloat(getParameterIndex());
            fail("Allowed get float by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getDouble method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetDouble() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getDouble(getParameterName());
            fail("Allowed get double by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getDouble(getParameterIndex());
            fail("Allowed get double by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getBigDecimal method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    @SuppressWarnings("deprecation")
    public void testGetBigDecimal() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getBigDecimal(getParameterName());
            fail("Allowed get big decimal by parameter nameafter close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
        try {
            instance.getBigDecimal(getParameterIndex());
            fail("Allowed get big decimal by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
        try {
            instance.getBigDecimal(getParameterIndex(), 0);
            fail("Allowed get big decimal by parameter index with scale after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getBytes method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetBytes() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getBytes(getParameterName());
            fail("Allowed get bytes by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getBytes(getParameterIndex());
            fail("Allowed get bytes by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getDate method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetDate() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getDate(getParameterName());
            fail("Allowed get date by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getDate(getParameterName(), Calendar.getInstance());
            fail("Allowed get date by parameter name and calendar after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getDate(getParameterIndex());
            fail("Allowed get date by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getDate(getParameterIndex(), Calendar.getInstance());
            fail("Allowed get date by parameter index and calendar after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getTime method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetTime() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getTime(getParameterName());
            fail("Allowed get time by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getTime(getParameterName(), Calendar.getInstance());
            fail("Allowed get time by parameter name and calendar after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getTime(getParameterIndex());
            fail("Allowed get time by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getTime(getParameterIndex(), Calendar.getInstance());
            fail("Allowed get time by parameter index and calendar after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getTimestamp method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetTimestamp() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getTimestamp(getParameterName());
            fail("Allowed get timestamp by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getTimestamp(getParameterName(), Calendar.getInstance());
            fail("Allowed get timestamp by parameter name and calendar after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getTimestamp(getParameterIndex());
            fail("Allowed get timestamp by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getTimestamp(getParameterIndex(), Calendar.getInstance());
            fail("Allowed get timestamp by parameter index and calendar after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getObject method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetObject() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getObject(getParameterIndex());
            fail("Allowed get object by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getObject(getParameterIndex(), new HashMap<String,Class<?>>());
            fail("Allowed get object by parameter index and type map after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getObject(getParameterName());
            fail("Allowed get object by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getObject(getParameterName(), new HashMap<String,Class<?>>());
            fail("Allowed get object by parameter name and type map after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getRef method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetRef() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getRef(getParameterName());
            fail("Allowed get ref by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getRef(getParameterIndex());
            fail("Allowed get ref by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getBlob method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetBlob() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getBlob(getParameterName());
            fail("Allowed get blob by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getBlob(getParameterIndex());
            fail("Allowed get blob by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getClob method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetClob() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getClob(getParameterName());
            fail("Allowed get clob by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getClob(getParameterIndex());
            fail("Allowed get clob by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getArray method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetArray() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getArray(getParameterName());
            fail("Allowed get array by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getArray(getParameterIndex());
            fail("Allowed get array by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getURL method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetURL() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.getURL(getParameterName());
            fail("Allowed get url by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getURL(getParameterIndex());
            fail("Allowed get url by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setURL method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetURL() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.setURL(getParameterName(), null);
            fail("Allowed set url by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setURL(getParameterIndex(), null);
            fail("Allowed set url by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setNull method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetNull() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.setNull(getParameterName(), getParameterType());
            fail("Allowed set null by parameter name and type after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setNull(getParameterName(), getParameterType(), getParameterTypeName());
            fail("Allowed set null by parameter name, type and type name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setNull(getParameterIndex(), getParameterType());
            fail("Allowed set null by parameter index and type after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setNull(getParameterIndex(), getParameterType(), getParameterTypeName());
            fail("Allowed set null by parameter index, type and type name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setBoolean method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetBoolean() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.setBoolean(getParameterName(), true);
            fail("Allowed set boolean by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setBoolean(getParameterIndex(), true);
            fail("Allowed set boolean by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setByte method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetByte() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.setByte(getParameterName(), (byte)0);
            fail("Allowed set byte by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setByte(getParameterIndex(), (byte)0);
            fail("Allowed set byte by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setShort method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetShort() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.setShort(getParameterName(), (short)0);
            fail("Allowed set byte by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setShort(getParameterIndex(), (short)0);
            fail("Allowed set short by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setInt method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetInt() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.setInt(getParameterName(), 0);
            fail("Allowed set int by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setInt(getParameterIndex(), 0);
            fail("Allowed set int by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setLong method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetLong() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.setLong(getParameterName(), 0L);
            fail("Allowed set long by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setLong(getParameterIndex(), 0L);
            fail("Allowed set long by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setFloat method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetFloat() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.setFloat(getParameterName(), 0F);
            fail("Allowed set float by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setFloat(getParameterIndex(), 0F);
            fail("Allowed set float by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setDouble method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetDouble() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.setDouble(getParameterName(), 0D);
            fail("Allowed set double by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setDouble(getParameterIndex(), 0D);
            fail("Allowed set double by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setBigDecimal method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetBigDecimal() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.setBigDecimal(getParameterName(), new BigDecimal("0.0"));
            fail("Allowed set big decimal by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setBigDecimal(getParameterIndex(), new BigDecimal("0.0"));
            fail("Allowed set big decimal by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setString method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetString() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.setString(getParameterName(), "");
            fail("Allowed set string by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setString(getParameterIndex(), "");
            fail("Allowed set string by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setBytes method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetBytes() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.setBytes(getParameterName(), new byte[0]);
            fail("Allowed set bytes by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setBytes(getParameterIndex(), new byte[0]);
            fail("Allowed set bytes by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setDate method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetDate() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.setDate(getParameterName(), Date.valueOf("2007-05-02"));
            fail("Allowed set date by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setDate(getParameterName(), Date.valueOf("2007-05-02"), Calendar.getInstance());
            fail("Allowed set date by parameter name and calendar after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setDate(getParameterIndex(), Date.valueOf("2007-05-02"));
            fail("Allowed set date by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setDate(getParameterIndex(), Date.valueOf("2007-05-02"), Calendar.getInstance());
            fail("Allowed set date by parameter index and calendar after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setTime method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetTime() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.setTime(getParameterName(), Time.valueOf("04:48:01"));
            fail("Allowed set time by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setTime(getParameterName(), Time.valueOf("04:48:01"), Calendar.getInstance());
            fail("Allowed set time by parameter name and calendar after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setTime(getParameterIndex(), Time.valueOf("04:48:01"));
            fail("Allowed set time by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setTime(getParameterIndex(), Time.valueOf("04:48:01"), Calendar.getInstance());
            fail("Allowed set date by parameter index and calendar after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setTimestamp method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetTimestamp() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.setTimestamp(getParameterName(), Timestamp.valueOf("2007-05-02 04:48:01"));
            fail("Allowed set timestamp by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setTimestamp(getParameterName(), Timestamp.valueOf("2007-05-02 04:48:01"), Calendar.getInstance());
            fail("Allowed set timestamp by parameter name and calendar after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setTimestamp(getParameterIndex(), Timestamp.valueOf("2007-05-02 04:48:01"));
            fail("Allowed set time by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setTimestamp(getParameterIndex(), Timestamp.valueOf("2007-05-02 04:48:01"), Calendar.getInstance());
            fail("Allowed set timestamp by parameter index and calendar after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setAsciiStream method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetAsciiStream() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.setAsciiStream(getParameterName(),  new ByteArrayInputStream(new byte[]{}));
            fail("Allowed set ascii stream by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setAsciiStream(getParameterIndex(),  new ByteArrayInputStream(new byte[]{}));
            fail("Allowed set ascii stream by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setBinaryStream method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetBinaryStream() throws Exception {
        CallableStatement instance = newClosedCall();

        try {
            instance.setBinaryStream(getParameterName(), new ByteArrayInputStream(new byte[]{}));
            fail("Allowed set binary stream by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setAsciiStream(getParameterIndex(), new ByteArrayInputStream(new byte[]{}));
            fail("Allowed set ascii stream by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setObject method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetObject() throws Exception {
        String parameterName = getParameterName();
        int parameterIndex = getParameterIndex();
        Object x = new Integer(1);
        int targetSqlType = getParameterType();
        int scale = getParameterScale();
        CallableStatement instance = newClosedCall();

        try {
            instance.setObject(parameterName, x);
            fail("Allowed set object by parameter name after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setObject(parameterIndex, x);
            fail("Allowed set object by parameter index after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setObject(parameterName, x, targetSqlType);
            fail("Allowed set object by parameter name and target sql type after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setObject(parameterIndex, x, targetSqlType);
            fail("Allowed set object by parameter index and target sql type after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setObject(parameterName, x, targetSqlType, scale);
            fail("Allowed set object by parameter name, target sql type and scale after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setObject(parameterIndex, x, targetSqlType, scale);
            fail("Allowed set object by parameter index, target sql type and scale after close");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setCharacterStream method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetCharacterStream() throws Exception {
        String parameterName = getParameterName();
        int parameterIndex = getParameterIndex();
        Reader reader = new java.io.StringReader("1");
        int intLength = 1;
        long longLength = 1L;
        CallableStatement instance = newClosedCall();

        try {
            instance.setCharacterStream(parameterName, reader);
            fail("Allowed set character stream by parameter name after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setCharacterStream(parameterIndex, reader);
            fail("Allowed set character stream by parameter index after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setCharacterStream(parameterName, reader, intLength);
            fail("Allowed set character stream by parameter name and int length after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setCharacterStream(parameterName, reader, longLength);
            fail("Allowed set character stream by parameter name and long length after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setCharacterStream(parameterIndex, reader, intLength);
            fail("Allowed set character stream by parameter index and int length after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setCharacterStream(parameterIndex, reader, longLength);
            fail("Allowed set character stream by parameter index and long length after close");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of getRowId method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetRowId() throws Exception {
        String parameterName = getParameterName();
        int parameterIndex = getParameterIndex();
        CallableStatement instance = newClosedCall();

        try {
            instance.getRowId(parameterName);
            fail("Allowed get rowid by parameter name after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.getRowId(parameterIndex);
            fail("Allowed get rowid by parameter index after close");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setRowId method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetRowId() throws Exception {
        String parameterName = getParameterName();
        int parameterIndex = getParameterIndex();
        RowId x = null;
        CallableStatement instance = newClosedCall();

        try {
            instance.setRowId(parameterName, x);
            fail("Allowed set row id by parameter name after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setRowId(parameterIndex, x);
            fail("Allowed set row id by parameter index after close");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setNString method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetNString() throws Exception {
        String parameterName = getParameterName();
        int parameterIndex = getParameterIndex();
        String value = "1";
        CallableStatement instance = newClosedCall();

        try {
            instance.setNString(parameterName, value);
            fail("Allowed set nstring by parameter name after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setNString(parameterIndex, value);
            fail("Allowed set nstring by parameter index after close");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setNCharacterStream method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetNCharacterStream() throws Exception {
        String parameterName = getParameterName();
        int parameterIndex = getParameterIndex();
        CallableStatement instance = newClosedCall();

        try {
            instance.setNCharacterStream(parameterName, new StringReader("1"));
            fail("Allowed set ncharacter stream by parameter name after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setNCharacterStream(parameterIndex, new StringReader("1"));
            fail("Allowed set ncharacter stream by parameter index after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setNCharacterStream(parameterName, new StringReader("1"), 1);
            fail("Allowed set ncharacter stream by parameter name and length after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setNCharacterStream(parameterIndex, new StringReader("1"), 1);
            fail("Allowed set ncharacter stream by parameter index and length after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

    }

    /**
     * Test of setNClob method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetNClob() throws Exception {
        String parameterName = getParameterName();
        int parameterIndex = getParameterIndex();
        CallableStatement instance = newClosedCall();
        NClob nclob = newConnection().createNClob();
        nclob.setString(1, "1");

        try {
            instance.setNClob(parameterName,nclob);
            fail("Allowed set nclob by parameter name after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setNClob(parameterName, new StringReader("1"));
            fail("Allowed set nclob using reader by parameter name after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setNClob(parameterIndex, nclob);
            fail("Allowed set nclob by parameter index after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setNClob(parameterIndex, new StringReader("1"));
            fail("Allowed set nclob using reader by parameter index after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setNClob(parameterName, new StringReader("1"), 1);
            fail("Allowed set nclob using reader and length by parameter name after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setNClob(parameterIndex, new StringReader("1"), 1);
            fail("Allowed set nclob using reader and length by parameter index after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

    }

    /**
     * Test of setClob method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetClob() throws Exception {
        String parameterName = getParameterName();
        int parameterIndex = getParameterIndex();

        long length = 0L;
        CallableStatement instance = newClosedCall();
        Clob x = newConnection().createClob(); // this will never get used

        try {
            instance.setClob(parameterName, x);
            fail("Allowed set clob by parameter name after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setClob(parameterName, new StringReader("1"));
            fail("Allowed set clob with reader by parameter name after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setClob(parameterIndex, x);
            fail("Allowed set clob by parameter index after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setClob(parameterName, new StringReader("1"), length);
            fail("Allowed set clob with reader and length by parameter name after close");
        } catch (SQLException ex) {
            checkException(ex);
        }

        try {
            instance.setClob(parameterIndex, new StringReader("1"), length);
            fail("Allowed set clob with reader and length by parameter index after close");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }

    /**
     * Test of setBlob method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetBlob() throws Exception {
        String parameterName = getParameterName();
        int parameterIndex = getParameterIndex();
        InputStream inputStream = null;
        long length = 0L;
        CallableStatement instance = null;

        //instance.setBlob(parameterName, inputStream, length);

        // TODO review the generated test code and remove the default call to fail.
        stubTestResult();
    }

    /**
     * Test of getNClob method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetNClob() throws Exception {
        int parameterIndex = 0;
        CallableStatement instance = null;

        NClob expResult = null;


        // TODO review the generated test code and remove the default call to fail.
        stubTestResult();
    }

    /**
     * Test of setSQLXML method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testSetSQLXML() throws Exception {
        String parameterName = "";
        SQLXML xmlObject = null;
        CallableStatement instance = null;

        //instance.setSQLXML(parameterName, xmlObject);

        // TODO review the generated test code and remove the default call to fail.
        stubTestResult();
    }

    /**
     * Test of getSQLXML method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetSQLXML() throws Exception {
        int parameterIndex = 0;
        CallableStatement instance = null;

        SQLXML expResult = null;
//        SQLXML result = instance.getSQLXML(parameterIndex);
//        assertEquals(expResult, result);

        // TODO review the generated test code and remove the default call to fail.
        stubTestResult();
    }

    /**
     * Test of getNString method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetNString() throws Exception {
        int parameterIndex = 0;
        CallableStatement instance = null;

        String expResult = "";
//        String result = instance.getNString(parameterIndex);
//        assertEquals(expResult, result);

        // TODO review the generated test code and remove the default call to fail.
        stubTestResult();
    }

    /**
     * Test of getNCharacterStream method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetNCharacterStream() throws Exception {
        int parameterIndex = 0;
        CallableStatement instance = null;

        Reader expResult = null;
//        Reader result = instance.getNCharacterStream(parameterIndex);
//        assertEquals(expResult, result);

        // TODO review the generated test code and remove the default call to fail.
        stubTestResult();
    }

    /**
     * Test of getCharacterStream method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testGetCharacterStream() throws Exception {
        int parameterIndex = 0;
        CallableStatement instance = null;

        Reader expResult = null;
//        Reader result = instance.getCharacterStream(parameterIndex);
//        assertEquals(expResult, result);

        // TODO review the generated test code and remove the default call to fail.
        stubTestResult();
    }

    /**
     * Test of close method, of interface java.sql.CallableStatement
     * @throws java.lang.Exception
     */
    public void testClose() throws Exception {
        //in this suite, no test is necessary for this method
        JDBCCallableStatement instance = null;

//        instance.close();

        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
    }

    public static void main(java.lang.String[] argList) {
        junit.textui.TestRunner.run(suite());
    }

}
