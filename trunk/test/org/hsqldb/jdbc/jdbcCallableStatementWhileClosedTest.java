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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.CallableStatement;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.util.Calendar;
import java.util.HashMap;
import junit.framework.TestSuite;
import org.hsqldb.Trace;

// TODO:  See if this can be done reflectively.

/**
 *
 * @author boucherb@users
 */
public class jdbcCallableStatementWhileClosedTest extends JdbcTestCase {
    
    public jdbcCallableStatementWhileClosedTest(String testName) {
        super(testName);
    }
    
    protected void setUp() throws Exception {
        super.setUp();
    }
    
    protected void tearDown() throws Exception {
        super.tearDown();
    }
    
    public static TestSuite suite() {
        return new TestSuite(jdbcCallableStatementWhileClosedTest.class);
    }
    
    protected String getSql() {
        return "{?= call cast(1 as integer)}";
    }
    
    protected int getParameterType() {
        return java.sql.Types.INTEGER;
    }
    
    protected String getParameterTypeName() {
        return "INTEGER";
    }
    
    protected int getParameterScale() {
        return 0;
    }
    
    protected int getParameterIndex() {
        return 1;
    }
    
    protected String getParameterName() {
        return "@p0";
    }
    
    protected int getStatementClosedErrorCode() {
        return -Trace.STATEMENT_IS_CLOSED;
    }
    
    protected CallableStatement newClosedCall() throws Exception {
        CallableStatement instance = connectionFactory().prepareCall(
                getSql(), 
                newConnection());
        
        instance.close();
        
        return instance;
    }
    
    /**
     * Checks to ensure either sql feature not supported with sql state '0A...'
     * or sql exception with error code that indicates statement is closed.
     */
    protected void checkException(SQLException ex) {
        if (ex instanceof SQLFeatureNotSupportedException) {
            assertEquals("0A", ex.getSQLState().substring(0,2));
        } else {
            assertEquals(
                    "Error code is not 'statement closed' for exception: "
                    + ex.getMessage(),
                    getStatementClosedErrorCode(),
                    ex.getErrorCode());
        }
    }

    /**
     * Test of registerOutParameter method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testRegisterOutParameter() throws Exception {
        println("registerOutParameter");
        
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
     * Test of wasNull method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testWasNull() throws Exception {
        println("wasNull");
        
        CallableStatement instance = newClosedCall();
        
        try {
            instance.wasNull();
            fail("Allowed was null after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }
    
    /**
     * Test of getString method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetString() throws Exception {
        println("getString");
        
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
     * Test of getBoolean method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetBoolean() throws Exception {
        println("getBoolean");
        
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
     * Test of getByte method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetByte() throws Exception {
        println("getByte");
        
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
     * Test of getShort method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetShort() throws Exception {
        println("getShort");
        
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
     * Test of getInt method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetInt() throws Exception {
        println("getInt");
        
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
     * Test of getLong method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetLong() throws Exception {
        println("getLong");
        
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
     * Test of getFloat method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetFloat() throws Exception {
        println("getFloat");
        
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
     * Test of getDouble method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetDouble() throws Exception {
        println("getDouble");
        
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
     * Test of getBigDecimal method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetBigDecimal() throws Exception {
        println("getBigDecimal");
        
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
     * Test of getBytes method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetBytes() throws Exception {
        println("getBytes");
        
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
     * Test of getDate method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetDate() throws Exception {
        println("getDate");
        
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
     * Test of getTime method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetTime() throws Exception {
        println("getTime");
        
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
     * Test of getTimestamp method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetTimestamp() throws Exception {
        println("getTimestamp");
        
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
     * Test of getObject method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetObject() throws Exception {
        println("getObject");
        
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
     * Test of getRef method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetRef() throws Exception {
        println("getRef");
        
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
     * Test of getBlob method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetBlob() throws Exception {
        println("getBlob");
        
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
     * Test of getClob method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetClob() throws Exception {
        println("getClob");
        
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
     * Test of getArray method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetArray() throws Exception {
        println("getArray");
        
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
     * Test of getURL method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetURL() throws Exception {
        println("getURL");
        
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
     * Test of setURL method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetURL() throws Exception {
        println("setURL");
        
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
     * Test of setNull method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetNull() throws Exception {
        println("setNull");
        
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
     * Test of setBoolean method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetBoolean() throws Exception {
        println("setBoolean");
        
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
     * Test of setByte method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetByte() throws Exception {
        println("setByte");
        
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
     * Test of setShort method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetShort() throws Exception {
        println("setShort");
        
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
     * Test of setInt method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetInt() throws Exception {
        println("setInt");
        
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
     * Test of setLong method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetLong() throws Exception {
        println("setLong");
        
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
     * Test of setFloat method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetFloat() throws Exception {
        println("setFloat");
        
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
     * Test of setDouble method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetDouble() throws Exception {
        println("setDouble");
        
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
     * Test of setBigDecimal method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetBigDecimal() throws Exception {
        println("setBigDecimal");
        
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
     * Test of setString method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetString() throws Exception {
        println("setString");
        
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
     * Test of setBytes method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetBytes() throws Exception {
        println("setBytes");
        
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
     * Test of setDate method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetDate() throws Exception {
        println("setDate");
        
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
     * Test of setTime method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetTime() throws Exception {
        println("setTime");
        
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
     * Test of setTimestamp method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetTimestamp() throws Exception {
        println("setTimestamp");
        
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
     * Test of setAsciiStream method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetAsciiStream() throws Exception {
        println("setAsciiStream");
        
        CallableStatement instance = newClosedCall();
        
        try {
            instance.setAsciiStream(getParameterName(), null);
            fail("Allowed set ascii stream by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
        
        try {
            instance.setAsciiStream(getParameterIndex(), null);
            fail("Allowed set ascii stream by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }
    
    /**
     * Test of setBinaryStream method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetBinaryStream() throws Exception {
        println("setBinaryStream");
        
        CallableStatement instance = newClosedCall();
        
        try {
            instance.setBinaryStream(getParameterName(), null);
            fail("Allowed set binary stream by parameter name after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
        
        try {
            instance.setAsciiStream(getParameterIndex(), null);
            fail("Allowed set ascii stream by parameter index after close.");
        } catch (SQLException ex) {
            checkException(ex);
        }
    }
    
    /**
     * Test of setObject method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetObject() throws Exception {
        println("setObject");
        
        String parameterName = "";
        Object x = null;
        int targetSqlType = 0;
        int scale = 0;
        jdbcCallableStatement instance = null;
        
        instance.setObject(parameterName, x, targetSqlType, scale);
        
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    /**
     * Test of setCharacterStream method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetCharacterStream() throws Exception {
        println("setCharacterStream");
        
        String parameterName = "";
        Reader reader = null;
        int length = 0;
        jdbcCallableStatement instance = null;
        
        instance.setCharacterStream(parameterName, reader, length);
        
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    /**
     * Test of getRowId method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetRowId() throws Exception {
        println("getRowId");
        
        int parameterIndex = 0;
        jdbcCallableStatement instance = null;
        
        RowId expResult = null;
        RowId result = instance.getRowId(parameterIndex);
        assertEquals(expResult, result);
        
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    /**
     * Test of setRowId method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetRowId() throws Exception {
        println("setRowId");
        
        String parameterName = "";
        RowId x = null;
        jdbcCallableStatement instance = null;
        
        instance.setRowId(parameterName, x);
        
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    /**
     * Test of setNString method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetNString() throws Exception {
        println("setNString");
        
        String parameterName = "";
        String value = "";
        jdbcCallableStatement instance = null;
        
        instance.setNString(parameterName, value);
        
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    /**
     * Test of setNCharacterStream method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetNCharacterStream() throws Exception {
        println("setNCharacterStream");
        
        String parameterName = "";
        Reader value = null;
        long length = 0L;
        jdbcCallableStatement instance = null;
        
        instance.setNCharacterStream(parameterName, value, length);
        
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    /**
     * Test of setNClob method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetNClob() throws Exception {
        println("setNClob");
        
        String parameterName = "";
        NClob value = null;
        jdbcCallableStatement instance = null;
        
        instance.setNClob(parameterName, value);
        
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    /**
     * Test of setClob method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetClob() throws Exception {
        println("setClob");
        
        String parameterName = "";
        Reader reader = null;
        long length = 0L;
        jdbcCallableStatement instance = null;
        
        instance.setClob(parameterName, reader, length);
        
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    /**
     * Test of setBlob method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetBlob() throws Exception {
        println("setBlob");
        
        String parameterName = "";
        InputStream inputStream = null;
        long length = 0L;
        jdbcCallableStatement instance = null;
        
        instance.setBlob(parameterName, inputStream, length);
        
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    /**
     * Test of getNClob method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetNClob() throws Exception {
        println("getNClob");
        
        int parameterIndex = 0;
        jdbcCallableStatement instance = null;
        
        NClob expResult = null;
        NClob result = instance.getNClob(parameterIndex);
        assertEquals(expResult, result);
        
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    /**
     * Test of setSQLXML method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetSQLXML() throws Exception {
        println("setSQLXML");
        
        String parameterName = "";
        SQLXML xmlObject = null;
        jdbcCallableStatement instance = null;
        
        instance.setSQLXML(parameterName, xmlObject);
        
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    /**
     * Test of getSQLXML method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetSQLXML() throws Exception {
        println("getSQLXML");
        
        int parameterIndex = 0;
        jdbcCallableStatement instance = null;
        
        SQLXML expResult = null;
        SQLXML result = instance.getSQLXML(parameterIndex);
        assertEquals(expResult, result);
        
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    /**
     * Test of getNString method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetNString() throws Exception {
        println("getNString");
        
        int parameterIndex = 0;
        jdbcCallableStatement instance = null;
        
        String expResult = "";
        String result = instance.getNString(parameterIndex);
        assertEquals(expResult, result);
        
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    /**
     * Test of getNCharacterStream method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetNCharacterStream() throws Exception {
        println("getNCharacterStream");
        
        int parameterIndex = 0;
        jdbcCallableStatement instance = null;
        
        Reader expResult = null;
        Reader result = instance.getNCharacterStream(parameterIndex);
        assertEquals(expResult, result);
        
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    /**
     * Test of getCharacterStream method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetCharacterStream() throws Exception {
        println("getCharacterStream");
        
        int parameterIndex = 0;
        jdbcCallableStatement instance = null;
        
        Reader expResult = null;
        Reader result = instance.getCharacterStream(parameterIndex);
        assertEquals(expResult, result);
        
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    /**
     * Test of close method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testClose() throws Exception {
        println("close");
        
        jdbcCallableStatement instance = null;
        
        instance.close();
        
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    public static void main(java.lang.String[] argList) {
        junit.textui.TestRunner.run(suite());
    }
    
}
