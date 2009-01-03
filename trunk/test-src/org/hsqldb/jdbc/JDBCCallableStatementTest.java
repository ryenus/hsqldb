/* Copyright (c) 2001-2009, The HSQL Development Group
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

import java.io.ByteArrayInputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.Types;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test of class org.hsqldb.jdbc.JDBCCallableStatement.
 *
 * @author boucherb@users
 */
public class JDBCCallableStatementTest extends JdbcTestCase {

    public JDBCCallableStatementTest(String testName) {
        super(testName);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(JDBCCallableStatementTest.class);

        return suite;
    }

    protected void setUpDualTable() throws Exception {
        executeScript("setup-dual-table.sql");
    }

    protected CallableStatement prepareCall(String call) throws Exception {
        return connectionFactory().prepareCall(call, newConnection());
    }

    protected boolean isTestOutParameters()
    {
        return super.getBooleanProperty(
                "test.callable.statement.out.parameters",
                true);
    }

    void setObjectTest(String typeName, Object x, int type) throws Exception {
        CallableStatement stmt
                = prepareCall("select cast(? as " + typeName + ") from dual");

        stmt.setObject("@p1", x, type);

        ResultSet rs = stmt.executeQuery();

        rs.next();

        Object result = rs.getObject(1);

        if (x instanceof Number) {
            assertEquals(((Number)x).doubleValue(), ((Number)result).doubleValue());
        } else if (x != null && x.getClass().isArray()) {
            assertJavaArrayEquals(x, result);
        } else {
            assertEquals(x, result);
        }
    }

    protected CallableStatement prepRegAndExec(String call,
                                                   int index,
                                                   int type) throws Exception {
        CallableStatement stmt = prepareCall(call);

        stmt.registerOutParameter(index, type);
        stmt.execute();

        return stmt;
    }

    /**
     * Test of close method, of inteface java.sql.CallableStatement.
     */
    public void testClose() throws Exception {
        println("close");

        CallableStatement stmt = prepareCall("call log10(?);");

        stmt.close();

        assertEquals("stmt.isClosed()", true, stmt.isClosed());
    }

    /**
     * Test of registerOutParameter method, of inteface java.sql.CallableStatement.
     */
    public void testRegisterOutParameter() throws Exception {
        println("registerOutParameter");

        if (!isTestOutParameters())
        {
            return;
        }

        int parameterIndex = 0;
        int sqlType = Types.INTEGER;
        CallableStatement stmt = prepareCall("{?= call abs(-5)}");

        try {
            stmt.registerOutParameter(parameterIndex, sqlType);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of wasNull method, of inteface java.sql.CallableStatement.
     */
    public void testWasNull() throws Exception {
        println("wasNull");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt = prepareCall("{?= call cast(null as integer)}");

        try {
            boolean expResult = true;

            stmt.registerOutParameter(1, Types.INTEGER);
            stmt.execute();
            stmt.getInt(1);

            boolean result = stmt.wasNull();

            assertEquals(expResult, result);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of getString method, of inteface java.sql.CallableStatement.
     */
    public void testGetString() throws Exception {
        println("getString");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        String            expResult = "getString";
        String            result    = null;

        try {
            stmt = prepRegAndExec("{?= call cast('getString' as varchar)}",
                                  1,
                                  Types.VARCHAR);

            result = stmt.getString(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of getBoolean method, of inteface java.sql.CallableStatement.
     */
    public void testGetBoolean() throws Exception {
        println("getBoolean");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        boolean           expResult = true;
        boolean           result    = false;

        try {
            stmt = prepRegAndExec("{?= call cast(true as boolean)}",
                                  1,
                                  Types.BOOLEAN);

            result = stmt.getBoolean(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of getByte method, of inteface java.sql.CallableStatement.
     */
    public void testGetByte() throws Exception {
        println("getByte");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        byte              expResult = (byte) 1;
        byte              result    = (byte) 0;

        try {
            stmt = prepRegAndExec("{?= call cast(1 as tinyint)}",
                                  1,
                                  Types.TINYINT);

            result = stmt.getByte(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of getShort method, of inteface java.sql.CallableStatement.
     */
    public void testGetShort() throws Exception {
        println("getShort");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        short             expResult = (short) 1;
        short             result    = (short) 0;

        try {
            stmt = prepRegAndExec("{?= call cast(1 as smallint)}",
                                  1,
                                  Types.INTEGER);

            result = stmt.getShort(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of getInt method, of inteface java.sql.CallableStatement.
     */
    public void testGetInt() throws Exception {
        println("getInt");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        int               expResult = 1;
        int               result    = 0;

        try {
            stmt = prepRegAndExec("{?= call cast(1 as integer)}",
                                  1,
                                  Types.INTEGER);

            result = stmt.getInt(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of getLong method, of inteface java.sql.CallableStatement.
     */
    public void testGetLong() throws Exception {
        println("getLong");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        long              expResult = 1;
        long              result    = 0;

        try {
            stmt = prepRegAndExec("{?= call cast(1 as bigint)}",
                                  1,
                                  Types.BIGINT);

            result = stmt.getLong(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of getFloat method, of inteface java.sql.CallableStatement.
     */
    public void testGetFloat() throws Exception {
        println("getFloat");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        float             expResult = 1F;
        float             result    = 0F;

        try {
            stmt = prepRegAndExec("{?= call cast(1 as real)}",
                                  1,
                                  Types.REAL);

            result = stmt.getFloat(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of getDouble method, of inteface java.sql.CallableStatement.
     */
    public void testGetDouble() throws Exception {
        println("getDouble");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        double            expResult = 1D;
        double            result    = 0D;

        try {
            stmt = prepRegAndExec("{?= call cast(1 as double)}",
                                  1,
                                  Types.DOUBLE);

            result = stmt.getDouble(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);

        try {
            stmt = prepRegAndExec("{?= call cast(1 as float)}",
                                  1,
                                  Types.DOUBLE);

            result = stmt.getDouble(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of getBigDecimal method, of inteface java.sql.CallableStatement.
     */
    public void testGetBigDecimal() throws Exception {
        println("getBigDecimal");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        BigDecimal        expResult = new BigDecimal("1.00");
        BigDecimal        result    = null;

        try {
            stmt = prepRegAndExec("{?= call cast(1.00 as decimal(3,2))}",
                                  1,
                                  Types.DECIMAL);

            result = stmt.getBigDecimal(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of getBytes method, of inteface java.sql.CallableStatement.
     */
    public void testGetBytes() throws Exception {
        println("getBytes");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        byte[]            expResult = new byte[]{(byte) 0xca,
                                                 (byte) 0xfe,
                                                 (byte) 0xba,
                                                 (byte) 0xbe};
        byte[]            result    = null;

        try {
            stmt = prepRegAndExec("{?= call cast(X'cafebabe' as binary(4))}",
                                  1,
                                  Types.BINARY);

            result = stmt.getBytes(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of getDate method, of inteface java.sql.CallableStatement.
     */
    public void testGetDate() throws Exception {
        println("getDate");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        Date              expResult = Date.valueOf("2005-12-13");
        Date              result    = null;

        try {
            stmt = prepRegAndExec("{?= call cast('2005-12-13' as date)}",
                                  1,
                                  Types.DATE);

            result = stmt.getDate(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of getTime method, of inteface java.sql.CallableStatement.
     */
    public void testGetTime() throws Exception {
        println("getTime");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        Time              expResult = Time.valueOf("11:12:02");
        Time              result    = null;

        try {
            stmt = prepRegAndExec("{?= call cast('11:12:02' as time)}",
                                  1,
                                  Types.TIME);

            result = stmt.getTime(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of getTimestamp method, of inteface java.sql.CallableStatement.
     */
    public void testGetTimestamp() throws Exception {
        println("getTimestamp");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        Timestamp         expResult = Timestamp.valueOf("2005-12-13 11:23:02.1234");
        Timestamp         result    = null;

        try {
            stmt = prepRegAndExec("{?= call cast('2005-12-13 11:23:02.1234' as timestamp(6))}",
                                  1,
                                  Types.TIMESTAMP);

            result = stmt.getTimestamp(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of getObject method, of inteface java.sql.CallableStatement.
     */
    public void testGetObject() throws Exception {
        println("getObject");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        byte[]            expResult = new byte[]{(byte) 0xca, (byte) 0xfe};
        byte[]            result    = null;
        Object            temp      = null;

        try {
            stmt = prepRegAndExec("{?= call cast(X'cafebabe' as object)}",
                                  1,
                                  Types.OTHER);

            temp = stmt.getObject(1);
            result = (byte[]) temp;
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of getRef method, of inteface java.sql.CallableStatement.
     */
    public void testGetRef() throws Exception {
        println("getRef");

        if (!isTestOutParameters())
        {
            return;
        }

        if (!getBooleanProperty("test.types.ref", true))
        {
            return;
        }

        fail("TODO: The test case is empty.");

    }

    /**
     * Test of getBlob method, of of inteface java.sql.CallableStatement.
     */
    public void testGetBlob() throws Exception {
        println("getBlob");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        byte[]            expResult = new byte[]{(byte) 0xca, (byte) 0xfe};
        Blob              result    = null;

        try {
            stmt = prepRegAndExec("{?= call cast(X'cafe' as binary(2))}",
                                  1,
                                  Types.BINARY);

            result = stmt.getBlob(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result.getBytes(1, (int) result.length()));
    }

    /**
     * Test of getClob method, of inteface java.sql.CallableStatement.
     */
    public void testGetClob() throws Exception {
        println("getClob");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        String            expResult = "getString";
        Clob              result    = null;

        try {
            stmt = prepRegAndExec("{?= call cast('getClob' as longvarchar)}",
                                  1,
                                  Types.LONGVARCHAR);

            result = stmt.getClob(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result.getSubString(1, (int) result.length()));
    }

    /**
     * Test of getArray method, of inteface java.sql.CallableStatement.
     */
    public void testGetArray() throws Exception {
        println("getArray");

        if (!isTestOutParameters())
        {
            return;
        }

        if (!getBooleanProperty("test.types.array", true))
        {
            return;
        }

        fail("TODO: The test case is empty.");
    }

    /**
     * Test of getURL method, of inteface java.sql.CallableStatement.
     */
    public void testGetURL() throws Exception {
        println("getURL");

        if (!isTestOutParameters())
        {
            return;
        }

        if (!getBooleanProperty("test.types.datalink", true))
        {
            return;
        }

        fail("TODO: The test case is empty.");
    }

    /**
     * Test of setURL method, of inteface java.sql.CallableStatement.
     */
    public void testSetURL() throws Exception {
        println("setURL");

        if (!getBooleanProperty("test.types.datalink", true))
        {
            return;
        }

        setUpDualTable();
        prepareCall("select cast(? as object) from dual")
            .setURL("@p1", new java.net.URL("http://localhost"));
    }

    /**
     * Test of setNull method, of inteface java.sql.CallableStatement.
     */
    public void testSetNull() throws Exception {
        println("setNull");

        setUpDualTable();
        prepareCall("select cast(? as varchar) from dual")
            .setNull("@p1", Types.VARCHAR);
    }

    /**
     * Test of setBoolean method, of inteface java.sql.CallableStatement.
     */
    public void testSetBoolean() throws Exception {
        println("setBoolean");

        setUpDualTable();

        java.sql.CallableStatement stmt
                = prepareCall("select ((1=1) or ?) from dual");

        stmt.setBoolean("@p1", true);
    }

    /**
     * Test of setByte method, of inteface java.sql.CallableStatement.
     */
    public void testSetByte() throws Exception {
        println("setByte");

        setUpDualTable();
        prepareCall("select cast(? as tinyint) from dual")
            .setByte("@p1", (byte) 1);
    }

    /**
     * Test of setShort method, of inteface java.sql.CallableStatement.
     */
    public void testSetShort() throws Exception {
        println("setShort");

        setUpDualTable();
        prepareCall("select cast(? as smallint) from dual")
            .setShort("@p1", (short) 1);
    }

    /**
     * Test of setInt method, of inteface java.sql.CallableStatement.
     */
    public void testSetInt() throws Exception {
        println("setInt");

        setUpDualTable();
        prepareCall("select cast(? as integer) from dual").setInt("@p1", 1);
    }

    /**
     * Test of setLong method, of inteface java.sql.CallableStatement.
     */
    public void testSetLong() throws Exception {
        println("setLong");

        setUpDualTable();
        prepareCall("select cast(? as bigint) from dual").setLong("@p1", 1);
    }

    /**
     * Test of setFloat method, of inteface java.sql.CallableStatement.
     */
    public void testSetFloat() throws Exception {
        println("setFloat");

        setUpDualTable();
        //
        prepareCall("select cast(? as real) from dual")
            .setFloat("@p1", Float.NEGATIVE_INFINITY);
        prepareCall("select cast(? as real) from dual")
            .setFloat("@p1", Float.MIN_VALUE);
        prepareCall("select cast(? as real) from dual")
            .setFloat("@p1", Float.NaN);
        prepareCall("select cast(? as real) from dual")
            .setFloat("@p1", Float.MAX_VALUE);
        prepareCall("select cast(? as real) from dual")
            .setFloat("@p1", Float.POSITIVE_INFINITY);
    }

    /**
     * Test of setDouble method, of inteface java.sql.CallableStatement.
     */
    public void testSetDouble() throws Exception {
        println("setDouble");

        setUpDualTable();
        //
        prepareCall("select cast(? as float) from dual")
            .setDouble("@p1", Double.NEGATIVE_INFINITY);
        prepareCall("select cast(? as float) from dual")
            .setDouble("@p1", Double.MIN_VALUE);
        prepareCall("select cast(? as float) from dual")
            .setDouble("@p1", Double.NaN);
        prepareCall("select cast(? as float) from dual")
            .setDouble("@p1", Double.MAX_VALUE);
        prepareCall("select cast(? as float) from dual")
            .setDouble("@p1", Double.POSITIVE_INFINITY);
        //
        prepareCall("select cast(? as double) from dual")
            .setDouble("@p1", Double.NEGATIVE_INFINITY);
        prepareCall("select cast(? as double) from dual")
            .setDouble("@p1", Double.MIN_VALUE);
        prepareCall("select cast(? as double) from dual")
            .setDouble("@p1", Double.NaN);
        prepareCall("select cast(? as double) from dual")
            .setDouble("@p1", Double.MAX_VALUE);
        prepareCall("select cast(? as double) from dual")
            .setDouble("@p1", Double.POSITIVE_INFINITY);
    }

    /**
     * Test of setBigDecimal method, of inteface java.sql.CallableStatement.
     */
    public void testSetBigDecimal() throws Exception {
        println("setBigDecimal");

        setUpDualTable();
        prepareCall("select cast(? as decimal) from dual")
            .setBigDecimal("@p1", new BigDecimal("1.23456789"));
    }

    /**
     * Test of setString method, of inteface java.sql.CallableStatement.
     */
    public void testSetString() throws Exception {
        println("setString");

        setUpDualTable();
        prepareCall("select cast(? as varchar) from dual")
            .setString("@p1", "setString");
    }

    /**
     * Test of setBytes method, of inteface java.sql.CallableStatement.
     */
    public void testSetBytes() throws Exception {
        println("setBytes");

        setUpDualTable();
        prepareCall("select (X'cafebabe' || ?) from dual")
            .setBytes("@p1", new byte[10]);
    }

    /**
     * Test of setDate method, of inteface java.sql.CallableStatement.
     */
    public void testSetDate() throws Exception {
        println("setDate");

        setUpDualTable();
        prepareCall("select cast(? as date) from dual")
            .setDate("@p1", Date.valueOf("2005-12-13"));
    }

    /**
     * Test of setTime method, of inteface java.sql.CallableStatement.
     */
    public void testSetTime() throws Exception {
        println("setTime");

        setUpDualTable();
        prepareCall("select cast(? as time) from dual")
            .setTime("@p1", Time.valueOf("11:23:02"));
    }

    /**
     * Test of setTimestamp method, of inteface java.sql.CallableStatement.
     */
    public void testSetTimestamp() throws Exception {
        println("setTimestamp");

        setUpDualTable();
        prepareCall("select cast(? as timestamp) from dual")
            .setTimestamp("@p1", Timestamp.valueOf("2005-12-13 11:23:02.1234"));
    }

    /**
     * Test of setAsciiStream method, of inteface java.sql.CallableStatement.
     */
    public void testSetAsciiStream() throws Exception {
        println("setAsciiStream");

        setUpDualTable();

        byte[]               bytes = "2005-12-13 11:23:02.1234".getBytes();
        ByteArrayInputStream bais  = new ByteArrayInputStream(bytes);

        prepareCall("select cast(? as varchar) from dual")
            .setAsciiStream("@p1", bais, bytes.length);
    }

    /**
     * Test of setBinaryStream method, of inteface java.sql.CallableStatement.
     */
    public void testSetBinaryStream() throws Exception {
        println("setBinaryStream");

        setUpDualTable();

        byte[]               bytes = "2005-12-13 11:23:02.1234".getBytes();
        ByteArrayInputStream bais  = new ByteArrayInputStream(bytes);

        prepareCall("select (X'cafebabe' || ?) from dual")
            .setBinaryStream("@p1", bais, bytes.length);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_BIGINT() throws Exception
    {
        println("setObject - BIGINT");

        setUpDualTable();

        setObjectTest("bigint", new Long(Long.MAX_VALUE), Types.BIGINT);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_BINARY() throws Exception
    {
        println("setObject - BINARY");

        setUpDualTable();

        byte[] bytes = "setObject_binary".getBytes();

        setObjectTest("binary(" + bytes.length + ")", bytes, Types.BINARY);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_BOOLEAN() throws Exception
    {
        println("setObject - BOOLEAN");

        setUpDualTable();

        setObjectTest("boolean",  Boolean.TRUE, Types.BOOLEAN);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_CHAR() throws Exception
    {
        println("setObject - CHAR");

        setUpDualTable();

        setObjectTest("char", "setObject  ", Types.CHAR);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_DATE() throws Exception
    {
        println("setObject - DATE");

        setUpDualTable();

        setObjectTest("date", Date.valueOf("2005-12-13"), Types.DATE);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_DECIMAL() throws Exception
    {
        println("setObject - DECIMAL");

        setUpDualTable();

        setObjectTest("decimal(10,9)", new BigDecimal(1.123456789), Types.DECIMAL);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_DOUBLE() throws Exception
    {
        println("setObject - DOUBLE");

        setUpDualTable();

        setObjectTest("double", new Double(Double.MAX_VALUE), Types.DOUBLE);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_FLOAT() throws Exception
    {
        println("setObject - FLOAT");

        setUpDualTable();

        setObjectTest("float", new Double(Double.MAX_VALUE), Types.FLOAT);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_INTEGER() throws Exception
    {
        println("setObject - INTEGER");

        setUpDualTable();

        setObjectTest("integer", new Integer(Integer.MIN_VALUE), Types.INTEGER);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_LONGVARBINARY() throws Exception
    {
        println("setObject - LONGVARBINARY");

        setUpDualTable();

        byte[] bytes = "setObject_longvarbinary".getBytes();

        setObjectTest(
                "longvarbinary(" + bytes.length + ")",
                bytes,
                Types.LONGVARBINARY);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_LONGVARCHAR() throws Exception
    {
        println("setObject - LONGVARCHAR");

        setUpDualTable();

        setObjectTest(
                "longvarchar",
                "setObject_longvarchar",
                Types.LONGVARCHAR);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_OTHER_boolean_array() throws Exception
    {
        println("setObject - OTHER - boolean[]");

        setUpDualTable();

        setObjectTest("object", new boolean[10], Types.OTHER);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_OTHER_byte_array() throws Exception
    {
        println("setObject - OTHER - byte[]");

        setUpDualTable();

        setObjectTest("object", new byte[10], Types.OTHER);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_OTHER_short_array() throws Exception
    {
        println("setObject - OTHER - short[]");

        setUpDualTable();

        setObjectTest("object", new short[10], Types.OTHER);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_OTHER_char_array() throws Exception
    {
        println("setObject - OTHER - char[]");

        setUpDualTable();

        setObjectTest("object", new char[10], Types.OTHER);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_OTHER_int_array() throws Exception
    {
        println("setObject - OTHER - int[]");

        setUpDualTable();

        setObjectTest("object", new int[10], Types.OTHER);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_OTHER_long_array() throws Exception
    {
        println("setObject - OTHER - long[]");

        setUpDualTable();

        setObjectTest("object", new long[10], Types.OTHER);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_OTHER_float_array() throws Exception
    {
        println("setObject - OTHER - float[]");

        setUpDualTable();

        setObjectTest("object", new float[10], Types.OTHER);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_OTHER_double_array() throws Exception
    {
        println("setObject - OTHER - double[]");

        setUpDualTable();

        setObjectTest("object", new double[10], Types.OTHER);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_OTHER_String_array() throws Exception
    {
        println("setObject - OTHER - String[]");

        setUpDualTable();

        setObjectTest("object", new String[10], Types.OTHER);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_OTHER_Boolean_array() throws Exception
    {
        println("setObject - OTHER - Boolean[]");

        setUpDualTable();

        setObjectTest("object", new Boolean[10], Types.OTHER);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_OTHER_Byte_array() throws Exception
    {
        println("setObject - OTHER - Byte[]");

        setUpDualTable();

        setObjectTest("object", new Byte[10], Types.OTHER);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_OTHER_Short_array() throws Exception
    {
        println("setObject - OTHER - Short[]");

        setUpDualTable();

        setObjectTest("object", new Short[10], Types.OTHER);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_OTHER_Character_array() throws Exception
    {
        println("setObject - OTHER - Character[]");

        setUpDualTable();

        setObjectTest("object", new Character[10], Types.OTHER);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_OTHER_Integer_array() throws Exception
    {
        println("setObject - OTHER - Integer[]");

        setUpDualTable();

        setObjectTest("object", new Integer[10], Types.OTHER);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_OTHER_Long_array() throws Exception
    {
        println("setObject - OTHER - Long[]");

        setUpDualTable();

        setObjectTest("object", new Long[10], Types.OTHER);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_OTHER_Float_array() throws Exception
    {
        println("setObject - OTHER - Float[]");

        setUpDualTable();

        setObjectTest("object", new Float[10], Types.OTHER);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_OTHER_Double_array() throws Exception
    {
        println("setObject - OTHER - Double[]");

        setUpDualTable();

        setObjectTest("object", new Double[10], Types.OTHER);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_REAL() throws Exception
    {
        println("setObject - REAL");

        setUpDualTable();

        setObjectTest("real", new Float(Float.MAX_VALUE), Types.REAL);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_SMALLINT() throws Exception
    {
        println("setObject - SMALLINT");

        setUpDualTable();

        setObjectTest("smallint", new Short(Short.MAX_VALUE), Types.SMALLINT);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_TIME() throws Exception
    {
        println("setObject - TIME");

        setUpDualTable();

        setObjectTest("time", Time.valueOf("1:23:47"), Types.TIME);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_TIMESTAMP() throws Exception
    {
        println("setObject - TIMESTAMP");

        setUpDualTable();

        setObjectTest(
                "timestamp(6)",
                Timestamp.valueOf("2005-12-13 01:23:47.123456"),
                Types.TIMESTAMP);

        setObjectTest(
                "timestamp(0)",
                Timestamp.valueOf("2005-12-13 01:23:47"),
                Types.TIMESTAMP);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_TINYINT() throws Exception
    {
        println("setObject - TINYINT");

        setUpDualTable();

        setObjectTest("tinyint", new Byte(Byte.MAX_VALUE), Types.TINYINT);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_VARBINARY() throws Exception
    {
        println("setObject - VARBINARY");

        setUpDualTable();

        byte[] bytes = "setObject_varbinary".getBytes();

        setObjectTest(
                "varbinary(" + bytes.length + ")",
                bytes,
                Types.VARBINARY);
    }

    /**
     * Test of setObject method, of inteface java.sql.CallableStatement.
     */
    public void testSetObject_VARCHAR() throws Exception
    {
        println("setObject - VARCHAR");

        setUpDualTable();

        setObjectTest("varchar", "setObject", Types.VARCHAR);
    }

    /**
     * Test of setCharacterStream method, of inteface java.sql.CallableStatement.
     */
    public void testSetCharacterStream() throws Exception {
        println("setCharacterStream");

        setUpDualTable();
        prepareCall("select cast(? as varchar) from dual")
            .setCharacterStream("@p1",
                                new java.io.StringReader("setCharacterStream"),
                                "setCharacterStream".length());
    }

    /**
     * Test of getRowId method, of inteface java.sql.CallableStatement.
     */
    public void testGetRowId() throws Exception {
        println("getRowId");

        if (!isTestOutParameters())
        {
            return;
        }

        if (!getBooleanProperty("test.types.rowid", true))
        {
            return;
        }

        fail("TODO: The test case is empty.");
    }

    /**
     * Test of setRowId method, of inteface java.sql.CallableStatement..
     */
    public void testSetRowId() throws Exception {
        println("setRowId");

        if (!getBooleanProperty("test.types.rowid", true))
        {
            return;
        }

        fail("TODO: The test case is empty.");
    }

    /**
     * Test of setNString method, of inteface java.sql.CallableStatement.
     */
    public void testSetNString() throws Exception {
        println("setNString");

        setUpDualTable();
        prepareCall("select cast(? as varchar) from dual")
            .setNString("@p1", "setString");
    }

    /**
     * Test of setNCharacterStream method, of inteface java.sql.CallableStatement.
     */
    public void testSetNCharacterStream() throws Exception {
        println("setNCharacterStream");

        setUpDualTable();
        prepareCall("select cast(? as varchar) from dual")
            .setNCharacterStream("@p1",
                                new java.io.StringReader("setCharacterStream"),
                                "setCharacterStream".length());
    }

    /**
     * Test of setNClob method, of inteface java.sql.CallableStatement.
     */
    public void testSetNClob() throws Exception {
        println("setNClob");

        setUpDualTable();
        prepareCall("select cast(? as varchar) from dual")
            .setNClob("@p1", new JDBCNClob("setCharacterStream"));
    }

    /**
     * Test of setClob method, of inteface java.sql.CallableStatement.
     */
    public void testSetClob() throws Exception {
        println("setClob");

        setUpDualTable();
        prepareCall("select cast(? as varchar) from dual")
            .setClob("@p1", new JDBCClob("setCharacterStream"));
    }

    /**
     * Test of setBlob method, of inteface java.sql.CallableStatement.
     */
    public void testSetBlob() throws Exception {
        println("setBlob");

        setUpDualTable();
        prepareCall("select cast(? as varchar) from dual")
            .setBlob("@p1", new JDBCBlob("setCharacterStream".getBytes()));
    }

    /**
     * Test of getNClob method, of inteface java.sql.CallableStatement.
     */
    public void testGetNClob() throws Exception {
        println("getNClob");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        NClob             expResult = new JDBCNClob("getString");
        NClob             result    = null;

        try {
            stmt = prepRegAndExec("{?= call cast('getString' as longvarchar)}",
                                  1,
                                  Types.LONGVARCHAR);

            result = stmt.getNClob(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of setSQLXML method, of inteface java.sql.CallableStatement.
     */
    public void testSetSQLXML() throws Exception {
        println("setSQLXML");

        if (!getBooleanProperty("test.types.sqlxml", true))
        {
            return;
        }

        fail("TODO: The test case is empty.");
    }

    /**
     * Test of getSQLXML method, of inteface java.sql.CallableStatement.
     */
    public void testGetSQLXML() throws Exception {
        println("getSQLXML");

        if (!isTestOutParameters())
        {
            return;
        }

        if (!getBooleanProperty("test.types.sqlxml", true))
        {
            return;
        }

        fail("TODO: The test case is empty.");
    }

    /**
     * Test of getNString method, of inteface java.sql.CallableStatement.
     */
    public void testGetNString() throws Exception {
        println("getNString");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        String            expResult = "getString";
        String            result    = null;

        try {
            stmt = prepRegAndExec("{?= call cast('getString' as varchar)}",
                                  1,
                                  Types.VARCHAR);

            result = stmt.getNString(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of getNCharacterStream method, of inteface java.sql.CallableStatement.
     */
    public void testGetNCharacterStream() throws Exception {
        println("getNCharacterStream");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        String            expResult = "getString";
        Reader            result    = null;

        try {
            stmt = prepRegAndExec("{?= call cast('getString' as varchar)}",
                                  1,
                                  Types.VARCHAR);

            result = stmt.getNCharacterStream(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    /**
     * Test of getCharacterStream method, of inteface java.sql.CallableStatement.
     */
    public void testGetCharacterStream() throws Exception {
        println("getCharacterStream");

        if (!isTestOutParameters())
        {
            return;
        }

        CallableStatement stmt;
        String            expResult = "getString";
        Reader            result    = null;

        try {
            stmt = prepRegAndExec("{?= call cast('getString' as varchar)}",
                                  1,
                                  Types.VARCHAR);

            result = stmt.getCharacterStream(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    /**
     *
     * @param argList
     */
    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }

}
