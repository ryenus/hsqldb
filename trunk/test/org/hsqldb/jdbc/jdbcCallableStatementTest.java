/* Copyright (c) 2001-2006, The HSQL Development Group
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
 * Test of class org.hsqldb.jdbc.jdbcCallableStatement.
 *
 * @author boucherb@users
 */
public class jdbcCallableStatementTest extends JdbcTestCase {

    public jdbcCallableStatementTest(String testName) {
        super(testName);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(jdbcCallableStatementTest.class);

        return suite;
    }

    protected void setUpDualTable() throws Exception {
        executeScript("setup-dual-table.sql");
    }

    protected CallableStatement prepareCall(String call) throws Exception {
        return newConnection().prepareCall(call);
    }

    void setObjectTest(String typeName, Object x, int type) throws Exception {
        CallableStatement stmt = prepareCall("select cast(? as " + typeName + ") from dual");

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
     * Test of close method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testClose() throws Exception {
        System.out.println("close");

        CallableStatement stmt = prepareCall(";");

        stmt.close();

        assertEquals("stmt.isClosed()", true, stmt.isClosed());
    }

    /**
     * Test of registerOutParameter method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testRegisterOutParameter() throws Exception {
        System.out.println("registerOutParameter");

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
     * Test of wasNull method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testWasNull() throws Exception {
        System.out.println("wasNull");

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
     * Test of getString method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetString() throws Exception {
        System.out.println("getString");

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
     * Test of getBoolean method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetBoolean() throws Exception {
        System.out.println("getBoolean");

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
     * Test of getByte method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetByte() throws Exception {
        System.out.println("getByte");

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
     * Test of getShort method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetShort() throws Exception {
        System.out.println("getShort");

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
     * Test of getInt method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetInt() throws Exception {
        System.out.println("getInt");

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
     * Test of getLong method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetLong() throws Exception {
        System.out.println("getLong");

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
     * Test of getFloat method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetFloat() throws Exception {
        System.out.println("getFloat");

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
     * Test of getDouble method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetDouble() throws Exception {
        System.out.println("getDouble");

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
     * Test of getBigDecimal method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetBigDecimal() throws Exception {
        System.out.println("getBigDecimal");

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
     * Test of getBytes method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetBytes() throws Exception {
        System.out.println("getBytes");

        CallableStatement stmt;
        byte[]            expResult = new byte[]{(byte) 0xca,
                                                 (byte) 0xfe,
                                                 (byte) 0xba,
                                                 (byte) 0xbe};
        byte[]            result    = null;

        try {
            stmt = prepRegAndExec("{?= call cast('cafebabe' as binary)}",
                                  1,
                                  Types.BINARY);

            result = stmt.getBytes(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of getDate method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetDate() throws Exception {
        System.out.println("getDate");

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
     * Test of getTime method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetTime() throws Exception {
        System.out.println("getTime");

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
     * Test of getTimestamp method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetTimestamp() throws Exception {
        System.out.println("getTimestamp");

        CallableStatement stmt;
        Timestamp         expResult = Timestamp.valueOf("2005-12-13 11:23:02.1234");
        Timestamp         result    = null;

        try {
            stmt = prepRegAndExec("{?= call cast('2005-12-13 11:23:02.1234' as timestamp)}",
                                  1,
                                  Types.TIMESTAMP);

            result = stmt.getTimestamp(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of getObject method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetObject() throws Exception {
        System.out.println("getObject");

        CallableStatement stmt;
        byte[]            expResult = new byte[]{(byte) 0xca, (byte) 0xfe};
        byte[]            result    = null;
        Object            temp      = null;

        try {
            stmt = prepRegAndExec("{?= call cast(cast('cafe' as binary) as object)}",
                                  1,
                                  Types.DATE);

            temp = stmt.getObject(1);
            result = (byte[]) temp;
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result);
    }

    /**
     * Test of getRef method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetRef() throws Exception {
        System.out.println("getRef");

        // TODO add your test code below by replacing the default call to fail.
        fail("TODO: The test case is empty.");
    }

    /**
     * Test of getBlob method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetBlob() throws Exception {
        System.out.println("getBlob");

        CallableStatement stmt;
        byte[]            expResult = new byte[]{(byte) 0xca, (byte) 0xfe};
        Blob              result    = null;

        try {
            stmt = prepRegAndExec("{?= call cast('cafe' as binary)}",
                                  1,
                                  Types.BINARY);

            result = stmt.getBlob(1);
        } catch (Exception ex) {
            fail(ex.getMessage());
        }

        assertEquals(expResult, result.getBytes(1, (int) result.length()));
    }

    /**
     * Test of getClob method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetClob() throws Exception {
        System.out.println("getClob");

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
     * Test of getArray method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetArray() throws Exception {
        System.out.println("getArray");

        fail("TODO: The test case is empty.");
    }

    /**
     * Test of getURL method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetURL() throws Exception {
        System.out.println("getURL");

        fail("TODO: The test case is empty.");
    }

    /**
     * Test of setURL method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetURL() throws Exception {
        System.out.println("setURL");

        fail("TODO: The test case is empty.");
    }

    /**
     * Test of setNull method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetNull() throws Exception {
        System.out.println("setNull");

        setUpDualTable();
        prepareCall("select cast(? as varchar) from dual")
            .setNull("@p1", Types.VARCHAR);
    }

    /**
     * Test of setBoolean method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetBoolean() throws Exception {
        System.out.println("setBoolean");

        setUpDualTable();
        prepareCall("select cast(? as boolean) from dual")
            .setBoolean("@p1", true);
    }

    /**
     * Test of setByte method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetByte() throws Exception {
        System.out.println("setByte");

        setUpDualTable();
        prepareCall("select cast(? as tinyint) from dual")
            .setByte("@p1", (byte) 1);
    }

    /**
     * Test of setShort method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetShort() throws Exception {
        System.out.println("setShort");

        setUpDualTable();
        prepareCall("select cast(? as smallint) from dual")
            .setShort("@p1", (short) 1);
    }

    /**
     * Test of setInt method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetInt() throws Exception {
        System.out.println("setInt");

        setUpDualTable();
        prepareCall("select cast(? as integer) from dual").setInt("@p1", 1);
    }

    /**
     * Test of setLong method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetLong() throws Exception {
        System.out.println("setLong");

        setUpDualTable();
        prepareCall("select cast(? as bigint) from dual").setLong("@p1", 1);
    }

    /**
     * Test of setFloat method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetFloat() throws Exception {
        System.out.println("setFloat");

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
     * Test of setDouble method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetDouble() throws Exception {
        System.out.println("setDouble");

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
     * Test of setBigDecimal method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetBigDecimal() throws Exception {
        System.out.println("setBigDecimal");

        setUpDualTable();
        prepareCall("select cast(? as decimal) from dual")
            .setBigDecimal("@p1", new BigDecimal("1.23456789"));
    }

    /**
     * Test of setString method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetString() throws Exception {
        System.out.println("setString");

        setUpDualTable();
        prepareCall("select cast(? as varchar) from dual")
            .setString("@p1", "setString");
    }

    /**
     * Test of setBytes method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetBytes() throws Exception {
        System.out.println("setBytes");

        setUpDualTable();
        prepareCall("select cast(? as binary) from dual")
            .setBytes("@p1", new byte[10]);
    }

    /**
     * Test of setDate method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetDate() throws Exception {
        System.out.println("setDate");

        setUpDualTable();
        prepareCall("select cast(? as date) from dual")
            .setDate("@p1", Date.valueOf("2005-12-13"));
    }

    /**
     * Test of setTime method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetTime() throws Exception {
        System.out.println("setTime");

        setUpDualTable();
        prepareCall("select cast(? as time) from dual")
            .setTime("@p1", Time.valueOf("11:23:02"));
    }

    /**
     * Test of setTimestamp method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetTimestamp() throws Exception {
        System.out.println("setTimestamp");

        setUpDualTable();
        prepareCall("select cast(? as timestamp) from dual")
            .setTimestamp("@p1", Timestamp.valueOf("2005-12-13 11:23:02.1234"));
    }

    /**
     * Test of setAsciiStream method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetAsciiStream() throws Exception {
        System.out.println("setAsciiStream");

        setUpDualTable();

        byte[]               bytes = "2005-12-13 11:23:02.1234".getBytes();
        ByteArrayInputStream bais  = new ByteArrayInputStream(bytes);

        prepareCall("select cast(? as varchar) from dual")
            .setAsciiStream("@p1", bais, bytes.length);
    }

    /**
     * Test of setBinaryStream method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetBinaryStream() throws Exception {
        System.out.println("setBinaryStream");

        setUpDualTable();

        byte[]               bytes = "2005-12-13 11:23:02.1234".getBytes();
        ByteArrayInputStream bais  = new ByteArrayInputStream(bytes);

        prepareCall("select cast(? as varchar) from dual")
            .setBinaryStream("@p1", bais, bytes.length);
    }

    /**
     * Test of setObject method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetObject() throws Exception {
        System.out.println("setObject");

        setUpDualTable();

        setObjectTest("bigint", new Long(Long.MAX_VALUE), Types.BIGINT);
        setObjectTest("binary", "setObject".getBytes(), Types.BINARY);
        setObjectTest("boolean",  Boolean.TRUE, Types.BOOLEAN);
        setObjectTest("char", "setObject  ", Types.CHAR);
        setObjectTest("date", Date.valueOf("2005-12-13"), Types.DATE);
        setObjectTest("decimal", new BigDecimal(1.123456789), Types.DECIMAL);
        setObjectTest("double", new Double(Double.MAX_VALUE), Types.DOUBLE);
        setObjectTest("float", new Double(Double.MAX_VALUE), Types.FLOAT);
        setObjectTest("integer", new Integer(Integer.MIN_VALUE), Types.INTEGER);
        setObjectTest("longvarbinary", "setObject".getBytes(), Types.LONGVARBINARY);
        setObjectTest("longvarchar", "setObject", Types.LONGVARCHAR);
        //
        setObjectTest("object", new boolean[10], Types.OTHER);
        setObjectTest("object", new byte[10], Types.OTHER);
        setObjectTest("object", new short[10], Types.OTHER);
        setObjectTest("object", new char[10], Types.OTHER);
        setObjectTest("object", new int[10], Types.OTHER);
        setObjectTest("object", new long[10], Types.OTHER);
        setObjectTest("object", new float[10], Types.OTHER);
        setObjectTest("object", new double[10], Types.OTHER);
        //
        setObjectTest("object", new String[10], Types.OTHER);
        setObjectTest("object", new Boolean[10], Types.OTHER);
        setObjectTest("object", new Byte[10], Types.OTHER);
        setObjectTest("object", new Short[10], Types.OTHER);
        setObjectTest("object", new Character[10], Types.OTHER);
        setObjectTest("object", new Integer[10], Types.OTHER);
        setObjectTest("object", new Long[10], Types.OTHER);
        setObjectTest("object", new Float[10], Types.OTHER);
        setObjectTest("object", new Double[10], Types.OTHER);
        //
        setObjectTest("real", new Float(Float.MAX_VALUE), Types.REAL);
        setObjectTest("smallint", new Short(Short.MAX_VALUE), Types.SMALLINT);
        setObjectTest("time", Time.valueOf("1:23:47"), Types.TIME);
        setObjectTest("timestamp", Timestamp.valueOf("2005-12-13 01:23:47.1234"), Types.TIMESTAMP);
        setObjectTest("tinyint", new Byte(Byte.MAX_VALUE), Types.TINYINT);
        setObjectTest("varbinary", "setObject".getBytes(), Types.VARBINARY);
        setObjectTest("varchar", "setObject", Types.VARCHAR);
    }

    /**
     * Test of setCharacterStream method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetCharacterStream() throws Exception {
        System.out.println("setCharacterStream");

        setUpDualTable();
        prepareCall("select cast(? as varchar) from dual")
            .setCharacterStream("@p1",
                                new java.io.StringReader("setCharacterStream"),
                                "setCharacterStream".length());
    }

    /**
     * Test of getRowId method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetRowId() throws Exception {
        System.out.println("getRowId");

        fail("TODO: The test case is empty.");
    }

    /**
     * Test of setRowId method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetRowId() throws Exception {
        System.out.println("setRowId");

        fail("TODO: The test case is empty.");
    }

    /**
     * Test of setNString method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetNString() throws Exception {
        System.out.println("setNString");

        setUpDualTable();
        prepareCall("select cast(? as varchar) from dual")
            .setNString("@p1", "setString");
    }

    /**
     * Test of setNCharacterStream method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetNCharacterStream() throws Exception {
        System.out.println("setNCharacterStream");

        setUpDualTable();
        prepareCall("select cast(? as varchar) from dual")
            .setNCharacterStream("@p1",
                                new java.io.StringReader("setCharacterStream"),
                                "setCharacterStream".length());
    }

    /**
     * Test of setNClob method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetNClob() throws Exception {
        System.out.println("setNClob");

        setUpDualTable();
        prepareCall("select cast(? as varchar) from dual")
            .setNClob("@p1", new jdbcNClob("setCharacterStream"));
    }

    /**
     * Test of setClob method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetClob() throws Exception {
        System.out.println("setClob");

        setUpDualTable();
        prepareCall("select cast(? as varchar) from dual")
            .setClob("@p1", new jdbcClob("setCharacterStream"));
    }

    /**
     * Test of setBlob method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetBlob() throws Exception {
        System.out.println("setBlob");

        setUpDualTable();
        prepareCall("select cast(? as varchar) from dual")
            .setBlob("@p1", new jdbcBlob("setCharacterStream".getBytes()));
    }

    /**
     * Test of getNClob method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetNClob() throws Exception {
        System.out.println("getNClob");

        CallableStatement stmt;
        NClob             expResult = new jdbcNClob("getString");
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
     * Test of setSQLXML method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testSetSQLXML() throws Exception {
        System.out.println("setSQLXML");

        fail("TODO: The test case is empty.");
    }

    /**
     * Test of getSQLXML method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetSQLXML() throws Exception {
        System.out.println("getSQLXML");

        // TODO add your test code below by replacing the default call to fail.
        fail("TODO: The test case is empty.");
    }

    /**
     * Test of getNString method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetNString() throws Exception {
        System.out.println("getNString");

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
     * Test of getNCharacterStream method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetNCharacterStream() throws Exception {
        System.out.println("getNCharacterStream");

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
     * Test of getCharacterStream method, of class org.hsqldb.jdbc.jdbcCallableStatement.
     */
    public void testGetCharacterStream() throws Exception {
        System.out.println("getCharacterStream");

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

    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }

}
