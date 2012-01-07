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

import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;
import org.hsqldb.jdbc.testbase.SqlState;

/**
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 */
public class JDBCResultSetTest extends BaseJdbcTestCase {

    public static final int DEFAULT_RESULT_SET_CLOSED_ERROR_CODE = -ErrorCode.X_24501;
    public static final int DEFAULT_RESULT_SET_BEFORE_FIRST_ERROR_CODE = -ErrorCode.X_24504;
    public static final int DEFAULT_RESULT_SET_AFTER_LAST_ERROR_CODE = -ErrorCode.X_24504;
    private List<ResultSet> m_resultSetList;

    public JDBCResultSetTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        m_resultSetList = new ArrayList<ResultSet>();

        executeScript("setup-all_types-table.sql");
        executeScript("populate-all_types-table.sql");
    }

    @Override
    protected void tearDown() throws Exception {
        for (ResultSet rs : m_resultSetList) {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception e) {
                }
            }
        }

        super.tearDown();
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(JDBCResultSetTest.class);

        return suite;
    }

    protected boolean isTestUpdates() {
        return getBooleanProperty("test.result.set.updates", true);
    }

    protected boolean isTestARRAY() {
        return getBooleanProperty("test.types.array", false);
    }

    protected boolean isTestREF() {
        return getBooleanProperty("test.types.ref", false);
    }

    protected boolean isTestROWID() {
        return getBooleanProperty("test.types.rowid", false);
    }

    protected boolean isTestSQLXML() {
        return getBooleanProperty("test.types.rowid", false);
    }

    protected int getResultSetClosedErrorCode() {
        return getIntProperty(
                "result.set.closed.error.code",
                DEFAULT_RESULT_SET_CLOSED_ERROR_CODE);
    }

    protected int getResultSetBeforeFirstErrorCode() {
        return getIntProperty(
                "result.set.before.first.error.code",
                DEFAULT_RESULT_SET_BEFORE_FIRST_ERROR_CODE);
    }

    protected int getResultSetAfterLastErrorCode() {
        return getIntProperty(
                "result.set.after.last.error.code",
                DEFAULT_RESULT_SET_AFTER_LAST_ERROR_CODE);
    }
    private final String m_select =
            "select id       as id_column, " // 1
            + "c_bigint        as bigint_column, "
            + "c_binary        as binary_column, "
            + "c_boolean       as boolean_column, "
            + "c_char          as char_column, " // 5
            + "c_date          as date_column, "
            + "c_decimal       as decimal_column, "
            + "c_double        as double_column, "
            + "c_float         as float_column, "
            + "c_integer       as integer_column, " // 10
            + "c_longvarbinary as longvarbinary_column, "
            + "c_longvarchar   as longvarchar_column, "
            + "c_object        as object_column, "
            + "c_real          as real_column, "
            + "c_smallint      as smallint_column, " // 15
            + "c_time          as time_column, "
            + "c_timestamp     as timestamp_column, "
            + "c_tinyint       as tinyint_column, "
            + "c_varbinary     as varbinary_column, "
            + "c_varchar       as varchar_column, " // 20
            + "c_varchar_ignorecase as varchar_ignorecase_column,"
            + "c_blob          as blob_column, "
            + "c_clob          as clob_column, "
            + "c_array         as array_column "
            + "from all_types";
    private final String[] m_names = new String[]{
        "c_bigint",
        "c_binary",
        "c_boolean",
        "c_char",
        "c_date",
        "c_decimal",
        "c_double",
        "c_float",
        "c_integer",
        "c_longvarbinary",
        "c_longvarchar",
        "c_real",
        "c_smallint",
        "c_time",
        "c_timestamp",
        "c_tinyint",
        "c_varbinary",
        "c_varchar",
        "c_varchar_ignorecase",
        "c_blob",
        "c_clob",
        "c_array"
    };
    private final String[] m_aliases = new String[]{
        "bigint_column",
        "binary_column",
        "boolean_column",
        "char_column",
        "date_column",
        "decimal_column",
        "double_column",
        "float_column",
        "integer_column",
        "longvarbinary_column",
        "longvarchar_column",
        "real_column",
        "smallint_column",
        "time_column",
        "timestamp_column",
        "tinyint_column",
        "varbinary_column",
        "varchar_column",
        "varchar_ignorecase_column",
        "blob_column",
        "clob_column",
        "array_column"
    };

    protected String getSelect() {
        return this.m_select;
    }

    protected String[] getColumnNames() {
        return this.m_names;
    }

    protected String[] getColumnAliases() {
        return this.m_aliases;
    }

    // Forward-Only, Read-Only
    protected ResultSet newFOROJdbcResultSet() throws Exception {
        return newJdbcResultSet(ResultSet.TYPE_FORWARD_ONLY);
    }

    // Scrollable, Read-Only
    protected ResultSet newScrollableROJdbcResultSet() throws Exception {
        return newJdbcResultSet(ResultSet.TYPE_SCROLL_INSENSITIVE);
    }

    // Scrollable,
    protected ResultSet newUpdateableJdbcResultSet() throws Exception {
        return newJdbcResultSet(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
    }

    protected ResultSet newJdbcResultSet(int type) throws Exception {
        return newJdbcResultSet(type, ResultSet.CONCUR_READ_ONLY);
    }

    protected ResultSet newJdbcResultSet(int type, int concur) throws Exception {

        JDBCConnection conn = (JDBCConnection) newConnection();
        conn.setAutoCommit(false);
        JDBCStatement stmt = (JDBCStatement) conn.createStatement(type, concur);

        ResultSet rs = stmt.executeQuery(m_select);

        m_resultSetList.add(rs);

        return rs;
    }

    /** @todo  Conversion from string to boolean in getBoolean() is supported
     * only with the values 'true' or 'false"
     * Similarly, the contents of the character field must be convertible to
     * the type.
     *
     *
     */
    @SuppressWarnings("CallToThreadDumpStack")
    protected void testGetXXX(String methodName) throws Exception {
        ResultSet rs = newFOROJdbcResultSet();

        rs.next();

        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            String typeName = rsmd.getColumnTypeName(i);
            String columnName = rsmd.getColumnName(i);
            String columnClass = rsmd.getColumnClassName(i);
            int dataType = rsmd.getColumnType(i);
            boolean required = isRequiredGetXXX(methodName, dataType);

            try {
                Method getXXX = rs.getClass().getMethod(
                        methodName,
                        new Class[]{int.class});

                Object value = getXXX.invoke(
                        rs,
                        new Object[]{new Integer(i)});

                Class valueClass = (value == null) ? Void.class
                        : value.getClass();

                if (!required) {
                    Method getObject = rs.getClass().getMethod(
                            "getObject",
                            new Class[]{int.class});

                    Object objectValue = getObject.invoke(
                            rs,
                            new Object[]{new Integer(i)});

                    printProgress("****************************************");
                    printProgress(
                            "Warn - JDBC 4.0, Table B-6 indicates this "
                            + "getter conversion is not required:");
                    printProgress("From SQL: " + columnName + "{");
                    printProgress("    type=" + typeName);
                    printProgress("   class=" + columnClass);
                    printProgress("   value=\"" + objectValue + "\"");
                    printProgress("}");
                    printProgress("To Java : " + valueClass + "{");
                    printProgress("    value=\"" + value + "\"");
                    printProgress("}");
                    printProgress("****************************************");
                } else {
                    printProgress(
                            "Info - Pass: " + columnName + "(" + typeName + ")");
                }
            } catch (Exception e) {

                Throwable t = e;

                while (t instanceof InvocationTargetException) {
                    t = ((InvocationTargetException) t).getTargetException();
                }

                if (t instanceof SQLException) {
                    SQLException ex = (SQLException) t;

                    if (required) {
                        if (ex.getErrorCode() != -ErrorCode.X_42561) {
                            fail(
                                    columnName + "(" + typeName + ") : " + t + ": [" + ex.getErrorCode() + "]");
                        } else {
                            printProgress(
                                    "Warn - Pass: " + columnName + "(" + typeName + ") : " + ex);
                        }
                    } else {
                        printProgress(
                                "Info - Pass: " + columnName + "(" + typeName + ") : " + ex);
                    }
                } else {
                    t.fillInStackTrace().printStackTrace();
                    //t.printStackTrace();
                    Throwable t2 = t.getCause();
                    if (t2 != null) {
                        t2.printStackTrace();
                    }
                    fail(
                            columnName + "(" + typeName + ") : " + t);
                }
            }
        }
    }

    /**
     * Test of next method, of interface java.sql.ResultSet.
     */
    public void testNext() throws Exception {
        ResultSet rs = newFOROJdbcResultSet();

        assertEquals(true, rs.next());

        while (rs.next()) {
        }

        assertEquals(false, rs.next());

        rs.close();

        try {
            rs.next();

            fail("Allowed next() after close().");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of close method, of interface java.sql.ResultSet.
     */
    public void testClose() throws Exception {
        ResultSet rs = newFOROJdbcResultSet();

        try {
            rs.close();
        } catch (SQLException ex) {
            fail("Failed to close result set: " + ex);
        }
    }

    /**
     * Test of wasNull method, of interface java.sql.ResultSet.
     */
    public void testWasNull() throws Exception {
        ResultSet rs = newFOROJdbcResultSet();

        rs.next();

        String[] columnAliases = getColumnAliases();

        for (int i = 0; i < columnAliases.length; i++) {
            String columnAlias = columnAliases[i];
            Object o = rs.getObject(columnAlias);
            assertEquals(columnAliases[i] + "(" + o + ")", false, rs.wasNull());
        }

        rs.next();
        rs.next();
        rs.next();

        for (int i = 0; i < columnAliases.length; i++) {
            rs.getObject(columnAliases[i]);
            assertEquals(columnAliases[i], true, rs.wasNull());
        }

        rs.close();

        try {
            rs.wasNull();

            fail("Allowed wasNull() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getString method, of interface java.sql.ResultSet.
     */
    public void testGetString() throws Exception {
        testGetXXX("getString");

        ResultSet rs = newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getString("varchar_column");

            fail("Allowed getString() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getBoolean method, of interface java.sql.ResultSet.
     */
    public void testGetBoolean() throws Exception {
        testGetXXX("getBoolean");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getBoolean("c_boolean");

            fail("Allowed getBoolean after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getByte method, of interface java.sql.ResultSet.
     */
    public void testGetByte() throws Exception {
        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getByte(1);

            fail("Allowed getByte after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getShort method, of interface java.sql.ResultSet.
     */
    public void testGetShort() throws Exception {
        testGetXXX("getShort");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getShort(1);

            fail("Allowed getShort after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getInt method, of interface java.sql.ResultSet.
     */
    public void testGetInt() throws Exception {
        testGetXXX("getInt");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getInt(1);

            fail("Allowed getInt after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getLong method, of interface java.sql.ResultSet.
     */
    public void testGetLong() throws Exception {
        testGetXXX("getLong");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getLong(1);

            fail("Allowed getLong after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getFloat method, of interface java.sql.ResultSet.
     */
    public void testGetFloat() throws Exception {
        testGetXXX("getFloat");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getFloat("c_float");

            fail("Allowed getFloat after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getDouble method, of interface java.sql.ResultSet.
     */
    public void testGetDouble() throws Exception {
        testGetXXX("getDouble");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getDouble("c_double");

            fail("Allowed getDouble after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getBigDecimal method, of interface java.sql.ResultSet.
     */
    public void testGetBigDecimal() throws Exception {
        testGetXXX("getBigDecimal");
    }

    /**
     * Test of getBigDecimal method, of interface java.sql.ResultSet.
     */
    public void testGetBigDecimal_afterClose() throws Exception {
        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getBigDecimal(1);

            fail("Allowed getBigDecimal after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    getResultSetClosedErrorCode(),
                    ex.getErrorCode());
        }
    }

    /**
     * Test of getBytes method, of interface java.sql.ResultSet.
     */
    public void testGetBytes() throws Exception {
        testGetXXX("getBytes");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getBytes(1);

            fail("Allowed getBytes after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getDate method, of interface java.sql.ResultSet.
     */
    public void testGetDate() throws Exception {
        testGetXXX("getDate");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getDate("c_date");

            fail("Allowed getDate after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getTime method, of interface java.sql.ResultSet.
     */
    public void testGetTime() throws Exception {
        testGetXXX("getTime");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getTime(1);

            fail("Allowed getTime after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getTimestamp method, of interface java.sql.ResultSet.
     */
    public void testGetTimestamp() throws Exception {
        testGetXXX("getTimestamp");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getTimestamp(1);

            fail("Allowed getTimestamp after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getAsciiStream method, of interface java.sql.ResultSet.
     */
    public void testGetAsciiStream() throws Exception {
        testGetXXX("getAsciiStream");
    }

    /**
     * Test of getAsciiStream method, of interface java.sql.ResultSet.
     */
    public void testGetAsciiStream_afterClose() throws Exception {
        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        assertEquals("isClosed", true, rs.isClosed());

        try {
            rs.getAsciiStream(1);

            fail("Allowed getAsciiStream after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    getResultSetClosedErrorCode(),
                    ex.getErrorCode());
        }
    }

    /**
     * Test of getAsciiStream method, of interface java.sql.ResultSet.
     */
    public void testGetAsciiStream_afterLast() throws Exception {
        ResultSet rs = newFOROJdbcResultSet();

        while (rs.next()) {
        }

        try {
            rs.getAsciiStream(1);

            fail("Allowed getAsciiStream while after last");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    getResultSetAfterLastErrorCode(),
                    ex.getErrorCode());
        }
    }

    /**
     * Test of getArray method, of interface java.sql.ResultSet.
     */
    public void testGetAsciiStream_beforeFirst() throws Exception {
        ResultSet rs = newFOROJdbcResultSet();

        assertEquals(true, rs.isBeforeFirst());

        try {
            rs.getAsciiStream(1);

            fail("Allowed getGetAsciiStream(int) while before first");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    getResultSetBeforeFirstErrorCode(),
                    ex.getErrorCode());
        }
    }

    /**
     * Test of getUnicodeStream method, of interface java.sql.ResultSet.
     */
    public void testGetUnicodeStream() throws Exception {
        testGetXXX("getUnicodeStream");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getUnicodeStream(1);

            fail("Allowed getUnicodeStream after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getBinaryStream method, of interface java.sql.ResultSet.
     */
    public void testGetBinaryStream() throws Exception {
        testGetXXX("getBinaryStream");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getBinaryStream(1);

            fail("Allowed getBinaryStream after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getWarnings method, of interface java.sql.ResultSet.
     */
    public void testGetWarnings() throws Exception {
        ResultSet rs = newFOROJdbcResultSet();

        java.sql.SQLWarning warnings = rs.getWarnings();

        assertNull("warnings", warnings);

        rs.next();
        rs.close();

        try {
            rs.getWarnings();

            fail("Allowed getWarnings() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }

        fail("TODO: cases to test spec'd generation of warnings");
    }

    /**
     * Test of clearWarnings method, of interface java.sql.ResultSet.
     */
    public void testClearWarnings() throws Exception {
        ResultSet rs = newFOROJdbcResultSet();

        rs.clearWarnings();

        assertEquals(null, rs.getWarnings());

        rs.close();
        try {
            rs.clearWarnings();

            fail("Allowed clearWarnings() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getCursorName method, of interface java.sql.ResultSet.
     */
    public void testGetCursorName() throws Exception {
        ResultSet rs = newFOROJdbcResultSet();

        String expResult = null;

        try {
            String result = rs.getCursorName();
            assertEquals(expResult, result);
        } catch (Exception e) {
            fail(e.toString());
        }

        rs.close();

        try {
            rs.getCursorName();

            fail("Allowed getCursorName() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getMetaData method, of interface java.sql.ResultSet.
     */
    public void testGetMetaData() throws Exception {
        ResultSet rs = newFOROJdbcResultSet();
        ResultSetMetaData rsmd = rs.getMetaData();

        assertNotNull(rsmd);

        rs.close();

        try {
            rs.getMetaData();

            fail("Allowed getMetaData after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }

        try {

            int count = rsmd.getColumnCount();

            for (int i = 1; i <= count; i++) {
                rsmd.getCatalogName(i);
                rsmd.getColumnClassName(i);
                rsmd.getColumnDisplaySize(i);
                rsmd.getColumnLabel(i);
                rsmd.getColumnName(i);
                rsmd.getColumnType(i);
                rsmd.getColumnTypeName(i);
                rsmd.getPrecision(i);
                rsmd.getScale(i);
                rsmd.getSchemaName(i);
                rsmd.getTableName(i);
                rsmd.isAutoIncrement(i);
                rsmd.isCaseSensitive(i);
                rsmd.isCurrency(i);
                rsmd.isDefinitelyWritable(i);
                rsmd.isNullable(i);
                rsmd.isReadOnly(i);
                rsmd.isSearchable(i);
                rsmd.isSigned(i);
                rsmd.isWritable(i);
            }
        } catch (SQLException ex) {
            fail("ResultSetMetaData should be valid after ResultSet is closed: " + ex.toString());
        }
    }

    /**
     * Test of getObject method, of interface java.sql.ResultSet.
     */
    public void testGetObject() throws Exception {
        testGetXXX("getObject");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getObject(1);

            fail("Allowed getObject after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of findColumn method, of interface java.sql.ResultSet.
     */
    public void testFindColumn() throws Exception {
        ResultSet rs = newFOROJdbcResultSet();
        String[] columnAliases = getColumnAliases();

        for (int i = 0; i < columnAliases.length; i++) {
            rs.findColumn(columnAliases[i]);
        }

        try {
            rs.findColumn("not a column label");

            fail("Allowed findColumn(String) for a non-existent column label");
        } catch (SQLException ex) {
        }

        rs.close();

        try {
            rs.findColumn(columnAliases[0]);

            fail("Allowed findColumn(String) after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getCharacterStream method, of interface java.sql.ResultSet.
     */
    public void testGetCharacterStream() throws Exception {
        testGetXXX("getCharacterStream");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getCharacterStream(1);

            fail("Allowed getCharacterStream after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of isBeforeFirst method, of interface java.sql.ResultSet.
     */
    public void testIsBeforeFirst() throws Exception {
        ResultSet rs = this.newScrollableROJdbcResultSet();

        try {
            rs.getObject(1);
            fail("get succeeded while before first");
        } catch (Exception e) {
        }

        assertEquals(true, rs.isBeforeFirst());

        rs.next();

        assertEquals(false, rs.isBeforeFirst());

        rs.previous();

        try {
            rs.getObject(1);
            fail("get succeeded while before first");
        } catch (Exception e) {
        }

        assertEquals(true, rs.isBeforeFirst());

        rs.next();

        assertEquals(false, rs.isBeforeFirst());

        rs.beforeFirst();

        try {
            rs.getObject(1);
            fail("get succeeded while before first");
        } catch (Exception e) {
        }

        assertEquals(true, rs.isBeforeFirst());

        rs.close();

        try {
            rs.isBeforeFirst();

            fail("Allowed isBeforeFirst() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }

    }

    /**
     * Test of isAfterLast method, of interface java.sql.ResultSet.
     */
    public void testIsAfterLast() throws Exception {
        ResultSet rs = this.newScrollableROJdbcResultSet();

        assertEquals(false, rs.isAfterLast());

        rs.next();

        assertEquals(false, rs.isAfterLast());

        while (rs.next()) {
        }

        try {
            rs.getObject(1);
            fail("get succeeded while after last");
        } catch (Exception e) {
        }

        assertEquals(true, rs.isAfterLast());

        rs.previous();

        try {
            rs.getObject(1);
        } catch (Exception e) {
            fail("get failed on previous to after last");
        }

        assertEquals(false, rs.isAfterLast());

        rs.next();

        assertEquals(true, rs.isAfterLast());

        rs.previous();

        assertEquals(false, rs.isAfterLast());

        rs.afterLast();

        assertEquals(true, rs.isAfterLast());

        rs.previous();

        assertEquals(false, rs.isAfterLast());

        rs.close();

        try {
            rs.afterLast();

            fail("Allowed afterLast() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of isFirst method, of interface java.sql.ResultSet.
     */
    public void testIsFirst() throws Exception {
        ResultSet rs = this.newScrollableROJdbcResultSet();

        assertEquals(
                "isFirst() while before first call to next()",
                false,
                rs.isFirst());

        rs.next();

        assertEquals(
                "isFirst() after first call to next()",
                true,
                rs.isFirst());

        rs.next();

        assertEquals(
                "isFirst() after next();next();",
                false,
                rs.isFirst());

        rs.previous();

        assertEquals(
                "isFirst() after next();next();previous()",
                true,
                rs.isFirst());

        while (rs.next()) {
        }

        assertEquals(
                "isFirst() while after all next()",
                false,
                rs.isFirst());

        while (rs.previous()) {
        }

        assertEquals(
                "isFirst() while before all previous()",
                false,
                rs.isFirst());

        rs.next();

        assertEquals(
                "isFirst() after next() after before all previous()",
                true,
                rs.isFirst());

        while (rs.next()) {
        }

        assertEquals(
                "isFirst() after all next() after all previous()",
                false,
                rs.isFirst());

        rs.first();

        assertEquals(
                "isFirst() after first() after all next() after all previous()",
                true,
                rs.isFirst());

        rs.close();

        try {
            rs.isFirst();

            fail("Allowed isFirst() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }

    }

    /**
     * Test of isLast method, of interface java.sql.ResultSet.
     */
    public void testIsLast() throws Exception {
        ResultSet rs = this.newScrollableROJdbcResultSet();

        assertEquals(
                "isLast() before first call to next()",
                false,
                rs.isLast());

        rs.next();

        assertEquals(
                "isLast() after first call to next()",
                false,
                rs.isLast());

        while (rs.next()) {
        }

        assertEquals(
                "isLast() after all next()",
                false,
                rs.isLast());

        rs.previous();

        assertEquals(
                "isLast() after previous() after all next()",
                true,
                rs.isLast());

        while (rs.previous()) {
        }

        assertEquals(
                "isLast() after all previous() after all next()",
                false,
                rs.isLast());

        rs.last();

        assertEquals(
                "isLast() after call to last()",
                true,
                rs.isLast());

        rs.close();

        try {
            rs.isLast();

            fail("Allowed isLast() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of beforeFirst method, of interface java.sql.ResultSet.
     */
    public void testBeforeFirst() throws Exception {
        ResultSet rs = this.newScrollableROJdbcResultSet();

        try {
            rs.getObject(1);
            fail("get succeeded while before first");
        } catch (Exception e) {
        }

        assertEquals(true, rs.isBeforeFirst());

        while (rs.next()) {
        }

        rs.beforeFirst();

        try {
            rs.getObject(1);
            fail("get succeeded while before first");
        } catch (Exception e) {
        }

        assertEquals(true, rs.isBeforeFirst());

        rs.next();
        rs.close();

        try {
            rs.beforeFirst();

            fail("Allowed beforeFirst() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of afterLast method, of interface java.sql.ResultSet.
     */
    public void testAfterLast() throws Exception {
        ResultSet rs = this.newScrollableROJdbcResultSet();

        rs.afterLast();

        assertEquals(true, rs.isAfterLast());

        try {
            rs.getObject(1);
            fail("get succeeded while after last");
        } catch (Exception e) {
        }

        rs.beforeFirst();
        rs.afterLast();

        assertEquals(true, rs.isAfterLast());

        try {
            rs.getObject(1);
            fail("get succeeded while after last");
        } catch (Exception e) {
        }

        rs.previous();

        try {
            rs.getObject(1);
        } catch (Exception e) {
            fail("get failed on previous to after last");
        }

        rs.afterLast();

        try {
            rs.getObject(1);
            fail("get succeeded while after last");
        } catch (Exception e) {
        }

        assertEquals(true, rs.isAfterLast());

        rs.previous();
        rs.close();

        try {
            rs.afterLast();

            fail("Allowed afterLast() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of first method, of interface java.sql.ResultSet.
     */
    public void testFirst() throws Exception {
        ResultSet rs = newScrollableROJdbcResultSet();

        rs.first();

        assertEquals("isFirst", true, rs.isFirst());

        rs.next();

        assertEquals("isFirst", false, rs.isFirst());

        rs.close();

        try {
            rs.first();

            fail("Allowed first() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }

    }

    /**
     * Test of last method, of interface java.sql.ResultSet.
     */
    public void testLast() throws Exception {
        ResultSet rs = this.newScrollableROJdbcResultSet();

        rs.last();

        assertEquals(true, rs.isLast());

        rs.close();

        try {
            rs.last();

            fail("Allowed last() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getRow method, of interface java.sql.ResultSet.
     */
    public void testGetRow() throws Exception {
        ResultSet rs = this.newScrollableROJdbcResultSet();
        int row = 0;

        while (rs.next()) {
            row++;
            assertEquals(row, rs.getRow());
        }

        int last = row;

        while (rs.previous()) {
            assertEquals(row, rs.getRow());
            row--;
        }

        rs.absolute(2);

        assertEquals(2, rs.getRow());

        rs.first();

        assertEquals(1, rs.getRow());

        rs.last();

        assertEquals(last, rs.getRow());

        rs.close();

        try {
            rs.getRow();

            fail("Allowed getRow() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of absolute method, of interface java.sql.ResultSet.
     */
    public void testAbsolute() throws Exception {
        ResultSet rs = this.newScrollableROJdbcResultSet();
        int rows = 0;

        while (rs.next()) {
            rows++;
        }

        for (int i = rows; i >= 1; i--) {
            rs.absolute(i);

            assertEquals(i, rs.getRow());
        }

        rs.close();

        try {
            rs.absolute(0);

            fail("Allowed absolute(int) after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    getResultSetClosedErrorCode(),
                    ex.getErrorCode());
        }
    }

    /**
     * Test of relative method, of interface java.sql.ResultSet.
     */
    public void testRelative() throws Exception {
        ResultSet rs = this.newScrollableROJdbcResultSet();

        while (!rs.isAfterLast()) {
            rs.relative(1);
        }

        while (!rs.isBeforeFirst()) {
            rs.relative(-1);
        }

        while (!rs.isAfterLast()) {
            rs.relative(2);
        }

        while (!rs.isBeforeFirst()) {
            rs.relative(-2);
        }
    }

    /**
     * Test of previous method, of interface java.sql.ResultSet.
     */
    public void testPrevious() throws Exception {
        ResultSet rs = this.newScrollableROJdbcResultSet();

        rs.afterLast();

        while (rs.previous()) {
            assertEquals(false, rs.isBeforeFirst());
        }

        assertEquals(true, rs.isBeforeFirst());

        rs.close();

        try {
            rs.previous();

            fail("Allowed previous() after close().");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of setFetchDirection method, of interface java.sql.ResultSet.
     */
    public void testSetFetchDirection() throws Exception {
        ResultSet rs = this.newScrollableROJdbcResultSet();

        rs.setFetchDirection(ResultSet.FETCH_FORWARD);
        rs.setFetchDirection(ResultSet.FETCH_REVERSE);
        rs.setFetchDirection(ResultSet.FETCH_UNKNOWN);
    }

    /**
     * Test of getFetchDirection method, of interface java.sql.ResultSet.
     */
    public void testGetFetchDirection() throws Exception {
        ResultSet rs = this.newScrollableROJdbcResultSet();

        assertEquals(
                "fetch direction",
                JDBCResultSet.FETCH_FORWARD,
                rs.getFetchDirection());

        rs.close();

        try {
            rs.getFetchDirection();

            fail("Allowed getFetchDirection() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of setFetchSize method, of interface java.sql.ResultSet.
     */
    public void testSetFetchSize() throws Exception {
        ResultSet rs = newFOROJdbcResultSet();

        rs.setFetchSize(1000);
    }

    /**
     * Test of getFetchSize method, of interface java.sql.ResultSet.
     */
    public void testGetFetchSize() throws Exception {
        ResultSet rs = newFOROJdbcResultSet();

        int expResult = 0;
        int result = rs.getFetchSize();
        assertEquals("fetch size", expResult, result);

        rs.close();

        try {
            rs.getFetchSize();

            fail("Allowed getFetchSize() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getType method, of interface java.sql.ResultSet.
     */
    public void testGetType_FORWARD_ONLY() throws Exception {
        assertEquals(ResultSet.TYPE_FORWARD_ONLY,
                newFOROJdbcResultSet().getType());

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getType();

            fail("Allowed getType after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getType method, of interface java.sql.ResultSet.
     */
    public void testGetType_SCROLL_INSENSITIVE() throws Exception {
        assertEquals(
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                newJdbcResultSet(ResultSet.TYPE_SCROLL_INSENSITIVE).getType());

        ResultSet rs = newJdbcResultSet(ResultSet.TYPE_SCROLL_INSENSITIVE);

        rs.next();
        rs.close();

        try {
            rs.getType();

            fail("Allowed getType after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getType method, of interface java.sql.ResultSet.
     */
    public void testGetType_SCROLL_SENSITIVE() throws Exception {
        assertEquals(
                ResultSet.TYPE_SCROLL_SENSITIVE,
                newJdbcResultSet(ResultSet.TYPE_SCROLL_SENSITIVE).getType());

        ResultSet rs = newJdbcResultSet(ResultSet.TYPE_SCROLL_SENSITIVE);

        rs.next();
        rs.close();

        try {
            rs.getType();

            fail("Allowed getType after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getConcurrency method, of interface java.sql.ResultSet.
     */
    public void testGetConcurrency() throws Exception {
        assertEquals(ResultSet.CONCUR_READ_ONLY,
                newFOROJdbcResultSet().getConcurrency());

        ResultSet rs = newJdbcResultSet(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);

        assertEquals(
                ResultSet.CONCUR_UPDATABLE,
                rs.getConcurrency());

        ( (JDBCResultSet) rs).connection.close();


        rs = this.newFOROJdbcResultSet();

        rs.close();

        try {
            rs.getConcurrency();

            fail("Allowed getConcurrency() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of rowUpdated method, of interface java.sql.ResultSet.
     */
    public void testRowUpdated() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            assertEquals(false, rs.rowUpdated());

            rs.updateObject(1, new Integer(1), java.sql.Types.INTEGER);

            assertEquals(true, rs.rowUpdated());
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of rowInserted method, of interface java.sql.ResultSet.
     */
    public void testRowInserted() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        // TODO:
        fail("TODO: The test case is empty.");
    }

    /**
     * Test of rowDeleted method, of interface java.sql.ResultSet.
     */
    public void testRowDeleted() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        fail("TODO: The test case is empty.");
    }

    /**
     * Test of updateNull method, of interface java.sql.ResultSet.
     */
    public void testUpdateNull() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateNull(1);

            assertEquals(true, rs.rowUpdated());
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateBoolean method, of interface java.sql.ResultSet.
     */
    public void testUpdateBoolean() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateBoolean("boolean_column", true);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateByte method, of interface java.sql.ResultSet.
     */
    public void testUpdateByte() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateByte("tinyint_column", (byte) 1);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateShort method, of interface java.sql.ResultSet.
     */
    public void testUpdateShort() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateShort("smallint_column", (short) 1);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateInt method, of interface java.sql.ResultSet.
     */
    public void testUpdateInt() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateInt("integer_column", 1);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateLong method, of interface java.sql.ResultSet.
     */
    public void testUpdateLong() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateLong("bigint_column", 1L);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateFloat method, of interface java.sql.ResultSet.
     */
    public void testUpdateFloat() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateFloat("real_column", 1F);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateDouble method, of interface java.sql.ResultSet.
     */
    public void testUpdateDouble() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateDouble("float_column", 1D);
            rs.updateDouble("double_column", 1D);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateBigDecimal method, of interface java.sql.ResultSet.
     */
    public void testUpdateBigDecimal() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateBigDecimal("decimal_column", new BigDecimal("1.0"));
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateString method, of interface java.sql.ResultSet.
     */
    public void testUpdateString() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateString("char_column", "updateString");
            rs.updateString("varchar_column", "updateString");
            rs.updateString("longvarchar_column", "updateString");

            rs.updateString("id_column", "20");

            rs.updateString("binary_column", "afde9856");
            rs.updateString("varbinary_column", "afde9856");
            rs.updateString("longvarbinary_column", "afde9856");

            rs.updateString("date_column", "2005-12-14");
            rs.updateString("time_column", "11:56:01");
            rs.updateString("timestamp_column", "2005-12-14 11:56:01.1234");

            rs.updateString("tinyint_column", "127");
            rs.updateString("smallint_column", "32767");
            rs.updateString("integer_column", "214748364");
            rs.updateString("bigint_column", "9223372036854775807");
            rs.updateString("real_column", "3.4028235E38");
            rs.updateString("float_column", "1.7976931348623157E308");
            rs.updateString("double_column", "1.7976931348623157E308");
            rs.updateString("decimal_column", "9223372036854775807000.1234567890");
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateBytes method, of interface java.sql.ResultSet.
     */
    public void testUpdateBytes() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateBytes("binary_column", "updateBytes".getBytes());
            rs.updateBytes("varbinary_column", "updateBytes".getBytes());
            rs.updateBytes("longvarbinary_column", "updateBytes".getBytes());
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateDate method, of interface java.sql.ResultSet.
     */
    public void testUpdateDate() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateDate("date_column", java.sql.Date.valueOf("2005-12-14"));
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateTime method, of interface java.sql.ResultSet.
     */
    public void testUpdateTime() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateTime("time_column", java.sql.Time.valueOf("11:57:02"));
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateTimestamp method, of interface java.sql.ResultSet.
     */
    public void testUpdateTimestamp() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateTimestamp(
                    "timestamp_column",
                    java.sql.Timestamp.valueOf("2005-12-14 11:57:02.1234"));
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateAsciiStream method, of interface java.sql.ResultSet.
     */
    public void testUpdateAsciiStream() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateAsciiStream(
                    "char_column",
                    new java.io.ByteArrayInputStream(
                    "updateAsciiStream".getBytes()), 10);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateBinaryStream method, of interface java.sql.ResultSet.
     */
    public void testUpdateBinaryStream() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateBinaryStream(
                    "binary_column",
                    new java.io.ByteArrayInputStream(
                    "updateBinaryStream".getBytes()), 10);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateCharacterStream method, of interface java.sql.ResultSet.
     */
    public void testUpdateCharacterStream() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateCharacterStream(
                    "char_column",
                    new java.io.StringReader("updateCharacterStream"), 10);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    public static final class SerVal implements java.io.Serializable {
        public int value;
    }

    /**
     * Test of updateObject method, of interface java.sql.ResultSet.
     */
    public void testUpdateObject() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            SerVal value = new SerVal();

            rs.updateObject("object_column", value);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of insertRow method, of interface java.sql.ResultSet.
     */
    @SuppressWarnings("CallToThreadDumpStack")
    public void testInsertRow() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        ResultSet rs = null;

        try {
                rs = this.newUpdateableJdbcResultSet();
                rs.close();
                rs.moveToInsertRow();

                int columnCount = rs.getMetaData().getColumnCount();

                rs.updateInt(1, 999999);

                fail("Allowed insertRow() after close()");
            } catch (SQLException ex) {
                // ex.printStackTrace();
            } finally {
                ( (JDBCResultSet) rs).connection.close();
            }
        try {

            rs = this.newUpdateableJdbcResultSet();

            rs.moveToInsertRow();

            int columnCount = rs.getMetaData().getColumnCount();

            rs.updateInt(1, 999999);

            for (int i = 2; i <= columnCount; i++) {
                rs.updateNull(i);
            }

            rs.insertRow();

            rs.moveToInsertRow();

            columnCount = rs.getMetaData().getColumnCount();

            rs.updateInt(1, 1000000);

            for (int i = 2; i <= columnCount; i++) {
                rs.updateNull(i);
            }

            rs.close();

        } catch (Exception ex) {
            fail(ex.toString());
        }
    }

    /**
     * Test of updateRow method, of interface java.sql.ResultSet.
     */
    public void testUpdateRow() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            int columnCount = rs.getMetaData().getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                rs.updateNull(i);
            }

            rs.updateRow();
        } catch (Exception ex) {
            fail(ex.toString());
        }
    }

    /**
     * Test of deleteRow method, of interface java.sql.ResultSet.
     */
    public void testDeleteRow() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        ResultSet rs = newUpdateableJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.deleteRow();

            fail("Allowed deleteRow() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        } finally {
            ( (JDBCResultSet) rs).connection.close();
        }

        try {
            rs = newUpdateableJdbcResultSet();

            rs.next();

            rs.deleteRow();
        } catch (Exception ex) {
            fail(ex.toString());
        }

    }

    /**
     * Test of refreshRow method, of interface java.sql.ResultSet.
     */
    public void testRefreshRow() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        ResultSet rs = null;
        try {
            rs = newUpdateableJdbcResultSet();

            rs.next();

            rs.refreshRow();
        } catch (Exception ex) {
           printException(ex);
           fail(ex.toString());
       } finally {
           ( (JDBCResultSet) rs).connection.close();
       }

        rs = newJdbcResultSet(ResultSet.TYPE_SCROLL_SENSITIVE);

        rs.next();

        rs.close();

        try {
            rs.refreshRow();

            fail("Allowed refreshRow() after close().");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        } finally {
            ( (JDBCResultSet) rs).connection.close();
        }
    }

    /**
     * Test of cancelRowUpdates method, of interface java.sql.ResultSet.
     */
    @SuppressWarnings("UseOfIndexZeroInJDBCResultSet")
    public void testCancelRowUpdates() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        ResultSet rs = null;
        try {
            rs = newUpdateableJdbcResultSet();

            rs.next();

            int columnCount = rs.getMetaData().getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                rs.updateNull(i);
            }

            rs.cancelRowUpdates();
        } catch (Exception ex) {
            fail(ex.toString());
        } finally {
            ( (JDBCResultSet) rs).connection.close();
        }

        rs = newUpdateableJdbcResultSet();

        rs.updateNull(1);
        rs.close();
        try {
            rs.cancelRowUpdates();

            fail("Allowed cancelRowUpdates() after close()");
        } catch (SQLException ex) {
            assertEquals("sql state",
                    SqlState.Exception.InvalidCursorState.IdentifiedCursorIsNotOpen.Value,
                    ex.getSQLState());
        } finally {
            ( (JDBCResultSet) rs).connection.close();
        }
    }

    /**
     * Test of moveToInsertRow method, of interface java.sql.ResultSet.
     */
    public void testMoveToInsertRow() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        ResultSet rs = null;

        try {
            rs = this.newUpdateableJdbcResultSet();

            rs.moveToInsertRow();
        } catch (Exception ex) {
            fail(ex.toString());
        } finally {
            ( (JDBCResultSet) rs).connection.close();
        }

        rs = this.newUpdateableJdbcResultSet();

        rs.close();

        try {
            rs.moveToInsertRow();

            fail("Allowed moveToInsertRow() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        } finally {
            ( (JDBCResultSet) rs).connection.close();
        }
    }

    /**
     * Test of moveToCurrentRow method, of interface java.sql.ResultSet.
     */
    public void testMoveToCurrentRow() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        ResultSet rs = null;
        try {
            rs = this.newUpdateableJdbcResultSet();

            rs.moveToInsertRow();
            rs.moveToCurrentRow();
        } catch (Exception ex) {
            fail(ex.toString());
        } finally {
            ( (JDBCResultSet) rs).connection.close();
        }

        rs = this.newUpdateableJdbcResultSet();

        rs.moveToInsertRow();

        rs.close();

        try {
            rs.moveToCurrentRow();

            fail("Allowed moveToCurrentRow() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        } finally {
            ( (JDBCResultSet) rs).connection.close();
        }
    }

    /**
     * Test of getStatement method, of interface java.sql.ResultSet.
     */
    public void testGetStatement() throws Exception {
        ResultSet rs = newFOROJdbcResultSet();

        Statement result = rs.getStatement();
        assertNotNull(result);

        rs.close();

        try {
            rs.getStatement();

            fail("Allowed getStatement() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getRef method, of interface java.sql.ResultSet.
     */
    public void testGetRef() throws Exception {
        testGetXXX("getRef");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getRef(1);

            fail("Allowed getRef after close()");
        } catch (SQLException ex) {
            boolean test = getResultSetClosedErrorCode() == ex.getErrorCode()
                    || ex instanceof java.sql.SQLFeatureNotSupportedException;
            assertTrue("error code", test);
        }
    }

    /**
     * Test of getBlob method, of interface java.sql.ResultSet.
     */
    public void testGetBlob() throws Exception {
        testGetXXX("getBlob");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getBlob("c_blob");

            fail("Allowed getBlob after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getClob method, of interface java.sql.ResultSet.
     */
    public void testGetClob() throws Exception {
        testGetXXX("getClob");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getClob("c_clob");

            fail("Allowed getClob after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getArray method, of interface java.sql.ResultSet.
     */
    public void testGetArray() throws Exception {
        if (!isTestARRAY()) {
            return;
        }

        testGetXXX("getArray");
    }

    /**
     * Test of getArray method, of interface java.sql.ResultSet.
     */
    public void testGetArray_afterClose() throws Exception {
        if (!isTestARRAY()) {
            return;
        }

        ResultSet rs = newFOROJdbcResultSet();

        rs.close();

        try {
            rs.getArray("c_array");

            fail("Allowed getArray(String) after close");
        } catch (SQLException ex) {
            boolean test = getResultSetClosedErrorCode() == ex.getErrorCode()
                    || ex instanceof java.sql.SQLFeatureNotSupportedException;
            assertTrue("error code", test);
        }
    }

    /**
     * Test of getArray method, of interface java.sql.ResultSet.
     */
    public void testGetArray_beforeFirst() throws Exception {
        if (!isTestARRAY()) {
            return;
        }

        ResultSet rs = newFOROJdbcResultSet();

        assertEquals("beforeFirst", true, rs.isBeforeFirst());

        try {
            rs.getArray("C_array");

            fail("Allowed getArray(String) while before first");
        } catch (SQLException ex) {
            if (!(ex instanceof java.sql.SQLFeatureNotSupportedException)) {
                assertEquals("error code", getResultSetBeforeFirstErrorCode(),
                        ex.getErrorCode());
            }
        }
    }

    /**
     * Test of getArray method, of interface java.sql.ResultSet.
     */
    public void testGetArray_afterLast() throws Exception {
        if (!isTestARRAY()) {
            return;
        }

        ResultSet rs = newFOROJdbcResultSet();

        while (rs.next()) {
        }

        try {
            rs.getArray("c_array");

            fail("Allowed getArray(String) while after last");
        } catch (SQLException ex) {
            if (!(ex instanceof java.sql.SQLFeatureNotSupportedException)) {
                assertEquals("error code", getResultSetAfterLastErrorCode(),
                        ex.getErrorCode());
            }
        }
    }

    /**
     * Test of getURL method, of interface java.sql.ResultSet.
     */
    public void testGetURL() throws Exception {
        testGetXXX("getURL");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getURL(1);

            fail("Allowed getURL after close()");
        } catch (SQLException ex) {
            boolean test = getResultSetClosedErrorCode() == ex.getErrorCode()
                    || ex instanceof java.sql.SQLFeatureNotSupportedException;
            assertTrue("error code", test);
        }
    }

    /**
     * Test of updateRef method, of interface java.sql.ResultSet.
     */
    public void testUpdateRef() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        if (!isTestREF()) {
            return;
        }

        fail("REFS are not supported.");
    }

    /**
     * Test of updateBlob method, of interface java.sql.ResultSet.
     */
    public void testUpdateBlob() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = newUpdateableJdbcResultSet();

            rs.next();

            Blob blob = newConnection().createBlob();

            blob.setBytes(1, new byte[]{(byte)0});

            rs.updateBlob("c_blob", blob.getBinaryStream());
            rs.updateBlob("c_blob", blob.getBinaryStream(),1);
            rs.updateBlob("c_blob", blob);
            rs.updateRow();

        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateClob method, of interface java.sql.ResultSet.
     */
    public void testUpdateClob() throws Exception {
        if (!isTestUpdates()) {
            return;
        }
        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            Clob clob = newConnection().createClob();

            clob.setString(1, "Clob");

            rs.updateClob("clob_column", clob);

            StringReader reader = new StringReader("Clob");

            rs.updateClob("clob_column", reader);

            reader = new StringReader("Clob");

            rs.updateClob("clob_column", reader, "Clob".length());
            rs.updateRow();
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateArray method, of interface java.sql.ResultSet.
     */
    public void testUpdateArray() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        if (!isTestARRAY()) {
            return;
        }

        try {
            ResultSet rs = newUpdateableJdbcResultSet();

            rs.next();

            Integer[] values = new Integer[]{
                new Integer(4),
                new Integer(3),
                new Integer(2),
                new Integer(1)
            };

            Array array = newConnection().createArrayOf("INTEGER", values);

            rs.updateArray("array_column", array);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of getRowId method, of interface java.sql.ResultSet.
     */
    public void testGetRowId() throws Exception {
        if (!isTestROWID()) {
            return;
        }

        testGetXXX("getRowId");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getRowId(1);

            fail("Allowed getRowId after close()");
        } catch (SQLException ex) {
            boolean test = getResultSetClosedErrorCode() == ex.getErrorCode()
                    || ex instanceof java.sql.SQLFeatureNotSupportedException;
            assertTrue("error code", test);
        }
    }

    /**
     * Test of updateRowId method, of interface java.sql.ResultSet.
     */
    public void testUpdateRowId() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        if (!isTestROWID()) {
            return;
        }

        try {
            ResultSet rs = newUpdateableJdbcResultSet();

            rs.next();

            JDBCRowId rowId = new JDBCRowId("00000002");

            rs.updateRowId("id_column",  rowId);
        } catch (Exception e) {
            if (!(e instanceof SQLFeatureNotSupportedException)) {
                fail(e.toString());
            }
        }
    }

    /**
     * Test of getHoldability method, of interface java.sql.ResultSet.
     */
    public void testGetHoldability() throws Exception {
        ResultSet rs = newFOROJdbcResultSet();

        int result = rs.getHoldability();
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, result);

        rs.next();
        rs.close();

        try {
            rs.getHoldability();

            fail("Allowed getHoldability() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of isClosed method, of interface java.sql.ResultSet.
     */
    public void testIsClosed() throws Exception {
        ResultSet rs = newFOROJdbcResultSet();

        assertEquals(false, rs.isClosed());

        rs.close();

        assertEquals(true, rs.isClosed());
    }

    /**
     * Test of updateNString method, of interface java.sql.ResultSet.
     */
    public void testUpdateNString() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateNString("char_column", "NString");
            rs.updateNString("varchar_column", "updateNString");
            rs.updateNString("longvarchar_column", "updateNString");
            rs.updateNString("clob_column", "updateNString");

        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of updateNClob method, of interface java.sql.ResultSet.
     */
    public void testUpdateNClob() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            NClob nClob = newConnection().createNClob();

            nClob.setString(1, "NClob");

            rs.updateNClob("clob_column", nClob);

            StringReader reader = new StringReader("NClob");

            rs.updateNClob("clob_column", reader);

            reader = new StringReader("NClob");

            rs.updateNClob("clob_column", reader, "NClob".length());
            rs.updateRow();

        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of getNClob method, of interface java.sql.ResultSet.
     */
    public void testGetNClob() throws Exception {
        testGetXXX("getNClob");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getNClob(1);

            fail("Allowed getNClob after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getSQLXML method, of interface java.sql.ResultSet.
     */
    public void testGetSQLXML() throws Exception {
        testGetXXX("getSQLXML");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getSQLXML(1);

            fail("Allowed getSQLXML after close()");
        } catch (SQLException ex) {
            boolean test = getResultSetClosedErrorCode() == ex.getErrorCode()
                    || ex instanceof java.sql.SQLFeatureNotSupportedException;
            assertTrue("error code", test);
        }
    }

    /**
     * Test of updateSQLXML method, of interface java.sql.ResultSet.
     */
    public void testUpdateSQLXML() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        if (!isTestSQLXML()) {
            return;
        }

        // TODO.
        fail("SQLXML is not supported.");
    }

    /**
     * Test of getNString method, of interface java.sql.ResultSet.
     */
    public void testGetNString() throws Exception {
        testGetXXX("getNString");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getNString(1);

            fail("Allowed getNString after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of getNCharacterStream method, of interface java.sql.ResultSet.
     */
    public void testGetNCharacterStream() throws Exception {
        testGetXXX("getNCharacterStream");

        ResultSet rs = this.newFOROJdbcResultSet();

        rs.next();
        rs.close();

        try {
            rs.getNCharacterStream(1);

            fail("Allowed getNCharacterStream after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        }
    }

    /**
     * Test of updateNCharacterStream method, of interface java.sql.ResultSet.
     */
    public void testUpdateNCharacterStream() throws Exception {
        if (!isTestUpdates()) {
            return;
        }
        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            String value = "NChars";

            rs.updateNCharacterStream("clob_column", new StringReader(value));
            rs.updateNCharacterStream("clob_column", new StringReader(value),value.length());

        } catch (Exception e) {
            fail(e.toString());
        }
    }

    /**
     * Test of unwrap method, of interface java.sql.ResultSet.
     */
    public void testUnwrap() throws Exception {
        Class<?> iface = JDBCResultSet.class;
        ResultSet rs = newFOROJdbcResultSet();

        Object expResult = rs;
        Object result = rs.unwrap(iface);
        assertEquals(expResult, result);
    }

    /**
     * Test of isWrapperFor method, of interface java.sql.ResultSet.
     */
    public void testIsWrapperFor() throws Exception {
        Class<?> iface = JDBCResultSet.class;
        ResultSet rs = newFOROJdbcResultSet();

        boolean expResult = true;
        boolean result = rs.isWrapperFor(iface);
        assertEquals(expResult, result);
    }

    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }
}
