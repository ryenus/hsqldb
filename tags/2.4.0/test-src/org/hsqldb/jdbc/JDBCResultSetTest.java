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
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.text.MessageFormat;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;
import org.hsqldb.jdbc.testbase.SqlState;
import org.hsqldb.testbase.ConnectionFactory;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
@ForSubject(ResultSet.class)
public class JDBCResultSetTest extends BaseJdbcTestCase {

    public JDBCResultSetTest(String testName) {
        super(testName);
    }


    @Override
    protected void setUp() throws Exception {
        super.setUp();

        executeScript("setup-all_types-table.sql");
        executeScript("populate-all_types-table.sql");
        executeScript("setup-jdbc_required_get_xxx-table.sql");
    }

    public static Test suite() {
        return new TestSuite(JDBCResultSetTest.class);
    }
    private final String m_selectFromJdbcRrequiredGetXXX =
            "select c_get_xxx_name,"
            + "c_tinyint,"
            + "c_smallint,"
            + "c_integer,"
            + "c_bigint,"
            + "c_real,"
            + "c_float,"
            + "c_double,"
            + "c_decimal,"
            + "c_numeric,"
            + "c_bit,"
            + "c_boolean,"
            + "c_char,"
            + "c_varchar,"
            + "c_longvarchar,"
            + "c_binary,"
            + "c_varbinary,"
            + "c_longvarbinary,"
            + "c_date,"
            + "c_time,"
            + "c_timestamp,"
            + "c_array,"
            + "c_blob,"
            + "c_clob,"
            + "c_struct,"
            + "c_ref,"
            + "c_datalink,"
            + "c_java_object,"
            + "c_rowid,"
            + "c_nchar,"
            + "c_nvarchar,"
            + "c_longnvarchar,"
            + "c_nclob,"
            + "c_sqlxml,"
            + "c_other "
            + "from jdbc_required_get_xxx;";
    private final String m_selectFromJdbcRrequiredGetXXXByPrimaryKey =
            "select "
            + "c_tinyint,"
            + "c_smallint,"
            + "c_integer,"
            + "c_bigint,"
            + "c_real,"
            + "c_float,"
            + "c_double,"
            + "c_decimal,"
            + "c_numeric,"
            + "c_bit,"
            + "c_boolean,"
            + "c_char,"
            + "c_varchar,"
            + "c_longvarchar,"
            + "c_binary,"
            + "c_varbinary,"
            + "c_longvarbinary,"
            + "c_date,"
            + "c_time,"
            + "c_timestamp,"
            + "c_array,"
            + "c_blob,"
            + "c_clob,"
            + "c_struct,"
            + "c_ref,"
            + "c_datalink,"
            + "c_java_object,"
            + "c_rowid,"
            + "c_nchar,"
            + "c_nvarchar,"
            + "c_longnvarchar,"
            + "c_nclob,"
            + "c_sqlxml,"
            + "c_other "
            + "from jdbc_required_get_xxx "
            + "where c_get_xxx_name = {0};";
    private final String m_selectFromAllTypes =
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
            + "c_blob          as blob_column, "
            + "c_clob          as clob_column, "
            + "c_array         as array_column "
            + "from all_types";
    private final String[] m_allTypesColumnNames = new String[]{
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
        "c_blob",
        "c_clob",
        "c_array"
    };
    private final String[] m_allTypesColumnAliases = new String[]{
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
        "blob_column",
        "clob_column",
        "array_column"
    };

    protected String getSelectFromRequiredGetXXX() {
        return m_selectFromJdbcRrequiredGetXXX;
    }

    protected String getSelectFromRequiredGetXXXByPrimaryKey(String primaryKey) {
        return MessageFormat.format(
                m_selectFromJdbcRrequiredGetXXXByPrimaryKey,
                new Object[]{
                    org.hsqldb.lib.StringConverter.toQuotedString(primaryKey, '\'', true)});
    }

    protected String getSelectFromAllTypes() {
        return m_selectFromAllTypes;
    }

    protected String[] getAllTypesColumnNames() {
        return m_allTypesColumnNames;
    }

    protected String[] getAllTypesColumnAliases() {
        return m_allTypesColumnAliases;
    }

    /**
     */
    protected void handleTestGetXXX(String methodName) throws Exception {
        handleTestGetXXXBeforeFirst(methodName);
        handleTestGetXXXAfterLast(methodName);
        handleTestGetXXXAfterClose(methodName);

        String[] select = new String[]{
            //this.getSelectFromRequiredGetXXXByPrimaryKey(methodName),
            this.getSelectFromAllTypes()
        };

        for (int i = 0; i < select.length; i++) {
            printProgress("Info - Using Select:");
            printProgress(select[i]);
            handleTestGetXXX0(select[i], methodName);
        }
    }

    private void handleTestGetXXX0(String select, String methodName) throws SQLException, Exception {
        ResultSet rs = newForwardOnlyReadOnlyResultSet(select);
        assertTrue("next()", rs.next());
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String typeName = rsmd.getColumnTypeName(i);
            String columnName = rsmd.getColumnName(i);
            String columnClass = rsmd.getColumnClassName(i);
            int dataType = rsmd.getColumnType(i);
            boolean required = isRequiredGetXXX(methodName, dataType);
            try {
                Method getXXX = rs.getClass().getMethod(methodName, new Class<?>[]{int.class});
                Object value = getXXX.invoke(rs, new Object[]{new Integer(i)});
                Class<?> valueClass = (value == null) ? Void.class : value.getClass();
                printProgress("Info - Pass: " + columnName + "(" + typeName + ")");
                if (!required) {
                    warnGetXXXNotRequired(methodName, rs, i, columnName, typeName, columnClass, valueClass, value);
                }
            } catch (Exception e) {
                handleTestGetXXXException(e, required, columnName, typeName);
            }
        }
    }

    protected void warnGetXXXNotRequired(String methodName, ResultSet rs,
            int columnIndex, String columnName, String columnSqlType,
            String columnJavaType, Class<?> getXXXReturnType,
            Object getXXXReturnValue) throws Exception {
        Method getObjectMethod = rs.getClass().getMethod("getObject",
                new Class<?>[]{int.class});
        Object getObjectMethodReturnValue = getObjectMethod.invoke(rs,
                new Object[]{new Integer(columnIndex)});
        printProgress("****************************************");
        printProgress("Warn - JDBC 4.0, Table B-6 indicates this "
                + methodName
                + " conversion is not required:");
        printProgress("From SQL: " + columnName + "{");
        printProgress(" SQL type=" + columnSqlType);
        printProgress("Java type=" + columnJavaType);
        printProgress("    value=\"" + getObjectMethodReturnValue + "\"");
        printProgress("}");
        printProgress("To Java type : " + getXXXReturnType + "{");
        printProgress("    value=\"" + getXXXReturnValue + "\"");
        printProgress("}");
        printProgress("****************************************");
    }

    protected void handleTestGetXXXException(Exception e,
            boolean wasRequiredGetXXX, String columnName, String sqlTypeName) {
        Throwable t = e;

        while (t instanceof InvocationTargetException) {
            t = ((InvocationTargetException) t).getTargetException();
        }

        if (!(t instanceof SQLException)) {
            printException(t.fillInStackTrace());
            Throwable t2 = t.getCause();
            if (t2 != null) {
                printException(t2);
            }
            fail(MessageFormat.format(
                    "{0}({1}) : {2}",
                    new Object[]{
                        columnName,
                        sqlTypeName,
                        t}));
        }

        SQLException ex = (SQLException) t;

        if (wasRequiredGetXXX) {
            if (ex.getErrorCode()
                    != getIncompatibleDataTypeConversionErrorCode()) {
                printException(ex);
                fail(MessageFormat.format(
                        "{0}({1}) : {2} : [Error Code: {3}][SQL State: {4}]",
                        new Object[]{
                            columnName,
                            sqlTypeName,
                            ex,
                            ex.getErrorCode(),
                            ex.getSQLState()}));
            } else {
//                fail(MessageFormat.format(
//                        "{0}({1}) : {2}",
//                        new Object[]{
//                            columnName,
//                            sqlTypeName,
//                            ex}));
                printProgress(MessageFormat.format(
                        "Warn - Pass: {0}({1}) : {2}",
                        new Object[]{
                            columnName,
                            sqlTypeName,
                            ex}));
            }
        } else {
            printProgress(MessageFormat.format(
                    "Info - Pass: {0}({1}) : {2}",
                    new Object[]{
                        columnName,
                        sqlTypeName,
                        ex}));
        }
    }

    protected void handleTestGetXXXAfterClose(String methodName) throws Exception {
        ResultSet rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

        assertTrue("next()", rs.next());

        rs.close();

        String[] columnAliases = getAllTypesColumnAliases();

        for (int i = 0; i < columnAliases.length; i++) {
            Throwable t = null;

            try {
                rs.getClass().getMethod(
                        methodName,
                        new Class<?>[]{String.class}).invoke(
                        rs,
                        new Object[]{columnAliases[i]});
            } catch (InvocationTargetException ite) {
                t = ite;

                while (t instanceof InvocationTargetException) {
                    t = ((InvocationTargetException) t).getTargetException();
                }
            }

            if (t == null) {
                fail("Allowed " + methodName + "(java.lang.String) after close()");
            } else if (t instanceof SQLException) {
                checkResultSetClosedOrNotSupportedException((SQLException) t);
            } else {
                throw (t instanceof Exception)
                        ? ((Exception) t)
                        : new RuntimeException(t.toString(), t);
            }
        }
    }

    protected void handleTestGetXXXBeforeFirst(String methodName) throws Exception {
        ResultSet rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

        assertTrue("isBeforeFirst", rs.isBeforeFirst());
        String[] columnAliases = getAllTypesColumnAliases();

        for (int i = 0; i < columnAliases.length; i++) {
            Throwable t = null;

            try {
                rs.getClass().getMethod(
                        methodName,
                        new Class<?>[]{String.class}).invoke(
                        rs,
                        new Object[]{columnAliases[i]});
            } catch (InvocationTargetException ite) {
                t = ite;

                while (t instanceof InvocationTargetException) {
                    t = ((InvocationTargetException) t).getTargetException();
                }
            }

            if (t == null) {
                fail("Allowed " + methodName + "(java.lang.String) before first");
            } else if (t instanceof SQLException) {
                checkResultSetBeforeFirstOrNotSupportedException((SQLException) t);
            } else {
                throw (t instanceof Exception)
                        ? ((Exception) t)
                        : new RuntimeException(t.toString(), t);
            }
        }
    }

    protected void handleTestGetXXXAfterLast(String methodName) throws Exception {
        ResultSet rs = newScrollableInsensitiveReadOnlyResultSet(
                getSelectFromAllTypes());

        assertTrue("isBeforeFirst", rs.isBeforeFirst());
        rs.afterLast();
        assertTrue("isAfterLast", rs.isAfterLast());
        String[] columnAliases = getAllTypesColumnAliases();

        for (int i = 0; i < columnAliases.length; i++) {
            Throwable t = null;

            try {
                rs.getClass().getMethod(
                        methodName,
                        new Class<?>[]{String.class}).invoke(
                        rs,
                        new Object[]{columnAliases[i]});
            } catch (InvocationTargetException ite) {
                t = ite;

                while (t instanceof InvocationTargetException) {
                    t = ((InvocationTargetException) t).getTargetException();
                }
            }

            if (t == null) {
                fail("Allowed " + methodName + "(java.lang.String) after last");
            } else if (t instanceof SQLException) {
                checkResultSetAfterLastOrNotSupportedException((SQLException) t);
            } else {
                throw (t instanceof Exception)
                        ? ((Exception) t)
                        : new RuntimeException(t.toString(), t);
            }
        }
    }

    /**
     * Test of next method, of interface java.sql.ResultSet.
     */
    @OfMethod("next()")
    public void testNext() throws Exception {
        ResultSet rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

        assertTrue("next()", rs.next());

        while (rs.next()) {
        }

        assertFalse("next()", rs.next());

        rs.close();

        try {
            rs.next();

            fail("Allowed next() after close().");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of close method, of interface java.sql.ResultSet.
     */
    @OfMethod("close()")
    public void testClose() throws Exception {
        ResultSet rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

        assertFalse("isClosed", rs.isClosed());

        try {
            rs.close();
        } catch (SQLException ex) {
            fail("Failed to close result set: " + ex);
        }

        assertTrue("isClosed", rs.isClosed());

        try {
            rs.close();
        } catch (SQLException ex) {
            fail("Failed to invoke subsequent close on already closed result set: " + ex);
        }

        assertTrue("isClosed", rs.isClosed());
    }

    /**
     * Test of wasNull method, of interface java.sql.ResultSet.
     */
    @OfMethod("wasNull()")
    public void testWasNull() throws Exception {
        ResultSet rs = newScrollableInsensitiveReadOnlyResultSet(
                getSelectFromAllTypes());

        assertTrue("next()", rs.next());

        String[] columnAliases = getAllTypesColumnAliases();

        for (int i = 0; i < columnAliases.length; i++) {
            String columnAlias = columnAliases[i];
            Object o = rs.getObject(columnAlias);
            assertFalse(columnAliases[i] + "(" + o + ")", rs.wasNull());
        }

        rs.last();

        for (int i = 0; i < columnAliases.length; i++) {
            String columnAlias = columnAliases[i];
            Object o = rs.getObject(columnAlias);
            assertTrue(columnAliases[i] + "(" + o + ")", rs.wasNull());
        }

        rs.close();

        assertTrue("isClosed()", rs.isClosed());

        try {
            rs.wasNull();

            fail("Allowed wasNull() after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of getString method, of interface java.sql.ResultSet.
     */
    @OfMethod("getString(java.lang.String)")
    public void testGetString() throws Exception {
        handleTestGetXXX("getString");
    }

    /**
     * Test of getBoolean method, of interface java.sql.ResultSet.
     */
    @OfMethod("getBoolean(java.lang.String)")
    public void testGetBoolean() throws Exception {
        handleTestGetXXX("getBoolean");
    }

    /**
     * Test of getByte method, of interface java.sql.ResultSet.
     */
    @OfMethod("getByte(java.lang.String)")
    public void testGetByte() throws Exception {
        handleTestGetXXX("getByte");
    }

    /**
     * Test of getShort method, of interface java.sql.ResultSet.
     */
    @OfMethod("getShort(java.lang.String)")
    public void testGetShort() throws Exception {
        handleTestGetXXX("getShort");
    }

    /**
     * Test of getInt method, of interface java.sql.ResultSet.
     */
    @OfMethod("getInt(java.lang.String)")
    public void testGetInt() throws Exception {
        handleTestGetXXX("getInt");
    }

    /**
     * Test of getLong method, of interface java.sql.ResultSet.
     */
    @OfMethod("getLong(java.lang.String)")
    public void testGetLong() throws Exception {
        handleTestGetXXX("getLong");
    }

    /**
     * Test of getFloat method, of interface java.sql.ResultSet.
     */
    @OfMethod("getFloat(java.lang.String)")
    public void testGetFloat() throws Exception {
        handleTestGetXXX("getFloat");
    }

    /**
     * Test of getDouble method, of interface java.sql.ResultSet.
     */
    @OfMethod("getDouble(java.lang.String)")
    public void testGetDouble() throws Exception {
        handleTestGetXXX("getDouble");
    }

    /**
     * Test of getBigDecimal method, of interface java.sql.ResultSet.
     */
    @OfMethod("getBigDecimal(java.lang.String)")
    public void testGetBigDecimal() throws Exception {
        handleTestGetXXX("getBigDecimal");
    }

    /**
     * Test of getBytes method, of interface java.sql.ResultSet.
     */
    @OfMethod("getBytes(java.lang.String)")
    public void testGetBytes() throws Exception {
        handleTestGetXXX("getBytes");
    }

    /**
     * Test of getDate method, of interface java.sql.ResultSet.
     */
    @OfMethod("getDate(java.lang.String)")
    public void testGetDate() throws Exception {
        handleTestGetXXX("getDate");
    }

    /**
     * Test of getTime method, of interface java.sql.ResultSet.
     */
    @OfMethod("getTime(java.lang.String)")
    public void testGetTime() throws Exception {
        handleTestGetXXX("getTime");
    }

    /**
     * Test of getTimestamp method, of interface java.sql.ResultSet.
     */
    @OfMethod("getTimestamp(java.lang.String)")
    public void testGetTimestamp() throws Exception {
        handleTestGetXXX("getTimestamp");
    }

    /**
     * Test of getAsciiStream method, of interface java.sql.ResultSet.
     */
    @OfMethod("getAsciiStream(java.lang.String)")
    public void testGetAsciiStream() throws Exception {
        handleTestGetXXX("getAsciiStream");
    }

    /**
     * Test of getUnicodeStream method, of interface java.sql.ResultSet.
     */
    @SuppressWarnings("deprecation")
    @OfMethod("getUnicodeStream(java.lang.String)")
    public void testGetUnicodeStream() throws Exception {
        handleTestGetXXX("getUnicodeStream");
    }

    /**
     * Test of getBinaryStream method, of interface java.sql.ResultSet.
     */
    @OfMethod("getBinaryStream(java.lang.String)")
    public void testGetBinaryStream() throws Exception {
        handleTestGetXXX("getBinaryStream");

        ResultSet rs = this.newForwardOnlyReadOnlyResultSet(
                getSelectFromAllTypes());

        rs.next();
        rs.close();

        try {
            rs.getBinaryStream("c_longvarbinary");

            fail("Allowed getBinaryStream after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of getWarnings method, of interface java.sql.ResultSet.
     */
    @OfMethod("getWarnings()")
    public void testGetWarnings() throws Exception {
        ResultSet rs = newForwardOnlyReadOnlyResultSet(
                getSelectFromAllTypes());

        java.sql.SQLWarning warnings = rs.getWarnings();

        assertNull("warnings", warnings);

        rs.next();
        rs.close();

        try {
            rs.getWarnings();

            fail("Allowed getWarnings() after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }

        stubTestResult("TODO: cases to test spec'd generation of warnings");
    }

    /**
     * Test of clearWarnings method, of interface java.sql.ResultSet.
     */
    @OfMethod("clearWarnings()")
    public void testClearWarnings() throws Exception {
        ResultSet rs = newForwardOnlyReadOnlyResultSet(
                getSelectFromAllTypes());

        rs.clearWarnings();

        assertEquals(null, rs.getWarnings());

        rs.close();
        try {
            rs.clearWarnings();

            fail("Allowed clearWarnings() after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of getCursorName method, of interface java.sql.ResultSet.
     */
    @OfMethod("getCursorName()")
    public void testGetCursorName() throws Exception {
        ResultSet rs = newForwardOnlyReadOnlyResultSet(
                getSelectFromAllTypes());

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
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of getMetaData method, of interface java.sql.ResultSet.
     */
    @OfMethod("getMetaData()")
    public void testGetMetaData() throws Exception {
        ResultSet rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());
        ResultSetMetaData rsmd = rs.getMetaData();

        assertNotNull(rsmd);

        rs.close();

        try {
            rs.getMetaData();

            fail("Allowed getMetaData after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
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
    @OfMethod("getObject(java.lang.String)")
    public void testGetObject() throws Exception {
        handleTestGetXXX("getObject");

        ResultSet rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

        rs.next();
        rs.close();

        try {
            rs.getObject("c_blob");

            fail("Allowed getObject after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of findColumn method, of interface java.sql.ResultSet.
     */
    @OfMethod("findColumn(java.lang.String)")
    public void testFindColumn() throws Exception {
        ResultSet rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());
        String[] columnAliases = getAllTypesColumnAliases();

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
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of getCharacterStream method, of interface java.sql.ResultSet.
     */
    @OfMethod("getCharacterStream(java.lang.String)")
    public void testGetCharacterStream() throws Exception {
        handleTestGetXXX("getCharacterStream");

        ResultSet rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

        rs.next();
        rs.close();

        try {
            rs.getCharacterStream(1);

            fail("Allowed getCharacterStream after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of isBeforeFirst method, of interface java.sql.ResultSet.
     */
    @OfMethod("isBeforeFirst()")
    public void testIsBeforeFirst() throws Exception {
        ResultSet rs = newScrollableInsensitiveReadOnlyResultSet(
                getSelectFromAllTypes());

        try {
            rs.getObject(1);
            fail("get succeeded while before first");
        } catch (Exception e) {
        }

        assertTrue("isBeforeFirst()", rs.isBeforeFirst());
        assertTrue("next()", rs.next());
        assertFalse("isBeforeFirst()", rs.isBeforeFirst());

        rs.previous();

        try {
            rs.getObject(1);
            fail("get succeeded while before first");
        } catch (Exception e) {
        }

        assertTrue("isBeforeFirst()", rs.isBeforeFirst());
        assertTrue("next()", rs.next());
        assertFalse("isBeforeFirst()", rs.isBeforeFirst());

        rs.beforeFirst();

        try {
            rs.getObject(1);
            fail("get succeeded while before first");
        } catch (Exception e) {
        }

        assertTrue("isBeforeFirst()", rs.isBeforeFirst());

        rs.close();

        try {
            rs.isBeforeFirst();

            fail("Allowed isBeforeFirst() after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of isAfterLast method, of interface java.sql.ResultSet.
     */
    @OfMethod("isAfterLast()")
    public void testIsAfterLast() throws Exception {
        ResultSet rs = newScrollableInsensitiveReadOnlyResultSet(
                getSelectFromAllTypes());

        assertFalse("isAfterLast()", rs.isAfterLast());
        assertTrue("next()", rs.next());
        assertFalse("isAfterLast()", rs.isAfterLast());

        while (rs.next()) {
        }

        assertTrue("isAfterLast()", rs.isAfterLast());

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

        assertFalse("isAfterLast()", rs.isAfterLast());
        assertTrue("isLast()", rs.isLast());
        assertFalse("next()", rs.next());
        assertTrue("isAfterLast()", rs.isAfterLast());
        assertTrue("previous()", rs.previous());

        assertFalse("isAfterLast()", rs.isAfterLast());

        rs.afterLast();

        assertTrue("isAfterLast()", rs.isAfterLast());

        rs.previous();

        assertFalse("isAfterLast()", rs.isAfterLast());
        assertTrue("isLast()", rs.isLast());

        rs.close();

        try {
            rs.afterLast();

            fail("Allowed afterLast() after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of isFirst method, of interface java.sql.ResultSet.
     */
    @OfMethod("isFirst()")
    public void testIsFirst() throws Exception {
        ResultSet rs = this.newScrollableInsensitiveReadOnlyResultSet(
                getSelectFromAllTypes());

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
    @OfMethod("isLast()")
    public void testIsLast() throws Exception {
        ResultSet rs = this.newScrollableInsensitiveReadOnlyResultSet(
                getSelectFromAllTypes());

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
    @OfMethod("beforeFirst()")
    public void testBeforeFirst() throws Exception {
        ResultSet rs = this.newScrollableInsensitiveReadOnlyResultSet(
                getSelectFromAllTypes());

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
    @OfMethod("afterLast()")
    public void testAfterLast() throws Exception {
        ResultSet rs = this.newScrollableInsensitiveReadOnlyResultSet(
                getSelectFromAllTypes());

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
    @OfMethod("first()")
    public void testFirst() throws Exception {
        ResultSet rs = newScrollableInsensitiveReadOnlyResultSet(
                getSelectFromAllTypes());

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
    @OfMethod("last()")
    public void testLast() throws Exception {
        ResultSet rs = this.newScrollableInsensitiveReadOnlyResultSet(
                getSelectFromAllTypes());

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
    @OfMethod("getRow()")
    public void testGetRow() throws Exception {
        ResultSet rs = this.newScrollableInsensitiveReadOnlyResultSet(
                getSelectFromAllTypes());
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
    @OfMethod("absolute(int)")
    public void testAbsolute() throws Exception {
        ResultSet rs = this.newScrollableInsensitiveReadOnlyResultSet(
                getSelectFromAllTypes());
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
    @OfMethod("relative(int)")
    public void testRelative() throws Exception {
        ResultSet rs = this.newScrollableInsensitiveReadOnlyResultSet(
                getSelectFromAllTypes());

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
    @OfMethod("previous()")
    public void testPrevious() throws Exception {
        ResultSet rs = this.newScrollableInsensitiveReadOnlyResultSet(
                getSelectFromAllTypes());

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
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of setFetchDirection method, of interface java.sql.ResultSet.
     */
    @OfMethod("setFetchDirection(int)")
    public void testSetFetchDirection() throws Exception {
        try {
            handleTestSetDirectionAfterClose();
        } finally {
             connectionFactory().closeRegisteredObjects();
        }

        for (int i = 0; i < s_rstype.length; i++) {
            String rsTypeLabel = s_rstype[i][0];
            int rsType = getFieldValue(s_rstype[i][1]);

            printProgress("result set type: " + rsTypeLabel);

            try {
                handleTestSetValidFetchDirection(rsType);
            } finally {
                connectionFactory().closeRegisteredObjects();
            }
        }

        try {
            handleTestSetInvalidFetchDirection();
        } finally {
             connectionFactory().closeRegisteredObjects();
        }
    }

    protected void handleTestSetDirectionAfterClose() throws Exception, SQLException {
        ResultSet rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

        assertTrue(rs.next());

        rs.close();

        try {
            rs.setFetchDirection(ResultSet.FETCH_FORWARD);
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    protected void handleTestSetInvalidFetchDirection() throws Exception {
        ResultSet rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());
        for (int i = 0; i < s_rsholdability.length; i++) {
            String holdabilityLabel = s_rsholdability[i][0];
            int holdabilityValue = getFieldValue(s_rsholdability[i][1]);
            try {
                rs.setFetchDirection(holdabilityValue);
                fail("ResultSet accepted illegal fetch direction value: " + holdabilityLabel);
            } catch (SQLException ex) {
            }
        }
        for (int i = 0; i < s_rsconcurrency.length; i++) {
            String concurrencyLabel = s_rsconcurrency[i][0];
            int concurrencyValue = getFieldValue(s_rsconcurrency[i][1]);
            try {
                rs.setFetchDirection(concurrencyValue);
                fail("ResultSet accepted illegal concurrency value: " + concurrencyLabel);
            } catch (SQLException ex) {
            }
        }
    }

    protected void handleTestSetValidFetchDirection(int rsType) throws Exception {
        ResultSet rs = newResultSet(rsType, ResultSet.CONCUR_READ_ONLY, getSelectFromAllTypes());
        printProgress("Fetch direction: ResultSet.FETCH_FORWARD");
        try {
            rs.setFetchDirection(ResultSet.FETCH_FORWARD);
            if (rs.getFetchDirection() != ResultSet.FETCH_FORWARD) {
                printProgress("Warn - getFetchDirection() != ResultSet.FETCH_FORWARD");
            }
        } catch (SQLException ex) {
            if (rsType == ResultSet.TYPE_FORWARD_ONLY) {
                fail(MessageFormat.format("TYPE_FORWARD_ONLY ResultSet rejected "
                        + "setFetchDirection(ResultSet.FETCH_FORWARD)\n"
                        + "{0}: SQLSTATE {1}, ERROR CODE {2}",
                        new Object[]{
                    ex.toString(),
                    ex.getSQLState(),
                    ex.getErrorCode()}));
            }
        }
        printProgress("Fetch direction: ResultSet.FETCH_REVERSE");
        try {
            rs.setFetchDirection(ResultSet.FETCH_REVERSE);
            if (rsType == ResultSet.TYPE_FORWARD_ONLY) {
                fail("TYPE_FORWARD_ONLY ResultSet violated JDBC contract to "
                        + "reject setFetchDirection(ResultSet.FETCH_REVERSE)");
            }
            if (rs.getFetchDirection() != ResultSet.FETCH_REVERSE) {
                printProgress("Warn - getFetchDirection() != ResultSet.FETCH_REVERSE");
            }
        } catch (SQLException ex) {
            printWarning(ex);
        }
        printProgress("Fetch direction: ResultSet.FETCH_UNKNOWN");
        try {
            rs.setFetchDirection(ResultSet.FETCH_UNKNOWN);
            if (rsType == ResultSet.TYPE_FORWARD_ONLY) {
                fail("TYPE_FORWARD_ONLY ResultSet violated JDBC contract to "
                        + "reject setFetchDirection(ResultSet.FETCH_UNKNOWN)");
            }
            if (rs.getFetchDirection() != ResultSet.FETCH_UNKNOWN) {
                printProgress("Warn - getFetchDirection() != ResultSet.FETCH_UNKNOWN");
            }
        } catch (SQLException ex) {
            printWarning(ex);
        }
    }

    /**
     * Test of getFetchDirection method, of interface java.sql.ResultSet.
     */
    @OfMethod("getFetchDirection()")
    public void testGetFetchDirection() throws Exception {
        ResultSet rs = this.newScrollableInsensitiveReadOnlyResultSet(
                getSelectFromAllTypes());

        assertEquals(
                "fetch direction",
                ResultSet.FETCH_FORWARD,
                rs.getFetchDirection());

        rs.close();

        try {
            rs.getFetchDirection();

            fail("Allowed getFetchDirection() after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of setFetchSize method, of interface java.sql.ResultSet.
     */
    @OfMethod("setFetchSize()")
    public void testSetFetchSize() throws Exception {
        ResultSet rs = newForwardOnlyReadOnlyResultSet(
                getSelectFromAllTypes());

        rs.clearWarnings();

        try {
            rs.setFetchSize(0);
        } catch (SQLException se) {
            fail("setFetchSize failed to accept value: 0");
        }

        try {
            rs.setFetchSize(-1);

            fail("setFetchSize accepted a negative value");
        } catch (SQLException se) {
        }

        rs.clearWarnings();

        int fetchSize = rs.getFetchSize();

        fetchSize = (fetchSize == 0) ? 1000 : fetchSize + 100;

        rs.setFetchSize(fetchSize);
    }

    /**
     * Test of getFetchSize method, of interface java.sql.ResultSet.
     */
    @OfMethod("getFetchSize()")
    public void testGetFetchSize() throws Exception {
        ResultSet rs = newForwardOnlyReadOnlyResultSet(
                getSelectFromAllTypes());

        int expResult = 0;
        int result = rs.getFetchSize();
        assertEquals("fetch size", expResult, result);

        rs.close();

        try {
            rs.getFetchSize();

            fail("Allowed getFetchSize() after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of getType method, of interface java.sql.ResultSet.
     */
    @OfMethod("getType()")
    public void testGetType_FORWARD_ONLY() throws Exception {
        ResultSet rs = newReadOnlyResultSet(ResultSet.TYPE_FORWARD_ONLY,
                getSelectFromAllTypes());

        SQLWarning warnings = rs.getStatement().getConnection().getWarnings();

        if (warnings == null) {
            assertEquals("getType() without generated warning",
                    ResultSet.TYPE_FORWARD_ONLY,
                    rs.getType());
        }

        rs.next();
        rs.close();

        try {
            rs.getType();

            fail("Allowed getType after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of getType method, of interface java.sql.ResultSet.
     */
    @OfMethod("getType()")
    public void testGetType_SCROLL_INSENSITIVE() throws Exception {
        ResultSet rs = newReadOnlyResultSet(ResultSet.TYPE_SCROLL_INSENSITIVE,
                getSelectFromAllTypes());

        SQLWarning warnings = rs.getStatement().getConnection().getWarnings();

        if (warnings == null) {
            assertEquals("getType() without generated warning",
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    rs.getType());
        }

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
    @OfMethod("getType()")
    public void testGetType_SCROLL_SENSITIVE() throws Exception {

        ResultSet rs = newReadOnlyResultSet(ResultSet.TYPE_SCROLL_SENSITIVE,
                getSelectFromAllTypes());

        SQLWarning warnings = rs.getStatement().getConnection().getWarnings();

        if (warnings == null) {
            assertEquals("getType() without generated warning.",
                    ResultSet.TYPE_SCROLL_SENSITIVE,
                    rs.getType());
        }

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
    @OfMethod("getConcurrency()")
    public void testGetConcurrency() throws Exception {
        ResultSet rs = null;
        ConnectionFactory connectionFactory = connectionFactory();

        try {
            rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

            assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        } finally {
            connectionFactory.closeRegisteredObjects();
        }

        // ***NOTE***
        //
        // With ResultSet.CONCUR_UPDATABLE requested, it reasonable to expect
        // that the typical JDBC back end behavior will be to perform the
        // underlying select "for update", meaning when the query returns,
        // the selected rows(s) will be locked against subsequent read/write
        // access from other sessions.
        //
        // Thus, to avoid blocking forever or throwing exceptions in subsequent
        // select statements in this test, we must either reuse the same
        // connection in subsequent queries; or commit explicitly the
        // transaction associated with the updatable result set; or close
        // the updatable result set's connection (which will also have the
        // implicit effect of ending the associated transaction);
        //
        // TODO : Maybe these kinds of tests would be better
        //        done "per test method" style or using a combinatoric style
        //        test case (a test case with
        //        only one test method (runTest) and a suite built of many
        //        called many times with different
        //        parameter values.
        //
        try {
            rs = newResultSet(
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_UPDATABLE,
                    getSelectFromAllTypes());

            assertEquals(
                    ResultSet.CONCUR_UPDATABLE,
                    rs.getConcurrency());
        } finally {
            connectionFactory.closeRegisteredObjects();
        }

        try {
            rs = this.newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

            rs.close();
            rs.getConcurrency();

            fail("Allowed getConcurrency() after close()");
        } catch (SQLException ex) {
            assertEquals(
                    "error code",
                    ex.getErrorCode(),
                    -ErrorCode.X_24501);
        } finally {
            connectionFactory.closeRegisteredObjects();
        }
    }

    /**
     * Test of rowUpdated method, of interface java.sql.ResultSet.
     */
    @OfMethod("rowUpdated()")
    public void testRowUpdated() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        Connection conn = newConnection();
        DatabaseMetaData metaData = conn.getMetaData();

        if (!metaData.updatesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE)
                & !metaData.ownUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE)) {
            return;
        }

        try {
            ResultSet rs = this.newResultSet(
                    ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_UPDATABLE,
                    getSelectFromAllTypes());

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
    @OfMethod("rowInserted()")
    public void testRowInserted() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        Connection conn = newConnection();
        DatabaseMetaData metaData = conn.getMetaData();
        int tss = ResultSet.TYPE_SCROLL_SENSITIVE;
        int cru = ResultSet.CONCUR_UPDATABLE;
        boolean insertsDetected = metaData.insertsAreDetected(tss);
        boolean ownInsertsVisible = metaData.ownInsertsAreVisible(tss);
        boolean othersInsertsVisible = metaData.othersInsertsAreVisible(tss);

        if (!insertsDetected) {
            return;
        }

        if (!(ownInsertsVisible || othersInsertsVisible)) {
            return;
        }

        try {
            ResultSet rs = newResultSet(tss, cru, getSelectFromAllTypes());

            if (ownInsertsVisible) {
                // TODO:  test rowInserted() against visible own insert.
                stubTestResult("TODO: test rowInserted() against visible own insert.");
            }
            if (othersInsertsVisible) {
                // TODO:  test rowInserted() against visible other's insert.
                stubTestResult("TODO: test rowInserted() against visible other's insert.");
            }
        } catch (Exception e) {
            fail(e.toString());
        }

    }

    /**
     * Test of rowDeleted method, of interface java.sql.ResultSet.
     */
    @OfMethod("rowInserted()")
    public void testRowDeleted() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        Connection conn = newConnection();
        DatabaseMetaData dbmd = conn.getMetaData();

        if (!dbmd.deletesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE)) {
            return;
        }

        boolean detectedForwardOnly = dbmd.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY);
        boolean detectedScrollInsensitive = dbmd.deletesAreDetected(ResultSet.TYPE_SCROLL_INSENSITIVE);
        boolean detectedScrollSensitive = dbmd.deletesAreDetected(ResultSet.TYPE_SCROLL_SENSITIVE);


        if (detectedForwardOnly) {
            if (dbmd.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY)) {
                // test self delete - forward only
                stubTestResult("test detection of self's visible delete - forward only");
            }

            if (dbmd.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY)) {
                // test other's delete - forward only
                stubTestResult("TODO: test detection of other's visible delete - forward only");
            }
        }

        if (detectedScrollInsensitive) {
            if (dbmd.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE)) {
                // test self delete - forward only
                stubTestResult("test detection of self's visible delete - scroll insensitive");
            }

            if (dbmd.othersDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE)) {
                // test other's delete - forward only
                stubTestResult("TODO: test detection of other's visible delete - scroll insensitive");
            }
        }

        if (detectedScrollSensitive) {
            if (dbmd.ownDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE)) {
                // test self delete - forward only
                stubTestResult("test detection of self's visible delete - scroll insensitive");
            }

            if (dbmd.othersDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE)) {
                // test other's delete - forward only
                stubTestResult("TODO: test detection of other's visible delete - scroll insensitive");
            }
        }
    }

    /**
     * Test of updateNull method, of interface java.sql.ResultSet.
     */
    @OfMethod("rowInserted()")
    public void testUpdateNull() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

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
            ResultSet rs = newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

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
            ResultSet rs = this.newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

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
            ResultSet rs = newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

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
            ResultSet rs = newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

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
            ResultSet rs = this.newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

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
            ResultSet rs = this.newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

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
            ResultSet rs = this.newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

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
            ResultSet rs = this.newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

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
            ResultSet rs = this.newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

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

            rs.updateRow();
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
            ResultSet rs = this.newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

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
            ResultSet rs = newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

            rs.next();

            rs.updateDate("date_column", java.sql.Date.valueOf("2005-12-14"));

            rs.updateRow();
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
            ResultSet rs = this.newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

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
            ResultSet rs = this.newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

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
            ResultSet rs = this.newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

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
            ResultSet rs = this.newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

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
            ResultSet rs = this.newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

            rs.next();

            rs.updateCharacterStream(
                    "char_column",
                    new java.io.StringReader("updateCharacterStream"), 10);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    public static final class SerVal implements java.io.Serializable {

        private static final long serialVersionUID = 1L;
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
            ResultSet rs = this.newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

            rs.next();

            SerVal value = new SerVal();

            rs.updateObject("object_column", value);

            rs.updateRow();

            rs.getStatement().getConnection().commit();
        } catch (Exception e) {
            fail(e.toString());


        }
    }

    /**
     * Test of insertRow method, of interface java.sql.ResultSet.
     */
    public void testInsertRow() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        ResultSet rs;

        try {
            rs = this.newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());
            rs.close();
            rs.moveToInsertRow();

            rs.updateInt(1, 999999);

            rs.insertRow();

            fail("Allowed insertRow() after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        } finally {
            // note: typically not possible to reselect selected rows
            // in a different SQL session without somehow ending the transaction
            // associated with an updateable result set.
            connectionFactory().closeRegisteredObjects();
        }

        try {
            rs = this.newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

            rs.moveToInsertRow();

            int columnCount = rs.getMetaData().getColumnCount();

            rs.updateInt(1, 999999);

            for (int i = 2; i <= columnCount; i++) {
                rs.updateNull(i);
            }

            rs.insertRow();

            //--

            rs.moveToInsertRow();
            rs.updateInt(1, 1000000);

            for (int i = 2; i <= columnCount; i++) {
                rs.updateNull(i);
            }

            rs.insertRow();
            rs.getStatement().getConnection().commit();
            rs.close();
        } catch (Exception ex) {
            fail(ex.toString());
        } finally {
            connectionFactory().closeRegisteredObjects();
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
            ResultSet rs = this.newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

            rs.next();

            int columnCount = rs.getMetaData().getColumnCount();

            rs.updateInt(1, 1000000);

            for (int i = 2; i <= columnCount; i++) {
                rs.updateNull(i);
            }

            rs.updateRow();
        } catch (Exception ex) {
            fail(ex.toString());
        } finally {
            // careful!  need to end transaction so we can reselect
            // the same row again, in later code.
            connectionFactory().closeRegisteredObjects();
        }
    }

    /**
     * Test of deleteRow method, of interface java.sql.ResultSet.
     */
    public void testDeleteRow() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        ResultSet rs = newScrollableInsensitiveUpdateableResultSet(
                getSelectFromAllTypes());

        rs.next();
        rs.close();

        try {
            rs.deleteRow();

            fail("Allowed deleteRow() after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        } finally {
            connectionFactory().closeRegisteredObjects();
        }

        try {
            rs = newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

            rs.next();

            rs.deleteRow();
        } catch (Exception ex) {
            fail(ex.toString());
        } finally {
            connectionFactory().closeRegisteredObjects();
        }
    }

    /**
     * Test of refreshRow method, of interface java.sql.ResultSet.
     */
    public void testRefreshRow() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

            rs.next();

            rs.refreshRow();
        } catch (Exception ex) {
            printException(ex);
            fail(ex.toString());
        } finally {
            connectionFactory().closeRegisteredObjects();
        }

        ResultSet rs = newReadOnlyResultSet(ResultSet.TYPE_SCROLL_SENSITIVE,
                getSelectFromAllTypes());

        rs.next();
        rs.close();

        try {
            rs.refreshRow();

            fail("Allowed refreshRow() after close().");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        } finally {
            connectionFactory().closeRegisteredObjects();
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

        try {
            ResultSet rs = newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

            rs.next();

            int columnCount = rs.getMetaData().getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                rs.updateNull(i);
            }

            rs.cancelRowUpdates();
        } catch (Exception ex) {
            fail(ex.toString());
        } finally {
            connectionFactory().closeRegisteredObjects();
        }

        ResultSet rs = newScrollableInsensitiveUpdateableResultSet(
                getSelectFromAllTypes());

        rs.updateNull(1);
        rs.close();
        try {
            rs.cancelRowUpdates();

            fail("Allowed cancelRowUpdates() after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        } finally {
            connectionFactory().closeRegisteredObjects();
        }
    }

    /**
     * Test of moveToInsertRow method, of interface java.sql.ResultSet.
     */
    public void testMoveToInsertRow() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

            rs.moveToInsertRow();
        } catch (Exception ex) {
            fail(ex.toString());
        } finally {
            connectionFactory().closeRegisteredObjects();
        }

        ResultSet rs = newScrollableInsensitiveUpdateableResultSet(
                getSelectFromAllTypes());

        rs.close();

        try {
            rs.moveToInsertRow();

            fail("Allowed moveToInsertRow() after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        } finally {
            connectionFactory().closeRegisteredObjects();
        }
    }

    /**
     * Test of moveToCurrentRow method, of interface java.sql.ResultSet.
     */
    public void testMoveToCurrentRow() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

            rs.moveToInsertRow();
            rs.moveToCurrentRow();
        } catch (Exception ex) {
            fail(ex.toString());
        } finally {
            connectionFactory().closeRegisteredObjects();
        }

        ResultSet rs = newScrollableInsensitiveUpdateableResultSet(
                getSelectFromAllTypes());

        rs.moveToInsertRow();

        rs.close();

        try {
            rs.moveToCurrentRow();

            fail("Allowed moveToCurrentRow() after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        } finally {
            connectionFactory().closeRegisteredObjects();
        }
    }

    /**
     * Test of getStatement method, of interface java.sql.ResultSet.
     */
    public void testGetStatement() throws Exception {
        ResultSet rs = newForwardOnlyReadOnlyResultSet(
                getSelectFromAllTypes());

        Statement result = rs.getStatement();
        assertNotNull(result);

        rs.close();

        try {
            rs.getStatement();

            fail("Allowed getStatement() after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of getRef method, of interface java.sql.ResultSet.
     */
    public void testGetRef() throws Exception {
        if (!isTestREF()) {
            return;
        }

        handleTestGetXXX("getRef");

        ResultSet rs = newForwardOnlyReadOnlyResultSet(
                getSelectFromAllTypes());

        rs.next();
        rs.close();

        try {
            rs.getRef(1);

            fail("Allowed getRef after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of getBlob method, of interface java.sql.ResultSet.
     */
    public void testGetBlob() throws Exception {
        handleTestGetXXX("getBlob");

        ResultSet rs = newForwardOnlyReadOnlyResultSet(
                getSelectFromAllTypes());

        rs.next();
        rs.close();

        try {
            rs.getBlob("c_blob");

            fail("Allowed getBlob after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of getClob method, of interface java.sql.ResultSet.
     */
    public void testGetClob() throws Exception {
        handleTestGetXXX("getClob");

        ResultSet rs = newForwardOnlyReadOnlyResultSet(
                getSelectFromAllTypes());

        rs.next();
        rs.close();

        try {
            rs.getClob("c_clob");

            fail("Allowed getClob after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of getArray method, of interface java.sql.ResultSet.
     */
    public void testGetArray() throws Exception {
        if (!isTestARRAY()) {
            return;
        }

        handleTestGetXXX("getArray");

        ResultSet rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

        assertEquals("beforeFirst", true, rs.isBeforeFirst());

        try {
            rs.getArray("c_array");

            fail("Allowed getArray(String) while before first");
        } catch (SQLException ex) {
            checkResultSetBeforeFirstOrNotSupportedException(ex);
        }

        while (rs.next()) {
        }

        try {
            rs.getArray("c_array");

            fail("Allowed getArray(String) while after last");
        } catch (SQLException ex) {
            checkResultSetAfterLastOrNotSupportedException(ex);
        }

        rs.close();

        try {
            rs.getArray("c_array");

            fail("Allowed getArray after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of getURL method, of interface java.sql.ResultSet.
     */
    public void testGetURL() throws Exception {
        handleTestGetXXX("getURL");

        ResultSet rs = this.newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

        rs.next();
        rs.close();

        try {
            rs.getURL(1);

            fail("Allowed getURL after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
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

        stubTestResult("TODO: REF update test is not yet implemented.");
    }

    /**
     * Test of updateBlob method, of interface java.sql.ResultSet.
     */
    public void testUpdateBlob() throws Exception {
        if (!isTestUpdates()) {
            return;
        }

        try {
            ResultSet rs = newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

            rs.next();

            Blob blob = newConnection().createBlob();

            blob.setBytes(1, new byte[]{(byte) 0});

            rs.updateBlob("c_blob", blob.getBinaryStream());
            rs.updateRow();

            rs.updateBlob("c_blob", blob.getBinaryStream(), 1);
            rs.updateRow();

            rs.updateBlob("c_blob", blob);
            rs.updateRow();

        } catch (Exception e) {
            fail(e.toString());
        } finally {
            connectionFactory().closeRegisteredObjects();
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
            ResultSet rs = this.newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

            rs.next();

            Clob clob = newConnection().createClob();

            clob.setString(1, "Clob");

            rs.updateClob("clob_column", clob);
            rs.updateRow();

            StringReader reader = new StringReader("Clob");

            rs.updateClob("clob_column", reader);
            rs.updateRow();

            reader = new StringReader("Clob");

            rs.updateClob("clob_column", reader, "Clob".length());
            rs.updateRow();
        } catch (Exception e) {
            fail(e.toString());
        } finally {
            connectionFactory().closeRegisteredObjects();
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
            ResultSet rs = newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

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
        } finally {
            connectionFactory().closeRegisteredObjects();
        }
    }

    /**
     * Test of getRowId method, of interface java.sql.ResultSet.
     */
    public void testGetRowId() throws Exception {
        if (!isTestROWID()) {
            return;
        }

        handleTestGetXXX("getRowId");

        ResultSet rs = this.newForwardOnlyReadOnlyResultSet(
                getSelectFromAllTypes());

        rs.next();
        rs.close();

        try {
            rs.getRowId(1);

            fail("Allowed getRowId after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
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
            ResultSet rs = newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

            rs.next();

            JDBCRowId rowId = new JDBCRowId("00000002");

            rs.updateRowId("id_column", rowId);
        } catch (Exception e) {
            // TODO : generic exception handling based on
            //        policy configured for driver being tested.
            if (!(e instanceof SQLFeatureNotSupportedException)) {
                fail(e.toString());
            }
        }
    }

    /**
     * Test of getHoldability method, of interface java.sql.ResultSet.
     */
    public void testGetHoldability() throws Exception {
        ResultSet rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

        int result = rs.getHoldability();
        assertEquals("Cursor Holdability", ResultSet.HOLD_CURSORS_OVER_COMMIT, result);

        rs.next();
        rs.close();

        try {
            rs.getHoldability();

            fail("Allowed getHoldability() after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of isClosed method, of interface java.sql.ResultSet.
     */
    public void testIsClosed() throws Exception {
        ResultSet rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

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
            ResultSet rs = this.newScrollableInsensitiveUpdateableResultSet(
                    getSelectFromAllTypes());

            rs.next();

            rs.updateNString("char_column", "NString");
            rs.updateRow();

            rs.updateNString("varchar_column", "updateNString");
            rs.updateRow();

            rs.updateNString("longvarchar_column", "updateNString");
            rs.updateRow();

            rs.updateNString("clob_column", "updateNString");
            rs.updateRow();
        } catch (Exception e) {
            fail(e.toString());
        } finally {
            connectionFactory().closeRegisteredObjects();
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
            ResultSet rs = this.newScrollableInsensitiveUpdateableResultSet(getSelectFromAllTypes());

            rs.next();

            NClob nClob = newConnection().createNClob();

            nClob.setString(1, "NClob");

            rs.updateNClob("clob_column", nClob);
            rs.updateRow();

            StringReader reader = new StringReader("NClob");

            rs.updateNClob("clob_column", reader);
            rs.updateRow();

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
        handleTestGetXXX("getNClob");

        ResultSet rs = this.newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

        rs.next();
        rs.close();

        try {
            rs.getNClob(1);

            fail("Allowed getNClob after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of getSQLXML method, of interface java.sql.ResultSet.
     */
    public void testGetSQLXML() throws Exception {
        handleTestGetXXX("getSQLXML");

        ResultSet rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

        rs.next();
        rs.close();

        try {
            rs.getSQLXML(1);

            fail("Allowed getSQLXML after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
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
        stubTestResult("SQLXML update test is not yet implemented.");
    }

    /**
     * Test of getNString method, of interface java.sql.ResultSet.
     */
    public void testGetNString() throws Exception {
        handleTestGetXXX("getNString");

        ResultSet rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

        rs.next();
        rs.close();

        try {
            rs.getNString(1);

            fail("Allowed getNString after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
        }
    }

    /**
     * Test of getNCharacterStream method, of interface java.sql.ResultSet.
     */
    public void testGetNCharacterStream() throws Exception {
        handleTestGetXXX("getNCharacterStream");

        ResultSet rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

        rs.next();
        rs.close();

        try {
            rs.getNCharacterStream(1);

            fail("Allowed getNCharacterStream after close()");
        } catch (SQLException ex) {
            checkResultSetClosedOrNotSupportedException(ex);
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
            ResultSet rs = this.newScrollableInsensitiveUpdateableResultSet(getSelectFromAllTypes());

            rs.next();

            String value = "NChars";

            rs.updateNCharacterStream("clob_column", new StringReader(value));
            rs.updateNCharacterStream("clob_column", new StringReader(value), value.length());

        } catch (Exception e) {
            fail(e.toString());
        } finally {
            connectionFactory().closeRegisteredObjects();
        }
    }

    /**
     * Test of unwrap method, of interface java.sql.ResultSet.
     */
    public void testUnwrap() throws Exception {
        Class<?> iface = JDBCResultSet.class;
        ResultSet rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

        Object expResult = rs;
        Object result = rs.unwrap(iface);
        assertEquals(expResult, result);
    }

    /**
     * Test of isWrapperFor method, of interface java.sql.ResultSet.
     */
    public void testIsWrapperFor() throws Exception {
        Class<?> iface = JDBCResultSet.class;
        ResultSet rs = newForwardOnlyReadOnlyResultSet(getSelectFromAllTypes());

        boolean expResult = true;
        boolean result = rs.isWrapperFor(iface);
        assertEquals(expResult, result);
    }

    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }
}
