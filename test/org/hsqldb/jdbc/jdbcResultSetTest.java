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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.Trace;

/**
 *
 * @author boucherb@users
 */
public class jdbcResultSetTest extends JdbcTestCase {

    public jdbcResultSetTest(String testName) {
        super(testName);
    }

    protected void setUp() throws Exception {
        super.setUp();

        executeScript("setup-all_types-table.sql");
        executeScript("populate-all_types-table.sql");
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(jdbcResultSetTest.class);

        return suite;
    }
    
    protected boolean isTestUpdates()
    {        
        return getBooleanProperty("test.result.set.updates", true);
    }

    private final String select =
    "select id as           id_column, " +                            // 1
           "c_bigint        as bigint_column, " +
           "c_binary        as binary_column, " +
           "c_boolean       as boolean_column, " +
           "c_char          as char_column, " +                       // 5
           "c_date          as date_column, " +
           "c_decimal       as decimal_column, " +
           "c_double        as double_column, " +
           "c_float         as float_column, " +
           "c_integer       as integer_column, " +                    // 10
           "c_longvarbinary as longvarbinary_column, " +
           "c_longvarchar   as longvarchar_column, " +
           "c_object        as object_column, " +
           "c_real          as real_column, " +
           "c_smallint      as smallint_column, " +                   // 15
           "c_time          as time_column, " +
           "c_timestamp     as timestamp_column, " +
           "c_tinyint       as tinyint_column, " +
           "c_varbinary     as varbinary_column, " +
           "c_varchar       as varchar_column, " +                    // 20
           "c_varchar_ignorecase as varchar_ignorecase_column " +
      "from all_types";

    private final String[] names = new String[] {
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
        "c_varchar_ignorecase"
    };

    private final String[] aliases = new String[] {
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
        "varchar_ignorecase_column"
    };

    protected String getSelect() {
        return this.select;
    }

    protected String[] getColumnNames() {
        return this.names;
    }

    protected String[] getColumnAliases() {
        return this.aliases;
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

        jdbcConnection conn = (jdbcConnection) newConnection();
        jdbcStatement  stmt = (jdbcStatement) conn.createStatement(type, concur);

        return (ResultSet) stmt.executeQuery(select);
    }

    protected void testGetXXX(String methodName) throws Exception {
        ResultSet rs = newFOROJdbcResultSet();

        rs.next();

        ResultSetMetaData rsmd        = rs.getMetaData();
        int               columnCount = rsmd.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            String  typeName    = rsmd.getColumnTypeName(i);
            String  columnName  = rsmd.getColumnName(i);
            String  columnClass = rsmd.getColumnClassName(i);
            int     dataType    = rsmd.getColumnType(i);
            boolean required    = isRequiredGetXXX(methodName, dataType);

            try {
                Method getXXX = rs.getClass().getMethod(methodName, new Class[]{int.class});

                Object value      = getXXX.invoke(rs, new Object[]{ new Integer(i) });
                Class  valueClass = (value == null) ? Void.class : value.getClass();

                if (!required) {
                    Method getObject   = rs.getClass().getMethod("getObject", new Class[]{int.class});
                    Object objectValue = getObject.invoke(rs, new Object[]{ new Integer(i) });

                    System.out.println(
                            "Warning - "
                            + columnName
                            + "{type=" + typeName
                            + ", class=" + columnClass
                            + ", value=\"" + objectValue
                            + "\"} => "+ valueClass
                            + "{value=\"" + value
                            + "\"}"
                            + " - JDBC 4.0, Table B-6 indicates this getter conversion is not required."
                            );
                }
            } catch (Exception e) {
                Throwable t = e;

                while (t instanceof InvocationTargetException) {
                    t = ((InvocationTargetException)t).getTargetException();
                }

                if (t instanceof SQLException) {
                    SQLException ex = (SQLException) t;

                    if (required) {
                        if (ex.getErrorCode() != -Trace.WRONG_DATA_TYPE) {
                            fail(columnName + ": " + t + ": [" +  ex.getErrorCode() + "]");
                        } else {
                            System.out.println("Info - " + columnName + ": " + ex);
                        }
                    } else {
                        System.out.println("Info - " + columnName + ": " + ex);
                    }
                } else {
                    fail(columnName + ": " + t);
                }
            }
        }
    }

    /**
     * Test of next method, of interface java.sql.ResultSet.
     */
    public void testNext() throws Exception {
        System.out.println("next");

        ResultSet rs = newFOROJdbcResultSet();

        assertEquals(true, rs.next());

        while(rs.next());

        assertEquals(false, rs.next());
    }

    /**
     * Test of close method, of interface java.sql.ResultSet.
     */
    public void testClose() throws Exception {
        System.out.println("close");

        ResultSet rs = newFOROJdbcResultSet();

        rs.close();
        rs.close();
    }

    /**
     * Test of wasNull method, of interface java.sql.ResultSet.
     */
    public void testWasNull() throws Exception {
        System.out.println("wasNull");

        ResultSet rs = newFOROJdbcResultSet();

        rs.next();
        rs.next();

        String[] aliases = getColumnAliases();

        for (int i = 0; i < aliases.length; i++) {
            String alias = aliases[i];
            Object o = rs.getObject(alias);
            assertEquals(aliases[i] + "(" + o + ")", false, rs.wasNull());
        }

        rs.next();
        rs.next();

        for (int i = 0; i < aliases.length; i++) {
            rs.getObject(aliases[i]);
            assertEquals(aliases[i], true, rs.wasNull());
        }
    }

    /**
     * Test of getString method, of interface java.sql.ResultSet.
     */
    public void testGetString() throws Exception {
        System.out.println("getString");

        testGetXXX("getString");
    }

    /**
     * Test of getBoolean method, of interface java.sql.ResultSet.
     */
    public void testGetBoolean() throws Exception {
        System.out.println("getBoolean");

        testGetXXX("getBoolean");
    }

    /**
     * Test of getByte method, of interface java.sql.ResultSet.
     */
    public void testGetByte() throws Exception {
        System.out.println("getByte");

        testGetXXX("getByte");
    }

    /**
     * Test of getShort method, of interface java.sql.ResultSet.
     */
    public void testGetShort() throws Exception {
        System.out.println("getShort");

        testGetXXX("getShort");
    }

    /**
     * Test of getInt method, of interface java.sql.ResultSet.
     */
    public void testGetInt() throws Exception {
        System.out.println("getInt");

        testGetXXX("getInt");
    }

    /**
     * Test of getLong method, of interface java.sql.ResultSet.
     */
    public void testGetLong() throws Exception {
        System.out.println("getLong");

        testGetXXX("getLong");
    }

    /**
     * Test of getFloat method, of interface java.sql.ResultSet.
     */
    public void testGetFloat() throws Exception {
        System.out.println("getFloat");

        testGetXXX("getFloat");
    }

    /**
     * Test of getDouble method, of interface java.sql.ResultSet.
     */
    public void testGetDouble() throws Exception {
        System.out.println("getDouble");

        testGetXXX("getDouble");
    }

    /**
     * Test of getBigDecimal method, of interface java.sql.ResultSet.
     */
    public void testGetBigDecimal() throws Exception {
        System.out.println("getBigDecimal");

        testGetXXX("getBigDecimal");
    }

    /**
     * Test of getBytes method, of interface java.sql.ResultSet.
     */
    public void testGetBytes() throws Exception {
        System.out.println("getBytes");

        testGetXXX("getBytes");
    }

    /**
     * Test of getDate method, of interface java.sql.ResultSet.
     */
    public void testGetDate() throws Exception {
        System.out.println("getDate");

        testGetXXX("getDate");
    }

    /**
     * Test of getTime method, of interface java.sql.ResultSet.
     */
    public void testGetTime() throws Exception {
        System.out.println("getTime");

        testGetXXX("getTime");
    }

    /**
     * Test of getTimestamp method, of interface java.sql.ResultSet.
     */
    public void testGetTimestamp() throws Exception {
        System.out.println("getTimestamp");

        testGetXXX("getTimestamp");
    }

    /**
     * Test of getAsciiStream method, of interface java.sql.ResultSet.
     */
    public void testGetAsciiStream() throws Exception {
        System.out.println("getAsciiStream");

        testGetXXX("getAsciiStream");
    }

    /**
     * Test of getUnicodeStream method, of interface java.sql.ResultSet.
     */
    public void testGetUnicodeStream() throws Exception {
        System.out.println("getUnicodeStream");

        testGetXXX("getUnicodeStream");
    }

    /**
     * Test of getBinaryStream method, of interface java.sql.ResultSet.
     */
    public void testGetBinaryStream() throws Exception {
        System.out.println("getBinaryStream");

        testGetXXX("getBinaryStream");
    }

    /**
     * Test of getWarnings method, of interface java.sql.ResultSet.
     */
    public void testGetWarnings() throws Exception {
        System.out.println("getWarnings");

        ResultSet rs = newFOROJdbcResultSet();

        // TODO - cases to test spec'd generation of warnings
        rs.getWarnings();
    }

    /**
     * Test of clearWarnings method, of interface java.sql.ResultSet.
     */
    public void testClearWarnings() throws Exception {
        System.out.println("clearWarnings");

        ResultSet rs = newFOROJdbcResultSet();

        rs.clearWarnings();

        assertEquals(null, rs.getWarnings());
    }

    /**
     * Test of getCursorName method, of interface java.sql.ResultSet.
     */
    public void testGetCursorName() throws Exception {
        System.out.println("getCursorName");

        ResultSet rs = newFOROJdbcResultSet();

        String expResult = null;

        try {
            String result = rs.getCursorName();
            assertEquals(expResult, result);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of getMetaData method, of interface java.sql.ResultSet.
     */
    public void testGetMetaData() throws Exception {
        System.out.println("getMetaData");

        ResultSet     rs   = newFOROJdbcResultSet();
        ResultSetMetaData rsmd = rs.getMetaData();

        assertNotNull(rsmd);
    }

    /**
     * Test of getObject method, of interface java.sql.ResultSet.
     */
    public void testGetObject() throws Exception {
        System.out.println("getObject");

        testGetXXX("getObject");
    }

    /**
     * Test of findColumn method, of interface java.sql.ResultSet.
     */
    public void testFindColumn() throws Exception {
        System.out.println("findColumn");

        ResultSet rs      = newFOROJdbcResultSet();
        String[]  aliases = getColumnAliases();

        for (int i = 0; i < aliases.length; i++) {
            rs.findColumn(aliases[i]);
        }
    }

    /**
     * Test of getCharacterStream method, of interface java.sql.ResultSet.
     */
    public void testGetCharacterStream() throws Exception {
        System.out.println("getCharacterStream");

        testGetXXX("getCharacterStream");
    }

    /**
     * Test of isBeforeFirst method, of interface java.sql.ResultSet.
     */
    public void testIsBeforeFirst() throws Exception {
        System.out.println("isBeforeFirst");

        ResultSet rs = this.newScrollableROJdbcResultSet();

        try {
            rs.getObject(1);
            fail("get succeeded while before first");
        } catch (Exception e) {}

        assertEquals(true, rs.isBeforeFirst());

        rs.next();

        assertEquals(false, rs.isBeforeFirst());

        rs.previous();

        try {
            rs.getObject(1);
            fail("get succeeded while before first");
        } catch (Exception e) {}

        assertEquals(true, rs.isBeforeFirst());

        rs.next();

        assertEquals(false, rs.isBeforeFirst());

        rs.beforeFirst();

        try {
            rs.getObject(1);
            fail("get succeeded while before first");
        } catch (Exception e) {}

        assertEquals(true, rs.isBeforeFirst());

    }

    /**
     * Test of isAfterLast method, of interface java.sql.ResultSet.
     */
    public void testIsAfterLast() throws Exception {
        System.out.println("isAfterLast");

        ResultSet rs = this.newScrollableROJdbcResultSet();

        assertEquals(false, rs.isAfterLast());

        rs.next();

        assertEquals(false, rs.isAfterLast());

        while(rs.next());

        try {
            rs.getObject(1);
            fail("get succeeded while after last");
        } catch (Exception e) {}

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
    }

    /**
     * Test of isFirst method, of interface java.sql.ResultSet.
     */
    public void testIsFirst() throws Exception {
        System.out.println("isFirst");

        ResultSet rs = this.newScrollableROJdbcResultSet();

        assertEquals(false, rs.isFirst());

        rs.next();

        assertEquals(true, rs.isFirst());

        rs.next();

        assertEquals(false, rs.isFirst());

        rs.previous();

        assertEquals(true, rs.isFirst());

        while(rs.next());

        assertEquals(false, rs.isFirst());

        while(rs.previous());

        assertEquals(false, rs.isFirst());

        rs.next();

        assertEquals(true, rs.isFirst());

        while(rs.next());

        assertEquals(false, rs.isFirst());

        rs.first();

        assertEquals(true, rs.isFirst());

    }

    /**
     * Test of isLast method, of interface java.sql.ResultSet.
     */
    public void testIsLast() throws Exception {
        System.out.println("isLast");

        ResultSet rs = this.newScrollableROJdbcResultSet();

        assertEquals(false, rs.isLast());

        rs.next();

        assertEquals(false, rs.isLast());

        while(rs.next());

        assertEquals(false, rs.isLast());

        rs.previous();

        assertEquals(true, rs.isLast());

        while(rs.previous());

        assertEquals(false, rs.isLast());

        rs.last();

        assertEquals(true, rs.isLast());
    }

    /**
     * Test of beforeFirst method, of interface java.sql.ResultSet.
     */
    public void testBeforeFirst() throws Exception {
        System.out.println("beforeFirst");

        ResultSet rs = this.newScrollableROJdbcResultSet();

        try {
            rs.getObject(1);
            fail("get succeeded while before first");
        } catch (Exception e) {}

        assertEquals(true, rs.isBeforeFirst());

        while (rs.next());

        rs.beforeFirst();

        try {
            rs.getObject(1);
            fail("get succeeded while before first");
        } catch (Exception e) {}

        assertEquals(true, rs.isBeforeFirst());
    }

    /**
     * Test of afterLast method, of interface java.sql.ResultSet.
     */
    public void testAfterLast() throws Exception {
        System.out.println("afterLast");

        ResultSet rs = this.newScrollableROJdbcResultSet();

        rs.afterLast();

        assertEquals(true, rs.isAfterLast());

        try {
            rs.getObject(1);
            fail("get succeeded while after last");
        } catch (Exception e) {}

        rs.beforeFirst();
        rs.afterLast();

        assertEquals(true, rs.isAfterLast());

        try {
            rs.getObject(1);
            fail("get succeeded while after last");
        } catch (Exception e) {}

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
        } catch (Exception e) {}

        assertEquals(true, rs.isAfterLast());

    }

    /**
     * Test of first method, of interface java.sql.ResultSet.
     */
    public void testFirst() throws Exception {
        System.out.println("first");

        ResultSet rs = this.newScrollableROJdbcResultSet();

        rs.first();

        assertEquals(true, rs.isFirst());

    }

    /**
     * Test of last method, of interface java.sql.ResultSet.
     */
    public void testLast() throws Exception {
        System.out.println("last");

        ResultSet rs = this.newScrollableROJdbcResultSet();

        rs.last();

        assertEquals(true, rs.isLast());
    }

    /**
     * Test of getRow method, of interface java.sql.ResultSet.
     */
    public void testGetRow() throws Exception {
        System.out.println("getRow");

        ResultSet rs = this.newScrollableROJdbcResultSet();
        int row = 0;

        while (rs.next()) {
            row++;
            assertEquals(row, rs.getRow());
        }

        int last = row;

        while(rs.previous()) {
            assertEquals(row, rs.getRow());
            row--;
        }

        rs.absolute(2);

        assertEquals(2, rs.getRow());

        rs.first();

        assertEquals(1, rs.getRow());

        rs.last();

        assertEquals(last, rs.getRow());
    }

    /**
     * Test of absolute method, of interface java.sql.ResultSet.
     */
    public void testAbsolute() throws Exception {
        System.out.println("absolute");

        ResultSet rs = this.newScrollableROJdbcResultSet();
        int rows = 0;

        while(rs.next()) {
            rows++;
        }

        for (int i = rows; i >= 1; i--) {
            rs.absolute(i);

            assertEquals(i, rs.getRow());
        }
    }

    /**
     * Test of relative method, of interface java.sql.ResultSet.
     */
    public void testRelative() throws Exception {
        System.out.println("relative");

        ResultSet rs = this.newScrollableROJdbcResultSet();

        while(!rs.isAfterLast()) {
            rs.relative(1);
        }

        while(!rs.isBeforeFirst()) {
            rs.relative(-1);
        }

        while(!rs.isAfterLast()) {
            rs.relative(2);
        }

        while(!rs.isBeforeFirst()) {
            rs.relative(-2);
        }
    }

    /**
     * Test of previous method, of interface java.sql.ResultSet.
     */
    public void testPrevious() throws Exception {
        System.out.println("previous");

        ResultSet rs = this.newScrollableROJdbcResultSet();

        rs.afterLast();

        while(rs.previous()) {
            assertEquals(false, rs.isBeforeFirst());
        }

        assertEquals(true, rs.isBeforeFirst());
    }

    /**
     * Test of setFetchDirection method, of interface java.sql.ResultSet.
     */
    public void testSetFetchDirection() throws Exception {
        System.out.println("setFetchDirection");

        ResultSet rs = this.newScrollableROJdbcResultSet();

        rs.setFetchDirection(ResultSet.FETCH_FORWARD);
        rs.setFetchDirection(ResultSet.FETCH_REVERSE);
        rs.setFetchDirection(ResultSet.FETCH_UNKNOWN);
    }

    /**
     * Test of getFetchDirection method, of interface java.sql.ResultSet.
     */
    public void testGetFetchDirection() throws Exception {
        System.out.println("getFetchDirection");

        ResultSet rs = this.newScrollableROJdbcResultSet();

        assertEquals(jdbcResultSet.FETCH_FORWARD, rs.getFetchDirection());
    }

    /**
     * Test of setFetchSize method, of interface java.sql.ResultSet.
     */
    public void testSetFetchSize() throws Exception {
        System.out.println("setFetchSize");

        ResultSet rs = newFOROJdbcResultSet();

        rs.setFetchSize(1000);
    }

    /**
     * Test of getFetchSize method, of interface java.sql.ResultSet.
     */
    public void testGetFetchSize() throws Exception {
        System.out.println("getFetchSize");

        ResultSet rs = newFOROJdbcResultSet();

        int expResult = 1;
        int result = rs.getFetchSize();
        assertEquals(expResult, result);
    }

    /**
     * Test of getType method, of interface java.sql.ResultSet.
     */
    public void testGetType_FORWARD_ONLY() throws Exception {
        System.out.println("getType_FORWARD_ONLY");

        assertEquals(ResultSet.TYPE_FORWARD_ONLY,
                     newFOROJdbcResultSet().getType());
    }
    
    /**
     * Test of getType method, of interface java.sql.ResultSet.
     */
    public void testGetType_SCROLL_INSENSITIVE() throws Exception {
        System.out.println("getType_SCROLL_INSENSITIVE");

        assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE,
                     newJdbcResultSet(ResultSet.TYPE_SCROLL_INSENSITIVE).getType());
    }   
    
    /**
     * Test of getType method, of interface java.sql.ResultSet.
     */
    public void testGetType_SCROLL_SENSITIVE() throws Exception {
        System.out.println("getType_SCROLL_SENSITIVE");

        assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE,
                     newJdbcResultSet(ResultSet.TYPE_SCROLL_SENSITIVE).getType());
    }       

    /**
     * Test of getConcurrency method, of interface java.sql.ResultSet.
     */
    public void testGetConcurrency() throws Exception {
        System.out.println("getConcurrency");

        assertEquals(ResultSet.CONCUR_READ_ONLY,
                     newFOROJdbcResultSet().getConcurrency());
        
        assertEquals(ResultSet.CONCUR_READ_ONLY,
                     newJdbcResultSet(ResultSet.TYPE_FORWARD_ONLY,
                                     ResultSet.CONCUR_UPDATABLE).getConcurrency());
    }

    /**
     * Test of rowUpdated method, of interface java.sql.ResultSet.
     */
    public void testRowUpdated() throws Exception {
        System.out.println("rowUpdated");
        
        if (!isTestUpdates())
        {
            return;
        }        

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            assertEquals(false, rs.rowUpdated());

            rs.updateObject(1, new Integer(1), java.sql.Types.INTEGER);

            assertEquals(true, rs.rowUpdated());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of rowInserted method, of interface java.sql.ResultSet.
     */
    public void testRowInserted() throws Exception {
        System.out.println("rowInserted");
        
        if (!isTestUpdates())
        {
            return;
        }        

        // TODO:
        fail("TODO: The test case is empty.");
    }

    /**
     * Test of rowDeleted method, of interface java.sql.ResultSet.
     */
    public void testRowDeleted() throws Exception {
        System.out.println("rowDeleted");
        
        if (!isTestUpdates())
        {
            return;
        }        

        fail("TODO: The test case is empty.");
    }

    /**
     * Test of updateNull method, of interface java.sql.ResultSet.
     */
    public void testUpdateNull() throws Exception {
        System.out.println("updateNull");
        
        if (!isTestUpdates())
        {
            return;
        }          

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateNull(1);

            assertEquals(true, rs.rowUpdated());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of updateBoolean method, of interface java.sql.ResultSet.
     */
    public void testUpdateBoolean() throws Exception {
        System.out.println("testUpdateBoolean");
        
        if (!isTestUpdates())
        {
            return;
        }
        
        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateBoolean("boolean_column", true);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of updateByte method, of interface java.sql.ResultSet.
     */
    public void testUpdateByte() throws Exception {
        System.out.println("testUpdateByte");
        
        if (!isTestUpdates())
        {
            return;
        }
        
        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateByte("tinyint_column", (byte)1);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of updateShort method, of interface java.sql.ResultSet.
     */
    public void testUpdateShort() throws Exception {
        System.out.println("updateShort");
        
        if (!isTestUpdates())
        {
            return;
        }          

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateShort("smallint_column", (short)1);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of updateInt method, of interface java.sql.ResultSet.
     */
    public void testUpdateInt() throws Exception {
        System.out.println("updateInt");
        
        if (!isTestUpdates())
        {
            return;
        }          

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateInt("integer_column", 1);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of updateLong method, of interface java.sql.ResultSet.
     */
    public void testUpdateLong() throws Exception {
        System.out.println("updateLong");
        
        if (!isTestUpdates())
        {
            return;
        }          

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateLong("bigint_column", 1L);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of updateFloat method, of interface java.sql.ResultSet.
     */
    public void testUpdateFloat() throws Exception {
        System.out.println("updateFloat");
        
        if (!isTestUpdates())
        {
            return;
        }          

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateFloat("real_column", 1F);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of updateDouble method, of interface java.sql.ResultSet.
     */
    public void testUpdateDouble() throws Exception {
        System.out.println("updateDouble");
        
        if (!isTestUpdates())
        {
            return;
        }          

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateDouble("float_column", 1D);
            rs.updateDouble("double_column", 1D);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of updateBigDecimal method, of interface java.sql.ResultSet.
     */
    public void testUpdateBigDecimal() throws Exception {
        System.out.println("updateBigDecimal");
        
        if (!isTestUpdates())
        {
            return;
        }        

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateBigDecimal("decimal_column", new BigDecimal("1.0"));
            rs.updateBigDecimal("numeric_column", new BigDecimal("1.0"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of updateString method, of interface java.sql.ResultSet.
     */
    public void testUpdateString() throws Exception {
        System.out.println("updateString");
        
        if (!isTestUpdates())
        {
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
            rs.updateString("numeric_column", "9223372036854775807000.1234567890");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of updateBytes method, of interface java.sql.ResultSet.
     */
    public void testUpdateBytes() throws Exception {
        System.out.println("updateBytes");
        
        if (!isTestUpdates())
        {
            return;
        }        

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateBytes("binary_column", "updateBytes".getBytes());
            rs.updateBytes("varbinary_column", "updateBytes".getBytes());
            rs.updateBytes("longbinary_column", "updateBytes".getBytes());
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of updateDate method, of interface java.sql.ResultSet.
     */
    public void testUpdateDate() throws Exception {
        System.out.println("updateDate");
        
        if (!isTestUpdates())
        {
            return;
        }          

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateDate("date_column", java.sql.Date.valueOf("2005-12-14"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of updateTime method, of interface java.sql.ResultSet.
     */
    public void testUpdateTime() throws Exception {
        System.out.println("updateTime");
        
        if (!isTestUpdates())
        {
            return;
        }          

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateTime("time_column", java.sql.Time.valueOf("11:57:02"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of updateTimestamp method, of interface java.sql.ResultSet.
     */
    public void testUpdateTimestamp() throws Exception {
        System.out.println("updateTimestamp");
        
        if (!isTestUpdates())
        {
            return;
        }          

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateTimestamp("timestamp_column", java.sql.Timestamp.valueOf("2005-12-14 11:57:02.1234"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of updateAsciiStream method, of interface java.sql.ResultSet.
     */
    public void testUpdateAsciiStream() throws Exception {
        System.out.println("updateAsciiStream");
        
        if (!isTestUpdates())
        {
            return;
        }        

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateAsciiStream("char_column", new java.io.ByteArrayInputStream("updateAsciiStream".getBytes()), 10);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of updateBinaryStream method, of interface java.sql.ResultSet.
     */
    public void testUpdateBinaryStream() throws Exception {
        System.out.println("updateBinaryStream");
        
        if (!isTestUpdates())
        {
            return;
        }        

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateBinaryStream("binary_column", new java.io.ByteArrayInputStream("updateBinaryStream".getBytes()), 10);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of updateCharacterStream method, of interface java.sql.ResultSet.
     */
    public void testUpdateCharacterStream() throws Exception {
        System.out.println("updateCharacterStream");
        
        if (!isTestUpdates())
        {
            return;
        }          

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateCharacterStream("char_column", new java.io.StringReader("updateCharacterStream"), 10);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of updateObject method, of interface java.sql.ResultSet.
     */
    public void testUpdateObject() throws Exception {
        System.out.println("updateObject");
        
        if (!isTestUpdates())
        {
            return;
        }          

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.updateObject("object_column", new java.io.Serializable() {
            });
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of insertRow method, of interface java.sql.ResultSet.
     */
    public void testInsertRow() throws Exception {
        System.out.println("insertRow");

        try {

            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.moveToInsertRow();
            int columnCount = rs.getMetaData().getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                rs.updateNull(i);
            }

            rs.insertRow();
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    /**
     * Test of updateRow method, of interface java.sql.ResultSet.
     */
    public void testUpdateRow() throws Exception {
        System.out.println("updateRow");
        
        if (!isTestUpdates())
        {
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
            fail(ex.getMessage());
        }
    }

    /**
     * Test of deleteRow method, of interface java.sql.ResultSet.
     */
    public void testDeleteRow() throws Exception {
        if (!isTestUpdates())
        {
            return;
        }        
        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            rs.deleteRow();
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    /**
     * Test of refreshRow method, of interface java.sql.ResultSet.
     */
    public void testRefreshRow() throws Exception {
        System.out.println("refreshRow");
        
        if (!isTestUpdates())
        {
            return;
        }        

        try {
            ResultSet rs = this.newJdbcResultSet(ResultSet.TYPE_SCROLL_SENSITIVE);

            rs.next();

            rs.refreshRow();
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    /**
     * Test of cancelRowUpdates method, of interface java.sql.ResultSet.
     */
    public void testCancelRowUpdates() throws Exception {
        System.out.println("cancelRowUpdates");
        
        if (!isTestUpdates())
        {
            return;
        }

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.next();

            int columnCount = rs.getMetaData().getColumnCount();

            for (int i = 1; i <= columnCount; i++) {
                rs.updateNull(i);
            }

            rs.cancelRowUpdates();
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    /**
     * Test of moveToInsertRow method, of interface java.sql.ResultSet.
     */
    public void testMoveToInsertRow() throws Exception {
        System.out.println("moveToInsertRow");
        
        if (!isTestUpdates())
        {
            return;
        }        

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.moveToInsertRow();
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    /**
     * Test of moveToCurrentRow method, of interface java.sql.ResultSet.
     */
    public void testMoveToCurrentRow() throws Exception {
        System.out.println("moveToCurrentRow");
        
        if (!isTestUpdates())
        {
            return;
        }        

        try {
            ResultSet rs = this.newUpdateableJdbcResultSet();

            rs.moveToInsertRow();
            rs.moveToCurrentRow();
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    /**
     * Test of getStatement method, of interface java.sql.ResultSet.
     */
    public void testGetStatement() throws Exception {
        System.out.println("getStatement");

        ResultSet rs = newFOROJdbcResultSet();

        Statement result = rs.getStatement();
        assertNotNull(result);
    }

    /**
     * Test of getRef method, of interface java.sql.ResultSet.
     */
    public void testGetRef() throws Exception {
        System.out.println("getRef");

        testGetXXX("getRef");
    }

    /**
     * Test of getBlob method, of interface java.sql.ResultSet.
     */
    public void testGetBlob() throws Exception {
        System.out.println("getBlob");

        testGetXXX("getBlob");
    }

    /**
     * Test of getClob method, of interface java.sql.ResultSet.
     */
    public void testGetClob() throws Exception {
        System.out.println("getClob");

        testGetXXX("getClob");
    }

    /**
     * Test of getArray method, of interface java.sql.ResultSet.
     */
    public void testGetArray() throws Exception {
        System.out.println("getArray");

        testGetXXX("getArray");
    }

    /**
     * Test of getURL method, of interface java.sql.ResultSet.
     */
    public void testGetURL() throws Exception {
        System.out.println("getURL");

        testGetXXX("getURL");
    }

    /**
     * Test of updateRef method, of interface java.sql.ResultSet.
     */
    public void testUpdateRef() throws Exception {
        System.out.println("updateRef");
        
        if (!isTestUpdates())
        {
            return;
        }          

        // TODO:
        fail("TODO: The test case is empty.");
    }

    /**
     * Test of updateBlob method, of interface java.sql.ResultSet.
     */
    public void testUpdateBlob() throws Exception {
        System.out.println("updateBlob");
        
        if (!isTestUpdates())
        {
            return;
        }        

        // TODO:
        fail("TODO: The test case is empty.");
    }

    /**
     * Test of updateClob method, of interface java.sql.ResultSet.
     */
    public void testUpdateClob() throws Exception {
        System.out.println("updateClob");
        
        if (!isTestUpdates())
        {
            return;
        }          

        // TODO:
        fail("TODO: The test case is empty.");
    }

    /**
     * Test of updateArray method, of interface java.sql.ResultSet.
     */
    public void testUpdateArray() throws Exception {
        System.out.println("updateArray");

        if (!isTestUpdates())
        {
            return;
        }
        
        // TODO:
        fail("TODO: The test case is empty.");
    }

    /**
     * Test of getRowId method, of interface java.sql.ResultSet.
     */
    public void testGetRowId() throws Exception {
        System.out.println("getRowId");

        testGetXXX("getRowId");
    }

    /**
     * Test of updateRowId method, of interface java.sql.ResultSet.
     */
    public void testUpdateRowId() throws Exception {
        System.out.println("updateRowId");
        
        if (!isTestUpdates())
        {
            return;
        }          

        // TODO:
        fail("TODO: The test case is empty.");
    }

    /**
     * Test of getHoldability method, of interface java.sql.ResultSet.
     */
    public void testGetHoldability() throws Exception {
        System.out.println("getHoldability");

        ResultSet rs = newFOROJdbcResultSet();

        int result = rs.getHoldability();
        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, result);
    }

    /**
     * Test of isClosed method, of interface java.sql.ResultSet.
     */
    public void testIsClosed() throws Exception {
        System.out.println("isClosed");

        ResultSet rs = newFOROJdbcResultSet();

        assertEquals(false, rs.isClosed());

        rs.close();

        assertEquals(true, rs.isClosed());
    }

    /**
     * Test of updateNString method, of interface java.sql.ResultSet.
     */
    public void testUpdateNString() throws Exception {
        System.out.println("updateNString");
        
        if (!isTestUpdates())
        {
            return;
        }          

        // TODO
        fail("TODO: The test case is empty.");
    }

    /**
     * Test of updateNClob method, of interface java.sql.ResultSet.
     */
    public void testUpdateNClob() throws Exception {
        System.out.println("updateNClob");
        
        if (!isTestUpdates())
        {
            return;
        }          

        // TODO
        fail("TODO: The test case is empty.");
    }

    /**
     * Test of getNClob method, of interface java.sql.ResultSet.
     */
    public void testGetNClob() throws Exception {
        System.out.println("getNClob");

        testGetXXX("getNClob");
    }

    /**
     * Test of getSQLXML method, of interface java.sql.ResultSet.
     */
    public void testGetSQLXML() throws Exception {
        System.out.println("getSQLXML");

        testGetXXX("getSQLXML");
    }

    /**
     * Test of updateSQLXML method, of interface java.sql.ResultSet.
     */
    public void testUpdateSQLXML() throws Exception {
        System.out.println("updateSQLXML");
        
        if (!isTestUpdates())
        {
            return;
        }          

        // TODO.
        fail("TODO: The test case is empty.");
    }

    /**
     * Test of getNString method, of interface java.sql.ResultSet.
     */
    public void testGetNString() throws Exception {
        System.out.println("getNString");

        testGetXXX("getNString");
    }

    /**
     * Test of getNCharacterStream method, of interface java.sql.ResultSet.
     */
    public void testGetNCharacterStream() throws Exception {
        System.out.println("getNCharacterStream");

        testGetXXX("getNCharacterStream");
    }

    /**
     * Test of updateNCharacterStream method, of interface java.sql.ResultSet.
     */
    public void testUpdateNCharacterStream() throws Exception {
        System.out.println("updateNCharacterStream");
        
        if (!isTestUpdates())
        {
            return;
        }          

        // TODO
        fail("TODO: The test case is empty.");
    }

    /**
     * Test of unwrap method, of interface java.sql.ResultSet.
     */
    public void testUnwrap() throws Exception {
        System.out.println("unwrap");

        Class<?> iface = jdbcResultSet.class;
        ResultSet rs = newFOROJdbcResultSet();

        Object expResult = rs;
        Object result = rs.unwrap(iface);
        assertEquals(expResult, result);
    }

    /**
     * Test of isWrapperFor method, of interface java.sql.ResultSet.
     */
    public void testIsWrapperFor() throws Exception {
        System.out.println("isWrapperFor");

        Class<?>  iface = jdbcResultSet.class;
        ResultSet rs = newFOROJdbcResultSet();

        boolean expResult = true;
        boolean result = rs.isWrapperFor(iface);
        assertEquals(expResult, result);
    }

    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }

}
