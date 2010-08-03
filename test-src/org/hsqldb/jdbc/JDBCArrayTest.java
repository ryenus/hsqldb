/* Copyright (c) 2001-2010, The HSQL Development Group
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

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 *
 * @author boucherb@users
 */
@ForSubject(JDBCArray.class)
public class JDBCArrayTest extends BaseJdbcTestCase {

    private static final String[] s_colNames = new String[]{
        "C_BIGINT",
        "C_BINARY",
        "C_BOOLEAN",
        "C_CHARACTER",
        "C_DATE",
        "C_DECIMAL",
        "C_DOUBLE",
        "C_FLOAT",
        "C_INTEGER",
        "C_LONGVARBINARY",
        "C_LONGVARCHAR",
        "C_REAL",
        "C_SMALLINT",
        "C_TIME",
        "C_TIMESTAMP",
        "C_TINYINT",
        "C_VARBINARY",
        "C_VARCHAR",
        "C_VARCHAR_IGNORECASE"
    };
    private static final String[] s_baseNames = new String[]{
        "BIGINT",
        "BINARY",
        "BOOLEAN",
        "CHARACTER",
        "DATE",
        "DECIMAL",
        "DOUBLE",
        "DOUBLE",
        "INTEGER",
        "VARBINARY",
        "VARCHAR",
        "DOUBLE",
        "SMALLINT",
        "TIME",
        "TIMESTAMP",
        "TINYINT",
        "VARBINARY",
        "VARCHAR",
        "VARCHAR_IGNORECASE"
    };
    private static final int[] s_baseTypes = new int[]{
        java.sql.Types.BIGINT,
        java.sql.Types.BINARY,
        java.sql.Types.BOOLEAN,
        java.sql.Types.CHAR,
        java.sql.Types.DATE,
        java.sql.Types.DECIMAL,
        java.sql.Types.DOUBLE,
        java.sql.Types.DOUBLE,
        java.sql.Types.INTEGER,
        java.sql.Types.VARBINARY,
        java.sql.Types.VARCHAR,
        java.sql.Types.DOUBLE,
        java.sql.Types.SMALLINT,
        java.sql.Types.TIME,
        java.sql.Types.TIMESTAMP,
        java.sql.Types.TINYINT,
        java.sql.Types.VARBINARY,
        java.sql.Types.VARCHAR,
        java.sql.Types.VARCHAR
    };
    private static final String[] s_toString = new String[]{
        "ARRAY[123456789]",
        "ARRAY[X'0123456789abcdef0000000000000000']",
        "ARRAY[TRUE]",
        "ARRAY['CHAR    ']",
        "ARRAY[DATE'2010-07-04']",
        "ARRAY[0.1234556789]",
        "ARRAY[0.1234556789E0]",
        "ARRAY[0.1234556789E0]",
        "ARRAY[123456789]",
        "ARRAY[X'123456789adbdef0123456789adbdef0123456789adbdef0123456789adbdef0123456789adbdef0123456789adbdef0123456789adbdef0']",
        "ARRAY['0123456789~!@#$%^&*()_+|<>?:\"{}`,./;''ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz']",
        "ARRAY[0.123456789E0]",
        "ARRAY[1234]",
        "ARRAY[TIME'23:11:54']",
        "ARRAY[TIMESTAMP'2010-07-04 23:06:49.005000']",
        "ARRAY[123]",
        "ARRAY[X'0123456789abcdef']",
        "ARRAY['VARCHAR']",
        "ARRAY['VARCHAR_IGNORECASE']"
    };

    public JDBCArrayTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        executeScript("setup-all_array_types-table.sql");
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    protected boolean isTestTypeMap() {
        return getBooleanProperty("test.typemap", false);
    }

    protected Array selectArray(String columnName, Connection conn) throws Exception {
        PreparedStatement ps = connectionFactory().prepareStatement(
                "select t." + columnName + " from all_array_types t where t.id = 1",
                conn);
        ResultSet rs = connectionFactory().executeQuery(ps);
        rs.next();

        Array array = rs.getArray(1);

        return array;
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(JDBCArrayTest.class);

        return suite;
    }

    /**
     * Test of getBaseTypeName method, of class JDBCArray.
     *
     * @throws Exception
     */
    @OfMethod("getBaseTypeName()")
    public void testGetBaseTypeName() throws Exception {
        Connection conn = newConnection();

        for (int i = 0; i < s_colNames.length; i++) {
            Array array = selectArray(s_colNames[i], conn);
            assertEquals(s_baseNames[i], array.getBaseTypeName());
        }
    }

    /**
     * Test of getBaseType method, of class JDBCArray.
     *
     * @throws Exception
     */
    @OfMethod("getBaseType()")
    public void testGetBaseType() throws Exception {
        Connection conn = newConnection();
        for (int i = 0; i < s_colNames.length; i++) {
            Array array = selectArray(s_colNames[i], conn);
            assertEquals(s_baseTypes[i], array.getBaseType());
        }
    }

    /**
     * Test of getArray method, of class JDBCArray.
     *
     * @throws Exception
     */
    @OfMethod("getArray()")
    public void testGetArray_0args() throws Exception {
        Connection conn = newConnection();
        for (int i = 0; i < s_colNames.length; i++) {
            Array array = selectArray(s_colNames[i], conn);
            Object value = array.getArray();
            if (value instanceof Object[]) {
                Object element = ((Object[]) value)[0];

                //printlnPlain(element);
            }
        }
    }

    /**
     * Test of getArray method, of class JDBCArray.
     *
     * @throws Exception
     */
    @OfMethod("getArray(Map<String,Class<?>>)")
    public void testGetArray_Map() throws Exception {
        if (!isTestTypeMap()) {
            return;
        }

        Connection conn = newConnection();
        @SuppressWarnings("CollectionWithoutInitialCapacity")
        Map<String, Class<?>> map = new HashMap<String, Class<?>>();

        for (int i = 0; i < s_colNames.length; i++) {
            Array array = selectArray(s_colNames[i], conn);
            Object value = array.getArray(map);
            if (value instanceof Object[]) {
                Object element = ((Object[]) value)[0];

                //printlnPlain(element);
            }
        }
    }

    /**
     * Test of getArray method, of class JDBCArray.
     *
     * @throws Exception
     */
    @OfMethod("getArray(long,int)")
    public void testGetArray_long_int() throws Exception {
        Connection conn = newConnection();

        for (int i = 0; i < s_colNames.length; i++) {
            Array array = selectArray(s_colNames[i], conn);
            Object value = array.getArray(1, 1);
            if (value instanceof Object[]) {
                Object element = ((Object[]) value)[0];

                //printlnPlain(element);
            }
        }
    }

    /**
     * Test of getArray method, of class JDBCArray.
     *
     * @throws Exception
     */
    @OfMethod("getArray(long,int,Map<String,Class<?>>)")
    public void testGetArray_long_int_Map() throws Exception {
        if (!isTestTypeMap()) {
            return;
        }
        Connection conn = newConnection();
        @SuppressWarnings("CollectionWithoutInitialCapacity")
        Map<String, Class<?>> map = new HashMap<String, Class<?>>();

        for (int i = 0; i < s_colNames.length; i++) {
            Array array = selectArray(s_colNames[i], conn);
            Object value = array.getArray(1, 1, map);
            if (value instanceof Object[]) {
                Object element = ((Object[]) value)[0];

                //printlnPlain(element);
            }
        }
    }

    /**
     * Test of getResultSet method, of class JDBCArray.
     *
     * @throws Exception
     */
    @OfMethod("getResultSet()")
    public void testGetResultSet_0args() throws Exception {
        Connection conn = newConnection();

        for (int i = 0; i < s_colNames.length; i++) {
            Array array = selectArray(s_colNames[i], conn);
            ResultSet rs = array.getResultSet();

            while (rs.next()) {
                Object element = rs.getObject(2);

                //println(element);
            }
        }
    }

    /**
     * Test of getResultSet method, of class JDBCArray.
     *
     * @throws Exception
     */
    @OfMethod("getResultSet(Map<String, Class<?>>)")
    public void testGetResultSet_Map() throws Exception {
        if (!isTestTypeMap()) {
            return;
        }
        Connection conn = newConnection();
        @SuppressWarnings("CollectionWithoutInitialCapacity")
        Map<String, Class<?>> map = new HashMap<String, Class<?>>();

        for (int i = 0; i < s_baseNames.length; i++) {
            Array array = selectArray(s_colNames[i], conn);
            ResultSet rs = array.getResultSet(map);

            while (rs.next()) {
                Object element = rs.getObject(2);

                //printlnPlain(element);
            }
        }
    }

    /**
     * Test of getResultSet method, of class JDBCArray.
     *
     * @throws Exception
     */
    @OfMethod("getResultSet(long,int)")
    public void testGetResultSet_long_int() throws Exception {
        Connection conn = newConnection();

        for (int i = 0; i < s_baseNames.length; i++) {
            Array array = selectArray(s_colNames[i], conn);
            ResultSet rs = array.getResultSet(1, 1);

            while (rs.next()) {
                Object element = rs.getObject(2);

                //printlnPlain(element);
            }
        }
    }

    /**
     * Test of getResultSet method, of class JDBCArray.
     *
     * @throws Exception
     */
    @OfMethod("getResultSet(long,int,HashMap<String, Class<?>>")
    public void testGetResultSet_long_int_Map() throws Exception {
        if (!isTestTypeMap()) {
            return;
        }
        Connection conn = newConnection();
        @SuppressWarnings("CollectionWithoutInitialCapacity")
        Map<String, Class<?>> map = new HashMap<String, Class<?>>();

        for (int i = 0; i < s_baseNames.length; i++) {
            Array array = selectArray(s_colNames[i], conn);
            ResultSet rs = array.getResultSet(1, 1, map);

            while (rs.next()) {
                Object element = rs.getObject(2);

                //println(element);
            }
        }
    }

    /**
     * Test of toString method, of class JDBCArray.
     *
     * @throws Exception
     */
    @OfMethod("toString()")
    public void testToString() throws Exception {
        Connection conn = newConnection();

        for (int i = 0; i < s_colNames.length; i++) {
            Array array = selectArray(s_colNames[i], conn);

            assertEquals(s_toString[i], array.toString());
        }
    }

    /**
     * Test of free method, of class JDBCArray.
     *
     * @throws Exception
     */
    @OfMethod("free()")
    public void testFree() throws Exception {
        Connection conn = newConnection();

        @SuppressWarnings("CollectionWithoutInitialCapacity")
        Map<String, Class<?>> map = new HashMap<String, Class<?>>();

        for (int i = 0; i < s_baseNames.length; i++) {
            Array array = selectArray(s_colNames[i], conn);

            array.free();

            try {
                array.getArray();

                //fail("getArray() suceeded after free()");
            } catch (SQLException ex) {
            }
            try {
                array.getArray(map);

                fail("getArray(Map<String,Class<?>>) suceeded after free()");
            } catch (SQLException ex) {
            }
            try {
                array.getArray(1, 1);

                fail("getArray(long, int) suceeded after free()");
            } catch (SQLException ex) {
            }
            try {
                array.getArray(1, 1, map);

                fail("getArray(long, int, Map<String,Class<?>>) suceeded after free()");
            } catch (SQLException ex) {
            }
            try {
                array.getBaseType();

                fail("getBaseType() suceeded after free()");
            } catch (SQLException ex) {
            }
            try {
                array.getBaseTypeName();

                fail("getBaseTypeName() suceeded after free()");
            } catch (SQLException ex) {
            }
            try {
                array.getResultSet();

                fail("getResultSet() suceeded after free()");
            } catch (SQLException ex) {
            }
            try {
                array.getResultSet(map);

                fail("getResultSet(Map<String,Class<?>>) suceeded after free()");
            } catch (SQLException ex) {
            }
            try {
                array.getResultSet(1, 1);

                fail("getResultSet(long, int) suceeded after free()");
            } catch (SQLException ex) {
            }
            try {
                array.getResultSet(1, 1, map);

                fail("getResultSet(long, int, Map<String,Class<?>>) suceeded after free()");
            } catch (SQLException ex) {
            }
        }
    }

    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }
}
