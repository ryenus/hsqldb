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

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.text.MessageFormat;

import java.util.HashMap;
import java.util.Map;

import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @version 2.1.0
 * @since HSQLDB 2.1.0
 */
@ForSubject(java.sql.Array.class)
public abstract class BaseArrayTestCase extends BaseJdbcTestCase {

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
        "C_VARCHAR"
    };

    private static final int s_resultColumnCount = s_colNames.length;

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
        "VARCHAR"
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
        "ARRAY['VARCHAR']"
    };
    private static final String s_setupScriptResourceName = "setup-all_array_types-table.sql";
    private static final String s_arraySelectStatement = "select t.{0} from all_array_types t where t.id = 1";

    public static String getExpectedArrayToStringValue(int i) {
        return s_toString[i];
    }

    public BaseArrayTestCase(String name) {
        super(name);
    }

    protected String getArraySelectStatement(String columnName) {
        return MessageFormat.format(s_arraySelectStatement, columnName);
    }

    protected int getResultColumnCount() {
        return s_resultColumnCount;
    }

    protected int getBaseType(int i) {
        return s_baseTypes[i];
    }

    protected String getResultBaseName(int i) {
        return s_baseNames[i];
    }

    protected String getResultColName(int i) {
        return s_colNames[i];
    }

    protected String getSetupScriptResourceName() {
        return s_setupScriptResourceName;
    }

    protected boolean isTestTypeMap() {
        return getBooleanProperty("test.typemap", false);
    }

    protected Array selectArray(String columnName, Connection conn) throws Exception {
        PreparedStatement ps = connectionFactory().prepareStatement(getArraySelectStatement(columnName), conn);
        ResultSet rs = connectionFactory().executeQuery(ps);
        rs.next();
        Array array = rs.getArray(1);
        return array;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        executeScript(getSetupScriptResourceName());
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    @OfMethod(value = "free()")
    public void testFree() throws Exception {
        Connection conn = newConnection();
        @SuppressWarnings(value = "CollectionWithoutInitialCapacity")
        Map<String, Class<?>> map = new HashMap<String, Class<?>>();
        for (int i = 0; i < getResultColumnCount(); i++) {
            Array array = selectArray(getResultColName(i), conn);
            array.free();
            try {
                array.getArray();
                fail("getArray() suceeded after free()");
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

    @OfMethod(value = "getArray()")
    public void testGetArray_0args() throws Exception {
        Connection conn = newConnection();
        for (int i = 0; i < getResultColumnCount(); i++) {
            Array array = selectArray(getResultColName(i), conn);
            Object value = array.getArray();
            if (value instanceof Object[]) {
                Object element = ((Object[]) value)[0];
                //printlnPlain(element);
            }
        }
    }

    @OfMethod(value = "getArray(Map<String,Class<?>>)")
    public void testGetArray_Map() throws Exception {
        if (!isTestTypeMap()) {
            return;
        }
        Connection conn = newConnection();
        @SuppressWarnings(value = "CollectionWithoutInitialCapacity")
        Map<String, Class<?>> map = new HashMap<String, Class<?>>();
        for (int i = 0; i < getResultColumnCount(); i++) {
            Array array = selectArray(getResultColName(i), conn);
            Object value = array.getArray(map);
            if (value instanceof Object[]) {
                Object element = ((Object[]) value)[0];
                //printlnPlain(element);
            }
        }
    }

    @OfMethod(value = "getArray(long,int)")
    public void testGetArray_long_int() throws Exception {
        Connection conn = newConnection();
        for (int i = 0; i < getResultColumnCount(); i++) {
            Array array = selectArray(getResultColName(i), conn);
            Object value = array.getArray(1, 1);
            if (value instanceof Object[]) {
                Object element = ((Object[]) value)[0];
                //printlnPlain(element);
            }
        }
    }

    @OfMethod(value = "getArray(long,int,Map<String,Class<?>>)")
    public void testGetArray_long_int_Map() throws Exception {
        if (!isTestTypeMap()) {
            return;
        }
        Connection conn = newConnection();
        @SuppressWarnings(value = "CollectionWithoutInitialCapacity")
        Map<String, Class<?>> map = new HashMap<String, Class<?>>();
        for (int i = 0; i < getResultColumnCount(); i++) {
            Array array = selectArray(getResultColName(i), conn);
            Object value = array.getArray(1, 1, map);
            if (value instanceof Object[]) {
                Object element = ((Object[]) value)[0];
                //printlnPlain(element);
            }
        }
    }

    @OfMethod(value = "getBaseType()")
    public void testGetBaseType() throws Exception {
        Connection conn = newConnection();
        for (int i = 0; i < getResultColumnCount(); i++) {
            Array array = selectArray(getResultColName(i), conn);
            assertEquals(getBaseType(i), array.getBaseType());
        }
    }

    @OfMethod(value = "getBaseTypeName()")
    public void testGetBaseTypeName() throws Exception {
        Connection conn = newConnection();
        for (int i = 0; i < getResultColumnCount(); i++) {
            Array array = selectArray(getResultColName(i), conn);
            assertEquals(getResultBaseName(i), array.getBaseTypeName());
        }
    }

    @OfMethod(value = "getResultSet()")
    public void testGetResultSet_0args() throws Exception {
        Connection conn = newConnection();
        for (int i = 0; i < getResultColumnCount(); i++) {
            Array array = selectArray(getResultColName(i), conn);
            ResultSet rs = array.getResultSet();
            while (rs.next()) {
                Object element = rs.getObject(2);
                //println(element);
            }
        }
    }

    @OfMethod(value = "getResultSet(Map<String, Class<?>>)")
    public void testGetResultSet_Map() throws Exception {
        if (!isTestTypeMap()) {
            return;
        }
        Connection conn = newConnection();
        @SuppressWarnings(value = "CollectionWithoutInitialCapacity")
        Map<String, Class<?>> map = new HashMap<String, Class<?>>();
        for (int i = 0; i < getResultColumnCount(); i++) {
            Array array = selectArray(getResultColName(i), conn);
            ResultSet rs = array.getResultSet(map);
            while (rs.next()) {
                Object element = rs.getObject(2);
                //printlnPlain(element);
            }
        }
    }

    @OfMethod(value = "getResultSet(long,int)")
    public void testGetResultSet_long_int() throws Exception {
        Connection conn = newConnection();
        for (int i = 0; i < getResultColumnCount(); i++) {
            Array array = selectArray(getResultColName(i), conn);
            ResultSet rs = array.getResultSet(1, 1);
            while (rs.next()) {
                Object element = rs.getObject(2);
                //printlnPlain(element);
            }
        }
    }

    @OfMethod(value = "getResultSet(long,int,HashMap<String, Class<?>>")
    public void testGetResultSet_long_int_Map() throws Exception {
        if (!isTestTypeMap()) {
            return;
        }
        Connection conn = newConnection();
        @SuppressWarnings(value = "CollectionWithoutInitialCapacity")
        Map<String, Class<?>> map = new HashMap<String, Class<?>>();
        for (int i = 0; i < getResultColumnCount(); i++) {
            Array array = selectArray(getResultColName(i), conn);
            ResultSet rs = array.getResultSet(1, 1, map);
            while (rs.next()) {
                Object element = rs.getObject(2);
                //println(element);
            }
        }
    }

    @OfMethod(value = "toString()")
    public void testToString() throws Exception {
        Connection conn = newConnection();
        for (int i = 0; i < getResultColumnCount(); i++) {
            Array array = selectArray(getResultColName(i), conn);
            assertEquals(getExpectedArrayToStringValue(i), array.toString());
        }
    }
}
