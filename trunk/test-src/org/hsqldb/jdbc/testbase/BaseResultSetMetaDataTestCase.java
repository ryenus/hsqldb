// <editor-fold defaultstate="collapsed" desc="Copyright Notice & Disclaimer">
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
// </editor-fold>
package org.hsqldb.jdbc.testbase;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 * Provides...
 *
 * @author Campbell Boucher-Burnet <campbell.boucherburnet@gov.sk.ca>;
 * @version 1.0
 * @since 1.0
 */
@ForSubject(java.sql.ResultSetMetaData.class)
public abstract class BaseResultSetMetaDataTestCase extends BaseJdbcTestCase {

    protected static final int MIN_INT = Integer.MIN_VALUE;
    protected static final int ZERO = 0;
    protected static final boolean DEFAULT_AUTOINCREMENT = false;
    protected static final boolean DEFAULT_CASE_SENSSIIVE = false;
    public static final boolean DEFAULT_SIGNED = false;
    public static final int DEFAULT_NULLABLE = ResultSetMetaData.columnNullableUnknown;
    public static final boolean DEFAULT_CURRENCY = false;
    public static final boolean DEFAULT_SEARCHABLE = false;
    public static final boolean DEFAULT_READ_ONLY = false;
    public static final boolean DEFAULT_WRITABLE = false;
    public static final boolean DEFAULT_DEFINITELY_WRITABLE = false;
    public static final String NULL_STRING = null;

    public BaseResultSetMetaDataTestCase(String testName) {
        super(testName);
    }

    protected int getPositiveIntProperty(String key) throws IllegalStateException {
        int value =  this.getIntProperty(key,
                MIN_INT);
        if (value < ZERO) {

            throw new IllegalStateException(
                    "Non-positive value configured for property key: "
                    + translatePropertyKey(key));
        }
        return value;
    }

    protected boolean isTestGetCatalogName() {
        return getBooleanProperty("rsmd.test.get.catalog.name", false);
    }

    protected boolean isTestGetSchemaName() {
        return getBooleanProperty("rsmd.test.get.schema.name", true);
    }

    protected boolean isIgnoreColumnNameCase() {
        return getBooleanProperty("rsmd.ignore.column.name.case", false);
    }

    protected boolean isIgnoreColumnLabelCase() {
        return getBooleanProperty("rsmd.ignore.column.label.case", false);
    }

    protected int getExpectedColumnCount() {
        return getPositiveIntProperty("rsmd.column.count");
    }

    protected boolean getExpectedIsAutoIncrement(int columnIndex) {
        return this.getBooleanProperty("rsmd.autoincrement." + columnIndex,
                DEFAULT_AUTOINCREMENT);
    }

    protected boolean getExpectedIsCaseSensitive(int columnIndex) {
        return this.getBooleanProperty("rsmd.case.sensitive." + columnIndex,
                DEFAULT_CASE_SENSSIIVE);
    }

    protected boolean getExpectedIsSearchable(int columnIndex) {
        return this.getBooleanProperty("rsmd.searchable." + columnIndex,
                DEFAULT_SEARCHABLE);
    }

    protected boolean getExpectedIsCurrency(int columnIndex) {
        return this.getBooleanProperty("rsmd.currency." + columnIndex,
                DEFAULT_CURRENCY);
    }

    protected int getExpectedIsNullable(int columnIndex) {
        String defaultFieldName = "java.sql.ResultSetMetaData.columnNullableUnknown";
        try {
            String fieldName = getProperty("rsmd.nullable." + columnIndex, defaultFieldName);
            return this.getFieldValue(fieldName);
        } catch (Exception ex) {
            throw new RuntimeException(ex.toString(), ex);
        }
    }

    protected boolean getExpectedIsSigned(int columnIndex) {
        return this.getBooleanProperty("rsmd.signed." + columnIndex,
                DEFAULT_SIGNED);
    }

    protected int getExpectedColumnDisplaySize(int columnIndex) {
        return getPositiveIntProperty("rsmd.column.display.size." + columnIndex);
    }

    protected String getExpectedColumnLabel(int columnIndex) {
        return this.getProperty("rsmd.column.label." + columnIndex, NULL_STRING);
    }

    protected String getExpectedColumnName(int columnIndex) {
        return this.getProperty("rsmd.column.name." + columnIndex, NULL_STRING);
    }

    protected String getExpectedSchemaName(int columnIndex) {
        return this.getProperty("rsmd.schema.name." + columnIndex, NULL_STRING);
    }

    protected int getExpectedPrecision(int columnIndex) {
        return getPositiveIntProperty("rsmd.precision." + columnIndex);
    }

    protected int getExpectedScale(int columnIndex) {
        return this.getIntProperty("rsmd.scale." + columnIndex,
                MIN_INT);
    }

    protected String getExpectedTableName(int column) {
        return this.getProperty("rsmd.table.name." + column, NULL_STRING);
    }

    protected String getExpectedCatalogName(int column) {
        return this.getProperty("rsmd.catalog.name." + column, NULL_STRING);
    }

    protected int getExpectedColumnType(int column) {
        String fieldName = getProperty("rsmd.column.type." + column, "java.sql.Types.NULL");

        try {
            return fieldName == null
                    ? MIN_INT
                    : this.getFieldValue(fieldName);
        } catch (Exception ex) {
            throw new RuntimeException(ex.toString(), ex);
        }
    }

    protected String getExpectedColumnTypeName(int column) {
        return this.getProperty("rsmd.column.type.name." + column, null);
    }

    protected boolean getExpectedIsReadOnly(int column) {
        return this.getBooleanProperty("expected.read.only." + column,
                DEFAULT_READ_ONLY);
    }

    protected boolean getExpectedIsWritable(int column) {
        return this.getBooleanProperty("rsmd.writable." + column,
                DEFAULT_WRITABLE);
    }

    protected boolean getExpectedIsDefinitelyWritable(int column) {
        return this.getBooleanProperty("rsmd.definitely.writable." + column,
                DEFAULT_DEFINITELY_WRITABLE);
    }

    protected String getExpectedColumnClassName(int column) {
        return getProperty("rsmd.column.class.name." + column, null);
    }

    protected String getExpectedUnwrapIfaceFQN() {
        return getProperty("rsmd.unwrap.iface.fqn", null);
    }

    protected Class<?> getTestSuccessfulUnwrapIFace() {
        try {
            return Class.forName(getExpectedUnwrapIfaceFQN());
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex.toString(), ex);
        }
    }

    protected Class<?> getTestFailureUnwrapIFace() {
        return String.class;
    }

    protected String getSelect() {
        return getProperty("rsmd.select", null);
    }

    protected ResultSetMetaData newResultSetMetaData() throws Exception {
        ResultSetMetaData rsmd = newConnection().createStatement(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.TYPE_FORWARD_ONLY).executeQuery(
                getSelect()).getMetaData();

        connectionFactory().closeRegisteredObjects();

        return rsmd;
    }

    /**
     * Test of getCatalogName method, of interface java.sql.ResultSetMetaData.
     */
    @OfMethod("getCatalogName(int)")
    public void testGetCatalogName() throws Exception {
        if (!isTestGetCatalogName()) {
            return;
        }
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            assertEquals("column: " + i,
                    this.getExpectedCatalogName(i),
                    rsmd.getCatalogName(i));
        }
    }

    /**
     * Test of getColumnName method, of interface java.sql.ResultSetMetaData.
     */
    @OfMethod("getColumnName(int)")
    public void testGetColumnName() throws Exception {
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        boolean ignoreCase = isIgnoreColumnNameCase();

        for (int i = 1; i <= columnCount; i++) {
            String expectedName = getExpectedColumnName(i);
            String actualName = rsmd.getColumnName(i);

            if (expectedName != null && ignoreCase) {
                expectedName = expectedName.toLowerCase();
            }

            if (actualName != null && ignoreCase) {
                actualName = actualName.toLowerCase();
            }

            assertEquals("column: " + i, expectedName, actualName);
        }
    }

    /**
     * Test of getColumnClassName method, of interface
     * java.sql.ResultSetMetaData.
     */
    @OfMethod("getColumnClassName(int)")
    public void testGetColumnClassName() throws Exception {
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            printProgress("Expected: " + getExpectedColumnClassName(i));
            assertEquals("column: " + i,
                    getExpectedColumnClassName(i),
                    rsmd.getColumnClassName(i));
        }
    }

    /**
     * Test of getColumnCount method, of interface java.sql.ResultSetMetaData.
     */
    @OfMethod("getColumnCount)")
    public void testGetColumnCount() throws Exception {
        assertEquals(getExpectedColumnCount(),
                newResultSetMetaData().getColumnCount());
    }

    /**
     * Test of getColumnDisplaySize method, of interface
     * java.sql.ResultSetMetaData.
     */
    @OfMethod("getColumnDisplaySize(int)")
    public void testGetColumnDisplaySize() throws Exception {
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            assertEquals("column: " + i,
                    this.getExpectedColumnDisplaySize(i),
                    getExpectedColumnDisplaySize(i),
                    rsmd.getColumnDisplaySize(i));
        }
    }

    /**
     * Test of getColumnLabel method, of interface java.sql.ResultSetMetaData.
     */
    @OfMethod("getColumnLabel(int)")
    public void testGetColumnLabel() throws Exception {
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        boolean ignoreCase = isIgnoreColumnLabelCase();

        for (int i = 1; i <= columnCount; i++) {
            String expectedLabel = getExpectedColumnLabel(i);
            String actualLabel = rsmd.getColumnLabel(i);

            if (expectedLabel != null && ignoreCase) {
                expectedLabel = expectedLabel.toLowerCase();
            }

            if (actualLabel != null && ignoreCase) {
                actualLabel = actualLabel.toLowerCase();
            }

            assertEquals("column: " + i, expectedLabel, actualLabel);
        }
    }

    /**
     * Test of getColumnType method, of interface java.sql.ResultSetMetaData.
     */
    @OfMethod("getColumnType(int)")
    public void testGetColumnType() throws Exception {
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            assertEquals("column: " + i,
                    getExpectedColumnType(i),
                    rsmd.getColumnType(i));
        }
    }

    /**
     * Test of getColumnTypeName method, of interface
     * java.sql.ResultSetMetaData.
     */
    @OfMethod("getColumnTypeName(int)")
    public void testGetColumnTypeName() throws Exception {
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            assertEquals("column: " + i,
                    getExpectedColumnTypeName(i),
                    rsmd.getColumnTypeName(i));
        }
    }

    /**
     * Test of getPrecision method, of class java.sql.ResultSetMetaData.
     */
    @OfMethod("getPrecision(int)")
    public void testGetPrecision() throws Exception {
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            assertEquals("column: " + i,
                    getExpectedPrecision(i),
                    rsmd.getPrecision(i));
        }
    }

    /**
     * Test of getScale method, of interface java.sql.ResultSetMetaData.
     */
    @OfMethod("getScale(int)")
    public void testGetScale() throws Exception {
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            assertEquals("column: " + i,
                    getExpectedScale(i),
                    rsmd.getScale(i));
        }
    }

    /**
     * Test of getSchemaName method, of interface java.sql.ResultSetMetaData.
     */
    @OfMethod("getSchemaName(int)")
    public void testGetSchemaName() throws Exception {
        if (!isTestGetSchemaName()) {
            return;
        }
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            assertEquals("column: " + i,
                    getExpectedSchemaName(i),
                    rsmd.getSchemaName(i));
        }
    }

    /**
     * Test of getTableName method, of interface java.sql.ResultSetMetaData.
     */
    @OfMethod("getTableName(int)")
    public void testGetTableName() throws Exception {
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            assertEquals("column: " + i,
                    getExpectedTableName(i),
                    rsmd.getTableName(i));
        }
    }

    /**
     * Test of isAutoIncrement method, of interface java.sql.ResultSetMetaData.
     */
    @OfMethod("isAutoIncrement(int)")
    public void testIsAutoIncrement() throws Exception {
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            assertEquals("column: " + i,
                    getExpectedIsAutoIncrement(i),
                    rsmd.isAutoIncrement(i));
        }
    }

    /**
     * Test of isCaseSensitive method, of interface java.sql.ResultSetMetaData.
     */
    @OfMethod("isCaseSensitive(int)")
    public void testIsCaseSensitive() throws Exception {
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            assertEquals("column: " + i,
                    getExpectedIsCaseSensitive(i),
                    rsmd.isCaseSensitive(i));
        }
    }

    /**
     * Test of isCurrency method, of interface java.sql.ResultSetMetaData.
     */
    @OfMethod("isCurrency(int)")
    public void testIsCurrency() throws Exception {
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        // don't support currency type
        for (int i = 1; i <= columnCount; i++) {
            assertEquals("column: " + i,
                    getExpectedIsCurrency(i),
                    rsmd.isCurrency(i));
        }
    }

    /**
     * Test of isDefinitelyWritable method, of interface
     * java.sql.ResultSetMetaData.
     */
    @OfMethod("isDefinitelyWritable(int)")
    public void testIsDefinitelyWritable() throws Exception {
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            assertEquals("column: " + i,
                    getExpectedIsDefinitelyWritable(i),
                    rsmd.isDefinitelyWritable(i));
        }
    }

    /**
     * Test of isNullable method, of interface java.sql.ResultSetMetaData.
     */
    @OfMethod("isNullable(int)")
    public void testIsNullable() throws Exception {
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            assertEquals("column: " + i,
                    getExpectedIsNullable(i),
                    rsmd.isNullable(i));
        }
    }

    /**
     * Test of isReadOnly method, of interface java.sql.ResultSetMetaData.
     */
    @OfMethod("isReadOnly(int)")
    public void testIsReadOnly() throws Exception {
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            assertEquals("column: " + i,
                    getExpectedIsReadOnly(i),
                    rsmd.isReadOnly(i));
        }
    }

    /**
     * Test of isSearchable method, of interface java.sql.ResultSetMetaData.
     */
    @OfMethod("isSearchable(int)")
    public void testIsSearchable() throws Exception {
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            assertEquals("column: " + i,
                    getExpectedIsSearchable(i),
                    rsmd.isSearchable(i));
        }
    }

    /**
     * Test of isSigned method, of interface java.sql.ResultSetMetaData.
     */
    @OfMethod("isSigned(int)")
    public void testIsSigned() throws Exception {
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            assertEquals("column: " + i,
                    getExpectedIsSigned(i),
                    rsmd.isSigned(i));
        }
    }

    /**
     * Test of isWrapperFor method, of interface java.sql.ResultSetMetaData.
     */
    @OfMethod("isWrapperFor(java.lang.Class<?>)")
    public void testIsWrapperFor() throws Exception {
        Class<?> iface = getTestSuccessfulUnwrapIFace();
        ResultSetMetaData rsmd = newResultSetMetaData();
        assertEquals(true, rsmd.isWrapperFor(iface));
    }

    /**
     * Test of isWritable method, of interface java.sql.ResultSetMetaData.
     */
    @OfMethod("isWritable(int)")
    public void testIsWritable() throws Exception {
        ResultSetMetaData rsmd = newResultSetMetaData();
        int columnCount = rsmd.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            assertEquals("column: " + i,
                    getExpectedIsWritable(i),
                    rsmd.isWritable(i));
        }
    }

    /**
     * Test of unwrap method, of interface java.sql.ResultSetMetaData.
     */
    public void testUnwrap() throws Exception {
        Class<?> iface = getTestSuccessfulUnwrapIFace();
        ResultSetMetaData rsmd = newResultSetMetaData();
        assertEquals(getExpectedUnwrapIfaceFQN(),
                rsmd.unwrap(iface).getClass().getName());
    }
}
