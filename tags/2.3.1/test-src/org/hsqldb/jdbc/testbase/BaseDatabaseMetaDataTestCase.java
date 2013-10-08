package org.hsqldb.jdbc.testbase;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import org.hsqldb.jdbc.JDBCResultSet;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 * Basis for test of interface java.sql.DatabaseMetaData.
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 */
@ForSubject(java.sql.DatabaseMetaData.class)
public abstract class BaseDatabaseMetaDataTestCase extends BaseJdbcTestCase {

    public BaseDatabaseMetaDataTestCase(String testName) {
        super(testName);
    }

    protected DatabaseMetaDataDefaultValues defaultValueFor() {
        return DatabaseMetaDataDefaultValues.newInstance("hsqldb.test.suite");
    }

    protected int getExpectedDatabaseMajorVersion() {
        int rval = getIntProperty("dbmd.database.major.version", Integer.MIN_VALUE);
        if (rval == Integer.MIN_VALUE) {
            throw new RuntimeException("The expected database major version must be set in the test properties.");
        }
        return rval;
    }

    protected int getExpectedDatabaseMinorVersion() {
        int rval = getIntProperty("dbmd.database.minor.version", Integer.MIN_VALUE);
        if (rval == Integer.MIN_VALUE) {
            throw new RuntimeException("The expected database minor version must be set in the test properties.");
        }
        return rval;
    }

    protected String getExpectedDatabaseProductName() {
        String rval = getProperty("dbmd.database.product.name", null);
        if (rval == null) {
            throw new RuntimeException("The expected database product name must be set in the test properties.");
        }
        return rval;
    }

    protected String getExpectedDatabaseProductVersion() {
        String rval = getProperty("dbmd.database.product.version", null);
        if (rval == null) {
            throw new RuntimeException("The expected database product version string must be set in the test properties.");
        }
        return rval;
    }

    protected int getExpectedDriverMajorVersion() {
        int rval = getIntProperty("dbmd.driver.major.version", Integer.MIN_VALUE);
        if (rval == Integer.MIN_VALUE) {
            throw new RuntimeException("The expected driver major version must be set in the test properties.");
        }
        return rval;
    }

    protected int getExpectedDriverMinorVersion() {
        int rval = getIntProperty("dbmd.driver.minor.version", Integer.MIN_VALUE);
        if (rval == Integer.MIN_VALUE) {
            throw new RuntimeException("The expected driver minor version must be set in the test properties.");
        }
        return rval;
    }

    protected String getExpectedDriverName() {
        String rval = getProperty("dbmd.driver.name", null);
        if (rval == null) {
            throw new RuntimeException("The expected driver name must be set in the test properties.");
        }
        return rval;
    }

    protected String getExpectedDriverVersion() {
        String rval = getProperty("dbmd.driver.version", null);
        if (rval == null) {
            throw new RuntimeException("The expected driver version string must be set in the test properties.");
        }
        return rval;
    }

    protected DatabaseMetaData getMetaData() throws Exception {
        return newConnection().getMetaData();
    }

    protected long readResultSet(final ResultSet rs) throws Exception {
        long rowCount = 0;
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int cols = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= cols; i++) {
                    Object o = rs.getObject(i);
                }
                rowCount++;
            }
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                }
            }
        }
        return rowCount;
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
     * Test of allProceduresAreCallable method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "allProceduresAreCallable()")
    public void testAllProceduresAreCallable() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.all.procedures.are.callable", defaultValueFor().allProceduresAreCallable()); // true
        boolean result = dbmd.allProceduresAreCallable();
        assertEquals(expResult, result);
    }

    /**
     * Test of allTablesAreSelectable method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "allTablesAreSelectable()")
    public void testAllTablesAreSelectable() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.all.tables.are.selectable", defaultValueFor().allTablesAreSelectable()); //  true
        boolean result = dbmd.allTablesAreSelectable();
        assertEquals(expResult, result);
    }

    /**
     * Test of autoCommitFailureClosesAllResultSets method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "autoCommitFailureClosesAllResultSets()")
    public void testAutoCommitFailureClosesAllResultSets() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.auto.commit.failure.closes.all.result.sets", defaultValueFor().autoCommitFailureClosesAllResultSets()); // true
        boolean result = dbmd.autoCommitFailureClosesAllResultSets();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of dataDefinitionCausesTransactionCommit method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "dataDefinitionCausesTransactionCommit()")
    public void testDataDefinitionCausesTransactionCommit() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.data.definition.causes.transaction.commit", defaultValueFor().dataDefinitionCausesTransactionCommit()); // true
        boolean result = dbmd.dataDefinitionCausesTransactionCommit();
        assertEquals(expResult, result);
    }

    /**
     * Test of dataDefinitionIgnoredInTransactions method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "dataDefinitionIgnoredInTransactions()")
    public void testDataDefinitionIgnoredInTransactions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.data.definition.ignored.in.transactions", defaultValueFor().dataDefinitionIgnoredInTransactions()); // false
        boolean result = dbmd.dataDefinitionIgnoredInTransactions();
        assertEquals(expResult, result);
    }

    /**
     * Test of deletesAreDetected method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "deletesAreDetected()")
    public void testDeletesAreDetected() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        //--
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        boolean expResult = getBooleanProperty("dbmd.deletes.are.detected.type.forward.only", defaultValueFor().deletesAreDetected(type)); // false
        boolean result = dbmd.deletesAreDetected(type);
        assertEquals("DeletesAreDetected - TYPE_FORWARD_ONLY", expResult, result);
        //--
        type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
        expResult = getBooleanProperty("dbmd.deletes.are.detected.type.scroll.insensitive", defaultValueFor().deletesAreDetected(type)); // false
        result = dbmd.deletesAreDetected(type);
        assertEquals("DeletesAreDetected - TYPE_SCROLL_INSENSITIVE", expResult, result);
        //--
        type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;
        expResult = getBooleanProperty("dbmd.deletes.are.detected.type.scroll.sensitive", defaultValueFor().deletesAreDetected(type)); // false
        result = dbmd.deletesAreDetected(type);
        assertEquals("DeletesAreDetected - TYPE_SCROLL_SENSITIVE", expResult, result);
    }

    /**
     * Test of doesMaxRowSizeIncludeBlobs method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "doesMaxRowSizeIncludeBlobs()")
    public void testDoesMaxRowSizeIncludeBlobs() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.does.max.row.size.include.blobs", defaultValueFor().doesMaxRowSizeIncludeBlobs()); // true
        boolean result = dbmd.doesMaxRowSizeIncludeBlobs();
        assertEquals(expResult, result);
    }

    /**
     * Test of getAttributes method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getAttributes(java.lang.String,java.lang.String,java.lang.String,java.lang.String)")
    public void testGetAttributes() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        String typeNamePattern = "%";
        String attributeNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getBestRowIdentifier method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getBestRowIdentifier(java.lang.String,java.lang.String,java.lang.String,int,boolean)")
    public void testGetBestRowIdentifier() throws Exception {
        String catalog = null;
        String schema = null;
        String table = "%";
        int[] scope = new int[]{DatabaseMetaData.bestRowTemporary, DatabaseMetaData.bestRowSession, DatabaseMetaData.bestRowSession};
        boolean[] nullable = new boolean[]{true, false};
        DatabaseMetaData dbmd = getMetaData();
        for (int i = 0; i < scope.length; i++) {
            for (int j = 0; j < nullable.length; j++) {
                ResultSet result = dbmd.getBestRowIdentifier(catalog, schema, table, scope[i], nullable[j]);
                readResultSet(result);
                result.close();
            }
        }
    }

    /**
     * Test of getCatalogSeparator method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getCatalogSeparator()")
    public void testGetCatalogSeparator() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getProperty("dbmd.catalog.separator", defaultValueFor().getCatalogSeparator());
        String result = dbmd.getCatalogSeparator();
        assertEquals(expResult, result);
    }

    /**
     * Test of getCatalogTerm method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getCatalogTerm()")
    public void testGetCatalogTerm() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getProperty("dbmd.catalog.term", defaultValueFor().getCatalogTerm());
        String result = dbmd.getCatalogTerm();
        assertEquals(expResult, result);
    }

    /**
     * Test of getCatalogs method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getCatalogs()")
    public void testGetCatalogs() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getCatalogs();
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getClientInfoProperties method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getClientInfoProperties()")
    public void testGetClientInfoProperties() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        try {
            ResultSet result = dbmd.getClientInfoProperties();
        } catch (Exception e) {
            stubTestResult("TODO: " + e.toString());
        }
    }

    /**
     * Test of getColumnPrivileges method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getColumnPrivileges(java.lang.String,java.lang.String,java.lang.String,java.lang.String)")
    public void testGetColumnPrivileges() throws Exception {
        String catalog = null;
        String schema = null;
        String table = null;
        String columnNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getColumnPrivileges(catalog, schema, table, columnNamePattern);
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getColumns method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getColumns(java.lang.String,java.lang.String,java.lang.String,java.lang.String)")
    public void testGetColumns() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        String tableNamePattern = "%";
        String columnNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getConnection method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getConnection()")
    public void testGetConnection() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        Connection result = dbmd.getConnection();
        assertNotNull("Connection", result);
    }

    /**
     * Test of getCrossReference method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getCrossReference(java.lang.String,java.lang.String,java.lang.String)")
    public void testGetCrossReference() throws Exception {
        String parentCatalog = null;
        String parentSchema = null;
        String parentTable = "%";
        String foreignCatalog = null;
        String foreignSchema = null;
        String foreignTable = "%";
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getCrossReference(parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable);
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getDatabaseMajorVersion method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getDatabaseMajorVersion()")
    public void testGetDatabaseMajorVersion() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.database.major.version", getExpectedDatabaseMajorVersion());
        int result = dbmd.getDatabaseMajorVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDatabaseMinorVersion method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getDatabaseMinorVersion()")
    public void testGetDatabaseMinorVersion() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.database.minor.version", getExpectedDatabaseMinorVersion());
        int result = dbmd.getDatabaseMinorVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDatabaseProductName method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getDatabaseProductName()")
    public void testGetDatabaseProductName() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getExpectedDatabaseProductName();
        String result = dbmd.getDatabaseProductName();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDatabaseProductVersion method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getDatabaseProductVersion()")
    public void testGetDatabaseProductVersion() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getExpectedDatabaseProductVersion();
        String result = dbmd.getDatabaseProductVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDefaultTransactionIsolation method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getDefaultTransactionIsolation()")
    public void testGetDefaultTransactionIsolation() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.default.transaction.isolation", defaultValueFor().getDefaultTransactionIsolation()); // Connection.TRANSACTION_SERIALIZABLE
        int result = dbmd.getDefaultTransactionIsolation();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDriverMajorVersion method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getDriverMajorVersion()")
    public void testGetDriverMajorVersion() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getExpectedDriverMajorVersion();
        int result = dbmd.getDriverMajorVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDriverMinorVersion method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getDriverMinorVersion()")
    public void testGetDriverMinorVersion() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getExpectedDriverMinorVersion();
        int result = dbmd.getDriverMinorVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDriverName method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getDriverName()")
    public void testGetDriverName() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getExpectedDriverName();
        String result = dbmd.getDriverName();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDriverVersion method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getDriverVersion()")
    public void testGetDriverVersion() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getExpectedDriverVersion();
        String result = dbmd.getDriverVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getExportedKeys method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getExportedKeys(java.lang.String,java.lang.String,java.lang.String)")
    public void testGetExportedKeys() throws Exception {
        String catalog = null;
        String schema = null;
        String table = "%";
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getExportedKeys(catalog, schema, table);
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getExtraNameCharacters method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getExtraNameCharacters()")
    public void testGetExtraNameCharacters() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getProperty("dbmd.extra.name.characters", defaultValueFor().getExtraNameCharacters());
        String result = null;
        try {
            result = dbmd.getExtraNameCharacters();
        } catch (Exception e) {
            fail(e.toString());
        }
        if (expResult != null) {
            assertEquals(expResult, result);
        }
    }

    /**
     * Test of getFunctionColumns method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getFunctions(java.lang.String,java.lang.String,java.lang.String,java.lang.String)")
    public void testGetFunctionColumns() throws Exception {
        String catalog = "";
        String schemaPattern = "%";
        String functionNamePattern = "%";
        String columnNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();
        ResultSet rs = dbmd.getFunctionColumns(catalog, schemaPattern, functionNamePattern, columnNamePattern);
        long rowCount = readResultSet(rs);
    }

    /**
     * Test of getFunctions method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getFunctions(java.lang.String,java.lang.String,java.lang.String)")
    public void testGetFunctions() throws Exception {
        String catalog = "";
        String schemaPattern = "%";
        String functionNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();
        ResultSet rs = dbmd.getFunctions(catalog, schemaPattern, functionNamePattern);
        long rowCount = readResultSet(rs);
    }

    /**
     * Test of getIdentifierQuoteString method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getIdentifierQuoteString()")
    public void testGetIdentifierQuoteString() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getProperty("dbmd.identifier.quote.string", defaultValueFor().getIdentifierQuoteString());
        String result = dbmd.getIdentifierQuoteString();
        assertEquals(expResult, result);
    }

    /**
     * Test of getImportedKeys method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getImportedKeys(java.lang.String,java.lang.String,java.lang.String)")
    public void testGetImportedKeys() throws Exception {
        String catalog = null;
        String schema = null;
        String table = "%";
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getImportedKeys(catalog, schema, table);
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getIndexInfo method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getIndexInfo(java.lang.String,java.lang.String,java.lang.String,boolean,boolean)")
    public void testGetIndexInfo() throws Exception {
        String catalog = null;
        String schema = null;
        String table = "%";
        boolean unique = true;
        boolean approximate = true;
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getIndexInfo(catalog, schema, table, unique, approximate);
        readResultSet(result);
    }

    /**
     * Test of getJDBCMajorVersion method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getJDBCMajorVersion()")
    public void testGetJDBCMajorVersion() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.jdbc.major.version", defaultValueFor().getJDBCMajorVersion());
        int result = dbmd.getJDBCMajorVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getJDBCMinorVersion method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getJDBCMinorVersion()")
    public void testGetJDBCMinorVersion() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.jdbc.minor.version", defaultValueFor().getJDBCMinorVersion());
        int result = dbmd.getJDBCMinorVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxBinaryLiteralLength method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxBinaryLiteralLength()")
    public void testGetMaxBinaryLiteralLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.binary.literal.length", defaultValueFor().getMaxBinaryLiteralLength());
        int result = dbmd.getMaxBinaryLiteralLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxCatalogNameLength method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxCatalogNameLength()")
    public void testGetMaxCatalogNameLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.catalog.name.length", defaultValueFor().getMaxCatalogNameLength());
        int result = dbmd.getMaxCatalogNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxCharLiteralLength method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxCharLiteralLength()")
    public void testGetMaxCharLiteralLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.char.literal.length", defaultValueFor().getMaxCharLiteralLength());
        int result = dbmd.getMaxCharLiteralLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxColumnNameLength method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxColumnNameLength()")
    public void testGetMaxColumnNameLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.column.name.length", defaultValueFor().getMaxColumnNameLength()); // 128
        int result = dbmd.getMaxColumnNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxColumnsInGroupBy method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxColumnsInGroupBy()")
    public void testGetMaxColumnsInGroupBy() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.columns.in.group.by", defaultValueFor().getMaxColumnsInGroupBy()); // 0
        int result = dbmd.getMaxColumnsInGroupBy();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxColumnsInIndex method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxColumnsInIndex()")
    public void testGetMaxColumnsInIndex() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.columns.in.index", defaultValueFor().getMaxColumnsInIndex()); // 0
        int result = dbmd.getMaxColumnsInIndex();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxColumnsInOrderBy method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxColumnsInOrderBy()")
    public void testGetMaxColumnsInOrderBy() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.columns.in.order.by", defaultValueFor().getMaxColumnsInOrderBy()); // 0
        int result = dbmd.getMaxColumnsInOrderBy();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxColumnsInSelect method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxColumnsInSelect()")
    public void testGetMaxColumnsInSelect() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.columns.in.select", defaultValueFor().getMaxColumnsInSelect()); // 0
        int result = dbmd.getMaxColumnsInSelect();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxColumnsInTable method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxColumnsInTable()")
    public void testGetMaxColumnsInTable() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.columns.in.table", defaultValueFor().getMaxColumnsInTable()); // 0
        int result = dbmd.getMaxColumnsInTable();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxConnections method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxConnections()")
    public void testGetMaxConnections() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.connections", defaultValueFor().getMaxConnections()); // 0
        int result = dbmd.getMaxConnections();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxCursorNameLength method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxCursorNameLength()")
    public void testGetMaxCursorNameLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.cursor.name.length", defaultValueFor().getMaxCursorNameLength());
        int result = dbmd.getMaxCursorNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxIndexLength method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxIndexLength()")
    public void testGetMaxIndexLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.index.length", defaultValueFor().getMaxIndexLength()); // 0
        int result = dbmd.getMaxIndexLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxProcedureNameLength method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxProcedureNameLength()")
    public void testGetMaxProcedureNameLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.procedure.name.length", defaultValueFor().getMaxProcedureNameLength());
        int result = dbmd.getMaxProcedureNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxRowSize method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxRowSize()")
    public void testGetMaxRowSize() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.row.size", defaultValueFor().getMaxRowSize()); // 0
        int result = dbmd.getMaxRowSize();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxSchemaNameLength method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxSchemaNameLength()")
    public void testGetMaxSchemaNameLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.schema.name.length", defaultValueFor().getMaxSchemaNameLength());
        int result = dbmd.getMaxSchemaNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxStatementLength method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxStatementLength()")
    public void testGetMaxStatementLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.statement.length", defaultValueFor().getMaxStatementLength()); // 0
        int result = dbmd.getMaxStatementLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxStatements method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxStatements()")
    public void testGetMaxStatements() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.statements", defaultValueFor().getMaxStatements()); // 0
        int result = dbmd.getMaxStatements();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxTableNameLength method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxTableNameLength()")
    public void testGetMaxTableNameLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.table.name.length", defaultValueFor().getMaxTableNameLength());
        int result = dbmd.getMaxTableNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxTablesInSelect method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxTablesInSelect()")
    public void testGetMaxTablesInSelect() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.tables.in.select", defaultValueFor().getMaxTablesInSelect()); // 0
        int result = dbmd.getMaxTablesInSelect();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxUserNameLength method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getMaxUserNameLength()")
    public void testGetMaxUserNameLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.max.user.name.length", defaultValueFor().getMaxUserNameLength());
        int result = dbmd.getMaxUserNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getNumericFunctions method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getNumericFunctions()")
    public void testGetNumericFunctions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getProperty("dbmd.numeric.functions", defaultValueFor().getNumericFunctions());
        String result = null;
        try {
            result = dbmd.getNumericFunctions();
        } catch (Exception e) {
            fail(e.toString());
        }
        if (expResult != null) {
            assertEquals(expResult, result);
        } else {
            assertNull(result);
        }
    }

    /**
     * Test of getPrimaryKeys method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getPrimaryKeys(java.lang.String,java.lang.String,java.lang.String)")
    public void testGetPrimaryKeys() throws Exception {
        String catalog = null;
        String schema = null;
        String table = "%";
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getPrimaryKeys(catalog, schema, table);
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getProcedureColumns method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getProcedureColumns(java.lang.String,java.lang.String,java.lang.String,java.lang.String)")
    public void testGetProcedureColumns() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        String procedureNamePattern = "%";
        String columnNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getProcedureTerm method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getProcedureTerm()")
    public void testGetProcedureTerm() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getProperty("dbmd.procedure.term", defaultValueFor().getProcedureTerm());
        String result = dbmd.getProcedureTerm();
        assertEquals(expResult, result);
    }

    /**
     * Test of getProcedures method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getProcedures(java.lang.String,java.lang.String,java.lang.String)")
    public void testGetProcedures() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        String procedureNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getProcedures(catalog, schemaPattern, procedureNamePattern);
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getResultSetHoldability method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsResultSetHoldability(int)")
    public void testGetResultSetHoldability() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.result.set.holdability", defaultValueFor().getResultSetHoldability());
        int result = dbmd.getResultSetHoldability();
        assertEquals(expResult, result);
    }

    /**
     * Test of getRowIdLifetime method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getRowIdLifetime()")
    public void testGetRowIdLifetime() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        RowIdLifetime expResult = RowIdLifetime.valueOf(getProperty("dbmd.get.row.id.lifetime", defaultValueFor().getRowIdLifetime().name())); // ROWID_UNSUPPORTED;
        RowIdLifetime result = dbmd.getRowIdLifetime();
        assertEquals(expResult, result);
    }

    /**
     * Test of getSQLKeywords method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getSQLKeywords()")
    public void testGetSQLKeywords() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getProperty("dbmd.sql.keywords", defaultValueFor().getSQLKeywords());
        String result = null;
        try {
            result = dbmd.getSQLKeywords();
        } catch (Exception e) {
            fail(e.toString());
        }
        if (expResult != null) {
            assertEquals(expResult, result);
        } else {
            assertNull(result);
        }
    }

    /**
     * Test of getSQLStateType method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getSQLStateType()")
    public void testGetSQLStateType() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        int expResult = getIntProperty("dbmd.sql.state.type", defaultValueFor().getSQLStateType()); // DatabaseMetaData.sqlStateSQL99
        int result = dbmd.getSQLStateType();
        assertEquals(expResult, result);
    }

    /**
     * Test of getSchemaTerm method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getSchemaTerm()")
    public void testGetSchemaTerm() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getProperty("dbmd.schema.term", defaultValueFor().getSchemaTerm());
        String result = dbmd.getSchemaTerm();
        assertEquals(expResult, result);
    }

    /**
     * Test of getSchemas() method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getSchemas()")
    public void testGetSchemas_0args() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getSchemas();
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getSchemas(java.lang.String,java.lang.String) method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getSchemas(java.lang.String,java.lang.String)")
    public void testGetSchemas_2args() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getSchemas(catalog, schemaPattern);
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getSearchStringEscape method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getSearchStringEscape()")
    public void testGetSearchStringEscape() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getProperty("dbmd.search.string.escape", defaultValueFor().getSearchStringEscape());
        String result = dbmd.getSearchStringEscape();
        assertEquals(expResult, result);
    }

    /**
     * Test of getStringFunctions method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getStringFunctions()")
    public void testGetStringFunctions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getProperty("dbmd.string.functions", defaultValueFor().getStringFunctions());
        String result = null;
        try {
            result = dbmd.getStringFunctions();
        } catch (Exception e) {
            fail(e.toString());
        }
        if (expResult != null) {
            assertEquals(expResult, result);
        } else {
            assertNull(result);
        }
    }

    /**
     * Test of getSuperTables method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getSuperTables(java.lang.String,java.lang.String,java.lang.String)")
    public void testGetSuperTables() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        String tableNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getSuperTables(catalog, schemaPattern, tableNamePattern);
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getSuperTypes method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getSuperTypes(java.lang.String,java.lang.String,java.lang.String)")
    public void testGetSuperTypes() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        String typeNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getSuperTypes(catalog, schemaPattern, typeNamePattern);
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getSystemFunctions method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getSystemFunctions()")
    public void testGetSystemFunctions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getProperty("dbmd.system.functions", defaultValueFor().getSystemFunctions());
        String result = null;
        try {
            result = dbmd.getSystemFunctions();
        } catch (Exception e) {
            fail(e.toString());
        }
        if (expResult != null) {
            assertEquals(expResult, result);
        } else {
            assertNull(result);
        }
    }

    /**
     * Test of getTablePrivileges method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getTablePrivileges(java.lang.String,java.lang.String,java.lang.String)")
    public void testGetTablePrivileges() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        String tableNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getTablePrivileges(catalog, schemaPattern, tableNamePattern);
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getTableTypes method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getTableTypes()")
    public void testGetTableTypes() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getTableTypes();
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getTables method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getTables(java.lang.String,java.lang.String,java.lang.String,java.lang.String[])")
    public void testGetTables() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        String tableNamePattern = "%";
        String[] types = null;
        DatabaseMetaData dbmd = getMetaData();
        final ResultSet result = dbmd.getTables(catalog, schemaPattern, tableNamePattern, types);
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getTimeDateFunctions method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getTimeDateFunctions()")
    public void testGetTimeDateFunctions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getProperty("dbmd.timedate.functions", defaultValueFor().getTimeDateFunctions());
        String result = null;
        try {
            result = dbmd.getTimeDateFunctions();
        } catch (Exception e) {
            fail(e.toString());
        }
        if (expResult != null) {
            assertEquals(expResult, result);
        } else {
            assertNull(result);
        }
    }

    /**
     * Test of getTypeInfo method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getTypeInfo()")
    public void testGetTypeInfo() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getTypeInfo();
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getUDTs method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getUDTs(java.lang.String,java.lang.String,java.lang.String,int[])")
    public void testGetUDTs() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        String typeNamePattern = "%";
        int[] types = null;
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getUDTs(catalog, schemaPattern, typeNamePattern, types);
        long rowCount = readResultSet(result);
    }

    /**
     * Test of getURL method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getURL()")
    public void testGetURL() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getUrl();
        String result = dbmd.getURL();
        assertEquals(expResult, result);
    }

    /**
     * Test of getUserName method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getUserName()")
    public void testGetUserName() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getUser();
        String result = dbmd.getUserName();
        assertEquals(expResult, result);
    }

    /**
     * Test of getVersionColumns method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "getVersionColumns(java.lang.String,java.lang.String,java.lang.String)")
    public void testGetVersionColumns() throws Exception {
        String catalog = null;
        String schema = null;
        String table = "%";
        DatabaseMetaData dbmd = getMetaData();
        ResultSet result = dbmd.getVersionColumns(catalog, schema, table);
        long rowCount = readResultSet(result);
    }

    /**
     * Test of insertsAreDetected method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "insertsAreDetected()")
    public void testInsertsAreDetected() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        //--
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        boolean expResult = getBooleanProperty("dbmd.inserts.are.detected.type.forward.only", defaultValueFor().insertsAreDetected(type)); // false
        boolean result = dbmd.insertsAreDetected(type);
        assertEquals("InsertsAreDetected - TYPE_FORWARD_ONLY", expResult, result);
        //--
        type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
        expResult = getBooleanProperty("dbmd.inserts.are.detected.type.scroll.insensitive", defaultValueFor().insertsAreDetected(type)); // false
        result = dbmd.insertsAreDetected(type);
        assertEquals("InsertsAreDetected - TYPE_SCROLL_INSENSITIVE", expResult, result);
        //--
        type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;
        expResult = getBooleanProperty("dbmd.inserts.are.detected.type.scroll.sensitive", defaultValueFor().insertsAreDetected(type)); // false
        result = dbmd.insertsAreDetected(type);
        assertEquals("InsertsAreDetected - TYPE_SCROLL_SENSITIVE", expResult, result);
    }

    /**
     * Test of isCatalogAtStart method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "isCatalogAtStart()")
    public void testIsCatalogAtStart() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.is.catalog.at.start", defaultValueFor().isCatalogAtStart());
        boolean result = dbmd.isCatalogAtStart();
        assertEquals(expResult, result);
    }

    /**
     * Test of isReadOnly method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "isReadOnly()")
    public void testIsReadOnly() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        // TODO:  set for and test both cases.
        boolean expResult = getBooleanProperty("dbmd.is.readonly", false);
        boolean result = dbmd.isReadOnly();
        assertEquals(expResult, result);
    }

    /**
     * Test of isWrapperFor method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "isWrapperFor(Class<?>)")
    public void testIsWrapperFor() throws Exception {
        Class<Object> iface = Object.class;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = true;
        boolean result = dbmd.isWrapperFor(iface);
        assertEquals(expResult, result);
    }

    /**
     * Test of locatorsUpdateCopy method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "locatorsUpdateCopy()")
    public void testLocatorsUpdateCopy() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.locators.update.copy", defaultValueFor().locatorsUpdateCopy()); // false
        boolean result = dbmd.locatorsUpdateCopy();
        assertEquals(expResult, result);
    }

    /**
     * Test of nullPlusNonNullIsNull method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "nullPlusNonNullIsNull()")
    public void testNullPlusNonNullIsNull() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.null.plus.non.null.is.null", defaultValueFor().nullPlusNonNullIsNull()); // true
        boolean result = dbmd.nullPlusNonNullIsNull();
        assertEquals(expResult, result);
    }

    /**
     * Test of nullsAreSortedAtEnd method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "nullsAreSortedAtEnd()")
    public void testNullsAreSortedAtEnd() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.nulls.are.sorted.at.end", defaultValueFor().nullsAreSortedAtEnd()); // false
        boolean result = dbmd.nullsAreSortedAtEnd();
        assertEquals(expResult, result);
    }

    /**
     * Test of nullsAreSortedAtStart method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "nullsAreSortedAtStart()")
    public void testNullsAreSortedAtStart() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.nulls.are.sorted.at.start", defaultValueFor().nullsAreSortedAtStart()); // true
        boolean result = dbmd.nullsAreSortedAtStart();
        assertEquals(expResult, result);
    }

    /**
     * Test of nullsAreSortedHigh method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "nullsAreSortedHigh()")
    public void testNullsAreSortedHigh() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.nulls.are.sorted.high", defaultValueFor().nullsAreSortedHigh()); // false
        boolean result = dbmd.nullsAreSortedHigh();
        assertEquals(expResult, result);
    }

    /**
     * Test of nullsAreSortedLow method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "(nullsAreSortedLow)")
    public void testNullsAreSortedLow() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.nulls.are.sorted.low", defaultValueFor().nullsAreSortedLow()); // false;
        boolean result = dbmd.nullsAreSortedLow();
        assertEquals(expResult, result);
    }

    /**
     * Test of othersDeletesAreVisible method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "othersDeletesAreVisible()")
    public void testOthersDeletesAreVisible() throws Exception {
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.others.deletes.are.visible.type.forward.only", defaultValueFor().othersDeletesAreVisible(type)); // false
        boolean result = dbmd.othersDeletesAreVisible(type);
        assertEquals("OthersDeletesAreVisible - TYPE_FORWARD_ONLY", expResult, result);
        type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
        expResult = getBooleanProperty("dbmd.others.deletes.are.visible.type.scroll.insensitive", defaultValueFor().othersDeletesAreVisible(type)); // false
        result = dbmd.othersDeletesAreVisible(type);
        assertEquals("OthersDeletesAreVisible - TYPE_SCROLL_INSENSITIVE", expResult, result);
        type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;
        expResult = getBooleanProperty("dbmd.others.deletes.are.visible.type.scroll.sensitive", defaultValueFor().othersDeletesAreVisible(type)); // false
        result = dbmd.othersDeletesAreVisible(type);
        assertEquals("OthersDeletesAreVisible - TYPE_SCROLL_SENSITIVE", expResult, result);
    }

    /**
     * Test of othersInsertsAreVisible method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "othersInsertsAreVisible()")
    public void testOthersInsertsAreVisible() throws Exception {
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.others.inserts.are.visible.type.forward.only", defaultValueFor().othersInsertsAreVisible(type)); // false
        boolean result = dbmd.othersInsertsAreVisible(type);
        assertEquals("OthersInsertsAreVisible - TYPE_FORWARD_ONLY", expResult, result);
        type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
        expResult = getBooleanProperty("dbmd.others.inserts.are.visible.type.scroll.insensitive", defaultValueFor().othersInsertsAreVisible(type)); // false
        result = dbmd.othersInsertsAreVisible(type);
        assertEquals("OthersInsertsAreVisible - TYPE_SCROLL_INSENSITIVE", expResult, result);
        type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;
        expResult = getBooleanProperty("dbmd.others.inserts.are.visible.type.scroll.sensitive", defaultValueFor().othersInsertsAreVisible(type)); // false
        result = dbmd.othersInsertsAreVisible(type);
        assertEquals("OthersInsertsAreVisible - TYPE_SCROLL_SENSITIVE", expResult, result);
    }

    /**
     * Test of othersUpdatesAreVisible method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "othersUpdatesAreVisible()")
    public void testOthersUpdatesAreVisible() throws Exception {
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.others.updates.are.visible.type.forward.only", defaultValueFor().othersUpdatesAreVisible(type)); // false
        boolean result = dbmd.othersUpdatesAreVisible(type);
        assertEquals("OthersUpdatesAreVisible - TYPE_FORWARD_ONLY", expResult, result);
        type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
        expResult = getBooleanProperty("dbmd.others.updates.are.visible.type.scroll.insensitive", defaultValueFor().othersUpdatesAreVisible(type)); // false
        result = dbmd.othersUpdatesAreVisible(type);
        assertEquals("OthersUpdatesAreVisible - TYPE_SCROLL_INSENSITIVE", expResult, result);
        type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;
        expResult = getBooleanProperty("dbmd.others.updates.are.visible.type.scroll.sensitive", defaultValueFor().othersUpdatesAreVisible(type)); // false
        result = dbmd.othersUpdatesAreVisible(type);
        assertEquals("OthersUpdatesAreVisible - TYPE_SCROLL_SENSITIVE", expResult, result);
    }

    /**
     * Test of ownDeletesAreVisible method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "ownDeletesAreVisible()")
    public void testOwnDeletesAreVisible() throws Exception {
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.own.deletes.are.visible.type.forward.only", defaultValueFor().ownDeletesAreVisible(type)); // false
        boolean result = dbmd.ownDeletesAreVisible(type);
        assertEquals("OwnDeletesAreVisible - TYPE_FORWARD_ONLY", expResult, result);
        type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
        expResult = getBooleanProperty("dbmd.own.deletes.are.visible.type.scroll.insensitive", defaultValueFor().ownDeletesAreVisible(type)); // false
        result = dbmd.ownDeletesAreVisible(type);
        assertEquals("OwnDeletesAreVisible - TYPE_SCROLL_INSENSITIVE", expResult, result);
        type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;
        expResult = getBooleanProperty("dbmd.own.deletes.are.visible.type.scroll.sensitive", defaultValueFor().ownDeletesAreVisible(type)); // false
        result = dbmd.ownDeletesAreVisible(type);
        assertEquals("OwnDeletesAreVisible - TYPE_SCROLL_SENSITIVE", expResult, result);
    }

    /**
     * Test of ownInsertsAreVisible method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "ownInsertsAreVisible()")
    public void testOwnInsertsAreVisible() throws Exception {
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.own.inserts.are.visible.type.forward.only", defaultValueFor().ownInsertsAreVisible(type)); // false
        boolean result = dbmd.ownInsertsAreVisible(type);
        assertEquals("OwnInsertsAreVisible - TYPE_FORWARD_ONLY", expResult, result);
        type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
        expResult = getBooleanProperty("dbmd.own.inserts.are.visible.type.scroll.insensitive", defaultValueFor().ownInsertsAreVisible(type)); // false
        result = dbmd.ownInsertsAreVisible(type);
        assertEquals("OwnInsertsAreVisible - TYPE_SCROLL_INSENSITIVE", expResult, result);
        type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;
        expResult = getBooleanProperty("dbmd.own.inserts.are.visible.type.scroll.sensitive", defaultValueFor().ownInsertsAreVisible(type)); // false
        result = dbmd.ownInsertsAreVisible(type);
        assertEquals("OwnInsertsAreVisible - TYPE_SCROLL_SENSITIVE", expResult, result);
    }

    /**
     * Test of ownUpdatesAreVisible method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "ownUpdatesAreVisible()")
    public void testOwnUpdatesAreVisible() throws Exception {
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.own.updates.are.visible.type.forward.only", defaultValueFor().ownUpdatesAreVisible(type)); // false
        boolean result = dbmd.ownUpdatesAreVisible(type);
        assertEquals("OwnUpdatesAreVisible - TYPE_FORWARD_ONLY", expResult, result);
        type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
        expResult = getBooleanProperty("dbmd.own.updates.are.visible.type.scroll.insensitive", defaultValueFor().ownUpdatesAreVisible(type)); // false
        result = dbmd.ownUpdatesAreVisible(type);
        assertEquals("OwnUpdatesAreVisible - TYPE_SCROLL_INSENSITIVE", expResult, result);
        type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;
        expResult = getBooleanProperty("dbmd.own.updates.are.visible.type.scroll.sensitive", defaultValueFor().ownUpdatesAreVisible(type)); // false
        result = dbmd.ownUpdatesAreVisible(type);
        assertEquals("OwnUpdatesAreVisible - TYPE_SCROLL_SENSITIVE", expResult, result);
    }

    /**
     * Test of storesLowerCaseIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "storesLowerCaseIdentifiers()")
    public void testStoresLowerCaseIdentifiers() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.stores.lower.case.identifiers", defaultValueFor().storesLowerCaseIdentifiers()); // false
        boolean result = dbmd.storesLowerCaseIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of storesLowerCaseQuotedIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "storesLowerCaseQuotedIdentifiers()")
    public void testStoresLowerCaseQuotedIdentifiers() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.stores.lower.case.quoted.identifiers", defaultValueFor().storesLowerCaseQuotedIdentifiers()); // false
        boolean result = dbmd.storesLowerCaseQuotedIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of storesMixedCaseIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "storesMixedCaseIdentifiers()")
    public void testStoresMixedCaseIdentifiers() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.stores.mixed.case.identifiers", defaultValueFor().storesMixedCaseIdentifiers()); // false
        boolean result = dbmd.storesMixedCaseIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of storesMixedCaseQuotedIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "storesMixedCaseQuotedIdentifiers")
    public void testStoresMixedCaseQuotedIdentifiers() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.stores.mixed.case.quoted.identifiers", defaultValueFor().storesMixedCaseQuotedIdentifiers()); // false
        boolean result = dbmd.storesMixedCaseQuotedIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of storesUpperCaseIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "storesUpperCaseIdentifiers()")
    public void testStoresUpperCaseIdentifiers() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.stores.upper.case.identifiers", defaultValueFor().storesUpperCaseIdentifiers()); // true
        boolean result = dbmd.storesUpperCaseIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of storesUpperCaseQuotedIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "storesUpperCaseQuotedIdentifiers()")
    public void testStoresUpperCaseQuotedIdentifiers() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.stores.uppper.case.quoted.identifiers", defaultValueFor().storesUpperCaseQuotedIdentifiers()); // false
        boolean result = dbmd.storesUpperCaseQuotedIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsANSI92EntryLevelSQL method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsANSI92EntryLevelSQL()")
    public void testSupportsANSI92EntryLevelSQL() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.ansi92.entry.level.sql", defaultValueFor().supportsANSI92EntryLevelSQL());
        boolean result = dbmd.supportsANSI92EntryLevelSQL();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsANSI92FullSQL method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsANSI92FullSQL()")
    public void testSupportsANSI92FullSQL() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.ansi92.full.sql", defaultValueFor().supportsANSI92FullSQL());
        boolean result = dbmd.supportsANSI92FullSQL();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsANSI92IntermediateSQL method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsANSI92IntermediateSQL()")
    public void testSupportsANSI92IntermediateSQL() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.ansi92.intermediate.sql", defaultValueFor().supportsANSI92IntermediateSQL());
        boolean result = dbmd.supportsANSI92IntermediateSQL();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsAlterTableWithAddColumn method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsAlterTableWithAddColumn()")
    public void testSupportsAlterTableWithAddColumn() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.alter.table.with.add.column", defaultValueFor().supportsAlterTableWithAddColumn()); // true
        boolean result = dbmd.supportsAlterTableWithAddColumn();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsAlterTableWithDropColumn method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsAlterTableWithDropColumn()")
    public void testSupportsAlterTableWithDropColumn() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.alter.table.with.drop.column", defaultValueFor().supportsAlterTableWithDropColumn()); // true
        boolean result = dbmd.supportsAlterTableWithDropColumn();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsBatchUpdates method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsBatchUpdates()")
    public void testSupportsBatchUpdates() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.batch.updates", defaultValueFor().supportsBatchUpdates()); // true
        boolean result = dbmd.supportsBatchUpdates();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsCatalogsInDataManipulation method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsCatalogsInDataManipulation()")
    public void testSupportsCatalogsInDataManipulation() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.catalogs.in.data.manipulation", defaultValueFor().supportsCatalogsInDataManipulation());
        boolean result = dbmd.supportsCatalogsInDataManipulation();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsCatalogsInIndexDefinitions method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsCatalogsInIndexDefinitions()")
    public void testSupportsCatalogsInIndexDefinitions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.catalogs.in.index.definitions", defaultValueFor().supportsCatalogsInIndexDefinitions());
        boolean result = dbmd.supportsCatalogsInIndexDefinitions();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsCatalogsInPrivilegeDefinitions method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsCatalogsInPrivilegeDefinitions()")
    public void testSupportsCatalogsInPrivilegeDefinitions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.catalogs.in.privilege.definitions", defaultValueFor().supportsCatalogsInPrivilegeDefinitions());
        boolean result = dbmd.supportsCatalogsInPrivilegeDefinitions();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsCatalogsInProcedureCalls method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsCatalogsInProcedureCalls()")
    public void testSupportsCatalogsInProcedureCalls() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.catalogs.in.procedure.calls", defaultValueFor().supportsCatalogsInProcedureCalls());
        boolean result = dbmd.supportsCatalogsInProcedureCalls();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsCatalogsInTableDefinitions method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsCatalogsInTableDefinitions()")
    public void testSupportsCatalogsInTableDefinitions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.catalogs.in.table.definitions", defaultValueFor().supportsCatalogsInTableDefinitions());
        boolean result = dbmd.supportsCatalogsInTableDefinitions();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsColumnAliasing method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsColumnAliasing()")
    public void testSupportsColumnAliasing() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.column.aliasing", defaultValueFor().supportsColumnAliasing()); // true
        boolean result = dbmd.supportsColumnAliasing();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsConvert method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsConvert()")
    public void testSupportsConvert() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.convert", defaultValueFor().supportsConvert()); // true
        boolean result = dbmd.supportsConvert();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsCoreSQLGrammar method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsCoreSQLGrammar()")
    public void testSupportsCoreSQLGrammar() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.core.sql.grammar", defaultValueFor().supportsCoreSQLGrammar());
        boolean result = dbmd.supportsCoreSQLGrammar();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsCorrelatedSubqueries method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsCorrelatedSubqueries()")
    public void testSupportsCorrelatedSubqueries() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.correlated.subqueries", defaultValueFor().supportsCorrelatedSubqueries());
        boolean result = dbmd.supportsCorrelatedSubqueries();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsDataDefinitionAndDataManipulationTransactions method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsDataDefinitionAndDataManipulationTransactions()")
    public void testSupportsDataDefinitionAndDataManipulationTransactions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.data.definition.and.data.manipulation.transactions", defaultValueFor().supportsDataDefinitionAndDataManipulationTransactions()); // false
        boolean result = dbmd.supportsDataDefinitionAndDataManipulationTransactions();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsDataManipulationTransactionsOnly method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsDataManipulationTransactionsOnly()")
    public void testSupportsDataManipulationTransactionsOnly() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.data.manipulation.transactions.only", defaultValueFor().supportsDataManipulationTransactionsOnly()); // true
        boolean result = dbmd.supportsDataManipulationTransactionsOnly();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsDifferentTableCorrelationNames method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsDifferentTableCorrelationNames()")
    public void testSupportsDifferentTableCorrelationNames() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.different.table.correlation.names", defaultValueFor().supportsDifferentTableCorrelationNames()); // true
        boolean result = dbmd.supportsDifferentTableCorrelationNames();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsExpressionsInOrderBy method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsExpressionsInOrderBy()")
    public void testSupportsExpressionsInOrderBy() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.expressions.in.order.by", defaultValueFor().supportsExpressionsInOrderBy()); // true
        boolean result = dbmd.supportsExpressionsInOrderBy();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsExtendedSQLGrammar method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsExtendedSQLGrammar()")
    public void testSupportsExtendedSQLGrammar() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.extended.sql.grammar", defaultValueFor().supportsExtendedSQLGrammar());
        boolean result = dbmd.supportsExtendedSQLGrammar();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsFullOuterJoins method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsFullOuterJoins()")
    public void testSupportsFullOuterJoins() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.full.outer.joins", defaultValueFor().supportsFullOuterJoins());
        boolean result = dbmd.supportsFullOuterJoins();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsGetGeneratedKeys method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsGetGeneratedKeys()")
    public void testSupportsGetGeneratedKeys() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.get.generated.keys", defaultValueFor().supportsGetGeneratedKeys()); // true
        boolean result = dbmd.supportsGetGeneratedKeys();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsGroupBy method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsGroupBy()")
    public void testSupportsGroupBy() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.group.by", defaultValueFor().supportsGroupBy()); // true
        boolean result = dbmd.supportsGroupBy();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsGroupByBeyondSelect method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsGroupByBeyondSelect()")
    public void testSupportsGroupByBeyondSelect() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.group.by.beyond.select", defaultValueFor().supportsGroupByBeyondSelect()); // true
        boolean result = dbmd.supportsGroupByBeyondSelect();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsGroupByUnrelated method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsGroupByUnrelated()")
    public void testSupportsGroupByUnrelated() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.group.by.unrelated", defaultValueFor().supportsGroupByUnrelated()); // true
        boolean result = dbmd.supportsGroupByUnrelated();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsIntegrityEnhancementFacility method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsIntegrityEnhancementFacility()")
    public void testSupportsIntegrityEnhancementFacility() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.integrity.enhancement.facility", defaultValueFor().supportsIntegrityEnhancementFacility());
        boolean result = dbmd.supportsIntegrityEnhancementFacility();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsLikeEscapeClause method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsLikeEscapeClause")
    public void testSupportsLikeEscapeClause() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.like.escape.clause", defaultValueFor().supportsLikeEscapeClause()); // true
        boolean result = dbmd.supportsLikeEscapeClause();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsLimitedOuterJoins method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsLimitedOuterJoins()")
    public void testSupportsLimitedOuterJoins() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.limited.outer.joins", true);
        boolean result = dbmd.supportsLimitedOuterJoins();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsMinimumSQLGrammar method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsMinimumSQLGrammar()")
    public void testSupportsMinimumSQLGrammar() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.minimum.sql.grammar", defaultValueFor().supportsMinimumSQLGrammar());
        boolean result = dbmd.supportsMinimumSQLGrammar();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsMixedCaseIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsMixedCaseIdentifiers()")
    public void testSupportsMixedCaseIdentifiers() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.mixed.case.identifiers", defaultValueFor().supportsMixedCaseIdentifiers()); // false
        boolean result = dbmd.supportsMixedCaseIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsMixedCaseQuotedIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsMixedCaseQuotedIdentifiers()")
    public void testSupportsMixedCaseQuotedIdentifiers() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.mixed.case.quoted.identifiers", defaultValueFor().supportsMixedCaseQuotedIdentifiers()); // true
        boolean result = dbmd.supportsMixedCaseQuotedIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsMultipleOpenResults method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsMultipleOpenResults()")
    public void testSupportsMultipleOpenResults() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.multiple.open.results", defaultValueFor().supportsMultipleOpenResults()); // true
        boolean result = dbmd.supportsMultipleOpenResults();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsMultipleResultSets method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsMultipleResultSets")
    public void testSupportsMultipleResultSets() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.multiple.result.sets", defaultValueFor().supportsMultipleResultSets());
        boolean result = dbmd.supportsMultipleResultSets();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsMultipleTransactions method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsMultipleTransactions()")
    public void testSupportsMultipleTransactions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.multiple.transactions", defaultValueFor().supportsMultipleTransactions());
        boolean result = dbmd.supportsMultipleTransactions();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsNamedParameters method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsNamedParameters()")
    public void testSupportsNamedParameters() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.named.parameters", defaultValueFor().supportsNamedParameters());
        boolean result = dbmd.supportsNamedParameters();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsNonNullableColumns method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsNonNullableColumns()")
    public void testSupportsNonNullableColumns() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.non.nullable.columns", defaultValueFor().supportsNonNullableColumns());
        boolean result = dbmd.supportsNonNullableColumns();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsOpenCursorsAcrossCommit method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsOpenCursorsAcrossCommit()")
    public void testSupportsOpenCursorsAcrossCommit() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.open.cursors.across.commit", defaultValueFor().supportsOpenCursorsAcrossCommit());
        boolean result = dbmd.supportsOpenCursorsAcrossCommit();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsOpenCursorsAcrossRollback method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsOpenCursorsAcrossRollback()")
    public void testSupportsOpenCursorsAcrossRollback() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.open.cursors.across.rollback", defaultValueFor().supportsOpenCursorsAcrossRollback());
        boolean result = dbmd.supportsOpenCursorsAcrossRollback();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsOpenStatementsAcrossCommit method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsOpenStatementsAcrossCommit()")
    public void testSupportsOpenStatementsAcrossCommit() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.open.statements.across.commit", defaultValueFor().supportsOpenStatementsAcrossCommit());
        boolean result = dbmd.supportsOpenStatementsAcrossCommit();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsOpenStatementsAcrossRollback method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsOpenStatementsAcrossRollback()")
    public void testSupportsOpenStatementsAcrossRollback() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.open.statements.across.rollback", defaultValueFor().supportsOpenStatementsAcrossRollback());
        boolean result = dbmd.supportsOpenStatementsAcrossRollback();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsOrderByUnrelated method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsOrderByUnrelated()")
    public void testSupportsOrderByUnrelated() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.order.by.related", defaultValueFor().supportsOrderByUnrelated()); // true
        boolean result = dbmd.supportsOrderByUnrelated();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsOuterJoins method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsOuterJoins()")
    public void testSupportsOuterJoins() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.outer.joins", defaultValueFor().supportsOuterJoins());
        boolean result = dbmd.supportsOuterJoins();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsPositionedDelete method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsPositionedDelete()")
    public void testSupportsPositionedDelete() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.positioned.delete", defaultValueFor().supportsPositionedDelete());
        boolean result = dbmd.supportsPositionedDelete();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsPositionedUpdate method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsPositionedUpdate()")
    public void testSupportsPositionedUpdate() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.positioned.update", defaultValueFor().supportsPositionedUpdate());
        boolean result = dbmd.supportsPositionedUpdate();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsResultSetConcurrency method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsResultSetConcurrency(int,int)")
    public void testSupportsResultSetConcurrency_TYPE_FORWARD_ONLY_CONCUR_READ_ONLY() throws Exception {
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        int concurrency = JDBCResultSet.CONCUR_READ_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.result.set.concurrency.forward.only.read.only", defaultValueFor().supportsResultSetConcurrency(type, concurrency)); // true
        boolean result = dbmd.supportsResultSetConcurrency(type, concurrency);
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsResultSetConcurrency method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsResultSetConcurrency(int,int)")
    public void testSupportsResultSetConcurrency_TYPE_FORWARD_ONLY_CONCUR_UPDATABLE() throws Exception {
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        int concurrency = JDBCResultSet.CONCUR_UPDATABLE;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.result.set.concurrency.forward.only.updatable", defaultValueFor().supportsResultSetConcurrency(type, concurrency)); // true
        boolean result = dbmd.supportsResultSetConcurrency(type, concurrency);
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsResultSetConcurrency method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsResultSetConcurrency(int,int)")
    public void testSupportsResultSetConcurrency_TYPE_SCROLL_INSENSITIVE_CONCUR_READ_ONLY() throws Exception {
        int type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
        int concurrency = JDBCResultSet.CONCUR_READ_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.result.set.concurrency.scroll.insensitive.read.only", defaultValueFor().supportsResultSetConcurrency(type, concurrency)); // true
        boolean result = dbmd.supportsResultSetConcurrency(type, concurrency);
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsResultSetConcurrency method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsResultSetConcurrency(int,int)")
    public void testSupportsResultSetConcurrency_TYPE_SCROLL_INSENSITIVE_CONCUR_UPDATABLE() throws Exception {
        int type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
        int concurrency = JDBCResultSet.CONCUR_UPDATABLE;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.result.set.concurrency.scroll.insensitive.updatable", defaultValueFor().supportsResultSetConcurrency(type, concurrency)); // true
        boolean result = dbmd.supportsResultSetConcurrency(type, concurrency);
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsResultSetConcurrency method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsResultSetConcurrency(int,int)")
    public void testSupportsResultSetConcurrency_TYPE_SCROLL_SENSITIVE_CONCUR_READ_ONLY() throws Exception {
        int type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;
        int concurrency = JDBCResultSet.CONCUR_READ_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.result.set.concurrency.scroll.sensitive.read.only", defaultValueFor().supportsResultSetConcurrency(type, concurrency)); // true
        boolean result = dbmd.supportsResultSetConcurrency(type, concurrency);
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsResultSetConcurrency method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsResultSetConcurrency(int,int)")
    public void testSupportsResultSetConcurrency_TYPE_SCROLL_SENSITIVE_CONCUR_UPDATABLE() throws Exception {
        int type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;
        int concurrency = JDBCResultSet.CONCUR_UPDATABLE;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.result.set.concurrency.scroll.sensitive.updatable", defaultValueFor().supportsResultSetConcurrency(type, concurrency)); // true
        boolean result = dbmd.supportsResultSetConcurrency(type, concurrency);
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsResultSetHoldability method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsResultSetHoldability(int)")
    public void testSupportsResultSetHoldability_CLOSE_CURSORS_AT_COMMIT() throws Exception {
        int holdability = JDBCResultSet.CLOSE_CURSORS_AT_COMMIT;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.result.set.holdability.close.cursors.at.commit", defaultValueFor().supportsResultSetHoldability(holdability)); // true
        boolean result = dbmd.supportsResultSetHoldability(holdability);
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsResultSetHoldability method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsResultSetHoldability(int)")
    public void testSupportsResultSetHoldability_HOLD_CURSORS_OVER_COMMIT() throws Exception {
        int holdability = JDBCResultSet.HOLD_CURSORS_OVER_COMMIT;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.result.set.holdability.hold.cursors.over.commit", defaultValueFor().supportsResultSetHoldability(holdability)); // true
        boolean result = dbmd.supportsResultSetHoldability(holdability);
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsResultSetType method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsResultSetType(int)")
    public void testSupportsResultSetType_TYPE_FORWARD_ONLY() throws Exception {
        int type = ResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.result.set.type.forward.only", defaultValueFor().supportsResultSetType(type)); // true
        boolean result = dbmd.supportsResultSetType(type);
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsResultSetType method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsResultSetType(int)")
    public void testSupportsResultSetType_TYPE_SCROLL_INSENSITIVE() throws Exception {
        int type = ResultSet.TYPE_SCROLL_INSENSITIVE;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.result.set.type.scroll.insensitive", defaultValueFor().supportsResultSetType(type)); // true
        boolean result = dbmd.supportsResultSetType(type);
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsResultSetType method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsResultSetType(int)")
    public void testSupportsResultSetType_TYPE_SCROLL_SENSITIVE() throws Exception {
        int type = ResultSet.TYPE_SCROLL_SENSITIVE;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.result.set.type.scroll.sensitive", defaultValueFor().supportsResultSetType(type)); // true
        boolean result = dbmd.supportsResultSetType(type);
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsSavepoints method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsSavepoints()")
    public void testSupportsSavepoints() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.savepoints", defaultValueFor().supportsSavepoints()); // true
        boolean result = dbmd.supportsSavepoints();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsSchemasInDataManipulation method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsSchemasInDataManipulation()")
    public void testSupportsSchemasInDataManipulation() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.schemas.in.data.manipulation", defaultValueFor().supportsSchemasInDataManipulation());
        boolean result = dbmd.supportsSchemasInDataManipulation();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsSchemasInIndexDefinitions method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsSchemasInIndexDefinitions()")
    public void testSupportsSchemasInIndexDefinitions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.schemas.in.index.definitions", defaultValueFor().supportsSchemasInIndexDefinitions());
        boolean result = dbmd.supportsSchemasInIndexDefinitions();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsSchemasInPrivilegeDefinitions method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsSchemasInPrivilegeDefinitions()")
    public void testSupportsSchemasInPrivilegeDefinitions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.schemas.in.privilege.definitions", defaultValueFor().supportsSchemasInPrivilegeDefinitions());
        boolean result = dbmd.supportsSchemasInPrivilegeDefinitions();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsSchemasInProcedureCalls method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsSchemasInProcedureCalls()")
    public void testSupportsSchemasInProcedureCalls() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.schemas.in.procedure.calls", defaultValueFor().supportsSchemasInProcedureCalls());
        boolean result = dbmd.supportsSchemasInProcedureCalls();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsSchemasInTableDefinitions method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsSchemasInTableDefinitions()")
    public void testSupportsSchemasInTableDefinitions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.schemas.in.table.definitions", defaultValueFor().supportsSchemasInTableDefinitions());
        boolean result = dbmd.supportsSchemasInTableDefinitions();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsSelectForUpdate method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsSelectForUpdate()")
    public void testSupportsSelectForUpdate() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.select.for.update", defaultValueFor().supportsSelectForUpdate());
        boolean result = dbmd.supportsSelectForUpdate();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsStatementPooling method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsStatementPooling()")
    public void testSupportsStatementPooling() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.statement.pooling", defaultValueFor().supportsStatementPooling()); // true
        boolean result = dbmd.supportsStatementPooling();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsStoredFunctionsUsingCallSyntax method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsStoredFunctionsUsingCallSyntax()")
    public void testSupportsStoredFunctionsUsingCallSyntax() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.stored.functions.using.call.syntax", defaultValueFor().supportsStoredFunctionsUsingCallSyntax()); // true
        boolean result = dbmd.supportsStoredFunctionsUsingCallSyntax();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsStoredProcedures method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsStoredProcedures()")
    public void testSupportsStoredProcedures() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.stored.procedures", defaultValueFor().supportsStoredProcedures());
        boolean result = dbmd.supportsStoredProcedures();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsSubqueriesInComparisons method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsSubqueriesInComparisons()")
    public void testSupportsSubqueriesInComparisons() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.subqueries.in.comparisons", defaultValueFor().supportsSubqueriesInComparisons());
        boolean result = dbmd.supportsSubqueriesInComparisons();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsSubqueriesInExists method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsSubqueriesInExists()")
    public void testSupportsSubqueriesInExists() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.subqueries.in.exists", defaultValueFor().supportsSubqueriesInExists());
        boolean result = dbmd.supportsSubqueriesInExists();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsSubqueriesInIns method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsSubqueriesInIns()")
    public void testSupportsSubqueriesInIns() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.subqueries.in.ins", defaultValueFor().supportsSubqueriesInIns());
        boolean result = dbmd.supportsSubqueriesInIns();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsSubqueriesInQuantifieds method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsSubqueriesInQuantifieds()")
    public void testSupportsSubqueriesInQuantifieds() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.subqueries.in.quantifieds", defaultValueFor().supportsSubqueriesInQuantifieds());
        boolean result = dbmd.supportsSubqueriesInQuantifieds();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsTableCorrelationNames method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsTableCorrelationNames()")
    public void testSupportsTableCorrelationNames() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.table.correlation.names", defaultValueFor().supportsTableCorrelationNames()); // true
        boolean result = dbmd.supportsTableCorrelationNames();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsTransactionIsolationLevel method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsTransactionIsolationLevel(int)")
    public void testSupportsTransactionIsolationLevel() throws Exception {
        int level = Connection.TRANSACTION_READ_UNCOMMITTED;
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.transaction.isolation.level.read.uncommited", defaultValueFor().supportsTransactionIsolationLevel(level));
        boolean result = dbmd.supportsTransactionIsolationLevel(level);
        assertEquals("Supports TRANSACTION_READ_UNCOMMITTED", expResult, result);
        level = Connection.TRANSACTION_READ_COMMITTED;
        expResult = getBooleanProperty("dbmd.supports.transaction.isolation.level.read.commited", defaultValueFor().supportsTransactionIsolationLevel(level));
        result = dbmd.supportsTransactionIsolationLevel(level);
        assertEquals("Supports TRANSACTION_READ_COMMITED", expResult, result);
        level = Connection.TRANSACTION_REPEATABLE_READ;
        expResult = getBooleanProperty("dbmd.supports.transaction.isolation.level.repeatable.read", defaultValueFor().supportsTransactionIsolationLevel(level));
        result = dbmd.supportsTransactionIsolationLevel(level);
        assertEquals("Supports TRANSACTION_REPEATABLE_READ", expResult, result);
        level = Connection.TRANSACTION_SERIALIZABLE;
        expResult = getBooleanProperty("dbmd.supports.transaction.isolation.level.serializable", defaultValueFor().supportsTransactionIsolationLevel(level));
        result = dbmd.supportsTransactionIsolationLevel(level);
        assertEquals("Supports TRANSACTION_SERIALIZABLE", expResult, result);
    }

    /**
     * Test of supportsTransactions method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsTransactions()")
    public void testSupportsTransactions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.transactions", defaultValueFor().supportsTransactions()); // true
        boolean result = dbmd.supportsTransactions();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsUnion method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsUnion()")
    public void testSupportsUnion() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.union", defaultValueFor().supportsUnion());
        boolean result = dbmd.supportsUnion();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsUnionAll method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "supportsUnionAll()")
    public void testSupportsUnionAll() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.supports.union.all", defaultValueFor().supportsUnionAll());
        boolean result = dbmd.supportsUnionAll();
        assertEquals(expResult, result);
    }

    /**
     * Test of providesQueryObjectGenerator method, of interface java.sql.DatabaseMetaData.
     */
    //    public void testProvidesQueryObjectGenerator() throws Exception {
    //        println("providesQueryObjectGenerator");
    //
    //        DatabaseMetaData dbmd = getMetaData();
    //
    //        boolean expResult = true;
    //        boolean result = dbmd.providesQueryObjectGenerator();
    //        assertEquals("TODO:", expResult, result);
    //    }
    /**
     * Test of unwrap method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "unwrap(Class<?>)")
    public void testUnwrap() throws Exception {
        Class<Object> iface = Object.class;
        DatabaseMetaData dbmd = getMetaData();
        Object expResult = dbmd;
        Object result = dbmd.unwrap(iface);
        assertSame(expResult, result);
    }

    /**
     * Test of updatesAreDetected method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "updatesAreDetected()")
    public void testUpdatesAreDetected() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        //--
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        boolean expResult = getBooleanProperty("dbmd.updates.are.detected.type.forward.only", defaultValueFor().updatesAreDetected(type)); // false
        boolean result = dbmd.updatesAreDetected(type);
        assertEquals("UpdatesAreDetected - TYPE_FORWARD_ONLY", expResult, result);
        //--
        type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
        expResult = getBooleanProperty("dbmd.updates.are.detected.type.scroll.insensitive", defaultValueFor().updatesAreDetected(type)); // false
        result = dbmd.updatesAreDetected(type);
        assertEquals("UpdatesAreDetected - TYPE_SCROLL_INSENSITIVE", expResult, result);
        //--
        type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;
        expResult = getBooleanProperty("dbmd.updates.are.detected.type.scroll.sensitive", defaultValueFor().updatesAreDetected(type)); // false
        result = dbmd.updatesAreDetected(type);
        assertEquals("UpdatesAreDetected - TYPE_SCROLL_SENSITIVE", expResult, result);
    }

    /**
     * Test of usesLocalFilePerTable method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "usesLocalFilePerTable()")
    public void testUsesLocalFilePerTable() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.uses.local.file.per.table", defaultValueFor().usesLocalFilePerTable()); // false
        boolean result = dbmd.usesLocalFilePerTable();
        assertEquals(expResult, result);
    }

    /**
     * Test of usesLocalFiles method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod(value = "usesLocalFiles()")
    public void testUsesLocalFiles() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        boolean expResult = getBooleanProperty("dbmd.uses.local.files", defaultValueFor().usesLocalFiles()); // false
        boolean result = dbmd.usesLocalFiles();
        assertEquals(expResult, result);
    }
}
