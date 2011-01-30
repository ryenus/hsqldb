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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.jdbc.testbase.BaseJdbcTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 */
@ForSubject(JDBCDatabaseMetaData.class)
public class JDBCDatabaseMetaDataTest extends BaseJdbcTestCase {

    public static final int StandardMaxIdentifierLength = 128;
    public static final int DefaultJDBCMajorVersion = 4;
    public static final int DefaultJDBCMinorVersion = 0;
    public static final int DefaultDriverMajorVersion = 2;
    public static final int DefaultDriverMinorVersion = 0;
    public static final String DefaultDriverName = "HSQL Database Engine Driver";
    public static final String DefaultDriverVersion = "2.0.1";
    public static final int DefaultDatabaseMajorVersion = 2;
    public static final int DefaultDatabaseMinorVersion = 0;
    private static final String DefaultDatabaseProductName = "HSQL Database Engine";
    private static final String DefaultDatabaseProductVersion = "2.0.1";

    protected final int getStandardMaxIdentifierLength() {
        return StandardMaxIdentifierLength;
    }

    protected int getDefaultJDBCMajorVersion() {
        return DefaultJDBCMajorVersion;
    }

    protected int getDefaultJDBCMinorVersion() {
        return DefaultJDBCMinorVersion;
    }

    protected int getDefaultDriverMajorVersion(){
        return DefaultDriverMajorVersion;
    }

    protected int getExpectedDriverMinorVersion(){
        return getIntProperty(
                "dbmd.driver.minor.version",
                DefaultDriverMinorVersion);
    }

    protected String getExpectedDriverName() {
        return getProperty(
                "dbmd.driver.name",
                DefaultDriverName);
    }

    protected String getExpectedDriverVersion() {
        return getProperty(
                "dbmd.driver.version",
                DefaultDriverVersion);
    }

    protected int getExpectedDatabaseMajorVersion() {
        return getIntProperty(
                "dbmd.database.major.version",
                DefaultDatabaseMajorVersion);
    }

    protected int getExpectedDatabaseMinorVersion() {
        return getIntProperty(
                "dbmd.database.minor.version",
                DefaultDatabaseMinorVersion);
    }

    protected String getExpectedDatabaseProductName() {
        return getProperty(
                "dbmd.database.product.name",
                DefaultDatabaseProductName);
    }

    protected String getExpectedDatabaseProductVersion()
    {
        return getProperty(
                "dbmd.database.product.version",
                DefaultDatabaseProductVersion);
    }

    public JDBCDatabaseMetaDataTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    protected DatabaseMetaData getMetaData() throws Exception {
        return newConnection().getMetaData();
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(JDBCDatabaseMetaDataTest.class);

        return suite;
    }

    /**
     * Test of allProceduresAreCallable method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod("allProceduresAreCallable()")
    public void testAllProceduresAreCallable() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.all.procedures.are.callable",
                true);

        boolean result = dbmd.allProceduresAreCallable();
        assertEquals(expResult, result);
    }

    /**
     * Test of allTablesAreSelectable method, of interface java.sql.DatabaseMetaData.
     */
    @OfMethod("allTablesAreSelectable()")
    public void testAllTablesAreSelectable() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.all.tables.are.selectable",
                true);
        boolean result = dbmd.allTablesAreSelectable();
        assertEquals(expResult, result);
    }

    /**
     * Test of getURL method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetURL() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        String expResult = getUrl();
        String result = dbmd.getURL();
        assertEquals(expResult, result);
    }

    /**
     * Test of getUserName method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetUserName() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        String expResult = getUser();
        String result = dbmd.getUserName();
        assertEquals(expResult, result);
    }

    /**
     * Test of isReadOnly method, of interface java.sql.DatabaseMetaData.
     */
    public void testIsReadOnly() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty("dbmd.is.readonly",false);
        boolean result = dbmd.isReadOnly();
        assertEquals(expResult, result);
    }

    /**
     * Test of nullsAreSortedHigh method, of interface java.sql.DatabaseMetaData.
     */
    public void testNullsAreSortedHigh() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.nulls.are.sorted.high",
                false);
        boolean result = dbmd.nullsAreSortedHigh();
        assertEquals(expResult, result);
    }

    /**
     * Test of nullsAreSortedLow method, of interface java.sql.DatabaseMetaData.
     */
    public void testNullsAreSortedLow() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.nulls.are.sorted.low",
                false);
        boolean result = dbmd.nullsAreSortedLow();
        assertEquals(expResult, result);
    }

    /**
     * Test of nullsAreSortedAtStart method, of interface java.sql.DatabaseMetaData.
     */
    public void testNullsAreSortedAtStart() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.nulls.are.sorted.at.start",
                true);
        boolean result = dbmd.nullsAreSortedAtStart();
        assertEquals(expResult, result);
    }

    /**
     * Test of nullsAreSortedAtEnd method, of interface java.sql.DatabaseMetaData.
     */
    public void testNullsAreSortedAtEnd() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.nulls.are.sorted.at.end",
                false);
        boolean result = dbmd.nullsAreSortedAtEnd();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDatabaseProductName method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetDatabaseProductName() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        String expResult = getExpectedDatabaseProductName();
        String result = dbmd.getDatabaseProductName();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDatabaseProductVersion method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetDatabaseProductVersion() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        String expResult = getExpectedDatabaseProductVersion();
        String result = dbmd.getDatabaseProductVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDriverName method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetDriverName() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        String expResult = getExpectedDriverName();
        String result = dbmd.getDriverName();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDriverVersion method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetDriverVersion() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        String expResult = getExpectedDriverVersion();
        String result = dbmd.getDriverVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDriverMajorVersion method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetDriverMajorVersion() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getDefaultDriverMajorVersion();
        int result = dbmd.getDriverMajorVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDriverMinorVersion method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetDriverMinorVersion() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getExpectedDriverMinorVersion();
        int result = dbmd.getDriverMinorVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of usesLocalFiles method, of interface java.sql.DatabaseMetaData.
     */
    public void testUsesLocalFiles() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.uses.local.files",
                false);
        boolean result = dbmd.usesLocalFiles();
        assertEquals(expResult, result);
    }

    /**
     * Test of usesLocalFilePerTable method, of interface java.sql.DatabaseMetaData.
     */
    public void testUsesLocalFilePerTable() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.uses.local.file.per.table",
                false);
        boolean result = dbmd.usesLocalFilePerTable();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsMixedCaseIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsMixedCaseIdentifiers() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.mixed.case.identifiers",
                false);
        boolean result = dbmd.supportsMixedCaseIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of storesUpperCaseIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    public void testStoresUpperCaseIdentifiers() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.stores.upper.case.identifiers",
                true);
        boolean result = dbmd.storesUpperCaseIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of storesLowerCaseIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    public void testStoresLowerCaseIdentifiers() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.stores.lower.case.identifiers",
                false);
        boolean result = dbmd.storesLowerCaseIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of storesMixedCaseIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    public void testStoresMixedCaseIdentifiers() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.stores.mixed.case.identifiers",
                false);
        boolean result = dbmd.storesMixedCaseIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsMixedCaseQuotedIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsMixedCaseQuotedIdentifiers() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.mixed.case.quoted.identifiers",
                true);
        boolean result = dbmd.supportsMixedCaseQuotedIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of storesUpperCaseQuotedIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    public void testStoresUpperCaseQuotedIdentifiers() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.stores.uppper.case.quoted.identifiers",
                false);
        boolean result = dbmd.storesUpperCaseQuotedIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of storesLowerCaseQuotedIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    public void testStoresLowerCaseQuotedIdentifiers() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.stores.lower.case.quoted.identifiers",
                false);
        boolean result = dbmd.storesLowerCaseQuotedIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of storesMixedCaseQuotedIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    public void testStoresMixedCaseQuotedIdentifiers() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.stores.mixed.case.quoted.identifiers",
                false);
        boolean result = dbmd.storesMixedCaseQuotedIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of getIdentifierQuoteString method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetIdentifierQuoteString() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        String expResult = getProperty(
                "dbmd.identifier.quote.string",
                "\"");
        String result = dbmd.getIdentifierQuoteString();
        assertEquals(expResult, result);
    }

    /**
     * Test of getSQLKeywords method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetSQLKeywords() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        String expResult = getProperty("dbmd.sql.keywords", null);
        String result = null;

        try {
             result = dbmd.getSQLKeywords();
        } catch (Exception e) {
            fail(e.getMessage());
        }

        if (expResult != null)
        {
            assertEquals(expResult, result);
        }
    }

    /**
     * Test of getNumericFunctions method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetNumericFunctions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        String expResult = getProperty("dbmd.numeric.functions", null);
        String result = null;

        try {
            result = dbmd.getNumericFunctions();
        } catch (Exception e) {
            fail(e.getMessage());
        }

        if (expResult != null)
        {
            assertEquals(expResult, result);
        }
    }

    /**
     * Test of getStringFunctions method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetStringFunctions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        String expResult = getProperty("dbmd.string.functions", null);
        String result = null;

        try {
            result = dbmd.getStringFunctions();
        } catch (Exception e) {
            fail(e.getMessage());
        }

        if (expResult != null)
        {
            assertEquals(expResult, result);
        }
    }

    /**
     * Test of getSystemFunctions method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetSystemFunctions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        String expResult = getProperty("dbmd.system.functions", null);
        String result = null;

        try {
            result = dbmd.getSystemFunctions();
        } catch (Exception e) {
            fail(e.getMessage());
        }

        if (expResult != null)
        {
            assertEquals(expResult, result);
        }
    }

    /**
     * Test of getTimeDateFunctions method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetTimeDateFunctions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        String expResult = getProperty("dbmd.timedate.functions", null);
        String result = null;

        try {
            result = dbmd.getTimeDateFunctions();
        } catch (Exception e) {
            fail(e.getMessage());
        }

        if (expResult != null)
        {
            assertEquals(expResult, result);
        }
    }

    /**
     * Test of getSearchStringEscape method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetSearchStringEscape() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        String expResult = getProperty(
                "dbmd.search.string.escape",
                "\\");
        String result = dbmd.getSearchStringEscape();
        assertEquals(expResult, result);
    }

    /**
     * Test of getExtraNameCharacters method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetExtraNameCharacters() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        String expResult = getProperty("dbmd.extra.name.characters", null);
        String result = null;

        try {
            result = dbmd.getExtraNameCharacters();
        } catch (Exception e) {
            fail(e.getMessage());
        }

        if (expResult != null)
        {
            assertEquals(expResult, result);
        }
    }

    /**
     * Test of supportsAlterTableWithAddColumn method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsAlterTableWithAddColumn() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.alter.table.with.add.column",
                true);
        boolean result = dbmd.supportsAlterTableWithAddColumn();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsAlterTableWithDropColumn method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsAlterTableWithDropColumn() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.alter.table.with.drop.column",
                true);
        boolean result = dbmd.supportsAlterTableWithDropColumn();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsColumnAliasing method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsColumnAliasing() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.column.aliasing",
                true);
        boolean result = dbmd.supportsColumnAliasing();
        assertEquals(expResult, result);
    }

    /**
     * Test of nullPlusNonNullIsNull method, of interface java.sql.DatabaseMetaData.
     */
    public void testNullPlusNonNullIsNull() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.null.plus.non.null.is.null",
                true);
        boolean result = dbmd.nullPlusNonNullIsNull();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsConvert method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsConvert() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.convert",
                true);
        boolean result = dbmd.supportsConvert();
        assertEquals(expResult, result);
    }


    /**
     * Test of supportsTableCorrelationNames method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsTableCorrelationNames() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.table.correlation.names",
                true);
        boolean result = dbmd.supportsTableCorrelationNames();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsDifferentTableCorrelationNames method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsDifferentTableCorrelationNames() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.different.table.correlation.names",
                true);
        boolean result = dbmd.supportsDifferentTableCorrelationNames();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsExpressionsInOrderBy method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsExpressionsInOrderBy() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.expressions.in.order.by",
                true);
        boolean result = dbmd.supportsExpressionsInOrderBy();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsOrderByUnrelated method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsOrderByUnrelated() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.order.by.related",
                true);
        boolean result = dbmd.supportsOrderByUnrelated();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsGroupBy method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsGroupBy() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.group.by",
                true);
        boolean result = dbmd.supportsGroupBy();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsGroupByUnrelated method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsGroupByUnrelated() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.group.by.unrelated",
                true);
        boolean result = dbmd.supportsGroupByUnrelated();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsGroupByBeyondSelect method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsGroupByBeyondSelect() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.group.by.beyond.select",
                true);
        boolean result = dbmd.supportsGroupByBeyondSelect();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsLikeEscapeClause method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsLikeEscapeClause() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.like.escape.clause",
                true);
        boolean result = dbmd.supportsLikeEscapeClause();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsMultipleResultSets method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsMultipleResultSets() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.multiple.result.sets",
                false);
        boolean result = dbmd.supportsMultipleResultSets();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsMultipleTransactions method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsMultipleTransactions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.multiple.transactions",
                true);
        boolean result = dbmd.supportsMultipleTransactions();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsNonNullableColumns method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsNonNullableColumns() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.non.nullable.columns",
                true);
        boolean result = dbmd.supportsNonNullableColumns();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsMinimumSQLGrammar method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsMinimumSQLGrammar() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.minimum.sql.grammar",
                true);
        boolean result = dbmd.supportsMinimumSQLGrammar();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsCoreSQLGrammar method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsCoreSQLGrammar() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.core.sql.grammar",
                true);
        boolean result = dbmd.supportsCoreSQLGrammar();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsExtendedSQLGrammar method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsExtendedSQLGrammar() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.extended.sql.grammar",
                true);
        boolean result = dbmd.supportsExtendedSQLGrammar();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsANSI92EntryLevelSQL method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsANSI92EntryLevelSQL() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.ansi92.entry.level.sql",
                true);
        boolean result = dbmd.supportsANSI92EntryLevelSQL();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsANSI92IntermediateSQL method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsANSI92IntermediateSQL() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.ansi92.intermediate.sql",
                true);
        boolean result = dbmd.supportsANSI92IntermediateSQL();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsANSI92FullSQL method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsANSI92FullSQL() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.ansi92.full.sql",
                true);
        boolean result = dbmd.supportsANSI92FullSQL();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsIntegrityEnhancementFacility method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsIntegrityEnhancementFacility() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.integrity.enhancement.facility",
                true);
        boolean result = dbmd.supportsIntegrityEnhancementFacility();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsOuterJoins method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsOuterJoins() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.outer.joins",
                true);
        boolean result = dbmd.supportsOuterJoins();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsFullOuterJoins method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsFullOuterJoins() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.full.outer.joins",
                true);
        boolean result = dbmd.supportsFullOuterJoins();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsLimitedOuterJoins method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsLimitedOuterJoins() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.limited.outer.joins",
                true);
        boolean result = dbmd.supportsLimitedOuterJoins();
        assertEquals(expResult, result);
    }

    /**
     * Test of getSchemaTerm method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetSchemaTerm() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        String expResult = getProperty(
                "dbmd.schema.term",
                "SCHEMA");
        String result = dbmd.getSchemaTerm();
        assertEquals(expResult, result);
    }

    /**
     * Test of getProcedureTerm method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetProcedureTerm() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        String expResult = getProperty(
                "dbmd.procedure.term",
                "");
        String result = dbmd.getProcedureTerm();
        assertEquals(expResult, result);
    }

    /**
     * Test of getCatalogTerm method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetCatalogTerm() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        String expResult = getProperty(
                "dbmd.catalog.term",
                "CATALOG");
        String result = dbmd.getCatalogTerm();
        assertEquals(expResult, result);
    }

    /**
     * Test of isCatalogAtStart method, of interface java.sql.DatabaseMetaData.
     */
    public void testIsCatalogAtStart() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.is.catalog.at.start",
                true);
        boolean result = dbmd.isCatalogAtStart();
        assertEquals(expResult, result);
    }

    /**
     * Test of getCatalogSeparator method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetCatalogSeparator() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        String expResult = getProperty(
                "dbmd.catalog.separator",
                ".");
        String result = dbmd.getCatalogSeparator();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsSchemasInDataManipulation method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSchemasInDataManipulation() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.schemas.in.data.manipulation",
                true);
        boolean result = dbmd.supportsSchemasInDataManipulation();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsSchemasInProcedureCalls method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSchemasInProcedureCalls() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.schemas.in.procedure.calls",
                true);
        boolean result = dbmd.supportsSchemasInProcedureCalls();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsSchemasInTableDefinitions method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSchemasInTableDefinitions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.schemas.in.table.definitions",
                true);
        boolean result = dbmd.supportsSchemasInTableDefinitions();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsSchemasInIndexDefinitions method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSchemasInIndexDefinitions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.schemas.in.index.definitions",
                true);
        boolean result = dbmd.supportsSchemasInIndexDefinitions();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsSchemasInPrivilegeDefinitions method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSchemasInPrivilegeDefinitions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.schemas.in.privilege.definitions",
                true);
        boolean result = dbmd.supportsSchemasInPrivilegeDefinitions();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsCatalogsInDataManipulation method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsCatalogsInDataManipulation() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.catalogs.in.data.manipulation",
                true);
        boolean result = dbmd.supportsCatalogsInDataManipulation();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsCatalogsInProcedureCalls method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsCatalogsInProcedureCalls() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.catalogs.in.procedure.calls",
                true);
        boolean result = dbmd.supportsCatalogsInProcedureCalls();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsCatalogsInTableDefinitions method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsCatalogsInTableDefinitions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.catalogs.in.table.definitions",
                true);
        boolean result = dbmd.supportsCatalogsInTableDefinitions();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsCatalogsInIndexDefinitions method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsCatalogsInIndexDefinitions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.catalogs.in.index.definitions",
                true);
        boolean result = dbmd.supportsCatalogsInIndexDefinitions();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsCatalogsInPrivilegeDefinitions method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsCatalogsInPrivilegeDefinitions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.catalogs.in.privilege.definitions",
                true);
        boolean result = dbmd.supportsCatalogsInPrivilegeDefinitions();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsPositionedDelete method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsPositionedDelete() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.positioned.delete",
                true);
        boolean result = dbmd.supportsPositionedDelete();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsPositionedUpdate method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsPositionedUpdate() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.positioned.update",
                true);
        boolean result = dbmd.supportsPositionedUpdate();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsSelectForUpdate method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSelectForUpdate() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.select.for.update",
                true);
        boolean result = dbmd.supportsSelectForUpdate();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsStoredProcedures method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsStoredProcedures() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.stored.procedures",
                true);
        boolean result = dbmd.supportsStoredProcedures();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsSubqueriesInComparisons method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSubqueriesInComparisons() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.subqueries.in.comparisons",
                true);
        boolean result = dbmd.supportsSubqueriesInComparisons();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsSubqueriesInExists method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSubqueriesInExists() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.subqueries.in.exists",
                true);
        boolean result = dbmd.supportsSubqueriesInExists();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsSubqueriesInIns method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSubqueriesInIns() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.subqueries.in.ins",
                true);
        boolean result = dbmd.supportsSubqueriesInIns();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsSubqueriesInQuantifieds method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSubqueriesInQuantifieds() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.subqueries.in.quantifieds",
                true);
        boolean result = dbmd.supportsSubqueriesInQuantifieds();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsCorrelatedSubqueries method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsCorrelatedSubqueries() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.correlated.subqueries",
                true);
        boolean result = dbmd.supportsCorrelatedSubqueries();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsUnion method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsUnion() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.union",
                true);
        boolean result = dbmd.supportsUnion();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsUnionAll method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsUnionAll() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.union.all",
                true);
        boolean result = dbmd.supportsUnionAll();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsOpenCursorsAcrossCommit method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsOpenCursorsAcrossCommit() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.open.cursors.across.commit",
                true);
        boolean result = dbmd.supportsOpenCursorsAcrossCommit();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsOpenCursorsAcrossRollback method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsOpenCursorsAcrossRollback() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.open.cursors.across.rollback",
                false);
        boolean result = dbmd.supportsOpenCursorsAcrossRollback();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsOpenStatementsAcrossCommit method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsOpenStatementsAcrossCommit() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.open.statements.across.commit",
                true);
        boolean result = dbmd.supportsOpenStatementsAcrossCommit();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsOpenStatementsAcrossRollback method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsOpenStatementsAcrossRollback() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.open.statements.across.rollback",
                true);
        boolean result = dbmd.supportsOpenStatementsAcrossRollback();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxBinaryLiteralLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxBinaryLiteralLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.binary.literal.length",
                0);
        int result = dbmd.getMaxBinaryLiteralLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxCharLiteralLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxCharLiteralLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.char.literal.length",
                0);
        int result = dbmd.getMaxCharLiteralLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxColumnNameLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxColumnNameLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                        "dbmd.max.column.name.length",
                        getStandardMaxIdentifierLength());
        int result = dbmd.getMaxColumnNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxColumnsInGroupBy method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxColumnsInGroupBy() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.columns.in.group.by",
                0);
        int result = dbmd.getMaxColumnsInGroupBy();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxColumnsInIndex method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxColumnsInIndex() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.columns.in.index",
                0);
        int result = dbmd.getMaxColumnsInIndex();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxColumnsInOrderBy method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxColumnsInOrderBy() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.columns.in.order.by",
                0);
        int result = dbmd.getMaxColumnsInOrderBy();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxColumnsInSelect method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxColumnsInSelect() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.columns.in.select",
                0);
        int result = dbmd.getMaxColumnsInSelect();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxColumnsInTable method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxColumnsInTable() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.columns.in.table",
                0);
        int result = dbmd.getMaxColumnsInTable();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxConnections method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxConnections() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.connections",
                0);
        int result = dbmd.getMaxConnections();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxCursorNameLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxCursorNameLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.cursor.name.length",
                getStandardMaxIdentifierLength());
        int result = dbmd.getMaxCursorNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxIndexLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxIndexLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.index.length",
                0);
        int result = dbmd.getMaxIndexLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxSchemaNameLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxSchemaNameLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.schema.name.length",
                getStandardMaxIdentifierLength());
        int result = dbmd.getMaxSchemaNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxProcedureNameLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxProcedureNameLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.procedure.name.length",
                getStandardMaxIdentifierLength());
        int result = dbmd.getMaxProcedureNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxCatalogNameLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxCatalogNameLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.catalog.name.length",
                getStandardMaxIdentifierLength());
        int result = dbmd.getMaxCatalogNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxRowSize method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxRowSize() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.row.size",
                0);
        int result = dbmd.getMaxRowSize();
        assertEquals(expResult, result);
    }

    /**
     * Test of doesMaxRowSizeIncludeBlobs method, of interface java.sql.DatabaseMetaData.
     */
    public void testDoesMaxRowSizeIncludeBlobs() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.does.max.row.size.include.blobs",
                true);
        boolean result = dbmd.doesMaxRowSizeIncludeBlobs();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxStatementLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxStatementLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.statement.length",
                0);
        int result = dbmd.getMaxStatementLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxStatements method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxStatements() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.statements",
                0);
        int result = dbmd.getMaxStatements();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxTableNameLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxTableNameLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.table.name.length",
                getStandardMaxIdentifierLength());
        int result = dbmd.getMaxTableNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxTablesInSelect method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxTablesInSelect() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.tables.in.select",
                0);
        int result = dbmd.getMaxTablesInSelect();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxUserNameLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxUserNameLength() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.max.user.name.length",
                getStandardMaxIdentifierLength());
        int result = dbmd.getMaxUserNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDefaultTransactionIsolation method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetDefaultTransactionIsolation() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.default.transaction.isolation",
                Connection.TRANSACTION_READ_COMMITTED);
        int result = dbmd.getDefaultTransactionIsolation();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsTransactions method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsTransactions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.transactions",
                true);
        boolean result = dbmd.supportsTransactions();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsTransactionIsolationLevel method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsTransactionIsolationLevel() throws Exception {
        int level = Connection.TRANSACTION_READ_UNCOMMITTED;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.transaction.isolation.level.read.uncommited",
                true);
        boolean result = dbmd.supportsTransactionIsolationLevel(level);

        assertEquals("Supports TRANSACTION_READ_UNCOMMITTED", expResult, result);

        level = Connection.TRANSACTION_READ_COMMITTED;

        expResult = getBooleanProperty(
                "dbmd.supports.transaction.isolation.level.read.commited",
                true);

        result = dbmd.supportsTransactionIsolationLevel(level);

        assertEquals("Supports TRANSACTION_READ_COMMITED", expResult, result);

        level = Connection.TRANSACTION_REPEATABLE_READ;

        expResult = getBooleanProperty(
                "dbmd.supports.transaction.isolation.level.repeatable.read",
                true);

        result = dbmd.supportsTransactionIsolationLevel(level);

        assertEquals("Supports TRANSACTION_REPEATABLE_READ", expResult, result);

        level = Connection.TRANSACTION_SERIALIZABLE;

        expResult = getBooleanProperty(
                "dbmd.supports.transaction.isolation.level.serializable",
                true);

        result = dbmd.supportsTransactionIsolationLevel(level);

        assertEquals("Supports TRANSACTION_SERIALIZABLE", expResult, result);
    }

    /**
     * Test of supportsDataDefinitionAndDataManipulationTransactions method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsDataDefinitionAndDataManipulationTransactions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.data.definition.and.data.manipulation.transactions",
                false);
        boolean result = dbmd.supportsDataDefinitionAndDataManipulationTransactions();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsDataManipulationTransactionsOnly method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsDataManipulationTransactionsOnly() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.data.manipulation.transactions.only",
                true);
        boolean result = dbmd.supportsDataManipulationTransactionsOnly();
        assertEquals(expResult, result);
    }

    /**
     * Test of dataDefinitionCausesTransactionCommit method, of interface java.sql.DatabaseMetaData.
     */
    public void testDataDefinitionCausesTransactionCommit() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.data.definition.causes.transaction.commit",
                true);
        boolean result = dbmd.dataDefinitionCausesTransactionCommit();
        assertEquals(expResult, result);
    }

    /**
     * Test of dataDefinitionIgnoredInTransactions method, of interface java.sql.DatabaseMetaData.
     */
    public void testDataDefinitionIgnoredInTransactions() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.data.definition.ignored.in.transactions",
                false);
        boolean result = dbmd.dataDefinitionIgnoredInTransactions();
        assertEquals(expResult, result);
    }


    protected void readResultSet(ResultSet rs) throws Exception
    {
        java.sql.ResultSetMetaData rsmd = rs.getMetaData();

        int cols = rsmd.getColumnCount();

        while(rs.next())
        {
            for(int i = 1; i <= cols; i++)
            {
                Object o = rs.getObject(i);
            }
        }

        rs.close();
    }

    /**
     * Test of getProcedures method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetProcedures() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        String procedureNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getProcedures(
                catalog,
                schemaPattern,
                procedureNamePattern);

        readResultSet(result);
    }

    /**
     * Test of getProcedureColumns method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetProcedureColumns() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        String procedureNamePattern = "%";
        String columnNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getProcedureColumns(
                catalog,
                schemaPattern,
                procedureNamePattern,
                columnNamePattern);

        readResultSet(result);
    }

    /**
     * Test of getTables method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetTables() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        String tableNamePattern = "%";
        String[] types = null;
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getTables(
                catalog,
                schemaPattern,
                tableNamePattern,
                types);

        readResultSet(result);
    }

    /**
     * Test of getSchemas method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetSchemas() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getSchemas();

        readResultSet(result);
    }

    /**
     * Test of getCatalogs method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetCatalogs() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getCatalogs();

        readResultSet(result);
    }

    /**
     * Test of getTableTypes method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetTableTypes() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getTableTypes();

        readResultSet(result);
    }

    /**
     * Test of getColumns method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetColumns() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        String tableNamePattern = "%";
        String columnNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getColumns(
                catalog,
                schemaPattern,
                tableNamePattern,
                columnNamePattern);

        readResultSet(result);
    }

    /**
     * Test of getColumnPrivileges method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetColumnPrivileges() throws Exception {
        String catalog = null;
        String schema = null;
        String table = "SYSTEM_TABLES";
        String columnNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getColumnPrivileges(
                catalog,
                schema,
                table,
                columnNamePattern);

        readResultSet(result);
    }

    /**
     * Test of getTablePrivileges method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetTablePrivileges() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        String tableNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getTablePrivileges(
                catalog,
                schemaPattern,
                tableNamePattern);

        readResultSet(result);
    }

    /**
     * Test of getBestRowIdentifier method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetBestRowIdentifier() throws Exception {
        String catalog = null;
        String schema = null;
        String table = "%";
        int scope = DatabaseMetaData.bestRowTemporary;
        boolean nullable = true;
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getBestRowIdentifier(
                catalog,
                schema,
                table,
                scope,
                nullable);

        readResultSet(result);
    }

    /**
     * Test of getVersionColumns method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetVersionColumns() throws Exception {
        String catalog = null;
        String schema = null;
        String table = "%";
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getVersionColumns(catalog, schema, table);

        readResultSet(result);
    }

    /**
     * Test of getPrimaryKeys method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetPrimaryKeys() throws Exception {
        String catalog = null;
        String schema = null;
        String table = "%";
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getPrimaryKeys(catalog, schema, table);

        readResultSet(result);
    }

    /**
     * Test of getImportedKeys method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetImportedKeys() throws Exception {
        String catalog = null;
        String schema = null;
        String table = "%";
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getImportedKeys(catalog, schema, table);

        readResultSet(result);
    }

    /**
     * Test of getExportedKeys method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetExportedKeys() throws Exception {
        String catalog = null;
        String schema = null;
        String table = "%";
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getExportedKeys(catalog, schema, table);

        readResultSet(result);
    }

    /**
     * Test of getCrossReference method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetCrossReference() throws Exception {
        String parentCatalog = null;
        String parentSchema = null;
        String parentTable = "%";
        String foreignCatalog = null;
        String foreignSchema = null;
        String foreignTable = "%";
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getCrossReference(
                parentCatalog,
                parentSchema,
                parentTable,
                foreignCatalog,
                foreignSchema,
                foreignTable);

        readResultSet(result);
    }

    /**
     * Test of getTypeInfo method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetTypeInfo() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getTypeInfo();

        readResultSet(result);
    }

    /**
     * Test of getIndexInfo method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetIndexInfo() throws Exception {
        String catalog = null;
        String schema = null;
        String table = "%";
        boolean unique = true;
        boolean approximate = true;
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getIndexInfo(
                catalog,
                schema,
                table,
                unique,
                approximate);

        readResultSet(result);
    }

    /**
     * Test of supportsResultSetType method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsResultSetType_TYPE_FORWARD_ONLY() throws Exception {
        int type = ResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.result.set.type.forward.only",
                true);
        boolean result = dbmd.supportsResultSetType(type);
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsResultSetType method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsResultSetType_TYPE_SCROLL_INSENSITIVE() throws Exception {
        int type = ResultSet.TYPE_SCROLL_INSENSITIVE;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.result.set.type.scroll.insensitive",
                true);
        boolean result = dbmd.supportsResultSetType(type);
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsResultSetType method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsResultSetType_TYPE_SCROLL_SENSITIVE() throws Exception {
        int type = ResultSet.TYPE_SCROLL_SENSITIVE;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.result.set.type.scroll.sensitive",
                true);
        boolean result = dbmd.supportsResultSetType(type);
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsResultSetConcurrency method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsResultSetConcurrency_TYPE_FORWARD_ONLY_CONCUR_READ_ONLY() throws Exception {
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        int concurrency = JDBCResultSet.CONCUR_READ_ONLY;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.result.set.concurrency.forward.only.read.only",
                true);
        boolean result = dbmd.supportsResultSetConcurrency(type, concurrency);

        assertEquals(expResult, result);
    }

    /**
     * Test of supportsResultSetConcurrency method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsResultSetConcurrency_TYPE_FORWARD_ONLY_CONCUR_UPDATABLE() throws Exception {
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        int concurrency = JDBCResultSet.CONCUR_UPDATABLE;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.result.set.concurrency.forward.only.updatable",
                true);
        boolean result = dbmd.supportsResultSetConcurrency(type, concurrency);

        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsResultSetConcurrency method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsResultSetConcurrency_TYPE_SCROLL_INSENSITIVE_CONCUR_READ_ONLY() throws Exception {
        int type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
        int concurrency = JDBCResultSet.CONCUR_READ_ONLY;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.result.set.concurrency.scroll.insensitive.read.only",
                true);
        boolean result = dbmd.supportsResultSetConcurrency(type, concurrency);

         assertEquals(expResult, result);
    }

    /**
     * Test of supportsResultSetConcurrency method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsResultSetConcurrency_TYPE_SCROLL_INSENSITIVE_CONCUR_UPDATABLE() throws Exception {
        int type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
        int concurrency = JDBCResultSet.CONCUR_UPDATABLE;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.result.set.concurrency.scroll.insensitive.updatable",
                true);
        boolean result = dbmd.supportsResultSetConcurrency(type, concurrency);

        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsResultSetConcurrency method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsResultSetConcurrency_TYPE_SCROLL_SENSITIVE_CONCUR_READ_ONLY() throws Exception {
        int type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;
        int concurrency = JDBCResultSet.CONCUR_READ_ONLY;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.result.set.concurrency.scroll.sensitive.read.only",
                true);
        boolean result = dbmd.supportsResultSetConcurrency(type, concurrency);

        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsResultSetConcurrency method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsResultSetConcurrency_TYPE_SCROLL_SENSITIVE_CONCUR_UPDATABLE() throws Exception {
        int type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;
        int concurrency = JDBCResultSet.CONCUR_UPDATABLE;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.result.set.concurrency.scroll.sensitive.updatable",
                true);
        boolean result = dbmd.supportsResultSetConcurrency(type, concurrency);

        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of ownUpdatesAreVisible method, of interface java.sql.DatabaseMetaData.
     */
    public void testOwnUpdatesAreVisible() throws Exception {
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.own.updates.are.visible.type.forward.only",
                false);

        boolean result = dbmd.ownUpdatesAreVisible(type);

        assertEquals(
                "OwnUpdatesAreVisible - TYPE_FORWARD_ONLY",
                expResult,
                result);

        type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;

        expResult = getBooleanProperty(
                "dbmd.own.updates.are.visible.type.scroll.insensitive",
                false);

        result = dbmd.ownUpdatesAreVisible(type);

        assertEquals(
                "OwnUpdatesAreVisible - TYPE_SCROLL_INSENSITIVE",
                expResult,
                result);

        type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;

        expResult = getBooleanProperty(
                "dbmd.own.updates.are.visible.type.scroll.sensitive",
                false);

        result = dbmd.ownUpdatesAreVisible(type);

        assertEquals(
                "OwnUpdatesAreVisible - TYPE_SCROLL_SENSITIVE",
                expResult,
                result);
    }

    /**
     * Test of ownDeletesAreVisible method, of interface java.sql.DatabaseMetaData.
     */
    public void testOwnDeletesAreVisible() throws Exception {
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.own.deletes.are.visible.type.forward.only",
                false);

        boolean result = dbmd.ownDeletesAreVisible(type);

        assertEquals(
                "OwnDeletesAreVisible - TYPE_FORWARD_ONLY",
                expResult,
                result);

        type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;

        expResult = getBooleanProperty(
                "dbmd.own.deletes.are.visible.type.scroll.insensitive",
                false);

        result = dbmd.ownDeletesAreVisible(type);

        assertEquals(
                "OwnDeletesAreVisible - TYPE_SCROLL_INSENSITIVE",
                expResult,
                result);

        type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;

        expResult = getBooleanProperty(
                "dbmd.own.deletes.are.visible.type.scroll.sensitive",
                false);

        result = dbmd.ownDeletesAreVisible(type);

        assertEquals(
                "OwnDeletesAreVisible - TYPE_SCROLL_SENSITIVE",
                expResult,
                result);
    }

    /**
     * Test of ownInsertsAreVisible method, of interface java.sql.DatabaseMetaData.
     */
    public void testOwnInsertsAreVisible() throws Exception {
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.own.inserts.are.visible.type.forward.only",
                false);

        boolean result = dbmd.ownInsertsAreVisible(type);

        assertEquals(
                "OwnInsertsAreVisible - TYPE_FORWARD_ONLY",
                expResult,
                result);

        type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;

        expResult = getBooleanProperty(
                "dbmd.own.inserts.are.visible.type.scroll.insensitive",
                false);

        result = dbmd.ownInsertsAreVisible(type);

        assertEquals(
                "OwnInsertsAreVisible - TYPE_SCROLL_INSENSITIVE",
                expResult,
                result);

        type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;

        expResult = getBooleanProperty(
                "dbmd.own.inserts.are.visible.type.scroll.sensitive",
                false);

        result = dbmd.ownInsertsAreVisible(type);

        assertEquals(
                "OwnInsertsAreVisible - TYPE_SCROLL_SENSITIVE",
                expResult,
                result);
    }

    /**
     * Test of othersUpdatesAreVisible method, of interface java.sql.DatabaseMetaData.
     */
    public void testOthersUpdatesAreVisible() throws Exception {
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.others.updates.are.visible.type.forward.only",
                false);

        boolean result = dbmd.othersUpdatesAreVisible(type);

        assertEquals(
                "OthersUpdatesAreVisible - TYPE_FORWARD_ONLY",
                expResult,
                result);

        type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;

        expResult = getBooleanProperty(
                "dbmd.others.updates.are.visible.type.scroll.insensitive",
                false);

        result = dbmd.othersUpdatesAreVisible(type);

        assertEquals(
                "OthersUpdatesAreVisible - TYPE_SCROLL_INSENSITIVE",
                expResult,
                result);

        type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;

        expResult = getBooleanProperty(
                "dbmd.others.updates.are.visible.type.scroll.sensitive",
                false);

        result = dbmd.othersUpdatesAreVisible(type);

        assertEquals(
                "OthersUpdatesAreVisible - TYPE_SCROLL_SENSITIVE",
                expResult,
                result);
    }

    /**
     * Test of othersDeletesAreVisible method, of interface java.sql.DatabaseMetaData.
     */
    public void testOthersDeletesAreVisible() throws Exception {
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.others.deletes.are.visible.type.forward.only",
                false);

        boolean result = dbmd.othersDeletesAreVisible(type);

        assertEquals(
                "OthersDeletesAreVisible - TYPE_FORWARD_ONLY",
                expResult,
                result);

        type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;

        expResult = getBooleanProperty(
                "dbmd.others.deletes.are.visible.type.scroll.insensitive",
                false);

        result = dbmd.othersDeletesAreVisible(type);

        assertEquals(
                "OthersDeletesAreVisible - TYPE_SCROLL_INSENSITIVE",
                expResult,
                result);

        type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;

        expResult = getBooleanProperty(
                "dbmd.others.deletes.are.visible.type.scroll.sensitive",
                false);

        result = dbmd.othersDeletesAreVisible(type);

        assertEquals(
                "OthersDeletesAreVisible - TYPE_SCROLL_SENSITIVE",
                expResult,
                result);
    }

    /**
     * Test of othersInsertsAreVisible method, of interface java.sql.DatabaseMetaData.
     */
    public void testOthersInsertsAreVisible() throws Exception {
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.others.inserts.are.visible.type.forward.only",
                false);

        boolean result = dbmd.othersInsertsAreVisible(type);

        assertEquals(
                "OthersInsertsAreVisible - TYPE_FORWARD_ONLY",
                expResult,
                result);

        type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;

        expResult = getBooleanProperty(
                "dbmd.others.inserts.are.visible.type.scroll.insensitive",
                false);

        result = dbmd.othersInsertsAreVisible(type);

        assertEquals(
                "OthersInsertsAreVisible - TYPE_SCROLL_INSENSITIVE",
                expResult,
                result);

        type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;

        expResult = getBooleanProperty(
                "dbmd.others.inserts.are.visible.type.scroll.sensitive",
                false);

        result = dbmd.othersInsertsAreVisible(type);

        assertEquals(
                "OthersInsertsAreVisible - TYPE_SCROLL_SENSITIVE",
                expResult,
                result);
    }

    /**
     * Test of updatesAreDetected method, of interface java.sql.DatabaseMetaData.
     */
    public void testUpdatesAreDetected() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        //--
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        boolean expResult = getBooleanProperty(
                "dbmd.updates.are.detected.type.forward.only",
                false);
        boolean result = dbmd.updatesAreDetected(type);
        assertEquals(
                "UpdatesAreDetected - TYPE_FORWARD_ONLY",
                expResult,
                result);
        //--
        type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
        expResult = getBooleanProperty(
                "dbmd.updates.are.detected.type.scroll.insensitive",
                false);
        result = dbmd.updatesAreDetected(type);
        assertEquals(
                "UpdatesAreDetected - TYPE_SCROLL_INSENSITIVE",
                expResult,
                result);
        //--
        type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;
        expResult = getBooleanProperty(
                "dbmd.updates.are.detected.type.scroll.sensitive",
                false);
        result = dbmd.updatesAreDetected(type);
        assertEquals(
                "UpdatesAreDetected - TYPE_SCROLL_SENSITIVE",
                expResult,
                result);
    }

    /**
     * Test of deletesAreDetected method, of interface java.sql.DatabaseMetaData.
     */
    public void testDeletesAreDetected() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        //--
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        boolean expResult = getBooleanProperty(
                "dbmd.deletes.are.detected.type.forward.only",
                false);
        boolean result = dbmd.deletesAreDetected(type);
        assertEquals(
                "DeletesAreDetected - TYPE_FORWARD_ONLY",
                expResult,
                result);
        //--
        type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
        expResult = getBooleanProperty(
                "dbmd.deletes.are.detected.type.scroll.insensitive",
                false);
        result =dbmd.deletesAreDetected(type);
        assertEquals(
                "DeletesAreDetected - TYPE_SCROLL_INSENSITIVE",
                expResult,
                result);
        //--
        type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;
        expResult = getBooleanProperty(
                "dbmd.deletes.are.detected.type.scroll.sensitive",
                false);
        result = dbmd.deletesAreDetected(type);
        assertEquals(
                "DeletesAreDetected - TYPE_SCROLL_SENSITIVE",
                expResult,
                result);
    }

    /**
     * Test of insertsAreDetected method, of interface java.sql.DatabaseMetaData.
     */
    public void testInsertsAreDetected() throws Exception {
        DatabaseMetaData dbmd = getMetaData();
        //--
        int type = JDBCResultSet.TYPE_FORWARD_ONLY;
        boolean expResult = getBooleanProperty(
                "dbmd.inserts.are.detected.type.forward.only",
                false);
        boolean result = dbmd.insertsAreDetected(type);
        assertEquals(
                "InsertsAreDetected - TYPE_FORWARD_ONLY",
                expResult,
                result);
        //--
        type = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
        expResult = getBooleanProperty(
                "dbmd.inserts.are.detected.type.scroll.insensitive",
                false);
        result =dbmd.insertsAreDetected(type);
        assertEquals(
                "InsertsAreDetected - TYPE_SCROLL_INSENSITIVE",
                expResult,
                result);
        //--
        type = JDBCResultSet.TYPE_SCROLL_SENSITIVE;
        expResult = getBooleanProperty(
                "dbmd.inserts.are.detected.type.scroll.sensitive",
                false);
        result = dbmd.insertsAreDetected(type);
        assertEquals(
                "InsertsAreDetected - TYPE_SCROLL_SENSITIVE",
                expResult,
                result);
    }

    /**
     * Test of supportsBatchUpdates method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsBatchUpdates() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.batch.updates",
                true);
        boolean result = dbmd.supportsBatchUpdates();
        assertEquals(expResult, result);
    }

    /**
     * Test of getUDTs method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetUDTs() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        String typeNamePattern = "%";
        int[] types = null;
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getUDTs(
                catalog,
                schemaPattern,
                typeNamePattern,
                types);

        readResultSet(result);
    }

    /**
     * Test of getConnection method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetConnection() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        Connection result = dbmd.getConnection();
        assertTrue(result != null);
    }

    /**
     * Test of supportsSavepoints method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSavepoints() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.savepoints",
                true);
        boolean result = dbmd.supportsSavepoints();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsNamedParameters method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsNamedParameters() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.named.parameters",
                true);
        boolean result = dbmd.supportsNamedParameters();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsMultipleOpenResults method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsMultipleOpenResults() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.multiple.open.results",
                true);
        boolean result = dbmd.supportsMultipleOpenResults();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsGetGeneratedKeys method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsGetGeneratedKeys() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.get.generated.keys",
                true);
        boolean result = dbmd.supportsGetGeneratedKeys();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of getSuperTypes method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetSuperTypes() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        String typeNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getSuperTypes(
                catalog,
                schemaPattern,
                typeNamePattern);

        readResultSet(result);
    }

    /**
     * Test of getSuperTables method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetSuperTables() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        String tableNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getSuperTables(
                catalog,
                schemaPattern,
                tableNamePattern);

        readResultSet(result);
    }

    /**
     * Test of getAttributes method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetAttributes() throws Exception {
        String catalog = null;
        String schemaPattern = null;
        String typeNamePattern = "%";
        String attributeNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getAttributes(
                catalog,
                schemaPattern,
                typeNamePattern,
                attributeNamePattern);

        readResultSet(result);
    }

    /**
     * Test of supportsResultSetHoldability method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsResultSetHoldability_HOLD_CURSORS_OVER_COMMIT() throws Exception {
        int holdability = JDBCResultSet.HOLD_CURSORS_OVER_COMMIT;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.result.set.holdability.hold.cursors.over.commit",
                true);
        boolean result = dbmd.supportsResultSetHoldability(holdability);
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsResultSetHoldability method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsResultSetHoldability_CLOSE_CURSORS_AT_COMMIT() throws Exception {
        int holdability = JDBCResultSet.CLOSE_CURSORS_AT_COMMIT;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.result.set.holdability.close.cursors.at.commit",
                true);
        boolean result = dbmd.supportsResultSetHoldability(holdability);
        assertEquals(expResult, result);
    }

    /**
     * Test of getResultSetHoldability method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetResultSetHoldability() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.result.set.holdability",
                JDBCResultSet.HOLD_CURSORS_OVER_COMMIT);
        int result = dbmd.getResultSetHoldability();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDatabaseMajorVersion method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetDatabaseMajorVersion() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.database.major.version",
                getExpectedDatabaseMajorVersion());
        int result = dbmd.getDatabaseMajorVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDatabaseMinorVersion method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetDatabaseMinorVersion() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.database.minor.version",
                getExpectedDatabaseMinorVersion());
        int result = dbmd.getDatabaseMinorVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getJDBCMajorVersion method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetJDBCMajorVersion() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.jdbc.major.version",
                getDefaultJDBCMajorVersion());
        int result = dbmd.getJDBCMajorVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getJDBCMinorVersion method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetJDBCMinorVersion() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.jdbc.minor.version",
                getDefaultJDBCMinorVersion());
        int result = dbmd.getJDBCMinorVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getSQLStateType method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetSQLStateType() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        int expResult = getIntProperty(
                "dbmd.sql.state.type",
                DatabaseMetaData.sqlStateSQL99);
        int result = dbmd.getSQLStateType();
        assertEquals(expResult, result);
    }

    /**
     * Test of locatorsUpdateCopy method, of interface java.sql.DatabaseMetaData.
     */
    public void testLocatorsUpdateCopy() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.locators.update.copy",
                false);
        boolean result = dbmd.locatorsUpdateCopy();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsStatementPooling method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsStatementPooling() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.statement.pooling",
                true);
        boolean result = dbmd.supportsStatementPooling();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of getRowIdLifetime method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetRowIdLifetime() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        RowIdLifetime expResult = RowIdLifetime.ROWID_UNSUPPORTED;
        RowIdLifetime result = dbmd.getRowIdLifetime();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsStoredFunctionsUsingCallSyntax method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsStoredFunctionsUsingCallSyntax() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.supports.stored.functions.using.call.syntax",
                true);
        boolean result = dbmd.supportsStoredFunctionsUsingCallSyntax();
        assertEquals(expResult, result);
    }

    /**
     * Test of autoCommitFailureClosesAllResultSets method, of interface java.sql.DatabaseMetaData.
     */
    public void testAutoCommitFailureClosesAllResultSets() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = getBooleanProperty(
                "dbmd.auto.commit.failure.closes.all.result.sets",
                true);
        boolean result = dbmd.autoCommitFailureClosesAllResultSets();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of getClientInfoProperties method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetClientInfoProperties() throws Exception {
        DatabaseMetaData dbmd = getMetaData();

        try {
            ResultSet result = dbmd.getClientInfoProperties();
        } catch (Exception e) {
            fail("TODO: " + e.getMessage());
        }
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
    public void testUnwrap() throws Exception {
        Class<Object> iface = Object.class;
        DatabaseMetaData dbmd = getMetaData();

        Object expResult = dbmd;
        Object result = dbmd.unwrap(iface);
        assertEquals(expResult, result);
    }

    /**
     * Test of isWrapperFor method, of interface java.sql.DatabaseMetaData.
     */
    public void testIsWrapperFor() throws Exception {
        Class<Object> iface = Object.class;
        DatabaseMetaData dbmd = getMetaData();

        boolean expResult = true;
        boolean result = dbmd.isWrapperFor(iface);
        assertEquals(expResult, result);
    }

    /**
     * Test of getFunctions method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetFunctions() throws Exception {
        String catalog              = "";
        String schemaPattern        = "%";
        String functionNamePattern  = "%";
        DatabaseMetaData dbmd       = getMetaData();

        ResultSet rs = dbmd.getFunctions(
                catalog,
                schemaPattern,
                functionNamePattern);

        readResultSet(rs);
    }

    /**
     * Test of getFunctionColumns method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetFunctionColumns() throws Exception {
        String catalog              = "";
        String schemaPattern        = "%";
        String functionNamePattern  = "%";
        String columnNamePattern    = "%";
        DatabaseMetaData dbmd       = getMetaData();

        ResultSet rs = dbmd.getFunctionColumns(
                catalog,
                schemaPattern,
                functionNamePattern,
                columnNamePattern);

        readResultSet(rs);
    }


    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }
}
