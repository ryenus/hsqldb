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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 *
 * @author boucherb@users
 */
public class jdbcDatabaseMetaDataTest extends JdbcTestCase {
    
    public jdbcDatabaseMetaDataTest(String testName) {
        super(testName);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }
    
    protected DatabaseMetaData getMetaData() throws Exception {
        return newConnection().getMetaData();
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(jdbcDatabaseMetaDataTest.class);
        
        return suite;
    }

    /**
     * Test of allProceduresAreCallable method, of interface java.sql.DatabaseMetaData.
     */
    public void testAllProceduresAreCallable() throws Exception {
        System.out.println("allProceduresAreCallable");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.allProceduresAreCallable();
        assertEquals(expResult, result);
    }

    /**
     * Test of allTablesAreSelectable method, of interface java.sql.DatabaseMetaData.
     */
    public void testAllTablesAreSelectable() throws Exception {
        System.out.println("allTablesAreSelectable");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.allTablesAreSelectable();
        assertEquals(expResult, result);
    }

    /**
     * Test of getURL method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetURL() throws Exception {
        System.out.println("getURL");
        
        DatabaseMetaData dbmd = getMetaData();
        
        String expResult = getUrl();
        String result = dbmd.getURL();
        assertEquals(expResult, result);
    }

    /**
     * Test of getUserName method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetUserName() throws Exception {
        System.out.println("getUserName");
        
        DatabaseMetaData dbmd = getMetaData();
        
        String expResult = "SA";
        String result = dbmd.getUserName();
        assertEquals(expResult, result);
    }

    /**
     * Test of isReadOnly method, of interface java.sql.DatabaseMetaData.
     */
    public void testIsReadOnly() throws Exception {
        System.out.println("isReadOnly");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.isReadOnly();
        assertEquals(expResult, result);
    }

    /**
     * Test of nullsAreSortedHigh method, of interface java.sql.DatabaseMetaData.
     */
    public void testNullsAreSortedHigh() throws Exception {
        System.out.println("nullsAreSortedHigh");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.nullsAreSortedHigh();
        assertEquals(expResult, result);
    }

    /**
     * Test of nullsAreSortedLow method, of interface java.sql.DatabaseMetaData.
     */
    public void testNullsAreSortedLow() throws Exception {
        System.out.println("nullsAreSortedLow");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.nullsAreSortedLow();
        assertEquals(expResult, result);
    }

    /**
     * Test of nullsAreSortedAtStart method, of interface java.sql.DatabaseMetaData.
     */
    public void testNullsAreSortedAtStart() throws Exception {
        System.out.println("nullsAreSortedAtStart");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.nullsAreSortedAtStart();
        assertEquals(expResult, result);
    }

    /**
     * Test of nullsAreSortedAtEnd method, of interface java.sql.DatabaseMetaData.
     */
    public void testNullsAreSortedAtEnd() throws Exception {
        System.out.println("nullsAreSortedAtEnd");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.nullsAreSortedAtEnd();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDatabaseProductName method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetDatabaseProductName() throws Exception {
        System.out.println("getDatabaseProductName");
        
        DatabaseMetaData dbmd = getMetaData();
        
        String expResult = "HSQL Database Engine";
        String result = dbmd.getDatabaseProductName();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDatabaseProductVersion method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetDatabaseProductVersion() throws Exception {
        System.out.println("getDatabaseProductVersion");
        
        DatabaseMetaData dbmd = getMetaData();
        
        String expResult = "1.8.1";
        String result = dbmd.getDatabaseProductVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDriverName method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetDriverName() throws Exception {
        System.out.println("getDriverName");
        
        DatabaseMetaData dbmd = getMetaData();
        
        String expResult = "HSQL Database Engine Driver";
        String result = dbmd.getDriverName();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDriverVersion method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetDriverVersion() throws Exception {
        System.out.println("getDriverVersion");
        
        DatabaseMetaData dbmd = getMetaData();
        
        String expResult = "1.8.1";
        String result = dbmd.getDriverVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDriverMajorVersion method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetDriverMajorVersion() throws Exception {
        System.out.println("getDriverMajorVersion");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 1;
        int result = dbmd.getDriverMajorVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDriverMinorVersion method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetDriverMinorVersion() throws Exception {
        System.out.println("getDriverMinorVersion");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 8;
        int result = dbmd.getDriverMinorVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of usesLocalFiles method, of interface java.sql.DatabaseMetaData.
     */
    public void testUsesLocalFiles() throws Exception {
        System.out.println("usesLocalFiles");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.usesLocalFiles();
        assertEquals(expResult, result);
    }

    /**
     * Test of usesLocalFilePerTable method, of interface java.sql.DatabaseMetaData.
     */
    public void testUsesLocalFilePerTable() throws Exception {
        System.out.println("usesLocalFilePerTable");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.usesLocalFilePerTable();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsMixedCaseIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsMixedCaseIdentifiers() throws Exception {
        System.out.println("supportsMixedCaseIdentifiers");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.supportsMixedCaseIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of storesUpperCaseIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    public void testStoresUpperCaseIdentifiers() throws Exception {
        System.out.println("storesUpperCaseIdentifiers");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.storesUpperCaseIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of storesLowerCaseIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    public void testStoresLowerCaseIdentifiers() throws Exception {
        System.out.println("storesLowerCaseIdentifiers");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.storesLowerCaseIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of storesMixedCaseIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    public void testStoresMixedCaseIdentifiers() throws Exception {
        System.out.println("storesMixedCaseIdentifiers");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.storesMixedCaseIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsMixedCaseQuotedIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsMixedCaseQuotedIdentifiers() throws Exception {
        System.out.println("supportsMixedCaseQuotedIdentifiers");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsMixedCaseQuotedIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of storesUpperCaseQuotedIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    public void testStoresUpperCaseQuotedIdentifiers() throws Exception {
        System.out.println("storesUpperCaseQuotedIdentifiers");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.storesUpperCaseQuotedIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of storesLowerCaseQuotedIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    public void testStoresLowerCaseQuotedIdentifiers() throws Exception {
        System.out.println("storesLowerCaseQuotedIdentifiers");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.storesLowerCaseQuotedIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of storesMixedCaseQuotedIdentifiers method, of interface java.sql.DatabaseMetaData.
     */
    public void testStoresMixedCaseQuotedIdentifiers() throws Exception {
        System.out.println("storesMixedCaseQuotedIdentifiers");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.storesMixedCaseQuotedIdentifiers();
        assertEquals(expResult, result);
    }

    /**
     * Test of getIdentifierQuoteString method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetIdentifierQuoteString() throws Exception {
        System.out.println("getIdentifierQuoteString");
        
        DatabaseMetaData dbmd = getMetaData();
        
        String expResult = "\"";
        String result = dbmd.getIdentifierQuoteString();
        assertEquals(expResult, result);
    }

    /**
     * Test of getSQLKeywords method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetSQLKeywords() throws Exception {
        System.out.println("getSQLKeywords");
        
        DatabaseMetaData dbmd = getMetaData();
        
        try {
            String result = dbmd.getSQLKeywords();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of getNumericFunctions method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetNumericFunctions() throws Exception {
        System.out.println("getNumericFunctions");
        
        DatabaseMetaData dbmd = getMetaData();
        
        try {
            String result = dbmd.getNumericFunctions();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of getStringFunctions method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetStringFunctions() throws Exception {
        System.out.println("getStringFunctions");
        
        DatabaseMetaData dbmd = getMetaData();
        
        try {
            String result = dbmd.getStringFunctions();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of getSystemFunctions method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetSystemFunctions() throws Exception {
        System.out.println("getSystemFunctions");
        
        DatabaseMetaData dbmd = getMetaData();
        
        try {
            String result = dbmd.getSystemFunctions();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of getTimeDateFunctions method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetTimeDateFunctions() throws Exception {
        System.out.println("getTimeDateFunctions");
        
        DatabaseMetaData dbmd = getMetaData();
        
        try {
            String result = dbmd.getTimeDateFunctions();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of getSearchStringEscape method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetSearchStringEscape() throws Exception {
        System.out.println("getSearchStringEscape");
        
        DatabaseMetaData dbmd = getMetaData();
        
        String expResult = "\\";
        String result = dbmd.getSearchStringEscape();
        assertEquals(expResult, result);
    }

    /**
     * Test of getExtraNameCharacters method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetExtraNameCharacters() throws Exception {
        System.out.println("getExtraNameCharacters");
        
        DatabaseMetaData dbmd = getMetaData();
        
        try {
            String result = dbmd.getExtraNameCharacters();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of supportsAlterTableWithAddColumn method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsAlterTableWithAddColumn() throws Exception {
        System.out.println("supportsAlterTableWithAddColumn");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsAlterTableWithAddColumn();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsAlterTableWithDropColumn method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsAlterTableWithDropColumn() throws Exception {
        System.out.println("supportsAlterTableWithDropColumn");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsAlterTableWithDropColumn();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsColumnAliasing method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsColumnAliasing() throws Exception {
        System.out.println("supportsColumnAliasing");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsColumnAliasing();
        assertEquals(expResult, result);
    }

    /**
     * Test of nullPlusNonNullIsNull method, of interface java.sql.DatabaseMetaData.
     */
    public void testNullPlusNonNullIsNull() throws Exception {
        System.out.println("nullPlusNonNullIsNull");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.nullPlusNonNullIsNull();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsConvert method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsConvert() throws Exception {
        System.out.println("supportsConvert");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsConvert();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsTableCorrelationNames method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsTableCorrelationNames() throws Exception {
        System.out.println("supportsTableCorrelationNames");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsTableCorrelationNames();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsDifferentTableCorrelationNames method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsDifferentTableCorrelationNames() throws Exception {
        System.out.println("supportsDifferentTableCorrelationNames");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsDifferentTableCorrelationNames();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsExpressionsInOrderBy method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsExpressionsInOrderBy() throws Exception {
        System.out.println("supportsExpressionsInOrderBy");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsExpressionsInOrderBy();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsOrderByUnrelated method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsOrderByUnrelated() throws Exception {
        System.out.println("supportsOrderByUnrelated");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsOrderByUnrelated();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsGroupBy method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsGroupBy() throws Exception {
        System.out.println("supportsGroupBy");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsGroupBy();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsGroupByUnrelated method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsGroupByUnrelated() throws Exception {
        System.out.println("supportsGroupByUnrelated");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsGroupByUnrelated();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsGroupByBeyondSelect method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsGroupByBeyondSelect() throws Exception {
        System.out.println("supportsGroupByBeyondSelect");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsGroupByBeyondSelect();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsLikeEscapeClause method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsLikeEscapeClause() throws Exception {
        System.out.println("supportsLikeEscapeClause");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsLikeEscapeClause();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsMultipleResultSets method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsMultipleResultSets() throws Exception {
        System.out.println("supportsMultipleResultSets");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.supportsMultipleResultSets();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsMultipleTransactions method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsMultipleTransactions() throws Exception {
        System.out.println("supportsMultipleTransactions");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsMultipleTransactions();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsNonNullableColumns method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsNonNullableColumns() throws Exception {
        System.out.println("supportsNonNullableColumns");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsNonNullableColumns();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsMinimumSQLGrammar method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsMinimumSQLGrammar() throws Exception {
        System.out.println("supportsMinimumSQLGrammar");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsMinimumSQLGrammar();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsCoreSQLGrammar method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsCoreSQLGrammar() throws Exception {
        System.out.println("supportsCoreSQLGrammar");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsCoreSQLGrammar();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsExtendedSQLGrammar method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsExtendedSQLGrammar() throws Exception {
        System.out.println("supportsExtendedSQLGrammar");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsExtendedSQLGrammar();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsANSI92EntryLevelSQL method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsANSI92EntryLevelSQL() throws Exception {
        System.out.println("supportsANSI92EntryLevelSQL");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsANSI92EntryLevelSQL();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsANSI92IntermediateSQL method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsANSI92IntermediateSQL() throws Exception {
        System.out.println("supportsANSI92IntermediateSQL");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsANSI92IntermediateSQL();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsANSI92FullSQL method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsANSI92FullSQL() throws Exception {
        System.out.println("supportsANSI92FullSQL");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsANSI92FullSQL();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsIntegrityEnhancementFacility method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsIntegrityEnhancementFacility() throws Exception {
        System.out.println("supportsIntegrityEnhancementFacility");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsIntegrityEnhancementFacility();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsOuterJoins method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsOuterJoins() throws Exception {
        System.out.println("supportsOuterJoins");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsOuterJoins();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsFullOuterJoins method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsFullOuterJoins() throws Exception {
        System.out.println("supportsFullOuterJoins");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsFullOuterJoins();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsLimitedOuterJoins method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsLimitedOuterJoins() throws Exception {
        System.out.println("supportsLimitedOuterJoins");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsLimitedOuterJoins();
        assertEquals(expResult, result);
    }

    /**
     * Test of getSchemaTerm method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetSchemaTerm() throws Exception {
        System.out.println("getSchemaTerm");
        
        DatabaseMetaData dbmd = getMetaData();
        
        String expResult = "SCHEMA";
        String result = dbmd.getSchemaTerm();
        assertEquals(expResult, result);
    }

    /**
     * Test of getProcedureTerm method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetProcedureTerm() throws Exception {
        System.out.println("getProcedureTerm");
        
        DatabaseMetaData dbmd = getMetaData();
        
        String expResult = "";
        String result = dbmd.getProcedureTerm();
        assertEquals(expResult, result);
    }

    /**
     * Test of getCatalogTerm method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetCatalogTerm() throws Exception {
        System.out.println("getCatalogTerm");
        
        DatabaseMetaData dbmd = getMetaData();
        
        String expResult = "";
        String result = dbmd.getCatalogTerm();
        assertEquals(expResult, result);
    }

    /**
     * Test of isCatalogAtStart method, of interface java.sql.DatabaseMetaData.
     */
    public void testIsCatalogAtStart() throws Exception {
        System.out.println("isCatalogAtStart");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.isCatalogAtStart();
        assertEquals(expResult, result);
    }

    /**
     * Test of getCatalogSeparator method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetCatalogSeparator() throws Exception {
        System.out.println("getCatalogSeparator");
        
        DatabaseMetaData dbmd = getMetaData();
        
        String expResult = "";
        String result = dbmd.getCatalogSeparator();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsSchemasInDataManipulation method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSchemasInDataManipulation() throws Exception {
        System.out.println("supportsSchemasInDataManipulation");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsSchemasInDataManipulation();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsSchemasInProcedureCalls method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSchemasInProcedureCalls() throws Exception {
        System.out.println("supportsSchemasInProcedureCalls");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsSchemasInProcedureCalls();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsSchemasInTableDefinitions method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSchemasInTableDefinitions() throws Exception {
        System.out.println("supportsSchemasInTableDefinitions");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsSchemasInTableDefinitions();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsSchemasInIndexDefinitions method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSchemasInIndexDefinitions() throws Exception {
        System.out.println("supportsSchemasInIndexDefinitions");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsSchemasInIndexDefinitions();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsSchemasInPrivilegeDefinitions method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSchemasInPrivilegeDefinitions() throws Exception {
        System.out.println("supportsSchemasInPrivilegeDefinitions");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsSchemasInPrivilegeDefinitions();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsCatalogsInDataManipulation method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsCatalogsInDataManipulation() throws Exception {
        System.out.println("supportsCatalogsInDataManipulation");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsCatalogsInDataManipulation();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsCatalogsInProcedureCalls method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsCatalogsInProcedureCalls() throws Exception {
        System.out.println("supportsCatalogsInProcedureCalls");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsCatalogsInProcedureCalls();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsCatalogsInTableDefinitions method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsCatalogsInTableDefinitions() throws Exception {
        System.out.println("supportsCatalogsInTableDefinitions");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsCatalogsInTableDefinitions();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsCatalogsInIndexDefinitions method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsCatalogsInIndexDefinitions() throws Exception {
        System.out.println("supportsCatalogsInIndexDefinitions");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsCatalogsInIndexDefinitions();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsCatalogsInPrivilegeDefinitions method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsCatalogsInPrivilegeDefinitions() throws Exception {
        System.out.println("supportsCatalogsInPrivilegeDefinitions");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsCatalogsInPrivilegeDefinitions();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsPositionedDelete method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsPositionedDelete() throws Exception {
        System.out.println("supportsPositionedDelete");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsPositionedDelete();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsPositionedUpdate method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsPositionedUpdate() throws Exception {
        System.out.println("supportsPositionedUpdate");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsPositionedUpdate();
        assertEquals("TODO:", expResult, result);;
    }

    /**
     * Test of supportsSelectForUpdate method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSelectForUpdate() throws Exception {
        System.out.println("supportsSelectForUpdate");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsSelectForUpdate();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsStoredProcedures method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsStoredProcedures() throws Exception {
        System.out.println("supportsStoredProcedures");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsStoredProcedures();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsSubqueriesInComparisons method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSubqueriesInComparisons() throws Exception {
        System.out.println("supportsSubqueriesInComparisons");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsSubqueriesInComparisons();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsSubqueriesInExists method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSubqueriesInExists() throws Exception {
        System.out.println("supportsSubqueriesInExists");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsSubqueriesInExists();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsSubqueriesInIns method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSubqueriesInIns() throws Exception {
        System.out.println("supportsSubqueriesInIns");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsSubqueriesInIns();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsSubqueriesInQuantifieds method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSubqueriesInQuantifieds() throws Exception {
        System.out.println("supportsSubqueriesInQuantifieds");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsSubqueriesInQuantifieds();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsCorrelatedSubqueries method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsCorrelatedSubqueries() throws Exception {
        System.out.println("supportsCorrelatedSubqueries");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsCorrelatedSubqueries();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsUnion method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsUnion() throws Exception {
        System.out.println("supportsUnion");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsUnion();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsUnionAll method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsUnionAll() throws Exception {
        System.out.println("supportsUnionAll");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsUnionAll();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsOpenCursorsAcrossCommit method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsOpenCursorsAcrossCommit() throws Exception {
        System.out.println("supportsOpenCursorsAcrossCommit");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.supportsOpenCursorsAcrossCommit();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsOpenCursorsAcrossRollback method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsOpenCursorsAcrossRollback() throws Exception {
        System.out.println("supportsOpenCursorsAcrossRollback");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.supportsOpenCursorsAcrossRollback();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsOpenStatementsAcrossCommit method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsOpenStatementsAcrossCommit() throws Exception {
        System.out.println("supportsOpenStatementsAcrossCommit");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsOpenStatementsAcrossCommit();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsOpenStatementsAcrossRollback method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsOpenStatementsAcrossRollback() throws Exception {
        System.out.println("supportsOpenStatementsAcrossRollback");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsOpenStatementsAcrossRollback();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxBinaryLiteralLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxBinaryLiteralLength() throws Exception {
        System.out.println("getMaxBinaryLiteralLength");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxBinaryLiteralLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxCharLiteralLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxCharLiteralLength() throws Exception {
        System.out.println("getMaxCharLiteralLength");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxCharLiteralLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxColumnNameLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxColumnNameLength() throws Exception {
        System.out.println("getMaxColumnNameLength");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxColumnNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxColumnsInGroupBy method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxColumnsInGroupBy() throws Exception {
        System.out.println("getMaxColumnsInGroupBy");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxColumnsInGroupBy();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxColumnsInIndex method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxColumnsInIndex() throws Exception {
        System.out.println("getMaxColumnsInIndex");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxColumnsInIndex();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxColumnsInOrderBy method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxColumnsInOrderBy() throws Exception {
        System.out.println("getMaxColumnsInOrderBy");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxColumnsInOrderBy();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxColumnsInSelect method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxColumnsInSelect() throws Exception {
        System.out.println("getMaxColumnsInSelect");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxColumnsInSelect();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxColumnsInTable method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxColumnsInTable() throws Exception {
        System.out.println("getMaxColumnsInTable");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxColumnsInTable();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxConnections method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxConnections() throws Exception {
        System.out.println("getMaxConnections");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxConnections();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxCursorNameLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxCursorNameLength() throws Exception {
        System.out.println("getMaxCursorNameLength");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxCursorNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxIndexLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxIndexLength() throws Exception {
        System.out.println("getMaxIndexLength");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxIndexLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxSchemaNameLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxSchemaNameLength() throws Exception {
        System.out.println("getMaxSchemaNameLength");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxSchemaNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxProcedureNameLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxProcedureNameLength() throws Exception {
        System.out.println("getMaxProcedureNameLength");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxProcedureNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxCatalogNameLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxCatalogNameLength() throws Exception {
        System.out.println("getMaxCatalogNameLength");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxCatalogNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxRowSize method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxRowSize() throws Exception {
        System.out.println("getMaxRowSize");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxRowSize();
        assertEquals(expResult, result);
    }

    /**
     * Test of doesMaxRowSizeIncludeBlobs method, of interface java.sql.DatabaseMetaData.
     */
    public void testDoesMaxRowSizeIncludeBlobs() throws Exception {
        System.out.println("doesMaxRowSizeIncludeBlobs");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.doesMaxRowSizeIncludeBlobs();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxStatementLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxStatementLength() throws Exception {
        System.out.println("getMaxStatementLength");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxStatementLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxStatements method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxStatements() throws Exception {
        System.out.println("getMaxStatements");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxStatements();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxTableNameLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxTableNameLength() throws Exception {
        System.out.println("getMaxTableNameLength");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxTableNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxTablesInSelect method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxTablesInSelect() throws Exception {
        System.out.println("getMaxTablesInSelect");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxTablesInSelect();
        assertEquals(expResult, result);
    }

    /**
     * Test of getMaxUserNameLength method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetMaxUserNameLength() throws Exception {
        System.out.println("getMaxUserNameLength");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getMaxUserNameLength();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDefaultTransactionIsolation method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetDefaultTransactionIsolation() throws Exception {
        System.out.println("getDefaultTransactionIsolation");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = Connection.TRANSACTION_READ_UNCOMMITTED;
        int result = dbmd.getDefaultTransactionIsolation();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsTransactions method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsTransactions() throws Exception {
        System.out.println("supportsTransactions");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsTransactions();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsTransactionIsolationLevel method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsTransactionIsolationLevel() throws Exception {
        System.out.println("supportsTransactionIsolationLevel");
        
        int level = Connection.TRANSACTION_READ_UNCOMMITTED;
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsTransactionIsolationLevel(level);
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsDataDefinitionAndDataManipulationTransactions method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsDataDefinitionAndDataManipulationTransactions() throws Exception {
        System.out.println("supportsDataDefinitionAndDataManipulationTransactions");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.supportsDataDefinitionAndDataManipulationTransactions();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsDataManipulationTransactionsOnly method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsDataManipulationTransactionsOnly() throws Exception {
        System.out.println("supportsDataManipulationTransactionsOnly");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsDataManipulationTransactionsOnly();
        assertEquals(expResult, result);
    }

    /**
     * Test of dataDefinitionCausesTransactionCommit method, of interface java.sql.DatabaseMetaData.
     */
    public void testDataDefinitionCausesTransactionCommit() throws Exception {
        System.out.println("dataDefinitionCausesTransactionCommit");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.dataDefinitionCausesTransactionCommit();
        assertEquals(expResult, result);
    }

    /**
     * Test of dataDefinitionIgnoredInTransactions method, of interface java.sql.DatabaseMetaData.
     */
    public void testDataDefinitionIgnoredInTransactions() throws Exception {
        System.out.println("dataDefinitionIgnoredInTransactions");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.dataDefinitionIgnoredInTransactions();
        assertEquals(expResult, result);
    }

    /**
     * Test of getProcedures method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetProcedures() throws Exception {
        System.out.println("getProcedures");
        
        String catalog = null;
        String schemaPattern = null;
        String procedureNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();
        
        
        ResultSet result = dbmd.getProcedures(catalog, schemaPattern, procedureNamePattern);
    }

    /**
     * Test of getProcedureColumns method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetProcedureColumns() throws Exception {
        System.out.println("getProcedureColumns");
        
        String catalog = null;
        String schemaPattern = null;
        String procedureNamePattern = "%";
        String columnNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();
        
        ResultSet result = dbmd.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
    }

    /**
     * Test of getTables method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetTables() throws Exception {
        System.out.println("getTables");
        
        String catalog = null;
        String schemaPattern = null;
        String tableNamePattern = "%";
        String[] types = null;
        DatabaseMetaData dbmd = getMetaData();
        
        ResultSet result = dbmd.getTables(catalog, schemaPattern, tableNamePattern, types);
    }

    /**
     * Test of getSchemas method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetSchemas() throws Exception {
        System.out.println("getSchemas");
        
        DatabaseMetaData dbmd = getMetaData();
        
        ResultSet result = dbmd.getSchemas();
    }

    /**
     * Test of getCatalogs method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetCatalogs() throws Exception {
        System.out.println("getCatalogs");
        
        DatabaseMetaData dbmd = getMetaData();
        
        ResultSet result = dbmd.getCatalogs();
    }

    /**
     * Test of getTableTypes method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetTableTypes() throws Exception {
        System.out.println("getTableTypes");
        
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getTableTypes();
    }

    /**
     * Test of getColumns method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetColumns() throws Exception {
        System.out.println("getColumns");
        
        String catalog = null;
        String schemaPattern = null;
        String tableNamePattern = "%";
        String columnNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();
        
        ResultSet result = dbmd.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    /**
     * Test of getColumnPrivileges method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetColumnPrivileges() throws Exception {
        System.out.println("getColumnPrivileges");
        
        String catalog = null;
        String schema = null;
        String table = "SYSTEM_TABLES";
        String columnNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();
        
        ResultSet result = dbmd.getColumnPrivileges(catalog, schema, table, columnNamePattern);
    }

    /**
     * Test of getTablePrivileges method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetTablePrivileges() throws Exception {
        System.out.println("getTablePrivileges");
        
        String catalog = null;
        String schemaPattern = null;
        String tableNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();
        
        ResultSet result = dbmd.getTablePrivileges(catalog, schemaPattern, tableNamePattern);
    }

    /**
     * Test of getBestRowIdentifier method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetBestRowIdentifier() throws Exception {
        System.out.println("getBestRowIdentifier");
        
        String catalog = null;
        String schema = null;
        String table = "%";
        int scope = DatabaseMetaData.bestRowTemporary;
        boolean nullable = true;
        DatabaseMetaData dbmd = getMetaData();
        
        ResultSet result = dbmd.getBestRowIdentifier(catalog, schema, table, scope, nullable);
    }

    /**
     * Test of getVersionColumns method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetVersionColumns() throws Exception {
        System.out.println("getVersionColumns");
        
        String catalog = null;
        String schema = null;
        String table = "%";
        DatabaseMetaData dbmd = getMetaData();
        
        ResultSet result = dbmd.getVersionColumns(catalog, schema, table);
    }

    /**
     * Test of getPrimaryKeys method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetPrimaryKeys() throws Exception {
        System.out.println("getPrimaryKeys");
        
        String catalog = null;
        String schema = null;
        String table = "%";
        DatabaseMetaData dbmd = getMetaData();
        
        ResultSet result = dbmd.getPrimaryKeys(catalog, schema, table);
    }

    /**
     * Test of getImportedKeys method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetImportedKeys() throws Exception {
        System.out.println("getImportedKeys");
        
        String catalog = null;
        String schema = null;
        String table = "%";
        DatabaseMetaData dbmd = getMetaData();
        
        ResultSet result = dbmd.getImportedKeys(catalog, schema, table);
    }

    /**
     * Test of getExportedKeys method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetExportedKeys() throws Exception {
        System.out.println("getExportedKeys");
        
        String catalog = null;
        String schema = null;
        String table = "%";
        DatabaseMetaData dbmd = getMetaData();

        ResultSet result = dbmd.getExportedKeys(catalog, schema, table);
    }

    /**
     * Test of getCrossReference method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetCrossReference() throws Exception {
        System.out.println("getCrossReference");
        
        String parentCatalog = null;
        String parentSchema = null;
        String parentTable = "%";
        String foreignCatalog = null;
        String foreignSchema = null;
        String foreignTable = "%";
        DatabaseMetaData dbmd = getMetaData();
        
        ResultSet result = dbmd.getCrossReference(parentCatalog, 
                                                      parentSchema,
                                                      parentTable,
                                                      foreignCatalog,
                                                      foreignSchema,
                                                      foreignTable);
    }

    /**
     * Test of getTypeInfo method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetTypeInfo() throws Exception {
        System.out.println("getTypeInfo");
        
        DatabaseMetaData dbmd = getMetaData();
        
        ResultSet result = dbmd.getTypeInfo();
    }

    /**
     * Test of getIndexInfo method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetIndexInfo() throws Exception {
        System.out.println("getIndexInfo");
        
        String catalog = null;
        String schema = null;
        String table = "%";
        boolean unique = true;
        boolean approximate = true;
        DatabaseMetaData dbmd = getMetaData();
        
        ResultSet result = dbmd.getIndexInfo(catalog, schema, table, unique, approximate);
    }

    /**
     * Test of supportsResultSetType method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsResultSetType() throws Exception {
        System.out.println("supportsResultSetType");
        
        int type = ResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsResultSetType(type);
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsResultSetConcurrency method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsResultSetConcurrency() throws Exception {
        System.out.println("supportsResultSetConcurrency");
        
        int type = jdbcResultSet.TYPE_FORWARD_ONLY;
        int concurrency = jdbcResultSet.CONCUR_READ_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsResultSetConcurrency(type, concurrency);
    }

    /**
     * Test of ownUpdatesAreVisible method, of interface java.sql.DatabaseMetaData.
     */
    public void testOwnUpdatesAreVisible() throws Exception {
        System.out.println("ownUpdatesAreVisible");
        
        int type = jdbcResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.ownUpdatesAreVisible(type);
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of ownDeletesAreVisible method, of interface java.sql.DatabaseMetaData.
     */
    public void testOwnDeletesAreVisible() throws Exception {
        System.out.println("ownDeletesAreVisible");
        
        int type = jdbcResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.ownDeletesAreVisible(type);
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of ownInsertsAreVisible method, of interface java.sql.DatabaseMetaData.
     */
    public void testOwnInsertsAreVisible() throws Exception {
        System.out.println("ownInsertsAreVisible");
        
        int type = jdbcResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.ownInsertsAreVisible(type);
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of othersUpdatesAreVisible method, of interface java.sql.DatabaseMetaData.
     */
    public void testOthersUpdatesAreVisible() throws Exception {
        System.out.println("othersUpdatesAreVisible");
        
        int type = jdbcResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.othersUpdatesAreVisible(type);
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of othersDeletesAreVisible method, of interface java.sql.DatabaseMetaData.
     */
    public void testOthersDeletesAreVisible() throws Exception {
        System.out.println("othersDeletesAreVisible");
        
        int type = jdbcResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.othersDeletesAreVisible(type);
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of othersInsertsAreVisible method, of interface java.sql.DatabaseMetaData.
     */
    public void testOthersInsertsAreVisible() throws Exception {
        System.out.println("othersInsertsAreVisible");
        
        int type = jdbcResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.othersInsertsAreVisible(type);
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of updatesAreDetected method, of interface java.sql.DatabaseMetaData.
     */
    public void testUpdatesAreDetected() throws Exception {
        System.out.println("updatesAreDetected");
        
        int type = jdbcResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.updatesAreDetected(type);
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of deletesAreDetected method, of interface java.sql.DatabaseMetaData.
     */
    public void testDeletesAreDetected() throws Exception {
        System.out.println("deletesAreDetected");
        
        int type = jdbcResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.deletesAreDetected(type);
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of insertsAreDetected method, of interface java.sql.DatabaseMetaData.
     */
    public void testInsertsAreDetected() throws Exception {
        System.out.println("insertsAreDetected");
        
        int type = jdbcResultSet.TYPE_FORWARD_ONLY;
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.insertsAreDetected(type);
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsBatchUpdates method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsBatchUpdates() throws Exception {
        System.out.println("supportsBatchUpdates");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsBatchUpdates();
        assertEquals(expResult, result);
    }

    /**
     * Test of getUDTs method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetUDTs() throws Exception {
        System.out.println("getUDTs");
        
        String catalog = null;
        String schemaPattern = null;
        String typeNamePattern = "%";
        int[] types = null;
        DatabaseMetaData dbmd = getMetaData();
        
        ResultSet result = dbmd.getUDTs(catalog, schemaPattern, typeNamePattern, types);
    }

    /**
     * Test of getConnection method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetConnection() throws Exception {
        System.out.println("getConnection");
        
        DatabaseMetaData dbmd = getMetaData();

        Connection result = dbmd.getConnection();
        assertTrue(result != null);
    }

    /**
     * Test of supportsSavepoints method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsSavepoints() throws Exception {
        System.out.println("supportsSavepoints");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsSavepoints();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsNamedParameters method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsNamedParameters() throws Exception {
        System.out.println("supportsNamedParameters");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsNamedParameters();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsMultipleOpenResults method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsMultipleOpenResults() throws Exception {
        System.out.println("supportsMultipleOpenResults");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsMultipleOpenResults();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of supportsGetGeneratedKeys method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsGetGeneratedKeys() throws Exception {
        System.out.println("supportsGetGeneratedKeys");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsGetGeneratedKeys();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of getSuperTypes method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetSuperTypes() throws Exception {
        System.out.println("getSuperTypes");
        
        String catalog = null;
        String schemaPattern = null;
        String typeNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();
        
        ResultSet result = dbmd.getSuperTypes(catalog, schemaPattern, typeNamePattern);
    }

    /**
     * Test of getSuperTables method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetSuperTables() throws Exception {
        System.out.println("getSuperTables");
        
        String catalog = null;
        String schemaPattern = null;
        String tableNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();
        
        ResultSet result = dbmd.getSuperTables(catalog, schemaPattern, tableNamePattern);
    }

    /**
     * Test of getAttributes method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetAttributes() throws Exception {
        System.out.println("getAttributes");
        
        String catalog = null;
        String schemaPattern = null;
        String typeNamePattern = "%";
        String attributeNamePattern = "%";
        DatabaseMetaData dbmd = getMetaData();
        
        ResultSet result = dbmd.getAttributes(catalog, 
                                                  schemaPattern,
                                                  typeNamePattern,
                                                  attributeNamePattern);
    }

    /**
     * Test of supportsResultSetHoldability method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsResultSetHoldability() throws Exception {
        System.out.println("supportsResultSetHoldability");
        
        int holdability = jdbcResultSet.HOLD_CURSORS_OVER_COMMIT;
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsResultSetHoldability(holdability);
        assertEquals(expResult, result);
    }

    /**
     * Test of getResultSetHoldability method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetResultSetHoldability() throws Exception {
        System.out.println("getResultSetHoldability");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = jdbcResultSet.HOLD_CURSORS_OVER_COMMIT;
        int result = dbmd.getResultSetHoldability();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDatabaseMajorVersion method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetDatabaseMajorVersion() throws Exception {
        System.out.println("getDatabaseMajorVersion");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 1;
        int result = dbmd.getDatabaseMajorVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getDatabaseMinorVersion method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetDatabaseMinorVersion() throws Exception {
        System.out.println("getDatabaseMinorVersion");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 8;
        int result = dbmd.getDatabaseMinorVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getJDBCMajorVersion method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetJDBCMajorVersion() throws Exception {
        System.out.println("getJDBCMajorVersion");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 4;
        int result = dbmd.getJDBCMajorVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getJDBCMinorVersion method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetJDBCMinorVersion() throws Exception {
        System.out.println("getJDBCMinorVersion");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = 0;
        int result = dbmd.getJDBCMinorVersion();
        assertEquals(expResult, result);
    }

    /**
     * Test of getSQLStateType method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetSQLStateType() throws Exception {
        System.out.println("getSQLStateType");
        
        DatabaseMetaData dbmd = getMetaData();
        
        int expResult = DatabaseMetaData.sqlStateSQL99;
        int result = dbmd.getSQLStateType();
        assertEquals(expResult, result);
    }

    /**
     * Test of locatorsUpdateCopy method, of interface java.sql.DatabaseMetaData.
     */
    public void testLocatorsUpdateCopy() throws Exception {
        System.out.println("locatorsUpdateCopy");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = false;
        boolean result = dbmd.locatorsUpdateCopy();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsStatementPooling method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsStatementPooling() throws Exception {
        System.out.println("supportsStatementPooling");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsStatementPooling();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of getRowIdLifetime method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetRowIdLifetime() throws Exception {
        System.out.println("getRowIdLifetime");
        
        DatabaseMetaData dbmd = getMetaData();
        
        RowIdLifetime expResult = RowIdLifetime.ROWID_UNSUPPORTED;
        RowIdLifetime result = dbmd.getRowIdLifetime();
        assertEquals(expResult, result);
    }

    /**
     * Test of supportsStoredFunctionsUsingCallSyntax method, of interface java.sql.DatabaseMetaData.
     */
    public void testSupportsStoredFunctionsUsingCallSyntax() throws Exception {
        System.out.println("supportsStoredFunctionsUsingCallSyntax");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.supportsStoredFunctionsUsingCallSyntax();
        assertEquals(expResult, result);
    }

    /**
     * Test of autoCommitFailureClosesAllResultSets method, of interface java.sql.DatabaseMetaData.
     */
    public void testAutoCommitFailureClosesAllResultSets() throws Exception {
        System.out.println("autoCommitFailureClosesAllResultSets");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.autoCommitFailureClosesAllResultSets();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of getClientInfoProperties method, of interface java.sql.DatabaseMetaData.
     */
    public void testGetClientInfoProperties() throws Exception {
        System.out.println("getClientInfoProperties");
        
        DatabaseMetaData dbmd = getMetaData();
        
        try {
            ResultSet result = dbmd.getClientInfoProperties();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test of providesQueryObjectGenerator method, of interface java.sql.DatabaseMetaData.
     */
    public void testProvidesQueryObjectGenerator() throws Exception {
        System.out.println("providesQueryObjectGenerator");
        
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.providesQueryObjectGenerator();
        assertEquals("TODO:", expResult, result);
    }

    /**
     * Test of unwrap method, of interface java.sql.DatabaseMetaData.
     */
    public void testUnwrap() throws Exception {
        System.out.println("unwrap");
        
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
        System.out.println("isWrapperFor");
        
        Class<Object> iface = Object.class;
        DatabaseMetaData dbmd = getMetaData();
        
        boolean expResult = true;
        boolean result = dbmd.isWrapperFor(iface);
        assertEquals(expResult, result);
    }

    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }

    /**
     * Test of getFunctions method, of class org.hsqldb.jdbc.jdbcDatabaseMetaData.
     */
    public void testGetFunctions() throws Exception {
        System.out.println("getFunctions");
        
        String catalog              = "";
        String schemaPattern        = "%";
        String functionNamePattern  = "%";
        jdbcDatabaseMetaData dbmd   = (jdbcDatabaseMetaData) getMetaData();
        
        ResultSet rs = dbmd.getFunctions(catalog, 
                                         schemaPattern,
                                         functionNamePattern);
    }

    /**
     * Test of getFunctionParameters method, of class org.hsqldb.jdbc.jdbcDatabaseMetaData.
     */
    public void testGetFunctionParameters() throws Exception {
        System.out.println("getFunctionParameters");
        
        String catalog              = "";
        String schemaPattern        = "%";
        String functionNamePattern  = "%";
        String parameterNamePattern = "%";
        jdbcDatabaseMetaData dbmd   = (jdbcDatabaseMetaData) getMetaData();
        
        ResultSet rs = dbmd.getFunctionParameters(catalog,
                                                  schemaPattern,
                                                  functionNamePattern,
                                                  parameterNamePattern);
    }    
}
