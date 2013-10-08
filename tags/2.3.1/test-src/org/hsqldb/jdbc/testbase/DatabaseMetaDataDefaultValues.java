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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hsqldb.testbase.PropertyGetter;

/**
 * A basis for supplying java.sql.DataaseMetaData 'getXXX', 'isXXX' and other
 * pure accessor method default value lookup.
 *
 * This is a start toward upgrading the test suite to allow cross driver testing
 * with spi style bases and inheritance. 
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @version 2.1
 * @since HSQLDB 2.1
 */
public class DatabaseMetaDataDefaultValues {
    //

    private static final int StandardMaxIdentifierLength = 128;
    //
    private static double s_javaVersion = 0D;

    protected static synchronized double javaVersion() {
        if (s_javaVersion == 0D) {
            try {
                s_javaVersion = Double.parseDouble(PropertyGetter.getProperty("java.specification.version", "1.0"));
            } catch (NumberFormatException numberFormatException) {
            }
        }
        return s_javaVersion;
    }

    public static DatabaseMetaDataDefaultValues newInstance(String prefix) {
        String fqn = PropertyGetter.getProperty(
                prefix + ".dbmd.default.values.class",
                DatabaseMetaDataDefaultValues.class.getName());

        if (!DatabaseMetaDataDefaultValues.class.getName().equals(fqn)) {
            try {
                return (DatabaseMetaDataDefaultValues) Class.forName(fqn).newInstance();
            } catch (InstantiationException ex) {
                Logger.getLogger(DatabaseMetaDataDefaultValues.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IllegalAccessException ex) {
                Logger.getLogger(DatabaseMetaDataDefaultValues.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(DatabaseMetaDataDefaultValues.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        return new DatabaseMetaDataDefaultValues();
    }

    // for subclases
    protected DatabaseMetaDataDefaultValues() {}

    public boolean allProceduresAreCallable() {
        return true;
    }

    public boolean allTablesAreSelectable() {
        return true;
    }

    public boolean nullsAreSortedHigh() {
        return false;
    }

    public boolean nullsAreSortedLow() {
        return false;
    }

    public boolean nullsAreSortedAtStart() {
        return true;
    }

    public boolean nullsAreSortedAtEnd() {
        return false;
    }

    public boolean usesLocalFiles() {
        return false;
    }

    public boolean usesLocalFilePerTable() {
        return false;
    }

    public boolean supportsMixedCaseIdentifiers() {
        return false;
    }

    public boolean storesUpperCaseIdentifiers() {
        return true;
    }

    public boolean storesLowerCaseIdentifiers() {
        return false;
    }

    public boolean storesMixedCaseIdentifiers() {
        return false;
    }

    public boolean supportsMixedCaseQuotedIdentifiers() {
        return true;
    }

    public boolean storesUpperCaseQuotedIdentifiers() {
        return false;
    }

    public boolean storesLowerCaseQuotedIdentifiers() {
        return false;
    }

    public boolean storesMixedCaseQuotedIdentifiers() {
        return false;
    }

    public String getIdentifierQuoteString() {
        return "\"";
    }

    public String getSQLKeywords() {
        return "";
    }

    public String getNumericFunctions() {
        return "";
    }

    public String getStringFunctions() {
        return "";
    }

    public String getSystemFunctions() {
        return "";
    }

    public String getTimeDateFunctions() {
        return "";
    }

    public String getSearchStringEscape() {
        return "\\";
    }

    public String getExtraNameCharacters() {
        return "";
    }

    public boolean supportsAlterTableWithAddColumn() {
        return true;
    }

    public boolean supportsAlterTableWithDropColumn() {
        return true;
    }

    public boolean supportsColumnAliasing() {
        return true;
    }

    public boolean nullPlusNonNullIsNull() {
        return true;
    }

    public boolean supportsConvert() {
        return true;
    }

    public boolean supportsConvert(int fromType, int toType) {
        // TODO implment the JDBC appendix table lookup
        return false;
    }

    public boolean supportsTableCorrelationNames() {
        return true;
    }

    public boolean supportsDifferentTableCorrelationNames() {
        return true;
    }

    public boolean supportsExpressionsInOrderBy() {
        return true;
    }

    public boolean supportsOrderByUnrelated() {
        return true;
    }

    public boolean supportsGroupBy() {
        return true;
    }

    public boolean supportsGroupByUnrelated() {
        return true;
    }

    public boolean supportsGroupByBeyondSelect() {
        return true;
    }

    public boolean supportsLikeEscapeClause() {
        return true;
    }

    public boolean supportsMultipleResultSets() {
        return true;
    }

    public boolean supportsMultipleTransactions() {
        return true;
    }

    public boolean supportsNonNullableColumns() {
        return true;
    }

    public boolean supportsMinimumSQLGrammar() {
        return true;
    }

    public boolean supportsCoreSQLGrammar() {
        return true;
    }

    public boolean supportsExtendedSQLGrammar() {
        return true;
    }

    public boolean supportsANSI92EntryLevelSQL() {
        return true;
    }

    public boolean supportsANSI92IntermediateSQL() {
        return true;
    }

    public boolean supportsANSI92FullSQL() {
        return true;
    }

    public boolean supportsIntegrityEnhancementFacility() {
        return true;
    }

    public boolean supportsOuterJoins() {
        return true;
    }

    public boolean supportsFullOuterJoins() {
        return true;
    }

    public boolean supportsLimitedOuterJoins() {
        return true;
    }

    public String getSchemaTerm() {
        return "SCHEMA";
    }

    public String getProcedureTerm() {
        return "PROCEDURE";
    }

    public String getCatalogTerm() {
        return "CATALOG";
    }

    public boolean isCatalogAtStart() {
        return true;
    }

    public boolean isReadOnly() {
        return false;
    }

    public String getCatalogSeparator() {
        return ".";
    }

    public boolean supportsSchemasInDataManipulation() {
        return true;
    }

    public boolean supportsSchemasInProcedureCalls() {
        return true;
    }

    public boolean supportsSchemasInTableDefinitions() {
        return true;
    }

    public boolean supportsSchemasInIndexDefinitions() {
        return true;
    }

    public boolean supportsSchemasInPrivilegeDefinitions() {
        return true;
    }

    public boolean supportsCatalogsInDataManipulation() {
        return true;
    }

    public boolean supportsCatalogsInProcedureCalls() {
        return true;
    }

    public boolean supportsCatalogsInTableDefinitions() {
        return true;
    }

    public boolean supportsCatalogsInIndexDefinitions() {
        return true;
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() {
        return true;
    }

    public boolean supportsPositionedDelete() {
        return true;
    }

    public boolean supportsPositionedUpdate() {
        return true;
    }

    public boolean supportsSelectForUpdate() {
        return true;
    }

    public boolean supportsStoredProcedures() {
        return true;
    }

    public boolean supportsSubqueriesInComparisons() {
        return true;
    }

    public boolean supportsSubqueriesInExists() {
        return true;
    }

    public boolean supportsSubqueriesInIns() {
        return true;
    }

    public boolean supportsSubqueriesInQuantifieds() {
        return true;
    }

    public boolean supportsCorrelatedSubqueries() {
        return true;
    }

    public boolean supportsUnion() {
        return true;
    }

    public boolean supportsUnionAll() {
        return true;
    }

    public boolean supportsOpenCursorsAcrossCommit() {
        return true;
    }

    public boolean supportsOpenCursorsAcrossRollback() {
        return true;
    }

    public boolean supportsOpenStatementsAcrossCommit() {
        return true;
    }

    public boolean supportsOpenStatementsAcrossRollback() {
        return true;
    }

    public int getMaxBinaryLiteralLength() {
        return 0;
    }

    public int getMaxCharLiteralLength() {
        return 0;
    }

    public int getMaxColumnNameLength() {
        return 128;
    }

    public int getMaxColumnsInGroupBy() {
        return 0;
    }

    public int getMaxColumnsInIndex() {
        return 0;
    }

    public int getMaxColumnsInOrderBy() {
        return 0;
    }

    public int getMaxColumnsInSelect() {
        return 0;
    }

    public int getMaxColumnsInTable() {
        return 0;
    }

    public int getMaxConnections() {
        return 0;
    }

    public int getMaxCursorNameLength() {
        return 128;
    }

    public int getMaxIndexLength() {
        return 0;
    }

    public int getMaxSchemaNameLength() {
        return 128;
    }

    public int getMaxProcedureNameLength() {
        return 128;
    }

    public int getMaxCatalogNameLength() {
        return 128;
    }

    public int getMaxRowSize() {
        return 0;
    }

    public boolean doesMaxRowSizeIncludeBlobs() {
        return true;
    }

    public int getMaxStatementLength() {
        return 0;
    }

    public int getMaxStatements() {
        return 0;
    }

    public int getMaxTableNameLength() {
        return 128;
    }

    public int getMaxTablesInSelect() {
        return 0;
    }

    public int getMaxUserNameLength() {
        return 128;
    }

    public int getDefaultTransactionIsolation() {
        return Connection.TRANSACTION_SERIALIZABLE;
    }

    public boolean supportsTransactions() {
        return true;
    }

    public boolean supportsTransactionIsolationLevel(int level) {
        switch (level) {
            case Connection.TRANSACTION_READ_UNCOMMITTED:
            case Connection.TRANSACTION_READ_COMMITTED:
            case Connection.TRANSACTION_REPEATABLE_READ:
            case Connection.TRANSACTION_SERIALIZABLE: {
                return true;
            }
            default: {
                return false;
            }
        }
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions() {
        return false;
    }

    public boolean supportsDataManipulationTransactionsOnly() {
        return true;
    }

    public boolean dataDefinitionCausesTransactionCommit() {
        return true;
    }

    public boolean dataDefinitionIgnoredInTransactions() {
        return false;
    }

    public boolean supportsResultSetType(int type) {
        switch (type) {
            case ResultSet.TYPE_FORWARD_ONLY:
            case ResultSet.TYPE_SCROLL_INSENSITIVE: {
                return true;
            }
            default: {
                return false;
            }
        }
    }

    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        if (!supportsResultSetType(type)) {
            return false;
        }

        switch (concurrency) {
            case ResultSet.CONCUR_READ_ONLY: {
                return true;
            }
            default: {
                return false;
            }
        }
    }

    public boolean ownUpdatesAreVisible(int type) {
        return false;
    }

    public boolean ownDeletesAreVisible(int type) {
        return false;
    }

    public boolean ownInsertsAreVisible(int type) {
        return false;
    }

    public boolean othersUpdatesAreVisible(int type) {
        return false;
    }

    public boolean othersDeletesAreVisible(int type) {
        return false;
    }

    public boolean othersInsertsAreVisible(int type) {
        return false;
    }

    public boolean updatesAreDetected(int type) {
        return false;
    }

    public boolean deletesAreDetected(int type) {
        return false;
    }

    public boolean insertsAreDetected(int type) {
        return false;
    }

    public boolean supportsBatchUpdates() {
        return true;
    }

    public boolean supportsSavepoints() {
        return false;
    }

    public boolean supportsNamedParameters() {
        return true;
    }

    public boolean supportsMultipleOpenResults() {
        return true;
    }

    public boolean supportsGetGeneratedKeys() {
        return true;
    }

    public boolean supportsResultSetHoldability(int holdability) {
        return true;
    }

    public int getResultSetHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    public int getJDBCMajorVersion() {
        final double javaVersion = javaVersion();

        if (javaVersion > 1.7) {
            throw new RuntimeException(
                    "JDBC Version is unknown for java version " + javaVersion);
        }

        if (javaVersion >= 1.6) {
            return 4;
        }

        try {
            Class.forName("java.sql.ParameterMetaData");
            return 3;
        } catch (ClassNotFoundException ex) {
        }
        try {
            Class.forName("java.sql.Blob");
            return 2;
        } catch (ClassNotFoundException ex) {
        }

        return 1;
    }

    public int getJDBCMinorVersion() {
        int majorVersion = getJDBCMajorVersion();
        int minorVersion = 0;

        switch (majorVersion) {
            case 1: {
                minorVersion = (javaVersion() < 1.1) ? 0 : 2;
                break;
            }
            case 2: {

                minorVersion = (javaVersion() < 1.4) ? 0 : 1;
                break;
            }
            case 3: {
                minorVersion = 0; // the only minor version in 3.x
                break;
            }
            case 4: {
                minorVersion = (javaVersion() < 1.7) ? 0 : 1;
            }
        }

        return minorVersion;
    }

    public int getSQLStateType() {
        return DatabaseMetaData.sqlStateSQL99;
    }

    public boolean locatorsUpdateCopy() {
        return false;
    }

    public boolean supportsStatementPooling() {
        return true;
    }

    public RowIdLifetime getRowIdLifetime() {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() {
        return true;
    }

    public boolean autoCommitFailureClosesAllResultSets() {
        return false;
    }

    public boolean generatedKeyAlwaysReturned() {
        return true;
    }

    public static List<String> asPropertyList(String prefix) throws Exception {

        Method[] methods = DatabaseMetaDataDefaultValues.class.getDeclaredMethods();
        Object[] noArgs = new Object[0];
        List<String> propertyList = new ArrayList<String>();

        for (int i = 0; i < methods.length; i++) {
            Method method = methods[i];

            int mods = method.getModifiers();

            if (Modifier.isStatic(mods) || !Modifier.isPublic(mods)) {
                continue;
            }

            Class<?>[] parameterTypes = method.getParameterTypes();

            if (parameterTypes != null && parameterTypes.length > 0) {
                continue;
            }

            String key = method.getName().replaceAll(
                    String.format("%s|%s|%s",
                    "(?<=[A-Z])(?=[A-Z][a-z])",
                    "(?<=[^A-Z])(?=[A-Z])",
                    "(?<=[A-Za-z])(?=[^A-Za-z])"), ".").toLowerCase();
            //
            Object value = method.invoke(DatabaseMetaDataDefaultValues.newInstance(prefix),
                    noArgs);

            key = key.replace("get.", "");

            propertyList.add(prefix + ".dbmd." + key + "=" + value);
        }

        Collections.sort(propertyList);

        return propertyList;
    }

    /**
     * Sends a test properties file character stream to the standard output
     * having the same default values supplied by this class
     *
     * @param args the first argument is interpreted as the property prefix.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String prefix = (args == null
                || args.length < 1
                || args[0] == null
                || args[0].trim().length() == 0)
                ? "hsqldb.test.suite"
                : args[0];

        for (Iterator<String> itr = asPropertyList(prefix).iterator(); itr.hasNext();) {
            System.out.println(itr.next());
        }
    }
}
