/* Copyright (c) 2001-2024, The HSQL Development Group
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
import java.sql.SQLException;

import org.hsqldb.lib.StringConverter;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.types.BlobType;
import org.hsqldb.types.Type;

// fredt@users 20020320 - patch 1.7.0 - JDBC 2 support and error trapping
// JDBC 2 methods can now be called from jdk 1.1.x - see javadoc comments
//
// boucherb &     20020409 - extensive review and update of docs and behaviour
// fredt@users  - 20020505   to comply with previous and latest java.sql
//                           specification
// campbell-burnet@users 20020509 - update to JDK 1.4 / JDBC3 methods and docs
// campbell-burnet@users 2002     - extensive rewrite to support new
//              - 20030121   1.7.2 system table and metadata features.
// campbell-burnet@users 20040422 - doc 1.7.2 - javadoc updates toward 1.7.2 final
// fredt@users    20050505 - patch 1.8.0 - enforced JDBC rules for non-pattern params
// campbell-burnet@users 20051207 - update to JDK 1.6 JDBC 4.0 methods and docs
//              - 20060709
// fredt@users    20080805 - full review and update to doc and method return values
// Revision 1.20  2006/07/12 12:06:54  boucherb
// patch 1.9.0
// - java.sql.Wrapper implementation section title added
// Revision 1.19  2006/07/09 07:07:01  boucherb
// - getting the CVS Log variable output format right
//
// Revision 1.18  2006/07/09 07:02:38  boucherb
// - patch 1.9.0 full synch up to JAVA 1.6 (Mustang) Build 90
// - getColumns() (finally!!!) officially includes IS_AUTOINCREMENT
//

/**
 * Comprehensive information about the database as a whole.
 * <P>
 * This interface is implemented by driver vendors to let users know the capabilities
 * of a Database Management System (DBMS) in combination with
 * the driver based on JDBC technology
 * ("JDBC driver") that is used with it.  Different relational DBMSs often support
 * different features, implement features in different ways, and use different
 * data types.  In addition, a driver may implement a feature on top of what the
 * DBMS offers.  Information returned by methods in this interface applies
 * to the capabilities of a particular driver and a particular DBMS working
 * together. Note that as used in this documentation, the term "database" is
 * used generically to refer to both the driver and DBMS.
 * <P>
 * A user for this interface is commonly a tool that needs to discover how to
 * deal with the underlying DBMS.  This is especially true for applications
 * that are intended to be used with more than one DBMS. For example, a tool might use the method
 * {@code getTypeInfo} to find out what data types can be used in a
 * {@code CREATE TABLE} statement.  Or a user might call the method
 * {@code supportsCorrelatedSubqueries} to see if it is possible to use
 * a correlated subquery or {@code supportsBatchUpdates} to see if it is
 * possible to use batch updates.
 * <P>
 * Some {@code DatabaseMetaData} methods return lists of information
 * in the form of {@code ResultSet} objects.
 * Regular {@code ResultSet} methods, such as
 * {@code getString} and {@code getInt}, can be used
 * to retrieve the data from these {@code ResultSet} objects.  If
 * a given form of metadata is not available, an empty {@code ResultSet}
 * will be returned. Additional columns beyond the columns defined to be
 * returned by the {@code ResultSet} object for a given method
 * can be defined by the JDBC driver vendor and must be accessed
 * by their <B>column label</B>.
 * <P>
 * Some {@code DatabaseMetaData} methods take arguments that are
 * String patterns.  These arguments all have names such as fooPattern.
 * Within a pattern String, "%" means match any substring of 0 or more
 * characters, and "_" means match any one character. Only metadata
 * entries matching the search pattern are returned. If a search pattern
 * argument is set to {@code null}, that argument's criterion will
 * be dropped from the search.
 *
 * <!-- start release-specific documentation -->
 * <div class="ReleaseSpecificDocumentation">
 * <p class="rshead">HSQLDB-Specific Information:</p>
 *
 * <p class="rshead2">Metadata Table Production</p>
 *
 * Starting with HSQLDB 1.7.2, the metadata table (a.k.a system table) production
 * implementation provided in the default build filters metadata based on each
 * SQL session user's access rights which in turn lifts the pre-HSQLDB 1.7.2
 * restriction that only users with the DBA role
 * ('admin' users in older HSQLDB parlance) could expect trouble-free access to
 * all metadata.<p>
 *
 * Also starting with HSQLDB 1.7.2, the metadata table production implementation
 * classes are loaded dynamically, using a precedence policy to find and load
 * the richest producer available at runtime.  In the event that no better
 * alternative is found, the default minimal (completely restricted) provider
 * is selected.  Under this scheme, it is possible for third party packagers to
 * create custom distributions targeted at supporting full (design-time),
 * custom-written (proprietary / micro environment), minimal (production-time)
 * or completely-restricted (space-constrained | device embedded | real-time |
 * hostile environment) metadata table production scenarios. To learn more
 * about this option, interested parties can review the documentation and source
 * code for the {@code org.hsqldb.dbinfo.DatabaseInformation class}.<p>
 *
 * Please also note that in addition to the metadata tables produced to
 * directly support this class, starting with HSQLDB 1.7.2, the default build
 * provides many additional tables covering all or most HSQLDB features, such
 * as descriptions of the triggers and aliases defined in the database. <p>
 *
 * For instance, in the default build, a fairly comprehensive description of
 * each INFORMATION_SCHEMA table and each INFORMATION_SCHEMA table
 * column is included in the REMARKS column of the {@link #getTables(
 * java.lang.String, java.lang.String, java.lang.String, java.lang.String[])
 * getTables(...)} and {@link #getColumns(java.lang.String, java.lang.String,
 * java.lang.String, java.lang.String) getColumns(...)} results, which derive
 * from INFORMATION_SCHEMA.SYSTEM_TABLES and INFORMATION_SCHEMA.SYSTEM_COLUMNS,
 * respectively.<p>
 *
 * Since HSQLDB 2.0 the INFORMATION_SCHEMA views have been vastly expanded
 * in compliance with the SQL:2011 Standard and report the properties of all
 * database objects.</p>
 *
 * <p class="rshead2">Schema Metadata</p>
 *
 * The SQL SCHEMA concept became fully supported in the HSQLDB 1.8.x series and
 * this fact is reflected in the all subsequent versions of this class.
 *
 * <p class="rshead2">Catalog Metadata</p>
 *
 * Starting with HSQLDB 2.0, SQL standards compliance up to SQL:2008 and beyond
 * is a major theme which is reflected in the provision of the majority of the
 * standard-defined full-name INFORMATION_SCHEMA views. <p>
 *
 * However, just as CATALOG semantics and handling are still considered to be
 * implementation defined by the most recent SQL standard (SQL:2011), so is the
 * HSQLDB CATALOG concept still in the process of being defined and refined in
 * HSQLDB 2.x. and beyond.<p>
 *
 * Similarly, starting with HSQLDB 2.x, from the perspective
 * of SQL identification, an HSQLDB JDBC URL connects to a single HSQLDB
 * database instance which consists of a single, default CATALOG
 * named PUBLIC in which each SCHEMA instance of the database resides. The name of
 * this catalog can be changed with the ALTER CATALOG &lt;name&gt; RENAME TO statement.
 * As of version 2.1.0, HSQLDB supports qualification by the containing CATALOG of
 * database objects at the syntactic level, but does not yet support operations
 * such as opening, manipulating or querying against multiple database
 * catalogs within a single session, not even in a one-at-a-time fashion.
 *
 * <p class="rshead2">Index Metadata</p>
 *
 * It must still be noted that as of the most recent release, HSQLDB continues
 * to ignore the {@code approximate} argument of {@link #getIndexInfo
 * getIndexInfo()} as no data is returned for CARDINALITY and PAGES coloumns.
 *
 * <p class="rshead2">Notes for developers extending metadata table production</p>
 *
 * Note that in the absence of an ORDER BY clause, queries against the metadata
 * tables that directly support this class are expected to return rows in JDBC
 * contract order.  The reason for this is that results typically come
 * back much faster when no &quot;ORDER BY&quot; clause is used. <p>
 *
 * As such, when adding, extending or replacing a JDBC database metadata table
 * production routine, developers need to be aware of this fact and either add the
 * contract &quot;ORDER BY&quot; clause to the driving SQL or, when possible,
 * preferably maintain rows in the contract order by correctly coding the
 * primary index definition in the table producer class.
 *
 * <hr>
 *
 * (fredt@users)<br>
 * (campbell-burnet@users)
 * </div>
 * <!-- end release-specific documentation -->
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
 * @since JDK 1.1 HSQLDB 1.9.0
 * @see org.hsqldb.dbinfo.DatabaseInformation
 */
public class JDBCDatabaseMetaData
        implements DatabaseMetaData, java.sql.Wrapper {

    private static final String[] openGroupNumericFunctions  = {
        "ABS", "ACOS", "ASIN", "ATAN", "ATAN2", "BITAND", "BITOR", "BITXOR",
        "CEILING", "COS", "COT", "DEGREES", "EXP", "FLOOR", "LOG", "LOG10",
        "MOD", "PI", "POWER", "RADIANS", "RAND", "ROUND", "ROUNDMAGIC", "SIGN",
        "SIN", "SQRT", "TAN", "TRUNCATE"
    };
    private static final String[] openGroupStringFunctions   = {
        "ASCII", "CHAR", "CONCAT", "DIFFERENCE", "HEXTORAW", "INSERT", "LCASE",
        "LEFT", "LENGTH", "LOCATE", "LTRIM", "RAWTOHEX", "REPEAT", "REPLACE",
        "RIGHT", "RTRIM", "SOUNDEX", "SPACE", "SUBSTR", "UCASE",
    };
    private static final String[] openGroupDateTimeFunctions = {
        "CURDATE", "CURTIME", "DATEDIFF", "DAYNAME", "DAYOFMONTH", "DAYOFWEEK",
        "DAYOFYEAR", "HOUR", "MINUTE", "MONTH", "MONTHNAME", "NOW", "QUARTER",
        "SECOND", "SECONDS_SINCE_MIDNIGHT", "TIMESTAMPADD", "TIMESTAMPDIFF",
        "TO_CHAR", "WEEK", "YEAR"
    };
    private static final String[] openGroupSystemFunctions = { "DATABASE",
            "IFNULL", "USER" };

    //----------------------------------------------------------------------
    // First, a variety of minor information about the target database.

    /**
     * Retrieves whether the current user can call all the procedures
     * returned by the method {@code getProcedures}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * This method <em>always</em> returns
     * {@code true because the listed procedures are those which
     * the current user can use}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean allProceduresAreCallable() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether the current user can use all the tables returned
     * by the method {@code getTables} in a {@code SELECT}
     * statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB always reports {@code true}.<p>
     *
     * The {@code getTables} call returns the list of tables to which the
     * invoking user has some access rights.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    /**
     * Retrieves the URL for this DBMS.
     *
     * @return the URL for this DBMS or {@code null} if it cannot be
     *          generated
     * @throws SQLException if a database access error occurs
     */
    public String getURL() throws SQLException {
        return connection.getURL();
    }

    /**
     * Retrieves the user name as known to this database.
     *
     * @return the database user name
     * @throws SQLException if a database access error occurs
     */
    public String getUserName() throws SQLException {

        ResultSet rs = execute("CALL USER()");

        rs.next();

        String result = rs.getString(1);

        rs.close();

        return result;
    }

    /**
     * Retrieves whether this database is in read-only mode.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * This makes an SQL call to the isReadOnlyDatabase function
     * which provides correct determination of the read-only status for
     * both local and remote database instances.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean isReadOnly() throws SQLException {

        ResultSet rs = execute("CALL IS_READONLY_DATABASE()");

        rs.next();

        boolean result = rs.getBoolean(1);

        rs.close();

        return result;
    }

    /**
     * Retrieves whether {@code NULL} values are sorted high.
     * Sorted high means that {@code NULL} values
     * sort higher than any other value in a domain.  In an ascending order,
     * if this method returns {@code true},  {@code NULL} values
     * will appear at the end. By contrast, the method
     * {@code nullsAreSortedAtEnd} indicates whether {@code NULL} values
     * are sorted at the end regardless of sort order.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * By default HSQLDB sorts null at start and
     * this method returns {@code false}.
     * But a different value is returned if {@code sql.nulls_first} or
     * {@code sql.nulls_lasst} properties have a non-default value.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean nullsAreSortedHigh() throws SQLException {
        setCurrentProperties();

        return !nullsFirst && !nullsOrder;
    }

    /**
     * Retrieves whether {@code NULL} values are sorted low.
     * Sorted low means that {@code NULL} values
     * sort lower than any other value in a domain.  In an ascending order,
     * if this method returns {@code true},  {@code NULL} values
     * will appear at the beginning. By contrast, the method
     * {@code nullsAreSortedAtStart} indicates whether {@code NULL} values
     * are sorted at the beginning regardless of sort order.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * By default HSQLDB sorts null at start and
     * this method returns {@code false}.
     * But a different value is returned if {@code sql.nulls_first} or
     * {@code sql.nulls_lasst} properties have a non-default value.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean nullsAreSortedLow() throws SQLException {
        setCurrentProperties();

        return nullsFirst && !nullsOrder;
    }

    /**
     * Retrieves whether {@code NULL} values are sorted at the start regardless
     * of sort order.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * By default HSQLDB sorts null at start and
     * this method returns {@code true}.
     * But a different value is returned if {@code sql.nulls_first} or
     * {@code sql.nulls_last} properties have a non-default value.<p>
     * Use NULLS LAST in the ORDER BY clause to sort null at the end.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean nullsAreSortedAtStart() throws SQLException {
        setCurrentProperties();

        return nullsFirst && nullsOrder;
    }

    /**
     * Retrieves whether {@code NULL} values are sorted at the end regardless of
     * sort order.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * By default HSQLDB sorts null at start and
     * this method returns {@code false}.
     * But a different value is returned if {@code sql.nulls_first} or
     * {@code sql.nulls_last} properties have a non-default value.<p>
     * Use NULLS LAST in the ORDER BY clause to sort null at the end.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean nullsAreSortedAtEnd() throws SQLException {
        setCurrentProperties();

        return !nullsFirst && nullsOrder;
    }

    /**
     * Retrieves the name of this database product.
     *
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Returns the name of the HSQLDB engine.
     * </div>
     *
     * @return database product name
     * @throws SQLException if a database access error occurs
     */
    public String getDatabaseProductName() throws SQLException {
        return HsqlDatabaseProperties.PRODUCT_NAME;
    }

    /**
     * Retrieves the version number of this database product.
     *
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Returns the full version string.
     * </div>
     *
     * @return database version number
     * @throws SQLException if a database access error occurs
     */
    public String getDatabaseProductVersion() throws SQLException {

        ResultSet rs = execute("CALL DATABASE_VERSION()");

        rs.next();

        return rs.getString(1);
    }

    /**
     * Retrieves the name of this JDBC driver.
     *
     * @return JDBC driver name
     * @throws SQLException if a database access error occurs
     */
    public String getDriverName() throws SQLException {
        return HsqlDatabaseProperties.PRODUCT_NAME + " Driver";
    }

    /**
     * Retrieves the version number of this JDBC driver as a {@code String}.
     *
     * @return JDBC driver version
     * @throws SQLException if a database access error occurs
     */
    public String getDriverVersion() throws SQLException {
        return THIS_VERSION;
    }

    /**
     * Retrieves this JDBC driver's major version number.
     *
     * @return JDBC driver major version
     */
    public int getDriverMajorVersion() {
        return HsqlDatabaseProperties.MAJOR;
    }

    /**
     * Retrieves this JDBC driver's minor version number.
     *
     * @return JDBC driver minor version number
     */
    public int getDriverMinorVersion() {
        return HsqlDatabaseProperties.MINOR;
    }

    /**
     * Retrieves whether this database stores tables in a local file.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From HSQLDB 1.7.2 it is assumed that this refers to data being stored
     * by the JDBC client. This method always returns false.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database uses a file for each table.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not use a file for each table.
     * This method always returns {@code false}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if this database uses a local file for each table;
     *         {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database treats mixed case unquoted SQL identifiers as
     * case sensitive and as a result stores them in mixed case.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB treats unquoted identifiers as case insensitive and stores
     * them in upper case. It treats quoted identifiers as case sensitive and
     * stores them verbatim; this method always returns {@code false}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database treats mixed case unquoted SQL identifiers as
     * case insensitive and stores them in upper case.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB treats unquoted identifiers as case insensitive and stores
     * them in upper case. It treats quoted identifiers as case sensitive and
     * stores them verbatim; this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database treats mixed case unquoted SQL identifiers as
     * case insensitive and stores them in lower case.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB treats unquoted identifiers as case insensitive and stores
     * them in upper case. It treats quoted identifiers as case sensitive and
     * stores them verbatim; this method always returns {@code false}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database treats mixed case unquoted SQL identifiers as
     * case insensitive and stores them in mixed case.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB treats unquoted identifiers as case insensitive and stores
     * them in upper case. It treats quoted identifiers as case sensitive and
     * stores them verbatim; this method always returns {@code false}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database treats mixed case quoted SQL identifiers as
     * case sensitive and as a result stores them in mixed case.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB treats unquoted identifiers as case insensitive and stores
     * them in upper case. It treats quoted identifiers as case sensitive and
     * stores them verbatim; this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database treats mixed case quoted SQL identifiers as
     * case insensitive and stores them in upper case.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB treats unquoted identifiers as case insensitive and stores
     * them in upper case. It treats quoted identifiers as case sensitive and
     * stores them verbatim; this method always returns {@code false}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database treats mixed case quoted SQL identifiers as
     * case insensitive and stores them in lower case.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB treats unquoted identifiers as case insensitive and stores
     * them in upper case. It treats quoted identifiers as case sensitive and
     * stores them verbatim; this method always returns {@code false}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database treats mixed case quoted SQL identifiers as
     * case insensitive and stores them in mixed case.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB treats unquoted identifiers as case insensitive and stores
     * them in upper case. It treats quoted identifiers as case sensitive and
     * stores them verbatim; this method always returns {@code false}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    /**
     * Retrieves the string used to quote SQL identifiers.
     * This method returns a space " " if identifier quoting is not supported.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB uses the standard SQL identifier quote character
     * (the double quote character); this method always returns <b>"</b>.
     * </div>
     * <!-- end release-specific documentation -->
     * @return the quoting string or a space if quoting is not supported
     * @throws SQLException if a database access error occurs
     */
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    /**
     * Retrieves a comma-separated list of all of this database's SQL keywords
     * that are NOT also SQL:2003 keywords.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * The list is empty. However, HSQLDB also supports SQL:2008 keywords
     * and disallows them for database object names without double quoting.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return the list of this database's keywords that are not also
     *         SQL:2003 keywords
     * @throws SQLException if a database access error occurs
     */
    public String getSQLKeywords() throws SQLException {
        return "";
    }

    /**
     * Retrieves a comma-separated list of math functions available with
     * this database.  These are the Open /Open CLI math function names used in
     * the JDBC function escape clause.
     *
     * @return the list of math functions supported by this database
     * @throws SQLException if a database access error occurs
     */
    public String getNumericFunctions() throws SQLException {
        return StringUtil.getList(openGroupNumericFunctions, ",", "");
    }

    /**
     * Retrieves a comma-separated list of string functions available with
     * this database.  These are the  Open Group CLI string function names used
     * in the JDBC function escape clause.
     *
     * @return the list of string functions supported by this database
     * @throws SQLException if a database access error occurs
     */
    public String getStringFunctions() throws SQLException {
        return StringUtil.getList(openGroupStringFunctions, ",", "");
    }

    /**
     * Retrieves a comma-separated list of system functions available with
     * this database.  These are the  Open Group CLI system function names used
     * in the JDBC function escape clause.
     *
     * @return a list of system functions supported by this database
     * @throws SQLException if a database access error occurs
     */
    public String getSystemFunctions() throws SQLException {
        return StringUtil.getList(openGroupSystemFunctions, ",", "");
    }

    /**
     * Retrieves a comma-separated list of the time and date functions available
     * with this database.
     *
     * @return the list of time and date functions supported by this database
     * @throws SQLException if a database access error occurs
     */
    public String getTimeDateFunctions() throws SQLException {
        return StringUtil.getList(openGroupDateTimeFunctions, ",", "");
    }

    /**
     * Retrieves the string that can be used to escape wildcard characters.
     * This is the string that can be used to escape '_' or '%' in
     * the catalog search parameters that are a pattern (and therefore use one
     * of the wildcard characters).
     *
     * <P>The '_' character represents any single character;
     * the '%' character represents any sequence of zero or
     * more characters.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB uses the "\" character to escape wildcard characters.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return the string used to escape wildcard characters
     * @throws SQLException if a database access error occurs
     */
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    /**
     * Retrieves all the "extra" characters that can be used in unquoted
     * identifier names (those beyond a-z, A-Z, 0-9 and _).
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * By default HSQLDB does not support using any "extra" characters in
     * unquoted identifier names; this method always returns the empty String.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return the string containing the extra characters
     * @throws SQLException if a database access error occurs
     */
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    //--------------------------------------------------------------------
    // Functions describing which features are supported.

    /**
     * Retrieves whether this database supports {@code ALTER TABLE}
     * with add column.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this type of
     * {@code ALTER TABLE} statement; this method always
     * returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports {@code ALTER TABLE}
     * with drop column.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this type of
     * {@code ALTER TABLE} statement; this method always
     * returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports column aliasing.
     *
     * <P>If so, the SQL AS clause can be used to provide names for
     * computed columns or to provide alias names for columns as
     * required.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports column aliasing; this method always
     * returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports concatenations between
     * {@code NULL} and non-{@code NULL} values being
     * {@code NULL}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * By default HSQLDB returns NULL when NULL and non-NULL values
     * are concatenated.
     * By default this method returns {@code false}.
     * But a different value is returned if the {@code sql.concat_nulls}
     * property has a non-default value.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return concatNulls;
    }

    /**
     * Retrieves whether this database supports the JDBC scalar function
     * {@code CONVERT} for the conversion of one JDBC type to another.
     * The JDBC types are the generic SQL data types defined
     * in {@code java.sql.Types}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports conversions; this method always
     * returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsConvert() throws SQLException {
        return true;
    }

    /* @todo needs the full conversion matrix here. Should use org.hsqldb.types */

    /**
     * Retrieves whether this database supports the JDBC scalar function
     * {@code CONVERT} for conversions between the JDBC types <i>fromType</i>
     * and <i>toType</i>.  The JDBC types are the generic SQL data types defined
     * in {@code java.sql.Types}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.0 supports conversion according to SQL standards. In addition,
     * it supports conversion between values of BOOLEAN and BIT types.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @param fromType the type to convert from; one of the type codes from
     *        the class {@code java.sql.Types}
     * @param toType the type to convert to; one of the type codes from
     *        the class {@code java.sql.Types}
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @see java.sql.Types
     */
    public boolean supportsConvert(
            int fromType,
            int toType)
            throws SQLException {

        Type from = Type.getDefaultTypeWithSize(
            Type.getHSQLDBTypeCode(fromType));
        Type to = Type.getDefaultTypeWithSize(Type.getHSQLDBTypeCode(toType));

        if (from == null || to == null) {
            return false;
        }

        if (fromType == java.sql.Types.NULL && toType == java.sql.Types.ARRAY) {
            return true;
        }

        return to.canConvertFrom(from);
    }

    /**
     * Retrieves whether this database supports table correlation names.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports table correlation names; this method always
     * returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether, when table correlation names are supported, they
     * are restricted to being different from the names of the tables.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not require that table correlation names are different from the
     * names of the tables; this method always returns {@code false}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsDifferentTableCorrelationNames()
            throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database supports expressions in
     * {@code ORDER BY} lists.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports expressions in {@code ORDER BY} lists; this
     * method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports using a column that is
     * not in the {@code SELECT} statement in an
     * {@code ORDER BY} clause.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports using a column that is not in the {@code SELECT}
     * statement in an {@code ORDER BY} clause; this method always
     * returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports some form of
     * {@code GROUP BY} clause.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports using the {@code GROUP BY} clause; this method
     * always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports using a column that is
     * not in the {@code SELECT} statement in a
     * {@code GROUP BY} clause.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports using a column that is
     * not in the {@code SELECT} statement in a
     * {@code GROUP BY} clause; this method
     * always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports using columns not included in
     * the {@code SELECT} statement in a {@code GROUP BY} clause
     * provided that all of the columns in the {@code SELECT} statement
     * are included in the {@code GROUP BY} clause.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports using columns not included in
     * the {@code SELECT} statement in a {@code GROUP BY} clause
     * provided that all of the columns in the {@code SELECT} statement
     * are included in the {@code GROUP BY} clause; this method
     * always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports specifying a
     * {@code LIKE} escape clause.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports specifying a
     * {@code LIKE} escape clause; this method
     * always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports getting multiple
     * {@code ResultSet} objects from a single call to the
     * method {@code execute}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.0 supports getting multiple
     * {@code ResultSet} objects from a single call to the method
     * {@code execute} of the CallableStatement interface;
     * this method returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsMultipleResultSets() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database allows having multiple
     * transactions open at once (on different connections).
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB allows having multiple
     * transactions open at once (on different connections); this method
     * always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsMultipleTransactions() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether columns in this database may be defined as non-nullable.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports the specification of non-nullable columns; this method
     * always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports the ODBC Minimum SQL grammar.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports the ODBC Minimum SQL grammar;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports the ODBC Core SQL grammar.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports the ODBC Core SQL grammar;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports the ODBC Extended SQL grammar.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports the ODBC Extended SQL grammar;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports the ANSI92 entry level SQL
     * grammar.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports the ANSI92 entry level SQL grammar;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports the ANSI92 intermediate SQL grammar supported.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports the ANSI92 intermediate SQL grammar;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports the ANSI92 full SQL grammar supported.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports the ANSI92 full SQL grammar. The exceptions,
     * such as support for ASSERTION, are not considered grammar issues.
     * This method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsANSI92FullSQL() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports the SQL Integrity
     * Enhancement Facility.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * This method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports some form of outer join.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports outer joins; this method always returns
     * {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports full nested outer joins.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports full nested outer
     * joins; this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database provides limited support for outer
     * joins.  (This will be {@code true} if the method
     * {@code supportsFullOuterJoins} returns {@code true}).
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports the LEFT OUTER join syntax;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    /**
     * Retrieves the database vendor's preferred term for "schema".
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with 1.8.0, HSQLDB supports schemas.
     * </div>
     * <!-- end release-specific documentation -->
     * @return the vendor term for "schema"
     * @throws SQLException if a database access error occurs
     */
    public String getSchemaTerm() throws SQLException {
        return "SCHEMA";
    }

    /**
     * Retrieves the database vendor's preferred term for "procedure".
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports declaration of
     * functions or procedures directly in SQL.
     * </div>
     * <!-- end release-specific documentation -->
     * @return the vendor term for "procedure"
     * @throws SQLException if a database access error occurs
     */
    public String getProcedureTerm() throws SQLException {
        return "PROCEDURE";
    }

    /**
     * Retrieves the database vendor's preferred term for "catalog".
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB uses the standard name CATALOG.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the vendor term for "catalog"
     * @throws SQLException if a database access error occurs
     */
    public String getCatalogTerm() throws SQLException {
        return "CATALOG";
    }

    /**
     * Retrieves whether a catalog appears at the start of a fully qualified
     * table name.  If not, the catalog appears at the end.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * When allowed, a catalog appears at the start of a fully qualified
     * table name; this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if the catalog name appears at the beginning
     *         of a fully qualified table name; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    /**
     * Retrieves the {@code String} that this database uses as the
     * separator between a catalog and table name.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * When used, a catalog name is separated with period;
     * this method <em>always</em> returns a period
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the separator string
     * @throws SQLException if a database access error occurs
     */
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    /**
     * Retrieves whether a schema name can be used in a data manipulation statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports schemas where allowed by the standard;
     * this method always returns {@code true}.
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsSchemasInDataManipulation() throws SQLException {

        // false for OOo client server compatibility
        // otherwise schema name is used by OOo in column references
        return !useSchemaDefault;
    }

    /**
     * Retrieves whether a schema name can be used in a procedure call statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports schemas where allowed by the standard;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsSchemasInProcedureCalls() throws SQLException {

        // false for OOo client server compatibility
        // otherwise schema name is used by OOo in column references
        return !useSchemaDefault;
    }

    /**
     * Retrieves whether a schema name can be used in a table definition statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports schemas where allowed by the standard;
     * this method always returns {@code true}.
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsSchemasInTableDefinitions() throws SQLException {

        // false for OOo client server compatibility
        // otherwise schema name is used by OOo in column references
        return !useSchemaDefault;
    }

    /**
     * Retrieves whether a schema name can be used in an index definition statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports schemas where allowed by the standard;
     * this method always returns {@code true}.
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {

        // false for OOo client server compatibility
        // otherwise schema name is used by OOo in column references
        return !useSchemaDefault;
    }

    /**
     * Retrieves whether a schema name can be used in a privilege definition statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports schemas where allowed by the standard;
     * this method always returns {@code true}.
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {

        // false for OOo client server compatibility
        // otherwise schema name is used by OOo in column references
        return !useSchemaDefault;
    }

    /**
     * Retrieves whether a catalog name can be used in a data manipulation statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports catalog names where allowed by the standard;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsCatalogsInDataManipulation() throws SQLException {

        // false for OOo client server compatibility
        // otherwise catalog name is used by OOo in column references
        return !useSchemaDefault;
    }

    /**
     * Retrieves whether a catalog name can be used in a procedure call statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports catalog names where allowed by the standard;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {

        // false for OOo client server compatibility
        // otherwise catalog name is used by OOo in column references
        return !useSchemaDefault;
    }

    /**
     * Retrieves whether a catalog name can be used in a table definition statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports catalog names where allowed by the standard;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {

        // false for OOo client server compatibility
        // otherwise catalog name is used by OOo in column references
        return !useSchemaDefault;
    }

    /**
     * Retrieves whether a catalog name can be used in an index definition statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports catalog names where allowed by the standard;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {

        // false for OOo client server compatibility
        // otherwise catalog name is used by OOo in column references
        return !useSchemaDefault;
    }

    /**
     * Retrieves whether a catalog name can be used in a privilege definition statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports catalog names where allowed by the standard;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsCatalogsInPrivilegeDefinitions()
            throws SQLException {

        // false for OOo client server compatibility
        // otherwise catalog name is used by OOo in column references
        return !useSchemaDefault;
    }

    /**
     * Retrieves whether this database supports positioned {@code DELETE}
     * statements.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.0 supports updatable result sets;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsPositionedDelete() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports positioned {@code UPDATE}
     * statements.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.0 supports updatable result sets;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsPositionedUpdate() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports {@code SELECT FOR UPDATE}
     * statements.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.0 supports updatable result sets;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsSelectForUpdate() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports stored procedure calls
     * that use the stored procedure escape syntax.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports calling public static Java methods in the context of SQL
     * Stored Procedures; this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsStoredProcedures() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports subqueries in comparison
     * expressions.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB has always supported subqueries in comparison expressions;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports subqueries in
     * {@code EXISTS} expressions.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB has always supported subqueries in {@code EXISTS}
     * expressions; this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsSubqueriesInExists() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports subqueries in
     * {@code IN} expressions.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB has always supported subqueries in {@code IN}
     * statements; this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports subqueries in quantified
     * expressions.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB has always supported subqueries in quantified
     * expressions; this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports correlated subqueries.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB has always supported correlated subqueries;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports SQL {@code UNION}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports SQL {@code UNION};
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports SQL {@code UNION ALL}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports SQL {@code UNION ALL};
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports keeping cursors open
     * across commits.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.0 supports keeping cursors open across commits.
     * This method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if cursors always remain open;
     *       {@code false} if they might not remain open
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports keeping cursors open
     * across rollbacks.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.0 closes open cursors at rollback.
     * This method always returns {@code false}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if cursors always remain open;
     *       {@code false} if they might not remain open
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database supports keeping statements open
     * across commits.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports keeping statements open across commits;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if statements always remain open;
     *       {@code false} if they might not remain open
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports keeping statements open
     * across rollbacks.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports keeping statements open  across rollbacks;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if statements always remain open;
     *       {@code false} if they might not remain open
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return true;
    }

    //----------------------------------------------------------------------
    // The following group of methods exposes various limitations
    // based on the target database with the current driver.
    // Unless otherwise specified, a result of zero means there is no
    // limit, or the limit is not known.

    /**
     * Retrieves the maximum number of hex characters this database allows in an
     * inline binary literal.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availability; this method always returns {@code 0}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return max the maximum length (in hex characters) for a binary literal;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxBinaryLiteralLength() throws SQLException {

        // hard limit is Integer.MAX_VALUE
        return 0;
    }

    /**
     * Retrieves the maximum number of characters this database allows
     * for a character literal.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availability; this method always returns {@code 0}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of characters allowed for a character literal;
     *      a result of zero means that there is no limit or the limit is
     *      not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of characters this database allows
     * for a column name.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with 2.0, HSQLDB implements the SQL standard, which is 128 for
     * all names.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of characters allowed for a column name;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxColumnNameLength() throws SQLException {
        return 128;
    }

    /**
     * Retrieves the maximum number of columns this database allows in a
     * {@code GROUP BY} clause.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availability; this method always returns {@code 0}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of columns allowed;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of columns this database allows in an index.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availability; this method always returns {@code 0}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of columns allowed;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of columns this database allows in an
     * {@code ORDER BY} clause.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availability; this method always returns {@code 0}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of columns allowed;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of columns this database allows in a
     * {@code SELECT} list.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availability; this method always returns {@code 0}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of columns allowed;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of columns this database allows in a table.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availability; this method always returns {@code 0}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of columns allowed;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of concurrent connections to this
     * database that are possible.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availability; this method always returns {@code 0}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of active connections possible at one time;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of characters that this database allows in a
     * cursor name.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with 2.0, HSQLDB implements the SQL standard, which is 128 for
     * all names.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of characters allowed in a cursor name;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxCursorNameLength() throws SQLException {
        return 128;
    }

    /**
     * Retrieves the maximum number of bytes this database allows for an
     * index, including all of the parts of the index.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory and disk availability; this method always returns {@code 0}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of bytes allowed; this limit includes the
     *      composite of all the constituent parts of the index;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of characters that this database allows in a
     * schema name.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with 2.0, HSQLDB implements the SQL standard, which is 128 for
     * all names.
     * </div>
     * <!-- end release-specific documentation -->
     * @return the maximum number of characters allowed in a schema name;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxSchemaNameLength() throws SQLException {
        return 128;
    }

    /**
     * Retrieves the maximum number of characters that this database allows in a
     * procedure name.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with 2.0, HSQLDB implements the SQL standard, which is 128 for
     * all names.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of characters allowed in a procedure name;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxProcedureNameLength() throws SQLException {
        return 128;
    }

    /**
     * Retrieves the maximum number of characters that this database allows in a
     * catalog name.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with 2.0, HSQLDB implements the SQL standard, which is 128 for
     * all names.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of characters allowed in a catalog name;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxCatalogNameLength() throws SQLException {
        return 128;
    }

    /**
     * Retrieves the maximum number of bytes this database allows in
     * a single row.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory and disk availability; this method always returns {@code 0}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of bytes allowed for a row; a result of
     *         zero means that there is no limit or the limit is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    /**
     * Retrieves whether the return value for the method
     * {@code getMaxRowSize} includes the SQL data types
     * {@code LONGVARCHAR} and {@code LONGVARBINARY}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Including 2.0, {@link #getMaxRowSize} <em>always</em> returns
     * 0, indicating that the maximum row size is unknown or has no limit.
     * This applies to the above types as well; this method <em>always</em>
     * returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return true;
    }

    /**
     * Retrieves the maximum number of characters this database allows in
     * an SQL statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availability; this method always returns {@code 0}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of characters allowed for an SQL statement;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of active statements to this database
     * that can be open at the same time.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availability; this method always returns {@code 0}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of statements that can be open at one time;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of characters this database allows in
     * a table name.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Up to and including 1.8.0.x, HSQLDB did not impose a "known" limit.  Th
     * hard limit was the maximum length of a java.lang.String
     * (java.lang.Integer.MAX_VALUE); this method always returned
     * {@code 0}.
     *
     * Starting with 2.0, HSQLDB implements the SQL standard, which is 128 for
     * all names.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of characters allowed for a table name;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxTableNameLength() throws SQLException {
        return 128;
    }

    /**
     * Retrieves the maximum number of tables this database allows in a
     * {@code SELECT} statement.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not impose a "known" limit.  The limit is subject to
     * memory availability; this method always returns {@code 0}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of tables allowed in a {@code SELECT}
     *         statement; a result of zero means that there is no limit or
     *         the limit is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    /**
     * Retrieves the maximum number of characters this database allows in
     * a user name.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with 2.0, HSQLDB implements the SQL standard, which is 128 for
     * all names.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the maximum number of characters allowed for a user name;
     *      a result of zero means that there is no limit or the limit
     *      is not known
     * @throws SQLException if a database access error occurs
     */
    public int getMaxUserNameLength() throws SQLException {
        return 128;
    }

    //----------------------------------------------------------------------

    /**
     * Retrieves this database's default transaction isolation level.  The
     * possible values are defined in {@code java.sql.Connection}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Default isolation mode in version 2.0 is TRANSACTION_READ_COMMITTED.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the default isolation level
     * @throws SQLException if a database access error occurs
     * @see JDBCConnection
     */
    public int getDefaultTransactionIsolation() throws SQLException {

        ResultSet rs = execute("CALL DATABASE_ISOLATION_LEVEL()");

        rs.next();

        String result = rs.getString(1);

        rs.close();

        if (result.startsWith("READ COMMITTED")) {
            return Connection.TRANSACTION_READ_COMMITTED;
        }

        if (result.startsWith("READ UNCOMMITTED")) {
            return Connection.TRANSACTION_READ_UNCOMMITTED;
        }

        if (result.startsWith("SERIALIZABLE")) {
            return Connection.TRANSACTION_SERIALIZABLE;
        }

        return Connection.TRANSACTION_READ_COMMITTED;
    }

    /**
     * Retrieves whether this database supports transactions. If not, invoking the
     * method {@code commit} is a noop, and the isolation level is
     * {@code TRANSACTION_NONE}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports transactions;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if transactions are supported;
     *         {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    /* @todo update javadoc */

    /**
     * Retrieves whether this database supports the given transaction isolation level.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     * HSQLDB supports all levels.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param level one of the transaction isolation levels defined in
     *         {@code java.sql.Connection}
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @see JDBCConnection
     */
    public boolean supportsTransactionIsolationLevel(
            int level)
            throws SQLException {

        return level == Connection.TRANSACTION_READ_UNCOMMITTED
               || level == Connection.TRANSACTION_READ_COMMITTED
               || level == Connection.TRANSACTION_REPEATABLE_READ
               || level == Connection.TRANSACTION_SERIALIZABLE;
    }

    /**
     * Retrieves whether this database supports both data definition and
     * data manipulation statements within a transaction.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not support a mix of both data definition and
     * data manipulation statements within a transaction.  DDL commits the
     * current transaction before proceeding;
     * this method always returns {@code false}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsDataDefinitionAndDataManipulationTransactions()
            throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database supports only data manipulation
     * statements within a transaction.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports only data manipulation
     * statements within a transaction.  DDL commits the
     * current transaction before proceeding, while DML does not;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean supportsDataManipulationTransactionsOnly()
            throws SQLException {
        return true;
    }

    /**
     * Retrieves whether a data definition statement within a transaction forces
     * the transaction to commit.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Including 2.0, a data definition statement within a transaction forces
     * the transaction to commit; this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database ignores a data definition statement
     * within a transaction.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Including 2.0, a data definition statement is not ignored within a
     * transaction.  Rather, a data definition statement within a
     * transaction forces the transaction to commit; this method
     * <em>always</em> returns {@code false}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    /**
     * Retrieves a description of the stored procedures available in the given
     * catalog.
     * <P>
     * Only procedure descriptions matching the schema and
     * procedure name criteria are returned.  They are ordered by
     * {@code PROCEDURE_CAT}, {@code PROCEDURE_SCHEM},
     * {@code PROCEDURE_NAME} and {@code SPECIFIC_ NAME}.
     *
     * <P>Each procedure description has the following columns:
     *  <OL>
     *  <LI><B>PROCEDURE_CAT</B> String {@code =>} procedure catalog (may be {@code null})
     *  <LI><B>PROCEDURE_SCHEM</B> String {@code =>} procedure schema (may be {@code null})
     *  <LI><B>PROCEDURE_NAME</B> String {@code =>} procedure name
     *  <LI> reserved for future use
     *       (HSQLDB-specific: NUM_INPUT_PARAMS)
     *  <LI> reserved for future use
     *       (HSQLDB-specific: NUM_OUTPUT_PARAMS)
     *  <LI> reserved for future use
     *       (HSQLDB-specific: NUM_RESULT_SETS)
     *  <LI><B>REMARKS</B> String {@code =>} explanatory comment on the procedure
     *  <LI><B>PROCEDURE_TYPE</B> short {@code =>} kind of procedure:
     *      <UL>
     *      <LI> procedureResultUnknown - Cannot determine if  a return value
     *       will be returned
     *      <LI> procedureNoResult - Does not return a return value
     *      <LI> procedureReturnsResult - Returns a return value
     *      </UL>
     *  <LI><B>SPECIFIC_NAME</B> String  {@code =>} The name which uniquely identifies this
     * procedure within its schema.
     *  </OL>
     * <p>
     * A user may not have permissions to execute any of the procedures that are
     * returned by {@code getProcedures}
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * In version 2.0, the rows returned by this method are based on rows in
     * the INFORMATION_SCHEMA.ROUTINES table.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        {@code null} means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        {@code null} means that the schema name should not be used to narrow
     *        the search
     * @param procedureNamePattern a procedure name pattern; must match the
     *        procedure name as it is stored in the database
     * @return {@code ResultSet} - each row is a procedure description
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getProcedures(
            String catalog,
            String schemaPattern,
            String procedureNamePattern)
            throws SQLException {

        StringBuilder sb = new StringBuilder();

        sb.append("select procedure_cat, procedure_schem, procedure_name, ")
          .append(
              "col_4, col_5, col_6, remarks, procedure_type, specific_name ")
          .append("from information_schema.system_procedures ")
          .append("where procedure_type = 1 ");

        if (wantsIsNull(procedureNamePattern)) {
            sb.append("and 1=0");

            return execute(sb.toString());
        }

        catalog       = translateCatalog(catalog);
        schemaPattern = translateSchema(schemaPattern);

        sb.append(and("PROCEDURE_CAT", "=", catalog))
          .append(and("PROCEDURE_SCHEM", "LIKE", schemaPattern))
          .append(and("PROCEDURE_NAME", "LIKE", procedureNamePattern))
          .append(
              " ORDER BY PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME, SPECIFIC_NAME");

        return execute(sb.toString());
    }

    /*
     * Indicates that it is not known whether the procedure returns
     * a result.
     * <P>
     * A possible value for column {@code PROCEDURE_TYPE} in the
     * {@code ResultSet} object returned by the method
     * {@code getProcedures}.
     */

//    int procedureResultUnknown        = 0;

    /*
     * Indicates that the procedure does not return a result.
     * <P>
     * A possible value for column {@code PROCEDURE_TYPE} in the
     * {@code ResultSet} object returned by the method
     * {@code getProcedures}.
     */

//    int procedureNoResult             = 1;

    /*
     * Indicates that the procedure returns a result.
     * <P>
     * A possible value for column {@code PROCEDURE_TYPE} in the
     * {@code ResultSet} object returned by the method
     * {@code getProcedures}.
     */

//    int procedureReturnsResult        = 2;

    /**
     * Retrieves a description of the given catalog's stored procedure parameter
     * and result columns.
     *
     * <P>Only descriptions matching the schema, procedure and
     * parameter name criteria are returned.  They are ordered by
     * PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME and SPECIFIC_NAME. Within this, the return value,
     * if any, is first. Next are the parameter descriptions in call
     * order. The column descriptions follow in column number order.
     *
     * <P>Each row in the {@code ResultSet} is a parameter description or
     * column description with the following fields:
     *  <OL>
     *  <LI><B>PROCEDURE_CAT</B> String {@code =>} procedure catalog (may be {@code null})
     *  <LI><B>PROCEDURE_SCHEM</B> String {@code =>} procedure schema (may be {@code null})
     *  <LI><B>PROCEDURE_NAME</B> String {@code =>} procedure name
     *  <LI><B>COLUMN_NAME</B> String {@code =>} column/parameter name
     *  <LI><B>COLUMN_TYPE</B> Short {@code =>} kind of column/parameter:
     *      <UL>
     *      <LI> procedureColumnUnknown - nobody knows
     *      <LI> procedureColumnIn - IN parameter
     *      <LI> procedureColumnInOut - INOUT parameter
     *      <LI> procedureColumnOut - OUT parameter
     *      <LI> procedureColumnReturn - procedure return value
     *      <LI> procedureColumnResult - result column in {@code ResultSet}
     *      </UL>
     *  <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
     *  <LI><B>TYPE_NAME</B> String {@code =>} SQL type name, for a UDT type the
     *  type name is fully qualified
     *  <LI><B>PRECISION</B> int {@code =>} precision
     *  <LI><B>LENGTH</B> int {@code =>} length in bytes of data
     *  <LI><B>SCALE</B> short {@code =>} scale -  null is returned for data types where
     * SCALE is not applicable.
     *  <LI><B>RADIX</B> short {@code =>} radix
     *  <LI><B>NULLABLE</B> short {@code =>} can it contain NULL.
     *      <UL>
     *      <LI> procedureNoNulls - does not allow NULL values
     *      <LI> procedureNullable - allows NULL values
     *      <LI> procedureNullableUnknown - nullability unknown
     *      </UL>
     *  <LI><B>REMARKS</B> String {@code =>} comment describing parameter/column
     *  <LI><B>COLUMN_DEF</B> String {@code =>} default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be {@code null})
     *      <UL>
     *      <LI> The string NULL (not enclosed in quotes) - if NULL was specified as the default value
     *      <LI> TRUNCATE (not enclosed in quotes)        - if the specified default value cannot be represented without truncation
     *      <LI> NULL                                     - if a default value was not specified
     *      </UL>
     *  <LI><B>SQL_DATA_TYPE</B> int  {@code =>} Reserved for future use
     *
     *        <p>HSQLDB-specific: CLI type from SQL 2003 Table 37,
     *        tables 6-9 Annex A1, and/or addenda in other
     *        documents, such as:<br>
     *        SQL 2003 Part 9: Management of External Data (SQL/MED) : DATALINK<br>
     *        SQL 2003 Part 14: XML-Related Specifications (SQL/XML) : XML
     *
     *  <LI><B>SQL_DATETIME_SUB</B> int  {@code =>} reserved for future use
     *
     *        <p>HSQLDB-specific: CLI SQL_DATETIME_SUB from SQL 2003 Table 37
     *
     *  <LI><B>CHAR_OCTET_LENGTH</B> int  {@code =>} the maximum length of binary and character based columns.  For any other datatype the returned value is a
     * NULL
     *  <LI><B>ORDINAL_POSITION</B> int  {@code =>} the ordinal position, starting from 1, for the input and output parameters for a procedure. A value of 0
     * is returned if this row describes the procedure's return value. For result set columns, it is the
     * ordinal position of the column in the result set starting from 1.  If there are
     * multiple result sets, the column ordinal positions are implementation
     * defined.
     *  <LI><B>IS_NULLABLE</B> String  {@code =>} ISO rules are used to determine the nullability for a column.
     *       <UL>
     *       <LI> YES           --- if the parameter or result column can include NULLs
     *       <LI> NO            --- if the parameter or result column cannot include NULLs
     *       <LI> empty string  --- if the nullability for the
     * parameter or result column is unknown
     *       </UL>
     *  <LI><B>SPECIFIC_NAME</B> String  {@code =>} the name which uniquely identifies this procedure within its schema.
     * </OL>
     *
     * <P><B>Note:</B> Some databases may not return the column
     * descriptions for a procedure. Additional columns beyond
     * SPECIFIC_NAME can be defined by the database and must be accessed by their <B>column name</B>.
     *
     * <p>The PRECISION column represents the specified column size for the given column.
     * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
     * For datetime datatypes, this is the length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
     * this is the length in bytes. Null is returned for data types where the
     * column size is not applicable.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        {@code null} means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        {@code null} means that the schema name should not be used to narrow
     *        the search
     * @param procedureNamePattern a procedure name pattern; must match the
     *        procedure name as it is stored in the database
     * @param columnNamePattern a column name pattern; must match the column name
     *        as it is stored in the database
     * @return {@code ResultSet} - each row describes a stored procedure parameter or
     *      column
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getProcedureColumns(
            String catalog,
            String schemaPattern,
            String procedureNamePattern,
            String columnNamePattern)
            throws SQLException {

        if (wantsIsNull(procedureNamePattern)
                || wantsIsNull(columnNamePattern)) {
            return executeSelect("SYSTEM_PROCEDURECOLUMNS", "0=1");
        }

        catalog       = translateCatalog(catalog);
        schemaPattern = translateSchema(schemaPattern);

        StringBuilder sb = toQueryPrefix("SYSTEM_PROCEDURECOLUMNS");

        sb.append(and("PROCEDURE_CAT", "=", catalog))
          .append(and("PROCEDURE_SCHEM", "LIKE", schemaPattern))
          .append(and("PROCEDURE_NAME", "LIKE", procedureNamePattern))
          .append(and("COLUMN_NAME", "LIKE", columnNamePattern))
          .append(
              " ORDER BY PROCEDURE_CAT, PROCEDURE_SCHEM, PROCEDURE_NAME, SPECIFIC_NAME");

        return execute(sb.toString());
    }

    /**
     * Retrieves a description of the tables available in the given catalog.
     * Only table descriptions matching the catalog, schema, table
     * name and type criteria are returned.  They are ordered by
     * {@code TABLE_TYPE}, {@code TABLE_CAT},
     * {@code TABLE_SCHEM} and {@code TABLE_NAME}.
     * <P>
     * Each table description has the following columns:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be {@code null})
     *  <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be {@code null})
     *  <LI><B>TABLE_NAME</B> String {@code =>} table name
     *  <LI><B>TABLE_TYPE</B> String {@code =>} table type.  Typical types are "TABLE",
     *                  "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY",
     *                  "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     *  <LI><B>REMARKS</B> String {@code =>} explanatory comment on the table (may be {@code null})
     *  <LI><B>TYPE_CAT</B> String {@code =>} the types catalog (may be {@code null})
     *  <LI><B>TYPE_SCHEM</B> String {@code =>} the types schema (may be {@code null})
     *  <LI><B>TYPE_NAME</B> String {@code =>} type name (may be {@code null})
     *  <LI><B>SELF_REFERENCING_COL_NAME</B> String {@code =>} name of the designated
     *                  "identifier" column of a typed table (may be {@code null})
     *  <LI><B>REF_GENERATION</B> String {@code =>} specifies how values in
     *                  SELF_REFERENCING_COL_NAME are created. Values are
     *                  "SYSTEM", "USER", "DERIVED". (may be {@code null})
     *  </OL>
     *
     * <P><B>Note:</B> Some databases may not return information for
     * all tables.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * HSQLDB returns extra information on TEXT tables in the REMARKS column. <p>
     *
     * HSQLDB includes the JDBC3 columns TYPE_CAT, TYPE_SCHEM, TYPE_NAME and
     * SELF_REFERENCING_COL_NAME in anticipation of JDBC3 compliant tools. <p>
     *
     * Since 1.7.2, this feature is supported by default.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        {@code null} means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        {@code null} means that the schema name should not be used to narrow
     *        the search
     * @param tableNamePattern a table name pattern; must match the
     *        table name as it is stored in the database
     * @param types a list of table types, which must be from the list of table types
     *         returned from {@link #getTableTypes}, to include; {@code null} returns
     * all types
     * @return {@code ResultSet} - each row is a table description
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getTables(
            String catalog,
            String schemaPattern,
            String tableNamePattern,
            String[] types)
            throws SQLException {

        if (wantsIsNull(tableNamePattern)
                || (types != null && types.length == 0)) {
            return executeSelect("SYSTEM_TABLES", "0=1");
        }

        catalog       = translateCatalog(catalog);
        schemaPattern = translateSchema(schemaPattern);

        StringBuilder sb = toQueryPrefix("SYSTEM_TABLES");

        sb.append(and("TABLE_CAT", "=", catalog))
          .append(and("TABLE_SCHEM", "LIKE", schemaPattern))
          .append(and("TABLE_NAME", "LIKE", tableNamePattern));

        if (types == null) {

            // do not use to narrow search
        } else {

            // JDBC4 clarification:
            // fredt - we shouldn't impose this test as it breaks compatibility with tools

/*
            String[] allowedTypes = new String[] {
                "GLOBAL TEMPORARY", "SYSTEM TABLE", "TABLE", "VIEW"
            };
            int      illegalIndex = 0;
            String   illegalType  = null;

            outer_loop:
            for (int i = 0; i < types.length; i++) {
                for (int j = 0; j < allowedTypes.length; j++) {
                    if (allowedTypes[j].equals(types[i])) {
                        continue outer_loop;
                    }
                }

                illegalIndex = i;
                illegalType  = types[illegalIndex];

                break;
            }

            if (illegalType != null) {
                throw Util.sqlException(Trace.JDBC_INVALID_ARGUMENT,
                                        "types[" + illegalIndex + "]{@code =>}\""
                                        + illegalType + "\"");
            }
*/

            // end JDBC4 clarification
            //
            sb.append(" AND TABLE_TYPE IN (")
              .append(StringUtil.getList(types, ",", "'"))
              .append(')');
        }

        sb.append(" ORDER BY TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME");

        return execute(sb.toString());
    }

    /**
     * Retrieves the schema names available in this database.  The results
     * are ordered by {@code TABLE_CATALOG} and
     * {@code TABLE_SCHEM}.
     *
     * <P>The schema columns are:
     *  <OL>
     *  <LI><B>TABLE_SCHEM</B> String {@code =>} schema name
     *  <LI><B>TABLE_CATALOG</B> String {@code =>} catalog name (may be {@code null})
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with 1.8.0, the list of schemas is returned.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return a {@code ResultSet} object in which each row is a
     *         schema description
     * @throws SQLException if a database access error occurs
     *
     */
    public ResultSet getSchemas() throws SQLException {

        // By default, query already returns the result in contract order
        return executeSelect("SYSTEM_SCHEMAS", null);
    }

    /**
     * Retrieves the catalog names available in this database.  The results
     * are ordered by catalog name.
     *
     * <P>The catalog column is:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String {@code =>} catalog name
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Since 1.7.2, this feature is supported by default.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return a {@code ResultSet} object in which each row has a
     *         single {@code String} column that is a catalog name
     * @throws SQLException if a database access error occurs
     */
    public ResultSet getCatalogs() throws SQLException {

        String select =
            "SELECT CATALOG_NAME AS TABLE_CAT FROM INFORMATION_SCHEMA.INFORMATION_SCHEMA_CATALOG_NAME";

        return execute(select);
    }

    /**
     * Retrieves the table types available in this database.  The results
     * are ordered by table type.
     *
     * <P>The table type is:
     *  <OL>
     *  <LI><B>TABLE_TYPE</B> String {@code =>} table type.  Typical types are "TABLE",
     *                  "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY",
     *                  "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Since 1.7.1, HSQLDB reports: "TABLE", "VIEW" and "GLOBAL TEMPORARY"
     * types.
     *
     * Since 1.7.2, this feature is supported by default.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return a {@code ResultSet} object in which each row has a
     *         single {@code String} column that is a table type
     * @throws SQLException if a database access error occurs
     */
    public ResultSet getTableTypes() throws SQLException {

        // system table producer returns rows in contract order
        return executeSelect("SYSTEM_TABLETYPES", null);
    }

    /**
     * Retrieves a description of table columns available in
     * the specified catalog.
     *
     * <P>Only column descriptions matching the catalog, schema, table
     * and column name criteria are returned.  They are ordered by
     * {@code TABLE_CAT},{@code TABLE_SCHEM},
     * {@code TABLE_NAME}, and {@code ORDINAL_POSITION}.
     *
     * <P>Each column description has the following columns:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be {@code null})
     *  <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be {@code null})
     *  <LI><B>TABLE_NAME</B> String {@code =>} table name
     *  <LI><B>COLUMN_NAME</B> String {@code =>} column name
     *  <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
     *  <LI><B>TYPE_NAME</B> String {@code =>} Data source dependent type name,
     *  for a UDT the type name is fully qualified
     *  <LI><B>COLUMN_SIZE</B> int {@code =>} column size.
     *  <LI><B>BUFFER_LENGTH</B> is not used.
     *  <LI><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned for data types where
     * DECIMAL_DIGITS is not applicable.
     *  <LI><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
     *  <LI><B>NULLABLE</B> int {@code =>} is NULL allowed.
     *      <UL>
     *      <LI> columnNoNulls - might not allow {@code NULL} values
     *      <LI> columnNullable - definitely allows {@code NULL} values
     *      <LI> columnNullableUnknown - nullability unknown
     *      </UL>
     *  <LI><B>REMARKS</B> String {@code =>} comment describing column (may be {@code null})
     *  <LI><B>COLUMN_DEF</B> String {@code =>} default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be {@code null})
     *  <LI><B>SQL_DATA_TYPE</B> int {@code =>} unused
     *
     *        <p>HSQLDB-specific: CLI type from SQL 2003 Table 37,
     *        tables 6-9 Annex A1, and/or addendums in other
     *        documents, such as:<br>
     *        SQL 2003 Part 9: Management of External Data (SQL/MED) : DATALINK<br>
     *        SQL 2003 Part 14: XML-Related Specifications (SQL/XML) : XML
     *
     *  <LI><B>SQL_DATETIME_SUB</B> int {@code =>} unused (HSQLDB-specific: SQL 2003 CLI datetime/interval subcode)
     *  <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the
     *       maximum number of bytes in the column
     *  <LI><B>ORDINAL_POSITION</B> int {@code =>} index of column in table
     *      (starting at 1)
     *  <LI><B>IS_NULLABLE</B> String  {@code =>} ISO rules are used to determine the nullability for a column.
     *       <UL>
     *       <LI> YES           --- if the column can include NULLs
     *       <LI> NO            --- if the column cannot include NULLs
     *       <LI> empty string  --- if the nullability for the
     * column is unknown
     *       </UL>
     *  <LI><B>SCOPE_CATALOG</B> String {@code =>} catalog of table that is the scope
     *      of a reference attribute ({@code null} if DATA_TYPE isn't REF)
     *  <LI><B>SCOPE_SCHEMA</B> String {@code =>} schema of table that is the scope
     *      of a reference attribute ({@code null} if the DATA_TYPE isn't REF)
     *  <LI><B>SCOPE_TABLE</B> String {@code =>} table name that this the scope
     *      of a reference attribute ({@code null} if the DATA_TYPE isn't REF)
     *  <LI><B>SOURCE_DATA_TYPE</B> short {@code =>} source type of a distinct type or user-generated
     *      Ref type, SQL type from java.sql.Types ({@code null} if DATA_TYPE
     *      isn't DISTINCT or user-generated REF)
     *  <LI><B>IS_AUTOINCREMENT</B> String  {@code =>} Indicates whether this column is auto incremented
     *      <UL>
     *      <LI> YES           --- if the column is auto incremented
     *      <LI> NO            --- if the column is not auto incremented
     *      <LI> empty string  --- if it cannot be determined whether the column is auto incremented
     *      </UL>
     *  <LI><B>IS_GENERATEDCOLUMN</B> String  {@code =>} Indicates whether this is a generated column
     *      <UL>
     *      <LI> YES           --- if this a generated column
     *      <LI> NO            --- if this not a generated column
     *      <LI> empty string  --- if it cannot be determined whether this is a generated column
     *      </UL>
     *  </OL>
     *
     * <p>The COLUMN_SIZE column specifies the column size for the given column.
     * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
     * For datetime datatypes, this is the length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
     * this is the length in bytes. Null is returned for data types where the
     * column size is not applicable.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * This feature is supported by default.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        {@code null} means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        {@code null} means that the schema name should not be used to narrow
     *        the search
     * @param tableNamePattern a table name pattern; must match the
     *        table name as it is stored in the database
     * @param columnNamePattern a column name pattern; must match the column
     *        name as it is stored in the database
     * @return {@code ResultSet} - each row is a column description
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getColumns(
            String catalog,
            String schemaPattern,
            String tableNamePattern,
            String columnNamePattern)
            throws SQLException {

        if (wantsIsNull(tableNamePattern) || wantsIsNull(columnNamePattern)) {
            return executeSelect("SYSTEM_COLUMNS", "0=1");
        }

        catalog       = translateCatalog(catalog);
        schemaPattern = translateSchema(schemaPattern);

        StringBuilder sb = toQueryPrefix("SYSTEM_COLUMNS");

        sb.append(and("TABLE_CAT", "=", catalog))
          .append(and("TABLE_SCHEM", "LIKE", schemaPattern))
          .append(and("TABLE_NAME", "LIKE", tableNamePattern))
          .append(and("COLUMN_NAME", "LIKE", columnNamePattern))
          .append(" ORDER BY TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION");

        return execute(sb.toString());
    }

    /**
     * Retrieves a description of the access rights for a table's columns.
     *
     * <P>Only privileges matching the column name criteria are
     * returned.  They are ordered by COLUMN_NAME and PRIVILEGE.
     *
     * <P>Each privilege description has the following columns:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be {@code null})
     *  <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be {@code null})
     *  <LI><B>TABLE_NAME</B> String {@code =>} table name
     *  <LI><B>COLUMN_NAME</B> String {@code =>} column name
     *  <LI><B>GRANTOR</B> String {@code =>} grantor of access (may be {@code null})
     *  <LI><B>GRANTEE</B> String {@code =>} grantee of access
     *  <LI><B>PRIVILEGE</B> String {@code =>} name of access (SELECT,
     *      INSERT, UPDATE, REFERENCES, ...)
     *  <LI><B>IS_GRANTABLE</B> String {@code =>} "YES" if grantee is permitted
     *      to grant to others; "NO" if not; {@code null} if unknown
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * This feature is supported by default.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        {@code null} means that the catalog name should not be used to narrow
     *        the search
     * @param schema a schema name; must match the schema name as it is
     *        stored in the database; "" retrieves those without a schema;
     *        {@code null} means that the schema name should not be used to narrow
     *        the search
     * @param table a table name; must match the table name as it is
     *        stored in the database
     * @param columnNamePattern a column name pattern; must match the column
     *        name as it is stored in the database
     * @return {@code ResultSet} - each row is a column privilege description
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getColumnPrivileges(
            String catalog,
            String schema,
            String table,
            String columnNamePattern)
            throws SQLException {

        if (table == null) {
            throw JDBCUtil.nullArgument("table");
        }

/*
        if (wantsIsNull(columnNamePattern)) {
            return executeSelect("SYSTEM_COLUMNPRIVILEGES", "0=1");
        }
*/
        catalog = translateCatalog(catalog);
        schema  = translateSchema(schema);

        StringBuilder sb = new StringBuilder();

        sb.append("SELECT TABLE_CATALOG TABLE_CAT, TABLE_SCHEMA TABLE_SCHEM,")
          .append(
              "TABLE_NAME, COLUMN_NAME, GRANTOR, GRANTEE, PRIVILEGE_TYPE PRIVILEGE, IS_GRANTABLE ")
          .append("FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES WHERE TRUE ")
          .append(and("TABLE_CATALOG", "=", catalog))
          .append(and("TABLE_SCHEMA", "=", schema))
          .append(and("TABLE_NAME", "=", table))
          .append(and("COLUMN_NAME", "LIKE", columnNamePattern))
          .append(" ORDER BY COLUMN_NAME, PRIVILEGE");

        return execute(sb.toString());
    }

    /**
     * Retrieves a description of the access rights for each table available
     * in a catalog. Note that a table privilege applies to one or
     * more columns in the table. It would be wrong to assume that
     * this privilege applies to all columns (this may be true for
     * some systems but is not true for all.)
     *
     * <P>Only privileges matching the schema and table name
     * criteria are returned.  They are ordered by
     * {@code TABLE_CAT},
     * {@code TABLE_SCHEM}, {@code TABLE_NAME},
     * and {@code PRIVILEGE}.
     *
     * <P>Each privilege description has the following columns:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be {@code null})
     *  <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be {@code null})
     *  <LI><B>TABLE_NAME</B> String {@code =>} table name
     *  <LI><B>GRANTOR</B> String {@code =>} grantor of access (may be {@code null})
     *  <LI><B>GRANTEE</B> String {@code =>} grantee of access
     *  <LI><B>PRIVILEGE</B> String {@code =>} name of access (SELECT,
     *      INSERT, UPDATE, REFERENCES, ...)
     *  <LI><B>IS_GRANTABLE</B> String {@code =>} "YES" if grantee is permitted
     *      to grant to others; "NO" if not; {@code null} if unknown
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        {@code null} means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        {@code null} means that the schema name should not be used to narrow
     *        the search
     * @param tableNamePattern a table name pattern; must match the
     *        table name as it is stored in the database
     * @return {@code ResultSet} - each row is a table privilege description
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getTablePrivileges(
            String catalog,
            String schemaPattern,
            String tableNamePattern)
            throws SQLException {

        catalog       = translateCatalog(catalog);
        schemaPattern = translateSchema(schemaPattern);

        StringBuilder sb = new StringBuilder();

        sb.append("SELECT TABLE_CATALOG TABLE_CAT, TABLE_SCHEMA TABLE_SCHEM,")
          .append(
              "TABLE_NAME, GRANTOR, GRANTEE, PRIVILEGE_TYPE PRIVILEGE, IS_GRANTABLE ")
          .append("FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES WHERE TRUE ")
          .append(and("TABLE_CATALOG", "=", catalog))
          .append(and("TABLE_SCHEMA", "LIKE", schemaPattern))
          .append(and("TABLE_NAME", "LIKE", tableNamePattern))
          .append(" ORDER BY TABLE_SCHEM, TABLE_NAME, PRIVILEGE");

/*
        if (wantsIsNull(tableNamePattern)) {
            return executeSelect("SYSTEM_TABLEPRIVILEGES", "0=1");
        }
*/
        return execute(sb.toString());
    }

    /**
     * Retrieves a description of a table's optimal set of columns that
     * uniquely identifies a row. They are ordered by SCOPE.
     *
     * <P>Each column description has the following columns:
     *  <OL>
     *  <LI><B>SCOPE</B> short {@code =>} actual scope of result
     *      <UL>
     *      <LI> bestRowTemporary - very temporary, while using row
     *      <LI> bestRowTransaction - valid for remainder of current transaction
     *      <LI> bestRowSession - valid for remainder of current session
     *      </UL>
     *  <LI><B>COLUMN_NAME</B> String {@code =>} column name
     *  <LI><B>DATA_TYPE</B> int {@code =>} SQL data type from java.sql.Types
     *  <LI><B>TYPE_NAME</B> String {@code =>} Data source dependent type name,
     *  for a UDT the type name is fully qualified
     *  <LI><B>COLUMN_SIZE</B> int {@code =>} precision
     *  <LI><B>BUFFER_LENGTH</B> int {@code =>} not used
     *  <LI><B>DECIMAL_DIGITS</B> short  {@code =>} scale - Null is returned for data types where
     * DECIMAL_DIGITS is not applicable.
     *  <LI><B>PSEUDO_COLUMN</B> short {@code =>} is this a pseudo column
     *      like an Oracle ROWID
     *      <UL>
     *      <LI> bestRowUnknown - may or may not be pseudo column
     *      <LI> bestRowNotPseudo - is NOT a pseudo column
     *      <LI> bestRowPseudo - is a pseudo column
     *      </UL>
     *  </OL>
     *
     * <p>The COLUMN_SIZE column represents the specified column size for the given column.
     * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
     * For datetime datatypes, this is the length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
     * this is the length in bytes. Null is returned for data types where the
     * column size is not applicable.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * If the name of a column is defined in the database without double
     * quotes, an all-uppercase name must be specified when calling this
     * method. Otherwise, the name must be specified in the exact case of
     * the column definition in the database.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        {@code null} means that the catalog name should not be used to narrow
     *        the search
     * @param schema a schema name; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        {@code null} means that the schema name should not be used to narrow
     *        the search
     * @param table a table name; must match the table name as it is stored
     *        in the database
     * @param scope the scope of interest; use same values as SCOPE
     * @param nullable include columns that are nullable.
     * @return {@code ResultSet} - each row is a column description
     * @throws SQLException if a database access error occurs
     */
    public ResultSet getBestRowIdentifier(
            String catalog,
            String schema,
            String table,
            int scope,
            boolean nullable)
            throws SQLException {

        if (table == null) {
            throw JDBCUtil.nullArgument("table");
        }

        String scopeIn;

        switch (scope) {

            case bestRowTemporary :
                scopeIn = BRI_TEMPORARY_SCOPE_IN_LIST;
                break;

            case bestRowTransaction :
                scopeIn = BRI_TRANSACTION_SCOPE_IN_LIST;
                break;

            case bestRowSession :
                scopeIn = BRI_SESSION_SCOPE_IN_LIST;
                break;

            default :
                throw JDBCUtil.invalidArgument("scope");
        }

        catalog = translateCatalog(catalog);
        schema  = translateSchema(schema);

        Integer       Nullable = (nullable)
                                 ? null
                                 : INT_COLUMNS_NO_NULLS;
        StringBuilder sb       = toQueryPrefix("SYSTEM_BESTROWIDENTIFIER");

        sb.append(and("TABLE_CAT", "=", catalog))
          .append(and("TABLE_SCHEM", "=", schema))
          .append(and("TABLE_NAME", "=", table))
          .append(and("NULLABLE", "=", Nullable))
          .append(" AND SCOPE IN ")
          .append(scopeIn)
          .append(" ORDER BY SCOPE");

        return execute(sb.toString());
    }

    /**
     * Retrieves a description of a table's columns that are automatically
     * updated when any value in a row is updated.  They are
     * unordered.
     *
     * <P>Each column description has the following columns:
     *  <OL>
     *  <LI><B>SCOPE</B> short {@code =>} is not used
     *  <LI><B>COLUMN_NAME</B> String {@code =>} column name
     *  <LI><B>DATA_TYPE</B> int {@code =>} SQL data type from {@code java.sql.Types}
     *  <LI><B>TYPE_NAME</B> String {@code =>} Data source-dependent type name
     *  <LI><B>COLUMN_SIZE</B> int {@code =>} precision
     *  <LI><B>BUFFER_LENGTH</B> int {@code =>} length of column value in bytes
     *  <LI><B>DECIMAL_DIGITS</B> short  {@code =>} scale - Null is returned for data types where
     * DECIMAL_DIGITS is not applicable.
     *  <LI><B>PSEUDO_COLUMN</B> short {@code =>} whether this is pseudo column
     *      like an Oracle ROWID
     *      <UL>
     *      <LI> versionColumnUnknown - may or may not be pseudo column
     *      <LI> versionColumnNotPseudo - is NOT a pseudo column
     *      <LI> versionColumnPseudo - is a pseudo column
     *      </UL>
     *  </OL>
     *
     * <p>The COLUMN_SIZE column represents the specified column size for the given column.
     * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
     * For datetime datatypes, this is the length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
     * this is the length in bytes. Null is returned for data types where the
     * column size is not applicable.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.5.0 and later returns information on auto-updated
     * TIMESTAMP columns defined with ON UPDATE CURRENT_TIMESTAMP, and the
     * columns of SYSTEM_TIME periods. Columns defined as GENERATED AS IDENTITY,
     * SEQUENCE, or an expression are not returned as they are not always
     * automatically updated when other columns in a row are updated.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        {@code null} means that the catalog name should not be used to narrow
     *        the search
     * @param schema a schema name; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        {@code null} means that the schema name should not be used to narrow
     *        the search
     * @param table a table name; must match the table name as it is stored
     *        in the database
     * @return a {@code ResultSet} object in which each row is a
     *         column description
     * @throws SQLException if a database access error occurs
     */
    public ResultSet getVersionColumns(
            String catalog,
            String schema,
            String table)
            throws SQLException {

        if (table == null) {
            throw JDBCUtil.nullArgument("table");
        }

        catalog = translateCatalog(catalog);
        schema  = translateSchema(schema);

        StringBuilder sb = toQueryPrefix("SYSTEM_VERSIONCOLUMNS");

        sb.append(and("TABLE_CAT", "=", catalog))
          .append(and("TABLE_SCHEM", "=", schema))
          .append(and("TABLE_NAME", "=", table));

        // result does not need to be ordered
        return execute(sb.toString());
    }

    /**
     * Retrieves a description of the given table's primary key columns.  They
     * are ordered by COLUMN_NAME.
     *
     * <P>Each primary key column description has the following columns:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be {@code null})
     *  <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be {@code null})
     *  <LI><B>TABLE_NAME</B> String {@code =>} table name
     *  <LI><B>COLUMN_NAME</B> String {@code =>} column name
     *  <LI><B>KEY_SEQ</B> short {@code =>} sequence number within primary key( a value
     *  of 1 represents the first column of the primary key, a value of 2 would
     *  represent the second column within the primary key).
     *  <LI><B>PK_NAME</B> String {@code =>} primary key name (may be {@code null})
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        {@code null} means that the catalog name should not be used to narrow
     *        the search
     * @param schema a schema name; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        {@code null} means that the schema name should not be used to narrow
     *        the search
     * @param table a table name; must match the table name as it is stored
     *        in the database
     * @return {@code ResultSet} - each row is a primary key column description
     * @throws SQLException if a database access error occurs
     * @see #supportsMixedCaseQuotedIdentifiers
     * @see #storesUpperCaseIdentifiers
     */
    public ResultSet getPrimaryKeys(
            String catalog,
            String schema,
            String table)
            throws SQLException {

        if (table == null) {
            throw JDBCUtil.nullArgument("table");
        }

        catalog = translateCatalog(catalog);
        schema  = translateSchema(schema);

        StringBuilder sb = toQueryPrefix("SYSTEM_PRIMARYKEYS");

        sb.append(and("TABLE_CAT", "=", catalog))
          .append(and("TABLE_SCHEM", "=", schema))
          .append(and("TABLE_NAME", "=", table))
          .append(" ORDER BY COLUMN_NAME");

        return execute(sb.toString());
    }

    /**
     * Retrieves a description of the primary key columns that are
     * referenced by the given table's foreign key columns (the primary keys
     * imported by a table).  They are ordered by PKTABLE_CAT,
     * PKTABLE_SCHEM, PKTABLE_NAME, and KEY_SEQ.
     *
     * <P>Each primary key column description has the following columns:
     *  <OL>
     *  <LI><B>PKTABLE_CAT</B> String {@code =>} primary key table catalog
     *      being imported (may be {@code null})
     *  <LI><B>PKTABLE_SCHEM</B> String {@code =>} primary key table schema
     *      being imported (may be {@code null})
     *  <LI><B>PKTABLE_NAME</B> String {@code =>} primary key table name
     *      being imported
     *  <LI><B>PKCOLUMN_NAME</B> String {@code =>} primary key column name
     *      being imported
     *  <LI><B>FKTABLE_CAT</B> String {@code =>} foreign key table catalog (may be {@code null})
     *  <LI><B>FKTABLE_SCHEM</B> String {@code =>} foreign key table schema (may be {@code null})
     *  <LI><B>FKTABLE_NAME</B> String {@code =>} foreign key table name
     *  <LI><B>FKCOLUMN_NAME</B> String {@code =>} foreign key column name
     *  <LI><B>KEY_SEQ</B> short {@code =>} sequence number within a foreign key( a value
     *  of 1 represents the first column of the foreign key, a value of 2 would
     *  represent the second column within the foreign key).
     *  <LI><B>UPDATE_RULE</B> short {@code =>} What happens to a
     *       foreign key when the primary key is updated:
     *      <UL>
     *      <LI> importedNoAction - do not allow update of primary
     *               key if it has been imported
     *      <LI> importedKeyCascade - change imported key to agree
     *               with primary key update
     *      <LI> importedKeySetNull - change imported key to {@code NULL}
     *               if its primary key has been updated
     *      <LI> importedKeySetDefault - change imported key to default values
     *               if its primary key has been updated
     *      <LI> importedKeyRestrict - same as importedKeyNoAction
     *                                 (for ODBC 2.x compatibility)
     *      </UL>
     *  <LI><B>DELETE_RULE</B> short {@code =>} What happens to
     *      the foreign key when primary is deleted.
     *      <UL>
     *      <LI> importedKeyNoAction - do not allow delete of primary
     *               key if it has been imported
     *      <LI> importedKeyCascade - delete rows that import a deleted key
     *      <LI> importedKeySetNull - change imported key to NULL if
     *               its primary key has been deleted
     *      <LI> importedKeyRestrict - same as importedKeyNoAction
     *                                 (for ODBC 2.x compatibility)
     *      <LI> importedKeySetDefault - change imported key to default if
     *               its primary key has been deleted
     *      </UL>
     *  <LI><B>FK_NAME</B> String {@code =>} foreign key name (may be {@code null})
     *  <LI><B>PK_NAME</B> String {@code =>} primary key name (may be {@code null})
     *  <LI><B>DEFERRABILITY</B> short {@code =>} can the evaluation of foreign key
     *      constraints be deferred until commit
     *      <UL>
     *      <LI> importedKeyInitiallyDeferred - see SQL92 for definition
     *      <LI> importedKeyInitiallyImmediate - see SQL92 for definition
     *      <LI> importedKeyNotDeferrable - see SQL92 for definition
     *      </UL>
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        {@code null} means that the catalog name should not be used to narrow
     *        the search
     * @param schema a schema name; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        {@code null} means that the schema name should not be used to narrow
     *        the search
     * @param table a table name; must match the table name as it is stored
     *        in the database
     * @return {@code ResultSet} - each row is a primary key column description
     * @throws SQLException if a database access error occurs
     * @see #getExportedKeys
     * @see #supportsMixedCaseQuotedIdentifiers
     * @see #storesUpperCaseIdentifiers
     */
    public ResultSet getImportedKeys(
            String catalog,
            String schema,
            String table)
            throws SQLException {

        if (table == null) {
            throw JDBCUtil.nullArgument("table");
        }

        catalog = translateCatalog(catalog);
        schema  = translateSchema(schema);

        StringBuilder sb = toQueryPrefix("SYSTEM_CROSSREFERENCE");

        sb.append(and("FKTABLE_CAT", "=", catalog))
          .append(and("FKTABLE_SCHEM", "=", schema))
          .append(and("FKTABLE_NAME", "=", table))
          .append(
              " ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ");

        return execute(sb.toString());
    }

    /**
     * Retrieves a description of the foreign key columns that reference the
     * given table's primary key columns (the foreign keys exported by a
     * table).  They are ordered by FKTABLE_CAT, FKTABLE_SCHEM,
     * FKTABLE_NAME, and KEY_SEQ.
     *
     * <P>Each foreign key column description has the following columns:
     *  <OL>
     *  <LI><B>PKTABLE_CAT</B> String {@code =>} primary key table catalog (may be {@code null})
     *  <LI><B>PKTABLE_SCHEM</B> String {@code =>} primary key table schema (may be {@code null})
     *  <LI><B>PKTABLE_NAME</B> String {@code =>} primary key table name
     *  <LI><B>PKCOLUMN_NAME</B> String {@code =>} primary key column name
     *  <LI><B>FKTABLE_CAT</B> String {@code =>} foreign key table catalog (may be {@code null})
     *      being exported (may be {@code null})
     *  <LI><B>FKTABLE_SCHEM</B> String {@code =>} foreign key table schema (may be {@code null})
     *      being exported (may be {@code null})
     *  <LI><B>FKTABLE_NAME</B> String {@code =>} foreign key table name
     *      being exported
     *  <LI><B>FKCOLUMN_NAME</B> String {@code =>} foreign key column name
     *      being exported
     *  <LI><B>KEY_SEQ</B> short {@code =>} sequence number within foreign key( a value
     *  of 1 represents the first column of the foreign key, a value of 2 would
     *  represent the second column within the foreign key).
     *  <LI><B>UPDATE_RULE</B> short {@code =>} What happens to
     *       foreign key when primary is updated:
     *      <UL>
     *      <LI> importedNoAction - do not allow update of primary
     *               key if it has been imported
     *      <LI> importedKeyCascade - change imported key to agree
     *               with primary key update
     *      <LI> importedKeySetNull - change imported key to {@code NULL} if
     *               its primary key has been updated
     *      <LI> importedKeySetDefault - change imported key to default values
     *               if its primary key has been updated
     *      <LI> importedKeyRestrict - same as importedKeyNoAction
     *                                 (for ODBC 2.x compatibility)
     *      </UL>
     *  <LI><B>DELETE_RULE</B> short {@code =>} What happens to
     *      the foreign key when primary is deleted.
     *      <UL>
     *      <LI> importedKeyNoAction - do not allow delete of primary
     *               key if it has been imported
     *      <LI> importedKeyCascade - delete rows that import a deleted key
     *      <LI> importedKeySetNull - change imported key to {@code NULL} if
     *               its primary key has been deleted
     *      <LI> importedKeyRestrict - same as importedKeyNoAction
     *                                 (for ODBC 2.x compatibility)
     *      <LI> importedKeySetDefault - change imported key to default if
     *               its primary key has been deleted
     *      </UL>
     *  <LI><B>FK_NAME</B> String {@code =>} foreign key name (may be {@code null})
     *  <LI><B>PK_NAME</B> String {@code =>} primary key name (may be {@code null})
     *  <LI><B>DEFERRABILITY</B> short {@code =>} can the evaluation of foreign key
     *      constraints be deferred until commit
     *      <UL>
     *      <LI> importedKeyInitiallyDeferred - see SQL92 for definition
     *      <LI> importedKeyInitiallyImmediate - see SQL92 for definition
     *      <LI> importedKeyNotDeferrable - see SQL92 for definition
     *      </UL>
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in this database; "" retrieves those without a catalog;
     *        {@code null} means that the catalog name should not be used to narrow
     *        the search
     * @param schema a schema name; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        {@code null} means that the schema name should not be used to narrow
     *        the search
     * @param table a table name; must match the table name as it is stored
     *        in this database
     * @return a {@code ResultSet} object in which each row is a
     *         foreign key column description
     * @throws SQLException if a database access error occurs
     * @see #getImportedKeys
     * @see #supportsMixedCaseQuotedIdentifiers
     * @see #storesUpperCaseIdentifiers
     */
    public ResultSet getExportedKeys(
            String catalog,
            String schema,
            String table)
            throws SQLException {

        if (table == null) {
            throw JDBCUtil.nullArgument("table");
        }

        catalog = translateCatalog(catalog);
        schema  = translateSchema(schema);

        StringBuilder sb = toQueryPrefix("SYSTEM_CROSSREFERENCE");

        sb.append(and("PKTABLE_CAT", "=", catalog))
          .append(and("PKTABLE_SCHEM", "=", schema))
          .append(and("PKTABLE_NAME", "=", table))
          .append(
              " ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ");

        return execute(sb.toString());
    }

    /**
     * Retrieves a description of the foreign key columns in the given foreign key
     * table that reference the primary key or the columns representing a unique constraint of the  parent table (could be the same or a different table).
     * The number of columns returned from the parent table must match the number of
     * columns that make up the foreign key.  They
     * are ordered by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, and
     * KEY_SEQ.
     *
     * <P>Each foreign key column description has the following columns:
     *  <OL>
     *  <LI><B>PKTABLE_CAT</B> String {@code =>} parent key table catalog (may be {@code null})
     *  <LI><B>PKTABLE_SCHEM</B> String {@code =>} parent key table schema (may be {@code null})
     *  <LI><B>PKTABLE_NAME</B> String {@code =>} parent key table name
     *  <LI><B>PKCOLUMN_NAME</B> String {@code =>} parent key column name
     *  <LI><B>FKTABLE_CAT</B> String {@code =>} foreign key table catalog (may be {@code null})
     *      being exported (may be {@code null})
     *  <LI><B>FKTABLE_SCHEM</B> String {@code =>} foreign key table schema (may be {@code null})
     *      being exported (may be {@code null})
     *  <LI><B>FKTABLE_NAME</B> String {@code =>} foreign key table name
     *      being exported
     *  <LI><B>FKCOLUMN_NAME</B> String {@code =>} foreign key column name
     *      being exported
     *  <LI><B>KEY_SEQ</B> short {@code =>} sequence number within foreign key( a value
     *  of 1 represents the first column of the foreign key, a value of 2 would
     *  represent the second column within the foreign key).
     *  <LI><B>UPDATE_RULE</B> short {@code =>} What happens to
     *       foreign key when parent key is updated:
     *      <UL>
     *      <LI> importedNoAction - do not allow update of parent
     *               key if it has been imported
     *      <LI> importedKeyCascade - change imported key to agree
     *               with parent key update
     *      <LI> importedKeySetNull - change imported key to {@code NULL} if
     *               its parent key has been updated
     *      <LI> importedKeySetDefault - change imported key to default values
     *               if its parent key has been updated
     *      <LI> importedKeyRestrict - same as importedKeyNoAction
     *                                 (for ODBC 2.x compatibility)
     *      </UL>
     *  <LI><B>DELETE_RULE</B> short {@code =>} What happens to
     *      the foreign key when parent key is deleted.
     *      <UL>
     *      <LI> importedKeyNoAction - do not allow delete of parent
     *               key if it has been imported
     *      <LI> importedKeyCascade - delete rows that import a deleted key
     *      <LI> importedKeySetNull - change imported key to {@code NULL} if
     *               its primary key has been deleted
     *      <LI> importedKeyRestrict - same as importedKeyNoAction
     *                                 (for ODBC 2.x compatibility)
     *      <LI> importedKeySetDefault - change imported key to default if
     *               its parent key has been deleted
     *      </UL>
     *  <LI><B>FK_NAME</B> String {@code =>} foreign key name (may be {@code null})
     *  <LI><B>PK_NAME</B> String {@code =>} parent key name (may be {@code null})
     *  <LI><B>DEFERRABILITY</B> short {@code =>} can the evaluation of foreign key
     *      constraints be deferred until commit
     *      <UL>
     *      <LI> importedKeyInitiallyDeferred - see SQL92 for definition
     *      <LI> importedKeyInitiallyImmediate - see SQL92 for definition
     *      <LI> importedKeyNotDeferrable - see SQL92 for definition
     *      </UL>
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parentCatalog a catalog name; must match the catalog name
     * as it is stored in the database; "" retrieves those without a
     * catalog; {@code null} means drop catalog name from the selection criteria
     * @param parentSchema a schema name; must match the schema name as
     * it is stored in the database; "" retrieves those without a schema;
     * {@code null} means drop schema name from the selection criteria
     * @param parentTable the name of the table that exports the key; must match
     * the table name as it is stored in the database
     * @param foreignCatalog a catalog name; must match the catalog name as
     * it is stored in the database; "" retrieves those without a
     * catalog; {@code null} means drop catalog name from the selection criteria
     * @param foreignSchema a schema name; must match the schema name as it
     * is stored in the database; "" retrieves those without a schema;
     * {@code null} means drop schema name from the selection criteria
     * @param foreignTable the name of the table that imports the key; must match
     * the table name as it is stored in the database
     * @return {@code ResultSet} - each row is a foreign key column description
     * @throws SQLException if a database access error occurs
     * @see #getImportedKeys
     * @see #supportsMixedCaseQuotedIdentifiers
     * @see #storesUpperCaseIdentifiers
     */
    public ResultSet getCrossReference(
            String parentCatalog,
            String parentSchema,
            String parentTable,
            String foreignCatalog,
            String foreignSchema,
            String foreignTable)
            throws SQLException {

        if (parentTable == null) {
            throw JDBCUtil.nullArgument("parentTable");
        }

        if (foreignTable == null) {
            throw JDBCUtil.nullArgument("foreignTable");
        }

        parentCatalog  = translateCatalog(parentCatalog);
        foreignCatalog = translateCatalog(foreignCatalog);
        parentSchema   = translateSchema(parentSchema);
        foreignSchema  = translateSchema(foreignSchema);

        StringBuilder sb = toQueryPrefix("SYSTEM_CROSSREFERENCE");

        sb.append(and("PKTABLE_CAT", "=", parentCatalog))
          .append(and("PKTABLE_SCHEM", "=", parentSchema))
          .append(and("PKTABLE_NAME", "=", parentTable))
          .append(and("FKTABLE_CAT", "=", foreignCatalog))
          .append(and("FKTABLE_SCHEM", "=", foreignSchema))
          .append(and("FKTABLE_NAME", "=", foreignTable))
          .append(
              " ORDER BY FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, KEY_SEQ");

        return execute(sb.toString());
    }

    /**
     * Retrieves a description of all the data types supported by
     * this database. They are ordered by DATA_TYPE and then by how
     * closely the data type maps to the corresponding JDBC SQL type.
     *
     * <P>If the database supports SQL distinct types, then getTypeInfo() will return
     * a single row with a TYPE_NAME of DISTINCT and a DATA_TYPE of Types.DISTINCT.
     * If the database supports SQL structured types, then getTypeInfo() will return
     * a single row with a TYPE_NAME of STRUCT and a DATA_TYPE of Types.STRUCT.
     *
     * <P>If SQL distinct or structured types are supported, then information on the
     * individual types may be obtained from the getUDTs() method.
     *
     *
     * <P>Each type description has the following columns:
     *  <OL>
     *  <LI><B>TYPE_NAME</B> String {@code =>} Type name
     *  <LI><B>DATA_TYPE</B> int {@code =>} SQL data type from java.sql.Types
     *  <LI><B>PRECISION</B> int {@code =>} maximum precision
     *  <LI><B>LITERAL_PREFIX</B> String {@code =>} prefix used to quote a literal
     *      (may be {@code null})
     *  <LI><B>LITERAL_SUFFIX</B> String {@code =>} suffix used to quote a literal
     *  (may be {@code null})
     *  <LI><B>CREATE_PARAMS</B> String {@code =>} parameters used in creating
     *      the type (may be {@code null})
     *  <LI><B>NULLABLE</B> short {@code =>} can you use NULL for this type.
     *      <UL>
     *      <LI> typeNoNulls - does not allow NULL values
     *      <LI> typeNullable - allows NULL values
     *      <LI> typeNullableUnknown - nullability unknown
     *      </UL>
     *  <LI><B>CASE_SENSITIVE</B> boolean{@code =>} is it case sensitive.
     *  <LI><B>SEARCHABLE</B> short {@code =>} can you use "WHERE" based on this type:
     *      <UL>
     *      <LI> typePredNone - No support
     *      <LI> typePredChar - Only supported with WHERE .. LIKE
     *      <LI> typePredBasic - Supported except for WHERE .. LIKE
     *      <LI> typeSearchable - Supported for all WHERE ..
     *      </UL>
     *  <LI><B>UNSIGNED_ATTRIBUTE</B> boolean {@code =>} is it unsigned.
     *  <LI><B>FIXED_PREC_SCALE</B> boolean {@code =>} can it be a money value.
     *  <LI><B>AUTO_INCREMENT</B> boolean {@code =>} can it be used for an
     *      auto-increment value.
     *  <LI><B>LOCAL_TYPE_NAME</B> String {@code =>} localized version of type name
     *      (may be {@code null})
     *  <LI><B>MINIMUM_SCALE</B> short {@code =>} minimum scale supported
     *  <LI><B>MAXIMUM_SCALE</B> short {@code =>} maximum scale supported
     *  <LI><B>SQL_DATA_TYPE</B> int {@code =>} unused
     *  <LI><B>SQL_DATETIME_SUB</B> int {@code =>} unused
     *  <LI><B>NUM_PREC_RADIX</B> int {@code =>} usually 2 or 10
     *  </OL>
     *
     * <p>The PRECISION column represents the maximum column size that the server supports for the given datatype.
     * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
     * For datetime datatypes, this is the length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
     * this is the length in bytes. Null is returned for data types where the
     * column size is not applicable.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * This feature is supported.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return a {@code ResultSet} object in which each row is an SQL
     *         type description
     * @throws SQLException if a database access error occurs
     */
    public ResultSet getTypeInfo() throws SQLException {

        // system table producer returns rows in contract order
        return executeSelect("SYSTEM_TYPEINFO", null);
    }

    /**
     * Retrieves a description of the given table's indices and statistics. They are
     * ordered by NON_UNIQUE, TYPE, INDEX_NAME, and ORDINAL_POSITION.
     *
     * <P>Each index column description has the following columns:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be {@code null})
     *  <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be {@code null})
     *  <LI><B>TABLE_NAME</B> String {@code =>} table name
     *  <LI><B>NON_UNIQUE</B> boolean {@code =>} Can index values be non-unique.
     *      false when TYPE is tableIndexStatistic
     *  <LI><B>INDEX_QUALIFIER</B> String {@code =>} index catalog (may be {@code null});
     *      {@code null} when TYPE is tableIndexStatistic
     *  <LI><B>INDEX_NAME</B> String {@code =>} index name; {@code null} when TYPE is
     *      tableIndexStatistic
     *  <LI><B>TYPE</B> short {@code =>} index type:
     *      <UL>
     *      <LI> tableIndexStatistic - this identifies table statistics that are
     *           returned in conjunction with a table's index descriptions
     *      <LI> tableIndexClustered - this is a clustered index
     *      <LI> tableIndexHashed - this is a hashed index
     *      <LI> tableIndexOther - this is some other style of index
     *      </UL>
     *  <LI><B>ORDINAL_POSITION</B> short {@code =>} column sequence number
     *      within index; zero when TYPE is tableIndexStatistic
     *  <LI><B>COLUMN_NAME</B> String {@code =>} column name; {@code null} when TYPE is
     *      tableIndexStatistic
     *  <LI><B>ASC_OR_DESC</B> String {@code =>} column sort sequence, "A" {@code =>} ascending,
     *      "D" {@code =>} descending, may be {@code null} if sort sequence is not supported;
     *      {@code null} when TYPE is tableIndexStatistic
     *  <LI><B>CARDINALITY</B> long {@code =>} When TYPE is tableIndexStatistic, then
     *      this is the number of rows in the table; otherwise, it is the
     *      number of unique values in the index.
     *  <LI><B>PAGES</B> long {@code =>} When TYPE is  tableIndexStatistic then
     *      this is the number of pages used for the table, otherwise it
     *      is the number of pages used for the current index.
     *  <LI><B>FILTER_CONDITION</B> String {@code =>} Filter condition, if any.
     *      (may be {@code null})
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in this database; "" retrieves those without a catalog;
     *        {@code null} means that the catalog name should not be used to narrow
     *        the search
     * @param schema a schema name; must match the schema name
     *        as it is stored in this database; "" retrieves those without a schema;
     *        {@code null} means that the schema name should not be used to narrow
     *        the search
     * @param table a table name; must match the table name as it is stored
     *        in this database
     * @param unique when true, return only indices for unique values;
     *     when false, return indices regardless of whether unique or not
     * @param approximate when true, result is allowed to reflect approximate
     *     or out of data values; when false, results are requested to be
     *     accurate
     * @return {@code ResultSet} - each row is an index column description
     * @throws SQLException if a database access error occurs
     */
    public ResultSet getIndexInfo(
            String catalog,
            String schema,
            String table,
            boolean unique,
            boolean approximate)
            throws SQLException {

        if (table == null) {
            throw JDBCUtil.nullArgument("table");
        }

        catalog = translateCatalog(catalog);
        schema  = translateSchema(schema);

        Boolean       nu = (unique)
                           ? Boolean.FALSE
                           : null;
        StringBuilder sb = toQueryPrefix("SYSTEM_INDEXINFO");

        sb.append(and("TABLE_CAT", "=", catalog))
          .append(and("TABLE_SCHEM", "=", schema))
          .append(and("TABLE_NAME", "=", table))
          .append(and("NON_UNIQUE", "=", nu))
          .append(" ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION");

        return execute(sb.toString());
    }

    //--------------------------JDBC 2.0-----------------------------

    /**
     * Retrieves whether this database supports the given result set type.
     *
     * @param type defined in {@code java.sql.ResultSet}
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @see JDBCConnection
     * @since  JDK 1.2
     */
    public boolean supportsResultSetType(int type) throws SQLException {
        return (type == ResultSet.TYPE_FORWARD_ONLY
                || type == ResultSet.TYPE_SCROLL_INSENSITIVE);
    }

    /**
     * Retrieves whether this database supports the given concurrency type
     * in combination with the given result set type.
     *
     * @param type defined in {@code java.sql.ResultSet}
     * @param concurrency type defined in {@code java.sql.ResultSet}
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @see JDBCConnection
     * @since  JDK 1.2
     */
    public boolean supportsResultSetConcurrency(
            int type,
            int concurrency)
            throws SQLException {
        return supportsResultSetType(type)
               && (concurrency == ResultSet.CONCUR_READ_ONLY
                   || concurrency == ResultSet.CONCUR_UPDATABLE);
    }

    /**
     *
     * Retrieves whether for the given type of {@code ResultSet} object,
     * the result set's own updates are visible.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     * Updates to ResultSet rows are not visible after moving from the updated
     * row.
     * </div>
     * <!-- end release-specific documentation -->
     * @param type the {@code ResultSet} type; one of
     *        {@code ResultSet.TYPE_FORWARD_ONLY},
     *        {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
     *        {@code ResultSet.TYPE_SCROLL_SENSITIVE}
     * @return {@code true} if updates are visible for the given result set type;
     *        {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @since JDK 1.2
     */
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * Retrieves whether a result set's own deletes are visible.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Rows deleted from the ResultSet are still visible after moving from the
     * deleted row.
     * </div>
     * <!-- end release-specific documentation -->
     * @param type the {@code ResultSet} type; one of
     *        {@code ResultSet.TYPE_FORWARD_ONLY},
     *        {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
     *        {@code ResultSet.TYPE_SCROLL_SENSITIVE}
     * @return {@code true} if deletes are visible for the given result set type;
     *        {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @since JDK 1.2
     */
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * Retrieves whether a result set's own inserts are visible.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Rows added to a ResultSet are not visible after moving from the
     * insert row; this method always returns {@code false}.
     * </div>
     * <!-- end release-specific documentation -->
     * @param type the {@code ResultSet} type; one of
     *        {@code ResultSet.TYPE_FORWARD_ONLY},
     *        {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
     *        {@code ResultSet.TYPE_SCROLL_SENSITIVE}
     * @return {@code true} if inserts are visible for the given result set type;
     *        {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @since JDK 1.2
     */
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * Retrieves whether updates made by others are visible.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Updates made by other connections or the same connection while the
     * ResultSet is open are not visible in the ResultSet.
     * </div>
     * <!-- end release-specific documentation -->
     * @param type the {@code ResultSet} type; one of
     *        {@code ResultSet.TYPE_FORWARD_ONLY},
     *        {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
     *        {@code ResultSet.TYPE_SCROLL_SENSITIVE}
     * @return {@code true} if updates made by others
     *        are visible for the given result set type;
     *        {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @since JDK 1.2
     */
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * Retrieves whether deletes made by others are visible.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Deletes made by other connections or the same connection while the
     * ResultSet is open are not visible in the ResultSet.
     * </div>
     * <!-- end release-specific documentation -->
     * @param type the {@code ResultSet} type; one of
     *        {@code ResultSet.TYPE_FORWARD_ONLY},
     *        {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
     *        {@code ResultSet.TYPE_SCROLL_SENSITIVE}
     * @return {@code true} if deletes made by others
     *        are visible for the given result set type;
     *        {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @since JDK 1.2
     */
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * Retrieves whether inserts made by others are visible.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Inserts made by other connections or the same connection while the
     * ResultSet is open are not visible in the ResultSet.
     * </div>
     * <!-- end release-specific documentation -->
     * @param type the {@code ResultSet} type; one of
     *        {@code ResultSet.TYPE_FORWARD_ONLY},
     *        {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
     *        {@code ResultSet.TYPE_SCROLL_SENSITIVE}
     * @return {@code true} if inserts made by others
     *         are visible for the given result set type;
     *         {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @since JDK 1.2
     */
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    /**
     * Retrieves whether or not a visible row update can be detected by
     * calling the method {@code ResultSet.rowUpdated}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Updates made to the rows of the ResultSet are not detected by
     * calling the {@code ResultSet.rowUpdated}.
     * </div>
     * <!-- end release-specific documentation -->
     * @param type the {@code ResultSet} type; one of
     *        {@code ResultSet.TYPE_FORWARD_ONLY},
     *        {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
     *        {@code ResultSet.TYPE_SCROLL_SENSITIVE}
     * @return {@code true} if changes are detected by the result set type;
     *         {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @since JDK 1.2
     */
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    /**
     * Retrieves whether or not a visible row delete can be detected by
     * calling the method {@code ResultSet.rowDeleted}.  If the method
     * {@code deletesAreDetected} returns {@code false}, it means that
     * deleted rows are removed from the result set.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Deletes made to the rows of the ResultSet are not detected by
     * calling the {@code ResultSet.rowDeleted}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @param type the {@code ResultSet} type; one of
     *        {@code ResultSet.TYPE_FORWARD_ONLY},
     *        {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
     *        {@code ResultSet.TYPE_SCROLL_SENSITIVE}
     * @return {@code true} if deletes are detected by the given result set type;
     *         {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @since JDK 1.2
     */
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    /**
     * Retrieves whether or not a visible row insert can be detected
     * by calling the method {@code ResultSet.rowInserted}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Inserts made into the ResultSet are not visible and thus not detected by
     * calling the {@code ResultSet.rowInserted}.
     * </div>
     * <!-- end release-specific documentation -->
     * @param type the {@code ResultSet} type; one of
     *        {@code ResultSet.TYPE_FORWARD_ONLY},
     *        {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
     *        {@code ResultSet.TYPE_SCROLL_SENSITIVE}
     * @return {@code true} if changes are detected by the specified result
     *         set type; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @since JDK 1.2
     */
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database supports batch updates.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports batch updates;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if this database supports batch updates;
     *         {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @since JDK 1.2
     */
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    /**
     * Retrieves a description of the user-defined types (UDTs) defined
     * in a particular schema.  Schema-specific UDTs may have type
     * {@code JAVA_OBJECT}, {@code STRUCT},
     * or {@code DISTINCT}.
     *
     * <P>Only types matching the catalog, schema, type name and type
     * criteria are returned.  They are ordered by {@code DATA_TYPE},
     * {@code TYPE_CAT}, {@code TYPE_SCHEM}  and
     * {@code TYPE_NAME}.  The type name parameter may be a fully-qualified
     * name.  In this case, the catalog and schemaPattern parameters are
     * ignored.
     *
     * <P>Each type description has the following columns:
     *  <OL>
     *  <LI><B>TYPE_CAT</B> String {@code =>} the type's catalog (may be {@code null})
     *  <LI><B>TYPE_SCHEM</B> String {@code =>} type's schema (may be {@code null})
     *  <LI><B>TYPE_NAME</B> String {@code =>} type name
     *  <LI><B>CLASS_NAME</B> String {@code =>} Java class name
     *  <LI><B>DATA_TYPE</B> int {@code =>} type value defined in java.sql.Types.
     *     One of JAVA_OBJECT, STRUCT, or DISTINCT
     *  <LI><B>REMARKS</B> String {@code =>} explanatory comment on the type
     *  <LI><B>BASE_TYPE</B> short {@code =>} type code of the source type of a
     *     DISTINCT type or the type that implements the user-generated
     *     reference type of the SELF_REFERENCING_COLUMN of a structured
     *     type as defined in java.sql.Types ({@code null} if DATA_TYPE is not
     *     DISTINCT or not STRUCT with REFERENCE_GENERATION = USER_DEFINED)
     *  </OL>
     *
     * <P><B>Note:</B> If the driver does not support UDTs, an empty
     * result set is returned.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * Starting with 2.0, DISTICT types are supported and are reported by this
     * method.
     * </div>
     * <!-- end release-specific documentation -->
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        {@code null} means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema pattern name; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        {@code null} means that the schema name should not be used to narrow
     *        the search
     * @param typeNamePattern a type name pattern; must match the type name
     *        as it is stored in the database; may be a fully qualified name
     * @param types a list of user-defined types (JAVA_OBJECT,
     *        STRUCT, or DISTINCT) to include; {@code null} returns all types
     * @return {@code ResultSet} object in which each row describes a UDT
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     * @since JDK 1.2
     */
    public ResultSet getUDTs(
            String catalog,
            String schemaPattern,
            String typeNamePattern,
            int[] types)
            throws SQLException {

        if (wantsIsNull(typeNamePattern)
                || (types != null && types.length == 0)) {
            executeSelect("SYSTEM_UDTS", "0=1");
        }

        catalog       = translateCatalog(catalog);
        schemaPattern = translateSchema(schemaPattern);

        StringBuilder sb = toQueryPrefix("SYSTEM_UDTS");

        sb.append(and("TYPE_CAT", "=", catalog))
          .append(and("TYPE_SCHEM", "LIKE", schemaPattern))
          .append(and("TYPE_NAME", "LIKE", typeNamePattern));

        if (types == null) {

            // do not use to narrow search
        } else {
            sb.append(" AND DATA_TYPE IN (")
              .append(StringUtil.getList(types, ",", ""))
              .append(')');
        }

        sb.append(" ORDER BY DATA_TYPE, TYPE_CAT, TYPE_SCHEM, TYPE_NAME");

        return execute(sb.toString());
    }

    /**
     * Retrieves the connection that produced this metadata object.
     *
     * @return the connection that produced this metadata object
     * @throws SQLException if a database access error occurs
     * @since  JDK 1.2
     */
    public Connection getConnection() throws SQLException {
        return connection;
    }

    // ------------------- JDBC 3.0 -------------------------

    /**
     * Retrieves whether this database supports savepoints.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * This SQL feature is supported through JDBC as well as SQL.
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if savepoints are supported;
     *         {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
    public boolean supportsSavepoints() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether this database supports named parameters to callable
     * statements.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports JDBC named parameters to
     * callable statements; this method returns true.
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if named parameters are supported;
     *         {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
    public boolean supportsNamedParameters() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether it is possible to have multiple {@code ResultSet} objects
     * returned from a {@code CallableStatement} object
     * simultaneously.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports multiple ResultSet
     * objects returned from a {@code CallableStatement};
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if a {@code CallableStatement} object
     *         can return multiple {@code ResultSet} objects
     *         simultaneously; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
    public boolean supportsMultipleOpenResults() throws SQLException {
        return true;
    }

    /**
     * Retrieves whether auto-generated keys can be retrieved after
     * a statement has been executed
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports retrieval of
     * autogenerated keys through the JDBC interface;
     * this method always returns {@code true}.
     * </div>
     * <!-- end release-specific documentation -->
     * @return {@code true} if auto-generated keys can be retrieved
     *         after a statement has executed; {@code false} otherwise
     * <p>If {@code true} is returned, the JDBC driver must support the
     * returning of auto-generated keys for at least SQL INSERT statements
     * @throws SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return true;
    }

    /**
     * Retrieves a description of the user-defined type (UDT) hierarchies defined in a
     * particular schema in this database. Only the immediate super type/
     * sub type relationship is modeled.
     * <P>
     * Only supertype information for UDTs matching the catalog,
     * schema, and type name is returned. The type name parameter
     * may be a fully-qualified name. When the UDT name supplied is a
     * fully-qualified name, the catalog and schemaPattern parameters are
     * ignored.
     * <P>
     * If a UDT does not have a direct super type, it is not listed here.
     * A row of the {@code ResultSet} object returned by this method
     * describes the designated UDT and a direct supertype. A row has the following
     * columns:
     *  <OL>
     *  <LI><B>TYPE_CAT</B> String {@code =>} the UDT's catalog (may be {@code null})
     *  <LI><B>TYPE_SCHEM</B> String {@code =>} UDT's schema (may be {@code null})
     *  <LI><B>TYPE_NAME</B> String {@code =>} type name of the UDT
     *  <LI><B>SUPERTYPE_CAT</B> String {@code =>} the direct super type's catalog
     *                           (may be {@code null})
     *  <LI><B>SUPERTYPE_SCHEM</B> String {@code =>} the direct super type's schema
     *                             (may be {@code null})
     *  <LI><B>SUPERTYPE_NAME</B> String {@code =>} the direct super type's name
     *  </OL>
     *
     * <P><B>Note:</B> If the driver does not support type hierarchies, an
     * empty result set is returned.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports the SQL Standard. It treats unquoted identifiers as
     * case insensitive in SQL and stores
     * them in upper case; it treats quoted identifiers as case sensitive and
     * stores them verbatim. All JDBCDatabaseMetaData methods perform
     * case-sensitive comparison between name (pattern) arguments and the
     * corresponding identifier values as they are stored in the database.
     * Therefore, care must be taken to specify name arguments precisely
     * (including case) as they are stored in the database. <p>
     *
     * From 2.0, this feature is supported by default and return supertypes
     * for DOMAIN and DISTINCT types.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; "" retrieves those without a catalog;
     *        {@code null} means drop catalog name from the selection criteria
     * @param schemaPattern a schema name pattern; "" retrieves those
     *        without a schema
     * @param typeNamePattern a UDT name pattern; may be a fully-qualified
     *        name
     * @return a {@code ResultSet} object in which a row gives information
     *         about the designated UDT
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     * @since JDK 1.4, HSQLDB 1.7
     */
    public ResultSet getSuperTypes(
            String catalog,
            String schemaPattern,
            String typeNamePattern)
            throws SQLException {

        if (wantsIsNull(typeNamePattern)) {
            return executeSelect("SYSTEM_SUPERTYPES", "0=1");
        }

        catalog       = translateCatalog(catalog);
        schemaPattern = translateSchema(schemaPattern);

        StringBuilder sb = new StringBuilder();

        sb.append(
            "SELECT * FROM (SELECT USER_DEFINED_TYPE_CATALOG, USER_DEFINED_TYPE_SCHEMA, USER_DEFINED_TYPE_NAME,")
          .append(
              "CAST (NULL AS INFORMATION_SCHEMA.SQL_IDENTIFIER), CAST (NULL AS INFORMATION_SCHEMA.SQL_IDENTIFIER), DATA_TYPE ")
          .append("FROM INFORMATION_SCHEMA.USER_DEFINED_TYPES ")
          .append(
              "UNION SELECT DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME,NULL,NULL, DATA_TYPE ")
          .append("FROM INFORMATION_SCHEMA.DOMAINS) ")
          .append(
              "AS SUPERTYPES(TYPE_CAT, TYPE_SCHEM, TYPE_NAME, SUPERTYPE_CAT, SUPERTYPE_SCHEM, SUPERTYPE_NAME) ")
          .append(whereTrue)
          .append(and("TYPE_CAT", "=", catalog))
          .append(and("TYPE_SCHEM", "LIKE", schemaPattern))
          .append(and("TYPE_NAME", "LIKE", typeNamePattern));

        return execute(sb.toString());
    }

    /**
     * Retrieves a description of the table hierarchies defined in a particular
     * schema in this database.
     *
     * <P>Only supertable information for tables matching the catalog, schema
     * and table name are returned. The table name parameter may be a fully-
     * qualified name, in which case, the catalog and schemaPattern parameters
     * are ignored. If a table does not have a super table, it is not listed here.
     * Supertables have to be defined in the same catalog and schema as the
     * sub tables. Therefore, the type description does not need to include
     * this information for the supertable.
     *
     * <P>Each type description has the following columns:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String {@code =>} the type's catalog (may be {@code null})
     *  <LI><B>TABLE_SCHEM</B> String {@code =>} type's schema (may be {@code null})
     *  <LI><B>TABLE_NAME</B> String {@code =>} type name
     *  <LI><B>SUPERTABLE_NAME</B> String {@code =>} the direct super type's name
     *  </OL>
     *
     * <P><B>Note:</B> If the driver does not support type hierarchies, an
     * empty result set is returned.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * This method is intended for tables of structured types.
     * From 2.0 this method returns an empty ResultSet.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; "" retrieves those without a catalog;
     *        {@code null} means drop catalog name from the selection criteria
     * @param schemaPattern a schema name pattern; "" retrieves those
     *        without a schema
     * @param tableNamePattern a table name pattern; may be a fully-qualified
     *        name
     * @return a {@code ResultSet} object in which each row is a type description
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     * @since JDK 1.4, HSQLDB 1.7
     */
    public ResultSet getSuperTables(
            String catalog,
            String schemaPattern,
            String tableNamePattern)
            throws SQLException {

        // query with no result
        StringBuilder sb = new StringBuilder();

        sb.append(
            "SELECT TABLE_NAME AS TABLE_CAT, TABLE_NAME AS TABLE_SCHEM, TABLE_NAME, TABLE_NAME AS SUPERTABLE_NAME ")
          .append("FROM INFORMATION_SCHEMA.TABLES ")
          .append(whereTrue)
          .append(and("TABLE_NAME", "=", ""));

        return execute(sb.toString());
    }

    /**
     * Retrieves a description of the given attribute of the given type
     * for a user-defined type (UDT) that is available in the given schema
     * and catalog.
     * <P>
     * Descriptions are returned only for attributes of UDTs matching the
     * catalog, schema, type, and attribute name criteria. They are ordered by
     * {@code TYPE_CAT}, {@code TYPE_SCHEM},
     * {@code TYPE_NAME} and {@code ORDINAL_POSITION}. This description
     * does not contain inherited attributes.
     * <P>
     * The {@code ResultSet} object that is returned has the following
     * columns:
     * <OL>
     *  <LI><B>TYPE_CAT</B> String {@code =>} type catalog (may be {@code null})
     *  <LI><B>TYPE_SCHEM</B> String {@code =>} type schema (may be {@code null})
     *  <LI><B>TYPE_NAME</B> String {@code =>} type name
     *  <LI><B>ATTR_NAME</B> String {@code =>} attribute name
     *  <LI><B>DATA_TYPE</B> int {@code =>} attribute type SQL type from java.sql.Types
     *  <LI><B>ATTR_TYPE_NAME</B> String {@code =>} Data source dependent type name.
     *  For a UDT, the type name is fully qualified. For a REF, the type name is
     *  fully qualified and represents the target type of the reference type.
     *  <LI><B>ATTR_SIZE</B> int {@code =>} column size.  For char or date
     *      types this is the maximum number of characters; for numeric or
     *      decimal types this is precision.
     *  <LI><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned for data types where
     * DECIMAL_DIGITS is not applicable.
     *  <LI><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
     *  <LI><B>NULLABLE</B> int {@code =>} whether NULL is allowed
     *      <UL>
     *      <LI> attributeNoNulls - might not allow NULL values
     *      <LI> attributeNullable - definitely allows NULL values
     *      <LI> attributeNullableUnknown - nullability unknown
     *      </UL>
     *  <LI><B>REMARKS</B> String {@code =>} comment describing column (may be {@code null})
     *  <LI><B>ATTR_DEF</B> String {@code =>} default value (may be {@code null})
     *  <LI><B>SQL_DATA_TYPE</B> int {@code =>} unused
     *  <LI><B>SQL_DATETIME_SUB</B> int {@code =>} unused
     *  <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the
     *       maximum number of bytes in the column
     *  <LI><B>ORDINAL_POSITION</B> int {@code =>} index of the attribute in the UDT
     *      (starting at 1)
     *  <LI><B>IS_NULLABLE</B> String  {@code =>} ISO rules are used to determine
     * the nullability for an attribute.
     *       <UL>
     *       <LI> YES           --- if the attribute can include NULLs
     *       <LI> NO            --- if the attribute cannot include NULLs
     *       <LI> empty string  --- if the nullability for the
     * attribute is unknown
     *       </UL>
     *  <LI><B>SCOPE_CATALOG</B> String {@code =>} catalog of table that is the
     *      scope of a reference attribute ({@code null} if DATA_TYPE isn't REF)
     *  <LI><B>SCOPE_SCHEMA</B> String {@code =>} schema of table that is the
     *      scope of a reference attribute ({@code null} if DATA_TYPE isn't REF)
     *  <LI><B>SCOPE_TABLE</B> String {@code =>} table name that is the scope of a
     *      reference attribute ({@code null} if the DATA_TYPE isn't REF)
     * <LI><B>SOURCE_DATA_TYPE</B> short {@code =>} source type of a distinct type or user-generated
     *      Ref type, SQL type from java.sql.Types ({@code null} if DATA_TYPE
     *      isn't DISTINCT or user-generated REF)
     *  </OL>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * This method is intended for attributes of structured types.
     * From 2.0 this method returns an empty ResultSet.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        {@code null} means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        {@code null} means that the schema name should not be used to narrow
     *        the search
     * @param typeNamePattern a type name pattern; must match the
     *        type name as it is stored in the database
     * @param attributeNamePattern an attribute name pattern; must match the attribute
     *        name as it is declared in the database
     * @return a {@code ResultSet} object in which each row is an
     *         attribute description
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     * @since JDK 1.4, HSQLDB 1.7
     */
    public ResultSet getAttributes(
            String catalog,
            String schemaPattern,
            String typeNamePattern,
            String attributeNamePattern)
            throws SQLException {

        if (wantsIsNull(typeNamePattern) || wantsIsNull(attributeNamePattern)) {
            return executeSelect("SYSTEM_UDTATTRIBUTES", "0=1");
        }

        schemaPattern = translateSchema(schemaPattern);

        StringBuilder sb = toQueryPrefix("SYSTEM_UDTATTRIBUTES");

        sb.append(and("TYPE_CAT", "=", catalog))
          .append(and("TYPE_SCHEM", "LIKE", schemaPattern))
          .append(and("TYPE_NAME", "LIKE", typeNamePattern))
          .append(and("ATTR_NAME", "LIKE", attributeNamePattern))
          .append(
              " ORDER BY TYPE_CAT, TYPE_SCHEM, TYPE_NAME, ORDINAL_POSITION");

        return execute(sb.toString());
    }

    /**
     * Retrieves whether this database supports the given result set holdability.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB returns true for both alternatives.
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @param holdability one of the following constants:
     *          {@code ResultSet.HOLD_CURSORS_OVER_COMMIT} or
     *          {@code ResultSet.CLOSE_CURSORS_AT_COMMIT}
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @see JDBCConnection
     * @since JDK 1.4, HSQLDB 1.7
     */
    public boolean supportsResultSetHoldability(
            int holdability)
            throws SQLException {
        return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT
               || holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    /**
     * Retrieves this database's default holdability for {@code ResultSet}
     * objects.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB defaults to HOLD_CURSORS_OVER_COMMIT for CONSUR_READ_ONLY
     * ResultSet objects.
     * If the ResultSet concurrency is CONCUR_UPDATABLE, then holdability is
     * is enforced as CLOSE_CURSORS_AT_COMMIT.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the default holdability; either
     *         {@code ResultSet.HOLD_CURSORS_OVER_COMMIT} or
     *         {@code ResultSet.CLOSE_CURSORS_AT_COMMIT}
     * @throws SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    /**
     * Retrieves the major version number of the underlying database.
     *
     * @return the underlying database's major version
     * @throws SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
    public int getDatabaseMajorVersion() throws SQLException {

        ResultSet rs = execute("call database_version()");

        rs.next();

        String v = rs.getString(1);

        rs.close();

        return Integer.parseInt(v.substring(0, v.indexOf(".")));
    }

    /**
     * Retrieves the minor version number of the underlying database.
     *
     * @return underlying database's minor version
     * @throws SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
    public int getDatabaseMinorVersion() throws SQLException {

        ResultSet rs = execute("call database_version()");

        rs.next();

        String v = rs.getString(1);

        rs.close();

        int start = v.indexOf(".") + 1;

        return Integer.parseInt(v.substring(start, v.indexOf(".", start)));
    }

    /**
     * Retrieves the major JDBC version number for this
     * driver.
     *
     * @return JDBC version major number
     * @throws SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
    public int getJDBCMajorVersion() throws SQLException {
        return JDBC_MAJOR;
    }

    /**
     * Retrieves the minor JDBC version number for this
     * driver.
     *
     * @return JDBC version minor number
     * @throws SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
    public int getJDBCMinorVersion() throws SQLException {
        return JDBC_MINOR;
    }

    /**
     * Indicates whether the SQLSTATE returned by {@code SQLException.getSQLState}
     * is X/Open (now known as Open Group) SQL CLI or SQL:2003.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB returns {@code sqlStateSQL} under JDBC4 which is equivalent
     * to JDBC3 value of sqlStateSQL99.
     * </div>
     * <!-- end release-specific documentation -->
     * @return the type of SQLSTATE; one of:
     *        sqlStateXOpen or
     *        sqlStateSQL
     * @throws SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL99;
    }

    /**
     * Indicates whether updates made to a LOB are made on a copy or directly
     * to the LOB.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     * Updates to a LOB are made directly. This means the lobs in an updatable
     * ResultSet can be updated and the change is applied when the updateRow()
     * method is applied. Lobs created by calling the Connection methods
     * createClob() and createBlob() can be updated. The lob can then be sent to
     * the database in a PreparedStatement with an UPDATE or INSERT SQL
     * statement.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if updates are made to a copy of the LOB;
     *         {@code false} if updates are made directly to the LOB
     * @throws SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    /**
     * Retrieves whether this database supports statement pooling.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with 2.0, HSQLDB supports statement pooling.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @since JDK 1.4, HSQLDB 1.7
     */
    public boolean supportsStatementPooling() throws SQLException {
        return (JDBC_MAJOR >= 4);
    }

    //------------------------- JDBC 4.0 -----------------------------------

    /**
     * Indicates whether this data source supports the SQL {@code ROWID} type,
     * and the lifetime for which a {@code java.sql.RowId} object remains valid.
     * <p>
     * The returned int values have the following relationship:
     * <pre>{@code
     *     ROWID_UNSUPPORTED < ROWID_VALID_OTHER < ROWID_VALID_TRANSACTION
     *         < ROWID_VALID_SESSION < ROWID_VALID_FOREVER
     * }</pre>
     * so conditional logic such as
     * <pre>{@code
     *     if (metadata.getRowIdLifetime() > DatabaseMetaData.ROWID_VALID_TRANSACTION)
     * }</pre>
     * can be used. Valid Forever means valid across all Sessions, and valid for
     * a Session means valid across all its contained Transactions.
     *
     * @return the status indicating the lifetime of a {@code  RowId}
     * @throws SQLException if a database access error occurs
     * @since JDK 1.6, HSQLDB 1.9
     */
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    /**
     * Retrieves the schema names available in this database.  The results
     * are ordered by {@code TABLE_CATALOG} and
     * {@code TABLE_SCHEM}.
     *
     * <P>The schema columns are:
     *  <OL>
     *  <LI><B>TABLE_SCHEM</B> String {@code =>} schema name
     *  <LI><B>TABLE_CATALOG</B> String {@code =>} catalog name (may be {@code null})
     *  </OL>
     *
     *
     * @param catalog a catalog name; must match the catalog name as it is stored
     * in the database;"" retrieves those without a catalog; null means catalog
     * name should not be used to narrow down the search.
     * @param schemaPattern a schema name; must match the schema name as it is
     * stored in the database; null means
     * schema name should not be used to narrow down the search.
     * @return a {@code ResultSet} object in which each row is a
     *         schema description
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     * @since JDK 1.6, HSQLDB 1.9
     */
    public ResultSet getSchemas(
            String catalog,
            String schemaPattern)
            throws SQLException {

        StringBuilder sb = toQueryPrefix(
            "SYSTEM_SCHEMAS").append(and("TABLE_CATALOG", "=", catalog))
                             .append(and("TABLE_SCHEM", "LIKE", schemaPattern))
                             .append(" ORDER BY TABLE_CATALOG, TABLE_SCHEM");

        return execute(sb.toString());
    }

    /**
     * Retrieves whether this database supports invoking user-defined or vendor functions
     * using the stored procedure escape syntax.
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @since JDK 1.6, HSQLDB 1.9
     */
    public boolean supportsStoredFunctionsUsingCallSyntax()
            throws SQLException {
        return true;
    }

    /* @todo */

    /**
     * Retrieves whether a {@code SQLException} while autoCommit is {@code true} indicates
     * that all open ResultSets are closed, even ones that are holdable.  When a {@code SQLException} occurs while
     * autocommit is {@code true}, it is vendor specific whether the JDBC driver responds with a commit operation, a
     * rollback operation, or by doing neither a commit nor a rollback.  A potential result of this difference
     * is in whether or not holdable ResultSets are closed.
     *
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @since JDK 1.6, HSQLDB 1.9
     */
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    /**
     * Retrieves a list of the client info properties
     * that the driver supports.  The result set contains the following columns
         *
     * <ol>
     * <li><b>NAME</b> String{@code =>} The name of the client info property<br>
     * <li><b>MAX_LEN</b> int{@code =>} The maximum length of the value for the property<br>
     * <li><b>DEFAULT_VALUE</b> String{@code =>} The default value of the property<br>
     * <li><b>DESCRIPTION</b> String{@code =>} A description of the property.  This will typically
     *                                                  contain information as to where this property is
     *                                                  stored in the database.
     * </ol>
     * <p>
     * The {@code ResultSet} is sorted by the NAME column
     *
     * @return  A {@code ResultSet} object; each row is a supported client info
     * property
     *
     *  @throws SQLException if a database access error occurs
     *
     * @since JDK 1.6, HSQLDB 1.9
     */
    public ResultSet getClientInfoProperties() throws SQLException {

        String s =
            "SELECT * FROM INFORMATION_SCHEMA.SYSTEM_CONNECTION_PROPERTIES";

        return execute(s);
    }

    /**
     * Retrieves a description of the system and user functions available
     * in the given catalog.
     * <P>
     * Only system and user function descriptions matching the schema and
     * function name criteria are returned.  They are ordered by
     * {@code FUNCTION_CAT}, {@code FUNCTION_SCHEM},
     * {@code FUNCTION_NAME} and
     * {@code SPECIFIC_ NAME}.
     *
     * <P>Each function description has the following columns:
     *  <OL>
     *  <LI><B>FUNCTION_CAT</B> String {@code =>} function catalog (may be {@code null})
     *  <LI><B>FUNCTION_SCHEM</B> String {@code =>} function schema (may be {@code null})
     *  <LI><B>FUNCTION_NAME</B> String {@code =>} function name.  This is the name
     * used to invoke the function
     *  <LI><B>REMARKS</B> String {@code =>} explanatory comment on the function
     * <LI><B>FUNCTION_TYPE</B> short {@code =>} kind of function:
     *      <UL>
     *      <LI>functionResultUnknown - Cannot determine if a return value
     *       or table will be returned
     *      <LI> functionNoTable- Does not return a table
     *      <LI> functionReturnsTable - Returns a table
     *      </UL>
     *  <LI><B>SPECIFIC_NAME</B> String  {@code =>} the name which uniquely identifies
     *  this function within its schema.  This is a user specified, or DBMS
     * generated, name that may be different then the {@code FUNCTION_NAME}
     * for example with overload functions
     *  </OL>
     * <p>
     * A user may not have permission to execute any of the functions that are
     * returned by {@code getFunctions}
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        {@code null} means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        {@code null} means that the schema name should not be used to narrow
     *        the search
     * @param functionNamePattern a function name pattern; must match the
     *        function name as it is stored in the database
     * @return {@code ResultSet} - each row is a function description
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     * @since JDK 1.6, HSQLDB 1.9
     */
    public ResultSet getFunctions(
            String catalog,
            String schemaPattern,
            String functionNamePattern)
            throws SQLException {

        StringBuilder sb = new StringBuilder();

        sb.append("select ")
          .append("sp.procedure_cat as FUNCTION_CAT,")
          .append("sp.procedure_schem as FUNCTION_SCHEM,")
          .append("sp.procedure_name as FUNCTION_NAME,")
          .append("sp.remarks as REMARKS,")
          .append("sp.function_type as FUNCTION_TYPE,")
          .append("sp.specific_name as SPECIFIC_NAME ")
          .append("from information_schema.system_procedures sp ")
          .append("where sp.procedure_type = 2 ");

        if (wantsIsNull(functionNamePattern)) {
            sb.append("and 1=0");

            return execute(sb.toString());
        }

        schemaPattern = translateSchema(schemaPattern);

        sb.append(and("sp.procedure_cat", "=", catalog))
          .append(and("sp.procedure_schem", "LIKE", schemaPattern))
          .append(and("sp.procedure_name", "LIKE", functionNamePattern));

        // By default, query already returns the result ordered by
        // FUNCTION_SCHEM, FUNCTION_NAME...
        return execute(sb.toString());
    }

    /**
     * Retrieves a description of the given catalog's system or user
     * function parameters and return type.
     *
     * <P>Only descriptions matching the schema,  function and
     * parameter name criteria are returned. They are ordered by
     * {@code FUNCTION_CAT}, {@code FUNCTION_SCHEM},
     * {@code FUNCTION_NAME} and
     * {@code SPECIFIC_ NAME}. Within this, the return value,
     * if any, is first. Next are the parameter descriptions in call
     * order. The column descriptions follow in column number order.
     *
     * <P>Each row in the {@code ResultSet}
     * is a parameter description, column description or
     * return type description with the following fields:
     *  <OL>
     *  <LI><B>FUNCTION_CAT</B> String {@code =>} function catalog (may be {@code null})
     *  <LI><B>FUNCTION_SCHEM</B> String {@code =>} function schema (may be {@code null})
     *  <LI><B>FUNCTION_NAME</B> String {@code =>} function name.  This is the name
     * used to invoke the function
     *  <LI><B>COLUMN_NAME</B> String {@code =>} column/parameter name
     *  <LI><B>COLUMN_TYPE</B> Short {@code =>} kind of column/parameter:
     *      <UL>
     *      <LI> functionColumnUnknown - nobody knows
     *      <LI> functionColumnIn - IN parameter
     *      <LI> functionColumnInOut - INOUT parameter
     *      <LI> functionColumnOut - OUT parameter
     *      <LI> functionColumnReturn - function return value
     *      <LI> functionColumnResult - Indicates that the parameter or column
     *  is a column in the {@code ResultSet}
     *      </UL>
     *  <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
     *  <LI><B>TYPE_NAME</B> String {@code =>} SQL type name, for a UDT type the
     *  type name is fully qualified
     *  <LI><B>PRECISION</B> int {@code =>} precision
     *  <LI><B>LENGTH</B> int {@code =>} length in bytes of data
     *  <LI><B>SCALE</B> short {@code =>} scale -  null is returned for data types where
     * SCALE is not applicable.
     *  <LI><B>RADIX</B> short {@code =>} radix
     *  <LI><B>NULLABLE</B> short {@code =>} can it contain NULL.
     *      <UL>
     *      <LI> functionNoNulls - does not allow NULL values
     *      <LI> functionNullable - allows NULL values
     *      <LI> functionNullableUnknown - nullability unknown
     *      </UL>
     *  <LI><B>REMARKS</B> String {@code =>} comment describing column/parameter
     *  <LI><B>CHAR_OCTET_LENGTH</B> int  {@code =>} the maximum length of binary
     * and character based parameters or columns.  For any other datatype the returned value
     * is a NULL
     *  <LI><B>ORDINAL_POSITION</B> int  {@code =>} the ordinal position, starting
     * from 1, for the input and output parameters. A value of 0
     * is returned if this row describes the function's return value.
     * For result set columns, it is the
     * ordinal position of the column in the result set starting from 1.
     *  <LI><B>IS_NULLABLE</B> String  {@code =>} ISO rules are used to determine
     * the nullability for a parameter or column.
     *       <UL>
     *       <LI> YES           --- if the parameter or column can include NULLs
     *       <LI> NO            --- if the parameter or column  cannot include NULLs
     *       <LI> empty string  --- if the nullability for the
     * parameter  or column is unknown
     *       </UL>
     *  <LI><B>SPECIFIC_NAME</B> String  {@code =>} the name which uniquely identifies
     * this function within its schema.  This is a user specified, or DBMS
     * generated, name that may be different then the {@code FUNCTION_NAME}
     * for example with overload functions
     *  </OL>
     *
     * <p>The PRECISION column represents the specified column size for the given
     * parameter or column.
     * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
     * For datetime datatypes, this is the length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
     * this is the length in bytes. Null is returned for data types where the
     * column size is not applicable.
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        {@code null} means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        {@code null} means that the schema name should not be used to narrow
     *        the search
     * @param functionNamePattern a procedure name pattern; must match the
     *        function name as it is stored in the database
     * @param columnNamePattern a parameter name pattern; must match the
     * parameter or column name as it is stored in the database
     * @return {@code ResultSet} - each row describes a
     * user function parameter, column  or return type
     *
     * @throws SQLException if a database access error occurs
     * @see #getSearchStringEscape
     * @since JDK 1.6, HSQLDB 1.9
     */
    public ResultSet getFunctionColumns(
            String catalog,
            String schemaPattern,
            String functionNamePattern,
            String columnNamePattern)
            throws SQLException {

        StringBuilder sb = new StringBuilder(256);

        sb.append("select pc.procedure_cat as FUNCTION_CAT,")
          .append("pc.procedure_schem as FUNCTION_SCHEM,")
          .append("pc.procedure_name as FUNCTION_NAME,")
          .append("pc.column_name as COLUMN_NAME,")
          .append("case pc.column_type")
          .append(" when 3 then 5")
          .append(" when 4 then 3")
          .append(" when 5 then 4")
          .append(" else pc.column_type")
          .append(" end as COLUMN_TYPE,")
          .append("pc.DATA_TYPE,")
          .append("pc.TYPE_NAME,")
          .append("pc.PRECISION,")
          .append("pc.LENGTH,")
          .append("pc.SCALE,")
          .append("pc.RADIX,")
          .append("pc.NULLABLE,")
          .append("pc.REMARKS,")
          .append("pc.CHAR_OCTET_LENGTH,")
          .append("pc.ORDINAL_POSITION,")
          .append("pc.IS_NULLABLE,")
          .append("pc.SPECIFIC_NAME,")
          .append("case pc.column_type")
          .append(" when 3 then 1")
          .append(" else 0")
          .append(" end AS COLUMN_GROUP ")
          .append("from information_schema.system_procedurecolumns pc ")
          .append("join (select procedure_schem,")
          .append("procedure_name,")
          .append("specific_name ")
          .append("from information_schema.system_procedures ")
          .append("where procedure_type = 2) p ")
          .append("on pc.procedure_schem = p.procedure_schem ")
          .append("and pc.procedure_name = p.procedure_name ")
          .append("and pc.specific_name = p.specific_name ")
          .append("and ((pc.column_type = 3 and pc.column_name = '@p0') ")
          .append("or ")
          .append("(pc.column_type <> 3)) ");

        if (wantsIsNull(functionNamePattern)
                || wantsIsNull(columnNamePattern)) {
            return execute(sb.append("where 1=0").toString());
        }

        schemaPattern = translateSchema(schemaPattern);

        sb.append("where 1=1 ")
          .append(and("pc.procedure_cat", "=", catalog))
          .append(and("pc.procedure_schem", "LIKE", schemaPattern))
          .append(and("pc.procedure_name", "LIKE", functionNamePattern))
          .append(and("pc.column_name", "LIKE", columnNamePattern))
          .append(" order by 1, 2, 3, 17, 18 , 15");

        // Order by FUNCTION_CAT, FUNCTION_SCHEM, FUNCTION_NAME, SPECIFIC_NAME
        //      COLUMN_GROUP and ORDINAL_POSITION
        return execute(sb.toString());
    }

    /**
     * Returns an object that implements the given interface to allow access to non-standard methods,
     * or standard methods not exposed by the proxy.
     * The result may be either the object found to implement the interface or a proxy for that object.
     * If the receiver implements the interface then that is the object. If the receiver is a wrapper
     * and the wrapped object implements the interface then that is the object. Otherwise the object is
     *  the result of calling {@code unwrap} recursively on the wrapped object. If the receiver is not a
     * wrapper and does not implement the interface, then an {@code SQLException} is thrown.
     *
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws java.sql.SQLException If no object found that implements the interface
     * @since JDK 1.6, HSQLDB 1.9
     */
    @SuppressWarnings("unchecked")
    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {

        if (isWrapperFor(iface)) {
            return (T) this;
        }

        throw JDBCUtil.invalidArgument("iface: " + iface);
    }

    /**
     * Returns true if this either implements the interface argument or is directly or indirectly a wrapper
     * for an object that does. Returns false otherwise. If this implements the interface then return true,
     * else if this is a wrapper then return the result of recursively calling {@code isWrapperFor} on the wrapped
     * object. If this does not implement the interface and is not a wrapper, return false.
     * This method should be implemented as a low-cost operation compared to {@code unwrap} so that
     * callers can use this method to avoid expensive {@code unwrap} calls that may fail. If this method
     * returns true then calling {@code unwrap} with the same argument should succeed.
     *
     * @param iface a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that does.
     * @throws java.sql.SQLException  if an error occurs while determining whether this is a wrapper
     * for an object with the given interface.
     * @since JDK 1.6, HSQLDB 1.9
     */
    public boolean isWrapperFor(
            java.lang.Class<?> iface)
            throws java.sql.SQLException {
        return (iface != null && iface.isAssignableFrom(this.getClass()));
    }

    //--------------------------JDBC 4.1 -----------------------------

    /**
     * Retrieves a description of the pseudo or hidden columns available
     * in a given table within the specified catalog and schema.
     * Pseudo or hidden columns may not always be stored within
     * a table and are not visible in a ResultSet unless they are
     * specified in the query's outermost SELECT list. Pseudo or hidden
     * columns may not necessarily be able to be modified. If there are
     * no pseudo or hidden columns, an empty ResultSet is returned.
     *
     * <P>Only column descriptions matching the catalog, schema, table
     * and column name criteria are returned.  They are ordered by
     * {@code TABLE_CAT},{@code TABLE_SCHEM}, {@code TABLE_NAME}
     * and {@code COLUMN_NAME}.
     *
     * <P>Each column description has the following columns:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be {@code null})
     *  <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be {@code null})
     *  <LI><B>TABLE_NAME</B> String {@code =>} table name
     *  <LI><B>COLUMN_NAME</B> String {@code =>} column name
     *  <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
     *  <LI><B>COLUMN_SIZE</B> int {@code =>} column size.
     *  <LI><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned for data types where
     * DECIMAL_DIGITS is not applicable.
     *  <LI><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
     *  <LI><B>COLUMN_USAGE</B> String {@code =>} The allowed usage for the column.  The
     *  value returned will correspond to the enum name returned by {@link java.sql.PseudoColumnUsage PseudoColumnUsage.name()}
     *  <LI><B>REMARKS</B> String {@code =>} comment describing column (may be {@code null})
     *  <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the
     *       maximum number of bytes in the column
     *  <LI><B>IS_NULLABLE</B> String  {@code =>} ISO rules are used to determine the nullability for a column.
     *       <UL>
     *       <LI> YES           --- if the column can include NULLs
     *       <LI> NO            --- if the column cannot include NULLs
     *       <LI> empty string  --- if the nullability for the column is unknown
     *       </UL>
     *  </OL>
     *
     * <p>The COLUMN_SIZE column specifies the column size for the given column.
     * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
     * For datetime datatypes, this is the length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
     * this is the length in bytes. Null is returned for data types where the
     * column size is not applicable.
     *
     * @param catalog a catalog name; must match the catalog name as it
     *        is stored in the database; "" retrieves those without a catalog;
     *        {@code null} means that the catalog name should not be used to narrow
     *        the search
     * @param schemaPattern a schema name pattern; must match the schema name
     *        as it is stored in the database; "" retrieves those without a schema;
     *        {@code null} means that the schema name should not be used to narrow
     *        the search
     * @param tableNamePattern a table name pattern; must match the
     *        table name as it is stored in the database
     * @param columnNamePattern a column name pattern; must match the column
     *        name as it is stored in the database
     * @return {@code ResultSet} - each row is a column description
     * @throws SQLException if a database access error occurs
     * @see java.sql.PseudoColumnUsage
     * @since JDK 1.7, HSQLDB 2.0.1
     */
    public ResultSet getPseudoColumns(
            String catalog,
            String schemaPattern,
            String tableNamePattern,
            String columnNamePattern)
            throws SQLException {
        throw JDBCUtil.notSupported();
    }

    /**
     * Retrieves whether a generated key will always be returned if the column
     * name(s) or index(es) specified for the auto generated key column(s)
     * are valid and the statement succeeds.  The key that is returned may or
     * may not be based on the column(s) for the auto generated key.
     * Consult your JDBC driver documentation for additional details.
     * @return {@code true} if so; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @since JDK 1.7, HSQLDB 2.0.1
     */
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return true;
    }

    //--------------------------JDBC 4.2 -----------------------------

    /**
     *
     * Retrieves the maximum number of bytes this database allows for
     * the logical size for a {@code LOB}.
     *
     * @return the maximum number of bytes allowed; a result of zero
     * means that there is no limit or the limit is not known
     * @throws SQLException if a database access error occurs
     * @since 1.8
     */
    public long getMaxLogicalLobSize() throws SQLException {
        return BlobType.maxBlobPrecision;
    }

    /**
     * Retrieves whether this database supports REF CURSOR.

     * @return {@code true} if this database supports REF CURSOR;
     *         {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * @since 1.8
     */
    public boolean supportsRefCursors() throws SQLException {
        return false;
    }

    //----------------------- Internal Implementation --------------------------

    /** Used by getBestRowIdentifier to avoid extra object construction */
    static final Integer INT_COLUMNS_NO_NULLS = Integer.valueOf(columnNoNulls);

    // -----------------------------------------------------------------------
    // private attributes
    // -----------------------------------------------------------------------

    /**
     * The connection this object uses to retrieve database instance-specific
     * metadata.
     */
    private JDBCConnection connection;

    /**
     * Connection property for schema reporting.
     */
    final private boolean useSchemaDefault;

    /**
     * NULL related properties are updated on each call.
     */
    private boolean concatNulls = true;
    private boolean nullsFirst  = true;
    private boolean nullsOrder  = true;

    /**
     * A CSV list representing the SQL IN list to use when generating
     * queries for {@code getBestRowIdentifier} when the
     * {@code scope} argument is {@code bestRowSession}.
     */
    private static final String BRI_SESSION_SCOPE_IN_LIST = "("
        + bestRowSession + ")";

    /**
     * A CSV list representing the SQL IN list to use when generating
     * queries for {@code getBestRowIdentifier} when the
     * {@code scope} argument is {@code bestRowTemporary}.
     */
    private static final String BRI_TEMPORARY_SCOPE_IN_LIST = "("
        + bestRowTemporary + "," + bestRowTransaction + "," + bestRowSession
        + ")";

    /**
     * A CSV list representing the SQL IN list to use when generating
     * queries for {@code getBestRowIdentifier} when the
     * {@code scope} argument is {@code bestRowTransaction}.
     */
    private static final String BRI_TRANSACTION_SCOPE_IN_LIST = "("
        + bestRowTransaction + "," + bestRowSession + ")";

    /**
     * "SELECT * FROM ". <p>
     *
     * This attribute is in support of methods that use SQL SELECT statements to
     * generate returned {@code ResultSet} objects.
     */
    private static final String selstar = "SELECT * FROM INFORMATION_SCHEMA.";

    /**
     * " WHERE TRUE ". <p>
     *
     * This attribute is in support of methods that use SQL SELECT statements to
     * generate returned {@code ResultSet} objects. <p>
     *
     * The optimizer will simply drop this when parsing a condition
     * expression. And it makes our code much easier to write, since we don't
     * have to check our "WHERE" clause productions as strictly for proper
     * conjunction:  we just stick additional conjunctive predicates on the
     * end of this and Presto! Everything works :-)
     */
    private static final String whereTrue  = " WHERE TRUE";
    public static final int     JDBC_MAJOR = 4;
    public static final int     JDBC_MINOR = 2;
    public static final String THIS_VERSION =
        HsqlDatabaseProperties.THIS_VERSION;

    /**
     * Constructs a new {@code JDBCDatabaseMetaData} object using the
     * specified connection.  This contructor is used by {@code JDBCConnection}
     * when producing a {@code DatabaseMetaData} object from a call to
     * {@link JDBCConnection#getMetaData() getMetaData}.
     * @param c the connection this object will use to retrieve
     *         instance-specific metadata
     * @throws SQLException never - reserved for future use
     */
    JDBCDatabaseMetaData(JDBCConnection c) throws SQLException {

        // PRE: is non-null and not closed
        connection       = c;
        useSchemaDefault = !c.isInternal && c.isDefaultSchema;
    }

    /**
     * Retrieves an "AND" predicate based on the (column) {@code id},
     * {@code op}(erator) and{@code val}(ue) arguments to be
     * included in an SQL "WHERE" clause, using the conventions laid out for
     * JDBC DatabaseMetaData filter parameter values.
     *
     * @return an "AND" predicate built from the arguments
     * @param id the simple, non-quoted identifier of a system table
     *      column upon which to filter. <p>
     *
     *      No checking is done for column name validity. <br>
     *      It is assumed the system table column name is correct.
     *
     * @param op the conditional operation to perform using the system table
     *      column name value and the {@code val} argument.
     *
     * @param val an object representing the value to use in some conditional
     *      operation, op, between the column identified by the id argument
     *      and this argument.
     *
     *      <UL>
     *          <LI>null causes the empty string to be returned.
     *
     *          <LI>toString().length() == 0 causes the returned expression
     *              to be built so that the IS NULL operation will occur
     *              against the specified column.
     *
     *          <LI>instanceof String causes the returned expression to be
     *              built so that the specified operation will occur between
     *              the specified column and the specified value, converted to
     *              an SQL string (single quoted, with internal single quotes
     *              escaped by doubling). If {@code op} is "LIKE" and
     *              {@code val} does not contain any "%" or "_" wild
     *              card characters, then {@code op} is silently
     *              converted to "=".
     *
     *          <LI>!instanceof String causes an expression to built so that
     *              the specified operation will occur between the specified
     *              column and {@code String.valueOf(val)}.
     *
     *      </UL>
     */
    private static String and(String id, String op, Object val) {

        // The JDBC standard for pattern arguments seems to be:
        //
        // - pass null to mean ignore (do not include in query),
        // - pass "" to mean filter on <column-ident> IS NULL,
        // - pass "%" to filter on <column-ident> IS NOT NULL.
        // - pass sequence with "%" and "_" for wildcard matches
        // - when searching on values reported directly from DatabaseMetaData
        //   results, typically an exact match is desired.  In this case, it
        //   is the client's responsibility to escape any reported "%" and "_"
        //   characters using whatever DatabaseMetaData returns from
        //   getSearchEscapeString(). In our case, this is the standard escape
        //   character: '\'. Typically, '%' will rarely be encountered, but
        //   certainly '_' is to be expected on a regular basis.
        // - checkme:  what about the (silly) case where an identifier
        //   has been declared such as:  'create table "xxx\_yyy"(...)'?
        //   Must the client still escape the Java string like this:
        //   "xxx\\\\_yyy"?
        //   Yes: because otherwise the driver is expected to
        //   construct something like:
        //   select ... where ... like 'xxx\_yyy' escape '\'
        //   which will try to match 'xxx_yyy', not 'xxx\_yyy'
        //   Testing indicates that indeed, higher quality popular JDBC
        //   database browsers do the escapes "properly."
        if (val == null) {
            return "";
        }

        StringBuilder sb    = new StringBuilder();
        boolean       isStr = (val instanceof String);

        if (isStr && ((String) val).isEmpty()) {
            return sb.append(" AND ").append(id).append(" IS NULL").toString();
        }

        String v = isStr
                   ? Type.SQL_VARCHAR.convertToSQLString(val)
                   : String.valueOf(val);

        sb.append(" AND ").append(id).append(' ');

        // add the escape to like if required
        if (isStr && "LIKE".equalsIgnoreCase(op)) {
            if (v.indexOf('_') < 0 && v.indexOf('%') < 0) {

                // then we can optimize.
                sb.append("=").append(' ').append(v);
            } else {
                sb.append("LIKE").append(' ').append(v);

                if (v.contains("\\_") || v.contains("\\%")) {

                    // then client has requested at least one escape.
                    sb.append(" ESCAPE '\\'");
                }
            }
        } else {
            sb.append(op).append(' ').append(v);
        }

        return sb.toString();
    }

    /**
     * The main SQL statement executor.  All SQL destined for execution
     * ultimately goes through this method. <p>
     *
     * The sqlStatement field for the result is set autoClose to comply with
     * ResultSet.getStatement() semantics for result sets that are not from
     * a user supplied Statement object. (fredt)
     *
     * @param sql SQL statement to execute
     * @return the result of issuing the statement
     * @throws SQLException is a database error occurs
     */
    private ResultSet execute(String sql) throws SQLException {

        // NOTE:
        // Need to create a JDBCStatement here so JDBCResultSet can return
        // its Statement object on call to getStatement().
        // The native JDBCConnection.execute() method does not
        // automatically assign a Statement object for the ResultSet, but
        // JDBCStatement does.  That is, without this, there is no way for the
        // JDBCResultSet to find its way back to its Connection (or Statement)
        // Also, cannot use single, shared JDBCStatement object, as each
        // fetchResult() closes any old JDBCResultSet before fetching the
        // next, causing the JDBCResultSet's Result object to be nullified
        final int scroll = ResultSet.TYPE_SCROLL_INSENSITIVE;
        final int concur = ResultSet.CONCUR_READ_ONLY;
        JDBCStatement st = (JDBCStatement) connection.createStatement(
            scroll,
            concur);

        st.maxRows = -1;

        ResultSet r = st.executeQuery(sql);

        ((JDBCResultSet) r).autoClose = true;

        return r;
    }

    /**
     * An SQL statement executor that knows how to create a "SELECT
     * * FROM" statement, given a table name and a <em>where</em> clause.<p>
     *
     *  If the <em>where</em> clause is null, it is omitted.  <p>
     *
     *  It is assumed that the table name is non-null, since this is a private
     *  method.  No check is performed.
     *
     * @return the result of executing "SELECT * FROM " + table " " + where
     * @param table the name of a table to "select * from"
     * @param where the where condition for the select
     * @throws SQLException if database error occurs
     */
    private ResultSet executeSelect(
            String table,
            String where)
            throws SQLException {

        String select = selstar + table;

        if (where != null) {
            select += " WHERE " + where;
        }

        return execute(select);
    }

    /**
     * Retrieves "SELECT * FROM INFORMATION_SCHEMA.&lt;table&gt; WHERE 1=1" in string
     * buffer form. <p>
     *
     * This is a convenience method provided because, for most
     * {@code DatabaseMetaData} queries, this is the most suitable
     * thing upon which to start building.
     *
     * @return an StringBuilder whose content is:
     *      "SELECT * FROM &lt;table&gt; WHERE 1=1"
     * @param t the name of the table
     */
    private StringBuilder toQueryPrefix(String t) {
        StringBuilder sb = new StringBuilder(255);

        return sb.append(selstar).append(t).append(whereTrue);
    }

    /**
     * Retrieves whether the JDBC {@code DatabaseMetaData} contract
     * specifies that the argument {@code s} is filter parameter
     * value that requires a corresponding IS NULL predicate.
     *
     * @param s the filter parameter to test
     * @return true if the argument, s, is filter parameter value that
     *        requires a corresponding IS NULL predicate
     */
    private static boolean wantsIsNull(String s) {
        return (s != null && s.isEmpty());
    }

    private void setCurrentProperties() throws SQLException {

        ResultSet rs = executeSelect(
            "SYSTEM_PROPERTIES",
            "PROPERTY_NAME IN "
            + "('sql.concat_nulls', 'sql.nulls_first' , 'sql.nulls_order')");

        while (rs.next()) {
            String  prop  = rs.getString(2);
            boolean value = Boolean.valueOf(rs.getString(3));

            if (prop.equals("sql.concat_nulls")) {
                concatNulls = value;
            } else if (prop.equals("sql.nulls_first")) {
                nullsFirst = value;
            } else if (prop.equals("sql.nulls_order")) {
                nullsOrder = value;
            }
        }

        rs.close();
    }

    /**
     * Returns the name of the default collation for database.
     * @return name of collation
     */
    public String getDatabaseDefaultCollation() {

        String value = null;

        try {
            ResultSet rs = executeSelect(
                "SYSTEM_PROPERTIES",
                "PROPERTY_NAME = 'sql.default_collation'");

            if (rs.next()) {
                value = rs.getString(4);
            }

            rs.close();
        } catch (Exception e) {}

        return value;
    }

    /**
     * Returns the name of the default schema for database.
     */
    String getDatabaseDefaultSchema() throws SQLException {

        final ResultSet rs = executeSelect("SYSTEM_SCHEMAS", "IS_DEFAULT=TRUE");
        String          value = rs.next()
                                ? rs.getString(1)
                                : null;

        rs.close();

        return value;
    }

    String getConnectionDefaultSchema() throws SQLException {

        ResultSet rs = execute("CALL CURRENT_SCHEMA");

        rs.next();

        String result = rs.getString(1);

        rs.close();

        return result;
    }

    void setConnectionDefaultSchema(String schemaName) throws SQLException {

        execute(
            "SET SCHEMA " + StringConverter.toQuotedString(schemaName,
                    '"',
                    true));
    }

    /**
     * For compatibility, when the connection property "default_schema=true"
     * is present, any DatabaseMetaData call with an empty string as the
     * schema parameter will use the default schema (normally "PUBLIC").
     */
    private String translateSchema(String schemaName) throws SQLException {

        if (useSchemaDefault && schemaName != null && schemaName.isEmpty()) {
            final String result = getDatabaseDefaultSchema();

            if (result != null) {
                schemaName = result;
            }
        }

        return schemaName;
    }

    /**
     * Returns the name of the catalog of the default schema.
     */
    String getDatabaseDefaultCatalog() throws SQLException {

        final ResultSet rs = executeSelect("SYSTEM_SCHEMAS", "IS_DEFAULT=TRUE");
        String          value = rs.next()
                                ? rs.getString(2)
                                : null;

        rs.close();

        return value;
    }

    /**
     * For compatibility, when the connection property "default_schema=true"
     * is present, any DatabaseMetaData call with an empty string as the
     * catalog parameter will use the default catalog "PUBLIC".
     */
    private String translateCatalog(String catalogName) throws SQLException {

        if (useSchemaDefault && catalogName != null && catalogName.isEmpty()) {
            String result = getDatabaseDefaultCatalog();

            if (result != null) {
                catalogName = result;
            }
        }

        return catalogName;
    }
}
