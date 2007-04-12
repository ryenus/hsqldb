/* Copyright (c) 2001-2007, The HSQL Development Group
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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.hsqldb.Trace;
import org.hsqldb.Types;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.result.ResultMetaData;

/* $Id: jdbcResultSetMetaData.java,v 1.15 2006/07/12 12:29:42 boucherb Exp $ */

// fredt@users    - 20040412 - removed DITypeInfo dependencies
// boucherb@users - 200404xx - removed unused imports;refinement for better
//                             usability of getColumnDisplaySize;
//                             javadoc updates
// boucherb@users - 20051207 - patch 1.8.0.x initial JDBC 4.0 support work
// boucherb@users - 20060522 - doc   1.9.0 full synch up to Mustang Build 84
/*
 * $Log: jdbcResultSetMetaData.java,v $
 * Revision 1.15  2006/07/12 12:29:42  boucherb
 * patch 1.9.0
 * - full synch up to Mustang b90
 * - minor update to toString
 *
 */

/**
 * <!-- start generic documentation -->
 * An object that can be used to get information about the types
 * and properties of the columns in a <code>ResultSet</code> object.
 * The following code fragment creates the <code>ResultSet</code> object rs,
 * creates the <code>ResultSetMetaData</code> object rsmd, and uses rsmd
 * to find out how many columns rs has and whether the first column in rs
 * can be used in a <code>WHERE</code> clause.
 * <PRE>
 *
 *     ResultSet rs = stmt.executeQuery("SELECT a, b, c FROM TABLE2");
 *     ResultSetMetaData rsmd = rs.getMetaData();
 *     int numberOfColumns = rsmd.getColumnCount();
 *     boolean b = rsmd.isSearchable(1);
 *
 * </PRE>
 * <!-- end generic documentation -->
 *
 * <!-- start Release-specific documentation -->
 * <div class="ReleaseSpecificDocumentation">
 * <h3>HSQLDB-Specific Information:</h3> <p>
 *
 * HSQLDB supports a subset of the <code>ResultSetMetaData</code> interface.<p>
 *
 * The JDBC specification for <code>ResultSetMetaData</code> is in part very
 * vague. This causes potential incompatibility between interpretations of the
 * specification as realized in different JDBC driver implementations. As such,
 * deciding to what degree reporting ResultSetMetaData is accurate has been
 * considered very carefully. Hopefully, the design decisions made in light of
 * these considerations have yeilded precisely the subset of full
 * ResultSetMetaData support that is most commonly needed and that is most
 * important, while also providing, under the most common use-cases, the
 * fastest access with the least overhead and the best comprimise between
 * speed, accuracy, jar-footprint and retention of JDBC resources. <p>
 *
 * (fredt@users) <br>
 * (boucherb@users)<p>
 * </div>
 * <!-- end release-specific documentation -->
 *
 * @author boucherb@users
 * @version 1.8.x
 * @revised JDK 1.6, HSQLDB 1.8.x
 * @see jdbcStatement#executeQuery
 * @see jdbcStatement#getResultSet
 * @see java.sql.ResultSetMetaData
 */
public class jdbcResultSetMetaData implements ResultSetMetaData {

    /**
     * <!-- start generic documentation -->
     * Returns the number of columns in this <code>ResultSet</code> object.
     *
     * <!-- end generic documentation -->
     * @return the number of columns
     * @exception SQLException if a database access error occurs
     */
    public int getColumnCount() throws SQLException {
        return columnCount;
    }

    /**
     * <!-- start generic documentation -->
     * Indicates whether the designated column is automatically numbered.
     * <p>(JDBC4 deleted:)[, thus read-only.]
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.7.1 did not report this value accurately,
     * either always throwing or always returning false, depending
     * upon client property values.<p>
     *
     * Starting with HSQLDB 1.7.2, this feature is better supported. <p>
     *
     * <hr>
     *
     * However, it must be stated here that contrary to the generic
     * documentation previous to the JDBC4 specification, HSQLDB automatically
     * numbered columns (IDENTITY columns, in HSQLDB parlance) are not
     * read-only. <p>
     *
     * In fact, the generic documentation previous to the JDBC4 specification
     * seems to contradict the general definition of what, at minimum,
     * an auto-increment column is: <p>
     *
     * Simply, an auto-increment column is one that guarantees it has a
     * unique value after a successful insert or update operation, even if
     * no value is supplied or NULL is explicitly specified by the application
     * or a default value expression. <p>
     *
     * Further, without SQL Feature T176, Sequence generator support, the
     * attributes of the internal source consulted for unique values are not
     * defined. That is, unlike for a standard SQL SEQUENCE object or a system
     * with full SQL 9x or 200n support for SQL Feature T174, Identity columns,
     * an application must not assume and cannot determine in a standard way
     * that auto-increment values start at any particular point, increment by
     * any particular value or have any of the other attributes generally
     * associated with SQL SEQUENCE generators. Further still, without full
     * support for both feature T174 and T176, if a unique value is supplied
     * by an application or provided by a declared or implicit default value
     * expression, then whether that value is used or substituted with one
     * from the automatic unique value  source is implementation-defined
     * and cannot be known in a standard way. Finally, without full support
     * for features T174 and T176, it is also implementation-defined and
     * cannot be know in a standard way whether an exception is thrown or
     * a unique value is automatically substituted when an application or
     * default value expression supplies a non-NULL,
     * non-unique value. <p>
     *
     * Up to and including HSQLDB 1.8.x, values supplied by an application or
     * default value expression are used if they are indeed non-NULL unique
     * values, while an exception is thrown if either possible value source
     * for the site attempts to supply a non-NULL, non-unique value.  This is
     * very likely to remain the behaviour of HSQLDB for its foreseable
     * lifetime and at the very least for the duration of the 1.y.x release
     * series.<p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isAutoIncrement(int column) throws SQLException {

        checkColumn(column);

        return columnMetaData[--column].isAutoIncrement;
    }

    /**
     * <!-- start generic documentation -->
     * Indicates whether a column's case matters.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.7.1 did not report this value accurately.  <p>
     *
     * Starting with 1.7.2, this feature is better supported.  <p>
     *
     * This method returns true for any column whose data type is a character
     * type, with the exception of VARCHAR_IGNORECASE for which it returns
     * false. It also returns false for any column whose data type is a
     * not a character data type. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isCaseSensitive(int column) throws SQLException {

        checkColumn(column);

        return columnMetaData[--column].isCaseSensitive;
    }

    /**
     * <!-- start generic documentation -->
     * Indicates whether the designated column can be used in a where clause.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.7.1 did not report this value accurately.  <p>
     *
     * Starting with 1.7.2, this feature is better supported.  <p>
     *
     * If the data type of the column is definitely known to be searchable
     * in any way under HSQLDB, then true is returned, else false.  That is,
     * if the type is reported in DatabaseMetaData.getTypeInfo() as having
     * DatabaseMetaData.typePredNone or is not reported, then false is
     * returned, else true.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isSearchable(int column) throws SQLException {

        checkColumn(column);

        return columnMetaData[--column].isSearchable;
    }

    /**
     * <!-- start generic documentation -->
     * Indicates whether the designated column is a cash value.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Up to and including HSQLDB 1.8.0.x, this method always returns
     * false. <p>
     *
     * This is because HSQLDB did not always enforce DECIMAL and NUMERIC
     * (precision, scale) constraints; still allows them to be turned on and
     * off at database scope; and to present does not recheck such contraints
     * when database scope checking is switched from off to on.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isCurrency(int column) throws SQLException {

        checkColumn(column);

        return columnMetaData[--column].isCurrency;
    }

    /**
     * <!-- start generic documentation -->
     * Indicates the nullability of values in the designated column.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Up to 1.7.1, HSQLDB did not report this value accurately. <p>
     *
     * Starting with 1.7.2, this feature is better supported.  <p>
     *
     * <tt>columnNullableUnknown</tt> is always returned for result set columns
     * that do not directly represent table column values (i.e. are calculated),
     * while the corresponding value in [INFORMATION_SCHEMA.]SYSTEM_COLUMNS.NULLABLE
     * is returned for result set columns that do directly represent table
     * column values. <p>
     *
     * To determine the nullable status of a table column in isolation from
     * ResultSetMetaData and in a DBMS-independent fashion, the
     * DatabaseMetaData.getColumns() method should be invoked with the
     * appropriate filter values and the result should be inspected at the
     * position described in the DatabaseMetaData.getColumns() API
     * documentation.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the nullability status of the given column; one of <code>columnNoNulls</code>,
     *          <code>columnNullable</code> or <code>columnNullableUnknown</code>
     * @exception SQLException if a database access error occurs
     */
    public int isNullable(int column) throws SQLException {

        checkColumn(column);

        return columnMetaData[--column].isNullable;
    }

    /**
     * <!-- start generic documentation -->
     * Indicates whether values in the designated column are signed numbers.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.7.1 introduced support for this feature and later releases
     * report the same values as 1.7.1 (although using a slightly different
     * implementation).<p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isSigned(int column) throws SQLException {

        checkColumn(column);

        return columnMetaData[--column].isSigned;
    }

    /**
     * <!-- start generic documentation -->
     * Indicates the designated column's normal maximum width in characters.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Up to and including HSQLDB 1.7.1, this method always returned
     * 0, which was intended to convey unknown display size.
     * Unfortunately, this value is not universally handled by all
     * clients and in the worst case can cause some applications to
     * crash. <p>
     *
     * Starting with 1.7.2, this feature is better supported.  <p>
     *
     * The current calculation follows these rules: <p>
     *
     * <ol>
     * <li>Long character types and datetime types:<p>
     *
     *     The maximum length/precision, repectively.<p>
     *
     * <li>CHAR and VARCHAR types: <p>
     *
     *      <ul>
     *      <li> If the result set column is a direct pass through of a table
     *           column value and column size was declared, then the declared
     *           value is returned. <p>
     *
     *           Starting at 1.7.2, the declared size was reported only if
     *           the connection was to an embedded database instance. At some
     *           point leading to 1.8.0.x, the network protocol was upated to
     *           enable equivalent reporting when connected to a network
     *           database server.
     *
     *      <li> Otherwise, the value of the system property
     *           hsqldb.max_xxxchar_display_size or the magic value
     *           32766 (0x7FFE) (tested usable/accepted by most tools and
     *           compatible with assumptions made by java.io read/write
     *           UTF) when the system property is not defined or is not
     *           accessible, due to security constraints. <p>
     *
     *      </ul>
     *
     *      It must be noted that the latter value in no way affects the
     *      ability of the HSQLDB JDBC driver to retrieve longer values
     *      and serves only as the current best effort at providing a
     *      value that maximizes usability across a wide range of tools,
     *      given that the HSQLDB database engine does not require the
     *      length to be declared and does not necessarily enforce it,
     *      even if declared. <p>
     *
     * <li>Number types: <p>
     *
     *     The max precision, plus the length of the negation character (1),
     *     plus (if applicable) the maximum number of characters that may
     *     occupy the exponent character sequence.  Note that some legacy tools
     *     do not correctly handle BIGINT values of greater than 18 digits. <p>
     *
     * <li>BOOLEAN (BIT) type: <p>
     *
     *     The length of the character sequence "false" (5), the longer of the
     *     two boolean value String representations. <p>
     *
     * <li>Remaining types: <p>
     *
     *     The maximum length/precision, respectively, as reported by
     *     DatabaseMetaData.getTypeInfo(), when applicable.  If the maximum
     *     display size is unknown, unknowable or inapplicable, then zero is
     *     returned. <p>
     *
     * </ol>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the normal maximum number of characters allowed as the width
     *          of the designated column
     * @exception SQLException if a database access error occurs
     */
    public int getColumnDisplaySize(int column) throws SQLException {

        checkColumn(column);

        return columnMetaData[--column].columnDisplaySize;
    }

    /**
     * <!-- start generic documentation -->
     * Gets the designated column's suggested title for use in printouts and
     * displays. (JDBC4 clarification:) The suggested title is usually specified by the SQL <code>AS</code>
     * clause.  If a SQL <code>AS</code> is not specified, the value returned from
     * <code>getColumnLabel</code> will be the same as the value returned by the
     * <code>getColumnName</code> method.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * In HSQLDB, a <code>ResultSet</code> column label is determined using the
     * following order of precedence:<p>
     *
     * <OL>
     * <LI>The label (alias) specified in the generating query.</LI>
     * <LI>The name of the underlying column, if no label is specified.<br>
     *    This also applies to aggregate functions.</LI>
     * <LI>An empty <code>String</code>.</LI>
     * </OL> <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the suggested column title
     * @exception SQLException if a database access error occurs
     */
    public String getColumnLabel(int column) throws SQLException {

        checkColumn(column);

        return columnMetaData[--column].columnLabel;
    }

    /**
     * <!-- start generic documentation -->
     * Get the designated column's name.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * In HSQLDB, a ResultSet column name is determined using the following
     * order of prcedence:<p>
     *
     * <OL>
     * <LI>The name of the underlying columnm, if the ResultSet column
     *   represents a column in a table.</LI>
     * <LI>The label or alias specified in the generating query.</LI>
     * <LI>An empty <code>String</code>.</LI>
     * </OL> <p>
     *
     * If the <code>jdbc.get_column_name</code> property of the database
     * has been set to false, this method returns the same value as
     * {@link #getColumnLabel(int)}.<p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return column name
     * @exception SQLException if a database access error occurs
     */
    public String getColumnName(int column) throws SQLException {

        checkColumn(column);

        column--;

        return useColumnName ? columnMetaData[column].columnName
                             : columnMetaData[column].columnLabel;
    }

    /**
     * <!-- start generic documentation -->
     * Get the designated column's table's schema.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Up to 1.7.1, HSQLDB did not support the notion of schemas at all,
     * including schema names in result set metadata; this method always
     * returned "" (the empty string). <p>
     *
     * Staring at 1.7.2, synthetic schema name reporting was supported, but only
     * as an optional, experimental feature that was disabled by default.
     * Enabling this feature required setting the database property
     * "hsqldb.schemas=true". <p>
     *
     * Since 1.8.0.x, HSQLDB implements relatively standard SQL SCHEMA support;
     * this method returns the actual schema of the column's table.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return schema name or "" if not applicable
     * @exception SQLException if a database access error occurs
     */
    public String getSchemaName(int column) throws SQLException {

        checkColumn(column);

        return columnMetaData[--column].schemaName;
    }

    /**
     * <!-- start generic documentation -->
     * (JDBC4 clarification:)
     * Get the designated column's specified column size.
     * For numeric data, this is the maximum precision.  For character data, this is the [maximum] length in characters.
     * For datetime datatypes, this is the [maximim] length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the [maximum] length in bytes.  For the ROWID datatype,
     * this is the length in bytes[, as returned by the implementation-specific java.sql.RowId.getBytes() method]. 0 is returned for data types where the
     * column size is not applicable.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.8.0, HSQLDB reports the declared length or precision
     * specifiers for table columns, if they are defined.<p>
     *
     * To pesent, precision declaration is still optional in the HSQLDB DDL
     * dialect because it may or may not be enforced, depending on the value of
     * the database property: <p>
     *
     * <pre>
     * sql.enforce_strict_size
     * </pre>
     *
     * Because the property may change from one instantiation of a Database
     * to the next (as well as possibly even during the lifetime of a session)
     * and because, even when set true, is not applied to pre-existing
     * values in table columns (only to new values introduced by following
     * inserts and updates), the length and/or precision specifiers for table
     * columns still do not neccessarily accurately reflect true constraints
     * upon the contents of the columns. This situation may or may not change
     * in a future release. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return precision
     * @exception SQLException if a database access error occurs
     */
    public int getPrecision(int column) throws SQLException {

        checkColumn(column);

        return columnMetaData[--column].precision;
    }

    /**
     * <!-- start generic documentation -->
     * Gets the designated column's number of digits to right of the decimal point.
     * 0 is returned for data types where the scale is not applicable.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Starting with 1.8.0, HSQLDB reports the declared
     * scale for table columns depending on the value of the database
     * property: <p>
     *
     * <pre>
     * sql.enforce_strict_size
     * </pre>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return scale
     * @exception SQLException if a database access error occurs
     */
    public int getScale(int column) throws SQLException {

        checkColumn(column);

        return columnMetaData[--column].scale;
    }

    /**
     * <!-- start generic documentation -->
     * Gets the designated column's table name.
     *
     * <!-- end generic documentation -->
     * @param column the first column is 1, the second is 2, ...
     * @return table name or "" if not applicable
     * @exception SQLException if a database access error occurs
     */
    public String getTableName(int column) throws SQLException {

        checkColumn(column);

        return columnMetaData[--column].tableName;
    }

    /**
     * <!-- start generic documentation -->
     * Gets the designated column's table's catalog name.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Up to and including 1.7.1, HSQLDB did not support the notion of
     * catalog and this method always returned "". <p>
     *
     * Starting with 1.7.2, HSQLDB supports catalog reporting only as an
     * optional, experimental feature that is disabled by default. Enabling
     * this feature requires <code>SET PROPERTY "hsqldb.catalogs" TRUE</code>.
     * When enabled, the catalog name for table columns is reported as the
     * name by which the hosting Database knows itself (i.e. the HSQLDB
     * database URI: (file:canonicalpath | res:path | mem:id). <p>
     *
     * HSQLDB does not yet support any use of catalog qualification in
     * DLL or DML. This fact is accurately indicated in the corresponding
     * DatabaseMetaData.supportsXXX() method return values. <p>
     *
     * However, not all clients respect the DatabaseMetaData.supportsXXX()
     * method return values, so "hsqldb.catalogs" typically must be set FALSE
     * for correct interoperation with tools that depend heavily upon correct
     * metadata, especially reverse-engineering and Object-Relational mapping
     * tools. <p>
     *
     * For greater detail, see discussion at:
     * {@link jdbcDatabaseMetaData}. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the name of the catalog for the table in which the given column
     *          appears or "" if not applicable
     * @exception SQLException if a database access error occurs
     */
    public String getCatalogName(int column) throws SQLException {

        checkColumn(column);

        return columnMetaData[--column].catalogName;
    }

    /**
     * <!-- start generic documentation -->
     * Retrieves the designated column's SQL type.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * This reports the SQL type of the column. HSQLDB can return Objects in
     * any Java integral type wider than <code>Integer</code> for an SQL
     * integral type.<p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     *
     * @param column the first column is 1, the second is 2, ...
     * @return SQL type from java.sql.Types
     * @exception SQLException if a database access error occurs
     * @see java.sql.Types
     */
    public int getColumnType(int column) throws SQLException {

        checkColumn(column);

        return columnMetaData[--column].columnType.getJDBCTypeNumber();
    }

    /**
     * <!-- start generic documentation -->
     * Retrieves the designated column's database-specific type name.
     * <!-- end generic documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return type name used by the database. If the column type is
     * a user-defined type, then a fully-qualified type name is returned.
     * @exception SQLException if a database access error occurs
     */
    public String getColumnTypeName(int column) throws SQLException {

        checkColumn(column);

        return columnMetaData[--column].columnType.getName();
    }

    /**
     * <!-- start generic documentation -->
     * Indicates whether the designated column is definitely not writable.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Up to and including 1.7.1, HSQLDB did not report this value accurately.
     * <p>
     *
     * Starting with HSQLDB 1.7.2, this feature is better supported. <p>
     *
     * For result set columns that do not directly represent table column
     * values (i.e. are calculated), true is reported. Otherwise, the current
     * read only status of the table and the database are used in the
     * calculation, but <em>not</em> the read-only status of the session. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isReadOnly(int column) throws SQLException {

        checkColumn(column);

        return columnMetaData[--column].isReadOnly;
    }

    /**
     * <!-- start generic documentation -->
     * Indicates whether it is possible for a write on the designated column to succeed.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * Up to and including 1.7.1, HSQLDB did not report this value accurately.
     * <p>
     *
     * Starting with HSQLDB 1.7.2, this feature is better supported. <p>
     *
     * In essense, the negation of isReadOnly() is reported. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isWritable(int column) throws SQLException {

        checkColumn(column);

        return columnMetaData[--column].isWritable;
    }

    /**
     * <!-- start generic documentation -->
     * Indicates whether a write on the designated column will definitely succeed.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.7.1 did not report this value accurately. <p>
     *
     * Starting with HSQLDB 1.7.2, this method always returns false. The
     * reason for this is that it is generally either very complex or
     * simply impossible to calculate deterministically true for table columns
     * under all concievable conditions. The value is of dubious usefulness,
     * except perhaps if there were support for updateable result sets using
     * "SELECT ... FOR UPDATE" style locking.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return <code>true</code> if so; <code>false</code> otherwise
     * @exception SQLException if a database access error occurs
     */
    public boolean isDefinitelyWritable(int column) throws SQLException {

        checkColumn(column);

// NOTE:
// boucherb@users - 20020329
// currently, we can't tell _for sure_ that this is true without a
// great deal more work (and even then, not necessarily deterministically),
// plus it is of dubious usefulness to know this is true _for sure_ anyway.
// It's not like one can do an insert or update without a try-catch block
// using JDBC, even if one knows that a column is definitely writable.  And
// the catch will always let us know if there is a failure and why.  Also,
// it is typically completely useless to know that, although it is  _possible_
// that a write may succeed (as indicated by a true value of isWritable()),
// it also might fail (as indicated by a false returned here).
        // as of 1.7.2, always false
        return columnMetaData[--column].isDefinitelyWritable;
    }

    //--------------------------JDBC 2.0-----------------------------------

    /**
     * <!-- start generic documentation -->
     * <p>Returns the fully-qualified name of the Java class whose instances
     * are manufactured if the method <code>ResultSet.getObject</code>
     * is called to retrieve a value
     * from the column.  <code>ResultSet.getObject</code> may return a subclass of the
     * class returned by this method.
     * <!-- end generic documentation -->
     *
     * <!-- start Release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <h3>HSQLDB-Specific Information:</h3> <p>
     *
     * HSQLDB 1.7.1 and previous did not support this feature; calling this method
     * always caused an <code>SQLException</code> to be thrown,
     * stating that the function was not supported. <p>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param column the first column is 1, the second is 2, ...
     * @return the fully-qualified name of the class in the Java programming
     *         language that would be used by the method
     * <code>ResultSet.getObject</code> to retrieve the value in the specified
     * column. This is the class name used for custom mapping.
     * @exception SQLException if a database access error occurs
     * @since JDK 1.2 (JDK 1.1.x developers: read the overview for
     *      jdbcResultSet)
     */
    public String getColumnClassName(int column) throws SQLException {

        checkColumn(column);

        return columnMetaData[--column].columnClassName;
    }

    //----------------------------- JDBC 4.0 -----------------------------------
    // ------------------- java.sql.Wrapper implementation ---------------------

    /**
     * Returns an object that implements the given interface to allow access to
     * non-standard methods, or standard methods not exposed by the proxy.
     *
     * If the receiver implements the interface then the result is the receiver
     * or a proxy for the receiver. If the receiver is a wrapper
     * and the wrapped object implements the interface then the result is the
     * wrapped object or a proxy for the wrapped object. Otherwise return the
     * the result of calling <code>unwrap</code> recursively on the wrapped object
     * or a proxy for that result. If the receiver is not a
     * wrapper and does not implement the interface, then an <code>SQLException</code> is thrown.
     *
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws java.sql.SQLException If no object found that implements the interface
     * @since JDK 1.6, HSQLDB 1.8.x
     */
//#ifdef JDBC4
    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
        if (isWrapperFor(iface)) {
            return (T) this;
        }

        throw Util.invalidArgument("iface: " + iface);
    }

//#endif JDBC4

    /**
     * Returns true if this either implements the interface argument or is directly or indirectly a wrapper
     * for an object that does. Returns false otherwise. If this implements the interface then return true,
     * else if this is a wrapper then return the result of recursively calling <code>isWrapperFor</code> on the wrapped
     * object. If this does not implement the interface and is not a wrapper, return false.
     * This method should be implemented as a low-cost operation compared to <code>unwrap</code> so that
     * callers can use this method to avoid expensive <code>unwrap</code> calls that may fail. If this method
     * returns true then calling <code>unwrap</code> with the same argument should succeed.
     *
     * @param iface a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that does.
     * @throws java.sql.SQLException  if an error occurs while determining whether this is a wrapper
     * for an object with the given interface.
     * @since JDK 1.6, HSQLDB 1.8.x
     */
//#ifdef JDBC4
    public boolean isWrapperFor(java.lang.Class<?> iface) throws java.sql.SQLException {
        return (iface != null && iface.isAssignableFrom(this.getClass()));
    }

//#endif JDBC4
// ------------------------- Internal Implementation ---------------------------

    /**
     * An array of objects, each of which represents the reported attributes
     * for a single column of this object's parent ResultSet.
     */
    private jdbcColumnMetaData[] columnMetaData;

    /** The number of columns in this object's parent ResultSet. */
    private int columnCount;

    /**
     * Whether to use the underlying column name or label when reporting
     * getColumnName().
     */
    private boolean useColumnName;

    /**
     * Constructs a new jdbcResultSetMetaData object from the specified
     * jdbcResultSet and HsqlProprties objects.
     *
     * @param rs the jdbcResultSet object from which to construct a new
     *        jdbcResultSetMetaData object
     * @param props the HsqlProperties object from which to construct a
     *        new jdbcResultSetMetaData object
     * @throws SQLException if a database access error occurs
     */
    jdbcResultSetMetaData(ResultMetaData meta,
                          HsqlProperties props) throws SQLException {
        init(meta, props);
    }

    /**
     *  Initializes this jdbcResultSetMetaData object from the specified
     *  Result and HsqlProperties objects.
     *
     *  @param r the Result object from which to initialize this
     *         jdbcResultSetMetaData object
     *  @param props the HsqlProperties object from which to initialize this
     *         jdbcResultSetMetaData object
     *  @throws SQLException if a database access error occurs
     */
    void init(ResultMetaData meta, HsqlProperties props) throws SQLException {

        jdbcColumnMetaData cmd;
        ResultMetaData     rmd;

        columnCount = meta.getColumnCount();

        // fredt -  props is null for internal connections, so always use the
        //          default behaviour in this case
        useColumnName = (props == null) ? true
                                        : props.isPropertyTrue(
                                            "get_column_name",

        // jdbcDriver.getPropertyInfo says
        // default is true
        true);
        columnMetaData = new jdbcColumnMetaData[columnCount];
        rmd            = meta;

        for (int i = 0; i < columnCount; i++) {
            cmd               = new jdbcColumnMetaData();
            columnMetaData[i] = cmd;

            // Typically, these null checks are not needed, but as
            // above, it is not _guaranteed_ that these values
            // will be non-null.   So, it is better to do the work
            // here than have to perform checks and conversions later.
            cmd.catalogName = rmd.catalogNames[i] == null ? ""
                                                          : rmd
                                                          .catalogNames[i];
            cmd.schemaName  = rmd.schemaNames[i] == null ? ""
                                                         : rmd.schemaNames[i];
            cmd.tableName   = rmd.tableNames[i] == null ? ""
                                                        : rmd.tableNames[i];
            cmd.columnName  = rmd.colNames[i] == null ? ""
                                                      : rmd.colNames[i];
            cmd.columnLabel = rmd.colLabels[i] == null ? ""
                                                       : rmd.colLabels[i];
            cmd.columnType  = rmd.colTypes[i];
            cmd.isWritable  = rmd.isWritable[i];
            cmd.isReadOnly  = !cmd.isWritable;

            // default: cmd.isDefinitelyWritable = false;
            cmd.isAutoIncrement = rmd.isIdentity[i];
            cmd.isNullable      = rmd.colNullable[i];
            cmd.columnClassName = rmd.classNames[i];

            int type = cmd.columnType.type;

            if (cmd.columnClassName == null
                    || cmd.columnClassName.length() == 0) {
                cmd.columnClassName = cmd.columnType.getJDBCClassName();
            }

            // Some tools, such as PowerBuilder, require that (for char and
            // varchar types, at any rate) getMaxDisplaySize returns a value
            // _at least_ as large as the length of the longest value in this
            // column of the result set, or else an internal error will occur
            // at retrieve time the instant a longer value is fetched.
            //
            // org.hsqldb.Types has been patched to retrieve, by default, either
            // a large-but-reasonable value or a value defined through system
            // properties that is expected to be unlikely to cause problems in
            // the majority of cases.
            cmd.columnDisplaySize = rmd.colTypes[i].displaySize();

            long precision = rmd.colTypes[i].size();
            cmd.precision = precision > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) precision;

            // Without a non-zero scale value, some (legacy only?) tools will
            // simply truncate digits to the right of the decimal point when
            // retrieving values from the result set. Before 1.8.0, the measure
            // below helps, but only as long as one is connected to an
            // embedded instance at design time, not via a network connection.
            // From 1.8.0, the HSQL network protocol transmits scale, so this
            // becomes a non-issue.
            if (Types.acceptsScaleCreateParam(type)) {
                cmd.scale = rmd.colTypes[i].scale();
            }

            Boolean iua = Types.isUnsignedAttribute(type);

            cmd.isSigned = iua != null &&!iua.booleanValue();

            Boolean ics = Types.isCaseSensitive(type);

            cmd.isCaseSensitive = ics != null && ics.booleanValue();
            cmd.isSearchable    = Types.isSearchable(type);
        }
    }

    /**
     * Performs an internal check for column index validity. <p>
     *
     * @param column index of column to check
     * @throws SQLException when this object's parent ResultSet has
     *      no such column
     */
    private void checkColumn(int column) throws SQLException {

        if (column < 1 || column > columnCount) {
            throw Util.sqlException(Trace.COLUMN_NOT_FOUND,
                                    String.valueOf(column));
        }
    }

    /**
     * Returns a string representation of the object. <p>
     *
     * The string consists of the name of the class of which the
     * object is an instance, the at-sign character `<code>@</code>',
     * the unsigned hexadecimal representation of the hash code of the
     * object and a comma-delimited list of this object's indexed attributes,
     * enclosed in square brakets.
     *
     * @return  a string representation of the object.
     */
    public String toString() {

        StringBuffer sb = new StringBuffer();

        sb.append(super.toString());

        if (columnCount == 0) {
            sb.append("[columnCount=0]");

            return sb.toString();
        }

        sb.append('[');

        for (int i = 0; i < columnCount; i++) {
            sb.append('\n');
            sb.append("   column_");
            sb.append(i + 1);
            sb.append('=');
            sb.append(columnMetaData[i]);

            if (i + 1 < columnCount) {
                sb.append(',');
                sb.append(' ');
            }
        }

        sb.append('\n');
        sb.append(']');

        return sb.toString();
    }
}
