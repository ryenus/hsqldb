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

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;

import java.math.BigDecimal;
import java.math.RoundingMode;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;

import java.time.Duration;
import java.time.Period;

import java.util.Calendar;
import java.util.Locale;
import java.util.Map;

import org.hsqldb.HsqlException;
import org.hsqldb.SchemaObject;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.IntValueHashMap;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.IntervalMonthData;
import org.hsqldb.types.IntervalSecondData;
import org.hsqldb.types.JavaObjectData;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;

/* @todo fredt 1.9.0 - continuous review wrt multiple result sets, named parameters etc. */

// campbell-burnet@users patch 1.7.2 - CallableStatement impl removed
// from JDBCParameterMetaData and moved here; sundry changes elsewhere to
// comply
// TODO: 1.7.2 Alpha N :: DONE
//       maybe implement set-by-parameter-name.  We have an informal spec,
//       being "@p1" => 1, "@p2" => 2, etc.  Problems: return value is "@p0"
//       and there is no support for registering the return value as an out
//       parameter.
// TODO: 1.9.x :: DONE
//       engine and client-side mechanisms for adding, retrieving,
//       navigating (and perhaps controlling holdability of) multiple
//       results generated from a single execution.
// campbell-burnet@users 2004-03-xx - patch 1.7.2 - some minor code cleanup
//                                            - parameter map NPE correction
//                                            - embedded SQL/SQLCLI client usability
//                                              (parameter naming changed from @n to @pn)
// campbell-burnet@users 2004-04-xx - doc 1.7.2 - javadocs added/updated
// campbell-burnet@users 2005-12-07 - patch 1.8.0.x - initial JDBC 4.0 support work
// campbell-burnet@users 2006-05-22 - doc 1.9.0 - full synch up to JAVA 1.6 (Mustang) Build 84
// campbell-burnet@users 2006-07-12 - full synch up to JAVA 1.6 (Mustang) b90

/**
 * The interface used to execute SQL stored procedures.  The JDBC API
 * provides a stored procedure SQL escape syntax that allows stored procedures
 * to be called in a standard way for all RDBMSs. This escape syntax has one
 * form that includes a result parameter and one that does not. If used, the result
 * parameter must be registered as an OUT parameter. The other parameters
 * can be used for input, output or both. Parameters are referred to
 * sequentially, by number, with the first parameter being 1.
 * <PRE>
 *   {?= call &lt;procedure-name&gt;[(&lt;arg1&gt;,&lt;arg2&gt;, ...)]}
 *   {call &lt;procedure-name&gt;[(&lt;arg1&gt;,&lt;arg2&gt;, ...)]}
 * </PRE>
 * <P>
 * IN parameter values are set using the {@code set} methods inherited from
 * {@link java.sql.PreparedStatement}.  The type of all OUT parameters must be
 * registered prior to executing the stored procedure; their values
 * are retrieved after execution via the {@code get} methods provided here.
 * <P>
 * A {@code CallableStatement} can return one {@link java.sql.ResultSet} object or
 * multiple {@code ResultSet} objects.  Multiple
 * {@code ResultSet} objects are handled using operations
 * inherited from {@link java.sql.Statement}.
 * <P>
 * For maximum portability, a call's {@code ResultSet} objects and
 * update counts should be processed prior to getting the values of output
 * parameters.
 *
 * <!-- start Release-specific documentation -->
 * <div class="ReleaseSpecificDocumentation">
 * <p class="rshead">HSQLDB-Specific Information:</p>
 *
 * Beyond the XOpen/ODBC extended scalar functions, stored procedures are
 * typically supported in ways that vary greatly from one DBMS implementation
 * to the next.  So, it is almost guaranteed that the code for a stored
 * procedure written under a specific DBMS product will not work without
 * at least some modification in the context of another vendor's product
 * or even across a single vendor's product lines.  Moving stored procedures
 * from one DBMS product line to another almost invariably involves complex
 * porting issues and often may not be possible at all. <em>Be warned</em>. <p>
 *
 * One kind of HSQLDB stored procedures and functions is SQL/JRT, Java routines
 * that map directly onto the static methods of compiled Java classes found on
 * the class path of the engine at runtime. The CREATE PROCEDURE or CREATE FUNCTION
 * statements are used in SQL to support the Java methods.<p>
 *
 * The other kind of HSQLDB stored procedures is SQL/PSM routines that are
 * written entirely in the SQL procedural language.
 *
 * Overloaded methods are supported and resolved according to the type of
 * parameters.
 *
 * With procedures, {@code OUT} and {@code IN OUT} parameters
 * are also supported. <p>
 *
 * In addition, HSQLDB stored procedure call mechanism allows the
 * more general HSQLDB SQL expression evaluation mechanism.  This
 * extension provides the ability to evaluate simple SQL expressions, possibly
 * containing Java method invocations. <p>
 *
 * With HSQLDB, executing a {@code CALL} statement that produces an opaque
 * (OTHER) or known scalar object reference has virtually the same effect as:
 *
 * <PRE class="SqlCodeExample">
 * CREATE TABLE DUAL (dummy VARCHAR);
 * INSERT INTO DUAL VALUES(NULL);
 * SELECT &lt;simple-expression&gt; FROM DUAL;
 * </PRE>
 *
 * HSQLDB functions can return a single result set. HSQLDB procedures can
 * return one or more result sets.
 *
 * Here is a very simple example of an HSQLDB stored procedure generating a
 * user-defined result set:
 *
 * <pre class="JavaCodeExample">
 * <span class="JavaKeyWord">package</span> mypackage;
 *
 * <span class="JavaKeyWord">import</span> java.sql.ResultSet;
 * <span class="JavaKeyWord">import</span> java.sql.SQLException;
 *
 * <span class="JavaKeyWord">class</span> MyLibraryClass {
 *
 *      <span class="JavaKeyWord">public static</span> ResultSet <b>mySp()</b> <span class="JavaKeyWord">throws</span> SQLException {
 *          <span class="JavaKeyWord">return</span> ctx.<b>getConnection</b>().<b>createStatement</b>().<b>executeQuery</b>(<span class="JavaStringLiteral">"select * from my_table"</span>);
 *      }
 * }
 * </pre>
 *
 * (campbell-burnet@users)
 * </div>
 * <!-- end Release-specific documentation -->
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
 * @since JDK 1.1, HSQLDB 1.9.0
 * @see JDBCConnection#prepareCall
 * @see JDBCResultSet
 */
public class JDBCCallableStatement extends JDBCPreparedStatement
        implements CallableStatement {

    /**
     * Registers the OUT parameter in ordinal position
     * {@code parameterIndex} to the JDBC type
     * {@code sqlType}.  All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by {@code sqlType} for an OUT
     * parameter determines the Java type that must be used
     * in the {@code get} method to read the value of that parameter.
     * <p>
     * If the JDBC type expected to be returned to this output parameter
     * is specific to this particular database, {@code sqlType}
     * should be {@code java.sql.Types.OTHER}.  The method
     * {@link #getObject} retrieves the value.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature. This method can be called after a
     * PrepareCall method. HSQLDB has already determined which parameters are
     * OUT or INOUT parameters, therefore this method only checks and
     * throws an exception if the parameter is not of the correct form.
     * The data type argument is ignored<p>
     *
     * The {@code get} method to read the value of the parameter is
     * determined by the engine based on the data type of the parameter.
     *
     * Furthermore, HSQLDB supports multiple OUT and INOUT parameters for
     * stored procedures.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @param sqlType the JDBC type code defined by {@code java.sql.Types}.
     *        If the parameter is of JDBC type {@code NUMERIC}
     *        or {@code DECIMAL}, the version of
     *        {@code registerOutParameter} that accepts a scale value
     *        should be used.
     *
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if {@code sqlType} is
     * a {@code ARRAY}, {@code BLOB}, {@code CLOB},
     * {@code DATALINK}, {@code JAVA_OBJECT}, {@code NCHAR},
     * {@code NCLOB}, {@code NVARCHAR}, {@code LONGNVARCHAR},
     *  {@code REF}, {@code ROWID}, {@code SQLXML}
     * or  {@code STRUCT} data type and the JDBC driver does not support
     * this data type
     * @see java.sql.Types
     */
    public synchronized void registerOutParameter(
            int parameterIndex,
            int sqlType)
            throws SQLException {

        checkGetParameterIndex(parameterIndex);

        if (parameterModes[--parameterIndex]
                == SchemaObject.ParameterModes.PARAM_IN) {
            throw JDBCUtil.invalidArgument();
        }
    }

    public ResultSet getResultSet() throws SQLException {

        if (resultIn.mode == ResultConstants.CALL_RESPONSE
                && resultIn.getChainedResult() != null) {
            getMoreResults();
        }

        return super.getResultSet();
    }

    /**
     * Registers the parameter in ordinal position
     * {@code parameterIndex} to be of JDBC type
     * {@code sqlType}. All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by {@code sqlType} for an OUT
     * parameter determines the Java type that must be used
     * in the {@code get} method to read the value of that parameter.
     * <p>
     * This version of {@code registerOutParameter} should be
     * used when the parameter is of JDBC type {@code NUMERIC}
     * or {@code DECIMAL}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @param sqlType the SQL type code defined by {@code java.sql.Types}.
     * @param scale the desired number of digits to the right of the
     * decimal point.  It must be greater than or equal to zero.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if {@code sqlType} is
     * a {@code ARRAY}, {@code BLOB}, {@code CLOB},
     * {@code DATALINK}, {@code JAVA_OBJECT}, {@code NCHAR},
     * {@code NCLOB}, {@code NVARCHAR}, {@code LONGNVARCHAR},
     *  {@code REF}, {@code ROWID}, {@code SQLXML}
     * or  {@code STRUCT} data type and the JDBC driver does not support
     * this data type
     * @see java.sql.Types
     */
    public synchronized void registerOutParameter(
            int parameterIndex,
            int sqlType,
            int scale)
            throws SQLException {
        registerOutParameter(parameterIndex, sqlType);
    }

    /**
     * Retrieves whether the last OUT parameter read had the value of
     * SQL {@code NULL}.  Note that this method should be called only after
     * calling a getter method; otherwise, there is no value to use in
     * determining whether it is {@code null} or not.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return {@code true} if the last parameter read was SQL
     * {@code NULL}; {@code false} otherwise
     * @throws SQLException if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     */
    public synchronized boolean wasNull() throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        return wasNullValue;
    }

    /**
     * Retrieves the value of the designated JDBC {@code CHAR},
     * {@code VARCHAR}, or {@code LONGVARCHAR} parameter as a
     * {@code String} in the Java programming language.
     * <p>
     * For the fixed-length type JDBC {@code CHAR},
     * the {@code String} object
     * returned has exactly the same value the SQL
     * {@code CHAR} value had in the
     * database, including any padding added by the database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value. If the value is SQL {@code NULL},
     *         the result
     *         is {@code null}.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @see #setString
     */
    public synchronized String getString(
            int parameterIndex)
            throws SQLException {
        return (String) getColumnInType(parameterIndex, Type.SQL_VARCHAR);
    }

    /**
     * Retrieves the value of the designated JDBC {@code BIT}
     * or {@code BOOLEAN} parameter as a
     * {@code boolean} in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL {@code NULL},
     *         the result is {@code false}.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @see #setBoolean
     */
    public synchronized boolean getBoolean(
            int parameterIndex)
            throws SQLException {
        Object o = getColumnInType(parameterIndex, Type.SQL_BOOLEAN);

        return o != null && ((Boolean) o).booleanValue();
    }

    /**
     * Retrieves the value of the designated JDBC {@code TINYINT} parameter
     * as a {@code byte} in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     * is {@code 0}.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @see #setByte
     */
    public synchronized byte getByte(int parameterIndex) throws SQLException {

        Object o = getColumnInType(parameterIndex, Type.TINYINT);

        return o == null
               ? 0
               : ((Number) o).byteValue();
    }

    /**
     * Retrieves the value of the designated JDBC {@code SMALLINT} parameter
     * as a {@code short} in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     * is {@code 0}.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @see #setShort
     */
    public synchronized short getShort(int parameterIndex) throws SQLException {

        Object o = getColumnInType(parameterIndex, Type.SQL_SMALLINT);

        return o == null
               ? 0
               : ((Number) o).shortValue();
    }

    /**
     * Retrieves the value of the designated JDBC {@code INTEGER} parameter
     * as an {@code int} in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     * is {@code 0}.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @see #setInt
     */
    public synchronized int getInt(int parameterIndex) throws SQLException {

        Object o = getColumnInType(parameterIndex, Type.SQL_INTEGER);

        return o == null
               ? 0
               : ((Number) o).intValue();
    }

    /**
     * Retrieves the value of the designated JDBC {@code BIGINT} parameter
     * as a {@code long} in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     * is {@code 0}.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @see #setLong
     */
    public synchronized long getLong(int parameterIndex) throws SQLException {

        Object o = getColumnInType(parameterIndex, Type.SQL_BIGINT);

        return o == null
               ? 0
               : ((Number) o).longValue();
    }

    /**
     * Retrieves the value of the designated JDBC {@code FLOAT} parameter
     * as a {@code float} in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     *         is {@code 0}.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @see #setFloat
     */
    public synchronized float getFloat(int parameterIndex) throws SQLException {

        Object o = getColumnInType(parameterIndex, Type.SQL_DOUBLE);

        return o == null
               ? (float) 0.0
               : ((Number) o).floatValue();
    }

    /**
     * Retrieves the value of the designated JDBC {@code DOUBLE} parameter as a {@code double}
     * in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     *         is {@code 0}.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @see #setDouble
     */
    public synchronized double getDouble(
            int parameterIndex)
            throws SQLException {

        Object o = getColumnInType(parameterIndex, Type.SQL_DOUBLE);

        return o == null
               ? 0.0
               : ((Number) o).doubleValue();
    }

    /**
     * Retrieves the value of the designated JDBC {@code NUMERIC} parameter as a
     * {@code java.math.BigDecimal} object with <i>scale</i> digits to
     * the right of the decimal point.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @param scale the number of digits to the right of the decimal point
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     *         is {@code null}.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @deprecated use {@code getBigDecimal(int parameterIndex)}
     *             or {@code getBigDecimal(String parameterName)}
     * @see #setBigDecimal
     */

//#ifdef DEPRECATEDJDBC
    @Deprecated
    public synchronized BigDecimal getBigDecimal(
            int parameterIndex,
            int scale)
            throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        if (scale < 0) {
            throw JDBCUtil.outOfRangeArgument();
        }

        BigDecimal bd = getBigDecimal(parameterIndex);

        if (bd != null) {
            bd = bd.setScale(scale, RoundingMode.DOWN);
        }

        return bd;
    }

//#endif DEPRECATEDJDBC

    /**
     * Retrieves the value of the designated JDBC {@code BINARY} or
     * {@code VARBINARY} parameter as an array of {@code byte}
     * values in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     *         is {@code null}.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @see #setBytes
     */
    public synchronized byte[] getBytes(
            int parameterIndex)
            throws SQLException {

        Object x = getColumnInType(parameterIndex, Type.SQL_VARBINARY);

        if (x == null) {
            return null;
        }

        return ((BinaryData) x).getBytes();
    }

    /**
     * Retrieves the value of the designated JDBC {@code DATE} parameter as a
     * {@code java.sql.Date} object.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     *         is {@code null}.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @see #setDate
     */
    public synchronized Date getDate(int parameterIndex) throws SQLException {

        TimestampData t = (TimestampData) getColumnInType(
            parameterIndex,
            Type.SQL_DATE);

        if (t == null) {
            return null;
        }

        return (Date) Type.SQL_DATE.convertSQLToJava(session, t);
    }

    /**
     * Retrieves the value of the designated JDBC {@code TIME} parameter as a
     * {@code java.sql.Time} object.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     *         is {@code null}.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @see #setTime
     */
    public synchronized Time getTime(int parameterIndex) throws SQLException {

        TimeData t = (TimeData) getColumnInType(parameterIndex, Type.SQL_TIME);

        if (t == null) {
            return null;
        }

        return (Time) Type.SQL_TIME.convertSQLToJava(session, t);
    }

    /**
     * Retrieves the value of the designated JDBC {@code TIMESTAMP}
     * parameter as a {@code java.sql.Timestamp} object.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     *         is {@code null}.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @see #setTimestamp
     */
    public synchronized Timestamp getTimestamp(
            int parameterIndex)
            throws SQLException {

        TimestampData t = (TimestampData) getColumnInType(
            parameterIndex,
            Type.SQL_TIMESTAMP);

        if (t == null) {
            return null;
        }

        return (Timestamp) Type.SQL_TIMESTAMP.convertSQLToJava(session, t);
    }

    //----------------------------------------------------------------------
    // Advanced features:

    /**
     * Retrieves the value of the designated parameter as an {@code Object}
     * in the Java programming language. If the value is an SQL {@code NULL},
     * the driver returns a Java {@code null}.
     * <p>
     * This method returns a Java object whose type corresponds to the JDBC
     * type that was registered for this parameter using the method
     * {@code registerOutParameter}.  By registering the target JDBC
     * type as {@code java.sql.Types.OTHER}, this method can be used
     * to read database-specific abstract data types.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @return A {@code java.lang.Object} holding the OUT parameter value
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @see java.sql.Types
     * @see #setObject
     */
    public synchronized Object getObject(
            int parameterIndex)
            throws SQLException {

        checkGetParameterIndex(parameterIndex);

        Type sourceType = parameterTypes[parameterIndex - 1];

        switch (sourceType.typeCode) {

            case Types.SQL_ARRAY :
                return getArray(parameterIndex);

            case Types.SQL_DATE :
                return getDate(parameterIndex);

            case Types.SQL_TIME :
                return getTime(parameterIndex);

            case Types.SQL_TIME_WITH_TIME_ZONE :
                return getTimeWithZone(parameterIndex);

            case Types.SQL_TIMESTAMP :
                return getTimestamp(parameterIndex);

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return getTimestampWithZone(parameterIndex);

            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
                return getBytes(parameterIndex);

            case Types.SQL_BIT : {
                boolean b = getBoolean(parameterIndex);

                return wasNull()
                       ? null
                       : b
                         ? Boolean.TRUE
                         : Boolean.FALSE;
            }

            case Types.SQL_CLOB :
                return getClob(parameterIndex);

            case Types.SQL_BLOB :
                return getBlob(parameterIndex);

            case Types.OTHER :
            case Types.JAVA_OBJECT : {
                Object o = getColumnInType(parameterIndex, sourceType);

                if (o == null) {
                    return null;
                }

                try {
                    return ((JavaObjectData) o).getObject();
                } catch (HsqlException e) {
                    throw JDBCUtil.sqlException(e);
                }
            }

            default :
                return getColumnInType(parameterIndex, sourceType);
        }
    }

    //--------------------------JDBC 2.0-----------------------------

    /**
     * Retrieves the value of the designated JDBC {@code NUMERIC} parameter as a
     * {@code java.math.BigDecimal} object with as many digits to the
     * right of the decimal point as the value contains.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value in full precision.  If the value is
     * SQL {@code NULL}, the result is {@code null}.
     * @throws SQLException  if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @see #setBigDecimal
     * @since JDK 1.2
     */
    public synchronized BigDecimal getBigDecimal(
            int parameterIndex)
            throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        Type targetType = parameterMetaData.columnTypes[parameterIndex - 1];

        switch (targetType.typeCode) {

            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                break;

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
                targetType = Type.SQL_DECIMAL;
                break;

            case Types.SQL_DOUBLE :
            default :
                targetType = Type.SQL_DECIMAL_DEFAULT;
                break;
        }

        return (BigDecimal) getColumnInType(parameterIndex, targetType);
    }

    /**
     * Returns an object representing the value of OUT parameter
     * {@code parameterIndex} and uses {@code map} for the custom
     * mapping of the parameter value.
     * <p>
     * This method returns a Java object whose type corresponds to the
     * JDBC type that was registered for this parameter using the method
     * {@code registerOutParameter}.  By registering the target
     * JDBC type as {@code java.sql.Types.OTHER}, this method can
     * be used to read database-specific abstract data types.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an {@code SQLException}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2, and so on
     * @param map the mapping from SQL type names to Java classes
     * @return a {@code java.lang.Object} holding the OUT parameter value
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setObject
     * @since JDK 1.2
     */
    public Object getObject(
            int parameterIndex,
            Map<String, Class<?>> map)
            throws SQLException {
        checkGetParameterIndex(parameterIndex);

        throw JDBCUtil.notSupported();
    }

    /**
     * Retrieves the value of the designated JDBC {@code REF(<structured-type>)}
     * parameter as a {@link java.sql.Ref} object in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an {@code SQLException}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @return the parameter value as a {@code Ref} object in the
     * Java programming language.  If the value was SQL {@code NULL}, the value
     * {@code null} is returned.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     */
    public Ref getRef(int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);

        throw JDBCUtil.notSupported();
    }

    /**
     * Retrieves the value of the designated JDBC {@code BLOB} parameter as a
     * {@link java.sql.Blob} object in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2, and so on
     * @return the parameter value as a {@code Blob} object in the
     * Java programming language.  If the value was SQL {@code NULL}, the value
     * {@code null} is returned.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     */
    public synchronized Blob getBlob(int parameterIndex) throws SQLException {

        checkGetParameterIndex(parameterIndex);

        Type   sourceType = parameterMetaData.columnTypes[parameterIndex - 1];
        Object o          = getColumnInType(parameterIndex, sourceType);

        if (o == null) {
            return null;
        }

        if (o instanceof BlobDataID) {
            return new JDBCBlobClient(session, (BlobDataID) o);
        }

        throw JDBCUtil.sqlException(ErrorCode.X_42561);
    }

    /**
     * Retrieves the value of the designated JDBC {@code CLOB} parameter as a
     * {@code java.sql.Clob} object in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2, and
     * so on
     * @return the parameter value as a {@code Clob} object in the
     * Java programming language.  If the value was SQL {@code NULL}, the
     * value {@code null} is returned.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     */
    public synchronized Clob getClob(int parameterIndex) throws SQLException {

        checkGetParameterIndex(parameterIndex);

        Type   sourceType = parameterMetaData.columnTypes[parameterIndex - 1];
        Object o          = getColumnInType(parameterIndex, sourceType);

        if (o == null) {
            return null;
        }

        if (o instanceof ClobDataID) {
            return new JDBCClobClient(session, (ClobDataID) o);
        }

        throw JDBCUtil.sqlException(ErrorCode.X_42561);
    }

    /**
     * Retrieves the value of the designated JDBC {@code ARRAY} parameter as an
     * {@link java.sql.Array} object in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2, and
     * so on
     * @return the parameter value as an {@code Array} object in
     * the Java programming language.  If the value was SQL {@code NULL}, the
     * value {@code null} is returned.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     */
    public Array getArray(int parameterIndex) throws SQLException {

        checkGetParameterIndex(parameterIndex);

        Type type = parameterMetaData.columnTypes[parameterIndex - 1];

        if (!type.isArrayType()) {
            throw JDBCUtil.sqlException(ErrorCode.X_42561);
        }

        Object[] data = (Object[]) parameterValues[parameterIndex - 1];

        if (data == null) {
            return null;
        }

        return new JDBCArray(data, type.collectionBaseType(), type, connection);
    }

    /**
     * Retrieves the value of the designated JDBC {@code DATE} parameter as a
     * {@code java.sql.Date} object, using
     * the given {@code Calendar} object
     * to construct the date.
     * With a {@code Calendar} object, the driver
     * can calculate the date taking into account a custom timezone and locale.
     * If no {@code Calendar} object is specified, the driver uses the
     * default timezone and locale.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @param cal the {@code Calendar} object the driver will use
     *            to construct the date
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     *         is {@code null}.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @see #setDate
     * @since JDK 1.2
     */
    public synchronized Date getDate(
            int parameterIndex,
            Calendar cal)
            throws SQLException {

        TimestampData t = (TimestampData) getColumnInType(
            parameterIndex,
            Type.SQL_DATE);

        if (t == null) {
            return null;
        }

        return (Date) Type.SQL_DATE.convertSQLToJava(session, t, cal);
    }

    /**
     * Retrieves the value of the designated JDBC {@code TIME} parameter as a
     * {@code java.sql.Time} object, using
     * the given {@code Calendar} object
     * to construct the time.
     * With a {@code Calendar} object, the driver
     * can calculate the time taking into account a custom timezone and locale.
     * If no {@code Calendar} object is specified, the driver uses the
     * default timezone and locale.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @param cal the {@code Calendar} object the driver will use
     *            to construct the time
     * @return the parameter value; if the value is SQL {@code NULL}, the result
     *         is {@code null}.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @see #setTime
     * @since JDK 1.2
     */
    public synchronized Time getTime(
            int parameterIndex,
            Calendar cal)
            throws SQLException {

        Object t = getColumnValue(parameterIndex);

        if (t == null) {
            return null;
        }

        return (Time) Type.SQL_TIME.convertSQLToJava(session, t, cal);
    }

    /**
     * Retrieves the value of the designated JDBC {@code TIMESTAMP} parameter as a
     * {@code java.sql.Timestamp} object, using
     * the given {@code Calendar} object to construct
     * the {@code Timestamp} object.
     * With a {@code Calendar} object, the driver
     * can calculate the timestamp taking into account a custom timezone and locale.
     * If no {@code Calendar} object is specified, the driver uses the
     * default timezone and locale.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @param cal the {@code Calendar} object the driver will use
     *            to construct the timestamp
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     *         is {@code null}.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @see #setTimestamp
     * @since JDK 1.2
     */
    public synchronized Timestamp getTimestamp(
            int parameterIndex,
            Calendar cal)
            throws SQLException {

        Object t = getColumnValue(parameterIndex);

        if (t == null) {
            return null;
        }

        return (Timestamp) Type.SQL_TIMESTAMP.convertSQLToJava(session, t, cal);
    }

    /**
     * Registers the designated output parameter.
     * This version of
     * the method {@code registerOutParameter}
     * should be used for a user-defined or {@code REF} output parameter.  Examples
     * of user-defined types include: {@code STRUCT}, {@code DISTINCT},
     * {@code JAVA_OBJECT}, and named array types.
     * <p>
     * All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>  For a user-defined parameter, the fully-qualified SQL
     * type name of the parameter should also be given, while a {@code REF}
     * parameter requires that the fully-qualified type name of the
     * referenced type be given.  A JDBC driver that does not need the
     * type code and type name information may ignore it.   To be portable,
     * however, applications should always provide these values for
     * user-defined and {@code REF} parameters.
     *
     * Although it is intended for user-defined and {@code REF} parameters,
     * this method may be used to register a parameter of any JDBC type.
     * If the parameter does not have a user-defined or {@code REF} type, the
     * <i>typeName</i> parameter is ignored.
     *
     * <P><B>Note:</B> When reading the value of an out parameter, you
     * must use the getter method whose Java type corresponds to the
     * parameter's registered SQL type.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,...
     * @param sqlType a value from {@link java.sql.Types}
     * @param typeName the fully-qualified name of an SQL structured type
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if {@code sqlType} is
     * a {@code ARRAY}, {@code BLOB}, {@code CLOB},
     * {@code DATALINK}, {@code JAVA_OBJECT}, {@code NCHAR},
     * {@code NCLOB}, {@code NVARCHAR}, {@code LONGNVARCHAR},
     *  {@code REF}, {@code ROWID}, {@code SQLXML}
     * or  {@code STRUCT} data type and the JDBC driver does not support
     * this data type
     * @see java.sql.Types
     * @since JDK 1.2
     */
    public synchronized void registerOutParameter(
            int parameterIndex,
            int sqlType,
            String typeName)
            throws SQLException {
        registerOutParameter(parameterIndex, sqlType);
    }

// ----------------------------------- JDBC 3.0----------------------------------

    /**
     * Registers the OUT parameter named
     * {@code parameterName} to the JDBC type
     * {@code sqlType}.  All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by {@code sqlType} for an OUT
     * parameter determines the Java type that must be used
     * in the {@code get} method to read the value of that parameter.
     * <p>
     * If the JDBC type expected to be returned to this output parameter
     * is specific to this particular database, {@code sqlType}
     * should be {@code java.sql.Types.OTHER}.  The method
     * {@link #getObject} retrieves the value.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param sqlType the JDBC type code defined by {@code java.sql.Types}.
     * If the parameter is of JDBC type {@code NUMERIC}
     * or {@code DECIMAL}, the version of
     * {@code registerOutParameter} that accepts a scale value
     * should be used.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if {@code sqlType} is
     * a {@code ARRAY}, {@code BLOB}, {@code CLOB},
     * {@code DATALINK}, {@code JAVA_OBJECT}, {@code NCHAR},
     * {@code NCLOB}, {@code NVARCHAR}, {@code LONGNVARCHAR},
     *  {@code REF}, {@code ROWID}, {@code SQLXML}
     * or  {@code STRUCT} data type and the JDBC driver does not support
     * this data type or if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQL 1.7.0
     * @see java.sql.Types
     */
    public synchronized void registerOutParameter(
            String parameterName,
            int sqlType)
            throws SQLException {
        registerOutParameter(findParameterIndex(parameterName), sqlType);
    }

    /**
     * Registers the parameter named
     * {@code parameterName} to be of JDBC type
     * {@code sqlType}. All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by {@code sqlType} for an OUT
     * parameter determines the Java type that must be used
     * in the {@code get} method to read the value of that parameter.
     * <p>
     * This version of {@code registerOutParameter} should be
     * used when the parameter is of JDBC type {@code NUMERIC}
     * or {@code DECIMAL}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param sqlType SQL type code defined by {@code java.sql.Types}.
     * @param scale the desired number of digits to the right of the
     * decimal point.  It must be greater than or equal to zero.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if {@code sqlType} is
     * a {@code ARRAY}, {@code BLOB}, {@code CLOB},
     * {@code DATALINK}, {@code JAVA_OBJECT}, {@code NCHAR},
     * {@code NCLOB}, {@code NVARCHAR}, {@code LONGNVARCHAR},
     *  {@code REF}, {@code ROWID}, {@code SQLXML}
     * or  {@code STRUCT} data type and the JDBC driver does not support
     * this data type or if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     * @see java.sql.Types
     */
    public synchronized void registerOutParameter(
            String parameterName,
            int sqlType,
            int scale)
            throws SQLException {
        registerOutParameter(findParameterIndex(parameterName), sqlType);
    }

    /**
     * Registers the designated output parameter.  This version of
     * the method {@code registerOutParameter}
     * should be used for a user-named or REF output parameter.  Examples
     * of user-named types include: STRUCT, DISTINCT, JAVA_OBJECT, and
     * named array types.
     * <p>
     * All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * For a user-named parameter the fully-qualified SQL
     * type name of the parameter should also be given, while a REF
     * parameter requires that the fully-qualified type name of the
     * referenced type be given.  A JDBC driver that does not need the
     * type code and type name information may ignore it.   To be portable,
     * however, applications should always provide these values for
     * user-named and REF parameters.
     *
     * Although it is intended for user-named and REF parameters,
     * this method may be used to register a parameter of any JDBC type.
     * If the parameter does not have a user-named or REF type, the
     * typeName parameter is ignored.
     *
     * <P><B>Note:</B> When reading the value of an out parameter, you
     * must use the {@code getXXX} method whose Java type XXX corresponds to the
     * parameter's registered SQL type.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param sqlType a value from {@link java.sql.Types}
     * @param typeName the fully-qualified name of an SQL structured type
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if {@code sqlType} is
     * a {@code ARRAY}, {@code BLOB}, {@code CLOB},
     * {@code DATALINK}, {@code JAVA_OBJECT}, {@code NCHAR},
     * {@code NCLOB}, {@code NVARCHAR}, {@code LONGNVARCHAR},
     *  {@code REF}, {@code ROWID}, {@code SQLXML}
     * or  {@code STRUCT} data type and the JDBC driver does not support
     * this data type or if the JDBC driver does not support
     * this method
     * @see java.sql.Types
     * @since JDK 1.4, HSQL 1.7.0
     */
    public synchronized void registerOutParameter(
            String parameterName,
            int sqlType,
            String typeName)
            throws SQLException {
        registerOutParameter(findParameterIndex(parameterName), sqlType);
    }

    /**
     * Retrieves the value of the designated JDBC {@code DATALINK} parameter as a
     * {@code java.net.URL} object.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an {@code SQLException}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,...
     * @return a {@code java.net.URL} object that represents the
     *         JDBC {@code DATALINK} value used as the designated
     *         parameter
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs,
     * this method is called on a closed {@code CallableStatement},
     *            or if the URL being returned is
     *            not a valid URL on the Java platform
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setURL
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public java.net.URL getURL(int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);

        throw JDBCUtil.notSupported();
    }

    /**
     * Sets the designated parameter to the given {@code java.net.URL} object.
     * The driver converts this to an SQL {@code DATALINK} value when
     * it sends it to the database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an {@code SQLException}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param val the parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs;
     * this method is called on a closed {@code CallableStatement}
     *            or if a URL is malformed
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getURL
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public void setURL(
            String parameterName,
            java.net.URL val)
            throws SQLException {
        setURL(findParameterIndex(parameterName), val);
    }

    /**
     * Sets the designated parameter to SQL {@code NULL}.
     *
     * <P><B>Note:</B> You must specify the parameter's SQL type.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param sqlType the SQL type code defined in {@code java.sql.Types}
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setNull(
            String parameterName,
            int sqlType)
            throws SQLException {
        setNull(findParameterIndex(parameterName), sqlType);
    }

    /**
     * Sets the designated parameter to the given Java {@code boolean} value.
     * The driver converts this
     * to an SQL {@code BIT} or {@code BOOLEAN} value when it sends it to the database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @see #getBoolean
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setBoolean(
            String parameterName,
            boolean x)
            throws SQLException {
        setBoolean(findParameterIndex(parameterName), x);
    }

    /**
     * Sets the designated parameter to the given Java {@code byte} value.
     * The driver converts this
     * to an SQL {@code TINYINT} value when it sends it to the database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getByte
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setByte(
            String parameterName,
            byte x)
            throws SQLException {
        setByte(findParameterIndex(parameterName), x);
    }

    /**
     * Sets the designated parameter to the given Java {@code short} value.
     * The driver converts this
     * to an SQL {@code SMALLINT} value when it sends it to the database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getShort
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setShort(
            String parameterName,
            short x)
            throws SQLException {
        setShort(findParameterIndex(parameterName), x);
    }

    /**
     * Sets the designated parameter to the given Java {@code int} value.
     * The driver converts this
     * to an SQL {@code INTEGER} value when it sends it to the database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getInt
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setInt(
            String parameterName,
            int x)
            throws SQLException {
        setInt(findParameterIndex(parameterName), x);
    }

    /**
     * Sets the designated parameter to the given Java {@code long} value.
     * The driver converts this
     * to an SQL {@code BIGINT} value when it sends it to the database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getLong
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setLong(
            String parameterName,
            long x)
            throws SQLException {
        setLong(findParameterIndex(parameterName), x);
    }

    /**
     * Sets the designated parameter to the given Java {@code float} value.
     * The driver converts this
     * to an SQL {@code FLOAT} value when it sends it to the database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getFloat
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setFloat(
            String parameterName,
            float x)
            throws SQLException {
        setFloat(findParameterIndex(parameterName), x);
    }

    /**
     * Sets the designated parameter to the given Java {@code double} value.
     * The driver converts this
     * to an SQL {@code DOUBLE} value when it sends it to the database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getDouble
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setDouble(
            String parameterName,
            double x)
            throws SQLException {
        setDouble(findParameterIndex(parameterName), x);
    }

    /**
     * Sets the designated parameter to the given
     * {@code java.math.BigDecimal} value.
     * The driver converts this to an SQL {@code NUMERIC} value when
     * it sends it to the database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getBigDecimal
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setBigDecimal(
            String parameterName,
            BigDecimal x)
            throws SQLException {
        setBigDecimal(findParameterIndex(parameterName), x);
    }

    /**
     * Sets the designated parameter to the given Java {@code String} value.
     * The driver converts this
     * to an SQL {@code VARCHAR} or {@code LONGVARCHAR} value
     * (depending on the argument's
     * size relative to the driver's limits on {@code VARCHAR} values)
     * when it sends it to the database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getString
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setString(
            String parameterName,
            String x)
            throws SQLException {
        setString(findParameterIndex(parameterName), x);
    }

    /**
     * Sets the designated parameter to the given Java array of bytes.
     * The driver converts this to an SQL {@code VARBINARY} or
     * {@code LONGVARBINARY} (depending on the argument's size relative
     * to the driver's limits on {@code VARBINARY} values) when it sends
     * it to the database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getBytes
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setBytes(
            String parameterName,
            byte[] x)
            throws SQLException {
        setBytes(findParameterIndex(parameterName), x);
    }

    /**
     * Sets the designated parameter to the given {@code java.sql.Date} value
     * using the default time zone of the virtual machine that is running
     * the application.
     * The driver converts this
     * to an SQL {@code DATE} value when it sends it to the database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getDate
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setDate(
            String parameterName,
            Date x)
            throws SQLException {
        setDate(findParameterIndex(parameterName), x);
    }

    /**
     * Sets the designated parameter to the given {@code java.sql.Time} value.
     * The driver converts this
     * to an SQL {@code TIME} value when it sends it to the database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getTime
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setTime(
            String parameterName,
            Time x)
            throws SQLException {
        setTime(findParameterIndex(parameterName), x);
    }

    /**
     * Sets the designated parameter to the given {@code java.sql.Timestamp} value.
     * The driver
     * converts this to an SQL {@code TIMESTAMP} value when it sends it to the
     * database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getTimestamp
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setTimestamp(
            String parameterName,
            Timestamp x)
            throws SQLException {
        setTimestamp(findParameterIndex(parameterName), x);
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large ASCII value is input to a {@code LONGVARCHAR}
     * parameter, it may be more practical to send it via a
     * {@code java.io.InputStream}. Data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from ASCII to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the Java input stream that contains the ASCII parameter value
     * @param length the number of bytes in the stream
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setAsciiStream(
            String parameterName,
            java.io.InputStream x,
            int length)
            throws SQLException {
        setAsciiStream(findParameterIndex(parameterName), x, length);
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large binary value is input to a {@code LONGVARBINARY}
     * parameter, it may be more practical to send it via a
     * {@code java.io.InputStream} object. The data will be read from the stream
     * as needed until end-of-file is reached.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the java input stream which contains the binary parameter value
     * @param length the number of bytes in the stream
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setBinaryStream(
            String parameterName,
            java.io.InputStream x,
            int length)
            throws SQLException {
        setBinaryStream(findParameterIndex(parameterName), x, length);
    }

    /**
     * Sets the value of the designated parameter with the given object.
     *
     * <p>The given Java object will be converted to the given targetSqlType
     * before being sent to the database.
     *
     * If the object has a custom mapping (is of a class implementing the
     * interface {@code SQLData}),
     * the JDBC driver should call the method {@code SQLData.writeSQL} to write it
     * to the SQL data stream.
     * If, on the other hand, the object is of a class implementing
     * {@code Ref}, {@code Blob}, {@code Clob},  {@code NClob},
     *  {@code Struct}, {@code java.net.URL},
     * or {@code Array}, the driver should pass it to the database as a
     * value of the corresponding SQL type.
     * <P>
     * Note that this method may be used to pass database-
     * specific abstract data types.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the object containing the input parameter value
     * @param targetSqlType the SQL type (as defined in java.sql.Types) to be
     * sent to the database. The scale argument may further qualify this type.
     * @param scale for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types,
     *          this is the number of digits after the decimal point.  For all other
     *          types, this value will be ignored.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified targetSqlType
     * @see Types
     * @see #getObject
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setObject(
            String parameterName,
            Object x,
            int targetSqlType,
            int scale)
            throws SQLException {
        setObject(findParameterIndex(parameterName), x, targetSqlType, scale);
    }

    /**
     *
     * Sets the value of the designated parameter with the given object.
     *
     * This method is similar to {@link #setObject(String parameterName,
     * Object x, int targetSqlType, int scaleOrLength)},
     * except that it assumes a scale of zero.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the object containing the input parameter value
     * @param targetSqlType the SQL type (as defined in java.sql.Types) to be
     *                      sent to the database
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified targetSqlType
     * @see #getObject
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setObject(
            String parameterName,
            Object x,
            int targetSqlType)
            throws SQLException {
        setObject(findParameterIndex(parameterName), x, targetSqlType);
    }

    /**
     * Sets the value of the designated parameter with the given object.
     *
     * <p>The JDBC specification specifies a standard mapping from
     * Java {@code Object} types to SQL types.  The given argument
     * will be converted to the corresponding SQL type before being
     * sent to the database.
     * <p>Note that this method may be used to pass database-
     * specific abstract data types, by using a driver-specific Java
     * type.
     *
     * If the object is of a class implementing the interface {@code SQLData},
     * the JDBC driver should call the method {@code SQLData.writeSQL}
     * to write it to the SQL data stream.
     * If, on the other hand, the object is of a class implementing
     * {@code Ref}, {@code Blob}, {@code Clob},  {@code NClob},
     *  {@code Struct}, {@code java.net.URL},
     * or {@code Array}, the driver should pass it to the database as a
     * value of the corresponding SQL type.
     * <P>
     * This method throws an exception if there is an ambiguity, for example, if the
     * object is of a class implementing more than one of the interfaces named above.
     * <p>
     *<b>Note:</b> Not all databases allow for a non-typed Null to be sent to
     * the backend. For maximum portability, the {@code setNull} or the
     * {@code setObject(String parameterName, Object x, int sqlType)}
     * method should be used
     * instead of {@code setObject(String parameterName, Object x)}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the object containing the input parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs,
     * this method is called on a closed {@code CallableStatement} or if the given
     *            {@code Object} parameter is ambiguous
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getObject
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setObject(
            String parameterName,
            Object x)
            throws SQLException {
        setObject(findParameterIndex(parameterName), x);
    }

    /**
     *
     * Sets the designated parameter to the given {@code Reader}
     * object, which is the given number of characters long.
     * When a very large UNICODE value is input to a {@code LONGVARCHAR}
     * parameter, it may be more practical to send it via a
     * {@code java.io.Reader} object. The data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param reader the {@code java.io.Reader} object that
     *        contains the UNICODE data used as the designated parameter
     * @param length the number of characters in the stream
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setCharacterStream(
            String parameterName,
            java.io.Reader reader,
            int length)
            throws SQLException {
        setCharacterStream(findParameterIndex(parameterName), reader, length);
    }

    /**
     *
     * Sets the designated parameter to the given {@code java.sql.Date} value,
     * using the given {@code Calendar} object.  The driver uses
     * the {@code Calendar} object to construct an SQL {@code DATE} value,
     * which the driver then sends to the database.  With
     * a {@code Calendar} object, the driver can calculate the date
     * taking into account a custom timezone.  If no
     * {@code Calendar} object is specified, the driver uses the default
     * timezone, which is that of the virtual machine running the application.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @param cal the {@code Calendar} object the driver will use
     *            to construct the date
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getDate
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setDate(
            String parameterName,
            Date x,
            Calendar cal)
            throws SQLException {
        setDate(findParameterIndex(parameterName), x, cal);
    }

    /**
     * Sets the designated parameter to the given {@code java.sql.Time} value,
     * using the given {@code Calendar} object.  The driver uses
     * the {@code Calendar} object to construct an SQL {@code TIME} value,
     * which the driver then sends to the database.  With
     * a {@code Calendar} object, the driver can calculate the time
     * taking into account a custom timezone.  If no
     * {@code Calendar} object is specified, the driver uses the default
     * timezone, which is that of the virtual machine running the application.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @param cal the {@code Calendar} object the driver will use
     *            to construct the time
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getTime
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setTime(
            String parameterName,
            Time x,
            Calendar cal)
            throws SQLException {
        setTime(findParameterIndex(parameterName), x, cal);
    }

    /**
     * Sets the designated parameter to the given {@code java.sql.Timestamp} value,
     * using the given {@code Calendar} object.  The driver uses
     * the {@code Calendar} object to construct an SQL {@code TIMESTAMP} value,
     * which the driver then sends to the database.  With
     * a {@code Calendar} object, the driver can calculate the timestamp
     * taking into account a custom timezone.  If no
     * {@code Calendar} object is specified, the driver uses the default
     * timezone, which is that of the virtual machine running the application.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @param cal the {@code Calendar} object the driver will use
     *            to construct the timestamp
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #getTimestamp
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setTimestamp(
            String parameterName,
            Timestamp x,
            Calendar cal)
            throws SQLException {
        setTimestamp(findParameterIndex(parameterName), x, cal);
    }

    /**
     * Sets the designated parameter to SQL {@code NULL}.
     * This version of the method {@code setNull} should
     * be used for user-defined types and REF type parameters.  Examples
     * of user-defined types include: STRUCT, DISTINCT, JAVA_OBJECT, and
     * named array types.
     *
     * <P><B>Note:</B> To be portable, applications must give the
     * SQL type code and the fully-qualified SQL type name when specifying
     * a NULL user-defined or REF parameter.  In the case of a user-defined type
     * the name is the type name of the parameter itself.  For a REF
     * parameter, the name is the type name of the referenced type.
     * <p>
     * Although it is intended for user-defined and Ref parameters,
     * this method may be used to set a null parameter of any JDBC type.
     * If the parameter does not have a user-defined or REF type, the given
     * typeName is ignored.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param sqlType a value from {@code java.sql.Types}
     * @param typeName the fully-qualified name of an SQL user-defined type;
     *        ignored if the parameter is not a user-defined type or
     *        SQL {@code REF} value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized void setNull(
            String parameterName,
            int sqlType,
            String typeName)
            throws SQLException {
        setNull(findParameterIndex(parameterName), sqlType, typeName);
    }

    /**
     * Retrieves the value of a JDBC {@code CHAR}, {@code VARCHAR},
     * or {@code LONGVARCHAR} parameter as a {@code String} in
     * the Java programming language.
     * <p>
     * For the fixed-length type JDBC {@code CHAR},
     * the {@code String} object
     * returned has exactly the same value the SQL
     * {@code CHAR} value had in the
     * database, including any padding added by the database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.<p>
     *
     * Calling this method always throws an {@code SQLException}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value. If the value is SQL {@code NULL}, the result
     * is {@code null}.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setString
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized String getString(
            String parameterName)
            throws SQLException {
        return getString(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of a JDBC {@code BIT} or {@code BOOLEAN}
     * parameter as a
     * {@code boolean} in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     * is {@code false}.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setBoolean
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized boolean getBoolean(
            String parameterName)
            throws SQLException {
        return getBoolean(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of a JDBC {@code TINYINT} parameter as a {@code byte}
     * in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     * is {@code 0}.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setByte
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized byte getByte(String parameterName) throws SQLException {
        return getByte(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of a JDBC {@code SMALLINT} parameter as a {@code short}
     * in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     * is {@code 0}.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setShort
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized short getShort(
            String parameterName)
            throws SQLException {
        return getShort(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of a JDBC {@code INTEGER} parameter as an {@code int}
     * in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature. <p>
     *
     * Calling this method always throws an {@code SQLException}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL {@code NULL},
     *         the result is {@code 0}.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setInt
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized int getInt(String parameterName) throws SQLException {
        return getInt(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of a JDBC {@code BIGINT} parameter as a {@code long}
     * in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL {@code NULL},
     *         the result is {@code 0}.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setLong
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized long getLong(String parameterName) throws SQLException {
        return getLong(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of a JDBC {@code FLOAT} parameter as a {@code float}
     * in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL {@code NULL},
     *         the result is {@code 0}.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setFloat
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized float getFloat(
            String parameterName)
            throws SQLException {
        return getFloat(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of a JDBC {@code DOUBLE} parameter as a {@code double}
     * in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL {@code NULL},
     *         the result is {@code 0}.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setDouble
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized double getDouble(
            String parameterName)
            throws SQLException {
        return getDouble(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of a JDBC {@code BINARY} or {@code VARBINARY}
     * parameter as an array of {@code byte} values in the Java
     * programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL {@code NULL}, the result is
     *  {@code null}.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setBytes
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized byte[] getBytes(
            String parameterName)
            throws SQLException {
        return getBytes(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of a JDBC {@code DATE} parameter as a
     * {@code java.sql.Date} object.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     * is {@code null}.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setDate
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized Date getDate(String parameterName) throws SQLException {
        return getDate(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of a JDBC {@code TIME} parameter as a
     * {@code java.sql.Time} object.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     * is {@code null}.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setTime
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized Time getTime(String parameterName) throws SQLException {
        return getTime(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of a JDBC {@code TIMESTAMP} parameter as a
     * {@code java.sql.Timestamp} object.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value.  If the value is SQL {@code NULL}, the result
     * is {@code null}.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setTimestamp
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized Timestamp getTimestamp(
            String parameterName)
            throws SQLException {
        return getTimestamp(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of a parameter as an {@code Object} in the Java
     * programming language. If the value is an SQL {@code NULL}, the
     * driver returns a Java {@code null}.
     * <p>
     * This method returns a Java object whose type corresponds to the JDBC
     * type that was registered for this parameter using the method
     * {@code registerOutParameter}.  By registering the target JDBC
     * type as {@code java.sql.Types.OTHER}, this method can be used
     * to read database-specific abstract data types.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return A {@code java.lang.Object} holding the OUT parameter value.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see java.sql.Types
     * @see #setObject
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized Object getObject(
            String parameterName)
            throws SQLException {
        return getObject(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of a JDBC {@code NUMERIC} parameter as a
     * {@code java.math.BigDecimal} object with as many digits to the
     * right of the decimal point as the value contains.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value in full precision.  If the value is
     * SQL {@code NULL}, the result is {@code null}.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setBigDecimal
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized BigDecimal getBigDecimal(
            String parameterName)
            throws SQLException {
        return getBigDecimal(findParameterIndex(parameterName));
    }

    /**
     * Returns an object representing the value of OUT parameter
     * {@code parameterName} and uses {@code map} for the custom
     * mapping of the parameter value.
     * <p>
     * This method returns a Java object whose type corresponds to the
     * JDBC type that was registered for this parameter using the method
     * {@code registerOutParameter}.  By registering the target
     * JDBC type as {@code java.sql.Types.OTHER}, this method can
     * be used to read database-specific abstract data types.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param map the mapping from SQL type names to Java classes
     * @return a {@code java.lang.Object} holding the OUT parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setObject
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized Object getObject(
            String parameterName,
            Map<String, Class<?>> map)
            throws SQLException {
        return getObject(findParameterIndex(parameterName), map);
    }

    /**
     * Retrieves the value of a JDBC {@code REF(&lt;structured-type&gt;)}
     * parameter as a {@link java.sql.Ref} object in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as a {@code Ref} object in the
     *         Java programming language.  If the value was SQL {@code NULL},
     *         the value {@code null} is returned.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized Ref getRef(String parameterName) throws SQLException {
        return getRef(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of a JDBC {@code BLOB} parameter as a
     * {@link java.sql.Blob} object in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as a {@code Blob} object in the
     *         Java programming language.  If the value was SQL {@code NULL},
     *         the value {@code null} is returned.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized Blob getBlob(String parameterName) throws SQLException {
        return getBlob(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of a JDBC {@code CLOB} parameter as a
     * {@link java.sql.Clob} object in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as a {@code Clob} object in the
     *         Java programming language.  If the value was SQL {@code NULL},
     *         the value {@code null} is returned.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized Clob getClob(String parameterName) throws SQLException {
        return getClob(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of a JDBC {@code ARRAY} parameter as an
     * {@link java.sql.Array} object in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as an {@code Array} object in
     *         Java programming language.  If the value was SQL {@code NULL},
     *         the value {@code null} is returned.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized Array getArray(
            String parameterName)
            throws SQLException {
        return getArray(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of a JDBC {@code DATE} parameter as a
     * {@code java.sql.Date} object, using
     * the given {@code Calendar} object
     * to construct the date.
     * With a {@code Calendar} object, the driver
     * can calculate the date taking into account a custom timezone and locale.
     * If no {@code Calendar} object is specified, the driver uses the
     * default timezone and locale.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param cal the {@code Calendar} object the driver will use
     *            to construct the date
     * @return the parameter value.  If the value is SQL {@code NULL},
     * the result is {@code null}.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setDate
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized Date getDate(
            String parameterName,
            Calendar cal)
            throws SQLException {
        return getDate(findParameterIndex(parameterName), cal);
    }

    /**
     * Retrieves the value of a JDBC {@code TIME} parameter as a
     * {@code java.sql.Time} object, using
     * the given {@code Calendar} object
     * to construct the time.
     * With a {@code Calendar} object, the driver
     * can calculate the time taking into account a custom timezone and locale.
     * If no {@code Calendar} object is specified, the driver uses the
     * default timezone and locale.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param cal the {@code Calendar} object the driver will use
     *            to construct the time
     * @return the parameter value; if the value is SQL {@code NULL}, the result is
     * {@code null}.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setTime
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized Time getTime(
            String parameterName,
            Calendar cal)
            throws SQLException {
        return getTime(findParameterIndex(parameterName), cal);
    }

    /**
     * Retrieves the value of a JDBC {@code TIMESTAMP} parameter as a
     * {@code java.sql.Timestamp} object, using
     * the given {@code Calendar} object to construct
     * the {@code Timestamp} object.
     * With a {@code Calendar} object, the driver
     * can calculate the timestamp taking into account a custom timezone and locale.
     * If no {@code Calendar} object is specified, the driver uses the
     * default timezone and locale.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param cal the {@code Calendar} object the driver will use
     *            to construct the timestamp
     * @return the parameter value.  If the value is SQL {@code NULL}, the result is
     * {@code null}.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setTimestamp
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public synchronized Timestamp getTimestamp(
            String parameterName,
            Calendar cal)
            throws SQLException {
        return getTimestamp(findParameterIndex(parameterName), cal);
    }

    /**
     * Retrieves the value of a JDBC {@code DATALINK} parameter as a
     * {@code java.net.URL} object.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as a {@code java.net.URL} object in the
     * Java programming language.  If the value was SQL {@code NULL}, the
     * value {@code null} is returned.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs,
     * this method is called on a closed {@code CallableStatement},
     *            or if there is a problem with the URL
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setURL
     * @since JDK 1.4, HSQLDB 1.7.0
     */
    public java.net.URL getURL(String parameterName) throws SQLException {
        return getURL(findParameterIndex(parameterName));
    }

    //------------------------- JDBC 4.0 -----------------------------------

    /**
     * Retrieves the value of the designated JDBC {@code ROWID} parameter as a
     * {@code java.sql.RowId} object.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an {@code SQLException}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2,...
     * @return a {@code RowId} object that represents the JDBC {@code ROWID}
     *     value is used as the designated parameter. If the parameter contains
     * a SQL {@code NULL}, then a {@code null} value is returned.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public RowId getRowId(int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);

        throw JDBCUtil.notSupported();
    }

    /**
     * Retrieves the value of the designated JDBC {@code ROWID} parameter as a
     * {@code java.sql.RowId} object.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an {@code SQLException}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return a {@code RowId} object that represents the JDBC {@code ROWID}
     *     value is used as the designated parameter. If the parameter contains
     * a SQL {@code NULL}, then a {@code null} value is returned.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized RowId getRowId(
            String parameterName)
            throws SQLException {
        return getRowId(findParameterIndex(parameterName));
    }

    /**
     * Sets the designated parameter to the given {@code java.sql.RowId} object. The
     * driver converts this to a SQL {@code ROWID} when it sends it to the
     * database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @param x the parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setRowId(
            String parameterName,
            RowId x)
            throws SQLException {
        super.setRowId(findParameterIndex(parameterName), x);
    }

    /**
     * Sets the designated parameter to the given {@code String} object.
     * The driver converts this to a SQL {@code NCHAR} or
     * {@code NVARCHAR} or {@code LONGNVARCHAR}
     * @param parameterName the name of the parameter to be set
     * @param value the parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setNString(
            String parameterName,
            String value)
            throws SQLException {
        super.setNString(findParameterIndex(parameterName), value);
    }

    /**
     * Sets the designated parameter to a {@code Reader} object. The
     * {@code Reader} reads the data till end-of-file is reached. The
     * driver does the necessary conversion from Java character format to
     * the national character set in the database.
     * @param parameterName the name of the parameter to be set
     * @param value the parameter value
     * @param length the number of characters in the parameter data.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setNCharacterStream(
            String parameterName,
            Reader value,
            long length)
            throws SQLException {

        super.setNCharacterStream(
            findParameterIndex(parameterName),
            value,
            length);
    }

    /**
     * Sets the designated parameter to a {@code java.sql.NClob} object. The object
     * implements the {@code java.sql.NClob} interface. This {@code NClob}
     * object maps to a SQL {@code NCLOB}.
     * @param parameterName the name of the parameter to be set
     * @param value the parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setNClob(
            String parameterName,
            NClob value)
            throws SQLException {
        super.setNClob(findParameterIndex(parameterName), value);
    }

    /**
     * Sets the designated parameter to a {@code Reader} object.  The {@code reader} must contain  the number
     * of characters specified by length otherwise a {@code SQLException} will be
     * generated when the {@code CallableStatement} is executed.
     * This method differs from the {@code setCharacterStream (int, Reader, int)} method
     * because it informs the driver that the parameter value should be sent to
     * the server as a {@code CLOB}.  When the {@code setCharacterStream} method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a {@code LONGVARCHAR} or a {@code CLOB}
     * @param parameterName the name of the parameter to be set
     * @param reader An object that contains the data to set the parameter value to.
     * @param length the number of characters in the parameter data.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if the length specified is less than zero;
     * a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     *
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setClob(
            String parameterName,
            Reader reader,
            long length)
            throws SQLException {
        super.setClob(findParameterIndex(parameterName), reader, length);
    }

    /**
     * Sets the designated parameter to a {@code InputStream} object.  The {@code inputstream} must contain  the number
     * of characters specified by length, otherwise a {@code SQLException} will be
     * generated when the {@code CallableStatement} is executed.
     * This method differs from the {@code setBinaryStream (int, InputStream, int)}
     * method because it informs the driver that the parameter value should be
     * sent to the server as a {@code BLOB}.  When the {@code setBinaryStream} method is used,
     * the driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a {@code LONGVARBINARY} or a {@code BLOB}
     *
     * @param parameterName the name of the parameter to be set
     * the second is 2, ...
     *
     * @param inputStream An object that contains the data to set the parameter
     * value to.
     * @param length the number of bytes in the parameter data.
     * @throws SQLException  if parameterName does not correspond to a named
     * parameter; if the length specified
     * is less than zero; if the number of bytes in the {@code InputStream}
     * does not match the specified length; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     *
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setBlob(
            String parameterName,
            InputStream inputStream,
            long length)
            throws SQLException {
        super.setBlob(findParameterIndex(parameterName), inputStream, length);
    }

    /**
     * Sets the designated parameter to a {@code Reader} object.  The {@code reader} must contain  the number
     * of characters specified by length otherwise a {@code SQLException} will be
     * generated when the {@code CallableStatement} is executed.
     * This method differs from the {@code setCharacterStream (int, Reader, int)} method
     * because it informs the driver that the parameter value should be sent to
     * the server as a {@code NCLOB}.  When the {@code setCharacterStream} method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a {@code LONGNVARCHAR} or a {@code NCLOB}
     *
     * @param parameterName the name of the parameter to be set
     * @param reader An object that contains the data to set the parameter value to.
     * @param length the number of characters in the parameter data.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if the length specified is less than zero;
     * if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setNClob(
            String parameterName,
            Reader reader,
            long length)
            throws SQLException {
        super.setNClob(findParameterIndex(parameterName), reader, length);
    }

    /**
     * Retrieves the value of the designated JDBC {@code NCLOB} parameter as a
     * {@code java.sql.NClob} object in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an {@code SQLException}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex the first parameter is 1, the second is 2, and
     * so on
     * @return the parameter value as a {@code NClob} object in the
     * Java programming language.  If the value was SQL {@code NULL}, the
     * value {@code null} is returned.
     * @throws SQLException if the parameterIndex is not valid;
     * if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public NClob getNClob(int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);

        throw JDBCUtil.notSupported();
    }

    /**
     * Retrieves the value of a JDBC {@code NCLOB} parameter as a
     * {@code java.sql.NClob} object in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an {@code SQLException}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return the parameter value as a {@code NClob} object in the
     *         Java programming language.  If the value was SQL {@code NULL},
     *         the value {@code null} is returned.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized NClob getNClob(
            String parameterName)
            throws SQLException {
        return getNClob(findParameterIndex(parameterName));
    }

    /**
     * Sets the designated parameter to the given {@code java.sql.SQLXML} object. The driver converts this to an
     * {@code SQL XML} value when it sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param xmlObject a {@code SQLXML} object that maps an {@code SQL XML} value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs;
     * this method is called on a closed {@code CallableStatement} or
     * the {@code java.xml.transform.Result},
     *  {@code Writer} or {@code OutputStream} has not been closed for the {@code SQLXML} object
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     *
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setSQLXML(
            String parameterName,
            SQLXML xmlObject)
            throws SQLException {
        super.setSQLXML(findParameterIndex(parameterName), xmlObject);
    }

    /**
     * Retrieves the value of the designated {@code SQL XML} parameter as a
     * {@code java.sql.SQLXML} object in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an {@code SQLException}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @return a {@code SQLXML} object that maps an {@code SQL XML} value
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);

        throw JDBCUtil.notSupported();
    }

    /**
     * Retrieves the value of the designated {@code SQL XML} parameter as a
     * {@code java.sql.SQLXML} object in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an {@code SQLException}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return a {@code SQLXML} object that maps an {@code SQL XML} value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized SQLXML getSQLXML(
            String parameterName)
            throws SQLException {
        return getSQLXML(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of the designated {@code NCHAR},
     * {@code NVARCHAR}
     * or {@code LONGNVARCHAR} parameter as
     * a {@code String} in the Java programming language.
     * <p>
     * For the fixed-length type JDBC {@code NCHAR},
     * the {@code String} object
     * returned has exactly the same value the SQL
     * {@code NCHAR} value had in the
     * database, including any padding added by the database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an {@code SQLException}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterIndex index of the first parameter is 1, the second is 2, ...
     * @return a {@code String} object that maps an
     * {@code NCHAR}, {@code NVARCHAR} or {@code LONGNVARCHAR} value
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     * @see #setNString
     */
    public String getNString(int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);

        throw JDBCUtil.notSupported();
    }

    /**
     *  Retrieves the value of the designated {@code NCHAR},
     * {@code NVARCHAR}
     * or {@code LONGNVARCHAR} parameter as
     * a {@code String} in the Java programming language.
     * <p>
     * For the fixed-length type JDBC {@code NCHAR},
     * the {@code String} object
     * returned has exactly the same value the SQL
     * {@code NCHAR} value had in the
     * database, including any padding added by the database.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an {@code SQLException}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return a {@code String} object that maps an
     * {@code NCHAR}, {@code NVARCHAR} or {@code LONGNVARCHAR} value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     * @see #setNString
     */
    public synchronized String getNString(
            String parameterName)
            throws SQLException {
        return getNString(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of the designated parameter as a
     * {@code java.io.Reader} object in the Java programming language.
     * It is intended for use when
     * accessing  {@code NCHAR},{@code NVARCHAR}
     * and {@code LONGNVARCHAR} parameters.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an {@code SQLException}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return a {@code java.io.Reader} object that contains the parameter
     * value; if the value is SQL {@code NULL}, the value returned is
     * {@code null} in the Java programming language.
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);

        throw JDBCUtil.notSupported();
    }

    /**
     * Retrieves the value of the designated parameter as a
     * {@code java.io.Reader} object in the Java programming language.
     * It is intended for use when
     * accessing  {@code NCHAR},{@code NVARCHAR}
     * and {@code LONGNVARCHAR} parameters.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not yet support this feature. <p>
     *
     * Calling this method always throws an {@code SQLException}.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return a {@code java.io.Reader} object that contains the parameter
     * value; if the value is SQL {@code NULL}, the value returned is
     * {@code null} in the Java programming language
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized Reader getNCharacterStream(
            String parameterName)
            throws SQLException {
        return getNCharacterStream(findParameterIndex(parameterName));
    }

    /**
     * Retrieves the value of the designated parameter as a
     * {@code java.io.Reader} object in the Java programming language.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return a {@code java.io.Reader} object that contains the parameter
     * value; if the value is SQL {@code NULL}, the value returned is
     * {@code null} in the Java programming language.
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @throws SQLException if the parameterIndex is not valid; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @since JDK 1.6, HSQLDB 2.0
     */
    public Reader getCharacterStream(int parameterIndex) throws SQLException {

        checkGetParameterIndex(parameterIndex);

        Type   sourceType = parameterMetaData.columnTypes[parameterIndex - 1];
        Object o          = getColumnInType(parameterIndex, sourceType);

        if (o == null) {
            return null;
        }

        if (o instanceof ClobDataID) {
            return ((ClobDataID) o).getCharacterStream(session);
        } else if (o instanceof Clob) {
            return ((Clob) o).getCharacterStream();
        } else if (o instanceof String) {
            return new StringReader((String) o);
        }

        throw JDBCUtil.sqlException(ErrorCode.X_42561);
    }

    /**
     * Retrieves the value of the designated parameter as a
     * {@code java.io.Reader} object in the Java programming language.
     *
     * <!-- end generic documentstion -->
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param parameterName the name of the parameter
     * @return a {@code java.io.Reader} object that contains the parameter
     * value; if the value is SQL {@code NULL}, the value returned is
     * {@code null} in the Java programming language
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized Reader getCharacterStream(
            String parameterName)
            throws SQLException {
        return getCharacterStream(findParameterIndex(parameterName));
    }

    /**
     * Sets the designated parameter to the given {@code java.sql.Blob} object.
     * The driver converts this to an SQL {@code BLOB} value when it
     * sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x a {@code Blob} object that maps an SQL {@code BLOB} value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     *  @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setBlob(
            String parameterName,
            Blob x)
            throws SQLException {
        super.setBlob(findParameterIndex(parameterName), x);
    }

    /**
     * Sets the designated parameter to the given {@code java.sql.Clob} object.
     * The driver converts this to an SQL {@code CLOB} value when it
     * sends it to the database.
     *
     * @param parameterName the name of the parameter
     * @param x a {@code Clob} object that maps an SQL {@code CLOB} value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     *  @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setClob(
            String parameterName,
            Clob x)
            throws SQLException {
        super.setClob(findParameterIndex(parameterName), x);
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large ASCII value is input to a {@code LONGVARCHAR}
     * parameter, it may be more practical to send it via a
     * {@code java.io.InputStream}. Data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from ASCII to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterName the name of the parameter
     * @param x the Java input stream that contains the ASCII parameter value
     * @param length the number of bytes in the stream
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setAsciiStream(
            String parameterName,
            java.io.InputStream x,
            long length)
            throws SQLException {

        if (length > Integer.MAX_VALUE) {
            String msg = "Maximum ASCII input octet length exceeded: " + length;    // NOI18N

            throw JDBCUtil.sqlException(ErrorCode.JDBC_INPUTSTREAM_ERROR, msg);
        }

        this.setAsciiStream(parameterName, x, (int) length);
    }

    /**
     * Sets the designated parameter to the given input stream, which will have
     * the specified number of bytes.
     * When a very large binary value is input to a {@code LONGVARBINARY}
     * parameter, it may be more practical to send it via a
     * {@code java.io.InputStream} object. The data will be read from the stream
     * as needed until end-of-file is reached.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterName the name of the parameter
     * @param x the java input stream which contains the binary parameter value
     * @param length the number of bytes in the stream
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setBinaryStream(
            String parameterName,
            java.io.InputStream x,
            long length)
            throws SQLException {

        if (length > Integer.MAX_VALUE) {
            String msg = "Maximum Binary input octet length exceeded: "
                         + length;    // NOI18N

            throw JDBCUtil.sqlException(ErrorCode.JDBC_INPUTSTREAM_ERROR, msg);
        }

        setBinaryStream(parameterName, x, (int) length);
    }

    /**
     * Sets the designated parameter to the given {@code Reader}
     * object, which is the given number of characters long.
     * When a very large UNICODE value is input to a {@code LONGVARCHAR}
     * parameter, it may be more practical to send it via a
     * {@code java.io.Reader} object. The data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     *
     * @param parameterName the name of the parameter
     * @param reader the {@code java.io.Reader} object that
     *        contains the UNICODE data used as the designated parameter
     * @param length the number of characters in the stream
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setCharacterStream(
            String parameterName,
            java.io.Reader reader,
            long length)
            throws SQLException {

        if (length > Integer.MAX_VALUE) {
            String msg = "Maximum character input length exceeded: " + length;    // NOI18N

            throw JDBCUtil.sqlException(ErrorCode.JDBC_INPUTSTREAM_ERROR, msg);
        }

        setCharacterStream(parameterName, reader, (int) length);
    }

    /**
     * Sets the designated parameter to the given input stream.
     * When a very large ASCII value is input to a {@code LONGVARCHAR}
     * parameter, it may be more practical to send it via a
     * {@code java.io.InputStream}. Data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from ASCII to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * {@code setAsciiStream} which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param x the Java input stream that contains the ASCII parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setAsciiStream(
            String parameterName,
            java.io.InputStream x)
            throws SQLException {
        super.setAsciiStream(findParameterIndex(parameterName), x);
    }

    /**
     * Sets the designated parameter to the given input stream.
     * When a very large binary value is input to a {@code LONGVARBINARY}
     * parameter, it may be more practical to send it via a
     * {@code java.io.InputStream} object. The data will be read from the
     * stream as needed until end-of-file is reached.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * {@code setBinaryStream} which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param x the java input stream which contains the binary parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     * @since JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setBinaryStream(
            String parameterName,
            java.io.InputStream x)
            throws SQLException {
        super.setBinaryStream(findParameterIndex(parameterName), x);
    }

    /**
     * Sets the designated parameter to the given {@code Reader}
     * object.
     * When a very large UNICODE value is input to a {@code LONGVARCHAR}
     * parameter, it may be more practical to send it via a
     * {@code java.io.Reader} object. The data will be read from the stream
     * as needed until end-of-file is reached.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * {@code setCharacterStream} which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param reader the {@code java.io.Reader} object that contains the
     *        Unicode data
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     * JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setCharacterStream(
            String parameterName,
            java.io.Reader reader)
            throws SQLException {
        super.setCharacterStream(findParameterIndex(parameterName), reader);
    }

    /**
     * Sets the designated parameter to a {@code Reader} object. The
     * {@code Reader} reads the data till end-of-file is reached. The
     * driver does the necessary conversion from Java character format to
     * the national character set in the database.
     * <P><B>Note:</B> This stream object can either be a standard
     * Java stream object or your own subclass that implements the
     * standard interface.
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * {@code setNCharacterStream} which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param value the parameter value
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if the driver does not support national
     *         character sets;  if the driver can detect that a data conversion
     *  error could occur; if a database access error occurs; or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     * JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setNCharacterStream(
            String parameterName,
            Reader value)
            throws SQLException {
        super.setNCharacterStream(findParameterIndex(parameterName), value);
    }

    /**
     * Sets the designated parameter to a {@code Reader} object.
     * This method differs from the {@code setCharacterStream (int, Reader)} method
     * because it informs the driver that the parameter value should be sent to
     * the server as a {@code CLOB}.  When the {@code setCharacterStream} method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a {@code LONGVARCHAR} or a {@code CLOB}
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * {@code setClob} which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param reader An object that contains the data to set the parameter value to.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or this method is called on
     * a closed {@code CallableStatement}
     *
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     * JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setClob(
            String parameterName,
            Reader reader)
            throws SQLException {
        super.setClob(findParameterIndex(parameterName), reader);
    }

    /**
     * Sets the designated parameter to an {@code InputStream} object.
     * This method differs from the {@code setBinaryStream (int, InputStream)}
     * method because it informs the driver that the parameter value should be
     * sent to the server as a {@code BLOB}.  When the {@code setBinaryStream} method is used,
     * the driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a {@code LONGVARBINARY} or a {@code BLOB}
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * {@code setBlob} which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param inputStream An object that contains the data to set the parameter
     * value to.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     *
     * JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setBlob(
            String parameterName,
            InputStream inputStream)
            throws SQLException {
        super.setBlob(findParameterIndex(parameterName), inputStream);
    }

    /**
     * Sets the designated parameter to a {@code Reader} object.
     * This method differs from the {@code setCharacterStream (int, Reader)} method
     * because it informs the driver that the parameter value should be sent to
     * the server as a {@code NCLOB}.  When the {@code setCharacterStream} method is used, the
     * driver may have to do extra work to determine whether the parameter
     * data should be sent to the server as a {@code LONGNVARCHAR} or a {@code NCLOB}
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * {@code setNClob} which takes a length parameter.
     *
     * @param parameterName the name of the parameter
     * @param reader An object that contains the data to set the parameter value to.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if the driver does not support national character sets;
     * if the driver can detect that a data conversion
     *  error could occur;  if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
     *
     * JDK 1.6, HSQLDB 2.0
     */
    public synchronized void setNClob(
            String parameterName,
            Reader reader)
            throws SQLException {
        super.setNClob(findParameterIndex(parameterName), reader);
    }

    //------------------------- JDBC 4.1 -----------------------------------

    /**
     * Returns an object representing the value of OUT parameter
     * {@code parameterIndex} and will convert from the
     * SQL type of the parameter to the requested Java data type, if the
     * conversion is supported. If the conversion is not
     * supported or null is specified for the type, a
     * {@code SQLException} is thrown.
     * <p>
     * At a minimum, an implementation must support the conversions defined in
     * Appendix B, Table B-3 and conversion of appropriate user defined SQL
     * types to a Java type which implements {@code SQLData}, or {@code Struct}.
     * Additional conversions may be supported and are vendor defined.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, and so on
     * @param type Class representing the Java data type to convert the
     * designated parameter to.
     * @param <T> the type of the class modeled by this Class object
     * @return an instance of {@code type} holding the OUT parameter value
     * @throws SQLException if conversion is not supported, type is null or
     *         another error occurs. The getCause() method of the
     * exception may provide a more detailed exception, for example, if
     * a conversion error occurs
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.7, HSQLDB 2.0.1
     */
    public <T> T getObject(
            int parameterIndex,
            Class<T> type)
            throws SQLException {

        if (type == null) {
            throw JDBCUtil.nullArgument();
        }

        final Object source = getColumnValue(parameterIndex);

        if (wasNullValue) {
            return null;
        }

        Object o = null;

        switch (type.getName()) {

            case "int" :
            case "java.lang.Integer" :
                o = getInt(parameterIndex);
                break;

            case "double" :
            case "java.lang.Double" :
                o = getDouble(parameterIndex);
                break;

            case "boolean" :
            case "java.lang.Boolean" :
                o = getBoolean(parameterIndex);
                break;

            case "byte" :
            case "java.lang.Byte" :
                o = getByte(parameterIndex);
                break;

            case "short" :
            case "java.lang.Short" :
                o = getShort(parameterIndex);
                break;

            case "long" :
            case "java.lang.Long" :
                o = getLong(parameterIndex);
                break;

            case "[B" :
                o = getBytes(parameterIndex);
                break;

            case "java.lang.Object" :
                o = getObject(parameterIndex);
                break;

            case "java.math.BigDecimal" :
                o = getBigDecimal(parameterIndex);
                break;

            case "java.sql.Blob" :
                o = getBlob(parameterIndex);
                break;

            case "java.sql.Clob" :
                o = getClob(parameterIndex);
                break;

            case "java.lang.String" :
            case "java.lang.CharSequence" :
                o = getString(parameterIndex);
                break;

            case "java.sql.Date" : {
                o = getDate(parameterIndex);
                break;
            }

            case "java.sql.Time" : {
                o = getTime(parameterIndex);
                break;
            }

            case "java.sql.Timestamp" : {
                o = getTimestamp(parameterIndex);
                break;
            }

            case "java.util.UUID" : {
                Type columnType = parameterTypes[parameterIndex - 1];

                if (columnType.isUUIDType()) {
                    o = Type.SQL_GUID.convertSQLToJava(session, source);
                } else {
                    Object value = Type.SQL_GUID.convertToTypeJDBC(
                        session,
                        source,
                        columnType);

                    o = Type.SQL_GUID.convertSQLToJava(session, value);
                }

                break;
            }

            case "java.time.Instant" : {
                Type columnType = parameterTypes[parameterIndex - 1];

                if (columnType.isDateOrTimestampType()) {
                    TimestampData v = (TimestampData) source;

                    o = ((DateTimeType) columnType).toInstant(session, v);
                }

                break;
            }

            case "java.time.LocalDate" : {
                Type columnType = parameterTypes[parameterIndex - 1];

                if (columnType.isDateOrTimestampType()) {
                    TimestampData v = (TimestampData) source;

                    o = ((DateTimeType) columnType).toLocalDate(session, v);
                }

                break;
            }

            case "java.time.LocalTime" : {
                Type columnType = parameterTypes[parameterIndex - 1];

                if (columnType.isTimeType()) {
                    TimeData v = (TimeData) source;

                    o = ((DateTimeType) columnType).toLocalTime(session, v);
                } else if (columnType.isTimestampType()) {
                    TimestampData v = (TimestampData) source;

                    o = ((DateTimeType) columnType).toLocalTime(session, v);
                }

                break;
            }

            case "java.time.LocalDateTime" : {
                Type columnType = parameterTypes[parameterIndex - 1];

                if (columnType.isDateOrTimestampType()) {
                    TimestampData v = (TimestampData) source;

                    o = ((DateTimeType) columnType).toLocalDateTime(session, v);
                }

                break;
            }

            case "java.time.OffsetTime" : {
                Type columnType = parameterTypes[parameterIndex - 1];

                if (columnType.isTimeType()) {
                    TimeData v = (TimeData) source;

                    o = ((DateTimeType) columnType).toOffsetTime(session, v);
                } else if (columnType.isTimestampType()) {
                    TimestampData v = (TimestampData) source;

                    o = ((DateTimeType) columnType).toOffsetTime(session, v);
                }

                break;
            }

            case "java.time.OffsetDateTime" : {
                Type columnType = parameterTypes[parameterIndex - 1];

                if (columnType.isDateOrTimestampType()) {
                    TimestampData v = (TimestampData) source;

                    o = ((DateTimeType) columnType).toOffsetDateTime(
                        session,
                        v);
                }

                break;
            }

            case "java.time.Duration" : {
                Type sourceType =
                    parameterMetaData.columnTypes[parameterIndex - 1];

                if (!sourceType.isIntervalDaySecondType()) {
                    break;
                }

                IntervalSecondData v = (IntervalSecondData) source;

                o = Duration.ofSeconds(v.getSeconds(), v.getNanos());
                break;
            }

            case "java.time.Period" : {
                Type sourceType =
                    parameterMetaData.columnTypes[parameterIndex - 1];

                if (!sourceType.isIntervalYearMonthType()) {
                    break;
                }

                IntervalMonthData v      = (IntervalMonthData) source;
                int               months = v.getMonths();

                if (sourceType.typeCode == Types.SQL_INTERVAL_MONTH) {
                    o = Period.ofMonths(months);
                } else {
                    o = Period.of(months / 12, months % 12, 0);
                }

                break;
            }
        }

        if (o == null) {
            throw JDBCUtil.sqlException(ErrorCode.X_42561);
        }

        return (T) o;
    }

    /**
     * Returns an object representing the value of OUT parameter
     * {@code parameterName} and will convert from the
     * SQL type of the parameter to the requested Java data type, if the
     * conversion is supported. If the conversion is not
     * supported  or null is specified for the type, a
     * {@code SQLException} is thrown.
     * <p>
     * At a minimum, an implementation must support the conversions defined in
     * Appendix B, Table B-3 and conversion of appropriate user defined SQL
     * types to a Java type which implements {@code SQLData}, or {@code Struct}.
     * Additional conversions may be supported and are vendor defined.
     *
     * @param parameterName the name of the parameter
     * @param type Class representing the Java data type to convert
     * the designated parameter to.
     * @param <T> the type of the class modeled by this Class object
     * @return an instance of {@code type} holding the OUT parameter
     * value
     * @throws SQLException if conversion is not supported, type is null or
     *         another error occurs. The getCause() method of the
     * exception may provide a more detailed exception, for example, if
     * a conversion error occurs
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.7, HSQLDB 2.0.1
     */
    public <T> T getObject(
            String parameterName,
            Class<T> type)
            throws SQLException {
        return getObject(findParameterIndex(parameterName), type);
    }

    //------------------------- JDBC 4.2 -----------------------------------

    /**
     * Sets the value of the designated parameter with the given object.
     *
     * If the second argument is an {@code InputStream} then the stream
     * must contain the number of bytes specified by scaleOrLength.
     * If the second argument is a {@code Reader} then the reader must
     * contain the number of characters specified
     * by scaleOrLength. If these conditions are not true the driver
     * will generate a
     * {@code SQLException} when the prepared statement is executed.
     *
     * <p>The given Java object will be converted to the given targetSqlType
     * before being sent to the database.
     *
     * If the object has a custom mapping (is of a class implementing the
     * interface {@code SQLData}),
     * the JDBC driver should call the method {@code SQLData.writeSQL} to
     * write it to the SQL data stream.
     * If, on the other hand, the object is of a class implementing
     * {@code Ref}, {@code Blob}, {@code Clob},  {@code NClob},
     *  {@code Struct}, {@code java.net.URL},
     * or {@code Array}, the driver should pass it to the database as a
     * value of the corresponding SQL type.
     *
     * <p>Note that this method may be used to pass database-specific
     * abstract data types.
     *<P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterName the name of the parameter
     * @param x the object containing the input parameter value
     * @param targetSqlType the SQL type to be
     * sent to the database. The scale argument may further qualify this type.
     * @param scaleOrLength for {@code java.sql.JDBCType.DECIMAL}
     *          or {@code java.sql.JDBCType.NUMERIC types},
     *          this is the number of digits after the decimal point. For
     *          Java Object types {@code InputStream} and {@code Reader},
     *          this is the length
     *          of the data in the stream or reader.  For all other types,
     *          this value will be ignored.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs
     * or this method is called on a closed {@code CallableStatement}  or
     *            if the Java Object specified by x is an InputStream
     *            or Reader object and the value of the scale parameter is less
     *            than zero
     * @throws SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified targetSqlType
     * @see JDBCType
     * @see SQLType
     *
     * @since 1.8
     */
    public void setObject(
            String parameterName,
            Object x,
            SQLType targetSqlType,
            int scaleOrLength)
            throws SQLException {

        setObject(
            parameterName,
            x,
            targetSqlType.getVendorTypeNumber(),
            scaleOrLength);
    }

    /**
     * Sets the value of the designated parameter with the given object.
     *
     * This method is similar to {@link #setObject(String parameterName,
     * Object x, SQLType targetSqlType, int scaleOrLength)},
     * except that it assumes a scale of zero.
     *<P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterName the name of the parameter
     * @param x the object containing the input parameter value
     * @param targetSqlType the SQL type to be sent to the database
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs
     * or this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified targetSqlType
     * @see JDBCType
     * @see SQLType
     * @since 1.8
     */
    public void setObject(
            String parameterName,
            Object x,
            SQLType targetSqlType)
            throws SQLException {
        setObject(parameterName, x, targetSqlType.getVendorTypeNumber());
    }

    /**
     * Registers the OUT parameter in ordinal position
     * {@code parameterIndex} to the JDBC type
     * {@code sqlType}.  All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by {@code sqlType} for an OUT
     * parameter determines the Java type that must be used
     * in the {@code get} method to read the value of that parameter.
     * <p>
     * If the JDBC type expected to be returned to this output parameter
     * is specific to this particular database, {@code sqlType}
     * may be {@code JDBCType.OTHER} or a {@code SQLType} that is supported by
     * the JDBC driver.  The method
     * {@link #getObject} retrieves the value.
     *<P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     *        and so on
     * @param sqlType the JDBC type code defined by {@code SQLType} to use to
     * register the OUT Parameter.
     *        If the parameter is of JDBC type {@code JDBCType.NUMERIC}
     *        or {@code JDBCType.DECIMAL}, the version of
     *        {@code registerOutParameter} that accepts a scale value
     *        should be used.
     *
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified sqlType
     * @see JDBCType
     * @see SQLType
     * @since 1.8
     */
    public void registerOutParameter(
            int parameterIndex,
            SQLType sqlType)
            throws SQLException {
        registerOutParameter(parameterIndex, sqlType.getVendorTypeNumber());
    }

    /**
     * Registers the parameter in ordinal position
     * {@code parameterIndex} to be of JDBC type
     * {@code sqlType}. All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by {@code sqlType} for an OUT
     * parameter determines the Java type that must be used
     * in the {@code get} method to read the value of that parameter.
     * <p>
     * This version of {@code  registerOutParameter} should be
     * used when the parameter is of JDBC type {@code JDBCType.NUMERIC}
     * or {@code JDBCType.DECIMAL}.
     *<P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterIndex the first parameter is 1, the second is 2,
     * and so on
     * @param sqlType the JDBC type code defined by {@code SQLType} to use to
     * register the OUT Parameter.
     * @param scale the desired number of digits to the right of the
     * decimal point.  It must be greater than or equal to zero.
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified sqlType
     * @see JDBCType
     * @see SQLType
     * @since 1.8
     */
    public void registerOutParameter(
            int parameterIndex,
            SQLType sqlType,
            int scale)
            throws SQLException {

        registerOutParameter(
            parameterIndex,
            sqlType.getVendorTypeNumber(),
            scale);
    }

    /**
     * Registers the designated output parameter.
     * This version of
     * the method {@code  registerOutParameter}
     * should be used for a user-defined or {@code REF} output parameter.
     * Examples
     * of user-defined types include: {@code STRUCT}, {@code DISTINCT},
     * {@code JAVA_OBJECT}, and named array types.
     *<p>
     * All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>  For a user-defined parameter, the fully-qualified SQL
     * type name of the parameter should also be given, while a {@code REF}
     * parameter requires that the fully-qualified type name of the
     * referenced type be given.  A JDBC driver that does not need the
     * type code and type name information may ignore it.   To be portable,
     * however, applications should always provide these values for
     * user-defined and {@code REF} parameters.
     *
     * Although it is intended for user-defined and {@code REF} parameters,
     * this method may be used to register a parameter of any JDBC type.
     * If the parameter does not have a user-defined or {@code REF} type, the
     * <i>typeName</i> parameter is ignored.
     *
     * <P><B>Note:</B> When reading the value of an out parameter, you
     * must use the getter method whose Java type corresponds to the
     * parameter's registered SQL type.
     *<P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterIndex the first parameter is 1, the second is 2,...
     * @param sqlType the JDBC type code defined by {@code SQLType} to use to
     * register the OUT Parameter.
     * @param typeName the fully-qualified name of an SQL structured type
     * @throws SQLException if the parameterIndex is not valid;
     * if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified sqlType
     * @see JDBCType
     * @see SQLType
     * @since 1.8
     */
    public void registerOutParameter(
            int parameterIndex,
            SQLType sqlType,
            String typeName)
            throws SQLException {

        registerOutParameter(
            parameterIndex,
            sqlType.getVendorTypeNumber(),
            typeName);
    }

    /**
     * Registers the OUT parameter named
     * {@code parameterName} to the JDBC type
     * {@code sqlType}.  All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by {@code sqlType} for an OUT
     * parameter determines the Java type that must be used
     * in the {@code get} method to read the value of that parameter.
     * <p>
     * If the JDBC type expected to be returned to this output parameter
     * is specific to this particular database, {@code sqlType}
     * should be {@code JDBCType.OTHER} or a {@code SQLType} that is supported
     * by the JDBC driver.  The method
     * {@link #getObject} retrieves the value.
     *<P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterName the name of the parameter
     * @param sqlType the JDBC type code defined by {@code SQLType} to use to
     * register the OUT Parameter.
     * If the parameter is of JDBC type {@code JDBCType.NUMERIC}
     * or {@code JDBCType.DECIMAL}, the version of
     * {@code  registerOutParameter} that accepts a scale value
     * should be used.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified sqlType
     * or if the JDBC driver does not support
     * this method
     * @since 1.8
     * @see JDBCType
     * @see SQLType
     */
    public void registerOutParameter(
            String parameterName,
            SQLType sqlType)
            throws SQLException {
        registerOutParameter(parameterName, sqlType.getVendorTypeNumber());
    }

    /**
     * Registers the parameter named
     * {@code parameterName} to be of JDBC type
     * {@code sqlType}.  All OUT parameters must be registered
     * before a stored procedure is executed.
     * <p>
     * The JDBC type specified by {@code sqlType} for an OUT
     * parameter determines the Java type that must be used
     * in the {@code get} method to read the value of that parameter.
     * <p>
     * This version of {@code  registerOutParameter} should be
     * used when the parameter is of JDBC type {@code JDBCType.NUMERIC}
     * or {@code JDBCType.DECIMAL}.
     *<P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterName the name of the parameter
     * @param sqlType the JDBC type code defined by {@code SQLType} to use to
     * register the OUT Parameter.
     * @param scale the desired number of digits to the right of the
     * decimal point.  It must be greater than or equal to zero.
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified sqlType
     * or if the JDBC driver does not support
     * this method
     * @since 1.8
     * @see JDBCType
     * @see SQLType
     */
    public void registerOutParameter(
            String parameterName,
            SQLType sqlType,
            int scale)
            throws SQLException {

        registerOutParameter(
            parameterName,
            sqlType.getVendorTypeNumber(),
            scale);
    }

    /**
     * Registers the designated output parameter.  This version of
     * the method {@code  registerOutParameter}
     * should be used for a user-named or REF output parameter.  Examples
     * of user-named types include: STRUCT, DISTINCT, JAVA_OBJECT, and
     * named array types.
     *<p>
     * All OUT parameters must be registered
     * before a stored procedure is executed.
     * </p>
     * For a user-named parameter the fully-qualified SQL
     * type name of the parameter should also be given, while a REF
     * parameter requires that the fully-qualified type name of the
     * referenced type be given.  A JDBC driver that does not need the
     * type code and type name information may ignore it.   To be portable,
     * however, applications should always provide these values for
     * user-named and REF parameters.
     *
     * Although it is intended for user-named and REF parameters,
     * this method may be used to register a parameter of any JDBC type.
     * If the parameter does not have a user-named or REF type, the
     * typeName parameter is ignored.
     *
     * <P><B>Note:</B> When reading the value of an out parameter, you
     * must use the {@code getXXX} method whose Java type XXX corresponds to the
     * parameter's registered SQL type.
     *<P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterName the name of the parameter
     * @param sqlType the JDBC type code defined by {@code SQLType} to use to
     * register the OUT Parameter.
     * @param typeName the fully-qualified name of an SQL structured type
     * @throws SQLException if parameterName does not correspond to a named
     * parameter; if a database access error occurs or
     * this method is called on a closed {@code CallableStatement}
     * @throws SQLFeatureNotSupportedException if
     * the JDBC driver does not support the specified sqlType
     * or if the JDBC driver does not support this method
     * @see JDBCType
     * @see SQLType
     * @since 1.8
     */
    public void registerOutParameter(
            String parameterName,
            SQLType sqlType,
            String typeName)
            throws SQLException {

        registerOutParameter(
            parameterName,
            sqlType.getVendorTypeNumber(),
            typeName);
    }

// --------------------------- Internal Implementation -------------------------

    /** parameter name maps to parameter index */
    private IntValueHashMap<String> parameterNameMap;
    private boolean                 wasNullValue;

    /* parameter index => registered OUT type */

//  private IntKeyIntValueHashMap outRegistrationMap;

    /**
     * Constructs a new JDBCCallableStatement with the specified connection and
     * result type.
     *
     * @param  c the connection on which this statement will execute
     * @param sql the SQL statement this object represents
     * @param resultSetType the type of result this statement will produce
     * @param resultSetConcurrency (updatability)
     * @param resultSetHoldability (validity beyond commit)
     * @throws HsqlException if the statement is not accepted by the database
     * @throws SQLException if preprocessing by driver fails
     */
    public JDBCCallableStatement(
            JDBCConnection c,
            String sql,
            int resultSetType,
            int resultSetConcurrency,
            int resultSetHoldability)
            throws HsqlException,
                   SQLException {

        super(
            c,
            sql,
            resultSetType,
            resultSetConcurrency,
            resultSetHoldability,
            ResultConstants.RETURN_NO_GENERATED_KEYS,
            null,
            null);

        String[] names;
        String   name;

        // outRegistrationMap = new IntKeyIntValueHashMap();
        parameterNameMap = new IntValueHashMap<>();

        if (parameterMetaData != null) {
            names = parameterMetaData.columnLabels;

            for (int i = 0; i < names.length; i++) {
                name = names[i];

                // PRE:  should never happen in practice
                if (name == null || name.isEmpty()) {
                    continue;    // throw?
                }

                parameterNameMap.put(name, i);
            }
        }
    }

    void fetchResult() throws SQLException {

        super.fetchResult();

        if (resultIn.getType() == ResultConstants.CALL_RESPONSE) {
            Object[] data = resultIn.getParameterData();

            System.arraycopy(
                data,
                0,
                parameterValues,
                0,
                parameterValues.length);
        }
    }

    /**
     * Retrieves the parameter index corresponding to the given
     * parameter name.
     *
     * @param parameterName to look up
     * @throws SQLException if not found
     * @return index for name
     */
    int findParameterIndex(String parameterName) throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        if (parameterName == null) {
            throw JDBCUtil.nullArgument();
        }

        int index = parameterNameMap.get(parameterName, -1);

        if (index >= 0) {
            return index + 1;
        }

        index = parameterNameMap.get(
            parameterName.toUpperCase(Locale.ENGLISH),
            -1);

        if (index >= 0) {
            return index + 1;
        }

        throw JDBCUtil.sqlException(
            ErrorCode.JDBC_COLUMN_NOT_FOUND,
            parameterName);
    }

    /**
     * Does the specialized work required to free this object's resources and
     * that of its parent classes.
     *
     * @throws SQLException if a database access error occurs
     */
    public synchronized void close() throws SQLException {

        if (isClosed()) {
            return;
        }

        // outRegistrationMap = null;
        parameterNameMap = null;

        super.close();
    }

    /*
     * Checks if the parameter of the given index has been successfully
     * registered as an OUT parameter. <p>
     *
     * @param parameterIndex to check
     * @throws SQLException if not registered
     */

/*
    private void checkIsRegisteredParameterIndex(int parameterIndex)
    throws SQLException {

        int    type;
        String msg;

        checkClosed();

        type = outRegistrationMap.get(parameterIndex, Integer.MIN_VALUE);

        if (type == Integer.MIN_VALUE) {
            msg = "Parameter not registered: " + parameterIndex; //NOI18N

            throw JDBCUtil.sqlException(ErrorCode.INVALID_JDBC_ARGUMENT, msg);
        }
    }
*/

    /**
     * Internal get value.
     */
    protected Object getColumnValue(int columnIndex) throws SQLException {

        checkGetParameterIndex(columnIndex);

        Object value = parameterValues[columnIndex - 1];

        trackNull(value);

        return value;
    }

    /**
     * Internal value converter. Similar to its counterpart in JDBCResultSet <p>
     *
     * All trivially successful getXXX methods eventually go through this
     * method, converting if necessary from the source type to the
     * requested type.  <p>
     *
     * Conversion to the JDBC representation, if different, is handled by the
     * calling methods.
     *
     * @param columnIndex of the column value for which to perform the
     *                 conversion
     * @param targetType the org.hsqldb.types.Type object for targetType
     * @return an Object of the requested targetType, representing the value of the
     *       specified column
     * @throws SQLException when there is no rowData, the column index is
     *    invalid, or the conversion cannot be performed
     */
    private Object getColumnInType(
            int columnIndex,
            Type targetType)
            throws SQLException {

        checkGetParameterIndex(columnIndex);

        Type   sourceType;
        Object value;

        sourceType = parameterTypes[--columnIndex];
        value      = parameterValues[columnIndex];

        if (trackNull(value)) {
            return null;
        }

        if (sourceType.typeCode != targetType.typeCode) {
            try {
                value = targetType.convertToTypeJDBC(
                    session,
                    value,
                    sourceType);
            } catch (HsqlException e) {
                String stringValue = (value instanceof Number
                                      || value instanceof String
                                      || value instanceof java.util.Date)
                                     ? value.toString()
                                     : "instance of "
                                       + value.getClass().getName();
                String msg = "from SQL type " + sourceType.getNameString()
                             + " to " + targetType.getJDBCClassName()
                             + ", value: " + stringValue;

                throw JDBCUtil.sqlException(ErrorCode.X_42561, msg);
            }
        }

        return value;
    }

    private Object getTimestampWithZone(int columnIndex) throws SQLException {

        TimestampData v = (TimestampData) getColumnInType(
            columnIndex,
            Type.SQL_TIMESTAMP_WITH_TIME_ZONE);

        if (v == null) {
            return null;
        }

        return Type.SQL_TIMESTAMP_WITH_TIME_ZONE.convertSQLToJava(session, v);
    }

    private Object getTimeWithZone(int columnIndex) throws SQLException {

        TimeData v = (TimeData) getColumnInType(
            columnIndex,
            Type.SQL_TIME_WITH_TIME_ZONE);

        if (v == null) {
            return null;
        }

        return Type.SQL_TIME_WITH_TIME_ZONE.convertSQLToJava(session, v);
    }

    private boolean trackNull(Object o) {
        return (wasNullValue = (o == null));
    }

    public void closeOnCompletion() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean isCloseOnCompletion() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Executes the SQL query in this {@code PreparedStatement} object
     * and returns the {@code ResultSet} object generated by the query.
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this method for a call to a FUNCTION that returns a result.
     * For a PROCEDURE that returns one or more results, the first result is
     * returned.<p>
     *
     * If the FUNCTION or PROCEDURE does not return a ResultSet, an
     * {@code SQLException} is thrown.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return a {@code ResultSet} object that contains the data produced by the
     *         query; never {@code null}
     * @throws SQLException if a database access error occurs,
     * this method is called on a closed  {@code PreparedStatement} or the SQL
     *            statement does not return a {@code ResultSet} object
     */
    public synchronized ResultSet executeQuery() throws SQLException {

        fetchResult();

        ResultSet rs = getResultSet();

        if (rs != null) {
            return rs;
        }

        if (getMoreResults()) {
            return getResultSet();
        }

        throw JDBCUtil.sqlException(ErrorCode.X_07504);
    }
}
