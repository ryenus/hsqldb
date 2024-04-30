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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLFeatureNotSupportedException;

//import java.sql.SQLData;
//import java.sql.SQLOutput;
//import java.sql.SQLInput;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

// import java.util.logging.Level;
// import java.util.logging.Logger;
import org.hsqldb.ClientConnection;
import org.hsqldb.ClientConnectionHTTP;
import org.hsqldb.DatabaseManager;
import org.hsqldb.DatabaseURL;
import org.hsqldb.HsqlException;
import org.hsqldb.SessionInterface;
import org.hsqldb.SessionInterface.Attributes;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.result.ResultProperties;
import org.hsqldb.types.ArrayType;
import org.hsqldb.types.Type;

// fredt@users    20020320 - patch 1.7.0 - JDBC 2 support and error trapping
//
// campbell-burnet@users 20020509 - added "throws SQLException" to all methods where
//                           it was missing here but specified in the
//                           java.sql.Connection interface,
//                           updated generic documentation to JDK 1.4, and
//                           added JDBC3 methods and docs
// boucherb &
// fredt@users    20020505 - extensive review and update of docs and behaviour
//                           to comply with java.sql specification
// fredt@users    20020830 - patch 487323 by xclayl@users - better synchronization
// fredt@users    20020930 - patch 1.7.1 - support for connection properties
// kneedeepincode@users
//                20021110 - patch 635816 - correction to properties
// unsaved@users  20021113 - patch 1.7.2 - SSL support
// campbell-burnet@users 2003 ??? - patch 1.7.2 - SSL support moved to factory interface
// fredt@users    20030620 - patch 1.7.2 - reworked to use a SessionInterface
// campbell-burnet@users 20030801 - JavaDoc updates to reflect new connection urls
// campbell-burnet@users 20030819 - patch 1.7.2 - partial fix for broken nativeSQL method
// campbell-burnet@users 20030819 - patch 1.7.2 - SQLWarning cases implemented
// campbell-burnet@users 20051207 - 1.9.0       - JDBC 4.0 support - docs and methods
//              - 20060712               - full synch up to JAVA 1.6 (Mustang) Build 90
// fredt@users    20090810 - 1.9.0       - full review and updates

/**
 * A connection (session) with a specific
 * database. SQL statements are executed and results are returned
 * within the context of a connection.
 * <P>
 * A {@code Connection} object's database is able to provide information
 * describing its tables, its supported SQL grammar, its stored
 * procedures, the capabilities of this connection, and so on. This
 * information is obtained with the {@code getMetaData} method.
 *
 * <P><B>Note:</B> When configuring a {@code Connection}, JDBC applications
 *  should use the appropriate {@code Connection} method such as
 *  {@code setAutoCommit} or {@code setTransactionIsolation}.
 *  Applications should not invoke SQL commands directly to change the connection's
 *   configuration when there is a JDBC method available.  By default a {@code Connection} object is in
 * auto-commit mode, which means that it automatically commits changes
 * after executing each statement. If auto-commit mode has been
 * disabled, the method {@code commit} must be called explicitly in
 * order to commit changes; otherwise, database changes will not be saved.
 * <P>
 * A new {@code Connection} object created using the JDBC 2.1 core API
 * has an initially empty type map associated with it. A user may enter a
 * custom mapping for a UDT in this type map.
 * When a UDT is retrieved from a data source with the
 * method {@code ResultSet.getObject}, the {@code getObject} method
 * will check the connection's type map to see if there is an entry for that
 * UDT.  If so, the {@code getObject} method will map the UDT to the
 * class indicated.  If there is no entry, the UDT will be mapped using the
 * standard mapping.
 * <p>
 * A user may create a new type map, which is a {@code java.util.Map}
 * object, make an entry in it, and pass it to the {@code java.sql}
 * methods that can perform custom mapping.  In this case, the method
 * will use the given type map instead of the one associated with
 * the connection.
 * <p>
 * For example, the following code fragment specifies that the SQL
 * type {@code ATHLETES} will be mapped to the class
 * {@code Athletes} in the Java programming language.
 * The code fragment retrieves the type map for the {@code Connection
 * } object {@code con}, inserts the entry into it, and then sets
 * the type map with the new entry as the connection's type map.
 * <pre>
 *      java.util.Map map = con.getTypeMap();
 *      map.put("mySchemaName.ATHLETES", Class.forName("Athletes"));
 *      con.setTypeMap(map);
 * </pre>
 *

 *
 * <!-- start release-specific documentation -->
 * <div class="ReleaseSpecificDocumentation">
 * <p class="rshead">HSQLDB-Specific Information:</p>
 *
 * To get a {@code Connection} to an HSQLDB database, the
 * following code may be used (updated to reflect the most recent
 * recommendations):
 *
 * <hr>
 *
 * When using HSQLDB, the database connection <b>&lt;url&gt;</b> must start with
 * <b>'jdbc:hsqldb:'</b><p>
 *
 * Since 1.7.2, connection properties (&lt;key-value-pairs&gt;) may be appended
 * to the database connection <b>&lt;url&gt;</b>, using the form:
 *
 * <blockquote>
 *      <b>'&lt;url&gt;[;key=value]*'</b>
 * </blockquote> <p>
 *
 * Also since 1.7.2, the allowable forms of the HSQLDB database connection
 * <b>&lt;url&gt;</b> have been extended.  However, all legacy forms continue
 * to work, with unchanged semantics.  The extensions are as described in the
 * following material.
 *
 * <hr>
 *
 * <b>Network Server Database Connections:</b> <p>
 *
 * The {@link org.hsqldb.server.Server Server} database connection <b>&lt;url&gt;</b>
 * takes one of the two following forms:
 *
 * <div class="GeneralExample">
 * <ol>
 * <li> <b>'jdbc:hsqldb:hsql://host[:port][/&lt;alias&gt;][&lt;key-value-pairs&gt;]'</b>
 *
 * <li> <b>'jdbc:hsqldb:hsqls://host[:port][/&lt;alias&gt;][&lt;key-value-pairs&gt;]'</b>
 *         (with TLS).
 * </ol>
 * </div> <p>
 *
 * The {@link org.hsqldb.server.WebServer WebServer} database connection <b>&lt;url&gt;</b>
 * takes one of two following forms:
 *
 * <div class="GeneralExample">
 * <ol>
 * <li> <b>'jdbc:hsqldb:http://host[:port][/&lt;alias&gt;][&lt;key-value-pairs&gt;]'</b>
 *
 * <li> <b>'jdbc:hsqldb:https://host[:port][/&lt;alias&gt;][&lt;key-value-pairs&gt;]'</b>
 *      (with TLS).
 * </ol>
 * </div><p>
 *
 * In both network server database connection <b>&lt;url&gt;</b> forms, the
 * optional <b>&lt;alias&gt;</b> component is used to identify one of possibly
 * several database instances available at the indicated host and port.  If the
 * <b>&lt;alias&gt;</b> component is omitted, then a connection is made to the
 * network server's default database instance, if such an instance is
 * available. <p>
 *
 * For more information on server configuration regarding mounting multiple
 * databases and assigning them <b>&lt;alias&gt;</b> values, please read the
 * Java API documentation for {@link org.hsqldb.server.Server Server} and related
 * chapters in the general documentation, especially the <em>HyperSQL User
 * Guide</em>.
 *
 * <hr>
 *
 * <b>Transient, In-Process Database Connections:</b> <p>
 *
 * The 100% in-memory (transient, in-process) database connection
 * <b>&lt;url&gt;</b> takes one of the two following forms:
 *
 * <div class="GeneralExample">
 * <ol>
 * <li> <b>'jdbc:hsqldb:.[&lt;key-value-pairs&gt;]'</b>
 *     (the legacy form, extended)
 *
 * <li> <b>'jdbc:hsqldb:mem:&lt;alias&gt;[&lt;key-value-pairs&gt;]'</b>
 *      (the new form)
 * </ol>
 * </div> <p>
 *
 * The driver converts the supplied <b>&lt;alias&gt;</b> component to
 * Local.ENGLISH lower case and uses the resulting character sequence as the
 * key used to look up a <b>mem:</b> protocol database instance amongst the
 * collection of all such instances already in existence within the current
 * class loading context in the current JVM. If no such instance exists, one
 * <em>may</em> be automatically created and mapped to the <b>&lt;alias&gt;</b>,
 * as governed by the <b>'ifexists=true|false'</b> connection property. <p>
 *
 * The rationale for converting the supplied <b>&lt;alias&gt;</b> component to
 * lower case is to provide consistency with the behavior of <b>res:</b>
 * protocol database connection <b>&lt;url&gt;</b>s, explained further on in
 * this overview.
 *
 * <hr>
 *
 * <b>Persistent, In-Process Database Connections:</b> <p>
 *
 * The standalone (persistent, in-process) database connection
 * <b>&lt;url&gt;</b> takes one of the three following forms:
 *
 * <div class="GeneralExample">
 * <ol>
 * <li> <b>'jdbc:hsqldb:&lt;path&gt;[&lt;key-value-pairs&gt;]'</b>
 *      (the legacy form, extended)
 *
 * <li> <b>'jdbc:hsqldb:file:&lt;path&gt;[&lt;key-value-pairs&gt;]'</b>
 *      (same semantics as the legacy form)
 *
 * <li> <b>'jdbc:hsqldb:res:&lt;path&gt;[&lt;key-value-pairs&gt;]'</b>
 *      (new form with 'files_in_jar' semantics)
 * </ol>
 * </div> <p>
 *
 * For the persistent, in-process database connection <b>&lt;url&gt;</b>,
 * the <b>&lt;path&gt;</b> component is the path prefix common to all of
 * the files that compose the database. <p>
 *
 * From 1.7.2, although other files may be involved (such as transient working
 * files and/or TEXT table CSV data source files), the essential set that may,
 * at any particular point in time, compose an HSQLDB database is:
 *
 * <div class="GeneralExample">
 * <ul>
 * <li>&lt;path&gt;.properties
 * <li>&lt;path&gt;.script
 * <li>&lt;path&gt;.log
 * <li>&lt;path&gt;.data
 * <li>&lt;path&gt;.backup
 * <li>&lt;path&gt;.lck
 * </ul>
 * </div> <p>
 *
 * For example: <b>'jdbc:hsqldb:file:test'</b> connects to a database
 * composed of some subset of the files listed above, where the expansion
 * of <b>&lt;path&gt;</b> is <b>'test'</b> prefixed with the canonical path of
 * the JVM's effective working directory at the time the designated database
 * is first opened in-process. <p>
 *
 * Be careful to note that this canonical expansion of <b>&lt;path&gt;</b> is
 * cached by the driver until JVM exit. So, although legacy JVMs tend to fix
 * the reported effective working directory at the one noted upon JVM startup,
 * there is no guarantee that modern JVMs will continue to uphold this
 * behaviour.  What this means is there is effectively no guarantee into the
 * future that a relative <b>file:</b> protocol database connection
 * <b>&lt;url&gt;</b> will connect to the same database instance for the life
 * of the JVM.  To avoid any future ambiguity issues, it is probably a best
 * practice for clients to attempt to pre-canonicalize the <b>&lt;path&gt;</b>
 * component of <b>file:</b> protocol database connection* <b>&lt;url&gt;</b>s.
 * <p>
 *
 * Under <em>Windows</em>,
 * <b>'jdbc:hsqldb:file:c:\databases\test'</b> connects to a database located
 * on drive <b>'C:'</b> in the directory <b>'databases'</b>, composed
 * of some subset of the files:
 *
 * <pre class="GeneralExample">
 * C:\
 * +--databases\
 *    +--test.properties
 *    +--test.script
 *    +--test.log
 *    +--test.data
 *    +--test.backup
 *    +--test.lck
 * </pre>
 *
 * Under most variations of UNIX, <b>'jdbc:hsqldb:file:/databases/test'</b>
 * connects to a database located in the directory <b>'databases'</b> directly
 * under root, once again composed of some subset of the files:
 *
 * <pre class="GeneralExample">
 *
 * +--databases
 *    +--test.properties
 *    +--test.script
 *    +--test.log
 *    +--test.data
 *    +--test.backup
 *    +--test.lck
 * </pre>
 *
 * <b>Some Guidelines:</b>
 *
 * <ol>
 * <li> Both relative and absolute database file paths are supported.
 *
 * <li> Relative database file paths can be specified in a platform independent
 *      manner as: <b>'[dir1/dir2/.../dirn/]&lt;file-name-prefix&gt;'</b>.
 *
 * <li> Specification of absolute file paths is operating-system specific.<br>
 *      Please read your OS file system documentation.
 *
 * <li> Specification of network mounts may be operating-system specific.<br>
 *      Please read your OS file system documentation.
 *
 * <li> Special care may be needed w.r.t. file path specifications
 *      containing whitespace, mixed-case, special characters and/or
 *      reserved file names.<br>
 *      Please read your OS file system documentation.
 * </ol> <p>
 *
 * <b>Note:</b>HSQLDB creates
 * directories along the file path specified in the persistent, in-process mode
 * database connection <b>&lt;url&gt;</b> form, in the case that they did
 * not already exist.
 * <hr>
 *
 * <b>res: protocol Connections:</b><p>
 *
 * The <b>'jdbc:hsqldb:res:&lt;path&gt;'</b> database connection
 * <b>&lt;url&gt;</b> has different semantics than the
 * <b>'jdbc:hsqldb:file:&lt;path&gt;'</b> form. The semantics are similar to
 * those of a <b>'files_readonly'</b> database, but with some additional
 * points to consider. <p>
 *
 * Specifically, the <b>'&lt;path&gt;'</b> component of a <b>res:</b> protocol
 * database connection <b>&lt;url&gt;</b> is first converted to lower case
 * with {@code Locale.ENGLISH} and only then used to obtain resource URL
 * objects, which in turn are used to read the database files as resources on
 * the class path. <p>
 *
 * Due to lower case conversion by the driver, <b>res:</b> <b>'&lt;path&gt;'</b>
 * components <em>never</em> find jar resources stored with
 * {@code Locale.ENGLISH} mixed case paths. The rationale for converting to
 * lower case is that not all pkzip implementations guarantee path case is
 * preserved when archiving resources, and conversion to lower case seems to
 * be the most common occurrence (although there is also no actual guarantee
 * that the conversion is {@code Locale.ENGLISH}).<p>
 *
 * More importantly, <b>res:</b> <b>'&lt;path&gt;'</b> components <em>must</em>
 * point only to resources contained in one or more jars on the class
 * path. That is, only resources having the jar sub-protocol are considered
 * valid. <p>
 *
 * This restriction is enforced to avoid the unfortunate situation in which,
 * because <b>res:</b> database instances do not create a <b>&lt;path&gt;</b>.lck
 * file (they are strictly files-read-only) and because the <b>&lt;path&gt;</b>
 * components of <b>res:</b> and <b>file:</b> database {@code URI}s are not
 * checked for file system equivalence, it is possible for the same database
 * files to be accessed concurrently by both <b>file:</b> and <b>res:</b>
 * database instances. That is, without this restriction, it is possible that
 * <b>&lt;path&gt;</b>.data and <b>&lt;path&gt;</b>.properties file content may
 * be written by a <b>file:</b> database instance without the knowledge or
 * cooperation of a <b>res:</b> database instance open on the same files,
 * potentially resulting in unexpected database errors, inconsistent operation
 * and/or data corruption. <p>
 *
 * In short, a <b>res:</b> type database connection <b>&lt;url&gt;</b> is
 * designed specifically to connect to a <b>'files_in_jar'</b> mode database
 * instance, which in turn is designed specifically to operate under
 * <em>Java WebStart</em> and
 * <em>Java Applet</em> configurations,
 * where co-locating the database files in the jars that make up the
 * <em>WebStart</em> application or Applet avoids the need for special security
 * configuration or code signing. <p>
 *
 * <b>Note:</b> Since it is difficult and often nearly impossible to determine
 * or control at runtime from where all classes are being loaded or which class
 * loader is doing the loading (and hence how relative path specifications
 * are resolved) under <b>'files_in_jar'</b> semantics, the <b>&lt;path&gt;</b>
 * component of the <b>res:</b> database connection <b>&lt;url&gt;</b> is always
 * taken to be relative to the default package and resource URL resolution is
 * always performed using the ClassLoader that loads the
 * org.hsqldb.persist.Logger class. That is, if the <b>&lt;path&gt;</b>
 * component does not start with '/', then'/' is prepended when obtaining the
 * resource URLs used to read the database files, and only the effective class
 * path of org.hsqldb.persist.Logger's ClassLoader is searched.
 *
 * <hr>
 *
 * For more information about HSQLDB file structure, various database modes
 * and other attributes such as those controlled through the HSQLDB properties
 * files, please read the general documentation, especially the HyperSQL User
 * Guide. <p>
 *
 * <b>JDBC 4.0 Notes:</b><p>
 *
 * Starting with JDBC 4.0 (JDK 1.6), the {@code DriverManager} methods
 * {@code getConnection} and {@code getDrivers} have been
 * enhanced to support the Java Standard Edition Service Provider mechanism.
 * HSQLDB distribution
 * jars containing the Driver implementation also include the file
 * {@code META-INF/services/java.sql.Driver}. This file contains the fully
 * qualified class name ('org.hsqldb.jdbc.JDBCDriver') of the HSQLDB implementation
 * of {@code java.sql.Driver}. <p>
 *
 * Hence, under JDBC 4.0 or greater, applications no longer need to explicitly
 * load the HSQLDB JDBC driver using {@code Class.forName()}. Of course,
 * existing programs which do load JDBC drivers using
 * {@code Class.forName()} will continue to work without modification.
 *
 * <hr>
 *
 * (fredt@users)<br>
 * (campbell-burnet@users)
 *
 * </div>
 * <!-- end release-specific documentation -->
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
 * @since HSQLDB 1.9.0
 * @see JDBCDriver
 * @see JDBCStatement
 * @see JDBCParameterMetaData
 * @see JDBCCallableStatement
 * @see JDBCResultSet
 * @see JDBCDatabaseMetaData
 * @see java.sql.DriverManager
 * @see java.sql.Statement
 * @see java.sql.ResultSet
 * @see java.sql.DatabaseMetaData
 */
public class JDBCConnection implements Connection {

    /**
     * Creates a {@code Statement} object for sending
     * SQL statements to the database.
     * SQL statements without parameters are normally
     * executed using {@code Statement} objects. If the same SQL statement
     * is executed many times, it may be more efficient to use a
     * {@code PreparedStatement} object.
     * <P>
     * Result sets created using the returned {@code Statement}
     * object will by default be type {@code TYPE_FORWARD_ONLY}
     * and have a concurrency level of {@code CONCUR_READ_ONLY}.
     * The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with HSQLDB 1.7.2, support for precompilation at the engine level
     * has been implemented, so it is now much more efficient and performant
     * to use a {@code PreparedStatement} object if the same short-running
     * SQL statement is to be executed many times. <p>
     *
     * HSQLDB supports {@code TYPE_FORWARD_ONLY},
     * {@code TYPE_SCROLL_INSENSITIVE} and {@code CONCUR_READ_ONLY}
     * results.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return a new default {@code Statement} object
     * @throws SQLException if a database access error occurs
     * or this method is called on a closed connection
     * @see #createStatement(int,int)
     * @see #createStatement(int,int,int)
     */
    public synchronized Statement createStatement() throws SQLException {

        checkClosed();

        int props = ResultProperties.getValueForJDBC(
            JDBCResultSet.TYPE_FORWARD_ONLY,
            JDBCResultSet.CONCUR_READ_ONLY,
            rsHoldability);
        Statement stmt = new JDBCStatement(this, props);

        return stmt;
    }

    /**
     * Creates a {@code PreparedStatement} object for sending
     * parameterized SQL statements to the database.
     * <P>
     * A SQL statement with or without IN parameters can be
     * pre-compiled and stored in a {@code PreparedStatement} object. This
     * object can then be used to efficiently execute this statement
     * multiple times.
     *
     * <P><B>Note:</B> This method is optimized for handling
     * parametric SQL statements that benefit from precompilation. If
     * the driver supports precompilation,
     * the method {@code prepareStatement} will send
     * the statement to the database for precompilation. Some drivers
     * may not support precompilation. In this case, the statement may
     * not be sent to the database until the {@code PreparedStatement}
     * object is executed.  This has no direct effect on users; however, it does
     * affect which methods throw certain {@code SQLException} objects.
     * <P>
     * Result sets created using the returned {@code PreparedStatement}
     * object will by default be type {@code TYPE_FORWARD_ONLY}
     * and have a concurrency level of {@code CONCUR_READ_ONLY}.
     * The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with HSQLDB 1.7.2, support for precompilation at the engine level
     * has been implemented, so it is now much more efficient and performant
     * to use a {@code PreparedStatement} object if the same short-running
     * SQL statement is to be executed many times. <p>
     *
     * The support for and behaviour of PreparedStatement complies with SQL and
     * JDBC standards.  Please read the introductory section
     * of the documentation for ${link JDBCParameterMetaData}.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param sql an SQL statement that may contain one or more '?' IN
     * parameter placeholders
     * @return a new default {@code PreparedStatement} object containing the
     * pre-compiled SQL statement
     * @throws SQLException if a database access error occurs
     * or this method is called on a closed connection
     */
    public synchronized PreparedStatement prepareStatement(
            String sql)
            throws SQLException {

        checkClosed();

        try {
            return new JDBCPreparedStatement(
                this,
                sql,
                JDBCResultSet.TYPE_FORWARD_ONLY,
                JDBCResultSet.CONCUR_READ_ONLY,
                rsHoldability,
                ResultConstants.RETURN_NO_GENERATED_KEYS,
                null,
                null);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Creates a {@code CallableStatement} object for calling
     * database stored procedures.
     * The {@code CallableStatement} object provides
     * methods for setting up its IN and OUT parameters, and
     * methods for executing the call to a stored procedure.
     *
     * <P><B>Note:</B> This method is optimized for handling stored
     * procedure call statements. Some drivers may send the call
     * statement to the database when the method {@code prepareCall}
     * is done; others
     * may wait until the {@code CallableStatement} object
     * is executed. This has no
     * direct effect on users; however, it does affect which method
     * throws certain SQLExceptions.
     * <P>
     * Result sets created using the returned {@code CallableStatement}
     * object will by default be type {@code TYPE_FORWARD_ONLY}
     * and have a concurrency level of {@code CONCUR_READ_ONLY}.
     * The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with 1.7.2, the support for and behaviour of
     * CallableStatement has changed.  Please read the introductory section
     * of the documentation for org.hsqldb.jdbc.JDBCCallableStatement.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param sql an SQL statement that may contain one or more '?'
     * parameter placeholders. Typically this statement is specified using JDBC
     * call escape syntax.
     * @return a new default {@code CallableStatement} object containing the
     * pre-compiled SQL statement
     * @throws SQLException if a database access error occurs
     * or this method is called on a closed connection
     */
    public synchronized CallableStatement prepareCall(
            String sql)
            throws SQLException {

        CallableStatement stmt;

        checkClosed();

        try {
            stmt = new JDBCCallableStatement(
                this,
                sql,
                JDBCResultSet.TYPE_FORWARD_ONLY,
                JDBCResultSet.CONCUR_READ_ONLY,
                rsHoldability);

            return stmt;
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Converts the given SQL statement into the system's native SQL grammar.
     * A driver may convert the JDBC SQL grammar into its system's
     * native SQL grammar prior to sending it. This method returns the
     * native form of the statement that the driver would have sent.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB converts the JDBC SQL
     * grammar into the system's native SQL grammar prior to sending
     * it, if escape processing is set true; this method returns the
     * native form of the statement that the driver would send in place
     * of client-specified JDBC SQL grammar. <p>
     *
     * Before 1.7.2, escape processing was incomplete and
     * also broken in terms of support for nested escapes. <p>
     *
     * Starting with 1.7.2, escape processing is complete and handles nesting
     * to arbitrary depth, but enforces a very strict interpretation of the
     * syntax and does not detect or process SQL comments. <p>
     *
     * In essence, the HSQLDB engine directly handles the prescribed syntax
     * and date / time formats specified internal to the JDBC escapes.
     * It also directly offers the XOpen / ODBC extended scalar
     * functions specified available internal to the {fn ...} JDBC escape.
     * As such, the driver simply removes the curly braces and JDBC escape
     * codes in the simplest and fastest fashion possible, by replacing them
     * with whitespace.
     *
     * But to avoid a great deal of complexity, certain forms of input
     * whitespace are currently not recognised.  For instance,
     * the driver handles "{?= call ...}" but not "{ ?= call ...} or
     * "{? = call ...}" <p>
     *
     * Also, comments embedded in SQL are currently not detected or
     * processed and thus may have unexpected effects on the output
     * of this method, for instance causing otherwise valid SQL to become
     * invalid. It is especially important to be aware of this because escape
     * processing is set true by default for Statement objects and is always
     * set true when producing a PreparedStatement from prepareStatement()
     * or CallableStatement from prepareCall().  Currently, it is simply
     * recommended to avoid submitting SQL having comments containing JDBC
     * escape sequence patterns and/or single or double quotation marks,
     * as this will avoid any potential problems.
     *
     * It is intended to implement a less strict handling of whitespace and
     * proper processing of SQL comments at some point in the near future.
     *
     * In any event, 1.7.2 now correctly processes the following JDBC escape
     * forms to arbitrary nesting depth, but only if the exact whitespace
     * layout described below is used:
     *
     * <ol>
     * <li>{call ...}
     * <li>{?= call ...}
     * <li>{fn ...}
     * <li>{oj ...}
     * <li>{d ...}
     * <li>{t ...}
     * <li>{ts ...}
     * </ol>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param sql an SQL statement that may contain one or more '?'
     * parameter placeholders
     * @return the native form of this statement
     * @throws SQLException if a database access error occurs
     * or this method is called on a closed connection
     */
    public synchronized String nativeSQL(final String sql) throws SQLException {

        checkClosed();

        if (sql == null || sql.isEmpty() || sql.indexOf('{') == -1) {
            return sql;
        }

        boolean       changed = false;
        int           state   = 0;
        int           len     = sql.length();
        int           nest    = 0;
        StringBuilder sb      = null;
        String        msg;

        //--
        final int outside_all                         = 0;
        final int outside_escape_inside_single_quotes = 1;
        final int outside_escape_inside_double_quotes = 2;

        //--
        final int inside_escape                      = 3;
        final int inside_escape_inside_single_quotes = 4;
        final int inside_escape_inside_double_quotes = 5;

        /* @todo */

        // final int inside_single_line_comment          = 6;
        // final int inside_multi_line_comment           = 7;
        // Better than old way for large inputs and for avoiding GC overhead;
        // toString() reuses internal char[], reducing memory requirement
        // and garbage items 3:2
        int tail = 0;

        for (int i = 0; i < len; i++) {
            char c = sql.charAt(i);

            switch (state) {

                case outside_all :                            // Not inside an escape or quotes
                    if (c == '\'') {
                        state = outside_escape_inside_single_quotes;
                    } else if (c == '"') {
                        state = outside_escape_inside_double_quotes;
                    } else if (c == '{') {
                        if (sb == null) {
                            sb = new StringBuilder(sql.length());
                        }

                        sb.append(sql, tail, i);

                        i       = onStartEscapeSequence(sql, sb, i);
                        tail    = i;
                        changed = true;

                        nest++;

                        state = inside_escape;
                    }

                    break;

                case outside_escape_inside_single_quotes :    // inside ' ' only
                case inside_escape_inside_single_quotes :     // inside { } and ' '
                    if (c == '\'') {
                        state -= 1;
                    }

                    break;

                case outside_escape_inside_double_quotes :    // inside " " only
                case inside_escape_inside_double_quotes :     // inside { } and " "
                    if (c == '"') {
                        state -= 2;
                    }

                    break;

                case inside_escape :                          // inside { }
                    if (c == '\'') {
                        state = inside_escape_inside_single_quotes;
                    } else if (c == '"') {
                        state = inside_escape_inside_double_quotes;
                    } else if (c == '}') {
                        sb.append(sql, tail, i);
                        sb.append(' ');

                        i++;

                        tail    = i;
                        changed = true;

                        nest--;

                        state = (nest == 0)
                                ? outside_all
                                : inside_escape;
                    } else if (c == '{') {
                        sb.append(sql, tail, i);

                        i       = onStartEscapeSequence(sql, sb, i);
                        tail    = i;
                        changed = true;

                        nest++;

                        state = inside_escape;
                    }

                    break;

                default :
            }
        }

        if (!changed) {
            return sql;
        }

        sb.append(sql.substring(tail));

        return sb.toString();
    }

    /*
     * @todo - semantics of autocommit regarding commit when the ResultSet is closed
     */

    /**
     * Sets this connection's auto-commit mode to the given state.
     * If a connection is in auto-commit mode, then all its SQL
     * statements will be executed and committed as individual
     * transactions.  Otherwise, its SQL statements are grouped into
     * transactions that are terminated by a call to either
     * the method {@code commit} or the method {@code rollback}.
     * By default, new connections are in auto-commit
     * mode.
     * <P>
     * The commit occurs when the statement completes. The time when the statement
     * completes depends on the type of SQL Statement:
     * <ul>
     * <li>For DML statements, such as Insert, Update or Delete, and DDL statements,
     * the statement is complete as soon as it has finished executing.
     * <li>For Select statements, the statement is complete when the associated result
     * set is closed.
     * <li>For {@code CallableStatement} objects or for statements that return
     * multiple results, the statement is complete
     * when all of the associated result sets have been closed, and all update
     * counts and output parameters have been retrieved.
     * </ul>
     * <P>
     * <B>NOTE:</B>  If this method is called during a transaction and the
     * auto-commit mode is changed, the transaction is committed.  If
     * {@code setAutoCommit} is called and the auto-commit mode is
     * not changed, the call is a no-op.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Up to and including HSQLDB 2.0,
     *
     * <ol>
     *   <li> All rows of a result set are retrieved internally <em>
     *   before</em> the first row can actually be fetched.<br>
     *   Therefore, a statement can be considered complete as soon as
     *   any XXXStatement.executeXXX method returns. </li>
     * </ol>
     * <p>
     *
     * Starting with 2.0, HSQLDB may not return a result set to the network
     * client as a whole; the generic documentation will apply. The fetch
     * size is taken into account
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param autoCommit {@code true} to enable auto-commit mode;
     *         {@code false} to disable it
     * @throws SQLException if a database access error occurs,
     *  setAutoCommit(true) is called while participating in a distributed transaction,
     * or this method is called on a closed connection
     * @see #getAutoCommit
     */
    public synchronized void setAutoCommit(
            boolean autoCommit)
            throws SQLException {

        checkClosed();

        try {
            sessionProxy.setAutoCommit(autoCommit);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Retrieves the current auto-commit mode for this {@code Connection}
     * object.
     *
     * @return the current state of this {@code Connection} object's
     *         auto-commit mode
     * @throws SQLException if a database access error occurs
     * or this method is called on a closed connection
     * @see #setAutoCommit
     */
    public synchronized boolean getAutoCommit() throws SQLException {

        checkClosed();

        try {
            return sessionProxy.isAutoCommit();
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Makes all changes made since the previous
     * commit/rollback permanent and releases any database locks
     * currently held by this {@code Connection} object.
     * This method should be
     * used only when auto-commit mode has been disabled.
     *
     * @throws SQLException if a database access error occurs,
     * this method is called while participating in a distributed transaction,
     * if this method is called on a closed connection or this
     *            {@code Connection} object is in auto-commit mode
     * @see #setAutoCommit
     */
    public synchronized void commit() throws SQLException {

        checkClosed();

        try {
            sessionProxy.commit(false);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Undoes all changes made in the current transaction
     * and releases any database locks currently held
     * by this {@code Connection} object. This method should be
     * used only when auto-commit mode has been disabled.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with HSQLDB 1.7.2, savepoints are fully supported both
     * in SQL and via the JDBC interface. <p>
     *
     * Using SQL, savepoints may be set, released and used in rollback
     * as follows:
     *
     * <pre>
     * SAVEPOINT &lt;savepoint-name&gt;
     * RELEASE SAVEPOINT &lt;savepoint-name&gt;
     * ROLLBACK TO SAVEPOINT &lt;savepoint-name&gt;
     * </pre>
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @throws SQLException if a database access error occurs,
     * this method is called while participating in a distributed transaction,
     * this method is called on a closed connection or this
     *            {@code Connection} object is in auto-commit mode
     * @see #setAutoCommit
     */
    public synchronized void rollback() throws SQLException {

        checkClosed();

        try {
            sessionProxy.rollback(false);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Releases this {@code Connection} object's database and JDBC resources
     * immediately instead of waiting for them to be automatically released.
     * <P>
     * Calling the method {@code close} on a {@code Connection}
     * object that is already closed is a no-op.
     * <P>
     * It is <b>strongly recommended</b> that an application explicitly
     * commits or rolls back an active transaction prior to calling the
     * {@code close} method.  If the {@code close} method is called
     * and there is an active transaction, the results are implementation-defined.
     *
     * @throws SQLException if a database access error occurs
     */
    public synchronized void close() throws SQLException {

        // Changed to synchronized above because
        // we would not want a sessionProxy.close()
        // operation to occur concurrently with a
        // statementXXX.executeXXX operation.
        if (isInternal || isClosed) {
            return;
        }

        isClosed       = true;
        rootWarning    = null;
        connProperties = null;

        if (isPooled) {
            if (poolEventListener != null) {
                poolEventListener.connectionClosed();

                poolEventListener = null;
            }
        } else if (sessionProxy != null) {
            sessionProxy.close();

            sessionProxy = null;
        }
    }

    /**
     * Retrieves whether this {@code Connection} object has been
     * closed.  A connection is closed if the method {@code close}
     * has been called on it or if certain fatal errors have occurred.
     * This method is guaranteed to return {@code true} only when
     * it is called after the method {@code Connection.close} has
     * been called.
     * <P>
     * This method generally cannot be called to determine whether a
     * connection to a database is valid or invalid.  A typical client
     * can determine that a connection is invalid by catching any
     * exceptions that might be thrown when an operation is attempted.
     *
     * @return {@code true} if this {@code Connection} object
     *         is closed; {@code false} if it is still open
     * @throws SQLException if a database access error occurs
     */
    public synchronized boolean isClosed() throws SQLException {
        return isClosed;
    }

    //======================================================================
    // Advanced features:

    /**
     * Retrieves a {@code DatabaseMetaData} object that contains
     * metadata about the database to which this
     * {@code Connection} object represents a connection.
     * The metadata includes information about the database's
     * tables, its supported SQL grammar, its stored
     * procedures, the capabilities of this connection, and so on.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.0 essentially supports full database metadata. <p>
     *
     * For discussion in greater detail, please follow the link to the
     * overview for JDBCDatabaseMetaData, below.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return a {@code DatabaseMetaData} object for this
     *         {@code Connection} object
     * @throws  SQLException if a database access error occurs
     * or this method is called on a closed connection
     */
    public synchronized DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();

        return new JDBCDatabaseMetaData(this);
    }

    /**
     * Puts this connection in read-only mode as a hint to the driver to enable
     * database optimizations.
     *
     * <P><B>Note:</B> This method cannot be called during a transaction.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.0 supports the SQL standard, which will not allow calls to
     * this method to succeed during a transaction.<p>
     *
     * Additionally, HSQLDB provides a way to put a whole database in
     * read-only mode. This is done by manually adding the line
     * 'readonly=true' to the database's .properties file while the
     * database is offline. Upon restart, all connections will be
     * readonly, since the entire database will be readonly. To take
     * a database out of readonly mode, simply take the database
     * offline and remove the line 'readonly=true' from the
     * database's .properties file. Upon restart, the database will
     * be in regular (read-write) mode. <p>
     *
     * When a database is put in readonly mode, its files are opened
     * in readonly mode, making it possible to create CD-based
     * readonly databases. To create a CD-based readonly database
     * that has CACHED tables and whose .data file is suspected of
     * being highly fragmented, it is recommended that the database
     * first be SHUTDOWN COMPACTed before copying the database
     * files to CD. This will reduce the space required and may
     * improve access times against the .data file which holds the
     * CACHED table data. <p>
     *
     * Starting with 1.7.2, an alternate approach to opimizing the
     * .data file before creating a CD-based readonly database is to issue
     * the CHECKPOINT DEFRAG command followed by SHUTDOWN to take the
     * database offline in preparation to burn the database files to CD.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param readOnly {@code true} enables read-only mode;
     *        {@code false} disables it
     * @throws SQLException if a database access error occurs, this
     *  method is called on a closed connection or this
     *            method is called during a transaction
     */
    public synchronized void setReadOnly(boolean readOnly) throws SQLException {

        checkClosed();

        try {
            sessionProxy.setReadOnlyDefault(readOnly);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Retrieves whether this {@code Connection}
     * object is in read-only mode.
     *
     * @return {@code true} if this {@code Connection} object
     *         is read-only; {@code false} otherwise
     * @throws SQLException if a database access error occurs
     * or this method is called on a closed connection
     */
    public synchronized boolean isReadOnly() throws SQLException {

        checkClosed();

        try {
            return sessionProxy.isReadOnlyDefault();
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Sets the given catalog name in order to select
     * a subspace of this {@code Connection} object's database
     * in which to work.
     * <P>
     * If the driver does not support catalogs, it will
     * silently ignore this request.
     * <p>
     * Calling {@code setCatalog} has no effect on previously created or prepared
     * {@code Statement} objects. It is implementation defined whether a DBMS
     * prepare operation takes place immediately when the {@code Connection}
     * method {@code prepareStatement} or {@code prepareCall} is invoked.
     * For maximum portability, {@code setCatalog} should be called before a
     * {@code Statement} is created or prepared.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports a single catalog per database. If the given catalog name
     * is not the same as the database catalog name, this method throws an
     * error.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param catalog the name of a catalog (subspace in this
     *        {@code Connection} object's database) in which to work
     * @throws SQLException if a database access error occurs
     * or this method is called on a closed connection
     * @see #getCatalog
     */
    public synchronized void setCatalog(String catalog) throws SQLException {

        checkClosed();

        try {
            sessionProxy.setAttribute(Attributes.INFO_CATALOG, catalog);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Retrieves this {@code Connection} object's current catalog name.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports a single catalog per database. This method
     * returns the catalog name for the current database
     * error.
     *
     * </div> <!-- end release-specific documentation -->
     *
     * @return the current catalog name or {@code null} if there is none
     * @throws SQLException if a database access error occurs
     * or this method is called on a closed connection
     * @see #setCatalog
     */
    public synchronized String getCatalog() throws SQLException {

        checkClosed();

        try {
            return (String) sessionProxy.getAttribute(Attributes.INFO_CATALOG);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     *  Attempts to change the transaction isolation level for this
     * {@code Connection} object to the one given.
     * The constants defined in the interface {@code Connection}
     * are the possible transaction isolation levels.
     * <P>
     * <B>Note:</B> If this method is called during a transaction, the result
     * is implementation-defined.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.0 accepts all isolation levels. {@code Connection.TRANSACTION_READ_UNCOMMITED}
     * is promoted to {@code Connection.TRANSACTION_READ_COMMITED}, but the transactions become read only.
     * Calling this method during a transaction always succeeds and the selected
     * isolation level is used from the next transaction.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param level one of the following {@code Connection} constants:
     *        {@code Connection.TRANSACTION_READ_UNCOMMITTED},
     *        {@code Connection.TRANSACTION_READ_COMMITTED},
     *        {@code Connection.TRANSACTION_REPEATABLE_READ}, or
     *        {@code Connection.TRANSACTION_SERIALIZABLE}.
     *        (Note that {@code Connection.TRANSACTION_NONE} cannot be used
     *        because it specifies that transactions are not supported.)
     * @throws SQLException if a database access error occurs, this
     * method is called on a closed connection
     *            or the given parameter is not one of the {@code Connection}
     *            constants
     * @see JDBCDatabaseMetaData#supportsTransactionIsolationLevel
     * @see #getTransactionIsolation
     */
    public synchronized void setTransactionIsolation(
            int level)
            throws SQLException {

        checkClosed();

        switch (level) {

            case TRANSACTION_READ_UNCOMMITTED :
            case TRANSACTION_READ_COMMITTED :
            case TRANSACTION_REPEATABLE_READ :
            case TRANSACTION_SERIALIZABLE :
                break;

            default :
                throw JDBCUtil.invalidArgument();
        }

        try {
            sessionProxy.setIsolationDefault(level);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Retrieves this {@code Connection} object's current
     * transaction isolation level.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.0 supports all isolation levels. {@code Connection.TRANSACTION_READ_UNCOMMITED}
     * is promoted to {@code Connection.TRANSACTION_READ_COMMITED}.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the current transaction isolation level, which will be one
     *         of the following constants:
     *        {@code Connection.TRANSACTION_READ_UNCOMMITTED},
     *        {@code Connection.TRANSACTION_READ_COMMITTED},
     *        {@code Connection.TRANSACTION_REPEATABLE_READ},
     *        {@code Connection.TRANSACTION_SERIALIZABLE}, or
     *        {@code Connection.TRANSACTION_NONE}.
     * @throws SQLException if a database access error occurs
     * or this method is called on a closed connection
     * @see #setTransactionIsolation
     */
    public synchronized int getTransactionIsolation() throws SQLException {

        checkClosed();

        try {
            return sessionProxy.getIsolation();
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Retrieves the first warning reported by calls on this
     * {@code Connection} object.  If there is more than one
     * warning, subsequent warnings will be chained to the first one
     * and can be retrieved by calling the method
     * {@code SQLWarning.getNextWarning} on the warning
     * that was retrieved previously.
     * <P>
     * This method may not be
     * called on a closed connection; doing so will cause an
     * {@code SQLException} to be thrown.
     *
     * <P><B>Note:</B> Subsequent warnings will be chained to this
     * SQLWarning.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB produces warnings whenever a createStatement(),
     * prepareStatement() or prepareCall() invocation requests an unsupported
     * but defined combination of result set type, concurrency and holdability,
     * such that another set is substituted.<p>
     * Other warnings are typically raised during the execution of data change
     * and query statements.<p>
     *
     * Only the warnings caused by the last operation on this connection are
     * returned by this method. A single operation may return up to 10 chained
     * warnings.
     *
     * </div>
     * <!-- end release-specific documentation -->
     * @return the first {@code SQLWarning} object or {@code null}
     *         if there are none
     * @throws SQLException if a database access error occurs or
     *            this method is called on a closed connection
     * @see java.sql.SQLWarning
     */
    public synchronized SQLWarning getWarnings() throws SQLException {
        checkClosed();

        return rootWarning;
    }

    /**
     * Clears all warnings reported for this {@code Connection} object.
     * After a call to this method, the method {@code getWarnings}
     * returns {@code null} until a new warning is
     * reported for this {@code Connection} object.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * The standard behaviour is implemented.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @throws SQLException if a database access error occurs
     * or this method is called on a closed connection
     */
    public synchronized void clearWarnings() throws SQLException {
        checkClosed();

        rootWarning = null;
    }

    //--------------------------JDBC 2.0-----------------------------

    /**
     * Creates a {@code Statement} object that will generate
     * {@code ResultSet} objects with the given type and concurrency.
     * This method is the same as the {@code createStatement} method
     * above, but it allows the default result set
     * type and concurrency to be overridden.
     * The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.0 adheres closely to SQL and JDBC standards. The
     * interpretation of of resultSetType and resultSetConcurrency has
     * changed in this version.<p>
     *
     * HSQLDB supports {@code TYPE_FORWARD_ONLY},
     * {@code TYPE_SCROLL_INSENSITIVE},
     * {@code CONCUR_READ_ONLY},
     * {@code CONCUR_UPDATABLE}
     * results. <p>
     *
     * If an unsupported combination is requested, a SQLWarning is issued on
     * this Connection and the closest supported combination is used instead.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param resultSetType a result set type; one of
     *        {@code ResultSet.TYPE_FORWARD_ONLY},
     *        {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
     *        {@code ResultSet.TYPE_SCROLL_SENSITIVE}
     * @param resultSetConcurrency a concurrency type; one of
     *        {@code ResultSet.CONCUR_READ_ONLY} or
     *        {@code ResultSet.CONCUR_UPDATABLE}
     * @return a new {@code Statement} object that will generate
     *         {@code ResultSet} objects with the given type and
     *         concurrency
     * @throws SQLException if a database access error occurs, this
     * method is called on a closed connection
     *         or the given parameters are not {@code ResultSet}
     *         constants indicating type and concurrency
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method or this method is not supported for the specified result
     * set type and result set concurrency.
     * @since JDK 1.2
     */
    public synchronized Statement createStatement(
            int resultSetType,
            int resultSetConcurrency)
            throws SQLException {

        checkClosed();

        int props = ResultProperties.getValueForJDBC(
            resultSetType,
            resultSetConcurrency,
            rsHoldability);

        return new JDBCStatement(this, props);
    }

    /**
     *
     * Creates a {@code PreparedStatement} object that will generate
     * {@code ResultSet} objects with the given type and concurrency.
     * This method is the same as the {@code prepareStatement} method
     * above, but it allows the default result set
     * type and concurrency to be overridden.
     * The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.0 adheres closely to SQL and JDBC standards. The
     * interpretation of of resultSetType and resultSetConcurrency has
     * changed in this version.<p>
     *
     * HSQLDB supports {@code TYPE_FORWARD_ONLY},
     * {@code TYPE_SCROLL_INSENSITIVE},
     * {@code CONCUR_READ_ONLY},
     * {@code CONCUR_UPDATABLE}
     * results. <p>
     *
     * If an unsupported combination is requested, a SQLWarning is issued on
     * this Connection and the closest supported combination is used instead.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param sql a {@code String} object that is the SQL statement to
     *            be sent to the database; may contain one or more '?' IN
     *            parameters
     * @param resultSetType a result set type; one of
     *         {@code ResultSet.TYPE_FORWARD_ONLY},
     *         {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
     *         {@code ResultSet.TYPE_SCROLL_SENSITIVE}
     * @param resultSetConcurrency a concurrency type; one of
     *         {@code ResultSet.CONCUR_READ_ONLY} or
     *         {@code ResultSet.CONCUR_UPDATABLE}
     * @return a new PreparedStatement object containing the
     * pre-compiled SQL statement that will produce {@code ResultSet}
     * objects with the given type and concurrency
     * @throws SQLException if a database access error occurs, this
     * method is called on a closed connection
     *         or the given parameters are not {@code ResultSet}
     *         constants indicating type and concurrency
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method or this method is not supported for the specified result
     * set type and result set concurrency.
     * @since JDK 1.2
     */
    public synchronized PreparedStatement prepareStatement(
            String sql,
            int resultSetType,
            int resultSetConcurrency)
            throws SQLException {

        checkClosed();

        try {
            return new JDBCPreparedStatement(
                this,
                sql,
                resultSetType,
                resultSetConcurrency,
                rsHoldability,
                ResultConstants.RETURN_NO_GENERATED_KEYS,
                null,
                null);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Creates a {@code CallableStatement} object that will generate
     * {@code ResultSet} objects with the given type and concurrency.
     * This method is the same as the {@code prepareCall} method
     * above, but it allows the default result set
     * type and concurrency to be overridden.
     * The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.0 adheres closely to SQL and JDBC standards. The
     * interpretation of of resultSetType and resultSetConcurrency has
     * changed in this version.<p>
     *
     * HSQLDB supports {@code TYPE_FORWARD_ONLY},
     * {@code TYPE_SCROLL_INSENSITIVE},
     * {@code CONCUR_READ_ONLY},
     * {@code CONCUR_UPDATABLE}
     * results. <p>
     *
     * If an unsupported combination is requested, a SQLWarning is issued on
     * this Connection and the closest supported combination is used instead.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param sql a {@code String} object that is the SQL statement to
     *            be sent to the database; may contain on or more '?' parameters
     * @param resultSetType a result set type; one of
     *         {@code ResultSet.TYPE_FORWARD_ONLY},
     *         {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
     *         {@code ResultSet.TYPE_SCROLL_SENSITIVE}
     * @param resultSetConcurrency a concurrency type; one of
     *         {@code ResultSet.CONCUR_READ_ONLY} or
     *         {@code ResultSet.CONCUR_UPDATABLE}
     * @return a new {@code CallableStatement} object containing the
     * pre-compiled SQL statement that will produce {@code ResultSet}
     * objects with the given type and concurrency
     * @throws SQLException if a database access error occurs, this method
     * is called on a closed connection
     *         or the given parameters are not {@code ResultSet}
     *         constants indicating type and concurrency
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method or this method is not supported for the specified result
     * set type and result set concurrency.
     * @since JDK 1.2
     */
    public synchronized CallableStatement prepareCall(
            String sql,
            int resultSetType,
            int resultSetConcurrency)
            throws SQLException {

        checkClosed();

        try {
            return new JDBCCallableStatement(
                this,
                sql,
                resultSetType,
                resultSetConcurrency,
                rsHoldability);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Retrieves the {@code Map} object associated with this
     * {@code Connection} object.
     * Unless the application has added an entry, the type map returned
     * will be empty.
     * <p>
     * You must invoke {@code setTypeMap} after making changes to the
     * {@code Map} object returned from
     *  {@code getTypeMap} as a JDBC driver may create an internal
     * copy of the {@code Map} object passed to {@code setTypeMap}:
     *
     * <pre>
     *      Map&lt;String,Class&lt;?&gt;&gt; myMap = con.getTypeMap();
     *      myMap.put("mySchemaName.ATHLETES", Athletes.class);
     *      con.setTypeMap(myMap);
     * </pre>
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * For compatibility, HSQLDB returns an empty map.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the {@code java.util.Map} object associated
     *         with this {@code Connection} object
     * @throws SQLException if a database access error occurs
     * or this method is called on a closed connection
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     * @see #setTypeMap
     */
    public synchronized Map<String, Class<?>> getTypeMap() throws SQLException {
        checkClosed();

        return new HashMap<>();
    }

    /**
     * Installs the given {@code TypeMap} object as the type map for
     * this {@code Connection} object.  The type map will be used for the
     * custom mapping of SQL structured types and distinct types.
     * <p>
     * You must set the values for the {@code TypeMap} prior to
     * calling {@code setMap} as a JDBC driver may create an internal copy
     * of the {@code TypeMap}:
     *
     * <pre>
     *      Map myMap&lt;String,Class&lt;?&gt;&gt; = new HashMap&lt;String,Class&lt;?&gt;&gt;();
     *      myMap.put("mySchemaName.ATHLETES", Athletes.class);
     *      con.setTypeMap(myMap);
     * </pre>]
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB does not yet support this feature. Calling this
     * method always throws a {@code SQLException}, stating that
     * the function is not supported.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param map the {@code java.util.Map} object to install
     *        as the replacement for this {@code Connection}
     *        object's default type map
     * @throws SQLException if a database access error occurs, this
     * method is called on a closed connection or
     *        the given parameter is not a {@code java.util.Map}
     *        object
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.2
     * @see #getTypeMap
     */
    public synchronized void setTypeMap(
            Map<String, Class<?>> map)
            throws SQLException {
        checkClosed();

        throw JDBCUtil.notSupported();
    }

    //--------------------------JDBC 3.0-----------------------------

    /**
     * Changes the default holdability of {@code ResultSet} objects
     * created using this {@code Connection} object to the given
     * holdability.  The default holdability of {@code ResultSet} objects
     * can be determined by invoking
     * {@link DatabaseMetaData#getResultSetHoldability}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB supports this feature.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param holdability a {@code ResultSet} holdability constant; one of
     *        {@code ResultSet.HOLD_CURSORS_OVER_COMMIT} or
     *        {@code ResultSet.CLOSE_CURSORS_AT_COMMIT}
     * @throws SQLException if a database access occurs, this method is called
     * on a closed connection, or the given parameter
     *         is not a {@code ResultSet} constant indicating holdability
     * @throws SQLFeatureNotSupportedException if the given holdability is not supported
     * @see #getHoldability
     * @see DatabaseMetaData#getResultSetHoldability
     * @see JDBCResultSet
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public synchronized void setHoldability(
            int holdability)
            throws SQLException {

        checkClosed();

        switch (holdability) {

            case JDBCResultSet.HOLD_CURSORS_OVER_COMMIT :
            case JDBCResultSet.CLOSE_CURSORS_AT_COMMIT :
                break;

            default :
                throw JDBCUtil.invalidArgument();
        }

        rsHoldability = holdability;
    }

    /**
     * Retrieves the current holdability of {@code ResultSet} objects
     * created using this {@code Connection} object.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB returns the current holdability.<p>
     *
     * The default is HOLD_CURSORS_OVER_COMMIT.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the holdability, one of
     *        {@code ResultSet.HOLD_CURSORS_OVER_COMMIT} or
     *        {@code ResultSet.CLOSE_CURSORS_AT_COMMIT}
     * @throws SQLException if a database access error occurs
     * or this method is called on a closed connection
     * @see #setHoldability
     * @see DatabaseMetaData#getResultSetHoldability
     * @see JDBCResultSet
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public synchronized int getHoldability() throws SQLException {
        checkClosed();

        return rsHoldability;
    }

    /**
     *
     * Creates an unnamed savepoint in the current transaction and
     * returns the new {@code Savepoint} object that represents it.
     *
     * <p> if setSavepoint is invoked outside of an active transaction, a transaction will be started at this newly created
     * savepoint.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * From 2.0, HSQLDB supports this feature. <p>
     *
     * Note: Unnamed savepoints are not part of the SQL:2003 standard.
     * Use setSavepoint(String name) instead.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @return the new {@code Savepoint} object
     * @throws SQLException if a database access error occurs,
     * this method is called while participating in a distributed transaction,
     * this method is called on a closed connection
     *            or this {@code Connection} object is currently in
     *            auto-commit mode
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see JDBCSavepoint
     * @see java.sql.Savepoint
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public synchronized Savepoint setSavepoint() throws SQLException {

        checkClosed();

        if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4 && getAutoCommit()) {
            throw JDBCUtil.sqlException(ErrorCode.X_3B001);
        }

        JDBCSavepoint savepoint = new JDBCSavepoint(this);

        try {
            sessionProxy.savepoint(savepoint.name);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }

        return savepoint;
    }

    /**
     * Creates a savepoint with the given name in the current transaction
     * and returns the new {@code Savepoint} object that represents it.
     *
     * <p> if setSavepoint is invoked outside of an active transaction, a transaction will be started at this newly created
     * savepoint.
     *
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Previous to JDBC 4, if the connection is autoCommit,
     * setting a savepoint has no effect, as it is cleared upon the execution
     * of the next transactional statement. When built for JDBC 4, this method
     * throws an SQLException when this {@code Connection} object is currently
     * in auto-commit mode, as per the JDBC 4 standard.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param name a {@code String} containing the name of the savepoint
     * @return the new {@code Savepoint} object
     * @throws SQLException if a database access error occurs,
     * this method is called while participating in a distributed transaction,
     * this method is called on a closed connection
     *            or this {@code Connection} object is currently in
     *            auto-commit mode
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see JDBCSavepoint
     * @see java.sql.Savepoint
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public synchronized Savepoint setSavepoint(
            String name)
            throws SQLException {

        checkClosed();

        if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4 && getAutoCommit()) {
            throw JDBCUtil.sqlException(ErrorCode.X_3B001);
        }

        if (name == null) {
            throw JDBCUtil.nullArgument();
        }

        if (name.startsWith("SYSTEM_SAVEPOINT_")) {
            throw JDBCUtil.invalidArgument();
        }

        try {
            sessionProxy.savepoint(name);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }

        return new JDBCSavepoint(name, this);
    }

    /**
     * Undoes all changes made after the given {@code Savepoint} object
     * was set.
     * <P>
     * This method should be used only when auto-commit has been disabled.
     *
     * @param savepoint the {@code Savepoint} object to roll back to
     * @throws SQLException if a database access error occurs,
     * this method is called while participating in a distributed transaction,
     * this method is called on a closed connection,
     *            the {@code Savepoint} object is no longer valid,
     *            or this {@code Connection} object is currently in
     *            auto-commit mode
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see JDBCSavepoint
     * @see java.sql.Savepoint
     * @see #rollback
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public synchronized void rollback(Savepoint savepoint) throws SQLException {

        JDBCSavepoint sp;

        checkClosed();

        if (savepoint == null) {
            throw JDBCUtil.nullArgument();
        }

        if (!(savepoint instanceof JDBCSavepoint)) {
            throw JDBCUtil.invalidArgument(ErrorCode.X_3B001);
        }

        sp = (JDBCSavepoint) savepoint;

        if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4 && sp.name == null) {
            throw JDBCUtil.invalidArgument(ErrorCode.X_3B001);
        }

        if (this != sp.connection) {
            throw JDBCUtil.invalidArgument(ErrorCode.X_3B001);
        }

        if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4 && getAutoCommit()) {
            sp.name       = null;
            sp.connection = null;

            throw JDBCUtil.sqlException(ErrorCode.X_3B001);
        }

        try {
            sessionProxy.rollbackToSavepoint(sp.name);

            if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4) {
                sp.connection = null;
                sp.name       = null;
            }
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Removes the specified {@code Savepoint}  and subsequent {@code Savepoint} objects from the current
     * transaction. Any reference to the savepoint after it have been removed
     * will cause an {@code SQLException} to be thrown.
     *
     * @param savepoint the {@code Savepoint} object to be removed
     * @throws SQLException if a database access error occurs, this
     *  method is called on a closed connection or
     *            the given {@code Savepoint} object is not a valid
     *            savepoint in the current transaction
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see JDBCSavepoint
     * @see java.sql.Savepoint
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public synchronized void releaseSavepoint(
            Savepoint savepoint)
            throws SQLException {

        JDBCSavepoint sp;
        Result        req;

        checkClosed();

        if (savepoint == null) {
            throw JDBCUtil.nullArgument();
        }

        if (!(savepoint instanceof JDBCSavepoint)) {
            throw JDBCUtil.invalidArgument(ErrorCode.X_3B001);
        }

        sp = (JDBCSavepoint) savepoint;

        if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4 && sp.name == null) {
            throw JDBCUtil.invalidArgument(ErrorCode.X_3B001);
        }

        if (this != sp.connection) {
            throw JDBCUtil.invalidArgument(ErrorCode.X_3B001);
        }

        if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4 && getAutoCommit()) {
            sp.name       = null;
            sp.connection = null;

            throw JDBCUtil.invalidArgument(ErrorCode.X_3B001);
        }

        try {
            sessionProxy.releaseSavepoint(sp.name);

            if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4) {
                sp.connection = null;
                sp.name       = null;
            }
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Creates a {@code Statement} object that will generate
     * {@code ResultSet} objects with the given type, concurrency,
     * and holdability.
     * This method is the same as the {@code createStatement} method
     * above, but it allows the default result set
     * type, concurrency, and holdability to be overridden.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.0 adheres closely to SQL and JDBC standards. The
     * interpretation of of resultSetType and resultSetConcurrency has
     * changed in this version.<p>
     *
     * HSQLDB supports {@code TYPE_FORWARD_ONLY},
     * {@code TYPE_SCROLL_INSENSITIVE},
     * {@code CONCUR_READ_ONLY},
     * {@code CONCUR_UPDATABLE}
     * results. <p>
     *
     * If an unsupported combination is requested, a SQLWarning is issued on
     * this Connection and the closest supported combination is used instead.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param resultSetType one of the following {@code ResultSet}
     *        constants:
     *         {@code ResultSet.TYPE_FORWARD_ONLY},
     *         {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
     *         {@code ResultSet.TYPE_SCROLL_SENSITIVE}
     * @param resultSetConcurrency one of the following {@code ResultSet}
     *        constants:
     *         {@code ResultSet.CONCUR_READ_ONLY} or
     *         {@code ResultSet.CONCUR_UPDATABLE}
     * @param resultSetHoldability one of the following {@code ResultSet}
     *        constants:
     *         {@code ResultSet.HOLD_CURSORS_OVER_COMMIT} or
     *         {@code ResultSet.CLOSE_CURSORS_AT_COMMIT}
     * @return a new {@code Statement} object that will generate
     *         {@code ResultSet} objects with the given type,
     *         concurrency, and holdability
     * @throws SQLException if a database access error occurs, this
     * method is called on a closed connection
     *            or the given parameters are not {@code ResultSet}
     *            constants indicating type, concurrency, and holdability
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method or this method is not supported for the specified result
     * set type, result set holdability and result set concurrency.
     * @see JDBCResultSet
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public synchronized Statement createStatement(
            int resultSetType,
            int resultSetConcurrency,
            int resultSetHoldability)
            throws SQLException {

        checkClosed();

        int props = ResultProperties.getValueForJDBC(
            resultSetType,
            resultSetConcurrency,
            resultSetHoldability);

        return new JDBCStatement(this, props);
    }

    /**
     * Creates a {@code PreparedStatement} object that will generate
     * {@code ResultSet} objects with the given type, concurrency,
     * and holdability.
     * <P>
     * This method is the same as the {@code prepareStatement} method
     * above, but it allows the default result set
     * type, concurrency, and holdability to be overridden.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.0 adheres closely to SQL and JDBC standards. The
     * interpretation of of resultSetType and resultSetConcurrency has
     * changed in this version.<p>
     *
     * HSQLDB supports {@code TYPE_FORWARD_ONLY},
     * {@code TYPE_SCROLL_INSENSITIVE},
     * {@code CONCUR_READ_ONLY},
     * {@code CONCUR_UPDATABLE}
     * results.<p>
     * {@code HOLD_CURSORS_OVER_COMMIT} is supported only when
     * {@code CONCUR_READ_ONLY} is requested.<p>
     *
     *
     * If an unsupported combination is requested, a SQLWarning is issued on
     * this Connection and the closest supported combination is used instead.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param sql a {@code String} object that is the SQL statement to
     *            be sent to the database; may contain one or more '?' IN
     *            parameters
     * @param resultSetType one of the following {@code ResultSet}
     *        constants:
     *         {@code ResultSet.TYPE_FORWARD_ONLY},
     *         {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
     *         {@code ResultSet.TYPE_SCROLL_SENSITIVE}
     * @param resultSetConcurrency one of the following {@code ResultSet}
     *        constants:
     *         {@code ResultSet.CONCUR_READ_ONLY} or
     *         {@code ResultSet.CONCUR_UPDATABLE}
     * @param resultSetHoldability one of the following {@code ResultSet}
     *        constants:
     *         {@code ResultSet.HOLD_CURSORS_OVER_COMMIT} or
     *         {@code ResultSet.CLOSE_CURSORS_AT_COMMIT}
     * @return a new {@code PreparedStatement} object, containing the
     *         pre-compiled SQL statement, that will generate
     *         {@code ResultSet} objects with the given type,
     *         concurrency, and holdability
     * @throws SQLException if a database access error occurs, this
     * method is called on a closed connection
     *            or the given parameters are not {@code ResultSet}
     *            constants indicating type, concurrency, and holdability
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method or this method is not supported for the specified result
     * set type, result set holdability and result set concurrency.
     * @see JDBCResultSet
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public synchronized PreparedStatement prepareStatement(
            String sql,
            int resultSetType,
            int resultSetConcurrency,
            int resultSetHoldability)
            throws SQLException {

        checkClosed();

        try {
            return new JDBCPreparedStatement(
                this,
                sql,
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability,
                ResultConstants.RETURN_NO_GENERATED_KEYS,
                null,
                null);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Creates a {@code CallableStatement} object that will generate
     * {@code ResultSet} objects with the given type and concurrency.
     * This method is the same as the {@code prepareCall} method
     * above, but it allows the default result set
     * type, result set concurrency type and holdability to be overridden.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.0 adheres closely to SQL and JDBC standards. The
     * interpretation of of resultSetType and resultSetConcurrency has
     * changed in this version.<p>
     *
     * HSQLDB supports {@code TYPE_FORWARD_ONLY},
     * {@code TYPE_SCROLL_INSENSITIVE},
     * {@code CONCUR_READ_ONLY},
     * {@code CONCUR_UPDATABLE}
     * results. <p>
     *
     * If an unsupported combination is requested, a SQLWarning is issued on
     * this Connection and the closest supported combination is used instead.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param sql a {@code String} object that is the SQL statement to
     *            be sent to the database; may contain on or more '?' parameters
     * @param resultSetType one of the following {@code ResultSet}
     *        constants:
     *         {@code ResultSet.TYPE_FORWARD_ONLY},
     *         {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
     *         {@code ResultSet.TYPE_SCROLL_SENSITIVE}
     * @param resultSetConcurrency one of the following {@code ResultSet}
     *        constants:
     *         {@code ResultSet.CONCUR_READ_ONLY} or
     *         {@code ResultSet.CONCUR_UPDATABLE}
     * @param resultSetHoldability one of the following {@code ResultSet}
     *        constants:
     *         {@code ResultSet.HOLD_CURSORS_OVER_COMMIT} or
     *         {@code ResultSet.CLOSE_CURSORS_AT_COMMIT}
     * @return a new {@code CallableStatement} object, containing the
     *         pre-compiled SQL statement, that will generate
     *         {@code ResultSet} objects with the given type,
     *         concurrency, and holdability
     * @throws SQLException if a database access error occurs, this
     * method is called on a closed connection
     *            or the given parameters are not {@code ResultSet}
     *            constants indicating type, concurrency, and holdability
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method or this method is not supported for the specified result
     * set type, result set holdability and result set concurrency.
     * @see JDBCResultSet
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public synchronized CallableStatement prepareCall(
            String sql,
            int resultSetType,
            int resultSetConcurrency,
            int resultSetHoldability)
            throws SQLException {

        checkClosed();

        try {
            return new JDBCCallableStatement(
                this,
                sql,
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Creates a default {@code PreparedStatement} object that has
     * the capability to retrieve auto-generated keys. The given constant
     * tells the driver whether it should make auto-generated keys
     * available for retrieval.  This parameter is ignored if the SQL statement
     * is not an {@code INSERT} statement, or an SQL statement able to return
     * auto-generated keys (the list of such statements is vendor-specific).
     * <P>
     * <B>Note:</B> This method is optimized for handling
     * parametric SQL statements that benefit from precompilation. If
     * the driver supports precompilation,
     * the method {@code prepareStatement} will send
     * the statement to the database for precompilation. Some drivers
     * may not support precompilation. In this case, the statement may
     * not be sent to the database until the {@code PreparedStatement}
     * object is executed.  This has no direct effect on users; however, it does
     * affect which methods throw certain SQLExceptions.
     * <P>
     * Result sets created using the returned {@code PreparedStatement}
     * object will by default be type {@code TYPE_FORWARD_ONLY}
     * and have a concurrency level of {@code CONCUR_READ_ONLY}.
     * The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with version 2.0, HSQLDB supports returning generated columns
     * with single-row and multi-row INSERT, UPDATE and MERGE statements. <p>
     * If the table has an IDENTITY or GENERATED column(s) the values for these
     * columns are returned in the next call to getGeneratedKeys() after each
     * execution of the PreparedStatement.<p>
     *
     * HSQLDB also supports returning primary key values from he rows by using the
     * {@code org.hsqldb.jdbc.JDBCStatement.RETURN_PRIMARY_KEYS} constant.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param sql an SQL statement that may contain one or more '?' IN
     *        parameter placeholders
     * @param autoGeneratedKeys a flag indicating whether auto-generated keys
     *        should be returned; one of
     *        {@code Statement.RETURN_GENERATED_KEYS} or
     *        {@code Statement.NO_GENERATED_KEYS}
     * @return a new {@code PreparedStatement} object, containing the
     *         pre-compiled SQL statement, that will have the capability of
     *         returning auto-generated keys
     * @throws SQLException if a database access error occurs, this
     *  method is called on a closed connection
     *         or the given parameter is not a {@code Statement}
     *         constant indicating whether auto-generated keys should be
     *         returned
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method with a constant of Statement.RETURN_GENERATED_KEYS
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public synchronized PreparedStatement prepareStatement(
            String sql,
            int autoGeneratedKeys)
            throws SQLException {

        checkClosed();

        try {
            if (autoGeneratedKeys != Statement.RETURN_GENERATED_KEYS
                    && autoGeneratedKeys != Statement.NO_GENERATED_KEYS
                    && autoGeneratedKeys != JDBCStatement.RETURN_PRIMARY_KEYS) {
                throw JDBCUtil.invalidArgument("autoGeneratedKeys");
            }

            return new JDBCPreparedStatement(
                this,
                sql,
                JDBCResultSet.TYPE_FORWARD_ONLY,
                JDBCResultSet.CONCUR_READ_ONLY,
                rsHoldability,
                autoGeneratedKeys,
                null,
                null);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Creates a default {@code PreparedStatement} object capable
     * of returning the auto-generated keys designated by the given array.
     * This array contains the indexes of the columns in the target
     * table that contain the auto-generated keys that should be made
     * available.  The driver will ignore the array if the SQL statement
     * is not an {@code INSERT} statement, or an SQL statement able to return
     * auto-generated keys (the list of such statements is vendor-specific).
     * <p>
     * An SQL statement with or without IN parameters can be
     * pre-compiled and stored in a {@code PreparedStatement} object. This
     * object can then be used to efficiently execute this statement
     * multiple times.
     * <P>
     * <B>Note:</B> This method is optimized for handling
     * parametric SQL statements that benefit from precompilation. If
     * the driver supports precompilation,
     * the method {@code prepareStatement} will send
     * the statement to the database for precompilation. Some drivers
     * may not support precompilation. In this case, the statement may
     * not be sent to the database until the {@code PreparedStatement}
     * object is executed.  This has no direct effect on users; however, it does
     * affect which methods throw certain SQLExceptions.
     * <P>
     * Result sets created using the returned {@code PreparedStatement}
     * object will by default be type {@code TYPE_FORWARD_ONLY}
     * and have a concurrency level of {@code CONCUR_READ_ONLY}.
     * The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with version 2.0, HSQLDB supports returning generated columns
     * with single-row and multi-row INSERT, UPDATE and MERGE statements. <p>
     * The columnIndexes may specify any set of columns of the table.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param sql an SQL statement that may contain one or more '?' IN
     *        parameter placeholders
     * @param columnIndexes an array of column indexes indicating the columns
     *        that should be returned from the inserted row or rows
     * @return a new {@code PreparedStatement} object, containing the
     *         pre-compiled statement, that is capable of returning the
     *         auto-generated keys designated by the given array of column
     *         indexes
     * @throws SQLException if a database access error occurs
     * or this method is called on a closed connection
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public synchronized PreparedStatement prepareStatement(
            String sql,
            int[] columnIndexes)
            throws SQLException {

        checkClosed();

        try {
            return new JDBCPreparedStatement(
                this,
                sql,
                JDBCResultSet.TYPE_FORWARD_ONLY,
                JDBCResultSet.CONCUR_READ_ONLY,
                rsHoldability,
                ResultConstants.RETURN_GENERATED_KEYS_COL_INDEXES,
                columnIndexes,
                null);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Creates a default {@code PreparedStatement} object capable
     * of returning the auto-generated keys designated by the given array.
     * This array contains the names of the columns in the target
     * table that contain the auto-generated keys that should be returned.
     * The driver will ignore the array if the SQL statement
     * is not an {@code INSERT} statement, or an SQL statement able to return
     * auto-generated keys (the list of such statements is vendor-specific).
     * <P>
     * An SQL statement with or without IN parameters can be
     * pre-compiled and stored in a {@code PreparedStatement} object. This
     * object can then be used to efficiently execute this statement
     * multiple times.
     * <P>
     * <B>Note:</B> This method is optimized for handling
     * parametric SQL statements that benefit from precompilation. If
     * the driver supports precompilation,
     * the method {@code prepareStatement} will send
     * the statement to the database for precompilation. Some drivers
     * may not support precompilation. In this case, the statement may
     * not be sent to the database until the {@code PreparedStatement}
     * object is executed.  This has no direct effect on users; however, it does
     * affect which methods throw certain SQLExceptions.
     * <P>
     * Result sets created using the returned {@code PreparedStatement}
     * object will by default be type {@code TYPE_FORWARD_ONLY}
     * and have a concurrency level of {@code CONCUR_READ_ONLY}.
     * The holdability of the created result sets can be determined by
     * calling {@link #getHoldability}.
     *
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * Starting with version 2.0, HSQLDB supports returning generated columns
     * with single-row and multi-row INSERT, UPDATE and MERGE statements. <p>
     * The columnNames may specify any set of columns of the table. The names
     * are case-sensitive, unlike column names in ResultSet methods.
     *
     * </div>
     *
     * @param sql an SQL statement that may contain one or more '?' IN
     *        parameter placeholders
     * @param columnNames an array of column names indicating the columns
     *        that should be returned from the inserted row or rows
     * @return a new {@code PreparedStatement} object, containing the
     *         pre-compiled statement, that is capable of returning the
     *         auto-generated keys designated by the given array of column
     *         names
     * @throws SQLException if a database access error occurs
     * or this method is called on a closed connection
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since JDK 1.4, HSQLDB 1.7.2
     */
    public synchronized PreparedStatement prepareStatement(
            String sql,
            String[] columnNames)
            throws SQLException {

        checkClosed();

        try {
            return new JDBCPreparedStatement(
                this,
                sql,
                JDBCResultSet.TYPE_FORWARD_ONLY,
                JDBCResultSet.CONCUR_READ_ONLY,
                rsHoldability,
                ResultConstants.RETURN_GENERATED_KEYS_COL_NAMES,
                null,
                columnNames);
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    //------------------------- JDBC 4.0 -----------------------------------

    /**
     * Constructs an object that implements the {@code Clob} interface. The object
     * returned initially contains no data.  The {@code setAsciiStream},
     * {@code setCharacterStream} and {@code setString} methods of
     * the {@code Clob} interface may be used to add data to the {@code Clob}.
     * @return An object that implements the {@code Clob} interface
     * @throws SQLException if an object that implements the
     * {@code Clob} interface can not be constructed, this method is
     * called on a closed connection or a database access error occurs.
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this data type
     *
     * @since JDK 1.6, HSQLDB 2.0
     */
    public Clob createClob() throws SQLException {
        checkClosed();

        return new JDBCClob();
    }

    /**
     * Constructs an object that implements the {@code Blob} interface. The object
     * returned initially contains no data.  The {@code setBinaryStream} and
     * {@code setBytes} methods of the {@code Blob} interface may be used to add data to
     * the {@code Blob}.
     * @return  An object that implements the {@code Blob} interface
     * @throws SQLException if an object that implements the
     * {@code Blob} interface can not be constructed, this method is
     * called on a closed connection or a database access error occurs.
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this data type
     *
     * @since JDK 1.6, HSQLDB 2.0
     */
    public Blob createBlob() throws SQLException {
        checkClosed();

        return new JDBCBlob();
    }

    /**
     * Constructs an object that implements the {@code NClob} interface. The object
     * returned initially contains no data.  The {@code setAsciiStream},
     * {@code setCharacterStream} and {@code setString} methods of the {@code NClob} interface may
     * be used to add data to the {@code NClob}.
     * @return An object that implements the {@code NClob} interface
     * @throws SQLException if an object that implements the
     * {@code NClob} interface can not be constructed, this method is
     * called on a closed connection or a database access error occurs.
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this data type
     *
     * @since JDK 1.6, HSQLDB 2.0
     */
    public NClob createNClob() throws SQLException {
        checkClosed();

        return new JDBCNClob();
    }

    /**
     * Constructs an object that implements the {@code SQLXML} interface. The object
     * returned initially contains no data. The {@code createXmlStreamWriter} object and
     * {@code setString} method of the {@code SQLXML} interface may be used to add data to the {@code SQLXML}
     * object.
     * @return An object that implements the {@code SQLXML} interface
     * @throws SQLException if an object that implements the {@code SQLXML} interface can not
     * be constructed, this method is
     * called on a closed connection or a database access error occurs.
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this data type
     * @since JDK 1.6, HSQLDB 2.0
     */
    public SQLXML createSQLXML() throws SQLException {
        checkClosed();

        return new JDBCSQLXML();
    }

    /**
     * Returns true if the connection has not been closed and is still valid.
     * The driver shall submit a query on the connection or use some other
     * mechanism that positively verifies the connection is still valid when
     * this method is called.
     * <p>
     * The query submitted by the driver to validate the connection shall be
     * executed in the context of the current transaction.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB uses a maximum timeout of 60 seconds if timeout has been specified
     * as zero.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param timeout The time in seconds to wait for the database operation
     *                used to validate the connection to complete.  If the
     *                timeout period expires before the operation completes,
     *                this method returns false.  A value of 0 indicates a
     *                timeout is not applied to the database operation.
     *
     * @return true if the connection is valid, false otherwise
     * @throws SQLException if the value supplied for {@code timeout}
     * is less than 0
     * @since JDK 1.6, HSQLDB 2.0
     *
     * @see JDBCDatabaseMetaData#getClientInfoProperties
     */
    public boolean isValid(int timeout) throws SQLException {

        if (timeout < 0) {
            throw JDBCUtil.outOfRangeArgument("timeout: " + timeout);
        }

        if (this.isInternal) {
            return true;
        } else if (!this.isNetConn) {
            return !this.isClosed();
        } else if (this.isClosed()) {
            return false;
        }

        final boolean[] flag = new boolean[]{ true };
        Thread          t    = new Thread() {

            public void run() {

                try {
                    getMetaData().getDatabaseMajorVersion();
                } catch (Throwable e) {
                    flag[0] = false;
                }
            }
        };

        if (timeout > 60) {
            timeout = 60;
        }

        // Remember:  param is in *seconds*
        timeout *= 1000;

        try {
            t.start();

            final long start = System.currentTimeMillis();

            t.join(timeout);

            try {
                t.setContextClassLoader(null);
            } catch (Throwable th) {}

            if (timeout == 0) {
                return flag[0];
            }

            return flag[0] && (System.currentTimeMillis() - start) < timeout;
        } catch (Throwable e) {
            return false;
        }
    }

    /**
     * Sets the value of the client info property specified by name to the
     * value specified by value.
     * <p>
     * Applications may use the {@code DatabaseMetaData.getClientInfoProperties}
     * method to determine the client info properties supported by the driver
     * and the maximum length that may be specified for each property.
     * <p>
     * The driver stores the value specified in a suitable location in the
     * database.  For example in a special register, session parameter, or
     * system table column.  For efficiency the driver may defer setting the
     * value in the database until the next time a statement is executed or
     * prepared.  Other than storing the client information in the appropriate
     * place in the database, these methods shall not alter the behavior of
     * the connection in any way.  The values supplied to these methods are
     * used for accounting, diagnostics and debugging purposes only.
     * <p>
     * The driver shall generate a warning if the client info name specified
     * is not recognized by the driver.
     * <p>
     * If the value specified to this method is greater than the maximum
     * length for the property the driver may either truncate the value and
     * generate a warning or generate a {@code SQLClientInfoException}.  If the driver
     * generates a {@code SQLClientInfoException}, the value specified was not set on the
     * connection.
     * <p>
     * The following are standard client info properties.  Drivers are not
     * required to support these properties however if the driver supports a
     * client info property that can be described by one of the standard
     * properties, the standard property name should be used.
     *
     * <ul>
     * <li>ApplicationName  -       The name of the application currently utilizing
     *                                                  the connection</li>
     * <li>ClientUser           -       The name of the user that the application using
     *                                                  the connection is performing work for.  This may
     *                                                  not be the same as the user name that was used
     *                                                  in establishing the connection.</li>
     * <li>ClientHostname   -       The hostname of the computer the application
     *                                                  using the connection is running on.</li>
     * </ul>
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB 2.0, throws an SQLClientInfoException when this method is
     * called.
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param name          The name of the client info property to set
     * @param value         The value to set the client info property to.  If the
     *                                      value is null, the current value of the specified
     *                                      property is cleared.
         *
     * @throws      SQLClientInfoException if the database server returns an error while
     *                      setting the client info value on the database server or this method
     * is called on a closed connection
     *
     * @since JDK 1.6, HSQLDB 2.0
     */
    public void setClientInfo(
            String name,
            String value)
            throws SQLClientInfoException {

        SQLClientInfoException ex = new SQLClientInfoException();

        ex.initCause(JDBCUtil.notSupported());

        throw ex;
    }

    /**
     * Sets the value of the connection's client info properties.  The
     * {@code Properties} object contains the names and values of the client info
     * properties to be set.  The set of client info properties contained in
     * the properties list replaces the current set of client info properties
     * on the connection.  If a property that is currently set on the
     * connection is not present in the properties list, that property is
     * cleared.  Specifying an empty properties list will clear all of the
     * properties on the connection.  See {@code setClientInfo (String, String)} for
     * more information.
     * <p>
     * If an error occurs in setting any of the client info properties, a
     * {@code SQLClientInfoException} is thrown. The {@code SQLClientInfoException}
     * contains information indicating which client info properties were not set.
     * The state of the client information is unknown because
     * some databases do not allow multiple client info properties to be set
     * atomically.  For those databases, one or more properties may have been
     * set before the error occurred.
     *
     *
     * @param properties                the list of client info properties to set
     *
     * @see java.sql.Connection#setClientInfo(String, String) setClientInfo(String, String)
     * @since JDK 1.6, HSQLDB 2.0
     *
     * @throws SQLClientInfoException if the database server returns an error while
     *                  setting the clientInfo values on the database server or this method
     * is called on a closed connection
     *
     */
    public void setClientInfo(
            Properties properties)
            throws SQLClientInfoException {

        if (!this.isClosed && (properties == null || properties.isEmpty())) {
            return;
        }

        SQLClientInfoException ex = new SQLClientInfoException();

        if (this.isClosed) {
            ex.initCause(JDBCUtil.connectionClosedException());
        } else {
            ex.initCause(JDBCUtil.notSupported());
        }

        throw ex;
    }

    /* @todo 1.9.0 */

    /**
     * Returns the value of the client info property specified by name.  This
     * method may return null if the specified client info property has not
     * been set and does not have a default value.  This method will also
     * return null if the specified client info property name is not supported
     * by the driver.
     * <p>
     * Applications may use the {@code DatabaseMetaData.getClientInfoProperties}
     * method to determine the client info properties supported by the driver.
     *
     * @param name              The name of the client info property to retrieve
     *
     * @return                  The value of the client info property specified
     *
     * @throws SQLException             if the database server returns an error when
     *                                                  fetching the client info value from the database
     * or this method is called on a closed connection
     *
     * @since JDK 1.6, HSQLDB 2.0
     *
     * @see java.sql.DatabaseMetaData#getClientInfoProperties
     */
    public String getClientInfo(String name) throws SQLException {
        checkClosed();

        return null;
    }

    /* @todo - 1.9 */

    /**
     * Returns a list containing the name and current value of each client info
     * property supported by the driver.  The value of a client info property
     * may be null if the property has not been set and does not have a
     * default value.
         *
     * @return  A {@code Properties} object that contains the name and current value of
     *                  each of the client info properties supported by the driver.
         *
     * @throws  SQLException if the database server returns an error when
     *                  fetching the client info values from the database
     * or this method is called on a closed connection
     *
     * @since JDK 1.6, HSQLDB 2.0
     */
    public Properties getClientInfo() throws SQLException {
        checkClosed();

        return null;
    }

    /**
     *  Factory method for creating Array objects.
     * <p>
     *  <b>Note: </b>When {@code createArrayOf} is used to create an array object
     *  that maps to a primitive data type, then it is implementation-defined
     *  whether the {@code Array} object is an array of that primitive
     *  data type or an array of {@code Object}.
     *  <p>
     *  <b>Note: </b>The JDBC driver is responsible for mapping the elements
     *  {@code Object} array to the default JDBC SQL type defined in
     *  java.sql.Types for the given class of {@code Object}. The default
     *  mapping is specified in Appendix B of the JDBC specification.  If the
     *  resulting JDBC type is not the appropriate type for the given typeName then
     *  it is implementation defined whether an {@code SQLException} is
     *  thrown or the driver supports the resulting conversion.
     *
     *  @param typeName the SQL name of the type the elements of the array map to. The typeName is a
     *  database-specific name which may be the name of a built-in type, a user-defined type or a standard  SQL type supported by this database. This
     *   is the value returned by {@code Array.getBaseTypeName}
     *  @param elements the elements that populate the returned object
     *  @return an Array object whose elements map to the specified SQL type
     *  @throws SQLException if a database error occurs, the JDBC type is not
     *   appropriate for the typeName and the conversion is not supported, the typeName is null or this method is called on a closed connection
     *  @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this data type
     *  @since 1.6
     */
    public Array createArrayOf(
            String typeName,
            Object[] elements)
            throws SQLException {

        checkClosed();

        if (typeName == null) {
            throw JDBCUtil.nullArgument();
        }

        typeName = typeName.toUpperCase();

        int typeNumber = Type.getTypeNr(typeName);

        if (typeNumber == Integer.MIN_VALUE) {
            throw JDBCUtil.invalidArgument(typeName);
        }

        Type type = Type.getDefaultType(typeNumber);

        if (type.isArrayType() || type.isLobType() || type.isRowType()) {
            throw JDBCUtil.invalidArgument(typeName);
        }

        Object[] newData = new Object[elements.length];

        try {
            for (int i = 0; i < elements.length; i++) {
                Object o = type.convertJavaToSQL(sessionProxy, elements[i]);

                newData[i] = type.convertToTypeLimits(sessionProxy, o);
            }
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }

        Type arrayType = new ArrayType(type, newData.length);

        return new JDBCArray(newData, type, arrayType, this);
    }

    /**
     * Factory method for creating Struct objects.
     *
     * @param typeName the SQL type name of the SQL structured type that this {@code Struct}
     * object maps to. The typeName is the name of  a user-defined type that
     * has been defined for this database. It is the value returned by
     * {@code Struct.getSQLTypeName}.
     * @param attributes the attributes that populate the returned object
     * @return a Struct object that maps to the given SQL type and is populated with the given attributes
     * @throws SQLException if a database error occurs, the typeName is null or this method is called on a closed connection
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support this data type
     * @since JDK 1.6, HSQLDB 2.0
     */
    public Struct createStruct(
            String typeName,
            Object[] attributes)
            throws SQLException {
        checkClosed();

        throw JDBCUtil.notSupported();
    }

// ------------------- java.sql.Wrapper implementation ---------------------

    /**
     * Returns an object that implements the given interface to allow access to
     * non-standard methods, or standard methods not exposed by the proxy.
     *
     * If the receiver implements the interface then the result is the receiver
     * or a proxy for the receiver. If the receiver is a wrapper
     * and the wrapped object implements the interface then the result is the
     * wrapped object or a proxy for the wrapped object. Otherwise return
     * the result of calling {@code unwrap} recursively on the wrapped object
     * or a proxy for that result. If the receiver is not a
     * wrapper and does not implement the interface, then an {@code SQLException} is thrown.
     *
     * @param <T> by which the return type is inferred from input parameter.
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws java.sql.SQLException If no object found that implements the interface
     * @since JDK 1.6, HSQLDB 2.0
     */
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws java.sql.SQLException {

        checkClosed();

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
     * @since JDK 1.6, HSQLDB 2.0
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        checkClosed();

        return (iface != null && iface.isAssignableFrom(this.getClass()));
    }

    //--------------------------JDBC 4.1 -----------------------------

    /**
     * Sets the given schema name to access.
     * <P>
     * If the driver does not support schemas, it will
     * silently ignore this request.
     * <p>
     * Calling {@code setSchema} has no effect on previously created or prepared
     * {@code Statement} objects. It is implementation defined whether a DBMS
     * prepare operation takes place immediately when the {@code Connection}
     * method {@code prepareStatement} or {@code prepareCall} is invoked.
     * For maximum portability, {@code setSchema} should be called before a
     * {@code Statement} is created or prepared.
     *
     * @param schema the name of a schema  in which to work
     * @throws SQLException if a database access error occurs
     * or this method is called on a closed connection
     * @see #getSchema
     * @since JDK 1.7, HSQLDB 2.0.1
     */
    public synchronized void setSchema(String schema) throws SQLException {

        checkClosed();

        if (schema == null) {
            throw JDBCUtil.nullArgument("schema");
        } else if (schema.isEmpty()) {
            throw JDBCUtil.invalidArgument("Zero-length schema");
        } else {
            (new JDBCDatabaseMetaData(this)).setConnectionDefaultSchema(schema);
        }
    }

    /**
     * Retrieves this {@code Connection} object's current schema name.
     *
     * @return the current schema name or {@code null} if there is none
     * @throws SQLException if a database access error occurs
     * or this method is called on a closed connection
     * @see #setSchema
     * @since JDK 1.7, HSQLDB 2.0.1
     */
    public String getSchema() throws SQLException {
        checkClosed();

        return new JDBCDatabaseMetaData(this).getConnectionDefaultSchema();
    }

    /**
     * Terminates an open connection.  Calling {@code abort} results in:
     * <ul>
     * <li>The connection marked as closed
     * <li>Closes any physical connection to the database
     * <li>Releases resources used by the connection
     * <li>Insures that any thread that is currently accessing the connection
     * will either progress to completion or throw an {@code SQLException}.
     * </ul>
     * <p>
     * Calling {@code abort} marks the connection closed and releases any
     * resources. Calling {@code abort} on a closed connection is a
     * no-op.
     * <p>
     * It is possible that the aborting and releasing of the resources that are
     * held by the connection can take an extended period of time.  When the
     * {@code abort} method returns, the connection will have been marked as
     * closed and the {@code Executor} that was passed as a parameter to abort
     * may still be executing tasks to release resources.
     * <p>
     * This method checks to see that there is an {@code SQLPermission}
     * object before allowing the method to proceed.  If a
     * {@code SecurityManager} exists and its
     * {@code checkPermission} method denies calling {@code abort},
     * this method throws a
     * {@code java.lang.SecurityException}.
     * @param executor  The {@code Executor}  implementation which will
     * be used by {@code abort}.
     * @throws java.sql.SQLException if a database access error occurs or
     * the {@code executor} is {@code null},
     * @throws java.lang.SecurityException if a security manager exists and its
     *    {@code checkPermission} method denies calling {@code abort}
     * @see SecurityManager#checkPermission
     * @see java.util.concurrent.Executor
     * @since JDK 1.7, HSQLDB 2.0.1
     */
    public void abort(
            java.util.concurrent.Executor executor)
            throws SQLException {

        if (executor == null) {
            throw JDBCUtil.nullArgument("executor");
        }

        close();
    }

    /**
     *
     * Sets the maximum period a {@code Connection} or
     * objects created from the {@code Connection}
     * will wait for the database to reply to any one request. If any
     *  request remains unanswered, the waiting method will
     * return with a {@code SQLException}, and the {@code Connection}
     * or objects created from the {@code Connection}  will be marked as
     * closed. Any subsequent use of
     * the objects, with the exception of the {@code close},
     * {@code isClosed} or {@code Connection.isValid}
     * methods, will result in  a {@code SQLException}.
     * <p>
     * <b>Note</b>: This method is intended to address a rare but serious
     * condition where network partitions can cause threads issuing JDBC calls
     * to hang uninterruptedly in socket reads, until the OS TCP-TIMEOUT
     * (typically 10 minutes). This method is related to the
     * {@link #abort abort() } method which provides an administrator
     * thread a means to free any such threads in cases where the
     * JDBC connection is accessible to the administrator thread.
     * The {@code setNetworkTimeout} method will cover cases where
     * there is no administrator thread, or it has no access to the
     * connection. This method is severe in its effects, and should be
     * given a high enough value so it is never triggered before any more
     * normal timeouts, such as transaction timeouts.
     * <p>
     * JDBC driver implementations  may also choose to support the
     * {@code setNetworkTimeout} method to impose a limit on database
     * response time, in environments where no network is present.
     * <p>
     * Drivers may internally implement some or all of their API calls with
     * multiple internal driver-database transmissions, and it is left to the
     * driver implementation to determine whether the limit will be
     * applied always to the response to the API call, or to any
     * single  request made during the API call.
     * <p>
     *
     * This method can be invoked more than once, such as to set a limit for an
     * area of JDBC code, and to reset to the default on exit from this area.
     * Invocation of this method has no impact on already outstanding
     * requests.
     * <p>
     * The {@code Statement.setQueryTimeout()} timeout value is independent of the
     * timeout value specified in {@code setNetworkTimeout}. If the query timeout
     * expires  before the network timeout then the
     * statement execution will be canceled. If the network is still
     * active the result will be that both the statement and connection
     * are still usable. However if the network timeout expires before
     * the query timeout or if the statement timeout fails due to network
     * problems, the connection will be marked as closed, any resources held by
     * the connection will be released and both the connection and
     * statement will be unusable.
     * <p>
     * When the driver determines that the {@code setNetworkTimeout} timeout
     * value has expired, the JDBC driver marks the connection
     * closed and releases any resources held by the connection.
     * <p>
     *
     * This method checks to see that there is an {@code SQLPermission}
     * object before allowing the method to proceed.  If a
     * {@code SecurityManager} exists and its
     * {@code checkPermission} method denies calling
     * {@code setNetworkTimeout}, this method throws a
     * {@code java.lang.SecurityException}.
     *
     * @param executor  The {@code Executor}  implementation which will
     * be used by {@code setNetworkTimeout}.
     * @param milliseconds The time in milliseconds to wait for the database
     * operation
     *  to complete.  If the JDBC driver does not support milliseconds, the
     * JDBC driver will round the value up to the nearest second.  If the
     * timeout period expires before the operation
     * completes, a SQLException will be thrown.
     * A value of 0 indicates that there is no timeout for database operations.
     * @throws java.sql.SQLException if a database access error occurs, this
     * method is called on a closed connection,
     * the {@code executor} is {@code null},
     * or the value specified for {@code seconds} is less than 0.
     * @throws java.lang.SecurityException if a security manager exists and its
     *    {@code checkPermission} method denies calling
     * {@code setNetworkTimeout}.
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see SecurityManager#checkPermission
     * @see Statement#setQueryTimeout
     * @see #getNetworkTimeout
     * @see #abort
     * @see java.util.concurrent.Executor
     * @since JDK 1.7, HSQLDB 2.0.1
     */
    public void setNetworkTimeout(
            java.util.concurrent.Executor executor,
            int milliseconds)
            throws SQLException {
        checkClosed();

        throw JDBCUtil.notSupported();
    }

    /**
     * Retrieves the number of milliseconds the driver will
     * wait for a database request to complete.
     * If the limit is exceeded, a
     * {@code SQLException} is thrown.
     *
     * @return the current timeout limit in milliseconds; zero means there is
     *         no limit
     * @throws SQLException if a database access error occurs or
     * this method is called on a closed {@code Connection}
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @see #setNetworkTimeout
     * @since JDK 1.7, HSQLDB 2.0.1
     */
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    //--------------------------JDBC 4.3 -----------------------------

    /**
    * Hints to the driver that a request, an independent unit of work, is beginning
    * on this connection. Each request is independent of all other requests
    * with regard to state local to the connection either on the client or the
    * server. Work done between {@code beginRequest}, {@code endRequest}
    * pairs does not depend on any other work done on the connection either as
    * part of another request or outside of any request. A request may include multiple
    * transactions. There may be dependencies on committed database state as
    * that is not local to the connection.
    * <p>
    * Local state is defined as any state associated with a Connection that is
    * local to the current Connection either in the client or the database that
    * is not transparently reproducible.
    * <p>
    * Calls to {@code beginRequest} and {@code endRequest}  are not nested.
    * Multiple calls to {@code beginRequest} without an intervening call
    * to {@code endRequest} is not an error. The first {@code beginRequest} call
    * marks the start of the request and subsequent calls are treated as
    * a no-op
    * <p>
    * Use of {@code beginRequest} and {@code endRequest} is optional, vendor
    * specific and should largely be transparent. In particular
    * implementations may detect conditions that indicate dependence on
    * other work such as an open transaction. It is recommended though not
    * required that implementations throw a {@code SQLException} if there is an active
    * transaction and {@code beginRequest} is called.
    * Using these methods may improve performance or provide other benefits.
    * Consult your vendors documentation for additional information.
    * <p>
    * It is recommended to
    * enclose each unit of work in {@code beginRequest}, {@code endRequest}
    * pairs such that there is no open transaction at the beginning or end of
    * the request and no dependency on local state that crosses request
    * boundaries. Committed database state is not local.
    *
    * <p>
    * The default implementation is a no-op.
    *
    * <p>
    * This method is to be used by Connection pooling managers.
    * <p>
    * The pooling manager should call {@code beginRequest} on the underlying connection
    * prior to returning a connection to the caller.
    * <p>
    * The pooling manager does not need to call {@code beginRequest} if:
    * <ul>
    * <li>The connection pool caches {@code PooledConnection} objects</li>
    * <li>Returns a logical connection handle when {@code getConnection} is
    * called by the application</li>
    * <li>The logical {@code Connection} is closed by calling
    * {@code Connection.close} prior to returning the {@code PooledConnection}
    * to the cache.</li>
    * </ul>
    * @throws SQLException if an error occurs
    * @since 9
    * @see javax.sql.PooledConnection
    */
    public void beginRequest() throws SQLException {

        // Default method takes no action
    }

    /**
     * Hints to the driver that a request, an independent unit of work,
     * has completed. Calls to {@code beginRequest}
     * and {@code endRequest} are not nested. Multiple
     * calls to {@code endRequest} without an intervening call to {@code beginRequest}
     * is not an error. The first {@code endRequest} call
     * marks the request completed and subsequent calls are treated as
     * a no-op. If {@code endRequest} is called without an initial call to
     * {@code beginRequest} is a no-op.
     * <p>
     * The exact behavior of this method is vendor specific. In particular
     * implementations may detect conditions that indicate dependence on
     * other work such as an open transaction. It is recommended though not
     * required that implementations throw a {@code SQLException} if there is an active
     * transaction and {@code endRequest} is called.
     *
     * <p>
     * The default implementation is a no-op.
     * <p>
     *
     * This method is to be used by Connection pooling managers.
     * <p>
     * The pooling manager should call {@code endRequest} on the underlying connection
     * when the application returns the connection back to the connection pool.
     * <p>
     * The pooling manager does not need to call {@code endRequest} if:
     * <ul>
     * <li>The connection pool caches {@code PooledConnection} objects</li>
     * <li>Returns a logical connection handle when {@code getConnection} is
     * called by the application</li>
     * <li>The logical {@code Connection} is closed by calling
     * {@code Connection.close} prior to returning the {@code PooledConnection}
     * to the cache.</li>
     * </ul>
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB resets the SQL session of this connection.<p>
     *
     * The user of an SQL session may declare session variables and
     * session-based temporary tables that keep their data at commit time.
     * A session reset removes these tables and variables and resets all
     * session settings to their defaults.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @throws SQLException if an error occurs
     * @since 9
     * @see javax.sql.PooledConnection
     */
    public void endRequest() throws SQLException {
        reset();
    }

//---------------------- internal implementation ---------------------------
// -------------------------- Common Attributes ------------------------------

    /** Shared, reused local TimeZone which should not be modified*/
    TimeZone timeZone = TimeZone.getDefault();

    /** Initial holdability */
    int rsHoldability = JDBCResultSet.HOLD_CURSORS_OVER_COMMIT;

    /** Properties for the connection */
    HsqlProperties connProperties;

    /** Properties for the session */
    HsqlProperties clientProperties;

    /**
     * This connection's interface to the corresponding Session
     * object in the database engine.
     */
    SessionInterface sessionProxy;

    /**
     * Is this an internal connection?
     */
    boolean isInternal;

    /** Is this connection to a network server instance. */
    protected boolean isNetConn;

    /**
     * Is this connection closed?
     */
    boolean isClosed;

    /** The first warning in the chain. Null if there are no warnings. */
    private SQLWarning rootWarning;

    /** Synchronizes concurrent modification of the warning chain */
    private final Object rootWarning_mutex = new Object();

    /** ID sequence for unnamed savepoints */
    private int savepointIDSequence;

    /** reuse count in ConnectionPool */
    int incarnation;

    /** used by a JDBCPool or other custom ConnectionPool instance */
    boolean isPooled;

    /** used */
    volatile boolean is;

    /** used by a JDBCPool or other custom ConnectionPool instance */
    JDBCConnectionEventListener poolEventListener;

    /** connection URL property close_result indicates to close old result when Statement is reused */
    boolean isCloseResultSet;

    /** connection URL property defult_schema for use with OpenOffic indicates to use PUBLIC schema by default */
    boolean isDefaultSchema;

    /** connection URL property memory_lobs indicates to load lobs fully into memory in ResultSet */
    boolean isMemoryLobs;

    /** connection URL property use_column_name indicates to return column name in ResultMetadata */
    boolean isUseColumnName = true;

    /** database property for translation of INTERVAL types to VARCHAR */
    boolean isTranslateTTIType = true;

    /** connection URL property allow_empty_batch indicates to accept executeBatch() when the batch is empty */
    boolean isAllowEmptyBatch = false;

    /** database URL property hsqldb.live_object indicates to store non-serialized object in OTHER columns */
    boolean isStoreLiveObject = false;

    /**
     * Constructs a new external {@code Connection} to an HSQLDB
     * {@code Database}. <p>
     *
     * This constructor is called on behalf of the
     * {@code java.sql.DriverManager} when getting a
     * {@code Connection} for use in normal (external)
     * client code. <p>
     *
     * Internal client code, that being code located in HSQLDB SQL
     * functions and stored procedures, receives an INTERNAL
     * connection constructed by the {@link
     * #JDBCConnection(org.hsqldb.SessionInterface)
     * JDBCConnection(SessionInterface)} constructor.
     *
     * @param props A {@code Properties} object containing the connection
     *      properties
     * @throws SQLException when the user/password combination is
     *     invalid, the connection url is invalid, or the
     *     {@code Database} is unavailable. <p>
     *
     *     The {@code Database} may be unavailable for a number
     *     of reasons, including network problems or the fact that it
     *     may already be in use by another process.
     */
    public JDBCConnection(HsqlProperties props) throws SQLException {

        String user     = props.getProperty("user");
        String password = props.getProperty("password");
        String connType = props.getProperty("connection_type");
        String host     = props.getProperty("host");
        int    port     = props.getIntegerProperty("port", 0);
        String path     = props.getProperty("path");
        String database = props.getProperty("database");
        boolean isTLS = (DatabaseURL.S_HSQLS.equals(connType)
                         || DatabaseURL.S_HTTPS.equals(connType));
        boolean isTLSWrapper = props.isPropertyTrue(
            HsqlDatabaseProperties.url_tls_wrapper,
            false);

        isTLSWrapper &= isTLS;

        if (user == null) {
            user = "SA";
        }

        if (password == null) {
            password = "";
        }

        try {
            if (DatabaseURL.isInProcessDatabaseType(connType)) {

                /*
                 * @todo - fredt - this should be the only static reference to
                 * a core class (apart form references to the Type package)
                 * from the jdbc package - we might make it dynamic
                 */
                sessionProxy = DatabaseManager.newSession(
                    connType,
                    database,
                    user,
                    password,
                    props,
                    timeZone);
            } else if (DatabaseURL.S_HSQL.equals(connType)
                       || DatabaseURL.S_HSQLS.equals(connType)) {
                sessionProxy = new ClientConnection(
                    host,
                    port,
                    path,
                    database,
                    isTLS,
                    isTLSWrapper,
                    user,
                    password,
                    timeZone);
                isNetConn = true;
            } else if (DatabaseURL.S_HTTP.equals(connType)
                       || DatabaseURL.S_HTTPS.equals(connType)) {
                sessionProxy = new ClientConnectionHTTP(
                    host,
                    port,
                    path,
                    database,
                    isTLS,
                    isTLSWrapper,
                    user,
                    password,
                    timeZone);
                isNetConn = true;
            } else {    // alias: type not yet implemented
                throw JDBCUtil.invalidArgument(connType);
            }

            sessionProxy.setJDBCConnection(this);

            connProperties   = props;
            clientProperties = sessionProxy.getClientProperties();

            setLocalVariables();
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(e);
        }
    }

    /**
     * Constructs an {@code INTERNAL} {@code Connection},
     * using the specified {@link org.hsqldb.SessionInterface
     * SessionInterface}. <p>
     *
     * This constructor is called only on behalf of an existing
     * {@code Session} (the internal parallel of a
     * {@code Connection}), to be used as a parameter to a SQL
     * function or stored procedure that needs to execute in the context
     * of that {@code Session}. <p>
     *
     * When a Java SQL function or stored procedure is called and its
     * first parameter is of type {@code Connection}, HSQLDB
     * automatically notices this and constructs an {@code INTERNAL}
     * {@code Connection} using the current {@code Session}.
     * HSQLDB then passes this {@code Connection} in the first
     * parameter position, moving any other parameter values
     * specified in the SQL statement to the right by one position.
     * <p>
     *
     * To read more about this, see
     * {@link org.hsqldb.Routine Routine}. <p>
     *
     * <B>Notes:</B> <p>
     *
     * Starting with HSQLDB 1.7.2, {@code INTERNAL} connections are not
     * closed by a call to close() or by a SQL DISCONNECT.
     *
     * For HSQLDB developers not involved with writing database
     * internals, this change only applies to connections obtained
     * automatically from the database as the first parameter to
     * Java stored procedures and functions. This is mainly an issue
     * to developers writing custom SQL function and stored procedure
     * libraries for HSQLDB. Presently, it is recommended that SQL function and
     * stored procedure code avoid depending on closing or issuing a
     * DISCONNECT on a connection obtained in this manner.
     *
     * @param c the Session requesting the construction of this
     *     Connection
     * @throws HsqlException never (reserved for future use);
     * @see org.hsqldb.Routine
     */
    public JDBCConnection(SessionInterface c) {

        // PRE: SessionInterface is non-null
        isInternal   = true;
        sessionProxy = c;
    }

    /**
     * Constructor for use with connection pooling and XA.
     *
     * @param c the connection
     * @param eventListener the listener
     */
    public JDBCConnection(
            JDBCConnection c,
            JDBCConnectionEventListener eventListener) {

        sessionProxy      = c.sessionProxy;
        connProperties    = c.connProperties;
        clientProperties  = c.clientProperties;
        isPooled          = true;
        poolEventListener = eventListener;

        setLocalVariables();
    }

    private void setLocalVariables() {

        if (connProperties == null) {
            return;
        }

        isMemoryLobs = connProperties.isPropertyTrue(
            HsqlDatabaseProperties.url_memory_lobs,
            false);
        isCloseResultSet = connProperties.isPropertyTrue(
            HsqlDatabaseProperties.url_close_result,
            false);
        isDefaultSchema = connProperties.isPropertyTrue(
            HsqlDatabaseProperties.url_default_schema,
            false);
        isAllowEmptyBatch = connProperties.isPropertyTrue(
            HsqlDatabaseProperties.url_allow_empty_batch,
            false);
        isUseColumnName = connProperties.isPropertyTrue(
            HsqlDatabaseProperties.url_get_column_name,
            true);
        isTranslateTTIType = clientProperties.isPropertyTrue(
            HsqlDatabaseProperties.jdbc_translate_tti_types,
            true);
        isStoreLiveObject = clientProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_live_object,
            false);

        if (isStoreLiveObject) {
            String connType = connProperties.getProperty("connection_type");

            if (!DatabaseURL.S_MEM.equals(connType)) {
                isStoreLiveObject = false;
            }
        }
    }

    synchronized int getSavepointID() {
        return savepointIDSequence++;
    }

    /**
     * Retrieves this connection's JDBC url.
     *
     * This method is in support of the JDBCDatabaseMetaData.getURL() method.
     * @return the database connection url with which this object was
     *      constructed
     * @throws SQLException if this connection is closed
     */
    synchronized public String getURL() throws SQLException {

        checkClosed();

        return isInternal
               ? sessionProxy.getInternalConnectionURL()
               : connProperties.getProperty("url");
    }

    /**
     * An internal check for closed connections.
     *
     * @throws SQLException when the connection is closed
     */
    synchronized void checkClosed() throws SQLException {
        if (isClosed) {
            throw JDBCUtil.connectionClosedException();
        }
    }

    /**
     * Adds another SQLWarning to this Connection object's warning chain.
     *
     * @param w the SQLWarning to add to the chain
     */
    void addWarning(SQLWarning w) {

        // PRE:  w is never null
        synchronized (rootWarning_mutex) {
            if (rootWarning == null) {
                rootWarning = w;
            } else {
                rootWarning.setNextWarning(w);
            }
        }
    }

    /**
     * Sets the warning chain
     */
    void setWarnings(SQLWarning w) {
        synchronized (rootWarning_mutex) {
            rootWarning = w;
        }
    }

    /**
     * Resets the SQL session of this connection, so it can be used again.
     * Used when connections are returned to a connection pool.
     *
     * @throws SQLException if a database access error occurs
     */
    public void reset() throws SQLException {

        try {
            incarnation++;

            this.sessionProxy.resetSession();
        } catch (HsqlException e) {
            throw JDBCUtil.sqlException(ErrorCode.X_08006, e.getMessage(), e);
        }
    }

    /**
     * Completely closes a pooled connection
     */
    public void closeFully() {

        try {
            close();
        } catch (Throwable t) {

            //
        }

        try {
            if (sessionProxy != null) {
                sessionProxy.close();

                sessionProxy = null;
            }
        } catch (Throwable t) {

            //
        }
    }

    /**
     * provides cross-package access to the proprietary (i.e. non-JDBC)
     * HSQLDB session interface.
     *
     * @return the underlying sessionProxy for this connection
     */
    public SessionInterface getSession() {
        return sessionProxy;
    }

    /**
     * is called from within nativeSQL when the start of an JDBC escape sequence is encountered
     */
    private int onStartEscapeSequence(
            String sql,
            StringBuilder sb,
            int i)
            throws SQLException {

        sb.append(' ');

        i++;

        i = StringUtil.skipSpaces(sql, i);

        if (sql.regionMatches(true, i, "fn ", 0, 3)
                || sql.regionMatches(true, i, "oj ", 0, 3)) {
            i += 2;
        } else if (sql.regionMatches(true, i, "ts ", 0, 3)) {
            sb.append("TIMESTAMP");

            i += 2;
        } else if (sql.regionMatches(true, i, "d ", 0, 2)) {
            sb.append("DATE");

            i++;
        } else if (sql.regionMatches(true, i, "t ", 0, 2)) {
            sb.append("TIME");

            i++;
        } else if (sql.regionMatches(true, i, "call ", 0, 5)) {
            sb.append("CALL");

            i += 4;
        } else if (sql.regionMatches(true, i, "?= call ", 0, 8)) {
            sb.append("CALL");

            i += 7;
        } else if (sql.regionMatches(true, i, "? = call ", 0, 8)) {
            sb.append("CALL");

            i += 8;
        } else if (sql.regionMatches(true, i, "escape ", 0, 7)) {
            i += 6;
        } else {
            i--;

            throw JDBCUtil.sqlException(
                ErrorCode.JDBC_CONNECTION_NATIVE_SQL,
                sql.substring(i));
        }

        return i;
    }

    public boolean isInternal() {
        return isInternal;
    }

    public boolean isNetwork() {
        return isNetConn;
    }

    public HsqlProperties getConnProperties() {

        HsqlProperties props = new HsqlProperties();

        props.addProperties(connProperties);

        return props;
    }
}
