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
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;

import java.util.Properties;

import java.sql.DriverAction;
import java.sql.SQLFeatureNotSupportedException;

import org.hsqldb.DatabaseURL;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.persist.HsqlProperties;

// fredt@users 20011220 - patch 1.7.0 by fredt
// new version numbering scheme
// fredt@users 20020320 - patch 1.7.0 - JDBC 2 support and error trapping
// fredt@users 20030528 - patch 1.7.2 suggested by Gerhard Hiller - support for properties in URL
// campbell-burnet@users 20051207 - patch 1.8.x initial JDBC 4.0 support work

/**
 * The interface that every driver class must implement.
 * <P>The Java SQL framework allows for multiple database drivers.
 *
 * <P>Each driver should supply a class that implements
 * the Driver interface.
  *
 * <P>The DriverManager will try to load as many drivers as it can
 * find and then for any given connection request, it will ask each
 * driver in turn to try to connect to the target URL.
 *
 * <P>It is strongly recommended that each Driver class should be
 * small and standalone so that the Driver class can be loaded and
 * queried without bringing in vast quantities of supporting code.
 *
 * <P>When a Driver class is loaded, it should create an instance of
 * itself and register it with the DriverManager. This means that a
 * user can load and register a driver by calling:
 * <p>
 * {@code Class.forName("foo.bah.Driver")}
 * <p>
 * A JDBC driver may create a {@linkplain DriverAction} implementation in order
 * to receive notifications when {@linkplain DriverManager#deregisterDriver} has
 * been called.
 *
 * <!-- start release-specific documentation -->
 * <div class="ReleaseSpecificDocumentation">
 * <p class="rshead">HSQLDB-Specific Information:</p>
 *
 *  When the HSQL Database Engine Driver class is loaded, it creates an
 *  instance of itself and register it with the DriverManager. This means
 *  that a user can load and register the HSQL Database Engine driver by
 *  calling:
 *  <pre>
 *  {@code Class.forName("org.hsqldb.jdbc.JDBCDriver")}
 *  </pre>
 *
 *  For detailed information about how to obtain HSQLDB JDBC Connections,
 *  please see {@link org.hsqldb.jdbc.JDBCConnection JDBCConnection}.
 *
 * <hr>
 *
 * <b>JDBC 4.0 notes:</b><p>
 *
 * Starting with JDBC 4.0 (JDK 1.6), the {@code DriverManager} methods
 * {@code getConnection} and {@code getDrivers} have been
 * enhanced to support the Java Standard Edition Service Provider mechanism.
 * When built under a Java runtime that supports JDBC 4.0, HSQLDB distribution
 * jars containing the Driver implementation also include the file
 * {@code META-INF/services/java.sql.Driver}. This file contains the fully
 * qualified class name ('org.hsqldb.jdbc.JDBCDriver') of the HSQLDB implementation
 * of {@code java.sql.Driver}. <p>
 *
 * Hence, under JDBC 4.0 or greater, applications no longer need to explicitly
 * load the HSQLDB JDBC driver using {@code Class.forName()}. Of course,
 * existing programs which do load JDBC drivers using
 * {@code Class.forName()} will continue to work without modification. <p>
 *
 * JDBC 4.2 methods added in Java 8 are generally supported.
 * </div>
 * <!-- end release-specific documentation -->
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
 * @since JDK 1.1, HSQLDB 1.9.0
 *
 *
 * @see java.sql.DriverManager
 * @see java.sql.Connection
 * @see java.sql.DriverAction
 */
public class JDBCDriver implements Driver {

    /**
     * name of loginTimeout property, can not change
     */
    static final String conn_loginTimeout = "loginTimeout";

    /**
     * Default constructor
     */
    public JDBCDriver() {}

    /**
     * Attempts to make a database connection to the given URL.
     * The driver should return "null" if it realizes it is the wrong kind
     * of driver to connect to the given URL.  This will be common, as when
     * the JDBC driver manager is asked to connect to a given URL it passes
     * the URL to each loaded driver in turn.
     *
     * <P>The driver should throw an {@code SQLException} if it is the right
     * driver to connect to the given URL but has trouble connecting to
     * the database.
     *
     * <P>The {@code java.util.Properties} argument can be used to pass
     * arbitrary string tag/value pairs as connection arguments.
     * Normally at least "user" and "password" properties should be
     * included in the {@code Properties} object.
     * <p>
     * <B>Note:</B> If a property is specified as part of the {@code url} and
     * is also specified in the {@code Properties} object, it is
     * implementation-defined as to which value will take precedence. For
     * maximum portability, an application should only specify a property once.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     *  For the HSQL Database Engine, at least "user" and
     *  "password" properties should be included in the Properties.<p>
     *
     *  From version 1.7.1, two optional properties are supported:
     *
     *  <ul>
     *      <li>{@code get_column_name} (default true) -  if set to false,
     *          a ResultSetMetaData.getColumnName() call will return the user
     *          defined label (getColumnLabel()) instead of the column
     *          name.<br>
     *
     *          This property is available in order to achieve
     *          compatibility with certain non-HSQLDB JDBC driver
     *          implementations.</li>
     *
     *      <li>{@code strict_md} if set to true, some ResultSetMetaData
     *          methods return more strict values for compatibility
     *          reasons.</li>
     *  </ul> <p>
     *
     *  From version 1.8.0.x, {@code strict_md} is deprecated (ignored)
     *  because metadata reporting is always strict (JDBC-compliant), and
     *  three new optional properties are supported:
     *
     *  <ul>
     *      <li>{@code ifexits} (default false) - when true, an exception
     *          is raised when attempting to connect to an in-process
     *          file: or mem: scheme database instance if it has not yet been
     *          created.  When false, an in-process file: or mem: scheme
     *          database instance is created automatically if it has not yet
     *          been created. This property does not apply to requests for
     *          network or res: (i.e. files_in_jar) scheme connections. <li>
     *
     *      <li>{@code shutdown} (default false) - when true, the
     *          the target database mimics the behaviour of 1.7.1 and older
     *          versions. When the last connection to a database is closed,
     *          the database is automatically shut down. The property takes
     *          effect only when the first connection is made to the database.
     *          This means the connection that opens the database. It has no
     *          effect if used with subsequent, simultaneous connections. <br>
     *
     *          This command has two uses. One is for test suites, where
     *          connections to the database are made from one JVM context,
     *          immediately followed by another context. The other use is for
     *          applications where it is not easy to configure the environment
     *          to shutdown the database. Examples reported by users include
     *          web application servers, where the closing of the last
     *          connection coincides with the web application being shut down.
     *          </li>
     *
     *      <li>{@code default_schema} - backwards compatibility feature.
     *          To be used for clients written before HSQLDB schema support.
     *          Denotes whether to use the default schema when a schema
     *          qualifier is not included in a database object's SQL identifier
     *          character sequence. Also affects the semantics of
     *          DatabaseMetaData calls that supply null-valued schemaNamePattern
     *          parameter values.</li>
     *  </ul>
     * </div>
     * <!-- end release-specific documentation -->
     * @param url the URL of the database to which to connect
     * @param info a list of arbitrary string tag/value pairs as
     * connection arguments. Normally at least a "user" and
     * "password" property should be included.
     * @return a {@code Connection} object that represents a
     *      connection to the URL
     * @throws SQLException if a database access error occurs or the url is
     * {@code null}
     */
    public Connection connect(String url, Properties info) throws SQLException {

        if (url.regionMatches(true,
                              0,
                              DatabaseURL.S_URL_INTERNAL,
                              0,
                              DatabaseURL.S_URL_INTERNAL.length())) {
            JDBCConnection conn = threadConnection.get();

            return conn;
        }

        return getConnection(url, info);
    }

    /**
     * The static equivalent of the {@code connect(String,Properties)}
     * method.
     *
     * @param url the URL of the database to which to connect
     * @param info a list of arbitrary string tag/value pairs as connection
     *      arguments including at least at a "user" and a "password" property
     * @throws java.sql.SQLException if a database access error occurs
     * @return a {@code Connection} object that represents a
     *      connection to the URL
     */
    @SuppressWarnings("deprecation")
    public static Connection getConnection(
            String url,
            Properties info)
            throws SQLException {

        final HsqlProperties props = DatabaseURL.parseURL(url, true, false);

        if (props == null) {

            // supposed to be an HSQLDB driver url but has errors
            throw JDBCUtil.invalidArgument();
        } else if (props.isEmpty()) {

            // is not an HSQLDB driver url
            return null;
        }

        long timeout = 0;

        if (info != null) {
            timeout = HsqlProperties.getIntegerProperty(
                info,
                conn_loginTimeout,
                0);
        }

        props.addProperties(info);

        if (timeout == 0) {
            timeout = DriverManager.getLoginTimeout();
        }

        // @todo:  maybe impose some sort of sane restriction
        //         on network connections regardless of user
        //         specification?
        if (timeout == 0) {

            // no timeout restriction
            return new JDBCConnection(props);
        }

        String connType = props.getProperty("connection_type");

        if (DatabaseURL.isInProcessDatabaseType(connType)) {
            return new JDBCConnection(props);
        }

        // @todo: Better: ThreadPool? HsqlTimer with callback?
        final JDBCConnection[] conn = new JDBCConnection[1];
        final SQLException[]   ex   = new SQLException[1];
        Thread                 t    = new Thread() {

            public void run() {

                try {
                    conn[0] = new JDBCConnection(props);
                } catch (SQLException se) {
                    ex[0] = se;
                }
            }
        };

        t.start();

        try {
            t.join(1000 * timeout);
        } catch (InterruptedException ie) {}

        if (ex[0] != null) {
            throw ex[0];
        }

        if (conn[0] != null) {
            return conn[0];
        }

        throw JDBCUtil.sqlException(ErrorCode.X_08501);
    }

    /**
     * Retrieves whether the driver thinks that it can open a connection
     * to the given URL.  Typically drivers will return {@code true} if they
     * understand the sub-protocol specified in the URL and {@code false} if
     * they do not.
     *
     * @param  url the URL of the database
     * @return {@code true} if this driver understands the given URL;
     *         {@code false} otherwise
     * {@code null}
     */
    public boolean acceptsURL(String url) {

        if (url == null) {
            return false;
        }

        if (url.regionMatches(true,
                              0,
                              DatabaseURL.S_URL_PREFIX,
                              0,
                              DatabaseURL.S_URL_PREFIX.length())) {
            return true;
        }

        return url.regionMatches(
            true,
            0,
            DatabaseURL.S_URL_INTERNAL,
            0,
            DatabaseURL.S_URL_INTERNAL.length());
    }

    /**
     * Gets information about the possible properties for this driver.
     * <P>
     * The {@code getPropertyInfo} method is intended to allow a generic
     * GUI tool to discover what properties it should prompt
     * a human for in order to get
     * enough information to connect to a database.  Note that depending on
     * the values the human has supplied so far, additional values may become
     * necessary, so it may be necessary to iterate though several calls
     * to the {@code getPropertyInfo} method.
     *
     * <!-- start release-specific documentation -->
     * <div class="ReleaseSpecificDocumentation">
     * <p class="rshead">HSQLDB-Specific Information:</p>
     *
     * HSQLDB uses the values submitted in info to set the value for
     * each DriverPropertyInfo object returned.<p>
     *
     * Only the connection properties are considered. General database
     * properties are ignored.
     *
     * </div>
     * <!-- end release-specific documentation -->
     *
     * @param  url the URL of the database to which to connect
     * @param  info a proposed list of tag/value pairs that will be sent on
     *      connect open
     * @return an array of {@code DriverPropertyInfo} objects describing
     *          possible properties.  This array may be an empty array if
     *          no properties are required.
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {

        if (!acceptsURL(url)) {
            return new DriverPropertyInfo[0];
        }

        String[]             choices = new String[]{ "true", "false" };
        DriverPropertyInfo[] pinfo   = new DriverPropertyInfo[6];
        DriverPropertyInfo   p;

        if (info == null) {
            info = new Properties();
        }

        p          = new DriverPropertyInfo("user", null);
        p.value    = info.getProperty("user");
        p.required = true;
        pinfo[0]   = p;
        p          = new DriverPropertyInfo("password", null);
        p.value    = info.getProperty("password");
        p.required = true;
        pinfo[1]   = p;
        p          = new DriverPropertyInfo("get_column_name", null);
        p.value    = info.getProperty("get_column_name", "true");
        p.required = false;
        p.choices  = choices;
        pinfo[2]   = p;
        p          = new DriverPropertyInfo("ifexists", null);
        p.value    = info.getProperty("ifexists", "false");
        p.required = false;
        p.choices  = choices;
        pinfo[3]   = p;
        p          = new DriverPropertyInfo("default_schema", null);
        p.value    = info.getProperty("default_schema", "false");
        p.required = false;
        p.choices  = choices;
        pinfo[4]   = p;
        p          = new DriverPropertyInfo("shutdown", null);
        p.value    = info.getProperty("shutdown", "false");
        p.required = false;
        p.choices  = choices;
        pinfo[5]   = p;

        return pinfo;
    }

    /**
     * Retrieves the driver's major version number. Initially this should be 1.
     *
     * @return  this driver's major version number
     */
    public int getMajorVersion() {
        return HsqlDatabaseProperties.MAJOR;
    }

    /**
     * Gets the driver's minor version number. Initially this should be 0.
     * @return  this driver's minor version number
     */
    public int getMinorVersion() {
        return HsqlDatabaseProperties.MINOR;
    }

    /**
     * Reports whether this driver is a genuine JDBC
     * Compliant driver.
     * A driver may only report {@code true} here if it passes the JDBC
     * compliance tests; otherwise it is required to return {@code false}.
     * <P>
     * JDBC compliance requires full support for the JDBC API and full support
     * for SQL 92 Entry Level.  It is expected that JDBC compliant drivers will
     * be available for all the major commercial databases.
     * <P>
     * This method is not intended to encourage the development of non-JDBC
     * compliant drivers, but is a recognition of the fact that some vendors
     * are interested in using the JDBC API and framework for lightweight
     * databases that do not support full database functionality, or for
     * special databases such as document information retrieval where a SQL
     * implementation may not be feasible.
     * @return {@code true} if this driver is JDBC Compliant; {@code false}
     *         otherwise
     */
    public boolean jdbcCompliant() {
        return true;
    }

    //------------------------- JDBC 4.1 -----------------------------------

    /**
     * Return the parent Logger of all the Loggers used by this driver. This
     * should be the Logger farthest from the root Logger that is
     * still an ancestor of all of the Loggers used by this driver. Configuring
     * this Logger will affect all of the log messages generated by the driver.
     * In the worst case, this may be the root Logger.
     *
     * @return the parent Logger for this driver
     * @throws SQLFeatureNotSupportedException if the driver does not use
     * {@code java.util.logging}.
     * @since JDK 1.7, HSQLDB 2.0.1
     */
    public java.util.logging.Logger getParentLogger()
            throws java.sql.SQLFeatureNotSupportedException {
        throw(java.sql.SQLFeatureNotSupportedException) JDBCUtil.notSupported();
    }

    public static final JDBCDriver driverInstance = new JDBCDriver();

    static {
        try {
            DriverManager.registerDriver(
                driverInstance,
                new EmptyDiverAction());
        } catch (Exception e) {}
    }

    /**
     * As a separate instance of this class is registered with DriverManager
     * for each class loader, the threadConnection is not declared as static.
     * The registered instance is kept to allow access to its threadConnection.
     *
     */
    public final ThreadLocal<JDBCConnection> threadConnection =
        new ThreadLocal<>();

    //------------------------- JDBC 4.2 -----------------------------------
    private static class EmptyDiverAction implements java.sql.DriverAction {
        public void deregister() {}
    }
}
