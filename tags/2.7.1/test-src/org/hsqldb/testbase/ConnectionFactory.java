/* Copyright (c) 2001-2022, The HSQL Development Group
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
package org.hsqldb.testbase;

import java.io.Closeable;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * For creating, tracking and closing the objects used by this test suite.
 * <p>
 *
 * Note that a facility is provided to notify parties interested in the
 * {@link #closeRegisteredObjects()} event.
 * <p>
 *
 * For example, this facility is used by the {@link BaseTestCase}
 * class to optionally register an action to close all embedded HSQLDB database
 * instances in response to test fixture tear down completion.
 * <p>
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @version 2.7.0
 * @since 1.9.0
 */
@SuppressWarnings("FinalClass")
public final class ConnectionFactory {

    private static final Logger LOG = Logger.getLogger(
            ConnectionFactory.class.getName());
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    private static final ThreadLocal<Set<String>> s_availableDrivers
            = ThreadLocal.withInitial(() -> new HashSet<>());

    /**
     * Given its fully qualified class name.
     * <p>
     * Class loading precedence is:
     * <ol>
     * <li>{@link ServiceLoader#load(Class) ServiceLoader.load(Driver.class)</li>
     * <li>{@link Thread#getContextClassLoader()</li>
     * <li>Calling class.{@link Class#getClassLoader() getClassLoader()}</li>
     * <li>this class.{@link Class#getClassLoader() getClassLoader()}</li>
     * <li>{@link Class#forName(String)}</li>
     * </ol>
     * Note that results are caller and thread sensitive.
     *
     * @param driver to test.
     * @return true if available, else false.
     * @throws NullPointerException if driver is null.
     */
    public static boolean isDriverAvailable(final String driver) {
        boolean result;
        try {
            result = checkIsDriverAvailable(driver);
        } catch (ClassNotFoundException cnfe) {
            result = false;
            LOG.log(Level.FINE, driver, cnfe);
        }
        return result;
    }

    /**
     * Given its fully qualified class name.
     * <p>
     * Class loading precedence is:
     * <ol>
     * <li>{@link ServiceLoader#load(Class) ServiceLoader.load(Driver.class)</li>
     * <li>{@link Thread#getContextClassLoader()</li>
     * <li>Calling class.{@link Class#getClassLoader() getClassLoader()}</li>
     * <li>this class.{@link Class#getClassLoader() getClassLoader()}</li>
     * <li>{@link Class#forName(String)}</li>
     * </ol>
     * Note that results are caller and thread sensitive.
     *
     * @param driver fully qualified class name to test
     * @throws NullPointerException   if driver is null.
     * @throws ClassNotFoundException if the driver is not available.
     */
    public static void checkDriverAvailable(final String driver)
            throws ClassNotFoundException {
        checkIsDriverAvailable(driver);
    }

    /**
     * Given its fully qualified class name.
     * <p>
     * Class loading precedence is:
     * <ol>
     * <li>{@link ServiceLoader#load(Class) ServiceLoader.load(Driver.class)</li>
     * <li>{@link Thread#getContextClassLoader()</li>
     * <li>Calling class.{@link Class#getClassLoader() getClassLoader()}</li>
     * <li>this class.{@link Class#getClassLoader() getClassLoader()}</li>
     * <li>{@link Class#forName(String)}</li>
     * </ol>
     * Note that results are caller and thread sensitive.
     *
     * @param driver fully qualified class name to test
     * @throws NullPointerException   if driver is null.
     * @throws ClassNotFoundException if the driver is not available.
     */
    @SuppressWarnings({"BroadCatchBlock", "TooBroadCatch", "UseSpecificCatch"})
    private static boolean checkIsDriverAvailable(final String driver)
            throws NullPointerException, ClassNotFoundException {
        Objects.requireNonNull(driver, "driver must not be null");
        if (s_availableDrivers.get().contains(driver)) {
            return true;
        }
        final Spliterator<Driver> spliterator = ServiceLoader
                .load(Driver.class).spliterator();
        final boolean parallel = false;
        boolean available = StreamSupport
                .stream(spliterator, parallel)
                .map(Object::getClass)
                .map(Class::getName)
                .anyMatch(name -> name.equals(driver));
        if (!available) {
            // fall back to legacy behaviour
            ClassLoader classLoader = null;
            try {
                classLoader = Thread.currentThread()
                        .getContextClassLoader();
            } catch (Exception e) {
                LOG.log(Level.WARNING, driver, e);
            }
            if (classLoader == null) {
                final String thisClassName
                        = ConnectionFactory.class.getName();
                try {
                    final String callerClassName = Stream.of(
                            Thread.currentThread().getStackTrace())
                            .map(StackTraceElement::getClassName)
                            .filter(Objects::nonNull)
                            .filter(className -> !className.equals(thisClassName))
                            .findFirst()
                            .orElse(thisClassName);
                    classLoader = Class.forName(callerClassName)
                            .getClassLoader();
                } catch (Throwable t) {
                    LOG.log(Level.WARNING, driver, t);
                }
            }
            try {
                if (classLoader == null) {
                    Class.forName(driver);
                } else {
                    Class.forName(driver, false, classLoader);
                }
                available = true;
            } catch (Throwable t) {
                s_availableDrivers.get().remove(driver);
                throw ClassNotFoundException.class.isInstance(t) 
                        ? ClassNotFoundException.class.cast(t)
                        : new ClassNotFoundException(driver, t);
            }
        }
        if (available) {
            s_availableDrivers.get().add(driver);
        }
        return available;
    }

    private final List<ConnectionRegistration> m_connectionRegistrations;
    private final List<Statement> m_statements;
    private final List<ResultSet> m_resultSets;
    private final List<Blob> m_blobs;
    private final List<Clob> m_clobs;
    private final List<Array> m_arrays;
    private final List<SQLXML> m_xmls;
    private final List<Closeable> m_closeable;
    private final List<ConnectionFactoryEventListener> m_listeners;

    /**
     * Creates a new instance of ConnectionFactory.
     */
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    public ConnectionFactory() {
        m_listeners = new ArrayList<>();
        m_closeable = new ArrayList<>();
        m_xmls = new ArrayList<>();
        m_arrays = new ArrayList<>();
        m_clobs = new ArrayList<>();
        m_blobs = new ArrayList<>();
        m_resultSets = new ArrayList<>();
        m_statements = new ArrayList<>();
        m_connectionRegistrations = new ArrayList<>();
    }

    /**
     * for notification of {@link #closeRegisteredObjects()} event.
     *
     * @param l listen to add.
     * @throws NullPointerException if l is null.
     */
    public void addEventListener(final ConnectionFactoryEventListener l)
            throws NullPointerException {
        Objects.requireNonNull(l, "l must not be null");
        if (!m_listeners.contains(l)) {
            m_listeners.add(l);
        }
    }

    /**
     * from notification of {@link #closeRegisteredObjects()} event.
     *
     * @param l listener to remove.
     * @throws NullPointerException if l is null.
     */
    public void removeEventListener(final ConnectionFactoryEventListener l)
            throws NullPointerException {
        Objects.requireNonNull(l, "l must not be null");
        m_listeners.remove(l);
    }

    /**
     * to be closed at tear down.
     *
     * @param conn to track for close.
     * @return a new registration representing the given connection and the
     *         configured default close action .
     * @throws NullPointerException if conn is null.
     */
    public ConnectionRegistration registerConnection(final Connection conn)
            throws NullPointerException {
        return registerConnection(conn, getDefaultConnectionCloseAction());
    }

    /**
     * to be closed at tear down.
     * <p>
     * Note that some drivers may already offer a commit / rollback policy
     * on close. To use that policy, specify {@link ConnectionCloseAction#None}.
     *
     * @param conn   to track for close.
     * @param action to perform before closing the connection.
     * @return a new registration representing the given connection and action.
     * @throws NullPointerException if conn is null or action is null.
     */
    public ConnectionRegistration registerConnection(final Connection conn,
            final ConnectionCloseAction action) throws NullPointerException {
        return registerConnection(new ConnectionRegistration(conn, action));
    }

    /**
     * to be closed at tear down.
     *
     * @param registration to register
     * @return the given registration.
     */
    public ConnectionRegistration registerConnection(
            final ConnectionRegistration registration)
            throws NullPointerException {
        Objects.requireNonNull(registration, "registration must not be null.");
        m_connectionRegistrations.add(registration);
        return registration;
    }

    /**
     * to be closed at tear down.
     *
     * @param stmt to track for close.
     * @return the given statement.
     * @throws NullPointerException if stmt is null.
     */
    public Statement registerStatement(final Statement stmt)
            throws NullPointerException {
        Objects.requireNonNull(stmt, "stmt must not be null.");
        m_statements.add(stmt);
        return stmt;
    }

    /**
     * to be closed at tear down.
     *
     * @param rs to track for close.
     * @return the given rs.
     * @throws NullPointerException if rs is null.
     */
    public ResultSet registerResultSet(final ResultSet rs)
            throws NullPointerException {
        Objects.requireNonNull(rs, "rs must not be null.");
        m_resultSets.add(rs);
        return rs;
    }

    /**
     * to be freed at tear down.
     *
     * @param array to track for close.
     * @return the given array.
     * @throws NullPointerException if array is null.
     */
    public Array registerArray(final Array array) throws NullPointerException {
        Objects.requireNonNull(array, "array must not be null.");
        m_arrays.add(array);
        return array;
    }

    /**
     * to be freed at tear down.
     *
     * @param blob to track for close.
     * @return the given blob.
     * @throws NullPointerException if blob is null.
     */
    public Blob registerBlob(final Blob blob) throws NullPointerException {
        Objects.requireNonNull(blob, "blob must not be null.");
        m_blobs.add(blob);
        return blob;
    }

    /**
     * to be freed at tear down.
     *
     * @param clob to track for close.
     * @return the given clob.
     * @throws NullPointerException if clob is null.
     */
    public Clob registerClob(final Clob clob) throws NullPointerException {
        Objects.requireNonNull(clob, "blob must not be null.");
        m_clobs.add(clob);
        return clob;
    }

    /**
     * to be closed at tear down as supplied by the given supplier.
     *
     * @param <T>      the generic closeable type
     * @param type     the actual closeable type
     * @param supplier that supplies thje closeable
     * @return the supplied closeable.
     * @throws NullPointerException if supplier is null or supplied closeable is
     *                              null.
     */
    public <T extends Closeable> T registerCloseable(final Class<T> type,
            final Supplier<T> supplier) {
        Objects.requireNonNull(supplier, "supplier must not be null.");
        return registerCloseable(type, supplier.get());
    }

    /**
     * to be closed at tear down.
     *
     * @param <T>       the generic closeable type
     * @param type      the actual closeable type
     * @param closeable to track for close
     * @return the given closeable.
     * @throws NullPointerException if closeable is null.
     */
    public <T extends Closeable> T registerCloseable(final Class<T> type,
            final T closeable)
            throws NullPointerException {
        Objects.requireNonNull(closeable, "closeable must not be null.");
        m_closeable.add(closeable);
        return closeable;
    }

    /**
     * to be freed at tear down.
     *
     * @param xml to track for close
     * @return the given xml.
     * @throws NullPointerException if xml is null.
     */
    public SQLXML registerSQLXML(final SQLXML xml) {
        Objects.requireNonNull(xml, "xml must not be null.");
        m_xmls.add(xml);
        return xml;
    }

    /**
     * from configuration.
     *
     * @return the default value from configuration.
     */
    public ConnectionCloseAction getDefaultConnectionCloseAction() {
        return PropertyGetter.getEnumProperty(
                getClass().getName() + ".default.connection.close.action",
                ConnectionCloseAction.class,
                ConnectionCloseAction.Rollback);
    }

    /**
     * closes all registered JDBC objects.
     */
    @SuppressWarnings({"BroadCatchBlock", "TooBroadCatch", "UseSpecificCatch"})
    public void closeRegisteredObjects() {
        for (final SQLXML xml : m_xmls) {
            try {
                xml.free();
            } catch (Throwable t) {
                LOG.log(Level.FINE, null, t);
            }
        }
        m_xmls.clear();
        for (final Array array : m_arrays) {
            try {
                array.free();
            } catch (Throwable t) {
                LOG.log(Level.FINE, null, t);
            }
        }
        m_arrays.clear();
        for (final Blob blob : m_blobs) {
            try {
                blob.free();
            } catch (Throwable t) {
                LOG.log(Level.FINE, null, t);
            }
        }
        m_blobs.clear();
        for (final Clob clob : m_clobs) {
            try {
                clob.free();
            } catch (Throwable t) {
                LOG.log(Level.FINE, null, t);
            }
        }
        m_clobs.clear();
        for (final ResultSet rs : m_resultSets) {
            try {
                rs.close();
            } catch (Throwable t) {
                LOG.log(Level.FINE, null, t);
            }
        }
        m_resultSets.clear();
        for (final Statement stmt : m_statements) {
            try {
                stmt.close();
            } catch (Throwable t) {
                LOG.log(Level.FINE, null, t);
            }
        }
        m_statements.clear();
        for (final ConnectionRegistration reg : m_connectionRegistrations) {
            final Connection conn = reg.getConnection();
            try {
                if (conn.isClosed()) {
                    continue;
                }
                switch (reg.getCloseAction()) {
                    case Rollback:
                        conn.rollback();
                        break;
                    case Commit:
                        conn.commit();
                        break;
                    default:
                        break;
                }
            } catch (Throwable t) {
                LOG.log(Level.FINE, null, t);
            }

            try {
                conn.close();
            } catch (Throwable t) {
                LOG.log(Level.FINE, null, t);
            }
        }
        m_connectionRegistrations.clear();

        final List<ConnectionFactoryEventListener> list = m_listeners;

        for (int i = 0; i < list.size(); i++) {
            final ConnectionFactoryEventListener l = list.get(i);
            try {
                l.registeredObjectsClosed(this);
            } catch (Throwable t) {
                LOG.log(Level.WARNING, null, t);
            }
        }
    }

    /**
     * with the specified JDBC driver, url, user and password.
     * <p>
     * Uses the {@link #getDefaultConnectionCloseAction() default connection
     * close action}.
     *
     * @param driver   fully qualified class name of a {@code java.sql.Driver}.
     * @param url      of connection.
     * @param user     of connection.
     * @param password of user.
     * @throws NullPointerException   if driver is null.
     * @throws ClassNotFoundException when the driver class is not available
     * @throws SQLException           if a database access error occurs or the
     *                                {@code url} is {@code null}.
     * @return a newly created and registered connection object.
     */
    public Connection newConnection(
            final String driver,
            final String url,
            final String user,
            final String password) throws Exception {
        return newConnection(driver, url, user, password,
                getDefaultConnectionCloseAction());
    }

    /**
     * with the specified JDBC driver, url, user and password.
     * <p>
     *
     * @param driver   fully qualified class name of a {@code java.sql.Driver}.
     * @param url      of connection.
     * @param user     of connection.
     * @param password of user.
     * @param action   to perform before closing the connection.
     * @throws NullPointerException   if driver is null or action is null.
     * @throws ClassNotFoundException when the driver class is not available
     * @throws SQLException           if a database access error occurs or the
     *                                {@code url} is {@code null}.
     *
     * @return a newly created and registered connection object.
     */
    public Connection newConnection(
            final String driver,
            final String url,
            final String user,
            final String password,
            final ConnectionCloseAction action) throws SQLException,
            ClassNotFoundException, NullPointerException {
        Objects.requireNonNull(driver, "driver  must not be null");
        Objects.requireNonNull(action, "action  must not be null");
        checkDriverAvailable(driver);
        final Connection conn = DriverManager.getConnection(
                url,
                user,
                password);
        registerConnection(conn, action);
        return conn;
    }

    /**
     * using the given connection.
     * <p>
     * The new statement is registered to be closed at tear down.
     *
     * @param conn with which to create.
     * @throws NullPointerException when conn is null.
     * @throws Exception            when the statement cannot be created.
     * @return a newly created and registered Statement object.
     */
    public Statement createStatement(
            final Connection conn) throws NullPointerException, Exception {
        Objects.requireNonNull(conn, "conn must not be null.");
        final Statement stmt = conn.createStatement();

        registerStatement(stmt);

        return stmt;
    }

    /**
     * using the given connection.
     * <p>
     * The new Blob is registered to be freed at tear down.
     *
     * @param conn used to create the Clob object.
     * @return a new Blob object.
     * @throws NullPointerException when conn is null.
     * @throws Exception            when a new Clob object cannot be created.
     */
    public Blob createBlob(final Connection conn) throws NullPointerException,
            Exception {
        Objects.requireNonNull(conn, "conn must not be null");
        final Blob blob = conn.createBlob();
        registerBlob(blob);
        return blob;
    }

    /**
     * using the given connection.
     * <p>
     * The new Clob is registered to be freed at tear down.
     *
     * @param conn used to create the SQLXML object.
     * @return a new Clob object.
     * @throws NullPointerException when conn is null.
     * @throws Exception            when a new Clob object cannot be created.
     */
    public Clob createClob(final Connection conn) throws NullPointerException,
            Exception {
        Objects.requireNonNull(conn, "conn must not be null");
        final Clob clob = conn.createClob();
        registerClob(clob);
        return clob;
    }

    /**
     * using the given connection.
     * <p>
     * The new NClob is registered to be freed at tear down.
     *
     * @param conn used to create the NClob object.
     * @return a new NClob object.
     * @throws NullPointerException when conn is null.
     * @throws Exception            when a new NClob object cannot be created.
     */
    public NClob createNClob(final Connection conn) throws NullPointerException,
            Exception {
        Objects.requireNonNull(conn, "conn must not be null");
        final NClob clob = conn.createNClob();
        registerClob(clob);
        return clob;
    }

    /**
     * using the given connection.
     * <p>
     * The new SQLXML is registered to be freed at tear down.
     *
     * @param conn used to create the SQLXML object.
     * @return a new SQLXML object.
     * @throws NullPointerException when conn is null.
     * @throws Exception            when a new SQLXML object cannot be created.
     */
    public SQLXML createSQLXML(final Connection conn) throws NullPointerException,
            Exception {
        Objects.requireNonNull(conn, "conn must not be null");
        final SQLXML xml = conn.createSQLXML();
        registerSQLXML(xml);
        return xml;
    }

    /**
     * for the given {@code sql} using the given connection.
     * <p>
     * The new statement is registered to be closed at tear down.
     *
     * @param sql  to prepare.
     * @param conn with which to prepare.
     * @throws Exception when the statement cannot be prepared.
     * @return a newly prepared and registered object.
     */
    public PreparedStatement prepareStatement(
            final String sql,
            final Connection conn) throws Exception {
        final PreparedStatement pstmt = conn.prepareStatement(sql);

        registerStatement(pstmt);

        return pstmt;
    }

    /**
     * for the given {@code sql} using the given connection.
     * <p>
     * The new statement is registered to be closed at tear down.
     *
     * @param sql  to prepare,
     * @param conn with which to prepare.
     * @throws NullPointerException when sql is null or conn is null.
     * @throws Exception            when the call cannot be prepared.
     * @return the newly prepared and registered object.
     */
    public CallableStatement prepareCall(
            final String sql,
            final Connection conn) throws NullPointerException, Exception {
        Objects.requireNonNull(sql, "sql must not be null.");
        Objects.requireNonNull(conn, "stmt must not be null.");
        final CallableStatement stmt = conn.prepareCall(sql);

        registerStatement(stmt);

        return stmt;
    }

    /**
     * using the given {@code sql} and statement object.
     * <p>
     * The returned {@code ResultSet} is registered to be closed at tear down.
     *
     * @param sql  to execute.
     * @param stmt against which to execute.
     * @throws NullPointerException if sql is null or stmt is null.
     * @throws Exception            when execution fails.
     * @return the query result.
     */
    public ResultSet executeQuery(
            final String sql,
            final Statement stmt) throws Exception {
        Objects.requireNonNull(sql, "sql must not be null.");
        Objects.requireNonNull(stmt, "stmt must not be null.");
        final ResultSet rs = stmt.executeQuery(sql);

        registerResultSet(rs);

        return rs;
    }

    /**
     * using the given statement object.
     * <p>
     * The returned {@code ResultSet} is registered to be closed at tear down.
     *
     * @param stmt to execute.
     * @throws NullPointerException when stmt is null.
     * @throws Exception            when execution fails.
     * @return the query result.
     */
    public ResultSet executeQuery(
            final PreparedStatement stmt) throws Exception {
        Objects.requireNonNull(stmt, "stmt must not be null.");
        final ResultSet rs = stmt.executeQuery();

        registerResultSet(rs);

        return rs;
    }
}
