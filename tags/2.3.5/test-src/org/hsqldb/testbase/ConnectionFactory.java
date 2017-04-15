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
package org.hsqldb.testbase;

import java.io.Closeable;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.EventListener;
import java.util.List;

/**
 * For creating, tracking and closing the
 * JDBC objects used by this test suite. <p>
 *
 * Note that a facility is provided to notify parties interested in the {@link
 * EventListener#closedRegisteredObjects(org.hsqldb.testbase.ConnectionFactory)}
 * event. <p>
 *
 * For example, this facility is used by the {@link BaseTestCase}
 * class to optionally register an action to close all embedded HSQLDB database
 * instances in response to test fixture tear down completion. <p>
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @version 2.0.1
 * @since 1.9.0
 */
public final class ConnectionFactory {

    // <editor-fold defaultstate="collapsed" desc="Registered Object Collections">
    private final class ConnectionRegistration {

        final Connection m_connection;
        final boolean m_rollback;

        ConnectionRegistration(Connection connection, boolean rollback) {
            m_connection = connection;
            m_rollback = rollback;
        }

        Connection getConnection() {
            return m_connection;
        }

        boolean isRollback() {
            return m_rollback;
        }
    }
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    private final List<ConnectionRegistration> m_connectionRegistrations = new ArrayList<ConnectionRegistration>();
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    private final List<Statement> m_statements = new ArrayList<Statement>();
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    private final List<ResultSet> m_resultSets = new ArrayList<ResultSet>();
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    private final List<Blob> m_blobs = new ArrayList<Blob>();
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    private final List<Clob> m_clobs = new ArrayList<Clob>();
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    private final List<Array> m_arrays = new ArrayList<Array>();
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    private final List<SQLXML> m_xmls = new ArrayList<SQLXML>();
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    private final List<Closeable> m_closeable = new ArrayList<Closeable>();

    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Event Listener Support">
    /**
     * for event "closedRegisteredObjects".
     */
    @SuppressWarnings("PublicInnerClass")
    public interface ConnectionFactoryEventListener extends EventListener {

        void finishedClosingRegisteredObjects(ConnectionFactory source);
    }
    private final List<ConnectionFactoryEventListener> m_listeners = new ArrayList<ConnectionFactoryEventListener>(2);

    public void addEventListener(ConnectionFactoryEventListener l) {
        if (!m_listeners.contains(l)) {
            m_listeners.add(l);
        }
    }

    public void removeEventListener(ConnectionFactoryEventListener l) {
        m_listeners.remove(l);
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Constructor">
    /**
     * Creates a new instance of ConnectionFactory.
     */
    public ConnectionFactory() {
    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Object Registration">
    /**
     * to be closed at teardown.
     *
     * @param conn to track for close.
     */
    public void registerConnection(Connection conn) {
        registerConnection(conn, isRollbackConnectionBeforeClose());
    }

    /**
     * to be closed at teardown.
     *
     * @param conn to track for close.
     * @param rollback
     */
    public void registerConnection(Connection conn, boolean rollback) {
        m_connectionRegistrations.add(new ConnectionRegistration(conn, rollback));
    }

    /**
     * to be closed at teardown.
     *
     * @param stmt to track for close.
     */
    public void registerStatement(Statement stmt) {
        m_statements.add(stmt);
    }

    /**
     * to be closed at teardown.
     *
     * @param rs to track for close.
     */
    public void registerResultSet(ResultSet rs) {
        m_resultSets.add(rs);
    }

    public void registerArray(Array array) {
        m_arrays.add(array);
    }

    public void registerBlob(Blob blob) {
        m_blobs.add(blob);
    }

    public void registerClob(Clob clob) {
        m_clobs.add(clob);
    }

    public void registerSQLXML(SQLXML xml) {
        m_xmls.add(xml);
    }

    public void registerClosable(java.io.Closeable closable) {
    }

    public boolean isRollbackConnectionBeforeClose() {
        return PropertyGetter.getBooleanProperty(
                getClass().getName() + ".rollback.connection.before.close",
                false);
    }

    /**
     * closes all registered JDBC objects.
     */
    @SuppressWarnings("CallToThreadDumpStack")
    public void closeRegisteredObjects() {
        for (SQLXML xml : m_xmls) {
            if (xml != null) {
                try {
                    xml.free();
                } catch (Exception ex) {
                }
            }
        }
        m_xmls.clear();
        for (Array array : m_arrays) {
            if (array != null) {
                try {
                    array.free();
                } catch (Exception ex) {
                }
            }
        }
        m_arrays.clear();
        for (Blob blob : m_blobs) {
            if (blob != null) {
                try {
                    blob.free();
                } catch (Exception ex) {
                }
            }
        }
        m_blobs.clear();
        for (Clob clob : m_clobs) {
            if (clob != null) {
                try {
                    clob.free();
                } catch (Exception ex) {
                }
            }
        }
        m_clobs.clear();
        for (ResultSet rs : m_resultSets) {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception ex) {
                }
            }
        }
        m_resultSets.clear();
        for (Statement stmt : m_statements) {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception ex) {
                }
            }
        }
        m_statements.clear();
        for (ConnectionRegistration reg : m_connectionRegistrations) {
            final Connection conn = reg.getConnection();
            final boolean rollback = reg.isRollback();

            if (conn != null) {
                if (rollback) {
                    try {
                        conn.rollback();
                    } catch (Exception ex) {
                    }
                }
                try {
                    conn.close();
                } catch (Exception ex) {
                }
            }
        }

        m_connectionRegistrations.clear();

        final List<ConnectionFactoryEventListener> list = m_listeners;

        for (int i = 0; i < list.size(); i++) {
            final ConnectionFactoryEventListener l = list.get(i);

            try {
                l.finishedClosingRegisteredObjects(this);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }


    }
    // </editor-fold>

    // <editor-fold defaultstate="collapsed" desc="Object Creation">
    /**
     * with the specified driver, url, user and password. <p>
     *
     * @param driver fully qualified class name of a <tt>java.sql.Driver</tt>.
     * @param url of connection.
     * @param user of connection.
     * @param password of user.
     * @throws java.lang.Exception when the connection cannot be created.
     * @return a newly created and registered object.
     */
    public Connection newConnection(
            final String driver,
            final String url,
            final String user,
            final String password) throws Exception {
        // Not actually needed under JDBC4, as long as
        // a classpath jar has a compatible META-INF service
        // entry.  However, its not guaranteed that
        // every driver tested has a service entry...
        Class.forName(driver);

        final Connection conn = DriverManager.getConnection(
                url,
                user,
                password);

        registerConnection(conn);

        return conn;
    }

    /**
     * using the given connection. <p>
     *
     * The new statement is registered to be closed at teardown.
     *
     * @param conn with which to create.
     * @throws java.lang.Exception when the statement cannot be created.
     * @return a newly created and registered object.
     */
    public Statement createStatement(
            final Connection conn) throws Exception {
        final Statement stmt = conn.createStatement();

        registerStatement(stmt);

        return stmt;
    }

    public Blob createBlob(final Connection conn) throws Exception {
        final Blob blob = conn.createBlob();
        registerBlob(blob);
        return blob;
    }

    public Clob createClob(final Connection conn) throws Exception {
        final Clob clob = conn.createClob();
        registerClob(clob);
        return clob;
    }

    public NClob createNClob(final Connection conn) throws Exception {
        final NClob clob = conn.createNClob();
        registerClob(clob);
        return clob;
    }

    public SQLXML createSQLXML(final Connection conn) throws Exception {
        final SQLXML xml = conn.createSQLXML();
        registerSQLXML(xml);
        return xml;
    }

    /**
     * for the given <tt>sql</tt> using the given connection. <p>
     *
     * The new statement is registered to be closed at teardown.
     *
     * @param sql to prepare.
     * @param conn with which to prepare.
     * @throws java.lang.Exception when the statement cannot be prepared.
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
     * for the given <tt>sql</tt> using the given connection. <p>
     *
     * The new statement is registered to be closed at teardown.
     *
     * @param conn with which to prepare.
     * @throws java.lang.Exception when the call cannot be prepared.
     * @return the newly prepared and registered object.
     */
    public CallableStatement prepareCall(
            final String sql,
            final Connection conn) throws Exception {
        final CallableStatement stmt = conn.prepareCall(sql);

        registerStatement(stmt);

        return stmt;
    }

    /**
     * using the given <tt>sql</tt> and statement object.
     *
     * The returned <tt>ResultSet</tt> is registered to be closed at teardown.
     *
     * @param sql to execute.
     * @param stmt against which to execute.
     * @throws java.lang.Exception when execution fails.
     * @return the query result.
     */
    public ResultSet executeQuery(
            final String sql,
            final Statement stmt) throws Exception {
        final ResultSet rs = stmt.executeQuery(sql);

        registerResultSet(rs);

        return rs;
    }

    /**
     * using the given statement object.
     *
     * The returned <tt>ResultSet</tt> is registered to be closed at teardown.
     *
     * @param stmt to execute.
     * @throws java.lang.Exception when execution fails.
     * @return the query result.
     */
    public final ResultSet executeQuery(
            final PreparedStatement stmt) throws Exception {
        final ResultSet rs = stmt.executeQuery();

        registerResultSet(rs);

        return rs;
    }
    // </editor-fold>
}
