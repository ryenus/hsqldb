package org.hsqldb.testbase;

import java.sql.Connection;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Encapsulates plus an action to perform when it is closed via the
 * {@link #close()} method.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
public class ConnectionRegistration implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(ConnectionRegistration.class.getName());

    final Connection m_connection;
    final ConnectionCloseAction m_action;

    /**
     * Constructs a new instance with the given connection and action.
     *
     * @param connection a connection to a database
     * @param action     to perform before closing the connection.
     */
    ConnectionRegistration(final Connection connection,
            final ConnectionCloseAction action) {
        Objects.requireNonNull(connection, "connection must not be null");
        Objects.requireNonNull(action, "action must not be null");
        m_connection = connection;
        m_action = action;
    }

    /**
     * with which this instance was constructed.
     *
     * @return the connection object.
     */
    Connection getConnection() {
        return m_connection;
    }

    /**
     * with which this instance was constructed.
     *
     * @return the enumerated action
     */
    ConnectionCloseAction getCloseAction() {
        return m_action;
    }

    /**
     * the {@link #getConnection() connection}, first performing the
     * {@link #getCloseAction() close action}.
     */
    @Override
    @SuppressWarnings({"BroadCatchBlock", "TooBroadCatch"})
    public void close() {
        try {
            switch (m_action) {
                case Rollback:
                    m_connection.rollback();
                    break;
                case Commit:
                    m_connection.commit();
                    break;
                case None:
                default:
                    break;
            }
        } catch (Throwable t) {
            LOG.log(Level.WARNING, null, t);
        }

        try {
            m_connection.close();
        } catch (Throwable t) {
            LOG.log(Level.WARNING, null, t);
        }
    }
}
