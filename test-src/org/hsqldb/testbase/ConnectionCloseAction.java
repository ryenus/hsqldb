package org.hsqldb.testbase;

/**
 * Denotes the action performed when closing a registered connection.
 */
public enum ConnectionCloseAction {
    /**
     * Perform a commit before close.
     */
    Commit,
    /**
     * Perform a rollback before close.
     */
    Rollback,
    /**
     * Do not perform a commit or a rollback before close.
     */
    None

}
