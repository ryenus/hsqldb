/* Copyright (c) 2001-2009, The HSQL Development Group
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.hsqldb.ErrorCode;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;

/**
 * Base class for HSQLDB's implementations of java.sql.Statement and
 * java.sql.PreparedStatement. Contains common members and methods.
 *
 * @author fredt@usrs
 * @version 1.9.0
 * @since 1.9.0
 */
class StatementBase {

    /**
     * Whether this Statement has been explicitly closed.  A JDBCConnection
     * object now explicitly closes all of its open jdbcXXXStatement objects
     * when it is closed.
     */
    volatile boolean isClosed;

    /** Is escape processing enabled? */
    protected boolean isEscapeProcessing = true;

    /** The connection used to execute this statement. */
    protected JDBCConnection connection;

    /** The maximum number of rows to generate when executing this statement. */
    protected int maxRows;

    /** The number of rows returned in a chunk. */
    protected int fetchSize = 0;

    /** Direction of results fetched. */
    protected int fetchDirection = JDBCResultSet.FETCH_FORWARD;

    /** The result of executing this statement. */
    protected Result resultIn;

    /** Any error returned from a batch execute. */
    protected Result errorResult;

    /** The currently existing generated key Result */
    protected Result generatedResult;

    /** The result set type obtained by executing this statement. */
    protected int rsScrollability = JDBCResultSet.TYPE_FORWARD_ONLY;

    /** The result set concurrency obtained by executing this statement. */
    protected int rsConcurrency = JDBCResultSet.CONCUR_READ_ONLY;

    /** The result set holdability obtained by executing this statement. */
    protected int rsHoldability = JDBCResultSet.HOLD_CURSORS_OVER_COMMIT;

    /** Used by this statement to communicate non-batched requests. */
    protected Result resultOut;

    /** Used by this statement to communicate batched execution requests */
    protected Result batchResultOut;

    /** The currently existing ResultSet object */
    protected JDBCResultSet currentResultSet;

    /** The currently existing ResultSet object for generated keys */
    protected JDBCResultSet generatedResultSet;

    /** The first warning in the chain. Null if there are no warnings. */
    protected SQLWarning rootWarning;

    /** Implementation in subclasses **/
    public synchronized void close() throws SQLException {}

    /**
     * An internal check for closed statements.
     *
     * @throws SQLException when the connection is closed
     */
    void checkClosed() throws SQLException {

        if (isClosed) {
            throw Util.sqlException(ErrorCode.X_07501);
        }

        if (connection.isClosed) {
            close();
            throw Util.sqlException(ErrorCode.X_07501);
        }
    }

    /**
     * processes chained warnings and any generated columns result set
     */
    void performPostExecute() throws SQLException {

        resultOut.clearLobResults();

        generatedResult = null;

        if (resultIn == null) {
            return;
        }

        Result current = resultIn;

        while (current.getChainedResult() != null) {
            current = current.getUnlinkChainedResult();

            if (current.getType() == ResultConstants.WARNING) {
                SQLWarning w = Util.sqlWarning(current);

                if (rootWarning == null) {
                    rootWarning = w;
                } else {
                    rootWarning.setNextWarning(w);
                }
            } else if (current.getType() == ResultConstants.ERROR) {
                errorResult = current;
            } else if (current.getType() == ResultConstants.DATA) {
                generatedResult = current;
            }
        }

        if (resultIn.isData()) {
            currentResultSet = new JDBCResultSet(connection.sessionProxy,
                    (Statement) this, resultIn, resultIn.metaData,
                    connection.connProperties, connection.isNetConn);
        }
    }

    ResultSet getGeneratedResultSet() throws SQLException {

        if (generatedResultSet != null) {
            generatedResultSet.close();
        }

        if (generatedResult == null) {
            generatedResult = Result.emptyGeneratedResult;
        }
        generatedResultSet = new JDBCResultSet(connection.sessionProxy, null,
                generatedResult, generatedResult.metaData,
                connection.connProperties, connection.isNetConn);

        return generatedResultSet;
    }

    void clearResultData() throws SQLException {

        if (currentResultSet != null) {
            currentResultSet.close();
        }

        if (generatedResultSet != null) {
            generatedResultSet.close();
        }
        generatedResultSet = null;
        generatedResult    = null;
        resultIn           = null;
    }
}
