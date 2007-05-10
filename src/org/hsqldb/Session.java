/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2007, The HSQL Development Group
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


package org.hsqldb;

import java.io.DataInput;
import java.sql.Date;
import java.sql.Timestamp;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.jdbc.jdbcConnection;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.SimpleLog;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.navigator.ClientRowSetNavigator;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.result.ResultLob;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.TimeData;
import org.hsqldb.rights.User;
import org.hsqldb.rights.Grantee;

// fredt@users 20020320 - doc 1.7.0 - update
// fredt@users 20020315 - patch 1.7.0 - switch for scripting
// fredt@users 20020130 - patch 476694 by velichko - transaction savepoints
// additions in different parts to support savepoint transactions
// fredt@users 20020910 - patch 1.7.1 by fredt - database readonly enforcement
// fredt@users 20020912 - patch 1.7.1 by fredt - permanent internal connection
// boucherb@users 20030512 - patch 1.7.2 - compiled statements
//                                       - session becomes execution hub
// boucherb@users 20050510 - patch 1.7.2 - generalized Result packet passing
//                                         based command execution
//                                       - batch execution handling
// fredt@users 20030628 - patch 1.7.2 - session proxy support
// fredt@users 20040509 - patch 1.7.2 - SQL conformance for CURRENT_TIMESTAMP and other datetime functions

/**
 *  Implementation of a user session with the database. In 1.7.2, Session
 *  becomes the public interface to an HSQLDB database, accessed locally or
 *  remotely via SessionInterface.
 *
 *  When a Session is closed, all references to internal engine objects are
 *  set to null. But the session id and scripting mode may still be used for
 *  scripting
 *
 * New class based on original Hypersonic code.<p>
 * Extensively rewritten and extended in successive versions of HSQLDB.
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 1.8.0
 * @since 1.7.0
 */
public class Session implements SessionInterface {

    //
    private volatile boolean isAutoCommit;
    private volatile boolean isReadOnly;
    private volatile boolean isClosed;

    //
    public Database database;
    private User    user;

    // transaction support
    int             isolationMode = SessionInterface.TX_READ_COMMITTED;
    int             actionIndex;
    long            actionTimestamp;
    long            transactionTimestamp;
    private boolean isNestedTransaction;
    HsqlArrayList   rowActionList;
    boolean         rollbackNestedTransaction;
    HashMappedList  savepoints;

    //
    private int           currentMaxRows;
    private int           sessionMaxRows;
    private Number        lastIdentity = ValuePool.getInt(0);
    private final long    sessionId;
    private boolean       script;
    private CommandParser parser;

    //
    private jdbcConnection intConnection;

    // schema
    public HsqlName  currentSchema;
    public HsqlName  loggedSchema;
    private HsqlName oldSchema;

    // query processing
    boolean                          isProcessingScript;
    boolean                          isProcessingLog;
    public CompiledStatementExecutor compiledStatementExecutor;
    boolean                          isNetwork;
    int                              resultMaxMemoryRows;
    public SessionData               sessionData;

    /** @todo fredt - clarify in which circumstances Session has to disconnect */
    Session getSession() {
        return this;
    }

    /**
     * Constructs a new Session object.
     *
     * @param  db the database to which this represents a connection
     * @param  user the initial user
     * @param  autocommit the initial autocommit value
     * @param  readonly the initial readonly value
     * @param  id the session identifier, as known to the database
     */
    Session(Database db, User user, boolean autocommit, boolean readonly,
            long id) {

        sessionId                 = id;
        database                  = db;
        this.user                 = user;
        rowActionList             = new HsqlArrayList(true);
        savepoints                = new HashMappedList(4);
        isAutoCommit              = autocommit;
        isReadOnly                = readonly;
        compiledStatementExecutor = new CompiledStatementExecutor(this);
        parser                    = new CommandParser(this, new Tokenizer());
        resultMaxMemoryRows       = database.getResultMaxMemoryRows();

        resetSchema();

        sessionData = new SessionData(database, this);
    }

    void resetSchema() {

        HsqlName initialSchema = user.getInitialSchema();

        currentSchema = initialSchema == null
                        ? database.schemaManager.getDefaultSchemaHsqlName()
                        : initialSchema;
    }

    /**
     *  Retrieves the session identifier for this Session.
     *
     * @return the session identifier for this Session
     */
    public long getId() {
        return sessionId;
    }

    /**
     * Closes this Session.
     */
    public void close() {

        if (isClosed) {
            return;
        }

        synchronized (database) {

            // test again inside block
            if (isClosed) {
                return;
            }

            database.sessionManager.removeSession(this);
            rollback();

            try {
                database.logger.writeToLog(this, Token.T_DISCONNECT);
            } catch (HsqlException e) {}

            sessionData.clearIndexRoots();
            sessionData.clearIndexRootsKeep();
            sessionData.closeAllNavigators();
            sessionData.closeResultCache();
            database.compiledStatementManager.removeSession(sessionId);
            database.closeIfLast();

            database                  = null;
            user                      = null;
            rowActionList             = null;
            savepoints                = null;
            intConnection             = null;
            compiledStatementExecutor = null;
            lastIdentity              = null;
            isClosed                  = true;
        }
    }

    /**
     * Retrieves whether this Session is closed.
     *
     * @return true if this Session is closed
     */
    public boolean isClosed() {
        return isClosed;
    }

    public void setIsolation(int level) throws HsqlException {
        isolationMode = level;
    }

    public int getIsolation() throws HsqlException {
        return isolationMode;
    }

    /**
     * Setter for iLastIdentity attribute.
     *
     * @param  i the new value
     */
    void setLastIdentity(Number i) {
        lastIdentity = i;
    }

    /**
     * Getter for iLastIdentity attribute.
     *
     * @return the current value
     */
    public Object getLastIdentity() {
        return lastIdentity;
    }

    /**
     * Retrieves the Database instance to which this
     * Session represents a connection.
     *
     * @return the Database object to which this Session is connected
     */
    public Database getDatabase() {
        return database;
    }

    /**
     * Retrieves the name, as known to the database, of the
     * user currently controlling this Session.
     *
     * @return the name of the user currently connected within this Session
     */
    public String getUsername() {
        return user.getName();
    }

    /**
     * Retrieves the User object representing the user currently controlling
     * this Session.
     *
     * @return this Session's User object
     */
    public User getUser() {
        return user;
    }

    public User getGrantee() {
        return user;
    }

    /**
     * Sets this Session's User object to the one specified by the
     * user argument.
     *
     * @param  user the new User object for this session
     */
    void setUser(User user) {
        this.user = user;
    }

    int getMaxRows() {
        return currentMaxRows;
    }

    public int getSQLMaxRows() {
        return sessionMaxRows;
    }

    /**
     * The SQL command SET MAXROWS n will override the Statement.setMaxRows(n)
     * until SET MAXROWS 0 is issued.
     *
     * NB this is dedicated to the SET MAXROWS sql statement and should not
     * otherwise be called. (fredt@users)
     */
    void setSQLMaxRows(int rows) {
        currentMaxRows = sessionMaxRows = rows;
    }

    /**
     * Checks whether this Session's current User has the privileges of
     * the ADMIN role.
     *
     * @throws HsqlException if this Session's User does not have the
     *      privileges of the ADMIN role.
     */
    void checkAdmin() throws HsqlException {
        user.checkAdmin();
    }

    /**
     * This is used for reading - writing to existing tables.
     * @throws  HsqlException
     */
    void checkReadWrite() throws HsqlException {

        if (isReadOnly) {
            throw Trace.error(Trace.DATABASE_IS_READONLY);
        }
    }

    /**
     * This is used for creating new database objects such as tables.
     * @throws  HsqlException
     */
    void checkDDLWrite() throws HsqlException {

        if (database.isFilesReadOnly()) {
            throw Trace.error(Trace.DATABASE_IS_READONLY);
        }

        checkReadWrite();
    }

    /**
     *  Adds a delete ation to the action list.
     *
     * @param  table the table of the row
     * @param  row the deleted row
     * @throws  HsqlException
     */
    void addDeleteAction(Table table, Row row) throws HsqlException {

        Transaction t = new Transaction(true, table, row, actionTimestamp);

        rowActionList.add(t);
        database.txManager.addTransaction(this, t);
    }

    /**
     *  Adds a row insert action to the action list.
     *
     * @param  table the table into which the row was inserted
     * @param  row the inserted row
     * @throws  HsqlException
     */
    void addInsertAction(Table table, Row row) throws HsqlException {

        Transaction t = new Transaction(false, table, row, actionTimestamp);

        rowActionList.add(t);
        database.txManager.addTransaction(this, t);
    }

    /**
     *  Setter for the autocommit attribute.
     *
     * @param  autocommit the new value
     * @throws  HsqlException
     */
    public void setAutoCommit(boolean autocommit) {

        if (isClosed) {
            return;
        }

        synchronized (database) {
            if (autocommit != isAutoCommit) {
                commit();

                isAutoCommit = autocommit;
            }
        }
    }

    public void beginAction() throws HsqlException {
        actionIndex = rowActionList.size();
    }

    public void endAction(Result r) throws HsqlException {

        if (r.isError()) {
            database.txManager.rollbackNested(this);
        }

        if (isAutoCommit) {
            commit();
        }
    }

    public void startPhasedTransaction() throws HsqlException {}

    public void prepareCommit() throws HsqlException {}

    /**
     * Commits any uncommited transaction this Session may have open
     *
     * @throws  HsqlException
     */
    public void commit() {

        if (isClosed) {
            return;
        }

        synchronized (database) {
            if (!rowActionList.isEmpty()) {
                try {
                    database.logger.writeCommitStatement(this);
                } catch (HsqlException e) {}
            }

            database.txManager.commit(this);
            sessionData.clearIndexRoots();
        }
    }

    /**
     * Rolls back any uncommited transaction this Session may have open.
     *
     * @throws  HsqlException
     */
    public void rollback() {

        if (isClosed) {
            return;
        }

        synchronized (database) {
            if (rowActionList.size() != 0) {
                try {
                    database.logger.writeToLog(this, Token.T_ROLLBACK);
                } catch (HsqlException e) {}
            }

            database.txManager.rollback(this);
            sessionData.clearIndexRoots();
        }
    }

    /**
     * No-op in this implementation
     */
    public void resetSession() throws HsqlException {
        throw new HsqlException("", "", 0);
    }

    /**
     *  Registers a transaction SAVEPOINT. A new SAVEPOINT with the
     *  name of an existing one replaces the old SAVEPOINT.
     *
     * @param  name name of the savepoint
     * @throws  HsqlException if there is no current transaction
     */
    public void savepoint(String name) throws HsqlException {

        savepoints.remove(name);
        savepoints.add(name, ValuePool.getInt(rowActionList.size()));

        try {
            database.logger.writeToLog(this,
                                       DatabaseScript.getSavepointDDL(name));
        } catch (HsqlException e) {}
    }

    /**
     *  Performs a partial transaction ROLLBACK to savepoint.
     *
     * @param  name name of savepoint
     * @throws  HsqlException
     */
    public void rollbackToSavepoint(String name) throws HsqlException {

        if (isClosed) {
            return;
        }

        try {
            database.logger.writeToLog(
                this, DatabaseScript.getSavepointRollbackDDL(name));
        } catch (HsqlException e) {}

        database.txManager.rollbackSavepoint(this, name);
    }

    /**
     * Releases a savepoint
     *
     * @param  name name of savepoint
     * @throws  HsqlException if name does not correspond to a savepoint
     */
    public void releaseSavepoint(String name) throws HsqlException {

        // remove this and all later savepoints
        int index = savepoints.getIndex(name);

        Trace.check(index >= 0, Trace.SAVEPOINT_NOT_FOUND, name);

        while (savepoints.size() > index) {
            savepoints.remove(savepoints.size() - 1);
        }
    }

    /**
     * Setter for readonly attribute.
     *
     * @param  readonly the new value
     */
    public void setReadOnly(boolean readonly) throws HsqlException {

        if (!readonly && database.databaseReadOnly) {
            throw Trace.error(Trace.DATABASE_IS_READONLY);
        }

        isReadOnly = readonly;
    }

    /**
     *  Getter for readonly attribute.
     *
     * @return the current value
     */
    public boolean isReadOnly() {
        return isReadOnly;
    }

    /**
     *  Getter for autoCommit attribute.
     *
     * @return the current value
     */
    public boolean isAutoCommit() {
        return isAutoCommit;
    }

    /**
     *  A switch to set scripting on the basis of type of statement executed.
     *  Afterwards the method reponsible for logging uses
     *  isScripting() to determine if logging is required for the executed
     *  statement. (fredt@users)
     *
     * @param  script The new scripting value
     */
    void setScripting(boolean script) {
        this.script = script;
    }

    /**
     * Getter for scripting attribute.
     *
     * @return  scripting for the last statement.
     */
    boolean isScripting() {
        return script;
    }

    /**
     * Retrieves an internal Connection object equivalent to the one
     * that created this Session.
     *
     * @return  internal connection.
     */
    jdbcConnection getInternalConnection() throws HsqlException {

        if (intConnection == null) {
            intConnection = new jdbcConnection(this);
        }

        return intConnection;
    }

// boucherb@users 20020810 metadata 1.7.2
//----------------------------------------------------------------
    private final long connectTime = System.currentTimeMillis();

// more effecient for MetaData concerns than checkAdmin

    /**
     * Getter for admin attribute.
     *
     * @ return the current value
     */
    public boolean isAdmin() {
        return user.isAdmin();
    }

    /**
     * Getter for connectTime attribute.
     *
     * @return the value
     */
    public long getConnectTime() {
        return connectTime;
    }

    /**
     * Getter for transactionSise attribute.
     *
     * @return the current value
     */
    public int getTransactionSize() {
        return rowActionList.size();
    }

    CompiledStatement compileStatement(String sql) throws HsqlException {

        parser.reset(sql);

        CompiledStatement cs = parser.compileStatement(currentSchema);

        cs.sql = sql;

        return cs;
    }

    /**
     * Executes the command encapsulated by the cmd argument.
     *
     * @param cmd the command to execute
     * @return the result of executing the command
     */
    public Result execute(Result cmd) {

        try {
            if (isClosed) {
                Trace.check(false, Trace.ACCESS_IS_DENIED,
                            Trace.getMessage(Trace.Session_execute));
            }
        } catch (Throwable t) {
            return Result.newErrorResult(t, null);
        }

        synchronized (database) {
            int type = cmd.getType();

            if (sessionMaxRows == 0) {
                currentMaxRows = cmd.getUpdateCount();
            }

            // we simply get the next system change number - no matter what type of query
            actionTimestamp = database.txManager.nextActionTimestamp();

            JavaSystem.gc();

            switch (type) {

                case ResultConstants.LARGE_OBJECT_OP : {
                    return performLOBOperation((ResultLob) cmd);
                }
                case ResultConstants.EXECUTE : {
                    Result result = executeCompiledStatement(cmd);

                    result = performPostExecute(cmd, result);

                    return result;
                }
                case ResultConstants.BATCHEXECUTE : {
                    Result result = executeCompiledBatchStatement(cmd);

                    result = performPostExecute(cmd, result);

                    return result;
                }
                case ResultConstants.EXECDIRECT : {
                    Result result =
                        executeDirectStatement(cmd.getMainString());

                    result = performPostExecute(cmd, result);

                    return result;
                }
                case ResultConstants.BATCHEXECDIRECT : {
                    Result result = executeDirectBatchStatement(cmd);

                    result = performPostExecute(cmd, result);

                    return result;
                }
                case ResultConstants.PREPARE : {
                    CompiledStatement cs;

                    try {
                        cs = database.compiledStatementManager.compile(this,
                                cmd.getMainString());
                    } catch (Throwable t) {
                        return Result.newErrorResult(t, cmd.getMainString());
                    }

                    cs.setGeneratedColumnInfo(
                        cmd.getGeneratedResultType(),
                        cmd.getGeneratedResultMetaData());

                    ResultMetaData rmd = cs.getResultMetaData();
                    ResultMetaData pmd = cs.getParametersMetaData();

                    return Result.newPrepareResponse(cs.id, cs.type, rmd, pmd);
                }
                case ResultConstants.CLOSE_RESULT : {
                    closeNavigator(cmd.getResultId());

                    return Result.updateZeroResult;
                }
                case ResultConstants.FREESTMT : {
                    database.compiledStatementManager.freeStatement(
                        cmd.getStatementID(), sessionId, false);

                    return Result.updateZeroResult;
                }
                case ResultConstants.GETSESSIONATTR : {
                    return getAttributes();
                }
                case ResultConstants.SETSESSIONATTR : {
                    return setAttributes(cmd);
                }
                case ResultConstants.ENDTRAN : {
                    switch (cmd.getEndTranType()) {

                        case ResultConstants.COMMIT :
                            commit();
                            break;

                        case ResultConstants.ROLLBACK :
                            rollback();
                            break;

                        case ResultConstants.SAVEPOINT_NAME_RELEASE :
                            try {
                                String name = cmd.getMainString();

                                releaseSavepoint(name);
                            } catch (Throwable t) {
                                return Result.newErrorResult(t, null);
                            }
                            break;

                        case ResultConstants.SAVEPOINT_NAME_ROLLBACK :
                            try {
                                rollbackToSavepoint(cmd.getMainString());
                            } catch (Throwable t) {
                                return Result.newErrorResult(t, null);
                            }
                            break;

                        // not yet
                        // case ResultConstants.COMMIT_AND_CHAIN :
                        // case ResultConstants.ROLLBACK_AND_CHAIN :
                    }

                    return Result.updateZeroResult;
                }
                case ResultConstants.SETCONNECTATTR : {
                    switch (cmd.getConnectionAttrType()) {

                        case ResultConstants.SQL_ATTR_SAVEPOINT_NAME :
                            try {
                                savepoint(cmd.getMainString());
                            } catch (Throwable t) {
                                return Result.newErrorResult(t, null);
                            }

                        // case ResultConstants.SQL_ATTR_AUTO_IPD
                        //   - always true
                        // default: throw - case never happens
                    }

                    return Result.updateZeroResult;
                }
                case ResultConstants.REQUESTDATA : {
                    return sessionData.getDataResultSlice(cmd.getResultId(),
                                                          cmd.getUpdateCount(),
                                                          cmd.getFetchSize());
                }
                case ResultConstants.DISCONNECT : {
                    close();

                    return Result.updateZeroResult;
                }
                default : {
                    return Result.newErrorResult(
                        Trace.runtimeError(
                            Trace.UNSUPPORTED_INTERNAL_OPERATION,
                            "Session.execute()"), null);
                }
            }
        }
    }

    private Result performPostExecute(Result command, Result result) {

        try {
            if (database != null) {
                database.schemaManager.logSequences(this, database.logger);

                if (isAutoCommit) {
                    database.logger.synchLog();
                }
            }

            if (result.isData()) {
                sessionData.getDataResultHead(command, result, isNetwork);
            }

            return result;
        } catch (Exception e) {
            return Result.newErrorResult(e, null);
        } finally {
            if (database != null && database.logger.needsCheckpoint()) {
                try {
                    database.logger.checkpoint(false);
                } catch (HsqlException e) {
                    database.logger.appLog.logContext(
                        SimpleLog.LOG_ERROR, "checkpoint did not complete");
                }
            }
        }
    }

    public ClientRowSetNavigator getRows(long navigatorId, int offset,
                                         int blockSize) throws HsqlException {
        return sessionData.getRowSetSlice(navigatorId, offset, blockSize);
    }

    public void closeNavigator(long id) {
        sessionData.closeNavigator(id);
    }

/*
    public Result sqlExecuteDirectNoPreChecks(String sql) {

        synchronized (database) {
            return dbCommandInterpreter.execute(sql);
        }
    }
*/
    public Result executeDirectStatement(String sql) {

        synchronized (database) {
            return parser.execute(sql);
        }
    }

    Result executeCompiledStatement(CompiledStatement cs, Object[] pvals) {

        Result r;

        try {
            beginAction();
        } catch (HsqlException e) {
            return Result.newErrorResult(e, null);
        }

//        tempActionHistory.add("sql execute " + cs.sql + " " + actionTimestamp + " " + rowActionList.size());
        r = compiledStatementExecutor.execute(cs, pvals);

//        tempActionHistory.add("sql execute end " + actionTimestamp + " " + rowActionList.size());
        try {
            endAction(r);
        } catch (HsqlException e) {
            return Result.newErrorResult(e, null);
        }

        return r;
    }

    private Result executeCompiledBatchStatement(Result cmd) {

        int               csid;
        CompiledStatement cs;
        int[]             updateCounts;
        int               count;

        csid = cmd.getStatementID();
        cs   = database.compiledStatementManager.getStatement(this, csid);

        if (cs == null) {

            // invalid sql has been removed already
            return Result.newErrorResult(
                Trace.runtimeError(Trace.INVALID_PREPARED_STATEMENT, null),
                null);
        }

        count = 0;

        RowSetNavigator nav = cmd.initialiseNavigator();

        updateCounts = new int[nav.getSize()];

        Result generatedResult = null;

        if (cs.hasGeneratedColumns()) {
            generatedResult = Result.newDataResult(cs.generatedResultMetaData);
        }

        Result error = null;

        while (nav.hasNext()) {
            Object[] pvals = (Object[]) nav.getNext();
            Result   in    = executeCompiledStatement(cs, pvals);

            // On the client side, iterate over the vals and throw
            // a BatchUpdateException if a batch status value of
            // esultConstants.EXECUTE_FAILED is encountered in the result
            if (in.isUpdateCount()) {
                if (cs.hasGeneratedColumns()) {
                    Object generatedRow = in.getNavigator().getNext();

                    generatedResult.getNavigator().add(generatedRow);
                }

                updateCounts[count++] = in.getUpdateCount();
            } else if (in.isData()) {

                // FIXME:  we don't have what it takes yet
                // to differentiate between things like
                // stored procedure calls to methods with
                // void return type and select statements with
                // a single row/column containg null
                updateCounts[count++] = ResultConstants.SUCCESS_NO_INFO;
            } else if (in.isError()) {
                updateCounts = ArrayUtil.arraySlice(updateCounts, 0, count);
                error        = in;

                break;
            } else {
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Session");
            }
        }

        return Result.newBatchedExecuteResponse(updateCounts, generatedResult,
                error);
    }

    private Result executeDirectBatchStatement(Result cmd) {

        int[] updateCounts;
        int   count;

        count = 0;

        RowSetNavigator nav = cmd.initialiseNavigator();

        updateCounts = new int[nav.getSize()];

        Result error = null;

        while (nav.hasNext()) {
            Result   in;
            Object[] data = (Object[]) nav.getNext();
            String   sql  = (String) data[0];

            try {
                in = executeDirectStatement(sql);
            } catch (Throwable t) {
                in = Result.newErrorResult(t, null);

                // if (t instanceof OutOfMemoryError) {
                // System.gc();
                // }
                // "in" alread equals "err"
                // maybe test for OOME and do a gc() ?
                // t.printStackTrace();
            }

            if (in.isUpdateCount()) {
                updateCounts[count++] = in.getUpdateCount();
            } else if (in.isData()) {

                // FIXME:  we don't have what it takes yet
                // to differentiate between things like
                // stored procedure calls to methods with
                // void return type and select statements with
                // a single row/column containg null
                updateCounts[count++] = ResultConstants.SUCCESS_NO_INFO;
            } else if (in.isError()) {
                updateCounts = ArrayUtil.arraySlice(updateCounts, 0, count);
                error        = in;

                break;
            } else {
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Session");
            }
        }

        return Result.newBatchedExecuteResponse(updateCounts, null, error);
    }

    /**
     * Retrieves the result of executing the prepared statement whose csid
     * and parameter values/types are encapsulated by the cmd argument.
     *
     * @return the result of executing the statement
     */
    private Result executeCompiledStatement(Result cmd) {

        int csid = cmd.getStatementID();
        CompiledStatement cs =
            database.compiledStatementManager.getStatement(this, csid);

        if (cs == null) {

            // invalid sql has been removed already
            return Result.newErrorResult(
                Trace.runtimeError(Trace.INVALID_PREPARED_STATEMENT, null),
                null);
        }

        Object[] pvals = cmd.getParameterData();

        return executeCompiledStatement(cs, pvals);
    }

// session DATETIME functions
    long              currentDateTimeSCN;
    long              currentMillis;
    private Date      currentDate;
    private TimeData  currentTime;
    private Timestamp currentTimestamp;

    /**
     * Returns the current date, unchanged for the duration of the current
     * execution unit (statement).<p>
     *
     * SQL standards require that CURRENT_DATE, CURRENT_TIME and
     * CURRENT_TIMESTAMP are all evaluated at the same point of
     * time in the duration of each SQL statement, no matter how long the
     * SQL statement takes to complete.<p>
     *
     * When this method or a corresponding method for CURRENT_TIME or
     * CURRENT_TIMESTAMP is first called in the scope of a system change
     * number, currentMillis is set to the current system time. All further
     * CURRENT_XXXX calls in this scope will use this millisecond value.
     * (fredt@users)
     */
    public Date getCurrentDate() {

        if (currentDateTimeSCN != actionTimestamp) {
            currentDateTimeSCN = actionTimestamp;
            currentMillis      = System.currentTimeMillis();
            currentDate        = HsqlDateTime.getCurrentDate(currentMillis);
            currentTime        = null;
            currentTimestamp   = null;
        } else if (currentDate == null) {
            currentDate = HsqlDateTime.getCurrentDate(currentMillis);
        }

        return currentDate;
    }

    /**
     * Returns the current time, unchanged for the duration of the current
     * execution unit (statement)
     */
    TimeData getCurrentTime() {

        if (currentDateTimeSCN != actionTimestamp) {
            currentDateTimeSCN = actionTimestamp;
            currentMillis      = System.currentTimeMillis();
            currentDate        = null;
            currentTime =
                new TimeData(HsqlDateTime.getNormalisedTime(currentMillis));
            currentTimestamp = null;
        } else if (currentTime == null) {
            currentTime =
                new TimeData(HsqlDateTime.getNormalisedTime(currentMillis));
        }

        return currentTime;
    }

    /**
     * Returns the current timestamp, unchanged for the duration of the current
     * execution unit (statement)
     */
    Timestamp getCurrentTimestamp() {

        if (currentDateTimeSCN != actionTimestamp) {
            currentDateTimeSCN = actionTimestamp;
            currentMillis      = System.currentTimeMillis();
            currentDate        = null;
            currentTime        = null;
            currentTimestamp   = HsqlDateTime.getTimestamp(currentMillis);
        } else if (currentTimestamp == null) {
            currentTimestamp = HsqlDateTime.getTimestamp(currentMillis);
        }

        return currentTimestamp;
    }

    Result getAttributes() {

        Result   r    = Result.newSessionAttributesResult();
        Object[] data = new Object[] {
            database.getURI(), getUsername(), ValuePool.getLong(sessionId),
            ValuePool.getInt(isolationMode),
            ValuePool.getBoolean(isAutoCommit),
            ValuePool.getBoolean(database.databaseReadOnly),
            ValuePool.getBoolean(isReadOnly)
        };

        r.setSessionAttributes(data);

        return r;
    }

    Result setAttributes(Result r) {

        Object[] row = r.getSessionAttributes();

        for (int i = 0; i < row.length; i++) {
            Object value = row[i];

            if (value == null) {
                continue;
            }

            try {
                switch (i) {

                    case SessionInterface.INFO_AUTOCOMMIT : {
                        this.setAutoCommit(((Boolean) value).booleanValue());

                        break;
                    }
                    case SessionInterface.INFO_CONNECTION_READONLY :
                        this.setReadOnly(((Boolean) value).booleanValue());
                        break;
                }
            } catch (HsqlException e) {
                return Result.newErrorResult(e, null);
            }
        }

        return Result.updateZeroResult;
    }

    Result performLOBOperation(ResultLob cmd) {

        long id        = cmd.getLobID();
        int  operation = cmd.getSubType();

        switch (operation) {

            case ResultLob.LobResultTypes.REQUEST_CREATE_BYTES :
                return ResultLob.newLobCreateBlobResponse(id);

            case ResultLob.LobResultTypes.REQUEST_CREATE_CHARS :
                return ResultLob.newLobCreateClobResponse(id);
        }

        Object lob = database.lobManager.getLob(id);

        if (lob == null) {
            return Result.newErrorResult(
                Trace.error(Trace.BLOB_IS_NO_LONGER_VALID), null);
        }

        switch (operation) {

            case ResultLob.LobResultTypes.REQUEST_OPEN :
            case ResultLob.LobResultTypes.REQUEST_CLOSE : {
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Session");
            }
            case ResultLob.LobResultTypes.REQUEST_GET_BYTES : {
                try {
                    byte[] bytes = ((BlobData) lob).getBytes(cmd.getOffset(),
                        (int) cmd.getBlockLength());

                    return ResultLob.newLobGetBytesResponse(id,
                            cmd.getOffset(), bytes);
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, "blob operation");
                }
            }
            case ResultLob.LobResultTypes.REQUEST_SET_BYTES : {
                try {
                    ((BlobData) lob).setBytes(cmd.getOffset(),
                                              cmd.getByteArray());

                    return ResultLob.newLobSetBytesResponse(id);
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, null);
                }
            }
            case ResultLob.LobResultTypes.REQUEST_GET_CHARS : {
                try {
                    char[] chars = ((ClobData) lob).getChars(cmd.getOffset(),
                        (int) cmd.getBlockLength());

                    return ResultLob.newLobGetCharsResponse(id,
                            cmd.getOffset(), chars);
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, "blob operation");
                }
            }
            case ResultLob.LobResultTypes.REQUEST_SET_CHARS : {
                try {
                    char[] chars = cmd.getCharArray();

                    ((ClobData) lob).setChars(cmd.getOffset(), chars, 0,
                                              chars.length);

                    return ResultLob.newLobSetCharsResponse(id);
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, null);
                }
            }
            case ResultLob.LobResultTypes.REQUEST_GET_BYTE_PATTERN_POSITION :
            case ResultLob.LobResultTypes.REQUEST_GET_CHAR_PATTERN_POSITION : {
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Session");
            }
            default : {
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Session");
            }
        }
    }

    // DatabaseMetaData.getURL should work as specified for
    // internal connections too.
    public String getInternalConnectionURL() {
        return DatabaseURL.S_URL_PREFIX + database.getURI();
    }

    boolean isProcessingScript() {
        return isProcessingScript;
    }

    boolean isProcessingLog() {
        return isProcessingLog;
    }

    // schema processing
    boolean isSchemaDefintion() {
        return oldSchema != null;
    }

    void startSchemaDefinition(String schema) throws HsqlException {

        if (isProcessingScript) {
            setSchema(schema);

            return;
        }

        oldSchema = currentSchema;

        setSchema(schema);
    }

    void endSchemaDefinition() throws HsqlException {

        if (oldSchema == null) {
            return;
        }

        currentSchema = oldSchema;
        oldSchema     = null;

        database.logger.writeToLog(this,
                                   DatabaseScript.getSetSchemaDDL(database,
                                       currentSchema));
    }

    // schema object methods
    public void setSchema(String schema) throws HsqlException {
        currentSchema = database.schemaManager.getSchemaHsqlName(schema);
    }

    /**
     * If schemaName is null, return the current schema name, else return
     * the HsqlName object for the schema. If schemaName does not exist,
     * throw.
     */
    HsqlName getSchemaHsqlName(String name) throws HsqlException {
        return name == null ? currentSchema
                            : database.schemaManager.getSchemaHsqlName(name);
    }

    /**
     * Same as above, but return string
     */
    public String getSchemaName(String name) throws HsqlException {
        return name == null ? currentSchema.name
                            : database.schemaManager.getSchemaName(name);
    }

    public HsqlName getCurrentSchemaHsqlName() {
        return currentSchema;
    }

// session tables
    Table[] transitionTables = new Table[0];

    public void setSessionTables(Table[] tables) {
        transitionTables = tables;
    }

    public Table findSessionTable(String name) {

        for (int i = 0; i < transitionTables.length; i++) {
            if (name.equals(transitionTables[i].getName().name)) {
                return transitionTables[i];
            }
        }

        return null;
    }

//
    public int getResultMemoryRowCount() {
        return resultMaxMemoryRows;
    }

    public void setResultMemoryRowCount(int count) {

        if (database.getTempDirectoryPath() != null) {
            resultMaxMemoryRows = count;
        }
    }

    // warnings

    /**
     * Placeholder for warnings
     */
    public void setWarning(int i) {}

    public synchronized long getLobId() {
        return database.lobManager.getNewLobId();
    }

    public void registerResultLobs(Result result) throws HsqlException {
        sessionData.registerLobForResult(result);
    }

    public void allocateResultLob(ResultLob result,
                                  DataInput dataInput) throws HsqlException {
        sessionData.allocateLobForResult(result, dataInput);
    }
}
