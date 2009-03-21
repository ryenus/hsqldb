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


package org.hsqldb;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.result.Result;
import org.hsqldb.rights.Grantee;
import org.hsqldb.rights.User;
import org.hsqldb.scriptio.ScriptWriterText;
import org.hsqldb.types.DTIType;
import org.hsqldb.types.IntervalSecondData;

/**
 * Implementation of Statement for SQL commands.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class StatementCommand extends Statement {

    Expression[] expressions;
    Object[]     parameters;

    StatementCommand(int type, Expression[] args) {

        super(type);

        this.expressions       = args;
        isTransactionStatement = false;

        switch (type) {

            case StatementTypes.SET_TIME_ZONE :
            case StatementTypes.SET_NAMES :
            case StatementTypes.SET_PATH :
            case StatementTypes.SET_ROLE :
            case StatementTypes.SET_SCHEMA :
            case StatementTypes.SET_SESSION_AUTHORIZATION :
            case StatementTypes.SET_COLLATION :
                group = StatementTypes.X_SQL_SESSION;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "StateemntCommand");
        }
    }

    StatementCommand(int type, Object[] args) {

        super(type);

        this.parameters        = args;
        isTransactionStatement = false;

        switch (type) {

            //
            case StatementTypes.DESCRIBE :
                group = StatementTypes.X_SQL_DYNAMIC;
                break;

            // cursor
            case StatementTypes.ALLOCATE_CURSOR :
                group = StatementTypes.X_SQL_DATA;
                break;

            case StatementTypes.ALLOCATE_DESCRIPTOR :
            case StatementTypes.DEALLOCATE_DESCRIPTOR :
            case StatementTypes.DEALLOCATE_PREPARE :
                group = StatementTypes.X_DYNAMIC;
                break;

            //
            case StatementTypes.DYNAMIC_DELETE_CURSOR :
                group = StatementTypes.X_SQL_DATA_CHANGE;
                break;

            case StatementTypes.DYNAMIC_CLOSE :
            case StatementTypes.DYNAMIC_FETCH :
            case StatementTypes.DYNAMIC_OPEN :
                group = StatementTypes.X_SQL_DATA;
                break;

            //
            case StatementTypes.OPEN :
            case StatementTypes.FETCH :
            case StatementTypes.FREE_LOCATOR :
            case StatementTypes.GET_DESCRIPTOR :
            case StatementTypes.HOLD_LOCATOR :
                group = StatementTypes.X_SQL_DATA;
                break;

            //
            case StatementTypes.PREPARABLE_DYNAMIC_DELETE_CURSOR :
            case StatementTypes.PREPARABLE_DYNAMIC_UPDATE_CURSOR :
            case StatementTypes.PREPARE :
                group = StatementTypes.X_DYNAMIC;
                break;

            //
            //
            case StatementTypes.RETURN :
                group = StatementTypes.X_SQL_CONTROL;
                break;

            //
            case StatementTypes.CONNECT :
            case StatementTypes.DISCONNECT :
                group = StatementTypes.X_SQL_CONNECTION;
                break;

            //
            case StatementTypes.SET_CONNECTION :
            case StatementTypes.SET_CONSTRAINT :
            case StatementTypes.SET_DESCRIPTOR :
            case StatementTypes.SET_CATALOG :
            case StatementTypes.SET_SESSION_CHARACTERISTICS :
            case StatementTypes.SET_TRANSFORM_GROUP :
            case StatementTypes.SET_SCHEMA :
                group = StatementTypes.X_SQL_SESSION;
                break;

            case StatementTypes.SET_SESSION_RESULT_MAX_ROWS :
            case StatementTypes.SET_SESSION_RESULT_MEMORY_ROWS :
            case StatementTypes.SET_AUTOCOMMIT :
                group = StatementTypes.X_HSQLDB_SESSION;
                break;

            case StatementTypes.DECLARE_VARIABLE :
                isTransactionStatement = false;
                group                  = StatementTypes.X_HSQLDB_SESSION;
                break;

            case StatementTypes.COMMIT_WORK :
            case StatementTypes.RELEASE_SAVEPOINT :
            case StatementTypes.ROLLBACK_SAVEPOINT :
            case StatementTypes.ROLLBACK_WORK :
            case StatementTypes.SAVEPOINT :
            case StatementTypes.SET_TRANSACTION :
            case StatementTypes.START_TRANSACTION :
                group = StatementTypes.X_SQL_TRANSACTION;
                break;

            case StatementTypes.LOCK_TABLE :
                isTransactionStatement = true;
                group                  = StatementTypes.X_HSQLDB_TRANSACTION;
                break;

            //
            case StatementTypes.BACKUP :
            case StatementTypes.CHECKPOINT :
            case StatementTypes.SCRIPT :
            case StatementTypes.SHUTDOWN :
                group = StatementTypes.X_HSQLDB_OPERATION;
                break;

            case StatementTypes.SET_TABLE_SOURCE :
            case StatementTypes.SET_TABLE_TYPE :
                group          = StatementTypes.X_HSQLDB_OPERATION;
                metaDataImpact = Statement.META_RESET_VIEWS;
                break;

            case StatementTypes.SET_DATABASE_EVENT_LOG :
            case StatementTypes.SET_DATABASE_COLLATION :
            case StatementTypes.SET_DATABASE_DEFRAG :
            case StatementTypes.SET_DATABASE_LOG_SIZE :
            case StatementTypes.SET_DATABASE_IGNORECASE :
            case StatementTypes.SET_DATABASE_SCRIPT_FORMAT :
            case StatementTypes.SET_DATABASE_REFERENTIAL_INTEGRITY :
            case StatementTypes.SET_DATABASE_RESULT_MEMORY_ROWS :
            case StatementTypes.SET_DATABASE_WRITE_DELAY :
            case StatementTypes.SET_DEFAULT_INITIAL_SCHEMA :
            case StatementTypes.SET_INITIAL_SCHEMA :
            case StatementTypes.SET_PASSWORD :
            case StatementTypes.SET_PROPERTY :
            case StatementTypes.SET_TABLE_INDEX :
                group = StatementTypes.X_HSQLDB_SETTING;
                break;

            case StatementTypes.SET_TABLE_READONLY :
                metaDataImpact = Statement.META_RESET_VIEWS;
                group          = StatementTypes.X_HSQLDB_SETTING;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "StatementCommand");
        }
    }

    public Result execute(Session session, Object[] args) {

        Result result = getResult(session);

        if (result.isError()) {
            result.getException().setStatementType(group, type);
        }

        return result;
    }

    Result getResult(Session session) {

        boolean startTransaction = false;

        if (this.isExplain) {
            return Result.newSingleColumnStringResult("OPERATION",
                    describe(session));
        }

        switch (type) {

            // cursor
            case StatementTypes.ALLOCATE_CURSOR :
            case StatementTypes.ALLOCATE_DESCRIPTOR :
                return Result.updateZeroResult;

            //
            case StatementTypes.COMMIT_WORK : {
                try {
                    boolean chain = parameters != null;

                    session.commit(chain);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.DEALLOCATE_DESCRIPTOR :
            case StatementTypes.DEALLOCATE_PREPARE :
                return Result.updateZeroResult;

            //
            case StatementTypes.DESCRIBE :
                return Result.updateZeroResult;

            case StatementTypes.DISCONNECT :
                session.close();

                return Result.updateZeroResult;

            //
            case StatementTypes.DYNAMIC_CLOSE :
            case StatementTypes.DYNAMIC_DELETE_CURSOR :
            case StatementTypes.DYNAMIC_FETCH :
            case StatementTypes.DYNAMIC_OPEN :

            //
            case StatementTypes.FETCH :
            case StatementTypes.FREE_LOCATOR :
            case StatementTypes.GET_DESCRIPTOR :
            case StatementTypes.HOLD_LOCATOR :

            //
            case StatementTypes.OPEN :
            case StatementTypes.PREPARABLE_DYNAMIC_DELETE_CURSOR :
            case StatementTypes.PREPARABLE_DYNAMIC_UPDATE_CURSOR :
            case StatementTypes.PREPARE :
                return Result.updateZeroResult;

            case StatementTypes.LOCK_TABLE : {
                return Result.updateZeroResult;
            }

            //
            case StatementTypes.RELEASE_SAVEPOINT : {
                String savepoint = (String) parameters[0];

                try {
                    session.releaseSavepoint(savepoint);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            //
            case StatementTypes.RETURN :
                return Result.updateZeroResult;

            //
            case StatementTypes.ROLLBACK_WORK : {
                boolean chain = ((Boolean) parameters[0]).booleanValue();

                session.rollback(chain);

                return Result.updateZeroResult;
            }
            case StatementTypes.ROLLBACK_SAVEPOINT : {
                String savepoint = (String) parameters[0];

                try {
                    session.rollbackToSavepoint(savepoint);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SAVEPOINT : {
                String savepoint = (String) parameters[0];

                session.savepoint(savepoint);

                return Result.updateZeroResult;
            }

            //
            case StatementTypes.SET_CATALOG : {
                String name;

                try {
                    name = (String) expressions[0].getValue(session);

                    if (session.database.getCatalogName().equals(name)) {
                        return Result.updateZeroResult;
                    }

                    return Result.newErrorResult(
                        Error.error(ErrorCode.X_3D000), sql);
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_CONNECTION :
            case StatementTypes.SET_CONSTRAINT :
            case StatementTypes.SET_DESCRIPTOR :
                return Result.updateZeroResult;

            case StatementTypes.SET_TIME_ZONE : {
                Object value = null;

                if (expressions[0].getType() == OpTypes.VALUE
                        && expressions[0].getConstantValueNoCheck(session)
                           == null) {
                    session.timeZoneSeconds = session.sessionTimeZoneSeconds;

                    return Result.updateZeroResult;
                }

                try {
                    value = expressions[0].getValue(session);
                } catch (HsqlException e) {}

                if (value instanceof Result) {
                    Result result = (Result) value;

                    if (result.isData()) {
                        Object[] data =
                            (Object[]) result.getNavigator().getNext();
                        boolean single = !result.getNavigator().next();

                        if (single && data != null && data[0] != null) {
                            value = data[0];

                            result.getNavigator().close();
                        } else {
                            result.getNavigator().close();

                            return Result.newErrorResult(
                                Error.error(ErrorCode.X_22009), sql);
                        }
                    } else {
                        return Result.newErrorResult(
                            Error.error(ErrorCode.X_22009), sql);
                    }
                } else {
                    if (value == null) {
                        return Result.newErrorResult(
                            Error.error(ErrorCode.X_22009), sql);
                    }
                }

                long seconds = ((IntervalSecondData) value).getSeconds();

                if (-DTIType.timezoneSecondsLimit <= seconds
                        && seconds <= DTIType.timezoneSecondsLimit) {
                    session.timeZoneSeconds = (int) seconds;

                    return Result.updateZeroResult;
                }

                return Result.newErrorResult(Error.error(ErrorCode.X_22009),
                                             sql);
            }
            case StatementTypes.SET_NAMES :
                return Result.updateZeroResult;

            case StatementTypes.SET_PATH :
                return Result.updateZeroResult;

            case StatementTypes.SET_ROLE : {
                String  name;
                Grantee role;

                try {
                    name = (String) expressions[0].getValue(session);
                    role = session.database.granteeManager.getRole(name);
                } catch (HsqlException e) {
                    return Result.newErrorResult(
                        Error.error(ErrorCode.X_0P000), sql);
                }

                if (role == null) {
                    return Result.newErrorResult(
                        Error.error(ErrorCode.X_0P000), sql);
                }

                if (session.getGrantee().hasRole(role)) {

                    /** @todo 1.9.0 - implement */
                    return Result.updateZeroResult;
                } else {
                    return Result.newErrorResult(
                        Error.error(ErrorCode.X_0P000), sql);
                }
            }
            case StatementTypes.SET_SCHEMA : {
                String   name;
                HsqlName schema;

                try {
                    if (expressions == null) {
                        name = ((HsqlName) parameters[0]).name;
                    } else {
                        name = (String) expressions[0].getValue(session);
                    }

                    schema =
                        session.database.schemaManager.getSchemaHsqlName(name);

                    session.setSchema(schema.name);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_SESSION_AUTHORIZATION : {
                if (session.isInMidTransaction()) {
                    return Result.newErrorResult(
                        Error.error(ErrorCode.X_25001), sql);
                }

                try {
                    String user;
                    String password = null;

                    user = (String) expressions[0].getValue(session);

                    if (expressions[1] != null) {
                        password = (String) expressions[1].getValue(session);
                    }

                    Grantee grantee;

                    if (password == null) {
                        grantee = session.database.userManager.get(user);
                    } else {
                        grantee =
                            session.database.getUserManager().getUser(user,
                                password);
                    }

                    if (grantee == null) {
                        throw Error.error(ErrorCode.X_28501);
                    }

                    if (grantee == session.getGrantee()) {
                        return Result.updateZeroResult;
                    }

                    if (session.getGrantee().canChangeAuthorisation()) {
                        session.setUser((User) grantee);
                        session.database.logger.logConnectUser(session);
                        session.resetSchema();

                        return Result.updateZeroResult;
                    }

                    /** @todo may need different error code */
                    throw Error.error(ErrorCode.X_28000);
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_SESSION_CHARACTERISTICS : {
                try {
                    if (parameters[0] != null) {
                        boolean readonly =
                            ((Boolean) parameters[0]).booleanValue();

                        session.setReadOnlyDefault(readonly);
                    }

                    if (parameters[1] != null) {
                        int level = ((Integer) parameters[1]).intValue();

                        session.setIsolationDefault(level);
                    }

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_COLLATION :
                return Result.updateZeroResult;

            case StatementTypes.SET_TRANSFORM_GROUP :
                return Result.updateZeroResult;

            case StatementTypes.START_TRANSACTION :
                startTransaction = true;

            // fall through
            case StatementTypes.SET_TRANSACTION : {
                try {
                    if (parameters[0] != null) {
                        boolean readonly =
                            ((Boolean) parameters[0]).booleanValue();

                        session.setReadOnly(readonly);
                    }

                    if (parameters[1] != null) {
                        int level = ((Integer) parameters[1]).intValue();

                        session.setIsolation(level);
                    }

                    if (startTransaction) {
                        session.startTransaction();
                    }

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            //
            case StatementTypes.SET_AUTOCOMMIT : {
                boolean mode = ((Boolean) parameters[0]).booleanValue();

                try {
                    session.setAutoCommit(mode);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.DECLARE_VARIABLE : {
                ColumnSchema variable = (ColumnSchema) parameters[0];

                try {
                    session.sessionContext.addSessionVariable(variable);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.BACKUP : {
                String  path       = ((String) parameters[0]);
                boolean blocking   = ((Boolean) parameters[1]).booleanValue();
                boolean script     = ((Boolean) parameters[2]).booleanValue();
                boolean compressed = ((Boolean) parameters[3]).booleanValue();

                if (!blocking) {

                    // SHOULD NEVER GET HERE.  Parser requires this option
                    // for v. 1.9.
                    return Result.newErrorResult(
                        Error.error(ErrorCode.X_0A000),
                        "'BLOCKING' required in: " + sql);
                }

                if (script) {    // Dev assertion

                    // SHOULD NEVER GET HERE.  Parser prohibits this option
                    // for v. 1.9.
                    return Result.newErrorResult(
                        Error.error(ErrorCode.X_0A000),
                        "'SCRIPT' unsupported in: " + sql);
                }

                try {
                    session.checkAdmin();

                    if (!session.database.getType().equals(
                            DatabaseURL.S_FILE)) {

                        // Do not enforce this constraint for SCRIPT type
                        // backup.
                        return Result.newErrorResult(
                            Error.error(ErrorCode.DATABASE_IS_NON_FILE));

                        // If we were to back up res: type DB's, could use
                        // DatabasURL.isFileBasedDataType(), but I see no
                        // point to back up one of these.
                    }

                    if (session.database.isReadOnly()) {

                        // Do not enforce this constraint for SCRIPT type
                        // backup.
                        return Result.newErrorResult(
                            Error.error(ErrorCode.DATABASE_IS_MEMORY_ONLY),
                            null);
                    }

                    session.database.logger.backup(path,
                                                   session.database.getPath(),
                                                   script, blocking,
                                                   compressed);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.CHECKPOINT : {
                boolean defrag = ((Boolean) parameters[0]).booleanValue();

                try {
                    session.database.logger.checkpoint(defrag);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_EVENT_LOG : {
                try {
                    int value = ((Integer) parameters[0]).intValue();

                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.getProperties().setProperty(
                        HsqlDatabaseProperties.hsqldb_applog, value);
                    session.database.logger.appLog.setLevel(value);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_COLLATION : {
                try {
                    String name = (String) parameters[0];

                    /** @todo 1.9.0 - ensure no data in character columns */
                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.collation.setCollation(name);
                    session.database.logger.writeToLog(session, sql);
                    session.database.setMetaDirty(false);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_TRANSACTION_CONTROL : {
                try {
                    boolean mvcc = ((Boolean) parameters[0]).booleanValue();

                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.txManager.setMVCC(mvcc);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_DEFRAG : {
                try {
                    int value = ((Integer) parameters[0]).intValue();

                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.getProperties().setProperty(
                        HsqlDatabaseProperties.hsqldb_defrag_limit, value);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_LOG_SIZE : {
                try {
                    int value = ((Integer) parameters[0]).intValue();

                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.logger.setLogSize(value);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_WRITE_DELAY : {
                try {
                    int value = ((Integer) parameters[0]).intValue();

                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.logger.writeToLog(session, sql);
                    session.database.setMetaDirty(false);
                    session.database.logger.setWriteDelay(value);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_IGNORECASE : {
                try {
                    boolean mode = ((Boolean) parameters[0]).booleanValue();

                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.setIgnoreCase(mode);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_SCRIPT_FORMAT : {
                try {
                    int value = ((Integer) parameters[0]).intValue();

                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.logger.setScriptType(value);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_REFERENTIAL_INTEGRITY : {
                boolean mode = ((Boolean) parameters[0]).booleanValue();

                session.database.setReferentialIntegrity(mode);

                return Result.updateZeroResult;
            }
            case StatementTypes.SET_DEFAULT_INITIAL_SCHEMA : {
                try {
                    HsqlName schema = (HsqlName) parameters[0];

                    //
                    session.database.schemaManager.setDefaultSchemaHsqlName(
                        schema);
                    session.database.logger.writeToLog(session, sql);
                    session.database.setMetaDirty(true);

                    //
                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_INITIAL_SCHEMA : {
                try {
                    User     user   = (User) parameters[0];
                    HsqlName schema = (HsqlName) parameters[1];

                    session.checkDDLWrite();

                    if (user == null) {
                        user = session.getUser();
                    } else {
                        session.checkAdmin();
                        session.checkDDLWrite();

                        user = session.database.userManager.get(
                            user.getNameString());
                    }

                    if (schema != null) {
                        schema =
                            session.database.schemaManager.getSchemaHsqlName(
                                schema.name);
                    }

                    //
                    user.setInitialSchema(schema);
                    session.database.logger.writeToLog(session, sql);
                    session.database.setMetaDirty(false);

                    //
                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_PASSWORD : {
                try {
                    User   user = parameters[0] == null ? session.getUser()
                                                        : (User) parameters[0];
                    String password = (String) parameters[1];

                    session.checkDDLWrite();
                    session.setScripting(true);
                    user.setPassword(password);
                    session.database.logger.writeToLog(session, sql);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_PROPERTY : {
                try {
                    String property = (String) parameters[0];
                    Object value    = parameters[1];

                    //
                    session.checkAdmin();
                    session.checkDDLWrite();

                    //
                    if (HsqlDatabaseProperties.hsqldb_cache_file_scale.equals(
                            property)) {
                        if (session.database.logger.hasCache()
                                || ((Integer) value).intValue() != 8) {
                            HsqlException e = Error.error(ErrorCode.X_42513,
                                                          property);

                            return Result.newErrorResult(e, sql);
                        }
                    }

                    HsqlDatabaseProperties p =
                        session.database.getProperties();

                    p.setDatabaseProperty(property,
                                          value.toString().toLowerCase());
                    p.setDatabaseVariables();
                    p.save();

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_SESSION_RESULT_MAX_ROWS : {
                int size = ((Integer) parameters[0]).intValue();

                session.setSQLMaxRows(size);

                return Result.updateZeroResult;
            }
            case StatementTypes.SET_DATABASE_RESULT_MEMORY_ROWS : {
                int size = ((Integer) parameters[0]).intValue();

                session.database.getProperties().setProperty(
                    HsqlDatabaseProperties.hsqldb_result_max_memory_rows,
                    size);

                return Result.updateZeroResult;
            }
            case StatementTypes.SET_SESSION_RESULT_MEMORY_ROWS : {
                int size = ((Integer) parameters[0]).intValue();

                session.setResultMemoryRowCount(size);

                return Result.updateZeroResult;
            }
            case StatementTypes.SET_TABLE_INDEX : {
                try {
                    HsqlName name  = (HsqlName) parameters[0];
                    String   value = (String) parameters[1];
                    Table table =
                        session.database.schemaManager.getTable(session,
                            name.name, name.schema.name);

                    table.setIndexRoots(session, value);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_TABLE_READONLY : {
                try {
                    HsqlName name = (HsqlName) parameters[0];
                    Table table =
                        session.database.schemaManager.getTable(session,
                            name.name, name.schema.name);
                    boolean mode = ((Boolean) parameters[1]).booleanValue();

                    session.checkAdmin();
                    session.checkDDLWrite();
                    table.setDataReadOnly(mode);
                    session.database.logger.writeToLog(session, sql);
                    session.database.setMetaDirty(false);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_TABLE_SOURCE : {
                try {
                    HsqlName name = (HsqlName) parameters[0];
                    Table table =
                        session.database.schemaManager.getTable(session,
                            name.name, name.schema.name);

                    if (!table.isText()) {
                        Exception e = Error.error(ErrorCode.X_S0522);

                        return Result.newErrorResult(e, sql);
                    }

                    if (parameters[1] != null) {
                        boolean mode =
                            ((Boolean) parameters[1]).booleanValue();

                        if (mode) {
                            ((TextTable) table).connect(session);
                        } else {
                            ((TextTable) table).disconnect();
                        }

                        session.database.logger.writeToLog(session, sql);
                        session.database.setMetaDirty(false);

                        return Result.updateZeroResult;
                    }

                    String  source = (String) parameters[2];
                    boolean isDesc = ((Boolean) parameters[3]).booleanValue();
                    boolean isHeader =
                        ((Boolean) parameters[4]).booleanValue();

                    if (isHeader) {
                        ((TextTable) table).setHeader(source);
                    } else {
                        ((TextTable) table).setDataSource(session, source,
                                                          isDesc, false);
                    }

                    session.database.logger.writeToLog(session, sql);

                    return Result.updateZeroResult;
                } catch (Throwable e) {
                    if (!(e instanceof HsqlException)) {
                        e = Error.error(ErrorCode.GENERAL_IO_ERROR,
                                        e.getMessage());
                    }

                    if (session.isProcessingLog()
                            || session.isProcessingScript()) {
                        session.addWarning((HsqlException) e);

                        //* @todo - add an entry to applog too */
                        return Result.updateZeroResult;
                    } else {
                        return Result.newErrorResult(e, sql);
                    }
                }
            }
            case StatementTypes.SET_TABLE_TYPE : {
                try {
                    HsqlName name = (HsqlName) parameters[0];
                    int      type = ((Integer) parameters[1]).intValue();

                    //
                    session.checkAdmin();
                    session.checkDDLWrite();

                    Table table =
                        session.database.schemaManager.getUserTable(session,
                            name.name, name.schema.name);

                    session.setScripting(true);

                    TableWorks tw = new TableWorks(session, table);

                    tw.setTableType(session, type);
                    session.database.logger.writeToLog(session, sql);
                    session.database.setMetaDirty(false);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SCRIPT : {
                ScriptWriterText dsw  = null;
                String           name = (String) parameters[0];

                if (name == null) {
                    return session.database.getScript(false);
                } else {
                    try {
                        dsw = new ScriptWriterText(session.database, name,
                                                   true, true, true);

                        dsw.writeAll();
                        dsw.close();
                    } catch (HsqlException e) {
                        return Result.newErrorResult(e, sql);
                    }

                    return Result.updateZeroResult;
                }
            }
            case StatementTypes.SHUTDOWN : {
                try {
                    int mode = ((Integer) parameters[0]).intValue();

                    session.database.close(mode);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "CompiledStateemntCommand");
        }
    }

    public String describe(Session session) {
        return sql;
    }
}
