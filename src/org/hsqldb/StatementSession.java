/* Copyright (c) 2001-2025, The HSQL Development Group
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

import java.util.TimeZone;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.SessionInterface.Attributes;
import org.hsqldb.ParserDQL.CompileContext;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.error.HsqlException;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.List;
import org.hsqldb.result.Result;
import org.hsqldb.rights.Grantee;
import org.hsqldb.rights.User;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.DTIType;
import org.hsqldb.types.IntervalSecondData;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;

/**
 * Implementation of Statement for SQL session statements.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
 * @since 1.9.0
 */
public class StatementSession extends Statement {

    public static final StatementSession commitNoChainStatement =
        new StatementSession(
            StatementTypes.COMMIT_WORK,
            new Object[]{ Boolean.FALSE });
    public static final StatementSession rollbackNoChainStatement =
        new StatementSession(
            StatementTypes.ROLLBACK_WORK,
            new Object[]{ Boolean.FALSE });
    public static final StatementSession commitAndChainStatement =
        new StatementSession(
            StatementTypes.COMMIT_WORK,
            new Object[]{ Boolean.TRUE });
    public static final StatementSession rollbackAndChainStatement =
        new StatementSession(
            StatementTypes.ROLLBACK_WORK,
            new Object[]{ Boolean.TRUE });

    static {
        commitNoChainStatement.sql                 = Tokens.T_COMMIT;
        commitAndChainStatement.sql = Tokens.T_COMMIT + ' ' + Tokens.T_CHAIN;
        rollbackNoChainStatement.sql               = Tokens.T_ROLLBACK;
        rollbackAndChainStatement.sql = Tokens.T_ROLLBACK + ' '
                                        + Tokens.T_CHAIN;
        commitNoChainStatement.compileTimestamp    = Long.MAX_VALUE;
        commitAndChainStatement.compileTimestamp   = Long.MAX_VALUE;
        rollbackNoChainStatement.compileTimestamp  = Long.MAX_VALUE;
        rollbackAndChainStatement.compileTimestamp = Long.MAX_VALUE;
    }

    Expression[] expressions;
    Object[]     arguments;

    StatementSession(
            Session session,
            CompileContext context,
            int type,
            Expression[] args) {

        super(type);

        expressions            = args;
        isTransactionStatement = false;
        isLogged               = false;

        Expression e = expressions[0];

        switch (type) {

            case StatementTypes.SET_TIME_ZONE :
                List<Expression> unresolved = e.resolveColumnReferences(
                    session,
                    RangeGroup.emptyGroup,
                    RangeGroup.emptyArray,
                    null);

                ExpressionColumn.checkColumnsResolved(unresolved);
                e.resolveTypes(session, null);

                if (e.dataType == null) {
                    e.setDataType(session, Type.SQL_INTERVAL_HOUR_TO_MINUTE);
                }

                if (e.dataType.isCharacterType()) {}
                else if (e.dataType.typeCode
                         != Types.SQL_INTERVAL_HOUR_TO_MINUTE) {
                    throw Error.error(ErrorCode.X_42563);
                }

                group = StatementTypes.X_SQL_SESSION;

                return;

            case StatementTypes.SET_PATH :
            case StatementTypes.SET_NAMES :
            case StatementTypes.SET_ROLE :
            case StatementTypes.SET_SCHEMA :
            case StatementTypes.SET_CATALOG :
            case StatementTypes.SET_SESSION_AUTHORIZATION :
            case StatementTypes.SET_COLLATION :
                group = StatementTypes.X_SQL_SESSION;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "StatementSession");
        }

        // e is null for SET ROLE NONE
        if (e != null) {
            e.resolveTypes(session, null);

            switch (e.getType()) {

                case OpTypes.VALUE :
                    break;

                case OpTypes.DYNAMIC_PARAM :
                    e.setDataType(session, Type.SQL_VARCHAR_DEFAULT);
                    break;

                case OpTypes.SQL_FUNCTION :
                    if (((FunctionSQL) e).isValueFunction()) {
                        break;
                    }

                    throw Error.error(ErrorCode.X_0P000);

                default :
                    throw Error.error(ErrorCode.X_0P000);
            }

            if (!e.getDataType().isCharacterType()) {
                throw Error.error(ErrorCode.X_0P000);
            }
        }

        setDatabaseObjects(session, context);
    }

    StatementSession(int type, Object[] args) {

        super(type);

        this.arguments         = args;
        isTransactionStatement = false;
        isLogged               = false;

        switch (type) {

            // logged by statement
            case StatementTypes.SET_SCHEMA :
                group    = StatementTypes.X_SQL_SESSION;
                isLogged = true;
                break;

            case StatementTypes.DECLARE_VARIABLE :
                group    = StatementTypes.X_HSQLDB_SESSION;
                isLogged = true;
                break;

            // cursor
            case StatementTypes.ALLOCATE_CURSOR :
                group = StatementTypes.X_SQL_DATA;
                break;

            case StatementTypes.ALLOCATE_DESCRIPTOR :
            case StatementTypes.DEALLOCATE_DESCRIPTOR :
            case StatementTypes.DEALLOCATE_PREPARE :
                group = StatementTypes.X_SQL_DYNAMIC;
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
                group = StatementTypes.X_SQL_DYNAMIC;
                break;

            // logged by session
            case StatementTypes.DISCONNECT :
                group = StatementTypes.X_SQL_CONNECTION;
                break;

            //
            case StatementTypes.SET_COLLATION :
            case StatementTypes.SET_CONNECTION :
            case StatementTypes.SET_CONSTRAINT :
            case StatementTypes.SET_DESCRIPTOR :
            case StatementTypes.SET_SESSION_AUTOCOMMIT :
            case StatementTypes.SET_SESSION_CHARACTERISTICS :
            case StatementTypes.SET_SESSION_FEATURE :
            case StatementTypes.SET_SESSION_RESULT_MAX_ROWS :
            case StatementTypes.SET_SESSION_RESULT_MEMORY_ROWS :
            case StatementTypes.SET_TRANSFORM_GROUP :
                group = StatementTypes.X_HSQLDB_SESSION;
                break;

            case StatementTypes.SET_SESSION_SQL_IGNORECASE :
                isLogged = true;
                group    = StatementTypes.X_HSQLDB_SESSION;
                break;

            // logged by session if necessary
            case StatementTypes.COMMIT_WORK :
            case StatementTypes.RELEASE_SAVEPOINT :
            case StatementTypes.ROLLBACK_SAVEPOINT :
            case StatementTypes.ROLLBACK_WORK :
            case StatementTypes.SAVEPOINT :
            case StatementTypes.SET_TRANSACTION :
            case StatementTypes.START_TRANSACTION :
                group = StatementTypes.X_SQL_TRANSACTION;
                break;

            case StatementTypes.DECLARE_SESSION_TABLE :
            case StatementTypes.DROP_TABLE :
                group = StatementTypes.X_SQL_SESSION;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "StatementSession");
        }
    }

    StatementSession(int type, HsqlName[] readNames, HsqlName[] writeNames) {

        super(type);

        isTransactionStatement = true;
        isLogged               = false;
        readTableNames         = readNames;
        writeTableNames        = writeNames;

        switch (type) {

            case StatementTypes.TRANSACTION_LOCK_CATALOG :
            case StatementTypes.TRANSACTION_UNLOCK_CATALOG :
            case StatementTypes.TRANSACTION_LOCK_TABLE :
                group = StatementTypes.X_HSQLDB_TRANSACTION;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "StatementSession");
        }
    }

    public Result execute(Session session) {

        Result result;

        try {
            result = getResult(session);
        } catch (Throwable t) {
            result = Result.newErrorResult(t, null);
        }

        if (result.isError()) {
            result.getException().setStatementType(group, type);

            return result;
        }

        try {
            if (isLogged) {
                session.database.logger.writeOtherStatement(session, sql);
            }
        } catch (Throwable e) {
            return Result.newErrorResult(e, sql);
        }

        return result;
    }

    Result getResult(Session session) {

        boolean startTransaction = false;

        if (this.isExplain) {
            return Result.newSingleColumnStringResult(
                "OPERATION",
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
                    boolean chain = ((Boolean) arguments[0]).booleanValue();

                    session.commit(chain);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.DEALLOCATE_DESCRIPTOR :
            case StatementTypes.DEALLOCATE_PREPARE :
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

            case StatementTypes.TRANSACTION_LOCK_CATALOG :
            case StatementTypes.TRANSACTION_UNLOCK_CATALOG :
            case StatementTypes.TRANSACTION_LOCK_TABLE : {
                return Result.updateZeroResult;
            }

            case StatementTypes.RELEASE_SAVEPOINT : {
                String savepoint = (String) arguments[0];

                try {
                    session.releaseSavepoint(savepoint);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.ROLLBACK_WORK : {
                boolean chain = ((Boolean) arguments[0]).booleanValue();

                session.rollback(chain);

                return Result.updateZeroResult;
            }

            case StatementTypes.ROLLBACK_SAVEPOINT : {
                String savepoint = (String) arguments[0];

                try {
                    session.rollbackToSavepoint(savepoint);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.SAVEPOINT : {
                String savepoint = (String) arguments[0];

                session.savepoint(savepoint);

                return Result.updateZeroResult;
            }

            case StatementTypes.SET_CATALOG : {
                String name;

                try {
                    name = (String) expressions[0].getValue(session);
                    name = (String) Type.SQL_VARCHAR.trim(
                        session,
                        name,
                        ' ',
                        true,
                        true);

                    if (session.database.getCatalogName().name.equals(name)) {
                        return Result.updateZeroResult;
                    }

                    return Result.newErrorResult(
                        Error.error(ErrorCode.X_3D000),
                        sql);
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
                    session.resetTimeZone();

                    Result result = Result.newUpdateZeroResult();
                    Result attribute = session.getSetAttributeResult(
                        Attributes.INFO_TIMEZONE);

                    result.addChainedResult(attribute);

                    return result;
                }

                try {
                    value = expressions[0].getValue(session);
                } catch (HsqlException e) {}

                if (value instanceof Result) {
                    Result result = (Result) value;

                    if (result.isData()) {
                        Object[] data   = null;
                        boolean  single = false;

                        if (result.getNavigator().next()) {
                            data   = result.getNavigator().getCurrent();
                            single = !result.getNavigator().next();
                        }

                        if (single && data != null && data[0] != null) {
                            value = data[0];

                            result.getNavigator().release();
                        } else {
                            result.getNavigator().release();

                            return Result.newErrorResult(
                                Error.error(ErrorCode.X_22009),
                                sql);
                        }
                    } else {
                        return Result.newErrorResult(
                            Error.error(ErrorCode.X_22009),
                            sql);
                    }
                } else {
                    if (value == null) {
                        return Result.newErrorResult(
                            Error.error(ErrorCode.X_22009),
                            sql);
                    }
                }

                if (value instanceof String) {
                    String zoneString = (String) value;

                    if (DateTimeType.zoneIDs.contains(zoneString)) {
                        TimeZone zone = TimeZone.getTimeZone(zoneString);

                        session.setTimeZone(zone);

                        Result result = Result.newUpdateZeroResult();
                        Result attribute = session.getSetAttributeResult(
                            Attributes.INFO_TIMEZONE);

                        result.addChainedResult(attribute);

                        return result;
                    } else {
                        value =
                            Type.SQL_INTERVAL_HOUR_TO_MINUTE.convertToDefaultType(
                                session,
                                value);
                    }
                }

                if (value instanceof IntervalSecondData) {
                    IntervalSecondData interval = (IntervalSecondData) value;
                    long               seconds  = interval.getSeconds();

                    if (-DTIType.timezoneSecondsLimit <= seconds
                            && seconds <= DTIType.timezoneSecondsLimit) {
                        String i =
                            Type.SQL_INTERVAL_HOUR_TO_MINUTE.convertToString(
                                interval);
                        String   sign       = seconds < 0
                                              ? ""
                                              : "+";
                        String   zoneString = "GMT" + sign + i;
                        TimeZone zone       = TimeZone.getTimeZone(zoneString);

                        session.setTimeZone(zone);

                        Result result = Result.newUpdateZeroResult();
                        Result attribute = session.getSetAttributeResult(
                            Attributes.INFO_TIMEZONE);

                        result.addChainedResult(attribute);

                        return result;
                    }
                }

                return Result.newErrorResult(
                    Error.error(ErrorCode.X_22009),
                    sql);
            }

            case StatementTypes.SET_NAMES :
                return Result.updateZeroResult;

            case StatementTypes.SET_PATH :
                return Result.updateZeroResult;

            case StatementTypes.SET_ROLE : {
                String  name;
                Grantee role = null;

                try {
                    if (expressions[0] != null) {
                        name = (String) expressions[0].getValue(session);

                        if (name == null) {
                            return Result.newErrorResult(
                                Error.error(ErrorCode.X_0P000),
                                sql);
                        }

                        name = (String) Type.SQL_VARCHAR.trim(
                            session,
                            name,
                            ' ',
                            true,
                            true);
                        role = session.database.granteeManager.getRole(name);
                    }
                } catch (HsqlException e) {
                    return Result.newErrorResult(
                        Error.error(ErrorCode.X_0P000),
                        sql);
                }

                if (session.isInMidTransaction()) {
                    return Result.newErrorResult(
                        Error.error(ErrorCode.X_25001),
                        sql);
                }

                if (role == null || session.getGrantee().hasRole(role)) {
                    session.setRole(role);

                    return Result.updateZeroResult;
                } else {
                    return Result.newErrorResult(
                        Error.error(ErrorCode.X_0P000),
                        sql);
                }
            }

            case StatementTypes.SET_SCHEMA : {
                String   name;
                HsqlName schema;

                try {
                    if (expressions == null) {
                        name = ((HsqlName) arguments[0]).name;
                    } else {
                        name = (String) expressions[0].getValue(session);
                    }

                    name = (String) Type.SQL_VARCHAR.trim(
                        session,
                        name,
                        ' ',
                        true,
                        true);
                    schema = session.database.schemaManager.getSchemaHsqlName(
                        name);

                    session.setCurrentSchemaHsqlName(schema);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.SET_SESSION_AUTHORIZATION : {
                if (session.isInMidTransaction()) {
                    return Result.newErrorResult(
                        Error.error(ErrorCode.X_25001),
                        sql);
                }

                try {
                    String user;
                    String password = null;

                    user = (String) expressions[0].getValue(session);
                    user = (String) Type.SQL_VARCHAR.trim(
                        session,
                        user,
                        ' ',
                        true,
                        true);

                    if (expressions[1] != null) {
                        password = (String) expressions[1].getValue(session);
                    }

                    User userObject;

                    if (password == null) {
                        userObject = session.database.userManager.get(user);
                    } else {
                        userObject = session.database.getUserManager()
                                                     .getUser(user, password);
                    }

                    if (userObject == null) {
                        throw Error.error(ErrorCode.X_28501);
                    }

                    // override the actual SQL at runtime
                    sql = userObject.getConnectUserSQL();

                    if (userObject == session.getGrantee()) {
                        return Result.updateZeroResult;
                    }

                    if (password == null
                            && !session.isProcessingLog()
                            && userObject.isAdmin()
                            && !session.getGrantee().isAdmin()) {
                        throw Error.error(ErrorCode.X_28000);
                    }

                    if (session.getGrantee().canChangeAuthorisation()) {
                        session.setUser(userObject);
                        session.setRole(null);
                        session.resetSchema();

                        return Result.updateZeroResult;
                    }

                    throw Error.error(ErrorCode.X_42507);
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.SET_SESSION_CHARACTERISTICS : {
                try {
                    if (arguments[0] != null) {
                        boolean readonly =
                            ((Boolean) arguments[0]).booleanValue();

                        session.setReadOnlyDefault(readonly);
                    }

                    if (arguments[1] != null) {
                        int level = ((Integer) arguments[1]).intValue();

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
                    if (arguments[0] != null) {
                        boolean readonly =
                            ((Boolean) arguments[0]).booleanValue();

                        session.setReadOnly(readonly);
                    }

                    if (arguments[1] != null) {
                        int level = ((Integer) arguments[1]).intValue();

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
            case StatementTypes.SET_SESSION_AUTOCOMMIT : {
                boolean mode = ((Boolean) arguments[0]).booleanValue();
                int     rows = ((Integer) arguments[1]).intValue();

                try {
                    if (rows < 0) {
                        session.setAutoCommit(mode);
                    } else {
                        session.setAutoCommitRows(rows);
                    }

                    Result result = Result.newUpdateZeroResult();
                    Result attribute = session.getSetAttributeResult(
                        Attributes.INFO_AUTOCOMMIT);

                    result.addChainedResult(attribute);

                    return result;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.DECLARE_VARIABLE : {
                ColumnSchema[] variables = (ColumnSchema[]) arguments[0];

                try {
                    for (int i = 0; i < variables.length; i++) {
                        session.sessionContext.addSessionVariable(variables[i]);
                    }

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.SET_SESSION_FEATURE : {
                String  feature = (String) arguments[0];
                Boolean value   = (Boolean) arguments[1];

                session.setFeature(feature, value.booleanValue());

                return Result.updateZeroResult;
            }

            case StatementTypes.SET_SESSION_RESULT_MAX_ROWS : {
                int size = ((Integer) arguments[0]).intValue();

                session.setSQLMaxRows(size);

                return Result.updateZeroResult;
            }

            case StatementTypes.SET_SESSION_RESULT_MEMORY_ROWS : {
                int size = ((Integer) arguments[0]).intValue();

                session.setResultMemoryRowCount(size);

                return Result.updateZeroResult;
            }

            case StatementTypes.SET_SESSION_SQL_IGNORECASE : {
                try {
                    boolean mode = ((Boolean) arguments[0]).booleanValue();

                    session.setIgnoreCase(mode);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.DECLARE_SESSION_TABLE : {
                Table         table       = (Table) arguments[0];
                HsqlArrayList<Constraint> tempConstraints =
                    (HsqlArrayList<Constraint>) arguments[1];
                StatementDMQL statement   = (StatementDMQL) arguments[3];
                Boolean       ifNotExists = (Boolean) arguments[4];

                try {
                    if (tempConstraints.size() > 0) {
                        table = ParserDDL.addTableConstraintDefinitions(
                            session,
                            table,
                            tempConstraints,
                            null,
                            false);
                    }

                    table.compile(session, null);

                    try {
                        session.sessionContext.addSessionTable(table);
                    } catch (HsqlException e) {
                        if (ifNotExists != null && ifNotExists.booleanValue()) {
                            return Result.updateZeroResult;
                        } else {
                            return Result.newErrorResult(e, sql);
                        }
                    }

                    if (table.hasLobColumn) {
                        throw Error.error(ErrorCode.X_42534);
                    }

                    if (statement != null) {
                        Result result = statement.execute(session);

                        table.insertIntoTable(session, result);
                    }

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }

            case StatementTypes.DROP_TABLE : {
                HsqlName name     = (HsqlName) arguments[0];
                Boolean  ifExists = (Boolean) arguments[1];
                Table table = session.sessionContext.findSessionTable(
                    name.name);

                if (table == null) {
                    if (ifExists.booleanValue()) {
                        return Result.updateZeroResult;
                    } else {
                        throw Error.error(ErrorCode.X_42501, name.name);
                    }
                }

                session.sessionData.persistentStoreCollection.removeStore(
                    table);
                session.sessionContext.dropSessionTable(name.name);

                return Result.updateZeroResult;
            }

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "StatementSession");
        }
    }

    public boolean isAutoCommitStatement() {
        return false;
    }

    public String describe(Session session) {
        return sql;
    }

    public boolean isCatalogLock(int model) {
        return false;
    }

    public boolean isCatalogChange() {
        return false;
    }
}
