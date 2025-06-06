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

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.error.HsqlException;
import org.hsqldb.index.IndexStats;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.List;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.map.ValuePool;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.persist.RowInsertInterface;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultProperties;
import org.hsqldb.rights.User;
import org.hsqldb.types.Charset;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;

/**
 * Parser for session and management statements
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.3
 * @since 1.9.0
 */
public class ParserCommand extends ParserDDL {

    ParserCommand(Session session, Scanner t) {
        super(session, t);
    }

    Statement compileStatement(int props) {

        Statement cs = compilePart(props);

        if (token.tokenType == Tokens.X_ENDPARSE) {
            if (cs.getSchemaName() == null) {
                cs.setSchemaHsqlName(session.getCurrentSchemaHsqlName());
            }

            return cs;
        }

        throw unexpectedToken();
    }

    HsqlArrayList<Statement> compileStatements(String sql, Result cmd) {

        HsqlArrayList<Statement> list = new HsqlArrayList<>();
        Statement                cs   = null;

        reset(session, sql);

        while (true) {
            if (token.tokenType == Tokens.X_ENDPARSE) {
                break;
            }

            try {
                lastError = null;
                cs        = compilePart(cmd.getExecuteProperties());
            } catch (HsqlException e) {
                if (lastError != null && lastError.getLevel() > e.getLevel()) {
                    throw lastError;
                }

                throw e;
            }

            if (!cs.isExplain
                    && cs.getParametersMetaData().getColumnCount() > 0) {
                throw Error.error(ErrorCode.X_42575);
            }

            cs.setCompileTimestamp(database.txManager.getSystemChangeNumber());
            list.add(cs);
        }

        if (list.size() > 1) {
            if (database.sqlRestrictExec) {
                throw Error.error(ErrorCode.X_07502);
            }
        }

        int returnType = cmd.getStatementType();

        if (returnType != StatementTypes.RETURN_ANY) {
            int group = cs.getGroup();

            if (group == StatementTypes.X_SQL_DATA) {
                if (returnType == StatementTypes.RETURN_COUNT) {
                    throw Error.error(ErrorCode.X_07503);
                }
            } else if (returnType == StatementTypes.RETURN_RESULT) {

                // allow update count statements with Statement.executeQuery()
                // to return an empty result set
                if (database.sqlRestrictExec) {
                    throw Error.error(ErrorCode.X_07504);
                }
            }
        }

        return list;
    }

    private Statement compilePart(int props) {

        Statement cs;

        compileContext.reset();
        setPartPosition(getPosition());

        if (token.tokenType == Tokens.X_STARTPARSE) {
            read();
        }

        switch (token.tokenType) {

            // DQL
            case Tokens.WITH :
            case Tokens.OPENBRACKET :
            case Tokens.SELECT :
            case Tokens.TABLE : {
                cs = compileCursorSpecification(
                    RangeGroup.emptyArray,
                    props,
                    false);
                break;
            }

            case Tokens.VALUES : {
                RangeGroup[] ranges =
                    session.sessionContext.sessionVariableRangeGroups;

                compileContext.setOuterRanges(ranges);

                cs = compileShortCursorSpecification(props);
                break;
            }

            // DML
            case Tokens.INSERT : {
                cs = compileInsertStatement(RangeGroup.emptyArray);
                break;
            }

            case Tokens.UPDATE : {
                cs = compileUpdateStatement(RangeGroup.emptyArray);
                break;
            }

            case Tokens.MERGE : {
                cs = compileMergeStatement(RangeGroup.emptyArray);
                break;
            }

            case Tokens.DELETE : {
                cs = compileDeleteStatement(RangeGroup.emptyArray);
                break;
            }

            case Tokens.TRUNCATE : {
                cs = compileTruncateStatement();
                break;
            }

            case Tokens.REPLACE : {
                cs = compileInsertStatement(RangeGroup.emptyArray);
                break;
            }

            // PROCEDURE
            case Tokens.CALL : {
                cs = compileCallStatement(
                    session.sessionContext.sessionVariableRangeGroups,
                    false);
                break;
            }

            // SQL SESSION
            case Tokens.SET :
                cs = compileSet();
                break;

            // diagnostic
            case Tokens.GET :
                cs = compileGetStatement(
                    session.sessionContext.sessionVariableRangeGroups);
                break;

            case Tokens.START :
                cs = compileStartTransaction();
                break;

            case Tokens.COMMIT :
                cs = compileCommit();
                break;

            case Tokens.ROLLBACK :
                cs = compileRollback();
                break;

            case Tokens.SAVEPOINT :
                cs = compileSavepoint();
                break;

            case Tokens.RELEASE :
                cs = compileReleaseSavepoint();
                break;

            // DDL
            case Tokens.CREATE :
                cs = compileCreate();
                break;

            case Tokens.ALTER :
                cs = compileAlter();
                break;

            case Tokens.DROP :
                cs = compileDrop();
                break;

            case Tokens.GRANT :
            case Tokens.REVOKE :
                cs = compileGrantOrRevoke();
                break;

            case Tokens.COMMENT :
                cs = compileComment();
                break;

            // HSQL SESSION
            case Tokens.LOCK :
                cs = compileLock();
                break;

            case Tokens.CONNECT :
                cs = compileConnect();
                break;

            case Tokens.DISCONNECT :
                cs = compileDisconnect();
                break;

            // HSQL COMMAND
            case Tokens.SCRIPT :
                cs = compileScript(false);
                break;

            case Tokens.SHUTDOWN :
                cs = compileShutdown();
                break;

            case Tokens.BACKUP :
                cs = compileBackup();
                break;

            case Tokens.CHECKPOINT :
                cs = compileCheckpoint();
                break;

            case Tokens.EXPLAIN : {
                cs = compileExplain();
                break;
            }

            case Tokens.DECLARE :
                cs = compileDeclare();
                break;

            case Tokens.PERFORM :
                cs = compilePerform();
                break;

            default :
                throw unexpectedToken();
        }

        // SET_SESSION_AUTHORIZATION is translated dynamically at runtime for logging
        switch (cs.type) {

            // these are set at compile time for logging
            case StatementTypes.COMMIT_WORK :
            case StatementTypes.ROLLBACK_WORK :
            case StatementTypes.SET_USER_PASSWORD :
            case StatementTypes.EXPLAIN_PLAN :
                break;

            default :
                cs.setSQL(getLastPart());
        }

        if (token.tokenType == Tokens.SEMICOLON) {
            read();
        } else if (token.tokenType == Tokens.X_ENDPARSE) {}

        return cs;
    }

    private Statement compileDeclare() {

        Statement      cs;
        ColumnSchema[] variables;

        cs = compileDeclareLocalTableOrNull();

        if (cs != null) {
            return cs;
        }

        variables = readLocalVariableDeclarationOrNull();

        if (variables != null) {
            Object[] args = new Object[]{ variables };

            cs = new StatementSession(StatementTypes.DECLARE_VARIABLE, args);

            return cs;
        }

        cs = compileDeclareCursorOrNull(RangeGroup.emptyArray, false);

        if (cs == null) {
            throw lastError == null
                  ? unexpectedToken()
                  : lastError;
        }

        return cs;
    }

    private Statement compileScript(boolean extended) {

        String        name      = null;
        int           scope     = 0;
        int           type      = 0;
        Boolean       colNames  = Boolean.FALSE;
        HsqlName      tableName = null;
        TimestampData timestamp = null;

        readThis(Tokens.SCRIPT);

        if (extended) {
            readThis(Tokens.FOR);
            checkIsAny(Tokens.DATABASE, Tokens.TABLE, 0, 0);

            if (readIfThis(Tokens.DATABASE)) {
                switch (token.tokenType) {

                    case Tokens.STRUCTURE :
                        read();

                        type = Tokens.STRUCTURE;
                        break;

                    case Tokens.VERSIONING :
                        read();
                        readThis(Tokens.DATA);

                        if (readIfThis(Tokens.FROM)) {
                            readThis(Tokens.TIMESTAMP);

                            String s = readQuotedString();

                            timestamp =
                                (TimestampData) Type.SQL_TIMESTAMP.convertToType(
                                    session,
                                    s,
                                    Type.SQL_VARCHAR_DEFAULT);
                        } else {
                            timestamp = DateTimeType.epochTimestamp;
                        }

                        type = Tokens.VERSIONING;
                        break;

                    case Tokens.DATA :
                        read();

                        type = Tokens.DATA;
                        break;

                    default :
                        type = Tokens.ALL;
                }

                if (readIfThis(Tokens.WITH)) {
                    readThis(Tokens.COLUMN);
                    readThis(Tokens.NAMES);

                    colNames = Boolean.TRUE;
                }

                readThis(Tokens.TO);

                scope = Tokens.DATABASE;
            } else if (readIfThis(Tokens.TABLE)) {
                Table table = readTableName();

                if (table.isView() || table.isTemp()) {
                    throw Error.error(ErrorCode.X_42501);
                }

                tableName = table.getName();

                readThis(Tokens.DATA);

                if (readIfThis(Tokens.WITH)) {
                    readThis(Tokens.COLUMN);
                    readThis(Tokens.NAMES);

                    colNames = Boolean.TRUE;
                }

                readThis(Tokens.TO);

                scope = Tokens.TABLE;
                type  = Tokens.DATA;
            } else {
                throw unexpectedToken();
            }
        }

        if (token.tokenType == Tokens.X_VALUE) {
            if (scope == 0) {
                scope = Tokens.DATABASE;
                type  = Tokens.ALL;
            }

            name = readQuotedString();
        } else if (scope == 0) {
            scope = Tokens.DATABASE;
            type  = Tokens.STRUCTURE;
        } else {
            throw unexpectedTokenRequire(Tokens.T_PATH);
        }

        HsqlName[] names = database.schemaManager.getCatalogAndBaseTableNames();
        Object[]   args  = new Object[] {
            name, Integer.valueOf(
                scope), Integer.valueOf(type), colNames, tableName, timestamp
        };

        return new StatementCommand(
            StatementTypes.DATABASE_SCRIPT,
            args,
            null,
            names);
    }

    private Statement compileConnect() {

        String userName;
        String password = null;

        read();
        readThis(Tokens.USER);
        checkIsSimpleName();

        userName = token.tokenString;

        read();

        if (!session.isProcessingLog()) {
            readThis(Tokens.PASSWORD);

            password = readPassword();
        }

        Expression[] args = new Expression[]{ new ExpressionValue(
            userName,
            Type.SQL_VARCHAR), new ExpressionValue(
                password,
                Type.SQL_VARCHAR) };
        Statement cs = new StatementSession(
            session,
            compileContext,
            StatementTypes.SET_SESSION_AUTHORIZATION,
            args);

        return cs;
    }

    private StatementCommand compileSetDefault() {

        read();

        switch (token.tokenType) {

            case Tokens.INITIAL : {
                read();
                readThis(Tokens.SCHEMA);

                HsqlName schema = database.schemaManager.getSchemaHsqlName(
                    token.tokenString);

                read();

                Object[] args = new Object[]{ schema };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_DEFAULT_INITIAL_SCHEMA,
                    args);
            }

            case Tokens.RESULT : {
                read();
                readThis(Tokens.MEMORY);
                readThis(Tokens.ROWS);

                Integer  size = readIntegerObject();
                Object[] args = new Object[]{ size };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_RESULT_MEMORY_ROWS,
                    args);
            }

            case Tokens.TABLE : {
                read();
                readThis(Tokens.TYPE);

                int type;

                switch (token.tokenType) {

                    case Tokens.MEMORY :
                        type = TableBase.MEMORY_TABLE;
                        break;

                    case Tokens.CACHED :
                        type = TableBase.CACHED_TABLE;
                        break;

                    default :
                        throw unexpectedToken();
                }

                read();

                Object[] args = new Object[]{ ValuePool.getInt(type) };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_DEFAULT_TABLE_TYPE,
                    args);
            }

            case Tokens.ISOLATION : {
                read();
                readThis(Tokens.LEVEL);

                int level;

                switch (token.tokenType) {

                    case Tokens.READ :
                        read();
                        readThis(Tokens.COMMITTED);

                        level = SessionInterface.TX_READ_COMMITTED;
                        break;

                    case Tokens.SERIALIZABLE :
                        read();

                        level = SessionInterface.TX_SERIALIZABLE;
                        break;

                    default :
                        throw unexpectedToken();
                }

                Object[] args = new Object[]{ ValuePool.getInt(level) };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_DEFAULT_ISOLATION_LEVEL,
                    args);
            }

            default :
                throw unexpectedToken();
        }
    }

    private StatementCommand compileSetProperty() {

        // command is a no-op from 1.9
        read();

        String property;
        Object value;

        checkIsSimpleName();
        checkIsDelimitedIdentifier();

        property = token.tokenString;

        read();

        if (token.tokenType == Tokens.TRUE) {
            value = Boolean.TRUE;
        } else if (token.tokenType == Tokens.FALSE) {
            value = Boolean.FALSE;
        } else {
            checkIsValue();

            value = token.tokenValue;
        }

        read();

        Object[] args = new Object[]{ property, value };

        return new StatementCommand(StatementTypes.SET_DATABASE_PROPERTY, args);
    }

    private Statement compileSet() {

        read();

        switch (token.tokenType) {

            case Tokens.CATALOG : {
                read();

                Expression e = XreadValueSpecificationOrNull();

                if (e == null) {
                    HsqlName name = readSchemaName();
                    Object[] args = new Object[]{ name };

                    return new StatementSession(
                        StatementTypes.SET_CATALOG,
                        args);
                }

                if (!e.getDataType().isCharacterType()) {
                    throw Error.error(ErrorCode.X_0P000);
                }

                if (e.getType() != OpTypes.VALUE
                        && (e.getType() != OpTypes.SQL_FUNCTION
                            || !((FunctionSQL) e).isValueFunction())) {
                    throw Error.error(ErrorCode.X_0P000);
                }

                Expression[] args = new Expression[]{ e };

                return new StatementSession(
                    session,
                    compileContext,
                    StatementTypes.SET_CATALOG,
                    args);
            }

            case Tokens.SCHEMA : {
                read();

                Expression e = XreadValueSpecificationOrNull();

                if (e == null) {
                    HsqlName name = readSchemaName();
                    Object[] args = new Object[]{ name };

                    return new StatementSession(
                        StatementTypes.SET_SCHEMA,
                        args);
                }

                if (e.isUnresolvedParam()) {
                    e.setDataType(session, Type.SQL_VARCHAR_DEFAULT);
                }

                if (!e.getDataType().isCharacterType()) {
                    throw Error.error(ErrorCode.X_0P000);
                }

                switch (e.getType()) {

                    case OpTypes.PARAMETER :
                    case OpTypes.DYNAMIC_PARAM :
                    case OpTypes.VALUE :
                        break;

                    case OpTypes.SQL_FUNCTION :
                        if (((FunctionSQL) e).isValueFunction()) {
                            break;
                        }

                        throw Error.error(ErrorCode.X_0P000);

                    default :
                        throw Error.error(ErrorCode.X_0P000);
                }

                Expression[] args = new Expression[]{ e };

                return new StatementSession(
                    session,
                    compileContext,
                    StatementTypes.SET_SCHEMA,
                    args);
            }

            case Tokens.NO : {
                read();
                readThis(Tokens.COLLATION);

                HsqlArrayList<SchemaObject> charsets = null;

                if (readIfThis(Tokens.FOR)) {
                    charsets = new HsqlArrayList<>();

                    while (true) {
                        SchemaObject charset = readSchemaObjectName(
                            SchemaObject.CHARSET);

                        charsets.add(charset);

                        if (token.tokenType == Tokens.COMMA) {
                            read();
                            continue;
                        }

                        break;
                    }
                }

                Object[] args = new Object[]{ null, Boolean.FALSE, charsets };

                return new StatementSession(StatementTypes.SET_COLLATION, args);
            }

            case Tokens.COLLATION : {
                read();

                Expression e = XreadValueSpecificationOrNull();

                if (e == null || !e.getDataType().isCharacterType()) {
                    throw Error.error(ErrorCode.X_2H000);
                }

                HsqlArrayList<SchemaObject> charsets = null;

                if (readIfThis(Tokens.FOR)) {
                    charsets = new HsqlArrayList<>();

                    while (true) {
                        SchemaObject charset = readSchemaObjectName(
                            SchemaObject.CHARSET);

                        charsets.add(charset);

                        if (token.tokenType == Tokens.COMMA) {
                            read();
                            continue;
                        }

                        break;
                    }
                }

                Object[] args = new Object[]{ e, Boolean.TRUE, charsets };

                return new StatementSession(StatementTypes.SET_COLLATION, args);
            }

            case Tokens.TIME : {
                read();

                return compileSetTimeZone();
            }

            case Tokens.ROLE : {
                read();

                return compileSetRole();
            }

            case Tokens.SESSION : {
                read();

                return compileSessionSettings();
            }

            case Tokens.TRANSACTION : {
                readAny(Tokens.READ, Tokens.ISOLATION, 0, 0);

                Object[] args = processTransactionCharacteristics();

                if (args[0] == null && args[1] == null) {
                    throw unexpectedToken();
                }

                return new StatementSession(
                    StatementTypes.SET_TRANSACTION,
                    args);
            }

            case Tokens.AUTOCOMMIT : {
                return compileSetAutoCommit();
            }

            // deprecated
            case Tokens.READONLY : {
                read();

                Boolean  readonly = processTrueOrFalseObject();
                Object[] args     = new Object[]{ readonly };

                return new StatementSession(
                    StatementTypes.SET_SESSION_CHARACTERISTICS,
                    args);
            }

            case Tokens.IGNORECASE : {
                read();

                Boolean  mode = processTrueOrFalseObject();
                Object[] args = new Object[]{ mode };

                return new StatementSession(
                    StatementTypes.SET_SESSION_SQL_IGNORECASE,
                    args);
            }

            case Tokens.MAXROWS : {
                read();

                Integer  size = readIntegerObject();
                Object[] args = new Object[]{ size };

                return new StatementSession(
                    StatementTypes.SET_SESSION_RESULT_MAX_ROWS,
                    args);
            }

            // for backward compatibility
            case Tokens.DEFAULT : {
                read();
                readThis(Tokens.TABLE);
                readThis(Tokens.TYPE);

                int type;

                switch (token.tokenType) {

                    case Tokens.MEMORY :
                        type = TableBase.MEMORY_TABLE;
                        break;

                    case Tokens.CACHED :
                        type = TableBase.CACHED_TABLE;
                        break;

                    default :
                        throw unexpectedToken();
                }

                read();

                Object[] args = new Object[]{ ValuePool.getInt(type) };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_DEFAULT_TABLE_TYPE,
                    args);
            }

            case Tokens.TABLE : {
                return compileSetTable();
            }

            case Tokens.WRITE_DELAY : {
                read();

                int delay = 0;

                if (token.tokenType == Tokens.TRUE) {
                    delay = database.getProperties().getDefaultWriteDelay();

                    read();
                } else if (token.tokenType == Tokens.FALSE) {
                    delay = 0;

                    read();
                } else {
                    delay = readInteger();

                    if (delay < 0) {
                        delay = 0;
                    }

                    if (token.tokenType == Tokens.MILLIS) {
                        read();
                    } else {
                        delay *= 1000;
                    }
                }

                Object[] args = new Object[]{ Integer.valueOf(delay) };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_FILES_WRITE_DELAY,
                    args,
                    null,
                    null);
            }

            case Tokens.PASSWORD : {
                String  password;
                Boolean isDigest = Boolean.FALSE;

                read();

                if (readIfThis(Tokens.DIGEST)) {
                    isDigest = Boolean.TRUE;
                }

                password = readPassword();

                Object[] args = new Object[]{ null, password, isDigest };
                Statement cs = new StatementCommand(
                    StatementTypes.SET_USER_PASSWORD,
                    args);
                String sql = User.getSetCurrentPasswordDigestSQL(
                    database.granteeManager,
                    password,
                    isDigest);

                cs.setSQL(sql);

                return cs;
            }

            case Tokens.INITIAL : {
                read();
                readThis(Tokens.SCHEMA);

                HsqlName schema;

                if (token.tokenType == Tokens.DEFAULT) {
                    schema = null;
                } else {
                    schema = database.schemaManager.getSchemaHsqlName(
                        token.tokenString);
                }

                read();

                Object[] args = new Object[]{ null, schema };

                return new StatementCommand(
                    StatementTypes.SET_USER_INITIAL_SCHEMA,
                    args);
            }

            case Tokens.FILES : {
                return compileSetFilesProperty();
            }

            case Tokens.DATABASE : {
                return compileSetDatabaseProperty();
            }

            case Tokens.PROPERTY : {
                return compileSetProperty();
            }

            default : {
                return compileSetStatement(
                    session.sessionContext.sessionVariableRangeGroups,
                    session.sessionContext.sessionVariablesRange);
            }
        }
    }

    StatementSession compileSetAutoCommit() {

        read();

        boolean mode = false;
        int     rows = -1;

        switch (token.tokenType) {

            case Tokens.TRUE :
            case Tokens.FALSE :
                mode = processTrueOrFalse();
                break;

            case Tokens.AT :
                read();

                rows = readInteger();

                if (rows < 0) {
                    throw Error.error(ErrorCode.X_22003);
                }

                readThis(Tokens.ROWS);
                break;

            default :
                throw unexpectedToken();
        }

        Object[] args = new Object[]{ mode, rows };

        return new StatementSession(
            StatementTypes.SET_SESSION_AUTOCOMMIT,
            args);
    }

    StatementCommand compileSetTable() {

        read();

        Table    table = readTableName();
        Object[] args  = new Object[]{ table.getName(), null, null };

        switch (token.tokenType) {

            default : {
                throw unexpectedToken();
            }

            case Tokens.SOURCE :
                read();

                return compileTableSource(table);

            case Tokens.READ : {
                read();

                boolean readonly = false;

                if (token.tokenType == Tokens.WRITE) {
                    read();
                } else {
                    readThis(Tokens.ONLY);

                    readonly = true;
                }

                args[1] = Boolean.valueOf(readonly);

                return new StatementCommand(
                    StatementTypes.SET_TABLE_READONLY,
                    args,
                    null,
                    new HsqlName[]{ table.getName() });
            }

            // deprecated
            case Tokens.READONLY : {
                read();

                Boolean readonly = processTrueOrFalseObject();

                args[1] = readonly;

                return new StatementCommand(
                    StatementTypes.SET_TABLE_READONLY,
                    args,
                    null,
                    new HsqlName[]{ table.getName() });
            }

            case Tokens.INDEX : {
                String value;

                read();
                checkIsValue();

                value = token.tokenString;

                read();

                args[1] = value;
                args[2] = Integer.valueOf(TableBase.CACHED_TABLE);

                return new StatementCommand(
                    StatementTypes.SET_TABLE_INDEX,
                    args,
                    null,
                    new HsqlName[]{ table.getName() });
            }

            case Tokens.TYPE : {
                read();

                int newType;

                switch (token.tokenType) {

                    case Tokens.MEMORY :
                        newType = TableBase.MEMORY_TABLE;
                        break;

                    case Tokens.CACHED :
                        newType = TableBase.CACHED_TABLE;
                        break;

                    default :
                        throw unexpectedToken();
                }

                switch (table.getTableType()) {

                    case TableBase.MEMORY_TABLE :
                    case TableBase.CACHED_TABLE :
                    case TableBase.TEXT_TABLE :
                        break;

                    default :
                        throw unexpectedToken();
                }

                read();

                args[1] = Integer.valueOf(newType);

                return new StatementCommand(
                    StatementTypes.SET_TABLE_TYPE,
                    args,
                    null,
                    new HsqlName[]{ table.getName() });
            }

            case Tokens.CLUSTERED : {
                read();
                readThis(Tokens.ON);

                OrderedHashSet<String> set = new OrderedHashSet<>();

                readThis(Tokens.OPENBRACKET);
                readSimpleColumnNames(set, table, false);
                readThis(Tokens.CLOSEBRACKET);

                int[] colIndex = table.getColumnIndexes(set);

                args[1] = colIndex;

                return new StatementCommand(
                    StatementTypes.SET_TABLE_CLUSTERED,
                    args,
                    null,
                    new HsqlName[]{ table.getName() });
            }

            case Tokens.NEW : {
                read();
                readThis(Tokens.SPACE);

                args = new Object[]{ table.getName() };

                HsqlName[] writeLockNames =
                    database.schemaManager.getCatalogAndBaseTableNames(
                        table.getName());

                return new StatementCommand(
                    StatementTypes.SET_TABLE_NEW_TABLESPACE,
                    args,
                    null,
                    writeLockNames);
            }

            case Tokens.SPACE : {
                read();

                Integer id = readIntegerObject();

                args = new Object[]{ table.getName(), id };

                HsqlName[] writeLockNames =
                    database.schemaManager.getCatalogAndBaseTableNames(
                        table.getName());

                return new StatementCommand(
                    StatementTypes.SET_TABLE_SET_TABLESPACE,
                    args,
                    null,
                    writeLockNames);
            }
        }
    }

    StatementCommand compileSetDatabaseProperty() {

        read();

        String name;

        checkDatabaseUpdateAuthorisation();

        switch (token.tokenType) {

            case Tokens.AUTHENTICATION : {
                read();
                readThis(Tokens.FUNCTION);

                Routine  routine = readCreateDatabaseAuthenticationFunction();
                Object[] args    = new Object[]{ routine };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_AUTHENTICATION,
                    args,
                    null,
                    null);
            }

            case Tokens.COLLATION : {
                Boolean padSpace = null;

                read();
                checkIsSimpleName();

                name = token.tokenString;

                read();

                if (readIfThis(Tokens.NO)) {
                    readThis(Tokens.PAD);

                    padSpace = Boolean.FALSE;
                } else if (readIfThis(Tokens.PAD)) {
                    readThis(Tokens.SPACE);

                    padSpace = Boolean.TRUE;
                }

                if (padSpace == null) {
                    padSpace = Boolean.TRUE;
                }

                Object[] args = new Object[]{ name, padSpace };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_SQL_COLLATION,
                    args,
                    null,
                    null);
            }

            case Tokens.DEFAULT : {
                return compileSetDefault();
            }

            case Tokens.EVENT : {
                read();
                readThis(Tokens.LOG);

                boolean sqlLog = readIfThis(Tokens.SQL);

                readThis(Tokens.LEVEL);

                Integer value = readIntegerObject();
                Object[] args = new Object[]{ value, Boolean.valueOf(
                    sqlLog), Boolean.TRUE };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_FILES_EVENT_LOG,
                    args,
                    null,
                    null);
            }

            case Tokens.EXTERNAL : {
                read();
                readThis(Tokens.EVENT);
                readThis(Tokens.LOG);
                readThis(Tokens.LEVEL);

                Integer value = readIntegerObject();
                Object[] args = new Object[]{ value, Boolean.FALSE,
                                              Boolean.FALSE };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_FILES_EVENT_LOG,
                    args,
                    null,
                    null);
            }

            case Tokens.GC : {
                read();

                Integer  size = readIntegerObject();
                Object[] args = new Object[]{ size };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_GC,
                    args,
                    null,
                    null);
            }

            case Tokens.PASSWORD : {
                read();

                switch (token.tokenType) {

                    case Tokens.CHECK : {
                        read();
                        readThis(Tokens.FUNCTION);

                        Routine  routine = readCreatePasswordCheckFunction();
                        Object[] args    = new Object[]{ routine };

                        return new StatementCommand(
                            StatementTypes.SET_DATABASE_PASSWORD_CHECK,
                            args,
                            null,
                            null);
                    }

                    case Tokens.DIGEST : {
                        read();

                        name = readQuotedString();

                        Object[] args = new Object[]{ name };

                        return new StatementCommand(
                            StatementTypes.SET_DATABASE_PASSWORD_DIGEST,
                            args,
                            null,
                            null);
                    }

                    default :
                        throw unexpectedToken();
                }
            }

            case Tokens.REFERENTIAL : {
                read();
                readThis(Tokens.INTEGRITY);

                boolean  mode = processTrueOrFalse();
                Object[] args = new Object[]{ Boolean.valueOf(mode) };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_SQL_REFERENTIAL_INTEGRITY,
                    args,
                    null,
                    null);
            }

            case Tokens.SQL : {
                read();

                int     type     = StatementTypes.SET_DATABASE_SQL;
                Boolean flag     = Boolean.TRUE;
                Integer value    = Integer.valueOf(0);
                String  property = null;

                switch (token.tokenType) {

                    case Tokens.AVG :
                        read();
                        readThis(Tokens.SCALE);

                        value    = readIntegerObject();
                        property = HsqlDatabaseProperties.sql_avg_scale;
                        break;

                    case Tokens.CHARACTER :
                        read();
                        readThis(Tokens.LITERAL);

                        flag     = processTrueOrFalseObject();
                        property = HsqlDatabaseProperties.sql_char_literal;
                        break;

                    case Tokens.CONCAT_WORD :
                        read();
                        readThis(Tokens.NULLS);

                        flag     = processTrueOrFalseObject();
                        property = HsqlDatabaseProperties.sql_concat_nulls;
                        break;

                    case Tokens.CONVERT :
                        read();
                        readThis(Tokens.TRUNCATE);

                        flag     = processTrueOrFalseObject();
                        property = HsqlDatabaseProperties.sql_convert_trunc;
                        break;

                    case Tokens.DOUBLE :
                        read();
                        readThis(Tokens.NAN);

                        flag     = processTrueOrFalseObject();
                        property = HsqlDatabaseProperties.sql_double_nan;
                        break;

                    case Tokens.IGNORECASE :
                        read();

                        flag     = processTrueOrFalseObject();
                        property = HsqlDatabaseProperties.sql_ignore_case;
                        break;

                    case Tokens.LIVE :
                        read();
                        readThis(Tokens.OBJECT);

                        property = HsqlDatabaseProperties.sql_live_object;
                        flag     = processTrueOrFalseObject();
                        break;

                    case Tokens.LONGVAR :
                        read();
                        readThis(Tokens.IS);
                        readThis(Tokens.LOB);

                        flag     = processTrueOrFalseObject();
                        property = HsqlDatabaseProperties.sql_longvar_is_lob;
                        break;

                    case Tokens.LOWER :
                        read();
                        readThis(Tokens.CASE);
                        readThis(Tokens.IDENTIFIER);

                        flag     = processTrueOrFalseObject();
                        property = HsqlDatabaseProperties.sql_lowercase_ident;
                        break;

                    case Tokens.MAX :
                        read();
                        readThis(Tokens.RECURSIVE);

                        value    = readIntegerObject();
                        property = HsqlDatabaseProperties.sql_max_recursive;
                        break;

                    case Tokens.NAMES :
                        read();

                        property = HsqlDatabaseProperties.sql_enforce_names;
                        flag     = processTrueOrFalseObject();
                        break;

                    case Tokens.NULLS :
                        read();

                        if (readIfThis(Tokens.FIRST)) {
                            property = HsqlDatabaseProperties.sql_nulls_first;
                        } else {
                            readThis(Tokens.ORDER);

                            property = HsqlDatabaseProperties.sql_nulls_order;
                        }

                        flag = processTrueOrFalseObject();
                        break;

                    case Tokens.REGULAR :
                        read();
                        readThis(Tokens.NAMES);

                        property = HsqlDatabaseProperties.sql_regular_names;
                        flag     = processTrueOrFalseObject();
                        break;

                    case Tokens.REFERENCES :
                        read();

                        flag     = processTrueOrFalseObject();
                        property = HsqlDatabaseProperties.sql_enforce_refs;
                        break;

                    case Tokens.RESTRICT :
                        read();
                        readThis(Tokens.EXEC);

                        property = HsqlDatabaseProperties.sql_restrict_exec;
                        flag     = processTrueOrFalseObject();
                        break;

                    case Tokens.SIZE :
                        read();

                        flag     = processTrueOrFalseObject();
                        property = HsqlDatabaseProperties.sql_enforce_size;
                        break;

                    case Tokens.SYNTAX :
                        read();

                        if (token.tokenString.equals(Tokens.T_DB2)) {
                            read();

                            property = HsqlDatabaseProperties.sql_syntax_db2;
                        } else if (token.tokenString.equals(Tokens.T_MSS)) {
                            read();

                            property = HsqlDatabaseProperties.sql_syntax_mss;
                        } else if (token.tokenString.equals(Tokens.T_MYS)) {
                            read();

                            property = HsqlDatabaseProperties.sql_syntax_mys;
                        } else if (token.tokenString.equals(Tokens.T_ORA)) {
                            read();

                            property = HsqlDatabaseProperties.sql_syntax_ora;
                        } else if (token.tokenString.equals(Tokens.T_PGS)) {
                            read();

                            property = HsqlDatabaseProperties.sql_syntax_pgs;
                        } else {
                            throw unexpectedToken();
                        }

                        flag = processTrueOrFalseObject();
                        break;

                    case Tokens.SYS :
                        read();
                        readThis(Tokens.INDEX);
                        readThis(Tokens.NAMES);

                        flag     = processTrueOrFalseObject();
                        property = HsqlDatabaseProperties.sql_sys_index_names;
                        break;

                    case Tokens.TDC :
                        read();

                        if (readIfThis(Tokens.DELETE)) {
                            property = HsqlDatabaseProperties.sql_enforce_tdcd;
                        } else {
                            readThis(Tokens.UPDATE);

                            property = HsqlDatabaseProperties.sql_enforce_tdcu;
                        }

                        flag = processTrueOrFalseObject();
                        break;

                    case Tokens.TRANSLATE :
                        read();
                        readThis(Tokens.TTI);
                        readThis(Tokens.TYPES);

                        flag = processTrueOrFalseObject();
                        property =
                            HsqlDatabaseProperties.jdbc_translate_tti_types;
                        break;

                    case Tokens.TRUNCATE :
                        read();
                        readThis(Tokens.TRAILING);

                        flag     = processTrueOrFalseObject();
                        property = HsqlDatabaseProperties.sql_trunc_trailing;
                        break;

                    case Tokens.TYPES :
                        read();

                        flag     = processTrueOrFalseObject();
                        property = HsqlDatabaseProperties.sql_enforce_types;
                        break;

                    case Tokens.UNIQUE :
                        read();
                        readThis(Tokens.NULLS);

                        flag     = processTrueOrFalseObject();
                        property = HsqlDatabaseProperties.sql_unique_nulls;
                        break;

                    default :
                        throw unexpectedToken();
                }

                Object[] args = new Object[]{ property, flag, value };

                return new StatementCommand(type, args, null, null);
            }

            case Tokens.TEXT : {
                read();
                readThis(Tokens.TABLE);
                readThis(Tokens.DEFAULTS);

                String   source = readQuotedString();
                Object[] args   = new Object[]{ source };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_TEXT_SOURCE,
                    args,
                    null,
                    null);
            }

            case Tokens.TRANSACTION : {
                read();

                if (readIfThis(Tokens.ROLLBACK)) {
                    readThis(Tokens.ON);

                    if (readIfThis(Tokens.INTERRUPT)) {
                        Boolean mode = processTrueOrFalseObject();
                        StatementCommand cs = new StatementCommand(
                            StatementTypes.SET_DATABASE_TRANSACTION_INTERRUPT,
                            new Object[]{ mode },
                            null,
                            null);

                        return cs;
                    } else {
                        readThis(Tokens.CONFLICT);

                        Boolean mode = processTrueOrFalseObject();
                        StatementCommand cs = new StatementCommand(
                            StatementTypes.SET_DATABASE_TRANSACTION_CONFLICT,
                            new Object[]{ mode },
                            null,
                            null);

                        return cs;
                    }
                }

                readThis(Tokens.CONTROL);

                int mode = TransactionManager.LOCKS;

                switch (token.tokenType) {

                    case Tokens.MVCC :
                        read();

                        mode = TransactionManager.MVCC;
                        break;

                    case Tokens.MVLOCKS :
                        read();

                        mode = TransactionManager.MVLOCKS;
                        break;

                    case Tokens.LOCKS :
                        read();

                        mode = TransactionManager.LOCKS;
                        break;

                    default :
                }

                HsqlName[] names =
                    database.schemaManager.getCatalogAndBaseTableNames();
                Object[] args = new Object[]{ ValuePool.getInt(mode) };
                StatementCommand cs = new StatementCommand(
                    StatementTypes.SET_DATABASE_TRANSACTION_CONTROL,
                    args,
                    null,
                    names);

                return cs;
            }

            case Tokens.UNIQUE : {
                read();
                readThis(Tokens.NAME);

                if (!isUndelimitedSimpleName()) {
                    throw unexpectedToken();
                }

                name = token.tokenString;

                read();

                if (name.length() != 16) {
                    throw Error.error(ErrorCode.X_42555);
                }

                if (!Charset.isInSet(name, Charset.unquotedIdentifier)
                        || !Charset.startsWith(name,
                                               Charset.uppercaseLetters)) {
                    throw Error.error(ErrorCode.X_42501);
                }

                Object[] args = new Object[]{ name };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_UNIQUE_NAME,
                    args,
                    null,
                    null);
            }

            default : {
                throw unexpectedToken();
            }
        }
    }

    StatementCommand compileSetFilesProperty() {

        read();

        int        type  = 0;
        Boolean    flag  = null;
        Object     value = null;
        Boolean    mode  = null;
        HsqlName[] names = database.schemaManager.getCatalogNameArray();

        checkDatabaseUpdateAuthorisation();

        switch (token.tokenType) {

            case Tokens.CHECK : {
                read();

                long longValue1 = readBigint();
                long longValue2 = -1;

                type  = StatementTypes.SET_DATABASE_FILES_CHECK;
                names = database.schemaManager.getCatalogNameArray();

                if (readIfThis(Tokens.COMMA)) {
                    longValue2 = readBigint();
                }

                Object[] args = new Object[2];

                args[0] = Long.valueOf(longValue1);
                args[1] = Long.valueOf(longValue2);

                return new StatementCommand(type, args, null, names);
            }

            case Tokens.CACHE : {
                read();

                if (readIfThis(Tokens.SIZE)) {
                    value = readIntegerObject();
                    type  = StatementTypes.SET_DATABASE_FILES_CACHE_SIZE;
                } else {
                    readThis(Tokens.ROWS);

                    value = readIntegerObject();
                    type  = StatementTypes.SET_DATABASE_FILES_CACHE_ROWS;
                }

                if (readIfThis(Tokens.NO)) {
                    readThis(Tokens.CHECK);

                    mode = Boolean.TRUE;
                }

                break;
            }

            case Tokens.SCALE : {
                read();

                value = readIntegerObject();
                type  = StatementTypes.SET_DATABASE_FILES_SCALE;
                names = database.schemaManager.getCatalogAndBaseTableNames();
                break;
            }

            case Tokens.SPACE : {
                read();

                if (token.tokenType == Tokens.TRUE) {
                    flag = Boolean.TRUE;

                    read();
                } else if (token.tokenType == Tokens.FALSE) {
                    flag = Boolean.FALSE;

                    read();
                } else {
                    value = readIntegerObject();
                }

                type  = StatementTypes.SET_DATABASE_FILES_SPACE;
                names = database.schemaManager.getCatalogAndBaseTableNames();
                break;
            }

            case Tokens.LOB : {
                read();

                if (readIfThis(Tokens.SCALE)) {
                    value = readIntegerObject();
                    type  = StatementTypes.SET_DATABASE_FILES_LOBS_SCALE;
                } else {
                    readThis(Tokens.COMPRESSED);

                    type = StatementTypes.SET_DATABASE_FILES_LOBS_COMPRESSED;
                    flag = processTrueOrFalseObject();
                }

                names = database.schemaManager.getCatalogAndBaseTableNames();
                break;
            }

            case Tokens.DEFRAG : {
                read();

                type  = StatementTypes.SET_DATABASE_FILES_DEFRAG;
                value = readIntegerObject();
                break;
            }

            case Tokens.NIO : {
                read();

                if (readIfThis(Tokens.SIZE)) {
                    value = readIntegerObject();
                } else {
                    flag = processTrueOrFalseObject();
                }

                type = StatementTypes.SET_DATABASE_FILES_NIO;
                break;
            }

            case Tokens.BACKUP : {
                read();

                type = StatementTypes.SET_DATABASE_FILES_BACKUP_INCREMENT;

                readThis(Tokens.INCREMENT);

                flag = processTrueOrFalseObject();
                break;
            }

            case Tokens.LOG : {
                read();

                if (readIfThis(Tokens.SIZE)) {
                    type  = StatementTypes.SET_DATABASE_FILES_LOG_SIZE;
                    value = readIntegerObject();
                } else {
                    type = StatementTypes.SET_DATABASE_FILES_LOG;
                    flag = processTrueOrFalseObject();
                }

                break;
            }

            case Tokens.TEMP : {
                read();
                readThis(Tokens.PATH);

                type  = StatementTypes.SET_DATABASE_FILES_TEMP_PATH;
                value = readQuotedString();
                break;
            }

            case Tokens.WRITE : {
                read();
                readThis(Tokens.DELAY);

                type = StatementTypes.SET_DATABASE_FILES_WRITE_DELAY;

                int delay = 0;

                if (token.tokenType == Tokens.TRUE) {
                    delay = database.getProperties().getDefaultWriteDelay();

                    read();
                } else if (token.tokenType == Tokens.FALSE) {
                    delay = 0;

                    read();
                } else {
                    delay = readInteger();

                    if (delay < 0) {
                        delay = 0;
                    }

                    if (token.tokenType == Tokens.MILLIS) {
                        read();
                    } else {
                        delay *= 1000;
                    }
                }

                value = Integer.valueOf(delay);
                break;
            }

            case Tokens.SCRIPT : {
                read();
                readThis(Tokens.FORMAT);

                if (token.tokenType == Tokens.TEXT) {
                    read();

                    value = Integer.valueOf(0);
                } else {
                    readThis(Tokens.COMPRESSED);

                    value = Integer.valueOf(3);
                }

                type = StatementTypes.SET_DATABASE_FILES_SCRIPT_FORMAT;
                break;
            }

            default :
                throw unexpectedToken();
        }

        Object[] args = new Object[2];

        args[0] = flag == null
                  ? value
                  : flag;
        args[1] = mode;

        return new StatementCommand(type, args, null, names);
    }

    Object[] processTransactionCharacteristics() {

        int      level    = 0;
        boolean  readonly = false;
        Object[] args     = new Object[2];

        outerloop:
        while (true) {
            switch (token.tokenType) {

                case Tokens.READ : {
                    if (args[0] != null) {
                        throw unexpectedToken();
                    }

                    read();

                    if (token.tokenType == Tokens.ONLY) {
                        read();

                        readonly = true;
                    } else {
                        readThis(Tokens.WRITE);

                        readonly = false;
                    }

                    args[0] = Boolean.valueOf(readonly);
                    break;
                }

                case Tokens.ISOLATION : {
                    if (args[1] != null) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.LEVEL);
                    checkIsAny(
                        Tokens.SERIALIZABLE,
                        Tokens.READ,
                        Tokens.REPEATABLE,
                        0);

                    switch (token.tokenType) {

                        case Tokens.SERIALIZABLE :
                            read();

                            level = SessionInterface.TX_SERIALIZABLE;
                            break;

                        case Tokens.READ :
                            readAny(Tokens.COMMITTED, Tokens.UNCOMMITTED, 0, 0);

                            if (token.tokenType == Tokens.COMMITTED) {
                                read();

                                level = SessionInterface.TX_READ_COMMITTED;
                            } else if (token.tokenType == Tokens.UNCOMMITTED) {
                                read();

                                level = SessionInterface.TX_READ_UNCOMMITTED;
                            } else {
                                throw unexpectedToken();
                            }

                            break;

                        case Tokens.REPEATABLE :
                            read();
                            readThis(Tokens.READ);

                            level = SessionInterface.TX_REPEATABLE_READ;
                            break;

                        default :
                            throw unexpectedToken();
                    }

                    args[1] = Integer.valueOf(level);
                    break;
                }

                case Tokens.COMMA : {
                    if (args[0] == null && args[1] == null) {
                        throw unexpectedToken();
                    }

                    read();
                    break;
                }

                default : {
                    break outerloop;
                }
            }
        }

        return args;
    }

    /**
     * Responsible for  handling the execution of COMMIT [WORK]
     */
    private Statement compileCommit() {

        boolean chain = false;

        read();
        readIfThis(Tokens.WORK);

        if (token.tokenType == Tokens.AND) {
            read();

            if (token.tokenType == Tokens.NO) {
                read();
            } else {
                chain = true;
            }

            readThis(Tokens.CHAIN);
        }

        String sql = chain
                     ? StatementSession.commitAndChainStatement.sql
                     : StatementSession.commitNoChainStatement.sql;
        Statement st = new StatementSession(
            StatementTypes.COMMIT_WORK,
            new Object[]{ Boolean.valueOf(chain) });

        st.setSQL(sql);

        return st;
    }

    private Statement compileStartTransaction() {

        read();
        readThis(Tokens.TRANSACTION);

        Object[] args = processTransactionCharacteristics();
        Statement cs = new StatementSession(
            StatementTypes.START_TRANSACTION,
            args);

        return cs;
    }

    private Statement compileLock() {

        read();

        if (readIfThis(Tokens.CATALOG)) {
            return compileLockCatalog();
        } else {
            readThis(Tokens.TABLE);

            return compileLockTable();
        }
    }

    private Statement compileLockCatalog() {

        boolean    isLock          = processTrueOrFalse();
        int        statementType   = isLock
                                     ? StatementTypes.TRANSACTION_LOCK_CATALOG
                                     : StatementTypes.TRANSACTION_UNLOCK_CATALOG;
        HsqlName[] writeTableNames = isLock
                                     ? database.schemaManager.getCatalogAndBaseTableNames()
                                     : null;
        Statement cs = new StatementSession(
            statementType,
            null,
            writeTableNames);

        return cs;
    }

    private Statement compileLockTable() {

        OrderedHashSet<HsqlName> readSet  = new OrderedHashSet<>();
        OrderedHashSet<HsqlName> writeSet = new OrderedHashSet<>();

        while (true) {
            Table table = readTableName(true);

            switch (token.tokenType) {

                case Tokens.READ :
                    read();
                    readSet.add(table.getName());
                    break;

                case Tokens.WRITE :
                    read();
                    writeSet.add(table.getName());
                    break;

                default :
                    throw unexpectedToken();
            }

            if (token.tokenType == Tokens.COMMA) {
                read();
                continue;
            }

            break;
        }

        HsqlName[] writeTableNames = new HsqlName[writeSet.size()];

        writeSet.toArray(writeTableNames);
        readSet.removeAll(writeTableNames);

        HsqlName[] readTableNames = new HsqlName[readSet.size()];

        readSet.toArray(readTableNames);

        Statement cs = new StatementSession(
            StatementTypes.TRANSACTION_LOCK_TABLE,
            readTableNames,
            writeTableNames);

        return cs;
    }

    private Statement compileRollback() {

        boolean chain     = false;
        String  savepoint = null;

        read();

        if (token.tokenType == Tokens.WORK) {
            read();
        }

        if (token.tokenType == Tokens.TO) {
            read();
            readThis(Tokens.SAVEPOINT);
            checkIsSimpleName();

            savepoint = token.tokenString;

            read();

            Object[] args = new Object[]{ savepoint };
            Statement cs = new StatementSession(
                StatementTypes.ROLLBACK_SAVEPOINT,
                args);

            return cs;
        } else {
            if (token.tokenType == Tokens.AND) {
                read();

                if (token.tokenType == Tokens.NO) {
                    read();
                } else {
                    chain = true;
                }

                readThis(Tokens.CHAIN);
            }
        }

        String sql = chain
                     ? StatementSession.rollbackAndChainStatement.sql
                     : StatementSession.rollbackNoChainStatement.sql;
        Statement st = new StatementSession(
            StatementTypes.ROLLBACK_WORK,
            new Object[]{ Boolean.valueOf(chain) });

        st.setSQL(sql);

        return st;
    }

    private Statement compileSavepoint() {

        String name;

        read();
        checkIsSimpleName();

        name = token.tokenString;

        read();

        Object[] args = new Object[]{ name };

        return new StatementSession(StatementTypes.SAVEPOINT, args);
    }

    private Statement compileReleaseSavepoint() {

        read();
        readThis(Tokens.SAVEPOINT);

        String name = token.tokenString;

        read();

        Object[] args = new Object[]{ name };

        return new StatementSession(StatementTypes.RELEASE_SAVEPOINT, args);
    }

    private Statement compileSessionSettings() {

        checkIsAny(
            Tokens.CHARACTERISTICS,
            Tokens.AUTHORIZATION,
            Tokens.RESULT,
            Tokens.FEATURE);

        switch (token.tokenType) {

            case Tokens.CHARACTERISTICS : {
                read();
                readThis(Tokens.AS);
                readThis(Tokens.TRANSACTION);
                checkIsAny(Tokens.READ, Tokens.ISOLATION, 0, 0);

                Object[] args = processTransactionCharacteristics();

                return new StatementSession(
                    StatementTypes.SET_SESSION_CHARACTERISTICS,
                    args);
            }

            case Tokens.AUTHORIZATION : {
                read();

                Expression e = XreadValueSpecificationOrNull();

                if (e == null) {
                    throw Error.error(ErrorCode.X_42584);
                }

                e.resolveTypes(session, null);

                if (e.isUnresolvedParam()) {
                    e.dataType = Type.SQL_VARCHAR;
                }

                if (e.dataType == null || !e.dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                Expression[] args = new Expression[]{ e, null };

                return new StatementSession(
                    session,
                    compileContext,
                    StatementTypes.SET_SESSION_AUTHORIZATION,
                    args);
            }

            case Tokens.RESULT : {
                read();
                readThis(Tokens.MEMORY);
                readThis(Tokens.ROWS);

                Integer  size = readIntegerObject();
                Object[] args = new Object[]{ size };

                return new StatementSession(
                    StatementTypes.SET_SESSION_RESULT_MEMORY_ROWS,
                    args);
            }

            case Tokens.FEATURE : {
                read();

                String   feature = parseSQLFeatureValue();
                Boolean  value   = processTrueOrFalseObject();
                Object[] args    = new Object[]{ feature, value };

                return new StatementSession(
                    StatementTypes.SET_SESSION_FEATURE,
                    args);
            }

            default :
                throw unexpectedToken();
        }
    }

    private Statement compileSetRole() {

        Expression e;

        if (token.tokenType == Tokens.NONE) {
            read();

            e = null;
        } else {
            e = XreadValueSpecificationOrNull();

            if (e == null) {
                throw Error.error(ErrorCode.X_2A000);
            }
        }

        return new StatementSession(
            session,
            compileContext,
            StatementTypes.SET_ROLE,
            new Expression[]{ e });
    }

    private Statement compileSetTimeZone() {

        Expression e;

        readThis(Tokens.ZONE);

        switch (token.tokenType) {

            case Tokens.LOCAL : {
                read();

                e = new ExpressionValue(null, Type.SQL_INTERVAL_HOUR_TO_MINUTE);
                break;
            }

            case Tokens.INTERVAL : {
                e = XreadIntervalValueExpression();

                List<Expression> unresolved = e.resolveColumnReferences(
                    session,
                    RangeGroup.emptyGroup,
                    RangeGroup.emptyArray,
                    null);

                ExpressionColumn.checkColumnsResolved(unresolved);
                e.resolveTypes(session, null);

                if (e.dataType == null) {
                    throw Error.error(ErrorCode.X_42563);
                }

                if (e.dataType.typeCode != Types.SQL_INTERVAL_HOUR_TO_MINUTE) {
                    throw Error.error(ErrorCode.X_42563);
                }

                break;
            }

            default :
                e = XreadValueExpression();
        }

        return new StatementSession(
            session,
            compileContext,
            StatementTypes.SET_TIME_ZONE,
            new Expression[]{ e });
    }

    private Statement compileShutdown() {

        int closemode;

        session.checkAdmin();

        closemode = Database.CLOSEMODE_NORMAL;

        read();

        switch (token.tokenType) {

            case Tokens.IMMEDIATELY :
                closemode = Database.CLOSEMODE_IMMEDIATELY;

                read();
                break;

            case Tokens.COMPACT :
                closemode = Database.CLOSEMODE_COMPACT;

                read();
                break;

            case Tokens.SCRIPT :
                closemode = Database.CLOSEMODE_SCRIPT;

                read();
                break;

            default :
        }

        // only semicolon is accepted here
        if (token.tokenType == Tokens.SEMICOLON) {
            read();
        }

        if (token.tokenType != Tokens.X_ENDPARSE) {
            throw unexpectedToken();
        }

        Object[] args = new Object[]{ Integer.valueOf(closemode) };
        Statement cs = new StatementCommand(
            StatementTypes.DATABASE_SHUTDOWN,
            args,
            null,
            null);

        return cs;
    }

    private Statement compileBackup() {

        String  path;
        Boolean blockingMode = null;    // defaults to blocking
        Boolean scriptMode   = null;    // defaults to non-script
        Boolean compression  = null;    // defaults to compressed
        Boolean files        = null;    // defaults to false

        read();
        readThis(Tokens.DATABASE);
        readThis(Tokens.TO);

        path = readQuotedString();
        path = path.trim();

        if (path.isEmpty()) {
            throw unexpectedToken(path);
        }

        outerLoop:
        while (true) {
            switch (token.tokenType) {

                case Tokens.BLOCKING :
                    if (blockingMode != null) {
                        throw unexpectedToken();
                    }

                    blockingMode = Boolean.TRUE;

                    read();
                    break;

                case Tokens.SCRIPT :
                    if (scriptMode != null) {
                        throw unexpectedToken();
                    }

                    scriptMode = Boolean.TRUE;

                    read();
                    break;

                case Tokens.COMPRESSED :
                    if (compression != null) {
                        throw unexpectedToken();
                    }

                    compression = Boolean.TRUE;

                    read();
                    break;

                case Tokens.NOT :
                    read();

                    if (token.tokenType == Tokens.COMPRESSED) {
                        if (compression != null) {
                            throw unexpectedToken();
                        }

                        compression = Boolean.FALSE;

                        read();
                    } else if (token.tokenType == Tokens.BLOCKING) {
                        if (blockingMode != null) {
                            throw unexpectedToken();
                        }

                        blockingMode = Boolean.FALSE;

                        read();
                    } else {
                        throw unexpectedToken();
                    }

                    break;

                case Tokens.AS :
                    if (files != null) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.FILES);

                    files = Boolean.TRUE;
                    break;

                default :
                    break outerLoop;
            }
        }

        if (scriptMode == null) {
            scriptMode = Boolean.FALSE;
        }

        if (blockingMode == null) {
            blockingMode = Boolean.TRUE;
        }

        if (compression == null) {
            compression = Boolean.TRUE;
        }

        if (files == null) {
            files = Boolean.FALSE;
        }

        if (scriptMode) {
            if (!blockingMode) {
                throw unexpectedToken(Tokens.T_NOT);
            }
        }

        HsqlName[] names = blockingMode
                           ? database.schemaManager.getCatalogAndBaseTableNames()
                           : HsqlName.emptyArray;
        Object[] args = new Object[]{ path, blockingMode, scriptMode,
                                      compression, files };
        Statement cs = new StatementCommand(
            StatementTypes.DATABASE_BACKUP,
            args,
            null,
            names);

        return cs;
    }

    private Statement compilePerform() {

        readAny(Tokens.CHECK, Tokens.EXPORT, Tokens.IMPORT, 0);

        switch (token.tokenType) {

            case Tokens.CHECK :
                return compileCheck();

            case Tokens.IMPORT :
                readAny(Tokens.SCRIPT, Tokens.DATA, 0, 0);

                if (token.tokenType == Tokens.SCRIPT) {
                    return compileImportScript();
                } else {
                    return compileImportData();
                }
            case Tokens.EXPORT :
                readAny(Tokens.SCRIPT, Tokens.DATA, 0, 0);

                if (token.tokenType == Tokens.SCRIPT) {
                    return compileScript(true);
                } else {
                    return compileExportData();
                }
            default :
                throw unexpectedToken();
        }
    }

    /*
     * PERFORM CHECK TABLE <name> INDEX [AND FIX]
     * PERFORM CHECK ALL TABLE INDEX [AND FIX]
     */
    private Statement compileCheck() {

        readAny(Tokens.ALL, Tokens.TABLE, 0, 0);

        boolean  isAll     = false;
        HsqlName tableName = null;
        Integer  type      = Integer.valueOf(IndexStats.checkRows);
        Integer  number    = Integer.valueOf(-1);

        switch (token.tokenType) {

            case Tokens.ALL : {
                read();

                isAll = true;
            }

            // fall through
            case Tokens.TABLE : {
                readThis(Tokens.TABLE);

                if (isAll) {
                    readThis(Tokens.INDEX);
                } else {
                    tableName = readTableName().getName();

                    readThis(Tokens.INDEX);
                }
            }
        }

        if (readIfThis(Tokens.AND)) {
            readThis("FIX");

            type = Integer.valueOf(IndexStats.fixAll);
        }

        Object[]   args  = new Object[]{ tableName, type, number };
        HsqlName[] names = isAll
                           ? database.schemaManager.getCatalogAndBaseTableNames()
                           : database.schemaManager.getCatalogAndBaseTableNames(
                               tableName);

        return new StatementCommand(
            StatementTypes.CHECK_INDEX,
            args,
            null,
            names);
    }

    private Statement compileExportData() {

        readThis(Tokens.DATA);
        readThis(Tokens.FROM);
        readThis(Tokens.TABLE);

        HsqlName tableName = readTableName().getName();

        readThis(Tokens.TO);

        String     fileName = readQuotedString();
        HsqlName[] names = database.schemaManager.getCatalogAndBaseTableNames();
        Object[]   args     = new Object[]{ tableName, fileName };

        return new StatementCommand(
            StatementTypes.UNLOAD_DATA,
            args,
            null,
            names);
    }

    private Statement compileImportScript() {

        String  fileName;
        int     mode         = RowInsertInterface.modes.continueOnError;
        Boolean isVersioning = Boolean.FALSE;

        readThis(Tokens.SCRIPT);
        checkIsAny(Tokens.VERSIONING, Tokens.DATA, 0, 0);

        if (token.tokenType == Tokens.VERSIONING) {
            readThis(Tokens.VERSIONING);

            isVersioning = Boolean.TRUE;
        }

        readThis(Tokens.DATA);
        readThis(Tokens.FROM);

        fileName = readQuotedString();

        if (!isVersioning) {
            mode = readLoadMode();
        }

        HsqlName[] names = database.schemaManager.getCatalogAndBaseTableNames();
        Object[] args = new Object[]{ fileName, Integer.valueOf(
            mode), isVersioning };

        return new StatementCommand(
            StatementTypes.LOAD_SCRIPT,
            args,
            null,
            names);
    }

    private Statement compileImportData() {

        readThis(Tokens.DATA);
        readThis(Tokens.INTO);
        readThis(Tokens.TABLE);

        HsqlName tableName = readTableName().getName();

        readThis(Tokens.FROM);

        String     fileName = readQuotedString();
        int        mode     = readLoadMode();
        HsqlName[] names = database.schemaManager.getCatalogAndBaseTableNames();
        Object[] args = new Object[]{ tableName, fileName,
                                      Integer.valueOf(mode) };

        return new StatementCommand(
            StatementTypes.LOAD_DATA,
            args,
            null,
            names);
    }

    private int readLoadMode() {

        int mode = -1;

        checkIsAny(Tokens.CONTINUE, Tokens.STOP, Tokens.CHECK, 0);

        switch (token.tokenType) {

            case Tokens.CONTINUE :
                mode = RowInsertInterface.modes.continueOnError;

                read();
                break;

            case Tokens.STOP :
                mode = RowInsertInterface.modes.stopOnError;

                read();
                break;

            case Tokens.CHECK :
                mode = RowInsertInterface.modes.checkUntillError;

                read();
                break;

            default :
                throw unexpectedToken();
        }

        readThis(Tokens.ON);
        readThis(Tokens.ERROR);

        return mode;
    }

    private Statement compileCheckpoint() {

        boolean defrag = false;

        read();

        if (token.tokenType == Tokens.DEFRAG) {
            defrag = true;

            read();
        } else if (token.tokenType == Tokens.SEMICOLON) {
            read();

            // only semicolon is accepted here
        }

        if (token.tokenType != Tokens.X_ENDPARSE) {
            throw unexpectedToken();
        }

        HsqlName[] names = database.schemaManager.getCatalogAndBaseTableNames();
        Object[]   args  = new Object[]{ Boolean.valueOf(defrag) };
        Statement cs = new StatementCommand(
            StatementTypes.DATABASE_CHECKPOINT,
            args,
            null,
            names);

        return cs;
    }

    public static Statement getAutoCheckpointStatement(Database database) {

        HsqlName[] names = database.schemaManager.getCatalogAndBaseTableNames();
        Object[]   args  = new Object[]{ Boolean.FALSE };
        Statement cs = new StatementCommand(
            StatementTypes.DATABASE_CHECKPOINT,
            args,
            null,
            names);

        cs.setCompileTimestamp(database.txManager.getSystemChangeNumber());
        cs.setSQL(Tokens.T_CHECKPOINT);

        return cs;
    }

    private Statement compileDisconnect() {

        read();

        String    sql = Tokens.T_DISCONNECT;
        Statement cs  = new StatementSession(StatementTypes.DISCONNECT, null);

        return cs;
    }

    private Statement compileExplain() {

        Statement cs;
        int       position = getPosition();

        readAny(Tokens.PLAN, Tokens.REFERENCES, 0, 0);

        if (token.tokenType == Tokens.PLAN) {
            cs = compileExplainPlan();
        } else {
            cs = compileExplainReferences();
        }

        cs.setSQL(getLastPart(position));

        return cs;
    }

    private Statement compileExplainPlan() {

        Statement cs;

        readThis(Tokens.PLAN);
        readThis(Tokens.FOR);

        cs = compilePart(ResultProperties.defaultPropsValue);

        cs.setDescribe();

        return new StatementCommand(
            StatementTypes.EXPLAIN_PLAN,
            new Object[]{ cs });
    }

    private Statement compileExplainReferences() {

        SchemaObject object;
        boolean      referencesFrom = false;

        readAny(Tokens.TO, Tokens.FROM, 0, 0);

        if (!readIfThis(Tokens.TO)) {
            readThis(Tokens.FROM);

            referencesFrom = true;
        }

        int type;

        switch (token.tokenType) {

            case Tokens.TABLE :
            case Tokens.VIEW :
                read();

                type = SchemaObject.TABLE;
                break;

            case Tokens.SPECIFIC :
                read();
                readThis(Tokens.ROUTINE);

                type = SchemaObject.SPECIFIC_ROUTINE;
                break;

            case Tokens.DOMAIN :
            case Tokens.TYPE :
                read();

                type = SchemaObject.DOMAIN;
                break;

            case Tokens.SEQUENCE :
                read();

                type = SchemaObject.SEQUENCE;
                break;

            default :
                throw unexpectedToken();
        }

        object = readSchemaObjectName(type);

        HsqlName name = object.getName();

        if (object instanceof Routine) {
            name = ((Routine) object).getSpecificName();
        }

        return new StatementCommand(
            StatementTypes.EXPLAIN_REFERENCES,
            new Object[]{ name, Boolean.valueOf(referencesFrom) });
    }

    private StatementCommand compileTableSource(Table t) {

        boolean  isSourceHeader = false;
        boolean  isDesc         = false;
        String   source;
        Object[] args = new Object[5];

        args[0] = t.getName();

        if (!t.isText()) {
            throw Error.error(ErrorCode.X_S0522);
        }

        // SET TABLE <table> SOURCE ON
        if (token.tokenType == Tokens.ON) {
            read();

            String sql = getLastPart();

            args[1] = Boolean.TRUE;

            return new StatementCommand(
                StatementTypes.SET_TABLE_SOURCE,
                args,
                null,
                new HsqlName[]{ t.getName() });
        } else if (token.tokenType == Tokens.OFF) {
            read();

            String sql = getLastPart();

            args[1] = Boolean.FALSE;

            return new StatementCommand(
                StatementTypes.SET_TABLE_SOURCE,
                args,
                null,
                new HsqlName[]{ t.getName() });
        } else if (token.tokenType == Tokens.HEADER) {
            read();

            isSourceHeader = true;
        }

        if (token.tokenType == Tokens.X_DELIMITED_IDENTIFIER) {
            source = token.tokenString;

            read();
        } else {
            source = readQuotedString();
        }

        if (!isSourceHeader && token.tokenType == Tokens.DESC) {
            isDesc = true;

            read();
        }

        String sql = getLastPart();

        args[2] = source;
        args[3] = Boolean.valueOf(isDesc);
        args[4] = Boolean.valueOf(isSourceHeader);

        int type = isSourceHeader
                   ? StatementTypes.SET_TABLE_SOURCE_HEADER
                   : StatementTypes.SET_TABLE_SOURCE;

        return new StatementCommand(
            type,
            args,
            null,
            new HsqlName[]{ t.getName() });
    }
}
