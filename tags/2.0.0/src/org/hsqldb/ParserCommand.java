/* Copyright (c) 2001-2010, The HSQL Development Group
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
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.HsqlList;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultProperties;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Charset;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;

/**
 * Parser for session and management statements
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
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

    HsqlArrayList compileStatements(String sql, Result cmd) {

        HsqlArrayList list = new HsqlArrayList();
        Statement     cs   = null;

        reset(sql);

        while (true) {
            if (token.tokenType == Tokens.X_ENDPARSE) {
                break;
            }

            cs = compilePart(cmd.getExecuteProperties());

            if (!cs.isExplain
                    && cs.getParametersMetaData().getColumnCount() > 0) {
                throw Error.error(ErrorCode.X_42575);
            }

            list.add(cs);
        }

        int returnType = cmd.getStatementType();

        if (returnType != StatementTypes.RETURN_ANY) {
            int group = cs.getGroup();

            if (group == StatementTypes.X_SQL_DATA) {
                if (returnType == StatementTypes.RETURN_COUNT) {
                    throw Error.error(ErrorCode.X_07503);
                }
            } else if (returnType == StatementTypes.RETURN_RESULT) {
                throw Error.error(ErrorCode.X_07504);
            }
        }

        return list;
    }

    private Statement compilePart(int props) {

        Statement cs;

        compileContext.reset();
        setParsePosition(getPosition());

        if (token.tokenType == Tokens.X_STARTPARSE) {
            read();
        }

        switch (token.tokenType) {

            // DQL
            case Tokens.WITH :
            case Tokens.OPENBRACKET :
            case Tokens.SELECT :
            case Tokens.TABLE : {
                cs = compileCursorSpecification(props);

                break;
            }
            case Tokens.VALUES : {
                cs = compileShortCursorSpecification(props);

                break;
            }

            // DML
            case Tokens.INSERT : {
                cs = compileInsertStatement(RangeVariable.emptyArray);

                break;
            }
            case Tokens.UPDATE : {
                cs = compileUpdateStatement(RangeVariable.emptyArray);

                break;
            }
            case Tokens.MERGE : {
                cs = compileMergeStatement(RangeVariable.emptyArray);

                break;
            }
            case Tokens.DELETE : {
                cs = compileDeleteStatement(RangeVariable.emptyArray);

                break;
            }
            case Tokens.TRUNCATE : {
                cs = compileDeleteStatement(RangeVariable.emptyArray);

                break;
            }

            // PROCEDURE
            case Tokens.CALL : {
                cs = compileCallStatement(
                    session.sessionContext.sessionVariablesRange, false);

                break;
            }

            // SQL SESSION
            case Tokens.SET :
                cs = compileSet();
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
                cs = compileScript();
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

            case Tokens.EXPLAIN :
                cs = compileExplainPlan();
                break;

            case Tokens.DECLARE :
                cs = compileDeclare();
                break;

            default :
                throw unexpectedToken();
        }

        if (cs.type != StatementTypes.SET_SESSION_AUTHORIZATION) {
            cs.setSQL(getLastPart());
        }

        if (token.tokenType == Tokens.SEMICOLON) {
            read();
        } else if (token.tokenType == Tokens.X_ENDPARSE) {}

        return cs;
    }

    private Statement compileDeclare() {

        Statement    cs;
        ColumnSchema variables[];

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

        cs = compileDeclareCursor();

        return cs;
    }

    private Statement compileScript() {

        String name = null;

        read();

        if (token.tokenType == Tokens.X_VALUE) {
            name = readQuotedString();
        }

        Object[] args = new Object[]{ name };
        Statement cs = new StatementCommand(StatementTypes.DATABASE_SCRIPT,
                                            args);
        HsqlName[] names =
            database.schemaManager.getCatalogAndBaseTableNames();

        cs.readTableNames = names;

        return cs;
    }

    private Statement compileConnect() {

        String userName;
        String password = null;

        read();
        readThis(Tokens.USER);
        checkIsSimpleName();

        userName = token.tokenString;

        read();
        readThis(Tokens.PASSWORD);

        password = readPassword();

        Expression[] args = new Expression[] {
            new ExpressionValue(userName, Type.SQL_VARCHAR),
            new ExpressionValue(password, Type.SQL_VARCHAR)
        };
        Statement cs =
            new StatementSession(StatementTypes.SET_SESSION_AUTHORIZATION,
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
                    StatementTypes.SET_DATABASE_DEFAULT_INITIAL_SCHEMA, args);
            }
            case Tokens.RESULT : {
                read();
                readThis(Tokens.MEMORY);
                readThis(Tokens.ROWS);

                Integer  size = readIntegerObject();
                Object[] args = new Object[]{ size };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_RESULT_MEMORY_ROWS, args);
            }
            case Tokens.TABLE : {
                read();
                readThis(Tokens.TYPE);

                int type = TableBase.MEMORY_TABLE;

                switch (token.tokenType) {

                    case Tokens.MEMORY :
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
                    StatementTypes.SET_DATABASE_DEFAULT_TABLE_TYPE, args);
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

                read();

                Object[] args = new Object[]{ ValuePool.getInt(level) };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_DEFAULT_ISOLATION_LEVEL, args);
            }
            default :
                throw unexpectedToken();
        }
    }

    private StatementCommand compileSetProperty() {

        read();

        String                 property;
        Object                 value;
        HsqlDatabaseProperties props = database.getProperties();

        checkIsSimpleName();
        checkIsDelimitedIdentifier();

        property = token.tokenString;

        boolean isboolean  = props.isBoolean(token.tokenString);
        boolean isintegral = props.isIntegral(token.tokenString);
        boolean isstring   = props.isString(token.tokenString);

        if (!(isboolean || isintegral || isstring)) {
            throw Error.error(ErrorCode.X_42555, property);
        }

        int typeCode = isboolean ? Types.SQL_BOOLEAN
                                 : isintegral ? Types.SQL_INTEGER
                                              : Types.SQL_CHAR;

        read();

        if (token.tokenType == Tokens.TRUE) {
            value = Boolean.TRUE;

            if (!isboolean) {
                throw Error.error(ErrorCode.X_42563, token.tokenString);
            }
        } else if (token.tokenType == Tokens.FALSE) {
            value = Boolean.FALSE;

            if (!isboolean) {
                throw Error.error(ErrorCode.X_42563, token.tokenString);
            }
        } else {
            checkIsValue();

            value = token.tokenValue;

            if (token.dataType.typeCode != typeCode) {
                throw Error.error(ErrorCode.X_42563, token.tokenString);
            }
        }

        read();

        Object[] args = new Object[] {
            property, value
        };

        return new StatementCommand(StatementTypes.SET_DATABASE_PROPERTY,
                                    args);
    }

    private Statement compileSet() {

        int position = super.getPosition();

        session.setScripting(false);
        read();

        switch (token.tokenType) {

            case Tokens.CATALOG : {
                read();

                Expression e = XreadValueSpecificationOrNull();

                if (e == null) {
                    HsqlName name = readSchemaName();
                    Object[] args = new Object[]{ name };

                    return new StatementSession(StatementTypes.SET_CATALOG,
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

                return new StatementSession(StatementTypes.SET_CATALOG, args);
            }
            case Tokens.SCHEMA : {
                read();

                Expression e = XreadValueSpecificationOrNull();

                if (e == null) {
                    HsqlName name = readSchemaName();
                    Object[] args = new Object[]{ name };

                    return new StatementSession(StatementTypes.SET_SCHEMA,
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

                return new StatementSession(StatementTypes.SET_SCHEMA, args);
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
                read();

                Object[] args = processTransactionCharacteristics();

                if (args[0] == null && args[1] == null) {
                    throw unexpectedToken();
                }

                return new StatementSession(StatementTypes.SET_TRANSACTION,
                                            args);
            }
            case Tokens.AUTOCOMMIT : {
                read();

                Boolean  mode = processTrueOrFalseObject();
                Object[] args = new Object[]{ mode };

                return new StatementSession(
                    StatementTypes.SET_SESSION_AUTOCOMMIT, args);
            }

            // deprecated
            case Tokens.READONLY : {
                read();

                Boolean  readonly = processTrueOrFalseObject();
                Object[] args     = new Object[]{ readonly };

                return new StatementSession(
                    StatementTypes.SET_SESSION_CHARACTERISTICS, args);
            }
            case Tokens.IGNORECASE : {
                read();

                Boolean  mode = processTrueOrFalseObject();
                Object[] args = new Object[]{ mode };

                return new StatementSession(
                    StatementTypes.SET_SESSION_SQL_IGNORECASE, args);
            }
            case Tokens.MAXROWS : {
                read();

                Integer  size = readIntegerObject();
                Object[] args = new Object[]{ size };

                return new StatementSession(
                    StatementTypes.SET_SESSION_RESULT_MAX_ROWS, args);
            }

            // for backward compatibility
            case Tokens.DEFAULT : {
                read();
                readThis(Tokens.TABLE);
                readThis(Tokens.TYPE);

                int type = TableBase.MEMORY_TABLE;

                switch (token.tokenType) {

                    case Tokens.MEMORY :
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
                    StatementTypes.SET_DATABASE_DEFAULT_TABLE_TYPE, args);
            }
            case Tokens.TABLE : {
                read();

                Table    t    = readTableName();
                Object[] args = new Object[] {
                    t.getName(), null
                };

                switch (token.tokenType) {

                    default : {
                        throw unexpectedToken();
                    }
                    case Tokens.SOURCE :
                        read();

                        return compileTextTableSource(t);

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
                            StatementTypes.SET_TABLE_READONLY, args, null,
                            t.getName());
                    }

                    // deprecated
                    case Tokens.READONLY : {
                        read();

                        Boolean readonly = processTrueOrFalseObject();

                        args[1] = readonly;

                        return new StatementCommand(
                            StatementTypes.SET_TABLE_READONLY, args, null,
                            t.getName());
                    }
                    case Tokens.INDEX : {
                        String value;

                        read();
                        checkIsValue();

                        value = token.tokenString;

                        read();

                        args[1] = value;

                        return new StatementCommand(
                            StatementTypes.SET_TABLE_INDEX, args);
                    }
                    case Tokens.TYPE : {
                        read();

                        int newType;

                        if (token.tokenType == Tokens.CACHED) {
                            newType = TableBase.CACHED_TABLE;
                        } else if (token.tokenType == Tokens.MEMORY) {
                            newType = TableBase.MEMORY_TABLE;
                        } else {
                            throw super.unexpectedToken();
                        }

                        read();

                        args[1] = new Integer(newType);

                        return new StatementCommand(
                            StatementTypes.SET_TABLE_TYPE, args, null,
                            t.getName());
                    }
                }
            }
/*
            case Tokens.CHECKPOINT : {
                read();
                readThis(Tokens.DEFRAG);

                int      size = readInteger();
                Object[] args = new Object[]{ new Integer(size) };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_FILES_DEFRAG, args, null,
                    null);
            }
*/
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
                    delay = this.readInteger();

                    if (delay < 0) {
                        delay = 0;
                    }

                    if (token.tokenType == Tokens.MILLIS) {
                        read();
                    } else {
                        delay *= 1000;
                    }
                }

                Object[] args = new Object[]{ new Integer(delay) };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_FILES_WRITE_DELAY, args, null,
                    null);
            }
            case Tokens.PASSWORD : {
                String password;

                read();

                password = readPassword();

                Object[] args = new Object[] {
                    null, password
                };

                return new StatementCommand(StatementTypes.SET_USER_PASSWORD,
                                            args);
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

                Object[] args = new Object[] {
                    null, schema
                };

                return new StatementCommand(
                    StatementTypes.SET_USER_INITIAL_SCHEMA, args);
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
                rewind(position);

                return compileSetStatement(
                    session.sessionContext.sessionVariablesRange);
            }
        }
    }

    StatementCommand compileSetDatabaseProperty() {

        read();

        String name;

        checkDatabaseUpdateAuthorisation();

        switch (token.tokenType) {

            case Tokens.COLLATION : {
                read();
                checkIsSimpleName();

                name = token.tokenString;

                read();

                Object[] args = new Object[]{ name };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_SQL_COLLATION, args, null,
                    null);
            }
            case Tokens.DEFAULT : {
                return compileSetDefault();
            }
            case Tokens.EVENT : {
                read();
                readThis(Tokens.LOG);
                readThis(Tokens.LEVEL);

                Integer  value = readIntegerObject();
                Object[] args  = new Object[]{ value };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_FILES_EVENT_LOG, args, null,
                    null);
            }
            case Tokens.GC : {
                read();

                Integer  size = readIntegerObject();
                Object[] args = new Object[]{ size };

                return new StatementCommand(StatementTypes.SET_DATABASE_GC,
                                            args, null, null);
            }
            case Tokens.REFERENTIAL : {
                read();
                readThis(Tokens.INTEGRITY);

                boolean  mode = processTrueOrFalse();
                Object[] args = new Object[]{ Boolean.valueOf(mode) };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_SQL_REFERENTIAL_INTEGRITY,
                    args, null, null);
            }
            case Tokens.SQL : {
                read();

                int     type = 0;
                Boolean flag = null;

                switch (token.tokenType) {

                    case Tokens.REFERENCES :
                        read();

                        type = StatementTypes.SET_DATABASE_SQL_REFERENCES;
                        flag = processTrueOrFalseObject();
                        break;


                    case Tokens.SIZE :
                        read();

                        type = StatementTypes.SET_DATABASE_SQL_STRICT_SIZE;
                        flag = processTrueOrFalseObject();
                        break;

                    case Tokens.NAMES :
                        read();

                        type = StatementTypes.SET_DATABASE_SQL_STRICT_NAMES;
                        flag = processTrueOrFalseObject();
                        break;

                    default :
                        unexpectedToken();
                }

                Object[] args = new Object[]{ flag };

                return new StatementCommand(type, args, null, null);
            }
            case Tokens.TEXT : {
                read();
                readThis(Tokens.TABLE);
                readThis(Tokens.DEFAULTS);

                String   source = readQuotedString();
                Object[] args   = new Object[]{ source };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_TEXT_SOURCE, args, null, null);
            }
            case Tokens.TRANSACTION : {
                read();
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
                }

                Object[] args = new Object[]{ ValuePool.getInt(mode) };
                StatementCommand cs = new StatementCommand(
                    StatementTypes.SET_DATABASE_TRANSACTION_CONTROL, args,
                    null, null);
                HsqlName[] names =
                    database.schemaManager.getCatalogAndBaseTableNames();

                cs.writeTableNames = names;

                return cs;
            }
            case Tokens.UNIQUE : {
                read();
                readThis(Tokens.NAME);
                isUndelimitedSimpleName();

                name = token.tokenString;

                read();

                /** @todo - only digits, letters and underscore */
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
                    StatementTypes.SET_DATABASE_UNIQUE_NAME, args, null, null);
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

    StatementCommand compileSetFilesProperty() {

        read();

        String  name;
        int     type  = 0;
        Boolean flag  = null;
        Integer value = null;

        checkDatabaseUpdateAuthorisation();

        switch (token.tokenType) {

            case Tokens.LOCK : {
                read();

                flag = processTrueOrFalseObject();
                type = StatementTypes.SET_DATABASE_FILES_LOCK;

                break;
            }
            case Tokens.CACHE : {
                read();

                if (readIfThis(Tokens.SIZE)) {
                    value = readIntegerObject();
                    type  = StatementTypes.SET_DATABASE_FILES_CACHE_SIZE;
                } else if (readIfThis(Tokens.ROWS)) {
                    value = readIntegerObject();
                    type  = StatementTypes.SET_DATABASE_FILES_CACHE_ROWS;
                }

                break;
            }
            case Tokens.SCALE : {
                read();

                value = readIntegerObject();
                type  = StatementTypes.SET_DATABASE_FILES_SCALE;

                break;
            }
            case Tokens.LOB : {
                read();
                readThis(Tokens.SCALE);

                value = readIntegerObject();
                type  = StatementTypes.SET_DATABASE_FILES_LOBS_SCALE;

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

                type = StatementTypes.SET_DATABASE_FILES_NIO;
                flag = processTrueOrFalseObject();

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

                if (token.tokenType == Tokens.SIZE) {
                    readThis(Tokens.SIZE);

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
                value = readIntegerObject();

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
                    delay = this.readInteger();

                    if (delay < 0) {
                        delay = 0;
                    }

                    if (token.tokenType == Tokens.MILLIS) {
                        read();
                    } else {
                        delay *= 1000;
                    }
                }

                value = new Integer(delay);

                break;
            }
            default :
                throw unexpectedToken();
        }

        Object[] args = new Object[]{ flag == null ? (Object) value
                                                   : (Object) flag };

        return new StatementCommand(type, args, database.getCatalogName(),
                                    null);
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

                    switch (token.tokenType) {

                        case Tokens.SERIALIZABLE :
                            read();

                            level = SessionInterface.TX_SERIALIZABLE;
                            break;

                        case Tokens.READ :
                            read();

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

                    args[1] = new Integer(level);

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

        if (!readonly && level == 1) {
            throw unexpectedToken(Tokens.T_WRITE);
        }

        return args;
    }

    /**
     * Retrieves boolean value corresponding to the next token.
     *
     * @return   true if next token is "TRUE"; false if next token is "FALSE"
     * @throws  HsqlException if the next token is neither "TRUE" or "FALSE"
     */
    private boolean processTrueOrFalse() {

        if (token.namePrefix != null) {
            throw unexpectedToken();
        }

        if (token.tokenType == Tokens.TRUE) {
            read();

            return true;
        } else if (token.tokenType == Tokens.FALSE) {
            read();

            return false;
        } else {
            throw unexpectedToken();
        }
    }

    private Boolean processTrueOrFalseObject() {

        if (token.namePrefix != null) {
            throw unexpectedToken();
        }

        if (token.tokenType == Tokens.TRUE) {
            read();

            return Boolean.TRUE;
        } else if (token.tokenType == Tokens.FALSE) {
            read();

            return Boolean.FALSE;
        } else {
            throw unexpectedToken();
        }
    }

    /**
     * Responsible for  handling the execution of COMMIT [WORK]
     *
     * @throws  HsqlException
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

        String    sql  = getLastPart();
        Object[]  args = new Object[]{ Boolean.valueOf(chain) };
        Statement cs = new StatementSession(StatementTypes.COMMIT_WORK, args);

        return cs;
    }

    private Statement compileStartTransaction() {

        read();
        readThis(Tokens.TRANSACTION);

        Object[] args = processTransactionCharacteristics();
        Statement cs = new StatementSession(StatementTypes.START_TRANSACTION,
                                            args);

        return cs;
    }

    private Statement compileLock() {

        read();
        readThis(Tokens.TABLE);

        OrderedHashSet readSet  = new OrderedHashSet();
        OrderedHashSet writeSet = new OrderedHashSet();

        outerloop:
        while (true) {
            Table table = readTableName();

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

            break outerloop;
        }

        HsqlName[] writeTableNames = new HsqlName[writeSet.size()];

        writeSet.toArray(writeTableNames);
        readSet.removeAll(writeTableNames);

        HsqlName[] readTableNames = new HsqlName[readSet.size()];

        readSet.toArray(readTableNames);

        Statement cs =
            new StatementSession(StatementTypes.TRANSACTION_LOCK_TABLE,
                                 readTableNames, writeTableNames);

        return cs;
    }

    private Statement compileRollback() {

        boolean chain     = false;
        String  savepoint = null;

        read();

        if (token.tokenType == Tokens.TO) {
            read();
            readThis(Tokens.SAVEPOINT);
            checkIsSimpleName();

            savepoint = token.tokenString;

            read();

            String   sql  = getLastPart();
            Object[] args = new Object[]{ savepoint };
            Statement cs =
                new StatementSession(StatementTypes.ROLLBACK_SAVEPOINT, args);

            return cs;
        } else {
            if (token.tokenType == Tokens.WORK) {
                read();
            }

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

        String   sql  = getLastPart();
        Object[] args = new Object[]{ Boolean.valueOf(chain) };
        Statement cs = new StatementSession(StatementTypes.ROLLBACK_WORK,
                                            args);

        return cs;
    }

    private Statement compileSavepoint() {

        String name;

        read();
        checkIsSimpleName();

        name = token.tokenString;

        read();

        String   sql  = getLastPart();
        Object[] args = new Object[]{ name };

        return new StatementSession(StatementTypes.SAVEPOINT, args);
    }

    private Statement compileReleaseSavepoint() {

        read();
        readThis(Tokens.SAVEPOINT);

        String name = token.tokenString;

        read();

        String   sql  = getLastPart();
        Object[] args = new Object[]{ name };

        return new StatementSession(StatementTypes.RELEASE_SAVEPOINT, args);
    }

    private Statement compileSessionSettings() {

        switch (token.tokenType) {

            case Tokens.CHARACTERISTICS : {
                read();
                readThis(Tokens.AS);
                readThis(Tokens.TRANSACTION);

                Object[] args = processTransactionCharacteristics();

                return new StatementSession(
                    StatementTypes.SET_SESSION_CHARACTERISTICS, args);
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

                Expression[] args = new Expression[] {
                    e, null
                };

                return new StatementSession(
                    StatementTypes.SET_SESSION_AUTHORIZATION, args);
            }
            case Tokens.RESULT : {
                read();
                readThis(Tokens.MEMORY);
                readThis(Tokens.ROWS);

                Integer  size = readIntegerObject();
                Object[] args = new Object[]{ size };

                return new StatementCommand(
                    StatementTypes.SET_SESSION_RESULT_MEMORY_ROWS, args);
            }
            default :
                throw unexpectedToken();
        }
    }

    private Statement compileSetRole() {

        Expression e;

        if (token.tokenType == Tokens.NONE) {
            read();

            e = new ExpressionValue(null, Type.SQL_VARCHAR);
        } else {
            e = XreadValueSpecificationOrNull();

            if (e == null) {
                throw Error.error(ErrorCode.X_2A000);
            }

            if (!e.getDataType().isCharacterType()) {
                throw Error.error(ErrorCode.X_0P000);
            }

            if (e.getType() != OpTypes.VALUE
                    && (e.getType() != OpTypes.SQL_FUNCTION
                        || !((FunctionSQL) e).isValueFunction())) {
                throw Error.error(ErrorCode.X_0P000);
            }
        }

        String sql = getLastPart();

        return new StatementSession(StatementTypes.SET_ROLE,
                                    new Expression[]{ e });
    }

    private Statement compileSetTimeZone() {

        Expression e;

        readThis(Tokens.ZONE);

        if (token.tokenType == Tokens.LOCAL) {
            read();

            e = new ExpressionValue(null, Type.SQL_INTERVAL_HOUR_TO_MINUTE);
        } else {
            e = XreadIntervalValueExpression();

            HsqlList unresolved =
                e.resolveColumnReferences(RangeVariable.emptyArray, null);

            ExpressionColumn.checkColumnsResolved(unresolved);
            e.resolveTypes(session, null);

            if (e.dataType == null) {
                throw Error.error(ErrorCode.X_42563);
            }

            if (e.dataType.typeCode != Types.SQL_INTERVAL_HOUR_TO_MINUTE) {
                throw Error.error(ErrorCode.X_42563);
            }
        }

        String sql = getLastPart();

        return new StatementSession(StatementTypes.SET_TIME_ZONE,
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

            // only semicolon is accepted here
        }

        if (token.tokenType == Tokens.SEMICOLON) {
            read();
        }

        if (token.tokenType != Tokens.X_ENDPARSE) {
            throw unexpectedToken();
        }

        String   sql  = getLastPart();
        Object[] args = new Object[]{ new Integer(closemode) };
        Statement cs = new StatementCommand(StatementTypes.DATABASE_SHUTDOWN,
                                            args, null, null);

        return cs;
    }

    private Statement compileBackup() {

        read();
        readThis(Tokens.DATABASE);
        readThis(Tokens.TO);

        String  path         = readQuotedString();
        Boolean blockingMode = null;    // Default to non-blocking
        Boolean scriptMode   = null;    // Default to non-script
        Boolean compression  = null;    // Defaults to compressed

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
                        blockingMode = Boolean.FALSE;

                        read();
                    } else {
                        throw unexpectedToken();
                    }
                    break;

                default :
                    break outerLoop;
            }
        }

        /**
         * @todo: This block is TEMPORARY.  Will be removed when we implement
         * Non-Blocking and SCRIPT mode.
         */
        if (scriptMode != null) {
            throw unsupportedFeature("SCRIPT");
        }

        scriptMode = Boolean.FALSE;

        if (blockingMode == null) {
            throw unexpectedTokenRequire("BLOCKING");
        }

        if (compression == null) {
            compression = Boolean.TRUE;
        }

        Object[] args = new Object[] {
            path, blockingMode, scriptMode, compression,
        };
        Statement cs = new StatementCommand(StatementTypes.DATABASE_BACKUP,
                                            args);
        HsqlName[] names =
            database.schemaManager.getCatalogAndBaseTableNames();

        cs.writeTableNames = names;

        return cs;
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

        String sql = getLastPart();

        return getCheckpointStatement(database, defrag);
    }

    public static Statement getCheckpointStatement(Database database,
            boolean defrag) {

        Object[] args = new Object[]{ Boolean.valueOf(defrag) };
        Statement cs = new StatementCommand(StatementTypes.DATABASE_CHECKPOINT,
                                            args);
        HsqlName[] names =
            database.schemaManager.getCatalogAndBaseTableNames();

        cs.writeTableNames = names;

        return cs;
    }

    private Statement compileDisconnect() {

        read();

        String sql = Tokens.T_DISCONNECT;
        Statement cs = new StatementSession(StatementTypes.DISCONNECT,
                                            (Object[]) null);

        return cs;
    }

    private Statement compileExplainPlan() {

        Statement cs;

        read();
        readThis(Tokens.PLAN);
        readThis(Tokens.FOR);

        cs = compilePart(ResultProperties.defaultPropsValue);

        cs.setDescribe();

        return cs;
    }

    private Statement compileTextTableSource(Table t) {

        boolean  isSourceHeader = false;
        boolean  isDesc         = false;
        String   source;
        Object[] args = new Object[5];

        args[0] = t.getName();

        if (!t.isText()) {
            Exception e = Error.error(ErrorCode.X_S0522);
        }

        // SET TABLE <table> SOURCE ON
        if (token.tokenType == Tokens.ON) {
            read();

            String sql = getLastPart();

            args[1] = Boolean.TRUE;

            return new StatementCommand(StatementTypes.SET_TABLE_SOURCE, args,
                                        null, t.getName());
        } else if (token.tokenType == Tokens.OFF) {
            read();

            String sql = getLastPart();

            args[1] = Boolean.FALSE;

            return new StatementCommand(StatementTypes.SET_TABLE_SOURCE, args,
                                        null, t.getName());
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

        int type = isSourceHeader ? StatementTypes.SET_TABLE_SOURCE_HEADER
                                  : StatementTypes.SET_TABLE_SOURCE;

        return new StatementCommand(type, args, null, t.getName());
    }
}
