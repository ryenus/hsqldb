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
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.HsqlList;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.scriptio.ScriptWriterBase;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Type;

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

    Statement compileStatement() throws HsqlException {

        Statement cs = compilePart();

        if (token.tokenType == Tokens.X_ENDPARSE) {
            if (cs.getSchemalName() == null) {
                cs.setSchemaHsqlName(session.getCurrentSchemaHsqlName());
            }

            return cs;
        }

        throw unexpectedToken();
    }

    HsqlArrayList compileStatements(String sql,
                                    int returnType) throws HsqlException {

        HsqlArrayList list = new HsqlArrayList();
        Statement     cs   = null;

        reset(sql);

        while (true) {
            if (token.tokenType == Tokens.X_ENDPARSE) {
                break;
            }

            compileContext.reset();

            cs = compilePart();

            if (cs == null) {
                list.add(cs);
            } else {
                list.add(cs);
            }
        }

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

    private Statement compilePart() throws HsqlException {

        Statement cs = null;

        setParsePosition(getPosition());

        if (token.tokenType == Tokens.X_STARTPARSE) {
            read();
        }

        switch (token.tokenType) {

            // DQL
            case Tokens.WITH :
            case Tokens.OPENBRACKET :
            case Tokens.VALUES :
            case Tokens.TABLE :
            case Tokens.SELECT : {
                cs = compileCursorSpecification();

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
                cs = readCallStatement(
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
                cs = compileSessionVariableDeclaration();
                break;

            default :
                throw unexpectedToken();
        }

        cs.setSQL(getLastPart());

        if (token.tokenType == Tokens.SEMICOLON) {
            read();
        } else if (token.tokenType == Tokens.X_ENDPARSE) {}

        return cs;
    }

    private Statement compileSessionVariableDeclaration()
    throws HsqlException {

        ColumnSchema variable = readLocalVariableDeclarationOrNull();
        Object[]     args     = new Object[]{ variable };
        Statement cs = new StatementCommand(StatementTypes.DECLARE_VARIABLE,
                                            args);

        return cs;
    }

    private Statement compileScript() throws HsqlException {

        String name = null;

        read();

        if (token.tokenType == Tokens.X_VALUE) {
            if (token.dataType.typeCode != Types.SQL_CHAR) {
                throw Error.error(ErrorCode.X_42581);
            }

            name = token.tokenString;

            read();
        }

        Object[]  args = new Object[]{ name };
        Statement cs   = new StatementCommand(StatementTypes.SCRIPT, args);

        return cs;
    }

    private Statement compileConnect() throws HsqlException {

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
            new StatementCommand(StatementTypes.SET_SESSION_AUTHORIZATION,
                                 args);

        return cs;
    }

    private Statement compileSet() throws HsqlException {

        int position = super.getPosition();

        session.setScripting(false);
        read();

        switch (token.tokenType) {

            case Tokens.PROPERTY : {
                read();

                String                 property;
                Object                 value;
                HsqlDatabaseProperties props;

                checkIsSimpleName();
                checkIsDelimitedIdentifier();

                property = token.tokenString;
                props    = database.getProperties();

                boolean isboolean  = props.isBoolean(token.tokenString);
                boolean isintegral = props.isIntegral(token.tokenString);
                boolean isstring   = props.isString(token.tokenString);

                if (!(isboolean || isintegral || isstring)) {
                    throw Error.error(ErrorCode.X_42511);
                }

                int typeCode = isboolean ? Types.SQL_BOOLEAN
                                         : isintegral ? Types.SQL_INTEGER
                                                      : Types.SQL_CHAR;

                read();

                if (token.tokenType == Tokens.TRUE) {
                    value = Boolean.TRUE;
                } else if (token.tokenType == Tokens.FALSE) {
                    value = Boolean.FALSE;
                } else {
                    checkIsValue();

                    value = token.tokenValue;

                    if (token.dataType.typeCode != typeCode) {
                        throw Error.error(ErrorCode.X_42565,
                                          token.tokenString);
                    }
                }

                read();

                Object[] args = new Object[] {
                    property, value
                };

                return new StatementCommand(StatementTypes.SET_PROPERTY, args);
            }
            case Tokens.SCHEMA : {
                read();

                Expression e = XreadValueSpecificationOrNull();

                if (e == null) {
                    HsqlName name = readSchemaName();
                    Object[] args = new Object[]{ name };

                    return new StatementCommand(StatementTypes.SET_SCHEMA,
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

                return new StatementCommand(StatementTypes.SET_SCHEMA, args);
            }
            case Tokens.PASSWORD : {
                String password;

                read();

                password = readPassword();

                Object[] args = new Object[] {
                    null, password
                };

                return new StatementCommand(StatementTypes.SET_PASSWORD, args);
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

                return new StatementCommand(StatementTypes.SET_TRANSACTION,
                                            args);
            }

            // deprecated
            case Tokens.READONLY : {
                read();

                boolean  readonly = processTrueOrFalse();
                Object[] args     = new Object[]{ Boolean.valueOf(readonly) };

                return new StatementCommand(
                    StatementTypes.SET_SESSION_CHARACTERISTICS, args);
            }
            case Tokens.LOGSIZE : {
                read();
                checkDatabaseUpdateAuthorisation();

                int      size = readInteger();
                Object[] args = new Object[]{ new Integer(size) };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_LOG_SIZE, args);
            }
            case Tokens.SCRIPTFORMAT : {
                read();
                checkDatabaseUpdateAuthorisation();

                switch (token.tokenType) {

                    case Tokens.TEXT :
                    case Tokens.BINARY :
                    case Tokens.COMPRESSED :
                        break;

                    default :
                        throw unexpectedToken();
                }

                int mode = ArrayUtil.find(ScriptWriterBase.LIST_SCRIPT_FORMATS,
                                          token.tokenString);

                if (mode == 0 || mode == 1 || mode == 3) {}
                else {
                    throw unexpectedToken();
                }

                read();

                Object[] args = new Object[]{ new Integer(mode) };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_SCRIPT_FORMAT, args);
            }
            case Tokens.IGNORECASE : {
                read();

                boolean  mode = processTrueOrFalse();
                Object[] args = new Object[]{ Boolean.valueOf(mode) };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_IGNORECASE, args);
            }
            case Tokens.MAXROWS : {
                read();

                int      size = readInteger();
                Object[] args = new Object[]{ new Integer(size) };

                return new StatementCommand(
                    StatementTypes.SET_SESSION_RESULT_MAX_ROWS, args);
            }
            case Tokens.AUTOCOMMIT : {
                read();

                boolean  mode = processTrueOrFalse();
                Object[] args = new Object[]{ Boolean.valueOf(mode) };

                return new StatementCommand(StatementTypes.SET_AUTOCOMMIT,
                                            args);
            }
            case Tokens.DEFAULT : {
                read();

                switch (token.tokenType) {

                    case Tokens.INITIAL : {
                        read();
                        readThis(Tokens.SCHEMA);

                        HsqlName schema =
                            database.schemaManager.getSchemaHsqlName(
                                token.tokenString);

                        read();

                        Object[] args = new Object[]{ schema };

                        return new StatementCommand(
                            StatementTypes.SET_DEFAULT_INITIAL_SCHEMA, args);
                    }
                    case Tokens.RESULT : {
                        read();
                        readThis(Tokens.MEMORY);
                        readThis(Tokens.SIZE);

                        int      size = readInteger();
                        Object[] args = new Object[]{ new Integer(size) };

                        return new StatementCommand(
                            StatementTypes.SET_DATABASE_RESULT_MEMORY_ROWS,
                            args);
                    }
                    case Tokens.TABLE : {
                        read();
                        readThis(Tokens.TYPE);

                        String type = Tokens.T_MEMORY;

                        switch (token.tokenType) {

                            case Tokens.MEMORY :
                                break;

                            case Tokens.CACHED :
                                type = Tokens.T_CACHED;
                                break;

                            default :
                                throw unexpectedToken();
                        }

                        read();

                        Object[] args = new Object[] {
                            HsqlDatabaseProperties.hsqldb_default_table_type,
                            type
                        };

                        return new StatementCommand(
                            StatementTypes.SET_PROPERTY, args);
                    }
                    default :
                        throw unexpectedToken();
                }
            }
            case Tokens.RESULT : {
                read();
                readThis(Tokens.MEMORY);
                readThis(Tokens.SIZE);

                int      size = readInteger();
                Object[] args = new Object[]{ new Integer(size) };

                return new StatementCommand(
                    StatementTypes.SET_SESSION_RESULT_MEMORY_ROWS, args);
            }
            case Tokens.TABLE : {
                read();

                Table    t    = readTableName();
                Object[] args = new Object[] {
                    t.tableName, null
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

                        if (token.tokenType == Tokens.WRITE) {}
                        else {
                            readThis(Tokens.ONLY);

                            readonly = true;
                        }

                        args[1] = Boolean.valueOf(readonly);

                        return new StatementCommand(
                            StatementTypes.SET_TABLE_READONLY, args);
                    }

                    // deprecated
                    case Tokens.READONLY : {
                        read();

                        boolean readonly = processTrueOrFalse();

                        args[1] = Boolean.valueOf(readonly);

                        return new StatementCommand(
                            StatementTypes.SET_TABLE_READONLY, args);
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
                            throw Error.error(ErrorCode.X_42581);
                        }

                        read();

                        args[1] = new Integer(newType);

                        return new StatementCommand(
                            StatementTypes.SET_TABLE_TYPE, args);
                    }
                }
            }
            case Tokens.REFERENTIAL_INTEGRITY : {
                read();

                boolean  mode = processTrueOrFalse();
                Object[] args = new Object[]{ Boolean.valueOf(mode) };

                return new StatementCommand(
                    StatementTypes.SET_DATABASE_REFERENTIAL_INTEGRITY, args);
            }
            case Tokens.CHECKPOINT : {
                read();
                readThis(Tokens.DEFRAG);

                int      size = readInteger();
                Object[] args = new Object[]{ new Integer(size) };

                return new StatementCommand(StatementTypes.SET_DATABASE_DEFRAG,
                                            args);
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
                    StatementTypes.SET_DATABASE_WRITE_DELAY, args);
            }
            case Tokens.DATABASE : {
                read();

                String name;

                switch (token.tokenType) {

                    case Tokens.COLLATION : {
                        read();
                        checkIsSimpleName();

                        name = token.tokenString;

                        read();

                        Object[] args = new Object[]{ name };

                        return new StatementCommand(
                            StatementTypes.SET_DATABASE_COLLATION, args);
                    }
                    case Tokens.EVENT : {
                        read();
                        readThis(Tokens.LOG);
                        readThis(Tokens.LEVEL);

                        int      level = readInteger();
                        Object[] args = new Object[]{ Integer.valueOf(level) };

                        return new StatementCommand(
                            StatementTypes.SET_DATABASE_EVENT_LOG, args);
                    }
                    case Tokens.TRANSACTION : {
                        read();
                        readThis(Tokens.CONTROL);

                        boolean mvcc = false;

                        if (token.tokenType == Tokens.MVCC) {
                            read();

                            mvcc = true;
                        } else {
                            readThis(Tokens.LOCKS);
                        }

                        Object[] args = new Object[]{ Boolean.valueOf(mvcc) };

                        return new StatementCommand(
                            StatementTypes.SET_DATABASE_TRANSACTION_CONTROL,
                            args);
                    }
                    default : {
                        throw unexpectedToken();
                    }
                }
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

                return new StatementCommand(StatementTypes.SET_INITIAL_SCHEMA,
                                            args);
            }
            default : {
                rewind(position);

                return compileSetStatement(
                    session.sessionContext.sessionVariablesRange);
            }
        }
    }

    Object[] processTransactionCharacteristics() throws HsqlException {

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
    private boolean processTrueOrFalse() throws HsqlException {

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

    /**
     * Responsible for  handling the execution of COMMIT [WORK]
     *
     * @throws  HsqlException
     */
    private Statement compileCommit() throws HsqlException {

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
        Statement cs = new StatementCommand(StatementTypes.COMMIT_WORK, args);

        return cs;
    }

    private Statement compileStartTransaction() throws HsqlException {

        read();
        readThis(Tokens.TRANSACTION);

        Object[] args = processTransactionCharacteristics();
        Statement cs = new StatementCommand(StatementTypes.START_TRANSACTION,
                                            args);

        return cs;
    }

    private Statement compileLock() throws HsqlException {

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

                break;
            }

            break outerloop;
        }

        Statement cs = new StatementCommand(StatementTypes.LOCK_TABLE,
                                            ValuePool.emptyObjectArray);

        cs.readTableNames = new HsqlName[readSet.size()];

        readSet.toArray(cs.readTableNames);

        cs.writeTableNames = new HsqlName[writeSet.size()];

        readSet.toArray(cs.writeTableNames);

        return cs;
    }

    private Statement compileRollback() throws HsqlException {

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
                new StatementCommand(StatementTypes.ROLLBACK_SAVEPOINT, args);

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
        Statement cs = new StatementCommand(StatementTypes.ROLLBACK_WORK,
                                            args);

        return cs;
    }

    private Statement compileSavepoint() throws HsqlException {

        String name;

        read();
        checkIsSimpleName();

        name = token.tokenString;

        read();

        String   sql  = getLastPart();
        Object[] args = new Object[]{ name };

        return new StatementCommand(StatementTypes.SAVEPOINT, args);
    }

    private Statement compileReleaseSavepoint() throws HsqlException {

        read();
        readThis(Tokens.SAVEPOINT);

        String name = token.tokenString;

        read();

        String   sql  = getLastPart();
        Object[] args = new Object[]{ name };

        return new StatementCommand(StatementTypes.RELEASE_SAVEPOINT, args);
    }

    private Statement compileSessionSettings() throws HsqlException {

        boolean readonly = false;

        if (token.tokenType == Tokens.CHARACTERISTICS) {
            read();
            readThis(Tokens.AS);

            Object[] args = processTransactionCharacteristics();

            return new StatementCommand(
                StatementTypes.SET_SESSION_CHARACTERISTICS, args);
        } else if (token.tokenType == Tokens.AUTHORIZATION) {
            read();

            Expression e = XreadValueSpecificationOrNull();

            if (e == null) {
                throw Error.error(ErrorCode.X_42584);
            }

            e.resolveTypes(session, null);

            if (e.isParam()) {
                e.dataType = Type.SQL_VARCHAR;
            }

            if (e.dataType == null || !e.dataType.isCharacterType()) {
                throw Error.error(ErrorCode.X_42565);
            }

            String       sql  = getLastPart();
            Expression[] args = new Expression[] {
                e, null
            };

            return new StatementCommand(
                StatementTypes.SET_SESSION_AUTHORIZATION, args);
        }

        throw unexpectedToken();
    }

    private Statement compileSetRole() throws HsqlException {

        Expression e;

        if (token.tokenType == Tokens.NONE) {
            read();

            e = new ExpressionOp(null, Type.SQL_VARCHAR);
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

        return new StatementCommand(StatementTypes.SET_ROLE,
                                    new Expression[]{ e });
    }

    private Statement compileSetTimeZone() throws HsqlException {

        Expression e;

        readThis(Tokens.ZONE);

        if (token.tokenType == Tokens.LOCAL) {
            read();

            e = new ExpressionValue(null, Type.SQL_VARCHAR);
        } else {
            e = XreadIntervalValueExpression();

            HsqlList unresolved =
                e.resolveColumnReferences(RangeVariable.emptyArray, null);

            ExpressionColumn.checkColumnsResolved(unresolved);
            e.resolveTypes(session, null);

            if (e.dataType == null) {
                throw Error.error(ErrorCode.X_42565);
            }

            if (e.dataType.typeCode != Types.SQL_INTERVAL_HOUR_TO_MINUTE) {
                throw Error.error(ErrorCode.X_42565);
            }
        }

        String sql = getLastPart();

        return new StatementCommand(StatementTypes.SET_TIME_ZONE,
                                    new Expression[]{ e });
    }

    private Statement compileShutdown() throws HsqlException {

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

        String    sql  = getLastPart();
        Object[]  args = new Object[]{ new Integer(closemode) };
        Statement cs   = new StatementCommand(StatementTypes.SHUTDOWN, args);

        return cs;
    }

    private Statement compileBackup() throws HsqlException {

        read();
        readThis(Tokens.DATABASE);
        readThis(Tokens.TO);
        checkIsValue();
        readQuotedString();

        String path = token.tokenString;

        read();

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

        // TODO:  Code above works, but replace with much more concise
        // readIfThis()s.
        // This block is TEMPORARY.  Will be removed when we implement
        // Non-Blocking and SCRIPT mode.
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

        return new StatementCommand(StatementTypes.BACKUP, new Object[] {
            path, blockingMode, scriptMode, compression
        });
    }

    private Statement compileCheckpoint() throws HsqlException {

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

        String    sql  = getLastPart();
        Object[]  args = new Object[]{ Boolean.valueOf(defrag) };
        Statement cs   = new StatementCommand(StatementTypes.CHECKPOINT, args);

        return cs;
    }

    private Statement compileDisconnect() throws HsqlException {

        read();

        String sql = Tokens.T_DISCONNECT;
        Statement cs = new StatementCommand(StatementTypes.DISCONNECT,
                                            (Object[]) null);

        return cs;
    }

    private Statement compileExplainPlan() throws HsqlException {

        Statement cs;

        read();
        readThis(Tokens.PLAN);
        readThis(Tokens.FOR);

        cs = compilePart();

        cs.setDescribe();

        return cs;
    }

    private Statement compileTextTableSource(Table t) throws HsqlException {

        boolean  isSourceHeader = false;
        boolean  isDesc         = false;
        String   source;
        Object[] args = new Object[5];

        args[0] = t.tableName;

        if (!t.isText()) {
            Exception e = Error.error(ErrorCode.X_S0522);
        }

        // SET TABLE <table> SOURCE ON
        if (token.tokenType == Tokens.ON) {
            read();

            String sql = getLastPart();

            args[1] = Boolean.TRUE;

            return new StatementCommand(StatementTypes.SET_TABLE_SOURCE, args);
        } else if (token.tokenType == Tokens.OFF) {
            read();

            String sql = getLastPart();

            args[1] = Boolean.FALSE;

            return new StatementCommand(StatementTypes.SET_TABLE_SOURCE, args);
        } else if (token.tokenType == Tokens.HEADER) {
            read();

            isSourceHeader = true;
        }

        if (token.tokenType != Tokens.X_DELIMITED_IDENTIFIER
                && (token.tokenType != Tokens.X_VALUE
                    || !token.dataType.isCharacterType())) {
            throw Error.error(ErrorCode.X_42581);
        }

        source = token.tokenString;

        read();

        if (!isSourceHeader && token.tokenType == Tokens.DESC) {
            isDesc = true;

            read();
        }

        String sql = getLastPart();

        args[2] = source;
        args[3] = Boolean.valueOf(isDesc);
        args[4] = Boolean.valueOf(isSourceHeader);

        return new StatementCommand(StatementTypes.SET_TABLE_SOURCE, args);
    }
}
