/* Copyright (c) 2001-2007, The HSQL Development Group
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

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.result.Result;
import org.hsqldb.rights.User;
import org.hsqldb.scriptio.ScriptWriterBase;
import org.hsqldb.scriptio.ScriptWriterText;
import org.hsqldb.types.Type;

public class CommandParser extends DDLParser {

    CommandParser(Session session, Database db, Tokenizer t) {
        super(session, db, t);
    }

    Result execute(String sql) {

        Result result = Result.updateZeroResult;

        JavaSystem.gc();
        reset(sql);

        while (true) {
            if (tokenType == Token.X_ENDPARSE) {
                break;
            }

            compileContext.reset();

            result = executePart();

            if (result.isError()) {
                break;
            }
        }

        return result;
    }

    Result executePart() {

        int    parsePosition = getPosition();
        Result result        = Result.updateZeroResult;

        session.setScripting(false);

        try {
            if (tokenType == Token.X_STARTPARSE) {
                read();
            }

            int brackets = 0;

            if (session.isSchemaDefintion()) {
                switch (tokenType) {

                    case Token.CREATE :
                    case Token.GRANT :
                        break;

                    default :
                        throw Trace.error(Trace.INVALID_IDENTIFIER,
                                          Trace.IN_SCHEMA_DEFINITION,
                                          new Object[]{ tokenString });
                }
            }

            switch (tokenType) {

                case Token.OPENBRACKET : {
                    brackets = readOpenBrackets();
                }
                case Token.SELECT : {
                    CompiledStatement cs = compileSelectStatement(brackets);

                    cs.sql = getLastPart(parsePosition);
                    result = session.executeCompiledStatement(cs, null);

                    break;
                }
                case Token.INSERT : {
                    CompiledStatement cs = compileInsertStatement();

                    result = session.executeCompiledStatement(cs, null);

                    break;
                }
                case Token.UPDATE : {
                    CompiledStatement cs = compileUpdateStatement();

                    result = session.executeCompiledStatement(cs, null);

                    break;
                }
                case Token.MERGE : {
                    CompiledStatement cs = compileMergeStatement();

                    result = session.executeCompiledStatement(cs, null);

                    break;
                }
                case Token.DELETE : {
                    CompiledStatement cs = compileDeleteStatement();

                    result = session.executeCompiledStatement(cs, null);

                    break;
                }
                case Token.CALL : {
                    CompiledStatement cs = compileCallStatement();

                    cs.sql = getLastPart(parsePosition);
                    result = session.executeCompiledStatement(cs, null);

                    break;
                }
                case Token.SET :
                    processSet();
                    break;

                case Token.COMMIT :
                    processCommit();
                    break;

                case Token.ROLLBACK :
                    processRollback();
                    break;

                case Token.SAVEPOINT :
                    processSavepoint();
                    break;

                case Token.RELEASE :
                    processReleaseSavepoint();
                    break;

                case Token.CREATE :
                    read();
                    processCreate();
                    database.setMetaDirty(false);
                    break;

                case Token.ALTER :
                    processAlter();
                    database.setMetaDirty(true);
                    break;

                case Token.DROP :
                    processDrop();
                    database.setMetaDirty(true);
                    break;

                case Token.GRANT :
                    processGrantOrRevoke(true);
                    database.setMetaDirty(false);
                    break;

                case Token.REVOKE :
                    processGrantOrRevoke(false);
                    database.setMetaDirty(true);
                    break;

                case Token.CONNECT :
                    processConnect();
                    database.setMetaDirty(false);
                    session.setScripting(false);
                    break;

                case Token.DISCONNECT :
                    processDisconnect();
                    session.setScripting(true);
                    break;

                case Token.SCRIPT :
                    result = processScript();
                    break;

                case Token.SHUTDOWN :
                    processShutdown();
                    break;

                case Token.CHECKPOINT :
                    processCheckpoint();
                    break;

                case Token.EXPLAIN :
                    result = processExplainPlan();
                    break;

                default :
                    throw unexpectedToken();
            }
        } catch (Throwable t) {
            try {
                if (session.isSchemaDefintion()) {
                    HsqlName schemaName = session.currentSchema;

                    database.schemaManager.dropSchema(schemaName.name, true);
                    database.logger.writeToLog(session,
                                               Token.T_DROP + ' '
                                               + Token.T_SCHEMA + ' '
                                               + schemaName.statementName
                                               + ' ' + Token.T_CASCADE);
                    session.endSchemaDefinition();
                }
            } catch (HsqlException e) {}

            String sql = getLastPartAndCurrent(parsePosition);

            result = Result.newErrorResult(t, sql);
        }

        try {
            if (session.isScripting() &&!result.isError()) {
                database.logger.writeToLog(session,
                                           getLastPart(parsePosition));
            }

            if (tokenType == Token.SEMICOLON) {
                session.endSchemaDefinition();
                read();
            }

            if (tokenType == Token.X_ENDPARSE) {
                session.endSchemaDefinition();
            }
        } catch (HsqlException e) {
            result = Result.newErrorResult(e.getMessage(), e.getSQLState(),
                                           0);
        }

        return result;
    }

    private Result processScript() throws IOException, HsqlException {

        read();

        ScriptWriterText dsw = null;

        session.checkAdmin();

        try {
            if (tokenType == Token.X_VALUE) {
                if (valueType.type != Types.SQL_CHAR) {
                    throw Trace.error(Trace.INVALID_IDENTIFIER);
                }

                dsw = new ScriptWriterText(database, tokenString, true, true,
                                           true);

                dsw.writeAll();

                return Result.updateZeroResult;
            } else {
                return DatabaseScript.getScript(database, false);
            }
        } finally {
            if (dsw != null) {
                dsw.close();
            }
        }
    }

    /**
     * Responsible for handling CONNECT
     *
     * @throws HsqlException
     */
    private void processConnect() throws HsqlException {

        String userName;
        String password;
        User   user;

        read();
        readThis(Token.USER);
        checkIsSimpleName();

        userName = tokenString;

        read();

        if (tokenType == Token.PASSWORD) {
            read();

            // legacy log statement or connect statement issued by user
            password = readPassword();
            user     = database.getUserManager().getUser(userName, password);

            session.commit();
            session.setUser(user);
            database.logger.logConnectUser(session);
        } else if (session.isProcessingLog) {

            // processing log statement
            // do not change the user, as isSys() must remain true when processing log
            session.commit();
        } else {

            // force throw if not log statement
            readThis(Token.PASSWORD);
        }
    }

    /**
     * Responsible for handling the execution of SET SQL statements
     *
     * @throws  HsqlException
     */
    private void processSet() throws HsqlException {

        session.setScripting(true);
        read();

        switch (tokenType) {

            case Token.PROPERTY : {
                String                 property;
                HsqlDatabaseProperties p;

                session.checkAdmin();
                read();
                checkIsSimpleName();
                checkIsQuoted();

                property = tokenString;
                p        = database.getProperties();

                boolean isboolean  = p.isBoolean(tokenString);
                boolean isintegral = p.isIntegral(tokenString);
                boolean isstring   = p.isString(tokenString);

                Trace.check(isboolean || isintegral || isstring,
                            Trace.ACCESS_IS_DENIED, tokenString);

                int type = isboolean ? Types.SQL_BOOLEAN
                                     : isintegral ? Types.SQL_INTEGER
                                                  : Types.SQL_CHAR;

                read();
                checkIsValue();

                if (valueType.type != type) {
                    throw Trace.error(Trace.WRONG_DATA_TYPE, tokenString);
                }

                if (HsqlDatabaseProperties.hsqldb_cache_file_scale.equals(
                        property)) {
                    if (database.logger.hasCache()
                            || ((Integer) value).intValue() != 8) {
                        Trace.throwerror(Trace.ACCESS_IS_DENIED, property);
                    }
                }

                p.setProperty(property, value.toString().toLowerCase());
                p.setDatabaseVariables();
                read();

                break;
            }
            case Token.SCHEMA : {
                session.setScripting(false);
                read();
                checkIsSimpleName();
                session.setSchema(tokenString);
                read();

                break;
            }
            case Token.PASSWORD : {
                session.checkDDLWrite();
                read();

                String password = readPassword();

                session.getUser().setPassword(password);

                break;
            }
            case Token.READONLY : {
                session.commit();
                read();
                session.setReadOnly(processTrueOrFalse());

                break;
            }
            case Token.LOGSIZE : {
                session.checkAdmin();
                session.checkDDLWrite();
                read();

                int i = readInteger();

                database.logger.setLogSize(i);

                break;
            }
            case Token.SCRIPTFORMAT : {
                session.checkAdmin();
                session.checkDDLWrite();
                session.setScripting(false);
                read();
                checkIsSimpleName();

                int i = ArrayUtil.find(ScriptWriterBase.LIST_SCRIPT_FORMATS,
                                       tokenString);

                if (i == 0 || i == 1 || i == 3) {
                    database.logger.setScriptType(i);
                } else {
                    throw unexpectedToken();
                }

                read();

                break;
            }
            case Token.IGNORECASE : {
                session.checkAdmin();
                read();
                session.checkDDLWrite();
                database.setIgnoreCase(processTrueOrFalse());

                break;
            }
            case Token.MAXROWS : {
                session.setScripting(false);
                read();

                int i = readInteger();

                session.setSQLMaxRows(i);

                break;
            }
            case Token.AUTOCOMMIT : {
                read();
                session.setAutoCommit(processTrueOrFalse());

                break;
            }
            case Token.DEFAULT : {
                session.setScripting(false);
                read();
                readThis(Token.RESULT);
                readThis(Token.MEMORY);
                readThis(Token.SIZE);

                int count = readInteger();

                database.getProperties().setProperty(
                    HsqlDatabaseProperties.hsqldb_result_max_memory_rows,
                    count);

                break;
            }
            case Token.RESULT : {
                session.setScripting(false);
                read();
                readThis(Token.MEMORY);
                readThis(Token.SIZE);

                int count = readInteger();

                session.setResultMemoryRowCount(count);

                break;
            }
            case Token.TABLE : {
                session.checkAdmin();
                session.checkDDLWrite();
                read();

                Table t = readTableName();

                checkSchemaUpdateAuthorization(t.getSchemaName().name);
                session.setScripting(true);

                switch (tokenType) {

                    default : {
                        throw unexpectedToken();
                    }
                    case Token.SOURCE : {
                        session.checkAdmin();
                        read();

                        if (tokenString.equals(Token.T_HEADER)) {
                            read();
                            checkIsQuoted();
                            t.setHeader(tokenString);
                            read();

                            break;
                        }

                        checkIsQuoted();

                        String  source = tokenString;
                        boolean isDesc = false;

                        read();

                        if (tokenType == Token.DESC) {
                            isDesc = true;

                            read();
                        }

                        t.setDataSource(session, source, isDesc, false);

                        break;
                    }
                    case Token.READONLY : {
                        session.checkAdmin();
                        read();
                        t.setDataReadOnly(processTrueOrFalse());

                        break;
                    }
                    case Token.INDEX : {
                        session.checkAdmin();
                        read();
                        checkIsValue();
                        t.setIndexRoots(tokenString);
                        read();

                        break;
                    }
                    case Token.TYPE : {
                        session.setScripting(false);

                        int newType;

                        read();

                        if (tokenType == Token.CACHED) {
                            newType = Table.CACHED_TABLE;
                        } else if (tokenType == Token.MEMORY) {
                            newType = Table.MEMORY_TABLE;
                        } else {
                            throw Trace.error(Trace.INVALID_CONVERSION);
                        }

                        read();
                        session.checkAdmin();
                        session.checkDDLWrite();

                        TableWorks tw = new TableWorks(session, t);

                        tw.setTableType(session, newType);
                    }
                }

                break;
            }
            case Token.REFERENTIAL_INTEGRITY : {
                session.checkAdmin();
                session.checkDDLWrite();
                session.setScripting(false);
                read();
                database.setReferentialIntegrity(processTrueOrFalse());

                break;
            }
            case Token.CHECKPOINT : {
                session.checkAdmin();
                session.checkDDLWrite();
                read();
                readThis(Token.DEFRAG);

                int size = readInteger();

                database.getProperties().setProperty(
                    HsqlDatabaseProperties.hsqldb_defrag_limit, size);

                break;
            }
            case Token.WRITE_DELAY : {
                session.checkAdmin();
                session.checkDDLWrite();

                int delay = 0;

                read();

                if (valueType.type == Types.SQL_INTEGER) {
                    delay = ((Integer) value).intValue();
                } else if (Boolean.TRUE.equals(value)) {
                    delay = database.getProperties().getDefaultWriteDelay();
                } else if (Boolean.FALSE.equals(value)) {
                    delay = 0;
                } else {
                    throw unexpectedToken();
                }

                read();

                if (tokenType == Token.MILLIS) {
                    read();
                } else {
                    delay *= 1000;
                }

                database.logger.setWriteDelay(delay);
                read();

                break;
            }
            case Token.DATABASE : {
                session.checkAdmin();
                session.checkDDLWrite();
                readThis(Token.COLLATION);
                checkIsQuoted();
                database.collation.setCollation(tokenString);

                break;
            }
            case Token.INITIAL : {
                read();
                readThis(Token.SCHEMA);
                session.setScripting(true);
                session.checkDDLWrite();

                HsqlName schemaName;

                if (tokenType == Token.DEFAULT) {
                    schemaName = null;
                } else {
                    schemaName =
                        database.schemaManager.getSchemaHsqlName(tokenString);
                }

                session.getUser().setInitialSchema(schemaName);
                database.setMetaDirty(true);
                read();

                break;
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

    /**
     * Retrieves boolean value corresponding to the next token.
     *
     * @return   true if next token is "TRUE"; false if next token is "FALSE"
     * @throws  HsqlException if the next token is neither "TRUE" or "FALSE"
     */
    private boolean processTrueOrFalse() throws HsqlException {

        checkIsValue();

        if (Boolean.TRUE.equals(value)) {
            read();

            return true;
        } else if (Boolean.FALSE.equals(value)) {
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
    private void processCommit() throws HsqlException {

        read();
        readNoiseWord(Token.WORK);
        session.commit();
    }

    /**
     * Responsible for handling the execution of ROLLBACK SQL statements.
     *
     * @throws  HsqlException
     */
    private void processRollback() throws HsqlException {

        read();
        readNoiseWord(Token.WORK);

        if (tokenType == Token.TO) {
            read();
            readThis(Token.SAVEPOINT);
            checkIsName();

            String name = tokenString;

            read();
            session.rollbackToSavepoint(name);

            return;
        }

        session.rollback();
    }

    /**
     * Responsible for handling the execution of SAVEPOINT SQL statements.
     *
     * @throws  HsqlException
     */
    private void processSavepoint() throws HsqlException {

        read();
        checkIsSimpleName();
        session.savepoint(tokenString);
    }

    private void processReleaseSavepoint() throws HsqlException {

        read();
        readThis(Token.SAVEPOINT);

        String name = tokenString;

        read();
        session.releaseSavepoint(name);
    }

    /**
     * Responsible for handling the execution of SHUTDOWN SQL statements
     *
     * @throws  HsqlException
     */
    private void processShutdown() throws HsqlException {

        int closemode;

        // HUH?  We should *NEVER* be able to get here if session is closed
        if (!session.isClosed()) {
            session.checkAdmin();
        }

        closemode = Database.CLOSEMODE_NORMAL;

        read();

        // catch misspelt qualifiers here
        switch (tokenType) {

            case Token.IMMEDIATELY :
                closemode = Database.CLOSEMODE_IMMEDIATELY;

                read();
                break;

            case Token.COMPACT :
                closemode = Database.CLOSEMODE_COMPACT;

                read();
                break;

            case Token.SCRIPT :
                closemode = Database.CLOSEMODE_SCRIPT;

                read();
                break;

            // only semicolon is accepted here
        }

        if (tokenType == Token.SEMICOLON) {
            read();
        }

        if (tokenType != Token.X_ENDPARSE) {
            throw unexpectedToken();
        }

        database.close(closemode);
    }

    /**
     * Responsible for handling CHECKPOINT [DEFRAG].
     *
     * @throws  HsqlException
     */
    private void processCheckpoint() throws HsqlException {

        boolean defrag;

        session.checkAdmin();
        session.checkDDLWrite();

        defrag = false;

        read();

        if (tokenType == Token.DEFRAG) {
            defrag = true;

            read();
        } else if (tokenType == Token.SEMICOLON) {
            read();

            // only semicolon is accepted here
        }

        if (tokenType != Token.X_ENDPARSE) {
            throw unexpectedToken();
        }

        database.logger.checkpoint(defrag);
    }

    private void processDisconnect() throws HsqlException {
        read();
        session.close();
    }

    private Result processExplainPlan() throws IOException, HsqlException {

        // PRE:  we assume only one DML or DQL has been submitted
        //       and simply ignore anything following the first
        //       sucessfully compliled statement
        CompiledStatement cs;
        Result            result;
        String            line;
        LineNumberReader  lnr;

        read();
        readThis(Token.PLAN);
        readThis(Token.FOR);

        result = Result.newSingleColumnResult("OPERATION", Type.SQL_VARCHAR);

        int brackets = 0;

        switch (tokenType) {

            case Token.OPENBRACKET :
                brackets = readOpenBrackets();
            case Token.SELECT :
                cs = compileSelectStatement(brackets);
                break;

            case Token.INSERT :
                cs = compileInsertStatement();
                break;

            case Token.UPDATE :
                cs = compileUpdateStatement();
                break;

            case Token.MERGE :
                cs = compileMergeStatement();
                break;

            case Token.DELETE :
                cs = compileDeleteStatement();
                break;

            case Token.CALL :
                cs = compileCallStatement();
                break;

            default :

                // - No real need to throw, so why bother?
                // - Just return result with no rows for now
                // - Later, maybe there will be plan desciptions
                //   for other operations
                return result;
        }

        lnr = new LineNumberReader(new StringReader(cs.describe(session)));

        while (null != (line = lnr.readLine())) {
            result.getNavigator().add(new Object[]{ line });
        }

        return result;
    }
}
