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
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.result.Result;
import org.hsqldb.rights.User;
import org.hsqldb.scriptio.ScriptWriterText;

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

    StatementCommand(int type, Object[] args) {
        this(type, args, null, null);
    }

    StatementCommand(int type, Object[] args, HsqlName readName,
                     HsqlName writeName) {

        super(type);

        this.isTransactionStatement = true;
        this.parameters             = args;

        if (readName != null && readName != writeName) {
            this.readTableNames = new HsqlName[]{ readName };
        }

        if (writeName != null) {
            this.writeTableNames = new HsqlName[]{ writeName };
        }

        switch (type) {

            case StatementTypes.DATABASE_CHECKPOINT :
                group    = StatementTypes.X_HSQLDB_DATABASE_OPERATION;
                isLogged = false;
                break;

            case StatementTypes.DATABASE_BACKUP :
            case StatementTypes.DATABASE_SCRIPT :
                group    = StatementTypes.X_HSQLDB_DATABASE_OPERATION;
                isLogged = false;
                break;

            case StatementTypes.SET_DATABASE_UNIQUE_NAME :
            case StatementTypes.SET_DATABASE_FILES_WRITE_DELAY :
            case StatementTypes.SET_DATABASE_FILES_TEMP_PATH :
                this.isTransactionStatement = false;
                group                       = StatementTypes.X_HSQLDB_SETTING;
                break;

//
            case StatementTypes.SET_DATABASE_DEFAULT_INITIAL_SCHEMA :
            case StatementTypes.SET_DATABASE_DEFAULT_TABLE_TYPE :
            case StatementTypes.SET_DATABASE_FILES_CACHE_ROWS :
            case StatementTypes.SET_DATABASE_FILES_CACHE_SIZE :
            case StatementTypes.SET_DATABASE_FILES_SCALE :
            case StatementTypes.SET_DATABASE_FILES_DEFRAG :
            case StatementTypes.SET_DATABASE_FILES_EVENT_LOG :
            case StatementTypes.SET_DATABASE_FILES_LOBS_SCALE :
            case StatementTypes.SET_DATABASE_FILES_LOCK :
            case StatementTypes.SET_DATABASE_FILES_LOG :
            case StatementTypes.SET_DATABASE_FILES_LOG_SIZE :
            case StatementTypes.SET_DATABASE_FILES_NIO :
            case StatementTypes.SET_DATABASE_FILES_SCRIPT_FORMAT :
            case StatementTypes.SET_DATABASE_PROPERTY :
            case StatementTypes.SET_DATABASE_RESULT_MEMORY_ROWS :
            case StatementTypes.SET_DATABASE_SQL_REFERENTIAL_INTEGRITY :
            case StatementTypes.SET_DATABASE_SQL_STRICT_NAMES :
            case StatementTypes.SET_DATABASE_SQL_STRICT_SIZE :
            case StatementTypes.SET_DATABASE_SQL_REFERENCES :
            case StatementTypes.SET_DATABASE_TRANSACTION_CONTROL :
            case StatementTypes.SET_DATABASE_DEFAULT_ISOLATION_LEVEL :
            case StatementTypes.SET_DATABASE_GC :

//
            case StatementTypes.SET_DATABASE_SQL_COLLATION :
            case StatementTypes.SET_DATABASE_FILES_BACKUP_INCREMENT :
            case StatementTypes.SET_DATABASE_SCRIPT_FORMAT :
            case StatementTypes.SET_DATABASE_TEXT_SOURCE :
                group                       = StatementTypes.X_HSQLDB_SETTING;
                this.isTransactionStatement = true;
                break;

            case StatementTypes.SET_TABLE_SOURCE_HEADER :
                isLogged = false;
            case StatementTypes.SET_TABLE_SOURCE :
                metaDataImpact              = Statement.META_RESET_VIEWS;
                group = StatementTypes.X_HSQLDB_SCHEMA_MANIPULATION;
                this.isTransactionStatement = true;
                break;

            case StatementTypes.SET_TABLE_READONLY :
                metaDataImpact              = Statement.META_RESET_VIEWS;
                group = StatementTypes.X_HSQLDB_SCHEMA_MANIPULATION;
                this.isTransactionStatement = true;
                break;

            case StatementTypes.DATABASE_SHUTDOWN :
                isLogged                    = false;
                group = StatementTypes.X_HSQLDB_DATABASE_OPERATION;
                this.isTransactionStatement = false;
                break;

            case StatementTypes.SET_TABLE_TYPE :
                group = StatementTypes.X_HSQLDB_SCHEMA_MANIPULATION;
                this.isTransactionStatement = true;
                break;

            case StatementTypes.SET_TABLE_INDEX :
                group                       = StatementTypes.X_HSQLDB_SETTING;
                this.isTransactionStatement = false;
                isLogged                    = false;
                break;

            case StatementTypes.SET_USER_INITIAL_SCHEMA :
            case StatementTypes.SET_USER_PASSWORD :
                group                       = StatementTypes.X_HSQLDB_SETTING;
                this.isTransactionStatement = false;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "StatementCommand");
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
                session.database.logger.writeToLog(session, sql);
            }
        } catch (Throwable e) {
            return Result.newErrorResult(e, sql);
        }

        return result;
    }

    Result getResult(Session session) {

        if (this.isExplain) {
            return Result.newSingleColumnStringResult("OPERATION",
                    describe(session));
        }

        switch (type) {

            case StatementTypes.DATABASE_BACKUP : {
                String  path       = ((String) parameters[0]);
                boolean blocking   = ((Boolean) parameters[1]).booleanValue();
                boolean script     = ((Boolean) parameters[2]).booleanValue();
                boolean compressed = ((Boolean) parameters[3]).booleanValue();

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

                    if (session.database.logger.isStoredFileAccess) {
                        return Result.newErrorResult(
                            Error.error(ErrorCode.DATABASE_IS_NON_FILE), null);
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
            case StatementTypes.DATABASE_CHECKPOINT : {
                boolean defrag = ((Boolean) parameters[0]).booleanValue();

                try {
                    session.database.logger.checkpoint(defrag);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_FILES_BACKUP_INCREMENT : {
                try {
                    boolean mode = ((Boolean) parameters[0]).booleanValue();

                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.logger.setIncrementBackup(mode);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_FILES_CACHE_ROWS : {
                try {
                    int value = ((Integer) parameters[0]).intValue();

                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.logger.setCacheMaxRows(value);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_FILES_CACHE_SIZE : {
                try {
                    int value = ((Integer) parameters[0]).intValue();

                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.logger.setCacheSize(value);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_FILES_LOBS_SCALE : {
                try {
                    int value = ((Integer) parameters[0]).intValue();

                    session.checkAdmin();
                    session.checkDDLWrite();
                    if (session.isProcessingScript) {
                        session.database.logger.setLobFileScaleNoCheck(value);
                    } else {
                        session.database.logger.setLobFileScale(value);
                    }
                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_FILES_SCALE : {
                try {
                    int value = ((Integer) parameters[0]).intValue();

                    session.checkAdmin();
                    session.checkDDLWrite();

                    if (session.isProcessingScript) {
                        session.database.logger.setCacheFileScaleNoCheck(value);
                    } else {
                        session.database.logger.setCacheFileScale(value);
                    }
                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_FILES_DEFRAG : {
                try {
                    int value = ((Integer) parameters[0]).intValue();

                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.logger.setDefagLimit(value);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_FILES_EVENT_LOG : {
                try {
                    int value = ((Integer) parameters[0]).intValue();

                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.logger.setEventLogLevel(value);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_FILES_LOCK : {
                try {

                    // no-op - to remove from release version
                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_FILES_NIO : {
                try {
                    boolean value = ((Boolean) parameters[0]).booleanValue();

                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.logger.setNioDataFile(value);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_FILES_LOG : {
                try {
                    boolean value = ((Boolean) parameters[0]).booleanValue();

                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.logger.setLogData(value);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_FILES_LOG_SIZE : {
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
            case StatementTypes.SET_DATABASE_FILES_TEMP_PATH : {
                try {
                    String value = (String) parameters[0];

                    session.checkAdmin();
                    session.checkDDLWrite();

                    // no action
                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_FILES_WRITE_DELAY : {
                try {
                    int value = ((Integer) parameters[0]).intValue();

                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.logger.setWriteDelay(value);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_SQL_COLLATION : {
                try {
                    String name = (String) parameters[0];

                    /** @todo 1.9.0 - ensure no data in character columns */
                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.collation.setCollation(name);
                    session.database.schemaManager.setSchemaChangeTimestamp();

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_SQL_REFERENTIAL_INTEGRITY : {
                boolean mode = ((Boolean) parameters[0]).booleanValue();

                session.database.setReferentialIntegrity(mode);

                return Result.updateZeroResult;
            }
            case StatementTypes.SET_DATABASE_SQL_STRICT_NAMES : {
                boolean mode = ((Boolean) parameters[0]).booleanValue();

                session.database.setStrictNames(mode);

                return Result.updateZeroResult;
            }
            case StatementTypes.SET_DATABASE_SQL_STRICT_SIZE : {
                boolean mode = ((Boolean) parameters[0]).booleanValue();

                session.database.setStrictColumnSize(mode);

                return Result.updateZeroResult;
            }
            case StatementTypes.SET_DATABASE_SQL_REFERENCES : {
                boolean mode = ((Boolean) parameters[0]).booleanValue();

                session.database.setStrictReferences(mode);

                return Result.updateZeroResult;
            }
            case StatementTypes.SET_DATABASE_DEFAULT_INITIAL_SCHEMA : {
                HsqlName schema = (HsqlName) parameters[0];

                //
                session.database.schemaManager.setDefaultSchemaHsqlName(
                    schema);
                session.database.schemaManager.setSchemaChangeTimestamp();

                //
                return Result.updateZeroResult;
            }
            case StatementTypes.SET_DATABASE_DEFAULT_TABLE_TYPE : {
                Integer type = (Integer) parameters[0];

                //
                session.database.schemaManager.setDefaultTableType(
                    type.intValue());

                //
                return Result.updateZeroResult;
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
            case StatementTypes.SET_DATABASE_TRANSACTION_CONTROL : {
                try {
                    int mode = ((Integer) parameters[0]).intValue();

                    session.checkAdmin();
                    session.checkDDLWrite();
                    session.database.txManager.setTransactionControl(session,
                            mode);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_DEFAULT_ISOLATION_LEVEL : {
                try {
                    int mode = ((Integer) parameters[0]).intValue();

                    session.checkAdmin();
                    session.checkDDLWrite();

                    session.database.defaultIsolationLevel = mode;

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_GC : {
                try {
                    int count = ((Integer) parameters[0]).intValue();

                    session.checkAdmin();

                    JavaSystem.gcFrequency = count;

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_PROPERTY : {
                try {
                    String property = (String) parameters[0];
                    Object value    = parameters[1];

                    // command is a no-op from 1.9
                    session.checkAdmin();
                    session.checkDDLWrite();

                    /*
                    HsqlDatabaseProperties p =
                        session.database.getProperties();

                    p.setDatabaseProperty(property,
                                          value.toString().toLowerCase());
                    */
                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_RESULT_MEMORY_ROWS : {
                int size = ((Integer) parameters[0]).intValue();

                session.database.getProperties().setProperty(
                    HsqlDatabaseProperties.hsqldb_result_max_memory_rows,
                    size);
                session.database.setResultMaxMemoryRows(size);

                return Result.updateZeroResult;
            }
            case StatementTypes.SET_DATABASE_TEXT_SOURCE : {
                try {
                    String         source = (String) parameters[0];
                    HsqlProperties props  = null;

                    if (source.length() > 0) {
                        props = HsqlProperties.delimitedArgPairsToProps(source,
                                "=", ";", null);

                        if (props.getErrorKeys().length > 0) {
                            throw Error.error(ErrorCode.TEXT_TABLE_SOURCE,
                                              props.getErrorKeys()[0]);
                        }
                    }

                    session.database.logger.setDefaultTextTableProperties(
                        source, props);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_DATABASE_UNIQUE_NAME : {
                try {
                    String name = (String) parameters[0];

                    session.database.setUniqueName(name);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.DATABASE_SCRIPT : {
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
            case StatementTypes.DATABASE_SHUTDOWN : {
                try {
                    int mode = ((Integer) parameters[0]).intValue();

                    session.database.close(mode);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
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
                    session.database.schemaManager.setSchemaChangeTimestamp();

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_TABLE_SOURCE :
            case StatementTypes.SET_TABLE_SOURCE_HEADER : {
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

                        session.database.schemaManager.setSchemaChangeTimestamp();

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

                    return Result.updateZeroResult;
                } catch (Throwable e) {
                    if (!(e instanceof HsqlException)) {
                        e = Error.error(ErrorCode.GENERAL_IO_ERROR,
                                        e.getMessage());
                    }

                    if (session.isProcessingLog()
                            || session.isProcessingScript()) {
                        session.addWarning((HsqlException) e);
                        session.database.logger.logWarningEvent(
                            "Problem processing SET TABLE SOURCE", e);

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
                    session.database.schemaManager.setSchemaChangeTimestamp();

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_USER_INITIAL_SCHEMA : {
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
                    session.database.schemaManager.setSchemaChangeTimestamp();

                    //
                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            case StatementTypes.SET_USER_PASSWORD : {
                try {
                    User   user = parameters[0] == null ? session.getUser()
                                                        : (User) parameters[0];
                    String password = (String) parameters[1];

                    session.checkDDLWrite();
                    session.setScripting(true);
                    user.setPassword(password);

                    return Result.updateZeroResult;
                } catch (HsqlException e) {
                    return Result.newErrorResult(e, sql);
                }
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "StatemntCommand");
        }
    }

    public boolean isAutoCommitStatement() {
        return isTransactionStatement;
    }

    public String describe(Session session) {
        return sql;
    }
}
