/* Copyright (c) 2001-2022, The HSQL Development Group
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


package org.hsqldb.persist;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import org.hsqldb.Database;
import org.hsqldb.DatabaseType;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.NumberSequence;
import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.SessionInterface;
import org.hsqldb.Statement;
import org.hsqldb.Table;
import org.hsqldb.TableBase;
import org.hsqldb.Tokens;
import org.hsqldb.TransactionManager;
import org.hsqldb.TransactionManagerMV2PL;
import org.hsqldb.TransactionManagerMVCC;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.index.IndexAVL;
import org.hsqldb.index.IndexAVLMemory;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.EventLogInterface;
import org.hsqldb.lib.FileAccess;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.FrameworkLogger;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.InputStreamInterface;
import org.hsqldb.lib.InputStreamWrapper;
import org.hsqldb.lib.SimpleLog;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.lib.tar.DbBackup;
import org.hsqldb.lib.tar.TarMalformatException;
import org.hsqldb.result.Result;
import org.hsqldb.scriptio.ScriptWriterBase;
import org.hsqldb.scriptio.ScriptWriterText;
import org.hsqldb.types.RowType;
import org.hsqldb.types.Type;

// campbell-burnet@users 20030510 - patch 1.7.2 - added cooperative file locking

/**
 *  The public interface of persistence and logging classes.<p>
 *
 *  Implements a storage manager wrapper that provides a consistent,
 *  always available interface to storage management for the Database
 *  class, despite the fact not all Database objects actually use file
 *  storage.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.0
 * @since 1.7.0
 */
public class Logger implements EventLogInterface {

    public SimpleLog appLog;
    public SimpleLog sqlLog;

    //
    FrameworkLogger fwLogger;
    FrameworkLogger sqlLogger;

    //
    private Database database;
    private boolean  logsStatements;    // false indicates Log is being opened
    private boolean  loggingEnabled;

    //
    private boolean propIsFileDatabase;
    boolean         propNioDataFile;
    long            propNioMaxSize    = 256 * 1024 * 1024L;
    int             propMaxFreeBlocks = 512;
    private int     propCacheMaxRows;
    private long    propCacheMaxSize;
    int             propDataFileDefragLimit;
    private int     propDataFileScale;
    int             propDataFileSpace;
    String          propTextSourceDefault = "";
    boolean         propTextAllowFullPath;
    private int     propWriteDelay;
    private int     propLogSize;
    private boolean propLogData = true;
    private int     propExternalEventLogLevel;
    private int     propEventLogLevel;
    int             propSqlLogLevel;
    int             propGC;
    int             propTxMode       = TransactionManager.LOCKS;
    boolean         propRefIntegrity = true;
    int             propLobBlockSize = 32 * 1024;
    boolean         propCompressLobs;
    int             propScriptFormat = 0;
    boolean         propLargeData;
    long            propFilesTimestamp;

    //
    Log               log;
    private LockFile  lockFile;
    private Crypto    crypto;
    boolean           cryptLobs;
    public FileAccess fileAccess;
    String            tempDirectoryPath;

    //
    public TextTableStorageManager textTableManager =
        new TextTableStorageManager();

    //
    public boolean isNewDatabase;

    //
    public boolean isSingleFile;

    //
    AtomicInteger backupState     = new AtomicInteger();
    AtomicInteger checkpointState = new AtomicInteger();

    //
    long maxLogSize;

    //
    static final int largeDataFactor = 128;

    // backupState cycle normal, backup, normal or normal, checkpoint, normal
    static final int stateNormal     = 0;
    static final int stateBackup     = 1;
    static final int stateCheckpoint = 2;

    // checkpointState cycle normal, required, due, normal
    static final int stateCheckpointNormal   = 0;
    static final int stateCheckpointRequired = 1;
    static final int stateCheckpointDue      = 2;

    //
    public static final String oldFileExtension        = ".old";
    public static final String newFileExtension        = ".new";
    public static final String appLogFileExtension     = ".app.log";
    public static final String sqlLogFileExtension     = ".sql.log";
    public static final String logFileExtension        = ".log";
    public static final String scriptFileExtension     = ".script";
    public static final String propertiesFileExtension = ".properties";
    public static final String dataFileExtension       = ".data";
    public static final String backupFileExtension     = ".backup";
    public static final String lobsFileExtension       = ".lobs";
    public static final String lockFileExtension       = ".lck";

    public Logger(Database database) {
        this.database = database;
    }

    /**
     *  Opens the specified Database object's database files and starts up
     *  the logging process. <p>
     *
     *  If the specified Database object is a new database, its database
     *  files are first created.
     *
     * @throws  org.hsqldb.HsqlException if there is a problem, such as the case when
     *      the specified files are in use by another process
     */
    public void open() {

        boolean hasFileProps = false;
        boolean hasScript    = false;

        fileAccess = FileUtil.getFileAccess(database.isFilesInJar());
        propIsFileDatabase          = database.getType().isFileBased();
        database.databaseProperties = new HsqlDatabaseProperties(database);
        propTextAllowFullPath = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.textdb_allow_full_path);

        if (propIsFileDatabase) {
            hasFileProps = database.databaseProperties.load();
            hasScript = fileAccess.isStreamElement(database.getPath()
                                                   + scriptFileExtension);

            boolean version18 = database.databaseProperties.isVersion18();

            if (version18) {
                throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION,
                                  "unsupported old database version");
            }

            boolean exists = hasScript;

            if (!exists) {
                exists = fileAccess.isStreamElement(database.getPath()
                                                    + scriptFileExtension
                                                    + Logger.newFileExtension);

                if (exists) {
                    database.databaseProperties.setDBModified(
                        HsqlDatabaseProperties.FILES_MODIFIED_NEW);
                } else {
                    exists = fileAccess.isStreamElement(database.getPath()
                                                        + dataFileExtension);

                    if (exists) {
                        throw Error.error(ErrorCode.DATA_FILE_ERROR,
                                          "database files not complete");
                    }

                    exists = fileAccess.isStreamElement(database.getPath()
                                                        + backupFileExtension);

                    if (exists) {
                        throw Error.error(ErrorCode.DATA_FILE_ERROR,
                                          "database files not complete");
                    }
                }
            }

            isNewDatabase = !exists;
        } else {
            isNewDatabase = true;
        }

        if (isNewDatabase) {
            String name = newUniqueName();

            database.setDatabaseName(name);

            boolean checkExists = database.isFilesInJar();

            checkExists |=
                (database.urlProperties
                    .isPropertyTrue(HsqlDatabaseProperties
                        .url_ifexists) || !database.urlProperties
                            .isPropertyTrue(HsqlDatabaseProperties
                                .url_create, true));

            if (checkExists) {
                throw Error.error(ErrorCode.DATABASE_NOT_EXISTS,
                                  database.getPath());
            }

            database.databaseProperties.setURLProperties(
                database.urlProperties);
        } else {
            if (!hasFileProps) {
                database.databaseProperties.setDBModified(
                    HsqlDatabaseProperties.FILES_MODIFIED);
            }

            // properties that also apply to existing database only if they exist
            if (database.urlProperties.isPropertyTrue(
                    HsqlDatabaseProperties.hsqldb_files_readonly)) {
                database.databaseProperties.setProperty(
                    HsqlDatabaseProperties.hsqldb_files_readonly, true);
            }

            if (database.urlProperties.isPropertyTrue(
                    HsqlDatabaseProperties.hsqldb_readonly)) {
                database.databaseProperties.setProperty(
                    HsqlDatabaseProperties.hsqldb_readonly, true);
            }

            // hsqldb.lock_file=false is applied
            if (!database.urlProperties.isPropertyTrue(
                    HsqlDatabaseProperties.hsqldb_lock_file, true)) {
                database.databaseProperties.setProperty(
                    HsqlDatabaseProperties.hsqldb_lock_file, false);
            }
        }

        setVariables();

        String appLogPath = null;
        String sqlLogPath = null;

        if (propIsFileDatabase && !database.isFilesReadOnly()) {
            appLogPath = database.getPath() + appLogFileExtension;
            sqlLogPath = database.getPath() + sqlLogFileExtension;
        }

        appLog = new SimpleLog(appLogPath, propEventLogLevel, false);
        sqlLog = new SimpleLog(sqlLogPath, propSqlLogLevel, true);

        database.setReferentialIntegrity(propRefIntegrity);

        if (!isFileDatabase()) {
            return;
        }

        checkpointState.set(stateCheckpointNormal);

        logsStatements = false;

        boolean useLock = database.getProperties().isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_lock_file);

        if (useLock && !database.isFilesReadOnly()) {
            acquireLock(database.getPath());
        }

        log = new Log(database);

        log.open();

        logsStatements = true;
        loggingEnabled = propLogData && !database.isFilesReadOnly();

        if (database.getNameString().isEmpty()) {
            database.setDatabaseName(newUniqueName());
        }

        // URL database properties that can override .script file settings
        int level = database.urlProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_applog, -1);

        if (level >= 0) {
            setEventLogLevel(level, false);
        }

        level = database.urlProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_extlog, -1);

        if (level >= 0) {
            setExternalEventLogLevel(level);
        }

        level = database.urlProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_sqllog, -1);

        if (level >= 0) {
            setEventLogLevel(level, true);
        }
    }

    private void setVariables() {

        String cryptKey = database.urlProperties.getProperty(
            HsqlDatabaseProperties.url_crypt_key);

        if (cryptKey != null) {
            String cryptType = database.urlProperties.getProperty(
                HsqlDatabaseProperties.url_crypt_type);
            String cryptProvider = database.urlProperties.getProperty(
                HsqlDatabaseProperties.url_crypt_provider);
            String cryptIv = database.urlProperties.getProperty(
                HsqlDatabaseProperties.url_crypt_iv);

            crypto = new Crypto(cryptKey, cryptIv, cryptType, cryptProvider);
            cryptLobs = database.urlProperties.isPropertyTrue(
                HsqlDatabaseProperties.url_crypt_lobs, true);
        }

        if (database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.hsqldb_readonly)) {
            database.setReadOnly();
        }

        if (database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.hsqldb_files_readonly)) {
            database.setFilesReadOnly();
        }

        // handle invalid paths as well as access issues
        if (!database.isFilesReadOnly()) {
            if (database.getType() == DatabaseType.DB_MEM) {
                tempDirectoryPath = database.getProperties().getStringProperty(
                    HsqlDatabaseProperties.hsqldb_temp_directory);
            } else {
                tempDirectoryPath = database.getPath() + ".tmp";
            }

            if (tempDirectoryPath != null) {
                tempDirectoryPath =
                    FileUtil.makeDirectories(tempDirectoryPath);
            }
        }

        propScriptFormat = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_script_format);
        propMaxFreeBlocks = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_cache_free_count);
        propMaxFreeBlocks = ArrayUtil.getTwoPowerFloor(propMaxFreeBlocks);

        if (database.urlProperties.isPropertyTrue(
                HsqlDatabaseProperties.hsqldb_large_data, false)) {
            propLargeData = true;
        }

        if (!database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.sql_pad_space, true)) {
            database.collation.setPadding(false);
        }

        String temp = database.getProperties().getStringProperty(
            HsqlDatabaseProperties.hsqldb_digest);

        database.granteeManager.setDigestAlgo(temp);

        if (!isNewDatabase) {
            return;
        }

        if (tempDirectoryPath != null) {
            int rows = database.databaseProperties.getIntegerProperty(
                HsqlDatabaseProperties.hsqldb_result_max_memory_rows);

            database.setResultMaxMemoryRows(rows);
        }

        String tableType = database.databaseProperties.getStringProperty(
            HsqlDatabaseProperties.hsqldb_default_table_type);

        if (Tokens.T_CACHED.equalsIgnoreCase(tableType)) {
            database.schemaManager.setDefaultTableType(TableBase.CACHED_TABLE);
        }

        String txMode = database.databaseProperties.getStringProperty(
            HsqlDatabaseProperties.hsqldb_tx);

        if (Tokens.T_MVCC.equalsIgnoreCase(txMode)) {
            propTxMode = TransactionManager.MVCC;
        } else if (Tokens.T_MVLOCKS.equalsIgnoreCase(txMode)) {
            propTxMode = TransactionManager.MVLOCKS;
        } else if (Tokens.T_LOCKS.equalsIgnoreCase(txMode)) {
            propTxMode = TransactionManager.LOCKS;
        }

        switch (propTxMode) {

            case TransactionManager.LOCKS :
                break;

            case TransactionManager.MVLOCKS :
                database.txManager = new TransactionManagerMV2PL(database);
                break;

            case TransactionManager.MVCC :
                database.txManager = new TransactionManagerMVCC(database);
                break;
        }

        String txLevel = database.databaseProperties.getStringProperty(
            HsqlDatabaseProperties.hsqldb_tx_level);

        if (Tokens.T_SERIALIZABLE.equalsIgnoreCase(txLevel)) {
            database.defaultIsolationLevel = SessionInterface.TX_SERIALIZABLE;
        } else {
            database.defaultIsolationLevel =
                SessionInterface.TX_READ_COMMITTED;
        }

        database.txConflictRollback =
            database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.hsqldb_tx_conflict_rollback);
        database.txInterruptRollback =
            database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.hsqldb_tx_interrupt_rollback);
        database.sqlRestrictExec = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_restrict_exec);
        database.sqlEnforceNames = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_enforce_names);
        database.sqlRegularNames = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_regular_names);
        database.sqlEnforceRefs = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_enforce_refs);
        database.sqlEnforceSize = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_enforce_size);
        database.sqlEnforceTypes = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_enforce_types);
        database.sqlEnforceTDCD = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_enforce_tdcd);
        database.sqlEnforceTDCU = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_enforce_tdcu);
        database.sqlTranslateTTI = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.jdbc_translate_tti_types);
        database.sqlLiveObject = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_live_object);
        database.sqlCharLiteral = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_char_literal);
        database.sqlConcatNulls = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_concat_nulls);
        database.sqlNullsFirst = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_nulls_first);
        database.sqlNullsOrder = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_nulls_order);
        database.sqlUniqueNulls = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_unique_nulls);
        database.sqlConvertTruncate =
            database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.sql_convert_trunc);
        database.sqlTruncateTrailing =
            database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.sql_trunc_trailing);
        database.sqlAvgScale = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.sql_avg_scale);
        database.sqlMaxRecursive =
            database.databaseProperties.getIntegerProperty(
                HsqlDatabaseProperties.sql_max_recursive);
        database.sqlDoubleNaN = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_double_nan);
        database.sqlLongvarIsLob = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_longvar_is_lob);
        database.sqlIgnoreCase = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_ignore_case);
        database.sqlSyntaxDb2 = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_syntax_db2);
        database.sqlSyntaxMss = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_syntax_mss);
        database.sqlSyntaxMys = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_syntax_mys);
        database.sqlSyntaxOra = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_syntax_ora);
        database.sqlSyntaxPgs = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_syntax_pgs);
        database.sqlSysIndexNames = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_sys_index_names);
        database.sqlLowerCaseIdentifier =
            database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.sql_lowercase_ident);

        if (database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.sql_compare_in_locale)) {
            database.collation.setCollationAsLocale();
        }

        propEventLogLevel = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_applog);
        propExternalEventLogLevel =
            database.databaseProperties.getIntegerProperty(
                HsqlDatabaseProperties.hsqldb_extlog);
        propSqlLogLevel = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_sqllog);

        if (database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.hsqldb_files_readonly)) {
            database.setFilesReadOnly();
        }

        if (database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.hsqldb_readonly)) {
            database.setReadOnly();
        }

        propNioDataFile = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_nio_data_file);
        propNioMaxSize =
            database.databaseProperties.getIntegerProperty(
                HsqlDatabaseProperties.hsqldb_nio_max_size) * 1024L * 1024L;
        propCacheMaxRows = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_cache_rows);
        propCacheMaxSize =
            database.databaseProperties.getIntegerProperty(
                HsqlDatabaseProperties.hsqldb_cache_size) * 1024L;

        setLobFileScaleNoCheck(
            database.databaseProperties.getIntegerProperty(
                HsqlDatabaseProperties.hsqldb_lob_file_scale));
        setLobFileCompressedNoCheck(
            database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.hsqldb_lob_file_compressed));
        setDataFileScaleNoCheck(
            database.databaseProperties.getIntegerProperty(
                HsqlDatabaseProperties.hsqldb_cache_file_scale));

        // linked with above FILES SCALE
        boolean fileSpace = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_files_space);

        setDataFileSpace(fileSpace);

        propDataFileDefragLimit =
            database.databaseProperties.getIntegerProperty(
                HsqlDatabaseProperties.hsqldb_defrag_limit);
        propWriteDelay = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_write_delay_millis);

        if (!database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.hsqldb_write_delay)) {
            propWriteDelay = 0;
        }

        setLogSize(
            database.databaseProperties.getIntegerProperty(
                HsqlDatabaseProperties.hsqldb_log_size));

        propLogData = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_log_data);
        propGC = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.runtime_gc_interval);
        propRefIntegrity = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_ref_integrity);
    }

// fredt@users 20020130 - patch 495484 by campbell-burnet@users

    /**
     *  Shuts down the logging process using the specified mode. <p>
     *
     * @param  closemode The mode in which to shut down the logging
     *      process
     *      <OL>
     *        <LI> CLOSEMODE_IMMEDIATELY performs SHUTDOWN IMMEDIATELY, equivalent
     *        to  a poweroff or crash.
     *        <LI> CLOSEMODE_NORMAL performs a normal SHUTDOWN that
     *        checkpoints the database normally.
     *        <LI> CLOSEMODE_COMPACT performs a shutdown compact that scripts
     *        out the contents of any CACHED tables to the log then
     *        deletes the existing *.data file that contains the data
     *        for all CACHED table before the normal checkpoint process
     *        which in turn creates a new, compact *.data file.
     *        <LI> CLOSEMODE_SCRIPT performs a SHUTDOWN SCRIPT.
     *      </OL>
     *
     * @return  true if closed with no problems or false if a problem was
     *        encountered.
     */
    public boolean close(int closemode) {

        boolean result = true;

        if (log == null) {
            textTableManager.closeAllTextCaches(false);

            return true;
        }

        log.synchLog();
        database.lobManager.synch();

        try {
            switch (closemode) {

                case Database.CLOSEMODE_IMMEDIATELY :
                    log.closeImmediately();
                    break;

                case Database.CLOSEMODE_NORMAL :
                    log.close(false);
                    break;

                case Database.CLOSEMODE_COMPACT :
                case Database.CLOSEMODE_SCRIPT :
                    log.close(true);
                    break;
            }

            database.persistentStoreCollection.release();
        } catch (Throwable e) {
            database.logger.logSevereEvent("error closing log", e);

            result = false;
        }

        logInfoEvent("Database closed");

        log = null;

        appLog.close();
        sqlLog.close();

        logsStatements = false;
        loggingEnabled = false;

        return result;
    }

    String newUniqueName() {

        String name = StringUtil.toPaddedString(
            Long.toHexString(System.currentTimeMillis()), 16, '0', false);

        name = "HSQLDB" + name.substring(6).toUpperCase(Locale.ENGLISH);

        return name;
    }

    /*
     * Must return correct mode prior to initialisation
     * @return  true if this object encapsulates a non-null Log instance,
     *      else false
     */
    public boolean isLogged() {
        return propIsFileDatabase && !database.isFilesReadOnly();
    }

    public boolean isCurrentlyLogged() {
        return loggingEnabled;
    }

    public boolean isAllowedFullPath() {
        return propTextAllowFullPath;
    }

    /**
     * All usage of FrameworkLogger should call this method before using an
     * instance.
     *
     * It ensures and requires that no logging should take place before a new
     * database unique name has been created for a new database or read from the
     * .script file for an old database.<p>
     *
     * An instance is returned when:
     * - database unique name has been created
     * - FrameworkLogger would use log4j
     *
     * Otherwise null is returned.
     *
     * This tactic avoids usage of file-based jdk logging for the time being.
     *
     */
    private void getEventLogger() {

        if (fwLogger != null) {
            return;
        }

        String name = database.getNameString();

        if (name.isEmpty()) {

            // The database unique name is set up at different times
            // depending on upgraded / exiting / new databases.
            // Therefore FrameworkLogger is not used until the unique
            // name is known.
            return;
        }

        fwLogger = FrameworkLogger.getLog(SimpleLog.logTypeNameEngine,
                                          "hsqldb.db."
                                          + database.getNameString());
        /*
        sqlLogger = FrameworkLogger.getLog(SimpleLog.logTypeNameEngine,
                                           "hsqldb.sql."
                                           + database.getUniqueName());
        */
    }

    public void setEventLogLevel(int level, boolean logSql) {

        if (level < SimpleLog.LOG_NONE || level > SimpleLog.LOG_RESULT) {
            throw Error.error(ErrorCode.X_42556);
        }

        if (logSql) {
            propSqlLogLevel = level;

            sqlLog.setLevel(level);
        } else {
            propEventLogLevel = level;

            appLog.setLevel(level);
        }
    }

    public void setExternalEventLogLevel(int level) {

        if (level < SimpleLog.LOG_NONE || level > SimpleLog.LOG_DETAIL) {
            throw Error.error(ErrorCode.X_42556);
        }

        propExternalEventLogLevel = level;
    }

    public void logSevereEvent(String message, Throwable t) {

        getEventLogger();

        if (fwLogger != null) {
            if (propExternalEventLogLevel >= SimpleLog.LOG_ERROR) {
                fwLogger.severe(message, t);
            }
        }

        if (appLog != null) {
            if (t == null) {
                appLog.logContext(SimpleLog.LOG_ERROR, message);
            } else {
                appLog.logContext(t, message, SimpleLog.LOG_ERROR);
            }
        }
    }

    public void logWarningEvent(String message, Throwable t) {

        getEventLogger();

        if (fwLogger != null) {
            if (propExternalEventLogLevel >= SimpleLog.LOG_WARNING) {
                fwLogger.warning(message, t);
            }
        }

        appLog.logContext(t, message, SimpleLog.LOG_WARNING);
    }

    public void logInfoEvent(String message) {

        getEventLogger();

        if (fwLogger != null) {
            if (propExternalEventLogLevel >= SimpleLog.LOG_NORMAL) {
                fwLogger.info(message);
            }
        }

        appLog.logContext(SimpleLog.LOG_NORMAL, message);
    }

    public void logDetailEvent(String message) {

        getEventLogger();

        if (fwLogger != null) {
            if (propExternalEventLogLevel >= SimpleLog.LOG_DETAIL) {
                fwLogger.finest(message);
            }
        }

        if (appLog != null) {
            appLog.logContext(SimpleLog.LOG_DETAIL, message);
        }
    }

    public void logStatementEvent(Session session, Statement statement,
                                  Object[] paramValues, Result result,
                                  int level) {

        if (sqlLog != null && level <= propSqlLogLevel) {
            String sessionId   = Long.toString(session.getId());
            String sql         = statement.getSQL();
            String values      = "";
            int    paramLength = 0;

            if (propSqlLogLevel < SimpleLog.LOG_DETAIL) {
                if (sql.length() > 256) {
                    sql = sql.substring(0, 256);
                }

                paramLength = 32;
            }

            if (paramValues != null && paramValues.length > 0) {
                values = RowType.convertToSQLString(
                    paramValues,
                    statement.getParametersMetaData().getParameterTypes(),
                    paramLength);
            }

            if (propSqlLogLevel == SimpleLog.LOG_RESULT) {
                StringBuilder sb = new StringBuilder(values);

                sb.append(' ').append('[');

                if (result.isError()) {
                    sb.append(result.getErrorCode());
                } else if (result.isData()) {
                    sb.append(result.getNavigator().getSize());
                } else if (result.isUpdateCount()) {
                    sb.append(result.getUpdateCount());
                }

                sb.append(']');

                values = sb.toString();
            }

            sqlLog.logContext(level, sessionId, sql, values);
        }
    }

    public int getSqlEventLogLevel() {
        return propSqlLogLevel;
    }

    /**
     * Returns the Cache object or null if one doesn't exist.
     */
    public DataFileCache getCache() {

        if (log == null) {
            return null;
        } else {
            return log.getCache();
        }
    }

    /**
     * Returns true if Cache object exists.
     */
    public boolean hasCache() {

        if (log == null) {
            return false;
        } else {
            return log.hasCache();
        }
    }

    /**
     * Records a Log entry for the specified SQL statement, on behalf of
     * the specified Session object.
     */
    public synchronized void writeOtherStatement(Session session,
            String statement) {

        if (loggingEnabled) {
            log.writeOtherStatement(session, statement);
        }
    }

    /**
     * Used exclusively by PersistentStore objects
     */
    public synchronized void writeInsertStatement(Session session, Row row,
            Table table) {

        if (loggingEnabled) {
            log.writeInsertStatement(session, row, table);
        }
    }

    /**
     * Used exclusively by PersistentStore objects
     */
    public synchronized void writeDeleteStatement(Session session, Table t,
            Object[] row) {

        if (loggingEnabled) {
            log.writeDeleteStatement(session, t, row);
        }
    }

    /**
     * Used at transaction commit
     */
    public synchronized void writeSequenceStatement(Session session,
            NumberSequence s) {

        if (loggingEnabled) {
            log.writeSequenceStatement(session, s);
        }
    }

    /**
     * Used at transaction commit
     */
    public synchronized void writeCommitStatement(Session session) {

        if (loggingEnabled) {
            log.writeCommitStatement(session);
        } else {
            if (maxLogSize > 0
                    && database.lobManager.getUsageChanged() > maxLogSize) {
                setCheckpointRequired();
            }
        }
    }

    public synchronized void synchLog() {

        if (loggingEnabled) {
            log.synchLog();
        }
    }

    /**
     *  Checkpoints the database. <p>
     *
     *  The most important effect of calling this method is to cause the
     *  log file to be rewritten in the most efficient form to
     *  reflect the current state of the database, i.e. only the DDL and
     *  insert DML required to recreate the database in its present state.
     *  Other house-keeping duties are performed w.r.t. other database
     *  files, in order to ensure as much as possible the ACID properties
     *  of the database.
     *
     * @throws  org.hsqldb.HsqlException if there is a problem checkpointing the
     *      database
     */
    public void checkpoint(Session session, boolean defrag, boolean lobs) {

        if (!backupState.compareAndSet(stateNormal, stateCheckpoint)) {
            throw Error.error(ErrorCode.ACCESS_IS_DENIED);
        }

        database.lobManager.lock();

        try {
            synchronized (this) {
                checkpointInternal(session, defrag);

                if (lobs) {
                    Result result = database.lobManager.deleteUnusedLobs();

                    if (log != null && result.getUpdateCount() > 0) {
                        log.synchLog();
                        logDetailEvent("Deleted unused LOBs, count: "
                                       + result.getUpdateCount());
                    }
                }
            }
        } finally {
            backupState.set(stateNormal);
            checkpointState.set(stateCheckpointNormal);
            database.lobManager.unlock();
        }
    }

    private void checkpointInternal(Session session, boolean defrag) {

        if (logsStatements) {
            logInfoEvent("Checkpoint start");
            log.checkpoint(session, defrag);
            logInfoEvent("Checkpoint end - txts: "
                         + database.txManager.getSystemChangeNumber());
        }
    }

    /**
     *  Sets the maximum size to which the log file can grow
     *  before being automatically checkpointed.
     *
     * @param  megas size in MB
     */
    public synchronized void setLogSize(int megas) {

        propLogSize = megas;
        maxLogSize  = propLogSize * 1024L * 1024;

        if (log != null) {
            log.setLogSize(propLogSize);
        }
    }

    /**
     *  Sets logging on or off.
     */
    public synchronized void setLogData(boolean mode) {

        propLogData    = mode;
        loggingEnabled = propLogData && !database.isFilesReadOnly();
        loggingEnabled &= logsStatements;
    }

    /**
     *  Sets the type of script file, currently 0 for text (default)
     *  3 for compressed
     *
     * @param  format The type
     */
    public synchronized void setScriptType(int format) {

        if (format == propScriptFormat) {
            return;
        }

        propScriptFormat = format;

        checkpointState.compareAndSet(stateCheckpointNormal,
                                      stateCheckpointRequired);
    }

    /**
     *  Sets the log write delay mode to number of seconds. By default
     *  executed commands written to the log are committed fully at most
     *  0.5 second after they are executed. This improves performance for
     *  applications that execute a large number
     *  of short running statements in a short period of time, but risks
     *  failing to log some possibly large number of statements in the
     *  event of a crash. A small value improves recovery.
     *  A value of 0 will severly slow down logging when autocommit is on,
     *  or many short transactions are committed.
     *
     * @param delay in milliseconds
     */
    public synchronized void setWriteDelay(int delay) {

        propWriteDelay = delay;

        if (log != null) {
            log.setWriteDelay(delay);
        }
    }

    public Crypto getCrypto() {
        return crypto;
    }

    public int getWriteDelay() {
        return propWriteDelay;
    }

    public int getLogSize() {
        return propLogSize;
    }

    public int getLobBlockSize() {
        return propLobBlockSize;
    }

    public void setCacheMaxRows(int value) {
        propCacheMaxRows = value;
    }

    public int getCacheMaxRows() {
        return propCacheMaxRows;
    }

    public void setCacheSize(int value) {
        propCacheMaxSize = value * 1024L;
    }

    public long getCacheSize() {
        return propCacheMaxSize;
    }

    public void setDataFileScale(int value) {

        if (value < 16) {
            throw Error.error(ErrorCode.X_42556);
        }

        setDataFileScaleNoCheck(value);
    }

    public void setDataFileScaleNoCheck(int value) {

        checkPower(value, 10);

        if (value < 8 && value != 1) {
            throw Error.error(ErrorCode.X_42556);
        }

        propDataFileScale = value;

        if (propDataFileSpace > 0) {
            propDataFileSpace = propDataFileScale / 16;    // change to default
        }
    }

    public int getDataFileScale() {
        return propDataFileScale;
    }

    public int getDataFileFactor() {
        return propLargeData ? largeDataFactor
                             : 1;
    }

    public void setDataFileSpace(boolean value) {

        if (value) {
            setDataFileSpace(propDataFileScale / 16);
        } else {
            setDataFileSpace(0);
        }
    }

    public void setDataFileSpace(int value) {

        if (value != 0) {
            checkPower(value, 6);
        }

        if (value > propDataFileScale / 16) {
            value = propDataFileScale / 16;
        }

        propDataFileSpace = value;
    }

    public int getDataFileSpace() {
        return propDataFileSpace;
    }

    public long getFilesTimestamp() {
        return propFilesTimestamp;
    }

    public void setFilesTimestamp(long value) {
        propFilesTimestamp = value;
    }

    public void setLobFileScale(int value) {

        if (propLobBlockSize == value * 1024) {
            return;
        }

        checkPower(value, 5);

        if (database.lobManager.getLobCount() > 0) {
            throw Error.error(ErrorCode.DATA_FILE_IN_USE);
        }

        propLobBlockSize = value * 1024;

        database.lobManager.close();
        database.lobManager.open();
    }

    public void setLobFileScaleNoCheck(int value) {

        checkPower(value, 5);

        propLobBlockSize = value * 1024;
    }

    public int getLobFileScale() {
        return propLobBlockSize / 1024;
    }

    public void setLobFileCompressed(boolean value) {

        if (propCompressLobs == value) {
            return;
        }

        if (database.lobManager.getLobCount() > 0) {
            throw Error.error(ErrorCode.DATA_FILE_IN_USE);
        }

        propCompressLobs = value;

        database.lobManager.close();
        database.lobManager.open();
    }

    public void setLobFileCompressedNoCheck(boolean value) {
        propCompressLobs = value;
    }

    public void setDefagLimit(int value) {

        if (value > 0 && value < 25) {
            value = 25;
        }

        propDataFileDefragLimit = value;
    }

    public int getDefragLimit() {
        return propDataFileDefragLimit;
    }

    public void setDefaultTextTableProperties(String source,
            HsqlProperties props) {

        props.setProperty(HsqlDatabaseProperties.url_check_props, true);
        database.getProperties().setURLProperties(props);

        propTextSourceDefault = source;
    }

    public void setNioDataFile(boolean value) {
        propNioDataFile = value;
    }

    public void setNioMaxSize(int value) {

        value = ArrayUtil.getTwoPowerFloor(value);

        if (value < 64) {
            value = 64;
        }

        propNioMaxSize = value * 1024L * 1024L;
    }

    public FileAccess getFileAccess() {
        return fileAccess;
    }

    public boolean isFileDatabase() {
        return propIsFileDatabase;
    }

    public String getTempDirectoryPath() {
        return tempDirectoryPath;
    }

    static void checkPower(int n, int max) {

        if (!ArrayUtil.isTwoPower(n, max)) {
            throw Error.error(ErrorCode.X_42556);
        }
    }

    public void setCheckpointRequired() {
        checkpointState.compareAndSet(stateCheckpointNormal,
                                      stateCheckpointRequired);
    }

    public boolean needsCheckpointReset() {
        return checkpointState.compareAndSet(stateCheckpointRequired,
                                             stateCheckpointDue);
    }

    public boolean hasLockFile() {
        return lockFile != null;
    }

    public void acquireLock(String path) {

        if (lockFile != null) {
            return;
        }

        lockFile = LockFile.newLockFileLock(path);
    }

    public void releaseLock() {

        try {
            if (lockFile != null) {
                lockFile.tryRelease();
            }
        } catch (Exception e) {}

        lockFile = null;
    }

    public PersistentStore newStore(Session session,
                                    PersistentStoreCollection collection,
                                    TableBase table) {

        switch (table.getTableType()) {

            case TableBase.CACHED_TABLE :
                DataFileCache cache = getCache();

                if (cache == null) {
                    break;
                }

                return new RowStoreAVLDisk(cache, (Table) table);

            case TableBase.MEMORY_TABLE :
            case TableBase.SYSTEM_TABLE :
                return new RowStoreAVLMemory((Table) table);

            case TableBase.TEXT_TABLE :
                return new RowStoreAVLDiskData((Table) table);

            case TableBase.INFO_SCHEMA_TABLE :
                return new RowStoreAVLHybridExtended(session, table, false);

            case TableBase.TEMP_TABLE :
                return new RowStoreAVLHybridExtended(session, table, true);

            case TableBase.CHANGE_SET_TABLE :
                return new RowStoreDataChange(session, table);

            case TableBase.FUNCTION_TABLE :
            case TableBase.RESULT_TABLE :
            case TableBase.SYSTEM_SUBQUERY :
            case TableBase.VIEW_TABLE :
            case TableBase.TRANSITION_TABLE :
                if (session == null) {
                    return null;
                }

                return new RowStoreAVLHybrid(session, table, true);
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "Logger");
    }

    public Index newIndex(HsqlName name, long id, TableBase table,
                          int[] columns, boolean[] descending,
                          boolean[] nullsLast, Type[] colTypes, boolean pk,
                          boolean unique, boolean constraint,
                          boolean forward) {

        switch (table.getTableType()) {

            case TableBase.INFO_SCHEMA_TABLE :
            case TableBase.SYSTEM_TABLE :
            case TableBase.MEMORY_TABLE :
                return new IndexAVLMemory(name, id, table, columns,
                                          descending, nullsLast, colTypes, pk,
                                          unique, constraint, forward);

            case TableBase.CACHED_TABLE :
            case TableBase.CHANGE_SET_TABLE :
            case TableBase.FUNCTION_TABLE :
            case TableBase.TEXT_TABLE :
            case TableBase.TEMP_TABLE :
            case TableBase.RESULT_TABLE :
            case TableBase.SYSTEM_SUBQUERY :
            case TableBase.VIEW_TABLE :
            case TableBase.TRANSITION_TABLE :
                return new IndexAVL(name, id, table, columns, descending,
                                    nullsLast, colTypes, pk, unique,
                                    constraint, forward);
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "Logger");
    }

    public HashMap getPropertyValueMap(Session session) {

        HashMap map   = new HashMap();
        String  value = null;

        map.put(HsqlDatabaseProperties.sql_avg_scale,
                String.valueOf(database.sqlAvgScale));
        map.put(HsqlDatabaseProperties.sql_char_literal,
                String.valueOf(database.sqlCharLiteral));
        map.put(HsqlDatabaseProperties.sql_concat_nulls,
                String.valueOf(database.sqlConcatNulls));
        map.put(HsqlDatabaseProperties.sql_convert_trunc,
                String.valueOf(database.sqlConvertTruncate));
        map.put(HsqlDatabaseProperties.sql_default_collation,
                String.valueOf(database.collation.getName().name));
        map.put(HsqlDatabaseProperties.sql_trunc_trailing,
                String.valueOf(database.sqlTruncateTrailing));
        map.put(HsqlDatabaseProperties.sql_double_nan,
                String.valueOf(database.sqlDoubleNaN));
        map.put(HsqlDatabaseProperties.sql_enforce_names,
                String.valueOf(database.sqlEnforceNames));
        map.put(HsqlDatabaseProperties.sql_enforce_refs,
                String.valueOf(database.sqlEnforceRefs));
        map.put(HsqlDatabaseProperties.sql_enforce_size,
                String.valueOf(database.sqlEnforceSize));
        map.put(HsqlDatabaseProperties.sql_enforce_tdcd,
                String.valueOf(database.sqlEnforceTDCD));
        map.put(HsqlDatabaseProperties.sql_enforce_tdcu,
                String.valueOf(database.sqlEnforceTDCU));
        map.put(HsqlDatabaseProperties.sql_enforce_types,
                String.valueOf(database.sqlEnforceTypes));
        map.put(HsqlDatabaseProperties.sql_ignore_case,
                String.valueOf(database.sqlIgnoreCase));
        map.put(HsqlDatabaseProperties.sql_live_object,
                String.valueOf(database.sqlLiveObject));
        map.put(HsqlDatabaseProperties.sql_longvar_is_lob,
                String.valueOf(database.sqlLongvarIsLob));
        map.put(HsqlDatabaseProperties.sql_lowercase_ident,
                String.valueOf(database.sqlLowerCaseIdentifier));
        map.put(HsqlDatabaseProperties.sql_max_recursive,
                String.valueOf(database.sqlMaxRecursive));
        map.put(HsqlDatabaseProperties.sql_nulls_first,
                String.valueOf(database.sqlNullsFirst));
        map.put(HsqlDatabaseProperties.sql_nulls_order,
                String.valueOf(database.sqlNullsOrder));
        map.put(HsqlDatabaseProperties.sql_pad_space,
                String.valueOf(database.collation.isPadSpace()));
        map.put(HsqlDatabaseProperties.sql_ref_integrity,
                String.valueOf(database.isReferentialIntegrity()));
        map.put(HsqlDatabaseProperties.sql_regular_names,
                String.valueOf(database.sqlRegularNames));
        map.put(HsqlDatabaseProperties.sql_restrict_exec,
                String.valueOf(database.sqlRestrictExec));
        map.put(HsqlDatabaseProperties.sql_syntax_db2,
                String.valueOf(database.sqlSyntaxDb2));
        map.put(HsqlDatabaseProperties.sql_syntax_mss,
                String.valueOf(database.sqlSyntaxMss));
        map.put(HsqlDatabaseProperties.sql_syntax_mys,
                String.valueOf(database.sqlSyntaxMys));
        map.put(HsqlDatabaseProperties.sql_syntax_ora,
                String.valueOf(database.sqlSyntaxOra));
        map.put(HsqlDatabaseProperties.sql_syntax_pgs,
                String.valueOf(database.sqlSyntaxPgs));
        map.put(HsqlDatabaseProperties.sql_sys_index_names,
                String.valueOf(database.sqlSysIndexNames));
        map.put(HsqlDatabaseProperties.sql_unique_nulls,
                String.valueOf(database.sqlUniqueNulls));

        //
        map.put(HsqlDatabaseProperties.jdbc_translate_tti_types,
                String.valueOf(database.sqlTranslateTTI));

        switch (database.txManager.getTransactionControl()) {

            case TransactionManager.MVCC :
                value = Tokens.T_MVCC;
                break;

            case TransactionManager.MVLOCKS :
                value = Tokens.T_MVLOCKS;
                break;

            case TransactionManager.LOCKS :
                value = Tokens.T_LOCKS;
                break;
        }

        map.put(HsqlDatabaseProperties.hsqldb_tx, value);

        switch (database.defaultIsolationLevel) {

            case SessionInterface.TX_READ_COMMITTED :
                value = new StringBuilder(Tokens.T_READ).append('_').append(
                    Tokens.T_COMMITTED).toString();
                break;

            case SessionInterface.TX_SERIALIZABLE :
                value = Tokens.T_SERIALIZABLE;
                break;
        }

        map.put(HsqlDatabaseProperties.hsqldb_tx_level, value);
        map.put(
            HsqlDatabaseProperties.hsqldb_reconfig_logging,
            System.getProperty(
                HsqlDatabaseProperties.hsqldb_reconfig_logging));

        if (HsqlDatabaseProperties.methodClassNames != null) {
            map.put(HsqlDatabaseProperties.hsqldb_method_class_names,
                    HsqlDatabaseProperties.methodClassNames);
        }

        map.put(HsqlDatabaseProperties.hsqldb_applog,
                String.valueOf(appLog.getLevel()));
        map.put(HsqlDatabaseProperties.hsqldb_extlog,
                String.valueOf(propExternalEventLogLevel));
        map.put(HsqlDatabaseProperties.hsqldb_sqllog,
                String.valueOf(sqlLog.getLevel()));
        map.put(HsqlDatabaseProperties.hsqldb_lob_file_scale,
                String.valueOf(propLobBlockSize / 1024));
        map.put(HsqlDatabaseProperties.hsqldb_lob_file_compressed,
                String.valueOf(propCompressLobs));
        map.put(HsqlDatabaseProperties.hsqldb_cache_file_scale,
                String.valueOf(propDataFileScale));
        map.put(HsqlDatabaseProperties.hsqldb_cache_free_count,
                String.valueOf(propMaxFreeBlocks));
        map.put(HsqlDatabaseProperties.hsqldb_cache_rows,
                String.valueOf(propCacheMaxRows));
        map.put(HsqlDatabaseProperties.hsqldb_cache_size,
                String.valueOf(propCacheMaxSize / 1024));

        {
            String prop;

            switch (database.schemaManager.getDefaultTableType()) {

                case TableBase.CACHED_TABLE :
                    prop = Tokens.T_CACHED;
                    break;

                case TableBase.MEMORY_TABLE :
                default :
                    prop = Tokens.T_MEMORY;
            }

            map.put(HsqlDatabaseProperties.hsqldb_default_table_type, prop);
        }

        map.put(HsqlDatabaseProperties.hsqldb_defrag_limit,
                String.valueOf(propDataFileDefragLimit));
        map.put(HsqlDatabaseProperties.hsqldb_files_space,
                String.valueOf(propDataFileSpace));
        map.put(
            HsqlDatabaseProperties.hsqldb_files_readonly,
            database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.hsqldb_files_readonly) ? "true"
                                                              : "false");
        map.put(HsqlDatabaseProperties.hsqldb_large_data,
                String.valueOf(propLargeData));
        map.put(
            HsqlDatabaseProperties.hsqldb_lock_file,
            database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.hsqldb_lock_file) ? "true"
                                                         : "false");
        map.put(HsqlDatabaseProperties.hsqldb_log_data,
                String.valueOf(propLogData));
        map.put(HsqlDatabaseProperties.hsqldb_log_size,
                String.valueOf(propLogSize));
        map.put(HsqlDatabaseProperties.hsqldb_nio_data_file,
                String.valueOf(propNioDataFile));
        map.put(HsqlDatabaseProperties.hsqldb_nio_max_size,
                String.valueOf(propNioMaxSize / (1024 * 1024)));
        map.put(HsqlDatabaseProperties.hsqldb_script_format,
                ScriptWriterBase.LIST_SCRIPT_FORMATS[propScriptFormat]
                    .toLowerCase());
        map.put(HsqlDatabaseProperties.hsqldb_temp_directory,
                tempDirectoryPath);
        map.put(HsqlDatabaseProperties.hsqldb_tx_conflict_rollback,
                String.valueOf(database.txConflictRollback));
        map.put(HsqlDatabaseProperties.hsqldb_tx_interrupt_rollback,
                String.valueOf(database.txInterruptRollback));
        map.put(HsqlDatabaseProperties.hsqldb_result_max_memory_rows,
                String.valueOf(database.getResultMaxMemoryRows()));
        map.put(HsqlDatabaseProperties.hsqldb_readonly,
                database.isReadOnly() ? "true"
                                      : "false");
        map.put(HsqlDatabaseProperties.hsqldb_files_readonly,
                database.isFilesReadOnly() ? "true"
                                           : "false");
        map.put(HsqlDatabaseProperties.hsqldb_write_delay,
                String.valueOf(propWriteDelay != 0));
        map.put(HsqlDatabaseProperties.hsqldb_write_delay_millis,
                String.valueOf(propWriteDelay));
        map.put(HsqlDatabaseProperties.hsqldb_digest,
                database.granteeManager.getDigestAlgo());

        return map;
    }

    public String[] getPropertiesSQL(boolean indexRoots) {

        HsqlArrayList list = new HsqlArrayList();
        StringBuilder sb   = new StringBuilder();

        sb.append("SET DATABASE ").append(Tokens.T_UNIQUE).append(' ');
        sb.append(Tokens.T_NAME).append(' ').append(database.getNameString());
        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET DATABASE ").append(Tokens.T_DEFAULT).append(' ');
        sb.append(Tokens.T_RESULT).append(' ').append(Tokens.T_MEMORY);
        sb.append(' ').append(Tokens.T_ROWS).append(' ');
        sb.append(database.getResultMaxMemoryRows());
        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET DATABASE ").append(Tokens.T_EVENT).append(' ');
        sb.append(Tokens.T_LOG).append(' ').append(Tokens.T_LEVEL);
        sb.append(' ').append(propEventLogLevel);
        list.add(sb.toString());
        sb.setLength(0);

        if (propExternalEventLogLevel != SimpleLog.LOG_NONE) {
            sb.append("SET DATABASE ").append(Tokens.T_EXTERNAL).append(' ');
            sb.append(Tokens.T_EVENT).append(' ');
            sb.append(Tokens.T_LOG).append(' ').append(Tokens.T_LEVEL);
            sb.append(' ').append(propExternalEventLogLevel);
            list.add(sb.toString());
            sb.setLength(0);
        }

        if (propSqlLogLevel != SimpleLog.LOG_NONE) {
            sb.append("SET DATABASE ").append(Tokens.T_EVENT).append(' ');
            sb.append(Tokens.T_LOG).append(' ').append(Tokens.T_SQL);
            sb.append(' ').append(Tokens.T_LEVEL);
            sb.append(' ').append(propEventLogLevel);
            list.add(sb.toString());
            sb.setLength(0);
        }

        sb.append("SET DATABASE ").append(Tokens.T_TRANSACTION);
        sb.append(' ').append(Tokens.T_CONTROL).append(' ');

        switch (database.txManager.getTransactionControl()) {

            case TransactionManager.MVCC :
                sb.append(Tokens.T_MVCC);
                break;

            case TransactionManager.MVLOCKS :
                sb.append(Tokens.T_MVLOCKS);
                break;

            case TransactionManager.LOCKS :
                sb.append(Tokens.T_LOCKS);
                break;
        }

        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET DATABASE ").append(Tokens.T_DEFAULT).append(' ');
        sb.append(Tokens.T_ISOLATION).append(' ').append(Tokens.T_LEVEL);
        sb.append(' ');

        switch (database.defaultIsolationLevel) {

            case SessionInterface.TX_READ_COMMITTED :
                sb.append(Tokens.T_READ).append(' ').append(
                    Tokens.T_COMMITTED);
                break;

            case SessionInterface.TX_SERIALIZABLE :
                sb.append(Tokens.T_SERIALIZABLE);
                break;
        }

        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET DATABASE ").append(Tokens.T_TRANSACTION).append(' ');
        sb.append(Tokens.T_ROLLBACK).append(' ');
        sb.append(Tokens.T_ON).append(' ');
        sb.append(Tokens.T_CONFLICT).append(' ');
        sb.append(database.txConflictRollback ? Tokens.T_TRUE
                                              : Tokens.T_FALSE);
        list.add(sb.toString());
        sb.setLength(0);

        if (database.txInterruptRollback) {
            sb.append("SET DATABASE ").append(Tokens.T_TRANSACTION);
            sb.append(' ').append(Tokens.T_ROLLBACK).append(' ');
            sb.append(Tokens.T_ON).append(' ');
            sb.append(Tokens.T_INTERRUPT).append(' ');
            sb.append(database.txInterruptRollback ? Tokens.T_TRUE
                                                   : Tokens.T_FALSE);
            list.add(sb.toString());
            sb.setLength(0);
        }

        sb.append("SET DATABASE ").append(Tokens.T_TEXT).append(' ');
        sb.append(Tokens.T_TABLE).append(' ').append(Tokens.T_DEFAULTS);
        sb.append(' ').append('\'');
        sb.append(propTextSourceDefault).append('\'');
        list.add(sb.toString());
        sb.setLength(0);

        String temp = HsqlDatabaseProperties.getStringPropertyDefault(
            HsqlDatabaseProperties.hsqldb_digest);

        if (!temp.equals(database.granteeManager.getDigestAlgo())) {
            sb.append("SET DATABASE ").append(Tokens.T_PASSWORD).append(' ');
            sb.append(Tokens.T_DIGEST).append(' ').append('\'');
            sb.append(database.granteeManager.getDigestAlgo()).append('\'');
            list.add(sb.toString());
            sb.setLength(0);
        }

        if (database.schemaManager.getDefaultTableType()
                == TableBase.CACHED_TABLE) {
            sb.append("SET DATABASE ").append(Tokens.T_DEFAULT).append(' ');
            sb.append(Tokens.T_TABLE).append(' ');
            sb.append(Tokens.T_TYPE).append(' ');
            sb.append(Tokens.T_CACHED);
            list.add(sb.toString());
            sb.setLength(0);
        }

        //
        sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
        sb.append(Tokens.T_NAMES).append(' ');
        sb.append(database.sqlEnforceNames ? Tokens.T_TRUE
                                           : Tokens.T_FALSE);
        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
        sb.append(Tokens.T_RESTRICT).append(' ');
        sb.append(Tokens.T_EXEC).append(' ');
        sb.append(database.sqlRestrictExec ? Tokens.T_TRUE
                                           : Tokens.T_FALSE);
        list.add(sb.toString());
        sb.setLength(0);

        if (!database.sqlRegularNames) {
            sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
            sb.append(Tokens.T_REGULAR).append(' ');
            sb.append(Tokens.T_NAMES).append(' ');
            sb.append(database.sqlRegularNames ? Tokens.T_TRUE
                                               : Tokens.T_FALSE);
            list.add(sb.toString());
            sb.setLength(0);
        }

        sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
        sb.append(Tokens.T_REFERENCES).append(' ');
        sb.append(database.sqlEnforceRefs ? Tokens.T_TRUE
                                          : Tokens.T_FALSE);
        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
        sb.append(Tokens.T_SIZE).append(' ');
        sb.append(database.sqlEnforceSize ? Tokens.T_TRUE
                                          : Tokens.T_FALSE);
        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
        sb.append(Tokens.T_TYPES).append(' ');
        sb.append(database.sqlEnforceTypes ? Tokens.T_TRUE
                                           : Tokens.T_FALSE);
        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
        sb.append(Tokens.T_TDC).append(' ');
        sb.append(Tokens.T_DELETE).append(' ');
        sb.append(database.sqlEnforceTDCD ? Tokens.T_TRUE
                                          : Tokens.T_FALSE);
        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
        sb.append(Tokens.T_TDC).append(' ');
        sb.append(Tokens.T_UPDATE).append(' ');
        sb.append(database.sqlEnforceTDCU ? Tokens.T_TRUE
                                          : Tokens.T_FALSE);
        list.add(sb.toString());

        if (!database.sqlTranslateTTI) {
            sb.setLength(0);
            sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
            sb.append(Tokens.T_TRANSLATE).append(' ').append(Tokens.T_TTI);
            sb.append(' ').append(Tokens.T_TYPES).append(' ');
            sb.append(database.sqlTranslateTTI ? Tokens.T_TRUE
                                               : Tokens.T_FALSE);
            list.add(sb.toString());
        }

        sb.setLength(0);
        sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
        sb.append(Tokens.T_SYS).append(' ').append(Tokens.T_INDEX);
        sb.append(' ').append(Tokens.T_NAMES).append(' ');
        sb.append(database.sqlSysIndexNames ? Tokens.T_TRUE
                                            : Tokens.T_FALSE);
        list.add(sb.toString());

        if (!database.sqlCharLiteral) {
            sb.setLength(0);
            sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
            sb.append(Tokens.T_CHARACTER).append(' ');
            sb.append(Tokens.T_LITERAL).append(' ');
            sb.append(database.sqlCharLiteral ? Tokens.T_TRUE
                                              : Tokens.T_FALSE);
            list.add(sb.toString());
        }

        sb.setLength(0);
        sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
        sb.append(Tokens.T_CONCAT_WORD).append(' ');
        sb.append(Tokens.T_NULLS).append(' ');
        sb.append(database.sqlConcatNulls ? Tokens.T_TRUE
                                          : Tokens.T_FALSE);
        list.add(sb.toString());

        if (!database.sqlNullsFirst) {
            sb.setLength(0);
            sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
            sb.append(Tokens.T_NULLS).append(' ');
            sb.append(Tokens.T_FIRST).append(' ');
            sb.append(database.sqlNullsFirst ? Tokens.T_TRUE
                                             : Tokens.T_FALSE);
            list.add(sb.toString());
        }

        if (!database.sqlNullsOrder) {
            sb.setLength(0);
            sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
            sb.append(Tokens.T_NULLS).append(' ');
            sb.append(Tokens.T_ORDER).append(' ');
            sb.append(database.sqlNullsOrder ? Tokens.T_TRUE
                                             : Tokens.T_FALSE);
            list.add(sb.toString());
        }

        sb.setLength(0);
        sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
        sb.append(Tokens.T_UNIQUE).append(' ');
        sb.append(Tokens.T_NULLS).append(' ');
        sb.append(database.sqlUniqueNulls ? Tokens.T_TRUE
                                          : Tokens.T_FALSE);
        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
        sb.append(Tokens.T_CONVERT).append(' ');
        sb.append(Tokens.T_TRUNCATE).append(' ');
        sb.append(database.sqlConvertTruncate ? Tokens.T_TRUE
                                              : Tokens.T_FALSE);
        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
        sb.append(Tokens.T_AVG).append(' ');
        sb.append(Tokens.T_SCALE).append(' ');
        sb.append(database.sqlAvgScale);
        list.add(sb.toString());
        sb.setLength(0);

        if (database.sqlMaxRecursive
                != HsqlDatabaseProperties.getIntegerPropertyDefault(
                    HsqlDatabaseProperties.sql_max_recursive)) {
            sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
            sb.append(Tokens.T_MAX).append(' ');
            sb.append(Tokens.T_RECURSIVE).append(' ');
            sb.append(database.sqlMaxRecursive);
            list.add(sb.toString());
            sb.setLength(0);
        }

        sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
        sb.append(Tokens.T_DOUBLE).append(' ');
        sb.append(Tokens.T_NAN).append(' ');
        sb.append(database.sqlDoubleNaN ? Tokens.T_TRUE
                                        : Tokens.T_FALSE);
        list.add(sb.toString());
        sb.setLength(0);

        if (database.sqlIgnoreCase) {
            sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
            sb.append(Tokens.T_IGNORECASE).append(' ');
            sb.append(database.sqlIgnoreCase ? Tokens.T_TRUE
                                             : Tokens.T_FALSE);
            list.add(sb.toString());
            sb.setLength(0);
        }

        if (database.sqlLongvarIsLob) {
            sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
            sb.append(Tokens.T_LONGVAR).append(' ');
            sb.append(Tokens.T_IS).append(' ');
            sb.append(Tokens.T_LOB).append(' ');
            sb.append(database.sqlLongvarIsLob ? Tokens.T_TRUE
                                               : Tokens.T_FALSE);
            list.add(sb.toString());
            sb.setLength(0);
        }

        if (database.sqlLowerCaseIdentifier) {
            sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
            sb.append(Tokens.T_LOWER).append(' ');
            sb.append(Tokens.T_CASE).append(' ');
            sb.append(Tokens.T_IDENTIFIER).append(' ');
            sb.append(database.sqlLowerCaseIdentifier ? Tokens.T_TRUE
                                                      : Tokens.T_FALSE);
            list.add(sb.toString());
            sb.setLength(0);
        }

        if (!database.sqlTruncateTrailing) {
            sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
            sb.append(Tokens.T_TRUNCATE).append(' ');
            sb.append(Tokens.T_TRAILING).append(' ');
            sb.append(database.sqlTruncateTrailing ? Tokens.T_TRUE
                                                   : Tokens.T_FALSE);
            list.add(sb.toString());
            sb.setLength(0);
        }

        if (database.sqlSyntaxDb2) {
            sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
            sb.append(Tokens.T_SYNTAX).append(' ');
            sb.append(Tokens.T_DB2).append(' ');
            sb.append(database.sqlSyntaxDb2 ? Tokens.T_TRUE
                                            : Tokens.T_FALSE);
            list.add(sb.toString());
            sb.setLength(0);
        }

        if (database.sqlSyntaxMss) {
            sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
            sb.append(Tokens.T_SYNTAX).append(' ');
            sb.append(Tokens.T_MSS).append(' ');
            sb.append(database.sqlSyntaxMss ? Tokens.T_TRUE
                                            : Tokens.T_FALSE);
            list.add(sb.toString());
            sb.setLength(0);
        }

        if (database.sqlSyntaxMys) {
            sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
            sb.append(Tokens.T_SYNTAX).append(' ');
            sb.append(Tokens.T_MYS).append(' ');
            sb.append(database.sqlSyntaxMys ? Tokens.T_TRUE
                                            : Tokens.T_FALSE);
            list.add(sb.toString());
            sb.setLength(0);
        }

        if (database.sqlSyntaxOra) {
            sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
            sb.append(Tokens.T_SYNTAX).append(' ');
            sb.append(Tokens.T_ORA).append(' ');
            sb.append(database.sqlSyntaxOra ? Tokens.T_TRUE
                                            : Tokens.T_FALSE);
            list.add(sb.toString());
            sb.setLength(0);
        }

        if (database.sqlSyntaxPgs) {
            sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
            sb.append(Tokens.T_SYNTAX).append(' ');
            sb.append(Tokens.T_PGS).append(' ');
            sb.append(database.sqlSyntaxPgs ? Tokens.T_TRUE
                                            : Tokens.T_FALSE);
            list.add(sb.toString());
            sb.setLength(0);
        }

        //
        int     delay  = propWriteDelay;
        boolean millis = delay > 0 && delay < 1000;

        if (millis) {
            if (delay < 20) {
                delay = 20;
            }
        } else {
            delay /= 1000;
        }

        sb.append("SET FILES ").append(Tokens.T_WRITE).append(' ');
        sb.append(Tokens.T_DELAY).append(' ').append(delay);

        if (millis) {
            sb.append(' ').append(Tokens.T_MILLIS);
        }

        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET FILES ").append(Tokens.T_BACKUP);
        sb.append(' ').append(Tokens.T_INCREMENT).append(' ');
        sb.append(Tokens.T_TRUE);
        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET FILES ").append(Tokens.T_CACHE);
        sb.append(' ').append(Tokens.T_SIZE).append(' ');
        sb.append(propCacheMaxSize / 1024);
        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET FILES ").append(Tokens.T_CACHE);
        sb.append(' ').append(Tokens.T_ROWS).append(' ');
        sb.append(propCacheMaxRows);
        list.add(sb.toString());

        {
            int fileScale = propDataFileScale;

            if (!indexRoots && fileScale < 32) {
                fileScale = 32;
            }

            sb.setLength(0);
            sb.append("SET FILES ").append(Tokens.T_SCALE);
            sb.append(' ').append(fileScale);
            list.add(sb.toString());
            sb.setLength(0);
        }

        if (propDataFileSpace != 0) {
            sb.append("SET FILES ").append(Tokens.T_SPACE).append(' ');
            sb.append(propDataFileSpace);
            list.add(sb.toString());
            sb.setLength(0);
        }

        sb.append("SET FILES ").append(Tokens.T_LOB).append(' ');
        sb.append(Tokens.T_SCALE);
        sb.append(' ').append(getLobFileScale());
        list.add(sb.toString());
        sb.setLength(0);

        if (propCompressLobs) {
            sb.append("SET FILES ").append(Tokens.T_LOB).append(' ');
            sb.append(Tokens.T_COMPRESSED).append(' ');
            sb.append(propCompressLobs ? Tokens.T_TRUE
                                       : Tokens.T_FALSE);
            list.add(sb.toString());
            sb.setLength(0);
        }

        sb.append("SET FILES ").append(Tokens.T_DEFRAG).append(' ');
        sb.append(propDataFileDefragLimit);
        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET FILES ").append(Tokens.T_NIO).append(' ');
        sb.append(propNioDataFile ? Tokens.T_TRUE
                                  : Tokens.T_FALSE);
        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET FILES ").append(Tokens.T_NIO).append(' ');
        sb.append(Tokens.T_SIZE).append(' ');
        sb.append(propNioMaxSize / (1024 * 1024));
        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET FILES ").append(Tokens.T_LOG).append(' ');
        sb.append(propLogData ? Tokens.T_TRUE
                              : Tokens.T_FALSE);
        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET FILES ").append(Tokens.T_LOG).append(' ');
        sb.append(Tokens.T_SIZE).append(' ').append(propLogSize);
        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET FILES ").append(Tokens.T_CHECK).append(' ');
        sb.append(propFilesTimestamp);
        list.add(sb.toString());
        sb.setLength(0);

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }

    public void backup(String destPath, boolean script, boolean blocking,
                       boolean compressed, boolean files) {

        if (!backupState.compareAndSet(stateNormal, stateBackup)) {
            throw Error.error(ErrorCode.BACKUP_ERROR, "backup in progress");
        }

        if (blocking) {
            database.lobManager.lock();

            try {
                synchronized (this) {
                    backupInternal(destPath, script, blocking, compressed,
                                   files);
                }
            } finally {
                backupState.set(stateNormal);
                database.lobManager.unlock();
            }
        } else {
            try {
                backupInternal(destPath, script, blocking, compressed, files);
            } finally {
                backupState.set(stateNormal);
            }
        }
    }

    public SimpleDateFormat fileDateFormat =
        new SimpleDateFormat("yyyyMMdd'T'HHmmss");
    private static char runtimeFileDelim =
        System.getProperty("file.separator").charAt(0);
    DbBackup backup;

    void backupInternal(String destPath, boolean script, boolean blocking,
                        boolean compressed, boolean asFiles) {

        String scriptName = null;
        String dbPath     = database.getPath();
        /* If want to add db Id also, will need to pass either Database
         * instead of dbPath, or pass dbPath + Id from StatementCommand.
         */
        String instanceName = new File(dbPath).getName();
        char   lastChar     = destPath.charAt(destPath.length() - 1);
        boolean generateName = (lastChar == '/'
                                || lastChar == runtimeFileDelim);
        File archiveFile;

        if (asFiles) {
            if (!generateName) {
                throw Error.error(null, ErrorCode.UNSUPPORTED_FILENAME_SUFFIX,
                                  0, new String[] {
                    "", "/"
                });
            }

            destPath = getSecurePath(destPath, true, false);

            if (destPath == null) {
                throw Error.error(ErrorCode.BACKUP_ERROR,
                                  "access to directory denied");
            }

            archiveFile = new File(destPath);

            archiveFile.mkdirs();

            File[] files = FileUtil.getDatabaseMainFileList(destPath
                + instanceName);

            if (files == null || files.length != 0) {
                throw Error.error(ErrorCode.BACKUP_ERROR,
                                  "files exist in directory");
            }
        } else {
            String defaultSuffix = compressed ? ".tar.gz"
                                              : ".tar";

            if (generateName) {
                archiveFile =
                    (new File(destPath.substring(0, destPath.length() - 1),
                              instanceName + '-'
                              + fileDateFormat.format(new java.util.Date())
                              + defaultSuffix));
            } else {
                archiveFile = new File(destPath);
            }

            boolean nameImpliesCompress =
                archiveFile.getName().endsWith(".tar.gz")
                || archiveFile.getName().endsWith(".tgz");

            if ((!nameImpliesCompress)
                    && !archiveFile.getName().endsWith(".tar")) {
                throw Error.error(null, ErrorCode.UNSUPPORTED_FILENAME_SUFFIX,
                                  0, new String[] {
                    archiveFile.getName(), ".tar, .tar.gz, .tgz"
                });
            }

            if (compressed != nameImpliesCompress) {
                throw Error.error(null, ErrorCode.COMPRESSION_SUFFIX_MISMATCH,
                                  0, new String[] {
                    String.valueOf(compressed), archiveFile.getName()
                });
            }

            if (archiveFile.exists()) {
                throw Error.error(ErrorCode.BACKUP_ERROR,
                                  "file exists :" + archiveFile.getName());
            }
        }

        if (blocking) {
            log.checkpointClose();
        }

        try {
            logInfoEvent("Initiating backup of instance '" + instanceName
                         + "'");

            // By default, DbBackup will throw if archiveFile (or
            // corresponding work file) already exist.  That's just what we
            // want here.
            if (script) {
                String path = getTempDirectoryPath();

                if (path == null) {
                    return;
                }

                path = path + "/" + new File(database.getPath()).getName();
                scriptName = path + scriptFileExtension;

                ScriptWriterText dsw = new ScriptWriterText(database,
                    scriptName, true, true, true);

                dsw.writeAll();
                dsw.close();

                backup = new DbBackup(archiveFile, path, true);

                backup.write();
            } else {
                backup = new DbBackup(archiveFile, dbPath);

                backup.setAbortUponModify(false);

                if (!blocking) {
                    InputStreamWrapper isw;
                    File               file = null;

                    if (hasCache()) {
                        DataFileCache dataFileCache = getCache();
                        RAShadowFile shadowFile =
                            dataFileCache.getShadowFile();

                        file = new File(dataFileCache.dataFileName);
                        isw = new InputStreamWrapper(
                            new FileInputStream(file));

                        isw.setSizeLimit(dataFileCache.fileStartFreePosition);
                        backup.setStream(dataFileExtension, isw);

                        InputStreamInterface isi = shadowFile.getInputStream();

                        backup.setStream(backupFileExtension, isi);
                    }

                    // log
                    file = new File(log.getLogFileName());

                    long fileLength = file.length();

                    if (fileLength == 0) {
                        backup.setFileIgnore(logFileExtension);
                    } else {
                        isw = new InputStreamWrapper(
                            new FileInputStream(file));

                        isw.setSizeLimit(fileLength);
                        backup.setStream(logFileExtension, isw);
                    }
                }

                if (asFiles) {
                    backup.writeAsFiles();
                } else {
                    backup.write();
                }
            }

            logInfoEvent("Successfully backed up instance '" + instanceName
                         + "' to '" + destPath + "'");
        } catch (IOException ioe) {
            throw Error.error(ioe, ErrorCode.FILE_IO_ERROR, ioe.toString());
        } catch (TarMalformatException tme) {
            throw Error.error(tme, ErrorCode.FILE_IO_ERROR, tme.toString());
        } finally {
            if (scriptName != null) {
                FileUtil.getFileUtil().delete(scriptName);
            }

            if (blocking) {
                log.checkpointReopen();
            }
        }
    }

    /**
     *  Returns a secure path or null for a user-defined path when
     *  hsqldb.allow_full_path is false. Returns the path otherwise.
     *
     */
    public String getSecurePath(String path, boolean allowFull,
                                boolean includeRes) {

        if (database.getType() == DatabaseType.DB_RES) {
            if (includeRes) {
                return path;
            } else {
                return null;
            }
        }

        if (database.getType() == DatabaseType.DB_MEM) {
            if (propTextAllowFullPath) {
                return path;
            } else {
                return null;
            }
        }

        // absolute paths
        if (path.startsWith("/") || path.startsWith("\\")
                || path.contains(":")) {
            if (allowFull || propTextAllowFullPath) {
                return path;
            } else {
                return null;
            }
        }

        if (path.contains("..")) {
            if (allowFull || propTextAllowFullPath) {

                // allow
            } else {
                return null;
            }
        }

        String fullPath =
            new File(new File(database.getPath()
                              + ".properties").getAbsolutePath()).getParent();

        if (fullPath != null) {
            path = fullPath + File.separator + path;
        }

        return path;
    }

    public boolean isNewDatabase() {
        return isNewDatabase;
    }
}
