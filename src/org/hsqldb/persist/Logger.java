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


package org.hsqldb.persist;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
import java.util.Locale;

import org.hsqldb.Database;
import org.hsqldb.DatabaseURL;
import org.hsqldb.HsqlNameManager;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.NumberSequence;
import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.SessionInterface;
import org.hsqldb.SqlInvariants;
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
import org.hsqldb.lib.FileAccess;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.FrameworkLogger;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.SimpleLog;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.lib.tar.DbBackup;
import org.hsqldb.lib.tar.TarMalformatException;
import org.hsqldb.scriptio.ScriptWriterBase;
import org.hsqldb.types.Type;

// boucherb@users 20030510 - patch 1.7.2 - added cooperative file locking

/**
 *  The public interface of persistence and logging classes.<p>
 *
 *  Implements a storage manager wrapper that provides a consistent,
 *  always available interface to storage management for the Database
 *  class, despite the fact not all Database objects actually use file
 *  storage.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 1.7.0
 */
public class Logger {

    public SimpleLog appLog;
    private Database database;
    public boolean   checkpointRequired;
    public boolean   checkpointDue;
    public boolean   checkpointDisabled;
    private boolean  logsStatements;    // false indicates Log is being opened
    private boolean  loggingEnabled;
    private boolean  syncFile = false;

    //
    boolean        propIsFileDatabase;
    boolean        propFilesReadOnly;
    boolean        propDatabaseReadOnly;
    boolean        propIncrementBackup;
    boolean        propNioDataFile;
    long           propNioMaxSize = 256 * 1024 * 1024L;
    int            propMaxFreeBlocks;
    int            propCacheMaxRows;
    int            propCacheMaxSize;
    int            propCacheFileScale;
    int            propCacheDefragLimit;
    String         propTextSourceDefault = "";
    HsqlProperties propTextSourceProps;
    boolean        propTextAllowFullPath;
    int            propWriteDelay;
    int            propLogSize;
    boolean        propLogData = true;
    int            propEventLogLevel;
    int            propGC;
    int            propTxMode       = TransactionManager.LOCKS;
    boolean        propRefIntegrity = true;
    int            propLobBlockSize = 32 * 1024;
    int            propScriptFormat = 0;

    //
    Log               log;
    private LockFile  lockFile;
    private Crypto    crypto;
    public FileAccess fileAccess;
    public boolean    isStoredFileAccess;
    String            tempDirectoryPath;

    //
    public boolean isNewDatabase;

    //
    public boolean isSingleFile;

    //
    public final static String oldFileExtension        = ".old";
    public final static String newFileExtension        = ".new";
    public final static String appLogFileExtension     = ".app.log";
    public final static String logFileExtension        = ".log";
    public final static String scriptFileExtension     = ".script";
    public final static String propertiesFileExtension = ".properties";
    public final static String dataFileExtension       = ".data";
    public final static String backupFileExtension     = ".backup";
    public final static String lobsFileExtension       = ".lobs";
    public final static String lockFileExtension       = ".lck";

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
     * @param  db the Database
     * @throws  HsqlException if there is a problem, such as the case when
     *      the specified files are in use by another process
     */
    public void openPersistence() {

        // oj@openoffice.org - changed to file access api
        String fileaccess_class_name =
            (String) database.getURLProperties().getProperty(
                HsqlDatabaseProperties.url_fileaccess_class_name);

        if (fileaccess_class_name != null) {
            String storagekey = database.getURLProperties().getProperty(
                HsqlDatabaseProperties.url_storage_key);

            try {
                Class zclass = Class.forName(fileaccess_class_name);
                Constructor constructor = zclass.getConstructor(new Class[]{
                    Object.class });

                fileAccess =
                    (FileAccess) constructor.newInstance(new Object[]{
                        storagekey });
                isStoredFileAccess = true;
            } catch (java.lang.ClassNotFoundException e) {
                System.out.println("ClassNotFoundException");
            } catch (java.lang.InstantiationException e) {
                System.out.println("InstantiationException");
            } catch (java.lang.IllegalAccessException e) {
                System.out.println("IllegalAccessException");
            } catch (Exception e) {
                System.out.println("Exception");
            }
        } else {
            fileAccess = FileUtil.getFileAccess(database.isFilesInJar());
        }

        propIsFileDatabase =
            DatabaseURL.isFileBasedDatabaseType(database.getType());
        database.databaseProperties = new HsqlDatabaseProperties(database);

        if (propIsFileDatabase) {
            boolean exists = fileAccess.isStreamElement(database.getPath()
                + scriptFileExtension);

            if (!exists) {
                exists = fileAccess.isStreamElement(database.getPath()
                                                    + scriptFileExtension
                                                    + Logger.newFileExtension);

                if (exists) {
                    database.databaseProperties.setDBModified(
                        HsqlDatabaseProperties.FILES_MODIFIED_NEW);
                }
            }

            isNewDatabase = !exists;
        } else {
            isNewDatabase = true;
        }

        if (isNewDatabase) {
            String name = newUniqueName();

            database.setUniqueName(name);

            boolean checkExists =
                database
                    .isFilesInJar() || (database.urlProperties
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
            boolean props = database.databaseProperties.load();

            if (!props) {
                database.databaseProperties.setDBModified(
                    HsqlDatabaseProperties.FILES_MODIFIED);
            }

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
        }

        setVariables();

        String logPath = null;

        if (DatabaseURL.isFileBasedDatabaseType(database.getType())
                && !database.isFilesReadOnly()) {
            logPath = database.getPath() + appLogFileExtension;
        }

        this.appLog = new SimpleLog(logPath, propEventLogLevel);

        database.setReferentialIntegrity(propRefIntegrity);

        if (!isFileDatabase()) {
            return;
        }

        checkpointRequired = false;
        logsStatements     = false;

        boolean useLock = database.getProperties().isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_lock_file);

        if (useLock && !database.isFilesReadOnly()) {
            acquireLock(database.getPath());
        }

        log = new Log(database);

        log.open();

        logsStatements = true;
        loggingEnabled = propLogData && !database.isFilesReadOnly();

        boolean version18 = database.databaseProperties.isVersion18();

        if (version18) {
            HsqlName name = database.schemaManager.findSchemaHsqlName(
                SqlInvariants.PUBLIC_SCHEMA);

            if (name != null) {
                database.schemaManager.setDefaultSchemaHsqlName(name);
            }

            database.setUniqueName(newUniqueName());
            checkpoint(false);
        }

        if (database.getUniqueName() == null) {
            database.setUniqueName(newUniqueName());
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

            crypto = new Crypto(cryptKey, cryptType, cryptProvider);
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
            if (database.getType() == DatabaseURL.S_MEM
                    || isStoredFileAccess) {
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

        boolean version18 = database.databaseProperties.isVersion18();

        if (!isNewDatabase && !version18) {
            return;
        }

        if (tempDirectoryPath != null) {
            int rows = database.databaseProperties.getIntegerProperty(
                HsqlDatabaseProperties.hsqldb_result_max_memory_rows);

            database.setResultMaxMemoryRows(rows);
        }

        String tableType = database.databaseProperties.getStringProperty(
            HsqlDatabaseProperties.hsqldb_default_table_type);

        if ("CACHED".equalsIgnoreCase(tableType)) {
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

        database.defaultDeadlockRollback =
            database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.hsqldb_tx_deadlock_rollback);
        database.sqlEnforceNames = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_enforce_names);
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
        database.sqlConcatNulls = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_concat_nulls);
        database.sqlUniqueNulls = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_unique_nulls);
        database.sqlConvertTruncate =
            database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.sql_convert_trunc);
        database.sqlSyntaxMss = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_syntax_mss);
        database.sqlSyntaxMys = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_syntax_mys);
        database.sqlSyntaxOra = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_syntax_ora);
        database.sqlSyntaxPgs = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_syntax_pgs);

        if (database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.sql_compare_in_locale)) {
            database.collation.setCollationAsLocale();
        }

        propEventLogLevel = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_applog);
        propFilesReadOnly = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_files_readonly);
        propDatabaseReadOnly = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_readonly);
        propIncrementBackup = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_inc_backup);
        propNioDataFile = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_nio_data_file);
        propNioMaxSize =
            database.databaseProperties.getIntegerProperty(
                HsqlDatabaseProperties.hsqldb_nio_max_size) * 1024 * 1024L;
        propCacheMaxRows = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_cache_rows);
        propCacheMaxSize =
            database.databaseProperties.getIntegerProperty(
                HsqlDatabaseProperties.hsqldb_cache_size) * 1024;

        setLobFileScaleNoCheck(
            database.databaseProperties.getIntegerProperty(
                HsqlDatabaseProperties.hsqldb_lob_file_scale));
        setCacheFileScaleNoCheck(
            database.databaseProperties.getIntegerProperty(
                HsqlDatabaseProperties.hsqldb_cache_file_scale));

        propCacheDefragLimit = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_defrag_limit);
        propMaxFreeBlocks = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_cache_free_count_scale);
        propMaxFreeBlocks = 1 << propMaxFreeBlocks;
        propTextAllowFullPath = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.textdb_allow_full_path);
        propWriteDelay = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_write_delay_millis);

        if (!database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.hsqldb_write_delay)) {
            propWriteDelay = 0;
        }

        propLogSize = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_log_size);
        propLogData = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_log_data);
        propGC = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.runtime_gc_interval);
        propRefIntegrity = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_ref_integrity);
    }

// fredt@users 20020130 - patch 495484 by boucherb@users

    /**
     *  Shuts down the logging process using the specified mode. <p>
     *
     * @param  closemode The mode in which to shut down the logging
     *      process
     *      <OL>
     *        <LI> closemode -1 performs SHUTDOWN IMMEDIATELY, equivalent
     *        to  a poweroff or crash.
     *        <LI> closemode 0 performs a normal SHUTDOWN that
     *        checkpoints the database normally.
     *        <LI> closemode 1 performs a shutdown compact that scripts
     *        out the contents of any CACHED tables to the log then
     *        deletes the existing *.data file that contains the data
     *        for all CACHED table before the normal checkpoint process
     *        which in turn creates a new, compact *.data file.
     *        <LI> closemode 2 performs a SHUTDOWN SCRIPT.
     *      </OL>
     *
     * @return  true if closed with no problems or false if a problem was
     *        encountered.
     */
    public boolean closePersistence(int closemode) {

        if (log == null) {
            return true;
        }

        database.lobManager.deleteUnusedLobs();

        try {
            switch (closemode) {

                case Database.CLOSEMODE_IMMEDIATELY :
                    log.shutdown();
                    break;

                case Database.CLOSEMODE_NORMAL :
                    log.close(false);
                    break;

                case Database.CLOSEMODE_COMPACT :
                case Database.CLOSEMODE_SCRIPT :
                    log.close(true);
                    break;
            }
        } catch (Throwable e) {
            database.logger.logSevereEvent("error closing log", e);

            log = null;

            return false;
        }

        database.logger.logInfoEvent("Database closed");

        log = null;

        appLog.close();

        logsStatements = false;
        loggingEnabled = false;

        return true;
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
        return DatabaseURL.isFileBasedDatabaseType(database.getType())
               && !database.isFilesReadOnly();
    }

    FrameworkLogger fwLogger;

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
    private FrameworkLogger getEventLogger() {

        if (fwLogger != null) {
            return fwLogger;
        }

        String name = database.getUniqueName();

        if (name == null) {

            // The database unique name is set up at different times
            // depending on upgraded / exiting / new databases.
            // Therefore FrameworkLogger is not used until the unique
            // name is known.
            return null;
        }

        fwLogger = FrameworkLogger.getLog(Logger.class,
                                          database.getUniqueName());

        return fwLogger;
    }

    public void setEventLogLevel(int level) {

        propEventLogLevel = level;

        appLog.setLevel(level);
    }

    public void logSevereEvent(String message, Throwable t) {

        getEventLogger();

        if (fwLogger != null) {
            fwLogger.severe(message, t);
        }

        if (appLog != null) {
            appLog.logContext(t, message);
        }
    }

    public void logWarningEvent(String message, Throwable t) {

        getEventLogger();

        if (fwLogger != null) {
            fwLogger.warning(message, t);
        }

        appLog.logContext(t, message);
    }

    public void logInfoEvent(String message) {

        getEventLogger();

        if (fwLogger != null) {
            fwLogger.info(message);
        }

        appLog.logContext(SimpleLog.LOG_NORMAL, message);
    }

    /**
     * Returns the Cache object or null if one doesn't exist.
     */
    private DataFileCache getCache() {

        if (log == null) {
            return null;
        } else {
            return log.getCache();
        }
    }

    /**
     * Records a Log entry representing start of a session or a new connection
     * action on the specified Session object.
     */
    public synchronized void writeStartSession(Session session) {

        if (loggingEnabled) {
            log.writeStatement(session, session.getUser().getConnectUserSQL());
        }
    }

    /**
     * Records a Log entry for the specified SQL statement, on behalf of
     * the specified Session object.
     */
    public synchronized void writeOtherStatement(Session session,
            String statement) {

        if (loggingEnabled) {
            log.writeStatement(session, statement);
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
            synchLog();
        }
    }

    /**
     * Used at transaction commit
     */
    public synchronized void writeRollbackStatement(Session session) {

        if (loggingEnabled) {
            log.writeStatement(session, "ROLLBACK");
        }
    }

    /**
     * Called after commits or after each statement when autocommit is on
     */
    private synchronized void synchLog() {

        if (loggingEnabled && syncFile) {
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
     *  files, in order to ensure as much as possible the ACID properites
     *  of the database.
     *
     * @throws  HsqlException if there is a problem checkpointing the
     *      database
     */
    public synchronized void checkpoint(boolean mode) {

        if (logsStatements) {
            database.logger.logInfoEvent("Checkpoint start");
            log.checkpoint(mode);
            database.sessionManager.resetLoggedSchemas();
            database.logger.logInfoEvent("Checkpoint end");
        }

        checkpointDue = false;
    }

    /**
     *  Sets the maximum size to which the log file can grow
     *  before being automatically checkpointed.
     *
     * @param  megas size in MB
     */
    public synchronized void setLogSize(int megas) {

        propLogSize = megas;

        if (log != null) {
            log.setLogSize(propLogSize);
        }
    }

    /**
     *  Sets logging on or off.
     *
     * @param  megas size in MB
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
     * @param  i The type
     */
    public synchronized void setScriptType(int format) {

        if (format == propScriptFormat) {
            return;
        }

        propScriptFormat   = format;
        checkpointRequired = true;
    }

    /**
     *  Sets the log write delay mode to number of seconds. By default
     *  executed commands written to the log are committed fully at most
     *  1 second after they are executed. This improves performance for
     *  applications that execute a large number
     *  of short running statements in a short period of time, but risks
     *  failing to log some possibly large number of statements in the
     *  event of a crash. A small value improves recovery.
     *  A value of 0 will severly slow down logging when autocommit is on,
     *  or many short transactions are committed.
     *
     * @param  delay in milliseconds
     */
    public synchronized void setWriteDelay(int delay) {

        propWriteDelay = delay;

        if (log != null) {
            syncFile = (delay == 0);

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

    public synchronized void setIncrementBackup(boolean val) {

        if (val == propIncrementBackup) {
            return;
        }

        if (log != null) {
            log.setIncrementBackup(val);

            if (log.hasCache()) {
                checkpointRequired = true;
            }
        }

        propIncrementBackup = val;
    }

    public void setCacheMaxRows(int value) {
        propCacheMaxRows = value;
    }

    public int getCacheRowsDefault() {
        return propCacheMaxRows;
    }

    public void setCacheSize(int value) {
        propCacheMaxSize = value * 1024;
    }

    public int getCacheSize() {
        return propCacheMaxSize;
    }

    public void setCacheFileScale(int value) {

        if (propCacheFileScale == value) {
            return;
        }

        checkPower(value, 8);

        if (value < 8 && value != 1) {
            throw Error.error(ErrorCode.X_42556);
        }

        if (getCache() != null) {
            throw Error.error(ErrorCode.DATA_FILE_IN_USE);
        }

        propCacheFileScale = value;
    }

    public void setCacheFileScaleNoCheck(int value) {

        checkPower(value, 8);

        if (value < 8 && value != 1) {
            throw Error.error(ErrorCode.X_42556);
        }

        propCacheFileScale = value;
    }

    public int getCacheFileScale() {
        return propCacheFileScale;
    }

    public void setLobFileScale(int value) {

        if (propLobBlockSize == value * 1024) {
            return;
        }

        checkPower(value, 6);

        if (database.lobManager.getLobCount() > 0) {
            throw Error.error(ErrorCode.DATA_FILE_IN_USE);
        }

        propLobBlockSize = value * 1024;

        database.lobManager.close();
        database.lobManager.open();
    }

    public void setLobFileScaleNoCheck(int value) {

        checkPower(value, 6);

        propLobBlockSize = value * 1024;
    }

    public int getLobFileScale() {
        return propLobBlockSize / 1024;
    }

    public void setDefagLimit(int value) {
        propCacheDefragLimit = value;
    }

    public int getDefragLimit() {
        return propCacheDefragLimit;
    }

    public void setDefaultTextTableProperties(String source,
            HsqlProperties props) {
        this.propTextSourceDefault = source;
        this.propTextSourceProps   = props;
    }

    public void setNioDataFile(boolean value) {
        propNioDataFile = value;
    }

    public void setNioMaxSize(int value) {

        checkPower(value, 12);

        if (value < 8) {
            throw Error.error(ErrorCode.X_42556);
        }

        propNioMaxSize = value * 1024L * 1024L;
    }

    public FileAccess getFileAccess() {
        return fileAccess;
    }

    public boolean isStoredFileAccess() {
        return isStoredFileAccess;
    }

    public boolean isFileDatabase() {
        return propIsFileDatabase;
    }

    public String getTempDirectoryPath() {
        return tempDirectoryPath;
    }

    static void checkPower(int n, int limit) {

        for (int i = 0; i < limit; i++) {
            if ((n & 1) != 0) {
                if ((n | 1) != 1) {
                    throw Error.error(ErrorCode.X_42556);
                }

                return;
            }

            n >>= 1;
        }

        throw Error.error(ErrorCode.X_42556);
    }

    /**
     *  Opens the TextCache object.
     */
    public DataFileCache openTextFilePersistence(Table table, String source,
            boolean readOnlyData, boolean reversed) {
        return log.openTextCache(table, source, readOnlyData, reversed);
    }

    /**
     *  Closes the TextCache object.
     */
    public void closeTextCache(Table table) {
        log.closeTextCache(table);
    }

    public synchronized boolean needsCheckpointReset() {

        if (checkpointRequired && !checkpointDue && !checkpointDisabled) {
            checkpointDue      = true;
            checkpointRequired = false;

            return true;
        }

        checkpointRequired = false;

        return false;
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
                                    TableBase table, boolean diskBased) {

        switch (table.getTableType()) {

            case TableBase.CACHED_TABLE :
                DataFileCache cache = getCache();

                if (cache == null) {
                    break;
                }

                return new RowStoreAVLDisk(collection, cache, (Table) table);

            case TableBase.FUNCTION_TABLE :
            case TableBase.MEMORY_TABLE :
            case TableBase.SYSTEM_TABLE :
                return new RowStoreAVLMemory(collection, (Table) table);

            case TableBase.TEXT_TABLE :
                return new RowStoreAVLDiskData(collection, (Table) table);

            case TableBase.RESULT_TABLE :
                if (session == null) {
                    return null;
                }

                return new RowStoreAVLHybrid(session, collection, table,
                                             diskBased);

            case TableBase.INFO_SCHEMA_TABLE :
            case TableBase.TEMP_TABLE :
                diskBased = false;

                return new RowStoreAVLHybridExtended(session, collection,
                                                     table, diskBased);

            // fall through
            case TableBase.SYSTEM_SUBQUERY :
            case TableBase.VIEW_TABLE :
            case TableBase.TRANSITION_TABLE :
                if (session == null) {
                    return null;
                }

                return new RowStoreAVLHybrid(session, collection, table,
                                             diskBased);
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

            case TableBase.FUNCTION_TABLE :
            case TableBase.CACHED_TABLE :
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

    public String getValueStringForProperty(String name) {

        String value = "";

        if (HsqlDatabaseProperties.hsqldb_tx.equals(name)) {
            switch (database.txManager.getTransactionControl()) {

                case TransactionManager.MVCC :
                    value = Tokens.T_MVCC.toLowerCase();
                    break;

                case TransactionManager.MVLOCKS :
                    value = Tokens.T_MVLOCKS.toLowerCase();
                    break;

                case TransactionManager.LOCKS :
                    value = Tokens.T_LOCKS.toLowerCase();
                    break;
            }

            return value;
        }

        if (HsqlDatabaseProperties.hsqldb_tx_level.equals(name)) {
            switch (database.defaultIsolationLevel) {

                case SessionInterface.TX_READ_COMMITTED :
                    value = new StringBuffer(Tokens.T_READ).append(' ').append(
                        Tokens.T_COMMITTED).toString().toLowerCase();
                    break;

                case SessionInterface.TX_SERIALIZABLE :
                    value = Tokens.T_SERIALIZABLE.toLowerCase();
                    break;
            }

            return value;
        }

        if (HsqlDatabaseProperties.hsqldb_applog.equals(name)) {
            return String.valueOf(appLog.getLevel());
        }

        if (HsqlDatabaseProperties.hsqldb_lob_file_scale.equals(name)) {
            return String.valueOf(propLobBlockSize);
        }

        if (HsqlDatabaseProperties.hsqldb_cache_file_scale.equals(name)) {
            return String.valueOf(propCacheFileScale);
        }

        if (HsqlDatabaseProperties.hsqldb_cache_free_count_scale.equals(
                name)) {
            return String.valueOf(this.propMaxFreeBlocks);
        }

        if (HsqlDatabaseProperties.hsqldb_cache_rows.equals(name)) {
            return String.valueOf(this.propCacheMaxRows);
        }

        if (HsqlDatabaseProperties.hsqldb_cache_size.equals(name)) {
            String.valueOf(this.propCacheMaxSize);
        }

        if (HsqlDatabaseProperties.hsqldb_default_table_type.equals(name)) {
            return database.schemaManager.getDefaultTableType()
                   == TableBase.CACHED_TABLE ? "cached"
                                             : "memory";
        }

        if (HsqlDatabaseProperties.hsqldb_defrag_limit.equals(name)) {
            return String.valueOf(this.propCacheDefragLimit);
        }

        if (HsqlDatabaseProperties.hsqldb_files_readonly.equals(name)) {
            return database.databaseProperties.getPropertyString(
                HsqlDatabaseProperties.hsqldb_files_readonly);
        }

        if (HsqlDatabaseProperties.hsqldb_lock_file.equals(name)) {
            return database.databaseProperties.getPropertyString(
                HsqlDatabaseProperties.hsqldb_lock_file);
        }

        if (HsqlDatabaseProperties.hsqldb_log_data.equals(name)) {
            return String.valueOf(this.propLogData);
        }

        if (HsqlDatabaseProperties.hsqldb_log_size.equals(name)) {
            return String.valueOf(this.propLogSize);
        }

        if (HsqlDatabaseProperties.hsqldb_nio_data_file.equals(name)) {
            return String.valueOf(this.propNioDataFile);
        }

        if (HsqlDatabaseProperties.hsqldb_nio_max_size.equals(name)) {
            return String.valueOf(this.propNioMaxSize);
        }

        if (HsqlDatabaseProperties.hsqldb_script_format.equals(name)) {
            return ScriptWriterBase.LIST_SCRIPT_FORMATS[0].toLowerCase();
        }

        if (HsqlDatabaseProperties.hsqldb_temp_directory.equals(name)) {
            return null;
        }

        if (HsqlDatabaseProperties.hsqldb_result_max_memory_rows.equals(
                name)) {
            return String.valueOf(this.propCacheDefragLimit);
        }

        if (HsqlDatabaseProperties.hsqldb_write_delay.equals(name)) {
            return String.valueOf(this.propWriteDelay != 0);
        }

        if (HsqlDatabaseProperties.hsqldb_write_delay_millis.equals(name)) {
            return String.valueOf(this.propWriteDelay);
        }

        if (HsqlDatabaseProperties.sql_ref_integrity.equals(name)) {
            return database.isReferentialIntegrity() ? "true"
                                                     : "false";
        }

        if (HsqlDatabaseProperties.sql_compare_in_locale.equals(name)) {
            return null;
        }

        if (HsqlDatabaseProperties.sql_enforce_size.equals(name)) {
            return String.valueOf(database.sqlEnforceSize);
        }

        if (HsqlDatabaseProperties.sql_enforce_refs.equals(name)) {
            return String.valueOf(database.sqlEnforceRefs);
        }

        if (HsqlDatabaseProperties.sql_enforce_names.equals(name)) {
            return String.valueOf(database.sqlEnforceNames);
        }

        if (HsqlDatabaseProperties.sql_enforce_types.equals(name)) {
            return String.valueOf(database.sqlEnforceTypes);
        }

        if (HsqlDatabaseProperties.jdbc_translate_tti_types.equals(name)) {
            return String.valueOf(database.sqlTranslateTTI);
        }

/*
        if (HsqlDatabaseProperties.sql_identity_is_pk.equals(name)) {
            return null;
        }

        if (HsqlDatabaseProperties.sql_longvar_is_lob.equals(name)) {
            return null;
        }

        if (HsqlDatabaseProperties.textdb_cache_scale.equals(name)) {
            return null;
        }

        if (HsqlDatabaseProperties.textdb_cache_size_scale.equals(name)) {
            return null;
        }

        if (HsqlDatabaseProperties.textdb_all_quoted.equals(name)) {
            return null;
        }

        if (HsqlDatabaseProperties.textdb_allow_full_path.equals(name)) {
            return null;
        }

        if (HsqlDatabaseProperties.textdb_encoding.equals(name)) {
            return null;
        }

        if (HsqlDatabaseProperties.textdb_ignore_first.equals(name)) {
            return null;
        }

        if (HsqlDatabaseProperties.textdb_quoted.equals(name)) {
            return null;
        }

        if (HsqlDatabaseProperties.textdb_fs.equals(name)) {
            return null;
        }

        if (HsqlDatabaseProperties.textdb_vs.equals(name)) {
            return null;
        }

        if (HsqlDatabaseProperties.textdb_lvs.equals(name)) {
            return null;
        }
*/
        return null;
    }

    public String[] getPropertiesSQL() {

        HsqlArrayList list = new HsqlArrayList();
        StringBuffer  sb   = new StringBuffer();

        sb.append("SET DATABASE ").append(Tokens.T_UNIQUE).append(' ');
        sb.append(Tokens.T_NAME).append(' ').append(database.getUniqueName());
        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET DATABASE ").append(Tokens.T_GC).append(' ');
        sb.append(propGC);
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
        sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
        sb.append(Tokens.T_NAMES).append(' ');
        sb.append(database.sqlEnforceNames ? Tokens.T_TRUE
                                           : Tokens.T_FALSE);
        list.add(sb.toString());
        sb.setLength(0);
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
        sb.setLength(0);
        sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
        sb.append(Tokens.T_TRANSLATE).append(' ').append(Tokens.T_TTI);
        sb.append(' ').append(Tokens.T_TYPES).append(' ');
        sb.append(database.sqlTranslateTTI ? Tokens.T_TRUE
                                           : Tokens.T_FALSE);
        list.add(sb.toString());
        sb.setLength(0);
        sb.append("SET DATABASE ").append(Tokens.T_SQL).append(' ');
        sb.append(Tokens.T_CONCAT_WORD).append(' ');
        sb.append(Tokens.T_NULLS).append(' ');
        sb.append(database.sqlConcatNulls ? Tokens.T_TRUE
                                          : Tokens.T_FALSE);
        list.add(sb.toString());
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

        if (isFileDatabase()) {
            if (database.schemaManager.getDefaultTableType()
                    == TableBase.CACHED_TABLE) {
                list.add("SET DATABASE DEFAULT TABLE TYPE CACHED");
            }

            int     delay  = propWriteDelay;
            boolean millis = delay > 0 && delay < 1000;

            if (millis) {
                if (delay < 20) {
                    delay = 20;
                }
            } else {
                delay /= 1000;
            }

            sb.setLength(0);
            sb.append("SET FILES ").append(Tokens.T_WRITE).append(' ');
            sb.append(Tokens.T_DELAY).append(' ').append(delay);

            if (millis) {
                sb.append(' ').append(Tokens.T_MILLIS);
            }

            list.add(sb.toString());
            sb.setLength(0);
            sb.append("SET FILES ").append(Tokens.T_BACKUP);
            sb.append(' ').append(Tokens.T_INCREMENT).append(' ');
            sb.append(propIncrementBackup ? Tokens.T_TRUE
                                          : Tokens.T_FALSE);
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
            sb.setLength(0);
            sb.append("SET FILES ").append(Tokens.T_SCALE);
            sb.append(' ').append(propCacheFileScale);
            list.add(sb.toString());
            sb.setLength(0);
            sb.append("SET FILES ").append(Tokens.T_LOB).append(' ').append(
                Tokens.T_SCALE);
            sb.append(' ').append(getLobFileScale());
            list.add(sb.toString());
            sb.setLength(0);
            sb.append("SET FILES ").append(Tokens.T_DEFRAG);
            sb.append(' ').append(propCacheDefragLimit);
            list.add(sb.toString());
            sb.setLength(0);
            sb.append("SET FILES ").append(Tokens.T_NIO);
            sb.append(' ').append(propNioDataFile ? Tokens.T_TRUE
                                                  : Tokens.T_FALSE);
            list.add(sb.toString());
            sb.setLength(0);
            sb.append("SET FILES ").append(Tokens.T_NIO).append(' ').append(
                Tokens.T_SIZE);
            sb.append(' ').append(propNioMaxSize / (1024 * 1024));
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
            /*
            if (propTempDirectoryPath != null) {
                sb.append("SET FILES ").append(Tokens.T_TEMP).append(' ');
                sb.append(Tokens.T_PATH).append(' ');
                sb.append(propTempDirectoryPath);
                list.add(sb.toString());
                sb.setLength(0);
            }
            */
            sb.append("SET DATABASE ").append(Tokens.T_TEXT).append(' ');
            sb.append(Tokens.T_TABLE).append(' ').append(Tokens.T_DEFAULTS);
            sb.append(' ').append('\'');
            sb.append(propTextSourceDefault).append('\'');
            list.add(sb.toString());
            sb.setLength(0);
        }

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }

    //
    static private SimpleDateFormat backupFileFormat =
        new SimpleDateFormat("yyyyMMdd'T'HHmmss");
    static private Character runtimeFileDelim = null;

    public synchronized void backup(String destPath, boolean script,
                                    boolean blocking, boolean compressed) {

        String dbPath = database.getPath();

        /* If want to add db Id also, will need to pass either Database
         * instead of dbPath, or pass dbPath + Id from CommandStatement.
         */
        if (runtimeFileDelim == null) {
            runtimeFileDelim =
                new Character(System.getProperty("file.separator").charAt(0));
        }

        String instanceName = new File(dbPath).getName();

        if (destPath == null || destPath.length() < 1) {
            throw Error.error(ErrorCode.X_2200F, "0-length destination path");
        }

        char lastChar = destPath.charAt(destPath.length() - 1);
        boolean generateName = (lastChar == '/'
                                || lastChar == runtimeFileDelim.charValue());
        String defaultCompressionSuffix = compressed ? ".tar.gz"
                                                     : ".tar";
        File archiveFile =
            generateName
            ? (new File(destPath.substring(0, destPath.length() - 1),
                        instanceName + '-'
                        + backupFileFormat.format(new java.util.Date())
                        + defaultCompressionSuffix))
            : (new File(destPath));
        boolean nameImpliesCompress =
            archiveFile.getName().endsWith(".tar.gz")
            || archiveFile.getName().endsWith(".tgz");

        if ((!nameImpliesCompress)
                && !archiveFile.getName().endsWith(".tar")) {
            throw Error.error(null, ErrorCode.UNSUPPORTED_FILENAME_SUFFIX, 0,
                              new String[] {
                archiveFile.getName(), ".tar, .tar.gz, .tgz"
            });
        }

        if (compressed != nameImpliesCompress) {
            throw Error.error(null, ErrorCode.COMPRESSION_SUFFIX_MISMATCH, 0,
                              new Object[] {
                new Boolean(compressed), archiveFile.getName()
            });
        }

        log.checkpointClose();

        try {
            database.logger.logInfoEvent("Initiating backup of instance '"
                                         + instanceName + "'");

            // By default, DbBackup will throw if archiveFile (or
            // corresponding work file) already exist.  That's just what we
            // want here.
            DbBackup backup = new DbBackup(archiveFile, dbPath);

            backup.setAbortUponModify(false);
            backup.write();
            database.logger.logInfoEvent("Successfully backed up instance '"
                                         + instanceName + "' to '" + destPath
                                         + "'");

            // RENAME tempPath to destPath
        } catch (IllegalArgumentException iae) {
            throw Error.error(ErrorCode.X_HV00A, iae.getMessage());
        } catch (IOException ioe) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, ioe.getMessage());
        } catch (TarMalformatException tme) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, tme.getMessage());
        } finally {
            log.checkpointReopen();
        }
    }
}
