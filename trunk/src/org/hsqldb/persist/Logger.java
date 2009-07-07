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


package org.hsqldb.persist;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;

import org.hsqldb.Database;
import org.hsqldb.DatabaseURL;
import org.hsqldb.Error;
import org.hsqldb.ErrorCode;
import org.hsqldb.NumberSequence;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.TableBase;
import org.hsqldb.Tokens;
import org.hsqldb.lib.FileAccess;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.SimpleLog;
import org.hsqldb.lib.tar.DbBackup;
import org.hsqldb.lib.tar.TarMalformatException;
import org.hsqldb.lib.java.JavaSystem;

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
 * @version 1.9.0
 * @since 1.7.0
 */
public class Logger {

    public SimpleLog appLog;
    private Log      log;
    private Database database;
    private LockFile lockFile;
    public boolean   checkpointRequired;
    public boolean   checkpointHandled;
    private boolean  logsStatements;
    private boolean  loggingEnabled;
    private boolean  syncFile = false;

    //
    boolean        propFilesReadOnly;
    boolean        propDatabaseReadOnly;
    boolean        propUseLockFile;
    boolean        propIncrementBackup;
    boolean        propNioDataFile;
    boolean        propLockFileEnabled;
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

    //
    public FileAccess fileaccess;
    public boolean    isStoredFileAccess;
    String            tempDirectoryPath;

    //
    public boolean isNewDatabase;

    public Logger(Database database) {

        this.database = database;
        appLog        = new SimpleLog(null, SimpleLog.LOG_NONE, false);

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

                fileaccess =
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
            fileaccess = FileUtil.getDefaultInstance();
        }
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

        database.databaseProperties = new HsqlDatabaseProperties(database);
        isNewDatabase =
            !DatabaseURL.isFileBasedDatabaseType(database.getType())
            || !database.databaseProperties.propertiesFileExists();

        if (isNewDatabase) {
            if (database.urlProperties.isPropertyTrue(
                    HsqlDatabaseProperties.url_ifexists)) {
                throw Error.error(ErrorCode.DATABASE_NOT_EXISTS,
                                  database.getPath());
            }

            database.databaseProperties.setURLProperties(
                database.urlProperties);
        } else {
            database.databaseProperties.load();
        }

        setVariables();

        if (!DatabaseURL.isFileBasedDatabaseType(database.getType())) {
            return;
        }

        //
        checkpointRequired = false;

        String path = database.getPath();
        int loglevel = database.getProperties().getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_applog);

        this.database = database;

        if (loglevel != SimpleLog.LOG_NONE) {
            appLog = new SimpleLog(path + ".app.log", loglevel,
                                   !database.isFilesReadOnly());
        }

        appLog.sendLine(SimpleLog.LOG_ERROR, "Database (re)opened");

        loggingEnabled = false;

        boolean useLock = database.getProperties().isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_lock_file);

        if (useLock && !database.isFilesReadOnly()) {
            acquireLock(path);
        }

        log = new Log(database);

        log.open();

        logsStatements = loggingEnabled = !database.isFilesReadOnly();
    }

    public void setVariables() {

        if (database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.hsqldb_readonly)) {
            database.setReadOnly();
        }

        if (database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.hsqldb_files_readonly)) {
            database.setFilesReadOnly();
        }

        //
        if (!database.isFilesReadOnly()) {
            tempDirectoryPath = database.getProperties().getStringProperty(
                HsqlDatabaseProperties.hsqldb_temp_directory);

            String path = database.getPath() + ".tmp";

            tempDirectoryPath = FileUtil.makeDirectories(path);
        }

        if (getTempDirectoryPath() != null) {
            int rows = database.databaseProperties.getIntegerProperty(
                HsqlDatabaseProperties.hsqldb_result_max_memory_rows);

            database.setResultMaxMemoryRows(rows);
        }

        database.sqlEnforceSize = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_enforce_size);
        database.sqlEnforceNames = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.sql_enforce_names);

        if (database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.sql_compare_in_locale)) {
            database.collation.setCollationAsLocale();
        }

        propFilesReadOnly = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_files_readonly);
        propDatabaseReadOnly = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_readonly);
        propUseLockFile = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_lock_file);
        propIncrementBackup = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_inc_backup);
        propNioDataFile = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_nio_data_file);
        propCacheMaxRows = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_cache_rows);
        propCacheMaxSize =
            database.databaseProperties.getIntegerProperty(
                HsqlDatabaseProperties.hsqldb_cache_size) * 1024;
        propCacheFileScale = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_cache_file_scale);
        propCacheDefragLimit = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_defrag_limit);
        propMaxFreeBlocks = database.databaseProperties.getIntegerProperty(
            HsqlDatabaseProperties.hsqldb_cache_free_count_scale);
        propMaxFreeBlocks = 1 << propMaxFreeBlocks;
        propTextAllowFullPath = database.databaseProperties.isPropertyTrue(
            HsqlDatabaseProperties.textdb_allow_full_path);
        propWriteDelay =
            database.databaseProperties.isPropertyTrue(
                HsqlDatabaseProperties.hsqldb_write_delay) ? 10000
                                                           : 0;
        JavaSystem.gcFrequency =
            database.databaseProperties.getIntegerProperty(
                HsqlDatabaseProperties.runtime_gc_interval);

        database.setMetaDirty(false);
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
            appLog.logContext(e, "error closing log");
            appLog.close();

            log = null;

            return false;
        }

        appLog.sendLine(SimpleLog.LOG_ERROR, "Database closed");
        appLog.close();

        log = null;

        return true;
    }

    /**
     *  Determines if the logging process actually does anything. <p>
     *
     *  In-memory Database objects do not need to log anything. This
     *  method is essentially equivalent to testing whether this logger's
     *  database is an in-memory mode database.
     *
     * @return  true if this object encapsulates a non-null Log instance,
     *      else false
     */
    public boolean hasPersistence() {
        return log != null;
    }

    /**
     *  Returns the Cache object or null if one doesn't exist.
     */
    public DataFileCache getCache() {

        if (log == null) {
            return null;
        } else {
            return log.getCache();
        }
    }

    /**
     *  Returns true if Cache object exists.
     */
    public boolean hasCache() {

        if (log == null) {
            return false;
        } else {
            return log.hasCache();
        }
    }

    /**
     *  Records a Log entry representing a new connection action on the
     *  specified Session object.
     *
     * @param  session the Session object for which to record the log
     *      entry
     * @throws  HsqlException if there is a problem recording the Log
     *      entry
     */
    public synchronized void logStartSession(Session session) {

        if (loggingEnabled) {
            writeToLog(session, session.getUser().getConnectUserSQL());
        }
    }

    /**
     *  Records a Log entry for the specified SQL statement, on behalf of
     *  the specified Session object.
     *
     * @param  session the Session object for which to record the Log
     *      entry
     * @param  statement the SQL statement to Log
     * @throws  HsqlException if there is a problem recording the entry
     */
    public synchronized void writeToLog(Session session, String statement) {

        if (loggingEnabled && log != null) {
            log.writeStatement(session, statement);
        }
    }

    public synchronized void writeInsertStatement(Session session,
            Table table, Object[] row) {

        if (loggingEnabled) {
            log.writeInsertStatement(session, table, row);
        }
    }

    public synchronized void writeDeleteStatement(Session session, Table t,
            Object[] row) {

        if (loggingEnabled) {
            log.writeDeleteStatement(session, t, row);
        }
    }

    public synchronized void writeSequenceStatement(Session session,
            NumberSequence s) {

        if (loggingEnabled) {
            log.writeSequenceStatement(session, s);
        }
    }

    public synchronized void writeCommitStatement(Session session) {

        if (loggingEnabled) {
            log.writeCommitStatement(session);
            synchLog();
        }
    }

    /**
     * Called after commits or after each statement when autocommit is on
     */
    public synchronized void synchLog() {

        if (loggingEnabled && syncFile) {
            log.synchLog();
        }
    }

    public synchronized void synchLogForce() {

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
     *  files, in order to ensure as much as possible the ACID properites
     *  of the database.
     *
     * @throws  HsqlException if there is a problem checkpointing the
     *      database
     */
    public synchronized void checkpoint(boolean mode) {

        if (loggingEnabled) {
            appLog.logContext(SimpleLog.LOG_NORMAL, "start");

            checkpointRequired = false;
            checkpointHandled  = false;

            log.checkpoint(mode);
            database.sessionManager.resetLoggedSchemas();
            appLog.logContext(SimpleLog.LOG_NORMAL, "end");
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

        if (log != null) {
            log.setLogSize(propLogSize);
        }
    }

    /**
     *  Sets the type of script file, currently 0 for text (default)
     *  1 for binary and 3 for compressed
     *
     * @param  i The type
     */
    public synchronized void setScriptType(int i) {

        if (log != null) {

            //
        }
    }

    /**
     *  Sets the log write delay mode to number of seconds. By default
     *  executed commands written to the log are committed fully at most
     *  60 second after they are executed. This improves performance for
     *  applications that execute a large number
     *  of short running statements in a short period of time, but risks
     *  failing to log some possibly large number of statements in the
     *  event of a crash. A small value improves recovery.
     *  A value of 0 will severly slow down logging when autocommit is on,
     *  or many short transactions are committed.
     *
     * @param  delay in seconds
     */
    public synchronized void setWriteDelay(int delay) {

        propWriteDelay = delay;

        if (log != null) {
            syncFile = (delay == 0);

            log.setWriteDelay(delay);
        }
    }

    public int getWriteDelay() {
        return propWriteDelay;
    }

    public int getLogSize() {
        return propLogSize;
    }

    public int getScriptType() {
        return log != null ? log.getScriptType()
                           : 0;
    }

    public synchronized void setIncrementBackup(boolean val) {

        if (val == propIncrementBackup) {
            return;
        }

        propIncrementBackup = val;

        if (log != null) {
            log.setIncrementBackup(val);

            if (log.hasCache()) {
                database.logger.checkpointRequired = true;
            }
        }
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
        propCacheFileScale = value;
    }

    public int getCacheFileScale() {
        return propCacheFileScale;
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

        if (checkpointRequired && !checkpointHandled) {
            checkpointHandled  = true;
            checkpointRequired = false;

            return true;
        }

        checkpointRequired = false;

        return false;
    }

    public void stopLogging() {
        loggingEnabled = false;
    }

    public void restartLogging() {
        loggingEnabled = logsStatements;
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

    // properties
    public void setFileLock(boolean value) {
        propLockFileEnabled = true;
    }

    public void setNioDataFile(boolean value) {
        propNioDataFile = value;
    }

    public void setDatabaseReadonly(boolean value) {
        propDatabaseReadOnly = true;
    }

    public void setFilesReadonly(boolean value) {
        propFilesReadOnly = true;
    }

    public FileAccess getFileAccess() {
        return fileaccess;
    }

    public boolean isStoredFileAccess() {
        return isStoredFileAccess;
    }

    public String getTempDirectoryPath() {
        return tempDirectoryPath;
    }

    public boolean isExistingDatabase() {

        if (!DatabaseURL.isFileBasedDatabaseType(database.getType())) {
            return false;
        }

        HsqlDatabaseProperties props = new HsqlDatabaseProperties(database);

        return props.propertiesFileExists();
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

            case TableBase.MEMORY_TABLE :
            case TableBase.SYSTEM_TABLE :
                return new RowStoreAVLMemory(collection, (Table) table);

            case TableBase.TEXT_TABLE :
                return new RowStoreAVLDiskData(collection, (Table) table);

            case TableBase.TEMP_TABLE :
                diskBased = false;

            // fall through
            case TableBase.RESULT_TABLE :
            case TableBase.SYSTEM_SUBQUERY :
            case TableBase.VIEW_TABLE :
            case TableBase.TRANSITION_TABLE :
                if (session == null) {
                    return null;
                }

                switch (table.persistenceScope) {

                    case TableBase.SCOPE_STATEMENT :
                        return new RowStoreAVLHybrid(session, collection,
                                                     table, diskBased);

                    case TableBase.SCOPE_TRANSACTION :
                        return new RowStoreAVLHybrid(session, collection,
                                                     table, diskBased);

                    case TableBase.SCOPE_SESSION :
                        return new RowStoreAVLHybrid(session, collection,
                                                     table, diskBased);
                }
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "Logger");
    }

    public String[] getPropertiesSQL() {

        HsqlArrayList list = new HsqlArrayList();
        StringBuffer  sb   = new StringBuffer();

        if (hasPersistence()) {
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

            if (database.isReadOnly()) {
                sb.append("SET FILES ").append(Tokens.T_READ);
                sb.append(' ').append(Tokens.T_ONLY);
                list.add(sb.toString());
                sb.setLength(0);
            }

            if (database.isFilesReadOnly()) {
                sb.append("SET FILES ").append(Tokens.T_READ);
                sb.append(' ').append(Tokens.T_ONLY).append(' ');
                sb.append(Tokens.T_FILES);
                list.add(sb.toString());
                sb.setLength(0);
            }

            if (propUseLockFile) {
                sb.append("SET FILES ").append(Tokens.T_LOCK);
                sb.append(' ').append(Tokens.T_TRUE);
                list.add(sb.toString());
                sb.setLength(0);
            }

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
            sb.append("SET FILES ").append(Tokens.T_DEFRAG);
            sb.append(' ').append(propCacheDefragLimit);
            list.add(sb.toString());
            sb.setLength(0);

            if (propNioDataFile) {
                sb.append("SET FILES ").append(Tokens.T_NIO);
                sb.append(' ').append(Tokens.T_TRUE);
                list.add(sb.toString());
                sb.setLength(0);
            }

            sb.append("SET FILES ").append(Tokens.T_LOG).append(' ');
            sb.append(Tokens.T_SIZE).append(' ').append(propLogSize);
            list.add(sb.toString());
            sb.setLength(0);
            sb.append("SET DATABASE TEXT TABLE DEFAULTS ").append('\'');
            sb.append(propTextSourceDefault).append('\'');
            list.add(sb.toString());
            sb.setLength(0);
            sb.append("SET DATABASE ").append(Tokens.T_GC).append(' ');
            sb.append(JavaSystem.gcFrequency);
            list.add(sb.toString());
            sb.setLength(0);
            sb.append("SET DATABASE ").append(Tokens.T_TRANSACTION);
            sb.append(' ').append(Tokens.T_CONTROL).append(' ');

            switch (database.txManager.getTransactionControl()) {

                case Database.MVCC :
                    sb.append(Tokens.T_MVCC);
                    break;

                case Database.MVLOCKS :
                    sb.append(Tokens.T_MVLOCKS);
                    break;

                case Database.LOCKS :
                    sb.append(Tokens.T_LOCKS);
                    break;
            }

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

    public synchronized void backup(String destPath, String dbPath,
                                    boolean script, boolean blocking,
                                    boolean compressed) {

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
            throw Error.error(ErrorCode.UNSUPPORTED_FILENAME_SUFFIX, 0,
                              new String[] {
                archiveFile.getName(), ".tar, .tar.gz, .tgz"
            });
        }

        if (compressed != nameImpliesCompress) {
            throw Error.error(ErrorCode.COMPRESSION_SUFFIX_MISMATCH, 0,
                              new Object[] {
                new Boolean(compressed), archiveFile.getName()
            });
        }

        log.closeForBackup();

        try {
            appLog.logContext(SimpleLog.LOG_NORMAL,
                              "Initiating backup of instance '" + instanceName
                              + "'");

            // By default, DbBackup will throw if archiveFile (or
            // corresponding work file) already exist.  That's just what we
            // want here.
            DbBackup backup = new DbBackup(archiveFile, dbPath);

            backup.setAbortUponModify(false);
            backup.write();
            appLog.logContext(SimpleLog.LOG_NORMAL,
                              "Successfully backed up instance '"
                              + instanceName + "' to '" + destPath + "'");

            // RENAME tempPath to destPath
        } catch (IllegalArgumentException iae) {
            throw Error.error(ErrorCode.X_HV00A, iae.getMessage());
        } catch (IOException ioe) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, ioe.getMessage());
        } catch (TarMalformatException tme) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, tme.getMessage());
        } finally {
            log.openAfterBackup();

            checkpointRequired = false;
        }
    }
}
