/* Copyright (c) 2001-2011, The HSQL Development Group
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

import org.hsqldb.Database;
import org.hsqldb.HsqlException;
import org.hsqldb.HsqlNameManager;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.NumberSequence;
import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.SqlInvariants;
import org.hsqldb.Table;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FileAccess;
import org.hsqldb.lib.FileArchiver;
import org.hsqldb.scriptio.ScriptReaderBase;
import org.hsqldb.scriptio.ScriptReaderDecode;
import org.hsqldb.scriptio.ScriptReaderText;
import org.hsqldb.scriptio.ScriptWriterBase;
import org.hsqldb.scriptio.ScriptWriterEncode;
import org.hsqldb.scriptio.ScriptWriterText;

/**
 *  This class is responsible for managing some of the database files.
 *  An HSQLDB database consists of
 *  a .properties file, a .script file (contains an SQL script),
 *  a .data file (contains data of cached tables) a .backup file
 *  a .log file and a .lobs file.<p>
 *
 *  When using TEXT tables, a data source for each table is also present.<p>
 *
 *  Notes on OpenOffice.org integration.
 *
 *  A Storage API is used when HSQLDB is integrated into OpenOffice.org. All
 *  file operations on the 4 main files are performed by OOo, which integrates
 *  the contents of these files into its database file. The script format is
 *  always TEXT in this case.
 *
 *  Class has the same name as a class in Hypersonic SQL, but has been
 *  completely rewritten since HSQLDB 1.8.0 and earlier.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @author Bob Preston (sqlbob@users dot sourceforge.net) - text table support
 * @version 2.1.1
 * @since 1.8.0
 */
public class Log {

    private HsqlDatabaseProperties properties;
    private String                 fileName;
    private Database               database;
    private FileAccess             fa;
    ScriptWriterBase               dbLogWriter;
    private String                 scriptFileName;
    private String                 logFileName;
    private boolean                filesReadOnly;
    private long                   maxLogSize;
    private int                    writeDelay;
    private DataFileCache          cache;

    Log(Database db) {

        database   = db;
        fa         = db.logger.getFileAccess();
        fileName   = db.getPath();
        properties = db.getProperties();
    }

    void initParams() {

        maxLogSize     = database.logger.propLogSize * 1024L * 1024;
        writeDelay     = database.logger.propWriteDelay;
        filesReadOnly  = database.isFilesReadOnly();
        scriptFileName = fileName + Logger.scriptFileExtension;
        logFileName    = fileName + Logger.logFileExtension;
    }

    /**
     * When opening a database, the hsqldb.compatible_version property is
     * used to determine if this version of the engine is equal to or greater
     * than the earliest version of the engine capable of opening that
     * database.<p>
     */
    void open() {

        initParams();

        int state = properties.getDBModified();

        switch (state) {

            case HsqlDatabaseProperties.FILES_NEW :
                break;

            case HsqlDatabaseProperties.FILES_MODIFIED :
                deleteNewAndOldFiles();
                deleteOldTempFiles();

                if (properties.isVersion18()) {
                    if (fa.isStreamElement(scriptFileName)) {
                        processScript();
                    } else {
                        database.schemaManager.createPublicSchema();
                    }

                    HsqlName name = database.schemaManager.findSchemaHsqlName(
                        SqlInvariants.PUBLIC_SCHEMA);

                    if (name != null) {
                        database.schemaManager.setDefaultSchemaHsqlName(name);
                    }
                } else {
                    processScript();
                }

                processLog();
                checkpoint();
                break;

            case HsqlDatabaseProperties.FILES_MODIFIED_NEW :
                renameNewDataFile();
                renameNewBackup();
                renameNewScript();
                deleteLog();
                properties.setDBModified(
                    HsqlDatabaseProperties.FILES_NOT_MODIFIED);

            // continue as non-modified files
            // fall through
            case HsqlDatabaseProperties.FILES_NOT_MODIFIED :

                /**
                 * if startup is after a SHUTDOWN SCRIPT and there are CACHED
                 * or TEXT tables, perform a checkpoint so that the .script
                 * file no longer contains CACHED or TEXT table rows.
                 */
                processScript();

                if (!filesReadOnly && isAnyCacheModified()) {
                    properties.setDBModified(
                        HsqlDatabaseProperties.FILES_MODIFIED);
                    checkpoint();
                }
                break;
        }

        if (!filesReadOnly) {
            openLog();
            properties.setDBModified(HsqlDatabaseProperties.FILES_MODIFIED);
        }
    }

    /**
     * Close all the database files. If script argument is true, no .data
     * or .backup file will remain and the .script file will contain all the
     * data of the cached tables as well as memory tables.
     *
     * This is not used for filesReadOnly databases which use shutdown.
     */
    void close(boolean script) {

        closeLog();
        deleteOldDataFiles();
        deleteOldTempFiles();
        deleteTempFileDirectory();
        writeScript(script);
        database.logger.closeAllTextCaches(script);

        if (cache != null) {
            cache.close(true);
        }

        // set this one last to save the props
        properties.setProperty(HsqlDatabaseProperties.hsqldb_script_format,
                               database.logger.propScriptFormat);
        properties.setDBModified(HsqlDatabaseProperties.FILES_MODIFIED_NEW);
        deleteLog();

        if (cache != null) {
            if (script) {
                cache.deleteFile();
                cache.deleteBackup();
            } else {
                cache.backupFile(false);
                cache.renameBackupFile();
            }
        }

        renameNewScript();
        properties.setDBModified(HsqlDatabaseProperties.FILES_NOT_MODIFIED);
    }

    /**
     * Fast counterpart to close(). Does not perform a checkpoint or a backup
     * of the .data file.
     */
    void shutdown() {

        if (cache != null) {
            cache.close(false);
        }

        database.logger.closeAllTextCaches(false);
        closeLog();
    }

    /**
     * Deletes the leftovers from any previous unfinished operations.
     */
    void deleteNewAndOldFiles() {

        deleteOldDataFiles();
        fa.removeElement(fileName + Logger.dataFileExtension
                         + Logger.newFileExtension);
        fa.removeElement(fileName + Logger.backupFileExtension
                         + Logger.newFileExtension);
        fa.removeElement(scriptFileName + Logger.newFileExtension);
    }

    void deleteBackup() {
        fa.removeElement(fileName + Logger.backupFileExtension);
    }

    void deleteData() {
        fa.removeElement(fileName + Logger.dataFileExtension);
    }

    void backupData() throws IOException {

        if (database.logger.propIncrementBackup) {
            fa.removeElement(fileName + Logger.backupFileExtension);

            return;
        }

        if (fa.isStreamElement(fileName + Logger.dataFileExtension)) {
            FileArchiver.archive(
                fileName + Logger.dataFileExtension,
                fileName + Logger.backupFileExtension
                + Logger.newFileExtension, database.logger.getFileAccess(),
                                           FileArchiver.COMPRESSION_ZIP);
        }
    }

    void renameNewDataFile() {

        if (fa.isStreamElement(fileName + Logger.dataFileExtension
                               + Logger.newFileExtension)) {
            fa.renameElement(fileName + Logger.dataFileExtension
                             + Logger.newFileExtension, fileName
                                 + Logger.dataFileExtension);
        }
    }

    void renameNewBackup() {

        // required for inc backup
        fa.removeElement(fileName + Logger.backupFileExtension);

        if (fa.isStreamElement(fileName + Logger.backupFileExtension
                               + Logger.newFileExtension)) {
            fa.renameElement(fileName + Logger.backupFileExtension
                             + Logger.newFileExtension, fileName
                                 + Logger.backupFileExtension);
        }
    }

    void renameNewScript() {

        if (fa.isStreamElement(scriptFileName + Logger.newFileExtension)) {
            fa.renameElement(scriptFileName + Logger.newFileExtension,
                             scriptFileName);
        }
    }

    void deleteNewScript() {
        fa.removeElement(scriptFileName + Logger.newFileExtension);
    }

    void deleteNewBackup() {
        fa.removeElement(fileName + Logger.backupFileExtension
                         + Logger.newFileExtension);
    }

    void deleteLog() {
        fa.removeElement(logFileName);
    }

    /**
     * Checks all the caches and returns true if the modified flag is set for any
     */
    boolean isAnyCacheModified() {

        if (cache != null && cache.isModified()) {
            return true;
        }

        return database.logger.isAnyTextCacheModified();
    }

    void checkpoint() {

        if (filesReadOnly) {
            return;
        }

        boolean result = checkpointClose();

        if (result) {
            checkpointReopen();
        } else {
            database.logger.logSevereEvent(
                "checkpoint failed - see previous error", null);
        }
    }

    /**
     * Performs checkpoint including pre and post operations. Returns to the
     * same state as before the checkpoint.
     */
    void checkpoint(boolean defrag) {

        if (filesReadOnly) {
            return;
        }

        if (cache == null) {
            defrag = false;
        } else if (forceDefrag()) {
            defrag = true;
        }

        if (defrag) {
            try {
                defrag();
                database.sessionManager.resetLoggedSchemas();

                return;
            } catch (Exception e) {
                database.logger.logSevereEvent("defrag failed", e);

                // do normal checkpoint
            }
        }

        checkpoint();
    }

    /**
     * Performs checkpoint including pre and post operations. Returns to the
     * same state as before the checkpoint.
     */
    boolean checkpointClose() {

        if (filesReadOnly) {
            return true;
        }

        database.logger.logInfoEvent("checkpointClose start");
        synchLog();
        database.lobManager.synch();

        if (!database.txManager.isMVCC()) {
            database.lobManager.deleteUnusedLobs();
        }

        deleteOldDataFiles();

        try {
            writeScript(false);
        } catch (HsqlException e) {
            deleteNewScript();
            database.logger.logSevereEvent("checkpoint failed - recovered", e);

            return false;
        }

        try {
            if (cache != null) {
                cache.commitChanges();
                cache.backupFile(false);
            }
        } catch (Exception ee) {

            // backup failed perhaps due to lack of disk space
            deleteNewScript();
            deleteNewBackup();

            try {
                if (!cache.isFileOpen()) {
                    cache.open(false);
                }
            } catch (Exception e1) {}

            database.logger.logSevereEvent("checkpoint failed - recovered",
                                           ee);

            return false;
        }

        closeLog();
        properties.setProperty(HsqlDatabaseProperties.hsqldb_script_format,
                               database.logger.propScriptFormat);
        properties.setDBModified(HsqlDatabaseProperties.FILES_MODIFIED_NEW);
        deleteLog();
        renameNewScript();
        renameNewBackup();

        try {
            properties.setDBModified(
                HsqlDatabaseProperties.FILES_NOT_MODIFIED);
        } catch (Exception e) {}

        database.logger.logInfoEvent("checkpointClose end");

        return true;
    }

    boolean checkpointReopen() {

        if (filesReadOnly) {
            return true;
        }

        database.sessionManager.resetLoggedSchemas();

        try {
            if (cache != null) {
                cache.openShadowFile();
            }

            if (dbLogWriter != null) {
                openLog();
            }

            properties.setDBModified(HsqlDatabaseProperties.FILES_MODIFIED);
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    /**
     *  Writes out all the rows to a new file without fragmentation.
     */
    public void defrag() {

        if (cache.fileFreePosition == cache.initialFreePos) {
            return;
        }

        database.logger.logInfoEvent("defrag start");

        try {

// test
/*
            DataFileDefrag.checkAllTables(database);
*/

//
            synchLog();
            database.lobManager.synch();
            deleteOldDataFiles();

            DataFileDefrag dfd = cache.defrag();
        } catch (HsqlException e) {
            throw e;
        } catch (Throwable e) {
            database.logger.logSevereEvent("defrag failure", e);

            throw Error.error(ErrorCode.DATA_FILE_ERROR, e);
        }

// test
/*
        DataFileDefrag.checkAllTables(database);
*/

//
        database.logger.logInfoEvent("defrag end");
    }

    /**
     * Returns true if lost space is above the threshold percentage
     */
    boolean forceDefrag() {

        long limit = database.logger.propCacheDefragLimit
                     * cache.getFileFreePos() / 100;
        long lostSize = cache.freeBlocks.getLostBlocksSize();

        return limit > 0 && lostSize > limit;
    }

    /**
     *
     */
    boolean hasCache() {
        return cache != null;
    }

    /**
     * Responsible for creating the data file cache instance.
     */
    DataFileCache getCache() {

        if (cache == null) {
            cache = new DataFileCache(database, fileName);

            cache.open(filesReadOnly);
        }

        return cache;
    }

    void setLogSize(int megas) {
        maxLogSize = megas * 1024L * 1024;
    }

    /**
     * Write delay specifies the frequency of FileDescriptor.sync() calls.
     */
    int getWriteDelay() {
        return writeDelay;
    }

    void setWriteDelay(int delay) {

        writeDelay = delay;

        if (dbLogWriter != null && dbLogWriter.getWriteDelay() != delay) {
            dbLogWriter.forceSync();
            dbLogWriter.stop();
            dbLogWriter.setWriteDelay(delay);
            dbLogWriter.start();
        }
    }

    public void setIncrementBackup(boolean val) {

        if (cache != null) {
            cache.setIncrementBackup(val);
        }
    }

    /**
     * Various writeXXX() methods are used for logging statements.
     */
    void writeOtherStatement(Session session, String s) {

        try {
            dbLogWriter.writeOtherStatement(session, s);
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, logFileName);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            database.logger.setCheckpointRequired();
        }
    }

    void writeInsertStatement(Session session, Row row, Table t) {

        try {
            dbLogWriter.writeInsertStatement(session, row, t);
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, logFileName);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            database.logger.setCheckpointRequired();
        }
    }

    void writeDeleteStatement(Session session, Table t, Object[] row) {

        try {
            dbLogWriter.writeDeleteStatement(session, t, row);
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, logFileName);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            database.logger.setCheckpointRequired();
        }
    }

    void writeSequenceStatement(Session session, NumberSequence s) {

        try {
            dbLogWriter.writeSequenceStatement(session, s);
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, logFileName);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            database.logger.setCheckpointRequired();
        }
    }

    void writeCommitStatement(Session session) {

        try {
            dbLogWriter.writeCommitStatement(session);
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, logFileName);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            database.logger.setCheckpointRequired();
        }
    }

    void synchLog() {

        if (dbLogWriter != null) {
            dbLogWriter.forceSync();
        }
    }

    /**
     * Wrappers for openning-starting / stoping-closing the log file and
     * writer.
     */
    void openLog() {

        if (filesReadOnly) {
            return;
        }

        Crypto crypto = database.logger.getCrypto();

        try {
            if (crypto == null) {
                dbLogWriter = new ScriptWriterText(database, logFileName,
                                                   false, false, false);
            } else {
                dbLogWriter = new ScriptWriterEncode(database, logFileName,
                                                     crypto);
            }

            dbLogWriter.setWriteDelay(writeDelay);
            dbLogWriter.start();
        } catch (Exception e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, logFileName);
        }
    }

    synchronized void closeLog() {

        if (dbLogWriter != null) {
            database.logger.logDetailEvent("log close size: "
                                           + dbLogWriter.size());
            dbLogWriter.close();
        }
    }

    /**
     * Write the .script file as .script.new.
     */
    void writeScript(boolean full) {

        deleteNewScript();

        ScriptWriterBase scw;
        Crypto           crypto = database.logger.getCrypto();

        if (crypto == null) {
            boolean compressed = database.logger.propScriptFormat == 3;

            scw = new ScriptWriterText(database,
                                       scriptFileName
                                       + Logger.newFileExtension, full,
                                           compressed);
        } else {
            scw = new ScriptWriterEncode(database,
                                         scriptFileName
                                         + Logger.newFileExtension, full,
                                             crypto);
        }

        scw.writeAll();
        scw.close();

        scw = null;
    }

    /**
     * Performs all the commands in the .script file.
     */
    private void processScript() {

        ScriptReaderBase scr = null;

        try {
            Crypto crypto = database.logger.getCrypto();

            if (crypto == null) {
                boolean compressed = database.logger.propScriptFormat == 3;

                scr = new ScriptReaderText(database, scriptFileName,
                                           compressed);
            } else {
                scr = new ScriptReaderDecode(database, scriptFileName, crypto,
                                             false);
            }

            Session session =
                database.sessionManager.getSysSessionForScript(database);

            scr.readAll(session);
            scr.close();
        } catch (Throwable e) {
            if (scr != null) {
                scr.close();

                if (cache != null) {
                    cache.close(false);
                }

                database.logger.closeAllTextCaches(false);
            }

            database.logger.logWarningEvent("Script processing failure", e);

            if (e instanceof HsqlException) {
                throw (HsqlException) e;
            } else if (e instanceof IOException) {
                throw Error.error(ErrorCode.FILE_IO_ERROR, e);
            } else if (e instanceof OutOfMemoryError) {
                throw Error.error(ErrorCode.OUT_OF_MEMORY);
            } else {
                throw Error.error(ErrorCode.GENERAL_ERROR, e);
            }
        }
    }

    /**
     * Performs all the commands in the .log file.
     */
    private void processLog() {

        if (fa.isStreamElement(logFileName)) {
            ScriptRunner.runScript(database, logFileName);
        }
    }

    void deleteOldDataFiles() {

        if (database.logger.isStoredFileAccess()) {
            return;
        }

        try {
            File   file = new File(database.getCanonicalPath());
            File[] list = file.getParentFile().listFiles();

            for (int i = 0; i < list.length; i++) {
                if (list[i].getName().startsWith(file.getName())
                        && list[i].getName().endsWith(
                            Logger.oldFileExtension)) {
                    list[i].delete();
                }
            }
        } catch (Throwable t) {}
    }

    void deleteOldTempFiles() {

        try {
            if (database.logger.tempDirectoryPath == null) {
                return;
            }

            File   file = new File(database.logger.tempDirectoryPath);
            File[] list = file.listFiles();

            for (int i = 0; i < list.length; i++) {
                list[i].delete();
            }
        } catch (Throwable t) {}
    }

    void deleteTempFileDirectory() {

        try {
            if (database.logger.tempDirectoryPath == null) {
                return;
            }

            File file = new File(database.logger.tempDirectoryPath);

            file.delete();
        } catch (Throwable t) {}
    }
}
