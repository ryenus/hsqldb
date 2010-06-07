/*
 * For work developed by the HSQL Development Group:
 *
 * Copyright (c) 2001-2010, The HSQL Development Group
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
 *
 *
 *
 * For work originally developed by the Hypersonic SQL Group:
 *
 * Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 */


package org.hsqldb.persist;

import java.io.File;
import java.io.IOException;

import org.hsqldb.Database;
import org.hsqldb.DatabaseURL;
import org.hsqldb.HsqlException;
import org.hsqldb.NumberSequence;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FileAccess;
import org.hsqldb.lib.FileArchiver;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.Iterator;
import org.hsqldb.scriptio.ScriptReaderBase;
import org.hsqldb.scriptio.ScriptReaderDecode;
import org.hsqldb.scriptio.ScriptReaderText;
import org.hsqldb.scriptio.ScriptWriterBase;
import org.hsqldb.scriptio.ScriptWriterEncode;
import org.hsqldb.scriptio.ScriptWriterText;

// fredt@users 20020215 - patch 1.7.0 by fredt
// to move operations on the database.properties files to new
// class HsqlDatabaseProperties
// fredt@users 20020220 - patch 488200 by xclayl@users - throw exception
// throw addded to all methods relying on file io
// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
// fredt@users 20020405 - patch 1.7.0 by fredt - no change in db location
// because important information about the database is now stored in the
// *.properties file, all database files should be in the same folder as the
// *.properties file
// tony_lai@users 20020820 - export hsqldb.log_size to .properties file
// tony_lai@users 20020820 - changes to shutdown compact to save memory
// fredt@users 20020910 - patch 1.7.1 by Nitin Chauhan - code improvements
// fredt@users 20021208 - ongoing revamp
// fredt@users 20021212 - do not rewrite the *.backup file if the *.data
// file has not been updated in the current session.
// boucherb@users 20030510 - patch 1.7.2 consolidated all periodic database
// tasks in one timed task queue
// fredt@users - 20050102 patch 1.8.0 - refactoring and clearer separation of concerns
/*
    - if props.modified, use .backup file - .data file is ready
    - read .script file and set index roots


    - if .data file is modified, use .backup with .data file flag for increment backup - .data file is ready
*/

/**
 *  This class is responsible for managing the database files. An HSQLDB database
 *  consists of a .properties file, a .script file (contains an SQL script),
 *  a .data file (contains data of cached tables) a .backup file
 *  and a .log file.<p>
 *  When using TEXT tables, a data source for each table is also present.<p>
 *
 *  Notes on OpenOffice.org integration.
 *
 *  A Storage API is used when HSQLDB is integrated into OpenOffice.org. All
 *  file operations on the 4 main files are performed by OOo, which integrates
 *  the contents of these files into its database file. The script format is
 *  always TEXT in this case.
 *
 * Extensively rewritten and extended in successive versions of HSQLDB.
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since Hypersonic SQL
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
    private int                    scriptFormat;
    private DataFileCache          cache;

    // We are using persist.Logger-instance-specific FrameworkLogger
    // because it is Database-instance specific.
    // If add any static level logging, should instantiate a standard,
    // context-agnostic FrameworkLogger for that purpose.
    Log(Database db) {

        database   = db;
        fa         = db.logger.getFileAccess();
        fileName   = db.getPath();
        properties = db.getProperties();
    }

    void initParams() {

        // Allows the user to set log size in the properties file.
        maxLogSize     = database.logger.propLogSize * 1024L * 1024;
        scriptFormat   = 0;
        writeDelay     = database.logger.propWriteDelay;
        filesReadOnly  = database.isFilesReadOnly();
        scriptFileName = fileName + ".script";
        logFileName    = fileName + ".log";
    }

    /**
     * When opening a database, the hsqldb.compatible_version property is
     * used to determine if this version of the engine is equal to or greater
     * than the earliest version of the engine capable of opening that
     * database.<p>
     *
     * @throws  HsqlException
     */
    void open() {

        initParams();

        int state = properties.getDBModified();

        switch (state) {

            case HsqlDatabaseProperties.FILES_MODIFIED :
                deleteNewAndOldFiles();
                processScript();
                processLog();
                close(false);

                if (cache != null) {
                    cache.open(filesReadOnly);
                }

                reopenAllTextCaches();
                break;

            case HsqlDatabaseProperties.FILES_NEW :
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

                if (isAnyCacheModified()) {
                    properties.setDBModified(
                        HsqlDatabaseProperties.FILES_MODIFIED);
                    close(false);

                    if (cache != null) {
                        cache.open(filesReadOnly);
                    }

                    reopenAllTextCaches();
                }
                break;
        }

        openLog();

        if (!filesReadOnly) {
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
        deleteNewAndOldFiles();
        writeScript(script);
        closeAllTextCaches(script);

        if (cache != null) {
            cache.close(true);
        }

        // set this one last to save the props
        properties.setDBModified(HsqlDatabaseProperties.FILES_NEW);
        deleteLog();

        if (cache != null) {
            if (script) {
                cache.deleteFile();
                cache.deleteBackup();
            } else {
                cache.backupFile();
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

        synchLog();

        if (cache != null) {
            cache.close(false);
        }

        closeAllTextCaches(false);
        closeLog();
    }

    /**
     * Deletes the leftovers from any previous unfinished operations.
     */
    void deleteNewAndOldFiles() {

        fa.removeElement(fileName + ".data" + ".old");
        fa.removeElement(fileName + ".data" + ".new");
        fa.removeElement(fileName + ".backup" + ".new");
        fa.removeElement(scriptFileName + ".new");
    }

    void deleteBackup() {
        fa.removeElement(fileName + ".backup");
    }

    void deleteData() {
        fa.removeElement(fileName + ".data");
    }

    void backupData() throws IOException {

        if (database.logger.propIncrementBackup) {
            fa.removeElement(fileName + ".backup");

            return;
        }

        if (fa.isStreamElement(fileName + ".data")) {
            FileArchiver.archive(fileName + ".data", fileName + ".backup.new",
                                 database.logger.getFileAccess(),
                                 FileArchiver.COMPRESSION_ZIP);
        }
    }

    void renameNewDataFile() {

        if (fa.isStreamElement(fileName + ".data.new")) {
            fa.renameElement(fileName + ".data.new", fileName + ".data");
        }
    }

    void renameNewBackup() {

        // required for inc backup
        fa.removeElement(fileName + ".backup");

        if (fa.isStreamElement(fileName + ".backup.new")) {
            fa.renameElement(fileName + ".backup.new", fileName + ".backup");
        }
    }

    void renameNewScript() {

        if (fa.isStreamElement(scriptFileName + ".new")) {
            fa.renameElement(scriptFileName + ".new", scriptFileName);
        }
    }

    void deleteNewScript() {
        fa.removeElement(scriptFileName + ".new");
    }

    void deleteNewBackup() {
        fa.removeElement(fileName + ".backup.new");
    }

    void deleteLog() {
        fa.removeElement(logFileName);
    }

    /**
     * Checks all the caches and returns true if the modified flag is set for any
     */
    boolean isAnyCacheModified() {

        if (cache != null && cache.isFileModified()) {
            return true;
        }

        return isAnyTextCacheModified();
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

                return;
            } catch (Exception e) {
                database.logger.checkpointDisabled = true;

                return;
            }
        }

        boolean result = checkpointClose();

        if (result) {
            checkpointReopen();
        }
    }

    /**
     * Performs checkpoint including pre and post operations. Returns to the
     * same state as before the checkpoint.
     */
    boolean checkpointClose() {

        if (filesReadOnly) {
            return true;
        }

        deleteNewAndOldFiles();

        try {
            writeScript(false);
        } catch (HsqlException e) {
            deleteNewScript();

            return false;
        }

        try {
            if (cache != null) {
                cache.close(true);
                cache.backupFile();
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

            return false;
        }

        properties.setDBModified(HsqlDatabaseProperties.FILES_NEW);
        closeLog();
        deleteLog();
        renameNewScript();
        renameNewBackup();

        try {
            properties.setDBModified(
                HsqlDatabaseProperties.FILES_NOT_MODIFIED);
        } catch (Exception e) {}

        return true;
    }

    boolean checkpointReopen() {

        if (filesReadOnly) {
            return true;
        }

        try {
            if (cache != null) {
                cache.open(false);
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
/*            {
                Session session = database.getSessionManager().getSysSession();
                HsqlArrayList allTables =
                    database.schemaManager.getAllTables();

                for (int i = 0, tSize = allTables.size(); i < tSize; i++) {
                    Table t     = (Table) allTables.get(i);
                    int   count = 0;

                    if (t.getTableType() == TableBase.CACHED_TABLE) {
                        RowIterator it = t.rowIterator(session);

                        for (; it.hasNext(); count++) {
                            CachedObject row = it.getNextRow();
                        }

                        System.out.println("table " + t.getName().name + " "
                                           + count);
                    }
                }
            }
*/

//
            DataFileDefrag dfd = cache.defrag();
        } catch (HsqlException e) {
            database.logger.logSevereEvent("defrag failure", e);

            throw (HsqlException) e;
        } catch (Throwable e) {
            database.logger.logSevereEvent("defrag failure", e);

            throw Error.error(ErrorCode.DATA_FILE_ERROR, e);
        }

// test
/*
        {
            Session session = database.getSessionManager().getSysSession();
            HsqlArrayList allTables = database.schemaManager.getAllTables();

            for (int i = 0, tSize = allTables.size(); i < tSize; i++) {
                Table t     = (Table) allTables.get(i);
                int   count = 0;

                if (t.getTableType() == Table.CACHED_TABLE) {
                    RowIterator it = t.rowIterator(session);

                    for (; it.hasNext(); count++) {
                        CachedObject row = it.getNextRow();
                    }

                    System.out.println("table " + t.getName().name + " "
                                       + count);
                }
            }
        }
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
     * Responsible for creating the cache instance.
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

    int getScriptType() {
        return scriptFormat;
    }

    /**
     * Write delay specifies the frequency of FileDescriptor.sync() calls.
     */
    int getWriteDelay() {
        return writeDelay;
    }

    void setWriteDelay(int delay) {

        writeDelay = delay;

        if (dbLogWriter != null) {
            dbLogWriter.forceSync();
            dbLogWriter.stop();
            dbLogWriter.setWriteDelay(delay);
            dbLogWriter.start();
        }
    }

    public void setIncrementBackup(boolean val) {

        if (cache != null) {
            cache.setIncrementBackup(val);

            cache.fileModified = true;
        }
    }

    /**
     * Various writeXXX() methods are used for logging statements.
     */
    void writeStatement(Session session, String s) {

        try {
            dbLogWriter.writeLogStatement(session, s);
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, logFileName);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            database.logger.checkpointRequired = true;
        }
    }

    void writeInsertStatement(Session session, Table t, Object[] row) {

        try {
            dbLogWriter.writeInsertStatement(session, t, row);
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, logFileName);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            database.logger.checkpointRequired = true;
        }
    }

    void writeDeleteStatement(Session session, Table t, Object[] row) {

        try {
            dbLogWriter.writeDeleteStatement(session, t, row);
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, logFileName);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            database.logger.checkpointRequired = true;
        }
    }

    void writeSequenceStatement(Session session, NumberSequence s) {

        try {
            dbLogWriter.writeSequenceStatement(session, s);
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, logFileName);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            database.logger.checkpointRequired = true;
        }
    }

    void writeCommitStatement(Session session) {

        try {
            dbLogWriter.writeCommitStatement(session);
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, logFileName);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            database.logger.checkpointRequired = true;
        }
    }

    void synchLog() {

        if (dbLogWriter != null) {
            dbLogWriter.sync();
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
            scw = new ScriptWriterText(database, scriptFileName + ".new",
                                       full, true, false);
        } else {
            scw = new ScriptWriterEncode(database, scriptFileName + ".new",
                                         full, crypto);
        }

        scw.writeAll();
        scw.close();
    }

    /**
     * Performs all the commands in the .script file.
     */
    private void processScript() {

        ScriptReaderBase scr = null;

        try {
            if (fa.isStreamElement(scriptFileName)) {
                Crypto crypto = database.logger.getCrypto();

                if (crypto == null) {
                    scr = new ScriptReaderText(database, scriptFileName);
                } else {
                    scr = new ScriptReaderDecode(database, scriptFileName,
                                                 crypto);
                }

                Session session =
                    database.sessionManager.getSysSessionForScript(database);

                scr.readAll(session);
                scr.close();
            }
        } catch (Throwable e) {
            if (scr != null) {
                scr.close();

                if (cache != null) {
                    cache.close(false);
                }

                closeAllTextCaches(false);
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

// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP) - text tables
    private HashMap textCacheList = new HashMap();

    DataFileCache openTextCache(Table table, String source,
                                boolean readOnlyData, boolean reversed) {

        closeTextCache(table);

        if (database.getType() != DatabaseURL.S_RES
                && !properties.isPropertyTrue(
                    HsqlDatabaseProperties.textdb_allow_full_path)) {
            if (source.indexOf("..") != -1) {
                throw (Error.error(ErrorCode.ACCESS_IS_DENIED, source));
            }

            String path = new File(
                new File(
                    database.getPath()
                    + ".properties").getAbsolutePath()).getParent();

            if (path != null) {
                source = path + File.separator + source;
            }
        }

        TextCache c = new TextCache(table, source);

        c.open(readOnlyData || filesReadOnly);
        textCacheList.put(table.getName(), c);

        return c;
    }

    void closeTextCache(Table table) {

        TextCache c = (TextCache) textCacheList.remove(table.getName());

        if (c != null) {
            try {
                c.close(true);
            } catch (HsqlException e) {}
        }
    }

    private void closeAllTextCaches(boolean script) {

        Iterator it = textCacheList.values().iterator();

        while (it.hasNext()) {
            TextCache textCache = ((TextCache) it.next());

            // use textCache.table to cover both cach and table readonly
            if (script && !textCache.table.isDataReadOnly()) {
                textCache.purge();
            } else {
                textCache.close(true);
            }
        }
    }

    private void reopenAllTextCaches() {

        Iterator it = textCacheList.values().iterator();

        while (it.hasNext()) {
            ((TextCache) it.next()).reopen();
        }
    }

    private boolean isAnyTextCacheModified() {

        Iterator it = textCacheList.values().iterator();

        while (it.hasNext()) {
            if (((TextCache) it.next()).isFileModified()) {
                return true;
            }
        }

        return false;
    }
}
