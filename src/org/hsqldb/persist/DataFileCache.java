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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hsqldb.Database;
import org.hsqldb.HsqlException;
import org.hsqldb.Session;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FileAccess;
import org.hsqldb.lib.FileArchiver;
import org.hsqldb.lib.IntIndex;
import org.hsqldb.map.BitMap;
import org.hsqldb.rowio.RowInputBinaryDecode;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputBinaryEncode;
import org.hsqldb.rowio.RowOutputInterface;

/**
 * Acts as a manager for CACHED table persistence.<p>
 *
 * This contains the top level functionality. Provides file management services
 * and access.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.1
 * @since 1.7.2
 */
public class DataFileCache {

    interface Flags {

        int FLAG_ISSHADOWED = 1;
        int FLAG_ISSAVED    = 2;
        int FLAG_ROWINFO    = 3;
        int FLAG_200        = 4;
        int FLAG_HX         = 5;
        int FLAG_251        = 6;
    }

    /**
     * file format fields
     */
    interface Positions {

        int LONG_EMPTY_SIZE      = 4;                                          // empty space size
        int LONG_FREE_POS        = 12;                                         // where iFreePos is saved
        int INT_SPACE_PROPS      = 20;                                         // (space size << 16) + scale
        int INT_SPACE_LIST_POS   = 24;                                         // space list
        int INT_FLAGS            = 28;
        int LONG_TIMESTAMP       = 32;                                         // db open tx timestamp
        int MIN_INITIAL_FREE_POS = 64;                                         // not used up to this
        int MAX_INITIAL_FREE_POS = DataSpaceManager.fixedDiskBlockSize * 2;    // not used up to this
    }

    protected FileAccess fa;

    //
    public DataSpaceManager spaceManager;
    static final int        initIOBufferSize = 4096;

    //
    protected String   dataFileName;
    protected String   backupFileName;
    protected Database database;
    protected boolean  logEvents = true;

    /**
     * this flag is used externally to determine if a backup is required
     */
    protected boolean fileModified;
    protected boolean cacheModified;
    protected int     dataFileScale;
    protected int     dataFileSpace;

    // post opening constant fields
    protected boolean cacheReadonly;

    //
    protected int cachedRowPadding;

    //
    protected long lostSpaceSize;
    protected long spaceManagerPosition;
    protected long fileStartFreePosition;
    protected int  storeCount;

    // reusable input / output streams
    protected RowInputInterface rowIn;
    public RowOutputInterface   rowOut;

    //
    public long maxDataFileSize;

    //
    boolean is251;

    //
    protected RandomAccessInterface dataFile;
    protected volatile long         fileFreePosition;
    protected int                   maxCacheRows;     // number of Rows
    protected long                  maxCacheBytes;    // number of bytes
    protected Cache                 cache;

    //
    private RAShadowFile shadowFile;

    //
    ReadWriteLock lock      = new ReentrantReadWriteLock();
    Lock          readLock  = lock.readLock();
    Lock          writeLock = lock.writeLock();

    public DataFileCache(Database db, String baseFileName) {

        initParams(db, baseFileName, false);

        cache = new Cache(this);
    }

    /**
     * used for defrag
     */
    public DataFileCache(Database db, String baseFileName, boolean defrag) {

        initParams(db, baseFileName, true);

        cache = new Cache(this);

        try {
            dataFile = new RAFileSimple(database.logger, dataFileName, "rw");
        } catch (Throwable t) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, t);
        }

        initNewFile();
        initBuffers();

        if (dataFileSpace > 0) {
            spaceManager = new DataSpaceManagerBlocks(this);
        } else {
            spaceManager = new DataSpaceManagerSimple(this, false);
        }
    }

    /**
     * initial external parameters are set here.
     */
    protected void initParams(Database database, String baseFileName,
                              boolean defrag) {

        this.database    = database;
        dataFileName     = baseFileName + Logger.dataFileExtension;
        backupFileName   = baseFileName + Logger.backupFileExtension;
        fa               = database.logger.getFileAccess();
        dataFileScale    = database.logger.getDataFileScale();
        dataFileSpace    = database.logger.getDataFileSpace();
        cachedRowPadding = dataFileScale;

        if (dataFileScale < 8) {
            cachedRowPadding = 8;
        }

        cacheReadonly = database.isFilesReadOnly();
        maxCacheRows  = database.logger.getCacheMaxRows();
        maxCacheBytes = database.logger.getCacheSize();
        maxDataFileSize = (long) Integer.MAX_VALUE * dataFileScale
                          * database.logger.getDataFileFactor();

        if (defrag) {
            dataFileName   = dataFileName + Logger.newFileExtension;
            backupFileName = backupFileName + Logger.newFileExtension;
            maxCacheRows   = 1024;
            maxCacheBytes  = 1024 * 4096;
        }
    }

    /**
     * Opens the *.data file for this cache, setting the variables that
     * allow access to the particular database version of the *.data file.
     */
    public void open(boolean readonly) {

        logInfoEvent("dataFileCache open start");

        try {
            boolean isNio = database.logger.propNioDataFile;
            int     fileType;

            if (database.isFilesInJar()) {
                fileType = RAFile.DATA_FILE_JAR;
            } else if (isNio) {
                fileType = RAFile.DATA_FILE_NIO;
            } else {
                fileType = RAFile.DATA_FILE_RAF;
            }

            if (readonly || database.isFilesInJar()) {
                dataFile = RAFile.newScaledRAFile(database, dataFileName,
                                                  true, fileType);

                int flags = getFlags();

                if (BitMap.isSet(flags, Flags.FLAG_HX)) {
                    throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
                }

                dataFile.seek(Positions.LONG_FREE_POS);

                fileFreePosition = dataFile.readLong();

                dataFile.seek(Positions.INT_SPACE_LIST_POS);

                spaceManagerPosition = (long) dataFile.readInt()
                                       * DataSpaceManager.fixedDiskBlockSize;

                initBuffers();

                spaceManager = new DataSpaceManagerSimple(this, true);

                return;
            }

            boolean preexists     = fa.isStreamElement(dataFileName);
            boolean isIncremental = true;
            boolean isSaved       = false;
            boolean doRestore     = false;

            if (preexists) {
                dataFile = new RAFileSimple(database.logger, dataFileName,
                                            "r");

                long    length       = dataFile.length();
                boolean wrongVersion = false;

                if (length > Positions.LONG_TIMESTAMP) {
                    int flags = getFlags();

                    isSaved       = BitMap.isSet(flags, Flags.FLAG_ISSAVED);
                    isIncremental = BitMap.isSet(flags, Flags.FLAG_ISSHADOWED);
                    is251         = BitMap.isSet(flags, Flags.FLAG_251);

                    if (BitMap.isSet(flags, Flags.FLAG_HX)) {
                        wrongVersion = true;
                    }
                } else {
                    preexists = false;
                }

                if (isSaved && is251) {
                    dataFile.seek(Positions.LONG_TIMESTAMP);

                    long timestamp = dataFile.readLong();

                    if (timestamp > database.logger.getFilesTimestamp()) {
                        doRestore = true;
                    }
                }

                dataFile.close();

                if (wrongVersion) {
                    throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
                }

                if (!database.logger.propLargeData) {
                    if (length > (maxDataFileSize / 8) * 7) {
                        database.logger.propLargeData = true;
                        maxDataFileSize =
                            (long) Integer.MAX_VALUE * dataFileScale
                            * database.logger.getDataFileFactor();
                    }
                }

                if (length > maxDataFileSize) {
                    throw Error.error(ErrorCode.DATA_FILE_IS_FULL,
                                      String.valueOf(maxDataFileSize));
                }
            }

            if (preexists) {
                if (isSaved) {
                    boolean existsBackup = fa.isStreamElement(backupFileName);

                    if (existsBackup) {
                        logInfoEvent(
                            "data file was not modified but inc backup exists");

                        if (doRestore) {
                            restoreBackupIncremental();
                        }
                    }

                    deleteBackupFile();
                } else {
                    boolean restored;

                    if (isIncremental) {
                        restored = restoreBackupIncremental();
                    } else {
                        restored = restoreBackup();
                    }

                    if (!restored) {
                        database.logger.logSevereEvent(
                            "DataFileCache data file modified but no backup exists",
                            null);

                        throw Error.error(ErrorCode.DATA_FILE_BACKUP_MISMATCH);
                    }
                }
            }

            dataFile = RAFile.newScaledRAFile(database, dataFileName,
                                              readonly, fileType);

            if (preexists) {
                dataFile.seek(Positions.LONG_EMPTY_SIZE);

                lostSpaceSize = dataFile.readLong();

                dataFile.seek(Positions.LONG_FREE_POS);

                fileFreePosition      = dataFile.readLong();
                fileStartFreePosition = fileFreePosition;

                dataFile.seek(Positions.INT_SPACE_PROPS);

                int spaceProps = dataFile.readInt();

                setSpaceProps(spaceProps);
                dataFile.seek(Positions.INT_SPACE_LIST_POS);

                spaceManagerPosition = (long) dataFile.readInt()
                                       * DataSpaceManager.fixedDiskBlockSize;

                int flags = getFlags();

                flags = BitMap.set(flags, Flags.FLAG_ISSHADOWED);
                flags = BitMap.set(flags, Flags.FLAG_ISSAVED);

                setFlags(flags);
            } else {
                initNewFile();
            }

            initBuffers();

            fileModified  = false;
            cacheModified = false;

            if (dataFileSpace > 0) {
                spaceManager = new DataSpaceManagerBlocks(this);
            } else {
                spaceManager = new DataSpaceManagerSimple(this, false);
            }

            if (!preexists) {
                reset();
            }

            openShadowFile();
            logInfoEvent("dataFileCache open end");
        } catch (HsqlException e) {
            throw e;
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.open", t);
            release();

            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_DataFileCache_open, new String[] {
                t.toString(), dataFileName
            });
        }
    }

    void setSpaceProps(int spaceProps) {

        if (spaceProps == 0) {
            spaceProps = dataFileScale | (dataFileSpace << 16);

            try {
                dataFile.seek(Positions.INT_SPACE_PROPS);
                dataFile.writeInt(spaceProps);
                dataFile.synch();
            } catch (Throwable t) {
                throw Error.error(ErrorCode.FILE_IO_ERROR, t);
            }

            return;
        }

        dataFileScale = spaceProps & 0xffff;
        dataFileSpace = spaceProps >>> 16;

        database.logger.setDataFileScaleNoCheck(dataFileScale);
        database.logger.setDataFileSpace(dataFileSpace);
    }

    void initNewFile() {

        try {
            int initialFreePos = Positions.MAX_INITIAL_FREE_POS;

            if (dataFileSpace == 0) {
                initialFreePos = Positions.MIN_INITIAL_FREE_POS;

                if (initialFreePos < dataFileScale) {
                    initialFreePos = dataFileScale;
                }
            }

            fileFreePosition      = initialFreePos;
            fileStartFreePosition = initialFreePos;

            dataFile.seek(Positions.LONG_FREE_POS);
            dataFile.writeLong(fileFreePosition);

            int spaceProps = dataFileScale | (dataFileSpace << 16);

            dataFile.seek(Positions.INT_SPACE_PROPS);
            dataFile.writeInt(spaceProps);
            dataFile.seek(Positions.LONG_TIMESTAMP);
            dataFile.writeLong(database.logger.getFilesTimestamp());

            // set shadowed flag;
            int flags = 0;

            flags = BitMap.set(flags, Flags.FLAG_ISSHADOWED);
            flags = BitMap.set(flags, Flags.FLAG_ISSAVED);
            flags = BitMap.set(flags, Flags.FLAG_200);
            flags = BitMap.set(flags, Flags.FLAG_251);

            setFlags(flags);

            is251 = true;
        } catch (Throwable t) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, t);
        }
    }

    private void openShadowFile() {

        shadowFile = new RAShadowFile(database.logger, dataFile,
                                      backupFileName, fileFreePosition,
                                      1 << 14);
    }

    /**
     * Restores a compressed backup or the .data file.
     */
    private boolean restoreBackup() {

        try {
            FileAccess fileAccess = database.logger.getFileAccess();

            deleteBackupFile();

            if (fileAccess.isStreamElement(backupFileName)) {
                FileArchiver.unarchive(backupFileName, dataFileName,
                                       fileAccess,
                                       FileArchiver.COMPRESSION_ZIP);

                return true;
            }

            return false;
        } catch (Throwable t) {
            database.logger.logSevereEvent("DataFileCache.restoreBackup", t);

            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_Message_Pair, new String[] {
                t.toString(), backupFileName
            });
        }
    }

    /**
     * Restores in from an incremental backup
     */
    private boolean restoreBackupIncremental() {

        try {
            FileAccess fileAccess = database.logger.getFileAccess();

            if (fileAccess.isStreamElement(backupFileName)) {
                RAShadowFile.restoreFile(database, backupFileName,
                                         dataFileName);
                deleteBackupFile();

                return true;
            }

            // this is an anomaly where no backup exists but .data file
            // modified flag has been set
            return false;
        } catch (Throwable e) {
            database.logger.logSevereEvent(
                "DataFileCache.restoreBackupIncremental", e);

            throw Error.error(ErrorCode.FILE_IO_ERROR, e);
        }
    }

    /**
     *  Abandons changed rows and closes the .data file.
     */
    public void release() {

        writeLock.lock();

        try {
            if (dataFile == null) {
                return;
            }

            if (shadowFile != null) {
                shadowFile.close();

                shadowFile = null;
            }

            dataFile.close();
            logDetailEvent("dataFileCache file closed");

            dataFile = null;
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.release", t);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     *  Writes out all cached rows that have been modified and the
     *  free position pointer for the *.data file and then closes the file.
     */
    public void close() {

        writeLock.lock();

        try {
            if (dataFile == null) {
                return;
            }

            reset();
            dataFile.close();
            logDetailEvent("dataFileCache file close end");

            dataFile = null;
        } catch (HsqlException e) {
            throw e;
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.close", t);

            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_DataFileCache_close, new String[] {
                t.toString(), dataFileName
            });
        } finally {
            writeLock.unlock();
        }
    }

    protected void clear() {

        writeLock.lock();

        try {
            cache.clear();
        } finally {
            writeLock.unlock();
        }
    }

    public void adjustStoreCount(int adjust) {

        writeLock.lock();

        try {
            storeCount += adjust;
        } finally {
            writeLock.unlock();
        }
    }

    public void reopen() {

        writeLock.lock();

        try {
            openShadowFile();
            spaceManager.initialiseSpaces();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Commits all the changes to the file
     */
    public void reset() {

        writeLock.lock();

        try {
            if (cacheReadonly) {
                return;
            }

            logInfoEvent("dataFileCache commit start");
            spaceManager.reset();
            cache.saveAll();

            // set empty
            long lostSize = spaceManager.getLostBlocksSize();

            dataFile.seek(Positions.LONG_EMPTY_SIZE);
            dataFile.writeLong(lostSize);

            // set end
            dataFile.seek(Positions.LONG_FREE_POS);
            dataFile.writeLong(fileFreePosition);

            // set space props
            int spaceProps = dataFileScale | (dataFileSpace << 16);

            dataFile.seek(Positions.INT_SPACE_PROPS);
            dataFile.writeInt(spaceProps);

            // set space list
            int pos = (int) (spaceManagerPosition
                             / DataSpaceManager.fixedDiskBlockSize);

            dataFile.seek(Positions.INT_SPACE_LIST_POS);
            dataFile.writeInt(pos);

            if (is251) {
                dataFile.seek(Positions.LONG_TIMESTAMP);
                dataFile.writeLong(database.logger.getFilesTimestamp());
            }

            // set saved flag and sync file
            setFlag(Flags.FLAG_ISSAVED, true);
            logDetailEvent("file sync end");

            fileModified          = false;
            cacheModified         = false;
            fileStartFreePosition = fileFreePosition;

            if (shadowFile != null) {
                shadowFile.close();

                shadowFile = null;
            }

            logInfoEvent("dataFileCache commit end");
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.reset commit", t);

            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_DataFileCache_close, new String[] {
                t.toString(), dataFileName
            });
        } finally {
            writeLock.unlock();
        }
    }

    protected void initBuffers() {

        if (rowOut == null) {
            rowOut = new RowOutputBinaryEncode(database.logger.getCrypto(),
                                               initIOBufferSize,
                                               cachedRowPadding);
        }

        if (rowIn == null) {
            rowIn = new RowInputBinaryDecode(database.logger.getCrypto(),
                                             new byte[initIOBufferSize]);
        }
    }

    DataFileDefrag defrag(Session session) {

        writeLock.lock();

        try {
            cache.saveAll();

            DataFileDefrag dfd = new DataFileDefrag(database, this);

            dfd.process(session);

            return dfd;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Used when a row is deleted as a result of some DML or DDL statement.
     * Removes the row from the cache data structures.
     */
    public void remove(CachedObject object) {
        release(object.getPos());
    }

    public void removePersistence(CachedObject object) {}

    public void add(CachedObject object, boolean keep) {

        writeLock.lock();

        try {
            cacheModified = true;

            cache.put(object);

            if (keep) {
                object.keepInMemory(true);
            }

            if (object.getStorageSize() > initIOBufferSize) {
                rowOut.reset(object.getStorageSize());
            }
        } finally {
            writeLock.unlock();
        }
    }

    public CachedObject get(CachedObject object, PersistentStore store,
                            boolean keep) {

        readLock.lock();

        long pos;

        try {
            if (object.isInMemory()) {
                if (keep) {
                    object.keepInMemory(true);
                }

                return object;
            }

            pos = object.getPos();

            if (pos < 0) {
                return null;
            }

            object = cache.get(pos);

            if (object != null) {
                if (keep) {
                    object.keepInMemory(true);
                }

                return object;
            }
        } finally {
            readLock.unlock();
        }

        return getFromFile(pos, store, keep);
    }

    public CachedObject get(long pos, int size, PersistentStore store,
                            boolean keep) {

        CachedObject object;

        if (pos < 0) {
            return null;
        }

        readLock.lock();

        try {
            object = cache.get(pos);

            if (object != null) {
                if (keep) {
                    object.keepInMemory(true);
                }

                return object;
            }
        } finally {
            readLock.unlock();
        }

        return getFromFile(pos, size, store, keep);
    }

    public CachedObject get(long pos, PersistentStore store, boolean keep) {

        CachedObject object;

        if (pos < 0) {
            return null;
        }

        readLock.lock();

        try {
            object = cache.get(pos);

            if (object != null) {
                if (keep) {
                    object.keepInMemory(true);
                }

                return object;
            }
        } finally {
            readLock.unlock();
        }

        return getFromFile(pos, store, keep);
    }

    private CachedObject getFromFile(long pos, PersistentStore store,
                                     boolean keep) {

        CachedObject object = null;

        writeLock.lock();

        try {
            object = cache.get(pos);

            if (object != null) {
                if (keep) {
                    object.keepInMemory(true);
                }

                return object;
            }

            for (int j = 0; j < 2; j++) {
                try {
                    readObject(pos);

                    object = store.get(rowIn);

                    if (object == null) {
                        throw Error.error(ErrorCode.GENERAL_IO_ERROR,
                                          "position " + pos);
                    }

                    break;
                } catch (Throwable t) {
                    if (t instanceof OutOfMemoryError) {
                        cache.clearUnchanged();

                        if (j > 0) {
                            logInfoEvent(dataFileName
                                         + " getFromFile out of mem, pos: "
                                         + pos);

                            HsqlException ex =
                                Error.error(ErrorCode.OUT_OF_MEMORY, t);

                            ex.info = rowIn;

                            throw ex;
                        }
                    } else if (t instanceof HsqlException) {
                        ((HsqlException) t).info = rowIn;

                        throw (HsqlException) t;
                    } else {
                        HsqlException ex =
                            Error.error(ErrorCode.GENERAL_IO_ERROR, t);

                        ex.info = rowIn;

                        throw ex;
                    }
                }
            }

            if (object == null) {
                throw Error.error(ErrorCode.DATA_FILE_ERROR);
            }

            // for text tables with empty rows at the beginning,
            // pos may move forward in readObject
            cache.put(object);

            if (keep) {
                object.keepInMemory(true);
            }

            return object;
        } catch (HsqlException e) {
            logSevereEvent(dataFileName + " getFromFile failed " + pos, e);

            throw e;
        } finally {
            writeLock.unlock();
        }
    }

    private CachedObject getFromFile(long pos, int size,
                                     PersistentStore store, boolean keep) {

        CachedObject object = null;

        writeLock.lock();

        try {
            object = cache.get(pos);

            if (object != null) {
                if (keep) {
                    object.keepInMemory(true);
                }

                return object;
            }

            for (int j = 0; j < 2; j++) {
                try {
                    readObject(pos, size);

                    object = store.get(rowIn);

                    break;
                } catch (OutOfMemoryError err) {
                    cache.clearUnchanged();
                    System.gc();

                    if (j > 0) {
                        logSevereEvent(dataFileName
                                       + " getFromFile out of mem "
                                       + pos, err);

                        throw err;
                    }
                }
            }

            if (object == null) {
                throw Error.error(ErrorCode.DATA_FILE_ERROR);
            }

            cache.putUsingReserve(object);

            if (keep) {
                object.keepInMemory(true);
            }

            return object;
        } catch (HsqlException e) {
            logSevereEvent(dataFileName + " getFromFile failed " + pos, e);

            throw e;
        } finally {
            writeLock.unlock();
        }
    }

    RowInputInterface getRaw(long pos) {

        writeLock.lock();

        try {
            readObject(pos);

            return rowIn;
        } finally {
            writeLock.unlock();
        }
    }

    private void readObject(long pos) {

        try {
            dataFile.seek(pos * dataFileScale);

            int size = dataFile.readInt();

            rowIn.resetRow(pos, size);
            dataFile.read(rowIn.getBuffer(), 4, size - 4);
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.readObject", t, pos);

            HsqlException ex = Error.error(ErrorCode.DATA_FILE_ERROR, t);

            if (rowIn.getFilePosition() != pos) {
                rowIn.resetRow(pos, 0);
            }

            ex.info = rowIn;

            throw ex;
        }
    }

    protected void readObject(long pos, int size) {

        try {
            rowIn.resetBlock(pos, size);
            dataFile.seek(pos * dataFileScale);
            dataFile.read(rowIn.getBuffer(), 0, size);
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.readObject", t, pos);

            HsqlException ex = Error.error(ErrorCode.DATA_FILE_ERROR, t);

            ex.info = rowIn;

            throw ex;
        }
    }

    public void releaseRange(long startPos, long limitPos) {

        writeLock.lock();

        try {
            cacheModified = true;

            cache.releaseRange(startPos, limitPos);
        } finally {
            writeLock.unlock();
        }
    }

    public void releaseRange(IntIndex list, int fileBlockItemCount) {

        writeLock.lock();

        try {
            cacheModified = true;

            cache.releaseRange(list, fileBlockItemCount);
        } finally {
            writeLock.unlock();
        }
    }

    public CachedObject release(long pos) {

        writeLock.lock();

        try {
            cacheModified = true;

            return cache.release(pos);
        } finally {
            writeLock.unlock();
        }
    }

    protected void saveRows(CachedObject[] rows, int offset, int count) {

        if (count == 0) {
            return;
        }

        int  pageCount   = copyShadow(rows, offset, count);
        long startTime   = cache.saveAllTimer.elapsedTime();
        long storageSize = 0;

        cache.saveAllTimer.start();

        if (pageCount > 0) {
            setFileModified();
        }

        for (int i = offset; i < offset + count; i++) {
            CachedObject r = rows[i];

            saveRowNoLock(r);

            rows[i]     = null;
            storageSize += r.getStorageSize();
        }

        cache.saveAllTimer.stop();
        cache.logSaveRowsEvent(count, storageSize, startTime);
    }

    /**
     * Writes out the specified Row. Will write only the Nodes or both Nodes
     * and table row data depending on what is not already persisted to disk.
     */
    public void saveRow(CachedObject row) {

        writeLock.lock();

        try {
            copyShadow(row);
            setFileModified();
            saveRowNoLock(row);
        } finally {
            writeLock.unlock();
        }
    }

    public void saveRowOutput(long pos) {

        try {
            dataFile.seek(pos * dataFileScale);
            dataFile.write(rowOut.getOutputStream().getBuffer(), 0,
                           rowOut.getOutputStream().size());
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.saveRowOutput", t, pos);

            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        }
    }

    protected void saveRowNoLock(CachedObject row) {

        try {
            rowOut.reset();
            row.write(rowOut);
            dataFile.seek(row.getPos() * dataFileScale);
            dataFile.write(rowOut.getOutputStream().getBuffer(), 0,
                           rowOut.getOutputStream().size());
            row.setChanged(false);
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.saveRowNoLock", t, row.getPos());

            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        }
    }

    protected int copyShadow(CachedObject[] rows, int offset, int count) {

        int pageCount = 0;

        if (shadowFile != null) {
            long time    = cache.shadowTimer.elapsedTime();
            long seekpos = 0;

            cache.shadowTimer.start();

            try {
                for (int i = offset; i < offset + count; i++) {
                    CachedObject row = rows[i];

                    seekpos = row.getPos() * dataFileScale;
                    pageCount += shadowFile.copy(seekpos,
                                                 row.getStorageSize());
                }

                if (pageCount > 0) {
                    shadowFile.synch();
                }
            } catch (Throwable t) {
                logSevereEvent("DataFileCache.copyShadow", t, seekpos);

                throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
            }

            cache.shadowTimer.stop();

            if (pageCount > 0) {
                time = cache.shadowTimer.elapsedTime() - time;

                logDetailEvent("copyShadow [size, time] "
                               + shadowFile.getSavedLength() + " " + time);
            }
        }

        return pageCount;
    }

    protected int copyShadow(CachedObject row) {

        if (shadowFile != null) {
            long seekpos = row.getPos() * dataFileScale;

            try {
                int pageCount = shadowFile.copy(seekpos, row.getStorageSize());

                shadowFile.synch();

                return pageCount;
            } catch (Throwable t) {
                logSevereEvent("DataFileCache.copyShadow", t, row.getPos());

                throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
            }
        }

        return 0;
    }

    void deleteDataFile() {
        Log.deleteFile(fa, dataFileName);
    }

    private void deleteBackupFile() {
        Log.deleteFile(fa, backupFileName);
    }

    /**
     * Delta must always result in block multiples
     */
    public long enlargeFileSpace(long newLength) {

        writeLock.lock();

        try {
            long position = fileFreePosition;

            if (newLength > maxDataFileSize) {
                logSevereEvent("data file reached maximum allowed size: "
                               + dataFileName + " " + maxDataFileSize, null);

                throw Error.error(ErrorCode.DATA_FILE_IS_FULL);
            }

            boolean result = dataFile.ensureLength(newLength);

            if (!result) {
                logSevereEvent("data file cannot be enlarged - disk space: "
                               + dataFileName + " " + newLength, null);

                throw Error.error(ErrorCode.DATA_FILE_IS_FULL);
            }

            fileFreePosition = newLength;

            return position;
        } finally {
            writeLock.unlock();
        }
    }

    public int capacity() {
        return maxCacheRows;
    }

    public long bytesCapacity() {
        return maxCacheBytes;
    }

    public long getTotalCachedBlockSize() {
        return cache.getTotalCachedBlockSize();
    }

    public long getLostBlockSize() {
        return spaceManager.getLostBlocksSize();
    }

    public long getFileFreePos() {
        return fileFreePosition;
    }

    public int getCachedObjectCount() {
        return cache.size();
    }

    public String getFileName() {
        return dataFileName;
    }

    public int getDataFileScale() {
        return dataFileScale;
    }

    public int getDataFileSpace() {
        return dataFileSpace;
    }

    public boolean isFileModified() {
        return fileModified;
    }

    public boolean isModified() {
        return cacheModified;
    }

    public boolean isFileOpen() {
        return dataFile != null;
    }

    protected void setFileModified() {

        try {
            if (!fileModified) {

                // unset saved flag;
                setFlag(Flags.FLAG_ISSAVED, false);
                logDetailEvent("setFileModified flag set ");

                fileModified = true;
            }
        } catch (Throwable t) {
            logSevereEvent("DataFileCache.setFileModified", t);

            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        }
    }

    int getFlags() throws IOException {

        dataFile.seek(Positions.INT_FLAGS);

        int flags = dataFile.readInt();

        return flags;
    }

    void setFlags(int flags) throws IOException {

        dataFile.seek(Positions.INT_FLAGS);
        dataFile.writeInt(flags);
        dataFile.synch();
    }

    void setFlag(int singleFlag, boolean val) throws IOException {

        dataFile.seek(Positions.INT_FLAGS);

        int flags = dataFile.readInt();

        flags = val ? BitMap.set(flags, singleFlag)
                    : BitMap.unset(flags, singleFlag);

        dataFile.seek(Positions.INT_FLAGS);
        dataFile.writeInt(flags);
        dataFile.synch();
    }

    public boolean isDataReadOnly() {
        return this.cacheReadonly;
    }

    public RAShadowFile getShadowFile() {
        return shadowFile;
    }

    public AtomicInteger getAccessCount() {
        return cache.getAccessCount();
    }

    private void logSevereEvent(String message, Throwable t, long position) {

        if (logEvents) {
            StringBuilder sb = new StringBuilder(message);

            sb.append(' ').append(position);

            message = sb.toString();

            database.logger.logSevereEvent(message, t);
        }
    }

    public void logSevereEvent(String message, Throwable t) {

        if (logEvents) {
            database.logger.logSevereEvent(message, t);
        }
    }

    void logInfoEvent(String message) {

        if (logEvents) {
            database.logger.logInfoEvent(message);
        }
    }

    void logDetailEvent(String message) {

        if (logEvents) {
            database.logger.logDetailEvent(message);
        }
    }
}
