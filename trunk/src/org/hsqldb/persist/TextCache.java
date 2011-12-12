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

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.hsqldb.Database;
import org.hsqldb.DatabaseURL;
import org.hsqldb.HsqlException;
import org.hsqldb.Table;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.HsqlByteArrayOutputStream;
import org.hsqldb.lib.IntKeyHashMap;
import org.hsqldb.rowio.RowInputText;
import org.hsqldb.rowio.RowInputTextQuoted;
import org.hsqldb.rowio.RowOutputText;
import org.hsqldb.rowio.RowOutputTextQuoted;
import org.hsqldb.scriptio.ScriptWriterText;

// Ito Kazumitsu 20030328 - patch 1.7.2 - character encoding support
// Dimitri Maziuk - patch for NL in string support
// sqlbob@users - updated for 1.8.0 to allow new-lines in fields
// fredt@users - updated for 1.8.0 to allow correct behaviour with transactions

/**
 * Acts as a buffer manager for a single TEXT table and its Row data.<p>
 *
 * Handles read/write operations on the table's text format data file using a
 * compatible pair of org.hsqldb.rowio input/output class instances.
 *
 *
 * fredt - This used to write rows as soon as they are inserted
 * but now this is subject to transaction management.
 * A memory buffer contains the rows not yet committed.
 * Refactored for version 2.2.6.
 *
 * @author Bob Preston (sqlbob@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.7
 * @since 1.7.0
 */
public class TextCache extends DataFileCache {

    //
    TextFileSettings textFileSettings;

    //state of Cache
    protected String          header;
    protected Table           table;
    private IntKeyHashMap     uncommittedCache;
    HsqlByteArrayOutputStream buffer = new HsqlByteArrayOutputStream(128);

    //

    /**
     *  The source string for a cached table is evaluated and the parameters
     *  are used to open the source file.<p>
     *
     *  Settings are used in this order: (1) settings specified in the
     *  source string for the table (2) global database settings in
     *  *.properties file (3) program defaults
     */
    TextCache(Table table, String name) {

        super(table.database, name);

        this.table       = table;
        uncommittedCache = new IntKeyHashMap();
    }

    protected void initParams(Database database, String fileSettingsString) {

        this.database    = database;
        fa               = FileUtil.getFileUtil();
        textFileSettings = new TextFileSettings(database, fileSettingsString);
        dataFileName     = textFileSettings.getFileName();

        if (dataFileName == null) {
            throw Error.error(ErrorCode.X_S0501);
        }

        dataFileName  = ((FileUtil) fa).canonicalOrAbsolutePath(dataFileName);
        maxCacheRows  = textFileSettings.getMaxCacheRows();
        maxCacheBytes = textFileSettings.getMaxCacheBytes();

        //-- Get size and scale
        maxDataFileSize  = Integer.MAX_VALUE;
        cachedRowPadding = 1;
        cacheFileScale   = 1;
    }

    protected void initBuffers() {

        if (textFileSettings.isQuoted || textFileSettings.isAllQuoted) {
            rowIn = new RowInputTextQuoted(textFileSettings.fs,
                                           textFileSettings.vs,
                                           textFileSettings.lvs,
                                           textFileSettings.isAllQuoted);
            rowOut = new RowOutputTextQuoted(textFileSettings.fs,
                                             textFileSettings.vs,
                                             textFileSettings.lvs,
                                             textFileSettings.isAllQuoted,
                                             textFileSettings.stringEncoding);
        } else {
            rowIn = new RowInputText(textFileSettings.fs, textFileSettings.vs,
                                     textFileSettings.lvs, false);
            rowOut = new RowOutputText(textFileSettings.fs,
                                       textFileSettings.vs,
                                       textFileSettings.lvs, false,
                                       textFileSettings.stringEncoding);
        }
    }

    /**
     *  Opens a data source file.
     */
    public void open(boolean readonly) {

        fileFreePosition = 0;

        try {
            int type = database.getType() == DatabaseURL.S_RES
                       ? ScaledRAFile.DATA_FILE_JAR
                       : ScaledRAFile.DATA_FILE_TEXT;

            dataFile = ScaledRAFile.newScaledRAFile(database, dataFileName,
                    readonly, type);
            fileFreePosition = dataFile.length();

            if (fileFreePosition > Integer.MAX_VALUE) {
                throw Error.error(ErrorCode.DATA_FILE_IS_FULL);
            }

            initBuffers();

            freeBlocks = new DataFileBlockManager(0, cacheFileScale, 0, 0);
        } catch (Throwable t) {
            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_TextCache_openning_file_error,
                              new Object[] {
                t.toString(), dataFileName
            });
        }

        cacheReadonly = readonly;
    }

    void reopen() {
        open(cacheReadonly);
    }

    /**
     *  Writes newly created rows to disk. In the current implentation,
     *  such rows have already been saved, so this method just removes a
     *  source file that has no rows.
     */
    public void close(boolean write) {

        if (dataFile == null) {
            return;
        }

        writeLock.lock();

        try {
            cache.saveAll();

            boolean empty = (dataFile.length()
                             <= TextFileSettings.NL.length());

            dataFile.synch();
            dataFile.close();

            dataFile = null;

            if (empty && !cacheReadonly) {
                FileUtil.getFileUtil().delete(dataFileName);
            }

            uncommittedCache.clear();
        } catch (Throwable t) {
            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_TextCache_closing_file_error,
                              new Object[] {
                t.toString(), dataFileName
            });
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Closes the source file and deletes it if it is not read-only.
     */
    void purge() {

        writeLock.lock();

        try {
            uncommittedCache.clear();

            if (cacheReadonly) {
                close(false);
            } else {
                if (dataFile != null) {
                    dataFile.close();

                    dataFile = null;
                }

                FileUtil.getFileUtil().delete(dataFileName);
            }
        } catch (Throwable t) {
            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_TextCache_purging_file_error,
                              new Object[] {
                t.toString(), dataFileName
            });
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Does not extend the end of file.
     */
    int setFilePos(CachedObject r) {

        int  rowSize         = r.getStorageSize();
        long newFreePosition = fileFreePosition + rowSize;

        if (newFreePosition > maxDataFileSize) {
            database.logger.logSevereEvent("data file reached maximum size "
                                           + this.dataFileName, null);

            throw Error.error(ErrorCode.DATA_FILE_IS_FULL);
        }

        int i = (int) fileFreePosition;

        r.setPos(i);
        clearRowImage(r);

        fileFreePosition = newFreePosition;

        return i;
    }

    /**
     *
     */
    public void remove(int pos, PersistentStore store) {

        writeLock.lock();

        try {
            CachedObject row = (CachedObject) uncommittedCache.remove(pos);

            if (row != null) {
                return;
            }

            row = cache.release(pos);
        } finally {
            writeLock.unlock();
        }
    }

    public void removePersistence(CachedObject row) {

        writeLock.lock();

        try {
            clearRowImage(row);
        } finally {
            writeLock.unlock();
        }
    }

    private void clearRowImage(CachedObject row) {

        try {
            int length = row.getStorageSize()
                         - ScriptWriterText.BYTES_LINE_SEP.length;

            rowOut.reset();

            HsqlByteArrayOutputStream out = rowOut.getOutputStream();

            out.fill(' ', length);
            out.write(ScriptWriterText.BYTES_LINE_SEP);
            dataFile.seek(row.getPos());
            dataFile.write(out.getBuffer(), 0, out.size());
        } catch (IOException e) {
            throw Error.runtimeError(ErrorCode.U_S0500, e.getMessage());
        }
    }

    public void addInit(CachedObject object) {

        writeLock.lock();

        try {
            cache.put(object.getPos(), object);
        } finally {
            writeLock.unlock();
        }
    }

    public void add(CachedObject object) {

        writeLock.lock();

        try {
            setFilePos(object);
            uncommittedCache.put(object.getPos(), object);
        } finally {
            writeLock.unlock();
        }
    }

    /** cannot use isInMemory() for text cached object */
    public CachedObject get(CachedObject object, PersistentStore store,
                            boolean keep) {

        if (object == null) {
            return null;
        }

        writeLock.lock();

        try {
            try {
                buffer.reset(object.getStorageSize());
                dataFile.seek(object.getPos());
                dataFile.read(buffer.getBuffer(), 0, object.getStorageSize());
                buffer.setSize(object.getStorageSize());

                String rowString =
                    buffer.toString(textFileSettings.stringEncoding);

                ((RowInputText) rowIn).setSource(rowString, object.getPos(),
                                                 buffer.size());
                store.get(rowIn);
                cache.put(object.getPos(), object);

                return object;
            } catch (IOException err) {
                database.logger.logSevereEvent(dataFileName
                                               + " getFromFile problem "
                                               + object.getPos(), err);
                cache.forceCleanUp();
                System.gc();

                return object;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public CachedObject get(int i, PersistentStore store, boolean keep) {
        throw Error.runtimeError(ErrorCode.U_S0500, "TextCache");
    }

    protected void saveRows(CachedObject[] rows, int offset, int count) {
        // no-op
    }

    /**
     * The row is always in uncommittedCache.
     * Saves the row as normal and removes it
     */
    public void saveRow(CachedObject row) {

        writeLock.lock();

        try {
            setFileModified();
            saveRowNoLock(row);
            uncommittedCache.remove(row.getPos());
            cache.put(row.getPos(), row);
        } catch (Throwable e) {
            database.logger.logSevereEvent("saveRow failed", e);

            throw Error.error(ErrorCode.DATA_FILE_ERROR, e);
        } finally {
            writeLock.unlock();
        }
    }

    public String getHeader() {
        return header;
    }

    public void setHeaderInitialise(String header) {
        this.header = header;
    }

    public void setHeader(String header) {

        if (textFileSettings.ignoreFirst && fileFreePosition == 0) {
            try {
                writeHeader(header);

                this.header = header;
            } catch (HsqlException e) {
                throw new HsqlException(
                    e, Error.getMessage(ErrorCode.GENERAL_IO_ERROR),
                    ErrorCode.GENERAL_IO_ERROR);
            }

            return;
        }

        throw Error.error(ErrorCode.TEXT_TABLE_HEADER);
    }

    private void writeHeader(String header) {

        try {
            byte[] buf       = null;
            String firstLine = header + TextFileSettings.NL;

            try {
                buf = firstLine.getBytes(textFileSettings.stringEncoding);
            } catch (UnsupportedEncodingException e) {
                buf = firstLine.getBytes();
            }

            dataFile.seek(0);
            dataFile.write(buf, 0, buf.length);

            fileFreePosition = buf.length;
        } catch (IOException e) {
            throw Error.error(ErrorCode.TEXT_FILE_IO, e);
        }
    }

    public int getLineNumber() {
        return ((RowInputText) rowIn).getLineNumber();
    }

    public TextFileSettings getTextFileSettings() {
        return textFileSettings;
    }

    public boolean isIgnoreFirstLine() {
        return textFileSettings.ignoreFirst;
    }

    protected void setFileModified() {
        fileModified = true;
    }

    public TextFileReader getTextFileReader() {
        return new TextFileReader(dataFile, textFileSettings, rowIn,
                                  cacheReadonly);
    }
}
