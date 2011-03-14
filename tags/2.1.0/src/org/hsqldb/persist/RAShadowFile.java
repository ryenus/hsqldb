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

import org.hsqldb.Database;
import org.hsqldb.lib.HsqlByteArrayOutputStream;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.store.BitMap;

/*
 * Wrapper for random access file for incremental backup of the .data file.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 1.9.0
 */
public class RAShadowFile {

    final Database              database;
    final String                pathName;
    final RandomAccessInterface source;
    RandomAccessInterface       dest;
    final int                   pageSize;
    final long                  maxSize;
    final BitMap                bitMap;
    boolean                     zeroPageSet;
    HsqlByteArrayOutputStream byteArrayOutputStream =
        new HsqlByteArrayOutputStream(new byte[]{});

    RAShadowFile(Database database, RandomAccessInterface source,
                 String pathName, long maxSize, int pageSize) {

        this.database = database;
        this.pathName = pathName;
        this.source   = source;
        this.pageSize = pageSize;
        this.maxSize  = maxSize;

        int bitSize = (int) (maxSize / pageSize);

        if (maxSize % pageSize != 0) {
            bitSize++;
        }

        bitMap = new BitMap(bitSize);
    }

    void copy(long fileOffset, int size) throws IOException {

        // always copy the first page
        if (!zeroPageSet) {
            copy(0);
            bitMap.set(0);

            zeroPageSet = true;
        }

        if (fileOffset >= maxSize) {
            return;
        }

        long endOffset       = fileOffset + size;
        int  startPageOffset = (int) (fileOffset / pageSize);
        int  endPageOffset   = (int) (endOffset / pageSize);

        if (endOffset % pageSize == 0) {
            endPageOffset--;
        }

        for (; startPageOffset <= endPageOffset; startPageOffset++) {
            copy(startPageOffset);
        }
    }

    private void copy(int pageOffset) throws IOException {

        if (bitMap.set(pageOffset) == 1) {
            return;
        }

        long position = (long) pageOffset * pageSize;
        int  readSize = pageSize;

        if (maxSize - position < pageSize) {
            readSize = (int) (maxSize - position);
        }

        if (dest == null) {
            open();
        }

        long writePos = dest.length();

        try {
            byte[] buffer = new byte[pageSize + 12];

            byteArrayOutputStream.setBuffer(buffer);
            byteArrayOutputStream.writeInt(pageSize);
            byteArrayOutputStream.writeLong(position);
            source.seek(position);
            source.read(buffer, 12, readSize);
            dest.seek(writePos);
            dest.write(buffer, 0, buffer.length);
        } catch (Throwable t) {
            bitMap.unset(pageOffset);
            dest.seek(0);
            dest.setLength(writePos);
            close();
            database.logger.logWarningEvent("pos" + position + " " + readSize,
                                            t);

            throw JavaSystem.toIOException(t);
        } finally {}
    }

    private void open() throws IOException {

        if (database.logger.isStoredFileAccess()) {
            dest = ScaledRAFile.newScaledRAFile(database, pathName, false,
                                                ScaledRAFile.DATA_FILE_STORED);
        } else {
            dest = new ScaledRAFileSimple(pathName, "rws");
        }
    }

    /**
     * Called externally after a series of copy() calls.
     * Called internally after a restore or when error in writing
     */
    void close() throws IOException {

        if (dest != null) {
            dest.synch();
            dest.close();

            dest = null;
        }
    }

    public void synch() {

        if (dest != null) {
            dest.synch();
        }
    }

    private static RandomAccessInterface getStorage(Database database,
            String pathName, String openMode) throws IOException {

        if (database.logger.isStoredFileAccess()) {
            return ScaledRAFile.newScaledRAFile(database, pathName,
                                                openMode.equals("r"),
                                                ScaledRAFile.DATA_FILE_STORED);
        } else {
            return new ScaledRAFileSimple(pathName, openMode);
        }
    }

    /** todo - take account of incomplete addition of block due to lack of disk */

    // buggy database files had size == position == 0 at the end
    public static void restoreFile(Database database, String sourceName,
                                   String destName) throws IOException {

        RandomAccessInterface source = getStorage(database, sourceName, "r");
        RandomAccessInterface dest   = getStorage(database, destName, "rw");

        while (source.getFilePointer() != source.length()) {
            int    size     = source.readInt();
            long   position = source.readLong();
            byte[] buffer   = new byte[size];

            source.read(buffer, 0, buffer.length);
            dest.seek(position);
            dest.write(buffer, 0, buffer.length);
        }

        source.close();
        dest.synch();
        dest.close();
    }
}
