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

import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;

import org.hsqldb.Database;
import org.hsqldb.lib.HsqlByteArrayInputStream;
import org.hsqldb.lib.Storage;

// fredt@users 20030111 - patch 1.7.2 by bohgammer@users - pad file before seek() beyond end
// some incompatible JVM implementations do not allow seek beyond the existing end of file

/**
 * This class is a wapper for a random access file such as that used for
 * CACHED table storage.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version  1.9.0
 * @since  1.7.2
 */
final class ScaledRAFile implements ScaledRAInterface {

    static final int  DATA_FILE_RAF    = 0;
    static final int  DATA_FILE_NIO    = 1;
    static final int  DATA_FILE_JAR    = 2;
    static final int  DATA_FILE_STORED = 3;
    static final long MAX_NIO_LENGTH   = (1L << 28);

    // We are using persist.Logger-instance-specific FrameworkLogger
    // because it is Database-instance specific.
    // If add any static level logging, should instantiate a standard,
    // context-agnostic FrameworkLogger for that purpose.
    final Database                 database;
    final RandomAccessFile         file;
    final FileDescriptor           fileDescriptor;
    private final boolean          readOnly;
    final String                   fileName;
    boolean                        isNio;
    boolean                        bufferDirty = true;
    final byte[]                   buffer;
    final HsqlByteArrayInputStream ba;
    long                           bufferOffset;
    long                           fileLength;

    //
    long seekPosition;
    long realPosition;
    int  cacheHit;

    /**
     * seekPosition is the position in seek() calls or after reading or writing
     * realPosition is the file position
     */
    static Storage newScaledRAFile(Database database, String name,
                                   boolean readonly,
                                   int type)
                                   throws FileNotFoundException, IOException {

        if (type == DATA_FILE_STORED) {
            try {
                String cname = database.getURLProperties().getProperty(
                    HsqlDatabaseProperties.url_storage_class_name);
                String skey = database.getURLProperties().getProperty(
                    HsqlDatabaseProperties.url_storage_key);
                Class       zclass      = Class.forName(cname);
                Constructor constructor = zclass.getConstructor(new Class[] {
                    String.class, Boolean.class, Object.class
                });

                return (Storage) constructor.newInstance(new Object[] {
                    name, new Boolean(readonly), skey
                });
            } catch (ClassNotFoundException e) {
                throw new IOException();
            } catch (NoSuchMethodException e) {
                throw new IOException();
            } catch (InstantiationException e) {
                throw new IOException();
            } catch (IllegalAccessException e) {
                throw new IOException();
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw new IOException();
            }
        }

        if (type == DATA_FILE_JAR) {
            return new ScaledRAFileInJar(name);
        } else if (type == DATA_FILE_RAF) {
            return new ScaledRAFile(database, name, readonly);
        } else {
            java.io.File fi     = new java.io.File(name);
            long         length = fi.length();

            if (length > MAX_NIO_LENGTH) {
                return new ScaledRAFile(database, name, readonly);
            }

            try {
                Class.forName("java.nio.MappedByteBuffer");

                return new ScaledRAFileHybrid(database, name, readonly);
            } catch (Exception e) {
                return new ScaledRAFile(database, name, readonly);
            }
        }
    }

    ScaledRAFile(Database database, String name,
                 boolean readonly) throws FileNotFoundException, IOException {

        this.database = database;
        this.readOnly = readonly;
        this.fileName = name;
        this.file     = new RandomAccessFile(name, readonly ? "r"
                                                            : "rw");

        int bufferSize = 1 << 12;

        buffer         = new byte[bufferSize];
        ba             = new HsqlByteArrayInputStream(buffer);
        fileDescriptor = file.getFD();
        fileLength = length();
    }

    public long length() throws IOException {
        return file.length();
    }

    /**
     * Some JVM's do not allow seek beyond end of file, so zeros are written
     * first in that case. Reported by bohgammer@users in Open Disucssion
     * Forum.
     */
    public void seek(long position) throws IOException {

        if (!readOnly && fileLength < position) {
            long tempSize = position - fileLength;

            if (tempSize > 1 << 16) {
                tempSize = 1 << 16;
            }

            byte[] temp = new byte[(int) tempSize];

            try {
                long pos = fileLength;

                for (; pos < position - tempSize; pos += tempSize) {
                    file.seek(pos);
                    file.write(temp, 0, (int) tempSize);
                }

                file.seek(pos);
                file.write(temp, 0, (int) (position - pos));

                realPosition = position;
                fileLength   = position;
            } catch (IOException e) {
                database.logger.logWarningEvent("seek failed", e);

                throw e;
            }
        }

        seekPosition = position;
    }

    public long getFilePointer() throws IOException {
        return seekPosition;
    }

    private void readIntoBuffer() throws IOException {

        long filePos    = seekPosition;
        long subOffset  = filePos % buffer.length;
        long readLength = fileLength - (filePos - subOffset);

        try {
            if (readLength <= 0) {
                throw new IOException("read beyond end of file");
            }

            if (readLength > buffer.length) {
                readLength = buffer.length;
            }

            if (realPosition != filePos - subOffset) {
                file.seek(filePos - subOffset);
            }

            file.readFully(buffer, 0, (int) readLength);

            bufferOffset = filePos - subOffset;
            realPosition = bufferOffset + readLength;
            bufferDirty  = false;
        } catch (IOException e) {
            resetPointer();
            database.logger.logWarningEvent(" " + realPosition + " "
                                            + readLength, e);

            throw e;
        }
    }

    public int read() throws IOException {

        try {
            if (seekPosition >= fileLength) {
                return -1;
            }

            if (bufferDirty || seekPosition < bufferOffset
                    || seekPosition >= bufferOffset + buffer.length) {
                readIntoBuffer();
            } else {
                cacheHit++;
            }

            ba.reset();
            ba.skip(seekPosition - bufferOffset);

            int val = ba.read();

            seekPosition++;

            return val;
        } catch (IOException e) {
            resetPointer();
            database.logger.logWarningEvent("read failed", e);

            throw e;
        }
    }

    public long readLong() throws IOException {

        try {
            if (bufferDirty || seekPosition < bufferOffset
                    || seekPosition >= bufferOffset + buffer.length) {
                readIntoBuffer();
            } else {
                cacheHit++;
            }

            ba.reset();

            if (seekPosition - bufferOffset
                    != ba.skip(seekPosition - bufferOffset)) {
                throw new EOFException();
            }

            long val;

            try {
                val = ba.readLong();
            } catch (EOFException e) {
                file.seek(seekPosition);

                val          = file.readLong();
                realPosition = file.getFilePointer();
            }

            seekPosition += 8;

            return val;
        } catch (IOException e) {
            resetPointer();
            database.logger.logWarningEvent("failed ot read a Long", e);

            throw e;
        }
    }

    public int readInt() throws IOException {

        try {
            if (bufferDirty || seekPosition < bufferOffset
                    || seekPosition >= bufferOffset + buffer.length) {
                readIntoBuffer();
            } else {
                cacheHit++;
            }

            ba.reset();

            if (seekPosition - bufferOffset
                    != ba.skip(seekPosition - bufferOffset)) {
                throw new EOFException();
            }

            int val;

            try {
                val = ba.readInt();
            } catch (EOFException e) {
                file.seek(seekPosition);

                val          = file.readInt();
                realPosition = file.getFilePointer();
            }

            seekPosition += 4;

            return val;
        } catch (IOException e) {
            resetPointer();
            database.logger.logWarningEvent("failed to read an Int", e);

            throw e;
        }
    }

    public void read(byte[] b, int offset, int length) throws IOException {

        try {
            if (bufferDirty || seekPosition < bufferOffset
                    || seekPosition >= bufferOffset + buffer.length) {
                readIntoBuffer();
            } else {
                cacheHit++;
            }

            ba.reset();

            if (seekPosition - bufferOffset
                    != ba.skip(seekPosition - bufferOffset)) {
                throw new EOFException();
            }

            int bytesRead = ba.read(b, offset, length);

            seekPosition += bytesRead;

            if (bytesRead < length) {
                if (seekPosition != realPosition) {
                    file.seek(seekPosition);
                }

                file.readFully(b, offset + bytesRead, length - bytesRead);

                seekPosition += (length - bytesRead);
                realPosition = seekPosition;
            }
        } catch (IOException e) {
            resetPointer();
            database.logger.logWarningEvent("faeild to read a byte array", e);

            throw e;
        }
    }

    public void write(byte[] b, int off, int len) throws IOException {

        try {
            if (realPosition != seekPosition) {
                file.seek(seekPosition);

                realPosition = seekPosition;
            }

            if (seekPosition < bufferOffset + buffer.length
                    && seekPosition + len > bufferOffset) {
                bufferDirty = true;
            }

            file.write(b, off, len);

            seekPosition += len;
            realPosition = seekPosition;

            if (realPosition > fileLength) {
                fileLength = realPosition;
            }
        } catch (IOException e) {
            resetPointer();
            database.logger.logWarningEvent("failed to write a byte array", e);

            throw e;
        }
    }

    public void writeInt(int i) throws IOException {

        try {
            if (realPosition != seekPosition) {
                file.seek(seekPosition);

                realPosition = seekPosition;
            }

            if (seekPosition < bufferOffset + buffer.length
                    && seekPosition + 4 > bufferOffset) {
                bufferDirty = true;
            }

            file.writeInt(i);

            seekPosition += 4;
            realPosition = seekPosition;

            if (realPosition > fileLength) {
                fileLength = realPosition;
            }
        } catch (IOException e) {
            resetPointer();
            database.logger.logWarningEvent("failed to write an int", e);

            throw e;
        }
    }

    public void writeLong(long i) throws IOException {

        try {
            if (realPosition != seekPosition) {
                file.seek(seekPosition);

                realPosition = seekPosition;
            }

            if (seekPosition < bufferOffset + buffer.length
                    && seekPosition + 8 > bufferOffset) {
                bufferDirty = true;
            }

            file.writeLong(i);

            seekPosition += 8;
            realPosition = seekPosition;

            if (realPosition > fileLength) {
                fileLength = realPosition;
            }
        } catch (IOException e) {
            resetPointer();
            database.logger.logWarningEvent("failed to write a Long", e);

            throw e;
        }
    }

    public void close() throws IOException {
        file.close();
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean wasNio() {
        return false;
    }

    public boolean canAccess(int length) {
        return true;
    }

    public boolean canSeek(long position) {
        return true;
    }

    public Database getDatabase() {
        return null;
    }

    public void synch() {

        try {
            fileDescriptor.sync();
        } catch (IOException e) {}
    }

    private void resetPointer() {

        try {
            bufferDirty = true;

            file.seek(seekPosition);

            realPosition = seekPosition;
            fileLength   = length();
        } catch (Throwable e) {}
    }
}
