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
import java.io.RandomAccessFile;
import java.io.FileDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.hsqldb.Database;

/**
 * New NIO version of ScaledRAFile. This class is used only for storing a CACHED
 * TABLE .data file and cannot be used for TEXT TABLE source files.
 *
 * Due to various issues with java.nio classes, this class will use a mapped
 * channel of fixed size. After reaching this size, the file and channel are
 * closed.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version  2.0.1
 * @since 1.8.0.5
 */
final class ScaledRAFileNIO implements RandomAccessInterface {

    private final Database   database;
    private final boolean    readOnly;
    private final long       maxLength;
    private long             fileLength;
    private RandomAccessFile file;
    private FileDescriptor   fileDescriptor;
    private MappedByteBuffer buffer;
    private long             bufferPosition;
    private int              bufferLength;
    private long             currentPosition;
    private FileChannel      channel;
    private boolean          buffersModified;

    //
    private MappedByteBuffer buffers[] = new MappedByteBuffer[]{};

    //
    private static final String JVM_ERROR = "JVM threw unsupported Exception";

    //
    static final int largeBufferScale = 24;
    static final int largeBufferSize  = 1 << largeBufferScale;
    static final long largeBufferMask = 0xffffffffffffffffl
                                        << largeBufferScale;

    ScaledRAFileNIO(Database database, String name, boolean readOnly,
                    long requiredLength, long maxLength) throws Throwable {

        this.database  = database;
        this.maxLength = maxLength;

        long         fileLength;
        java.io.File fi = new java.io.File(name);

        fileLength = fi.length();

        if (readOnly) {
            requiredLength = fileLength;
        } else {
            if (fileLength > requiredLength) {
                requiredLength = fileLength;
            }

            requiredLength =
                ScaledRAFile.getBinaryNormalisedCeiling(requiredLength,
                    largeBufferScale);
        }

        file                = new RandomAccessFile(name, readOnly ? "r"
                                                                  : "rw");
        this.readOnly       = readOnly;
        this.channel        = file.getChannel();
        this.fileDescriptor = file.getFD();

        if (ensureLength(requiredLength)) {
            buffer          = buffers[0];
            bufferLength    = buffer.limit();
            bufferPosition  = 0;
            currentPosition = 0;
        } else {
            IOException io = new IOException("NIO buffer allocation failed");

            throw io;
        }
    }

    public long length() throws IOException {

        try {
            return file.length();
        } catch (IOException e) {
            database.logger.logWarningEvent("nio", e);

            throw e;
        } catch (Throwable e) {
            database.logger.logWarningEvent(JVM_ERROR, e);

            IOException io = new IOException(e.toString());

            try {
                io.initCause(e);
            } catch (Throwable e1) {}

            throw io;
        }
    }

    public void seek(long newPos) throws IOException {

        try {
            positionBufferSeek(newPos);
            buffer.position((int) (newPos - bufferPosition));
        } catch (IllegalArgumentException e) {
            database.logger.logWarningEvent("nio", e);

            IOException io = new IOException(e.toString());

            try {
                io.initCause(e);
            } catch (Throwable e1) {}

            throw io;
        } catch (Throwable e) {
            database.logger.logWarningEvent(JVM_ERROR, e);

            IOException io = new IOException(e.toString());

            try {
                io.initCause(e);
            } catch (Throwable e1) {}

            throw io;
        }
    }

    public long getFilePointer() throws IOException {

        try {
            return currentPosition;
        } catch (Throwable e) {
            database.logger.logWarningEvent(JVM_ERROR, e);

            IOException io = new IOException(e.toString());

            try {
                io.initCause(e);
            } catch (Throwable e1) {}

            throw io;
        }
    }

    public int read() throws IOException {

        try {
            int value = buffer.get();

            positionBufferMove(1);

            return value;
        } catch (Throwable e) {
            database.logger.logWarningEvent(JVM_ERROR, e);

            IOException io = new IOException(e.toString());

            try {
                io.initCause(e);
            } catch (Throwable e1) {}

            throw io;
        }
    }

    public void read(byte[] b, int offset, int length) throws IOException {

        try {
            while (true) {
                long transferLength = bufferPosition + bufferLength
                                      - currentPosition;

                if (transferLength > length) {
                    transferLength = length;
                }

                buffer.get(b, offset, (int) transferLength);
                positionBufferMove((int) transferLength);

                length -= transferLength;
                offset += transferLength;

                if (length == 0) {
                    break;
                }
            }
        } catch (Throwable e) {
            database.logger.logWarningEvent(JVM_ERROR, e);

            IOException io = new IOException(e.toString());

            try {
                io.initCause(e);
            } catch (Throwable e1) {}

            throw io;
        }
    }

    public int readInt() throws IOException {

        try {
            int value = buffer.getInt();

            positionBufferMove(4);

            return value;
        } catch (Throwable e) {
            database.logger.logWarningEvent(JVM_ERROR, e);

            IOException io = new IOException(e.toString());

            try {
                io.initCause(e);
            } catch (Throwable e1) {}

            throw io;
        }
    }

    public long readLong() throws IOException {

        try {
            long value = buffer.getLong();

            positionBufferMove(8);

            return value;
        } catch (Throwable e) {
            database.logger.logWarningEvent(JVM_ERROR, e);

            IOException io = new IOException(e.toString());

            try {
                io.initCause(e);
            } catch (Throwable e1) {}

            throw io;
        }
    }

    public void write(byte[] b, int offset, int length) throws IOException {

        try {
            buffersModified = true;

            while (true) {
                long transferLength = bufferPosition + bufferLength
                                      - currentPosition;

                if (transferLength > length) {
                    transferLength = length;
                }

                buffer.put(b, offset, (int) transferLength);
                positionBufferMove((int) transferLength);

                length -= transferLength;
                offset += transferLength;

                if (length == 0) {
                    break;
                }
            }
        } catch (Throwable e) {
            database.logger.logWarningEvent(JVM_ERROR, e);

            IOException io = new IOException(e.toString());

            try {
                io.initCause(e);
            } catch (Throwable e1) {}

            throw io;
        }
    }

    public void writeInt(int i) throws IOException {

        try {
            buffersModified = true;

            buffer.putInt(i);
            positionBufferMove(4);
        } catch (Throwable e) {
            database.logger.logWarningEvent(JVM_ERROR, e);

            IOException io = new IOException(e.toString());

            try {
                io.initCause(e);
            } catch (Throwable e1) {}

            throw io;
        }
    }

    public void writeLong(long i) throws IOException {

        try {
            buffersModified = true;

            buffer.putLong(i);
            positionBufferMove(8);
        } catch (Throwable e) {
            database.logger.logWarningEvent(JVM_ERROR, e);

            IOException io = new IOException(e.toString());

            try {
                io.initCause(e);
            } catch (Throwable e1) {}

            throw io;
        }
    }

    public void close() throws IOException {

        try {
            database.logger.logDetailEvent("NIO file close, size: "
                                           + fileLength);

            buffer  = null;
            channel = null;

            for (int i = 0; i < buffers.length; i++) {
                unmap(buffers[i]);
                buffers[i] = null;
            }

            file.close();

            // System.gc();
        } catch (Throwable e) {
            database.logger.logWarningEvent("NIO buffer close error "
                                            + JVM_ERROR + " ", e);

            IOException io = new IOException(e.toString());

            try {
                io.initCause(e);
            } catch (Throwable e1) {}

            throw io;
        }
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean wasNio() {
        return true;
    }

    public boolean ensureLength(long newLength) {

        if (newLength > maxLength) {
            return false;
        }

        while (newLength > fileLength) {
            if (!enlargeFile(newLength)) {
                return false;
            }
        }

        return true;
    }

    private boolean enlargeFile(long newFileLength) {

        try {
            long newBufferLength = newFileLength;

            if (!readOnly) {
                newBufferLength = largeBufferSize;
            }

            MapMode mapMode = readOnly ? FileChannel.MapMode.READ_ONLY
                                       : FileChannel.MapMode.READ_WRITE;

            if (!readOnly && file.length() < fileLength + newBufferLength) {
                file.seek(fileLength + newBufferLength - 1);
                file.writeByte(0);
            }

            MappedByteBuffer newBuffer = channel.map(mapMode, fileLength,
                newBufferLength);
            MappedByteBuffer[] newBuffers =
                new MappedByteBuffer[buffers.length + 1];

            System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);

            newBuffers[buffers.length] = newBuffer;
            buffers                    = newBuffers;
            fileLength                 += newBufferLength;

            database.logger.logDetailEvent("NIO buffer instance, file size "
                                           + fileLength);
        } catch (Throwable e) {
            database.logger.logDetailEvent(
                "NOI buffer allocate failed, file size " + newFileLength);

            try {
                close();
            } catch (Throwable t) {}

            return false;
        }

        return true;
    }

    public boolean setLength(long newLength) {

        if (newLength > fileLength) {
            return enlargeFile(newLength);
        } else {
            try {
                seek(0);
            } catch (Throwable t) {

                //
            }

            return true;
        }
    }

    public Database getDatabase() {
        return null;
    }

    public void synch() {

        boolean error = false;

        for (int i = 0; i < buffers.length; i++) {
            try {
                buffers[i].force();
            } catch (Throwable t) {
                database.logger.logWarningEvent("NIO buffer force error "
                                                + JVM_ERROR + " ", t);

                error = true;
            }
        }

        if (error) {
            for (int i = 0; i < buffers.length; i++) {
                try {
                    buffers[i].force();
                } catch (Throwable t) {
                    database.logger.logWarningEvent("NIO buffer force error "
                                                    + JVM_ERROR + " ", t);
                }
            }
        }

        try {
            fileDescriptor.sync();

            buffersModified = false;
        } catch (Throwable t) {}
    }

    private void positionBufferSeek(long offset) {

        if (offset < bufferPosition
                || offset >= bufferPosition + bufferLength) {
            setCurrentBuffer(offset);
        }

        buffer.position((int) (offset - bufferPosition));

        currentPosition = offset;
    }

    private void positionBufferMove(int relOffset) {

        long offset = currentPosition + relOffset;

        if (offset >= bufferPosition + bufferLength) {
            setCurrentBuffer(offset);
        }

        buffer.position((int) (offset - bufferPosition));

        currentPosition = offset;
    }

    private void setCurrentBuffer(long offset) {

        int bufferIndex = (int) (offset >> largeBufferScale);

        buffer         = buffers[bufferIndex];
        bufferPosition = offset &= largeBufferMask;
    }

    /**
     * Non-essential unmap method - see http://bugs.sun.com/view_bug.do?bug_id=4724038
     * reported by joel_turkel at users.sourceforge.net
     */
    private void unmap(MappedByteBuffer buffer) throws IOException {

        if (buffer == null) {
            return;
        }

        try {
            Method cleanerMethod = buffer.getClass().getMethod("cleaner");

            cleanerMethod.setAccessible(true);

            Object cleaner     = cleanerMethod.invoke(buffer);
            Method clearMethod = cleaner.getClass().getMethod("clean");

            clearMethod.invoke(cleaner);
        } catch (InvocationTargetException e) {
        } catch (NoSuchMethodException e) {

            // Means we're not dealing with a Sun JVM?
        } catch (Throwable e) {}
    }
}
