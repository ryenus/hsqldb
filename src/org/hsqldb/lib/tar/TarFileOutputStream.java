/* 
 * @(#)$Id$
 *
 * Copyright (c) 2001-2008, The HSQL Development Group
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


package org.hsqldb.lib.tar;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.File;
import java.io.Closeable;
import java.io.Flushable;
import java.util.zip.GZIPOutputStream;

/**
 * Note that this class <b>is not</b> a java.io.FileOutputStream,
 * because our goal is to greatly restrict the public methods of
 * FileOutputStream, yet we must use public methods of the underlying
 * FileOutputStream internally.  Can't accomplish these goals in Java if we
 * subclass.
 * <P>
 * Users write file data by means of populating the provided, public byte array,
 * then calling the single write(int) method to write a portion of that array.
 * This design purposefully goes with efficiency, simplicity, and performance
 * over Java convention, which would not use public fields.
 * <P>
 * At this time, we do not support appending.  That would greatly decrease the
 * generality and simplicity of the our design, since appending is trivial
 * without compression and very difficult with compression.
 * <P>
 * Users must finish tar file creation by using the finish() method.
 * Just like a normal OutputStream, if processing is aborted for any reason,
 * the close() method must be used to free up system resources.
 *
 * @see #finish
 * @see #close
 */
public class TarFileOutputStream implements Closeable, Flushable{
    protected int blocksPerRecord;
    protected long bytesWritten = 0;
    private OutputStream writeStream;
    /* This is not a "Writer", but the byte "Stream" that we write() to. */
    public byte[] writeBuffer;
    /* We purposefully provide no public getter or setter for writeBuffer.
     * No getter because the whole point of this class is that the byte
     * array is a direct, optimally efficient byte array.  No setter because
     * the inside implementation of this class is intimately dependent upon
     * the nature of the write buffer. */
    final public static byte[] ZERO_BLOCK = new byte[512];

    final static public int NO_COMPRESSION = 0;
    final static public int GZIP_COMPRESSION = 1;
    //TODO:  Use an email once Java 1.5 is required.
    final static public int DEFAULT_COMPRESSION = NO_COMPRESSION;
    final static public int DEFAULT_BLOCKS_PER_RECORD = 20;

    /**
     * Convenience wrapper to use default blocksPerRecord and compressionType.
     *
     * @see TarFileOutputStream(File, int, int)
     */
    public TarFileOutputStream(File targetFile) throws IOException {
        this(targetFile, DEFAULT_COMPRESSION);
    }

    /**
     * Convenience wrapper to use default blocksPerRecord.
     *
     * @see TarFileOutputStream(File, int, int)
     */
    public TarFileOutputStream(File targetFile, int compressionType)
            throws IOException {
        this(targetFile, compressionType,
                TarFileOutputStream.DEFAULT_BLOCKS_PER_RECORD);
    }

    /**
     * This class does no validation or enforcement of file naming conventions.
     * If desired, the caller should enforce extensions like "tar" and
     * "tar.gz" (and that they match the specified compression type).
     * <P/>
     * It also overwrites files without warning (just like FileOutputStream).
     */
    public TarFileOutputStream(
            File targetFile, int compressionType, int blocksPerRecord)
            throws IOException {
        this.blocksPerRecord = blocksPerRecord;
        writeBuffer = new byte[blocksPerRecord * 512];
        switch (compressionType) {
            case TarFileOutputStream.NO_COMPRESSION:
                writeStream = new FileOutputStream(targetFile);
                break;
            case TarFileOutputStream.GZIP_COMPRESSION:
                writeStream = new GZIPOutputStream(
                        new FileOutputStream(targetFile));
                break;
            default:
                throw new IllegalArgumentException(
                        "Unexpected compression type: " + compressionType);
        }

        targetFile.setExecutable(false, true);
        targetFile.setExecutable(false, false);
        targetFile.setReadable(false, false);
        targetFile.setReadable(true, true);
        targetFile.setWritable(false, false);
        targetFile.setWritable(true, true);
        // We restrict permissions to the file owner before writing anything,
        // in case we will be writing anything private into this file.
    }

    /**
     * This class and subclasses should write to the underlying writeStream
     * <b>ONLY WITH THIS METHOD</b>.
     * That way we can be confident that bytesWritten will always be accurate.
     */
    public void write(byte[] byteArray, int byteCount) throws IOException {
        writeStream.write(byteArray, 0, byteCount);
        bytesWritten += byteCount;
    }

    /**
     * The normal way to write file data (as opposed to header data or padding)
     * using this class.
     */
    public void write(int byteCount) throws IOException {
        write(writeBuffer, byteCount);
    }

    /**
     * Write a user-specified 512-byte block.
     *
     * For efficiency, write(int) should be used when writing file body content.
     *
     * @see #write(int)
     */
    public void writeBlock(byte[] block) throws IOException {
        if (block.length != 512) {
            throw new IllegalArgumentException(
                    "Specified block is" + block.length
                    + " bytes long instead of 512");
        }
        write(block, block.length);
    }

    /**
     * Writes the specified quantity of zero'd blocks.
     */
    public void writePadBlocks(int blockCount) throws IOException {
        for (int i = 0; i < blockCount; i++) {
            write(ZERO_BLOCK, ZERO_BLOCK.length);
        }
    }

    /**
     * Writes a single zero'd block.
     */
    public void writePadBlock() throws IOException {
        writePadBlocks(1);
    }

    public int bytesLeftInBlock() {
        int modulus = (int) (bytesWritten % 512L);
        if (modulus == 0) return 0;
        return 512 - modulus;
    }

    /**
     * @throws IllegalStateException if end of file not on a block boundary.
     */
    public void assertAtBlockBoundary() {
        if (bytesLeftInBlock() != 0) {
            throw new IllegalStateException("Current file length "
                    + bytesWritten + " is not an even 512-byte-block multiple");
        }
    }

    /**
     * Rounds out the current block to the next block bondary.
     * If we are currently at a block boundary, nothing is done.
     */
    public void padCurrentBlock() throws IOException {
        int padBytes = bytesLeftInBlock();
        if (padBytes == 0) return;
        write(ZERO_BLOCK, padBytes);
// REMOVE THIS DEV-ASSERTION:
assertAtBlockBoundary();
    }

    /**
     * Implements java.io.Flushable.
     *
     * @see java.io.Flushable
     */
    public void flush() throws IOException {
        writeStream.flush();
    }

    /**
     * Implements java.io.Closeable.
     *
     * @see java.io.Closeable
     */
    public void close() throws IOException {
        writeStream.close();
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    /**
     * (Only) when this method returns successfully, the generated file will be
     * a valid tar file.
     *
     * This method always performs a close, so you never need to call the close
     * if your code makes it to this method.
     * (You do need to call close if processing is aborted before calling
     * finish()).
     *
     * @see #close
     */
    public void finish() throws IOException {
        try {
            long finalBlock = bytesWritten / 512 + 2;
            if (finalBlock % blocksPerRecord != 0) {
                // Round up total archive size to a blocksPerRecord multiple
                finalBlock =
                        (finalBlock / blocksPerRecord + 1) * blocksPerRecord;
            }
            int finalPadBlocks = (int) (finalBlock - bytesWritten/512L);
            System.err.println(
                    "Padding archive with " + finalPadBlocks + " zero blocks");
            writePadBlocks(finalPadBlocks);
        } finally {
            close();
        }
    }
}
