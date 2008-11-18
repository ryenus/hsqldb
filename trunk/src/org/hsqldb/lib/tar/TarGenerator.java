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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.ArrayList;
import java.io.FileInputStream;

/**
 * Generates a tar archive from specified Files and InputStreams.
 *
 * @author Blaine Simpson
 */
public class TarGenerator {
    final protected static byte[] ZERO_BLOCK = new byte[512];

    static public void main(String[] sa) throws IOException {
        if (sa.length < 1)
            throw new IllegalArgumentException(
                    "SYNTAX: java " + TarGenerator.class.getName()
                    + " new.tar [entryFile1...]");
        TarGenerator generator = new TarGenerator(sa[0]);
        if (sa.length == 1) {
            generator.queueEntry("stdin", System.in);
        } else {
            for (int i = 1; i < sa.length; i++)
                generator.queueEntry(new File(sa[i]));
        }
        generator.write();
    }

    protected int blocksPerRecord = 20;
    // N.b. the blocksPerRecord is primarily a Tar-file-level concept, not a
    // Tar-Entry-level concept.
    // The only influence on the latter is, we use this as a clue to estimate
    // an efficient read-write buffer size for copying file data.
    // Other htan that, TarEntrySupplicant works only with 512 byte blocks.
    protected File archiveFile;
    protected boolean overWrite = false;
    protected List entryQueue = new ArrayList();

    public TarGenerator(String archivePath) {
        archiveFile = new File(archivePath);
    }

    public void setBlocksPerRecord(int blocksPerRecord) {
        this.blocksPerRecord = blocksPerRecord;
    }
    
    public void setOverwrite(boolean overWrite) {
        this.overWrite = overWrite;
    }
    
    public void queueEntry(File file) throws FileNotFoundException {
        entryQueue.add(new TarEntrySupplicant(null, file));
    }
    public void queueEntry(String entryPath, InputStream inStream) {
        entryQueue.add(new TarEntrySupplicant(entryPath, inStream));
    }

    public void write() throws IOException {
        if (archiveFile.exists() && !overWrite) {
            throw new IOException(
                    "Destination file already exists: "
                    + archiveFile.getAbsolutePath());
        }
        System.err.println(Integer.toString(entryQueue.size())
                    + " supplicants queued for writing...");
        RandomAccessFile archive = new RandomAccessFile(archiveFile, "rw");
        try {
            for (int i = 0; i < entryQueue.size(); i++) {
                ((TarEntrySupplicant) entryQueue.get(i))
                        .write(archive,blocksPerRecord);
                if (archive.getFilePointer() != archive.length()) {
                    throw new IllegalStateException("Entry method did not exit "
                            + "with file pointer at end.  Expected end offset "
                            + archive.length() + ", got "
                            + archive.getFilePointer());
                }
                if (archive.getFilePointer() % 512 != 0) {
                    throw new IllegalStateException(
                            "Entry method did not write even blocks");
                }
                System.err.println(Integer.toString(i + 1)
                        + " / " + entryQueue.size() + " done");
            }
            long finalBlock = archive.length() / 512 + 2;
            if (finalBlock % blocksPerRecord != 0) {
                // Round up total archive size to a blocksPerRecord multiple
                finalBlock =
                        (finalBlock / blocksPerRecord + 1) * blocksPerRecord;
            }
            System.err.println("Padding archive with "
                    + (finalBlock - archive.length() / 512) + " zero blocks");
            for (long block = archive.length() / 512;
                    block < finalBlock; block++) {
                archive.write(TarGenerator.ZERO_BLOCK);
            }
        } finally {
            archive.close();
        }
    }

    /**
     * Slots for supplicant files and input streams to be added to a Tar archive.
     *
     * @author Blaine Simpson
     */
    static protected class TarEntrySupplicant {
        static protected byte[] HEADER_TEMPLATE = new byte[512];
        final protected static byte[] ustarBytes = { 'u', 's', 't', 'a', 'r' };
        static {
            for (int i = 108; i < 115; i++) {
                // uid field
                HEADER_TEMPLATE[i] = '0';
            }
            for (int i = 116; i < 123; i++) {
                // uid field
                HEADER_TEMPLATE[i] = '0';
            }
            writeField(TarHeaderFields.OWNERNAME,
                    System.getProperty("user.name"), HEADER_TEMPLATE);
            // UStar owner name field
            for (int i = 0; i < ustarBytes.length; i++) {
                // UStar magic field
                HEADER_TEMPLATE[257 + i] = ustarBytes[i];
            }
            HEADER_TEMPLATE[263] = '0';
            HEADER_TEMPLATE[264] = '0';
            // UStar version field, version = 00
            // This is the field that Gnu Tar desecrates.
        }

        static protected void writeField(
                int fieldId, String newValue, byte[] target) {
            int start = TarHeaderFields.getStart(fieldId);
            int stop = TarHeaderFields.getStop(fieldId);
            byte[] ba = newValue.getBytes();
            if (ba.length > stop - start) {
                throw new IllegalArgumentException(
                        "Input too long for field " + fieldId + ": "
                        + newValue);
            }
            for (int i = 0; i < ba.length; i++) target[start + i] = ba[i];
        }
        protected byte[] rawHeader = (byte[]) HEADER_TEMPLATE.clone();
        protected File file = null;
        protected InputStream inputStream = null;
        protected String path;

        public TarEntrySupplicant(String path, File file)
                throws FileNotFoundException {
            this(((path == null) ? file.getPath() : path),
                    new FileInputStream(file));
            // Purposefully do not buffer.
            // We set up our own read-write buffer tuned to caller's
            // specification.
            this.file = file;
            if (!file.isFile()) {
                throw new IllegalArgumentException("This method intentionally "
                       + "creates TarEntries only for files");
            }
            if (!file.isFile()) {
                throw new IllegalArgumentException(
                        "Can't read file '" + file + "'");
            }
        }

        public TarEntrySupplicant(String path, InputStream inputStream) {
            this.inputStream = inputStream;
            if (path == null) {
                throw new IllegalArgumentException("Path required if "
                    + "existing component file not specified");
            }
            this.path = path;
            // TODO:  Check if this.file.separator (a final field which we
            // can't change) != '/'.  Of so , we should iterate throug all
            // of the elements of path to replace with '/', since tar
            // should only work with /-separated directories.
        }

        protected long headerChecksum() {
            long sum = 0;
            for (int i = 0; i < rawHeader.length; i++) {
                sum += (i >= TarHeaderFields.getStart(TarHeaderFields.CHECKSUM)
                        && i < TarHeaderFields.getStop(
                        TarHeaderFields.CHECKSUM)) ? 32 : (255 & rawHeader[i]);
                // We ignore current contents of the checksum field so that
                // this method will continue to work right, even if we later
                // recycle the header or RE-calculate a header.
            }
            return sum;
        }

        protected void writeField(int fieldId, String newValue) {
            TarEntrySupplicant.writeField(fieldId, newValue, rawHeader);
        }

        protected String prePaddedOctalString(long val, int width) {
            StringBuffer sb = new StringBuffer(Long.toOctalString(val));
            int needZeros = width - sb.length();
            while (needZeros-- > 0) sb.insert(0, '0');
            return sb.toString();
        }

        protected void writeField(int fieldId, long newValue) {
            writeField(fieldId, prePaddedOctalString(newValue,
                    TarHeaderFields.getStop(fieldId)
                    - TarHeaderFields.getStart(fieldId)));
        }

        /**
         * If exits successfully, always leaves pointer at very end of file.
         *
         * @param raf RandomAccessFile with pointer positioned exactly where
         *            the new TarEntrySupplicant should be inserted.
         * @recSize This method will write an integer multiple of recSize
         *          512-byte blocks.  I.e. recSize is in units of 512 bytes.
         *          (We will also buffer read/write pipes to this size).
         */
        public void write(RandomAccessFile raf, int recSize)
                throws IOException {
            int bytesCopied = 0;
            int i;
            byte[] buffer = new byte[recSize * 512];
            long startPointer = raf.getFilePointer();

            raf.write(TarGenerator.ZERO_BLOCK);
            // forwards pointer to where we write file data

            while ((i = inputStream.read(buffer)) > 0) {
                raf.write(buffer, 0, i);
                bytesCopied += i;
            }
            inputStream.close();
            int modulusBytes = bytesCopied % 512;
            if (modulusBytes > 0) {
                raf.write(TarGenerator.ZERO_BLOCK, 0, 512 - modulusBytes);
                System.err.println("Padded with " + (512 - modulusBytes)
                        + " bytes to make total tar file size now "
                        + raf.length());  // TODO: Remove this debug statement
            }
            raf.seek(startPointer);

            // We have the file size, so we will now assemble header in memory
            writeField(TarHeaderFields.FILEPATH, path);
            // TODO:  If path.length() > 99, then attempt to split into
            // PATHPREFIX and FILEPATH fields.
            writeField(TarHeaderFields.FILEMODE, getLameMode());
            writeField(TarHeaderFields.SIZE, bytesCopied);
            writeField(TarHeaderFields.MODTIME, ((file == null)
                    ? new java.util.Date().getTime()
                    : file.lastModified()) / 1000);
            writeField(TarHeaderFields.CHECKSUM, prePaddedOctalString(
                    headerChecksum(), 6) + "\0 ");
            // Silly, but that's what the base header spec calls for.

            raf.write(rawHeader);
            raf.seek(raf.length());
        }

        protected String getLameMode() {
            if (file == null) return DEFAULT_FILE_MODES;
            int umod = file.canExecute() ? 1 : 0;
            if (file.canWrite()) umod += 2;
            if (file.canRead()) umod += 4;
            return "0" + umod + "00";
            // Conservative since Java gives us no way to determine group or
            // other privileges on a file, and this file may contain passwords.
        }

        final static public String DEFAULT_FILE_MODES = "600";
        // Be conservative, because these files contain passwords
    }
}
