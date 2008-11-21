/* Copyright (c) 2001-2008, The HSQL Development Group
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
import java.util.List;
import java.util.ArrayList;
import java.io.FileInputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

/**
 * Generates a tar archive from specified Files and InputStreams.
 *
 * @author Blaine Simpson
 */
public class TarGenerator {
    /**
     * Creates specified tar file to contain specified files, or stdin,
     * using default blocks-per-record and replacing tar file if it already
     * exists.
     */
    static public void main(String[] sa) throws IOException {
        if (sa.length < 1) {
            System.err.println(
                    "SYNTAX: java " + TarGenerator.class.getName()
                    + " new.tar [entryFile1...]\n"
                    + "If no entryFiles are specified, stdin will be read to "
                    + "write an entry with name 'stdin'.\n"
                    + " In this latter case, input is limited to 10240 bytes");
            System.exit(0);
        }
        TarGenerator generator = new TarGenerator(new File(sa[0]), true, null);
        if (sa.length == 1) {
            generator.queueEntry("stdin", System.in, 10240);
        } else {
            for (int i = 1; i < sa.length; i++) {
                generator.queueEntry(new File(sa[i]));
            }
        }
        generator.write();
    }

    protected TarFileOutputStream archive;
    protected List entryQueue = new ArrayList();

    /**
     * Compression is determined directly by the suffix of the file name in
     * the specified path.
     *
     * @param archiveFile  Absolute or relative (from user.dir) File for
     *                     tar file to be created.  getName() Suffix must
     *                     indicate tar file and may indicate a compression
     *                     method.
     * @param overWrite    True to replace an existing file of same path.
     * @param blocksPerRecord  Null will use default tar value.
     */
    public TarGenerator(
            File inFile, boolean overWrite, Integer blocksPerRecord)
            throws IOException {
        File archiveFile = inFile.getAbsoluteFile();
        // Do this so we can be sure .getParent*() is non-null.  (Also allows
        // us to use .getPath() instead of very long .getAbsolutePath() for
        // error messages.

        int compression = TarFileOutputStream.NO_COMPRESSION;
        if (archiveFile.getName().endsWith(".tgz")
                || archiveFile.getName().endsWith(".tar.gz")) {
                compression = TarFileOutputStream.GZIP_COMPRESSION;
        } else if (archiveFile.getName().endsWith(".tar")) {
        } else {
            throw new IllegalArgumentException(
                getClass().getName() + " only generates files with extensions "
                + "'.tar', '.tgz.', or '.tar.gz':  " + archiveFile.getPath());
        }
        if (archiveFile.exists()) {
            if (!overWrite) {
                throw new IOException("Destination file already exists: "
                        + archiveFile.getPath());
            }
        } else {
            File parentDir = archiveFile.getParentFile();
            // parentDir will be absolute, since archiveFile is absolute.
            if (parentDir.exists()) {
                if (!parentDir.isDirectory()) {
                    throw new IOException(
                        "Parent node of specified file is not a directory: "
                        + parentDir.getPath());
                }
                if (!parentDir.canWrite()) {
                    throw new IOException(
                        "Parent directory of specified file is not writable: "
                        + parentDir.getPath());
                }
            } else {
                if (!parentDir.mkdirs()) {
                    throw new IOException(
                        "Failed to create parent directory for tar file: "
                        + parentDir.getPath());
                }
            }
        }
        archive = (blocksPerRecord == null)
                  ? new TarFileOutputStream(archiveFile, compression)
                  : new TarFileOutputStream(archiveFile, compression,
                          blocksPerRecord.intValue());
        if (blocksPerRecord != null) {
            System.out.println("Will write at " + blocksPerRecord
                    + " blocks-per-record");
        }
    }

    public void queueEntry(File file) throws FileNotFoundException {
        queueEntry(null, file);
    }
    public void queueEntry(String entryPath, File file)
            throws FileNotFoundException {
        entryQueue.add(new TarEntrySupplicant(entryPath, file, archive));
    }
    public void queueEntry(
            String entryPath, InputStream inStream, int maxBytes)
            throws IOException {
        entryQueue.add(
                new TarEntrySupplicant(entryPath, inStream, maxBytes, archive));
    }

    /**
     * This method does release all of the streams, even if there is a failure.
     */
    public void write() throws IOException {
        System.err.println(Integer.toString(entryQueue.size())
                    + " supplicants queued for writing...");
        TarEntrySupplicant entry;
        try {
            for (int i = 0; i < entryQueue.size(); i++) {
                System.err.print(Integer.toString(i + 1)
                        + " / " + entryQueue.size() + ' ');
                entry = (TarEntrySupplicant) entryQueue.get(i);
                System.err.print(entry.getPath() + "... ");
                entry.write();
                archive.assertAtBlockBoundary();
                System.err.println("Done");
            }
            archive.finish();
        } catch (IOException ioe) {
            System.err.println("Failed");
            // Just release resources from any Entry's input, which may be
            // left open.
            for (int i = 0; i < entryQueue.size(); i++) {
                ((TarEntrySupplicant) entryQueue.get(i)).close();
            }
            throw ioe;
        } finally {
            archive.close();
        }
    }

    /**
     * Slots for supplicant files and input streams to be added to a Tar
     * archive.
     *
     * @author Blaine Simpson
     */
    static protected class TarEntrySupplicant {
        static protected byte[] HEADER_TEMPLATE =
                TarFileOutputStream.ZERO_BLOCK.clone();
        static Character swapOutDelim = null;
        final protected static byte[] ustarBytes = { 'u', 's', 't', 'a', 'r' };
        static {
            char c = System.getProperty("file.separator").charAt(0);
            if (c != '/') {
                swapOutDelim = new Character(c);
            }
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
            writeField(TarHeaderFields.TYPE, "0", HEADER_TEMPLATE);
            // Difficult call here.  binary 0 and character '0' both mean
            // regular file.  Binary 0 pre-UStar and probably more portable,
            // but we are writing a valid UStar header, and I doubt anybody's
            // tar implementation would choke on this since there is no
            // outcry of UStar archives failing to work with older tars.
            int magicStart = TarHeaderFields.getStart(TarHeaderFields.MAGIC);
            for (int i = 0; i < ustarBytes.length; i++) {
                // UStar magic field
                HEADER_TEMPLATE[magicStart + i] = ustarBytes[i];
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
                        "Input too long for field "
                        + TarHeaderFields.toString(fieldId) + ": " + newValue);
            }
            for (int i = 0; i < ba.length; i++) {
                target[start + i] = ba[i];
            }
        }
        protected byte[] rawHeader = (byte[]) HEADER_TEMPLATE.clone();
        protected String fileMode = DEFAULT_FILE_MODES;

        // Following fields are always initialized by constructors.
        protected InputStream inputStream;
        protected String path;
        protected long modTime;
        protected TarFileOutputStream tarStream;
        protected long dataSize;  // In bytes

        public String getPath() {
            return path;
        }

        /*
         * Internal constructor that validates the entry's path.
         */
        protected TarEntrySupplicant(String path,
                TarFileOutputStream tarStream) {
            if (path == null) {
                throw new IllegalArgumentException("Path required if "
                    + "existing component file not specified");
            }
            this.path = (swapOutDelim == null
                      ? path
                      : path.replace(swapOutDelim.charValue(), '/'));
            this.tarStream = tarStream;
        }

        /**
         * After instantiating a TarEntrySupplicant, the user must either invoke
         * write() or close(), to release system resources on the input
         * File/Stream.
         */
        public TarEntrySupplicant(
                String path, File file, TarFileOutputStream tarStream)
                throws FileNotFoundException {
            this(((path == null) ? file.getPath() : path), tarStream);
            if (!file.isFile()) {
                throw new IllegalArgumentException("This method intentionally "
                       + "creates TarEntries only for files");
            }
            if (!file.canRead()) {
                throw new IllegalArgumentException(
                        "Can't read file '" + file + "'");
            }
            modTime = file.lastModified() / 1000L;
            fileMode = TarEntrySupplicant.getLameMode(file);
            dataSize = file.length();
            inputStream = new FileInputStream(file);
        }

        /**
         * After instantiating a TarEntrySupplicant, the user must either invoke
         * write() or close(), to release system resources on the input
         * File/Stream.
         * <P/>
         * <B>WARNING:</B>
         * Do not use this method unless the quantity of available RAM is
         * sufficient to accommodate the specified maxBytes all at one time.
         * This constructor loads all input from the specified InputStream into
         * RAM before anything is written to disk.
         *
         * @param maxBytes This method will fail if more than maxBytes bytes
         *                 are supplied on the specified InputStream.
         */
        public TarEntrySupplicant(String path, InputStream origStream,
                int maxBytes, TarFileOutputStream tarStream)
                throws IOException {
            /**
             * If you modify this, make sure to not intermix reading/writing of
             * the PipedInputStream and the PipedOutputStream, or you could
             * cause dead-lock.  Everything is safe if you close the
             * PipedOutputStream before reading the PipedInputStream.
             */
            this(path, tarStream);
            int i;
            PipedOutputStream outPipe = new PipedOutputStream();
            inputStream = new PipedInputStream(outPipe, maxBytes);
            try {
                while ((i = origStream.read(tarStream.writeBuffer, 0,
                        tarStream.writeBuffer.length)) > 0) {
                    outPipe.write(tarStream.writeBuffer, 0, i);
                }
                dataSize = inputStream.available();
                System.err.println("Buffered " + dataSize
                    + " bytes from given InputStream into RAM");
            } catch (IOException ioe) {
                inputStream.close();
                throw ioe;
            } finally {
                outPipe.close();
            }
            modTime = new java.util.Date().getTime() / 1000L;
        }

        public void close() throws IOException {
            inputStream.close();
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
            while (needZeros-- > 0) {
                sb.insert(0, '0');
            }
            return sb.toString();
        }

        protected void writeField(int fieldId, long newValue) {
            writeField(fieldId, prePaddedOctalString(newValue,
                    TarHeaderFields.getStop(fieldId)
                    - TarHeaderFields.getStart(fieldId)));
        }

        /**
         * Writes entire entry to this object's tarStream.
         *
         * This method is guaranteed to close the supplicant's input stream.
         */
        public void write() throws IOException {
            int i;

            try {
                writeField(TarHeaderFields.FILEPATH, path);
                // TODO:  If path.length() > 99, then attempt to split into
                // PATHPREFIX and FILEPATH fields.
                writeField(TarHeaderFields.FILEMODE, fileMode);
                writeField(TarHeaderFields.SIZE, dataSize);
                writeField(TarHeaderFields.MODTIME, modTime);
                writeField(TarHeaderFields.CHECKSUM, prePaddedOctalString(
                        headerChecksum(), 6) + "\0 ");
                // Silly, but that's what the base header spec calls for.
                tarStream.writeBlock(rawHeader);

                long dataStart = tarStream.getBytesWritten();
                while ((i = inputStream.read(tarStream.writeBuffer)) > 0) {
                    tarStream.write(i);
                }
                if (dataStart + dataSize != tarStream.getBytesWritten()) {
                    throw new IOException(
                            "Seems that the input data changed.  "
                            + "Input data was " + dataSize
                            + " bytes, but we wrote "
                            + (tarStream.getBytesWritten() - dataStart)
                            + " bytes of data");
                }
                tarStream.padCurrentBlock();
            } finally {
                close();
            }
        }

        static protected String getLameMode(File file) {
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
