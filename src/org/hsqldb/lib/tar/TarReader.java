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
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.util.regex.Pattern;
import java.text.SimpleDateFormat;

/**
 * Reads a Tar file for reporting or extraction.
 * N.b. this is not a <I>Reader</I> in the <CODE>java.io.Reader</CODE> sense,
 * but in the sense of differentiating <CODE>tar x</CODE> and
 * <CODE>tar t</CODE> from <CODE>tar c</CODE>.
 *
 * @author Blaine Simpson
 */
public class TarReader {
    final static public int LIST_MODE = 0;
    /**
     * EXTRACT_MODE refuses to overwrite existing files.
     */
    final static public int EXTRACT_MODE = 1;
    /**
     * OVERWRITE_MODE is just EXTRACT_MODE where we will silently overwrite
     * existing files upon extraction.
     */
    final static public int OVERWRITE_MODE = 2;

    /**
     * Reads a specified tar file or stdin in order to either list or extract
     * the file tar entries, depending on the first argument being "t" or "x",
     * using default read buffer blocks.
     */
    static public void main(String[] sa)
            throws IOException, TarMalformatException {
        if (sa.length < 2
                || ((!sa[0].equals("t")) && !sa[0].equals("x"))) {
            throw new IllegalArgumentException(
                    "SYNTAX: java " + TarReader.class.getName()
                    + " {t|x} tar/file/path [pattern1...]");
        }
        String[] patternStrings = null;
        if (sa.length > 2) {
            patternStrings = new String[sa.length - 2];
            for (int i = 2; i < sa.length; i++) {
                patternStrings[i - 2] = sa[i];
            }
        }
        new TarReader(sa[1],
                (sa[0].equals("t") ? LIST_MODE : EXTRACT_MODE),
                patternStrings, null).read();
    }

    protected TarFileInputStream archive;
    protected Pattern[] patterns = null;
    protected int mode;

    /**
     * Compression is determined directly by the suffix of the file name in
     * the specified path.
     *
     * @param archivePath  Absolute or relative (from user.dir) path to
     *                     tar file to be read.  Suffix may indicate
     *                     a compression method.
     * @param mode    Whether to list, extract-without-overwrite, or
     *                extract-with-overwrite.
     * @param patterns  List of regular expressions to match against tar entry
     *                  names.  If null, all entries will be listed or
     *                  extracted.  If non-null, then only entries with names
     *                  which match will be extracted or listed.
     * @param readBufferBlocks  Null will use default tar value.
     * @throws IllegalArgumentException if any given pattern is an invalid
     *                  regular expression.  Don't have to worry about this if
     *                  you call with null 'patterns' param.
     *
     * @see java.util.regex.Pattern
     */
    public TarReader(String archivePath, int mode, String[] patternStrings,
            Integer readBufferBlocks) throws IOException {
        this.mode = mode;
        File archiveFile = new File(archivePath).getAbsoluteFile();
        int compression = TarFileOutputStream.NO_COMPRESSION;
        if (archiveFile.getName().endsWith(".tgz")
                || archiveFile.getName().endsWith(".gz")) {
                compression = TarFileOutputStream.GZIP_COMPRESSION;
        }
        if (patternStrings != null) {
            patterns = new Pattern[patternStrings.length];
            for (int i = 0; i < patternStrings.length; i++) {
                patterns[i] = Pattern.compile(patternStrings[i]);
            }
        }
        // Don't check for archive file existence here.  We can depend upon the
        // TarFileInputStream to check that.
        archive = (readBufferBlocks == null)
                  ? new TarFileInputStream(archiveFile, compression)
                  : new TarFileInputStream(archiveFile, compression,
                          readBufferBlocks.intValue());
    }
    
    public void read() throws IOException, TarMalformatException {
        TarEntryHeader header;
        boolean anyUnsupporteds = false;
        boolean matched;
        try {
            EACH_HEADER:
            while (archive.readNextHeaderBlock()) {
                header = new TarEntryHeader(archive.readBuffer);
                if (patterns != null) {
                    matched = false;
                    for (int i = 0; i < patterns.length; i++) {
                        if (patterns[i].matcher(header.getPath()).matches()) {
                            matched = true;
                            break;
                        }
                    }
                    if (!matched) {
                        skipFileData(header);
                        continue EACH_HEADER;
                    }
                }
                switch (mode) {
                    case LIST_MODE:
                        System.out.println(header.toString());
                        skipFileData(header);
                        break;
                    case EXTRACT_MODE:
                    case OVERWRITE_MODE:
                        // Instance variable mode will be used to differentiate
                        // behavior inside of extractFile().
                        //System.out.println(header.toString());
                        extractFile(header);
                        break;
                    default:
                        throw new RuntimeException(
                                "Sorry, mode not supported yet: " + mode);
                }
            }
            if (anyUnsupporteds) {
                System.out.println("Archive contains unsupported entry type(s)."
                        + "  We only support default type (1st column).");
            }
        } catch (IOException ioe) {
            archive.close();
            throw ioe;
        }
    }

    protected void extractFile(TarEntryHeader header)
            throws IOException, TarMalformatException {
        int readNow;
        int readBlocks = (int) (header.getDataSize() / 512L);
        int modulus = (int) (header.getDataSize() % 512L);
        File newFile = header.generateFile();
        File parentDir = newFile.getParentFile();
        if (newFile.exists()) {
            if (mode != TarReader.OVERWRITE_MODE) {
                throw new IOException("Extracted file already exists: "
                        + newFile.getAbsolutePath());
            }
            if (!newFile.isFile()) {
                throw new IOException(
                        "Node already exist but is not a file: "
                        + newFile.getAbsolutePath());
            }
            // Better to let FileOutputStream creation zero it than to
            // to newFile.delete().
        }
        FileOutputStream outStream = new FileOutputStream(newFile);
        if (parentDir.exists()) {
            if (!parentDir.isDirectory()) {
                throw new IOException(
                    "Parent node of extracted path is not a directory: "
                    + parentDir.getAbsolutePath());
            }
            if (!parentDir.canWrite()) {
                throw new IOException(
                    "Parent directory of extracted path is not writable: "
                    + parentDir.getAbsolutePath());
            }
        } else {
            if (!parentDir.mkdirs()) {
                throw new IOException(
                    "Failed to create parent directory for extracted file: "
                    + parentDir.getAbsolutePath());
            }
        }
        try {
            while (readBlocks > 0) {
                readNow = (readBlocks > archive.getReadBufferBlocks())
                         ? archive.getReadBufferBlocks()
                         : readBlocks;
                archive.readBlocks(readNow);
                readBlocks -= readNow;
                outStream.write(archive.readBuffer, 0, readNow * 512);
            }
if (readBlocks != 0) throw new IllegalStateException(
"Finished skipping but skip blocks to go = " + readBlocks + "?");
// TODO:  Remove this Dev debug assertion
            if (modulus != 0) {
                archive.readBlock();
                outStream.write(archive.readBuffer, 0, modulus);
            }
            outStream.flush();
        } finally {
            outStream.close();
        }
        if (newFile.length() != header.getDataSize()) {
            throw new IOException("Attempted to write " + header.getDataSize()
                    + " bytes to '" + newFile.getAbsolutePath()
                    + "', but only wrote " + newFile.length());
        }
    }

    protected void skipFileData(TarEntryHeader header)
            throws IOException, TarMalformatException {
        int skipNow;
        int skipBlocks = (int) (header.getDataSize() / 512L)
                + ((header.getDataSize() % 512L == 0L) ? 0 : 1);
        while (skipBlocks > 0) {
            skipNow = (skipBlocks > archive.getReadBufferBlocks())
                     ? archive.getReadBufferBlocks()
                     : skipBlocks;
            archive.readBlocks(skipNow);
            skipBlocks -= skipNow;
        }
if (skipBlocks != 0) throw new IllegalStateException(
"Finished skipping but skip blocks to go = " + skipBlocks + "?");
// TODO:  Remove this Dev debug assertion
    }

    /**
     * A Tar entry header constituted from a header block in a tar file.
     *
     * @author Blaine Simpson
     */
    static protected class TarEntryHeader {
        protected SimpleDateFormat sdf =
                new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        /**
         * @param rawHeader  May be longer than 512 bytes, but the first 512
         *                   bytes MUST COMPRISE a raw tar entry header.
         */
        public TarEntryHeader(byte[] rawHeader) throws TarMalformatException {
            this.rawHeader = rawHeader;
            Long expectedCheckSum = readInteger(TarHeaderFields.CHECKSUM);
            if (expectedCheckSum == null) {
                throw new TarMalformatException(
                    "Required field 'CHECKSUM' missing in tar entry header");
            }
            long calculatedCheckSum = headerChecksum();
            if (expectedCheckSum.longValue() != calculatedCheckSum) {
                throw new TarMalformatException(
                    "Corrupted tar entry header.  Expected checksum "
                    + expectedCheckSum + ", but calculated "
                    + calculatedCheckSum);
            }
            path = readString(TarHeaderFields.FILEPATH);
            if (path == null) {
                throw new TarMalformatException(
                    "Required field 'FILEPATH' missing in tar entry header");
            }
            Long longObject = readInteger(TarHeaderFields.FILEMODE);
            if (longObject == null) {
                throw new TarMalformatException(
                    "Required field 'FILEMODE' missing in tar entry header");
            }
            fileMode = (int) longObject.longValue();
            longObject = readInteger(TarHeaderFields.SIZE);
            if (longObject == null) {
                throw new TarMalformatException(
                    "Required field 'SIZE' missing in tar entry header.  "
                    + "Sorry if it is set via Pax Interchange Format.  "
                    + "We haven't implemented that yet");
            }
            dataSize = longObject.longValue();
            longObject = readInteger(TarHeaderFields.MODTIME);
            if (longObject == null) {
                throw new TarMalformatException(
                    "Required field 'MODTIME' missing in tar entry header.");
            }
            modTime = longObject.longValue();
            String typeString = readString(TarHeaderFields.TYPE);
            if (typeString != null) {
                entryType = typeString.charAt(0);
            }
            ownerName = readString(TarHeaderFields.OWNERNAME);
            String pathPrefix = readString(TarHeaderFields.PATHPREFIX);
            if (pathPrefix != null) {
                path = pathPrefix + '/' + path;
            }
            ustar = isUstar();
        }
        protected byte[] rawHeader;
        /* CRITICALLY IMPORTANT:  TO NOT USE rawHeader.length OR DEPEND ON
         * THE LENGTH OF the rawHeader ARRAY!  Use only the first 512 bytes!
         */
        protected String path;
        protected int fileMode;
        protected long dataSize;  // In bytes
        protected long modTime;
        protected char entryType = '\0';
        protected String ownerName;
        protected boolean ustar;

        /**
         * @returns a new Absolutized File object generated from this
         * TarEntryHeader.
         */
        public File generateFile() {
            if (entryType != '\0' && entryType != '0') {
                throw new IllegalStateException(
                        "At this time, we only support creation of normal "
                        + "files from Tar entries");
            }
            File newFile = new File(path).getAbsoluteFile();
            newFile.setLastModified(modTime * 1000);
            newFile.setExecutable(false, false);
            newFile.setReadable(false, false);
            newFile.setWritable(false, false);
            newFile.setExecutable(((fileMode & 0100) != 0), true);
            newFile.setReadable((fileMode & 0400) != 0, true);
            newFile.setWritable((fileMode & 0200) != 0, true);
            return newFile;
        }

        public String getPath() {
            return path;
        }
        public long getDataSize() {
            return dataSize;
        }
        public String toString() {
            StringBuffer sb = new StringBuffer(
                    (entryType == '\0') ? ' ' : entryType);
            sb.append(ustar ? '*' : ' ');
            sb.append(' ' + sdf.format(modTime) + ' '
                    + Integer.toOctalString(fileMode) + "  "
                    + dataSize + "  ");
            sb.append((ownerName == null) ? '-' : ownerName);
            sb.append("  " + path);
            return sb.toString();
        }

        /**
         * Is this any UStar variant
         */
        public boolean isUstar() throws TarMalformatException {
            String magicString = readString(TarHeaderFields.MAGIC);
            return magicString != null && magicString.startsWith("ustart");
        }

        /**
         * @returns index based at 0 == from
         */
        static public int indexOf(byte[] ba, byte val, int from, int to) {
            for (int i = from; i < to; i++) {
                if (ba[i] == val) {
                    return i - from;
                }
            }
            return -1;
        }

        /**
         * Returns null instead of 0-length Strings.
         */
        protected String readString(int fieldId) throws TarMalformatException {
            int start = TarHeaderFields.getStart(fieldId);
            int stop = TarHeaderFields.getStop(fieldId);
            int termIndex =
                    TarEntryHeader.indexOf(rawHeader, (byte) 0, start, stop);
            switch (termIndex) {
                case 0:
                    return  null;
                case -1:
                    termIndex = stop - start;
                    break;
            }
            try {
                return new String(rawHeader, start, termIndex);
            } catch (Throwable t) {
                // Java API does not specify behavior if decoding fails.
                throw new TarMalformatException(
                        "Bad value in header for field "
                        + TarHeaderFields.toString(fieldId));
            }
        }

        /**
         * Integer as in positive whole number, which does not imply Java
         * types of <CODE>int</CODE> or <CODE>Integer</CODE>.
         */
        protected Long readInteger(int fieldId) throws TarMalformatException {
            try {
                return Long.valueOf(readString(fieldId), 8);
            } catch (NumberFormatException nfe) {
                throw new TarMalformatException(
                        "Bad value in header for field "
                        + TarHeaderFields.toString(fieldId) + ": "
                        + nfe.getMessage());
            }
        }

        protected long headerChecksum() {
            long sum = 0;
            for (int i = 0; i < 512; i++) {
                sum += (i >= TarHeaderFields.getStart(TarHeaderFields.CHECKSUM)
                        && i < TarHeaderFields.getStop(
                        TarHeaderFields.CHECKSUM)) ? 32 : (255 & rawHeader[i]);
                // We ignore current contents of the checksum field so that
                // this method will continue to work right, even if we later
                // recycle the header or RE-calculate a header.
            }
            return sum;
        }
    }
}
