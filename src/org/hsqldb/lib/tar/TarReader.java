package org.hsqldb.lib.tar;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

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
    final static private String SYNTAX_MSG = "SYNTAX: java "
        + TarReader.class.getName()
        + " {t|x} [--directory=path] file/path/tar[.gz] [regex1...]";

    /**
     * Reads a specified tar file or stdin in order to either list or extract
     * the file tar entries, depending on the first argument being "t" or "x",
     * using default read buffer blocks.
     */
    static public void main(String[] sa)
    throws IOException, TarMalformatException {

        if (sa.length < 1) {
            System.err.println(SYNTAX_MSG);
            System.exit(0);
        }

        File exDir = (sa.length > 1 && sa[1].startsWith("--directory="))
                     ? (new File(sa[1].substring("--directory=".length())))
                     : null;
        int firstPatInd = (exDir == null) ? 2
                                          : 3;

        if (sa.length < firstPatInd
                || ((!sa[0].equals("t")) && !sa[0].equals("x"))) {
            throw new IllegalArgumentException("Run 'java "
                                               + TarReader.class.getName()
                                               + "' for help");
        }

        String[] patternStrings = null;

        if (sa.length > firstPatInd) {
            patternStrings = new String[sa.length - firstPatInd];

            for (int i = firstPatInd; i < sa.length; i++) {
                patternStrings[i - firstPatInd] = sa[i];
            }
        }

        if (sa[0].equals("t") && exDir != null) {
            throw new IllegalArgumentException(
                "The '--directory=' switch only makes sense with 'x' mode");
        }

        int dirIndex      = (exDir == null) ? 1
                                            : 2;
        int tarReaderMode = sa[0].equals("t") ? LIST_MODE
                                              : EXTRACT_MODE;

        new TarReader(new File(sa[dirIndex]), tarReaderMode, patternStrings,
                      null, exDir).read();
    }

    protected TarFileInputStream archive;
    protected Pattern[]          patterns = null;
    protected int                mode;
    protected File               extractBaseDir;    // null means current directory

    // Not used for Absolute path entries
    // This path is always absolutized

    /**
     * Compression is determined directly by the suffix of the file name in
     * the specified path.
     *
     * @param inFile  Absolute or relative (from user.dir) path to
     *                tar file to be read.  Suffix may indicate
     *                a compression method.
     * @param mode    Whether to list, extract-without-overwrite, or
     *                extract-with-overwrite.
     * @param patterns  List of regular expressions to match against tar entry
     *                  names.  If null, all entries will be listed or
     *                  extracted.  If non-null, then only entries with names
     *                  which match will be extracted or listed.
     * @param readBufferBlocks  Null will use default tar value.
     * @param inDir   Directory that RELATIVE entries will be extracted
     *                relative to.  Defaults to current directory (user.dir).
     *                Only used for extract modes and relative file entries.
     * @throws IllegalArgumentException if any given pattern is an invalid
     *                  regular expression.  Don't have to worry about this if
     *                  you call with null 'patterns' param.
     * @see java.util.regex.Pattern
     */
    public TarReader(File inFile, int mode, String[] patternStrings,
                     Integer readBufferBlocks, File inDir) throws IOException {

        this.mode = mode;

        File archiveFile = inFile.getAbsoluteFile();

        extractBaseDir = (inDir == null) ? null
                                         : inDir.getAbsoluteFile();

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
        boolean        anyUnsupporteds = false;
        boolean        matched;
        Long           paxSize = null;
        String         paxString = null;

        try {
            EACH_HEADER:
            while (archive.readNextHeaderBlock()) {
                header = new TarEntryHeader(archive.readBuffer);
                char entryType = header.getEntryType();

                if (entryType == 'x') {
                    /* Since we don't know the name of the target file yet,
                     * we must load the size from all pax headers.
                     * If the target file is not thereafter excluded via
                     * patterns, we will need this size for the listing or to
                     * extract the data.
                     */
                    paxSize = getPifData(header).getSize();
                    paxString = header.toString();
                    continue;
                }

                if (paxSize != null) {
                    // Ignore "size" field in the entry header because PIF
                    // setting overrides.
                    header.setDataSize(paxSize.longValue());
                    paxSize = null;
                }

                if (patterns != null) {
                    matched = false;

                    for (int i = 0; i < patterns.length; i++) {
                        if (patterns[i].matcher(header.getPath()).matches()) {
                            matched = true;

                            break;
                        }
                    }

                    if (!matched) {
                        paxString = null;
                        skipFileData(header);

                        continue EACH_HEADER;
                    }
                }

                switch (mode) {

                    case LIST_MODE :
                        if (paxString != null) {
                            System.out.println(paxString);
                        }
                        System.out.println(header.toString());

                        if (entryType != '\0' && entryType != '0'
                                && entryType != 'x') {
                            anyUnsupporteds = true;
                        }

                        skipFileData(header);
                        break;

                    case EXTRACT_MODE :
                    case OVERWRITE_MODE :

                        // Don't need to check type here, since we will throw
                        // if typeflag is unsupported.
                        //
                        // Instance variable mode will be used to differentiate
                        // behavior inside of extractFile().
                        //System.out.println(header.toString());
                        extractFile(header);
                        if (paxString != null) {
                            System.out.println(paxString);
                        }
                        // Display entry summary after successful extraction
                        System.out.println(header.toString());

                        break;

                    default :
                        throw new RuntimeException(
                            "Sorry, mode not supported yet: " + mode);
                }
                paxString = null;
            }

            if (anyUnsupporteds) {
                System.out.println(
                    "Archive contains unsupported entry type(s)."
                    + "  We only support default type (1st column).");
            }
        } catch (IOException ioe) {
            archive.close();

            throw ioe;
        }
    }

    protected PIFData getPifData(TarEntryHeader header)
    throws IOException, TarMalformatException {
        /*
         * If you modify this, make sure to not intermix reading/writing of
         * the PipedInputStream and the PipedOutputStream, or you could
         * cause dead-lock.  Everything is safe if you close the
         * PipedOutputStream before reading the PipedInputStream.
         */

        long dataSize = header.getDataSize();
        if (dataSize < 1) {
            throw new TarMalformatException("PIF Data size unknown");
        }

        if (dataSize > Integer.MAX_VALUE) {
            throw new TarMalformatException(
                    "PIF Data exceeds max supported size.  "
                    + dataSize + " > " + Integer.MAX_VALUE);
        }

        int  readNow;
        int  readBlocks = (int) (dataSize / 512L);
        int  modulus    = (int) (dataSize % 512L);

        // Couldn't care less about the entry "name".

        PipedOutputStream outPipe = new PipedOutputStream();
        PipedInputStream inPipe =
                new PipedInputStream(outPipe, (int) dataSize);
        try {
            while (readBlocks > 0) {
                readNow = (readBlocks > archive.getReadBufferBlocks())
                          ? archive.getReadBufferBlocks()
                          : readBlocks;

                archive.readBlocks(readNow);

                readBlocks -= readNow;

                outPipe.write(archive.readBuffer, 0, readNow * 512);
            }

// TODO:  Remove this Dev debug assertion
            if (readBlocks != 0) {
                throw new IllegalStateException(
                    "Finished skipping but skip blocks to go = " + readBlocks
                    + "?");
            }

            if (modulus != 0) {
                archive.readBlock();
                outPipe.write(archive.readBuffer, 0, modulus);
            }

            outPipe.flush();  // Do any good on a pipe?

        } catch (IOException ioe) {
            inPipe.close();
            throw ioe;
        } finally {
            outPipe.close();
        }
        return new PIFData(inPipe);

    }

    protected void extractFile(TarEntryHeader header)
    throws IOException, TarMalformatException {
        
        if (header.getDataSize() < 1) {
            throw new TarMalformatException("Data size unknown");
        }

        int  readNow;
        int  readBlocks = (int) (header.getDataSize() / 512L);
        int  modulus    = (int) (header.getDataSize() % 512L);
        File newFile    = header.generateFile();

        if (!newFile.isAbsolute()) {
            newFile = (extractBaseDir == null) ? newFile.getAbsoluteFile()
                                               : new File(extractBaseDir,
                                               newFile.getPath());
        }

        // newFile is definitively Absolutized at this point
        File parentDir = newFile.getParentFile();

        if (newFile.exists()) {
            if (mode != TarReader.OVERWRITE_MODE) {
                throw new IOException("Extracted file already exists: "
                                      + newFile.getAbsolutePath());
            }

            if (!newFile.isFile()) {
                throw new IOException("Node already exist but is not a file: "
                                      + newFile.getAbsolutePath());
            }

            // Better to let FileOutputStream creation zero it than to
            // to newFile.delete().
        }

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

        int              fileMode  = header.getFileMode();
        FileOutputStream outStream = new FileOutputStream(newFile);

        try {
            newFile.setExecutable(false, false);
            newFile.setReadable(false, false);
            newFile.setWritable(false, false);
            newFile.setExecutable(((fileMode & 0100) != 0), true);
            newFile.setReadable((fileMode & 0400) != 0, true);
            newFile.setWritable((fileMode & 0200) != 0, true);

            // Don't know exactly why I am still able to write to the file
            // after removing read and write privs from myself, but it does
            // work.
            while (readBlocks > 0) {
                readNow = (readBlocks > archive.getReadBufferBlocks())
                          ? archive.getReadBufferBlocks()
                          : readBlocks;

                archive.readBlocks(readNow);

                readBlocks -= readNow;

                outStream.write(archive.readBuffer, 0, readNow * 512);
            }

// TODO:  Remove this Dev debug assertion
            if (readBlocks != 0) {
                throw new IllegalStateException(
                    "Finished skipping but skip blocks to go = " + readBlocks
                    + "?");
            }

            if (modulus != 0) {
                archive.readBlock();
                outStream.write(archive.readBuffer, 0, modulus);
            }

            outStream.flush();

        } finally {
            outStream.close();
        }

        newFile.setLastModified(header.getModTime() * 1000);

        if (newFile.length() != header.getDataSize()) {
            throw new IOException("Attempted to write " + header.getDataSize()
                                  + " bytes to '" + newFile.getAbsolutePath()
                                  + "', but only wrote " + newFile.length());
        }
    }

    protected void skipFileData(TarEntryHeader header)
    throws IOException, TarMalformatException {
        if (header.getDataSize() < 1) {
            throw new TarMalformatException("Data size unknown");
        }

        int skipNow;
        int oddBlocks  = (header.getDataSize() % 512L == 0L) ? 0
                                                             : 1;
        int skipBlocks = (int) (header.getDataSize() / 512L) + oddBlocks;

        while (skipBlocks > 0) {
            skipNow = (skipBlocks > archive.getReadBufferBlocks())
                      ? archive.getReadBufferBlocks()
                      : skipBlocks;

            archive.readBlocks(skipNow);

            skipBlocks -= skipNow;
        }

// TODO:  Remove this Dev debug assertion
        if (skipBlocks != 0) {
            throw new IllegalStateException(
                "Finished skipping but skip blocks to go = " + skipBlocks
                + "?");
        }

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

            path = readString(TarHeaderFields.NAME);

            if (path == null) {
                throw new TarMalformatException(
                    "Required field 'NAME' missing in tar entry header");
            }

            Long longObject = readInteger(TarHeaderFields.MODE);

            if (longObject == null) {
                throw new TarMalformatException(
                    "Required field 'MODE' missing in tar entry header");
            }

            fileMode   = (int) longObject.longValue();
            longObject = readInteger(TarHeaderFields.SIZE);

            if (longObject != null) {
                dataSize   = longObject.longValue();
            }

            longObject = readInteger(TarHeaderFields.MTIME);

            if (longObject == null) {
                throw new TarMalformatException(
                    "Required field 'MTIME' missing in tar entry header.");
            }

            modTime   = longObject.longValue();
            entryType = readChar(TarHeaderFields.TYPEFLAG);
            ownerName = readString(TarHeaderFields.UNAME);

            String pathPrefix = readString(TarHeaderFields.PREFIX);

            if (pathPrefix != null) {
                path = pathPrefix + '/' + path;
            }

            // We're not loading the "gname" field, since there is nothing at
            // all that Java can do with it.
            ustar = isUstar();
        }

        protected byte[] rawHeader;
        /* CRITICALLY IMPORTANT:  TO NOT USE rawHeader.length OR DEPEND ON
         * THE LENGTH OF the rawHeader ARRAY!  Use only the first 512 bytes!
         */
        protected String  path;
        protected int     fileMode;
        protected long    dataSize = -1;    // In bytes
        protected long    modTime;
        protected char    entryType;
        protected String  ownerName;
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

            // Unfortunately, it does no good to set modification times or
            // privileges here, since those settings have no effect on our
            // new file until after is created by the FileOutputStream
            // constructor.
            return new File(path);
        }

        public char getEntryType() {
            return entryType;
        }

        public String getPath() {
            return path;
        }

        /**
         * Setter is needed in order to override header size setting for Pax.
         */
        public void setDataSize(long dataSize) {
            this.dataSize = dataSize;
        }

        public long getDataSize() {
            return dataSize;
        }

        public long getModTime() {
            return modTime;
        }

        public int getFileMode() {
            return fileMode;
        }

        /**
         * Choosing not to report fields that we don't write (e.g. "gname"),
         * but which would certainly be useful for a general Java tar client
         * implementation.
         * This design decision is subject to change.
         *
         * TODO:  Format output into colums.  Looks terrible when the fields
         *        don't line up in columns.
         */
        public String toString() {

            StringBuffer sb = new StringBuffer();

            sb.append((entryType == '\0') ? ' '
                                          : entryType);
            sb.append(ustar ? '*'
                            : ' ');
            sb.append(' ' + sdf.format(modTime * 1000) + ' '
                      + Integer.toOctalString(fileMode) + "  " + dataSize
                      + "  ");
            sb.append((ownerName == null) ? '-'
                                          : ownerName);
            sb.append("  " + path);

            return sb.toString();
        }

        /**
         * Is this any UStar variant
         */
        public boolean isUstar() throws TarMalformatException {

            String magicString = readString(TarHeaderFields.MAGIC);

            return magicString != null && magicString.startsWith("ustar");
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

        protected char readChar(int fieldId) throws TarMalformatException {

            /* Depends on readString(int) contract that it will never return
             * a 0-length String */
            String s = readString(fieldId);

            return (s == null) ? '\0'
                               : s.charAt(0);
        }

        /**
         * @returns null or String with length() > 0.
         */
        protected String readString(int fieldId) throws TarMalformatException {

            int start = TarHeaderFields.getStart(fieldId);
            int stop  = TarHeaderFields.getStop(fieldId);
            int termIndex = TarEntryHeader.indexOf(rawHeader, (byte) 0, start,
                                                   stop);

            switch (termIndex) {

                case 0 :
                    return null;

                case -1 :
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
            String s = readString(fieldId);
            if (s == null) {
                return null;
            }

            try {
                return Long.valueOf(s, 8);
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
                boolean isInRange =
                    (i >= TarHeaderFields.getStart(TarHeaderFields.CHECKSUM)
                     && i < TarHeaderFields.getStop(TarHeaderFields.CHECKSUM));

                // We ignore current contents of the checksum field so that
                // this method will continue to work right, even if we later
                // recycle the header or RE-calculate a header.

                sum += isInRange ? 32
                                 : (255 & rawHeader[i]);
            }

            return sum;
        }
    }
}
