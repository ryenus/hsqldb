/* Copyright (c) 2001-2009, The HSQL Development Group
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

import java.util.HashMap;
import java.util.Map;

import org.hsqldb.lib.ValidatingResourceBundle;

/* $Id$ */

/**
 * Resource Bundle for Tar classes
 * <P>
 * Purpose of this class is to wrap a RefCapablePropertyResourceBundle to
 *  reliably detect any possible use of a missing property key as soon as
 *  this class is clinitted.
 * The reason for this is to allow us developers to detect all such errors
 *  before end-users ever use this class.
 * </P> <P>
 * IMPORTANT:  To add a new ResourceBundle element, add two new lines, one
 * like <PRE>
 *    static public final int NEWKEYID = keyCounter++;
 * </PRE> and one line <PRE>
 *      new Integer(KEY2), "key2",
 * </PRE>
 * Both should be inserted right after all of the other lines of the same type.
 * NEWKEYID is obviously a new constant which you will use in calling code
 * like RB.NEWKEYID.
 * </P>
 */
public class RB extends ValidatingResourceBundle {
    private static int keyCounter = 0;
    public static final int DBBACKUP_SYNTAX = keyCounter++;
    public static final int DBBACKUP_SYNTAXERR = keyCounter++;
    public static final int TARGENERATOR_SYNTAX = keyCounter++;
    public static final int PAD_BLOCK_WRITE = keyCounter++;
    public static final int CLEANUP_RMFAIL = keyCounter++;
    public static final int TARREADER_SYNTAX = keyCounter++;
    public static final int UNSUPPORTED_ENTRY_PRESENT = keyCounter++;
    public static final int BPR_WRITE = keyCounter++;
    public static final int STREAM_BUFFER_REPORT = keyCounter++;
    public static final int WRITE_QUEUE_REPORT = keyCounter++;
    public static final int FILE_MISSING = keyCounter++;
    public static final int MODIFIED_PROPERTY = keyCounter++;
    public static final int FILE_DISAPPEARED = keyCounter++;
    public static final int FILE_CHANGED = keyCounter++;
    public static final int FILE_APPEARED = keyCounter++;
    public static final int PIF_MALFORMAT = keyCounter++;
    public static final int PIF_MALFORMAT_SIZE = keyCounter++;
    public static final int ZERO_WRITE = keyCounter++;
    public static final int PIF_TOOBIG = keyCounter++;
    public static final int READ_DENIED = keyCounter++;
    public static final int COMPRESSION_UNKNOWN = keyCounter++;
    public static final int INSUFFICIENT_READ = keyCounter++;
    public static final int DECOMPRESS_RANOUT = keyCounter++;
    public static final int MOVE_WORK_FILE = keyCounter++;
    public static final int CANT_OVERWRITE = keyCounter++;
    public static final int CANT_WRITE_DIR = keyCounter++;
    public static final int NO_PARENT_DIR = keyCounter++;
    public static final int BAD_BLOCK_WRITE_LEN = keyCounter++;
    public static final int ILLEGAL_BLOCK_BOUNDARY = keyCounter++;
    public static final int WORKFILE_DELETE_FAIL = keyCounter++;
    public static final int UNSUPPORTED_EXT = keyCounter++;
    public static final int DEST_EXISTS = keyCounter++;
    public static final int PARENT_NOT_DIR = keyCounter++;
    public static final int CANT_WRITE_PARENT = keyCounter++;
    public static final int PARENT_CREATE_FAIL = keyCounter++;
    public static final int TAR_FIELD_TOOBIG = keyCounter++;
    public static final int MISSING_SUPP_PATH = keyCounter++;
    public static final int NONFILE_ENTRY = keyCounter++;
    public static final int READ_LT_1 = keyCounter++;
    public static final int DATA_CHANGED = keyCounter++;
    public static final int UNEXPECTED_HEADER_KEY = keyCounter++;
    public static final int TARREADER_SYNTAXERR = keyCounter++;
    public static final int UNSUPPORTED_MODE = keyCounter++;
    public static final int DIR_X_CONFLICT = keyCounter++;
    public static final int PIF_UNKNOWN_DATASIZE = keyCounter++;
    public static final int PIF_DATA_TOOBIG = keyCounter++;
    public static final int DATA_SIZE_UNKNOWN = keyCounter++;
    public static final int EXTRACTION_EXISTS = keyCounter++;
    public static final int EXTRACTION_EXISTS_NOTFILE = keyCounter++;
    public static final int EXTRACTION_PARENT_NOT_DIR = keyCounter++;
    public static final int EXTRACTION_PARENT_NOT_WRITABLE = keyCounter++;
    public static final int EXTRACTION_PARENT_MKFAIL = keyCounter++;
    public static final int WRITE_COUNT_MISMATCH = keyCounter++;
    public static final int HEADER_FIELD_MISSING = keyCounter++;
    public static final int CHECKSUM_MISMATCH = keyCounter++;
    public static final int CREATE_ONLY_NORMAL = keyCounter++;
    public static final int BAD_HEADER_VALUE = keyCounter++;
    public static final int BAD_NUMERIC_HEADER_VALUE = keyCounter++;
    public static final int LISTING_FORMAT = keyCounter++;

    private static Object[] memberKeyArray = new Object[] {
        DBBACKUP_SYNTAX, "DbBackup.syntax",
        DBBACKUP_SYNTAXERR, "DbBackup.syntaxerr",
        TARGENERATOR_SYNTAX, "TarGenerator.syntax",
        PAD_BLOCK_WRITE, "pad.block.write",
        CLEANUP_RMFAIL, "cleanup.rmfail",
        TARREADER_SYNTAX, "TarReader.syntax",
        UNSUPPORTED_ENTRY_PRESENT, "unsupported.entry.present",
        BPR_WRITE, "bpr.write",
        STREAM_BUFFER_REPORT, "stream.buffer.report",
        WRITE_QUEUE_REPORT, "write.queue.report",
        FILE_MISSING, "file.missing",
        MODIFIED_PROPERTY, "modified.property",
        FILE_DISAPPEARED, "file.disappeared",
        FILE_CHANGED, "file.changed",
        FILE_APPEARED, "file.appeared",
        PIF_MALFORMAT, "pif.malformat",
        PIF_MALFORMAT_SIZE, "pif.malformat.size",
        ZERO_WRITE, "zero.write",
        PIF_TOOBIG, "pif.toobig",
        READ_DENIED, "read.denied",
        COMPRESSION_UNKNOWN, "compression.unknown",
        INSUFFICIENT_READ, "insufficient.read",
        DECOMPRESS_RANOUT, "decompression.ranout",
        MOVE_WORK_FILE, "move.work.file",
        CANT_OVERWRITE, "cant.overwrite",
        CANT_WRITE_DIR, "cant.write.dir",
        NO_PARENT_DIR, "no.parent.dir",
        BAD_BLOCK_WRITE_LEN, "bad.block.write.len",
        ILLEGAL_BLOCK_BOUNDARY, "illegal.block.boundary",
        WORKFILE_DELETE_FAIL, "workfile.delete.fail",
        UNSUPPORTED_EXT, "unsupported.ext",
        DEST_EXISTS, "dest.exists",
        PARENT_NOT_DIR, "parent.not.dir",
        CANT_WRITE_PARENT, "cant.write.parent",
        PARENT_CREATE_FAIL, "parent.create.fail",
        TAR_FIELD_TOOBIG, "tar.field.toobig",
        MISSING_SUPP_PATH, "missing.supp.path",
        NONFILE_ENTRY, "nonfile.entry",
        READ_LT_1, "read.lt.1",
        DATA_CHANGED, "data.changed",
        UNEXPECTED_HEADER_KEY, "unexpected.header.key",
        TARREADER_SYNTAXERR, "tarreader.syntaxerr",
        UNSUPPORTED_MODE, "unsupported.mode",
        DIR_X_CONFLICT, "dir.x.conflict",
        PIF_UNKNOWN_DATASIZE, "pif.unknown.datasize",
        PIF_DATA_TOOBIG, "pif.data.toobig",
        DATA_SIZE_UNKNOWN, "data.size.unknown",
        EXTRACTION_EXISTS, "extraction.exists",
        EXTRACTION_EXISTS_NOTFILE, "extraction.exists.notfile",
        EXTRACTION_PARENT_NOT_DIR, "extraction.parent.not.dir",
        EXTRACTION_PARENT_NOT_WRITABLE, "extraction.parent.not.writable",
        EXTRACTION_PARENT_MKFAIL, "extraction.parent.mkfail",
        WRITE_COUNT_MISMATCH, "write.count.mismatch",
        HEADER_FIELD_MISSING, "header.field.missing",
        CHECKSUM_MISMATCH, "checksum.mismatch",
        CREATE_ONLY_NORMAL, "create.only.normal",
        BAD_HEADER_VALUE, "bad.header.value",
        BAD_NUMERIC_HEADER_VALUE, "bad.numeric.header.value",
        LISTING_FORMAT, "listing.format",
    };

    private Map<Integer, String> keyIdToString = new HashMap<Integer, String>();

    protected Map<Integer, String> getKeyIdToString() {
        return keyIdToString;
    }

    public RB() {
        super("org.hsqldb.lib.tar.rb");
        if (memberKeyArray == null)
            throw new RuntimeException("'static memberKeyArray not set");
        for (int i = 0; i < memberKeyArray.length; i += 2) {
            keyIdToString.put(
                    (Integer) memberKeyArray[i], (String) memberKeyArray[i+1]);
        }
    }

    static {
        if (memberKeyArray == null)
            throw new RuntimeException("'static memberKeyArray not set");
        if (memberKeyArray.length % 2 != 0)
            throw new RuntimeException("memberKeyArray has an odd length");
        for (int i = 0; i < memberKeyArray.length; i += 2) {
            if (!(memberKeyArray[i] instanceof Integer))
                throw new RuntimeException("Element #" + i + " ("
                        + ((i - 1 < 0) ? "first item"
                            : ("after item \"" + memberKeyArray[i-1] + "\""))
                        + ") is a " + memberKeyArray[i].getClass().getName()
                        + ", not an Integer, in memberKeyArray in class "
                        + RB.class.getName());
            if (!(memberKeyArray[i+1] instanceof String))
                throw new RuntimeException("Element #" + (i+1) + " ("
                        + ((i - 2 < 0) ? "first item"
                            : ("after item \"" + memberKeyArray[i-1] + "\""))
                        + ") is a " + memberKeyArray[i+1].getClass().getName()
                        + ", not a String, in memberKeyArray in class "
                        + RB.class.getName());
            if (((Integer) memberKeyArray[i]).intValue() != i/2)
                throw new RuntimeException("Element #" +  i
                        + " is wrong constant for item " + memberKeyArray[i+1]
                        + " in memberKeyArray in class "
                        + RB.class.getName());
        }
    }

    /* IMPORTANT:  Leave the singleton instantiation at the end here!
     * Otherwise there will be a confusing tangle between clinitting and
     * singleton instantiation.  */
    static public RB singleton = new RB();
    static {
        singleton.validate();
    }
}
