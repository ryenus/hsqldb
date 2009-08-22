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


package org.hsqldb.scriptio;

import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import org.hsqldb.Database;
import org.hsqldb.Error;
import org.hsqldb.ErrorCode;
import org.hsqldb.persist.Crypto;

/**
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @since 1.9.0
 * @version 1.9.0
 */
public class ScriptWriterEncode extends ScriptWriterText {

    private static final int bufferSize = 1 << 15;

    public ScriptWriterEncode(Database db, String file, boolean includeCached,
                              Crypto crypto) {

        super(db, file, includeCached, true, false);

        try {
            fileStreamOut = crypto.getOutputStream(fileStreamOut);
            fileStreamOut = new GZIPOutputStream(fileStreamOut);
        } catch (IOException e) {
            throw Error.error(e, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_Message_Pair, new Object[] {
                e.getMessage(), outFile
            });
        }
    }

    /**
     * Override the underlying method with no operation.
     */
    public void sync() {}

    /**
     * This may not really be necessary, unless we add implementations where
     * non-compressed data is added to the end of the copressed part.
     */
    protected void finishStream() throws IOException {
        ((GZIPOutputStream) fileStreamOut).finish();
        fileStreamOut.flush();
    }
}
