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


package org.hsqldb.scriptio;

import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import org.hsqldb.Database;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HsqlByteArrayOutputStream;
import org.hsqldb.persist.Crypto;

/**
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @since 1.9.0
 * @version 1.9.0
 */
public class ScriptWriterEncode extends ScriptWriterText {

    Crypto                    crypto;
    HsqlByteArrayOutputStream byteOut;

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

    public ScriptWriterEncode(Database db, String file, Crypto crypto) {

        super(db, file, false, false, false);

        this.crypto = crypto;
        byteOut     = new HsqlByteArrayOutputStream();
    }

    protected void finishStream() throws IOException {

        if (fileStreamOut instanceof GZIPOutputStream) {
            ((GZIPOutputStream) fileStreamOut).finish();
        }

        fileStreamOut.flush();
    }

    void writeRowOutToFile() throws IOException {

        synchronized (fileStreamOut) {
            if (byteOut == null) {
                super.writeRowOutToFile();

                return;
            }

            int count = crypto.getEncodedSize(rowOut.size());
            byteOut.ensureRoom(count + 4);

            count = crypto.encode(rowOut.getBuffer(), 0, rowOut.size(),
                                      byteOut.getBuffer(), 4);
            byteOut.setPosition(0);
            byteOut.writeInt(count);
            fileStreamOut.write(byteOut.getBuffer(), 0, count + 4);
        }
    }
}
