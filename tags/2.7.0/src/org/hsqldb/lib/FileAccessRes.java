/* Copyright (c) 2001-2022, The HSQL Development Group
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


package org.hsqldb.lib;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

/**
* @author Fred Toussi (fredt@users dot sourceforge.net)
* @version 2.7.0
* @since 1.8.0
*/
public class FileAccessRes implements FileAccess {

    public boolean isStreamElement(String fileName) {

        URL url = null;

        try {
            url = getClass().getResource(fileName);

            if (url == null) {
                ClassLoader cl =
                    Thread.currentThread().getContextClassLoader();

                if (cl != null) {
                    url = cl.getResource(fileName);
                }
            }
        } catch (Throwable t) {

            //
        }

        return url != null;
    }

    public InputStream openInputStreamElement(final String fileName)
    throws IOException {

        InputStream fis = null;

        try {
            fis = getClass().getResourceAsStream(fileName);

            if (fis == null) {
                ClassLoader cl =
                    Thread.currentThread().getContextClassLoader();

                if (cl != null) {
                    fis = cl.getResourceAsStream(fileName);
                }
            }
        } catch (Throwable t) {

            //
        } finally {
            if (fis == null) {
                throw new FileNotFoundException(fileName);
            }
        }

        return fis;
    }

    public void createParentDirs(String filename) {}

    public boolean removeElement(String filename) {
        return false;
    }

    public boolean renameElement(String oldName, String newName) {
        return false;
    }

    public boolean renameElementOrCopy(String oldName, String newName,
                                       EventLogInterface logger) {
        return false;
    }

    public OutputStream openOutputStreamElement(String streamName)
    throws IOException {
        throw new IOException();
    }

    public OutputStream openOutputStreamElementAppend(String streamName)
    throws IOException {
        throw new IOException();
    }

    public FileAccess.FileSync getFileSync(OutputStream os)
    throws IOException {
        throw new IOException();
    }
}
