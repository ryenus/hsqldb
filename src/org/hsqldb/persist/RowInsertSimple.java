/* Copyright (c) 2001-2021, The HSQL Development Group
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

import org.hsqldb.Database;
import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.scriptio.ScriptWriterText;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.0
 * @since 2.5.0
 */
public class RowInsertSimple implements RowInsertInterface {

    final Session     session;
    final ErrorLogger callback;
    final int         mode;

    public RowInsertSimple(Session session, ErrorLogger callback, int mode) {

        this.session  = session;
        this.callback = callback;
        this.mode     = mode;
    }

    public void finishTable() {}

    public void close() {
        callback.close();
    }

    public long getErrorLineNumber() {
        return 0L;
    }

    public void insert(Table table, PersistentStore store, Object[] rowData) {
        table.insertFromScript(session, store, rowData);
    }

    public void setStartLineNumber(long number) {}

    public static class InsertErrorHandler implements ErrorLogger {

        Database         database;
        String           fileNamePath;
        ScriptWriterText scrwriter;

        public InsertErrorHandler(Database database, String fileNamePath) {
            this.database     = database;
            this.fileNamePath = fileNamePath;
        }

        public void writeLogStatement(long lineNumber, String s) {

            setScrWriter();

            try {

                // todo - write line number etc
                scrwriter.writeLogStatement(null, s);
            } catch (Throwable t) {}
        }

        public void writeRow(long lineNumber, Row row) {

            setScrWriter();

            try {

                // todo - write line number etc
                scrwriter.writeInsertStatement(null, row,
                                               (Table) row.getTable());
            } catch (Throwable t) {}
        }

        public void close() {

            if (scrwriter != null) {
                scrwriter.close();
            }
        }

        private void setScrWriter() {

            if (scrwriter == null) {
                String timestamp = database.logger.fileDateFormat.format(
                    new java.util.Date());
                String name = fileNamePath + "." + timestamp + ".reject";

                scrwriter = new ScriptWriterText(database, name, false, false,
                                                 true);
            }
        }
    }

    public static class DefaultErrorHandler implements ErrorLogger {

        public DefaultErrorHandler() {}

        public void writeLogStatement(long lineNumber, String s) {}

        public void writeRow(long lineNumber, Row row) {}

        public void close() {}
    }
}
