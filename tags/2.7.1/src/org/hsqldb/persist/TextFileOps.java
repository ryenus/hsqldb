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


package org.hsqldb.persist;

import org.hsqldb.Database;
import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.result.Result;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowInputText;
import org.hsqldb.rowio.RowInputTextQuoted;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.rowio.RowOutputText;
import org.hsqldb.rowio.RowOutputTextQuoted;
import org.hsqldb.types.Type;

/**
 * Read and write delimited text files.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.0
 * @since 2.7.0
*/
public class TextFileOps {

    public static Result loadTextData(Session session, String fileSettings,
                                      Table table, int mode) {

        TextFileReader reader;
        Database       database = session.database;
        FileUtil       fa       = FileUtil.getFileUtil();
        TextFileSettings textFileSettings =
            new TextFileSettings(database.getProperties(), fileSettings);
        String                dataFileName = textFileSettings.getFileName();
        RandomAccessInterface dataFile;
        RowInputInterface     rowIn;
        RowInsertSimple       rowInsert;

        if (dataFileName == null || dataFileName.length() == 0) {
            throw Error.error(ErrorCode.X_S0501);
        }

        dataFileName = fa.canonicalOrAbsolutePath(dataFileName);

        boolean exists = FileUtil.getFileUtil().exists(dataFileName);

        if (!exists) {
            throw Error.error(null, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_TextCache_opening_file_error,
                              new String[] {
                dataFileName, "file does not exist"
            });
        }

        try {
            dataFile = RAFile.newScaledRAFile(database, dataFileName, true,
                                              RAFile.DATA_FILE_TEXT);
        } catch (Throwable t) {
            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_TextCache_opening_file_error,
                              new String[] {
                dataFileName, t.toString()
            });
        }

        if (textFileSettings.isQuoted || textFileSettings.isAllQuoted) {
            rowIn = new RowInputTextQuoted(textFileSettings);
        } else {
            rowIn = new RowInputText(textFileSettings);
        }

        reader = TextFileReader8.newTextFileReader(dataFile, textFileSettings,
                rowIn, true);

        RowInsertInterface.ErrorLogger callback;

        if (mode == RowInsertInterface.modes.continueOnError) {
            callback = new RowInsertSimple.InsertErrorHandler(database,
                    dataFileName);
        } else {
            callback = new RowInsertSimple.DefaultErrorHandler();
        }

        rowInsert = new RowInsertSimple(session, callback, mode);

        PersistentStore store       = table.getRowStore(session);
        Type[]          types       = table.getColumnTypes();
        boolean         ignoreFirst = textFileSettings.isIgnoreFirst;

        try {
            while (true) {
                rowIn = reader.readObject();

                if (rowIn == null) {
                    break;
                }

                if (ignoreFirst) {
                    ignoreFirst = false;

                    continue;
                }

                Object[] data = rowIn.readData(types);

                if (textFileSettings.isNullDef) {
                    table.generateDefaultForNull(data);
                }

                table.generateAndCheckData(session, data);

                if (mode == RowInsertInterface.modes.checkUntillError) {
                    continue;
                }

                rowInsert.insert(table, store, data);
            }

            rowInsert.finishTable();
        } catch (Throwable t) {
            long linenumber = reader.getLineNumber();

            throw Error.error(t, ErrorCode.TEXT_FILE, 0, new String[] {
                String.valueOf(linenumber), t.toString()
            });
        } finally {
            rowInsert.close();

            try {
                if (callback != null) {
                    callback.close();
                }
            } catch (Exception e) {}

            try {
                dataFile.close();
            } catch (Exception e) {}
        }

        return Result.newUpdateCountResult((int) reader.getLineNumber());
    }

    public static Result unloadTextData(Session session, String fileSettings,
                                        Table table) {

        Database database = session.database;
        FileUtil fa       = FileUtil.getFileUtil();
        TextFileSettings textFileSettings =
            new TextFileSettings(database.getProperties(), fileSettings);
        String                dataFileName = textFileSettings.getFileName();
        RandomAccessInterface dataFile;
        RowOutputInterface    rowOut;
        int                   lineCount = 0;

        if (dataFileName == null || dataFileName.length() == 0) {
            throw Error.error(ErrorCode.X_S0501);
        }

        dataFileName = fa.canonicalOrAbsolutePath(dataFileName);

        boolean exists = FileUtil.getFileUtil().exists(dataFileName);

        if (exists) {
            throw Error.error(null, ErrorCode.TEXT_SOURCE_EXISTS,
                              ErrorCode.M_TextCache_opening_file_error,
                              new String[] {
                dataFileName, "file exists"
            });
        }

        try {
            dataFile = new RAFileSimple(database.logger, dataFileName, "rw");
        } catch (Throwable t) {
            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_TextCache_opening_file_error,
                              new String[]{ dataFileName });
        }

        if (textFileSettings.isQuoted || textFileSettings.isAllQuoted) {
            rowOut = new RowOutputTextQuoted(textFileSettings);
        } else {
            rowOut = new RowOutputText(textFileSettings);
        }

        PersistentStore store        = table.getRowStore(session);
        Type[]          types        = table.getColumnTypes();
        RowIterator     it           = table.rowIteratorForScript(store);
        long            filePosition = 0;

        try {
            while (it.next()) {
                rowOut.reset();

                Row row = it.getCurrentRow();

                rowOut.writeData(row, types);
                rowOut.writeEnd();
                dataFile.seek(filePosition);
                dataFile.write(rowOut.getOutputStream().getBuffer(), 0,
                               rowOut.getOutputStream().size());

                filePosition += rowOut.getOutputStream().size();

                lineCount++;
            }

            dataFile.synch();
        } catch (Throwable t) {
            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_TextCache_opening_file_error,
                              new String[]{ dataFileName });
        } finally {
            try {
                dataFile.close();
            } catch (Exception e) {}
        }

        return Result.newUpdateCountResult((int) lineCount);
    }
}
