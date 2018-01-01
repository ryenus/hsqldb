/* Copyright (c) 2001-2018, The HSQL Development Group
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

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.hsqldb.Database;
import org.hsqldb.HsqlException;
import org.hsqldb.Session;
import org.hsqldb.Statement;
import org.hsqldb.StatementTypes;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.LineReader;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.map.ValuePool;
import org.hsqldb.result.Result;
import org.hsqldb.rowio.RowInputTextLog;
import org.hsqldb.types.Type;

/**
 * Handles operations involving reading back a script or log file written
 * out by ScriptWriterText. This implementation
 * corresponds to ScriptWriterText.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 *  @version 2.4.1
 *  @since 1.7.2
 */
public class ScriptReaderText extends ScriptReaderBase {

    // this is used only to enable reading one logged line at a time
    LineReader      dataStreamIn;
    InputStream     inputStream;
    InputStream     bufferedStream;
    GZIPInputStream gzipStream;
    RowInputTextLog rowIn;
    boolean         isInsert;

    ScriptReaderText(Database db, String fileName) {
        super(db, fileName);
    }

    public ScriptReaderText(Database db, String fileName,
                            boolean compressed) throws IOException {

        super(db, fileName);

        inputStream =
            database.logger.getFileAccess().openInputStreamElement(fileName);
        bufferedStream = new BufferedInputStream(inputStream);

        InputStream tempStream;

        if (compressed) {
            gzipStream = new GZIPInputStream(bufferedStream);
            tempStream = gzipStream;
        } else {
            tempStream = bufferedStream;
        }

        dataStreamIn = new LineReader(tempStream, ScriptWriterText.ISO_8859_1);
        rowIn = new RowInputTextLog(db.databaseProperties.isVersion18());
    }

    protected void readDDL(Session session) {

        for (;;) {
            Statement cs     = null;
            Result    result = null;

            try {
                boolean hasRow = readLoggedStatement(session);

                if (!hasRow) {
                    break;
                }

                if (rowIn.getStatementType() == INSERT_STATEMENT) {
                    isInsert = true;

                    break;
                }

                cs = session.compileStatement(statement);
                result = session.executeCompiledStatement(cs,
                        ValuePool.emptyObjectArray, 0);
            } catch (HsqlException e) {
                result = Result.newErrorResult(e);
            }

            if (result.isError()) {
                if (cs == null) {

                    // compile error
                } else if (cs.getType() == StatementTypes.GRANT) {

                    // handle grants on math and library routines in old versions
                    continue;
                } else if (cs.getType() == StatementTypes.CREATE_ROUTINE) {

                    // ignore legacy references
                    if (result.getMainString().indexOf("org.hsqldb.Library")
                            > -1) {
                        continue;
                    }
                }

                database.logger.logWarningEvent(result.getMainString(),
                                                result.getException());

                HsqlException e = getError(result.getException(), lineCount);

                handleError(e);
            }
        }
    }

    protected void readExistingData(Session session) {

        String tablename = null;

        for (;;) {
            try {
                boolean hasRow = false;

                if (isInsert) {
                    isInsert = false;
                    hasRow   = true;
                } else {
                    hasRow = readLoggedStatement(session);
                }

                if (!hasRow) {
                    break;
                }

                if (statementType == SET_SCHEMA_STATEMENT) {
                    session.setSchema(currentSchema);

                    tablename = null;

                    continue;
                } else if (statementType == INSERT_STATEMENT) {
                    if (!rowIn.getTableName().equals(tablename)) {
                        tablename = rowIn.getTableName();

                        String schema = session.getSchemaName(currentSchema);

                        currentTable =
                            database.schemaManager.getUserTable(tablename,
                                schema);
                        currentStore =
                            database.persistentStoreCollection.getStore(
                                currentTable);
                    }

                    currentTable.insertFromScript(session, currentStore,
                                                  rowData);
                } else {
                    HsqlException e = Error.error(ErrorCode.GENERAL_ERROR,
                                                  statement);

                    throw e;
                }
            } catch (Throwable t) {
                HsqlException e = getError(t, lineCount);

                handleError(e);
            }
        }
    }

    public boolean readLoggedStatement(Session session) {

        if (!sessionChanged) {
            try {
                rawStatement = dataStreamIn.readLine();
            } catch (EOFException e) {
                return false;
            } catch (IOException e) {
                throw Error.error(e, ErrorCode.FILE_IO_ERROR, null);
            }

            lineCount++;

            //        System.out.println(lineCount);
            statement = StringConverter.unicodeStringToString(rawStatement);

            if (statement == null) {
                return false;
            }
        }

        processStatement(session);

        return true;
    }

    void processStatement(Session session) {

        if (statement.startsWith("/*C")) {
            int endid = statement.indexOf('*', 4);

            sessionNumber  = Integer.parseInt(statement.substring(3, endid));
            statement      = statement.substring(endid + 2);
            sessionChanged = true;
            statementType  = SESSION_ID;

            return;
        }

        sessionChanged = false;

        rowIn.setSource(session, statement);

        statementType = rowIn.getStatementType();

        if (statementType == ANY_STATEMENT) {
            rowData      = null;
            currentTable = null;

            return;
        } else if (statementType == COMMIT_STATEMENT) {
            rowData      = null;
            currentTable = null;

            return;
        } else if (statementType == SET_SCHEMA_STATEMENT) {
            rowData       = null;
            currentTable  = null;
            currentSchema = rowIn.getSchemaName();

            return;
        }

        String name   = rowIn.getTableName();
        String schema = session.getCurrentSchemaHsqlName().name;

        currentTable = database.schemaManager.getUserTable(name, schema);
        currentStore =
            database.persistentStoreCollection.getStore(currentTable);

        Type[] colTypes;

        if (statementType == INSERT_STATEMENT) {
            colTypes = currentTable.getColumnTypes();
        } else if (currentTable.hasPrimaryKey()) {
            colTypes = currentTable.getPrimaryKeyTypes();
        } else {
            colTypes = currentTable.getColumnTypes();
        }

        rowData = rowIn.readData(colTypes);
    }

    public void close() {

        try {
            if (dataStreamIn != null) {
                dataStreamIn.close();
            }
        } catch (Exception e) {}

        try {
            if (gzipStream != null) {
                gzipStream.close();
            }
        } catch (Exception e) {}

        try {
            if (inputStream != null) {
                inputStream.close();
            }
        } catch (Exception e) {}

        try {
            if (scrwriter != null) {
                scrwriter.close();
            }

            database.recoveryMode = 0;
        } catch (Exception e) {}
    }

    HsqlException getError(Throwable t, long lineCount) {

        if (t instanceof HsqlException) {
            HsqlException e = ((HsqlException) t);

            if (e.getErrorCode() == -ErrorCode.ERROR_IN_SCRIPT_FILE) {
                return e;
            }
        }

        return Error.error(t, ErrorCode.ERROR_IN_SCRIPT_FILE,
                           ErrorCode.M_DatabaseScriptReader_read,
                           new Object[] {
            Long.valueOf(lineCount), t.toString()
        });
    }

    private void handleError(HsqlException e) {

        database.logger.logSevereEvent("bad line in script file " + lineCount,
                                       e);

        if (database.recoveryMode == 0) {
            throw e;
        }

        openScriptWriter();

        try {
            scrwriter.writeLogStatement(null, rawStatement);
        } catch (Throwable t) {}
    }

    private void openScriptWriter() {

        if (scrwriter == null) {
            String timestamp =
                database.logger.fileDateFormat.format(new java.util.Date());
            String name = fileNamePath + "." + timestamp + ".reject";

            scrwriter = new ScriptWriterText(database, name, false, false,
                                             true);
        }
    }
}
