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


package org.hsqldb.scriptio;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.hsqldb.Database;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.NumberSequence;
import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FileAccess;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.rowio.RowOutputTextLog;

/**
 * Handles all scripting and logging operations. A script consists of two blocks:<p>
 *
 * DDL: SQL statements for table and user definitions
 * DATA: INSERT statements for memory tables
 *
 * This happens as part of the CHECKPOINT and SHUTDOWN COMPACT
 * process. In this case, the
 * DATA block contains the CACHED table data as well.<p>
 *
 * A related use for this class is for saving a current snapshot of the
 * database data to a user-defined file with the SCRIPT command
 *
 * A log consists of SQL statements of different types. Each statement is
 * encoded as ASCII and saved.
 *
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.1
 * @since 1.7.2
 */
public class ScriptWriterText extends ScriptWriterBase {

    static byte[] BYTES_COMMIT       = "COMMIT".getBytes(JavaSystem.CS_ISO_8859_1);
    static byte[] BYTES_INSERT_INTO  = "INSERT INTO ".getBytes(JavaSystem.CS_ISO_8859_1);
    static byte[] BYTES_VALUES       = " VALUES(".getBytes(JavaSystem.CS_ISO_8859_1);
    static byte[] BYTES_TERM         = ")".getBytes(JavaSystem.CS_ISO_8859_1);
    static byte[] BYTES_DELETE_FROM  = "DELETE FROM ".getBytes(JavaSystem.CS_ISO_8859_1);
    static byte[] BYTES_WHERE        = " WHERE ".getBytes(JavaSystem.CS_ISO_8859_1);
    static byte[] BYTES_SEQUENCE     = "ALTER SEQUENCE ".getBytes(JavaSystem.CS_ISO_8859_1);
    static byte[] BYTES_SEQUENCE_MID = " RESTART WITH ".getBytes(JavaSystem.CS_ISO_8859_1);
    static byte[] BYTES_C_ID_INIT    = "/*C".getBytes(JavaSystem.CS_ISO_8859_1);
    static byte[] BYTES_C_ID_TERM    = "*/".getBytes(JavaSystem.CS_ISO_8859_1);
    static byte[] BYTES_SCHEMA       = "SET SCHEMA ".getBytes(JavaSystem.CS_ISO_8859_1);

    /* @todo - perhaps move this global into a lib utility class */
    static byte[] BYTES_LINE_SEP = System.getProperty("line.separator",
        "\n").getBytes(JavaSystem.CS_ISO_8859_1);

    static {
        if (BYTES_LINE_SEP[0] != 0x0A && BYTES_LINE_SEP[0] != 0x0D) {
            BYTES_LINE_SEP = new byte[]{ 0x0A };
        }
    }

    RowOutputInterface rowOut;

    public ScriptWriterText(Database db, OutputStream outputStream,
                            FileAccess.FileSync descriptor,
                            boolean includeCachedData) {
        super(db, outputStream, descriptor, includeCachedData);
    }

    public ScriptWriterText(Database db, String file,
                            boolean includeCachedData, boolean newFile,
                            boolean isUserScript) {
        super(db, file, includeCachedData, newFile, isUserScript);
    }

    public ScriptWriterText(Database db, String file,
                            boolean includeCachedData, boolean compressed) {

        super(db, file, includeCachedData, true, false);

        if (compressed) {
            isCompressed = true;

            try {
                fileStreamOut = new GZIPOutputStream(fileStreamOut);
            } catch (IOException e) {
                throw Error.error(e, ErrorCode.FILE_IO_ERROR,
                                  ErrorCode.M_Message_Pair, new String[] {
                    e.toString(), outFile
                });
            }
        }
    }

    protected void initBuffers() {
        rowOut = new RowOutputTextLog();
    }

    protected void writeDataTerm() {}

    protected void writeSessionIdAndSchema(Session session) {

        if (session == null) {
            return;
        }

        if (session != currentSession) {
            rowOut.reset();
            rowOut.writeBytes(BYTES_C_ID_INIT);
            rowOut.writeLong(session.getId());
            rowOut.writeBytes(BYTES_C_ID_TERM);

            currentSession = session;

            writeRowOutToFile();
        }

        if (schemaToLog != session.loggedSchema) {
            rowOut.reset();
            writeSchemaStatement(schemaToLog);

            session.loggedSchema = schemaToLog;

            writeRowOutToFile();
        }
    }

    private void writeSchemaStatement(HsqlName schema) {

        rowOut.writeBytes(BYTES_SCHEMA);
        rowOut.writeString(schema.statementName);
        rowOut.writeBytes(BYTES_LINE_SEP);
    }

    public void writeLogStatement(Session session, String s) {

        if (session != null) {
            schemaToLog = session.currentSchema;

            writeSessionIdAndSchema(session);
        }

        rowOut.reset();
        rowOut.writeString(s);
        rowOut.writeBytes(BYTES_LINE_SEP);
        writeRowOutToFile();

        needsSync = true;
    }

    public void writeRow(Session session, Row row, Table table) {

        schemaToLog = table.getName().schema;

        writeSessionIdAndSchema(session);
        rowOut.reset();
        rowOut.setMode(RowOutputTextLog.MODE_INSERT);
        rowOut.writeBytes(BYTES_INSERT_INTO);
        rowOut.writeString(table.getName().statementName);
        rowOut.writeBytes(BYTES_VALUES);
        rowOut.writeData(row, table.getColumnTypes());
        rowOut.writeBytes(BYTES_TERM);
        rowOut.writeBytes(BYTES_LINE_SEP);
        writeRowOutToFile();
    }

    public void writeTableInit(Table t) {

        if (t.isEmpty(currentSession)) {
            return;
        }

        if (!includeTableInit && schemaToLog == currentSession.loggedSchema) {
            return;
        }

        rowOut.reset();
        writeSchemaStatement(t.getName().schema);
        writeRowOutToFile();

        currentSession.loggedSchema = schemaToLog;
    }

    public void writeOtherStatement(Session session, String s) {

        writeLogStatement(session, s);

        if (writeDelay == 0) {
            sync();
        }
    }

    public void writeInsertStatement(Session session, Row row, Table table) {

        schemaToLog = table.getName().schema;

        writeRow(session, row, table);
    }

    public void writeDeleteStatement(Session session, Table table,
                                     Object[] data) {

        schemaToLog = table.getName().schema;

        writeSessionIdAndSchema(session);
        rowOut.reset();
        rowOut.setMode(RowOutputTextLog.MODE_DELETE);
        rowOut.writeBytes(BYTES_DELETE_FROM);
        rowOut.writeString(table.getName().statementName);
        rowOut.writeBytes(BYTES_WHERE);
        rowOut.writeData(table.getColumnCount(), table.getColumnTypes(), data,
                         table.columnList, table.getPrimaryKey());
        rowOut.writeBytes(BYTES_LINE_SEP);
        writeRowOutToFile();
    }

    public void writeSequenceStatement(Session session, NumberSequence seq) {

        schemaToLog = seq.getName().schema;

        writeSessionIdAndSchema(session);
        rowOut.reset();
        rowOut.writeBytes(BYTES_SEQUENCE);
        rowOut.writeString(seq.getSchemaName().statementName);
        rowOut.writeByte('.');
        rowOut.writeString(seq.getName().statementName);
        rowOut.writeBytes(BYTES_SEQUENCE_MID);
        rowOut.writeLong(seq.peek());
        rowOut.writeBytes(BYTES_LINE_SEP);
        writeRowOutToFile();

        needsSync = true;
    }

    public void writeCommitStatement(Session session) {

        writeSessionIdAndSchema(session);
        rowOut.reset();
        rowOut.writeBytes(BYTES_COMMIT);
        rowOut.writeBytes(BYTES_LINE_SEP);
        writeRowOutToFile();

        needsSync = true;

        if (writeDelay == 0) {
            sync();
        }
    }

    protected void finishStream() {

        try {
            if (isCompressed) {
                ((GZIPOutputStream) fileStreamOut).finish();
            }
        } catch (IOException io) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, outFile);
        }
    }

    void writeRowOutToFile() {

        if (fileStreamOut == null) {
            return;
        }

        synchronized (fileStreamOut) {
            try {
                fileStreamOut.write(rowOut.getBuffer(), 0, rowOut.size());

                byteCount += rowOut.size();

                lineCount++;
            } catch (IOException io) {
                throw Error.error(ErrorCode.FILE_IO_ERROR, outFile);
            }
        }
    }
}
