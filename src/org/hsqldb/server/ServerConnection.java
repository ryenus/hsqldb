/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2009, The HSQL Development Group
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


package org.hsqldb.server;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.EOFException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.net.Socket;

import org.hsqldb.ClientConnection;
import org.hsqldb.DatabaseManager;
import org.hsqldb.Error;
import org.hsqldb.ErrorCode;
import org.hsqldb.HsqlException;
import org.hsqldb.Session;
import org.hsqldb.lib.DataOutputStream;
import org.hsqldb.resources.BundleHandler;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.rowio.RowInputBinary;
import org.hsqldb.rowio.RowInputBinaryNet;
import org.hsqldb.rowio.RowOutputBinaryNet;
import org.hsqldb.rowio.RowOutputInterface;

// fredt@users 20020215 - patch 461556 by paul-h@users - server factory
// fredt@users 20020424 - patch 1.7.0 by fredt - shutdown without exit
// fredt@users 20021002 - patch 1.7.1 by fredt - changed notification method
// fredt@users 20030618 - patch 1.7.2 by fredt - changed read/write methods

/**
 *  All ServerConnection objects are listed in a Set in server
 *  and removed by this class when closed.<p>
 *
 *  When the database or server is shutdown, the signalClose() method is called
 *  for all current ServerConnection instances. This will call the private
 *  close() method unless the ServerConnection thread itself has caused the
 *  shutdown. In this case, the keepAlive flag is set to false, allowing the
 *  thread to terminate once it has returned the result of the operation to
 *  the client.
 *  (fredt@users)<p>
 *
 * Rewritten in  HSQLDB version 1.7.2, based on original Hypersonic code.
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since Hypersonic SQL
 */
class ServerConnection implements Runnable {

    boolean                  keepAlive;
    private String           user;
    int                      dbID;
    int                      dbIndex;
    private volatile Session session;
    private Socket           socket;
    private Server           server;
    private DataInputStream  dataInput;
    private DataOutputStream dataOutput;
    private static int       mCurrentThread = 0;
    private int              mThread;
    static final int         BUFFER_SIZE = 0x1000;
    final byte[]             mainBuffer  = new byte[BUFFER_SIZE];
    RowOutputInterface       rowOut;
    RowInputBinary           rowIn;
    Thread                   runnerThread;
    protected static String  TEXTBANNER_PART1 = null;
    protected static String  TEXTBANNER_PART2 = null;
    static {
        int serverBundleHandle =
            BundleHandler.getBundleHandle("org_hsqldb_Server_messages", null);
        if (serverBundleHandle < 0) {
            throw new RuntimeException(
                    "MISSING Resource Bundle.  See source code");
            // This will be caught before prod release.
            // Not necessary to localize message.
        }
        TEXTBANNER_PART1 = BundleHandler.getString(serverBundleHandle,
                "textbanner.part1");
        TEXTBANNER_PART2 = BundleHandler.getString(serverBundleHandle,
                "textbanner.part2");
        if (TEXTBANNER_PART1 == null || TEXTBANNER_PART2 == null) {
            throw new RuntimeException(
                    "MISSING Resource Bundle msg definition.  See source code");
            // This will be caught before prod release.
            // Not necessary to localize message.
        }
    }

    /**
     * Creates a new ServerConnection to the specified Server on the
     * specified socket.
     *
     * @param socket the network socket on which Server communication
     *      takes place
     * @param server the Server instance to which the object
     *      represents a connection
     */
    ServerConnection(Socket socket, Server server) {

        RowOutputBinaryNet rowOutTemp = new RowOutputBinaryNet(mainBuffer);

        rowIn  = new RowInputBinaryNet(rowOutTemp);
        rowOut = rowOutTemp;

        Thread runnerThread;

        this.socket = socket;
        this.server = server;

        synchronized (ServerConnection.class) {
            mThread = mCurrentThread++;
        }

        synchronized (server.serverConnSet) {
            server.serverConnSet.add(this);
        }
    }

    /**
     * Signals this object to close, including exiting the thread running
     * the request handling loop
     */
    void signalClose() {

        keepAlive = false;

        if (!Thread.currentThread().equals(runnerThread)) {
            close();
        }
    }

    /**
     * Closes this connection.
     */
    private void close() {

        if (session != null) {
            session.close();
        }

        session = null;

        // fredt@user - closing the socket is to stop this thread
        try {
            socket.close();
        } catch (IOException e) {}

        synchronized (server.serverConnSet) {
            server.serverConnSet.remove(this);
        }
    }

    /**
     * Initializes this connection.
     * <p>
     * Will return (not throw) if fail to initialize the connection.
     * </p>
     */
    private void init() {

        runnerThread = Thread.currentThread();
        keepAlive    = true;

        try {
            socket.setTcpNoDelay(true);

            dataInput  = new DataInputStream(socket.getInputStream());
            dataOutput = new DataOutputStream(socket.getOutputStream());
            int firstInt = handshake();
            switch (streamProtocol) {

                case HSQL_STREAM_PROTOCOL:
                    String verString = ClientConnection.toNcvString(firstInt);
                    if (!verString.equals(
                        ClientConnection.NETWORK_COMPATIBILITY_VERSION)) {
                        throw Error.error(
                            ErrorCode.SERVER_VERSIONS_INCOMPATIBLE, 0,
                            new String[] {verString,
                            ClientConnection.NETWORK_COMPATIBILITY_VERSION});
                    }

                    Result resultIn = Result.newResult(dataInput, rowIn);

                    resultIn.readAdditionalResults(session, dataInput, rowIn);

                    Result resultOut;

                    resultOut = setDatabase(resultIn);

                    resultOut.write(dataOutput, rowOut);
                    break;
                case ODBC_STREAM_PROTOCOL:
                    odbcConnect(firstInt);
                    break;
                default:
                    // Protocol detection failures should already have been
                    // handled.
                    keepAlive = false;
            }
        } catch (Exception e) {
            // Only "unexpected" failures are caught here.
            // Expected failures will have been handled (by sending feedback
            // to user-- with an output Result for normal protocols), then
            // continuing.
            StringBuffer sb =
                new StringBuffer(mThread + ":Failed to connect client.");
            if (user != null) {
                sb.append("  User '" + user + "'.");
            }
            server.printWithThread(sb.toString() + "  Stack trace follows.");
            server.printStackTrace(e);
            // TODO:  Check this after merging with trunk (in either direction).
            // Could be either a conflict or duplication here, since I have
            // applied the same mod both in the odbcproto1 branch in the trunk
            // branch.
        }
    }

    private class CleanExit extends Exception {}
    private class ClientFailure extends Exception {
        private String clientMessage = null;
        public ClientFailure(String ourMessage, String clientMessage) {
            super(ourMessage);
            this.clientMessage = clientMessage;
        }
        public String getClientMessage() {
            return clientMessage;
        }
    }
    private class RecoverableFailure extends Exception {
        private String clientMessage = null;
        public RecoverableFailure(String m) {
            this(m, m);
        }
        public RecoverableFailure(String ourMessage, String clientMessage) {
            super(ourMessage);
            this.clientMessage = clientMessage;
        }
        public String getClientMessage() {
            return clientMessage;
        }
    }

    private CleanExit cleanExit = new CleanExit();

    private void hsqlResultCycle()
    throws CleanExit, IOException, HsqlException {
        Result resultIn = Result.newResult(dataInput, rowIn);

        resultIn.readAdditionalResults(session, dataInput, rowIn);
        server.printRequest(mThread, resultIn);

        Result resultOut = null;
        int    type      = resultIn.getType();

        if (type == ResultConstants.CONNECT) {
            resultOut = setDatabase(resultIn);
        } else if (type == ResultConstants.DISCONNECT) {

            throw cleanExit;
        } else if (type == ResultConstants.RESETSESSION) {
            resetSession();

            return;
        } else {
            resultOut = session.execute(resultIn);
        }

        resultOut.write(dataOutput, rowOut);

        if (resultOut.getNavigator() != null) {
            resultOut.getNavigator().close();
        }

        rowOut.setBuffer(mainBuffer);
        rowIn.resetRow(mainBuffer.length);
    }

    private boolean inOdbcTrans = false;
    private void odbcXmitCycle() throws IOException, CleanExit {
        char op;
        boolean newTran = false;
        int len = 0;
        Result errorResult = null;
        try {
            op = (char) dataInput.readByte();
            server.printWithThread("Got op (" + op + ')');
            if (op == 'X') { // All other op types will send a length
                throw cleanExit;
            }
            len = dataInput.readInt() - 4;
            server.printWithThread("Got packet length of " + len);
        } catch (IOException e) {
            server.printWithThread("Fatal ODBC protocol failure: " + e);
            alertOdbcClient(ODBC_SEVERITY_FATAL, e.getMessage());
            // TODO:  Figure out whether should zend a Z..E packet here.
            return;
        }
        try { switch (op) {
            case 'Q':
                String sql = readNullTermdUTF(len - 1);
                 // We don't ask for the null terminator
                if (sql.startsWith("BEGIN;")) {
                    sql = sql.substring("BEGIN;".length());
                    server.printWithThread("ODBC Trans started");
                    inOdbcTrans = true;
                    dataOutput.writeByte('C'); // end of rows
                    dataOutput.writeInt(10); // size
                    writeNullTermdUTF("BEGIN");
                }
                String normalized = sql.trim().toLowerCase();
                if (server.isTrace()) {
                    server.printWithThread("Received query (" + sql + ')');
                }
                if (normalized.startsWith("select n.nspname,")) {
                    // Executed by psqlodbc after every user-specified query.
                    server.printWithThread("Swallowing 'select n.nspname,...'");

                    PacketOutputStream outPacket =
                        new PacketOutputStream(new ByteArrayOutputStream());
                    dataOutput.writeByte('T'); // sending a Tuple (row)
                    outPacket.writeShort(1);  // Num cols.
                    outPacket.writeUTF("oid", true);
                    outPacket.writeInt(201);
                    outPacket.writeShort(1);
                    outPacket.writeInt(23);
                    outPacket.writeShort(4);
                    outPacket.writeInt(-1);
                    outPacket.writeShort(0);
                    dataOutput.write(outPacket.toByteArray());
                    outPacket.reset();
                    outPacket.close();
                    // This query returns no rows.  typenam "lo"??
                    dataOutput.writeByte('C'); // end of rows
                    dataOutput.writeInt(11); // size
                    writeNullTermdUTF("SELECT");

                    dataOutput.writeByte('C'); // end of rows
                    dataOutput.writeInt(11); // size
                    writeNullTermdUTF("SELECT");
                    dataOutput.writeByte('Z');
                    if (server.isTrace()) {
                        server.printWithThread("### Writing size 11");
                    }
                    dataOutput.writeInt(5); //size
                    dataOutput.writeByte(inOdbcTrans ? 'T' : 'I');

                } else if (normalized.startsWith(
                    "select oid, typbasetype from")) {
                    // Executed by psqlodbc immediately after connecting.
                    server.printWithThread(
                        "Simulating 'select oid, typbasetype...'");
                    /*
                     * This query is run as "a hack to get the oid of our
                     * large object oid type.
                     */
                    dataOutput.writeByte('T'); // sending a Tuple (row)
                    dataOutput.writeInt(58); // size
                    if (server.isTrace()) {
                        server.printWithThread("### Writing size 58");
                    }
                    write((short) 2);          // Num cols.
                    writeNullTermdUTF("oid"); // Col. name
                    dataOutput.writeInt(101); // table ID
                    write((short) 102); // column id
                    dataOutput.writeInt(26); // Datatype ID  [adtid]
                    write((short) 4);       // Datatype size  [adtsize]
                    dataOutput.writeInt(-1); // Var size (always -1 so far)
                                             // [atttypmod]
                    write((short) 0);        // text format
                    writeNullTermdUTF("typbasetype"); // Col. name
                    dataOutput.writeInt(101); // table ID
                    write((short) 103); // column id
                    dataOutput.writeInt(26); // Datatype ID  [adtid]
                    write((short) 4);       // Datatype size  [adtsize]
                    dataOutput.writeInt(-1); // Var size (always -1 so far)
                                             // [atttypmod]
                    write((short) 0);        // text format

                    // This query returns no rows.  typenam "lo"??
                    dataOutput.writeByte('C'); // end of rows
                    dataOutput.writeInt(11); // size
                    writeNullTermdUTF("SELECT");
                    dataOutput.writeByte('Z');
                    if (server.isTrace()) {
                        server.printWithThread("### Writing size 5");
                    }
                    dataOutput.writeInt(5); //size
                    dataOutput.writeByte(inOdbcTrans ? 'T' : 'I');
                } else if (normalized.startsWith("select ")) {
                    server.printWithThread(
                        "Performing a real non-prepared SELECT...");
                    PacketOutputStream outPacket =
                        new PacketOutputStream(new ByteArrayOutputStream());
                    Result r = Result.newExecuteDirectRequest();
                    // sePrepare...() normally used on client side in
                    // JDBCStatement.
                    r.setPrepareOrExecuteProperties(normalized,0, 0,
                        org.hsqldb.StatementTypes.RETURN_RESULT,
                        org.hsqldb.jdbc.JDBCResultSet.TYPE_FORWARD_ONLY,
                        org.hsqldb.jdbc.JDBCResultSet.CONCUR_READ_ONLY,
                        org.hsqldb.jdbc.JDBCResultSet.HOLD_CURSORS_OVER_COMMIT,
                        java.sql.Statement.NO_GENERATED_KEYS, null, null);
                    Result rOut = session.execute(r);
                    switch (rOut.getType()) {
                        case ResultConstants.DATA:
                            break;
                        case ResultConstants.ERROR:
                            errorResult = rOut;
                        default:
                            throw new RecoverableFailure(
                                "Output Result from Query execution is of "
                                + "unexpected type: " + rOut.getType());
                    }
                    // See Result.newDataHeadResult() for what we have here
                    // .metaData, .navigator
                    org.hsqldb.navigator.RowSetNavigator navigator =
                            rOut.getNavigator();
                    if (!(navigator instanceof
                        org.hsqldb.navigator.RowSetNavigatorData)) {
                        throw new RecoverableFailure(
                                "Unexpected RowSetNavigator instance type: "
                                + navigator.getClass().getName());
                    }
                    org.hsqldb.navigator.RowSetNavigatorData navData =
                        (org.hsqldb.navigator.RowSetNavigatorData) navigator;
                    org.hsqldb.result.ResultMetaData md = rOut.metaData;
                    if (md == null) {
                        throw new RecoverableFailure(
                            "Failed to get metadata for query results");
                    }
                    if (md.getColumnCount() != md.getColumnCount()) {
                        throw new RecoverableFailure(
                            "Output column count mismatch: "
                            + md.getColumnCount() + " cols. and "
                            + md.getExtendedColumnCount()
                            + " extended cols. reported");
                    }
                    String[] colNames = md.getGeneratedColumnNames();
                    if (md.getColumnCount() != colNames.length) {
                        throw new RecoverableFailure(
                            "Couldn't get all column names: "
                            + md.getColumnCount() + " cols. but only got "
                            + colNames.length + " col. names");
                    }
                    org.hsqldb.types.Type[] colTypes = md.getParameterTypes();
                    boolean[] integers = new boolean[colTypes.length];
                    for (int i = 0; i < integers.length; i++) {
                        integers[i] =
                            colTypes[i] instanceof org.hsqldb.types.NumberType
                            && ((org.hsqldb.types.NumberType) colTypes[i])
                            .getPrecision()/8 == 4;
                    }
for (int j = 0; j < colTypes.length; j++) server.print("coltype " + j + ": " + colTypes[j].typeCode + " / " + colTypes[j].getNameString());
                    org.hsqldb.ColumnBase[] colDefs = md.columns;
for (int j = 0; j < colDefs.length; j++) server.print("col def name ("
+ colDefs[j].getNameString() + ") tbl name (" + colDefs[j].getTableNameString()
+ ')');
                    if (colNames.length != colDefs.length) {
                        throw new RecoverableFailure("Col data mismatch.  "
                                + colDefs.length + " col instances but "
                                + colNames.length + " col names");
                    }
                    dataOutput.writeByte('T'); // sending a Tuple (row)
                    outPacket.writeShort(colNames.length);  // Num cols.
                    for (int i = 0; i < colNames.length; i++) {
                        outPacket.writeUTF(colNames[i], true); // Col. name
                        // table ID  [relid]:
                        outPacket.writeInt((colDefs[i].getNameString() == null)
                            ? 0
                            : (colDefs[i].getSchemaNameString() + '.'
                                + colDefs[i].getTableNameString()).hashCode());
                        // column id  [attid]
                        outPacket.writeShort(
                            (colDefs[i].getTableNameString() == null)
                            ? 0 : (i + 1));
                            // TODO:  FIX This ID does not stick with the
                            // column, but just represents the position in this
                            // query.
                        // TODO:  Map from colTypes[i] to PG adtid:
                        outPacket.writeInt(integers[i] ? 23 : 1043);
                        // Datatype size  [adtsize]
                        outPacket.writeShort(integers[i] ? 4 : -1);
                        outPacket.writeInt(-1); // Var size [atttypmod]
                            // TODO:  Get from the colType[i]
                        // This is the size constraint integer
                        // like VARCHAR(12) or DECIMAL(4).
                        // -1 if none specified for this column.
                        outPacket.writeShort(0);  // text format 0 for all
                                                  // "displayable" as text.
                                                  // 1 for binaries like Objs.
                    }
                    dataOutput.write(outPacket.toByteArray());
                    outPacket.reset();
                    int rowNum = 0;
                    while (navData.next()) {
                        rowNum++;
                        Object[] rowData = (Object[]) navData.getCurrent();
                        // Row.getData().  Don't know why *Data.getCurrent()
                        //                 method returns Object instead of O[].
                        //  TODO:  Remove the assertion here:
                        if (rowData == null)
                            throw new RecoverableFailure("Null row?");
                        if (rowData.length < colNames.length) {
                            throw new RecoverableFailure(
                                "Data element mismatch. "
                                + colNames.length + " metadata cols, yet "
                                + rowData.length + " data elements for row "
                                + rowNum);
                        }
                        server.printWithThread("Row " + rowNum + " has "
                                + rowData.length + " elements");
                        dataOutput.writeByte('D'); // text row Data
                        outPacket.writeShort(colNames.length);
                         // This field could is just swallowed by PG ODBC
                         // client, but validated by psql.
                        for (int i = 0; i < colNames.length; i++) {
                            if (rowData[i] == null) {
                                outPacket.writeInt(-1);
                            } else {
                                outPacket.writeSized(rowData[i].toString());
                            }
                            server.printWithThread("R" + rowNum + "C" + (i+1)
                                    + " => (" + rowData[i].getClass().getName()
                                    + ") [" + rowData[i] + ']');
                        }
                        dataOutput.write(outPacket.toByteArray());
                        outPacket.reset();
                    }
                    outPacket.close();
                    dataOutput.writeByte('C'); // end of rows
                    dataOutput.writeInt(11); // size
                    writeNullTermdUTF("SELECT");
                    dataOutput.writeByte('Z');
                    if (server.isTrace()) {
                        server.printWithThread("### Writing size 5");
                    }
                    dataOutput.writeInt(5); //size
                    dataOutput.writeByte(inOdbcTrans ? 'T' : 'I');
                } else if (normalized.startsWith("set client_encoding to ")) {
                    server.printWithThread(
                        "Stubbing a 'set client_encoding to...'");
                    dataOutput.writeByte('C');
                    dataOutput.writeInt("SET".length() + 5); // size
                    writeNullTermdUTF("SET");
                    dataOutput.writeByte('Z');
                    if (server.isTrace()) {
                        server.printWithThread("### Writing size 5");
                    }
                    dataOutput.writeInt(5); //size
                    dataOutput.writeByte(inOdbcTrans ? 'T' : 'I');
                } else {
                    // TODO:  ROLLBACK is badly broken.
                    // I think that when a ROLLBACK of an update is done here,
                    // it actually inserts new rows!
                    server.printWithThread("Performing a real EXECDIRECT...");
                    Result r = Result.newExecuteDirectRequest();
                    // sePrepare...() normally used on client side in
                    // JDBCStatement.
                    r.setPrepareOrExecuteProperties(normalized,0, 0,
                        org.hsqldb.StatementTypes.RETURN_COUNT,
                        org.hsqldb.jdbc.JDBCResultSet.TYPE_FORWARD_ONLY,
                        org.hsqldb.jdbc.JDBCResultSet.CONCUR_READ_ONLY,
                        org.hsqldb.jdbc.JDBCResultSet.HOLD_CURSORS_OVER_COMMIT,
                        java.sql.Statement.NO_GENERATED_KEYS, null, null);
                    Result rOut = session.execute(r);
                    switch (rOut.getType()) {
                        case ResultConstants.UPDATECOUNT:
                            break;
                        case ResultConstants.ERROR:
                            errorResult = rOut;
                        default:
                            throw new RecoverableFailure(
                                "Output Result from execution is of "
                                + "unexpected type: " + rOut.getType());
                    }
                    String replyString = execDirectReplyString(normalized,
                            rOut.getUpdateCount());
                    dataOutput.writeByte('C');
                    dataOutput.writeInt(replyString.length() + 5); // size
                    writeNullTermdUTF(replyString);

                    dataOutput.writeByte('Z');
                    if (server.isTrace()) {
                        server.printWithThread("### Writing size 5");
                    }
                    dataOutput.writeInt(5); //size
                    dataOutput.writeByte(inOdbcTrans ? 'T' : 'I');

                    // A guess about how keeping inOdbcTrans in sync with
                    // client.  N.b. HSQLDB will need to more liberal with
                    // resetting, since DDL causes commits.
                    if (normalized.equals("commit")
                        || normalized.startsWith("commit ")
                        || normalized.equals("savepoint")
                        || normalized.startsWith("savepoint ")) {
                        inOdbcTrans = false;
                    }
                }
                break;
            default:
                throw new RecoverableFailure("Unsupported op type (" + op + ')',
                    "Unsupported operation type (" + op + ')');
        } } catch (RecoverableFailure rf) {
            if (errorResult == null) {
                server.printWithThread(rf.getMessage());
                alertOdbcClient(ODBC_SEVERITY_ERROR, rf.getClientMessage());
                // We continue on, hoping for client to send us a valid op.
            } else {
                alertOdbcClient(ODBC_SEVERITY_ERROR,
                    errorResult.getMainString(), errorResult.getSubString());
            }
            // We continue on, hoping for client to send us a valid op.
            dataOutput.writeByte('Z');
            if (server.isTrace()) {
                server.printWithThread("### Writing size 5");
            }
            dataOutput.writeInt(5); //size
            dataOutput.writeByte('E');
        }
    }

    private String execDirectReplyString(String command, int retval) {
        int firstWhiteSpace;
        for (firstWhiteSpace = 0; firstWhiteSpace < command.length();
            firstWhiteSpace++) {
            if (Character.isWhitespace(command.charAt(firstWhiteSpace))) {
                break;
            }
        }
        StringBuffer replyString = new StringBuffer(
            command.substring(0, firstWhiteSpace).toUpperCase());
        if (replyString.equals("update") || replyString.equals("delete")
            || replyString.equals("drop")) {
            replyString.append(" " + retval);
        } else if (replyString.equals("create") || replyString.equals("drop")) {
            // TODO: Add error-checking
            int wordStart;
            for (wordStart = firstWhiteSpace; wordStart < command.length();
                wordStart++) {
                if (!Character.isWhitespace(command.charAt(wordStart))) {
                    break;
                }
            }
            int wordEnd;
            for (wordEnd = wordStart; wordEnd < command.length();
                wordEnd++) {
                if (!Character.isWhitespace(command.charAt(wordEnd))) {
                    break;
                }
            }
            replyString.append(" " + command.substring(wordStart, wordEnd));
        } else if (replyString.equals("insert")) {
            replyString.append(" " + 98765 + ' ' + retval);
            // TODO:  Find out what the first numerical param is.
            // Probably a transaction identifier of some sort.
        }
        return replyString.toString();
    }

    /**
     * Initializes this connection and runs the request handling
     * loop until closed.
     */
    public void run() {

        init();

        if (session != null) {
            try {
                while (keepAlive) {
                    switch (streamProtocol) {
                        case HSQL_STREAM_PROTOCOL:
                            hsqlResultCycle();
                            break;
                        case ODBC_STREAM_PROTOCOL:
                            odbcXmitCycle();
                            break;
                        default:
                            throw new RuntimeException("Internal problem.  "
                                    + "Handshake should have unset keepAlive.");
                    }
                }
            } catch (CleanExit ce) {

                keepAlive = false;
            } catch (IOException e) {

                // fredt - is thrown when connection drops
                server.printWithThread(mThread + ":disconnected " + user);
            } catch (HsqlException e) {

                // fredt - is thrown while constructing the result or server shutdown
                if (keepAlive) {
                    server.printStackTrace(e);
                }
            }
        }

        close();
    }

    private Result setDatabase(Result resultIn) {

        try {
            String databaseName = resultIn.getDatabaseName();

            dbIndex = server.getDBIndex(databaseName);
            dbID    = server.dbID[dbIndex];
            user    = resultIn.getMainString();

            if (!server.isSilent()) {
                server.printWithThread(mThread + ":Trying to connect user '"
                                   + user + "' to DB (" + databaseName + ')');
            }

            session = DatabaseManager.newSession(dbID, user,
                                                 resultIn.getSubString(),
                                                 resultIn.getUpdateCount());

            if (!server.isSilent()) {
                server.printWithThread(mThread + ":Connected user '"
                                       + user + "'");
            }

            return Result.newConnectionAcknowledgeResponse(session.getId(),
                    session.getDatabase().getDatabaseID());
        } catch (HsqlException e) {
            session = null;

            return Result.newErrorResult(e, null);
        } catch (RuntimeException e) {
            session = null;

            return Result.newErrorResult(e, null);
        }
    }

    /**
     * Used by pooled connections to close the existing SQL session and open
     * a new one.
     */
    private void resetSession() {
        session.close();
    }

    /**
     * Retrieves the thread name to be used  when
     * this object is the Runnable object of a Thread.
     *
     * @return the thread name to be used  when this object is the Runnable
     * object of a Thread.
     */
    String getConnectionThreadName() {
        return "HSQLDB Connection @" + Integer.toString(hashCode(), 16);
    }

    /** Don't want this too high, or users may give up before seeing the
     *  banner.  Can't be too low or we could close a valid but slow
     *  client connection. */
    public static long MAX_WAIT_FOR_CLIENT_DATA = 1000;  // ms.
    public static long CLIENT_DATA_POLLING_PERIOD = 100;  // ms.

    /**
     * The only known case where a connection attempt will get stuck is
     * if client connects with hsqls to a https server; or
     * hsql to a http server.
     * All other client X server combinations are handled gracefully.
     * <P/>
     * If returns (a.o.t. throws), then state variable streamProtocol will
     * be set.
     *
     * @return int read as first thing off of stream
     */
    public int handshake() throws IOException, HsqlException {
        long clientDataDeadline = new java.util.Date().getTime()
                + MAX_WAIT_FOR_CLIENT_DATA;
        if (!(socket instanceof javax.net.ssl.SSLSocket)) {
            // available() does not work for SSL socket input stream
            do try {
                Thread.sleep(CLIENT_DATA_POLLING_PERIOD);
            } catch (InterruptedException ie) {
            } while (dataInput.available() < 5
                    && new java.util.Date().getTime() < clientDataDeadline);
                // Old HSQLDB clients will send resultType byte + 4 length bytes
                // New HSQLDB clients will send NCV int + above = 9 bytes
                // ODBC clients will send a much larger StartupPacket
            if (dataInput.available() < 1) {
                dataOutput.write((TEXTBANNER_PART1
                        + ClientConnection.NETWORK_COMPATIBILITY_VERSION
                        + TEXTBANNER_PART2 + '\n').getBytes());
                dataOutput.flush();
                throw Error.error(ErrorCode.SERVER_UNKNOWN_CLIENT);
            }
        }

        int firstInt = dataInput.readInt();
        switch (firstInt >> 24) {
            case 80: // Empirically
                throw Error.error(ErrorCode.SERVER_HTTP_NOT_HSQL_PROTOCOL);
            case 0:
                streamProtocol = ODBC_STREAM_PROTOCOL;
                break;
                /*
                    case 34:
                         // Determined empirically.
                         // Code looks like it should be
                         // ResultConstants.CONNECT
                        // TODO:  Send client a 1.8-compatible SQLException
                        throw Error.error(
                             ErrorCode.SERVER_VERSIONS_INCOMPATIBLE, 0,
                                 new String[] { "pre-9.0",
                             ClientConnection.NETWORK_COMPATIBILITY_VERSION});
                            throw Error.error(
                                    ErrorCode.SERVER_INCOMPLETE_HANDSHAKE_READ);
                    */
            default:
                streamProtocol = HSQL_STREAM_PROTOCOL;
                // HSQL protocol client
        }
        return firstInt;
    }

    /**
     * Reads a size indicator (which includes the size indicator bytes)
     * + that amount of bytes.
     */
    private byte[] readPacket() throws IOException {
        return readPacket(-1);
    }
    /**
     * Reads the specified amount of bytes;
     * if no specified size < 0, then
     * reads a size indicator (which includes the size indicator bytes)
     * + that amount of bytes.
     */
    private byte[] readPacket(int size) throws IOException {
        if (size < 0) {
            size = dataInput.readInt() - 4;
        }
        if (server.isTrace()) {
            server.printWithThread("Reading " + size + " more bytes (after 4)");
        }
        byte[] ba = new byte[size];
        dataInput.read(ba);
        return ba;
    }

    private void odbcConnect(int firstInt) throws IOException, HsqlException {
        int major = dataInput.readUnsignedShort();
        int minor = dataInput.readUnsignedShort();
        server.printWithThread("ODBC client connected.  "
                + "ODBC Protocol Compatibility Version " + major + '.' + minor);
        byte[] packet = readPacket(firstInt - 8);
          // - 4 for size of firstInt - 2 for major - 2 for minor
        java.util.Map stringPairs = readStringPairs(packet);
        //server.print("String Pairs: " + stringPairs);
        try {
            if (!stringPairs.containsKey("database")) {
                throw new ClientFailure("Client did not identify database",
                        "Target database not identified");
            }
            if (!stringPairs.containsKey("user")) {
                throw new ClientFailure("Client did not identify user",
                        "Target account not identified");
            }
            String databaseName = (String) stringPairs.get("database");
            user = (String) stringPairs.get("user");

            if (databaseName.equals("/")) {
                // Work-around because ODBC doesn't allow "" for Database name
                databaseName = "";
            }
            server.printWithThread("DB: " + databaseName);
            server.printWithThread("User: " + user);

            /* Unencoded/unsalted authentication */
            dataOutput.writeByte('R');
            dataOutput.writeInt(8); //size
            dataOutput.writeInt(ODBC_AUTH_REQ_PASSWORD); // areq of auth. mode.
            char c = '\0';
            try {
                c = (char) dataInput.readByte();
            } catch (EOFException eofe) {
                server.printWithThread(
                    "Looks like we got a goofy psql no-auth attempt.  "
                    + "Will probably retry properly very shortly");
                return;
            }
            if (c != 'p') {
                throw new ClientFailure("Expected password prefix 'p', "
                    + "but got '" + c + "'",
                    "Password value not prefixed with 'p'");
            }
            int len = dataInput.readInt() - 5;
                // Is password len after -4 for count int -1 for null term
            if (len < 0) {
                throw new ClientFailure(
                    "User submitted password length " + len,
                    "Empty passwords not allowed");
            }
            String password = readNullTermdUTF(len);

            dbIndex = server.getDBIndex(databaseName);
            dbID    = server.dbID[dbIndex];

            if (!server.isSilent()) {
                server.printWithThread(mThread + ":Trying to connect user '"
                                   + user + "' to DB (" + databaseName + ')');
            }

            try {
                session = DatabaseManager.newSession(dbID, user, password, 0);
                // TODO:  Find out what updateCount, the last para, is for:
                //                                   resultIn.getUpdateCount());
            } catch (Exception e) {
                throw new ClientFailure("User name or password denied: " + e,
                    "Login attempt rejected");
            }
        } catch (ClientFailure cf) {
            server.print(cf.getMessage());
            alertOdbcClient(ODBC_SEVERITY_FATAL, cf.getClientMessage());
            return;
        }

        dataOutput.writeByte('R'); // Notify client of success
        dataOutput.writeInt(8); //size
        dataOutput.writeInt(ODBC_AUTH_REQ_OK); //success

        if (!server.isSilent()) {
            server.printWithThread(mThread + ":Connected user '" + user + "'");
        }

        for (int i = 0; i < hardcodedOdbcParams.length; i++) {
            writeOdbcParam(hardcodedOdbcParams[i][0],
                hardcodedOdbcParams[i][1]);
        }

        dataOutput.writeByte('Z');
        if (server.isTrace()) {
            server.printWithThread("### Writing size 5");
        }
        dataOutput.writeInt(5); //size
        dataOutput.writeByte('I'); // I think this says to inherit transaction,
                                   // if there is one.  Could be wrong.

        dataOutput.writeByte('N');
        writeUTFPacket(new String[] {
            "MHello",
            "MYou have connected to HyperSQL ODBC Server"
        });
    }

    private java.util.Map readStringPairs(byte[] inbuf) throws IOException {
        java.util.List lengths = new java.util.ArrayList();
        ByteArrayOutputStream dataBaos = new ByteArrayOutputStream();
        int curlen = 0;

        if (inbuf[inbuf.length - 1] != 0) {
            throw new IOException(
                    "String-pair packet not terminated with null");
        }
        for (int i = 0; i < inbuf.length - 1; i++) {
            if (curlen == 0) {
                dataBaos.write((byte) 'X');
                dataBaos.write((byte) 'X');
            }
            if (inbuf[i] == 0) {
                lengths.add(new Integer(curlen));
                curlen = 0;
                continue;
            }
            curlen++;
            dataBaos.write(inbuf[i]);
        }
        if (curlen != 0) {
            throw new IOException(
                    "String-pair packet did not finish with a complete value");
        }
        byte[] data = dataBaos.toByteArray();
        dataBaos.close();
        int len;
        int offset = 0;
        while (lengths.size() > 0) {
            len = ((Integer) lengths.remove(0)).intValue();
            data[offset++] = (byte) (len >>> 8);
            data[offset++] = (byte) len;
            offset += len;
        }
        String k = null;
        java.io.DataInputStream dis =
            new java.io.DataInputStream(new ByteArrayInputStream(data));

        java.util.Map stringPairs = new java.util.HashMap();
        while (dis.available() > 0) {
            //String s = java.io.DataInputStream.readUTF(dis);
            // TODO:  Test the previous two to see if one works better for
            // high-order characters.
            if (k == null) {
                k = dis.readUTF();
            } else {
                stringPairs.put(k, dis.readUTF());
                k = null;
            }
        }
        dis.close();
        if (k != null) {
            throw new IOException(
                    "Value missing for key '" + k + "'");
        }
        return stringPairs;
    }

    private String readNullTermdUTF() throws IOException {
        /* Would be MUCH easier to do this with Java6's String
         * encoding/decoding operations */
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write((byte) 'X');
        baos.write((byte) 'X');
        // Place-holders to be replaced with short length

        int i;
        while ((i = dataInput.readByte()) > 0) {
            baos.write((byte) i);
        }
        byte[] ba = baos.toByteArray();
        baos.close();

        int len = ba.length - 2;
        ba[0] = (byte) (len >>> 8);
        ba[1] = (byte) len;

        java.io.DataInputStream dis =
            new java.io.DataInputStream(new ByteArrayInputStream(ba));
        String s = dis.readUTF();
        //String s = java.io.DataInputStream.readUTF(dis);
        // TODO:  Test the previous two to see if one works better for
        // high-order characters.
        dis.close();
        return s;
    }

    /**
     * @param reqLength Required length
     */
    private String readNullTermdUTF(int reqLength) throws IOException {
        /* Would be MUCH easier to do this with Java6's String
         * encoding/decoding operations */
        int bytesRead = 0;
        byte[] ba = new byte[reqLength + 3];
        ba[0] = (byte) (reqLength >>> 8);
        ba[1] = (byte) reqLength;
        while (bytesRead < reqLength + 1) {
            bytesRead += dataInput.read(ba, 2 + bytesRead, reqLength + 1- bytesRead);
        }
        if (ba[ba.length - 1] != 0) {
            throw new IOException("String not null-terminated");
        }
        for (int i = 2; i < ba.length - 1; i++) {
            if (ba[i] == 0) {
                throw new RuntimeException(
                        "Null internal to String at offset " + (i - 2));
            }
        }

        java.io.DataInputStream dis =
            new java.io.DataInputStream(new ByteArrayInputStream(ba));
        String s = dis.readUTF();
        //String s = java.io.DataInputStream.readUTF(dis);
        // TODO:  Test the previous two to see if one works better for
        // high-order characters.
        dis.close();
        return s;
    }

    /**
     * Writes unsigned short
     */
    private void write(short s) throws IOException {
        dataOutput.writeByte(s >>> 8);
        dataOutput.writeByte(s);
    }

    /**
     * Convenience werapper for writeUTF() method to write null-terminated
     * Strings.
     */
    private void writeNullTermdUTF(String s) throws IOException {
        writeUTF(s, true);
    }

    /**
     * With null-term true, writes null-terminated String without any size;
     * With null-term false, behaves
     * just like java.io.DataOutput.writeUTF() method, withe the 
     * exceptions listed below.
     * <P>
     * nullTerm false behavior differences from java.io.DataOutput
     * <OL>
     *   <LI>We write the size with a 4-byte int intead of a 2-byte short.
     *   <LI>Our size includes the 4 byte size prefix
     *       (counter-intuitive, but that's what our client requires).
     * </OL>
     *
     * @param nullTerm boolean switches between null-termination and
     *                 size-prefixing behavior, as described above.
     * @see java.io.DataOutput#writeUTF(String)
     */
    private void writeUTF(String s, boolean nullTerm)
    throws IOException {
        /* Would be MUCH easier to do this with Java6's String
         * encoding/decoding operations */
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        java.io.DataOutputStream dos = new java.io.DataOutputStream(baos);
        dos.writeUTF(s);
        byte[] ba = baos.toByteArray();
        dos.close();

        /* TODO:  Remove this block.
         * This is just to verify that the count written by DOS.writeUTF()
         * matches the number of bytes that it writes to the stream. */
        int len = ((ba[0] & 0xff) << 8) + (ba[1] & 0xff);
        if (len != ba.length - 2)
            throw new IOException("DOS.writeUTF count mismatch.  "
                    + (ba.length - 2)
                    + " written to stream, yet short val reports " + len);
        /**********************************************************/
        if (!nullTerm) {
            dataOutput.writeInt(ba.length + 2);
        }
        dataOutput.write(ba, 2, ba.length - 2);
        if (nullTerm) {
            dataOutput.writeByte(0);
        }
    }

    private void writeOdbcParam(String key, String val) throws IOException {
        dataOutput.writeByte('S');
        writeUTFPacket(new String[] { key, val }, false);
    }

    /**
     * Wrapper to null-terminate the packet.
     */
    private void writeUTFPacket(String[] strings) throws IOException {
        writeUTFPacket(strings, true);
    }

    private class PacketOutputStream extends java.io.DataOutputStream {
        private ByteArrayOutputStream byteArrayOutputStream;
        private ByteArrayOutputStream stringWriterOS =
            new ByteArrayOutputStream();
        private java.io.DataOutputStream stringWriterDos =
            new java.io.DataOutputStream(stringWriterOS);

        synchronized void writeUTF(String s, boolean nullTerm)
        throws IOException {
            stringWriterDos.writeUTF(s);
            write(stringWriterOS.toByteArray(), 2, stringWriterOS.size() - 2);
            stringWriterOS.reset();
            if (nullTerm) {
                writeByte(0);
            }
        }

        synchronized void writeSized(String s) throws IOException {
            stringWriterDos.writeUTF(s);
            byte[] ba = stringWriterOS.toByteArray();
            stringWriterOS.reset();

            writeInt(ba.length - 2);
            write(ba, 2, ba.length - 2);
        }

        public void reset() throws IOException {
            byteArrayOutputStream.reset();
            write('X'); // length placeholder
            write('X'); // length placeholder
            write('X'); // length placeholder
            write('X'); // length placeholder
        }

        public PacketOutputStream(ByteArrayOutputStream byteArrayOutputStream)
        throws IOException {
            super(byteArrayOutputStream);
            this.byteArrayOutputStream = byteArrayOutputStream;
            reset();
        }

        public byte[] toByteArray() {
            byte[] ba = byteArrayOutputStream.toByteArray();
            if (server.isTrace()) {
                server.printWithThread("Returning byte array with Write size "
                    + ba.length);
            }
            ba[0] = (byte) (ba.length >>> 24);
            ba[1] = (byte) (ba.length >>> 16);
            ba[2] = (byte) (ba.length >>> 8);
            ba[3] = (byte) ba.length;
            return ba;
        }

        public void close() throws IOException {
            super.close();
            stringWriterDos.close();
        }
    }

    /**
     * Writes size + null-terminated Strings + additional optional
     * terminating '\0'.
     *
     * @param nullTerminate true to terminate packet with an additional '\0'.
     */
    private void writeUTFPacket(String[] strings, boolean nullTerminate)
    throws IOException {
        /* Would be MUCH easier to do this with Java6's String
         * encoding/decoding operations */
        ByteArrayOutputStream accumulator = new ByteArrayOutputStream();
        // Allocate space for size prefix when done
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        java.io.DataOutputStream dos = new java.io.DataOutputStream(baos);
        for (int i = 0; i < strings.length; i++) {
            dos.writeUTF(strings[i]);
            accumulator.write(baos.toByteArray(), 2, baos.size() - 2);
            baos.reset();
            accumulator.write(0);
        }
        dos.close();
        if (nullTerminate) {
            accumulator.write(0);
        }
        dataOutput.writeInt(accumulator.size() + 4);
        if (server.isTrace()) {
            server.printWithThread("### Writing size " + (accumulator.size()
                + 4));
        }
        dataOutput.write(accumulator.toByteArray());
        accumulator.close();
    }

    // Constants taken from connection.h
    static private final int ODBC_SM_DATABASE = 64;
    static private final int ODBC_SM_USER = 32;
    static private final int ODBC_SM_OPTIONS = 64;
    static private final int ODBC_SM_UNUSED = 64;
    static private final int ODBC_SM_TTY = 64;
    static private final int ODBC_AUTH_REQ_PASSWORD = 3;
    static private final int ODBC_AUTH_REQ_OK = 0;

    // Tentative state variable
    static private final int UNDEFINED_STREAM_PROTOCOL = 0;
    static private final int HSQL_STREAM_PROTOCOL = 1;
    static private final int ODBC_STREAM_PROTOCOL = 2;
    private int streamProtocol = UNDEFINED_STREAM_PROTOCOL;

    private void alertOdbcClient(int severity, String message)
    throws IOException {
        alertOdbcClient(severity, message, "00000");
    }

    private void alertOdbcClient(int severity, String message, String sqlCode)
    throws IOException {
        if (!odbcSeverityMap.containsKey(severity)) {
            throw new IllegalArgumentException(
                "Unknown severity value (" + severity + ')');
        }
        dataOutput.writeByte('E');
        writeUTFPacket(new String[] {
            "S" + odbcSeverityMap.get(severity),
            "C" + sqlCode,
            "M" + message,
        });
    }

    static String[][] hardcodedOdbcParams = new String[][] {
        new String[] { "client_encoding", "SQL_ASCII" },
        new String[] { "DateStyle", "ISO, MDY" },
        new String[] { "integer_datetimes", "on" },
        new String[] { "is_superuser", "on" },
        new String[] { "server_encoding", "SQL_ASCII" },
        new String[] { "server_version", "8.3.1" },
        new String[] { "session_authorization", "blaine" },
        new String[] { "standard_conforming_strings", "off" },
        new String[] { "TimeZone", "US/Eastern" },
    };

    private static final int ODBC_SEVERITY_FATAL = 1;
    private static final int ODBC_SEVERITY_ERROR = 2;
    private static org.hsqldb.lib.IntKeyHashMap odbcSeverityMap =
        new org.hsqldb.lib.IntKeyHashMap();
    static {
        odbcSeverityMap.put(ODBC_SEVERITY_FATAL, "FATAL");
        odbcSeverityMap.put(ODBC_SEVERITY_ERROR, "ERROR");
    }
}
