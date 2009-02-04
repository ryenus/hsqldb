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
import java.net.SocketException;

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
    private OdbcPacketOutputStream outPacket = null;

    private void odbcXmitCycle() throws IOException, CleanExit {
        char op, c;
        boolean newTran = false;
        String stringVal, psHandle, portalHandle, replyString, handle;
        Result r, rOut;
        int paramCount;
        OdbcPreparedStatement odbcPs;
        StatementPortal portal;
        org.hsqldb.result.ResultMetaData pmd;
        OdbcPacketInputStream inPacket = null;

        try {
            inPacket =
                OdbcPacketInputStream.newOdbcPacketInputStream(dataInput);
            if (inPacket == null) {
                // This is an 'X' packet!
                // All other packet types will send a length, and we will get
                // a non-null "inPacket".
                if (sessionOdbcPsMap.size() >
                    (sessionOdbcPsMap.containsKey("") ? 1 : 0)) {
                    server.printWithThread("Client left " + 
                        sessionOdbcPsMap.size() + " PS objects open");
                }
                if (sessionOdbcPortalMap.size() >
                    (sessionOdbcPortalMap.containsKey("") ? 1 : 0)) {
                    server.printWithThread("Client left " + 
                        sessionOdbcPortalMap.size() + " Portal objects open");
                }
                throw cleanExit;
            }
            server.printWithThread("Got op (" + inPacket.packetType + ')');
            server.printWithThread("Got packet length of "
                    + inPacket.available() + " + type byte + 4 size header");
            if (inPacket.available() >= 1000000000) {
                throw new IOException("Insane packet length: "
                        + inPacket.available()
                        + " + type byte + 4 size header");
            }
        } catch (SocketException se) {
            server.printWithThread("Ungraceful client exit: " + se);
            throw cleanExit; // not "clean", but handled
        } catch (IOException ioe) {
            server.printWithThread("Fatal ODBC protocol failure: " + ioe);
            try {
                OdbcUtil.alertClient(OdbcUtil.ODBC_SEVERITY_FATAL,
                    ioe.getMessage(), "08P01", dataOutput);
                    // Code here means Protocol Violation
            } catch (Exception e) {
                // We just make an honest effort to notify the client
            }
            throw cleanExit; // not "clean", but handled
        }

        /******************************************************************
         * ODBC Service State Machine  (the remainder of this method)
         ******************************************************************/
        switch (odbcCommMode) {
            case OdbcUtil.ODBC_EXT_RECOVER_MODE:
                if (inPacket.packetType != 'S') {
                    if (server.isTrace()) {
                        server.printWithThread("Ignoring a '"
                            + inPacket.packetType + "'");
                    }
                    return;
                }
                odbcCommMode = OdbcUtil.ODBC_EXTENDED_MODE;
                server.printWithThread("EXTENDED comm session being recovered");
                // Now the main switch will handle the Sync packet carefully
                // the same as if there were no recovery.
                break;
            case OdbcUtil.ODBC_SIMPLE_MODE:
                switch (inPacket.packetType) {
                    case 'P':
                        // This is the only way to make this switch, according
                        // to docs, but that does not allow for intermixing of
                        // static and prepared statement (selects or other).
                        // Therefore we allow all of the following, which works
                        // great.
                    case 'H':
                    case 'S':
                    case 'D':
                    case 'B':
                    case 'E':
                    case 'C':
                        odbcCommMode = OdbcUtil.ODBC_EXTENDED_MODE;
                        server.printWithThread(
                                "Switching mode from SIMPLE to EXTENDED");
                    // Do not detect unexpected ops here.
                    // In that case, leave the mode as it is, and the main
                    // switch below will handle appropriately.
                }
                break;
            case OdbcUtil.ODBC_EXTENDED_MODE:
                switch (inPacket.packetType) {
                    case 'Q':
                        odbcCommMode = OdbcUtil.ODBC_SIMPLE_MODE;
                        server.printWithThread(
                                "Switching mode from EXTENDED to SIMPLE");
                    // Do not detect unexpected ops here.
                    // In that case, leave the mode as it is, and the main
                    // switch below will handle appropriately.
                }
                break;
            default:
                throw new RuntimeException("Unexpected ODBC comm mode value: "
                        + odbcCommMode);
        }

        outPacket.reset();
        try {
            // MAIN Comm Switch.
            // The resolution for each operation is one of:
            //   throw:  Handling will depend on the Throwable type
            //   break:  A Z (ReadyForQuery) packet will be sent to client
            //   run OdbcUtil.validateInputPacketSize() then return:  Nothing.  
            switch (inPacket.packetType) {
              case 'Q':
                String sql = inPacket.readString();
                // We don't ask for the null terminator

                /* **********************************************
                 * These first few cases handle the driver's implicit handling
                 * of transactions. */
                if (sql.startsWith("BEGIN;") || sql.equals("BEGIN")) {
                    sql = sql.equals("BEGIN")
                        ? null : sql.substring("BEGIN;".length());
                    server.printWithThread("ODBC Trans started");
                    inOdbcTrans = true;
                    outPacket.write("BEGIN");
                    outPacket.xmit('C', dataOutput);
                    if (sql == null) {
                       outPacket.writeByte(inOdbcTrans ? 'T' : 'I');
                       outPacket.xmit('Z', dataOutput);
                       OdbcUtil.validateInputPacketSize(inPacket);
                       return;
                    }
                }
                if (sql.startsWith("SAVEPOINT ") && sql.indexOf(';') > 0) {
                    throw new RecoverableOdbcFailure(
                        "SAVEPOINT prefix not supported yet", "0A000");
                    // TODO:  Implement this in similar fashion to BEGIN; prefix
                }
                if (sql.indexOf(";RELEASE ") > 0) {
                    // TODO:  Ensure no semicolons after "RELEASE".
                    throw new RecoverableOdbcFailure(
                        "';RELEASE <id>' suffix not supported yet", "0A000");
                    // TODO:  Issue a RELASE _after_ processing main command,
                    // but perhaps do not issue a separate reply for it.
                    // TODO:  Test if Postgresql server sends replies for
                    // RELEASE commands, both separately and in a compound.
                }
                /***********************************************/

                String normalized = sql.trim().toLowerCase();
                if (server.isTrace()) {
                    server.printWithThread("Received query (" + sql + ')');
                }

                // BEWARE:  We aren't supporting multiple result-sets from a
                // compound statement.  Plus for ODBC only, we have the added
                // constraint that the SELECT must be the first component
                // statement so that we can detect the statement type.
                // If we do parse out the component statement here, the states
                // set above apply to all executions, and only one Z packet
                // should be sent at the very end.

                if (normalized.startsWith("select current_schema()")) {
                    server.printWithThread(
                            "Implement 'select current_schema() emulation!");
                    throw new RecoverableOdbcFailure(
                        "current_schema() not supported yet", "0A000");
                }
                if (normalized.startsWith("select n.nspname,")) {
                    // Executed by psqlodbc after every user-specified query.
                    server.printWithThread("Swallowing 'select n.nspname,...'");

                    outPacket.writeShort(1);  // Num cols.
                    outPacket.write("oid");
                    outPacket.writeInt(201);
                    outPacket.writeShort(1);
                    outPacket.writeInt(23);
                    outPacket.writeShort(4);
                    outPacket.writeInt(-1);
                    outPacket.writeShort(0);
                    outPacket.xmit('T', dataOutput); // Xmit Row Definition

                    // This query returns no rows.  typenam "lo"??
                    outPacket.write("SELECT");
                    outPacket.xmit('C', dataOutput);
                    break;
                }
                if (normalized.startsWith(
                    "select oid, typbasetype from")) {
                    // Executed by psqlodbc immediately after connecting.
                    server.printWithThread(
                        "Simulating 'select oid, typbasetype...'");
                    /*
                     * This query is run as "a hack to get the oid of our
                     * large object oid type.
                     */
                    outPacket.writeShort(2);          // Num cols.
                    outPacket.write("oid"); // Col. name
                    outPacket.writeInt(101); // table ID
                    outPacket.writeShort(102); // column id
                    outPacket.writeInt(26); // Datatype ID  [adtid]
                    outPacket.writeShort(4);       // Datatype size  [adtsize]
                    outPacket.writeInt(-1); // Var size (always -1 so far)
                                             // [atttypmod]
                    outPacket.writeShort(0);        // text "format code"
                    outPacket.write("typbasetype"); // Col. name
                    outPacket.writeInt(101); // table ID
                    outPacket.writeShort(103); // column id
                    outPacket.writeInt(26); // Datatype ID  [adtid]
                    outPacket.writeShort(4);       // Datatype size  [adtsize]
                    outPacket.writeInt(-1); // Var size (always -1 so far)
                                             // [atttypmod]
                    outPacket.writeShort(0);        // text "format code"
                    outPacket.xmit('T', dataOutput); // sending a Tuple (row)

                    // This query returns no rows.  typenam "lo"??
                    outPacket.write("SELECT");
                    outPacket.xmit('C', dataOutput);
                    break;
                }
                if (normalized.startsWith("select ")) {
                    server.printWithThread(
                        "Performing a real non-prepared SELECT...");
                    r = Result.newExecuteDirectRequest();
                    r.setPrepareOrExecuteProperties(normalized, 0, 0,
                        org.hsqldb.StatementTypes.RETURN_RESULT,
                        org.hsqldb.jdbc.JDBCResultSet.TYPE_FORWARD_ONLY,
                        org.hsqldb.jdbc.JDBCResultSet.CONCUR_READ_ONLY,
                        org.hsqldb.jdbc.JDBCResultSet.HOLD_CURSORS_OVER_COMMIT,
                        java.sql.Statement.NO_GENERATED_KEYS, null, null);
                    rOut = session.execute(r);
                    switch (rOut.getType()) {
                        case ResultConstants.DATA:
                            break;
                        case ResultConstants.ERROR:
                            throw new RecoverableOdbcFailure(rOut);
                        default:
                            throw new RecoverableOdbcFailure(
                                "Output Result from Query execution is of "
                                + "unexpected type: " + rOut.getType());
                    }
                    // See Result.newDataHeadResult() for what we have here
                    // .metaData, .navigator
                    org.hsqldb.navigator.RowSetNavigator navigator =
                            rOut.getNavigator();
                    if (!(navigator instanceof
                        org.hsqldb.navigator.RowSetNavigatorData)) {
                        throw new RecoverableOdbcFailure(
                                "Unexpected RowSetNavigator instance type: "
                                + navigator.getClass().getName());
                    }
                    org.hsqldb.navigator.RowSetNavigatorData navData =
                        (org.hsqldb.navigator.RowSetNavigatorData) navigator;
                    org.hsqldb.result.ResultMetaData md = rOut.metaData;
                    if (md == null) {
                        throw new RecoverableOdbcFailure(
                            "Failed to get metadata for query results");
                    }
                    if (md.getColumnCount() != md.getExtendedColumnCount()) {
                        throw new RecoverableOdbcFailure(
                            "Output column count mismatch: "
                            + md.getColumnCount() + " cols. and "
                            + md.getExtendedColumnCount()
                            + " extended cols. reported");
                    }
                    String[] colNames = md.getGeneratedColumnNames();
                    if (md.getColumnCount() != colNames.length) {
                        throw new RecoverableOdbcFailure(
                            "Couldn't get all column names: "
                            + md.getColumnCount() + " cols. but only got "
                            + colNames.length + " col. names");
                    }
                    org.hsqldb.types.Type[] colTypes = md.getParameterTypes();
                    PgType[] pgTypes = new PgType[colTypes.length];
                    for (int i = 0; i < pgTypes.length; i++) {
                        pgTypes[i] = new PgType(colTypes[i]);
                    }
                    org.hsqldb.ColumnBase[] colDefs = md.columns;
                    if (colNames.length != colDefs.length) {
                        throw new RecoverableOdbcFailure("Col data mismatch.  "
                                + colDefs.length + " col instances but "
                                + colNames.length + " col names");
                    }
                    outPacket.writeShort(colNames.length);  // Num cols.
                    for (int i = 0; i < colNames.length; i++) {
                        outPacket.write(colNames[i]); // Col. name
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
                        outPacket.writeInt(pgTypes[i].getOid());
                        // Datatype size  [adtsize]
                        outPacket.writeShort(pgTypes[i].getTypeSize());
                        outPacket.writeInt(-1); // Var size [atttypmod]
                            // TODO:  Get from the colType[i]
                        // This is the size constraint integer
                        // like VARCHAR(12) or DECIMAL(4).
                        // -1 if none specified for this column.
                        outPacket.writeShort(0);
                        // format code must be 0 if D packets will follow;
                        // and 1 if B packets will follow.
                    }
                    outPacket.xmit('T', dataOutput); // Xmit Row Definition
                    int rowNum = 0;
                    while (navData.next()) {
                        rowNum++;
                        Object[] rowData = (Object[]) navData.getCurrent();
                        // Row.getData().  Don't know why *Data.getCurrent()
                        //                 method returns Object instead of O[].
                        //  TODO:  Remove the assertion here:
                        if (rowData == null)
                            throw new RecoverableOdbcFailure("Null row?");
                        if (rowData.length < colNames.length) {
                            throw new RecoverableOdbcFailure(
                                "Data element mismatch. "
                                + colNames.length + " metadata cols, yet "
                                + rowData.length + " data elements for row "
                                + rowNum);
                        }
                        //server.printWithThread("Row " + rowNum + " has "
                                //+ rowData.length + " elements");
                        outPacket.writeShort(colNames.length);
                         // This field is just swallowed by PG ODBC
                         // client, but OdbcUtil.validated by psql.
                        for (int i = 0; i < colNames.length; i++) {
                            if (rowData[i] == null) {
                                /*
                                server.printWithThread("R" + rowNum + "C"
                                    + (i+1) + " => [null]");
                                */
                                outPacket.writeInt(-1);
                            } else {
                                outPacket.writeSized(rowData[i].toString());
                                // N.b., Postgresql returns "t" and "f",
                                // not "" and "" like we are returning.
                                // But that makes no difference, since
                                // psqlodbc totally mishandles them either
                                // way.
                                /*
                                server.printWithThread("R" + rowNum + "C"
                                    + (i+1) + " => ("
                                    + rowData[i].getClass().getName()
                                    + ") [" + rowData[i] + ']');
                                */
                            }
                        }
                        outPacket.xmit('D', dataOutput); // text row Data
                          /* Should be 'B' to return binary results like Objs
                           * or BLOBs.  Otherwise should be 'B'.
                           * This must corresopnd to the T packet's format
                           * code. */
                    }
                    outPacket.write("SELECT");
                    outPacket.xmit('C', dataOutput);
                    break;
                }
                if (normalized.startsWith("deallocate ")) {
                    handle =
                        sql.trim().substring("deallocate ".length()).trim();
                        // Must use "sql" directly since name is case-sensitive
                    odbcPs = (OdbcPreparedStatement)
                        sessionOdbcPsMap.get(handle);
                    if (odbcPs != null) {
                        odbcPs.close();
                    }
                    portal = (StatementPortal) sessionOdbcPortalMap.get(handle);
                    if (portal != null) {
                        portal.close();
                    }
                    if (odbcPs == null && portal == null) {
                        /*
                        throw new RecoverableOdbcFailure(null,
                            "No object present for handle: " + handle, "08P01");
                        Driver does not handle state change correctly, so
                        for now we just issue a warning:
                        OdbcUtil.alertClient(OdbcUtil.ODBC_SEVERITY_ERROR,
                            "No object present for handle: " + handle,
                            dataOutput);
                        TODO:  Retest this.  May have been side-effect of
                               other problems.
                        */
                        server.printWithThread("Ignoring bad 'DEALLOCATE' cmd");
                    }
                    if (server.isTrace()) {
                        server.printWithThread("Deallocated PS/Portal '"
                                + handle + "'");
                    }
                    outPacket.write("DEALLOCATE");
                    outPacket.xmit('C', dataOutput);
                    break;
                }
                if (normalized.startsWith("set client_encoding to ")) {
                    server.printWithThread("Stubbing EXECDIR for: " +  sql);
                    outPacket.write("SET");
                    outPacket.xmit('C', dataOutput);
                    break;
                }
                // Case below is non-String-matched Qs:
                // TODO:  ROLLBACK is badly broken.
                // I think that when a ROLLBACK of an update is done here,
                // it actually inserts new rows!
                server.printWithThread("Performing a real EXECDIRECT...");
                r = Result.newExecuteDirectRequest();
                r.setPrepareOrExecuteProperties(normalized,0, 0,
                    org.hsqldb.StatementTypes.RETURN_COUNT,
                    org.hsqldb.jdbc.JDBCResultSet.TYPE_FORWARD_ONLY,
                    org.hsqldb.jdbc.JDBCResultSet.CONCUR_READ_ONLY,
                    org.hsqldb.jdbc.JDBCResultSet.HOLD_CURSORS_OVER_COMMIT,
                    java.sql.Statement.NO_GENERATED_KEYS, null, null);
                rOut = session.execute(r);
                switch (rOut.getType()) {
                    case ResultConstants.UPDATECOUNT:
                        break;
                    case ResultConstants.ERROR:
                        throw new RecoverableOdbcFailure(rOut);
                    default:
                        throw new RecoverableOdbcFailure(
                            "Output Result from execution is of "
                            + "unexpected type: " + rOut.getType());
                }
                replyString = OdbcUtil.echoBackReplyString(
                    normalized, rOut.getUpdateCount());
                outPacket.write(replyString);
                outPacket.xmit('C', dataOutput);

                // A guess about how keeping inOdbcTrans in sync with
                // client.  N.b. HSQLDB will need to more liberal with
                // resetting, since DDL causes commits.
                // TODO:  Consider implications of autocommit mode.
                if (normalized.equals("commit")
                    || normalized.startsWith("commit ")
                    || normalized.equals("savepoint")
                    || normalized.startsWith("savepoint ")) {
                    inOdbcTrans = false;
                }
                break;
              case 'H':
                // No-op.  It is impossible to cache while supporting multiple
                // ps and portal objects, so there is nothing for a Flush to
                // do.  There isn't even a reply to a Flush packet.
                OdbcUtil.validateInputPacketSize(inPacket);
                return;
              case 'S':
                // Special case for Sync packets.
                // To facilitate recovery, we do not abort in case of problems.
                if (!inOdbcTrans) try {
                    server.printWithThread("Implicit commit by Sync");
                    session.commit(true);
                    // TODO:  Find out if chain param should be T or F.
                } catch (HsqlException he) {
                    server.printWithThread("Implicit commit failed: " + he);
                    OdbcUtil.alertClient(OdbcUtil.ODBC_SEVERITY_ERROR,
                            "Implicit commit failed", he.getSQLState(),
                            dataOutput);
                }
                break;
              case 'P':
                psHandle = inPacket.readString();
                String query = OdbcUtil.revertMungledPreparedQuery(
                    inPacket.readString());
                paramCount = inPacket.readUnsignedShort();
                for (int i = 0; i < paramCount; i++) {
                    if (inPacket.readInt()  != 0) {
                        throw new RecoverableOdbcFailure(null,
                            "Parameter-type OID specifiers not supported yet",
                            "0A000");
                    }
                }
                if (server.isTrace()) {
                    server.printWithThread(
                        "Received Prepare request for query (" + query +
                        ") with handle '" + psHandle + "'");
                }
                if (psHandle.length() > 0
                        && sessionOdbcPsMap.containsKey(psHandle)) {
                    throw new RecoverableOdbcFailure(null,
                        "PS handle '" + psHandle + "' already in use.  "
                        + "You must close it before recreating", "08P01");
                }
                new OdbcPreparedStatement(
                        psHandle, query, sessionOdbcPsMap, session);

                outPacket.xmit('1', dataOutput);
                OdbcUtil.validateInputPacketSize(inPacket);
                return;
              case 'D':
                c = inPacket.readByteChar();
                handle = inPacket.readString();
                odbcPs = null;
                portal = null;
                if (c == 'S') {
                    odbcPs = (OdbcPreparedStatement)
                        sessionOdbcPsMap.get(handle);
                } else if (c == 'P') {
                    portal = (StatementPortal) sessionOdbcPortalMap.get(handle);
                } else {
                    throw new RecoverableOdbcFailure(null,
                        "Description packet request type invalid: " + c,
                        "08P01");
                }
                if (server.isTrace()) {
                    server.printWithThread(
                        "Received Describe request for " + c + " of  handle '"
                        + handle + "'");
                }
                if (odbcPs == null && portal == null) {
                    throw new RecoverableOdbcFailure(null,
                        "No object present for " + c + " handle: " + handle,
                        "08P01");
                }
                Result ackResult = (odbcPs == null)
                    ? portal.ackResult : odbcPs.ackResult;
                pmd = ackResult.parameterMetaData;
                paramCount = pmd.getColumnCount();
                org.hsqldb.types.Type[] paramTypes = pmd.getParameterTypes();
                if (paramCount != paramTypes.length) {
                    throw new RecoverableOdbcFailure(
                        "Parameter count mismatch.  Count of "
                        + paramCount + " reported, but there are "
                        + paramTypes.length + " param md objects");
                }

                if (c == 'S') {
                    outPacket.writeShort(paramCount);
                    for (int i = 0; i < paramTypes.length; i++) {
                        outPacket.writeInt(new PgType(paramTypes[i]).getOid());
                    }
                    outPacket.xmit('t', dataOutput);
                      // ParameterDescription packet
                }

                org.hsqldb.result.ResultMetaData md = ackResult.metaData;
                if (md.getColumnCount() < 1) {
                    if (server.isTrace()) {
                        server.printWithThread(
                            "Non-rowset query so returning NoData packet");
                    }
                    // Send NoData packet because no columnar output from
                    // this statement.
                    outPacket.xmit('n', dataOutput);
                    OdbcUtil.validateInputPacketSize(inPacket);
                    return;
                }
                if (md.getColumnCount() != md.getExtendedColumnCount()) {
                    throw new RecoverableOdbcFailure(
                        "Output column count mismatch: "
                        + md.getColumnCount() + " cols. and "
                        + md.getExtendedColumnCount()
                        + " extended cols. reported");
                }
                String[] colNames = md.getGeneratedColumnNames();
                if (md.getColumnCount() != colNames.length) {
                    throw new RecoverableOdbcFailure(
                        "Couldn't get all column names: "
                        + md.getColumnCount() + " cols. but only got "
                        + colNames.length + " col. names");
                }
                org.hsqldb.types.Type[] colTypes = md.getParameterTypes();
                PgType[] pgTypes = new PgType[colTypes.length];
                org.hsqldb.ColumnBase[] colDefs = md.columns;
                for (int i = 0; i < pgTypes.length; i++) {
                    pgTypes[i] = new PgType(colTypes[i]);
                }
                if (colNames.length != colDefs.length) {
                    throw new RecoverableOdbcFailure("Col data mismatch.  "
                            + colDefs.length + " col instances but "
                            + colNames.length + " col names");
                }

                outPacket.writeShort(colNames.length);  // Num cols.
                for (int i = 0; i < colNames.length; i++) {
                    outPacket.write(colNames[i]); // Col. name
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
                    outPacket.writeInt(pgTypes[i].getOid());
                    // Datatype size  [adtsize]
                    outPacket.writeShort(pgTypes[i].getTypeSize());
                    outPacket.writeInt(-1); // Var size [atttypmod]
                        // TODO:  Get from the colType[i]
                    // This is the size constraint integer
                    // like VARCHAR(12) or DECIMAL(4).
                    // -1 if none specified for this column.
                    outPacket.writeShort(0);
                    // format code must be 0 for describing PS;
                    // otherwise 0 if D packets will follow;
                    // and 1 if B packets will follow.
                }
                outPacket.xmit('T', dataOutput); // Xmit Row Definition
                OdbcUtil.validateInputPacketSize(inPacket);
                return;
              case 'B':
                portalHandle = inPacket.readString();
                psHandle = inPacket.readString();
                int paramFormatCount = inPacket.readUnsignedShort();
                for (int i = 0; i < paramFormatCount; i++) {
                    if (inPacket.readUnsignedShort()  != 0) {
                        throw new RecoverableOdbcFailure(null,
                            "Binary param values not supported yet", "0A000");
                    }
                }
                paramCount = inPacket.readUnsignedShort();
                String[] paramVals = new String[paramCount];
                for (int i = 0; i < paramCount; i++) {
                    paramVals[i] = inPacket.readSizedString();
                }
                int outFormatCount = inPacket.readUnsignedShort();
                for (int i = 0; i < outFormatCount; i++) {
                    if (inPacket.readUnsignedShort()  != 0) {
                        throw new RecoverableOdbcFailure(null,
                            "Binary output values not supported yet", "0A000");
                    }
                }
                if (server.isTrace()) {
                    server.printWithThread(
                        "Received Bind request to make Portal from ("
                        + psHandle + ")' with handle '" + portalHandle + "'");
                }
                odbcPs = (OdbcPreparedStatement) sessionOdbcPsMap.get(psHandle);
                if (odbcPs == null) {
                    throw new RecoverableOdbcFailure(null,
                        "No object present for PS handle: " + psHandle,
                        "08P01");
                }
                if (portalHandle.length() > 0
                        && sessionOdbcPortalMap.containsKey(portalHandle)) {
                    throw new RecoverableOdbcFailure(null,
                        "Portal handle '" + portalHandle + "' already in use.  "
                        + "You must close it before recreating", "08P01");
                }
                pmd = odbcPs.ackResult.parameterMetaData;
                if (paramCount != pmd.getColumnCount()) {
                    throw new RecoverableOdbcFailure(null,
                        "Client didn't specify all " + pmd.getColumnCount()
                        + " parameters (" + paramCount + ')', "08P01");
                }
                new StatementPortal(
                    portalHandle, odbcPs, paramVals, sessionOdbcPortalMap);
                outPacket.xmit('2', dataOutput);
                OdbcUtil.validateInputPacketSize(inPacket);
                return;
              case 'E':
                portalHandle = inPacket.readString();
                int fetchRows = inPacket.readInt();
                if (server.isTrace()) {
                    server.printWithThread(
                        "Received Exec request for " + fetchRows
                        + " rows from portal handle '" + portalHandle + "'");
                }
                portal = (StatementPortal) sessionOdbcPortalMap.get(portalHandle);
                if (portal == null) {
                    throw new RecoverableOdbcFailure(null,
                        "No object present for Portal handle: " + portalHandle,
                        "08P01");
                }

                portal.bindResult.setPreparedExecuteProperties(
                    portal.parameters, fetchRows, 0);
                    // 0 for maxRows means unlimited.  Same for fetchRows.
                rOut = session.execute(portal.bindResult);
                switch (rOut.getType()) {
                    case ResultConstants.UPDATECOUNT:
                        replyString = OdbcUtil.echoBackReplyString(
                            portal.lcQuery, rOut.getUpdateCount());
                        outPacket.write(replyString);
                        outPacket.xmit('C', dataOutput);
                        // end of rows (B or D packets)

                        // A guess about how keeping inOdbcTrans in sync with
                        // client.  N.b. HSQLDB will need to more liberal with
                        // resetting, since DDL causes commits.
                        // TODO:  Consider implications of autocommit mode.
                        if (portal.lcQuery.equals("commit")
                            || portal.lcQuery.startsWith("commit ")
                            || portal.lcQuery.equals("savepoint")
                            || portal.lcQuery.startsWith("savepoint ")) {
                            inOdbcTrans = false;
                        }
                        OdbcUtil.validateInputPacketSize(inPacket);
                        return;
                    case ResultConstants.DATA:
                        break;
                    case ResultConstants.ERROR:
                        throw new RecoverableOdbcFailure(rOut);
                    default:
                        throw new RecoverableOdbcFailure(
                            "Output Result from Portal execution is of "
                            + "unexpected type: " + rOut.getType());
                }
                // See Result.newDataHeadResult() for what we have here
                // .metaData, .navigator
                org.hsqldb.navigator.RowSetNavigator navigator =
                        rOut.getNavigator();
                if (!(navigator instanceof
                    org.hsqldb.navigator.RowSetNavigatorData)) {
                    throw new RecoverableOdbcFailure(
                            "Unexpected RowSetNavigator instance type: "
                            + navigator.getClass().getName());
                }
                org.hsqldb.navigator.RowSetNavigatorData navData =
                    (org.hsqldb.navigator.RowSetNavigatorData) navigator;
                int rowNum = 0;
                int colCount =
                    portal.ackResult.metaData.getExtendedColumnCount();
                while (navData.next()) {
                    rowNum++;
                    Object[] rowData = (Object[]) navData.getCurrent();
                    // Row.getData().  Don't know why *Data.getCurrent()
                    //                 method returns Object instead of O[].
                    //  TODO:  Remove the assertion here:
                    if (rowData == null)
                        throw new RecoverableOdbcFailure("Null row?");
                    if (rowData.length < colCount) {
                        throw new RecoverableOdbcFailure(
                            "Data element mismatch. "
                            + colCount + " metadata cols, yet "
                            + rowData.length + " data elements for row "
                            + rowNum);
                    }
                    //server.printWithThread("Row " + rowNum + " has "
                            //+ rowData.length + " elements");
                    outPacket.writeShort(colCount);
                     // This field is just swallowed by PG ODBC
                     // client, but validated by psql.
                    for (int i = 0; i < colCount; i++) {
                        if (rowData[i] == null) {
                            /*
                            server.printWithThread("R" + rowNum + "C"
                                + (i+1) + " => [null]");
                            */
                            outPacket.writeInt(-1);
                        } else {
                            outPacket.writeSized(rowData[i].toString());
                            // N.b., Postgresql returns "t" and "f",
                            // not "" and "" like we are returning.
                            // But that makes no difference, since
                            // psqlodbc totally mishandles them either
                            // way.
                            /*
                            server.printWithThread("R" + rowNum + "C"
                                + (i+1) + " => ("
                                + rowData[i].getClass().getName()
                                + ") [" + rowData[i] + ']');
                            */
                        }
                    }
                    outPacket.xmit('D', dataOutput); // text row Data
                      /* Should be 'B' to return binary results like Objs
                       * or BLOBs.  Otherwise should be 'B'.
                       * This must corresopnd to the T packet's format
                       * code. */
                }
                if (navigator.afterLast()) {
                    outPacket.write("SELECT");
                    outPacket.xmit('C', dataOutput);
                    // end of rows (B or D packets)
                } else {
                    outPacket.xmit('s', dataOutput);
                }
                // N.b., we return.
                // You might think that this completion of an EXTENDED sequence
                // would end in ReadyForQuery/Z, but no.
                OdbcUtil.validateInputPacketSize(inPacket);
                return;
              case 'C':
                c = inPacket.readByteChar();
                handle = inPacket.readString();
                odbcPs = null;
                portal = null;
                if (c == 'S') {
                    odbcPs = (OdbcPreparedStatement)
                        sessionOdbcPsMap.get(handle);
                    if (odbcPs != null) {
                        odbcPs.close();
                    }
                } else if (c == 'P') {
                    portal = (StatementPortal) sessionOdbcPortalMap.get(handle);
                    if (portal != null) {
                        portal.close();
                    }
                } else {
                    throw new RecoverableOdbcFailure(null,
                        "Description packet request type invalid: " + c,
                        "08P01");
                }
                // TODO:  Try sending a warning to client for both == null.
                // Broke things earlier, but that may have been due to
                // other problems.
                if (server.isTrace()) {
                    server.printWithThread("Closed PS/Portal '"
                            + handle + "'? "
                            + (odbcPs != null || portal != null));
                }
                outPacket.xmit('3', dataOutput);
                OdbcUtil.validateInputPacketSize(inPacket);
                return;
             default:
                throw new RecoverableOdbcFailure(
                    null, "Unsupported operation type (" + inPacket.packetType
                    + ')', "0A000");
                }
           OdbcUtil.validateInputPacketSize(inPacket);
           // All switches that "break" get this ReadyForQuery packet:
           outPacket.reset();
           // The reset is unnecessary now.  For safety in case somebody codes
           // something above which may abort processing of a packet before
           // xmit.
           outPacket.writeByte(inOdbcTrans ? 'T' : 'I');
           outPacket.xmit('Z', dataOutput);
        } catch (RecoverableOdbcFailure rf) {
            Result errorResult = rf.getErrorResult();
            if (errorResult == null) {
                String stateCode = rf.getSqlStateCode();
                String svrMsg = rf.getMessage();
                String cliMsg = rf.getClientMessage();
                if (svrMsg != null) {
                    server.printWithThread(svrMsg);
                } else if (server.isTrace()) {
                    server.printWithThread("Client error: " + cliMsg);
                }
                if (cliMsg != null) {
                    OdbcUtil.alertClient(OdbcUtil.ODBC_SEVERITY_ERROR,
                        cliMsg, stateCode, dataOutput);
                }
            } else {
                if (server.isTrace()) {
                    server.printWithThread("Result object error: "
                        + errorResult.getMainString());
                }
                // This class of error is not considered a Server problem, so
                // we don't log on the server side.
                OdbcUtil.alertClient(OdbcUtil.ODBC_SEVERITY_ERROR,
                    errorResult.getMainString(), errorResult.getSubString(),
                    dataOutput);
            }
            switch (odbcCommMode) {
               case OdbcUtil.ODBC_SIMPLE_MODE:
                    outPacket.reset();  /// transaction status = Error
                    outPacket.writeByte('E');  /// transaction status = Error
                    outPacket.xmit('Z', dataOutput);
                    break;
                case OdbcUtil.ODBC_EXTENDED_MODE:
                    odbcCommMode = OdbcUtil.ODBC_EXT_RECOVER_MODE;
                    server.printWithThread("Reverting to EXT_RECOVER mode");
                    break;
            }
        }
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
                server.print(
                    "Rejected attempt from client using hsql HTTP protocol");
                return 0;
            case 0:
                // For ODBC protocol, this is the first byte of a 4-byte int
                // size.  The size can never be large enough that the first
                // byte will be non-zero.
                streamProtocol = ODBC_STREAM_PROTOCOL;
                break;
            default:
                streamProtocol = HSQL_STREAM_PROTOCOL;
                // HSQL protocol client
        }
        return firstInt;
    }

    private void odbcConnect(int firstInt) throws IOException, HsqlException {
        /* Until client receives teh ReadyForQuery packet at the end of this
         * method, we (the server) initiate all packet exchanges. */
        int major = dataInput.readUnsignedShort();
        int minor = dataInput.readUnsignedShort();

        // Can just return to fail, until the value of "session" is set below.
        if (major == 1 && minor == 7) {
            // This is what old HyperSQL versions always send
            // TODO:  Send client a 1.8-compatible SQLException
            server.print("A pre-9.0 client attempted to connect.  "
                    + "We rejected them.");
            return;
        }
        if (major == 1234 && minor == 5679) {
            // No reason to pay any attention to the size header in this case.
            dataOutput.writeByte('N');  // SSL not supported yet
            // TODO:  Implement SSL here (and reply with 'S')
            odbcConnect(dataInput.readInt());
            return;
        }
        if (major == 1234 && minor == 5678) {
            // No reason to pay any attention to the size header in this case.
            if (firstInt != 16) {
                server.print(
                    "ODBC cancellation request sent wrong packet length: "
                    + firstInt);
            }
            server.print("Got an ODBC cancelation request for thread ID "
                    + dataInput.readInt() + ", but we don't support "
                    + "OOB cancellation yet.  "
                    + "Ignoring this request and closing the connection.");
            // N.b.,  Spec says to NOT reply to client in this case.
            return;
        }
        server.printWithThread("ODBC client connected.  "
                + "ODBC Protocol Compatibility Version " + major + '.' + minor);
        OdbcPacketInputStream inPacket =
            OdbcPacketInputStream.newOdbcPacketInputStream(
            dataInput, firstInt - 8);
            // - 4 for size of firstInt - 2 for major - 2 for minor
        java.util.Map stringPairs = inPacket.readStringPairs();
        // server.print("String Pairs: " + stringPairs);
        try {
            try {
                OdbcUtil.validateInputPacketSize(inPacket);
            } catch (RecoverableOdbcFailure rf) {
                // In this case, we do not treat it as recoverable
                throw new ClientFailure(rf.getMessage(), rf.getClientMessage());
            }
            inPacket.close();
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
            dataOutput.writeInt(OdbcUtil.ODBC_AUTH_REQ_PASSWORD);
            // areq of auth. mode.
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
                    "Client submitted invalid password length " + len,
                    "Invalid password length " + len);
            }
            String password = ServerConnection.readNullTermdUTF(len, dataInput);

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
            OdbcUtil.alertClient(
                OdbcUtil.ODBC_SEVERITY_FATAL, cf.getClientMessage(),
                "08006", dataOutput); // Code means CONNECTION FAILURE
            return;
        }
        outPacket = OdbcPacketOutputStream.newOdbcPacketOutputStream();

        outPacket.writeInt(OdbcUtil.ODBC_AUTH_REQ_OK); //success
        outPacket.xmit('R', dataOutput); // Notify client of success

        if (!server.isSilent()) {
            server.printWithThread(mThread + ":Connected user '" + user + "'");
        }

        for (int i = 0; i < OdbcUtil.hardcodedParams.length; i++) {
            OdbcUtil.writeParam(OdbcUtil.hardcodedParams[i][0],
                OdbcUtil.hardcodedParams[i][1], dataOutput);
        }

        // If/when we implement OOB cancellation, we would send the
        // Session identifier and key here, with a 'K' packet.

        outPacket.writeByte('I'); // Trans. status = Not in transaction
        outPacket.xmit('Z', dataOutput); // Notify client of success
        // This ReadyForQuery turns over responsibility to initiate packet
        // exchanges to the client.

        OdbcUtil.alertClient(OdbcUtil.ODBC_SEVERITY_INFO,
            "MHello\nYou have connected to HyperSQL ODBC Server", dataOutput);
    }

    private java.util.Map sessionOdbcPsMap = new java.util.HashMap();
    private java.util.Map sessionOdbcPortalMap = new java.util.HashMap();

    /**
     * Read String directy from dataInput.
     *
     * @param reqLength Required length
     */
    private static String readNullTermdUTF(
    int reqLength, java.io.InputStream istream) throws IOException {
        /* Would be MUCH easier to do this with Java6's String
         * encoding/decoding operations */
        int bytesRead = 0;
        byte[] ba = new byte[reqLength + 3];
        ba[0] = (byte) (reqLength >>> 8);
        ba[1] = (byte) reqLength;
        while (bytesRead < reqLength + 1) {
            bytesRead +=
                istream.read(ba, 2 + bytesRead, reqLength + 1 - bytesRead);
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

    private int streamProtocol = UNDEFINED_STREAM_PROTOCOL;

    // Tentative state variable
    static final int UNDEFINED_STREAM_PROTOCOL = 0;
    static final int HSQL_STREAM_PROTOCOL = 1;
    static final int ODBC_STREAM_PROTOCOL = 2;

    int odbcCommMode = OdbcUtil.ODBC_SIMPLE_MODE;
}
