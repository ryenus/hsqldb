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
            handshake();
            switch (streamProtocol) {
                case HSQL_STREAM_PROTOCOL:

                    Result resultIn = Result.newResult(dataInput, rowIn);

                    resultIn.readAdditionalResults(session, dataInput, rowIn);

                    Result resultOut;

                    resultOut = setDatabase(resultIn);

                    resultOut.write(dataOutput, rowOut);
                    break;
                case ODBC_STREAM_PROTOCOL:
                    odbcConnect();
                    break;
                default:
                    // handshake() is responsible for notifications if it fails
                    // to detect a valid protocol on the stream.
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
        char op = (char) dataInput.readByte();
        server.print("Got op (" + op + ')');
        switch (op) {
            case 'X':
                throw cleanExit;
            case 'Q':
                String sql = readNullTermdUTF();
                if (sql.startsWith("BEGIN;")) {
                    sql = sql.substring("BEGIN;".length());
                    server.printWithThread("ODBC Trans started");
                    inOdbcTrans = true;
                    dataOutput.writeByte('C');
                    writeNullTermdUTF("BEGIN");
                }
                String normalized = sql.trim().toLowerCase();
                if (server.isTrace()) {
                    server.printWithThread("Received query (" + sql + ')');
                }
                if (sql.trim().length() < 1) {
                    dataOutput.writeByte('I'); // Empty Resultset
                    dataOutput.writeByte(0);
                    dataOutput.writeByte('Z');
                } else if (normalized.startsWith("select version()")) {
                    server.print("Simulating 'select version()'...");
                    // N.b., skipping 'P' xmit here.
                    // This sends a portal name, but it is completely ignored
                    // by the ODBC client.
                    dataOutput.writeByte('T'); // sending a Tuple (row)
                    write((short) 1);          // Num cols.
                    writeNullTermdUTF("version"); // Col. name
                    dataOutput.writeInt(25); // Datatype ID  [adtid]
                    write((short) -1);       // Datatype size  [adtsize]
                    dataOutput.writeInt(-1); // Var size (always -1 so far)
                                             // [atttypmod]
                    dataOutput.writeByte('D'); // text row Data
                    dataOutput.writeByte(-1);   // bit map of null vals in row
                    writeUTF("PostgreSQL 8.3.1 on x86_64-unknown-linux-gnu, "
                            + "compiled by GCC gcc (SUSE Linux) 4.3.1 20080507 "
                            + "(prerelease) [gcc-4_3-branch revision 135036]",
                            false);
                    dataOutput.writeByte('C'); // end of rows
                    writeNullTermdUTF("SELECT");
                    dataOutput.writeByte('Z');
                } else if (normalized.startsWith(
                    "select pg_client_encoding()")) {
                    server.print("Simulating 'select pg_client_encoding()'...");
                    // N.b., skipping 'P' xmit here.
                    // This sends a portal name, but it is completely ignored
                    // by the ODBC client.
                    dataOutput.writeByte('T'); // sending a Tuple (row)
                    write((short) 1);          // Num cols.
                    writeNullTermdUTF("pg_client_encoding"); // Col. name
                    dataOutput.writeInt(19); // Datatype ID  [adtid]
                    write((short) 64);       // Datatype size  [adtsize]
                    dataOutput.writeInt(-1); // Var size (always -1 so far)
                                             // [atttypmod]
                    dataOutput.writeByte('D'); // text row Data
                    dataOutput.writeByte(-1);   // bit map of null vals in row
                    writeUTF("SQL_ASCII", false);
                    dataOutput.writeByte('C'); // end of rows
                    writeNullTermdUTF("SELECT");
                    dataOutput.writeByte('Z');
                } else if (normalized.startsWith(
                    "select oid, typbasetype from")) {
                    server.print("Simulating 'select oid, typbasetype...'");
                    // N.b., skipping a 'P' xmit here, which Postgresql servers
                    // transmit.  I haven't figured out the purpose of that yet.
                    dataOutput.writeByte('T'); // sending a Tuple (row)
                    write((short) 2);          // Num cols.
                    writeNullTermdUTF("oid"); // Col. name
                    dataOutput.writeInt(26); // Datatype ID  [adtid]
                    write((short) 4);       // Datatype size  [adtsize]
                    dataOutput.writeInt(-1); // Var size (always -1 so far)
                                             // [atttypmod]
                    writeNullTermdUTF("typbasetype"); // Col. name
                    dataOutput.writeInt(26); // Datatype ID  [adtid]
                    write((short) 4);       // Datatype size  [adtsize]
                    dataOutput.writeInt(-1); // Var size (always -1 so far)
                                             // [atttypmod]
                    // This query returns no rows.  typenam "lo"??
                    dataOutput.writeByte('C'); // end of rows
                    writeNullTermdUTF("SELECT");
                    dataOutput.writeByte('Z');
                } else if (normalized.startsWith("set datestyle to ")
                    || normalized.startsWith("set extra_float_digits to ")) {
                    //|| normalized.startsWith("set ksqo to ")) {
                    server.print("Stubbing a 'SET command'...");
                    dataOutput.writeByte('C');
                    writeNullTermdUTF("SET");
                    dataOutput.writeByte('Z');
                } else if (normalized.startsWith("select ")) {
                    server.print("Performing a real non-prepared SELECT...");
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
                    if (rOut.getType() != ResultConstants.DATA) {
                        throw new RuntimeException(
                                "Output Result from SELECT statement not of "
                                + "type DATA.  Type (" + rOut.getType()
                                + ')');
                    }
                    // See Result.newDataHeadResult() for what we have here
                    // .metaData, .navigator
                    org.hsqldb.navigator.RowSetNavigator navigator =
                            rOut.getNavigator();
                    if (!(navigator instanceof
                        org.hsqldb.navigator.RowSetNavigatorData)) {
                        throw new RuntimeException(
                                "Unexpected RowSetNavigator instance type: "
                                + navigator.getClass().getName());
                    }
                    org.hsqldb.navigator.RowSetNavigatorData navData =
                        (org.hsqldb.navigator.RowSetNavigatorData) navigator;
                    // N.b., skipping a 'P' xmit here, which Postgresql servers
                    // transmit.  I haven't figured out the purpose of that yet.
                    dataOutput.writeByte('T'); // sending a Tuple (row)
                    int rowNum = 0;
                    while (navData.next()) {
                        rowNum++;
                        Object[] rowData = (Object[]) navData.getCurrent();
                        if (rowNum == 1) {
                            //TODO: This isn't going to work for 0 row queries.
                            //Need to get the metadata before getting any data!
                            //Just don't know how to do that yet.
                            write((short) (rowData.length - 1));  // Num cols.
                            for (int i = 0; i < rowData.length - 1; i++) {
                                writeNullTermdUTF("C" + (i+1)); // Col. name
                                dataOutput.writeInt(25); // Datatype ID  [adtid]
                                write((short) -1);       // Datatype size  [adtsize]
                                dataOutput.writeInt(-1); // Var size (always -1 so far)
                                                         // [atttypmod]
                            }
                        }
                        // Row.getData().  Don't know why *Data.getCurrent()
                        //                 method returns Object instead of O[].
                        //  TODO:  Remove the assertion here:
                        if (rowData == null)
                            throw new RuntimeException("Null row?");
                        dataOutput.writeByte('D'); // text row Data
                        dataOutput.writeByte(-1);   // bit map of null vals in row
                        for (int i = 0; i < rowData.length - 1; i++) {
                            /*
                            System.err.println("R" + rowNum + "C" + (i+1)
                                    + " => (" + rowData[i].getClass().getName()
                                    + ") [" + rowData[i] + ']');
                            */
                            writeUTF(rowData[i].toString(), false);
                        }
                    }
                    dataOutput.writeByte('C'); // end of rows
                    writeNullTermdUTF("SELECT");
                    dataOutput.writeByte('Z');

                } else {
                /*
                } else if (normalized.startsWith("update ")
                        || normalized.startsWith("commit ")
                        || normalized.startsWith("rollback ")
                        || normalized.equals("commit")
                        || normalized.equals("rollback")
                ) {
                */
                    // TODO:  ROLLBACK is badly broken.
                    // I think that when a ROLLBACK of an update is done here,
                    // it actually inserts new rows!
                    server.print("Performing a real EXECDIRECT...");
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
                    if (rOut.getType() != ResultConstants.UPDATECOUNT) {
                        throw new RuntimeException(
                                "Output Result from UPDATE statement not of "
                                + "type UPDATECOUNT.  Type (" + rOut.getType()
                                + ')');
                    }
                    dataOutput.writeByte('C');
                    writeNullTermdUTF("UPDATE " + rOut.getUpdateCount());
                    dataOutput.writeByte('Z');
                /*
                } else {
                    warnOdbcClient(
                            false, "Sorry, only null queries supported so far");
                */
                }
                break;
            default:
                warnOdbcClient(
                        false, "Unsupported operation type (" + op + ')');
                // May be impossible to recover in practice, since every
                // op. type will probably be followed by data which we will
                // choke on forthwith.  }
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
     */
    public void handshake() throws IOException, HsqlException {
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
            if (dataInput.available() < 1) {
                dataOutput.write((TEXTBANNER_PART1
                        + ClientConnection.NETWORK_COMPATIBILITY_VERSION
                        + TEXTBANNER_PART2 + '\n').getBytes());
                dataOutput.flush();
                throw Error.error(ErrorCode.SERVER_UNKNOWN_CLIENT);
            }
        }

        DataInputStream pipeInput = null;
        { // This block is only for testing for HSQLDB client < 1.9
            // Need to use a pipe because we need to re-read the data
            // as a different data type after this test.
            byte[] littleBuffer = new byte[3];
            PipedInputStream inPipe = new PipedInputStream();
            PipedOutputStream outPipe = new PipedOutputStream(inPipe);
            pipeInput = new DataInputStream(inPipe);
            DataOutputStream pipeOutput = new DataOutputStream(outPipe);

            byte firstByte = dataInput.readByte();
            int legacyResultType = firstByte;
            switch (legacyResultType) {
                case 0:
                    // Read 3 more bytest to make an int, to determine ODBC
                    // client vs. Legacy HSQL:
                    int descriminatorInt = ((firstByte & 0xff) << 24)
                        + ((dataInput.readByte() & 0xff) << 16)
                        + ((dataInput.readByte() & 0xff) << 8)
                        + (dataInput.readByte() & 0xff);
                    switch (descriminatorInt) {
                        case 34:
                             // Determined empirically.
                             // Code looks like it should be
                             // ResultConstants.CONNECT
                            // TODO:  Send client a 1.8-compatible SQLException
                            throw Error.error(
                                 ErrorCode.SERVER_VERSIONS_INCOMPATIBLE, 0,
                                 new String[] { "pre-9.0",
                             ClientConnection.NETWORK_COMPATIBILITY_VERSION});
                        case 296:
                            streamProtocol = ODBC_STREAM_PROTOCOL;
                            return;  // Success case
                        default:
                            throw Error.error(
                                    ErrorCode.SERVER_INCOMPLETE_HANDSHAKE_READ);
                            // TODO:  Better error message, like:
                            // "Unrecognized Client"
                    }
                case 80: // Empirically
                    throw Error.error(ErrorCode.SERVER_HTTP_NOT_HSQL_PROTOCOL);
                default:
                    // A Ok.
            }

            // Write entire int to the Pipe, since we've already read one
            // byte of the int from dataInput.
            pipeOutput.writeByte(legacyResultType);
            if (dataInput.read(littleBuffer) != 3) {
                throw Error.error(ErrorCode.SERVER_INCOMPLETE_HANDSHAKE_READ);
            }
            pipeOutput.write(littleBuffer);
            pipeOutput.close();
        }

        int verInt = pipeInput.readInt();
        pipeInput.close();
        // If we didn't need to read the byte off of dataInput for legacy
        // testing above, we would read like this:
        //int verInt = dataInput.readInt();
        //if (verInt > 0)
        String verString = ClientConnection.toNcvString(verInt);
        if (verString.equals(
                ClientConnection.NETWORK_COMPATIBILITY_VERSION)) {
            streamProtocol = HSQL_STREAM_PROTOCOL;
            return;  // Success case
        }
        // Only error handling remains

        throw Error.error(ErrorCode.SERVER_VERSIONS_INCOMPATIBLE, 0,
                new String[] {verString,
                ClientConnection.NETWORK_COMPATIBILITY_VERSION});
    }

    private void odbcConnect() throws IOException, HsqlException {
        int major = dataInput.readUnsignedShort();
        int minor = dataInput.readUnsignedShort();
        server.print("ODBC client connected.  "
                + "ODBC Protocol Compatibility Version " + major + '.' + minor);
        String databaseName = readNullTermdUTF(ODBC_SM_DATABASE);
        if (databaseName.equals("/")) {
            // Work-around because ODBC doesn't allow "" for Database name
            databaseName = "";
        }
        server.print("DB: " + databaseName);
        user = readNullTermdUTF(ODBC_SM_USER);
        server.print("User: " + user);
        server.print("Opts: " + readNullTermdUTF(ODBC_SM_OPTIONS));
        dataInput.skipBytes(ODBC_SM_UNUSED);
        server.print("tty: " + readNullTermdUTF(ODBC_SM_TTY));
        dataOutput.writeByte('N');
        writeNullTermdUTF("Hello, you have connected to HyperSQL ODBC Server");
        /*  Seems that this sequence is equivalent to doing nothing.
         *  Not required by Postgresql ODBC driver (though it wouldn't hurt).
         *  I leave this here, commented out, because non-ODBC Postgresql
         *  clients may expect R behavior, and this may satisfy them.
        dataOutput.writeByte('R');
        dataOutput.writeByte(0);
        */

        /* Unencoded/unsalted authentication */
        dataOutput.writeByte('R');
        dataOutput.writeInt(ODBC_AUTH_REQ_PASSWORD);
        int len = dataInput.readInt() - 5;
            // Is password len after -4 for count int -1 for null term
        if (len < 0)
            throw new IllegalArgumentException(
                    "Non-empty passwords required.  "
                    + "User submitted password length " + len);
        String password = readNullTermdUTF(len + 1);
        if (password.length() != len)
            throw new IllegalStateException("Password contain a null?  "
                + "Expected length " + len
                + ", but received password has length " + password.length());

        dbIndex = server.getDBIndex(databaseName);
        dbID    = server.dbID[dbIndex];

        if (!server.isSilent()) {
            server.printWithThread(mThread + ":Trying to connect user '"
                               + user + "' to DB (" + databaseName + ')');
        }

        session = DatabaseManager.newSession(dbID, user,
                                             password, 0);
        // TODO:  Find out what updateCount, the last para, is for:
        //                                   resultIn.getUpdateCount());
        //
        if (!server.isSilent()) {
            server.printWithThread(mThread + ":Connected user '" + user + "'");
        }

        dataOutput.writeByte('Z');
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

    private String readNullTermdUTF(int length) throws IOException {
        /* Would be MUCH easier to do this with Java6's String
         * encoding/decoding operations */
        int bytesRead = 0;
        byte[] ba = new byte[length + 2];
        while (bytesRead < length) {
            bytesRead += dataInput.read(ba, 2 + bytesRead, length - bytesRead);
        }
        // Could read bytes 1-at-a-time then skipBytes() after see null byte.
        // Reading in chunks like this is probably more efficient even though
        // it writes nulls for nothing.
        int firstNull = 1;
        while (true) {
            firstNull++;
            if (firstNull == ba.length) {
                throw new IOException("Unterminated string on input");
            }
            if (ba[firstNull] == (byte) 0) {
                break;
            }
        }

        firstNull -= 2;  // Want length from AFTER the size prefix
        ba[0] = (byte) (firstNull >>> 8);
        ba[1] = (byte) firstNull;

        java.io.DataInputStream dis =
            new java.io.DataInputStream(new ByteArrayInputStream(ba));
        String s = dis.readUTF();
        //String s = java.io.DataInputStream.readUTF(dis);
        // TODO:  Test the previous two to see if one works better for
        // high-order characters.
        dis.close();
        return s;
    }

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

    // Constants taken from connection.h
    static private final int ODBC_SM_DATABASE = 64;
    static private final int ODBC_SM_USER = 32;
    static private final int ODBC_SM_OPTIONS = 64;
    static private final int ODBC_SM_UNUSED = 64;
    static private final int ODBC_SM_TTY = 64;
    static private final int ODBC_AUTH_REQ_PASSWORD = 3;

    // Tentative state variable
    static private final int UNDEFINED_STREAM_PROTOCOL = 0;
    static private final int HSQL_STREAM_PROTOCOL = 1;
    static private final int ODBC_STREAM_PROTOCOL = 2;
    private int streamProtocol = UNDEFINED_STREAM_PROTOCOL;

    private void warnOdbcClient(boolean disconnect, String message)
    throws IOException {
        dataOutput.writeByte('E');
        writeNullTermdUTF((disconnect ? "FATAL " : "") + message);
        /*
         * This method makes more sense from Java, but the "new_format" method
         * sends 'E', '\0', length, message
         * where length = int length of message + 4 for the length int itself.
         */
    }
}
