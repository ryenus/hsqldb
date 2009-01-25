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
     */
    private void init() {

        runnerThread = Thread.currentThread();
        keepAlive    = true;

        try {
            socket.setTcpNoDelay(true);

            dataInput  = new DataInputStream(socket.getInputStream());
            dataOutput = new DataOutputStream(socket.getOutputStream());

            handshake();

            Result resultIn = Result.newResult(dataInput, rowIn);

            resultIn.readAdditionalResults(session, dataInput, rowIn);

            Result resultOut;

            resultOut = setDatabase(resultIn);

            resultOut.write(dataOutput, rowOut);
        } catch (Exception e) {

            // Only "unexpected" failures are caught here.
            // Expected failures will have been handled (by sending feedback
            // to user-- with an output Result for normal protocols), then
            // continuing.
            StringBuffer sb =
                new StringBuffer(mThread + ": Failed to connect client.");

            if (user != null) {
                sb.append("  User '" + user + "'.");
            }

            server.printWithThread(sb.toString() + "  Stack trace follows.");
            server.printStackTrace(e);
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
                    Result resultIn = Result.newResult(dataInput, rowIn);

                    resultIn.readAdditionalResults(session, dataInput, rowIn);
                    server.printRequest(mThread, resultIn);

                    Result resultOut = null;
                    int    type      = resultIn.getType();

                    if (type == ResultConstants.CONNECT) {
                        resultOut = setDatabase(resultIn);
                    } else if (type == ResultConstants.DISCONNECT) {
                        keepAlive = false;

                        break;
                    } else if (type == ResultConstants.RESETSESSION) {
                        resetSession();

                        continue;
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
                server.printWithThread(mThread + ":trying to connect user "
                                       + user);
            }

            session = DatabaseManager.newSession(dbID, user,
                                                 resultIn.getSubString(),
                                                 resultIn.getUpdateCount());

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

    /**
     * Don't want this too high, or users may give up before seeing the
     *  banner.  Can't be too low or we could close a valid but slow
     *  client connection.
     */
    public static long MAX_WAIT_FOR_CLIENT_DATA   = 1000;    // ms.
    public static long CLIENT_DATA_POLLING_PERIOD = 100;     // ms.

    /**
     * The only known case where a connection attempt will get stuck is
     * if client connects with hsqls to a https server; or
     * hsql to a http server.
     * All other client X server combinations are handled gracefully.
     */
    public void handshake() throws IOException, HsqlException {

        long clientDataDeadline = new java.util.Date().getTime()
                                  + MAX_WAIT_FOR_CLIENT_DATA;

        if (!(socket instanceof javax.net.ssl.SSLSocket)) {

            // available() does not work for SSL socket input stream
            do {
                try {
                    Thread.sleep(CLIENT_DATA_POLLING_PERIOD);
                } catch (InterruptedException ie) {}
            } while (dataInput.available() < 5
                     && new java.util.Date().getTime() < clientDataDeadline);

            // Old HSQLDB clients will send resultType byte + 4 length bytes
            // New HSQLDB clients will send NCV int + above = 9 bytes
            if (dataInput.available() < 1) {
                dataOutput.write(
                    (TEXTBANNER_PART1
                     + ClientConnection.NETWORK_COMPATIBILITY_VERSION
                     + TEXTBANNER_PART2 + '\n').getBytes());
                dataOutput.flush();

                throw Error.error(ErrorCode.SERVER_UNKNOWN_CLIENT);
            }
        }

        java.io.DataInputStream pipeInput = null;

        {                    // This block is only for testing for HSQLDB client < 1.9

            // Need to use a pipe because we need to re-read the data
            // as a different data type after this test.
            // FilterInputStream's mark/reset capability is definitely not
            // supported for the stream we are working with here.
            byte[]            littleBuffer = new byte[3];
            PipedInputStream  inPipe       = new PipedInputStream();
            PipedOutputStream outPipe      = new PipedOutputStream(inPipe);

            pipeInput = new java.io.DataInputStream(inPipe);

            java.io.DataOutputStream pipeOutput =
                new java.io.DataOutputStream(outPipe);
            int legacyResultType = dataInput.readByte();

            switch (legacyResultType) {

                case 0 :

                    // Determined empirically.
                    // Code looks like it should be
                    // ResultConstants.CONNECT
                    // TODO:  Send client a 1.8-compatible SQLException
                    throw Error.error(ErrorCode.SERVER_VERSIONS_INCOMPATIBLE,
                                      0, new String[] {
                        "pre-9.0",
                        ClientConnection.NETWORK_COMPATIBILITY_VERSION
                    });
                case 80 :    // Empirically
                    throw Error.error(ErrorCode.SERVER_HTTP_NOT_HSQL_PROTOCOL);
                default :

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

        if (verString.equals(ClientConnection.NETWORK_COMPATIBILITY_VERSION)) {
            return;    // Success case
        }

        // Only error handling remains
        throw Error.error(ErrorCode.SERVER_VERSIONS_INCOMPATIBLE, 0,
                          new String[] {
            verString, ClientConnection.NETWORK_COMPATIBILITY_VERSION
        });
    }
}
