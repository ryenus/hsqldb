/* Copyright (c) 2001-2007, The HSQL Development Group
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


package org.hsqldb;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;

import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.DataOutputStream;
import org.hsqldb.navigator.ClientRowSetNavigator;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.result.ResultLob;
import org.hsqldb.rowio.RowInputBinaryNet;
import org.hsqldb.rowio.RowOutputBinaryNet;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.server.HsqlSocketFactory;
import org.hsqldb.store.ValuePool;

/**
 * Base remote session proxy implementation. Uses instances of Result to
 * transmit and recieve data. This implementation utilises the updated HSQL
 * protocol.
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.7.2
 */
public class HSQLClientConnection implements SessionInterface {

    static final int             BUFFER_SIZE = 0x1000;
    final byte[]                 mainBuffer  = new byte[BUFFER_SIZE];
    private boolean              isClosed;
    private Socket               socket;
    protected DataOutputStream   dataOutput;
    protected DataInputStream    dataInput;
    protected RowOutputInterface rowOut;
    protected RowInputBinaryNet  rowIn;
    private Result               resultOut;
    private long                 sessionID;
    private long                 lobIDSequence;

    //
    private boolean isReadOnly   = false;
    private boolean isAutoCommit = true;

    //
    String  host;
    int     port;
    String  path;
    String  database;
    boolean isTLS;
    int     databaseID;

    /**
     * Establishes a connection to the server.
     */
    public HSQLClientConnection(String host, int port, String path,
                                String database, boolean isTLS, String user,
                                String password) throws HsqlException {

        this.host     = host;
        this.port     = port;
        this.path     = path;
        this.database = database;
        this.isTLS    = isTLS;

        initStructures();

        Result login = Result.newConnectionAttemptRequest(user, password,
            database);

        initConnection(host, port, isTLS);

        Result resultIn = execute(login);

        if (resultIn.isError()) {
            throw Trace.error(resultIn);
        }

        sessionID  = resultIn.getSessionId();
        databaseID = resultIn.getDatabaseId();
    }

    /**
     * resultOut is reused to trasmit all remote calls for session management.
     * Here the structure is preset for sending attributes.
     */
    private void initStructures() {

        RowOutputBinaryNet rowOutTemp = new RowOutputBinaryNet(mainBuffer);

        rowOut    = rowOutTemp;
        rowIn     = new RowInputBinaryNet(rowOutTemp);
        resultOut = Result.newSessionAttributesResult();

        resultOut.getNavigator().add(new Object[7]);
    }

    protected void initConnection(String host, int port,
                                  boolean isTLS) throws HsqlException {
        openConnection(host, port, isTLS);
    }

    protected void openConnection(String host, int port,
                                  boolean isTLS) throws HsqlException {

        try {
            socket = HsqlSocketFactory.getInstance(isTLS).createSocket(host,
                                                   port);
            dataOutput = new DataOutputStream(socket.getOutputStream());
            dataInput = new DataInputStream(
                new BufferedInputStream(socket.getInputStream()));
        } catch (Exception e) {

/** @todo fredt - change error to no connetion established */
            throw Trace.error(Trace.SOCKET_ERROR);
        }
    }

    protected void closeConnection() {

        try {
            if (socket != null) {
                socket.close();
            }
        } catch (Exception e) {}

        socket = null;
    }

    public synchronized Result execute(Result r) throws HsqlException {

        try {
            r.setSessionId(sessionID);
            r.setDatabaseId(databaseID);
            write(r);

            return read();
        } catch (Throwable e) {
            throw Trace.error(Trace.CONNECTION_IS_BROKEN, e.toString());
        }
    }

    public ClientRowSetNavigator getRows(long navigatorId, int offset,
                                         int size) throws HsqlException {

        try {
            resultOut.setResultType(ResultConstants.REQUESTDATA);
            resultOut.setResultId(navigatorId);
            resultOut.setUpdateCount(offset);
            resultOut.setFetchSize(size);

            Result result = execute(resultOut);

            return (ClientRowSetNavigator) result.getNavigator();
        } catch (Throwable e) {
            throw Trace.error(Trace.CONNECTION_IS_BROKEN, e.toString());
        }
    }

    public void closeNavigator(long navigatorId) {

        try {
            resultOut.setResultType(ResultConstants.CLOSE_RESULT);
            resultOut.setResultId(navigatorId);
            execute(resultOut);
        } catch (Throwable e) {}
    }

    public synchronized void close() {

        if (isClosed) {
            return;
        }

        isClosed = true;

        try {
            resultOut.setResultType(ResultConstants.DISCONNECT);
            execute(resultOut);
        } catch (Exception e) {}

        try {
            closeConnection();
        } catch (Exception e) {}
    }

    private Object getAttribute(int id) throws HsqlException {

        resultOut.setResultType(ResultConstants.GETSESSIONATTR);

        Result in = execute(resultOut);

        if (in.isError()) {
            throw Trace.error(in);
        }

        return in.getSingleRowData()[id];
    }

    private void setAttribute(Object property, int id) throws HsqlException {

        resultOut.setResultType(ResultConstants.SETSESSIONATTR);

        Object[] data = resultOut.getSingleRowData();

        ArrayUtil.fillArray(data, null);

        data[id] = property;

        Result resultIn = execute(resultOut);

        if (resultIn.isError()) {
            throw Trace.error(resultIn);
        }
    }

    public synchronized boolean isReadOnly() throws HsqlException {

        Object info = getAttribute(Session.INFO_CONNECTION_READONLY);

        isReadOnly = ((Boolean) info).booleanValue();

        return isReadOnly;
    }

    public synchronized void setReadOnly(boolean mode) throws HsqlException {

        if (mode != isReadOnly) {
            setAttribute(mode ? Boolean.TRUE
                              : Boolean.FALSE, Session
                                  .INFO_CONNECTION_READONLY);

            isReadOnly = mode;
        }
    }

    public synchronized boolean isAutoCommit() throws HsqlException {

        Object info = getAttribute(SessionInterface.INFO_AUTOCOMMIT);

        isAutoCommit = ((Boolean) info).booleanValue();

        return isAutoCommit;
    }

    public synchronized void setAutoCommit(boolean mode)
    throws HsqlException {

        if (mode != isAutoCommit) {
            setAttribute(mode ? Boolean.TRUE
                              : Boolean.FALSE, SessionInterface
                                  .INFO_AUTOCOMMIT);

            isAutoCommit = mode;
        }
    }

    public synchronized void setIsolation(int level) throws HsqlException {
        setAttribute(ValuePool.getInt(level),
                     SessionInterface.INFO_ISOLATION);
    }

    public synchronized int getIsolation() throws HsqlException {

        Object info = getAttribute(Session.INFO_ISOLATION);

        return ((Integer) info).intValue();
    }

    public synchronized boolean isClosed() {
        return isClosed;
    }

    public Session getSession() {
        return null;
    }

    public synchronized void startPhasedTransaction() throws HsqlException {}

    public synchronized void prepareCommit() throws HsqlException {

        resultOut.setAsTransactionEndRequest(ResultConstants.PREPARECOMMIT,
                                             null);
        execute(resultOut);
    }

    public synchronized void commit() throws HsqlException {
        resultOut.setAsTransactionEndRequest(ResultConstants.COMMIT, null);
        execute(resultOut);
    }

    public synchronized void rollback() throws HsqlException {
        resultOut.setAsTransactionEndRequest(ResultConstants.ROLLBACK, null);
        execute(resultOut);
    }

    public synchronized void rollbackToSavepoint(String name)
    throws HsqlException {

        resultOut.setAsTransactionEndRequest(
            ResultConstants.SAVEPOINT_NAME_ROLLBACK, name);
        execute(resultOut);
    }

    public synchronized void savepoint(String name) throws HsqlException {

        Result result = Result.newSetSavepointRequest(name);

        execute(result);
    }

    public synchronized void releaseSavepoint(String name)
    throws HsqlException {

        resultOut.setAsTransactionEndRequest(
            ResultConstants.SAVEPOINT_NAME_RELEASE, name);
        execute(resultOut);
    }

    public synchronized long getId() {
        return sessionID;
    }

    /**
     * Used by pooled connections to reset the server-side session to a new
     * one. In case of failure, the connection is closed.
     *
     * When the Connection.close() method is called, a pooled connection calls
     * this method instead of HSQLClientConnection.close(). It can then
     * reuse the HSQLClientConnection object with no further initialisation.
     *
     */
    public synchronized void resetSession() throws HsqlException {

        Result login    = Result.newResetSessionRequest();
        Result resultIn = execute(login);

        if (resultIn.isError()) {
            isClosed = true;

            closeConnection();

            throw Trace.error(resultIn);
        }

        sessionID  = resultIn.getSessionId();
        databaseID = resultIn.getDatabaseId();
    }

    protected void write(Result r) throws IOException, HsqlException {
        r.write(dataOutput, rowOut);
    }

    protected Result read() throws IOException, HsqlException {

        Result result = Result.newResult(dataInput, rowIn);

        result.readAdditionalResults(this, dataInput, rowIn);
        rowOut.setBuffer(mainBuffer);
        rowIn.resetRow(mainBuffer.length);

        return result;
    }

    /**
     * Never called on this class
     */
    public String getInternalConnectionURL() {
        return null;
    }

    public synchronized long getLobId() {
        return lobIDSequence++;
    }

    /**
     * Does nothing here
     */
    public void allocateResultLob(ResultLob resultLob, DataInput dataInput) {}
}
