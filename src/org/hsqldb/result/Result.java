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


package org.hsqldb.result;

import java.io.DataInput;
import java.io.IOException;

import org.hsqldb.CompiledStatement;
import org.hsqldb.HsqlException;
import org.hsqldb.Session;
import org.hsqldb.SessionInterface;
import org.hsqldb.Trace;
import org.hsqldb.lib.DataOutputStream;
import org.hsqldb.navigator.ClientRowSetNavigator;
import org.hsqldb.navigator.LinkedListRowSetNavigator;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.rowio.RowInputBinary;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.BooleanType;
import org.hsqldb.types.CharacterType;
import org.hsqldb.types.NumberType;
import org.hsqldb.types.Type;

/**
 *  The primary unit of communication between Connection, Server and Session
 *  objects.
 *
 *  An HSQLDB Result object encapsulates all requests (such as to alter or
 *  query session settings, to allocate and execute statements, etc.) and all
 *  responses (such as exception indications, update counts, result sets and
 *  result set metadata). It also implements the HSQL wire protocol for
 *  comunicating all such requests and responses across the network.
 *
 *  Originally a class in Hypersonic, rewritten in successive
 *  versions of HSQLDB. Uses a navigator for data since 1.9.0.
 *
 * @author fredt@users, boucherb@users
 * @version 1.9.0
 */
public class Result {

    public static final Result updateZeroResult =
        newResult(ResultConstants.UPDATECOUNT);
    public static final Result updateOneResult =
        newResult(ResultConstants.UPDATECOUNT);

    static {
        updateOneResult.setUpdateCount(1);
    }

    // type of result
    byte mode;

    // database ID
    int databaseID;

    // session ID
    long sessionID;

    // result id
    long id;

    // database name for new connection
    private String databaseName;

    // user / password for new connection or error strings
    private String mainString;
    private String subString;

    // vendor error code
    int errorCode;

    // the exception if this is an error
    private Throwable exception;

    // prepared statement id
    int statementID;

    // statement type based on whether it returns an update count or a result set
    int statementType;

    // max rows (out) or update count (in)
    private int updateCount;

    // fetch size (in)
    private int fetchSize;

    // secondary result
    private Result chainedResult;

    //
    private int lobCount;
    ResultLob   lobResults;

    // transient - number of significant data columns
    private int significantColumns;

    /** A Result object's metadata */
    public ResultMetaData metaData;

    /** Additional meta data for parameters used in PREPARE_ACK results */
    public ResultMetaData parameterMetaData;

    /** Additional meta data for required generated columns */
    public ResultMetaData generatedMetaData;

    //
    public int rsScrollability;
    public int rsConcurrency;
    public int rsHoldability;

    //
    int generateKeys;

    public static Result newResult(RowSetNavigator nav) {

        Result result = new Result();

        result.mode      = ResultConstants.DATA;
        result.navigator = nav;

        return result;
    }

    private static Result newResult(int type) {

        RowSetNavigator navigator = null;
        Result          result    = null;

        switch (type) {

            case ResultConstants.EXECUTE :
            case ResultConstants.BATCHEXECUTE :
            case ResultConstants.BATCHEXECDIRECT :
            case ResultConstants.SETSESSIONATTR :
                navigator = new LinkedListRowSetNavigator();
                break;

            case ResultConstants.PARAM_METADATA :
                navigator = new LinkedListRowSetNavigator();
                break;

            case ResultConstants.BATCHEXECRESPONSE :
                navigator = new ClientRowSetNavigator();
                break;

            case ResultConstants.DATA :
            case ResultConstants.DATAHEAD :
            case ResultConstants.DATAROWS :
                break;

            case ResultConstants.LARGE_OBJECT_OP :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Result");
            default :
        }

        result           = new Result();
        result.mode      = (byte) type;
        result.navigator = navigator;

        return result;
    }

    public static Result newResult(DataInput dataInput,
                                   RowInputBinary in)
                                   throws IOException, HsqlException {

        try {
            int mode = dataInput.readByte();

            if (mode == ResultConstants.LARGE_OBJECT_OP) {
                return ResultLob.newLob(dataInput, false);
            }

            Result result = newResult(dataInput, in, mode);

            return result;
        } catch (IOException e) {
            throw Trace.error(Trace.TRANSFER_CORRUPTED);
        }
    }

    public void readAdditionalResults(SessionInterface session,
                                      DataInput dataInput,
                                      RowInputBinary in)
                                      throws IOException, HsqlException {

        setSession(session);

        Result  currentResult = this;
        boolean hasLob        = false;

        while (true) {
            int addedResultMode = dataInput.readByte();

            if (addedResultMode == ResultConstants.LARGE_OBJECT_OP) {
                ResultLob resultLob = ResultLob.newLob(dataInput, false);

                session.allocateResultLob(resultLob, dataInput);
                currentResult.addLobResult(resultLob);

                hasLob = true;

                continue;
            }

            if (hasLob) {
                hasLob = false;

                if (session instanceof Session) {
                    ((Session) session).registerResultLobs(currentResult);
                }
            }

            if (addedResultMode == ResultConstants.NONE) {
                return;
            }

            currentResult = newResult(dataInput, in, addedResultMode);

            addChainedResult(currentResult);
        }
    }

    private static Result newResult(DataInput dataInput, RowInputBinary in,
                                    int mode)
                                    throws IOException, HsqlException {

        Result result = newResult(mode);
        int    length = dataInput.readInt();

        in.resetRow(0, length);

        byte[] byteArray = in.getBuffer();
        int    offset    = 4;

        dataInput.readFully(byteArray, offset, length - offset);

        switch (mode) {

            case ResultConstants.GETSESSIONATTR :
            case ResultConstants.DISCONNECT :
            case ResultConstants.RESETSESSION :
            case ResultConstants.STARTTRAN :
                break;

            case ResultConstants.PREPARE :
                result.setStatementType(in.readInt());

                result.mainString      = in.readString();
                result.rsScrollability = in.readShort();
                result.rsConcurrency   = in.readShort();
                result.rsHoldability   = in.readShort();
                result.generateKeys    = in.readByte();

                if (result.generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_NAMES || result
                        .generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_INDEXES) {
                    result.generatedMetaData = new ResultMetaData(in);
                }
                break;

            case ResultConstants.CLOSE_RESULT :
                result.id = in.readLong();
                break;

            case ResultConstants.FREESTMT :
                result.statementID = in.readInt();
                break;

            case ResultConstants.EXECDIRECT :
                result.updateCount     = in.readInt();
                result.fetchSize       = in.readInt();
                result.statementType   = in.readByte();
                result.mainString      = in.readString();
                result.rsScrollability = in.readShort();
                result.rsConcurrency   = in.readShort();
                result.rsHoldability   = in.readShort();
                result.generateKeys    = in.readByte();

                if (result.generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_NAMES || result
                        .generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_INDEXES) {
                    result.generatedMetaData = new ResultMetaData(in);
                }
                break;

            case ResultConstants.CONNECT :
                result.databaseName = in.readString();
                result.mainString   = in.readString();
                result.subString    = in.readString();
                break;

            case ResultConstants.ERROR :
                result.mainString = in.readString();
                result.subString  = in.readString();
                result.errorCode  = in.readInt();
                break;

            case ResultConstants.CONNECTACKNOWLEDGE :
                result.databaseID = in.readInt();
                result.sessionID  = in.readLong();
                break;

            case ResultConstants.UPDATECOUNT :
                result.updateCount = in.readInt();
                break;

            case ResultConstants.ENDTRAN : {
                int type = in.readInt();

                result.setEndTranType(type);                    // endtran type

                switch (type) {

                    case ResultConstants.SAVEPOINT_NAME_RELEASE :
                    case ResultConstants.SAVEPOINT_NAME_ROLLBACK :
                        result.mainString = in.readString();    // savepoint name
                }

                break;
            }
            case ResultConstants.SETCONNECTATTR : {
                int type = in.readInt();                        // attr type

                result.setConnectionAttrType(type);

                switch (type) {

                    case ResultConstants.SQL_ATTR_SAVEPOINT_NAME :
                        result.mainString = in.readString();    // savepoint name

                    //  case ResultConstants.SQL_ATTR_AUTO_IPD :
                    //      - always true
                    //  default: throw - case never happens
                }

                break;
            }
            case ResultConstants.PREPARE_ACK :
                result.statementType     = in.readByte();
                result.statementID       = in.readInt();
                result.metaData          = new ResultMetaData(in);
                result.parameterMetaData = new ResultMetaData(in);
                break;

            case ResultConstants.EXECUTE :
                result.updateCount        = in.readInt();
                result.fetchSize          = in.readInt();
                result.statementID        = in.readInt();
                result.rsScrollability    = in.readShort();
                result.rsConcurrency      = in.readShort();
                result.rsHoldability      = in.readShort();
                result.metaData           = new ResultMetaData(in);
                result.significantColumns = result.metaData.getColumnCount();

                result.navigator.read(in, result.metaData);
                break;

            case ResultConstants.BATCHEXECRESPONSE :
            case ResultConstants.BATCHEXECUTE :
            case ResultConstants.BATCHEXECDIRECT :
            case ResultConstants.SETSESSIONATTR : {
                result.updateCount        = in.readInt();
                result.fetchSize          = in.readInt();
                result.statementID        = in.readInt();
                result.metaData           = new ResultMetaData(in);
                result.significantColumns = result.metaData.getColumnCount();

                result.navigator.read(in, result.metaData);

                break;
            }
            case ResultConstants.PARAM_METADATA : {
                result.metaData           = new ResultMetaData(in);
                result.significantColumns = result.metaData.colLabels.length;

                result.navigator.read(in, result.metaData);

                break;
            }
            case ResultConstants.REQUESTDATA : {
                result.id          = in.readLong();
                result.updateCount = in.readInt();
                result.fetchSize   = in.readInt();

                break;
            }
            case ResultConstants.DATA : {
                result.updateCount        = in.readInt();
                result.fetchSize          = in.readInt();
                result.rsScrollability    = in.readShort();
                result.rsConcurrency      = in.readShort();
                result.rsHoldability      = in.readShort();
                result.metaData           = new ResultMetaData(in);
                result.significantColumns = result.metaData.getColumnCount();
                result.navigator          = new ClientRowSetNavigator();

                result.navigator.read(in, result.metaData);

                break;
            }
            case ResultConstants.DATAHEAD : {
                result.updateCount        = in.readInt();
                result.fetchSize          = in.readInt();
                result.rsScrollability    = in.readShort();
                result.rsConcurrency      = in.readShort();
                result.rsHoldability      = in.readShort();
                result.metaData           = new ResultMetaData(in);
                result.significantColumns = result.metaData.getColumnCount();
                result.navigator          = new ClientRowSetNavigator();

                result.navigator.read(in, result.metaData);

                break;
            }
            case ResultConstants.DATAROWS : {
                result.metaData           = new ResultMetaData(in);
                result.significantColumns = result.metaData.getColumnCount();
                result.navigator          = new ClientRowSetNavigator();

                result.navigator.read(in, result.metaData);

                break;
            }
            default :
                throw new HsqlException(
                    Trace.getMessage(
                        Trace.Result_Result, true, new Object[]{
                            new Integer(mode) }), null, 0);
        }

        return result;
    }

    /**
     * For BATCHEXECUTE, BATCHEXECDIRECT
     */
    public static Result newBatchExecuteResult(int type, Type[] types,
            int id) {

        Result result = newResult(type);

        result.metaData = ResultMetaData.newSimpleResultMetaData(types);
        result.significantColumns = types.length;
        result.statementID        = id;

        return result;
    }

    /**
     * For SQLPREPARE
     * For parparation of SQL parepared statements.
     */
    public static Result newPrepareStatementRequest() {
        return newResult(ResultConstants.PREPARE);
    }

    public static Result newPrepareStatementRequest(String sql,
            int resultSetType, int resultSetConcurrency,
            int resultSetHoldability, int generatedKeys,
            int[] generatedIndexes, String[] generatedNames) {

        Result result = newResult(ResultConstants.PREPARE);

        result.mainString      = sql;
        result.rsScrollability = resultSetType;
        result.rsConcurrency   = resultSetConcurrency;
        result.rsHoldability   = resultSetHoldability;
        result.generateKeys    = generatedKeys;
        result.generatedMetaData =
            ResultMetaData.newGeneratedColumnsMetaData(generatedIndexes,
                generatedNames);

        return result;
    }

    /**
     * For SQLEXECUTE
     * For execution of SQL prepared statements.
     * The parameters are set afterwards as the Result is reused
     */
    public static Result newPreparedExecuteRequest(Type[] types, int id) {

        Result result = newResult(ResultConstants.EXECUTE);

        result.metaData = ResultMetaData.newSimpleResultMetaData(types);
        result.significantColumns = types.length;
        result.statementID        = id;

        return result;
    }

    /**
     * For SQLEXECUTE results
     * The parameters are set by this method as the Result is reused
     */
    public void setPreparedExecuteProperties(Object[] parameterValues,
            int maxRows, int fetchSize) {

        navigator.clear();
        navigator.add(parameterValues);

        updateCount    = maxRows;
        this.fetchSize = fetchSize;
    }

    /**
     * For BATCHEXECUTE
     */
    public static Result newBatchedPreparedExecuteRequest(Type[] types,
            int id) {

        Result result = newResult(ResultConstants.BATCHEXECUTE);

        result.metaData = ResultMetaData.newSimpleResultMetaData(types);
        result.significantColumns = types.length;
        result.statementID        = id;

        return result;
    }

    /**
     * For BATCHEXECDIRECT
     */
    public static Result newBatchedExecuteRequest() {

        Type[] types  = new Type[]{ Type.SQL_VARCHAR };
        Result result = newResult(ResultConstants.BATCHEXECDIRECT);

        result.metaData = ResultMetaData.newSimpleResultMetaData(types);
        result.significantColumns = result.metaData.getColumnCount();

        return result;
    }

    /**
     * For BATCHEXERESPONSE for a BATCHEXECUTE or BATCHEXECDIRECT
     */
    public static Result newBatchedExecuteResponse(int[] updateCounts,
            Result generatedResult, Result e) {

        Result result = newResult(ResultConstants.BATCHEXECRESPONSE);

        result.addChainedResult(generatedResult);
        result.addChainedResult(e);

        Type[] types = new Type[]{ Type.SQL_INTEGER };

        result.metaData = ResultMetaData.newSimpleResultMetaData(types);
        result.significantColumns = result.metaData.getColumnCount();

        Object[][] table = new Object[updateCounts.length][];

        for (int i = 0; i < updateCounts.length; i++) {
            table[i] = new Object[]{ ValuePool.getInt(updateCounts[i]) };
        }

        ((ClientRowSetNavigator) result.navigator).setData(table);

        return result;
    }

    public static Result newResetSessionRequest() {

        Result result = newResult(ResultConstants.RESETSESSION);

        return result;
    }

    public static Result newConnectionAttemptRequest(String user,
            String password, String database) {

        Result result = newResult(ResultConstants.CONNECT);

        result.mainString   = user;
        result.subString    = password;
        result.databaseName = database;

        return result;
    }

    public static Result newConnectionAcknowledgeResponse(long sessionID,
            int databaseID) {

        Result result = newResult(ResultConstants.CONNECTACKNOWLEDGE);

        result.sessionID  = sessionID;
        result.databaseID = databaseID;

        return result;
    }

    public static Result newUpdateCountResult(int count) {

        switch (count) {

            case 0 :
                return Result.updateZeroResult;

            case 1 :
                return Result.updateOneResult;

            default :
        }

        Result result = newResult(ResultConstants.UPDATECOUNT);

        result.updateCount = count;

        return result;
    }

    public static Result newUpdateCountResult(ResultMetaData meta, int count) {

        Result result     = newResult(ResultConstants.UPDATECOUNT);
        Result dataResult = newDataResult(meta);

        result.updateCount = count;

        result.addChainedResult(dataResult);

        return result;
    }

    public static Result newSingleColumnResult(String colName, Type type) {

        Result result = newResult(ResultConstants.DATA);

        result.navigator              = new LinkedListRowSetNavigator();
        result.metaData               = ResultMetaData.newResultMetaData(1);
        result.significantColumns     = 1;
        result.metaData.colNames[0]   = colName;
        result.metaData.colLabels[0]  = colName;
        result.metaData.tableNames[0] = "";
        result.metaData.colTypes[0]   = type;

        return result;
    }

    public static Result newPrepareResponse(int csID, int csType,
            ResultMetaData rsmd, ResultMetaData pmd) {

        Result r = newResult(ResultConstants.PREPARE_ACK);

        r.statementID = csID;
        r.statementType =
            (csType == CompiledStatement.SELECT || csType == CompiledStatement
                .CALL) ? ResultConstants.RETURN_RESULT
                       : ResultConstants.RETURN_COUNT;
        r.metaData          = rsmd;
        r.parameterMetaData = pmd;

        return r;
    }

    public static Result newFreeStmtRequest(int statementID) {

        Result r = newResult(ResultConstants.FREESTMT);

        r.statementID = statementID;

        return r;
    }

    /**
     * For direct execution of SQL statements. The statement and other
     *  parameters are set afterwards as the Result is reused
     */
    public static Result newExecuteDirectRequest() {
        return newResult(ResultConstants.EXECDIRECT);
    }

    /**
     * For both EXECDIRECT and PREPARE
     */
    public void setPrepareOrExecuteProperties(String sql, int maxRows,
            int fetchSize, int statementRetType, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability, int keyMode,
            int[] generatedIndexes, String[] generatedNames) {

        mainString      = sql;
        updateCount     = maxRows;
        this.fetchSize  = fetchSize;
        statementType   = statementRetType;
        rsScrollability = resultSetType;
        rsConcurrency   = resultSetConcurrency;
        rsHoldability   = resultSetHoldability;
        generateKeys    = keyMode;
        generatedMetaData =
            ResultMetaData.newGeneratedColumnsMetaData(generatedIndexes,
                generatedNames);
    }

    public static Result newReleaseSavepointRequest(String name) {

        Result result;

        result = newResult(ResultConstants.ENDTRAN);

        result.setMainString(name);
        result.setEndTranType(ResultConstants.SAVEPOINT_NAME_RELEASE);

        return result;
    }

    public static Result newRollbackToSavepointRequest(String name) {

        Result result;

        result = newResult(ResultConstants.ENDTRAN);

        result.setMainString(name);
        result.setEndTranType(ResultConstants.SAVEPOINT_NAME_ROLLBACK);

        return result;
    }

    public static Result newSetSavepointRequest(String name) {

        Result result;

        result = newResult(ResultConstants.SETCONNECTATTR);

        result.setConnectionAttrType(ResultConstants.SQL_ATTR_SAVEPOINT_NAME);
        result.setMainString(name);

        return result;
    }

    public static Result newErrorResult(String error, String state, int code) {

        Result result = newResult(ResultConstants.ERROR);

        result.mainString = error;
        result.subString  = state;
        result.errorCode  = code;

        return result;
    }

    public static Result newRequestDataResult(long id, int offset, int count) {

        Result result = newResult(ResultConstants.REQUESTDATA);

        result.id          = id;
        result.updateCount = offset;
        result.fetchSize   = count;

        return result;
    }

    public static Result newDataResult(ResultMetaData md) {

        Result result = newResult(ResultConstants.DATA);

        result.navigator          = new LinkedListRowSetNavigator();
        result.significantColumns = md.getColumnCount();
        result.metaData           = md;

        return result;
    }

    /**
     * For DATA
     */
    public void setDataResultProperties(int maxRows, int fetchSize,
                                        int resultSetType,
                                        int resultSetConcurrency,
                                        int resultSetHoldability) {

        updateCount     = maxRows;
        this.fetchSize  = fetchSize;
        rsScrollability = resultSetType;
        rsConcurrency   = resultSetConcurrency;
        rsHoldability   = resultSetHoldability;
    }

    public static Result newDataHeadResult(SessionInterface session,
                                           Result source, int offset,
                                           int count) {

        if (offset + count > source.navigator.getSize()) {
            count = source.navigator.getSize() - offset;
        }

        Result result = newResult(ResultConstants.DATAHEAD);

        result.significantColumns = source.significantColumns;
        result.metaData           = source.metaData;
        result.navigator = new ClientRowSetNavigator(source.navigator, offset,
                count);

        result.navigator.setId(source.navigator.getId());
        result.setSession(session);

        return result;
    }

    public static Result newDataRowsResult(Result source, int offset,
                                           int count) {

        if (offset + count > source.navigator.getSize()) {
            count = source.navigator.getSize() - offset;
        }

        Result result = newResult(ResultConstants.DATAROWS);

        result.id                 = source.id;
        result.significantColumns = source.significantColumns;
        result.metaData           = source.metaData;
        result.navigator = new ClientRowSetNavigator(source.navigator, offset,
                count);

        return result;
    }

    public static Result newDataRowsResult(RowSetNavigator navigator) {

        Result result = newResult(ResultConstants.DATAROWS);

        result.navigator = navigator;

        return result;
    }

    /**
     * Result structure used for set/get session attributes
     */
    public static Result newSessionAttributesResult() {

        Result result = newResult(ResultConstants.DATA);

        result.navigator          = new LinkedListRowSetNavigator();
        result.metaData           = ResultMetaData.newResultMetaData(7);
        result.significantColumns = 7;
        result.metaData.colNames = result.metaData.colLabels =
            result.metaData.tableNames = new String[] {
            "", "", "", "", "", "", ""
        };
        result.metaData.colTypes = new Type[] {
            CharacterType.SQL_VARCHAR, CharacterType.SQL_VARCHAR,
            NumberType.SQL_BIGINT, NumberType.SQL_INTEGER,
            BooleanType.SQL_BOOLEAN, BooleanType.SQL_BOOLEAN,
            BooleanType.SQL_BOOLEAN
        };

        return result;
    }

    /** @todo fredt - move the messages to Trace.java */
    public static Result newErrorResult(Throwable t, String statement) {

        Result result = newResult(ResultConstants.ERROR);

        result.exception = t;

        if (t instanceof HsqlException) {
            HsqlException he = (HsqlException) t;

            result.mainString = he.getMessage();
            result.subString  = he.getSQLState();

            if (statement != null) {
                result.mainString += " in statement [" + statement + "]";
            }

            result.errorCode = he.getErrorCode();
        } else if (t instanceof OutOfMemoryError) {

            // At this point, we've nothing to lose by doing this
            System.gc();
            t.printStackTrace();

            result.mainString = "out of memory";
            result.subString  = "S1000";
            result.errorCode  = Trace.OUT_OF_MEMORY;
        } else {
            t.printStackTrace();

            result.mainString = Trace.getMessage(Trace.GENERAL_ERROR) + " "
                                + t;
            result.subString = "S1000";

            if (statement != null) {
                result.mainString += " in statement [" + statement + "]";
            }

            result.errorCode = Trace.GENERAL_ERROR;
        }

        return result;
    }

    /**
     *  Method declaration
     *
     * @param  columns
     */
    public void setColumnCount(int columns) {

        significantColumns = columns;

        metaData.setColumnCount(significantColumns);
    }

    /**
     *  Method declaration
     *
     * @return
     */
    public int getColumnCount() {
        return significantColumns;
    }

    public void write(DataOutputStream dataOut,
                      RowOutputInterface rowOut)
                      throws IOException, HsqlException {

        rowOut.reset();
        rowOut.writeByte(mode);

        int startPos = rowOut.size();

        rowOut.writeSize(0);

        switch (mode) {

            case ResultConstants.GETSESSIONATTR :
            case ResultConstants.DISCONNECT :
            case ResultConstants.RESETSESSION :
            case ResultConstants.STARTTRAN :
                break;

            case ResultConstants.PREPARE :
                rowOut.writeInt(statementID);
                rowOut.writeString(mainString);
                rowOut.writeShort(rsScrollability);
                rowOut.writeShort(rsConcurrency);
                rowOut.writeShort(rsHoldability);
                rowOut.writeByte(generateKeys);

                if (generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_NAMES || generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_INDEXES) {
                    generatedMetaData.write(rowOut);
                }
                break;

            case ResultConstants.FREESTMT :
                rowOut.writeInt(statementID);
                break;

            case ResultConstants.CLOSE_RESULT :
                rowOut.writeLong(id);
                break;

            case ResultConstants.EXECDIRECT :
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeByte(statementType);           // currently unused
                rowOut.writeString(mainString);
                rowOut.writeShort(rsScrollability);
                rowOut.writeShort(rsConcurrency);
                rowOut.writeShort(rsHoldability);
                rowOut.writeByte(generateKeys);

                if (generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_NAMES || generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_INDEXES) {
                    generatedMetaData.write(rowOut);
                }
                break;

            case ResultConstants.CONNECT :
                rowOut.writeString(databaseName);
                rowOut.writeString(mainString);
                rowOut.writeString(subString);
                break;

            case ResultConstants.ERROR :
                rowOut.writeString(mainString);
                rowOut.writeString(subString);
                rowOut.writeInt(errorCode);
                break;

            case ResultConstants.CONNECTACKNOWLEDGE :
                rowOut.writeInt(databaseID);
                rowOut.writeLong(sessionID);
                break;

            case ResultConstants.UPDATECOUNT :
                rowOut.writeInt(updateCount);
                break;

            case ResultConstants.ENDTRAN : {
                int type = getEndTranType();

                rowOut.writeInt(type);                     // endtran type

                switch (type) {

                    case ResultConstants.SAVEPOINT_NAME_RELEASE :
                    case ResultConstants.SAVEPOINT_NAME_ROLLBACK :
                        rowOut.writeString(mainString);    // savepoint name
                    default :

                    // do nothing
                }

                break;
            }
            case ResultConstants.PREPARE_ACK :
                rowOut.writeByte(statementType);
                rowOut.writeInt(statementID);
                metaData.write(rowOut);
                parameterMetaData.write(rowOut);
                break;

            case ResultConstants.EXECUTE :
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeInt(statementID);
                rowOut.writeShort(rsScrollability);
                rowOut.writeShort(rsConcurrency);
                rowOut.writeShort(rsHoldability);
                metaData.write(rowOut);
                navigator.write(rowOut, metaData);
                break;

            case ResultConstants.BATCHEXECRESPONSE :
            case ResultConstants.BATCHEXECUTE :
            case ResultConstants.BATCHEXECDIRECT :
            case ResultConstants.SETSESSIONATTR : {
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeInt(statementID);
                metaData.write(rowOut);
                navigator.write(rowOut, metaData);

                break;
            }
            case ResultConstants.PARAM_METADATA : {
                metaData.write(rowOut);
                navigator.write(rowOut, metaData);

                break;
            }
            case ResultConstants.SETCONNECTATTR : {
                int type = getConnectionAttrType();

                rowOut.writeInt(type);                     // attr type

                switch (type) {

                    case ResultConstants.SQL_ATTR_SAVEPOINT_NAME :
                        rowOut.writeString(mainString);    // savepoint name

                    // case ResultConstants.SQL_ATTR_AUTO_IPD // always true
                    // default: // throw, but case never happens
                }

                break;
            }
            case ResultConstants.REQUESTDATA : {
                rowOut.writeLong(id);
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);

                break;
            }
            case ResultConstants.DATAHEAD :
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeShort(rsScrollability);
                rowOut.writeShort(rsConcurrency);
                rowOut.writeShort(rsHoldability);
                metaData.write(rowOut, significantColumns);
                navigator.write(rowOut, metaData);
                break;

            case ResultConstants.DATAROWS :
                metaData.write(rowOut, significantColumns);
                navigator.write(rowOut, metaData);
                break;

            case ResultConstants.DATA :
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeShort(rsScrollability);
                rowOut.writeShort(rsConcurrency);
                rowOut.writeShort(rsHoldability);
                metaData.write(rowOut, significantColumns);
                navigator.write(rowOut, metaData);
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Result");
        }

        rowOut.writeIntData(rowOut.size() - startPos, startPos);
        dataOut.write(rowOut.getOutputStream().getBuffer(), 0, rowOut.size());

        int    count   = getLobCount();
        Result current = this;

        for (int i = 0; i < count; i++) {
            ResultLob lob = current.lobResults;

            lob.writeBody(dataOut);

            current = current.lobResults;
        }

        if (chainedResult == null) {
            dataOut.writeByte(ResultConstants.NONE);
        } else {
            chainedResult.write(dataOut, rowOut);
        }

        dataOut.flush();
    }

    public int getType() {
        return mode;
    }

    public boolean isData() {
        return mode == ResultConstants.DATA
               || mode == ResultConstants.DATAHEAD;
    }

    public boolean isError() {
        return mode == ResultConstants.ERROR;
    }

    public boolean isUpdateCount() {
        return mode == ResultConstants.UPDATECOUNT;
    }

    public boolean hasGeneratedKeys() {
        return mode == ResultConstants.UPDATECOUNT && chainedResult != null;
    }

    public Throwable getException() {
        return exception;
    }

    public int getStatementID() {
        return statementID;
    }

    public void setStatementID(int id) {
        statementID = id;
    }

    public String getMainString() {
        return mainString;
    }

    public void setMainString(String sql) {
        mainString = sql;
    }

    public String getSubString() {
        return subString;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setMaxRows(int count) {
        updateCount = count;
    }

    public int getFetchSize() {
        return this.fetchSize;
    }

    public void setFetchSize(int count) {
        fetchSize = count;
    }

    public int getUpdateCount() {
        return updateCount;
    }

    public int getConnectionAttrType() {
        return updateCount;
    }

    public void setConnectionAttrType(int type) {
        updateCount = type;
    }

    public int getEndTranType() {
        return updateCount;
    }

    public void setEndTranType(int type) {
        updateCount = type;
    }

    public long getSessionId() {
        return sessionID;
    }

    public void setSessionId(long id) {
        sessionID = id;
    }

    public void setSession(SessionInterface session) {

        if (navigator != null) {
            navigator.setSession(session);
        }
    }

    public int getDatabaseId() {
        return databaseID;
    }

    public void setDatabaseId(int id) {
        databaseID = id;
    }

    public long getResultId() {
        return id;
    }

    public void setResultId(long id) {

        this.id = id;

        if (navigator != null) {
            navigator.setId(id);
        }
    }

    public void setUpdateCount(int count) {
        updateCount = count;
    }

    public void setAsTransactionEndRequest(int subType, String savepoint) {

        mode        = ResultConstants.ENDTRAN;
        updateCount = subType;
        mainString  = savepoint == null ? ""
                                        : savepoint;
    }

    public Object[] getSingleRowData() {
        return (Object[]) initialiseNavigator().getNext();
    }

    public Object[] getParameterData() {
        return (Object[]) initialiseNavigator().getNext();
    }

    public void setSessionAttributes(Object[] data) {
        navigator.add(data);
    }

    public Object[] getSessionAttributes() {
        return (Object[]) initialiseNavigator().getNext();
    }

    public void setResultType(int type) {
        mode = (byte) type;
    }

    public void setStatementType(int type) {
        statementType = type;
    }

    public int getStatementType() {
        return statementType;
    }

    public int getGeneratedResultType() {
        return generateKeys;
    }

    public ResultMetaData getGeneratedResultMetaData() {
        return generatedMetaData;
    }

    public Result getChainedResult() {
        return chainedResult;
    }

    public Result getUnlinkChainedResult() {

        Result result = chainedResult;

        chainedResult = null;

        return result;
    }

    public void addChainedResult(Result result) {

        Result current = this;

        while (current.chainedResult != null) {
            current = current.chainedResult;
        }

        current.chainedResult = result;
    }

    public int getLobCount() {
        return lobCount;
    }

    public ResultLob getLOBResult() {
        return lobResults;
    }

    public void addLobResult(ResultLob result) {

        Result current = this;

        while (current.lobResults != null) {
            current = current.lobResults;
        }

        current.lobResults = result;

        lobCount++;
    }

    public void clearLobResults() {
        lobResults = null;
        lobCount   = 0;
    }

//----------- Navigation
    RowSetNavigator navigator;

    public RowSetNavigator getNavigator() {
        return navigator;
    }

    public RowSetNavigator initialiseNavigator() {

        switch (mode) {

            case ResultConstants.BATCHEXECUTE :
            case ResultConstants.BATCHEXECDIRECT :
            case ResultConstants.BATCHEXECRESPONSE :
            case ResultConstants.EXECUTE :
            case ResultConstants.SETSESSIONATTR :
            case ResultConstants.PARAM_METADATA :
                navigator.beforeFirst();

                return navigator;

            case ResultConstants.DATA :
            case ResultConstants.DATAHEAD :
                navigator.reset();

                return navigator;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Result");
        }
    }
}
