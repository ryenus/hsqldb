/* Copyright (c) 2001-2025, The HSQL Development Group
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
import java.io.DataInputStream;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;

import org.hsqldb.ColumnBase;
import org.hsqldb.Session;
import org.hsqldb.SessionInterface;
import org.hsqldb.SessionInterface.AttributePos;
import org.hsqldb.SqlInvariants;
import org.hsqldb.Statement;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.error.HsqlException;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.DataOutputStream;
import org.hsqldb.lib.List;
import org.hsqldb.map.ValuePool;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.navigator.RowSetNavigatorClient;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.types.Charset;
import org.hsqldb.types.Collation;
import org.hsqldb.types.Type;

/**
 *  The primary unit of communication between Connection, Server and Session
 *  objects.
 *
 *  An HSQLDB Result object encapsulates all requests (such as to alter or
 *  query session settings, to allocate and execute statements, etc.) and all
 *  responses (such as exception indications, update counts, result sets and
 *  result set metadata). It also implements the HSQL wire protocol for
 *  communicating all such requests and responses across the network.
 *  Uses a navigator for data.
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.5
 * @since 1.9.0
 */
public class Result {

    public static final Result[]       emptyArray = new Result[0];
    public static final ResultMetaData sessionAttributesMetaData;

    static {

        // calls to initialise some global statics
        SqlInvariants.isSystemSchemaName(SqlInvariants.SYSTEM_SCHEMA);
        Charset.getDefaultInstance();
        Collation.getDefaultInstance();

        sessionAttributesMetaData = ResultMetaData.newResultMetaData(
            AttributePos.INFO_LIMIT);

        for (int i = 0; i < AttributePos.INFO_LIMIT; i++) {
            sessionAttributesMetaData.columns[i] = new ColumnBase(
                null,
                null,
                null,
                null);
        }

        sessionAttributesMetaData.columns[AttributePos.INFO_ID].setType(
            Type.SQL_INTEGER);
        sessionAttributesMetaData.columns[AttributePos.INFO_INTEGER].setType(
            Type.SQL_INTEGER);
        sessionAttributesMetaData.columns[AttributePos.INFO_BOOLEAN].setType(
            Type.SQL_BOOLEAN);
        sessionAttributesMetaData.columns[AttributePos.INFO_VARCHAR].setType(
            Type.SQL_VARCHAR);
        sessionAttributesMetaData.prepareData();
    }

    private static final ResultMetaData emptyMeta =
        ResultMetaData.newResultMetaData(
            0);
    public static final Result updateZeroResult = newUpdateCountResult(0);
    public static final Result updateOneResult  = newUpdateCountResult(1);

    // type of result
    public byte mode;

    // database ID
    int databaseID;

    // session ID
    long sessionID;

    // result id
    private long id;

    // database name for new connection
    private String databaseName;

    // user / password for new connection
    // error strings in error results
    private String mainString;
    private String subString;
    private String zoneString;

    // vendor error code
    int errorCode;

    // the exception if this is an error
    private HsqlException exception;

    // prepared statement id
    long statementID;

    // statement type based on whether it returns an update count or a result set
    // type of session info requested
    int statementReturnType;

    // max rows (out)
    // update count (in)
    // fetch part result count (in)
    // time zone seconds (connect)
    public int updateCount;

    // fetch size (in)
    private int fetchSize;

    // secondary result
    private Result chainedResult;

    //
    private int lobCount;
    ResultLob   lobResults;

    /** A Result object's metadata */
    public ResultMetaData metaData;

    /** Additional meta data for parameters used in PREPARE_ACK results */
    public ResultMetaData parameterMetaData;

    /** Additional meta data for required generated columns */
    public ResultMetaData generatedMetaData;

    //
    public int rsProperties;

    //
    public int queryTimeout;

    //
    int generateKeys;

    // simple value for PSM, or parameter array
    public Object valueData;

    //
    public Statement statement;

    Result(int mode) {
        this.mode = (byte) mode;
    }

    public Result(int mode, int count) {
        this.mode   = (byte) mode;
        updateCount = count;
    }

    public static Result newResult(RowSetNavigator nav) {

        Result result = new Result(ResultConstants.DATA);

        result.navigator = nav;

        return result;
    }

    public static Result newResult(int type) {

        RowSetNavigator navigator = null;
        Result          result;

        switch (type) {

            case ResultConstants.CALL_RESPONSE :
            case ResultConstants.EXECUTE :
            case ResultConstants.UPDATE_RESULT :
                break;

            case ResultConstants.BATCHEXECUTE :
            case ResultConstants.BATCHEXECDIRECT :
                navigator = new RowSetNavigatorClient(4);
                break;

            case ResultConstants.SETSESSIONATTR :
            case ResultConstants.PARAM_METADATA :
                navigator = new RowSetNavigatorClient(1);
                break;

            case ResultConstants.BATCHEXECRESPONSE :
                navigator = new RowSetNavigatorClient(4);
                break;

            case ResultConstants.DATA :
            case ResultConstants.DATAHEAD :
            case ResultConstants.DATAROWS :
            case ResultConstants.GENERATED :
                break;

            case ResultConstants.LARGE_OBJECT_OP :
                throw Error.runtimeError(ErrorCode.U_S0500, "Result");

            default :
        }

        result           = new Result(type);
        result.navigator = navigator;

        return result;
    }

    public static Result newResult(
            DataInputStream dataInput,
            RowInputInterface in)
            throws IOException {
        int readMode = dataInput.readByte();

        return newResult(null, readMode, dataInput, in);
    }

    public static Result newResult(
            Session session,
            int mode,
            DataInputStream dataInput,
            RowInputInterface in) {

        try {
            if (mode == ResultConstants.LARGE_OBJECT_OP) {
                return ResultLob.newLob(dataInput, false);
            }

            Result result = newResult(session, dataInput, in, mode);

            return result;
        } catch (IOException e) {
            throw Error.error(ErrorCode.X_08000, e);
        }
    }

    public void readAdditionalResults(
            SessionInterface session,
            DataInputStream inputStream,
            RowInputInterface in)
            throws IOException {

        setSession(session);

        while (true) {
            int addedResultMode = inputStream.readByte();

            if (addedResultMode == ResultConstants.NONE) {
                return;
            }

            Result currentResult = newResult(
                null,
                inputStream,
                in,
                addedResultMode);

            addChainedResult(currentResult);
        }
    }

    public Result readLobResults(
            Session session,
            DataInputStream inputStream)
            throws IOException {

        boolean hasLob = false;

        setSession(session);

        while (true) {
            int addedResultMode = inputStream.readByte();

            if (addedResultMode == ResultConstants.LARGE_OBJECT_OP) {
                ResultLob resultLob    = ResultLob.newLob(inputStream, false);
                Result    actionResult = session.allocateResultLob(resultLob);

                if (actionResult.isError()) {
                    return actionResult;
                }

                hasLob = true;
            } else if (addedResultMode == ResultConstants.NONE) {
                break;
            } else {
                throw Error.runtimeError(ErrorCode.U_S0500, "Result");
            }
        }

        if (hasLob) {
            session.registerResultLobs(this);
        }

        return Result.updateZeroResult;
    }

    private static Result newResult(
            Session session,
            DataInput dataInput,
            RowInputInterface in,
            int mode)
            throws IOException {

        Result result = newResult(mode);
        int    length = dataInput.readInt();

        in.resetRow(0, length);

        byte[]    byteArray = in.getBuffer();
        final int offset    = 4;

        dataInput.readFully(byteArray, offset, length - offset);

        switch (mode) {

            case ResultConstants.GETSESSIONATTR :
                result.statementReturnType = in.readByte();
                break;

            case ResultConstants.DISCONNECT :
            case ResultConstants.RESETSESSION :
            case ResultConstants.STARTTRAN :
                break;

            case ResultConstants.PREPARE :
                result.setStatementType(in.readByte());

                result.mainString   = in.readString();
                result.rsProperties = in.readByte();
                result.generateKeys = in.readByte();

                if (result.generateKeys
                        == ResultConstants.RETURN_GENERATED_KEYS_COL_NAMES
                        || result.generateKeys
                        == ResultConstants.RETURN_GENERATED_KEYS_COL_INDEXES) {
                    result.generatedMetaData = new ResultMetaData(in);
                }

                break;

            case ResultConstants.CLOSE_RESULT :
                result.id = in.readLong();
                break;

            case ResultConstants.FREESTMT :
                result.statementID = in.readLong();
                break;

            case ResultConstants.EXECDIRECT :
                result.updateCount         = in.readInt();
                result.fetchSize           = in.readInt();
                result.statementReturnType = in.readByte();
                result.mainString          = in.readString();
                result.rsProperties        = in.readByte();
                result.queryTimeout        = in.readShort();
                result.generateKeys        = in.readByte();

                if (result.generateKeys
                        == ResultConstants.RETURN_GENERATED_KEYS_COL_NAMES
                        || result.generateKeys
                        == ResultConstants.RETURN_GENERATED_KEYS_COL_INDEXES) {
                    result.generatedMetaData = new ResultMetaData(in);
                }

                break;

            case ResultConstants.CONNECT :
                result.databaseName = in.readString();
                result.mainString   = in.readString();
                result.subString    = in.readString();
                result.zoneString   = in.readString();
                result.updateCount  = in.readInt();
                break;

            case ResultConstants.ERROR :
            case ResultConstants.WARNING :
                result.mainString = in.readString();
                result.subString  = in.readString();
                result.errorCode  = in.readInt();
                break;

            case ResultConstants.CONNECTACKNOWLEDGE :
                result.databaseID   = in.readInt();
                result.sessionID    = in.readLong();
                result.databaseName = in.readString();
                result.mainString   = in.readString();
                result.generateKeys = in.readInt();
                break;

            case ResultConstants.UPDATECOUNT :
                result.updateCount = in.readInt();
                break;

            case ResultConstants.ENDTRAN : {
                int type = in.readInt();

                result.setActionType(type);                     // endtran type

                switch (type) {

                    case ResultConstants.TX_SAVEPOINT_NAME_RELEASE :
                    case ResultConstants.TX_SAVEPOINT_NAME_ROLLBACK :
                        result.mainString = in.readString();    // savepoint name
                        break;

                    case ResultConstants.TX_COMMIT :
                    case ResultConstants.TX_ROLLBACK :
                    case ResultConstants.TX_COMMIT_AND_CHAIN :
                    case ResultConstants.TX_ROLLBACK_AND_CHAIN :
                    case ResultConstants.PREPARECOMMIT :
                        break;

                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500, "Result");
                }

                break;
            }

            case ResultConstants.SETCONNECTATTR : {
                int type = in.readInt();                        // attr type

                result.setConnectionAttrType(type);

                switch (type) {

                    case ResultConstants.SQL_ATTR_SAVEPOINT_NAME :
                        result.mainString = in.readString();    // savepoint name
                        break;

                    //  case ResultConstants.SQL_ATTR_AUTO_IPD :
                    //      - always true
                    //  default: throw - case never happens
                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500, "Result");
                }

                break;
            }

            case ResultConstants.SQLCANCEL :
                result.databaseID   = in.readInt();
                result.sessionID    = in.readLong();
                result.statementID  = in.readLong();
                result.generateKeys = in.readInt();
                result.mainString   = in.readString();
                break;

            case ResultConstants.PREPARE_ACK :
                result.statementReturnType = in.readByte();
                result.statementID         = in.readLong();
                result.rsProperties        = in.readByte();
                result.metaData            = new ResultMetaData(in);
                result.parameterMetaData   = new ResultMetaData(in);
                break;

            case ResultConstants.CALL_RESPONSE :
                result.updateCount         = in.readInt();
                result.fetchSize           = in.readInt();
                result.statementID         = in.readLong();
                result.statementReturnType = in.readByte();
                result.rsProperties        = in.readByte();
                result.metaData            = new ResultMetaData(in);
                result.valueData           = readSimple(in, result.metaData);
                break;

            case ResultConstants.EXECUTE :
                result.updateCount  = in.readInt();
                result.fetchSize    = in.readInt();
                result.statementID  = in.readLong();
                result.rsProperties = in.readByte();
                result.queryTimeout = in.readShort();

                Statement statement = session.statementManager.getStatement(
                    result.statementID);

                if (statement == null) {

                    // invalid statement
                    result.mode      = ResultConstants.EXECUTE_INVALID;
                    result.valueData = ValuePool.emptyObjectArray;
                    break;
                }

                result.statement = statement;
                result.metaData  = result.statement.getParametersMetaData();
                result.valueData = readSimple(in, result.metaData);
                break;

            case ResultConstants.UPDATE_RESULT : {
                result.id = in.readLong();

                int type = in.readInt();

                result.setActionType(type);

                result.metaData  = new ResultMetaData(in);
                result.valueData = readSimple(in, result.metaData);
                break;
            }

            case ResultConstants.BATCHEXECRESPONSE :
            case ResultConstants.BATCHEXECUTE :
            case ResultConstants.BATCHEXECDIRECT :
            case ResultConstants.SETSESSIONATTR : {
                result.updateCount  = in.readInt();
                result.fetchSize    = in.readInt();
                result.statementID  = in.readLong();
                result.queryTimeout = in.readShort();
                result.metaData     = new ResultMetaData(in);

                result.navigator.readSimple(in, result.metaData);
                break;
            }

            case ResultConstants.PARAM_METADATA : {
                result.metaData = new ResultMetaData(in);

                result.navigator.read(in, result.metaData);
                break;
            }

            case ResultConstants.REQUESTDATA : {
                result.id          = in.readLong();
                result.updateCount = in.readInt();
                result.fetchSize   = in.readInt();
                break;
            }

            case ResultConstants.DATAHEAD :
            case ResultConstants.DATA :
            case ResultConstants.GENERATED : {
                result.id           = in.readLong();
                result.updateCount  = in.readInt();
                result.fetchSize    = in.readInt();
                result.rsProperties = in.readByte();
                result.metaData     = new ResultMetaData(in);
                result.navigator    = new RowSetNavigatorClient();

                result.navigator.read(in, result.metaData);
                break;
            }

            case ResultConstants.DATAROWS : {
                result.metaData  = new ResultMetaData(in);
                result.navigator = new RowSetNavigatorClient();

                result.navigator.read(in, result.metaData);
                break;
            }

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Result");
        }

        return result;
    }

    /**
     * For interval PSM return values
     */
    public static Result newPSMResult(int type, String label, Object value) {

        Result result = newResult(ResultConstants.VALUE);

        result.errorCode  = type;
        result.mainString = label;
        result.valueData  = value;

        return result;
    }

    /**
     * For interval PSM return values
     */
    public static Result newPSMResult(Object value) {

        Result result = newResult(ResultConstants.VALUE);

        result.valueData = value;

        return result;
    }

    /**
     * For SQLPREPARE
     * For preparation of SQL prepared statements.
     */
    public static Result newPrepareStatementRequest() {
        return newResult(ResultConstants.PREPARE);
    }

    /**
     * For SQLEXECUTE
     * For execution of SQL prepared statements.
     * The parameters are set afterwards as the Result is reused
     */
    public static Result newPreparedExecuteRequest(
            Type[] types,
            long statementId) {

        Result result = newResult(ResultConstants.EXECUTE);

        result.metaData    = ResultMetaData.newSimpleResultMetaData(types);
        result.statementID = statementId;
        result.valueData   = ValuePool.emptyObjectArray;

        return result;
    }

    /**
     * For CALL_RESPONSE
     * For execution of SQL callable statements.
     */
    public static Result newCallResponse(
            Type[] types,
            long statementId,
            Object[] values) {

        Result result = newResult(ResultConstants.CALL_RESPONSE);

        result.metaData    = ResultMetaData.newSimpleResultMetaData(types);
        result.statementID = statementId;
        result.valueData   = values;

        return result;
    }

    /**
     * For UPDATE_RESULT
     * The parameters are set afterwards as the Result is reused
     */
    public static Result newUpdateResultRequest(Type[] types, long id) {

        Result result = newResult(ResultConstants.UPDATE_RESULT);

        result.metaData  = ResultMetaData.newUpdateResultMetaData(types);
        result.id        = id;
        result.valueData = ValuePool.emptyObjectArray;

        return result;
    }

    /**
     * For UPDATE_RESULT results
     * The parameters are set by this method as the Result is reused
     */
    public void setPreparedResultUpdateProperties(Object[] parameterValues) {
        valueData = parameterValues;
    }

    /**
     * For SQLEXECUTE results
     * The parameters are set by this method as the Result is reused
     */
    public void setPreparedExecuteProperties(
            Object[] parameterValues,
            int maxRows,
            int fetchSize,
            int resultProps,
            int timeout) {

        mode              = ResultConstants.EXECUTE;
        valueData         = parameterValues;
        updateCount       = maxRows;
        this.fetchSize    = fetchSize;
        this.rsProperties = resultProps;
        queryTimeout      = timeout;
    }

    /**
     * For BATCHEXECUTE
     */
    public void setBatchedPreparedExecuteRequest() {

        mode = ResultConstants.BATCHEXECUTE;

        if (navigator == null) {
            navigator = new RowSetNavigatorClient(4);
        } else {
            navigator.clear();
        }

        updateCount    = 0;
        this.fetchSize = 0;
    }

    public void addBatchedPreparedExecuteRequest(Object[] parameterValues) {
        navigator.add(parameterValues);
    }

    /**
     * For BATCHEXECDIRECT
     */
    public static Result newBatchedExecuteRequest() {

        Type[] types  = new Type[]{ Type.SQL_VARCHAR };
        Result result = newResult(ResultConstants.BATCHEXECDIRECT);

        result.metaData = ResultMetaData.newSimpleResultMetaData(types);

        return result;
    }

    /**
     * For BATCHEXERESPONSE for a BATCHEXECUTE or BATCHEXECDIRECT
     */
    public static Result newBatchedExecuteResponse(
            int[] updateCounts,
            Result generatedResult,
            Result e) {

        Result result = newResult(ResultConstants.BATCHEXECRESPONSE);

        result.addChainedResult(generatedResult);
        result.addChainedResult(e);

        Type[] types = new Type[]{ Type.SQL_INTEGER };

        result.metaData = ResultMetaData.newSimpleResultMetaData(types);

        Object[][] table = new Object[updateCounts.length][];

        for (int i = 0; i < updateCounts.length; i++) {
            table[i] = new Object[]{ ValuePool.getInt(updateCounts[i]) };
        }

        ((RowSetNavigatorClient) result.navigator).setData(table);

        return result;
    }

    public static Result newResetSessionRequest() {
        return newResult(ResultConstants.RESETSESSION);
    }

    public static Result newConnectionAttemptRequest(
            String user,
            String password,
            String database,
            String zoneString,
            int timeZoneSeconds) {

        Result result = newResult(ResultConstants.CONNECT);

        result.mainString   = user;
        result.subString    = password;
        result.zoneString   = zoneString;
        result.databaseName = database;
        result.updateCount  = timeZoneSeconds;

        return result;
    }

    public static Result newConnectionAcknowledgeResponse(Session session) {

        Result result = newResult(ResultConstants.CONNECTACKNOWLEDGE);

        result.sessionID    = session.getId();
        result.databaseID   = session.getDatabase().getDatabaseID();
        result.databaseName = session.getDatabase().getNameString();
        result.mainString = session.getDatabase()
                                   .getProperties()
                                   .getClientPropertiesAsString();
        result.generateKeys = session.getRandomId();

        return result;
    }

    public static Result newUpdateZeroResult() {
        return new Result(ResultConstants.UPDATECOUNT, 0);
    }

    public static Result newUpdateCountResult(int count) {
        return new Result(ResultConstants.UPDATECOUNT, count);
    }

    public static Result newUpdateCountResult(ResultMetaData meta, int count) {

        Result result     = newResult(ResultConstants.UPDATECOUNT);
        Result dataResult = newGeneratedDataResult(meta);

        result.updateCount = count;

        result.addChainedResult(dataResult);

        return result;
    }

    public static Result newSingleColumnResult(ResultMetaData meta) {

        Result result = newResult(ResultConstants.DATA);

        result.metaData  = meta;
        result.navigator = new RowSetNavigatorClient();

        return result;
    }

    public static Result newSingleColumnResult(String colName) {

        Result result = newResult(ResultConstants.DATA);

        result.metaData  = ResultMetaData.newSingleColumnMetaData(colName);
        result.navigator = new RowSetNavigatorClient(8);

        return result;
    }

    public static Result newSingleColumnStringResult(
            String colName,
            String contents) {

        Result           result = Result.newSingleColumnResult(colName);
        LineNumberReader lnr = new LineNumberReader(new StringReader(contents));

        while (true) {
            String line = null;

            try {
                line = lnr.readLine();
            } catch (Exception e) {}

            if (line == null) {
                break;
            }

            result.getNavigator().add(new Object[]{ line });
        }

        return result;
    }

    public static Result newMultiColumnResult(String[] names, Type[] types) {

        Result result = newResult(ResultConstants.DATA);

        result.metaData  = ResultMetaData.newMetaData(names, types);
        result.navigator = new RowSetNavigatorClient(8);

        return result;
    }

    public static Result newPrepareResponse(Statement statement) {

        Result r = newResult(ResultConstants.PREPARE_ACK);

        r.statement   = statement;
        r.statementID = statement.getID();

        int csType = statement.getType();

        r.statementReturnType = statement.getStatementReturnType();
        r.parameterMetaData   = statement.getParametersMetaData();
        r.metaData            = statement.getResultMetaData();

        ResultMetaData generatedMeta = statement.generatedResultMetaData();

        if (generatedMeta != null
                && generatedMeta.getColumnCount() > 0
                && r.metaData.getColumnCount() == 0) {
            r.metaData = generatedMeta;
        }

        return r;
    }

    public static Result newCancelRequest(
            int randomId,
            long statementId,
            String sql) {

        Result r = newResult(ResultConstants.SQLCANCEL);

        r.statementID  = statementId;
        r.mainString   = sql;
        r.generateKeys = randomId;

        return r;
    }

    public static Result newFreeStmtRequest(long statementID) {

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
    public void setPrepareOrExecuteProperties(
            String sql,
            int maxRows,
            int fetchSize,
            int statementReturnType,
            int timeout,
            int resultSetProperties,
            int keyMode,
            int[] generatedIndexes,
            String[] generatedNames) {

        mainString               = sql;
        updateCount              = maxRows;
        this.fetchSize           = fetchSize;
        this.statementReturnType = statementReturnType;
        queryTimeout             = timeout;
        rsProperties             = resultSetProperties;
        generateKeys             = keyMode;
        generatedMetaData = ResultMetaData.newGeneratedColumnsMetaData(
            generatedIndexes,
            generatedNames);
    }

    public static Result newSetSavepointRequest(String name) {

        Result result;

        result = newResult(ResultConstants.SETCONNECTATTR);

        result.setConnectionAttrType(ResultConstants.SQL_ATTR_SAVEPOINT_NAME);
        result.setMainString(name);

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

        result.navigator = new RowSetNavigatorClient();
        result.metaData  = md;

        return result;
    }

    public static Result newGeneratedDataResult(ResultMetaData md) {

        Result result = newResult(ResultConstants.GENERATED);

        result.navigator = new RowSetNavigatorClient();
        result.metaData  = md;

        return result;
    }

    public static Result newEmptyGeneratedResult() {
        return Result.newGeneratedDataResult(emptyMeta);
    }

    /**
     * initially, only used for updatability
     */
    public int getExecuteProperties() {
        return rsProperties;
    }

    public static Result newDataHeadResult(
            SessionInterface session,
            Result source,
            int offset,
            int count) {

        if (offset + count > source.navigator.getSize()) {
            count = source.navigator.getSize() - offset;
        }

        Result result = newResult(ResultConstants.DATAHEAD);

        result.metaData = source.metaData;
        result.navigator = new RowSetNavigatorClient(
            source.navigator,
            offset,
            count);

        result.navigator.setId(source.navigator.getId());
        result.setSession(session);

        result.rsProperties = source.rsProperties;
        result.fetchSize    = source.fetchSize;

        return result;
    }

    public static Result newDataRowsResult(
            Result source,
            int offset,
            int count) {

        if (offset + count > source.navigator.getSize()) {
            count = source.navigator.getSize() - offset;
        }

        Result result = newResult(ResultConstants.DATAROWS);

        result.id       = source.id;
        result.metaData = source.metaData;
        result.navigator = new RowSetNavigatorClient(
            source.navigator,
            offset,
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

        result.navigator = new RowSetNavigatorClient(1);
        result.metaData  = sessionAttributesMetaData;

        result.navigator.add(new Object[AttributePos.INFO_LIMIT]);

        return result;
    }

    public static Result newWarningResult(HsqlException w) {

        Result result = newResult(ResultConstants.WARNING);

        result.mainString = w.getMessage();
        result.subString  = w.getSQLState();
        result.errorCode  = w.getErrorCode();

        return result;
    }

    public static Result newErrorResult(Throwable t) {
        return newErrorResult(t, null);
    }

    /* @todo 1.9.0 fredt - move the messages to Error.java */
    public static Result newErrorResult(Throwable t, String statement) {

        Result result = newResult(ResultConstants.ERROR);

        if (t instanceof HsqlException) {
            result.exception  = (HsqlException) t;
            result.mainString = result.exception.getMessage();
            result.subString  = result.exception.getSQLState();

            if (statement != null) {
                result.mainString += " in statement [" + statement + "]";
            }

            result.errorCode = result.exception.getErrorCode();
        } else if (t instanceof OutOfMemoryError) {
            result.exception  = Error.error(ErrorCode.OUT_OF_MEMORY, t);
            result.mainString = result.exception.getMessage();
            result.subString  = result.exception.getSQLState();
            result.errorCode  = result.exception.getErrorCode();
        } else if (t instanceof IOException) {
            result.exception = Error.error(ErrorCode.GENERAL_IO_ERROR);
            result.mainString = result.exception.getMessage() + ' '
                                + t.getMessage();
            result.subString = result.exception.getSQLState();
            result.errorCode = result.exception.getErrorCode();
        } else {
            result.exception  = Error.error(ErrorCode.GENERAL_ERROR);
            result.mainString = result.exception.getMessage();
            result.subString  = result.exception.getSQLState();
            result.errorCode  = result.exception.getErrorCode();
        }

        return result;
    }

    public void write(
            SessionInterface session,
            DataOutputStream dataOut,
            RowOutputInterface rowOut)
            throws IOException {

        rowOut.reset();
        rowOut.writeByte(mode);

        int startPos = rowOut.size();

        rowOut.writeSize(0);

        switch (mode) {

            case ResultConstants.GETSESSIONATTR :
                rowOut.writeByte(statementReturnType);
                break;

            case ResultConstants.DISCONNECT :
            case ResultConstants.RESETSESSION :
            case ResultConstants.STARTTRAN :
                break;

            case ResultConstants.PREPARE :
                rowOut.writeByte(statementReturnType);
                rowOut.writeString(mainString);
                rowOut.writeByte(rsProperties);
                rowOut.writeByte(generateKeys);

                if (generateKeys
                        == ResultConstants.RETURN_GENERATED_KEYS_COL_NAMES
                        || generateKeys
                        == ResultConstants.RETURN_GENERATED_KEYS_COL_INDEXES) {
                    generatedMetaData.write(rowOut);
                }

                break;

            case ResultConstants.FREESTMT :
                rowOut.writeLong(statementID);
                break;

            case ResultConstants.CLOSE_RESULT :
                rowOut.writeLong(id);
                break;

            case ResultConstants.EXECDIRECT :
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeByte(statementReturnType);
                rowOut.writeString(mainString);
                rowOut.writeByte(rsProperties);
                rowOut.writeShort(queryTimeout);
                rowOut.writeByte(generateKeys);

                if (generateKeys
                        == ResultConstants.RETURN_GENERATED_KEYS_COL_NAMES
                        || generateKeys
                        == ResultConstants.RETURN_GENERATED_KEYS_COL_INDEXES) {
                    generatedMetaData.write(rowOut);
                }

                break;

            case ResultConstants.CONNECT :
                rowOut.writeString(databaseName);
                rowOut.writeString(mainString);
                rowOut.writeString(subString);
                rowOut.writeString(zoneString);
                rowOut.writeInt(updateCount);
                break;

            case ResultConstants.ERROR :
            case ResultConstants.WARNING :
                rowOut.writeString(mainString);
                rowOut.writeString(subString);
                rowOut.writeInt(errorCode);
                break;

            case ResultConstants.CONNECTACKNOWLEDGE :
                rowOut.writeInt(databaseID);
                rowOut.writeLong(sessionID);
                rowOut.writeString(databaseName);
                rowOut.writeString(mainString);
                rowOut.writeInt(generateKeys);
                break;

            case ResultConstants.UPDATECOUNT :
                rowOut.writeInt(updateCount);
                break;

            case ResultConstants.ENDTRAN : {
                int type = getActionType();

                rowOut.writeInt(type);                     // endtran type

                switch (type) {

                    case ResultConstants.TX_SAVEPOINT_NAME_RELEASE :
                    case ResultConstants.TX_SAVEPOINT_NAME_ROLLBACK :
                        rowOut.writeString(mainString);    // savepoint name
                        break;

                    case ResultConstants.TX_COMMIT :
                    case ResultConstants.TX_ROLLBACK :
                    case ResultConstants.TX_COMMIT_AND_CHAIN :
                    case ResultConstants.TX_ROLLBACK_AND_CHAIN :
                    case ResultConstants.PREPARECOMMIT :
                        break;

                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500, "Result");
                }

                break;
            }

            case ResultConstants.SQLCANCEL :
                rowOut.writeInt(databaseID);
                rowOut.writeLong(sessionID);
                rowOut.writeLong(statementID);
                rowOut.writeInt(generateKeys);
                rowOut.writeString(mainString);
                break;

            case ResultConstants.PREPARE_ACK :
                rowOut.writeByte(statementReturnType);
                rowOut.writeLong(statementID);
                rowOut.writeByte(rsProperties);
                metaData.write(rowOut);
                parameterMetaData.write(rowOut);
                break;

            case ResultConstants.CALL_RESPONSE :
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeLong(statementID);
                rowOut.writeByte(statementReturnType);
                rowOut.writeByte(rsProperties);
                metaData.write(rowOut);
                writeSimple(rowOut, metaData, (Object[]) valueData);
                break;

            case ResultConstants.EXECUTE :
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeLong(statementID);
                rowOut.writeByte(rsProperties);
                rowOut.writeShort(queryTimeout);
                writeSimple(rowOut, metaData, (Object[]) valueData);
                break;

            case ResultConstants.UPDATE_RESULT :
                rowOut.writeLong(id);
                rowOut.writeInt(getActionType());
                metaData.write(rowOut);
                writeSimple(rowOut, metaData, (Object[]) valueData);
                break;

            case ResultConstants.BATCHEXECRESPONSE :
            case ResultConstants.BATCHEXECUTE :
            case ResultConstants.BATCHEXECDIRECT :
            case ResultConstants.SETSESSIONATTR : {
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeLong(statementID);
                rowOut.writeShort(queryTimeout);
                metaData.write(rowOut);
                navigator.writeSimple(rowOut, metaData);
                break;
            }

            case ResultConstants.PARAM_METADATA : {
                metaData.write(rowOut);
                navigator.write(rowOut, metaData);
                break;
            }

            case ResultConstants.SETCONNECTATTR : {
                int type = getConnectionAttrType();

                rowOut.writeInt(type);                     // attr type / updateCount

                switch (type) {

                    case ResultConstants.SQL_ATTR_SAVEPOINT_NAME :
                        rowOut.writeString(mainString);    // savepoint name
                        break;

                    // case ResultConstants.SQL_ATTR_AUTO_IPD // always true
                    // default: // throw, but case never happens
                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500, "Result");
                }

                break;
            }

            case ResultConstants.REQUESTDATA : {
                rowOut.writeLong(id);
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                break;
            }

            case ResultConstants.DATAROWS :
                metaData.write(rowOut);
                navigator.write(rowOut, metaData);
                break;

            case ResultConstants.DATAHEAD :
            case ResultConstants.DATA :
            case ResultConstants.GENERATED :
                rowOut.writeLong(id);
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeByte(rsProperties);
                metaData.write(rowOut);
                navigator.write(rowOut, metaData);
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Result");
        }

        rowOut.writeSize(rowOut.size() - startPos);
        dataOut.write(rowOut.getOutputStream().getBuffer(), 0, rowOut.size());

        int    count   = getLobCount();
        Result current = this;

        for (int i = 0; i < count; i++) {
            ResultLob lob = current.lobResults;

            lob.writeBody(session, dataOut);

            current = current.lobResults;
        }

        if (chainedResult == null) {
            dataOut.writeByte(ResultConstants.NONE);
        } else {
            chainedResult.write(session, dataOut, rowOut);
        }

        dataOut.flush();
    }

    public int getType() {
        return mode;
    }

    public boolean isData() {
        return mode == ResultConstants.DATA || mode == ResultConstants.DATAHEAD;
    }

    public boolean isError() {
        return mode == ResultConstants.ERROR;
    }

    public boolean isWarning() {
        return mode == ResultConstants.WARNING;
    }

    public boolean isUpdateCount() {
        return mode == ResultConstants.UPDATECOUNT;
    }

    public boolean isSimpleValue() {
        return mode == ResultConstants.VALUE;
    }

    public boolean hasGeneratedKeys() {
        return mode == ResultConstants.UPDATECOUNT && chainedResult != null;
    }

    public HsqlException getException() {

        if (exception == null && mode == ResultConstants.ERROR) {
            exception = Error.error(this);
        }

        return exception;
    }

    public long getStatementID() {
        return statementID;
    }

    public void setStatementID(long statementId) {
        this.statementID = statementId;
    }

    public String getMainString() {
        return mainString;
    }

    public void setMainString(String sql) {
        this.mainString = sql;
    }

    public String getSubString() {
        return subString;
    }

    public String getZoneString() {
        return zoneString;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public Object getValueObject() {
        return valueData;
    }

    public void setValueObject(Object value) {
        valueData = value;
    }

    public Statement getStatement() {
        return statement;
    }

    public void setStatement(Statement statement) {
        this.statement = statement;
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

    public int getActionType() {
        return updateCount;
    }

    public void setActionType(int type) {
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
        mainString  = savepoint == null
                      ? ""
                      : savepoint;
    }

    public Object[] getSingleRowData() {

        initialiseNavigator();
        navigator.next();

        Object[] data = navigator.getCurrent();

        data = (Object[]) ArrayUtil.resizeArrayIfDifferent(
            data,
            metaData.getColumnCount());

        return data;
    }

    public Object[] getParameterData() {
        return (Object[]) valueData;
    }

    public Object[] getSessionAttributes() {

        initialiseNavigator();
        navigator.next();

        return navigator.getCurrent();
    }

    public void setResultType(int type) {
        mode = (byte) type;
    }

    public void setStatementType(int type) {
        statementReturnType = type;
    }

    public int getStatementType() {
        return statementReturnType;
    }

    public void setSessionRandomID(int id) {
        generateKeys = id;
    }

    public int getSessionRandomID() {
        return generateKeys;
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

    public void addWarnings(HsqlException[] warnings) {

        for (int i = 0; i < warnings.length; i++) {
            Result warning = newWarningResult(warnings[i]);

            addChainedResult(warning);
        }
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

    public void addRows(List<String> sqlArray) {

        if (sqlArray == null) {
            return;
        }

        for (int i = 0; i < sqlArray.size(); i++) {
            String[] s = new String[1];

            s[0] = sqlArray.get(i);

            initialiseNavigator().add(s);
        }
    }

    private static Object[] readSimple(
            RowInputInterface in,
            ResultMetaData meta) {
        int size = in.readInt();

        return in.readData(meta.columnTypes);
    }

    private static void writeSimple(
            RowOutputInterface out,
            ResultMetaData meta,
            Object[] data) {

        out.writeInt(1);
        out.writeData(
            meta.getColumnCount(),
            meta.columnTypes,
            data,
            null,
            null);
    }

//----------- Navigation
    public RowSetNavigator navigator;

    public RowSetNavigator getNavigator() {
        return navigator;
    }

    public void setNavigator(RowSetNavigator navigator) {
        this.navigator = navigator;
    }

    public RowSetNavigator initialiseNavigator() {

        switch (mode) {

            case ResultConstants.BATCHEXECUTE :
            case ResultConstants.BATCHEXECDIRECT :
            case ResultConstants.BATCHEXECRESPONSE :
            case ResultConstants.SETSESSIONATTR :
            case ResultConstants.PARAM_METADATA :
                navigator.beforeFirst();

                return navigator;

            case ResultConstants.DATA :
            case ResultConstants.DATAHEAD :
            case ResultConstants.GENERATED :
                navigator.reset();

                return navigator;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Result");
        }
    }
}
