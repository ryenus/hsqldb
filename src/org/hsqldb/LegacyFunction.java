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

import org.hsqldb.lib.IntValueHashMap;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Type;

import java.sql.Timestamp;

import org.hsqldb.types.DateTimeIntervalType;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.IntervalSecondData;
import org.hsqldb.types.IntervalType;
import org.hsqldb.types.IntervalMonthData;

/**
 * Implementation of HSQL functions with reserved names or value functions
 * that have an SQL standard equivalent.<p>
 *
 * Some functions are translated into
 * equivalent SQL Standard functions.
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class LegacyFunction extends SQLFunction {

    private final static int FUNC_IDENTITY      = 81;
    private final static int FUNC_SYSDATE       = 82;
    private final static int FUNC_TIMESTAMPADD  = 83;
    private final static int FUNC_TIMESTAMPDIFF = 84;

    //
    static IntValueHashMap legacyRegularFuncMap = new IntValueHashMap();

    static {
        legacyRegularFuncMap.put(Token.T_YEAR, FUNC_EXTRACT);
        legacyRegularFuncMap.put(Token.T_MONTH, FUNC_EXTRACT);
        legacyRegularFuncMap.put(Token.T_DAY, FUNC_EXTRACT);
        legacyRegularFuncMap.put(Token.T_HOUR, FUNC_EXTRACT);
        legacyRegularFuncMap.put(Token.T_MINUTE, FUNC_EXTRACT);
        legacyRegularFuncMap.put(Token.T_SECOND, FUNC_EXTRACT);
        legacyRegularFuncMap.put(Token.T_IDENTITY, FUNC_IDENTITY);
        legacyRegularFuncMap.put(Token.T_TIMESTAMPADD, FUNC_TIMESTAMPADD);
        legacyRegularFuncMap.put(Token.T_TIMESTAMPDIFF, FUNC_TIMESTAMPDIFF);
    }

    static IntValueHashMap legacyValueFuncMap = new IntValueHashMap();

    static {
        legacyValueFuncMap.put(Token.T_SYSDATE, FUNC_SYSDATE);
        legacyValueFuncMap.put(Token.T_TODAY, FUNC_CURRENT_DATE);
        legacyValueFuncMap.put(Token.T_NOW, FUNC_CURRENT_TIMESTAMP);
    }

    private int extractSpec;

    public static SQLFunction newLegacyFunction(String token) {

        int id = legacyRegularFuncMap.get(token, -1);

        if (id == -1) {
            id = legacyValueFuncMap.get(token, -1);
        }

        if (id == -1) {
            return null;
        }

        if (token.equals(Token.T_TODAY) || token.equals(Token.T_NOW)) {
            return new SQLFunction(id);
        }

        LegacyFunction function = new LegacyFunction(id);

        function.extractSpec = Token.get(token);

        return function;
    }

    public static boolean isRegularFunction(String token) {
        return legacyRegularFuncMap.containsKey(token);
    }

    public static boolean isValueFunction(String token) {
        return legacyValueFuncMap.containsKey(token);
    }

    private LegacyFunction(int id) {

        super();

        this.funcType = id;

        switch (id) {

            case FUNC_EXTRACT :
                name      = Token.T_EXTRACT;
                parseList = singleParamList;
                break;

            case FUNC_IDENTITY :
                name      = Token.T_IDENTITY;
                parseList = emptyParamList;
                dataType  = Type.SQL_BIGINT;
                break;

            case FUNC_SYSDATE :
                name      = Token.T_SYSDATE;
                parseList = noParamList;
                dataType  = Type.SQL_TIMESTAMP_NO_FRACTION;
                break;

            case FUNC_TIMESTAMPADD :
                name      = Token.T_TIMESTAMPADD;
                parseList = new short[] {
                    Token.OPENBRACKET, Token.X_KEYSET, 9,
                    Token.SQL_TSI_FRAC_SECOND, Token.SQL_TSI_SECOND,
                    Token.SQL_TSI_MINUTE, Token.SQL_TSI_HOUR,
                    Token.SQL_TSI_DAY, Token.SQL_TSI_WEEK, Token.SQL_TSI_MONTH,
                    Token.SQL_TSI_QUARTER, Token.SQL_TSI_YEAR, Token.COMMA,
                    Token.QUESTION, Token.COMMA, Token.QUESTION,
                    Token.CLOSEBRACKET
                };
                break;

            case FUNC_TIMESTAMPDIFF :
                name      = Token.T_TIMESTAMPADD;
                parseList = new short[] {
                    Token.OPENBRACKET, Token.X_KEYSET, 9,
                    Token.SQL_TSI_FRAC_SECOND, Token.SQL_TSI_SECOND,
                    Token.SQL_TSI_MINUTE, Token.SQL_TSI_HOUR,
                    Token.SQL_TSI_DAY, Token.SQL_TSI_WEEK, Token.SQL_TSI_MONTH,
                    Token.SQL_TSI_QUARTER, Token.SQL_TSI_YEAR, Token.COMMA,
                    Token.QUESTION, Token.COMMA, Token.QUESTION,
                    Token.CLOSEBRACKET
                };
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "SQLFunction");
        }
    }

    public void setArguments(Expression[] argList) {

        switch (funcType) {

            case FUNC_EXTRACT :
                Expression[] newArgList = new Expression[2];

                newArgList[0] = new Expression(ValuePool.getInt(extractSpec),
                                               Type.SQL_INTEGER);
                newArgList[1] = argList[0];
                argList       = newArgList;
                break;
        }

        super.setArguments(argList);
    }

    Object getValue(Session session, Object[] data) throws HsqlException {

        switch (funcType) {

            case FUNC_EXTRACT :
                return super.getValue(session, data);

            case FUNC_IDENTITY :
                return session.getLastIdentity();

            case FUNC_SYSDATE :
                Timestamp ts = session.getCurrentTimestamp();

                Type.SQL_TIMESTAMP.convertToTypeLimits(ts);

                return ts;

            case FUNC_TIMESTAMPADD : {
                if (data[1] == null || data[2] == null) {
                    return null;
                }

                int          part = ((Number) argList[0].valueData).intValue();
                long         units  = ((Number) data[1]).longValue();
                Timestamp    source = (Timestamp) data[2];
                IntervalType t;
                Object       o;

                switch (part) {

                    case Token.SQL_TSI_FRAC_SECOND : {
                        long seconds = (units / DateTimeType.limitNanoseconds);
                        int nanos = (int) (units
                                           % DateTimeType.limitNanoseconds);

                        t = Type.SQL_INTERVAL_SECOND;
                        o = new IntervalSecondData(seconds, nanos, t);

                        return dataType.add(source, o);
                    }
                    case Token.SQL_TSI_SECOND :
                        t = Type.SQL_INTERVAL_SECOND;
                        o = IntervalSecondData.newIntervalSeconds((int) units,
                                t);

                        return dataType.add(source, o);

                    case Token.SQL_TSI_MINUTE :
                        t = Type.SQL_INTERVAL_MINUTE;
                        o = IntervalSecondData.newIntervalMinute((int) units,
                                t);

                        return dataType.add(source, o);

                    case Token.SQL_TSI_HOUR :
                        t = Type.SQL_INTERVAL_HOUR;
                        o = IntervalSecondData.newIntervalHour((int) units, t);

                        return dataType.add(source, o);

                    case Token.SQL_TSI_DAY :
                        t = Type.SQL_INTERVAL_DAY;
                        o = IntervalSecondData.newIntervalDay((int) units, t);

                        return dataType.add(source, o);

                    case Token.SQL_TSI_WEEK :
                        t = Type.SQL_INTERVAL_DAY;
                        o = IntervalSecondData.newIntervalDay((int) units * 7,
                                                              t);

                        return dataType.add(source, o);

                    case Token.SQL_TSI_MONTH :
                        t = Type.SQL_INTERVAL_MONTH;
                        o = IntervalMonthData.newIntervalMonth((int) units, t);

                        return dataType.add(source, o);

                    case Token.SQL_TSI_QUARTER :
                        t = Type.SQL_INTERVAL_MONTH;
                        o = IntervalMonthData.newIntervalMonth((int) units
                                                               * 3, t);

                        return dataType.add(source, o);

                    case Token.SQL_TSI_YEAR :
                        t = Type.SQL_INTERVAL_YEAR;
                        o = IntervalMonthData.newIntervalMonth((int) units, t);

                        return dataType.add(source, o);
                }
            }
            case FUNC_TIMESTAMPDIFF : {
                if (data[1] == null || data[2] == null) {
                    return null;
                }

                int          part = ((Number) argList[0].valueData).intValue();
                Timestamp    a    = (Timestamp) data[2];
                Timestamp    b    = (Timestamp) data[1];
                IntervalType t;

                switch (part) {

                    case Token.SQL_TSI_FRAC_SECOND :
                        t = Type.SQL_INTERVAL_SECOND;

                        return new Long(DateTimeType.limitNanoseconds
                                        * (long) t.convertToInt(t.subtract(a,
                                            b)));

                    case Token.SQL_TSI_SECOND :
                        t = Type.SQL_INTERVAL_SECOND;

                        return new Long(t.convertToInt(t.subtract(a, b)));

                    case Token.SQL_TSI_MINUTE :
                        t = Type.SQL_INTERVAL_MINUTE;

                        return new Long(t.convertToInt(t.subtract(a, b)));

                    case Token.SQL_TSI_HOUR :
                        t = Type.SQL_INTERVAL_HOUR;

                        return new Long(t.convertToInt(t.subtract(a, b)));

                    case Token.SQL_TSI_DAY :
                        t = Type.SQL_INTERVAL_DAY;

                        return new Long(t.convertToInt(t.subtract(a, b)));

                    case Token.SQL_TSI_WEEK :
                        t = Type.SQL_INTERVAL_DAY;

                        return new Long(t.convertToInt(t.subtract(a, b)) / 7);

                    case Token.SQL_TSI_MONTH :
                        t = Type.SQL_INTERVAL_MONTH;

                        return new Long(t.convertToInt(t.subtract(a, b)));

                    case Token.SQL_TSI_QUARTER :
                        t = Type.SQL_INTERVAL_MONTH;

                        return new Long(t.convertToInt(t.subtract(a, b)) / 3);

                    case Token.SQL_TSI_YEAR :
                        t = Type.SQL_INTERVAL_YEAR;

                        return new Long(t.convertToInt(t.subtract(a, b)));

                    default :
                        throw Trace.runtimeError(
                            Trace.UNSUPPORTED_INTERNAL_OPERATION,
                            "LegacyFunction");
                }
            }
            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "LegacyFunction");
        }
    }

    public void resolveTypes(Session session,
                             Expression parent) throws HsqlException {

        switch (funcType) {

            case FUNC_EXTRACT :
                super.resolveTypes(session, parent);

                return;

            case FUNC_IDENTITY :
                return;

            case FUNC_SYSDATE :
                return;

            case FUNC_TIMESTAMPADD :
                if (argList[1].dataType == null) {
                    argList[1].dataType = Type.SQL_BIGINT;
                }

                if (argList[2].dataType == null) {
                    argList[2].dataType = Type.SQL_TIMESTAMP;
                }

                if (!argList[1].dataType.isIntegralType()
                        || argList[2].dataType.type != Types.SQL_TIMESTAMP) {
                    throw Trace.error(Trace.WRONG_DATA_TYPE);
                }

                dataType = argList[2].dataType;

                return;

            case FUNC_TIMESTAMPDIFF :
                if (argList[1].dataType == null) {
                    argList[1].dataType = Type.SQL_TIMESTAMP;
                }

                if (argList[2].dataType == null) {
                    argList[2].dataType = Type.SQL_TIMESTAMP;
                }

                if (argList[1].dataType.type != Types.SQL_TIMESTAMP
                        || argList[2].dataType.type != Types.SQL_TIMESTAMP) {
                    throw Trace.error(Trace.WRONG_DATA_TYPE);
                }

                dataType = argList[2].dataType;

                return;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "LegacyFunction");
        }
    }

    public String getDDL() {

        switch (funcType) {

            case FUNC_EXTRACT :
                return super.getDDL();

            case FUNC_IDENTITY :
                return new StringBuffer(Token.T_IDENTITY).append(
                    Token.T_OPENBRACKET).append(
                    Token.T_CLOSEBRACKET).toString();

            case FUNC_SYSDATE :
                return Token.T_SYSDATE;

            case FUNC_TIMESTAMPADD :
            case FUNC_TIMESTAMPDIFF :
                return new StringBuffer(Token.T_TIMESTAMPADD).append(
                    Token.T_OPENBRACKET).append(argList[0].getDDL()).append(
                    Token.T_COMMA).append(argList[1].getDDL()).append(
                    Token.T_COMMA).append(argList[2].getDDL()).append(
                    Token.T_COMMA).append(Token.T_CLOSEBRACKET).toString();

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "LegacyFunction");
        }
    }

    public String describe(Session session) {
        return super.describe(session);
    }
}
