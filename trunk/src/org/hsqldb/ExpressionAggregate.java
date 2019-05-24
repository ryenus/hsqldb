/* Copyright (c) 2001-2019, The HSQL Development Group
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

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayListIdentity;
import org.hsqldb.lib.HsqlList;
import org.hsqldb.map.ValuePool;
import org.hsqldb.types.DTIType;
import org.hsqldb.types.IntervalType;
import org.hsqldb.types.NumberType;
import org.hsqldb.types.RowType;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;

/**
 * Implementation of aggregate operations
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.0
 * @since 1.9.0
 */
public class ExpressionAggregate extends Expression {

    ExpressionAggregate(int type, boolean distinct, Expression e) {

        super(type);

        nodes               = new Expression[BINARY];
        isDistinctAggregate = distinct;
        nodes[LEFT]         = e;
        nodes[RIGHT]        = Expression.EXPR_TRUE;
    }

    public boolean isSelfAggregate() {
        return true;
    }

    public String getSQL() {

        StringBuilder sb   = new StringBuilder(64);
        String        left = getContextSQL(nodes.length > 0 ? nodes[LEFT]
                                                            : null);

        switch (opType) {

            case OpTypes.COUNT :
                sb.append(' ').append(Tokens.T_COUNT).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.SUM :
                sb.append(' ').append(Tokens.T_SUM).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.MIN :
                sb.append(' ').append(Tokens.T_MIN).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.MAX :
                sb.append(' ').append(Tokens.T_MAX).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.AVG :
                sb.append(' ').append(Tokens.T_AVG).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.EVERY :
                sb.append(' ').append(Tokens.T_EVERY).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.SOME :
                sb.append(' ').append(Tokens.T_SOME).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.STDDEV_POP :
                sb.append(' ').append(Tokens.T_STDDEV_POP).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.STDDEV_SAMP :
                sb.append(' ').append(Tokens.T_STDDEV_SAMP).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.VAR_POP :
                sb.append(' ').append(Tokens.T_VAR_POP).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.VAR_SAMP :
                sb.append(' ').append(Tokens.T_VAR_SAMP).append('(');
                sb.append(left).append(')');
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionAggregate");
        }

        return sb.toString();
    }

    protected String describe(Session session, int blanks) {

        StringBuilder sb = new StringBuilder(64);

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        switch (opType) {

            case OpTypes.COUNT :
                sb.append(Tokens.T_COUNT).append(' ');
                break;

            case OpTypes.SUM :
                sb.append(Tokens.T_SUM).append(' ');
                break;

            case OpTypes.MIN :
                sb.append(Tokens.T_MIN).append(' ');
                break;

            case OpTypes.MAX :
                sb.append(Tokens.T_MAX).append(' ');
                break;

            case OpTypes.AVG :
                sb.append(Tokens.T_AVG).append(' ');
                break;

            case OpTypes.EVERY :
                sb.append(Tokens.T_EVERY).append(' ');
                break;

            case OpTypes.SOME :
                sb.append(Tokens.T_SOME).append(' ');
                break;

            case OpTypes.STDDEV_POP :
                sb.append(Tokens.T_STDDEV_POP).append(' ');
                break;

            case OpTypes.STDDEV_SAMP :
                sb.append(Tokens.T_STDDEV_SAMP).append(' ');
                break;

            case OpTypes.VAR_POP :
                sb.append(Tokens.T_VAR_POP).append(' ');
                break;

            case OpTypes.VAR_SAMP :
                sb.append(Tokens.T_VAR_SAMP).append(' ');
                break;

            default :
        }

        if (getLeftNode() != null) {
            sb.append(" arg=[");
            sb.append(nodes[LEFT].describe(session, blanks + 1));
            sb.append(']');
        }

        return sb.toString();
    }

    public HsqlList resolveColumnReferences(Session session,
            RangeGroup rangeGroup, int rangeCount, RangeGroup[] rangeGroups,
            HsqlList unresolvedSet, boolean acceptsSequences) {

        HsqlList conditionSet = nodes[RIGHT].resolveColumnReferences(session,
            rangeGroup, rangeCount, rangeGroups, null, false);

        if (conditionSet != null) {
            ExpressionColumn.checkColumnsResolved(conditionSet);
        }

        if (unresolvedSet == null) {
            unresolvedSet = new ArrayListIdentity();
        }

        unresolvedSet.add(this);

        if (rangeGroup.getRangeVariables().length > 0) {
            this.rangeGroups = rangeGroups;
            this.rangeGroup  = rangeGroup;
        }

        return unresolvedSet;
    }

    public void resolveTypes(Session session, Expression parent) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        if (nodes[LEFT].getDegree() > 1) {
            nodes[LEFT].dataType = new RowType(nodes[LEFT].nodeDataTypes);
        }

        if (nodes[LEFT].isUnresolvedParam()) {
            throw Error.error(ErrorCode.X_42567);
        }

        if (isDistinctAggregate) {
            if (nodes[LEFT].dataType.isLobType()) {
                throw Error.error(ErrorCode.X_42534);
            }
        }

        dataType = getType(session, opType, nodes[LEFT].dataType);
    }

    /**
     * During parsing and before an instance of SetFunction is created,
     * getType is called with type parameter set to correct type when main
     * SELECT statements contain aggregates.
     *
     */
    static Type getType(Session session, int setType, Type dataType) {

        if (setType == OpTypes.COUNT) {
            return Type.SQL_BIGINT;
        }

        int typeCode = dataType.typeCode;

        if (dataType.isIntervalYearMonthType()) {
            typeCode = Types.SQL_INTERVAL_MONTH;
        } else if (dataType.isIntervalDaySecondType()) {
            typeCode = Types.SQL_INTERVAL_SECOND;
        }

        switch (setType) {

            case OpTypes.AVG :
            case OpTypes.MEDIAN : {
                switch (typeCode) {

                    case Types.TINYINT :
                    case Types.SQL_SMALLINT :
                    case Types.SQL_INTEGER :
                    case Types.SQL_BIGINT :
                    case Types.SQL_NUMERIC :
                    case Types.SQL_DECIMAL :
                        int scale = session.database.sqlAvgScale;

                        if (scale <= dataType.scale) {
                            return dataType;
                        }

                        int digits =
                            ((NumberType) dataType).getDecimalPrecision();

                        return NumberType.getNumberType(Types.SQL_DECIMAL,
                                                        digits + scale, scale);

                    case Types.SQL_REAL :
                    case Types.SQL_FLOAT :
                    case Types.SQL_DOUBLE :
                    case Types.SQL_INTERVAL_MONTH :
                    case Types.SQL_INTERVAL_SECOND :
                    case Types.SQL_DATE :
                    case Types.SQL_TIMESTAMP :
                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                        return dataType;

                    default :
                        throw Error.error(ErrorCode.X_42563);
                }
            }
            case OpTypes.SUM : {
                switch (typeCode) {

                    case Types.TINYINT :
                    case Types.SQL_SMALLINT :
                    case Types.SQL_INTEGER :
                        return Type.SQL_BIGINT;

                    case Types.SQL_BIGINT :
                        return Type.SQL_DECIMAL_BIGINT_SQR;

                    case Types.SQL_REAL :
                    case Types.SQL_FLOAT :
                    case Types.SQL_DOUBLE :
                        return Type.SQL_DOUBLE;

                    case Types.SQL_NUMERIC :
                    case Types.SQL_DECIMAL :
                        return Type.getType(dataType.typeCode, null, null,
                                            dataType.precision * 2,
                                            dataType.scale);

                    case Types.SQL_INTERVAL_MONTH :
                    case Types.SQL_INTERVAL_SECOND :
                        return IntervalType.newIntervalType(
                            dataType.typeCode, DTIType.maxIntervalPrecision,
                            dataType.scale);

                    default :
                        throw Error.error(ErrorCode.X_42563);
                }
            }
            case OpTypes.MIN :
            case OpTypes.MAX :
                if (dataType.isArrayType() || dataType.isLobType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                return dataType;

            case OpTypes.EVERY :
            case OpTypes.SOME :
                if (dataType.isBooleanType()) {
                    return Type.SQL_BOOLEAN;
                }
                break;

            case OpTypes.STDDEV_POP :
            case OpTypes.STDDEV_SAMP :
            case OpTypes.VAR_POP :
            case OpTypes.VAR_SAMP :
                if (dataType.isNumberType()) {
                    return Type.SQL_DOUBLE;
                }
                break;

            case OpTypes.USER_AGGREGATE :
                return dataType;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionAggregate");
        }

        throw Error.error(ErrorCode.X_42563);
    }

    boolean equals(Expression other) {

        if (other instanceof ExpressionAggregate) {
            ExpressionAggregate o = (ExpressionAggregate) other;
            boolean result = super.equals(other)
                             && isDistinctAggregate == o.isDistinctAggregate;

            return result;
        }

        return false;
    }

    public SetFunction updateAggregatingValue(Session session,
            SetFunction currValue) {

        if (!nodes[RIGHT].testCondition(session)) {
            return currValue;
        }

        if (currValue == null) {
            currValue = getSetFunction(session);
        }

        Object newValue = nodes[LEFT].opType == OpTypes.ASTERISK
                          ? ValuePool.INTEGER_1
                          : nodes[LEFT].getValue(session);

        currValue.add(newValue);

        return currValue;
    }

    SetFunction getSetFunction(Session session) {

        return new SetFunctionValueAggregate(session, opType,
                                             nodes[LEFT].dataType, dataType,
                                             isDistinctAggregate);
    }

    /**
     * Get the result of a SetFunction or an ordinary value
     *
     * @param session session
     * @param currValue instance of set function or value
     * @return object
     */
    public Object getAggregatedValue(Session session, SetFunction currValue) {

        if (currValue == null) {
            return opType == OpTypes.COUNT ? Long.valueOf(0)
                                           : null;
        }

        return currValue.getValue();
    }

    public Expression getCondition() {
        return nodes[RIGHT];
    }

    public boolean hasCondition() {
        return !nodes[RIGHT].isTrue();
    }

    public void setCondition(ExpressionLogical e) {
        nodes[RIGHT] = e;
    }
}
