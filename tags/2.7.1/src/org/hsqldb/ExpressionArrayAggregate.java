/* Copyright (c) 2001-2022, The HSQL Development Group
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
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.List;
import org.hsqldb.types.ArrayType;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.RowType;
import org.hsqldb.types.Type;

/**
 * Implementation of array aggregate operations
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.0
 * @since 2.0.1
 */
public class ExpressionArrayAggregate extends Expression {

    SortAndSlice sort;
    SortAndSlice distinctSort;
    String       separator = ",";
    ArrayType    arrayDataType;
    Type         exprDataType;
    int          exprOpType;    // original opType, may change during resolution
    Expression   condition = Expression.EXPR_TRUE;

    ExpressionArrayAggregate(int type, boolean distinct, Expression e,
                             SortAndSlice sort, String separator) {

        super(type);

        this.isDistinctAggregate = distinct;
        this.sort                = sort;
        this.exprOpType          = e.opType;

        if (separator != null) {
            this.separator = separator;
        }

        if (sort == null) {
            nodes = new Expression[]{ e };
        } else {
            HsqlArrayList list = sort.getExpressionList();

            nodes = new Expression[list.size() + 1];

            list.toArray(nodes);

            nodes[list.size()] = e;

            sort.prepareExtraColumn(1);
        }

        if (isDistinctAggregate) {
            distinctSort = new SortAndSlice();

            distinctSort.prepareSingleColumn(nodes.length - 1);
        }
    }

    public boolean isSelfAggregate() {
        return true;
    }

    public String getSQL() {

        StringBuilder sb   = new StringBuilder(64);
        String        left = getContextSQL(nodes.length > 0 ? nodes[LEFT]
                                                            : null);

        switch (opType) {

            case OpTypes.ARRAY_AGG :
                sb.append(' ').append(Tokens.T_ARRAY_AGG).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.GROUP_CONCAT :
                sb.append(' ').append(Tokens.T_GROUP_CONCAT).append('(');
                sb.append(left).append(')');
                break;

            case OpTypes.MEDIAN :
                sb.append(' ').append(Tokens.T_MEDIAN).append('(');
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

            case OpTypes.ARRAY_AGG :
                sb.append(Tokens.T_ARRAY_AGG).append(' ');
                break;

            case OpTypes.GROUP_CONCAT :
                sb.append(Tokens.T_GROUP_CONCAT).append(' ');
                break;

            case OpTypes.MEDIAN :
                sb.append(Tokens.T_MEDIAN).append(' ');
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

    public List resolveColumnReferences(Session session,
                                        RangeGroup rangeGroup, int rangeCount,
                                        RangeGroup[] rangeGroups,
                                        List unresolvedSet,
                                        boolean acceptsSequences) {

        List conditionSet = condition.resolveColumnReferences(session,
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

        nodeDataTypes = new Type[nodes.length];

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);

                if (nodes[i].isUnresolvedParam()) {
                    throw Error.error(ErrorCode.X_42567);
                }

                if (nodes[i].dataType == null) {
                    throw Error.error(ErrorCode.X_42567);
                }

                nodeDataTypes[i] = nodes[i].dataType;
            }
        }

        exprDataType = nodes[nodes.length - 1].dataType;

        if (exprDataType.isLobType()) {
            throw Error.error(ErrorCode.X_42534);
        }

        if (exprDataType.isArrayType()) {
            throw Error.error(ErrorCode.X_42534);
        }

        Type rowDataType = new RowType(nodeDataTypes);

        switch (opType) {

            case OpTypes.ARRAY_AGG :
                arrayDataType =
                    new ArrayType(rowDataType,
                                  ArrayType.defaultLargeArrayCardinality);
                dataType = new ArrayType(exprDataType,
                                         ArrayType.defaultArrayCardinality);
                break;

            case OpTypes.GROUP_CONCAT :
                arrayDataType =
                    new ArrayType(rowDataType,
                                  ArrayType.defaultLargeArrayCardinality);
                dataType = Type.SQL_VARCHAR_DEFAULT;
                break;

            case OpTypes.MEDIAN :
                arrayDataType =
                    new ArrayType(nodeDataTypes[0],
                                  ArrayType.defaultLargeArrayCardinality);
                dataType = ExpressionAggregate.getType(session,
                                                       OpTypes.MEDIAN,
                                                       exprDataType);
                break;
        }

        condition.resolveTypes(session, null);
    }

    boolean equals(Expression other) {

        if (other instanceof ExpressionArrayAggregate) {
            ExpressionArrayAggregate o = (ExpressionArrayAggregate) other;

            return super.equals(other) && opType == other.opType
                   && exprSubType == other.exprSubType
                   && isDistinctAggregate == o.isDistinctAggregate
                   && separator.equals(o.separator)
                   && condition.equals(o.condition);
        }

        return false;
    }

    public SetFunction updateAggregatingValue(Session session,
            SetFunction currValue) {

        if (!condition.testCondition(session)) {
            return currValue;
        }

        Object   value = null;
        Object[] data;

        switch (opType) {

            case OpTypes.ARRAY_AGG :
                data = new Object[nodes.length];

                for (int i = 0; i < nodes.length; i++) {
                    data[i] = nodes[i].getValue(session);
                }

                value = data;
                break;

            case OpTypes.GROUP_CONCAT :
                data = new Object[nodes.length];

                for (int i = 0; i < nodes.length; i++) {
                    data[i] = nodes[i].getValue(session);
                }

                if (data[data.length - 1] == null) {
                    return currValue;
                }

                value = data;
                break;

            case OpTypes.MEDIAN :
                value = nodes[0].getValue(session);

                if (value == null) {
                    return currValue;
                }
                break;
        }

        if (currValue == null) {
            currValue = new SetFunctionValueArray();
        }

        currValue.add(value);

        return currValue;
    }

    public SetFunction updateAggregatingValue(Session session,
            SetFunction currValue, SetFunction value) {

        if (currValue == null) {
            currValue = new SetFunctionValueArray();
        }

        currValue.addGroup(value);

        return currValue;
    }

    public Object getAggregatedValue(Session session, SetFunction currValue) {

        if (currValue == null) {
            return null;
        }

        Object[] array = (Object[]) currValue.getValue();

        if (isDistinctAggregate) {
            arrayDataType.sort(session, array, distinctSort);

            int size = arrayDataType.deDuplicate(session, array, distinctSort);

            array = (Object[]) ArrayUtil.resizeArrayIfDifferent(array, size);
        }

        if (sort != null) {
            arrayDataType.sort(session, array, sort);
        }

        switch (opType) {

            case OpTypes.ARRAY_AGG : {
                Object[] resultArray = new Object[array.length];

                for (int i = 0; i < array.length; i++) {
                    Object[] row = (Object[]) array[i];

                    resultArray[i] = row[row.length - 1];
                }

                return resultArray;
            }
            case OpTypes.GROUP_CONCAT : {
                StringBuilder sb = new StringBuilder(16 * array.length);

                for (int i = 0; i < array.length; i++) {
                    if (i > 0) {
                        sb.append(separator);
                    }

                    Object[] row   = (Object[]) array[i];
                    Object   value = row[row.length - 1];
                    String string =
                        (String) Type.SQL_VARCHAR.convertToType(session,
                            value, exprDataType);

                    sb.append(string);
                }

                return sb.toString();
            }
            case OpTypes.MEDIAN : {
                SortAndSlice exprSort = new SortAndSlice();

                exprSort.prepareSingleColumn(1);
                arrayDataType.sort(session, array, exprSort);

                boolean even = array.length % 2 == 0;
                Object  value;

                if (even) {
                    SetFunctionValueAggregate sf =
                        new SetFunctionValueAggregate(session, OpTypes.AVG,
                                                      nodes[LEFT].dataType,
                                                      dataType, false);

                    sf.add(array[(array.length / 2) - 1]);
                    sf.add(array[(array.length / 2)]);

                    value = sf.getValue();
                } else {
                    value = array[array.length / 2];
                }

                if (dataType.isDateTimeTypeWithZone()) {
                    value = ((DateTimeType)dataType).changeZoneToUTC(value);
                }

                return dataType.convertToType(session, value, exprDataType);
            }
        }

        return null;
    }

    public Expression getCondition() {
        return condition;
    }

    public boolean hasCondition() {
        return !condition.isTrue();
    }

    public void setCondition(Expression e) {
        condition = e;
    }

    public Expression duplicate() {

        ExpressionArrayAggregate e =
            (ExpressionArrayAggregate) super.duplicate();

        e.condition = condition.duplicate();

        return e;
    }
}
