/* Copyright (c) 2001-2010, The HSQL Development Group
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
import org.hsqldb.lib.HsqlList;
import org.hsqldb.types.ArrayType;
import org.hsqldb.types.NumberType;
import org.hsqldb.types.RowType;
import org.hsqldb.types.Type;

/**
 * Implementation of array aggregate operations
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 2.0.1
 */
public class ExpressionArrayAggregate extends Expression {

    boolean      isDistinctAggregate;
    SortAndSlice sort;
    String       separator = ",";
    ArrayType    arrayDataType;
    Type         exprType;
    Expression   condition = Expression.EXPR_TRUE;

    ExpressionArrayAggregate(int type, boolean distinct, Expression e,
                             SortAndSlice sort, String separator) {

        super(type);

        this.isDistinctAggregate = distinct;
        this.sort                = sort;

        if (separator != null) {
            this.separator = separator;
        }

        if (type == OpTypes.MEDIAN) {
            nodes = new Expression[]{ e };

            return;
        }

        if (sort == null) {
            nodes = new Expression[]{ e };
        } else {
            HsqlArrayList list = sort.getExpressionList();

            nodes = new Expression[list.size() + 1];

            list.toArray(nodes);

            nodes[list.size()] = e;

            sort.prepare(1);
        }
    }

    boolean isSelfAggregate() {
        return true;
    }

    public String getSQL() {

        StringBuffer sb   = new StringBuffer(64);
        String       left = getContextSQL(nodes.length > 0 ? nodes[LEFT]
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

        StringBuffer sb = new StringBuffer(64);

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
        }

        if (getLeftNode() != null) {
            sb.append(" arg=[");
            sb.append(nodes[LEFT].describe(session, blanks + 1));
            sb.append(']');
        }

        return sb.toString();
    }

    public HsqlList resolveColumnReferences(Session session,
            RangeVariable[] rangeVarArray, int rangeCount,
            HsqlList unresolvedSet, boolean acceptsSequences) {

        HsqlList conditionSet = condition.resolveColumnReferences(session,
            rangeVarArray, rangeCount, null, false);

        if (conditionSet != null) {
            ExpressionColumn.checkColumnsResolved(conditionSet);
        }

        if (unresolvedSet == null) {
            unresolvedSet = new ArrayListIdentity();
        }

        unresolvedSet.add(this);

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

        exprType = nodes[nodes.length - 1].dataType;

        if (exprType.isLobType()) {
            throw Error.error(ErrorCode.X_42534);
        }

        if (exprType.isArrayType()) {
            throw Error.error(ErrorCode.X_42534);
        }

        if (opType == OpTypes.MEDIAN) {
            if (!exprType.isNumberType()) {
                throw Error.error(ErrorCode.X_42534);
            }
        }

        Type rowDataType = new RowType(nodeDataTypes);

        switch (opType) {

            case OpTypes.ARRAY_AGG :
                arrayDataType =
                    new ArrayType(rowDataType,
                                  ArrayType.defaultArrayCardinality);
                dataType = new ArrayType(exprType,
                                         ArrayType.defaultArrayCardinality);
                break;

            case OpTypes.GROUP_CONCAT :
                arrayDataType =
                    new ArrayType(rowDataType,
                                  ArrayType.defaultArrayCardinality);
                dataType = Type.SQL_VARCHAR_DEFAULT;
                break;

            case OpTypes.MEDIAN :
                arrayDataType =
                    new ArrayType(nodeDataTypes[0],
                                  ArrayType.defaultArrayCardinality);
                dataType = exprType;
                break;
        }

        condition.resolveTypes(session, null);
    }

    public boolean equals(Expression other) {

        if (!(other instanceof ExpressionArrayAggregate)) {
            return false;
        }

        ExpressionArrayAggregate o = (ExpressionArrayAggregate) other;

        if (opType == other.opType && exprSubType == other.exprSubType
                && isDistinctAggregate == o.isDistinctAggregate
                && separator.equals(o.separator)
                && condition.equals(o.condition)) {
            return super.equals(other);
        }

        return false;
    }

    public Object updateAggregatingValue(Session session, Object currValue) {

        if (!condition.testCondition(session)) {
            return currValue;
        }

        Object currentVal = null;

        switch (opType) {

            case OpTypes.ARRAY_AGG :
            case OpTypes.GROUP_CONCAT :
                Object[] row = new Object[nodes.length];

                for (int i = 0; i < nodes.length; i++) {
                    row[i] = nodes[i].getValue(session);
                }

                if (opType == OpTypes.GROUP_CONCAT
                        && row[row.length - 1] == null) {
                    return currValue;
                }

                currentVal = row;
                break;

            case OpTypes.MEDIAN :
                currentVal = nodes[0].getValue(session);
                break;
        }

        HsqlArrayList list = (HsqlArrayList) currValue;

        if (list == null) {
            list = new HsqlArrayList();
        }

        list.add(currentVal);

        return list;
    }

    public Object getAggregatedValue(Session session, Object currValue) {

        if (currValue == null) {
            return null;
        }

        HsqlArrayList list  = (HsqlArrayList) currValue;
        Object[]      array = list.toArray();

        if (isDistinctAggregate) {
            SortAndSlice exprSort = new SortAndSlice();

            exprSort.prepareSingleColumn(nodes.length - 1);
            arrayDataType.sort(session, array, exprSort);

            int size = arrayDataType.deDuplicate(session, array, exprSort);

            array = (Object[]) ArrayUtil.resizeArrayIfDifferent(array, size);
        }

        if (sort != null) {
            arrayDataType.sort(session, array, sort);
        }

        switch (opType) {

            case OpTypes.ARRAY_AGG : {
                Object[] resultArray = new Object[array.length];

                for (int i = 0; i < list.size(); i++) {
                    Object[] row = (Object[]) array[i];

                    resultArray[i] = row[row.length - 1];
                }

                return resultArray;
            }
            case OpTypes.GROUP_CONCAT : {
                StringBuffer sb = new StringBuffer(16 * list.size());

                for (int i = 0; i < array.length; i++) {
                    if (i > 0) {
                        sb.append(separator);
                    }

                    Object[] row = (Object[]) array[i];
                    String value =
                        exprType.convertToString(row[row.length - 1]);

                    sb.append(value);
                }

                return sb.toString();
            }
            case OpTypes.MEDIAN : {
                SortAndSlice exprSort = new SortAndSlice();

                exprSort.prepareSingleColumn(1);
                arrayDataType.sort(session, array, exprSort);

                boolean even = array.length % 2 == 0;

                if (even) {
                    Object val1 = array[(array.length / 2) - 1];
                    Object val2 = array[array.length / 2];
                    Object val3 = ((NumberType) exprType).add(val1, val2,
                        exprType);

                    return ((NumberType) exprType).divide(session, val3,
                                                          Integer.valueOf(2));
                } else {
                    return array[array.length / 2];
                }
            }
        }

        return null;
    }

    public Expression getCondition() {
        return condition;
    }

    public boolean hasCondition() {
        return condition != null && condition != Expression.EXPR_TRUE;
    }

    public void setCondition(Expression e) {
        condition = e;
    }
}
