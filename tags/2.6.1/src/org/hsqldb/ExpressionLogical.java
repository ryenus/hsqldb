/* Copyright (c) 2001-2021, The HSQL Development Group
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
import org.hsqldb.index.Index;
import org.hsqldb.lib.List;
import org.hsqldb.lib.OrderedIntHashSet;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.types.ArrayType;
import org.hsqldb.types.DTIType;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;

/**
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.1
 * @since 1.9.0
 */
public class ExpressionLogical extends Expression {

    boolean noOptimisation;
    boolean isQuantified;
    boolean isTerminal;

    /**
     * For LIKE
     */
    ExpressionLogical(int type) {

        super(type);

        dataType = Type.SQL_BOOLEAN;
    }

    /**
     * For boolean constants
     */
    ExpressionLogical(boolean b) {

        super(OpTypes.VALUE);

        dataType  = Type.SQL_BOOLEAN;
        valueData = b ? Boolean.TRUE
                      : Boolean.FALSE;
    }

    /*
     * Create an equality expressions using existing columns and
     * range variables. The expression is fully resolved in constructor.
     */
    ExpressionLogical(RangeVariable leftRangeVar, int colIndexLeft,
                      RangeVariable rightRangeVar, int colIndexRight) {

        super(OpTypes.EQUAL);

        ExpressionColumn leftExpression = new ExpressionColumn(leftRangeVar,
            colIndexLeft);
        ExpressionColumn rightExpression = new ExpressionColumn(rightRangeVar,
            colIndexRight);

        nodes        = new Expression[BINARY];
        nodes[LEFT]  = leftExpression;
        nodes[RIGHT] = rightExpression;

        setEqualityMode();

        dataType = Type.SQL_BOOLEAN;
    }

    /**
     * Creates an equality expression
     */
    ExpressionLogical(Expression left, Expression right) {

        super(OpTypes.EQUAL);

        nodes        = new Expression[BINARY];
        nodes[LEFT]  = left;
        nodes[RIGHT] = right;

        setEqualityMode();

        dataType = Type.SQL_BOOLEAN;
    }

    /**
     * Creates a binary operation expression
     */
    ExpressionLogical(int type, Expression left, Expression right) {

        super(type);

        nodes        = new Expression[BINARY];
        nodes[LEFT]  = left;
        nodes[RIGHT] = right;

        switch (opType) {

            case OpTypes.EQUAL :
            case OpTypes.GREATER_EQUAL :
            case OpTypes.GREATER_EQUAL_PRE :
            case OpTypes.GREATER :
            case OpTypes.SMALLER :
            case OpTypes.SMALLER_EQUAL :
                setEqualityMode();
                break;

            case OpTypes.NOT_EQUAL :
            case OpTypes.OVERLAPS :
            case OpTypes.RANGE_CONTAINS :
            case OpTypes.RANGE_EQUALS :
            case OpTypes.RANGE_OVERLAPS :
            case OpTypes.RANGE_IMMEDIATELY_PRECEDES :
            case OpTypes.RANGE_IMMEDIATELY_SUCCEEDS :
            case OpTypes.RANGE_PRECEDES :
            case OpTypes.RANGE_SUCCEEDS :
            case OpTypes.NOT_DISTINCT :
            case OpTypes.IN :
            case OpTypes.MATCH_SIMPLE :
            case OpTypes.MATCH_PARTIAL :
            case OpTypes.MATCH_FULL :
            case OpTypes.MATCH_UNIQUE_SIMPLE :
            case OpTypes.MATCH_UNIQUE_PARTIAL :
            case OpTypes.MATCH_UNIQUE_FULL :
            case OpTypes.AND :
            case OpTypes.OR :
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionLogical");
        }

        dataType = Type.SQL_BOOLEAN;
    }

    /**
     * Creates a modified LIKE comparison
     */
    ExpressionLogical(int type, Expression left, Expression right,
                      Expression end) {

        super(type);

        nodes        = new Expression[TERNARY];
        nodes[LEFT]  = left;
        nodes[RIGHT] = right;
        nodes[2]     = end;
    }

    /**
     * Creates a unary operation expression
     */
    ExpressionLogical(int type, Expression e) {

        super(type);

        nodes       = new Expression[UNARY];
        nodes[LEFT] = e;

        switch (opType) {

            case OpTypes.UNIQUE :
            case OpTypes.EXISTS :
            case OpTypes.IS_NULL :
            case OpTypes.IS_NOT_NULL :
            case OpTypes.NOT :
                dataType = Type.SQL_BOOLEAN;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionLogical");
        }

        if (opType == OpTypes.IS_NULL
                && nodes[LEFT].opType == OpTypes.COLUMN) {
            isSingleColumnNull = true;
        }

        if (opType == OpTypes.NOT && nodes[LEFT].isSingleColumnNull) {
            isSingleColumnNotNull = true;
        }
    }

    /**
     * Creates a column not null expression for check constraints
     */
    ExpressionLogical(ColumnSchema column) {

        super(OpTypes.NOT);

        nodes    = new Expression[UNARY];
        dataType = Type.SQL_BOOLEAN;

        Expression e = new ExpressionColumn(column);

        e           = new ExpressionLogical(OpTypes.IS_NULL, e);
        nodes[LEFT] = e;
    }

    void setEqualityMode() {

        if (nodes[LEFT].opType == OpTypes.COLUMN) {
            nodes[LEFT].nullability = SchemaObject.Nullability.NO_NULLS;

            switch (nodes[RIGHT].opType) {

                case OpTypes.COLUMN :
                    isColumnCondition = true;

                    if (opType == OpTypes.EQUAL) {
                        isColumnEqual = true;
                    }

                    nodes[RIGHT].nullability =
                        SchemaObject.Nullability.NO_NULLS;
                    break;

                case OpTypes.VALUE :
                case OpTypes.DYNAMIC_PARAM :
                case OpTypes.PARAMETER :
                case OpTypes.VARIABLE :
                    isSingleColumnCondition = true;

                    if (opType == OpTypes.EQUAL) {
                        isSingleColumnEqual = true;
                    }
                    break;

                default :
            }
        } else if (nodes[RIGHT].opType == OpTypes.COLUMN) {
            nodes[RIGHT].nullability = SchemaObject.Nullability.NO_NULLS;

            switch (nodes[LEFT].opType) {

                case OpTypes.VALUE :
                case OpTypes.DYNAMIC_PARAM :
                case OpTypes.PARAMETER :
                case OpTypes.VARIABLE :
                    isSingleColumnCondition = true;

                    if (opType == OpTypes.EQUAL) {
                        isSingleColumnEqual = true;
                    }
                    break;

                default :
            }
        }
    }

    /**
     * Creates a NOT NULL condition
     */
    static ExpressionLogical newNotNullCondition(Expression e) {

        e = new ExpressionLogical(OpTypes.IS_NULL, e);

        return new ExpressionLogical(OpTypes.NOT, e);
    }

    // logical ops
    static Expression andExpressions(Expression e1, Expression e2) {

        if (e1 == null) {
            return e2;
        }

        if (e2 == null) {
            return e1;
        }

        if (ExpressionLogical.EXPR_FALSE.equals(e1)
                || ExpressionLogical.EXPR_FALSE.equals(e2)) {
            return new ExpressionBoolean(false);
        }

        if (e1 == e2) {
            return e1;
        }

        return new ExpressionLogical(OpTypes.AND, e1, e2);
    }

    static Expression orExpressions(Expression e1, Expression e2) {

        if (e1 == null) {
            return e2;
        }

        if (e2 == null) {
            return e1;
        }

        if (e1 == e2) {
            return e1;
        }

        return new ExpressionLogical(OpTypes.OR, e1, e2);
    }

    public void addLeftColumnsForAllAny(RangeVariable range,
                                        OrderedIntHashSet set) {

        if (nodes.length == 0) {
            return;
        }

        for (int j = 0; j < nodes[LEFT].nodes.length; j++) {
            int index = nodes[LEFT].nodes[j].getColumnIndex();

            if (index < 0
                    || nodes[LEFT].nodes[j].getRangeVariable() != range) {
                set.clear();

                return;
            }

            set.add(index);
        }
    }

    public void setSubType(int type) {

        exprSubType = type;

        if (exprSubType == OpTypes.ALL_QUANTIFIED
                || exprSubType == OpTypes.ANY_QUANTIFIED) {
            isQuantified = true;
        }
    }

    public String getSQL() {

        StringBuilder sb = new StringBuilder(64);

        if (opType == OpTypes.VALUE) {
            return super.getSQL();
        }

        String left  = getContextSQL(nodes[LEFT]);
        String right = getContextSQL(nodes.length > 1 ? nodes[RIGHT]
                                                      : null);

        switch (opType) {

            case OpTypes.NOT :
                if (nodes[LEFT].opType == OpTypes.IS_NULL) {
                    sb.append(getContextSQL(nodes[LEFT].nodes[LEFT])).append(
                        ' ').append(Tokens.T_IS).append(' ').append(
                        Tokens.T_NOT).append(' ').append(Tokens.T_NULL);

                    return sb.toString();
                }

                if (nodes[LEFT].opType == OpTypes.NOT_DISTINCT) {
                    sb.append(getContextSQL(nodes[LEFT].nodes[LEFT])).append(
                        ' ').append(Tokens.T_IS).append(' ').append(
                        Tokens.T_DISTINCT).append(' ').append(
                        Tokens.T_FROM).append(' ').append(
                        getContextSQL(nodes[LEFT].nodes[RIGHT]));

                    return sb.toString();
                }

                sb.append(Tokens.T_NOT).append(' ').append(left);

                return sb.toString();

            case OpTypes.NOT_DISTINCT :
                sb.append(left).append(' ').append(Tokens.T_IS).append(
                    ' ').append(Tokens.T_NOT).append(' ').append(
                    Tokens.T_DISTINCT).append(' ').append(
                    Tokens.T_FROM).append(' ').append(right);

                return sb.toString();

            case OpTypes.IS_NULL :
                sb.append(left).append(' ').append(Tokens.T_IS).append(
                    ' ').append(Tokens.T_NULL);

                return sb.toString();

            case OpTypes.UNIQUE :
                sb.append(' ').append(Tokens.T_UNIQUE).append(' ');
                break;

            case OpTypes.EXISTS :
                sb.append(' ').append(Tokens.T_EXISTS).append(' ');
                break;

            case OpTypes.EQUAL :
                sb.append(left).append('=').append(right);

                return sb.toString();

            case OpTypes.GREATER_EQUAL :
            case OpTypes.GREATER_EQUAL_PRE :
                sb.append(left).append(">=").append(right);

                return sb.toString();

            case OpTypes.GREATER :
                sb.append(left).append('>').append(right);

                return sb.toString();

            case OpTypes.SMALLER :
                sb.append(left).append('<').append(right);

                return sb.toString();

            case OpTypes.SMALLER_EQUAL :
                sb.append(left).append("<=").append(right);

                return sb.toString();

            case OpTypes.NOT_EQUAL :
                if (Tokens.T_NULL.equals(right)) {
                    sb.append(left).append(" IS NOT ").append(right);
                } else {
                    sb.append(left).append("!=").append(right);
                }

                return sb.toString();

            case OpTypes.AND :
                sb.append(left).append(' ').append(Tokens.T_AND).append(
                    ' ').append(right);

                return sb.toString();

            case OpTypes.OR :
                sb.append(left).append(' ').append(Tokens.T_OR).append(
                    ' ').append(right);

                return sb.toString();

            case OpTypes.IN :
                sb.append(left).append(' ').append(Tokens.T_IN).append(
                    ' ').append(right);

                return sb.toString();

            case OpTypes.MATCH_SIMPLE :
                sb.append(left).append(' ').append(Tokens.T_MATCH).append(
                    ' ').append(right);

                return sb.toString();

            case OpTypes.MATCH_PARTIAL :
                sb.append(left).append(' ').append(Tokens.T_MATCH).append(
                    ' ').append(Tokens.PARTIAL).append(right);

                return sb.toString();

            case OpTypes.MATCH_FULL :
                sb.append(left).append(' ').append(Tokens.T_MATCH).append(
                    ' ').append(Tokens.FULL).append(right);

                return sb.toString();

            case OpTypes.MATCH_UNIQUE_SIMPLE :
                sb.append(left).append(' ').append(Tokens.T_MATCH).append(
                    ' ').append(Tokens.UNIQUE).append(right);

                return sb.toString();

            case OpTypes.MATCH_UNIQUE_PARTIAL :
                sb.append(left).append(' ').append(Tokens.T_MATCH).append(
                    ' ').append(Tokens.UNIQUE).append(' ').append(
                    Tokens.PARTIAL).append(right);

                return sb.toString();

            case OpTypes.MATCH_UNIQUE_FULL :
                sb.append(left).append(' ').append(Tokens.T_MATCH).append(
                    ' ').append(Tokens.UNIQUE).append(' ').append(
                    Tokens.FULL).append(right);

                return sb.toString();

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionLogical");
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

            case OpTypes.VALUE :
                sb.append("VALUE = ").append(
                    dataType.convertToSQLString(valueData));
                sb.append(", TYPE = ").append(dataType.getNameString());

                return sb.toString();

            case OpTypes.NOT :
                if (nodes[LEFT].opType == OpTypes.NOT_DISTINCT) {
                    sb.append(Tokens.T_DISTINCT);

                    return sb.toString();
                }

                sb.append(Tokens.T_NOT);
                break;

            case OpTypes.NOT_DISTINCT :
                sb.append(Tokens.T_NOT).append(' ').append(Tokens.T_DISTINCT);
                break;

            case OpTypes.EQUAL :
                sb.append("EQUAL");
                break;

            case OpTypes.GREATER_EQUAL :
            case OpTypes.GREATER_EQUAL_PRE :
                sb.append("GREATER_EQUAL");
                break;

            case OpTypes.GREATER :
                sb.append("GREATER");
                break;

            case OpTypes.SMALLER :
                sb.append("SMALLER");
                break;

            case OpTypes.SMALLER_EQUAL :
                sb.append("SMALLER_EQUAL");
                break;

            case OpTypes.NOT_EQUAL :
                sb.append("NOT_EQUAL");
                break;

            case OpTypes.AND :
                sb.append(Tokens.T_AND);
                break;

            case OpTypes.OR :
                sb.append(Tokens.T_OR);
                break;

            case OpTypes.MATCH_SIMPLE :
            case OpTypes.MATCH_PARTIAL :
            case OpTypes.MATCH_FULL :
            case OpTypes.MATCH_UNIQUE_SIMPLE :
            case OpTypes.MATCH_UNIQUE_PARTIAL :
            case OpTypes.MATCH_UNIQUE_FULL :
                sb.append(Tokens.T_MATCH);
                break;

            case OpTypes.IS_NULL :
                sb.append(Tokens.T_IS).append(' ').append(Tokens.T_NULL);
                break;

            case OpTypes.UNIQUE :
                sb.append(Tokens.T_UNIQUE);
                break;

            case OpTypes.EXISTS :
                sb.append(Tokens.T_EXISTS);
                break;

            case OpTypes.OVERLAPS :
                sb.append(Tokens.T_OVERLAPS);
                break;

            case OpTypes.RANGE_CONTAINS :
                sb.append(Tokens.T_CONTAINS);
                break;

            case OpTypes.RANGE_EQUALS :
                sb.append(Tokens.T_EQUALS);
                break;

            case OpTypes.RANGE_OVERLAPS :
                sb.append(Tokens.T_OVERLAPS);
                break;

            case OpTypes.RANGE_PRECEDES :
                sb.append(Tokens.T_PRECEDES);
                break;

            case OpTypes.RANGE_SUCCEEDS :
                sb.append(Tokens.T_SUCCEEDS);
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionLogical");
        }

        if (getLeftNode() != null) {
            sb.append(" arg_left=[");
            sb.append(nodes[LEFT].describe(session, blanks + 1));
            sb.append(']');
        }

        if (getRightNode() != null) {
            sb.append(" arg_right=[");
            sb.append(nodes[RIGHT].describe(session, blanks + 1));
            sb.append(']');
        }

        return sb.toString();
    }

    public void resolveTypes(Session session, Expression parent) {

        // parametric ALL / ANY
        if (isQuantified) {
            if (nodes[RIGHT].opType == OpTypes.TABLE) {
                if (nodes[RIGHT] instanceof ExpressionTable) {
                    if (nodes[RIGHT].nodes[LEFT].opType
                            == OpTypes.DYNAMIC_PARAM) {
                        nodes[LEFT].resolveTypes(session, this);

                        nodes[RIGHT].nodes[LEFT].dataType = new ArrayType(
                            nodes[LEFT].dataType,
                            ArrayType.defaultLargeArrayCardinality);
                    }
                }
            }
        }

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        switch (opType) {

            case OpTypes.VALUE :
                break;

            case OpTypes.NOT_DISTINCT :
                changeToRowExpression(LEFT);
                changeToRowExpression(RIGHT);
                resolveRowTypes();
                checkRowComparison();
                break;

            case OpTypes.EQUAL :
            case OpTypes.GREATER_EQUAL :
            case OpTypes.GREATER_EQUAL_PRE :
            case OpTypes.GREATER :
            case OpTypes.SMALLER :
            case OpTypes.SMALLER_EQUAL :
            case OpTypes.NOT_EQUAL :
                resolveTypesForComparison(session, parent);
                break;

            case OpTypes.AND : {
                resolveTypesForLogicalOp();

                if (nodes[LEFT].opType == OpTypes.VALUE) {
                    if (nodes[RIGHT].opType == OpTypes.VALUE) {
                        setAsConstantValue(session, parent);
                    } else {
                        Object value = nodes[LEFT].getValue(session);

                        if (value == null || Boolean.FALSE.equals(value)) {
                            setAsConstantValue(Boolean.FALSE, parent);
                        }
                    }
                } else if (nodes[RIGHT].opType == OpTypes.VALUE) {
                    Object value = nodes[RIGHT].getValue(session);

                    if (value == null || Boolean.FALSE.equals(value)) {
                        setAsConstantValue(Boolean.FALSE, parent);
                    }
                }

                break;
            }
            case OpTypes.OR : {
                resolveTypesForLogicalOp();

                if (nodes[LEFT].opType == OpTypes.VALUE) {
                    if (nodes[RIGHT].opType == OpTypes.VALUE) {
                        setAsConstantValue(session, parent);
                    } else {
                        Object value = nodes[LEFT].getValue(session);

                        if (Boolean.TRUE.equals(value)) {
                            setAsConstantValue(Boolean.TRUE, parent);
                        }
                    }
                } else if (nodes[RIGHT].opType == OpTypes.VALUE) {
                    Object value = nodes[RIGHT].getValue(session);

                    if (Boolean.TRUE.equals(value)) {
                        setAsConstantValue(Boolean.TRUE, parent);
                    }
                }

                break;
            }
            case OpTypes.IS_NOT_NULL :
            case OpTypes.IS_NULL : {
                switch (nodes[LEFT].opType) {

                    case OpTypes.ROW : {
                        Expression[] sourceNodes = nodes[LEFT].nodes;
                        Expression   result      = null;

                        for (int i = 0; i < sourceNodes.length; i++) {
                            Expression node;

                            node = new ExpressionLogical(OpTypes.IS_NULL,
                                                         sourceNodes[i]);

                            if (opType == OpTypes.IS_NOT_NULL) {
                                node = new ExpressionLogical(OpTypes.NOT,
                                                             node);
                            }

                            result = andExpressions(result, node);
                        }

                        opType = OpTypes.AND;
                        nodes  = result.nodes;

                        resolveTypes(session, parent);

                        break;
                    }
                    case OpTypes.ROW_SUBQUERY :
                    case OpTypes.TABLE_SUBQUERY : {
                        break;
                    }
                    default : {
                        if (nodes[LEFT].isUnresolvedParam()) {
                            if (session.database.sqlEnforceTypes) {
                                throw Error.error(ErrorCode.X_42563);
                            }

                            nodes[LEFT].dataType = Type.SQL_VARCHAR_DEFAULT;
                        }

                        if (opType == OpTypes.IS_NOT_NULL) {
                            Expression node;

                            node = new ExpressionLogical(OpTypes.IS_NULL,
                                                         nodes[LEFT]);
                            nodes[LEFT] = node;
                            opType      = OpTypes.NOT;

                            resolveTypes(session, parent);

                            break;
                        }

                        if (nodes[LEFT].opType == OpTypes.VALUE) {
                            setAsConstantValue(session, parent);
                        }

                        break;
                    }
                }

                break;
            }
            case OpTypes.NOT : {
                if (nodes[LEFT].isUnresolvedParam()) {
                    nodes[LEFT].dataType = Type.SQL_BOOLEAN;

                    break;
                }

                if (nodes[LEFT].opType == OpTypes.VALUE) {
                    if (nodes[LEFT].dataType.isBooleanType()) {
                        setAsConstantValue(session, parent);

                        break;
                    } else {
                        throw Error.error(ErrorCode.X_42563);
                    }
                }

                if (nodes[LEFT].dataType == null
                        || !nodes[LEFT].dataType.isBooleanType()) {
                    throw Error.error(ErrorCode.X_42563);
                }

                dataType = Type.SQL_BOOLEAN;

                break;
            }
            case OpTypes.OVERLAPS :
                resolveTypesForOverlaps();
                break;

            case OpTypes.IN :
                resolveTypesForIn(session);
                break;

            case OpTypes.MATCH_SIMPLE :
            case OpTypes.MATCH_PARTIAL :
            case OpTypes.MATCH_FULL :
            case OpTypes.MATCH_UNIQUE_SIMPLE :
            case OpTypes.MATCH_UNIQUE_PARTIAL :
            case OpTypes.MATCH_UNIQUE_FULL :
                resolveTypesForAllAny(session);
                break;

            case OpTypes.UNIQUE :

                // create the full index before meterialization
                // needed for VALUES expression
                nodes[LEFT].table.getFullIndex(session);
                break;

            case OpTypes.EXISTS :
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionLogical");
        }
    }

    private void resolveTypesForLogicalOp() {

        if (nodes[LEFT].isUnresolvedParam()) {
            nodes[LEFT].dataType = Type.SQL_BOOLEAN;
        }

        if (nodes[RIGHT].isUnresolvedParam()) {
            nodes[RIGHT].dataType = Type.SQL_BOOLEAN;
        }

        if (nodes[LEFT].dataType == null || nodes[RIGHT].dataType == null) {
            throw Error.error(ErrorCode.X_42571);
        }

        if (nodes[LEFT].opType == OpTypes.ROW
                || nodes[RIGHT].opType == OpTypes.ROW) {
            throw Error.error(ErrorCode.X_42565);
        }

        if (Type.SQL_BOOLEAN != nodes[LEFT].dataType
                || Type.SQL_BOOLEAN != nodes[RIGHT].dataType) {
            throw Error.error(ErrorCode.X_42568);
        }
    }

    private void resolveTypesForComparison(Session session,
                                           Expression parent) {

        if (exprSubType == OpTypes.ALL_QUANTIFIED
                || exprSubType == OpTypes.ANY_QUANTIFIED) {
            resolveTypesForAllAny(session);
            checkRowComparison();

            return;
        }

        int leftDegree  = nodes[LEFT].getDegree();
        int rightDegree = nodes[RIGHT].getDegree();

        if (leftDegree > 1 || rightDegree > 1) {
            if (leftDegree != rightDegree) {
                throw Error.error(ErrorCode.X_42564);
            }

            resolveRowTypes();
            checkRowComparison();

            return;
        } else {
            if (nodes[LEFT].isUnresolvedParam()) {
                nodes[LEFT].dataType = nodes[RIGHT].dataType;
            } else if (nodes[RIGHT].isUnresolvedParam()) {
                nodes[RIGHT].dataType = nodes[LEFT].dataType;
            }

            if (nodes[LEFT].dataType == null) {
                nodes[LEFT].dataType = nodes[RIGHT].dataType;
            } else if (nodes[RIGHT].dataType == null) {
                nodes[RIGHT].dataType = nodes[LEFT].dataType;
            }

            if (nodes[LEFT].dataType == null
                    || nodes[RIGHT].dataType == null) {
                throw Error.error(ErrorCode.X_42567);
            }

            if (!nodes[LEFT].dataType.canCompareDirect(
                    nodes[RIGHT].dataType)) {
                if (convertDateTime(session)) {

                    // compatibility for BIT with number and BOOLEAN - convert bit to other type
                } else if (nodes[LEFT].dataType.isBitType()
                           || nodes[LEFT].dataType.isBooleanType()) {
                    if (session.database.sqlEnforceTypes) {
                        throw Error.error(ErrorCode.X_42562);
                    }

                    if (nodes[LEFT].dataType.canConvertFrom(
                            nodes[RIGHT].dataType)) {
                        nodes[RIGHT] = ExpressionOp.getCastExpression(session,
                                nodes[RIGHT], nodes[LEFT].dataType);
                    }
                } else if (nodes[RIGHT].dataType.isBitType()
                           || nodes[RIGHT].dataType.isBooleanType()) {
                    if (session.database.sqlEnforceTypes) {
                        throw Error.error(ErrorCode.X_42562);
                    }

                    if (nodes[RIGHT].dataType.canConvertFrom(
                            nodes[LEFT].dataType)) {
                        nodes[LEFT] = ExpressionOp.getCastExpression(session,
                                nodes[LEFT], nodes[RIGHT].dataType);
                    }
                } else if (nodes[LEFT].dataType.isNumberType()) {
                    if (session.database.sqlEnforceTypes) {
                        throw Error.error(ErrorCode.X_42562);
                    }

                    if (nodes[LEFT].dataType.canConvertFrom(
                            nodes[RIGHT].dataType)) {
                        nodes[RIGHT] = ExpressionOp.getCastExpression(session,
                                nodes[RIGHT], nodes[LEFT].dataType);
                    }
                } else if (nodes[RIGHT].dataType.isNumberType()) {
                    if (session.database.sqlEnforceTypes) {
                        throw Error.error(ErrorCode.X_42562);
                    }

                    if (nodes[RIGHT].dataType.canConvertFrom(
                            nodes[LEFT].dataType)) {
                        nodes[LEFT] = ExpressionOp.getCastExpression(session,
                                nodes[LEFT], nodes[RIGHT].dataType);
                    }
                } else if (nodes[LEFT].dataType.isDateTimeType()) {
                    if (nodes[LEFT].dataType.isDateTimeTypeWithZone()
                            ^ nodes[RIGHT].dataType.isDateTimeTypeWithZone()) {
                        nodes[LEFT] = new ExpressionOp(nodes[LEFT]);
                    }
                } else if (nodes[LEFT].dataType.canConvertFrom(
                        nodes[RIGHT].dataType)) {
                    nodes[RIGHT] = ExpressionOp.getCastExpression(session,
                            nodes[RIGHT], nodes[LEFT].dataType);
                } else {
                    throw Error.error(ErrorCode.X_42562);
                }
            }

            if (opType == OpTypes.EQUAL || opType == OpTypes.NOT_EQUAL) {

                //
            } else {
                if (nodes[LEFT].dataType.isArrayType()
                        || nodes[LEFT].dataType.isLobType()
                        || nodes[RIGHT].dataType.isLobType()) {
                    throw Error.error(ErrorCode.X_42534);
                }
            }

            if (nodes[LEFT].opType == OpTypes.ROWNUM
                    && nodes[RIGHT].opType == OpTypes.VALUE) {
                isTerminal = true;
            }

            if (nodes[LEFT].dataType.typeComparisonGroup
                    != nodes[RIGHT].dataType.typeComparisonGroup) {
                throw Error.error(ErrorCode.X_42562);
            }

            if (nodes[LEFT].opType == OpTypes.VALUE
                    && nodes[RIGHT].opType == OpTypes.VALUE) {
                setAsConstantValue(session, parent);
            } else if (session.database.sqlSyntaxDb2) {
                if (nodes[LEFT].dataType.typeComparisonGroup
                        == Types.SQL_VARCHAR) {
                    if (nodes[LEFT].opType == OpTypes.VALUE) {
                        nodes[RIGHT].dataType.convertToTypeLimits(session,
                                nodes[LEFT].valueData);
                    }

                    if (nodes[RIGHT].opType == OpTypes.VALUE) {
                        nodes[LEFT].dataType.convertToTypeLimits(session,
                                nodes[RIGHT].valueData);
                    }
                }
            }
        }
    }

    private void changeToRowExpression(int nodeIndex) {

        switch (nodes[nodeIndex].opType) {

            case OpTypes.ROW :
            case OpTypes.ROW_SUBQUERY :
                break;

            default :
                nodes[nodeIndex] = new Expression(OpTypes.ROW,
                                                  new Expression[]{
                                                      nodes[nodeIndex] });
                nodes[nodeIndex].nodeDataTypes = new Type[]{
                    nodes[nodeIndex].nodes[0].dataType };
        }
    }

    private void resolveRowTypes() {

        for (int i = 0; i < nodes[LEFT].nodeDataTypes.length; i++) {
            Type leftType  = nodes[LEFT].nodeDataTypes[i];
            Type rightType = nodes[RIGHT].nodeDataTypes[i];

            if (leftType == null) {
                leftType = nodes[LEFT].nodeDataTypes[i] = rightType;
            } else if (nodes[RIGHT].dataType == null) {
                rightType = nodes[RIGHT].nodeDataTypes[i] = leftType;
            }

            if (leftType == null || rightType == null) {
                throw Error.error(ErrorCode.X_42567);
            }

            if (leftType.typeComparisonGroup
                    != rightType.typeComparisonGroup) {
                throw Error.error(ErrorCode.X_42562);
            } else if (leftType.isDateTimeType()) {
                if (leftType.isDateTimeTypeWithZone()
                        ^ rightType.isDateTimeTypeWithZone()) {
                    nodes[LEFT].nodes[i] =
                        new ExpressionOp(nodes[LEFT].nodes[i]);
                    nodes[LEFT].nodeDataTypes[i] =
                        nodes[LEFT].nodes[i].dataType;
                }
            }
        }
    }

    void checkRowComparison() {

        if (opType == OpTypes.EQUAL || opType == OpTypes.NOT_EQUAL) {
            return;
        }

        for (int i = 0; i < nodes[LEFT].nodeDataTypes.length; i++) {
            Type leftType  = nodes[LEFT].nodeDataTypes[i];
            Type rightType = nodes[RIGHT].nodeDataTypes[i];

            if (leftType.isArrayType() || leftType.isLobType()
                    || rightType.isLobType()) {
                throw Error.error(ErrorCode.X_42534);
            }
        }
    }

    /**
     * for compatibility, convert a datetime character string to a datetime
     * value for comparison
     */
    private boolean convertDateTime(Session session) {

        int a = LEFT;
        int b = RIGHT;

        if (nodes[a].dataType.isDateTimeType()) {

            //
        } else if (nodes[b].dataType.isDateTimeType()) {
            a = RIGHT;
            b = LEFT;
        } else {
            return false;
        }

        if (nodes[a].dataType.isDateTimeTypeWithZone()) {
            return false;
        }

        if (nodes[b].dataType.isCharacterType()) {
            if (nodes[b].opType == OpTypes.VALUE) {
                try {
                    nodes[b].valueData = nodes[a].dataType.castToType(session,
                            nodes[b].valueData, nodes[b].dataType);
                    nodes[b].dataType = nodes[a].dataType;
                } catch (HsqlException e) {
                    if (nodes[a].dataType == Type.SQL_DATE) {
                        nodes[b].valueData =
                            Type.SQL_TIMESTAMP.castToType(session,
                                                          nodes[b].valueData,
                                                          nodes[b].dataType);
                        nodes[b].dataType = Type.SQL_TIMESTAMP;
                    }
                }

                return true;
            } else {
                nodes[b] = new ExpressionOp(nodes[b], nodes[a].dataType);

                nodes[b].resolveTypes(session, this);

                return true;
            }
        }

        return false;
    }

    void resolveTypesForOverlaps() {

        if (nodes[LEFT].nodes[0].isUnresolvedParam()) {
            nodes[LEFT].nodes[0].dataType = nodes[RIGHT].nodes[0].dataType;
        }

        if (nodes[RIGHT].nodes[0].isUnresolvedParam()) {
            nodes[RIGHT].nodes[0].dataType = nodes[LEFT].nodes[0].dataType;
        }

        if (nodes[LEFT].nodes[0].dataType == null) {
            nodes[LEFT].nodes[0].dataType  = Type.SQL_TIMESTAMP;
            nodes[RIGHT].nodes[0].dataType = Type.SQL_TIMESTAMP;
        }

        if (nodes[LEFT].nodes[1].isUnresolvedParam()) {
            nodes[LEFT].nodes[1].dataType = nodes[RIGHT].nodes[0].dataType;
        }

        if (nodes[RIGHT].nodes[1].isUnresolvedParam()) {
            nodes[RIGHT].nodes[1].dataType = nodes[LEFT].nodes[0].dataType;
        }

        if (!DTIType.isValidDatetimeRange(nodes[LEFT].nodes[0].dataType,
                                          nodes[LEFT].nodes[1].dataType)) {
            throw Error.error(ErrorCode.X_42563);
        }

        if (!DTIType.isValidDatetimeRange(nodes[RIGHT].nodes[0].dataType,
                                          nodes[RIGHT].nodes[1].dataType)) {
            throw Error.error(ErrorCode.X_42563);
        }

        nodes[LEFT].nodeDataTypes[0]  = nodes[LEFT].nodes[0].dataType;
        nodes[LEFT].nodeDataTypes[1]  = nodes[LEFT].nodes[1].dataType;
        nodes[RIGHT].nodeDataTypes[0] = nodes[RIGHT].nodes[0].dataType;
        nodes[RIGHT].nodeDataTypes[1] = nodes[RIGHT].nodes[1].dataType;
    }

    void resolveTypesForAllAny(Session session) {

        int degree = nodes[LEFT].getDegree();

        if (degree == 1 && nodes[LEFT].opType != OpTypes.ROW) {
            nodes[LEFT] = new Expression(OpTypes.ROW,
                                         new Expression[]{ nodes[LEFT] });
        }

        if (nodes[RIGHT].opType == OpTypes.VALUELIST) {
            nodes[RIGHT].prepareTable(session, nodes[LEFT], degree);
            nodes[RIGHT].table.prepareTable(session);
        }

        // encounterd in system generated MATCH predicates
        if (nodes[RIGHT].nodeDataTypes == null) {
            nodes[RIGHT].prepareTable(session, nodes[LEFT], degree);
        }

        if (degree != nodes[RIGHT].nodeDataTypes.length) {
            throw Error.error(ErrorCode.X_42564);
        }

        if (nodes[RIGHT].opType == OpTypes.VALUELIST) {}

        if (nodes[LEFT].nodeDataTypes == null) {
            nodes[LEFT].nodeDataTypes = new Type[nodes[LEFT].nodes.length];
        }

        for (int i = 0; i < nodes[LEFT].nodeDataTypes.length; i++) {
            Type type = nodes[LEFT].nodes[i].dataType;

            if (type == null) {
                type = nodes[RIGHT].nodeDataTypes[i];
            }

            if (type == null) {
                throw Error.error(ErrorCode.X_42567);
            }

            if (type.typeComparisonGroup
                    != nodes[RIGHT].nodeDataTypes[i].typeComparisonGroup) {
                throw Error.error(ErrorCode.X_42563);
            }

            nodes[LEFT].nodeDataTypes[i]  = type;
            nodes[LEFT].nodes[i].dataType = type;
        }
    }

    void resolveTypesForIn(Session session) {
        resolveTypesForAllAny(session);
    }

    public Object getValue(Session session) {

        switch (opType) {

            case OpTypes.VALUE :
                return valueData;

            case OpTypes.NEGATE :
                return dataType.negate(nodes[LEFT].getValue(session,
                        nodes[LEFT].dataType));

            case OpTypes.IS_NOT_NULL :
            case OpTypes.IS_NULL : {
                switch (nodes[LEFT].opType) {

                    case OpTypes.ROW :
                    case OpTypes.ROW_SUBQUERY :
                    case OpTypes.TABLE_SUBQUERY : {
                        Object[] values = nodes[LEFT].getRowValue(session);

                        for (int i = 0; i < values.length; i++) {
                            if (values[i] == null) {
                                if (opType == OpTypes.IS_NOT_NULL) {
                                    return Boolean.FALSE;
                                }
                            } else {
                                if (opType == OpTypes.IS_NULL) {
                                    return Boolean.FALSE;
                                }
                            }
                        }

                        return Boolean.TRUE;
                    }
                }

                return nodes[LEFT].getValue(session) == null ? Boolean.TRUE
                                                             : Boolean.FALSE;
            }
            case OpTypes.OVERLAPS : {
                Object[] left  = nodes[LEFT].getRowValue(session);
                Object[] right = nodes[RIGHT].getRowValue(session);

                return DateTimeType.overlaps(session, left,
                                             nodes[LEFT].nodeDataTypes, right,
                                             nodes[RIGHT].nodeDataTypes);
            }
            case OpTypes.IN : {
                return testInCondition(session);
            }
            case OpTypes.MATCH_SIMPLE :
            case OpTypes.MATCH_PARTIAL :
            case OpTypes.MATCH_FULL :
            case OpTypes.MATCH_UNIQUE_SIMPLE :
            case OpTypes.MATCH_UNIQUE_PARTIAL :
            case OpTypes.MATCH_UNIQUE_FULL : {
                return testMatchCondition(session);
            }
            case OpTypes.NOT_DISTINCT :
                return testNotDistinctCondition(session);

            case OpTypes.UNIQUE : {
                nodes[LEFT].materialise(session);

                return nodes[LEFT].table.hasUniqueNotNullRows(session)
                       ? Boolean.TRUE
                       : Boolean.FALSE;
            }
            case OpTypes.EXISTS : {
                return testExistsCondition(session);
            }
            case OpTypes.NOT : {
                Boolean result = (Boolean) nodes[LEFT].getValue(session);

                return result == null ? null
                                      : result.booleanValue() ? Boolean.FALSE
                                                              : Boolean.TRUE;
            }
            case OpTypes.AND : {
                Boolean r1 = (Boolean) nodes[LEFT].getValue(session);

                if (Boolean.FALSE.equals(r1)) {
                    return Boolean.FALSE;
                }

                Boolean r2 = (Boolean) nodes[RIGHT].getValue(session);

                if (Boolean.FALSE.equals(r2)) {
                    return Boolean.FALSE;
                }

                if (r1 == null || r2 == null) {
                    return null;
                }

                return Boolean.TRUE;
            }
            case OpTypes.OR : {
                Boolean r1 = (Boolean) nodes[LEFT].getValue(session);

                if (Boolean.TRUE.equals(r1)) {
                    return Boolean.TRUE;
                }

                Boolean r2 = (Boolean) nodes[RIGHT].getValue(session);

                if (Boolean.TRUE.equals(r2)) {
                    return Boolean.TRUE;
                }

                if (r1 == null || r2 == null) {
                    return null;
                }

                return Boolean.FALSE;
            }
            case OpTypes.EQUAL :
            case OpTypes.GREATER :
            case OpTypes.GREATER_EQUAL :
            case OpTypes.SMALLER_EQUAL :
            case OpTypes.GREATER_EQUAL_PRE :
            case OpTypes.SMALLER :
            case OpTypes.NOT_EQUAL : {
                if (exprSubType == OpTypes.ANY_QUANTIFIED
                        || exprSubType == OpTypes.ALL_QUANTIFIED) {
                    return testAllAnyCondition(session);
                }

                Object o1 = nodes[LEFT].getValue(session);
                Object o2 = nodes[RIGHT].getValue(session);

                if (nodes[LEFT].dataType != null
                        && nodes[LEFT].dataType.isArrayType()) {
                    return compareValues(session, o1, o2);
                }

                if (o1 instanceof Object[]) {
                    if (o2 != null && !(o2 instanceof Object[])) {
                        throw Error.runtimeError(ErrorCode.U_S0500,
                                                 "ExpressionLogical");
                    }

                    return compareValues(session, (Object[]) o1,
                                         (Object[]) o2);
                } else {
                    if (o2 instanceof Object[]) {
                        o2 = ((Object[]) o2)[0];
                    }

                    return compareValues(session, o1, o2);
                }
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionLogical");
        }
    }

    /**
     * For MATCH SIMPLE and FULL expressions, nulls in left are handled
     * prior to calling this method
     */
    private Boolean compareValues(Session session, Object left, Object right) {

        int result;

        if (left == null || right == null) {
            return null;
        }

        result = nodes[LEFT].dataType.compare(session, left, right, opType);

        switch (opType) {

            case OpTypes.EQUAL :
                return result == 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case OpTypes.NOT_EQUAL :
                return result != 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case OpTypes.GREATER :
                return result > 0 ? Boolean.TRUE
                                  : Boolean.FALSE;

            case OpTypes.GREATER_EQUAL :
            case OpTypes.GREATER_EQUAL_PRE :
                return result >= 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case OpTypes.SMALLER_EQUAL :
                return result <= 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case OpTypes.SMALLER :
                return result < 0 ? Boolean.TRUE
                                  : Boolean.FALSE;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionLogical");
        }
    }

    /**
     * For MATCH SIMPLE and FULL expressions, nulls in left are handled
     * prior to calling this method
     */
    private Boolean matchValues(Session session, Object[] leftData,
                                Object[] rightData) {

        Type[] types  = nodes[LEFT].nodeDataTypes;
        int    result = 0;

        switch (opType) {

            case OpTypes.MATCH_SIMPLE :
            case OpTypes.MATCH_UNIQUE_SIMPLE :
            case OpTypes.MATCH_PARTIAL :
            case OpTypes.MATCH_UNIQUE_PARTIAL :
            case OpTypes.MATCH_FULL :
            case OpTypes.MATCH_UNIQUE_FULL :
            case OpTypes.NOT_DISTINCT :
                for (int i = 0; i < nodes[LEFT].nodes.length; i++) {
                    Object leftValue  = leftData[i];
                    Object rightValue = rightData[i];

                    if (leftValue == null) {
                        if (opType == OpTypes.MATCH_PARTIAL
                                || opType == OpTypes.MATCH_UNIQUE_PARTIAL) {
                            continue;
                        }
                    }

                    result = types[i].compare(session, leftValue, rightValue);

                    if (result != 0) {
                        break;
                    }
                }

                return result == 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionLogical");
        }
    }

    private Boolean compareValues(Session session, Object[] leftData,
                                  Object[] rightData) {

        Type[]  types   = nodes[LEFT].nodeDataTypes;
        int     result  = 0;
        boolean hasNull = false;

        if (leftData == null || rightData == null) {
            return null;
        }

        for (int i = 0; i < nodes[LEFT].nodes.length; i++) {
            Object leftValue  = leftData[i];
            Object rightValue = rightData[i];

            if (leftValue == null || rightValue == null) {
                hasNull = true;

                continue;
            }

            result = types[i].compare(session, leftValue, rightValue);

            if (result != 0) {
                break;
            }
        }

        switch (opType) {

            case OpTypes.IN :
            case OpTypes.EQUAL :
                if (result == 0) {
                    if (hasNull) {
                        return null;
                    }

                    return Boolean.TRUE;
                } else {
                    return Boolean.FALSE;
                }
            case OpTypes.NOT_EQUAL :
                if (result == 0) {
                    if (hasNull) {
                        return null;
                    }

                    return Boolean.FALSE;
                } else {
                    return Boolean.TRUE;
                }
            case OpTypes.GREATER :
                if (hasNull) {
                    return null;
                }

                return result > 0 ? Boolean.TRUE
                                  : Boolean.FALSE;

            case OpTypes.GREATER_EQUAL :
            case OpTypes.GREATER_EQUAL_PRE :
                if (hasNull) {
                    return null;
                }

                return result >= 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case OpTypes.SMALLER_EQUAL :
                if (hasNull) {
                    return null;
                }

                return result <= 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case OpTypes.SMALLER :
                if (hasNull) {
                    return null;
                }

                return result < 0 ? Boolean.TRUE
                                  : Boolean.FALSE;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionLogical");
        }
    }

    /**
     * Returns the result of testing a VALUE_LIST expression
     */
    private Boolean testInCondition(Session session) {

        Object[] data = nodes[LEFT].getRowValue(session);

        if (data == null) {
            return null;
        }

        if (Expression.countNulls(data) != 0) {
            return null;
        }

        if (nodes[RIGHT].opType == OpTypes.VALUELIST) {
            final int length = nodes[RIGHT].nodes.length;

            for (int i = 0; i < length; i++) {
                Object[] rowData = nodes[RIGHT].nodes[i].getRowValue(session);

                if (Boolean.TRUE.equals(compareValues(session, data,
                                                      rowData))) {
                    return Boolean.TRUE;
                }
            }

            return Boolean.FALSE;
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "ExpressionLogical");
    }

    private Boolean testNotDistinctCondition(Session session) {

        Object[] leftData  = nodes[LEFT].getRowValue(session);
        Object[] rightData = nodes[RIGHT].getRowValue(session);

        if (leftData == null || rightData == null) {
            return leftData == rightData;
        }

        return matchValues(session, leftData, rightData);
    }

    private Boolean testMatchCondition(Session session) {

        Object[] data = nodes[LEFT].getRowValue(session);

        if (data == null) {
            return Boolean.TRUE;
        }

        final int nulls = countNulls(data);

        if (nulls != 0) {
            switch (opType) {

                case OpTypes.MATCH_SIMPLE :
                case OpTypes.MATCH_UNIQUE_SIMPLE :
                    return Boolean.TRUE;

                case OpTypes.MATCH_PARTIAL :
                case OpTypes.MATCH_UNIQUE_PARTIAL :
                    if (nulls == data.length) {
                        return Boolean.TRUE;
                    }
                    break;

                case OpTypes.MATCH_FULL :
                case OpTypes.MATCH_UNIQUE_FULL :
                    return nulls == data.length ? Boolean.TRUE
                                                : Boolean.FALSE;
            }
        }

        switch (nodes[RIGHT].opType) {

            case OpTypes.VALUELIST : {
                final int length   = nodes[RIGHT].nodes.length;
                boolean   hasMatch = false;

                for (int i = 0; i < length; i++) {
                    Object[] rowData =
                        nodes[RIGHT].nodes[i].getRowValue(session);
                    Boolean result = matchValues(session, data, rowData);

                    if (!result.booleanValue()) {
                        continue;
                    }

                    switch (opType) {

                        case OpTypes.MATCH_SIMPLE :
                        case OpTypes.MATCH_PARTIAL :
                        case OpTypes.MATCH_FULL :
                            return Boolean.TRUE;

                        case OpTypes.MATCH_UNIQUE_SIMPLE :
                        case OpTypes.MATCH_UNIQUE_PARTIAL :
                        case OpTypes.MATCH_UNIQUE_FULL :
                            if (hasMatch) {
                                return Boolean.FALSE;
                            }

                            hasMatch = true;
                            break;

                        default :
                    }
                }

                return hasMatch ? Boolean.TRUE
                                : Boolean.FALSE;
            }
            case OpTypes.TABLE_SUBQUERY : {
                PersistentStore store =
                    nodes[RIGHT].getTable().getRowStore(session);

                nodes[RIGHT].materialise(session);
                convertToType(session, data, nodes[LEFT].nodeDataTypes,
                              nodes[RIGHT].nodeDataTypes);

                if (nulls != 0
                        && (opType == OpTypes.MATCH_PARTIAL
                            || opType == OpTypes.MATCH_UNIQUE_PARTIAL)) {
                    boolean hasMatch = false;
                    RowIterator it =
                        nodes[RIGHT].getTable().rowIterator(session);

                    while (it.next()) {
                        Object[] rowData = it.getCurrent();
                        Boolean  result  = matchValues(session, data, rowData);

                        if (result == null) {
                            continue;
                        }

                        if (result.booleanValue()) {
                            if (opType == OpTypes.MATCH_PARTIAL) {
                                return Boolean.TRUE;
                            }

                            if (hasMatch) {
                                return Boolean.FALSE;
                            }

                            hasMatch = true;
                        }
                    }

                    return hasMatch ? Boolean.TRUE
                                    : Boolean.FALSE;
                }

                // now nulls == 0;
                RowIterator it = nodes[RIGHT].getTable().getFullIndex(
                    session).findFirstRow(session, store, data);
                boolean result = it.next();

                if (!result) {
                    return Boolean.FALSE;
                }

                switch (opType) {

                    case OpTypes.MATCH_SIMPLE :
                    case OpTypes.MATCH_PARTIAL :
                    case OpTypes.MATCH_FULL :
                        return Boolean.TRUE;
                }

                // only one match allowed for MATCH UNIQUE xxxx
                while (true) {
                    result = it.next();

                    if (!result) {
                        break;
                    }

                    Object[] rowData = it.getCurrent();

                    if (Boolean.TRUE.equals(matchValues(session, data,
                                                        rowData))) {
                        return Boolean.FALSE;
                    }
                }

                return Boolean.TRUE;
            }
            default : {
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionLogical");
            }
        }
    }

    private Boolean testExistsCondition(Session session) {

        nodes[LEFT].materialise(session);

        return nodes[LEFT].getTable().isEmpty(session) ? Boolean.FALSE
                                                       : Boolean.TRUE;
    }

    private Boolean testAllAnyCondition(Session session) {

        Object[]     rowData = nodes[LEFT].getRowValue(session);
        TableDerived td      = nodes[RIGHT].table;

        td.materialiseCorrelated(session);

        Boolean result = getAllAnyValue(session, rowData, td);

        return result;
    }

    private Boolean getAllAnyValue(Session session, Object[] data,
                                   TableDerived td) {

        boolean         empty     = td.isEmpty(session);
        Index           index     = td.getFullIndex(session);
        PersistentStore store     = td.getRowStore(session);
        int             nullCount = countNulls(data);
        boolean         hasNulls  = false;
        boolean         comparedNull;
        RowIterator     it;

        for (int i = 0; i < td.columnCount; i++) {
            hasNulls |= store.hasNull(i);
        }

        convertToType(session, data, nodes[LEFT].nodeDataTypes,
                      nodes[RIGHT].nodeDataTypes);

        switch (exprSubType) {

            case OpTypes.ANY_QUANTIFIED : {
                if (empty) {
                    return Boolean.FALSE;
                }

                if (nullCount == data.length) {
                    return null;
                }

                int searchType = opType == OpTypes.NOT_EQUAL ? OpTypes.EQUAL
                                                             : opType;

                it = index.findFirstRow(session, store, data, data.length, 0,
                                        searchType, false, null);

                switch (opType) {

                    case OpTypes.EQUAL :
                        if (it.next()) {
                            if (nullCount == 0) {
                                return Boolean.TRUE;
                            } else {
                                return null;
                            }
                        } else {
                            if (nullCount == 0) {
                                if (!hasNulls) {
                                    return Boolean.FALSE;
                                }
                            } else {
                                return null;
                            }
                        }
                        break;

                    case OpTypes.NOT_EQUAL :
                        break;
                }

                it           = index.firstRow(store);
                comparedNull = false;

                while (it.next()) {
                    Object[] currdata = it.getCurrent();
                    Boolean  result   = compareValues(session, data, currdata);

                    if (result == null) {
                        comparedNull = true;

                        continue;
                    }

                    if (result.booleanValue()) {
                        it.release();

                        return Boolean.TRUE;
                    }
                }

                if (comparedNull) {
                    return null;
                }

                return Boolean.FALSE;
            }
            case OpTypes.ALL_QUANTIFIED : {
                if (empty) {
                    return Boolean.TRUE;
                }

                if (nullCount == data.length) {
                    return null;
                }

                switch (opType) {

                    case OpTypes.EQUAL :
                        break;

                    case OpTypes.NOT_EQUAL :
                        if (!hasNulls) {
                            it = index.findFirstRow(session, store, data);

                            if (it.next()) {
                                return Boolean.FALSE;
                            } else {
                                if (nullCount == 0) {
                                    return Boolean.TRUE;
                                }
                            }
                        }
                        break;

                    case OpTypes.GREATER :
                    case OpTypes.GREATER_EQUAL :
                    case OpTypes.GREATER_EQUAL_PRE :
                        if (!hasNulls) {
                            it = index.lastRow(session, store, 0, null);

                            it.next();

                            Object[] lastdata = it.getCurrent();

                            return compareValues(session, data, lastdata);
                        }
                        break;

                    case OpTypes.SMALLER :
                    case OpTypes.SMALLER_EQUAL :
                        if (!hasNulls) {
                            it = index.firstRow(store);

                            it.next();

                            Object[] firstdata = it.getCurrent();

                            return compareValues(session, data, firstdata);
                        }
                }

                it           = index.firstRow(store);
                comparedNull = false;

                while (it.next()) {
                    Object[] currdata = it.getCurrent();
                    Boolean  result   = compareValues(session, data, currdata);

                    if (result == null) {
                        comparedNull = true;

                        continue;
                    }

                    if (!result.booleanValue()) {
                        it.release();

                        return Boolean.FALSE;
                    }
                }

                if (comparedNull) {
                    return null;
                }

                return Boolean.TRUE;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionLogical");
        }
    }

    /**
     * Converts an OR containing an AND to an AND
     */
    void distributeOr() {

        if (opType != OpTypes.OR) {
            return;
        }

        if (nodes[LEFT].opType == OpTypes.AND) {
            opType = OpTypes.AND;

            Expression temp = new ExpressionLogical(OpTypes.OR,
                nodes[LEFT].nodes[RIGHT], nodes[RIGHT]);

            nodes[LEFT].opType       = OpTypes.OR;
            nodes[LEFT].nodes[RIGHT] = nodes[RIGHT];
            nodes[RIGHT]             = temp;
        } else if (nodes[RIGHT].opType == OpTypes.AND) {
            Expression temp = nodes[LEFT];

            nodes[LEFT]  = nodes[RIGHT];
            nodes[RIGHT] = temp;

            distributeOr();

            return;
        }

        ((ExpressionLogical) nodes[LEFT]).distributeOr();
        ((ExpressionLogical) nodes[RIGHT]).distributeOr();
    }

    /**
     *
     */
    public boolean isIndexable(RangeVariable rangeVar) {

        boolean result;

        switch (opType) {

            case OpTypes.AND : {
                result = nodes[LEFT].isIndexable(rangeVar)
                         || nodes[RIGHT].isIndexable(rangeVar);

                return result;
            }
            case OpTypes.OR : {
                result = nodes[LEFT].isIndexable(rangeVar)
                         && nodes[RIGHT].isIndexable(rangeVar);

                return result;
            }
            default : {
                Expression temp = getIndexableExpression(rangeVar);

                return temp != null;
            }
        }
    }

    Expression getIndexableExpression(RangeVariable rangeVar) {

        switch (opType) {

            case OpTypes.IS_NULL :
                return nodes[LEFT].opType == OpTypes.COLUMN
                       && nodes[LEFT].isIndexable(rangeVar) ? this
                                                            : null;

            case OpTypes.NOT :
                return nodes[LEFT].opType == OpTypes.IS_NULL
                       && nodes[LEFT].nodes[LEFT].opType == OpTypes.COLUMN
                       && nodes[LEFT].nodes[LEFT].isIndexable(rangeVar) ? this
                                                                        : null;

            case OpTypes.EQUAL :
                if (exprSubType == OpTypes.ANY_QUANTIFIED) {
                    if (nodes[RIGHT].isCorrelated()) {
                        return null;
                    }

                    for (int node = 0; node < nodes[LEFT].nodes.length;
                            node++) {
                        if (nodes[LEFT].nodes[node].opType == OpTypes.COLUMN
                                && nodes[LEFT].nodes[node].isIndexable(
                                    rangeVar)) {
                            return this;
                        }
                    }

                    return null;
                }

            // fall through
            case OpTypes.GREATER :
            case OpTypes.GREATER_EQUAL :
            case OpTypes.GREATER_EQUAL_PRE :
            case OpTypes.SMALLER :
            case OpTypes.SMALLER_EQUAL :
                if (exprSubType != 0) {
                    return null;
                }

                if (nodes[RIGHT].isCorrelated()) {
                    return null;
                }

                if (nodes[LEFT].opType == OpTypes.COLUMN
                        && nodes[LEFT].isIndexable(rangeVar)) {
                    if (nodes[RIGHT].hasReference(rangeVar)) {
                        return null;
                    }

                    return this;
                }

                if (nodes[LEFT].hasReference(rangeVar)) {
                    return null;
                }

                if (nodes[RIGHT].opType == OpTypes.COLUMN
                        && nodes[RIGHT].isIndexable(rangeVar)) {
                    swapCondition();

                    return this;
                }

                return null;

            case OpTypes.OR :
                if (isIndexable(rangeVar)) {
                    return this;
                }

                return null;

            default :
                return null;
        }
    }

    /**
     * Called only on comparison expressions after reordering which have
     * a COLUMN left leaf
     */
    boolean isSimpleBound() {

        if (opType == OpTypes.IS_NULL) {
            return true;
        }

        if (nodes[RIGHT] != null) {
            if (nodes[RIGHT].opType == OpTypes.VALUE) {

                // also true for all parameters
                return true;
            }

            if (nodes[RIGHT].opType == OpTypes.SQL_FUNCTION) {
                if (((FunctionSQL) nodes[RIGHT]).isValueFunction()) {
                    return true;
                }
            }
        }

        return false;
    }

    boolean convertToSmaller() {

        switch (opType) {

            case OpTypes.GREATER_EQUAL :
            case OpTypes.GREATER :
                swapCondition();

                return true;

            case OpTypes.SMALLER_EQUAL :
            case OpTypes.SMALLER :
                return true;

            default :
                return false;
        }
    }

    /**
     * Swap the condition with its complement
     */
    void swapCondition() {

        int i = OpTypes.EQUAL;

        switch (opType) {

            case OpTypes.GREATER_EQUAL :
            case OpTypes.GREATER_EQUAL_PRE :
                i = OpTypes.SMALLER_EQUAL;
                break;

            case OpTypes.SMALLER_EQUAL :
                i = OpTypes.GREATER_EQUAL;
                break;

            case OpTypes.SMALLER :
                i = OpTypes.GREATER;
                break;

            case OpTypes.GREATER :
                i = OpTypes.SMALLER;
                break;

            case OpTypes.NOT_DISTINCT :
                i = OpTypes.NOT_DISTINCT;
                break;

            case OpTypes.EQUAL :
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionLogical");
        }

        opType = i;

        Expression e = nodes[LEFT];

        nodes[LEFT]  = nodes[RIGHT];
        nodes[RIGHT] = e;
    }

    boolean reorderComparison(Session session, Expression parent) {

        Expression colExpression    = null;
        Expression nonColExpression = null;
        boolean    left             = false;
        boolean    replaceColumn    = false;
        int        operation        = 0;

        if (nodes[LEFT].opType == OpTypes.ADD) {
            operation = OpTypes.SUBTRACT;
            left      = true;
        } else if (nodes[LEFT].opType == OpTypes.SUBTRACT) {
            operation = OpTypes.ADD;
            left      = true;
        } else if (nodes[RIGHT].opType == OpTypes.ADD) {
            operation = OpTypes.SUBTRACT;
        } else if (nodes[RIGHT].opType == OpTypes.SUBTRACT) {
            operation = OpTypes.ADD;
        }

        if (operation == 0) {
            return false;
        }

        if (left) {
            if (nodes[LEFT].nodes[LEFT].opType == OpTypes.COLUMN) {
                colExpression    = nodes[LEFT].nodes[LEFT];
                nonColExpression = nodes[LEFT].nodes[RIGHT];
            } else if (nodes[LEFT].nodes[RIGHT].opType == OpTypes.COLUMN) {
                replaceColumn    = operation == OpTypes.ADD;
                colExpression    = nodes[LEFT].nodes[RIGHT];
                nonColExpression = nodes[LEFT].nodes[LEFT];
            }
        } else {
            if (nodes[RIGHT].nodes[LEFT].opType == OpTypes.COLUMN) {
                colExpression    = nodes[RIGHT].nodes[LEFT];
                nonColExpression = nodes[RIGHT].nodes[RIGHT];
            } else if (nodes[RIGHT].nodes[RIGHT].opType == OpTypes.COLUMN) {
                replaceColumn    = operation == OpTypes.ADD;
                colExpression    = nodes[RIGHT].nodes[RIGHT];
                nonColExpression = nodes[RIGHT].nodes[LEFT];
            }
        }

        if (colExpression == null) {
            return false;
        }

        Expression           otherExpression = left ? nodes[RIGHT]
                                                    : nodes[LEFT];
        ExpressionArithmetic newArg          = null;

        if (!replaceColumn) {
            newArg = new ExpressionArithmetic(operation, otherExpression,
                                              nonColExpression);

            newArg.resolveTypesForArithmetic(session, parent);
        }

        if (left) {
            if (replaceColumn) {
                nodes[RIGHT]             = colExpression;
                nodes[LEFT].nodes[RIGHT] = otherExpression;

                ((ExpressionArithmetic) nodes[LEFT]).resolveTypesForArithmetic(
                    session, parent);
            } else {
                nodes[LEFT]  = colExpression;
                nodes[RIGHT] = newArg;
            }
        } else {
            if (replaceColumn) {
                nodes[LEFT]               = colExpression;
                nodes[RIGHT].nodes[RIGHT] = otherExpression;

                ((ExpressionArithmetic) nodes[RIGHT])
                    .resolveTypesForArithmetic(session, parent);
            } else {
                nodes[RIGHT] = colExpression;
                nodes[LEFT]  = newArg;
            }
        }

        return true;
    }

    boolean isConditionRangeVariable(RangeVariable range) {

        if (nodes[LEFT].getRangeVariable() == range) {
            return true;
        }

        if (nodes[RIGHT].getRangeVariable() == range) {
            return true;
        }

        return false;
    }

    void getJoinRangeVariables(RangeVariable[] ranges, List list) {

        for (int i = 0; i < nodes.length; i++) {
            nodes[i].getJoinRangeVariables(ranges, list);
        }
    }

    double costFactor(Session session, RangeVariable rangeVar, int operation) {

        double cost;

        switch (opType) {

            case OpTypes.OR : {
                return nodes[LEFT].costFactor(session, rangeVar, opType)
                       + nodes[RIGHT].costFactor(session, rangeVar, opType);
            }
            case OpTypes.OVERLAPS :
            case OpTypes.IN :
            case OpTypes.MATCH_SIMPLE :
            case OpTypes.MATCH_PARTIAL :
            case OpTypes.MATCH_FULL :
            case OpTypes.MATCH_UNIQUE_SIMPLE :
            case OpTypes.MATCH_UNIQUE_PARTIAL :
            case OpTypes.MATCH_UNIQUE_FULL :
            case OpTypes.NOT_DISTINCT : {
                PersistentStore store =
                    rangeVar.rangeTable.getRowStore(session);

                cost = store.elementCount();

                if (cost < Index.minimumSelectivity) {
                    cost = Index.minimumSelectivity;
                }

                break;
            }
            case OpTypes.IS_NULL :
            case OpTypes.NOT : {
                cost = costFactorUnaryColumn(session, rangeVar);

                break;
            }
            case OpTypes.EQUAL : {
                switch (exprSubType) {

                    case OpTypes.ANY_QUANTIFIED : {
                        if (nodes[LEFT].opType == OpTypes.COLUMN
                                && nodes[LEFT].getRangeVariable()
                                   == rangeVar) {
                            cost = costFactorColumns(session, rangeVar);
                            cost *= 1024;

                            break;
                        }
                    }

                    // fall through
                    case OpTypes.ALL_QUANTIFIED : {
                        PersistentStore store =
                            rangeVar.rangeTable.getRowStore(session);

                        cost = store.elementCount();

                        if (cost < Index.minimumSelectivity) {
                            cost = Index.minimumSelectivity;
                        }

                        cost *= 1024;

                        break;
                    }
                    default :
                        cost = costFactorColumns(session, rangeVar);
                }

                break;
            }
            case OpTypes.GREATER :
            case OpTypes.GREATER_EQUAL :
            case OpTypes.GREATER_EQUAL_PRE :
            case OpTypes.SMALLER :
            case OpTypes.SMALLER_EQUAL : {
                cost = costFactorColumns(session, rangeVar);

                break;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionLogical");
        }

        return cost;
    }

    double costFactorUnaryColumn(Session session, RangeVariable rangeVar) {

        if (nodes[LEFT].opType == OpTypes.COLUMN
                && nodes[LEFT].getRangeVariable() == rangeVar) {
            return nodes[LEFT].costFactor(session, rangeVar, opType);
        } else {
            PersistentStore store = rangeVar.rangeTable.getRowStore(session);
            double          cost  = store.elementCount();

            return cost < Index.minimumSelectivity ? Index.minimumSelectivity
                                                   : cost;
        }
    }

    double costFactorColumns(Session session, RangeVariable rangeVar) {

        double cost = 0;

        if (nodes[LEFT].opType == OpTypes.COLUMN
                && nodes[LEFT].getRangeVariable() == rangeVar) {
            if (!nodes[RIGHT].hasReference(rangeVar)) {
                cost = nodes[LEFT].costFactor(session, rangeVar, opType);
            }
        } else if (nodes[RIGHT].opType == OpTypes.COLUMN
                   && nodes[RIGHT].getRangeVariable() == rangeVar) {
            if (!nodes[LEFT].hasReference(rangeVar)) {
                cost = nodes[RIGHT].costFactor(session, rangeVar, opType);
            }
        } else {
            PersistentStore store = rangeVar.rangeTable.getRowStore(session);

            cost = store.elementCount();
        }

        if (cost == 0) {
            PersistentStore store = rangeVar.rangeTable.getRowStore(session);

            cost = store.elementCount();
        }

        if (cost < Index.minimumSelectivity) {
            cost = Index.minimumSelectivity;
        }

        return cost;
    }
}
