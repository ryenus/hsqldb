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
import org.hsqldb.lib.List;
import org.hsqldb.lib.Set;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.Type;

/**
 * Represents a PERIOD condition.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.1
 * @since 2.5.0
 */
public class ExpressionPeriodOp extends ExpressionLogical {

    // for object reference collection
    PeriodDefinition leftPeriod;
    PeriodDefinition rightPeriod;

    // for SYSTEM_TIME
    final boolean isSystemVersionCondition;

    //
    boolean transformed;

    /**
     * check for period node ordering
     */
    private ExpressionPeriodOp(Expression[] nodes) {

        super(OpTypes.SMALLER);

        this.nodes                    = nodes;
        this.isSystemVersionCondition = false;
    }

    /**
     * general constructor
     */
    ExpressionPeriodOp(int type, Expression left, Expression right) {

        super(type, left, right);

        this.isSystemVersionCondition = false;
    }

    /**
     * FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP (as default condition)
     */
    ExpressionPeriodOp() {

        super(OpTypes.RANGE_EQUALS);

        Expression left  = new ExpressionPeriod();
        Expression right = getEpochLimitExpression();

        nodes = new Expression[] {
            left, right
        };

        // default condition does not count
        this.isSystemVersionCondition = false;
    }

    /**
     * FOR SYSTEM_TIME AS OF
     */
    ExpressionPeriodOp(Expression pointOfTime) {

        super(OpTypes.RANGE_CONTAINS);

        Expression left = new ExpressionPeriod();

        nodes                         = new Expression[] {
            left, pointOfTime
        };
        this.isSystemVersionCondition = true;
    }

    /**
     * FOR SYSTEM_TIME FROM TO and BETWEEN
     */
    ExpressionPeriodOp(Expression start, Expression end) {

        super(OpTypes.RANGE_OVERLAPS);

        Expression left  = new ExpressionPeriod();
        Expression right = new ExpressionPeriod(start, end);

        nodes                         = new Expression[] {
            left, right
        };
        this.isSystemVersionCondition = true;
    }

    boolean isSystemVersionCondition() {
        return isSystemVersionCondition;
    }

    void setSystemRangeVariable(Session session, RangeGroup[] rangeGroups,
                                RangeVariable range) {

        ExpressionPeriod period = (ExpressionPeriod) nodes[LEFT];

        period.setRangeVariable(range);

        Expression right = nodes[RIGHT];
        List unresolved = right.resolveColumnReferences(session,
            RangeGroup.emptyGroup, rangeGroups, null);

        ExpressionColumn.checkColumnsResolved(unresolved);
        right.resolveTypes(session, null);
        transform();
    }

    public List resolveColumnReferences(Session session,
            RangeGroup rangeGroup, int rangeCount, RangeGroup[] rangeGroups,
            List unresolvedSet, boolean acceptsSequences) {

        // special treatment of column or period for CONTAINS
        if (opType == OpTypes.RANGE_CONTAINS) {
            if (nodes[RIGHT] instanceof ExpressionPeriod) {
                Expression columnExpr =
                    ((ExpressionPeriod) nodes[RIGHT]).columnExpr;

                if (columnExpr != null) {
                    try {
                        nodes[RIGHT].resolveColumnReferences(session,
                                                             rangeGroup,
                                                             rangeCount,
                                                             rangeGroups,
                                                             unresolvedSet,
                                                             acceptsSequences);
                    } catch (HsqlException e) {
                        nodes[RIGHT] = columnExpr;
                    }
                }
            }
        }

        for (int i = 0; i < nodes.length; i++) {
            unresolvedSet = nodes[i].resolveColumnReferences(session,
                    rangeGroup, rangeCount, rangeGroups, unresolvedSet,
                    acceptsSequences);
        }

        if (nodes[LEFT] instanceof ExpressionPeriod) {
            leftPeriod = ((ExpressionPeriod) nodes[LEFT]).period;
        }

        if (nodes[RIGHT] instanceof ExpressionPeriod) {
            rightPeriod = ((ExpressionPeriod) nodes[RIGHT]).period;
        }

        if (!transformed) {
            transform();
        }

        return unresolvedSet;
    }

    public void resolveTypes(Session session, Expression parent) {
        super.resolveTypes(session, parent);
    }

    private void transform() {

        // todo - keep the unnamed periods and check the types of start and end expressions to be timestamp or date
        ExpressionPeriod left       = (ExpressionPeriod) nodes[LEFT];
        boolean          checkLeft  = true;
        boolean          checkRight = true;

        if (left.isNamedPeriod()) {
            if (left.getPeriodType()
                    == SchemaObject.PeriodType.PERIOD_SYSTEM) {

                // todo - review for query opt - may not be necessary
                // left.getRangeVariable().setSystemPeriodCondition(this);
            }

            checkLeft = false;
        } else {
            Expression node = left.getLeftNode();

            if (node.opType == OpTypes.DYNAMIC_PARAM) {
                node.dataType = Type.SQL_TIMESTAMP_WITH_TIME_ZONE;
            }

            node = left.getRightNode();

            if (node.opType == OpTypes.DYNAMIC_PARAM) {
                node.dataType = Type.SQL_TIMESTAMP_WITH_TIME_ZONE;
            }
        }

        if (nodes[RIGHT] instanceof ExpressionPeriod) {
            ExpressionPeriod right = (ExpressionPeriod) nodes[RIGHT];

            if (right.isNamedPeriod()) {
                if (right.getPeriodType()
                        == SchemaObject.PeriodType.PERIOD_SYSTEM) {

                    // todo - review for query opt - may not be necessary
                    // right.getRangeVariable().setSystemPeriodCondition(this);
                }

                checkRight = false;
            } else {
                Expression node = right.getLeftNode();

                if (node.opType == OpTypes.DYNAMIC_PARAM) {
                    node.dataType = Type.SQL_TIMESTAMP_WITH_TIME_ZONE;
                }

                node = right.getRightNode();

                if (node.opType == OpTypes.DYNAMIC_PARAM) {
                    node.dataType = Type.SQL_TIMESTAMP_WITH_TIME_ZONE;
                }
            }
        } else {
            checkRight = false;
        }

        Expression expanded = newExpression(opType, nodes, checkLeft,
                                            checkRight);

        this.nodes  = expanded.nodes;
        this.opType = expanded.opType;
        transformed = true;
    }

    public Object getValue(Session session) {

        Object result = super.getValue(session);

        if (opType == OpTypes.SMALLER) {
            if (Boolean.FALSE.equals(result)) {
                throw Error.error(ErrorCode.X_22020);
            }
        }

        return result;
    }

    void collectObjectNames(Set set) {

        if (leftPeriod != null) {
            set.add(leftPeriod.getName());
        }

        if (rightPeriod != null) {
            set.add(rightPeriod.getName());
        }
    }

    static Expression getEpochLimitExpression() {
        return new ExpressionValue(DateTimeType.epochLimitTimestamp,
                                   Type.SQL_TIMESTAMP_WITH_TIME_ZONE);
    }

    static ExpressionLogical newExpression(int type, Expression[] nodes,
                                           boolean checkLeft,
                                           boolean checkRight) {

        ExpressionLogical a;
        ExpressionLogical b;
        ExpressionLogical c;
        Expression        left  = nodes[LEFT];
        Expression        right = nodes[RIGHT];

        switch (type) {

            case OpTypes.RANGE_CONTAINS :
                if (right instanceof ExpressionPeriod) {
                    a = new ExpressionLogical(OpTypes.SMALLER_EQUAL,
                                              left.getLeftNode(),
                                              right.getLeftNode());
                    b = new ExpressionLogical(OpTypes.GREATER_EQUAL,
                                              left.getRightNode(),
                                              right.getRightNode());
                } else {
                    a = new ExpressionLogical(OpTypes.SMALLER_EQUAL,
                                              left.getLeftNode(), right);
                    b = new ExpressionLogical(OpTypes.GREATER,
                                              left.getRightNode(), right);
                }

                c = new ExpressionLogical(OpTypes.AND, a, b);
                break;

            case OpTypes.RANGE_EQUALS :
                if (right instanceof ExpressionPeriod) {
                    a = new ExpressionLogical(OpTypes.EQUAL,
                                              left.getLeftNode(),
                                              right.getLeftNode());
                    b = new ExpressionLogical(OpTypes.EQUAL,
                                              left.getRightNode(),
                                              right.getRightNode());
                    c = new ExpressionLogical(OpTypes.AND, a, b);
                } else {

                    // default SYSTEM_TIME condition
                    c = new ExpressionLogical(OpTypes.EQUAL,
                                              left.getRightNode(), right);
                }
                break;

            case OpTypes.RANGE_OVERLAPS :
                a = new ExpressionLogical(OpTypes.SMALLER, left.getLeftNode(),
                                          right.getRightNode());
                b = new ExpressionLogical(OpTypes.GREATER,
                                          left.getRightNode(),
                                          right.getLeftNode());
                c = new ExpressionLogical(OpTypes.AND, a, b);
                break;

            case OpTypes.RANGE_PRECEDES :
                c = new ExpressionLogical(OpTypes.SMALLER_EQUAL,
                                          left.getRightNode(),
                                          right.getLeftNode());
                break;

            case OpTypes.RANGE_IMMEDIATELY_PRECEDES :
                c = new ExpressionLogical(OpTypes.EQUAL, left.getRightNode(),
                                          right.getLeftNode());
                break;

            case OpTypes.RANGE_SUCCEEDS :
                c = new ExpressionLogical(OpTypes.GREATER_EQUAL,
                                          left.getLeftNode(),
                                          right.getRightNode());
                break;

            case OpTypes.RANGE_IMMEDIATELY_SUCCEEDS :
                c = new ExpressionLogical(OpTypes.EQUAL, left.getLeftNode(),
                                          right.getRightNode());
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionLogical");
        }

        a = null;
        b = null;

        if (checkLeft) {
            a = new ExpressionPeriodOp(left.nodes);
        }

        if (checkRight) {
            b = new ExpressionPeriodOp(right.nodes);
        }

        if (a == null) {
            a = b;
            b = null;
        }

        if (b != null) {
            a = new ExpressionLogical(OpTypes.AND, a, b);
            b = null;
        }

        if (a != null) {
            c = new ExpressionLogical(OpTypes.AND, a, c);
        }

        return c;
    }
}
