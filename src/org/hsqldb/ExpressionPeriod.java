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
import org.hsqldb.lib.HsqlList;
import org.hsqldb.lib.Set;

/**
 * Nodes represent PERIOD start and end.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.0
 * @since 2.5.0
 */
public class ExpressionPeriod extends Expression {

    PeriodDefinition period;

    // temporary use during parsing
    ExpressionColumn columnExpr;

    /**
     * for default SYSTEM_TIME in table reference
     */
    ExpressionPeriod() {
        super(OpTypes.PERIOD);
    }

    /**
     * FOR SYSTEM_TIME FROM ... TO
     */
    ExpressionPeriod(Expression start, Expression end) {

        super(OpTypes.PERIOD);

        this.nodes = new Expression[] {
            start, end
        };
    }

    /**
     * any named period
     */
    ExpressionPeriod(ExpressionColumn colExpr) {

        super(OpTypes.PERIOD);

        this.columnExpr = colExpr;
    }

    /**
     * any defined period
     */
    ExpressionPeriod(PeriodDefinition period) {

        super(OpTypes.PERIOD);

        this.period = period;
    }

    /**
     * paranthesized period elements
     */
    ExpressionPeriod(Expression rowExpr) {

        super(OpTypes.PERIOD);

        this.nodes = rowExpr.nodes;
    }

    boolean isNamedPeriod() {
        return period != null;
    }

    int getPeriodType() {

        if (period == null) {
            return SchemaObject.PeriodType.PERIOD_NONE;
        }

        return period.getPeriodType();
    }

    /**
     * For system period
     */
    void setRangeVariable(RangeVariable rangeVar) {

        Table table = rangeVar.getTable();

        period = table.getSystemPeriod();

        Expression left  = new ExpressionColumn(rangeVar, period.startColumn);
        Expression right = new ExpressionColumn(rangeVar, period.endColumn);

        nodes = new Expression[] {
            left, right
        };
    }

    public HsqlList resolveColumnReferences(Session session,
            RangeGroup rangeGroup, int rangeCount, RangeGroup[] rangeGroups,
            HsqlList unresolvedSet, boolean acceptsSequences) {

        for (int i = 0; i < nodes.length; i++) {
            unresolvedSet = nodes[i].resolveColumnReferences(session,
                    rangeGroup, rangeCount, rangeGroups, unresolvedSet,
                    acceptsSequences);
        }

        RangeVariable[] rangeVarArray = rangeGroup.getRangeVariables();

        if (columnExpr != null) {
            for (int i = 0; i < rangeCount; i++) {
                RangeVariable rangeVar = rangeVarArray[i];
                PeriodDefinition p = rangeVar.findPeriod(columnExpr.schema,
                    columnExpr.tableName, columnExpr.columnName);

                if (p != null) {
                    if (period == null) {
                        period = p;

                        Expression left =
                            new ExpressionColumn(rangeVar, period.startColumn);
                        Expression right = new ExpressionColumn(rangeVar,
                            period.endColumn);

                        nodes = new Expression[] {
                            left, right
                        };
                    } else {
                        throw Error.error(ErrorCode.X_42516);
                    }
                }
            }

            if (period == null) {
                throw Error.error(ErrorCode.X_42516);
            }
        }

        return unresolvedSet;
    }

    public void resolveTypes(Session session, Expression parent) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }
    }

    void collectObjectNames(Set set) {

        if (period != null) {
            set.add(period.getName());
        }
    }
}
