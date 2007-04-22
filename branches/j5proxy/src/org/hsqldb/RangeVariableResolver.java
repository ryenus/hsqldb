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

import org.hsqldb.Parser.CompileContext;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.MultiValueHashMap;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.OrderedIntHashSet;

/**
 * Determines how JOIN and WHERE expressions are used in query
 * processing and which indexes are used for table access.
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class RangeVariableResolver {

    RangeVariable[] rangeVariables;
    Expression      conditions;
    OrderedHashSet  rangeVarSet = new OrderedHashSet();
    CompileContext  compileContext;

    //
    HsqlArrayList[] tempJoinExpressions;
    HsqlArrayList[] joinExpressions;
    HsqlArrayList[] whereExpressions;
    HsqlArrayList   queryExpressions = new HsqlArrayList();

    //
    Expression[] inExpressions;
    boolean[]    flags;

    //
    OrderedHashSet set = new OrderedHashSet();

    //
    OrderedIntHashSet colIndexSetEqual  = new OrderedIntHashSet();
    OrderedIntHashSet colIndexSetOther  = new OrderedIntHashSet();
    MultiValueHashMap map               = new MultiValueHashMap();
    int               inExpressionCount = 0;
    boolean           hasOuterJoin      = false;

    RangeVariableResolver(RangeVariable[] rangeVars, Expression conditions,
                          CompileContext compileContext) {

        this.rangeVariables = rangeVars;
        this.conditions     = conditions;
        this.compileContext = compileContext;

        for (int i = 0; i < rangeVars.length; i++) {
            RangeVariable range = rangeVars[i];

            rangeVarSet.add(range);

            if (range.isOuterJoin) {
                hasOuterJoin = true;
            }
        }

        inExpressions       = new Expression[rangeVars.length];
        flags               = new boolean[rangeVars.length];
        tempJoinExpressions = new HsqlArrayList[rangeVars.length];

        for (int i = 0; i < rangeVars.length; i++) {
            tempJoinExpressions[i] = new HsqlArrayList();
        }

        joinExpressions = new HsqlArrayList[rangeVars.length];

        for (int i = 0; i < rangeVars.length; i++) {
            joinExpressions[i] = new HsqlArrayList();
        }

        whereExpressions = new HsqlArrayList[rangeVars.length];

        for (int i = 0; i < rangeVars.length; i++) {
            whereExpressions[i] = new HsqlArrayList();
        }
    }

    void processConditions(Session session) throws HsqlException {

        decomposeCondition(session, conditions, queryExpressions);

        for (int i = 0; i < rangeVariables.length; i++) {
            if (rangeVariables[i].nonIndexJoinCondition == null) {
                continue;
            }

            decomposeCondition(session,
                               rangeVariables[i].nonIndexJoinCondition,
                               tempJoinExpressions[i]);

            rangeVariables[i].nonIndexJoinCondition = null;
        }

        conditions = null;

        assignToLists();
        expandConditions();
        assignToRangeVariables(session);
        processFullJoins();
    }

    /**
     * Divides AND conditions and assigns
     */
    static Expression decomposeCondition(Session session, Expression e,
                                         HsqlArrayList conditions)
                                         throws HsqlException {

        if (e == null) {
            return Expression.EXPR_TRUE;
        }

        Expression arg1 = e.getArg();
        Expression arg2 = e.getArg2();
        int        type = e.getType();

        if (type == Expression.AND) {
            arg1 = decomposeCondition(session, arg1, conditions);
            arg2 = decomposeCondition(session, arg2, conditions);

            if (arg1 == Expression.EXPR_TRUE) {
                return arg2;
            }

            if (arg2 == Expression.EXPR_TRUE) {
                return arg1;
            }

            e.setLeftExpression(arg1);
            e.setRightExpression(arg2);

            return e;
        } else if (type == Expression.EQUAL) {
            if (arg1.getType() == Expression.ROW
                    && arg2.getType() == Expression.ROW) {
                for (int i = 0; i < arg1.argList.length; i++) {
                    Expression part = new Expression(arg1.argList[i],
                                                     arg2.argList[i]);

                    part.resolveTypes(session, null);
                    conditions.add(part);
                }

                return Expression.EXPR_TRUE;
            }
        }

        if (e != Expression.EXPR_TRUE) {
            conditions.add(e);
        }

        return Expression.EXPR_TRUE;
    }

    /**
     * Assigns the conditions to separate lists
     */
    void assignToLists() throws HsqlException {

        for (int i = 0; i < queryExpressions.size(); i++) {
            assignToLists((Expression) queryExpressions.get(i),
                          whereExpressions, 0);
        }

        int lastOuterIndex = -1;

        for (int i = 0; i < rangeVariables.length; i++) {
            if (rangeVariables[i].isOuterJoin) {
                lastOuterIndex = i;
            }

            if (lastOuterIndex == i) {
                joinExpressions[i].addAll(tempJoinExpressions[i]);
            } else {
                for (int j = 0; j < tempJoinExpressions[i].size(); j++) {
                    assignToLists((Expression) tempJoinExpressions[i].get(j),
                                  joinExpressions, lastOuterIndex + 1);
                }
            }
        }
    }

    /**
     * Assigns a single condition to the relevant list of conditions
     *
     * Parameter first indicates the first range variable to which condition
     * can be assigned
     */
    void assignToLists(Expression e, HsqlArrayList[] expressionLists,
                       int first) throws HsqlException {

        set.clear();
        e.collectRangeVariables(rangeVariables, set);

        int index = rangeVarSet.getLargestIndex(set);

        // condition is independent of tables if no range variable is found
        if (index == -1) {
            index = 0;
        }

        // condition is assigned to first non-outer range variable
        if (index < first) {
            index = first;
        }

        expressionLists[index].add(e);
    }

    void expandConditions() {
        expandConditions(whereExpressions, false);
        expandConditions(joinExpressions, false);
    }

    void expandConditions(HsqlArrayList[] array, boolean isJoin) {

        for (int i = 0; i < rangeVariables.length; i++) {
            HsqlArrayList list = array[i];

            map.clear();
            set.clear();

            boolean hasChain = false;

            for (int j = 0; j < list.size(); j++) {
                Expression e = (Expression) list.get(j);

                if (!e.isColumnEqual
                        || e.eArg.rangeVariable == e.eArg2.rangeVariable) {
                    continue;
                }

                if (e.eArg.rangeVariable == rangeVariables[i]) {
                    map.put(e.eArg.column, e.eArg2);

                    if (!set.add(e.eArg.column)) {
                        hasChain = true;
                    }
                } else {
                    map.put(e.eArg2.column, e.eArg);

                    if (!set.add(e.eArg2.column)) {
                        hasChain = true;
                    }
                }
            }

            if (hasChain &&!(hasOuterJoin && isJoin)) {
                Iterator keyIt = map.keySet().iterator();

                while (keyIt.hasNext()) {
                    Object   key = keyIt.next();
                    Iterator it  = map.get(key);

                    set.clear();

                    while (it.hasNext()) {
                        set.add(it.next());
                    }

                    while (set.size() > 1) {
                        Expression e1 = (Expression) set.remove(set.size()
                            - 1);

                        for (int j = 0; j < set.size(); j++) {
                            Expression e2 = (Expression) set.get(j);

                            closeJoinChain(array, e1, e2);
                        }
                    }
                }
            }
        }
    }

    void closeJoinChain(HsqlArrayList[] array, Expression e1, Expression e2) {

        int idx1  = rangeVarSet.getIndex(e1.rangeVariable);
        int idx2  = rangeVarSet.getIndex(e2.rangeVariable);
        int index = idx1 > idx2 ? idx1
                                : idx2;

        array[index].add(new Expression(e1, e2));
    }

    /**
     * Assigns conditions to range variables and converts suitable IN conditions
     * to table lookup.
     */
    void assignToRangeVariables(Session session) throws HsqlException {

        for (int i = 0; i < rangeVariables.length; i++) {
            if (rangeVariables[i].isOuterJoin) {
                assignToRangeVariable(session, rangeVariables[i], i,
                                      joinExpressions[i], true);
                assignToRangeVariable(session, rangeVariables[i], i,
                                      whereExpressions[i], false);
            } else {
                joinExpressions[i].addAll(whereExpressions[i]);
                assignToRangeVariable(session, rangeVariables[i], i,
                                      joinExpressions[i], true);
            }

            if (rangeVariables[i].hasIndexCondition
                    && inExpressions[i] != null) {
                if (!flags[i] && rangeVariables[i].isOuterJoin) {
                    rangeVariables[i].addWhereCondition(inExpressions[i]);
                } else {
                    rangeVariables[i].addJoinCondition(inExpressions[i]);
                }

                inExpressions[i] = null;

                inExpressionCount--;
            }
        }

        if (inExpressionCount != 0) {
            setInConditionsAsTables(session);
        }
    }

    /**
     * Assigns a set of conditions to a range variable or IN condition list.
     */
    void assignToRangeVariable(Session session, RangeVariable rangeVar,
                               int rangeVarIndex, HsqlArrayList exprList,
                               boolean isJoin) throws HsqlException {

        if (exprList.isEmpty()) {
            return;
        }

        // assign all non-indexables
        for (int j = 0, size = exprList.size(); j < size; j++) {
            Expression e = (Expression) exprList.get(j);

            if (rangeVar.hasIndexCondition) {
                rangeVar.addCondition(e, isJoin);
                exprList.set(j, null);

                continue;
            } else if (e.getIndexableExpression(session, rangeVar) == null) {
                rangeVar.addCondition(e, isJoin);
                exprList.set(j, null);

                continue;
            }

            //
            int type = e.getType();

            if (type == Expression.EQUAL) {
                int colIndex = e.getArg().getColumnIndex();

                colIndexSetEqual.add(colIndex);
            } else if (type != Expression.IN) {
                int colIndex = e.getArg().getColumnIndex();

                colIndexSetOther.add(colIndex);
            } else {

                // IN expression
                Index index = rangeVar.rangeTable.getIndexForColumn(session,
                    e.eArg.argList[0].getColumnIndex());

                if (index != null && inExpressions[rangeVarIndex] == null) {
                    inExpressions[rangeVarIndex] = e;

                    inExpressionCount++;
                } else {
                    rangeVar.addCondition(e, isJoin);
                }

                exprList.set(j, null);

                continue;
            }
        }

        boolean isEqual = true;
        Index idx = rangeVar.rangeTable.getIndexForColumns(session,
            colIndexSetEqual);

        if (idx == null) {
            isEqual = false;
            idx = rangeVar.rangeTable.getIndexForColumns(session,
                    colIndexSetOther);
        }

        // different procedure for all temp tables
        if (idx == null && rangeVar.rangeTable.isTemp) {
            if (!colIndexSetEqual.isEmpty()) {
                int[] cols = colIndexSetEqual.toArray();

                idx = rangeVar.rangeTable.getIndexForColumns(session, cols);
            }

            if (idx == null &&!colIndexSetOther.isEmpty()) {
                int[] cols = colIndexSetOther.toArray();

                idx = rangeVar.rangeTable.getIndexForColumns(session, cols);
            }
        }

        // no index found
        if (idx == null) {
            for (int j = 0, size = exprList.size(); j < size; j++) {
                Expression e = (Expression) exprList.get(j);

                if (e != null) {
                    rangeVar.addCondition(e, isJoin);
                }
            }

            return;
        }

        // index found
        int[] cols     = idx.getColumns();
        int   colCount = cols.length;

        if (isEqual && colCount > 1) {
            Expression[] firstRowExpressions = new Expression[cols.length];

            for (int j = 0; j < exprList.size(); j++) {
                Expression e = (Expression) exprList.get(j);

                if (e == null) {
                    continue;
                }

                int type = e.getType();

                if (type == Expression.EQUAL) {
                    int offset = ArrayUtil.find(cols,
                                                e.getArg().getColumnIndex());

                    if (offset != -1 && firstRowExpressions[offset] == null) {
                        firstRowExpressions[offset] = e;

                        exprList.set(j, null);

                        continue;
                    }
                }

                // not used in index lookup
                rangeVar.addCondition(e, isJoin);
                exprList.set(j, null);
            }

            boolean hasNull = false;

            for (int i = 0; i < firstRowExpressions.length; i++) {
                Expression e = firstRowExpressions[i];

                if (e == null) {
                    if (colCount == cols.length) {
                        colCount = i;
                    }

                    hasNull = true;

                    continue;
                }

                if (hasNull) {
                    rangeVar.addCondition(e, isJoin);

                    firstRowExpressions[i] = null;
                }
            }

            rangeVar.addIndexCondition(firstRowExpressions, idx, colCount,
                                       isJoin);
        } else {
            for (int j = 0; j < exprList.size(); j++) {
                Expression e = (Expression) exprList.get(j);

                if (e == null) {
                    continue;
                }

                if (!rangeVar.hasIndexCondition && e.getArg2() != null
                        &&!e.getArg2().isCorrelated()
                        && cols[0] == e.getArg().getColumnIndex()) {
                    rangeVar.addIndexCondition(e, idx, isJoin);
                } else {
                    rangeVar.addCondition(e, isJoin);
                }

                exprList.set(j, null);
            }
        }
    }

    /**
     * Converts an IN conditions into a JOIN
     */
    void setInConditionsAsTables(Session session) {

        for (int i = rangeVariables.length - 1; i >= 0; i--) {
            RangeVariable rangeVar = rangeVariables[i];
            Expression    in       = inExpressions[i];

            if (in != null) {
                Index index = rangeVar.rangeTable.getIndexForColumn(session,
                    in.eArg.argList[0].getColumnIndex());
                RangeVariable newRangeVar =
                    new RangeVariable(in.getArg2().subQuery.table, null,
                                      null, compileContext);

                RangeVariable[] newList =
                    new RangeVariable[rangeVariables.length + 1];

                ArrayUtil.copyAdjustArray(rangeVariables, newList,
                                          newRangeVar, i, 1);

                rangeVariables = newList;

                // make two columns as arg
                Column left = rangeVar.rangeTable.getColumn(
                    in.eArg.argList[0].getColumnIndex());
                Column right = newRangeVar.rangeTable.getColumn(0);
                Expression e = new Expression(rangeVar, left, newRangeVar,
                                              right);

                rangeVar.addIndexCondition(e, index, flags[i]);
            }
        }
    }

    void processFullJoins() {

        for (int i = 0; i < rangeVariables.length; i++) {
            if (rangeVariables[i].isFullJoin) {}
        }
    }
}
