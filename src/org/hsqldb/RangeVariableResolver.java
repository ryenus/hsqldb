/* Copyright (c) 2001-2009, The HSQL Development Group
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
import org.hsqldb.ParserDQL.CompileContext;
import org.hsqldb.RangeVariable.RangeVariableConditions;
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
 * @author Fred Toussi (fredt@users dot sourceforge.net)
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
    boolean[]    inInJoin;
    int          inExpressionCount = 0;

    //
    boolean hasOuterJoin = false;

    //
    OrderedIntHashSet colIndexSetEqual = new OrderedIntHashSet();
    OrderedIntHashSet colIndexSetOther = new OrderedIntHashSet();
    OrderedHashSet    tempSet          = new OrderedHashSet();
    MultiValueHashMap tempMap          = new MultiValueHashMap();

    RangeVariableResolver(RangeVariable[] rangeVars, Expression conditions,
                          CompileContext compileContext) {

        this.rangeVariables = rangeVars;
        this.conditions     = conditions;
        this.compileContext = compileContext;

        for (int i = 0; i < rangeVars.length; i++) {
            RangeVariable range = rangeVars[i];

            rangeVarSet.add(range);

            if (range.isLeftJoin || range.isRightJoin) {
                hasOuterJoin = true;
            }
        }

        inExpressions       = new Expression[rangeVars.length];
        inInJoin            = new boolean[rangeVars.length];
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

    void processConditions() {

        decomposeAndConditions(conditions, queryExpressions);

        for (int i = 0; i < rangeVariables.length; i++) {
            if (rangeVariables[i].joinCondition == null) {
                continue;
            }

            decomposeAndConditions(rangeVariables[i].joinCondition,
                                   tempJoinExpressions[i]);

            rangeVariables[i].joinCondition = null;
        }

        conditions = null;

        assignToLists();
        expandConditions();
        assignToRangeVariables();
    }

    /**
     * Divides AND and OR conditions and assigns
     */
    static Expression decomposeAndConditions(Expression e,
            HsqlArrayList conditions) {

        if (e == null) {
            return Expression.EXPR_TRUE;
        }

        Expression arg1 = e.getLeftNode();
        Expression arg2 = e.getRightNode();
        int        type = e.getType();

        if (type == OpTypes.AND) {
            arg1 = decomposeAndConditions(arg1, conditions);
            arg2 = decomposeAndConditions(arg2, conditions);

            if (arg1 == Expression.EXPR_TRUE) {
                return arg2;
            }

            if (arg2 == Expression.EXPR_TRUE) {
                return arg1;
            }

            e.setLeftNode(arg1);
            e.setRightNode(arg2);

            return e;
        } else if (type == OpTypes.EQUAL) {
            if (arg1.getType() == OpTypes.ROW
                    && arg2.getType() == OpTypes.ROW) {
                for (int i = 0; i < arg1.nodes.length; i++) {
                    Expression part = new ExpressionLogical(arg1.nodes[i],
                        arg2.nodes[i]);

                    part.resolveTypes(null, null);
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
     * Divides AND and OR conditions and assigns
     */
    static Expression decomposeOrConditions(Expression e,
            HsqlArrayList conditions) {

        if (e == null) {
            return Expression.EXPR_FALSE;
        }

        Expression arg1 = e.getLeftNode();
        Expression arg2 = e.getRightNode();
        int        type = e.getType();

        if (type == OpTypes.OR) {
            arg1 = decomposeOrConditions(arg1, conditions);
            arg2 = decomposeOrConditions(arg2, conditions);

            if (arg1 == Expression.EXPR_FALSE) {
                return arg2;
            }

            if (arg2 == Expression.EXPR_FALSE) {
                return arg1;
            }

            e.setLeftNode(arg1);
            e.setRightNode(arg2);

            return e;
        }

        if (e != Expression.EXPR_FALSE) {
            conditions.add(e);
        }

        return Expression.EXPR_FALSE;
    }

    /**
     * Assigns the conditions to separate lists
     */
    void assignToLists() {

        int lastOuterIndex = -1;
        int lastRightIndex = -1;

        for (int i = 0; i < rangeVariables.length; i++) {
            if (rangeVariables[i].isLeftJoin) {
                lastOuterIndex = i;
            }

            if (rangeVariables[i].isRightJoin) {
                lastOuterIndex = i;
                lastRightIndex = i;
            }

            if (lastOuterIndex == i) {
                joinExpressions[i].addAll(tempJoinExpressions[i]);
            } else {
                for (int j = 0; j < tempJoinExpressions[i].size(); j++) {
                    assignToJoinLists(
                        (Expression) tempJoinExpressions[i].get(j),
                        joinExpressions, lastOuterIndex + 1);
                }
            }
        }

        for (int i = 0; i < queryExpressions.size(); i++) {
            assignToWhereLists((Expression) queryExpressions.get(i),
                               whereExpressions, lastRightIndex);
        }
    }

    /**
     * Assigns a single condition to the relevant list of conditions
     *
     * Parameter first indicates the first range variable to which condition
     * can be assigned
     */
    void assignToJoinLists(Expression e, HsqlArrayList[] expressionLists,
                           int first) {

        tempSet.clear();
        e.collectRangeVariables(rangeVariables, tempSet);

        int index = rangeVarSet.getLargestIndex(tempSet);

        // condition is independent of tables if no range variable is found
        if (index == -1) {
            index = 0;
        }

        if (tempSet.size() == 1) {
            switch (e.getType()) {

                case OpTypes.COLUMN :
                case OpTypes.EQUAL :
            }
        }

        // condition is assigned to first non-outer range variable
        if (index < first) {
            index = first;
        }

        expressionLists[index].add(e);
    }

    /**
     * Assigns a single condition to the relevant list of conditions
     *
     * Parameter first indicates the first range variable to which condition
     * can be assigned
     */
    void assignToWhereLists(Expression e, HsqlArrayList[] expressionLists,
                            int first) {

        tempSet.clear();
        e.collectRangeVariables(rangeVariables, tempSet);

        int index = rangeVarSet.getLargestIndex(tempSet);

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

            tempMap.clear();
            tempSet.clear();

            boolean hasChain = false;

            for (int j = 0; j < list.size(); j++) {
                Expression e = (Expression) list.get(j);

                if (!e.isColumnEqual
                        || e.getLeftNode().getRangeVariable()
                           == e.getRightNode().getRangeVariable()) {
                    continue;
                }

                if (e.getLeftNode().getRangeVariable() == rangeVariables[i]) {
                    tempMap.put(e.getLeftNode().getColumn(), e.getRightNode());

                    if (!tempSet.add(e.getLeftNode().getColumn())) {
                        hasChain = true;
                    }
                } else {
                    tempMap.put(e.getRightNode().getColumn(), e.getLeftNode());

                    if (!tempSet.add(e.getRightNode().getColumn())) {
                        hasChain = true;
                    }
                }
            }

            if (hasChain && !(hasOuterJoin && isJoin)) {
                Iterator keyIt = tempMap.keySet().iterator();

                while (keyIt.hasNext()) {
                    Object   key = keyIt.next();
                    Iterator it  = tempMap.get(key);

                    tempSet.clear();

                    while (it.hasNext()) {
                        tempSet.add(it.next());
                    }

                    while (tempSet.size() > 1) {
                        Expression e1 =
                            (Expression) tempSet.remove(tempSet.size() - 1);

                        for (int j = 0; j < tempSet.size(); j++) {
                            Expression e2 = (Expression) tempSet.get(j);

                            closeJoinChain(array, e1, e2);
                        }
                    }
                }
            }
        }
    }

    void closeJoinChain(HsqlArrayList[] array, Expression e1, Expression e2) {

        int idx1  = rangeVarSet.getIndex(e1.getRangeVariable());
        int idx2  = rangeVarSet.getIndex(e2.getRangeVariable());
        int index = idx1 > idx2 ? idx1
                                : idx2;

        Expression e = new ExpressionLogical(e1, e2);

        for (int i = 0; i < array[index].size(); i++) {
            if (e.equals(array[index].get(i)) ){
                return;
            }
        }

        array[index].add(e);
    }

    /**
     * Assigns conditions to range variables and converts suitable IN conditions
     * to table lookup.
     */
    void assignToRangeVariables() {

        for (int i = 0; i < rangeVariables.length; i++) {
            boolean hasIndex = false;
            boolean isOuter = rangeVariables[i].isLeftJoin
                              || rangeVariables[i].isRightJoin;
            RangeVariableConditions conditions;

            if (isOuter) {
                conditions = rangeVariables[i].joinConditions[0];

                assignToRangeVariable(rangeVariables[i], conditions, i,
                                      joinExpressions[i]);

                // index only on one condition -- right and full can have index
                conditions = rangeVariables[i].joinConditions[0];

                if (conditions.hasIndexCondition()) {
                    hasIndex = true;
                }

                conditions = rangeVariables[i].whereConditions[0];

                if (rangeVariables[i].isRightJoin) {
                    assignToRangeVariable(conditions, whereExpressions[i]);
                    assignToRangeVariable(rangeVariables[i], conditions, i,
                                          whereExpressions[i]);
                } else if (hasIndex) {
                    assignToRangeVariable(conditions, whereExpressions[i]);
                } else {
                    assignToRangeVariable(rangeVariables[i], conditions, i,
                                          whereExpressions[i]);
                }

                conditions = rangeVariables[i].whereConditions[0];

                if (conditions.hasIndexCondition()) {
                    hasIndex = true;
                }
            } else {
                conditions = rangeVariables[i].joinConditions[0];

                joinExpressions[i].addAll(whereExpressions[i]);
                assignToRangeVariable(rangeVariables[i], conditions, i,
                                      joinExpressions[i]);

                conditions = rangeVariables[i].joinConditions[0];

                if (conditions.hasIndexCondition()) {
                    hasIndex = true;
                }
            }

            if (inExpressions[i] != null) {
                if (hasIndex) {
                    if (!inInJoin[i] && isOuter) {
                        rangeVariables[i].whereConditions[0].addCondition(
                            inExpressions[i]);
                    } else {
                        rangeVariables[i].joinConditions[0].addCondition(
                            inExpressions[i]);
                    }

                    inExpressions[i] = null;

                    inExpressionCount--;
                }
            }
        }

        if (inExpressionCount != 0) {
            setInConditionsAsTables();
        }
    }

    void assignToRangeVariable(RangeVariableConditions conditions,
                               HsqlArrayList exprList) {

        for (int j = 0, size = exprList.size(); j < size; j++) {
            Expression e = (Expression) exprList.get(j);

            conditions.addCondition(e);
        }
    }

    /**
     * Assigns a set of conditions to a range variable.
     */
    void assignToRangeVariable(RangeVariable rangeVar,
                               RangeVariableConditions conditions,
                               int rangeVarIndex, HsqlArrayList exprList) {

        if (exprList.isEmpty()) {
            return;
        }

        for (int j = 0, size = exprList.size(); j < size; j++) {
            Expression e = (Expression) exprList.get(j);

            if (!e.isIndexable(rangeVar)) {
                conditions.addCondition(e);
                exprList.set(j, null);

                continue;
            }

            if (e.getType() == OpTypes.EQUAL
                    && e.exprSubType == OpTypes.ANY_QUANTIFIED) {
                OrderedIntHashSet set = new OrderedIntHashSet();

                ((ExpressionLogical) e).addLeftColumnsForAllAny(set);

                Index index = rangeVar.rangeTable.getIndexForColumns(set,
                    false);

                // code to disable IN optimisation
                // index = null;
                if (index != null && inExpressions[rangeVarIndex] == null) {
                    inExpressions[rangeVarIndex] = e;
                    inInJoin[rangeVarIndex]      = conditions.isJoin;

                    inExpressionCount++;

                    exprList.set(j, null);
                }

                continue;
            }
        }

        if (inExpressions[rangeVarIndex] == null) {
            setIndexConditions(conditions, exprList, true);
        } else {
            assignToRangeVariable(conditions, exprList);
        }
    }

    private void setIndexConditions(RangeVariableConditions conditions,
                                    HsqlArrayList exprList,
                                    boolean includeOr) {

        colIndexSetEqual.clear();
        colIndexSetOther.clear();

        for (int j = 0, size = exprList.size(); j < size; j++) {
            Expression e = (Expression) exprList.get(j);

            if (e == null) {
                continue;
            }

            if (conditions.hasIndexCondition()) {
                conditions.addCondition(e);
                exprList.set(j, null);

                continue;
            }

            // repeat check required for OR
            if (!e.isIndexable(conditions.rangeVar)) {
                conditions.addCondition(e);
                exprList.set(j, null);

                continue;
            }

            int type = e.getType();

            switch (type) {

                case OpTypes.OR : {
                    continue;
                }
                case OpTypes.COLUMN : {
                    continue;
                }
                case OpTypes.EQUAL : {
                    if (e.exprSubType == OpTypes.ANY_QUANTIFIED) {
                        continue;
                    }

                    int colIndex = e.getLeftNode().getColumnIndex();

                    colIndexSetEqual.add(colIndex);

                    break;
                }
                case OpTypes.IS_NULL : {
                    int colIndex = e.getLeftNode().getColumnIndex();

                    colIndexSetEqual.add(colIndex);

                    break;
                }
                case OpTypes.NOT : {
                    int colIndex =
                        e.getLeftNode().getLeftNode().getColumnIndex();

                    colIndexSetOther.add(colIndex);

                    break;
                }
                case OpTypes.SMALLER :
                case OpTypes.SMALLER_EQUAL :
                case OpTypes.GREATER :
                case OpTypes.GREATER_EQUAL :
                    int colIndex = e.getLeftNode().getColumnIndex();

                    colIndexSetOther.add(colIndex);
                    break;

                default : {
                    Error.runtimeError(ErrorCode.U_S0500,
                                       "RangeVariableResolver");
                }
            }
        }

        Index idx =
            conditions.rangeVar.rangeTable.getIndexForColumns(colIndexSetEqual,
                false);

        if (idx != null) {
            setEqaulityConditions(conditions, exprList, idx);

            return;
        }

        idx = conditions.rangeVar.rangeTable.getIndexForColumns(
            colIndexSetOther, false);

        if (idx != null) {
            setNonEqualityConditions(conditions, exprList, idx);

            return;
        }

        // no index found
        for (int j = 0, size = exprList.size(); j < size; j++) {
            Expression e = (Expression) exprList.get(j);

            if (e == null) {
                continue;
            }

            boolean result = false;

            if (includeOr && e.getType() == OpTypes.OR) {

                // already done
                boolean indexable =
                    ((ExpressionLogical) e).isIndexable(conditions.rangeVar);

                result = setOrConditions(conditions, (ExpressionLogical) e);
            }

            if (!result) {
                conditions.addCondition(e);
                exprList.set(j, null);
            }
        }
    }

    private boolean setOrConditions(RangeVariableConditions conditions,
                                    ExpressionLogical orExpression) {

        HsqlArrayList orExprList = new HsqlArrayList();

        decomposeOrConditions(orExpression, orExprList);

        RangeVariableConditions[] conditionsArray =
            new RangeVariableConditions[orExprList.size()];

        for (int i = 0; i < orExprList.size(); i++) {
            HsqlArrayList exprList = new HsqlArrayList();
            Expression    e        = (Expression) orExprList.get(i);

            decomposeAndConditions(e, exprList);

            RangeVariableConditions c =
                new RangeVariableConditions(conditions);

            setIndexConditions(c, exprList, false);

            conditionsArray[i] = c;

            if (!c.hasIndexCondition()) {

                // deep OR
                return false;
            }
        }

        Expression e = null;

        for (int i = 0; i < conditionsArray.length; i++) {
            RangeVariableConditions c = conditionsArray[i];

            c.excludeConditions = e;

            if (c.indexCond != null) {
                for (int k = 0; k < c.indexedColumnCount; k++) {
                    e = ExpressionLogical.andExpressions(
                        e, c.indexCond[k]);
                }
            }

            e = ExpressionLogical.andExpressions(e, c.indexEndCondition);
            e = ExpressionLogical.andExpressions(e, c.nonIndexCondition);
        }

        if (conditions.isJoin) {
            conditions.rangeVar.joinConditions = conditionsArray;
            conditionsArray = new RangeVariableConditions[orExprList.size()];

            ArrayUtil.fillArray(conditionsArray,
                                conditions.rangeVar.whereConditions[0]);

            conditions.rangeVar.whereConditions = conditionsArray;
        } else {
            conditions.rangeVar.whereConditions = conditionsArray;
            conditionsArray = new RangeVariableConditions[orExprList.size()];

            ArrayUtil.fillArray(conditionsArray,
                                conditions.rangeVar.joinConditions[0]);

            conditions.rangeVar.joinConditions = conditionsArray;
        }

        return true;
    }

    private void setEqaulityConditions(RangeVariableConditions conditions,
                                       HsqlArrayList exprList, Index idx) {

        int[]        cols                = idx.getColumns();
        int          colCount            = cols.length;
        Expression[] firstRowExpressions = new Expression[cols.length];

        for (int j = 0; j < exprList.size(); j++) {
            Expression e = (Expression) exprList.get(j);

            if (e == null) {
                continue;
            }

            int type = e.getType();

            if (type == OpTypes.EQUAL || type == OpTypes.IS_NULL) {
                int offset = ArrayUtil.find(cols,
                                            e.getLeftNode().getColumnIndex());

                if (offset != -1 && firstRowExpressions[offset] == null) {
                    firstRowExpressions[offset] = e;

                    exprList.set(j, null);

                    continue;
                }
            }
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
                conditions.addCondition(e);

                firstRowExpressions[i] = null;
            }
        }

        conditions.addIndexCondition(firstRowExpressions, idx, colCount);


        for (int j = 0; j < exprList.size(); j++) {
            Expression e = (Expression) exprList.get(j);

            if (e == null) {
                continue;
            }

            // not used in index lookup
            conditions.addCondition(e);
            exprList.set(j, null);

        }
    }

    private void setNonEqualityConditions(RangeVariableConditions conditions,
                                          HsqlArrayList exprList, Index idx) {

        int[] cols = idx.getColumns();

        for (int j = 0; j < exprList.size(); j++) {
            Expression e = (Expression) exprList.get(j);

            if (e == null) {
                continue;
            }

            if (conditions.hasIndexCondition()) {
                conditions.addCondition(e);
                exprList.set(j, null);

                continue;
            }

            boolean isIndexed = false;

            if (e.getType() == OpTypes.NOT
                    && e.getLeftNode().getType() == OpTypes.IS_NULL
                    && cols[0]
                       == e.getLeftNode().getLeftNode().getColumnIndex()) {
                isIndexed = true;
            }

            if (cols[0] == e.getLeftNode().getColumnIndex()) {
                if (e.getRightNode() != null
                        && !e.getRightNode().isCorrelated()) {
                    isIndexed = true;
                }

                if (e.getType() == OpTypes.IS_NULL) {
                    isIndexed = true;
                }
            }

            if (isIndexed) {
                conditions.addIndexCondition(new Expression[]{ e }, idx, 1);
            } else {
                conditions.addCondition(e);
            }

            exprList.set(j, null);
        }
    }

    /**
     * Converts an IN conditions into a JOIN
     */
    void setInConditionsAsTables() {

        for (int i = rangeVariables.length - 1; i >= 0; i--) {
            RangeVariable     rangeVar = rangeVariables[i];
            ExpressionLogical in       = (ExpressionLogical) inExpressions[i];

            if (in != null) {
                OrderedIntHashSet set = new OrderedIntHashSet();

                in.addLeftColumnsForAllAny(set);

                Index index = rangeVar.rangeTable.getIndexForColumns(set,
                    false);
                int colCount = 0;

                for (int j = 0; j < index.getColumnCount(); j++) {
                    if (set.contains(index.getColumns()[j])) {
                        colCount++;
                    }
                }

                RangeVariable newRangeVar =
                    new RangeVariable(in.getRightNode().subQuery.getTable(),
                                      null, null, null, compileContext);
                RangeVariable[] newList =
                    new RangeVariable[rangeVariables.length + 1];

                ArrayUtil.copyAdjustArray(rangeVariables, newList,
                                          newRangeVar, i, 1);

                rangeVariables = newList;

                // make two columns as arg
                Expression[] exprList = new Expression[colCount];

                for (int j = 0; j < colCount; j++) {
                    int leftIndex  = index.getColumns()[j];
                    int rightIndex = set.getIndex(leftIndex);
                    Expression e = new ExpressionLogical(rangeVar, leftIndex,
                                                         newRangeVar,
                                                         rightIndex);

                    exprList[j] = e;
                }

                boolean isOuter = rangeVariables[i].isLeftJoin
                                  || rangeVariables[i].isRightJoin;
                RangeVariableConditions conditions =
                    !inInJoin[i] && isOuter ? rangeVar.whereConditions[0]
                                            : rangeVar.joinConditions[0];

                conditions.addIndexCondition(exprList, index, exprList.length);
                conditions.addCondition(in);
            }
        }
    }
}
