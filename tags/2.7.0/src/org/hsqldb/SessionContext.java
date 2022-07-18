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

import org.hsqldb.RangeGroup.RangeGroupSimple;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.List;
import org.hsqldb.lib.LongDeque;
import org.hsqldb.lib.OrderedHashMap;
import org.hsqldb.map.ValuePool;
import org.hsqldb.navigator.RangeIterator;
import org.hsqldb.navigator.RowSetNavigatorDataChange;
import org.hsqldb.navigator.RowSetNavigatorDataChangeMemory;
import org.hsqldb.result.Result;

/*
 * Session execution context and temporary data structures
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.0
 * @since 1.9.0
 */
public class SessionContext {

    Session session;

    //
    public boolean isAutoCommit;
    boolean        isReadOnly;
    boolean        noSQL;
    int            autoCommitRows;
    int            currentMaxRows;

    //
    OrderedHashMap  sessionVariables;
    RangeVariable[] sessionVariablesRange;
    RangeGroup[]    sessionVariableRangeGroups;

    //
    private HsqlArrayList stack;
    Object[]              diagnosticsVariables = ValuePool.emptyObjectArray;
    Object[]              routineArguments     = ValuePool.emptyObjectArray;
    Object[]              routineVariables     = ValuePool.emptyObjectArray;
    Result[]              routineCursors       = Result.emptyArray;
    Object[]              dynamicArguments     = ValuePool.emptyObjectArray;
    Object[][]            triggerArguments     = null;
    public int            depth;
    boolean               isInRoutine;

    //
    Number         lastIdentity = ValuePool.INTEGER_0;
    OrderedHashMap savepoints;
    LongDeque      savepointTimestamps;

    // range variable data
    RangeIterator[] rangeIterators;

    // grouping sets data
    GroupSet groupSet;
    List     currentGroup;

    // session tables
    OrderedHashMap sessionTables;
    OrderedHashMap popSessionTables;

    //
    public Statement currentStatement;

    //
    public int rownum;

    /**
     * Reusable set of all FK constraints that have so far been enforced while
     * a cascading insert or delete is in progress.
     */
    HashSet               constraintPath;
    StatementResultUpdate rowUpdateStatement = new StatementResultUpdate();

    //

    /**
     * Creates a new instance of CompiledStatementExecutor.
     *
     * @param session the context in which to perform the execution
     */
    SessionContext(Session session) {

        this.session = session;
        diagnosticsVariables =
            new Object[ExpressionColumn.diagnosticsVariableTokens.length];
        rangeIterators        = new RangeIterator[8];
        savepoints            = new OrderedHashMap(4);
        savepointTimestamps   = new LongDeque();
        sessionVariables      = new OrderedHashMap();
        sessionVariablesRange = new RangeVariable[1];
        sessionVariablesRange[0] = new RangeVariable(sessionVariables, null,
                true, RangeVariable.VARIALBE_RANGE);
        sessionVariableRangeGroups = new RangeGroup[]{
            new RangeGroupSimple(sessionVariablesRange, true) };
        isAutoCommit = false;
        isReadOnly   = false;
        noSQL        = false;
        isInRoutine  = false;
    }

    void resetStack() {

        while (depth > 0) {
            pop(isInRoutine);
        }
    }

    public void push() {
        push(false);
    }

    private void push(boolean isRoutine) {

        if (depth > 256) {
            throw Error.error(ErrorCode.GENERAL_ERROR);
        }

        session.sessionData.persistentStoreCollection.push(isRoutine);

        if (stack == null) {
            stack = new HsqlArrayList(32, true);
        }

        stack.add(diagnosticsVariables);
        stack.add(dynamicArguments);
        stack.add(routineArguments);
        stack.add(triggerArguments);
        stack.add(routineVariables);
        stack.add(routineCursors);
        stack.add(rangeIterators);
        stack.add(savepoints);
        stack.add(savepointTimestamps);
        stack.add(lastIdentity);
        stack.add(isAutoCommit);
        stack.add(isReadOnly);
        stack.add(noSQL);
        stack.add(isInRoutine);
        stack.add(ValuePool.getInt(currentMaxRows));
        stack.add(ValuePool.getInt(rownum));

        diagnosticsVariables =
            new Object[ExpressionColumn.diagnosticsVariableTokens.length];
        rangeIterators      = new RangeIterator[8];
        savepoints          = new OrderedHashMap(4);
        savepointTimestamps = new LongDeque();
        isAutoCommit        = false;
        currentMaxRows      = 0;
        isInRoutine         = Boolean.valueOf(isRoutine);

        depth++;
    }

    public void pop() {
        pop(false);
    }

    private void pop(boolean isRoutine) {

        session.sessionData.persistentStoreCollection.pop(isRoutine);

        rownum = ((Integer) stack.remove(stack.size() - 1)).intValue();
        currentMaxRows = ((Integer) stack.remove(stack.size() - 1)).intValue();
        isInRoutine          = (Boolean) stack.remove(stack.size() - 1);
        noSQL                = (Boolean) stack.remove(stack.size() - 1);
        isReadOnly           = (Boolean) stack.remove(stack.size() - 1);
        isAutoCommit         = (Boolean) stack.remove(stack.size() - 1);
        lastIdentity         = (Number) stack.remove(stack.size() - 1);
        savepointTimestamps  = (LongDeque) stack.remove(stack.size() - 1);
        savepoints           = (OrderedHashMap) stack.remove(stack.size() - 1);
        rangeIterators = (RangeIterator[]) stack.remove(stack.size() - 1);
        routineCursors       = (Result[]) stack.remove(stack.size() - 1);
        routineVariables     = (Object[]) stack.remove(stack.size() - 1);
        triggerArguments     = ((Object[][]) stack.remove(stack.size() - 1));
        routineArguments     = (Object[]) stack.remove(stack.size() - 1);
        dynamicArguments     = (Object[]) stack.remove(stack.size() - 1);
        diagnosticsVariables = (Object[]) stack.remove(stack.size() - 1);

        depth--;
    }

    public void pushRoutineInvocation() {
        push(true);
    }

    public void popRoutineInvocation() {
        pop(true);
    }

    public void pushDynamicArguments(Object[] args) {

        push();

        dynamicArguments = args;
    }

    public void pushStatementState() {

        if (stack == null) {
            stack = new HsqlArrayList(32, true);
        }

        stack.add(ValuePool.getInt(rownum));
    }

    public void popStatementState() {
        rownum = ((Integer) stack.remove(stack.size() - 1)).intValue();
    }

    public void setDynamicArguments(Object[] args) {
        dynamicArguments = args;
    }

    RowSetNavigatorDataChange getRowSetDataChange() {
        return new RowSetNavigatorDataChangeMemory(session);
    }

    void clearStructures(StatementDMQL cs) {

        int count = cs.rangeIteratorCount;

        if (count > rangeIterators.length) {
            count = rangeIterators.length;
        }

        for (int i = 0; i < count; i++) {
            if (rangeIterators[i] != null) {
                rangeIterators[i].release();

                rangeIterators[i] = null;
            }
        }
    }

    RangeIterator checkIterator = new RangeVariable.RangeIteratorCheck();

    public RangeIterator getCheckIterator(RangeVariable rangeVariable) {

        int position = rangeVariable.rangePosition;

        if (position >= rangeIterators.length) {
            int size = (int) ArrayUtil.getBinaryNormalisedCeiling(position
                + 1);

            rangeIterators =
                (RangeIterator[]) ArrayUtil.resizeArray(rangeIterators, size);
        }

        rangeIterators[position] = checkIterator;

        return checkIterator;
    }

    public void setRangeIterator(RangeIterator iterator) {

        int position = iterator.getRangePosition();

        if (position >= rangeIterators.length) {
            int size = (int) ArrayUtil.getBinaryNormalisedCeiling(position
                + 1);

            rangeIterators =
                (RangeIterator[]) ArrayUtil.resizeArray(rangeIterators, size);
        }

        rangeIterators[position] = iterator;
    }

    public RangeIterator getRangeIterator(int position) {

        RangeIterator[] ranges = rangeIterators;

        if (stack != null) {
            for (int i = 0; i < stack.size(); i++) {
                Object o = stack.get(i);

                if (o instanceof RangeIterator[]) {
                    ranges = (RangeIterator[]) o;

                    break;
                }
            }
        }

        return ranges[position];
    }

    public void unsetRangeIterator(RangeIterator iterator) {

        int position = iterator.getRangePosition();

        rangeIterators[position] = null;
    }

    public void setGroupSet(GroupSet set) {
        groupSet = set;
    }

    public void setGroup(List group) {
        currentGroup = group;
    }

    /**
     * For cascade operations
     */
    public HashSet getConstraintPath() {

        if (constraintPath == null) {
            constraintPath = new HashSet();
        } else {
            constraintPath.clear();
        }

        return constraintPath;
    }

    public void addSessionVariable(ColumnSchema variable) {

        int index = sessionVariables.size();

        if (!sessionVariables.add(variable.getName().name, variable)) {
            throw Error.error(ErrorCode.X_42504);
        }

        Object[] vars = new Object[sessionVariables.size()];

        ArrayUtil.copyArray(routineVariables, vars, routineVariables.length);

        routineVariables        = vars;
        routineVariables[index] = variable.getDefaultValue(session);
    }

    public void pushRoutineTables() {
        popSessionTables = sessionTables;
        sessionTables    = new OrderedHashMap();
    }

    public void popRoutineTables() {

        sessionTables.clear();

        sessionTables = popSessionTables;
    }

    public void addSessionTable(Table table) {

        if (sessionTables == null) {
            sessionTables = new OrderedHashMap();
        }

        if (sessionTables.containsKey(table.getName().name)) {
            throw Error.error(ErrorCode.X_42504);
        }

        sessionTables.add(table.getName().name, table);
    }

    public void setSessionTables(Table[] tables) {}

    public Table findSessionTable(String name) {

        if (sessionTables == null) {
            return null;
        }

        return (Table) sessionTables.get(name);
    }

    public void dropSessionTable(String name) {
        sessionTables.remove(name);
    }
}
