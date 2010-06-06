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

import org.hsqldb.RangeVariable.RangeIteratorBase;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.LongDeque;
import org.hsqldb.navigator.RangeIterator;
import org.hsqldb.store.ValuePool;

/*
 * Session execution context and temporary data structures
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class SessionContext {

    Session session;

    //
    Boolean isAutoCommit;
    Boolean isReadOnly;
    Boolean noSQL;
    int     currentMaxRows;

    //
    HashMappedList  sessionVariables;
    RangeVariable[] sessionVariablesRange;

    //
    private HsqlArrayList stack;
    public Object[]       routineArguments = ValuePool.emptyObjectArray;
    public Object[]       routineVariables = ValuePool.emptyObjectArray;
    Object[]              dynamicArguments = ValuePool.emptyObjectArray;
    public int            depth;

    //
    HashMappedList savepoints;
    LongDeque      savepointTimestamps;

    // range variable data
    RangeIterator[] rangeIterators;

    //
    public Statement currentStatement;

    /**
     * Reusable set of all FK constraints that have so far been enforced while
     * a cascading insert or delete is in progress.
     */
    HashSet               constraintPath;
    StatementResultUpdate rowUpdateStatement = new StatementResultUpdate();

    /**
     * Creates a new instance of CompiledStatementExecutor.
     *
     * @param session the context in which to perform the execution
     */
    SessionContext(Session session) {

        this.session             = session;
        rangeIterators           = new RangeIterator[4];
        savepoints               = new HashMappedList(4);
        savepointTimestamps      = new LongDeque();
        sessionVariables         = new HashMappedList();
        sessionVariablesRange    = new RangeVariable[1];
        sessionVariablesRange[0] = new RangeVariable(sessionVariables, true);
        isAutoCommit             = Boolean.FALSE;
        isReadOnly               = Boolean.FALSE;
        noSQL                    = Boolean.FALSE;
    }

    public void push() {

        if (stack == null) {
            stack = new HsqlArrayList(true);
        }

        stack.add(dynamicArguments);
        stack.add(routineArguments);
        stack.add(routineVariables);
        stack.add(rangeIterators);
        stack.add(savepoints);
        stack.add(savepointTimestamps);
        stack.add(isAutoCommit);
        stack.add(isReadOnly);
        stack.add(noSQL);
        stack.add(ValuePool.getInt(currentMaxRows));

        rangeIterators      = new RangeIterator[4];
        savepoints          = new HashMappedList(4);
        savepointTimestamps = new LongDeque();
        isAutoCommit        = Boolean.FALSE;
        currentMaxRows      = 0;

        depth++;
    }

    public void pop() {

        currentMaxRows = ((Integer) stack.remove(stack.size() - 1)).intValue();
        noSQL               = (Boolean) stack.remove(stack.size() - 1);
        isReadOnly          = (Boolean) stack.remove(stack.size() - 1);
        isAutoCommit        = (Boolean) stack.remove(stack.size() - 1);
        savepointTimestamps = (LongDeque) stack.remove(stack.size() - 1);
        savepoints          = (HashMappedList) stack.remove(stack.size() - 1);
        rangeIterators      = (RangeIterator[]) stack.remove(stack.size() - 1);
        routineVariables    = (Object[]) stack.remove(stack.size() - 1);
        routineArguments    = (Object[]) stack.remove(stack.size() - 1);
        dynamicArguments    = (Object[]) stack.remove(stack.size() - 1);

        depth--;
    }

    public void pushDynamicArguments(Object[] args) {

        push();

        dynamicArguments = args;
    }

    public void setDynamicArguments(Object[] args) {
        dynamicArguments = args;
    }

    void clearStructures(StatementDMQL cs) {

        int count = cs.rangeIteratorCount;

        if (count > rangeIterators.length) {
            count = rangeIterators.length;
        }

        for (int i = 0; i < count; i++) {
            if (rangeIterators[i] != null) {
                rangeIterators[i].reset();

                rangeIterators[i] = null;
            }
        }
    }

    public RangeIteratorBase getCheckIterator(RangeVariable rangeVariable) {

        RangeIterator it = rangeIterators[1];

        if (it == null) {
            it                = rangeVariable.getIterator(session);
            rangeIterators[1] = it;
        }

        return (RangeIteratorBase) it;
    }

    public void setRangeIterator(RangeIterator iterator) {

        int position = iterator.getRangePosition();

        if (position >= rangeIterators.length) {
            rangeIterators =
                (RangeIterator[]) ArrayUtil.resizeArray(rangeIterators,
                    position + 1);
        }

        rangeIterators[iterator.getRangePosition()] = iterator;
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
}
