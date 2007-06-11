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

import org.hsqldb.lib.OrderedHashSet;

/**
 * Implementation of SQL TRIGGER objects.<p>
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class TriggerDefSQL extends TriggerDef {

    Table[]             transitions;
    RangeVariable[]     rangeVars;
    Expression          condition;
    CompiledStatement[] compiledStatements;
    boolean             hasTransitionTables;
    boolean             hasTransitionRanges;
    String              conditionSQL;
    String              procedureSQL;
    OrderedHashSet      references;

    public TriggerDefSQL(HsqlNameManager.HsqlName name, String when,
                         String operation, boolean forEachRow, Table table,
                         Table[] transitions, RangeVariable[] rangeVars,
                         Expression condition,
                         CompiledStatement[] compiledStatements,
                         String conditionSQL, String procedureSQL,
                         OrderedHashSet references) throws HsqlException {

        this.name               = name;
        this.when               = when;
        this.operation          = operation;
        this.forEachRow         = forEachRow;
        this.table              = table;
        this.transitions        = transitions;
        this.rangeVars          = rangeVars;
        this.condition          = condition == null ? Expression.EXPR_TRUE
                                                    : condition;
        this.compiledStatements = compiledStatements;
        this.conditionSQL       = conditionSQL;
        this.procedureSQL       = procedureSQL;
        this.references         = references;
        hasTransitionRanges = transitions[OLD_ROW] != null
                              || transitions[NEW_ROW] != null;
        hasTransitionTables = transitions[OLD_TABLE] != null
                              || transitions[NEW_TABLE] != null;

        setUpIndexesAndTypes();

        //
    }

    public OrderedHashSet getReferences() {
        return references;
    }

    public void compile(Session session) throws HsqlException {}

    public String getClassName() {
        return null;
    }

    public boolean hasOldTable() {
        return transitions[OLD_TABLE] != null;
    }

    public boolean hasNewTable() {
        return transitions[NEW_TABLE] != null;
    }

    synchronized void pushPair(Session session, Object[] oldData,
                               Object[] newData) throws HsqlException {

        if (transitions[OLD_ROW] != null) {
            rangeVars[OLD_ROW].getIterator(session).currentData = oldData;
        }

        if (transitions[NEW_ROW] != null) {
            rangeVars[NEW_ROW].getIterator(session).currentData = newData;
        }

        if (!condition.testCondition(session)) {
            return;
        }

        for (int i = 0; i < compiledStatements.length; i++) {
            session.compiledStatementExecutor.execute(compiledStatements[i],
                    null);
        }
    }

    public String getDDL() {

        boolean      isBlock = compiledStatements.length > 1;
        StringBuffer a       = new StringBuffer(256);

        a.append(Token.T_CREATE).append(' ');
        a.append(Token.T_TRIGGER).append(' ');
        a.append(name.statementName).append(' ');
        a.append(when).append(' ');
        a.append(operation).append(' ');
        a.append(Token.T_ON).append(' ');
        a.append(table.getName().statementName).append(' ');

        if (hasTransitionRanges || hasTransitionTables) {
            a.append(Token.T_REFERENCING).append(' ');

            String separator = "";

            if (transitions[OLD_ROW] != null) {
                a.append(Token.T_OLD).append(' ').append(Token.T_ROW);
                a.append(' ').append(Token.T_AS).append(' ');
                a.append(transitions[OLD_ROW].getName().statementName);

                separator = Token.T_COMMA;
            }

            if (transitions[NEW_ROW] != null) {
                a.append(separator);
                a.append(Token.T_NEW).append(' ').append(Token.T_ROW);
                a.append(' ').append(Token.T_AS).append(' ');
                a.append(transitions[NEW_ROW].getName().statementName);

                separator = Token.T_COMMA;
            }

            if (transitions[OLD_TABLE] != null) {
                a.append(separator);
                a.append(Token.T_OLD).append(' ').append(Token.T_TABLE);
                a.append(' ').append(Token.T_AS).append(' ');
                a.append(transitions[OLD_TABLE].getName().statementName);

                separator = Token.T_COMMA;
            }

            if (transitions[NEW_TABLE] != null) {
                a.append(separator);
                a.append(Token.T_OLD).append(' ').append(Token.T_TABLE);
                a.append(' ').append(Token.T_AS).append(' ');
                a.append(transitions[NEW_TABLE].getName().statementName);
            }

            a.append(' ');
        }

        if (forEachRow) {
            a.append(Token.T_FOR).append(' ');
            a.append(Token.T_EACH).append(' ');
            a.append(Token.T_ROW).append(' ');
        }

        if (condition != Expression.EXPR_TRUE) {
            a.append(Token.T_WHEN).append(' ');
            a.append(Token.T_OPENBRACKET).append(conditionSQL);
            a.append(Token.T_CLOSEBRACKET).append(' ');
        }

        if (isBlock) {
            a.append(Token.T_BEGIN).append(' ').append(Token.T_ATOMIC);
            a.append(' ');
        }

        a.append(procedureSQL).append(' ');

        if (isBlock) {
            a.append(Token.T_END);
        }

        return a.toString();
    }
}
