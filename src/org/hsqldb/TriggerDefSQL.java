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

import org.hsqldb.lib.OrderedHashSet;

/**
 * Implementation of SQL TRIGGER objects.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class TriggerDefSQL extends TriggerDef {

    OrderedHashSet references;

    public TriggerDefSQL(HsqlNameManager.HsqlName name, String when,
                         String operation, boolean forEachRow, Table table,
                         Table[] transitions, RangeVariable[] rangeVars,
                         Expression condition, String conditionSQL,
                         int[] updateColumns,
                         StatementDMQL[] compiledStatements,
                         String procedureSQL, OrderedHashSet references) {

        super(name, when, operation, forEachRow, table, transitions,
              rangeVars, condition, conditionSQL, updateColumns);

        this.statements   = compiledStatements;
        this.procedureSQL = procedureSQL;
        this.references   = references;
    }

    public OrderedHashSet getReferences() {
        return references;
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session, SchemaObject parentObject) {}

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
                               Object[] newData) {

        if (transitions[OLD_ROW] != null) {
            rangeVars[OLD_ROW].getIterator(session).setCurrent(oldData);
        }

        if (transitions[NEW_ROW] != null) {
            rangeVars[NEW_ROW].getIterator(session).setCurrent(newData);
        }

        if (!condition.testCondition(session)) {
            return;
        }

        for (int i = 0; i < statements.length; i++) {
            statements[i].execute(session);
        }
    }

    public String getSQL() {

        boolean      isBlock = statements.length > 1;
        StringBuffer sb      = getSQLMain();

        if (isBlock) {
            sb.append(Tokens.T_BEGIN).append(' ').append(Tokens.T_ATOMIC);
            sb.append(' ');
        }

        sb.append(procedureSQL).append(' ');

        if (isBlock) {
            sb.append(Tokens.T_END);
        }

        return sb.toString();
    }
}
