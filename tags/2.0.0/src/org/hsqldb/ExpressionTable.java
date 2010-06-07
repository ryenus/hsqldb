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
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.navigator.RowSetNavigatorData;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.result.Result;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.RowType;
import org.hsqldb.types.Type;

/**
 * Implementation of table conversion.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.0
 * @since 2.0.0
 */
public class ExpressionTable extends Expression {

    boolean isTable;
    boolean ordinality = false;

    /**
     * Creates an UNNEST ARRAY or MULTISET expression
     */
    ExpressionTable(Expression e, SubQuery sq, boolean ordinality) {

        super(OpTypes.TABLE);

        nodes           = new Expression[]{ e };
        this.subQuery   = sq;
        this.ordinality = ordinality;
    }

    public String getSQL() {

        if (isTable) {
            return Tokens.T_TABLE;
        } else {
            return Tokens.T_UNNEST;
        }
    }

    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer(64);

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        if (isTable) {
            sb.append(Tokens.T_TABLE).append(' ');
        } else {
            sb.append(Tokens.T_UNNEST).append(' ');
        }

        sb.append(nodes[LEFT].describe(session, blanks));

        return sb.toString();
    }

    public void resolveTypes(Session session, Expression parent) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        if (nodes[LEFT].dataType.isRowType()) {
            isTable       = true;
            nodeDataTypes = ((RowType) nodes[LEFT].dataType).getTypesArray();

            subQuery.prepareTable(session);

            subQuery.getTable().columnList =
                ((FunctionSQLInvoked) nodes[LEFT]).routine.getTable()
                    .columnList;
        } else {
            isTable = false;

            int columnCount = ordinality ? 2
                                         : 1;

            nodeDataTypes       = new Type[columnCount];
            nodeDataTypes[LEFT] = nodes[LEFT].dataType.collectionBaseType();

            if (ordinality) {
                nodeDataTypes[RIGHT] = Type.SQL_INTEGER;
            }

            subQuery.prepareTable(session);
        }
    }

    public Result getResult(Session session) {

        switch (opType) {

            case OpTypes.TABLE : {
                RowSetNavigatorData navigator = subQuery.getNavigator(session);
                Result              result    = Result.newResult(navigator);

                result.metaData = subQuery.queryExpression.getMetaData();

                return result;
            }
            default : {
                throw Error.runtimeError(ErrorCode.U_S0500, "ExpressionTable");
            }
        }
    }

    public Object[] getRowValue(Session session) {

        switch (opType) {

            case OpTypes.TABLE : {
                return subQuery.queryExpression.getValues(session);
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    Object getValue(Session session, Type type) {

        switch (opType) {

            case OpTypes.TABLE : {
                materialise(session);

                Object[] value = subQuery.getValues(session);

                if (value.length == 1) {
                    return ((Object[]) value)[0];
                }

                return value;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    public Object getValue(Session session) {
        return valueData;
    }

    void insertValuesIntoSubqueryTable(Session session,
                                       PersistentStore store) {

        if (isTable) {
            Result          result = nodes[LEFT].getResult(session);
            RowSetNavigator nav    = result.navigator;
            int             size   = nav.getSize();

            while (nav.hasNext()) {
                Object[] data = nav.getNext();
                Row      row  = (Row) store.getNewCachedObject(session, data);

                try {
                    store.indexRow(session, row);
                } catch (HsqlException e) {}
            }
        } else {
            Object[] array = (Object[]) nodes[LEFT].getValue(session);

            for (int i = 0; i < array.length; i++) {
                Object[] data;

                if (ordinality) {
                    data = new Object[] {
                        array[i], ValuePool.getInt(i)
                    };
                } else {
                    data = new Object[]{ array[i] };
                }

                Row row = (Row) store.getNewCachedObject(session, data);

                try {
                    store.indexRow(session, row);
                } catch (HsqlException e) {}
            }
        }
    }
}
