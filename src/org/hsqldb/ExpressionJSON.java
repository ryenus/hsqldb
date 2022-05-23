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

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.OrderedHashMap;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.types.ArrayType;
import org.hsqldb.types.Type;

/**
 * JSON functions
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.0
 * @since 2.7.0
 */
public interface ExpressionJSON {

    class ExpressionJSONWrapper extends Expression {

        public ExpressionJSONWrapper(Expression expr) {

            super(OpTypes.JSON_FUNCTION);

            nodes = new Expression[]{ expr };
        }

        public void resolveTypes(Session session, Expression parent) {

            nodes[LEFT].resolveTypes(session, this);

            dataType = nodes[LEFT].dataType;
        }

        public Object getValue(Session session) {
            return nodes[LEFT].getValue(session);
        }

        public String getSQL() {
            return nodes[LEFT].getSQL();
        }

        public String describe(Session session, int blanks) {
            return nodes[LEFT].getSQL();
        }
    }

    class ExpressionJSONArrayFromQuery extends Expression {

        final Expression exprQuery;
        final boolean    nullOnNull;

        public ExpressionJSONArrayFromQuery(Expression expressionQuery,
                                            boolean nullOnNull,
                                            Type dataType) {

            super(OpTypes.JSON_FUNCTION);

            this.exprQuery  = expressionQuery;
            this.nullOnNull = nullOnNull;
            this.dataType   = dataType == null ? Type.SQL_VARCHAR_LONG
                                               : dataType;
            nodes           = new Expression[]{ expressionQuery };
        }

        public void resolveTypes(Session session, Expression parent) {
            nodes[LEFT].resolveTypes(session, this);
        }

        public Object getValue(Session session) {

            StringBuilder sb     = new StringBuilder();
            Object        values = nodes[LEFT].getValue(session);

            nodes[LEFT].dataType.convertToJSON(values, sb);

            if (sb.length() > dataType.precision) {
                throw Error.error(ErrorCode.X_22001);
            }

            return sb.toString();
        }

        public String getSQL() {
            return Tokens.T_JSON_ARRAY + "()";
        }

        public String describe(Session session, int blanks) {
            return Tokens.T_JSON_ARRAY + "()";
        }
    }

    class ExpressionJSONArrayFromValues extends Expression {

        final HsqlArrayList exprList;
        final boolean       nullOnNull;

        public ExpressionJSONArrayFromValues(HsqlArrayList expressionList,
                                             boolean nullOnNull,
                                             Type dataType) {

            super(OpTypes.JSON_FUNCTION);

            this.exprList   = expressionList;
            this.nullOnNull = nullOnNull;
            this.dataType   = dataType == null ? Type.SQL_VARCHAR_LONG
                                               : dataType;
            nodes           = new Expression[expressionList.size()];

            expressionList.toArray(nodes);
        }

        public void resolveTypes(Session session, Expression parent) {

            for (int i = 0; i < nodes.length; i++) {
                if (nodes[i] != null) {
                    nodes[i].resolveTypes(session, this);
                }
            }
        }

        public Object getValue(Session session) {

            StringBuilder sb    = new StringBuilder();
            int           count = 0;

            sb.append('[');

            for (int i = 0; i < nodes.length; i++) {
                if (count > 0) {
                    sb.append(',');
                }

                Object value = nodes[i].getValue(session);

                if (nodes[i].opType == OpTypes.JSON_FUNCTION) {
                    sb.append((String) value);
                } else {
                    nodes[i].dataType.convertToJSON(value, sb);
                }

                count++;
            }

            sb.append(']');

            if (sb.length() > dataType.precision) {
                throw Error.error(ErrorCode.X_22001);
            }

            return sb.toString();
        }

        public String getSQL() {
            return Tokens.T_JSON_ARRAY + "()";
        }

        public String describe(Session session, int blanks) {
            return Tokens.T_JSON_ARRAY + "()";
        }
    }

    class ExpressionJSONArrayAgg extends Expression {

        final boolean isValueJSON;
        final boolean nullOnNull;

        public ExpressionJSONArrayAgg(ExpressionArrayAggregate valuesAgg,
                                      boolean nullOnNull, Type dataType) {

            super(OpTypes.JSON_FUNCTION);

            this.nullOnNull  = nullOnNull;
            this.dataType    = dataType == null ? Type.SQL_VARCHAR_LONG
                                                : dataType;
            this.isValueJSON = valuesAgg.exprOpType == OpTypes.JSON_FUNCTION;
            nodes            = new Expression[]{ valuesAgg };
        }

        public void resolveTypes(Session session, Expression parent) {
            nodes[LEFT].resolveTypes(session, this);
        }

        public Object getValue(Session session) {

            StringBuilder sb         = new StringBuilder();
            Object        values     = nodes[LEFT].getValue(session);
            ArrayType     valuesType = (ArrayType) nodes[LEFT].dataType;

            if (isValueJSON) {
                valuesType.convertToJSONsimple(values, sb);
            } else {
                valuesType.convertToJSON(values, sb);
            }

            if (sb.length() > dataType.precision) {
                throw Error.error(ErrorCode.X_22001);
            }

            return sb.toString();
        }

        public String getSQL() {
            return Tokens.T_JSON_ARRAYAGG + "()";
        }

        public String describe(Session session, int blanks) {
            return Tokens.T_JSON_ARRAYAGG + "()";
        }
    }

    class ExpressionJSONObject extends Expression {

        final OrderedHashMap exprMap;
        final boolean        nullOnNull;
        final boolean        uniqueKeys;

        public ExpressionJSONObject(OrderedHashMap exprMap,
                                    boolean nullOnNull, boolean uniqueKeys,
                                    Type dataType) {

            super(OpTypes.JSON_FUNCTION);

            this.exprMap    = exprMap;
            this.nullOnNull = nullOnNull;
            this.uniqueKeys = uniqueKeys;
            this.dataType   = dataType == null ? Type.SQL_VARCHAR_LONG
                                               : dataType;
            nodes           = new Expression[exprMap.size() * 2];

            for (int i = 0; i < exprMap.size(); i++) {
                nodes[i * 2]     = (Expression) exprMap.getKeyAt(i);
                nodes[i * 2 + 1] = (Expression) exprMap.get(i);
            }
        }

        public void resolveTypes(Session session, Expression parent) {

            for (int i = 0; i < nodes.length; i++) {
                if (nodes[i] != null) {
                    nodes[i].resolveTypes(session, this);
                }
            }
        }

        public Object getValue(Session session) {

            StringBuilder  sb     = new StringBuilder();
            int            count  = 0;
            OrderedHashSet keySet = new OrderedHashSet();

            sb.append('{');

            for (int i = 0; i < exprMap.size(); i++) {
                int j = i * 2;

                if (count > 0) {
                    sb.append(',');
                }

                Object name = nodes[j].getValue(session);

                if (uniqueKeys) {
                    String s = nodes[j].dataType.convertToString(name);

                    if (!keySet.add(s)) {
                        throw Error.error(ErrorCode.X_23505);
                    }
                }

                if (nodes[j].dataType.isCharacterType()) {
                    nodes[j].dataType.convertToJSON(name, sb);
                } else {
                    String s = nodes[j].dataType.convertToString(name);

                    s = StringConverter.toQuotedString(s, '"', false);

                    sb.append(s);
                }

                sb.append(':');

                Object value = nodes[j + 1].getValue(session);

                if (nodes[j + 1].opType == OpTypes.JSON_FUNCTION) {
                    sb.append((String) value);
                } else {
                    nodes[j + 1].dataType.convertToJSON(value, sb);
                }

                count++;
            }

            sb.append('}');

            if (sb.length() > dataType.precision) {
                throw Error.error(ErrorCode.X_22001);
            }

            return sb.toString();
        }

        public String getSQL() {
            return Tokens.T_JSON_OBJECT + "()";
        }

        public String describe(Session session, int blanks) {
            return Tokens.T_JSON_OBJECT + "()";
        }
    }

    class ExpressionJSONObjectAgg extends Expression {

        final ExpressionArrayAggregate namesAgg;
        final ExpressionArrayAggregate valuesAgg;
        final boolean                  nullOnNull;
        final boolean                  uniqueKeys;
        boolean                        isValueJSON;

        public ExpressionJSONObjectAgg(ExpressionArrayAggregate namesAgg,
                                       ExpressionArrayAggregate valuesAgg,
                                       boolean nullOnNull, boolean uniqueKeys,
                                       Type dataType) {

            super(OpTypes.JSON_FUNCTION);

            this.namesAgg    = namesAgg;
            this.valuesAgg   = valuesAgg;
            this.nullOnNull  = nullOnNull;
            this.uniqueKeys  = uniqueKeys;
            this.dataType    = dataType == null ? Type.SQL_VARCHAR_LONG
                                                : dataType;
            this.isValueJSON = valuesAgg.exprOpType == OpTypes.JSON_FUNCTION;
            nodes            = new Expression[] {
                namesAgg, valuesAgg
            };
        }

        public void resolveTypes(Session session, Expression parent) {

            for (int i = 0; i < nodes.length; i++) {
                if (nodes[i] != null) {
                    nodes[i].resolveTypes(session, this);
                }
            }
        }

        public Object getValue(Session session) {

            StringBuilder  sb     = new StringBuilder();
            int            count  = 0;
            Object[]       names  = (Object[]) nodes[LEFT].getValue(session);
            Object[]       values = (Object[]) nodes[RIGHT].getValue(session);
            Type nameType         = nodes[LEFT].dataType.collectionBaseType();
            Type valueType        = nodes[RIGHT].dataType.collectionBaseType();
            OrderedHashSet keySet = new OrderedHashSet();

            sb.append('{');

            for (int i = 0; i < names.length; i++) {
                String name  = (String) names[i];
                String value = (String) values[i];

                if (name == null) {
                    continue;
                }

                if (!nullOnNull && value == null) {
                    continue;
                }

                if (count > 0) {
                    sb.append(',');
                }

                if (uniqueKeys) {
                    String s = nameType.convertToString(name);

                    if (!keySet.add(s)) {
                        throw Error.error(ErrorCode.X_23505);
                    }
                }

                if (nameType.isCharacterType()) {
                    nameType.convertToJSON(name, sb);
                } else {
                    String s = nameType.convertToString(name);

                    s = StringConverter.toQuotedString(s, '"', false);

                    sb.append(s);
                }

                sb.append(':');

                if (isValueJSON) {
                    sb.append((String) value);
                } else {
                    valueType.convertToJSON(value, sb);
                }

                count++;
            }

            sb.append('}');

            if (sb.length() > dataType.precision) {
                throw Error.error(ErrorCode.X_22001);
            }

            return sb.toString();
        }

        public String getSQL() {
            return Tokens.T_JSON_OBJECTAGG + "()";
        }

        public String describe(Session session, int blanks) {
            return Tokens.T_JSON_OBJECTAGG + "()";
        }
    }
}
