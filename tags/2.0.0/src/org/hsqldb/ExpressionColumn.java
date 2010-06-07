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

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.HsqlNameManager.SimpleName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayListIdentity;
import org.hsqldb.lib.HsqlList;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.Set;
import org.hsqldb.types.Type;

/**
 * Implementation of column, variable, parameter, etc. access operations.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class ExpressionColumn extends Expression {

    public final static ExpressionColumn[] emptyArray =
        new ExpressionColumn[]{};

    //
    ColumnSchema  column;
    String        schema;
    String        tableName;
    String        columnName;
    RangeVariable rangeVariable;

    //
    NumberSequence sequence;
    boolean        isWritable;    // = false; true if column of writable table

    //
    boolean isParam;

    //
    boolean strictReference;

    /**
     * Creates a OpCodes.COLUMN expression
     */
    ExpressionColumn(String schema, String table, String column,
                     boolean strictReference) {

        super(OpTypes.COLUMN);

        this.schema          = schema;
        this.tableName       = table;
        this.columnName      = column;
        this.strictReference = strictReference;
    }

    ExpressionColumn(ColumnSchema column) {

        super(OpTypes.COLUMN);

        this.column   = column;
        this.dataType = column.getDataType();
        columnName    = column.getName().name;
    }

    ExpressionColumn(RangeVariable rangeVar, int index) {

        super(OpTypes.COLUMN);

        columnIndex = index;

        setAutoAttributesAsColumn(rangeVar, columnIndex);
    }

    /**
     * Creates a temporary OpCodes.COLUMN expression
     */
    ExpressionColumn(Expression e, int colIndex, int rangePosition) {

        super(OpTypes.SIMPLE_COLUMN);

        dataType           = e.dataType;
        columnIndex        = colIndex;
        alias              = e.alias;
        this.rangePosition = rangePosition;
    }

    ExpressionColumn() {
        super(OpTypes.ASTERISK);
    }

    ExpressionColumn(int type) {

        super(type);

        if (type == OpTypes.DYNAMIC_PARAM) {
            isParam = true;
        }
    }

    ExpressionColumn(Expression[] nodes, String name) {

        super(OpTypes.COALESCE);

        this.nodes      = nodes;
        this.columnName = name;
    }

    /**
     * Creates an OpCodes.ASTERISK expression
     */
    ExpressionColumn(String schema, String table) {

        super(OpTypes.MULTICOLUMN);

        this.schema = schema;
        tableName   = table;
    }

    /**
     * Creates a OpCodes.SEQUENCE expression
     */
    ExpressionColumn(NumberSequence sequence) {

        super(OpTypes.SEQUENCE);

        this.sequence = sequence;
        dataType      = sequence.getDataType();
    }

    void setAutoAttributesAsColumn(RangeVariable range, int i) {

        columnIndex   = i;
        column        = range.getColumn(i);
        dataType      = column.getDataType();
        columnName    = range.getColumnAlias(i);
        tableName     = range.getTableAlias();
        rangeVariable = range;

        rangeVariable.addColumn(columnIndex);
    }

    void setAttributesAsColumn(RangeVariable range, int i) {

        if (range.variables != null) {
            columnIndex   = i;
            column        = range.getColumn(i);
            dataType      = column.getDataType();
            rangeVariable = range;
        } else {
            columnIndex   = i;
            column        = range.getColumn(i);
            dataType      = column.getDataType();
            rangeVariable = range;

            rangeVariable.addColumn(columnIndex);
        }
    }

    void setAttributesAsColumn(ColumnSchema column, boolean isWritable) {

        this.column     = column;
        dataType        = column.getDataType();
        this.isWritable = isWritable;
    }

    SimpleName getSimpleName() {

        if (alias != null) {
            return alias;
        }

        if (column != null) {
            return column.getName();
        }

        if (opType == OpTypes.COALESCE) {
            return nodes[LEFT].getSimpleName();
        }

        return null;
    }

    String getAlias() {

        if (alias != null) {
            return alias.name;
        }

        if (opType == OpTypes.COLUMN) {
            return columnName;
        }

        if (opType == OpTypes.COALESCE) {
            return columnName;
        }

        return "";
    }

    public String getBaseColumnName() {

        if (opType == OpTypes.COLUMN && rangeVariable != null) {
            return rangeVariable.getTable().getColumn(
                columnIndex).getName().name;
        }

        return null;
    }

    public HsqlName getBaseColumnHsqlName() {
        return column.getName();
    }

    void collectObjectNames(Set set) {

        switch (opType) {

            case OpTypes.SEQUENCE :
                HsqlName name = sequence.getName();

                set.add(name);

                return;

            case OpTypes.MULTICOLUMN :
            case OpTypes.DYNAMIC_PARAM :
            case OpTypes.ASTERISK :
            case OpTypes.SIMPLE_COLUMN :
            case OpTypes.COALESCE :
                break;

            case OpTypes.PARAMETER :
            case OpTypes.VARIABLE :
                break;

            case OpTypes.COLUMN :
                set.add(column.getName());

                if (column.getName().parent != null) {
                    set.add(column.getName().parent);
                }

                return;
        }
    }

    String getColumnName() {

        if (opType == OpTypes.COLUMN && column != null) {
            return column.getName().name;
        }

        return getAlias();
    }

    ColumnSchema getColumn() {
        return column;
    }

    String getSchemaName() {
        return schema;
    }

    RangeVariable getRangeVariable() {
        return rangeVariable;
    }

    public HsqlList resolveColumnReferences(RangeVariable[] rangeVarArray,
            int rangeCount, HsqlList unresolvedSet, boolean acceptsSequences) {

        switch (opType) {

            case OpTypes.SEQUENCE :
                if (!acceptsSequences) {
                    throw Error.error(ErrorCode.X_42598);
                }
                break;

            case OpTypes.MULTICOLUMN :
            case OpTypes.DYNAMIC_PARAM :
            case OpTypes.ASTERISK :
            case OpTypes.SIMPLE_COLUMN :
            case OpTypes.COALESCE :
                break;

            case OpTypes.PARAMETER :
            case OpTypes.VARIABLE :
            case OpTypes.COLUMN : {
                boolean resolved       = false;
                boolean tableQualified = tableName != null;

                if (rangeVariable != null) {
                    return unresolvedSet;
                }

                for (int i = 0; i < rangeCount; i++) {
                    RangeVariable rangeVar = rangeVarArray[i];

                    if (rangeVar == null) {
                        continue;
                    }

                    if (resolved) {
                        if (resolvesDuplicateColumnReference(rangeVar)) {
                            if (strictReference) {
                                String message = getColumnName();

                                if (alias != null) {
                                    StringBuffer sb =
                                        new StringBuffer(message);

                                    sb.append(' ').append(Tokens.T_AS).append(
                                        ' ').append(alias.getStatementName());

                                    message = sb.toString();
                                }

                                throw Error.error(ErrorCode.X_42580, message);
                            }
                        }
                    } else {
                        if (resolveColumnReference(rangeVar)) {
                            if (tableQualified) {
                                return unresolvedSet;
                            }

                            resolved = true;

                            continue;
                        }
                    }
                }

                if (resolved) {
                    return unresolvedSet;
                }

                if (unresolvedSet == null) {
                    unresolvedSet = new ArrayListIdentity();
                }

                unresolvedSet.add(this);
            }
        }

        return unresolvedSet;
    }

    public boolean resolveColumnReference(RangeVariable rangeVar) {

        if (tableName == null) {
            Expression e = rangeVar.getColumnExpression(columnName);

            if (e != null) {
                opType   = e.opType;
                nodes    = e.nodes;
                dataType = e.dataType;

                return true;
            }

            if (rangeVar.variables != null) {
                int colIndex = rangeVar.findColumn(columnName);

                if (colIndex == -1) {
                    return false;
                }

                ColumnSchema column = rangeVar.getColumn(colIndex);

                if (column.getParameterMode()
                        == SchemaObject.ParameterModes.PARAM_OUT) {
                    return false;
                } else {
                    opType = rangeVar.isVariable ? OpTypes.VARIABLE
                                                 : OpTypes.PARAMETER;

                    setAttributesAsColumn(rangeVar, colIndex);

                    return true;
                }
            }
        }

        if (!rangeVar.resolvesTableName(this)) {
            return false;
        }

        int colIndex = rangeVar.findColumn(columnName);

        if (colIndex != -1) {
            setAttributesAsColumn(rangeVar, colIndex);

            return true;
        }

        return false;
    }

    boolean resolvesDuplicateColumnReference(RangeVariable rangeVar) {

        if (tableName == null) {
            Expression e = rangeVar.getColumnExpression(columnName);

            if (e != null) {
                return false;
            }

            if (rangeVar.variables != null) {
                int colIndex = rangeVar.findColumn(columnName);

                if (colIndex == -1) {
                    return false;
                }

                ColumnSchema column = rangeVar.getColumn(colIndex);

                if (column.getParameterMode()
                        == SchemaObject.ParameterModes.PARAM_OUT) {
                    return false;
                } else {
                    return true;
                }
            }
        }

        if (!rangeVar.resolvesTableName(this)) {
            return false;
        }

        int colIndex = rangeVar.findColumn(columnName);

        if (colIndex != -1) {
            return true;
        }

        return false;
    }

    public void resolveTypes(Session session, Expression parent) {

        switch (opType) {

            case OpTypes.DEFAULT :
                if (parent != null && parent.opType != OpTypes.ROW) {
                    throw Error.error(ErrorCode.X_42544);
                }
                break;

            case OpTypes.COALESCE : {
                Type type = null;

                for (int i = 0; i < nodes.length; i++) {
                    type = Type.getAggregateType(nodes[i].dataType, type);
                }

                dataType = type;

                break;
            }
        }
    }

    public Object getValue(Session session) {

        switch (opType) {

            case OpTypes.DEFAULT :
                return null;

            case OpTypes.VARIABLE : {
                return session.sessionContext.routineVariables[columnIndex];
            }
            case OpTypes.PARAMETER : {
                return session.sessionContext.routineArguments[columnIndex];
            }
            case OpTypes.COLUMN : {
                Object[] data =
                    (Object[]) session.sessionContext
                        .rangeIterators[rangeVariable.rangePosition]
                        .getCurrent();
                Object value = data[columnIndex];

                if (dataType != column.dataType) {
                    value = dataType.convertToType(session, value,
                                                   column.dataType);
                }

                return value;
            }
            case OpTypes.SIMPLE_COLUMN : {
                Object[] data =
                    (Object[]) session.sessionContext
                        .rangeIterators[rangePosition].getCurrent();

                return data[columnIndex];
            }
            case OpTypes.COALESCE : {
                Object value = null;

                for (int i = 0; i < nodes.length; i++) {
                    value = nodes[i].getValue(session, dataType);

                    if (value != null) {
                        return value;
                    }
                }

                return value;
            }
            case OpTypes.DYNAMIC_PARAM : {
                return session.sessionContext.dynamicArguments[parameterIndex];
            }
            case OpTypes.SEQUENCE : {
                return session.sessionData.getSequenceValue(sequence);
            }
            case OpTypes.ASTERISK :
            case OpTypes.MULTICOLUMN :
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionColumn");
        }
    }

    public String getSQL() {

        switch (opType) {

            case OpTypes.DEFAULT :
                return Tokens.T_DEFAULT;

            case OpTypes.DYNAMIC_PARAM :
                return Tokens.T_QUESTION;

            case OpTypes.ASTERISK :
                return "*";

            case OpTypes.COALESCE :
                return alias.getStatementName();

            case OpTypes.VARIABLE :
            case OpTypes.PARAMETER :
            case OpTypes.COLUMN : {
                if (column == null) {
                    if (alias != null) {
                        return alias.getStatementName();
                    } else {
                        return columnName;
                    }
                }

                if (rangeVariable.tableAlias == null) {
                    return column.getName().getSchemaQualifiedStatementName();
                } else {
                    StringBuffer sb = new StringBuffer();

                    sb.append(rangeVariable.tableAlias.getStatementName());
                    sb.append('.');
                    sb.append(column.getName().statementName);

                    return sb.toString();
                }
            }
            case OpTypes.MULTICOLUMN : {
                if (nodes.length == 0) {
                    return "*";
                }

                StringBuffer sb = new StringBuffer();

                for (int i = 0; i < nodes.length; i++) {
                    Expression e = nodes[i];

                    if (i > 0) {
                        sb.append(',');
                    }

                    String s = e.getSQL();

                    sb.append(s);
                }

                return sb.toString();
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionColumn");
        }
    }

    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer(64);

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        switch (opType) {

            case OpTypes.DEFAULT :
                sb.append(Tokens.T_DEFAULT);
                break;

            case OpTypes.ASTERISK :
                sb.append("OpTypes.ASTERISK ");
                break;

            case OpTypes.VARIABLE :
                sb.append("VARIABLE: ");
                sb.append(column.getName().name);
                break;

            case OpTypes.PARAMETER :
                sb.append(Tokens.T_PARAMETER).append(": ");
                sb.append(column.getName().name);
                break;

            case OpTypes.COALESCE :
                sb.append(Tokens.T_COLUMN).append(": ");
                sb.append(columnName);

                if (alias != null) {
                    sb.append(" AS ").append(alias.name);
                }
                break;

            case OpTypes.COLUMN :
                sb.append(Tokens.T_COLUMN).append(": ");
                sb.append(column.getName().getSchemaQualifiedStatementName());

                if (alias != null) {
                    sb.append(" AS ").append(alias.name);
                }
                break;

            case OpTypes.DYNAMIC_PARAM :
                sb.append("DYNAMIC PARAM: ");
                sb.append(", TYPE = ").append(dataType.getNameString());
                break;

            case OpTypes.SEQUENCE :
                sb.append(Tokens.T_SEQUENCE).append(": ");
                sb.append(sequence.getName().name);
                break;

            case OpTypes.MULTICOLUMN :

            // shouldn't get here
        }

        return sb.toString();
    }

    /**
     * Returns the table name used in query
     *
     * @return table name
     */
    String getTableName() {

        if (opType == OpTypes.MULTICOLUMN) {
            return tableName;
        }

        if (opType == OpTypes.COLUMN) {
            if (rangeVariable == null) {
                return tableName;
            } else {
                return rangeVariable.getTable().getName().name;
            }
        }

        return "";
    }

    static void checkColumnsResolved(HsqlList set) {

        if (set != null && !set.isEmpty()) {
            StringBuffer sb = new StringBuffer();
            Expression   e  = (Expression) set.get(0);

            if (e instanceof ExpressionColumn) {
                ExpressionColumn c = (ExpressionColumn) e;

                if (c.schema != null) {
                    sb.append(c.schema + '.');
                }

                if (c.tableName != null) {
                    sb.append(c.tableName + '.');
                }

                throw Error.error(ErrorCode.X_42501,
                                  sb.toString() + c.getColumnName());
            } else {
                throw Error.error(ErrorCode.X_42501);
            }
        }
    }

    public OrderedHashSet getUnkeyedColumns(OrderedHashSet unresolvedSet) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }

            unresolvedSet = nodes[i].getUnkeyedColumns(unresolvedSet);
        }

        if (opType == OpTypes.COLUMN
                && !rangeVariable.hasKeyedColumnInGroupBy) {
            if (unresolvedSet == null) {
                unresolvedSet = new OrderedHashSet();
            }

            unresolvedSet.add(this);
        }

        return unresolvedSet;
    }

    /**
     * collects all range variables in expression tree
     */
    void collectRangeVariables(RangeVariable[] rangeVariables, Set set) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].collectRangeVariables(rangeVariables, set);
            }
        }

        if (rangeVariable != null) {
            for (int i = 0; i < rangeVariables.length; i++) {
                if (rangeVariables[i] == rangeVariable) {
                    set.add(rangeVariable);

                    break;
                }
            }
        }
    }

    Expression replaceAliasInOrderBy(Expression[] columns, int length) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }

            nodes[i] = nodes[i].replaceAliasInOrderBy(columns, length);
        }

        switch (opType) {

            case OpTypes.COALESCE :
            case OpTypes.COLUMN : {
                for (int i = 0; i < length; i++) {
                    SimpleName aliasName = columns[i].alias;
                    String     alias     = aliasName == null ? null
                                                             : aliasName.name;

                    if (schema == null && tableName == null
                            && columnName.equals(alias)) {
                        return columns[i];
                    }
                }

                for (int i = 0; i < length; i++) {
                    if (columns[i] instanceof ExpressionColumn) {
                        if (this.equals(columns[i])) {
                            return columns[i];
                        }

                        if (tableName == null && schema == null
                                && columnName
                                    .equals(((ExpressionColumn) columns[i])
                                        .columnName)) {
                            return columns[i];
                        }
                    }
                }
            }
            default :
        }

        return this;
    }

    Expression replaceColumnReferences(RangeVariable range,
                                       Expression[] list) {

        if (opType == OpTypes.COLUMN && rangeVariable == range) {
            return list[columnIndex];
        }

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }

            nodes[i] = nodes[i].replaceColumnReferences(range, list);
        }

        return this;
    }

    int findMatchingRangeVariableIndex(RangeVariable[] rangeVarArray) {

        for (int i = 0; i < rangeVarArray.length; i++) {
            RangeVariable rangeVar = rangeVarArray[i];

            if (rangeVar.resolvesTableName(this)) {
                return i;
            }
        }

        return -1;
    }

    /**
     * return true if given RangeVariable is used in expression tree
     */
    boolean hasReference(RangeVariable range) {

        if (range == rangeVariable) {
            return true;
        }

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                if (nodes[i].hasReference(range)) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * SIMPLE_COLUMN expressions can be of different Java types
     */
    public boolean equals(Expression other) {

        if (other == this) {
            return true;
        }

        if (other == null) {
            return false;
        }

        if (opType != ((Expression) other).opType) {
            return false;
        }

        switch (opType) {

            case OpTypes.SIMPLE_COLUMN :
                return this.columnIndex == ((Expression) other).columnIndex;

            case OpTypes.COALESCE :
                return nodes == ((Expression) other).nodes;

            case OpTypes.COLUMN :
                return column == ((Expression) other).getColumn();

            default :
                return false;
        }
    }

    void replaceRangeVariables(RangeVariable[] ranges,
                               RangeVariable[] newRanges) {

        for (int i = 0; i < nodes.length; i++) {
            nodes[i].replaceRangeVariables(ranges, newRanges);
        }

        for (int i = 0; i < ranges.length; i++) {
            if (rangeVariable == ranges[i]) {
                rangeVariable = newRanges[i];

                break;
            }
        }
    }

    void resetColumnReferences() {
        rangeVariable = null;
        columnIndex   = -1;
    }

    public boolean isIndexable(RangeVariable range) {

        if (opType == OpTypes.COLUMN) {
            return rangeVariable == range;
        }

        return false;
    }

    public boolean isUnresolvedParam() {
        return isParam && dataType == null;
    }

    boolean isDynamicParam() {
        return isParam;
    }
}
