/* Copyright (c) 2001-2024, The HSQL Development Group
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
import org.hsqldb.RangeGroup.RangeGroupSimple;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.List;
import org.hsqldb.lib.LongDeque;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.map.ValuePool;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;

/**
 * Parser for DML statements
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.4
 * @since 1.9.0
 */
public class ParserDML extends ParserDQL {

    ParserDML(Session session, Scanner scanner) {
        super(session, scanner, null);
    }

    /**
     * Retrieves an INSERT Statement from this parse context.
     */
    StatementDMQL compileInsertStatement(RangeGroup[] rangeGroups) {

        boolean[]       insertColumnCheckList;
        boolean[]       updateColumnCheckList = null;
        int[]           insertColumnMap;
        int[]           updateColumnMap = ValuePool.emptyIntArray;
        int             colCount;
        Table           table;
        RangeVariable   targetRange;
        RangeVariable[] rangeVariables;
        boolean         overridingUser               = false;
        boolean         overridingSystem             = false;
        boolean         assignsToIdentityOrGenerated = false;
        Token           tableToken;
        boolean         hasColumnList = false;
        int             specialAction = StatementInsert.isNone;
        Expression      insertExpressions;
        Expression[]    updateExpressions = Expression.emptyArray;
        Expression[]    targets           = null;

        if (database.sqlSyntaxMys) {
            if (readIfThis(Tokens.REPLACE)) {
                specialAction = StatementInsert.isReplace;
            }

            if (specialAction == StatementInsert.isNone) {
                readThis(Tokens.INSERT);

                if (readIfThis(Tokens.IGNORE)) {
                    specialAction = StatementInsert.isIgnore;
                }
            }

            readIfThis(Tokens.INTO);
        } else {
            readThis(Tokens.INSERT);
            readThis(Tokens.INTO);
        }

        tableToken  = getRecordedToken();
        targetRange = readRangeVariableForDataChange(StatementTypes.INSERT);

        targetRange.resolveRangeTableTypes(session, RangeVariable.emptyArray);

        rangeVariables        = new RangeVariable[]{ targetRange };
        table                 = targetRange.getTable();
        insertColumnCheckList = null;
        insertColumnMap       = table.getColumnMap();
        colCount              = table.getColumnCount();

        int   position  = getPosition();
        Table baseTable = table.isTriggerInsertable()
                          ? table
                          : table.getBaseTable();

        switch (token.tokenType) {

            case Tokens.DEFAULT : {
                read();
                readThis(Tokens.VALUES);

                Expression row = new Expression(
                    OpTypes.ROW,
                    Expression.emptyArray);

                insertExpressions = new Expression(
                    OpTypes.VALUELIST,
                    new Expression[]{ row });
                insertColumnCheckList = table.getNewColumnCheckList();

                StatementDMQL cs = new StatementInsert(
                    session,
                    table,
                    rangeVariables,
                    insertColumnMap,
                    insertExpressions,
                    insertColumnCheckList,
                    updateExpressions,
                    updateColumnCheckList,
                    updateColumnMap,
                    null,
                    specialAction,
                    compileContext);

                return cs;
            }

            case Tokens.OPENBRACKET : {
                int brackets = readOpenBrackets();

                if (brackets == 1) {
                    boolean isQuery = false;

                    switch (token.tokenType) {

                        case Tokens.WITH :
                        case Tokens.SELECT :
                        case Tokens.TABLE : {
                            rewind(position);

                            isQuery = true;
                            break;
                        }

                        default :
                    }

                    if (isQuery) {
                        break;
                    }

                    OrderedHashSet<String> columnNames = new OrderedHashSet<>();
                    boolean                withPrefix  = database.sqlSyntaxOra;

                    readSimpleColumnNames(columnNames, targetRange, withPrefix);
                    readThis(Tokens.CLOSEBRACKET);

                    colCount        = columnNames.size();
                    insertColumnMap = table.getColumnIndexes(columnNames);
                    hasColumnList   = true;
                } else {
                    rewind(position);
                }

                break;
            }

            default :
        }

        if (token.tokenType == Tokens.OVERRIDING) {
            read();

            if (token.tokenType == Tokens.USER) {
                read();

                overridingUser = true;
            } else if (token.tokenType == Tokens.SYSTEM) {
                read();

                overridingSystem = true;
            } else {
                throw unexpectedToken();
            }

            readThis(Tokens.VALUE);
        }

        switch (token.tokenType) {

            case Tokens.VALUE : {
                if (!database.sqlSyntaxMys) {
                    throw unexpectedToken();
                }
            }

            // fall through
            case Tokens.VALUES : {
                read();

                insertColumnCheckList = table.getColumnCheckList(
                    insertColumnMap);
                insertExpressions = XreadContextuallyTypedTable(colCount);

                List<Expression> unresolved =
                    insertExpressions.resolveColumnReferences(
                        session,
                        RangeGroup.emptyGroup,
                        rangeGroups,
                        null);

                ExpressionColumn.checkColumnsResolved(unresolved);
                insertExpressions.resolveTypes(session, null);
                setParameterTypes(insertExpressions, table, insertColumnMap);

                if (table != baseTable) {
                    int[] baseColumnMap = table.getBaseTableColumnMap();
                    int[] newColumnMap  = new int[insertColumnMap.length];

                    ArrayUtil.projectRow(
                        baseColumnMap,
                        insertColumnMap,
                        newColumnMap);

                    insertColumnMap = newColumnMap;
                }

                Expression[] rowList = insertExpressions.nodes;

                for (int j = 0; j < rowList.length; j++) {
                    Expression[] rowArgs = rowList[j].nodes;

                    for (int i = 0; i < rowArgs.length; i++) {
                        Expression e       = rowArgs[i];
                        ColumnSchema column = baseTable.getColumn(
                            insertColumnMap[i]);
                        Type       colType = column.getDataType();

                        if (column.isIdentity()) {
                            assignsToIdentityOrGenerated = true;

                            if (e.getType() != OpTypes.DEFAULT) {
                                if (overridingUser) {
                                    rowArgs[i] = new ExpressionColumn(
                                        OpTypes.DEFAULT);
                                } else if (overridingSystem) {

                                    // user value allowed
                                } else {
                                    if (baseTable.identitySequence.isAlways()) {
                                        throw Error.error(ErrorCode.X_42542);
                                    }
                                }
                            }
                        } else if (column.hasDefault()) {

                            //
                        } else if (column.isGenerated()
                                   || column.isSystemPeriod()) {
                            assignsToIdentityOrGenerated = true;

                            if (e.getType() != OpTypes.DEFAULT) {
                                if (overridingUser) {
                                    rowArgs[i] = new ExpressionColumn(
                                        OpTypes.DEFAULT);
                                } else {
                                    throw Error.error(ErrorCode.X_42543);
                                }
                            }
                        } else {

                            // no explicit default
                        }

                        if (e.isUnresolvedParam()) {
                            e.setAttributesAsColumn(column);
                        }

                        // DYNAMIC_PARAM and PARAMETER expressions may have wider values
                        if (e.opType != OpTypes.DEFAULT) {
                            if (e.dataType == null
                                    || colType.isArrayType()
                                    || colType.typeDataGroup
                                       != e.dataType.typeDataGroup) {
                                rowArgs[i] = ExpressionOp.getConvertExpression(
                                    session,
                                    e,
                                    colType);
                            }
                        }
                    }
                }

                if (!assignsToIdentityOrGenerated
                        && (overridingUser || overridingSystem)) {
                    throw unexpectedToken(Tokens.T_OVERRIDING);
                }

                if (!hasColumnList) {
                    tableToken.setWithColumnList();
                }

                if (database.sqlSyntaxMys
                        && specialAction == StatementInsert.isNone
                        && readIfThis(Tokens.ON)) {
                    readThis(Tokens.DUPLICATE);
                    readThis(Tokens.KEY);
                    readThis(Tokens.UPDATE);

                    OrderedHashSet<Expression> targetSet =
                        new OrderedHashSet<>();
                    LongDeque                 colIndexList = new LongDeque();
                    HsqlArrayList<Expression> exprList = new HsqlArrayList<>();
                    RangeGroup rangeGroup = new RangeGroupSimple(
                        rangeVariables,
                        false);
                    RangeVariable valueRange = new RangeVariable(
                        targetRange.getTable(),
                        2);

                    specialAction = StatementInsert.isUpdate;

                    readOnDuplicateClauseList(
                        rangeVariables,
                        targetSet,
                        colIndexList,
                        exprList);

                    updateColumnMap = new int[colIndexList.size()];

                    colIndexList.toArray(updateColumnMap);

                    targets = new Expression[targetSet.size()];

                    targetSet.toArray(targets);

                    for (int i = 0; i < targets.length; i++) {
                        resolveReferencesAndTypes(
                            rangeGroup,
                            rangeGroups,
                            targets[i]);
                    }

                    updateColumnCheckList = table.getColumnCheckList(
                        updateColumnMap);
                    updateExpressions = new Expression[exprList.size()];

                    exprList.toArray(updateExpressions);
                    resolveUpdateExpressions(
                        table,
                        rangeGroup,
                        updateColumnMap,
                        targets,
                        updateExpressions,
                        rangeGroups,
                        valueRange);
                }

                StatementDMQL cs = new StatementInsert(
                    session,
                    table,
                    rangeVariables,
                    insertColumnMap,
                    insertExpressions,
                    insertColumnCheckList,
                    updateExpressions,
                    updateColumnCheckList,
                    updateColumnMap,
                    targets,
                    specialAction,
                    compileContext);

                return cs;
            }

            case Tokens.OPENBRACKET :
            case Tokens.WITH :
            case Tokens.SELECT :
            case Tokens.TABLE : {
                break;
            }

            default : {
                throw unexpectedToken();
            }
        }

        insertColumnCheckList = table.getColumnCheckList(insertColumnMap);

        if (table != baseTable) {
            int[] baseColumnMap = table.getBaseTableColumnMap();
            int[] newColumnMap  = new int[insertColumnMap.length];

            ArrayUtil.projectRow(baseColumnMap, insertColumnMap, newColumnMap);

            insertColumnMap = newColumnMap;
        }

        int enforcedDefaultIndex = baseTable.getIdentityColumnIndex();
        int overrideIndex        = -1;

        if (enforcedDefaultIndex != -1
                && ArrayUtil.find(insertColumnMap, enforcedDefaultIndex) > -1) {
            if (baseTable.identitySequence.isAlways()) {
                if (!overridingUser && !overridingSystem) {
                    throw Error.error(ErrorCode.X_42543);
                }
            }

            if (overridingUser) {
                overrideIndex = enforcedDefaultIndex;
            }
        } else if (overridingUser || overridingSystem) {
            throw unexpectedToken(Tokens.T_OVERRIDING);
        }

        Type[] types = new Type[insertColumnMap.length];

        ArrayUtil.projectRow(
            baseTable.getColumnTypes(),
            insertColumnMap,
            types);
        compileContext.setOuterRanges(rangeGroups);

        QueryExpression queryExpression = XreadQueryExpression();

        queryExpression.setReturningResult();
        queryExpression.resolve(session, rangeGroups, types);

        if (colCount != queryExpression.getColumnCount()) {
            throw Error.error(ErrorCode.X_42546);
        }

        if (!hasColumnList) {
            tableToken.setWithColumnList();
        }

        if (database.sqlSyntaxMys
                && specialAction == StatementInsert.isNone
                && readIfThis(Tokens.ON)) {
            readThis(Tokens.DUPLICATE);
            readThis(Tokens.KEY);
            readThis(Tokens.UPDATE);

            OrderedHashSet<Expression> targetSet    = new OrderedHashSet<>();
            LongDeque                  colIndexList = new LongDeque();
            HsqlArrayList<Expression>  exprList     = new HsqlArrayList<>();
            RangeGroup rangeGroup = new RangeGroupSimple(rangeVariables, false);
            RangeVariable valueRange = new RangeVariable(
                targetRange.getTable(),
                2);

            specialAction = StatementInsert.isUpdate;

            readOnDuplicateClauseList(
                rangeVariables,
                targetSet,
                colIndexList,
                exprList);

            updateColumnMap = new int[colIndexList.size()];

            colIndexList.toArray(updateColumnMap);

            targets = new Expression[targetSet.size()];

            targetSet.toArray(targets);

            for (int i = 0; i < targets.length; i++) {
                resolveReferencesAndTypes(rangeGroup, rangeGroups, targets[i]);
            }

            updateColumnCheckList = table.getColumnCheckList(updateColumnMap);
            updateExpressions     = new Expression[exprList.size()];

            exprList.toArray(updateExpressions);
            resolveUpdateExpressions(
                table,
                rangeGroup,
                updateColumnMap,
                targets,
                updateExpressions,
                rangeGroups,
                valueRange);
        }

        StatementDMQL cs = new StatementInsert(
            session,
            table,
            rangeVariables,
            insertColumnMap,
            insertColumnCheckList,
            queryExpression,
            updateExpressions,
            updateColumnCheckList,
            updateColumnMap,
            targets,
            specialAction,
            overrideIndex,
            compileContext);

        return cs;
    }

    private static void setParameterTypes(
            Expression tableExpression,
            Table table,
            int[] columnMap) {

        for (int i = 0; i < tableExpression.nodes.length; i++) {
            Expression[] list = tableExpression.nodes[i].nodes;

            for (int j = 0; j < list.length; j++) {
                if (list[j].isUnresolvedParam()) {
                    list[j].setAttributesAsColumn(
                        table.getColumn(columnMap[j]));
                }
            }
        }
    }

    Statement compileTruncateStatement() {

        boolean         isTable         = false;
        boolean         withCommit      = false;
        boolean         noCheck         = false;
        boolean         restartIdentity = false;
        HsqlName        objectName      = null;
        RangeVariable[] rangeVariables  = null;
        Table           table           = null;
        HsqlName[]      writeTableNames = null;
        RangeVariable   targetRange     = null;
        TimestampData   timestamp       = null;

        readThis(Tokens.TRUNCATE);

        if (token.tokenType == Tokens.TABLE) {
            readThis(Tokens.TABLE);

            targetRange = readRangeVariableForDataChange(
                StatementTypes.TRUNCATE);
            rangeVariables = new RangeVariable[]{ targetRange };
            table          = rangeVariables[0].getTable();
            objectName     = table.getName();
            isTable        = true;
        } else {
            readThis(Tokens.SCHEMA);

            objectName = readSchemaName();
        }

        switch (token.tokenType) {

            case Tokens.CONTINUE : {
                read();
                readThis(Tokens.IDENTITY);
                break;
            }

            case Tokens.RESTART : {
                read();
                readThis(Tokens.IDENTITY);

                restartIdentity = true;
                break;
            }

            case Tokens.VERSIONING : {
                if (!isTable) {
                    throw unexpectedToken();
                }

                if (!table.isSystemVersioned()) {
                    throw unexpectedToken();
                }

                read();
                readThis(Tokens.TO);

                if (readIfThis(Tokens.TIMESTAMP)) {
                    String s = readQuotedString();

                    timestamp =
                        (TimestampData) Type.SQL_TIMESTAMP_WITH_TIME_ZONE.convertToType(
                            session,
                            s,
                            Type.SQL_VARCHAR_DEFAULT);
                } else {
                    readThis(Tokens.CURRENT_TIMESTAMP);

                    timestamp = session.getTransactionUTC();
                }

                break;
            }

            default :
        }

        if (!isTable) {
            checkIsThis(Tokens.AND);
        }

        if (readIfThis(Tokens.AND)) {
            readThis(Tokens.COMMIT);

            withCommit = true;

            if (readIfThis(Tokens.NO)) {
                readThis(Tokens.CHECK);

                noCheck = true;
            }
        }

        if (isTable) {
            writeTableNames = new HsqlName[]{ table.getName() };
        } else {
            writeTableNames =
                session.database.schemaManager.getCatalogAndBaseTableNames();
        }

        if (withCommit || timestamp != null) {
            Object[] args = new Object[]{ objectName,
                                          Boolean.valueOf(
                                              restartIdentity), Boolean.valueOf(
                                                      noCheck), timestamp };

            return new StatementCommand(
                StatementTypes.TRUNCATE,
                args,
                null,
                writeTableNames);
        }

        Statement cs = new StatementDML(
            session,
            table,
            targetRange,
            rangeVariables,
            compileContext,
            restartIdentity,
            StatementTypes.TRUNCATE,
            null);

        return cs;
    }

    /**
     * Creates a DELETE-type Statement from this parse context.
     */
    Statement compileDeleteStatement(RangeGroup[] rangeGroups) {

        Expression      condition       = null;
        boolean         restartIdentity = false;
        RangeVariable   targetRange;
        RangeVariable[] rangeVariables;
        RangeGroup      rangeGroup;
        Table           table;

        readThis(Tokens.DELETE);

        if (database.sqlSyntaxOra) {
            readIfThis(Tokens.FROM);
        } else {
            readThis(Tokens.FROM);
        }

        targetRange = readRangeVariableForDataChange(
            StatementTypes.DELETE_WHERE);
        rangeVariables = new RangeVariable[]{ targetRange };
        rangeGroup     = new RangeGroupSimple(rangeVariables, false);
        table          = rangeVariables[0].getTable();

        compileContext.setOuterRanges(rangeGroups);

        if (token.tokenType == Tokens.WHERE) {
            read();

            condition = XreadAndResolveBooleanValueExpression(
                rangeGroups,
                rangeGroup);
        }

        SortAndSlice sortAndSlice = null;

        if (token.tokenType == Tokens.LIMIT) {
            sortAndSlice = XreadOrderByExpression();
        }

        Table baseTable = table.isTriggerDeletable()
                          ? table
                          : table.getBaseTable();

        if (table != baseTable) {
            QuerySpecification baseSelect = table.getQueryExpression()
                    .getMainSelect();

            if (condition != null) {
                condition = condition.replaceColumnReferences(
                    session,
                    rangeVariables[0],
                    baseSelect.exprColumns);
            }

            condition = ExpressionLogical.andExpressions(
                baseSelect.queryCondition,
                condition);
            rangeVariables = baseSelect.rangeVariables;

            ArrayUtil.fillArray(rangeVariables[0].usedColumns, true);
        }

        if (condition != null) {
            rangeVariables[0].addJoinCondition(condition);

            RangeVariableResolver resolver = new RangeVariableResolver(
                session,
                rangeVariables,
                null,
                compileContext,
                false);

            resolver.processConditions();

            rangeVariables = resolver.rangeVariables;
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            rangeVariables[i].resolveRangeTableTypes(
                session,
                RangeVariable.emptyArray);
        }

        Statement cs = new StatementDML(
            session,
            table,
            targetRange,
            rangeVariables,
            compileContext,
            restartIdentity,
            StatementTypes.DELETE_WHERE,
            sortAndSlice);

        return cs;
    }

    /**
     * Creates an UPDATE-type Statement from this parse context.
     */
    StatementDMQL compileUpdateStatement(RangeGroup[] rangeGroups) {

        read();

        Expression[]               updateExpressions;
        int[]                      columnMap;
        boolean[]                  columnCheckList;
        OrderedHashSet<Expression> targetSet    = new OrderedHashSet<>();
        LongDeque                  colIndexList = new LongDeque();
        HsqlArrayList<Expression>  exprList     = new HsqlArrayList<>();
        RangeVariable              targetRange;
        RangeVariable[]            rangeVariables;
        RangeGroup                 rangeGroup;
        Table                      table;
        Table                      baseTable;

        targetRange = readRangeVariableForDataChange(
            StatementTypes.UPDATE_WHERE);
        rangeVariables = new RangeVariable[]{ targetRange };
        rangeGroup     = new RangeGroupSimple(rangeVariables, false);
        table          = rangeVariables[0].rangeTable;
        baseTable      = table.isTriggerUpdatable()
                         ? table
                         : table.getBaseTable();

        readThis(Tokens.SET);
        readSetClauseList(
            rangeGroups,
            rangeVariables,
            targetSet,
            colIndexList,
            exprList);

        columnMap = new int[colIndexList.size()];

        colIndexList.toArray(columnMap);

        Expression[] targets = new Expression[targetSet.size()];

        targetSet.toArray(targets);

        for (int i = 0; i < targets.length; i++) {
            resolveReferencesAndTypes(rangeGroup, rangeGroups, targets[i]);
        }

        columnCheckList   = table.getColumnCheckList(columnMap);
        updateExpressions = new Expression[exprList.size()];

        exprList.toArray(updateExpressions);

        Expression condition = null;

        if (token.tokenType == Tokens.WHERE) {
            read();

            condition = XreadAndResolveBooleanValueExpression(
                rangeGroups,
                rangeGroup);
        }

        SortAndSlice sortAndSlice = null;

        if (token.tokenType == Tokens.LIMIT) {
            sortAndSlice = XreadOrderByExpression();
        }

        resolveUpdateExpressions(
            table,
            rangeGroup,
            columnMap,
            targets,
            updateExpressions,
            rangeGroups,
            null);

        if (table != baseTable) {
            QuerySpecification baseSelect = table.getQueryExpression()
                    .getMainSelect();

            if (condition != null) {
                condition = condition.replaceColumnReferences(
                    session,
                    rangeVariables[0],
                    baseSelect.exprColumns);
            }

            for (int i = 0; i < updateExpressions.length; i++) {
                updateExpressions[i] =
                    updateExpressions[i].replaceColumnReferences(
                        session,
                        rangeVariables[0],
                        baseSelect.exprColumns);
            }

            condition = ExpressionLogical.andExpressions(
                baseSelect.queryCondition,
                condition);
            rangeVariables = baseSelect.rangeVariables;

            ArrayUtil.fillArray(rangeVariables[0].usedColumns, true);
        }

        if (condition != null) {
            rangeVariables[0].addJoinCondition(condition);

            RangeVariableResolver resolver = new RangeVariableResolver(
                session,
                rangeVariables,
                null,
                compileContext,
                false);

            resolver.processConditions();

            rangeVariables = resolver.rangeVariables;
        }

        for (int i = 0; i < rangeVariables.length; i++) {
            rangeVariables[i].resolveRangeTableTypes(
                session,
                RangeVariable.emptyArray);
        }

        if (table != baseTable) {
            int[] baseColumnMap = table.getBaseTableColumnMap();
            int[] newColumnMap  = new int[columnMap.length];

            ArrayUtil.projectRow(baseColumnMap, columnMap, newColumnMap);

            columnMap = newColumnMap;

            for (int i = 0; i < columnMap.length; i++) {
                if (baseTable.colGenerated[columnMap[i]]) {
                    throw Error.error(ErrorCode.X_42513);
                }
            }
        }

        StatementDMQL cs = new StatementDML(
            session,
            targets,
            table,
            targetRange,
            rangeVariables,
            columnMap,
            updateExpressions,
            columnCheckList,
            compileContext,
            sortAndSlice);

        return cs;
    }

    Expression XreadAndResolveBooleanValueExpression(
            RangeGroup[] rangeGroups,
            RangeGroup rangeGroup) {

        Expression condition = XreadBooleanValueExpression();
        List<Expression> unresolved = condition.resolveColumnReferences(
            session,
            rangeGroup,
            rangeGroups,
            null);

        ExpressionColumn.checkColumnsResolved(unresolved);
        condition.resolveTypes(session, null);

        if (condition.isUnresolvedParam()) {
            condition.dataType = Type.SQL_BOOLEAN;
        }

        if (condition.getDataType() != Type.SQL_BOOLEAN) {
            throw Error.error(ErrorCode.X_42568);
        }

        return condition;
    }

    void resolveUpdateExpressions(
            Table targetTable,
            RangeGroup rangeGroup,
            int[] columnMap,
            Expression[] targets,
            Expression[] colExpressions,
            RangeGroup[] rangeGroups,
            RangeVariable valuesRange) {

        int enforcedDefaultIndex = -1;

        if (targetTable.hasIdentityColumn()
                && targetTable.identitySequence.isAlways()) {
            enforcedDefaultIndex = targetTable.getIdentityColumnIndex();
        }

        for (int i = 0, ix = 0; i < columnMap.length; ix++) {
            Expression expr = colExpressions[ix];
            Expression e;

            // no generated column can be updated
            if (targetTable.colGenerated[columnMap[i]]) {
                throw Error.error(ErrorCode.X_42513);
            }

            if (expr.getType() == OpTypes.ROW) {
                Expression[] elements = expr.nodes;

                for (int j = 0; j < elements.length; j++, i++) {
                    e = elements[j];

                    if (enforcedDefaultIndex == columnMap[i]) {
                        if (e.getType() != OpTypes.DEFAULT) {
                            throw Error.error(ErrorCode.X_42541);
                        }
                    }

                    if (e.isUnresolvedParam()) {
                        e.setAttributesAsColumn(
                            targetTable.getColumn(columnMap[i]));
                    } else if (e.getType() == OpTypes.DEFAULT) {

                        //
                    } else {
                        List<Expression> unresolved =
                            expr.resolveColumnReferences(
                                session,
                                rangeGroup,
                                rangeGroups,
                                null);

                        ExpressionColumn.checkColumnsResolved(unresolved);
                        e.resolveTypes(session, null);
                    }
                }
            } else if (expr.getType() == OpTypes.ROW_SUBQUERY) {
                List<Expression> unresolved = expr.resolveColumnReferences(
                    session,
                    rangeGroup,
                    rangeGroups,
                    null);

                ExpressionColumn.checkColumnsResolved(unresolved);
                expr.resolveTypes(session, null);

                int count = expr.table.queryExpression.getColumnCount();

                for (int j = 0; j < count; j++, i++) {
                    if (enforcedDefaultIndex == columnMap[i]) {
                        throw Error.error(ErrorCode.X_42541);
                    }
                }
            } else {
                e = expr;

                if (enforcedDefaultIndex == columnMap[i]) {
                    if (e.getType() != OpTypes.DEFAULT) {
                        throw Error.error(ErrorCode.X_42541);
                    }
                }

                if (e.isUnresolvedParam()) {
                    if (targets.length > i
                            && targets[i].opType == OpTypes.ARRAY_ACCESS) {
                        Type type = targetTable.getColumn(columnMap[i])
                                               .getDataType()
                                               .collectionBaseType();

                        e.setDataType(session, type);
                    } else {
                        e.setAttributesAsColumn(
                            targetTable.getColumn(columnMap[i]));
                    }
                } else if (e.getType() == OpTypes.DEFAULT) {

                    //
                } else {
                    List<Expression> unresolved = expr.resolveColumnReferences(
                        session,
                        rangeGroup,
                        rangeGroups,
                        null);

                    if (valuesRange != null && unresolved != null) {
                        for (int j = unresolved.size() - 1; j >= 0; j--) {
                            ExpressionColumn col =
                                (ExpressionColumn) unresolved.get(
                                    j);

                            col.resolveColumnReference(valuesRange, false);

                            if (col.rangeVariable != null) {
                                unresolved.remove(j);
                            }
                        }
                    }

                    ExpressionColumn.checkColumnsResolved(unresolved);
                    e.resolveTypes(session, null);
                }

                i++;
            }
        }
    }

    void readSetClauseList(
            RangeGroup[] rangeGroups,
            RangeVariable[] rangeVars,
            OrderedHashSet<Expression> targets,
            LongDeque colIndexList,
            HsqlArrayList<Expression> expressions) {

        while (true) {
            int degree;

            if (token.tokenType == Tokens.OPENBRACKET) {
                read();

                int oldCount = targets.size();

                readTargetSpecificationList(targets, rangeVars, colIndexList);

                degree = targets.size() - oldCount;

                readThis(Tokens.CLOSEBRACKET);
            } else {
                Expression target = XreadTargetSpecification(
                    rangeVars,
                    colIndexList);

                if (!targets.add(target)) {
                    ColumnSchema col = target.getColumn();

                    throw Error.error(ErrorCode.X_42579, col.getName().name);
                }

                degree = 1;
            }

            readThis(Tokens.EQUALS_OP);

            int position = getPosition();
            int brackets = readOpenBrackets();

            if (token.tokenType == Tokens.SELECT) {
                rewind(position);
                compileContext.setOuterRanges(rangeGroups);

                TableDerived td = XreadSubqueryTableBody(OpTypes.ROW_SUBQUERY);
                QueryExpression qe = td.getQueryExpression();

                qe.setReturningResult();

                if (degree != qe.getColumnCount()) {
                    throw Error.error(ErrorCode.X_42546);
                }

                Expression e = new Expression(OpTypes.ROW_SUBQUERY, td);

                expressions.add(e);

                if (token.tokenType == Tokens.COMMA) {
                    read();
                    continue;
                }

                break;
            }

            if (brackets > 0) {
                rewind(position);
            }

            if (degree > 1) {
                readThis(Tokens.OPENBRACKET);

                Expression e = readRow();

                readThis(Tokens.CLOSEBRACKET);

                int rowDegree = e.getType() == OpTypes.ROW
                                ? e.nodes.length
                                : 1;

                if (degree != rowDegree) {
                    throw Error.error(ErrorCode.X_42546);
                }

                expressions.add(e);
            } else {
                Expression e = XreadValueExpressionWithContext();

                expressions.add(e);
            }

            if (token.tokenType == Tokens.COMMA) {
                read();
                continue;
            }

            break;
        }
    }

    void readOnDuplicateClauseList(
            RangeVariable[] rangeVars,
            OrderedHashSet<Expression> targets,
            LongDeque colIndexList,
            HsqlArrayList<Expression> expressions) {

        while (true) {
            Expression target = XreadTargetSpecification(
                rangeVars,
                colIndexList);

            if (!targets.add(target)) {
                ColumnSchema col = target.getColumn();

                throw Error.error(ErrorCode.X_42579, col.getName().name);
            }

            readThis(Tokens.EQUALS_OP);

            Expression e = XreadValueExpressionOnDuplicate();

            expressions.add(e);

            if (token.tokenType == Tokens.COMMA) {
                read();
                continue;
            }

            break;
        }
    }

    void readGetClauseList(
            RangeVariable[] rangeVars,
            OrderedHashSet<Expression> targets,
            LongDeque colIndexList,
            HsqlArrayList<Expression> expressions) {

        while (true) {
            Expression target = XreadTargetSpecification(
                rangeVars,
                colIndexList);

            if (!targets.add(target)) {
                ColumnSchema col = target.getColumn();

                throw Error.error(ErrorCode.X_42579, col.getName().name);
            }

            readThis(Tokens.EQUALS_OP);

            switch (token.tokenType) {

                case Tokens.ROW_COUNT :
                case Tokens.MORE :
                    int columnIndex = ExpressionColumn.diagnosticsList.getIndex(
                        token.tokenString);
                    Expression e = new ExpressionColumn(
                        OpTypes.DIAGNOSTICS_VARIABLE,
                        columnIndex);

                    expressions.add(e);
                    read();
                    break;

                default :
            }

            if (token.tokenType == Tokens.COMMA) {
                read();
                continue;
            }

            break;
        }
    }

    /**
     * Retrieves a MERGE Statement from this parse context.
     */
    StatementDMQL compileMergeStatement(RangeGroup[] rangeGroups) {

        boolean[]                 insertColumnCheckList;
        int[]                     insertColumnMap = null;
        int[]                     updateColumnMap = null;
        int[]                     baseUpdateColumnMap;
        Table                     table;
        RangeVariable             targetRange;
        RangeVariable             sourceRange;
        Expression                mergeCondition;
        Expression[]              targets           = null;
        HsqlArrayList<Expression> updateList        = new HsqlArrayList<>();
        Expression[]              updateExpressions = Expression.emptyArray;
        HsqlArrayList<Expression> insertList        = new HsqlArrayList<>();
        Expression                insertExpression  = null;

        read();
        readThis(Tokens.INTO);

        targetRange = readRangeVariableForDataChange(StatementTypes.MERGE);
        table       = targetRange.rangeTable;

        readThis(Tokens.USING);
        compileContext.setOuterRanges(rangeGroups);

        sourceRange = readTableOrSubquery();

        RangeVariable[] targetRanges = new RangeVariable[]{ targetRange };
        RangeGroup      rangeGroup   = new RangeGroupSimple(
            targetRanges,
            false);

        sourceRange.resolveRangeTable(session, rangeGroup, rangeGroups);
        sourceRange.resolveRangeTableTypes(session, targetRanges);
        compileContext.setOuterRanges(RangeGroup.emptyArray);

        RangeVariable[] fullRangeVars = new RangeVariable[]{ sourceRange,
                targetRange };
        RangeVariable[] sourceRangeVars = new RangeVariable[]{ sourceRange };
        RangeVariable[] targetRangeVars = new RangeVariable[]{ targetRange };
        RangeGroup fullRangeGroup = new RangeGroupSimple(fullRangeVars, false);
        RangeGroup sourceRangeGroup = new RangeGroupSimple(
            sourceRangeVars,
            false);

        // parse ON search conditions
        readThis(Tokens.ON);

        mergeCondition = XreadAndResolveBooleanValueExpression(
            rangeGroups,
            fullRangeGroup);

        // parse WHEN clause(s) and convert lists to arrays
        insertColumnMap       = table.getColumnMap();
        insertColumnCheckList = table.getNewColumnCheckList();

        OrderedHashSet<Expression> updateTargetSet    = new OrderedHashSet<>();
        OrderedHashSet<String>     insertColNames     = new OrderedHashSet<>();
        LongDeque                  updateColIndexList = new LongDeque();
        Expression[]               conditions         = new Expression[3];
        boolean                    deleteFirst        = false;
        int opOne = readMergeWhen(
            rangeGroups,
            fullRangeGroup,
            updateColIndexList,
            insertColNames,
            updateTargetSet,
            insertList,
            updateList,
            targetRangeVars,
            sourceRange,
            conditions);

        if (opOne == StatementTypes.DELETE_WHERE) {
            deleteFirst = true;
        }

        if (token.tokenType == Tokens.WHEN) {
            int opTwo = readMergeWhen(
                rangeGroups,
                fullRangeGroup,
                updateColIndexList,
                insertColNames,
                updateTargetSet,
                insertList,
                updateList,
                targetRangeVars,
                sourceRange,
                conditions);

            if (opTwo == StatementTypes.DELETE_WHERE
                    && opOne == StatementTypes.INSERT) {
                deleteFirst = true;
            }
        }

        if (token.tokenType == Tokens.WHEN) {
            readMergeWhen(
                rangeGroups,
                fullRangeGroup,
                updateColIndexList,
                insertColNames,
                updateTargetSet,
                insertList,
                updateList,
                targetRangeVars,
                sourceRange,
                conditions);
        }

        if (insertList.size() > 0) {
            int colCount = insertColNames.size();

            if (colCount != 0) {
                insertColumnMap = table.getColumnIndexes(insertColNames);
                insertColumnCheckList = table.getColumnCheckList(
                    insertColumnMap);
            }

            insertExpression = insertList.get(0);

            setParameterTypes(insertExpression, table, insertColumnMap);

            if (conditions[0] == null) {
                conditions[0] = Expression.EXPR_TRUE;
            }
        }

        if (updateList.size() > 0) {
            targets = new Expression[updateTargetSet.size()];

            updateTargetSet.toArray(targets);

            for (int i = 0; i < targets.length; i++) {
                resolveReferencesAndTypes(rangeGroup, rangeGroups, targets[i]);
            }

            updateExpressions = new Expression[updateList.size()];

            updateList.toArray(updateExpressions);

            updateColumnMap = new int[updateColIndexList.size()];

            updateColIndexList.toArray(updateColumnMap);

            if (conditions[1] == null) {
                conditions[1] = Expression.EXPR_TRUE;
            }
        }

        if (updateExpressions.length != 0) {
            Table baseTable = table.isTriggerUpdatable()
                              ? table
                              : table.getBaseTable();

            baseUpdateColumnMap = updateColumnMap;

            if (table != baseTable) {
                baseUpdateColumnMap = new int[updateColumnMap.length];

                ArrayUtil.projectRow(
                    table.getBaseTableColumnMap(),
                    updateColumnMap,
                    baseUpdateColumnMap);
            }

            resolveUpdateExpressions(
                table,
                fullRangeGroup,
                updateColumnMap,
                targets,
                updateExpressions,
                rangeGroups,
                null);
        }

        List<Expression> unresolved = mergeCondition.resolveColumnReferences(
            session,
            fullRangeGroup,
            rangeGroups,
            null);

        ExpressionColumn.checkColumnsResolved(unresolved);
        mergeCondition.resolveTypes(session, null);

        if (mergeCondition.isUnresolvedParam()) {
            mergeCondition.dataType = Type.SQL_BOOLEAN;
        }

        if (mergeCondition.getDataType() != Type.SQL_BOOLEAN) {
            throw Error.error(ErrorCode.X_42568);
        }

        fullRangeVars[1].addJoinCondition(mergeCondition);

        RangeVariableResolver resolver = new RangeVariableResolver(
            session,
            fullRangeVars,
            null,
            compileContext,
            false);

        resolver.processConditions();

        fullRangeVars = resolver.rangeVariables;

        for (int i = 0; i < fullRangeVars.length; i++) {
            fullRangeVars[i].resolveRangeTableTypes(
                session,
                RangeVariable.emptyArray);
        }

        if (insertExpression != null) {
            unresolved = insertExpression.resolveColumnReferences(
                session,
                sourceRangeGroup,
                RangeGroup.emptyArray,
                null);
            unresolved = Expression.resolveColumnSet(
                session,
                RangeVariable.emptyArray,
                rangeGroups,
                unresolved);

            ExpressionColumn.checkColumnsResolved(unresolved);
            insertExpression.resolveTypes(session, null);

            Expression[] rowList = insertExpression.nodes;

            for (int j = 0; j < rowList.length; j++) {
                Expression[] rowArgs = rowList[j].nodes;

                for (int i = 0; i < rowArgs.length; i++) {
                    Expression   e       = rowArgs[i];
                    ColumnSchema column  = table.getColumn(insertColumnMap[i]);
                    Type         colType = column.getDataType();

                    if (e.isUnresolvedParam()) {
                        e.setAttributesAsColumn(column);
                    }

                    // DYNAMIC_PARAM and PARAMETER expressions may have wider values
                    if (e.opType != OpTypes.DEFAULT) {
                        if (e.dataType == null
                                || colType.typeDataGroup
                                   != e.dataType.typeDataGroup
                                || colType.isArrayType()) {
                            rowArgs[i] = ExpressionOp.getConvertExpression(
                                session,
                                e,
                                colType);
                        }
                    }
                }
            }
        }

        StatementDMQL cs = new StatementDML(
            session,
            targets,
            sourceRange,
            targetRange,
            fullRangeVars,
            insertColumnMap,
            updateColumnMap,
            insertColumnCheckList,
            mergeCondition,
            insertExpression,
            updateExpressions,
            deleteFirst,
            conditions[0],
            conditions[1],
            conditions[2],
            compileContext);

        return cs;
    }

    /**
     * Parses a WHEN clause from a MERGE statement. This can be either a
     * WHEN MATCHED or WHEN NOT MATCHED clause, and the appropriate
     * values will be updated.
     */
    private int readMergeWhen(
            RangeGroup[] rangeGroups,
            RangeGroup rangeGroup,
            LongDeque updateColIndexList,
            OrderedHashSet<String> insertColumnNames,
            OrderedHashSet<Expression> updateTargetSet,
            HsqlArrayList<Expression> insertExpressions,
            HsqlArrayList<Expression> updateExpressions,
            RangeVariable[] targetRangeVars,
            RangeVariable sourceRangeVar,
            Expression[] conditions) {

        Table      table       = targetRangeVars[0].rangeTable;
        int        columnCount = table.getColumnCount();
        Expression condition   = null;

        readThis(Tokens.WHEN);

        if (token.tokenType == Tokens.MATCHED) {
            read();

            if (readIfThis(Tokens.AND)) {
                condition = XreadAndResolveBooleanValueExpression(
                    rangeGroups,
                    rangeGroup);
            }

            readThis(Tokens.THEN);

            if (readIfThis(Tokens.UPDATE)) {
                if (updateExpressions.size() > 0) {
                    throw Error.error(ErrorCode.X_42547);
                }

                conditions[1] = condition;

                readThis(Tokens.SET);
                readSetClauseList(
                    rangeGroups,
                    targetRangeVars,
                    updateTargetSet,
                    updateColIndexList,
                    updateExpressions);

                return StatementTypes.UPDATE_WHERE;
            } else {
                if (conditions[2] != null) {
                    throw Error.error(ErrorCode.X_42547);
                }

                if (condition == null) {
                    condition = Expression.EXPR_TRUE;
                }

                conditions[2] = condition;

                readThis(Tokens.DELETE);

                return StatementTypes.DELETE_WHERE;
            }
        } else if (token.tokenType == Tokens.NOT) {
            if (insertExpressions.size() > 0) {
                throw Error.error(ErrorCode.X_42548);
            }

            read();
            readThis(Tokens.MATCHED);

            if (readIfThis(Tokens.AND)) {
                condition = XreadAndResolveBooleanValueExpression(
                    rangeGroups,
                    rangeGroup);
            }

            conditions[0] = condition;

            readThis(Tokens.THEN);
            readThis(Tokens.INSERT);

            // parse INSERT statement
            // optional column list
            int brackets = readOpenBrackets();

            if (brackets == 1) {
                boolean withPrefix = database.sqlSyntaxOra;

                readSimpleColumnNames(
                    insertColumnNames,
                    targetRangeVars[0],
                    withPrefix);

                columnCount = insertColumnNames.size();

                readThis(Tokens.CLOSEBRACKET);

                brackets = 0;
            }

            readThis(Tokens.VALUES);

            Expression e = XreadContextuallyTypedTable(columnCount);

            if (e.nodes.length != 1) {
                throw Error.error(ErrorCode.X_21000);
            }

            insertExpressions.add(e);

            return StatementTypes.INSERT;
        } else {
            throw unexpectedToken();
        }
    }

    /**
     * Retrieves a CALL Statement from this parse context.
     */

    // to do call argument name and type resolution
    StatementDMQL compileCallStatement(
            RangeGroup[] rangeGroups,
            boolean isStrictlyProcedure) {

        read();

        if (isIdentifier()) {
            RoutineSchema routineSchema =
                (RoutineSchema) database.schemaManager.findSchemaObject(
                    session,
                    token.tokenString,
                    token.namePrefix,
                    token.namePrePrefix,
                    SchemaObject.PROCEDURE);

            if (routineSchema == null && token.namePrefix == null) {
                String schema = session.getSchemaName(null);
                ReferenceObject synonym = database.schemaManager.findSynonym(
                    token.tokenString,
                    schema,
                    SchemaObject.ROUTINE);

                if (synonym != null) {
                    HsqlName name = synonym.getTarget();

                    routineSchema =
                        (RoutineSchema) database.schemaManager.findSchemaObject(
                            name.name,
                            name.schema.name,
                            name.type);
                }
            }

            if (routineSchema != null) {
                read();

                return compileProcedureCall(rangeGroups, routineSchema);
            }
        }

        if (isStrictlyProcedure) {
            throw Error.error(ErrorCode.X_42501, token.tokenString);
        }

        Expression expression = XreadValueExpression();
        List<Expression> unresolved = expression.resolveColumnReferences(
            session,
            RangeGroup.emptyGroup,
            rangeGroups,
            null);

        ExpressionColumn.checkColumnsResolved(unresolved);
        expression.resolveTypes(session, null);

        StatementDMQL cs = new StatementProcedure(
            session,
            expression,
            compileContext);

        return cs;
    }

    StatementDMQL compileProcedureCall(
            RangeGroup[] rangeGroups,
            RoutineSchema routineSchema) {

        HsqlArrayList<Expression> list    = new HsqlArrayList<>();
        boolean                   bracket = true;

        if (database.sqlSyntaxOra) {
            bracket = readIfThis(Tokens.OPENBRACKET);
        } else {
            readThis(Tokens.OPENBRACKET);
        }

        if (bracket) {
            if (token.tokenType == Tokens.CLOSEBRACKET) {
                read();
            } else {
                while (true) {
                    Expression e = XreadValueExpression();

                    list.add(e);

                    if (token.tokenType == Tokens.COMMA) {
                        read();
                    } else {
                        readThis(Tokens.CLOSEBRACKET);
                        break;
                    }
                }
            }
        }

        Expression[] arguments = new Expression[list.size()];

        list.toArray(arguments);

        Routine routine = routineSchema.getSpecificRoutine(arguments.length);

        compileContext.addProcedureCall(routine);

        List<Expression> unresolved = null;

        for (int i = 0; i < arguments.length; i++) {
            Expression e = arguments[i];

            if (e.isUnresolvedParam()) {
                e.setAttributesAsColumn(routine.getParameter(i));
            } else {
                int paramMode = routine.getParameter(i).getParameterMode();

                unresolved = arguments[i].resolveColumnReferences(
                    session,
                    RangeGroup.emptyGroup,
                    rangeGroups,
                    unresolved);

                if (paramMode != SchemaObject.ParameterModes.PARAM_IN) {
                    if (e.getType() != OpTypes.VARIABLE) {
                        throw Error.error(ErrorCode.X_42603);
                    }
                }
            }
        }

        ExpressionColumn.checkColumnsResolved(unresolved);

        for (int i = 0; i < arguments.length; i++) {
            arguments[i].resolveTypes(session, null);

            if (!routine.getParameter(i)
                        .getDataType()
                        .canBeAssignedFrom(arguments[i].getDataType())) {
                throw Error.error(ErrorCode.X_42561);
            }
        }

        StatementDMQL cs = new StatementProcedure(
            session,
            routine,
            arguments,
            compileContext);

        return cs;
    }

    void resolveReferencesAndTypes(
            RangeGroup rangeGroup,
            RangeGroup[] rangeGroups,
            Expression e) {

        List<Expression> unresolved = e.resolveColumnReferences(
            session,
            rangeGroup,
            rangeGroup.getRangeVariables().length,
            rangeGroups,
            null,
            false);

        ExpressionColumn.checkColumnsResolved(unresolved);
        e.resolveTypes(session, null);
    }

    /**
     * Used in ROUTINE statements. Accepts NEXT VALUE FOR SEQUENCE as source
     */
    void resolveOuterReferencesAndTypes(
            RangeGroup[] rangeGroups,
            Expression e) {

        List<Expression> unresolved = e.resolveColumnReferences(
            session,
            RangeGroup.emptyGroup,
            rangeGroups,
            null);

        ExpressionColumn.checkColumnsResolved(unresolved);
        e.resolveTypes(session, null);
    }
}
