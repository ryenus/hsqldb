/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2007, The HSQL Development Group
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
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.rights.GrantConstants;
import org.hsqldb.store.BitMap;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.IntervalType;
import org.hsqldb.types.Type;
import org.hsqldb.types.TypedData;

// fredt@users 20020130 - patch 497872 by Nitin Chauhan - reordering for speed
// fredt@users 20020215 - patch 1.7.0 by fredt - support GROUP BY with more than one column
// fredt@users 20020215 - patch 1.7.0 by fredt - SQL standard quoted identifiers
// fredt@users 20020218 - patch 1.7.0 by fredt - DEFAULT keyword
// fredt@users 20020221 - patch 513005 by sqlbob@users - SELECT INTO types
// fredt@users 20020425 - patch 548182 by skitt@users - DEFAULT enhancement
// thertz@users 20020320 - patch 473613 by thertz - outer join condition bug
// fredt@users 20021229 - patch 1.7.2 by fredt - new solution for above
// fredt@users 20020420 - patch 523880 by leptipre@users - VIEW support
// fredt@users 20020525 - patch 559914 by fredt@users - SELECT INTO logging
// tony_lai@users 20021020 - patch 1.7.2 - improved aggregates and HAVING
// aggregate functions can now be used in expressions - HAVING supported
// kloska@users 20021030 - patch 1.7.2 - ON UPDATE CASCADE
// fredt@users 20021112 - patch 1.7.2 by Nitin Chauhan - use of switch
// rewrite of the majority of multiple if(){}else{} chains with switch(){}
// boucherb@users 20030705 - patch 1.7.2 - prepared statement support
// fredt@users 20030819 - patch 1.7.2 - EXTRACT({YEAR | MONTH | DAY | HOUR | MINUTE | SECOND } FROM datetime)
// fredt@users 20030820 - patch 1.7.2 - CHAR_LENGTH | CHARACTER_LENGTH | OCTET_LENGTH(string)
// fredt@users 20030820 - patch 1.7.2 - POSITION(string IN string)
// fredt@users 20030820 - patch 1.7.2 - SUBSTRING(string FROM pos [FOR length])
// fredt@users 20030820 - patch 1.7.2 - TRIM({LEADING | TRAILING | BOTH} [<character>] FROM <string expression>)
// fredt@users 20030820 - patch 1.7.2 - CASE [expr] WHEN ... THEN ... [ELSE ...] END and its variants
// fredt@users 20030820 - patch 1.7.2 - NULLIF(expr,expr)
// fredt@users 20030820 - patch 1.7.2 - COALESCE(expr,expr,...)
// fredt@users 20031012 - patch 1.7.2 - improved scoping for column names in all areas
// boucherb@users 200403xx - patch 1.7.2 - added support for prepared SELECT INTO
// boucherb@users 200403xx - doc 1.7.2 - some
// MarcH HuugO RIGHT JOIN SUPPORT
// thomasm@users 20041001 - patch 1.7.3 - BOOLEAN undefined handling
// fredt@users 20050220 - patch 1.8.0 - CAST with precision / scale
// fredt@users - version 1.9.0 - rewrite and support for many new statement types

/**
 * Responsible for parsing non-DDL statements.
 *
 * Based on Parser.java in HypersonicSQL.<p>
 * Extensively rewritten and extended in successive versions of HSQLDB.
 *
 * Rewrite in version 1.9.0.<p>
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @author fredt@users
 * @version 1.9.0
 * @since Hypersonic SQL
 */
class Parser extends BaseParser {

    protected CompileContext compileContext = new CompileContext();

    //

    /**
     *  Constructs a new Parser object with the given context.
     *
     * @param  db the Database instance against which to resolve named
     *      database object references
     * @param  t the token source from which to parse commands
     * @param  session the connected context
     */
    Parser(Session session, Database db, Tokenizer t) {
        super(session, db, t);
    }

    /**
     *  Resets this parse context with the given SQL character sequence.
     *
     * Internal structures are reset as though a new parser were created
     * with the given sql and the originally specified database and session
     *
     * @param sql a new SQL character sequence to replace the current one
     */
    void reset(String sql) {
        super.reset(sql);
        compileContext.reset();
    }

    void readColumnNames(OrderedHashSet columns,
                         RangeVariable rangeVar) throws HsqlException {

        while (true) {
            Column col = readColumnName(rangeVar);

            if (!columns.add(col.getName().name)) {
                throw Trace.error(Trace.COLUMN_ALREADY_EXISTS,
                                  col.getName().name);
            }

            if (tokenType == Token.COMMA) {
                read();

                continue;
            }

            if (tokenType == Token.CLOSEBRACKET) {
                break;
            }

            throw unexpectedToken();
        }
    }

    HsqlName[] readColumnNames() throws HsqlException {
        return (HsqlName[]) readColumnNames(true, false);
    }

    OrderedHashSet readColumnNames(boolean readAscDesc) throws HsqlException {
        return (OrderedHashSet) readColumnNames(false, readAscDesc);
    }

    private Object readColumnNames(boolean fullNames,
                                   boolean readAscDesc) throws HsqlException {

        int    i           = 0;
        BitMap quotedFlags = null;

        if (fullNames) {
            quotedFlags = new BitMap(32);
        }

        readThis(Token.OPENBRACKET);

        OrderedHashSet set = new OrderedHashSet();

        while (true) {
            checkIsSimpleName();

            if (namePrefix != null) {
                throw unexpectedToken();
            }

            if (!set.add(tokenString)) {
                throw Trace.error(Trace.COLUMN_ALREADY_EXISTS, tokenString);
            }

            if (fullNames && isQuoted) {
                quotedFlags.set(i);
            }

            read();

            i++;

            if (readAscDesc) {
                if (tokenType == Token.ASC || tokenType == Token.DESC) {
                    read();
                }
            }

            if (tokenType == Token.COMMA) {
                read();

                continue;
            }

            if (tokenType == Token.CLOSEBRACKET) {
                read();

                break;
            }

            throw unexpectedToken();
        }

        if (fullNames) {
            HsqlName[] colList = new HsqlName[set.size()];

            for (i = 0; i < colList.length; i++) {
                String  name   = (String) set.get(i);
                boolean quoted = quotedFlags.isSet(i);

                colList[i] = database.nameManager.newHsqlName(name, quoted);
            }

            return colList;
        }

        return set;
    }

    /**
     * The SubQuery objects are added to the end of subquery list.
     *
     * When parsing the SELECT for a view, optional HsqlName[] array is used
     * for view column aliases.
     *
     */
    SubQuery parseSubquery(int brackets, View view, boolean resolveAll,
                           int predicateType) throws HsqlException {

        compileContext.subQueryLevel++;

        boolean canHaveOrder = predicateType == Expression.VIEW;
        boolean limitWithOrder =
            predicateType == Expression.VIEW
            || predicateType == Expression.TABLE_SUBQUERY
            || predicateType == Expression.IN
            || predicateType == Expression.ALL
            || predicateType == Expression.ANY
            || predicateType == Expression.SCALAR_SUBQUERY
            || predicateType == Expression.ROW_SUBQUERY
            || predicateType == Expression.UNIQUE;
        boolean isExists = predicateType == Expression.EXISTS;
        boolean uniqueValues = predicateType == Expression.EXISTS
                               || predicateType == Expression.IN
                               || predicateType == Expression.ALL
                               || predicateType == Expression.ANY;
        Select select = readQueryExpression(brackets, canHaveOrder, false,
                                            limitWithOrder, resolveAll);

        if (predicateType == Expression.SCALAR_SUBQUERY) {
            if (select.visibleColumnCount != 1) {
                throw Trace.error(Trace.SINGLE_COLUMN_EXPECTED);
            }
        }

        boolean isCorrelated = !select.areColumnsResolved();

        if (view != null && view.colList != null) {
            if (view.colList.length != select.visibleColumnCount) {
                throw Trace.error(Trace.COLUMN_COUNT_DOES_NOT_MATCH);
            }

            for (int i = 0; i < select.visibleColumnCount; i++) {
                HsqlName name = view.colList[i];

                select.exprColumns[i].setAlias(name.name, name.isNameQuoted);
            }
        } else {
            for (int i = 0; i < select.visibleColumnCount; i++) {
                String colname = select.exprColumns[i].getAlias();

                if (colname == null || colname.length() == 0) {

                    // fredt - this does not guarantee the uniqueness of column
                    // names but addColumns() will throw later if names are not unique.
                    colname = HsqlNameManager.getAutoColumnNameString(i);

                    select.exprColumns[i].setAlias(colname, false);
                }
            }
        }

        SubQuery sq = new SubQuery(database, compileContext.subQueryLevel,
                                   isCorrelated, isExists, uniqueValues,
                                   select, view);

        compileContext.subQueryList.add(sq);

        compileContext.subQueryLevel--;

        return sq;
    }

    SubQuery getViewSubquery(View v) {

        SubQuery sq = v.viewSubQuery;

        for (int i = 0; i < v.viewSubqueries.length; i++) {
            compileContext.subQueryList.add(v.viewSubqueries[i]);
        }

        return sq;
    }

    /**
     * Parses the given token and any further tokens in tokenizer to return
     * any UNION or other set operation ID.
     */
    int readUnionType() throws HsqlException {

        int unionType = Select.NOUNION;

        switch (tokenType) {

            case Token.UNION :
                read();

                if (tokenType == Token.ALL) {
                    unionType = Select.UNIONALL;

                    read();
                } else if (tokenType == Token.DISTINCT) {
                    unionType = Select.UNION;

                    read();
                } else {
                    unionType = Select.UNION;
                }
                break;

            case Token.INTERSECT :
                read();
                readNoiseWord(Token.DISTINCT);

                unionType = Select.INTERSECT;
                break;

            case Token.EXCEPT :
            case Token.MINUS_EXCEPT :
                read();
                readNoiseWord(Token.DISTINCT);

                unionType = Select.EXCEPT;
                break;

            default :
                break;
        }

        return unionType;
    }

    Select getFullSelect(Table table) throws HsqlException {

        RangeVariable range = new RangeVariable(table, null, null,
            compileContext);
        Select select = new Select(compileContext, true);

        range.addTableColumns(select.exprColumnList, 0);

        select.visibleColumnCount = select.exprColumnList.size();

        select.addRangeVariable(range);
        select.finaliseColumns();
        select.finaliseRangeVariables();
        select.finishPrepare(session);

        return select;
    }

    /**
     *  Constructs and returns a Select object.
     *
     * @param canHaveOrder whether the SELECT being parsed can have an ORDER BY
     * @param canHaveLimit whether LIMIT without ORDER BY is allowed
     * @param limitWithOrder whether LIMIT is allowed only with ORDER BY
     * @param resolveAll whether all column references are resolved
     * @return a new Select object
     * @throws  HsqlException if a parsing error occurs
     */
    Select readQueryExpression(int brackets, boolean canHaveOrder,
                               boolean canHaveLimit, boolean limitWithOrder,
                               boolean resolveAll) throws HsqlException {

        Select  mainSelect    = null;
        Select  currentSelect = null;
        boolean end           = false;
        int     unionType     = Select.NOUNION;
        int     openBrackets  = brackets;

        while (true) {
            Select  select   = null;
            boolean isSelect = false;

            switch (tokenType) {

                case Token.TABLE :
                    read();

                    Table table = readTableName();

                    select = getFullSelect(table);
                    break;

                case Token.VALUES :
                    read();

                    SubQuery sq = readTableConstructor();

                    select = getFullSelect(sq.table);
                    break;

                case Token.SELECT :
                    select   = readQuerySpecification(mainSelect == null);
                    isSelect = true;
                    break;

                case Token.OPENBRACKET :
                    openBrackets = readOpenBrackets();
                    brackets     += openBrackets;

                    continue;
                default :
                    end = true;
                    break;
            }

            if (end) {
                break;
            }

            if (isSelect && (mainSelect == null || openBrackets != 0)) {
                readOrderByExpression(select);
            }

            int closeBrackets = readCloseBrackets(brackets);

            brackets  -= closeBrackets;
            unionType = readUnionType();

            boolean enclosed = openBrackets != 0 && closeBrackets != 0;

            openBrackets      = 0;
            select.unionType  = unionType;
            select.unionDepth = brackets;

            if (unionType != Select.NOUNION &&!enclosed) {
                if (select.orderByColumnCount != 0) {
                    throw Trace.error(Trace.INVALID_ORDER_BY);
                }

                if (select.limitCondition != null) {
                    throw Trace.error(Trace.INVALID_LIMIT);
                }
            }

            if (currentSelect == null) {
                currentSelect = select;
                mainSelect    = select;
            } else {
                select.finaliseAndResolve(resolveAll);

                currentSelect.unionSelect = select;
                currentSelect             = select;

                if (mainSelect.unionSelect.visibleColumnCount
                        != currentSelect.visibleColumnCount) {
                    throw Trace.error(Trace.COLUMN_COUNT_DOES_NOT_MATCH);
                }
            }

            if (unionType == Select.NOUNION) {
                break;
            }
        }

        brackets -= readCloseBrackets(brackets);

        if (mainSelect.orderByColumnCount == 0
                && mainSelect.limitCondition == null) {
            readOrderByExpression(mainSelect);

            if (mainSelect.orderByColumnCount != 0) {
                mainSelect.sortUnion = true;
            }
        }

        checkOrderBy(mainSelect, canHaveOrder, canHaveLimit, limitWithOrder);

        brackets -= readCloseBrackets(brackets);

        if (brackets != 0) {
            throw Trace.error(Trace.TOKEN_REQUIRED, Token.T_CLOSEBRACKET);
        }

        mainSelect.finaliseAndResolve(resolveAll);
        mainSelect.finishPrepare(session);

        return mainSelect;
    }

    Select readQuerySpecification(boolean isMain) throws HsqlException {

        readThis(Token.SELECT);

        Select select = readSelect(isMain);

        // table expression
        readFromClause(select);
        readWhereGroupHaving(select);

        return select;
    }

    Select readSelect(boolean isMain) throws HsqlException {

        Select select = new Select(compileContext, isMain);

        if (isMain) {
            readLimit(select, false);
        }

        if (tokenType == Token.DISTINCT) {
            select.isDistinctSelect = true;

            read();
        } else if (tokenType == Token.ALL) {
            read();
        }

        while (true) {
            Expression e = readOr();

            readNoiseWord(Token.AS);

            if (tokenType == Token.X_NAME) {
                checkIsSimpleName();
                e.setAlias(tokenString, isQuoted);
                read();
            }

            select.addSelectColumnExpression(e);

            if (tokenType == Token.INTO || tokenType == Token.FROM) {
                break;
            }

            if (tokenType == Token.COMMA) {
                read();

                continue;
            }

            throw unexpectedToken();
        }

        if (tokenType == Token.INTO) {
            read();

            switch (tokenType) {

                case Token.CACHED :
                    select.intoType = Table.CACHED_TABLE;

                    read();
                    break;

                case Token.TEMP :
                    select.intoType = Table.TEMP_TABLE;

                    read();
                    break;

                case Token.TEXT :
                    select.intoType = Table.TEXT_TABLE;

                    read();
                    break;

                case Token.MEMORY :
                    select.intoType = Table.MEMORY_TABLE;

                    read();
                    break;

                default :
                    select.intoType = database.getDefaultTableType();
                    break;
            }

            checkIsName();
            checkCatalogName();

            select.intoTableName =
                database.nameManager.newHsqlName(tokenString, isQuoted);
            select.intoTableName.schema =
                session.getSchemaHsqlName(namePrefix);

            read();
        }

        return select;
    }

    void readFromClause(Select select) throws HsqlException {

        readThis(Token.FROM);

        while (true) {
            readTableReference(select);

            if (tokenType == Token.COMMA) {
                read();

                continue;
            }

            break;
        }
    }

    void readTableReference(Select select) throws HsqlException {

        boolean       natural = false;
        RangeVariable range   = readTableOrSubquery();

        // parse table list
        select.addRangeVariable(range);

        while (true) {
            int     type  = tokenType;
            boolean outer = false;
            boolean full  = false;
            boolean end   = false;

            type = tokenType;

            switch (tokenType) {

                case Token.INNER :
                    read();
                    readThis(Token.JOIN);
                    break;

                case Token.CROSS :
                    if (natural) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Token.JOIN);
                    break;

                case Token.UNION :
                    if (natural) {
                        throw unexpectedToken();
                    }

                    int position = getPosition();

                    read();

                    if (tokenType == Token.JOIN) {
                        read();

                        break;
                    } else {
                        rewind(position);

                        end = true;

                        break;
                    }
                case Token.NATURAL :
                    if (natural) {
                        throw unexpectedToken();
                    }

                    read();

                    natural = true;

                    continue;
                case Token.LEFT :
                    read();
                    readNoiseWord(Token.OUTER);
                    readThis(Token.JOIN);

                    outer = true;
                    break;

                case Token.RIGHT :
                    read();
                    readNoiseWord(Token.OUTER);
                    readThis(Token.JOIN);

                    outer = true;
                    break;

                case Token.FULL :
                    read();
                    readNoiseWord(Token.OUTER);
                    readThis(Token.JOIN);

                    outer = true;
                    full  = true;
                    break;

                case Token.JOIN :
                    read();

                    type = Token.INNER;
                    break;

                case Token.COMMA :
                    if (natural) {
                        throw unexpectedToken();
                    }

                    read();

                    type = Token.CROSS;
                    break;

                default :
                    if (natural) {
                        throw unexpectedToken();
                    }

                    end = true;
                    break;
            }

            if (end) {
                break;
            }

            range = readTableOrSubquery();

            Expression condition = null;

            switch (type) {

                case Token.CROSS :
                    select.addRangeVariable(range);
                    break;

                case Token.UNION :
                    select.addRangeVariable(range);

                    condition = Expression.EXPR_FALSE;

                    range.setJoinType(true, true);
                    break;

                case Token.LEFT :
                case Token.RIGHT :
                case Token.INNER :
                case Token.FULL : {
                    if (natural) {
                        OrderedHashSet setA = select.getUniqueColumnNameSet();
                        OrderedHashSet setB =
                            range.getTable().getUniqueColumnNameSet();
                        OrderedHashSet columns = new OrderedHashSet();

                        for (int i = 0; i < setB.size(); i++) {
                            Object name = setB.get(i);

                            if (setA.contains(name)) {
                                columns.add(name);
                            }
                        }

                        Expression a = getRowExpression(columns);
                        Expression b = getRowExpression(columns);

                        select.resolveColumnReferences(a);
                        select.checkColumnsResolved();

                        // resolve against previous select columns too
                        OrderedHashSet set = b.resolveColumnReferences(
                            new RangeVariable[]{ range }, null);

                        if (set != null) {
                            String name =
                                ((Expression) set.get(0)).getColumnName();

                            throw Trace.error(Trace.COLUMN_NOT_FOUND, name);
                        }

                        condition = new Expression(a, b);

                        select.addRangeVariable(range);
                        range.addNamedJoinColumns(columns);
                    } else if (tokenType == Token.USING) {
                        read();

                        OrderedHashSet columns = new OrderedHashSet();

                        readThis(Token.OPENBRACKET);
                        readColumnNames(columns, range);
                        readThis(Token.CLOSEBRACKET);
                        select.checkUniqueColumnNameSet(columns);

                        Expression a = getRowExpression(columns);
                        Expression b = getRowExpression(columns);

                        select.resolveColumnReferences(a);
                        select.checkColumnsResolved();

                        // resolve against previous select columns too
                        OrderedHashSet set = b.resolveColumnReferences(
                            new RangeVariable[]{ range }, null);

                        if (set != null) {
                            String name =
                                ((Expression) set.get(0)).getColumnName();

                            throw Trace.error(Trace.COLUMN_NOT_FOUND, name);
                        }

                        condition = new Expression(a, b);

                        range.addNamedJoinColumns(columns);

                        if (type == Token.RIGHT) {
                            select.addRangeVariable(range, true);

                            range =
                                (RangeVariable) select.rangeVariableList.get(
                                    1);
                        } else {
                            select.addRangeVariable(range);
                        }
                    } else if (tokenType == Token.ON) {
                        read();

                        condition = readOr();

                        if (type == Token.RIGHT) {
                            select.addRangeVariable(range, true);

                            range =
                                (RangeVariable) select.rangeVariableList.get(
                                    1);
                        } else {
                            select.addRangeVariable(range);
                        }

                        select.resolveColumnReferences(condition);
                        select.checkColumnsResolved();

                        if (condition.getDataType().type
                                != Types.SQL_BOOLEAN) {
                            throw Trace.error(Trace.NOT_A_CONDITION);
                        }
                    } else {
                        throw Trace.error(Trace.UNEXPECTED_TOKEN,
                                          Trace.TOKEN_REQUIRED, new Object[] {
                            tokenString, Token.T_ON
                        });
                    }

                    range.setJoinType(outer, full);

                    break;
                }
            }

            range.addJoinCondition(condition);

            natural = false;
        }

        resolveColumnRangeVariables(select);
    }

    Expression getRowExpression(OrderedHashSet columnNames) {

        Expression[] argList = new Expression[columnNames.size()];

        for (int i = 0; i < argList.length; i++) {
            String name = (String) columnNames.get(i);

            argList[i] = new Expression(null, null, name, true);
        }

        return new Expression(Expression.ROW, argList);
    }

    void readWhereGroupHaving(Select select) throws HsqlException {

        // where
        if (tokenType == Token.WHERE) {
            read();

            Expression e    = readOr();
            Type       type = e.getDataType();

            if (type == null ||!type.isBooleanType()) {
                throw Trace.error(Trace.NOT_A_CONDITION);
            }

            select.addQueryCondition(e);
        }

        // group by
        if (tokenType == Token.GROUP) {
            read();
            readThis(Token.BY);

            while (true) {
                Expression e = readOr();

                select.addGroupByColumnExpression(e);

                if (tokenType == Token.COMMA) {
                    read();

                    continue;
                }

                break;
            }
        }

        // having
        if (tokenType == Token.HAVING) {
            read();

            Expression e    = readOr();
            Type       type = e.getDataType();

            if (type == null ||!type.isBooleanType()) {
                throw Trace.error(Trace.NOT_A_CONDITION);
            }

            select.addHavingExpression(e);
        }
    }

    void readOrderByExpression(Select select) throws HsqlException {

        if (tokenType == Token.ORDER) {
            read();
            readThis(Token.BY);
            readOrderBy(select);    // todo
        }

        if (tokenType == Token.LIMIT) {
            readLimit(select, true);
        }
    }

    void checkOrderBy(Select select, boolean canHaveOrder,
                      boolean canHaveLimit,
                      boolean limitWithOrder) throws HsqlException {

        boolean hasOrder = select.orderByColumnCount != 0;
        boolean hasLimit = select.limitCondition != null;

        if (limitWithOrder) {
            if (hasLimit &&!hasOrder) {
                throw Trace.error(Trace.ORDER_LIMIT_REQUIRED);
            }
        } else {
            if (hasOrder &&!canHaveOrder) {
                throw Trace.error(Trace.INVALID_ORDER_BY);
            }

            if (hasLimit &&!canHaveLimit) {
                throw Trace.error(Trace.INVALID_LIMIT);
            }
        }
    }

// fredt@users 20011010 - patch 471710 by fredt - LIMIT rewritten
// SELECT LIMIT n m DISTINCT ... queries and error message
// "SELECT LIMIT n m ..." creates the result set for the SELECT statement then
// discards the first n rows and returns m rows of the remaining result set
// "SELECT LIMIT 0 m" is equivalent to "SELECT TOP m" or "SELECT FIRST m"
// in other RDBMS's
// "SELECT LIMIT n 0" discards the first n rows and returns the remaining rows
// fredt@users 20020225 - patch 456679 by hiep256 - TOP keyword
    private void readLimit(Select select,
                           boolean isEnd) throws HsqlException {

        if (select.limitCondition != null) {
            return;
        }

        int        position = getPosition();
        Expression e1       = null;
        Expression e2;
        boolean    islimit = false;

        if (isEnd) {
            if (tokenType == Token.LIMIT) {
                islimit = true;

                read();

                e2 = readTerm();

                if (tokenType == Token.OFFSET) {
                    read();

                    e1 = readTerm();
                }
            } else {
                return;
            }
        } else if (tokenType == Token.LIMIT) {
            read();

            if (tokenType == Token.AS || tokenType == Token.COMMA
                    || tokenType == Token.FROM || isSimpleName()) {
                rewind(position);

                return;
            }

            e1      = readTerm();
            e2      = readTerm();
            islimit = true;
        } else if (tokenType == Token.TOP) {
            read();

            if (tokenType == Token.AS || tokenType == Token.COMMA
                    || tokenType == Token.FROM || isSimpleName()) {
                rewind(position);

                return;
            }

            e2 = readTerm();
        } else {
            return;
        }

        if (e1 == null) {
            e1 = new Expression(ValuePool.getInt(0), Type.SQL_INTEGER);
        }

        e1.resolveTypes(session, null);
        e2.resolveTypes(session, null);

        boolean valid = true;

        if (e1.isParam()) {
            e1.setDataType(Type.SQL_INTEGER);
        } else {
            valid = (e1.getType() == Expression.VALUE && e1.valueData != null
                     && e1.getDataType().type == Types.SQL_INTEGER
                     && ((Integer) e1.getValue(null)).intValue() >= 0);
        }

        if (e2.isParam()) {
            e2.setDataType(Type.SQL_INTEGER);
        } else {
            valid &= (e2.getType() == Expression.VALUE
                      && e2.valueData != null
                      && e2.getDataType().type == Types.SQL_INTEGER
                      && ((Integer) e2.getValue(null)).intValue() >= 0);
        }

        if (valid) {
            select.limitCondition = new Expression(Expression.LIMIT, e1, e2);

            return;
        }

        int messageid = islimit ? Trace.INVALID_LIMIT_EXPRESSION
                                : Trace.INVALID_TOP_EXPRESSION;

        throw Trace.error(Trace.WRONG_DATA_TYPE, messageid);
    }

    private void readOrderBy(Select select) throws HsqlException {

        while (true) {
            Expression e = readOr();

            e = new Expression(Expression.ORDER_BY, e);

            if (tokenType == Token.DESC) {
                e.setDescending();
                read();
            } else if (tokenType == Token.ASC) {
                read();
            }

            select.addOrderByExpression(e);

            if (tokenType == Token.COMMA) {
                read();

                continue;
            }

            break;
        }
    }

    private void resolveColumnRangeVariables(Select select)
    throws HsqlException {

        select.finaliseRangeVariables();

        for (int pos = 0; pos < select.exprColumnList.size(); ) {
            Expression e = (Expression) (select.exprColumnList.get(pos));

            if (e.getType() == Expression.ASTERISK) {

                // expand [table.]* columns
                select.exprColumnList.remove(pos);

                String tablename = e.getTableName();

                if (tablename == null) {
                    select.addAllJoinedColumns(select.exprColumnList, pos);
                } else {
                    RangeVariable range =
                        e.findMatchingRangeVariable(select.rangeVariables);

                    if (range == null) {
                        throw Trace.error(Trace.TABLE_NOT_FOUND, tablename);
                    }

                    pos = range.addTableColumns(select.exprColumnList, pos);
                }
            } else {
                if (e.getRangeVariable() == null) {
                    select.resolveColumnReferences(e);
                }

                pos++;
            }
        }

        select.visibleColumnCount = select.exprColumnList.size();
    }

    private RangeVariable readSimpleRangeVariable(int type)
    throws HsqlException {

        Table  table = readTableName();
        String alias = null;

        if (tokenString == Token.T_AS) {
            checkIsNameOrKeyword();
            read();
        }

        if (isSimpleName()) {
            alias = tokenString;

            read();
        }

        RangeVariable range = new RangeVariable(table, alias, null,
            compileContext);

        return range;
    }

    RangeVariable readRangeVariableWithAlias(Table t) throws HsqlException {

        String         alias      = null;
        OrderedHashSet columnList = null;

        if (tokenType == Token.LEFT || tokenType == Token.RIGHT) {}
        else {
            readNoiseWord(Token.OUTER);

            if (isSimpleName()) {
                alias = tokenString;

                read();

                if (tokenType == Token.OPENBRACKET) {
                    columnList = readColumnNames(false);

                    if (t.getColumnCount() != columnList.size()) {
                        throw Trace.error(Trace.COLUMN_COUNT_DOES_NOT_MATCH);
                    }
                }
            }
        }

        RangeVariable range = new RangeVariable(t, alias, columnList,
            compileContext);

        return range;
    }

    /**
     * Creates a RangeVariable from the parse context. <p>
     */
    private RangeVariable readTableOrSubquery() throws HsqlException {

        Table          t          = null;
        SubQuery       sq         = null;
        String         alias      = null;
        OrderedHashSet columnList = null;

        if (tokenType == Token.OPENBRACKET) {
            read();

            sq = parseSubquery(0, null, true, Expression.TABLE_SUBQUERY);

            readThis(Token.CLOSEBRACKET);

            t = sq.table;
        } else {
            t = readTableName();

            if (t.isView()) {
                sq        = getViewSubquery((View) t);
                sq.select = ((View) t).viewSelect;
                t         = sq.table;
            }
        }

        if (tokenType == Token.AS) {
            read();
            checkIsSimpleName();

            alias = tokenString;

            read();

            if (tokenType == Token.OPENBRACKET) {
                columnList = readColumnNames(false);
            }
        }

        if (tokenType == Token.LEFT || tokenType == Token.RIGHT) {

            // allow for LEFT(...), RIGHT(...)
        } else if (isSimpleName()) {
            int position = -1;

            if (tokenType == Token.LIMIT) {
                position = getPosition();
            }

            alias = tokenString;

            read();

            if (tokenType == Token.OPENBRACKET) {
                columnList = readColumnNames(false);
            } else if (tokenType == Token.QUESTION
                       || tokenType == Token.X_VALUE) {
                if (position != -1) {
                    alias = null;

                    rewind(position);
                }
            }
        }

        if (columnList != null && t.getColumnCount() != columnList.size()) {
            throw Trace.error(Trace.COLUMN_COUNT_DOES_NOT_MATCH);
        }

        RangeVariable range = new RangeVariable(t, alias, columnList,
            compileContext);

        return range;
    }

    /**
     *  Method declaration
     *
     * @return the Expression resulting from the parse
     * @throws  HsqlException
     */
    Expression parseExpression() throws HsqlException {

        read();

        Expression e = readOr();

        return e;
    }

    private Expression readAggregate() throws HsqlException {

        int     type     = Parser.getExpressionType(tokenType);
        boolean distinct = false;
        boolean all      = false;

        read();
        readThis(Token.OPENBRACKET);

        if (tokenType == Token.DISTINCT) {
            distinct = true;

            read();
        } else if (tokenType == Token.ALL) {
            all = true;

            read();
        }

        Expression s = readOr();

        readThis(Token.CLOSEBRACKET);

        if ((all || distinct)
                && (type == Expression.STDDEV_POP
                    || type == Expression.STDDEV_SAMP
                    || type == Expression.VAR_POP
                    || type == Expression.VAR_SAMP)) {
            throw Trace.error(Trace.INVALID_FUNCTION_ARGUMENT);
        }

        Expression aggregateExp = new Expression(type, distinct, s);

        return aggregateExp;
    }

    /**
     *  Method declaration
     *
     * @return a disjuntion, possibly degenerate
     * @throws  HsqlException
     */
    Expression readOr() throws HsqlException {

        switch (tokenType) {

            case Token.DEFAULT :
                read();

                return new Expression(Expression.DEFAULT);

            case Token.NEXT :
                read();

                return readSequenceExpression();

            default :
        }

        Expression e = readAnd();

        while (tokenType == Token.OR) {
            Expression a = e;

            read();

            e = new Expression(Expression.OR, a, readAnd());
        }

        return e;
    }

    /**
     *  Method declaration
     *
     * @return a conjunction, possibly degenerate
     * @throws  HsqlException
     */
    private Expression readAnd() throws HsqlException {

        Expression e = readCondition();

        while (tokenType == Token.AND) {
            Expression a = e;

            read();

            e = new Expression(Expression.AND, a, readCondition());
        }

        return e;
    }

    /**
     *  Method declaration
     *
     * @return a predicate, possibly composite
     * @throws  HsqlException
     */
    Expression readCondition() throws HsqlException {

        switch (tokenType) {

            case Token.NOT : {
                read();

                return new Expression(Expression.NOT, readCondition());
            }
            case Token.EXISTS : {
                read();
                readThis(Token.OPENBRACKET);

                int brackets = readOpenBrackets();

                if (tokenType != Token.SELECT) {
                    throw unexpectedToken();
                }

                SubQuery sq = parseSubquery(brackets, null, false,
                                            Expression.EXISTS);
                Expression s = new Expression(Expression.TABLE_SUBQUERY, sq);

                readThis(Token.CLOSEBRACKET);

                return new Expression(Expression.EXISTS, s);
            }
            case Token.UNIQUE : {
                read();
                readThis(Token.OPENBRACKET);

                int brackets = readOpenBrackets();

                if (tokenType != Token.SELECT) {
                    throw unexpectedToken();
                }

                SubQuery sq = parseSubquery(brackets, null, false,
                                            Expression.UNIQUE);
                Expression s = new Expression(Expression.TABLE_SUBQUERY, sq);

                readThis(Token.CLOSEBRACKET);

                return new Expression(Expression.UNIQUE, s);
            }
            default : {
                Expression a = readConcat();

                return readConditionRightPart(a);
            }
        }
    }

    private Expression readConditionRightPart(Expression l)
    throws HsqlException {

        boolean hasNot = false;

        if (tokenType == Token.NOT) {
            hasNot = true;

            read();
        }

        switch (tokenType) {

            case Token.IS : {
                if (hasNot) {
                    throw unexpectedToken();
                }

                read();

                if (tokenType == Token.NOT) {
                    hasNot = true;

                    read();
                }

                if (tokenType == Token.DISTINCT) {
                    read();
                    readThis(Token.FROM);

                    l = new Expression(Expression.NOT_DISTINCT, l,
                                       readConcat());
                    hasNot = !hasNot;

                    break;
                }

                if (tokenType == Token.X_VALUE) {
                    if (value == null) {
                        l = new Expression(Expression.IS_NULL, l);

                        read();

                        break;
                    } else if (Type.SQL_BOOLEAN == valueType) {
                        Expression b = ((Boolean) value).booleanValue()
                                       ? Expression.EXPR_TRUE
                                       : Expression.EXPR_FALSE;

                        l = new Expression(l, b);

                        read();

                        break;
                    }
                }

                if (tokenType == Token.UNKNOWN) {
                    l = new Expression(Expression.IS_NULL, l);

                    read();

                    break;
                }

                throw unexpectedToken();
            }
            case Token.LIKE : {
                l = readLikePredicate(l);

                break;
            }
            case Token.BETWEEN : {
                l = readBetweenPredicate(l);

                break;
            }
            case Token.IN : {
                l = readInPredicate(l);

                break;
            }
            case Token.OVERLAPS : {
                if (hasNot) {
                    throw unexpectedToken();
                }

                return readOverlapsPredicate(l);
            }
            case Token.EQUALS :
            case Token.GREATER_EQUALS :
            case Token.GREATER :
            case Token.LESS :
            case Token.LESS_EQUALS :
            case Token.NOT_EQUALS : {
                if (hasNot) {
                    throw unexpectedToken();
                }

                int type = getExpressionType(tokenType);

                read();

                return new Expression(type, l, readConcat());
            }
            case Token.MATCH : {
                l = readMatchPredicate(l);

                break;
            }
            default : {
                if (hasNot) {
                    throw unexpectedToken();
                }

                return l;
            }
        }

        if (hasNot) {
            l = new Expression(Expression.NOT, l);
        }

        return l;
    }

    private Expression readLikePredicate(Expression a) throws HsqlException {

        read();

        Expression b      = readConcat();
        Character  escape = null;

        if (tokenString.equals(Token.T_ESCAPE)) {
            read();

            Expression c = readTerm();

            Trace.check(c.getType() == Expression.VALUE,
                        Trace.INVALID_ESCAPE);

            String s = (String) c.getValue(session, Type.SQL_VARCHAR);

            // boucherb@users 2003-09-25
            // TODO:
            // SQL200n says binary escape can be 1 or more octets.
            // Maybe we need to retain s and check this in
            // Expression.resolve()?
            if (s == null || s.length() < 1) {
                throw Trace.error(Trace.INVALID_ESCAPE, s);
            }

            escape = new Character(s.charAt(0));
        }

        boolean hasCollation = database.collation.name != null;

        a = new Expression(a, b, escape, hasCollation);

        return a;
    }

    private Expression readMatchPredicate(Expression a) throws HsqlException {

        boolean isUnique  = false;
        int     matchType = Expression.MATCH_SIMPLE;

        read();

        if (tokenType == Token.UNIQUE) {
            isUnique = true;

            read();
        }

        if (tokenType == Token.SIMPLE) {
            read();

            matchType = isUnique ? Expression.MATCH_UNIQUE_SIMPLE
                                 : Expression.MATCH_SIMPLE;
        } else if (tokenType == Token.PARTIAL) {
            read();

            matchType = isUnique ? Expression.MATCH_UNIQUE_PARTIAL
                                 : Expression.MATCH_PARTIAL;
        } else if (tokenType == Token.FULL) {
            read();

            matchType = isUnique ? Expression.MATCH_UNIQUE_FULL
                                 : Expression.MATCH_FULL;
        }

        readThis(Token.OPENBRACKET);

        int brackets = readOpenBrackets();

        if (tokenType != Token.SELECT) {
            throw unexpectedToken();
        }

        SubQuery   sq = parseSubquery(brackets, null, false, Expression.IN);
        Expression s  = new Expression(Expression.TABLE_SUBQUERY, sq);

        readThis(Token.CLOSEBRACKET);

        return new Expression(matchType, a, s);
    }

    private Expression readBetweenPredicate(Expression a)
    throws HsqlException {

        boolean symmetric = false;

        read();

        if (tokenType == Token.ASYMMETRIC) {
            read();
        } else if (tokenType == Token.SYMMETRIC) {
            symmetric = true;

            read();
        }

        Expression left = readConcat();

        readThis(Token.AND);

        Expression right = readConcat();

        if (a.isParam() && left.isParam()) {
            throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                              Trace.Parser_ambiguous_between1);
        }

        if (a.isParam() && right.isParam()) {
            throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                              Trace.Parser_ambiguous_between1);
        }

        Expression l = new Expression(Expression.GREATER_EQUAL, a, left);
        Expression r = new Expression(Expression.SMALLER_EQUAL, a, right);
        Expression leftToRight = new Expression(Expression.AND, l, r);

        if (symmetric) {
            l = new Expression(Expression.SMALLER_EQUAL, a, left);
            r = new Expression(Expression.GREATER_EQUAL, a, right);

            Expression rightToLeft = new Expression(Expression.AND, l, r);

            return new Expression(Expression.OR, leftToRight, rightToLeft);
        } else {
            return leftToRight;
        }
    }

    private Expression readOverlapsPredicate(Expression l)
    throws HsqlException {

        if (l.exprType != Expression.ROW) {

            // todo -SQL error
            throw Trace.error(Trace.WRONG_DATA_TYPE);
        }

        if (l.argList.length != 2) {

            // todo -SQL error
            throw Trace.error(Trace.WRONG_DATA_TYPE);
        }

        read();

        if (tokenType != Token.OPENBRACKET) {
            throw unexpectedToken();
        }

        Expression r = readRow();

        if (r.argList.length != 2) {

            // todo -SQL error
            throw Trace.error(Trace.WRONG_DATA_TYPE);
        }

        readThis(Token.CLOSEBRACKET);

        return new Expression(Expression.OVERLAPS, l, r);
    }

    private Expression readInPredicate(Expression l) throws HsqlException {

        int        degree = l.exprType == Expression.ROW ? l.argList.length
                                                         : 1;
        Expression e      = null;
        int        brackets;

        read();
        readThis(Token.OPENBRACKET);

        int position = getPosition();

        brackets = readOpenBrackets();

        if (tokenType == Token.SELECT) {
            SubQuery sq = parseSubquery(brackets, null, false, Expression.IN);

            e = new Expression(Expression.TABLE_SUBQUERY, sq);

            readThis(Token.CLOSEBRACKET);
        } else {
            if (brackets > 0) {
                rewind(position);
            }

            e = readFormattedTable(degree);

            readThis(Token.CLOSEBRACKET);

            compileContext.subQueryLevel++;

            SubQuery sq = new SubQuery(database,
                                       compileContext.subQueryLevel, true,
                                       false, true, null, null);

            compileContext.subQueryList.add(sq);

            compileContext.subQueryLevel--;

            e.subQuery = sq;
        }

        return new Expression(Expression.IN, l, e);
    }

    private Expression readAllAnyPredicate(int type) throws HsqlException {

        read();
        readThis(Token.OPENBRACKET);

        Expression e        = null;
        int        brackets = readOpenBrackets();

        if (tokenType != Token.SELECT) {
            throw Trace.error(Trace.INVALID_IDENTIFIER);
        }

        SubQuery sq = parseSubquery(brackets, null, false, type);

        e = new Expression(Expression.TABLE_SUBQUERY, sq);

        readThis(Token.CLOSEBRACKET);

        return new Expression(type, e);
    }

    /**
     */
    private Expression readConcat() throws HsqlException {

        Expression e = readSum();

        while (tokenType == Token.CONCAT) {
            Expression a = e;

            read();

            e = new Expression(Expression.CONCAT, a, readSum());
        }

        return e;
    }

    /**
     */
    private Expression readSum() throws HsqlException {

        Expression e = readFactor();

        while (true) {
            int type;

            if (tokenType == Token.PLUS) {
                type = Expression.ADD;
            } else if (tokenType == Token.MINUS) {
                type = Expression.SUBTRACT;
            } else {
                break;
            }

            Expression a = e;

            read();

            e = new Expression(type, a, readFactor());
        }

        return e;
    }

    private Expression readFactor() throws HsqlException {

        Expression e = readTerm();
        int        type;

        while (true) {
            if (tokenType == Token.ASTERISK) {
                type = Expression.MULTIPLY;
            } else if (tokenType == Token.DIVIDE) {
                type = Expression.DIVIDE;
            } else {
                break;
            }

            Expression a = e;

            read();

            e = new Expression(type, a, readTerm());
        }

        return e;
    }

    private Expression readTerm() throws HsqlException {

        Expression  e        = null;
        SQLFunction function = LegacyFunction.newLegacyFunction(tokenString);

        try {
            if (function != null) {
                return readSQLFunction(function);
            }
        } catch (HsqlException ex) {}

        function = SQLFunction.newSQLFunction(tokenString);

        if (function != null) {
            return readSQLFunction(function);
        }

        switch (tokenType) {

            case Token.LEFT :
            case Token.RIGHT :
            case Token.LIMIT :
            case Token.TOP :
            case Token.X_NAME :
                return readColumnOrFunctionExpression();

            case Token.MINUS :
                read();

                return new Expression(Expression.NEGATE, readTerm());

            case Token.PLUS :
                read();

                return readTerm();

            case Token.OPENBRACKET :
                read();

                e = readRow();

                readThis(Token.CLOSEBRACKET);

                if (e.getType() == Expression.SUBTRACT) {
                    e.dataType = readPossibleIntervalType();
                }

                return e;

            case Token.X_VALUE :
                e = readValueExpression();

                return e;

            case Token.QUESTION :
                e = new Expression((Object) null, (Type) null, true);

                compileContext.parameters.add(e);
                read();

                return e;

            case Token.SELECT :
                SubQuery sq = parseSubquery(0, null, false,
                                            Expression.SCALAR_SUBQUERY);

                e = new Expression(Expression.SCALAR_SUBQUERY, sq);

                return e;

            case Token.ANY :
                return readAllAnyPredicate(Expression.ANY);

            case Token.ALL :
                return readAllAnyPredicate(Expression.ALL);

            case Token.ASTERISK :
                e = new Expression(namePrePrefix, namePrefix);

                read();

                return e;

            case Token.CASEWHEN :
                return readCaseWhenExpression();

            case Token.CASE :
                return readCaseExpression();

            case Token.NULLIF :
                return readNullIfExpression();

            case Token.COALESCE :
            case Token.IFNULL :
                return readCoalesceExpression();

            case Token.CAST :
            case Token.CONVERT :
                return readCastExpression();

            case Token.DATE :
                return readDateExpression();

            case Token.TIME :
                return readTimeExpression();

            case Token.TIMESTAMP :
                return readTimestampExpression();

            case Token.INTERVAL :
                return readIntervalExpression();

            case Token.COUNT :
            case Token.MAX :
            case Token.MIN :
            case Token.SUM :
            case Token.AVG :
            case Token.EVERY :
            case Token.SOME :
            case Token.STDDEV_POP :
            case Token.STDDEV_SAMP :
            case Token.VAR_POP :
            case Token.VAR_SAMP :
                return readAggregate();

            default :
                if (isCoreReservedKey || isSpecial) {
                    throw unexpectedToken();
                }

                return readColumnOrFunctionExpression();
        }
    }

    Expression readValueExpression() throws HsqlException {

        Expression e = new Expression(value, valueType);

        read();

        while (true) {
            if (valueType == null || e.dataType == null) {
                break;
            }

            if (e.dataType.isCharacterType()) {
                if (tokenType == Token.X_VALUE
                        && valueType.isCharacterType()) {
                    e.dataType = e.dataType.getCombinedType(valueType,
                            Expression.CONCAT);
                    e.valueData = e.dataType.concat(null, e.valueData, value);

                    read();

                    continue;
                }
            } else if (e.dataType.isBinaryType()) {
                if (tokenType == Token.X_VALUE && valueType.isBinaryType()) {
                    e.dataType = e.dataType.getCombinedType(valueType,
                            Expression.CONCAT);
                    e.valueData = e.dataType.concat(null, e.valueData, value);

                    read();

                    continue;
                }
            }

            break;
        }

        return e;
    }

    Expression readRow() throws HsqlException {

        Expression r = null;

//        read();
        while (true) {
            Expression e = readOr();

            if (r == null) {
                r = e;
            } else if (r.exprType == Expression.ROW) {
                if (e.exprType == Expression.ROW
                        && r.argList[0].exprType != Expression.ROW) {
                    r = new Expression(Expression.ROW, new Expression[] {
                        r, e
                    });
                } else {
                    r.argList =
                        (Expression[]) ArrayUtil.resizeArray(r.argList,
                            r.argList.length + 1);
                    r.argList[r.argList.length - 1] = e;
                }
            } else {
                r = new Expression(Expression.ROW, new Expression[] {
                    r, e
                });
            }

            if (tokenType != Token.COMMA) {
                break;
            }

            read();
        }

        return r;
    }

    Expression readFormattedRowOrTable(int degree) throws HsqlException {

        Expression   e       = readRow();
        Expression[] list    = e.argList;
        boolean      isTable = false;

        if (degree == 1) {
            if (e.exprType == Expression.ROW) {
                e.exprType = Expression.TABLE;

                for (int i = 0; i < list.length; i++) {
                    if (list[i].exprType != Expression.ROW) {
                        list[i] = new Expression(Expression.ROW,
                                                 new Expression[]{ list[i] });
                    } else if (list[i].argList.length != degree) {

                        // SQL error message
                        throw Trace.error(Trace.COLUMN_COUNT_DOES_NOT_MATCH);
                    }
                }

                return e;
            } else {
                e = new Expression(Expression.ROW, new Expression[]{ e });
                e = new Expression(Expression.TABLE, new Expression[]{ e });

                return e;
            }
        }

        if (e.exprType != Expression.ROW) {

            // SQL error message
            throw Trace.error(Trace.COLUMN_COUNT_DOES_NOT_MATCH);
        }

        for (int i = 0; i < list.length; i++) {
            if (list[i].exprType == Expression.ROW) {
                isTable = true;

                break;
            }
        }

        if (isTable) {
            e.exprType = Expression.TABLE;

            for (int i = 0; i < list.length; i++) {
                if (list[i].exprType != Expression.ROW) {

                    // SQL error message
                    throw Trace.error(Trace.COLUMN_COUNT_DOES_NOT_MATCH);
                }

                Expression[] args = list[i].argList;

                if (args.length != degree) {

                    // SQL error message
                    throw Trace.error(Trace.COLUMN_COUNT_DOES_NOT_MATCH);
                }

                for (int j = 0; j < degree; j++) {
                    if (args[j].exprType == Expression.ROW) {

                        // SQL error message
                        throw Trace.error(Trace.COLUMN_COUNT_DOES_NOT_MATCH);
                    }
                }
            }
        } else {
            if (list.length != degree) {

                // SQL error message
                throw Trace.error(Trace.COLUMN_COUNT_DOES_NOT_MATCH);
            }

            e = new Expression(Expression.TABLE, new Expression[]{ e });
        }

        return e;
    }

    private SubQuery readTableConstructor() throws HsqlException {

        compileContext.subQueryLevel++;

        Expression e = readFormattedTable();
        OrderedHashSet set = e.resolveColumnReferences(new RangeVariable[]{},
            null);

        if (set != null) {
            throw Trace.error(Trace.UNRESOLVED_TYPE);
        }

        e.resolveTypes(session, null);
        e.prepareTable(session, null, e.argList[0].argList.length);

        SubQuery sq = new SubQuery(database, compileContext.subQueryLevel,
                                   false, e);

        compileContext.subQueryList.add(sq);

        compileContext.subQueryLevel--;

        return sq;
    }

    Expression readFormattedTable() throws HsqlException {

        Expression r = null;

        while (true) {
            int        brackets = readOpenBrackets();
            Expression e        = readRow();

            readCloseBrackets(brackets);

            if (r == null) {
                r = new Expression(Expression.ROW, new Expression[]{ e });
            } else {
                r.argList = (Expression[]) ArrayUtil.resizeArray(r.argList,
                        r.argList.length + 1);
                r.argList[r.argList.length - 1] = e;
            }

            if (tokenType != Token.COMMA) {
                break;
            }

            read();
        }

        Expression[] list   = r.argList;
        int          degree = 1;

        if (list[0].argList != null) {
            degree = list[0].argList.length;
        }

        r.exprType = Expression.TABLE;

        for (int i = 0; i < list.length; i++) {
            if (list[i].exprType == Expression.ROW) {
                if (list[i].argList.length != degree) {

                    // SQL error message
                    throw Trace.error(Trace.COLUMN_COUNT_DOES_NOT_MATCH);
                }
            } else {
                if (degree != 1) {

                    // SQL error message
                    throw Trace.error(Trace.COLUMN_COUNT_DOES_NOT_MATCH);
                }

                list[i] = new Expression(Expression.ROW,
                                         new Expression[]{ list[i] });
            }
        }

        return r;
    }

    Expression readFormattedTable(int degree) throws HsqlException {

        Expression e = readRow();

        if (e.exprType == Expression.ROW) {
            e.exprType = Expression.TABLE;
        } else {
            e = new Expression(Expression.ROW, new Expression[]{ e });
            e = new Expression(Expression.TABLE, new Expression[]{ e });
        }

        Expression[] list = e.argList;

        for (int i = 0; i < list.length; i++) {
            if (list[i].exprType != Expression.ROW) {
                list[i] = new Expression(Expression.ROW,
                                         new Expression[]{ list[i] });
            }

            Expression[] args = list[i].argList;

            if (args.length != degree) {

                // SQL error message
                throw unexpectedToken();
            }

            for (int j = 0; j < degree; j++) {
                if (args[j].exprType == Expression.ROW) {

                    // SQL error message
                    throw unexpectedToken();
                }
            }
        }

        return e;
    }

    Expression readDateExpression() throws HsqlException {

        read();

        Expression e = readTerm();

        if (e.getType() == Expression.VALUE
                && e.getDataType().type == Types.SQL_CHAR) {
            String s    = (String) e.getValue(null);
            Object date = HsqlDateTime.dateValue(s);

            return new Expression(date, DateTimeType.SQL_DATE);
        }

        throw unexpectedToken();
    }

    Expression readTimeExpression() throws HsqlException {

        read();

        Expression e = readTerm();

        if (e.getType() == Expression.VALUE
                && e.getDataType().type == Types.SQL_CHAR) {
            String    s    = (String) e.getValue(null);
            TypedData data = DateTimeType.newTime(s);

            return new Expression(data.value, data.type);
        }

        throw unexpectedToken();
    }

    Expression readTimestampExpression() throws HsqlException {

        read();

        Expression e = readTerm();

        if (e.getType() == Expression.VALUE
                && e.getDataType().type == Types.SQL_CHAR) {
            String s     = (String) e.getValue(null);
            Object date  = HsqlDateTime.timestampValue(s);
            int    scale = DateTimeType.getScale(s);
            Type type = DateTimeType.getDateTimeType(Types.SQL_TIMESTAMP,
                scale);

            return new Expression(date, type);
        }

        throw unexpectedToken();
    }

    Expression readIntervalExpression() throws HsqlException {

        readQuotedString();

        String s = tokenString;

        read();

        IntervalType type = readIntervalType();
        Object       o    = type.newInterval(s);

        return new Expression(o, type);
    }

    IntervalType readPossibleIntervalType() throws HsqlException {

        if (ArrayUtil.find(Token.SQL_INTERVAL_FIELD_NAMES, tokenType) == -1) {
            return null;
        }

        return readIntervalType();
    }

    /**
     * Reads the type part of the INTERVAL
     */
    IntervalType readIntervalType() throws HsqlException {

        int precision = -1;
        int scale     = -1;
        int startToken;
        int endToken;

        startToken = endToken = tokenType;

        read();

        if (tokenType == Token.OPENBRACKET) {
            read();

            precision = readInteger();

            if (precision <= 0) {
                throw Trace.error(Trace.INVALID_IDENTIFIER);
            }

            if (tokenType == Token.COMMA) {
                if (startToken != Token.SECOND) {
                    throw Trace.error(Trace.INVALID_IDENTIFIER);
                }

                read();

                scale = readInteger();

                if (scale < 0) {
                    throw Trace.error(Trace.INVALID_IDENTIFIER);
                }
            }

            readThis(Token.CLOSEBRACKET);
        }

        if (tokenType == Token.TO) {
            read();

            endToken = tokenType;

            read();
        }

        if (tokenType == Token.OPENBRACKET) {
            if (endToken != Token.SECOND || endToken == startToken) {
                throw Trace.error(Trace.INVALID_IDENTIFIER);
            }

            read();

            scale = readInteger();

            if (scale < 0) {
                throw Trace.error(Trace.INVALID_IDENTIFIER);
            }

            readThis(Token.CLOSEBRACKET);
        }

        return getIntervalSpecificType(startToken, endToken, precision,
                                       scale);
    }

    IntervalType getIntervalSpecificType(int startToken, int endToken,
                                         int precision,
                                         int scale) throws HsqlException {

        int startIndex = ArrayUtil.find(Token.SQL_INTERVAL_FIELD_NAMES,
                                        startToken);
        int endIndex = ArrayUtil.find(Token.SQL_INTERVAL_FIELD_NAMES,
                                      endToken);

        return IntervalType.getIntervalType(startIndex, endIndex, precision,
                                            scale);
    }

    Expression readCaseExpression() throws HsqlException {

        Expression predicand = null;

        read();

        if (tokenType != Token.WHEN) {
            predicand = readOr();
        }

        return readCaseWhen(predicand);
    }

    /**
     * Reads part of a CASE .. WHEN  expression
     */
    private Expression readCaseWhen(final Expression l) throws HsqlException {

        readThis(Token.WHEN);

        Expression condition = null;

        if (l == null) {
            condition = readOr();
        } else {
            while (true) {
                Expression newCondition = readConditionRightPart(l);

                if (l == newCondition) {
                    newCondition = new Expression(l, readOr());
                }

                if (condition == null) {
                    condition = newCondition;
                } else {
                    condition = new Expression(Expression.OR, condition,
                                               newCondition);
                }

                if (tokenType == Token.COMMA) {
                    read();
                } else {
                    break;
                }
            }
        }

        readThis(Token.THEN);

        Expression current  = readOr();
        Expression elseExpr = null;

        if (tokenType == Token.WHEN) {
            elseExpr = readCaseWhen(l);
        } else if (tokenType == Token.ELSE) {
            read();

            elseExpr = readOr();

            readThis(Token.END);
            readNoiseWord(Token.CASE);
        } else {
            elseExpr = l == null
                       ? new Expression((Object) null, Type.SQL_ALL_TYPES)
                       : new Expression((Object) "", Type.SQL_CHAR);

            readThis(Token.END);
            readNoiseWord(Token.CASE);
        }

        Expression alternatives = new Expression(Expression.ALTERNATIVE,
            current, elseExpr);
        Expression casewhen = new Expression(Expression.CASEWHEN, condition,
                                             alternatives);

        return casewhen;
    }

    /**
     * reads a CASEWHEN expression
     */
    private Expression readCaseWhenExpression() throws HsqlException {

        Expression l = null;

        read();
        readThis(Token.OPENBRACKET);

        l = readOr();

        readThis(Token.COMMA);

        Expression thenelse = readOr();

        readThis(Token.COMMA);

        // thenelse part is never evaluated; only init
        thenelse = new Expression(Expression.ALTERNATIVE, thenelse, readOr());
        l        = new Expression(Expression.CASEWHEN, l, thenelse);

        readThis(Token.CLOSEBRACKET);

        return l;
    }

    /**
     * Reads a CAST or CONVERT expression
     */
    private Expression readCastExpression() throws HsqlException {

        boolean isConvert = tokenType == Token.CONVERT;

        read();
        readThis(Token.OPENBRACKET);

        Expression l = readOr();

        if (isConvert) {
            readThis(Token.COMMA);
        } else {
            readThis(Token.AS);
        }

        Type typeObject = readTypeDefinition();

        if (l.isParam()) {
            l.setDataType(typeObject);
        }

        l = new Expression(l, typeObject);

        readThis(Token.CLOSEBRACKET);

        return l;
    }

    Type readTypeDefinition() throws HsqlException {

        int typeNumber = Type.getTypeNr(tokenString);

        read();

        switch (typeNumber) {

            case Types.SQL_CHAR :
                if (tokenType == Token.VARYING) {
                    read();

                    typeNumber = Types.SQL_VARCHAR;
                } else if (tokenType == Token.LARGE) {
                    readThis(Token.OBJECT);
                    read();

                    typeNumber = Types.SQL_CLOB;
                }
                break;

            case Types.SQL_DOUBLE :
                if (tokenType == Token.PRECISION) {
                    read();
                }
                break;

            case Types.SQL_BINARY :
                if (tokenType == Token.VARYING) {
                    read();

                    typeNumber = Types.SQL_VARBINARY;
                } else if (tokenType == Token.LARGE) {
                    readThis(Token.OBJECT);
                    read();

                    typeNumber = Types.SQL_BLOB;
                }
                break;

            case Types.SQL_INTERVAL :
                return readIntervalType();

            default :
        }

        int length = 0;
        int scale  = 0;

        if (Types.acceptsPrecisionCreateParam(typeNumber)
                && tokenType == Token.OPENBRACKET) {
            read();

            length = readInteger();

            if (Types.acceptsScaleCreateParam(typeNumber)
                    && tokenType == Token.COMMA) {
                read();

                scale = readInteger();
            }

            readThis(Token.CLOSEBRACKET);

            if (length < 0 || scale < 0) {
                throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
            }
        }

        if (typeNumber == Types.SQL_TIMESTAMP
                || typeNumber == Types.SQL_TIME) {
            scale  = length;
            length = 0;

            if (scale > DateTimeType.maxFractionPrecision) {
                throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
            }
        }

        Type typeObject = Type.getType(typeNumber, 0, length, scale);

        return typeObject;
    }

    /**
     * reads a Column or Function expression
     */
    private Expression readColumnOrFunctionExpression() throws HsqlException {

        String  name   = tokenString;
        boolean quoted = isQuoted;
        String  table  = namePrefix;
        String  schema = namePrePrefix;

        read();

        if (tokenType != Token.OPENBRACKET) {
            return new Expression(schema, table, name, quoted);
        }

        String   javaName = database.aliasManager.getJavaName(name);
        Function f        = new Function(name, javaName);

        compileContext.routineExpressions.add(f.getFullyQualifiedJavaName());

        int i = 0;

        read();

        if (tokenType != Token.CLOSEBRACKET) {
            while (true) {
                f.setArgument(i++, readOr());

                if (tokenType != Token.COMMA) {
                    break;
                }

                read();
            }
        }

        readThis(Token.CLOSEBRACKET);

        return f;
    }

    /**
     * Reads a NULLIF expression
     */
    private Expression readNullIfExpression() throws HsqlException {

        // turn into a CASEWHEN
        read();
        readThis(Token.OPENBRACKET);

        Expression c = readOr();

        readThis(Token.COMMA);

        Expression thenelse =
            new Expression(Expression.ALTERNATIVE,
                           new Expression((Object) null, (Type) null), c);

        c = new Expression(c, readOr());
        c = new Expression(Expression.CASEWHEN, c, thenelse);

        readThis(Token.CLOSEBRACKET);

        return c;
    }

    /**
     * Reads a COALESE or IFNULL expression
     */
    private Expression readCoalesceExpression() throws HsqlException {

        Expression c = null;

        // turn into a CASEWHEN
        read();
        readThis(Token.OPENBRACKET);

        Expression leaf = null;

        while (true) {
            Expression current = readOr();

            if (leaf != null && tokenType == Token.CLOSEBRACKET) {
                readThis(Token.CLOSEBRACKET);
                leaf.setLeftExpression(current);

                break;
            }

            Expression condition = new Expression(Expression.IS_NULL,
                                                  current);
            Expression alternatives = new Expression(Expression.ALTERNATIVE,
                new Expression((Object) null, (Type) null), current);
            Expression casewhen = new Expression(Expression.CASEWHEN,
                                                 condition, alternatives);

            if (c == null) {
                c = casewhen;
            } else {
                leaf.setLeftExpression(casewhen);
            }

            leaf = alternatives;

            readThis(Token.COMMA);
        }

        return c;
    }

    Expression readSQLFunction(String token) throws HsqlException {

        SQLFunction function = SQLFunction.newSQLFunction(token);

        return readSQLFunction(function);
    }

    Expression readSQLFunction(SQLFunction function) throws HsqlException {

        read();

        short[] parseList = function.parseList;

        if (parseList.length == 0) {
            return function;
        }

        HsqlArrayList exprList = new HsqlArrayList();

        readExpression(exprList, parseList, 0, parseList.length, false);

        Expression[] expr = new Expression[exprList.size()];

        exprList.toArray(expr);
        function.setArguments(expr);

        return function;
    }

    void readExpression(HsqlArrayList exprList, short[] parseList, int start,
                        int count, boolean isOption) throws HsqlException {

        for (int i = start; i < start + count; i++) {
            int exprType = parseList[i];

            switch (exprType) {

                case Token.QUESTION : {
                    Expression e = null;

                    try {
                        e = readOr();
                    } catch (HsqlException ex) {
                        if (!isOption) {
                            throw ex;
                        }
                    }

                    exprList.add(e);

                    continue;
                }
                case Token.X_OPTION : {
                    i++;

                    int expressionCount  = exprList.size();
                    int position         = getPosition();
                    int elementCount     = parseList[i++];
                    int initialExprIndex = exprList.size();

                    try {
                        readExpression(exprList, parseList, i, elementCount,
                                       true);
                    } catch (HsqlException e) {
                        rewind(position);
                        exprList.setSize(expressionCount);

                        for (int j = i; j < i + elementCount; j++) {
                            if (parseList[j] == Token.QUESTION
                                    || parseList[j] == Token.X_KEYSET) {
                                exprList.add(null);
                            }
                        }

                        i += elementCount - 1;

                        continue;
                    }

                    if (initialExprIndex == exprList.size()) {
                        exprList.add(null);
                    }

                    i += elementCount - 1;

                    continue;
                }
                case Token.X_REPEAT : {
                    i++;

                    int elementCount = parseList[i++];
                    int parseIndex   = i;

                    while (true) {
                        int initialExprIndex = exprList.size();

                        readExpression(exprList, parseList, parseIndex,
                                       elementCount, true);

                        if (exprList.size() == initialExprIndex) {
                            break;
                        }
                    }

                    i += elementCount - 1;

                    continue;
                }
                case Token.X_KEYSET : {
                    int        elementCount = parseList[++i];
                    Expression e            = null;

                    if (ArrayUtil.find(parseList, tokenType, i
                                       + 1, elementCount) == -1) {
                        if (!isOption) {
                            throw unexpectedToken();
                        }
                    } else {
                        e = new Expression(ValuePool.getInt(tokenType),
                                           Type.SQL_INTEGER);

                        read();
                    }

                    exprList.add(e);

                    i += elementCount;

                    continue;
                }
                case Token.OPENBRACKET :
                case Token.CLOSEBRACKET :
                case Token.COMMA :
                default :
                    if (tokenType != exprType) {
                        throw unexpectedToken();
                    }

                    read();

                    continue;
            }
        }
    }

    Expression readOption() {
        return null;
    }

    Expression readChoice() {
        return null;
    }

    private Expression readSequenceExpression() throws HsqlException {

        readThis(Token.VALUE);
        readThis(Token.FOR);
        checkIsNameOrKeyword();

        String schema = session.getSchemaName(namePrefix);

        NumberSequence sequence = database.schemaManager.getSequence(tokenString,
            schema);

        read();

        Expression e = new Expression(sequence);

        compileContext.sequenceExpressions.add(sequence);

        return e;
    }

    /**
     * Sets the subqueries as belonging to the View being constructed
     */
    void setAsView(View view) {

        for (int i = 0; i < compileContext.subQueryList.size(); i++) {
            SubQuery sq = (SubQuery) compileContext.subQueryList.get(i);

            if (sq.parentView == null) {
                sq.parentView = view;
            }
        }
    }

    CompiledStatement compileStatement(HsqlName currentSchema)
    throws HsqlException {

        CompiledStatement cs;
        int               brackets = 0;

        read();

        switch (tokenType) {

            case Token.OPENBRACKET : {
                brackets = readOpenBrackets();

                readThis(Token.SELECT);
            }
            case Token.SELECT : {
                cs = compileSelectStatement(brackets);

                break;
            }
            case Token.INSERT : {
                cs = compileInsertStatement();

                break;
            }
            case Token.UPDATE : {
                cs = compileUpdateStatement();

                break;
            }
            case Token.MERGE : {
                cs = compileMergeStatement();

                break;
            }
            case Token.DELETE : {
                cs = compileDeleteStatement();

                break;
            }
            case Token.CALL : {
                cs = compileCallStatement();

                break;
            }
            default : {

                // DDL statements
                cs = new CompiledStatement(currentSchema);

                break;
            }
        }

        if (cs.type != CompiledStatement.DDL) {
            while (true) {
                read();

                if (tokenType == Token.SEMICOLON) {
                    continue;
                }

                if (tokenType == Token.X_ENDPARSE) {
                    break;
                }

                throw unexpectedToken();
            }
        }

        return cs;
    }

    /**
     * Retrieves a CALL-type CompiledStatement from this parse context.
     */
    CompiledStatement compileCallStatement() throws HsqlException {

        Expression expression = parseExpression();
        CompiledStatement cs = new CompiledStatement(session,
            session.currentSchema, expression, compileContext);

        return cs;
    }

    /**
     * Retrieves a DELETE-type CompiledStatement from this parse context.
     */
    CompiledStatement compileDeleteStatement() throws HsqlException {

        Expression    condition = null;
        RangeVariable rangeVar;

        read();
        readThis(Token.FROM);

        rangeVar = readSimpleRangeVariable(GrantConstants.DELETE);

        if (tokenType == Token.WHERE) {
            read();

            condition = readOr();

            if (condition.getDataType().type != Types.SQL_BOOLEAN) {
                throw Trace.error(Trace.NOT_A_CONDITION);
            }
        }

        CompiledStatement cs = new CompiledStatement(session,
            session.currentSchema, rangeVar, condition, compileContext);

        return cs;
    }

    private void checkCatalogName() throws HsqlException {

        if (namePrePrefix != null
                &&!namePrePrefix.equals(database.getCatalog())) {

            // todo - SQL error catalog not found
            throw Trace.error(Trace.TOO_MANY_IDENTIFIER_PARTS);
        }
    }

    public void checkSchemaUpdateAuthorization(String schemaName)
    throws HsqlException {

        if (database.schemaManager.isSystemSchema(schemaName)) {
            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS);
        }

        session.getUser().checkSchemaUpdateOrGrantRights(schemaName);
    }

    public void checkSchemaGrantAuthorization(String schemaName)
    throws HsqlException {
        session.getUser().checkSchemaUpdateOrGrantRights(schemaName);
    }

    HsqlName readNewSchemaName() throws HsqlException {

        checkIsName();

        if (namePrePrefix != null) {
            throw Trace.error(Trace.TOO_MANY_IDENTIFIER_PARTS);
        }

        if (namePrefix != null
                &&!namePrePrefix.equals(database.getCatalog())) {
            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS,
                              namePrefix);
        }

        if (database.schemaManager.isSystemSchema(tokenString)) {
            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS,
                              namePrefix);
        }

        HsqlName name = database.nameManager.newHsqlName(tokenString,
            isQuoted);

        read();

        return name;
    }

    HsqlName readNewSchemaObjectName() throws HsqlException {

        if (namePrePrefix != null) {
            throw Trace.error(Trace.TOO_MANY_IDENTIFIER_PARTS);
        }

        checkIsName();

        HsqlName schemaName = session.getSchemaHsqlNameForWrite(namePrefix);

        checkSchemaUpdateAuthorization(schemaName.name);

        HsqlName hsqlName = database.nameManager.newHsqlName(tokenString,
            isQuoted);

        hsqlName.schema = schemaName;

        read();

        return hsqlName;
    }

    HsqlName readNewDependentSchemaObjectName() throws HsqlException {

        if (namePrePrefix != null) {
            throw Trace.error(Trace.TOO_MANY_IDENTIFIER_PARTS);
        }

        checkIsName();

        HsqlName schemaName = null;

        if (namePrefix != null) {
            schemaName = session.getSchemaHsqlNameForWrite(namePrefix);
        }

        HsqlName hsqlName = database.nameManager.newHsqlName(tokenString,
            isQuoted);

        hsqlName.schema = schemaName;

        read();

        return hsqlName;
    }

    HsqlName readNewDependentSchemaObjectName(HsqlName schemaName)
    throws HsqlException {

        HsqlName name = readNewDependentSchemaObjectName();

        name.setAndCheckSchema(schemaName);

        return name;
    }

    HsqlName readSchemaName() throws HsqlException {

        checkIsName();

        if (namePrePrefix != null) {
            throw Trace.error(Trace.TOO_MANY_IDENTIFIER_PARTS);
        }

        if (namePrefix != null
                &&!namePrePrefix.equals(database.getCatalog())) {
            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS,
                              namePrefix);
        }

        HsqlName schema = session.getSchemaHsqlName(tokenString);

        read();

        return schema;
    }

    Table readTableName() throws HsqlException {

        checkIsName();

        if (namePrePrefix != null) {
            throw Trace.error(Trace.TOO_MANY_IDENTIFIER_PARTS);
        }

        Table table = database.schemaManager.getTable(session, tokenString,
            namePrefix);

        read();

        return table;
    }

    Table readTableName(String schema) throws HsqlException {

        checkIsName();

        if (namePrePrefix != null) {
            throw Trace.error(Trace.TOO_MANY_IDENTIFIER_PARTS);
        }

        if (namePrefix != null &&!namePrefix.equals(schema)) {
            throw Trace.error(Trace.INVALID_SCHEMA_NAME_NO_SUBCLASS,
                              namePrefix);
        }

        Table table = database.schemaManager.getTable(session, tokenString,
            schema);

        read();

        return table;
    }

    Column readColumnName(Table table) throws HsqlException {

        int i = table.getColumnIndex(tokenString);

        if (namePrefix != null && namePrefix != table.getName().name) {
            throw unexpectedToken();
        }

        if (namePrePrefix != null
                && namePrePrefix != table.getName().schema.name) {
            throw unexpectedToken();
        }

        read();

        return table.getColumn(i);
    }

    Column readColumnName(RangeVariable rangeVar) throws HsqlException {

        int i = rangeVar.findColumn(tokenString);

        if (i == -1 ||!rangeVar.resolvesTableName(namePrefix)
                ||!rangeVar.resolvesSchemaName(namePrePrefix)) {
            throw Trace.error(Trace.COLUMN_NOT_FOUND, tokenString);
        }

        read();

        return rangeVar.getTable().getColumn(i);
    }

    /**
     * Retrieves an INSERT_XXX-type CompiledStatement from this parse context.
     *
     * todo: DEFAULT VALUES expression instead of source
     */
    CompiledStatement compileInsertStatement() throws HsqlException {

        read();
        readThis(Token.INTO);

        boolean[] columnCheckList;
        int[]     columnMap;
        int       colCount;
        Table     table = readTableName();

        columnCheckList = null;
        columnMap       = table.getColumnMap();
        colCount        = table.getColumnCount();

        if (tokenType == Token.DEFAULT) {
            read();
            readThis(Token.VALUES);

            Expression e = new Expression(Expression.ROW, new Expression[]{});

            e = new Expression(Expression.TABLE, new Expression[]{ e });
            columnCheckList = table.getNewColumnCheckList();

            for (int i = 0; i < table.colDefaults.length; i++) {
                if (table.colDefaults[i] == null
                        && table.identityColumn != i) {

                    // todo - SQL error
                    throw Trace.error(Trace.WRONG_DEFAULT_CLAUSE);
                }
            }

            CompiledStatement cs = new CompiledStatement(session,
                session.currentSchema, table, columnMap, e, columnCheckList,
                compileContext);

            return cs;
        }

        int brackets = readOpenBrackets();

        if (brackets == 1 && tokenType != Token.SELECT) {
            RangeVariable range = new RangeVariable(table, null, null,
                compileContext);
            OrderedHashSet columnNames = new OrderedHashSet();

            readColumnNames(columnNames, range);
            readThis(Token.CLOSEBRACKET);

            brackets        = readOpenBrackets();
            colCount        = columnNames.size();
            columnCheckList = table.getNewColumnCheckList();
            columnMap       = table.getColumnIndexes(columnNames);
            columnCheckList = table.getColumnCheckList(columnMap);
        }

        switch (tokenType) {

            case Token.VALUES : {
                read();

                Expression e = readFormattedRowOrTable(colCount);

                e.resolveTypes(session, null);

                Type[] tableColumnTypes = table.getColumnTypes();

                setParameterTypes(e, tableColumnTypes, columnMap);

                CompiledStatement cs = new CompiledStatement(session,
                    session.currentSchema, table, columnMap, e,
                    columnCheckList, compileContext);

                return cs;
            }
            case Token.SELECT : {

                // accept ORDER BY or ORDRY BY with LIMIT, or just LIMIT
                Select select = this.readQueryExpression(brackets, true,
                    true, false, false);

                select.checkColumnsResolved();

                if (colCount != select.visibleColumnCount) {
                    throw Trace.error(Trace.COLUMN_COUNT_DOES_NOT_MATCH);
                }

                CompiledStatement cs = new CompiledStatement(session,
                    session.currentSchema, table, columnMap, columnCheckList,
                    select, compileContext);

                return cs;
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

    private void setParameterTypes(Expression tableExpression,
                                   Type[] tableColumnTypes, int[] columnMap) {

        for (int i = 0; i < tableExpression.argList.length; i++) {
            Expression[] list = tableExpression.argList[i].argList;

            for (int j = 0; j < list.length; j++) {
                if (list[j].isParam()) {
                    list[j].dataType = tableColumnTypes[columnMap[j]];
                }
            }
        }
    }

    /**
     * Retrieves a SELECT-type CompiledStatement from this parse context.
     */
    CompiledStatement compileSelectStatement(int brackets)
    throws HsqlException {

        Select select = readQueryExpression(brackets, true, true, false,
                                            true);

        if (select.intoTableName != null) {
            String name   = select.intoTableName.name;
            String schema = select.intoTableName.schema.name;

            if (database.schemaManager.findUserTable(session, name, schema)
                    != null) {
                throw Trace.error(Trace.TABLE_ALREADY_EXISTS, name);
            }
        }

        CompiledStatement cs = new CompiledStatement(session,
            session.currentSchema, select, compileContext);

        return cs;
    }

    /**
     * Retrieves an UPDATE-type CompiledStatement from this parse context.
     */
    CompiledStatement compileUpdateStatement() throws HsqlException {

        read();

        RangeVariable rangeVar =
            readSimpleRangeVariable(GrantConstants.UPDATE);
        Table          table = rangeVar.rangeTable;
        Expression[]   expressions;
        int[]          colMap;
        OrderedHashSet colNames = new OrderedHashSet();
        HsqlArrayList  exprList = new HsqlArrayList();

        readThis(Token.SET);
        readSetClauseList(rangeVar, colNames, exprList);

        colMap      = table.getColumnIndexes(colNames);
        expressions = new Expression[exprList.size()];

        exprList.toArray(expressions);

        Expression condition = null;

        if (tokenType == Token.WHERE) {
            read();

            condition = readOr();

            if (condition.getDataType().type != Types.SQL_BOOLEAN) {
                throw Trace.error(Trace.NOT_A_CONDITION);
            }
        }

        CompiledStatement cs = new CompiledStatement(session,
            session.currentSchema, rangeVar, colMap, expressions, condition,
            compileContext);

        return cs;
    }

    private void readSetClauseList(RangeVariable rangeVar,
                                   OrderedHashSet colNames,
                                   HsqlArrayList expressions)
                                   throws HsqlException {

        while (true) {
            int degree;

            if (tokenType == Token.OPENBRACKET) {
                read();

                int oldCount = colNames.size();

                readColumnNames(colNames, rangeVar);

                degree = colNames.size() - oldCount;

                readThis(Token.CLOSEBRACKET);
            } else {
                Column column = readColumnName(rangeVar);

                if (!colNames.add(column.getName().name)) {
                    throw Trace.error(Trace.COLUMN_ALREADY_EXISTS,
                                      column.getName().name);
                }

                degree = 1;
            }

            readThis(Token.EQUALS);

            int brackets = readOpenBrackets();

            if (brackets > 0) {
                if (tokenType == Token.SELECT) {
                    SubQuery sq = parseSubquery(brackets - 1, null, false,
                                                Expression.ROW_SUBQUERY);

                    readThis(Token.CLOSEBRACKET);

                    if (degree != sq.select.visibleColumnCount) {
                        throw Trace.error(Trace.COLUMN_COUNT_DOES_NOT_MATCH);
                    }

                    Expression e = new Expression(Expression.ROW_SUBQUERY,
                                                  sq);

                    expressions.add(e);
                } else {
                    Expression e = readRow();

                    readThis(Token.CLOSEBRACKET);

                    int rowDegree = e.exprType == Expression.ROW
                                    ? e.argList.length
                                    : 1;

                    if (degree != rowDegree) {
                        throw Trace.error(Trace.COLUMN_COUNT_DOES_NOT_MATCH);
                    }

                    expressions.add(e);
                }
            } else {
                Expression e = readOr();

                expressions.add(e);
            }

            if (tokenType == Token.COMMA) {
                read();

                continue;
            }

            break;
        }
    }

    /**
     * Retrieves a MERGE-type CompiledStatement from this parse context.
     */
    CompiledStatement compileMergeStatement() throws HsqlException {

        boolean[]     insertColumnCheckList;
        int[]         insertColumnMap = null;
        int[]         updateColumnMap = null;
        Table         targetTable;
        RangeVariable targetRange;
        RangeVariable sourceRange;
        Expression    onCondition;
        HsqlArrayList updateList       = new HsqlArrayList();
        Expression[]  updateValues     = null;
        HsqlArrayList insertList       = new HsqlArrayList();
        Expression    insertExpression = null;

        read();
        readThis(Token.INTO);

        targetRange = readSimpleRangeVariable(GrantConstants.UPDATE);
        targetTable = targetRange.rangeTable;

        readThis(Token.USING);

        sourceRange = this.readTableOrSubquery();

        // parse ON search conditions
        readThis(Token.ON);

        onCondition = readOr();

        Type type = onCondition.getDataType();

        if (type == null ||!type.isBooleanType()) {
            throw Trace.error(Trace.NOT_A_CONDITION);
        }

        // parse WHEN clause(s) and convert lists to arrays
        insertColumnMap       = targetTable.getColumnMap();
        insertColumnCheckList = targetTable.getNewColumnCheckList();

        OrderedHashSet updateColNames = new OrderedHashSet();
        OrderedHashSet insertColNames = new OrderedHashSet();

        readMergeWhen(insertColNames, updateColNames, insertList, updateList,
                      targetRange, sourceRange, insertColumnCheckList);

        if (insertList.size() > 0) {
            int colCount = insertColNames.size();

            if (colCount != 0) {
                insertColumnMap =
                    targetTable.getColumnIndexes(insertColNames);
                insertColumnCheckList =
                    targetTable.getColumnCheckList(insertColumnMap);
            }

            Type[] tableColumnTypes = targetTable.getColumnTypes();

            insertExpression = (Expression) insertList.get(0);

            setParameterTypes(insertExpression, tableColumnTypes,
                              insertColumnMap);
        }

        if (updateList.size() > 0) {
            updateValues = new Expression[updateList.size()];

            updateList.toArray(updateValues);

            updateColumnMap = targetTable.getColumnIndexes(updateColNames);
        }

        CompiledStatement cs = new CompiledStatement(session,
            session.currentSchema, targetRange, sourceRange, insertColumnMap,
            updateColumnMap, insertColumnCheckList, onCondition,
            insertExpression, updateValues, compileContext);

        return cs;
    }

    /**
     * Parses a WHEN clause from a MERGE statement. This can be either a
     * WHEN MATCHED or WHEN NOT MATCHED clause, or both, and the appropriate
     * values will be updated.
     *
     * If the var that is to hold the data is not null, then we already
     * encountered this type of clause, which is only allowed once, and at least
     * one is required.
     */
    private void readMergeWhen(OrderedHashSet insertColumnNames,
                               OrderedHashSet updateColumnNames,
                               HsqlArrayList insertExpressions,
                               HsqlArrayList updateExpressions,
                               RangeVariable targetRangeVar,
                               RangeVariable sourceRangeVar,
                               boolean[] insertColumnCheckList)
                               throws HsqlException {

        Table table       = targetRangeVar.rangeTable;
        int   columnCount = table.getColumnCount();

        readThis(Token.WHEN);

        if (tokenType == Token.MATCHED) {
            if (updateExpressions != null && updateExpressions.size() != 0) {
                throw Trace.error(Trace.MERGE_WHEN_MATCHED_ALREADY_USED);
            }

            read();
            readThis(Token.THEN);
            readThis(Token.UPDATE);
            readThis(Token.SET);
            readSetClauseList(targetRangeVar, updateColumnNames,
                              updateExpressions);
        } else if (tokenType == Token.NOT) {
            if (insertExpressions != null && insertExpressions.size() != 0) {
                throw Trace.error(Trace.MERGE_WHEN_NOT_MATCHED_ALREADY_USED);
            }

            read();
            readThis(Token.MATCHED);
            readThis(Token.THEN);
            readThis(Token.INSERT);

            // parse INSERT statement
            // optional column list
            int brackets = readOpenBrackets();

            if (brackets == 1) {
                readColumnNames(insertColumnNames, targetRangeVar);
                readThis(Token.CLOSEBRACKET);

                brackets = 0;
            }

            readThis(Token.VALUES);

            Expression e = readFormattedRowOrTable(columnCount);

            if (e.argList.length != 1) {
                throw Trace.error(Trace.CARDINALITY_VIOLATION_NO_SUBCLASS);
            }

            insertExpressions.add(e);
        } else {
            throw unexpectedToken();
        }

        if (tokenType == Token.WHEN) {
            readMergeWhen(insertColumnNames, updateColumnNames,
                          insertExpressions, updateExpressions,
                          targetRangeVar, sourceRangeVar,
                          insertColumnCheckList);
        }
    }

    int readCloseBrackets(int limit) throws HsqlException {

        int count = 0;

        while (count < limit && tokenType == Token.CLOSEBRACKET) {
            read();

            count++;
        }

        return count;
    }

    int readOpenBrackets() throws HsqlException {

        int count = 0;

        while (tokenType == Token.OPENBRACKET) {
            count++;

            read();
        }

        return count;
    }

    /**
     * parseViewSubquery
     *
     * @param view View
     */
    public SubQuery parseViewSubquery(View view) throws HsqlException {

        read();

        int brackets = readOpenBrackets();
        SubQuery subQuery = parseSubquery(brackets, view, true,
                                          Expression.VIEW);

        setAsView(view);

        return subQuery;
    }

    public static class CompileContext {

        private static final Expression[] noParameters = new Expression[0];
        private static final SubQuery[]   noSubqueries = new SubQuery[0];

        //
        private int           subQueryLevel;
        private HsqlArrayList subQueryList  = new HsqlArrayList();
        private HsqlArrayList aggregateList = new HsqlArrayList();
        private int           aggregateIndex;
        HsqlArrayList         parameters          = new HsqlArrayList();
        OrderedHashSet        sequenceExpressions = new OrderedHashSet();
        OrderedHashSet        routineExpressions  = new OrderedHashSet();
        HsqlArrayList         rangeVariables      = new HsqlArrayList();

        //
        private int rangeVarIndex;

        public void registerRangeVariable(RangeVariable range) {

            range.index = getNextRangeVarIndex();

            rangeVariables.add(range);
        }

        public int getNextRangeVarIndex() {
            return rangeVarIndex++;
        }

        public int getRangeVarCount() {
            return rangeVarIndex;
        }

        /**
         * Return the list of subqueries as an array sorted according to the order
         * of materialization, then clear the internal subquery list
         */
        SubQuery[] getSortedSubqueries(Session session) throws HsqlException {

            if (subQueryList.size() == 0) {
                return noSubqueries;
            }

            subQueryList.sort((SubQuery) subQueryList.get(0));

            SubQuery[] subqueries = new SubQuery[subQueryList.size()];

            subQueryList.toArray(subqueries);
            subQueryList.clear();

            for (int i = 0; i < subqueries.length; i++) {
                Select select = subqueries[i].select;

                if (select != null) {
                    select.resolveTypesAndPrepare(session);
                }
            }

            return subqueries;
        }

        Expression[] getParameters() {

            if (parameters.size() == 0) {
                return noParameters;
            }

            Expression[] result = (Expression[]) parameters.toArray(
                new Expression[parameters.size()]);

            parameters.clear();

            return result;
        }

        void clearParameters() {
            parameters.clear();
        }

        public void reset() {

            rangeVarIndex = 0;

            subQueryList.clear();
            aggregateList.clear();

            subQueryLevel  = 0;
            aggregateIndex = 0;

            parameters.clear();

            sequenceExpressions = new OrderedHashSet();
            routineExpressions  = new OrderedHashSet();
            rangeVariables      = new HsqlArrayList();
        }
    }
}
