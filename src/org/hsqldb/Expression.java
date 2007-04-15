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
import org.hsqldb.Parser.CompileContext;
import org.hsqldb.index.Index;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.OrderedIntHashSet;
import org.hsqldb.lib.Set;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.result.Result;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.CharacterType;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.NullType;
import org.hsqldb.types.NumberType;
import org.hsqldb.types.Type;

// fredt@users 20020215 - patch 1.7.0 by fredt
// to preserve column size etc. when SELECT INTO TABLE is used
// tony_lai@users 20021020 - patch 1.7.2 - improved aggregates and HAVING
// fredt@users 20021112 - patch 1.7.2 by Nitin Chauhan - use of switch
// rewrite of the majority of multiple if(){}else{} chains with switch(){}
// vorburger@users 20021229 - patch 1.7.2 - null handling
// boucherb@users 200307?? - patch 1.7.2 - resolve param nodes
// boucherb@users 200307?? - patch 1.7.2 - compress constant expr during resolve
// boucherb@users 200307?? - patch 1.7.2 - eager pmd and rsmd
// boucherb@users 20031005 - patch 1.7.2 - optimised LIKE
// boucherb@users 20031005 - patch 1.7.2 - improved IN value lists
// fredt@users 20031012 - patch 1.7.2 - better OUTER JOIN implementation
// thomasm@users 20041001 - patch 1.7.3 - BOOLEAN undefined handling
// fredt@users 200412xx - patch 1.7.2 - evaluation of time functions
// boucherb@users 20050516 - patch 1.8.0 - remove DITypeInfo usage for faster
//                                         statement compilation
// fredt@users 2006 - 1.9.0 - restructuring, constructors rewritten and several methods made redundant and removed

/**
 * Expression class.
 *
 * The core functionality of this class was inherited from HypersonicSQL and
 * extensively rewritten and extended in successive versions of HSQLDB.
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version    1.8.0
 * @since Hypersonic SQL
 */

/** @todo - fredt - constant TRUE and FALSE type expressions have valueData of
  * type BOOLEAN, while computed expressions have no valueData; this should be
  * normalised in future
  */
public class Expression {

    // leaf types
    public static final int VALUE           = 1,
                            COLUMN          = 2,
                            DEFAULT         = 4,
                            ALTERNATIVE     = 5;
    public static final int ORDER_BY        = 10,
                            LIMIT           = 11,
                            SEQUENCE        = 12,
                            ASTERISK        = 13;
    static final int        SCALAR_SUBQUERY = 21,
                            ROW_SUBQUERY    = 22,
                            TABLE_SUBQUERY  = 23,
                            ROW             = 25,
                            TABLE           = 26,
                            FUNCTION        = 27,
                            SQL_FUNCTION    = 28;

// --
    // operations
    public static final int NEGATE   = 31,
                            ADD      = 32,
                            SUBTRACT = 33,
                            MULTIPLY = 34,
                            DIVIDE   = 35,
                            CONCAT   = 36;

    // logical operations
    public static final int NOT                  = 41,
                            EQUAL                = 42,
                            GREATER_EQUAL        = 43,
                            GREATER              = 44,
                            SMALLER              = 45,
                            SMALLER_EQUAL        = 46,
                            NOT_EQUAL            = 47,
                            IS_NULL              = 48,
                            AND                  = 49,
                            OR                   = 50,
                            LIKE                 = 51,
                            IN                   = 52,
                            EXISTS               = 53,
                            ALL                  = 54,
                            ANY                  = 55,
                            OVERLAPS             = 56,
                            UNIQUE               = 57,
                            NOT_DISTINCT         = 58,
                            MATCH_SIMPLE         = 61,
                            MATCH_PARTIAL        = 62,
                            MATCH_FULL           = 63,
                            MATCH_UNIQUE_SIMPLE  = 64,
                            MATCH_UNIQUE_PARTIAL = 65,
                            MATCH_UNIQUE_FULL    = 66;

    // aggregate functions
    static final int COUNT       = 71,
                     SUM         = 72,
                     MIN         = 73,
                     MAX         = 74,
                     AVG         = 75,
                     EVERY       = 76,
                     SOME        = 77,
                     STDDEV_POP  = 78,
                     STDDEV_SAMP = 79,
                     VAR_POP     = 80,
                     VAR_SAMP    = 81;

    // system functions
    static final int CONVERT  = 91,
                     CASEWHEN = 92;

    // temporary used during parsing
    static final int VIEW = 93;

    //
    static final int AGGREGATE_SELF  = -1;
    static final int AGGREGATE_NONE  = 0;
    static final int AGGREGATE_LEFT  = 1;
    static final int AGGREGATE_RIGHT = 2;
    static final int AGGREGATE_BOTH  = 3;

    //
    static final Integer INTEGER_0 = ValuePool.getInt(0);
    static final Integer INTEGER_1 = ValuePool.getInt(1);

    //
    static Expression EXPR_TRUE  = new Expression(true);
    static Expression EXPR_FALSE = new Expression(false);

    //
    static OrderedIntHashSet aggregateFunctionSet = new OrderedIntHashSet();

    static {
        aggregateFunctionSet.add(COUNT);
        aggregateFunctionSet.add(SUM);
        aggregateFunctionSet.add(MIN);
        aggregateFunctionSet.add(MAX);
        aggregateFunctionSet.add(AVG);
        aggregateFunctionSet.add(EVERY);
        aggregateFunctionSet.add(SOME);
        aggregateFunctionSet.add(STDDEV_POP);
        aggregateFunctionSet.add(STDDEV_SAMP);
        aggregateFunctionSet.add(VAR_POP);
        aggregateFunctionSet.add(VAR_SAMP);
    }

    static OrderedIntHashSet columnExpressionSet = new OrderedIntHashSet();

    static {
        columnExpressionSet.add(COLUMN);
    }

    static OrderedIntHashSet subqueryExpressionSet = new OrderedIntHashSet();

    static {
        subqueryExpressionSet.add(TABLE_SUBQUERY);
    }

    static OrderedIntHashSet subqueryAggregateExpressionSet =
        new OrderedIntHashSet();

    static {
        subqueryAggregateExpressionSet.add(COUNT);
        subqueryAggregateExpressionSet.add(SUM);
        subqueryAggregateExpressionSet.add(MIN);
        subqueryAggregateExpressionSet.add(MAX);
        subqueryAggregateExpressionSet.add(AVG);
        subqueryAggregateExpressionSet.add(EVERY);
        subqueryAggregateExpressionSet.add(SOME);
        subqueryAggregateExpressionSet.add(STDDEV_POP);
        subqueryAggregateExpressionSet.add(STDDEV_SAMP);
        subqueryAggregateExpressionSet.add(VAR_POP);
        subqueryAggregateExpressionSet.add(VAR_SAMP);

        //
        subqueryAggregateExpressionSet.add(TABLE_SUBQUERY);
    }

    static OrderedIntHashSet emptyExpressionSet = new OrderedIntHashSet();

    // type
    protected int exprType;

    // aggregate type
    protected int aggregateSpec = AGGREGATE_NONE;

    // nodes
    protected Expression eArg, eArg2;

    // VALUE
    protected Object valueData;

    // VALUE LIST
    protected Expression[] argList;
    private boolean        isConstantValueList;
    boolean                isCorrelated;           // also for query query
    Type[]                 argListDataType;
    Type[]                 argListOpDataType;

    // QUERY - in single value selects, IN, EXISTS etc.
    SubQuery subQuery;

    // LIKE
    private Like likeObject;

    // COLUMN
    Column          column;
    String          schema;
    String          tableName;
    String          columnName;
    private int     columnIndex;
    private boolean columnQuoted;
    RangeVariable   rangeVariable;                 // null if not yet resolved
    String          columnAlias;                   // if it is a column of a select column list
    boolean         aliasQuoted;

    // data type
    protected Type dataType;
    protected Type opDataType;

    //
    private boolean isDescending;                  // if it is a column in a order by
    int             queryTableColumnIndex = -1;    // >= 0 when it is used for order by
    boolean         isDistinctAggregate;
    private boolean isParam;

    // index of a session-dependent field
    int parameterIndex;

    //
    boolean isColumnEqual;
    boolean isIndexed;

    Expression(int type) {
        exprType = type;
    }

    /**
     * Creates a boolean expression
     */
    Expression(boolean b) {

        exprType  = VALUE;
        dataType  = Type.SQL_BOOLEAN;
        valueData = b ? Boolean.TRUE
                      : Boolean.FALSE;
    }

    /**
     * Creates a SEQUENCE expression
     */
    Expression(NumberSequence sequence) {

        exprType  = SEQUENCE;
        valueData = sequence;
        dataType  = sequence.getType();
    }

    // IN condition optimisation

    /**
     * Create a equality expressions using existing columns and
     * range variables. The expression is fully resolved in constructor.
     */
    Expression(RangeVariable leftRangeVar, Column left,
               RangeVariable rightRangeVar, Column right) {

        exprType            = EQUAL;
        dataType            = Type.SQL_BOOLEAN;
        eArg                = new Expression(leftRangeVar, left);
        eArg.rangeVariable  = leftRangeVar;
        eArg2               = new Expression(rightRangeVar, right);
        eArg2.rangeVariable = rightRangeVar;
        eArg.dataType       = left.getType();
        eArg2.dataType      = right.getType();
    }

    /**
     * Creates a SCALAR SUBQUERY expression. Is called as ROW_SUBQUERY
     */
    Expression(int exprType, SubQuery sq) {
        this.exprType = TABLE_SUBQUERY;
        subQuery      = sq;
    }

    /**
     * ROW or VALUELIST
     */
    Expression(int type, Expression[] list) {
        exprType     = type;
        this.argList = list;
    }

    /**
     * Creates an equality expression
     */
    Expression(Expression e, Expression e2) {

        exprType = EQUAL;
        eArg     = e;
        eArg2    = e2;

        if (eArg.exprType == COLUMN && eArg2.exprType == COLUMN) {
            isColumnEqual = true;
        } else {
            setAggregateSpec();
        }

        dataType = Type.SQL_BOOLEAN;
    }

    /**
     * Creates a binary operation expression
     */
    Expression(int type, Expression e, Expression e2) {

        exprType = type;
        eArg     = e;
        eArg2    = e2;

        setAggregateSpec();

        switch (exprType) {

            case EQUAL :
                if (eArg.exprType == COLUMN && eArg2.exprType == COLUMN) {
                    isColumnEqual = true;
                }
            case GREATER_EQUAL :
            case GREATER :
            case SMALLER :
            case SMALLER_EQUAL :
            case NOT_EQUAL :
            case OVERLAPS :
            case NOT_DISTINCT :

            // temporary until RangeVariable code is fixed - then use unary constructor
            case IS_NULL :
            case IN :
            case MATCH_SIMPLE :
            case MATCH_PARTIAL :
            case MATCH_FULL :
            case MATCH_UNIQUE_SIMPLE :
            case MATCH_UNIQUE_PARTIAL :
            case MATCH_UNIQUE_FULL :
                dataType = Type.SQL_BOOLEAN;

                return;

            case AND :
            case OR :
                dataType = Type.SQL_BOOLEAN;
                break;

            case ADD :
            case SUBTRACT :
            case MULTIPLY :
            case DIVIDE :
                return;

            case CONCAT :
            case ALTERNATIVE :
            case CASEWHEN :
            case LIMIT :
                return;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Expression");
        }
    }

    /**
     * Creates a unary operation expression
     */
    Expression(int type, Expression e) {

        exprType = type;
        eArg     = e;

        setAggregateSpec();

        switch (exprType) {

            case UNIQUE :
            case EXISTS :
            case IS_NULL :
            case NOT :
                dataType = Type.SQL_BOOLEAN;
                break;

            case ORDER_BY :
            case NEGATE :
                break;

            case ANY :
            case ALL :
                break;

            default :
                Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                   "Expression");
        }
    }

    /**
     * creates a CONVERT expression
     */
    Expression(Expression e, Type dataType) {

        this.exprType    = CONVERT;
        this.eArg        = e;
        this.dataType    = dataType;
        this.columnAlias = e.columnAlias;
        this.aliasQuoted = e.aliasQuoted;

        setAggregateSpec();
    }

    /**
     * Creates a LIKE expression
     */
    Expression(Expression e, Expression e2, Character escape,
               boolean hasCollation) {

        dataType   = Type.SQL_BOOLEAN;
        exprType   = LIKE;
        eArg       = e;
        eArg2      = e2;
        likeObject = new Like(escape, hasCollation);

        setAggregateSpec();
    }

    /**
     * Creates an ASTERISK expression
     */
    Expression(String schema, String table) {

        exprType    = ASTERISK;
        this.schema = schema;
        tableName   = table;
    }

    /**
     * Creates a possibly quoted COLUMN expression
     */
    Expression(String schema, String table, String column, boolean isquoted) {

        exprType     = COLUMN;
        this.schema  = schema;
        tableName    = table;
        columnName   = column;
        columnQuoted = isquoted;
    }

    Expression(RangeVariable rangeVar, Column column) {

        exprType      = COLUMN;
        rangeVariable = rangeVar;
        columnIndex   = rangeVar.rangeTable.findColumn(column.getName().name);

        setAttributesAsColumn(rangeVar.rangeTable, columnIndex);

        rangeVar.usedColumns[columnIndex] = true;
    }

    /**
     * Creates a VALUE expression
     */
    Expression(Object o, Type datatype) {

        exprType  = VALUE;
        dataType  = datatype;
        valueData = o;
    }

    /**
     * Creates an aggregate expression
     */
    Expression(int type, boolean distinct, Expression e) {

        exprType            = type;
        isDistinctAggregate = distinct;
        eArg                = e;
        aggregateSpec       = AGGREGATE_SELF;
    }

    /**
     * Creates a (possibly PARAM) VALUE expression
     */
    Expression(Object o, Type dataType, boolean isParam) {

        this(o, dataType);

        this.isParam = isParam;

        if (isParam) {
            paramMode = PARAM_IN;
        }
    }

    /**
     * Creates a column not null expression
     */
    Expression(Column column) {

        exprType = NOT;
        dataType = Type.SQL_BOOLEAN;

        Expression e = new Expression(Expression.COLUMN);

        e.columnName = column.getName().name;
        e            = new Expression(IS_NULL, e);
        eArg         = e;
    }

    public boolean isValidColumnDefaultExpression() {

        return exprType == VALUE
               || (exprType == SQL_FUNCTION
                   && ((SQLFunction) this).isValueFunction());
    }

    boolean hasSequence(NumberSequence sequence) {
        return exprType == Expression.SEQUENCE && valueData == sequence;
    }

    boolean isConstantCondition() {
        return dataType.type == Types.SQL_BOOLEAN && exprType == VALUE
               &&!isParam;
    }

    private void setAggregateSpec() {

        if (isAggregate(exprType)) {
            aggregateSpec = AGGREGATE_SELF;
        } else {
            aggregateSpec = AGGREGATE_NONE;

            if ((eArg != null) && eArg.isAggregate()) {
                aggregateSpec += AGGREGATE_LEFT;
            }

            if ((eArg2 != null) && eArg2.isAggregate()) {
                aggregateSpec += AGGREGATE_RIGHT;
            }
        }
    }

    public String describe(Session session) {
        return describe(session, 0);
    }

    static String getContextDDL(Expression expression) {

        String ddl = expression.getDDL();

        if (expression.exprType != VALUE && expression.exprType != COLUMN
                && expression.exprType != ROW
                && expression.exprType != FUNCTION
                && expression.exprType != SQL_FUNCTION
                && expression.exprType != ALTERNATIVE
                && expression.exprType != CASEWHEN
                && expression.exprType != CONVERT) {
            StringBuffer temp = new StringBuffer();

            ddl = temp.append('(').append(ddl).append(')').toString();
        }

        return ddl;
    }

    /**
     * For use with CHECK constraints. Under development.
     *
     * Currently supports a subset of expressions and is suitable for CHECK
     * search conditions that refer only to the inserted/updated row.
     *
     * For full DDL reporting of VIEW select queries and CHECK search
     * conditions, future improvements here are dependent upon improvements to
     * SELECT query parsing, so that it is performed in a number of passes.
     * An early pass should result in the query turned into an Expression tree
     * that contains the information in the original SQL without any
     * alterations, and with tables and columns all resolved. This Expression
     * can then be preserved for future use. Table and column names that
     * are not user-defined aliases should be kept as the HsqlName structures
     * so that table or column renaming is reflected in the precompiled
     * query.
     */
    public String getDDL() {

        StringBuffer buf   = new StringBuffer(64);
        String       left  = null;
        String       right = null;

        if (eArg != null) {
            left = Expression.getContextDDL(eArg);
        }

        if (eArg2 != null) {
            right = Expression.getContextDDL(eArg2);
        }

        switch (exprType) {

            case FUNCTION :
            case SQL_FUNCTION :

                // must go to Function
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Expression");
            case VALUE :
                if (isParam) {
                    return Token.T_QUESTION;
                }

                if (valueData == null) {
                    return Token.T_NULL;
                }

                if (dataType == null) {
                    throw Trace.runtimeError(
                        Trace.UNSUPPORTED_INTERNAL_OPERATION,
                        "Expression.getDDL()");
                }

                return dataType.convertToSQLString(valueData);

            case COLUMN :

                // this is a limited solution
                if (rangeVariable != null) {
                    Table table = rangeVariable.getTable();

                    if (tableName != null) {
                        buf.append(table.tableName.statementName);
                        buf.append('.');
                    }

                    buf.append(
                        table.getColumn(
                            columnIndex).columnName.statementName);
                } else {
                    buf.append(getAlias());
                }

                return buf.toString();

            case DEFAULT :
                return Token.T_DEFAULT;

            case ROW :
                buf.append('(');

                for (int i = 0; i < argList.length; i++) {
                    buf.append(argList[i].getDDL());

                    if (i < argList.length - 1) {
                        buf.append(',');
                    }
                }

                buf.append(')');

                return buf.toString();

            //
            case TABLE :
                for (int i = 0; i < argList.length; i++) {
                    buf.append(argList[i].getDDL());

                    if (i < argList.length - 1) {
                        buf.append(',');
                    }
                }

                return buf.toString();

            case ASTERISK :
                buf.append('*');

                return buf.toString();

            case ORDER_BY :
                buf.append(left);

                if (isDescending) {
                    buf.append(' ').append(Token.T_DESC);
                }

                return buf.toString();

            case NEGATE :
                buf.append('-').append(left);

                return buf.toString();

            case ADD :
                buf.append(left).append('+').append(right);

                return buf.toString();

            case SUBTRACT :
                buf.append(left).append('-').append(right);

                return buf.toString();

            case MULTIPLY :
                buf.append(left).append('*').append(right);

                return buf.toString();

            case DIVIDE :
                buf.append(left).append('/').append(right);

                return buf.toString();

            case CONCAT :
                buf.append(left).append("||").append(right);

                return buf.toString();

            case NOT :
                if (eArg.exprType == IS_NULL) {
                    buf.append(getContextDDL(eArg.eArg)).append(' ').append(
                        Token.T_IS).append(' ').append(Token.T_NOT).append(
                        ' ').append(Token.T_NULL);

                    return buf.toString();
                }

                if (eArg.exprType == NOT_DISTINCT) {
                    buf.append(getContextDDL(eArg.eArg)).append(' ').append(
                        Token.T_IS).append(' ').append(
                        Token.T_DISTINCT).append(' ').append(
                        Token.T_FROM).append(' ').append(
                        getContextDDL(eArg.eArg2));

                    return buf.toString();
                }

                buf.append(Token.T_NOT).append(' ').append(left);

                return buf.toString();

            case NOT_DISTINCT :
                buf.append(Token.T_NOT).append(' ').append(
                    getContextDDL(eArg.eArg)).append(' ').append(
                    Token.T_IS).append(' ').append(Token.T_DISTINCT).append(
                    ' ').append(Token.T_FROM).append(' ').append(
                    getContextDDL(eArg.eArg2));

                return buf.toString();

            case EQUAL :
                buf.append(left).append('=').append(right);

                return buf.toString();

            case GREATER_EQUAL :
                buf.append(left).append(">=").append(right);

                return buf.toString();

            case GREATER :
                buf.append(left).append('>').append(right);

                return buf.toString();

            case SMALLER :
                buf.append(left).append('<').append(right);

                return buf.toString();

            case SMALLER_EQUAL :
                buf.append(left).append("<=").append(right);

                return buf.toString();

            case NOT_EQUAL :
                if (Token.T_NULL.equals(right)) {
                    buf.append(left).append(" IS NOT ").append(right);
                } else {
                    buf.append(left).append("!=").append(right);
                }

                return buf.toString();

            case LIKE :
                buf.append(left).append(' ').append(Token.T_LIKE).append(' ');
                buf.append(right);

                /** @todo fredt - scripting of non-ascii escapes needs changes to general script logging */
                if (likeObject.escapeChar != null) {
                    buf.append(' ').append(Token.T_ESCAPE).append(' ').append(
                        '\'');
                    buf.append(likeObject.escapeChar.toString()).append('\'');
                    buf.append(' ');
                }

                return buf.toString();

            case AND :
                buf.append(left).append(' ').append(Token.T_AND).append(
                    ' ').append(right);

                return buf.toString();

            case OR :
                buf.append(left).append(' ').append(Token.T_OR).append(
                    ' ').append(right);

                return buf.toString();

            case ALL :
                buf.append(left).append(' ').append(Token.T_ALL).append(
                    ' ').append(right);

                return buf.toString();

            case ANY :
                buf.append(left).append(' ').append(Token.T_ANY).append(
                    ' ').append(right);

                return buf.toString();

            case IN :
                buf.append(left).append(' ').append(Token.T_IN).append(
                    ' ').append(right);

                return buf.toString();

            case MATCH_SIMPLE :
                buf.append(left).append(' ').append(Token.T_MATCH).append(
                    ' ').append(right);

                return buf.toString();

            case MATCH_PARTIAL :
                buf.append(left).append(' ').append(Token.T_MATCH).append(
                    ' ').append(Token.PARTIAL).append(right);

                return buf.toString();

            case MATCH_FULL :
                buf.append(left).append(' ').append(Token.T_MATCH).append(
                    ' ').append(Token.FULL).append(right);

                return buf.toString();

            case MATCH_UNIQUE_SIMPLE :
                buf.append(left).append(' ').append(Token.T_MATCH).append(
                    ' ').append(Token.UNIQUE).append(right);

                return buf.toString();

            case MATCH_UNIQUE_PARTIAL :
                buf.append(left).append(' ').append(Token.T_MATCH).append(
                    ' ').append(Token.UNIQUE).append(' ').append(
                    Token.PARTIAL).append(right);

                return buf.toString();

            case MATCH_UNIQUE_FULL :
                buf.append(left).append(' ').append(Token.T_MATCH).append(
                    ' ').append(Token.UNIQUE).append(' ').append(
                    Token.FULL).append(right);

                return buf.toString();

            case CONVERT :
                buf.append(' ').append(Token.T_CONVERT).append('(');
                buf.append(left).append(',');
                buf.append(dataType.getDefinition());
                buf.append(')');

                return buf.toString();

            case CASEWHEN :
                buf.append(' ').append(Token.T_CASEWHEN).append('(');
                buf.append(left).append(',').append(right).append(')');

                return buf.toString();

            case IS_NULL :
                buf.append(left).append(' ').append(Token.T_IS).append(
                    ' ').append(Token.T_NULL);

                return buf.toString();

            case ALTERNATIVE :
                buf.append(left).append(',').append(right);

                return buf.toString();

            case TABLE_SUBQUERY :
/*
                buf.append('(');
                buf.append(subSelect.getDDL());
                buf.append(')');
*/
                break;

            case UNIQUE :
                buf.append(' ').append(Token.T_UNIQUE).append(' ');
                break;

            case EXISTS :
                buf.append(' ').append(Token.T_EXISTS).append(' ');
                break;

            case COUNT :
                buf.append(' ').append(Token.T_COUNT).append('(');
                break;

            case SUM :
                buf.append(' ').append(Token.T_SUM).append('(');
                buf.append(left).append(')');
                break;

            case MIN :
                buf.append(' ').append(Token.T_MIN).append('(');
                buf.append(left).append(')');
                break;

            case MAX :
                buf.append(' ').append(Token.T_MAX).append('(');
                buf.append(left).append(')');
                break;

            case AVG :
                buf.append(' ').append(Token.T_AVG).append('(');
                buf.append(left).append(')');
                break;

            case EVERY :
                buf.append(' ').append(Token.T_EVERY).append('(');
                buf.append(left).append(')');
                break;

            case SOME :
                buf.append(' ').append(Token.T_SOME).append('(');
                buf.append(left).append(')');
                break;

            case STDDEV_POP :
                buf.append(' ').append(Token.T_STDDEV_POP).append('(');
                buf.append(left).append(')');
                break;

            case STDDEV_SAMP :
                buf.append(' ').append(Token.T_STDDEV_SAMP).append('(');
                buf.append(left).append(')');
                break;

            case VAR_POP :
                buf.append(' ').append(Token.T_VAR_POP).append('(');
                buf.append(left).append(')');
                break;

            case VAR_SAMP :
                buf.append(' ').append(Token.T_VAR_SAMP).append('(');
                buf.append(left).append(')');
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Expression");
        }

        return buf.toString();
    }

    private String describe(Session session, int blanks) {

        StringBuffer buf = new StringBuffer(64);

        buf.append('\n');

        for (int i = 0; i < blanks; i++) {
            buf.append(' ');
        }

        switch (exprType) {

            case FUNCTION :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Expression.describe()");
            case VALUE :
                if (isParam) {
                    buf.append("PARAM ");
                }

                buf.append("VALUE = ").append(valueData);
                buf.append(", TYPE = ").append(dataType.getName());

                return buf.toString();

            case COLUMN :
                buf.append("COLUMN ");

                if (tableName != null) {
                    buf.append(tableName);
                    buf.append('.');
                }

                buf.append(columnName);

                return buf.toString();

            case DEFAULT :
                return Token.T_DEFAULT;

            case TABLE_SUBQUERY :
                buf.append("QUERY ");
                buf.append(subQuery.select.describe(session));

                return buf.toString();

            case ROW :

            //
            case TABLE :
                buf.append("VALUELIST ");
                buf.append(" TYPE = ").append(dataType.getName());

                if (argList != null) {
                    for (int i = 0; i < argList.length; i++) {
                        buf.append(argList[i].describe(session,
                                                       blanks + blanks));
                        buf.append(' ');
                    }
                }
                break;

            case ASTERISK :
                buf.append("* ");
                break;

            case ORDER_BY :
                buf.append(Token.T_ORDER).append(' ').append(Token.T_BY);
                buf.append(' ');

                if (isDescending) {
                    buf.append(Token.T_DESC).append(' ');
                }
                break;

            case NEGATE :
                buf.append("NEGATE ");
                break;

            case ADD :
                buf.append("ADD ");
                break;

            case SUBTRACT :
                buf.append("SUBTRACT ");
                break;

            case MULTIPLY :
                buf.append("MULTIPLY ");
                break;

            case DIVIDE :
                buf.append("DIVIDE ");
                break;

            case CONCAT :
                buf.append("CONCAT ");
                break;

            case NOT :
                if (eArg.exprType == NOT_DISTINCT) {
                    buf.append(Token.T_DISTINCT);

                    return buf.toString();
                }

                buf.append("NOT ");
                break;

            case NOT_DISTINCT :
                buf.append("NOT ");
                buf.append("DISTINCT ");
                break;

            case EQUAL :
                buf.append("EQUAL ");
                break;

            case GREATER_EQUAL :
                buf.append("GREATER_EQUAL ");
                break;

            case GREATER :
                buf.append("GREATER ");
                break;

            case SMALLER :
                buf.append("SMALLER ");
                break;

            case SMALLER_EQUAL :
                buf.append("SMALLER_EQUAL ");
                break;

            case NOT_EQUAL :
                buf.append("NOT_EQUAL ");
                break;

            case LIKE :
                buf.append("LIKE ");
                buf.append(likeObject.describe(session));
                break;

            case AND :
                buf.append("AND ");
                break;

            case OR :
                buf.append("OR ");
                break;

            case ALL :
                buf.append("ALL ");
                break;

            case ANY :
                buf.append("ANY ");
                break;

            case IN :
                buf.append("IN ");
                break;

            case MATCH_SIMPLE :
            case MATCH_PARTIAL :
            case MATCH_FULL :
            case MATCH_UNIQUE_SIMPLE :
            case MATCH_UNIQUE_PARTIAL :
            case MATCH_UNIQUE_FULL :
                buf.append("MATCH ");
                break;

            case IS_NULL :
                buf.append("IS_NULL ");
                break;

            case UNIQUE :
                buf.append("UNIQUE ");
                break;

            case EXISTS :
                buf.append("EXISTS ");
                break;

            case COUNT :
                buf.append("COUNT ");
                break;

            case SUM :
                buf.append("SUM ");
                break;

            case MIN :
                buf.append("MIN ");
                break;

            case MAX :
                buf.append("MAX ");
                break;

            case AVG :
                buf.append("AVG ");
                break;

            case EVERY :
                buf.append(Token.T_EVERY).append(' ');
                break;

            case SOME :
                buf.append(Token.T_SOME).append(' ');
                break;

            case STDDEV_POP :
                buf.append(Token.T_STDDEV_POP).append(' ');
                break;

            case STDDEV_SAMP :
                buf.append(Token.T_STDDEV_SAMP).append(' ');
                break;

            case VAR_POP :
                buf.append(Token.T_VAR_POP).append(' ');
                break;

            case VAR_SAMP :
                buf.append(Token.T_VAR_SAMP).append(' ');
                break;

            case CONVERT :
                buf.append("CONVERT ");
                buf.append(dataType.getDefinition());
                buf.append(' ');
                break;

            case CASEWHEN :
                buf.append("CASEWHEN ");
                break;
        }

        if (eArg != null) {
            buf.append(" arg1=[");
            buf.append(eArg.describe(session, blanks + 1));
            buf.append(']');
        }

        if (eArg2 != null) {
            buf.append(" arg2=[");
            buf.append(eArg2.describe(session, blanks + 1));
            buf.append(']');
        }

        return buf.toString();
    }

    /**
     * Set the data type
     */
    void setDataType(Type type) {
        dataType = type;
    }

    private boolean equals(Expression other) {

        if (other == this) {
            return true;
        }

        if (other == null) {
            return false;
        }

        /** @todo fredt - equals() method for subSelect needed */
        if (exprType != other.exprType ||!equals(dataType, other.dataType)) {
            return false;
        }

        switch (exprType) {

            case COLUMN :
                return equals(tableName, other.tableName)
                       && equals(columnName, other.columnName)
                       && equals(schema, other.schema)
                       && equals(rangeVariable, other.rangeVariable);

            case VALUE :
                return equals(valueData, other.valueData);

            default :
                return equals(argList, other.argList)
                       && equals(subQuery, other.subQuery)
                       && equals(eArg, other.eArg)
                       && equals(eArg2, other.eArg2);
        }
    }

    public boolean equals(Object other) {

        if (other == this) {
            return true;
        }

        if (other instanceof Expression) {
            return equals((Expression) other);
        }

        return false;
    }

    private static boolean equals(Object o1, Object o2) {

        if (o1 == o2) {
            return true;
        }

        return (o1 == null) ? o2 == null
                            : o1.equals(o2);
    }

    private static boolean equals(Expression[] row1, Expression[] row2) {

        if (row1 == row2) {
            return true;
        }

        if (row1.length != row2.length) {
            return false;
        }

        int len = row1.length;

        for (int i = 0; i < len; i++) {
            Expression e1 = row1[i];
            Expression e2 = row2[i];

            if (!equals(e1, e2)) {
                return false;
            }
        }

        return true;
    }

    static boolean equals(Expression e1, Expression e2) {
        return (e1 == null) ? e2 == null
                            : e1.equals(e2);
    }

    boolean isComposedOf(Expression exprList[], int start, int end,
                         OrderedIntHashSet excludeSet) {

        if (exprType == VALUE) {
            return true;
        }

        if (excludeSet.contains(exprType)) {
            return true;
        }

        for (int i = start; i < end; i++) {
            if (equals(exprList[i])) {
                return true;
            }
        }

        switch (exprType) {

            case LIKE :
            case ALL :
            case ANY :
            case IN :
            case MATCH_SIMPLE :
            case MATCH_PARTIAL :
            case MATCH_FULL :
            case MATCH_UNIQUE_SIMPLE :
            case MATCH_UNIQUE_PARTIAL :
            case MATCH_UNIQUE_FULL :
            case UNIQUE :
            case EXISTS :
            case TABLE_SUBQUERY :
            case COUNT :
            case SUM :
            case MIN :
            case MAX :
            case AVG :
            case EVERY :
            case SOME :
            case STDDEV_POP :
            case STDDEV_SAMP :
            case VAR_POP :
            case VAR_SAMP :
                return false;
        }

        boolean result = true;

        if (argList != null) {
            for (int i = 0; i < argList.length; i++) {
                result &= (argList[i] == null
                           || argList[i].isComposedOf(exprList, start, end,
                                                      excludeSet));
            }

            return result;
        }

        if (eArg == null && eArg2 == null) {
            return false;
        }

        result = (eArg == null
                  || eArg.isComposedOf(exprList, start, end, excludeSet));
        result &= (eArg2 == null
                   || eArg2.isComposedOf(exprList, start, end, excludeSet));

        return result;
    }

    boolean isComposedOf(OrderedHashSet expressions,
                         OrderedIntHashSet excludeSet) {

        if (exprType == VALUE) {
            return true;
        }

        if (excludeSet.contains(exprType)) {
            return true;
        }

        for (int i = 0; i < expressions.size(); i++) {
            if (equals(expressions.get(i))) {
                return true;
            }
        }

        switch (exprType) {

            case LIKE :
            case ALL :
            case ANY :
            case IN :
            case MATCH_SIMPLE :
            case MATCH_PARTIAL :
            case MATCH_FULL :
            case MATCH_UNIQUE_SIMPLE :
            case MATCH_UNIQUE_PARTIAL :
            case MATCH_UNIQUE_FULL :
            case UNIQUE :
            case EXISTS :
            case TABLE_SUBQUERY :
            case COUNT :
            case SUM :
            case MIN :
            case MAX :
            case AVG :
            case EVERY :
            case SOME :
            case STDDEV_POP :
            case STDDEV_SAMP :
            case VAR_POP :
            case VAR_SAMP :
                return false;
        }

        boolean result = true;

        if (argList != null) {
            for (int i = 0; i < argList.length; i++) {
                result &= (argList[i] == null
                           || argList[i].isComposedOf(expressions,
                                                      excludeSet));
            }

            return result;
        }

        if (eArg == null && eArg2 == null) {
            return false;
        }

        result = (eArg == null || eArg.isComposedOf(expressions, excludeSet));
        result &= (eArg2 == null
                   || eArg2.isComposedOf(expressions, excludeSet));

        return result;
    }

    /**
     * Check if this expression defines a constant value.
     * <p>
     * It does, if it is a constant value expression, or all the argument
     * expressions define constant values.
     */
    boolean isConstant() {

        switch (exprType) {

            case VALUE :
                return true;

            case NEGATE :
                return eArg.isConstant();

            case ADD :
            case SUBTRACT :
            case MULTIPLY :
            case DIVIDE :
            case CONCAT :
                return eArg.isConstant() && eArg2.isConstant();
        }

        return false;
    }

    /**
     *  Is this (indirectly) an aggregate expression
     */
    boolean isAggregate() {
        return aggregateSpec != AGGREGATE_NONE;
    }

    static boolean isAggregate(int type) {

        switch (type) {

            case COUNT :
            case MAX :
            case MIN :
            case SUM :
            case AVG :
            case EVERY :
            case SOME :
            case STDDEV_POP :
            case STDDEV_SAMP :
            case VAR_POP :
            case VAR_SAMP :
                return true;
        }

        return false;
    }

    /**
     * Set an ORDER BY column expression DESC
     */
    void setDescending() {
        isDescending = true;
    }

    /**
     * Is an ORDER BY column expression DESC
     */
    boolean isDescending() {
        return isDescending;
    }

    /**
     * Set the column alias and whether the name is quoted
     */
    void setAlias(String s, boolean isquoted) {
        columnAlias = s;
        aliasQuoted = isquoted;
    }

    /**
     * Change the column name
     */
    void setColumnName(String newname, boolean isquoted) {
        columnName   = newname;
        columnQuoted = isquoted;
    }

    /**
     * Change the table name
     */
    void setTableName(String newname) {
        tableName = newname;
    }

    /**
     * Return the user defined alias or null if none
     */
    String getDefinedAlias() {
        return columnAlias;
    }

    /**
     * Get the column alias
     */
    String getAlias() {

        if (columnAlias != null) {
            return columnAlias;
        }

        if (exprType == COLUMN) {
            return columnName;
        }

        return "";
    }

    /**
     * Is a column alias quoted
     */
    boolean isAliasQuoted() {

        if (columnAlias != null) {
            return aliasQuoted;
        }

        if (exprType == COLUMN) {
            return columnQuoted;
        }

        return false;
    }

    /**
     * Returns the type of expression
     */
    public int getType() {
        return exprType;
    }

    /**
     * Returns the type of expression
     */
    public Object getMetaData() {
        return null;
    }

    /**
     * Returns the left node
     */
    Expression getArg() {
        return eArg;
    }

    /**
     * Returns the right node
     */
    Expression getArg2() {
        return eArg2;
    }

    /**
     * Returns the range variable for a COLUMN expression
     */
    RangeVariable getRangeVariable() {
        return rangeVariable;
    }

    void replaceColumnReferenceInOrderBy(Expression[] columns, int length) {

        Expression e = eArg;

        for (int i = 0; i < length; i++) {
            if (eArg.equals(columns[i])) {
                eArg                  = columns[i];
                queryTableColumnIndex = i;

                return;
            }

            if (e.columnName != null
                    && e.columnName.equals(columns[i].columnName)
                    && (e.tableName == null || e.tableName.equals(
                        columns[i].tableName)) && (e.schema == null
                                                   || e.schema.equals(
                                                       columns[i].schema))) {
                queryTableColumnIndex = i;
                eArg                  = columns[i];
            }
        }
    }

    /**
     * return the expression for an aliase used in an ORDER BY clause
     */
    Expression replaceAliasInOrderBy(Expression[] columns, int length,
                                     Expression parent) {

        if (eArg != null) {
            eArg = eArg.replaceAliasInOrderBy(columns, length, this);
        }

        if (eArg2 != null) {
            eArg2 = eArg2.replaceAliasInOrderBy(columns, length, this);
        }

        switch (exprType) {

            case FUNCTION :
            case SQL_FUNCTION : {
                Expression e;

                for (int i = 0; i < argList.length; i++) {
                    e = argList[i];

                    if (e != null) {
                        argList[i] = e.replaceAliasInOrderBy(columns, length,
                                                             this);
                    }
                }

                break;
            }
            case COLUMN : {
                for (int i = 0; i < length; i++) {
                    if (columnName.equals(columns[i].columnAlias)
                            && tableName == null && schema == null) {
                        if (parent.exprType == ORDER_BY) {
                            parent.queryTableColumnIndex = i;
                        }

                        return columns[i];
                    }
                }
            }
            default :
        }

        return this;
    }

    /**
     * Workaround for CHECK constraints. We don't want optimisation so we
     * flag all LIKE expressions as already optimised.
     */
    void setLikeOptimised() throws HsqlException {

        if (eArg != null) {
            eArg.setLikeOptimised();
        }

        if (eArg2 != null) {
            eArg2.setLikeOptimised();
        }

        if (exprType == LIKE) {
            likeObject.optimised = true;
        }
    }

    /**
     * Find a range variable with the given table alias
     */
    RangeVariable findMatchingRangeVariable(RangeVariable[] rangeVarArray) {

        for (int i = 0; i < rangeVarArray.length; i++) {
            RangeVariable rangeVar = rangeVarArray[i];

            if (rangeVar.resolvesTableName(this)) {
                return rangeVar;
            }
        }

        return null;
    }

    /**
     * collects all range variables in expression tree
     */
    void collectRangeVariables(RangeVariable[] rangeVariables, Set set) {

        if (eArg != null) {
            eArg.collectRangeVariables(rangeVariables, set);
        }

        if (eArg2 != null) {
            eArg2.collectRangeVariables(rangeVariables, set);
        }

        if (argList != null) {
            for (int i = 0; i < argList.length; i++) {
                if (argList[i] != null) {
                    argList[i].collectRangeVariables(rangeVariables, set);
                }
            }
        } else if (rangeVariable != null) {
            for (int i = 0; i < rangeVariables.length; i++) {
                if (rangeVariables[i] == rangeVariable) {
                    set.add(rangeVariables[i]);
                }
            }
        } else if (subQuery != null) {
            if (subQuery.select != null) {
                subQuery.select.collectRangeVariables(rangeVariables, set);
            }
        }
    }

    /**
     * collects all range variables in expression tree
     */
    void markRangeVariables(RangeVariable[] rangeVariables, boolean[] flags) {

        if (eArg != null) {
            eArg.markRangeVariables(rangeVariables, flags);
        }

        if (eArg2 != null) {
            eArg2.markRangeVariables(rangeVariables, flags);
        }

        if (argList != null) {
            for (int i = 0; i < argList.length; i++) {
                if (argList[i] != null) {
                    argList[i].markRangeVariables(rangeVariables, flags);
                }
            }
        } else if (rangeVariable != null) {
            for (int i = 0; i < rangeVariables.length; i++) {
                if (rangeVariables[i] == rangeVariable) {
                    flags[i] = true;
                }
            }
        } else if (subQuery != null) {
            if (subQuery.select != null) {
                subQuery.select.markRangeVariables(rangeVariables, flags);
            }
        }
    }

    /**
     * return true if given RangeVariable is used in expression tree
     */
    boolean hasReference(RangeVariable range) {

        if (range == rangeVariable) {
            return true;
        }

        if (eArg != null && eArg.hasReference(range)) {
            return true;
        }

        if (eArg2 != null && eArg2.hasReference(range)) {
            return true;
        }

        if (argList != null) {
            for (int i = 0; i < argList.length; i++) {
                if (argList[i] != null) {
                    if (argList[i].hasReference(range)) {
                        return true;
                    }
                }
            }
        } else if (subQuery != null && subQuery.select != null) {
            if (subQuery.select.hasOuterReference(range)) {
                return true;
            }
        }

        return false;
    }

    /**
     * resolve tables and collect unresolved column expressions
     */
    public OrderedHashSet resolveColumnReferences(
            RangeVariable[] rangeVarArray,
            OrderedHashSet unresolvedSet) throws HsqlException {

        if (isParam || rangeVarArray == null
                || exprType == Expression.VALUE) {
            return unresolvedSet;
        }

        if (eArg != null) {
            unresolvedSet = eArg.resolveColumnReferences(rangeVarArray,
                    unresolvedSet);
        }

        if (eArg2 != null) {
            unresolvedSet = eArg2.resolveColumnReferences(rangeVarArray,
                    unresolvedSet);
        }

        switch (exprType) {

            case COLUMN :
                if (rangeVariable != null) {
                    break;
                }

                for (int i = 0; i < rangeVarArray.length; i++) {
                    RangeVariable rangeVar = rangeVarArray[i];

                    if (rangeVar == null
                            ||!rangeVar.resolvesTableName(this)) {
                        continue;
                    }

                    Table table    = rangeVar.rangeTable;
                    int   colIndex = rangeVar.findColumn(columnName);

                    if (colIndex != -1) {
                        rangeVariable = rangeVar;

                        setAttributesAsColumn(table, colIndex);
                        rangeVariable.addColumn(colIndex);

                        break;
                    }
                }

                if (rangeVariable == null) {
                    if (unresolvedSet == null) {
                        unresolvedSet = new OrderedHashSet();
                    }

                    unresolvedSet.add(this);
                }
                break;

            case TABLE_SUBQUERY :

                // we now (1_7_2_ALPHA_R) resolve independently first, then
                // resolve in the enclosing context
                if (subQuery != null) {
                    unresolvedSet = subQuery.select.resolveCorrelatedColumns(
                        rangeVarArray, unresolvedSet);
                }
                break;

            case FUNCTION :
            case SQL_FUNCTION :
            case TABLE :
            case ROW :
                if (argList == null) {
                    break;
                }

                for (int i = 0; i < argList.length; i++) {
                    if (argList[i] == null) {
                        continue;
                    }

                    unresolvedSet =
                        argList[i].resolveColumnReferences(rangeVarArray,
                                                           unresolvedSet);
                }
                break;

            default :
        }

        return unresolvedSet;
    }

    /**
     * collects expressions without keys
     */
    public OrderedHashSet getUnkeyedColumns(OrderedHashSet unresolvedSet) {

        if (isParam || exprType == Expression.VALUE) {
            return unresolvedSet;
        }

        if (eArg != null) {
            unresolvedSet = eArg.getUnkeyedColumns(unresolvedSet);
        }

        if (eArg2 != null) {
            unresolvedSet = eArg2.getUnkeyedColumns(unresolvedSet);
        }

        switch (exprType) {

            case COLUMN :
                if (!rangeVariable.hasKeyedColumnInGroupBy) {
                    if (unresolvedSet == null) {
                        unresolvedSet = new OrderedHashSet();
                    }

                    unresolvedSet.add(this);
                }
                break;

            case TABLE_SUBQUERY :
                if (subQuery != null) {
                    if (unresolvedSet == null) {
                        unresolvedSet = new OrderedHashSet();
                    }

                    unresolvedSet.add(this);
                }
                break;

            case FUNCTION :
            case SQL_FUNCTION :
            case TABLE :
            case ROW :
                if (argList == null) {
                    break;
                }

                for (int i = 0; i < argList.length; i++) {
                    if (argList[i] == null) {
                        continue;
                    }

                    unresolvedSet =
                        argList[i].getUnkeyedColumns(unresolvedSet);
                }
                break;

            default :
        }

        return unresolvedSet;
    }

    /**
     * For CASE WHEN and its special cases section 9.3 of the SQL standard
     * on type aggregation is implemented.
     */
    void resolveCaseWhenTypes(Session session) throws HsqlException {

        if (dataType != null) {
            return;
        }

        Expression expr = this;

        while (expr.exprType == Expression.CASEWHEN) {
            expr.eArg.resolveTypes(session, expr);

            if (expr.eArg.isParam) {
                expr.eArg.dataType = Type.SQL_BOOLEAN;
            }

            expr.eArg2.eArg.resolveTypes(session, eArg2);
            expr.eArg2.eArg2.resolveTypes(session, eArg2);

            expr = expr.eArg2.eArg2;
        }

        expr = this;

        while (expr.exprType == Expression.CASEWHEN) {
            dataType = Type.getAggregatedType(expr.eArg2.eArg.dataType,
                                              dataType);
            dataType = Type.getAggregatedType(expr.eArg2.eArg2.dataType,
                                              dataType);
            expr = expr.eArg2.eArg2;
        }

        expr = this;

        while (expr.exprType == Expression.CASEWHEN) {
            if (expr.eArg2.eArg.dataType == null) {
                expr.eArg2.eArg.dataType = dataType;
            }

            if (expr.eArg2.eArg2.dataType == null) {
                expr.eArg2.eArg2.dataType = dataType;
            }

            if (expr.eArg2.dataType == null) {
                expr.eArg2.dataType = dataType;
            }

            expr = expr.eArg2.eArg2;
        }
    }

    private void resolveTypesForLogicalOp(Session session)
    throws HsqlException {

        if (eArg.isParam) {
            eArg.dataType = Type.SQL_BOOLEAN;
        }

        if (eArg2.isParam) {
            eArg2.dataType = Type.SQL_BOOLEAN;
        }

        if (eArg.dataType == null || eArg2.dataType == null) {
            throw Trace.error(Trace.NULL_LITERAL_NOT_ALLOWED);
        }

        if (eArg.exprType == Expression.ROW
                || eArg2.exprType == Expression.ROW
                || Type.SQL_BOOLEAN != eArg.dataType
                || Type.SQL_BOOLEAN != eArg2.dataType) {

            // todo SQL type
            throw Trace.error(Trace.WRONG_DATA_TYPE);
        }
    }

    private void resolveTypesForComparison(Session session,
                                           Expression parent)
                                           throws HsqlException {

        if (eArg.isParam && eArg2.isParam) {
            throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                              Trace.Expression_resolveTypes3);
        }

        if (eArg2.exprType == Expression.ALL
                || eArg2.exprType == Expression.ANY) {
            resolveTypesForAllAny(session);

            return;
        }

        if (eArg.exprType == Expression.ROW
                || eArg2.exprType == Expression.ROW) {
            if (eArg.exprType != Expression.ROW
                    || eArg2.exprType != Expression.ROW
                    || eArg.argList.length != eArg2.argList.length) {

                // todo better message
                throw Trace.error(Trace.WRONG_DATA_TYPE);
            }
        }

        if (isFixedConditional()) {
            valueData = getValue(session);
            exprType  = VALUE;
            eArg      = null;
            eArg2     = null;

            return;
        }

        if (eArg.exprType == Expression.ROW) {
            argListOpDataType = new Type[eArg.argList.length];

            for (int i = 0; i < argListOpDataType.length; i++) {
                Type leftType  = eArg.argListDataType[i];
                Type rightType = eArg2.argListDataType[i];

                argListOpDataType[i] = getComparisonType(leftType, rightType);
            }
        } else {
            if (eArg.isParam) {
                eArg.dataType = eArg2.dataType;
            } else if (eArg2.isParam) {
                eArg2.dataType = eArg.dataType;
            }

            if (eArg.dataType == null || eArg2.dataType == null) {
                throw Trace.error(Trace.NULL_LITERAL_NOT_ALLOWED);
            }

            opDataType = getComparisonType(eArg.dataType, eArg2.dataType);
        }
    }

    public Type getComparisonType(Type leftType,
                                  Type rightType) throws HsqlException {

        if (leftType.isNumberType()) {
            return leftType.getCombinedType(rightType, exprType);
        } else if (leftType.isDateTimeType()) {
            if (rightType.isCharacterType()) {
                return leftType;
            }
        } else if (rightType.isDateTimeType()) {
            return rightType;
        } else {

            // simply throw if not compatible, otherwise conversion not required
            leftType.getCombinedType(rightType, EQUAL);
        }

        return null;
    }

    public void resolveTypes(Session session,
                             Expression parent) throws HsqlException {

        if (isParam) {
            return;
        }

        if (eArg != null) {
            eArg.resolveTypes(session, this);
        }

        if (eArg2 != null) {
            eArg2.resolveTypes(session, this);
        }

        switch (exprType) {

            case VALUE :
                break;

            case COLUMN :
                break;

            case TABLE :
                for (int i = 0; i < argList.length; i++) {
                    if (argList[i] != null) {
                        argList[i].resolveTypes(session, this);
                    }
                }
                break;

            case ROW :
                argListDataType = new Type[argList.length];

                for (int i = 0; i < argList.length; i++) {
                    if (argList[i] != null) {
                        argList[i].resolveTypes(session, this);

                        argListDataType[i] = argList[i].dataType;
                    }
                }
                break;

            case FUNCTION :
            case SQL_FUNCTION :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "resolveTypes()");
            case TABLE_SUBQUERY : {
                subQuery.select.resolveTypes(session);

                argListDataType =
                    new Type[subQuery.select.visibleColumnCount];

                for (int i = 0; i < argListDataType.length; i++) {
                    argListDataType[i] =
                        subQuery.select.exprColumns[i].dataType;
                }

                dataType = argListDataType[0];

                break;
            }
            case ORDER_BY :
                if (eArg.isParam) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                                      Trace.Expression_resolveTypes1);
                }

                dataType = eArg.dataType;
                break;

            case NEGATE :
                if (eArg.isParam || eArg.dataType == null) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE);
                }

                dataType = eArg.dataType;

                if (!dataType.isNumberType()) {

                    // todo SQL type
                    throw Trace.error(Trace.WRONG_DATA_TYPE);
                }

                if (isFixedConstant()) {
                    valueData = getValue(session, dataType);
                    eArg      = null;
                    exprType  = VALUE;
                }
                break;

            case ADD :

                // special case for concat using +
                if ((eArg.dataType != null && eArg.dataType.isCharacterType())
                        || (eArg2.dataType != null
                            && eArg2.dataType.isCharacterType())) {
                    exprType = Expression.CONCAT;

                    resolveTypesForConcat(session);

                    break;
                }
            case SUBTRACT :
            case MULTIPLY :
            case DIVIDE :
                resolveTypesForArithmetic(session);
                break;

            case CONCAT :
                resolveTypesForConcat(session);
                break;

            case NOT_DISTINCT :
            case EQUAL :
            case GREATER_EQUAL :
            case GREATER :
            case SMALLER :
            case SMALLER_EQUAL :
            case NOT_EQUAL :
                resolveTypesForComparison(session, parent);
                break;

            case LIKE :
                resolveTypesForLike(session);
                break;

            case AND : {
                resolveTypesForLogicalOp(session);

                if (eArg.isFixedConditional()) {
                    if (eArg2.isFixedConditional()) {
                        valueData = getValue(session);
                        exprType  = VALUE;
                        eArg      = null;
                        eArg2     = null;
                    } else {
                        if (Boolean.FALSE.equals(eArg.getValue(session))) {
                            valueData = Boolean.FALSE;
                            exprType  = VALUE;
                            eArg      = null;
                            eArg2     = null;
                        }
                    }
                } else if (eArg2.isFixedConditional()) {
                    if (Boolean.FALSE.equals(eArg2.getValue(session))) {
                        valueData = Boolean.FALSE;
                        exprType  = VALUE;
                        eArg      = null;
                        eArg2     = null;
                    }
                }

                break;
            }
            case OR : {
                resolveTypesForLogicalOp(session);

                // optimisation
                if (eArg.isFixedConditional()) {
                    if (eArg2.isFixedConditional()) {
                        valueData = getValue(session);
                        exprType  = VALUE;
                        eArg      = null;
                        eArg2     = null;
                    } else if (Boolean.TRUE.equals(eArg.getValue(session))) {
                        valueData = Boolean.TRUE;
                        exprType  = VALUE;
                        eArg      = null;
                        eArg2     = null;
                    }
                } else if (eArg2.isFixedConditional()) {
                    if (Boolean.TRUE.equals(eArg2.getValue(session))) {
                        valueData = Boolean.TRUE;
                        exprType  = VALUE;
                        eArg      = null;
                        eArg2     = null;
                    }
                }

                break;
            }
            case IS_NULL :
                if (isFixedConditional()) {
                    valueData = getValue(session);
                    exprType  = VALUE;
                    eArg      = null;
                } else if (eArg.dataType == null) {
                    eArg.dataType = Type.SQL_VARCHAR;
                }
                break;

            case NOT :
                if (eArg.isParam) {
                    eArg.dataType = Type.SQL_BOOLEAN;
                }

                if (!dataType.isBooleanType()) {

                    // todo SQL type
                    throw Trace.error(Trace.WRONG_DATA_TYPE);
                }

                if (isFixedConditional()) {
                    valueData = (Boolean) eArg.getValue(session);
                    exprType  = VALUE;
                    eArg      = null;
                    eArg2     = null;
                }
                break;

            case OVERLAPS :
                resolveTypesForOverlaps();
                break;

            case ALL :
            case ANY :
                break;

            case IN :
            case MATCH_SIMPLE :
            case MATCH_PARTIAL :
            case MATCH_FULL :
            case MATCH_UNIQUE_SIMPLE :
            case MATCH_UNIQUE_PARTIAL :
            case MATCH_UNIQUE_FULL :
                resolveTypesForIn(session);
                break;

            case UNIQUE :
            case EXISTS :
                break;

            case COUNT :
                if (eArg.isParam) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                                      Trace.Expression_resolveTypes4);
                }

                dataType = Type.SQL_INTEGER;
                break;

            case MAX :
            case MIN :
            case SUM :
            case AVG :
            case EVERY :
            case SOME :
            case STDDEV_POP :
            case STDDEV_SAMP :
            case VAR_POP :
            case VAR_SAMP :
                if (eArg.isParam) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                                      Trace.Expression_resolveTypes4);
                }

                dataType = SetFunction.getType(exprType, eArg.dataType);
                break;

            case CONVERT :

                // NOTE: both iDataType for this expr and for eArg (if isParm)
                // are already set in Parser during read
                if (eArg.isFixedConstant() || eArg.isFixedConditional()) {
                    valueData = getValue(session);
                    exprType  = VALUE;
                    eArg      = null;
                }
                break;

            case CASEWHEN :

                // We use CASEWHEN as parent type.
                // In the parent, eArg is the condition, and eArg2 is
                // the leaf, tagged as type ALTERNATIVE; its eArg is
                // case 1 (how to get the value when the condition in
                // the parent evaluates to true), while its eArg2 is case 2
                // (how to get the value when the condition in
                // the parent evaluates to false).
                resolveCaseWhenTypes(session);

                if (dataType == null) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                                      Trace.Expression_resolveTypes6);
                }
                break;

            case ALTERNATIVE : {
                break;
            }
        }
    }

    void resolveTypesForArithmetic(Session session) throws HsqlException {

        if (eArg.isParam && eArg2.isParam) {
            throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                              Trace.Expression_resolveTypes2);
        }

        if (eArg.isParam) {
            eArg.dataType = eArg2.dataType;
        } else if (eArg2.isParam) {
            eArg2.dataType = eArg.dataType;
        }

        if (eArg.dataType == null || eArg2.dataType == null) {
            throw Trace.error(Trace.NULL_LITERAL_NOT_ALLOWED);
        }

        // datetime subtract - type predetermined
        if (eArg.dataType.isDateTimeType()
                && eArg2.dataType.isDateTimeType()) {
            if (dataType == null) {
                throw Trace.error(Trace.UNRESOLVED_TYPE);
            } else if (!dataType.isIntervalType()
                       || eArg.dataType.type != eArg2.dataType.type) {
                throw Trace.error(Trace.WRONG_DATA_TYPE);
            }
        } else {
            dataType = eArg.dataType.getCombinedType(eArg2.dataType,
                    exprType);
        }

        if (isFixedConstant()) {
            valueData = getValue(session, dataType);
            eArg      = null;
            eArg2     = null;
            exprType  = VALUE;
        }
    }

    void resolveTypesForConcat(Session session) throws HsqlException {

        if (dataType != null) {
            return;
        }

        if (eArg.isParam && eArg2.isParam) {
            throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                              Trace.Expression_resolveTypes2);
        }

        if (eArg.isParam) {
            eArg.dataType = eArg2.dataType;
        } else if (eArg2.isParam) {
            eArg2.dataType = eArg.dataType;
        }

        if (eArg.dataType == null || eArg2.dataType == null) {
            throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                              Trace.Expression_resolveTypes2);
        }

        if (eArg.dataType.isBinaryType() ^ eArg2.dataType.isBinaryType()) {
            throw Trace.error(Trace.WRONG_DATA_TYPE);
        }

        // conversion of second arg to character for backward compatibility
        if (eArg.dataType.isCharacterType()
                &&!eArg2.dataType.isCharacterType()) {
            Type newType = CharacterType.getCharacterType(Types.SQL_VARCHAR,
                eArg2.dataType.displaySize());

            eArg2 = new Expression(eArg2, newType);
        }

        dataType = eArg.dataType.getCombinedType(eArg2.dataType,
                Expression.CONCAT);

        if (isFixedConstant()) {
            valueData = getValue(session, dataType);
            eArg      = null;
            eArg2     = null;
            exprType  = VALUE;
        }
    }

    void resolveTypesForLike(Session session) throws HsqlException {

        if (eArg.isParam && eArg2.isParam) {
            throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                              Trace.Expression_resolveTypeForLike);
        }

        if (isFixedConditional()) {
            valueData = (Boolean) eArg.getValue(session);
            exprType  = VALUE;
            eArg      = null;
            eArg2     = null;
        } else if (eArg.isParam) {
            eArg.dataType = eArg2.dataType;
        } else if (eArg2.isParam) {
            eArg2.dataType = eArg.dataType;
        }

        if (eArg.dataType == null || eArg2.dataType == null) {
            throw Trace.error(Trace.NULL_LITERAL_NOT_ALLOWED);
        }

// boucherb@users 2003-09-25 - patch 1.7.2 Alpha P
//
// Some optimizations for LIKE
//
// TODO:
//
// See if the same optimizations can be done dynamically at execute time when
// eArg2 is PARAM.  Unfortunately, this currently requires re-resolving from
// the root any expression containing at least one parameterized LIKE in the
// compiled statement and reseting conditions on any involved range variable,
// so the answer is: probably not, at least not under the current code.
//
// CHECKME:
//
// Test for correct results under all XXXCHAR types (padding, etc.?)
//
// NOTE:
//
// For the old behaviour, simply comment out the block below
        if (likeObject.optimised) {
            return;
        }

        boolean isRightArgFixedConstant = eArg2.isFixedConstant();
        String likeStr = isRightArgFixedConstant
                         ? (String) eArg2.getValue(session, Type.SQL_VARCHAR)
                         : null;
        boolean ignoreCase = eArg.dataType.type == Types.VARCHAR_IGNORECASE
                             || eArg2.dataType.type
                                == Types.VARCHAR_IGNORECASE;

        likeObject.setParams(session, likeStr, ignoreCase);

        if (!isRightArgFixedConstant) {

            // Then we are done here, since it's impossible
            // to determine at this point if the right expression
            // will have a fixed prefix that can be used to optimize
            // any involved range variable
            return;
        }

        if (likeObject.isEquivalentToFalsePredicate()) {
            valueData  = Boolean.FALSE;
            exprType   = VALUE;
            eArg       = null;
            eArg2      = null;
            likeObject = null;
        } else if (likeObject.isEquivalentToEqualsPredicate()) {
            exprType = EQUAL;
            eArg2 = new Expression(likeObject.getRangeLow(),
                                   Type.SQL_VARCHAR);
            likeObject = null;
        } else if (likeObject.isEquivalentToNotNullPredicate()) {}
        else {
            if (eArg.exprType != Expression.COLUMN) {
                return;
            }

            if (!eArg.dataType.isCharacterType()) {
                return;
            }

            boolean between = false;
            boolean like    = false;
            boolean larger  = false;

            if (likeObject.isEquivalentToBetweenPredicate()) {

                // X LIKE 'abc%' <=> X >= 'abc' AND X <= 'abc' || max_collation_char
                larger  = likeObject.hasCollation;
                between = !larger;
                like    = larger;
            } else if (likeObject
                    .isEquivalentToBetweenPredicateAugmentedWithLike()) {

                // X LIKE 'abc%...' <=> X >= 'abc' AND X <= 'abc' || max_collation_char AND X LIKE 'abc%...'
                larger  = likeObject.hasCollation;
                between = !larger;
                like    = true;
            }

            if (between == false && larger == false) {
                return;
            }

            Expression eFirst = new Expression(likeObject.getRangeLow(),
                                               Type.SQL_VARCHAR);
            Expression eLast = new Expression(likeObject.getRangeHigh(),
                                              Type.SQL_VARCHAR);

            if (between &&!like) {
                Expression eArgOld = eArg;

                eArg       = new Expression(GREATER_EQUAL, eArgOld, eFirst);
                eArg2      = new Expression(SMALLER_EQUAL, eArgOld, eLast);
                exprType   = AND;
                likeObject = null;
            } else if (between && like) {
                Expression gte = new Expression(GREATER_EQUAL, eArg, eFirst);
                Expression lte = new Expression(SMALLER_EQUAL, eArg, eLast);

                eArg2 = new Expression(eArg, eArg2, likeObject.escapeChar,
                                       likeObject.hasCollation);
                eArg2.likeObject = likeObject;
                eArg             = new Expression(AND, gte, lte);
                exprType         = AND;
                likeObject       = null;
            } else if (larger) {
                Expression gte = new Expression(GREATER_EQUAL, eArg, eFirst);

                eArg2 = new Expression(eArg, eArg2, likeObject.escapeChar,
                                       likeObject.hasCollation);
                eArg2.likeObject = likeObject;
                eArg             = gte;
                exprType         = AND;
                likeObject       = null;
            }
        }
    }

    void resolveTypesForOverlaps() throws HsqlException {

        if (eArg.argList[0].isParam) {
            eArg.argList[0].dataType = eArg2.argList[0].dataType;
        }

        if (eArg2.argList[0].isParam) {
            eArg2.argList[0].dataType = eArg.argList[0].dataType;
        }

        if (eArg.argList[0].dataType == null) {
            eArg.argList[0].dataType = eArg2.argList[0].dataType =
                Type.SQL_TIMESTAMP;
        }

        if (eArg.argList[1].isParam) {
            eArg.argList[1].dataType = eArg2.argList[0].dataType;
        }

        if (eArg2.argList[1].isParam) {
            eArg2.argList[1].dataType = eArg.argList[0].dataType;
        }

        if (!isValidDatetimeRange(eArg.argList)
                ||!isValidDatetimeRange(eArg2.argList)) {
            Trace.error(Trace.WRONG_DATA_TYPE);
        }

        if (!isValidDatetimeRange(eArg.argList[0].dataType,
                                  eArg.argList[1].dataType)) {
            Trace.error(Trace.WRONG_DATA_TYPE);
        }

        eArg.argListDataType[0]  = eArg.argList[0].dataType;
        eArg.argListDataType[1]  = eArg.argList[1].dataType;
        eArg2.argListDataType[0] = eArg2.argList[0].dataType;
        eArg2.argListDataType[1] = eArg2.argList[1].dataType;
    }

    boolean isValidDatetimeRange(Expression[] pair) {
        return isValidDatetimeRange(pair[0].dataType, pair[1].dataType);
    }

    boolean isValidDatetimeRange(Type a, Type b) {

        if (!a.isDateTimeType()) {
            return false;
        }

        if (b.isDateTimeType()) {
            if ((a.type == Types.SQL_TIME && b.type == Types.SQL_DATE)
                    || (a.type == Types.SQL_DATE
                        && b.type == Types.SQL_TIME)) {
                return false;
            }

            return true;
        }

        if (b.isIntervalType()) {
            return ((DateTimeType) a).canAdd(b);
        }

        return false;
    }

    void resolveTypesForAllAny(Session session) throws HsqlException {

        eArg2.argListDataType = eArg2.eArg.argListDataType;

        int degree = eArg.exprType == ROW ? eArg.argList.length
                                          : 1;

        if (degree == 1 && eArg.exprType != ROW) {
            eArg = new Expression(Expression.ROW, new Expression[]{ eArg });
        }

        if (degree != eArg2.argListDataType.length) {

            // todo SQL message
            throw Trace.error(Trace.WRONG_DATA_TYPE);
        }

        for (int j = 0; j < degree; j++) {
            if (eArg.argList[j].isParam) {
                eArg.argList[j].dataType = eArg2.eArg.argListDataType[j];
            }
        }

        eArg.argListDataType = new Type[eArg.argList.length];

        for (int i = 0; i < eArg.argListDataType.length; i++) {
            eArg.argListDataType[i] = eArg.argList[i].dataType;
        }
    }

    /**
     * Parametric or fixed value lists plus queries are handled.
     *
     * Empty lists are not allowed.
     *
     * Parametric predicand is resolved against the value list and vice versa.
     */
    void resolveTypesForIn(Session session) throws HsqlException {

        int degree = eArg.exprType == ROW ? eArg.argList.length
                                          : 1;

        if (degree == 1 && eArg.exprType != ROW) {
            eArg = new Expression(Expression.ROW, new Expression[]{ eArg });
        }

        if (eArg2.exprType == TABLE_SUBQUERY) {
            if (degree != eArg2.argListDataType.length) {

                // todo SQL message
                throw Trace.error(Trace.WRONG_DATA_TYPE);
            }

            for (int j = 0; j < degree; j++) {
                if (eArg.argList[j].isParam) {
                    eArg.argList[j].dataType = eArg2.argListDataType[j];
                }
            }

            eArg2.isCorrelated = eArg2.subQuery.isCorrelated;
        } else {
            eArg2.prepareTable(session, eArg, degree);

            if (degree != eArg2.argListDataType.length) {

                // todo SQL message
                throw Trace.error(Trace.WRONG_DATA_TYPE);
            }

            // IN condition optimisation
            if (eArg2.isCorrelated) {
                eArg2.subQuery.table = null;
            } else {
                eArg2.subQuery.setAsInSubquery(eArg2);
            }
        }

        eArg.argListDataType = new Type[eArg.argList.length];

        for (int i = 0; i < eArg.argListDataType.length; i++) {
            Type type = eArg.argList[i].dataType;

            if (type == null) {
                type = eArg2.argListDataType[i];
            }

            if (type == null) {
                throw Trace.error(Trace.UNRESOLVED_TYPE);
            }

            eArg.argListDataType[i] = type;
        }
    }

    void prepareTable(Session session, Expression row,
                      int degree) throws HsqlException {

        if (argListDataType != null) {
            return;
        }

        for (int i = 0; i < argList.length; i++) {
            Expression e = argList[i];

            if (e.exprType == ROW) {
                if (degree != e.argList.length) {

                    // todo - SQL error message
                    throw Trace.error(Trace.UNEXPECTED_TOKEN);
                }
            } else if (degree == 1) {
                argList[i]         = new Expression(Expression.ROW);
                argList[i].argList = new Expression[]{ e };
            } else {

                // todo - SQL error message
                throw Trace.error(Trace.UNEXPECTED_TOKEN);
            }
        }

        argListDataType = new Type[degree];

        for (int j = 0; j < degree; j++) {
            Type type = row == null ? null
                                    : row.argList[j].dataType;

            for (int i = 0; i < argList.length; i++) {
                argList[i].argList[j].resolveTypes(session, this);

                type = Type.getAggregatedType(argList[i].argList[j].dataType,
                                              type);
            }

            if (type == null) {
                throw Trace.error(Trace.UNRESOLVED_TYPE);
            }

            // needs a new variable for row type array
            argListDataType[j] = type;

            if (row != null && row.argList[j].isParam) {
                row.argList[j].dataType = type;
            }

            for (int i = 0; i < argList.length; i++) {
                if (argList[i].argList[j].isParam) {
                    isConstantValueList            = false;
                    argList[i].argList[j].dataType = argListDataType[j];

                    continue;
                }

                if (argList[i].argList[j].exprType == VALUE) {
                    if (argList[i].argList[j].valueData == null) {
                        argList[i].argList[j].dataType = argListDataType[j];
                    }
                } else {
                    isCorrelated        = true;
                    isConstantValueList = false;
                }
            }

            if (argListDataType[j].isCharacterType()
                    &&!((CharacterType) argListDataType[j])
                        .isEqualIdentical()) {
                isConstantValueList = false;
            }
        }
    }

    /**
     * Details of IN condition optimisation for 1.9.0
     * Predicates with SELECT are QUERY expressions
     *
     * Predicates with IN list
     *
     * Parser adds a SubQuery to the list for each predicate
     * At type resolution IN lists that are entirely fixed constant or parameter
     * values are selected for possible optimisation. The flags:
     *
     * IN expression eArg2.isCorrelated == true if there are non-constant,
     * non-param expressions in the list (Expressions may have to be resolved
     * against the full set of columns of the query, so must be re-evaluated
     * for each row and evaluated after all the joins have been made)
     *
     * VALUELIST expression isFixedConstantValueList == true when all
     * expressions are fixed constant and none is a param. With this flag,
     * a single-column VALUELIST can be accessed as a HashMap.
     *
     * Predicates may be optimised as joins if isCorrelated == false
     *
     */
    void insertValuesIntoSubqueryTable(Session session) throws HsqlException {

        Table table = subQuery.table;

        for (int i = 0; i < argList.length; i++) {
            Object[] data = (Object[]) argList[i].getRowValue(session);

            for (int j = 0; j < argListDataType.length; j++) {
                data[j] = argListDataType[j].convertToType(session, data[j],
                        argList[i].argList[j].dataType);
            }

            Row row = table.newRow(data);

            try {
                table.indexRow(session, row);
            } catch (HsqlException e) {}
        }
    }

    /**
     * Is the argument expression type a comparison expression
     *
     * @param i expresion type
     *
     * @return boolean
     */
    static boolean isCompare(int i) {

        switch (i) {

            case NOT_DISTINCT :
            case EQUAL :
            case GREATER_EQUAL :
            case GREATER :
            case SMALLER :
            case SMALLER_EQUAL :
            case NOT_EQUAL :
                return true;
        }

        return false;
    }

    /**
     * Returns the table name for a column expression as a string
     *
     * @return table name
     */
    String getTableName() {

        if (exprType == ASTERISK) {
            return tableName;
        }

        if (exprType == COLUMN) {
            if (rangeVariable == null) {
                return tableName;
            } else {
                return rangeVariable.getTable().getName().name;
            }
        }

        // todo
        return "";
    }

    /**
     * Returns the HsqlName of the table for a column expression
     *
     * @return table name
     */
    public HsqlName getTableHsqlName() {

        if (rangeVariable == null) {
            return null;
        } else {
            return rangeVariable.getTable().getName();
        }
    }

    public Table getTable() {

        if (rangeVariable == null) {
            return null;
        } else {
            return rangeVariable.getTable();
        }
    }

    public String getTableSchemaName() {

        if (rangeVariable == null) {
            return null;
        } else {
            return rangeVariable.getTable().getName().schema.name;
        }
    }

    /**
     * Returns the name of a column as string
     *
     * @return column name
     */
    String getColumnName() {

        if (exprType == COLUMN && column != null) {
            return column.getName().name;
        }

        return getAlias();
    }

    /**
     * Returns the name of a column as string
     */
    public String getBaseColumnName() {

        if (exprType == COLUMN && rangeVariable != null) {
            return rangeVariable.getTable().getColumn(
                columnIndex).columnName.name;
        }

        return null;
    }

    /**
     * Returns the column index in the table
     */
    int getColumnIndex() {
        return columnIndex;
    }

    /**
     * Returns the column size
     */
    long getColumnSize() {
        return dataType == null ? 0
                                : dataType.size();
    }

    /**
     * Returns the column scale
     */
    int getColumnScale() {
        return dataType == null ? 0
                                : dataType.scale();
    }

    /**
     * Set this as a set function with / without DISTINCT
     */
    void setDistinctAggregate(boolean distinct) {

        isDistinctAggregate = distinct && (eArg.exprType != ASTERISK);

        if (exprType == COUNT) {
            dataType = distinct ? dataType
                                : Type.SQL_INTEGER;
        }
    }

    /**
     * Returns the data type
     */
    Type getDataType() {
        return dataType;
    }

    /**
     * Get the value in the given type in the given session context
     */
    Object getValue(Session session, Type type) throws HsqlException {

        Object o = getValue(session);

        if (o == null || dataType == type) {
            return o;
        }

        return type.convertToType(session, o, dataType);
    }

    /**
     * Get the result of a SetFunction or an ordinary value
     */
    public Object getAggregatedValue(Session session,
                                     Object currValue) throws HsqlException {

        boolean compareNulls = false;
        Object  leftValue    = null,
                rightValue   = null;

        switch (aggregateSpec) {

            case AGGREGATE_SELF : {

                // handles results of aggregates plus NEGATE and CONVERT
                switch (exprType) {

                    case COUNT :
                        if (currValue == null) {
                            return INTEGER_0;
                        }

                        return ((SetFunction) currValue).getValue();

                    case MAX :
                    case MIN :
                    case SUM :
                    case AVG :
                    case EVERY :
                    case SOME :
                    case STDDEV_POP :
                    case STDDEV_SAMP :
                    case VAR_POP :
                    case VAR_SAMP :
                        if (currValue == null) {
                            return null;
                        }

                        return ((SetFunction) currValue).getValue();
                }
            }
            case AGGREGATE_LEFT :
                leftValue = eArg.getAggregatedValue(session,
                                                    currValue == null ? null
                                                                      : ((Object[]) currValue)[0]);

                if (currValue == null) {
                    rightValue = eArg2 == null ? null
                                               : eArg2.getValue(session);
                } else {
                    rightValue = ((Object[]) currValue)[1];
                }
                break;

            case AGGREGATE_RIGHT :
                if (currValue == null) {
                    leftValue = eArg == null ? null
                                             : eArg.getValue(session);
                } else {
                    leftValue = ((Object[]) currValue)[0];
                }

                rightValue = eArg2.getAggregatedValue(session,
                                                      currValue == null ? null
                                                                        : ((Object[]) currValue)[1]);
                break;

            case AGGREGATE_BOTH :
                if (currValue == null) {
                    currValue = new Object[2];
                }

                leftValue =
                    eArg.getAggregatedValue(session,
                                            ((Object[]) currValue)[0]);
                rightValue =
                    eArg2.getAggregatedValue(session,
                                             ((Object[]) currValue)[1]);
                break;

            case AGGREGATE_NONE :
                return currValue;

            default :
                return currValue;
        }

        // handle other operations
        switch (exprType) {

            case ORDER_BY :
                return leftValue;

            case NEGATE :
                return ((NumberType) dataType).negate(leftValue);

            case CONVERT :
                return dataType.castToType(session, leftValue, eArg.dataType);

            case NOT :
                if (leftValue == null) {
                    return null;
                }

                return ((Boolean) leftValue).booleanValue() ? Boolean.FALSE
                                                            : Boolean.TRUE;

            case AND :
                if (Boolean.FALSE.equals(leftValue)) {
                    return Boolean.FALSE;
                }

                if (Boolean.FALSE.equals(rightValue)) {
                    return Boolean.FALSE;
                }

                if (leftValue == null || rightValue == null) {
                    return null;
                }

                return Boolean.TRUE;

            case OR :
                if (Boolean.TRUE.equals(leftValue)) {
                    return Boolean.TRUE;
                }

                if (Boolean.TRUE.equals(rightValue)) {
                    return Boolean.TRUE;
                }

                if (leftValue == null || rightValue == null) {
                    return null;
                }

                return Boolean.FALSE;

            case IS_NULL :
                return leftValue == null ? Boolean.TRUE
                                         : Boolean.FALSE;

            case LIKE :
                String s = (String) Type.SQL_VARCHAR.convertToType(session,
                    rightValue, eArg2.dataType);

                if (eArg2.isParam || eArg2.exprType != VALUE) {
                    likeObject.resetPattern(session, s);
                }

                String c = (String) Type.SQL_VARCHAR.convertToType(session,
                    leftValue, eArg.dataType);

                return likeObject.compare(session, c);

            case ALL :
            case ANY :
                return null;

            case IN :
                return testInCondition(session, (Object[]) leftValue);

            case MATCH_SIMPLE :
            case MATCH_PARTIAL :
            case MATCH_FULL :
            case MATCH_UNIQUE_SIMPLE :
            case MATCH_UNIQUE_PARTIAL :
            case MATCH_UNIQUE_FULL :
                return testMatchCondition(session, (Object[]) leftValue);

            case UNIQUE :
                return eArg.subQuery.hasUniqueNotNullRows(session)
                       ? Boolean.FALSE
                       : Boolean.TRUE;

            case EXISTS :
                return eArg.testExistsCondition(session);

            case CASEWHEN :
                boolean test = Boolean.TRUE.equals(leftValue);
                Object  result;
                Type    type;

                if (test) {
                    result = ((Object[]) rightValue)[0];
                    type   = eArg2.eArg.dataType;
                } else {
                    result = ((Object[]) rightValue)[1];
                    type   = eArg2.eArg2.dataType;
                }

                return dataType.convertToType(session, result, type);

            case ALTERNATIVE :
                leftValue = dataType.convertToType(session, leftValue,
                                                   eArg.dataType);
                rightValue = dataType.convertToType(session, rightValue,
                                                    eArg2.dataType);

                Object[] objectPair = new Object[2];

                objectPair[0] = leftValue;
                objectPair[1] = rightValue;

                return objectPair;

            case ROW :
                if (currValue == null) {
                    currValue = new Object[argList.length];
                }
            case FUNCTION :
            case SQL_FUNCTION :
                throw Trace.error(Trace.UNSUPPORTED_INTERNAL_OPERATION);
            case NOT_DISTINCT :
                compareNulls = true;
            case EQUAL :
            case GREATER_EQUAL :
            case GREATER :
            case SMALLER :
            case SMALLER_EQUAL :
            case NOT_EQUAL : {
                if (eArg2.exprType == Expression.ANY
                        || eArg2.exprType == Expression.ALL) {
                    return testAnyAllCondition(session, (Object[]) leftValue);
                }

                return compareValues(session, leftValue, rightValue);
            }
            case ADD :
                return dataType.add(leftValue, rightValue);

            case SUBTRACT :
                return dataType.subtract(leftValue, rightValue);

            case MULTIPLY :
                return dataType.multiply(leftValue, rightValue);

            case DIVIDE :
                return dataType.divide(leftValue, rightValue);

            case CONCAT :
                return ((CharacterType) dataType).concat(null, leftValue,
                        rightValue);

            default :
                throw Trace.error(Trace.NEED_AGGREGATE,
                                  this.describe(session));
        }
    }

    /**
     * Instantiate the SetFunction or recurse, returning the result
     */
    public Object updateAggregatingValue(Session session,
                                         Object currValue)
                                         throws HsqlException {

        switch (aggregateSpec) {

            case AGGREGATE_SELF : {
                if (currValue == null) {
                    currValue = new SetFunction(exprType, eArg.dataType,
                                                isDistinctAggregate);
                }

                Object newValue = eArg.exprType == ASTERISK ? INTEGER_1
                                                            : eArg.getValue(
                                                                session);

                ((SetFunction) currValue).add(session, newValue);

                return currValue;
            }
            case AGGREGATE_BOTH : {
                Object[] valuePair = (Object[]) currValue;

                if (valuePair == null) {
                    valuePair = new Object[2];
                }

                valuePair[0] = eArg.updateAggregatingValue(session,
                        valuePair[0]);
                valuePair[1] = eArg2.updateAggregatingValue(session,
                        valuePair[1]);

                return valuePair;
            }
            case AGGREGATE_LEFT : {
                Object[] valuePair = (Object[]) currValue;

                if (valuePair == null) {
                    valuePair = new Object[2];
                }

                valuePair[0] = eArg.updateAggregatingValue(session,
                        valuePair[0]);

                if (eArg2 != null) {
                    valuePair[1] = eArg2.getValue(session);
                }

                return valuePair;
            }
            case AGGREGATE_RIGHT : {
                Object[] valuePair = (Object[]) currValue;

                if (valuePair == null) {
                    valuePair = new Object[2];
                }

                if (eArg != null) {
                    valuePair[0] = eArg.getValue(session);
                }

                valuePair[1] = eArg2.updateAggregatingValue(session,
                        valuePair[1]);

                return valuePair;
            }
            default :

                // never gets here
                return currValue;
        }
    }

    public Object[] getRowValue(Session session) throws HsqlException {

        switch (exprType) {

            case ROW : {
                Object[] data = new Object[argList.length];

                for (int i = 0; i < argList.length; i++) {
                    data[i] = argList[i].getValue(session);
                }

                return data;
            }
            case Expression.TABLE_SUBQUERY : {
                return subQuery.select.getValues(session);
            }
            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Expression");
        }
    }

    public Object getValue(Session session) throws HsqlException {

        boolean compareNulls = false;

        switch (exprType) {

            case VALUE :
                return isParam
                       ? session.compiledStatementExecutor
                           .paramValues[parameterIndex]
                       : valueData;

            case COLUMN :
                try {
                    return session.compiledStatementExecutor
                        .rangeIterators[rangeVariable.index]
                        .currentData[columnIndex];
                } catch (NullPointerException e) {
                    throw Trace.error(Trace.COLUMN_NOT_FOUND, columnName);
                }
            case DEFAULT :
                return null;

            case ROW :
                if (argList.length == 1) {
                    return argList[0].getValue(session);
                }

                Object[] row = new Object[argList.length];

                for (int i = 0; i < argList.length; i++) {
                    row[i] = argList[i].getValue(session);
                }

                return row;

            case FUNCTION :
            case SQL_FUNCTION :
                throw Trace.error(Trace.UNSUPPORTED_INTERNAL_OPERATION);
            case TABLE_SUBQUERY :
                Object value = subQuery.getSingleObjectResult(session);

                return value;

            case ORDER_BY :
                return eArg.getValue(session);

            case NEGATE :
                return ((NumberType) dataType).negate(eArg.getValue(session,
                        eArg.dataType));

            case ALL :
            case ANY :
                return null;

            case IS_NULL :
                return eArg.getValue(session) == null ? Boolean.TRUE
                                                      : Boolean.FALSE;

            case LIKE :
                String s = (String) eArg2.getValue(session, Type.SQL_VARCHAR);

                if (eArg2.isParam || eArg2.exprType != VALUE) {
                    likeObject.resetPattern(session, s);
                }

                String c = (String) eArg.getValue(session, Type.SQL_VARCHAR);

                return likeObject.compare(session, c);

            case OVERLAPS :
                Object[] left  = eArg.getRowValue(session);
                Object[] right = eArg2.getRowValue(session);

                return DateTimeType.overlaps(session, left,
                                             eArg.argListDataType, right,
                                             eArg2.argListDataType);

            case IN :
                return testInCondition(session, eArg.getRowValue(session));

            case MATCH_SIMPLE :
            case MATCH_PARTIAL :
            case MATCH_FULL :
            case MATCH_UNIQUE_SIMPLE :
            case MATCH_UNIQUE_PARTIAL :
            case MATCH_UNIQUE_FULL :
                return testMatchCondition(session, eArg.getRowValue(session));

            case UNIQUE :
                return eArg.subQuery.hasUniqueNotNullRows(session)
                       ? Boolean.TRUE
                       : Boolean.FALSE;

            case EXISTS :
                return eArg.testExistsCondition(session);

            case NOT : {
                if (eArg2 != null) {
                    Trace.doAssert(false, "Expression.test");
                }

                Boolean result = (Boolean) eArg.getValue(session);

                return result == null ? null
                                      : result.booleanValue() ? Boolean.FALSE
                                                              : Boolean.TRUE;
            }
            case AND : {
                Boolean r1 = (Boolean) eArg.getValue(session);

                if (Boolean.FALSE.equals(r1)) {
                    return Boolean.FALSE;
                }

                Boolean r2 = (Boolean) eArg2.getValue(session);

                if (Boolean.FALSE.equals(r2)) {
                    return Boolean.FALSE;
                }

                if (r1 == null || r2 == null) {
                    return null;
                }

                return Boolean.TRUE;
            }
            case OR : {
                Boolean r1 = (Boolean) eArg.getValue(session);

                if (Boolean.TRUE.equals(r1)) {
                    return Boolean.TRUE;
                }

                Boolean r2 = (Boolean) eArg2.getValue(session);

                if (Boolean.TRUE.equals(r2)) {
                    return Boolean.TRUE;
                }

                if (r1 == null || r2 == null) {
                    return null;
                }

                return Boolean.FALSE;
            }
            case CONVERT :
                return dataType.castToType(session, eArg.getValue(session),
                                           eArg.dataType);

            case CASEWHEN : {
                Boolean result = (Boolean) eArg.getValue(session);

                if (Boolean.TRUE.equals(result)) {
                    return eArg2.eArg.getValue(session, dataType);
                } else {
                    return eArg2.eArg2.getValue(session, dataType);
                }
            }

            // gets here from getAggregatedValue()
            case ALTERNATIVE : {
                return new Object[] {
                    eArg.getValue(session, dataType),
                    eArg2.getValue(session, dataType)
                };
            }
            case NOT_DISTINCT :
                compareNulls = true;
            case EQUAL :
            case GREATER :
            case GREATER_EQUAL :
            case SMALLER_EQUAL :
            case SMALLER :
            case NOT_EQUAL : {
                if (eArg2.exprType == Expression.ANY
                        || eArg2.exprType == Expression.ALL) {
                    return testAnyAllCondition(
                        session, (Object[]) eArg.getRowValue(session));
                }

                Object o1 = eArg.getValue(session);
                Object o2 = eArg2.getValue(session);

                return compareValues(session, o1, o2);
            }
            case SEQUENCE :
                NumberSequence sequence = (NumberSequence) valueData;

                session.getUser().checkAccess(sequence);

                return sequence.getValueObject();
        }

        Object a = null,
               b = null;

        if (eArg != null) {
            a = eArg.getValue(session);
        }

        if (eArg2 != null) {
            b = eArg2.getValue(session);
        }

        switch (exprType) {

            case ADD :
                return dataType.add(a, b);

            case SUBTRACT :
                return dataType.subtract(a, b);

            case MULTIPLY :
                return dataType.multiply(a, b);

            case DIVIDE :
                return dataType.divide(a, b);

            case CONCAT :
                return dataType.concat(null, a, b);

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Expression");
        }
    }

    boolean testCondition(Session session) throws HsqlException {
        return Boolean.TRUE.equals(getValue(session));
    }

    /**
     * For MATCH SIMPLE and FULL expressions, nulls in left are handled
     * prior to calling this method
     */
    private Boolean compareValues(Session session, Object left,
                                  Object right) throws HsqlException {

        int     result  = 0;
        boolean hasNull = false;

        if (left == null || right == null) {
            return null;
        }

        if (left instanceof Object[]) {
            Object[] leftList  = (Object[]) left;
            Object[] rightList = (Object[]) right;

            for (int i = 0; i < eArg.argList.length; i++) {
                if (leftList[i] == null) {
                    if (exprType == MATCH_PARTIAL
                            || exprType == MATCH_UNIQUE_PARTIAL) {
                        continue;
                    }

                    hasNull = true;
                }

                if (rightList[i] == null) {
                    hasNull = true;
                }

                Object leftValue  = leftList[i];
                Object rightValue = rightList[i];
                Type[] types      = eArg.argListDataType;

                if (argListOpDataType != null
                        && argListOpDataType[i] != null) {
                    leftValue = argListOpDataType[i].convertToType(session,
                            leftValue, eArg.argListDataType[i]);
                    rightValue = argListOpDataType[i].convertToType(session,
                            rightValue, eArg2.argListDataType[i]);
                    types = argListOpDataType;
                }

                result = types[i].compare(leftValue, rightValue);

                if (result != 0) {
                    break;
                }
            }
        } else {
            Type type = eArg.dataType;

            if (opDataType != null) {
                left = opDataType.convertToType(session, left, eArg.dataType);
                right = opDataType.convertToType(session, right,
                                                 eArg2.dataType);
            }

            result = type.compare(left, right);
        }

        switch (exprType) {

            case MATCH_SIMPLE :
            case MATCH_UNIQUE_SIMPLE :
            case MATCH_PARTIAL :
            case MATCH_UNIQUE_PARTIAL :
            case MATCH_FULL :
            case MATCH_UNIQUE_FULL :
            case NOT_DISTINCT :
                return result == 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case IN :
            case EQUAL :
                if (hasNull) {
                    return null;
                }

                return result == 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case NOT_EQUAL :
                if (hasNull) {
                    return null;
                }

                return result != 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case GREATER :
                if (hasNull) {
                    return null;
                }

                return result > 0 ? Boolean.TRUE
                                  : Boolean.FALSE;

            case GREATER_EQUAL :
                if (hasNull) {
                    return null;
                }

                return result >= 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case SMALLER_EQUAL :
                if (hasNull) {
                    return null;
                }

                return result <= 0 ? Boolean.TRUE
                                   : Boolean.FALSE;

            case SMALLER :
                if (hasNull) {
                    return null;
                }

                return result < 0 ? Boolean.TRUE
                                  : Boolean.FALSE;

            default :
                throw Trace.error(Trace.GENERAL_ERROR,
                                  Trace.Expression_compareValues);
        }
    }

    static int countNulls(Object[] a) {

        int nulls = 0;

        for (int i = 0; i < a.length; i++) {
            if (a[i] == null) {
                nulls++;
            }
        }

        return nulls;
    }

    static void convertToType(Session session, Object[] data,
                              Type[] dataType,
                              Type[] newType) throws HsqlException {

        for (int i = 0; i < data.length; i++) {
            data[i] = newType[i].convertToType(session, data[i], dataType[i]);
        }
    }

    /**
     * Returns the result of testing a VALUE_LIST expression
     */
    private Boolean testInCondition(Session session,
                                    Object[] data) throws HsqlException {

        if (data == null) {
            return null;
        }

        if (Expression.countNulls(data) != 0) {
            return null;
        }

        if (eArg2.exprType == TABLE) {
            final int length = eArg2.argList.length;

            for (int i = 0; i < length; i++) {
                Object[] rowData = eArg2.argList[i].getRowValue(session);

                if (Boolean.TRUE.equals(compareValues(session, data,
                                                      rowData))) {
                    return Boolean.TRUE;
                }
            }

            return Boolean.FALSE;
        } else if (eArg2.exprType == TABLE_SUBQUERY) {
            if (eArg2.subQuery.isCorrelated) {
                eArg2.subQuery.materialise(session);
            }

            convertToType(session, data, eArg.argListDataType,
                          eArg2.argListDataType);

            RowIterator it =
                eArg2.subQuery.table.getPrimaryIndex().findFirstRow(session,
                    data);
            Boolean result = it.hasNext() ? Boolean.TRUE
                                          : Boolean.FALSE;

            eArg2.subQuery.dematerialiseCorrelated(session);

            return result;
        }

        throw Trace.error(Trace.WRONG_DATA_TYPE);
    }

    private Boolean testMatchCondition(Session session,
                                       Object[] data) throws HsqlException {

        int nulls;

        if (data == null) {
            return Boolean.TRUE;
        }

        nulls = countNulls(data);

        if (nulls != 0) {
            switch (exprType) {

                case MATCH_SIMPLE :
                case MATCH_UNIQUE_SIMPLE :
                    return Boolean.TRUE;

                case MATCH_PARTIAL :
                case MATCH_UNIQUE_PARTIAL :
                    if (nulls == data.length) {
                        return Boolean.TRUE;
                    }
                    break;

                case MATCH_FULL :
                case MATCH_UNIQUE_FULL :
                    return nulls == data.length ? Boolean.TRUE
                                                : Boolean.FALSE;
            }
        }

        if (eArg2.exprType == TABLE) {
            final int length   = eArg2.argList.length;
            boolean   hasMatch = false;

            for (int i = 0; i < length; i++) {
                Object[] rowData = eArg2.argList[i].getRowValue(session);
                Boolean  result  = compareValues(session, data, rowData);

                if (result == null ||!result.booleanValue()) {
                    continue;
                }

                switch (exprType) {

                    case MATCH_SIMPLE :
                    case MATCH_PARTIAL :
                    case MATCH_FULL :
                        return Boolean.TRUE;

                    case MATCH_UNIQUE_SIMPLE :
                    case MATCH_UNIQUE_PARTIAL :
                    case MATCH_UNIQUE_FULL :
                        if (hasMatch) {
                            return Boolean.FALSE;
                        }

                        hasMatch = true;
                }
            }

            return hasMatch ? Boolean.TRUE
                            : Boolean.FALSE;
        } else if (eArg2.exprType == TABLE_SUBQUERY) {
            if (eArg2.subQuery.isCorrelated) {
                eArg2.subQuery.materialise(session);
            }

            convertToType(session, data, eArg.argListDataType,
                          eArg2.argListDataType);

            if (nulls != 0
                    && (exprType == MATCH_PARTIAL
                        || exprType == MATCH_UNIQUE_PARTIAL)) {
                boolean hasMatch = false;
                RowIterator it =
                    eArg2.subQuery.table.getPrimaryIndex().firstRow(session);

                while (it.hasNext()) {
                    Object[] rowData = it.getNext().getData();
                    Boolean  result  = compareValues(session, data, rowData);

                    if (result.booleanValue()) {
                        if (exprType == MATCH_PARTIAL) {
                            return Boolean.TRUE;
                        }

                        if (hasMatch) {
                            return Boolean.FALSE;
                        }

                        hasMatch = true;
                    }
                }

                return hasMatch ? Boolean.TRUE
                                : Boolean.FALSE;
            }

            RowIterator it =
                eArg2.subQuery.table.getPrimaryIndex().findFirstRow(session,
                    data);
            boolean result = it.hasNext();

            if (!result) {
                eArg2.subQuery.dematerialiseCorrelated(session);

                return Boolean.FALSE;
            }

            switch (exprType) {

                case MATCH_SIMPLE :
                case MATCH_PARTIAL :
                case MATCH_FULL :
                    eArg2.subQuery.dematerialiseCorrelated(session);

                    return Boolean.TRUE;
            }

            it.getNext();

            result = it.hasNext();

            if (!result) {
                eArg2.subQuery.dematerialiseCorrelated(session);

                return Boolean.TRUE;
            }

            Object[] rowData = it.getNext().getData();
            Boolean returnValue =
                Boolean.TRUE.equals(compareValues(session, data, rowData))
                ? Boolean.FALSE
                : Boolean.TRUE;

            eArg2.subQuery.dematerialiseCorrelated(session);

            return returnValue;
        }

        throw Trace.error(Trace.WRONG_DATA_TYPE);
    }

    private Boolean testExistsCondition(Session session)
    throws HsqlException {

        if (!subQuery.isCorrelated) {
            return subQuery.table.isEmpty(session) ? Boolean.FALSE
                                                   : Boolean.TRUE;
        } else {
            Result r = subQuery.select.getResult(session, 1);    // 1 is already enough

            return r.getNavigator().isEmpty() ? Boolean.FALSE
                                              : Boolean.TRUE;
        }
    }

    private Boolean testAnyAllCondition(Session session,
                                        Object[] o) throws HsqlException {

        if (o == null) {
            return null;
        }

        SubQuery subquery = eArg2.eArg.subQuery;
        boolean  populate = subquery.isCorrelated;

        if (populate) {
            subquery.materialise(session);
        }

        Boolean result = getAnyAllValue(session, o, subquery);

        subquery.dematerialiseCorrelated(session);

        return result;
    }

    private Boolean getAnyAllValue(Session session, Object[] data,
                                   SubQuery subquery) throws HsqlException {

        boolean     empty    = subquery.table.isEmpty(session);
        Index       index    = subquery.table.getPrimaryIndex();
        RowIterator it       = index.findFirstRowNotNull(session);
        Row         firstrow = it.getNext();

        if (data[0] == null) {
            return null;
        }

        switch (eArg2.exprType) {

            case ANY : {
                if (empty) {
                    return Boolean.FALSE;
                }

                if (firstrow == null) {
                    return null;
                }

                int range =
                    eArg2.eArg.argListDataType[0].compareToTypeRange(data[0]);

                if (range != 0) {
                    switch (exprType) {

                        case EQUAL :
                            return Boolean.FALSE;

                        case NOT_EQUAL :
                            return Boolean.TRUE;

                        case GREATER :
                        case GREATER_EQUAL :
                            return range > 0 ? Boolean.TRUE
                                             : Boolean.FALSE;

                        case SMALLER_EQUAL :
                        case SMALLER :
                            return range < 0 ? Boolean.TRUE
                                             : Boolean.FALSE;
                    }
                }

                convertToType(session, data, eArg.argListDataType,
                              eArg2.eArg.argListDataType);

                if (exprType == EQUAL) {
                    it = index.findFirstRow(session, data);

                    return it.hasNext() ? Boolean.TRUE
                                        : Boolean.FALSE;
                }

                Row      lastrow   = index.lastRow(session);
                Object[] firstdata = firstrow.getData();
                Object[] lastdata  = lastrow.getData();
                Boolean comparefirst = compareValues(session, data,
                                                     firstdata);
                Boolean comparelast = compareValues(session, data, lastdata);

                switch (exprType) {

                    case NOT_EQUAL :
                        return comparefirst.booleanValue()
                               && comparelast.booleanValue() ? Boolean.FALSE
                                                             : Boolean.TRUE;

                    case GREATER :
                        return comparefirst;

                    case GREATER_EQUAL :
                        return comparefirst;

                    case SMALLER :
                        return comparelast;

                    case SMALLER_EQUAL :
                        return comparelast;
                }

                break;
            }
            case ALL : {
                if (empty) {
                    return Boolean.TRUE;
                }

                if (firstrow == null) {
                    return null;
                }

                int range =
                    eArg2.eArg.argListDataType[0].compareToTypeRange(data[0]);

                if (range != 0) {
                    switch (exprType) {

                        case EQUAL :
                            return Boolean.FALSE;

                        case NOT_EQUAL :
                            return Boolean.TRUE;

                        case GREATER :
                        case GREATER_EQUAL :
                            return range > 0 ? Boolean.TRUE
                                             : Boolean.FALSE;

                        case SMALLER_EQUAL :
                        case SMALLER :
                            return range < 0 ? Boolean.TRUE
                                             : Boolean.FALSE;
                    }
                }

                convertToType(session, data, eArg.argListDataType,
                              eArg2.eArg.argListDataType);

                if (exprType == EQUAL || exprType == NOT_EQUAL) {
                    it = index.findFirstRow(session, data);

                    if (exprType == EQUAL) {
                        return (it.hasNext() && subquery.table.getRowCount(session) == 1)
                               ? Boolean.TRUE
                               : Boolean.FALSE;
                    }

                    return (it.hasNext()) ? Boolean.FALSE
                                          : Boolean.TRUE;
                }

                Row      lastrow   = index.lastRow(session);
                Object[] firstdata = firstrow.getData();
                Object[] lastdata  = lastrow.getData();
                Boolean comparefirst = compareValues(session, data,
                                                     firstdata);
                Boolean comparelast = compareValues(session, data, lastdata);

                switch (exprType) {

                    case NOT_EQUAL :
                        return comparefirst.booleanValue()
                               && comparelast.booleanValue() ? Boolean.TRUE
                                                             : Boolean.FALSE;

                    case GREATER :
                        return comparelast;

                    case GREATER_EQUAL :
                        return comparelast;

                    case SMALLER :
                        return comparefirst;

                    case SMALLER_EQUAL :
                        return comparefirst;
                }

                break;
            }
        }

        return null;
    }

    /**
     * Returns a Select object that can be used for checking the contents
     * of an existing table against the given CHECK search condition.
     */
    static Select getCheckSelect(Session session, Table t,
                                 Expression e) throws HsqlException {

        CompileContext compileContext = new CompileContext();
        Select         s              = new Select(compileContext, true);

        s.exprColumns    = new Expression[1];
        s.exprColumns[0] = EXPR_TRUE;

        RangeVariable range = new RangeVariable(t, null, null,
            compileContext);

        s.rangeVariables = new RangeVariable[]{ range };

        OrderedHashSet set = e.resolveColumnReferences(s.rangeVariables,
            null);

        Select.checkColumnsResolved(set);
        e.resolveTypes(session, null);

        if (Types.SQL_BOOLEAN != e.getDataType().type) {
            throw Trace.error(Trace.NOT_A_CONDITION);
        }

        Expression condition = new Expression(NOT, e);

        s.queryCondition = condition;

        s.resolveTypesAndPrepare(session);

        return s;
    }

    /**
     * Sets the left leaf.
     */
    void setLeftExpression(Expression e) {
        eArg = e;
    }

    void setRightExpression(Expression e) {
        eArg2 = e;
    }

    /**
     * Gets the right leaf.
     */
    Expression getRightExpression() {
        return eArg2;
    }

    boolean isParam() {
        return isParam;
    }

    boolean isFixedConstant() {

        switch (exprType) {

            case VALUE :
                return !isParam;

            case NEGATE :
                return eArg.isFixedConstant();

            case ADD :
            case SUBTRACT :
            case MULTIPLY :
            case DIVIDE :
            case CONCAT :
                return eArg.isFixedConstant() && eArg2.isFixedConstant();
        }

        return false;
    }

    boolean isFixedConditional() {

        switch (exprType) {

            case VALUE :
                // CBB (2007-04-14 32:11:50 CMT)
                // CAREFUL : *both* cases have to be true.
                // FIXED: missing !isParam was causing NPE on things like
                //        cast(? as boolean) because getValue(Session)
                //        was entered erronously, finding
                //        session.compiledStatementExecutor.paramValues
                //        array null.
                // SEE ESPECIALLY:
                //        case CONVERT : of resolveTypes(Session session, Expression parent)
                return !isParam && (dataType.type == Types.SQL_BOOLEAN);

            case NOT_DISTINCT :
            case EQUAL :
            case GREATER_EQUAL :
            case GREATER :
            case SMALLER :
            case SMALLER_EQUAL :
            case NOT_EQUAL :
            case LIKE :

                //case IN : todo
                return eArg.isFixedConstant() && eArg2.isFixedConstant();

            case IS_NULL :
                return eArg.isFixedConstant();

            case NOT :
                return eArg.isFixedConditional();

            case AND :
            case OR :
                return eArg.isFixedConditional()
                       && eArg2.isFixedConditional();

            default :
                return false;
        }
    }

    void setTableColumnTypeAttributes(Expression e) {

        dataType    = e.dataType;
        isIdentity  = e.isIdentity;
        nullability = e.nullability;
        isWritable  = e.isWritable;
        schema      = e.schema;
    }

    void setAttributesAsColumn(Table t, int i) {

        columnIndex  = i;
        column       = t.getColumn(i);
        dataType     = column.getType();
        isWritable   = t.isWritable();
        isIdentity   = column.isIdentity();
        nullability = column.isNullable() &&!column.isPrimaryKey() ? NULLABLE
                                                                   : NO_NULLS;
        columnName   = column.columnName.name;
        columnQuoted = column.columnName.isNameQuoted;
        tableName    = t.getName().name;
        schema       = t.getSchemaName().name;
        isIndexed    = t.isIndexed(i);
    }

    String getValueClassName() {

        // boucherb@users 20050516 - patch 1.8.0 removed DITypeInfo dependency
        if (valueClassName == null) {
            Type type = dataType == null ? NullType.getNullType()
                                         : dataType;

            return type.getJDBCClassName();
        }

        return valueClassName;
    }

    public void collectAllBaseColumnExpressions(OrderedHashSet set) {

        Expression.collectAllExpressions(set, this, Expression.COLUMN);

        Iterator iterator = set.iterator();

        // calculate distinct column references
        while (iterator.hasNext()) {
            Expression expression = (Expression) iterator.next();

            if (expression.rangeVariable == null) {
                iterator.remove();

                continue;
            }

            Table table = expression.rangeVariable.getTable();

            if (table == null
                    || table.getTableType() == Table.SYSTEM_SUBQUERY) {
                iterator.remove();
            }
        }
    }

    public void collectAllFunctionExpressions(HashSet set) {
        Expression.collectAllExpressions(set, this, Expression.FUNCTION);
    }

    // parameter modes
    public static final int PARAM_UNKNOWN = 0;    // java.sql.ParameterMetaData.parameterModeUnknown
    public static final int PARAM_IN = 1;         // java.sql.ParameterMetaData.parameterModeIn
    public static final int PARAM_IN_OUT = 2;     // java.sql.ParameterMetaData.parameterModeInOut
    public static final int PARAM_OUT = 4;        // java.sql.ParameterMetaData.parameterModeOut

    // result set (output column value) or parameter expression nullability
    static final byte NO_NULLS = 0;               // java.sql.ResultSetMetaData.columnNoNulls
    static final byte NULLABLE = 1;               // java.sql.ResultSetMetaData.columnNullable
    static final byte NULLABLE_UNKNOWN = 2;       // java.sql.ResultSetMetaData.columnNullableUnknown

    // output column and parameter expression metadata values
    boolean isIdentity;                           // = false
    byte    nullability = NULLABLE_UNKNOWN;
    boolean isWritable;                           // = false; true if column of writable table
    byte    paramMode = PARAM_UNKNOWN;
    String  valueClassName;                       // = null

// boucherb@users 20040111 - patch 1.7.2 RC1 - metadata xxxusage support
//-------------------------------------------------------------------
    // TODO:  Maybe provide an interface or abstract class + a default
    // implementation instead?  This would allow a more powerful system
    // of collectors to be created, for example to assist in the optimization
    // of condition expression trees:
    //
    // HashSet joins = new JoinConditionCollector();
    // joins.addAll(select.whereCondition);
    // for(Iterator it = joins.iterator(); it.hasNext();) {
    //      process((it.next());
    // }
    static void collectAllExpressions(HashSet set, Select select, int type) {

        for (; select != null; select = select.unionSelect) {
            Expression[] list = select.exprColumns;

            for (int i = 0; i < select.orderByLimitIndex; i++) {
                collectAllExpressions(set, list[i], type);
            }

            collectAllExpressions(set, select.queryCondition, type);
            collectAllExpressions(set, select.havingCondition, type);
        }
    }

    static void collectAllExpressions(HashSet set, Expression e, int type) {

        Expression[] list;

        if (e == null) {
            return;
        }

        collectAllExpressions(set, e.getArg(), type);
        collectAllExpressions(set, e.getArg2(), type);

        if (e.exprType == type) {
            set.add(e);
        }

        if (e.subQuery != null) {
            collectAllExpressions(set, e.subQuery.select, type);
        }

        if (e.exprType == FUNCTION || e.exprType == SQL_FUNCTION) {
            if (e.argList != null) {
                for (int i = 0; i < e.argList.length; i++) {
                    collectAllExpressions(set, e.argList[i], type);
                }
            }
        }

        list = e.argList;

        if (list != null) {
            for (int i = 0; i < list.length; i++) {
                collectAllExpressions(set, list[i], type);
            }
        }
    }

    /**
     * collect all extrassions of a set of expression types appearing anywhere
     * in a select statement and its subselects, etc.
     */
    static void collectAllExpressions(HashSet set, Select select,
                                      OrderedIntHashSet typeSet,
                                      OrderedIntHashSet stopAtTypeSet) {

        for (; select != null; select = select.unionSelect) {
            Expression[] list = select.exprColumns;

            for (int i = 0; i < select.orderByLimitIndex; i++) {
                collectAllExpressions(set, list[i], typeSet, stopAtTypeSet);
            }

            collectAllExpressions(set, select.queryCondition, typeSet,
                                  stopAtTypeSet);
            collectAllExpressions(set, select.havingCondition, typeSet,
                                  stopAtTypeSet);

            // todo order by columns
        }
    }

    static void collectAllExpressions(HashSet set, Expression e,
                                      OrderedIntHashSet typeSet,
                                      OrderedIntHashSet stopAtTypeSet) {

        Expression[] list;

        if (e == null) {
            return;
        }

        if (stopAtTypeSet.contains(e.exprType)) {
            return;
        }

        collectAllExpressions(set, e.getArg(), typeSet, stopAtTypeSet);
        collectAllExpressions(set, e.getArg2(), typeSet, stopAtTypeSet);

        if (typeSet.contains(e.exprType)) {
            set.add(e);
        }

        if (e.subQuery != null) {
            collectAllExpressions(set, e.subQuery.select, typeSet,
                                  stopAtTypeSet);
        }

        if (e.exprType == FUNCTION || e.exprType == SQL_FUNCTION) {
            if (e.argList != null) {
                for (int i = 0; i < e.argList.length; i++) {
                    collectAllExpressions(set, e.argList[i], typeSet,
                                          stopAtTypeSet);
                }
            }
        }

        list = e.argList;

        if (list != null) {
            for (int i = 0; i < list.length; i++) {
                collectAllExpressions(set, list[i], typeSet, stopAtTypeSet);
            }
        }
    }

    /**
     * Returns the schema name for a column expression as a string
     */
    String getSchemaName() {
        return schema;
    }

    /**
     * isCorrelated
     */
    public boolean isCorrelated() {

        if (exprType == TABLE_SUBQUERY && subQuery != null
                && subQuery.isCorrelated) {
            return true;
        }

        return false;
    }

    /**
     * checkValidCheckConstraint
     */
    public void checkValidCheckConstraint() throws HsqlException {

        HashSet set = new HashSet();

        Expression.collectAllExpressions(set, this,
                                         Expression.TABLE_SUBQUERY);

        if (!set.isEmpty()) {
            throw Trace.error(Trace.OPERATION_NOT_SUPPORTED,
                              "CHECK CONSTRAINT");
        }
    }

    /**
     * Converts an OR containing an AND to an AND
     */
    void distributeOr() {

        if (exprType != Expression.OR) {
            return;
        }

        if (eArg.exprType == Expression.AND) {
            exprType = Expression.AND;

            Expression temp = new Expression(Expression.OR, eArg.eArg2,
                                             eArg2);

            eArg.exprType = Expression.OR;
            eArg.eArg2    = eArg2;
            eArg2         = temp;
        } else if (eArg2.exprType == Expression.AND) {
            Expression temp = eArg;

            eArg  = eArg2;
            eArg2 = temp;

            distributeOr();

            return;
        }

        eArg.distributeOr();
        eArg2.distributeOr();
    }

    /**
     *
     */
    Expression getIndexableExpression(Session session,
                                      RangeVariable rangeVar)
                                      throws HsqlException {

        switch (exprType) {

            case Expression.IN :
                if (eArg2.isCorrelated) {
                    return null;
                }

                return eArg.argList[0].exprType == COLUMN
                       && eArg.argList[0].rangeVariable == rangeVar ? this
                                                                    : null;

            case Expression.IS_NULL :
                return eArg.exprType == COLUMN
                       && eArg.rangeVariable == rangeVar ? this
                                                         : null;

            case Expression.EQUAL :
            case Expression.GREATER :
            case Expression.GREATER_EQUAL :
            case Expression.SMALLER :
            case Expression.SMALLER_EQUAL :
                reorderComparison(session);

                if (eArg.exprType == COLUMN
                        && eArg.rangeVariable == rangeVar) {
                    if (eArg2.hasReference(rangeVar)) {
                        return null;
                    }

                    return this;
                }

                if (eArg2.exprType == COLUMN
                        && eArg2.rangeVariable == rangeVar) {
                    swapCondition();

                    if (eArg2.hasReference(rangeVar)) {
                        return null;
                    }

                    return this;
                }
            default :
                return null;
        }
    }

    /**
     * Called only on comparison expressions after reordering which have
     * a COLUMN left leaf
     */
    boolean isSimpleBound() {

        if (exprType == Expression.IS_NULL) {
            return true;
        }

        if (eArg2 != null) {
            if (eArg2.exprType == Expression.VALUE) {
                return true;
            }

            if (eArg2.exprType == Expression.SQL_FUNCTION) {
                if (((SQLFunction) eArg2).isValueFunction()) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Swap the condition with its complement
     */
    void swapCondition() {

        int i = EQUAL;

        switch (exprType) {

            case GREATER_EQUAL :
                i = SMALLER_EQUAL;
                break;

            case SMALLER_EQUAL :
                i = GREATER_EQUAL;
                break;

            case SMALLER :
                i = GREATER;
                break;

            case GREATER :
                i = SMALLER;
                break;

            case NOT_DISTINCT :
                i = NOT_DISTINCT;
                break;

            case EQUAL :
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "Expression.swapCondition");
        }

        exprType = i;

        Expression e = eArg;

        eArg  = eArg2;
        eArg2 = e;
    }

    boolean reorderComparison(Session session) throws HsqlException {

        Expression colExpression    = null;
        Expression nonColExpression = null;
        boolean    left             = false;
        boolean    replaceColumn    = false;
        int        operation        = 0;

        if (eArg.exprType == Expression.ADD) {
            operation = Expression.SUBTRACT;
            left      = true;
        } else if (eArg.exprType == Expression.SUBTRACT) {
            operation = Expression.ADD;
            left      = true;
        } else if (eArg2.exprType == Expression.ADD) {
            operation = Expression.SUBTRACT;
        } else if (eArg2.exprType == Expression.SUBTRACT) {
            operation = Expression.ADD;
        }

        if (operation == 0) {
            return false;
        }

        if (left) {
            if (eArg.eArg.exprType == Expression.COLUMN) {
                colExpression    = eArg.eArg;
                nonColExpression = eArg.eArg2;
            } else if (eArg.eArg2.exprType == Expression.COLUMN) {
                replaceColumn    = operation == Expression.ADD;
                colExpression    = eArg.eArg2;
                nonColExpression = eArg.eArg;
            }
        } else {
            if (eArg2.eArg.exprType == Expression.COLUMN) {
                colExpression    = eArg2.eArg;
                nonColExpression = eArg2.eArg2;
            } else if (eArg2.eArg2.exprType == Expression.COLUMN) {
                replaceColumn    = operation == Expression.ADD;
                colExpression    = eArg2.eArg2;
                nonColExpression = eArg2.eArg;
            }
        }

        if (colExpression == null) {
            return false;
        }

        Expression otherExpression = left ? eArg2
                                          : eArg;
        Expression newArg          = null;

        if (!replaceColumn) {
            newArg = new Expression(operation, otherExpression,
                                    nonColExpression);

            newArg.resolveTypesForArithmetic(session);
        }

        if (left) {
            if (replaceColumn) {
                eArg2      = colExpression;
                eArg.eArg2 = otherExpression;

                eArg.resolveTypesForArithmetic(session);
            } else {
                eArg  = colExpression;
                eArg2 = newArg;
            }
        } else {
            if (replaceColumn) {
                eArg        = colExpression;
                eArg2.eArg2 = otherExpression;

                eArg2.resolveTypesForArithmetic(session);
            } else {
                eArg2 = colExpression;
                eArg  = newArg;
            }
        }

        return true;
    }
}
