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
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.HsqlList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.LongDeque;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.OrderedIntKeyHashMap;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.result.ResultProperties;
import org.hsqldb.store.BitMap;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.ArrayType;
import org.hsqldb.types.BlobType;
import org.hsqldb.types.Charset;
import org.hsqldb.types.DTIType;
import org.hsqldb.types.IntervalType;
import org.hsqldb.types.NumberType;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;

/**
 * Parser for DQL statements
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.0
 * @since 1.9.0
 */
public class ParserDQL extends ParserBase {

    protected Database             database;
    protected Session              session;
    protected final CompileContext compileContext;
    HsqlException                  lastError;

    /**
     *  Constructs a new Parser object with the given context.
     *
     * @param  session the connected context
     * @param  t the token source from which to parse commands
     */
    ParserDQL(Session session, Scanner t) {

        super(t);

        this.session   = session;
        database       = session.getDatabase();
        compileContext = new CompileContext(session, this);
    }

    /**
     *  Resets this parse context with the given SQL character sequence.
     *
     * @param sql a new SQL character sequence to replace the current one
     */
    void reset(String sql) {

        super.reset(sql);
        compileContext.reset();

        lastError = null;
    }

    void checkIsSchemaObjectName() {

        if (database.sqlEnforceNames) {
            checkIsNonReservedIdentifier();
        } else {
            checkIsNonCoreReservedIdentifier();
        }
    }

    Type readTypeDefinition(boolean includeUserTypes) {

        int     typeNumber = Integer.MIN_VALUE;
        boolean hasLength  = false;
        boolean hasScale   = false;

        checkIsIdentifier();

        if (token.namePrefix == null) {
            typeNumber = Type.getTypeNr(token.tokenString);
        }

        if (typeNumber == Integer.MIN_VALUE) {
            if (includeUserTypes) {
                checkIsSchemaObjectName();

                String schemaName = session.getSchemaName(token.namePrefix);
                Type type = database.schemaManager.getDomain(token.tokenString,
                    schemaName, false);

                if (type != null) {
                    getRecordedToken().setExpression(type);
                    compileContext.addSchemaObject(type);
                    read();

                    return type;
                }
            }

            throw Error.error(ErrorCode.X_42509, token.tokenString);
        }

        read();

        switch (typeNumber) {

            case Types.SQL_CHAR :
                if (token.tokenType == Tokens.VARYING) {
                    read();

                    typeNumber = Types.SQL_VARCHAR;
                } else if (token.tokenType == Tokens.LARGE) {
                    readThis(Tokens.OBJECT);
                    read();

                    typeNumber = Types.SQL_CLOB;
                }
                break;

            case Types.SQL_DOUBLE :
                if (token.tokenType == Tokens.PRECISION) {
                    read();
                }
                break;

            case Types.SQL_BINARY :
                if (token.tokenType == Tokens.VARYING) {
                    read();

                    typeNumber = Types.SQL_VARBINARY;
                } else if (token.tokenType == Tokens.LARGE) {
                    readThis(Tokens.OBJECT);
                    read();

                    typeNumber = Types.SQL_BLOB;
                }
                break;

            case Types.SQL_BIT :
                if (token.tokenType == Tokens.VARYING) {
                    read();

                    typeNumber = Types.SQL_BIT_VARYING;
                }
                break;

            case Types.SQL_INTERVAL :
                return readIntervalType(false);

            default :
        }

        long length = typeNumber == Types.SQL_TIMESTAMP
                      ? DTIType.defaultTimestampFractionPrecision
                      : 0;
        int scale = 0;

        if (Types.requiresPrecision(typeNumber)
                && token.tokenType != Tokens.OPENBRACKET
                && database.sqlEnforceSize && !session.isProcessingScript) {
            throw Error.error(ErrorCode.X_42599,
                              Type.getDefaultType(typeNumber).getNameString());
        }

        if (Types.acceptsPrecision(typeNumber)) {
            if (token.tokenType == Tokens.OPENBRACKET) {
                int multiplier = 1;

                read();

                switch (token.tokenType) {

                    case Tokens.X_VALUE :
                        if (token.dataType.typeCode != Types.SQL_INTEGER
                                && token.dataType.typeCode
                                   != Types.SQL_BIGINT) {
                            throw unexpectedToken();
                        }
                        break;

                    case Tokens.X_LOB_SIZE :
                        if (typeNumber == Types.SQL_BLOB
                                || typeNumber == Types.SQL_CLOB) {
                            switch (token.lobMultiplierType) {

                                case Tokens.K :
                                    multiplier = 1024;
                                    break;

                                case Tokens.M :
                                    multiplier = 1024 * 1024;
                                    break;

                                case Tokens.G :
                                    multiplier = 1024 * 1024 * 1024;
                                    break;

                                case Tokens.P :
                                case Tokens.T :
                                default :
                                    throw unexpectedToken();
                            }

                            break;
                        } else {
                            throw unexpectedToken(token.getFullString());
                        }
                    default :
                        throw unexpectedToken();
                }

                hasLength = true;
                length    = ((Number) token.tokenValue).longValue();

                if (length < 0
                        || (length == 0
                            && !Types.acceptsZeroPrecision(typeNumber))) {
                    throw Error.error(ErrorCode.X_42592);
                }

                length *= multiplier;

                read();

                if (typeNumber == Types.SQL_CHAR
                        || typeNumber == Types.SQL_VARCHAR
                        || typeNumber == Types.SQL_CLOB) {
                    if (token.tokenType == Tokens.CHARACTERS) {
                        read();
                    } else if (token.tokenType == Tokens.OCTETS) {
                        read();

                        length /= 2;
                    }
                }

                if (Types.acceptsScaleCreateParam(typeNumber)
                        && token.tokenType == Tokens.COMMA) {
                    read();

                    scale = readInteger();

                    if (scale < 0) {
                        throw Error.error(ErrorCode.X_42592);
                    }

                    hasScale = true;
                }

                readThis(Tokens.CLOSEBRACKET);
            } else if (typeNumber == Types.SQL_BIT) {
                length = 1;
            } else if (typeNumber == Types.SQL_BLOB
                       || typeNumber == Types.SQL_CLOB) {
                length = BlobType.defaultBlobSize;
            } else if (database.sqlEnforceSize) {

                // BIT is always BIT(1), regardless of sqlEnforceSize
                if (typeNumber == Types.SQL_CHAR
                        || typeNumber == Types.SQL_BINARY) {
                    length = 1;
                }
            }

            if (typeNumber == Types.SQL_TIMESTAMP
                    || typeNumber == Types.SQL_TIME) {
                if (length > DTIType.maxFractionPrecision) {
                    throw Error.error(ErrorCode.X_42592);
                }

                scale  = (int) length;
                length = 0;

                if (token.tokenType == Tokens.WITH) {
                    read();
                    readThis(Tokens.TIME);
                    readThis(Tokens.ZONE);

                    if (typeNumber == Types.SQL_TIMESTAMP) {
                        typeNumber = Types.SQL_TIMESTAMP_WITH_TIME_ZONE;
                    } else {
                        typeNumber = Types.SQL_TIME_WITH_TIME_ZONE;
                    }
                } else if (token.tokenType == Tokens.WITHOUT) {
                    read();
                    readThis(Tokens.TIME);
                    readThis(Tokens.ZONE);
                }
            }
        }

        switch (typeNumber) {

            case Types.LONGVARCHAR : {
                typeNumber = Types.SQL_VARCHAR;

                if (!hasLength) {
                    length = 1024 * 1024;
                }

                break;
            }
            case Types.LONGVARBINARY : {
                typeNumber = Types.SQL_VARBINARY;

                if (!hasLength) {
                    length = 1024 * 1024;
                }

                break;
            }
            case Types.SQL_CHAR :
            case Types.SQL_BINARY :
                break;

            case Types.SQL_VARCHAR :
            case Types.SQL_VARBINARY :
                if (!hasLength) {
                    length = 32 * 1024;
                }
                break;

            case Types.SQL_DECIMAL :
            case Types.SQL_NUMERIC :
                if (!hasLength && !hasScale && !database.sqlEnforceSize) {
                    length = NumberType.defaultNumericPrecision;
                    scale  = NumberType.defaultNumericScale;
                }
                break;
        }

        if (session.ignoreCase && typeNumber == Types.SQL_VARCHAR) {
            typeNumber = Types.VARCHAR_IGNORECASE;
        }

        Type typeObject = Type.getType(typeNumber, 0, length, scale);

        if (typeObject.isCharacterType()) {
            if (token.tokenType == Tokens.CHARACTER) {
                read();
                readThis(Tokens.SET);
                checkIsSchemaObjectName();

                String schemaName = session.getSchemaName(token.namePrefix);
                Charset charset =
                    (Charset) database.schemaManager.getSchemaObject(
                        token.tokenString, schemaName, SchemaObject.CHARSET);

                read();
            }
        }

        if (token.tokenType == Tokens.ARRAY) {
            if (typeObject.isLobType()) {
                throw unexpectedToken();
            }

            read();

            int maxCardinality = Type.defaultArrayCardinality;

            if (token.tokenType == Tokens.LEFTBRACKET) {
                read();

                maxCardinality = readInteger();

                if (scale < 0) {
                    throw Error.error(ErrorCode.X_42592);
                }

                readThis(Tokens.RIGHTBRACKET);
            }

            typeObject = new ArrayType(typeObject, maxCardinality);
        }

        return typeObject;
    }

    void readSimpleColumnNames(OrderedHashSet columns,
                               RangeVariable rangeVar) {

        while (true) {
            ColumnSchema col = readSimpleColumnName(rangeVar);

            if (!columns.add(col.getName().name)) {
                throw Error.error(ErrorCode.X_42579, col.getName().name);
            }

            if (readIfThis(Tokens.COMMA)) {
                continue;
            }

            if (token.tokenType == Tokens.CLOSEBRACKET) {
                break;
            }

            throw unexpectedToken();
        }
    }

    void readTargetSpecificationList(OrderedHashSet targets,
                                     RangeVariable[] rangeVars,
                                     LongDeque colIndexList) {

        while (true) {
            Expression target = XreadTargetSpecification(rangeVars,
                colIndexList);

            if (!targets.add(target)) {
                ColumnSchema col = target.getColumn();

                throw Error.error(ErrorCode.X_42579, col.getName().name);
            }

            if (readIfThis(Tokens.COMMA)) {
                continue;
            }

            if (token.tokenType == Tokens.CLOSEBRACKET) {
                break;
            }

            if (token.tokenType == Tokens.FROM) {
                break;
            }

            throw unexpectedToken();
        }
    }

    void readSimpleColumnNames(OrderedHashSet columns, Table table) {

        while (true) {
            ColumnSchema col = readSimpleColumnName(table);

            if (!columns.add(col.getName().name)) {
                throw Error.error(ErrorCode.X_42577, col.getName().name);
            }

            if (readIfThis(Tokens.COMMA)) {
                continue;
            }

            if (token.tokenType == Tokens.CLOSEBRACKET) {
                break;
            }

            throw unexpectedToken();
        }
    }

    HsqlName[] readColumnNames(HsqlName tableName) {

        BitMap         quotedFlags = new BitMap(32);
        OrderedHashSet set         = readColumnNames(quotedFlags, false);
        HsqlName[]     colList     = new HsqlName[set.size()];

        for (int i = 0; i < colList.length; i++) {
            String  name   = (String) set.get(i);
            boolean quoted = quotedFlags.isSet(i);

            colList[i] = database.nameManager.newHsqlName(tableName.schema,
                    name, quoted, SchemaObject.COLUMN, tableName);
        }

        return colList;
    }

    OrderedHashSet readColumnNames(boolean readAscDesc) {
        return readColumnNames(null, readAscDesc);
    }

    OrderedHashSet readColumnNames(BitMap quotedFlags, boolean readAscDesc) {

        readThis(Tokens.OPENBRACKET);

        OrderedHashSet set = new OrderedHashSet();

        readColumnNameList(set, quotedFlags, readAscDesc);
        readThis(Tokens.CLOSEBRACKET);

        return set;
    }

    void readColumnNameList(OrderedHashSet set, BitMap quotedFlags,
                            boolean readAscDesc) {

        int i = 0;

        while (true) {
            if (session.isProcessingScript) {

                // for old scripts
                if (!isSimpleName()) {
                    token.isDelimitedIdentifier = true;
                }
            } else {
                checkIsSimpleName();
            }

            if (!set.add(token.tokenString)) {
                throw Error.error(ErrorCode.X_42577, token.tokenString);
            }

            if (quotedFlags != null && isDelimitedIdentifier()) {
                quotedFlags.set(i);
            }

            read();

            i++;

            if (readAscDesc) {
                if (token.tokenType == Tokens.ASC
                        || token.tokenType == Tokens.DESC) {
                    read();
                }
            }

            if (readIfThis(Tokens.COMMA)) {
                continue;
            }

            break;
        }
    }

    SimpleName[] readColumnNameList(OrderedHashSet set) {

        BitMap columnNameQuoted = new BitMap(32);

        readThis(Tokens.OPENBRACKET);
        readColumnNameList(set, columnNameQuoted, false);
        readThis(Tokens.CLOSEBRACKET);

        SimpleName[] columnNameList = new SimpleName[set.size()];

        for (int i = 0; i < set.size(); i++) {
            SimpleName name =
                HsqlNameManager.getSimpleName((String) set.get(i),
                                              columnNameQuoted.isSet(i));

            columnNameList[i] = name;
        }

        return columnNameList;
    }

    int XreadUnionType() {

        int unionType = QueryExpression.NOUNION;

        switch (token.tokenType) {

            case Tokens.UNION :
                read();

                unionType = QueryExpression.UNION;

                if (token.tokenType == Tokens.ALL) {
                    unionType = QueryExpression.UNION_ALL;

                    read();
                } else if (token.tokenType == Tokens.DISTINCT) {
                    read();
                }
                break;

            case Tokens.INTERSECT :
                read();

                unionType = QueryExpression.INTERSECT;

                if (token.tokenType == Tokens.ALL) {
                    unionType = QueryExpression.INTERSECT_ALL;

                    read();
                } else if (token.tokenType == Tokens.DISTINCT) {
                    read();
                }
                break;

            case Tokens.EXCEPT :
            case Tokens.MINUS_EXCEPT :
                read();

                unionType = QueryExpression.EXCEPT;

                if (token.tokenType == Tokens.ALL) {
                    unionType = QueryExpression.EXCEPT_ALL;

                    read();
                } else if (token.tokenType == Tokens.DISTINCT) {
                    read();
                }
                break;

            default :
                break;
        }

        return unionType;
    }

    void XreadUnionCorrespondingClause(QueryExpression queryExpression) {

        if (token.tokenType == Tokens.CORRESPONDING) {
            read();
            queryExpression.setUnionCorresoponding();

            if (token.tokenType == Tokens.BY) {
                read();

                OrderedHashSet names = readColumnNames(false);

                queryExpression.setUnionCorrespondingColumns(names);
            }
        }
    }

    QueryExpression XreadQueryExpression() {

        if (token.tokenType == Tokens.WITH) {
            read();

            if (token.tokenType == Tokens.RECURSIVE) {
                throw super.unsupportedFeature();
            }

            compileContext.initSubqueryNames();

            while (true) {
                checkIsSimpleName();

                HsqlName[] nameList = null;
                HsqlName queryName =
                    database.nameManager.newHsqlName(token.tokenString,
                                                     isDelimitedIdentifier(),
                                                     SchemaObject.SUBQUERY);

                queryName.schema = SqlInvariants.SYSTEM_SUBQUERY_HSQLNAME;

                read();

                if (token.tokenType == Tokens.OPENBRACKET) {
                    nameList = readColumnNames(queryName);
                }

                readThis(Tokens.AS);
                readThis(Tokens.OPENBRACKET);

                SubQuery subQuery = XreadTableNamedSubqueryBody(queryName,
                    nameList);

                readThis(Tokens.CLOSEBRACKET);
                compileContext.registerSubquery(queryName.name, subQuery);

                if (token.tokenType == Tokens.COMMA) {
                    read();

                    continue;
                }

                break;
            }
        }

        QueryExpression queryExpression = XreadQueryExpressionBody();
        SortAndSlice    sortAndSlice    = XreadOrderByExpression();

        if (queryExpression.sortAndSlice == null) {
            queryExpression.addSortAndSlice(sortAndSlice);
        } else {
            if (queryExpression.sortAndSlice.hasLimit()) {
                if (sortAndSlice.hasLimit()) {
                    throw Error.error(ErrorCode.X_42549);
                }

                for (int i = 0; i < sortAndSlice.exprList.size(); i++) {
                    Expression e = (Expression) sortAndSlice.exprList.get(i);

                    queryExpression.sortAndSlice.addOrderExpression(e);
                }
            } else {
                queryExpression.addSortAndSlice(sortAndSlice);
            }
        }

        return queryExpression;
    }

    QueryExpression XreadQueryExpressionBody() {

        QueryExpression queryExpression = XreadQueryTerm();

        while (true) {
            switch (token.tokenType) {

                case Tokens.UNION :
                case Tokens.EXCEPT :
                case Tokens.MINUS_EXCEPT : {
                    queryExpression = XreadSetOperation(queryExpression);

                    break;
                }
                default : {
                    return queryExpression;
                }
            }
        }
    }

    QueryExpression XreadQueryTerm() {

        QueryExpression queryExpression = XreadQueryPrimary();

        while (true) {
            if (token.tokenType == Tokens.INTERSECT) {
                queryExpression = XreadSetOperation(queryExpression);
            } else {
                return queryExpression;
            }
        }
    }

    private QueryExpression XreadSetOperation(
            QueryExpression queryExpression) {

        queryExpression = new QueryExpression(compileContext, queryExpression);

        int unionType = XreadUnionType();

        XreadUnionCorrespondingClause(queryExpression);

        QueryExpression rightQueryExpression = XreadQueryTerm();

        queryExpression.addUnion(rightQueryExpression, unionType);

        return queryExpression;
    }

    QueryExpression XreadQueryPrimary() {

        switch (token.tokenType) {

            case Tokens.TABLE :
            case Tokens.VALUES :
            case Tokens.SELECT : {
                QuerySpecification select = XreadSimpleTable();

                return select;
            }
            case Tokens.OPENBRACKET : {
                read();

                QueryExpression queryExpression = XreadQueryExpressionBody();
                SortAndSlice    sortAndSlice    = XreadOrderByExpression();

                readThis(Tokens.CLOSEBRACKET);

                if (queryExpression.sortAndSlice == null) {
                    queryExpression.addSortAndSlice(sortAndSlice);
                } else {
                    if (queryExpression.sortAndSlice.hasLimit()) {
                        if (sortAndSlice.hasLimit()) {
                            throw Error.error(ErrorCode.X_42549);
                        }

                        for (int i = 0; i < sortAndSlice.exprList.size();
                                i++) {
                            Expression e =
                                (Expression) sortAndSlice.exprList.get(i);

                            queryExpression.sortAndSlice.addOrderExpression(e);
                        }
                    } else {
                        queryExpression.addSortAndSlice(sortAndSlice);
                    }
                }

                return queryExpression;
            }
            default : {
                throw unexpectedToken();
            }
        }
    }

    QuerySpecification XreadSimpleTable() {

        QuerySpecification select;

        switch (token.tokenType) {

            case Tokens.TABLE : {
                read();

                Table table = readTableName();

                select = new QuerySpecification(session, table,
                                                compileContext);

                break;
            }
            case Tokens.VALUES : {
                read();

                SubQuery sq = XreadRowValueExpressionList();

                select = new QuerySpecification(session, sq.getTable(),
                                                compileContext);

                break;
            }
            case Tokens.SELECT : {
                select = XreadQuerySpecification();

                break;
            }
            default : {
                throw unexpectedToken();
            }
        }

        return select;
    }

    QuerySpecification XreadQuerySpecification() {

        QuerySpecification select = XreadSelect();

        XreadTableExpression(select);

        return select;
    }

    void XreadTableExpression(QuerySpecification select) {
        XreadFromClause(select);
        readWhereGroupHaving(select);
    }

    QuerySpecification XreadSelect() {

        QuerySpecification select = new QuerySpecification(compileContext);

        readThis(Tokens.SELECT);

        if (token.tokenType == Tokens.TOP || token.tokenType == Tokens.LIMIT) {
            SortAndSlice sortAndSlice = XreadTopOrLimit();

            if (sortAndSlice != null) {
                select.addSortAndSlice(sortAndSlice);
            }
        }

        if (token.tokenType == Tokens.DISTINCT) {
            select.isDistinctSelect = true;

            read();
        } else if (token.tokenType == Tokens.ALL) {
            read();
        }

        while (true) {
            Expression e = XreadValueExpression();

            if (token.tokenType == Tokens.AS) {
                read();
                checkIsNonCoreReservedIdentifier();
            }

            if (isNonCoreReservedIdentifier()) {
                e.setAlias(HsqlNameManager.getSimpleName(token.tokenString,
                        isDelimitedIdentifier()));
                read();
            }

            select.addSelectColumnExpression(e);

            if (token.tokenType == Tokens.FROM) {
                break;
            }

            if (token.tokenType == Tokens.INTO) {
                break;
            }

            if (readIfThis(Tokens.COMMA)) {
                continue;
            }

            throw unexpectedToken();
        }

        return select;
    }

    void XreadFromClause(QuerySpecification select) {

        readThis(Tokens.FROM);

        while (true) {
            XreadTableReference(select);

            if (readIfThis(Tokens.COMMA)) {
                continue;
            }

            break;
        }
    }

    void XreadTableReference(QuerySpecification select) {

        boolean       natural = false;
        RangeVariable range   = readTableOrSubquery();

        select.addRangeVariable(range);

        while (true) {
            int     type  = token.tokenType;
            boolean left  = false;
            boolean right = false;
            boolean end   = false;

            type = token.tokenType;

            switch (token.tokenType) {

                case Tokens.INNER :
                    read();
                    readThis(Tokens.JOIN);
                    break;

                case Tokens.CROSS :
                    if (natural) {
                        throw unexpectedToken();
                    }

                    read();
                    readThis(Tokens.JOIN);
                    break;

                case Tokens.UNION :
                    if (natural) {
                        throw unexpectedToken();
                    }

                    int position = getPosition();

                    read();

                    if (token.tokenType == Tokens.JOIN) {
                        read();

                        break;
                    } else {
                        rewind(position);

                        end = true;

                        break;
                    }
                case Tokens.NATURAL :
                    if (natural) {
                        throw unexpectedToken();
                    }

                    read();

                    natural = true;

                    continue;
                case Tokens.LEFT :
                    read();
                    readIfThis(Tokens.OUTER);
                    readThis(Tokens.JOIN);

                    left = true;
                    break;

                case Tokens.RIGHT :
                    read();
                    readIfThis(Tokens.OUTER);
                    readThis(Tokens.JOIN);

                    right = true;
                    break;

                case Tokens.FULL :
                    read();
                    readIfThis(Tokens.OUTER);
                    readThis(Tokens.JOIN);

                    left  = true;
                    right = true;
                    break;

                case Tokens.JOIN :
                    read();

                    type = Tokens.INNER;
                    break;

                case Tokens.COMMA :
                    if (natural) {
                        throw unexpectedToken();
                    }

                    read();

                    type = Tokens.COMMA;
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

                case Tokens.COMMA :
                    range.isBoundary = true;

                    select.addRangeVariable(range);
                    break;

                case Tokens.CROSS :
                    select.addRangeVariable(range);
                    break;

                case Tokens.UNION :
                    select.addRangeVariable(range);

                    condition = Expression.EXPR_FALSE;

                    range.setJoinType(true, true);
                    break;

                case Tokens.LEFT :
                case Tokens.RIGHT :
                case Tokens.INNER :
                case Tokens.FULL : {
                    if (natural) {
                        OrderedHashSet columns =
                            range.getUniqueColumnNameSet();

                        condition = select.getEquiJoinExpressions(columns,
                                range, false);

                        select.addRangeVariable(range);
                    } else if (token.tokenType == Tokens.USING) {
                        read();

                        OrderedHashSet columns = new OrderedHashSet();

                        readThis(Tokens.OPENBRACKET);
                        readSimpleColumnNames(columns, range);
                        readThis(Tokens.CLOSEBRACKET);

                        condition = select.getEquiJoinExpressions(columns,
                                range, true);

                        select.addRangeVariable(range);
                    } else if (token.tokenType == Tokens.ON) {
                        read();

                        condition = XreadBooleanValueExpression();

                        select.addRangeVariable(range);
                    } else {
                        throw unexpectedToken();
                    }

                    range.setJoinType(left, right);

                    break;
                }
            }

            range.addJoinCondition(condition);

            natural = false;
        }
    }

    Expression getRowExpression(OrderedHashSet columnNames) {

        Expression[] elements = new Expression[columnNames.size()];

        for (int i = 0; i < elements.length; i++) {
            String name = (String) columnNames.get(i);

            elements[i] = new ExpressionColumn(null, null, name, false);
        }

        return new Expression(OpTypes.ROW, elements);
    }

    void readWhereGroupHaving(QuerySpecification select) {

        // where
        if (token.tokenType == Tokens.WHERE) {
            read();

            Expression e = XreadBooleanValueExpression();

            select.addQueryCondition(e);
        }

        // group by
        if (token.tokenType == Tokens.GROUP) {
            read();
            readThis(Tokens.BY);

            while (true) {
                Expression e = XreadValueExpression();

                select.addGroupByColumnExpression(e);

                if (token.tokenType == Tokens.COMMA) {
                    read();

                    continue;
                }

                break;
            }
        }

        // having
        if (token.tokenType == Tokens.HAVING) {
            read();

            Expression e = XreadBooleanValueExpression();

            select.addHavingExpression(e);
        }
    }

    SortAndSlice XreadOrderByExpression() {

        SortAndSlice sortAndSlice = null;

        if (token.tokenType == Tokens.ORDER) {
            read();
            readThis(Tokens.BY);

            sortAndSlice = XreadOrderBy();
        }

        if (token.tokenType == Tokens.LIMIT || token.tokenType == Tokens.FETCH
                || token.tokenType == Tokens.OFFSET) {
            if (sortAndSlice == null) {
                sortAndSlice = new SortAndSlice();
            }

            XreadLimit(sortAndSlice);
        }

        return sortAndSlice == null ? SortAndSlice.noSort
                                    : sortAndSlice;
    }

    private SortAndSlice XreadTopOrLimit() {

        Expression e1 = null;
        Expression e2 = null;

        if (token.tokenType == Tokens.LIMIT) {
            int position = getPosition();

            read();

            e1 = XreadSimpleValueSpecificationOrNull();

            if (e1 == null) {
                rewind(position);

                return null;
            }

            e2 = XreadSimpleValueSpecificationOrNull();

            if (e2 == null) {
                throw Error.error(ErrorCode.X_42563,
                                  ErrorCode.M_INVALID_LIMIT);
            }
        } else if (token.tokenType == Tokens.TOP) {
            int position = getPosition();

            read();

            e2 = XreadSimpleValueSpecificationOrNull();

            if (e2 == null) {
                rewind(position);

                return null;
            }

            e1 = new ExpressionValue(ValuePool.INTEGER_0, Type.SQL_INTEGER);
        }

        boolean valid = true;

        if (e1.isUnresolvedParam()) {
            e1.setDataType(session, Type.SQL_INTEGER);
        } else {
            valid = (e1.getDataType().typeCode == Types.SQL_INTEGER
                     && ((Integer) e1.getValue(null)).intValue() >= 0);
        }

        if (e2.isUnresolvedParam()) {
            e2.setDataType(session, Type.SQL_INTEGER);
        } else {
            valid &= (e2.getDataType().typeCode == Types.SQL_INTEGER
                      && ((Integer) e2.getValue(null)).intValue() >= 0);
        }

        if (valid) {
            SortAndSlice sortAndSlice = new SortAndSlice();

            sortAndSlice.addLimitCondition(new ExpressionOp(OpTypes.LIMIT, e1,
                    e2));

            return sortAndSlice;
        }

        throw Error.error(ErrorCode.X_42563, ErrorCode.M_INVALID_LIMIT);
    }

    private void XreadLimit(SortAndSlice sortAndSlice) {

        Expression e1 = null;
        Expression e2 = null;

        if (token.tokenType == Tokens.OFFSET) {
            read();

            e1 = XreadSimpleValueSpecificationOrNull();

            if (e1 == null) {
                throw Error.error(ErrorCode.X_42563,
                                  ErrorCode.M_INVALID_LIMIT);
            }

            if (token.tokenType == Tokens.ROW
                    || token.tokenType == Tokens.ROWS) {
                read();
            }
        }

        if (token.tokenType == Tokens.LIMIT) {
            read();

            e2 = XreadSimpleValueSpecificationOrNull();

            if (e2 == null) {
                throw Error.error(ErrorCode.X_42563,
                                  ErrorCode.M_INVALID_LIMIT);
            }

            if (e1 == null && token.tokenType == Tokens.OFFSET) {
                read();

                e1 = XreadSimpleValueSpecificationOrNull();
            }
        } else if (token.tokenType == Tokens.FETCH) {
            read();

            if (token.tokenType == Tokens.FIRST
                    || token.tokenType == Tokens.NEXT) {
                read();
            }

            e2 = XreadSimpleValueSpecificationOrNull();

            if (e2 == null) {
                e2 = new ExpressionValue(ValuePool.INTEGER_1,
                                         Type.SQL_INTEGER);
            }

            if (token.tokenType == Tokens.ROW
                    || token.tokenType == Tokens.ROWS) {
                read();
            }

            readThis(Tokens.ONLY);
        }

        if (e1 == null) {
            e1 = new ExpressionValue(ValuePool.INTEGER_0, Type.SQL_INTEGER);
        }

        boolean valid = true;

        if (e1.isUnresolvedParam()) {
            e1.setDataType(session, Type.SQL_INTEGER);
        } else {
            valid = (e1.getDataType().typeCode == Types.SQL_INTEGER
                     && ((Integer) e1.getValue(null)).intValue() >= 0);
        }

        if (e2 != null) {
            if (e2.isUnresolvedParam()) {
                e2.setDataType(session, Type.SQL_INTEGER);
            } else {
                valid &= (e2.getDataType().typeCode == Types.SQL_INTEGER);

                Integer value = ((Integer) e2.getValue(null));

                valid &= (value.intValue() >= 0);
            }
        }

        if (valid) {
            sortAndSlice.addLimitCondition(new ExpressionOp(OpTypes.LIMIT, e1,
                    e2));

            return;
        }

        throw Error.error(ErrorCode.X_42563, ErrorCode.M_INVALID_LIMIT);
    }

    private SortAndSlice XreadOrderBy() {

        SortAndSlice sortAndSlice = new SortAndSlice();

        while (true) {
            Expression        e = XreadValueExpression();
            ExpressionOrderBy o = new ExpressionOrderBy(e);

            if (token.tokenType == Tokens.DESC) {
                o.setDescending();
                read();
            } else if (token.tokenType == Tokens.ASC) {
                read();
            }

            if (token.tokenType == Tokens.NULLS) {
                read();

                if (token.tokenType == Tokens.FIRST) {
                    read();
                } else if (token.tokenType == Tokens.LAST) {
                    read();
                    o.setNullsLast();
                } else {
                    throw unexpectedToken();
                }
            }

            sortAndSlice.addOrderExpression(o);

            if (token.tokenType == Tokens.COMMA) {
                read();

                continue;
            }

            break;
        }

        return sortAndSlice;
    }

    protected RangeVariable readSimpleRangeVariable(int operation) {

        Table      table = readTableName();
        SimpleName alias = null;

        if (operation != StatementTypes.TRUNCATE) {
            if (token.tokenType == Tokens.AS) {
                read();
                checkIsNonCoreReservedIdentifier();
            }

            if (isNonCoreReservedIdentifier()) {
                alias = HsqlNameManager.getSimpleName(token.tokenString,
                                                      isDelimitedIdentifier());

                read();
            }
        }

        if (table.isView) {
            switch (operation) {

                case StatementTypes.MERGE :
                    if (table.isTriggerUpdatable()
                            && table.isTriggerDeletable()) {
                        break;
                    }

                    if (table.isUpdatable() && table.isInsertable()) {
                        break;
                    }

                    throw Error.error(ErrorCode.X_42545);
                case StatementTypes.UPDATE_WHERE :
                    if (table.isTriggerUpdatable()) {
                        break;
                    }

                    if (table.isUpdatable()) {
                        break;
                    }

                    throw Error.error(ErrorCode.X_42545);
                case StatementTypes.DELETE_WHERE :
                    if (table.isTriggerDeletable()) {
                        break;
                    }

                    if (table.isUpdatable()) {
                        break;
                    }

                    throw Error.error(ErrorCode.X_42545);
            }

            table = ((View) table).getSubqueryTable();
        }

        RangeVariable range = new RangeVariable(table, alias, null, null,
            compileContext);

        return range;
    }

    protected Table readNamedSubqueryOrNull() {

        if (!isSimpleName()) {
            return null;
        }

        SubQuery sq = compileContext.getNamedSubQuery(token.tokenString);

        if (sq == null) {
            return null;
        }

        read();

        return sq.getTable();
    }

    /**
     * Creates a RangeVariable from the parse context. <p>
     */
    protected RangeVariable readTableOrSubquery() {

        Table          table          = null;
        SimpleName     alias          = null;
        SimpleName[]   columnNameList = null;
        OrderedHashSet columnList     = null;

        switch (token.tokenType) {

            case Tokens.OPENBRACKET : {
                Expression e = XreadTableSubqueryOrJoinedTable();

                table = e.getTable();

                break;
            }
            case Tokens.UNNEST : {
                Expression e = XreadCollectionDerivedTable();

                table = e.getTable();

                break;
            }
            case Tokens.LATERAL : {
                Expression e = XreadLateralDerivedTable();

                table = e.getTable();

                break;
            }
            case Tokens.TABLE : {
                Expression e = XreadTableFunctionDerivedTable();

                table = e.getTable();

                break;
            }
            default : {
                table = readNamedSubqueryOrNull();

                if (table == null) {
                    table = readTableName();
                }

                if (table.isView()) {
                    table = ((View) table).getSubqueryTable();
                }
            }
        }

        boolean hasAs = false;

        if (token.tokenType == Tokens.AS) {
            read();
            checkIsNonCoreReservedIdentifier();

            hasAs = true;
        }

        if (isNonCoreReservedIdentifier()) {
            boolean limit = token.tokenType == Tokens.LIMIT
                            || token.tokenType == Tokens.OFFSET
                            || token.tokenType == Tokens.FETCH;
            boolean minus    = token.tokenType == Tokens.MINUS_EXCEPT;
            int     position = getPosition();

            alias = HsqlNameManager.getSimpleName(token.tokenString,
                                                  isDelimitedIdentifier());

            read();

            if (token.tokenType == Tokens.OPENBRACKET) {
                columnList     = new OrderedHashSet();
                columnNameList = readColumnNameList(columnList);
            } else if (!hasAs && limit) {
                if (token.tokenType == Tokens.COLON
                        || token.tokenType == Tokens.QUESTION
                        || token.tokenType == Tokens.X_VALUE) {
                    alias = null;

                    rewind(position);
                }
            } else if (!hasAs && minus) {
                rewind(position);
            }
        }

        RangeVariable range = new RangeVariable(table, alias, columnList,
            columnNameList, compileContext);

        return range;
    }

    private Expression readAggregate() {

        int        tokenT = token.tokenType;
        Expression e;

        read();
        readThis(Tokens.OPENBRACKET);

        e = readAggregateExpression(tokenT);

        readThis(Tokens.CLOSEBRACKET);

        return e;
    }

    private Expression readAggregateExpression(int tokenT) {

        int     type     = ParserDQL.getExpressionType(tokenT);
        boolean distinct = false;
        boolean all      = false;

        if (token.tokenType == Tokens.DISTINCT) {
            distinct = true;

            read();
        } else if (token.tokenType == Tokens.ALL) {
            all = true;

            read();
        }

        Expression e = XreadValueExpression();

        switch (type) {

            case OpTypes.COUNT :
                if (e.getType() == OpTypes.MULTICOLUMN) {
                    if (((ExpressionColumn) e).tableName != null) {
                        throw unexpectedToken();
                    }

                    if (all || distinct) {
                        throw unexpectedToken();
                    }

                    e.opType = OpTypes.ASTERISK;

                    break;
                } else {
                    break;
                }
            case OpTypes.STDDEV_POP :
            case OpTypes.STDDEV_SAMP :
            case OpTypes.VAR_POP :
            case OpTypes.VAR_SAMP :
                if (all || distinct) {
                    throw Error.error(ErrorCode.X_42582, all ? Tokens.T_ALL
                                                             : Tokens
                                                             .T_DISTINCT);
                }
                break;

            default :
                if (e.getType() == OpTypes.ASTERISK) {
                    throw unexpectedToken();
                }
        }

        Expression aggregateExp = new ExpressionAggregate(type, distinct, e);

        return aggregateExp;
    }

//--------------------------------------
    // returns null
    // := <unsigned literal> | <general value specification>
    Expression XreadValueSpecificationOrNull() {

        Expression e     = null;
        boolean    minus = false;

        switch (token.tokenType) {

            case Tokens.PLUS :
                read();
                break;

            case Tokens.MINUS :
                read();

                minus = true;
                break;
        }

        e = XreadUnsignedValueSpecificationOrNull();

        if (e == null) {
            return null;
        }

        if (minus) {
            e = new ExpressionArithmetic(OpTypes.NEGATE, e);
        }

        return e;
    }

    // returns null
    // <unsigned literl> | <general value specification>
    Expression XreadUnsignedValueSpecificationOrNull() {

        Expression e;

        switch (token.tokenType) {

            case Tokens.TRUE :
                read();

                return Expression.EXPR_TRUE;

            case Tokens.FALSE :
                read();

                return Expression.EXPR_FALSE;

            case Tokens.DEFAULT :
                if (compileContext.contextuallyTypedExpression) {
                    read();

                    e = new ExpressionColumn(OpTypes.DEFAULT);

                    return e;
                }
                break;

            case Tokens.NULL :
                e = new ExpressionValue(null, null);

                read();

                return e;

            case Tokens.X_VALUE :
                e = new ExpressionValue(token.tokenValue, token.dataType);

                read();

                return e;

            case Tokens.X_DELIMITED_IDENTIFIER :
            case Tokens.X_IDENTIFIER :
                if (!token.isHostParameter) {
                    return null;
                }

                return null;

            case Tokens.COLON :
                read();

                if (token.tokenType == Tokens.X_DELIMITED_IDENTIFIER
                        || token.tokenType == Tokens.X_IDENTIFIER) {}
                else {
                    throw unexpectedToken(Tokens.T_COLON);
                }

            // fall through
            case Tokens.QUESTION :
                e = new ExpressionColumn(OpTypes.DYNAMIC_PARAM);

                compileContext.addParameter(e, getPosition());
                read();

                return e;

            case Tokens.COLLATION :
                return XreadCurrentCollationSpec();

            case Tokens.VALUE :
            case Tokens.CURRENT_CATALOG :
            case Tokens.CURRENT_DEFAULT_TRANSFORM_GROUP :
            case Tokens.CURRENT_PATH :
            case Tokens.CURRENT_ROLE :
            case Tokens.CURRENT_SCHEMA :
            case Tokens.CURRENT_TRANSFORM_GROUP_FOR_TYPE :
            case Tokens.CURRENT_USER :
            case Tokens.SESSION_USER :
            case Tokens.SYSTEM_USER :
            case Tokens.USER :
                FunctionSQL function =
                    FunctionSQL.newSQLFunction(token.tokenString,
                                               compileContext);

                if (function == null) {
                    return null;
                }

                return readSQLFunction(function);

            // read SQL parameter reference
        }

        return null;
    }

    // <unsigned literl> | <dynamic parameter> | <variable>
    Expression XreadSimpleValueSpecificationOrNull() {

        Expression e;

        switch (token.tokenType) {

            case Tokens.X_VALUE :
                e = new ExpressionValue(token.tokenValue, token.dataType);

                read();

                return e;

            case Tokens.COLON :
                read();

                if (token.tokenType == Tokens.X_DELIMITED_IDENTIFIER
                        || token.tokenType == Tokens.X_IDENTIFIER) {}
                else {
                    throw unexpectedToken(Tokens.T_COLON);
                }

            // fall through
            case Tokens.QUESTION :
                e = new ExpressionColumn(OpTypes.DYNAMIC_PARAM);

                compileContext.addParameter(e, getPosition());
                read();

                return e;

            case Tokens.X_IDENTIFIER :
            case Tokens.X_DELIMITED_IDENTIFIER :
                checkValidCatalogName(token.namePrePrePrefix);

                return new ExpressionColumn(token.namePrePrefix,
                                            token.namePrefix,
                                            token.tokenString,
                                            database.sqlEnforceRefs);

            default :
                return null;
        }
    }

    // combined <value expression primary> and <predicate>
    // exclusively called
    // <explicit row value constructor> needed for predicate
    Expression XreadAllTypesValueExpressionPrimary(boolean boole) {

        Expression e = null;

        switch (token.tokenType) {

            case Tokens.EXISTS :
            case Tokens.UNIQUE :
                if (boole) {
                    return XreadPredicate();
                }
                break;

            case Tokens.ROW :
                if (boole) {
                    break;
                }

                read();
                readThis(Tokens.OPENBRACKET);

                e = XreadRowElementList(true);

                readThis(Tokens.CLOSEBRACKET);
                break;

            default :
                e = XreadSimpleValueExpressionPrimary();

                if (e != null) {
                    e = XreadArrayElementReference(e);
                }
        }

        if (e == null && token.tokenType == Tokens.OPENBRACKET) {
            read();

            e = XreadRowElementList(true);

            readThis(Tokens.CLOSEBRACKET);
        }

        if (boole && e != null) {
            e = XreadPredicateRightPart(e);
        }

        return e;
    }

    // doesn't return null
    // <value expression primary> ::= <parenthesized value expression>
    // | <nonparenthesized value expression primary>
    Expression XreadValueExpressionPrimary() {

        Expression e;

        e = XreadSimpleValueExpressionPrimary();

        if (e != null) {
            e = XreadArrayElementReference(e);

            return e;
        }

        if (token.tokenType == Tokens.OPENBRACKET) {
            read();

            e = XreadValueExpression();

            readThis(Tokens.CLOSEBRACKET);
        } else {
            return null;
        }

        return e;
    }

    // returns null
    //  <row value special case> :== this
    // <boolean predicand> :== this | <parenthesized boolean value expression>
    Expression XreadSimpleValueExpressionPrimary() {

        Expression e;

        e = XreadUnsignedValueSpecificationOrNull();

        if (e != null) {
            return e;
        }

        switch (token.tokenType) {

            case Tokens.OPENBRACKET :
                int position = getPosition();

                read();

                int subqueryPosition = getPosition();

                readOpenBrackets();

                switch (token.tokenType) {

                    case Tokens.TABLE :
                    case Tokens.VALUES :
                    case Tokens.SELECT :
                        SubQuery sq = null;

                        rewind(subqueryPosition);

                        try {
                            sq = XreadSubqueryBody(false,
                                                   OpTypes.SCALAR_SUBQUERY);

                            readThis(Tokens.CLOSEBRACKET);
                        } catch (HsqlException ex) {
                            ex.setLevel(compileContext.subqueryDepth);

                            if (lastError == null
                                    || lastError.getLevel() < ex.getLevel()) {
                                lastError = ex;
                            }

                            rewind(position);

                            return null;
                        }

                        if (sq.queryExpression.isSingleColumn()) {
                            return new Expression(OpTypes.SCALAR_SUBQUERY, sq);
                        } else {
                            return new Expression(OpTypes.ROW_SUBQUERY, sq);
                        }
                    default :
                        rewind(position);

                        return null;
                }
            case Tokens.ASTERISK :
                e = new ExpressionColumn(token.namePrePrefix,
                                         token.namePrefix);

                getRecordedToken().setExpression(e);
                read();

                return e;

            case Tokens.LEAST :
                return readLeastExpression();

            case Tokens.GREATEST :
                return readGreatestExpression();

            case Tokens.DECODE :
                return readDecodeExpression();

            case Tokens.CASEWHEN :
                return readCaseWhenExpression();

            case Tokens.CASE :
                return readCaseExpression();

            case Tokens.NULLIF :
                return readNullIfExpression();

            case Tokens.COALESCE :
            case Tokens.IFNULL :
                return readCoalesceExpression();

            case Tokens.CAST :
            case Tokens.CONVERT :
                return readCastExpression();

            case Tokens.DATE :
            case Tokens.TIME :
            case Tokens.TIMESTAMP :
            case Tokens.INTERVAL :
                e = readDateTimeIntervalLiteral();

                if (e != null) {
                    return e;
                }
                break;

            case Tokens.ARRAY :
                return readCollection(OpTypes.ARRAY);

            case Tokens.ANY :
            case Tokens.SOME :
            case Tokens.EVERY :
            case Tokens.COUNT :
            case Tokens.MAX :
            case Tokens.MIN :
            case Tokens.SUM :
            case Tokens.AVG :
            case Tokens.STDDEV_POP :
            case Tokens.STDDEV_SAMP :
            case Tokens.VAR_POP :
            case Tokens.VAR_SAMP :
                return readAggregate();

            case Tokens.NEXT :
                return readSequenceExpression();

            case Tokens.LEFT :
            case Tokens.RIGHT :

                // CLI function names
                break;

            case Tokens.TABLE : {
                read();
                readThis(Tokens.OPENBRACKET);

                SubQuery sq = XreadTableSubqueryBody(false);

                readThis(Tokens.CLOSEBRACKET);

                return new Expression(OpTypes.TABLE_SUBQUERY, sq);
            }
            default :
                if (isCoreReservedKey()) {
                    throw unexpectedToken();
                }
        }

        e = readColumnOrFunctionExpression();

        return e;
    }

    // OK - composite production -
    // <numeric primary> <charactr primary> <binary primary> <datetime primary> <interval primary>
    Expression XreadAllTypesPrimary(boolean boole) {

        Expression e = null;

        switch (token.tokenType) {

            case Tokens.SUBSTRING :
            case Tokens.SUBSTRING_REGEX :
            case Tokens.LOWER :
            case Tokens.UPPER :
            case Tokens.TRANSLATE_REGEX :
            case Tokens.TRIM :
            case Tokens.OVERLAY :
            case Tokens.NORMALIZE :

            //
            case Tokens.POSITION :
            case Tokens.OCCURRENCES_REGEX :
            case Tokens.POSITION_REGEX :
            case Tokens.EXTRACT :
            case Tokens.CHAR_LENGTH :
            case Tokens.CHARACTER_LENGTH :
            case Tokens.OCTET_LENGTH :
            case Tokens.CARDINALITY :
            case Tokens.ABS :
            case Tokens.MOD :
            case Tokens.LN :
            case Tokens.EXP :
            case Tokens.POWER :
            case Tokens.SQRT :
            case Tokens.FLOOR :
            case Tokens.CEILING :
            case Tokens.CEIL :
            case Tokens.WIDTH_BUCKET :
                FunctionSQL function =
                    FunctionSQL.newSQLFunction(token.tokenString,
                                               compileContext);

                if (function == null) {
                    throw unsupportedFeature();
                }

                e = readSQLFunction(function);

                if (e != null) {
                    break;
                }
            default :
                e = XreadAllTypesValueExpressionPrimary(boole);
        }

        e = XreadModifier(e);

        return e;
    }

    Expression XreadModifier(Expression e) {

        switch (token.tokenType) {

            case Tokens.AT : {
                read();

                Expression e1 = null;

                if (token.tokenType == Tokens.LOCAL) {
                    read();
                } else {
                    readThis(Tokens.TIME);
                    readThis(Tokens.ZONE);

                    e1 = XreadValueExpressionPrimary();

                    switch (token.tokenType) {

                        case Tokens.YEAR :
                        case Tokens.MONTH :
                        case Tokens.DAY :
                        case Tokens.HOUR :
                        case Tokens.MINUTE :
                        case Tokens.SECOND : {
                            IntervalType type = readIntervalType(false);

                            if (e1.getType() == OpTypes.SUBTRACT) {
                                e1.dataType = type;
                            } else {
                                e1 = new ExpressionOp(e1, type);
                            }
                        }
                    }
                }

                e = new ExpressionOp(OpTypes.ZONE_MODIFIER, e, e1);

                break;
            }
            case Tokens.YEAR :
            case Tokens.MONTH :
            case Tokens.DAY :
            case Tokens.HOUR :
            case Tokens.MINUTE :
            case Tokens.SECOND : {
                IntervalType type = readIntervalType(true);

                if (e.getType() == OpTypes.SUBTRACT) {
                    e.dataType = type;
                } else {
                    e = new ExpressionOp(e, type);
                }

                break;
            }
            case Tokens.COLLATE : {
                read();

                SchemaObject collation =
                    database.schemaManager.getSchemaObject(token.namePrefix,
                        token.tokenString, SchemaObject.COLLATION);
            }
        }

        return e;
    }

    Expression XreadValueExpressionWithContext() {

        Expression e;

        compileContext.contextuallyTypedExpression = true;
        e = XreadValueExpressionOrNull();
        compileContext.contextuallyTypedExpression = false;

        return e;
    }

    Expression XreadValueExpressionOrNull() {

        Expression e = XreadAllTypesCommonValueExpression(true);

        if (e == null) {
            return null;
        }

        return e;
    }

    /**
     *     <value expression> ::=
     *   <common value expression>
     *   | <boolean value expression>
     *   | <row value expression>
     *
     */
    Expression XreadValueExpression() {

        Expression e = XreadAllTypesCommonValueExpression(true);

        if (token.tokenType == Tokens.LEFTBRACKET) {
            read();

            Expression e1 = XreadNumericValueExpression();

            readThis(Tokens.RIGHTBRACKET);

            e = new ExpressionAccessor(e, e1);
        }

        return e;
    }

    // union of <numeric | datetime | string | interval value expression>
    Expression XreadRowOrCommonValueExpression() {
        return XreadAllTypesCommonValueExpression(false);
    }

    // union of <numeric | datetime | string | interval | boolean value expression>
    // no <row value expression> and no <predicate>
    Expression XreadAllTypesCommonValueExpression(boolean boole) {

        Expression e    = XreadAllTypesTerm(boole);
        int        type = 0;
        boolean    end  = false;

        while (true) {
            switch (token.tokenType) {

                case Tokens.PLUS :
                    type  = OpTypes.ADD;
                    boole = false;
                    break;

                case Tokens.MINUS :
                    type  = OpTypes.SUBTRACT;
                    boole = false;
                    break;

                case Tokens.CONCAT :
                    type  = OpTypes.CONCAT;
                    boole = false;
                    break;

                case Tokens.OR :
                    if (boole) {
                        type = OpTypes.OR;

                        break;
                    }

                // fall through
                default :
                    end = true;
                    break;
            }

            if (end) {
                break;
            }

            read();

            Expression a = e;

            e = XreadAllTypesTerm(boole);
            e = boole ? (Expression) new ExpressionLogical(type, a, e)
                      : new ExpressionArithmetic(type, a, e);
        }

        return e;
    }

    Expression XreadAllTypesTerm(boolean boole) {

        Expression e    = XreadAllTypesFactor(boole);
        int        type = 0;
        boolean    end  = false;

        while (true) {
            switch (token.tokenType) {

                case Tokens.ASTERISK :
                    type  = OpTypes.MULTIPLY;
                    boole = false;
                    break;

                case Tokens.DIVIDE :
                    type  = OpTypes.DIVIDE;
                    boole = false;
                    break;

                case Tokens.AND :
                    if (boole) {
                        type = OpTypes.AND;

                        break;
                    }

                // fall through
                default :
                    end = true;
                    break;
            }

            if (end) {
                break;
            }

            read();

            Expression a = e;

            e = XreadAllTypesFactor(boole);

            if (e == null) {
                throw unexpectedToken();
            }

            e = boole ? (Expression) new ExpressionLogical(type, a, e)
                      : new ExpressionArithmetic(type, a, e);
        }

        return e;
    }

    Expression XreadAllTypesFactor(boolean boole) {

        Expression e;
        boolean    minus   = false;
        boolean    not     = false;
        boolean    unknown = false;

        switch (token.tokenType) {

            case Tokens.PLUS :
                read();

                boole = false;
                break;

            case Tokens.MINUS :
                read();

                boole = false;
                minus = true;
                break;

            case Tokens.NOT :
                if (boole) {
                    read();

                    not = true;
                }
                break;
        }

        e = XreadAllTypesPrimary(boole);

        if (boole && token.tokenType == Tokens.IS) {
            read();

            if (token.tokenType == Tokens.NOT) {
                read();

                not = !not;
            }

            if (token.tokenType == Tokens.TRUE) {
                read();
            } else if (token.tokenType == Tokens.FALSE) {
                read();

                not = !not;
            } else if (token.tokenType == Tokens.UNKNOWN) {
                read();

                unknown = true;
            } else {
                throw unexpectedToken();
            }
        }

        if (unknown) {
            e = new ExpressionLogical(OpTypes.IS_NULL, e);
        } else if (minus) {
            e = new ExpressionArithmetic(OpTypes.NEGATE, e);
        } else if (not) {
            e = new ExpressionLogical(OpTypes.NOT, e);
        }

        return e;
    }

    Expression XreadStringValueExpression() {

        return XreadCharacterValueExpression();

//        XreadBinaryValueExpression();
    }

    Expression XreadCharacterValueExpression() {

        Expression   e         = XreadCharacterPrimary();
        SchemaObject collation = readCollateClauseOrNull();

        while (token.tokenType == Tokens.CONCAT) {
            read();

            Expression a = e;

            e         = XreadCharacterPrimary();
            collation = readCollateClauseOrNull();
            e         = new ExpressionArithmetic(OpTypes.CONCAT, a, e);
        }

        return e;
    }

    Expression XreadCharacterPrimary() {

        switch (token.tokenType) {

            case Tokens.SUBSTRING :

//            case Token.SUBSTRING_REGEX :
            case Tokens.LOWER :
            case Tokens.UPPER :

//            case Token.TRANSLATE_REGEX :
            case Tokens.TRIM :
            case Tokens.OVERLAY :

//            case Token.NORMALIZE :
                FunctionSQL function =
                    FunctionSQL.newSQLFunction(token.tokenString,
                                               compileContext);
                Expression e = readSQLFunction(function);

                if (e != null) {
                    return e;
                }
            default :
        }

        return XreadValueExpressionPrimary();
    }

    Expression XreadNumericPrimary() {

        switch (token.tokenType) {

            case Tokens.POSITION :

//            case Token.OCCURRENCES_REGEX :
//            case Token.POSITION_REGEX :
            case Tokens.EXTRACT :
            case Tokens.CHAR_LENGTH :
            case Tokens.CHARACTER_LENGTH :
            case Tokens.OCTET_LENGTH :
            case Tokens.CARDINALITY :
            case Tokens.ABS :
            case Tokens.MOD :
            case Tokens.LN :
            case Tokens.EXP :
            case Tokens.POWER :
            case Tokens.SQRT :
            case Tokens.FLOOR :
            case Tokens.CEILING :
            case Tokens.CEIL :

//            case Token.WIDTH_BUCKET :
                FunctionSQL function =
                    FunctionSQL.newSQLFunction(token.tokenString,
                                               compileContext);

                if (function == null) {
                    throw super.unexpectedToken();
                }

                Expression e = readSQLFunction(function);

                if (e != null) {
                    return e;
                }
            default :
        }

        return XreadValueExpressionPrimary();
    }

    Expression XreadNumericValueExpression() {

        Expression e = XreadTerm();

        while (true) {
            int type;

            if (token.tokenType == Tokens.PLUS) {
                type = OpTypes.ADD;
            } else if (token.tokenType == Tokens.MINUS) {
                type = OpTypes.SUBTRACT;
            } else {
                break;
            }

            read();

            Expression a = e;

            e = XreadTerm();
            e = new ExpressionArithmetic(type, a, e);
        }

        return e;
    }

    Expression XreadTerm() {

        Expression e = XreadFactor();
        int        type;

        while (true) {
            if (token.tokenType == Tokens.ASTERISK) {
                type = OpTypes.MULTIPLY;
            } else if (token.tokenType == Tokens.DIVIDE) {
                type = OpTypes.DIVIDE;
            } else {
                break;
            }

            read();

            Expression a = e;

            e = XreadFactor();

            if (e == null) {
                throw unexpectedToken();
            }

            e = new ExpressionArithmetic(type, a, e);
        }

        return e;
    }

    Expression XreadFactor() {

        Expression e;
        boolean    minus = false;

        if (token.tokenType == Tokens.PLUS) {
            read();
        } else if (token.tokenType == Tokens.MINUS) {
            read();

            minus = true;
        }

        e = XreadNumericPrimary();

        if (e == null) {
            return null;
        }

        if (minus) {
            e = new ExpressionArithmetic(OpTypes.NEGATE, e);
        }

        return e;
    }

    Expression XreadDatetimeValueExpression() {

        Expression e = XreadDateTimeIntervalTerm();

        while (true) {
            int type;

            if (token.tokenType == Tokens.PLUS) {
                type = OpTypes.ADD;
            } else if (token.tokenType == Tokens.MINUS) {
                type = OpTypes.SUBTRACT;
            } else {
                break;
            }

            read();

            Expression a = e;

            e = XreadDateTimeIntervalTerm();
            e = new ExpressionArithmetic(type, a, e);
        }

        return e;
    }

    Expression XreadIntervalValueExpression() {

        Expression e = XreadDateTimeIntervalTerm();

        while (true) {
            int type;

            if (token.tokenType == Tokens.PLUS) {
                type = OpTypes.ADD;
            } else if (token.tokenType == Tokens.MINUS) {
                type = OpTypes.SUBTRACT;
            } else {
                break;
            }

            read();

            Expression a = e;

            e = XreadDateTimeIntervalTerm();
            e = new ExpressionArithmetic(type, a, e);
        }

        return e;
    }

    Expression XreadDateTimeIntervalTerm() {

        switch (token.tokenType) {

            case Tokens.CURRENT_DATE :
            case Tokens.CURRENT_TIME :
            case Tokens.CURRENT_TIMESTAMP :
            case Tokens.LOCALTIME :
            case Tokens.LOCALTIMESTAMP :

            //
            case Tokens.ABS :
                FunctionSQL function =
                    FunctionSQL.newSQLFunction(token.tokenString,
                                               compileContext);

                if (function == null) {
                    throw super.unexpectedToken();
                }

                return readSQLFunction(function);

            default :
        }

        return XreadValueExpressionPrimary();
    }

    // returns null
    Expression XreadDateTimeValueFunctionOrNull() {

        FunctionSQL function = null;

        switch (token.tokenType) {

            case Tokens.CURRENT_DATE :
            case Tokens.CURRENT_TIME :
            case Tokens.CURRENT_TIMESTAMP :
            case Tokens.LOCALTIME :
            case Tokens.LOCALTIMESTAMP :
                function = FunctionSQL.newSQLFunction(token.tokenString,
                                                      compileContext);
                break;

            case Tokens.NOW :
            case Tokens.TODAY :
                function = FunctionCustom.newCustomFunction(token.tokenString,
                        token.tokenType);
                break;

            default :
                return null;
        }

        if (function == null) {
            throw super.unexpectedToken();
        }

        return readSQLFunction(function);
    }

    Expression XreadBooleanValueExpression() {

        try {
            Expression e = XreadBooleanTermOrNull();

            if (e == null) {
                throw Error.error(ErrorCode.X_42568);
            }

            while (true) {
                int type;

                if (token.tokenType == Tokens.OR) {
                    type = OpTypes.OR;
                } else {
                    break;
                }

                read();

                Expression a = e;

                e = XreadBooleanTermOrNull();

                if (e == null) {
                    throw Error.error(ErrorCode.X_42568);
                }

                e = new ExpressionLogical(type, a, e);
            }

            if (e == null) {
                throw Error.error(ErrorCode.X_42568);
            }

            return e;
        } catch (HsqlException ex) {
            ex.setLevel(compileContext.subqueryDepth);

            if (lastError == null || lastError.getLevel() < ex.getLevel()) {
                lastError = ex;
            }

            throw lastError;
        }
    }

    Expression XreadBooleanTermOrNull() {

        Expression e = XreadBooleanFactorOrNull();

        if (e == null) {
            return null;
        }

        int type;

        while (true) {
            if (token.tokenType == Tokens.AND) {
                type = OpTypes.AND;
            } else {
                break;
            }

            read();

            Expression a = e;

            e = XreadBooleanFactorOrNull();

            if (e == null) {
                throw unexpectedToken();
            }

            e = new ExpressionLogical(type, a, e);
        }

        return e;
    }

    Expression XreadBooleanFactorOrNull() {

        Expression e;
        boolean    not     = false;
        boolean    unknown = false;

        if (token.tokenType == Tokens.NOT) {
            read();

            not = true;
        }

        e = XreadBooleanPrimaryOrNull();

        if (e == null) {
            return null;
        }

        if (token.tokenType == Tokens.IS) {
            read();

            if (token.tokenType == Tokens.NOT) {
                read();

                not = !not;
            }

            if (token.tokenType == Tokens.TRUE) {
                read();
            } else if (token.tokenType == Tokens.FALSE) {
                not = !not;

                read();
            } else if (token.tokenType == Tokens.UNKNOWN) {
                unknown = true;

                read();
            } else {
                throw unexpectedToken();
            }
        }

        if (unknown) {
            e = new ExpressionLogical(OpTypes.IS_NULL, e);
        }

        if (not) {
            e = new ExpressionLogical(OpTypes.NOT, e);
        }

        return e;
    }

    // <boolean primary> ::= <predicate> | <boolean predicand>
    Expression XreadBooleanPrimaryOrNull() {

        Expression e = null;
        int        position;

        switch (token.tokenType) {

            case Tokens.EXISTS :
            case Tokens.UNIQUE :
                return XreadPredicate();

            case Tokens.ROW :
                read();
                readThis(Tokens.OPENBRACKET);

                e = XreadRowElementList(true);

                readThis(Tokens.CLOSEBRACKET);
                break;

            default :
                position = getPosition();

                try {
                    e = XreadAllTypesCommonValueExpression(false);
                } catch (HsqlException ex) {
                    ex.setLevel(compileContext.subqueryDepth);

                    if (lastError == null
                            || lastError.getLevel() < ex.getLevel()) {
                        lastError = ex;
                    }

                    rewind(position);
                }
        }

        if (e == null && token.tokenType == Tokens.OPENBRACKET) {
            read();

            position = getPosition();

            try {
                e = XreadRowElementList(true);

                readThis(Tokens.CLOSEBRACKET);
            } catch (HsqlException ex) {
                ex.setLevel(compileContext.subqueryDepth);

                if (lastError == null
                        || lastError.getLevel() < ex.getLevel()) {
                    lastError = ex;
                }

                rewind(position);

                e = XreadBooleanValueExpression();

                readThis(Tokens.CLOSEBRACKET);
            }
        }

        if (e != null) {
            e = XreadPredicateRightPart(e);
        }

        return e;
    }

    // similar to <value expression primary>
    Expression XreadBooleanPredicand() {

        Expression e;

        if (token.tokenType == Tokens.OPENBRACKET) {
            read();

            e = XreadBooleanValueExpression();

            readThis(Tokens.CLOSEBRACKET);

            return e;
        } else {
            e = XreadSimpleValueExpressionPrimary();

            if (e != null) {
                e = XreadArrayElementReference(e);
            }

            return e;
        }
    }

    Expression XreadPredicate() {

        switch (token.tokenType) {

            case Tokens.EXISTS : {
                read();

                Expression s = XreadTableSubqueryForPredicate(OpTypes.EXISTS);

                return new ExpressionLogical(OpTypes.EXISTS, s);
            }
            case Tokens.UNIQUE : {
                read();

                Expression s = XreadTableSubqueryForPredicate(OpTypes.UNIQUE);

                return new ExpressionLogical(OpTypes.UNIQUE, s);
            }
            default : {
                Expression a = XreadRowValuePredicand();

                return XreadPredicateRightPart(a);
            }
        }
    }

    Expression XreadPredicateRightPart(final Expression l) {

        boolean           hasNot = false;
        ExpressionLogical e      = null;
        Expression        r;

        if (token.tokenType == Tokens.NOT) {
            read();

            hasNot = true;
        }

        switch (token.tokenType) {

            case Tokens.IS : {
                if (hasNot) {
                    throw unexpectedToken();
                }

                read();

                if (token.tokenType == Tokens.NOT) {
                    hasNot = true;

                    read();
                }

                if (token.tokenType == Tokens.DISTINCT) {
                    read();
                    readThis(Tokens.FROM);

                    r      = XreadRowValuePredicand();
                    e      = new ExpressionLogical(OpTypes.NOT_DISTINCT, l, r);
                    hasNot = !hasNot;

                    break;
                }

                if (token.tokenType == Tokens.NULL
                        || token.tokenType == Tokens.UNKNOWN) {
                    read();

                    e = new ExpressionLogical(OpTypes.IS_NULL, l);

                    break;
                }

                throw unexpectedToken();
            }
            case Tokens.LIKE : {
                e                = XreadLikePredicateRightPart(l);
                e.noOptimisation = isCheckOrTriggerCondition;

                break;
            }
            case Tokens.BETWEEN : {
                e = XreadBetweenPredicateRightPart(l);

                break;
            }
            case Tokens.IN : {
                e                = XreadInPredicateRightPart(l);
                e.noOptimisation = isCheckOrTriggerCondition;

                break;
            }
            case Tokens.OVERLAPS : {
                if (hasNot) {
                    throw unexpectedToken();
                }

                e = XreadOverlapsPredicateRightPart(l);

                break;
            }
            case Tokens.EQUALS :
            case Tokens.GREATER_EQUALS :
            case Tokens.GREATER :
            case Tokens.LESS :
            case Tokens.LESS_EQUALS :
            case Tokens.NOT_EQUALS : {
                if (hasNot) {
                    throw unexpectedToken();
                }

                int type = getExpressionType(token.tokenType);

                read();

                switch (token.tokenType) {

                    case Tokens.ANY :
                    case Tokens.SOME :
                    case Tokens.ALL :
                        e = XreadQuantifiedComparisonRightPart(type, l);
                        break;

                    default : {
                        Expression row = XreadRowValuePredicand();

                        e = new ExpressionLogical(type, l, row);

                        break;
                    }
                }

                break;
            }
            case Tokens.MATCH : {
                e = XreadMatchPredicateRightPart(l);

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
            e = new ExpressionLogical(OpTypes.NOT, e);
        }

        return e;
    }

    private ExpressionLogical XreadBetweenPredicateRightPart(
            final Expression a) {

        boolean symmetric = false;

        read();

        if (token.tokenType == Tokens.ASYMMETRIC) {
            read();
        } else if (token.tokenType == Tokens.SYMMETRIC) {
            symmetric = true;

            read();
        }

        Expression left = XreadRowValuePredicand();

        readThis(Tokens.AND);

        Expression right = XreadRowValuePredicand();

        if (a.isUnresolvedParam() && left.isUnresolvedParam()) {
            throw Error.error(ErrorCode.X_42567);
        }

        if (a.isUnresolvedParam() && right.isUnresolvedParam()) {
            throw Error.error(ErrorCode.X_42567);
        }

        Expression l = new ExpressionLogical(OpTypes.GREATER_EQUAL, a, left);
        Expression r = new ExpressionLogical(OpTypes.SMALLER_EQUAL, a, right);
        ExpressionLogical leftToRight = new ExpressionLogical(OpTypes.AND, l,
            r);

        if (symmetric) {
            l = new ExpressionLogical(OpTypes.SMALLER_EQUAL, a, left);
            r = new ExpressionLogical(OpTypes.GREATER_EQUAL, a, right);

            Expression rightToLeft = new ExpressionLogical(OpTypes.AND, l, r);

            return new ExpressionLogical(OpTypes.OR, leftToRight, rightToLeft);
        } else {
            return leftToRight;
        }
    }

    private ExpressionLogical XreadQuantifiedComparisonRightPart(int exprType,
            Expression l) {

        int        tokenT      = token.tokenType;
        int        exprSubType = 0;
        Expression e;

        switch (token.tokenType) {

            case Tokens.ANY :
            case Tokens.SOME :
                exprSubType = OpTypes.ANY_QUANTIFIED;
                break;

            case Tokens.ALL :
                exprSubType = OpTypes.ALL_QUANTIFIED;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "ParserDQL");
        }

        read();
        readThis(Tokens.OPENBRACKET);

        int position = getPosition();

        readOpenBrackets();

        switch (token.tokenType) {

            case Tokens.TABLE :
            case Tokens.VALUES :
            case Tokens.SELECT :
                rewind(position);

                SubQuery sq = XreadSubqueryBody(false, OpTypes.IN);

                e = new Expression(OpTypes.TABLE_SUBQUERY, sq);

                readThis(Tokens.CLOSEBRACKET);
                break;

            default :
                rewind(position);

                e = readAggregateExpression(tokenT);

                readThis(Tokens.CLOSEBRACKET);
        }

        ExpressionLogical r = new ExpressionLogical(exprType, l, e);

        r.setSubType(exprSubType);

        return r;
    }

    private ExpressionLogical XreadInPredicateRightPart(Expression l) {

        int        degree = l.getDegree();
        Expression e      = null;

        read();
        readThis(Tokens.OPENBRACKET);

        int position = getPosition();
        int brackets = readOpenBrackets();

        switch (token.tokenType) {

            case Tokens.UNNEST :
                e = XreadCollectionDerivedTable();

                e.getTable().getSubQuery().setUniqueRows();
                readThis(Tokens.CLOSEBRACKET);
                this.readCloseBrackets(brackets);
                break;

            case Tokens.TABLE :
            case Tokens.VALUES :
            case Tokens.SELECT : {
                rewind(position);

                SubQuery sq = XreadSubqueryBody(false, OpTypes.IN);

                e = new Expression(OpTypes.TABLE_SUBQUERY, sq);

                readThis(Tokens.CLOSEBRACKET);

                break;
            }
            default : {
                rewind(position);

                e = XreadInValueListConstructor(degree);

                readThis(Tokens.CLOSEBRACKET);

                break;
            }
        }

        ExpressionLogical r;

        if (isCheckOrTriggerCondition) {
            r = new ExpressionLogical(OpTypes.IN, l, e);
        } else {
            r = new ExpressionLogical(OpTypes.EQUAL, l, e);

            r.setSubType(OpTypes.ANY_QUANTIFIED);
        }

        return r;
    }

    Expression XreadInValueList(int degree) {

        HsqlArrayList list = new HsqlArrayList();

        while (true) {
            Expression e = XreadValueExpression();

            if (e.getType() != OpTypes.ROW) {
                e = new Expression(OpTypes.ROW, new Expression[]{ e });
            }

            list.add(e);

            if (token.tokenType == Tokens.COMMA) {
                read();

                continue;
            }

            break;
        }

        Expression[] array = new Expression[list.size()];

        list.toArray(array);

        Expression e = new Expression(OpTypes.TABLE, array);

        for (int i = 0; i < array.length; i++) {
            if (array[i].getType() != OpTypes.ROW) {
                array[i] = new Expression(OpTypes.ROW,
                                          new Expression[]{ array[i] });
            }

            Expression[] args = array[i].nodes;

            if (args.length != degree) {

                // SQL error message
                throw unexpectedToken();
            }

            for (int j = 0; j < degree; j++) {
                if (args[j].getType() == OpTypes.ROW) {

                    // SQL error message
                    throw unexpectedToken();
                }
            }
        }

        return e;
    }

    private ExpressionLogical XreadLikePredicateRightPart(Expression a) {

        read();

        Expression b      = XreadStringValueExpression();
        Expression escape = null;

        if (token.tokenString.equals(Tokens.T_ESCAPE)) {
            read();

            escape = XreadStringValueExpression();
        }

        return new ExpressionLike(a, b, escape,
                                  this.isCheckOrTriggerCondition);
    }

    private ExpressionLogical XreadMatchPredicateRightPart(Expression a) {

        boolean isUnique  = false;
        int     matchType = OpTypes.MATCH_SIMPLE;

        read();

        if (token.tokenType == Tokens.UNIQUE) {
            read();

            isUnique = true;
        }

        if (token.tokenType == Tokens.SIMPLE) {
            read();

            matchType = isUnique ? OpTypes.MATCH_UNIQUE_SIMPLE
                                 : OpTypes.MATCH_SIMPLE;
        } else if (token.tokenType == Tokens.PARTIAL) {
            read();

            matchType = isUnique ? OpTypes.MATCH_UNIQUE_PARTIAL
                                 : OpTypes.MATCH_PARTIAL;
        } else if (token.tokenType == Tokens.FULL) {
            read();

            matchType = isUnique ? OpTypes.MATCH_UNIQUE_FULL
                                 : OpTypes.MATCH_FULL;
        }

        int        mode = isUnique ? OpTypes.TABLE_SUBQUERY
                                   : OpTypes.IN;
        Expression s    = XreadTableSubqueryForPredicate(mode);

        return new ExpressionLogical(matchType, a, s);
    }

    private ExpressionLogical XreadOverlapsPredicateRightPart(Expression l) {

        if (l.getType() != OpTypes.ROW) {
            throw Error.error(ErrorCode.X_42564);
        }

        if (l.nodes.length != 2) {
            throw Error.error(ErrorCode.X_42564);
        }

        read();

        if (token.tokenType != Tokens.OPENBRACKET) {
            throw unexpectedToken();
        }

        Expression r = XreadRowValuePredicand();

        if (r.nodes.length != 2) {
            throw Error.error(ErrorCode.X_42564);
        }

        return new ExpressionLogical(OpTypes.OVERLAPS, l, r);
    }

    Expression XreadRowValueExpression() {

        Expression e = XreadExplicitRowValueConstructorOrNull();

        if (e != null) {
            return e;
        }

        return XreadRowValueSpecialCase();
    }

    Expression XreadTableRowValueConstructor() {

        Expression e = XreadExplicitRowValueConstructorOrNull();

        if (e != null) {
            return e;
        }

        return XreadRowValueSpecialCase();
    }

    //  union of <row value expression> |
    // <boolean predicand> | <non parenthesized value expression primary> |
    //  translated to <explicit row value constructor>
    // <value expression primary> | <non parenthesized value expression primary> |
    Expression XreadRowValuePredicand() {
        return XreadRowOrCommonValueExpression();
    }

    Expression XreadRowValueSpecialCase() {

        Expression e = XreadSimpleValueExpressionPrimary();

        if (e != null) {
            e = XreadArrayElementReference(e);
        }

        return e;
    }

    // <row value constructor>
    // ISSUE - XreadCommonValueExpression and XreadBooleanValueExpression should merge
    Expression XreadRowValueConstructor() {

        Expression e;

        e = XreadExplicitRowValueConstructorOrNull();

        if (e != null) {
            return e;
        }

        e = XreadRowOrCommonValueExpression();

        if (e != null) {
            return e;
        }

        return XreadBooleanValueExpression();
    }

    // returns null
    // must be called in conjusnction with <parenthesized ..
    Expression XreadExplicitRowValueConstructorOrNull() {

        Expression e;

        switch (token.tokenType) {

            case Tokens.OPENBRACKET : {
                read();

                int position = getPosition();
                int brackets = readOpenBrackets();

                switch (token.tokenType) {

                    case Tokens.TABLE :
                    case Tokens.VALUES :
                    case Tokens.SELECT :
                        rewind(position);

                        SubQuery sq = XreadSubqueryBody(false,
                                                        OpTypes.ROW_SUBQUERY);

                        readThis(Tokens.CLOSEBRACKET);

                        return new Expression(OpTypes.ROW_SUBQUERY, sq);

                    default :
                        rewind(position);

                        e = XreadRowElementList(true);

                        readThis(Tokens.CLOSEBRACKET);

                        return e;
                }
            }
            case Tokens.ROW : {
                read();
                readThis(Tokens.OPENBRACKET);

                e = XreadRowElementList(false);

                readThis(Tokens.CLOSEBRACKET);

                return e;
            }
        }

        return null;
    }

    Expression XreadRowElementList(boolean multiple) {

        Expression    e;
        HsqlArrayList list = new HsqlArrayList();

        while (true) {
            e = XreadValueExpression();

            list.add(e);

            if (token.tokenType == Tokens.COMMA) {
                read();

                continue;
            }

            if (multiple && list.size() == 1) {
                return e;
            }

            break;
        }

        Expression[] array = new Expression[list.size()];

        list.toArray(array);

        return new Expression(OpTypes.ROW, array);
    }

    Expression XreadCurrentCollationSpec() {
        throw Error.error(ErrorCode.X_0A000);
    }

    Expression XreadRowSubquery() {

        readThis(Tokens.OPENBRACKET);

        SubQuery sq = XreadSubqueryBody(false, OpTypes.ROW_SUBQUERY);

        readThis(Tokens.CLOSEBRACKET);

        return new Expression(OpTypes.ROW_SUBQUERY, sq);
    }

    Expression XreadTableSubqueryForPredicate(int mode) {

        readThis(Tokens.OPENBRACKET);

        SubQuery sq = XreadSubqueryBody(false, mode);

        readThis(Tokens.CLOSEBRACKET);

        return new Expression(OpTypes.TABLE_SUBQUERY, sq);
    }

    Expression XreadTableSubqueryOrJoinedTable() {

        boolean joinedTable = false;
        int     position;

        readThis(Tokens.OPENBRACKET);

        position = getPosition();

        readOpenBrackets();

        switch (token.tokenType) {

            case Tokens.TABLE :
            case Tokens.VALUES :
            case Tokens.SELECT :
            case Tokens.WITH :
                break;

            default :
                joinedTable = true;
        }

        rewind(position);

        if (joinedTable) {
            SubQuery sq = XreadJoinedTableAsSubquery();

            readThis(Tokens.CLOSEBRACKET);

            return new Expression(OpTypes.TABLE_SUBQUERY, sq);
        } else {
            SubQuery sq = XreadTableSubqueryBody(true);

            readThis(Tokens.CLOSEBRACKET);

            return new Expression(OpTypes.TABLE_SUBQUERY, sq);
        }
    }

    SubQuery XreadJoinedTableAsSubquery() {

        int position = getPosition();

        compileContext.subqueryDepth++;

        QueryExpression queryExpression = XreadJoinedTable();

        queryExpression.resolve(session);

        if (((QuerySpecification) queryExpression).rangeVariables.length < 2) {
            throw unexpectedTokenRequire(Tokens.T_JOIN);
        }

        SubQuery sq = new SubQuery(database, compileContext.subqueryDepth,
                                   queryExpression, OpTypes.TABLE_SUBQUERY);

        sq.sql = getLastPart(position);

        sq.prepareTable(session);

        compileContext.subqueryDepth--;

        return sq;
    }

    QueryExpression XreadJoinedTable() {

        QuerySpecification select = new QuerySpecification(compileContext);
        Expression         e      = new ExpressionColumn(OpTypes.MULTICOLUMN);

        select.addSelectColumnExpression(e);
        XreadTableReference(select);

        return select;
    }

    SubQuery XreadTableNamedSubqueryBody(HsqlName name,
                                         HsqlName[] columnNames) {

        SubQuery sq = XreadSubqueryBody(true, OpTypes.TABLE_SUBQUERY);

        sq.prepareTable(session, name, columnNames);

        return sq;
    }

    SubQuery XreadTableSubqueryBody(boolean resolve) {

        SubQuery sq = XreadSubqueryBody(resolve, OpTypes.TABLE_SUBQUERY);

        if (resolve) {
            sq.prepareTable(session);
        }

        return sq;
    }

    SubQuery XreadSubqueryBody(boolean resolve, int mode) {

        int position = getPosition();

        compileContext.subqueryDepth++;

        QueryExpression queryExpression = XreadQueryExpression();

        if (resolve) {
            queryExpression.resolve(session);
        } else {
            queryExpression.resolveReferences(session);
        }

        SubQuery sq = new SubQuery(database, compileContext.subqueryDepth,
                                   queryExpression, mode);

        sq.sql = getLastPart(position);

        compileContext.subqueryDepth--;

        return sq;
    }

    SubQuery XreadViewSubquery(View view) {

        compileContext.subqueryDepth++;

        QueryExpression queryExpression;

        try {
            queryExpression = XreadQueryExpression();
        } catch (HsqlException e) {
            queryExpression = XreadJoinedTable();
        }

        queryExpression.setView(view);
        queryExpression.resolve(session);

        SubQuery sq = new SubQuery(database, compileContext.subqueryDepth,
                                   queryExpression, view);

        compileContext.subqueryDepth--;

        return sq;
    }

    Expression XreadContextuallyTypedTable(int degree) {

        Expression   e       = readRow();
        Expression[] list    = e.nodes;
        boolean      isTable = false;

        if (degree == 1) {
            if (e.getType() == OpTypes.ROW) {
                e.opType = OpTypes.TABLE;

                for (int i = 0; i < list.length; i++) {
                    if (list[i].getType() != OpTypes.ROW) {
                        list[i] = new Expression(OpTypes.ROW,
                                                 new Expression[]{ list[i] });
                    } else if (list[i].nodes.length != degree) {
                        throw Error.error(ErrorCode.X_42564);
                    }
                }

                return e;
            } else {
                e = new Expression(OpTypes.ROW, new Expression[]{ e });
                e = new Expression(OpTypes.TABLE, new Expression[]{ e });

                return e;
            }
        }

        if (e.getType() != OpTypes.ROW) {
            throw Error.error(ErrorCode.X_42564);
        }

        for (int i = 0; i < list.length; i++) {
            if (list[i].getType() == OpTypes.ROW) {
                isTable = true;

                break;
            }
        }

        if (isTable) {
            e.opType = OpTypes.TABLE;

            for (int i = 0; i < list.length; i++) {
                if (list[i].getType() != OpTypes.ROW) {
                    throw Error.error(ErrorCode.X_42564);
                }

                Expression[] args = list[i].nodes;

                if (args.length != degree) {
                    throw Error.error(ErrorCode.X_42564);
                }

                for (int j = 0; j < degree; j++) {
                    if (args[j].getType() == OpTypes.ROW) {
                        throw Error.error(ErrorCode.X_42564);
                    }
                }
            }
        } else {
            if (list.length != degree) {
                throw Error.error(ErrorCode.X_42564);
            }

            e = new Expression(OpTypes.TABLE, new Expression[]{ e });
        }

        return e;
    }

    private Expression XreadInValueListConstructor(int degree) {

        int position = getPosition();

        compileContext.subqueryDepth++;

        Expression e = XreadInValueList(degree);
        SubQuery sq = new SubQuery(database, compileContext.subqueryDepth, e,
                                   OpTypes.IN);

        sq.sql = getLastPart(position);

        compileContext.subqueryDepth--;

        return e;
    }

    private SubQuery XreadRowValueExpressionList() {

        compileContext.subqueryDepth++;

        Expression e = XreadRowValueExpressionListBody();
        HsqlList unresolved =
            e.resolveColumnReferences(RangeVariable.emptyArray, null);

        ExpressionColumn.checkColumnsResolved(unresolved);
        e.resolveTypes(session, null);
        e.prepareTable(session, null, e.nodes[0].nodes.length);

        SubQuery sq = new SubQuery(database, compileContext.subqueryDepth, e,
                                   OpTypes.TABLE);

        sq.prepareTable(session);

        compileContext.subqueryDepth--;

        return sq;
    }

    Expression XreadRowValueExpressionListBody() {

        Expression r = null;

        while (true) {
            int        brackets = readOpenBrackets();
            Expression e        = readRow();

            readCloseBrackets(brackets);

            if (r == null) {
                r = new Expression(OpTypes.ROW, new Expression[]{ e });
            } else {
                r.nodes = (Expression[]) ArrayUtil.resizeArray(r.nodes,
                        r.nodes.length + 1);
                r.nodes[r.nodes.length - 1] = e;
            }

            if (token.tokenType != Tokens.COMMA) {
                break;
            }

            read();
        }

        Expression[] list   = r.nodes;
        int          degree = 1;

        if (list[0].getType() == OpTypes.ROW) {
            degree = list[0].nodes.length;
        }

        r.opType = OpTypes.TABLE;

        for (int i = 0; i < list.length; i++) {
            if (list[i].getType() == OpTypes.ROW) {
                if (list[i].nodes.length != degree) {
                    throw Error.error(ErrorCode.X_42564);
                }
            } else {
                if (degree != 1) {
                    throw Error.error(ErrorCode.X_42564);
                }

                list[i] = new Expression(OpTypes.ROW,
                                         new Expression[]{ list[i] });
            }
        }

        return r;
    }

    Expression XreadTargetSpecification(RangeVariable[] rangeVars,
                                        LongDeque colIndexList) {

        ColumnSchema column = null;
        int          index  = -1;

        checkIsIdentifier();

        if (token.namePrePrePrefix != null) {
            checkValidCatalogName(token.namePrePrePrefix);
        }

        for (int i = 0; i < rangeVars.length; i++) {
            if (rangeVars[i] == null) {
                continue;
            }

            index = rangeVars[i].findColumn(token.tokenString);

            if (index > -1 && rangeVars[i].resolvesTableName(token.namePrefix)
                    && rangeVars[i].resolvesSchemaName(token.namePrePrefix)) {
                column = rangeVars[i].getColumn(index);

                read();

                break;
            }
        }

        if (column == null) {
            throw Error.error(ErrorCode.X_42501, token.tokenString);
        }

        colIndexList.add(index);

        if (token.tokenType == Tokens.LEFTBRACKET) {
            if (!column.getDataType().isArrayType()) {
                throw unexpectedToken();
            }

            read();

            Expression e = XreadNumericValueExpression();

            if (e == null) {
                throw Error.error(ErrorCode.X_42501, token.tokenString);
            }

            e = new ExpressionAccessor(column.getAccessor(), e);

            readThis(Tokens.RIGHTBRACKET);

            return e;
        }

        return column.getAccessor();
    }

    Expression XreadCollectionDerivedTable() {

        boolean ordinality = false;
        int     position   = getPosition();

        readThis(Tokens.UNNEST);
        readThis(Tokens.OPENBRACKET);

        compileContext.subqueryDepth++;

        Expression e = XreadValueExpression();

        compileContext.subqueryDepth--;

        readThis(Tokens.CLOSEBRACKET);

        if (token.tokenType == Tokens.WITH) {
            read();
            readThis(Tokens.ORDINALITY);

            ordinality = true;
        }

        e = new ExpressionTable(e, null, ordinality);

        SubQuery sq = new SubQuery(database, compileContext.subqueryDepth, e,
                                   OpTypes.TABLE_SUBQUERY);

        sq.createTable();

        sq.sql = getLastPart(position);

        return e;
    }

    Expression XreadTableFunctionDerivedTable() {

        int position = getPosition();

        readThis(Tokens.TABLE);
        readThis(Tokens.OPENBRACKET);

        compileContext.subqueryDepth++;

        Expression e = this.XreadValueExpression();

        if (e.getType() != OpTypes.FUNCTION) {
            throw this.unexpectedToken(Tokens.T_TABLE);
        }

        compileContext.subqueryDepth--;

        readThis(Tokens.CLOSEBRACKET);

        e = new ExpressionTable(e, null, false);

        SubQuery sq = new SubQuery(database, compileContext.subqueryDepth, e,
                                   OpTypes.TABLE_SUBQUERY);

        sq.createTable();

        sq.sql = getLastPart(position);

        return e;
    }

    Expression XreadLateralDerivedTable() {

        readThis(Tokens.LATERAL);
        readThis(Tokens.OPENBRACKET);

        int position = getPosition();

        compileContext.subqueryDepth++;

        QueryExpression queryExpression = XreadQueryExpression();
        SubQuery sq = new SubQuery(database, compileContext.subqueryDepth,
                                   queryExpression, OpTypes.TABLE_SUBQUERY);

        sq.createTable();

        sq.sql = getLastPart(position);

        compileContext.subqueryDepth--;

        readThis(Tokens.CLOSEBRACKET);

        return new Expression(OpTypes.TABLE_SUBQUERY, sq);
    }

    Expression XreadArrayConstructor() {

        readThis(Tokens.OPENBRACKET);

        int position = getPosition();

        compileContext.subqueryDepth++;

        QueryExpression queryExpression = XreadQueryExpression();

        queryExpression.resolveReferences(session);

        SubQuery sq = new SubQuery(database, compileContext.subqueryDepth,
                                   queryExpression, OpTypes.TABLE_SUBQUERY);

        sq.sql = getLastPart(position);

        compileContext.subqueryDepth--;

        readThis(Tokens.CLOSEBRACKET);

        return new Expression(OpTypes.ARRAY_SUBQUERY, sq);
    }

    // Additional Common Elements
    SchemaObject readCollateClauseOrNull() {

        if (token.tokenType == Tokens.COLLATE) {
            read();

            SchemaObject collation =
                database.schemaManager.getSchemaObject(token.namePrefix,
                    token.tokenString, SchemaObject.COLLATION);

            return collation;
        }

        return null;
    }

    Expression XreadArrayElementReference(Expression e) {

        if (token.tokenType == Tokens.LEFTBRACKET) {
            read();

            Expression e1 = XreadNumericValueExpression();

            readThis(Tokens.RIGHTBRACKET);

            e = new ExpressionAccessor(e, e1);
        }

        return e;
    }

    Expression readRow() {

        Expression r = null;

        while (true) {
            Expression e = XreadValueExpressionWithContext();

            if (r == null) {
                r = e;
            } else if (r.getType() == OpTypes.ROW) {
                if (e.getType() == OpTypes.ROW
                        && r.nodes[0].getType() != OpTypes.ROW) {
                    r = new Expression(OpTypes.ROW, new Expression[] {
                        r, e
                    });
                } else {
                    r.nodes = (Expression[]) ArrayUtil.resizeArray(r.nodes,
                            r.nodes.length + 1);
                    r.nodes[r.nodes.length - 1] = e;
                }
            } else {
                r = new Expression(OpTypes.ROW, new Expression[] {
                    r, e
                });
            }

            if (token.tokenType != Tokens.COMMA) {
                break;
            }

            read();
        }

        return r;
    }

    Expression readCaseExpression() {

        Expression predicand = null;

        read();

        if (token.tokenType != Tokens.WHEN) {
            predicand = XreadRowValuePredicand();
        }

        return readCaseWhen(predicand);
    }

    /**
     * Reads part of a CASE .. WHEN  expression
     */
    private Expression readCaseWhen(final Expression l) {

        readThis(Tokens.WHEN);

        Expression condition = null;

        if (l == null) {
            condition = XreadBooleanValueExpression();
        } else {
            while (true) {
                Expression newCondition = XreadPredicateRightPart(l);

                if (l == newCondition) {
                    newCondition =
                        new ExpressionLogical(l, XreadRowValuePredicand());
                }

                if (condition == null) {
                    condition = newCondition;
                } else {
                    condition = new ExpressionLogical(OpTypes.OR, condition,
                                                      newCondition);
                }

                if (token.tokenType == Tokens.COMMA) {
                    read();
                } else {
                    break;
                }
            }
        }

        readThis(Tokens.THEN);

        Expression current  = XreadValueExpression();
        Expression elseExpr = null;

        if (token.tokenType == Tokens.WHEN) {
            elseExpr = readCaseWhen(l);
        } else if (token.tokenType == Tokens.ELSE) {
            read();

            elseExpr = XreadValueExpression();

            readThis(Tokens.END);
            readIfThis(Tokens.CASE);
        } else {
            elseExpr = new ExpressionValue((Object) null, Type.SQL_ALL_TYPES);

            readThis(Tokens.END);
            readIfThis(Tokens.CASE);
        }

        Expression alternatives = new ExpressionOp(OpTypes.ALTERNATIVE,
            current, elseExpr);
        Expression casewhen = new ExpressionOp(OpTypes.CASEWHEN, condition,
                                               alternatives);

        return casewhen;
    }

    /**
     * reads a CASEWHEN expression
     */
    private Expression readCaseWhenExpression() {

        Expression l = null;

        read();
        readThis(Tokens.OPENBRACKET);

        l = XreadBooleanValueExpression();

        readThis(Tokens.COMMA);

        Expression thenelse = XreadRowValueExpression();

        readThis(Tokens.COMMA);

        thenelse = new ExpressionOp(OpTypes.ALTERNATIVE, thenelse,
                                    XreadValueExpression());
        l = new ExpressionOp(OpTypes.CASEWHEN, l, thenelse);

        readThis(Tokens.CLOSEBRACKET);

        return l;
    }

    /**
     * Reads a CAST or CONVERT expression
     */
    private Expression readCastExpression() {

        boolean isConvert = token.tokenType == Tokens.CONVERT;

        read();
        readThis(Tokens.OPENBRACKET);

        Expression l = this.XreadValueExpressionOrNull();

        if (isConvert) {
            readThis(Tokens.COMMA);
        } else {
            readThis(Tokens.AS);
        }

        Type typeObject = readTypeDefinition(true);

        if (l.isUnresolvedParam()) {
            l.setDataType(session, typeObject);
        } else {
            l = new ExpressionOp(l, typeObject);
        }

        readThis(Tokens.CLOSEBRACKET);

        return l;
    }

    /**
     * reads a Column or Function expression
     */
    private Expression readColumnOrFunctionExpression() {

        String  name           = token.tokenString;
        boolean isSimpleQuoted = isDelimitedSimpleName();
        String  prefix         = token.namePrefix;
        String  prePrefix      = token.namePrePrefix;
        String  prePrePrefix   = token.namePrePrePrefix;
        Token   recordedToken  = getRecordedToken();

        checkIsIdentifier();

        if (isUndelimitedSimpleName()) {
            FunctionSQL function =
                FunctionCustom.newCustomFunction(token.tokenString,
                                                 token.tokenType);

            if (function != null) {
                int pos = getPosition();

                try {
                    Expression e = readSQLFunction(function);

                    if (e != null) {
                        return e;
                    }
                } catch (HsqlException ex) {
                    ex.setLevel(compileContext.subqueryDepth);

                    if (lastError == null
                            || lastError.getLevel() < ex.getLevel()) {
                        lastError = ex;
                    }

                    rewind(pos);
                }
            } else if (isReservedKey()) {
                function = FunctionSQL.newSQLFunction(name, compileContext);

                if (function != null) {
                    Expression e = readSQLFunction(function);

                    if (e != null) {
                        return e;
                    }
                }
            }
        }

        read();

        if (token.tokenType != Tokens.OPENBRACKET) {
            checkValidCatalogName(prePrePrefix);

            Expression column = new ExpressionColumn(prePrefix, prefix, name,
                database.sqlEnforceRefs);

            return column;
        }

        if (prePrePrefix != null) {
            throw Error.error(ErrorCode.X_42551, prePrePrefix);
        }

        checkValidCatalogName(prePrefix);

        prefix = session.getSchemaName(prefix);

        RoutineSchema routineSchema =
            (RoutineSchema) database.schemaManager.findSchemaObject(name,
                prefix, SchemaObject.FUNCTION);

        if (routineSchema == null && isSimpleQuoted) {
            HsqlName schema =
                database.schemaManager.getDefaultSchemaHsqlName();

            routineSchema =
                (RoutineSchema) database.schemaManager.findSchemaObject(name,
                    schema.name, SchemaObject.FUNCTION);

            if (routineSchema == null) {
                Routine.createRoutines(session, schema, name);

                routineSchema =
                    (RoutineSchema) database.schemaManager.findSchemaObject(
                        name, schema.name, SchemaObject.FUNCTION);
            }
        }

        if (routineSchema == null) {
            throw Error.error(ErrorCode.X_42501, name);
        }

        HsqlArrayList list = new HsqlArrayList();

        readThis(Tokens.OPENBRACKET);

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

        FunctionSQLInvoked function  = new FunctionSQLInvoked(routineSchema);
        Expression[]       arguments = new Expression[list.size()];

        list.toArray(arguments);
        function.setArguments(arguments);
        compileContext.addFunctionCall(function);
        recordedToken.setExpression(routineSchema);

        return function;
    }

    Expression readCollection(int type) {

        read();

        if (token.tokenType == Tokens.OPENBRACKET) {
            return XreadArrayConstructor();
        } else {
            readThis(Tokens.LEFTBRACKET);

            HsqlArrayList list = new HsqlArrayList();

            for (int i = 0; ; i++) {
                if (token.tokenType == Tokens.RIGHTBRACKET) {
                    read();

                    break;
                }

                if (i > 0) {
                    readThis(Tokens.COMMA);
                }

                Expression e = XreadValueExpressionOrNull();

                list.add(e);
            }

            Expression[] array = new Expression[list.size()];

            list.toArray(array);

            return new Expression(OpTypes.ARRAY, array);
        }
    }

    private Expression readDecodeExpression() {

        // turn into a CASEWHEN
        read();
        readThis(Tokens.OPENBRACKET);

        Expression casewhen    = null;
        Expression alternative = null;
        Expression main        = XreadValueExpression();

        readThis(Tokens.COMMA);

        do {
            Expression v = XreadValueExpression();

            if (token.tokenType == Tokens.COMMA) {
                readThis(Tokens.COMMA);
            } else {
                alternative.setRightNode(v);

                break;
            }

            Expression l = new ExpressionLogical(main, v);
            Expression r = XreadValueExpression();
            Expression a = new ExpressionOp(OpTypes.ALTERNATIVE, r, null);
            Expression c = new ExpressionOp(OpTypes.CASEWHEN, l, a);

            if (casewhen == null) {
                casewhen = c;
            } else {
                alternative.setRightNode(c);
            }

            alternative = a;

            if (token.tokenType == Tokens.COMMA) {
                readThis(Tokens.COMMA);
            } else {
                alternative.setRightNode(new ExpressionValue(null, null));;

                break;
            }
        } while (true);

        readThis(Tokens.CLOSEBRACKET);

        return casewhen;
    }

    private Expression readLeastExpression() {

        // turn into a CASEWHEN
        read();
        readThis(Tokens.OPENBRACKET);

        Expression casewhen = null;

        do {
            casewhen = readValue(casewhen, OpTypes.SMALLER);

            if (token.tokenType == Tokens.COMMA) {
                readThis(Tokens.COMMA);
            } else {
                break;
            }
        } while (true);

        readThis(Tokens.CLOSEBRACKET);

        return casewhen;
    }

    private Expression readGreatestExpression() {

        // turn into a CASEWHEN
        read();
        readThis(Tokens.OPENBRACKET);

        Expression casewhen = null;

        do {
            casewhen = readValue(casewhen, OpTypes.GREATER);

            if (token.tokenType == Tokens.COMMA) {
                readThis(Tokens.COMMA);
            } else {
                break;
            }
        } while (true);

        readThis(Tokens.CLOSEBRACKET);

        return casewhen;
    }

    private Expression readValue(Expression e, int opType) {

        Expression r = XreadValueExpression();

        if (e == null) {
            return r;
        }

        Expression l = new ExpressionLogical(opType, e, r);
        Expression a = new ExpressionOp(OpTypes.ALTERNATIVE, e, r);

        return new ExpressionOp(OpTypes.CASEWHEN, l, a);
    }

    /**
     * Reads a NULLIF expression
     */
    private Expression readNullIfExpression() {

        // turn into a CASEWHEN
        read();
        readThis(Tokens.OPENBRACKET);

        Expression c = XreadValueExpression();

        readThis(Tokens.COMMA);

        Expression thenelse =
            new ExpressionOp(OpTypes.ALTERNATIVE,
                             new ExpressionValue((Object) null, (Type) null),
                             c);

        c = new ExpressionLogical(c, XreadValueExpression());
        c = new ExpressionOp(OpTypes.CASEWHEN, c, thenelse);

        readThis(Tokens.CLOSEBRACKET);

        return c;
    }

    /**
     * Reads a COALESE or IFNULL expression
     */
    private Expression readCoalesceExpression() {

        Expression c = null;

        // turn into a CASEWHEN
        read();
        readThis(Tokens.OPENBRACKET);

        Expression leaf = null;

        while (true) {
            Expression current = XreadValueExpression();

            if (leaf != null && token.tokenType == Tokens.CLOSEBRACKET) {
                readThis(Tokens.CLOSEBRACKET);
                leaf.setLeftNode(current);

                break;
            }

            Expression condition = new ExpressionLogical(OpTypes.IS_NULL,
                current);
            Expression alternatives = new ExpressionOp(OpTypes.ALTERNATIVE,
                new ExpressionValue((Object) null, (Type) null), current);
            Expression casewhen = new ExpressionOp(OpTypes.CASEWHEN,
                                                   condition, alternatives);

            if (c == null) {
                c = casewhen;
            } else {
                leaf.setLeftNode(casewhen);
            }

            leaf = alternatives;

            readThis(Tokens.COMMA);
        }

        return c;
    }

    Expression readSQLFunction(FunctionSQL function) {

        int position = getPosition();

        read();

        short[] parseList = function.parseList;

        if (parseList.length == 0) {
            return function;
        }

        HsqlArrayList exprList      = new HsqlArrayList();
        boolean       isOpenBracket = token.tokenType == Tokens.OPENBRACKET;

        try {
            readExpression(exprList, parseList, 0, parseList.length, false);
        } catch (HsqlException e) {
            if (!isOpenBracket) {
                rewind(position);

                return null;
            }

            if (function.parseListAlt == null) {
                throw e;
            }

            rewind(position);
            read();

            parseList = function.parseListAlt;
            exprList  = new HsqlArrayList();

            readExpression(exprList, parseList, 0, parseList.length, false);
        }

        Expression[] expr = new Expression[exprList.size()];

        exprList.toArray(expr);
        function.setArguments(expr);

        return function.getFunctionExpression();
    }

    void readExpression(HsqlArrayList exprList, short[] parseList, int start,
                        int count, boolean isOption) {

        for (int i = start; i < start + count; i++) {
            int exprType = parseList[i];

            switch (exprType) {

                case Tokens.QUESTION : {
                    Expression e = null;

                    e = XreadAllTypesCommonValueExpression(false);

                    exprList.add(e);

                    continue;
                }
                case Tokens.X_POS_INTEGER : {
                    Expression e     = null;
                    Integer    value = readIntegerObject();

                    if (value.intValue() < 0) {
                        throw Error.error(ErrorCode.X_42592);
                    }

                    e = new ExpressionValue(value, Type.SQL_INTEGER);

                    exprList.add(e);

                    continue;
                }
                case Tokens.X_OPTION : {
                    i++;

                    int expressionCount  = exprList.size();
                    int position         = getPosition();
                    int elementCount     = parseList[i++];
                    int initialExprIndex = exprList.size();

                    try {
                        readExpression(exprList, parseList, i, elementCount,
                                       true);
                    } catch (HsqlException ex) {
                        ex.setLevel(compileContext.subqueryDepth);

                        if (lastError == null
                                || lastError.getLevel() < ex.getLevel()) {
                            lastError = ex;
                        }

                        rewind(position);
                        exprList.setSize(expressionCount);

                        for (int j = i; j < i + elementCount; j++) {
                            if (parseList[j] == Tokens.QUESTION
                                    || parseList[j] == Tokens.X_KEYSET
                                    || parseList[j] == Tokens.X_POS_INTEGER) {
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
                case Tokens.X_REPEAT : {
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
                case Tokens.X_KEYSET : {
                    int        elementCount = parseList[++i];
                    Expression e            = null;

                    if (ArrayUtil.find(parseList, token.tokenType, i
                                       + 1, elementCount) == -1) {
                        if (!isOption) {
                            throw unexpectedToken();
                        }
                    } else {
                        e = new ExpressionValue(
                            ValuePool.getInt(token.tokenType),
                            Type.SQL_INTEGER);

                        read();
                    }

                    exprList.add(e);

                    i += elementCount;

                    continue;
                }
                case Tokens.OPENBRACKET :
                case Tokens.CLOSEBRACKET :
                case Tokens.COMMA :
                default :
                    if (token.tokenType != exprType) {
                        throw unexpectedToken();
                    }

                    read();

                    continue;
            }
        }
    }

    private Expression readSequenceExpression() {

        read();
        readThis(Tokens.VALUE);
        readThis(Tokens.FOR);
        checkIsSchemaObjectName();

        String schema = session.getSchemaName(token.namePrefix);
        NumberSequence sequence =
            database.schemaManager.getSequence(token.tokenString, schema,
                                               true);
        Token recordedToken = getRecordedToken();

        read();

        Expression e = new ExpressionColumn(sequence);

        recordedToken.setExpression(sequence);
        compileContext.addSequence(sequence);

        return e;
    }

    HsqlName readNewSchemaName() {

        checkIsSchemaObjectName();
        checkValidCatalogName(token.namePrefix);
        SqlInvariants.checkSchemaNameNotSystem(token.tokenString);

        HsqlName name = database.nameManager.newHsqlName(token.tokenString,
            isDelimitedIdentifier(), SchemaObject.SCHEMA);

        read();

        return name;
    }

    HsqlName readNewSchemaObjectName(int type, boolean checkSchema) {

        checkIsSchemaObjectName();

        HsqlName hsqlName = database.nameManager.newHsqlName(token.tokenString,
            isDelimitedIdentifier(), type);

        if (token.namePrefix != null) {
            switch (type) {

                case SchemaObject.LABEL :
                case SchemaObject.VARIABLE :
                case SchemaObject.GRANTEE :
                case SchemaObject.CATALOG :
                    throw unexpectedToken();
                case SchemaObject.CURSOR : {
                    if (token.namePrePrefix == null
                            && Tokens.T_MODULE.equals(token.namePrefix)
                            && !token.isDelimitedPrefix) {}
                    else {
                        throw unexpectedTokenRequire(Tokens.T_MODULE);
                    }

                    break;
                }
                case SchemaObject.SCHEMA : {
                    checkValidCatalogName(token.namePrefix);

                    if (token.namePrePrefix != null) {
                        throw tooManyIdentifiers();
                    }

                    break;
                }
                case SchemaObject.SERVER :
                case SchemaObject.WRAPPER : {
                    checkValidCatalogName(token.namePrefix);

                    if (token.namePrePrefix != null) {
                        throw tooManyIdentifiers();
                    }

                    break;
                }
                case SchemaObject.COLUMN : {
                    if (token.namePrefix != null) {
                        throw tooManyIdentifiers();
                    }

                    break;
                }
                default : {
                    checkValidCatalogName(token.namePrePrefix);

                    HsqlName schemaName;

                    if (checkSchema) {
                        schemaName =
                            session.getSchemaHsqlName(token.namePrefix);
                    } else {
                        schemaName =
                            session.database.schemaManager.findSchemaHsqlName(
                                token.namePrefix);

                        if (schemaName == null) {
                            schemaName = database.nameManager.newHsqlName(
                                token.namePrefix, isDelimitedIdentifier(),
                                SchemaObject.SCHEMA);
                        }
                    }

                    hsqlName.setSchemaIfNull(schemaName);

                    break;
                }
            }
        }

        read();

        return hsqlName;
    }

    HsqlName readNewDependentSchemaObjectName(HsqlName parentName, int type) {

        HsqlName name = readNewSchemaObjectName(type, true);

        name.parent = parentName;

        name.setSchemaIfNull(parentName.schema);

        if (name.schema != null && parentName.schema != null
                && name.schema != parentName.schema) {
            throw Error.error(ErrorCode.X_42505, token.namePrefix);
        }

        return name;
    }

    HsqlName readSchemaName() {

        checkIsSchemaObjectName();
        checkValidCatalogName(token.namePrefix);

        HsqlName schema = session.getSchemaHsqlName(token.tokenString);

        read();

        return schema;
    }

    SchemaObject readSchemaObjectName(int type) {

        checkIsSchemaObjectName();
        checkValidCatalogName(token.namePrePrefix);

        SchemaObject object =
            database.schemaManager.getSchemaObject(token.tokenString,
                token.namePrefix, type);

        read();

        return object;
    }

    SchemaObject readSchemaObjectName(HsqlName schemaName, int type) {

        checkIsSchemaObjectName();

        SchemaObject object =
            database.schemaManager.getSchemaObject(token.tokenString,
                schemaName.name, type);

        if (token.namePrefix != null) {
            if (!token.namePrefix.equals(schemaName.name)) {

                // todo - better error message
                throw Error.error(ErrorCode.X_42505, token.namePrefix);
            }

            if (token.namePrePrefix != null) {
                if (!token.namePrePrefix.equals(
                        database.getCatalogName().name)) {

                    // todo - better error message
                    throw Error.error(ErrorCode.X_42505, token.namePrefix);
                }
            }
        }

        read();

        return object;
    }

    Table readTableName() {

        checkIsIdentifier();

        if (token.namePrePrefix != null) {
            checkValidCatalogName(token.namePrePrefix);
        }

        Table table = database.schemaManager.getTable(session,
            token.tokenString, token.namePrefix);

        getRecordedToken().setExpression(table);
        read();

        return table;
    }

    ColumnSchema readSimpleColumnName(RangeVariable rangeVar) {

        ColumnSchema column = null;

        checkIsIdentifier();

        if (token.namePrefix != null) {
            throw tooManyIdentifiers();
        }

        int index = rangeVar.findColumn(token.tokenString);

        if (index > -1 && rangeVar.resolvesTableName(token.namePrefix)
                && rangeVar.resolvesSchemaName(token.namePrePrefix)) {
            column = rangeVar.getTable().getColumn(index);

            read();

            return column;
        }

        throw Error.error(ErrorCode.X_42501, token.tokenString);
    }

    ColumnSchema readSimpleColumnName(Table table) {

        checkIsIdentifier();

        if (token.namePrefix != null) {
            throw tooManyIdentifiers();
        }

        int index = table.findColumn(token.tokenString);

        if (index == -1) {
            throw Error.error(ErrorCode.X_42501, token.tokenString);
        }

        ColumnSchema column = table.getColumn(index);

        read();

        return column;
    }

    StatementDMQL compileDeclareCursor() {

        int sensitivity   = ResultConstants.SQL_ASENSITIVE;
        int scrollability = ResultConstants.SQL_NONSCROLLABLE;
        int holdability   = ResultConstants.SQL_NONHOLDABLE;
        int returnability = ResultConstants.SQL_WITHOUT_RETURN;

        readThis(Tokens.DECLARE);
        readNewSchemaObjectName(SchemaObject.CURSOR, true);

        switch (token.tokenType) {

            case Tokens.SENSITIVE :
                read();

                sensitivity = ResultConstants.SQL_SENSITIVE;
                break;

            case Tokens.INSENSITIVE :
                read();

                sensitivity = ResultConstants.SQL_INSENSITIVE;
                break;

            case Tokens.ASENSITIVE :
                read();
                break;
        }

        if (token.tokenType == Tokens.NO) {
            readThis(Tokens.SCROLL);
        } else {
            if (token.tokenType == Tokens.SCROLL) {
                read();

                scrollability = ResultConstants.SQL_SCROLLABLE;
            }
        }

        readThis(Tokens.CURSOR);

        for (int round = 0; round < 2; round++) {
            if (token.tokenType == Tokens.WITH) {
                read();

                if (round == 0 && token.tokenType == Tokens.HOLD) {
                    read();

                    holdability = ResultConstants.SQL_HOLDABLE;
                } else {
                    readThis(Tokens.RETURN);

                    round++;

                    returnability = ResultConstants.SQL_WITH_RETURN;
                }
            } else if (token.tokenType == Tokens.WITHOUT) {
                read();

                if (round == 0 && token.tokenType == Tokens.HOLD) {
                    read();
                } else {
                    readThis(Tokens.RETURN);

                    round++;
                }
            }
        }

        readThis(Tokens.FOR);

        int props = ResultProperties.getProperties(sensitivity,
            ResultConstants.SQL_UPDATABLE, scrollability, holdability,
            returnability);
        StatementDMQL cs = compileCursorSpecification(props);

        return cs;
    }

    /**
     * Retrieves a SELECT or other query expression Statement from this parse context.
     */
    StatementDMQL compileCursorSpecification(int props) {

        OrderedHashSet  colNames        = null;
        QueryExpression queryExpression = XreadQueryExpression();

        if (token.tokenType == Tokens.FOR) {
            read();

            if (token.tokenType == Tokens.READ) {
                read();
                readThis(Tokens.ONLY);
            } else {
                readThis(Tokens.UPDATE);

                props = ResultProperties.addUpdatable(props, true);

                if (token.tokenType == Tokens.OF) {
                    readThis(Tokens.OF);

                    colNames = new OrderedHashSet();

                    readColumnNameList(colNames, null, false);
                }
            }
        }

        if (ResultProperties.isUpdatable(props)) {
            queryExpression.isUpdatable = true;
        }

        queryExpression.setReturningResult();
        queryExpression.resolve(session);

        StatementDMQL cs = new StatementQuery(session, queryExpression,
                                              compileContext);

        return cs;
    }

    StatementDMQL compileShortCursorSpecification(int props) {

        QueryExpression queryExpression = XreadQueryExpression();

        if (ResultProperties.isUpdatable(props)) {
            queryExpression.isUpdatable = true;
        }

        queryExpression.setReturningResult();
        queryExpression.resolve(session);

        StatementDMQL cs = new StatementQuery(session, queryExpression,
                                              compileContext);

        return cs;
    }

    int readCloseBrackets(int limit) {

        int count = 0;

        while (count < limit && token.tokenType == Tokens.CLOSEBRACKET) {
            read();

            count++;
        }

        return count;
    }

    int readOpenBrackets() {

        int count = 0;

        while (token.tokenType == Tokens.OPENBRACKET) {
            count++;

            read();
        }

        return count;
    }

    void checkValidCatalogName(String name) {

        if (name != null && !name.equals(database.getCatalogName().name)) {
            throw Error.error(ErrorCode.X_42501, name);
        }
    }

    void rewind(int position) {
        super.rewind(position);
        compileContext.rewind(position);
    }

    public static final class CompileContext {

        final Session    session;
        final ParserBase parser;

        //
        private int           subqueryDepth;
        private HsqlArrayList namedSubqueries;

        //
        private OrderedIntKeyHashMap parameters = new OrderedIntKeyHashMap();
        private HsqlArrayList        subQueryList   = new HsqlArrayList(true);
        private HsqlArrayList        usedSequences  = new HsqlArrayList(true);
        private HsqlArrayList        usedRoutines   = new HsqlArrayList(true);
        private HsqlArrayList        rangeVariables = new HsqlArrayList(true);
        private HsqlArrayList        usedObjects    = new HsqlArrayList(true);
        Type                         currentDomain;
        boolean                      contextuallyTypedExpression;
        Routine                      callProcedure;

        //
        private int rangeVarIndex = 0;

        public CompileContext(Session session, ParserBase parser) {

            this.session = session;
            this.parser  = parser;

            reset();
        }

        public void reset() {

            rangeVarIndex = 0;

            rangeVariables.clear();
            subQueryList.clear();

            subqueryDepth = 0;

            parameters.clear();
            usedSequences.clear();
            usedRoutines.clear();

            callProcedure = null;

            usedObjects.clear();

            //
            currentDomain               = null;
            contextuallyTypedExpression = false;
        }

        public void rewind(int position) {

            for (int i = rangeVariables.size() - 1; i >= 0; i--) {
                RangeVariable range = (RangeVariable) rangeVariables.get(i);

                if (range.parsePosition > position) {
                    rangeVariables.remove(i);
                }
            }

//            rangeVarIndex = rangeVariables.size();
            for (int i = subQueryList.size() - 1; i >= 0; i--) {
                SubQuery subQuery = (SubQuery) subQueryList.get(i);

                if (subQuery.parsePosition >= position) {
                    subQueryList.remove(i);
                }
            }

            Iterator it = parameters.keySet().iterator();

            while (it.hasNext()) {
                int pos = it.nextInt();

                if (pos >= position) {
                    it.remove();
                }
            }
        }

        public void registerRangeVariable(RangeVariable range) {

            range.parsePosition = parser == null ? 0
                                                 : parser.getPosition();
            range.rangePosition = getNextRangeVarIndex();
            range.level         = subqueryDepth;

            rangeVariables.add(range);
        }

        public int getNextRangeVarIndex() {
            return rangeVarIndex++;
        }

        public int getRangeVarCount() {
            return rangeVarIndex;
        }

        public RangeVariable[] getRangeVariables() {

            RangeVariable[] array = new RangeVariable[rangeVariables.size()];

            rangeVariables.toArray(array);

            return array;
        }

        public NumberSequence[] getSequences() {

            if (usedSequences.size() == 0) {
                return NumberSequence.emptyArray;
            }

            NumberSequence[] array = new NumberSequence[usedSequences.size()];

            usedSequences.toArray(array);

            return array;
        }

        public Routine[] getRoutines() {

            if (callProcedure == null && usedRoutines.size() == 0) {
                return Routine.emptyArray;
            }

            OrderedHashSet set = new OrderedHashSet();

            for (int i = 0; i < usedRoutines.size(); i++) {
                FunctionSQLInvoked function =
                    (FunctionSQLInvoked) usedRoutines.get(i);

                set.add(function.routine);
            }

            if (callProcedure != null) {
                set.add(callProcedure);
            }

            Routine[] array = new Routine[set.size()];

            set.toArray(array);

            return array;
        }

        private void initSubqueryNames() {

            if (namedSubqueries == null) {
                namedSubqueries = new HsqlArrayList();
            }

            if (namedSubqueries.size() <= subqueryDepth) {
                namedSubqueries.setSize(subqueryDepth + 1);
            }

            HashMappedList set =
                (HashMappedList) namedSubqueries.get(subqueryDepth);

            if (set == null) {
                set = new HashMappedList();

                namedSubqueries.set(subqueryDepth, set);
            } else {
                set.clear();
            }
        }

        private void registerSubquery(String name, SubQuery subquery) {

            HashMappedList set =
                (HashMappedList) namedSubqueries.get(subqueryDepth);
            boolean added = set.add(name, subquery);

            if (!added) {
                throw Error.error(ErrorCode.X_42504);
            }
        }

        private SubQuery getNamedSubQuery(String name) {

            if (namedSubqueries == null) {
                return null;
            }

            for (int i = subqueryDepth; i >= 0; i--) {
                if (namedSubqueries.size() <= i) {
                    continue;
                }

                HashMappedList set = (HashMappedList) namedSubqueries.get(i);

                if (set == null) {
                    continue;
                }

                SubQuery sq = (SubQuery) set.get(name);

                if (sq != null) {
                    return sq;
                }
            }

            return null;
        }

        private void addParameter(Expression e, int position) {
            parameters.put(position, e);
        }

        private void addSchemaObject(SchemaObject object) {
            usedObjects.add(object);
        }

        private void addSequence(SchemaObject object) {
            usedSequences.add(object);
        }

        void addFunctionCall(FunctionSQLInvoked function) {
            usedRoutines.add(function);
        }

        void addProcedureCall(Routine procedure) {
            callProcedure = procedure;
        }

        ExpressionColumn[] getParameters() {

            if (parameters.size() == 0) {
                return ExpressionColumn.emptyArray;
            }

            ExpressionColumn[] result =
                new ExpressionColumn[parameters.size()];

            parameters.valuesToArray(result);
            parameters.clear();

            return result;
        }

        void clearParameters() {
            parameters.clear();
        }

        public OrderedHashSet getSchemaObjectNames() {

            OrderedHashSet set = new OrderedHashSet();

            for (int i = 0; i < usedSequences.size(); i++) {
                SchemaObject object = (SchemaObject) usedSequences.get(i);

                set.add(object.getName());
            }

            for (int i = 0; i < usedObjects.size(); i++) {
                SchemaObject object = (SchemaObject) usedObjects.get(i);

                set.add(object.getName());
            }

            for (int i = 0; i < rangeVariables.size(); i++) {
                RangeVariable range = (RangeVariable) rangeVariables.get(i);
                HsqlName      name  = range.rangeTable.getName();

                if (name.schema != SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                    set.add(range.rangeTable.getName());
                    set.addAll(range.getColumnNames());
                } else if (name.type == SchemaObject.TRANSITION) {
                    set.addAll(range.getColumnNames());
                }
            }

            Routine[] routines = getRoutines();

            for (int i = 0; i < routines.length; i++) {
                set.add(routines[i].getSpecificName());
            }

            return set;
        }
    }
}
