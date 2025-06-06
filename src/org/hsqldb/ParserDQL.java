/* Copyright (c) 2001-2025, The HSQL Development Group
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
import org.hsqldb.QueryExpression.RecursiveQuerySettings;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.error.HsqlException;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.List;
import org.hsqldb.lib.LongDeque;
import org.hsqldb.lib.OrderedHashMap;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.OrderedIntKeyHashMap;
import org.hsqldb.map.BitMap;
import org.hsqldb.map.ValuePool;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.result.ResultProperties;
import org.hsqldb.types.ArrayType;
import org.hsqldb.types.BinaryType;
import org.hsqldb.types.BlobType;
import org.hsqldb.types.CharacterType;
import org.hsqldb.types.Charset;
import org.hsqldb.types.ClobType;
import org.hsqldb.types.Collation;
import org.hsqldb.types.DTIType;
import org.hsqldb.types.IntervalType;
import org.hsqldb.types.NumberType;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;
import org.hsqldb.types.UserTypeModifier;

/**
 * Parser for DQL statements
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.7.4
 * @since 1.9.0
 */
public class ParserDQL extends ParserBase {

    protected Database             database;
    protected Session              session;
    protected final CompileContext compileContext;

    /**
     *  Constructs a new Parser object with the given context.
     *
     * @param  session the connected context
     * @param  scanner the token source from which to parse commands
     */
    ParserDQL(Session session, Scanner scanner, CompileContext baseContext) {

        super(scanner);

        this.session        = session;
        this.database       = session.getDatabase();
        this.compileContext = new CompileContext(session, this, baseContext);
    }

    /**
     *  Resets this parse context with the given SQL character sequence.
     *
     * @param sql a new SQL character sequence to replace the current one
     */
    void reset(Session session, String sql) {

        super.reset(session, sql);
        compileContext.reset();

        lastError = null;
    }

    void checkIsSchemaObjectName() {

        if (database.sqlEnforceNames) {
            checkIsNonReservedIdentifier();
        } else {
            checkIsNonCoreReservedIdentifier();
        }

        if (database.sqlRegularNames) {
            checkIsIrregularCharInIdentifier();
        }
    }

    Type readTypeDefinition(boolean allowCollation, boolean includeUserTypes) {

        int     typeNumber       = Integer.MIN_VALUE;
        boolean hasLength        = false;
        boolean hasScale         = false;
        boolean isCharacter      = false;
        boolean isIgnoreCase     = false;
        boolean readByteOrChar   = false;
        boolean enforceSize      = database.sqlEnforceSize;
        boolean acceptsPrecision = true;
        long    length           = 0;
        int     scale            = 0;

        checkIsIdentifier();

        if (token.namePrefix == null) {
            typeNumber = Type.getTypeNr(token.tokenString);
        }

        if (database.sqlSyntaxOra && !session.isProcessingScript()) {
            if (typeNumber == Types.SQL_DATE) {
                read();

                return Type.SQL_TIMESTAMP_NO_FRACTION;
            }
        }

        if (typeNumber == Integer.MIN_VALUE) {
            if (includeUserTypes) {
                checkIsSchemaObjectName();

                Type type = database.schemaManager.findDomainOrUDT(
                    session,
                    token.tokenString,
                    token.namePrefix,
                    token.namePrePrefix,
                    token.namePrePrePrefix);

                if (type != null) {
                    getRecordedToken().setExpression(type);
                    compileContext.addDomainOrType(type);
                    read();

                    return type;
                }
            }

            if (token.namePrefix != null) {
                throw Error.error(ErrorCode.X_42509, token.tokenString);
            }

            if (database.sqlSyntaxOra) {
                switch (token.tokenType) {

                    case Tokens.BINARY_DOUBLE :
                    case Tokens.BINARY_FLOAT :
                        read();

                        return Type.SQL_DOUBLE;

                    case Tokens.LONG :
                        read();

                        if (token.tokenType == Tokens.RAW) {
                            read();

                            return Type.getType(
                                Types.SQL_VARBINARY,
                                null,
                                null,
                                BlobType.defaultBlobSize,
                                0);
                        } else {
                            readIfThis(Tokens.VARCHAR);

                            return Type.getType(
                                Types.SQL_VARCHAR,
                                null,
                                database.collation,
                                ClobType.defaultClobSize,
                                0);
                        }
                    case Tokens.NUMBER :
                        read();

                        if (token.tokenType == Tokens.OPENBRACKET) {
                            read();

                            int precision;

                            if (token.tokenType == Tokens.ASTERISK) {
                                read();

                                precision = 40;
                            } else {
                                precision = readInteger();
                            }

                            scale = 0;

                            if (token.tokenType == Tokens.COMMA) {
                                read();

                                scale = readInteger();
                            }

                            readThis(Tokens.CLOSEBRACKET);

                            return Type.getType(
                                Types.SQL_DECIMAL,
                                null,
                                null,
                                precision,
                                scale);
                        } else {
                            return Type.SQL_DECIMAL_DEFAULT;
                        }
                    case Tokens.RAW :
                        typeNumber = Types.SQL_VARBINARY;
                        break;

                    case Tokens.VARCHAR2 :
                        readByteOrChar = true;
                        enforceSize    = false;
                        typeNumber     = Types.SQL_VARCHAR;
                        break;

                    case Tokens.NVARCHAR2 :
                        typeNumber = Types.SQL_VARCHAR;
                        break;

                    case Tokens.NCHAR :
                        typeNumber = Types.SQL_CHAR;
                        break;

                    default :
                }
            }

            if (database.sqlSyntaxMss) {
                if (Tokens.T_MONEY.equals(token.tokenString)) {
                    return Type.getType(Types.SQL_DECIMAL, null, null, 18, 4);
                } else if (Tokens.T_UNIQUEIDENTIFIER.equals(
                        token.tokenString)) {
                    return Type.SQL_GUID;
                } else if (Tokens.T_DATETIME2.equals(token.tokenString)) {
                    typeNumber = Types.SQL_TIMESTAMP;
                } else if (Tokens.T_IMAGE.equals(token.tokenString)) {
                    typeNumber = Types.LONGVARBINARY;
                } else if (Tokens.T_NTEXT.equals(token.tokenString)) {
                    typeNumber = Types.LONGVARCHAR;
                }

                switch (token.tokenType) {

                    case Tokens.DATETIMEOFFSET :
                        typeNumber = Types.SQL_TIMESTAMP_WITH_TIME_ZONE;
                        break;

                    case Tokens.TEXT :
                        typeNumber = Types.LONGVARCHAR;
                        break;
                }
            }

            if (database.sqlSyntaxPgs) {
                switch (token.tokenType) {

                    case Tokens.TEXT :
                        typeNumber     = Types.LONGVARCHAR;
                        readByteOrChar = true;
                        break;

                    case Tokens.CITEXT :
                        typeNumber = Types.VARCHAR_IGNORECASE;
                        break;

                    default :
                        if ("BYTEA".equals(token.tokenString)) {
                            typeNumber = Types.LONGVARBINARY;
                        } else if ("INT2".equals(token.tokenString)) {
                            typeNumber = Types.SQL_SMALLINT;
                        } else if ("INT4".equals(token.tokenString)) {
                            typeNumber = Types.SQL_INTEGER;
                        } else if ("INT8".equals(token.tokenString)) {
                            typeNumber = Types.SQL_BIGINT;
                        } else if ("REAL".equals(token.tokenString)) {
                            typeNumber = Types.SQL_DOUBLE;
                        } else if ("TIMESTAMPTZ".equals(token.tokenString)) {
                            typeNumber = Types.SQL_TIMESTAMP_WITH_TIME_ZONE;
                            scale = DTIType.defaultTimestampFractionPrecision;
                        }
                }
            }

            if (database.sqlSyntaxMys) {
                switch (token.tokenType) {

                    case Tokens.ENUM :
                        return readMysEnum();

                    case Tokens.TINYTEXT :
                        typeNumber       = Types.VARCHAR;
                        acceptsPrecision = false;
                        break;

                    case Tokens.TEXT :
                    case Tokens.MEDIUMTEXT :
                    case Tokens.LONGTEXT :
                        typeNumber       = Types.LONGVARCHAR;
                        acceptsPrecision = false;
                        break;

                    case Tokens.TINYBLOB :
                        typeNumber       = Types.VARBINARY;
                        acceptsPrecision = false;
                        break;

                    case Tokens.MEDIUMBLOB :
                    case Tokens.LONGBLOB :
                        typeNumber       = Types.LONGVARBINARY;
                        acceptsPrecision = false;
                        break;
                }
            }

            if (typeNumber == Integer.MIN_VALUE) {
                throw Error.error(ErrorCode.X_42509, token.tokenString);
            }
        }

        read();

        switch (typeNumber) {

            case Types.SQL_CHAR :
                if (token.tokenType == Tokens.VARYING) {
                    read();

                    typeNumber = Types.SQL_VARCHAR;
                } else if (token.tokenType == Tokens.LARGE) {
                    read();
                    readThis(Tokens.OBJECT);

                    typeNumber = Types.SQL_CLOB;
                } else {
                    if (database.sqlSyntaxOra) {
                        readByteOrChar = true;
                    }
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
                    read();
                    readThis(Tokens.OBJECT);

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
                return readIntervalType(session, false);

            default :
        }

        if (typeNumber == Types.SQL_TIMESTAMP) {
            length = DTIType.defaultTimestampFractionPrecision;
        }

        acceptsPrecision &= Types.acceptsPrecision(typeNumber);

        if (acceptsPrecision
                && enforceSize
                && Types.requiresPrecision(typeNumber)) {
            if (token.tokenType != Tokens.OPENBRACKET
                    && !session.isProcessingScript()) {
                throw Error.error(
                    ErrorCode.X_42599,
                    Type.getDefaultType(typeNumber).getNameString());
            }
        }

        if (database.sqlSyntaxMys) {
            switch (typeNumber) {

                case Types.TINYINT :
                case Types.SQL_INTEGER :
                case Types.SQL_SMALLINT :
                case Types.SQL_BIGINT :
                    acceptsPrecision = true;
                    break;
            }
        }

        if (database.sqlSyntaxMss) {
            switch (typeNumber) {
                case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                    acceptsPrecision = true;
                    break;
            }
        }

        if (acceptsPrecision) {
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
                                || typeNumber == Types.SQL_CLOB
                                || typeNumber == Types.SQL_VARBINARY
                                || typeNumber == Types.SQL_VARCHAR) {
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
                    case Tokens.MAX :
                        if (database.sqlSyntaxMss) {
                            token.tokenValue = Long.valueOf(Integer.MAX_VALUE);
                            break;
                        }

                        throw unexpectedToken();

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
                    }
                }

                if (Types.acceptsScaleCreateParam(typeNumber)
                        && token.tokenType == Tokens.COMMA) {
                    read();

                    scale = readInteger();

                    if (scale < 0) {
                        if (typeNumber == Types.SQL_DECIMAL
                                && database.sqlSyntaxOra) {

                            //
                        } else {
                            throw Error.error(ErrorCode.X_42592);
                        }
                    }

                    if (typeNumber == Types.SQL_DECIMAL && scale > length) {
                        throw Error.error(ErrorCode.X_42592);
                    }

                    hasScale = true;
                }

                if (readByteOrChar) {
                    if (!readIfThis(Tokens.CHAR)) {
                        readIfThis(Tokens.BYTE);
                    }
                }

                readThis(Tokens.CLOSEBRACKET);
            } else if (typeNumber == Types.SQL_BIT) {
                length = 1;
            } else if (typeNumber == Types.SQL_BLOB
                       || typeNumber == Types.SQL_CLOB) {
                length = BlobType.defaultBlobSize;
            } else if (enforceSize) {

                // BIT is always BIT(1), regardless of sqlEnforceSize
                if (typeNumber == Types.SQL_CHAR
                        || typeNumber == Types.SQL_BINARY) {
                    length = 1;
                }
            }

            switch (typeNumber) {

                case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                case Types.SQL_TIMESTAMP :
                case Types.SQL_TIME :
                    if (length > DTIType.maxFractionPrecision) {
                        throw Error.error(ErrorCode.X_42592);
                    }

                    scale  = (int) length;
                    length = 0;

                    if (typeNumber == Types.SQL_TIMESTAMP_WITH_TIME_ZONE) {
                        break;
                    }

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
                if (database.sqlLongvarIsLob) {
                    typeNumber = Types.SQL_CLOB;
                    length     = ClobType.defaultClobSize;
                } else {
                    typeNumber = Types.SQL_VARCHAR;

                    if (!hasLength) {
                        length = ClobType.defaultShortClobSize;
                    }
                }

                break;
            }

            case Types.LONGVARBINARY : {
                if (database.sqlLongvarIsLob) {
                    typeNumber = Types.SQL_BLOB;
                    length     = BlobType.defaultBlobSize;
                } else {
                    typeNumber = Types.SQL_VARBINARY;

                    if (!hasLength) {
                        length = BlobType.defaultShortBlobSize;
                    }
                }

                break;
            }

            case Types.SQL_CHAR :
                if (database.sqlSyntaxDb2) {
                    if (readIfThis(Tokens.FOR)) {
                        readThis(Tokens.BIT);
                        readThis(Tokens.DATA);

                        typeNumber = Types.SQL_BINARY;
                        break;
                    }
                }

                isCharacter = true;
                break;

            case Types.SQL_CLOB :
                isCharacter = true;
                break;

            case Types.VARCHAR_IGNORECASE :
                typeNumber   = Types.SQL_VARCHAR;
                isIgnoreCase = true;

            // fall through
            case Types.SQL_VARCHAR :
                if (database.sqlSyntaxDb2) {
                    if (readIfThis(Tokens.FOR)) {
                        readThis(Tokens.BIT);
                        readThis(Tokens.DATA);

                        typeNumber = Types.SQL_VARBINARY;

                        if (!hasLength) {
                            length = 32 * 1024;
                        }

                        break;
                    }
                }

                isCharacter = true;

                if (!hasLength) {
                    length = 32 * 1024;
                }

                if (session.isIgnorecase()) {
                    if (!session.isProcessingScript()) {
                        isIgnoreCase = true;
                    }
                }

                if (length > CharacterType.maxCharPrecision) {
                    throw Error.error(ErrorCode.X_42592);
                }

                break;

            case Types.SQL_BINARY :
                break;

            case Types.SQL_VARBINARY :
                if (!hasLength) {
                    length = 32 * 1024;
                }

                if (length > BinaryType.maxBinaryPrecision) {
                    throw Error.error(ErrorCode.X_42592);
                }

                break;

            case Types.SQL_DECIMAL :
            case Types.SQL_NUMERIC :
                if (!hasLength && !hasScale && !enforceSize) {
                    length = NumberType.defaultNumericPrecision;
                    scale  = NumberType.defaultNumericScale;
                }

                break;
        }

        Collation collation = database.collation;
        Charset   charset   = null;

        if (isCharacter && allowCollation) {
            if (token.tokenType == Tokens.CHARACTER) {
                read();
                readThis(Tokens.SET);
                checkIsSchemaObjectName();

                charset = (Charset) database.schemaManager.getCharacterSet(
                    session,
                    token.tokenString,
                    token.namePrefix);

                read();
            }

            if (token.tokenType == Tokens.COLLATE) {
                read();
                checkIsSchemaObjectName();

                collation = database.schemaManager.getCollation(
                    session,
                    token.tokenString,
                    token.namePrefix);

                read();
            } else if (isIgnoreCase) {
                collation = Collation.getUpperCaseCompareCollation(collation);
            }
        }

        Type typeObject = Type.getType(
            typeNumber,
            charset,
            collation,
            length,
            scale);

        if (token.tokenType == Tokens.ARRAY) {
            if (typeObject.isLobType()) {
                throw unexpectedToken();
            }

            read();

            int maxCardinality = ArrayType.defaultArrayCardinality;

            if (token.tokenType == Tokens.LEFTBRACKET) {
                read();

                maxCardinality = readInteger();

                if (maxCardinality < 0) {
                    throw Error.error(ErrorCode.X_42592);
                }

                readThis(Tokens.RIGHTBRACKET);
            }

            typeObject = new ArrayType(typeObject, maxCardinality);
        }

        return typeObject;
    }

    Type readMysEnum() {

        read();
        checkIsThis(Tokens.OPENBRACKET);

        HsqlName name = database.nameManager.newHsqlName(
            "ENUM",
            false,
            SchemaObject.DOMAIN);
        Type t = Type.getType(Types.SQL_VARCHAR, null, null, 32, 0);

        t.userTypeModifier = new UserTypeModifier(name, SchemaObject.DOMAIN, t);

        return t;
    }

    void readSimpleColumnNames(
            OrderedHashSet<String> columns,
            RangeVariable rangeVar,
            boolean withPrefix) {

        while (true) {
            ColumnSchema col = readSimpleColumnName(rangeVar, withPrefix);

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

    void readTargetSpecificationList(
            OrderedHashSet<Expression> targets,
            RangeVariable[] rangeVars,
            LongDeque colIndexList) {

        while (true) {
            Expression target = XreadTargetSpecification(
                rangeVars,
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

    /**
     * Process a bracketed column list as used in the declaration of SQL
     * CONSTRAINTS and return an array containing the indexes of the columns
     * within the table.
     *
     * @param table table that contains the columns
     * @param ascOrDesc boolean
     * @return array of column indexes
     */
    int[] readColumnList(Table table, boolean ascOrDesc) {
        OrderedHashSet<String> set = readColumnNames(ascOrDesc);

        return table.getColumnIndexes(set);
    }

    void readSimpleColumnNames(
            OrderedHashSet<String> columns,
            Table table,
            boolean withPrefix) {

        while (true) {
            ColumnSchema col = readSimpleColumnName(table, withPrefix);

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

        BitMap                 quotedFlags = new BitMap(0, true);
        OrderedHashSet<String> set         = readColumnNames(
            quotedFlags,
            false);
        HsqlName[]             colList     = new HsqlName[set.size()];

        for (int i = 0; i < colList.length; i++) {
            String  name   = set.get(i);
            boolean quoted = quotedFlags.isSet(i);

            colList[i] = database.nameManager.newHsqlName(
                tableName.schema,
                name,
                quoted,
                SchemaObject.COLUMN,
                tableName);
        }

        return colList;
    }

    OrderedHashSet<String> readColumnNames(boolean readAscDesc) {
        return readColumnNames(null, readAscDesc);
    }

    OrderedHashSet<String> readColumnNames(
            BitMap quotedFlags,
            boolean readAscDesc) {

        readThis(Tokens.OPENBRACKET);

        OrderedHashSet<String> set = new OrderedHashSet<>();

        readColumnNameList(set, quotedFlags, readAscDesc);
        readThis(Tokens.CLOSEBRACKET);

        return set;
    }

    void readColumnNameList(
            OrderedHashSet<String> set,
            BitMap quotedFlags,
            boolean readAscDesc) {

        int i = 0;

        while (true) {
            if (session.isProcessingScript()) {

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

            if (quotedFlags != null) {
                quotedFlags.setValue(i, isDelimitedIdentifier());
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

    SimpleName[] readColumnNameList(OrderedHashSet<String> set) {

        BitMap columnNameQuoted = new BitMap(0, true);

        readThis(Tokens.OPENBRACKET);
        readColumnNameList(set, columnNameQuoted, false);
        readThis(Tokens.CLOSEBRACKET);

        SimpleName[] columnNameList = new SimpleName[set.size()];

        for (int i = 0; i < set.size(); i++) {
            SimpleName name = HsqlNameManager.getSimpleName(
                set.get(i),
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

                OrderedHashSet<String> names = readColumnNames(false);

                queryExpression.setUnionCorrespondingColumns(names);
            }
        }
    }

    QueryExpression XreadQueryExpression() {

        try {
            XreadWithClause();

            QueryExpression queryExpression = XreadQueryExpressionBody();
            SortAndSlice    sortAndSlice    = XreadOrderByExpression();

            if (queryExpression.sortAndSlice == SortAndSlice.noSort) {
                queryExpression.addSortAndSlice(sortAndSlice);
            } else {
                if (queryExpression.sortAndSlice.hasLimit()) {
                    if (sortAndSlice.hasLimit()) {
                        throw Error.error(ErrorCode.X_42549);
                    }

                    for (int i = 0; i < sortAndSlice.exprList.size(); i++) {
                        Expression e = sortAndSlice.exprList.get(i);

                        queryExpression.sortAndSlice.addOrderExpression(e);
                    }
                } else {
                    if (sortAndSlice != SortAndSlice.noSort) {
                        queryExpression.addSortAndSlice(sortAndSlice);
                    }
                }
            }

            return queryExpression;
        } finally {
            compileContext.unregisterSubqueries();
        }
    }

    void XreadWithClause() {

        if (token.tokenType == Tokens.WITH) {
            read();
            compileContext.unregisterSubqueries();
            compileContext.registerSubquery(SqlInvariants.RECURSIVE_TABLE);

            boolean recursive = readIfThis(Tokens.RECURSIVE);

            while (true) {
                checkIsSimpleName();

                HsqlName[] nameList = null;
                HsqlName queryName = database.nameManager.newHsqlName(
                    token.tokenString,
                    isDelimitedIdentifier(),
                    SchemaObject.SUBQUERY);

                queryName.schema = SqlInvariants.SYSTEM_SCHEMA_HSQLNAME;

                read();

                if (token.tokenType == Tokens.OPENBRACKET) {
                    nameList = readColumnNames(queryName);
                } else if (recursive) {
                    throw unexpectedTokenRequire(Tokens.T_OPENBRACKET);
                }

                readThis(Tokens.AS);
                readThis(Tokens.OPENBRACKET);

                TableDerived td;

                compileContext.registerSubquery(queryName.name);

                td = XreadTableNamedSubqueryBody(queryName, nameList);

                if (nameList == null) {
                    boolean[] cols = td.queryExpression.accessibleColumns;

                    if (ArrayUtil.countTrueElements(cols) < cols.length) {
                        throw Error.error(ErrorCode.X_42578);
                    }
                }

                readThis(Tokens.CLOSEBRACKET);

                if (td.queryExpression != null
                        && td.queryExpression.isRecursive()) {
                    XreadRecursiveFeatures(td);
                }

                compileContext.registerSubquery(queryName.name, td);
                compileContext.registerSubquery(
                    SqlInvariants.RECURSIVE_TABLE,
                    null);

                if (token.tokenType == Tokens.COMMA) {
                    read();
                    continue;
                }

                break;
            }
        }
    }

    void XreadRecursiveFeatures(TableDerived table) {

        RecursiveQuerySettings settings = null;

        if (token.tokenType == Tokens.SEARCH) {
            read();

            settings = new RecursiveQuerySettings();

            switch (token.tokenType) {

                case Tokens.DEPTH :
                    read();

                    settings.searchOrderType =
                        RecursiveQuerySettings.depthFirst;
                    break;

                case Tokens.BREADTH :
                    read();

                    settings.searchOrderType =
                        RecursiveQuerySettings.breadthFirst;
                    break;

                default :
                    throw unexpectedTokenRequire("DEPTH or BREADTH");
            }

            readThis(Tokens.FIRST);
            readThis(Tokens.BY);

            settings.searchOrderSort = XreadOrderBy();

            readThis(Tokens.SET);
            checkIsSimpleName();

            HsqlName searchOrderName = database.nameManager.newColumnHsqlName(
                table.getName(),
                token.tokenString,
                token.isDelimitedIdentifier);

            if (table.findColumn(searchOrderName.name) != -1) {
                throw Error.error(ErrorCode.X_42578, token.tokenString);
            }

            read();

            settings.searchOrderSetColumn = new ColumnSchema(
                searchOrderName,
                Type.SQL_INTEGER,
                true,
                false,
                null);
        }

        if (token.tokenType == Tokens.CYCLE) {
            read();

            if (settings == null) {
                settings = new RecursiveQuerySettings();
            }

            OrderedHashSet<String> set = new OrderedHashSet<>();

            readColumnNameList(set, null, false);

            settings.cycleColumnList = table.getColumnIndexes(set);

            if (set.size() > 1) {
                throw Error.error(ErrorCode.X_42564, token.tokenString);
            }

            settings.cycleColumnFirst = table.getColumn(
                settings.cycleColumnList[0]);

            readThis(Tokens.SET);
            checkIsSimpleName();

            HsqlName cycleName = database.nameManager.newColumnHsqlName(
                table.getName(),
                token.tokenString,
                token.isDelimitedIdentifier);

            settings.cycleMarkColumn = new ColumnSchema(
                cycleName,
                Type.SQL_CHAR,
                false,
                false,
                null);

            if (table.findColumn(cycleName.name) != -1) {
                throw Error.error(ErrorCode.X_42578, token.tokenString);
            }

            read();
            readThis(Tokens.TO);

            settings.cycleMarkValue = readQuotedString();

            if (settings.cycleMarkValue.length() != 1) {
                throw unexpectedToken(settings.cycleMarkValue);
            }

            readThis(Tokens.DEFAULT);

            settings.noCycleMarkValue = readQuotedString();

            if (settings.noCycleMarkValue.length() != 1) {
                throw unexpectedToken(settings.noCycleMarkValue);
            }

            if (settings.cycleMarkValue.equals(settings.noCycleMarkValue)) {
                throw unexpectedToken(settings.cycleMarkValue);
            }

            if (readIfThis(Tokens.USING)) {
                checkIsSimpleName();

                HsqlName pathName = database.nameManager.newColumnHsqlName(
                    table.getName(),
                    token.tokenString,
                    token.isDelimitedIdentifier);

                if (table.findColumn(pathName.name) != -1) {
                    throw Error.error(ErrorCode.X_42578, token.tokenString);
                }

                read();

                Type arrayType = new ArrayType(
                    settings.cycleColumnFirst.getDataType(),
                    ArrayType.defaultArrayCardinality);

                settings.cyclePathColumn = new ColumnSchema(
                    pathName,
                    arrayType,
                    false,
                    false,
                    null);
            }
        }

        table.queryExpression.setRecursiveQuerySettings(settings);
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

    private QueryExpression XreadSetOperation(QueryExpression queryExpression) {

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

                if (queryExpression.sortAndSlice == SortAndSlice.noSort) {
                    queryExpression.addSortAndSlice(sortAndSlice);
                } else {
                    if (queryExpression.sortAndSlice.hasLimit()) {
                        if (sortAndSlice.hasLimit()) {
                            throw Error.error(ErrorCode.X_42549);
                        }

                        for (int i = 0; i < sortAndSlice.exprList.size(); i++) {
                            Expression e = sortAndSlice.exprList.get(i);

                            queryExpression.sortAndSlice.addOrderExpression(e);
                        }
                    } else {
                        if (sortAndSlice != SortAndSlice.noSort) {
                            queryExpression.addSortAndSlice(sortAndSlice);
                        }
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

                Table table = readNamedSubqueryOrNull();

                if (table == null) {
                    table = readTableName(true);
                }

                if (table.isView()) {
                    table = ((View) table).newDerivedTable(
                        session,
                        compileContext);
                }

                select = new QuerySpecification(
                    session,
                    table,
                    compileContext,
                    false);
                break;
            }

            case Tokens.VALUES : {
                read();

                TableDerived td = XreadRowValueExpressionList();

                select = new QuerySpecification(
                    session,
                    td,
                    compileContext,
                    true);
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

        if (!select.isValueList && select.getCurrentRangeVariableCount() == 0) {
            XreadTableExpression(select);
        }

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
            select.setDistinctSelect();
            read();
        } else if (token.tokenType == Tokens.ALL) {
            read();
        }

        while (true) {
            Expression e = XreadValueExpression();

            if (token.tokenType == Tokens.AS) {
                if (e.getType() == OpTypes.MULTICOLUMN) {
                    throw unexpectedToken();
                }

                read();
                checkIsNonCoreReservedIdentifier();
            }

            if (isNonCoreReservedIdentifier()) {
                if (e.getType() == OpTypes.MULTICOLUMN) {
                    throw unexpectedToken();
                }

                e.setAlias(
                    HsqlNameManager.getSimpleName(token.tokenString,
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

            if (token.tokenType == Tokens.CLOSEBRACKET
                    || token.tokenType == Tokens.X_ENDPARSE
                    || token.tokenType == Tokens.SEMICOLON) {
                if (database.sqlSyntaxMss
                        || database.sqlSyntaxMys
                        || database.sqlSyntaxPgs) {
                    RangeVariable range = new RangeVariable(
                        database.schemaManager.dualTable,
                        null,
                        null,
                        null,
                        compileContext);

                    select.addRangeVariable(session, range);

                    return select;
                }
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

                // set last range as boundary
                continue;
            }

            break;
        }
    }

    void XreadTableReference(QuerySpecification select) {

        boolean       natural   = false;
        RangeVariable leftRange = readTableOrSubquery();

        select.addRangeVariable(session, leftRange);

        while (true) {
            int     type;
            boolean left  = false;
            boolean right = false;
            boolean end   = false;

            type = token.tokenType;

            switch (token.tokenType) {

                case Tokens.NATURAL :
                    if (natural) {
                        throw unexpectedToken();
                    }

                    read();

                    natural = true;
                    continue;
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

                        left  = true;
                        right = true;
                    } else {
                        rewind(position);

                        end = true;
                    }

                    break;

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

            RangeVariable rightRange = readTableOrSubquery();
            Expression    condition  = null;

            rightRange.setJoinType(left, right);

            switch (type) {

                case Tokens.CROSS :
                    select.addRangeVariable(session, rightRange);
                    break;

                case Tokens.UNION :
                    condition = Expression.EXPR_FALSE;

                    rightRange.addJoinCondition(condition);
                    select.addRangeVariable(session, rightRange);
                    break;

                case Tokens.LEFT :
                case Tokens.RIGHT :
                case Tokens.INNER :
                case Tokens.FULL : {
                    boolean using = token.tokenType == Tokens.USING;

                    if (natural || using) {
                        leftRange.resolveRangeTable(
                            session,
                            RangeGroup.emptyGroup,
                            compileContext.getOuterRanges());
                        rightRange.resolveRangeTable(
                            session,
                            RangeGroup.emptyGroup,
                            compileContext.getOuterRanges());
                    }

                    if (natural) {
                        OrderedHashSet<String> columns =
                            rightRange.getUniqueColumnNameSet();

                        condition = select.getEquiJoinExpressions(
                            columns,
                            rightRange,
                            false);

                        rightRange.addJoinCondition(condition);
                        select.addRangeVariable(session, rightRange);
                    } else if (using) {
                        read();

                        OrderedHashSet<String> columns = new OrderedHashSet<>();

                        readThis(Tokens.OPENBRACKET);
                        readSimpleColumnNames(columns, rightRange, false);
                        readThis(Tokens.CLOSEBRACKET);

                        condition = select.getEquiJoinExpressions(
                            columns,
                            rightRange,
                            true);

                        rightRange.addJoinCondition(condition);
                        select.addRangeVariable(session, rightRange);
                    } else if (token.tokenType == Tokens.ON) {
                        read();

                        condition = XreadBooleanValueExpression();

                        rightRange.addJoinCondition(condition);
                        select.addRangeVariable(session, rightRange);
                    } else {
                        throw unexpectedToken();
                    }

                    break;
                }
            }

            natural = false;
        }
    }

    Expression getRowExpression(OrderedHashSet<String> columnNames) {

        Expression[] elements = new Expression[columnNames.size()];

        for (int i = 0; i < elements.length; i++) {
            String name = columnNames.get(i);

            elements[i] = new ExpressionColumn(null, null, name);
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

            if (readIfThis(Tokens.DISTINCT)) {
                select.setDistinctGroups();
            } else {
                readIfThis(Tokens.ALL);
            }

            HsqlArrayList<Expression> expressions = new HsqlArrayList<>();

            while (true) {
                Expression e = XreadGroupByExpressionPrimary();

                expressions.add(e);

                if (token.tokenType == Tokens.COMMA) {
                    read();
                    continue;
                }

                break;
            }

            Expression[] exprArray = new Expression[expressions.size()];

            expressions.toArray(exprArray);
            select.addGroupingSets(exprArray);
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

        if (token.tokenType == Tokens.LIMIT
                || token.tokenType == Tokens.FETCH
                || token.tokenType == Tokens.OFFSET) {
            if (sortAndSlice == null) {
                sortAndSlice = new SortAndSlice();
            }

            XreadLimit(sortAndSlice);
        }

        return sortAndSlice == null
               ? SortAndSlice.noSort
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

            // optional comma
            readIfThis(Tokens.COMMA);

            e2 = XreadSimpleValueSpecificationOrNull();

            if (e2 == null) {
                throw Error.error(ErrorCode.X_42563, ErrorCode.M_INVALID_LIMIT);
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
        } else {
            throw unexpectedToken();
        }

        boolean valid = true;

        if (e1.isUnresolvedParam()) {
            e1.setDataType(session, Type.SQL_INTEGER);
        } else if (e1.opType == OpTypes.VALUE) {
            valid = (e1.getDataType().typeCode == Types.SQL_INTEGER
                     && ((Integer) e1.getValue(null)).intValue() >= 0);
        } else {
            throw Error.error(ErrorCode.X_42563, ErrorCode.M_INVALID_LIMIT);
        }

        if (e2.isUnresolvedParam()) {
            e2.setDataType(session, Type.SQL_INTEGER);
        } else if (e2.opType == OpTypes.VALUE) {
            valid &= (e2.getDataType().typeCode == Types.SQL_INTEGER
                      && ((Integer) e2.getValue(null)).intValue() >= 0);
        } else {
            throw Error.error(ErrorCode.X_42563, ErrorCode.M_INVALID_LIMIT);
        }

        if (valid) {
            SortAndSlice sortAndSlice = new SortAndSlice();

            sortAndSlice.addLimitCondition(
                new ExpressionOp(OpTypes.LIMIT, e1, e2));

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
                throw Error.error(ErrorCode.X_42563, ErrorCode.M_INVALID_LIMIT);
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
                throw Error.error(ErrorCode.X_42563, ErrorCode.M_INVALID_LIMIT);
            }

            if (e1 == null) {
                if (token.tokenType == Tokens.COMMA) {
                    read();

                    e1 = e2;
                    e2 = XreadSimpleValueSpecificationOrNull();
                } else if (token.tokenType == Tokens.OFFSET) {
                    read();

                    e1 = XreadSimpleValueSpecificationOrNull();
                }
            }

            if (database.sqlSyntaxPgs || database.sqlSyntaxMys) {
                sortAndSlice.setZeroLimitIsZero();
            }
        } else if (token.tokenType == Tokens.FETCH) {
            read();

            boolean first = false;

            if (token.tokenType == Tokens.FIRST
                    || token.tokenType == Tokens.NEXT) {
                first = true;

                read();
            }

            e2 = XreadSimpleValueSpecificationOrNull();

            if (e2 == null) {
                if (!first) {
                    throw super.unexpectedTokenRequire(Tokens.T_FIRST);
                }

                e2 = new ExpressionValue(ValuePool.INTEGER_1, Type.SQL_INTEGER);
            }

            if (token.tokenType == Tokens.ROW
                    || token.tokenType == Tokens.ROWS) {
                read();
            }

            readThis(Tokens.ONLY);
            sortAndSlice.setStrictLimit();
        }

        if (sortAndSlice.hasOrder() && token.tokenType == Tokens.USING) {
            read();
            readThis(Tokens.INDEX);
            sortAndSlice.setUsingIndex();
        }

        if (e1 == null) {
            e1 = new ExpressionValue(ValuePool.INTEGER_0, Type.SQL_INTEGER);
        }

        boolean valid = true;

        if (e1.isUnresolvedParam()) {
            e1.setDataType(session, Type.SQL_INTEGER);
        }

        if (e2 != null) {
            if (e2.isUnresolvedParam()) {
                e2.setDataType(session, Type.SQL_INTEGER);
            }
        }

        if (valid) {
            sortAndSlice.addLimitCondition(
                new ExpressionOp(OpTypes.LIMIT, e1, e2));

            return;
        }

        throw Error.error(ErrorCode.X_42563, ErrorCode.M_INVALID_LIMIT);
    }

    private SortAndSlice XreadOrderBy() {

        SortAndSlice sortAndSlice = new SortAndSlice();

        while (true) {
            Expression        e;
            ExpressionOrderBy o;
            boolean           isDesc    = false;
            boolean           nullsLast = false;

            e = XreadValueExpression();
            o = new ExpressionOrderBy(e);

            if (token.tokenType == Tokens.DESC) {
                o.setDescending();

                isDesc = true;

                read();
            } else if (token.tokenType == Tokens.ASC) {
                read();
            }

            if (database.sqlNullsOrder) {
                nullsLast = !database.sqlNullsFirst;
            } else {
                nullsLast = database.sqlNullsFirst == isDesc;
            }

            o.setNullsLast(nullsLast);

            if (token.tokenType == Tokens.NULLS) {
                read();

                if (token.tokenType == Tokens.FIRST) {
                    read();
                    o.setNullsLast(false);
                } else if (token.tokenType == Tokens.LAST) {
                    read();
                    o.setNullsLast(true);
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

    protected RangeVariable readRangeVariableForDataChange(int operation) {

        Table      table = readTableName(true);
        ExpressionPeriodOp appPeriodSpec =
            XreadQueryApplicationPeriodSpecOrNull(
                table);
        SimpleName alias = null;

        if (appPeriodSpec != null) {
            throw Error.error(ErrorCode.X_0A501);
        }

        if (operation != StatementTypes.TRUNCATE) {
            switch (token.tokenType) {

                case Tokens.OVERRIDING :
                    break;

                case Tokens.AS :
                    read();
                    checkIsNonCoreReservedIdentifier();

                // fall through
                default :
                    if (isNonCoreReservedIdentifier()) {
                        if (!database.sqlSyntaxMys
                                || operation != StatementTypes.INSERT) {
                            alias = HsqlNameManager.getSimpleName(
                                token.tokenString,
                                isDelimitedIdentifier());

                            read();
                        }
                    }
            }

            if (alias == null && lastSynonym != null) {
                alias = HsqlNameManager.getSimpleName(
                    lastSynonym.name,
                    lastSynonym.isNameQuoted);
            }
        }

        if (table.isView) {
            switch (operation) {

                case StatementTypes.MERGE :
                    if (table.isTriggerUpdatable()
                            && table.isTriggerInsertable()) {
                        break;
                    }

                    if (table.isTriggerUpdatable()
                            || table.isTriggerInsertable()) {

                        // all or nothing
                    } else if (table.isUpdatable() && table.isInsertable()) {
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

                case StatementTypes.INSERT :
                    if (table.isTriggerInsertable()) {
                        break;
                    }

                    if (table.isInsertable()) {
                        break;
                    }

                    if (session.isProcessingScript()) {
                        break;
                    }

                    throw Error.error(ErrorCode.X_42545);

                case StatementTypes.TRUNCATE :
                    throw Error.error(ErrorCode.X_42545);
            }

            table = ((View) table).newDerivedTable(session, compileContext);
        }

        RangeVariable range = new RangeVariable(
            table,
            alias,
            null,
            null,
            compileContext);

        if (table.isSystemVersioned()) {
            ExpressionPeriodOp sysPeriodSpec = new ExpressionPeriodOp();

            sysPeriodSpec.setSystemRangeVariable(
                session,
                RangeGroup.emptyArray,
                range);
            range.setSystemPeriodCondition(sysPeriodSpec);
        }

        return range;
    }

    protected Table readNamedSubqueryOrNull() {

        if (!isSimpleName()) {
            return null;
        }

        TableDerived td = compileContext.getNamedSubQuery(token.tokenString);

        if (td == null) {
            return null;
        }

        read();

        if (td.isRecompiled()) {
            td = td.newDerivedTable(session, compileContext);
        }

        return td;
    }

    /**
     * Creates a RangeVariable from the parse context.
     */
    protected RangeVariable readTableOrSubquery() {

        Table                  table          = null;
        SimpleName             alias          = null;
        SimpleName[]           columnNameList = null;
        OrderedHashSet<String> columnList     = null;
        ExpressionPeriodOp     sysPeriodSpec  = null;
        boolean                joinedTable    = false;
        boolean                isLateral      = false;
        boolean                isTableName    = false;

        switch (token.tokenType) {

            case Tokens.OPENBRACKET : {
                table = XreadTableSubqueryOrNull(false);

                if (table == null) {
                    table = XreadJoinedTableAsSubqueryOrNull();

                    if (table == null) {
                        table = XreadTableSubqueryOrNull(true);

                        if (table == null) {
                            throw unexpectedToken();
                        }

                        break;
                    }

                    joinedTable = true;
                }

                break;
            }

            case Tokens.UNNEST : {
                Expression e = XreadCollectionDerivedTable(
                    OpTypes.TABLE_SUBQUERY);

                table     = e.getTable();
                isLateral = true;
                break;
            }

            case Tokens.LATERAL : {
                Expression e = XreadLateralDerivedTable();

                table     = e.getTable();
                isLateral = true;
                break;
            }

            case Tokens.TABLE : {
                Expression e = XreadTableFunctionDerivedTable();

                table     = e.getTable();
                isLateral = true;
                break;
            }

            default : {
                table = readNamedSubqueryOrNull();

                if (table == null) {
                    table       = readTableName(true);
                    isTableName = true;

                    // attach to range variable below
                    sysPeriodSpec = XreadQuerySystemPeriodSpecOrNull(table);
                }

                if (table.isView()) {
                    table = ((View) table).newDerivedTable(
                        session,
                        compileContext);
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

            alias = HsqlNameManager.getSimpleName(
                token.tokenString,
                isDelimitedIdentifier());

            read();

            if (token.tokenType == Tokens.OPENBRACKET) {
                columnList     = new OrderedHashSet<>();
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

        if (isTableName && alias == null && lastSynonym != null) {
            alias = HsqlNameManager.getSimpleName(
                lastSynonym.name,
                lastSynonym.isNameQuoted);
        }

        if (database.sqlSyntaxMss) {
            if (readIfThis(Tokens.WITH)) {
                readNestedParenthesisedTokens();
            }
        }

        RangeVariable range;

        if (joinedTable) {
            range = new RangeVariableJoined(
                table,
                alias,
                columnList,
                columnNameList,
                compileContext);
        } else {
            range = new RangeVariable(
                table,
                alias,
                columnList,
                columnNameList,
                compileContext);

            if (sysPeriodSpec != null) {
                RangeGroup[] ranges = compileContext.getOuterRanges();

                sysPeriodSpec.setSystemRangeVariable(session, ranges, range);
                range.setSystemPeriodCondition(sysPeriodSpec);
            }
        }

        if (isLateral) {
            range.isLateral = true;
        }

        return range;
    }

    private Expression readAggregateFunctionOrNull() {

        int        position = getPosition();
        int        tokenT   = token.tokenType;
        Expression expr;

        read();

        if (token.tokenType != Tokens.OPENBRACKET) {
            rewind(position);

            return null;
        }

        readThis(Tokens.OPENBRACKET);

        expr = readAggregateExpression(tokenT);

        readThis(Tokens.CLOSEBRACKET);
        readFilterClause(expr);

        return expr;
    }

    private void readFilterClause(Expression e) {

        Expression condition = XreadFilterExpressionOrNull();

        if (condition != null) {
            e.setCondition(condition);
        }
    }

    Expression XreadFilterExpressionOrNull() {

        int        position  = getPosition();
        Expression condition = null;

        if (token.tokenType == Tokens.FILTER) {
            read();

            if (token.tokenType != Tokens.OPENBRACKET) {
                rewind(position);

                return null;
            }

            readThis(Tokens.OPENBRACKET);
            readThis(Tokens.WHERE);

            condition = XreadBooleanValueExpression();

            readThis(Tokens.CLOSEBRACKET);
        }

        return condition;
    }

    private Expression readAggregateExpression(int tokenType) {

        final int    opType    = ParserDQL.getExpressionType(tokenType);
        boolean      distinct  = false;
        boolean      all       = false;
        SortAndSlice sort      = null;
        String       separator = null;

        if (token.tokenType == Tokens.DISTINCT) {
            distinct = true;

            read();
        } else if (token.tokenType == Tokens.ALL) {
            all = true;

            read();
        }

        int        position = getPosition();
        Expression e        = XreadValueExpression();

        switch (tokenType) {

            case Tokens.COUNT :
                if (e.getType() == OpTypes.MULTICOLUMN) {
                    if (((ExpressionColumn) e).tableName != null) {
                        throw unexpectedToken();
                    }

                    if (all || distinct) {
                        throw unexpectedToken();
                    }

                    e.opType = OpTypes.ASTERISK;
                } else {
                    if (token.tokenType == Tokens.COMMA) {
                        rewind(position);

                        e = XreadRowElementList(false);
                    }
                }
            case Tokens.AVG :
            case Tokens.MAX :
            case Tokens.MIN :
            case Tokens.SUM :
            case Tokens.EVERY :
            case Tokens.ANY :
            case Tokens.ANY_VALUE :
            case Tokens.SOME :
                break;

            case Tokens.STDDEV :
            case Tokens.STDDEV_POP :
            case Tokens.STDDEV_SAMP :
            case Tokens.VARIANCE :
            case Tokens.VAR_SAMP :
            case Tokens.VAR_POP :
                if (all || distinct) {
                    throw unexpectedToken(all
                                          ? Tokens.T_ALL
                                          : Tokens.T_DISTINCT);
                }

                break;

            case Tokens.ARRAY_AGG : {
                if (token.tokenType == Tokens.ORDER) {
                    read();
                    readThis(Tokens.BY);

                    sort = XreadOrderBy();
                }

                return new ExpressionArrayAggregate(
                    OpTypes.ARRAY_AGG,
                    distinct,
                    e,
                    sort,
                    separator,
                    null,
                    false,
                    false,
                    false,
                    0);
            }

            case Tokens.GROUP_CONCAT : {
                if (token.tokenType == Tokens.ORDER) {
                    read();
                    readThis(Tokens.BY);

                    sort = XreadOrderBy();
                }

                if (token.tokenType == Tokens.SEPARATOR) {
                    read();
                    checkIsQuotedString();

                    separator = (String) token.tokenValue;

                    read();
                }

                return new ExpressionArrayAggregate(
                    OpTypes.LISTAGG,
                    distinct,
                    e,
                    sort,
                    separator,
                    null,
                    false,
                    false,
                    false,
                    0);
            }

            case Tokens.MEDIAN : {
                return new ExpressionArrayAggregate(
                    OpTypes.MEDIAN,
                    distinct,
                    e,
                    sort,
                    separator,
                    null,
                    false,
                    false,
                    false,
                    0);
            }

            default :
                if (e.getType() == OpTypes.MULTICOLUMN
                        || e.getType() == OpTypes.ASTERISK) {
                    throw unexpectedToken(Tokens.T_ASTERISK);
                }
        }

        Expression aggregateExp = new ExpressionAggregate(opType, distinct, e);

        return aggregateExp;
    }

//--------------------------------------
    // returns null
    // := <unsigned literal> | <general value specification>
    Expression XreadValueSpecificationOrNull() {

        Expression e     = null;
        boolean    minus = false;

        switch (token.tokenType) {

            case Tokens.PLUS_OP :
                read();
                break;

            case Tokens.MINUS_OP :
                read();

                minus = true;
                break;

            default :
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
    // <unsigned literal> | <general value specification>
    Expression XreadUnsignedValueSpecificationOrNull() {

        Expression e;

        switch (token.tokenType) {

            case Tokens.TRUE :
                read();

                return new ExpressionBoolean(true);

            case Tokens.FALSE :
                read();

                return new ExpressionBoolean(false);

            case Tokens.DEFAULT :
                if (compileContext.contextuallyTypedExpression) {
                    read();

                    e = new ExpressionColumn(OpTypes.DEFAULT);

                    return e;
                }

                break;

            case Tokens.VALUES :
                if (compileContext.onDuplicateTypedExpression) {
                    read();
                    readThis(Tokens.OPENBRACKET);
                    checkIsSimpleName();

                    e = new ExpressionColumn(token.tokenString);

                    read();
                    readThis(Tokens.CLOSEBRACKET);

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
            case Tokens.QUESTION :
                return XreadDynamicParameterOrNull();

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
                FunctionSQL function = FunctionSQL.newSQLFunction(
                    token.tokenString,
                    compileContext);

                if (function == null) {
                    return null;
                }

                return readSQLFunction(function);

            // read SQL parameter reference
        }

        return null;
    }

    Expression XreadDynamicParameterOrNull() {

        switch (token.tokenType) {

            case Tokens.COLON :
                read();

                if (isIntegral()) {
                    int pos = readInteger();
                    ExpressionColumn p = new ExpressionColumn(
                        OpTypes.DYNAMIC_PARAM,
                        pos);

                    compileContext.addParameter(p, getPosition());

                    return p;
                } else if (isUndelimitedSimpleName()) {
                    int pos = compileContext.parameters.size();
                    ExpressionColumn p = new ExpressionColumn(
                        OpTypes.DYNAMIC_PARAM,
                        pos);

                    compileContext.addParameter(p, getPosition());

                    Token newToken = new Token();

                    newToken.tokenType   = Tokens.X_VALUE;
                    newToken.dataType    = Type.SQL_INTEGER;
                    newToken.tokenString = String.valueOf(p.parameterIndex);

                    replaceToken(newToken, null);
                    read();

                    return p;
                } else {
                    throw unexpectedToken(Tokens.T_COLON);
                }
            case Tokens.QUESTION :
                int pos = compileContext.parameters.size();
                ExpressionColumn p = new ExpressionColumn(
                    OpTypes.DYNAMIC_PARAM,
                    pos);

                compileContext.addParameter(p, getPosition());

                Token changedToken = new Token();

                changedToken.tokenType   = Tokens.COLON;
                changedToken.tokenString = Tokens.T_COLON;

                Token newToken = new Token();

                newToken.tokenType   = Tokens.X_VALUE;
                newToken.dataType    = Type.SQL_INTEGER;
                newToken.tokenValue  = Integer.valueOf(p.parameterIndex);
                newToken.tokenString = String.valueOf(p.parameterIndex);

                replaceToken(changedToken, newToken);
                read();

                return p;

            default :
                return null;
        }
    }

    // <unsigned literal> | <dynamic parameter> | <variable>
    Expression XreadSimpleValueSpecificationOrNull() {

        Expression e;

        switch (token.tokenType) {

            case Tokens.X_VALUE :
                e = new ExpressionValue(token.tokenValue, token.dataType);

                read();

                return e;

            case Tokens.COLON :
            case Tokens.QUESTION :
                return XreadDynamicParameterOrNull();

            case Tokens.X_IDENTIFIER :
            case Tokens.X_DELIMITED_IDENTIFIER :
                checkValidCatalogName(token.namePrePrePrefix);

                e = new ExpressionColumn(
                    token.namePrePrefix,
                    token.namePrefix,
                    token.tokenString);

                read();

                return e;

            default :
                return null;
        }
    }

    // combined <value expression primary> and <predicate>
    // exclusively called
    // <explicit row value constructor> needed for predicate
    Expression XreadAllTypesValueExpressionPrimary(boolean isBoolean) {

        Expression e        = null;
        int        position = getPosition();

        switch (token.tokenType) {

            case Tokens.EXISTS :
            case Tokens.UNIQUE :
                if (isBoolean) {
                    return XreadPredicate();
                }

                break;

            case Tokens.PERIOD :
                if (isBoolean) {
                    break;
                }

                read();

                if (readIfThis(Tokens.OPENBRACKET)) {
                    e = XreadRowElementList(true);

                    if (e.nodes.length != 2) {
                        throw Error.error(ErrorCode.X_42564);
                    }

                    e = new ExpressionPeriod(e);

                    readThis(Tokens.CLOSEBRACKET);
                } else {
                    rewind(position);

                    e = XreadSimpleValueExpressionPrimary();

                    if (e != null) {
                        e = XreadArrayElementReference(e);
                    }
                }

                break;

            case Tokens.ROW :
                if (isBoolean) {
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

        if (e == null) {
            if (token.tokenType == Tokens.ROW) {
                read();
                checkIsThis(Tokens.OPENBRACKET);
            } else if (token.tokenType == Tokens.PERIOD) {
                read();

                if (readIfThis(Tokens.OPENBRACKET)) {
                    e = XreadRowElementList(true);

                    if (e.nodes.length != 2) {
                        throw Error.error(ErrorCode.X_42564);
                    }

                    e = new ExpressionPeriod(e);

                    readThis(Tokens.CLOSEBRACKET);
                } else {
                    rewind(position);

                    e = XreadSimpleValueExpressionPrimary();

                    if (e != null) {
                        e = XreadArrayElementReference(e);
                    }
                }
            }

            if (token.tokenType == Tokens.OPENBRACKET) {
                read();

                e = XreadRowElementList(true);

                readThis(Tokens.CLOSEBRACKET);
            }
        }

        if (isBoolean && e != null) {
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

        int position = getPosition();

        switch (token.tokenType) {

            case Tokens.OPENBRACKET :
                read();

                int subqueryPosition = getPosition();

                readOpenBrackets();

                switch (token.tokenType) {

                    case Tokens.WITH :
                    case Tokens.TABLE :
                    case Tokens.VALUES :
                    case Tokens.SELECT :
                        TableDerived td = null;

                        rewind(subqueryPosition);

                        try {
                            td = XreadSubqueryTableBody(
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

                        if (td.queryExpression != null) {
                            if (td.queryExpression.isSingleColumn()) {
                                e = new Expression(OpTypes.SCALAR_SUBQUERY, td);
                            } else {
                                e = new Expression(OpTypes.ROW_SUBQUERY, td);
                            }
                        }

                        if (e != null) {
                            return e;
                        }

                        rewind(position);

                        return null;

                    default :
                        rewind(position);

                        return null;
                }
            case Tokens.ASTERISK :
                e = new ExpressionColumn(token.namePrePrefix, token.namePrefix);

                getRecordedToken().setExpression(e);
                read();

                return e;

            case Tokens.LEAST : {
                e = readLeastExpressionOrNull();

                if (e != null) {
                    return e;
                }

                break;
            }

            case Tokens.GREATEST : {
                e = readGreatestExpressionOrNull();

                if (e != null) {
                    return e;
                }

                break;
            }

            case Tokens.DECODE : {
                e = readDecodeExpressionOrNull();

                if (e != null) {
                    return e;
                }

                break;
            }

            case Tokens.CONCAT_WORD : {
                e = readConcatExpressionOrNull();

                if (e != null) {
                    return e;
                }

                break;
            }

            case Tokens.CONCAT_WS : {
                e = readConcatSeparatorExpressionOrNull();

                if (e != null) {
                    return e;
                }

                break;
            }

            case Tokens.IF :
            case Tokens.CASEWHEN : {
                e = readCaseWhenExpressionOrNull();

                if (e != null) {
                    return e;
                }

                break;
            }

            case Tokens.CASE :
                return readCaseExpression();

            case Tokens.NULLIF :
                return readNullIfExpression();

            case Tokens.COALESCE :
                return readCoalesceExpression();

            case Tokens.IFNULL :
            case Tokens.ISNULL : {
                e = readIfNullExpressionOrNull();

                if (e != null) {
                    return e;
                }

                break;
            }

            case Tokens.NVL2 : {
                e = readIfNull2ExpressionOrNull();

                if (e != null) {
                    return e;
                }

                break;
            }

            case Tokens.CAST :
                e = readCastExpression();

                if (e != null) {
                    return e;
                }

                break;

            case Tokens.CONVERT : {
                e = readConvertExpressionOrNull();

                if (e != null) {
                    return e;
                }

                break;
            }

            case Tokens.DATE :
            case Tokens.TIME :
            case Tokens.TIMESTAMP :
            case Tokens.INTERVAL :
                e = readDateTimeIntervalLiteral(session);

                if (e != null) {
                    return e;
                }

                break;

            case Tokens.ARRAY :
                return readCollection(OpTypes.ARRAY);

            case Tokens.ANY_VALUE :
            case Tokens.COUNT :
            case Tokens.AVG :
            case Tokens.MAX :
            case Tokens.MIN :
            case Tokens.SUM :
            case Tokens.ANY :
            case Tokens.SOME :
            case Tokens.EVERY :
            case Tokens.STDDEV_POP :
            case Tokens.STDDEV_SAMP :
            case Tokens.VAR_POP :
            case Tokens.VAR_SAMP :
            case Tokens.ARRAY_AGG :

            // non-standard
            case Tokens.GROUP_CONCAT :
            case Tokens.MEDIAN :
            case Tokens.STDDEV :
            case Tokens.VARIANCE :
                e = readAggregateFunctionOrNull();    // general set function

                if (e != null) {
                    return e;
                }

                break;

            case Tokens.STRING_AGG :
                e = readStringAggFunctionOrNull();

                if (e != null) {
                    return e;
                }

                break;

            case Tokens.LISTAGG :
                e = readListAggFunctionOrNull();

                if (e != null) {
                    return e;
                }

                break;

            case Tokens.NEXT : {
                e = readSequenceExpressionOrNull(OpTypes.SEQUENCE);

                if (e != null) {
                    return e;
                }

                break;
            }

            case Tokens.PREVVAL :
            case Tokens.CURRENT : {
                e = readSequenceExpressionOrNull(OpTypes.SEQUENCE_CURRENT);

                if (e != null) {
                    return e;
                }

                break;
            }

            case Tokens.CURRVAL : {
                if (database.sqlSyntaxPgs) {
                    read();
                    readThis(Tokens.OPENBRACKET);

                    String  spec    = readQuotedString();
                    Scanner scanner = session.getScanner();

                    scanner.reset(session, spec);
                    scanner.scanNext();

                    String schemaName = session.getSchemaName(
                        scanner.token.namePrefix);
                    NumberSequence sequence =
                        database.schemaManager.getSequence(
                            scanner.token.tokenString,
                            schemaName,
                            true);

                    e = new ExpressionColumn(
                        sequence,
                        OpTypes.SEQUENCE_CURRENT);

                    readThis(Tokens.CLOSEBRACKET);

                    return e;
                }

                break;
            }

            case Tokens.LASTVAL : {
                if (database.sqlSyntaxPgs) {
                    read();
                    readThis(Tokens.OPENBRACKET);
                    readThis(Tokens.CLOSEBRACKET);

                    return FunctionCustom.newCustomFunction(
                        session,
                        Tokens.T_IDENTITY,
                        Tokens.IDENTITY);
                }

                break;
            }

            case Tokens.NEXTVAL : {
                if (database.sqlSyntaxPgs) {
                    return readNextvalFunction();
                } else if (database.sqlSyntaxDb2) {
                    e = readSequenceExpressionOrNull(OpTypes.SEQUENCE);

                    if (e != null) {
                        return e;
                    }
                }

                break;
            }

            case Tokens.ROW_NUMBER : {
                read();

                if (token.tokenType == Tokens.OPENBRACKET) {
                    read();
                    readThis(Tokens.CLOSEBRACKET);
                    readThis(Tokens.OVER);
                    readThis(Tokens.OPENBRACKET);
                    readThis(Tokens.CLOSEBRACKET);
                } else {
                    rewind(position);
                    break;
                }

                return new ExpressionColumn(OpTypes.ROWNUM);
            }

            case Tokens.ROWNUM : {
                read();

                if (token.tokenType == Tokens.OPENBRACKET) {
                    read();

                    if (token.tokenType == Tokens.CLOSEBRACKET) {
                        read();
                    } else {
                        rewind(position);
                        break;
                    }
                } else if (!database.sqlSyntaxOra && !database.sqlSyntaxDb2) {
                    rewind(position);
                    break;
                }

                return new ExpressionColumn(OpTypes.ROWNUM);
            }

            case Tokens.LEFT :
            case Tokens.RIGHT :

                // CLI function names
                break;

            case Tokens.TABLE : {
                read();
                readThis(Tokens.OPENBRACKET);

                TableDerived td = XreadSubqueryTableBody(
                    OpTypes.TABLE_SUBQUERY);

                readThis(Tokens.CLOSEBRACKET);

                return new Expression(OpTypes.TABLE_SUBQUERY, td);
            }

            case Tokens.VALUES : {
                throw unexpectedToken();
            }

            case Tokens.GROUPING : {
                read();
                readThis(Tokens.OPENBRACKET);

                Expression groupingElements = XreadRowElementList(true);

                readThis(Tokens.CLOSEBRACKET);

                return new ExpressionColumn(groupingElements);
            }

            case Tokens.JSON_ARRAY : {
                read();

                return readJSONArray();
            }

            case Tokens.JSON_ARRAYAGG : {
                read();

                return readJSONArrayAgg();
            }

            case Tokens.JSON_OBJECT : {
                read();

                return readJSONObject();
            }

            case Tokens.JSON_OBJECTAGG : {
                read();

                return readJSONObjectAgg();
            }

            default :
                if (isCoreReservedKey()) {
                    throw unexpectedToken();
                }
        }

        e = readColumnOrFunctionExpression();

        if (e.isSelfAggregate()) {
            readFilterClause(e);
        }

        return e;
    }

    Expression readJSONArray() {

        HsqlArrayList<Expression> list       = new HsqlArrayList<>();
        Expression                qe         = null;
        boolean                   nullOnNull = true;
        Type                      dataType   = Type.SQL_VARCHAR_LONG;

        readThis(Tokens.OPENBRACKET);

        int position = getPosition();

        loop:
        while (true) {
            Expression value;

            switch (token.tokenType) {

                case Tokens.ABSENT :
                case Tokens.NULL :
                case Tokens.CLOSEBRACKET :
                    break loop;

                case Tokens.COMMA :
                    if (list.isEmpty()) {
                        throw unexpectedToken();
                    }

                    read();
            }

            try {
                value = XreadValueExpression();

                list.add(value);
            } catch (HsqlException e) {
                if (list.size() > 0) {
                    throw e;
                }

                rewind(position);
                break loop;
            }
        }

        if (list.isEmpty()) {
            if (token.tokenType != Tokens.CLOSEBRACKET) {
                TableDerived td = XreadSubqueryTableBody(
                    OpTypes.TABLE_SUBQUERY);

                qe = new Expression(OpTypes.ARRAY_SUBQUERY, td);
            }
        }

        nullOnNull = readJSONNullClause(nullOnNull);

        if (readIfThis(Tokens.RETURNING)) {
            dataType = readJSONReturningClause();
        }

        readThis(Tokens.CLOSEBRACKET);

        if (qe == null) {
            return new ExpressionJSON.ExpressionJSONArrayFromValues(
                list,
                nullOnNull,
                dataType);
        } else {
            return new ExpressionJSON.ExpressionJSONArrayFromQuery(
                qe,
                nullOnNull,
                dataType);
        }
    }

    Expression readJSONArrayAgg() {

        boolean                  nullOnNull = false;
        Type                     dataType   = Type.SQL_VARCHAR_LONG;
        Expression               valueExpr;
        ExpressionArrayAggregate arrayAgg;
        SortAndSlice             sort = null;

        readThis(Tokens.OPENBRACKET);

        valueExpr = XreadValueExpression();

        if (token.tokenType == Tokens.ORDER) {
            read();
            readThis(Tokens.BY);

            sort = XreadOrderBy();
        }

        arrayAgg = new ExpressionArrayAggregate(
            OpTypes.ARRAY_AGG,
            false,
            valueExpr,
            sort,
            null,
            null,
            false,
            false,
            false,
            0);
        nullOnNull = readJSONNullClause(nullOnNull);

        if (readIfThis(Tokens.RETURNING)) {
            dataType = readJSONReturningClause();
        }

        readThis(Tokens.CLOSEBRACKET);

        return new ExpressionJSON.ExpressionJSONArrayAgg(
            arrayAgg,
            nullOnNull,
            dataType);
    }

    Expression readJSONObject() {

        OrderedHashMap<Expression, Expression> map = new OrderedHashMap<>();
        boolean                                nullOnNull   = true;
        boolean                                uniqueKeys   = false;
        Type                                   dataType = Type.SQL_VARCHAR_LONG;
        boolean                                hasDuplicate = false;

        readThis(Tokens.OPENBRACKET);
        loop:
        while (true) {
            Expression nameExpr;
            Expression valueExpr;
            boolean    isKey = false;

            switch (token.tokenType) {

                case Tokens.ABSENT :
                case Tokens.NULL :
                case Tokens.WITH :
                case Tokens.WITHOUT :
                case Tokens.CLOSEBRACKET :
                    break loop;

                case Tokens.COMMA :
                    if (map.isEmpty()) {
                        throw unexpectedToken();
                    }

                    read();
            }

            if (readIfThis(Tokens.KEY)) {
                isKey = true;
            }

            int position = getPosition();

            nameExpr = XreadCharacterValueExpression();

            if (nameExpr.opType == OpTypes.JSON_FUNCTION) {
                rewind(position);

                throw unexpectedToken();
            }

            if (!readIfThis(Tokens.VALUE)) {
                if (isKey) {
                    throw unexpectedToken();
                }

                readThis(Tokens.COLON);
            }

            valueExpr = XreadValueExpression();

            if (readIfThis(Tokens.FORMAT)) {
                readThis(Tokens.JSON);

                valueExpr = new ExpressionJSON.ExpressionJSONWrapper(valueExpr);
            }

            Object e = map.put(nameExpr, valueExpr);

            if (e != null) {
                hasDuplicate = true;
            }
        }

        nullOnNull = readJSONNullClause(nullOnNull);
        uniqueKeys = readJSONUniqueClause(uniqueKeys);

        if (uniqueKeys && hasDuplicate) {
            throw Error.error(ErrorCode.X_23505);
        }

        if (readIfThis(Tokens.RETURNING)) {
            dataType = readJSONReturningClause();
        }

        readThis(Tokens.CLOSEBRACKET);

        return new ExpressionJSON.ExpressionJSONObject(
            map,
            nullOnNull,
            uniqueKeys,
            dataType);
    }

    Expression readJSONObjectAgg() {

        boolean                  nullOnNull = true;
        boolean                  uniqueKeys = false;
        boolean                  isKey      = false;
        Type                     dataType   = Type.SQL_VARCHAR_LONG;
        Expression               nameExpr;
        Expression               valueExpr;
        ExpressionArrayAggregate arrayAggName;
        ExpressionArrayAggregate arrayAggValue;

        readThis(Tokens.OPENBRACKET);

        int position = getPosition();

        if (readIfThis(Tokens.KEY)) {
            isKey = true;
        }

        nameExpr = XreadCharacterValueExpression();

        if (nameExpr.opType == OpTypes.JSON_FUNCTION) {
            rewind(position);

            throw unexpectedToken();
        }

        if (!readIfThis(Tokens.VALUE)) {
            if (isKey) {
                throw unexpectedToken();
            }

            readThis(Tokens.COLON);
        }

        valueExpr  = XreadValueExpression();
        arrayAggName = new ExpressionArrayAggregate(
            OpTypes.ARRAY_AGG,
            false,
            nameExpr,
            null,
            null,
            null,
            false,
            false,
            false,
            0);
        arrayAggValue = new ExpressionArrayAggregate(
            OpTypes.ARRAY_AGG,
            false,
            valueExpr,
            null,
            null,
            null,
            false,
            false,
            false,
            0);
        nullOnNull = readJSONNullClause(nullOnNull);
        uniqueKeys = readJSONUniqueClause(uniqueKeys);

        if (readIfThis(Tokens.RETURNING)) {
            dataType = readJSONReturningClause();
        }

        readThis(Tokens.CLOSEBRACKET);

        return new ExpressionJSON.ExpressionJSONObjectAgg(
            arrayAggName,
            arrayAggValue,
            nullOnNull,
            uniqueKeys,
            dataType);
    }

    boolean readJSONNullClause(boolean defaultValue) {

        boolean nullOnNull = defaultValue;

        switch (token.tokenType) {

            case Tokens.ABSENT : {
                read();
                readThis(Tokens.ON);
                readThis(Tokens.NULL);

                nullOnNull = false;
                break;
            }

            case Tokens.NULL : {
                read();
                readThis(Tokens.ON);
                readThis(Tokens.NULL);

                nullOnNull = true;
                break;
            }
        }

        return nullOnNull;
    }

    boolean readJSONUniqueClause(boolean defaultValue) {

        boolean uniqueKeys = defaultValue;

        switch (token.tokenType) {

            case Tokens.WITH : {
                read();
                readThis(Tokens.UNIQUE);
                readIfThis(Tokens.KEYS);

                uniqueKeys = true;
                break;
            }

            case Tokens.WITHOUT : {
                read();
                readThis(Tokens.UNIQUE);
                readIfThis(Tokens.KEYS);

                uniqueKeys = false;
                break;
            }
        }

        return uniqueKeys;
    }

    Type readJSONReturningClause() {

        int  position = getPosition();
        Type dataType = readTypeDefinition(false, true);

        if (dataType.typeCode == Types.SQL_VARCHAR
                || dataType.typeCode == Types.SQL_CLOB) {
            return dataType;
        }

        rewind(position);

        throw super.unexpectedTokenRequire("VARCHAR or CLOB");
    }

    Expression readNextvalFunction() {

        read();
        readThis(Tokens.OPENBRACKET);

        String  spec    = readQuotedString();
        Scanner scanner = session.getScanner();

        scanner.reset(session, spec);
        scanner.scanNext();

        String schemaName = session.getSchemaName(scanner.token.namePrefix);
        NumberSequence sequence = database.schemaManager.getSequence(
            scanner.token.tokenString,
            schemaName,
            true);
        Expression e = new ExpressionColumn(sequence, OpTypes.SEQUENCE);

        readThis(Tokens.CLOSEBRACKET);

        return e;
    }

    // OK - composite production -
    // <numeric primary> <character primary> <binary primary> <datetime primary> <interval primary>
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
            case Tokens.ARRAY_MAX_CARDINALITY :
            case Tokens.MAX_CARDINALITY :
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
                FunctionSQL function = FunctionSQL.newSQLFunction(
                    token.tokenString,
                    compileContext);

                if (function == null) {
                    throw unsupportedFeature();
                }

                e = readSQLFunction(function);

                if (e != null) {
                    break;
                }

            // fall through
            default :
                e = XreadAllTypesValueExpressionPrimary(boole);
        }

        e = XreadModifier(e);

        return e;
    }

    Expression XreadModifier(Expression e) {

        int position = getPosition();

        switch (token.tokenType) {

            case Tokens.AT : {
                read();

                Expression e1 = null;

                if (token.tokenType == Tokens.LOCAL) {
                    read();
                } else if (token.tokenType == Tokens.TIME) {
                    read();
                    readThis(Tokens.ZONE);

                    e1 = XreadValueExpressionPrimary();

                    switch (token.tokenType) {

                        case Tokens.YEAR :
                        case Tokens.MONTH :
                        case Tokens.DAY :
                        case Tokens.HOUR :
                        case Tokens.MINUTE :
                        case Tokens.SECOND : {
                            IntervalType type = readIntervalType(
                                session,
                                false);

                            if (e1.getType() == OpTypes.SUBTRACT) {
                                e1.dataType = type;
                            } else {
                                e1 = new ExpressionOp(e1, type);
                            }

                            break;
                        }

                        default :
                    }
                } else {
                    rewind(position);

                    return e;
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
                IntervalType type = readIntervalType(session, true);

                if (e.getType() == OpTypes.SUBTRACT) {
                    e.dataType = type;
                } else {
                    e = new ExpressionOp(e, type);
                }

                break;
            }

            case Tokens.COLLATE : {
                read();

                Collation collation = database.schemaManager.getCollation(
                    session,
                    token.tokenString,
                    token.namePrefix);

                e.setCollation(collation);
                read();
            }
        }

        return e;
    }

    Expression XreadValueExpressionWithContext() {

        Expression e;

        compileContext.contextuallyTypedExpression = true;

        try {
            e = XreadValueExpression();
        } finally {
            compileContext.contextuallyTypedExpression = false;
        }

        return e;
    }

    Expression XreadValueExpressionOnDuplicate() {

        Expression e;

        compileContext.onDuplicateTypedExpression = true;

        try {
            e = XreadValueExpression();
        } finally {
            compileContext.onDuplicateTypedExpression = false;
        }

        return e;
    }

    Expression XreadGroupByExpressionPrimary() {

        if (token.tokenType == Tokens.GROUPING) {
            read();

            // simple syntax error if SETS is missing
            readThis(Tokens.SETS);
            readThis(Tokens.OPENBRACKET);

            HsqlArrayList<Expression> list = new HsqlArrayList<>();

            while (true) {
                Expression e = XreadGroupByExpressionPrimary();

                list.add(e);

                if (token.tokenType == Tokens.COMMA) {
                    read();
                    continue;
                }

                break;
            }

            readThis(Tokens.CLOSEBRACKET);

            Expression[] array = new Expression[list.size()];

            list.toArray(array);

            Expression e = new Expression(OpTypes.VALUELIST, array);

            e.groupingType = Tokens.SETS;

            return e;
        }

        Expression e = XreadGroupByExpression();

        return e;
    }

    Expression XreadGroupByExpression() {

        int        groupingType = 0;
        Expression e;

        switch (token.tokenType) {

            case Tokens.CUBE :
            case Tokens.ROLLUP :
                groupingType = token.tokenType;

                read();
                readThis(Tokens.OPENBRACKET);

                e = XreadRowElementList(true);

                readThis(Tokens.CLOSEBRACKET);
                checkIfGroupingOrAggregate(e);

                e.groupingType = groupingType;

                return e;

            case Tokens.OPENBRACKET :
                int position = getPosition();

                read();

                // handle empty set here
                if (token.tokenType == Tokens.CLOSEBRACKET) {
                    read();

                    return new ExpressionColumn(OpTypes.NONE);
                }

                rewind(position);

                e = XreadValueExpression();

                checkIfGroupingOrAggregate(e);

                return e;

            default :
                e = XreadValueExpression();

                checkIfGroupingOrAggregate(e);

                return e;
        }
    }

    private void checkIfGroupingOrAggregate(Expression e) {

        if (e == null) {
            return;
        }

        if (OpTypes.GROUPING == e.opType
                || OpTypes.subqueryAggregateExpressionSet.contains(e.opType)) {
            throw Error.error(
                ErrorCode.X_42572,
                "aggregate functions / subqueries are not allowed in GROUP BY");
        }

        for (int i = 0; i < e.nodes.length; i++) {
            checkIfGroupingOrAggregate(e.nodes[i]);
        }
    }

    /**
     *   value expression ::=
     *     common value expression
     *   | boolean value expression
     *   | row value expression
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

                case Tokens.PLUS_OP :
                    type  = OpTypes.ADD;
                    boole = false;
                    break;

                case Tokens.MINUS_OP :
                    type  = OpTypes.SUBTRACT;
                    boole = false;
                    break;

                case Tokens.CONCAT_OP :
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
            e = boole
                ? new ExpressionLogical(type, a, e)
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

                case Tokens.DIVIDE_OP :
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

            e = boole
                ? new ExpressionLogical(type, a, e)
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

            case Tokens.PLUS_OP :
                read();

                boole = false;
                break;

            case Tokens.MINUS_OP :
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

            default :
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

        if (minus) {
            e = new ExpressionArithmetic(OpTypes.NEGATE, e);
        }

        if (unknown) {
            e = new ExpressionLogical(OpTypes.IS_NULL, e);
        }

        if (not) {
            e = new ExpressionLogical(OpTypes.NOT, e);
        }

        return e;
    }

    Expression XreadStringValueExpression() {
        return XreadCharacterValueExpression();

//        XreadBinaryValueExpression();
    }

    Expression XreadCharacterValueExpression() {

        Expression e         = XreadCharacterPrimary();
        Collation  collation = readCollateClauseOrNull();

        while (token.tokenType == Tokens.CONCAT_OP
                || token.tokenType == Tokens.PLUS_OP) {
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
                FunctionSQL function = FunctionSQL.newSQLFunction(
                    token.tokenString,
                    compileContext);
                Expression e = readSQLFunction(function);

                if (e != null) {
                    return e;
                }
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
            case Tokens.WIDTH_BUCKET :
                FunctionSQL function = FunctionSQL.newSQLFunction(
                    token.tokenString,
                    compileContext);

                if (function == null) {
                    throw unexpectedToken();
                }

                Expression e = readSQLFunction(function);

                if (e != null) {
                    return e;
                }
        }

        return XreadValueExpressionPrimary();
    }

    Expression XreadNumericValueExpression() {

        Expression e = XreadTerm();

        while (true) {
            int type;

            if (token.tokenType == Tokens.PLUS_OP) {
                type = OpTypes.ADD;
            } else if (token.tokenType == Tokens.MINUS_OP) {
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
            } else if (token.tokenType == Tokens.DIVIDE_OP) {
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

        if (token.tokenType == Tokens.PLUS_OP) {
            read();
        } else if (token.tokenType == Tokens.MINUS_OP) {
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

            if (token.tokenType == Tokens.PLUS_OP) {
                type = OpTypes.ADD;
            } else if (token.tokenType == Tokens.MINUS_OP) {
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

            if (token.tokenType == Tokens.PLUS_OP) {
                type = OpTypes.ADD;
            } else if (token.tokenType == Tokens.MINUS_OP) {
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
                FunctionSQL function = FunctionSQL.newSQLFunction(
                    token.tokenString,
                    compileContext);

                if (function == null) {
                    throw unexpectedToken();
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
                function = FunctionSQL.newSQLFunction(
                    token.tokenString,
                    compileContext);
                break;

            case Tokens.SYSTIMESTAMP :
            case Tokens.NOW :
            case Tokens.TODAY :
            case Tokens.SYSDATE :
                function = FunctionCustom.newCustomFunction(
                    session,
                    token.tokenString,
                    token.tokenType);

                if (function == null) {
                    return null;
                }

                break;

            default :
                return null;
        }

        if (function == null) {
            throw unexpectedToken();
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

            if (lastError != null && lastError.getLevel() >= ex.getLevel()) {
                ex        = lastError;
                lastError = null;
            }

            throw ex;
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
        boolean    not = false;

        if (token.tokenType == Tokens.NOT) {
            read();

            not = true;
        }

        e = XreadBooleanTestOrNull();

        if (e == null) {
            return null;
        }

        if (not) {
            e = new ExpressionLogical(OpTypes.NOT, e);
        }

        return e;
    }

    Expression XreadBooleanTestOrNull() {

        boolean    unknown = false;
        boolean    isNot   = false;
        Expression e       = XreadBooleanPrimaryOrNull();

        if (e == null) {
            return e;
        }

        if (token.tokenType == Tokens.IS) {
            read();

            if (token.tokenType == Tokens.NOT) {
                read();

                isNot = true;
            }

            if (token.tokenType == Tokens.TRUE) {
                read();

                e = new ExpressionLogical(e, new ExpressionBoolean(true));
            } else if (token.tokenType == Tokens.FALSE) {
                read();

                e = new ExpressionLogical(e, new ExpressionBoolean(false));
            } else if (token.tokenType == Tokens.UNKNOWN) {
                read();

                unknown = true;
            } else {
                throw unexpectedToken();
            }
        }

        if (unknown) {
            e = new ExpressionLogical(OpTypes.IS_NULL, e);
        }

        if (isNot) {
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

                if (lastError == null || lastError.getLevel() < ex.getLevel()) {
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
        } else {
            e = XreadSimpleValueExpressionPrimary();

            if (e != null) {
                e = XreadArrayElementReference(e);
            }
        }

        return e;
    }

    Expression XreadPredicate() {

        switch (token.tokenType) {

            case Tokens.EXISTS : {
                read();

                Expression s = XreadTableSubquery(OpTypes.EXISTS);

                return new ExpressionLogical(OpTypes.EXISTS, s);
            }

            case Tokens.UNIQUE : {
                read();

                Expression s = XreadTableSubquery(OpTypes.UNIQUE);

                return new ExpressionLogical(OpTypes.UNIQUE, s);
            }

            default : {
                Expression a = XreadRowValuePredicand();

                return XreadPredicateRightPart(a);
            }
        }
    }

    Expression XreadPredicateRightPart(final Expression l) {

        boolean           hasNot      = false;
        boolean           immediately = false;
        ExpressionLogical e           = null;
        Expression        r;
        int               position = getPosition();

        if (token.tokenType == Tokens.NOT) {
            read();

            hasNot = true;
        }

        // valid for PRECEDES and SUCCEEDS predicates
        if (token.tokenType == Tokens.IMMEDIATELY) {
            read();

            if (token.tokenType != Tokens.PRECEDES
                    && token.tokenType != Tokens.SUCCEEDS) {
                throw unexpectedToken();
            }

            immediately = true;
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

                if (token.tokenType == Tokens.NULL) {
                    read();

                    if (hasNot) {
                        e      = new ExpressionLogical(OpTypes.IS_NOT_NULL, l);
                        hasNot = false;
                    } else {
                        e = new ExpressionLogical(OpTypes.IS_NULL, l);
                    }

                    break;
                }

                if (token.tokenType == Tokens.DISTINCT) {
                    read();
                    readThis(Tokens.FROM);

                    r      = XreadRowValuePredicand();
                    e      = new ExpressionLogical(OpTypes.NOT_DISTINCT, l, r);
                    hasNot = !hasNot;
                    break;
                }

                rewind(position);

                return l;
            }

            case Tokens.LIKE : {
                e = XreadLikePredicateRightPart(l);
                break;
            }

            case Tokens.BETWEEN : {
                e = XreadBetweenPredicateRightPart(l);
                break;
            }

            case Tokens.IN : {
                e = XreadInPredicateRightPart(l);
                break;
            }

            case Tokens.CONTAINS : {
                if (hasNot) {
                    throw unexpectedToken();
                }

                e = XreadPeriodPredicateRightPart(OpTypes.RANGE_CONTAINS, l);
                break;
            }

            case Tokens.EQUALS : {
                if (hasNot) {
                    throw unexpectedToken();
                }

                e = XreadPeriodPredicateRightPart(OpTypes.RANGE_EQUALS, l);
                break;
            }

            case Tokens.OVERLAPS : {
                if (hasNot) {
                    throw unexpectedToken();
                }

                e = XreadPeriodPredicateRightPart(OpTypes.RANGE_OVERLAPS, l);
                break;
            }

            case Tokens.PRECEDES : {
                if (hasNot) {
                    throw unexpectedToken();
                }

                if (immediately) {
                    e = XreadPeriodPredicateRightPart(
                        OpTypes.RANGE_IMMEDIATELY_PRECEDES,
                        l);
                } else {
                    e = XreadPeriodPredicateRightPart(
                        OpTypes.RANGE_PRECEDES,
                        l);
                }

                break;
            }

            case Tokens.SUCCEEDS : {
                if (hasNot) {
                    throw unexpectedToken();
                }

                if (immediately) {
                    e = XreadPeriodPredicateRightPart(
                        OpTypes.RANGE_IMMEDIATELY_SUCCEEDS,
                        l);
                } else {
                    e = XreadPeriodPredicateRightPart(
                        OpTypes.RANGE_SUCCEEDS,
                        l);
                }

                break;
            }

            case Tokens.EQUALS_OP :
            case Tokens.GREATER_EQUALS :
            case Tokens.GREATER_OP :
            case Tokens.LESS_OP :
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

        Expression        right       = XreadRowValuePredicand();
        Expression l = new ExpressionLogical(OpTypes.GREATER_EQUAL, a, left);
        Expression r = new ExpressionLogical(OpTypes.SMALLER_EQUAL, a, right);
        ExpressionLogical leftToRight = new ExpressionLogical(
            OpTypes.AND,
            l,
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

    private ExpressionLogical XreadQuantifiedComparisonRightPart(
            int exprType,
            Expression l) {

        int        tokenType   = token.tokenType;
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
        int brackets = readOpenBrackets();

        switch (token.tokenType) {

            case Tokens.UNNEST :
                e = XreadCollectionDerivedTable(OpTypes.IN);

                readThis(Tokens.CLOSEBRACKET);
                readCloseBrackets(brackets);
                break;

            case Tokens.WITH :
            case Tokens.TABLE :
            case Tokens.VALUES :
            case Tokens.SELECT :
                rewind(position);

                TableDerived td = XreadSubqueryTableBody(OpTypes.IN);

                e = new Expression(OpTypes.TABLE_SUBQUERY, td);

                readThis(Tokens.CLOSEBRACKET);
                break;

            default :
                rewind(position);

                e = readAggregateExpression(tokenType);

                readThis(Tokens.CLOSEBRACKET);
                readFilterClause(e);
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
                e = XreadCollectionDerivedTable(OpTypes.IN);

                readThis(Tokens.CLOSEBRACKET);
                readCloseBrackets(brackets);
                break;

            case Tokens.WITH :
            case Tokens.TABLE :
            case Tokens.VALUES :
            case Tokens.SELECT : {
                rewind(position);

                TableDerived td = XreadSubqueryTableBody(OpTypes.IN);

                e = new Expression(OpTypes.TABLE_SUBQUERY, td);

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

        r = new ExpressionLogical(OpTypes.EQUAL, l, e);

        r.setSubType(OpTypes.ANY_QUANTIFIED);

        return r;
    }

    Expression XreadInValueList(int degree) {

        HsqlArrayList<Expression> list = new HsqlArrayList<>();

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

        Expression e = new Expression(OpTypes.VALUELIST, array);

        for (int i = 0; i < array.length; i++) {
            if (array[i].getType() != OpTypes.ROW) {
                array[i] = new Expression(
                    OpTypes.ROW,
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

        return new ExpressionLike(a, b, escape);
    }

    private ExpressionLogical XreadMatchPredicateRightPart(Expression a) {

        boolean isUnique  = false;
        int     matchType = OpTypes.MATCH_SIMPLE;

        read();

        if (token.tokenType == Tokens.UNIQUE) {
            read();

            isUnique = true;
        }

        switch (token.tokenType) {

            default :
                matchType = isUnique
                            ? OpTypes.MATCH_UNIQUE_SIMPLE
                            : OpTypes.MATCH_SIMPLE;
                break;

            case Tokens.SIMPLE :
                read();

                matchType = isUnique
                            ? OpTypes.MATCH_UNIQUE_SIMPLE
                            : OpTypes.MATCH_SIMPLE;
                break;

            case Tokens.PARTIAL :
                read();

                matchType = isUnique
                            ? OpTypes.MATCH_UNIQUE_PARTIAL
                            : OpTypes.MATCH_PARTIAL;
                break;

            case Tokens.FULL :
                read();

                matchType = isUnique
                            ? OpTypes.MATCH_UNIQUE_FULL
                            : OpTypes.MATCH_FULL;
                break;
        }

        int        mode = isUnique
                          ? OpTypes.MATCH_SIMPLE
                          : OpTypes.IN;
        Expression s    = XreadTableSubquery(mode);

        return new ExpressionLogical(matchType, a, s);
    }

    /**
     * OVERLAPS does not require PERIOD, others do.
     * CONTAINS can have single value right side
     */
    private ExpressionLogical XreadPeriodPredicateRightPart(
            int opType,
            Expression left) {

        boolean isLeftRow = false;

        switch (left.getType()) {

            case OpTypes.COLUMN :
                left = new ExpressionPeriod((ExpressionColumn) left);
                break;

            case OpTypes.PERIOD :
                break;

            case OpTypes.ROW :
                if (left.nodes.length != 2) {
                    throw Error.error(ErrorCode.X_42564);
                }

                isLeftRow = true;
                break;

            default :
                throw Error.error(ErrorCode.X_42564);
        }

        read();

        boolean period = false;

        if (token.tokenType == Tokens.PERIOD) {
            if (isLeftRow) {
                throw unexpectedToken();
            }

            read();

            period = true;

            if (token.tokenType != Tokens.OPENBRACKET) {
                throw unexpectedTokenRequire(Tokens.T_OPENBRACKET);
            }
        }

        Expression right = XreadRowValuePredicand();

        switch (right.getType()) {

            case OpTypes.COLUMN :
                if (period) {
                    throw Error.error(ErrorCode.X_42564);
                }

                right = new ExpressionPeriod((ExpressionColumn) right);
                break;

            case OpTypes.ROW :
                if (right.nodes.length == 2) {
                    if (period) {
                        right = new ExpressionPeriod(right);
                    } else {
                        if (opType != OpTypes.RANGE_OVERLAPS) {
                            throw Error.error(ErrorCode.X_42564);
                        }

                        return new ExpressionLogical(
                            OpTypes.OVERLAPS,
                            left,
                            right);
                    }

                    break;
                }

                throw Error.error(ErrorCode.X_42564);

            default :
                if (opType != OpTypes.RANGE_CONTAINS) {
                    throw Error.error(ErrorCode.X_42564);
                }

                if (period) {
                    throw Error.error(ErrorCode.X_42564);
                }

                break;
        }

        return new ExpressionPeriodOp(opType, left, right);
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
    // must be called in conjunction with <parenthesized ..
    Expression XreadExplicitRowValueConstructorOrNull() {

        Expression e;

        switch (token.tokenType) {

            case Tokens.OPENBRACKET : {
                read();

                int position = getPosition();

                readOpenBrackets();

                switch (token.tokenType) {

                    case Tokens.TABLE :
                    case Tokens.VALUES :
                    case Tokens.SELECT :
                        rewind(position);

                        TableDerived td = XreadSubqueryTableBody(
                            OpTypes.ROW_SUBQUERY);

                        readThis(Tokens.CLOSEBRACKET);

                        return new Expression(OpTypes.ROW_SUBQUERY, td);

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

        Expression                e;
        HsqlArrayList<Expression> list = new HsqlArrayList<>();

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

    Expression XreadTableSubquery(int mode) {

        readThis(Tokens.OPENBRACKET);

        TableDerived td = XreadSubqueryTableBody(mode);

        readThis(Tokens.CLOSEBRACKET);

        return new Expression(OpTypes.TABLE_SUBQUERY, td);
    }

    Table XreadTableSubqueryOrNull(boolean parens) {

        boolean joinedTable = false;
        int     position;

        position = getPosition();

        readThis(Tokens.OPENBRACKET);

        switch (token.tokenType) {

            case Tokens.TABLE :
            case Tokens.VALUES :
            case Tokens.SELECT :
            case Tokens.WITH :
                break;

            case Tokens.OPENBRACKET :
                if (parens) {
                    break;
                }

            // fall through
            default :
                joinedTable = true;
        }

        if (joinedTable) {
            rewind(position);

            return null;
        } else {
            TableDerived td = XreadSubqueryTableBody(OpTypes.TABLE_SUBQUERY);

            readThis(Tokens.CLOSEBRACKET);

            return td;
        }
    }

    TableDerived XreadJoinedTableAsSubqueryOrNull() {

        int      position = getPosition();
        Recorder recorder = startRecording();

        readThis(Tokens.OPENBRACKET);
        compileContext.incrementDepth();

        QuerySpecification qs = XreadJoinedTableAsView();

        qs.resolveReferences(session, compileContext.getOuterRanges());

        if (qs.rangeVariables.length < 2) {
            compileContext.decrementDepth();
            rewind(position);

            return null;
        }

        qs.resolveTypesPartOne(session);
        qs.resolveTypesPartTwo(session);

        TableDerived td = newSubQueryTable(qs, OpTypes.TABLE_SUBQUERY);

        readThis(Tokens.CLOSEBRACKET);

        String sql = recorder.getSQL();

        td.setSQL(sql);
        td.prepareTable(session);
        compileContext.decrementDepth();

        return td;
    }

    QuerySpecification XreadJoinedTableAsView() {

        QuerySpecification select = new QuerySpecification(compileContext);
        Expression         e      = new ExpressionColumn(OpTypes.MULTICOLUMN);

        select.addSelectColumnExpression(e);
        XreadTableReference(select);

        return select;
    }

    TableDerived XreadTableNamedSubqueryBody(
            HsqlName name,
            HsqlName[] columnNames) {

        TableDerived td;
        TableDerived namedtd  = null;
        int          position = getPosition();

        if (columnNames == null) {
            td = XreadSubqueryTableBody(name, OpTypes.TABLE_SUBQUERY);

            if (td.queryExpression != null) {
                td.queryExpression.resolve(
                    session,
                    compileContext.getOuterRanges(),
                    null);
            }

            td.prepareTable(session);
        } else {
            try {
                td = XreadSubqueryTableBody(name, OpTypes.TABLE_SUBQUERY);

                if (td.queryExpression != null) {
                    td.queryExpression.resolve(
                        session,
                        compileContext.getOuterRanges(),
                        null);
                }

                td.prepareTable(session, columnNames);
            } catch (HsqlException e) {
                HsqlException ex = e;

                if (e.getErrorCode() != ErrorCode.X_42501) {
                    if (lastError != null
                            && lastError.getErrorCode() == -ErrorCode.X_42501) {
                        ex = lastError;
                    }
                }

                String token = ex.getToken();

                if (SqlInvariants.RECURSIVE_TABLE.equals(token)
                        || name.name.equals(token)) {
                    rewind(position);

                    td = XreadRecursiveSubqueryBody(name, columnNames);
                } else {
                    throw e;
                }
            }
        }

        return td;
    }

    TableDerived XreadRecursiveSubqueryBody(
            HsqlName name,
            HsqlName[] columnNames) {

        int             position            = getPosition();
        QueryExpression leftQueryExpression = XreadSimpleTable();

        leftQueryExpression.isBaseMergeable = false;
        leftQueryExpression.isMergeable     = false;

        leftQueryExpression.resolveReferences(
            session,
            compileContext.getOuterRanges());
        leftQueryExpression.resolve(session);

        HsqlName leftName = database.nameManager.newHsqlName(
            SqlInvariants.SYSTEM_SUBQUERY,
            false,
            SchemaObject.SUBQUERY);
        TableDerived leftTable = new TableDerived(
            database,
            leftName,
            TableBase.SYSTEM_SUBQUERY,
            columnNames,
            leftQueryExpression.getColumnTypes());
        TableDerived workTable = new TableDerived(
            database,
            name,
            TableBase.SYSTEM_SUBQUERY,
            columnNames,
            leftQueryExpression.getColumnTypes());
        HsqlName recursiveName = database.nameManager.newHsqlName(
            SqlInvariants.RECURSIVE_TABLE,
            false,
            SchemaObject.SUBQUERY);
        TableDerived recursiveTable = new TableDerived(
            database,
            recursiveName,
            TableBase.SYSTEM_SUBQUERY,
            columnNames,
            leftQueryExpression.getColumnTypes());

        compileContext.registerSubquery(recursiveName.name, recursiveTable);
        compileContext.registerSubquery(name.name, workTable);
        checkIsThis(Tokens.UNION);

        int unionType = XreadUnionType();

        compileContext.incrementDepth();

        QuerySpecification rightQuerySpecification = XreadSimpleTable();
        QueryExpression queryExpression = new QueryExpression(
            compileContext,
            leftQueryExpression);

        rightQuerySpecification.isBaseMergeable = false;
        rightQuerySpecification.isMergeable     = false;

        rightQuerySpecification.resolveReferences(
            session,
            compileContext.getOuterRanges());
        queryExpression.addUnion(rightQuerySpecification, unionType);

        queryExpression.isRecursive          = true;
        queryExpression.recursiveWorkTable   = workTable;
        queryExpression.recursiveResultTable = recursiveTable;
        queryExpression.isBaseMergeable      = false;
        queryExpression.isMergeable          = false;

        queryExpression.resolve(session);

        TableDerived maintd = newSubQueryTable(
            name,
            queryExpression,
            OpTypes.TABLE_SUBQUERY);

        maintd.prepareTable(session, columnNames);
        maintd.setSQL(getLastPart(position));
        compileContext.decrementDepth();

        return maintd;
    }

    TableDerived newSubQueryTable(Expression e, int opType) {

        HsqlName name = database.nameManager.getSubqueryTableName();
        TableDerived td = new TableDerived(
            database,
            name,
            TableBase.SYSTEM_SUBQUERY,
            null,
            e,
            opType,
            compileContext.getDepth());

        return td;
    }

    TableDerived newSubQueryTable(QueryExpression qe, int opType) {
        return newSubQueryTable(null, qe, opType);
    }

    TableDerived newSubQueryTable(
            HsqlName name,
            QueryExpression qe,
            int opType) {

        if (name == null) {
            name = database.nameManager.getSubqueryTableName();
        }

        TableDerived td = new TableDerived(
            database,
            name,
            TableBase.SYSTEM_SUBQUERY,
            qe,
            null,
            opType,
            compileContext.getDepth());

        return td;
    }

    TableDerived XreadSubqueryTableBody(int type) {
        return XreadSubqueryTableBody(null, type);
    }

    TableDerived XreadSubqueryTableBody(HsqlName name, int type) {

        int position = getPosition();

        compileContext.incrementDepth();

        try {
            QueryExpression queryExpression = XreadQueryExpression();
            TableDerived    td              = null;

            if (type == OpTypes.EXISTS) {
                queryExpression.setAsExists();
            }

            if (queryExpression.isValueList) {
                td = ((QuerySpecification) queryExpression).getValueListTable();
            }

            if (td == null) {
                td = newSubQueryTable(name, queryExpression, type);
            }

            String sql = getLastPart(position);

            td.setSQL(sql);

            return td;
        } finally {
            compileContext.decrementDepth();
        }
    }

    TableDerived XreadViewSubqueryTable(View view, boolean resolve) {

        compileContext.incrementDepth();

        QueryExpression queryExpression;

        try {
            queryExpression = XreadQueryExpression();
        } catch (HsqlException e) {
            queryExpression = XreadJoinedTableAsView();
        }

        queryExpression.setView(view);
        queryExpression.resolveReferences(session, RangeGroup.emptyArray);
        queryExpression.resolveTypesPartOne(session);
        queryExpression.resolveTypesPartTwo(session);

        if (resolve) {
            queryExpression.resolveTypesPartThree(session);
        }

        TableDerived td = new TableDerived(
            database,
            view.getName(),
            TableBase.VIEW_TABLE,
            queryExpression,
            null,
            OpTypes.NONE,
            compileContext.getDepth());

        td.view        = view;
        td.columnList  = view.columnList;
        td.columnCount = view.columnList.size();

        td.createPrimaryKey();

        td.triggerList  = view.triggerList;
        td.triggerLists = view.triggerLists;

        compileContext.decrementDepth();

        return td;
    }

    Expression XreadContextuallyTypedTable(int degree) {

        Expression   e       = readRow();
        Expression[] list    = e.nodes;
        boolean      isTable = false;

        if (degree == 1) {
            if (e.getType() == OpTypes.ROW) {
                e.opType = OpTypes.VALUELIST;

                for (int i = 0; i < list.length; i++) {
                    if (list[i].getType() != OpTypes.ROW) {
                        list[i] = new Expression(
                            OpTypes.ROW,
                            new Expression[]{ list[i] });
                    } else if (list[i].nodes.length != degree) {
                        throw Error.error(ErrorCode.X_42564);
                    }
                }
            } else {
                e = new Expression(OpTypes.ROW, new Expression[]{ e });
                e = new Expression(OpTypes.VALUELIST, new Expression[]{ e });
            }

            return e;
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
            e.opType = OpTypes.VALUELIST;

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

            e = new Expression(OpTypes.VALUELIST, new Expression[]{ e });
        }

        return e;
    }

    Expression XreadInValueListConstructor(int degree) {

        int position = getPosition();

        compileContext.incrementDepth();

        Expression   e  = XreadInValueList(degree);
        TableDerived td = newSubQueryTable(e, OpTypes.IN);

        td.setSQL(getLastPart(position));

        e.table = td;

        compileContext.decrementDepth();

        return e;
    }

    private TableDerived XreadRowValueExpressionList() {

        compileContext.incrementDepth();

        Expression   e  = XreadRowValueExpressionListBody();
        TableDerived td = prepareSubqueryTable(e, null);

        compileContext.decrementDepth();

        return td;
    }

    private TableDerived prepareSubqueryTable(
            Expression e,
            HsqlName[] colNames) {

        List<Expression> unresolved = e.resolveColumnReferences(
            session,
            RangeGroup.emptyGroup,
            compileContext.getOuterRanges(),
            null);

        ExpressionColumn.checkColumnsResolved(unresolved);
        e.resolveTypes(session, null);
        e.prepareTable(session, null, e.nodes[0].nodes.length);

        TableDerived td = newSubQueryTable(e, OpTypes.VALUELIST);

        td.prepareTable(session, colNames);

        return td;
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
                r.nodes = (Expression[]) ArrayUtil.resizeArray(
                    r.nodes,
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

        r.opType = OpTypes.VALUELIST;

        for (int i = 0; i < list.length; i++) {
            if (list[i].getType() == OpTypes.ROW) {
                if (list[i].nodes.length != degree) {
                    throw Error.error(ErrorCode.X_42564);
                }
            } else {
                if (degree != 1) {
                    throw Error.error(ErrorCode.X_42564);
                }

                list[i] = new Expression(
                    OpTypes.ROW,
                    new Expression[]{ list[i] });
            }
        }

        return r;
    }

    Expression XreadTargetSpecification(
            RangeVariable[] rangeVars,
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

            index = rangeVars[i].findColumn(
                token.namePrePrefix,
                token.namePrefix,
                token.tokenString);

            if (index > -1) {
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

    Expression XreadCollectionDerivedTable(int type) {

        boolean ordinality = false;
        int     position   = getPosition();

        readThis(Tokens.UNNEST);
        readThis(Tokens.OPENBRACKET);
        compileContext.incrementDepth();

        HsqlArrayList<Expression> list = new HsqlArrayList<>();

        while (true) {
            Expression e = XreadValueExpression();

            list.add(e);

            if (token.tokenType == Tokens.COMMA) {
                read();
            } else {
                break;
            }
        }

        Expression[] array = new Expression[list.size()];

        list.toArray(array);
        readThis(Tokens.CLOSEBRACKET);

        if (token.tokenType == Tokens.WITH) {
            read();
            readThis(Tokens.ORDINALITY);

            ordinality = true;
        }

        Expression   e  = new ExpressionTable(array, ordinality);
        TableDerived td = newSubQueryTable(e, type);

        td.setSQL(getLastPart(position));
        compileContext.decrementDepth();

        return e;
    }

    Expression XreadTableFunctionDerivedTable() {

        int position = getPosition();

        readThis(Tokens.TABLE);
        readThis(Tokens.OPENBRACKET);
        compileContext.incrementDepth();

        Expression e = XreadValueExpression();

        if (e.getType() != OpTypes.FUNCTION
                && e.getType() != OpTypes.SQL_FUNCTION) {
            compileContext.decrementDepth();

            throw unexpectedToken(Tokens.T_TABLE);
        }

        readThis(Tokens.CLOSEBRACKET);

        e = new ExpressionTable(new Expression[]{ e }, false);

        TableDerived td = newSubQueryTable(e, OpTypes.TABLE_SUBQUERY);

        td.setSQL(getLastPart(position));
        compileContext.decrementDepth();

        return e;
    }

    Expression XreadLateralDerivedTable() {

        readThis(Tokens.LATERAL);
        readThis(Tokens.OPENBRACKET);

        TableDerived td = XreadSubqueryTableBody(OpTypes.TABLE_SUBQUERY);

        readThis(Tokens.CLOSEBRACKET);

        return new Expression(OpTypes.TABLE_SUBQUERY, td);
    }

    Expression XreadArrayConstructor() {

        readThis(Tokens.OPENBRACKET);

        TableDerived td = XreadSubqueryTableBody(OpTypes.TABLE_SUBQUERY);

        readThis(Tokens.CLOSEBRACKET);

        return new Expression(OpTypes.ARRAY_SUBQUERY, td);
    }

    // Additional Common Elements
    Collation readCollateClauseOrNull() {

        if (token.tokenType == Tokens.COLLATE) {
            read();

            Collation collation = database.schemaManager.getCollation(
                session,
                token.tokenString,
                token.namePrefix);

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
                    r = new Expression(OpTypes.ROW, new Expression[]{ r, e });
                } else {
                    r.nodes = (Expression[]) ArrayUtil.resizeArray(
                        r.nodes,
                        r.nodes.length + 1);
                    r.nodes[r.nodes.length - 1] = e;
                }
            } else {
                r = new Expression(OpTypes.ROW, new Expression[]{ r, e });
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
     * Reads part of a CASE ... WHEN  expression
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
                    newCondition = new ExpressionLogical(
                        l,
                        XreadRowValuePredicand());
                }

                if (condition == null) {
                    condition = newCondition;
                } else {
                    condition = new ExpressionLogical(
                        OpTypes.OR,
                        condition,
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
            elseExpr = new ExpressionValue(null, null);

            readThis(Tokens.END);
            readIfThis(Tokens.CASE);
        }

        Expression alt = new ExpressionOp(
            OpTypes.ALTERNATIVE,
            current,
            elseExpr);
        Expression casewhen = new ExpressionOp(
            OpTypes.CASEWHEN,
            condition,
            alt);

        return casewhen;
    }

    /**
     * reads a CASEWHEN expression
     */
    private Expression readCaseWhenExpressionOrNull() {

        Expression l        = null;
        int        position = getPosition();

        if (token.tokenType == Tokens.IF) {
            if (database.sqlSyntaxMys || database.sqlSyntaxMss) {}
            else {
                return null;
            }
        }

        read();

        if (!readIfThis(Tokens.OPENBRACKET)) {
            rewind(position);

            return null;
        }

        l = XreadBooleanValueExpression();

        readThis(Tokens.COMMA);

        Expression then = XreadValueExpression();

        readThis(Tokens.COMMA);

        Expression thenelse = new ExpressionOp(
            OpTypes.ALTERNATIVE,
            then,
            XreadValueExpression());

        l = new ExpressionOp(OpTypes.CASEWHEN, l, thenelse);

        readThis(Tokens.CLOSEBRACKET);

        return l;
    }

    /**
     * Reads a CAST expression
     */
    private Expression readCastExpression() {

        Expression e;
        Type       typeObject;
        String     template;

        read();
        readThis(Tokens.OPENBRACKET);

        e = XreadValueExpression();

        readThis(Tokens.AS);

        typeObject = readTypeDefinition(false, true);

        if (readIfThis(Tokens.FORMAT)) {
            template = readQuotedString();
        } else {
            template = null;
        }

        if (e.isUnresolvedParam()) {
            e.setDataType(session, typeObject);
        } else {
            e = new ExpressionOp(e, typeObject, template);
        }

        readThis(Tokens.CLOSEBRACKET);

        return e;
    }

    private Expression readConvertExpressionOrNull() {

        Expression e;
        Expression format = null;
        Type       typeObject;
        int        position = getPosition();

        read();

        if (!readIfThis(Tokens.OPENBRACKET)) {
            rewind(position);

            return null;
        }

        if (database.sqlSyntaxMss) {
            typeObject = readTypeDefinition(false, true);

            readThis(Tokens.COMMA);

            e = XreadValueExpression();

            if (readIfThis(Tokens.COMMA)) {
                format = this.XreadSimpleValueSpecificationOrNull();
            }
        } else {
            e = XreadValueExpression();

            readThis(Tokens.COMMA);

            typeObject = Type.getTypeForJDBCConvertToken(token.tokenType);

            if (typeObject == null) {
                typeObject = readTypeDefinition(false, true);
            } else {
                read();
            }
        }

        if (e.isUnresolvedParam()) {
            e.setDataType(session, typeObject);
        } else {
            e = new ExpressionOp(e, typeObject);
        }

        readThis(Tokens.CLOSEBRACKET);

        return e;
    }

    /**
     * reads a Column or Function expression
     */
    private Expression readColumnOrFunctionExpression() {

        String  name           = token.tokenString;
        boolean isSimpleQuoted = isDelimitedSimpleName();
        Token   recordedToken  = getRecordedToken();

        checkIsIdentifier();

        if (isUndelimitedSimpleName()) {
            Expression e = readFunction();

            if (e != null) {
                return e;
            }
        }

        read();

        if (token.tokenType != Tokens.OPENBRACKET) {
            checkValidCatalogName(recordedToken.namePrePrePrefix);

            Expression column = new ExpressionColumn(
                recordedToken.namePrePrefix,
                recordedToken.namePrefix,
                name);

            return column;
        }

        RoutineSchema routineSchema =
            (RoutineSchema) database.schemaManager.findSchemaObject(
                session,
                name,
                recordedToken.namePrefix,
                recordedToken.namePrePrefix,
                SchemaObject.FUNCTION);

        if (routineSchema == null
                && recordedToken.namePrefix == null
                && !isViewDefinition) {
            String schema = session.getSchemaName(null);
            ReferenceObject synonym = database.schemaManager.findSynonym(
                recordedToken.tokenString,
                schema,
                SchemaObject.ROUTINE);

            if (synonym != null) {
                HsqlName synonymName = synonym.getTarget();

                routineSchema =
                    (RoutineSchema) database.schemaManager.findSchemaObject(
                        synonymName.name,
                        synonymName.schema.name,
                        SchemaObject.ROUTINE);
            }
        }

        if (routineSchema == null && isSimpleQuoted) {
            HsqlName schema = database.schemaManager.getDefaultSchemaHsqlName();

            routineSchema =
                (RoutineSchema) database.schemaManager.findSchemaObject(
                    name,
                    schema.name,
                    SchemaObject.FUNCTION);

            if (routineSchema == null) {
                Routine.createRoutines(session, schema, name);

                routineSchema =
                    (RoutineSchema) database.schemaManager.findSchemaObject(
                        name,
                        schema.name,
                        SchemaObject.FUNCTION);
            }
        }

        if (routineSchema == null) {
            if (lastError != null) {
                throw lastError;
            }

            throw Error.error(ErrorCode.X_42501, name);
        }

        HsqlArrayList<Expression> list = new HsqlArrayList<>();

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

    Expression readFunction() {

        FunctionSQL function = FunctionCustom.newCustomFunction(
            session,
            token.tokenString,
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

                if (lastError == null || lastError.getLevel() < ex.getLevel()) {
                    lastError = ex;
                }

                rewind(pos);
            }
        } else if (isReservedKey()) {
            function = FunctionSQL.newSQLFunction(
                token.tokenString,
                compileContext);

            if (function != null) {
                Expression e = readSQLFunction(function);

                return e;
            }
        }

        return null;
    }

    Expression readCollection(int type) {

        read();

        if (token.tokenType == Tokens.OPENBRACKET) {
            return XreadArrayConstructor();
        } else {
            readThis(Tokens.LEFTBRACKET);

            HsqlArrayList<Expression> list = new HsqlArrayList<>();

            for (int i = 0; ; i++) {
                if (token.tokenType == Tokens.RIGHTBRACKET) {
                    read();
                    break;
                }

                if (i > 0) {
                    readThis(Tokens.COMMA);
                }

                Expression e = XreadValueExpression();

                list.add(e);
            }

            Expression[] array = new Expression[list.size()];

            list.toArray(array);

            return new Expression(OpTypes.ARRAY, array);
        }
    }

    private Expression readDecodeExpressionOrNull() {

        int position = getPosition();

        read();

        if (!readIfThis(Tokens.OPENBRACKET)) {
            rewind(position);

            return null;
        }

        Expression casewhen    = null;
        Expression alternative = null;
        Expression main        = XreadValueExpression();

        readThis(Tokens.COMMA);

        do {
            Expression v = XreadValueExpression();

            if (token.tokenType == Tokens.COMMA) {
                readThis(Tokens.COMMA);
            } else {
                if (alternative == null) {
                    throw unexpectedToken();
                }

                alternative.setRightNode(v);
                break;
            }

            Expression l = new ExpressionLogical(OpTypes.NOT_DISTINCT, main, v);
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
                alternative.setRightNode(new ExpressionValue(null, null));
                break;
            }
        } while (true);

        readThis(Tokens.CLOSEBRACKET);

        return casewhen;
    }

    private Expression readConcatExpressionOrNull() {

        Expression root;
        Expression r;

        // turn into a concatenation
        int position = getPosition();

        read();

        if (!readIfThis(Tokens.OPENBRACKET)) {
            rewind(position);

            return null;
        }

        root = XreadValueExpression();

        readThis(Tokens.COMMA);

        do {
            r    = XreadValueExpression();
            root = new ExpressionArithmetic(OpTypes.CONCAT, root, r);

            if (token.tokenType == Tokens.COMMA) {
                readThis(Tokens.COMMA);
            } else if (token.tokenType == Tokens.CLOSEBRACKET) {
                readThis(Tokens.CLOSEBRACKET);
                break;
            }
        } while (true);

        return root;
    }

    private Expression readConcatSeparatorExpressionOrNull() {

        HsqlArrayList<Expression> array    = new HsqlArrayList<>();
        int                       position = getPosition();

        read();

        if (!readIfThis(Tokens.OPENBRACKET)) {
            rewind(position);

            return null;
        }

        Expression e = XreadValueExpression();

        array.add(e);
        readThis(Tokens.COMMA);

        e = XreadValueExpression();

        array.add(e);
        readThis(Tokens.COMMA);

        do {
            e = XreadValueExpression();

            array.add(e);

            if (token.tokenType == Tokens.COMMA) {
                readThis(Tokens.COMMA);
            } else if (token.tokenType == Tokens.CLOSEBRACKET) {
                readThis(Tokens.CLOSEBRACKET);
                break;
            }
        } while (true);

        Expression[] expressions = new Expression[array.size()];

        array.toArray(expressions);

        return new ExpressionOp(OpTypes.CONCAT_WS, expressions);
    }

    private Expression readLeastExpressionOrNull() {

        int position = getPosition();

        read();

        if (!readIfThis(Tokens.OPENBRACKET)) {
            rewind(position);

            return null;
        }

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

    private Expression readGreatestExpressionOrNull() {

        int position = getPosition();

        read();

        if (!readIfThis(Tokens.OPENBRACKET)) {
            rewind(position);

            return null;
        }

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

        read();
        readThis(Tokens.OPENBRACKET);

        Expression c = XreadValueExpression();

        readThis(Tokens.COMMA);

        Expression alternative = new ExpressionOp(
            OpTypes.ALTERNATIVE,
            new ExpressionValue(null, null),
            c);

        c = new ExpressionLogical(c, XreadValueExpression());
        c = new ExpressionOp(OpTypes.CASEWHEN, c, alternative);

        readThis(Tokens.CLOSEBRACKET);

        return c;
    }

    /**
     * Reads a ISNULL or ISNULL of NVL expression
     */
    private Expression readIfNullExpressionOrNull() {

        int position = getPosition();

        read();

        if (!readIfThis(Tokens.OPENBRACKET)) {
            rewind(position);

            return null;
        }

        Expression c = XreadValueExpression();

        readThis(Tokens.COMMA);

        Expression e         = XreadValueExpression();
        Expression condition = new ExpressionLogical(OpTypes.IS_NULL, c);
        Expression alt       = new ExpressionOp(OpTypes.ALTERNATIVE, e, c);

        c = new ExpressionOp(OpTypes.CASEWHEN, condition, alt);

        c.setSubType(OpTypes.CAST);
        alt.setSubType(OpTypes.CAST);
        readThis(Tokens.CLOSEBRACKET);

        return c;
    }

    /**
     * Reads a NVL2 expression
     */
    private Expression readIfNull2ExpressionOrNull() {

        int position = getPosition();

        read();

        if (!readIfThis(Tokens.OPENBRACKET)) {
            rewind(position);

            return null;
        }

        Expression c = XreadValueExpression();

        readThis(Tokens.COMMA);

        Expression e1 = XreadValueExpression();

        readThis(Tokens.COMMA);

        Expression e2        = XreadValueExpression();
        Expression condition = new ExpressionLogical(OpTypes.IS_NULL, c);
        Expression alt       = new ExpressionOp(OpTypes.ALTERNATIVE, e2, e1);

        c = new ExpressionOp(OpTypes.CASEWHEN, condition, alt);

        c.setSubType(OpTypes.CAST);
        alt.setSubType(OpTypes.CAST);
        readThis(Tokens.CLOSEBRACKET);

        return c;
    }

    /**
     * Reads a COALESE expression
     */
    private Expression readCoalesceExpression() {

        Expression c = null;

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

            Expression expressionNull = new ExpressionValue(null, null);
            Expression condition = new ExpressionLogical(
                OpTypes.IS_NULL,
                current);
            Expression alt = new ExpressionOp(
                OpTypes.ALTERNATIVE,
                expressionNull,
                current);
            Expression casewhen = new ExpressionOp(
                OpTypes.CASEWHEN,
                condition,
                alt);

            if (session.database.sqlSyntaxMys) {
                alt.setSubType(OpTypes.CAST);
                casewhen.setSubType(OpTypes.CAST);
            }

            if (c == null) {
                c = casewhen;
            } else {
                leaf.setLeftNode(casewhen);
            }

            leaf = alt;

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

        HsqlArrayList<Expression> exprList = new HsqlArrayList<>();
        boolean isOpenBracket = token.tokenType == Tokens.OPENBRACKET;

        if (!isOpenBracket) {
            if (parseList[0] == Tokens.X_OPTION) {
                return function;
            } else {
                rewind(position);

                return null;
            }
        }

        try {
            readExpression(exprList, parseList, 0, parseList.length, false);

            lastError = null;
        } catch (HsqlException e) {
            if (function.parseListAlt == null) {
                throw e;
            }

            rewind(position);
            read();

            parseList = function.parseListAlt;
            exprList  = new HsqlArrayList<>();

            readExpression(exprList, parseList, 0, parseList.length, false);

            lastError = null;
        }

        Expression[] expr = new Expression[exprList.size()];

        exprList.toArray(expr);
        function.setArguments(expr);

        return function.getFunctionExpression();
    }

    void readExpression(
            HsqlArrayList<Expression> exprList,
            short[] parseList,
            int start,
            int count,
            boolean isOption) {

        for (int i = start; i < start + count; i++) {
            int exprType = parseList[i];

            switch (exprType) {

                case Tokens.QUESTION : {
                    Expression e = null;

                    e = XreadAllTypesCommonValueExpression(false);

                    exprList.add(e);
                    continue;
                }

                case Tokens.X_TOKEN : {
                    if (super.isUndelimitedSimpleName()) {
                        Expression e = new ExpressionValue(
                            token.tokenString,
                            Type.SQL_VARCHAR);

                        read();
                        exprList.add(e);
                        continue;
                    }

                    throw unexpectedToken();
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
                        readExpression(
                            exprList,
                            parseList,
                            i,
                            elementCount,
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
                        if (parseList[i] != Tokens.OPENBRACKET) {
                            exprList.add(null);
                        }
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

                        readExpression(
                            exprList,
                            parseList,
                            parseIndex,
                            elementCount,
                            true);

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

                    if (ArrayUtil.find(parseList,
                                       token.tokenType,
                                       i + 1,
                                       elementCount) == -1) {
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
            }
        }
    }

    private Expression readSequenceExpressionOrNull(int opType) {

        int position = getPosition();

        switch (opType) {

            case OpTypes.SEQUENCE :
                if (token.tokenType == Tokens.NEXT) {
                    read();

                    if (token.tokenType != Tokens.VALUE) {
                        rewind(position);

                        return null;
                    }

                    readThis(Tokens.VALUE);
                } else if (database.sqlSyntaxDb2
                           && token.tokenType == Tokens.NEXTVAL) {
                    read();
                } else if (database.sqlSyntaxDb2
                           && token.tokenType == Tokens.PREVVAL) {
                    read();
                } else {
                    rewind(position);

                    return null;
                }

                break;

            case OpTypes.SEQUENCE_CURRENT :
                read();
                readThis(Tokens.VALUE);
                break;

            default :
        }

        readThis(Tokens.FOR);
        checkIsSchemaObjectName();

        NumberSequence sequence = database.schemaManager.findSequence(
            session,
            token.tokenString,
            token.namePrefix);

        if (sequence == null) {
            throw Error.error(ErrorCode.X_42501, token.tokenString);
        }

        Token recordedToken = getRecordedToken();

        read();

        Expression e = new ExpressionColumn(sequence, opType);

        recordedToken.setExpression(sequence);
        compileContext.addSequence(sequence);

        return e;
    }

    SimpleName readSimpleName() {

        checkIsSimpleName();

        SimpleName name = HsqlNameManager.getSimpleName(
            token.tokenString,
            isDelimitedIdentifier());

        read();

        return name;
    }

    HsqlName readNewSchemaName() {

        HsqlName name = readNewSchemaObjectName(SchemaObject.SCHEMA, false);

        SqlInvariants.checkSchemaNameNotSystem(name.name);

        return name;
    }

    HsqlName readNewSchemaObjectName(int type, boolean checkSchema) {

        checkIsSchemaObjectName();

        HsqlName hsqlName = database.nameManager.newHsqlName(
            token.tokenString,
            isDelimitedIdentifier(),
            type);

        if (token.namePrefix != null) {
            switch (type) {

                case SchemaObject.LABEL :
                case SchemaObject.VARIABLE :
                case SchemaObject.GRANTEE :
                case SchemaObject.CATALOG :
                    throw unexpectedToken();

                case SchemaObject.CURSOR : {
                    if (token.namePrePrefix == null
                            && !token.isDelimitedPrefix
                            && (Tokens.T_MODULE.equals(token.namePrefix))) {

                        // local
                    } else {
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
                    throw tooManyIdentifiers();
                }

                default : {
                    checkValidCatalogName(token.namePrePrefix);

                    HsqlName schemaName;

                    if (checkSchema) {
                        schemaName = session.getSchemaHsqlName(
                            token.namePrefix);
                    } else {
                        schemaName =
                            session.database.schemaManager.findSchemaHsqlName(
                                token.namePrefix);

                        if (schemaName == null) {
                            schemaName = database.nameManager.newHsqlName(
                                token.namePrefix,
                                isDelimitedIdentifier(),
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

        if (name.schema != null
                && parentName.schema != null
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

        String schema = session.getSchemaName(token.namePrefix);
        SchemaObject object = database.schemaManager.getSchemaObject(
            token.tokenString,
            schema,
            type);

        read();

        return object;
    }

    SchemaObject readSchemaObjectName(HsqlName schemaName, int type) {

        checkIsSchemaObjectName();

        SchemaObject object = database.schemaManager.getSchemaObject(
            token.tokenString,
            schemaName.name,
            type);

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
        return readTableName(false);
    }

    Table readTableName(boolean orSynonym) {

        checkIsIdentifier();

        lastSynonym = null;

        Table table = database.schemaManager.findTable(
            session,
            token.tokenString,
            token.namePrefix,
            token.namePrePrefix);

        if (table == null) {
            boolean trySynonym = orSynonym
                                 && token.namePrefix == null
                                 && !isViewDefinition;

            if (trySynonym) {
                ReferenceObject reference = database.schemaManager.findSynonym(
                    token.tokenString,
                    session.getCurrentSchemaHsqlName().name,
                    SchemaObject.TABLE);

                if (reference != null) {
                    table = (Table) database.schemaManager.findSchemaObject(
                        reference.getTarget());
                    lastSynonym = reference.getName();
                }
            }

            if (table == null) {
                HsqlException ex = Error.error(
                    ErrorCode.X_42501,
                    token.tokenString);

                ex.setToken(token.tokenString);

                throw ex;
            }
        }

        getRecordedToken().setExpression(table);
        read();

        return table;
    }

    /**
     * Returns a period condition (including a default) for all tables with a
     * system period, otherwise null;
     */
    ExpressionPeriodOp XreadQuerySystemPeriodSpecOrNull(Table table) {

        int position = getPosition();

        if (!table.isSystemVersioned()) {
            return null;
        }

        if (token.tokenType == Tokens.FOR) {
            read();
        } else {
            ExpressionPeriodOp periodExpression = new ExpressionPeriodOp();

            return periodExpression;
        }

        if (token.tokenType == Tokens.SYSTEM_TIME) {
            read();
        } else {
            rewind(position);

            return null;
        }

        switch (token.tokenType) {

            case Tokens.AS : {
                read();
                readThis(Tokens.OF);

                Expression point = XreadValueExpression();

                return new ExpressionPeriodOp(point);
            }

            case Tokens.BETWEEN : {
                read();
                readIfThis(Tokens.ASYMMETRIC);

                Expression pointStart = XreadRowValuePredicand();

                readThis(Tokens.AND);

                Expression pointEnd = XreadRowValuePredicand();

                return new ExpressionPeriodOp(pointStart, pointEnd);
            }

            case Tokens.FROM : {
                read();

                Expression pointStart = XreadValueExpression();

                readThis(Tokens.TO);

                Expression pointEnd = XreadValueExpression();

                return new ExpressionPeriodOp(pointStart, pointEnd);
            }

            default :
                throw unexpectedToken();
        }
    }

    ExpressionPeriodOp XreadQueryApplicationPeriodSpecOrNull(Table table) {

        PeriodDefinition period = table.getApplicationPeriod();

        if (period == null) {
            return null;
        }

        if (token.tokenType == Tokens.FOR) {
            read();
        } else {
            return null;
        }

        readThis(Tokens.PORTION);
        readThis(Tokens.OF);
        checkIsSimpleName();

        if (!token.tokenString.equals(period.periodName.getNameString())) {
            throw Error.error(ErrorCode.X_42501, token.tokenString);
        }

        read();
        readThis(Tokens.FROM);

        Expression pointStart = XreadValueExpression();

        readThis(Tokens.TO);

        Expression       pointEnd = XreadValueExpression();
        ExpressionPeriod left     = new ExpressionPeriod(period);
        ExpressionPeriod right    = new ExpressionPeriod(pointStart, pointEnd);

        return new ExpressionPeriodOp(OpTypes.RANGE_OVERLAPS, left, right);
    }

    ColumnSchema readSimpleColumnName(
            RangeVariable rangeVar,
            boolean withPrefix) {

        ColumnSchema column = null;

        checkIsIdentifier();

        if (!withPrefix && token.namePrefix != null) {
            throw tooManyIdentifiers();
        }

        int index = rangeVar.findColumn(
            token.namePrePrefix,
            token.namePrefix,
            token.tokenString);

        if (index == -1) {
            throw Error.error(ErrorCode.X_42501, token.tokenString);
        }

        column = rangeVar.getTable().getColumn(index);

        read();

        return column;
    }

    ColumnSchema readSimpleColumnName(Table table, boolean withPrefix) {

        checkIsIdentifier();

        if (withPrefix) {
            if (token.namePrefix != null
                    && !table.getName().name.equals(token.namePrefix)) {
                throw Error.error(ErrorCode.X_42501, token.namePrefix);
            }
        } else if (token.namePrefix != null) {
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

    StatementQuery compileDeclareCursorOrNull(
            RangeGroup[] rangeGroups,
            boolean isRoutine) {

        int sensitivity   = ResultConstants.SQL_ASENSITIVE;
        int scrollability = ResultConstants.SQL_NONSCROLLABLE;
        int holdability   = ResultConstants.SQL_NONHOLDABLE;
        int returnability = ResultConstants.SQL_WITHOUT_RETURN;
        int position      = getPosition();

        readThis(Tokens.DECLARE);

        if (isReservedKey()) {
            rewind(position);

            return null;
        }

        HsqlName cursorName = readNewSchemaObjectName(
            SchemaObject.CURSOR,
            false);

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

            default :
        }

        if (token.tokenType == Tokens.NO) {
            readThis(Tokens.SCROLL);
        } else {
            if (token.tokenType == Tokens.SCROLL) {
                read();

                scrollability = ResultConstants.SQL_SCROLLABLE;
            }
        }

        if (token.tokenType != Tokens.CURSOR) {
            rewind(position);

            return null;
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

        int props = ResultProperties.getProperties(
            sensitivity,
            ResultConstants.SQL_UPDATABLE,
            scrollability,
            holdability,
            returnability);
        StatementQuery cs = compileCursorSpecification(
            rangeGroups,
            props,
            isRoutine);

        cs.setCursorName(cursorName);

        return cs;
    }

    /**
     * Retrieves a SELECT or other query expression Statement from this parse context.
     */
    StatementQuery compileCursorSpecification(
            RangeGroup[] rangeGroups,
            int props,
            boolean isRoutine) {

        OrderedHashSet<String> colNames        = null;
        QueryExpression        queryExpression = XreadQueryExpression();

        if (token.tokenType == Tokens.FOR) {
            read();

            if (token.tokenType == Tokens.READ
                    || token.tokenType == Tokens.FETCH) {
                read();
                readThis(Tokens.ONLY);

                props = ResultProperties.addUpdatable(props, false);
            } else {
                readThis(Tokens.UPDATE);

                props = ResultProperties.addUpdatable(props, true);

                if (token.tokenType == Tokens.OF) {
                    readThis(Tokens.OF);

                    colNames = new OrderedHashSet<>();

                    readColumnNameList(colNames, null, false);
                }

                if (database.sqlSyntaxOra) {
                    readIfThis(Tokens.NOWAIT);
                }
            }
        }

        if (database.sqlSyntaxDb2) {
            if (readIfThis(Tokens.WITH)) {
                if (!readIfThis("CS")) {
                    if (!readIfThis("RR")) {
                        if (!readIfThis("RS")) {
                            readThis("UR");
                        }
                    }
                }
            }

            if (readIfThis(Tokens.USE)) {
                readThis(Tokens.AND);
                readThis(Tokens.T_KEEP);

                if (!readIfThis(Tokens.T_EXCLUSIVE)) {
                    if (!readIfThis(Tokens.T_SHARE)) {
                        readThis(Tokens.UPDATE);
                    }
                }

                readThis(Tokens.LOCKS);
            }
        }

        if (ResultProperties.isUpdatable(props)) {
            queryExpression.isUpdatable = true;
        }

        queryExpression.setReturningResult();

        if (database.sqlLowerCaseIdentifier && !isRoutine) {
            queryExpression.setLowerCaseResultIdentifer();
        }

        queryExpression.resolve(session, rangeGroups, null);

        StatementQuery cs = isRoutine
                            ? new StatementCursor(
                                session,
                                queryExpression,
                                compileContext)
                            : new StatementQuery(
                                session,
                                queryExpression,
                                compileContext);

        return cs;
    }

    StatementDMQL compileShortCursorSpecification(int props) {

        QueryExpression select = XreadSimpleTable();

        if (ResultProperties.isUpdatable(props)) {
            select.isUpdatable = true;
        }

        select.setReturningResult();
        select.resolve(session);

        StatementDMQL cs = new StatementQuery(session, select, compileContext);

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

    void readNestedParenthesisedTokens() {

        readThis(Tokens.OPENBRACKET);

        do {
            read();

            if (token.tokenType == Tokens.OPENBRACKET) {
                readNestedParenthesisedTokens();
            }

            if (token.tokenType == Tokens.X_ENDPARSE) {
                throw unexpectedToken();
            }
        } while (token.tokenType != Tokens.CLOSEBRACKET);

        read();
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

        final Session        session;
        final ParserBase     parser;
        final CompileContext baseContext;
        final int            basePosition;
        boolean              isViewTable;

        //
        private int subqueryDepth;
        private HsqlArrayList<OrderedHashMap<String, TableDerived>> namedSubqueries;

        //
        private final OrderedIntKeyHashMap<ExpressionColumn> parameters =
            new OrderedIntKeyHashMap<>();
        private final HsqlArrayList<SchemaObject> usedSequences =
            new HsqlArrayList<>(
                16,
                true);
        private final HsqlArrayList<FunctionSQLInvoked> usedRoutines =
            new HsqlArrayList<>(
                16,
                true);
        private final OrderedIntKeyHashMap<RangeVariable> rangeVariables =
            new OrderedIntKeyHashMap<>();
        private final HsqlArrayList<SchemaObject> usedTypes =
            new HsqlArrayList<>(
                16,
                true);
        Type    currentDomain;
        boolean contextuallyTypedExpression;
        boolean onDuplicateTypedExpression;
        Routine callProcedure;

        //
        private RangeGroup[] outerRangeGroups = RangeGroup.emptyArray;

        //
        private final int initialRangeVarIndex;
        private int       rangeVarIndex;

        public CompileContext(Session session) {
            this(session, null, null);
        }

        public CompileContext(
                Session session,
                ParserBase parser,
                CompileContext baseContext) {

            this.session     = session;
            this.parser      = parser;
            this.baseContext = baseContext;

            if (baseContext == null) {
                initialRangeVarIndex = rangeVarIndex = 1;
                basePosition         = 0;
            } else {
                initialRangeVarIndex = rangeVarIndex =
                    baseContext.getRangeVarCount();
                basePosition  = baseContext.parser.getPosition();
                subqueryDepth = baseContext.getDepth();
            }
        }

        public void reset() {

            rangeVarIndex = initialRangeVarIndex;
            subqueryDepth = 0;

            rangeVariables.clear();
            parameters.clear();
            usedSequences.clear();
            usedRoutines.clear();

            callProcedure = null;

            usedTypes.clear();

            outerRangeGroups = RangeGroup.emptyArray;

            //
            currentDomain               = null;
            contextuallyTypedExpression = false;
        }

        public int getDepth() {
            return subqueryDepth;
        }

        public void incrementDepth() {

            subqueryDepth++;

            if (baseContext != null) {
                baseContext.subqueryDepth++;
            }
        }

        public void decrementDepth() {

            clearSubqueries();

            subqueryDepth--;

            if (baseContext != null) {
                baseContext.subqueryDepth--;
            }
        }

        public void decrementDepth(int toDepth) {
            while (subqueryDepth > toDepth) {
                decrementDepth();
            }
        }

        public void rewind(int position) {

            if (baseContext != null) {
                baseContext.rewind(basePosition + position);

                return;
            }

            rewindRangeVariables(position);
            rewindParameters(position);
        }

        private void rewindRangeVariables(int position) {

            for (int i = rangeVariables.size() - 1; i >= 0; i--) {
                int rangePos = rangeVariables.getKeyAt(i, -1);

                if (rangePos > position) {
                    rangeVariables.removeEntry(i);
                }
            }

            if (rangeVariables.size() > 0) {
                RangeVariable range = (RangeVariable) rangeVariables.getValueAt(
                    rangeVariables.size() - 1);

                rangeVarIndex = range.rangePosition + 1;
            } else {
                rangeVarIndex = initialRangeVarIndex;
            }
        }

        private void rewindParameters(int position) {

            if (baseContext != null) {
                baseContext.rewindParameters(basePosition + position);

                return;
            }

            Iterator<Integer> it = parameters.keySet().iterator();

            while (it.hasNext()) {
                int pos = it.nextInt();

                if (pos >= position) {
                    it.remove();
                }
            }
        }

        public void setCurrentSubquery(HsqlName name) {
            isViewTable = name.type == SchemaObject.VIEW;
        }

        public void registerRangeVariable(RangeVariable range) {

            int nextRangePosition = basePosition;

            if (parser != null) {
                nextRangePosition += parser.getPosition();
            }

            if (isViewTable) {
                range.isViewSubquery = true;
            }

            registerRangeVariable(range, nextRangePosition);
        }

        private void registerRangeVariable(RangeVariable range, int position) {

            if (baseContext != null) {
                baseContext.registerRangeVariable(range, position);

                return;
            }

            range.rangePosition = getNextRangeVarIndex();
            range.level         = subqueryDepth;

            rangeVariables.put(position, range);
        }

        public void setNextRangeVarIndex(int n) {

            if (baseContext != null) {
                baseContext.setNextRangeVarIndex(n);

                return;
            }

            rangeVarIndex = n;
        }

        private int getNextRangeVarIndex() {

            if (baseContext != null) {
                return baseContext.getNextRangeVarIndex();
            }

            return rangeVarIndex++;
        }

        public int getNextResultRangeVarIndex() {

            RangeVariable range = new RangeVariable(
                null,
                null,
                false,
                RangeVariable.PLACEHOLDER_RANGE);

            registerRangeVariable(range);

            return range.rangePosition;
        }

        public int getRangeVarCount() {

            if (baseContext != null) {
                return baseContext.getRangeVarCount();
            }

            return rangeVarIndex;
        }

        public RangeVariable[] getAllRangeVariables() {

            HsqlArrayList<RangeVariable> list = new HsqlArrayList<>();

            for (int i = 0; i < rangeVariables.size(); i++) {
                RangeVariable range = (RangeVariable) rangeVariables.getValueAt(
                    i);

                if (range.rangeType != RangeVariable.PLACEHOLDER_RANGE) {
                    list.add(range);
                }
            }

            RangeVariable[] array = new RangeVariable[list.size()];

            list.toArray(array);

            return array;
        }

        public RangeGroup[] getOuterRanges() {

            if (baseContext != null) {
                return baseContext.outerRangeGroups;
            }

            return outerRangeGroups;
        }

        public void setOuterRanges(RangeGroup[] rangeGroups) {
            outerRangeGroups = rangeGroups;
        }

        public NumberSequence[] getSequences() {

            if (usedSequences.isEmpty()) {
                return NumberSequence.emptyArray;
            }

            NumberSequence[] array = new NumberSequence[usedSequences.size()];

            usedSequences.toArray(array);

            return array;
        }

        public Routine[] getRoutines() {

            if (callProcedure == null && usedRoutines.isEmpty()) {
                return Routine.emptyArray;
            }

            OrderedHashSet<Routine> set = new OrderedHashSet<>();

            for (int i = 0; i < usedRoutines.size(); i++) {
                FunctionSQLInvoked function = usedRoutines.get(i);

                if (function.routine != null) {
                    set.add(function.routine);
                }
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
                namedSubqueries = new HsqlArrayList<>();
            }

            if (namedSubqueries.size() <= subqueryDepth) {
                namedSubqueries.setSize(subqueryDepth + 1);
            }

            OrderedHashMap<String, TableDerived> set = namedSubqueries.get(
                subqueryDepth);

            if (set == null) {
                set = new OrderedHashMap<>();

                namedSubqueries.set(subqueryDepth, set);
            }
        }

        private void clearSubqueries() {

            if (namedSubqueries != null) {
                if (namedSubqueries.size() > subqueryDepth) {
                    OrderedHashMap<String, TableDerived> set =
                        namedSubqueries.get(
                            subqueryDepth);

                    if (set != null) {
                        set.clear();
                    }
                }
            }
        }

        private void registerSubquery(String name) {

            initSubqueryNames();

            OrderedHashMap<String, TableDerived> set = namedSubqueries.get(
                subqueryDepth);
            boolean added = set.add(name, null);

            if (!added) {
                throw Error.error(ErrorCode.X_42504, name);
            }
        }

        private void registerSubquery(String name, TableDerived td) {

            OrderedHashMap<String, TableDerived> set = namedSubqueries.get(
                subqueryDepth);

            set.put(name, td);
        }

        private void unregisterSubqueries() {

            if (namedSubqueries == null) {
                return;
            }

            for (int i = subqueryDepth; i < namedSubqueries.size(); i++) {
                OrderedHashMap<String, TableDerived> set = namedSubqueries.get(
                    i);

                if (set != null) {
                    set.clear();
                }
            }
        }

        private TableDerived getNamedSubQuery(String name) {

            if (baseContext != null) {
                TableDerived td = baseContext.getNamedSubQuery(name);

                if (td != null) {
                    return td;
                }
            }

            if (namedSubqueries == null) {
                return null;
            }

            for (int i = subqueryDepth; i >= 0; i--) {
                if (namedSubqueries.size() <= i) {
                    continue;
                }

                OrderedHashMap<String, TableDerived> set = namedSubqueries.get(
                    i);

                if (set == null) {
                    continue;
                }

                if (set.getIndex(name) >= 0) {
                    TableDerived td = set.get(name);

                    if (td == null) {
                        HsqlException ex = Error.error(ErrorCode.X_42501, name);

                        ex.setToken(name);

                        throw ex;
                    }

                    return td;
                }
            }

            return null;
        }

        private void addParameter(ExpressionColumn e, int position) {
            parameters.put(position, e);
        }

        private void addDomainOrType(SchemaObject object) {
            usedTypes.add(object);
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

            if (parameters.isEmpty()) {
                return ExpressionColumn.emptyArray;
            }

            ExpressionColumn[] result = new ExpressionColumn[parameters.size()];

            parameters.valuesToArray(result);
            parameters.clear();

            return result;
        }

        public OrderedHashSet<HsqlName> getSchemaObjectNames() {

            OrderedHashSet<HsqlName> set = new OrderedHashSet<>();

            for (int i = 0; i < usedSequences.size(); i++) {
                SchemaObject object = usedSequences.get(i);

                set.add(object.getName());
            }

            for (int i = 0; i < usedTypes.size(); i++) {
                SchemaObject object = usedTypes.get(i);

                set.add(object.getName());
            }

            for (int i = 0; i < rangeVariables.size(); i++) {
                RangeVariable range = (RangeVariable) rangeVariables.getValueAt(
                    i);

                if (range.isViewSubquery) {
                    continue;
                }

                if (range.rangeType == RangeVariable.PLACEHOLDER_RANGE) {
                    continue;
                }

                HsqlName name = range.rangeTable.getName();

                if (name.type == SchemaObject.TRANSITION) {
                    set.addAll(range.getColumnNames());
                    continue;
                }

                if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                    continue;
                }

                set.add(name);
                set.addAll(range.getColumnNames());

                if (range.periodCondition != null) {
                    if (range.periodCondition.isSystemVersionCondition()) {
                        set.add(range.rangeTable.systemPeriod.getName());
                    }
                }
            }

            Routine[] routines = getRoutines();

            for (int i = 0; i < routines.length; i++) {
                set.add(routines[i].getSpecificName());
            }

            return set;
        }
    }

    private Expression readStringAggFunctionOrNull() {

        Expression   expr;
        SortAndSlice order         = null;
        int          position      = getPosition();
        boolean      distinct      = false;
        String       separator     = null;
        int          maxCount      = 0;
        boolean      overflowError = false;
        boolean      overflowTrunc = false;
        String       filler        = null;
        boolean      withCount     = false;

        read();

        if (token.tokenType != Tokens.OPENBRACKET) {
            rewind(position);

            return null;
        }

        readThis(Tokens.OPENBRACKET);

        if (readIfThis(Tokens.DISTINCT)) {
            distinct = true;
        } else {
            readIfThis(Tokens.ALL);
        }

        Expression e = XreadValueExpression();

        if (token.tokenType == Tokens.COMMA) {
            read();
            checkIsQuotedString();

            separator = (String) token.tokenValue;

            read();
        }

        if (token.tokenType == Tokens.ORDER) {
            read();
            readThis(Tokens.BY);

            order = XreadOrderBy();
        }

        readThis(Tokens.CLOSEBRACKET);

        if (order == null && token.tokenType == Tokens.WITHIN) {
            order = XreadWithinGroup();
        }

        expr = new ExpressionArrayAggregate(
            OpTypes.LISTAGG,
            distinct,
            e,
            order,
            separator,
            filler,
            overflowTrunc,
            overflowError,
            withCount,
            maxCount);

        readFilterClause(expr);

        return expr;
    }

    private Expression readListAggFunctionOrNull() {

        Expression   expr;
        SortAndSlice order;
        int          position      = getPosition();
        boolean      distinct      = false;
        String       separator     = null;
        int          maxCount      = 0;
        boolean      overflowError = false;
        boolean      overflowTrunc = false;
        String       filler        = null;
        boolean      withCount     = false;

        read();

        if (token.tokenType != Tokens.OPENBRACKET) {
            rewind(position);

            return null;
        }

        readThis(Tokens.OPENBRACKET);

        if (readIfThis(Tokens.DISTINCT)) {
            distinct = true;
        } else {
            readIfThis(Tokens.ALL);
        }

        Expression e = XreadValueExpression();

        if (token.tokenType == Tokens.COMMA) {
            read();
            checkIsQuotedString();

            separator = (String) token.tokenValue;

            read();
        }

        if (readIfThis(Tokens.FETCH)) {
            maxCount = readInteger();

            if (readIfThis(Tokens.ROWS)) {
                readIfThis(Tokens.ONLY);
            }
        }

        if (readIfThis(Tokens.ON)) {
            readThis(Tokens.OVERFLOW);

            if (readIfThis(Tokens.TRUNCATE)) {
                overflowTrunc = true;

                if (isQuotedString()) {
                    filler = readQuotedString();
                }

                if (readIfThis(Tokens.WITH)) {
                    withCount = true;

                    readThis(Tokens.COUNT);
                } else if (readIfThis(Tokens.WITHOUT)) {
                    readThis(Tokens.COUNT);
                }
            } else {
                overflowError = true;

                readThis(Tokens.ERROR);
            }
        }

        readThis(Tokens.CLOSEBRACKET);

        order = XreadWithinGroup();
        expr = new ExpressionArrayAggregate(
            OpTypes.LISTAGG,
            distinct,
            e,
            order,
            separator,
            filler,
            overflowTrunc,
            overflowError,
            withCount,
            maxCount);

        readFilterClause(expr);

        return expr;
    }

    private SortAndSlice XreadWithinGroup() {

        readThis(Tokens.WITHIN);
        readThis(Tokens.GROUP);
        readThis(Tokens.OPENBRACKET);
        readThis(Tokens.ORDER);
        readThis(Tokens.BY);

        SortAndSlice order = XreadOrderBy();

        readThis(Tokens.CLOSEBRACKET);

        return order;
    }
}
