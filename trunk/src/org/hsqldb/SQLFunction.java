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

import org.hsqldb.lib.IntValueHashMap;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BinaryType;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.CharacterType;
import org.hsqldb.types.DateTimeIntervalType;
import org.hsqldb.types.NumberType;
import org.hsqldb.types.Type;

/**
 * Implementation of SQL standard functions.<p>
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class SQLFunction extends Expression {

    private final static int   FUNC_POSITION_CHAR                    = 1;     // numeric
    private final static int   FUNC_POSITION_BINARY                  = 2;
    private final static int   FUNC_OCCURENCES_REGEX                 = 3;
    private final static int   FUNC_POSITION_REGEX                   = 4;
    protected final static int FUNC_EXTRACT                          = 5;
    private final static int   FUNC_CHAR_LENGTH                      = 6;
    private final static int   FUNC_OCTET_LENGTH                     = 7;
    private final static int   FUNC_CARDINALITY                      = 8;
    private final static int   FUNC_ABS                              = 9;
    private final static int   FUNC_MOD                              = 10;
    private final static int   FUNC_LN                               = 11;
    private final static int   FUNC_EXP                              = 12;
    private final static int   FUNC_POWER                            = 13;
    private final static int   FUNC_SQRT                             = 14;
    private final static int   FUNC_FLOOR                            = 15;
    private final static int   FUNC_CEILING                          = 16;
    private final static int   FUNC_WIDTH_BUCKET                     = 17;
    private final static int   FUNC_SUBSTRING_CHAR                   = 20;    // string
    private final static int   FUNC_SUBSTRING_REG_EXPR               = 21;
    private final static int   FUNC_SUBSTRING_REGEX                  = 22;
    private final static int   FUNC_FOLD_LOWER                       = 23;
    private final static int   FUNC_FOLD_UPPER                       = 24;
    private final static int   FUNC_TRANSCODING                      = 25;
    private final static int   FUNC_TRANSLITERATION                  = 26;
    private final static int   FUNC_REGEX_TRANSLITERATION            = 27;
    private final static int   FUNC_TRIM_CHAR                        = 28;
    private final static int   FUNC_OVERLAY_CHAR                     = 29;
    private final static int   FUNC_CHAR_NORMALIZE                   = 30;
    private final static int   FUNC_SUBSTRING_BINARY                 = 31;
    private final static int   FUNC_TRIM_BINARY                      = 32;
    private final static int   FUNC_OVERLAY_BINARY                   = 33;
    protected final static int FUNC_CURRENT_DATE                     = 40;    // datetime
    private final static int   FUNC_CURRENT_TIME                     = 41;
    protected final static int FUNC_CURRENT_TIMESTAMP                = 42;
    private final static int   FUNC_LOCALTIME                        = 43;
    private final static int   FUNC_LOCALTIMESTAMP                   = 44;
    private final static int   FUNC_CURRENT_CATALOG                  = 50;    // general
    private final static int   FUNC_CURRENT_DEFAULT_TRANSFORM_GROUP  = 51;
    private final static int   FUNC_CURRENT_PATH                     = 52;
    private final static int   FUNC_CURRENT_ROLE                     = 53;
    private final static int   FUNC_CURRENT_SCHEMA                   = 54;
    private final static int   FUNC_CURRENT_TRANSFORM_GROUP_FOR_TYPE = 55;
    private final static int   FUNC_CURRENT_USER                     = 56;
    private final static int   FUNC_SESSION_USER                     = 57;
    private final static int   FUNC_SYSTEM_USER                      = 58;
    protected final static int FUNC_USER                             = 59;
    private final static int   FUNC_VALUE                            = 60;

    //
    static final Expression[] emptyArgList    = new Expression[0];
    static final short[]      noParamList     = new short[]{};
    static final short[]      emptyParamList  = new short[] {
        Token.OPENBRACKET, Token.CLOSEBRACKET
    };
    static final short[]      singleParamList = new short[] {
        Token.OPENBRACKET, Token.QUESTION, Token.CLOSEBRACKET
    };
    static final short[]      doubleParamList = new short[] {
        Token.OPENBRACKET, Token.QUESTION, Token.COMMA, Token.QUESTION,
        Token.CLOSEBRACKET
    };

    //
    static IntValueHashMap valueFuncMap   = new IntValueHashMap();
    static IntValueHashMap regularFuncMap = new IntValueHashMap();

    static {
        regularFuncMap.put(Token.T_POSITION, FUNC_POSITION_CHAR);
        /*
        regularFuncMap.put(Token.T_OCCURENCES_REGEX, FUNC_OCCURENCES_REGEX);
        */
        regularFuncMap.put(Token.T_POSITION_REGEX, FUNC_POSITION_REGEX);
        regularFuncMap.put(Token.T_EXTRACT, FUNC_EXTRACT);
        regularFuncMap.put(Token.T_CHAR_LENGTH, FUNC_CHAR_LENGTH);
        regularFuncMap.put(Token.T_CHARACTER_LENGTH, FUNC_CHAR_LENGTH);
        regularFuncMap.put(Token.T_OCTET_LENGTH, FUNC_OCTET_LENGTH);
        /*
        regularFuncMap.put(Token.T_CARDINALITY, FUNC_CARDINALITY);
        */
        regularFuncMap.put(Token.T_ABS, FUNC_ABS);
        regularFuncMap.put(Token.T_MOD, FUNC_MOD);
        regularFuncMap.put(Token.T_LN, FUNC_LN);
        regularFuncMap.put(Token.T_EXP, FUNC_EXP);
        regularFuncMap.put(Token.T_POWER, FUNC_POWER);
        regularFuncMap.put(Token.T_SQRT, FUNC_SQRT);
        regularFuncMap.put(Token.T_FLOOR, FUNC_FLOOR);
        regularFuncMap.put(Token.T_CEILING, FUNC_CEILING);
        regularFuncMap.put(Token.T_CEIL, FUNC_CEILING);
        regularFuncMap.put(Token.T_WIDTH_BUCKET, FUNC_WIDTH_BUCKET);
        regularFuncMap.put(Token.T_SUBSTRING, FUNC_SUBSTRING_CHAR);
        /*
        regularFuncMap.put(Token.T_SUBSTRING_REG_EXPR,
                           FUNC_SUBSTRING_REG_EXPR);
        */
        regularFuncMap.put(Token.T_SUBSTRING_REGEX, FUNC_SUBSTRING_REGEX);
        regularFuncMap.put(Token.T_LOWER, FUNC_FOLD_LOWER);
        regularFuncMap.put(Token.T_UPPER, FUNC_FOLD_UPPER);
        /*
        regularFuncMap.put(Token.T_TRANSCODING, FUNC_TRANSCODING);
        regularFuncMap.put(Token.T_TRANSLITERATION, FUNC_TRANSLITERATION);
        regularFuncMap.put(Token.T_REGEX_TRANSLITERATION,
                           FUNC_REGEX_TRANSLITERATION);
        */
        regularFuncMap.put(Token.T_TRIM, FUNC_TRIM_CHAR);
        regularFuncMap.put(Token.T_OVERLAY, FUNC_OVERLAY_CHAR);
        /*
        regularFuncMap.put(Token.T_CHAR_NORMALIZE, FUNC_CHAR_NORMALIZE);
        */
        regularFuncMap.put(Token.T_TRIM, FUNC_TRIM_BINARY);
    }

    static {
        valueFuncMap.put(Token.T_CURRENT_DATE, FUNC_CURRENT_DATE);
        valueFuncMap.put(Token.T_CURRENT_TIME, FUNC_CURRENT_TIME);
        valueFuncMap.put(Token.T_CURRENT_TIMESTAMP, FUNC_CURRENT_TIMESTAMP);
        valueFuncMap.put(Token.T_LOCALTIME, FUNC_LOCALTIME);
        valueFuncMap.put(Token.T_LOCALTIMESTAMP, FUNC_LOCALTIMESTAMP);
        valueFuncMap.put(Token.T_CURRENT_CATALOG, FUNC_CURRENT_CATALOG);
        /*
        valueFuncMap.put(Token.T_CURRENT_DEFAULT_TRANSFORM_GROUP,
                FUNC_CURRENT_DEFAULT_TRANSFORM_GROUP);
        */
        valueFuncMap.put(Token.T_CURRENT_PATH, FUNC_CURRENT_PATH);
        valueFuncMap.put(Token.T_CURRENT_ROLE, FUNC_CURRENT_ROLE);
        valueFuncMap.put(Token.T_CURRENT_SCHEMA, FUNC_CURRENT_SCHEMA);
        /*
        valueFuncMap.put(Token.T_CURRENT_TRANSFORM_GROUP_FOR_TYPE,
                FUNC_CURRENT_TRANSFORM_GROUP_FOR_TYPE);
        */
        valueFuncMap.put(Token.T_CURRENT_USER, FUNC_CURRENT_USER);
        valueFuncMap.put(Token.T_SESSION_USER, FUNC_SESSION_USER);
        valueFuncMap.put(Token.T_SYSTEM_USER, FUNC_SYSTEM_USER);
        valueFuncMap.put(Token.T_USER, FUNC_USER);
        /*
        only for domain constraints - probably shouldn't be handled here at all
        valueFuncMap.put(Token.T_VALUE, FUNC_VALUE);
        */
    }

    //
    int     id;
    String  name;
    short[] parseList;
    boolean isValueFunction;

    public static SQLFunction newSQLFunction(String token) {

        int id = regularFuncMap.get(token, -1);

        if (id == -1) {
            id = valueFuncMap.get(token, -1);
        }

        if (id == -1) {
            return null;
        }

        SQLFunction function = new SQLFunction(id);

        return function;
    }

    public static boolean isFunction(String token) {
        return isRegularFunction(token) || isValueFunction(token);
    }

    public static boolean isRegularFunction(String token) {
        return regularFuncMap.containsKey(token);
    }

    public static boolean isValueFunction(String token) {
        return valueFuncMap.containsKey(token);
    }

    protected SQLFunction() {

        super(Expression.SQL_FUNCTION);

        argList = emptyArgList;
    }

    protected SQLFunction(int id) {

        this();

        this.id = id;

        switch (id) {

            case FUNC_POSITION_CHAR :
            case FUNC_POSITION_BINARY :
                name      = Token.T_POSITION;
                parseList = new short[] {
                    Token.OPENBRACKET, Token.QUESTION, Token.IN,
                    Token.QUESTION, Token.X_OPTION, 5, Token.USING,
                    Token.X_KEYSET, 2, Token.CHARACTERS, Token.OCTETS,
                    Token.CLOSEBRACKET
                };
                break;

            case FUNC_OCCURENCES_REGEX :
            case FUNC_POSITION_REGEX :
                break;

            case FUNC_EXTRACT :
                name      = Token.T_EXTRACT;
                parseList = new short[] {
                    Token.OPENBRACKET, Token.X_KEYSET, 8, Token.YEAR,
                    Token.MONTH, Token.DAY, Token.HOUR, Token.MINUTE,
                    Token.SECOND, Token.TIMEZONE_HOUR, Token.TIMEZONE_MINUTE,
                    Token.FROM, Token.QUESTION, Token.CLOSEBRACKET
                };
                break;

            case FUNC_CHAR_LENGTH :
                name      = Token.T_CHAR_LENGTH;
                parseList = new short[] {
                    Token.OPENBRACKET, Token.QUESTION, Token.X_OPTION, 5,
                    Token.USING, Token.X_KEYSET, 2, Token.CHARACTERS,
                    Token.OCTETS, Token.CLOSEBRACKET
                };
                break;

            case FUNC_OCTET_LENGTH :
                name      = Token.T_OCTET_LENGTH;
                parseList = singleParamList;
                break;

            case FUNC_CARDINALITY :
                parseList = singleParamList;
                break;

            case FUNC_ABS :
                name      = Token.T_ABS;
                parseList = singleParamList;
                break;

            case FUNC_MOD :
                name      = Token.T_MOD;
                parseList = singleParamList;
                break;

            case FUNC_LN :
                name      = Token.T_LN;
                parseList = singleParamList;
                break;

            case FUNC_EXP :
                name      = Token.T_EXP;
                parseList = singleParamList;
                break;

            case FUNC_POWER :
                name      = Token.T_POWER;
                parseList = doubleParamList;
                break;

            case FUNC_SQRT :
                name      = Token.T_SQRT;
                parseList = singleParamList;
                break;

            case FUNC_FLOOR :
                name      = Token.T_FLOOR;
                parseList = singleParamList;
                break;

            case FUNC_CEILING :
                name      = Token.T_CEILING;
                parseList = singleParamList;
                break;

            case FUNC_WIDTH_BUCKET :
                name      = Token.T_WIDTH_BUCKET;
                parseList = new short[] {
                    Token.OPENBRACKET, Token.QUESTION, Token.COMMA,
                    Token.QUESTION, Token.COMMA, Token.QUESTION, Token.COMMA,
                    Token.QUESTION, Token.CLOSEBRACKET
                };
                break;

            case FUNC_SUBSTRING_CHAR :
            case FUNC_SUBSTRING_BINARY :
                name      = Token.T_SUBSTRING;
                parseList = new short[] {
                    Token.OPENBRACKET, Token.QUESTION, Token.FROM,
                    Token.QUESTION, Token.X_OPTION, 2, Token.FOR,
                    Token.QUESTION, Token.X_OPTION, 5, Token.USING,
                    Token.X_KEYSET, 2, Token.CHARACTERS, Token.OCTETS,
                    Token.CLOSEBRACKET
                };
                break;

            /*
            case FUNCTION_SUBSTRING_REG_EXPR :
                break;
            case FUNCTION_SUBSTRING_REGEX :
                break;
            */
            case FUNC_FOLD_LOWER :
                name      = Token.T_LOWER;
                parseList = singleParamList;
                break;

            case FUNC_FOLD_UPPER :
                name      = Token.T_UPPER;
                parseList = singleParamList;
                break;

            /*
            case FUNCTION_TRANSCODING :
                break;
            case FUNCTION_TRANSLITERATION :
                break;
            case FUNCTION_REGEX_TRANSLITERATION :
                break;
             */
            case FUNC_TRIM_CHAR :
            case FUNC_TRIM_BINARY :
                name      = Token.T_TRIM;
                parseList = new short[] {
                    Token.OPENBRACKET, Token.X_OPTION, 11,    //
                    Token.X_OPTION, 5,                        //
                    Token.X_KEYSET, 3, Token.LEADING, Token.TRAILING,
                    Token.BOTH,                               //
                    Token.X_OPTION, 1, Token.QUESTION,        //
                    Token.FROM, Token.QUESTION, Token.CLOSEBRACKET
                };
                break;

            /*
            case FUNCTION_CHAR_NORMALIZE :
                break;
            */
            case FUNC_OVERLAY_CHAR :
            case FUNC_OVERLAY_BINARY :
                name      = Token.T_OVERLAY;
                parseList = new short[] {
                    Token.OPENBRACKET, Token.QUESTION, Token.PLACING,
                    Token.QUESTION, Token.FROM, Token.QUESTION, Token.X_OPTION,
                    2, Token.FOR, Token.QUESTION, Token.X_OPTION, 2,
                    Token.USING, Token.X_KEYSET, Token.CLOSEBRACKET
                };
                break;

            case FUNC_CURRENT_CATALOG :
                name            = Token.T_CURRENT_CATALOG;
                parseList       = noParamList;
                isValueFunction = true;
                break;

            /*
            case FUNC_CURRENT_DEFAULT_TRANSFORM_GROUP :
                break;
            case FUNC_CURRENT_PATH :
                break;
            */
            case FUNC_CURRENT_ROLE :
                name            = Token.T_CURRENT_ROLE;
                parseList       = noParamList;
                isValueFunction = true;
                break;

            case FUNC_CURRENT_SCHEMA :
                name            = Token.T_CURRENT_SCHEMA;
                parseList       = noParamList;
                isValueFunction = true;
                break;

            /*
            case FUNC_CURRENT_TRANSFORM_GROUP_FOR_TYPE :
                break;
            */
            case FUNC_CURRENT_USER :
                name            = Token.T_CURRENT_USER;
                parseList       = noParamList;
                isValueFunction = true;
                break;

            case FUNC_SESSION_USER :
                name            = Token.T_SESSION_USER;
                parseList       = noParamList;
                isValueFunction = true;
                break;

            case FUNC_SYSTEM_USER :
                name            = Token.T_SYSTEM_USER;
                parseList       = noParamList;
                isValueFunction = true;
                break;

            case FUNC_USER :
                name            = Token.T_USER;
                parseList       = new short[] {
                    Token.X_OPTION, 2, Token.OPENBRACKET, Token.CLOSEBRACKET
                };
                isValueFunction = true;
                break;

            /*
            case FUNC_VALUE :
                break;
            */
            case FUNC_CURRENT_DATE :
                name            = Token.T_CURRENT_DATE;
                parseList       = noParamList;
                isValueFunction = true;
                break;

            case FUNC_CURRENT_TIME :
                name            = Token.T_CURRENT_TIME;
                parseList       = noParamList;
                isValueFunction = true;
                break;

            case FUNC_CURRENT_TIMESTAMP :
                name            = Token.T_CURRENT_TIMESTAMP;
                parseList       = noParamList;
                isValueFunction = true;
                break;

            case FUNC_LOCALTIME :
                name            = Token.T_LOCALTIME;
                parseList       = noParamList;
                isValueFunction = true;
                break;

            case FUNC_LOCALTIMESTAMP :
                name            = Token.T_LOCALTIMESTAMP;
                parseList       = noParamList;
                isValueFunction = true;
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "SQLFunction");
        }
    }

    public void setArguments(Expression[] argList) {

        this.argList = argList;

        for (int i = 0; i < argList.length; i++) {
            Expression e = argList[i];

            if (e != null && e.isAggregate()) {
                aggregateSpec = AGGREGATE_SELF;
            }
        }
    }

    /**
     * Evaluates and returns this Function in the context of the session.<p>
     */
    public Object getValue(Session session) throws HsqlException {

        Object[] data = new Object[argList.length];

        for (int i = 0; i < argList.length; i++) {
            Expression e = argList[i];

            if (e != null) {
                data[i] = e.getValue(session, e.dataType);
            }
        }

        return getValue(session, data);
    }

    Object getValue(Session session, Object[] data) throws HsqlException {

        switch (id) {

            case FUNC_POSITION_CHAR : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                long result =
                    ((CharacterType) argList[1].dataType).position(
                        data[1], data[0], argList[0].dataType, 0) + 1;

                if (argList[2] != null
                        && ((Number) argList[2].valueData).intValue()
                           == Token.OCTETS) {
                    result *= 2;
                }

                return ValuePool.getLong(result);
            }
            case FUNC_POSITION_BINARY : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                long result =
                    ((BinaryType) argList[1].dataType).position(
                        (BlobData) data[1], (BlobData) data[0],
                        argList[0].dataType, 0) + 1;

                if (argList[2] != null
                        && ((Number) argList[2].valueData).intValue()
                           == Token.OCTETS) {
                    result *= 2;
                }

                return ValuePool.getLong(result);
            }
            /*
            case FUNC_OCCURENCES_REGEX :
            case FUNC_POSITION_REGEX :
            */
            case FUNC_EXTRACT : {
                if (data[1] == null) {
                    return null;
                }

                int part = ((Number) argList[0].valueData).intValue();

                part = DateTimeIntervalType.getFieldNameTypeForToken(part);

                if (part == Types.SQL_INTERVAL_SECOND) {
                    return ((DateTimeIntervalType) argList[1].dataType)
                        .getSecondPart(data[1]);
                } else {
                    int value =
                        ((DateTimeIntervalType) argList[1].dataType).getPart(
                            data[1], part);

                    return ValuePool.getInt(value);
                }
            }
            case FUNC_CHAR_LENGTH : {
                if (data[0] == null) {
                    return null;
                }

                long result =
                    ((CharacterType) argList[0].dataType).size(data[0]);

                return ValuePool.getLong(result);
            }
            case FUNC_OCTET_LENGTH : {
                if (data[0] == null) {
                    return null;
                }

                long result;

                if (argList[0].dataType.isBinaryType()) {
                    result = ((BinaryType) argList[0].dataType).size(data[0]);
                } else {
                    result = 2 * ((CharacterType) argList[0].dataType).size(
                        data[0]);
                }

                return ValuePool.getLong(result);
            }
            /*
            case FUNC_CARDINALITY :
            */
            case FUNC_ABS : {
                if (data[0] == null) {
                    return null;
                }

                return ((NumberType) dataType).absolute(data[0]);
            }
            case FUNC_MOD : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                // non-integral arguments are accepted with conversion
                // todo - check if widening has an effect
                Object value =
                    ((NumberType) argList[0].dataType).divide(argList[0],
                        argList[1]);

                value = ((NumberType) argList[0].dataType).subtract(argList[0],
                        value);

                // result type is the same as argList[1]
                return ((NumberType) dataType).convertToTypeLimits(value);
            }
            case FUNC_LN : {
                if (data[0] == null) {
                    return null;
                }

                double val = Math.log(((Number) data[0]).doubleValue());

                return ValuePool.getDouble(Double.doubleToLongBits(val));
            }
            case FUNC_EXP : {
                if (data[0] == null) {
                    return null;
                }

                double val = Math.exp(((Number) data[0]).doubleValue());

                return ValuePool.getDouble(Double.doubleToLongBits(val));
            }
            case FUNC_POWER : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                double val = Math.pow(((Number) data[0]).doubleValue(),
                                      ((Number) data[1]).doubleValue());

                return ValuePool.getDouble(Double.doubleToLongBits(val));
            }
            case FUNC_SQRT : {
                if (data[0] == null) {
                    return null;
                }

                double val = Math.sqrt(((Number) data[0]).doubleValue());

                return ValuePool.getDouble(Double.doubleToLongBits(val));
            }
            case FUNC_FLOOR : {
                if (data[0] == null) {
                    return null;
                }

                return ((NumberType) dataType).floor(data[0]);
            }
            case FUNC_CEILING : {
                if (data[0] == null) {
                    return null;
                }

                return ((NumberType) dataType).ceiling(data[0]);
            }
            case FUNC_WIDTH_BUCKET : {
                return null;
            }
            case FUNC_SUBSTRING_CHAR : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                Object value;

                value = Type.SQL_BIGINT.convertToType(session, data[1],
                                                      argList[1].dataType);

                long offset = ((Number) value).longValue() - 1;
                long length = 0;

                if (argList[2] != null) {
                    if (data[2] == null) {
                        return null;
                    }

                    value = Type.SQL_BIGINT.convertToType(session, data[2],
                                                          argList[2].dataType);
                    length = ((Number) value).longValue();
                }

                if (argList[3] != null
                        && ((Number) argList[2].valueData).intValue()
                           == Token.OCTETS) {

                    // not clear what the rules on USING OCTECTS are
                }

                return ((CharacterType) dataType).substring(session, data[0],
                        offset, length, argList[2] != null);
            }
            /*
            case FUNCTION_SUBSTRING_REG_EXPR :
                break;
            case FUNCTION_SUBSTRING_REGEX :
                break;
            */
            case FUNC_FOLD_LOWER :
                if (data[0] == null) {
                    return null;
                }

                return ((CharacterType) dataType).lower(session, data[0]);

            case FUNC_FOLD_UPPER :
                if (data[0] == null) {
                    return null;
                }

                return ((CharacterType) dataType).upper(session, data[0]);

            /*
            case FUNCTION_TRANSCODING :
                break;
            case FUNCTION_TRANSLITERATION :
                break;
            case FUNCTION_REGEX_TRANSLITERATION :
                break;
             */
            case FUNC_TRIM_CHAR : {
                if (data[1] == null || data[2] == null) {
                    return null;
                }

                boolean leading  = false;
                boolean trailing = false;

                switch (((Number) argList[0].valueData).intValue()) {

                    case Token.BOTH :
                        leading = trailing = true;
                        break;

                    case Token.LEADING :
                        leading = true;
                        break;

                    case Token.TRAILING :
                        trailing = true;
                        break;

                    default :
                        throw Trace.runtimeError(
                            Trace.UNSUPPORTED_INTERNAL_OPERATION,
                            "SQLFunction");
                }

                String string = (String) data[1];

                if (string.length() != 1) {
                    throw Trace.error(Trace.SQL_DATA_TRIM_ERROR);
                }

                int character = string.charAt(0);

                return ((CharacterType) dataType).trim(session, data[2],
                                                       character, leading,
                                                       trailing);
            }
            case FUNC_OVERLAY_CHAR : {
                if (data[0] == null || data[1] == null || data[2] == null) {
                    return null;
                }

                Object value;

                value = Type.SQL_BIGINT.convertToType(session, data[2],
                                                      argList[2].dataType);

                long offset = ((Number) value).longValue() - 1;
                long length = 0;

                if (argList[3] != null) {
                    if (data[3] == null) {
                        return null;
                    }

                    value = Type.SQL_BIGINT.convertToType(session, data[3],
                                                          argList[3].dataType);
                    length = ((Number) value).longValue();
                }

                return ((CharacterType) dataType).overlay(null, data[0],
                        data[1], offset, length, argList[3] != null);
            }
            /*
            case FUNCTION_CHAR_NORMALIZE :
                break;
            */
            case FUNC_SUBSTRING_BINARY : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                Object value;

                value = Type.SQL_BIGINT.convertToType(session, data[1],
                                                      argList[1].dataType);

                long offset = ((Number) value).longValue() - 1;
                long length = 0;

                if (argList[2] != null) {
                    if (data[2] == null) {
                        return null;
                    }

                    value = Type.SQL_BIGINT.convertToType(session, data[2],
                                                          argList[2].dataType);
                    length = ((Number) value).intValue();
                }

                return ((BinaryType) dataType).substring((BlobData) data[0],
                        offset, length, argList[2] != null);
            }
            case FUNC_TRIM_BINARY : {
                if (data[1] == null || data[2] == null) {
                    return null;
                }

                boolean leading  = false;
                boolean trailing = false;
                int     spec     = ((Number) argList[0].valueData).intValue();

                switch (((Number) argList[0].valueData).intValue()) {

                    case Token.BOTH :
                        leading = trailing = true;
                        break;

                    case Token.LEADING :
                        leading = true;
                        break;

                    case Token.TRAILING :
                        trailing = true;
                        break;

                    default :
                        throw Trace.runtimeError(
                            Trace.UNSUPPORTED_INTERNAL_OPERATION,
                            "SQLFunction");
                }

                BlobData string = (BlobData) data[1];

                if (string.length() != 1) {
                    throw Trace.error(Trace.SQL_DATA_TRIM_ERROR);
                }

                byte[] bytes = string.getBytes();

                return ((BinaryType) dataType).trim(session,
                                                    (BlobData) data[3],
                                                    bytes[0], leading,
                                                    trailing);
            }
            case FUNC_OVERLAY_BINARY : {
                if (data[0] == null || data[1] == null || data[2] == null) {
                    return null;
                }

                Object value;

                value = Type.SQL_BIGINT.convertToType(session, data[2],
                                                      argList[2].dataType);

                long offset = ((Number) value).longValue() - 1;
                long length = 0;

                if (argList[3] != null) {
                    if (data[3] == null) {
                        return null;
                    }

                    value = Type.SQL_BIGINT.convertToType(session, data[3],
                                                          argList[3].dataType);
                    length = ((Number) value).longValue();
                }

                return ((BinaryType) dataType).overlay(session,
                                                       (BlobData) data[0],
                                                       (BlobData) data[1],
                                                       offset, length,
                                                       argList[3] != null);
            }
            case FUNC_CURRENT_CATALOG :
                return session.database.getCatalog();

            /*
            case FUNC_CURRENT_DEFAULT_TRANSFORM_GROUP :
            case FUNC_CURRENT_PATH :
            */
            case FUNC_CURRENT_ROLE :
                return null;

            case FUNC_CURRENT_SCHEMA :
                return session.currentSchema.name;

            /*
            case FUNC_CURRENT_TRANSFORM_GROUP_FOR_TYPE :
            */
            case FUNC_CURRENT_USER :
                return session.getUser().getName();

            case FUNC_SESSION_USER :
                return session.getUser().getName();

            /*
            case FUNC_SYSTEM_USER :
            */
            case FUNC_USER :
                return session.getUser().getName();

            case FUNC_VALUE :
                return null;

            case FUNC_CURRENT_DATE :
                return session.getCurrentDate();

            case FUNC_CURRENT_TIME :
                return session.getCurrentTime();

            case FUNC_CURRENT_TIMESTAMP :
                return session.getCurrentTimestamp();

            case FUNC_LOCALTIME :
                return session.getCurrentTime();

            case FUNC_LOCALTIMESTAMP :
                return session.getCurrentTimestamp();

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "SQLFunction");
        }
    }

    public void resolveTypes(Session session,
                             Expression expression) throws HsqlException {

        for (int i = 0; i < argList.length; i++) {
            if (argList[i] != null) {
                argList[i].resolveTypes(session, this);
            }
        }

        switch (id) {

            case FUNC_POSITION_CHAR :
            case FUNC_POSITION_BINARY : {
                if (argList[0].dataType == null) {
                    if (argList[1].dataType == null) {
                        throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE);
                    }

                    if (argList[1].dataType.type == Types.SQL_CLOB
                            || argList[1].dataType.isBinaryType()) {
                        argList[0].dataType = argList[1].dataType;
                    } else {
                        argList[0].dataType = Type.SQL_VARCHAR_MAX_WIDTH;
                    }
                }

                if (argList[1].dataType == null) {
                    if (argList[0].dataType.type == Types.SQL_CLOB
                            || argList[0].dataType.isBinaryType()) {
                        argList[1].dataType = argList[0].dataType;
                    } else {
                        argList[1].dataType = Type.SQL_VARCHAR_MAX_WIDTH;
                    }
                }

                if (argList[0].dataType.isCharacterType()
                        && argList[1].dataType.isCharacterType()) {
                    id = FUNC_POSITION_CHAR;
                } else if (argList[0].dataType.isBinaryType()
                           && argList[1].dataType.isBinaryType()) {
                    id = FUNC_POSITION_BINARY;
                } else {
                    throw Trace.error(Trace.WRONG_DATA_TYPE);
                }

                dataType = Type.SQL_BIGINT;

                break;
            }
            /*
            case FUNC_OCCURENCES_REGEX :
            case FUNC_POSITION_REGEX :
            */
            case FUNC_EXTRACT : {
                if (argList[1].dataType == null) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE);
                }

                if (!argList[1].dataType.isIntervalType()
                        && !argList[1].dataType.isDateTimeType()) {
                    throw Trace.error(Trace.WRONG_DATA_TYPE);
                }

                if (((Number) argList[0].valueData).intValue()
                        == Token.SECOND) {
                    dataType = DateTimeIntervalType.extractSecondType;
                } else {
                    dataType = Type.SQL_INTEGER;
                }

                break;
            }
            case FUNC_CHAR_LENGTH :
            case FUNC_OCTET_LENGTH : {
                if (argList[0].dataType == null) {
                    argList[0].dataType = Type.SQL_VARCHAR_MAX_WIDTH;
                }

                if (!argList[0].dataType.isCharacterType()
                        && !argList[0].dataType.isBinaryType()) {
                    throw Trace.error(Trace.WRONG_DATA_TYPE);
                }

                dataType = Type.SQL_BIGINT;

                break;
            }
            case FUNC_CARDINALITY : {
                dataType = Type.SQL_BIGINT;

                break;
            }
            case FUNC_MOD : {
                if (argList[0].dataType == null) {
                    argList[1].dataType = argList[0].dataType;
                }

                if (argList[1].dataType == null) {
                    argList[0].dataType = argList[1].dataType;
                }

                if (argList[0].dataType == null) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE);
                }

                if (!argList[0].dataType.isNumberType()
                        || !argList[1].dataType.isNumberType()) {
                    throw Trace.error(Trace.WRONG_DATA_TYPE);
                }

                argList[0].dataType =
                    ((NumberType) argList[0].dataType).getIntegralType();
                argList[1].dataType =
                    ((NumberType) argList[1].dataType).getIntegralType();
                dataType = argList[1].dataType;

                break;
            }
            case FUNC_POWER : {
                if (argList[0].dataType == null) {
                    argList[1].dataType = argList[0].dataType;
                }

                if (argList[1].dataType == null) {
                    argList[0].dataType = argList[1].dataType;
                }

                if (argList[0].dataType == null) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE);
                }

                if (!argList[0].dataType.isNumberType()
                        || !argList[1].dataType.isNumberType()) {
                    throw Trace.error(Trace.WRONG_DATA_TYPE);
                }

                argList[0].dataType = Type.SQL_DOUBLE;
                argList[1].dataType = Type.SQL_DOUBLE;
                dataType            = Type.SQL_DOUBLE;

                break;
            }
            case FUNC_LN :
            case FUNC_EXP :
            case FUNC_SQRT : {
                if (argList[0].dataType == null) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE);
                }

                if (!argList[0].dataType.isNumberType()) {
                    throw Trace.error(Trace.WRONG_DATA_TYPE);
                }

                argList[0].dataType = Type.SQL_DOUBLE;
                dataType            = Type.SQL_DOUBLE;

                break;
            }
            case FUNC_ABS :
            case FUNC_FLOOR :
            case FUNC_CEILING : {
                if (argList[0].dataType == null) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE);
                }

                if (!argList[0].dataType.isNumberType()) {
                    throw Trace.error(Trace.WRONG_DATA_TYPE);
                }

                dataType = argList[0].dataType;

                break;
            }
            case FUNC_WIDTH_BUCKET : {
                if (argList[0].dataType == null || argList[1].dataType == null
                        || argList[2].dataType == null
                        || argList[3].dataType == null) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE);
                }

                if (!argList[0].dataType.isNumberType()
                        || !argList[1].dataType.isNumberType()
                        || !argList[2].dataType.isNumberType()
                        || !argList[3].dataType.isIntegralType()) {
                    throw Trace.error(Trace.WRONG_DATA_TYPE);
                }

                dataType = argList[3].dataType;

                break;
            }
            case FUNC_SUBSTRING_CHAR :
            case FUNC_SUBSTRING_BINARY : {
                if (argList[0].dataType == null) {

                    // in 20.6 parameter not allowed as type cannot be determined as binary or char
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE);
                }

                if (argList[1].dataType == null) {
                    argList[1].dataType = NumberType.SQL_NUMERIC_DEFAULT_INT;
                }

                if (!argList[1].dataType.isNumberType()) {
                    throw Trace.error(Trace.WRONG_DATA_TYPE);
                }

                if (argList[2] != null) {
                    if (argList[2].dataType == null) {
                        argList[2].dataType =
                            NumberType.SQL_NUMERIC_DEFAULT_INT;
                    }

                    if (!argList[2].dataType.isNumberType()) {
                        throw Trace.error(Trace.WRONG_DATA_TYPE);
                    }

                    argList[2].dataType =
                        ((NumberType) argList[2].dataType).getIntegralType();
                }

                dataType = argList[0].dataType;

                if (dataType.isCharacterType()) {
                    id = FUNC_SUBSTRING_CHAR;

                    if (dataType.type == Types.SQL_CHAR) {
                        dataType =
                            CharacterType.getCharacterType(Types.SQL_VARCHAR,
                                                           dataType.size());
                    }
                } else if (dataType.isBinaryType()) {
                    id = FUNC_SUBSTRING_BINARY;
                } else {
                    throw Trace.error(Trace.WRONG_DATA_TYPE);
                }

                if (argList[3] != null) {

                    // always boolean constant if defined
                }

                break;
            }
            /*
            case FUNCTION_SUBSTRING_REG_EXPR :
                break;
            case FUNCTION_SUBSTRING_REGEX :
                break;
            */
            case FUNC_FOLD_LOWER :
            case FUNC_FOLD_UPPER :
                if (argList[0].dataType == null) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE);
                }

                dataType = argList[0].dataType;
                break;

            /*
            case FUNCTION_TRANSCODING :
                break;
            case FUNCTION_TRANSLITERATION :
                break;
            case FUNCTION_REGEX_TRANSLITERATION :
                break;
             */
            case FUNC_TRIM_CHAR :
            case FUNC_TRIM_BINARY :
                if (argList[0] == null) {
                    argList[0] = new Expression(ValuePool.getInt(Token.BOTH),
                                                Type.SQL_INTEGER);
                }

                if (argList[2].dataType == null) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE);
                }

                dataType = argList[2].dataType;

                if (dataType.isCharacterType()) {
                    id = FUNC_TRIM_CHAR;

                    if (dataType.type == Types.SQL_CHAR) {
                        dataType =
                            CharacterType.getCharacterType(Types.SQL_VARCHAR,
                                                           dataType.size());
                    }

                    if (argList[1] == null) {
                        argList[1] = new Expression(" ", Type.SQL_CHAR);
                    }
                } else if (dataType.isBinaryType()) {
                    id = FUNC_TRIM_BINARY;

                    if (argList[1] == null) {
                        argList[1] = new Expression(
                            new BinaryData(new byte[]{ 0 }, false),
                            Type.SQL_BINARY);
                    }
                } else {
                    throw Trace.error(Trace.WRONG_DATA_TYPE);
                }
                break;

            case FUNC_OVERLAY_CHAR :
            case FUNC_OVERLAY_BINARY : {
                if (argList[0].dataType == null) {
                    if (argList[1].dataType == null) {
                        throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE);
                    }

                    if (argList[1].dataType.type == Types.SQL_CLOB
                            || argList[1].dataType.isBinaryType()) {
                        argList[0].dataType = argList[1].dataType;
                    } else {
                        argList[0].dataType = Type.SQL_VARCHAR_MAX_WIDTH;
                    }
                }

                if (argList[1].dataType == null) {
                    if (argList[0].dataType.type == Types.SQL_CLOB
                            || argList[0].dataType.isBinaryType()) {
                        argList[1].dataType = argList[0].dataType;
                    } else {
                        argList[1].dataType = Type.SQL_VARCHAR_MAX_WIDTH;
                    }
                }

                if (argList[0].dataType.isCharacterType()
                        && argList[1].dataType.isCharacterType()) {
                    id = FUNC_OVERLAY_CHAR;

                    if (argList[0].dataType.type == Types.SQL_CLOB
                            || argList[1].dataType.type == Types.SQL_CLOB) {
                        dataType = CharacterType.getCharacterType(
                            Types.SQL_CLOB,
                            argList[0].dataType.size()
                            + argList[1].dataType.size());
                    } else {
                        dataType = CharacterType.getCharacterType(
                            Types.SQL_VARCHAR,
                            argList[0].dataType.size()
                            + argList[1].dataType.size());
                    }
                } else if (argList[0].dataType.isBinaryType()
                           && argList[1].dataType.isBinaryType()) {
                    id = FUNC_OVERLAY_BINARY;

                    if (argList[0].dataType.type == Types.SQL_BLOB
                            || argList[1].dataType.type == Types.SQL_BLOB) {
                        dataType = BinaryType.getBinaryType(
                            Types.SQL_BLOB,
                            argList[0].dataType.size()
                            + argList[1].dataType.size());
                    } else {
                        dataType = BinaryType.getBinaryType(
                            Types.SQL_VARBINARY,
                            argList[0].dataType.size()
                            + argList[1].dataType.size());
                    }
                } else {
                    throw Trace.error(Trace.WRONG_DATA_TYPE);
                }

                if (argList[2].dataType == null) {
                    argList[2].dataType = NumberType.SQL_NUMERIC_DEFAULT_INT;
                }

                if (!argList[2].dataType.isNumberType()) {
                    throw Trace.error(Trace.WRONG_DATA_TYPE);
                }

                argList[2].dataType =
                    ((NumberType) argList[2].dataType).getIntegralType();

                if (argList[3] != null) {
                    if (argList[3].dataType == null) {
                        argList[3].dataType =
                            NumberType.SQL_NUMERIC_DEFAULT_INT;
                    }

                    if (!argList[3].dataType.isNumberType()) {
                        throw Trace.error(Trace.WRONG_DATA_TYPE);
                    }

                    argList[3].dataType =
                        ((NumberType) argList[3].dataType).getIntegralType();
                }

                break;
            }
            /*
            case FUNCTION_CHAR_NORMALIZE :
                break;
            */
            case FUNC_CURRENT_CATALOG :
            case FUNC_CURRENT_DEFAULT_TRANSFORM_GROUP :
            case FUNC_CURRENT_PATH :
            case FUNC_CURRENT_ROLE :
            case FUNC_CURRENT_SCHEMA :
            case FUNC_CURRENT_TRANSFORM_GROUP_FOR_TYPE :
            case FUNC_CURRENT_USER :
            case FUNC_SESSION_USER :
            case FUNC_SYSTEM_USER :
            case FUNC_USER :
            case FUNC_VALUE :
                dataType = CharacterType.sqlIdentifierType;
                break;

            case FUNC_CURRENT_DATE :
                dataType = CharacterType.SQL_DATE;
                break;

            case FUNC_CURRENT_TIME :
                dataType = CharacterType.SQL_TIME;
                break;

            case FUNC_CURRENT_TIMESTAMP :
                dataType = CharacterType.SQL_TIMESTAMP;
                break;

            case FUNC_LOCALTIME :
                dataType = CharacterType.SQL_TIME;
                break;

            case FUNC_LOCALTIMESTAMP :
                dataType = CharacterType.SQL_TIMESTAMP;
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "SQLFunction");
        }
    }

    public Object getAggregatedValue(Session session,
                                     Object currValue) throws HsqlException {

        Object[] data = (Object[]) currValue;

        if (data == null) {
            data = new Object[argList.length];
        }

        for (int i = 0; i < argList.length; i++) {
            Expression e = argList[i];

            if (e != null) {
                if (e.isAggregate()) {
                    data[i] = e.getAggregatedValue(session, data[i]);
                } else {
                    data[i] = e.getValue(session, e.dataType);
                }
            }
        }

        return getValue(session, data);
    }

    public Object updateAggregatingValue(Session session,
                                         Object currValue)
                                         throws HsqlException {

        Object[] data = (Object[]) currValue;

        if (data == null) {
            data = new Object[argList.length];
        }

        for (int i = 0; i < argList.length; i++) {
            Expression e = argList[i];

            if (argList[i] != null) {
                data[i] = e.updateAggregatingValue(session, data[i]);
            }
        }

        return data;
    }

    public String getDDL() {

        StringBuffer buf = new StringBuffer();

        switch (id) {

            case FUNC_POSITION_CHAR :
            case FUNC_POSITION_BINARY : {
                buf.append(Token.T_POSITION).append('(')               //
                    .append(argList[0].getDDL()).append(' ')           //
                    .append(Token.T_IN).append(' ')                    //
                    .append(argList[1].getDDL());

                if (argList[2] != null
                        && Boolean.TRUE.equals(argList[2].valueData)) {
                    buf.append(' ').append(Token.T_USING).append(' ').append(
                        Token.T_OCTETS);
                }

                buf.append(')');

                break;
            }
            case FUNC_OCCURENCES_REGEX :
                break;

            case FUNC_POSITION_REGEX :
                break;

            case FUNC_EXTRACT : {
                buf.append(Token.T_EXTRACT).append('(')                //
                    .append(argList[0].getDDL()).append(' ')           //
                    .append(Token.T_FROM).append(' ')                  //
                    .append(argList[1].getDDL()).append(')');

                break;
            }
            case FUNC_CHAR_LENGTH : {
                buf.append(Token.T_CHAR_LENGTH).append('(')            //
                    .append(argList[0].getDDL()).append(')');

                break;
            }
            case FUNC_OCTET_LENGTH : {
                buf.append(Token.T_OCTET_LENGTH).append('(')           //
                    .append(argList[0].getDDL()).append(')');

                break;
            }
            /*
            case FUNC_CARDINALITY :{
                buf.append(Token.T_CARDINALITY).append('(').append(
                    argList[0].getDDL()).append(')');

                break;
            }
            */
            case FUNC_ABS : {
                buf.append(Token.T_ABS).append('(')                    //
                    .append(argList[0].getDDL()).append(')');

                break;
            }
            case FUNC_MOD : {
                buf.append(Token.T_MOD).append('(')                    //
                    .append(argList[0].getDDL()).append(',')           //
                    .append(argList[1].getDDL()).append(')');

                break;
            }
            case FUNC_LN : {
                buf.append(Token.T_LN).append('(')                     //
                    .append(argList[0].getDDL()).append(')');

                break;
            }
            case FUNC_EXP : {
                buf.append(Token.T_EXP).append('(')                    //
                    .append(argList[0].getDDL()).append(')');

                break;
            }
            case FUNC_POWER : {
                buf.append(Token.T_POWER).append('(')                  //
                    .append(argList[0].getDDL()).append(',')           //
                    .append(argList[1].getDDL()).append(')');

                break;
            }
            case FUNC_SQRT : {
                buf.append(Token.T_SQRT).append('(')                   //
                    .append(argList[0].getDDL()).append(')');

                break;
            }
            case FUNC_FLOOR : {
                buf.append(Token.T_FLOOR).append('(')                  //
                    .append(argList[0].getDDL()).append(')');

                break;
            }
            case FUNC_CEILING : {
                buf.append(Token.T_CEILING).append('(').               //
                    append(argList[0].getDDL()).append(')');

                break;
            }
            case FUNC_WIDTH_BUCKET : {
                buf.append(Token.T_WIDTH_BUCKET).append('(')           //
                    .append(argList[0].getDDL()).append(',')           //
                    .append(argList[1].getDDL()).append(',')           //
                    .append(argList[2].getDDL()).append(',')           //
                    .append(argList[3].getDDL()).append(')');

                break;
            }
            case FUNC_SUBSTRING_CHAR :
            case FUNC_SUBSTRING_BINARY :
                buf.append(Token.T_SUBSTRING).append('(')              //
                    .append(argList[0].getDDL()).append(' ')           //
                    .append(Token.T_FROM).append(' ')                  //
                    .append(argList[1].getDDL());

                if (argList[2] != null) {
                    buf.append(' ').append(Token.T_FOR).append(' ')    //
                        .append(argList[2].getDDL());
                }

                if (argList[3] != null) {
                    if (Boolean.TRUE.equals(argList[3].valueData)) {
                        buf.append(' ').append(Token.T_USING).append(
                            ' ').append(Token.T_OCTETS);
                    }
                }

                buf.append(')');
                break;

            /*
            case FUNCTION_SUBSTRING_REG_EXPR :
                break;
            case FUNCTION_SUBSTRING_REGEX :
                break;
            */
            case FUNC_FOLD_LOWER :
                buf.append(Token.T_LOWER).append('(').append(
                    argList[0].getDDL()).append(')');
                break;

            case FUNC_FOLD_UPPER :
                buf.append(Token.T_UPPER).append('(').append(
                    argList[0].getDDL()).append(')');
                break;

            /*
            case FUNCTION_TRANSCODING :
                break;
            case FUNCTION_TRANSLITERATION :
                break;
            case FUNCTION_REGEX_TRANSLITERATION :
                break;
             */
            case FUNC_OVERLAY_CHAR :
            case FUNC_OVERLAY_BINARY :
                buf.append(Token.T_OVERLAY).append('(')                //
                    .append(argList[0].getDDL()).append(' ')           //
                    .append(Token.T_PLACING).append(' ')               //
                    .append(argList[1].getDDL()).append(' ')           //
                    .append(Token.T_FROM).append(' ')                  //
                    .append(argList[2].getDDL());

                if (argList[3] != null) {
                    buf.append(' ').append(Token.T_FOR).append(' ').append(
                        argList[3].getDDL());
                }

                if (argList[4] != null) {
                    if (Boolean.TRUE.equals(argList[4].valueData)) {
                        buf.append(' ').append(Token.T_USING).append(
                            ' ').append(Token.T_OCTETS);
                    }
                }

                buf.append(')');
                break;

            /*
            case FUNCTION_CHAR_NORMALIZE :
                break;
            */
            case FUNC_TRIM_CHAR :
            case FUNC_TRIM_BINARY :
                String spec = null;

                switch (((Number) argList[0].valueData).intValue()) {

                    case Token.BOTH :
                        spec = Token.T_BOTH;
                        break;

                    case Token.LEADING :
                        spec = Token.T_LEADING;
                        break;

                    case Token.TRAILING :
                        spec = Token.T_TRAILING;
                        break;
                }

                buf.append(Token.T_TRIM).append('(')                   //
                    .append(spec).append(' ')                          //
                    .append(argList[1].getDDL()).append(' ')           //
                    .append(Token.T_FROM).append(' ')                  //
                    .append(argList[2].getDDL()).append(')');
                break;

            case FUNC_CURRENT_CATALOG :
            case FUNC_CURRENT_DEFAULT_TRANSFORM_GROUP :
            case FUNC_CURRENT_PATH :
            case FUNC_CURRENT_ROLE :
            case FUNC_CURRENT_SCHEMA :
            case FUNC_CURRENT_TRANSFORM_GROUP_FOR_TYPE :
            case FUNC_CURRENT_USER :
            case FUNC_SESSION_USER :
            case FUNC_SYSTEM_USER :
            case FUNC_USER :
            case FUNC_CURRENT_DATE :
            case FUNC_CURRENT_TIME :
            case FUNC_CURRENT_TIMESTAMP :
            case FUNC_LOCALTIME :
            case FUNC_LOCALTIMESTAMP :
                return name;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "SQLFunction");
        }

        return buf.toString();
    }

    public boolean equals(Object other) {

        if (other instanceof SQLFunction && id == ((SQLFunction) other).id) {
            return super.equals(other);
        }

        return false;
    }

    /**
     * Returns a String representation of this object. <p>
     */
    public String describe(Session session) {

        StringBuffer sb = new StringBuffer();

        sb.append("FUNCTION ").append("=[\n");
        sb.append(name).append("(");

        for (int i = 0; i < argList.length; i++) {
            sb.append("[").append(argList[i].describe(session)).append("]");
        }

        sb.append(") returns ").append(dataType.getName());
        sb.append("]\n");

        return sb.toString();
    }

    /**
     * isAalueFunction
     *
     * @return boolean
     */
    public boolean isValueFunction() {
        return isValueFunction;
    }
}
