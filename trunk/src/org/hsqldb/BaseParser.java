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

import org.hsqldb.lib.IntKeyIntValueHashMap;
import org.hsqldb.types.Type;

public class BaseParser {

    protected Database database;
    private Tokenizer  tokenizer;
    protected Session  session;
    protected String   namePrePrefix;
    protected String   namePrefix;
    protected String   tokenString;
    protected boolean  isQuoted;
    protected Object   value;
    protected Type     valueType;
    protected int      tokenType;
    protected boolean  isReservedKey;
    protected boolean  isCoreReservedKey;
    protected boolean  isSpecial;

    /**
     *  Constructs a new BaseParser object with the given context.
     *
     * @param  session the connected context
     * @param  t the token source from which to parse commands
     */
    BaseParser(Session session, Tokenizer t) {

        tokenizer    = t;
        this.session = session;
        database     = session.getDatabase();
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

        namePrefix  = null;
        tokenType   = Token.X_STARTPARSE;
        tokenString = null;
        value       = null;

        tokenizer.reset(sql);
    }

    /**
     * @throws  HsqlException
     */
    void read() throws HsqlException {

        namePrePrefix     = null;
        namePrefix        = null;
        value             = null;
        tokenString       = tokenizer.getString();
        isQuoted          = tokenizer.wasQuotedIdentifier();
        isReservedKey     = false;
        isCoreReservedKey = false;
        isSpecial         = false;

        if (tokenizer.wasValue()) {
            tokenType = Token.X_VALUE;
            value     = tokenizer.getAsValue();
            valueType = Type.getValueType(tokenizer.getType(), value);
        } else if (isQuoted || tokenizer.wasLongName()) {
            namePrePrefix = tokenizer.getNamePrePrefix();
            namePrefix    = tokenizer.getNamePrefix();

            if (tokenString.equals(Token.T_ASTERISK)) {
                tokenType = Token.ASTERISK;
            } else {
                tokenType = Token.X_NAME;
            }
        } else if (tokenString.length() == 0) {
            tokenType = Token.X_ENDPARSE;
        } else {
            tokenType = Token.getKeyword(tokenString, Token.X_ENDPARSE);

            if (tokenType == Token.X_ENDPARSE) {
                tokenType = Token.getNonKeyword(tokenString, Token.X_ENDPARSE);
                isSpecial = tokenizer.wasSpecial();
            } else {
                isReservedKey     = true;
                isCoreReservedKey = Token.isCoreKeyword(tokenType);
            }

            if (tokenType == Token.X_ENDPARSE) {
                tokenType = Token.X_NAME;
            }
        }
    }

    boolean isName() throws HsqlException {
        return tokenizer.wasNameOrKeyword() && !isCoreReservedKey;
    }

    void checkIsName() throws HsqlException {

        if (!isName()) {
            throw unexpectedToken();
        }
    }

    void checkIsNameOrKeyword() throws HsqlException {

        if (!tokenizer.wasNameOrKeyword()) {
            throw unexpectedToken();
        }
    }

    void readThis(int tokenId) throws HsqlException {

        if (tokenType != tokenId) {
            throw unexpectedToken();
        }

        read();
    }

    boolean readIfThis(int tokenId) throws HsqlException {

        if (tokenType == tokenId) {
            read();

            return true;
        }

        return false;
    }

    void checkIsNotQuoted() throws HsqlException {

        if (isQuoted) {
            throw unexpectedToken();
        }
    }

    void checkIsQuoted() throws HsqlException {

        if (!isQuoted) {
            throw Trace.error(Trace.QUOTED_IDENTIFIER_REQUIRED, tokenString);
        }
    }

    void checkIsValue() throws HsqlException {

        if (tokenType != Token.X_VALUE) {
            throw unexpectedToken();
        }
    }

    boolean isSimpleName() {
        return tokenizer.wasSimpleName() && !isCoreReservedKey;
    }

    void checkIsSimpleName() throws HsqlException {

        if (!isSimpleName()) {
            throw unexpectedToken();
        }
    }

    void readQuotedString() throws HsqlException {
        tokenString = (String) tokenizer.getInType(Types.SQL_CHAR);
        tokenType   = Token.X_VALUE;
    }

    int readInteger() throws HsqlException {

        boolean minus = false;

        if (tokenType == Token.MINUS) {
            minus = true;

            read();
        }

        checkIsValue();

        if (minus && valueType.type == Types.SQL_BIGINT
                && ((Number) value).longValue() == -(long) Integer.MIN_VALUE) {
            read();

            return Integer.MIN_VALUE;
        }

        if (valueType.type != Types.SQL_INTEGER) {
            throw Trace.error(Trace.WRONG_DATA_TYPE);
        }

        int val = ((Number) value).intValue();

        if (minus) {
            val = -val;
        }

        read();

        return val;
    }

    long readBigint() throws HsqlException {

        boolean minus = false;

        if (tokenType == Token.MINUS) {
            minus = true;

            read();
        }

        checkIsValue();

        if (minus && valueType.type == Types.SQL_NUMERIC
                && Tokenizer.LONG_MAX_VALUE_INCREMENT.equals(value)) {
            read();

            return Long.MIN_VALUE;
        }

        if (valueType.type != Types.SQL_INTEGER
                && valueType.type != Types.SQL_BIGINT) {
            throw Trace.error(Trace.WRONG_DATA_TYPE);
        }

        long val = ((Number) value).longValue();

        if (minus) {
            val = -val;
        }

        read();

        return val;
    }

    int getPosition() {
        return tokenizer.getTokenPosition();
    }

    void rewind(int position) throws HsqlException {
        tokenizer.position(position);
        read();
    }

    String getLastPart(int position) {
        return tokenizer.getPart(position, tokenizer.getTokenPosition());
    }

    String getLastPartAndCurrent(int position) {
        return tokenizer.getPart(position, tokenizer.getPosition());
    }

    static int getExpressionType(int token) {

        int type = expressionTypeMap.get(token, -1);

        if (type == -1) {
            throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                     "Parser");
        }

        return type;
    }

    private static IntKeyIntValueHashMap expressionTypeMap =
        new IntKeyIntValueHashMap(37);

    static {

        // comparison
        expressionTypeMap.put(Token.EQUALS, Expression.EQUAL);
        expressionTypeMap.put(Token.GREATER, Expression.GREATER);
        expressionTypeMap.put(Token.LESS, Expression.SMALLER);
        expressionTypeMap.put(Token.GREATER_EQUALS, Expression.GREATER_EQUAL);
        expressionTypeMap.put(Token.LESS_EQUALS, Expression.SMALLER_EQUAL);
        expressionTypeMap.put(Token.NOT_EQUALS, Expression.NOT_EQUAL);

        // aggregates
        expressionTypeMap.put(Token.COUNT, Expression.COUNT);
        expressionTypeMap.put(Token.MAX, Expression.MAX);
        expressionTypeMap.put(Token.MIN, Expression.MIN);
        expressionTypeMap.put(Token.SUM, Expression.SUM);
        expressionTypeMap.put(Token.AVG, Expression.AVG);
        expressionTypeMap.put(Token.EVERY, Expression.EVERY);
        expressionTypeMap.put(Token.SOME, Expression.SOME);
        expressionTypeMap.put(Token.STDDEV_POP, Expression.STDDEV_POP);
        expressionTypeMap.put(Token.STDDEV_SAMP, Expression.STDDEV_SAMP);
        expressionTypeMap.put(Token.VAR_POP, Expression.VAR_POP);
        expressionTypeMap.put(Token.VAR_SAMP, Expression.VAR_SAMP);
    }

    HsqlException unexpectedToken() {

        String token = namePrePrefix != null ? namePrePrefix
                                             : namePrefix != null ? namePrefix
                                                                  : tokenString;

        return Trace.error(Trace.UNEXPECTED_TOKEN, token);
    }

    HsqlException requiredToken(String required) {

        String token = namePrePrefix != null ? namePrePrefix
                                             : namePrefix != null ? namePrefix
                                                                  : tokenString;

        return Trace.error(Trace.UNEXPECTED_TOKEN, token);
    }
}
