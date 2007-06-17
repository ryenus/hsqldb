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

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Locale;

import org.hsqldb.lib.IntValueHashMap;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.store.BitMap;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.BinaryData;

// fredt@users 20020218 - patch 455785 by hjbusch@users - large DECIMAL inserts
// also Long.MIM_VALUE (bug 473388) inserts - applied to different parts
// fredt@users 20020408 - patch 1.7.0 by fredt - exact integral types
// integral values are cast into the smallest type that can hold them
// fredt@users 20020501 - patch 550970 by boucherb@users - fewer StringBuffers
// fredt@users 20020611 - patch 1.7.0 by fredt - correct statement logging
// changes to the working of getLastPart() to return the correct statement for
// logging in the .script file.
// also restructuring to reduce use of objects and speed up tokenising of
// strings and quoted identifiers
// fredt@users 20021112 - patch 1.7.2 by Nitin Chauhan - use of switch
// rewrite of the majority of multiple if(){}else{} chains with switch(){}
// fredt@users 20030610 - patch 1.7.2 - no StringBuffers
// blaine / fred - three part identifier support

/**
 * Provides the ability to tokenize SQL character sequences.
 *
 * Extensively rewritten and extended in successive versions of HSQLDB.
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 1.9.0
 * @since Hypersonic SQL
 */
public class Tokenizer {

    private static final int NO_TYPE     = Types.SQL_TYPE_NUMBER_LIMIT + 1,
                             NAME        = Types.SQL_TYPE_NUMBER_LIMIT + 2,
                             LONG_NAME   = Types.SQL_TYPE_NUMBER_LIMIT + 3,
                             SPECIAL     = Types.SQL_TYPE_NUMBER_LIMIT + 4,
                             QUOTED_NAME = Types.SQL_TYPE_NUMBER_LIMIT + 5,
                             REMARK_LINE = Types.SQL_TYPE_NUMBER_LIMIT + 6,
                             REMARK      = Types.SQL_TYPE_NUMBER_LIMIT + 7;
    private String           sCommand;
    private int              iLength;
    private int              iIndex;
    private int              tokenIndex;
    private int              nextTokenIndex;
    private int              beginIndex;
    private int              iType = NO_TYPE;
    private String           sToken;

    // Qualifiers are object name qualifiers like in
    //     "qualifier1.qualier2.objectname"
    private String  namePrefix    = null;
    private String  namePrePrefix = null;
    private boolean recursing     = false;

    // WAIT.  Don't do anything before popping another Token (because the
    // state variables aren't set properly due to a call of wait()).
    private boolean bWait;
    private boolean lastTokenQuotedID;

    // literals that are values
    static IntValueHashMap valueTokens;

    static {
        valueTokens = new IntValueHashMap();

        valueTokens.put(Token.T_NULL, Types.SQL_ALL_TYPES);
        valueTokens.put(Token.T_TRUE, Types.SQL_BOOLEAN);
        valueTokens.put(Token.T_FALSE, Types.SQL_BOOLEAN);
    }

    public Tokenizer() {}

    public Tokenizer(String s) {

        sCommand = s;
        iLength  = s.length();
        iIndex   = 0;
    }

    public void reset(String s) {

        sCommand          = s;
        iLength           = s.length();
        iIndex            = 0;
        tokenIndex        = 0;
        nextTokenIndex    = 0;
        beginIndex        = 0;
        iType             = NO_TYPE;
        sToken            = null;
        namePrefix        = null;
        namePrePrefix     = null;
        bWait             = false;
        lastTokenQuotedID = false;
        recursing         = false;
    }

    /**
     *
     */
    void back() {

        if (bWait) {
            Trace.doAssert(false, "Querying state when in Wait mode");
        }

        nextTokenIndex = iIndex;
        iIndex         = tokenIndex;
        bWait          = true;
    }

    void position(int pos) {
        iIndex = pos;
        bWait  = false;
    }

    /**
     * get the given token or throw for commands and simple unquoted identifiers
     * only
     *
     * @param match String
     * @throws HsqlException
     * @return String
     */
    String getThis(String match) throws HsqlException {

        getToken();
        matchThis(match);

        return sToken;
    }

    /**
     * for commands and simple unquoted identifiers only
     *
     * @param match String
     * @throws HsqlException
     */
    void matchThis(String match) throws HsqlException {

        if (bWait) {
            Trace.doAssert(false, "Querying state when in Wait mode");
        }

        if (!sToken.equals(match) || iType == QUOTED_NAME
                || iType == LONG_NAME) {

            // match is a single token - when no match, we report the first
            // unmatched token as the unexpected token
            String token = namePrePrefix != null ? namePrePrefix
                                                 : namePrefix != null
                                                   ? namePrefix
                                                   : sToken;

            throw Trace.error(Trace.UNEXPECTED_TOKEN, Trace.TOKEN_REQUIRED,
                              new Object[] {
                token, match
            });
        }
    }

    /**
     * Used for commands only
     *
     * @param match String
     * @return boolean
     */
    public boolean isGetThis(String match) throws HsqlException {

        getToken();

        if (iType != QUOTED_NAME && iType != LONG_NAME
                && sToken.equals(match)) {
            return true;
        }

        back();

        return false;
    }

    boolean wasSpecial() {

        if (bWait) {
            Trace.doAssert(false, "Querying state when in Wait mode");
        }

        return iType == SPECIAL;
    }

    /**
     * this methode is called before other wasXXX methods and takes precedence
     *
     * @return boolean
     */
    boolean wasValue() {

        if (bWait) {
            Trace.doAssert(false, "Querying state when in Wait mode");
        }

        switch (iType) {

            case Types.SQL_BINARY :
            case Types.SQL_BIT :
            case Types.SQL_CHAR :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
            case Types.SQL_DOUBLE :
            case Types.SQL_NUMERIC :
            case Types.SQL_BOOLEAN :
            case Types.SQL_ALL_TYPES :
                return true;

            default :
                return false;
        }
    }

    boolean wasQuotedIdentifier() {

        if (bWait) {
            Trace.doAssert(false, "Querying state when in Wait mode");
        }

        return lastTokenQuotedID;
    }

    /**
     * Method declaration
     *
     * @return boolean
     */
    boolean wasLongName() {

        if (bWait) {
            Trace.doAssert(false, "Querying state when in Wait mode");
        }

        return iType == LONG_NAME;
    }

    /**
     * Simple Name means a quoted or unquoted identifier without qualifiers
     * provided it is not in the hKeyword list.
     *
     * @return boolean
     * @throws HsqlException
     */
    boolean wasSimpleName() {

        if (bWait) {
            throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                     "Querying state when in Wait mode");
        }

        if (iType == NAME || (iType == QUOTED_NAME && sToken.length() != 0)) {
            return true;
        }

        return false;
    }

    boolean wasNameOrKeyword() throws HsqlException {

        if (bWait) {
            throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                     "Querying state when in Wait mode");
        }

        return (iType == NAME || iType == QUOTED_NAME || iType == LONG_NAME);
    }

    /**
     * Return first part of long name.
     *
     * @return String prefix
     */
    String getNamePrefix() {

        if (bWait) {
            Trace.doAssert(false, "Querying state when in Wait mode");
        }

        return namePrefix;
    }

    /**
     * Return second part of long name
     *
     * @return String pre-prefix
     */
    String getNamePrePrefix() {

        if (bWait) {
            Trace.doAssert(false, "Querying state when in Wait mode");
        }

        return namePrePrefix;
    }

    boolean wasSimpleToken() {

        if (bWait) {
            throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                     "Querying state when in Wait mode");
        }

        return iType != QUOTED_NAME && iType != LONG_NAME
               && iType != Types.SQL_CHAR;
    }

    String getSimpleToken() throws HsqlException {

        getToken();

        if (iType == QUOTED_NAME || iType == LONG_NAME
                || iType == Types.SQL_CHAR) {
            String token = iType == LONG_NAME ? namePrefix
                                              : sToken;

            throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
        }

        return sToken;
    }

    public boolean wasThis(String match) {

        if (sToken.equals(match) && iType != QUOTED_NAME && iType != LONG_NAME
                && iType != Types.SQL_CHAR) {
            return true;
        }

        return false;
    }

    /**
     * getName() is more broad than getSimpleName() in that it includes
     * 2-part names as well
     *
     * @return popped name
     * @throws HsqlException if next token is not an AName
     */
    String getName() throws HsqlException {

        getToken();

        if (!wasNameOrKeyword()) {
            throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
        }

        return sToken;
    }

    /**
     * Returns a single, unqualified name (identifier)
     *
     * @return name
     * @throws HsqlException
     */
    public String getSimpleName() throws HsqlException {

        getToken();

        if (!wasSimpleName()) {
            String token = iType == LONG_NAME ? namePrefix
                                              : sToken;

            throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
        }

        return sToken;
    }

    /**
     * Return any token.
     *
     * @return token
     * @throws HsqlException
     */
    public String getString() throws HsqlException {

        getToken();

        return sToken;
    }

    int getInt() throws HsqlException {

        long v = getBigint();

        if (v > Integer.MAX_VALUE || v < Integer.MIN_VALUE) {
            throw Trace.error(Trace.NUMERIC_VALUE_OUT_OF_RANGE);
        }

        return (int) v;
    }

    static BigDecimal LONG_MAX_VALUE_INCREMENT =
        BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.valueOf(1));

    long getBigint() throws HsqlException {

        boolean minus = false;

        getToken();

        if (sToken.equals(Token.T_MINUS)) {
            minus = true;

            getToken();
        }

        Object o = getAsValue();
        int    t = getType();

        switch (t) {

            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
                break;

            case Types.SQL_NUMERIC :

                // only Long.MAX_VALUE + 1 together with minus is acceptable
                if (minus && LONG_MAX_VALUE_INCREMENT.equals(o)) {
                    return Long.MIN_VALUE;
                }
            default :
                throw Trace.error(Trace.WRONG_DATA_TYPE);
        }

        long v = ((Number) o).longValue();

        return minus ? -v
                     : v;
    }

    Object getInType(int type) throws HsqlException {

        getToken();

        Object o = getAsValue();
        int    t = getType();

        if (t != type) {
            throw Trace.error(Trace.WRONG_DATA_TYPE);
        }

        return o;
    }

    /**
     *
     * @return data type
     * @throws HsqlException
     */
    public int getType() {

        if (bWait) {
            Trace.doAssert(false, "Querying state when in Wait mode");
        }

        // todo: make sure it's used only for Values!
        // todo: synchronize iType with hColumn
        switch (iType) {

            case Types.SQL_CHAR :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
            case Types.SQL_DOUBLE :
            case Types.SQL_NUMERIC :
            case Types.SQL_BINARY :
            case Types.SQL_BIT :
            case Types.SQL_BOOLEAN :
                return iType;

            default :
                return Types.SQL_ALL_TYPES;
        }
    }

    /**
     * Method declaration
     *
     * @return value Object
     * @throws HsqlException
     */
    Object getAsValue() throws HsqlException {

        if (!wasValue()) {
            throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
        }

        switch (iType) {

            case Types.SQL_ALL_TYPES :
                return null;

            case Types.SQL_CHAR :

                //fredt - no longer returning string with a singlequote as last char
                return sToken;

            case Types.SQL_BIT : {
                try {
                    BitMap map = StringConverter.bitToBitMap(sToken);

                    return new BinaryData(map.getBytes(), map.size());
                } catch (IOException e) {
                    throw Trace.error(Trace.INVALID_CHARACTER_ENCODING,
                                      e.toString());
                }
            }
            case Types.SQL_BINARY : {
                byte[] array;

                try {
                    array = StringConverter.hexToByteArray(sToken);

                    return new BinaryData(array, false);
                } catch (IOException e) {
                    throw Trace.error(Trace.INVALID_CHARACTER_ENCODING,
                                      e.toString());
                }
            }
            case Types.SQL_BIGINT :
                return ValuePool.getLong(Long.parseLong(sToken));

            case Types.SQL_INTEGER :

                // fredt - this returns unsigned values which are later negated.
                // as a result Integer.MIN_VALUE or Long.MIN_VALUE are promoted
                // to a wider type.
                if (sToken.length() < 11) {
                    try {
                        return ValuePool.getInt(Integer.parseInt(sToken));
                    } catch (Exception e1) {}
                }

                if (sToken.length() < 20) {
                    try {
                        iType = Types.SQL_BIGINT;

                        return ValuePool.getLong(Long.parseLong(sToken));
                    } catch (Exception e2) {}
                }

                iType = Types.SQL_NUMERIC;

                return new BigDecimal(sToken);

            case Types.SQL_DOUBLE :
                double d = JavaSystem.parseDouble(sToken);
                long   l = Double.doubleToLongBits(d);

                return ValuePool.getDouble(l);

            case Types.SQL_NUMERIC :
                return new BigDecimal(sToken);

            case Types.SQL_BOOLEAN :
                return sToken.equalsIgnoreCase("TRUE") ? Boolean.TRUE
                                                       : Boolean.FALSE;

            default :
                return sToken;
        }
    }

    /**
     * return the current position to be used for VIEW processing
     *
     * @return current postion
     */
    int getPosition() {
        return iIndex;
    }

    int getTokenPosition() {
        return tokenIndex;
    }

    /**
     * return part of the command String
     *
     * @return part of String
     * @param begin int
     * @param end int
     */
    String getPart(int begin, int end) {
        return sCommand.substring(begin, end);
    }

// fredt@users 20020910 - patch 1.7.1 by Nitin Chauhan - rewrite as switch

    /**
     * Gets a token
     *
     * @throws HsqlException
     */
    private void getToken() throws HsqlException {

        /* Should synchronize this method, due to use of instance
         * variable 'recursing'? */
        /* For LONG_NAMEs, iType will be LONG_NAME.  Must check
         * qualifier1Type and qualifier2Type to find out the types of
         * the embedded qualifiers */
        if (bWait) {
            bWait  = false;
            iIndex = nextTokenIndex;

            return;
        }

        if (!recursing) {

            // We are not recursing, so clear qualifiers
            namePrefix = namePrePrefix = null;

            while (iIndex < iLength
                    && Character.isWhitespace(sCommand.charAt(iIndex))) {
                iIndex++;
            }

            tokenIndex = iIndex;
        }

        sToken = "";

        if (iIndex >= iLength) {
            lastTokenQuotedID = false;
            iType             = NO_TYPE;

            return;
        }

        char    c        = sCommand.charAt(iIndex);
        boolean point    = false,
                digit    = false,
                exp      = false,
                afterexp = false;
        boolean end      = false;
        char    cfirst   = 0;

        lastTokenQuotedID = false;

        if (c == 'B' || c == 'b') {
            cfirst = c;
            iType  = Types.SQL_BIT;
        } else if (c == 'X' || c == 'x') {
            cfirst = c;
            iType  = Types.SQL_BINARY;
        } else if (Character.isJavaIdentifierStart(c)) {
            iType = NAME;
        } else if (Character.isDigit(c)) {
            iType = Types.SQL_INTEGER;
            digit = true;
        } else {
            switch (c) {

                case '(' :
                    sToken = Token.T_OPENBRACKET;
                    iType  = SPECIAL;

                    iIndex++;

                    return;

                case ')' :
                    sToken = Token.T_CLOSEBRACKET;
                    iType  = SPECIAL;

                    iIndex++;

                    return;

                case ',' :
                    sToken = Token.T_COMMA;
                    iType  = SPECIAL;

                    iIndex++;

                    return;

                case '*' :
                    sToken = Token.T_MULTIPLY;
                    iType  = SPECIAL;

                    iIndex++;

                    return;

                case '=' :
                    sToken = Token.T_EQUALS;
                    iType  = SPECIAL;

                    iIndex++;

                    return;

                case ';' :
                    sToken = Token.T_SEMICOLON;
                    iType  = SPECIAL;

                    iIndex++;

                    return;

                case '+' :
                    sToken = Token.T_PLUS;
                    iType  = SPECIAL;

                    iIndex++;

                    return;

                case '%' :
                    sToken = Token.T_PERCENT;
                    iType  = SPECIAL;

                    iIndex++;

                    return;

                case '?' :
                    sToken = Token.T_QUESTION;
                    iType  = SPECIAL;

                    iIndex++;

                    return;

                case '\"' :
                    lastTokenQuotedID = true;
                    iType             = QUOTED_NAME;

                    iIndex++;

                    sToken = getString('"');

                    if (iIndex == sCommand.length()) {
                        return;
                    }

                    c = sCommand.charAt(iIndex);

                    if (c == '.') {

                        // Set another level of qualifiers
                        if (namePrePrefix != null) {
                            throw Trace.error(Trace.UNEXPECTED_TOKEN);
                        }

                        if (namePrefix != null) {
                            namePrePrefix = namePrefix;
                        }

                        namePrefix = sToken;

                        iIndex++;

// fredt - todo - avoid recursion - this has problems when there is whitespace
// after the dot - the same with NAME
// Do I need to test this condition, or is this warning obsolete?
                        recursing = true;

                        getToken();

                        recursing = false;
                        iType     = LONG_NAME;
                    }

                    return;

                case '\'' :
                    iType = Types.SQL_CHAR;

                    iIndex++;

                    sToken = getString('\'');

                    return;

                case '!' :
                case '<' :
                case '>' :
                case '|' :
                case '/' :
                case '-' :
                    cfirst = c;
                    iType  = SPECIAL;
                    break;

                case '.' :
                    iType = Types.SQL_NUMERIC;
                    point = true;
                    break;

                default :
                    throw Trace.error(Trace.UNEXPECTED_TOKEN,
                                      String.valueOf(c));
            }
        }

        int start = iIndex++;

        while (true) {
            if (iIndex >= iLength) {
                c   = ' ';
                end = true;

                Trace.check(iType != Types.SQL_CHAR && iType != QUOTED_NAME,
                            Trace.UNEXPECTED_END_OF_COMMAND);
            } else {
                c = sCommand.charAt(iIndex);
            }

            switch (iType) {

                case Types.SQL_BIT :
                case Types.SQL_BINARY :
                    if (c == '\'') {
                        iIndex++;

                        sToken = getString('\'');

                        return;
                    } else {
                        iType = NAME;
                    }
                case NAME :
                    if (Character.isJavaIdentifierPart(c)) {
                        break;
                    }

                    // fredt - todo new char[] to back sToken
                    sToken =
                        sCommand.substring(start,
                                           iIndex).toUpperCase(Locale.ENGLISH);

                    if (c == '.') {

                        // Set another level of qualifiers
                        if (namePrePrefix != null) {
                            throw Trace.error(Trace.TOO_MANY_IDENTIFIER_PARTS);
                        }

                        if (namePrefix != null) {
                            namePrePrefix = namePrefix;
                        }

                        namePrefix = sToken;

                        iIndex++;

                        recursing = true;

                        getToken();    // todo: eliminate recursion

                        recursing = false;
                        iType     = LONG_NAME;
                    } else if (c == '(') {

                        // it is a function call
                    } else {

                        // if in value list then it is a value
                        int type = valueTokens.get(sToken, -1);

                        if (type != -1) {
                            iType = type;
                        }
                    }

                    return;

                case QUOTED_NAME :
                case Types.SQL_CHAR :

                    // shouldn't get here
                    break;

                case REMARK :
                    if (end) {

                        // unfinished remark
                        // maybe print error here
                        iType = NO_TYPE;

                        return;
                    } else if (c == '*') {
                        iIndex++;

                        if (iIndex < iLength
                                && sCommand.charAt(iIndex) == '/') {

                            // using recursion here
                            iIndex++;

                            getToken();

                            return;
                        }
                    }
                    break;

                case REMARK_LINE :
                    if (end) {
                        iType = NO_TYPE;

                        return;
                    } else if (c == '\r' || c == '\n') {

                        // using recursion here
                        getToken();

                        return;
                    }
                    break;

                case SPECIAL :
                    if (c == '/' && cfirst == '/') {
                        iType = REMARK_LINE;

                        break;
                    } else if (c == '-' && cfirst == '-') {
                        iType = REMARK_LINE;

                        break;
                    } else if (c == '*' && cfirst == '/') {
                        iType = REMARK;

                        break;
                    } else if (c == '>' || c == '=' || c == '|') {
                        break;
                    }

                    sToken = sCommand.substring(start, iIndex);

                    return;

                case Types.SQL_INTEGER :
                case Types.SQL_DOUBLE :
                case Types.SQL_NUMERIC :
                    if (Character.isDigit(c)) {
                        digit = true;
                    } else if (c == '.') {
                        iType = Types.SQL_NUMERIC;

                        if (point) {
                            throw Trace.error(Trace.UNEXPECTED_TOKEN, ".");
                        }

                        point = true;
                    } else if (c == 'E' || c == 'e') {
                        if (exp) {
                            throw Trace.error(Trace.UNEXPECTED_TOKEN, "E");
                        }

                        // HJB-2001-08-2001 - now we are sure it's a float
                        iType = Types.SQL_DOUBLE;

                        // first character after exp may be + or -
                        afterexp = true;
                        point    = true;
                        exp      = true;
                    } else if (c == '-' && afterexp) {
                        afterexp = false;
                    } else if (c == '+' && afterexp) {
                        afterexp = false;
                    } else {
                        afterexp = false;

                        if (!digit) {
                            if (point && start == iIndex - 1) {
                                sToken = ".";
                                iType  = SPECIAL;

                                return;
                            }

                            throw Trace.error(Trace.UNEXPECTED_TOKEN,
                                              String.valueOf(c));
                        }

                        sToken = sCommand.substring(start, iIndex);

                        return;
                    }
            }

            iIndex++;
        }
    }

// fredt - strings are constructed from new char[] objects to avoid slack
// because these strings might end up as part of internal data structures
// or table elements.
// we may consider using pools to avoid recreating the strings
    private String getString(char quoteChar) throws HsqlException {

        try {
            int     nextIndex   = iIndex;
            boolean quoteInside = false;

            for (;;) {
                nextIndex = sCommand.indexOf(quoteChar, nextIndex);

                if (nextIndex < 0) {
                    throw Trace.error(Trace.UNEXPECTED_END_OF_COMMAND);
                }

                if (nextIndex < iLength - 1
                        && sCommand.charAt(nextIndex + 1) == quoteChar) {
                    quoteInside = true;
                    nextIndex   += 2;

                    continue;
                }

                break;
            }

            char[] chBuffer = new char[nextIndex - iIndex];

            sCommand.getChars(iIndex, nextIndex, chBuffer, 0);

            int j = chBuffer.length;

            if (quoteInside) {
                j = 0;

                // fredt - loop assumes all occurences of quoteChar are paired
                // this has already been checked by the preprocessing loop
                for (int i = 0; i < chBuffer.length; i++, j++) {
                    if (chBuffer[i] == quoteChar) {
                        i++;
                    }

                    chBuffer[j] = chBuffer[i];
                }
            }

            iIndex = ++nextIndex;

            return new String(chBuffer, 0, j);
        } catch (HsqlException e) {
            throw e;
        } catch (Exception e) {
            e.getMessage();
        }

        return null;
    }

    /**
     *
     * @return length of command
     */
    int getLength() {
        return iLength;
    }
}
