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
import org.hsqldb.types.Type;

/**
 * Implementation of HSQL functions with reserved names or value functions
 * that have an SQL standard equivalent.<p>
 *
 * Apart from IDENTITY() all functions are translated into equivalent SQL
 * Standard functions.
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class LegacyFunction extends SQLFunction {

    private final static int FUNC_IDENTITY = 81;

    //
    static IntValueHashMap legacyRegularFuncMap = new IntValueHashMap();

    static {
        legacyRegularFuncMap.put(Token.T_YEAR, FUNC_EXTRACT);
        legacyRegularFuncMap.put(Token.T_MONTH, FUNC_EXTRACT);
        legacyRegularFuncMap.put(Token.T_DAY, FUNC_EXTRACT);
        legacyRegularFuncMap.put(Token.T_HOUR, FUNC_EXTRACT);
        legacyRegularFuncMap.put(Token.T_MINUTE, FUNC_EXTRACT);
        legacyRegularFuncMap.put(Token.T_SECOND, FUNC_EXTRACT);
        legacyRegularFuncMap.put(Token.T_IDENTITY, FUNC_IDENTITY);
    }

    static IntValueHashMap legacyValueFuncMap = new IntValueHashMap();

    static {
        legacyValueFuncMap.put(Token.T_USER, FUNC_USER);
        legacyValueFuncMap.put(Token.T_SYSDATE, FUNC_CURRENT_DATE);
        legacyValueFuncMap.put(Token.T_TODAY, FUNC_CURRENT_DATE);
        legacyValueFuncMap.put(Token.T_NOW, FUNC_CURRENT_TIMESTAMP);
    }

    private int extractSpec;

    public static SQLFunction newLegacyFunction(String token) {

        int id = legacyRegularFuncMap.get(token, -1);

        if (id == -1) {
            id = legacyValueFuncMap.get(token, -1);
        }

        if (id == -1) {
            return null;
        }

        if (token.equals(Token.T_USER)) {
            SQLFunction function = new SQLFunction(id);

            function.parseList = new short[] {
                Token.X_OPTION, 2, Token.OPENBRACKET, Token.CLOSEBRACKET
            };

            return function;
        }

        if (token.equals(Token.T_SYSDATE) || token.equals(Token.T_TODAY)
                || token.equals(Token.T_NOW)) {
            return new SQLFunction(id);
        }

        LegacyFunction function = new LegacyFunction(id);

        function.extractSpec = Token.get(token);

        return function;
    }

    public static boolean isRegularFunction(String token) {
        return legacyRegularFuncMap.containsKey(token);
    }

    public static boolean isValueFunction(String token) {
        return legacyValueFuncMap.containsKey(token);
    }

    private LegacyFunction(int id) {

        super();

        this.id = id;

        switch (id) {

            case FUNC_EXTRACT :
                name      = Token.T_EXTRACT;
                parseList = singleParamList;
                break;

            case FUNC_IDENTITY :
                name      = Token.T_IDENTITY;
                parseList = emptyParamList;
                dataType  = Type.SQL_BIGINT;
                break;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "SQLFunction");
        }
    }

    private void setExtractArgList(Expression[] argList) {

        this.argList = new Expression[2];
        this.argList[0] = new Expression(ValuePool.getInt(extractSpec),
                                         Type.SQL_INTEGER);
        this.argList[1] = argList[0];
    }

    public void setArguments(Expression[] argList) throws HsqlException {

        switch (id) {

            case FUNC_EXTRACT :
                setExtractArgList(argList);

                return;

            case FUNC_IDENTITY :
                return;

            default :
                throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                         "SQLFunction");
        }
    }

    public void resolveTypes(Session session,
                             Expression parent) throws HsqlException {

        if (id == FUNC_IDENTITY) {
            return;
        }

        super.resolveTypes(session, this);
    }

    public Object getValue(Session session) throws HsqlException {

        if (id == FUNC_IDENTITY) {
            return session.getLastIdentity();
        }

        return super.getValue(session);
    }

    public String getDDL() {

        if (id == FUNC_IDENTITY) {
            return new StringBuffer(Token.T_IDENTITY).append(
                Token.T_OPENBRACKET).append(Token.T_CLOSEBRACKET).toString();
        }

        return super.getDDL();
    }

    public String describe(Session session) {
        return super.describe(session);
    }
}
