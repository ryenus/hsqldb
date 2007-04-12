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


package org.hsqldb.types;

import org.hsqldb.HsqlException;
import org.hsqldb.Session;
import org.hsqldb.Token;
import org.hsqldb.Trace;
import org.hsqldb.Types;

public class NullType extends Type {

    static NullType nullType = new NullType();

    private NullType() {
        super(Types.SQL_ALL_TYPES, 0, 0);
    }

    public int displaySize() {
        return 4;
    }

    public int getJDBCTypeNumber() {
        return type;
    }

    public String getJDBCClassName() {
        return "java.lang.Void";
    }

    public int getSQLGenericTypeNumber() {
        return type;
    }

    public int getSQLSpecificTypeNumber() {
        return type;
    }

    public String getName() {
        return Token.T_NULL;
    }

    public String getDefinition() {
        return Token.T_NULL;
    }

    public Type getAggregateType(Type other) throws HsqlException {
        return other;
    }

    public Type getCombinedType(Type other,
                                int operation) throws HsqlException {
        return other;
    }


    public int compare(Object a, Object b) {
        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                 "NullType");
    }

    public Object convertToTypeLimits(Object a) throws HsqlException {
        return null;
    }

    public Object convertToType(Session session, Object a,
                                Type otherType) throws HsqlException {
        return null;
    }

    public Object convertToDefaultType(Object a) throws HsqlException {
        return null;
    }

    public String convertToString(Object a) {
        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                 "NullType");
    }

    public String convertToSQLString(Object a) {
        throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                 "NullType");
    }

    public static Type getNullType() {
        return nullType;
    }
}
