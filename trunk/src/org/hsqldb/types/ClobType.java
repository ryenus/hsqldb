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
import org.hsqldb.lib.StringConverter;

public class ClobType extends CharacterType {

    static final int defaultClobSize = 1024 * 1024;

    public ClobType() {
        super(Types.SQL_CLOB, defaultClobSize);
    }

    public ClobType(long precision) {
        super(Types.SQL_CLOB, precision);
    }

    public int displaySize() {
        return precision > Integer.MAX_VALUE ? Integer.MAX_VALUE
                                             : (int) precision;
    }

    public int getJDBCTypeNumber() {
        return Types.CLOB;
    }

    public String getJDBCClassName() {
        return "java.sql.Clob";
    }

    public int getSQLGenericTypeNumber() {
        return type;
    }

    public int getSQLSpecificTypeNumber() {
        return type;
    }

    public String getName() {
        return Token.T_CLOB;
    }

    public String getDefinition() {

        long   factor     = precision;
        String multiplier = null;

        if (precision % (1024 * 1024 * 1024) == 0) {
            factor     = precision / (1024 * 1024 * 1024);
            multiplier = Token.T_G_MULTIPLIER;
        } else if (precision % (1024 * 1024) == 0) {
            factor     = precision / (1024 * 1024);
            multiplier = Token.T_M_MULTIPLIER;
        } else if (precision % (1024) == 0) {
            factor     = precision / (1024);
            multiplier = Token.T_K_MULTIPLIER;
        }

        StringBuffer sb = new StringBuffer(16);

        sb.append(getName());
        sb.append('(');
        sb.append(factor);

        if (multiplier != null) {
            sb.append(' ').append(multiplier);
        }

        sb.append(')');

        return sb.toString();
    }

    public boolean isLobType() {
        return true;
    }

    public int compare(Object a, Object b) {

        if (a == b) {
            return 0;
        }

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        long aId = ((ClobData) a).getId();
        long bId = ((ClobData) b).getId();

        return (aId > bId) ? 1
                           : (bId > aId ? -1
                                        : 0);
    }

    public Object convertToTypeLimits(Object a) throws HsqlException {
        return a;
    }

    public Object convertToType(Session session, Object a,
                                Type otherType) throws HsqlException {

        if (a == null) {
            return null;
        }

        if (otherType.type == Types.SQL_CLOB) {
            return a;
        }

        if (otherType.isCharacterType()) {
            return new ClobDataMemory(((String) a).toCharArray(), false);
        }

        throw Trace.error(Trace.INVALID_CONVERSION);
    }

    public Object convertToDefaultType(Object a) throws HsqlException {

        if (a == null) {
            return a;
        }

        // conversion to Clob via PreparedStatement.setObject();
        if (a instanceof String) {
            return new ClobDataMemory(((String) a).toCharArray(), false);
        }

        throw Trace.error(Trace.INVALID_CONVERSION);
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        return ((ClobData) a).toString();
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return "NULL";
        }

        String s = convertToString(a);

        return StringConverter.toQuotedString(s, '\'', true);
    }

    public long position(Object data, Object otherData, Type otherType,
                         long start) throws HsqlException {

        if (otherType.type == Types.SQL_CLOB) {
            return ((ClobData) data).position((ClobData) otherData, start);
        } else if (otherType.isCharacterType()) {
            return ((ClobData) data).position((String) otherData, start);
        } else {
            throw Trace.runtimeError(Trace.UNSUPPORTED_INTERNAL_OPERATION,
                                     "CharacterType");
        }
    }
}
