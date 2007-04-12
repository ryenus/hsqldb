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
import org.hsqldb.Library;
import org.hsqldb.Session;
import org.hsqldb.Token;
import org.hsqldb.Trace;
import org.hsqldb.Types;

/**
 * Type implementation for BOOLEAN.<p>
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class BooleanType extends Type {

    static BooleanType booleanType = new BooleanType();

    private BooleanType() {
        super(Types.SQL_BOOLEAN, 0, 0);
    }

    public int displaySize() {
        return  5;

    }
    public int getJDBCTypeNumber() {
        return Types.BOOLEAN;
    }

    public String getJDBCClassName() {
        return "java.lang.Boolean";
    }

    public int getSQLGenericTypeNumber() {
        return type;
    }

    public int getSQLSpecificTypeNumber() {
        return type;
    }

    public String getName() {
        return Token.T_BOOLEAN;
    }

    public String getDefinition() {
        return Token.T_BOOLEAN;
    }

    public boolean isBooleanType() {
        return true;
    }

    public Type getAggregateType(Type other) throws HsqlException {

        if (type == other.type) {
            return this;
        }

        throw Trace.error(Trace.INVALID_CONVERSION);
    }

    public Type getCombinedType(Type other,
                                int operation) throws HsqlException {
        return this;
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

        boolean boola = ((Boolean) a).booleanValue();
        boolean boolb = ((Boolean) b).booleanValue();

        return (boola == boolb) ? 0
                                : (boolb ? -1
                                         : 1);
    }

    public Object convertToTypeLimits(Object a) throws HsqlException {
        return a;
    }

    public Object convertToType(Session session, Object a,
                                Type otherType) throws HsqlException {

        if (a == null) {
            return a;
        }

        switch (otherType.type) {

            case Types.SQL_BOOLEAN :
                return a;

            case Types.SQL_CLOB :
                a = Type.SQL_VARCHAR.convertToType(session, a, otherType);
            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR :
            case Types.VARCHAR_IGNORECASE : {
                a = Library.trim((String) a, " ", true, true);

                if (((String) a).equalsIgnoreCase("TRUE")) {
                    return Boolean.TRUE;
                } else if (((String) a).equalsIgnoreCase("FALSE")) {
                    return Boolean.FALSE;
                } else if (((String) a).equalsIgnoreCase("UNKNOWN")) {
                    return null;
                } else if (((String) a).equalsIgnoreCase("NULL")) {
                    return null;
                }
            }
            default :
                throw Trace.error(Trace.STRING_DATA_TRUNCATION);
        }
    }

    public Object convertToDefaultType(Object a) throws HsqlException {

        if (a == null) {
            return null;
        }

        if (a instanceof Boolean) {
            return a;
        } else if (a instanceof String) {
            convertToType(null, a, Type.SQL_VARCHAR);
        }

        throw Trace.error(Trace.INVALID_CONVERSION);
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        return ((Boolean) a).booleanValue() ? "TRUE"
                                            : "FALSE";
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return "UNKNOWN";
        }

        return ((Boolean) a).booleanValue() ? "TRUE"
                                            : "FALSE";
    }

    public static BooleanType getBooleanType() {
        return booleanType;
    }
}
