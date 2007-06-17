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

import java.io.Serializable;

import org.hsqldb.HsqlException;
import org.hsqldb.Session;
import org.hsqldb.Token;
import org.hsqldb.Trace;
import org.hsqldb.Types;
import org.hsqldb.lib.StringConverter;

/**
 * Type implementation for OTHER type.<p>
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class OtherType extends Type {

    static OtherType otherType = new OtherType();

    private OtherType() {
        super(Types.OTHER, 0, 0);
    }

    public int displaySize() {
        return precision > Integer.MAX_VALUE ? Integer.MAX_VALUE
                                             : (int) precision;
    }

    public int getJDBCTypeNumber() {
        return type;
    }

    public String getJDBCClassName() {
        return "java.lang.Object";
    }

    public int getSQLGenericTypeNumber() {

        // return Types.SQL_UDT;
        return type;
    }

    public int getSQLSpecificTypeNumber() {

        // return Types.SQL_UDT;
        return type;
    }

    public String getNameString() {
        return Token.T_OTHER;
    }

    public String getDefinition() {
        return Token.T_OTHER;
    }

    public Type getAggregateType(Type other) throws HsqlException {

        if (type == other.type) {
            return this;
        }

        if (other == SQL_ALL_TYPES) {
            return this;
        }

        throw Trace.error(Trace.INVALID_CONVERSION);
    }

    public Type getCombinedType(Type other,
                                int operation) throws HsqlException {
        return this;
    }

    public int compare(Object a, Object b) {

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        return 0;
    }

    public Object convertToTypeLimits(Object a) throws HsqlException {
        return a;
    }

    public Object convertToType(Session session, Object a,
                                Type otherType) throws HsqlException {
        return a;
    }

    public Object convertToDefaultType(Object a) throws HsqlException {

        if (a instanceof Serializable) {
            return a;
        }

        throw Trace.error(Trace.INVALID_CONVERSION);
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        return StringConverter.byteArrayToHexString(
            ((JavaObjectData) a).getBytes());
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return "NULL";
        }

        return StringConverter.byteArrayToSQLHexString(
            ((JavaObjectData) a).getBytes());
    }

    public static OtherType getOtherType() {
        return otherType;
    }
}
