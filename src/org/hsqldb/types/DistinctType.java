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
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.HsqlNameManager;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.rights.Grantee;
import org.hsqldb.SchemaObject;

/**
 * Class for DISTINCT type objects.<p>
 *
 * @author fredt@users
 * @version 1.9.0
 * @since 1.9.0
 */
public class DistinctType extends Type implements SchemaObject {

    Type     baseType;
    HsqlName name;

    public DistinctType(HsqlName name, Type baseType) {

        super(baseType.type, baseType.precision, baseType.scale);

        this.name = name;
    }

    // interface specific methods
    public HsqlName getName() {
        return name;
    }

    public HsqlName getSchemaName() {
        return name.schema;
    }

    public Grantee getOwner() {
        return null;
    }

    public OrderedHashSet getReferences() {
        return null;
    }

    public void compile(Session session) {}

    // abstract methods
    public int compare(Object a, Object b) {
        return baseType.compare(a, b);
    }

    public Object convertToDefaultType(Object o) throws HsqlException {
        return baseType.convertToDefaultType(o);
    }

    public String convertToSQLString(Object a) {
        return baseType.convertToSQLString(a);
    }

    public String convertToString(Object a) {
        return baseType.convertToString(a);
    }

    public Object convertToType(Session session, Object a,
                                Type type) throws HsqlException {
        return baseType.convertToType(session, a, type);
    }

    public Object convertToTypeLimits(Object a) throws HsqlException {
        return baseType.convertToTypeLimits(a);
    }

    public int displaySize() {
        return baseType.displaySize();
    }

    public Type getAggregateType(Type other) throws HsqlException {
        return baseType.getAggregateType(other);
    }

    public Type getCombinedType(Type other,
                                int operation) throws HsqlException {
        return baseType.getCombinedType(other, operation);
    }

    public String getDefinition() {
        return baseType.getDefinition();
    }

    public String getJDBCClassName() {
        return baseType.getJDBCClassName();
    }

    public int getJDBCTypeNumber() {
        return baseType.getJDBCTypeNumber();
    }

    public String getNameString() {
        return name.name;
    }

    public int getSQLGenericTypeNumber() {
        return baseType.getSQLGenericTypeNumber();
    }

    public int getSQLSpecificTypeNumber() {
        return baseType.getSQLSpecificTypeNumber();
    }

    // non-abstract methods
    public boolean acceptsFractionalPrecision() {
        return false;
    }

    public boolean acceptsPrecision() {
        return false;
    }

    public boolean acceptsScale() {
        return false;
    }

    public Object add(Object a, Object b) throws HsqlException {
        return baseType.add(a, b);
    }

    public Object castToType(Session session, Object a,
                             Type type) throws HsqlException {
        return baseType.castToType(session, a, type);
    }

    public int compareToTypeRange(Object o) {
        return baseType.compareToTypeRange(o);
    }

    public Object concat(Session session, Object a,
                         Object b) throws HsqlException {
        return baseType.concat(session, a, b);
    }

    public Object divide(Object a, Object b) throws HsqlException {
        return baseType.divide(a, b);
    }

    public boolean isBinaryType() {
        return baseType.isBinaryType();
    }

    public boolean isBooleanType() {
        return baseType.isBooleanType();
    }

    public boolean isCharacterType() {
        return baseType.isCharacterType();
    }

    public boolean isDateTimeType() {
        return baseType.isDateTimeType();
    }

    public boolean isDistinctType() {
        return true;
    }

    public boolean isIntegralType() {
        return baseType.isIntegralType();
    }

    public boolean isIntervalType() {
        return baseType.isIntervalType();
    }

    public boolean isLobType() {
        return baseType.isLobType();
    }

    public boolean isNumberType() {
        return baseType.isNumberType();
    }

    public Object multiply(Object a, Object b) throws HsqlException {
        return baseType.multiply(a, b);
    }

    public boolean requiresPrecision() {
        return false;
    }

    public Object subtract(Object a, Object b) throws HsqlException {
        return baseType.subtract(a, b);
    }
}
