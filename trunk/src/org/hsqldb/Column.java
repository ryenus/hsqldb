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

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.types.Type;
import org.hsqldb.types.DomainType;

/**
 *  Implementation of SQL table columns as defined in DDL statements with
 *  static methods to process their values.<p>
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @author fredt@users
 * @version    1.8.0
 * @since Hypersonic SQL
 */
public class Column {

    // most variables are final but not declared so because of a bug in
    // JDK 1.1.8 compiler
    public HsqlName        columnName;
    private Type           type;
    private boolean        isNullable;
    private boolean        isIdentity;
    private boolean        isPrimaryKey;
    private Expression     defaultExpression;
    private Expression     generatingExpression;
    private NumberSequence sequence;

    /**
     *  Creates a column defined in DDL statement.
     *
     * @param  name
     * @param  nullable
     * @param  type
     * @param  primaryKey
     * @param  defExpression
     */
    public Column(HsqlName name, Type type, boolean nullable,
                  boolean isPrimaryKey, Expression defaultExpression) {

        columnName             = name;
        isNullable             = nullable;
        this.type              = type;
        this.isPrimaryKey      = isPrimaryKey;
        this.defaultExpression = defaultExpression;
    }

    void setIdentity(NumberSequence sequence) {
        this.sequence = sequence;
        isIdentity    = sequence != null;
    }

    private Column() {}

    /**
     * Used for primary key changes.
     */
    Column duplicate(boolean withIdentity) throws HsqlException {

        Column newCol = new Column();

        newCol.columnName        = columnName;
        newCol.isNullable        = isNullable;
        newCol.type              = type;
        newCol.defaultExpression = defaultExpression;
        newCol.isIdentity        = isIdentity;

        if (withIdentity && isIdentity) {
            newCol.sequence = sequence.duplicate();
/*
                setIdentity(isIdentity, identityStart, identityIncrement,
                               identityMin, identityMax, identityCycle, identityAlways);
*/
        }

        return newCol;
    }

    void setType(Column other) {
        isNullable = other.isNullable;
        type       = other.type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    /**
     *  Is this the identity column in the table.
     *
     * @return boolean
     */
    public boolean isIdentity() {
        return isIdentity;
    }

    public NumberSequence getIdentitySequence() {
        return sequence;
    }

    /**
     *  Is column nullable.
     *
     * @return boolean
     */
    public boolean isNullable() {

        if (isNullable) {
            if (type.isDomainType()) {
                return ((DomainType) type).isNullable();
            }
        }

        return isNullable;
    }

    /**
     * Is column writeable or always generated
     *
     * @return boolean
     */
    public boolean isWriteable() {
        return true;
    }

    /**
     *  Set nullable.
     *
     */
    void setNullable(boolean value) {
        isNullable = value;
    }

    /**
     *  Is this single column primary key of the table.
     *
     * @return boolean
     */
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    /**
     *  Set primary key.
     *
     */
    void setPrimaryKey(boolean value) {
        isPrimaryKey = value;
    }

    /**
     *  Returns default value in the session context.
     */
    Object getDefaultValue(Session session) throws HsqlException {

        return defaultExpression == null ? null
                                         : defaultExpression.getValue(session,
                                         type);
    }

    /**
     *  Returns generated value in the session context.
     */
    Object getGeneratedValue(Session session) throws HsqlException {

        return generatingExpression == null ? null
                                            : generatingExpression.getValue(
                                            session, type);
    }

    /**
     *  Returns DDL for default value.
     */
    public String getDefaultDDL() {

        String ddl = null;

        ddl = defaultExpression == null ? null
                                        : defaultExpression.getDDL();

        return ddl;
    }

    public HsqlName getName() {
        return columnName;
    }

    /**
     *  Returns default expression for the column.
     */
    Expression getDefaultExpression() {

        if (defaultExpression == null) {
            if (type.isDomainType()) {
                return ((DomainType) type).getDefaultClause();
            }

            return null;
        } else {
            return defaultExpression;
        }
    }

    void setDefaultExpression(Expression expr) {
        defaultExpression = expr;
    }

    /**
     *  Returns generated expression for the column.
     */
    Expression getGeneratingExpression() {
        return generatingExpression;
    }

    void setGeneratingExpression(Expression expr) {
        generatingExpression = expr;
    }

    public Type getType() {
        return type;
    }

    /**
     * Leaving here, avoiding references to database object classes in ResultMetaData
     */
    static void setMetaDataColumnInfo(ResultMetaData meta, int index,
                                      Table table, Column column) {

        meta.catalogNames[index] = table.getCatalogName();
        meta.schemaNames[index]  = table.getSchemaName().name;
        meta.tableNames[index]   = table.getName().name;
        meta.colNames[index]     = column.getName().name;
        meta.colTypes[index]     = column.getType();
        meta.colNullable[index] =
            column.isNullable() && !column.isPrimaryKey() ? Expression.NULLABLE
                                                          : Expression
                                                          .NO_NULLS;
        meta.isIdentity[index] = column.isIdentity();
        meta.isWritable[index] = column.isWriteable();
    }
}
