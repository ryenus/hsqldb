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


package org.hsqldb.rights;

import org.hsqldb.HsqlException;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.Token;
import org.hsqldb.Trace;
import org.hsqldb.lib.StringConverter;

// fredt@users 20021103 - patch 1.7.2 - fix bug in revokeAll()
// fredt@users 20021103 - patch 1.7.2 - allow for drop table, etc.
// when tables are dropped or renamed, changes are reflected in the
// permissions held in User objects.
// boucherb@users 200208-200212 - doc 1.7.2 - update
// boucherb@users 200208-200212 - patch 1.7.2 - metadata
// unsaved@users - patch 1.8.0 moved right managament to new classes

/**
 * A User Object holds the grantee and password for a
 * particular database user.<p>
 *
 * Enhanced in successive versions of HSQLDB.
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 1.8.0
 * @since Hypersonic SQL
 */
public class User extends Grantee {

    /** password. */
    private String sPassword;

    /** default schema when new Sessions started (defaults to PUBLIC schema) */
    private HsqlName initialSchema = null;

    /**
     * Constructor
     */
    User(HsqlName name, GranteeManager manager) {

        super(name, manager);
    }

    public void setPassword(String password) throws HsqlException {

        // TODO:
        // checkComplexity(password);
        // requires: UserManager.createSAUser(), UserManager.createPublicUser()
        sPassword = password;
    }

    /**
     * Checks if this object's password attibute equals
     * specified argument, else throws.
     */
    public void checkPassword(String test) throws HsqlException {
        Trace.check(test.equals(sPassword), Trace.ACCESS_IS_DENIED);
    }

    /**
     * Returns the initial schema for the user
     */
    public HsqlName getInitialSchema() {
        return initialSchema;
    }

    /**
     * This class does not have access to the SchemaManager, therefore
     * caller should verify that the given schemaName exists.
     *
     * @param schemaName Name of an existing schema.  Null value allowed,
     *                   which means use the DB default session schema.
     */
    public void setInitialSchema(HsqlName schema) {
        initialSchema = schema;
    }

    /**
     * Returns the ALTER USER DDL character sequence that preserves the
     * this user's current password value and mode. <p>
     *
     * @return  the DDL
     */
    public String getAlterUserDDL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Token.T_ALTER).append(' ');
        sb.append(Token.T_USER).append(' ');
        sb.append(getStatementName()).append(' ');
        sb.append(Token.T_SET).append(' ');
        sb.append(Token.T_PASSWORD).append(' ');
        sb.append('"').append(sPassword).append('"');

        return sb.toString();
    }

    public String getInitialSchemaDDL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Token.T_ALTER).append(' ');
        sb.append(Token.T_USER).append(' ');
        sb.append(getStatementName()).append(' ');
        sb.append(Token.T_SET).append(' ');
        sb.append(Token.T_INITIAL).append(' ');
        sb.append(Token.T_SCHEMA).append(' ');
        sb.append('"').append(initialSchema.name).append('"');

        return sb.toString();
    }

    /**
     * returns the DDL string
     * sequence that creates this user.
     *
     */
    public String getCreateUserDDL() {

        StringBuffer sb = new StringBuffer(64);

        sb.append(Token.T_CREATE).append(' ');
        sb.append(Token.T_USER).append(' ');
        sb.append(getStatementName()).append(' ');
        sb.append(Token.T_PASSWORD).append(' ');
        sb.append(StringConverter.toQuotedString(sPassword, '"', true));

        return sb.toString();
    }

    /**
     * Retrieves the redo log character sequence for connecting
     * this user
     *
     * @return the redo log character sequence for connecting
     *      this user
     */
    public String getConnectStatement() {

        StringBuffer sb = new StringBuffer();

        sb.append(Token.T_CONNECT).append(' ');
        sb.append(Token.T_USER).append(' ');
        sb.append(getStatementName());

        return sb.toString();
    }

    /**
     * Retrieves the Grantee object for this User.
     */
    public Grantee getGrantee() {
        return this;
    }
}
