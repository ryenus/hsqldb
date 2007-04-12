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

import org.hsqldb.Database;
import org.hsqldb.HsqlException;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.Session;
import org.hsqldb.Trace;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HsqlArrayList;

// fredt@users 20020130 - patch 497872 by Nitin Chauhan - loop optimisation
// fredt@users 20020320 - doc 1.7.0 - update
// fredt@users 20021103 - patch 1.7.2 - allow for drop table, etc.
// fredt@users 20030613 - patch 1.7.2 - simplified data structures and reporting
// unsaved@users - patch 1.8.0 moved right managament to new classes

/**
 *
 * Manages the User objects for a Database instance.
 * The special users PUBLIC_USER_NAME and SYSTEM_AUTHORIZATION_NAME
 * are created and managed here.  SYSTEM_AUTHORIZATION_NAME is also
 * special in that the name is not kept in the user "list"
 * (PUBLIC_USER_NAME is kept in the list because it's needed by MetaData
 * routines via "listVisibleUsers(x, true)").
 *
 * Partly based on Hypersonic code.
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @author boucherb@users
 * @author fredt@users
 *
 * @version  1.8.0
 * @since  1.7.2
 * @see  User
 */
public class UserManager {

    /**
     * This object's set of User objects. <p>
     *
     * Note: The special _SYSTEM  role
     * is not included in this list but the special PUBLIC
     * User object is kept in the list because it's needed by MetaData
     * routines via "listVisibleUsers(x, true)".
     */
    private HashMappedList userList;
    private GranteeManager granteeManager;

    /**
     * Construction happens once for each Database object.
     *
     * Creates special users PUBLIC_USER_NAME and SYSTEM_AUTHORIZATION_NAME.
     * Sets up association with the GranteeManager for this database.
     */
    public UserManager(Database database) throws HsqlException {

        granteeManager = database.getGranteeManager();
        userList       = new HashMappedList();
    }

    /**
     * Creates a new User object under management of this object. <p>
     *
     *  A set of constraints regarding user creation is imposed: <p>
     *
     *  <OL>
     *    <LI>If the specified name is null, then an
     *        ASSERTION_FAILED exception is thrown stating that
     *        the name is null.
     *
     *    <LI>If this object's collection already contains an element whose
     *        name attribute equals the name argument, then
     *        a GRANTEE_ALREADY_EXISTS exception is thrown.
     *        (This will catch attempts to create Reserved grantee names).
     *  </OL>
     */
    public User createUser(HsqlName name,
                           String password) throws HsqlException {

        if (name == null) {
            throw Trace.runtimeError(Trace.NULL_NAME, "UserManager");
        }

        // This will throw an appropriate Trace if grantee already exists,
        // regardless of whether the name is in any User, Role, etc. list.
        User u       = granteeManager.addUser(name);
        u.setPassword(password);
        boolean success = userList.add(name.name, u);

        if (!success) {
            throw Trace.error(Trace.USER_ALREADY_EXISTS, name.name);
        }

        return u;
    }

    /**
     * Attempts to drop a User object with the specified name
     *  from this object's set. <p>
     *
     *  A successful drop action consists of: <p>
     *
     *  <UL>
     *
     *    <LI>removing the User object with the specified name
     *        from the set.
     *
     *    <LI>revoking all rights from the removed object<br>
     *        (this ensures that in case there are still references to the
     *        just dropped User object, those references
     *        cannot be used to erronously access database objects).
     *
     *  </UL> <p>
     *
     */
    public void dropUser(String name) throws HsqlException {

        boolean reservedUser = GranteeManager.isReserved(name);

        Trace.check(!reservedUser, Trace.NONMOD_ACCOUNT, name);

        boolean result = granteeManager.removeGrantee(name);

        Trace.check(result, Trace.NO_SUCH_GRANTEE, name);

        User u = (User) userList.remove(name);

        Trace.check(u != null, Trace.USER_NOT_FOUND, name);
    }

    /**
     * Returns the User object with the specified name and
     * password from this object's set.
     */
    public User getUser(String name, String password) throws HsqlException {

        if (name == null) {
            name = "";
        }

        if (password == null) {
            password = "";
        }

        User u = get(name);

        u.checkPassword(password);

        return u;
    }

    /**
     * Retrieves this object's set of User objects as
     *  an HsqlArrayList. <p>
     */
    public HashMappedList getUsers() {
        return userList;
    }

    public boolean exists(String name) {
        return userList.get(name) == null ? false
                                          : true;
    }

    /**
     * Returns the User object identified by the
     * name argument. <p>
     */
    public User get(String name) throws HsqlException {

        User u = (User) userList.get(name);

        if (u == null) {
            throw Trace.error(Trace.USER_NOT_FOUND, name);
        }

        return u;
    }

    /**
     * Retrieves the <code>User</code> objects representing the database
     * users that are visible to the <code>User</code> object
     * represented by the <code>session</code> argument. <p>
     *
     * If the <code>session</code> argument's <code>User</code> object
     * attribute has isAdmin() true (directly or by virtue of a Role),
     * then all of the
     * <code>User</code> objects in this collection are considered visible.
     * Otherwise, only this object's special <code>PUBLIC</code>
     * <code>User</code> object attribute and the session <code>User</code>
     * object, if it exists in this collection, are considered visible. <p>
     *
     * @param session The <code>Session</code> object used to determine
     *          visibility
     * @return a list of <code>User</code> objects visible to
     *          the <code>User</code> object contained by the
     *         <code>session</code> argument.
     *
     */
    public HsqlArrayList listVisibleUsers(Session session) {

        HsqlArrayList list;
        User          user;
        boolean       isAdmin;
        String        sessName;
        String        userName;

        list     = new HsqlArrayList();
        isAdmin  = session.isAdmin();
        sessName = session.getUsername();

        if (userList == null || userList.size() == 0) {
            return list;
        }

        for (int i = 0; i < userList.size(); i++) {
            user = (User) userList.get(i);

            if (user == null) {
                continue;
            }

            userName = user.getName();

            if (isAdmin) {
                list.add(user);
            } else if (sessName.equals(userName)) {
                list.add(user);
            }
        }

        return list;
    }

    /**
     * Returns the specially constructed
     * <code>SYSTEM_AUTHORIZATION_NAME</code>
     * <code>User</code> object for the current <code>Database</code> object.
     *
     * @throws HsqlException - if the specified <code>Database</code>
     *          has no <code>SYS_AUTHORIZATION_NAME</code>
     *          <code>User</code> object.
     * @return the <code>SYS_AUTHORIZATION_NAME</code>
     *          <code>User</code> object
     *
     */
    public User getSysUser() {
        return granteeManager.systemRole;
    }

    public synchronized void removeSchemaReference(String schemaName) {

        for (int i = 0; i < userList.size(); i++) {
            User     user   = (User) userList.get(i);
            HsqlName schema = user.getInitialSchema();

            if (schema == null) {
                continue;
            }

            if (schemaName.equals(schema.name)) {
                user.setInitialSchema(null);
            }
        }
    }
}
