/* Copyright (c) 2001-2025, The HSQL Development Group
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
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.Routine;
import org.hsqldb.Schema;
import org.hsqldb.SchemaObject;
import org.hsqldb.Session;
import org.hsqldb.SqlInvariants;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.error.HsqlException;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.List;
import org.hsqldb.lib.OrderedHashMap;
import org.hsqldb.result.Result;

/**
 * Manages the User objects for a Database instance.
 * The special users PUBLIC_USER_NAME and SYSTEM_AUTHORIZATION_NAME
 * are created and managed here.  SYSTEM_AUTHORIZATION_NAME is also
 * special in that the name is not kept in the user "list"
 * (PUBLIC_USER_NAME is kept in the list because it's needed by MetaData
 * routines via "listVisibleUsers(x, true)").
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 *
 * @version 2.7.3
 * @since 1.7.2
 * @see  User
 */
public final class UserManager {

    /**
     * This object's set of User objects. <p>
     *
     * Note: The special _SYSTEM  role
     * is not included in this list but the special PUBLIC
     * User object is kept in the list because it's needed by MetaData
     * routines via "listVisibleUsers(x, true)".
     */
    private OrderedHashMap<String, User> userList;
    private GranteeManager               granteeManager;

    /**
     * The function for password complexity.
     */
    Routine pwCheckFunction;
    Routine extAuthenticationFunction;

    /**
     * Construction happens once for each Database instance.
     *
     * Creates special users PUBLIC_USER_NAME and SYSTEM_AUTHORIZATION_NAME.
     * Sets up association with the GranteeManager for this database.
     */
    public UserManager(Database database) {
        granteeManager = database.getGranteeManager();
        userList       = new OrderedHashMap<>();
    }

    /**
     * Creates a new User object under management of this object. <p>
     *
     *  A set of constraints regarding user creation is imposed:
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
    public User createUser(
            Session session,
            HsqlName name,
            String password,
            boolean isDigest) {

        // This will throw an appropriate exception if grantee already exists,
        // regardless of whether the name is in any User, Role, etc. list.
        User user = granteeManager.addUser(name);

        if (session == null) {
            user.setPassword(password, isDigest);
        } else {
            try {
                setPassword(session, user, password, isDigest);
            } catch (HsqlException e) {
                granteeManager.removeNewUser(name);

                throw e;
            }
        }

        // this cannot fail
        userList.add(name.name, user);

        return user;
    }

    public void setPassword(
            Session session,
            User user,
            String password,
            boolean isDigest) {

        if (!isDigest && !checkComplexity(session, password)) {
            throw Error.error(ErrorCode.PASSWORD_COMPLEXITY);
        }

        // requires: UserManager.createSAUser(), UserManager.createPublicUser()
        user.setPassword(password, isDigest);
    }

    public boolean checkComplexity(Session session, String password) {

        if (session == null || pwCheckFunction == null) {
            return true;
        }

        Result result = pwCheckFunction.invoke(
            session,
            new Object[]{ password },
            null,
            true);
        Boolean check = (Boolean) result.getValueObject();

        if (check == null || !check.booleanValue()) {
            return false;
        }

        return true;
    }

    /**
     * Attempts to drop a User object with the specified name
     *  from this object's set. <p>
     *
     *  A successful drop action consists of:
     *
     *  <UL>
     *
     *    <LI>removing the User object with the specified name
     *        from the set.
     *
     *    <LI>revoking all rights from the removed User<br>
     *        (this ensures that in case there are still references to the
     *        just dropped User object, those references
     *        cannot be used to erroneously access database objects).
     *
     *  </UL>
     *
     */
    public void dropUser(String name) {

        boolean reservedUser = GranteeManager.isReserved(name);

        if (reservedUser) {
            throw Error.error(ErrorCode.X_28502, name);
        }

        boolean result = granteeManager.removeGrantee(name);

        if (!result) {
            throw Error.error(ErrorCode.X_28501, name);
        }

        User user = userList.remove(name);

        if (user == null) {
            throw Error.error(ErrorCode.X_28501, name);
        }
    }

    public void createFirstUser(String username, String password) {

        boolean isQuoted = true;

        if (username.equalsIgnoreCase("SA")) {
            username = "SA";
            isQuoted = false;
        }

        HsqlName name = granteeManager.database.nameManager.newHsqlName(
            username,
            isQuoted,
            SchemaObject.GRANTEE);
        User user = createUser(null, name, password, false);

        user.isLocalOnly = true;

        granteeManager.grant(
            name.name,
            SqlInvariants.DBA_ADMIN_ROLE_NAME,
            granteeManager.getDBARole());
    }

    /**
     * Returns the User object with the specified name and
     * password from this object's set.
     */
    public User getUser(String name, String password) {

        if (name == null) {
            name = "";
        }

        if (password == null) {
            password = "";
        }

        User    user    = userList.get(name);
        boolean isLocal = user != null && user.isLocalOnly;

        if (extAuthenticationFunction == null || isLocal) {
            user = get(name);

            user.checkPassword(password);

            return user;
        }

        /*
         * Authentication returns String[]. When null, use the existing
         * user object only, with existing privileges.
         * When not null, ignore if user exists. Otherwise create a user and
         * assign the list of roles to the user.
         */
        Result result = extAuthenticationFunction.invokeJavaMethodDirect(
            new String[]{ granteeManager.database.getNameString(), name,
                          password });

        if (result.isError()) {
            throw Error.error(ErrorCode.X_28501, result.getMainString());
        }

        Object[] roles = (Object[]) result.getValueObject();

        if (user == null) {
            HsqlName hsqlName = granteeManager.database.nameManager.newHsqlName(
                name,
                true,
                SchemaObject.GRANTEE);

            user                = createUser(null, hsqlName, "", false);
            user.isExternalOnly = true;
        }

        if (roles == null) {
            user.updateAllRights();

            return user;
        }

        // this clears all existing privileges of the user
        user.clearPrivileges();

        // assigns the roles to the user
        for (int i = 0; i < roles.length; i++) {
            try {
                Grantee role = granteeManager.getRole((String) roles[i]);

                user.grant(role);
            } catch (HsqlException e) {}
        }

        user.updateAllRights();

        for (int i = 0; i < roles.length; i++) {
            Schema schema = granteeManager.database.schemaManager.findSchema(
                (String) roles[i]);

            if (schema != null) {
                user.setInitialSchema(schema.getName());
                break;
            }
        }

        return user;
    }

    /**
     * Retrieves this object's set of User objects as
     *  an associative list.
     */
    public OrderedHashMap<String, User> getUsers() {
        return userList;
    }

    public boolean exists(String name) {
        return userList.get(name) == null
               ? false
               : true;
    }

    /**
     * Returns the User object identified by the
     * name argument.
     */
    public User get(String name) {

        User user = userList.get(name);

        if (user == null) {
            throw Error.error(ErrorCode.X_28501, name);
        }

        return user;
    }

    /**
     * Retrieves the {@code User} objects representing the database
     * users that are visible to the {@code User} object
     * represented by the {@code session} argument. <p>
     *
     * If the {@code session} argument's {@code User} object
     * attribute has isAdmin() true (directly or by virtue of a Role),
     * then all of the
     * {@code User} objects in this collection are considered visible.
     * Otherwise, only this object's special {@code PUBLIC}
     * {@code User} object attribute and the session {@code User}
     * object, if it exists in this collection, are considered visible. <p>
     *
     * @param session The {@code Session} object used to determine
     *          visibility
     * @return a list of {@code User} objects visible to
     *          the {@code User} object contained by the
     *         {@code session} argument.
     *
     */
    public HsqlArrayList<User> listVisibleUsers(Session session) {

        HsqlArrayList<User> list        = new HsqlArrayList<>();
        boolean             isAdmin     = session.isAdmin();
        String              sessionName = session.getUsername();

        if (userList == null || userList.isEmpty()) {
            return list;
        }

        for (int i = 0; i < userList.size(); i++) {
            User user = userList.get(i);

            if (user == null) {
                continue;
            }

            String userName = user.getName().getNameString();

            if (isAdmin) {
                list.add(user);
            } else if (sessionName.equals(userName)) {
                list.add(user);
            }
        }

        return list;
    }

    /**
     * Returns the specially constructed
     * {@code SYSTEM_AUTHORIZATION_NAME}
     * {@code User} object for the current {@code Database} object.
     *
     * @return the {@code SYS_AUTHORIZATION_NAME}
     *          {@code User} object
     *
     */
    public User getSysUser() {
        return GranteeManager.systemAuthorisation;
    }

    public synchronized void removeSchemaReference(HsqlName schemaName) {

        for (int i = 0; i < userList.size(); i++) {
            User     user   = userList.get(i);
            HsqlName schema = user.getInitialSchema();

            if (schema == null) {
                continue;
            }

            if (schemaName.equals(schema)) {
                user.setInitialSchema(null);
            }
        }
    }

    public void setPasswordCheckFunction(Routine function) {
        pwCheckFunction = function;
    }

    public void setExtAuthenticationFunction(Routine function) {
        extAuthenticationFunction = function;
    }

    public List<String> getInitialSchemaSQLArray() {

        HsqlArrayList<String> list = new HsqlArrayList<>(userList.size());

        for (int i = 0; i < userList.size(); i++) {
            User user = userList.get(i);

            if (user.isSystem) {
                continue;
            }

            HsqlName name = user.getInitialSchema();

            if (name == null) {
                continue;
            }

            list.add(user.getInitialSchemaSQL());
        }

        return list;
    }

    public List<String> getAuthenticationSQLArray() {

        HsqlArrayList<String> list = new HsqlArrayList<>();

        if (pwCheckFunction != null) {
            StringBuilder sb = new StringBuilder(64);

            sb.append(Tokens.T_SET)
              .append(' ')
              .append(Tokens.T_DATABASE)
              .append(' ')
              .append(Tokens.T_PASSWORD)
              .append(' ')
              .append(Tokens.T_CHECK)
              .append(' ')
              .append(Tokens.T_FUNCTION)
              .append(' ')
              .append(pwCheckFunction.getSQLBodyDefinition());
            list.add(sb.toString());
        }

        if (extAuthenticationFunction != null) {
            StringBuilder sb = new StringBuilder(64);

            sb.append(Tokens.T_SET)
              .append(' ')
              .append(Tokens.T_DATABASE)
              .append(' ')
              .append(Tokens.T_AUTHENTICATION)
              .append(' ')
              .append(Tokens.T_FUNCTION)
              .append(' ')
              .append(extAuthenticationFunction.getSQLBodyDefinition());
            list.add(sb.toString());
        }

        return list;
    }
}
