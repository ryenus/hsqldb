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


package org.hsqldb.rights;

import org.hsqldb.Database;
import org.hsqldb.HsqlException;
import org.hsqldb.HsqlNameManager;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.SchemaObject;
import org.hsqldb.Trace;
import org.hsqldb.lib.Collection;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.IntValueHashMap;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.Set;

/**
 * Contains a set of Grantee objects, and supports operations for creating,
 * finding, modifying and deleting Grantee objects for a Database; plus
 * Administrative privileges.
 *
 *
 * @author boucherb@users
 * @author fredt@users
 * @author unsaved@users
 *
 * @version 1.8.0
 * @since 1.8.0
 * @see Grantee
 */
public class GranteeManager {

    /**
     * The role name reserved for authorization of INFORMATION_SCHEMA and
     * system objects.
     */
    public static final String SYSTEM_AUTHORIZATION_NAME = "_SYSTEM";

    /** The role name reserved for ADMIN users. */
    public static final String DBA_ADMIN_ROLE_NAME = "DBA";

    /** The role name reserved for the special PUBLIC pseudo-user. */
    public static final String PUBLIC_ROLE_NAME = "PUBLIC";

    /**
     * An empty list that is returned from
     * {@link #listTablePrivileges listTablePrivileges} when
     * it is detected that neither this <code>User</code> object or
     * its <code>PUBLIC</code> <code>User</code> object attribute have been
     * granted any rights on the <code>Table</code> object identified by
     * the specified <code>HsqlName</code> object.
     *
     */
    static final String[] emptyRightsList = new String[0];

    /**
     * Map of grantee-String-to-Grantee-objects.<p>
     * Keys include all USER and ROLE names
     */
    private HashMappedList map = new HashMappedList();

    /**
     * Map of role-Strings-to-Grantee-object.<p>
     * Keys include all ROLES names
     */
    private HashMappedList roleMap = new HashMappedList();

    /**
     * Used only to pass the SchemaManager to Grantees for checking
     * schema authorizations.
     */
    Database db;

    /**
     * The grantee object for the PUBLIC role.
     */
    Grantee publicRole;

    /**
     * The grantee object for the _SYSTEM role.
     */
    static User systemRole;

    static {
        HsqlName name = HsqlNameManager.newHsqlSystemObjectName(
            SYSTEM_AUTHORIZATION_NAME);

        systemRole        = new User(name, null);
        systemRole.isRole = true;

        systemRole.setAdminDirect();
    }

    /**
     * The grantee object for the DBA role.
     */
    Grantee dbaRole;

    /**
     * Construct the GranteeManager for a Database.
     *
     * Construct special Grantee objects for _SYSTEM, PUBLIC and DBA, and add
     * them to the Grantee map.
     *
     * @param inDatabase Only needed to link to the RoleManager later on.
     */
    public GranteeManager(Database database) throws HsqlException {

        db = database;

        map.add(systemRole.getName(), systemRole);
        roleMap.add(systemRole.getName(), systemRole);
        addRole(db.nameManager.newHsqlName(GranteeManager.PUBLIC_ROLE_NAME));

        publicRole          = getRole(GranteeManager.PUBLIC_ROLE_NAME);
        publicRole.isPublic = true;

        addRole(
            db.nameManager.newHsqlName(GranteeManager.DBA_ADMIN_ROLE_NAME));

        dbaRole = getRole(GranteeManager.DBA_ADMIN_ROLE_NAME);

        dbaRole.setAdminDirect();
    }

    static final IntValueHashMap rightsStringLookup = new IntValueHashMap(7);

    static {
        rightsStringLookup.put(GrantConstants.S_R_ALL, GrantConstants.ALL);
        rightsStringLookup.put(GrantConstants.S_R_SELECT,
                               GrantConstants.SELECT);
        rightsStringLookup.put(GrantConstants.S_R_UPDATE,
                               GrantConstants.UPDATE);
        rightsStringLookup.put(GrantConstants.S_R_DELETE,
                               GrantConstants.DELETE);
        rightsStringLookup.put(GrantConstants.S_R_INSERT,
                               GrantConstants.INSERT);
        rightsStringLookup.put(GrantConstants.S_R_EXECUTE,
                               GrantConstants.EXECUTE);
        rightsStringLookup.put(GrantConstants.S_R_USAGE,
                               GrantConstants.USAGE);
        rightsStringLookup.put(GrantConstants.S_R_REFERENCES,
                               GrantConstants.REFERENCES);
        rightsStringLookup.put(GrantConstants.S_R_TRIGGER,
                               GrantConstants.TRIGGER);
    }

    public Grantee getDBARole() {
        return dbaRole;
    }

    public static Grantee getSystemRole() {
        return systemRole;
    }

    /**
     * Grants the rights represented by the rights argument on
     * the database object identified by the dbobject argument
     * to the Grantee object identified by name argument.<p>
     *
     *  Note: For the dbobject argument, Java Class objects are identified
     *  using a String object whose value is the fully qualified name
     *  of the Class, while Table and other objects are
     *  identified by an HsqlName object.  A Table
     *  object identifier must be precisely the one obtained by calling
     *  table.getName(); if a different HsqlName
     *  object with an identical name attribute is specified, then
     *  rights checks and tests will fail, since the HsqlName
     *  class implements its {@link HsqlName#hashCode hashCode} and
     *  {@link HsqlName#equals equals} methods based on pure object
     *  identity, rather than on attribute values. <p>
     */
    public void grant(HsqlArrayList granteeList, SchemaObject dbObject,
                      Right right, String grantor,
                      boolean withGrant) throws HsqlException {

        Grantee c = get(grantor);

        if (!c.isFullyAccessibleByRole(dbObject)) {
            throw Trace.error(Trace.ACCESS_IS_DENIED);
        }

        for (int i = 0; i < granteeList.size(); i++) {
            String  name = (String) granteeList.get(i);
            Grantee g    = get(name);

            if (g == null) {
                throw Trace.error(Trace.NO_SUCH_GRANTEE, name);
            }

            if (isImmutable(name)) {
                throw Trace.error(Trace.NONMOD_GRANTEE, name);
            }
        }

        for (int i = 0; i < granteeList.size(); i++) {
            String  name = (String) granteeList.get(i);
            Grantee g    = get(name);

            g.grant(dbObject, right, grantor, withGrant);
            g.updateAllRights();

            if (g.isRole) {
                updateAllRights(g);
            }
        }
    }

    public void grant(String name, SchemaObject dbObject, Right rights,
                      String grantor,
                      boolean withGrant) throws HsqlException {

        Grantee g = get(name);

        if (g == null) {
            throw Trace.error(Trace.NO_SUCH_GRANTEE, name);
        }

        if (isImmutable(name)) {
            throw Trace.error(Trace.NONMOD_GRANTEE, name);
        }

        // check columnList and grantor, withGrant
        g.grant(dbObject, rights, grantor, withGrant);
        g.updateAllRights();

        if (g.isRole) {
            updateAllRights(g);
        }
    }

    /**
     * Grant a role to this Grantee.
     */
    public void grant(String name, String role,
                      String grantor) throws HsqlException {

        Grantee c = get(grantor);

        if (!c.isAdmin()) {
            throw Trace.error(Trace.ACCESS_IS_DENIED);
        }

        Grantee grantee = get(name);

        if (grantee == null) {
            throw Trace.error(Trace.NO_SUCH_GRANTEE, name);
        }

        if (isImmutable(name)) {
            throw Trace.error(Trace.NONMOD_GRANTEE, name);
        }

        Grantee r = get(role);

        if (r == null) {
            throw Trace.error(Trace.NO_SUCH_ROLE, role);
        }

        if (r == grantee) {
            throw Trace.error(Trace.CIRCULAR_GRANT, name);
        }

        // boucherb@users 20050515
        // SQL 2003 Foundation, 4.34.3
        // No cycles of role grants are allowed.
        if (r.hasRole(name)) {

            // boucherb@users
            // TODO: Correct reporting of actual grant path
            throw Trace.error(Trace.CIRCULAR_GRANT,
                              Trace.getMessage(Trace.ALREADY_HAVE_ROLE)
                              + " GRANT " + name + " TO " + role);
        }

        if (grantee.getDirectRoles().containsKey(role)) {
            throw Trace.error(Trace.ALREADY_HAVE_ROLE, role);
        }

        grantee.grant(role, false);
        grantee.updateAllRights();

        if (grantee.isRole) {
            updateAllRights(grantee);
        }
    }

    /**
     * Revoke a role from a Grantee
     */
    public void revoke(String name, String role,
                       String grantor) throws HsqlException {

        Grantee c = get(grantor);

        if (!c.isAdmin()) {
            throw Trace.error(Trace.ACCESS_IS_DENIED);
        }

        Grantee g = get(name);

        if (g == null) {
            throw Trace.error(Trace.NO_SUCH_GRANTEE, name);
        }

        g.revoke(role);
        g.updateAllRights();

        if (g.isRole) {
            updateAllRights(g);
        }
    }

    /**
     * Revokes the rights represented by the rights argument on
     * the database object identified by the dbobject argument
     * from the User object identified by the name
     * argument.<p>
     * @see #grant
     */
    public void revoke(HsqlArrayList granteeList, SchemaObject dbObject,
                       Right rights, String grantor) throws HsqlException {

        Grantee c = get(grantor);

        if (!c.isFullyAccessibleByRole(dbObject)) {
            throw Trace.error(Trace.ACCESS_IS_DENIED);
        }

        for (int i = 0; i < granteeList.size(); i++) {
            String  name = (String) granteeList.get(i);
            Grantee g    = get(name);

            if (g == null) {
                throw Trace.error(Trace.NO_SUCH_GRANTEE, name);
            }

            if (isImmutable(name)) {
                throw Trace.error(Trace.NONMOD_GRANTEE, name);
            }
        }

        for (int i = 0; i < granteeList.size(); i++) {
            String name = (String) granteeList.get(i);

            revoke(name, dbObject, rights);
        }
    }

    private void revoke(String name, SchemaObject dbObject,
                        Right rights) throws HsqlException {

        Grantee g = get(name);

        g.revoke(dbObject, rights);
        g.updateAllRights();

        if (g.isRole) {
            updateAllRights(g);
        }
    }

    /**
     * Removes a role without any privileges from all grantees
     */
    void removeEmptyRole(Grantee role) {

        String name = role.getName();

        for (int i = 0; i < map.size(); i++) {
            Grantee grantee = (Grantee) map.get(i);

            grantee.roles.remove(name);
        }
    }

    /**
     * Removes all rights mappings for the database object identified by
     * the dbobject argument from all Grantee objects in the set.
     */
    public void removeDbObject(SchemaObject dbobject) {

        for (int i = 0; i < map.size(); i++) {
            Grantee g = (Grantee) map.get(i);

            g.revokeDbObject(dbobject);
        }
    }

    /**
     * First updates all ROLE Grantee objects. Then updates all USER Grantee
     * Objects.
     */
    void updateAllRights(Grantee role) {

        String name = role.getName();

        for (int i = 0; i < map.size(); i++) {
            Grantee grantee = (Grantee) map.get(i);

            if (grantee.isRole) {
                grantee.updateNestedRoles(name);
            }
        }

        for (int i = 0; i < map.size(); i++) {
            Grantee grantee = (Grantee) map.get(i);

            if (!grantee.isRole) {
                grantee.updateAllRights();
            }
        }
    }

    /**
     */
    public boolean removeGrantee(String name) {

        /*
         * Explicitly can't remove PUBLIC_USER_NAME and system grantees.
         */
        if (isReserved(name)) {
            return false;
        }

        Grantee g = (Grantee) map.remove(name);

        if (g == null) {
            return false;
        }

        g.clearPrivileges();
        updateAllRights(g);

        if (g.isRole) {
            roleMap.remove(name);
            removeEmptyRole(g);
        }

        return true;
    }

    /**
     * Creates a new Role object under management of this object. <p>
     *
     *  A set of constraints regarding user creation is imposed: <p>
     *
     *  <OL>
     *    <LI>Can't create a role with name same as any right.
     *
     *    <LI>If this object's collection already contains an element whose
     *        name attribute equals the name argument, then
     *        a GRANTEE_ALREADY_EXISTS or ROLE_ALREADY_EXISTS Trace
     *        is thrown.
     *        (This will catch attempts to create Reserved grantee names).
     *  </OL>
     */
    public Grantee addRole(HsqlName name) throws HsqlException {

        if (map.containsKey(name.name)) {
            throw Trace.error(Trace.GRANTEE_ALREADY_EXISTS, name.name);
        }

        Grantee g = new Grantee(name, this);

        g.isRole = true;

        map.put(name.name, g);
        roleMap.add(name.name, g);

        return g;
    }

    public User addUser(HsqlName name) throws HsqlException {

        // reject action names (ALL, SELECT, DELETE, USAGE, etc.)
        if (GranteeManager.validRightString(name.name)) {
            throw Trace.error(Trace.ILLEGAL_ROLE_NAME, name.name);
        }

        if (map.containsKey(name.name)) {
            throw Trace.error(Trace.GRANTEE_ALREADY_EXISTS, name.name);
        }

        User g = new User(name, this);

        map.put(name.name, g);

        return g;
    }

    /**
     * Returns true if named Grantee object exists.
     * This will return true for reserved Grantees
     * SYSTEM_AUTHORIZATION_NAME, ADMIN_ROLE_NAME, PUBLIC_USER_NAME.
     */
    boolean isGrantee(String name) {
        return (map.containsKey(name));
    }

    public static int getCheckSingleRight(String right) throws HsqlException {

        int r = getRight(right);

        if (r != 0) {
            return r;
        }

        throw Trace.error(Trace.NO_SUCH_RIGHT, right);
    }

    /**
     * Translate a string representation or right(s) into its numeric form.
     */
    public static int getRight(String right) {
        return rightsStringLookup.get(right, 0);
    }

    /**
     * Retrieves the set of distinct, fully qualified Java <code>Class</code>
     * names upon which any grants currently exist to elements in
     * this collection. <p>
     * @return the set of distinct, fully qualified Java Class names, as
     *        <code>String</code> objects, upon which grants currently exist
     *        to the elements of this collection
     *
     */
    HashSet getGrantedClassNames() throws HsqlException {

        int      size;
        Grantee  grantee;
        HashSet  out;
        Iterator it;

        size = map.size();
        out  = new HashSet();

        for (int i = 0; i < size; i++) {
            grantee = (Grantee) map.get(i);

            if (grantee == null) {
                continue;
            }

            it = grantee.getGrantedClassNames(false).iterator();

            while (it.hasNext()) {
                out.add(it.next());
            }
        }

        return out;
    }

    public Grantee get(String name) {
        return (Grantee) map.get(name);
    }

    public Collection getGrantees() {
        return map.values();
    }

    public static boolean validRightString(String rightString) {
        return getRight(rightString) != 0;
    }

    public static boolean isImmutable(String name) {
        return name.equals(SYSTEM_AUTHORIZATION_NAME)
               || name.equals(DBA_ADMIN_ROLE_NAME);
    }

    public static boolean isReserved(String name) {

        return name.equals(SYSTEM_AUTHORIZATION_NAME)
               || name.equals(DBA_ADMIN_ROLE_NAME)
               || name.equals(PUBLIC_ROLE_NAME);
    }

    /**
     * Attempts to drop a Role with the specified name
     *  from this object's set. <p>
     *
     *  A successful drop action consists of: <p>
     *
     *  <UL>
     *
     *    <LI>removing the Grantee object with the specified name
     *        from the set.
     *  </UL> <p>
     *
     */
    public void dropRole(String name) throws HsqlException {

        if (!isRole(name)) {
            throw Trace.error(Trace.NO_SUCH_ROLE, name);
        }

        if (GranteeManager.isReserved(name)) {
            throw Trace.error(Trace.ACCESS_IS_DENIED);
        }

        removeGrantee(name);
        roleMap.remove(name);
    }

    public Set getRoleNames() {
        return roleMap.keySet();
    }

    /**
     * Returns Grantee for the named Role
     */
    public Grantee getRole(String name) throws HsqlException {

        Grantee g = (Grantee) roleMap.get(name);

        if (g == null) {
            throw Trace.error(Trace.MISSING_GRANTEE, name);
        }

        return g;
    }

    public boolean isRole(String name) throws HsqlException {
        return roleMap.containsKey(name);
    }
}
