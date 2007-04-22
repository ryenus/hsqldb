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

import org.hsqldb.HsqlException;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.NumberSequence;
import org.hsqldb.SchemaObject;
import org.hsqldb.Table;
import org.hsqldb.Token;
import org.hsqldb.Trace;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.Set;

/**
 * A Grantee Object holds the name, access and administrative rights for a
 * particular grantee.<p>
 * It supplies the methods used to grant, revoke, test
 * and check a grantee's access rights to other database objects.
 * It also holds a reference to the common PUBLIC User Object,
 * which represent the special user refered to in
 * GRANT ... TO PUBLIC statements.<p>
 * The check(), isAccessible() and getGrantedClassNames() methods check the
 * rights granted to the PUBLIC User Object, in addition to individually
 * granted rights, in order to decide which rights exist for the user.
 *
 * Method names ending in Direct indicate methods which do not recurse
 * to look through Roles which "this" object is a member of.
 *
 * We use the word "Admin" (e.g., in private variable "admin" and method
 * "isAdmin()) to mean this Grantee has admin priv by any means.
 * We use the word "adminDirect" (e.g., in private variable "adminDirect"
 * and method "isAdminDirect()) to mean this Grantee has admin priv
 * directly.
 *
 * @author boucherb@users
 * @author fredt@usrs
 * @author unsaved@users
 *
 * @version 1.9.0
 * @since 1.8.0
 */
public class Grantee {

    boolean isRole;

    /**
     * true if this grantee has database administrator priv directly
     *  (ie., not by membership in any role)
     */
    private boolean isAdminDirect = false;

    /** true if this grantee has database administrator priv by any means. */
    private boolean isAdmin = false;

    /** true if this grantee has database administrator priv by any means. */
    boolean isPublic = false;

    /** Grantee name. */
    private HsqlName granteeName;

    /** map with database object identifier keys and access privileges values */
    private HashMap directRightsMap;

    /** contains righs granted direct, or via roles, expept those of PUBLIC */
    private HashMap fullRightsMap;

    /** These are the DIRECT roles.  Each of these may contain nested roles */
    HashMap roles;;

    /** These are tables with column update rights */

    /** Needed only to give access to the roles for this database */
    private GranteeManager granteeManager;

    /**
     * Constructor.
     */
    Grantee(HsqlName name, GranteeManager man) {

        fullRightsMap   = new HashMap();
        directRightsMap = new HashMap();
        granteeName     = name;
        granteeManager  = man;
        roles           = new HashMap();
    }

    public String getName() {
        return granteeName.name;
    }

    public String getStatementName() {
        return granteeName.statementName;
    }

    public boolean isRole() {
        return isRole;
    }

    /**
     * Retrieves the map object that represents the rights that have been
     * granted on database objects.  <p>
     *
     * The map has keys and values with the following interpretation: <P>
     *
     * <UL>
     * <LI> The keys are generally (but not limited to) objects having
     *      an attribute or value equal to the name of an actual database
     *      object.
     *
     * <LI> Specifically, the keys act as database object identifiers.
     *
     * <LI> The values are always Integer objects, each formed by combining
     *      a set of flags, one for each of the access rights defined in
     *      UserManager: {SELECT, INSERT, UPDATE and DELETE}.
     * </UL>
     */
    public HashMap getRights() {

        // necessary to create the script
        return directRightsMap;
    }

    /**
     * Grant a role
     */
    public void grant(String role, boolean withGrant) throws HsqlException {

        Boolean option = withGrant ? Boolean.TRUE
                                   : null;

        roles.put(role, option);
    }

    /**
     * Revoke a direct role only
     */
    public void revoke(String role) throws HsqlException {

        if (!hasRoleDirect(role)) {
            throw Trace.error(Trace.DONT_HAVE_ROLE, role);
        }

        roles.remove(role);
    }

    /**
     * Gets direct roles, not roles nested within them.
     */
    public HashMap getDirectRoles() {
        return roles;
    }

    String getDirectRolesString() {
        return roleMapToString(roles);
    }

    String getAllRolesString() {
        return roleMapToString(getAllRoles());
    }

    public String roleMapToString(HashMap rolesMap) {

        // Should be sorted
        // Iterator it = (new java.util.TreeSet(roles)).iterator();
        Iterator     it = rolesMap.keySet().iterator();
        StringBuffer sb = new StringBuffer();

        while (it.hasNext()) {
            if (sb.length() > 0) {
                sb.append(',');
            }

            sb.append(it.next());
        }

        return sb.toString();
    }

    /**
     * Gets direct and nested roles.
     */
    public HashMap getAllRoles() {

        HashMap newMap = new HashMap();

        addGranteeAndRoles(newMap);

        // Since we added "Grantee" in addition to Roles, need to remove self.
        newMap.remove(granteeName.name);

        return newMap;
    }

    /**
     * Adds to given Set this.sName plus all roles and nested roles.
     *
     * @return Given role with new elements added.
     */
    private HashMap addGranteeAndRoles(HashMap map) {

        String candidateRole;

        map.put(granteeName.name, null);

        Iterator it = roles.keySet().iterator();

        while (it.hasNext()) {
            candidateRole = (String) it.next();

            if (!map.containsKey(candidateRole)) {
                try {
                    granteeManager.getRole(candidateRole).addGranteeAndRoles(
                        map);
                } catch (HsqlException he) {
                    throw Trace.runtimeError(
                        Trace.UNSUPPORTED_INTERNAL_OPERATION, he.getMessage());
                }
            }
        }

        return map;
    }

    public boolean hasRoleDirect(String role) {
        return roles.keySet().contains(role);
    }

    public boolean hasRole(String role) {
        return getAllRoles().keySet().contains(role);
    }

    public String allRolesString() {

        HashMap allRoles = getAllRoles();

        if (allRoles.size() < 1) {
            return null;
        }

        Iterator     it = getAllRoles().keySet().iterator();
        StringBuffer sb = new StringBuffer();

        while (it.hasNext()) {
            if (sb.length() > 0) {
                sb.append(',');
            }

            sb.append((String) it.next());
        }

        return sb.toString();
    }

    /**
     * Grants the specified rights on the specified database object. <p>
     *
     * Keys stored in rightsMap for database tables are their HsqlName
     * attribute. This allows rights to persist when a table is renamed. <p>
     */
    void grant(SchemaObject object, Right right, String grantor,
               boolean withGrant) {

        HsqlName name     = object.getName();
        Right    existing = (Right) directRightsMap.get(name);

        if (existing == null || right.isFull) {
            directRightsMap.put(name, right);
        } else {
            existing.add(right);
        }
    }

    /**
     * Revokes the specified rights on the specified database object. <p>
     *
     * If, after removing the specified rights, no rights remain on the
     * database object, then the key/value pair for that object is removed
     * from the rights map
     */
    void revoke(SchemaObject object, Right right) {

        HsqlName name     = object.getName();
        Right    existing = (Right) directRightsMap.get(name);

        if (existing == null) {
            return;
        }

        if (right.isFull) {
            directRightsMap.remove(name);

            return;
        }

        if (existing.isFull) {
            existing = new Right((Table) object);

            directRightsMap.put(name, existing);
        }

        existing.remove(right);

        if (existing.isEmpty()) {
            directRightsMap.remove(name);
        }
    }

    /**
     * Revokes all rights on the specified database object.<p>
     *
     * This method removes any existing mapping from the rights map
     */
    void revokeDbObject(SchemaObject object) {

        HsqlName name = object.getName();

        directRightsMap.remove(name);
        fullRightsMap.remove(name);
    }

    /**
     * Revokes all rights from this Grantee object.  The map is cleared and
     * the database administrator role attribute is set false.
     */
    void clearPrivileges() {

        roles.clear();
        directRightsMap.clear();
        fullRightsMap.clear();

        isAdminDirect = false;
        isAdmin       = false;
    }

    /**
     * Checks if a right represented by the methods
     * have been granted on the specified database object. <p>
     *
     * This is done by checking that a mapping exists in the rights map
     * from the dbobject argument. Otherwise, it throws.
     */
    public void checkSelect(Table table,
                            boolean[] checkList) throws HsqlException {

        if (isFullyAccessibleByRole(table)) {
            return;
        }

        Right right = (Right) fullRightsMap.get(table.getName());

        if (right != null && right.canSelect(table, checkList)) {
            return;
        }

        if (!isPublic) {
            if (granteeManager.publicRole.isFullyAccessibleByRole(table)) {
                return;
            }

            right = (Right) granteeManager.publicRole.fullRightsMap.get(
                table.getName());

            if (right != null && right.canSelect(table, checkList)) {
                return;
            }
        }

        throw Trace.error(Trace.ACCESS_IS_DENIED);
    }

    public void checkInsert(Table table,
                            boolean[] checkList) throws HsqlException {

        if (isFullyAccessibleByRole(table)) {
            return;
        }

        Right right = (Right) fullRightsMap.get(table.getName());

        if (right != null && right.canInsert(table, checkList)) {
            return;
        }

        if (!isPublic) {
            if (granteeManager.publicRole.isFullyAccessibleByRole(table)) {
                return;
            }

            right = (Right) granteeManager.publicRole.fullRightsMap.get(
                table.getName());

            if (right != null && right.canInsert(table, checkList)) {
                return;
            }
        }

        throw Trace.error(Trace.ACCESS_IS_DENIED);
    }

    public void checkUpdate(Table table,
                            boolean[] checkList) throws HsqlException {

        if (isFullyAccessibleByRole(table)) {
            return;
        }

        Right right = (Right) fullRightsMap.get(table.getName());

        if (right != null && right.canUpdate(table, checkList)) {
            return;
        }

        if (!isPublic) {
            if (granteeManager.publicRole.isFullyAccessibleByRole(table)) {
                return;
            }

            right = (Right) granteeManager.publicRole.fullRightsMap.get(
                table.getName());

            if (right != null && right.canUpdate(table, checkList)) {
                return;
            }
        }

        throw Trace.error(Trace.ACCESS_IS_DENIED);
    }

    public void checkDelete(Table table) throws HsqlException {

        if (isFullyAccessibleByRole(table)) {
            return;
        }

        Right right = (Right) fullRightsMap.get(table.getName());

        if (right != null && right.canDelete()) {
            return;
        }

        if (!isPublic) {
            if (granteeManager.publicRole.isFullyAccessibleByRole(table)) {
                return;
            }

            right = (Right) granteeManager.publicRole.fullRightsMap.get(
                table.getName());

            if (right != null && right.canDelete()) {
                return;
            }
        }

        throw Trace.error(Trace.ACCESS_IS_DENIED);
    }

    public void checkAccess(SchemaObject object) throws HsqlException {

        if (isFullyAccessibleByRole(object)) {
            return;
        }

        Right right = (Right) fullRightsMap.get(object.getName());

        if (right != null) {
            return;
        }

        if (!isPublic) {
            if (granteeManager.publicRole.isFullyAccessibleByRole(object)) {
                return;
            }

            right = (Right) granteeManager.publicRole.fullRightsMap.get(
                object.getName());

            if (right != null) {
                return;
            }
        }

        throw Trace.error(Trace.ACCESS_IS_DENIED);
    }

    public void checkAccess(String object) throws HsqlException {

        if (!isAccessible(object)) {
            throw Trace.error(Trace.ACCESS_IS_DENIED);
        }
    }

    /**
     * Checks if this object can modify schema objects or grant access rights
     * to them.
     */
    public void checkSchemaUpdateOrGrantRights(String schemaName)
    throws HsqlException {

        if (schemaName == null) {
            throw Trace.runtimeError(Trace.ASSERT_FAILED, "Grantee");
        }

        // If a DBA
        if (isAdmin()) {
            return;
        }

        Grantee schemaOwner =
            granteeManager.db.schemaManager.toSchemaOwner(schemaName);

        // If owner of Schema
        if (schemaOwner == this) {
            return;
        }

        // If a member of Schema authorization role
        if (hasRole(schemaOwner.getName())) {
            return;
        }

        throw Trace.error(Trace.NOT_AUTHORIZED, "Update schema " + schemaName);
    }

    public boolean isFullyAccessibleByRole(SchemaObject object) {

        if (isAdmin) {
            return true;
        }

        Grantee owner = object.getOwner();

        if (owner == this) {
            return true;
        }

        if (hasRole(owner.getName())) {
            return true;
        }

        return false;
    }

    /**
     * Returns true if any right at all has been granted to this User object
     * on the database object identified by the dbObject argument.
     */
    public boolean isAccessible(String functionName) throws HsqlException {

        if (functionName.startsWith("org.hsqldb.Library")
                || functionName.startsWith("java.lang.Math")) {
            return true;
        }

        if (isAdmin) {
            return true;
        }

        if (!isPublic
                && granteeManager.publicRole.isAccessible(functionName)) {
            return true;
        }

        Right right = (Right) fullRightsMap.get(functionName);

        return right != null;
    }

    /**
     * Returns true if any of the rights represented by the
     * rights argument has been granted on the database object identified
     * by the dbObject argument. <p>
     *
     * This is done by checking that a mapping exists in the rights map
     * from the dbObject argument for at least one of the rights
     * contained in the rights argument.
     *
     * Considers none of pubGranee, nested roles, admin privs, globally
     * available Class object.
     */
    protected boolean isDirectlyAccessible(Object dbObject,
                                           int rights) throws HsqlException {

        Right right = (Right) directRightsMap.get(dbObject);

        return false;
    }

    /**
     * Checks whether this Grantee has administrative privs either directly
     * or indirectly. Otherwise it throws.
     */
    public void checkAdmin() throws HsqlException {

        if (!isAdmin()) {
            throw Trace.error(Trace.ACCESS_IS_DENIED);
        }
    }

    /**
     * Returns true if this Grantee has administrative privs either directly
     * or indirectly.
     */
    public boolean isAdmin() {
        return isAdmin;
    }

    /**
     * Returns true if this grantee object is for a user with Direct
     * database administrator privileges.
     * I.e., if this User/Role has Admin priv. directly, not via a
     * nested Role.
     */
    boolean isAdminDirect() {
        return isAdminDirect;
    }

    /**
     * Returns true if this grantee object is for the PUBLIC role.
     */
    public boolean isPublic() {
        return isPublic;
    }

    /**
     * Retrieves the distinct set of Java <code>Class</code> FQNs
     * for which this <code>User</code> object has been
     * granted <code>ALL</code> (the Class execution privilege). <p>
     * @param andToPublic if <code>true</code>, then the set includes the
     *        names of classes accessible to this <code>User</code> object
     *        through grants to its Roles + <code>PUBLIC</code>
     *        <code>User</code> object attribute, else only role grants
     *        + direct grants are included.
     * @return the distinct set of Java Class FQNs for which this
     *        this <code>User</code> object has been granted
     *        <code>ALL</code>.
     */
    public OrderedHashSet getGrantedClassNames(boolean andToPublic)
    throws HsqlException {

        HashMap  rights;
        Object   key;
        Right    right;
        Iterator i;

        rights = directRightsMap;

        OrderedHashSet out = getGrantedClassNamesDirect();

        if (andToPublic && !isPublic) {
            rights = granteeManager.publicRole.directRightsMap;
            i      = rights.keySet().iterator();

            while (i.hasNext()) {
                key = i.next();

                if (key instanceof String) {
                    right = (Right) rights.get(key);

                    if (right.isFull) {
                        out.add(key);
                    }
                }
            }
        }

        Iterator it = getAllRoles().keySet().iterator();

        while (it.hasNext()) {
            out.addAll(
                ((Grantee) granteeManager.getRole(
                    (String) it.next())).getGrantedClassNamesDirect());
        }

        return out;
    }

    /**
     * Retrieves the distinct set of Java <code>Class</code> FQNs
     * for which this <code>User</code> object has directly been
     * granted <code>EXECUTE</code> (the Class execution privilege).
     *
     * Does NOT check nested the pubGrantee nor nested roles.
     * @return the distinct set of Java Class FQNs for which this
     *        this <code>User</code> object has been granted
     *        <code>ALL</code>.
     *
     */
    public OrderedHashSet getGrantedClassNamesDirect() throws HsqlException {

        OrderedHashSet out = new OrderedHashSet();
        Iterator       it  = directRightsMap.keySet().iterator();
        Object         key;

        while (it.hasNext()) {
            key = it.next();

            if (key instanceof String) {
                out.add(key);
            }
        }

        return out;
    }

    /**
     * Violates naming convention (for backward compatibility).
     * Should be "setAdminDirect(boolean").
     */
    void setAdminDirect() {
        isAdmin = isAdminDirect = true;
    }

    /**
     * Recursive method used with ROLE Grantee objects to set the fullRightsMap
     * and admin flag for all the roles.
     *
     * If a new ROLE is granted to a ROLE Grantee object, the ROLE should first
     * be added to the Set of ROLE Grantee objects (roles) for the grantee.
     * The grantee will be the parameter.
     *
     * If the direct permissions granted to an existing ROLE Grentee is
     * modified no extra initial action is necessary.
     * The existing Grantee will be the parameter.
     *
     * If an existing ROLE is REVOKEed from a ROLE, it should first be removed
     * from the set of ROLE Grantee objects in the containing ROLE.
     * The containing ROLE will be the parameter.
     *
     * If an existing ROLE is DROPped, all its privileges should be cleared
     * first. The ROLE will be the parameter. After calling this method on
     * all other roles, the DROPped role should be removed from all grantees.
     *
     * After the initial modification, this method should be called iteratively
     * on all the ROLE Grantee objects contained in RoleManager.
     *
     * The updateAllRights() method is then called iteratively on all the
     * USER Grantee objects contained in UserManager.
     * @param role a modified, revoked or dropped role.
     * @return true if this Grantee has possibly changed as a result
     */
    boolean updateNestedRoles(String role) {

        boolean hasNested = false;
        boolean isSelf    = role.equals(granteeName.name);

        if (!isSelf) {
            Iterator it = roles.keySet().iterator();

            while (it.hasNext()) {
                String roleName = (String) it.next();

                try {
                    Grantee currentRole = granteeManager.getRole(roleName);

                    hasNested |= currentRole.updateNestedRoles(role);
                } catch (HsqlException e) {}
            }
        }

        if (hasNested) {
            updateAllRights();
        }

        return hasNested || isSelf;
    }

    /**
     * Method used with all Grantee objects to set the full set of rights
     * according to those inherited form ROLE Grantee objects and those
     * granted to the object itself.
     */
    void updateAllRights() {

        fullRightsMap.clear();

        isAdmin = isAdminDirect;

        Iterator it = roles.keySet().iterator();

        while (it.hasNext()) {
            String roleName = (String) it.next();

            try {
                Grantee currentRole = granteeManager.getRole(roleName);

                addToFullRights(currentRole.fullRightsMap);

                isAdmin |= currentRole.isAdmin();
            } catch (HsqlException e) {}
        }

        addToFullRights(directRightsMap);
    }

    /**
     * Full or partial rights are added to existing
     */
    void addToFullRights(HashMap map) {

        Iterator it = map.keySet().iterator();

        while (it.hasNext()) {
            Object key      = it.next();
            Right  add      = (Right) map.get(key);
            Right  existing = (Right) fullRightsMap.get(key);

            if (existing == null) {
                fullRightsMap.put(key, add.duplicate());

                continue;
            }

            if (existing.isFull) {
                continue;
            }

            existing.add(add);
        }
    }

    /**
     * Iteration of all visible grantees, including self. <p>
     *
     * For grantees with admin, this is all grantees.
     * For regular grantees, this is self plus all roles granted directly
     * or indirectly
     */
    public Set visibleGrantees() throws HsqlException {

        HashSet        grantees = new HashSet();
        GranteeManager gm       = granteeManager;

        if (isAdmin()) {
            grantees.addAll(gm.getGrantees());
        } else {
            grantees.add(this);

            for (Iterator it =
                    getAllRoles().keySet().iterator(); it.hasNext(); ) {
                grantees.add(gm.getRole((String) it.next()));
            }
        }

        return grantees;
    }

    /**
     * Set of all non-reserved visible grantees, including self. <p>
     *
     * For grantees with admin, this is all grantees.
     * For regular grantees, this is self plus all roles granted directly
     * or indirectly. <P>
     *
     * @param andPublic when <tt>true</tt> retains the reserved PUBLIC grantee
     */
    public Set nonReservedVisibleGrantees(boolean andPublic)
    throws HsqlException {

        Set            grantees = visibleGrantees();
        GranteeManager gm       = granteeManager;

        grantees.remove(gm.dbaRole);
        grantees.remove(gm.systemRole);

        if (!andPublic) {
            grantees.remove(gm.publicRole);
        }

        return grantees;
    }

// support for legacy system tables where only table-level granulity is supported
    public String[] getFullTableRightsArray() {
        return Right.fullRights.getTableRightsArray();
    }

    private static String[] noRightsArray = new String[0];

    public String[] getRightsArray(Table table) {

        if (isAdmin()) {
            return getFullTableRightsArray();
        }

        Right right = (Right) fullRightsMap.get(table.getName());

        if (right == null) {
            return noRightsArray;
        }

        return right.getTableRightsArray();
    }

    public boolean isAccessible(SchemaObject object, int privilegeType) {

        if (isFullyAccessibleByRole(object)) {
            return true;
        }

        Right right = (Right) fullRightsMap.get(object.getName());

        if (right == null) {
            return false;
        }

        return right.canAccess(object, privilegeType);
    }

    /**
     * returns true if grantee has any privilege (to any column) of the object
     */
    public boolean isAccessible(SchemaObject object) {

        if (isFullyAccessibleByRole(object)) {
            return true;
        }

        Right right = (Right) fullRightsMap.get(object.getName());

        if (right != null && !right.isEmpty()) {
            return true;
        }

        return false;
    }

// end legacy support
    public HsqlArrayList getRightsDDL() {

        HsqlArrayList list       = new HsqlArrayList();
        String        roleString = allRolesString();

        if (roleString != null) {
            list.add("GRANT " + roleString + " TO " + getStatementName());
        }

        HashMap rightsmap = getRights();

        if (rightsmap == null) {
            return list;
        }

        Iterator dbobjects = rightsmap.keySet().iterator();

        while (dbobjects.hasNext()) {
            Object       nameobject = dbobjects.next();
            Right        right      = (Right) rightsmap.get(nameobject);
            StringBuffer a          = new StringBuffer(64);

            if (nameobject instanceof String) {

                // permissions to internal methods are granted to PUBLIC
                // these need not be persisted
                if (nameobject.equals("java.lang.Math")
                        || nameobject.equals("org.hsqldb.Library")) {
                    continue;
                }

                a.append(Token.T_GRANT).append(' ');
                a.append(right.getMethodRightsDDL());
                a.append(' ').append(Token.T_ON).append(' ');
                a.append("CLASS \"");
                a.append((String) nameobject);
                a.append('\"');
            } else {
                HsqlName hsqlname = (HsqlName) nameobject;
                Table table =
                    granteeManager.db.schemaManager.findUserTable(null,
                        hsqlname.name, hsqlname.schema.name);

                if (table != null) {
                    a.append(Token.T_GRANT).append(' ');
                    a.append(right.getTableRightsDDL(table));
                    a.append(' ').append(Token.T_ON).append(' ');
                    a.append("TABLE ").append(
                        hsqlname.schema.statementName).append('.').append(
                        hsqlname.statementName);
                }

                NumberSequence sequence =
                    granteeManager.db.schemaManager.findUserSequence(
                        hsqlname.name, hsqlname.schema.name);

                if (sequence != null) {
                    a.append(Token.T_GRANT).append(' ');
                    a.append(right.getSequenceRightsDDL());
                    a.append(' ').append(Token.T_ON).append(' ');
                    a.append("SEQUENCE ").append(
                        hsqlname.schema.statementName).append('.').append(
                        hsqlname.statementName);
                }

                // permissions to some INFORMATION_SCHEMA tables are granted
                // to PUBLIC. These are not persisted.
            }

            if (a.length() == 0) {
                continue;
            }

            a.append(' ').append(Token.T_TO).append(' ');
            a.append(getStatementName());
            list.add(a.toString());
        }

        return list;
    }
}
