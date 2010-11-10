/* Copyright (c) 2001-2010, The HSQL Development Group
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

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.SchemaObject;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.Set;

/**
 * Interface for GranteeObject.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 *
 * @version 2.0.1
 * @since 2.0.1
*/
public interface Grantee extends SchemaObject {

    /**
     * Returns true if this Grantee can change to a different user.
     */
    boolean canChangeAuthorisation();

    void checkAccess(SchemaObject object);

    /**
     * Checks whether this Grantee has administrative privs either directly
     * or indirectly. Otherwise it throws.
     */
    void checkAdmin();

    void checkDelete(SchemaObject table);

    void checkInsert(SchemaObject table, boolean[] checkList);

    void checkReferences(SchemaObject table, boolean[] checkList);

    /**
     * Checks if a right represented by the methods
     * have been granted on the specified database object. <p>
     *
     * This is done by checking that a mapping exists in the rights map
     * from the dbobject argument. Otherwise, it throws.
     */
    void checkSelect(SchemaObject table, boolean[] checkList);

    void checkTrigger(SchemaObject table, boolean[] checkList);

    void checkUpdate(SchemaObject table, boolean[] checkList);

    /**
     * Checks if this object can modify schema objects or grant access rights
     * to them.
     */
    void checkSchemaUpdateOrGrantRights(String schemaName);

    OrderedHashSet getAllDirectPrivileges(SchemaObject object);

    OrderedHashSet getAllGrantedPrivileges(SchemaObject object);

    /**
     * Gets direct and indirect roles.
     */
    OrderedHashSet getAllRoles();

    OrderedHashSet getColumnsForAllPrivileges(SchemaObject object);

    /**
     * Gets direct roles, not roles nested within them.
     */
    OrderedHashSet getDirectRoles();

    OrderedHashSet getGranteeAndAllRoles();

    OrderedHashSet getGranteeAndAllRolesWithPublic();

    boolean hasNonSelectTableRight(SchemaObject object);

    boolean hasRole(Grantee role);

    /**
     * Checks if this object can modify schema objects or grant access rights
     * to them.
     */
    boolean hasSchemaUpdateOrGrantRights(String schemaName);

    boolean isAccessible(HsqlName name);

    boolean isAccessible(HsqlName name, int privilegeType);

    /**
     * returns true if grantee has any privilege (to any column) of the object
     */
    boolean isAccessible(SchemaObject object);

    /**
     * Returns true if this Grantee has administrative privs either directly
     * or indirectly.
     */
    boolean isAdmin();

    boolean isGrantable(SchemaObject object, Right right);

    boolean isGrantable(Grantee role);

    boolean isFullyAccessibleByRole(HsqlName name);

    /**
     * Returns true if this grantee object is for the PUBLIC role.
     */
    boolean isPublic();

    boolean isRole();

    /**
     * Returns true if this Grantee can create schemas with own authorization.
     */
    boolean isSchemaCreator();

    boolean isSystem();

    /**
     * Iteration of all visible grantees, including self. <p>
     *
     * For grantees with admin, this is all grantees.
     * For regular grantees, this is self plus all roles granted directly
     * or indirectly
     */
    Set visibleGrantees();
}
