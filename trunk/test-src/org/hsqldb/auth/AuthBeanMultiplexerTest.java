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


package org.hsqldb.auth;

import java.sql.Array;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import org.hsqldb.jdbc.JDBCArrayBasic;

public class AuthBeanMultiplexerTest extends junit.framework.TestCase {
    private static final String[] twoRoles = new String[] { "role1", "role2" };
    private static final AuthTriggerBean nullPermittingAuthTriggerBean =
            new AuthTriggerBean() {
        public String[] authenticate(
                String dbName, String userName, String password) {
            return null;
        }
    };
    private static final AuthTriggerBean twoRolePermittingAuthTriggerBean =
            new AuthTriggerBean() {
        public String[] authenticate(
                String dbName, String userName, String password) {
            return twoRoles;
        }
    };
    private static final AuthTriggerBean purposefullyBrokenAuthTriggerBean =
            new AuthTriggerBean() {
        public String[] authenticate(
                String dbName, String userName, String password) {
            throw new RuntimeException("Emulating broken AuthTriggerBean");
        }
    };
    private static final AuthTriggerBean denyingAuthTriggerBean =
            new AuthTriggerBean() {
        public String[] authenticate(
                String dbName, String userName, String password)
                throws Exception {
            throw new Exception("Deny!");
        }
    };

    /**
     * @throws RutnimeException if jab
     *         param is neither null not an instance of JDBCArrayBasic wrapping
     *         an array of Strings.
     */
    private static String[] toStrings(Array jab) {
        if (jab == null) {
            return null;
        }
        if (!(jab instanceof JDBCArrayBasic)) {
            throw new IllegalArgumentException(
                    "Parameter is a " + jab.getClass().getName()
                    + " instead of a " + JDBCArrayBasic.class.getName());
        }
        Object internalArray = null;
        try {
            internalArray = ((JDBCArrayBasic) jab).getArray();
        } catch (SQLException se) {
            throw new IllegalArgumentException(
                    "Failed to get array from JDBCArrayBasic: " + se);
        }
        if (!(internalArray instanceof String[]))
            throw new IllegalArgumentException(
                    "JDBCArrayBasic internal data is not a String array, but a "
                    + internalArray.getClass().getName());
        return (String[]) internalArray;
    }

    private static boolean isWrapperFor(Array array, String[] strings) {
        if (array == null && strings == null) {
            return true;
        }
        if (array == null || strings == null) {
            return false;
        }
        String[] wrappedStrings = toStrings(array);
        if (wrappedStrings.length != strings.length) {
            return false;
        }
        for (int i = 0; i < strings.length; i++) {
            if (!strings[i].equals(wrappedStrings[i])) {
                return false;
            }
        }
        return true;
    }

    public void testPrecedences() {
        AuthBeanMultiplexer plexer = AuthBeanMultiplexer.getSingleton();

        try {
            plexer.authenticate("dbNameKey", "x", "y");
            fail("Use of uninitialized AuthBeanMultiplexer did not throw");
        } catch (RuntimeException re) {
            // Intentionally empty
        } catch (Exception e) {
            fail("Use of uninitialized AuthBeanMultiplexer threw a "
                    + e.getClass().getName() + " instead of a RTE");
        }

        plexer.setAuthTriggerBeans(Collections.singletonMap("dbNameKey",
                Arrays.asList(new AuthTriggerBean[] {
                        twoRolePermittingAuthTriggerBean,
                        purposefullyBrokenAuthTriggerBean,
                denyingAuthTriggerBean})));
        Array res = null;
        try {
            res = plexer.authenticate("dbNameKey", "u", "p");
        } catch (Exception e) {
            fail("2-role success test threw: " + e);
        }
        if (!isWrapperFor(res, twoRoles)) {
            fail("2-role success test return success with roles: "
                    + toStrings(res));
        }

    }

    /**
     * This method allows to easily run this unit test independent of the other
     * unit tests, and without dealing with Ant or unrelated test suites.
     */
    static public void main(String[] sa) {
        if (sa.length > 0 && sa[0].startsWith("-g")) {
            junit.swingui.TestRunner.run(AuthBeanMultiplexerTest.class);
        } else {
            junit.textui.TestRunner runner = new junit.textui.TestRunner();
            junit.framework.TestResult result =
                runner.run(runner.getTest(AuthBeanMultiplexerTest.class.getName()));

            System.exit(result.wasSuccessful() ? 0 : 1);
        }
    }
}
