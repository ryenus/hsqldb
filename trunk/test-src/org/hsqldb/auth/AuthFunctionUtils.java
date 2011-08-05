/* Copyright (c) 2001-2011, The HSQL Development Group
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
import java.sql.Statement;
import org.hsqldb.types.Type;
import org.hsqldb.jdbc.JDBCArrayBasic;
import org.hsqldb.lib.FrameworkLogger;

public class AuthFunctionUtils {
    private static FrameworkLogger logger =
            FrameworkLogger.getLog(AuthFunctionUtils.class);

    /**
     * Do not instantiate an AuthFunctionUtils, because the only purpose of
     * this class is to provide static methods.
     */
    private AuthFunctionUtils() {
        // Intentionally empty
    }

    public static Array noRoleFn(
            String database, String user, String password) {
        return new JDBCArrayBasic(new String[0], Type.SQL_VARCHAR);
    }

    public static Array dbaFn(
            String database, String user, String password) {
        return new JDBCArrayBasic(new String[] { "DBA" }, Type.SQL_VARCHAR);
    }

    public static Array changeAuthFn(
            String database, String user, String password) {
        return new JDBCArrayBasic(
                new String[] { "CHANGE_AUTHORIZATION" }, Type.SQL_VARCHAR);
    }

    public static Array nullFn(
            String database, String user, String password) {
        return null;
    }

    /**
     * @throws RuntimeException if jab
     *         param is neither null nor an instance of JDBCArrayBasic wrapping
     *         an array of Strings.
     */
    static String[] toStrings(Array jab) {
        if (jab == null) {
            return null;
        }
        if (!(jab instanceof JDBCArrayBasic)) {
            throw new IllegalArgumentException(
                    "Parameter is a " + jab.getClass().getName()
                    + " instead of a " + JDBCArrayBasic.class.getName());
        }
        Object internalArray = ((JDBCArrayBasic) jab).getArray();
        if (!(internalArray instanceof String[]))
            throw new IllegalArgumentException(
                    "JDBCArrayBasic internal data is not a String array, but a "
                    + internalArray.getClass().getName());
        return (String[]) internalArray;
    }

    static boolean isWrapperFor(Array array, String[] strings) {
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

    static boolean updateDoesThrow(Statement st, String sql) {
        try {
            st.executeUpdate(sql);
            return false;
        } catch (SQLException se) {
            return true;
        }
    }
}
