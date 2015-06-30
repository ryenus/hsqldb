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
package org.hsqldb.cmdline;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

@ForSubject(SqltoolRB.class)
public class SqltoolRBTest extends BaseTestCase {

    private static final String[] testParams = {"one", "two", "three", "four"};
    static private final String RAW_CONN_MSG =
            "JDBC Connection established to a %{1} v. %{2} database"
            + System.getProperty("line.separator")
            + "as \"%{3}\" with %{4} Isolation.";
    static private final String SUBSTITUTED_CONN_MSG =
            "JDBC Connection established to a one v. two database"
            + System.getProperty("line.separator")
            + "as \"three\" with four Isolation.";

    /**
     * No positional parameters set...
     */
    @OfMethod("SqltoolRB.jdbc_established.getString()")
    public void testNoPosParams() {
        try {
            // When no substitution parameter at all given to getString(), behavior
            // settings have no influence, and no subsitution is performed.
            assertEquals(RAW_CONN_MSG, SqltoolRB.jdbc_established.getString());
        } catch (Exception e) {
            fail("RB system choked w/ " + e);
        }
    }

    /**
     * With positional parameters set to one/two/three/four
     */
    @OfMethod("SqltoolRB.jdbc_established.getString(String)")
    public void testWithParams() {
        try {
            //ValidatingResourceBundle.NOOP_BEHAVIOR);  This is SqltoolRB
            assertEquals(SUBSTITUTED_CONN_MSG,
                    SqltoolRB.jdbc_established.getString(testParams));
        } catch (Exception e) {
            fail("RB system choked w/ " + e);
        }
    }

    public static Test suite() {
        return new TestSuite(SqltoolRBTest.class);
    }
    /**
     * This method allows to easily run this unit test independent of the other
     * unit tests, and without dealing with Ant or unrelated test suites.
     */
    public static void main(String[] sa) {
        if (sa.length > 0 && sa[0].startsWith("-g")) {
            junit.swingui.TestRunner.run(SqltoolRBTest.class);
        } else {
            junit.framework.TestResult result =
                    junit.textui.TestRunner.run(suite());

            System.exit(result.wasSuccessful() ? 0 : 1);
        }
    }
}
