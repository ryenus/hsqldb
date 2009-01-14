/* Copyright (c) 2001-2009, The HSQL Development Group
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

import org.hsqldb.lib.ValidatingResourceBundle;
import org.hsqldb.lib.RefCapablePropertyResourceBundle;

public class SqltoolRBTest {
    /**
     * Does a quick test of this class.
     */
    static public void main(String[] sa) {
        SqltoolRB rb1 = new SqltoolRB();
        SqltoolRB rb2 = new SqltoolRB();
        String[] testParams = { "one", "two", "three" };
        rb1.validate();
        rb2.validate();
        System.err.println("rb1 size = " + rb1.getSize());
        System.err.println("rb2 size = " + rb2.getSize());
        rb1.setMissingPosValueBehavior(
                ValidatingResourceBundle.EMPTYSTRING_BEHAVIOR);
        rb2.setMissingPosValueBehavior(
                ValidatingResourceBundle.NOOP_BEHAVIOR);

        /*
        rb1.setMissingPropertyBehavior(
                ValidatingResourceBundle.THROW_BEHAVIOR);
        System.out.println("("
                + rb1.getExpandedString(SqltoolRB.JDBC_ESTABLISHED) + ')');
        */

        System.out.println("First, with no positional parameters set...");
        System.out.println("JDBC_ESTABLISHED String w/ EMPTYSTRING_BEHAVIOR: "
                + rb1.getString(SqltoolRB.JDBC_ESTABLISHED));
        System.out.println("JDBC_ESTABLISHED String w/ NOOP_BEHAVIOR: "
                + rb2.getString(SqltoolRB.JDBC_ESTABLISHED));
        System.out.println("Now, with no positional values set...");
        System.out.println("JDBC_ESTABLISHED String w/ EMPTYSTRING_BEHAVIOR: "
                + rb1.getString(SqltoolRB.JDBC_ESTABLISHED, new String[] {}));
        System.out.println("JDBC_ESTABLISHED String w/ NOOP_BEHAVIOR: "
                + rb2.getString(SqltoolRB.JDBC_ESTABLISHED, new String[] {}));
        System.out.println();
        System.out.println("Now, with positional params set to one/two/three");
        System.out.println("JDBC_ESTABLISHED String w/ EMPTYSTRING_BEHAVIOR: "
                + rb1.getString(SqltoolRB.JDBC_ESTABLISHED, testParams));
        System.out.println("JDBC_ESTABLISHED String w/ NOOP_BEHAVIOR: "
                + rb2.getString(SqltoolRB.JDBC_ESTABLISHED, testParams));
        rb1.setMissingPosValueBehavior(
             RefCapablePropertyResourceBundle.THROW_BEHAVIOR);
        System.out.println("JDBC_ESTABLISHED String w/ THROW_BEHAVIOR: "
                + rb1.getString(SqltoolRB.JDBC_ESTABLISHED, testParams));
        System.out.println();
        System.out.println("Now, with no parameters set");
        System.out.println("JDBC_ESTABLISHED String w/ THROW_BEHAVIOR: ");
        System.out.println(
                rb1.getString(SqltoolRB.JDBC_ESTABLISHED, new String[] {}));
    }
}
