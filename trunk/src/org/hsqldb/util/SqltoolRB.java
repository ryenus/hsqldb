/* Copyright (c) 2007, The HSQL Development Group
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
 *
 * $Id$
 */


package org.hsqldb.util;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Enumeration;

/**
 * Resource Bundle for SqlTool and associated classes.
 *
 * Purpose of this class is to wrap a RefCapablePropertyResourceBundle to
 *  reliably detect any possible use of a missing property key as soon as
 *  this class is clinitted.
 * The reason for this is to allow us developers to detect all such errors
 *  before end-users ever use this class.
 *
 * IMPORTANT:  To add a new ResourceBundle element, add two new lines, one
 * like <PRE>
 *    static public final int NEWKEYID = keyCounter++;
 * </PRE> and one line <PRE>
 *      new Integer(KEY2), "key2",
 * </PRE>
 * Both should be inserted right after all of the other lines of the same type.
 * NEWKEYID is obviously a new constant which you will use in calling code
 * like SqltoolRB.NEWKEYID.
 */
public class SqltoolRB {
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
        rb1.setMissingSubstValueBehavior(
             RefCapablePropertyResourceBundle.EMPTYSTRING_BEHAVIOR);
        rb2.setMissingSubstValueBehavior(
             RefCapablePropertyResourceBundle.NOOP_BEHAVIOR);
        System.out.println("First, with no positional parameters set...");
        System.out.println("JDBC_ESTABLISHED String w/ EMPTYSTRING_BEHAVIOR: "
                + rb1.getString(SqltoolRB.JDBC_ESTABLISHED), new String[] {});
        System.out.println("JDBC_ESTABLISHED String w/ NOOP_BEHAVIOR: "
                + rb2.getString(SqltoolRB.JDBC_ESTABLISHED), new String[] {});
        System.out.println();
        System.out.println("Now, with positional params set to one/two/three");
        System.out.println("JDBC_ESTABLISHED String w/ EMPTYSTRING_BEHAVIOR: "
                + rb1.getString(SqltoolRB.JDBC_ESTABLISHED), testParams);
        System.out.println("JDBC_ESTABLISHED String w/ NOOP_BEHAVIOR: "
                + rb2.getString(SqltoolRB.JDBC_ESTABLISHED), testParams);
        rb1.setMissingSubstValueBehavior(
             RefCapablePropertyResourceBundle.THROW_BEHAVIOR);
        System.out.println("JDBC_ESTABLISHED String w/ THROW_BEHAVIOR: "
                + rb1.getString(SqltoolRB.JDBC_ESTABLISHED), testParams);
        System.out.println();
        System.out.println("Now, with no parameters set");
        System.out.println("JDBC_ESTABLISHED String w/ THROW_BEHAVIOR: "
                + rb1.getString(SqltoolRB.JDBC_ESTABLISHED), new String[] {});
    }

    static private int keyCounter = 0;
    static public final int SQLTOOL_SYNTAX = keyCounter++;
    static public final int BANNER = keyCounter++;
    static public final int PASSWORDFOR_PROMPT = keyCounter++;
    static public final int SQLTOOL_VARSET_BADFORMAT = keyCounter++;
    static public final int SQLTOOL_ABORTCONTINUE_MUTUALLYEXCLUSIVE =
            keyCounter++;
    static public final int SQLTEMPFILE_FAILURE = keyCounter++;
    static public final int RCDATA_INLINEURL_MISSING = keyCounter++;
    static public final int RCDATA_INLINEUSERNAME_MISSING = keyCounter++;
    static public final int PASSWORD_BAD = keyCounter++;
    static public final int CONNECTION_FAILURE = keyCounter++;
    static public final int RCDATA_GENFROMVALUES_FAILURE = keyCounter++;
    static public final int CONNDATA_RETRIEVAL_FAILURE = keyCounter++;
    static public final int JDBC_ESTABLISHED = keyCounter++;
    static public final int TEMPFILE_REMOVAL_FAILURE = keyCounter++;
    private boolean validated = false;

    private static Object[] memberKeyArray = new Object[] {
        // SqlTool class, file references:
        new Integer(SQLTOOL_SYNTAX), "SqlTool.syntax",
        new Integer(BANNER), "banner",
        // SqlTool inline properties:
        new Integer(PASSWORDFOR_PROMPT), "passwordFor.prompt",
        new Integer(SQLTOOL_VARSET_BADFORMAT), "SqlTool.varset.badformat",
        new Integer(SQLTOOL_ABORTCONTINUE_MUTUALLYEXCLUSIVE),
                "SqlTool.abort_continue.mutuallyexclusive",
        new Integer(SQLTEMPFILE_FAILURE), "sqltempfile.failure",
        new Integer(RCDATA_INLINEURL_MISSING), "rcdata.inlineurl.missing",
        new Integer(RCDATA_INLINEUSERNAME_MISSING),
                "rcdata.inlineusername.missing",
        new Integer(PASSWORD_BAD), "password.bad",
        new Integer(CONNECTION_FAILURE), "connection.failure",
        new Integer(RCDATA_GENFROMVALUES_FAILURE),
                "rcdata.genfromvalues.failure",
        new Integer(CONNDATA_RETRIEVAL_FAILURE), "conndata.retrieval.failure",
        new Integer(JDBC_ESTABLISHED), "jdbc.established",
        new Integer(TEMPFILE_REMOVAL_FAILURE), "tempfile.removal.failure",
    };


    private RefCapablePropertyResourceBundle wrappedRCPRB =
        RefCapablePropertyResourceBundle.getBundle("org.hsqldb.util.sqltool",
                getClass().getClassLoader());

    static private Map keyIdToString = new HashMap();
    static {
        if (memberKeyArray == null)
            throw new RuntimeException("'memberKeyArray not overridden");
        Integer iger;
        String s;
        for (int i = 0; i < memberKeyArray.length; i += 2) {
            if (!(memberKeyArray[i] instanceof Integer))
                throw new RuntimeException("Element #" +  i
                        + " of memberKeyArray is not an Integer:  "
                        + memberKeyArray[i].getClass().getName());
            if (!(memberKeyArray[i+1] instanceof String))
                throw new RuntimeException("Element #" +  (i+1)
                        + " of memberKeyArray is not an Integer:  "
                        + memberKeyArray[i+1].getClass().getName());
            if (((Integer) memberKeyArray[i]).intValue() != i/2)
                throw new RuntimeException(
                        "Wrong contstant before element \""
                        + memberKeyArray[i+1] + "\" in array "
                        + "memberKeyArray in class "
                        + SqltoolRB.class.getName());
            keyIdToString.put(memberKeyArray[i], memberKeyArray[i+1]);
        }
        /* DEBUG
        System.err.println("Initialized keyIdToString map with "
                + keyIdToString.size() + " mappings");
        */
    }

    public void validate() {
        if (validated) return;
        validated = true;
        Set allIdStrings = new HashSet(keyIdToString.values());
        Enumeration allKeys = wrappedRCPRB.getKeys();
        while (allKeys.hasMoreElements())
            allIdStrings.remove(allKeys.nextElement());
        if (allIdStrings.size() > 0)
            throw new RuntimeException(
                    "Resource Bundle pre-validation failed.  "
                    + "Following property key(s) not mapped.\n" + allIdStrings);
    }

    // The following methods are a passthru wrappers for the wrapped RCPRB.

    /** @see RefCapablePropertyResourceBundle#getString(String) */
    public String getString(int id) {
        return wrappedRCPRB.getString((String) keyIdToString.get(id));
    }

    /** @see RefCapablePropertyResourceBundle#getString(String, String[]) */
    public String getString(int id, String[] sa) {
        return wrappedRCPRB.getString((String) keyIdToString.get(id), sa);
    }

    /** @see RefCapablePropertyResourceBundle#getExpandedString(String) */
    public String getExpandedString(int id) {
        return wrappedRCPRB.getExpandedString((String) keyIdToString.get(id));
    }

    /** @see RefCapablePropertyResourceBundle#getExpandedString(String, String[]) */
    public String getExpandedString(int id, String[] sa) {
        return wrappedRCPRB.getExpandedString(
                (String) keyIdToString.get(id), sa);
    }

    /** @see RefCapablePropertyResourceBundle#setMissingPropertyBehavior(int) */
    public void setMissingPropertyBehavior(int missingPropertyBehavior) {
        wrappedRCPRB.setMissingPropertyBehavior(missingPropertyBehavior);
    }

    /** @see RefCapablePropertyResourceBundle#setMissingSubstValueBehavior(int) */
    public void setMissingSubstValueBehavior(
            int missingSubstValueBehavior) {
        wrappedRCPRB.setMissingSubstValueBehavior(missingSubstValueBehavior);
    }

    /** @see RefCapablePropertyResourceBundle#getMissingPropertyBehavior() */
    public int getMissingPropertyBehavior() {
        return wrappedRCPRB.getMissingPropertyBehavior();
    }

    /** @see RefCapablePropertyResourceBundle#getMissingSubstValueBehavior() */
    public int getMissingSubstValueBehavior() {
        return wrappedRCPRB.getMissingSubstValueBehavior();
    }
}
