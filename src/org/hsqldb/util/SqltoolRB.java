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
public class SqltoolRB extends ValidatingResourceBundle {
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

    static private int keyCounter = 0;
    static public final int SQLTOOL_SYNTAX = keyCounter++;
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

    static public final int SQLFILE_BANNER = keyCounter++;
    static public final int BUFFER_HELP = keyCounter++;
    static public final int SPECIAL_HELP = keyCounter++;
    static public final int PL_HELP = keyCounter++;
    static public final int DSV_OPTIONS = keyCounter++;
    static public final int D_OPTIONS = keyCounter++;
    static public final int RAW_LEADIN = keyCounter++;
    static public final int ERRORAT = keyCounter++;
    static public final int ERRORAT_WITHECHO = keyCounter++;
    static public final int REJECTREPORT_TOP = keyCounter++;
    static public final int REJECTREPORT_ROW = keyCounter++;
    static public final int REJECTREPORT_BOTTOM = keyCounter++;

    static public final int SQLFILE_NOREAD = keyCounter++;
    static public final int RAWMODE_PROMPT = keyCounter++;
    static public final int RAW_MOVEDTOBUFFER = keyCounter++;
    static public final int INPUT_MOVEDTOBUFFER = keyCounter++;
    static public final int SQLSTATEMENT_EMPTY = keyCounter++;
    static public final int CAUSE = keyCounter++;
    static public final int BREAK_UNSATISFIED = keyCounter++;
    static public final int CONTINUE_UNSATISFIED = keyCounter++;
    static public final int CONTINUE_UNSATISFIED_TYPED = keyCounter++;
    static public final int PRIMARYINPUT_ACCESSFAILURE = keyCounter++;
    static public final int INPUT_UNTERMINATED = keyCounter++;
    static public final int PLVARSET_INCOMPLETE = keyCounter++;
    static public final int ABORTING = keyCounter++;
    static public final int INPUTREADER_CLOSEFAILURE = keyCounter++;
    static public final int ROLLINGBACK = keyCounter++;
    static public final int SPECIAL_UNSPECIFIED = keyCounter++;
    static public final int NOBUFFER = keyCounter++;
    static public final int BUFFER_EXECUTING = keyCounter++;
    static public final int EXECUTING = keyCounter++;
    static public final int NOBUFFER_YET = keyCounter++;
    static public final int BUFFER_CURRENT = keyCounter++;
    static public final int COMMANDNUM_MALFORMAT = keyCounter++;
    static public final int BUFFER_RESTORED = keyCounter++;
    static public final int SUBSTITUTION_MALFORMAT = keyCounter++;
    static public final int SUBSTITUTION_SAMPLE = keyCounter++;
    static public final int SUBSTITUTION_NOMATCH = keyCounter++;
    static public final int SUBSTITUTION_SYNTAX = keyCounter++;
    static public final int BUFFER_UNKNOWN = keyCounter++;
    static public final int SPECIAL_EXTRACHARS = keyCounter++;
    static public final int BUFFER_EXTRACHARS = keyCounter++;
    static public final int SPECIAL_MALFORMAT = keyCounter++;
    static public final int HTML_MODE = keyCounter++;
    static public final int DSV_TARGETFILE_REQUIRED = keyCounter++;
    static public final int FILE_WROTECHARS = keyCounter++;
    static public final int FILE_NOWRITE = keyCounter++;
    static public final int METADATA_NOOBTAIN = keyCounter++;
    static public final int SPECIAL_D_LIKE = keyCounter++;
    static public final int OUTPUTFILE_NONETOCLOSE = keyCounter++;
    static public final int OUTPUTFILE_REOPENING = keyCounter++;
    static public final int OUTPUTFILE_HEADER = keyCounter++;
    static public final int DESTFILE_DEMAND = keyCounter++;
    static public final int BUFFER_EMPTY = keyCounter++;
    static public final int FILE_NOAPPEND = keyCounter++;
    static public final int SQLFILE_NAME_DEMAND = keyCounter++;
    static public final int SQLFILE_EXECUTE_FAIL = keyCounter++;
    static public final int A_SETTING = keyCounter++;
    static public final int COMMITTED = keyCounter++;
    static public final int SPECIAL_B_MALFORMAT = keyCounter++;
    static public final int BINARY_LOADEDBYTESINTO = keyCounter++;
    static public final int BINARY_FILEFAIL = keyCounter++;
    static public final int C_SETTING = keyCounter++;
    static public final int BANG_INCOMPLETE = keyCounter++;
    static public final int BANG_COMMAND_FAIL = keyCounter++;
    static public final int SPECIAL_UNKNOWN = keyCounter++;
    static public final int DSV_M_SYNTAX = keyCounter++;
    static public final int DSV_X_SYNTAX = keyCounter++;

    private static Object[] memberKeyArray = new Object[] {
        // SqlTool class, file references:
        new Integer(SQLTOOL_SYNTAX), "SqlTool.syntax",
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

        // SqlFile class, file references:
        new Integer(SQLFILE_BANNER), "SqlFile.banner",
        new Integer(BUFFER_HELP), "buffer.help",
        new Integer(SPECIAL_HELP), "special.help",
        new Integer(PL_HELP), "pl.help",
        new Integer(DSV_OPTIONS), "dsv.options",
        new Integer(D_OPTIONS), "d.options",
        new Integer(RAW_LEADIN), "raw.leadin",
        new Integer(ERRORAT), "errorat",
        new Integer(ERRORAT_WITHECHO), "errorat.withecho",
        new Integer(REJECTREPORT_TOP), "rejectreport.top",
        new Integer(REJECTREPORT_ROW), "rejectreport.row",
        new Integer(REJECTREPORT_BOTTOM), "rejectreport.bottom",

        // SqlFile inline properties:
        new Integer(SQLFILE_NOREAD), "sqlfile.noread",
        new Integer(RAWMODE_PROMPT), "rawmode.prompt",
        new Integer(RAW_MOVEDTOBUFFER), "raw.movedtobuffer",
        new Integer(INPUT_MOVEDTOBUFFER), "input.movedtobuffer",
        new Integer(SQLSTATEMENT_EMPTY), "sqlstatement.empty",
        new Integer(CAUSE), "cause",
        new Integer(BREAK_UNSATISFIED), "break.unsatisfied",
        new Integer(CONTINUE_UNSATISFIED), "continue.unsatisfied",
        new Integer(CONTINUE_UNSATISFIED_TYPED), "continue.unsatisfied.typed",
        new Integer(PRIMARYINPUT_ACCESSFAILURE), "primaryinput.accessfailure",
        new Integer(INPUT_UNTERMINATED), "input.unterminated",
        new Integer(PLVARSET_INCOMPLETE), "plvarset.incomplete",
        new Integer(ABORTING), "aborting",
        new Integer(INPUTREADER_CLOSEFAILURE), "inputreader.closefailure",
        new Integer(ROLLINGBACK), "rollingback",
        new Integer(SPECIAL_UNSPECIFIED), "special.unspecified",
        new Integer(NOBUFFER), "nobuffer",
        new Integer(BUFFER_EXECUTING), "buffer.executing",
        new Integer(EXECUTING), "executing",
        new Integer(NOBUFFER_YET), "nobuffer.yet",
        new Integer(BUFFER_CURRENT), "buffer.current",
        new Integer(COMMANDNUM_MALFORMAT), "commandnum.malformat",
        new Integer(BUFFER_RESTORED), "buffer.restored",
        new Integer(SUBSTITUTION_MALFORMAT), "substitution.malformat",
        new Integer(SUBSTITUTION_SAMPLE), "substitution.sample",
        new Integer(SUBSTITUTION_NOMATCH), "substitution.nomatch",
        new Integer(SUBSTITUTION_SYNTAX), "substitution.syntax",
        new Integer(BUFFER_UNKNOWN), "buffer.unknown",
        new Integer(SPECIAL_EXTRACHARS), "special.extrachars",
        new Integer(BUFFER_EXTRACHARS), "buffer.extrachars",
        new Integer(SPECIAL_MALFORMAT), "special.malformat",
        new Integer(HTML_MODE), "html.mode",
        new Integer(DSV_TARGETFILE_REQUIRED), "dsv.targetfile.required",
        new Integer(FILE_WROTECHARS), "file.wrotechars",
        new Integer(FILE_NOWRITE), "file.nowrite",
        new Integer(METADATA_NOOBTAIN), "metadata.noobtain",
        new Integer(SPECIAL_D_LIKE), "special.d.like",
        new Integer(OUTPUTFILE_NONETOCLOSE), "outputfile.nonetoclose",
        new Integer(OUTPUTFILE_REOPENING), "outputfile.reopening",
        new Integer(OUTPUTFILE_HEADER), "outputfile.header",
        new Integer(DESTFILE_DEMAND), "destfile.demand",
        new Integer(BUFFER_EMPTY), "buffer.empty",
        new Integer(FILE_NOAPPEND), "file.noappend",
        new Integer(SQLFILE_NAME_DEMAND), "sqlfile.name.demand",
        new Integer(SQLFILE_EXECUTE_FAIL), "sqlfile.execute.fail",
        new Integer(A_SETTING), "a.setting",
        new Integer(COMMITTED), "committed",
        new Integer(SPECIAL_B_MALFORMAT), "special.b.malformat",
        new Integer(BINARY_LOADEDBYTESINTO), "binary.loadedbytesinto",
        new Integer(BINARY_FILEFAIL), "binary.filefail",
        new Integer(C_SETTING), "c.setting",
        new Integer(BANG_INCOMPLETE), "bang.incomplete",
        new Integer(BANG_COMMAND_FAIL), "bang.command.fail",
        new Integer(SPECIAL_UNKNOWN), "special.unknown",
        new Integer(DSV_M_SYNTAX), "dsv.m.syntax",
        new Integer(DSV_X_SYNTAX), "dsv.x.syntax",
    };

    private Map keyIdToString = new HashMap();

    protected Map getKeyIdToString() {
        return keyIdToString;
    }

    public SqltoolRB() {
        super("org.hsqldb.util.sqltool");
        if (memberKeyArray == null)
            throw new RuntimeException("'static memberKeyArray not set");
        for (int i = 0; i < memberKeyArray.length; i += 2) {
            keyIdToString.put(memberKeyArray[i], memberKeyArray[i+1]);
        }
    }

    static {
        if (memberKeyArray == null)
            throw new RuntimeException("'static memberKeyArray not set");
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
        }
        /* DEBUG
        System.err.println("Initialized keyIdToString map with "
                + keyIdToString.size() + " mappings");
        */
    }
}
