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

import java.util.Map;
import java.util.HashMap;
import org.hsqldb.lib.ValidatingResourceBundle;

/* $Id$ */

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
    static private int keyCounter = 0;
    static public final int SQLTOOL_SYNTAX = keyCounter++;
    static public final int PASSWORDFOR_PROMPT = keyCounter++;
    static public final int SQLTOOL_VARSET_BADFORMAT = keyCounter++;
    static public final int SQLTOOL_ABORTCONTINUE_MUTUALLYEXCLUSIVE =
            keyCounter++;
    static public final int SQLTEMPFILE_FAIL = keyCounter++;
    static public final int RCDATA_INLINEURL_MISSING = keyCounter++;
    static public final int RCDATA_INLINE_EXTRAVARS = keyCounter++;
    static public final int RCDATA_INLINEUSERNAME_MISSING = keyCounter++;
    static public final int RCDATA_PASSWORD_VISIBLE = keyCounter++;
    static public final int PASSWORD_READFAIL = keyCounter++;
    static public final int CONNECTION_FAIL = keyCounter++;
    static public final int RCDATA_GENFROMVALUES_FAIL = keyCounter++;
    static public final int CONNDATA_RETRIEVAL_FAIL = keyCounter++;
    static public final int JDBC_ESTABLISHED = keyCounter++;

    static public final int SQLFILE_BANNER = keyCounter++;
    static public final int BUFFER_HELP = keyCounter++;
    static public final int SPECIAL_HELP = keyCounter++;
    static public final int PL_HELP = keyCounter++;
    static public final int DSV_OPTIONS = keyCounter++;
    static public final int D_OPTIONS = keyCounter++;
    static public final int RAW_LEADIN = keyCounter++;
    static public final int ERRORAT = keyCounter++;
    static public final int REJECTREPORT_TOP = keyCounter++;
    static public final int REJECTREPORT_ROW = keyCounter++;
    static public final int REJECTREPORT_BOTTOM = keyCounter++;
    static public final int MACRO_HELP = keyCounter++;

    static public final int RAWMODE_PROMPT = keyCounter++;
    static public final int RAW_MOVEDTOBUFFER = keyCounter++;
    static public final int INPUT_MOVEDTOBUFFER = keyCounter++;
    static public final int SQLSTATEMENT_EMPTY = keyCounter++;
    static public final int CAUSEREPORT = keyCounter++;
    static public final int BREAK_UNSATISFIED = keyCounter++;
    static public final int CONTINUE_UNSATISFIED = keyCounter++;
    static public final int PRIMARYINPUT_ACCESSFAIL = keyCounter++;
    static public final int INPUT_UNTERMINATED = keyCounter++;
    static public final int PLVAR_SET_INCOMPLETE = keyCounter++;
    static public final int ABORTING = keyCounter++;
    static public final int ROLLINGBACK = keyCounter++;
    static public final int SPECIAL_UNSPECIFIED = keyCounter++;
    static public final int BUFHIST_UNSPECIFIED = keyCounter++;
    static public final int BUFFER_EXECUTING = keyCounter++;
    static public final int NOBUFFER_YET = keyCounter++;
    static public final int COMMANDNUM_MALFORMAT = keyCounter++;
    static public final int BUFFER_RESTORED = keyCounter++;
    static public final int SUBSTITUTION_MALFORMAT = keyCounter++;
    static public final int SUBSTITUTION_NOMATCH = keyCounter++;
    static public final int SUBSTITUTION_SYNTAX = keyCounter++;
    static public final int BUFFER_UNKNOWN = keyCounter++;
    static public final int SPECIAL_EXTRACHARS = keyCounter++;
    static public final int BUFFER_EXTRACHARS = keyCounter++;
    static public final int SPECIAL_MALFORMAT = keyCounter++;
    static public final int HTML_MODE = keyCounter++;
    static public final int DSV_TARGETFILE_DEMAND = keyCounter++;
    static public final int FILE_WROTECHARS = keyCounter++;
    static public final int FILE_WRITEFAIL = keyCounter++;
    static public final int SPECIAL_D_LIKE = keyCounter++;
    static public final int OUTPUTFILE_NONETOCLOSE = keyCounter++;
    static public final int OUTPUTFILE_REOPENING = keyCounter++;
    static public final int OUTPUTFILE_HEADER = keyCounter++;
    static public final int DESTFILE_DEMAND = keyCounter++;
    static public final int BUFFER_EMPTY = keyCounter++;
    static public final int FILE_APPENDFAIL = keyCounter++;
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
    static public final int RAW_EMPTY = keyCounter++;
    static public final int DSV_NOCOLSLEFT = keyCounter++;
    static public final int DSV_SKIPCOLS_MISSING = keyCounter++;
    static public final int PLALIAS_MALFORMAT = keyCounter++;
    static public final int PLVAR_UNDEFINED = keyCounter++;
    static public final int SYSPROP_EMPTY = keyCounter++;
    static public final int SYSPROP_UNTERMINATED = keyCounter++;
    static public final int SYSPROP_UNDEFINED = keyCounter++;
    static public final int VAR_INFINITE = keyCounter++;
    static public final int PLVAR_NAMEEMPTY = keyCounter++;
    static public final int PLVAR_UNTERMINATED = keyCounter++;
    static public final int PL_MALFORMAT = keyCounter++;
    static public final int PL_EXPANSIONMODE = keyCounter++;
    static public final int END_NOBLOCK = keyCounter++;
    static public final int CONTINUE_SYNTAX = keyCounter++;
    static public final int BREAK_SYNTAX = keyCounter++;
    static public final int PL_LIST_PARENS = keyCounter++;
    static public final int PL_LIST_LENGTHS = keyCounter++;
    static public final int DUMPLOAD_MALFORMAT = keyCounter++;
    static public final int PLVAR_NOCOLON = keyCounter++;
    static public final int PLVAR_TILDEDASH_NOMOREARGS = keyCounter++;
    static public final int DUMPLOAD_FAIL = keyCounter++;
    static public final int PREPARE_MALFORMAT = keyCounter++;
    static public final int FOREACH_MALFORMAT = keyCounter++;
    static public final int PL_BLOCK_FAIL = keyCounter++;
    static public final int IFWHILE_MALFORMAT = keyCounter++;
    static public final int IF_MALFORMAT = keyCounter++;
    static public final int WHILE_MALFORMAT = keyCounter++;
    static public final int PL_UNKNOWN = keyCounter++;
    static public final int PL_BLOCK_UNTERMINATED = keyCounter++;
    static public final int VENDOR_ORACLE_DS = keyCounter++;
    static public final int VENDOR_DERBY_DR = keyCounter++;
    static public final int VENDOR_NOSUP_D = keyCounter++;
    static public final int VENDOR_DERBY_DU = keyCounter++;
    static public final int SPECIAL_D_UNKNOWN = keyCounter++;
    static public final int METADATA_FETCH_FAIL = keyCounter++;
    static public final int METADATA_FETCH_FAILFOR = keyCounter++;
    static public final int PREPARE_DEMANDQM = keyCounter++;
    static public final int BINBUFFER_EMPTY = keyCounter++;
    static public final int VENDOR_NOSUP_SYSSCHEMAS = keyCounter++;
    static public final int NORESULT = keyCounter++;
    static public final int DSV_BINCOL = keyCounter++;
    static public final int BINBUF_WRITE = keyCounter++;
    static public final int ROWS_FETCHED = keyCounter++;
    static public final int ROWS_FETCHED_DSV = keyCounter++;
    static public final int ROW_UPDATE_SINGULAR = keyCounter++;
    static public final int ROW_UPDATE_MULTIPLE = keyCounter++;
    static public final int HISTORY_UNAVAILABLE = keyCounter++;
    static public final int HISTORY_NONE = keyCounter++;
    static public final int EDITBUFFER_CONTENTS = keyCounter++;
    static public final int BUFFER_INSTRUCTIONS = keyCounter++;
    static public final int HISTORY_NUMBER_REQ = keyCounter++;
    static public final int HISTORY_BACKTO = keyCounter++;
    static public final int HISTORY_UPTO = keyCounter++;
    static public final int HISTORY_BACK = keyCounter++;
    static public final int DESCRIBE_TABLE_NAME = keyCounter++;
    static public final int DESCRIBE_TABLE_DATATYPE = keyCounter++;
    static public final int DESCRIBE_TABLE_WIDTH = keyCounter++;
    static public final int DESCRIBE_TABLE_NONULLS = keyCounter++;
    static public final int LOGICAL_UNRECOGNIZED = keyCounter++;
    static public final int READ_TOOBIG = keyCounter++;
    static public final int READ_PARTIAL = keyCounter++;
    static public final int READ_CONVERTFAIL = keyCounter++;
    static public final int DSV_COLDELIM_PRESENT = keyCounter++;
    static public final int DSV_ROWDELIM_PRESENT = keyCounter++;
    static public final int DSV_NULLREP_PRESENT = keyCounter++;
    static public final int DSV_CONSTCOLS_NULLCOL = keyCounter++;
    static public final int FILE_READFAIL = keyCounter++;
    static public final int INPUTFILE_CLOSEFAIL = keyCounter++;
    static public final int DSV_HEADER_NONE = keyCounter++;
    static public final int DSV_HEADER_NOSWITCHTARG = keyCounter++;
    static public final int DSV_HEADER_NOSWITCHMATCH = keyCounter++;
    static public final int DSV_HEADER_NONSWITCHED = keyCounter++;
    static public final int DSV_NOCOLHEADER = keyCounter++;
    static public final int DSV_METADATA_MISMATCH = keyCounter++;
    static public final int QUERY_METADATAFAIL = keyCounter++;
    static public final int DSV_REJECTFILE_SETUPFAIL = keyCounter++;
    static public final int DSV_REJECTREPORT_SETUPFAIL = keyCounter++;
    static public final int NONE = keyCounter++;
    static public final int INSERTION_PREPAREFAIL = keyCounter++;
    static public final int DSV_HEADER_MATCHERNONHEAD = keyCounter++;
    static public final int DSV_COLCOUNT_MISMATCH = keyCounter++;
    static public final int DSV_INSERTCOL_MISMATCH = keyCounter++;
    static public final int TIME_BAD = keyCounter++;
    static public final int BOOLEAN_BAD = keyCounter++;
    static public final int INPUTREC_MODIFIED = keyCounter++;
    static public final int DSV_RECIN_FAIL = keyCounter++;
    static public final int DSV_IMPORT_SUMMARY = keyCounter++;
    static public final int INSERTIONS_NOTCOMMITTED = keyCounter++;
    static public final int AUTOCOMMIT_FETCHFAIL = keyCounter++;
    static public final int DSV_REJECTFILE_PURGEFAIL = keyCounter++;
    static public final int DSV_REJECTREPORT_PURGEFAIL = keyCounter++;
    static public final int EDIT_MALFORMAT = keyCounter++;
    static public final int INPUT_MALFORMAT = keyCounter++;
    static public final int APPEND_EMPTY = keyCounter++;
    static public final int TRANSISO_REPORT = keyCounter++;
    static public final int EXECTIME_REPORTING = keyCounter++;
    static public final int EXECTIME_REPORT = keyCounter++;
    static public final int REGEX_MALFORMAT = keyCounter++;
    static public final int ENCODE_FAIL = keyCounter++;
    static public final int MACRO_TIP = keyCounter++;
    static public final int MACRODEF_MALFORMAT = keyCounter++;
    static public final int MACRODEF_EMPTY = keyCounter++;
    static public final int MACRODEF_SEMI = keyCounter++;
    static public final int MACRO_MALFORMAT = keyCounter++;
    static public final int MACRO_UNDEFINED = keyCounter++;
    static public final int LOG_SYNTAX = keyCounter++;
    static public final int LOG_SYNTAX_ERROR = keyCounter++;
    static public final int REJECT_RPC = keyCounter++;
    static public final int RPC_AUTOCOMMIT_FAILURE = keyCounter++;
    static public final int RPC_COMMIT_FAILURE = keyCounter++;
    static public final int DISCONNECT_SUCCESS = keyCounter++;
    static public final int DISCONNECT_FAILURE = keyCounter++;
    static public final int NO_REQUIRED_CONN = keyCounter++;
    static public final int DISCONNECTED_MSG = keyCounter++;
    static public final int CONNECTED_FALLBACKMSG = keyCounter++;

    @SuppressWarnings("boxing")
    private static Object[] memberKeyArray = new Object[] {
        /* With Java 5, can use auto-boxing and get rid of all of the
         * Integer instantiations below.*/
        // SqlTool class, file references:
        SQLTOOL_SYNTAX, "SqlTool.syntax",
        // SqlTool inline properties:
        PASSWORDFOR_PROMPT, "passwordFor.prompt",
        SQLTOOL_VARSET_BADFORMAT, "SqlTool.varset.badformat",
        SQLTOOL_ABORTCONTINUE_MUTUALLYEXCLUSIVE,
                "SqlTool.abort_continue.mutuallyexclusive",
        SQLTEMPFILE_FAIL, "sqltempfile.fail",
        RCDATA_INLINEURL_MISSING, "rcdata.inlineurl.missing",
        RCDATA_INLINE_EXTRAVARS, "rcdata.inline.extravars",
        RCDATA_INLINEUSERNAME_MISSING,
                "rcdata.inlineusername.missing",
        RCDATA_PASSWORD_VISIBLE, "rcdata.password.visible",
        PASSWORD_READFAIL, "password.readfail",
        CONNECTION_FAIL, "connection.fail",
        RCDATA_GENFROMVALUES_FAIL,
                "rcdata.genfromvalues.fail",
        CONNDATA_RETRIEVAL_FAIL, "conndata.retrieval.fail",
        JDBC_ESTABLISHED, "jdbc.established",

        // SqlFile class, file references:
        SQLFILE_BANNER, "SqlFile.banner",
        BUFFER_HELP, "buffer.help",
        SPECIAL_HELP, "special.help",
        PL_HELP, "pl.help",
        DSV_OPTIONS, "dsv.options",
        D_OPTIONS, "d.options",
        RAW_LEADIN, "raw.leadin",
        ERRORAT, "errorat",
        REJECTREPORT_TOP, "rejectreport.top",
        REJECTREPORT_ROW, "rejectreport.row",
        REJECTREPORT_BOTTOM, "rejectreport.bottom",
        MACRO_HELP, "macro.help",

        // SqlFile inline properties:
        RAWMODE_PROMPT, "rawmode.prompt",
        RAW_MOVEDTOBUFFER, "raw.movedtobuffer",
        INPUT_MOVEDTOBUFFER, "input.movedtobuffer",
        SQLSTATEMENT_EMPTY, "sqlstatement.empty",
        CAUSEREPORT, "causereport",
        BREAK_UNSATISFIED, "break.unsatisfied",
        CONTINUE_UNSATISFIED, "continue.unsatisfied",
        PRIMARYINPUT_ACCESSFAIL, "primaryinput.accessfail",
        INPUT_UNTERMINATED, "input.unterminated",
        PLVAR_SET_INCOMPLETE, "plvar.set.incomplete",
        ABORTING, "aborting",
        ROLLINGBACK, "rollingback",
        SPECIAL_UNSPECIFIED, "special.unspecified",
        BUFHIST_UNSPECIFIED, "bufhist.unspecified",
        BUFFER_EXECUTING, "buffer.executing",
        NOBUFFER_YET, "nobuffer.yet",
        COMMANDNUM_MALFORMAT, "commandnum.malformat",
        BUFFER_RESTORED, "buffer.restored",
        SUBSTITUTION_MALFORMAT, "substitution.malformat",
        SUBSTITUTION_NOMATCH, "substitution.nomatch",
        SUBSTITUTION_SYNTAX, "substitution.syntax",
        BUFFER_UNKNOWN, "buffer.unknown",
        SPECIAL_EXTRACHARS, "special.extrachars",
        BUFFER_EXTRACHARS, "buffer.extrachars",
        SPECIAL_MALFORMAT, "special.malformat",
        HTML_MODE, "html.mode",
        DSV_TARGETFILE_DEMAND, "dsv.targetfile.demand",
        FILE_WROTECHARS, "file.wrotechars",
        FILE_WRITEFAIL, "file.writefail",
        SPECIAL_D_LIKE, "special.d.like",
        OUTPUTFILE_NONETOCLOSE, "outputfile.nonetoclose",
        OUTPUTFILE_REOPENING, "outputfile.reopening",
        OUTPUTFILE_HEADER, "outputfile.header",
        DESTFILE_DEMAND, "destfile.demand",
        BUFFER_EMPTY, "buffer.empty",
        FILE_APPENDFAIL, "file.appendfail",
        SQLFILE_NAME_DEMAND, "sqlfile.name.demand",
        SQLFILE_EXECUTE_FAIL, "sqlfile.execute.fail",
        A_SETTING, "a.setting",
        COMMITTED, "committed",
        SPECIAL_B_MALFORMAT, "special.b.malformat",
        BINARY_LOADEDBYTESINTO, "binary.loadedbytesinto",
        BINARY_FILEFAIL, "binary.filefail",
        C_SETTING, "c.setting",
        BANG_INCOMPLETE, "bang.incomplete",
        BANG_COMMAND_FAIL, "bang.command.fail",
        SPECIAL_UNKNOWN, "special.unknown",
        DSV_M_SYNTAX, "dsv.m.syntax",
        DSV_X_SYNTAX, "dsv.x.syntax",
        RAW_EMPTY, "raw.empty",
        DSV_NOCOLSLEFT, "dsv.nocolsleft",
        DSV_SKIPCOLS_MISSING, "dsv.skipcols.missing",
        PLALIAS_MALFORMAT, "plalias.malformat",
        PLVAR_UNDEFINED, "plvar.undefined",
        SYSPROP_EMPTY, "sysprop.empty",
        SYSPROP_UNTERMINATED, "sysprop.unterminated",
        SYSPROP_UNDEFINED, "sysprop.undefined",
        VAR_INFINITE, "var.infinite",
        PLVAR_NAMEEMPTY, "plvar.nameempty",
        PLVAR_UNTERMINATED, "plvar.unterminated",
        PL_MALFORMAT, "pl.malformat",
        PL_EXPANSIONMODE, "pl.expansionmode",
        END_NOBLOCK, "end.noblock",
        CONTINUE_SYNTAX, "continue.syntax",
        BREAK_SYNTAX, "break.syntax",
        PL_LIST_PARENS, "pl.list.parens",
        PL_LIST_LENGTHS, "pl.list.lengths",
        DUMPLOAD_MALFORMAT, "dumpload.malformat",
        PLVAR_NOCOLON, "plvar.nocolon",
        PLVAR_TILDEDASH_NOMOREARGS, "plvar.tildedash.nomoreargs",
        DUMPLOAD_FAIL, "dumpload.fail",
        PREPARE_MALFORMAT, "prepare.malformat",
        FOREACH_MALFORMAT, "foreach.malformat",
        PL_BLOCK_FAIL, "pl.block.fail",
        IFWHILE_MALFORMAT, "ifwhile.malformat",
        IF_MALFORMAT, "if.malformat",
        WHILE_MALFORMAT, "while.malformat",
        PL_UNKNOWN, "pl.unknown",
        PL_BLOCK_UNTERMINATED, "pl.block.unterminated",
        VENDOR_ORACLE_DS, "vendor.oracle.dS",
        VENDOR_DERBY_DR, "vendor.derby.dr",
        VENDOR_NOSUP_D, "vendor.nosup.d",
        VENDOR_DERBY_DU, "vendor.derby.du",
        SPECIAL_D_UNKNOWN, "special.d.unknown",
        METADATA_FETCH_FAIL, "metadata.fetch.fail",
        METADATA_FETCH_FAILFOR, "metadata.fetch.failfor",
        PREPARE_DEMANDQM, "prepare.demandqm",
        BINBUFFER_EMPTY, "binbuffer.empty",
        VENDOR_NOSUP_SYSSCHEMAS, "vendor.nosup.sysschemas",
        NORESULT, "noresult",
        DSV_BINCOL, "dsv.bincol",
        BINBUF_WRITE, "binbuf.write",
        ROWS_FETCHED, "rows.fetched",
        ROWS_FETCHED_DSV, "rows.fetched.dsv",
        ROW_UPDATE_SINGULAR, "row.update.singular",
        ROW_UPDATE_MULTIPLE, "row.update.multiple",
        HISTORY_UNAVAILABLE, "history.unavailable",
        HISTORY_NONE, "history.none",
        EDITBUFFER_CONTENTS, "editbuffer.contents",
        BUFFER_INSTRUCTIONS, "buffer.instructions",
        HISTORY_NUMBER_REQ, "history.number.req",
        HISTORY_BACKTO, "history.backto",
        HISTORY_UPTO, "history.upto",
        HISTORY_BACK, "history.back",
        DESCRIBE_TABLE_NAME, "describe.table.name",
        DESCRIBE_TABLE_DATATYPE, "describe.table.datatype",
        DESCRIBE_TABLE_WIDTH, "describe.table.width",
        DESCRIBE_TABLE_NONULLS, "describe.table.nonulls",
        LOGICAL_UNRECOGNIZED, "logical.unrecognized",
        READ_TOOBIG, "read.toobig",
        READ_PARTIAL, "read.partial",
        READ_CONVERTFAIL, "read.convertfail",
        DSV_COLDELIM_PRESENT, "dsv.coldelim.present",
        DSV_ROWDELIM_PRESENT, "dsv.rowdelim.present",
        DSV_NULLREP_PRESENT, "dsv.nullrep.present",
        DSV_CONSTCOLS_NULLCOL, "dsv.constcols.nullcol",
        FILE_READFAIL, "file.readfail",
        INPUTFILE_CLOSEFAIL, "inputfile.closefail",
        DSV_HEADER_NONE, "dsv.header.none",
        DSV_HEADER_NOSWITCHTARG, "dsv.header.noswitchtarg",
        DSV_HEADER_NOSWITCHMATCH, "dsv.header.noswitchmatch",
        DSV_HEADER_NONSWITCHED, "dsv.header.nonswitched",
        DSV_NOCOLHEADER, "dsv.nocolheader",
        DSV_METADATA_MISMATCH, "dsv.metadata.mismatch",
        QUERY_METADATAFAIL, "query.metadatafail",
        DSV_REJECTFILE_SETUPFAIL, "dsv.rejectfile.setupfail",
        DSV_REJECTREPORT_SETUPFAIL, "dsv.rejectreport.setupfail",
        NONE, "none",
        INSERTION_PREPAREFAIL, "insertion.preparefail",
        DSV_HEADER_MATCHERNONHEAD, "dsv.header.matchernonhead",
        DSV_COLCOUNT_MISMATCH, "dsv.colcount.mismatch",
        DSV_INSERTCOL_MISMATCH, "dsv.insertcol.mismatch",
        TIME_BAD, "time.bad",
        BOOLEAN_BAD, "boolean.bad",
        INPUTREC_MODIFIED, "inputrec.modified",
        DSV_RECIN_FAIL, "dsv.recin.fail",
        DSV_IMPORT_SUMMARY, "dsv.import.summary",
        INSERTIONS_NOTCOMMITTED, "insertions.notcommitted",
        AUTOCOMMIT_FETCHFAIL, "autocommit.fetchfail",
        DSV_REJECTFILE_PURGEFAIL, "dsv.rejectfile.purgefail",
        DSV_REJECTREPORT_PURGEFAIL, "dsv.rejectreport.purgefail",
        EDIT_MALFORMAT, "edit.malformat",
        INPUT_MALFORMAT, "input.malformat",
        APPEND_EMPTY, "append.empty",
        TRANSISO_REPORT, "transiso.report",
        EXECTIME_REPORTING, "exectime.reporting",
        EXECTIME_REPORT, "exectime.report",
        REGEX_MALFORMAT, "regex.malformat",
        ENCODE_FAIL, "encode.fail",
        MACRO_TIP, "macro.tip",
        MACRODEF_MALFORMAT, "macrodef.malformat",
        MACRODEF_EMPTY, "macrodef.empty",
        MACRODEF_SEMI, "macrodef.semi",
        MACRO_MALFORMAT, "macro.malformat",
        MACRO_UNDEFINED, "macro.undefined",
        LOG_SYNTAX, "log.syntax",
        LOG_SYNTAX_ERROR, "log.syntax.error",
        REJECT_RPC, "reject.rpc",
        RPC_AUTOCOMMIT_FAILURE, "rpc.autocommit.failure",
        RPC_COMMIT_FAILURE, "rpc.commit.failure",
        DISCONNECT_SUCCESS, "disconnect.success",
        DISCONNECT_FAILURE, "disconnect.failure",
        NO_REQUIRED_CONN, "no.required.conn",
        DISCONNECTED_MSG, "disconnected.msg",
        CONNECTED_FALLBACKMSG, "connected.fallbackmsg",
    };

    private Map<Integer, String> keyIdToString = new HashMap<Integer, String>();

    protected Map<Integer, String> getKeyIdToString() {
        return keyIdToString;
    }

    public SqltoolRB() {
        super("org.hsqldb.cmdline.sqltool");
        if (memberKeyArray == null)
            throw new RuntimeException("'static memberKeyArray not set");
        for (int i = 0; i < memberKeyArray.length; i += 2) {
            keyIdToString.put(
                    (Integer) memberKeyArray[i], (String) memberKeyArray[i+1]);
        }
    }

    static {
        if (memberKeyArray == null)
            throw new RuntimeException("'static memberKeyArray not set");
        if (memberKeyArray.length % 2 != 0)
            throw new RuntimeException("memberKeyArray has an odd length");
        for (int i = 0; i < memberKeyArray.length; i += 2) {
            if (!(memberKeyArray[i] instanceof Integer))
                throw new RuntimeException("Element #" + i + " ("
                        + ((i - 1 < 0) ? "first item"
                            : ("after item \"" + memberKeyArray[i-1] + "\""))
                        + ") is a " + memberKeyArray[i].getClass().getName()
                        + ", not an Integer, in memberKeyArray in class "
                        + SqltoolRB.class.getName());
            if (!(memberKeyArray[i+1] instanceof String))
                throw new RuntimeException("Element #" + (i+1) + " ("
                        + ((i - 2 < 0) ? "first item"
                            : ("after item \"" + memberKeyArray[i-1] + "\""))
                        + ") is a " + memberKeyArray[i+1].getClass().getName()
                        + ", not a String, in memberKeyArray in class "
                        + SqltoolRB.class.getName());
            if (((Integer) memberKeyArray[i]).intValue() != i/2)
                throw new RuntimeException("Element #" +  i
                        + " is wrong constant for item " + memberKeyArray[i+1]
                        + " in memberKeyArray in class "
                        + SqltoolRB.class.getName());
        }
        /* DEBUG
        System.err.println("Initialized keyIdToString map with "
                + keyIdToString.size() + " mappings");
        */
    }
}
