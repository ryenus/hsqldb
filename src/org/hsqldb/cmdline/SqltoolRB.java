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

    private static Object[] memberKeyArray = new Object[] {
        /* With Java 5, can use auto-boxing and get rid of all of the
         * Integer instantiations below.*/
        // SqlTool class, file references:
        new Integer(SQLTOOL_SYNTAX), "SqlTool.syntax",
        // SqlTool inline properties:
        new Integer(PASSWORDFOR_PROMPT), "passwordFor.prompt",
        new Integer(SQLTOOL_VARSET_BADFORMAT), "SqlTool.varset.badformat",
        new Integer(SQLTOOL_ABORTCONTINUE_MUTUALLYEXCLUSIVE),
                "SqlTool.abort_continue.mutuallyexclusive",
        new Integer(SQLTEMPFILE_FAIL), "sqltempfile.fail",
        new Integer(RCDATA_INLINEURL_MISSING), "rcdata.inlineurl.missing",
        new Integer(RCDATA_INLINE_EXTRAVARS), "rcdata.inline.extravars",
        new Integer(RCDATA_INLINEUSERNAME_MISSING),
                "rcdata.inlineusername.missing",
        new Integer(RCDATA_PASSWORD_VISIBLE), "rcdata.password.visible",
        new Integer(PASSWORD_READFAIL), "password.readfail",
        new Integer(CONNECTION_FAIL), "connection.fail",
        new Integer(RCDATA_GENFROMVALUES_FAIL),
                "rcdata.genfromvalues.fail",
        new Integer(CONNDATA_RETRIEVAL_FAIL), "conndata.retrieval.fail",
        new Integer(JDBC_ESTABLISHED), "jdbc.established",

        // SqlFile class, file references:
        new Integer(SQLFILE_BANNER), "SqlFile.banner",
        new Integer(BUFFER_HELP), "buffer.help",
        new Integer(SPECIAL_HELP), "special.help",
        new Integer(PL_HELP), "pl.help",
        new Integer(DSV_OPTIONS), "dsv.options",
        new Integer(D_OPTIONS), "d.options",
        new Integer(RAW_LEADIN), "raw.leadin",
        new Integer(ERRORAT), "errorat",
        new Integer(REJECTREPORT_TOP), "rejectreport.top",
        new Integer(REJECTREPORT_ROW), "rejectreport.row",
        new Integer(REJECTREPORT_BOTTOM), "rejectreport.bottom",
        new Integer(MACRO_HELP), "macro.help",

        // SqlFile inline properties:
        new Integer(RAWMODE_PROMPT), "rawmode.prompt",
        new Integer(RAW_MOVEDTOBUFFER), "raw.movedtobuffer",
        new Integer(INPUT_MOVEDTOBUFFER), "input.movedtobuffer",
        new Integer(SQLSTATEMENT_EMPTY), "sqlstatement.empty",
        new Integer(CAUSEREPORT), "causereport",
        new Integer(BREAK_UNSATISFIED), "break.unsatisfied",
        new Integer(CONTINUE_UNSATISFIED), "continue.unsatisfied",
        new Integer(PRIMARYINPUT_ACCESSFAIL), "primaryinput.accessfail",
        new Integer(INPUT_UNTERMINATED), "input.unterminated",
        new Integer(PLVAR_SET_INCOMPLETE), "plvar.set.incomplete",
        new Integer(ABORTING), "aborting",
        new Integer(ROLLINGBACK), "rollingback",
        new Integer(SPECIAL_UNSPECIFIED), "special.unspecified",
        new Integer(BUFHIST_UNSPECIFIED), "bufhist.unspecified",
        new Integer(BUFFER_EXECUTING), "buffer.executing",
        new Integer(NOBUFFER_YET), "nobuffer.yet",
        new Integer(COMMANDNUM_MALFORMAT), "commandnum.malformat",
        new Integer(BUFFER_RESTORED), "buffer.restored",
        new Integer(SUBSTITUTION_MALFORMAT), "substitution.malformat",
        new Integer(SUBSTITUTION_NOMATCH), "substitution.nomatch",
        new Integer(SUBSTITUTION_SYNTAX), "substitution.syntax",
        new Integer(BUFFER_UNKNOWN), "buffer.unknown",
        new Integer(SPECIAL_EXTRACHARS), "special.extrachars",
        new Integer(BUFFER_EXTRACHARS), "buffer.extrachars",
        new Integer(SPECIAL_MALFORMAT), "special.malformat",
        new Integer(HTML_MODE), "html.mode",
        new Integer(DSV_TARGETFILE_DEMAND), "dsv.targetfile.demand",
        new Integer(FILE_WROTECHARS), "file.wrotechars",
        new Integer(FILE_WRITEFAIL), "file.writefail",
        new Integer(SPECIAL_D_LIKE), "special.d.like",
        new Integer(OUTPUTFILE_NONETOCLOSE), "outputfile.nonetoclose",
        new Integer(OUTPUTFILE_REOPENING), "outputfile.reopening",
        new Integer(OUTPUTFILE_HEADER), "outputfile.header",
        new Integer(DESTFILE_DEMAND), "destfile.demand",
        new Integer(BUFFER_EMPTY), "buffer.empty",
        new Integer(FILE_APPENDFAIL), "file.appendfail",
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
        new Integer(RAW_EMPTY), "raw.empty",
        new Integer(DSV_NOCOLSLEFT), "dsv.nocolsleft",
        new Integer(DSV_SKIPCOLS_MISSING), "dsv.skipcols.missing",
        new Integer(PLALIAS_MALFORMAT), "plalias.malformat",
        new Integer(PLVAR_UNDEFINED), "plvar.undefined",
        new Integer(SYSPROP_EMPTY), "sysprop.empty",
        new Integer(SYSPROP_UNTERMINATED), "sysprop.unterminated",
        new Integer(SYSPROP_UNDEFINED), "sysprop.undefined",
        new Integer(VAR_INFINITE), "var.infinite",
        new Integer(PLVAR_NAMEEMPTY), "plvar.nameempty",
        new Integer(PLVAR_UNTERMINATED), "plvar.unterminated",
        new Integer(PL_MALFORMAT), "pl.malformat",
        new Integer(PL_EXPANSIONMODE), "pl.expansionmode",
        new Integer(END_NOBLOCK), "end.noblock",
        new Integer(CONTINUE_SYNTAX), "continue.syntax",
        new Integer(BREAK_SYNTAX), "break.syntax",
        new Integer(PL_LIST_PARENS), "pl.list.parens",
        new Integer(PL_LIST_LENGTHS), "pl.list.lengths",
        new Integer(DUMPLOAD_MALFORMAT), "dumpload.malformat",
        new Integer(PLVAR_NOCOLON), "plvar.nocolon",
        new Integer(PLVAR_TILDEDASH_NOMOREARGS), "plvar.tildedash.nomoreargs",
        new Integer(DUMPLOAD_FAIL), "dumpload.fail",
        new Integer(PREPARE_MALFORMAT), "prepare.malformat",
        new Integer(FOREACH_MALFORMAT), "foreach.malformat",
        new Integer(PL_BLOCK_FAIL), "pl.block.fail",
        new Integer(IFWHILE_MALFORMAT), "ifwhile.malformat",
        new Integer(IF_MALFORMAT), "if.malformat",
        new Integer(WHILE_MALFORMAT), "while.malformat",
        new Integer(PL_UNKNOWN), "pl.unknown",
        new Integer(PL_BLOCK_UNTERMINATED), "pl.block.unterminated",
        new Integer(VENDOR_ORACLE_DS), "vendor.oracle.dS",
        new Integer(VENDOR_DERBY_DR), "vendor.derby.dr",
        new Integer(VENDOR_NOSUP_D), "vendor.nosup.d",
        new Integer(VENDOR_DERBY_DU), "vendor.derby.du",
        new Integer(SPECIAL_D_UNKNOWN), "special.d.unknown",
        new Integer(METADATA_FETCH_FAIL), "metadata.fetch.fail",
        new Integer(METADATA_FETCH_FAILFOR), "metadata.fetch.failfor",
        new Integer(PREPARE_DEMANDQM), "prepare.demandqm",
        new Integer(BINBUFFER_EMPTY), "binbuffer.empty",
        new Integer(VENDOR_NOSUP_SYSSCHEMAS), "vendor.nosup.sysschemas",
        new Integer(NORESULT), "noresult",
        new Integer(DSV_BINCOL), "dsv.bincol",
        new Integer(BINBUF_WRITE), "binbuf.write",
        new Integer(ROWS_FETCHED), "rows.fetched",
        new Integer(ROWS_FETCHED_DSV), "rows.fetched.dsv",
        new Integer(ROW_UPDATE_SINGULAR), "row.update.singular",
        new Integer(ROW_UPDATE_MULTIPLE), "row.update.multiple",
        new Integer(HISTORY_UNAVAILABLE), "history.unavailable",
        new Integer(HISTORY_NONE), "history.none",
        new Integer(EDITBUFFER_CONTENTS), "editbuffer.contents",
        new Integer(BUFFER_INSTRUCTIONS), "buffer.instructions",
        new Integer(HISTORY_NUMBER_REQ), "history.number.req",
        new Integer(HISTORY_BACKTO), "history.backto",
        new Integer(HISTORY_UPTO), "history.upto",
        new Integer(HISTORY_BACK), "history.back",
        new Integer(DESCRIBE_TABLE_NAME), "describe.table.name",
        new Integer(DESCRIBE_TABLE_DATATYPE), "describe.table.datatype",
        new Integer(DESCRIBE_TABLE_WIDTH), "describe.table.width",
        new Integer(DESCRIBE_TABLE_NONULLS), "describe.table.nonulls",
        new Integer(LOGICAL_UNRECOGNIZED), "logical.unrecognized",
        new Integer(READ_TOOBIG), "read.toobig",
        new Integer(READ_PARTIAL), "read.partial",
        new Integer(READ_CONVERTFAIL), "read.convertfail",
        new Integer(DSV_COLDELIM_PRESENT), "dsv.coldelim.present",
        new Integer(DSV_ROWDELIM_PRESENT), "dsv.rowdelim.present",
        new Integer(DSV_NULLREP_PRESENT), "dsv.nullrep.present",
        new Integer(DSV_CONSTCOLS_NULLCOL), "dsv.constcols.nullcol",
        new Integer(FILE_READFAIL), "file.readfail",
        new Integer(INPUTFILE_CLOSEFAIL), "inputfile.closefail",
        new Integer(DSV_HEADER_NONE), "dsv.header.none",
        new Integer(DSV_HEADER_NOSWITCHTARG), "dsv.header.noswitchtarg",
        new Integer(DSV_HEADER_NOSWITCHMATCH), "dsv.header.noswitchmatch",
        new Integer(DSV_HEADER_NONSWITCHED), "dsv.header.nonswitched",
        new Integer(DSV_NOCOLHEADER), "dsv.nocolheader",
        new Integer(DSV_METADATA_MISMATCH), "dsv.metadata.mismatch",
        new Integer(QUERY_METADATAFAIL), "query.metadatafail",
        new Integer(DSV_REJECTFILE_SETUPFAIL), "dsv.rejectfile.setupfail",
        new Integer(DSV_REJECTREPORT_SETUPFAIL), "dsv.rejectreport.setupfail",
        new Integer(NONE), "none",
        new Integer(INSERTION_PREPAREFAIL), "insertion.preparefail",
        new Integer(DSV_HEADER_MATCHERNONHEAD), "dsv.header.matchernonhead",
        new Integer(DSV_COLCOUNT_MISMATCH), "dsv.colcount.mismatch",
        new Integer(DSV_INSERTCOL_MISMATCH), "dsv.insertcol.mismatch",
        new Integer(TIME_BAD), "time.bad",
        new Integer(BOOLEAN_BAD), "boolean.bad",
        new Integer(INPUTREC_MODIFIED), "inputrec.modified",
        new Integer(DSV_RECIN_FAIL), "dsv.recin.fail",
        new Integer(DSV_IMPORT_SUMMARY), "dsv.import.summary",
        new Integer(INSERTIONS_NOTCOMMITTED), "insertions.notcommitted",
        new Integer(AUTOCOMMIT_FETCHFAIL), "autocommit.fetchfail",
        new Integer(DSV_REJECTFILE_PURGEFAIL), "dsv.rejectfile.purgefail",
        new Integer(DSV_REJECTREPORT_PURGEFAIL), "dsv.rejectreport.purgefail",
        new Integer(EDIT_MALFORMAT), "edit.malformat",
        new Integer(INPUT_MALFORMAT), "input.malformat",
        new Integer(APPEND_EMPTY), "append.empty",
        new Integer(TRANSISO_REPORT), "transiso.report",
        new Integer(EXECTIME_REPORTING), "exectime.reporting",
        new Integer(EXECTIME_REPORT), "exectime.report",
        new Integer(REGEX_MALFORMAT), "regex.malformat",
        new Integer(ENCODE_FAIL), "encode.fail",
        new Integer(MACRO_TIP), "macro.tip",
        new Integer(MACRODEF_MALFORMAT), "macrodef.malformat",
        new Integer(MACRODEF_EMPTY), "macrodef.empty",
        new Integer(MACRODEF_SEMI), "macrodef.semi",
        new Integer(MACRO_MALFORMAT), "macro.malformat",
        new Integer(MACRO_UNDEFINED), "macro.undefined",
        new Integer(LOG_SYNTAX), "log.syntax",
        new Integer(LOG_SYNTAX_ERROR), "log.syntax.error",
        new Integer(REJECT_RPC), "reject.rpc",
        new Integer(RPC_AUTOCOMMIT_FAILURE), "rpc.autocommit.failure",
        new Integer(RPC_COMMIT_FAILURE), "rpc.commit.failure",
        new Integer(DISCONNECT_SUCCESS), "disconnect.success",
        new Integer(DISCONNECT_FAILURE), "disconnect.failure",
        new Integer(NO_REQUIRED_CONN), "no.required.conn",
        new Integer(DISCONNECTED_MSG), "disconnected.msg",
        new Integer(CONNECTED_FALLBACKMSG), "connected.fallbackmsg",
    };

    private Map keyIdToString = new HashMap();

    protected Map getKeyIdToString() {
        return keyIdToString;
    }

    public SqltoolRB() {
        super("org.hsqldb.cmdline.sqltool");
        if (memberKeyArray == null)
            throw new RuntimeException("'static memberKeyArray not set");
        for (int i = 0; i < memberKeyArray.length; i += 2) {
            keyIdToString.put(memberKeyArray[i], memberKeyArray[i+1]);
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
