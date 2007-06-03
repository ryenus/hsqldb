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


package org.hsqldb.util;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

/* $Id$ */

/**
 * Encapsulation of a sql text file like 'myscript.sql'.
 * The ultimate goal is to run the execute() method to feed the SQL
 * commands within the file to a jdbc connection.
 *
 * Some implementation comments and variable names use keywords based
 * on the following definitions.  <UL>
 * <LI> COMMAND = Statement || SpecialCommand || BufferCommand
 * Statement = SQL statement like "SQL Statement;"
 * SpecialCommand =  Special Command like "\x arg..."
 * BufferCommand =  Editing/buffer command like ":s/this/that/"
 *
 * When entering SQL statements, you are always "appending" to the
 * "current" command (not the "buffer", which is a different thing).
 * All you can do to the current command is append new lines to it,
 * execute it, or save it to buffer.
 *
 * In general, the special commands mirror those of Postgresql's psql,
 * but SqlFile handles command editing much different from Postgresql
 * because of Java's lack of support for raw tty I/O.
 * The \p special command, in particular, is very different from psql's.
 * Also, to keep the code simpler, we're sticking to only single-char
 * special commands until we really need more.
 *
 * Buffer commands are unique to SQLFile.  The ":" commands allow
 * you to edit the buffer and to execute the buffer.
 *
 * The command history consists only of SQL Statements (i.e., special
 * commands and editing commands are not stored for later viewing or
 * editing).
 *
 * Most of the Special Commands and Editing Commands are for
 * interactive use only.
 *
 * \d commands are very poorly supported for Mysql because
 * (a) Mysql lacks most of the most basic JDBC support elements, and
 * the most basic role and schema features, and
 * (b) to access the Mysql data dictionay, one must change the database
 * instance (to do that would require work to restore the original state
 * and could have disastrous effects upon transactions).
 *
 * To make changes to this class less destructive to external callers,
 * the input parameters should be moved to setters (probably JavaBean
 * setters would be best) instead of constructor args and System
 * Properties.
 *
 * @version $Revision$
 * @author Blaine Simpson unsaved@users
 */

public class SqlFile {
    /*  The LS's in the help text blocks are extremely ugly.  Once we can
     *  use Java v. 4, we can
     *  write the text using "\n"s, then run replaceAll("\n", LS) in a
     *  static initializer if !equals("\n", "LS)  (or very similar to that). */
    private static final int DEFAULT_HISTORY_SIZE = 20;
    private File             file;
    private boolean          interactive;
    private String           primaryPrompt    = "sql> ";
    private String           chunkPrompt      = "raw> ";
    private String           contPrompt       = "  +> ";
    private Connection       curConn          = null;
    private boolean          htmlMode         = false;
    private Map              userVars         = null;
    private String[]         statementHistory = null;
    private boolean          chunking         = false;
    private String           dsvNullRep       = null;
    public static String     LS = System.getProperty("line.separator");

    /**
     * Private class to "share" a variable among a family of SqlFile
     * instances.
     */
    private static class BooleanBucket {
        BooleanBucket() {}
        private boolean bPriv = false;

        public void set(boolean bIn) {
            bPriv = bIn;
        }

        public boolean get() {
            return bPriv;
        }
    }

    // This is an imperfect solution since when user runs SQL they could
    // be running DDL or a commit or rollback statement.  All we know is,
    // they MAY run some DML that needs to be committed.
    BooleanBucket possiblyUncommitteds = new BooleanBucket();

    // Ascii field separator blanks
    private static final String DIVIDER =
        "-----------------------------------------------------------------"
        + "-----------------------------------------------------------------";
    private static final String SPACES =
        "                                                                 "
        + "                                                                 ";
    private static String revnum = null;

    static {
        revnum = "$Revision$".substring("$Revision: ".length(),
                "$Revision$".length() - 2);
    }

    private static String BANNER =
        "(SqlFile processor v. " + revnum + ")" + LS
        + "Distribution is permitted under the terms of the HSQLDB license." + LS
        + "(c) 2004-2007 Blaine Simpson and the HSQLDB Development Group." + LS + LS
        + "    \\q    to Quit." + LS + "    \\?    lists Special Commands." + LS
        + "    :?    lists Buffer/Editing commands." + LS
        + "    *?    lists PL commands (including alias commands)." + LS + LS
        + "SPECIAL Commands begin with '\\' and execute when you hit ENTER." + LS
        + "BUFFER Commands begin with ':' and execute when you hit ENTER." + LS
        + "COMMENTS begin with '/*' and end with the very next '*/'." + LS
        + "PROCEDURAL LANGUAGE commands begin with '*' and end when you hit ENTER." + LS
        + "All other lines comprise SQL Statements." + LS
        + "  SQL Statements are terminated by either a blank line (which moves the" + LS
        + "  statement into the buffer without executing) or a line ending with ';'" + LS
        + "  (which executes the statement)." + LS
        + "  SQL Statements may begin with '/PLVARNAME' and/or contain *{PLVARNAME}s." + LS;
    private static final String BUFFER_HELP_TEXT =
        "BUFFER Commands (only \":;\" is available for non-interactive use)." + LS
        + "    :?                Help" + LS
        + "    :;                Execute current buffer as an SQL Statement" + LS
        + "    :a[text]          Enter append mode with a copy of the buffer" + LS
        + "    :l                List current contents of buffer" + LS
        + "    :s/from/to        Substitute \"to\" for first occurrence of \"from\"" + LS
        + "    :s/from/to/[i;g2] Substitute \"to\" for occurrence(s) of \"from\"" + LS
        + "                from:  '$'s represent line breaks" + LS
        + "                to:    If empty, from's will be deleted (e.g. \":s/x//\")." + LS
        + "                       '$'s represent line breaks" + LS
        + "                       You can't use ';' in order to execute the SQL (use" + LS
        + "                       the ';' switch for this purpose, as explained below)." + LS
        + "                /:     Can actually be any character which occurs in" + LS
        + "                       neither \"to\" string nor \"from\" string." + LS
        + "                SUBSTITUTION MODE SWITCHES:" + LS
        + "                       i:  case Insensitive" + LS
        + "                       ;:  execute immediately after substitution" + LS
        + "                       g:  Global (substitute ALL occurrences of \"from\" string)" + LS
        + "                       2:  Narrows substitution to specified buffer line number" + LS
        + "                           (Use any line number in place of '2')." + LS
    ;
    private static final String HELP_TEXT = "SPECIAL Commands." + LS
        + "* commands only available for interactive use." + LS
        + "In place of \"3\" below, you can use nothing for the previous command, or" + LS
        + "an integer \"X\" to indicate the Xth previous command." + LS
        + "Filter substrings are cases-sensitive!  Use \"SCHEMANAME.\" to narrow schema." + LS
        + "    \\?                   Help" + LS
        + "    \\p [line to print]   Print string to stdout" + LS
        + "    \\w file/path.sql     Append current buffer to file" + LS
        + "    \\i file/path.sql     Include/execute commands from external file" + LS
        + "    \\d{tvsiSanur*?} [substr]  List objects of specified type:" + LS
        + "  (Tbls/Views/Seqs/Indexes/SysTbls/Aliases/schemaNames/Users/Roles/table-like)" + LS
        + "    \\d OBJECTNAME [subs] Describe table or view columns" + LS
        + "    \\o [file/path.html]  Tee (or stop teeing) query output to specified file" + LS
        + "    \\H                   Toggle HTML output mode" + LS
        + "    \\! COMMAND ARGS      Execute external program (no support for stdin)" + LS
        + "    \\c [true|false]      Continue upon errors (a.o.t. abort upon error)" + LS
        + "    \\a [true|false]      Auto-commit JDBC DML commands" + LS
        + "    \\b                   save next result to Binary buffer (no display)" + LS
        + "    \\bd file/path.bin    Dump Binary buffer to file" + LS
        + "    \\bl file/path.bin    Load file into Binary buffer" + LS
        + "    \\bp                  Use ? in next SQL statement to upload Bin. buffer" + LS
        + "    \\.                   Enter raw SQL.  End with line containing only \".\"" + LS
        + "    \\s                   * Show previous commands (i.e. SQL command history)" + LS
        + "    \\-[3][;]             * reload a command to buffer (opt. exec. w/ \":;\"))" + LS
        + "    \\=                   commit JDBC session" + LS
        + "    \\x {TABLE|SELECT...} eXport table or query to DSV text file (options \\x?)" + LS
        + "    \\m file/path.dsv [*] iMport DSV text file records into a table (opts \\m?)" + LS
        + "    \\q [abort message]   Quit (or you can end input with Ctrl-Z or Ctrl-D)" + LS
    ;
    private static final String PL_HELP_TEXT = "PROCEDURAL LANGUAGE Commands." + LS
        + "    *?                            Help" + LS
        + "    *                             Expand PL variables from now on." + LS
        + "                                  (this is also implied by all the following)." + LS
        + "    * VARNAME = Variable value    Set variable value" + LS
        + "    * VARNAME =                   Unset variable" + LS
        + "    * VARNAME ~                   Set variable value to the value of the very" + LS
        + "                                  next SQL statement executed (see details" + LS
        + "                                  at the bottom of this listing)." + LS
        + "    * VARNAME _                   Same as * VARNAME _, except the query is" + LS
        + "                                  done silently (i.e, no rows to screen)" + LS
        + "    * list[value] [VARNAME1...]   List variable(s) (defaults to all)" + LS
        + "    * load VARNAME path.txt       Load variable value from text file" + LS
        + "    * dump VARNAME path.txt       Dump variable value to text file" + LS
        + "    * prepare VARNAME             Use ? in next SQL statement to upload val." + LS
        + "    * foreach VARNAME ([val1...]) Repeat the following PL block with the" + LS
        + "                                  variable set to each value in turn." + LS
        + "    * if (logical expr)           Execute following PL block only if expr true" + LS
        + "    * while (logical expr)        Repeat following PL block while expr true" + LS
        + "    * end foreach|if|while        Ends a PL block" + LS
        + "    * break [foreach|if|while|file] Exits a PL block or file early" + LS
        + "    * continue [foreach|while]    Exits a PL block iteration early" + LS + LS
        + "Use PL variables (which you have set) like: *{VARNAME}." + LS
        + "You may use /VARNAME instead iff /VARNAME is the first word of a SQL command." + LS
        + "Use PL variables in logical expressions like: *VARNAME." + LS + LS
        + "'* VARNAME ~' or '* VARNAME _' sets the variable value according to the very" + LS
        + "next SQL statement (~ will echo the value, _ will do it silently):" + LS
        + "    Query:  The value of the first field of the first row returned." + LS
        + "    other:  Return status of the command (for updates this will be" + LS
        + "            the number of rows updated)." + LS
    ;

    private static final String DSV_OPTIONS_TEXT =
        "DSV stands for Delimiter-Separated-Values, which is just CSV (comma-"
        + LS
        + "separated-values) but always using a proper delimiter to prevent the"
        + LS
        + "need for quoting and escaping which CSV files have." + LS
        + "All of the DSV PL variables are optional.  To see all PL var. values,"
        + LS + "run '* listvalue'.  Set the values like:" + LS
        + "    * *DSV_COL_DELIM = ," + LS
        + "Don't forget the * indicating a PL command PLUS the leading * in"
        + LS + "all of these variable names.  \\x or \\m below indicates where" + LS
        + "the setting is applicable.  Default value/behavior is in [square brackes]." + LS
        + "    *DSV_SKIP_PREFIX   \\m    Comment line prefix in DSV files.  "
        + "[\"#\"]" + LS
        + "    *DSV_COL_DELIM     \\m\\x  Column delimiter.  "
        + "[\"|\"]" + LS
        + "    *DSV_ROW_DELIM     \\m\\x  Row delimiter" + LS
        + "                              [OS-dependent (Java line.separator)]" + LS
        + "    *DSV_NULL_REP      \\m\\x  String to represent database null.  "
        + "[\"[null]\"]" + LS
        + "    *DSV_TARGET_FILE   \\x    File which exports will write to" + LS
        + "                              [source table name + \".dsv\"]" + LS
        + "    *DSV_TARGET_TABLE  \\m    Table which imports will write to" + LS
        + "                              [DSV filename without extension]" + LS
        + "    *DSV_CONST_COLS    \\m    Column values to write to every row.  "
        + "[None]" + LS
        + "    *DSV_REJECT_FILE   \\m    DSV file to be created with rejected records.  " + LS
        + "                              [None*]" + LS
        + "    *DSV_REJECT_REPORT \\m    HTML report to explain reject records"
        + "[None*]" + LS
        + "* Imports will abort immediately upon the first import record failure, unless" + LS
        + "either *DSV_REJECT_FILE or *DSV_REJECT_REPORT (or both) are set.  (Whether" + LS
        + "SqlTool will roll back and quit depends on your settings for \\c and \\a).";

    private static final String D_OPTIONS_TEXT =
        "\\dX [parameter...] where X is one of the following."  + LS
        + "    t:  list Tables" + LS
        + "    v:  list Views" + LS
        + "    s:  list Sequences" + LS
        + "    i:  list Indexes" + LS
        + "    S:  list System tables" + LS
        + "    a:  list Aliases" + LS
        + "    n:  list schema Names" + LS
        + "    u:  list Users" + LS
        + "    r:  list Roles" + LS
        + "    *:  list table-like objects" + LS;

    /**
     * Interpret lines of input file as SQL Statements, Comments,
     * Special Commands, and Buffer Commands.
     * Most Special Commands and many Buffer commands are only for
     * interactive use.
     *
     * @param inFile  inFile of null means to read stdin.
     * @param inInteractive  If true, prompts are printed, the interactive
     *                       Special commands are enabled, and
     *                       continueOnError defaults to true.
     * @throws IOException  If can't open specified SQL file.
     */
    public SqlFile(File inFile, boolean inInteractive, Map inVars)
            throws IOException {
        file        = inFile;
        interactive = inInteractive;
        userVars    = inVars;

        try {
            statementHistory =
                new String[interactive ? Integer.parseInt(System.getProperty("sqltool.historyLength"))
                                       : 1];
        } catch (Throwable t) {
            statementHistory = null;
        }

        if (statementHistory == null) {
            statementHistory = new String[DEFAULT_HISTORY_SIZE];
        }

        if (file != null &&!file.canRead()) {
            throw new IOException("Can't read SQL file '" + file + "'");
        }
    }

    /**
     * Constructor for reading stdin instead of a file for commands.
     *
     * @see #SqlFile(File,boolean)
     */
    public SqlFile(boolean inInteractive, Map inVars) throws IOException {
        this(null, inInteractive, inVars);
    }

    /**
     * Process all the commands on stdin.
     *
     * @param conn The JDBC connection to use for SQL Commands.
     * @see #execute(Connection,PrintStream,PrintStream,boolean)
     */
    public void execute(Connection conn,
                        Boolean coeOverride)
                        throws SqlToolError, SQLException {
        execute(conn, System.out, System.err, coeOverride);
    }

    /**
     * Process all the commands on stdin.
     *
     * @param conn The JDBC connection to use for SQL Commands.
     * @see #execute(Connection,PrintStream,PrintStream,boolean)
     */
    public void execute(Connection conn,
                        boolean coeOverride)
                        throws SqlToolError, SQLException {
        execute(conn, System.out, System.err, new Boolean(coeOverride));
    }

    // So we can tell how to handle quit and break commands.
    public boolean      recursed     = false;
    private String      curCommand   = null;
    private int         curLinenum   = -1;
    private int         curHist      = -1;
    private PrintStream psStd        = null;
    private PrintStream psErr        = null;
    private PrintWriter pwQuery      = null;
    private PrintWriter pwDsv        = null;
    StringBuffer        stringBuffer = new StringBuffer();
    /*
     * This is reset upon each execute() invocation (to true if interactive,
     * false otherwise).
     */
    private boolean             continueOnError = false;
    private static final String DEFAULT_CHARSET = "US-ASCII";
    private BufferedReader      br              = null;
    private String              charset         = null;

    /**
     * Process all the commands in the file (or stdin) associated with
     * "this" object.
     * Run SQL in the file through the given database connection.
     *
     * This is synchronized so that I can use object variables to keep
     * track of current line number, command, connection, i/o streams, etc.
     *
     * Sets encoding character set to that specified with System Property
     * 'sqlfile.charset'.  Defaults to "US-ASCII".
     *
     * @param conn The JDBC connection to use for SQL Commands.
     * @throws SQLExceptions thrown by JDBC driver.
     * @throws SqlToolError  all other errors.
     *               This includes including QuitNow, BreakException,
     *               ContinueException for recursive calls only.
     */
    public synchronized void execute(Connection conn, PrintStream stdIn,
                                     PrintStream errIn,
                                     Boolean coeOverride)
                                     throws SqlToolError,
                                         SQLException {
        psStd      = stdIn;
        psErr      = errIn;
        curConn    = conn;
        curLinenum = -1;

        String  inputLine;
        String  trimmedCommand;
        String  trimmedInput;
        String  deTerminated;
        boolean inComment = false;    // Gobbling up a comment
        int     postCommentIndex;
        boolean rollbackUncoms = true;

        continueOnError = (coeOverride == null) ? interactive
                                                : coeOverride.booleanValue();

        if (userVars != null && userVars.size() > 0) {
            plMode = true;
        }

        String specifiedCharSet = System.getProperty("sqlfile.charset");

        charset = ((specifiedCharSet == null) ? DEFAULT_CHARSET
                                              : specifiedCharSet);

        try {
            br = new BufferedReader(new InputStreamReader((file == null)
                    ? System.in
                    : new FileInputStream(file), charset));
            curLinenum = 0;

            if (interactive) {
                stdprintln(BANNER);
            }

            while (true) {
                if (interactive) {
                    psStd.print((stringBuffer.length() == 0)
                                ? (chunking ? chunkPrompt
                                            : primaryPrompt)
                                : contPrompt);
                }

                inputLine = br.readLine();

                if (inputLine == null) {
                    /*
                     * This is because interactive EOD on some OSes doesn't
                     * send a line-break, resulting in no linebreak at all
                     * after the SqlFile prompt or whatever happens to be
                     * on their screen.
                     */
                    if (interactive) {
                        psStd.println();
                    }

                    break;
                }

                curLinenum++;

                if (chunking) {
                    if (inputLine.equals(".")) {
                        chunking = false;

                        setBuf(stringBuffer.toString());
                        stringBuffer.setLength(0);

                        if (interactive) {
                            stdprintln("Raw SQL chunk moved into buffer.  "
                                       + "Run \":;\" to execute the chunk.");
                        }
                    } else {
                        if (stringBuffer.length() > 0) {
                            stringBuffer.append('\n');
                        }

                        stringBuffer.append(inputLine);
                    }

                    continue;
                }

                if (inComment) {
                    postCommentIndex = inputLine.indexOf("*/") + 2;

                    if (postCommentIndex > 1) {
                        // I see no reason to leave comments in history.
                        inputLine = inputLine.substring(postCommentIndex);

                        // Empty the buffer.  The non-comment remainder of
                        // this line is either the beginning of a new SQL
                        // or Special command, or an empty line.
                        stringBuffer.setLength(0);

                        inComment = false;
                    } else {
                        // Just completely ignore the input line.
                        continue;
                    }
                }

                trimmedInput = inputLine.trim();

                try {
                    if (stringBuffer.length() == 0) {
                        if (trimmedInput.startsWith("/*")) {
                            postCommentIndex = trimmedInput.indexOf("*/", 2)
                                               + 2;

                            if (postCommentIndex > 1) {
                                // I see no reason to leave comments in
                                // history.
                                inputLine = inputLine.substring(
                                    postCommentIndex + inputLine.length()
                                    - trimmedInput.length());
                                trimmedInput = inputLine.trim();
                            } else {
                                // Just so we get continuation lines:
                                stringBuffer.append("COMMENT");

                                inComment = true;

                                continue;
                            }
                        }

                        // This is just to filter out useless newlines at
                        // beginning of commands.
                        if (trimmedInput.length() == 0) {
                            continue;
                        }

                        if (trimmedInput.charAt(0) == '*'
                                && (trimmedInput.length() < 2
                                    || trimmedInput.charAt(1) != '{')) {
                            processPL((trimmedInput.length() == 1) ? ""
                                                                   : trimmedInput
                                                                   .substring(1)
                                                                   .trim());
                            continue;
                        }

                        if (trimmedInput.charAt(0) == '\\') {
                            processSpecial(trimmedInput.substring(1));
                            continue;
                        }

                        if (trimmedInput.charAt(0) == ':'
                                && (interactive
                                    || (trimmedInput.charAt(1) == ';'))) {
                            processBuffer(trimmedInput.substring(1));
                            continue;
                        }

                        String ucased = trimmedInput.toUpperCase();

                        if (ucased.startsWith("DECLARE")
                                || ucased.startsWith("BEGIN")) {
                            chunking = true;

                            stringBuffer.append(inputLine);

                            if (interactive) {
                                stdprintln(
                                    "Enter RAW SQL.  No \\, :, * commands.  "
                                    + "End with a line containing only \".\":");
                            }

                            continue;
                        }
                    }

                    if (trimmedInput.length() == 0 && interactive &&!inComment) {
                        // Blank lines delimit commands ONLY IN INTERACTIVE
                        // MODE!
                        setBuf(stringBuffer.toString());
                        stringBuffer.setLength(0);
                        stdprintln("Current input moved into buffer.");
                        continue;
                    }

                    deTerminated = SqlFile.deTerminated(inputLine);

                    // A null terminal line (i.e., /\s*;\s*$/) is never useful.
                    if (!trimmedInput.equals(";")) {
                        if (stringBuffer.length() > 0) {
                            stringBuffer.append('\n');
                        }

                        stringBuffer.append((deTerminated == null) ? inputLine
                                                                   : deTerminated);
                    }

                    if (deTerminated == null) {
                        continue;
                    }

                    // If we reach here, then stringBuffer contains a complete
                    // SQL command.
                    curCommand     = stringBuffer.toString();
                    trimmedCommand = curCommand.trim();

                    if (trimmedCommand.length() == 0) {
                        throw new SqlToolError("Empty SQL Statement");
                        // There is nothing inherently wrong with issuing
                        // an empty command, like to test DB server hearlth.
                        // But, this check effectively catches many syntax
                        // errors early, and the DB check can be done by
                        // sending a comment like "// comment".
                    }

                    setBuf(curCommand);
                    processSQL();
                } catch (BadSpecial bs) {
                    errprintln("Error at '"
                               + ((file == null) ? "stdin"
                                                 : file.toString()) + "' line "
                                                 + curLinenum + ':');
                    errprintln("\"" + inputLine + '"');
                    errprintln(bs.getMessage());
                    Throwable cause = bs.getCause();
                    if (cause != null) {
                        errprintln("Cause: " + cause);
                    }

                    if (!continueOnError) {
                        throw new SqlToolError(bs);
                    }
                } catch (SQLException se) {
                    errprintln("SQL Error at '" + ((file == null) ? "stdin"
                                                                  : file.toString()) + "' line "
                                                                  + curLinenum
                                                                      + ':');
                    if (curCommand != null) errprintln("\"" + curCommand + '"');
                    errprintln(se.getMessage());

                    if (!continueOnError) {
                        throw se;
                    }
                } catch (BreakException be) {
                    String msg = be.getMessage();

                    if (recursed) {
                        rollbackUncoms = false;
                        // Recursion level will exit by rethrowing the BE.
                        // We set rollbackUncoms to false because only the
                        // top level should detect break errors and
                        // possibly roll back.
                    } else if (msg == null || msg.equals("file")) {
                        break;
                    } else {
                        errprintln("Unsatisfied break statement"
                                + " (type " + msg + ").");
                    }

                    if (recursed ||!continueOnError) {
                        throw be;
                    }
                } catch (ContinueException ce) {
                    String msg = ce.getMessage();

                    if (recursed) {
                        rollbackUncoms = false;
                    } else {
                        errprintln("Unsatisfied continue statement"
                                   + ((msg == null) ? ""
                                                    : (" (type " + msg
                                                       + ')')) + '.');
                    }

                    if (recursed ||!continueOnError) {
                        throw ce;
                    }
                } catch (QuitNow qn) {
                    throw qn;
                } catch (SqlToolError ste) {
                    errprintln("Error at '"
                               + ((file == null) ? "stdin"
                                                 : file.toString()) + "' line "
                                                 + curLinenum + ':');
                    errprintln("\"" + inputLine + '"');
                    errprintln(ste.getMessage());
                    Throwable cause = ste.getCause();
                    if (cause != null) {
                        errprintln("Cause: " + cause);
                    }
                    if (!continueOnError) {
                        throw ste;
                    }
                }

                stringBuffer.setLength(0);
            }

            if (inComment || stringBuffer.length() != 0) {
                errprintln("Unterminated input:  [" + stringBuffer + ']');

                throw new SqlToolError("Unterminated input:  ["
                                       + stringBuffer + ']');
            }

            rollbackUncoms = false;
            // Exiting gracefully, so don't roll back.
        } catch (IOException ioe) {
            throw new SqlToolError("Error accessing primary input", ioe);
        } catch (QuitNow qn) {
            if (recursed) {
                throw qn;
                // Will rollback if conditions otherwise require.
                // Otherwise top level will decide based upon qn.getMessage().
            }
            rollbackUncoms = (qn.getMessage() != null);

            if (rollbackUncoms) {
                errprintln("Aborting: " + qn.getMessage());
            }

            return;
        } finally {
            closeQueryOutputStream();

            if (fetchingVar != null) {
                errprintln("PL variable setting incomplete:  " + fetchingVar);

                rollbackUncoms = true;
            }

            if (br != null) try {
                br.close();
            } catch (IOException ioe) {
                throw new SqlToolError("Failed to close input reader", ioe);
            }

            if (rollbackUncoms && possiblyUncommitteds.get()) {
                errprintln("Rolling back SQL transaction.");
                curConn.rollback();
                possiblyUncommitteds.set(false);
            }
        }
    }

    /**
     * Returns a copy of given string without a terminating semicolon.
     * If there is no terminating semicolon, null is returned.
     *
     * @param inString Base String, which will not be modified (because
     *                 a "copy" will be returned).
     * @returns Null if inString contains no terminating semi-colon.
     */
    private static String deTerminated(String inString) {
        int index = inString.lastIndexOf(';');

        if (index < 0) {
            return null;
        }

        for (int i = index + 1; i < inString.length(); i++) {
            if (!Character.isWhitespace(inString.charAt(i))) {
                return null;
            }
        }

        return inString.substring(0, index);
    }

    /**
     * Utility nested Exception class for internal use only.
     */
    private class BadSpecial extends AppendableException {
        static final long serialVersionUID = 7162440064026570590L;

        BadSpecial(String s) {
            super(s);
        }
        BadSpecial(String s, Throwable t) {
            super(s, t);
        }
    }

    /**
     * Utility nested Exception class for internal use.
     * This must extend SqlToolError because it has to percolate up from
     * recursions of SqlTool.execute(), yet SqlTool.execute() is public.
     * Therefore, external users have no reason to specifically handle
     * QuitNow.
     */
    private class QuitNow extends SqlToolError {
        static final long serialVersionUID = 1811094258670900488L;

        public QuitNow(String s) {
            super(s);
        }

        public QuitNow() {
            super();
        }
    }

    /**
     * Utility nested Exception class for internal use.
     * This must extend SqlToolError because it has to percolate up from
     * recursions of SqlTool.execute(), yet SqlTool.execute() is public.
     * Therefore, external users have no reason to specifically handle
     * BreakException.
     */
    private class BreakException extends SqlToolError {
        static final long serialVersionUID = 351150072817675994L;

        public BreakException() {
            super();
        }

        public BreakException(String s) {
            super(s);
        }
    }

    /**
     * Utility nested Exception class for internal use.
     * This must extend SqlToolError because it has to percolate up from
     * recursions of SqlTool.execute(), yet SqlTool.execute() is public.
     * Therefore, external users have no reason to specifically handle
     * ContinueException.
     */
    private class ContinueException extends SqlToolError {
        static final long serialVersionUID = 5064604160827106014L;

        public ContinueException() {
            super();
        }

        public ContinueException(String s) {
            super(s);
        }
    }

    /**
     * Utility nested Exception class for internal use only.
     */
    private class BadSwitch extends Exception {
        static final long serialVersionUID = 7325933736897253269L;

        BadSwitch(int i) {
            super(Integer.toString(i));
        }
    }

    /**
     * Utility nested Exception class for internal use only.
     */
    private class RowError extends AppendableException {
        static final long serialVersionUID = 754346434606022750L;

        RowError(String s) {
            super(s);
        }

        RowError(Throwable t) {
            super(t.getMessage(), t);
        }

        RowError(String s, Throwable t) {
            super(s, t);
        }
    }

    /**
     * Process a Buffer/Edit Command.
     *
     * Due to the nature of the goal here, we don't trim() "other" like
     * we do for other kinds of commands.
     *
     * @param inString Complete command, less the leading ':' character.
     * @throws SQLException  thrown by JDBC driver.
     * @throws BadSpecial    special-command-specific errors.
     * @throws SqlToolError  all other errors.
     */
    private void processBuffer(String inString)
    throws BadSpecial, SQLException, SqlToolError {
        char   commandChar = 'i';
        String other       = null;

        if (inString.length() > 0) {
            commandChar = inString.charAt(0);
            other       = inString.substring(1);

            if (other.trim().length() == 0) {
                other = null;
            }
        }

        switch (commandChar) {
            case ';' :
                curCommand = commandFromHistory(0);

                stdprintln("Executing command from buffer:");
                stdprintln(curCommand);
                stdprintln();
                processSQL();

                return;

            case 'a' :
            case 'A' :
                stringBuffer.append(commandFromHistory(0));

                if (other != null) {
                    String deTerminated = SqlFile.deTerminated(other);

                    if (!other.equals(";")) {
                        stringBuffer.append(((deTerminated == null) ? other
                                                                    : deTerminated));
                    }

                    if (deTerminated != null) {
                        // If we reach here, then stringBuffer contains a
                        // complete SQL command.
                        curCommand = stringBuffer.toString();

                        setBuf(curCommand);
                        stdprintln("Executing:");
                        stdprintln(curCommand);
                        stdprintln();
                        processSQL();
                        stringBuffer.setLength(0);

                        return;
                    }
                }

                stdprintln("Appending to:");
                stdprintln(stringBuffer.toString());

                return;

            case 'l' :
            case 'L' :
                stdprintln("Current Buffer:");
                stdprintln(commandFromHistory(0));

                return;

            case 's' :
            case 'S' :

                // Sat Apr 23 14:14:57 EDT 2005.  Changing history behavior.
                // It's very inconvenient to lose all modified SQL
                // commands from history just because _some_ may be modified
                // because they are bad or obsolete.
                boolean modeIC      = false;
                boolean modeGlobal  = false;
                boolean modeExecute = false;
                int     modeLine    = 0;

                try {
                    String       fromHist = commandFromHistory(0);
                    StringBuffer sb       = new StringBuffer(fromHist);

                    if (other == null) {
                        throw new BadSwitch(0);
                    }

                    String delim = other.substring(0, 1);
                    StringTokenizer toker = new StringTokenizer(other, delim,
                        true);

                    if (toker.countTokens() < 4
                            ||!toker.nextToken().equals(delim)) {
                        throw new BadSwitch(1);
                    }

                    String from = toker.nextToken().replace('$', '\n');

                    if (!toker.nextToken().equals(delim)) {
                        throw new BadSwitch(2);
                    }

                    String to = toker.nextToken().replace('$', '\n');

                    if (to.equals(delim)) {
                        to = "";
                    } else {
                        if (toker.countTokens() > 0
                                &&!toker.nextToken().equals(delim)) {
                            throw new BadSwitch(3);
                        }
                    }

                    if (toker.countTokens() > 0) {
                        String opts = toker.nextToken("");

                        for (int j = 0; j < opts.length(); j++) {
                            switch (opts.charAt(j)) {
                                case 'i' :
                                    modeIC = true;
                                    break;

                                case ';' :
                                    modeExecute = true;
                                    break;

                                case 'g' :
                                    modeGlobal = true;
                                    break;

                                case '1' :
                                case '2' :
                                case '3' :
                                case '4' :
                                case '5' :
                                case '6' :
                                case '7' :
                                case '8' :
                                case '9' :
                                    modeLine = Character.digit(opts.charAt(j),
                                                               10);
                                    break;

                                default :
                                    throw new BadSpecial(
                                        "Unknown Substitution option: "
                                        + opts.charAt(j));
                            }
                        }
                    }

                    if (modeIC) {
                        fromHist = fromHist.toUpperCase();
                        from     = from.toUpperCase();
                    }

                    // lineStart will be either 0 or char FOLLOWING a \n.
                    int lineStart = 0;

                    // lineStop is the \n AFTER what we consider.
                    int lineStop = -1;

                    if (modeLine > 0) {
                        for (int j = 1; j < modeLine; j++) {
                            lineStart = fromHist.indexOf('\n', lineStart) + 1;

                            if (lineStart < 1) {
                                throw new BadSpecial(
                                    "There are not " + modeLine
                                    + " lines in the buffer.");
                            }
                        }

                        lineStop = fromHist.indexOf('\n', lineStart);
                    }

                    if (lineStop < 0) {
                        lineStop = fromHist.length();
                    }

                    // System.err.println("["
                    // + fromHist.substring(lineStart, lineStop) + ']');
                    int i;

                    if (modeGlobal) {
                        i = lineStop;

                        while ((i = fromHist.lastIndexOf(from, i - 1))
                                >= lineStart) {
                            sb.replace(i, i + from.length(), to);
                        }
                    } else if ((i = fromHist.indexOf(from, lineStart)) > -1
                               && i < lineStop) {
                        sb.replace(i, i + from.length(), to);
                    }

                    //statementHistory[curHist] = sb.toString();
                    curCommand = sb.toString();

                    setBuf(curCommand);
                    stdprintln((modeExecute ? "Executing"
                                            : "Current Buffer") + ':');
                    stdprintln(curCommand);

                    if (modeExecute) {
                        stdprintln();
                    }
                } catch (BadSwitch badswitch) {
                    throw new BadSpecial(
                        "Substitution syntax:  \":s/from this/to that/i;g2\".  "
                        + "Use '$' for line separations.  ["
                        + badswitch.getMessage() + ']');
                }

                if (modeExecute) {
                    processSQL();
                    stringBuffer.setLength(0);
                }

                return;

            case '?' :
                stdprintln(BUFFER_HELP_TEXT);

                return;
        }

        throw new BadSpecial("Unknown Buffer Command");
    }

    private boolean doPrepare   = false;
    private String  prepareVar  = null;
    private String  dsvColDelim = null;
    private String  dsvSkipPrefix = null;
    private String  dsvRowDelim = null;
    private static final String DSV_X_SYNTAX_MSG =
        "Export syntax:  \\x table_or_view_name "
        + "[column_delimiter [record_delimiter]]";
    private static final String DSV_M_SYNTAX_MSG =
        "Import syntax:  \\m file/path.dsv "
        + "[*]   (* means no comments in DSV file)";

    /**
     * Process a Special Command.
     *
     * @param inString Complete command, less the leading '\' character.
     * @throws SQLException thrown by JDBC driver.
     * @throws BadSpecial special-command-specific errors.
     * @throws SqlToolError all other errors, plus QuitNow,
     *                      BreakException, ContinueException.
     */
    private void processSpecial(String inString)
    throws BadSpecial, QuitNow, SQLException, SqlToolError {
        String arg1, other = null;

        String string = inString;
        // This is just to quiet compiler warning about assigning to
        // parameter pointer.

        if (string.length() < 1) {
            throw new BadSpecial("Null special command");
        }

        if (plMode) {
            string = dereference(string, false);
        }

        StringTokenizer toker = new StringTokenizer(string);

        arg1 = toker.nextToken();

        if (toker.hasMoreTokens()) {
            other = toker.nextToken("").trim();
        }

        switch (arg1.charAt(0)) {
            case 'q' :
                if (other != null) {
                    throw new QuitNow(other);
                }

                throw new QuitNow();
            case 'H' :
                htmlMode = !htmlMode;

                stdprintln("HTML Mode is now set to: " + htmlMode);

                return;

            case 'm' :
                if (arg1.equals("m?") ||
                        (arg1.equals("m") && other != null
                                 && other.equals("?"))) {
                    stdprintln(DSV_OPTIONS_TEXT + LS + DSV_M_SYNTAX_MSG);
                    return;
                }
                if (arg1.length() != 1 || other == null) {
                    throw new BadSpecial(DSV_M_SYNTAX_MSG);
                }
                boolean noComments = other.charAt(other.length() - 1) == '*';

                if (noComments) {
                    dsvSkipPrefix = null;
                    other = other.substring(0, other.length()-1).trim();
                    if (other.length() < 1) {
                        throw new BadSpecial(DSV_M_SYNTAX_MSG);
                    }
                } else {
                    dsvSkipPrefix = SqlFile.convertEscapes(
                            (String) userVars.get("*DSV_SKIP_PREFIX"));
                    if (dsvSkipPrefix == null) {
                        dsvSkipPrefix = DEFAULT_SKIP_PREFIX;
                    }

                }
                dsvColDelim =
                    SqlFile.convertEscapes((String) userVars.get("*DSV_COL_DELIM"));
                if (dsvColDelim == null) {
                    dsvColDelim =
                        SqlFile.convertEscapes((String) userVars.get("*CSV_COL_DELIM"));
                }
                dsvRowDelim =
                    SqlFile.convertEscapes((String) userVars.get("*DSV_ROW_DELIM"));
                if (dsvRowDelim == null) {
                    dsvRowDelim =
                        SqlFile.convertEscapes((String) userVars.get("*CSV_ROW_DELIM"));
                }
                dsvNullRep = (String) userVars.get("*DSV_NULL_REP");
                if (dsvNullRep == null) {
                    dsvNullRep = (String) userVars.get("*CSV_NULL_REP");
                }
                int colonIndex = other.indexOf(" :");
                if (colonIndex > -1 && colonIndex < other.length() - 2) {
                    dsvSkipPrefix = other.substring(colonIndex + 2);
                    other = other.substring(0, colonIndex).trim();
                }

                if (dsvColDelim == null) {
                    dsvColDelim = DEFAULT_COL_DELIM;
                }

                if (dsvRowDelim == null) {
                    dsvRowDelim = DEFAULT_ROW_DELIM;
                }

                if (dsvNullRep == null) {
                    dsvNullRep = DEFAULT_NULL_REP;
                }

                importDsv(other);

                return;

            case 'x' :
                if (arg1.equals("x?") ||
                        (arg1.equals("x") && other != null
                                 && other.equals("?"))) {
                    stdprintln(DSV_OPTIONS_TEXT + LS + DSV_X_SYNTAX_MSG);
                    return;
                }
                try {
                    if (arg1.length() != 1 || other == null) {
                        throw new BadSpecial(DSV_X_SYNTAX_MSG);
                    }

                    String tableName = ((other.indexOf(' ') > 0) ? null
                                                                 : other);

                    dsvColDelim = SqlFile.convertEscapes(
                        (String) userVars.get("*DSV_COL_DELIM"));
                    if (dsvColDelim == null) {
                        dsvColDelim = SqlFile.convertEscapes(
                            (String) userVars.get("*CSV_COL_DELIM"));
                    }
                    dsvRowDelim = SqlFile.convertEscapes(
                        (String) userVars.get("*DSV_ROW_DELIM"));
                    if (dsvRowDelim == null) {
                        dsvRowDelim = SqlFile.convertEscapes(
                            (String) userVars.get("*CSV_ROW_DELIM"));
                    }
                    dsvNullRep = (String) userVars.get("*DSV_NULL_REP");
                    if (dsvNullRep == null) {
                        dsvNullRep = (String) userVars.get("*CSV_NULL_REP");
                    }

                    String dsvFilepath =
                        (String) userVars.get("*DSV_TARGET_FILE");
                    if (dsvFilepath == null) {
                        dsvFilepath = (String) userVars.get("*CSV_FILEPATH");
                    }

                    if (dsvFilepath == null && tableName == null) {
                        throw new BadSpecial(
                            "You must set PL variable '*DSV_TARGET_FILE' in "
                            + "order to use the query variant of \\x");
                    }

                    File dsvFile = new File((dsvFilepath == null)
                                            ? (tableName + ".dsv")
                                            : dsvFilepath);

                    if (dsvColDelim == null) {
                        dsvColDelim = DEFAULT_COL_DELIM;
                    }

                    if (dsvRowDelim == null) {
                        dsvRowDelim = DEFAULT_ROW_DELIM;
                    }

                    if (dsvNullRep == null) {
                        dsvNullRep = DEFAULT_NULL_REP;
                    }

                    pwDsv = new PrintWriter(
                        new OutputStreamWriter(
                            new FileOutputStream(dsvFile), charset));

                    displayResultSet(
                        null,
                        curConn.createStatement().executeQuery(
                            (tableName == null) ? other
                                                : ("SELECT * FROM "
                                                   + tableName)), null, null);
                    pwDsv.flush();
                    stdprintln("Wrote " + dsvFile.length()
                               + " characters to file '" + dsvFile + "'");
                } catch (FileNotFoundException e) {
                    throw new BadSpecial("Failed to write to file '" + other
                                         + "'", e);
                } catch (UnsupportedEncodingException e) {
                    throw new BadSpecial("Failed to write to file '" + other
                                         + "'", e);
                } finally {
                    // Reset all state changes
                    if (pwDsv != null) {
                        pwDsv.close();
                    }

                    pwDsv       = null;
                    dsvColDelim = null;
                    dsvRowDelim = null;
                }

                return;

            case 'd' :
                if (arg1.equals("d?") ||
                        (arg1.equals("d") && other != null
                                 && other.equals("?"))) {
                    stdprintln(D_OPTIONS_TEXT);
                    return;
                }
                if (arg1.length() == 2) {
                    listTables(arg1.charAt(1), other);

                    return;
                }

                if (arg1.length() == 1 && other != null) try {
                    int space = other.indexOf(' ');

                    if (space < 0) {
                        describe(other, null);
                    } else {
                        describe(other.substring(0, space),
                                 other.substring(space + 1).trim());
                    }

                    return;
                } catch (SQLException se) {
                    throw new BadSpecial("Failed to obtain metadata", se);
                }

                throw new BadSpecial("Describe commands must be like "
                                     + "'\\dX' or like '\\d OBJECTNAME'.");
            case 'o' :
                if (other == null) {
                    if (pwQuery == null) {
                        throw new BadSpecial(
                            "There is no query output file to close");
                    }

                    closeQueryOutputStream();

                    return;
                }

                if (pwQuery != null) {
                    stdprintln(
                        "Closing current query output file and opening "
                        + "new one");
                    closeQueryOutputStream();
                }

                try {
                    pwQuery = new PrintWriter(
                        new OutputStreamWriter(
                            new FileOutputStream(other, true), charset));

                    /* Opening in append mode, so it's possible that we will
                     * be adding superfluous <HTML> and <BODY> tags.
                     * I think that browsers can handle that */
                    pwQuery.println((htmlMode ? ("<HTML>" + LS + "<!--")
                                              : "#") + " "
                                                     + (new java.util.Date())
                                                     + ".  Query output from "
                                                     + getClass().getName()
                                                     + (htmlMode
                                                        ? (". -->" + LS + LS
                                                            + "<BODY>")
                                                        : ("." + LS)));
                    pwQuery.flush();
                } catch (Exception e) {
                    throw new BadSpecial("Failed to write to file '" + other
                                         + "':  " + e);
                }

                return;

            case 'w' :
                if (other == null) {
                    throw new BadSpecial(
                        "You must supply a destination file name");
                }

                if (commandFromHistory(0).length() == 0) {
                    throw new BadSpecial("Empty command in buffer");
                }

                try {
                    PrintWriter pw = new PrintWriter(
                        new OutputStreamWriter(
                            new FileOutputStream(other, true), charset));

                    pw.println(commandFromHistory(0) + ';');
                    pw.flush();
                    pw.close();
                } catch (Exception e) {
                    throw new BadSpecial("Failed to append to file '" + other
                                         + "':  " + e);
                }

                return;

            case 'i' :
                if (other == null) {
                    throw new BadSpecial("You must supply an SQL file name");
                }

                try {
                    SqlFile sf = new SqlFile(new File(other), false,
                                             userVars);

                    sf.recursed = true;

                    // Share the possiblyUncommitted state
                    sf.possiblyUncommitteds = possiblyUncommitteds;
                    sf.plMode               = plMode;

                    sf.execute(curConn, continueOnError);
                } catch (ContinueException ce) {
                    throw ce;
                } catch (BreakException be) {
                    String beMessage = be.getMessage();

                    // Handle "file" and plain breaks (by doing nothing)
                    if (beMessage != null &&!beMessage.equals("file")) {
                        throw be;
                    }
                } catch (QuitNow qn) {
                    throw qn;
                } catch (Exception e) {
                    throw new BadSpecial("Failed to execute contents of file '"
                                         + other + "'", e);
                }

                return;

            case 'p' :
                if (other == null) {
                    stdprintln(true);
                } else {
                    stdprintln(other, true);
                }

                return;

            case 'a' :
                if (other != null) {
                    curConn.setAutoCommit(
                        Boolean.valueOf(other).booleanValue());
                }

                stdprintln("Auto-commit is set to: "
                           + curConn.getAutoCommit());

                return;
            case '=' :
                curConn.commit();
                possiblyUncommitteds.set(false);
                stdprintln("Session committed");

                return;

            case 'b' :
                if (arg1.length() == 1) {
                    fetchBinary = true;

                    return;
                }

                if (arg1.charAt(1) == 'p') {
                    doPrepare = true;

                    return;
                }

                if ((arg1.charAt(1) != 'd' && arg1.charAt(1) != 'l')
                        || other == null) {
                    throw new BadSpecial("Malformatted binary command");
                }

                File file = new File(other);

                try {
                    if (arg1.charAt(1) == 'd') {
                        dump(file);
                    } else {
                        binBuffer = SqlFile.loadBinary(file);
                        stdprintln("Loaded " + binBuffer.length
                                   + " bytes into Binary buffer");
                                    }
                } catch (BadSpecial bs) {
                    throw bs;
                } catch (IOException ioe) {
                    throw new BadSpecial(
                        "Failed to load/dump binary data to file '" + other
                        + "'", ioe);
                }

                return;

            case '*' :
            case 'c' :
                if (other != null) {
                    // But remember that we have to abort on some I/O errors.
                    continueOnError = Boolean.valueOf(other).booleanValue();
                }

                stdprintln("Continue-on-error is set to: " + continueOnError);

                return;

            case 's' :
                showHistory();

                return;

            case '-' :
                int     commandsAgo = 0;
                String  numStr;
                boolean executeMode = arg1.charAt(arg1.length() - 1) == ';';

                if (executeMode) {
                    // Trim off terminating ';'
                    arg1 = arg1.substring(0, arg1.length() - 1);
                }

                numStr = (arg1.length() == 1) ? null
                                              : arg1.substring(1,
                                              arg1.length());

                if (numStr == null) {
                    commandsAgo = 0;
                } else {
                    try {
                        commandsAgo = Integer.parseInt(numStr);
                    } catch (NumberFormatException nfe) {
                        throw new BadSpecial("Malformatted command number",
                                nfe);
                    }
                }

                setBuf(commandFromHistory(commandsAgo));

                if (executeMode) {
                    processBuffer(";");
                } else {
                    stdprintln(
                        "RESTORED following command to buffer.  Enter \":?\" "
                        + "to see buffer commands:");
                    stdprintln(commandFromHistory(0));
                }

                return;

            case '?' :
                stdprintln(HELP_TEXT);

                return;

            case '!' :
                InputStream stream;
                byte[]      ba         = new byte[1024];
                String      extCommand = ((arg1.length() == 1) ? ""
                                                               : arg1.substring(1)) + ((arg1.length() > 1 && other != null)
                                                                   ? " "
                                                                   : "") + ((other == null)
                                                                       ? ""
                                                                       : other);

                try {
                    Process proc = Runtime.getRuntime().exec(extCommand);

                    proc.getOutputStream().close();

                    int i;

                    stream = proc.getInputStream();

                    while ((i = stream.read(ba)) > 0) {
                        stdprint(new String(ba, 0, i));
                    }

                    stream.close();

                    stream = proc.getErrorStream();

                    while ((i = stream.read(ba)) > 0) {
                        errprint(new String(ba, 0, i));
                    }

                    stream.close();

                    if (proc.waitFor() != 0) {
                        throw new BadSpecial("External command failed: '"
                                             + extCommand + "'");
                    }
                } catch (Exception e) {
                    throw new BadSpecial("Failed to execute external command '"
                                         + extCommand + "'", e);
                }

                return;

            case '.' :
                chunking = true;

                if (interactive) {
                    stdprintln("Enter RAW SQL.  No \\, :, * commands.  "
                               + "End with a line containing only \".\":");
                }

                return;
        }

        throw new BadSpecial("Unknown Special Command");
    }

    private static final char[] nonVarChars = {
        ' ', '\t', '=', '}', '\n', '\r'
    };

    /**
     * Returns index specifying 1 past end of a variable name.
     *
     * @param inString String containing a variable name
     * @param startIndex Index within inString where the variable name begins
     * @returns Index within inString, 1 past end of the variable name
     */
    static int pastName(String inString, int startIndex) {
        String workString = inString.substring(startIndex);
        int    e          = inString.length();    // Index 1 past end of var name.
        int    nonVarIndex;

        for (int i = 0; i < nonVarChars.length; i++) {
            nonVarIndex = workString.indexOf(nonVarChars[i]);

            if (nonVarIndex > -1 && nonVarIndex < e) {
                e = nonVarIndex;
            }
        }

        return startIndex + e;
    }

    /**
     * Deference PL variables.
     *
     * @throws SqlToolError
     */
    private String dereference(String inString,
                               boolean permitAlias) throws SqlToolError {
        String       varName, varValue;
        StringBuffer expandBuffer = new StringBuffer(inString);
        int          b, e;    // begin and end of name.  end really 1 PAST name

        if (permitAlias && inString.trim().charAt(0) == '/') {
            int slashIndex = inString.indexOf('/');

            e = SqlFile.pastName(inString.substring(slashIndex + 1), 0);

            // In this case, e is the exact length of the var name.
            if (e < 1) {
                throw new SqlToolError("Malformed PL alias use");
            }

            varName  = inString.substring(slashIndex + 1, slashIndex + 1 + e);
            varValue = (String) userVars.get(varName);

            if (varValue == null) {
                throw new SqlToolError("Undefined PL variable:  " + varName);
            }

            expandBuffer.replace(slashIndex, slashIndex + 1 + e,
                                 (String) userVars.get(varName));
        }

        String s;
        boolean permitUnset;
        // Permit unset with:     ${:varname}
        // Prohibit unset with :  ${varnam}

        while (true) {
            s = expandBuffer.toString();
            b = s.indexOf("*{");

            if (b < 0) {
                // No more unexpanded variable uses
                break;
            }

            e = s.indexOf('}', b + 2);

            if (e == b + 2) {
                throw new SqlToolError("Empty PL variable name");
            }

            if (e < 0) {
                throw new SqlToolError("Unterminated PL variable name");
            }

            permitUnset = (s.charAt(b + 2) == ':');

            varName = s.substring(b + (permitUnset ? 3 : 2), e);

            varValue = (String) userVars.get(varName);
            if (varValue == null) {
                if (permitUnset) {
                    varValue = "";
                } else {
                    throw new SqlToolError("Use of undefined PL variable: "
                                           + varName);
                }
            }

            expandBuffer.replace(b, e + 1, varValue);
        }

        return expandBuffer.toString();
    }

    public boolean plMode = false;

    //  PL variable name currently awaiting query output.
    private String  fetchingVar = null;
    private boolean silentFetch = false;
    private boolean fetchBinary = false;

    /**
     * Process a Process Language Command.
     * Nesting not supported yet.
     *
     * @param inString Complete command, less the leading '\' character.
     * @throws BadSpecial special-command-specific errors.
     * @throws SqlToolError all other errors, plus BreakException and
     *                      ContinueException.
     */
    private void processPL(String inString) throws BadSpecial, SqlToolError {
        String string = inString;
        // This is just to quiet compiler warning about assigning to
        // parameter pointer.

        if (string.length() < 1) {
            plMode = true;

            stdprintln("PL variable expansion mode is now on");

            return;
        }

        if (string.charAt(0) == '?') {
            stdprintln(PL_HELP_TEXT);

            return;
        }

        if (plMode) {
            string = dereference(string, false);
        }

        StringTokenizer toker      = new StringTokenizer(string);
        String          arg1       = toker.nextToken();
        String[]        tokenArray = null;

        // If user runs any PL command, we turn PL mode on.
        plMode = true;

        if (userVars == null) {
            userVars = new HashMap();
        }

        if (arg1.equals("end")) {
            throw new BadSpecial("PL end statements may only occur inside of "
                                 + "a PL block");
        }

        if (arg1.equals("continue")) {
            if (toker.hasMoreTokens()) {
                String s = toker.nextToken("").trim();

                if (s.equals("foreach") || s.equals("while")) {
                    throw new ContinueException(s);
                }
                throw new BadSpecial(
                    "Bad continue statement."
                    + "You may use no argument or one of 'foreach', "
                    + "'while'");
            }

            throw new ContinueException();
        }

        if (arg1.equals("break")) {
            if (toker.hasMoreTokens()) {
                String s = toker.nextToken("").trim();

                if (s.equals("foreach") || s.equals("if")
                        || s.equals("while") || s.equals("file")) {
                    throw new BreakException(s);
                }
                throw new BadSpecial(
                    "Bad break statement."
                    + "You may use no argument or one of 'foreach', "
                    + "'if', 'while', 'file'");
            }

            throw new BreakException();
        }

        if (arg1.equals("list") || arg1.equals("listvalue")) {
            String  s;
            boolean doValues = (arg1.equals("listvalue"));

            if (toker.countTokens() == 0) {
                stdprint(SqlFile.formatNicely(userVars, doValues));
            } else {
                tokenArray = SqlFile.getTokenArray(toker.nextToken(""));

                if (doValues) {
                    stdprintln("The outermost parentheses are not part of "
                               + "the values.");
                } else {
                    stdprintln("Showing variable names and length of values "
                               + "(use 'listvalue' to see values).");
                }

                for (int i = 0; i < tokenArray.length; i++) {
                    s = (String) userVars.get(tokenArray[i]);

                    stdprintln("    " + tokenArray[i] + ": "
                               + (doValues ? ("(" + s + ')')
                                           : Integer.toString(s.length())));
                }
            }

            return;
        }

        if (arg1.equals("dump") || arg1.equals("load")) {
            if (toker.countTokens() != 2) {
                throw new BadSpecial("Malformatted PL dump/load command");
            }

            String varName = toker.nextToken();

            if (varName.charAt(0) == ':') {
                throw new BadSpecial("PL variable names may not begin with ':'");
            }
            File   file    = new File(toker.nextToken());

            try {
                if (arg1.equals("dump")) {
                    dump(varName, file);
                } else {
                    load(varName, file, charset);
                }
            } catch (IOException ioe) {
                throw new BadSpecial("Failed to dump/load variable '"
                                     + varName + "' to file '" + file + "'",
                                     ioe);
            }

            return;
        }

        if (arg1.equals("prepare")) {
            if (toker.countTokens() != 1) {
                throw new BadSpecial("Malformatted prepare command");
            }

            String s = toker.nextToken();

            if (userVars.get(s) == null) {
                throw new BadSpecial("Use of unset PL variable: " + s);
            }

            prepareVar = s;
            doPrepare  = true;

            return;
        }

        if (arg1.equals("foreach")) {
            if (toker.countTokens() < 2) {
                throw new BadSpecial("Malformatted PL foreach command (1)");
            }

            String varName   = toker.nextToken();
            if (varName.charAt(0) == ':') {
                throw new BadSpecial("PL variable names may not begin with ':'");
            }
            String parenExpr = toker.nextToken("").trim();

            if (parenExpr.length() < 2 || parenExpr.charAt(0) != '('
                    || parenExpr.charAt(parenExpr.length() - 1) != ')') {
                throw new BadSpecial("Malformatted PL foreach command (2)");
            }

            String[] values = SqlFile.getTokenArray(parenExpr.substring(1,
                parenExpr.length() - 1));
            File   tmpFile = null;
            String varVal;

            try {
                tmpFile = plBlockFile("foreach");
            } catch (IOException ioe) {
                throw new BadSpecial(
                    "Failed to write given PL block temp file", ioe);
            }

            String origval = (String) userVars.get(varName);

            try {
                SqlFile sf;

                for (int i = 0; i < values.length; i++) {
                    try {
                        varVal = values[i];

                        userVars.put(varName, varVal);

                        sf          = new SqlFile(tmpFile, false, userVars);
                        sf.plMode   = true;
                        sf.recursed = true;

                        // Share the possiblyUncommitted state
                        sf.possiblyUncommitteds = possiblyUncommitteds;

                        sf.execute(curConn, continueOnError);
                    } catch (ContinueException ce) {
                        String ceMessage = ce.getMessage();

                        if (ceMessage != null
                                &&!ceMessage.equals("foreach")) {
                            throw ce;
                        }
                    }
                }
            } catch (BreakException be) {
                String beMessage = be.getMessage();

                // Handle "foreach" and plain breaks (by doing nothing)
                if (beMessage != null &&!beMessage.equals("foreach")) {
                    throw be;
                }
            } catch (QuitNow qn) {
                throw qn;
            } catch (Exception e) {
                throw new BadSpecial("Failed to execute SQL from PL block", e);
            }

            if (origval == null) {
                userVars.remove(varName);
            } else {
                userVars.put(varName, origval);
            }

            if (tmpFile != null &&!tmpFile.delete()) {
                throw new BadSpecial(
                    "Error occurred while trying to remove temp file '"
                    + tmpFile + "'");
            }

            return;
        }

        if (arg1.equals("if")) {
            if (toker.countTokens() < 1) {
                throw new BadSpecial("Malformatted PL if command (1)");
            }

            String parenExpr = toker.nextToken("").trim();

            if (parenExpr.length() < 2 || parenExpr.charAt(0) != '('
                    || parenExpr.charAt(parenExpr.length() - 1) != ')') {
                throw new BadSpecial("Malformatted PL if command (2)");
            }

            String[] values = SqlFile.getTokenArray(parenExpr.substring(1,
                parenExpr.length() - 1));
            File tmpFile = null;

            try {
                tmpFile = plBlockFile("if");
            } catch (IOException ioe) {
                throw new BadSpecial(
                    "Failed to write given PL block temp file", ioe);
            }

            try {
                if (eval(values)) {
                    SqlFile sf = new SqlFile(tmpFile, false, userVars);

                    sf.plMode   = true;
                    sf.recursed = true;

                    // Share the possiblyUncommitted state
                    sf.possiblyUncommitteds = possiblyUncommitteds;

                    sf.execute(curConn, continueOnError);
                }
            } catch (BreakException be) {
                String beMessage = be.getMessage();

                // Handle "if" and plain breaks (by doing nothing)
                if (beMessage == null ||!beMessage.equals("if")) {
                    throw be;
                }
            } catch (ContinueException ce) {
                throw ce;
            } catch (QuitNow qn) {
                throw qn;
            } catch (BadSpecial bs) {
                bs.appendMessage("Malformatted PL if command (3)");
                throw bs;
            } catch (Exception e) {
                throw new BadSpecial("Failed to execute SQL from PL block", e);
            }

            if (tmpFile != null &&!tmpFile.delete()) {
                throw new BadSpecial(
                    "Error occurred while trying to remove temp file '"
                    + tmpFile + "'");
            }

            return;
        }

        if (arg1.equals("while")) {
            if (toker.countTokens() < 1) {
                throw new BadSpecial("Malformatted PL while command (1)");
            }

            String parenExpr = toker.nextToken("").trim();

            if (parenExpr.length() < 2 || parenExpr.charAt(0) != '('
                    || parenExpr.charAt(parenExpr.length() - 1) != ')') {
                throw new BadSpecial("Malformatted PL while command (2)");
            }

            String[] values = SqlFile.getTokenArray(parenExpr.substring(1,
                parenExpr.length() - 1));
            File tmpFile = null;

            try {
                tmpFile = plBlockFile("while");
            } catch (IOException ioe) {
                throw new BadSpecial(
                    "Failed to write given PL block temp file", ioe);
            }

            try {
                SqlFile sf;

                while (eval(values)) {
                    try {
                        sf          = new SqlFile(tmpFile, false, userVars);
                        sf.recursed = true;

                        // Share the possiblyUncommitted state
                        sf.possiblyUncommitteds = possiblyUncommitteds;
                        sf.plMode               = true;

                        sf.execute(curConn, continueOnError);
                    } catch (ContinueException ce) {
                        String ceMessage = ce.getMessage();

                        if (ceMessage != null &&!ceMessage.equals("while")) {
                            throw ce;
                        }
                    }
                }
            } catch (BreakException be) {
                String beMessage = be.getMessage();

                // Handle "while" and plain breaks (by doing nothing)
                if (beMessage != null &&!beMessage.equals("while")) {
                    throw be;
                }
            } catch (QuitNow qn) {
                throw qn;
            } catch (BadSpecial bs) {
                bs.appendMessage("Malformatted PL while command (3)");
                throw bs;
            } catch (Exception e) {
                throw new BadSpecial("Failed to execute SQL from PL block", e);
            }

            if (tmpFile != null &&!tmpFile.delete()) {
                throw new BadSpecial(
                    "Error occurred while trying to remove temp file '"
                    + tmpFile + "'");
            }

            return;
        }

        /* Since we don't want to permit both "* VARNAME = X" and
         * "* VARNAME=X" (i.e., whitespace is OPTIONAL in both positions),
         * we can't use the Tokenzier.  Therefore, start over again with
         * the string. */
        toker = null;

        int    index    = SqlFile.pastName(string, 0);
        int    inLength = string.length();
        String varName  = string.substring(0, index);

        if (varName.charAt(0) == ':') {
            throw new BadSpecial("PL variable names may not begin with ':'");
        }

        while (index + 1 < inLength
                && (string.charAt(index) == ' '
                    || string.charAt(index) == '\t')) {
            index++;
        }

        // index now set to the next non-whitespace AFTER the var name.
        if (index + 1 > inLength) {
            throw new BadSpecial("Unterminated PL variable definition");
        }

        String remainder = string.substring(index + 1);

        switch (string.charAt(index)) {
            case '_' :
                silentFetch = true;
            case '~' :
                if (remainder.length() > 0) {
                    throw new BadSpecial(
                        "PL ~/_ set commands take no other args");
                }

                userVars.remove(varName);

                fetchingVar = varName;

                return;

            case '=' :
                if (fetchingVar != null && fetchingVar.equals(varName)) {
                    fetchingVar = null;
                }

                if (remainder.length() > 0) {
                    userVars.put(varName,
                                 string.substring(index + 1).trim());
                } else {
                    userVars.remove(varName);
                }

                return;
        }

        throw new BadSpecial("Unknown PL command (3)");
    }

    /*
     * Read a PL block into a new temp file.
     *
     * WARNING!!! foreach blocks are not yet smart about comments
     * and strings.  We just look for a line beginning with a PL "end"
     * command without worrying about comments or quotes (for now).
     *
     * WARNING!!! This is very rudimentary.
     * Users give up all editing and feedback capabilities while
     * in the foreach loop.
     * A better solution would be to pass current input stream to a
     * new SqlFile.execute() with a mode whereby commands are written
     * to a separate history but not executed.
     *
     * @throws IOException
     * @throws SqlToolError
     */
    private File plBlockFile(String type) throws IOException, SqlToolError {
        String          s;
        StringTokenizer toker;

        // Have already read the if/while/foreach statement, so we are already
        // at nest level 1.  When we reach nestlevel 1 (read 1 net "end"
        // statement), we're at level 0 and return.
        int    nestlevel = 1;
        String curPlCommand;

        if (type == null
                || ((!type.equals("foreach")) && (!type.equals("if"))
                    && (!type.equals("while")))) {
            throw new RuntimeException(
                "Assertion failed.  Unsupported PL block type:  " + type);
        }

        File tmpFile = File.createTempFile("sqltool-", ".sql");
        PrintWriter pw = new PrintWriter(
            new OutputStreamWriter(new FileOutputStream(tmpFile), charset));

        pw.println("/* " + (new java.util.Date()) + ". "
                   + getClass().getName() + " PL block. */");
        pw.println();

        while (true) {
            s = br.readLine();

            if (s == null) {
                errprintln("Unterminated '" + type + "' PL block");

                throw new SqlToolError("Unterminated '" + type
                                       + "' PL block");
            }

            curLinenum++;

            if (s.trim().length() > 1 && s.trim().charAt(0) == '*') {
                toker        = new StringTokenizer(s.trim().substring(1));
                curPlCommand = toker.nextToken();

                // PL COMMAND of some sort.
                if (curPlCommand.equals(type)) {
                    nestlevel++;
                } else if (curPlCommand.equals("end")) {
                    if (toker.countTokens() < 1) {
                        errprintln("PL end statement requires arg of "
                                   + "'foreach' or 'if' or 'while' (1)");

                        throw new SqlToolError(
                            "PL end statement requires arg "
                            + " of 'foreach' or 'if' or 'while' (1)");
                    }

                    String inType = toker.nextToken();

                    if (inType.equals(type)) {
                        nestlevel--;

                        if (nestlevel < 1) {
                            break;
                        }
                    }

                    if ((!inType.equals("foreach")) && (!inType.equals("if"))
                            && (!inType.equals("while"))) {
                        errprintln("PL end statement requires arg of "
                                   + "'foreach' or 'if' or 'while' (2)");

                        throw new SqlToolError(
                            "PL end statement requires arg of "
                            + "'foreach' or 'if' or 'while' (2)");
                    }
                }
            }

            pw.println(s);
        }

        pw.flush();
        pw.close();

        return tmpFile;
    }

    /**
     * Wrapper methods so don't need to call x(..., false) in most cases.
     */
    private void stdprintln() {
        stdprintln(false);
    }

    private void stdprint(String s) {
        stdprint(s, false);
    }

    private void stdprintln(String s) {
        stdprintln(s, false);
    }

    /**
     * Encapsulates normal output.
     *
     * Conditionally HTML-ifies output.
     */
    private void stdprintln(boolean queryOutput) {
        if (htmlMode) {
            psStd.println("<BR>");
        } else {
            psStd.println();
        }

        if (queryOutput && pwQuery != null) {
            if (htmlMode) {
                pwQuery.println("<BR>");
            } else {
                pwQuery.println();
            }

            pwQuery.flush();
        }
    }

    /**
     * Encapsulates error output.
     *
     * Conditionally HTML-ifies error output.
     */
    private void errprint(String s) {
        psErr.print(htmlMode
                    ? ("<DIV style='color:white; background: red; "
                       + "font-weight: bold'>" + s + "</DIV>")
                    : s);
    }

    /**
     * Encapsulates error output.
     *
     * Conditionally HTML-ifies error output.
     */
    private void errprintln(String s) {
        psErr.println(htmlMode
                      ? ("<DIV style='color:white; background: red; "
                         + "font-weight: bold'>" + s + "</DIV>")
                      : s);
    }

    /**
     * Encapsulates normal output.
     *
     * Conditionally HTML-ifies output.
     */
    private void stdprint(String s, boolean queryOutput) {
        psStd.print(htmlMode ? ("<P>" + s + "</P>")
                             : s);

        if (queryOutput && pwQuery != null) {
            pwQuery.print(htmlMode ? ("<P>" + s + "</P>")
                                   : s);
            pwQuery.flush();
        }
    }

    /**
     * Encapsulates normal output.
     *
     * Conditionally HTML-ifies output.
     */
    private void stdprintln(String s, boolean queryOutput) {
        psStd.println(htmlMode ? ("<P>" + s + "</P>")
                               : s);

        if (queryOutput && pwQuery != null) {
            pwQuery.println(htmlMode ? ("<P>" + s + "</P>")
                                     : s);
            pwQuery.flush();
        }
    }

    // Just because users may be used to seeing "[null]" in normal
    // SqlFile output, we use the same default value for null in DSV
    // files, but this DSV null representation can be changed to anything.
    private static final String DEFAULT_NULL_REP = "[null]";
    private static final String DEFAULT_ROW_DELIM =
        System.getProperty("line.separator");
    private static final String DEFAULT_COL_DELIM = "|";
    private static final String DEFAULT_SKIP_PREFIX = "#";
    private static final int    DEFAULT_ELEMENT   = 0,
                                HSQLDB_ELEMENT    = 1,
                                ORACLE_ELEMENT    = 2
    ;

    // These do not specify order listed, just inclusion.
    private static final int[] listMDSchemaCols = { 1 };
    private static final int[] listMDIndexCols  = {
        2, 6, 3, 9, 4, 10, 11
    };

    /** Column numbering starting at 1. */
    private static final int[][] listMDTableCols = {
        {
            2, 3
        },    // Default
        {
            2, 3
        },    // HSQLDB
        {
            2, 3
        },    // Oracle
    };

    /**
     * SYS and SYSTEM are the only base system accounts in Oracle, however,
     * from an empirical perspective, all of these other accounts are
     * system accounts because <UL>
     * <LI> they are hidden from the casual user
     * <LI> they are created by the installer at installation-time
     * <LI> they are used automatically by the Oracle engine when the
     *      specific Oracle sub-product is used
     * <LI> the accounts should not be <I>messed with</I> by database users
     * <LI> the accounts should certainly not be used if the specific
     *      Oracle sub-product is going to be used.
     * </UL>
     *
     * General advice:  If you aren't going to use an Oracle sub-product,
     * then <B>don't install it!</B>
     * Don't blindly accept default when running OUI.
     *
     * If users also see accounts that they didn't create with names like
     * SCOTT, ADAMS, JONES, CLARK, BLAKE, OE, PM, SH, QS, QS_*, these
     * contain sample data and the schemas can safely be removed.
     */
    private static final String[] oracleSysSchemas = {
        "SYS", "SYSTEM", "OUTLN", "DBSNMP", "OUTLN", "MDSYS", "ORDSYS",
        "ORDPLUGINS", "CTXSYS", "DSSYS", "PERFSTAT", "WKPROXY", "WKSYS",
        "WMSYS", "XDB", "ANONYMOUS", "ODM", "ODM_MTR", "OLAPSYS", "TRACESVR",
        "REPADMIN"
    };

    /**
     * Lists available database tables.
     *
     * When a filter is given, we assume that there are no lower-case
     * characters in the object names (which would require "quotes" when
     * creating them).
     *
     * @throws BadSpecial usually wrap a cause (which cause is a
     *                    SQLException in some cases).
     * @throws SqlToolError passed through from other methods in this class.
     */
    private void listTables(char c, String inFilter) throws BadSpecial,
            SqlToolError {
        String   schema  = null;
        int[]    listSet = null;
        String[] types   = null;

        /** For workaround for \T for Oracle */
        String[] additionalSchemas = null;

        /** This is for specific non-getTable() queries */
        Statement statement = null;
        ResultSet rs        = null;
        String    narrower  = "";
        /*
         * Doing case-sensitive filters now, for greater portability.
        String                    filter = ((inFilter == null)
                                          ? null : inFilter.toUpperCase());
         */
        String filter = inFilter;

        try {
            DatabaseMetaData md            = curConn.getMetaData();
            String           dbProductName = md.getDatabaseProductName();

            //System.err.println("DB NAME = (" + dbProductName + ')');
            // Database-specific table filtering.

            /* 3 Types of actions:
             *    1) Special handling.  Return from the "case" block directly.
             *    2) Execute a specific query.  Set statement in the "case".
             *    3) Otherwise, set filter info for dbmd.getTable() in the
             *       "case".
             */
            types = new String[1];

            switch (c) {
                case '*' :
                    types = null;
                    break;

                case 'S' :
                    if (dbProductName.indexOf("Oracle") > -1) {
                        errprintln("*** WARNING:");
                        errprintln("*** Listing tables in "
                            + "system-supplied schemas since");
                        errprintln("*** Oracle"
                            + "(TM) doesn't return a JDBC system table list.");

                        types[0]          = "TABLE";
                        schema            = "SYS";
                        additionalSchemas = oracleSysSchemas;
                    } else {
                        types[0] = "SYSTEM TABLE";
                    }
                    break;

                case 's' :
                    if (dbProductName.indexOf("HSQL") > -1) {
                        //  HSQLDB does not consider Sequences as "tables",
                        //  hence we do not list them in
                        //  DatabaseMetaData.getTables().
                        if (filter != null
                                && filter.charAt(filter.length() - 1)
                                   == '.') {
                            narrower =
                                "\nWHERE sequence_schema = '"
                                + filter.substring(0, filter.length() - 1)
                                + "'";
                            filter = null;
                        }

                        statement = curConn.createStatement();

                        statement.execute(
                            "SELECT sequence_schema, sequence_name FROM "
                            + "information_schema.system_sequences"
                            + narrower);
                    } else {
                        types[0] = "SEQUENCE";
                    }
                    break;

                case 'r' :
                    if (dbProductName.indexOf("HSQL") > -1) {
                        statement = curConn.createStatement();

                        statement.execute(
                            "SELECT authorization_name FROM "
                            + "information_schema.system_authorizations\n"
                            + "WHERE authorization_type = 'ROLE'\n"
                            + "ORDER BY authorization_name");
                    } else if (dbProductName.indexOf(
                            "Adaptive Server Enterprise") > -1) {
                        // This is the basic Sybase server.  Sybase also has
                        // their "Anywhere", ASA (for embedded), and replication
                        // databases, but I don't know the Metadata strings for
                        // those.
                        statement = curConn.createStatement();

                        statement.execute(
                            "SELECT name FROM syssrvroles ORDER BY name");
                    } else if (dbProductName.indexOf(
                            "Apache Derby") > -1) {
                        throw new BadSpecial(
                            "Derby has not implemented SQL Roles");
                    } else {
                        throw new BadSpecial(
                            "SqlFile does not yet support "
                            + "\\dr for your database vendor");
                    }
                    break;

                case 'u' :
                    if (dbProductName.indexOf("HSQL") > -1) {
                        statement = curConn.createStatement();

                        statement.execute(
                            "SELECT user, admin FROM "
                            + "information_schema.system_users\n"
                            + "ORDER BY user");
                    } else if (dbProductName.indexOf("Oracle") > -1) {
                        statement = curConn.createStatement();

                        statement.execute(
                            "SELECT username, created FROM all_users "
                            + "ORDER BY username");
                    } else if (dbProductName.indexOf("PostgreSQL") > -1) {
                        statement = curConn.createStatement();

                        statement.execute(
                            "SELECT usename, usesuper FROM pg_catalog.pg_user "
                            + "ORDER BY usename");
                    } else if (dbProductName.indexOf(
                            "Adaptive Server Enterprise") > -1) {
                        // This is the basic Sybase server.  Sybase also has
                        // their "Anywhere", ASA (for embedded), and replication
                        // databases, but I don't know the Metadata strings for
                        // those.
                        statement = curConn.createStatement();

                        statement.execute(
                            "SELECT name, accdate, fullname FROM syslogins "
                            + "ORDER BY name");
                    } else if (dbProductName.indexOf(
                            "Apache Derby") > -1) {
                        throw new BadSpecial(
                            "It's impossible to get a reliable user list from "
                            + "Derby");
                    } else {
                        throw new BadSpecial(
                            "SqlFile does not yet support "
                            + "\\du for your database vendor");
                    }
                    break;

                case 'a' :
                    if (dbProductName.indexOf("HSQL") > -1) {
                        //  HSQLDB Aliases are not the same things as the
                        //  aliases listed in DatabaseMetaData.getTables().
                        if (filter != null
                                && filter.charAt(filter.length() - 1)
                                   == '.') {
                            narrower =
                                "\nWHERE alias_schem = '"
                                + filter.substring(0, filter.length() - 1)
                                + "'";
                            filter = null;
                        }

                        statement = curConn.createStatement();

                        statement.execute(
                            "SELECT alias_schem, alias FROM "
                            + "information_schema.system_aliases" + narrower);
                    } else {
                        types[0] = "ALIAS";
                    }
                    break;

                case 't' :
                    excludeSysSchemas = (dbProductName.indexOf("Oracle")
                                         > -1);
                    types[0] = "TABLE";
                    break;

                case 'v' :
                    types[0] = "VIEW";
                    break;

                case 'n' :
                    rs = md.getSchemas();

                    if (rs == null) {
                        throw new BadSpecial(
                            "Failed to get metadata from database");
                    }

                    displayResultSet(null, rs, listMDSchemaCols, filter);

                    return;

                case 'i' :

                    // Some databases require to specify table, some don't.
                    /*
                    if (filter == null) {
                        throw new BadSpecial("You must specify the index's "
                                + "table as argument to \\di");
                    }
                     */
                    String table = null;

                    if (filter != null) {
                        int dotat = filter.indexOf('.');

                        schema = ((dotat > 0) ? filter.substring(0, dotat)
                                              : null);

                        if (dotat < filter.length() - 1) {
                            // Not a schema-only specifier
                            table = ((dotat > 0) ? filter.substring(dotat + 1)
                                                 : filter);
                        }

                        filter = null;
                    }

                    // N.b. Oracle incorrectly reports the INDEX SCHEMA as
                    // the TABLE SCHEMA.  The Metadata structure seems to
                    // be designed with the assumption that the INDEX schema
                    // will be the same as the TABLE schema.
                    rs = md.getIndexInfo(null, schema, table, false, true);

                    if (rs == null) {
                        throw new BadSpecial(
                            "Failed to get metadata from database");
                    }

                    displayResultSet(null, rs, listMDIndexCols, null);

                    return;

                default :
                    throw new BadSpecial("Unknown describe option: '" + c
                                         + "'" + LS + D_OPTIONS_TEXT);
            }

            if (statement == null) {
                if (dbProductName.indexOf("HSQL") > -1) {
                    listSet = listMDTableCols[HSQLDB_ELEMENT];
                } else if (dbProductName.indexOf("Oracle") > -1) {
                    listSet = listMDTableCols[ORACLE_ELEMENT];
                } else {
                    listSet = listMDTableCols[DEFAULT_ELEMENT];
                }

                if (schema == null && filter != null
                        && filter.charAt(filter.length() - 1) == '.') {
                    schema = filter.substring(0, filter.length() - 1);
                    filter = null;
                }
            }

            rs = ((statement == null)
                  ? md.getTables(null, schema, null, types)
                  : statement.getResultSet());

            if (rs == null) {
                throw new BadSpecial("Failed to get metadata from database");
            }

            displayResultSet(null, rs, listSet, filter);

            if (additionalSchemas != null) {
                for (int i = 1; i < additionalSchemas.length; i++) {
                    /*
                     * Inefficient, but we have to do each successful query
                     * twice in order to prevent calling displayResultSet
                     * for empty/non-existent schemas
                     */
                    rs = md.getTables(null, additionalSchemas[i], null,
                                      types);

                    if (rs == null) {
                        throw new BadSpecial(
                            "Failed to get metadata from database for '"
                            + additionalSchemas[i] + "'");
                    }

                    if (!rs.next()) {
                        continue;
                    }

                    displayResultSet(
                        null,
                        md.getTables(
                            null, additionalSchemas[i], null, types), listSet, filter);
                }
            }
        } catch (SQLException se) {
            throw new BadSpecial("Failure to retrieve MetaData", se);
        } catch (NullPointerException npe) {
            throw new BadSpecial("Failure to retrieve MetaData", npe);
        } finally {
            excludeSysSchemas = false;

            if (rs != null) {
                rs = null;
            }

            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {}

                statement = null;
            }
        }
    }

    private boolean excludeSysSchemas = false;

    /**
     * Process the current command as an SQL Statement
     *
     * @throws SQLException thrown by JDBC driver.
     * @throws SqlToolError all other errors.
     */
    private void processSQL() throws SQLException, SqlToolError {
        // Really don't know whether to take the network latency hit here
        // in order to check autoCommit in order to set
        // possiblyUncommitteds more accurately.
        // I'm going with "NO" for now, since autoCommit will usually be off.
        // If we do ever check autocommit, we have to keep track of the
        // autocommit state when every SQL statement is run, since I may
        // be able to have uncommitted DML, turn autocommit on, then run
        // other DDL with autocommit on.  As a result, I could be running
        // SQL commands with autotommit on but still have uncommitted mods.
        String    sql       = (plMode ? dereference(curCommand, true)
                                      : curCommand);
        Statement statement = null;

        if (doPrepare) {
            if (sql.indexOf('?') < 1) {
                throw new SqlToolError(
                    "Prepared statements must contain one '?'");
            }

            doPrepare = false;

            PreparedStatement ps = curConn.prepareStatement(sql);

            if (prepareVar == null) {
                if (binBuffer == null) {
                    throw new SqlToolError("Binary SqlFile buffer is empty");
                }

                ps.setBytes(1, binBuffer);
            } else {
                String val = (String) userVars.get(prepareVar);

                if (val == null) {
                    throw new SqlToolError("PL Variable '" + prepareVar
                                           + "' is empty");
                }

                prepareVar = null;

                ps.setString(1, val);
            }

            ps.executeUpdate();

            statement = ps;
        } else {
            statement = curConn.createStatement();

            statement.execute(sql);
        }

        possiblyUncommitteds.set(true);

        try {
            displayResultSet(statement, statement.getResultSet(), null, null);
        } finally {
            try {
                statement.close();
            } catch (Exception e) {}
        }
    }

    /**
     * Display the given result set for user.
     * The last 3 params are to narrow down records and columns where
     * that can not be done with a where clause (like in metadata queries).
     *
     * @param statement The SQL Statement that the result set is for.
     *                  (This is so we can get the statement's update count.
     *                  Can be null for non-update queries.)
     * @param r         The ResultSet to display.
     * @param incCols   Optional list of which columns to include (i.e., if
     *                  given, then other columns will be skipped).
     * @param incFilter Optional case-insensitive substring.
     *                  Rows are skipped which to not contain this substring.
     * @throws SQLException thrown by JDBC driver.
     * @throws SqlToolError all other errors.
     */
    private void displayResultSet(Statement statement, ResultSet r,
                                  int[] incCols,
                                  String filter) throws SQLException,
                                  SqlToolError {
        java.sql.Timestamp ts;
        int dotAt;
        int                updateCount = (statement == null) ? -1
                                                             : statement
                                                                 .getUpdateCount();
        boolean            silent      = silentFetch;
        boolean            binary      = fetchBinary;

        silentFetch = false;
        fetchBinary = false;

        if (excludeSysSchemas) {
            stdprintln("*** WARNING:");
            stdprintln("*** Omitting tables from system-supplied schemas");
            stdprintln(
                "*** (because DB vendor doesn't differentiate them to JDBC).");
        }

        switch (updateCount) {
            case -1 :
                if (r == null) {
                    stdprintln("No result", true);

                    break;
                }

                ResultSetMetaData m        = r.getMetaData();
                int               cols     = m.getColumnCount();
                int               incCount = (incCols == null) ? cols
                                                               : incCols
                                                                   .length;
                String            val;
                List              rows        = new ArrayList();
                String[]          headerArray = null;
                String[]          fieldArray;
                int[]             maxWidth = new int[incCount];
                int               insi;
                boolean           skip;

                // STEP 1: GATHER DATA
                if (!htmlMode) {
                    for (int i = 0; i < maxWidth.length; i++) {
                        maxWidth[i] = 0;
                    }
                }

                boolean[] rightJust = new boolean[incCount];
                int[]     dataType  = new int[incCount];
                boolean[] autonulls = new boolean[incCount];

                insi        = -1;
                headerArray = new String[incCount];

                for (int i = 1; i <= cols; i++) {
                    if (incCols != null) {
                        skip = true;

                        for (int j = 0; j < incCols.length; j++) {
                            if (i == incCols[j]) {
                                skip = false;
                            }
                        }

                        if (skip) {
                            continue;
                        }
                    }

                    headerArray[++insi] = m.getColumnLabel(i);
                    dataType[insi]      = m.getColumnType(i);
                    rightJust[insi]     = false;
                    autonulls[insi]     = true;

                    switch (dataType[insi]) {
                        case java.sql.Types.BIGINT :
                        case java.sql.Types.BIT :
                        case java.sql.Types.DECIMAL :
                        case java.sql.Types.DOUBLE :
                        case java.sql.Types.FLOAT :
                        case java.sql.Types.INTEGER :
                        case java.sql.Types.NUMERIC :
                        case java.sql.Types.REAL :
                        case java.sql.Types.SMALLINT :
                        case java.sql.Types.TINYINT :
                            rightJust[insi] = true;
                            break;

                        case java.sql.Types.VARBINARY :
                        case java.sql.Types.VARCHAR :
                            autonulls[insi] = false;
                            break;
                    }

                    if (htmlMode) {
                        continue;
                    }

                    if (headerArray[insi].length() > maxWidth[insi]) {
                        maxWidth[insi] = headerArray[insi].length();
                    }
                }

                boolean filteredOut;

                while (r.next()) {
                    fieldArray  = new String[incCount];
                    insi        = -1;
                    filteredOut = filter != null;

                    for (int i = 1; i <= cols; i++) {
                        // This is the only case where we can save a data
                        // read by recognizing we don't need this datum early.
                        if (incCols != null) {
                            skip = true;

                            for (int j = 0; j < incCols.length; j++) {
                                if (i == incCols[j]) {
                                    skip = false;
                                }
                            }

                            if (skip) {
                                continue;
                            }
                        }

                        // This row may still be ditched, but it is now
                        // certain that we need to increment the fieldArray
                        // index.
                        ++insi;

                        if (!SqlFile.canDisplayType(dataType[insi])) {
                            binary = true;
                        }

                        val = null;

                        if (!binary) {
                            /*
                             * The special formatting for all time-related
                             * fields is because the most popular current
                             * databases are extremely inconsistent about
                             * what resolution is returned for the same types.
                             * In my experience so far, Dates MAY have
                             * resolution down to second, but only TIMESTAMPs
                             * support sub-second res. (and always can).
                             * On top of that there is no consistency across
                             * getObject().toString().  Oracle doesn't even
                             * implement it for their custom TIMESTAMP type.
                             */
                            switch (dataType[insi]) {
                                case java.sql.Types.TIMESTAMP:
                                case java.sql.Types.DATE:
                                case java.sql.Types.TIME:
                                    ts  = r.getTimestamp(i);
                                    val = ((ts == null) ? null : ts.toString());
                                    // Following block truncates non-zero
                                    // sub-seconds from time types OTHER than 
                                    // TIMESTAMP.
                                    if (dataType[insi]
                                            != java.sql.Types.TIMESTAMP
                                            && val != null) {
                                        dotAt = val.lastIndexOf('.');
                                        for (int z = dotAt + 1;
                                                z < val.length(); z++) {
                                            if (val.charAt(z) != '0') {
                                                dotAt = 0;
                                                break;
                                            }
                                        }
                                        if (dotAt > 1) {
                                            val = val.substring(0, dotAt);
                                        }
                                    }
                                    break;
                                default:
                                    val = r.getString(i);

                                    // If we tried to get a String but it
                                    // failed, try getting it with a String
                                    // Stream
                                    if (val == null) {
                                        try {
                                            val = SqlFile.streamToString(
                                                r.getAsciiStream(i),
                                                charset);
                                        } catch (Exception e) {}
                                    }
                            }
                        }

                        if (binary || (val == null &&!r.wasNull())) {
                            if (pwDsv != null) {
                                throw new SqlToolError(
                                    "Table has a binary column.  DSV files "
                                    + "are text, not binary, files");
                            }

                            // DB has a value but we either explicitly want
                            // it as binary, or we failed to get it as String.
                            try {
                                binBuffer =
                                    SqlFile.streamToBytes(r.getBinaryStream(i));
                            } catch (IOException ioe) {
                                throw new SqlToolError(
                                    "Failed to read value using stream",
                                    ioe);
                            }

                            stdprintln("Read " + binBuffer.length
                                       + " bytes from field '"
                                       + headerArray[insi] + "' (type "
                                       + SqlFile.sqlTypeToString(dataType[insi])
                                       + ") into binary buffer");

                            return;
                        }

                        if (excludeSysSchemas && val != null && i == 2) {
                            for (int z = 0; z < oracleSysSchemas.length;
                                    z++) {
                                if (val.equals(oracleSysSchemas[z])) {
                                    filteredOut = true;

                                    break;
                                }
                            }
                        }

                        if (fetchingVar != null) {
                            userVars.put(fetchingVar, val);

                            fetchingVar = null;
                        }

                        if (silent) {
                            return;
                        }

                        // We do not omit rows here.  We collect information
                        // so we can make the decision after all rows are
                        // read in.
                        if (filter != null
                                && (val == null
                                    || val.indexOf(filter) > -1)) {
                            filteredOut = false;
                        }

                        ///////////////////////////////
                        // A little tricky here.  fieldArray[] MUST get set.
                        if (val == null && pwDsv == null) {
                            if (dataType[insi] == java.sql.Types.VARCHAR) {
                                fieldArray[insi] = (htmlMode ? "<I>null</I>"
                                                             : "[null]");
                            } else {
                                fieldArray[insi] = "";
                            }
                        } else {
                            fieldArray[insi] = val;
                        }

                        ///////////////////////////////
                        if (htmlMode || pwDsv != null) {
                            continue;
                        }

                        if (fieldArray[insi].length() > maxWidth[insi]) {
                            maxWidth[insi] = fieldArray[insi].length();
                        }
                    }

                    if (!filteredOut) {
                        rows.add(fieldArray);
                    }
                }

                // STEP 2: DISPLAY DATA  (= 2a OR 2b)
                // STEP 2a (Non-DSV)
                if (pwDsv == null) {
                    condlPrintln("<TABLE border='1'>", true);

                    if (incCount > 1) {
                        condlPrint(SqlFile.htmlRow(COL_HEAD) + LS + PRE_TD, true);

                        for (int i = 0; i < headerArray.length; i++) {
                            condlPrint("<TD>" + headerArray[i] + "</TD>",
                                       true);
                            condlPrint(((i > 0) ? SqlFile.spaces(2)
                                                : "") + SqlFile.pad(
                                                    headerArray[i],
                                                    maxWidth[i],
                                                    rightJust[i],
                                                    (i < headerArray.length
                                                     - 1 || rightJust[i])), false);
                        }

                        condlPrintln(LS + PRE_TR + "</TR>", true);
                        condlPrintln("", false);

                        if (!htmlMode) {
                            for (int i = 0; i < headerArray.length; i++) {
                                condlPrint(((i > 0) ? SqlFile.spaces(2)
                                                    : "") + SqlFile.divider(
                                                        maxWidth[i]), false);
                            }

                            condlPrintln("", false);
                        }
                    }

                    for (int i = 0; i < rows.size(); i++) {
                        condlPrint(SqlFile.htmlRow(((i % 2) == 0) ? COL_EVEN
                                                          : COL_ODD) + LS
                                                          + PRE_TD, true);

                        fieldArray = (String[]) rows.get(i);

                        for (int j = 0; j < fieldArray.length; j++) {
                            condlPrint("<TD>" + fieldArray[j] + "</TD>",
                                       true);
                            condlPrint(((j > 0) ? SqlFile.spaces(2)
                                                : "") + SqlFile.pad(
                                                    fieldArray[j],
                                                    maxWidth[j],
                                                    rightJust[j],
                                                    (j < fieldArray.length
                                                     - 1 || rightJust[j])), false);
                        }

                        condlPrintln(LS + PRE_TR + "</TR>", true);
                        condlPrintln("", false);
                    }

                    condlPrintln("</TABLE>", true);

                    if (rows.size() != 1) {
                        stdprintln(LS + rows.size() + " rows", true);
                    }

                    condlPrintln("<HR>", true);

                    break;
                }

                // STEP 2b (DSV)
                if (incCount > 0) {
                    for (int i = 0; i < headerArray.length; i++) {
                        dsvSafe(headerArray[i]);
                        pwDsv.print(headerArray[i]);

                        if (i < headerArray.length - 1) {
                            pwDsv.print(dsvColDelim);
                        }
                    }

                    pwDsv.print(dsvRowDelim);
                }

                for (int i = 0; i < rows.size(); i++) {
                    fieldArray = (String[]) rows.get(i);

                    for (int j = 0; j < fieldArray.length; j++) {
                        dsvSafe(fieldArray[j]);
                        pwDsv.print((fieldArray[j] == null)
                                    ? (autonulls[j] ? ""
                                                    : dsvNullRep)
                                    : fieldArray[j]);

                        if (j < fieldArray.length - 1) {
                            pwDsv.print(dsvColDelim);
                        }
                    }

                    pwDsv.print(dsvRowDelim);
                }

                stdprintln(Integer.toString(rows.size())
                           + " rows read from DB");
                break;

            default :
                if (fetchingVar != null) {
                    userVars.put(fetchingVar, Integer.toString(updateCount));

                    fetchingVar = null;
                }

                if (updateCount != 0) {
                    stdprintln(Integer.toString(updateCount) + " row"
                               + ((updateCount == 1) ? ""
                                                     : "s") + " updated");
                }
                break;
        }
    }

    private static final int    COL_HEAD = 0,
                                COL_ODD  = 1,
                                COL_EVEN = 2
    ;
    private static final String PRE_TR   = spaces(4);
    private static final String PRE_TD   = spaces(8);

    /**
     * Print a properly formatted HTML &lt;TR&gt; command for the given
     * situation.
     *
     * @param colType Column type:  COL_HEAD, COL_ODD or COL_EVEN.
     */
    private static String htmlRow(int colType) {
        switch (colType) {
            case COL_HEAD :
                return PRE_TR + "<TR style='font-weight: bold;'>";

            case COL_ODD :
                return PRE_TR
                       + "<TR style='background: #94d6ef; font: normal "
                       + "normal 10px/10px Arial, Helvitica, sans-serif;'>";

            case COL_EVEN :
                return PRE_TR
                       + "<TR style='background: silver; font: normal "
                       + "normal 10px/10px Arial, Helvitica, sans-serif;'>";
        }

        return null;
    }

    /**
     * Returns a divider of hypens of requested length.
     *
     * @param len Length of output String.
     */
    private static String divider(int len) {
        return (len > DIVIDER.length()) ? DIVIDER
                                        : DIVIDER.substring(0, len);
    }

    /**
     * Returns a String of spaces of requested length.
     *
     * @param len Length of output String.
     */
    private static String spaces(int len) {
        return (len > SPACES.length()) ? SPACES
                                       : SPACES.substring(0, len);
    }

    /**
     * Pads given input string out to requested length with space
     * characters.
     *
     * @param inString Base string.
     * @param fulllen  Output String length.
     * @param rightJustify  True to right justify, false to left justify.
     */
    private static String pad(String inString, int fulllen,
                              boolean rightJustify, boolean doPad) {
        if (!doPad) {
            return inString;
        }

        int len = fulllen - inString.length();

        if (len < 1) {
            return inString;
        }

        String pad = SqlFile.spaces(len);

        return ((rightJustify ? pad
                              : "") + inString + (rightJustify ? ""
                                                               : pad));
    }

    /**
     * Display command history, which consists of complete or incomplete SQL
     * commands.
     */
    private void showHistory() {
        int      ctr = -1;
        String   s;
        String[] reversedList = new String[statementHistory.length];

        try {
            for (int i = curHist; i >= 0; i--) {
                s = statementHistory[i];

                if (s == null) {
                    return;
                }

                reversedList[++ctr] = s;
            }

            for (int i = statementHistory.length - 1; i > curHist; i--) {
                s = statementHistory[i];

                if (s == null) {
                    return;
                }

                reversedList[++ctr] = s;
            }
        } finally {
            if (ctr < 0) {
                stdprintln("<<<    No history yet    >>>");

                return;
            }

            for (int i = ctr; i >= 0; i--) {
                psStd.println(((i == 0) ? "BUFR"
                                        : ("-" + i + "  ")) + " **********************************************"
                                        + LS + reversedList[i]);
            }

            psStd.println();
            psStd.println(
                "<<<  Copy a command to buffer like \"\\-3\"       "
                + "Re-execute buffer like \":;\"  >>>");
        }
    }

    /**
     * Return a SQL Command from command history.
     */
    private String commandFromHistory(int commandsAgo) throws BadSpecial {
        if (commandsAgo >= statementHistory.length) {
            throw new BadSpecial("History can only hold up to "
                                 + statementHistory.length + " commands");
        }

        String s =
            statementHistory[(statementHistory.length + curHist - commandsAgo) % statementHistory.length];

        if (s == null) {
            throw new BadSpecial("History doesn't go back that far");
        }

        return s;
    }

    /**
     * Push a command onto the history array (the first element of which
     * is the "Buffer").
     */
    private void setBuf(String inString) {
        curHist++;

        if (curHist == statementHistory.length) {
            curHist = 0;
        }

        statementHistory[curHist] = inString;
    }

    /**
     * Describe the columns of specified table.
     *
     * @param tableName  Table that will be described.
     * @param filter  Substring to filter by
     */
    private void describe(String tableName,
                          String inFilter) throws SQLException {
        /*
         * Doing case-sensitive filters now, for greater portability.
        String filter = ((inFilter == null) ? null : inFilter.toUpperCase());
         */
        String    filter = inFilter;
        List      rows        = new ArrayList();
        String[]  headerArray = {
            "name", "datatype", "width", "no-nulls"
        };
        String[]  fieldArray;
        int[]     maxWidth  = {
            0, 0, 0, 0
        };
        boolean[] rightJust = {
            false, false, true, false
        };

        // STEP 1: GATHER DATA
        for (int i = 0; i < headerArray.length; i++) {
            if (htmlMode) {
                continue;
            }

            if (headerArray[i].length() > maxWidth[i]) {
                maxWidth[i] = headerArray[i].length();
            }
        }

        Statement statement = curConn.createStatement();
        ResultSet r         = null;

        try {
            statement.execute("SELECT * FROM " + tableName + " WHERE 1 = 2");

            r = statement.getResultSet();

            ResultSetMetaData m    = r.getMetaData();
            int               cols = m.getColumnCount();

            for (int i = 0; i < cols; i++) {
                fieldArray    = new String[4];
                fieldArray[0] = m.getColumnName(i + 1);

                if (filter != null && fieldArray[0].indexOf(filter) < 0) {
                    continue;
                }

                fieldArray[1] = m.getColumnTypeName(i + 1);
                fieldArray[2] = Integer.toString(m.getColumnDisplaySize(i
                        + 1));
                fieldArray[3] =
                    ((m.isNullable(i + 1) == java.sql.ResultSetMetaData.columnNullable)
                     ? (htmlMode ? "&nbsp;"
                                 : "")
                     : "*");

                rows.add(fieldArray);

                for (int j = 0; j < fieldArray.length; j++) {
                    if (fieldArray[j].length() > maxWidth[j]) {
                        maxWidth[j] = fieldArray[j].length();
                    }
                }
            }

            // STEP 2: DISPLAY DATA
            condlPrint("<TABLE border='1'>" + LS + SqlFile.htmlRow(COL_HEAD) + LS
                       + PRE_TD, true);

            for (int i = 0; i < headerArray.length; i++) {
                condlPrint("<TD>" + headerArray[i] + "</TD>", true);
                condlPrint(((i > 0) ? SqlFile.spaces(2)
                                    : "") + SqlFile.pad(headerArray[i], maxWidth[i],
                                                rightJust[i],
                                                (i < headerArray.length - 1
                                                 || rightJust[i])), false);
            }

            condlPrintln(LS + PRE_TR + "</TR>", true);
            condlPrintln("", false);

            if (!htmlMode) {
                for (int i = 0; i < headerArray.length; i++) {
                    condlPrint(((i > 0) ? SqlFile.spaces(2)
                                        : "") + SqlFile.divider(maxWidth[i]), false);
                }

                condlPrintln("", false);
            }

            for (int i = 0; i < rows.size(); i++) {
                condlPrint(SqlFile.htmlRow(((i % 2) == 0) ? COL_EVEN
                                                  : COL_ODD) + LS
                                                  + PRE_TD, true);

                fieldArray = (String[]) rows.get(i);

                for (int j = 0; j < fieldArray.length; j++) {
                    condlPrint("<TD>" + fieldArray[j] + "</TD>", true);
                    condlPrint(((j > 0) ? SqlFile.spaces(2)
                                        : "") + SqlFile.pad(
                                            fieldArray[j], maxWidth[j],
                                            rightJust[j],
                                            (j < fieldArray.length - 1
                                             || rightJust[j])), false);
                }

                condlPrintln(LS + PRE_TR + "</TR>", true);
                condlPrintln("", false);
            }

            condlPrintln(LS + "</TABLE>" + LS + "<HR>", true);
        } finally {
            try {
                if (r != null) {
                    r.close();

                    r = null;
                }

                statement.close();
            } catch (Exception e) {}
        }
    }

    public static String[] getTokenArray(String inString) {
        // I forget how to code a String array literal outside of a
        // definition.
        String[] mtString = {};

        if (inString == null) {
            return mtString;
        }

        StringTokenizer toker = new StringTokenizer(inString);
        String[]        sa    = new String[toker.countTokens()];

        for (int i = 0; i < sa.length; i++) {
            sa[i] = toker.nextToken();
        }

        return sa;
    }

    private boolean eval(String[] inTokens) throws BadSpecial {
        // dereference *VARNAME variables.
        // N.b. we work with a "copy" of the tokens.
        boolean  negate = inTokens.length > 0 && inTokens[0].equals("!");
        String[] tokens = new String[negate ? (inTokens.length - 1)
                                            : inTokens.length];

        for (int i = 0; i < tokens.length; i++) {
            tokens[i] = (inTokens[i + (negate ? 1
                                              : 0)].length() > 1 && inTokens[i + (negate ? 1
                                                                                         : 0)].charAt(
                                                                                         0) == '*') ? ((String) userVars.get(
                                                                                             inTokens[i + (negate ? 1
                                                                                                                  : 0)]
                                                                                                                  .substring(
                                                                                                                      1)))
                                                                                                    : inTokens[i + (negate ? 1
                                                                                                                           : 0)];

            if (tokens[i] == null) {
                tokens[i] = "";
            }
        }

        if (tokens.length == 1) {
            return (tokens[0].length() > 0 &&!tokens[0].equals("0")) ^ negate;
        }

        if (tokens.length == 3) {
            if (tokens[1].equals("==")) {
                return tokens[0].equals(tokens[2]) ^ negate;
            }

            if (tokens[1].equals("!=") || tokens[1].equals("<>")
                    || tokens[1].equals("><")) {
                return (!tokens[0].equals(tokens[2])) ^ negate;
            }

            if (tokens[1].equals(">")) {
                return (tokens[0].length() > tokens[2].length() || ((tokens[0].length() == tokens[2].length()) && tokens[0].compareTo(tokens[2]) > 0))
                       ^ negate;
            }

            if (tokens[1].equals("<")) {
                return (tokens[2].length() > tokens[0].length() || ((tokens[2].length() == tokens[0].length()) && tokens[2].compareTo(tokens[0]) > 0))
                       ^ negate;
            }
        }

        throw new BadSpecial("Unrecognized logical operation");
    }

    private void closeQueryOutputStream() {
        if (pwQuery == null) {
            return;
        }

        if (htmlMode) {
            pwQuery.println("</BODY></HTML>");
            pwQuery.flush();
        }

        pwQuery.close();

        pwQuery = null;
    }

    /**
     * Print to psStd and possibly pwQuery iff current HTML mode matches
     * supplied printHtml.
     */
    private void condlPrintln(String s, boolean printHtml) {
        if ((printHtml &&!htmlMode) || (htmlMode &&!printHtml)) {
            return;
        }

        psStd.println(s);

        if (pwQuery != null) {
            pwQuery.println(s);
            pwQuery.flush();
        }
    }

    /**
     * Print to psStd and possibly pwQuery iff current HTML mode matches
     * supplied printHtml.
     */
    private void condlPrint(String s, boolean printHtml) {
        if ((printHtml &&!htmlMode) || (htmlMode &&!printHtml)) {
            return;
        }

        psStd.print(s);

        if (pwQuery != null) {
            pwQuery.print(s);
            pwQuery.flush();
        }
    }

    private static String formatNicely(Map map, boolean withValues) {
        String       key;
        StringBuffer sb = new StringBuffer();
        Iterator     it = (new TreeMap(map)).keySet().iterator();

        if (withValues) {
            SqlFile.appendLine(sb, "The outermost parentheses are not part of "
                      + "the values.");
        } else {
            SqlFile.appendLine(sb, "Showing variable names and length of values "
                      + "(use 'listvalue' to see values).");
        }

        while (it.hasNext()) {
            key = (String) it.next();

            String s = (String) map.get(key);

            SqlFile.appendLine(sb, "    " + key + ": " + (withValues ? ("(" + s + ')')
                                                        : Integer.toString(
                                                        s.length())));
        }

        return sb.toString();
    }

    /**
     * Ascii file dump.
     */
    private void dump(String varName,
                      File dumpFile) throws IOException, BadSpecial {
        String val = (String) userVars.get(varName);

        if (val == null) {
            throw new BadSpecial("Variable '" + varName
                                 + "' has no value set");
        }

        OutputStreamWriter osw =
            new OutputStreamWriter(new FileOutputStream(dumpFile), charset);

        osw.write(val);

        if (val.length() > 0) {
            char lastChar = val.charAt(val.length() - 1);

            if (lastChar != '\n' && lastChar != '\r') {
                osw.write(LS);
            }
        }

        osw.flush();
        osw.close();

        // Since opened in overwrite mode, since we didn't exception out,
        // we can be confident that we wrote all the bytest in the file.
        stdprintln("Saved " + dumpFile.length() + " characters to '"
                   + dumpFile + "'");
    }

    byte[] binBuffer = null;

    /**
     * Binary file dump
     */
    private void dump(File dumpFile) throws IOException, BadSpecial {
        if (binBuffer == null) {
            throw new BadSpecial("Binary SqlFile buffer is currently empty");
        }

        FileOutputStream fos = new FileOutputStream(dumpFile);

        fos.write(binBuffer);

        int len = binBuffer.length;

        binBuffer = null;

        fos.flush();
        fos.close();
        stdprintln("Saved " + len + " bytes to '" + dumpFile + "'");
    }

    static public String streamToString(InputStream is, String cs)
            throws IOException {
        char[]            xferBuffer   = new char[10240];
        StringWriter      stringWriter = new StringWriter();
        InputStreamReader isr          = new InputStreamReader(is, cs);
        int               i;

        while ((i = isr.read(xferBuffer)) > 0) {
            stringWriter.write(xferBuffer, 0, i);
        }

        return stringWriter.toString();
    }

    /**
     * Ascii file load.
     */
    private void load(String varName, File asciiFile, String cs)
            throws IOException {
        FileInputStream fis = new FileInputStream(asciiFile);
        String string = SqlFile.streamToString(fis, cs);
        fis.close();
        userVars.put(varName, string);
    }

    static public byte[] streamToBytes(InputStream is) throws IOException {
        byte[]                xferBuffer = new byte[10240];
        ByteArrayOutputStream baos       = new ByteArrayOutputStream();
        int                   i;

        while ((i = is.read(xferBuffer)) > 0) {
            baos.write(xferBuffer, 0, i);
        }

        return baos.toByteArray();
    }

    /**
     * Binary file load
     */
    static public byte[] loadBinary(File binFile) throws IOException {
        byte[]                xferBuffer = new byte[10240];
        ByteArrayOutputStream baos       = new ByteArrayOutputStream();
        FileInputStream       fis        = new FileInputStream(binFile);
        int                   i;

        while ((i = fis.read(xferBuffer)) > 0) {
            baos.write(xferBuffer, 0, i);
        }

        fis.close();

        byte[] ba = baos.toByteArray();

        return ba;
    }

    /**
     * This method is used to tell SqlFile whether this Sql Type must
     * ALWAYS be loaded to the binary buffer without displaying.
     *
     * N.b.:  If this returns "true" for a type, then the user can never
     * "see" values for these columns.
     * Therefore, if a type may-or-may-not-be displayable, better to return
     * false here and let the user choose.
     * In general, if there is a toString() operator for this Sql Type
     * then return false, since the JDBC driver should know how to make the
     * value displayable.
     *
     * The table on this page lists the most common SqlTypes, all of which
     * must implement toString():
     *     http://java.sun.com/docs/books/tutorial/jdbc/basics/retrieving.html
     *
     * @see java.sql.Types
     */
    public static boolean canDisplayType(int i) {
        /* I don't now about some of the more obscure types, like REF and
         * DATALINK */
        switch (i) {
            //case java.sql.Types.BINARY :
            case java.sql.Types.BLOB :
            case java.sql.Types.JAVA_OBJECT :

            //case java.sql.Types.LONGVARBINARY :
            //case java.sql.Types.LONGVARCHAR :
            case java.sql.Types.OTHER :
            case java.sql.Types.STRUCT :

                //case java.sql.Types.VARBINARY :
                return false;
        }

        return true;
    }

    // won't compile with JDK 1.3 without these
    private static final int JDBC3_BOOLEAN  = 16;
    private static final int JDBC3_DATALINK = 70;

    public static String sqlTypeToString(int i) {
        switch (i) {
            case java.sql.Types.ARRAY :
                return "ARRAY";

            case java.sql.Types.BIGINT :
                return "BIGINT";

            case java.sql.Types.BINARY :
                return "BINARY";

            case java.sql.Types.BIT :
                return "BIT";

            case java.sql.Types.BLOB :
                return "BLOB";

            case JDBC3_BOOLEAN :
                return "BOOLEAN";

            case java.sql.Types.CHAR :
                return "CHAR";

            case java.sql.Types.CLOB :
                return "CLOB";

            case JDBC3_DATALINK :
                return "DATALINK";

            case java.sql.Types.DATE :
                return "DATE";

            case java.sql.Types.DECIMAL :
                return "DECIMAL";

            case java.sql.Types.DISTINCT :
                return "DISTINCT";

            case java.sql.Types.DOUBLE :
                return "DOUBLE";

            case java.sql.Types.FLOAT :
                return "FLOAT";

            case java.sql.Types.INTEGER :
                return "INTEGER";

            case java.sql.Types.JAVA_OBJECT :
                return "JAVA_OBJECT";

            case java.sql.Types.LONGVARBINARY :
                return "LONGVARBINARY";

            case java.sql.Types.LONGVARCHAR :
                return "LONGVARCHAR";

            case java.sql.Types.NULL :
                return "NULL";

            case java.sql.Types.NUMERIC :
                return "NUMERIC";

            case java.sql.Types.OTHER :
                return "OTHER";

            case java.sql.Types.REAL :
                return "REAL";

            case java.sql.Types.REF :
                return "REF";

            case java.sql.Types.SMALLINT :
                return "SMALLINT";

            case java.sql.Types.STRUCT :
                return "STRUCT";

            case java.sql.Types.TIME :
                return "TIME";

            case java.sql.Types.TIMESTAMP :
                return "TIMESTAMP";

            case java.sql.Types.TINYINT :
                return "TINYINT";

            case java.sql.Types.VARBINARY :
                return "VARBINARY";

            case java.sql.Types.VARCHAR :
                return "VARCHAR";
        }

        return "Unknown type " + i;
    }

    /**
     * Validate that String is safe to display in a DSV file.
     *
     * @throws SqlToolError if validation fails.
     */
    public void dsvSafe(String s) throws SqlToolError {
        if (pwDsv == null || dsvColDelim == null || dsvRowDelim == null
                || dsvNullRep == null) {
            throw new RuntimeException(
                "Assertion failed.  \n"
                + "dsvSafe called when DSV settings are incomplete");
        }

        if (s == null) {
            return;
        }

        if (s.indexOf(dsvColDelim) > 0) {
            throw new SqlToolError(
                "Table data contains our column delimiter '" + dsvColDelim
                + "'");
        }

        if (s.indexOf(dsvRowDelim) > 0) {
            throw new SqlToolError("Table data contains our row delimiter '"
                                   + dsvRowDelim + "'");
        }

        if (s.indexOf(dsvNullRep) > 0) {
            throw new SqlToolError(
                "Table data contains our null representation '" + dsvNullRep
                + "'");
        }
    }

    /**
     * Translates user-supplied escapes into the traditionaly corresponding
     * corresponding binary characters.
     *
     * Allowed sequences:
     * <UL>
     *  <LI>\0\d+   (an octal digit)
     *  <LI>\[0-9]\d*  (a decimal digit)
     *  <LI>\[Xx][0-9]{2}  (a hex digit)
     *  <LI>\n  Newline  (Ctrl-J)
     *  <LI>\r  Carriage return  (Ctrl-M)
     *  <LI>\t  Horizontal tab  (Ctrl-I)
     *  <LI>\f  Form feed  (Ctrl-L)
     * </UL>
     *
     * Java 1.4 String methods will make this into a 1 or 2 line task.
     */
    public static String convertEscapes(String inString) {
        if (inString == null) {
            return null;
        }
        return convertNumericEscapes(
                convertEscapes(convertEscapes(convertEscapes(convertEscapes(
                    convertEscapes(inString, "\\n", "\n"), "\\r", "\r"),
                "\\t", "\t"), "\\\\", "\\"),
            "\\f", "\f")
        );
    }

    /**
     * @param string  Non-null String to modify.
     */
    private static String convertNumericEscapes(String string) {
        String workString = string;
        int i = 0;

        for (char dig = '0'; dig <= '9'; dig++) {
            while ((i = workString.indexOf("\\" + dig, i)) > -1
                    && i < workString.length() - 1) {
                workString = convertNumericEscape(string, i);
            }
            while ((i = workString.indexOf("\\x" + dig, i)) > -1
                    && i < workString.length() - 1) {
                workString = convertNumericEscape(string, i);
            }
            while ((i = workString.indexOf("\\X" + dig, i)) > -1
                    && i < workString.length() - 1) {
                workString = convertNumericEscape(string, i);
            }
        }
        return workString;
    }

    /**
     * @offset  Position of the leading \.
     */
    private static String convertNumericEscape(String string, int offset) {
        int post = -1;
        int firstDigit = -1;
        int radix = -1;
        if (Character.toUpperCase(string.charAt(offset + 1)) == 'X') {
            firstDigit = offset + 2;
            radix = 16;
            post = firstDigit + 2;
            if (post > string.length()) post = string.length();
        } else {
            firstDigit = offset + 1;
            radix = (Character.toUpperCase(string.charAt(firstDigit)) == '0')
                    ? 8 : 10;
            for (post = firstDigit + 1; post < string.length()
                    && Character.isDigit(string.charAt(post)); post++) ;
        }
        return string.substring(0, offset) + ((char)
                Integer.parseInt(string.substring(firstDigit, post), radix))
                + string.substring(post);
    }

    /**
     * @param string  Non-null String to modify.
     */
    private static String convertEscapes(String string, String from, String to) {
        String workString = string;
        int i = 0;
        int fromLen = from.length();

        while ((i = workString.indexOf(from, i)) > -1
                && i < workString.length() - 1) {
            workString = workString.substring(0, i) + to
                         + workString.substring(i + fromLen);
        }
        return workString;
    }

    /**
     * Name is self-explanatory.
     *
     * If there is user demand, open file in random access mode so don't
     * need to load 2 copies of the entire file into memory.
     * This will be difficult because can't use standard Java language
     * features to search through a character array for multi-character
     * substrings.
     *
     * @throws SqlToolError  Would prefer to throw an internal exception,
     *                       but we want this method to have external
     *                       visibility.
     */
    public void importDsv(String filePath) throws SqlToolError {
        char[] bfr  = null;
        File   file = new File(filePath);
        String tmpString = (String) userVars.get("*DSV_CONST_COLS");
        SortedMap constColMap = null;
        int constColMapSize = 0;
        if (tmpString != null) {
            // Can't use StringTokenizer, since our delimiters are fixed
            // whereas StringTokenizer delimiters are a list of OR delimiters.

            // We trim col. names, but not values.  Must allow users to
            // specify values as spaces, empty string, null.
            constColMap = new TreeMap();
            int startOffset;
            int postOffset = -1;
            int firstEq;
            String n;
            do {
                startOffset = postOffset + 1;
                postOffset = tmpString.indexOf(dsvColDelim, startOffset);
                if (postOffset < 0) postOffset = tmpString.length();
                if (postOffset == startOffset)
                    throw new SqlToolError("*DSV_CONST_COLS has null setting");
                firstEq = tmpString.indexOf('=', startOffset);
                if (firstEq < startOffset + 1 || firstEq > postOffset)
                    throw new SqlToolError("*DSV_CONST_COLS element malformatted");
                n = tmpString.substring(startOffset, firstEq).trim();
                if (n.length() < 1)
                    throw new SqlToolError(
                            "*DSV_CONST_COLS element has null col. name");
                constColMap.put(n,
                        tmpString.substring(firstEq + 1, postOffset));
            } while (postOffset < tmpString.length());
            stdprintln("Using Constant Column map:  " + constColMap);
            constColMapSize = constColMap.size();
        }

        if (!file.canRead()) {
            throw new SqlToolError("Can't read file '" + file + "'");
        }

        int fileLength = (int) (file.length());

        try {
            bfr = new char[fileLength];
        } catch (RuntimeException re) {
            throw new SqlToolError(
                "SqlFile can only read in your DSV file in one chunk at this time.\n"
                + "Please run the program with more RAM (try Java -Xm* switches).",
                re);
        }

        int retval = -1;

        try {
            InputStreamReader isr =
                new InputStreamReader(new FileInputStream(file), charset);
            retval = isr.read(bfr, 0, bfr.length);

            isr.close();
        } catch (IOException ioe) {
            throw new SqlToolError(ioe);
        }
        /*  Per tracker 1547196, File.length is in bytes, but
         *  InputStreamReader.read returns size in characters.
         *  Therefore, this test fails if char size != 1 byte.
        if (retval != bfr.length) {
            throw new SqlToolError("Didn't read all characters.  Read in "
                                  + retval + " characters");
        }
        */

        String string = null;
        String dateString;

        try {
            string = new String(bfr, 0, retval);
            // Sized explicitly to truncate nulls due to multibye characters.
        } catch (RuntimeException re) {
            throw new SqlToolError(
                "SqlFile converts your entire DSV file to a String at this time.\n"
                + "Please run the program with more RAM (try Java -Xm* switches).",
                re);
        }

        List     headerList = new ArrayList();
        String    tableName = (String) userVars.get("*DSV_TARGET_TABLE");
        if (tableName == null) {
            tableName = (String) userVars.get("*CSV_TABLENAME");
            // This just for legacy variable name.
        }

        // N.b.  ENDs are the index of 1 PAST the current item
        int recEnd = -1000; // Recognizable value incase something goes
                            // horrifically wrong.
        int colStart;
        int colEnd;

        // First read one until we get one header line
        int lineCount = 0; // Assume a 1 line header?
        int recStart = -1;
        String trimmedLine = null;
        boolean switching = false;

        while (true) {
            recStart = (recStart < 0) ? 0 : (recEnd + dsvRowDelim.length());
            if (recStart > string.length() - 2) {
                throw new SqlToolError("No header record");
            }
            recEnd = string.indexOf(dsvRowDelim, recStart);
            lineCount++; // Increment when we have line start and end

            if (recEnd < 0) {
                // Last line in file.  No data records.
                recEnd = string.length();
            }
            trimmedLine = string.substring(recStart, recEnd).trim();
            if (trimmedLine.length() < 1
                    || (dsvSkipPrefix != null
                            && trimmedLine.startsWith(dsvSkipPrefix))) {
                continue;
            }
            if (trimmedLine.startsWith("targettable=")) {
                if (tableName == null) {
                    tableName = trimmedLine.substring(
                            "targettable=".length()).trim();
                }
                continue;
            }
            if (trimmedLine.equals("headerswitch{")) {
                if (tableName == null) {
                    throw new SqlToolError("Headerswitch in DSV file, but "
                            + "no target table specified yet.  Line "
                            + lineCount);
                }
                switching = true;
                continue;
            }
            if (trimmedLine.equals("}")) {
                throw new SqlToolError(
                        "Reached close of headerswitch at line " + lineCount
                        + " without matching a header line");
            }
            if (!switching) {
                break;
            }
            int colonAt = trimmedLine.indexOf(':');
            if (colonAt < 1 || colonAt == trimmedLine.length() - 1) {
                throw new SqlToolError(
                        "Header line without table matcher at line "
                        + lineCount);
            }
            String matcher = trimmedLine.substring(0, colonAt).trim();
            // Need to be sure here that tableName is not null (in
            // which case it would be determined later on by the file name).
            if (matcher.equals("*") || matcher.equalsIgnoreCase(tableName)){
                recStart = 1 + string.indexOf(':', recStart);
                break;
            }
            // Skip non-matched header line
        }

        String headerLine = string.substring(recStart, recEnd);
        colStart = recStart;
        colEnd   = -1;

        while (true) {
            if (colEnd == recEnd) {
                // We processed final column last time through loop
                break;
            }

            colEnd = string.indexOf(dsvColDelim, colStart);

            if (colEnd < 0 || colEnd > recEnd) {
                colEnd = recEnd;
            }

            if (colEnd - colStart < 1) {
                throw new SqlToolError("No column header for column "
                                      + (headerList.size() + 1));
            }

            headerList.add(
                (colEnd - colStart == 1 && string.charAt(colStart) == '-')
                ? ((String) null)
                : string.substring(colStart, colEnd));

            colStart = colEnd + dsvColDelim.length();
        }

        if (constColMap != null) {
            headerList.addAll(constColMap.keySet());
        }

        String[]  headers   = (String[]) headerList.toArray(new String[0]);

        if (tableName == null) {
            tableName = file.getName();

            int i = tableName.lastIndexOf('.');

            if (i > 0) {
                tableName = tableName.substring(0, i);
            }
        }

        StringBuffer tmpSb = new StringBuffer();

        int skippers = 0;
        for (int i = 0; i < headers.length; i++) {
            if (headers[i] == null) {
                skippers++;
                continue;
            }
            if (tmpSb.length() > 0) {
                tmpSb.append(", ");
            }

            tmpSb.append(headers[i]);
        }
        boolean[] autonulls = new boolean[headers.length - skippers];
        boolean[] parseDate = new boolean[autonulls.length];
        // Remember that the headers array has all columns in DSV file,
        // even skipped columns.
        // The autonulls array only has columns that we will insert into.

        StringBuffer sb = new StringBuffer("INSERT INTO " + tableName + " ("
                                           + tmpSb + ") VALUES (");
        StringBuffer typeQuerySb = new StringBuffer("SELECT " + tmpSb
            + " FROM " + tableName + " WHERE 1 = 2");

        try {
            ResultSetMetaData rsmd = curConn.createStatement().executeQuery(
                typeQuerySb.toString()).getMetaData();

            if (rsmd.getColumnCount() != autonulls.length) {
                throw new SqlToolError("Metadata mismatch for columns");
            }

            for (int i = 0; i < autonulls.length; i++) {
                autonulls[i] = true;
                parseDate[i] = false;
                switch(rsmd.getColumnType(i + 1)) {
                    case java.sql.Types.VARBINARY:
                    case java.sql.Types.VARCHAR:
                        // to insert "".  Otherwise, we'll insert null for "".
                        autonulls[i] = false;
                        break;
                    case java.sql.Types.DATE:
                    case java.sql.Types.TIME:
                    case java.sql.Types.TIMESTAMP:
                        parseDate[i] = true;
                }
            }
        } catch (SQLException se) {
            throw new SqlToolError("Failed to get metadata for query: "
                     + se.getMessage() + "  (Used: " + typeQuerySb + ')', se);
        }

        for (int i = 0; i < autonulls.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }

            sb.append('?');
        }

        // Initialize REJECT file(s)
        int rejectCount = 0;
        File rejectFile = null;
        File rejectReportFile = null;
        PrintWriter rejectWriter = null;
        PrintWriter rejectReportWriter = null;
        String tmp = (String) userVars.get("*DSV_REJECT_FILE");
        if (tmp != null) try {
            rejectFile = new File(tmp);
            rejectWriter = new PrintWriter(
                        new OutputStreamWriter(
                            new FileOutputStream(rejectFile), charset));
            rejectWriter.print(headerLine + dsvRowDelim);
        } catch (IOException ioe) {
            throw new SqlToolError("Failed to set up reject file '"
                    + tmp + "'", ioe);
        }
        tmp = (String) userVars.get("*DSV_REJECT_REPORT");
        if (tmp != null) try {
            rejectReportFile = new File(tmp);
            rejectReportWriter = new PrintWriter(
                        new OutputStreamWriter(
                            new FileOutputStream(rejectReportFile), charset));
            rejectReportWriter.println("<HTML>");
            rejectReportWriter.println("<HEAD><STYLE>");
            rejectReportWriter.println("    .right { text-align:right; }");
            rejectReportWriter.println("    .reason { font-size: 75%; font-family:courier; color:red; }");
            rejectReportWriter.println("</STYLE></HEAD>");
            rejectReportWriter.println("<BODY style='background:silver;'>");
            rejectReportWriter.println("<P>Import performed at "
                    + "<SPAN style='font-weight:bold;'>" + new java.util.Date()
                    + "</SPAN></P>");
            rejectReportWriter.println("<P>Input DSV file: "
                + "<SPAN style='font-weight:bold; font-style:courier'>"
                    + file.getPath() + "</SPAN></P>");
            if (rejectFile != null) {
                rejectReportWriter.println("<P>Reject DSV file: "
                    + "<SPAN style='font-weight:bold; font-style:courier;'>"
                        + rejectFile.getPath() + "</SPAN></P>");
            }
            if (rejectWriter != null) {
                rejectReportWriter.println(
                        "<P>The corresponding records in '" + rejectFile
                        + "' are at line numbers of (reject # + 1), since the "
                        + "header record occupies the first line.</P>");
            }
            rejectReportWriter.println(
                    "<TABLE border='1px' cellpadding='5px' style='background-color:white;'>");
            rejectReportWriter.println("    <THEAD><TR><TH>reject #</TH>"
                    + "<TH>input line #</TH><TH>reason</TH></TR></THEAD>");
            rejectReportWriter.println("<TBODY>");
        } catch (IOException ioe) {
            throw new SqlToolError("Failed to set up reject report file '"
                    + tmp + "'", ioe);
        }

        int recCount = 0;
        int skipCount = 0;
        PreparedStatement ps = null;
        boolean importAborted = false;

        try {
            try {
                ps = curConn.prepareStatement(sb.toString() + ')');
            } catch (SQLException se) {
                throw new SqlToolError("Failed to prepare insertion setup "
                        + "string: " + sb + ')', se);
            }
            String[] dataVals = new String[autonulls.length];
            // Length is number of cols to insert INTO, not nec. # in DSV file.
            int      readColCount;
            int      storeColCount;

            // Insert data rows 1-row-at-a-time
            while (true) try { try {
                recStart = recEnd + dsvRowDelim.length();

                if (recStart >= string.length()) {
                    break;
                }

                recEnd = string.indexOf(dsvRowDelim, recStart);
                lineCount++; // Increment when we have line start and end

                if (recEnd < 0) {
                    // Last record
                    recEnd = string.length();
                }
                trimmedLine = string.substring(recStart, recEnd).trim();
                if (trimmedLine.length() < 1) {
                    continue;  // Silently skip blank lines
                }
                if (dsvSkipPrefix != null
                        && trimmedLine.startsWith(dsvSkipPrefix)) {
                    skipCount++;
                    continue;
                }
                if (switching) {
                    if (trimmedLine.equals("}")) {
                        switching = false;
                        continue;
                    }
                    int colonAt = trimmedLine.indexOf(':');
                    if (colonAt < 1 || colonAt == trimmedLine.length() - 1) {
                        throw new SqlToolError(
                                "Non-Header line within table matcher block "
                                + "at line " + lineCount);
                    }
                    continue;
                }

                // Finally we will attempt to add a record!
                recCount++;
                // Remember that recCount counts both inserts + rejects

                colStart = recStart;
                colEnd   = -1;
                readColCount = 0;
                storeColCount = 0;

                while (true) {
                    if (colEnd == recEnd) {
                        // We processed final column last time through loop
                        break;
                    }

                    colEnd = string.indexOf(dsvColDelim, colStart);

                    if (colEnd < 0 || colEnd > recEnd) {
                        colEnd = recEnd;
                    }

                    if (readColCount == headers.length - constColMapSize) {
                        throw new RowError(
                            "Header has "
                                    + (headers.length - constColMapSize)
                            + " columns, but input record "
                            + "has too many column values ("
                            + (1 + readColCount) + ").");
                    }

                    if (headers[readColCount++] != null) {
                        dataVals[storeColCount++] =
                                string.substring(colStart, colEnd);
                    }
                    colStart             = colEnd + dsvColDelim.length();
                }

                if (constColMap != null) {
                    Iterator it = constColMap.values().iterator();
                    while (it.hasNext()) {
                        dataVals[storeColCount++] = (String) it.next();
                    }
                }
                /* It's too late for the following two tests, since if
                 * we inserted *ColCount > array.length, we would have
                 * generated a runtime array index exception. */
                if (readColCount != headers.length - constColMapSize) {
                    throw new RowError("Header has "
                            + (headers.length - constColMapSize)
                            + " columns, but input record has "
                            + readColCount + " column values.");
                }
                if (storeColCount != dataVals.length) {
                    throw new RowError("Header has "
                            + (dataVals.length - constColMapSize)
                            + " non-skip columns, but input record has "
                            + (storeColCount - constColMapSize)
                            + " column insertion values.");
                }

                for (int i = 0; i < dataVals.length; i++) {
                    // N.b. WE SPECIFICALLY DO NOT HANDLE TIMES WITHOUT
                    // DATES, LIKE "3:14:00", BECAUSE, WHILE THIS MAY BE
                    // USEFUL AND EFFICIENT, IT IS NOT PORTABLE.
                    //System.err.println("ps.setString(" + i + ", "
                    //      + dataVals[i] + ')');
                    if (parseDate[i]) {
                        if ((dataVals[i].length() < 1 && autonulls[i])
                              || dataVals[i].equals(dsvNullRep)) {
                            ps.setTimestamp(i + 1, null);
                        } else {
                            dateString = (dataVals[i].indexOf(':') > 0)
                                ? dataVals[i] : (dataVals[i] + " 0:00:00");
                            try {
                                ps.setTimestamp(i + 1,
                                        java.sql.Timestamp.valueOf(dateString));
                            } catch (IllegalArgumentException iae) {
                                throw new RowError("Bad value '"
                                    + dateString + "'", iae);
                            }
                        }
                    } else {
                        ps.setString(
                            i + 1,
                            (((dataVals[i].length() < 1 && autonulls[i])
                              || dataVals[i].equals(dsvNullRep))
                             ? null
                             : dataVals[i]));
                    }
                }

                retval = ps.executeUpdate();

                if (retval != 1) {
                    throw new RowError(Integer.toString(retval)
                            + " rows modified");
                }

                possiblyUncommitteds.set(true);
            } catch (SQLException se) {
                throw new RowError(se);
            } } catch (RowError re) {
                rejectCount++;
                Throwable cause = re.getCause();
                if (rejectWriter != null || rejectReportWriter != null) {
                    if (rejectWriter != null) {
                        rejectWriter.print(string.substring(
                                recStart, recEnd) + dsvRowDelim);
                    }
                    if (rejectReportWriter != null) {
                        rejectReportWriter.println("    <TR>"
                                + "<TD align='right' class='right'>"
                                + rejectCount
                                + "</TD><TD align='right' class='right'>"
                                + lineCount + "</TD><TD><PRE class='reason'>"
                                + re.getMessage()
                                + ((cause == null) ? "" : ("<HR/>" + cause))
                                + "</PRE></TD></TR>");
                    }
                } else {
                    importAborted = true;
                    throw new SqlToolError("Parse or insert of input line "
                            + lineCount + " failed.  " + re.getMessage(),
                            cause);
                }
            }
        } finally {
            String summaryString = null;
            if (recCount > 0) {
                summaryString = "Import summary ("
                        + ((dsvSkipPrefix == null) ? "" : ("'" + dsvSkipPrefix
                        + "'-"))
                        + "skips / rejects / inserts):  "
                        + skipCount + " / " + rejectCount + " / "
                        + (recCount - rejectCount)
                        + (importAborted ? " before aborting." : ".");
                stdprintln(summaryString);
            }
            if (recCount > rejectCount) {
                stdprintln("(Unless you have Autocommit on, insertions will be "
                    + "lost if you don't commit).");
            }
            if (rejectWriter != null) {
                rejectWriter.flush();
                rejectWriter.close();
            }
            if (rejectReportWriter != null) {
                if (rejectCount > 0) {
                    rejectReportWriter.println(
                            "    <TR><TD colspan='3' style='background:blue; "
                            + "font-weight:bold; color:white'>");
                    rejectReportWriter.println("        " + summaryString);
                    rejectReportWriter.println("    </TD></TR>");
                    rejectReportWriter.println("</TBODY>");
                    rejectReportWriter.println("</TABLE>");
                    rejectReportWriter.println("</BODY>");
                    rejectReportWriter.println("</HTML>");
                    rejectReportWriter.flush();
                    rejectReportWriter.close();
                }
            }
            if (rejectCount == 0) {
                if (rejectFile != null && !rejectFile.delete())
                    errprintln("Failed to purge unnecessary reject file '"
                            + rejectFile + "'");
                if (rejectReportFile != null && !rejectReportFile.delete())
                    errprintln("Failed to purge unnecessary reject file '"
                            + rejectReportFile + "'");
                // These are trivial, non-fatal errors.
            }
        }
    }

    public static void appendLine(StringBuffer sb, String s) {
        sb.append(s + LS);
    }
}
