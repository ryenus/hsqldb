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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

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
 * "immediate" command (not the "buffer", which is a different thing).
 * All you can do to the immediate command is append new lines to it,
 * execute it, or save it to buffer.
 * When you are entering a buffer edit command like ":s/this/that/",
 * your immediate command is the buffer-edit-command.  The buffer
 * is the command string that you are editing.
 * The buffer usually contains either an exact copy of the last command
 * executed or sent to buffer by entering a blank line,
 * but BUFFER commands can change the contents of the buffer.
 *
 * In general, the special commands mirror those of Postgresql's psql,
 * but SqlFile handles command editing much different from Postgresql
 * because of Java's lack of support for raw tty I/O.
 * The \p special command, in particular, is very different from psql's.
 *
 * Buffer commands are unique to SQLFile.  The ":" commands allow
 * you to edit the buffer and to execute the buffer.
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
 * The process*() methods, other than processBuffHist() ALWAYS execute
 * on "buffer", and expect it to contain the method specific prefix
 * (if any).
 *
 * @version $Revision$
 * @author Blaine Simpson unsaved@users
 */

public class SqlFile {
    private static final int DEFAULT_HISTORY_SIZE = 40;
    private File             file;
    private boolean          interactive;
    private String           primaryPrompt    = "sql> ";
    private String           chunkPrompt      =
            bundle.getString("rawmode.prompt") + "> ";
    private String           contPrompt       = "  +> ";
    private Connection       curConn          = null;
    private boolean          htmlMode         = false;
    private Map              userVars; // Always a non-null map set in cons.
    private List             history          = null;
    private boolean          rawMode          = false;
    private String           nullRepToken     = null;
    private String           dsvTargetFile    = null;
    private String           dsvTargetTable   = null;
    private String           dsvConstCols     = null;
    private String           dsvRejectFile    = null;
    private String           dsvRejectReport  = null;
    public static String     LS = System.getProperty("line.separator");
    private int              maxHistoryLength = 1;

    /**
     * N.b. javax.util.regex Optional capture groups (...)? are completely
     * unpredictable wrt whether you get a null capture group vs. no capture.
     * Must always check count!
     */
    private static Pattern   specialPattern   =
            Pattern.compile("\\s*\\\\(\\S+)(?:\\s+(.*\\S))?\\s*");
    private static Pattern   plPattern   =
            Pattern.compile("\\s*\\*\\s*(.*\\S)?\\s*");
    private static Pattern   foreachPattern   =
            Pattern.compile("\\s*\\*\\s*foreach\\s+(\\S+)\\s*\\(([^)]*)\\)\\s*");
    private static Pattern   ifwhilePattern   =
            Pattern.compile("\\s*\\*\\s*\\S+\\s*\\(([^)]*)\\)\\s*");
    private static Pattern   varsetPattern   =
            Pattern.compile("\\s*\\*\\s*(\\S+)\\s*([=_~])\\s*(?:(.*\\S)\\s*)?");
    private static Pattern   substitutionPattern   =
            Pattern.compile("\\s*s(\\S)(.+?)\\1(.*?)\\1(.+)?\\s*");
            // Note that this pattern does not include the leading :.
    private static Pattern wincmdPattern = null;

    static {
        if (System.getProperty("os.name").startsWith("Windows")) {
            wincmdPattern = Pattern.compile("([^\"]+)?(\"[^\"]*\")?");
        }
    }
    // This can throw a runtime exception, but since the pattern
    // Strings are constant, one test run of the program will tell
    // if the patterns are good.

    /**
     * Encapsulate updating local variables which depend upon PL variables.
     *
     * Right now this is called whenever the user variable map is changed.
     * It would be more efficient to do it JIT by keeping track of when
     * the vars may be "dirty" by a variable map change, and having all
     * methods that use the settings call a conditional updater, but that
     * is less reliable since there is no way to guarantee that the vars
     * are not used without checking.
     */
    private void updateUserSettings() {
        dsvSkipPrefix = SqlFile.convertEscapes(
                (String) userVars.get("*DSV_SKIP_PREFIX"));
        if (dsvSkipPrefix == null) {
            dsvSkipPrefix = DEFAULT_SKIP_PREFIX;
        }
        dsvColDelim =
            SqlFile.convertEscapes((String) userVars.get("*DSV_COL_DELIM"));
        if (dsvColDelim == null) {
            dsvColDelim =
                SqlFile.convertEscapes((String) userVars.get("*CSV_COL_DELIM"));
        }
        if (dsvColDelim == null) {
            dsvColDelim = DEFAULT_COL_DELIM;
        }

        dsvRowDelim =
            SqlFile.convertEscapes((String) userVars.get("*DSV_ROW_DELIM"));
        if (dsvRowDelim == null) {
            dsvRowDelim =
                SqlFile.convertEscapes((String) userVars.get("*CSV_ROW_DELIM"));
        }
        if (dsvRowDelim == null) {
            dsvRowDelim = DEFAULT_ROW_DELIM;
        }

        dsvTargetFile = (String) userVars.get("*DSV_TARGET_FILE");
        if (dsvTargetFile == null) {
            dsvTargetFile = (String) userVars.get("*CSV_FILEPATH");
        }
        dsvTargetTable = (String) userVars.get("*DSV_TARGET_TABLE");
        if (dsvTargetTable == null) {
            dsvTargetTable = (String) userVars.get("*CSV_TABLENAME");
            // This just for legacy variable name.
        }

        dsvConstCols = (String) userVars.get("*DSV_CONST_COLS");
        dsvRejectFile = (String) userVars.get("*DSV_REJECT_FILE");
        dsvRejectReport = (String) userVars.get("*DSV_REJECT_REPORT");

        nullRepToken = (String) userVars.get("*NULL_REP_TOKEN");
        if (nullRepToken == null) {
            nullRepToken = (String) userVars.get("*CSV_NULL_REP");
        }
        if (nullRepToken == null) {
            nullRepToken = DEFAULT_NULL_REP;
        }
    }

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

    private static RefCapablePropertyResourceBundle bundle =
            RefCapablePropertyResourceBundle.getBundle(
                    "org.hsqldb.util.sqltool");

    private static final String BANNER = "(SqlFile processor v. " + revnum + ")"
            + LS + bundle.getString("banner");
    private static final String BUFFER_HELP_TEXT =
            bundle.getString("buffer.help");
    private static final String SPECIAL_HELP_TEXT =
            bundle.getString("special.help");
    private static final String PL_HELP_TEXT = bundle.getString("pl.help");
    private static final String DSV_OPTIONS_TEXT =
            bundle.getString("dsv.options");
    private static final String D_OPTIONS_TEXT = bundle.getString("d.options");
    private static final String RAW_LEADIN_MSG = bundle.getString("raw.leadin");

    private static String sqlfileNoreadString = null;
    private static String rawMovedtobufferString = null;
    private static String inputMovedtobufferString = null;
    private static String sqlstatementEmptyString = null;
    private static String causeString = null;
    private static String erroratString = null;
    private static String lineString = null;
    private static String breakUnsatisfiedString = null;
    private static String continueUnsatisfiedString = null;
    private static String primaryinputAccessfailureString = null;
    private static String typeString = null;
    private static String inputUnterminatedString = null;
    private static String plvarsetIncompleteString = null;
    private static String abortingString = null;
    private static String inputreaderClosefailureString = null;
    private static String rollingbackString = null;
    private static String specialUnspecifiedString = null;
    private static String nobufferString = null;
    private static String bufferExecutingString = null;
    private static String executingString = null;
    private static String nobufferYetString = null;
    private static String bufferCurrentString = null;
    private static String commandnumMalformatString = null;
    private static String bufferRestoredString = null;
    private static String substitutionMalformattedString = null;
    private static String substitutionNomatchString = null;
    private static String substitutionSyntaxString = null;
    private static String bufferUnknownString = null;
    private static String specialExtracharsString = null;
    private static String bufferExtracharsString = null;
    private static String specialMalformattedString = null;
    private static String htmlModeString = null;
    private static String dsvTargetfileRequiredString = null;
    private static String wroteString = null;
    private static String characterstofile = null;
    private static String fileNowriteString = null;
    private static String metadataNoobtainString = null;
    private static String specialDLikeString = null;
    private static String outputfileNonetocloseString = null;
    private static String outputfileReopeningString = null;
	private static String outputfileHeaderString = null;
    private static String destfileDemandString = null;
    private static String bufferEmptyString = null;
    private static String fileNoappendString = null;
    private static String sqlfileNameDemandString = null;
    private static String sqlfileExecuteFailString = null;
    private static String autocommitSettingString = null;
    private static String committedString = null;
    private static String specialBMalformattedString = null;
    private static String loadedString = null;
    private static String binaryBytesintoString = null;
    private static String binaryFilefailString = null;
    private static String cSettingString = null;
    private static String bangIncompleteString = null;
    private static String bangCommandFailString = null;
    private static String specialUnknownString = null;

    static {
        try {
            sqlfileNoreadString = bundle.getString("sqlfile.noread");
            rawMovedtobufferString = bundle.getString("raw.movedtobuffer");
            inputMovedtobufferString = bundle.getString("input.movedtobuffer");
            sqlstatementEmptyString = bundle.getString("sqlstatement.empty");
            causeString = bundle.getString("cause");
            erroratString = bundle.getString("errorat");
            lineString = bundle.getString("line");
            breakUnsatisfiedString = bundle.getString("break.unsatisfied");
            continueUnsatisfiedString =
                    bundle.getString("continue.unsatisfied");
            primaryinputAccessfailureString =
                    bundle.getString("primaryinput.accessfailure");
            typeString = bundle.getString("type");
            inputUnterminatedString = bundle.getString("input.unterminated");
            plvarsetIncompleteString = bundle.getString("plvarset.incomplete");
            abortingString = bundle.getString("aborting");
            inputreaderClosefailureString = bundle.getString(
                    "inputreader.closefailure");
            rollingbackString = bundle.getString("rollingback");
            specialUnspecifiedString = bundle.getString("special.unspecified");
            nobufferString = bundle.getString("nobuffer");
            bufferExecutingString = bundle.getString("buffer.executing");
            executingString = bundle.getString("executing");
            nobufferYetString = bundle.getString("nobuffer.yet");
            bufferCurrentString = bundle.getString("buffer.current");
			commandnumMalformatString = bundle.getString("commandnum.malformat");
			bufferRestoredString = bundle.getString("buffer.restored");
			substitutionMalformattedString =
					bundle.getString("substitution.malformatted");
			substitutionNomatchString = bundle.getString("substitution.nomatch");
			substitutionSyntaxString = bundle.getString("substitution.syntax");
			bufferUnknownString = bundle.getString("buffer.unknown");
			specialExtracharsString = bundle.getString("special.extrachars");
			bufferExtracharsString = bundle.getString("buffer.extrachars");
			specialMalformattedString =
					bundle.getString("special.malformatted");
			htmlModeString = bundle.getString("html.mode");
			dsvTargetfileRequiredString =
					bundle.getString("dsv.targetfile.required");
			wroteString = bundle.getString("wrote");
			characterstofile = bundle.getString("characterstofile");
			fileNowriteString = bundle.getString("file.nowrite");
			metadataNoobtainString = bundle.getString("metadata.noobtain");
			specialDLikeString = bundle.getString("special.d.like");
			outputfileNonetocloseString =
					bundle.getString("outputfile.nonetoclose");
			outputfileReopeningString =
					bundle.getString("outputfile.reopening");
			outputfileHeaderString = bundle.getString("outputfile.header");
			destfileDemandString = bundle.getString("destfile.demand");
			bufferEmptyString = bundle.getString("buffer.empty");
			fileNoappendString = bundle.getString("file.noappend");
			sqlfileNameDemandString = bundle.getString("sqlfile.name.demand");
			sqlfileExecuteFailString = bundle.getString("sqlfile.execute.fail");
			autocommitSettingString = bundle.getString("autocommit.setting");
			committedString = bundle.getString("committed");
			specialBMalformattedString =
					bundle.getString("special.b.malformatted");
			loadedString = bundle.getString("loaded");
			binaryBytesintoString = bundle.getString("binary.bytesinto");
			binaryFilefailString = bundle.getString("binary.filefail");
			cSettingString = bundle.getString("c.setting");
			bangIncompleteString = bundle.getString("bang.incomplete");
			bangCommandFailString = bundle.getString("bang.command.fail");
			specialUnknownString = bundle.getString("special.unknown");
        } catch (RuntimeException re) {
            System.err.println("Early abort due to localized String lookup");
        }
    }

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
        if (userVars == null) {
            userVars = new HashMap();
        }
        updateUserSettings();

        if (file != null &&!file.canRead()) {
            throw new IOException(sqlfileNoreadString + " '" + file + "'");
        }
        if (interactive) {
            history = new ArrayList();
            String histLenString = System.getProperty("sqltool.historyLength");
            if (histLenString != null) try {
                maxHistoryLength = Integer.parseInt(histLenString);
            } catch (Exception e) {
            } else {
                maxHistoryLength = DEFAULT_HISTORY_SIZE;
            }
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
    private String      lastSqlStatement   = null;
    private int         curLinenum   = -1;
    private int         curHist      = -1;
    private PrintStream psStd        = null;
    private PrintStream psErr        = null;
    private PrintWriter pwQuery      = null;
    private PrintWriter pwDsv        = null;
    StringBuffer        immCmdSB     = new StringBuffer();
    /*
     * This is reset upon each execute() invocation (to true if interactive,
     * false otherwise).
     */
    private boolean             continueOnError = false;
    private static final String DEFAULT_CHARSET = "US-ASCII";
    private BufferedReader      br              = null;
    private String              charset         = null;
    private String              buffer          = null;
    private boolean             withholdPrompt  = false;

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
     *                       Only possible if in "\c false" mode.
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
        String  trimmedInput;
        String  deTerminated;
        boolean inComment = false;    // Gobbling up a comment
        int     postCommentIndex;
        boolean rollbackUncoms = true;

        continueOnError = (coeOverride == null) ? interactive
                                                : coeOverride.booleanValue();

        if (userVars.size() > 0) {
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
                if (withholdPrompt) {
                    withholdPrompt = false;
                } else if (interactive) {
                    psStd.print((immCmdSB.length() == 0)
                                ? (rawMode ? chunkPrompt
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

                if (inComment) {
                    postCommentIndex = inputLine.indexOf("*/") + 2;

                    if (postCommentIndex > 1) {
                        // I see no reason to leave comments in history.
                        inputLine = inputLine.substring(postCommentIndex);

                        // Empty the buffer.  The non-comment remainder of
                        // this line is either the beginning of a new SQL
                        // or Special command, or an empty line.
                        immCmdSB.setLength(0);

                        inComment = false;
                    } else {
                        // Just completely ignore the input line.
                        continue;
                    }
                }

                trimmedInput = inputLine.trim();

                try {
                    if (rawMode) {
                        boolean rawExecute = inputLine.equals(".;");
                        if (rawExecute || inputLine.equals(":.")) {
                            rawMode = false;

                            setBuf(immCmdSB.toString());
                            immCmdSB.setLength(0);

                            if (rawExecute) {
                                historize();
                                processSQL();
                            } else if (interactive) {
                                stdprintln(rawMovedtobufferString);
                            }
                        } else {
                            if (immCmdSB.length() > 0) {
                                immCmdSB.append('\n');
                            }

                            immCmdSB.append(inputLine);
                        }

                        continue;
                    }

                    if (immCmdSB.length() == 0) {
                    // NEW Immediate Command (i.e., not appending).
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
                                immCmdSB.append("COMMENT");

                                inComment = true;

                                continue;
                            }
                        }

                        // This is just to filter out useless newlines at
                        // beginning of commands.
                        if (trimmedInput.length() == 0) {
                            continue;
                        }

                        if ((trimmedInput.charAt(0) == '*'
                                && (trimmedInput.length() < 2
                                    || trimmedInput.charAt(1) != '{'))
                                || trimmedInput.charAt(0) == '\\') {
                            setBuf(trimmedInput);
                            processFromBuffer();
                            continue;
                        }

                        if (trimmedInput.charAt(0) == ':' && interactive) {
                            processBuffHist(trimmedInput.substring(1));
                            continue;
                        }

                        String ucased = trimmedInput.toUpperCase();

                        if (ucased.startsWith("DECLARE")
                                || ucased.startsWith("BEGIN")) {
                            rawMode = true;

                            immCmdSB.append(inputLine);

                            if (interactive) {
                                stdprintln(RAW_LEADIN_MSG);
                            }

                            continue;
                        }
                    }

                    if (trimmedInput.length() == 0 && interactive &&!inComment) {
                        // Blank lines delimit commands ONLY IN INTERACTIVE
                        // MODE!
                        setBuf(immCmdSB.toString());
                        immCmdSB.setLength(0);
                        stdprintln(inputMovedtobufferString);
                        continue;
                    }

                    deTerminated = SqlFile.deTerminated(inputLine);

                    // A null terminal line (i.e., /\s*;\s*$/) is never useful.
                    if (!trimmedInput.equals(";")) {
                        if (immCmdSB.length() > 0) {
                            immCmdSB.append('\n');
                        }

                        immCmdSB.append((deTerminated == null) ? inputLine
                                                                   : deTerminated);
                    }

                    if (deTerminated == null) {
                        continue;
                    }

                    // If we reach here, then immCmdSB contains a complete
                    // SQL command.

                    if (immCmdSB.toString().trim().length() == 0) {
                        immCmdSB.setLength(0);
                        throw new SqlToolError(sqlstatementEmptyString);
                        // There is nothing inherently wrong with issuing
                        // an empty command, like to test DB server health.
                        // But, this check effectively catches many syntax
                        // errors early, and the DB check can be done by
                        // sending a comment like "// comment".
                    }

                    setBuf(immCmdSB.toString());
                    immCmdSB.setLength(0);
                    historize();
                    processSQL();
                } catch (BadSpecial bs) {
                    errprintln(erroratString + " '"
                               + ((file == null) ? "stdin"
                                                 : file.toString()) + "' "
                                                + lineString
                                                 + ' ' + curLinenum + ':');
                    errprintln("\"" + inputLine + '"');
                    errprintln(bs.getMessage());
                    Throwable cause = bs.getCause();
                    if (cause != null) {
                        errprintln(causeString + ": " + cause);
                    }

                    if (!continueOnError) {
                        throw new SqlToolError(bs);
                    }
                } catch (SQLException se) {
                    errprintln("SQL " + erroratString + " '" + ((file == null)
                                ? "stdin"
                                : file.toString()) + "' " + lineString + ' '
                                        + curLinenum + ':');
                    if (lastSqlStatement != null)
                        errprintln("\"" + lastSqlStatement + '"');
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
                        errprintln(breakUnsatisfiedString + " (" + typeString
                                + ' ' + msg + ").");
                    }

                    if (recursed ||!continueOnError) {
                        throw be;
                    }
                } catch (ContinueException ce) {
                    String msg = ce.getMessage();

                    if (recursed) {
                        rollbackUncoms = false;
                    } else {
                        errprintln(continueUnsatisfiedString
                                   + ((msg == null) ? ""
                                                    : (" (" + typeString + ' '
                                                        + msg + ')')) + '.');
                    }

                    if (recursed ||!continueOnError) {
                        throw ce;
                    }
                } catch (QuitNow qn) {
                    throw qn;
                } catch (SqlToolError ste) {
                    errprintln(erroratString + " '"
                               + ((file == null) ? "stdin"
                                                 : file.toString()) + "' "
                                                        + lineString + ' '
                                                        + curLinenum + ':');
                    errprintln("\"" + inputLine + '"');
                    errprintln(ste.getMessage());
                    Throwable cause = ste.getCause();
                    if (cause != null) {
                        errprintln(causeString + ": " + cause);
                    }
                    if (!continueOnError) {
                        throw ste;
                    }
                }

                immCmdSB.setLength(0);
            }

            if (inComment || immCmdSB.length() != 0) {
                errprintln(inputUnterminatedString + ":  [" + immCmdSB + ']');
                throw new SqlToolError(inputUnterminatedString + ":  ["
                                       + immCmdSB + ']');
            }

            rollbackUncoms = false;
            // Exiting gracefully, so don't roll back.
        } catch (IOException ioe) {
            throw new SqlToolError(primaryinputAccessfailureString, ioe);
        } catch (QuitNow qn) {
            if (recursed) {
                throw qn;
                // Will rollback if conditions otherwise require.
                // Otherwise top level will decide based upon qn.getMessage().
            }
            rollbackUncoms = (qn.getMessage() != null);

            if (rollbackUncoms) {
                errprintln(abortingString + ": " + qn.getMessage());
                throw new SqlToolError(qn.getMessage());
            }

            return;
        } finally {
            closeQueryOutputStream();

            if (fetchingVar != null) {
                errprintln(plvarsetIncompleteString + ":  " + fetchingVar);
                rollbackUncoms = true;
            }

            if (br != null) try {
                br.close();
            } catch (IOException ioe) {
                throw new SqlToolError(inputreaderClosefailureString, ioe);
            }

            if (rollbackUncoms && possiblyUncommitteds.get()) {
                errprintln(rollingbackString);
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
    static private class BadSpecial extends AppendableException {
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
    private class BadSubst extends Exception {
        static final long serialVersionUID = 7325933736897253269L;

        BadSubst(String s) {
            super(s);
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
     * Execute processSql/processPL/processSpecial from buffer.
     */
    public void processFromBuffer()
            throws BadSpecial, SQLException, SqlToolError {
        historize();
        if (buffer.charAt(0) == '*' && (buffer.length() < 2
                || buffer.charAt(1) != '{')) {
            // Test above just means commands starting with *, EXCEPT
            // for commands beginning with *{.
            processPL(buffer);
            return;
        }

        if (buffer.charAt(0) == '\\') {
            processSpecial(buffer);
            return;
        }
        processSQL();
    }

    /**
     * Process a Buffer/History Command.
     *
     * Due to the nature of the goal here, we don't trim() "other" like
     * we do for other kinds of commands.
     *
     * @param inString Complete command, less the leading ':' character.
     * @throws SQLException  thrown by JDBC driver.
     * @throws BadSpecial    special-command-specific errors.
     * @throws SqlToolError  all other errors.
     */
    private void processBuffHist(String inString)
    throws BadSpecial, SQLException, SqlToolError {
        if (inString.length() < 1) {
            throw new BadSpecial(specialUnspecifiedString);
        }

        char commandChar = inString.charAt(0);
        String other       = inString.substring(1);
        if (other.trim().length() == 0) {
            other = null;
        }
        // other is useful for some, but not all buffer commands.
        // "-32" and "281" must use inString directly.

        switch (commandChar) {
            case ';' :
                SqlFile.enforce1charBH(other, ';');
                if (buffer == null) throw new BadSpecial(nobufferString);

                stdprintln(bufferExecutingString + ':');
                stdprintln(buffer);
                stdprintln();
                processFromBuffer();

                return;

            case 'a' :
                if (buffer == null) throw new BadSpecial(nobufferString);
                immCmdSB.append(buffer);

                if (other != null) {
                    String deTerminated = SqlFile.deTerminated(other);

                    if (!other.equals(";")) {
                        immCmdSB.append(((deTerminated == null)
                                ? other : deTerminated));
                    }

                    if (deTerminated != null) {
                        // If we reach here, then immCmdSB contains a
                        // complete command.

                        setBuf(immCmdSB.toString());
                        immCmdSB.setLength(0);
                        stdprintln(executingString + ':');
                        stdprintln(buffer);
                        stdprintln();
                        processFromBuffer();

                        return;
                    }
                }

                withholdPrompt = true;
                stdprint(immCmdSB.toString());

                return;

            case 'l' :
            case 'b' :
                SqlFile.enforce1charBH(other, 'l');
                if (buffer == null) {
                    stdprintln(nobufferYetString);
                } else {
                    stdprintln(bufferCurrentString + ':');
                    stdprintln(buffer);
                }

                return;

            case 'h' :
                SqlFile.enforce1charBH(other, 'h');
                showHistory();

                return;

            case '-' :
            case '0' :
            case '1' :
            case '2' :
            case '3' :
            case '4' :
            case '5' :
            case '6' :
            case '7' :
            case '8' :
            case '9' :
                boolean executeMode =
                    inString.charAt(inString.length() - 1) == ';';

                String numStr = inString.substring(0,
                        inString.length() + (executeMode ? -1 : 0));
                        // Trim off terminating ';'

                try {
                    setBuf(commandFromHistory(Integer.parseInt(numStr)));
                } catch (NumberFormatException nfe) {
                    throw new BadSpecial(commandnumMalformatString + " "
                            + numStr + "'", nfe);
                }

                if (executeMode) {
                    processFromBuffer();
                } else {
                    stdprintln(bufferRestoredString + ':');
                    stdprintln(buffer);
                }

                return;

            case 's' :
                boolean modeExecute = false;
                boolean modeGlobal = false;

                try {
                    if (other == null || other.length() < 3) {
                        throw new BadSubst(substitutionMalformattedString);
                    }
                    char delim = other.charAt(0);
                    Matcher m = substitutionPattern.matcher(inString);
                    if (buffer == null) {
                        stdprintln(nobufferYetString);
                        return;
                    }
                    if (!m.matches()) {
                        throw new BadSubst(substitutionMalformattedString);
                    }

                    // Note that this pattern does not include the leading :.
                    if (m.groupCount() < 3 || m.groupCount() > 4) {
                        // Assertion failed
                        throw new RuntimeException("Matched substitution pattern, "
                            + "but captured " + m.groupCount() + " groups");
                    }
                    String optionGroup = (
                            (m.groupCount() > 3 && m.group(4) != null)
                            ? (new String(m.group(4))) : null);

                    if (optionGroup != null) {
                        if (optionGroup.indexOf(';') > -1) {
                            modeExecute = true;
                            optionGroup = optionGroup.replaceFirst(";", "");
                        }
                        if (optionGroup.indexOf('g') > -1) {
                            modeGlobal = true;
                            optionGroup = optionGroup.replaceFirst("g", "");
                        }
                    }

                    Matcher bufferMatcher = Pattern.compile("(?s"
                            + ((optionGroup == null) ? "" : optionGroup)
                            + ')' + m.group(2)).matcher(buffer);
                    String newBuffer = (modeGlobal
                            ? bufferMatcher.replaceAll(m.group(3))
                            : bufferMatcher.replaceFirst(m.group(3)));
                    if (newBuffer.equals(buffer)) {
                        stdprintln(substitutionNomatchString);
                        return;
                    }

                    setBuf(newBuffer);
                    stdprintln((modeExecute ? executingString
                                            : bufferCurrentString) + ':');
                    stdprintln(buffer);

                    if (modeExecute) {
                        stdprintln();
                    }
                } catch (PatternSyntaxException pse) {
                    throw new BadSpecial(substitutionSyntaxString
							+ ":  \":s/from regex/to string/igm;\".  ", pse);
                } catch (BadSubst badswitch) {
                    throw new BadSpecial(badswitch.getMessage()
                            + LS + substitutionSyntaxString + ":  \":s/from "
                            + "regex/to string/igm;\".");
                }

                if (modeExecute) {
                    immCmdSB.setLength(0);
                    processFromBuffer();
                }

                return;

            case '?' :
                stdprintln(BUFFER_HELP_TEXT);

                return;
        }

        throw new BadSpecial(bufferUnknownString + ": " + commandChar);
    }

    private boolean doPrepare   = false;
    private String  prepareVar  = null;
    private String  dsvColDelim = null;
    private String  dsvSkipPrefix = null;
    private String  dsvRowDelim = null;
    private static final String DSV_X_SYNTAX_MSG =
		bundle.getString("dsv.x.syntax");
    private static final String DSV_M_SYNTAX_MSG =
		bundle.getString("dsv.m.syntax");

    private static void enforce1charSpecial(String token, char command)
            throws BadSpecial {
        if (token.length() != 1) {
            throw new BadSpecial(specialExtracharsString + command
                    + ":  " + token.substring(1));
        }
    }
    private static void enforce1charBH(String token, char command)
            throws BadSpecial {
        if (token != null) {
            throw new BadSpecial(bufferExtracharsString + command
                    + ":  " + token);
        }
    }

    /**
     * Process a Special Command.
     *
     * @param inString Complete command, including the leading '\' character.
     * @throws SQLException thrown by JDBC driver.
     * @throws BadSpecial special-command-specific errors.
     * @throws SqlToolError all other errors, plus QuitNow,
     *                      BreakException, ContinueException.
     */
    private void processSpecial(String inString)
    throws BadSpecial, QuitNow, SQLException, SqlToolError {
        Matcher m = specialPattern.matcher(
                plMode ? dereference(inString, false) : inString);
        if (!m.matches()) {
            throw new BadSpecial(specialMalformattedString + ":  "
                    + inString);
        }
        if (m.groupCount() < 1 || m.groupCount() > 2) {
            // Failed assertion
            throw new RuntimeException(
                    "Pattern matched, yet captured " + m.groupCount()
                    + " groups");
        }

        String arg1 = m.group(1);
        String other = ((m.groupCount() > 1) ? m.group(2) : null);

        switch (arg1.charAt(0)) {
            case 'q' :
                SqlFile.enforce1charSpecial(arg1, 'q');
                if (other != null) {
                    throw new QuitNow(other);
                }

                throw new QuitNow();
            case 'H' :
                SqlFile.enforce1charSpecial(arg1, 'H');
                htmlMode = !htmlMode;

                stdprintln(htmlModeString + ": " + htmlMode);

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
                String skipPrefix = null;

                if (noComments) {
                    other = other.substring(0, other.length()-1).trim();
                    if (other.length() < 1) {
                        throw new BadSpecial(DSV_M_SYNTAX_MSG);
                    }
                } else {
                    skipPrefix = dsvSkipPrefix;
                }
                int colonIndex = other.indexOf(" :");
                if (colonIndex > -1 && colonIndex < other.length() - 2) {
                    skipPrefix = other.substring(colonIndex + 2);
                    other = other.substring(0, colonIndex).trim();
                }

                importDsv(other, skipPrefix);

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

                    if (dsvTargetFile == null && tableName == null) {
                        throw new BadSpecial(dsvTargetfileRequiredString);
                    }
                    File dsvFile = new File((dsvTargetFile == null)
                                            ? (tableName + ".dsv")
                                            : dsvTargetFile);

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
                    stdprintln(wroteString + ' ' + dsvFile.length() + ' '
                               + characterstofile + " '" + dsvFile + "'");
                } catch (FileNotFoundException e) {
                    throw new BadSpecial(fileNowriteString + " '" + other
                                         + "'", e);
                } catch (UnsupportedEncodingException e) {
                    throw new BadSpecial(fileNowriteString + " '" + other
                                         + "'", e);
                } finally {
                    // Reset all state changes
                    if (pwDsv != null) {
                        pwDsv.close();
                    }

                    pwDsv       = null;
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
                    throw new BadSpecial(metadataNoobtainString, se);
                }

                throw new BadSpecial(specialDLikeString);
            case 'o' :
                SqlFile.enforce1charSpecial(arg1, 'o');
                if (other == null) {
                    if (pwQuery == null) {
                        throw new BadSpecial(outputfileNonetocloseString);
                    }

                    closeQueryOutputStream();

                    return;
                }

                if (pwQuery != null) {
                    stdprintln(outputfileReopeningString);
                    closeQueryOutputStream();
                }

                try {
                    pwQuery = new PrintWriter(
                        new OutputStreamWriter(
                            new FileOutputStream(other, true), charset));

                    /* Opening in append mode, so it's possible that we will
                     * be adding superfluous <HTML> and <BODY> tags.
                     * I think that browsers can handle that */
                    pwQuery.println((htmlMode
							? ("<HTML>" + LS + "<!--")
							: "#") + " " + (new java.util.Date()) + ".  "
									+ outputfileHeaderString + ' '
									+ getClass().getName()
									+ (htmlMode ? (". -->" + LS + LS + "<BODY>")
												: ("." + LS)));
                    pwQuery.flush();
                } catch (Exception e) {
                    throw new BadSpecial(fileNowriteString + " '" + other
							+ "':  " + e);
                }

                return;

            case 'w' :
                SqlFile.enforce1charSpecial(arg1, 'w');
                if (other == null) {
                    throw new BadSpecial(destfileDemandString);
                }

                if (buffer == null || buffer.length() == 0) {
                    throw new BadSpecial(bufferEmptyString);
                }

                try {
                    PrintWriter pw = new PrintWriter(
                        new OutputStreamWriter(
                            new FileOutputStream(other, true), charset));

                    pw.println(buffer + ';');
                    pw.flush();
                    pw.close();
                } catch (Exception e) {
                    throw new BadSpecial(fileNoappendString + " '" + other
                                         + "':  " + e);
                }

                return;

            case 'i' :
                SqlFile.enforce1charSpecial(arg1, 'i');
                if (other == null) {
                    throw new BadSpecial(sqlfileNameDemandString);
                }

                try {
                    SqlFile sf = new SqlFile(new File(other), false, userVars);

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
                    throw new BadSpecial(sqlfileExecuteFailString
							 + " '" + other + "'", e);
                }

                return;

            case 'p' :
                SqlFile.enforce1charSpecial(arg1, 'p');
                if (other == null) {
                    stdprintln(true);
                } else {
                    stdprintln(other, true);
                }

                return;

            case 'a' :
                SqlFile.enforce1charSpecial(arg1, 'a');
                if (other != null) {
                    curConn.setAutoCommit(
                        Boolean.valueOf(other).booleanValue());
                }

                stdprintln(autocommitSettingString + ": "
                           + curConn.getAutoCommit());

                return;
            case '=' :
                SqlFile.enforce1charSpecial(arg1, '=');
                curConn.commit();
                possiblyUncommitteds.set(false);
                stdprintln(committedString);

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
                    throw new BadSpecial(specialBMalformattedString);
                }

                File file = new File(other);

                try {
                    if (arg1.charAt(1) == 'd') {
                        dump(file);
                    } else {
                        binBuffer = SqlFile.loadBinary(file);
                        stdprintln(loadedString + ' ' + binBuffer.length
                                   + ' ' + binaryBytesintoString);
                                    }
                } catch (BadSpecial bs) {
                    throw bs;
                } catch (IOException ioe) {
                    throw new BadSpecial(binaryFilefailString + " '" + other
                        + "'", ioe);
                }

                return;

            case '*' :
            case 'c' :
                SqlFile.enforce1charSpecial(arg1, '=');
                if (other != null) {
                    // But remember that we have to abort on some I/O errors.
                    continueOnError = Boolean.valueOf(other).booleanValue();
                }

                stdprintln(cSettingString + ": " + continueOnError);

                return;

            case '?' :
                stdprintln(SPECIAL_HELP_TEXT);

                return;

            case '!' :
                // N.b. This DOES NOT HANDLE UNIX shell wildcards, since there
                // is no UNIX shell involved.
                // Doesn't make sense to incur overhead of a shell without
                // stdin capability.
                // Can't provide stdin to the executed program because
                // the forked program could gobble up program input,
                // depending on how SqlTool was invoked, nested scripts,
                // etc.

                // I'd like to execute the user's default shell if they
                // ran "\!" with no argument, but (a) there is no portable
                // way to determine the user's default or login shell; and
                // (b) shell is useless without stdin ability.
                InputStream stream;
                byte[]      ba         = new byte[1024];
                String      extCommand = ((arg1.length() == 1)
                        ? "" : arg1.substring(1))
                    + ((arg1.length() > 1 && other != null)
                       ? " " : "") + ((other == null) ? "" : other);
                if (extCommand.trim().length() < 1)
                    throw new BadSpecial(bangIncompleteString);

                try {
                    Runtime runtime = Runtime.getRuntime();
                    Process proc = ((wincmdPattern == null)
                            ? runtime.exec(extCommand)
                            : runtime.exec(genWinArgs(extCommand))
                    );

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
                        throw new BadSpecial(bangCommandFailString + ": '"
								+ extCommand + "'");
                    }
                } catch (BadSpecial bs) {
                    throw bs;
                } catch (Exception e) {
                    throw new BadSpecial(bangCommandFailString + " '"
                                         + extCommand + "'", e);
                }

                return;

            case '.' :
                SqlFile.enforce1charSpecial(arg1, '.');
                rawMode = true;

                if (interactive) {
                    stdprintln(RAW_LEADIN_MSG);
                }

                return;
        }

        throw new BadSpecial(specialUnknownString);
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

            varValue = (varName.equals("?") ? lastVal
                        : (String) userVars.get(varName));
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
    private String  lastVal = null;
    private boolean silentFetch = false;
    private boolean fetchBinary = false;

    /**
     * Process a Process Language Command.
     * Nesting not supported yet.
     *
     * @param inString Complete command, including the leading '\' character.
     * @throws BadSpecial special-command-specific errors.
     * @throws SqlToolError all other errors, plus BreakException and
     *                      ContinueException.
     */
    private void processPL(String inString) throws BadSpecial, SqlToolError {
        Matcher m = plPattern.matcher(dereference(inString, false));
        if (!m.matches()) {
            throw new BadSpecial("Malformatted PL command:  " + inString);
        }
        if (m.groupCount() < 1 || m.group(1) == null) {
            plMode = true;
            stdprintln("PL variable expansion mode is now on");
            return;
        }

        String[] tokens = m.group(1).split("\\s+");

        if (tokens[0].charAt(0) == '?') {
            stdprintln(PL_HELP_TEXT);

            return;
        }

        // If user runs any PL command, we turn PL mode on.
        plMode = true;

        if (tokens[0].equals("end")) {
            throw new BadSpecial("PL end statements may only occur inside of "
                                 + "a PL block");
        }

        if (tokens[0].equals("continue")) {
            if (tokens.length > 1) {
                if (tokens.length == 2 &&
                        (tokens[1].equals("foreach") ||
                         tokens[1].equals("while"))) {
                    throw new ContinueException(tokens[1]);
                }
                throw new BadSpecial(
                    "Bad continue statement."
                    + "You may use no argument or one of 'foreach', "
                    + "'while'");
            }

            throw new ContinueException();
        }

        if (tokens[0].equals("break")) {
            if (tokens.length > 1) {
                if (tokens.length == 2 &&
                        (tokens[1].equals("foreach") ||
                         tokens[1].equals("if") ||
                         tokens[1].equals("while") ||
                         tokens[1].equals("file"))) {
                    throw new BreakException(tokens[1]);
                }
                throw new BadSpecial(
                    "Bad break statement."
                    + "You may use no argument or one of 'foreach', "
                    + "'if', 'while', 'file'");
            }

            throw new BreakException();
        }

        if (tokens[0].equals("list") || tokens[0].equals("listvalues")) {
            String  s;
            boolean doValues = (tokens[0].equals("listvalues"));

            if (tokens.length == 1) {
                stdprint(SqlFile.formatNicely(userVars, doValues));
            } else {
                if (doValues) {
                    stdprintln("The outermost parentheses are not part of "
                               + "the values.");
                } else {
                    stdprintln("Showing variable names and length of values "
                               + "(use 'listvalues' to see values).");
                }

                for (int i = 1; i < tokens.length; i++) {
                    s = (String) userVars.get(tokens[i]);

                    stdprintln("    " + tokens[i] + ": "
                               + (doValues ? ("(" + s + ')')
                                           : Integer.toString(s.length())));
                }
            }

            return;
        }

        if (tokens[0].equals("dump") || tokens[0].equals("load")) {
            if (tokens.length != 3) {
                throw new BadSpecial("Malformatted PL dump/load command");
            }

            String varName = tokens[1];

            if (varName.charAt(0) == ':') {
                throw new BadSpecial("PL variable names may not begin with ':'");
            }
            File   file    = new File(tokens[2]);

            try {
                if (tokens[0].equals("dump")) {
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

        if (tokens[0].equals("prepare")) {
            if (tokens.length != 2) {
                throw new BadSpecial("Malformatted prepare command");
            }

            if (userVars.get(tokens[1]) == null) {
                throw new BadSpecial("Use of unset PL variable: " + tokens[1]);
            }

            prepareVar = tokens[1];
            doPrepare  = true;

            return;
        }

        if (tokens[0].equals("foreach")) {
            Matcher foreachM= foreachPattern.matcher(
                    dereference(inString, false));
            if (!foreachM.matches()) {
                throw new BadSpecial("Malformatted PL foreach command: "
                        + inString);
            }
            if (foreachM.groupCount() != 2) {
                throw new RuntimeException(
                        "foreach pattern matched, but captured "
                        + foreachM.groupCount() + " groups");
            }

            String varName   = foreachM.group(1);
            if (varName.charAt(0) == ':') {
                throw new BadSpecial("PL variable names may not begin with ':'");
            }
            String[] values = foreachM.group(2).split("\\s+");
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
                        updateUserSettings();

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
                updateUserSettings();
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

        if (tokens[0].equals("if") || tokens[0].equals("while")) {
            Matcher ifwhileM= ifwhilePattern.matcher(
                    dereference(inString, false));
            if (!ifwhileM.matches()) {
                throw new BadSpecial("Malformatted PL if/while command: "
                        + inString);
            }
            if (ifwhileM.groupCount() != 1) {
                throw new RuntimeException(
                        "if/while pattern matched, but captured "
                        + ifwhileM.groupCount() + " groups");
            }

            String[] values =
                    ifwhileM.group(1).replaceAll("!([a-zA-Z0-9*])", "! $1").
                            replaceAll("([a-zA-Z0-9*])!", "$1 !").split("\\s+");
            File tmpFile = null;

            if (tokens[0].equals("if")) {
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
            } else if (tokens[0].equals("while")) {
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
            } else {
                // Assertion
                throw new RuntimeException("Unexpected PL command: "
                        + tokens[0]);
            }

            if (tmpFile != null &&!tmpFile.delete()) {
                throw new BadSpecial(
                    "Error occurred while trying to remove temp file '"
                    + tmpFile + "'");
            }

            return;
        }

        /* Since we want to permit both "* VARNAME = X" and
         * "* VARNAME=X" (i.e., whitespace is OPTIONAL in both positions),
         * we can't use the Tokenzier.  Therefore, start over again with
         * the string. */
        m = varsetPattern.matcher(dereference(inString, false));
        if (!m.matches()) {
            throw new BadSpecial("Malformatted PL var set command:  "
                    + inString);
        }
        if (m.groupCount() < 2 || m.groupCount() > 3) {
            // Assertion
            throw new RuntimeException("varset patter matched but captured "
                    + m.groupCount() + " groups");
        }

        String varName  = m.group(1);

        if (varName.charAt(0) == ':') {
            throw new BadSpecial("PL variable names may not begin with ':'");
        }

        switch (m.group(2).charAt(0)) {
            case '_' :
                silentFetch = true;
            case '~' :
                if (m.groupCount() > 2 && m.group(3) != null) {
                    throw new BadSpecial(
                        "PL ~/_ set commands take no other args ("
                        + m.group(3) + ')');
                }

                userVars.remove(varName);
                updateUserSettings();

                fetchingVar = varName;

                return;

            case '=' :
                if (fetchingVar != null && fetchingVar.equals(varName)) {
                    fetchingVar = null;
                }

                if (m.groupCount() > 2 && m.group(3) != null) {
                    userVars.put(varName, m.group(3));
                    updateUserSettings();
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
    private File plBlockFile(String seeking) throws IOException, SqlToolError {
        String          s;

        // Have already read the if/while/foreach statement, so we are already
        // at nest level 1.  When we reach nestlevel 1 (read 1 net "end"
        // statement), we're at level 0 and return.
        int    nestlevel = 1;
        String curPlCommand;

        if (seeking == null
                || ((!seeking.equals("foreach")) && (!seeking.equals("if"))
                    && (!seeking.equals("while")))) {
            throw new RuntimeException(
                "Assertion failed.  Unsupported PL block type:  " + seeking);
        }

        File tmpFile = File.createTempFile("sqltool-", ".sql");
        PrintWriter pw = new PrintWriter(
            new OutputStreamWriter(new FileOutputStream(tmpFile), charset));

        pw.println("/* " + (new java.util.Date()) + ". "
                   + getClass().getName() + " PL block. */");
        pw.println();
        Matcher m;

        while (true) {
            s = br.readLine();

            if (s == null) {
                errprintln("Unterminated '" + seeking + "' PL block");

                throw new SqlToolError("Unterminated '" + seeking
                                       + "' PL block");
            }

            curLinenum++;

            m = plPattern.matcher(s);
            if (m.matches() && m.groupCount() > 0 && m.group(1) != null) {
                String[] tokens = m.group(1).split("\\s+");
                curPlCommand = tokens[0];

                // PL COMMAND of some sort.
                if (curPlCommand.equals(seeking)) {
                    nestlevel++;
                } else if (curPlCommand.equals("end")) {
                    if (tokens.length < 2) {
                        errprintln("PL end statement requires arg of "
                                   + "'foreach' or 'if' or 'while' (1)");

                        throw new SqlToolError(
                            "PL end statement requires arg "
                            + " of 'foreach' or 'if' or 'while' (1)");
                    }

                    String inType = tokens[1];

                    if (inType.equals(seeking)) {
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
     * Process the immediate command as an SQL Statement
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
        lastSqlStatement    = (plMode ? dereference(buffer, true)
                                      : buffer);
        Statement statement = null;

        if (doPrepare) {
            if (lastSqlStatement.indexOf('?') < 1) {
                lastSqlStatement = null;
                throw new SqlToolError(
                    "Prepared statements must contain one '?'");
            }

            doPrepare = false;

            PreparedStatement ps = curConn.prepareStatement(lastSqlStatement);

            if (prepareVar == null) {
                if (binBuffer == null) {
                    lastSqlStatement = null;
                    throw new SqlToolError("Binary SqlFile buffer is empty");
                }

                ps.setBytes(1, binBuffer);
            } else {
                String val = (String) userVars.get(prepareVar);

                if (val == null) {
                    lastSqlStatement = null;
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

            statement.execute(lastSqlStatement);
        }

        possiblyUncommitteds.set(true);

        try {
            displayResultSet(statement, statement.getResultSet(), null, null);
        } finally {
            try {
                statement.close();
            } catch (Exception e) {}
        }
        lastSqlStatement = null;
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
                        case java.sql.Types.ARRAY :
                            // Guessing at how to handle ARRAY.
                        case java.sql.Types.BLOB :
                        case java.sql.Types.CLOB :
                        case java.sql.Types.LONGVARBINARY :
                        case java.sql.Types.LONGVARCHAR :
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

                        lastVal = (val == null) ? nullRepToken : val;
                        if (fetchingVar != null) {
                            userVars.put(fetchingVar, lastVal);
                            updateUserSettings();

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
                                                             : nullRepToken);
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
                                                    : nullRepToken)
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
                lastVal = Integer.toString(updateCount);
                if (fetchingVar != null) {
                    userVars.put(fetchingVar, lastVal);
                    updateUserSettings();
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
     * Display command history.
     */
    private void showHistory() throws BadSpecial {
        if (history == null) {
            throw new BadSpecial("Command history not available");
        }
        if (history.size() < 1) {
            stdprintln("<<    No history yet    >>");
        } else {
            String s;
            for (int i = 0; i < history.size(); i++) {
                psStd.println("#" + (i + oldestHist) + " or "
                        + (i - history.size()) + ':');
                psStd.println((String) history.get(i));
            }
        }
        if (buffer != null) {
            psStd.println("EDIT BUFFER:");
            psStd.println(buffer);
        }

        psStd.println();
        psStd.println(
                "<< Copy a command to buffer like \":27\" or \":-3\".  "
                + "Re-execute buffer like \":;\" >>");
    }

    /**
     * Return a Command from command history.
     */
    private String commandFromHistory(int index) throws BadSpecial {
        if (history == null) {
            throw new BadSpecial("Command history not available");
        }
        if (index == 0) {
            throw new BadSpecial(
                    "You must specify a positive absolute command number, "
                    + "or a negative number meaning X commands 'back'");
        }
        if (index > 0) {
            // Positive command# given
            index -= oldestHist;
            if (index < 0) {
                throw new BadSpecial("History only goes back to #"
                        + oldestHist);
            }
            if (index >= history.size()) {
                throw new BadSpecial("History only goes up to #"
                        + (history.size() + oldestHist - 1));
            }
        } else {
            // Negative command# given
            index += history.size();
            if (index < 0) {
                throw new BadSpecial("History only goes back "
                        + history.size() + " commands");
            }
        }
        return (String) history.get(index);
    }

    private void setBuf(String newContent) {
        buffer = new String(newContent);
        // System.err.println("Buffer is now (" + buffer + ')');
    }

    int oldestHist = 1;

    /**
     * Add a command onto the history list.
     */
    private void historize() {
        if (history == null || buffer == null) {
            return;
        }
        if (history.size() > 0 &&
                history.get(history.size() - 1).equals(buffer)) {
            // Don't store two consecutive commands that are exactly the same.
            return;
        }
        history.add(buffer);
        if (history.size() <= maxHistoryLength) {
            return;
        }
        history.remove(0);
        oldestHist++;
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

    private boolean eval(String[] inTokens) throws BadSpecial {
        // dereference *VARNAME variables.
        // N.b. we work with a "copy" of the tokens.
        boolean  negate = inTokens.length > 0 && inTokens[0].equals("!");
        String[] tokens = new String[negate ? (inTokens.length - 1)
                                            : inTokens.length];
        String inToken;
        String varName;

        for (int i = 0; i < tokens.length; i++) {
            inToken = inTokens[i + (negate ? 1 : 0)];
            if (inToken.length() > 1 && inToken.charAt(0) == '*') {
                varName = inToken.substring(1);
                tokens[i] = varName.equals("?") ? lastVal
                          : (String) userVars.get(inToken.substring(1));
            } else {
                tokens[i] = inTokens[i + (negate ? 1 : 0)];
            }

            // Unset variables permitted in expressions as long as use
            // the short *VARNAME form.
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
                      + "(use 'listvalues' to see values).");
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
        updateUserSettings();
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
                || nullRepToken == null) {
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

        if (s.trim().equals(nullRepToken)) {
            // The trim() is to avoid the situation where the contents of a
            // field "looks like" the null-rep token.
            throw new SqlToolError(
                "Table data contains just our null representation '"
                + nullRepToken + "'");
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
    public void importDsv(String filePath, String skipPrefix)
            throws SqlToolError {
        char[] bfr  = null;
        File   file = new File(filePath);
        SortedMap constColMap = null;
        int constColMapSize = 0;
        if (dsvConstCols != null) {
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
                postOffset = dsvConstCols.indexOf(dsvColDelim, startOffset);
                if (postOffset < 0) postOffset = dsvConstCols.length();
                if (postOffset == startOffset)
                    throw new SqlToolError("*DSV_CONST_COLS has null setting");
                firstEq = dsvConstCols.indexOf('=', startOffset);
                if (firstEq < startOffset + 1 || firstEq > postOffset)
                    throw new SqlToolError("*DSV_CONST_COLS element malformatted");
                n = dsvConstCols.substring(startOffset, firstEq).trim();
                if (n.length() < 1)
                    throw new SqlToolError(
                            "*DSV_CONST_COLS element has null col. name");
                constColMap.put(n,
                        dsvConstCols.substring(firstEq + 1, postOffset));
            } while (postOffset < dsvConstCols.length());
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
        String    tableName = dsvTargetTable;

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
                    || (skipPrefix != null
                            && trimmedLine.startsWith(skipPrefix))) {
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
                : string.substring(colStart, colEnd).trim());

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
        List tmpList = new ArrayList();

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
            tmpList.add(headers[i]);
        }
        boolean[] autonulls = new boolean[headers.length - skippers];
        boolean[] parseDate = new boolean[autonulls.length];
        boolean[] parseBool = new boolean[autonulls.length];
        String[] insertFieldName = (String[]) tmpList.toArray(new String[] {});
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
                parseBool[i] = false;
                switch(rsmd.getColumnType(i + 1)) {
                    case java.sql.Types.BOOLEAN:
                        parseBool[i] = true;
                        break;
                    case java.sql.Types.VARBINARY :
                    case java.sql.Types.VARCHAR :
                    case java.sql.Types.ARRAY :
                        // Guessing at how to handle ARRAY.
                    case java.sql.Types.BLOB :
                    case java.sql.Types.CLOB :
                    case java.sql.Types.LONGVARBINARY :
                    case java.sql.Types.LONGVARCHAR :
                        autonulls[i] = false;
                        // This means to preserve white space and to insert
                        // "" for "".  Otherwise we trim white space and
                        // insert null for \s*.
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
        String tmp = dsvRejectFile;
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
        tmp = dsvRejectReport;
        if (tmp != null) try {
            rejectReportFile = new File(tmp);
            rejectReportWriter = new PrintWriter(
                        new OutputStreamWriter(
                            new FileOutputStream(rejectReportFile), charset));
            rejectReportWriter.println("<HTML>");
            rejectReportWriter.println("<HEAD><STYLE>");
            rejectReportWriter.println("    th { background-color:aqua; }");
            rejectReportWriter.println("    .right { text-align:right; }");
            rejectReportWriter.println("    .reason { font-size: 95%; "
                    + "font-weight:bold;}");
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
            rejectReportWriter.println("    <THEAD><TR><TH>rej.&nbsp;#</TH>"
                    + "<TH>input<BR/>line&nbsp;#</TH><TH>bad&nbsp;column<BR/>"
                    + "(if&nbsp;known)</TH>"
                    + "<TH style='color:red;'>reason</TH></TR></THEAD>");
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
            String   currentFieldName = null;

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
                if (skipPrefix != null
                        && trimmedLine.startsWith(skipPrefix)) {
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
                    currentFieldName = insertFieldName[i];
                    if (autonulls[i]) dataVals[i] = dataVals[i].trim();
                    // N.b. WE SPECIFICALLY DO NOT HANDLE TIMES WITHOUT
                    // DATES, LIKE "3:14:00", BECAUSE, WHILE THIS MAY BE
                    // USEFUL AND EFFICIENT, IT IS NOT PORTABLE.
                    //System.err.println("ps.setString(" + i + ", "
                    //      + dataVals[i] + ')');

                    if (parseDate[i]) {
                        if ((dataVals[i].length() < 1 && autonulls[i])
                              || dataVals[i].equals(nullRepToken)) {
                            ps.setTimestamp(i + 1, null);
                        } else {
                            dateString = (dataVals[i].indexOf(':') > 0)
                                       ? dataVals[i]
                                       : (dataVals[i] + " 0:00:00");
                            // BEWARE:  This may not work for some foreign
                            // date/time formats.
                            try {
                                ps.setTimestamp(i + 1,
                                        java.sql.Timestamp.valueOf(dateString));
                            } catch (IllegalArgumentException iae) {
                                throw new RowError("Bad date/time value '"
                                    + dateString + "'", iae);
                            }
                        }
                    } else if (parseBool[i]) {
                        if ((dataVals[i].length() < 1 && autonulls[i])
                              || dataVals[i].equals(nullRepToken)) {
                            ps.setNull(i + 1, java.sql.Types.BOOLEAN);
                        } else {
                            try {
                                ps.setBoolean(i + 1, Boolean.valueOf(
                                        dataVals[i]).booleanValue());
                                // Boolean... is equivalent to Java 4's
                                // Boolean.parseBoolean().
                            } catch (IllegalArgumentException iae) {
                                throw new RowError("Bad boolean value '"
                                    + dataVals[i] + "'", iae);
                            }
                        }
                    } else {
                        ps.setString(
                            i + 1,
                            (((dataVals[i].length() < 1 && autonulls[i])
                              || dataVals[i].equals(nullRepToken))
                             ? null
                             : dataVals[i]));
                    }
                    currentFieldName = null;
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
                                + lineCount + "</TD><TD>"
                                + ((currentFieldName == null) ? "&nbsp;"
                                        : currentFieldName)
                                + "</TD><TD><PRE class='reason'>"
                                + re.getMessage()
                                + ((cause == null) ? "" : ("<HR/>" + cause))
                                + "</PRE></TD></TR>");
                    }
                } else {
                    importAborted = true;
                    throw new SqlToolError("Parse or insert of input line "
                            + lineCount + " failed"
                            + ((currentFieldName == null) ? ""
                                : (", bad column '" + currentFieldName + "'"))
                            + ".  " + re.getMessage(),
                            cause);
                }
            }
        } finally {
            String summaryString = null;
            if (recCount > 0) {
                summaryString = "Import summary ("
                        + ((skipPrefix == null) ? "" : ("'" + skipPrefix
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
                            "    <TR><TD colspan='4' "
                            + "style='border:3px solid blue; color:blue; "
                            + "font-weight:bold;'>");
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

    /**
     * Does a poor-man's parse of a MSDOS command line and parses it
     * into a WIndows cmd.exe invocation to approximate.
     */
    static private String[] genWinArgs(String monolithic) {
        List list = new ArrayList();
        list.add("cmd.exe");
        list.add("/y");
        list.add("/c");
        Matcher m = wincmdPattern.matcher(monolithic);
        String[] internalTokens;
        while (m.find()) {
            for (int i = 1; i <= m.groupCount(); i++) {
                if (m.group(i) == null) continue;
                if (m.group(i).length() > 1 && m.group(i).charAt(0) == '"') {
                    list.add(m.group(i).substring(1, m.group(i).length() - 1));
                    continue;
                }
                internalTokens = m.group(i).split("\\s+");
                for (int j = 0; j < internalTokens.length; j++)
                    list.add(internalTokens[j]);
            }
        }
        return (String[]) list.toArray(new String[] {});
    }
}
